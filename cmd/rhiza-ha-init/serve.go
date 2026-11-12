package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/managedbootstrap"
	"github.com/mrchypark/rhiza/pkg/managedsql"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func managedConfig(ctx context.Context, kube *operator.Kubernetes, ref operator.V1Alpha2ClusterRef, podName, podUID, identityPath, handoffPath string, profile operator.V1Alpha2RuntimeProfile) (managedsql.Config, error) {
	return managedbootstrap.ManagedConfig(ctx, kube, ref, podName, podUID, identityPath, handoffPath, profile)
}

func managedRuntimeConfig(ctx context.Context, kube *operator.Kubernetes, ref operator.V1Alpha2ClusterRef, podUID, identityPath string, profile operator.V1Alpha2RuntimeProfile, plan operator.V1Alpha2BootstrapPlan) (managedsql.Config, error) {
	return managedbootstrap.ManagedRuntimeConfig(ctx, kube, ref, podUID, identityPath, profile, plan)
}

func serveManaged(ctx context.Context, cfg managedsql.Config, listen string) (resultErr error) {
	host, err := managedsql.Open(ctx, cfg)
	if host != nil {
		defer func() { resultErr = errors.Join(resultErr, host.Close()) }()
	}
	if err != nil {
		return err
	}
	handler, err := managedHandler(host, cfg.Runtime.AdminToken)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 60 * time.Second}
	serveCtx, cancelServe := context.WithCancel(ctx)
	defer cancelServe()
	repairDone := make(chan struct{})
	go func() { defer close(repairDone); runManagedReplacementLoop(serveCtx, host, cfg) }()
	defer func() { cancelServe(); <-repairDone }()
	go func() {
		<-serveCtx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	err = server.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func runManagedReplacementLoop(ctx context.Context, host *managedsql.Host, cfg managedsql.Config) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	last := ""
	for {
		if ctx.Err() != nil {
			return
		}
		attempt, cancel := context.WithTimeout(ctx, 30*time.Second)
		// Control has its own current-voter/live-authority checks; pending SQL
		// must not prevent resuming the exact already-closed repair.
		stage, err := reconcileManagedReplacement(attempt, host, cfg)
		cancel()
		outcome := stage + ":ok"
		if err != nil {
			outcome = fmt.Sprintf("%s:%T", stage, err)
		}
		if outcome != last && ctx.Err() == nil {
			// Only fixed stage/error type is logged, never SDK/profile contents.
			log.Printf("managed replacement %s", outcome)
			last = outcome
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func reconcileManagedReplacement(ctx context.Context, host *managedsql.Host, cfg managedsql.Config) (string, error) {
	stage, err := host.ReconcileExternalReplicas(ctx)
	if err != nil || stage != "disabled" && stage != "idle" && stage != "ordinary-repair" {
		return stage, err
	}
	a, err := operator.LoadV1Alpha2Admission(ctx, cfg.Store, cfg.Ref)
	if err != nil {
		return "authority", err
	}
	if r, ok := a.Repair(); ok && r.State == "Closed" {
		plan, err := host.PlanVoterReplacement(ctx, operator.V1Alpha2RepairPodRequest{}, nil)
		if err != nil {
			return "closed-candidate-blocked", err
		}
		stage := "closed-remove"
		if r.PreparedHandoff != nil {
			stage = "closed-learner-add"
		}
		return stage, host.ReplaceVoter(ctx, *plan)
	}
	_, port, err := net.SplitHostPort(cfg.Runtime.PeerAddr)
	if err != nil {
		return "discovery", err
	}
	peerPort, err := strconv.Atoi(port)
	if err != nil {
		return "discovery", err
	}
	pods, err := operator.ObserveV1Alpha2ManagedPods(ctx, cfg.Store, cfg.Ref, peerPort)
	if err != nil {
		return "discovery", err
	}
	present := make([]types.NodeID, 0, len(pods))
	for _, pod := range pods {
		present = append(present, pod.Member.NodeID)
	}
	for _, pod := range pods {
		if !pod.Candidate {
			continue
		}
		plan, err := host.PlanVoterReplacement(ctx, pod.Member, present)
		if err != nil {
			return "plan", err
		}
		if plan != nil {
			return "replace", host.ReplaceVoter(ctx, *plan)
		}
	}
	return "idle", nil
}

func checkReady(ctx context.Context, client *http.Client, endpoint, token string) error {
	if token == "" {
		return errors.New("private API authentication token required")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return errors.New("invalid readiness endpoint")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := client.Do(req)
	if err != nil {
		return errors.New("managed host not ready")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return errors.New("managed host not ready")
	}
	return nil
}

func managedOperationUnavailable(w http.ResponseWriter, operation string, err error) {
	switch operation {
	case "execute", "query", "recover-pending", "leave-voter":
	default:
		operation = "unknown"
	}
	code, stage, recoveryRequired := "internal", "unknown", false
	switch {
	case errors.Is(err, network.ErrCommitUnknown):
		code, recoveryRequired = "commit_unknown", true
	case errors.Is(err, managedsql.ErrSQLNotSubmitted):
		code = "sql_not_submitted"
		var pre *managedsql.SQLNotSubmittedError
		if errors.As(err, &pre) {
			recoveryRequired = pre.RecoveryRequired
			if recoveryRequired {
				code = "sql_recovery_required"
			}
			switch pre.Stage {
			case managedsql.SQLStageValidation, managedsql.SQLStageAuthority, managedsql.SQLStagePending, managedsql.SQLStageRepair, managedsql.SQLStageAdmission, managedsql.SQLStageReservation:
				stage = string(pre.Stage)
			}
		}
	case errors.Is(err, context.DeadlineExceeded):
		code = "timeout"
	case errors.Is(err, context.Canceled):
		code = "canceled"
	case errors.Is(err, network.ErrInvalidRequest):
		code = "invalid_request"
	case errors.Is(err, network.ErrNotReady):
		code = "not_ready"
	case errors.Is(err, quepaxa.ErrQuorumUnavailable):
		code = "quorum_unavailable"
	case errors.Is(err, network.ErrRequestConflict):
		code = "request_conflict"
	case errors.Is(err, network.ErrDurabilityUnavailable):
		code = "durability_unavailable"
	case errors.Is(err, network.ErrOverloaded):
		code = "overloaded"
	}
	// Only allowlisted values: causes may contain SQL, request IDs or credentials.
	log.Printf("managed SQL unavailable operation=%s code=%s stage=%s recovery_required=%t", operation, code, stage, recoveryRequired)
	http.Error(w, "operation unavailable", http.StatusServiceUnavailable)
}

// managedHandler exposes only the private reference host's guarded SQL API.
func managedHandler(host *managedsql.Host, token string) (http.Handler, error) {
	if host == nil || token == "" {
		return nil, errors.New("managed host and authentication token required")
	}
	mux := http.NewServeMux()
	decode := func(w http.ResponseWriter, r *http.Request, dst any) bool {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return false
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		d := json.NewDecoder(r.Body)
		d.DisallowUnknownFields()
		if d.Decode(dst) != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return false
		}
		var extra any
		if d.Decode(&extra) != io.EOF {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return false
		}
		return true
	}
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if host.Ready(r.Context()) != nil {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/execute", func(w http.ResponseWriter, r *http.Request) {
		var req managedsql.ExecuteRequest
		if !decode(w, r, &req) {
			return
		}
		result, err := host.Execute(r.Context(), req)
		if err != nil {
			managedOperationUnavailable(w, "execute", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	})
	mux.HandleFunc("/recover-pending", func(w http.ResponseWriter, r *http.Request) {
		var req struct{}
		if !decode(w, r, &req) {
			return
		}
		// Resolve only the durable original; never accept replacement SQL or
		// replay the canceled mutation. Unknown outcomes stay write-blocked.
		if err := host.RecoverPending(r.Context()); err != nil {
			managedOperationUnavailable(w, "recover-pending", err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/query", func(w http.ResponseWriter, r *http.Request) {
		var req managedsql.QueryRequest
		if !decode(w, r, &req) {
			return
		}
		result, err := host.Query(r.Context(), req)
		if err != nil {
			managedOperationUnavailable(w, "query", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	})
	mux.HandleFunc("/leave-voter", func(w http.ResponseWriter, r *http.Request) {
		var req managedsql.LeaveVoterRequest
		if !decode(w, r, &req) {
			return
		}
		if err := host.LeaveVoter(r.Context(), req); err != nil {
			managedOperationUnavailable(w, "leave-voter", err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := r.Header.Get("Authorization")
		want := "Bearer " + token
		if !strings.HasPrefix(got, "Bearer ") || subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		mux.ServeHTTP(w, r)
	}), nil
}
