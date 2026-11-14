package managedbootstrap

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/mrchypark/rhiza/pkg/managedsql"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func ManagedOperationUnavailable(w http.ResponseWriter, operation string, err error) {
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
func ManagedHandler(host *managedsql.Host, token string) (http.Handler, error) {
	return ManagedHandlerWithReadiness(host, token, nil)
}

// ManagedHandlerWithReadiness also checks application readiness on /readyz.
func ManagedHandlerWithReadiness(host *managedsql.Host, token string, applicationReady func(context.Context) error) (http.Handler, error) {
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
		if applicationReady != nil && applicationReady(r.Context()) != nil {
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
			ManagedOperationUnavailable(w, "execute", err)
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
			ManagedOperationUnavailable(w, "recover-pending", err)
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
			ManagedOperationUnavailable(w, "query", err)
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
			ManagedOperationUnavailable(w, "leave-voter", err)
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
