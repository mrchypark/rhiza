package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/managedsql"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
)

func managedConfig(ctx context.Context, kube *operator.Kubernetes, ref operator.V1Alpha2ClusterRef, podName, podUID, identityPath, handoffPath string, profile operator.V1Alpha2RuntimeProfile) (managedsql.Config, error) {
	var result managedsql.Config
	plan, err := checkGate(ctx, kube, ref, 0, podName, podUID, identityPath)
	if err != nil {
		return result, err
	}
	f, err := os.Open(handoffPath)
	if err != nil {
		return result, errors.New("invalid public handoff")
	}
	data, readErr := io.ReadAll(io.LimitReader(f, 65537))
	err = errors.Join(readErr, f.Close())
	if err != nil || len(data) > 65536 {
		return result, errors.New("invalid public handoff")
	}
	var handoff operator.V1Alpha2BootstrapPlan
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if d.Decode(&handoff) != nil || d.Decode(new(any)) != io.EOF || !reflect.DeepEqual(handoff, plan) {
		return result, errors.New("public handoff differs from live plan")
	}
	store := &operator.V1Alpha2StatusStore{Kube: kube}
	init, err := operator.LoadV1Alpha2ApplicationInit(ctx, store, ref)
	if err != nil {
		return result, err
	}
	if init.Assignment().Plan.EvidenceFormat != profile.EvidenceFormat || profile.EvidenceFormat != managedsql.EvidenceFormat {
		return result, errors.New("unsupported evidence profile")
	}
	cluster, err := types.ManagedClusterID(ref.UID, 0)
	if err != nil {
		return result, err
	}
	id, err := types.ManagedNodeID(podUID)
	if err != nil {
		return result, err
	}
	secret, err := readIdentityFile(identityPath, string(cluster), string(id))
	if err != nil {
		return result, err
	}
	root := profile.Storage.ObjStorePrefix
	namespace, err := node.ManagedNamespaceFor(root, ref.UID, 0, init.Assignment().Plan.AssignmentID, string(cluster))
	if err != nil {
		return result, err
	}
	cfg := node.CloneManagedRuntimeConfig(profile.Storage)
	cfg.ClusterID = cluster
	cfg.NodeID = id
	cfg.PeerToken = secret
	cfg.DataDir = "/var/lib/rhiza/data"
	cfg.ObjStorePrefix = namespace.ConfigPrefix
	cfg.BindAddr = ""
	cfg.PeerAddr = net.JoinHostPort("0.0.0.0", fmt.Sprint(profile.PeerPort))
	cfg.Members = nil
	for _, m := range plan.Members {
		cfg.Members = append(cfg.Members, m.Member)
		if m.PodUID == podUID {
			u, e := url.Parse(m.Member.PeerURL)
			if e != nil {
				return result, e
			}
			_, port, e := net.SplitHostPort(u.Host)
			if e != nil || port != fmt.Sprint(profile.PeerPort) {
				return result, errors.New("peer port mismatch")
			}
			cfg.PeerAddr = u.Host
		}
	}
	if cfg.AdminToken == "" || cfg.AdminToken == secret {
		return result, errors.New("private API authentication token required")
	}
	return managedsql.Config{StorageRoot: root, Runtime: cfg, Store: store, Ref: ref, Plan: init.Assignment().Plan, PodUID: podUID}, nil
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
			http.Error(w, "operation unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	})
	mux.HandleFunc("/query", func(w http.ResponseWriter, r *http.Request) {
		var req managedsql.QueryRequest
		if !decode(w, r, &req) {
			return
		}
		result, err := host.Query(r.Context(), req)
		if err != nil {
			http.Error(w, "operation unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
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
