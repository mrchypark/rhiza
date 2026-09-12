// Test-only application: SQL uses the embedded API, never db.Handler().
// Supports RHIZA_RECOVERY_ANCHOR_ID for external CAS anchor verification.
// Guard rereads every write, reserves pending before db.Execute, clears after.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/mrchypark/rhiza"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/recoveryanchor"
)

var errAnchorBlocked = errors.New("anchor binding not verified")

func main() {
	config, err := rhiza.ConfigFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	db, err := rhiza.Open(context.Background(), config)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	anchorID := os.Getenv("RHIZA_RECOVERY_ANCHOR_ID")
	var guard *anchorGuard
	if anchorID != "" {
		ns := os.Getenv("RHIZA_NAMESPACE")
		if ns == "" {
			log.Fatal("RHIZA_NAMESPACE required with RHIZA_RECOVERY_ANCHOR_ID")
		}
		kube, err := operator.NewInCluster(ns)
		if err != nil {
			log.Fatalf("kubernetes client: %v", err)
		}
		backend := &operator.KubernetesAnchorBackend{Kube: kube}
		guard = newAnchorGuard(backend, anchorID, config.ClusterID)
		log.Printf("anchor guard active: id=%s cluster=%s", anchorID, config.ClusterID)
	}

	management := &http.Server{Addr: config.BindAddr, Handler: db.OperatorHandler(), ReadHeaderTimeout: 5 * time.Second}
	go func() { log.Fatal(management.ListenAndServe()) }()

	app := http.NewServeMux()
	app.HandleFunc("GET /host", func(w http.ResponseWriter, r *http.Request) {
		respond(w, map[string]string{"host": "go-embedded"}, nil)
	})
	app.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		respond(w, map[string]bool{"alive": true}, nil)
	})

	app.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) {
		if !db.Ready() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		respond(w, map[string]bool{"ready": true}, nil)
	})
	app.HandleFunc("POST /sql/execute", func(w http.ResponseWriter, r *http.Request) {
		var req rhiza.ExecuteRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 65536)).Decode(&req); err != nil {
			http.Error(w, "invalid request", 400)
			return
		}
		if guard != nil {
			snap, err := guard.reserve(r.Context())
			if err != nil {
				respond(w, nil, err)
				return
			}
			result, err := db.Execute(r.Context(), req)
			if err != nil {
				guard.fail(r.Context(), snap)
				respond(w, result, err)
				return
			}
			if clearErr := guard.clear(r.Context(), snap); clearErr != nil {
				respond(w, result, clearErr)
				return
			}
			respond(w, result, nil)
			return
		}

		result, err := db.Execute(r.Context(), req)
		respond(w, result, err)
	})
	app.HandleFunc("POST /sql/query", func(w http.ResponseWriter, r *http.Request) {
		var req rhiza.QueryRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 65536)).Decode(&req); err != nil {
			http.Error(w, "invalid request", 400)
			return
		}
		result, err := db.Query(r.Context(), req)
		respond(w, result, err)
	})
	server := &http.Server{Addr: ":8080", Handler: app, ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(server.ListenAndServe())
}

func respond(w http.ResponseWriter, value any, err error) {
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		value = map[string]string{"error_code": "operation_failed"}
	}
	_ = json.NewEncoder(w).Encode(value)
}

func storageID(provider, endpoint, bucket, prefix, cluster string) string {
	arr := []string{provider, endpoint, bucket, prefix, cluster}
	b, _ := json.Marshal(arr)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

type snap struct {
	binding    recoveryanchor.Binding
	generation uint64
	evidence   []byte
}

type anchorGuard struct {
	backend           recoveryanchor.Backend
	anchorID          string
	clusterID         string
	format            string
	expectedStorageID string
}

func newAnchorGuard(b recoveryanchor.Backend, anchorID, clusterID string) *anchorGuard {
	return &anchorGuard{
		backend:           b,
		anchorID:          anchorID,
		clusterID:         clusterID,
		format:            "sql-epoch-token/v1",
		expectedStorageID: storageID("s3", "rhiza-minio:9000", "rhiza", "rhiza", clusterID),
	}
}

func (g *anchorGuard) reserve(ctx context.Context) (*snap, error) {
	record, version, err := g.backend.Read(ctx, g.anchorID)
	if err != nil {
		return nil, errAnchorBlocked
	}
	if record.Version != recoveryanchor.Version1 {
		return nil, errAnchorBlocked
	}
	if record.EvidenceFormat != g.format {
		return nil, errAnchorBlocked
	}
	if record.PendingWrite {
		return nil, errAnchorBlocked
	}
	if record.Transition != nil {
		return nil, errAnchorBlocked
	}
	if len(record.Evidence) == 0 {
		return nil, errAnchorBlocked
	}
	if record.Binding.ClusterID != g.clusterID {
		return nil, errAnchorBlocked
	}
	if record.Binding.StorageID != g.expectedStorageID {
		return nil, errAnchorBlocked
	}
	s := &snap{
		binding:    record.Binding,
		generation: record.Generation,
		evidence:   make([]byte, len(record.Evidence)),
	}
	copy(s.evidence, record.Evidence)
	if err := recoveryanchor.UpdateApplication(ctx, g.backend, g.anchorID,
		version, s.binding, s.generation, s.evidence, true); err != nil {
		return nil, errAnchorBlocked
	}
	return s, nil
}

// clear re-reads backend, verifies binding+gen+Pending=true+!Transition,
// and evidence bytes must match exactly to avoid overwriting concurrent app evidence.
func (g *anchorGuard) clear(ctx context.Context, s *snap) error {
	record, freshVersion, err := g.backend.Read(ctx, g.anchorID)
	if err != nil {
		return errAnchorBlocked
	}
	if record.Binding != s.binding {
		return errAnchorBlocked
	}
	if record.Generation != s.generation {
		return errAnchorBlocked
	}
	if !record.PendingWrite {
		return errAnchorBlocked
	}
	if record.Transition != nil {
		return errAnchorBlocked
	}
	if !bytes.Equal(record.Evidence, s.evidence) {
		return errAnchorBlocked
	}
	return recoveryanchor.UpdateApplication(ctx, g.backend, g.anchorID,
		freshVersion, s.binding, s.generation, s.evidence, false)
}

func (g *anchorGuard) fail(_ context.Context, _ *snap) {}

