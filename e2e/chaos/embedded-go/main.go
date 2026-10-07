// Test-only application: SQL uses the embedded API, never db.Handler().
// Supports RHIZA_RECOVERY_ANCHOR_ID for external CAS anchor verification.
// Guard rereads every write, reserves pending before db.Execute, clears after.
package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"regexp"
	"time"

	"github.com/mrchypark/rhiza"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/recoveryanchor"
	"github.com/quic-go/quic-go"
)

var errAnchorBlocked = errors.New("anchor binding not verified")

func main() {
	if len(os.Args) > 1 && os.Args[1] == "qualification-quic-probe" {
		if len(os.Args) == 3 && os.Args[2] == "hold" {
			time.Sleep(20 * time.Minute)
			return
		}
		if len(os.Args) != 5 || (os.Args[2] != "connected" && os.Args[2] != "blocked") || !regexp.MustCompile(`^rhiza-voter-[012]$`).MatchString(os.Args[3]) {
			log.Fatal("invalid QUIC probe arguments")
		}
		host, port, err := net.SplitHostPort(os.Args[4])
		if err != nil || net.ParseIP(host) == nil || port != "9090" {
			log.Fatal("literal voter IP and peer port required")
		}
		config, err := rhiza.ConfigFromEnv()
		if err != nil {
			log.Fatal("probe configuration invalid")
		}
		var target rhiza.Member
		for _, member := range config.Members {
			if string(member.ID) == os.Args[3] {
				target = member
			}
		}
		if target.PublicKey == (rhiza.PublicKey{}) {
			log.Fatal("probe peer identity absent")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		err = probeQUICHandshake(ctx, os.Args[4], string(target.ID), target.PublicKey)
		if os.Args[2] == "connected" && err == nil {
			fmt.Println("QUIC pinned handshake verified; no application stream")
			return
		}
		if os.Args[2] == "blocked" && errors.Is(err, context.DeadlineExceeded) {
			fmt.Println("QUIC context deadline verified")
			return
		}
		log.Fatal("QUIC probe expectation failed")
	}
	if len(os.Args) > 1 && os.Args[1] == "qualification-fixture" {
		if len(os.Args) != 5 {
			log.Fatal("invalid qualification fixture arguments")
		}
		fixture, err := qualificationFixture(os.Args[2], os.Args[3], os.Args[4], rand.Reader)
		if err != nil {
			log.Fatal("qualification fixture generation failed")
		}
		// Only the trusted bootstrap may redirect this credential-bearing output.
		if json.NewEncoder(os.Stdout).Encode(fixture) != nil {
			log.Fatal("qualification fixture output failed")
		}
		return
	}
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
	// Test-harness telemetry only; never expose Config or credentials.
	app.Handle("GET /qualification/object-store", objectStoreCounters(db.ObjectStoreStats))
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

// Same TLS13/ALPN/public-key pin as network.Transport. No client credential,
// application stream, database Open, RPC, or object-store access is involved.
func probeQUICHandshake(ctx context.Context, address, nodeID string, expected rhiza.PublicKey) error {
	conn, err := quic.DialAddr(ctx, address, &tls.Config{
		MinVersion: tls.VersionTLS13, NextProtos: []string{sqlpolicy.PeerALPN}, ServerName: nodeID,
		InsecureSkipVerify: true, // Exact peer public-key pin verified below, as in Transport.
		VerifyConnection: func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) != 1 {
				return errors.New("probe certificate count mismatch")
			}
			key, ok := state.PeerCertificates[0].PublicKey.(ed25519.PublicKey)
			if !ok || subtle.ConstantTimeCompare(key, expected[:]) != 1 {
				return errors.New("probe certificate identity mismatch")
			}
			return nil
		},
	}, &quic.Config{HandshakeIdleTimeout: 5 * time.Second, MaxIdleTimeout: 5 * time.Second, MaxIncomingStreams: -1, MaxIncomingUniStreams: -1})
	if err != nil {
		return err
	}
	return conn.CloseWithError(0, "qualification handshake only")
}

// qualificationFixture is test-host-only: no node opens and no remote calls.
func qualificationFixture(namespace, run, owner string, entropy io.Reader) (map[string]any, error) {
	if !regexp.MustCompile(`^[a-f0-9]{8}$`).MatchString(run) || namespace != "rhiza-v0190-20261008-"+run ||
		!regexp.MustCompile(`^rhiza-postrelease-`+run+`-[a-f0-9]{40}$`).MatchString(owner) {
		return nil, errors.New("invalid fixture identity")
	}
	tokens := make([]string, 5)
	seen := make(map[string]bool, 5)
	for i := range tokens {
		var secret [32]byte
		if _, err := io.ReadFull(entropy, secret[:]); err != nil {
			return nil, errors.New("fixture entropy unavailable")
		}
		tokens[i] = hex.EncodeToString(secret[:])
		if seen[tokens[i]] {
			return nil, errors.New("fixture entropy repeated")
		}
		seen[tokens[i]] = true
	}
	peers := make(map[string]string, 3)
	members := make([]rhiza.Member, 3)
	for i := range members {
		id := fmt.Sprintf("rhiza-voter-%d", i)
		peers[id] = tokens[i+1]
		members[i] = rhiza.Member{ID: rhiza.NodeID(id), URL: "http://" + id + ".rhiza-peers:8080",
			PeerURL: "quic://" + id + ".rhiza-peers:9090", PublicKey: rhiza.PeerPublicKey("gcs-"+run, id, tokens[i+1])}
	}
	learner := rhiza.Member{ID: "rhiza-learner", URL: "http://rhiza-learner:8080", PeerURL: "quic://rhiza-learner:9090",
		PublicKey: rhiza.PeerPublicKey("gcs-"+run, "rhiza-learner", tokens[4])}
	peerJSON, err := json.Marshal(peers)
	if err != nil {
		return nil, err
	}
	memberJSON, err := json.Marshal(members)
	if err != nil {
		return nil, err
	}
	learnerJSON, err := json.Marshal(learner)
	if err != nil {
		return nil, err
	}
	return map[string]any{"apiVersion": "v1", "kind": "Secret", "type": "Opaque", "immutable": true,
		"metadata": map[string]any{"name": "rhiza-qualification-auth", "namespace": namespace,
			"labels": map[string]string{"chaos.rhiza.io/run": run}, "annotations": map[string]string{"rhiza.dev/auth-owner": owner}},
		"stringData": map[string]string{"RHIZA_ADMIN_TOKEN": tokens[0], "RHIZA_PEER_TOKENS": string(peerJSON),
			"RHIZA_CLUSTER_MEMBERS": string(memberJSON), "learner_token": tokens[4], "learner_member": string(learnerJSON)}}, nil
}

func objectStoreCounters(read func() (rhiza.ObjectStoreStats, bool)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stats, ok := read()
		if !ok {
			http.Error(w, "object store unavailable", http.StatusServiceUnavailable)
			return
		}
		// Deliberate allowlist: adding a field to the engine's Stats does not
		// automatically change this test endpoint's disclosure boundary.
		respond(w, map[string]uint64{
			"uploads": stats.Uploads, "gets": stats.Gets, "lists": stats.Lists,
			"heads": stats.Heads, "deletes": stats.Deletes, "failures": stats.Failures,
			"bytes_uploaded": stats.BytesUploaded, "bytes_published": stats.BytesPublished,
			"bytes_downloaded": stats.BytesDownloaded, "http_requests": stats.HTTPRequests,
			"http_request_body_bytes":  stats.HTTPRequestBodyBytes,
			"http_response_body_bytes": stats.HTTPResponseBodyBytes,
			"http_failures":            stats.HTTPFailures, "condition_conflicts": stats.ConditionConflicts,
			"sdk_retries": stats.SDKRetries, "retry_metadata_unknown_requests": stats.RetryMetadataUnknownRequests,
		}, nil)
	})
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
