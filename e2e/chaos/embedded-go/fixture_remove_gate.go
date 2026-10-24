package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"io"
	"net/http"
	"sort"
	"sync"
)

const fixtureRemoveGateSourceEnv = "RHIZA_QUALIFICATION_REMOVE_GATE_SOURCE"
const fixtureRemoveGateMaxBody = 64 << 10

type fixtureRemoveGateSnapshot struct {
	Enabled             bool     `json:"enabled"`
	SourceClusterID     string   `json:"source_cluster_id,omitempty"`
	CurrentClusterID    string   `json:"current_cluster_id,omitempty"`
	LocalNodeID         string   `json:"local_node_id,omitempty"`
	ConfiguredVoters    []string `json:"configured_voters,omitempty"`
	SuccessfulRemoves   uint64   `json:"successful_removes"`
	BlockedSuccesses    uint64   `json:"blocked_successes"`
	LastRequestSHA256   string   `json:"last_request_sha256,omitempty"`
	LastResponseSHA256  string   `json:"last_response_sha256,omitempty"`
	LastHandlerStatus   int      `json:"last_handler_status,omitempty"`
	UnexpectedAdditions uint64   `json:"unexpected_additions"`
	CaptureFailures     uint64   `json:"capture_failures"`
	IdentityViolations  uint64   `json:"identity_violations"`
}

type fixtureRemoveGate struct {
	next http.Handler
	mu   sync.Mutex
	snap fixtureRemoveGateSnapshot
}

func newFixtureRemoveGate(source, cluster, node string, voterIDs []string, startupRole fencing.StartupRole, pendingLearnerID string, next http.Handler) (*fixtureRemoveGate, error) {
	if source == "" {
		return nil, nil
	}
	if next == nil || cluster == "" || node == "" {
		return nil, errors.New("fixture REMOVE gate requires a handler and local cluster identity")
	}
	voters := append([]string(nil), voterIDs...)
	sort.Strings(voters)
	if source != cluster {
		return &fixtureRemoveGate{next: next, snap: fixtureRemoveGateSnapshot{
			SourceClusterID: source, CurrentClusterID: cluster, LocalNodeID: node, ConfiguredVoters: voters,
		}}, nil
	}
	if startupRole == fencing.StartupRoleLearnerPendingRemove {
		if pendingLearnerID == "" || pendingLearnerID != node || containsString(voters, node) {
			return nil, errors.New("fixture REMOVE gate pending learner identity differs from its authenticated startup assignment")
		}
		return &fixtureRemoveGate{next: next, snap: fixtureRemoveGateSnapshot{
			SourceClusterID: source, CurrentClusterID: cluster, LocalNodeID: node, ConfiguredVoters: voters,
		}}, nil
	}
	if startupRole != fencing.StartupRoleVoterActive {
		return nil, errors.New("fixture REMOVE gate requires an authenticated startup role")
	}
	if len(voters) != 3 {
		return nil, errors.New("fixture REMOVE gate requires the exact source and three configured voter endpoints")
	}
	for i, voter := range voters {
		if voter == "" || (i > 0 && voters[i-1] == voter) {
			return nil, errors.New("fixture REMOVE gate voter endpoints must be unique")
		}
	}
	if !containsString(voters, node) {
		return nil, errors.New("fixture REMOVE gate local node is outside the original voter set")
	}
	return &fixtureRemoveGate{next: next, snap: fixtureRemoveGateSnapshot{
		Enabled: true, SourceClusterID: source, CurrentClusterID: cluster, LocalNodeID: node, ConfiguredVoters: voters,
	}}, nil
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func (g *fixtureRemoveGate) snapshot() fixtureRemoveGateSnapshot {
	g.mu.Lock()
	defer g.mu.Unlock()
	snapshot := g.snap
	snapshot.ConfiguredVoters = append([]string(nil), g.snap.ConfiguredVoters...)
	return snapshot
}

func (g *fixtureRemoveGate) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !g.snap.Enabled || r.Method != http.MethodPost || r.URL.Path != "/membership/change" {
		g.next.ServeHTTP(w, r)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, fixtureRemoveGateMaxBody+1))
	if err != nil || len(body) > fixtureRemoveGateMaxBody {
		g.mu.Lock()
		incrementFixtureGateCounter(&g.snap.CaptureFailures)
		g.mu.Unlock()
		http.Error(w, "fixture membership request exceeds limit", http.StatusRequestEntityTooLarge)
		return
	}
	var request struct {
		ClusterID string          `json:"cluster_id"`
		Remove    string          `json:"remove"`
		Add       json.RawMessage `json:"add"`
	}
	if err := json.Unmarshal(body, &request); err != nil {
		g.mu.Lock()
		incrementFixtureGateCounter(&g.snap.CaptureFailures)
		g.mu.Unlock()
		http.Error(w, "fixture REMOVE gate rejected malformed membership request", http.StatusServiceUnavailable)
		return
	}
	if request.ClusterID != g.snap.SourceClusterID {
		g.mu.Lock()
		incrementFixtureGateCounter(&g.snap.IdentityViolations)
		g.mu.Unlock()
		http.Error(w, "fixture REMOVE gate rejected mismatched source identity", http.StatusServiceUnavailable)
		return
	}
	if len(request.Add) != 0 && !bytes.Equal(bytes.TrimSpace(request.Add), []byte("null")) {
		g.mu.Lock()
		incrementFixtureGateCounter(&g.snap.UnexpectedAdditions)
		g.mu.Unlock()
		http.Error(w, "fixture REMOVE gate rejected unexpected ADD", http.StatusServiceUnavailable)
		return
	}
	if request.Remove == "" {
		g.mu.Lock()
		incrementFixtureGateCounter(&g.snap.CaptureFailures)
		g.mu.Unlock()
		http.Error(w, "fixture REMOVE gate rejected an unexpected membership change", http.StatusServiceUnavailable)
		return
	}

	capture := newFixtureRemoveGateResponseWriter(fixtureRemoveGateMaxBody)
	r.Body = io.NopCloser(bytes.NewReader(body))
	g.next.ServeHTTP(capture, r)
	status := capture.statusCode()
	digest := sha256.Sum256(body)
	responseDigest := ""
	if !capture.overrun {
		responseDigestBytes := sha256.Sum256(capture.body.Bytes())
		responseDigest = hex.EncodeToString(responseDigestBytes[:])
	}
	g.mu.Lock()
	g.snap.LastHandlerStatus = status
	g.snap.LastRequestSHA256 = hex.EncodeToString(digest[:])
	g.snap.LastResponseSHA256 = responseDigest
	if capture.overrun {
		incrementFixtureGateCounter(&g.snap.CaptureFailures)
	}
	if status == http.StatusOK && !capture.overrun {
		incrementFixtureGateCounter(&g.snap.SuccessfulRemoves)
		incrementFixtureGateCounter(&g.snap.BlockedSuccesses)
	}
	g.mu.Unlock()
	if status == http.StatusOK {
		http.Error(w, "fixture gate withheld successful REMOVE response", http.StatusServiceUnavailable)
		return
	}
	if err := capture.validate(); err != nil {
		http.Error(w, "fixture gate could not safely capture handler response", http.StatusServiceUnavailable)
		return
	}
	capture.copyTo(w)
}

// Counters are telemetry only; saturate rather than wrap so long-lived fixture
// processes cannot turn repeated violations into an apparently clean zero.
func incrementFixtureGateCounter(counter *uint64) {
	if *counter < ^uint64(0) {
		*counter++
	}
}

type fixtureRemoveGateResponseWriter struct {
	header  http.Header
	status  int
	body    bytes.Buffer
	limit   int
	overrun bool
}

func newFixtureRemoveGateResponseWriter(limit int) *fixtureRemoveGateResponseWriter {
	return &fixtureRemoveGateResponseWriter{header: make(http.Header), limit: limit}
}

func (w *fixtureRemoveGateResponseWriter) Header() http.Header { return w.header }

func (w *fixtureRemoveGateResponseWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

func (w *fixtureRemoveGateResponseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	remaining := w.limit - w.body.Len()
	if remaining > 0 {
		_, _ = w.body.Write(p[:min(len(p), remaining)])
	}
	if len(p) > remaining {
		w.overrun = true
	}
	return len(p), nil
}

func (w *fixtureRemoveGateResponseWriter) statusCode() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

func (w *fixtureRemoveGateResponseWriter) copyTo(dst http.ResponseWriter) {
	for key, values := range w.header {
		for _, value := range values {
			dst.Header().Add(key, value)
		}
	}
	dst.WriteHeader(w.statusCode())
	_, _ = dst.Write(w.body.Bytes())
}

func (w *fixtureRemoveGateResponseWriter) validate() error {
	if w.overrun {
		return errors.New("fixture REMOVE gate response exceeds buffer limit")
	}
	return nil
}
