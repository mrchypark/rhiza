package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

func newSourceFixtureRemoveGate(t *testing.T, source, cluster, node string, voters []string, next http.Handler) (*fixtureRemoveGate, error) {
	t.Helper()
	return newFixtureRemoveGate(source, cluster, node, voters, fencing.StartupRoleVoterActive, "", next)
}

func TestFixtureRemoveGateWithholdsOnlySuccessfulSourceRemove(t *testing.T) {
	requestBody := []byte(`{"operation_id":"remove-op","cluster_id":"source","expected_config_id":4,"remove":"n3"}`)
	called := 0
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called++
		body, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Equal(body, requestBody) {
			t.Errorf("operator did not receive the original request: %q, %v", body, err)
		}
		w.Header().Set("X-Operator-Result", "real")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"status":"completed"}`)
	})
	gate, err := newSourceFixtureRemoveGate(t, "source", "source", "n1", []string{"n1", "n2", "n3"}, upstream)
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	gate.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/membership/change", bytes.NewReader(requestBody)))
	if called != 1 || recorder.Code != http.StatusServiceUnavailable || recorder.Header().Get("X-Operator-Result") != "" || bytes.Contains(recorder.Body.Bytes(), []byte("completed")) {
		t.Fatalf("successful REMOVE response escaped fixture gate: called=%d response=%d %v %s", called, recorder.Code, recorder.Header(), recorder.Body.String())
	}
	digest := sha256.Sum256(requestBody)
	responseDigest := sha256.Sum256([]byte(`{"status":"completed"}`))
	snapshot := gate.snapshot()
	if snapshot.SuccessfulRemoves != 1 || snapshot.BlockedSuccesses != 1 || snapshot.LastHandlerStatus != http.StatusOK ||
		snapshot.LastRequestSHA256 != hex.EncodeToString(digest[:]) || snapshot.LastResponseSHA256 != hex.EncodeToString(responseDigest[:]) || snapshot.UnexpectedAdditions != 0 {
		t.Fatalf("fixture did not record the actual successful REMOVE: %+v", snapshot)
	}
}

func TestFixtureRemoveGateBoundsBufferedHandlerResponse(t *testing.T) {
	gate, err := newSourceFixtureRemoveGate(t, "source", "source", "n1", []string{"n1", "n2", "n3"}, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = io.WriteString(w, strings.Repeat("x", fixtureRemoveGateMaxBody+1))
	}))
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	gate.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/membership/change", bytes.NewBufferString(`{"cluster_id":"source","remove":"n3"}`)))
	if recorder.Code != http.StatusServiceUnavailable || len(recorder.Body.Bytes()) >= fixtureRemoveGateMaxBody || gate.snapshot().CaptureFailures != 1 {
		t.Fatalf("oversized response was not bounded and made observable: status=%d bytes=%d telemetry=%+v", recorder.Code, recorder.Body.Len(), gate.snapshot())
	}
}

func TestFixtureRemoveGateForwardsRealFailureAndUnrelatedRequests(t *testing.T) {
	calls := 0
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("X-Operator-Result", "forwarded")
		if r.URL.Path == "/membership/change" {
			w.WriteHeader(http.StatusConflict)
			_, _ = io.WriteString(w, "actual conflict")
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "health")
	})
	gate, err := newSourceFixtureRemoveGate(t, "source", "source", "n1", []string{"n1", "n2", "n3"}, upstream)
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range []struct {
		method string
		path   string
		body   string
		code   int
		text   string
	}{
		{method: http.MethodPost, path: "/membership/change", body: `{"cluster_id":"source","remove":"n3"}`, code: http.StatusConflict, text: "actual conflict"},
		{method: http.MethodGet, path: "/membership/status", code: http.StatusOK, text: "health"},
	} {
		recorder := httptest.NewRecorder()
		gate.ServeHTTP(recorder, httptest.NewRequest(request.method, request.path, bytes.NewBufferString(request.body)))
		if recorder.Code != request.code || recorder.Body.String() != request.text || recorder.Header().Get("X-Operator-Result") != "forwarded" {
			t.Fatalf("real failure or unrelated request changed: %s got %d %s", request.path, recorder.Code, recorder.Body.String())
		}
	}
	if calls != 2 || gate.snapshot().SuccessfulRemoves != 0 {
		t.Fatalf("unexpected requests counted as gated success: calls=%d snapshot=%+v", calls, gate.snapshot())
	}
}

func TestFixtureRemoveGateFailsClosedForMalformedAndMismatchedSourceEvenWhenUpstreamWouldSucceed(t *testing.T) {
	calls := 0
	gate, err := newSourceFixtureRemoveGate(t, "source", "source", "n1", []string{"n1", "n2", "n3"}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"status":"completed"}`)
	}))
	if err != nil {
		t.Fatal(err)
	}
	malformed := httptest.NewRecorder()
	gate.ServeHTTP(malformed, httptest.NewRequest(http.MethodPost, "/membership/change", bytes.NewBufferString(`{"cluster_id":`)))
	mismatch := httptest.NewRecorder()
	gate.ServeHTTP(mismatch, httptest.NewRequest(http.MethodPost, "/membership/change", bytes.NewBufferString(`{"cluster_id":"successor","remove":"n3"}`)))
	oversized := httptest.NewRecorder()
	gate.ServeHTTP(oversized, httptest.NewRequest(http.MethodPost, "/membership/change", strings.NewReader(strings.Repeat("x", fixtureRemoveGateMaxBody+1))))
	snapshot := gate.snapshot()
	if calls != 0 || malformed.Code != http.StatusServiceUnavailable || mismatch.Code != http.StatusServiceUnavailable || oversized.Code != http.StatusRequestEntityTooLarge ||
		strings.Contains(malformed.Body.String(), "completed") || strings.Contains(mismatch.Body.String(), "completed") || strings.Contains(oversized.Body.String(), "completed") ||
		snapshot.SuccessfulRemoves != 0 || snapshot.BlockedSuccesses != 0 || snapshot.CaptureFailures != 2 || snapshot.IdentityViolations != 1 {
		t.Fatalf("invalid or mismatched requests escaped the active source gate: calls=%d malformed=%d mismatch=%d oversized=%d telemetry=%+v", calls, malformed.Code, mismatch.Code, oversized.Code, snapshot)
	}
}

func TestFixtureRemoveGateDisabledOnSuccessorForwardsMalformedAndMismatchedRequests(t *testing.T) {
	calls := 0
	wantBody := ""
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		body, _ := io.ReadAll(r.Body)
		if r.URL.Path == "/membership/change" && !bytes.Equal(body, []byte(wantBody)) {
			t.Errorf("successor request body changed: %q", body)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"status":"completed"}`)
	})
	gate, err := newFixtureRemoveGate("source", "successor", "n1", []string{"n1", "n2", "n3"}, fencing.StartupRoleVoterActive, "", next)
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{`{"cluster_id":`, `{"cluster_id":"source","remove":"n3"}`} {
		wantBody = body
		recorder := httptest.NewRecorder()
		gate.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/membership/change", strings.NewReader(body)))
		if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "completed") {
			t.Fatalf("disabled successor gate did not transparently forward: body=%q status=%d response=%s", body, recorder.Code, recorder.Body.String())
		}
	}
	if calls != 2 || gate.snapshot().Enabled || gate.snapshot().CaptureFailures != 0 || gate.snapshot().IdentityViolations != 0 {
		t.Fatalf("successor requests were captured by original-source gate: calls=%d telemetry=%+v", calls, gate.snapshot())
	}
}

func TestFixtureRemoveGateBlocksAndRecordsUnexpectedSourceAdd(t *testing.T) {
	calls := 0
	gate, err := newSourceFixtureRemoveGate(t, "source", "source", "n1", []string{"n1", "n2", "n3"}, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		calls++
	}))
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	gate.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/membership/change", bytes.NewBufferString(`{"cluster_id":"source","remove":"n3","add":{"id":"learner"}}`)))
	snapshot := gate.snapshot()
	if calls != 0 || recorder.Code != http.StatusServiceUnavailable || snapshot.UnexpectedAdditions != 1 || snapshot.SuccessfulRemoves != 0 {
		t.Fatalf("unexpected ADD was not blocked and made observable as a run failure: calls=%d status=%d telemetry=%+v", calls, recorder.Code, snapshot)
	}
}

func TestFixtureRemoveGatePassesAuthenticatedPendingLearnerManagementAndHealth(t *testing.T) {
	t.Setenv("RHIZA_STARTUP_ROLE", "learner-pending-remove")
	t.Setenv("RHIZA_NODE_ID", "assigned-learner")
	t.Setenv("RHIZA_LEARNER", `{"node_id":"assigned-learner"}`)
	calls := 0
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusOK)
		if r.URL.Path == "/membership/status" {
			_, _ = io.WriteString(w, `{"cluster_id":"source","members":["n1","n2"]}`)
			return
		}
		_, _ = io.WriteString(w, `{"ready":true}`)
	})
	gate, err := newFixtureRemoveGate("source", "source", "assigned-learner", []string{"n1", "n2"}, fixtureSupervisorStartupRole(), "assigned-learner", upstream)
	if err != nil {
		t.Fatalf("authenticated pending learner could not initialize management server: %v", err)
	}
	server := httptest.NewServer(gate)
	defer server.Close()
	for _, path := range []string{"/membership/status", "/healthz"} {
		response, err := server.Client().Get(server.URL + path)
		if err != nil {
			t.Fatalf("pending learner management server request %s failed: %v", path, err)
		}
		body, readErr := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if readErr != nil || response.StatusCode != http.StatusOK {
			t.Fatalf("pending learner management request %s failed: status=%d body=%s err=%v", path, response.StatusCode, body, readErr)
		}
	}
	if calls != 2 || gate.snapshot().Enabled || gate.snapshot().SuccessfulRemoves != 0 {
		t.Fatalf("pending learner was not a transparent, ungated source handler: calls=%d telemetry=%+v", calls, gate.snapshot())
	}
}

func TestNewFixtureRemoveGatePinsSourceAndAllThreeVoterIdentities(t *testing.T) {
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	for _, tc := range []struct {
		name      string
		source    string
		cluster   string
		node      string
		voters    []string
		role      fencing.StartupRole
		learnerID string
		wantErr   bool
	}{
		{name: "wrong source disables on successor", source: "original", cluster: "successor", node: "n1", voters: []string{"n1", "n2", "n3"}, role: fencing.StartupRoleVoterActive},
		{name: "missing endpoint", source: "source", cluster: "source", node: "n1", voters: []string{"n1", "n2"}, role: fencing.StartupRoleVoterActive, wantErr: true},
		{name: "duplicate endpoint", source: "source", cluster: "source", node: "n1", voters: []string{"n1", "n1", "n3"}, role: fencing.StartupRoleVoterActive, wantErr: true},
		{name: "local endpoint absent", source: "source", cluster: "source", node: "n4", voters: []string{"n1", "n2", "n3"}, role: fencing.StartupRoleVoterActive, wantErr: true},
		{name: "unknown startup role", source: "source", cluster: "source", node: "n1", voters: []string{"n1", "n2", "n3"}, role: "unknown", wantErr: true},
		{name: "pending assignment mismatch", source: "source", cluster: "source", node: "assigned-learner", voters: []string{"n1", "n2"}, role: fencing.StartupRoleLearnerPendingRemove, learnerID: "other-learner", wantErr: true},
		{name: "pending logical ID collides with voter", source: "source", cluster: "source", node: "n1", voters: []string{"n1", "n2"}, role: fencing.StartupRoleLearnerPendingRemove, learnerID: "n1", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gate, err := newFixtureRemoveGate(tc.source, tc.cluster, tc.node, tc.voters, tc.role, tc.learnerID, next)
			if (err != nil) != tc.wantErr {
				t.Fatalf("newFixtureRemoveGate() error = %v, wantErr=%t", err, tc.wantErr)
			}
			if tc.name == "wrong source disables on successor" && (gate == nil || gate.snapshot().Enabled || gate.snapshot().SourceClusterID != tc.source || gate.snapshot().CurrentClusterID != tc.cluster) {
				t.Fatalf("successor did not expose an explicitly disabled source gate: %+v", gate)
			}
		})
	}
}
