package operator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/thanos-io/objstore"
)

type retirementEscalationFixture struct {
	c                  *Controller
	r                  *ClusterResource
	child              *Resource
	api                *operatorAPI
	learnerPod         object
	live, fencePending bool
	fences             []FenceRequest
	probeHook          func()
}

type retirementRoundTripper func(*http.Request) (*http.Response, error)

func (roundTrip retirementRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}

func newRetirementEscalationFixture(t *testing.T) *retirementEscalationFixture {
	t.Helper()
	ctx, child, api, c := membershipFixture(t)
	child.Spec.Container = "rhiza"
	child.Spec.Membership.OperationID = "recover"
	child.Spec.Membership.RemovedPod = "n3"
	api.learnerID = "learner"
	api.membership["learner"] = network.MembershipStatus{NodeID: "learner", ClusterID: "source", WALIdentity: strings.Repeat("a", 64)}
	api.membershipSecret["data"].(object)["member"] = base64.StdEncoding.EncodeToString(jsonBytes(voterMember("source", "learner", "new-token")))
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	api.membership["n1"] = membershipStatus("n1", 2, "n1", "n2")
	api.membership["n2"] = membershipStatus("n2", 2, "n1", "n2")
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"n1", "n2"} {
		status := api.membership[id]
		status.Pending = true
		api.membership[id] = status
	}
	learnerPod := api.replacement
	api.replacement = nil
	delete(api.membership, "learner")
	r := autoClusterResource("cluster", "binding-uid-1", "logical")
	state := validAutoControl()
	state.StatefulSetUID = "sts-uid"
	state.Voters = child.Spec.Membership.VoterPods
	state.Voters = append(state.Voters, VoterPod{NodeID: "n3", Pod: "n3"})
	r.Spec.VoterCount = len(state.Voters)
	request := FenceRequest{LogicalID: r.Spec.LogicalID, BindingUID: state.BindingUID, Namespace: "default", StatefulSet: "rhiza", StatefulSetUID: state.StatefulSetUID, SourceClusterID: "source", OperationID: "recover", Scope: ScopeVoter, Targets: []FenceTarget{{NodeID: "n3", Pod: "n3", PodUID: "pod-n3", WALIdentity: strings.Repeat("b", 64)}}}
	count := 0
	proof, err := (&countingFencer{count: &count}).Fence(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	proof.ProofID = child.Spec.Membership.Fence.Evidence
	state.Intent = &autoIntent{Request: request, Proof: proof, Failed: "n3", Voters: append([]VoterPod(nil), state.Voters...), Durability: "before-ack", RecoveryName: "recover", RecoveryUID: "resource-uid", LearnerName: "learner", LearnerUID: "learner-uid", LearnerWAL: strings.Repeat("a", 64), LearnerMissingSince: time.Now().Add(-time.Minute), NoQuorumSince: time.Now().Add(-2 * time.Second), Policy: AutoPolicy{FailureGraceSeconds: 15}}
	child.Metadata["annotations"] = object{automaticOwner: state.BindingUID, automaticLogical: r.Spec.LogicalID}
	state.Intent.SpecHash = hashJSON(child.Spec)
	for _, ref := range state.Voters {
		state.Identities[ref.NodeID] = FenceTarget{NodeID: string(ref.NodeID), Pod: ref.Pod, PodUID: "pod-" + string(ref.NodeID), WALIdentity: "wal-" + string(ref.NodeID)}
	}
	f := &retirementEscalationFixture{c: c, r: r, child: child, api: api, learnerPod: learnerPod, live: true, fencePending: true}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		if q.URL.Path == c.resourcePath("recover") {
			if q.Method == http.MethodPut {
				if err := json.NewDecoder(q.Body).Decode(child); err != nil {
					t.Error(err)
				}
			}
			_ = json.NewEncoder(w).Encode(child)
			return
		}
		if q.Method == http.MethodPut && q.URL.Path == c.clusterPath("cluster")+"/status" {
			_ = json.NewEncoder(w).Encode(r)
			return
		}
		api.serve(w, q)
	}))
	t.Cleanup(server.Close)
	c.Kube.BaseURL, c.Kube.Client = server.URL, server.Client()
	c.HTTP = &http.Client{Transport: escalationProbeTransport(func(q *http.Request) (*http.Response, error) {
		if q.URL.Path == "/recovery/probe" {
			if f.probeHook != nil {
				f.probeHook()
			}
			id := "n1"
			if api.learner != nil && q.URL.Host == strings.TrimPrefix(api.learner.URL, "http://") {
				id = "learner"
			} else {
				for i, peer := range api.peers {
					if q.URL.Host == strings.TrimPrefix(peer.URL, "http://") {
						id = []string{"n1", "n2", "n3"}[i]
						break
					}
				}
			}
			api.mu.Lock()
			pending := api.membership[id].Pending
			walIdentity := api.membership[id].WALIdentity
			api.mu.Unlock()
			if id == "learner" {
				if walIdentity == "" {
					walIdentity = strings.Repeat("a", 64)
				}
				return progressProbeResponseForMembership(q, id, f.live && !pending, 3, []string{"n1", "n2", "learner"}, walIdentity), nil
			}
			return progressProbeResponse(q, id, f.live && !pending), nil
		}
		if !f.live && (q.URL.Path == "/membership/abort" || q.URL.Path == "/membership/change") {
			return &http.Response{StatusCode: 503, Body: http.NoBody, Header: make(http.Header)}, nil
		}
		return http.DefaultTransport.RoundTrip(q)
	})}
	c.Fencer = retirementFencer(func(ctx context.Context, request FenceRequest) (*FenceProof, error) {
		f.fences = append(f.fences, request)
		if f.fencePending {
			return nil, ErrFencePending
		}
		return (&countingFencer{count: &count}).Fence(ctx, request)
	})
	if err := c.saveAuto(ctx, r, state, nil); err != nil {
		t.Fatal(err)
	}
	return f
}

func TestAutomaticRetirementEligibilityRecheckedAfterProbeCrossesGrace(t *testing.T) {
	f := newRetirementEscalationFixture(t)
	f.live = false
	fenceGrace := time.Second
	state, version := f.load(t)
	state.Intent.Aborting = false
	state.Intent.RetirementSince = time.Time{}
	state.Intent.LearnerMissingSince = time.Now().Add(-900 * time.Millisecond)
	state.Intent.NoQuorumSince = time.Now().Add(-time.Minute)
	state.Intent.Policy.FailureGraceSeconds = 1
	if err := f.c.saveAuto(t.Context(), f.r, state, version); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	f.probeHook = func() { once.Do(func() { close(entered); <-release }) }
	state, version = f.load(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- f.c.advanceAutomatic(ctx, f.r, state, version, f.api.sts) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatalf("assessment probe did not reach deterministic barrier: %v", ctx.Err())
	}
	// The handler already observed the learner within its missing grace window.
	// Hold the later no-quorum probes until that same captured window expires.
	timer := time.NewTimer(fenceGrace + 100*time.Millisecond)
	select {
	case <-timer.C:
	case <-ctx.Done():
		t.Fatalf("probe barrier exceeded bounded timeout: %v", ctx.Err())
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	state, _ = f.load(t)
	if state.Intent.Request.Scope != ScopeVoter || !state.Intent.Aborting || state.Intent.RetirementSince.IsZero() || state.Intent.LearnerUID != "learner-uid" || state.Intent.LearnerWAL != strings.Repeat("a", 64) || len(f.fences) != 0 {
		t.Fatalf("probe crossed learner retirement eligibility without reserving exact abort: %s", retirementStateDiagnostic(t, f, state))
	}
}

func TestExpiredAbortRevisionGetsExactChildCASAndPutBeforeEscalation(t *testing.T) {
	f := newRetirementEscalationFixture(t)
	f.live = false
	state, version := f.load(t)
	add := f.child.Status.Membership.Add
	if add == nil || add.OperationID == "" || add.ExpectedConfigID == 0 || add.ExpectedAbortSlot != 0 {
		t.Fatalf("fixture lacks exact pending ADD identity: %+v", add)
	}
	state.Intent.Aborting = true
	state.Intent.RetirementSince = time.Now().Add(-time.Minute)
	state.Intent.NoQuorumSince = time.Now().Add(-time.Minute)
	state.Intent.Policy.FailureGraceSeconds = 1
	if err := f.c.saveAuto(t.Context(), f.r, state, version); err != nil {
		t.Fatal(err)
	}
	state = f.advance(t)
	if state.Intent.Request.Scope != ScopeVoter || !state.Intent.Aborting || f.child.Spec.Membership.AbortAddition || state.Intent.PreviousSpecHash == "" || state.Intent.SpecHash == hashJSON(f.child.Spec) {
		t.Fatalf("expired abort did not reserve its exact child revision first: %s", retirementStateDiagnostic(t, f, state))
	}
	state = f.advance(t)
	if state.Intent.Request.Scope != ScopeVoter || !state.Intent.Aborting || !f.child.Spec.Membership.AbortAddition || hashJSON(f.child.Spec) != state.Intent.SpecHash || len(f.fences) != 0 {
		t.Fatalf("expired abort did not submit the reserved exact child revision before generation escalation: %s", retirementStateDiagnostic(t, f, state))
	}
	got := f.child.Status.Membership.Add
	if got == nil || got.OperationID != add.OperationID || got.ExpectedConfigID != add.ExpectedConfigID || got.ExpectedAbortSlot != add.ExpectedAbortSlot {
		t.Fatalf("abort revision changed the pending ADD identity: before=%+v after=%+v", add, got)
	}
}

func TestFailedAbortPUTDoesNotExtendGenerationEscalationForever(t *testing.T) {
	f := newRetirementEscalationFixture(t)
	f.live = false
	state, version := f.load(t)
	state.Intent.Aborting = true
	state.Intent.RetirementSince = time.Now().Add(-time.Minute)
	state.Intent.NoQuorumSince = time.Now().Add(-time.Minute)
	state.Intent.Policy.FailureGraceSeconds = 1
	if err := f.c.saveAuto(t.Context(), f.r, state, version); err != nil {
		t.Fatal(err)
	}
	state = f.advance(t) // Persist the exact SpecHash revision before attempting the child PUT.
	reservedHash := state.Intent.SpecHash
	originalClient := f.c.Kube.Client
	putAttempts := 0
	f.c.Kube.Client = &http.Client{Transport: retirementRoundTripper(func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodPut && request.URL.Path == f.c.resourcePath("recover") {
			putAttempts++
			return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header), Request: request}, nil
		}
		return originalClient.Transport.RoundTrip(request)
	})}
	state, version = f.load(t)
	err := f.c.advanceAutomatic(t.Context(), f.r, state, version, f.api.sts)
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("failed child PUT error=%v, want observable HTTP 503", err)
	}
	state, _ = f.load(t)
	if putAttempts != 1 || state.Intent.Request.Scope != ScopeGeneration || state.Intent.Proof != nil || state.Intent.Supersedes == "" || f.child.Spec.Membership.AbortAddition || reservedHash == "" {
		t.Fatalf("failed exact abort PUT shielded bounded escalation or changed child: attempts=%d reservedHash=%s diagnostic=%s", putAttempts, reservedHash, retirementStateDiagnostic(t, f, state))
	}
}

func retirementStateDiagnostic(t *testing.T, f *retirementEscalationFixture, state *autoControl) string {
	t.Helper()
	current := func(intent *autoIntent) string {
		if intent == nil {
			return "<nil>"
		}
		return fmt.Sprintf("scope=%s op=%s failed=%s aborting=%t proof=%t missing=%s noQuorum=%s retirement=%s supersedes=%s spec=%s previous=%s", intent.Request.Scope, intent.RecoveryName, intent.Failed, intent.Aborting, intent.Proof != nil, intent.LearnerMissingSince.UTC().Format(time.RFC3339Nano), intent.NoQuorumSince.UTC().Format(time.RFC3339Nano), intent.RetirementSince.UTC().Format(time.RFC3339Nano), intent.Supersedes, intent.SpecHash, intent.PreviousSpecHash)
	}
	snapshot := "<none>"
	if state.Intent != nil && state.Intent.Supersedes != "" {
		key := path.Join(f.c.Prefix, "automatic", f.c.Kube.Namespace, f.r.Spec.LogicalID, "superseded", state.Intent.Supersedes+".json")
		reader, err := f.c.Bucket.Get(t.Context(), key)
		if err == nil {
			data, readErr := io.ReadAll(io.LimitReader(reader, 1<<20))
			closeErr := reader.Close()
			var original autoIntent
			if readErr == nil && closeErr == nil && json.Unmarshal(data, &original) == nil {
				snapshot = current(&original)
			}
		}
	}
	return fmt.Sprintf("current={%s} superseded={%s} childUID=%s childAbort=%t", current(state.Intent), snapshot, str(f.child.Metadata["uid"]), f.child.Spec.Membership != nil && f.child.Spec.Membership.AbortAddition)
}

func (f *retirementEscalationFixture) load(t *testing.T) (*autoControl, *objstore.ObjectVersion) {
	t.Helper()
	state, version, err := f.c.loadAuto(t.Context(), f.c.autoKey(f.r))
	if err != nil {
		t.Fatal(err)
	}
	return state, version
}

func (f *retirementEscalationFixture) advance(t *testing.T) *autoControl {
	t.Helper()
	state, version := f.load(t)
	if err := f.c.advanceAutomatic(t.Context(), f.r, state, version, f.api.sts); err != nil {
		t.Fatal(err)
	}
	state, _ = f.load(t)
	return state
}

func TestAutomaticRetirementEscalationPendingAddReachesAbortBeforeFence(t *testing.T) {
	f := newRetirementEscalationFixture(t)
	// The candidate is unreachable to the probe but its frozen Pod UID/IP must
	// remain observable while the test exercises retirement policy.
	f.api.replacement = f.learnerPod
	state := f.advance(t)
	if !state.Intent.Aborting || state.Intent.RetirementSince.IsZero() || len(f.fences) != 0 {
		t.Fatalf("pending Add probe failure starved retirement reservation: %+v fences=%d", state.Intent, len(f.fences))
	}
	retirementSince := state.Intent.RetirementSince
	for range 3 {
		state = f.advance(t)
		if f.child.Spec.Membership.AbortAddition {
			break
		}
	}
	if !f.child.Spec.Membership.AbortAddition || !state.Intent.Aborting {
		t.Fatalf("abort revision was starved by unavailable ReadIndex: %+v child=%+v clusterStatus=%+v", state.Intent, f.child.Spec.Membership, f.r.Status)
	}
	if err := f.c.reconcile(t.Context(), f.child); err != nil {
		t.Fatal(err)
	}
	if f.child.Status.Phase != "Aborted" || len(f.api.aborts) != 1 {
		t.Fatalf("pending round did not abort: %+v", f.child.Status)
	}
	for _, id := range []string{"n1", "n2"} {
		status := f.api.membership[id]
		status.Pending, status.AbortSlot = false, 1
		f.api.membership[id] = status
	}
	state, version := f.load(t)
	state.Intent.RetirementSince = time.Now().Add(-time.Hour)
	retirementSince = state.Intent.RetirementSince
	if err := f.c.saveAuto(t.Context(), f.r, state, version); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		state = f.advance(t)
		if state.Intent == nil || state.Intent.Request.Scope != ScopeVoter || state.Intent.LearnerAttempt != 0 || state.Intent.LearnerUID != "learner-uid" {
			t.Fatalf("slow fence caused false DR or unfenced rotation: %+v", state.Intent)
		}
	}
	if len(f.fences) == 0 {
		t.Fatal("retired learner fence was never requested")
	}
	for _, request := range f.fences {
		if len(request.Targets) != 1 || request.Targets[0].NodeID != "learner" || request.Targets[0].PodUID != "learner-uid" || request.Targets[0].WALIdentity != strings.Repeat("a", 64) {
			t.Fatalf("fence escaped frozen incarnation: %+v", request)
		}
	}
	f.fencePending = false
	state = f.advance(t)
	if state.Intent.LearnerAttempt != 1 || state.Intent.LearnerUID != "" || state.Intent.LearnerName == "learner" || !state.Intent.RetirementSince.Equal(retirementSince) {
		t.Fatalf("verified proof did not rotate once or reset the incident budget: %+v", state.Intent)
	}
}

func TestAutomaticRetirementMissingPendingAddAbortsThenRetries(t *testing.T) {
	f := newRetirementEscalationFixture(t)
	f.live = false
	f.api.replacement = nil // The captured learner UID/WAL remains; its Pod is gone.
	for _, id := range []string{"n1", "n2"} {
		status := f.api.membership[id]
		status.Pending = true
		f.api.membership[id] = status
	}
	containerSpec, err := container(f.api.sts, "rhiza")
	if err != nil {
		t.Fatal(err)
	}
	containerSpec["ports"] = []any{
		object{"name": "http", "containerPort": float64(8080)},
		object{"name": "peer-quic", "containerPort": float64(9090), "protocol": "UDP"},
	}
	state, version := f.load(t)
	if state.Intent.Aborting || state.Intent.LearnerUID != "learner-uid" || state.Intent.LearnerWAL != strings.Repeat("a", 64) || state.Intent.LearnerName != "learner" {
		t.Fatalf("fixture must start with a bound learner and no pre-injected abort: %+v", state.Intent)
	}
	if f.child.Spec.Membership.AbortAddition || f.child.Status.Membership == nil || f.child.Status.Membership.Add == nil {
		t.Fatalf("fixture must start with an outstanding Add request: spec=%+v status=%+v", f.child.Spec.Membership, f.child.Status.Membership)
	}
	add := f.child.Status.Membership.Add
	if add.ExpectedConfigID != 2 || add.ExpectedAbortSlot != 0 || add.OperationID == "" {
		t.Fatalf("fixture must retain the exact pending Add identity: %+v", add)
	}
	state.Intent.LearnerMissingSince = time.Time{}
	state.Intent.RetirementSince = time.Time{}
	state.Intent.NoQuorumSince = time.Time{}
	state.Intent.Policy.FailureGraceSeconds = 1
	if err := f.c.saveAuto(t.Context(), f.r, state, version); err != nil {
		t.Fatal(err)
	}
	f.fencePending = true

	var candidate = "learner"
	var candidatePod, candidateSecret, candidateService object
	podCreates := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case q.URL.Path == f.c.resourcePath("recover") && q.Method == http.MethodGet:
			_ = json.NewEncoder(w).Encode(f.child)
			return
		case q.URL.Path == f.c.resourcePath("recover") && q.Method == http.MethodPut:
			if err := json.NewDecoder(q.Body).Decode(f.child); err != nil {
				t.Errorf("decode recovery update: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(f.child)
			return
		case q.URL.Path == f.c.resourcePath("recover")+"/status" && q.Method == http.MethodPut:
			if err := json.NewDecoder(q.Body).Decode(f.child); err != nil {
				t.Errorf("decode recovery status: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(f.child)
			return
		case q.URL.Path == f.c.clusterPath("cluster")+"/status" && q.Method == http.MethodPut:
			_ = json.NewEncoder(w).Encode(f.r)
			return
		case q.URL.Path == f.c.corePath("pods", candidate) && q.Method == http.MethodGet:
			if candidatePod == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(candidatePod)
			return
		case q.URL.Path == f.c.corePath("pods", "") && q.Method == http.MethodPost:
			if err := json.NewDecoder(q.Body).Decode(&candidatePod); err != nil {
				t.Errorf("decode learner Pod: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			asObject(candidatePod["metadata"])["uid"] = "retry-learner-uid"
			candidatePod["status"] = object{"podIP": "127.0.0.8"}
			podCreates++
			_ = json.NewEncoder(w).Encode(candidatePod)
			return
		case q.URL.Path == f.c.corePath("secrets", candidate+"-credentials") && q.Method == http.MethodGet:
			if candidateSecret == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(candidateSecret)
			return
		case q.URL.Path == f.c.corePath("secrets", "") && q.Method == http.MethodPost:
			if err := json.NewDecoder(q.Body).Decode(&candidateSecret); err != nil {
				t.Errorf("decode learner Secret: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			asObject(candidateSecret["metadata"])["uid"] = "retry-learner-secret-uid"
			_ = json.NewEncoder(w).Encode(candidateSecret)
			return
		case q.URL.Path == f.c.corePath("services", candidate) && q.Method == http.MethodGet:
			if candidateService == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(candidateService)
			return
		case q.URL.Path == f.c.corePath("services", "") && q.Method == http.MethodPost:
			if err := json.NewDecoder(q.Body).Decode(&candidateService); err != nil {
				t.Errorf("decode learner Service: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(candidateService)
			return
		default:
			f.api.serve(w, q)
		}
	}))
	t.Cleanup(server.Close)
	f.c.Kube.BaseURL, f.c.Kube.Client = server.URL, server.Client()

	deadline := time.Now().Add(5 * time.Second)
	for !f.child.Spec.Membership.AbortAddition {
		state = f.advance(t)
		if f.child.Spec.Membership.AbortAddition {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("actual advanceAutomatic never submitted the abort revision: intent=%+v child=%+v cluster=%+v", state.Intent, f.child.Spec.Membership, f.r.Status)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !state.Intent.Aborting || len(f.fences) != 0 || podCreates != 0 || state.Intent.LearnerUID != "learner-uid" || state.Intent.LearnerWAL != strings.Repeat("a", 64) {
		t.Fatalf("abort revision escaped before preserving the captured learner or before its fence: intent=%+v fences=%+v podCreates=%d", state.Intent, f.fences, podCreates)
	}
	childUID := str(f.child.Metadata["uid"])

	// The pending Add cannot be acknowledged while both survivors still report
	// Pending. Once the test authority accepts the same abort, the child records
	// the terminal abort without requiring the deleted candidate Pod.
	if err := f.c.reconcile(t.Context(), f.child); err != nil {
		t.Fatal(err)
	}
	if f.child.Status.Membership.Phase != "Aborting" || len(f.api.aborts) != 0 {
		t.Fatalf("pending Add was treated as aborted without survivor acknowledgement: %+v aborts=%d", f.child.Status.Membership, len(f.api.aborts))
	}
	f.live = true
	// Keep the live survivor quorum's Pending bit set until its authority has
	// accepted the exact abort request. Pending is not evidence that the add
	// was already aborted.
	if err := f.c.reconcile(t.Context(), f.child); err != nil {
		t.Fatal(err)
	}
	if f.child.Status.Membership.Phase != "Aborted" || len(f.api.aborts) != 1 {
		t.Fatalf("survivor authority did not acknowledge the abort: %+v aborts=%d", f.child.Status.Membership, len(f.api.aborts))
	}
	abort := f.api.aborts[0]
	if abort.OperationID != add.OperationID || abort.ClusterID != f.child.Spec.SourceClusterID || abort.ExpectedConfigID != 2 || abort.ExpectedConfigID != add.ExpectedConfigID || abort.ExpectedAbortSlot != 0 || abort.ExpectedAbortSlot != add.ExpectedAbortSlot {
		t.Fatalf("abort did not target the exact pending Add operation: abort=%+v add=%+v", abort, add)
	}
	for _, id := range []string{"n1", "n2"} {
		status := f.api.membership[id]
		if !status.Pending {
			t.Fatalf("fixture lost Pending before accepted abort response for %s: %+v", id, status)
		}
		status.Pending = false
		status.AbortSlot = 1
		f.api.membership[id] = status
	}

	// A missing independent learner fence proof must retain the exact target and
	// must not create a replacement under a new identity.
	state = f.advance(t)
	if !state.Intent.Aborting || state.Intent.LearnerName != "learner" || state.Intent.LearnerUID != "learner-uid" || state.Intent.LearnerWAL != strings.Repeat("a", 64) || len(f.fences) != 1 || podCreates != 0 || str(f.child.Metadata["uid"]) != childUID {
		t.Fatalf("fence-pending state rotated/created a learner or lost its exact target: intent=%+v fences=%+v podCreates=%d childUID=%q", state.Intent, f.fences, podCreates, str(f.child.Metadata["uid"]))
	}
	request := f.fences[0]
	if request.Scope != ScopeVoter || len(request.Targets) != 1 || request.Targets[0] != (FenceTarget{NodeID: "learner", Pod: "learner", PodUID: "learner-uid", WALIdentity: strings.Repeat("a", 64)}) {
		t.Fatalf("retirement fence did not retain the captured UID/WAL: %+v", request)
	}

	f.fencePending = false
	state = f.advance(t)
	if state.Intent.Aborting || state.Intent.LearnerAttempt != 1 || state.Intent.LearnerName == "learner" || state.Intent.LearnerUID != "" || len(state.Learners) != 1 || state.Learners[0].Target.PodUID != "learner-uid" || state.Learners[0].Target.WALIdentity != strings.Repeat("a", 64) {
		t.Fatalf("verified retirement proof did not preserve old history and reserve a fresh learner: intent=%+v learners=%+v", state.Intent, state.Learners)
	}
	candidate = state.Intent.LearnerName
	state = f.advance(t)
	if podCreates != 1 || candidatePod == nil || str(nested(candidatePod, "metadata", "name")) != candidate || state.Intent.LearnerUID != "retry-learner-uid" || state.Intent.LearnerWAL != "" || len(state.Learners) != 2 || state.Learners[0].Target.PodUID != "learner-uid" || state.Learners[1].Target.PodUID != "retry-learner-uid" {
		t.Fatalf("fresh automatic retry did not create/journal a new UID while retaining the retired target: candidate=%q pod=%+v intent=%+v learners=%+v creates=%d", candidate, candidatePod, state.Intent, state.Learners, podCreates)
	}
}

func TestAutomaticRetirementEscalationRealQuorumLossRemainsBounded(t *testing.T) {
	f := newRetirementEscalationFixture(t)
	// Keep the bound candidate Pod observable; this case tests quorum loss, not
	// disappearance or recreation of the candidate incarnation.
	f.api.replacement = f.learnerPod
	f.live = false
	state := f.advance(t)
	if !state.Intent.Aborting {
		t.Fatalf("retirement not reserved: %+v", state.Intent)
	}
	for range 3 {
		state = f.advance(t)
		if f.child.Spec.Membership.AbortAddition {
			break
		}
	}
	if !f.child.Spec.Membership.AbortAddition {
		t.Fatalf("finite retirement window did not submit abort: clusterStatus=%+v", f.r.Status)
	}
	if err := f.c.reconcile(t.Context(), f.child); err != nil {
		t.Fatal(err)
	}
	if f.child.Status.Phase == "Aborted" {
		t.Fatal("unreachable quorum manufactured abort acknowledgment")
	}
	state, version := f.load(t)
	state.Intent.RetirementSince, state.Intent.NoQuorumSince = time.Now().Add(-time.Minute), time.Now().Add(-time.Minute)
	if err := f.c.saveAuto(t.Context(), f.r, state, version); err != nil {
		t.Fatal(err)
	}
	state = f.advance(t)
	if state.Intent.Request.Scope != ScopeGeneration || state.Intent.Proof != nil || state.Intent.Failed != "" || len(state.Intent.Request.Targets) != 4 || state.Intent.Supersedes == "" {
		t.Fatalf("retirement blocked genuine generation escalation: %+v", state.Intent)
	}
	if len(f.fences) != 0 {
		t.Fatal("unproven generation fence was executed before reservation")
	}
}

func TestAutomaticRetirementEscalationChangedLearnerWALHardStops(t *testing.T) {
	f := newRetirementEscalationFixture(t)
	f.live = false
	f.api.replacement = f.learnerPod
	f.api.membership["learner"] = network.MembershipStatus{NodeID: "learner", ClusterID: "source", WALIdentity: strings.Repeat("c", 64)}
	state, version := f.load(t)
	state.Intent.NoQuorumSince = time.Now().Add(-time.Hour)
	if err := f.c.saveAuto(t.Context(), f.r, state, version); err != nil {
		t.Fatal(err)
	}
	before := hashJSON(state)
	state = f.advance(t)
	if hashJSON(state) != before || len(f.fences) != 0 || f.child.Spec.Membership.AbortAddition {
		t.Fatalf("changed learner WAL escaped hard stop: intent=%+v fences=%d", state.Intent, len(f.fences))
	}
}
