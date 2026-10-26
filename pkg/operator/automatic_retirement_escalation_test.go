package operator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
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
}

func newRetirementEscalationFixture(t *testing.T) *retirementEscalationFixture {
	t.Helper()
	ctx, child, api, c := membershipFixture(t)
	child.Spec.Container = "rhiza"
	child.Spec.Membership.OperationID = "recover"
	child.Spec.Membership.RemovedPod = "n3"
	seedLegacyRemoveMembershipJournal(child)
	api.learnerID = "learner"
	api.membership["learner"] = network.MembershipStatus{NodeID: "learner", ClusterID: "source", WALIdentity: strings.Repeat("a", 64)}
	api.membershipSecret["data"].(object)["member"] = base64.StdEncoding.EncodeToString(jsonBytes(voterMember("source", "learner", "new-token")))
	asObject(api.membershipSecret["metadata"])["annotations"] = object{ownerAnnotation: "recover"}
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
	if err := rememberLearner(state, learnerIncarnation{
		SourceClusterID: state.ActiveClusterID,
		OperationID:     state.Intent.Request.OperationID,
		CredentialName:  learnerCredentialName(state.Intent),
		CredentialUID:   str(nested(api.membershipSecret, "metadata", "uid")),
		Target: FenceTarget{
			NodeID: state.Intent.LearnerName, Pod: learnerPhysicalPod(state.Intent),
			PodUID: state.Intent.LearnerUID, WALIdentity: state.Intent.LearnerWAL,
		},
	}); err != nil {
		t.Fatalf("persist production-equivalent standalone learner history: %v", err)
	}
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
			id := "n1"
			if q.URL.Host == strings.TrimPrefix(api.peers[1].URL, "http://") {
				id = "n2"
			}
			api.mu.Lock()
			pending := api.membership[id].Pending
			api.mu.Unlock()
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
		t.Fatalf("abort revision was starved by unavailable ReadIndex: %+v child=%+v", state.Intent, f.child.Spec.Membership)
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

func TestAutomaticRetirementEscalationDoesNotStopOnNextSameOrdinalUID(t *testing.T) {
	f := newRetirementEscalationFixture(t)
	state, version := f.load(t)
	state.Intent.Aborting = true
	state.Intent.RetirementSince = time.Now()
	f.api.replacement = f.learnerPod
	asObject(f.api.replacement["metadata"])["uid"] = "learner-u2-uid"
	if err := f.c.saveAuto(t.Context(), f.r, state, version); err != nil {
		t.Fatal(err)
	}
	state, version = f.load(t)
	disposition, err := f.c.assessAutomaticEscalation(t.Context(), f.r, state, version, f.api.sts)
	if err != nil {
		t.Fatalf("assess exact learner retirement after next ordinal UID: %v", err)
	}
	if disposition == automaticEscalationStop || state.Intent.Request.Scope != ScopeVoter || state.Intent.LearnerUID != "learner-uid" {
		t.Fatalf("next ordinal UID prevented propagation of the exact learner abort: disposition=%v intent=%+v", disposition, state.Intent)
	}
}

func TestAutomaticRetirementEscalationRealQuorumLossRemainsBounded(t *testing.T) {
	f := newRetirementEscalationFixture(t)
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
		t.Fatal("finite retirement window did not submit abort")
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
