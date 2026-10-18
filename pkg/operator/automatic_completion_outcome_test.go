package operator

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

// The child really completed before all target membership endpoints disappear.
// Losing the final health check must not make its source active again.
func TestAutomaticCompletionSurvivesTargetFailure(t *testing.T) {
	for _, mode := range []string{"target failure", "single target failure", "two target failures after adoption", "failure before identity capture", "failure before identity capture async", "failure before identity capture empty", "legacy completed child", "legacy completed child async", "partial installed inventory", "capture unauthorized", "capture wrong cluster", "stale CAS", "bare complete", "changed reservation", "changed lineage", "duplicate quorum", "changed UID", "duplicate installed identity"} {
		t.Run(mode, func(t *testing.T) { testAutomaticCompletionOutcome(t, mode) })
	}
}

func TestAutomaticMembershipCompletionInstallsFailedLearner(t *testing.T) {
	ctx, child, api, c := membershipFixture(t)
	child.Spec.Container = "rhiza"
	child.Spec.Membership.OperationID = "recover"
	child.Spec.Membership.RemovedPod = "n3"
	api.learnerID = "learner"
	api.membership["learner"] = network.MembershipStatus{NodeID: "learner", ClusterID: "source", ConfigID: 1, Voters: []quepaxa.NodeID{"n1", "n2", "n3"}, WALIdentity: strings.Repeat("a", 64)}
	api.membershipSecret["data"].(object)["member"] = base64.StdEncoding.EncodeToString(jsonBytes(voterMember("source", "learner", "new-token")))
	advance := func() {
		t.Helper()
		if err := c.reconcileAll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	advance() // remove journal
	api.membership["n1"] = membershipStatus("n1", 2, "n1", "n2")
	api.membership["n2"] = membershipStatus("n2", 2, "n1", "n2")
	advance() // addition journal
	api.membership["n1"] = membershipStatus("n1", 3, "n1", "n2", "learner")
	api.membership["n2"] = membershipStatus("n2", 3, "n1", "n2", "learner")
	api.membership["learner"] = membershipStatus("learner", 3, "n1", "n2", "learner")
	status := api.membership["learner"]
	status.WALIdentity = strings.Repeat("a", 64)
	api.membership["learner"] = status
	advance()
	if child.Status.Phase != "Complete" {
		t.Fatalf("membership did not commit: %+v", child.Status)
	}
	r := autoClusterResource("cluster-1", "binding-uid-1", "logical-1")
	state := validAutoControl()
	state.StatefulSetUID = "sts-uid"
	state.Voters = []VoterPod{{NodeID: "n1", Pod: "n1"}, {NodeID: "n2", Pod: "n2"}, {NodeID: "n3", Pod: "n3"}}
	r.Spec.VoterCount = len(state.Voters)
	request := FenceRequest{LogicalID: r.Spec.LogicalID, BindingUID: state.BindingUID, Namespace: "default", StatefulSet: "rhiza", StatefulSetUID: state.StatefulSetUID, SourceClusterID: "source", OperationID: "recover", Scope: ScopeVoter, Targets: []FenceTarget{{NodeID: "n3", Pod: "n3", PodUID: "pod-n3", WALIdentity: strings.Repeat("b", 64)}}}
	hash, err := requestHash(&request)
	if err != nil {
		t.Fatal(err)
	}
	state.Intent = &autoIntent{Request: request, RecoveryName: "recover", RecoveryUID: "resource-uid", Durability: "before-ack", Failed: "n3", Voters: append([]VoterPod(nil), state.Voters...), LearnerName: "learner", LearnerUID: "learner-uid", LearnerWAL: strings.Repeat("a", 64), Aborting: true, Proof: &FenceProof{OperationID: request.OperationID, SourceClusterID: request.SourceClusterID, BindingUID: request.BindingUID, RequestHash: hash, ProofID: child.Spec.Membership.Fence.Evidence, ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true}}
	child.Spec.Membership.AbortAddition = true
	child.Metadata["annotations"] = object{automaticOwner: state.BindingUID, automaticLogical: r.Spec.LogicalID}
	state.Intent.SpecHash = hashJSON(child.Spec)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		if q.Method == http.MethodGet && q.URL.Path == c.resourcePath("recover") {
			json.NewEncoder(w).Encode(child)
			return
		}
		if q.Method == http.MethodPut && q.URL.Path == c.clusterPath("cluster-1")+"/status" {
			json.NewEncoder(w).Encode(r)
			return
		}
		api.serve(w, q)
	}))
	t.Cleanup(server.Close)
	c.Kube.BaseURL, c.Kube.Client = server.URL, server.Client()
	fenced := 0
	c.Fencer = &countingFencer{count: &fenced}
	if err := c.saveAuto(ctx, r, state, nil); err != nil {
		t.Fatal(err)
	}
	state, version, err := c.loadAuto(ctx, c.autoKey(r))
	if err != nil {
		t.Fatal(err)
	}
	// Candidate and surviving voters fail after the committed completion.
	api.membership = nil
	if err := c.advanceAutomatic(ctx, r, state, version, api.sts); err != nil {
		t.Fatal(err)
	}
	loaded, _, err := c.loadAuto(ctx, c.autoKey(r))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Intent != nil || loaded.Completed == nil || len(loaded.Voters) != 3 || loaded.Voters[2].NodeID != "learner" || loaded.Identities["learner"].WALIdentity != strings.Repeat("a", 64) || loaded.Identities["learner"].PodUID != "learner-uid" {
		t.Fatalf("committed failed learner lost: %+v status=%+v", loaded, r.Status)
	}
}

// automaticTargetFixture drives the shared recovery fixture to a restarted
// target that already carries the annotations an automatic child has.
func automaticTargetFixture(t *testing.T, targetMode string, voters []quepaxa.Member) (context.Context, objstore.Bucket, *Resource, *operatorAPI, *Controller) {
	t.Helper()
	ctx, bucket, child, api, c := controllerFixtureVoters(t, "before-ack", targetMode, voters)
	if targetMode == "async" {
		// The shared fixture's recovery HTTP responder is fixed to before-ack;
		// serve this candidate's actual configured target mode instead.
		c.HTTP = &http.Client{Transport: completionOutcomeTransport(func(q *http.Request) (*http.Response, error) {
			response, err := http.DefaultTransport.RoundTrip(q)
			if err != nil || q.URL.Path != "/recovery/status" {
				return response, err
			}
			defer response.Body.Close()
			var peer network.VoterRecoveryStatus
			if err := json.NewDecoder(response.Body).Decode(&peer); err != nil {
				return nil, err
			}
			peer.Durability = targetMode
			response.Body = io.NopCloser(bytes.NewReader(jsonBytes(peer)))
			return response, nil
		})}
	}
	child.Spec.RecoveryID = "recovery-1"
	child.Spec.Fence = fence(child)
	for range 12 {
		if err := c.reconcileAll(ctx); err != nil {
			t.Fatal(err)
		}
		if child.Status.Stage == "Restarted" {
			break
		}
	}
	child.Metadata["annotations"] = object{automaticOwner: "binding-uid-1", automaticLogical: "logical-1"}
	return ctx, bucket, child, api, c
}

// Automatic observation and completion must follow the configured voter count,
// not the two-ready-peer threshold of a three-voter cluster.
func TestAutomaticVoterMajorityCompletion(t *testing.T) {
	for _, tc := range []struct{ voters, ready int }{
		{voters: 3, ready: 1},
		{voters: 3, ready: 2},
		{voters: 4, ready: 2},
		{voters: 4, ready: 3},
		{voters: 5, ready: 2},
		{voters: 5, ready: 3},
	} {
		t.Run(fmt.Sprintf("voters=%d ready=%d", tc.voters, tc.ready), func(t *testing.T) {
			s := completedAutomaticTarget(t, voterMembers(tc.voters))
			s.api.membership = nil
			s.child.Status.Peers = s.child.Status.Peers[:tc.ready]
			if err := s.c.advanceAutomatic(s.ctx, s.r, s.state, s.version, s.api.sts); err != nil {
				t.Fatal(err)
			}
			next, _, err := s.c.loadAuto(s.ctx, s.c.autoKey(s.r))
			if err != nil {
				t.Fatal(err)
			}
			if tc.ready < tc.voters/2+1 {
				if next.Intent == nil || s.r.Status.Phase != "Blocked" || !strings.Contains(s.r.Status.Message, "did not record the recovered target quorum") {
					t.Fatalf("below-majority target was accepted: %+v status=%+v", next, s.r.Status)
				}
				return
			}
			if next.Intent != nil || len(next.Voters) != tc.voters || next.ActiveClusterID != s.child.Status.Target || s.r.Status.Phase != "Stabilizing" {
				t.Fatalf("majority target was not installed: active=%s pending=%t voters=%d status=%+v", next.ActiveClusterID, next.Intent != nil, len(next.Voters), s.r.Status)
			}
		})
	}
}

// A learner promoted into the generation is a legitimate voter even though its
// Pod name is not a derived StatefulSet ordinal.
func TestAutomaticCompletionAcceptsPromotedLearnerVoter(t *testing.T) {
	ctx := t.Context()
	voters := voterMembers(3)
	learner := "auto-promoted-learner-1"
	s := completedAutomaticTarget(t, voters)
	s.state.Learners = []learnerIncarnation{{SourceClusterID: "source", OperationID: "op", CredentialUID: "cred", Target: FenceTarget{NodeID: learner, Pod: learner, PodUID: "pod-" + learner, WALIdentity: strings.Repeat("a", 64)}}}
	s.state.Intent.Voters = []VoterPod{{NodeID: "rhiza-0", Pod: "rhiza-0"}, {NodeID: "rhiza-1", Pod: "rhiza-1"}, {NodeID: "rhiza-2", Pod: "rhiza-2"}}
	s.child.Status.TargetIdentities[0] = FenceTarget{NodeID: "rhiza-0", Pod: "rhiza-0", PodUID: "pod-rhiza-0", WALIdentity: strings.Repeat("a", 64)}
	s.child.Status.TargetIdentities[1] = FenceTarget{NodeID: "rhiza-1", Pod: "rhiza-1", PodUID: "pod-rhiza-1", WALIdentity: strings.Repeat("a", 64)}
	s.child.Status.TargetIdentities[2] = FenceTarget{NodeID: learner, Pod: learner, PodUID: "pod-" + learner, WALIdentity: strings.Repeat("a", 64)}
	if err := s.c.saveAuto(ctx, s.r, s.state, s.version); err != nil {
		t.Fatal(err)
	}
	state, version, err := s.c.loadAuto(ctx, s.c.autoKey(s.r))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.c.advanceAutomatic(ctx, s.r, state, version, s.api.sts); err != nil {
		t.Fatal(err)
	}
	loaded, _, err := s.c.loadAuto(ctx, s.c.autoKey(s.r))
	if err != nil || loaded.Intent != nil || s.r.Status.Phase != "Stabilizing" {
		t.Fatalf("promoted learner was not accepted: state=%+v status=%+v err=%v", loaded, s.r.Status, err)
	}
}

func TestAutomaticCompletionRejectsUnknownVoterIdentity(t *testing.T) {
	ctx := t.Context()
	voters := voterMembers(3)
	learner := "auto-promoted-learner-1"
	s := completedAutomaticTarget(t, voters)
	s.state.Learners = []learnerIncarnation{{SourceClusterID: "source", OperationID: "op", CredentialUID: "cred", Target: FenceTarget{NodeID: learner, Pod: learner, PodUID: "pod-" + learner, WALIdentity: strings.Repeat("a", 64)}}}
	s.state.Intent.Voters = []VoterPod{{NodeID: "rhiza-0", Pod: "rhiza-0"}, {NodeID: "rhiza-1", Pod: "rhiza-1"}, {NodeID: "rhiza-2", Pod: "rhiza-2"}}
	s.child.Status.TargetIdentities[0] = FenceTarget{NodeID: "rhiza-0", Pod: "rhiza-0", PodUID: "pod-rhiza-0", WALIdentity: strings.Repeat("a", 64)}
	s.child.Status.TargetIdentities[1] = FenceTarget{NodeID: "rhiza-1", Pod: "rhiza-1", PodUID: "pod-rhiza-1", WALIdentity: strings.Repeat("a", 64)}
	s.child.Status.TargetIdentities[2] = FenceTarget{NodeID: "unknown-voter", Pod: "unknown-voter", PodUID: "pod-unknown-voter", WALIdentity: strings.Repeat("a", 64)}
	if err := s.c.saveAuto(ctx, s.r, s.state, s.version); err != nil {
		t.Fatal(err)
	}
	state, version, err := s.c.loadAuto(ctx, s.c.autoKey(s.r))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.c.advanceAutomatic(ctx, s.r, state, version, s.api.sts); err != nil {
		t.Fatal(err)
	}
	if s.r.Status.Phase != "Blocked" || !strings.Contains(s.r.Status.Message, "installed target identity inventory is incomplete or conflicting") {
		t.Fatalf("unknown voter was not rejected: %+v", s.r.Status)
	}
}

// automaticCompletion holds one installed automatic target generation, its
// durable intent, and the fixtures advanceAutomatic authenticates against.
type automaticCompletion struct {
	ctx     context.Context
	child   *Resource
	api     *operatorAPI
	c       *Controller
	r       *ClusterResource
	state   *autoControl
	version *objstore.ObjectVersion
}

// completedAutomaticTarget installs an automatic target for every configured
// voter and stages the durable intent that advanceAutomatic later verifies.
func completedAutomaticTarget(t *testing.T, voters []quepaxa.Member) *automaticCompletion {
	t.Helper()
	ctx, bucket, child, api, c := automaticTargetFixture(t, "before-ack", voters)
	ids := make([]string, 0, len(voters))
	for _, member := range voters {
		ids = append(ids, string(member.ID))
	}
	api.membership = map[string]network.MembershipStatus{}
	for _, id := range ids {
		status := membershipStatus(id, 1, ids...)
		status.ClusterID, status.WALIdentity = child.Status.Target, "target-wal-"+id
		api.membership[id] = status
	}
	if err := c.startAndObserve(ctx, child, api.sts); err != nil {
		t.Fatal(err)
	}
	if child.Status.Phase != "Complete" || len(child.Status.TargetIdentities) != len(voters) {
		t.Fatalf("target did not complete: %+v", child.Status)
	}
	r := autoClusterResource("cluster-1", "binding-uid-1", "logical-1")
	state := validAutoControl()
	state.StatefulSetUID, state.Voters = "sts-uid", nil
	for _, id := range ids {
		state.Voters = append(state.Voters, VoterPod{NodeID: quepaxa.NodeID(id), Pod: id})
	}
	r.Spec.VoterCount = len(state.Voters)
	observed := *state
	observed.ActiveClusterID, observed.Identities = child.Status.Target, map[quepaxa.NodeID]FenceTarget{}
	if obs, _, err := c.observeAutomatic(ctx, r, &observed, api.sts); err != nil || !obs.Known || obs.Voters != len(voters) || obs.Reachable != len(voters) {
		t.Fatalf("observation of %d voters: %+v err=%v", len(voters), obs, err)
	}
	targets := make([]FenceTarget, 0, len(voters))
	for _, id := range ids {
		targets = append(targets, FenceTarget{NodeID: id, Pod: id})
	}
	request := FenceRequest{LogicalID: r.Spec.LogicalID, BindingUID: state.BindingUID, Namespace: "default", StatefulSet: r.Spec.StatefulSet, StatefulSetUID: state.StatefulSetUID, SourceClusterID: "source", OperationID: "recovery-1", Scope: ScopeGeneration, Targets: targets}
	hash, err := requestHash(&request)
	if err != nil {
		t.Fatal(err)
	}
	state.Intent = &autoIntent{Request: request, Voters: append([]VoterPod(nil), state.Voters...), RecoveryName: "recovery-1", RecoveryUID: "resource-uid", Durability: "before-ack", Proof: &FenceProof{OperationID: request.OperationID, SourceClusterID: request.SourceClusterID, BindingUID: request.BindingUID, RequestHash: hash, ProofID: "proof-1", ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true}}
	child.Metadata["name"] = "recovery-1"
	child.Spec.Container = "rhiza"
	child.Spec.Fence.Evidence = state.Intent.Proof.ProofID
	child.Status.SpecHash = immutableSpecHash(child.Spec)
	state.Intent.SpecHash = hashJSON(child.Spec)
	if err := bucket.Upload(ctx, "root/source/recovery/successor.json", bytes.NewReader(jsonBytes(c.reservation(child)))); err != nil {
		t.Fatal(err)
	}
	c.Fencer = &countingFencer{count: new(int)}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		if q.Method == http.MethodGet && q.URL.Path == c.resourcePath("recovery-1") {
			json.NewEncoder(w).Encode(child)
			return
		}
		if q.Method == http.MethodPut && q.URL.Path == c.clusterPath("cluster-1")+"/status" {
			json.NewEncoder(w).Encode(r)
			return
		}
		api.serve(w, q)
	}))
	t.Cleanup(server.Close)
	c.Kube.BaseURL, c.Kube.Client = server.URL, server.Client()
	if err := c.saveAuto(ctx, r, state, nil); err != nil {
		t.Fatal(err)
	}
	loaded, version, err := c.loadAuto(ctx, c.autoKey(r))
	if err != nil {
		t.Fatal(err)
	}
	return &automaticCompletion{ctx: ctx, child: child, api: api, c: c, r: r, state: loaded, version: version}
}

// A five-voter target that lost desired replicas is its own majority as soon as
// three peers answer, but it must never be installed as the next generation.
func TestAutomaticCompletionRejectsTargetReplicaShortfall(t *testing.T) {
	s := completedAutomaticTarget(t, voterMembers(5))
	asObject(s.api.sts["spec"])["replicas"] = float64(3)
	s.api.membership = nil
	if err := s.c.advanceAutomatic(s.ctx, s.r, s.state, s.version, s.api.sts); err != nil {
		t.Fatal(err)
	}
	next, _, err := s.c.loadAuto(s.ctx, s.c.autoKey(s.r))
	if err != nil {
		t.Fatal(err)
	}
	if want := "installed target StatefulSet has 3 desired replicas but the recovered generation has 5 voters"; next.Intent == nil || s.r.Status.Phase != "Blocked" || !strings.Contains(s.r.Status.Message, want) {
		t.Fatalf("scaled-down target was installed: %+v status=%+v want=%q", next.Intent, s.r.Status, want)
	}
	if next.ActiveClusterID != "source" {
		t.Fatalf("scaled-down target advanced the active generation: %s", next.ActiveClusterID)
	}
}

// The automatic path must refuse a scaled-down target and an identity capture
// that misses a configured voter.
func TestAutomaticTargetRejectsIncompleteInventory(t *testing.T) {
	voters := voterMembers(5)
	ids := make([]string, 0, len(voters))
	for _, member := range voters {
		ids = append(ids, string(member.ID))
	}
	for _, tc := range []struct {
		name    string
		phase   string
		scaled  bool
		partial bool
		want    string
	}{
		{name: "replica shortfall", phase: "Blocked", scaled: true, want: "target StatefulSet has 3 desired replicas but 5 target voters are frozen"},
		{name: "identity capture shortfall", phase: "WaitingReady", partial: true, want: "target identity inventory covers 3 of 5 configured voters"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, _, child, api, c := automaticTargetFixture(t, "before-ack", voters)
			authenticated := ids
			if tc.partial {
				authenticated = ids[:3]
			}
			api.membership = map[string]network.MembershipStatus{}
			for _, id := range authenticated {
				status := membershipStatus(id, 1, ids...)
				status.ClusterID, status.WALIdentity = child.Status.Target, "target-wal-"+id
				api.membership[id] = status
			}
			if tc.scaled {
				asObject(api.sts["spec"])["replicas"] = float64(3)
			}
			if err := c.startAndObserve(ctx, child, api.sts); err != nil {
				t.Fatal(err)
			}
			if child.Status.Phase != tc.phase || !strings.Contains(child.Status.Message, tc.want) {
				t.Fatalf("status=%+v want %s containing %q", child.Status, tc.phase, tc.want)
			}
		})
	}
}

func testAutomaticCompletionOutcome(t *testing.T, mode string) {
	legacy := strings.HasPrefix(mode, "legacy completed child")
	heldCapture := strings.HasPrefix(mode, "failure before identity capture")
	// A durable record without every identity stays verifiable; the next incident
	// must then fence the whole generation instead of one voter.
	partialInventory := mode == "partial installed inventory" || legacy
	twoFailures := mode == "two target failures after adoption"
	targetMode := "before-ack"
	if strings.HasSuffix(mode, " async") {
		targetMode = "async"
	}
	ctx, bucket, child, api, c := automaticTargetFixture(t, targetMode, members())
	api.membership = map[string]network.MembershipStatus{}
	for _, id := range []string{"n1", "n2", "n3"} {
		if heldCapture && (id == "n3" || strings.HasSuffix(mode, " empty")) {
			continue
		}
		status := membershipStatus(id, 1, "n1", "n2", "n3")
		status.ClusterID, status.WALIdentity = child.Status.Target, "target-wal-"+id
		api.membership[id] = status
	}
	if mode == "capture wrong cluster" {
		status := api.membership["n3"]
		status.ClusterID = "other"
		api.membership["n3"] = status
	}
	if mode == "capture unauthorized" {
		c.HTTP = &http.Client{Transport: completionOutcomeTransport(func(q *http.Request) (*http.Response, error) {
			if q.URL.Path == "/membership/status" && q.URL.Host == strings.TrimPrefix(api.peers[2].URL, "http://") {
				return &http.Response{StatusCode: http.StatusUnauthorized, Body: io.NopCloser(strings.NewReader("{}")), Header: make(http.Header)}, nil
			}
			return http.DefaultTransport.RoundTrip(q)
		})}
	}
	if err := c.startAndObserve(ctx, child, api.sts); err != nil {
		t.Fatal(err)
	}
	if heldCapture {
		// A capture that misses a configured voter must not install an incomplete
		// identity inventory as the next generation's fencing authority.
		if child.Status.Phase != "WaitingReady" || !strings.HasPrefix(child.Status.Message, "automatic target identities are not verified") {
			t.Fatalf("incomplete identity capture installed a target: %+v", child.Status)
		}
		return
	}
	if strings.HasPrefix(mode, "capture ") {
		if child.Status.Phase != "WaitingReady" || child.Status.TargetIdentities != nil {
			t.Fatalf("untrusted identity was recorded as an unavailable target: %+v", child.Status)
		}
		return
	}
	if child.Status.Phase != "Complete" {
		t.Fatalf("child did not complete: %+v", child.Status)
	}
	r := autoClusterResource("cluster-1", "binding-uid-1", "logical-1")
	r.Spec.VoterCount = 3
	state := validAutoControl()
	state.StatefulSetUID = "sts-uid"
	state.Voters = []VoterPod{{NodeID: "n1", Pod: "n1"}, {NodeID: "n2", Pod: "n2"}, {NodeID: "n3", Pod: "n3"}}
	request := FenceRequest{LogicalID: r.Spec.LogicalID, BindingUID: state.BindingUID, Namespace: "default", StatefulSet: "rhiza", StatefulSetUID: state.StatefulSetUID, SourceClusterID: "source", OperationID: "recovery-1", Scope: ScopeGeneration, Targets: []FenceTarget{{NodeID: "n1", Pod: "n1"}, {NodeID: "n2", Pod: "n2"}, {NodeID: "n3", Pod: "n3"}}}
	hash, err := requestHash(&request)
	if err != nil {
		t.Fatal(err)
	}
	state.Intent = &autoIntent{Request: request, Voters: append([]VoterPod(nil), state.Voters...), RecoveryName: "recovery-1", RecoveryUID: "resource-uid", Durability: targetMode, Proof: &FenceProof{OperationID: request.OperationID, SourceClusterID: request.SourceClusterID, BindingUID: request.BindingUID, RequestHash: hash, ProofID: "proof-1", ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true}}
	child.Metadata["name"] = "recovery-1"
	child.Metadata["annotations"] = object{automaticOwner: state.BindingUID, automaticLogical: r.Spec.LogicalID}
	child.Spec.Container = "rhiza"
	child.Spec.Fence.Evidence = state.Intent.Proof.ProofID
	child.Status.SpecHash = immutableSpecHash(child.Spec)
	state.Intent.SpecHash = hashJSON(child.Spec)
	// The reservation includes the immutable specification. Reproduce the
	// equivalent real reservation after normalizing the explicit container.
	if err := bucket.Upload(ctx, "root/source/recovery/successor.json", bytes.NewReader(jsonBytes(c.reservation(child)))); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		if q.Method == http.MethodGet && q.URL.Path == c.resourcePath("recovery-1") {
			json.NewEncoder(w).Encode(child)
			return
		}
		if q.Method == http.MethodPut && q.URL.Path == c.clusterPath("cluster-1")+"/status" {
			json.NewEncoder(w).Encode(r)
			return
		}
		api.serve(w, q)
	}))
	t.Cleanup(server.Close)
	c.Kube.BaseURL, c.Kube.Client = server.URL, server.Client()
	fenced := 0
	c.Fencer = &countingFencer{count: &fenced}
	if err := c.saveAuto(ctx, r, state, nil); err != nil {
		t.Fatal(err)
	}
	state, version, err := c.loadAuto(ctx, c.autoKey(r))
	if err != nil {
		t.Fatal(err)
	}
	api.membership = nil
	switch mode {
	case "bare complete":
		child.Status = Status{Phase: "Complete", Stage: "Complete", Target: child.Status.Target}
	case "changed reservation":
		if err := bucket.Upload(ctx, "root/source/recovery/successor.json", bytes.NewReader([]byte(`{}`))); err != nil {
			t.Fatal(err)
		}
	case "changed lineage":
		if err := bucket.Upload(ctx, "root/"+child.Status.Target+"/recovery/generation.json", bytes.NewReader([]byte(`{}`))); err != nil {
			t.Fatal(err)
		}
	case "duplicate quorum":
		child.Status.Peers = append(child.Status.Peers[:1], child.Status.Peers[0])
	case "changed UID":
		child.Metadata["uid"] = "recreated-child"
	case "legacy completed child", "legacy completed child async":
		child.Status.TargetIdentities = nil
	case "partial installed inventory":
		child.Status.TargetIdentities = child.Status.TargetIdentities[:2]
	case "duplicate installed identity":
		child.Status.TargetIdentities[2] = child.Status.TargetIdentities[0]
	case "stale CAS":
		if err := c.saveAuto(ctx, r, state, version); err != nil {
			t.Fatal(err)
		}
	}
	err = c.advanceAutomatic(ctx, r, state, version, api.sts)
	if mode == "stale CAS" {
		if !bucket.IsConditionNotMetErr(err) {
			t.Fatalf("stale completion did not fail CAS: %v", err)
		}
		state, version, err = c.loadAuto(ctx, c.autoKey(r))
		if err != nil || state.Intent == nil || state.ActiveClusterID != "source" {
			t.Fatalf("failed CAS advanced durable authority: %+v %v", state, err)
		}
		err = c.advanceAutomatic(ctx, r, state, version, api.sts)
	}
	if err != nil {
		t.Fatal(err)
	}
	loaded, _, err := c.loadAuto(ctx, c.autoKey(r))
	if err != nil {
		t.Fatal(err)
	}
	if mode != "target failure" && mode != "single target failure" && !twoFailures && !partialInventory && mode != "stale CAS" {
		if loaded.Intent == nil || loaded.ActiveClusterID != "source" || len(loaded.Completions) != 0 || r.Status.Phase != "Blocked" {
			t.Fatalf("invalid completion changed durable authority: %+v status=%+v", loaded, r.Status)
		}
		return
	}
	if loaded.Intent != nil || loaded.ActiveClusterID != child.Status.Target || len(loaded.Completions) != 1 {
		t.Fatalf("completed target lost after health failure: active=%s pending=%t completions=%d status=%+v", loaded.ActiveClusterID, loaded.Intent != nil, len(loaded.Completions), r.Status)
	}
	if len(loaded.Voters) != 3 || loaded.Voters[0].NodeID != quepaxa.NodeID("n1") || fenced != 0 {
		t.Fatal("installed inventory or fencing changed")
	}
	if loaded.Completed == nil || loaded.Completed.RecoveryUID != "resource-uid" || loaded.Completed.ClusterID != child.Status.Target {
		t.Fatal("installed outcome was not preserved independently of health")
	}
	wantKnown := 3
	if partialInventory && !legacy {
		wantKnown = 2
	}
	if legacy {
		wantKnown = 0
	}
	if len(loaded.Identities) != wantKnown || (!partialInventory && (loaded.Identities["n3"].PodUID != "pod-n3" || loaded.Identities["n3"].WALIdentity != "target-wal-n3")) {
		t.Fatalf("installed target identities lost before next incident: %+v", loaded.Identities)
	}
	if mode == "single target failure" || twoFailures || partialInventory {
		r.Spec.Automatic = true
		r.Spec.Policy = AutoPolicy{FailureGraceSeconds: 1, CooldownSeconds: 1, MaxRecoveriesPerDay: 10}
		loaded.BindingHash = autoBinding(r)
		loaded.SuspectedSince = time.Now().Add(-3 * time.Second)
		loaded.Completions = []time.Time{loaded.SuspectedSince}
		_, version, err = c.loadAuto(ctx, c.autoKey(r))
		if err != nil {
			t.Fatal(err)
		}
		if err := c.saveAuto(ctx, r, loaded, version); err != nil {
			t.Fatal(err)
		}
		api.membership = map[string]network.MembershipStatus{}
		for _, id := range []string{"n1", "n2"} {
			if twoFailures && id == "n2" {
				continue
			}
			status := membershipStatus(id, 1, "n1", "n2", "n3")
			status.ClusterID, status.WALIdentity = child.Status.Target, "target-wal-"+id
			api.membership[id] = status
		}
		ctr, err := container(api.sts, "rhiza")
		if err != nil {
			t.Fatal(err)
		}
		env, err := c.environment(ctx, ctr)
		if err != nil {
			t.Fatal(err)
		}
		c.HTTP = &http.Client{Transport: completionOutcomeTransport(func(q *http.Request) (*http.Response, error) {
			if q.URL.Path != "/recovery/probe" {
				return http.DefaultTransport.RoundTrip(q)
			}
			id := "n1"
			if q.URL.Host == strings.TrimPrefix(api.peers[1].URL, "http://") {
				id = "n2"
			}
			probe := network.RecoveryProbe{Excluded: q.URL.Query().Get("exclude"), Nonce: q.URL.Query().Get("nonce"), Status: network.VoterRecoveryStatus{NodeID: id, ClusterID: child.Status.Target, Quorum: !twoFailures}}
			mac := hmac.New(sha256.New, []byte(env["RHIZA_ADMIN_TOKEN"]))
			mac.Write(jsonBytes(probe))
			probe.MAC = hex.EncodeToString(mac.Sum(nil))
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(jsonBytes(probe))), Header: make(http.Header)}, nil
		})}
		if err := c.reconcileCluster(ctx, r); err != nil {
			t.Fatal(err)
		}
		next, _, err := c.loadAuto(ctx, c.autoKey(r))
		if err != nil {
			t.Fatal(err)
		}
		if partialInventory && targetMode == "async" {
			if next.Intent != nil || next.ActiveClusterID != child.Status.Target || r.Status.Phase != "Blocked" || !strings.Contains(r.Status.Message, "async source requires allowDataLoss") || fenced != 0 {
				t.Fatalf("unknown incarnation bypassed async generation loss approval: %+v status=%+v", next, r.Status)
			}
			return
		}
		if partialInventory || twoFailures {
			wantIdentities := 3
			if partialInventory {
				wantIdentities = 2
			}
			if next.Intent == nil || next.Intent.Failed != "" || next.Intent.Request.Scope != ScopeGeneration || next.Intent.Request.SourceClusterID != child.Status.Target || next.Intent.RecoveryName == "recovery-1" || len(next.Intent.Request.Targets) != 3 || next.Intent.Proof != nil || fenced != 0 || len(next.Identities) != wantIdentities {
				t.Fatalf("unknown incarnation did not reserve fresh full-generation fencing: %+v status=%+v", next, r.Status)
			}
			reader, err := bucket.Get(ctx, "root/source/recovery/successor.json")
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(reader)
			reader.Close()
			if err != nil || !bytes.Equal(data, jsonBytes(c.reservation(child))) {
				t.Fatalf("old source reservation was reopened or reforked: %s %v", data, err)
			}
			return
		}
		if next.Intent == nil || next.Intent.Failed != "n3" || next.Intent.Request.SourceClusterID != child.Status.Target || next.Intent.RecoveryName == "recovery-1" || len(next.Intent.Request.Targets) != 1 || next.Intent.Request.Targets[0].WALIdentity != "target-wal-n3" || next.Intent.Request.Targets[0].PodUID != "pod-n3" {
			t.Fatalf("new incident cannot fence installed failed voter: %+v status=%+v", next, r.Status)
		}
		return
	}
	obs, _, err := c.observeAutomatic(ctx, r, loaded, api.sts)
	if err != nil || !obs.Known || obs.QuorumVerified || obs.Reachable != 0 {
		t.Fatalf("current target outage not retained by health observation: %+v %v", obs, err)
	}
	if ok, err := c.authorizeAutomaticChild(ctx, child); err != nil || ok {
		t.Fatalf("completed child retained execution authority: %t %v", ok, err)
	}
}

type completionOutcomeTransport func(*http.Request) (*http.Response, error)

func (f completionOutcomeTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
