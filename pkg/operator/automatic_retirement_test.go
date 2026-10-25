package operator

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// seedLegacyRemoveMembershipJournal models status persisted by an older
// operator before engine-owned ordinary replacement became the policy. The
// current controller may drain this exact request, but must not invent it.
func seedLegacyRemoveMembershipJournal(r *Resource) {
	m := r.Spec.Membership
	r.Status.Membership = &MembershipStatus{Phase: "Removing", RemoveRequest: &network.MembershipChange{
		OperationID: membershipOperationID(r, "remove"), ClusterID: r.Spec.SourceClusterID,
		ExpectedConfigID: 1, Remove: m.Remove,
		Fence: &network.MembershipFence{NodeID: m.Fence.NodeID, WALIdentity: m.Fence.WALIdentity,
			WorkloadUID: m.Fence.WorkloadUID, Confirmed: m.Fence.Confirmed, Evidence: m.Fence.Evidence},
	}}
}

type retirementTransport func(*http.Request) (*http.Response, error)

func (f retirementTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type retirementFencer func(context.Context, FenceRequest) (*FenceProof, error)

func (f retirementFencer) Fence(ctx context.Context, r FenceRequest) (*FenceProof, error) {
	return f(ctx, r)
}

func TestLearnerRetirementReservesHandshakeWithoutSnapshotJournal(t *testing.T) {
	r := autoClusterResource("cluster", "uid", "logical")
	state := validAutoControl()
	request := FenceRequest{LogicalID: "logical", BindingUID: state.BindingUID, Namespace: "default", StatefulSet: "rhiza", StatefulSetUID: state.StatefulSetUID, SourceClusterID: "source", OperationID: "replace", Scope: ScopeVoter,
		Targets: []FenceTarget{{NodeID: "n3", Pod: "rhiza-2", PodUID: "failed-uid", WALIdentity: "failed-wal"}}}
	state.Intent = &autoIntent{RecoveryName: "replace", RecoveryUID: "recovery-uid", Failed: "n3", Voters: append([]VoterPod(nil), state.Voters...),
		Durability: "before-ack", Proof: &FenceProof{ProofID: "fence-proof"}, LearnerName: "replace-learner-0", LearnerUID: "learner-uid",
		LearnerPodName: "rhiza-2", LearnerSecretName: sameOrdinalSecretName(state.BindingUID, "learner-uid"), LearnerSecretUID: "learner-secret-uid",
		LearnerSecretRV: "17", LearnerWAL: strings.Repeat("a", 64), LearnerMissingSince: time.Now().Add(-time.Hour), Request: request}
	proofCalls := 0
	proof, err := (&countingFencer{count: &proofCalls}).Fence(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	proof.ProofID = "fence-proof"
	state.Intent.Proof = proof
	peerToken := "learner-peer-token"
	member := voterMember("source", state.Intent.LearnerName, peerToken)
	member.WALIdentity = state.Intent.LearnerWAL
	secretData := object{
		"member":           base64.StdEncoding.EncodeToString(jsonBytes(member)),
		"RHIZA_PEER_TOKEN": base64.StdEncoding.EncodeToString([]byte(peerToken)),
	}
	state.Intent.LearnerSecretHash = hashJSON(secretData)
	secret := object{
		"immutable": true,
		"metadata": object{
			"name": state.Intent.LearnerSecretName, "uid": state.Intent.LearnerSecretUID, "resourceVersion": state.Intent.LearnerSecretRV,
			"annotations": object{ownerAnnotation: state.Intent.Request.OperationID},
		},
		"data": secretData,
	}
	if err := rememberLearner(state, learnerIncarnation{
		SourceClusterID: state.ActiveClusterID, OperationID: state.Intent.Request.OperationID,
		CredentialName: state.Intent.LearnerSecretName, CredentialUID: state.Intent.LearnerSecretUID,
		Target: FenceTarget{NodeID: state.Intent.LearnerName, Pod: learnerPhysicalPod(state.Intent), PodUID: state.Intent.LearnerUID, WALIdentity: state.Intent.LearnerWAL},
	}); err != nil {
		t.Fatalf("seed canonical learner credential history: %v", err)
	}
	c := autoController(autoBucket(t))
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodGet && req.URL.Path == c.corePath("secrets", state.Intent.LearnerSecretName) {
			_ = json.NewEncoder(w).Encode(secret)
			return
		}
		if req.Method == http.MethodPut {
			_ = json.NewEncoder(w).Encode(r)
			return
		}
		http.NotFound(w, req)
	}))
	var closeAPIOne sync.Once
	closeAPI := func() { closeAPIOne.Do(func() { api.Close() }) }
	defer closeAPI()
	count := 0
	c.Kube.BaseURL, c.Kube.Client = api.URL, api.Client()
	c.Kube.TokenFile = filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(c.Kube.TokenFile, []byte("token"), 0600); err != nil {
		t.Fatal(err)
	}
	child := &Resource{} // Snapshot read before a delayed executor journals its add.
	liveChild := &Resource{Metadata: object{"name": state.Intent.RecoveryName, "uid": state.Intent.RecoveryUID, "generation": float64(1),
		"annotations": object{automaticOwner: state.BindingUID, automaticLogical: r.Spec.LogicalID}}}
	liveChild.Spec = automaticRecoveryChildSpec(r, state, state.Intent)
	state.Intent.SpecHash = hashJSON(liveChild.Spec)
	assignment, err := sameOrdinalAssignmentDigest(state, state.Intent)
	if err != nil {
		t.Fatalf("build canonical learner assignment: %v", err)
	}
	state.Intent.LearnerAssignmentID = assignment
	addRequest := network.MembershipChange{
		OperationID: membershipOperationID(liveChild, "add"), ClusterID: state.ActiveClusterID,
		ExpectedConfigID: 2, ExpectedAbortSlot: 0, Add: &member,
	}
	add := &MembershipAddJournal{
		OperationID: addRequest.OperationID, ExpectedConfigID: addRequest.ExpectedConfigID, ExpectedAbortSlot: addRequest.ExpectedAbortSlot,
		MemberID: member.ID, WALIdentity: member.WALIdentity, RequestHash: hashJSON(addRequest),
		SecretUID: state.Intent.LearnerSecretUID, SecretResourceVer: state.Intent.LearnerSecretRV,
	}
	removeRequest := &network.MembershipChange{
		OperationID: membershipOperationID(liveChild, "remove"), ClusterID: state.ActiveClusterID, ExpectedConfigID: 1,
		Remove: liveChild.Spec.Membership.Remove,
		Fence: &network.MembershipFence{
			NodeID: liveChild.Spec.Membership.Fence.NodeID, WALIdentity: liveChild.Spec.Membership.Fence.WALIdentity,
			WorkloadUID: liveChild.Spec.Membership.Fence.WorkloadUID, Confirmed: true, Evidence: liveChild.Spec.Membership.Fence.Evidence,
		},
	}
	liveChild.Status.Membership = &MembershipStatus{Phase: "Adding", Add: add, RemoveRequest: removeRequest}
	if matches, err := c.membershipAddMatchesIntent(context.Background(), liveChild, state, add, state.Intent); err != nil || !matches {
		t.Fatalf("delayed ADD fixture is not bound to the current immutable credential and request: matches=%t err=%v", matches, err)
	}
	if _, err := c.handleAutomaticLearner(context.Background(), r, state, nil, nil, child); err != nil {
		t.Fatal(err)
	}
	if !state.Intent.Aborting || state.Intent.LearnerAttempt != 0 || state.Intent.LearnerName != "replace-learner-0" {
		t.Fatalf("candidate changed before executor retirement acknowledgment: %+v", state.Intent)
	}
	if liveChild.Status.Membership.Add == nil {
		t.Fatal("delayed journal was lost")
	}
	if count != 0 {
		t.Fatal("physical fencing started before retirement was acknowledged")
	}
	state, version, err := c.loadAuto(context.Background(), c.autoKey(r))
	if err != nil {
		t.Fatal(err)
	}
	state.Intent.PreviousSpecHash = state.Intent.SpecHash
	liveChild.Spec = automaticRecoveryChildSpec(r, state, state.Intent)
	state.Intent.SpecHash = hashJSON(liveChild.Spec)
	assignment, err = sameOrdinalAssignmentDigest(state, state.Intent)
	if err != nil {
		t.Fatalf("update canonical learner assignment for abort revision: %v", err)
	}
	state.Intent.LearnerAssignmentID = assignment
	liveChild.Metadata["generation"] = float64(2)
	addBefore, removeBefore := hashJSON(liveChild.Status.Membership.Add), hashJSON(liveChild.Status.Membership.RemoveRequest)
	acknowledgeCanonicalLearnerAbort(liveChild)
	liveChild.Status.ObservedGeneration = 1
	if hashJSON(liveChild.Status.Membership.Add) != addBefore || hashJSON(liveChild.Status.Membership.RemoveRequest) != removeBefore {
		t.Fatal("abort acknowledgment helper changed the delayed ADD or REMOVE journal")
	}
	if err := c.saveAuto(context.Background(), r, state, version); err != nil {
		t.Fatal(err)
	}
	if handled, err := c.handleAutomaticLearner(context.Background(), r, state, nil, nil, liveChild); handled || err != nil || state.Intent.LearnerAttempt != 0 || count != 0 {
		t.Fatalf("stale acknowledgment advanced the candidate: handled=%t err=%v intent=%+v", handled, err, state.Intent)
	}
	if hashJSON(liveChild.Status.Membership.Add) != addBefore || hashJSON(liveChild.Status.Membership.RemoveRequest) != removeBefore {
		t.Fatal("stale acknowledgment changed the delayed ADD or REMOVE journal")
	}
	acknowledgeCanonicalLearnerAbort(liveChild)
	if hashJSON(liveChild.Status.Membership.Add) != addBefore || hashJSON(liveChild.Status.Membership.RemoveRequest) != removeBefore {
		t.Fatal("current-generation acknowledgment changed the delayed ADD or REMOVE journal")
	}
	c.Fencer = &countingFencer{count: &count}
	for _, tc := range []struct {
		name   string
		mutate func(*MembershipAddJournal)
	}{
		{name: "request hash", mutate: func(add *MembershipAddJournal) { add.RequestHash = strings.Repeat("f", 64) }},
		{name: "secret UID", mutate: func(add *MembershipAddJournal) { add.SecretUID = "replacement-secret-uid" }},
	} {
		t.Run("reject changed "+tc.name, func(t *testing.T) {
			candidate := *liveChild
			membership := *liveChild.Status.Membership
			add := *membership.Add
			membership.Add = &add
			candidate.Status = liveChild.Status
			candidate.Status.Membership = &membership
			tc.mutate(candidate.Status.Membership.Add)
			stored, _, err := c.loadAuto(context.Background(), c.autoKey(r))
			if err != nil {
				t.Fatal(err)
			}
			handled, err := c.handleAutomaticLearner(context.Background(), r, stored, nil, nil, &candidate)
			if !handled || err != nil || count != 0 {
				t.Fatalf("changed delayed ADD was fenced: handled=%t fences=%d err=%v", handled, count, err)
			}
			unchanged, _, err := c.loadAuto(context.Background(), c.autoKey(r))
			if err != nil {
				t.Fatal(err)
			}
			if unchanged.Intent.LearnerAttempt != 0 || unchanged.Intent.LearnerName != "replace-learner-0" || unchanged.Intent.LearnerUID != "learner-uid" {
				t.Fatalf("invalid delayed ADD rotated the learner candidate: %+v", unchanged.Intent)
			}
		})
	}
	state, version, err = c.loadAuto(context.Background(), c.autoKey(r))
	if err != nil {
		t.Fatal(err)
	}
	retirementSinceBeforeRace := state.Intent.RetirementSince
	if retirementSinceBeforeRace.IsZero() {
		t.Fatal("incident retirement budget was not persisted before the race")
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseFence := func() { releaseOnce.Do(func() { close(release) }) }
	var fencedRequest FenceRequest
	c.Fencer = retirementFencer(func(ctx context.Context, request FenceRequest) (*FenceProof, error) {
		fencedRequest = request
		close(entered)
		<-release
		return (&countingFencer{count: &count}).Fence(ctx, request)
	})
	type handlerOutcome struct {
		handled bool
		err     error
	}
	done := make(chan handlerOutcome, 1)
	handlerCtx, cancelHandler := context.WithCancel(context.Background())
	handlerFinished := false
	defer func() {
		cancelHandler()
		releaseFence()
		if !handlerFinished {
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Errorf("retirement handler did not exit after test cleanup released the fencer")
			}
		}
	}()
	go func() {
		handled, err := c.handleAutomaticLearner(handlerCtx, r, state, version, nil, liveChild)
		done <- handlerOutcome{handled: handled, err: err}
	}()
	select {
	case <-entered:
	case outcome := <-done:
		handlerFinished = true
		t.Fatalf("retirement handler returned before starting the fencer: handled=%t err=%v intent=%+v", outcome.handled, outcome.err, state.Intent)
	case <-time.After(5 * time.Second):
		cancelHandler()
		releaseFence()
		closeAPI()
		select {
		case outcome := <-done:
			handlerFinished = true
			t.Fatalf("timed out waiting for retirement fencer; handler returned after cancellation: handled=%t err=%v intent=%+v", outcome.handled, outcome.err, state.Intent)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for retirement fencer; handler did not exit after cancellation and fencer release")
		}
	}
	waitHandler := func() (handlerOutcome, bool) {
		select {
		case outcome := <-done:
			handlerFinished = true
			return outcome, true
		case <-time.After(5 * time.Second):
			return handlerOutcome{}, false
		}
	}
	// Another reconciler can finish this exact retirement while the old call is
	// paused. Its CAS must prevent the delayed proof from rotating a second time.
	current, currentVersion, err := c.loadAuto(context.Background(), c.autoKey(r))
	if err != nil {
		releaseFence()
		if _, ok := waitHandler(); !ok {
			t.Errorf("retirement handler did not finish after releasing its fencer")
		}
		t.Fatal(err)
	}
	current.Intent.Aborting = false
	current.Intent.LearnerAttempt = 1
	current.Intent.LearnerName = "replace-learner-1"
	current.Intent.LearnerUID, current.Intent.LearnerWAL = "", ""
	if err := c.saveAuto(context.Background(), r, current, currentVersion); err != nil {
		releaseFence()
		if _, ok := waitHandler(); !ok {
			t.Errorf("retirement handler did not finish after releasing its fencer")
		}
		t.Fatal(err)
	}
	releaseFence()
	outcome, ok := waitHandler()
	if !ok {
		t.Fatal("retirement handler did not finish after releasing its fencer")
	}
	if !c.Bucket.IsConditionNotMetErr(outcome.err) {
		t.Fatalf("stale post-fence CAS was accepted: %v", outcome.err)
	}
	wantFenceTarget := FenceTarget{NodeID: "replace-learner-0", Pod: "rhiza-2", PodUID: "learner-uid", WALIdentity: strings.Repeat("a", 64)}
	if count != 1 || len(fencedRequest.Targets) != 1 || fencedRequest.Targets[0] != wantFenceTarget {
		t.Fatalf("delayed fence escaped its retired incarnation: count=%d target=%+v want=%+v", count, fencedRequest.Targets, wantFenceTarget)
	}
	current, _, err = c.loadAuto(context.Background(), c.autoKey(r))
	if err != nil {
		t.Fatal(err)
	}
	if current.Intent.LearnerAttempt != 1 || current.Intent.LearnerName != "replace-learner-1" {
		t.Fatalf("stale fence changed the next candidate: %+v", current.Intent)
	}
	if !current.Intent.RetirementSince.Equal(retirementSinceBeforeRace) {
		t.Fatalf("incident retirement budget changed across the race: before=%s after=%s", retirementSinceBeforeRace, current.Intent.RetirementSince)
	}
}

func TestMembershipRetirementBlocksNeverJournaledAddition(t *testing.T) {
	ctx, r, api, c := membershipFixture(t)
	r.Spec.Membership.AbortAddition = true
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase != "Blocked" || !strings.Contains(r.Status.Message, "new operator-managed membership changes are disabled") {
		t.Fatalf("never-journaled request was not rejected by the engine-owned policy: phase=%q message=%q", r.Status.Phase, r.Status.Message)
	}
	if r.Status.Membership != nil || len(api.changes) != 0 || len(api.aborts) != 0 || api.stsPuts != 0 {
		t.Fatalf("never-journaled request created or mutated a membership operation: status=%+v changes=%d aborts=%d StatefulSetPuts=%d", r.Status.Membership, len(api.changes), len(api.aborts), api.stsPuts)
	}
}

func TestMembershipRetirementRecognizesCommittedAdditionAfterLearnerLoss(t *testing.T) {
	ctx, r, api, c := membershipFixture(t)
	// This test covers a removal request journaled by the previous operator
	// policy and then resumed by the current controller. New requests without
	// persisted status are separately required to block above.
	seedLegacyRemoveMembershipJournal(r)
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	api.membership["n1"] = membershipStatus("n1", 2, "n1", "n2")
	api.membership["n2"] = membershipStatus("n2", 2, "n1", "n2")
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	add := r.Status.Membership.Add
	if add == nil {
		t.Fatal("missing addition journal")
	}
	if err := writeRetirementOperation(ctx, c, r); err != nil {
		t.Fatal(err)
	}
	api.membership["n1"] = membershipStatus("n1", 3, "n1", "n2", "n4")
	api.membership["n2"] = membershipStatus("n2", 3, "n1", "n2", "n4")
	api.replacement = nil
	delete(api.membership, "n4")
	r.Spec.Membership.AbortAddition = true
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase != "Complete" || r.Status.Membership.Add == nil || len(api.aborts) != 0 {
		t.Fatalf("committed promotion blocked on unavailable learner: %+v aborts=%d", r.Status, len(api.aborts))
	}
}

func TestMembershipRetirementRejectsDelayedJournalAfterAcknowledgment(t *testing.T) {
	ctx, r, api, c := membershipFixture(t)
	seedLegacyRemoveMembershipJournal(r)
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if len(api.changes) != 1 || api.changes[0].Remove != "n3" || api.changes[0].Add != nil {
		t.Fatalf("legacy setup did not dispatch only the journaled REMOVE: %+v", api.changes)
	}
	api.membership["n1"] = membershipStatus("n1", 2, "n1", "n2")
	api.membership["n2"] = membershipStatus("n2", 2, "n1", "n2")
	r.Metadata["resourceVersion"] = "1"
	var stale Resource
	if err := json.Unmarshal(jsonBytes(r), &stale); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	c.HTTP = &http.Client{Transport: retirementTransport(func(req *http.Request) (*http.Response, error) {
		if req.URL.Host == strings.TrimPrefix(api.learner.URL, "http://") {
			close(entered)
			select {
			case <-release:
			case <-req.Context().Done():
				return nil, req.Context().Err()
			}
		}
		return http.DefaultTransport.RoundTrip(req)
	})}
	var mu sync.Mutex
	current := "2" // The automatic parent has submitted its retirement revision.
	casAPI := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == c.resourcePath(resourceName(r)) && req.Method == http.MethodGet {
			mu.Lock()
			defer mu.Unlock()
			_ = json.NewEncoder(w).Encode(r)
			return
		}
		if strings.HasSuffix(req.URL.Path, "/recover/status") && req.Method == http.MethodPut {
			mu.Lock()
			defer mu.Unlock()
			var next Resource
			if json.NewDecoder(req.Body).Decode(&next) != nil {
				http.Error(w, "bad request", 400)
				return
			}
			if str(next.Metadata["resourceVersion"]) != current {
				w.WriteHeader(http.StatusConflict)
				return
			}
			current = "3"
			next.Metadata["resourceVersion"] = current
			*r = next
			_ = json.NewEncoder(w).Encode(next)
			return
		}
		api.serve(w, req)
	}))
	defer casAPI.Close()
	c.Kube.BaseURL, c.Kube.Client = casAPI.URL, casAPI.Client()
	done := make(chan error, 1)
	go func() { done <- c.reconcileMembership(ctx, &stale) }()
	<-entered // Old executor has observed a live learner but has not journaled Add.
	r.Metadata["resourceVersion"] = "2"
	r.Spec.Membership.AbortAddition = true
	if err := c.reconcileMembership(ctx, r); err != nil {
		close(release)
		<-done
		t.Fatal(err)
	}
	close(release)
	err := <-done
	var conflict *APIError
	if !errors.As(err, &conflict) || conflict.StatusCode != http.StatusConflict {
		t.Fatalf("stale journal write error=%v", err)
	}
	if r.Status.Membership.Phase != "Aborted" || r.Status.Membership.Add != nil || len(api.changes) != 1 {
		t.Fatalf("delayed executor reopened retired candidate: %+v changes=%d", r.Status, len(api.changes))
	}
}

func TestMembershipRetirementReplaysJournalBeforeFirstDispatch(t *testing.T) {
	ctx, r, api, c := membershipFixture(t)
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	api.membership["n1"] = membershipStatus("n1", 2, "n1", "n2")
	api.membership["n2"] = membershipStatus("n2", 2, "n1", "n2")
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	r.Spec.Membership.AbortAddition = true
	requestHash := r.Status.Membership.Add.RequestHash
	c.HTTP = &http.Client{Transport: retirementTransport(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/membership/abort" {
			return &http.Response{StatusCode: http.StatusConflict, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
		}
		return http.DefaultTransport.RoundTrip(req)
	})}
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase != "Aborting" || len(api.changes) != 2 || api.changes[0].Remove != "n3" || api.changes[0].Add != nil || api.changes[0].ExpectedConfigID != 1 || api.changes[1].ExpectedConfigID != 2 || hashJSON(api.changes[1]) != requestHash {
		t.Fatalf("journaled request was not replayed verbatim: %+v changes=%+v", r.Status, api.changes)
	}
	c.HTTP = nil
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase != "Aborted" || len(api.aborts) != 1 {
		t.Fatalf("replayed round did not retire: %+v", r.Status)
	}
}

func TestMembershipRetirementDelayedDispatchCannotEscapeAbortedRound(t *testing.T) {
	ctx, r, api, c := membershipFixture(t)
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	api.membership["n1"] = membershipStatus("n1", 2, "n1", "n2")
	api.membership["n2"] = membershipStatus("n2", 2, "n1", "n2")
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	frozen, err := c.retiringMembershipRequest(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	pods, err := c.membershipPods(ctx, r.Spec.Membership.VoterPods)
	if err != nil {
		t.Fatal(err)
	}
	// Model the node's existing abort-revision contract; its real Core/WAL
	// implementation is covered by TestMembershipAdditionAbortAllowsNextRevision.
	var mu sync.Mutex
	var revision quepaxa.Slot
	pending := false
	model := retirementTransport(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path != "/membership/change" && req.URL.Path != "/membership/abort" {
			return http.DefaultTransport.RoundTrip(req)
		}
		var request network.MembershipChange
		if err := json.NewDecoder(req.Body).Decode(&request); err != nil {
			return nil, err
		}
		mu.Lock()
		defer mu.Unlock()
		code := http.StatusConflict
		if request.ExpectedConfigID == 2 && request.ExpectedAbortSlot == revision {
			if req.URL.Path == "/membership/change" {
				pending = true
				code = http.StatusServiceUnavailable
			} else if pending {
				pending = false
				revision++
				code = http.StatusOK
			}
		}
		api.mu.Lock()
		for _, id := range []string{"n1", "n2"} {
			status := api.membership[id]
			status.Pending, status.AbortSlot = pending, revision
			api.membership[id] = status
		}
		api.mu.Unlock()
		return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
	})
	c.HTTP = &http.Client{Transport: model}
	entered, release := make(chan struct{}), make(chan struct{})
	delayed := *c
	delayed.HTTP = &http.Client{Transport: retirementTransport(func(req *http.Request) (*http.Response, error) {
		close(entered)
		<-release
		return model.RoundTrip(req)
	})}
	done := make(chan error, 1)
	go func() { done <- delayed.changeMembership(ctx, pods["n1"], "rhiza", "admin", frozen) }()
	<-entered
	r.Spec.Membership.AbortAddition = true
	if err := c.reconcileMembership(ctx, r); err != nil {
		close(release)
		<-done
		t.Fatal(err)
	}
	if r.Status.Phase != "Aborting" {
		close(release)
		<-done
		t.Fatalf("unstarted delayed round not reserved: %+v", r.Status)
	}
	if err := c.reconcileMembership(ctx, r); err != nil {
		close(release)
		<-done
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err == nil {
		t.Fatal("delayed HTTP request escaped its aborted round")
	}
	if r.Status.Phase != "Aborted" || r.Status.Membership.Add == nil || revision == 0 || pending {
		t.Fatalf("round remained executable: %+v revision=%d pending=%t", r.Status, revision, pending)
	}
}

func TestMembershipRetirementCommittedEvidenceFailsClosed(t *testing.T) {
	for _, scenario := range []string{"missing-operation", "changed-operation-target", "changed-request", "changed-secret", "wrong-wal", "pending-survivor", "abort-revision", "unexpected-voter"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, r, api, c := membershipFixture(t)
			if err := c.reconcileAll(ctx); err != nil {
				t.Fatal(err)
			}
			api.membership["n1"] = membershipStatus("n1", 2, "n1", "n2")
			api.membership["n2"] = membershipStatus("n2", 2, "n1", "n2")
			if err := c.reconcileAll(ctx); err != nil {
				t.Fatal(err)
			}
			if len(api.changes) != 1 || api.changes[0].Remove != "n3" || api.changes[0].Add != nil {
				t.Fatalf("valid replacement setup did not acknowledge only the removal: %+v", api.changes)
			}
			acknowledgedChanges := len(api.changes)
			if scenario != "missing-operation" {
				if err := writeRetirementOperation(ctx, c, r); err != nil {
					t.Fatal(err)
				}
			}
			api.membership["n1"] = membershipStatus("n1", 3, "n1", "n2", "n4")
			api.membership["n2"] = membershipStatus("n2", 3, "n1", "n2", "n4")
			api.replacement = nil
			r.Spec.Membership.AbortAddition = true
			switch scenario {
			case "changed-operation-target":
				key := path.Join(c.Prefix, r.Spec.SourceClusterID, "membership", "2.json")
				reader, err := c.Bucket.Get(ctx, key)
				if err != nil {
					t.Fatal(err)
				}
				data, err := io.ReadAll(reader)
				if err != nil {
					t.Fatal(err)
				}
				if err := reader.Close(); err != nil {
					t.Fatal(err)
				}
				var operation struct {
					Request network.MembershipChange `json:"request"`
					Target  quepaxa.Cluster          `json:"target"`
				}
				if err := json.Unmarshal(data, &operation); err != nil {
					t.Fatal(err)
				}
				operation.Target.Members[2].WALIdentity = strings.Repeat("e", 64)
				if err := c.Bucket.Upload(ctx, key, bytes.NewReader(jsonBytes(operation))); err != nil {
					t.Fatal(err)
				}
			case "changed-request":
				r.Status.Membership.Add.RequestHash = "changed"
			case "changed-secret":
				api.membershipSecret["metadata"].(object)["uid"] = "new-secret"
			case "wrong-wal":
				r.Status.Membership.Add.WALIdentity = strings.Repeat("d", 64)
			case "pending-survivor":
				s := api.membership["n2"]
				s.Pending = true
				api.membership["n2"] = s
			case "abort-revision":
				s := api.membership["n2"]
				s.AbortSlot = 7
				api.membership["n2"] = s
			case "unexpected-voter":
				api.membership["n1"] = membershipStatus("n1", 3, "n1", "n2", "other")
				api.membership["n2"] = membershipStatus("n2", 3, "n1", "n2", "other")
			}
			if err := c.reconcileAll(ctx); err != nil {
				t.Fatal(err)
			}
			if r.Status.Phase != "Blocked" || r.Status.Membership.Add == nil || len(api.changes) != acknowledgedChanges || len(api.aborts) != 0 {
				t.Fatalf("unproven commit accepted: %+v", r.Status)
			}
		})
	}
}

func TestMembershipRetirementRestartKeepsRoundUntilAbortRevisionObserved(t *testing.T) {
	ctx, r, api, c := membershipFixture(t)
	seedLegacyRemoveMembershipJournal(r)
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	api.membership["n1"] = membershipStatus("n1", 2, "n1", "n2")
	api.membership["n2"] = membershipStatus("n2", 2, "n1", "n2")
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	r.Spec.Membership.AbortAddition = true
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	var restarted Resource
	if err := json.Unmarshal(jsonBytes(r), &restarted); err != nil {
		t.Fatal(err)
	}
	restarted.Spec.Membership.AbortAddition = false
	restarted.Spec.Membership.ReplacementPod = "learner2"
	restarted.Spec.Membership.ReplacementSecret = "learner2-credentials"
	if err := c.reconcileMembership(ctx, &restarted); err != nil {
		t.Fatal(err)
	}
	if restarted.Status.Phase != "Aborted" || restarted.Status.Membership.Add == nil {
		t.Fatalf("unconfirmed abort discarded journal after restart: %+v", restarted.Status)
	}
	api.membership["n1"] = network.MembershipStatus{NodeID: "n1", ClusterID: "source", ConfigID: 2, Voters: []quepaxa.NodeID{"n1", "n2"}, Voting: true, AbortSlot: 1}
	api.membership["n2"] = network.MembershipStatus{NodeID: "n2", ClusterID: "source", ConfigID: 2, Voters: []quepaxa.NodeID{"n1", "n2"}, Voting: true, AbortSlot: 1}
	if err := c.reconcileMembership(ctx, &restarted); err != nil {
		t.Fatal(err)
	}
	if restarted.Status.Phase != "Adding" || restarted.Status.Membership.Add != nil {
		t.Fatalf("confirmed abort did not release round after restart: %+v", restarted.Status)
	}
}

func writeRetirementOperation(ctx context.Context, c *Controller, r *Resource) error {
	add := r.Status.Membership.Add
	member, _, _, err := c.replacementMember(ctx, r.Spec.Membership.ReplacementSecret)
	if err != nil {
		return err
	}
	member.WALIdentity = add.WALIdentity
	request := network.MembershipChange{OperationID: add.OperationID, ClusterID: r.Spec.SourceClusterID, ExpectedConfigID: add.ExpectedConfigID, ExpectedAbortSlot: add.ExpectedAbortSlot, Add: &member}
	data := jsonBytes(struct {
		Request network.MembershipChange `json:"request"`
		Target  quepaxa.Cluster          `json:"target"`
	}{request, quepaxa.Cluster{ConfigID: add.ExpectedConfigID + 1, Members: []quepaxa.Member{{ID: "n1"}, {ID: "n2"}, member}}})
	return c.Bucket.Upload(ctx, path.Join(c.Prefix, r.Spec.SourceClusterID, "membership", fmt.Sprintf("%d.json", add.ExpectedConfigID)), bytes.NewReader(data))
}
