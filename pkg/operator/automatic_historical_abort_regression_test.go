package operator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func historicalMembershipFixture(t *testing.T) (context.Context, *Resource, *operatorAPI, *Controller, *autoControl, map[quepaxa.NodeID]object, map[quepaxa.NodeID]network.MembershipStatus) {
	t.Helper()
	ctx, r, api, c := membershipFixture(t)
	add := &MembershipAddJournal{
		OperationID: membershipOperationID(r, "add"), ExpectedConfigID: 2, ExpectedAbortSlot: 0,
		MemberID: "old-learner", WALIdentity: strings.Repeat("a", 64), SecretUID: "old-secret-uid", SecretResourceVer: "7",
	}
	member := voterMember("source", "old-learner", "old-peer-token")
	member.WALIdentity = add.WALIdentity
	api.membershipSecrets = map[string]object{"old-secret": {
		"immutable": true,
		"metadata":  object{"name": "old-secret", "uid": add.SecretUID, "resourceVersion": add.SecretResourceVer, "annotations": object{ownerAnnotation: "replace-n3"}},
		"data":      object{"member": base64.StdEncoding.EncodeToString(jsonBytes(member))},
	}}
	request := network.MembershipChange{OperationID: add.OperationID, ClusterID: "source", ExpectedConfigID: add.ExpectedConfigID, ExpectedAbortSlot: add.ExpectedAbortSlot, Add: &member}
	add.RequestHash = hashJSON(request)
	r.Status.Membership = &MembershipStatus{Phase: "Blocked", RemoveRequest: &network.MembershipChange{OperationID: "remove-op", ClusterID: "source", ExpectedConfigID: 1, Remove: "n3"}, Add: add}
	state := validAutoControl()
	state.Intent = &autoIntent{RecoveryName: "recover", LearnerAttempt: 1, LearnerName: "new-learner", LearnerUID: "new-learner-uid", Request: FenceRequest{
		LogicalID: "logical", BindingUID: state.BindingUID, Namespace: "default", StatefulSet: "rhiza", StatefulSetUID: state.StatefulSetUID,
		SourceClusterID: "source", OperationID: "replace-n3",
	}}
	entryTarget := FenceTarget{NodeID: "old-learner", Pod: "rhiza-2", PodUID: "old-learner-uid", WALIdentity: add.WALIdentity}
	retirementRequest := FenceRequest{
		LogicalID: "logical", BindingUID: state.BindingUID, Namespace: "default", StatefulSet: "rhiza", StatefulSetUID: state.StatefulSetUID,
		SourceClusterID: "source", OperationID: "recover-learner-fence-0", Scope: ScopeVoter, Targets: []FenceTarget{entryTarget},
	}
	hash, err := requestHash(&retirementRequest)
	if err != nil {
		t.Fatal(err)
	}
	state.Learners = []learnerIncarnation{{
		SourceClusterID: "source", OperationID: "replace-n3", CredentialName: "old-secret", CredentialUID: add.SecretUID, Target: entryTarget,
		Retirement: &learnerRetirement{Request: retirementRequest, Proof: FenceProof{
			OperationID: retirementRequest.OperationID, SourceClusterID: "source", BindingUID: state.BindingUID,
			RequestHash: hash, ProofID: "old-learner-fenced", ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true,
		}},
	}}
	pods, err := c.membershipPods(ctx, r.Spec.Membership.VoterPods)
	if err != nil {
		t.Fatal(err)
	}
	statuses := map[quepaxa.NodeID]network.MembershipStatus{}
	for _, id := range []quepaxa.NodeID{"n1", "n2"} {
		statuses[id] = network.MembershipStatus{NodeID: id, ClusterID: "source", ConfigID: 2, AbortSlot: 1, Voters: []quepaxa.NodeID{"n1", "n2"}, Voting: true}
	}
	return ctx, r, api, c, state, pods, statuses
}

func TestHistoricalAdditionAbortReconfirmsExactSettledRoundWithoutRedispatch(t *testing.T) {
	ctx, r, api, c, state, pods, statuses := historicalMembershipFixture(t)
	current := statuses["n1"]
	if err := c.confirmHistoricalMembershipAbort(ctx, r, state, pods, statuses, controllerFixtureAdminToken, current); err != nil {
		t.Fatal(err)
	}
	if len(api.aborts) != 1 || api.aborts[0].Add != nil || api.aborts[0].OperationID != r.Status.Membership.Add.OperationID {
		t.Fatalf("historical abort was not re-confirmed exactly: %+v", api.aborts)
	}
	if len(api.changes) != 0 || r.Status.Membership.Add == nil || r.Status.Membership.RemoveRequest == nil {
		t.Fatalf("historical confirmation redispatched or dropped journal state: changes=%d membership=%+v", len(api.changes), r.Status.Membership)
	}
}

func TestHistoricalAdditionAbortRejectsUnknownOrUnsettledBinding(t *testing.T) {
	for _, mutate := range []struct {
		name string
		edit func(*Resource, *autoControl, *operatorAPI, map[quepaxa.NodeID]network.MembershipStatus)
	}{
		{name: "wrong request hash", edit: func(r *Resource, _ *autoControl, _ *operatorAPI, _ map[quepaxa.NodeID]network.MembershipStatus) {
			r.Status.Membership.Add.RequestHash = strings.Repeat("f", 64)
		}},
		{name: "missing retirement proof", edit: func(_ *Resource, s *autoControl, _ *operatorAPI, _ map[quepaxa.NodeID]network.MembershipStatus) {
			s.Learners[0].Retirement = nil
		}},
		{name: "wrong immutable secret UID", edit: func(_ *Resource, _ *autoControl, api *operatorAPI, _ map[quepaxa.NodeID]network.MembershipStatus) {
			asObject(api.membershipSecrets["old-secret"]["metadata"])["uid"] = "replacement-secret-uid"
		}},
		{name: "unsettled abort", edit: func(_ *Resource, _ *autoControl, _ *operatorAPI, _ map[quepaxa.NodeID]network.MembershipStatus) {}},
	} {
		t.Run(mutate.name, func(t *testing.T) {
			ctx, r, api, c, state, pods, statuses := historicalMembershipFixture(t)
			mutate.edit(r, state, api, statuses)
			current := statuses["n1"]
			if mutate.name == "unsettled abort" {
				current.AbortSlot = 0
			}
			if err := c.confirmHistoricalMembershipAbort(ctx, r, state, pods, statuses, controllerFixtureAdminToken, current); err == nil {
				t.Fatal("invalid historical binding unexpectedly acknowledged")
			}
			if len(api.aborts) != 0 || len(api.changes) != 0 || r.Status.Membership.Add == nil || r.Status.Membership.RemoveRequest == nil {
				t.Fatalf("invalid historical state caused mutation or journal loss: aborts=%d changes=%d membership=%+v", len(api.aborts), len(api.changes), r.Status.Membership)
			}
		})
	}
}

func TestRecordMembershipAcknowledgesObservedGeneration(t *testing.T) {
	ctx, r, _, c := membershipFixture(t)
	r.Metadata["generation"] = float64(9)
	r.Status.Membership = &MembershipStatus{}
	if err := c.recordMembership(ctx, r, "Aborted", "test terminal status"); err != nil {
		t.Fatal(err)
	}
	if r.Status.ObservedGeneration != 9 || r.Status.Phase != "Aborted" || r.Status.Membership.Phase != "Aborted" {
		t.Fatalf("terminal status did not bind current generation: %+v", r.Status)
	}
}

func configureHistoricalAutomaticChild(t *testing.T, aborting bool, phase string) (context.Context, *operatorAPI, *Controller, *Resource, *MembershipAddJournal) {
	t.Helper()
	ctx, api, c, child := automaticMembershipIncarnationFixture(t, "learner", false)
	root := autoClusterResource("cluster", "binding-uid", "logical")
	state, version, err := c.loadAuto(ctx, c.autoKey(root))
	if err != nil {
		t.Fatal(err)
	}
	old := *state.Intent
	oldWAL := "old-learner-wal"
	oldSecret := api.membershipSecrets[old.LearnerSecretName]
	oldMember, oldSecretUID, oldSecretRV, err := replacementMemberFromSecret(oldSecret)
	if err != nil {
		t.Fatal(err)
	}
	oldMember.WALIdentity = oldWAL
	add := &MembershipAddJournal{
		OperationID: membershipOperationID(child, "add"), ExpectedConfigID: 2, ExpectedAbortSlot: 0,
		MemberID: oldMember.ID, WALIdentity: oldWAL, SecretUID: oldSecretUID, SecretResourceVer: oldSecretRV,
	}
	add.RequestHash = hashJSON(network.MembershipChange{OperationID: add.OperationID, ClusterID: "source", ExpectedConfigID: add.ExpectedConfigID, ExpectedAbortSlot: add.ExpectedAbortSlot, Add: &oldMember})
	retirementRequest := old.Request
	retirementRequest.Scope = ScopeVoter
	retirementRequest.OperationID = old.RecoveryName + "-learner-fence-0"
	retirementRequest.Targets = []FenceTarget{{NodeID: string(oldMember.ID), Pod: old.LearnerPodName, PodUID: old.LearnerUID, WALIdentity: oldWAL}}
	retirementHash, err := requestHash(&retirementRequest)
	if err != nil {
		t.Fatal(err)
	}
	state.Learners = []learnerIncarnation{{
		SourceClusterID: state.ActiveClusterID, OperationID: old.Request.OperationID,
		CredentialName: old.LearnerSecretName, CredentialUID: oldSecretUID,
		Target: retirementRequest.Targets[0], Retirement: &learnerRetirement{Request: retirementRequest, Proof: FenceProof{
			OperationID: retirementRequest.OperationID, SourceClusterID: retirementRequest.SourceClusterID, BindingUID: retirementRequest.BindingUID,
			RequestHash: retirementHash, ProofID: "retired-u0-proof", ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true,
		}},
	}}
	intent := old
	intent.LearnerAttempt = 1
	intent.Aborting = aborting
	intent.LearnerUID, intent.LearnerWAL = "learner-u1-uid", "new-learner-wal"
	intent.LearnerName = "inc-" + shortID(string(intent.Failed)+":"+intent.Request.OperationID+":"+intent.LearnerUID)
	intent.LearnerPodName = "learner"
	intent.LearnerSecretName = sameOrdinalSecretName(state.BindingUID, intent.LearnerUID)
	intent.LearnerSecretUID, intent.LearnerSecretRV = "u1-secret-uid", "3"
	peerToken := "u1-peer-token"
	newMember := voterMember("source", intent.LearnerName, peerToken)
	secretData := object{"member": base64.StdEncoding.EncodeToString(jsonBytes(newMember)), "RHIZA_PEER_TOKEN": base64.StdEncoding.EncodeToString([]byte(peerToken))}
	intent.LearnerSecretHash = hashJSON(secretData)
	api.membershipSecrets[intent.LearnerSecretName] = object{
		"immutable": true,
		"metadata":  object{"name": intent.LearnerSecretName, "uid": intent.LearnerSecretUID, "resourceVersion": intent.LearnerSecretRV, "annotations": object{ownerAnnotation: intent.Request.OperationID}},
		"data":      secretData,
	}
	intent.LearnerAssignmentID = ""
	state.Intent = &intent
	state.Learners = append(state.Learners, learnerIncarnation{
		SourceClusterID: state.ActiveClusterID, OperationID: intent.Request.OperationID,
		CredentialName: intent.LearnerSecretName, CredentialUID: intent.LearnerSecretUID,
		Target: FenceTarget{NodeID: intent.LearnerName, Pod: learnerPhysicalPod(&intent), PodUID: intent.LearnerUID},
	})
	child.Spec = automaticRecoveryChildSpec(root, state, state.Intent)
	state.Intent.SpecHash = hashJSON(child.Spec)
	state.Intent.LearnerAssignmentID, err = sameOrdinalAssignmentDigest(state, state.Intent)
	if err != nil {
		t.Fatal(err)
	}
	child.Metadata["annotations"] = object{automaticOwner: state.BindingUID, automaticLogical: root.Spec.LogicalID}
	child.Status.Membership = &MembershipStatus{Phase: phase, RemoveRequest: &network.MembershipChange{
		OperationID: membershipOperationID(child, "remove"), ClusterID: "source", ExpectedConfigID: 1, Remove: child.Spec.Membership.Remove,
		Fence: &network.MembershipFence{NodeID: child.Spec.Membership.Fence.NodeID, WALIdentity: child.Spec.Membership.Fence.WALIdentity,
			WorkloadUID: child.Spec.Membership.Fence.WorkloadUID, Confirmed: true, Evidence: child.Spec.Membership.Fence.Evidence},
	}, Add: add}
	newPod := api.replacement
	metadata := asObject(newPod["metadata"])
	metadata["uid"] = intent.LearnerUID
	metadata["annotations"] = object{
		fencingProtocolAnnotation: "1", fencingRootAnnotation: state.BindingUID, ownerAnnotation: intent.Request.OperationID,
		fencingCredentialAnnotation: intent.LearnerSecretUID,
	}
	labels := asObject(metadata["labels"])
	if labels == nil {
		labels = object{}
		metadata["labels"] = labels
	}
	labels["rhiza.mrchypark.dev/automatic-learner"] = intent.LearnerName
	metadata["ownerReferences"] = []any{object{"apiVersion": "apps/v1", "uid": state.StatefulSetUID, "name": "rhiza", "kind": "StatefulSet", "controller": true}}
	api.replacement = newPod
	api.learnerID = intent.LearnerName
	api.membership[intent.LearnerName] = network.MembershipStatus{NodeID: quepaxa.NodeID(intent.LearnerName), ClusterID: "source", WALIdentity: intent.LearnerWAL}
	api.membership["n1"] = network.MembershipStatus{NodeID: "n1", ClusterID: "source", ConfigID: 2, AbortSlot: 1, Voters: []quepaxa.NodeID{"n1", "n2"}, Voting: true}
	api.membership["n2"] = network.MembershipStatus{NodeID: "n2", ClusterID: "source", ConfigID: 2, AbortSlot: 1, Voters: []quepaxa.NodeID{"n1", "n2"}, Voting: true}
	if err := c.saveAuto(ctx, root, state, version); err != nil {
		t.Fatal(err)
	}
	return ctx, api, c, child, add
}

func TestHistoricalSettledAddReconcileWritesCurrentGenerationAckAndPreservesJournals(t *testing.T) {
	ctx, api, c, child, add := configureHistoricalAutomaticChild(t, true, "Blocked")
	remove := hashJSON(child.Status.Membership.RemoveRequest)
	if err := c.reconcileMembership(ctx, child); err != nil {
		t.Fatal(err)
	}
	if child.Status.Phase != "Aborted" || child.Status.ObservedGeneration != number(child.Metadata["generation"]) ||
		child.Status.Membership.Phase != "Aborted" || child.Status.Membership.Add == nil || hashJSON(child.Status.Membership.RemoveRequest) != remove {
		t.Fatalf("historical ADD was not re-evaluated into a current-generation ACK while retaining journal: status=%+v membership=%+v", child.Status, child.Status.Membership)
	}
	if len(api.aborts) != 1 || api.aborts[0].Add != nil || api.aborts[0].OperationID != add.OperationID || len(api.changes) != 0 {
		t.Fatalf("historical reconciliation dispatched ADD or failed to confirm exact abort: aborts=%+v changes=%+v", api.aborts, api.changes)
	}
	root := autoClusterResource("cluster", "binding-uid", "logical")
	state, version, err := c.loadAuto(ctx, c.autoKey(root))
	if err != nil {
		t.Fatal(err)
	}
	var sts object
	if err := c.Kube.Get(ctx, c.stsPath("rhiza"), &sts); err != nil {
		t.Fatal(err)
	}
	var fenceCalls int
	c.Fencer = &countingFencer{count: &fenceCalls}
	handled, err := c.handleAutomaticLearner(ctx, root, state, version, sts, child)
	if !handled || (err != nil && !apiNotFound(err)) || fenceCalls != 1 {
		t.Fatalf("verified historical ADD acknowledgment blocked fencing of the already-checkpointed current learner: handled=%t fences=%d err=%v", handled, fenceCalls, err)
	}
}

func TestHistoricalSettledAddAllowsCurrentNeverBoundLearnerWithoutWAL(t *testing.T) {
	ctx, api, c, child, _ := configureHistoricalAutomaticChild(t, true, "Blocked")
	if err := c.reconcileMembership(ctx, child); err != nil {
		t.Fatal(err)
	}
	if child.Status.Phase != "Aborted" || child.Status.Membership.Add == nil || len(api.aborts) != 1 {
		t.Fatalf("historical ADD was not acknowledged without dropping its journal: status=%+v aborts=%+v", child.Status, api.aborts)
	}
	root := autoClusterResource("cluster", "binding-uid", "logical")
	state, version, err := c.loadAuto(ctx, c.autoKey(root))
	if err != nil {
		t.Fatal(err)
	}
	state.Intent.LearnerWAL = ""
	for i := range state.Learners {
		if state.Learners[i].Target.PodUID == state.Intent.LearnerUID {
			state.Learners[i].Target.WALIdentity = ""
		}
	}
	if err := c.saveAuto(ctx, root, state, version); err != nil {
		t.Fatal(err)
	}
	state, version, err = c.loadAuto(ctx, c.autoKey(root))
	if err != nil {
		t.Fatal(err)
	}
	var got FenceRequest
	fenceCalls := 0
	c.Fencer = membershipRegressionFencer(func(ctx context.Context, request FenceRequest) (*FenceProof, error) {
		got = request
		return (&countingFencer{count: &fenceCalls}).Fence(ctx, request)
	})
	var sts object
	if err := c.Kube.Get(ctx, c.stsPath("rhiza"), &sts); err != nil {
		t.Fatal(err)
	}
	attemptBefore := state.Intent.LearnerAttempt
	learnerUID := state.Intent.LearnerUID
	handled, err := c.handleAutomaticLearner(ctx, root, state, version, sts, child)
	if !handled || fenceCalls != 1 || len(got.Targets) != 1 ||
		got.Targets[0].PodUID != learnerUID || got.Targets[0].WALIdentity != "" {
		t.Fatalf("verified historical ADD incorrectly required a never-bound current WAL: handled=%t fences=%d request=%+v err=%v", handled, fenceCalls, got, err)
	}
	saved, _, loadErr := c.loadAuto(ctx, c.autoKey(root))
	if loadErr != nil || saved.Intent.LearnerAttempt != attemptBefore+1 || len(saved.Learners) < 2 {
		t.Fatalf("empty-WAL retirement proof was not durably recorded before expected fake status error: state=%+v err=%v loadErr=%v", saved, err, loadErr)
	}
}

func TestNeverJournaledLearnerMayFenceWithoutObservedWAL(t *testing.T) {
	ctx, _, c, child, _ := configureHistoricalAutomaticChild(t, true, "Aborted")
	child.Status.Membership.Add = nil
	child.Status.Phase = "Aborted"
	child.Status.ObservedGeneration = number(child.Metadata["generation"])
	child.Status.Membership.Phase = "Aborted"
	root := autoClusterResource("cluster", "binding-uid", "logical")
	state, version, err := c.loadAuto(ctx, c.autoKey(root))
	if err != nil {
		t.Fatal(err)
	}
	state.Intent.LearnerWAL = ""
	for i := range state.Learners {
		if state.Learners[i].Target.PodUID == state.Intent.LearnerUID {
			state.Learners[i].Target.WALIdentity = ""
		}
	}
	if err := c.saveAuto(ctx, root, state, version); err != nil {
		t.Fatal(err)
	}
	state, version, err = c.loadAuto(ctx, c.autoKey(root))
	if err != nil {
		t.Fatal(err)
	}
	var got FenceRequest
	fenceCalls := 0
	c.Fencer = membershipRegressionFencer(func(ctx context.Context, request FenceRequest) (*FenceProof, error) {
		got = request
		return (&countingFencer{count: &fenceCalls}).Fence(ctx, request)
	})
	var sts object
	if err := c.Kube.Get(ctx, c.stsPath("rhiza"), &sts); err != nil {
		t.Fatal(err)
	}
	attemptBefore := state.Intent.LearnerAttempt
	learnerUID := state.Intent.LearnerUID
	handled, err := c.handleAutomaticLearner(ctx, root, state, version, sts, child)
	if !handled || fenceCalls != 1 || len(got.Targets) != 1 ||
		got.Targets[0].PodUID != learnerUID || got.Targets[0].WALIdentity != "" {
		t.Fatalf("never-journaled learner was incorrectly blocked on an observed WAL: handled=%t fences=%d request=%+v err=%v", handled, fenceCalls, got, err)
	}
	saved, _, loadErr := c.loadAuto(ctx, c.autoKey(root))
	if loadErr != nil || saved.Intent.LearnerAttempt != attemptBefore+1 || len(saved.Learners) < 2 {
		t.Fatalf("never-bound empty-WAL retirement proof was not durably recorded before expected fake status error: state=%+v err=%v loadErr=%v", saved, err, loadErr)
	}
}

func TestCurrentAddWithUncheckpointedIntentWALIsNotHistorical(t *testing.T) {
	ctx, api, c, child, _ := configureHistoricalAutomaticChild(t, false, "Blocked")
	root := autoClusterResource("cluster", "binding-uid", "logical")
	state, version, err := c.loadAuto(ctx, c.autoKey(root))
	if err != nil {
		t.Fatal(err)
	}
	if state.Intent == nil || state.Intent.LearnerWAL == "" || child.Status.Membership == nil {
		t.Fatalf("fixture did not start with a checkpointed learner WAL: %+v", state.Intent)
	}
	currentWAL := state.Intent.LearnerWAL
	if api.membership[state.Intent.LearnerName].WALIdentity != currentWAL {
		t.Fatalf("fixture learner status does not supply the expected WAL: status=%+v intent=%+v", api.membership[state.Intent.LearnerName], state.Intent)
	}
	state.Intent.LearnerWAL = ""
	for i := range state.Learners {
		if state.Learners[i].Target.PodUID == state.Intent.LearnerUID {
			state.Learners[i].Target.WALIdentity = ""
		}
	}
	// Let the production child reconciler create its ADD journal from the
	// exact current immutable Secret and authenticated learner status while
	// the parent has not yet observed/checkpointed that WAL.
	child.Status.Membership.Add = nil
	child.Status.Membership.Phase = "Adding"
	child.Status.Phase = "Recovering"
	if err := c.saveAuto(ctx, root, state, version); err != nil {
		t.Fatal(err)
	}
	if err := c.reconcileMembership(ctx, child); err != nil {
		t.Fatalf("production reconciliation did not create the current learner ADD journal: %v", err)
	}
	add := child.Status.Membership.Add
	if add == nil || string(add.MemberID) != state.Intent.LearnerName || add.WALIdentity != currentWAL || add.SecretUID != state.Intent.LearnerSecretUID || add.SecretResourceVer != state.Intent.LearnerSecretRV {
		t.Fatalf("production child did not journal the exact current ADD identity: %+v intent=%+v", add, state.Intent)
	}
	if len(api.changes) == 0 {
		t.Fatal("production current ADD reconciliation did not pass through its journaled membership boundary")
	}

	// The parent's durable current WAL remains unobserved while the child has
	// journaled that exact ADD. The child must still acknowledge its abort.
	state, version, err = c.loadAuto(ctx, c.autoKey(root))
	if err != nil {
		t.Fatal(err)
	}
	if state.Intent.LearnerWAL != "" {
		t.Fatalf("parent unexpectedly checkpointed learner WAL before the ADD boundary: %+v", state.Intent)
	}
	state.Intent.Aborting = true
	child.Spec = automaticRecoveryChildSpec(root, state, state.Intent)
	state.Intent.SpecHash = hashJSON(child.Spec)
	child.Metadata["generation"] = float64(2)
	if err := c.saveAuto(ctx, root, state, version); err != nil {
		t.Fatal(err)
	}
	if err := c.reconcileMembership(ctx, child); err != nil {
		t.Fatal(err)
	}
	if child.Status.Phase != "Aborted" || child.Status.ObservedGeneration != 2 || len(api.aborts) != 1 ||
		api.aborts[0].OperationID != add.OperationID || api.aborts[0].Add != nil || len(api.changes) == 0 {
		t.Fatalf("current ADD with an uncheckpointed parent WAL was misclassified instead of taking current abort path: phase=%q message=%q observed=%v membership=%+v aborts=%+v changes=%+v", child.Status.Phase, child.Status.Message, child.Status.ObservedGeneration, child.Status.Membership, api.aborts, api.changes)
	}

	var fenceCalls int
	c.Fencer = &countingFencer{count: &fenceCalls}
	state, version, err = c.loadAuto(ctx, c.autoKey(root))
	if err != nil {
		t.Fatal(err)
	}
	var sts object
	if err := c.Kube.Get(ctx, c.stsPath("rhiza"), &sts); err != nil {
		t.Fatal(err)
	}
	handled, err := c.handleAutomaticLearner(ctx, root, state, version, sts, child)
	// This fixture intentionally exposes the recovery child and StatefulSet but
	// not the parent RhizaCluster status endpoint. The control CAS happens before
	// the expected status 404; inspect the durable record below.
	if !handled || (err != nil && !apiNotFound(err)) || fenceCalls != 0 {
		t.Fatalf("missing WAL was not checkpointed and returned before fencing: handled=%t fences=%d err=%v", handled, fenceCalls, err)
	}
	state, version, err = c.loadAuto(ctx, c.autoKey(root))
	if err != nil {
		t.Fatal(err)
	}
	if state.Intent.LearnerWAL != currentWAL || state.Intent.LearnerUID != "learner-u1-uid" || state.Intent.LearnerAttempt != 1 {
		t.Fatalf("authenticated WAL checkpoint changed identity or attempt: %+v", state.Intent)
	}
	currentHistory := 0
	for _, entry := range state.Learners {
		if entry.Target.PodUID == state.Intent.LearnerUID {
			currentHistory++
			if entry.Target.WALIdentity != currentWAL || entry.Retirement != nil {
				t.Fatalf("current history WAL was not checkpointed before retirement: %+v", entry)
			}
		}
	}
	if currentHistory != 1 {
		t.Fatalf("current learner history matches=%d, want exactly one", currentHistory)
	}

	handled, err = c.handleAutomaticLearner(ctx, root, state, version, sts, child)
	if !handled || (err != nil && !apiNotFound(err)) || fenceCalls != 1 {
		t.Fatalf("fencing did not follow the durable WAL checkpoint on the next reconcile: handled=%t fences=%d err=%v", handled, fenceCalls, err)
	}
	state, version, err = c.loadAuto(ctx, c.autoKey(root))
	if err != nil {
		t.Fatal(err)
	}
	if state.Intent.LearnerAttempt != 2 || state.Intent.LearnerWAL != "" || state.Intent.LearnerUID != "" {
		t.Fatalf("learner identity rotated before/without durable retirement: %+v", state.Intent)
	}
	retired := false
	for _, entry := range state.Learners {
		if entry.Target.PodUID == "learner-u1-uid" && entry.Target.WALIdentity == currentWAL && entry.Retirement != nil &&
			entry.Retirement.Request.Targets[0].WALIdentity == currentWAL && entry.Retirement.Proof.ProofID != "" {
			retired = true
		}
	}
	if !retired {
		t.Fatal("exact current WAL was not retained with its retirement proof before identity rotation")
	}

	// The fake membership server records RPCs but does not advance status after
	// the abort; model the successful abort-slot confirmation before reset.
	ackN1 := membershipStatus("n1", 2, "n1", "n2")
	ackN1.AbortSlot = add.ExpectedAbortSlot + 1
	api.membership["n1"] = ackN1
	ackN2 := membershipStatus("n2", 2, "n1", "n2")
	ackN2.AbortSlot = add.ExpectedAbortSlot + 1
	api.membership["n2"] = ackN2
	newSpec := automaticRecoveryChildSpec(root, state, state.Intent)
	state.Intent.SpecHash = hashJSON(newSpec)
	state.Intent.LearnerAssignmentID = ""
	if err := c.saveAuto(ctx, root, state, version); err != nil {
		t.Fatal(err)
	}
	child.Spec = newSpec
	if err := c.reconcileMembership(ctx, child); err != nil {
		t.Fatal(err)
	}
	addRPCs := 0
	for _, change := range api.changes {
		if change.Add != nil {
			addRPCs++
		}
	}
	if child.Status.Membership.Add != nil || child.Status.Membership.RemoveRequest == nil || addRPCs != 0 || len(api.aborts) != 2 {
		t.Fatalf("historical ADD was not reset without losing REMOVE or redispatching: phase=%s message=%q membership=%+v changes=%+v aborts=%+v", child.Status.Phase, child.Status.Message, child.Status.Membership, api.changes, api.aborts)
	}
}

func TestStandaloneCurrentLearnerWALCheckpointUsesRetainedHistory(t *testing.T) {
	_, _, _, _, state, _, _ := historicalMembershipFixture(t)
	intent := &autoIntent{
		Request:     FenceRequest{OperationID: "standalone-operation"},
		LearnerName: "standalone-learner", LearnerUID: "standalone-pod-uid",
		LearnerSecretName: "standalone-learner-credentials",
	}
	state.Learners = []learnerIncarnation{{
		SourceClusterID: state.ActiveClusterID, OperationID: intent.Request.OperationID,
		CredentialName: intent.LearnerSecretName, CredentialUID: "standalone-secret-uid",
		Target: FenceTarget{NodeID: intent.LearnerName, Pod: intent.LearnerName, PodUID: intent.LearnerUID},
	}}
	if err := checkpointCurrentLearnerWAL(state, intent, "standalone-current-wal"); err != nil {
		t.Fatalf("standalone WAL checkpoint incorrectly required same-ordinal intent Secret fields: %v", err)
	}
	if state.Learners[0].Target.WALIdentity != "standalone-current-wal" || intent.LearnerSecretUID != "" ||
		intent.LearnerSecretRV != "" || intent.LearnerSecretHash != "" {
		t.Fatalf("standalone checkpoint did not use retained identity history: intent=%+v history=%+v", intent, state.Learners[0])
	}
}

func TestCurrentAddRejectsWALConflictingWithRetainedHistory(t *testing.T) {
	ctx, api, c, child, add := configureHistoricalAutomaticChild(t, true, "Blocked")
	root := autoClusterResource("cluster", "binding-uid", "logical")
	state, _, err := c.loadAuto(ctx, c.autoKey(root))
	if err != nil {
		t.Fatal(err)
	}
	member, uid, rv, err := c.replacementMember(ctx, state.Intent.LearnerSecretName)
	if err != nil {
		t.Fatal(err)
	}
	member.WALIdentity = "wrong-current-wal"
	add.MemberID, add.WALIdentity, add.SecretUID, add.SecretResourceVer = member.ID, member.WALIdentity, uid, rv
	add.RequestHash = hashJSON(network.MembershipChange{
		OperationID: add.OperationID, ClusterID: child.Spec.SourceClusterID,
		ExpectedConfigID: add.ExpectedConfigID, ExpectedAbortSlot: add.ExpectedAbortSlot, Add: &member,
	})
	child.Status.Membership.Add = add
	for i := range state.Learners {
		entry := &state.Learners[i]
		if entry.Target.PodUID == state.Intent.LearnerUID && entry.Target.NodeID == state.Intent.LearnerName {
			entry.Target.WALIdentity = "known-different-wal"
		}
	}
	if _, err := c.membershipAddMatchesIntent(ctx, child, state, add, state.Intent); err == nil {
		t.Fatal("current ADD was accepted despite a conflicting retained WAL identity")
	}
	if err := c.reconcileMembership(ctx, child); err != nil {
		t.Fatal(err)
	}
	if child.Status.Phase != "Blocked" || len(api.aborts) != 0 || len(api.changes) != 0 {
		t.Fatalf("WAL-conflicting current ADD was acknowledged or redispatched: status=%+v aborts=%+v changes=%+v", child.Status, api.aborts, api.changes)
	}
}

func TestCurrentAddRejectsKnownIntentWALWhenRetainedHistoryWALIsEmpty(t *testing.T) {
	ctx, api, c, child, _ := configureHistoricalAutomaticChild(t, true, "Blocked")
	root := autoClusterResource("cluster", "binding-uid", "logical")
	state, _, err := c.loadAuto(ctx, c.autoKey(root))
	if err != nil {
		t.Fatal(err)
	}
	intent := state.Intent
	if intent.LearnerWAL == "" {
		t.Fatal("fixture must have a known parent WAL")
	}
	var currentHistory *learnerIncarnation
	for i := range state.Learners {
		entry := &state.Learners[i]
		if entry.Target.PodUID == intent.LearnerUID && entry.Target.NodeID == intent.LearnerName {
			currentHistory = entry
		}
	}
	if currentHistory == nil || currentHistory.Target.WALIdentity != "" {
		t.Fatalf("fixture must have empty retained-history WAL: %+v", currentHistory)
	}

	member, uid, rv, err := c.replacementMember(ctx, intent.LearnerSecretName)
	if err != nil {
		t.Fatal(err)
	}
	wrongWAL := "different-authenticated-current-wal"
	member.WALIdentity = wrongWAL
	add := &MembershipAddJournal{
		OperationID: membershipOperationID(child, "add"), ExpectedConfigID: 2, ExpectedAbortSlot: 0,
		MemberID: member.ID, WALIdentity: wrongWAL, SecretUID: uid, SecretResourceVer: rv,
	}
	request := network.MembershipChange{
		OperationID: add.OperationID, ClusterID: child.Spec.SourceClusterID,
		ExpectedConfigID: add.ExpectedConfigID, ExpectedAbortSlot: add.ExpectedAbortSlot, Add: &member,
	}
	add.RequestHash = hashJSON(request)
	child.Status.Membership.Add = add
	if add.RequestHash != hashJSON(request) {
		t.Fatal("fixture ADD does not carry the exact complete current request hash")
	}
	if _, err := c.membershipAddMatchesIntent(ctx, child, state, add, intent); err == nil {
		t.Fatal("current ADD with exact request hash bypassed the known intent WAL mismatch because history WAL was empty")
	}
	if err := c.reconcileMembership(ctx, child); err != nil {
		t.Fatal(err)
	}
	if child.Status.Phase != "Blocked" || child.Status.Membership.Phase == "Aborted" || len(api.aborts) != 0 || len(api.changes) != 0 {
		t.Fatalf("known parent WAL mismatch was acknowledged or redispatched: status=%+v aborts=%+v changes=%+v", child.Status, api.aborts, api.changes)
	}
}

func TestStandaloneCurrentAddJournalCheckpointsWALBeforeFencing(t *testing.T) {
	ctx, api, c, child, _ := configureHistoricalAutomaticChild(t, false, "Blocked")
	root := autoClusterResource("cluster", "binding-uid", "logical")
	state, version, err := c.loadAuto(ctx, c.autoKey(root))
	if err != nil {
		t.Fatal(err)
	}
	intent := state.Intent
	priorLearnerName, priorLearnerUID := intent.LearnerName, intent.LearnerUID
	intent.LearnerName = "learner2"
	intent.LearnerUID = "standalone-u1-pod-uid"
	intent.LearnerPodName = ""
	intent.LearnerWAL = ""
	intent.LearnerSecretName = "learner2-credentials"
	intent.LearnerSecretUID, intent.LearnerSecretRV, intent.LearnerSecretHash = "", "", ""
	intent.LearnerAssignmentID = ""
	intent.Aborting = false
	for i := range state.Learners {
		entry := &state.Learners[i]
		if entry.Target.NodeID == priorLearnerName && entry.Target.PodUID == priorLearnerUID {
			entry.Target.NodeID, entry.Target.Pod, entry.Target.PodUID = intent.LearnerName, intent.LearnerName, intent.LearnerUID
			entry.Target.WALIdentity = ""
			entry.CredentialName = intent.LearnerSecretName
			entry.CredentialUID = "standalone-u1-secret-uid"
		}
	}
	var currentHistory *learnerIncarnation
	for i := range state.Learners {
		entry := &state.Learners[i]
		if entry.Target.NodeID == intent.LearnerName && entry.Target.PodUID == intent.LearnerUID {
			currentHistory = entry
		}
	}
	if currentHistory == nil {
		t.Fatal("standalone learner lacks its canonical retained identity history")
	}

	peerToken := "standalone-u1-peer-token"
	member := voterMember(state.ActiveClusterID, intent.LearnerName, peerToken)
	secretData := object{
		"member":           base64.StdEncoding.EncodeToString(jsonBytes(member)),
		"RHIZA_PEER_TOKEN": base64.StdEncoding.EncodeToString([]byte(peerToken)),
	}
	api.membershipSecrets[intent.LearnerSecretName] = object{
		"immutable": true,
		"metadata":  object{"name": intent.LearnerSecretName, "uid": currentHistory.CredentialUID, "resourceVersion": "9", "annotations": object{ownerAnnotation: intent.Request.OperationID}},
		"data":      secretData,
	}
	api.learnerID = intent.LearnerName
	currentWAL := "standalone-u1-authenticated-wal"
	api.membership[intent.LearnerName] = network.MembershipStatus{NodeID: quepaxa.NodeID(intent.LearnerName), ClusterID: state.ActiveClusterID, WALIdentity: currentWAL}
	api.replacement2 = deepCopyObject(api.replacement)
	metadata := asObject(api.replacement2["metadata"])
	metadata["name"], metadata["uid"] = intent.LearnerName, intent.LearnerUID
	metadata["ownerReferences"] = []any{object{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "rhiza", "uid": state.StatefulSetUID, "controller": false}}
	metadata["annotations"] = object{
		fencingProtocolAnnotation: "1", fencingRootAnnotation: state.BindingUID,
		fencingCredentialAnnotation: currentHistory.CredentialUID, ownerAnnotation: intent.Request.OperationID,
	}
	metadata["labels"] = object{"rhiza.mrchypark.dev/automatic-learner": intent.LearnerName}
	templateMetadata := asObject(nested(api.sts, "spec", "template", "metadata"))
	if templateMetadata == nil {
		templateMetadata = object{}
		asObject(nested(api.sts, "spec", "template"))["metadata"] = templateMetadata
	}
	templateMetadata["annotations"] = object{fencingProtocolAnnotation: "1"}

	rootList := []ClusterResource{*root}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodGet && req.URL.Path == "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/default/rhizaclusters" {
			_ = json.NewEncoder(w).Encode(struct {
				Items []ClusterResource `json:"items"`
			}{Items: rootList})
			return
		}
		api.serve(w, req)
	}))
	t.Cleanup(server.Close)
	c.Kube.BaseURL, c.Kube.Client = server.URL, server.Client()
	child.Spec = automaticRecoveryChildSpec(root, state, intent)
	intent.SpecHash = hashJSON(child.Spec)
	child.Metadata["generation"] = float64(2)
	child.Status.Membership.Add = nil
	child.Status.Membership.Phase = "Adding"
	child.Status.Phase = "Recovering"
	child.Metadata["annotations"] = object{automaticOwner: state.BindingUID, automaticLogical: root.Spec.LogicalID}
	if err := c.saveAuto(ctx, root, state, version); err != nil {
		t.Fatal(err)
	}

	// The child controller must create the journal from the standalone Pod,
	// immutable credential and authenticated live WAL while both canonical WAL
	// checkpoints are still empty.
	if err := c.reconcileMembership(ctx, child); err != nil {
		t.Fatalf("production standalone ADD reconciliation: %v", err)
	}
	add := child.Status.Membership.Add
	if add == nil || string(add.MemberID) != intent.LearnerName || add.WALIdentity != currentWAL ||
		add.SecretUID != currentHistory.CredentialUID || add.SecretResourceVer != "9" || len(api.changes) == 0 {
		t.Fatalf("production child did not journal the standalone learner identity: add=%+v changes=%+v", add, api.changes)
	}
	state, version, err = c.loadAuto(ctx, c.autoKey(root))
	if err != nil {
		t.Fatal(err)
	}
	if state.Intent.LearnerWAL != "" {
		t.Fatalf("parent WAL was checkpointed before observing the standalone ADD: %+v", state.Intent)
	}
	for _, entry := range state.Learners {
		if entry.Target.PodUID == intent.LearnerUID && entry.Target.WALIdentity != "" {
			t.Fatalf("history WAL was checkpointed before observing the standalone ADD: %+v", entry)
		}
	}

	state.Intent.Aborting = true
	child.Spec = automaticRecoveryChildSpec(root, state, state.Intent)
	state.Intent.SpecHash = hashJSON(child.Spec)
	child.Metadata["generation"] = float64(3)
	if err := c.saveAuto(ctx, root, state, version); err != nil {
		t.Fatal(err)
	}
	if err := c.reconcileMembership(ctx, child); err != nil {
		t.Fatalf("production standalone abort reconciliation: %v", err)
	}
	if child.Status.Phase != "Aborted" || child.Status.ObservedGeneration != 3 || len(api.aborts) != 1 || api.aborts[0].Add != nil {
		t.Fatalf("standalone current ADD was not abort-acknowledged before fencing: status=%+v aborts=%+v", child.Status, api.aborts)
	}

	var fenceCalls int
	c.Fencer = &countingFencer{count: &fenceCalls}
	var sts object
	if err := c.Kube.Get(ctx, c.stsPath("rhiza"), &sts); err != nil {
		t.Fatal(err)
	}
	state, version, err = c.loadAuto(ctx, c.autoKey(root))
	if err != nil {
		t.Fatal(err)
	}
	handled, err := c.handleAutomaticLearner(ctx, root, state, version, sts, child)
	if !handled || (err != nil && !apiNotFound(err)) || fenceCalls != 0 {
		t.Fatalf("standalone exact ADD did not checkpoint both WALs before fencing: handled=%t fences=%d err=%v", handled, fenceCalls, err)
	}
	saved, version, err := c.loadAuto(ctx, c.autoKey(root))
	if err != nil {
		t.Fatal(err)
	}
	if saved.Intent.LearnerWAL != currentWAL {
		t.Fatalf("standalone parent intent WAL was not checkpointed: %+v", saved.Intent)
	}
	matchedHistory := 0
	for _, entry := range saved.Learners {
		if entry.Target.NodeID == saved.Intent.LearnerName && entry.Target.PodUID == saved.Intent.LearnerUID {
			matchedHistory++
			if entry.Target.WALIdentity != currentWAL || entry.CredentialUID != "standalone-u1-secret-uid" {
				t.Fatalf("standalone retained history was not checkpointed from the exact ADD: %+v", entry)
			}
		}
	}
	if matchedHistory != 1 || fenceCalls != 0 {
		t.Fatalf("standalone WAL checkpoint was ambiguous or fenced early: matches=%d fences=%d", matchedHistory, fenceCalls)
	}
	handled, err = c.handleAutomaticLearner(ctx, root, saved, version, sts, child)
	if !handled || (err != nil && !apiNotFound(err)) || fenceCalls != 1 {
		t.Fatalf("next reconcile did not fence only after standalone WAL CAS: handled=%t fences=%d err=%v", handled, fenceCalls, err)
	}
}

func TestBlockedHistoricalAddResetReturnsBeforeNewAddDispatch(t *testing.T) {
	ctx, api, c, child, oldAdd := configureHistoricalAutomaticChild(t, false, "Blocked")
	remove := hashJSON(child.Status.Membership.RemoveRequest)
	if err := c.reconcileMembership(ctx, child); err != nil {
		t.Fatal(err)
	}
	if child.Status.Phase != "Adding" || child.Status.ObservedGeneration != number(child.Metadata["generation"]) ||
		child.Status.Membership.Add != nil || hashJSON(child.Status.Membership.RemoveRequest) != remove || len(api.changes) != 0 || len(api.aborts) != 1 {
		t.Fatalf("historical ADD reset did not CAS/return preserving REMOVE before any new ADD: status=%+v membership=%+v changes=%+v aborts=%+v", child.Status, child.Status.Membership, api.changes, api.aborts)
	}
	if err := c.reconcileMembership(ctx, child); err != nil {
		t.Fatal(err)
	}
	if child.Status.Membership.Add == nil || child.Status.Membership.Add.MemberID == oldAdd.MemberID || len(api.changes) != 1 || api.changes[0].Add != nil {
		t.Fatalf("next reconcile did not journal the current learner only after reset: membership=%+v changes=%+v", child.Status.Membership, api.changes)
	}
	if err := c.reconcileMembership(ctx, child); err != nil {
		t.Fatal(err)
	}
	if len(api.changes) != 2 || api.changes[1].Add == nil || string(api.changes[1].Add.ID) != api.learnerID {
		t.Fatalf("current learner ADD did not dispatch on the later reconcile: changes=%+v", api.changes)
	}
}

func TestHistoricalAddCommitOrAbortErrorNeverAcknowledgesOrRedispatches(t *testing.T) {
	for _, scenario := range []string{"committed current configuration", "abort API failure"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, api, c, child, add := configureHistoricalAutomaticChild(t, true, "Blocked")
			if scenario == "committed current configuration" {
				for _, id := range []string{"n1", "n2"} {
					api.membership[id] = network.MembershipStatus{NodeID: quepaxa.NodeID(id), ClusterID: "source", ConfigID: 3, AbortSlot: 0, Voters: []quepaxa.NodeID{"n1", "n2", add.MemberID}, Voting: true}
				}
			} else {
				api.membership = map[string]network.MembershipStatus{
					"n1": {NodeID: "n1", ClusterID: "source", ConfigID: 2, AbortSlot: 1, Voters: []quepaxa.NodeID{"n1", "n2"}, Voting: true},
					"n2": {NodeID: "n2", ClusterID: "source", ConfigID: 2, AbortSlot: 1, Voters: []quepaxa.NodeID{"n1", "n2"}, Voting: true},
				}
				transport := &historicalAbortFailureTransport{base: http.DefaultTransport}
				c.HTTP = &http.Client{Transport: transport}
				defer func() {
					if transport.abortRequests != 1 {
						t.Errorf("historical abort API attempts=%d, want 1", transport.abortRequests)
					}
				}()
			}
			if err := c.reconcileMembership(ctx, child); err != nil {
				t.Fatal(err)
			}
			if child.Status.Phase == "Aborted" || child.Status.Membership.Phase == "Aborted" || child.Status.Membership.Add == nil || len(api.changes) != 0 {
				t.Fatalf("unsettled/committed historical ADD was acknowledged or redispatched: status=%+v changes=%+v", child.Status, api.changes)
			}
			if scenario == "committed current configuration" && len(api.aborts) != 0 {
				t.Fatalf("committed ADD attempted abort confirmation: %+v", api.aborts)
			}
			if scenario == "abort API failure" && len(api.aborts) != 0 {
				t.Fatalf("failed historical abort reached the API fixture unexpectedly: %+v", api.aborts)
			}
		})
	}
}

type historicalAbortFailureTransport struct {
	base          http.RoundTripper
	abortRequests int
}

func (t *historicalAbortFailureTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Path == "/membership/abort" {
		t.abortRequests++
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
	}
	return t.base.RoundTrip(req)
}
