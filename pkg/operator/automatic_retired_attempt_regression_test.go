package operator

import (
	"context"
	"strings"
	"testing"
)

func retiredLearnerAttemptFixture(t *testing.T, attempt, fenceAttempt int, operation string, proofMode string) (context.Context, *Controller, *autoControl, object) {
	t.Helper()
	ctx, api, c, _ := automaticMembershipIncarnationFixture(t, "learner", false)
	root := autoClusterResource("cluster", "binding-uid", "logical")
	state, _, err := c.loadAuto(ctx, c.autoKey(root))
	if err != nil {
		t.Fatal(err)
	}
	intent := state.Intent
	intent.LearnerAttempt = attempt
	intent.LearnerName = ""
	intent.LearnerPodName = ""
	intent.LearnerUID = ""

	old, ok := state.Identities[intent.Failed]
	if !ok {
		t.Fatal("fixture has no canonical old-voter identity")
	}
	historyTarget := FenceTarget{
		NodeID: "inc-" + shortID(string(intent.Failed)+":"+intent.Request.OperationID+":"+"learner-u1-uid"),
		Pod:    old.Pod, PodUID: "learner-u1-uid", WALIdentity: strings.Repeat("c", 64),
	}
	retirementRequest := FenceRequest{
		LogicalID: intent.Request.LogicalID, BindingUID: state.BindingUID, Namespace: c.Kube.Namespace,
		StatefulSet: intent.Request.StatefulSet, StatefulSetUID: state.StatefulSetUID,
		SourceClusterID: state.ActiveClusterID, OperationID: operation, Scope: ScopeVoter,
		Targets: []FenceTarget{historyTarget},
	}
	count := 0
	retirementProof, err := (&countingFencer{count: &count}).Fence(ctx, retirementRequest)
	if err != nil {
		t.Fatal(err)
	}
	if proofMode == "missing" {
		retirementProof = nil
	} else if proofMode == "wrong" {
		retirementProof.RequestHash = strings.Repeat("e", 64)
	}
	entry := learnerIncarnation{
		SourceClusterID: state.ActiveClusterID, OperationID: intent.Request.OperationID, Target: historyTarget,
	}
	if retirementProof != nil {
		entry.Retirement = &learnerRetirement{Request: retirementRequest, Proof: *retirementProof}
	}
	state.Learners = []learnerIncarnation{entry}
	asObject(api.replacement["metadata"])["uid"] = "learner-u1-uid"
	sts := learnerSTS(t)
	asObject(sts["metadata"])["uid"] = state.StatefulSetUID
	return ctx, c, state, sts
}

func TestAutomaticSameOrdinalAcceptsOnlyEarlierRetirementAttempts(t *testing.T) {
	tests := []struct {
		name         string
		attempt      int
		fenceAttempt int
		operation    string
		proofMode    string
		wantAccepted bool
		wantError    string
	}{
		{name: "attempt one accepts fence zero", attempt: 1, fenceAttempt: 0, operation: "recover-learner-fence-0", wantAccepted: true},
		{name: "attempt two accepts fence zero", attempt: 2, fenceAttempt: 0, operation: "recover-learner-fence-0", wantAccepted: true},
		{name: "attempt two accepts fence one", attempt: 2, fenceAttempt: 1, operation: "recover-learner-fence-1", wantAccepted: true},
		{name: "attempt three accepts fence zero", attempt: 3, fenceAttempt: 0, operation: "recover-learner-fence-0", wantAccepted: true},
		{name: "attempt three accepts fence two", attempt: 3, fenceAttempt: 2, operation: "recover-learner-fence-2", wantAccepted: true},
		{name: "wrong recovery rejected", attempt: 2, fenceAttempt: 0, operation: "other-learner-fence-0", wantError: "unproven learner retirement history"},
		{name: "current attempt rejected", attempt: 2, fenceAttempt: 2, operation: "recover-learner-fence-2", wantError: "unproven learner retirement history"},
		{name: "future attempt rejected", attempt: 2, fenceAttempt: 3, operation: "recover-learner-fence-3", wantError: "unproven learner retirement history"},
		{name: "attempt three current fence rejected", attempt: 3, fenceAttempt: 3, operation: "recover-learner-fence-3", wantError: "unproven learner retirement history"},
		{name: "attempt three future fence rejected", attempt: 3, fenceAttempt: 4, operation: "recover-learner-fence-4", wantError: "unproven learner retirement history"},
		{name: "malformed operation rejected", attempt: 2, fenceAttempt: 0, operation: "recover-learner-fence-00", wantError: "unproven learner retirement history"},
		{name: "missing proof rejected", attempt: 2, fenceAttempt: 0, operation: "recover-learner-fence-0", proofMode: "missing", wantError: "collides with retained learner history"},
		{name: "wrong proof rejected", attempt: 2, fenceAttempt: 0, operation: "recover-learner-fence-0", proofMode: "wrong", wantError: "unproven learner retirement history"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, c, state, sts := retiredLearnerAttemptFixture(t, tt.attempt, tt.fenceAttempt, tt.operation, tt.proofMode)
			assigned, err := c.prepareSameOrdinalLearner(ctx, state, sts, "rhiza")
			if tt.wantAccepted {
				if err != nil || assigned {
					t.Fatalf("earlier exact retirement proof was not accepted as retained history: assigned=%t err=%v", assigned, err)
				}
				return
			}
			if err == nil || assigned {
				t.Fatalf("invalid retirement reference was accepted: assigned=%t err=%v", assigned, err)
			}
			if !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("invalid retirement reference failed outside the intended history guard: %v", err)
			}
		})
	}
}
