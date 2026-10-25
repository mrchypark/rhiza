package operator

import (
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

func TestEngineRepairStartupAcceptsOnlyPinnedWALUnboundLearnerRuns(t *testing.T) {
	e, store, proposal := engineRepairStoreFixture(t)
	created, err := store.CreateOrReadEngineRepair(t.Context(), proposal)
	if err != nil {
		t.Fatal(err)
	}
	pinned := pinEngineRepairStoreCandidate(t, store, created)
	assertEngineRepairStartupRunIsWALUnboundLearner(t, e, pinned)

	priorIdentity := pinned.CurrentCandidateIdentity()
	nextIdentity := priorIdentity
	nextIdentity.RunID = "candidate-continuation-run"
	nextRuntime := engineRepairStoreCandidateRuntime(pinned)
	nextRuntime.RunID = nextIdentity.RunID
	nextRuntime.RuntimeID = "containerd-continuation"
	nextRuntime.ContainerID = "candidate-container-continuation"
	continued, err := store.ContinueCandidateRuntime(t.Context(), pinned.Binding.SourceClusterID, pinned.Binding.OperationID,
		pinned.Digest(), priorIdentity.RunID, nextIdentity, nextRuntime)
	if err != nil {
		t.Fatalf("continue assigned candidate with fresh physical RunID: %v", err)
	}
	assertEngineRepairStartupRunIsWALUnboundLearner(t, e, continued)
}

func assertEngineRepairStartupRunIsWALUnboundLearner(t *testing.T, e *FencingExecutor, record fencing.EngineRepairRecord) {
	t.Helper()
	identity := record.CurrentCandidateIdentity()
	if err := e.registerEngineRepairCandidateRun(t.Context(), record.Binding.StatefulSetUID, record, identity); err != nil {
		t.Fatalf("accept authority-pinned producer-less learner run: %v", err)
	}
	root, _, err := e.AuthorityState.ReadRoot(t.Context(), record.Binding.StatefulSetUID, record.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	run, ok := root.Ledger.Runs[identity.RunID]
	want := identity
	want.WALIdentity = ""
	if !ok || run.Identity != want || run.Runtime == nil || *run.Runtime != *record.CurrentCandidateRuntime() ||
		fencing.RunRole(run) != fencing.StartupRoleLearnerPendingRemove || run.Stop != nil || run.Promotion != nil ||
		run.MutationProducerUID != "" || run.MutationScopeHash != "" || run.MutationProfile != "" || run.MutationStageIncarnation != "" {
		t.Fatalf("startup changed or granted authority to the pinned learner run: run=%+v exists=%t", run, ok)
	}
}
