package operator

import (
	"reflect"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

func TestEngineRepairPromotionCallerFailsClosedWhenProvenanceReferenceIsMissing(t *testing.T) {
	e, store, reservation := engineRepairStoreFixture(t)
	canonical, err := store.CreateOrReadEngineRepair(t.Context(), reservation)
	if err != nil {
		t.Fatal(err)
	}
	record := pinEngineRepairStoreCandidate(t, store, canonical)
	identity := record.CurrentCandidateIdentity()
	identity.WALIdentity = record.CandidateMember.WALIdentity
	if _, err := e.change(t.Context(), record.Binding.StatefulSetUID, identity.SourceClusterID, func(ledger *fencing.Ledger) error {
		return ledger.Bind(identity)
	}); err != nil {
		t.Fatalf("bind exact pinned candidate WAL: %v", err)
	}
	key := mustEngineRepairGenerationKey(t, record)
	if _, err := e.change(t.Context(), record.Binding.StatefulSetUID, identity.SourceClusterID, func(ledger *fencing.Ledger) error {
		delete(ledger.EngineRepairs, key)
		return nil
	}); err != nil {
		t.Fatalf("remove canonical engine-repair reference: %v", err)
	}
	before, _, err := e.AuthorityState.ReadRoot(t.Context(), record.Binding.StatefulSetUID, identity.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	runtime := *record.CurrentCandidateRuntime()
	_, _, _, producerErrBefore := e.AuthorityState.ReadProducer(t.Context(), runtime)
	if producerErrBefore == nil {
		t.Fatal("candidate unexpectedly has a producer before promotion")
	}
	if _, err := e.promoteSupervisorLearner(t.Context(), record.Binding.StatefulSetUID, identity); err == nil {
		t.Fatal("generic promotion accepted an engine-provenance Run after its canonical repair reference disappeared")
	}
	after, _, err := e.AuthorityState.ReadRoot(t.Context(), record.Binding.StatefulSetUID, identity.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("missing-reference promotion mutated the authority root")
	}
	_, _, _, producerErrAfter := e.AuthorityState.ReadProducer(t.Context(), runtime)
	if producerErrAfter == nil {
		t.Fatal("missing-reference promotion created a producer")
	}
}

func TestEngineRepairFinalStartupRereadRejectsClosedRootAndMissingProvenance(t *testing.T) {
	t.Run("closed root", func(t *testing.T) {
		e, store, proposal := engineRepairStoreFixture(t)
		created, err := store.CreateOrReadEngineRepair(t.Context(), proposal)
		if err != nil {
			t.Fatal(err)
		}
		record := pinEngineRepairStoreCandidate(t, store, created)
		identity := record.CurrentCandidateIdentity()
		if err := e.registerEngineRepairCandidateRun(t.Context(), record.Binding.StatefulSetUID, record, identity); err != nil {
			t.Fatalf("register pinned learner Run: %v", err)
		}
		if _, err := e.change(t.Context(), record.Binding.StatefulSetUID, identity.SourceClusterID, func(ledger *fencing.Ledger) error {
			ledger.ClosedBy = "recovery-close"
			ledger.ClosedHash = hashBytes([]byte("closed"))
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := e.verifyEngineRepairCandidateGrantCurrent(t.Context(), record.Binding.StatefulSetUID, record, identity); err == nil {
			t.Fatal("final startup reread returned a grant from a closed root")
		}
	})

	t.Run("provenance missing", func(t *testing.T) {
		e, store, proposal := engineRepairStoreFixture(t)
		created, err := store.CreateOrReadEngineRepair(t.Context(), proposal)
		if err != nil {
			t.Fatal(err)
		}
		record := pinEngineRepairStoreCandidate(t, store, created)
		identity := record.CurrentCandidateIdentity()
		if err := e.registerEngineRepairCandidateRun(t.Context(), record.Binding.StatefulSetUID, record, identity); err != nil {
			t.Fatalf("register pinned learner Run: %v", err)
		}
		if _, err := e.change(t.Context(), record.Binding.StatefulSetUID, identity.SourceClusterID, func(ledger *fencing.Ledger) error {
			run := ledger.Runs[identity.RunID]
			run.EngineRepairProvenance = nil
			ledger.Runs[identity.RunID] = run
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := e.verifyEngineRepairCandidateGrantCurrent(t.Context(), record.Binding.StatefulSetUID, record, identity); err == nil {
			t.Fatal("final startup reread accepted a Run without its engine-repair provenance")
		}
	})
}

func mustEngineRepairGenerationKey(t *testing.T, record fencing.EngineRepairRecord) string {
	t.Helper()
	key, err := record.Binding.EngineRepairGenerationKey()
	if err != nil {
		t.Fatal(err)
	}
	return key
}
