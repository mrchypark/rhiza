package operator

import (
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

func TestPromotedBoundLearnerReservationRetryReturnsExactBinding(t *testing.T) {
	e, fixture, id, runtime := newMutationStoreFixture(t)
	e.RequireMutationJournal = true
	scope := preparePendingArchiveGenesis(t, e, fixture, id, runtime)
	id.WALIdentity = "learner-wal"
	if _, err := e.change(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID, func(ledger *fencing.Ledger) error {
		run := ledger.Runs[id.RunID]
		run.Role = fencing.StartupRoleLearnerPendingRemove
		ledger.Runs[id.RunID] = run
		return ledger.Bind(id)
	}); err != nil {
		t.Fatalf("bind pending learner WAL: %v", err)
	}
	const operationID = "membership-remove-add"
	evidenceHash := strings.Repeat("a", 64)
	if _, err := e.change(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID, func(ledger *fencing.Ledger) error {
		return ledger.PromoteLearner(id, operationID, evidenceHash)
	}); err != nil {
		t.Fatalf("persist canonical learner promotion: %v", err)
	}

	// The producer object is created, then the root's producer-UID pin is
	// durably written while its response and immediate readback are lost. This
	// leaves the exact post-promotion, pre-activation state for retry.
	faultBucket := &mutationCASReplyFaultBucket{
		Bucket:             e.AuthorityState.bucket,
		loseCASAfter:       2, // reserved producer creation, then root UID pin
		failReadAfterReply: true,
	}
	e.AuthorityState.bucket = faultBucket
	if _, err := e.reserveMutationProducer(t.Context(), runtime); err == nil {
		t.Fatal("injected post-promotion root-pin response loss was not surfaced")
	}
	root, err := e.read(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	run := root.Ledger.Runs[id.RunID]
	if run.Promotion == nil || run.Promotion.OperationID != operationID || run.Promotion.EvidenceHash != evidenceHash || run.MutationProducerUID != stableMutationProducerUID(runtime) {
		t.Fatalf("fixture did not retain the exact committed promotion and producer pin: %+v", run)
	}

	if _, err := e.reserveMutationProducer(t.Context(), runtime); err != nil {
		t.Fatalf("exact promoted producer reservation retry failed: %v", err)
	}
	receipt, err := e.bindWALAtScope(t.Context(), runtime.StatefulSetUID, id, scope)
	if err != nil {
		t.Fatalf("promoted learner could not finish its exact binding: %v", err)
	}
	if err := receipt.Validate(id); err != nil || receipt.Role != fencing.StartupRoleVoterActive || receipt.Producer == nil ||
		receipt.Producer.ProducerUID != stableMutationProducerUID(runtime) || receipt.Producer.Runtime != runtime || receipt.Producer.StoreScopeHash != scope {
		t.Fatalf("retry returned a different or incomplete active binding: receipt=%+v err=%v", receipt, err)
	}
	producer := readFixtureProducer(t, e, fixture, runtime)
	if producer.Phase != fencing.MutationOpen || producer.WALIdentity != id.WALIdentity || producer.LastAdmitted != 0 {
		t.Fatalf("retry did not activate the exact idle producer: %+v", producer)
	}
}
