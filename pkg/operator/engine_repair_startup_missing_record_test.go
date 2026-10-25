package operator

import (
	"reflect"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

func TestEngineRepairBootstrapRegistrationFailsClosedWhenReferencedRecordIsMissing(t *testing.T) {
	e, store, proposal := engineRepairStoreFixture(t)
	ctx := t.Context()
	record, err := store.CreateOrReadEngineRepair(ctx, proposal)
	if err != nil {
		t.Fatalf("create canonical engine repair reservation: %v", err)
	}
	rootBefore, _, err := e.AuthorityState.ReadRoot(ctx, record.Binding.StatefulSetUID, record.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	generationKey := mustEngineRepairGenerationKey(t, record)
	reference, ok := rootBefore.Ledger.EngineRepairs[generationKey]
	if !ok || reference.OperationID != record.Binding.OperationID {
		t.Fatalf("fixture lacks the canonical repair reference: %+v", reference)
	}
	evidence, err := NewAuthorityEvidenceStore(e.AuthorityState.bucket, e.AuthorityState.prefix,
		record.Binding.StatefulSetUID, record.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	objectKey, err := evidence.objectKey(reference.RecordKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.AuthorityState.bucket.Delete(ctx, objectKey); err != nil {
		t.Fatalf("remove only the referenced immutable operation record: %v", err)
	}

	request := fencing.Identity{
		SourceClusterID: record.Binding.SourceClusterID,
		NodeID:          record.Binding.OldMemberID,
		PodName:         record.CurrentCandidateIdentity().PodName,
		PodUID:          record.CurrentCandidateIdentity().PodUID,
		RunID:           record.CurrentCandidateIdentity().RunID,
	}
	grant, assigned, err := e.resolveEngineRepairBootstrapRegistration(ctx, record.Binding.StatefulSetUID, request)
	if err == nil {
		t.Fatalf("bootstrap resolver treated a missing referenced operation record as uninitialized authority: assigned=%t grant=%+v", assigned, grant)
	}
	if grant.Role != "" || grant.Identity != (fencing.Identity{}) || len(grant.Environment) != 0 {
		t.Fatalf("failed bootstrap lookup returned a grant: %+v", grant)
	}

	rootAfter, _, err := e.AuthorityState.ReadRoot(ctx, record.Binding.StatefulSetUID, record.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rootBefore, rootAfter) {
		t.Fatal("missing referenced operation record changed or reinitialized the canonical root")
	}
}
