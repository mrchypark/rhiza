package operator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/thanos-io/objstore"
)

type logicalNoopMutationProvider struct{ calls int }

func (p *logicalNoopMutationProvider) observe(context.Context, string) (int64, bool, error) {
	p.calls++
	return 0, false, nil
}
func (p *logicalNoopMutationProvider) upload(context.Context, string, int64, fencing.MutationIntent, io.Reader) (mutationProviderResult, error) {
	p.calls++
	return mutationProviderResult{}, nil
}
func (p *logicalNoopMutationProvider) delete(context.Context, string, int64) (mutationProviderResult, error) {
	p.calls++
	return mutationProviderResult{}, nil
}
func (p *logicalNoopMutationProvider) committed(context.Context, string, fencing.MutationIntent, fencing.MutationPreparation) (mutationProviderResult, bool, error) {
	p.calls++
	return mutationProviderResult{}, false, nil
}

func logicalCompactionScope() AttestationStoreScope {
	return AttestationStoreScope{Provider: "gcs", Bucket: "logical-compaction-bucket", Prefix: "archive"}
}

func newLogicalCompactionFixture(t *testing.T) (*FencingExecutor, string, fencing.Identity, fencing.LogicalWriterBindingV1) {
	t.Helper()
	ctx := t.Context()
	bucket := objstore.NewInMemBucket()
	t.Cleanup(func() { _ = bucket.Close() })
	versionedBucket := &archiveTestBucket{Bucket: bucket}
	store, err := NewAuthorityStateStore(versionedBucket, "logical-compaction")
	if err != nil {
		t.Fatal(err)
	}
	const uid, source, node, runID = "logical-compaction-sts", "logical-compaction-source", "voter-a", "run-a"
	e := &FencingExecutor{Kube: newMissingLegacyLedgerKube(t), AuthorityState: store,
		RequireMutationJournal: true, AuthorityProfile: fencing.LogicalWriterProfileV1}
	if err := e.initializeRoot(ctx, uid, source); err != nil {
		t.Fatal(err)
	}
	id := fencing.Identity{SourceClusterID: source, NodeID: node, RunID: runID}
	binding := fencing.LogicalWriterBindingV1{Version: fencing.LogicalWriterVersionV1, Profile: fencing.LogicalWriterProfileV1,
		SourceClusterID: source, NodeID: node, RunID: runID, Role: fencing.StartupRoleVoterActive,
		ProducerUID: "logical-compaction-producer", StoreScopeHash: hashJSON(logicalCompactionScope())}
	root, version, err := store.ReadRoot(ctx, uid, source)
	if err != nil {
		t.Fatal(err)
	}
	if err := root.Ledger.RegisterLogicalWithRole(id, binding); err != nil {
		t.Fatal(err)
	}
	if err := root.Ledger.BindLogicalMutationProducer(id, binding.ProducerUID); err != nil {
		t.Fatal(err)
	}
	if err := root.Ledger.BindLogicalMutationScope(id, binding.StoreScopeHash, fencing.MutationExecutionProfile); err != nil {
		t.Fatal(err)
	}
	if err := root.Ledger.BindLogicalMutationStage(id, hashJSON([]string{"stage", source})); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteRoot(ctx, uid, source, root, version); err != nil {
		t.Fatal(err)
	}
	if err := e.reserveLogicalMutationProducer(ctx, uid, binding); err != nil {
		t.Fatal(err)
	}
	id.WALIdentity = hashJSON([]string{"wal", runID})
	receipt, err := e.bindLogicalSupervisorWAL(ctx, uid, id)
	if err != nil || receipt.LogicalBinding == nil {
		t.Fatalf("bind logical WAL: receipt=%+v err=%v", receipt, err)
	}
	return e, uid, id, *receipt.LogicalBinding
}

func appendSettledLogicalStoreMutation(t *testing.T, e *FencingExecutor, uid string, binding fencing.LogicalWriterBindingV1, sequence uint64, intent fencing.MutationIntent) {
	t.Helper()
	authority := logicalMutationAuthority(uid, binding)
	_, err := e.changeMutationProducerForAuthority(t.Context(), authority, binding.ProducerUID, func(p *fencing.MutationProducer) error {
		if err := p.Admit(sequence, intent); err != nil {
			return err
		}
		return p.CancelUnprepared(sequence, strings.Repeat("a", 64))
	})
	if err != nil {
		t.Fatalf("admit settled logical mutation %d: %v", sequence, err)
	}
}

func TestLogicalMutationCompactionBoundsJournalAndResolvesArchivedRetry(t *testing.T) {
	e, uid, _, binding := newLogicalCompactionFixture(t)
	var first fencing.MutationIntent
	stages, err := e.sourceStagesForRoot(t.Context(), uid, binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("logical staged payload")
	ticket := strings.Repeat("b", 64)
	draft := completeStoreIntent(fencing.MutationIntent{StoreScopeHash: binding.StoreScopeHash, Kind: "upload", Key: "key-001"})
	draft.OperationID, draft.PayloadRef, draft.PayloadDigest, draft.PayloadSize = "", "", "", 0
	draft.OwnerStep = fencing.MutationOwnerStep{Owner: "fixture", Workflow: "publication", Step: "upload", Attempt: 1}
	if err := stages.ReserveContext(t.Context(), binding.ProducerUID, ticket, draft); err != nil {
		t.Fatal(err)
	}
	digest := hashBytes(payload)
	if _, err := stages.SealContext(t.Context(), binding.ProducerUID, ticket, digest, int64(len(payload)), bytes.NewReader(payload)); err != nil {
		t.Fatal(err)
	}
	first = draft
	first.OperationID, first.PayloadRef, first.PayloadDigest, first.PayloadSize = fencing.MutationRequestID(binding.ProducerUID, 1), "payload-"+ticket, digest, int64(len(payload))
	for sequence := uint64(1); sequence <= 130; sequence++ {
		if err := e.compactLogicalSourceMutationJournal(t.Context(), uid, binding, sequence); err != nil {
			t.Fatalf("pre-admission compaction at %d: %v", sequence, err)
		}
		intent := first
		if sequence > 1 {
			intent = completeStoreIntent(fencing.MutationIntent{OperationID: fencing.MutationRequestID(binding.ProducerUID, sequence),
				Kind: "delete", Key: fmt.Sprintf("key-%03d", sequence), StoreScopeHash: binding.StoreScopeHash})
		}
		appendSettledLogicalStoreMutation(t, e, uid, binding, sequence, intent)
	}
	producer, producerUID, _, err := e.readMutationProducerForAuthority(t.Context(), logicalMutationAuthority(uid, binding))
	if err != nil || producerUID != binding.ProducerUID {
		t.Fatalf("read logical producer: uid=%q err=%v", producerUID, err)
	}
	if producer.CompactedThrough < 96 || producer.LastAdmitted-producer.CompactedThrough >= fencing.MaxMutationJournalEntries || producer.ArchiveHead == nil {
		t.Fatalf("logical journal was not kept bounded across archive segments: compacted=%d admitted=%d head=%v", producer.CompactedThrough, producer.LastAdmitted, producer.ArchiveHead)
	}
	stageRecord, _, err := stages.readRemoteStage(t.Context(), binding.ProducerUID, ticket)
	if err != nil || !stageRecord.Retired || stageRecord.Payload == nil || *stageRecord.Payload != (sourceStageDescriptor{Ref: first.PayloadRef, Digest: first.PayloadDigest, Size: first.PayloadSize}) {
		t.Fatalf("archived logical upload stage was not retired with its exact descriptor: record=%+v err=%v", stageRecord, err)
	}
	if _, _, err := stages.evidence.Read(t.Context(), sourceStagePayloadKey(binding.ProducerUID, ticket)); !stages.evidence.IsNotFound(err) {
		t.Fatalf("archived logical stage payload remains live: %v", err)
	}
	entry, err := e.lookupArchivedMutationForAuthority(t.Context(), logicalMutationAuthority(uid, binding), binding.ProducerUID, 1, first)
	if err != nil || entry.Intent != first || entry.Resolution == nil {
		t.Fatalf("exact archived retry did not resolve to original terminal entry: entry=%+v err=%v", entry, err)
	}
	changed := first
	changed.Key += "-changed"
	if _, err := e.lookupArchivedMutationForAuthority(t.Context(), logicalMutationAuthority(uid, binding), binding.ProducerUID, 1, changed); err == nil {
		t.Fatal("archived retry accepted a changed mutation intent")
	}
	producer, _, _, err = e.readMutationProducerForAuthority(t.Context(), logicalMutationAuthority(uid, binding))
	if err != nil || producer.EvidenceFault != "" {
		t.Fatalf("caller-supplied changed tuple poisoned valid archive evidence: fault=%q err=%v", producer.EvidenceFault, err)
	}
	receipts, err := e.receiptsForLogicalBinding(uid, binding)
	if err != nil {
		t.Fatal(err)
	}
	provider := &logicalNoopMutationProvider{}
	executionErr := e.ExecuteLogicalSourceMutation(t.Context(), uid, binding, binding.ProducerUID, 1, first, logicalCompactionScope(), provider, receipts)
	if executionErr == nil || !strings.Contains(executionErr.Error(), "terminally rejected") || provider.calls != 0 {
		t.Fatalf("archived no-work retry reached provider or lost terminal result: err=%v calls=%d", executionErr, provider.calls)
	}
	if _, err := e.readMutationKeyState(t.Context(), first.StoreScopeHash, first.Key); !errors.Is(err, errAuthorityStateNotFound) {
		t.Fatalf("archived no-work retry created a mutation key lock: %v", err)
	}
}

func TestLogicalMutationCompactionStopsAtUnresolvedPrefix(t *testing.T) {
	e, uid, _, binding := newLogicalCompactionFixture(t)
	for sequence := uint64(1); sequence <= 65; sequence++ {
		if err := e.compactLogicalSourceMutationJournal(t.Context(), uid, binding, sequence); err != nil {
			t.Fatalf("pre-admission compaction at %d: %v", sequence, err)
		}
		intent := completeStoreIntent(fencing.MutationIntent{OperationID: fencing.MutationRequestID(binding.ProducerUID, sequence),
			Kind: "delete", Key: fmt.Sprintf("prefix-key-%03d", sequence), StoreScopeHash: binding.StoreScopeHash})
		if sequence == 2 {
			_, err := e.changeMutationProducerForAuthority(t.Context(), logicalMutationAuthority(uid, binding), binding.ProducerUID,
				func(p *fencing.MutationProducer) error { return p.Admit(sequence, intent) })
			if err != nil {
				t.Fatal(err)
			}
			continue
		}
		appendSettledLogicalStoreMutation(t, e, uid, binding, sequence, intent)
	}
	producer, _, _, err := e.readMutationProducerForAuthority(t.Context(), logicalMutationAuthority(uid, binding))
	if err != nil {
		t.Fatal(err)
	}
	if producer.CompactedThrough != 1 || producer.ArchiveHead == nil {
		t.Fatalf("compaction skipped unresolved sequence 2: compacted=%d head=%+v", producer.CompactedThrough, producer.ArchiveHead)
	}
	plan, err := e.readMutationArchiveForAuthority(t.Context(), logicalMutationAuthority(uid, binding), *producer.ArchiveHead,
		binding.ProducerUID, producer.WALIdentity)
	if err != nil || plan.Through != 1 {
		t.Fatalf("archive included entries beyond unresolved sequence 2: through=%d err=%v", plan.Through, err)
	}
}
