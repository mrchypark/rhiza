package operator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/thanos-io/objstore"
)

type memoryEvidenceObject struct {
	data       []byte
	generation int64
}

type memoryEvidenceObjects struct {
	mu                     sync.Mutex
	objects                map[string]memoryEvidenceObject
	nextGeneration         int64
	partialNextCreate      bool
	loseNextCreateResponse bool
	blockCreateSuffix      string
	createStarted          chan struct{}
	unblockCreate          chan struct{}
	blockCreateOnce        sync.Once
}

func newMemoryEvidenceObjects() *memoryEvidenceObjects {
	return &memoryEvidenceObjects{objects: make(map[string]memoryEvidenceObject)}
}

func (m *memoryEvidenceObjects) create(_ context.Context, key string, input io.Reader, size int64) (*objstore.ObjectVersion, error) {
	if m.blockCreateSuffix != "" && strings.HasSuffix(key, m.blockCreateSuffix) {
		m.blockCreateOnce.Do(func() { close(m.createStarted) })
		<-m.unblockCreate
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.objects[key]; exists {
		return nil, errMemoryEvidenceConflict
	}
	if m.partialNextCreate {
		m.partialNextCreate = false
		buf := make([]byte, size/2)
		_, _ = io.ReadFull(input, buf)
		return nil, errors.New("injected partial object write")
	}
	data, err := io.ReadAll(io.LimitReader(input, size+1))
	if err != nil || int64(len(data)) != size {
		return nil, errors.Join(err, errors.New("size mismatch"))
	}
	m.nextGeneration++
	m.objects[key] = memoryEvidenceObject{data: data, generation: m.nextGeneration}
	if m.loseNextCreateResponse {
		m.loseNextCreateResponse = false
		return nil, errors.New("injected lost create response")
	}
	return memoryEvidenceVersion(m.nextGeneration), nil
}

func (m *memoryEvidenceObjects) read(_ context.Context, key string) (io.ReadCloser, *objstore.ObjectVersion, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, ok := m.objects[key]
	if !ok {
		return nil, nil, os.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(append([]byte(nil), obj.data...))), memoryEvidenceVersion(obj.generation), nil
}

func (m *memoryEvidenceObjects) compareAndSwap(_ context.Context, key string, generation *objstore.ObjectVersion, body []byte) (*objstore.ObjectVersion, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, ok := m.objects[key]
	if !ok || generation == nil || generation.Value != strconv.FormatInt(obj.generation, 10) {
		return nil, errMemoryEvidenceConflict
	}
	m.nextGeneration++
	m.objects[key] = memoryEvidenceObject{data: append([]byte(nil), body...), generation: m.nextGeneration}
	return memoryEvidenceVersion(m.nextGeneration), nil
}

func (m *memoryEvidenceObjects) deleteVersion(_ context.Context, key string, version objstore.ObjectVersion) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, ok := m.objects[key]
	if !ok || version.Type != objstore.Generation || version.Value != strconv.FormatInt(obj.generation, 10) {
		return errMemoryEvidenceConflict
	}
	delete(m.objects, key)
	return nil
}

func (m *memoryEvidenceObjects) isNotFound(err error) bool { return errors.Is(err, os.ErrNotExist) }

func (m *memoryEvidenceObjects) isConflict(err error) bool {
	return errors.Is(err, errMemoryEvidenceConflict)
}

var errMemoryEvidenceConflict = errors.New("generation conflict")

func memoryEvidenceVersion(generation int64) *objstore.ObjectVersion {
	return &objstore.ObjectVersion{Type: objstore.Generation, Value: strconv.FormatInt(generation, 10)}
}

func (m *memoryEvidenceObjects) corrupt(key string, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj := m.objects[key]
	obj.data = append([]byte(nil), data...)
	m.objects[key] = obj
}

func testAuthorityEvidence(t *testing.T, objects *memoryEvidenceObjects, root string) *AuthorityEvidenceStore {
	t.Helper()
	store, err := newAuthorityEvidenceStore(objects, "test-prefix", hashJSON([]string{"rhiza-authority-root-v1", "root-uid", root}))
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func testRemoteStages(t *testing.T, objects *memoryEvidenceObjects, root string) *SourceStageStore {
	t.Helper()
	store, err := NewRemoteSourceStageStore(testAuthorityEvidence(t, objects, root))
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestAuthorityEvidenceStoreStableRootAndGenerationCAS(t *testing.T) {
	ctx := context.Background()
	objects := newMemoryEvidenceObjects()
	a := testAuthorityEvidence(t, objects, "root-uid")
	b := testAuthorityEvidence(t, objects, "root-uid")
	otherRoot := testAuthorityEvidence(t, objects, "other-root-uid")
	if a.Identity() != b.Identity() || a.Identity() == otherRoot.Identity() {
		t.Fatal("evidence identity is not stable and root-bound")
	}
	generation, err := a.CreateImmutable(ctx, "producer/root.json", []byte(`{"v":1}`))
	if err != nil {
		t.Fatal(err)
	}
	r, gotGeneration, err := b.Read(ctx, "producer/root.json")
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil || gotGeneration == nil || generation == nil || *gotGeneration != *generation || string(data) != `{"v":1}` {
		t.Fatalf("read=%q generation=%v err=%v", data, gotGeneration, err)
	}
	if _, err := a.CompareAndSwap(ctx, "producer/root.json", generation, []byte(`{"v":2}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := b.CompareAndSwap(ctx, "producer/root.json", generation, []byte(`{"v":3}`)); err == nil {
		t.Fatal("stale generation CAS succeeded")
	}
	if _, _, err := otherRoot.Read(ctx, "producer/root.json"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("foreign root read=%v", err)
	}
}

func TestExecutorSelectsStableRootBoundRemoteSourceStages(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	state, err := NewAuthorityStateStore(bucket, "test-prefix")
	if err != nil {
		t.Fatal(err)
	}
	executor := &FencingExecutor{AuthorityState: state}
	ctx := context.Background()
	first, err := executor.sourceStagesFor(ctx, fencing.RuntimeBinding{StatefulSetUID: "sts-a", SourceClusterID: "cluster-a"})
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := executor.sourceStagesFor(ctx, fencing.RuntimeBinding{StatefulSetUID: "sts-a", SourceClusterID: "cluster-a"})
	if err != nil {
		t.Fatal(err)
	}
	otherRoot, err := executor.sourceStagesFor(ctx, fencing.RuntimeBinding{StatefulSetUID: "sts-b", SourceClusterID: "cluster-a"})
	if err != nil {
		t.Fatal(err)
	}
	if first.evidence == nil || reopened.evidence == nil || first.evidence.Identity() != reopened.evidence.Identity() || first.incarnation != reopened.incarnation {
		t.Fatal("remote source-stage identity changed when its executor was recreated")
	}
	if first.evidence.Identity() == otherRoot.evidence.Identity() {
		t.Fatal("different StatefulSet roots shared source-stage evidence")
	}
}

func TestRemoteMutationExecutionReceiptsAreImmutableAndRootBound(t *testing.T) {
	objects := newMemoryEvidenceObjects()
	first, err := NewRemoteMutationExecutionReceiptStore(testAuthorityEvidence(t, objects, "root-uid"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewRemoteMutationExecutionReceiptStore(testAuthorityEvidence(t, objects, "root-uid"))
	if err != nil {
		t.Fatal(err)
	}
	otherRoot, err := NewRemoteMutationExecutionReceiptStore(testAuthorityEvidence(t, objects, "other-root"))
	if err != nil {
		t.Fatal(err)
	}
	receipt := mutationExecutionReceipt{WALIdentity: "wal", ProducerUID: "producer", Sequence: 1, DispatchID: "dispatch", Outcome: fencing.MutationCommitted}
	receipt.ReceiptDigest = receipt.digest()
	name := mutationExecutionReceiptName(receipt)
	if digest, err := first.put(receipt); err != nil || digest != receipt.ReceiptDigest {
		t.Fatalf("remote receipt create failed: %q %v", digest, err)
	}
	got, err := second.get(name)
	if err != nil || got != receipt {
		t.Fatalf("receipt was not readable through the shared root: %+v %v", got, err)
	}
	receipt.Outcome = fencing.MutationRejected
	receipt.ReceiptDigest = receipt.digest()
	if _, err := second.put(receipt); err == nil {
		t.Fatal("immutable receipt key accepted changed bytes")
	}
	if _, err := otherRoot.get(name); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("receipt escaped its authority root: %v", err)
	}
}

func TestReceiptsForRuntimePrefersCanonicalRootOverLocalFallback(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	t.Cleanup(func() { _ = bucket.Close() })
	state, err := NewAuthorityStateStore(bucket, "authority")
	if err != nil {
		t.Fatal(err)
	}
	local := &MutationExecutionReceiptStore{}
	e := &FencingExecutor{AuthorityState: state, SourceMutationReceipts: local}
	store, err := e.receiptsForRuntime(fencing.RuntimeBinding{StatefulSetUID: "root-uid", SourceClusterID: "source-a"})
	if err != nil {
		t.Fatal(err)
	}
	if store == local || store.evidence == nil || store.evidence.Identity() == "" {
		t.Fatal("canonical authority selected a local receipt store")
	}
}

func TestRemoteSourceStageRehydratesAfterLocalDiskLoss(t *testing.T) {
	ctx := context.Background()
	objects := newMemoryEvidenceObjects()
	store := testRemoteStages(t, objects, "root-uid")
	ticket := strings.Repeat("b", 64)
	intent := stageStoreIntent()
	if err := store.ReserveContext(ctx, "producer-uid", ticket, intent); err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"membership":"original"}`)
	hash := sha256.Sum256(payload)
	digest := hex.EncodeToString(hash[:])
	descriptor, err := store.SealContext(ctx, "producer-uid", ticket, digest, int64(len(payload)), bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	// Constructing a fresh store intentionally has no local directory to reopen.
	reopened := testRemoteStages(t, objects, "root-uid")
	intent.PayloadRef, intent.PayloadDigest, intent.PayloadSize = descriptor.Ref, descriptor.Digest, descriptor.Size
	r, err := reopened.openMutationPayloadContext(ctx, "producer-uid", intent)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("rehydrated=%q err=%v", got, err)
	}
}

func TestRemoteSourceStageQuotaIsCASBoundedAndRetirementLeavesTombstone(t *testing.T) {
	ctx := context.Background()
	store := testRemoteStages(t, newMemoryEvidenceObjects(), "root-uid")
	size := int64(maxSourceStageBytes)
	for i := range maxSourceStageTickets {
		ticket := fmt.Sprintf("%064x", i+1)
		if err := store.reserveRemoteStageQuota(ctx, "producer-uid", ticket, &size); err != nil {
			t.Fatalf("reserve ticket %d: %v", i, err)
		}
	}
	if err := store.reserveRemoteStageQuota(ctx, "producer-uid", strings.Repeat("f", 64), nil); err == nil {
		t.Fatal("remote source-stage ticket quota was exceeded")
	}
	changedSize := size - 1
	if err := store.reserveRemoteStageQuota(ctx, "producer-uid", fmt.Sprintf("%064x", 1), &changedSize); err == nil {
		t.Fatal("remote source-stage reservation changed its payload size")
	}
	firstTicket := fmt.Sprintf("%064x", 1)
	if err := store.retireRemoteStageQuota(ctx, "producer-uid", firstTicket, size); err != nil {
		t.Fatal(err)
	}
	if err := store.reserveRemoteStageQuota(ctx, "producer-uid", firstTicket, nil); err == nil {
		t.Fatal("retired remote source-stage ticket was reopened")
	}
	if err := store.reserveRemoteStageQuota(ctx, "producer-uid", strings.Repeat("e", 64), &size); err != nil {
		t.Fatalf("verified retirement did not free one bounded quota slot: %v", err)
	}
	if !quotaExceedsLimit(map[string]int64{strings.Repeat("a", 64): maxSourceStageBytes + 1}) || quotaExceedsLimit(map[string]int64{strings.Repeat("a", 64): -1}) {
		t.Fatal("remote source-stage quota size validation differs")
	}
}

func TestArchivedMutationRetiresExactRemoteStageOnlyAfterCommittedArchive(t *testing.T) {
	for _, failure := range []string{"payload-delete", "stage-tombstone", "quota-release"} {
		t.Run(failure, func(t *testing.T) { testArchivedStageRetirementRecovery(t, failure) })
	}
}

func testArchivedStageRetirementRecovery(t *testing.T, failure string) {
	e, _, id, runtime := newMutationArchiveFixture(t)
	activateStoredProducer(t, e, id, runtime)
	run, err := e.enrolledMutationRun(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	intent := fencing.MutationIntent{
		ContractVersion: fencing.MutationIntentVersion,
		Kind:            "upload",
		Key:             "artifact",
		StoreScopeHash:  strings.Repeat("a", 64),
		OwnerStep:       fencing.MutationOwnerStep{Owner: "test", Workflow: "publish", Step: "artifact", Attempt: 1},
		Condition:       fencing.MutationCondition{Mode: fencing.MutationUnconditional},
	}
	ticket := fmt.Sprintf("%064x", 1)
	stages, err := e.sourceStagesFor(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	if err := stages.ReserveContext(t.Context(), run.MutationProducerUID, ticket, intent); err != nil {
		t.Fatal(err)
	}
	for i := 2; i <= maxSourceStageTickets; i++ {
		if err := stages.ReserveContext(t.Context(), run.MutationProducerUID, fmt.Sprintf("%064x", i), intent); err != nil {
			t.Fatalf("fill bounded quota ticket %d: %v", i, err)
		}
	}
	payload := []byte("retained until archived")
	payloadHash := sha256.Sum256(payload)
	digest := hex.EncodeToString(payloadHash[:])
	descriptor, err := stages.SealContext(t.Context(), run.MutationProducerUID, ticket, digest, int64(len(payload)), bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	intent.OperationID = fencing.MutationRequestID(run.MutationProducerUID, 1)
	intent.PayloadRef, intent.PayloadDigest, intent.PayloadSize = descriptor.Ref, descriptor.Digest, descriptor.Size
	_, err = e.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error {
		if err := p.Admit(1, intent); err != nil {
			return err
		}
		if err := p.Prepare(1, fencing.MutationPreparation{Profile: fencing.MutationExecutionProfile, StoreScopeHash: intent.StoreScopeHash, GenerationSet: true, Generation: 7}); err != nil {
			return err
		}
		hash, err := p.PreparationHash(1)
		if err != nil {
			return err
		}
		return p.Resolve(1, fencing.MutationResolution{Outcome: fencing.MutationExcluded, PreparationHash: hash, ReceiptDigest: strings.Repeat("a", 64)})
	})
	if err != nil {
		t.Fatal(err)
	}
	e.VerifyMutationEvidence = func(context.Context, fencing.RuntimeBinding, uint64, fencing.MutationEntry) error { return nil }
	plan := storeCompactionPlan(t, e, runtime, 1)
	producer, err := e.commitMutationArchive(t.Context(), runtime, plan)
	if err != nil || producer.ArchiveHead == nil {
		t.Fatalf("archive was not committed before retirement: %+v %v", producer, err)
	}
	evidence, err := e.authorityEvidenceFor(runtime)
	if err != nil {
		t.Fatal(err)
	}
	payloadKey, err := evidence.objectKey(sourceStagePayloadKey(run.MutationProducerUID, ticket))
	if err != nil {
		t.Fatal(err)
	}
	wrapped := &archiveTestBucket{Bucket: e.AuthorityState.bucket}
	switch failure {
	case "payload-delete":
		wrapped.failDeleteKey = payloadKey
	case "stage-tombstone":
		wrapped.failUploadKey, err = evidence.objectKey(sourceStageRecordKey(run.MutationProducerUID, ticket))
	case "quota-release":
		wrapped.failUploadKey, err = evidence.objectKey(sourceStageQuotaKey(run.MutationProducerUID))
	}
	if err != nil {
		t.Fatal(err)
	}
	e.AuthorityState.bucket = wrapped
	if err := e.ReconcileArchivedStages(t.Context(), runtime); err == nil {
		t.Fatal("transient payload deletion failure was hidden")
	}
	producer = readFixtureProducer(t, e, nil, runtime)
	if producer.RetiredStageThrough != 0 || producer.CompactedThrough != 1 {
		t.Fatalf("retirement cursor advanced before deletion completed: %+v", producer)
	}
	restarted := &FencingExecutor{Kube: e.Kube, AuthorityState: e.AuthorityState, VerifyMutationEvidence: e.VerifyMutationEvidence}
	if err := restarted.ReconcileArchivedStages(t.Context(), runtime); err != nil {
		t.Fatalf("restart did not resume pending stage retirement: %v", err)
	}
	producer = readFixtureProducer(t, restarted, nil, runtime)
	if producer.RetiredStageThrough != 1 {
		t.Fatalf("restart did not persist completed retirement progress: %+v", producer)
	}
	recoveredStages, err := restarted.sourceStagesFor(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	retryDraft := intent
	retryDraft.OperationID, retryDraft.PayloadRef, retryDraft.PayloadDigest, retryDraft.PayloadSize = "", "", "", 0
	if err := restarted.reserveSourceStage(t.Context(), runtime, recoveredStages, run.MutationProducerUID, fmt.Sprintf("%064x", maxSourceStageTickets+1), retryDraft); err != nil {
		t.Fatalf("reserve did not reconcile committed retirement before enforcing full quota: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := restarted.retireArchivedSourceStage(t.Context(), runtime, run.MutationProducerUID, 1, intent, *producer.ArchiveHead); err != nil {
			t.Fatalf("repeat exact retirement %d was not idempotent: %v", i, err)
		}
	}
	ticketRecord, _, err := stages.readRemoteStage(t.Context(), run.MutationProducerUID, ticket)
	if err != nil || !ticketRecord.Retired {
		t.Fatalf("stage tombstone was not retained: %+v %v", ticketRecord, err)
	}
	draft := intent
	draft.OperationID, draft.PayloadRef, draft.PayloadDigest, draft.PayloadSize = "", "", "", 0
	if err := stages.ReserveContext(t.Context(), run.MutationProducerUID, ticket, draft); err == nil {
		t.Fatal("retired ticket was reopened after its quota slot was reclaimed")
	}
}

func TestRemoteSourceStageCancellationReleasesOnlyUnsealedReservations(t *testing.T) {
	ctx := context.Background()
	objects := newMemoryEvidenceObjects()
	store := testRemoteStages(t, objects, "root-uid")
	intent := stageStoreIntent()
	for i := 0; i < maxSourceStageTickets+8; i++ {
		ticket := fmt.Sprintf("%064x", i+1)
		if err := store.ReserveContext(ctx, "producer-uid", ticket, intent); err != nil {
			t.Fatalf("reserve ticket %d: %v", i+1, err)
		}
		if err := store.CancelContext(ctx, "producer-uid", ticket); err != nil {
			t.Fatalf("cancel ticket %d: %v", i+1, err)
		}
	}
	restarted := testRemoteStages(t, objects, "root-uid")
	if err := restarted.CancelContext(ctx, "producer-uid", fmt.Sprintf("%064x", 1)); err != nil {
		t.Fatalf("restart cancellation was not idempotent: %v", err)
	}
	if err := restarted.ReserveContext(ctx, "producer-uid", fmt.Sprintf("%064x", maxSourceStageTickets+9), intent); err != nil {
		t.Fatalf("cancelled reservations did not free bounded capacity: %v", err)
	}
}

func TestRemoteSourceStageCancellationWaitsForBlockedSeal(t *testing.T) {
	ctx := context.Background()
	objects := newMemoryEvidenceObjects()
	store := testRemoteStages(t, objects, "root-uid")
	intent := stageStoreIntent()
	ticket := fmt.Sprintf("%064x", 1)
	if err := store.ReserveContext(ctx, "producer-uid", ticket, intent); err != nil {
		t.Fatal(err)
	}
	for i := 2; i <= maxSourceStageTickets; i++ {
		if err := store.ReserveContext(ctx, "producer-uid", fmt.Sprintf("%064x", i), intent); err != nil {
			t.Fatalf("fill quota ticket %d: %v", i, err)
		}
	}
	objects.blockCreateSuffix = sourceStagePayloadKey("producer-uid", ticket)
	objects.createStarted = make(chan struct{})
	objects.unblockCreate = make(chan struct{})
	payload := []byte("in-flight payload")
	digestBytes := sha256.Sum256(payload)
	digest := hex.EncodeToString(digestBytes[:])
	sealDone := make(chan error, 1)
	go func() {
		_, err := store.SealContext(ctx, "producer-uid", ticket, digest, int64(len(payload)), bytes.NewReader(payload))
		sealDone <- err
	}()
	<-objects.createStarted // Receiving is durable before payload creation begins.
	if err := store.CancelContext(ctx, "producer-uid", ticket); !errors.Is(err, ErrFencePending) {
		t.Fatalf("cancel of an in-flight seal=%v, want pending", err)
	}
	if err := store.ReserveContext(ctx, "producer-uid", fmt.Sprintf("%064x", maxSourceStageTickets+1), intent); err == nil {
		t.Fatal("in-flight seal cancellation released quota before terminal proof")
	}
	close(objects.unblockCreate)
	if err := <-sealDone; err != nil {
		t.Fatalf("blocked seal did not reach terminal success: %v", err)
	}
	if err := store.CancelContext(ctx, "producer-uid", ticket); err == nil {
		t.Fatal("completed sealed stage was cancelled without archive proof")
	}
}

func TestRemoteSourceStagePartialAndLostCreateResponse(t *testing.T) {
	ctx := context.Background()
	objects := newMemoryEvidenceObjects()
	store := testRemoteStages(t, objects, "root-uid")
	ticket := strings.Repeat("c", 64)
	if err := store.ReserveContext(ctx, "producer-uid", ticket, stageStoreIntent()); err != nil {
		t.Fatal(err)
	}
	payload := []byte("durable payload")
	hash := sha256.Sum256(payload)
	digest := hex.EncodeToString(hash[:])
	objects.partialNextCreate = true
	if _, err := store.SealContext(ctx, "producer-uid", ticket, digest, int64(len(payload)), bytes.NewReader(payload)); err == nil {
		t.Fatal("partial remote upload was accepted")
	}
	objects.loseNextCreateResponse = true
	if _, err := store.SealContext(ctx, "producer-uid", ticket, digest, int64(len(payload)), bytes.NewReader(payload)); err != nil {
		t.Fatalf("same-object retry did not reconcile lost response: %v", err)
	}
	if err := store.ReserveContext(ctx, "producer-uid", ticket, stageStoreIntent()); err != nil {
		t.Fatalf("reserve retry after seal: %v", err)
	}
}

func TestRemoteSourceStageRejectsConflictsCorruptionAndStaleRoot(t *testing.T) {
	ctx := context.Background()
	objects := newMemoryEvidenceObjects()
	store := testRemoteStages(t, objects, "root-uid")
	ticket := strings.Repeat("d", 64)
	intent := stageStoreIntent()
	if err := store.ReserveContext(ctx, "producer-uid", ticket, intent); err != nil {
		t.Fatal(err)
	}
	payload := []byte("original")
	hash := sha256.Sum256(payload)
	digest := hex.EncodeToString(hash[:])
	descriptor, err := store.SealContext(ctx, "producer-uid", ticket, digest, int64(len(payload)), bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	changed := intent
	changed.Key = "membership/changed.json"
	if err := store.ReserveContext(ctx, "producer-uid", ticket, changed); err == nil {
		t.Fatal("ticket rebound to changed intent")
	}
	foreign := testRemoteStages(t, objects, "other-root-uid")
	intent.PayloadRef, intent.PayloadDigest, intent.PayloadSize = descriptor.Ref, descriptor.Digest, descriptor.Size
	if _, err := foreign.openMutationPayloadContext(ctx, "producer-uid", intent); err == nil {
		t.Fatal("different root recovered another root's payload")
	}
	objects.corrupt(store.evidence.prefix+"/"+sourceStagePayloadKey("producer-uid", ticket), []byte("corrupt"))
	if _, err := store.openMutationPayloadContext(ctx, "producer-uid", intent); err == nil {
		t.Fatal("corrupt remote payload accepted")
	}
}
