package operator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/thanos-io/objstore"
)

type archiveTestBucket struct {
	objstore.Bucket
	failReadKey   string
	failReadOnce  string
	failDeleteKey string
	failUploadKey string
	loseCreate    string
	reads         int
}

func (b *archiveTestBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if strings.Contains(name, "/evidence/") {
		b.reads++
	}
	if name == b.failReadKey {
		return nil, errors.New("transient evidence read failure")
	}
	if name == b.failReadOnce {
		b.failReadOnce = ""
		return nil, errors.New("lost CAS response readback")
	}
	return b.Bucket.Get(ctx, name)
}

func (b *archiveTestBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	if name == b.failUploadKey {
		b.failUploadKey = ""
		return errors.New("injected transient exact-version CAS failure")
	}
	err := b.Bucket.Upload(ctx, name, r, opts...)
	if err == nil && name == b.loseCreate {
		b.loseCreate = ""
		return errors.New("lost immutable create response")
	}
	return err
}

func (b *archiveTestBucket) SupportsVersionedDelete() bool { return true }

func (b *archiveTestBucket) DeleteVersion(ctx context.Context, name string, version objstore.ObjectVersion) error {
	if name == b.failDeleteKey {
		b.failDeleteKey = ""
		return errors.New("injected transient exact-version delete failure")
	}
	attrs, err := b.Bucket.Attributes(ctx, name)
	if err != nil {
		return err
	}
	if attrs.Version == nil || attrs.Version.Type != version.Type || attrs.Version.Value != version.Value {
		return errors.New("test bucket version differs")
	}
	return b.Bucket.Delete(ctx, name)
}

func newMutationArchiveFixture(t *testing.T) (*FencingExecutor, *mutationStoreFixture, fencing.Identity, fencing.RuntimeBinding) {
	t.Helper()
	f := &mutationStoreFixture{objects: make(map[string]object)}
	api := httptest.NewTLSServer(http.HandlerFunc(f.serve))
	t.Cleanup(api.Close)
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	bucket := objstore.NewInMemBucket()
	t.Cleanup(func() { _ = bucket.Close() })
	state, err := NewAuthorityStateStore(bucket, "test-authority")
	if err != nil {
		t.Fatal(err)
	}
	e := &FencingExecutor{Kube: &Kubernetes{BaseURL: api.URL, Namespace: "test", TokenFile: token, Client: api.Client()}, AuthorityState: state}
	id := fencing.Identity{SourceClusterID: "source", NodeID: "n0", PodUID: "pod-0", RunID: "run-0"}
	runtime := fencing.RuntimeBinding{AuthorityID: "authority", NamespaceUID: "namespace", BindingUID: "binding", StatefulSetUID: "sts",
		SourceClusterID: id.SourceClusterID, NodeID: id.NodeID, PodUID: id.PodUID, RunID: id.RunID, NodeName: "node", NodeUID: "node-uid",
		MachineID: "machine", BootID: "boot", RuntimeID: "containerd", SandboxID: "sandbox", ContainerID: "container", ImageID: "image"}
	if err := e.initializeRoot(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID); err != nil {
		t.Fatal(err)
	}
	if _, err := e.change(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID, func(l *fencing.Ledger) error {
		l.RequireRuntimeBinding, l.RequireMutationJournal = true, true
		return l.RegisterEnrolled(id, runtime)
	}); err != nil {
		t.Fatal(err)
	}
	return e, f, id, runtime
}

func appendSettledStoreMutation(t *testing.T, e *FencingExecutor, runtime fencing.RuntimeBinding, sequence uint64) {
	t.Helper()
	run, err := e.enrolledMutationRun(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error {
		if err := p.Admit(sequence, completeStoreIntent(fencing.MutationIntent{OperationID: fencing.MutationRequestID(run.MutationProducerUID, sequence), Kind: "delete", Key: fmt.Sprint("key-", sequence)})); err != nil {
			return err
		}
		return p.CancelUnprepared(sequence, strings.Repeat("a", 64))
	})
	if err != nil {
		t.Fatal(err)
	}
}

func storeCompactionPlan(t *testing.T, e *FencingExecutor, runtime fencing.RuntimeBinding, through uint64) fencing.MutationCompactionPlan {
	t.Helper()
	p, err := e.reserveMutationProducer(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	run, err := e.enrolledMutationRun(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := p.PlanMutationCompaction(run.MutationProducerUID, through)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func readFixtureProducer(t *testing.T, e *FencingExecutor, f *mutationStoreFixture, runtime fencing.RuntimeBinding) fencing.MutationProducer {
	t.Helper()
	p, _, _, err := e.AuthorityState.ReadProducer(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestMutationArchiveStorePersistsImmutableContentAfterResponseLoss(t *testing.T) {
	e, _, id, runtime := newMutationArchiveFixture(t)
	activateStoredProducer(t, e, id, runtime)
	appendSettledStoreMutation(t, e, runtime, 1)
	plan := storeCompactionPlan(t, e, runtime, 1)
	evidence, err := e.authorityEvidenceFor(runtime)
	if err != nil {
		t.Fatal(err)
	}
	key, err := evidence.objectKey(fencing.MutationArchiveName(plan.Digest()))
	if err != nil {
		t.Fatal(err)
	}
	bucket := e.AuthorityState.bucket
	wrapped := &archiveTestBucket{Bucket: bucket, loseCreate: key}
	e.AuthorityState.bucket = wrapped
	p, err := e.commitMutationArchive(t.Context(), runtime, plan)
	if err != nil || p.CompactedThrough != 1 || p.ArchiveHead == nil {
		t.Fatalf("readback did not resolve create response loss: %+v %v", p, err)
	}
	attrs, err := bucket.Attributes(t.Context(), key)
	if err != nil || attrs.Version == nil || attrs.Version.Value != p.ArchiveHead.UID {
		t.Fatal("producer references unpinned archive generation")
	}
	if err := bucket.Upload(t.Context(), key, bytes.NewReader([]byte("corrupt")), objstore.WithIfNotExists()); err == nil {
		t.Fatal("fixture accepted immutable object replacement")
	}
	run, err := e.enrolledMutationRun(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.readMutationArchive(t.Context(), *p.ArchiveHead, runtime, run.MutationProducerUID, "wal"); err != nil {
		t.Fatal("immutable record was overwritten:", err)
	}
	if _, err := e.commitMutationArchive(t.Context(), runtime, plan); err != nil {
		t.Fatal("uncertain original plan not idempotent:", err)
	}
}

func TestMutationArchiveStoreResumesHistoryAcrossExecutorRestarts(t *testing.T) {
	e, f, id, runtime := newMutationArchiveFixture(t)
	activateStoredProducer(t, e, id, runtime)
	for sequence := uint64(1); sequence <= 70; sequence++ {
		appendSettledStoreMutation(t, e, runtime, sequence)
		if _, err := e.commitMutationArchive(t.Context(), runtime, storeCompactionPlan(t, e, runtime, sequence)); err != nil {
			t.Fatal(err)
		}
	}
	archiveBucket := &archiveTestBucket{Bucket: e.AuthorityState.bucket}
	e.AuthorityState.bucket = archiveBucket
	appendSettledStoreMutation(t, e, runtime, 71)
	reads := archiveBucket.reads
	if reads != 1 {
		t.Fatalf("normal admission traversed %d archive records instead of pinned head", reads)
	}
	state, err := e.change(t.Context(), "sts", "source", func(l *fencing.Ledger) error { return l.Close("fence", strings.Repeat("a", 64)) })
	if err != nil {
		t.Fatal(err)
	}
	for pass := 1; pass <= 3; pass++ {
		restarted := &FencingExecutor{Kube: e.Kube, AuthorityState: e.AuthorityState}
		done, err := restarted.closeMutationProducers(t.Context(), state, nil)
		if err != nil || done != (pass == 3) {
			t.Fatalf("history pass %d: complete=%v err=%v", pass, done, err)
		}
		p := readFixtureProducer(t, e, f, runtime)
		want := uint64(pass * 32)
		if want > 70 {
			want = 70
		}
		if p.ArchiveScan == nil || p.ArchiveScan.Links != want {
			t.Fatalf("persisted history position lost: %+v", p.ArchiveScan)
		}
	}
}

func TestMutationArchiveStoreProductionTriggerCompactsAcrossRestarts(t *testing.T) {
	f := &mutationStoreFixture{objects: make(map[string]object)}
	api := httptest.NewTLSServer(http.HandlerFunc(f.serve))
	t.Cleanup(api.Close)
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	bucket := objstore.NewInMemBucket()
	t.Cleanup(func() { _ = bucket.Close() })
	state, err := NewAuthorityStateStore(bucket, "test-authority")
	if err != nil {
		t.Fatal(err)
	}
	e := &FencingExecutor{Kube: &Kubernetes{BaseURL: api.URL, Namespace: "test", TokenFile: token, Client: api.Client()}, AuthorityState: state}
	id := fencing.Identity{SourceClusterID: "source", NodeID: "n0", PodUID: "pod-0", RunID: "run-0"}
	runtime := fencing.RuntimeBinding{AuthorityID: "authority", NamespaceUID: "namespace", BindingUID: "binding", StatefulSetUID: "sts",
		SourceClusterID: id.SourceClusterID, NodeID: id.NodeID, PodUID: id.PodUID, RunID: id.RunID, NodeName: "node", NodeUID: "node-uid",
		MachineID: "machine", BootID: "boot", RuntimeID: "containerd", SandboxID: "sandbox", ContainerID: "container", ImageID: "image"}
	if err := e.initializeRoot(t.Context(), "sts", "source"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.change(t.Context(), "sts", "source", func(l *fencing.Ledger) error {
		l.RequireRuntimeBinding, l.RequireMutationJournal = true, true
		return l.RegisterEnrolled(id, runtime)
	}); err != nil {
		t.Fatal(err)
	}
	activateStoredProducer(t, e, id, runtime)
	for sequence := uint64(1); sequence <= 130; sequence++ {
		if err := e.compactSourceMutationJournal(t.Context(), runtime, sequence); err != nil {
			t.Fatalf("pre-admission compaction at %d: %v", sequence, err)
		}
		appendSettledStoreMutation(t, e, runtime, sequence)
	}
	producer, _, _, err := e.AuthorityState.ReadProducer(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	if producer.CompactedThrough < 96 || producer.LastAdmitted-producer.CompactedThrough >= fencing.MaxMutationJournalEntries {
		t.Fatalf("normal admission trigger did not keep a multi-segment journal bounded: compacted=%d admitted=%d", producer.CompactedThrough, producer.LastAdmitted)
	}
	segments := 0
	for ref := producer.ArchiveHead; ref != nil; {
		segments++
		plan, err := e.readMutationArchive(t.Context(), *ref, runtime, stableMutationProducerUID(runtime), "wal")
		if err != nil {
			t.Fatalf("archive segment %d is not verified: %v", segments, err)
		}
		ref = plan.Previous
	}
	if segments < 3 {
		t.Fatalf("expected multiple production archive segments, got %d", segments)
	}
	restarted := &FencingExecutor{Kube: e.Kube, AuthorityState: e.AuthorityState}
	if err := restarted.compactSourceMutationJournal(t.Context(), runtime, 131); err != nil {
		t.Fatalf("restart could not load archived watermark: %v", err)
	}
	appendSettledStoreMutation(t, restarted, runtime, 131)
	producer, _, _, err = restarted.AuthorityState.ReadProducer(t.Context(), runtime)
	if err != nil || producer.LastAdmitted != 131 {
		t.Fatalf("admission after restart was not advanced: %d %v", producer.LastAdmitted, err)
	}
}

func TestMutationArchiveStoreMissingReplacedCorruptEvidenceLatches(t *testing.T) {
	for _, where := range []string{"head", "ancestor"} {
		for _, fault := range []string{"missing", "replaced", "corrupt", "mutable"} {
			t.Run(where+"/"+fault, func(t *testing.T) {
				e, f, id, runtime := newMutationArchiveFixture(t)
				activateStoredProducer(t, e, id, runtime)
				appendSettledStoreMutation(t, e, runtime, 1)
				first, err := e.commitMutationArchive(t.Context(), runtime, storeCompactionPlan(t, e, runtime, 1))
				if err != nil {
					t.Fatal(err)
				}
				appendSettledStoreMutation(t, e, runtime, 2)
				second, err := e.commitMutationArchive(t.Context(), runtime, storeCompactionPlan(t, e, runtime, 2))
				if err != nil {
					t.Fatal(err)
				}
				ref := first.ArchiveHead
				if where == "head" {
					ref = second.ArchiveHead
				}
				evidence, err := e.authorityEvidenceFor(runtime)
				if err != nil {
					t.Fatal(err)
				}
				path, err := evidence.objectKey(ref.Name)
				if err != nil {
					t.Fatal(err)
				}
				bucket := e.AuthorityState.bucket
				attrs, err := bucket.Attributes(t.Context(), path)
				if err != nil || attrs.Version == nil {
					t.Fatalf("read archive generation: %v", err)
				}
				switch fault {
				case "missing":
					if err := bucket.Delete(t.Context(), path); err != nil {
						t.Fatal(err)
					}
				case "replaced":
					if err := bucket.Upload(t.Context(), path, bytes.NewReader([]byte("replacement")), objstore.WithIfMatch(attrs.Version)); err != nil {
						t.Fatal(err)
					}
				case "corrupt":
					if err := bucket.Upload(t.Context(), path, bytes.NewReader([]byte("{}")), objstore.WithIfMatch(attrs.Version)); err != nil {
						t.Fatal(err)
					}
				case "mutable":
					if err := bucket.Upload(t.Context(), path, bytes.NewReader([]byte("mutable")), objstore.WithIfMatch(attrs.Version)); err != nil {
						t.Fatal(err)
					}
				}
				if where == "ancestor" {
					appendSettledStoreMutation(t, e, runtime, 3)
				}
				state, err := e.change(t.Context(), "sts", "source", func(l *fencing.Ledger) error { return l.Close("fence", strings.Repeat("a", 64)) })
				if err != nil {
					t.Fatal(err)
				}
				if done, err := e.closeMutationProducers(t.Context(), state, nil); err == nil || done {
					t.Fatal("bad retained history authorized terminal barrier")
				}
				p := readFixtureProducer(t, e, f, runtime)
				if len(p.EvidenceFault) != 64 || p.BarrierComplete() {
					t.Fatal("evidence fault was not retained")
				}
				if _, err := e.reserveMutationProducer(t.Context(), runtime); !errors.Is(err, fencing.ErrUnavailable) {
					t.Fatal("repair/retry silently cleared recorded evidence loss:", err)
				}
			})
		}
	}
}

func TestMutationArchiveStoreTransientReadFailureIsNotPermanentCorruption(t *testing.T) {
	e, _, id, runtime := newMutationArchiveFixture(t)
	activateStoredProducer(t, e, id, runtime)
	appendSettledStoreMutation(t, e, runtime, 1)
	p, err := e.commitMutationArchive(t.Context(), runtime, storeCompactionPlan(t, e, runtime, 1))
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := e.authorityEvidenceFor(runtime)
	if err != nil {
		t.Fatal(err)
	}
	path, err := evidence.objectKey(p.ArchiveHead.Name)
	if err != nil {
		t.Fatal(err)
	}
	wrapped := &archiveTestBucket{Bucket: e.AuthorityState.bucket, failReadKey: path}
	e.AuthorityState.bucket = wrapped
	if _, err := e.reserveMutationProducer(t.Context(), runtime); err == nil {
		t.Fatal("API unavailable did not block current request")
	}
	if readFixtureProducer(t, e, nil, runtime).EvidenceFault != "" {
		t.Fatal("transient 503 was misclassified as corrupt evidence")
	}
	wrapped.failReadKey = ""
	if _, err := e.reserveMutationProducer(t.Context(), runtime); err != nil {
		t.Fatal(err)
	}
}

func TestMutationArchiveStoreLostCASAndOrphanRequireExactCommittedChain(t *testing.T) {
	e, f, id, runtime := newMutationArchiveFixture(t)
	activateStoredProducer(t, e, id, runtime)
	appendSettledStoreMutation(t, e, runtime, 1)
	appendSettledStoreMutation(t, e, runtime, 2)
	a := storeCompactionPlan(t, e, runtime, 1)
	orphan := storeCompactionPlan(t, e, runtime, 2)
	ref, err := e.persistMutationArchive(t.Context(), a)
	if err != nil {
		t.Fatal(err)
	}
	producerKey, _, err := e.AuthorityState.producerKey(runtime)
	if err != nil {
		t.Fatal(err)
	}
	wrapped := &archiveTestBucket{Bucket: e.AuthorityState.bucket, loseCreate: producerKey, failReadOnce: producerKey}
	e.AuthorityState.bucket = wrapped
	_, err = e.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error { return p.CommitMutationCompaction(a.ProducerUID, a, ref) })
	if err == nil {
		t.Fatal("injected post-commit response loss not observed")
	}
	e.AuthorityState.bucket = wrapped.Bucket
	appendSettledStoreMutation(t, e, runtime, 3)
	if _, err := e.commitMutationArchive(t.Context(), runtime, storeCompactionPlan(t, e, runtime, 3)); err != nil {
		t.Fatal(err)
	}
	p, err := e.commitMutationArchive(t.Context(), runtime, a)
	if err != nil || p.CompactedThrough != 3 {
		t.Fatalf("original committed archive was not found after later compaction: %+v %v", p, err)
	}
	if _, err := e.commitMutationArchive(t.Context(), runtime, orphan); err == nil {
		t.Fatal("larger watermark falsely resolved an unreferenced overlapping archive")
	}
	if p := readFixtureProducer(t, e, f, runtime); p.CompactedThrough != 3 || p.EvidenceFault != "" {
		t.Fatal("rejected orphan corrupted committed history")
	}
}

func TestMutationArchiveStoreArchivedLookupPreservesIdentityAndOutcome(t *testing.T) {
	e, _, id, runtime := newMutationArchiveFixture(t)
	activateStoredProducer(t, e, id, runtime)
	run, err := e.enrolledMutationRun(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	intent := completeStoreIntent(fencing.MutationIntent{OperationID: fencing.MutationRequestID(run.MutationProducerUID, 1), Kind: "upload", Key: "key", PayloadRef: "private/staged-object", PayloadDigest: strings.Repeat("a", 64)})
	_, err = e.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error {
		if err := p.Admit(1, intent); err != nil {
			return err
		}
		if err := p.Prepare(1, fencing.MutationPreparation{Profile: "gcs-json-v1", StoreScopeHash: strings.Repeat("a", 64), GenerationSet: true, Generation: 9}); err != nil {
			return err
		}
		hash, err := p.PreparationHash(1)
		if err != nil {
			return err
		}
		return p.Resolve(1, fencing.MutationResolution{Outcome: fencing.MutationExcluded, PreparationHash: hash, ReceiptDigest: strings.Repeat("b", 64)})
	})
	if err != nil {
		t.Fatal(err)
	}
	plan := storeCompactionPlan(t, e, runtime, 1)
	if _, err := e.commitMutationArchive(t.Context(), runtime, plan); err == nil {
		t.Fatal("prepared work qualified without a provider receipt verifier")
	}
	// This deterministic fixture checks the callback contract, not actual GCS.
	verified := 0
	e.VerifyMutationEvidence = func(_ context.Context, got fencing.RuntimeBinding, sequence uint64, entry fencing.MutationEntry) error {
		if got != runtime || sequence != 1 || entry.Intent != intent || entry.Resolution == nil || entry.Resolution.ReceiptDigest != strings.Repeat("b", 64) {
			return fmt.Errorf("fixture receipt identity differs")
		}
		verified++
		return nil
	}
	if _, err := e.commitMutationArchive(t.Context(), runtime, storeCompactionPlan(t, e, runtime, 1)); err != nil {
		t.Fatal(err)
	}
	restarted := &FencingExecutor{Kube: e.Kube, AuthorityState: e.AuthorityState, VerifyMutationEvidence: e.VerifyMutationEvidence}
	entry, err := restarted.lookupArchivedMutation(t.Context(), runtime, run.MutationProducerUID, 1, intent)
	if err != nil || entry.Intent != intent || entry.Resolution == nil || entry.Resolution.Outcome != fencing.MutationExcluded {
		t.Fatalf("lookup converted exclusion to commitment or lost request: %+v %v", entry, err)
	}
	changed := intent
	changed.PayloadDigest = strings.Repeat("c", 64)
	if _, err := restarted.lookupArchivedMutation(t.Context(), runtime, run.MutationProducerUID, 1, changed); err == nil {
		t.Fatal("old tuple accepted different payload")
	}
	if _, err := restarted.lookupArchivedMutation(t.Context(), runtime, "other-producer", 1, intent); err == nil {
		t.Fatal("old sequence accepted another producer")
	}
	if _, err := restarted.lookupArchivedMutation(t.Context(), runtime, run.MutationProducerUID, 2, intent); err == nil {
		t.Fatal("uncertain request silently renumbered")
	}
	_, err = restarted.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error { return p.Admit(1, intent) })
	if !errors.Is(err, fencing.ErrMutationArchived) {
		t.Fatalf("archived lookup recreated admission rights: %v", err)
	}
	if verified < 3 {
		t.Fatal("archive creation/history/lookup did not require retained receipt evidence")
	}
}

func TestMutationArchiveStoreLiveBarrierRequiresPreparedReceiptVerifier(t *testing.T) {
	e, _, id, runtime := newMutationArchiveFixture(t)
	activateStoredProducer(t, e, id, runtime)
	run, err := e.enrolledMutationRun(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error {
		intent := completeStoreIntent(fencing.MutationIntent{OperationID: fencing.MutationRequestID(run.MutationProducerUID, 1), Kind: "delete", Key: "key"})
		if err := p.Admit(1, intent); err != nil {
			return err
		}
		if err := p.Prepare(1, fencing.MutationPreparation{Profile: "gcs-json-v1", StoreScopeHash: strings.Repeat("a", 64), GenerationSet: true, Generation: 12}); err != nil {
			return err
		}
		hash, err := p.PreparationHash(1)
		if err != nil {
			return err
		}
		return p.Resolve(1, fencing.MutationResolution{Outcome: fencing.MutationExcluded, PreparationHash: hash, ReceiptDigest: strings.Repeat("b", 64)})
	})
	if err != nil {
		t.Fatal(err)
	}
	state, err := e.change(t.Context(), "sts", "source", func(l *fencing.Ledger) error { return l.Close("fence", strings.Repeat("a", 64)) })
	if err != nil {
		t.Fatal(err)
	}
	if done, err := e.closeMutationProducers(t.Context(), state, nil); err == nil || done {
		t.Fatal("live digest-only prepared state authorized a barrier")
	}
	verified := 0
	e.VerifyMutationEvidence = func(_ context.Context, got fencing.RuntimeBinding, seq uint64, entry fencing.MutationEntry) error {
		if got != runtime || seq != 1 || entry.Resolution == nil || entry.Resolution.Outcome != fencing.MutationExcluded {
			return fmt.Errorf("fixture receipt differs")
		}
		verified++
		return nil
	}
	if done, err := e.closeMutationProducers(t.Context(), state, nil); err != nil || !done || verified != 1 {
		t.Fatalf("verified live outcome not considered: done=%v count=%d err=%v", done, verified, err)
	}
}

func TestMutationArchiveStoreNewFenceRechecksPreviouslyVerifiedAncestors(t *testing.T) {
	e, f, id, runtime := newMutationArchiveFixture(t)
	activateStoredProducer(t, e, id, runtime)
	appendSettledStoreMutation(t, e, runtime, 1)
	first, err := e.commitMutationArchive(t.Context(), runtime, storeCompactionPlan(t, e, runtime, 1))
	if err != nil {
		t.Fatal(err)
	}
	appendSettledStoreMutation(t, e, runtime, 2)
	if _, err := e.commitMutationArchive(t.Context(), runtime, storeCompactionPlan(t, e, runtime, 2)); err != nil {
		t.Fatal(err)
	}
	if done, err := e.verifyMutationArchiveHistory(t.Context(), runtime, strings.Repeat("a", 64), 32); err != nil || !done {
		t.Fatal("first verification failed:", err)
	}
	evidence, err := e.authorityEvidenceFor(runtime)
	if err != nil {
		t.Fatal(err)
	}
	path, err := evidence.objectKey(first.ArchiveHead.Name)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.AuthorityState.bucket.Delete(t.Context(), path); err != nil {
		t.Fatal(err)
	}
	if done, err := e.verifyMutationArchiveHistory(t.Context(), runtime, strings.Repeat("b", 64), 32); err == nil || done {
		t.Fatal("new fence reused old completed verification without checking ancestors")
	}
	if readFixtureProducer(t, e, f, runtime).EvidenceFault == "" {
		t.Fatal("newly observed evidence loss not latched")
	}
}
