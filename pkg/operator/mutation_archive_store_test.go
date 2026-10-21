package operator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

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
	f.mu.Lock()
	defer f.mu.Unlock()
	var p fencing.MutationProducer
	if err := json.Unmarshal([]byte(str(nested(f.objects[e.mutationProducerPath(runtime)], "data", "producer.json"))), &p); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestMutationArchiveStorePersistsImmutableContentAfterResponseLoss(t *testing.T) {
	e, f, id, runtime := newMutationStoreFixture(t)
	activateStoredProducer(t, e, id, runtime)
	appendSettledStoreMutation(t, e, runtime, 1)
	plan := storeCompactionPlan(t, e, runtime, 1)
	f.mu.Lock()
	f.lostArchiveResponse = true
	f.mu.Unlock()
	p, err := e.commitMutationArchive(t.Context(), runtime, plan)
	if err != nil || p.CompactedThrough != 1 || p.ArchiveHead == nil {
		t.Fatalf("readback did not resolve create response loss: %+v %v", p, err)
	}
	var archive object
	if err := e.Kube.Get(t.Context(), e.mutationArchivePath(p.ArchiveHead.Name), &archive); err != nil {
		t.Fatal(err)
	}
	if archive["immutable"] != true || str(nested(archive, "metadata", "uid")) != p.ArchiveHead.UID {
		t.Fatal("producer references mutable or unpinned archive")
	}
	asObject(archive["data"])["archive.json"] = "corrupt"
	if err := e.Kube.Put(t.Context(), e.mutationArchivePath(p.ArchiveHead.Name), archive, nil); err == nil {
		t.Fatal("fixture accepted immutable data update")
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
	e, f, id, runtime := newMutationStoreFixture(t)
	activateStoredProducer(t, e, id, runtime)
	for sequence := uint64(1); sequence <= 70; sequence++ {
		appendSettledStoreMutation(t, e, runtime, sequence)
		if _, err := e.commitMutationArchive(t.Context(), runtime, storeCompactionPlan(t, e, runtime, sequence)); err != nil {
			t.Fatal(err)
		}
	}
	f.mu.Lock()
	before := f.archiveReads
	f.mu.Unlock()
	appendSettledStoreMutation(t, e, runtime, 71)
	f.mu.Lock()
	reads := f.archiveReads - before
	f.mu.Unlock()
	if reads != 1 {
		t.Fatalf("normal admission traversed %d archive records instead of pinned head", reads)
	}
	state, err := e.change(t.Context(), "sts", "source", func(l *fencing.Ledger) error { return l.Close("fence", strings.Repeat("a", 64)) })
	if err != nil {
		t.Fatal(err)
	}
	for pass := 1; pass <= 3; pass++ {
		restarted := &FencingExecutor{Kube: e.Kube}
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

func TestMutationArchiveStoreMissingReplacedCorruptEvidenceLatches(t *testing.T) {
	for _, where := range []string{"head", "ancestor"} {
		for _, fault := range []string{"missing", "replaced", "corrupt", "mutable"} {
			t.Run(where+"/"+fault, func(t *testing.T) {
				e, f, id, runtime := newMutationStoreFixture(t)
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
				path := e.mutationArchivePath(ref.Name)
				// Deliberately bypass fixture immutability to model privileged
				// external loss/corruption, not a supported archive update.
				f.mu.Lock()
				backup, _ := json.Marshal(f.objects[path])
				switch fault {
				case "missing":
					delete(f.objects, path)
				case "replaced":
					asObject(f.objects[path]["metadata"])["uid"] = "replacement"
				case "corrupt":
					asObject(f.objects[path]["data"])["archive.json"] = "{}"
				case "mutable":
					f.objects[path]["immutable"] = false
				}
				f.mu.Unlock()
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
				f.mu.Lock()
				var restored object
				_ = json.Unmarshal(backup, &restored)
				f.objects[path] = restored
				f.mu.Unlock()
				if _, err := e.reserveMutationProducer(t.Context(), runtime); !errors.Is(err, fencing.ErrUnavailable) {
					t.Fatal("repair/retry silently cleared recorded evidence loss:", err)
				}
			})
		}
	}
}

func TestMutationArchiveStoreTransientReadFailureIsNotPermanentCorruption(t *testing.T) {
	e, f, id, runtime := newMutationStoreFixture(t)
	activateStoredProducer(t, e, id, runtime)
	appendSettledStoreMutation(t, e, runtime, 1)
	p, err := e.commitMutationArchive(t.Context(), runtime, storeCompactionPlan(t, e, runtime, 1))
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.archiveReadError = e.mutationArchivePath(p.ArchiveHead.Name)
	f.mu.Unlock()
	if _, err := e.reserveMutationProducer(t.Context(), runtime); err == nil {
		t.Fatal("API unavailable did not block current request")
	}
	if readFixtureProducer(t, e, f, runtime).EvidenceFault != "" {
		t.Fatal("transient 503 was misclassified as corrupt evidence")
	}
	f.mu.Lock()
	f.archiveReadError = ""
	f.mu.Unlock()
	if _, err := e.reserveMutationProducer(t.Context(), runtime); err != nil {
		t.Fatal(err)
	}
}

func TestMutationArchiveStoreLostCASAndOrphanRequireExactCommittedChain(t *testing.T) {
	e, f, id, runtime := newMutationStoreFixture(t)
	activateStoredProducer(t, e, id, runtime)
	appendSettledStoreMutation(t, e, runtime, 1)
	appendSettledStoreMutation(t, e, runtime, 2)
	a := storeCompactionPlan(t, e, runtime, 1)
	orphan := storeCompactionPlan(t, e, runtime, 2)
	ref, err := e.persistMutationArchive(t.Context(), a)
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.lostResponse = true
	f.mu.Unlock()
	_, err = e.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error { return p.CommitMutationCompaction(a.ProducerUID, a, ref) })
	if err == nil {
		t.Fatal("injected post-commit response loss not observed")
	}
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
	e, _, id, runtime := newMutationStoreFixture(t)
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
	restarted := &FencingExecutor{Kube: e.Kube, VerifyMutationEvidence: e.VerifyMutationEvidence}
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
	e, _, id, runtime := newMutationStoreFixture(t)
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
	e, f, id, runtime := newMutationStoreFixture(t)
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
	f.mu.Lock()
	delete(f.objects, e.mutationArchivePath(first.ArchiveHead.Name))
	f.mu.Unlock()
	if done, err := e.verifyMutationArchiveHistory(t.Context(), runtime, strings.Repeat("b", 64), 32); err == nil || done {
		t.Fatal("new fence reused old completed verification without checking ancestors")
	}
	if readFixtureProducer(t, e, f, runtime).EvidenceFault == "" {
		t.Fatal("newly observed evidence loss not latched")
	}
}
