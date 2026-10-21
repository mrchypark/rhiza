package fencing

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func newSourceRetryTest(t *testing.T) (*sourceRetryState, BindReceipt, string) {
	t.Helper()
	r := testProducerReceipt()
	dir := filepath.Join(t.TempDir(), "retry")
	s, err := createSourceRetryState(dir, r)
	mustJournal(t, err)
	t.Cleanup(func() { mustJournal(t, s.Close()) })
	return s, r, dir
}

func sourceRetryIntent(step string) MutationIntent {
	intent := journalUpload(step)
	intent.OperationID = ""
	intent.Condition = MutationCondition{Mode: MutationCreateOnly}
	return intent
}

func stageSourceRetryTest(t *testing.T, s *sourceRetryState, intent MutationIntent) {
	t.Helper()
	stage, err := s.reserveUpload(uploadStageDraft(intent))
	mustJournal(t, err)
	mustJournal(t, s.sealedUpload(stage.Ticket, sourceSealedPayload{Ref: intent.PayloadRef, Digest: intent.PayloadDigest, Size: intent.PayloadSize}))
}

func reopenSourceRetryTest(t *testing.T, s *sourceRetryState, r BindReceipt, dir string) *sourceRetryState {
	t.Helper()
	mustJournal(t, s.Close())
	reopened, err := openSourceRetryState(dir, r)
	mustJournal(t, err)
	t.Cleanup(func() { mustJournal(t, reopened.Close()) })
	return reopened
}

func TestSourceRetryLostAdmissionResponseRetainsOriginalTuple(t *testing.T) {
	s, r, dir := newSourceRetryTest(t)
	intent := sourceRetryIntent("first")
	stageSourceRetryTest(t, s, intent)
	first, err := s.allocate(intent)
	mustJournal(t, err)
	p, err := NewMutationProducer(r.Identity, r.Producer.Runtime)
	mustJournal(t, err)
	mustJournal(t, p.Activate(r.Identity))
	mustJournal(t, p.Admit(first.Sequence, first.Entry.Intent))
	// The admission persisted, but its response never reached the source.
	s = reopenSourceRetryTest(t, s, r, dir)
	retry, err := s.allocate(intent)
	mustJournal(t, err)
	if retry.Sequence != first.Sequence || retry.Entry.Intent != first.Entry.Intent {
		t.Fatal("lost response or source restart changed original tuple")
	}
	if _, err := s.allocate(sourceRetryIntent("second")); !errors.Is(err, ErrPending) {
		t.Fatalf("allocator advanced past uncertain admission: %v", err)
	}
	mustJournal(t, p.Admit(retry.Sequence, retry.Entry.Intent))
	mustJournal(t, s.admitted(retry.Sequence, retry.Entry.Intent))
	stageSourceRetryTest(t, s, sourceRetryIntent("second"))
	second, err := s.allocate(sourceRetryIntent("second"))
	mustJournal(t, err)
	if second.Sequence != 2 || p.Entries[1].Resolution != nil || first.Entry.Intent.OperationID == second.Entry.Intent.OperationID {
		t.Fatal("known admission must allow the next distinct owner before provider outcome")
	}
	if p.LastAdmitted != 1 {
		t.Fatal("local allocation performed an authority admission")
	}
}

func TestSourceRetryPreservesEveryOriginalIntentField(t *testing.T) {
	s, _, _ := newSourceRetryTest(t)
	original := sourceRetryIntent("original")
	stageSourceRetryTest(t, s, original)
	step, err := s.allocate(original)
	mustJournal(t, err)
	for name, change := range map[string]func(*MutationIntent){
		"condition": func(i *MutationIntent) {
			i.Condition = MutationCondition{Mode: MutationMatchGeneration, Generation: 12}
		},
		"scope":   func(i *MutationIntent) { i.StoreScopeHash = strings.Repeat("b", 64) },
		"key":     func(i *MutationIntent) { i.Key = "another" },
		"stage":   func(i *MutationIntent) { i.PayloadRef = "another" },
		"digest":  func(i *MutationIntent) { i.PayloadDigest = strings.Repeat("b", 64) },
		"size":    func(i *MutationIntent) { i.PayloadSize++ },
		"version": func(i *MutationIntent) { i.ContractVersion++ },
		"request": func(i *MutationIntent) { i.OperationID = "another" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := original
			change(&changed)
			if _, err := s.allocate(changed); err == nil {
				t.Fatal("same owner refreshed its original intent")
			}
		})
	}
	retry, err := s.allocate(original)
	mustJournal(t, err)
	if retry.Entry.Intent != step.Entry.Intent || retry.Sequence != 1 {
		t.Fatal("rejected retry mutated saved tuple")
	}
}

func TestSourceRetryMissingStageReferenceConsumesNoSequence(t *testing.T) {
	s, _, _ := newSourceRetryTest(t)
	intent := sourceRetryIntent("first")
	intent.PayloadRef = ""
	if _, err := s.allocate(intent); err == nil {
		t.Fatal("missing stage reference consumed a sequence")
	}
	stageSourceRetryTest(t, s, sourceRetryIntent("first"))
	step, err := s.allocate(sourceRetryIntent("first"))
	mustJournal(t, err)
	if step.Sequence != 1 {
		t.Fatal("missing stage reference left a sequence gap")
	}
}

func TestSourceRetryRejectsInvalidOrReplacedOriginalResult(t *testing.T) {
	s, r, _ := newSourceRetryTest(t)
	stageSourceRetryTest(t, s, sourceRetryIntent("first"))
	step, err := s.allocate(sourceRetryIntent("first"))
	mustJournal(t, err)
	p, err := NewMutationProducer(r.Identity, r.Producer.Runtime)
	mustJournal(t, err)
	mustJournal(t, p.Activate(r.Identity))
	mustJournal(t, p.Admit(step.Sequence, step.Entry.Intent))
	mustJournal(t, p.Prepare(step.Sequence, journalPreparation(0)))
	hash, err := p.PreparationHash(step.Sequence)
	mustJournal(t, err)
	mustJournal(t, p.Resolve(step.Sequence, MutationResolution{Outcome: MutationCommitted,
		PreparationHash: hash, ReceiptDigest: journalTestDigest, PayloadDigest: step.Entry.Intent.PayloadDigest}))
	valid := p.Entries[step.Sequence]
	for name, change := range map[string]func(*MutationEntry){
		"scope":      func(e *MutationEntry) { e.Intent.StoreScopeHash = strings.Repeat("b", 64) },
		"pending":    func(e *MutationEntry) { e.Resolution = nil },
		"receipt":    func(e *MutationEntry) { e.Resolution.ReceiptDigest = "" },
		"payload":    func(e *MutationEntry) { e.Resolution.PayloadDigest = strings.Repeat("b", 64) },
		"condition":  func(e *MutationEntry) { e.Preparation.Generation = 12 },
		"excluded":   func(e *MutationEntry) { e.Resolution.Outcome = MutationExcluded; e.Resolution.PayloadDigest = "" },
		"copy claim": func(e *MutationEntry) { e.Resolution.AllCopiesTerminal = true },
	} {
		t.Run(name, func(t *testing.T) {
			bad := cloneSourceRetryStep(sourceRetryStep{Entry: valid}).Entry
			change(&bad)
			if err := s.resolved(step.Sequence, bad); err == nil {
				t.Fatal("invalid provider result became durable source success")
			}
		})
	}
	mustJournal(t, s.resolved(step.Sequence, valid))
	replacement := cloneSourceRetryStep(sourceRetryStep{Entry: valid}).Entry
	replacement.Resolution.ReceiptDigest = strings.Repeat("b", 64)
	if err := s.resolved(step.Sequence, replacement); err == nil {
		t.Fatal("replacement evidence overwrote the original result")
	}
}

func TestSourceRetryPersistenceUncertaintyPoisonsUntilReopen(t *testing.T) {
	for _, boundary := range []string{"file", "directory"} {
		t.Run(boundary, func(t *testing.T) {
			s, r, dir := newSourceRetryTest(t)
			original := sourceRetryIntent("first")
			stageSourceRetryTest(t, s, original)
			fault := errors.New("injected sync failure")
			if boundary == "file" {
				s.syncFile = func(*os.File) error { return fault }
			} else {
				s.syncDir = func(string) error { return fault }
			}
			returned, err := s.allocate(original)
			if !errors.Is(err, fault) || returned.Sequence != 0 {
				t.Fatalf("uncertain persistence exposed an admissible tuple: %+v %v", returned, err)
			}
			if _, err := s.allocate(sourceRetryIntent("second")); !errors.Is(err, fault) {
				t.Fatal("uncertain writer allocated another request")
			}
			s = reopenSourceRetryTest(t, s, r, dir)
			recovered, err := s.allocate(original)
			mustJournal(t, err)
			if recovered.Sequence != 1 || recovered.Entry.Intent.OperationID != MutationRequestID(r.Producer.ProducerUID, 1) {
				t.Fatal("reopen skipped or renumbered original prospective work")
			}
		})
	}
}

func TestSourceRetryMissingChangedOrCorruptHeaderFailsClosed(t *testing.T) {
	s, r, dir := newSourceRetryTest(t)
	mustJournal(t, s.Close())
	changed := r.clone()
	changed.Producer.ProducerUID = "replacement"
	if unexpected, err := openSourceRetryState(dir, changed); err == nil {
		_ = unexpected.Close()
		t.Fatal("changed producer accepted old owner state")
	}
	changed = r.clone()
	changed.Identity.WALIdentity = "replacement"
	if unexpected, err := openSourceRetryState(dir, changed); err == nil {
		_ = unexpected.Close()
		t.Fatal("changed WAL accepted old owner state")
	}
	path := filepath.Join(dir, "retry.json")
	data, err := os.ReadFile(path)
	mustJournal(t, err)
	corrupt := strings.Replace(string(data), "pinned-producer", "changed-producer", 1)
	mustJournal(t, os.WriteFile(path, []byte(corrupt), 0600))
	if unexpected, err := openSourceRetryState(dir, r); err == nil {
		_ = unexpected.Close()
		t.Fatal("corrupt header accepted")
	}
	mustJournal(t, os.Remove(path))
	if unexpected, err := openSourceRetryState(dir, r); err == nil {
		_ = unexpected.Close()
		t.Fatal("missing retry state reconstructed an old allocator")
	}
	if unexpected, err := createSourceRetryState(dir, r); err == nil {
		_ = unexpected.Close()
		t.Fatal("creation was used as missing-state recovery")
	}
}

func TestSourceRetryExclusiveOwnerAndBoundedPressure(t *testing.T) {
	s, r, dir := newSourceRetryTest(t)
	if other, err := openSourceRetryState(dir, r); err == nil {
		_ = other.Close()
		t.Fatal("concurrent allocator acquired the same producer")
	}
	var first sourceRetryStep
	for i := range MaxMutationJournalEntries {
		intent := sourceRetryIntent("same-bytes")
		intent.OwnerStep.Attempt = uint64(i) + 1
		stageSourceRetryTest(t, s, intent)
		step, err := s.allocate(intent)
		mustJournal(t, err)
		mustJournal(t, s.admitted(step.Sequence, step.Entry.Intent))
		if i == 0 {
			first = step
		}
	}
	newOwner := sourceRetryIntent("new-owner")
	if _, err := s.allocate(newOwner); err == nil {
		t.Fatal("unbounded owner history allocated")
	}
	entry := first.Entry
	entry.DispatchClosed = true
	entry.Resolution = &MutationResolution{Outcome: MutationRejected, ReceiptDigest: journalTestDigest, AllCopiesTerminal: true}
	mustJournal(t, s.resolved(first.Sequence, entry))
	mustJournal(t, s.advance(first.Sequence, first.Entry.Intent))
	retry, err := s.allocate(first.Entry.Intent)
	mustJournal(t, err)
	if !retry.Advanced || retry.Entry.Resolution == nil || retry.Sequence != first.Sequence {
		t.Fatal("pressure erased terminal owner identity")
	}
	// Caller mutation cannot alter retained provider evidence.
	retry.Entry.Resolution.Outcome = MutationCommitted
	again, err := s.allocate(first.Entry.Intent)
	mustJournal(t, err)
	if again.Entry.Resolution.Outcome != MutationRejected {
		t.Fatal("caller mutated retained original result")
	}
}

func TestSourceRetryResultDurableBeforeOwnerAdvancement(t *testing.T) {
	for _, outcome := range []MutationOutcome{MutationCommitted, MutationExcluded} {
		t.Run(string(outcome), func(t *testing.T) {
			s, r, dir := newSourceRetryTest(t)
			intent := sourceRetryIntent("original")
			intent.Condition = MutationCondition{Mode: MutationMatchGeneration, Generation: 12}
			stageSourceRetryTest(t, s, intent)
			step, err := s.allocate(intent)
			mustJournal(t, err)
			if !errors.Is(s.advance(step.Sequence, step.Entry.Intent), ErrPending) {
				t.Fatal("owner advanced without original result")
			}
			mustJournal(t, s.admitted(step.Sequence, step.Entry.Intent))
			p, err := NewMutationProducer(r.Identity, r.Producer.Runtime)
			mustJournal(t, err)
			mustJournal(t, p.Activate(r.Identity))
			mustJournal(t, p.Admit(step.Sequence, step.Entry.Intent))
			mustJournal(t, p.Prepare(step.Sequence, journalPreparation(12)))
			hash, err := p.PreparationHash(step.Sequence)
			mustJournal(t, err)
			resolution := MutationResolution{Outcome: outcome, PreparationHash: hash, ReceiptDigest: journalTestDigest}
			if outcome == MutationCommitted {
				resolution.PayloadDigest = step.Entry.Intent.PayloadDigest
			}
			mustJournal(t, p.Resolve(step.Sequence, resolution))
			fault := errors.New("result rename sync failed")
			s.syncDir = func(string) error { return fault }
			if err := s.resolved(step.Sequence, p.Entries[step.Sequence]); !errors.Is(err, fault) {
				t.Fatal("uncertain result persistence returned success")
			}
			if !errors.Is(s.advance(step.Sequence, step.Entry.Intent), fault) {
				t.Fatal("owner advanced on uncertain result durability")
			}
			s = reopenSourceRetryTest(t, s, r, dir)
			retained, err := s.allocate(intent)
			mustJournal(t, err)
			if retained.Advanced || retained.Entry.Resolution == nil || retained.Entry.Resolution.Outcome != outcome {
				t.Fatal("recovery lost or reinterpreted original provider outcome")
			}
			mustJournal(t, s.advance(step.Sequence, step.Entry.Intent))
		})
	}
}
