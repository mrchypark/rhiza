package fencing

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

const compactionProducerUID = "producer-compaction"

func settledMutation(t *testing.T, p *MutationProducer, sequence uint64) {
	t.Helper()
	intent := journalUpload(fmt.Sprint(sequence))
	intent.OperationID = MutationRequestID(compactionProducerUID, sequence)
	mustJournal(t, p.Admit(sequence, intent))
	mustJournal(t, p.CancelUnprepared(sequence, journalTestDigest))
}

func archiveReference(plan MutationCompactionPlan) MutationArchiveReference {
	digest := plan.Digest()
	return MutationArchiveReference{Name: MutationArchiveName(digest), UID: fmt.Sprint("archive-", plan.Through), Digest: digest}
}

func TestMutationCompactionPreservesTailAndClosureAndRejectsOverlaps(t *testing.T) {
	p := newJournalProducer(t, "compaction")
	settledMutation(t, p, 1)
	settledMutation(t, p, 2)
	a, err := p.PlanMutationCompaction(compactionProducerUID, 1)
	mustJournal(t, err)
	b, err := p.PlanMutationCompaction(compactionProducerUID, 2)
	mustJournal(t, err)
	mustJournal(t, p.CommitMutationCompaction(compactionProducerUID, a, archiveReference(a)))
	rejectJournalUnchanged(t, p, func() error { return p.CommitMutationCompaction(compactionProducerUID, b, archiveReference(b)) })
	b, err = p.PlanMutationCompaction(compactionProducerUID, 2)
	mustJournal(t, err)
	// A tail-only CAS conflict may preserve the already archived exact prefix.
	settledMutation(t, p, 3)
	mustJournal(t, p.Close("fence", journalTestDigest))
	mustJournal(t, p.CommitMutationCompaction(compactionProducerUID, b, archiveReference(b)))
	mustJournal(t, p.CommitMutationCompaction(compactionProducerUID, b, archiveReference(b)))
	p = reloadProducer(t, p)
	if p.LastAdmitted != 3 || p.ClosedThrough != 3 || p.Phase != MutationClosed || p.CompactedThrough != 2 || len(p.Entries) != 1 || !p.BarrierComplete() {
		t.Fatalf("compaction changed current tail/closure: %+v", p)
	}
	if _, exists := p.Entries[3]; !exists {
		t.Fatal("concurrent tail was dropped")
	}
}

func TestMutationCompactionCannotSkipUnresolvedPrefixOrDispatch(t *testing.T) {
	p := newJournalProducer(t, "unresolved-prefix")
	intent := journalUpload("1")
	intent.OperationID = MutationRequestID(compactionProducerUID, 1)
	mustJournal(t, p.Admit(1, intent))
	settledMutation(t, p, 2)
	if _, err := p.PlanMutationCompaction(compactionProducerUID, 2); err == nil {
		t.Fatal("terminal suffix concealed unresolved prefix")
	}
	mustJournal(t, p.Prepare(1, journalPreparation(0)))
	mustJournal(t, p.GrantDispatch(1, "paused-before-send"))
	hash, err := p.PreparationHash(1)
	mustJournal(t, err)
	mustJournal(t, p.Resolve(1, MutationResolution{Outcome: MutationCommitted, PreparationHash: hash, ReceiptDigest: journalTestDigest, PayloadDigest: journalTestDigest}))
	mustJournal(t, p.SealDispatch(1))
	if _, err := p.PlanMutationCompaction(compactionProducerUID, 2); err == nil {
		t.Fatal("create-zero success hid an executable permission")
	}
	mustJournal(t, p.RecordDispatchTerminal(1, "paused-before-send", journalTestDigest))
	mustJournal(t, p.ConfirmCopiesTerminal(1, MutationCopyCompletion{PreparationHash: hash, ReceiptDigest: journalTestDigest}))
	plan, err := p.PlanMutationCompaction(compactionProducerUID, 2)
	mustJournal(t, err)
	mustJournal(t, p.CommitMutationCompaction(compactionProducerUID, plan, archiveReference(plan)))
}

func TestMutationCompactionNeverRecreatesArchivedDispatchRights(t *testing.T) {
	p := newJournalProducer(t, "late-worker")
	settledMutation(t, p, 1)
	plan, err := p.PlanMutationCompaction(compactionProducerUID, 1)
	mustJournal(t, err)
	mustJournal(t, p.CommitMutationCompaction(compactionProducerUID, plan, archiveReference(plan)))
	p = reloadProducer(t, p)
	intent := journalUpload("1")
	intent.OperationID = MutationRequestID(compactionProducerUID, 1)
	if err := p.Admit(1, intent); !errors.Is(err, ErrMutationArchived) {
		t.Fatalf("archived retry was not directed to lookup: %v", err)
	}
	for name, transition := range map[string]func() error{
		"prepare": func() error { return p.Prepare(1, journalPreparation(10)) },
		"resolve": func() error {
			return p.Resolve(1, MutationResolution{Outcome: MutationCommitted, ReceiptDigest: journalTestDigest})
		},
		"cancel":           func() error { return p.CancelUnprepared(1, journalTestDigest) },
		"dispatch":         func() error { return p.GrantDispatch(1, "late") },
		"seal":             func() error { return p.SealDispatch(1) },
		"dispatch receipt": func() error { return p.RecordDispatchTerminal(1, "late", journalTestDigest) },
		"copy receipt": func() error {
			return p.ConfirmCopiesTerminal(1, MutationCopyCompletion{ReceiptDigest: journalTestDigest})
		},
	} {
		t.Run(name, func(t *testing.T) { rejectJournalUnchanged(t, p, transition) })
	}
	// New requests have a distinct structural identity; a caller cannot treat
	// an arbitrary repeated business label as lifetime operation-ID deduplication.
	if MutationRequestID(compactionProducerUID, 1) == MutationRequestID(compactionProducerUID, 2) || MutationRequestID(compactionProducerUID, 1) == MutationRequestID("replacement", 1) {
		t.Fatal("request tuple is not structurally bound")
	}
	settledMutation(t, p, 2)
}

func TestArchivedStageRetirementCursorIsContiguousAndBoundedByWatermark(t *testing.T) {
	p := newJournalProducer(t, "stage-retirement-cursor")
	settledMutation(t, p, 1)
	plan, err := p.PlanMutationCompaction(compactionProducerUID, 1)
	mustJournal(t, err)
	mustJournal(t, p.CommitMutationCompaction(compactionProducerUID, plan, archiveReference(plan)))
	if err := p.AdvanceArchivedStageRetirement(2); err == nil {
		t.Fatal("retirement cursor skipped an archived sequence")
	}
	mustJournal(t, p.AdvanceArchivedStageRetirement(1))
	mustJournal(t, p.AdvanceArchivedStageRetirement(1))
	if err := p.AdvanceArchivedStageRetirement(2); err == nil {
		t.Fatal("retirement cursor advanced beyond archive coverage")
	}
	copy := *p
	copy.RetiredStageThrough = 2
	if err := copy.Validate(); err == nil {
		t.Fatal("producer accepted retirement cursor beyond compaction watermark")
	}
}

func TestMutationCompactionChecksExactSnapshotAndPersistedReference(t *testing.T) {
	p := newJournalProducer(t, "snapshot")
	settledMutation(t, p, 1)
	plan, err := p.PlanMutationCompaction(compactionProducerUID, 1)
	mustJournal(t, err)
	for name, change := range map[string]func(*MutationArchiveReference){
		"missing UID":  func(r *MutationArchiveReference) { r.UID = "" },
		"wrong name":   func(r *MutationArchiveReference) { r.Name = "other" },
		"wrong digest": func(r *MutationArchiveReference) { r.Digest = strings.Repeat("b", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			ref := archiveReference(plan)
			change(&ref)
			rejectJournalUnchanged(t, p, func() error { return p.CommitMutationCompaction(compactionProducerUID, plan, ref) })
		})
	}
	rejectJournalUnchanged(t, p, func() error { return p.CommitMutationCompaction("other-producer", plan, archiveReference(plan)) })
	// Prepared archive data does not share nested pointers with the live state.
	entry := p.Entries[1]
	entry.Resolution.ReceiptDigest = strings.Repeat("d", 64)
	p.Entries[1] = entry
	if plan.Entries[1].Resolution.ReceiptDigest != journalTestDigest {
		t.Fatal("archive snapshot aliases live mutable state")
	}
	rejectJournalUnchanged(t, p, func() error { return p.CommitMutationCompaction(compactionProducerUID, plan, archiveReference(plan)) })
	q := newJournalProducer(t, "legacy-op-ID")
	mustJournal(t, q.Admit(1, journalUpload("arbitrary-operation-label")))
	mustJournal(t, q.CancelUnprepared(1, journalTestDigest))
	if _, err := q.PlanMutationCompaction(compactionProducerUID, 1); err == nil {
		t.Fatal("legacy operation-ID contract silently compacted")
	}
}

func TestMutationCompactionThousandsOfReloadsKeepLiveStateBounded(t *testing.T) {
	p := newJournalProducer(t, "long-lived")
	archives := make(map[string]MutationCompactionPlan)
	for cycle := uint64(1); cycle <= 1024; cycle++ {
		for range 4 {
			settledMutation(t, p, p.LastAdmitted+1)
		}
		plan, err := p.PlanMutationCompaction(compactionProducerUID, p.LastAdmitted)
		mustJournal(t, err)
		ref := archiveReference(plan)
		archives[ref.UID] = plan
		mustJournal(t, p.CommitMutationCompaction(compactionProducerUID, plan, ref))
		p = reloadProducer(t, p)
		if len(p.Entries) != 0 || p.CompactedThrough != cycle*4 || p.LastAdmitted != cycle*4 {
			t.Fatal("live work grew with historical sequence count")
		}
		data, err := json.Marshal(p)
		mustJournal(t, err)
		if len(data) > 3000 {
			t.Fatalf("live journal grew to %d bytes", len(data))
		}
	}
	// Reconstruct history by exact predecessor links, not just a larger watermark.
	next := p.ArchiveHead
	through := p.CompactedThrough
	links := 0
	for next != nil {
		plan, exists := archives[next.UID]
		if !exists || next.Digest != plan.Digest() || plan.Through != through {
			t.Fatal("archive chain does not cover compacted interval")
		}
		mustJournal(t, plan.Validate())
		through, next = plan.PreviousThrough, plan.Previous
		links++
	}
	if links != 1024 || through != 0 {
		t.Fatal("history was discarded instead of archived")
	}
	mustJournal(t, p.Close("fence", journalTestDigest))
	if !p.BarrierComplete() {
		t.Fatal("closed live journal is not structurally complete")
	}
}
