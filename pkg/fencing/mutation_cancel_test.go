package fencing

import (
	"errors"
	"strings"
	"testing"
)

func TestMutationAllocatedCancellationConsumesOriginalSequence(t *testing.T) {
	p := newJournalProducer(t, "cancel")
	intent := journalUpload("original")
	mustJournal(t, p.AdmitAndCancel(1, intent, journalTestDigest))
	p = reloadProducer(t, p)
	entry := p.Entries[1]
	if p.LastAdmitted != 1 || entry.Intent != intent || entry.Preparation != nil || !entry.DispatchClosed || entry.Resolution == nil || entry.Resolution.Outcome != MutationRejected || !entry.Resolution.AllCopiesTerminal {
		t.Fatal("allocated cancellation lost its original terminal obligation")
	}
	mustJournal(t, p.AdmitAndCancel(1, intent, journalTestDigest))
	changed := intent
	changed.Condition = MutationCondition{Mode: MutationCreateOnly}
	rejectJournalUnchanged(t, p, func() error { return p.AdmitAndCancel(1, changed, journalTestDigest) })
	rejectJournalUnchanged(t, p, func() error { return p.Prepare(1, journalPreparation(12)) })
	mustJournal(t, p.Admit(2, journalUpload("next")))
}

func TestMutationAllocatedCancellationRejectsWithoutPartialAdmission(t *testing.T) {
	p := newJournalProducer(t, "invalid-cancel")
	rejectJournalUnchanged(t, p, func() error { return p.AdmitAndCancel(1, journalUpload("original"), "bad-receipt") })
	if p.LastAdmitted != 0 {
		t.Fatal("failed cancellation consumed a sequence")
	}
	mustJournal(t, p.Admit(1, journalUpload("original")))
	mustJournal(t, p.Prepare(1, journalPreparation(12)))
	rejectJournalUnchanged(t, p, func() error { return p.AdmitAndCancel(1, journalUpload("original"), journalTestDigest) })
	if p.Entries[1].Resolution != nil {
		t.Fatal("cancellation settled prepared work")
	}
}

func TestMutationAllocatedCancellationVersusClosure(t *testing.T) {
	for _, admitted := range []bool{false, true} {
		p := newJournalProducer(t, "closed-cancel")
		intent := journalUpload("original")
		if admitted {
			mustJournal(t, p.Admit(1, intent))
		}
		mustJournal(t, p.Close("close", journalTestDigest))
		p = reloadProducer(t, p)
		if !admitted {
			rejectJournalUnchanged(t, p, func() error {
				err := p.AdmitAndCancel(1, intent, journalTestDigest)
				if !errors.Is(err, ErrClosed) {
					t.Fatalf("closure winner is not definitive: %v", err)
				}
				return err
			})
		} else {
			mustJournal(t, p.AdmitAndCancel(1, intent, journalTestDigest))
			if !p.BarrierComplete() {
				t.Fatal("accepted unprepared obligation could not settle after closure")
			}
			rejectJournalUnchanged(t, p, func() error { return p.AdmitAndCancel(1, intent, strings.Repeat("b", 64)) })
		}
	}
}

func TestClosedMutationSealsDispatchAndCancelsPreparedUnsentEntry(t *testing.T) {
	p := newJournalProducer(t, "closed-prepared-unsent")
	intent := journalUpload("prepared-unsent")
	mustJournal(t, p.Admit(1, intent))
	mustJournal(t, p.Prepare(1, journalPreparation(12)))
	mustJournal(t, p.Close("fence", journalTestDigest))
	if !p.Entries[1].DispatchClosed {
		t.Fatal("producer closure did not seal dispatch admission")
	}
	rejectJournalUnchanged(t, p, func() error { return p.GrantDispatch(1, "late") })
	preparationHash, err := p.PreparationHash(1)
	mustJournal(t, err)
	mustJournal(t, p.CancelPreparedNoDispatch(1, preparationHash, journalTestDigest))
	p = reloadProducer(t, p)
	entry := p.Entries[1]
	if entry.Preparation == nil || entry.Resolution == nil || entry.Resolution.Outcome != MutationRejected ||
		entry.Resolution.PreparationHash != preparationHash || !entry.Resolution.AllCopiesTerminal || len(entry.Dispatches) != 0 || !p.BarrierComplete() {
		t.Fatalf("prepared entry without a dispatch was not safely terminalized: %+v", entry)
	}
	rejectJournalUnchanged(t, p, func() error { return p.CancelPreparedNoDispatch(1, preparationHash, strings.Repeat("b", 64)) })
}

func TestClosePreservesPreviouslyGrantedDispatchObligation(t *testing.T) {
	p := newJournalProducer(t, "closed-granted")
	mustJournal(t, p.Admit(1, journalUpload("granted-before-close")))
	mustJournal(t, p.Prepare(1, journalPreparation(12)))
	mustJournal(t, p.GrantDispatch(1, "already-granted"))
	mustJournal(t, p.Close("fence", journalTestDigest))
	entry := p.Entries[1]
	if !entry.DispatchClosed || len(entry.Dispatches) != 1 || p.BarrierComplete() {
		t.Fatal("closure discarded or failed to seal an existing dispatch obligation")
	}
	rejectJournalUnchanged(t, p, func() error { return p.CancelPreparedNoDispatch(1, p.preparationHash(1, entry), journalTestDigest) })
}
