package fencing

import (
	"fmt"
	"strings"
	"testing"
)

func TestMutationDispatchPausedPermissionPreventsCopyCompletion(t *testing.T) {
	p := newJournalProducer(t, "paused-dispatch")
	mustJournal(t, p.Admit(1, journalUpload("create")))
	mustJournal(t, p.Prepare(1, journalPreparation(0)))
	// The worker already has executable permission, but has sent no request.
	// Looking only at requests observed by the provider would miss this worker.
	mustJournal(t, p.GrantDispatch(1, "paused-before-send"))
	mustJournal(t, p.GrantDispatch(1, "finished-copy"))
	mustJournal(t, p.GrantDispatch(1, "finished-copy"))
	mustJournal(t, p.RecordDispatchTerminal(1, "finished-copy", journalTestDigest))
	hash, err := p.PreparationHash(1)
	mustJournal(t, err)
	r := MutationResolution{Outcome: MutationCommitted, PreparationHash: hash, ReceiptDigest: journalTestDigest, PayloadDigest: journalTestDigest}
	mustJournal(t, p.Resolve(1, r))
	mustJournal(t, p.Close("fence", journalTestDigest))
	mustJournal(t, p.SealDispatch(1))
	p = reloadProducer(t, p)
	completion := MutationCopyCompletion{PreparationHash: hash, ReceiptDigest: strings.Repeat("b", 64)}
	rejectJournalUnchanged(t, p, func() error { return p.ConfirmCopiesTerminal(1, completion) })
	if p.BarrierComplete() {
		t.Fatal("paused dispatch right was mistaken for no future effects")
	}
	rejectJournalUnchanged(t, p, func() error { return p.GrantDispatch(1, "late-copy") })
	// Only terminal evidence for the paused permission removes the obligation.
	mustJournal(t, p.RecordDispatchTerminal(1, "paused-before-send", strings.Repeat("c", 64)))
	mustJournal(t, p.ConfirmCopiesTerminal(1, completion))
	if !reloadProducer(t, p).BarrierComplete() {
		t.Fatal("fully sealed and settled permissions failed to close")
	}
	if *p.Entries[1].Resolution != r {
		t.Fatal("later dispatch evidence erased original commitment")
	}
	rejectJournalUnchanged(t, p, func() error { return p.GrantDispatch(1, "paused-before-send") })
}

func TestMutationDispatchPermissionSetIsBoundedAndImmutable(t *testing.T) {
	p := newJournalProducer(t, "permissions")
	mustJournal(t, p.Admit(1, journalUpload("op")))
	rejectJournalUnchanged(t, p, func() error { return p.GrantDispatch(1, "unprepared") })
	mustJournal(t, p.Prepare(1, journalPreparation(10)))
	for i := range MaxMutationDispatches {
		mustJournal(t, p.GrantDispatch(1, fmt.Sprint(i)))
	}
	rejectJournalUnchanged(t, p, func() error { return p.GrantDispatch(1, "overflow") })
	rejectJournalUnchanged(t, p, func() error { return p.RecordDispatchTerminal(1, "unknown", journalTestDigest) })
	mustJournal(t, p.RecordDispatchTerminal(1, "0", journalTestDigest))
	mustJournal(t, p.RecordDispatchTerminal(1, "0", journalTestDigest))
	rejectJournalUnchanged(t, p, func() error { return p.RecordDispatchTerminal(1, "0", strings.Repeat("d", 64)) })
	rejectJournalUnchanged(t, p, func() error { return p.GrantDispatch(1, "0") })
	mustJournal(t, p.SealDispatch(1))
	mustJournal(t, p.SealDispatch(1))
	hash, err := p.PreparationHash(1)
	mustJournal(t, err)
	resolution := MutationResolution{Outcome: MutationRejected, PreparationHash: hash, ReceiptDigest: journalTestDigest, AllCopiesTerminal: true}
	rejectJournalUnchanged(t, p, func() error { return p.Resolve(1, resolution) })
	for i := 1; i < MaxMutationDispatches; i++ {
		mustJournal(t, p.RecordDispatchTerminal(1, fmt.Sprint(i), journalTestDigest))
	}
	mustJournal(t, p.Resolve(1, resolution))
	mustJournal(t, p.Close("fence", journalTestDigest))
	if !p.BarrierComplete() {
		t.Fatal("settled rejected operation left unresolved obligation")
	}
}
