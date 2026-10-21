package fencing

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
)

var journalTestDigest = strings.Repeat("a", 64)

func newJournalProducer(t *testing.T, run string) *MutationProducer {
	t.Helper()
	id := testIdentity()
	id.RunID = run
	id.WALIdentity = "wal-" + run
	p, err := NewMutationProducer(id, enrolledRuntime(id, "container-"+run))
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Activate(id); err != nil {
		t.Fatal(err)
	}
	return p
}

func journalUpload(operation string) MutationIntent {
	return MutationIntent{ContractVersion: MutationIntentVersion, StoreScopeHash: journalTestDigest,
		OwnerStep: MutationOwnerStep{Owner: "test", Workflow: "publication", Step: operation, Attempt: 1},
		Condition: MutationCondition{Mode: MutationUnconditional}, OperationID: operation,
		Kind: "upload", Key: "source/object", PayloadRef: "staged/" + operation, PayloadDigest: journalTestDigest, PayloadSize: 1}
}

func journalPreparation(generation int64) MutationPreparation {
	return MutationPreparation{Profile: "gcs-json-v1", StoreScopeHash: journalTestDigest, GenerationSet: true, Generation: generation}
}

func reloadProducer(t *testing.T, p *MutationProducer) *MutationProducer {
	t.Helper()
	data, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var restored MutationProducer
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}
	if err := restored.Validate(); err != nil {
		t.Fatal(err)
	}
	return &restored
}

func mustJournal(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func rejectJournalUnchanged(t *testing.T, p *MutationProducer, transition func() error) {
	t.Helper()
	before, err := json.Marshal(p)
	mustJournal(t, err)
	if err := transition(); err == nil {
		t.Fatal("unsafe transition accepted")
	}
	after, err := json.Marshal(p)
	mustJournal(t, err)
	if string(before) != string(after) {
		t.Fatal("rejected transition mutated state")
	}
}

func TestMutationJournalClosureIncludesAdmittedUnscheduledWork(t *testing.T) {
	p := newJournalProducer(t, "old")
	mustJournal(t, p.Admit(1, journalUpload("op-1")))
	mustJournal(t, p.Close("fence", journalTestDigest))
	p = reloadProducer(t, p)
	if p.ClosedThrough != 1 || p.BarrierComplete() {
		t.Fatal("closure omitted admitted but unscheduled work")
	}
	rejectJournalUnchanged(t, p, func() error { return p.Admit(2, journalUpload("late")) })
	// A lost admission response is recoverable, but is not a dispatch permit.
	mustJournal(t, p.Admit(1, journalUpload("op-1")))
	mustJournal(t, p.CancelUnprepared(1, journalTestDigest))
	p = reloadProducer(t, p)
	if !p.BarrierComplete() {
		t.Fatal("durably cancelled unscheduled work did not settle closure")
	}
	rejectJournalUnchanged(t, p, func() error { return p.Prepare(1, journalPreparation(7)) })
	rejectJournalUnchanged(t, p, func() error { return p.Admit(1, journalUpload("replacement")) })
	rejectJournalUnchanged(t, p, func() error { return p.Close("other-fence", journalTestDigest) })
	mustJournal(t, p.Close("fence", journalTestDigest))
}

func TestMutationJournalAdmittedWorkCanSettleAfterClosure(t *testing.T) {
	p := newJournalProducer(t, "settle")
	mustJournal(t, p.Admit(1, journalUpload("admitted")))
	mustJournal(t, p.Close("fence", journalTestDigest))
	// Closing new admission cannot strand existing obligations. This remains
	// legal only for the descriptor bound to this previously admitted sequence.
	mustJournal(t, p.Prepare(1, journalPreparation(9)))
	hash, err := p.PreparationHash(1)
	mustJournal(t, err)
	r := MutationResolution{Outcome: MutationCommitted, PreparationHash: hash, ReceiptDigest: journalTestDigest, PayloadDigest: journalTestDigest}
	mustJournal(t, p.Resolve(1, r))
	mustJournal(t, p.Resolve(1, r))
	if !reloadProducer(t, p).BarrierComplete() {
		t.Fatal("nonzero fixed-generation commitment did not settle obligation")
	}
	r.ReceiptDigest = strings.Repeat("b", 64)
	rejectJournalUnchanged(t, p, func() error { return p.Resolve(1, r) })
}

func TestMutationJournalCreateZeroCannotHideDelayedCopy(t *testing.T) {
	for _, profile := range []string{"gcs-json-v1", "gcs-resumable-v1"} {
		t.Run(profile, func(t *testing.T) {
			p := newJournalProducer(t, profile)
			mustJournal(t, p.Admit(1, journalUpload("create-only")))
			prepared := journalPreparation(0)
			prepared.Profile = profile
			if profile == "gcs-resumable-v1" {
				prepared.SessionRef = "private/session-A"
			}
			mustJournal(t, p.Prepare(1, prepared))
			hash, err := p.PreparationHash(1)
			mustJournal(t, err)
			r := MutationResolution{Outcome: MutationCommitted, PreparationHash: hash, ReceiptDigest: journalTestDigest, PayloadDigest: journalTestDigest}
			mustJournal(t, p.Resolve(1, r))
			mustJournal(t, p.Close("fence", journalTestDigest))
			p = reloadProducer(t, p)
			if p.BarrierComplete() {
				t.Fatal("one create-only success incorrectly excludes delayed copies after GC deletion")
			}
			// Preserve the original uncertain-copies receipt, then append distinct
			// evidence only after the trusted executor settles all copies/sessions.
			completion := MutationCopyCompletion{PreparationHash: hash, ReceiptDigest: strings.Repeat("b", 64)}
			rejectJournalUnchanged(t, p, func() error { return p.ConfirmCopiesTerminal(1, completion) })
			mustJournal(t, p.SealDispatch(1))
			mustJournal(t, p.ConfirmCopiesTerminal(1, completion))
			mustJournal(t, p.ConfirmCopiesTerminal(1, completion))
			p = reloadProducer(t, p)
			if !p.BarrierComplete() || *p.Entries[1].Resolution != r {
				t.Fatal("copy evidence did not settle closure or erased original outcome")
			}
			completion.ReceiptDigest = strings.Repeat("c", 64)
			rejectJournalUnchanged(t, p, func() error { return p.ConfirmCopiesTerminal(1, completion) })
		})
	}
}

func TestMutationJournalExclusionIsNotCommitment(t *testing.T) {
	for _, generation := range []int64{0, 42} {
		t.Run(fmt.Sprint(generation), func(t *testing.T) {
			p := newJournalProducer(t, "exclude")
			mustJournal(t, p.Admit(1, journalUpload("unknown")))
			mustJournal(t, p.Prepare(1, journalPreparation(generation)))
			hash, err := p.PreparationHash(1)
			mustJournal(t, err)
			r := MutationResolution{Outcome: MutationExcluded, PreparationHash: hash, ReceiptDigest: journalTestDigest}
			if generation == 0 {
				rejectJournalUnchanged(t, p, func() error { return p.Resolve(1, r) })
				return
			}
			mustJournal(t, p.Resolve(1, r))
			mustJournal(t, p.Close("fence", journalTestDigest))
			if !p.BarrierComplete() || p.Entries[1].Resolution.Outcome == MutationCommitted {
				t.Fatal("future-effect exclusion was confused with an acknowledged commit")
			}
			r.Outcome, r.PayloadDigest = MutationCommitted, journalTestDigest
			rejectJournalUnchanged(t, p, func() error { return p.Resolve(1, r) })
		})
	}
}

func TestMutationJournalPreparationAndReceiptsBindExactProducer(t *testing.T) {
	p := newJournalProducer(t, "first")
	q := newJournalProducer(t, "second")
	for _, producer := range []*MutationProducer{p, q} {
		mustJournal(t, producer.Admit(1, journalUpload("same-op")))
		mustJournal(t, producer.Prepare(1, journalPreparation(7)))
	}
	hash, err := p.PreparationHash(1)
	mustJournal(t, err)
	receipt := MutationResolution{Outcome: MutationCommitted, PreparationHash: hash, ReceiptDigest: journalTestDigest, PayloadDigest: journalTestDigest}
	rejectJournalUnchanged(t, q, func() error { return q.Resolve(1, receipt) })
	for _, change := range []func(*MutationProducer){
		func(v *MutationProducer) { v.WALIdentity = "other-wal" },
		func(v *MutationProducer) { v.Runtime.BootID = "other-boot" },
		func(v *MutationProducer) { v.Runtime.ContainerID = "other-container" },
	} {
		v := reloadProducer(t, p)
		change(v)
		rejectJournalUnchanged(t, v, func() error { return v.Resolve(1, receipt) })
	}
	prepared := journalPreparation(8)
	rejectJournalUnchanged(t, p, func() error { return p.Prepare(1, prepared) })
	rejectJournalUnchanged(t, p, func() error { return p.CancelUnprepared(1, journalTestDigest) })
	receipt.PayloadDigest = strings.Repeat("b", 64)
	rejectJournalUnchanged(t, p, func() error { return p.Resolve(1, receipt) })
	receipt.Outcome, receipt.PayloadDigest = MutationRejected, ""
	rejectJournalUnchanged(t, p, func() error { return p.Resolve(1, receipt) })
	receipt.AllCopiesTerminal = true
	mustJournal(t, p.SealDispatch(1))
	mustJournal(t, p.Resolve(1, receipt))
}

func TestMutationJournalLateResumableSessionCannotChangePreparation(t *testing.T) {
	p := newJournalProducer(t, "session")
	mustJournal(t, p.Admit(1, journalUpload("resumable")))
	prepared := journalPreparation(0)
	prepared.Profile, prepared.SessionRef = "gcs-resumable-v1", "private/session-A"
	mustJournal(t, p.Prepare(1, prepared))
	prepared.SessionRef = "private/session-B"
	rejectJournalUnchanged(t, p, func() error { return p.Prepare(1, prepared) })
	hash, err := p.PreparationHash(1)
	mustJournal(t, err)
	mustJournal(t, p.SealDispatch(1))
	mustJournal(t, p.Resolve(1, MutationResolution{Outcome: MutationRejected, PreparationHash: hash, ReceiptDigest: journalTestDigest, AllCopiesTerminal: true}))
	rejectJournalUnchanged(t, p, func() error { return p.Prepare(1, prepared) })
	// Creating B externally is not an executable authorization. The executor
	// must not send a body for B unless this exact preparation CAS succeeded.
}

func TestMutationJournalBoundedStateAndDamagedReloadFailClosed(t *testing.T) {
	p := newJournalProducer(t, "capacity")
	rejectJournalUnchanged(t, p, func() error { return p.Admit(2, journalUpload("gap")) })
	for i := uint64(1); i <= MaxMutationJournalEntries; i++ {
		mustJournal(t, p.Admit(i, journalUpload(fmt.Sprintf("op-%d", i))))
		mustJournal(t, p.CancelUnprepared(i, journalTestDigest))
	}
	rejectJournalUnchanged(t, p, func() error { return p.Admit(65, journalUpload("over-capacity")) })
	rejectJournalUnchanged(t, p, func() error { return p.Admit(1, journalUpload("reuse-terminal")) })
	mustJournal(t, p.Close("fence", journalTestDigest))
	if !p.BarrierComplete() {
		t.Fatal("terminal bounded producer did not close")
	}
	for name, corrupt := range map[string]func(*MutationProducer){
		"missing map":            func(v *MutationProducer) { v.Entries = nil },
		"missing sequence":       func(v *MutationProducer) { delete(v.Entries, 7) },
		"short closure":          func(v *MutationProducer) { v.ClosedThrough-- },
		"forged watermark":       func(v *MutationProducer) { v.LastAdmitted = ^uint64(0) },
		"missing runtime":        func(v *MutationProducer) { v.Runtime.BootID = "" },
		"missing close identity": func(v *MutationProducer) { v.CloseRequestHash = "" },
		"duplicate operation": func(v *MutationProducer) {
			entry := v.Entries[2]
			entry.Intent.OperationID = v.Entries[1].Intent.OperationID
			v.Entries[2] = entry
		},
	} {
		t.Run(name, func(t *testing.T) {
			v := reloadProducer(t, p)
			corrupt(v)
			if v.Validate() == nil || v.BarrierComplete() {
				t.Fatal("damaged state authorized a barrier")
			}
			rejectJournalUnchanged(t, v, func() error { return v.Close("fence", journalTestDigest) })
		})
	}
	var missing MutationProducer
	if missing.Validate() == nil || missing.BarrierComplete() {
		t.Fatal("missing producer implicitly became complete")
	}
	rejectJournalUnchanged(t, &missing, func() error { return missing.Admit(1, journalUpload("late")) })
}

func TestMutationJournalReservedCanCloseAndSurvivorKeepsWriting(t *testing.T) {
	id := testIdentity()
	reserved, err := NewMutationProducer(id, enrolledRuntime(id, "reserved"))
	mustJournal(t, err)
	mustJournal(t, reserved.Close("fence", journalTestDigest))
	if !reloadProducer(t, reserved).BarrierComplete() {
		t.Fatal("enrolled but never activated producer cannot be closed")
	}
	rejectJournalUnchanged(t, reserved, func() error { return reserved.Activate(id) })
	survivor := newJournalProducer(t, "survivor")
	for i := uint64(1); i <= 10; i++ {
		mustJournal(t, survivor.Admit(i, journalUpload(fmt.Sprint(i))))
		mustJournal(t, survivor.CancelUnprepared(i, journalTestDigest))
		if !reserved.BarrierComplete() || survivor.Phase != MutationOpen {
			t.Fatal("voter closure depends on generation-wide quietness")
		}
	}
}

// The fixture persists JSON and a revision in the same CAS, matching the
// contract the eventual control-plane backend must implement. It is not
// Kubernetes or GCS qualification evidence.
func TestMutationJournalStaleCASCannotAdmitAcrossClosure(t *testing.T) {
	p := newJournalProducer(t, "cas")
	var mu sync.Mutex
	revision := 1
	durable, err := json.Marshal(p)
	mustJournal(t, err)
	load := func() (*MutationProducer, int) {
		mu.Lock()
		defer mu.Unlock()
		var v MutationProducer
		mustJournal(t, json.Unmarshal(durable, &v))
		return &v, revision
	}
	commit := func(v *MutationProducer, expected int) bool {
		mu.Lock()
		defer mu.Unlock()
		if expected != revision {
			return false
		}
		data, err := json.Marshal(v)
		mustJournal(t, err)
		durable, revision = data, revision+1
		return true
	}
	stale, oldRevision := load()
	mustJournal(t, stale.Admit(1, journalUpload("delayed")))
	closed, closeRevision := load()
	mustJournal(t, closed.Close("fence", journalTestDigest))
	if !commit(closed, closeRevision) || commit(stale, oldRevision) {
		t.Fatal("stale admitted value overwrote permanent closure")
	}
	current, _ := load()
	if !current.BarrierComplete() || current.LastAdmitted != 0 {
		t.Fatal("CAS lost closure or accepted stale admission")
	}
	// The inverse interleaving forces closure to reload and include work.
	p = newJournalProducer(t, "cas-inverse")
	durable, err = json.Marshal(p)
	mustJournal(t, err)
	revision++
	staleClose, staleRevision := load()
	mustJournal(t, staleClose.Close("fence", journalTestDigest))
	admitted, admitRevision := load()
	mustJournal(t, admitted.Admit(1, journalUpload("first")))
	if !commit(admitted, admitRevision) || commit(staleClose, staleRevision) {
		t.Fatal("stale close omitted an admission")
	}
	current, currentRevision := load()
	mustJournal(t, current.Close("fence", journalTestDigest))
	if !commit(current, currentRevision) || current.BarrierComplete() || current.ClosedThrough != 1 {
		t.Fatal("retried closure did not retain admitted unscheduled obligation")
	}
}
