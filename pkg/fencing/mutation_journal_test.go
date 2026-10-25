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
	return newJournalProducerForIdentity(t, id)
}

func newJournalProducerForIdentity(t *testing.T, id Identity) *MutationProducer {
	t.Helper()
	p, err := NewMutationProducer(id, enrolledRuntime(id, "container-"+id.RunID))
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
			mustJournal(t, p.GrantDispatch(1, "create-copy"))
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
			mustJournal(t, p.RecordDispatchTerminal(1, "create-copy", strings.Repeat("d", 64)))
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

func TestMutationJournalCopyCompletionRequiresDispatch(t *testing.T) {
	p := newJournalProducer(t, "copy-completion-without-dispatch")
	mustJournal(t, p.Admit(1, journalUpload("create-only")))
	prepared := journalPreparation(0)
	mustJournal(t, p.Prepare(1, prepared))
	hash, err := p.PreparationHash(1)
	mustJournal(t, err)
	mustJournal(t, p.Resolve(1, MutationResolution{Outcome: MutationCommitted, PreparationHash: hash,
		ReceiptDigest: journalTestDigest, PayloadDigest: journalTestDigest}))
	mustJournal(t, p.Close("fence", journalTestDigest))
	completion := MutationCopyCompletion{PreparationHash: hash, ReceiptDigest: strings.Repeat("b", 64)}
	rejectJournalUnchanged(t, p, func() error { return p.ConfirmCopiesTerminal(1, completion) })

	entry := p.Entries[1]
	entry.CopyCompletion = &completion
	p.Entries[1] = entry
	if err := p.Validate(); err == nil {
		t.Fatal("persisted copy completion without a granted dispatch was accepted")
	}
	entry.CopyCompletion = nil
	p.Entries[1] = entry
	resolution := *entry.Resolution
	resolution.AllCopiesTerminal = true
	rejectJournalUnchanged(t, p, func() error { return p.Resolve(1, resolution) })
}

func TestPreparedNoDispatchCancellationIsValidLookup(t *testing.T) {
	p := newJournalProducer(t, "prepared-no-dispatch-cancel")
	mustJournal(t, p.Admit(1, journalUpload("prepared-unsent")))
	mustJournal(t, p.Prepare(1, journalPreparation(12)))
	mustJournal(t, p.Close("fence", journalTestDigest))
	preparationHash, err := p.PreparationHash(1)
	mustJournal(t, err)
	mustJournal(t, p.CancelPreparedNoDispatch(1, preparationHash, journalTestDigest))
	if err := p.Validate(); err != nil {
		t.Fatalf("closed prepared cancellation failed producer validation: %v", err)
	}
	if err := validateMutationLookupEntry(p.Entries[1]); err != nil {
		t.Fatalf("closed prepared cancellation failed authority lookup validation: %v", err)
	}
	if len(p.Entries[1].Dispatches) != 0 || p.Entries[1].CopyCompletion != nil || !p.BarrierComplete() {
		t.Fatal("no-dispatch cancellation created physical completion evidence or left closure pending")
	}
}

func guardedCreateProducer(t *testing.T, profile string) (*MutationProducer, MutationGuardedLogicalResolution) {
	t.Helper()
	p := newJournalProducer(t, "guarded-create")
	return guardedCreateProducerWithProducer(t, p, profile)
}

func guardedCreateProducerForIdentity(t *testing.T, profile string, id Identity) (*MutationProducer, MutationGuardedLogicalResolution) {
	t.Helper()
	return guardedCreateProducerWithProducer(t, newJournalProducerForIdentity(t, id), profile)
}

func guardedCreateProducerWithProducer(t *testing.T, p *MutationProducer, profile string) (*MutationProducer, MutationGuardedLogicalResolution) {
	t.Helper()
	intent := journalUpload("guarded-create")
	intent.Condition = MutationCondition{Mode: MutationCreateOnly}
	mustJournal(t, p.Admit(1, intent))
	prepared := journalPreparation(0)
	prepared.Profile = profile
	mustJournal(t, p.Prepare(1, prepared))
	mustJournal(t, p.GrantDispatch(1, "original-grant"))
	mustJournal(t, p.SealDispatch(1))
	hash, err := p.PreparationHash(1)
	mustJournal(t, err)
	mustJournal(t, p.Resolve(1, MutationResolution{Outcome: MutationCommitted, PreparationHash: hash,
		ReceiptDigest: journalTestDigest, PayloadDigest: intent.PayloadDigest}))
	mustJournal(t, p.Close("guarded-fence", journalTestDigest))
	evidence := MutationGuardedLogicalResolution{Version: MutationGuardedLogicalResolutionVersion,
		Runtime: p.Runtime, WALIdentity: p.WALIdentity, ProducerUID: "producer-uid", Sequence: 1,
		Intent: intent, Preparation: prepared, PreparationHash: hash, DispatchID: "original-grant",
		Profile: MutationHeldExecutionProfile, GuardRootIdentity: journalTestDigest, GuardVersion: MutationGuardedLogicalResolutionVersion,
		GuardRef: "mutation-guards/create-only.json", GuardDigest: journalTestDigest,
		ObjectGeneration: 42, ObjectMetageneration: 1, ObjectSize: intent.PayloadSize, PayloadDigest: intent.PayloadDigest}
	return p, evidence
}

func TestMutationJournalGuardedLogicalResolutionPreservesPhysicalUncertainty(t *testing.T) {
	p, evidence := guardedCreateProducer(t, MutationHeldExecutionProfile)
	mustJournal(t, p.ResolveGuardedLogical(evidence.ProducerUID, 1, evidence))
	mustJournal(t, p.ResolveGuardedLogical(evidence.ProducerUID, 1, evidence))
	entry := reloadProducer(t, p).Entries[1]
	if !p.BarrierComplete() || entry.GuardedLogicalResolution == nil || entry.Resolution == nil || entry.Resolution.AllCopiesTerminal {
		t.Fatal("guarded logical resolution failed to settle only the logical barrier")
	}
	if dispatch := entry.Dispatches[evidence.DispatchID]; dispatch.ReceiptDigest != "" || !entry.DispatchClosed {
		t.Fatalf("guarded resolution rewrote original physical permission: %+v", dispatch)
	}
	if entry.Resolution.ReceiptDigest != journalTestDigest || entry.Resolution.PayloadDigest != evidence.PayloadDigest {
		t.Fatal("guarded resolution replaced the original committed receipt")
	}
	q := reloadProducer(t, p)
	rejectJournalUnchanged(t, q, func() error { return q.ResolveGuardedLogical("another-producer", 1, evidence) })
}

func TestMutationJournalGuardedLogicalResolutionSettlesUnknownOriginalWithoutTerminalizingGrant(t *testing.T) {
	p, evidence := guardedCreateProducer(t, MutationHeldExecutionProfile)
	entry := p.Entries[1]
	entry.Resolution = nil // the original provider response and receipt are unknown
	p.Entries[1] = entry
	mustJournal(t, p.ResolveGuardedLogical(evidence.ProducerUID, 1, evidence))
	resolved := reloadProducer(t, p).Entries[1]
	if !p.BarrierComplete() || resolved.Resolution == nil || resolved.Resolution.Outcome != MutationCommitted || resolved.Resolution.AllCopiesTerminal {
		t.Fatal("guarded evidence did not resolve only the unknown logical result")
	}
	if resolved.Dispatches[evidence.DispatchID].ReceiptDigest != "" {
		t.Fatal("logical resolution forged a terminal receipt for the original dispatch")
	}
}

func TestMutationCompactionRetainsGuardedLogicalEvidence(t *testing.T) {
	p := newJournalProducer(t, "guarded-compaction")
	producerUID := compactionProducerUID
	intent := journalUpload(MutationRequestID(producerUID, 1))
	intent.OperationID = MutationRequestID(producerUID, 1)
	intent.Condition = MutationCondition{Mode: MutationCreateOnly}
	mustJournal(t, p.Admit(1, intent))
	prepared := journalPreparation(0)
	prepared.Profile = MutationHeldExecutionProfile
	mustJournal(t, p.Prepare(1, prepared))
	mustJournal(t, p.GrantDispatch(1, "original-grant"))
	mustJournal(t, p.SealDispatch(1))
	hash, err := p.PreparationHash(1)
	mustJournal(t, err)
	mustJournal(t, p.Resolve(1, MutationResolution{Outcome: MutationCommitted, PreparationHash: hash,
		ReceiptDigest: journalTestDigest, PayloadDigest: intent.PayloadDigest}))
	mustJournal(t, p.Close("guarded-fence", journalTestDigest))
	evidence := MutationGuardedLogicalResolution{Version: MutationGuardedLogicalResolutionVersion,
		Runtime: p.Runtime, WALIdentity: p.WALIdentity, ProducerUID: producerUID, Sequence: 1, Intent: intent, Preparation: prepared,
		PreparationHash: hash, DispatchID: "original-grant", Profile: MutationHeldExecutionProfile,
		GuardRootIdentity: journalTestDigest, GuardVersion: MutationGuardedLogicalResolutionVersion,
		GuardRef: "mutation-guards/create-only.json", GuardDigest: journalTestDigest,
		ObjectGeneration: 42, ObjectMetageneration: 1, ObjectSize: intent.PayloadSize, PayloadDigest: intent.PayloadDigest}
	mustJournal(t, p.ResolveGuardedLogical(producerUID, 1, evidence))
	plan, err := p.PlanMutationCompaction(producerUID, 1)
	mustJournal(t, err)
	archived := plan.Entries[1]
	if archived.GuardedLogicalResolution == nil || *archived.GuardedLogicalResolution != evidence || archived.Dispatches["original-grant"].ReceiptDigest != "" {
		t.Fatal("compaction did not preserve the canonical guard and unresolved original permission")
	}
	mustJournal(t, p.CommitMutationCompaction(producerUID, plan, archiveReference(plan)))
	if p.CompactedThrough != 1 || len(p.Entries) != 0 {
		t.Fatal("guarded mutation was not compacted as a terminal logical entry")
	}
}

func TestMutationJournalGuardedLogicalResolutionRejectsMismatchedEvidence(t *testing.T) {
	mutations := map[string]func(*MutationGuardedLogicalResolution){
		"wrong runtime":          func(v *MutationGuardedLogicalResolution) { v.Runtime.BootID = "other" },
		"wrong WAL":              func(v *MutationGuardedLogicalResolution) { v.WALIdentity = "other" },
		"wrong producer":         func(v *MutationGuardedLogicalResolution) { v.ProducerUID = "" },
		"wrong sequence":         func(v *MutationGuardedLogicalResolution) { v.Sequence++ },
		"wrong intent":           func(v *MutationGuardedLogicalResolution) { v.Intent.Key = "other" },
		"wrong preparation":      func(v *MutationGuardedLogicalResolution) { v.Preparation.Generation = 1 },
		"wrong dispatch":         func(v *MutationGuardedLogicalResolution) { v.DispatchID = "other" },
		"wrong profile":          func(v *MutationGuardedLogicalResolution) { v.Profile = MutationExecutionProfile },
		"wrong root identity":    func(v *MutationGuardedLogicalResolution) { v.GuardRootIdentity = "invalid" },
		"wrong guard version":    func(v *MutationGuardedLogicalResolution) { v.GuardVersion++ },
		"wrong guard reference":  func(v *MutationGuardedLogicalResolution) { v.GuardRef = "../outside" },
		"wrong guard digest":     func(v *MutationGuardedLogicalResolution) { v.GuardDigest = "invalid" },
		"missing generation":     func(v *MutationGuardedLogicalResolution) { v.ObjectGeneration = 0 },
		"missing metageneration": func(v *MutationGuardedLogicalResolution) { v.ObjectMetageneration = 0 },
		"wrong size":             func(v *MutationGuardedLogicalResolution) { v.ObjectSize++ },
		"wrong bytes":            func(v *MutationGuardedLogicalResolution) { v.PayloadDigest = strings.Repeat("b", 64) },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			p, evidence := guardedCreateProducer(t, MutationHeldExecutionProfile)
			mutate(&evidence)
			rejectJournalUnchanged(t, p, func() error { return p.ResolveGuardedLogical(evidence.ProducerUID, 1, evidence) })
		})
	}
	t.Run("legacy unheld grant", func(t *testing.T) {
		p, evidence := guardedCreateProducer(t, MutationExecutionProfile)
		evidence.Profile = MutationHeldExecutionProfile
		rejectJournalUnchanged(t, p, func() error { return p.ResolveGuardedLogical(evidence.ProducerUID, 1, evidence) })
	})
}

func TestMutationJournalAssignedLearnerRuntimeAndGuardedResolution(t *testing.T) {
	id := testIdentity()
	id.RunID, id.WALIdentity, id.AssignmentID = "guarded-create", "wal-guarded-create", "learner-assignment"
	p, evidence := guardedCreateProducerForIdentity(t, MutationHeldExecutionProfile, id)
	if err := p.Validate(); err != nil {
		t.Fatalf("assigned learner producer did not validate: %v", err)
	}
	mustJournal(t, p.ResolveGuardedLogical(evidence.ProducerUID, 1, evidence))
	entry := p.Entries[1]
	if err := p.ValidateGuardedLogicalResolution(evidence.ProducerUID, 1, entry); err != nil {
		t.Fatalf("assigned learner guarded resolution did not validate: %v", err)
	}
	wrong := *entry.GuardedLogicalResolution
	wrong.Runtime.AssignmentID = "different-assignment"
	entry.GuardedLogicalResolution = &wrong
	if err := p.ValidateGuardedLogicalResolution(evidence.ProducerUID, 1, entry); err == nil {
		t.Fatal("guarded resolution with a mismatched assignment was accepted")
	}
}

func TestHeldProfileRetainsOrdinaryGenerationCASAndDeleteOperations(t *testing.T) {
	for _, test := range []struct {
		name   string
		intent MutationIntent
		gen    int64
	}{
		{name: "nonzero upload", intent: journalUpload("cas-upload"), gen: 17},
		{name: "exact delete", intent: MutationIntent{ContractVersion: MutationIntentVersion, StoreScopeHash: journalTestDigest,
			OwnerStep: MutationOwnerStep{Owner: "test", Workflow: "gc", Step: "delete", Attempt: 1},
			Condition: MutationCondition{Mode: MutationMatchGeneration, Generation: 17}, OperationID: "cas-delete", Kind: "delete", Key: "source/object"}, gen: 17},
	} {
		t.Run(test.name, func(t *testing.T) {
			prepared := MutationPreparation{Profile: MutationHeldExecutionProfile, StoreScopeHash: journalTestDigest, GenerationSet: true, Generation: test.gen}
			if err := validateMutationPreparation(test.intent, prepared); err != nil {
				t.Fatalf("held profile rejected ordinary source mutation: %v", err)
			}
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

func TestHeldCreateEligibilityPinsOnlyHeldZeroGenerationUploads(t *testing.T) {
	scopeHash := strings.Repeat("a", 64)
	intent := MutationIntent{Kind: "upload", StoreScopeHash: scopeHash, Condition: MutationCondition{Mode: MutationCreateOnly}}
	preparation := MutationPreparation{Profile: MutationHeldExecutionProfile, StoreScopeHash: scopeHash, GenerationSet: true}
	tests := []struct {
		name        string
		intent      MutationIntent
		preparation MutationPreparation
		eligible    bool
		valid       bool
	}{
		{name: "create only", intent: intent, preparation: preparation, eligible: true, valid: true},
		{name: "unconditional observed absent", intent: MutationIntent{Kind: "upload", StoreScopeHash: scopeHash, Condition: MutationCondition{Mode: MutationUnconditional}}, preparation: preparation, eligible: true, valid: true},
		{name: "unconditional fixed CAS", intent: MutationIntent{Kind: "upload", StoreScopeHash: scopeHash, Condition: MutationCondition{Mode: MutationUnconditional}}, preparation: MutationPreparation{Profile: MutationHeldExecutionProfile, StoreScopeHash: scopeHash, GenerationSet: true, Generation: 9}, valid: true},
		{name: "create only cannot target existing generation", intent: intent, preparation: MutationPreparation{Profile: MutationHeldExecutionProfile, StoreScopeHash: scopeHash, GenerationSet: true, Generation: 9}},
		{name: "legacy profile", intent: MutationIntent{Kind: "upload", StoreScopeHash: scopeHash, Condition: MutationCondition{Mode: MutationUnconditional}}, preparation: MutationPreparation{Profile: MutationExecutionProfile, StoreScopeHash: scopeHash, GenerationSet: true}, valid: true},
		{name: "delete", intent: MutationIntent{Kind: "delete", StoreScopeHash: scopeHash, Condition: MutationCondition{Mode: MutationMatchGeneration, Generation: 9}}, preparation: MutationPreparation{Profile: MutationHeldExecutionProfile, StoreScopeHash: scopeHash, GenerationSet: true}},
		{name: "generation not pinned", intent: MutationIntent{Kind: "upload", StoreScopeHash: scopeHash, Condition: MutationCondition{Mode: MutationUnconditional}}, preparation: MutationPreparation{Profile: MutationHeldExecutionProfile, StoreScopeHash: scopeHash}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsHeldCreateEligible(tt.intent, tt.preparation); got != tt.eligible {
				t.Fatalf("IsHeldCreateEligible() = %v, want %v", got, tt.eligible)
			}
			if err := validateMutationPreparation(tt.intent, tt.preparation); (err == nil) != tt.valid {
				t.Fatalf("validateMutationPreparation() error = %v, valid = %v", err, tt.valid)
			}
		})
	}
}
