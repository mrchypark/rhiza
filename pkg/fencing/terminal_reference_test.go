package fencing

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func terminalFixture(t *testing.T) (*Ledger, TerminalReference) {
	t.Helper()
	id := testIdentity()
	id.WALIdentity = ""
	l := &Ledger{SourceClusterID: id.SourceClusterID, RequireRuntimeBinding: true, RequireMutationJournal: true}
	runtime := enrolledRuntime(id, "container")
	mustJournal(t, l.RegisterEnrolled(id, runtime))
	mustJournal(t, l.BindMutationProducer(id, runtime, "producer-uid"))
	id.WALIdentity = "wal"
	mustJournal(t, l.Bind(id))
	mustJournal(t, l.CloseNodes("voter-fence", journalTestDigest, []string{id.NodeID}))
	return l, TerminalReference{
		EvidenceVersion: 2, Request: *l.Runs[id.RunID].Stop,
		RuntimeHash: RuntimeBindingDigest(runtime), ProducerUID: "producer-uid",
		ProofName: "rhiza-terminal-proof", ProofUID: "proof-uid", ProofDigest: journalTestDigest,
		RuntimeReceiptDigest: journalTestDigest, MutationReceiptDigest: journalTestDigest, StorageScopeDigest: journalTestDigest,
	}
}

func TestIndependentTerminalReferenceSurvivesVoterThenGenerationClosure(t *testing.T) {
	l, reference := terminalFixture(t)
	mustJournal(t, l.RecordIndependentTerminal(reference))
	mustJournal(t, l.RecordIndependentTerminal(reference))
	data, err := json.Marshal(l)
	mustJournal(t, err)
	var restored Ledger
	mustJournal(t, json.Unmarshal(data, &restored))
	mustJournal(t, restored.Close("generation-fence", strings.Repeat("b", 64)))
	got, err := restored.IndependentTerminal(reference.Request.Identity.RunID, journalTestDigest)
	mustJournal(t, err)
	if got != reference || *restored.Runs[reference.Request.Identity.RunID].Stop != reference.Request {
		t.Fatal("generation closure rewrote the original voter termination evidence")
	}
	// This is independent evidence, not an invented cooperative engine drain.
	run := restored.Runs[reference.Request.Identity.RunID]
	if run.Exited || run.Drain != nil || run.NeverBoundExit != nil || restored.Complete("generation-fence", strings.Repeat("b", 64), nil) {
		t.Fatal("independent reference fabricated cooperative completion")
	}
	got.ProofUID = "caller-mutated-copy"
	again, err := restored.IndependentTerminal(reference.Request.Identity.RunID, journalTestDigest)
	mustJournal(t, err)
	if again != reference {
		t.Fatal("caller changed retained evidence via returned copy")
	}
}

func TestIndependentTerminalReferenceRejectsChangedOrV1Evidence(t *testing.T) {
	l, reference := terminalFixture(t)
	for name, change := range map[string]func(*TerminalReference){
		"legacy evidence":           func(v *TerminalReference) { v.EvidenceVersion = 1 },
		"other run":                 func(v *TerminalReference) { v.Request.Identity.RunID = "other" },
		"other WAL":                 func(v *TerminalReference) { v.Request.Identity.WALIdentity = "other" },
		"other operation":           func(v *TerminalReference) { v.Request.OperationID = "other" },
		"other request":             func(v *TerminalReference) { v.Request.RequestHash = strings.Repeat("b", 64) },
		"other runtime":             func(v *TerminalReference) { v.RuntimeHash = strings.Repeat("b", 64) },
		"other producer":            func(v *TerminalReference) { v.ProducerUID = "other" },
		"missing proof name":        func(v *TerminalReference) { v.ProofName = "" },
		"missing proof uid":         func(v *TerminalReference) { v.ProofUID = "" },
		"missing proof digest":      func(v *TerminalReference) { v.ProofDigest = "" },
		"missing runtime evidence":  func(v *TerminalReference) { v.RuntimeReceiptDigest = "" },
		"missing mutation evidence": func(v *TerminalReference) { v.MutationReceiptDigest = "" },
		"missing storage identity":  func(v *TerminalReference) { v.StorageScopeDigest = "" },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := reference
			change(&candidate)
			before, err := json.Marshal(l)
			mustJournal(t, err)
			if l.RecordIndependentTerminal(candidate) == nil {
				t.Fatal("invalid evidence accepted")
			}
			after, err := json.Marshal(l)
			mustJournal(t, err)
			if string(before) != string(after) {
				t.Fatal("rejected evidence changed history")
			}
		})
	}
	mustJournal(t, l.RecordIndependentTerminal(reference))
	changed := reference
	changed.ProofUID = "replacement"
	if l.RecordIndependentTerminal(changed) == nil {
		t.Fatal("same run replaced immutable proof reference")
	}
}

func TestIndependentTerminalReferenceReloadRevalidatesEveryBinding(t *testing.T) {
	l, reference := terminalFixture(t)
	mustJournal(t, l.RecordIndependentTerminal(reference))
	for name, change := range map[string]func(*Ledger){
		"other storage": func(v *Ledger) {
			r := v.Runs[reference.Request.Identity.RunID]
			r.IndependentTerminal.StorageScopeDigest = strings.Repeat("c", 64)
			v.Runs[reference.Request.Identity.RunID] = r
		},
		"runtime deleted": func(v *Ledger) {
			r := v.Runs[reference.Request.Identity.RunID]
			r.Runtime = nil
			v.Runs[reference.Request.Identity.RunID] = r
		},
		"runtime replaced": func(v *Ledger) {
			r := v.Runs[reference.Request.Identity.RunID]
			r.Runtime.ContainerID = "replacement"
			v.Runs[reference.Request.Identity.RunID] = r
		},
		"producer replaced": func(v *Ledger) {
			r := v.Runs[reference.Request.Identity.RunID]
			r.MutationProducerUID = "replacement"
			v.Runs[reference.Request.Identity.RunID] = r
		},
		"stop deleted": func(v *Ledger) {
			r := v.Runs[reference.Request.Identity.RunID]
			r.Stop = nil
			v.Runs[reference.Request.Identity.RunID] = r
		},
		"other generation": func(v *Ledger) { v.SourceClusterID = "replacement" },
	} {
		t.Run(name, func(t *testing.T) {
			data, err := json.Marshal(l)
			mustJournal(t, err)
			var damaged Ledger
			mustJournal(t, json.Unmarshal(data, &damaged))
			change(&damaged)
			if _, err := damaged.IndependentTerminal(reference.Request.Identity.RunID, journalTestDigest); err == nil {
				t.Fatal("damaged/replaced identity retained usable evidence")
			}
		})
	}
	data, err := json.Marshal(l)
	mustJournal(t, err)
	var restored Ledger
	mustJournal(t, json.Unmarshal(data, &restored))
	got, err := restored.IndependentTerminal(reference.Request.Identity.RunID, journalTestDigest)
	mustJournal(t, err)
	if !reflect.DeepEqual(got, reference) {
		t.Fatal("exact reload changed evidence")
	}
}

func pendingTerminalFixture(t *testing.T) (*Ledger, TerminalReference) {
	t.Helper()
	id := testIdentity()
	id.WALIdentity = ""
	l := &Ledger{SourceClusterID: id.SourceClusterID, RequireRuntimeBinding: true, RequireMutationJournal: true}
	runtime := enrolledRuntime(id, "pending-container")
	mustJournal(t, l.RegisterEnrolledWithRole(id, runtime, StartupRoleLearnerPendingRemove))
	id.WALIdentity = "pending-wal"
	mustJournal(t, l.Bind(id))
	mustJournal(t, l.CloseNodes("pending-stop", journalTestDigest, []string{id.NodeID}))
	return l, TerminalReference{
		EvidenceVersion: 3, Request: *l.Runs[id.RunID].Stop, RuntimeHash: RuntimeBindingDigest(runtime),
		ProofName: "rhiza-terminal-pending", ProofUID: "proof-uid", ProofDigest: journalTestDigest,
		RuntimeReceiptDigest: journalTestDigest, StorageScopeDigest: journalTestDigest,
	}
}

func TestPendingLearnerTerminalReferenceRequiresClosedNoProducerProfile(t *testing.T) {
	l, reference := pendingTerminalFixture(t)
	mustJournal(t, l.RecordIndependentTerminal(reference))
	got, err := l.IndependentTerminal(reference.Request.Identity.RunID, journalTestDigest)
	mustJournal(t, err)
	if got != reference {
		t.Fatal("pending terminal reference changed on read")
	}
	for name, mutate := range map[string]func(*Ledger, *TerminalReference){
		"active role": func(v *Ledger, _ *TerminalReference) {
			run := v.Runs[reference.Request.Identity.RunID]
			run.Role = StartupRoleVoterActive
			v.Runs[reference.Request.Identity.RunID] = run
		},
		"promotion": func(v *Ledger, _ *TerminalReference) {
			run := v.Runs[reference.Request.Identity.RunID]
			run.Promotion = &LearnerPromotion{OperationID: "promotion", EvidenceHash: journalTestDigest}
			v.Runs[reference.Request.Identity.RunID] = run
		},
		"producer uid":          func(_ *Ledger, ref *TerminalReference) { ref.ProducerUID = "forged" },
		"mutation digest":       func(_ *Ledger, ref *TerminalReference) { ref.MutationReceiptDigest = journalTestDigest },
		"changed original stop": func(_ *Ledger, ref *TerminalReference) { ref.Request.OperationID = "replacement-stop" },
		"runtime profile missing": func(v *Ledger, _ *TerminalReference) { v.RequireRuntimeBinding = false },
		"scope profile missing": func(v *Ledger, _ *TerminalReference) { v.RequireMutationJournal = false },
		"producer scope present": func(v *Ledger, _ *TerminalReference) {
			run := v.Runs[reference.Request.Identity.RunID]
			run.MutationScopeHash = journalTestDigest
			v.Runs[reference.Request.Identity.RunID] = run
		},
		"producer execution profile present": func(v *Ledger, _ *TerminalReference) {
			run := v.Runs[reference.Request.Identity.RunID]
			run.MutationProfile = "unexpected"
			v.Runs[reference.Request.Identity.RunID] = run
		},
		"producer stage incarnation present": func(v *Ledger, _ *TerminalReference) {
			run := v.Runs[reference.Request.Identity.RunID]
			run.MutationStageIncarnation = journalTestDigest
			v.Runs[reference.Request.Identity.RunID] = run
		},
		"not closed":          func(v *Ledger, _ *TerminalReference) { v.ClosedBy = ""; v.ClosedNodes = nil },
		"unsupported version": func(_ *Ledger, ref *TerminalReference) { ref.EvidenceVersion = 4 },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := *l
			candidate.Runs = map[string]Run{}
			for id, run := range l.Runs {
				candidate.Runs[id] = run
			}
			candidate.ClosedNodes = map[string]StopRequest{}
			for id, stop := range l.ClosedNodes {
				candidate.ClosedNodes[id] = stop
			}
			ref := reference
			mutate(&candidate, &ref)
			run := candidate.Runs[reference.Request.Identity.RunID]
			run.IndependentTerminal = &ref
			candidate.Runs[reference.Request.Identity.RunID] = run
			if _, err := candidate.IndependentTerminal(reference.Request.Identity.RunID, journalTestDigest); err == nil {
				t.Fatal("invalid pending terminal reference was accepted")
			}
		})
	}
}

func TestPendingLearnerTerminalPromotionClosureInterleavings(t *testing.T) {
	// Promotion wins first: a stale pending V3 reference must no longer satisfy
	// the V2/active-voter terminal contract or be attached to the promoted run.
	identity := testIdentity()
	identity.WALIdentity = "pending-wal"
	active := &Ledger{SourceClusterID: identity.SourceClusterID, RequireRuntimeBinding: true, RequireMutationJournal: true}
	runtime := enrolledRuntime(identity, "pending-container")
	initial := identity
	initial.WALIdentity = ""
	mustJournal(t, active.RegisterEnrolledWithRole(initial, runtime, StartupRoleLearnerPendingRemove))
	mustJournal(t, active.Bind(identity))
	if err := active.PromoteLearner(identity, "promotion", journalTestDigest); err != nil {
		t.Fatalf("promote before closure: %v", err)
	}
	if err := active.CloseNodes("pending-stop", journalTestDigest, []string{identity.NodeID}); err != nil {
		t.Fatalf("close promoted active voter: %v", err)
	}
	run := active.Runs[identity.RunID]
	stalePending := TerminalReference{EvidenceVersion: 3, Request: *run.Stop, RuntimeHash: RuntimeBindingDigest(runtime),
		ProofName: "pending-proof", ProofUID: "proof-uid", ProofDigest: journalTestDigest,
		RuntimeReceiptDigest: journalTestDigest, StorageScopeDigest: journalTestDigest}
	if err := active.RecordIndependentTerminal(stalePending); err == nil {
		t.Fatal("stale V3 pending reference accepted after promotion; active V2 obligations must remain")
	}
	if RunRole(run) != StartupRoleVoterActive || run.Promotion == nil {
		// Producer enrollment is an operator obligation; the pure ledger test
		// checks that V3 cannot substitute for active-voter V2 evidence.
		t.Fatalf("promotion winner was not retained as active: %+v", run)
	}

	// Closure wins first: neither node-scoped nor generation-scoped closure
	// admits promotion afterward.
	closed, pending := pendingTerminalFixture(t)
	if err := closed.PromoteLearner(pending.Request.Identity, "promotion", journalTestDigest); err == nil {
		t.Fatal("promotion succeeded after node closure")
	}
	if err := closed.Close("generation-stop", strings.Repeat("c", 64)); err != nil {
		t.Fatalf("generation closure after node closure: %v", err)
	}
	if err := closed.PromoteLearner(pending.Request.Identity, "promotion", journalTestDigest); err == nil {
		t.Fatal("promotion succeeded after generation closure")
	}
}
