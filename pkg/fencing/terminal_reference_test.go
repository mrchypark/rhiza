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
