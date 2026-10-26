package fencing

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestMutationIntentRetryPreservesCompleteCallerDescriptor(t *testing.T) {
	original := journalUpload("original")
	original.Condition = MutationCondition{Mode: MutationCreateOnly}
	mutations := map[string]func(*MutationIntent){
		"condition": func(i *MutationIntent) { i.Condition = MutationCondition{Mode: MutationMatchGeneration, Generation: 7} },
		"scope":     func(i *MutationIntent) { i.StoreScopeHash = strings.Repeat("b", 64) },
		"key":       func(i *MutationIntent) { i.Key = "another" },
		"stage":     func(i *MutationIntent) { i.PayloadRef = "another-stage" },
		"digest":    func(i *MutationIntent) { i.PayloadDigest = strings.Repeat("b", 64) },
		"size":      func(i *MutationIntent) { i.PayloadSize++ },
		"owner":     func(i *MutationIntent) { i.OwnerStep.Owner = "another" },
		"workflow":  func(i *MutationIntent) { i.OwnerStep.Workflow = "another" },
		"step":      func(i *MutationIntent) { i.OwnerStep.Step = "another" },
		"attempt":   func(i *MutationIntent) { i.OwnerStep.Attempt++ },
		"version":   func(i *MutationIntent) { i.ContractVersion++ },
	}
	for name, change := range mutations {
		t.Run(name, func(t *testing.T) {
			p := newJournalProducer(t, name)
			mustJournal(t, p.Admit(1, original))
			p = reloadProducer(t, p)
			mustJournal(t, p.Admit(1, original))
			changed := original
			change(&changed)
			rejectJournalUnchanged(t, p, func() error { return p.Admit(1, changed) })
		})
	}
}

func TestMutationIntentRejectsIncompleteCallerSelection(t *testing.T) {
	invalid := map[string]func(*MutationIntent){
		"legacy version":            func(i *MutationIntent) { i.ContractVersion = 0 },
		"scope":                     func(i *MutationIntent) { i.StoreScopeHash = "" },
		"owner":                     func(i *MutationIntent) { i.OwnerStep.Owner = "" },
		"oversize owner":            func(i *MutationIntent) { i.OwnerStep.Owner = strings.Repeat("a", 129) },
		"workflow":                  func(i *MutationIntent) { i.OwnerStep.Workflow = "" },
		"step":                      func(i *MutationIntent) { i.OwnerStep.Step = "" },
		"attempt":                   func(i *MutationIntent) { i.OwnerStep.Attempt = 0 },
		"condition":                 func(i *MutationIntent) { i.Condition.Mode = "" },
		"create with generation":    func(i *MutationIntent) { i.Condition = MutationCondition{Mode: MutationCreateOnly, Generation: 1} },
		"missing exact generation":  func(i *MutationIntent) { i.Condition.Mode = MutationMatchGeneration },
		"negative length":           func(i *MutationIntent) { i.PayloadSize = -1 },
		"unversioned delete":        func(i *MutationIntent) { i.Kind, i.PayloadRef, i.PayloadDigest, i.PayloadSize = "delete", "", "", 0 },
		"dot segment key alias":     func(i *MutationIntent) { i.Key = "nested/../target" },
		"prefix escape key alias":   func(i *MutationIntent) { i.Key = "../source/target" },
		"duplicate separator alias": func(i *MutationIntent) { i.Key = "nested//target" },
		"absolute object key":       func(i *MutationIntent) { i.Key = "/target" },
	}
	for name, change := range invalid {
		t.Run(name, func(t *testing.T) {
			p := newJournalProducer(t, "invalid")
			intent := journalUpload("op")
			change(&intent)
			rejectJournalUnchanged(t, p, func() error { return p.Admit(1, intent) })
		})
	}
}

func TestMutationPreparationCannotRefreshSelectedVersionOrScope(t *testing.T) {
	for _, kind := range []string{"upload", "delete"} {
		t.Run(kind, func(t *testing.T) {
			p := newJournalProducer(t, kind)
			intent := journalUpload("selected-g1")
			intent.Kind = kind
			intent.Condition = MutationCondition{Mode: MutationMatchGeneration, Generation: 11}
			if kind == "delete" {
				intent.PayloadRef, intent.PayloadDigest, intent.PayloadSize = "", "", 0
			}
			mustJournal(t, p.Admit(1, intent))
			// Selecting G1 must not authorize replacing it with the executor's G2.
			rejectJournalUnchanged(t, p, func() error { return p.Prepare(1, journalPreparation(12)) })
			wrongScope := journalPreparation(11)
			wrongScope.StoreScopeHash = strings.Repeat("b", 64)
			rejectJournalUnchanged(t, p, func() error { return p.Prepare(1, wrongScope) })
			mustJournal(t, p.Prepare(1, journalPreparation(11)))
			p = reloadProducer(t, p)
			rejectJournalUnchanged(t, p, func() error { return p.Prepare(1, journalPreparation(12)) })
		})
	}
	p := newJournalProducer(t, "create")
	intent := journalUpload("create-only")
	intent.Condition = MutationCondition{Mode: MutationCreateOnly}
	mustJournal(t, p.Admit(1, intent))
	rejectJournalUnchanged(t, p, func() error { return p.Prepare(1, journalPreparation(1)) })
	mustJournal(t, p.Prepare(1, journalPreparation(0)))
}

func TestMutationIntentJSONReloadDoesNotDropCallerFields(t *testing.T) {
	intent := journalUpload("op")
	data, err := json.Marshal(intent)
	mustJournal(t, err)
	var restored MutationIntent
	mustJournal(t, json.Unmarshal(data, &restored))
	if restored != intent {
		t.Fatal("caller descriptor changed after durable serialization")
	}
	// The unchanged pre-client descriptor is not a fully bound executable intent.
	legacy := MutationIntent{OperationID: "op", Kind: "upload", Key: "key", PayloadRef: "stage", PayloadDigest: journalTestDigest}
	if validateMutationIntent(legacy) == nil {
		t.Fatal("legacy incomplete caller descriptor was accepted")
	}
}
