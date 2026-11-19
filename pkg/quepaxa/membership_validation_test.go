package quepaxa

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"testing"
	"time"
)

func TestMembershipHistoryAcceptsTerminalRecorderOutsideFreezeQuorum(t *testing.T) {
	initial := Cluster{ConfigID: 1, Members: []Member{{ID: "a"}, {ID: "b"}, {ID: "c"}}}
	target := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}
	freezeValue, err := encodeReconfiguration(reconfigurationValue{Freeze: 1, TerminalSlot: 17, Target: target})
	if err != nil {
		t.Fatal(err)
	}
	freezeProposal := newProposal(highestPriority, "a", freezeValue)
	freezeHash := freezeProposal.Hash
	freezeDecision := Decision{ConfigID: 1, Slot: 1, Step: 4, Proposal: freezeProposal, Summaries: []Summary{
		{RecorderID: "a", Step: 4, ReconfigurationID: freezeHash, FirstCurrent: cloneProposal(&freezeProposal)},
		{RecorderID: "b", Step: 4, ReconfigurationID: freezeHash, FirstCurrent: cloneProposal(&freezeProposal)},
	}}
	freezeCertificate, err := encodeCertificate(1, freezeDecision)
	if err != nil {
		t.Fatal(err)
	}
	terminalValue, err := encodeReconfiguration(reconfigurationValue{Terminal: true, Freeze: 1, TerminalSlot: 17, Target: target})
	if err != nil {
		t.Fatal(err)
	}
	terminalProposal := newProposal(highestPriority, "b", terminalValue)
	terminalDecision := Decision{ConfigID: 1, Slot: 17, Step: 4, Proposal: terminalProposal, Summaries: []Summary{
		{RecorderID: "a", Step: 4, ReconfigurationID: freezeHash, FirstCurrent: cloneProposal(&terminalProposal)},
		{RecorderID: "c", Step: 4, ReconfigurationID: freezeHash, FirstCurrent: cloneProposal(&terminalProposal)},
	}}
	terminalCertificate, err := encodeCertificate(1, terminalDecision)
	if err != nil {
		t.Fatal(err)
	}
	record := MembershipRecord{Genesis: initial, Transitions: []ConfigTransition{{
		Freeze:   DecidedValue{Slot: 1, Hash: sha256.Sum256(freezeValue), Value: freezeValue, Certificate: freezeCertificate},
		Terminal: DecidedValue{Slot: 17, Hash: sha256.Sum256(terminalValue), Value: terminalValue, Certificate: terminalCertificate},
	}}}
	verified, err := validateMembershipHistory(initial, record)
	if err != nil {
		t.Fatalf("terminal recorder outside the local freeze quorum was rejected: %v", err)
	}
	if verified.clusterForSlot(18).ConfigID != 2 {
		t.Fatal("validated transition did not activate its target")
	}
}

func TestMembershipHistoryValidation(t *testing.T) {
	cores, _ := reconfigCluster(t)
	core := cores["a"]
	initial := core.CurrentCluster()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := core.BeginReconfiguration(ctx, Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}); err != nil {
		t.Fatal(err)
	}
	if err := core.FinishReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	// Extract the actual certified decisions independently of the export API.
	freeze, _ := core.decision(1)
	terminal, _ := core.decision(17)
	record := MembershipRecord{Genesis: initial, Transitions: []ConfigTransition{{Freeze: freeze, Terminal: terminal}}}
	verifier, err := validateMembershipHistory(initial, record)
	if err != nil {
		t.Fatal(err)
	}
	if verifier.clusterForSlot(18).ConfigID != 2 || len(verifier.clusterForSlot(1).Members) != 3 {
		t.Fatal("lost transition boundary")
	}
	if _, retired := verifier.retiredIDs["c"]; !retired {
		t.Fatal("lost retired identity")
	}
	encoded, _ := json.Marshal(record)
	for _, test := range []struct {
		name   string
		mutate func(*MembershipRecord)
	}{
		{"bootstrap", func(r *MembershipRecord) { r.Genesis.Members[0].PublicKey = PublicKey{1} }},
		{"value", func(r *MembershipRecord) { r.Transitions[0].Terminal.Value[0] ^= 1 }},
		{"slot", func(r *MembershipRecord) { r.Transitions[0].Terminal.Slot++ }},
		{"missing-quorum", func(r *MembershipRecord) {
			var c certificate
			_ = json.Unmarshal(r.Transitions[0].Terminal.Certificate, &c)
			c.Summaries = c.Summaries[:1]
			r.Transitions[0].Terminal.Certificate, _ = json.Marshal(c)
		}},
		{"wrong-freeze", func(r *MembershipRecord) {
			var c certificate
			_ = json.Unmarshal(r.Transitions[0].Terminal.Certificate, &c)
			wrong := ValueHash{9}
			c.Summaries[0].ReconfigurationID = &wrong
			r.Transitions[0].Terminal.Certificate, _ = json.Marshal(c)
		}},
		{"reordered", func(r *MembershipRecord) { r.Transitions = append(r.Transitions, r.Transitions[0]) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			var copy MembershipRecord
			if err := json.Unmarshal(encoded, &copy); err != nil {
				t.Fatal(err)
			}
			test.mutate(&copy)
			if _, err := validateMembershipHistory(initial, copy); err == nil {
				t.Fatal("accepted invalid history")
			}
		})
	}
}
