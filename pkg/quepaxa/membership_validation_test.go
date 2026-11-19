package quepaxa

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

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

func TestMembershipHistoryAllowsDivergentFreezeAndTerminalQuorums(t *testing.T) {
	cores, transport := reconfigCluster(t)
	initial := cores["a"].CurrentCluster()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Freeze is certified by {a, b} while c is partitioned.
	transport.fail("c")
	target := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "c"}}}
	freezeSlot, err := cores["a"].BeginReconfiguration(ctx, target)
	if err != nil {
		t.Fatalf("begin reconfiguration: %v", err)
	}
	transport.recover("c")

	// Partition b before finishing removal of b, forcing terminal to certify via {a, c}.
	transport.fail("b")
	if err := cores["a"].FinishReconfiguration(ctx); err != nil {
		t.Fatalf("finish reconfiguration via {a, c}: %v", err)
	}
	freeze, _ := cores["a"].decision(freezeSlot)
	terminal, _ := cores["a"].decision(freezeSlot + 16)
	record := MembershipRecord{Genesis: initial, Transitions: []ConfigTransition{{Freeze: freeze, Terminal: terminal}}}
	verifier, err := validateMembershipHistory(initial, record)
	if err != nil {
		t.Fatalf("divergent freeze {a,b} / terminal {a,c} rejected: %v", err)
	}
	if got := verifier.clusterForSlot(terminal.Slot + 1).ConfigID; got != 2 {
		t.Fatalf("verified configID=%d want 2", got)
	}
}
