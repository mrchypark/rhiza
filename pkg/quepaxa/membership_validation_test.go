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
		{"bootstrap", func(r *MembershipRecord) { r.Genesis.Members[0].Token = "other" }},
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
