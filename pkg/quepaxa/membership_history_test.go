package quepaxa

import (
	"context"
	"testing"
	"time"
)

func TestMembershipHistoryExport(t *testing.T) {
	cores, _ := reconfigCluster(t)
	core := cores["a"]
	empty, err := core.MembershipHistory()
	if err != nil || len(empty.Transitions) != 0 || len(empty.Genesis.Members) != 3 {
		t.Fatalf("initial history: %+v %v", empty, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := core.BeginReconfiguration(ctx, Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}); err != nil {
		t.Fatal(err)
	}
	pending, err := core.MembershipHistory()
	if err != nil || len(pending.Transitions) != 0 {
		t.Fatalf("unfinished freeze reported transition: %+v %v", pending, err)
	}
	if err := core.FinishReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	history, err := core.MembershipHistory()
	if err != nil {
		t.Fatal(err)
	}
	if len(history.Transitions) != 1 || history.Transitions[0].Freeze.Slot != 1 || history.Transitions[0].Terminal.Slot != 17 {
		t.Fatalf("incorrect transition: %+v", history)
	}
	history.Genesis.Members[0].ID = "intruder"
	history.Transitions[0].Freeze.Value[0] ^= 1
	history.Transitions[0].Freeze.Certificate[0] ^= 1
	history.Transitions[0].Terminal.Value[0] ^= 1
	history.Transitions[0].Terminal.Certificate[0] ^= 1
	fresh, err := core.MembershipHistory()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := validateMembershipHistory(core.ClusterForSlot(1), fresh); err != nil {
		t.Fatalf("caller corrupted core: %v", err)
	}
	core.mu.Lock()
	core.durable[17] = false
	core.mu.Unlock()
	if _, err := core.MembershipHistory(); err == nil {
		t.Fatal("exported undurable terminal")
	}
	core.mu.Lock()
	core.durable[17] = true
	delete(core.decided, 1)
	core.mu.Unlock()
	if _, err := core.MembershipHistory(); err == nil {
		t.Fatal("exported unavailable freeze")
	}
}
