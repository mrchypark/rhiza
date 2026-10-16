package fencing

import "testing"

func testIdentity() Identity {
	return Identity{SourceClusterID: "source", NodeID: "n1", PodUID: "pod-uid", RunID: "run-1"}
}

func TestLedgerRequiresSeparateDrainAndExit(t *testing.T) {
	l := Ledger{SourceClusterID: "source"}
	id := testIdentity()
	if err := l.Register(id); err != nil {
		t.Fatal(err)
	}
	id.WALIdentity = "wal"
	if err := l.Bind(id); err != nil {
		t.Fatal(err)
	}
	if err := l.Close("op", "hash"); err != nil {
		t.Fatal(err)
	}
	r := *l.Runs[id.RunID].Stop
	if err := l.RecordExit(r); err != ErrPending {
		t.Fatalf("exit before drain: %v", err)
	}
	if err := l.RecordDrain(DrainReceipt{Request: r, WritersJoined: true}); err != nil {
		t.Fatal(err)
	}
	if l.Complete("op", "hash", []string{id.PodUID}) {
		t.Fatal("drain alone proved process exit")
	}
	if err := l.RecordExit(r); err != nil {
		t.Fatal(err)
	}
	if !l.Complete("op", "hash", []string{id.PodUID}) {
		t.Fatal("complete evidence rejected")
	}
	if l.Complete("op", "hash", []string{"unregistered-pod"}) {
		t.Fatal("unregistered Pod accepted")
	}
	if l.Complete("op", "different", []string{id.PodUID}) {
		t.Fatal("different request accepted")
	}
}

func TestLedgerUnknownHistoryCannotBecomeClean(t *testing.T) {
	l := Ledger{SourceClusterID: "source"}
	id := testIdentity()
	if err := l.Register(id); err != nil {
		t.Fatal(err)
	}
	next := id
	next.RunID = "restart"
	if err := l.Register(next); err == nil {
		t.Fatal("restart erased unresolved run")
	}
	if err := l.Close("op", "hash"); err != nil {
		t.Fatal(err)
	}
	r := *l.Runs[id.RunID].Stop
	drain := DrainReceipt{Request: r, WritersJoined: true, UnknownMutations: 1}
	if err := l.RecordDrain(drain); err != nil {
		t.Fatal(err)
	}
	drain.UnknownMutations = 0
	if err := l.RecordDrain(drain); err == nil {
		t.Fatal("unknown outcome overwritten")
	}
	if err := l.RecordExit(r); err != ErrPending {
		t.Fatalf("uncertain storage accepted: %v", err)
	}
	if err := l.Register(next); err != ErrClosed {
		t.Fatalf("closed generation restarted: %v", err)
	}
}

func TestLedgerVoterFenceThenGenerationPreservesReceipts(t *testing.T) {
	for _, finishFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending", true: "complete"}[finishFirst], func(t *testing.T) {
			l := Ledger{SourceClusterID: "source"}
			first := testIdentity()
			second := Identity{SourceClusterID: "source", NodeID: "n2", PodUID: "pod2", RunID: "run2"}
			for _, id := range []Identity{first, second} {
				if err := l.Register(id); err != nil {
					t.Fatal(err)
				}
				id.WALIdentity = "wal-" + id.NodeID
				if err := l.Bind(id); err != nil {
					t.Fatal(err)
				}
			}
			if err := l.CloseNodes("voter-op", "voter-hash", []string{"n1"}); err != nil {
				t.Fatal(err)
			}
			original := *l.Runs[first.RunID].Stop
			finish := func(id string) {
				r := *l.Runs[id].Stop
				if err := l.RecordDrain(DrainReceipt{Request: r, WritersJoined: true}); err != nil {
					t.Fatal(err)
				}
				if err := l.RecordExit(r); err != nil {
					t.Fatal(err)
				}
			}
			if finishFirst {
				finish(first.RunID)
			}
			if err := l.Close("generation-op", "generation-hash"); err != nil {
				t.Fatal(err)
			}
			if *l.Runs[first.RunID].Stop != original {
				t.Fatal("prior immutable request overwritten")
			}
			finish(second.RunID)
			if !finishFirst && l.Complete("generation-op", "generation-hash", nil) {
				t.Fatal("pending prior stop became complete")
			}
			if !finishFirst {
				finish(first.RunID)
			}
			if !l.Complete("generation-op", "generation-hash", []string{first.PodUID, second.PodUID}) {
				t.Fatal("prior clean stop not reused")
			}
		})
	}
}

func TestLedgerBindingAndOperationImmutable(t *testing.T) {
	l := Ledger{SourceClusterID: "source"}
	id := testIdentity()
	if err := l.Register(id); err != nil {
		t.Fatal(err)
	}
	id.WALIdentity = "wal"
	if err := l.Bind(id); err != nil {
		t.Fatal(err)
	}
	wrong := id
	wrong.WALIdentity = "other"
	if err := l.Bind(wrong); err == nil {
		t.Fatal("WAL changed")
	}
	if err := l.Close("op", "hash"); err != nil {
		t.Fatal(err)
	}
	if err := l.Close("op", "changed"); err == nil {
		t.Fatal("request changed")
	}
	if err := l.Close("other", "hash"); err != ErrClosed {
		t.Fatalf("operation changed: %v", err)
	}
	wrong.PodUID = "replacement"
	if err := l.RecordDrain(DrainReceipt{Request: StopRequest{OperationID: "op", RequestHash: "hash", Identity: wrong}, WritersJoined: true}); err == nil {
		t.Fatal("replacement drain accepted")
	}
}
