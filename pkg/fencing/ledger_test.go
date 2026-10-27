package fencing

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

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
	next.PodUID = "different-pod"
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

func TestPeerClosureReservationRejectsAbortRoundTargetSwitch(t *testing.T) {
	l := peerClosureLedger(t)
	first := peerClosureReservation("op-b", "hash-b", 3, 7, "B", "wal-B", []string{"A", "B", "C"}, "pod-B")
	if err := l.ReservePeerClosure(first); err != nil {
		t.Fatal(err)
	}
	if err := l.ReservePeerClosure(first); err != nil {
		t.Fatalf("exact reservation retry failed: %v", err)
	}
	second := peerClosureReservation("op-c", "hash-c", 3, 8, "C", "wal-C", []string{"A", "B", "C"}, "pod-C")
	if err := l.ReservePeerClosure(second); !errors.Is(err, ErrPending) {
		t.Fatalf("different target in the same membership generation = %v, want pending", err)
	}
	if l.PeerClosure == nil || l.PeerClosure.TargetNodeID != "B" {
		t.Fatalf("rejected request replaced the durable reservation: %+v", l.PeerClosure)
	}
}

func TestPeerClosureReservationAllowsNextTargetOnlyAfterRemovalAndTerminalProof(t *testing.T) {
	l := peerClosureLedger(t)
	first := peerClosureReservation("op-b", "hash-b", 3, 7, "B", "wal-B", []string{"A", "B", "C"}, "pod-B")
	if err := l.ReservePeerClosure(first); err != nil {
		t.Fatal(err)
	}
	if err := l.ReservePeerClosure(peerClosureReservation("op-c", "hash-c", 4, 8, "C", "wal-C", []string{"A", "C"}, "pod-C")); !errors.Is(err, ErrPending) {
		t.Fatalf("target switch before termination proof = %v, want pending", err)
	}
	request := *l.Runs["run-B"].Stop
	if err := l.RecordDrain(DrainReceipt{Request: request, WritersJoined: true}); err != nil {
		t.Fatal(err)
	}
	if err := l.RecordExit(request); err != nil {
		t.Fatal(err)
	}
	second := peerClosureReservation("op-c", "hash-c", 4, 8, "C", "wal-C", []string{"A", "C"}, "pod-C")
	if err := l.ReservePeerClosure(second); err != nil {
		t.Fatalf("target switch after prior target left membership and exited = %v", err)
	}
	if l.ClosedNodes["B"].OperationID != first.OperationID || l.PeerClosure.TargetNodeID != "C" {
		t.Fatalf("reservation replacement discarded prior closure: peer=%+v closed=%+v", l.PeerClosure, l.ClosedNodes)
	}
}

func TestPeerClosureReservationConflictsWithOtherVoterFence(t *testing.T) {
	l := peerClosureLedger(t)
	if err := l.CloseNodes("operator-op", "operator-hash", []string{"B"}); err != nil {
		t.Fatal(err)
	}
	request := peerClosureReservation("peer-op", "peer-hash", 3, 7, "C", "wal-C", []string{"A", "B", "C"}, "pod-C")
	if err := l.ReservePeerClosure(request); !errors.Is(err, ErrPending) {
		t.Fatalf("peer closure ignored an existing in-membership operator fence = %v, want pending", err)
	}

	// The reverse race is equally serialized: an operator CR may exist before
	// its reconciler reaches the ledger CAS, but it cannot execute after this
	// reservation wins.
	reverse := peerClosureLedger(t)
	request = peerClosureReservation("peer-op", "peer-hash", 3, 7, "B", "wal-B", []string{"A", "B", "C"}, "pod-B")
	if err := reverse.ReservePeerClosure(request); err != nil {
		t.Fatal(err)
	}
	if err := reverse.CloseNodes("operator-op", "operator-hash", []string{"B"}); !errors.Is(err, ErrPending) {
		t.Fatalf("delayed operator voter fence passed a peer reservation = %v, want pending", err)
	}
	if err := reverse.Close("generation-op", "generation-hash"); err != nil {
		t.Fatalf("generation-wide emergency close blocked by voter reservation: %v", err)
	}
}

func TestPeerClosureReservationRetainsUnboundHistoryAlongsideRequestedWAL(t *testing.T) {
	l := Ledger{SourceClusterID: "source", Runs: map[string]Run{
		"run-caller": {Identity: Identity{SourceClusterID: "source", NodeID: "A", PodUID: "pod-A", RunID: "run-caller", WALIdentity: "wal-A"}},
		"run-empty":  {Identity: Identity{SourceClusterID: "source", NodeID: "B", PodUID: "pod-B", RunID: "run-empty"}},
		"run-bound":  {Identity: Identity{SourceClusterID: "source", NodeID: "B", PodUID: "pod-B", RunID: "run-bound", WALIdentity: "wal-B"}},
	}}
	request := PeerClosureReservation{
		OperationID: "op-b", RequestHash: "hash-b", ConfigID: 3, AbortSlot: 7,
		TargetNodeID: "B", TargetWAL: "wal-B", Voters: []string{"A", "B", "C"},
		Targets: []PeerClosureTarget{
			{NodeID: "B", Pod: "B", PodUID: "pod-B"},
			{NodeID: "B", Pod: "B", PodUID: "pod-B", WALIdentity: "wal-B"},
		},
	}
	if err := l.ReservePeerClosure(request); err != nil {
		t.Fatalf("exact requested WAL with retained unbound history rejected: %v", err)
	}
	if l.PeerClosure.Targets[0].WALIdentity != "" && l.PeerClosure.Targets[1].WALIdentity != "" {
		t.Fatal("unbound history was rewritten to a bound WAL")
	}
}

func TestAuthorityClientAcceptsRetainedUnboundPeerTarget(t *testing.T) {
	caller := Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "caller-uid", RunID: "caller-run", WALIdentity: "wal-caller"}
	closure := PeerClosureRequest{ExpectedConfigID: 3, ExpectedAbortSlot: uint64Pointer(7), TargetNodeID: "rhiza-1", TargetWALIdentity: "wal-target"}
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("test-token"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name      string
		targets   []PeerClosureTarget
		wantError bool
	}{
		{name: "retained unbound run plus requested WAL", targets: []PeerClosureTarget{{NodeID: "rhiza-1", Pod: "rhiza-1", PodUID: "target-uid"}, {NodeID: "rhiza-1", Pod: "rhiza-1", PodUID: "target-uid", WALIdentity: "wal-target"}}},
		{name: "missing requested WAL", targets: []PeerClosureTarget{{NodeID: "rhiza-1", Pod: "rhiza-1", PodUID: "target-uid"}}, wantError: true},
		{name: "conflicting nonempty WAL", targets: []PeerClosureTarget{{NodeID: "rhiza-1", Pod: "rhiza-1", PodUID: "target-uid", WALIdentity: "other-wal"}, {NodeID: "rhiza-1", Pod: "rhiza-1", PodUID: "target-uid", WALIdentity: "wal-target"}}, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			receipt := PeerClosureReceipt{
				OperationID: "peer-operation", SourceClusterID: caller.SourceClusterID, BindingUID: "root-uid", StatefulSetUID: "workload-uid",
				RequestHash: "request-hash", ExpectedConfigID: closure.ExpectedConfigID, ExpectedAbortSlot: uint64Pointer(*closure.ExpectedAbortSlot),
				TargetNodeID: closure.TargetNodeID, TargetWALIdentity: closure.TargetWALIdentity, Targets: test.targets,
				ProofID: "proof-id", ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true,
			}
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/peer-closure/request" {
					http.NotFound(w, r)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(receipt)
			}))
			defer server.Close()
			client := &AuthorityClient{URL: server.URL + "/v1/supervisor", TokenFile: token, Client: server.Client()}
			_, err := client.RequestPeerClosure(context.Background(), caller, closure)
			if (err != nil) != test.wantError {
				t.Fatalf("RequestPeerClosure err=%v, wantError=%t", err, test.wantError)
			}
		})
	}
}

func peerClosureLedger(t *testing.T) Ledger {
	t.Helper()
	l := Ledger{SourceClusterID: "source", Runs: map[string]Run{}}
	for _, node := range []string{"A", "B", "C"} {
		id := Identity{SourceClusterID: "source", NodeID: node, PodUID: "pod-" + node, RunID: "run-" + node}
		if err := l.Register(id); err != nil {
			t.Fatal(err)
		}
		id.WALIdentity = "wal-" + node
		if err := l.Bind(id); err != nil {
			t.Fatal(err)
		}
	}
	return l
}

func peerClosureReservation(operationID, requestHash string, configID, abortSlot uint64, targetNode, targetWAL string, voters []string, podUID string) PeerClosureReservation {
	return PeerClosureReservation{
		OperationID: operationID, RequestHash: requestHash, ConfigID: configID, AbortSlot: abortSlot,
		TargetNodeID: targetNode, TargetWAL: targetWAL, Voters: voters,
		Targets: []PeerClosureTarget{{NodeID: targetNode, Pod: targetNode, PodUID: podUID, WALIdentity: targetWAL,
			NodeName: "worker-" + targetNode, NodeUID: "node-uid-" + targetNode, ContainerID: "containerd://" + podUID, ImageID: "sha256:" + podUID}},
	}
}

func TestLedgerIntactWALRestartDoesNotEraseOldUncertainty(t *testing.T) {
	l := Ledger{SourceClusterID: "source"}
	first := testIdentity()
	if err := l.Register(first); err != nil {
		t.Fatal(err)
	}
	first.WALIdentity = "wal"
	if err := l.Bind(first); err != nil {
		t.Fatal(err)
	}
	second := first
	second.RunID = "restart"
	second.WALIdentity = ""
	if err := l.Register(second); err != nil {
		t.Fatalf("same Pod intact WAL restart registration: %v", err)
	}
	wrong := second
	wrong.WALIdentity = "new-wal"
	if err := l.Bind(wrong); err == nil {
		t.Fatal("replacement WAL accepted")
	}
	second.WALIdentity = "wal"
	if err := l.Bind(second); err != nil {
		t.Fatalf("intact WAL bind: %v", err)
	}
	if len(l.Runs) != 2 || l.Runs[first.RunID].Exited || l.Runs[first.RunID].Drain != nil {
		t.Fatal("old uncertainty was erased")
	}
	if err := l.Close("op", "hash"); err != nil {
		t.Fatal(err)
	}
	request := *l.Runs[second.RunID].Stop
	if err := l.RecordDrain(DrainReceipt{Request: request, WritersJoined: true}); err != nil {
		t.Fatal(err)
	}
	if err := l.RecordExit(request); err != nil {
		t.Fatal(err)
	}
	if l.Complete("op", "hash", []string{first.PodUID}) {
		t.Fatal("new run proved an old uncertain run")
	}
	if l.Runs[first.RunID].Drain != nil || l.Runs[first.RunID].Exited {
		t.Fatal("receipt flowed backward")
	}
}

func TestLedgerClosureBeforeWALBindNeedsSeparateNeverBoundExit(t *testing.T) {
	l := Ledger{SourceClusterID: "source"}
	id := testIdentity()
	if err := l.Register(id); err != nil {
		t.Fatal(err)
	}
	if err := l.Close("op", "hash"); err != nil {
		t.Fatal(err)
	}
	want := *l.Runs[id.RunID].Stop
	attempt := id
	attempt.WALIdentity = "wal"
	if err := l.Bind(attempt); err != ErrClosed {
		t.Fatalf("bind after close: %v", err)
	}
	if l.Complete("op", "hash", []string{id.PodUID}) {
		t.Fatal("reservation alone became a proof")
	}
	wrong := want
	wrong.Identity.RunID = "unverified"
	if err := l.RecordNeverBoundExit(wrong); err == nil {
		t.Fatal("unverified exit accepted")
	}
	if err := l.RecordNeverBoundExit(want); err != nil {
		t.Fatal(err)
	}
	if !l.Complete("op", "hash", []string{id.PodUID}) {
		t.Fatal("verified never-bound termination rejected")
	}
	run := l.Runs[id.RunID]
	if run.Drain != nil || run.Identity.WALIdentity != "" || run.NeverBoundExit == nil {
		t.Fatal("fabricated WAL/drain instead of distinct exit evidence")
	}
}

func TestLedgerSuccessfulOrLostWALBindCannotUseNeverBoundExit(t *testing.T) {
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
	request := *l.Runs[id.RunID].Stop
	if err := l.RecordNeverBoundExit(request); err == nil {
		t.Fatal("bound run used no-write exception")
	}
	lostResponse := request
	lostResponse.Identity.WALIdentity = ""
	if err := l.RecordNeverBoundExit(lostResponse); err == nil {
		t.Fatal("lost successful bind response became never-bound evidence")
	}
	if l.Complete("op", "hash", []string{id.PodUID}) {
		t.Fatal("bound run completed without normal drain")
	}
	if err := l.RecordDrain(DrainReceipt{Request: request, WritersJoined: true}); err != nil {
		t.Fatal(err)
	}
	if err := l.RecordExit(request); err != nil {
		t.Fatal(err)
	}
	if !l.Complete("op", "hash", []string{id.PodUID}) {
		t.Fatal("normal bound drain/exit rejected")
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
