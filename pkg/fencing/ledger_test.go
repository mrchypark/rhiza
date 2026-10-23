package fencing

import (
	"strings"
	"testing"
)

func testIdentity() Identity {
	return Identity{SourceClusterID: "source", NodeID: "n1", PodUID: "pod-uid", RunID: "run-1"}
}

func TestStartupGrantRoleIsAuthorityOwnedAndPhysicalPodBound(t *testing.T) {
	request := testIdentity()
	request.NodeID = "physical-ordinal-0"
	request.PodName = request.NodeID
	grant := StartupGrant{Identity: request, Role: StartupRoleLearnerPendingRemove}
	if err := grant.Validate(request); err != nil {
		t.Fatal(err)
	}
	grant.Role = StartupRole("voter-active")
	grant.Identity.NodeID = "replacement-logical-id"
	if err := grant.Validate(request); err == nil {
		t.Fatal("active voter grant changed logical identity away from physical Pod")
	}
	grant.Role = StartupRoleLearnerPendingRemove
	grant.Identity.PodName = "another-pod"
	if err := grant.Validate(request); err == nil {
		t.Fatal("learner grant escaped authenticated physical Pod binding")
	}
}

func TestStartupGrantAllowsOnlyBoundPendingLogicalAssignment(t *testing.T) {
	request := testIdentity()
	request.PodName = "rhiza-2"
	grant := StartupGrant{Identity: request, Role: StartupRoleLearnerPendingRemove}
	grant.Identity.NodeID = "logical-incarnation-7f3a"
	grant.Identity.AssignmentID = strings.Repeat("a", 64)
	grant.Environment = map[string]string{"RHIZA_NODE_ID": grant.Identity.NodeID, "RHIZA_LEARNER": `{"id":"logical-incarnation-7f3a"}`, "RHIZA_PEER_TOKEN": "private-token", "RHIZA_PEER_TOKENS": ""}
	if err := grant.Validate(request); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*StartupGrant){
		"changed physical Pod":    func(g *StartupGrant) { g.Identity.PodName = "rhiza-3" },
		"missing assignment":      func(g *StartupGrant) { g.Identity.AssignmentID = "" },
		"unsupported environment": func(g *StartupGrant) { g.Environment["RHIZA_ADMIN_TOKEN"] = "secret" },
		"learner role omitted":    func(g *StartupGrant) { delete(g.Environment, "RHIZA_LEARNER") },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := grant
			candidate.Environment = make(map[string]string, len(grant.Environment))
			for key, value := range grant.Environment {
				candidate.Environment[key] = value
			}
			mutate(&candidate)
			if err := candidate.Validate(request); err == nil {
				t.Fatal("invalid logical assignment accepted")
			}
		})
	}
	active := grant
	active.Role = StartupRoleVoterActive
	active.Environment = map[string]string{
		"RHIZA_NODE_ID":         active.Identity.NodeID,
		"RHIZA_LEARNER":         "",
		"RHIZA_PEER_TOKEN":      "private-token",
		"RHIZA_PEER_TOKENS":     "",
		"RHIZA_CLUSTER_MEMBERS": "[{\"id\":\"existing-voter\"},{\"id\":\"logical-incarnation-7f3a\"}]",
	}
	if err := active.Validate(request); err != nil {
		t.Fatalf("authority-issued assigned identity could not restart as active after canonical completion: %v", err)
	}
	active.Environment["RHIZA_LEARNER"] = "{\"id\":\"logical-incarnation-7f3a\"}"
	if err := active.Validate(request); err == nil {
		t.Fatal("active assigned restart retained learner mode")
	}
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
