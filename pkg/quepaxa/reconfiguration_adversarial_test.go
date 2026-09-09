package quepaxa

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

// TestReconfigurationSparseFreezeCertificates keeps the normal sparse
// pipeline intact: a later certified freeze is buffered until the first
// contiguous freeze arrives, then becomes a drain no-op.
func TestReconfigurationSparseFreezeCertificates(t *testing.T) {
	cores, transport := reconfigCluster(t)
	transport.dropDecision["c"] = true
	ctx := context.Background()

	firstDecision, err := cores["a"].runSlot(ctx, 1, []byte("before-freeze"), true)
	if err != nil {
		t.Fatal(err)
	}
	target := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}
	freezeValue, err := encodeReconfiguration(reconfigurationValue{
		Freeze: 2, TerminalSlot: 18, Target: target,
		PrefixHash: AdvancePrefixHash([32]byte{}, 1, firstDecision.Proposal.Hash),
	})
	if err != nil {
		t.Fatal(err)
	}
	lateValue, err := encodeReconfiguration(reconfigurationValue{
		Freeze: 3, TerminalSlot: 19, Target: target,
		PrefixHash: [32]byte{3},
	})
	if err != nil {
		t.Fatal(err)
	}
	freezeDecision, err := cores["a"].runSlot(ctx, 2, freezeValue, true)
	if err != nil {
		t.Fatal(err)
	}
	late, err := cores["a"].runSlot(ctx, 3, lateValue, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := cores["a"].AcceptDecision(firstDecision); err != nil {
		t.Fatal(err)
	}
	if err := cores["a"].AcceptDecision(freezeDecision); err != nil {
		t.Fatal(err)
	}
	if err := cores["a"].AcceptDecision(late); err != nil {
		t.Fatal(err)
	}

	first, ok := cores["a"].decision(1)
	if !ok {
		t.Fatal("missing slot 1 decision")
	}
	freezeValueWithCertificate, ok := cores["a"].decision(2)
	if !ok {
		t.Fatal("missing freeze decision")
	}
	lateValueWithCertificate, ok := cores["a"].decision(3)
	if !ok {
		t.Fatal("missing later freeze decision")
	}

	learner := cores["c"]
	if err := learner.AcceptCertifiedValue(lateValueWithCertificate); err != nil {
		t.Fatalf("accept sparse later freeze: %v", err)
	}
	if learner.Tip() != 0 || learner.reconfiguration != nil {
		t.Fatalf("later freeze advanced before its prefix: tip=%d state=%+v", learner.Tip(), learner.reconfiguration)
	}
	if err := learner.AcceptCertifiedValue(first); err != nil {
		t.Fatalf("fill pre-freeze gap: %v", err)
	}
	if err := learner.AcceptCertifiedValue(freezeValueWithCertificate); err != nil {
		t.Fatalf("accept first contiguous freeze: %v", err)
	}
	if learner.reconfiguration == nil || learner.reconfiguration.freeze != 2 {
		t.Fatalf("selected freeze=%+v, want slot 2", learner.reconfiguration)
	}
	if got, ok := learner.decision(3); !ok || !bytes.Equal(got.Value, lateValue) {
		t.Fatalf("later freeze bytes were not retained: ok=%t value=%x", ok, got.Value)
	}
}

func TestReconfigurationTerminalUsesFreezeQuorumNotTargetIntersection(t *testing.T) {
	cores, transport := reconfigCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// c has an old ordinary ISR at the future terminal.  It cannot be allowed
	// to influence terminal recovery merely because the target retains c.
	cores["c"].reconfigEnabled = false
	stale := newProposal(highestPriority, "c", []byte("old-terminal-value"))
	if _, err := cores["c"].Record(ctx, RecordRequest{Slot: 17, Step: 4, Proposal: stale}); err != nil {
		t.Fatal(err)
	}
	transport.fail("c")
	target := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "c"}}}
	if freeze, err := cores["a"].BeginReconfiguration(ctx, target); err != nil || freeze != 1 {
		t.Fatalf("freeze=%d err=%v", freeze, err)
	}
	cores["c"].reconfigEnabled = true // Upgraded, but the old ISR is retained.
	transport.recover("c")
	if err := cores["a"].FinishReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	terminal, ok := cores["a"].decision(17)
	if !ok {
		t.Fatal("missing terminal decision")
	}
	decision, err := cores["a"].certifiedDecision(terminal)
	if err != nil {
		t.Fatal(err)
	}
	if len(decision.Summaries) != 2 || decision.Summaries[0].RecorderID != "a" || decision.Summaries[1].RecorderID != "b" {
		t.Fatalf("terminal quorum=%v want freeze quorum [a b]", summarySources(decision.Summaries))
	}
}

func TestReconfigurationFrozenTerminalRejectsStagedHashOnlyValue(t *testing.T) {
	cores, _ := reconfigCluster(t)
	ctx := context.Background()
	target := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}
	if _, err := cores["a"].BeginReconfiguration(ctx, target); err != nil {
		t.Fatal(err)
	}
	core := cores["a"]
	state := core.reconfiguration
	if state == nil {
		t.Fatal("missing frozen state")
	}
	if err := core.RecoverThrough(ctx, state.terminal-1); err != nil {
		t.Fatal(err)
	}

	reject := func(t *testing.T, value []byte) {
		t.Helper()
		hash := newProposal(highestPriority, "x", value).Hash
		if err := core.StageValue(hash, value); err != nil {
			t.Fatal(err)
		}
		_, err := core.Record(ctx, RecordRequest{
			Slot: state.terminal, Step: 4, ConfigID: 1, ReconfigurationID: state.id,
			Proposal: Proposal{Priority: highestPriority, ProposerID: "a", Hash: hash},
		})
		if err == nil {
			t.Fatal("accepted staged value at frozen terminal")
		}
		core.mu.RLock()
		_, recorded := core.recorders[state.terminal]
		core.mu.RUnlock()
		if recorded {
			t.Fatal("rejected staged value mutated terminal ISR")
		}
	}
	t.Run("ordinary", func(t *testing.T) { reject(t, []byte("staged ordinary value")) })
	t.Run("wrong terminal", func(t *testing.T) {
		wrong, err := encodeReconfiguration(reconfigurationValue{
			Terminal: true, Freeze: state.freeze, TerminalSlot: state.terminal,
			Target: Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "c"}}},
		})
		if err != nil {
			t.Fatal(err)
		}
		reject(t, wrong)
	})
	t.Run("wrong prefix", func(t *testing.T) {
		wrong, err := encodeReconfiguration(reconfigurationValue{
			Terminal: true, Freeze: state.freeze, TerminalSlot: state.terminal,
			Target: state.target, PrefixHash: [32]byte{0xff},
		})
		if err != nil {
			t.Fatal(err)
		}
		reject(t, wrong)
	})
}

func TestReconfigurationRestartReplaysCertifiedMembershipHistory(t *testing.T) {
	members := []Member{{ID: "a"}, {ID: "b"}, {ID: "c"}}
	cluster := Cluster{ConfigID: 1, Members: members}
	transport := &clusterTransport{cores: map[NodeID]*Core{}, down: map[NodeID]bool{}, dropDecision: map[NodeID]bool{}}
	dirs := map[NodeID]string{}
	wals := map[NodeID]*qlog.WAL{}
	for _, member := range members {
		dirs[member.ID] = filepath.Join(t.TempDir(), string(member.ID))
		wal, err := qlog.Open(dirs[member.ID])
		if err != nil {
			t.Fatal(err)
		}
		wals[member.ID] = wal
		core, err := New(Config{NodeID: member.ID, Cluster: cluster, WAL: wal, Transport: transport, EnableReconfiguration: true})
		if err != nil {
			t.Fatal(err)
		}
		transport.cores[member.ID] = core
	}
	t.Cleanup(func() {
		for _, wal := range wals {
			if wal != nil {
				_ = wal.Close()
			}
		}
	})

	target := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}
	if _, err := transport.cores["a"].BeginReconfiguration(context.Background(), target); err != nil {
		t.Fatal(err)
	}
	if err := transport.cores["a"].FinishReconfiguration(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := wals["a"].Close(); err != nil {
		t.Fatal(err)
	}
	wals["a"] = nil
	reopenedWAL, err := qlog.Open(dirs["a"])
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := New(Config{NodeID: "a", Cluster: cluster, WAL: reopenedWAL, Transport: transport, EnableReconfiguration: true})
	if err != nil {
		t.Fatalf("restart did not replay certified membership history: %v", err)
	}
	wals["a"] = reopenedWAL
	transport.cores["a"] = restarted
	if got := restarted.CurrentCluster(); !sameCluster(got, target) {
		t.Fatalf("restarted cluster=%+v want=%+v", got, target)
	}
}

func TestReconfigurationWALCannotStartDisabled(t *testing.T) {
	dir := t.TempDir()
	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	cluster := Cluster{ConfigID: 1, Members: []Member{{ID: "a"}}}
	core, err := New(Config{NodeID: "a", Cluster: cluster, WAL: wal, EnableReconfiguration: true})
	if err != nil {
		t.Fatal(err)
	}
	proposal := newProposal(highestPriority, "a", []byte("marker"))
	if _, err := core.Record(context.Background(), RecordRequest{Slot: 1, Step: 4, ConfigID: 1, Proposal: proposal}); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := New(Config{NodeID: "a", Cluster: cluster, WAL: reopened}); err == nil {
		t.Fatal("disabled startup accepted a reconfiguration-enabled WAL")
	}
}

func TestTerminalRecordCatchesUpInsidePipelineWindow(t *testing.T) {
	cores, _ := reconfigCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	target := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}
	if _, err := cores["a"].BeginReconfiguration(ctx, target); err != nil {
		t.Fatal(err)
	}
	if err := cores["a"].RecoverThrough(ctx, 16); err != nil {
		t.Fatal(err)
	}
	prefix, ok := cores["a"].PrefixHash(16)
	if !ok {
		t.Fatal("missing drain prefix")
	}
	value, err := encodeReconfiguration(reconfigurationValue{Terminal: true, Freeze: 1, TerminalSlot: 17, Target: target, PrefixHash: prefix})
	if err != nil {
		t.Fatal(err)
	}
	laggedCores, _ := reconfigCluster(t)
	lagged := laggedCores["b"]
	freeze, ok := cores["a"].decision(1)
	if !ok {
		t.Fatal("missing freeze")
	}
	if err := lagged.AcceptCertifiedValue(freeze); err != nil {
		t.Fatal(err)
	}
	request := RecordRequest{Slot: 17, Step: 4, ConfigID: 1, ReconfigurationID: freeze.Hash, Proposal: newProposal(highestPriority, "a", value)}
	if request.Slot > lagged.Tip()+16 {
		t.Fatal("fixture is outside ordinary pipeline window")
	}
	if _, err := lagged.Record(ctx, request); err == nil {
		t.Fatal("voted without drain prefix")
	}
	through := lagged.RecordCatchUpThrough(request)
	if through != 16 {
		t.Fatalf("catch-up through=%d, want 16", through)
	}
	if err := lagged.StageValue(request.Proposal.Hash, value); err != nil {
		t.Fatal(err)
	}
	request.Proposal.Value = nil
	if got := lagged.RecordCatchUpThrough(request); got != through {
		t.Fatalf("hash-only catch-up=%d", got)
	}
	values, _, err := cores["a"].DecisionsFrom(lagged.Tip()+1, int(through-lagged.Tip()))
	if err != nil {
		t.Fatal(err)
	}
	if err := lagged.AcceptCertifiedHints(values); err != nil {
		t.Fatal(err)
	}
	if _, err := lagged.Record(ctx, request); err != nil {
		t.Fatalf("terminal vote after catch-up: %v", err)
	}
	ordinary := RecordRequest{Slot: 18, Proposal: newProposal(highestPriority, "a", []byte("ordinary"))}
	if got := lagged.RecordCatchUpThrough(ordinary); got != 2 {
		t.Fatalf("ordinary window changed: %d", got)
	}
}
