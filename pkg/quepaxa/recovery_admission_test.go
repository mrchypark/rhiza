package quepaxa

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

func TestGapRecoveryProgressesWithFullWritePipeline(t *testing.T) {
	cores, _ := newTestCluster(t)
	core := cores["n1"]
	// Model every frontend proposal waiting for an earlier undecided slot.
	for range cap(core.pipeline) {
		core.pipeline <- struct{}{}
	}
	defer func() {
		for range cap(core.pipeline) {
			<-core.pipeline
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := core.RecoverThrough(ctx, 1); err != nil {
		t.Fatalf("gap recovery was starved by frontend proposals: %v", err)
	}
	if core.Tip() != 1 {
		t.Fatalf("recovered tip=%d", core.Tip())
	}
}

func TestNewEpochVotingWaitsForDurableTerminal(t *testing.T) {
	cores, transport := reconfigCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	target := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}
	if _, err := cores["a"].BeginReconfiguration(ctx, target); err != nil {
		t.Fatal(err)
	}
	if err := cores["a"].FinishReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	values, _, err := cores["a"].DecisionsFrom(1, 128)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := qlog.Open(filepath.Join(t.TempDir(), "wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	replica, err := New(Config{NodeID: "b", Cluster: *cores["a"].config, WAL: wal, Transport: transport, EnableReconfiguration: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := replica.AcceptCertifiedValues(values[:len(values)-1]); err != nil {
		t.Fatal(err)
	}
	terminal, err := replica.certifiedDecision(values[len(values)-1])
	if err != nil {
		t.Fatal(err)
	}
	// Pause at the state between learning the terminal and its disk barrier.
	if err := replica.acceptDecision(terminal); err != nil {
		t.Fatal(err)
	}
	request := RecordRequest{Slot: terminal.Slot + 1, Step: 4, ConfigID: 2, Proposal: newProposal(highestPriority, "a", []byte("next"))}
	if replica.IsVoter() {
		t.Fatal("non-durable terminal activated voting")
	}
	if _, err := replica.Record(ctx, request); err == nil {
		t.Fatal("next configuration voted before terminal durability")
	}
	if err := replica.EnsureDurable(terminal.Slot); err != nil {
		t.Fatal(err)
	}
	if !replica.IsVoter() {
		t.Fatal("durable terminal did not activate voter")
	}
	if _, err := replica.Record(ctx, request); err != nil {
		t.Fatalf("durable voter could not record: %v", err)
	}
}

func TestSparseFreezeWALRestartsBeforePrefixArrives(t *testing.T) {
	cores, transport := reconfigCluster(t)
	core := cores["a"]
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first, err := core.runSlot(ctx, 1, []byte("prefix-gap"), true)
	if err != nil {
		t.Fatal(err)
	}
	value, err := encodeReconfiguration(reconfigurationValue{
		Freeze: 2, TerminalSlot: 18,
		Target:     Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}},
		PrefixHash: AdvancePrefixHash([32]byte{}, 1, first.Proposal.Hash),
	})
	if err != nil {
		t.Fatal(err)
	}
	freeze, err := core.runSlot(ctx, 2, value, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := core.AcceptDecision(freeze); err != nil {
		t.Fatal(err)
	}
	// Copy the actual persisted stream into a separately locked WAL for restart.
	wal, err := qlog.Open(filepath.Join(t.TempDir(), "wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	if err := core.wal.Scan(func(entry qlog.Entry) error { return wal.Append(entry) }); err != nil {
		t.Fatal(err)
	}
	if err := wal.Sync(); err != nil {
		t.Fatal(err)
	}
	restarted, err := New(Config{NodeID: "a", Cluster: *core.config, WAL: wal, Transport: transport, EnableReconfiguration: true})
	if err != nil {
		t.Fatal(err)
	}
	if restarted.Tip() != 0 || restarted.reconfiguration != nil {
		t.Fatal("sparse freeze activated before its prefix")
	}
	if err := restarted.AcceptDecision(first); err != nil {
		t.Fatal(err)
	}
	if restarted.Tip() != 2 || restarted.reconfiguration == nil || restarted.reconfiguration.freeze != 2 {
		t.Fatal("replayed freeze did not activate when its prefix arrived")
	}
}

func TestFreezeAdmissionRejectsUnsafeStagedValues(t *testing.T) {
	for _, kind := range []string{"legacy", "slot", "prefix", "disabled"} {
		t.Run(kind, func(t *testing.T) {
			cores, _ := reconfigCluster(t)
			core, ctx := cores["a"], context.Background()
			control := reconfigurationValue{Freeze: 1, TerminalSlot: 17, Target: Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}}
			switch kind {
			case "legacy":
				core.reconfigEnabled = false
				if _, err := core.Record(ctx, RecordRequest{Slot: 17, Step: 4, Proposal: newProposal(highestPriority, "a", []byte("old"))}); err != nil {
					t.Fatal(err)
				}
				core.reconfigEnabled = true
			case "slot":
				control.Freeze, control.TerminalSlot = 2, 18
			case "prefix":
				control.PrefixHash[0] = 1
			case "disabled":
				core.reconfigEnabled = false
			}
			value, err := encodeReconfiguration(control)
			if err != nil {
				t.Fatal(err)
			}
			proposal := newProposal(highestPriority, "a", value)
			if err := core.StageValue(proposal.Hash, value); err != nil {
				t.Fatal(err)
			}
			proposal.Value = nil
			if _, err := core.Record(ctx, RecordRequest{Slot: 1, Step: 4, ConfigID: 1, ReconfigurationID: proposal.Hash, Proposal: proposal}); err == nil {
				t.Fatal("unsafe freeze accepted")
			}
			if _, exists := core.recorders[1]; exists {
				t.Fatal("rejected freeze left recorder state")
			}
		})
	}
}
