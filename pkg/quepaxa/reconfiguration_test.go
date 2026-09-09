package quepaxa

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

func reconfigCluster(t *testing.T) (map[NodeID]*Core, *clusterTransport) {
	t.Helper()
	members := []Member{{ID: "a"}, {ID: "b"}, {ID: "c"}}
	transport := &clusterTransport{cores: map[NodeID]*Core{}, down: map[NodeID]bool{}, dropDecision: map[NodeID]bool{}}
	for _, member := range members {
		wal, err := qlog.Open(filepath.Join(t.TempDir(), string(member.ID)))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		core := newCore(member.ID, &Cluster{ConfigID: 1, Members: members}, wal, transport)
		core.reconfigEnabled = true
		transport.cores[member.ID] = core
	}
	return transport.cores, transport
}

func TestClusterSnapshotsDoNotExposeMembership(t *testing.T) {
	cores, _ := reconfigCluster(t)
	core := cores["a"]
	before := core.clusterForSlot(1)
	if core.ConfigID() != before.ConfigID || core.ConfigIDForSlot(1) != before.ConfigID {
		t.Fatal("configuration ID lookup differs from initial snapshot")
	}
	for _, snapshot := range []Cluster{core.CurrentCluster(), core.ClusterForSlot(1)} {
		snapshot.Members[0].ID = "intruder"
	}
	if !core.IsVoter() || core.ClusterForSlot(1).Members[0].ID != "a" {
		t.Fatal("public snapshot changed voter membership")
	}
	ctx := context.Background()
	if _, err := core.BeginReconfiguration(ctx, Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}); err != nil {
		t.Fatal(err)
	}
	if err := core.FinishReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	if len(before.Members) != 3 || !sameCluster(before, core.ClusterForSlot(1)) {
		t.Fatal("reconfiguration mutated historical snapshot")
	}
	snapshot := core.CurrentCluster()
	if core.ConfigID() != snapshot.ConfigID || core.ConfigIDForSlot(core.Tip()+1) != snapshot.ConfigID || core.ConfigIDForSlot(1) != before.ConfigID {
		t.Fatal("configuration ID lookup lost transition boundary or historical configuration")
	}
	if allocs := testing.AllocsPerRun(100, func() {
		_ = core.ConfigID()
		_ = core.ConfigIDForSlot(1)
	}); allocs != 0 {
		t.Fatalf("configuration ID lookup allocated %g times", allocs)
	}
	snapshot.Members[0].ID = "intruder"
	if !core.IsVoter() || core.CurrentCluster().Members[0].ID != "a" {
		t.Fatal("public snapshot changed new voter membership")
	}
}

func TestReconfigurationFreezeDrainAndRemoval(t *testing.T) {
	cores, transport := reconfigCluster(t)
	ctx := context.Background()
	if _, _, err := cores["a"].Propose(ctx, []byte("before")); err != nil {
		t.Fatal(err)
	}
	freeze, err := cores["a"].BeginReconfiguration(ctx, Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}})
	if err != nil {
		t.Fatal(err)
	}
	if freeze != 2 {
		t.Fatalf("freeze=%d want 2", freeze)
	}
	if _, _, err := cores["a"].Propose(ctx, []byte("blocked")); err == nil {
		t.Fatal("ordinary write passed frozen drain")
	}
	if err := cores["a"].FinishReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	if got := cores["a"].CurrentCluster(); got.ConfigID != 2 || len(got.Members) != 2 {
		t.Fatalf("active cluster=%+v", got)
	}
	transport.fail("c")
	if _, _, err := cores["a"].Propose(ctx, []byte("after")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 120; i++ {
		order := cores["a"].ProposerOrder()
		for _, id := range order {
			if id == "c" {
				t.Fatalf("retired voter appeared in leader order at write %d", i)
			}
		}
		if _, _, err := cores["a"].Propose(ctx, []byte{byte(i)}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	cores["a"].mu.RLock()
	observations := len(cores["a"].timings)
	cores["a"].mu.RUnlock()
	if observations == 0 {
		t.Fatal("new generation stopped collecting adaptive leader timings")
	}
	if _, _, err := cores["c"].Propose(ctx, []byte("stale")); !errors.Is(err, ErrQuorumUnavailable) {
		t.Fatalf("old voter error=%v", err)
	}
	if err := cores["a"].CompactThrough(1, [32]byte{1}); err == nil {
		t.Fatal("reconfiguration mode allowed compaction")
	}
}

func TestReconfigurationRecordBoundAndConfigID(t *testing.T) {
	cores, _ := reconfigCluster(t)
	proposal := newProposal(highestPriority, "a", []byte("x"))
	if _, err := cores["a"].Record(context.Background(), RecordRequest{Slot: 17, Step: 4, ConfigID: 1, Proposal: proposal}); err == nil {
		t.Fatal("accepted slot beyond tip+16")
	}
	if _, err := cores["a"].Record(context.Background(), RecordRequest{Slot: 1, Step: 4, ConfigID: 2, Proposal: proposal}); err == nil {
		t.Fatal("accepted wrong config ID")
	}
}

func TestReconfigurationFreezeRequiresCapableQuorum(t *testing.T) {
	cores, _ := reconfigCluster(t)
	cores["b"].reconfigEnabled = false
	cores["c"].reconfigEnabled = false
	if _, err := cores["a"].BeginReconfiguration(context.Background(), Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}); err == nil {
		t.Fatal("upgraded minority froze a mixed-version quorum")
	}
}

func TestLearnerWALIdentityPreventsReplayedPromotion(t *testing.T) {
	initial := Cluster{ConfigID: 1, Members: []Member{{ID: "a"}, {ID: "b"}, {ID: "c"}}}
	learnerDir := t.TempDir()
	open := func(dir string) *qlog.WAL {
		wal, err := qlog.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		return wal
	}
	wal := open(learnerDir)
	learner, err := NewLearner(Config{NodeID: "d", Cluster: initial, WAL: wal, Transport: &clusterTransport{}})
	if err != nil {
		t.Fatal(err)
	}
	target := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}, {ID: "d", WALIdentity: learner.WALIdentity()}}}
	learner.configHistory = append(learner.configHistory, configEpoch{start: 2, cluster: target})
	learner.tip = 1
	learner.durable[1] = true
	if !learner.IsVoter() {
		t.Fatal("intact learner was not promoted")
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := open(learnerDir)
	defer reopened.Close()
	intact, err := NewLearner(Config{NodeID: "d", Cluster: initial, WAL: reopened, Transport: &clusterTransport{}})
	if err != nil {
		t.Fatal(err)
	}
	intact.configHistory = append(intact.configHistory, configEpoch{start: 2, cluster: target})
	intact.tip = 1
	intact.durable[1] = true
	if !intact.IsVoter() {
		t.Fatal("intact WAL restart lost promotion")
	}
	freshWal := open(t.TempDir())
	defer freshWal.Close()
	fresh, err := NewLearner(Config{NodeID: "d", Cluster: initial, WAL: freshWal, Transport: &clusterTransport{}})
	if err != nil {
		t.Fatal(err)
	}
	fresh.configHistory = append(fresh.configHistory, configEpoch{start: 2, cluster: target})
	fresh.tip = 1
	if fresh.IsVoter() {
		t.Fatal("fresh WAL replayed old promotion")
	}
	if _, err := New(Config{NodeID: "d", Cluster: target, WAL: freshWal, Transport: &clusterTransport{}}); err == nil {
		t.Fatal("ordinary voter constructor bypassed enrolled WAL identity")
	}
}
