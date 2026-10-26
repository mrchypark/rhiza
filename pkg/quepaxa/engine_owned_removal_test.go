package quepaxa

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

func engineOwnedRemovalCluster(t *testing.T) (map[NodeID]*Core, *clusterTransport, Cluster) {
	t.Helper()
	cores, transport := reconfigCluster(t)
	initial := Cluster{ConfigID: 1, Members: []Member{
		{ID: "a", WALIdentity: "wal-a"},
		{ID: "b", WALIdentity: "wal-b"},
		{ID: "c", WALIdentity: "wal-c"},
	}}
	for _, core := range cores {
		core.config.Members = append([]Member(nil), initial.Members...)
		core.configHistory[0].cluster = cloneCluster(initial)
		core.walIdentity = initial.MemberSet()[core.nodeID].WALIdentity
		core.engineOwnedRemovals = true
	}
	target := Cluster{ConfigID: 2, Members: []Member{initial.Members[0], initial.Members[1]}}
	return cores, transport, target
}

func TestEngineOwnedRemovalFreezesWithoutRemovedVoterAtOriginalQuorum(t *testing.T) {
	cores, transport, target := engineOwnedRemovalCluster(t)
	ctx := context.Background()
	transport.fail("b")
	if _, err := cores["a"].BeginEngineOwnedRemovalAt(ctx, target, "remove-c-1", 0); err == nil {
		t.Fatal("freeze succeeded using the removed voter while surviving voter b was unavailable")
	}
	transport.recover("b")
	operation, err := cores["a"].BeginEngineOwnedRemovalAt(ctx, target, "remove-c-1", 0)
	if err != nil {
		t.Fatalf("freeze with the original quorum excluding c: %v", err)
	}
	value, ok := cores["a"].decision(operation.Freeze)
	if !ok {
		t.Fatal("missing decided engine-owned freeze")
	}
	decision, err := decodeDecision(value.Certificate)
	if err != nil {
		t.Fatalf("decode freeze certificate: %v", err)
	}
	if decision.Slot != operation.Freeze || decision.Proposal.Hash != value.Hash {
		t.Fatalf("freeze certificate does not match decided value: %+v", decision)
	}
	got := make(map[NodeID]bool, len(decision.Summaries))
	for _, summary := range decision.Summaries {
		got[summary.RecorderID] = true
	}
	if len(got) != 2 || !got["a"] || !got["b"] || got["c"] {
		t.Fatalf("freeze recorders=%v, want exactly the original quorum a/b without removed c", got)
	}
	if operation.FreezeHash != value.Hash {
		t.Fatalf("freeze hash=%x, decision=%x", operation.FreezeHash, value.Hash)
	}
}

func TestEngineOwnedRemovalRequiresMarkerAndStorageAdmission(t *testing.T) {
	cores, _, target := engineOwnedRemovalCluster(t)
	ctx := context.Background()
	if _, err := cores["a"].BeginReconfiguration(ctx, target); err == nil {
		t.Fatal("legacy BeginReconfiguration accepted a new removal with engine-owned capability enabled")
	}
	legacyFreeze, err := encodeReconfiguration(reconfigurationValue{Freeze: 1, TerminalSlot: 17, Target: target})
	if err != nil {
		t.Fatal(err)
	}
	proposal := newProposal(highestPriority, "a", legacyFreeze)
	if _, err := cores["a"].Record(ctx, RecordRequest{Slot: 1, Step: 4, ConfigID: 1, Proposal: proposal}); err == nil {
		t.Fatal("new unmarked removal freeze passed the engine-owned record guard")
	}

	operation, err := cores["a"].BeginEngineOwnedRemovalAt(ctx, target, "remove-c-2", 0)
	if err != nil {
		t.Fatalf("begin engine-owned removal: %v", err)
	}
	if err := cores["b"].FinishReconfiguration(ctx); err == nil {
		t.Fatal("another survivor used the legacy finish path to bypass pending storage closure")
	}
	operation.StorageEffectsExcludedHash = ValueHash{1}
	if err := cores["b"].FinishEngineOwnedRemovalAt(ctx, operation); err == nil {
		t.Fatal("engine-owned terminal passed without a storage admission validator")
	}
	for _, core := range cores {
		core.reconfigAdmission = func(context.Context, ReconfigurationAdmission) error {
			return errors.New("storage closure remains pending")
		}
	}
	if err := cores["b"].FinishEngineOwnedRemovalAt(ctx, operation); err == nil {
		t.Fatal("another survivor committed the terminal while storage closure was pending")
	}
	if cores["b"].CurrentCluster().ConfigID != 1 {
		t.Fatal("pending storage closure changed the active membership")
	}
}

func TestLegacyRemovalHistoryReplaysWithEngineOwnedCapability(t *testing.T) {
	cores, _ := reconfigCluster(t)
	target := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}
	ctx := context.Background()
	if _, err := cores["a"].BeginReconfiguration(ctx, target); err != nil {
		t.Fatalf("legacy removal freeze: %v", err)
	}
	if err := cores["a"].FinishReconfiguration(ctx); err != nil {
		t.Fatalf("legacy removal terminal: %v", err)
	}
	decisions, _, err := cores["a"].DecisionsFrom(1, 128)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := qlog.Open(filepath.Join(t.TempDir(), "replay"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	replayed, err := New(Config{
		NodeID: "a", Cluster: Cluster{ConfigID: 1, Members: []Member{{ID: "a"}, {ID: "b"}, {ID: "c"}}},
		WAL: wal, Transport: cores["a"].transport, EnableReconfiguration: true, EngineOwnedRemovals: true,
	})
	if err != nil {
		t.Fatalf("create engine-owned replay core: %v", err)
	}
	if err := replayed.AcceptCertifiedHints(decisions); err != nil {
		t.Fatalf("replay historical unmarked removal: %v", err)
	}
	if !sameCluster(replayed.CurrentCluster(), target) {
		t.Fatalf("replayed cluster=%+v, want historical target %+v", replayed.CurrentCluster(), target)
	}
}
