package quepaxa

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

func TestInstallFencedGenerationBasePersistsTargetLineage(t *testing.T) {
	dir := t.TempDir()
	config := Config{NodeID: "target", Cluster: Cluster{ConfigID: 1, Members: []Member{{ID: "target"}}}, EnableReconfiguration: true}
	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	config.WAL = wal
	core, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	fenced := FencedGenerationBase{
		ConfigID: 1, Index: 137,
		PrefixHash: sha256.Sum256([]byte("source-prefix")),
		RootHash:   sha256.Sum256([]byte("source-root")),
		AnchorHash: sha256.Sum256([]byte("immutable-generation-anchor")),
	}
	if err := core.InstallFencedGenerationBase(context.Background(), fenced); err != nil {
		t.Fatal(err)
	}
	if got := core.GenerationAnchorHash(); got != fenced.AnchorHash {
		t.Fatalf("generation anchor=%x want %x", got, fenced.AnchorHash)
	}
	if core.Tip() != fenced.Index || core.CompactionFloor() != fenced.Index {
		t.Fatalf("installed floor/tip = %d/%d", core.CompactionFloor(), core.Tip())
	}
	if err := core.InstallFencedGenerationBase(context.Background(), fenced); err != nil {
		t.Fatalf("same fenced base is not idempotent: %v", err)
	}
	wrong := fenced
	wrong.AnchorHash = sha256.Sum256([]byte("other-anchor"))
	if err := core.InstallFencedGenerationBase(context.Background(), wrong); err == nil {
		t.Fatal("accepted a different anchored lineage")
	}

	if slot, _, err := core.Propose(context.Background(), []byte("target-first")); err != nil || slot != fenced.Index+1 {
		t.Fatalf("first target proposal = %d, %v", slot, err)
	}
	index := core.Tip()
	prefix, ok := core.PrefixHash(index)
	if !ok {
		t.Fatal("missing target prefix")
	}
	next, following, err := core.CheckpointLeaderOrders(index)
	if err != nil {
		t.Fatal(err)
	}
	membership, err := core.CheckpointMembership(index)
	if err != nil {
		t.Fatal(err)
	}
	seal := CheckpointSeal{
		ConfigID: 1, Index: index,
		RootHash:   sha256.Sum256([]byte("target-checkpoint-root")),
		StateHash:  sha256.Sum256([]byte("target-checkpoint-state")),
		PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following,
		Membership: &membership, GenerationAnchorHash: fenced.AnchorHash,
	}
	core.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	if err := core.PrepareCheckpoint(context.Background(), seal); err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(context.Background(), encoded); err != nil {
		t.Fatal(err)
	}
	if err := core.CompactThrough(index, seal.RootHash); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	reopenedWAL, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedWAL.Close()
	config.WAL = reopenedWAL
	restarted, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	if got := restarted.GenerationAnchorHash(); got != fenced.AnchorHash {
		t.Fatalf("recovered generation anchor=%x want %x", got, fenced.AnchorHash)
	}
	if restarted.CompactionFloor() != index {
		t.Fatalf("recovered floor=%d want %d", restarted.CompactionFloor(), index)
	}
	if slot, _, err := restarted.Propose(context.Background(), []byte("target-after-restart")); err != nil || slot != index+2 {
		t.Fatalf("restart proposal = %d, %v", slot, err)
	}
}

func TestInstallFencedGenerationBaseRejectsConsensusHistory(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := New(Config{NodeID: "target", Cluster: Cluster{ConfigID: 1, Members: []Member{{ID: "target"}}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(context.Background(), []byte("ordinary-history")); err != nil {
		t.Fatal(err)
	}
	err = core.InstallFencedGenerationBase(context.Background(), FencedGenerationBase{
		ConfigID: 1, Index: 7,
		PrefixHash: sha256.Sum256([]byte("prefix")), RootHash: sha256.Sum256([]byte("root")), AnchorHash: sha256.Sum256([]byte("anchor")),
	})
	if err == nil {
		t.Fatal("accepted nonempty unanchored WAL")
	}
}

func TestInstallFencedGenerationBaseRejectsMaxSlot(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := New(Config{NodeID: "target", Cluster: Cluster{ConfigID: 1, Members: []Member{{ID: "target"}}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	if err := core.InstallFencedGenerationBase(context.Background(), FencedGenerationBase{
		ConfigID: 1, Index: ^Slot(0),
		PrefixHash: sha256.Sum256([]byte("prefix")), RootHash: sha256.Sum256([]byte("root")), AnchorHash: sha256.Sum256([]byte("anchor")),
	}); err == nil {
		t.Fatal("accepted fenced base at maximum slot")
	}
}
