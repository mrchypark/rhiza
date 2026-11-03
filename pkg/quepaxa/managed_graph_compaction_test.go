package quepaxa_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestManagedGraphCompactionRetainsSelectedValueGuard(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dir := t.TempDir()
	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = wal.Close() }()
	config := quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, WAL: wal, ValidateValue: types.ValidateManagedGraphExecutionPolicy}
	core, err := quepaxa.New(config)
	if err != nil {
		t.Fatal(err)
	}
	small, err := types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "base", Cypher: "RETURN 1"})
	if err != nil {
		t.Fatal(err)
	}
	if slot, _, err := core.Propose(ctx, small); err != nil || slot != 1 {
		t.Fatalf("base proposal slot=%d err=%v", slot, err)
	}
	prefix, _ := core.PrefixHash(1)
	order, _ := core.LeaderOrder(2)
	seal := quepaxa.CheckpointSeal{ConfigID: 1, Index: 1, RootHash: sha256.Sum256([]byte("verified-graph-root")), StateHash: sha256.Sum256([]byte("graph-state")), PrefixHash: prefix, NextLeaderOrder: order}
	// This consensus-only fixture supplies the independent snapshot validator;
	// it does not claim a native Graph checkpoint or provider qualification.
	core.SetCheckpointValidator(func(context.Context, quepaxa.CheckpointSeal) error { return nil })
	if err := core.PrepareCheckpoint(ctx, seal); err != nil {
		t.Fatal(err)
	}
	encodedSeal, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if slot, _, err := core.Propose(ctx, encodedSeal); err != nil || slot != 2 {
		t.Fatalf("seal proposal slot=%d err=%v", slot, err)
	}
	items := make([]any, 21800)
	for i := range items {
		items[i] = float64(1)
	}
	large, err := types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "suffix", Cypher: "RETURN $x", Args: map[string]any{"x": items}})
	if err != nil || len(large) <= quepaxa.MaxReplicatedValueBytes {
		t.Fatalf("large fixture bytes=%d err=%v", len(large), err)
	}
	if slot, _, err := core.Propose(ctx, large); err != nil || slot != 3 {
		t.Fatalf("certified suffix slot=%d err=%v", slot, err)
	}
	wantPrefix, _ := core.PrefixHash(3)
	before, tip, err := core.DecisionsFrom(3, 1)
	if err != nil || tip != 3 || len(before) != 1 || len(before[0].Certificate) == 0 {
		t.Fatalf("actual certified suffix tip=%d count=%d err=%v", tip, len(before), err)
	}
	if err := core.CompactThrough(1, seal.RootHash); err != nil {
		t.Fatalf("compact certified Graph suffix: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	wal, err = qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	config.WAL = wal
	config.ValidateValue = nil
	if _, err := quepaxa.New(config); err == nil {
		t.Fatal("legacy 128KiB profile reopened enlarged suffix")
	}
	config.ValidateValue = types.ValidateManagedGraphExecutionPolicy
	reopened, err := quepaxa.New(config)
	if err != nil {
		t.Fatal(err)
	}
	got, tip, err := reopened.DecisionsFrom(3, 1)
	gotPrefix, ok := reopened.PrefixHash(3)
	if err != nil || tip != 3 || reopened.CompactionFloor() != 1 || len(got) != 1 || !ok || gotPrefix != wantPrefix || !bytes.Equal(got[0].Value, large) || got[0].Hash != before[0].Hash || !bytes.Equal(got[0].Certificate, before[0].Certificate) {
		t.Fatalf("compaction changed certified suffix tip=%d floor=%d count=%d err=%v", tip, reopened.CompactionFloor(), len(got), err)
	}
	if _, _, err := reopened.Propose(ctx, bytes.Repeat([]byte{'x'}, 129<<10)); err == nil {
		t.Fatal("selected profile accepted oversized non-Graph value")
	}
}
