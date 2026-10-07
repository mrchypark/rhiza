package node

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestAdvanceCheckpointReadBarriersUsesRealServerAndStopsAtReservedTip(t *testing.T) {
	wal, err := qlog.Open(filepath.Join(t.TempDir(), "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	if err := wal.SetMaxBytes(4 << 20); err != nil {
		t.Fatal(err)
	}
	core, err := quepaxa.New(quepaxa.Config{
		NodeID: "node", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "node"}}}, WAL: wal, LocalMode: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	material, err := materializer.Open(filepath.Join(t.TempDir(), "db.sqlite"), 4)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	if err := material.ConfigureLocalGraphNodePropertyIndexes(nil); err != nil {
		t.Fatal(err)
	}
	server := network.NewServer(core, material, "cluster", true, nil)
	defer server.Close()

	if err := advanceCheckpointReadBarriers(context.Background(), 2, material, server); err != nil {
		t.Fatal(err)
	}
	if got := material.Tip(); got != 2 {
		t.Fatalf("materialized tip=%d, want reserved tip 2", got)
	}
	if err := advanceCheckpointReadBarriers(context.Background(), 2, material, server); err != nil {
		t.Fatal(err)
	}
	if got := core.Tip(); got != 2 {
		t.Fatalf("core tip=%d after already-satisfied floor, want 2", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := advanceCheckpointReadBarriers(ctx, 3, material, server); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled checkpoint advance error=%v, want context.Canceled", err)
	}
	if got := core.Tip(); got != 2 {
		t.Fatalf("core tip=%d after canceled advance, want 2", got)
	}
}
