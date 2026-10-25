package node

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestNodeOpenRecoversDurablePendingRecorderBeforeReturning(t *testing.T) {
	ctx := context.Background()
	config := types.ExecutionConfig{Local: true, NodeID: "local", DataDir: t.TempDir()}

	initial := New(&config)
	if err := initial.Open(ctx); err != nil {
		t.Fatalf("initial Node.Open: %v", err)
	}
	if err := initial.Shutdown(); err != nil {
		t.Fatalf("initial Node.Shutdown: %v", err)
	}

	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "startup-recorder", SQL: "CREATE TABLE startup_recorder(id INTEGER PRIMARY KEY)"}})
	if err != nil {
		t.Fatal(err)
	}
	walPath := filepath.Join(config.DataDir, "qlog")
	wal, err := qlog.Open(walPath)
	if err != nil {
		t.Fatalf("open WAL for durable recorder fixture: %v", err)
	}
	appendLocalRecorderFixture(t, wal, value)
	if err := wal.Close(); err != nil {
		t.Fatalf("close durable recorder fixture WAL: %v", err)
	}

	// Confirm the persisted ISR is a supported ahead-of-tip state before
	// exercising the real Node.Open startup path.
	probeWAL, err := qlog.Open(walPath)
	if err != nil {
		t.Fatalf("reopen WAL to inspect durable recorder state: %v", err)
	}
	if err := probeWAL.SetMaxBytes(4 << 30); err != nil {
		_ = probeWAL.Close()
		t.Fatalf("set finite capacity for local Core inspection: %v", err)
	}
	probe, err := quepaxa.New(quepaxa.Config{
		NodeID: "local", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "local"}}},
		WAL: probeWAL, LocalMode: true,
	})
	if err != nil {
		_ = probeWAL.Close()
		t.Fatalf("recover core view of durable recorder state: %v", err)
	}
	if gotTip, gotRecorderTip := probe.Tip(), probe.RecorderTip(); gotTip != 0 || gotRecorderTip != 1 {
		_ = probeWAL.Close()
		t.Fatalf("pre-open state tip=%d recorder_tip=%d, want 0/1", gotTip, gotRecorderTip)
	}
	if err := probeWAL.Close(); err != nil {
		t.Fatalf("close inspected WAL: %v", err)
	}

	n := New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatalf("Node.Open with durable recorder ahead of Core.Tip: %v", err)
	}
	defer func() {
		if err := n.Shutdown(); err != nil {
			t.Errorf("Node.Shutdown: %v", err)
		}
	}()
	if gotTip, gotRecorderTip := n.core.Tip(), n.core.RecorderTip(); gotTip != 1 || gotRecorderTip != 1 {
		t.Fatalf("post-open state tip=%d recorder_tip=%d, want 1/1", gotTip, gotRecorderTip)
	}
	if !n.Ready() {
		t.Fatal("local Node did not become ready after recovering its durable recorder state")
	}
	if _, err := n.server.Query(ctx, network.QueryRequest{SQL: "SELECT id FROM startup_recorder", Consistency: "linearizable"}); err != nil {
		t.Fatalf("query materialized recovered decision: %v", err)
	}
}
