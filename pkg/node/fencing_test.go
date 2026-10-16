package node

import (
	"context"
	"errors"
	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"testing"
	"time"
)

func TestTerminalDrainDoesNotInventReceiptForClosedIncarnation(t *testing.T) {
	n := &Node{}
	for range 2 {
		result, err := n.TerminalDrain(context.Background(), network.TerminalDrainRequest{})
		if err == nil || result.StorageKnown || result.WALIdentity != "" {
			t.Fatalf("unopened/closed incarnation gained receipt: %+v %v", result, err)
		}
	}
	if err := n.Open(context.Background()); err == nil {
		t.Fatal("terminal node reopened")
	}
}

func TestTerminalDrainRepeatRespectsDeadlineWhileFirstDrainBlocked(t *testing.T) {
	expected := network.TerminalDrainRequest{ClusterID: "cluster", NodeID: "node", WALIdentity: "wal"}
	n := &Node{terminalDone: make(chan struct{}), terminalIdentity: expected}
	n.terminalRequested.Store(true)
	n.shutdownMu.Lock()
	defer n.shutdownMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := n.TerminalDrain(ctx, expected); done <- err }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("repeat drain=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("repeat drain blocked on shutdown lock instead of context")
	}
}

func TestTerminalDrainValidatesIdentityBeforeStoppingLiveEngine(t *testing.T) {
	n := New(&types.ExecutionConfig{ClusterID: "drain-cluster", NodeID: "n1"})
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := wal.BindIdentity([]byte("01234567890123456789012345678901")); err != nil {
		t.Fatal(err)
	}
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := objectstore.NewBucket(objectstore.Config{Provider: objectstore.ProviderFilesystem, FilesystemDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	n.wal, n.core, n.bucket = wal, core, bucket
	n.opened.Store(true)
	defer n.Shutdown()
	expected := network.TerminalDrainRequest{ClusterID: "drain-cluster", NodeID: "n1", WALIdentity: n.core.WALIdentity()}
	for _, invalid := range []network.TerminalDrainRequest{
		{},
		{ClusterID: "other", NodeID: expected.NodeID, WALIdentity: expected.WALIdentity},
		{ClusterID: expected.ClusterID, NodeID: "other", WALIdentity: expected.WALIdentity},
		{ClusterID: expected.ClusterID, NodeID: expected.NodeID, WALIdentity: "other"},
	} {
		if _, err := n.TerminalDrain(context.Background(), invalid); err == nil {
			t.Fatal("wrong identity drained")
		}
		if !n.opened.Load() || n.terminalRequested.Load() || n.core == nil {
			t.Fatal("identity mismatch stopped engine")
		}
	}
	for range 2 {
		result, err := n.TerminalDrain(context.Background(), expected)
		if err != nil || !result.StorageKnown || result.WALIdentity != expected.WALIdentity {
			t.Fatalf("terminal result=%+v error=%v", result, err)
		}
	}
	if err := n.Open(context.Background()); err == nil {
		t.Fatal("terminal engine reopened")
	}
}
