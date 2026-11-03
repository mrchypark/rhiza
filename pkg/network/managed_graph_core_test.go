package network

import (
	"context"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestManagedGraphCoreGuardAndReopen(t *testing.T) {
	items := make([]any, 21800)
	for i := range items {
		items[i] = float64(1)
	}
	frame, err := PrepareManagedGraphExecution(types.GraphCommand{RequestID: "r", Cypher: "RETURN $x", Args: map[string]any{"x": items}})
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = wal.Close() }()
	config := quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, WAL: wal}
	legacy, err := quepaxa.New(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, _, err := legacy.Propose(ctx, frame); err == nil {
		t.Fatal("legacy accepted enlarged frame")
	}
	config.ValidateValue = types.ValidateManagedGraphExecutionPolicy
	core, err := quepaxa.New(config)
	if err != nil {
		t.Fatal(err)
	}
	if slot, _, err := core.Propose(ctx, frame); err != nil || slot != 1 {
		t.Fatalf("actual proposal slot=%d err=%v", slot, err)
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
		t.Fatal("legacy reopened enlarged WAL")
	}
	config.ValidateValue = types.ValidateManagedGraphExecutionPolicy
	reopened, err := quepaxa.New(config)
	if err != nil || reopened.Tip() != 1 {
		t.Fatalf("profile reopen tip/error %v %v", reopened, err)
	}
	if _, _, err := reopened.Propose(ctx, make([]byte, 129<<10)); err == nil {
		t.Fatal("new profile accepted oversized unknown value")
	}
}
