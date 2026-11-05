package node

import (
	"context"
	"errors"
	"testing"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
)

// This qualifies only the pre-Core paired-file boundary, not authority or a
// fabricated startup binding. Sources come from the actual policy10 snapshot.
func TestManagedGraphCheckpointPairBeforeCore(t *testing.T) {
	ctx := context.Background()
	m, err := materializer.OpenManagedSQLKVGraphWithOriginals(t.TempDir()+"/sqlite.db", 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	value, err := types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "\xff", Cypher: "CREATE (:Item {n: 5})"})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 1, value); err != nil {
		t.Fatal(err)
	}
	snapshot, tip, release, err := m.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	files := make([]checkpoint.File, len(snapshot))
	for i, file := range snapshot {
		files[i] = checkpoint.File{Role: string(file.Role), Path: file.Path}
	}
	n := New(&types.ExecutionConfig{})
	n.managedStartup = &ManagedStartupConfig{expectedPolicy: sqlpolicy.ManagedGraphRequestMaterializationPolicy}
	if err := n.validateCheckpointFiles(ctx, files, tip); err != nil {
		t.Fatalf("actual paired snapshot rejected before Core: %v", err)
	}
	if err := n.validateCheckpointFiles(ctx, files, tip+1); err == nil {
		t.Fatal("wrong certified checkpoint tip accepted")
	}
	if err := n.validateCheckpointFiles(ctx, files[:1], tip); err == nil {
		t.Fatal("missing Graph checkpoint role accepted")
	}
	if n.core != nil || n.material != nil {
		t.Fatal("checkpoint validation installed the target runtime")
	}
}

// No synthetic binding or permissive checker claims a live Graph runtime.
func TestManagedGraphCancelledReadNeverTouchesUnopenedResources(t *testing.T) {
	n := New(&types.ExecutionConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	checks := []func() error{
		func() error { _, err := n.GraphQueryManaged(ctx, network.GraphQueryRequest{}); return err },
		func() error { _, err := n.GraphReachableManaged(ctx, types.GraphReachableRequest{}); return err },
		func() error { _, err := n.GraphChangesManaged(ctx, network.GraphStreamReadRequest{}); return err },
		func() error { _, err := n.GraphStreamReadManaged(ctx, network.GraphStreamReadRequest{}); return err },
		func() error {
			_, err := n.GraphStreamOffsetManaged(ctx, network.GraphStreamOffsetRequest{})
			return err
		},
	}
	for i, check := range checks {
		if err := check(); !errors.Is(err, context.Canceled) {
			t.Fatalf("read %d did not preserve cancellation before resource access: %v", i, err)
		}
	}
}
