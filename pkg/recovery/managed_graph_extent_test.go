package recovery

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

func TestManagedGraphExtentUsesExplicitValidator(t *testing.T) {
	items := make([]any, 21800)
	for i := range items {
		items[i] = float64(1)
	}
	value, err := types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "r", Cypher: "RETURN $x", Args: map[string]any{"x": items}})
	if err != nil {
		t.Fatal(err)
	}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, WAL: wal, ValidateValue: types.ValidateManagedGraphExecutionPolicy})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	slot, _, err := core.Propose(ctx, value)
	if err != nil {
		t.Fatal(err)
	}
	decision, ok := core.CertifiedValue(slot)
	if !ok {
		t.Fatal("missing actual certificate")
	}
	extent := Extent{ConfigID: 1, Start: slot, End: slot, Decisions: []quepaxa.DecidedValue{decision}, EndPrefix: quepaxa.AdvancePrefixHash([32]byte{}, slot, decision.Hash)}
	if _, err := encodeExtent(extent); err == nil {
		t.Fatal("legacy encoded oversized value")
	}
	encoded, err := encodeExtentWithValueValidator(extent, types.ValidateManagedGraphExecutionPolicy)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeExtent(encoded); err == nil {
		t.Fatal("legacy decoded oversized value")
	}
	got, err := decodeExtentWithValueValidator(encoded, types.ValidateManagedGraphExecutionPolicy)
	if err != nil || len(got.Decisions) != 1 || !bytes.Equal(got.Decisions[0].Value, value) || !bytes.Equal(got.Decisions[0].Certificate, decision.Certificate) {
		t.Fatalf("profile extent mismatch: %v", err)
	}
	bucket := objstore.NewInMemBucket()
	writer, err := NewAppendOnlyManagerWithValueValidator(bucket, "graph", 1, types.ValidateManagedGraphExecutionPolicy)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if err := writer.SyncThrough(ctx, core, slot); err != nil {
		t.Fatal(err)
	}
	reader, err := NewAppendOnlyManagerWithValueValidator(bucket, "graph", 1, types.ValidateManagedGraphExecutionPolicy)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if err := reader.Load(ctx); err != nil {
		t.Fatal(err)
	}
	page, tip, err := reader.DecisionsFrom(ctx, 1, 1)
	if err != nil || tip != slot || len(page) != 1 || !bytes.Equal(page[0].Value, value) {
		t.Fatalf("actual Manager roundtrip tip=%d len=%d err=%v", tip, len(page), err)
	}
	legacy, err := NewAppendOnlyManager(bucket, "graph", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Close()
	if err := legacy.Load(ctx); err == nil {
		t.Fatal("legacy Manager loaded enlarged Graph archive")
	}
}
