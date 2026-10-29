package network

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func resolveExecuteFixture(t *testing.T) (*Server, *quepaxa.Core, ExecuteRequest, ExecuteResponse) {
	t.Helper()
	core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}}, nil, nil)
	material, err := materializer.Open(t.TempDir()+"/state.sqlite", 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := material.Close(); err != nil {
			t.Error(err)
		}
	})
	server := NewServer(core, material, "cluster", true, nil)
	t.Cleanup(server.Close)
	if _, err := server.Execute(context.Background(), ExecuteRequest{RequestID: "setup", SQL: "CREATE TABLE resolution (id INTEGER PRIMARY KEY)"}); err != nil {
		t.Fatal(err)
	}
	req := ExecuteRequest{RequestID: "insert", SQL: "INSERT INTO resolution VALUES (7)"}
	result, err := server.Execute(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	return server, core, req, result
}

func TestResolveExecuteExpiredRemainsUnknown(t *testing.T) {
	server, core, req, receipt := resolveExecuteFixture(t)
	var decisions []quepaxa.DecidedValue
	barrier := types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{})
	for core.Tip() <= quepaxa.Slot(receipt.RetryThroughSlot) {
		_, _, err := core.Propose(context.Background(), barrier)
		if err != nil {
			t.Fatal(err)
		}
	}
	for slot := quepaxa.Slot(server.material.Tip() + 1); slot <= core.Tip(); slot++ {
		decision, ok := core.CertifiedValue(slot)
		if !ok {
			t.Fatal("certified barrier missing")
		}
		decisions = append(decisions, decision)
	}
	if err := server.material.ApplyBatch(context.Background(), decisions); err != nil {
		t.Fatal(err)
	}
	before := core.Tip()
	result, err := server.ResolveExecute(context.Background(), req)
	if err != nil || result.State != ResolveExecuteUnknownOrExpired || result.Receipt != nil || result.Durability != ResolveExecuteDurabilityUnknown {
		t.Fatalf("expired=%+v err=%v", result, err)
	}
	if core.Tip() != before {
		t.Fatal("resolution changed tip")
	}
}

func TestResolveExecuteDoesNotWaitForWriterStripe(t *testing.T) {
	server, _, req, _ := resolveExecuteFixture(t)
	req.RequestID = "inflight"
	release, err := server.lockRequest(context.Background(), req.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := server.ResolveExecute(ctx, req)
	if err != nil || result.State != ResolveExecuteUnknownOrExpired || result.Receipt != nil {
		t.Fatalf("held writer stripe result=%+v err=%v", result, err)
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if _, err := server.ResolveExecute(canceled, req); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled lookup: %v", err)
	}
}

func TestResolveExecuteExactConflictAndUnknown(t *testing.T) {
	server, core, req, expected := resolveExecuteFixture(t)
	ctx := context.Background()
	before := core.Tip()
	result, err := server.ResolveExecute(ctx, req)
	if err != nil || result.State != ResolveExecuteRetained || result.Durability != ResolveExecuteDurabilityNotConfigured || result.Receipt == nil || *result.Receipt != expected.MutationReceipt || !reflect.DeepEqual(result.Statements, expected.Statements) {
		t.Fatalf("exact result=%+v err=%v", result, err)
	}
	conflict := req
	conflict.SQL = "INSERT INTO resolution VALUES (8)"
	if _, err := server.ResolveExecute(ctx, conflict); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("conflict: %v", err)
	}
	missing := req
	missing.RequestID = "missing"
	unknown, err := server.ResolveExecute(ctx, missing)
	if err != nil || unknown.State != ResolveExecuteUnknownOrExpired || unknown.Receipt != nil || unknown.Durability != ResolveExecuteDurabilityUnknown {
		t.Fatalf("missing result=%+v err=%v", unknown, err)
	}
	if core.Tip() != before {
		t.Fatalf("resolution changed tip: %d -> %d", before, core.Tip())
	}
	rows, err := server.Query(ctx, QueryRequest{SQL: "SELECT id FROM resolution"})
	if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != int64(7) {
		t.Fatalf("rows=%v err=%v", rows.Rows, err)
	}
}

func TestResolveExecuteDurabilityAndCancellation(t *testing.T) {
	server, core, req, expected := resolveExecuteFixture(t)
	before := core.Tip()
	var gotSlot quepaxa.Slot
	server.SetDurabilityBarrier(func(_ context.Context, slot quepaxa.Slot) error { gotSlot = slot; return nil })
	result, err := server.ResolveExecute(context.Background(), req)
	if err != nil || result.Durability != ResolveExecuteDurabilityConfirmed || result.Receipt == nil || *result.Receipt != expected.MutationReceipt || uint64(gotSlot) != expected.Slot {
		t.Fatalf("confirmed=%+v slot=%d err=%v", result, gotSlot, err)
	}
	server.SetDurabilityBarrier(func(context.Context, quepaxa.Slot) error { return errors.New("unavailable") })
	result, err = server.ResolveExecute(context.Background(), req)
	if err == nil || result.State != ResolveExecuteRetained || result.Durability != ResolveExecuteDurabilityUnknown || result.Receipt == nil || *result.Receipt != expected.MutationReceipt {
		t.Fatalf("failed barrier=%+v err=%v", result, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server.SetDurabilityBarrier(func(ctx context.Context, _ quepaxa.Slot) error { cancel(); return ctx.Err() })
	result, err = server.ResolveExecute(ctx, req)
	if !errors.Is(err, ErrCommitUnknown) || !errors.Is(err, ErrDurabilityUnavailable) || result.State != ResolveExecuteRetained || result.Durability != ResolveExecuteDurabilityUnknown || result.Receipt == nil || *result.Receipt != expected.MutationReceipt {
		t.Fatalf("canceled barrier=%+v err=%v", result, err)
	}
	if core.Tip() != before {
		t.Fatalf("resolution changed tip: %d -> %d", before, core.Tip())
	}
	rows, err := server.Query(context.Background(), QueryRequest{SQL: "SELECT id FROM resolution"})
	if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != int64(7) {
		t.Fatalf("rows=%v err=%v", rows.Rows, err)
	}
}
