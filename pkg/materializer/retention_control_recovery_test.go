package materializer

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestManagedControlSlotsExpireLogicallyButReclaimOnApplication(t *testing.T) {
	m := requestMaterializer(t)
	requestSQL(t, m, 1, types.SQLCommand{RequestID: "schema", SQL: `CREATE TABLE item(v INTEGER)`})
	requestSQL(t, m, 2, types.SQLCommand{RequestID: "result", SQL: `INSERT INTO item VALUES(1) RETURNING v`, WantRows: true})
	before := m.sqlReceiptRetention.resultBytes
	if before == 0 {
		t.Fatal("expected retained SQL result bytes")
	}
	maxBytes, headroom := before+2, uint64(3)
	if target, err := m.sqlRetentionBarrierTarget(context.Background(), maxBytes, headroom); err != nil || target != 0 {
		t.Fatalf("barriers requested without full rejection: target=%d err=%v", target, err)
	}
	_, err := m.writer.Exec(`INSERT INTO _rhiza_idempotency(kind, request_id, fingerprint, commit_slot, status, error_code, rows_affected, last_insert_id, applied, sql_result) VALUES (?, ?, zeroblob(32), 2, ?, ?, 0, 0, 0, X'')`, types.MutationSQL, "full", types.MutationRejected, types.MutationErrorCodeResultRetentionFull)
	if err != nil {
		t.Fatal(err)
	}
	m.sqlReceiptRetention.receipts++
	m.sqlRetentionPlan = sqlRetentionBarrierPlan{}
	if target, err := m.sqlRetentionBarrierTarget(context.Background(), maxBytes, headroom); err != nil || target != 1026 {
		t.Fatalf("incorrect retention target: target=%d err=%v", target, err)
	}
	barrier := types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{})
	controls := make([]quepaxa.DecidedValue, 0, 1023)
	for slot := 3; slot <= 1025; slot++ {
		controls = append(controls, quepaxa.DecidedValue{Slot: quepaxa.Slot(slot), Value: barrier})
	}
	if err := m.ApplyBatch(context.Background(), controls); err != nil {
		t.Fatal(err)
	}
	if _, found, err := m.RetainedRequestOriginal(context.Background(), types.MutationSQL, "result"); err != nil || !found {
		t.Fatalf("original not visible through inclusive boundary: found=%t err=%v", found, err)
	}
	if err := m.Apply(context.Background(), 1026, barrier); err != nil {
		t.Fatal(err)
	}
	if target, err := m.sqlRetentionBarrierTarget(context.Background(), maxBytes, headroom); err != nil || target != 0 {
		t.Fatalf("barriers continued after logical expiry: target=%d err=%v", target, err)
	}
	if _, found, err := m.RetainedRequestOriginal(context.Background(), types.MutationSQL, "result"); err != nil || found {
		t.Fatalf("expired original remains logically visible: found=%t err=%v", found, err)
	}
	if got := m.sqlReceiptRetention.resultBytes; got != before {
		t.Fatalf("control slots physically reclaimed result: before=%d after=%d", before, got)
	}
	requestSQL(t, m, 1027, types.SQLCommand{RequestID: "new", SQL: `INSERT INTO item VALUES(2)`})
	if got := m.sqlReceiptRetention.resultBytes; got != 0 {
		t.Fatalf("application did not reclaim expired result bytes: got=%d", got)
	}
}

func TestOrdinaryControlSlotsReclaimAtRetentionBoundary(t *testing.T) {
	m, err := Open(filepath.Join(t.TempDir(), "ordinary.db"), 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	requestSQL(t, m, 1, types.SQLCommand{RequestID: "schema", SQL: `CREATE TABLE item(v INTEGER)`})
	requestSQL(t, m, 2, types.SQLCommand{RequestID: "result", SQL: `INSERT INTO item VALUES(1) RETURNING v`, WantRows: true})
	before := m.sqlReceiptRetention.resultBytes
	if before == 0 {
		t.Fatal("expected retained SQL result bytes")
	}
	_, err = m.writer.Exec(`INSERT INTO _rhiza_idempotency(kind, request_id, fingerprint, commit_slot, status, error_code, rows_affected, last_insert_id, applied, sql_result) VALUES (?, ?, zeroblob(32), 2, ?, ?, 0, 0, 0, X'')`, types.MutationSQL, "full", types.MutationRejected, types.MutationErrorCodeResultRetentionFull)
	if err != nil {
		t.Fatal(err)
	}
	m.sqlReceiptRetention.receipts++
	m.sqlRetentionPlan = sqlRetentionBarrierPlan{}
	maxBytes, headroom := before+2, uint64(3)
	if target, err := m.sqlRetentionBarrierTarget(context.Background(), maxBytes, headroom); err != nil || target != 1026 {
		t.Fatalf("incorrect retention target: target=%d err=%v", target, err)
	}
	barrier := types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{})
	controls := make([]quepaxa.DecidedValue, 0, 1024)
	for slot := 3; slot <= 1026; slot++ {
		controls = append(controls, quepaxa.DecidedValue{Slot: quepaxa.Slot(slot), Value: barrier})
	}
	if err := m.ApplyBatch(context.Background(), controls); err != nil {
		t.Fatal(err)
	}
	if target, err := m.sqlRetentionBarrierTarget(context.Background(), maxBytes, headroom); err != nil || target != 0 {
		t.Fatalf("barriers continued after expiry: target=%d err=%v", target, err)
	}
	if got := m.sqlReceiptRetention.resultBytes; got != 0 {
		t.Fatalf("control slots did not reclaim expired result bytes: got=%d", got)
	}
}
