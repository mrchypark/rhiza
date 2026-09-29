package materializer

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestSQLReceiptRetentionUsageBounds(t *testing.T) {
	var usage sqlReceiptRetentionUsage
	if !usage.admits(maxRetainedSQLResultBytes, 1) {
		t.Fatal("one maximum-size result should fit")
	}
	usage.add(maxRetainedSQLResultBytes)
	if usage.admits(1, 1) {
		t.Fatal("result byte budget exceeded")
	}
	if got, want := usage.chargedBytes(), uint64(maxRetainedSQLResultBytes+maxSQLReceiptEnvelopeBytes); got != want {
		t.Fatalf("charged bytes=%d want=%d", got, want)
	}

	usage = sqlReceiptRetentionUsage{}
	for range types.MaxSQLCommandsPerDecidedValue {
		if !usage.admits(0, 1) {
			t.Fatal("receipt envelope budget filled before one slot")
		}
		usage.add(0)
	}
	if usage.admits(0, 1) {
		t.Fatal("receipt envelope budget exceeded")
	}
	if got, want := usage.chargedBytes(), uint64(types.MaxSQLCommandsPerDecidedValue)*maxSQLReceiptEnvelopeBytes; got != want {
		t.Fatalf("charged bytes=%d want=%d", got, want)
	}
	if err := usage.remove(0, uint64(types.MaxSQLCommandsPerDecidedValue)); err != nil || usage != (sqlReceiptRetentionUsage{}) {
		t.Fatalf("remove receipts usage=%+v err=%v", usage, err)
	}
	if err := usage.remove(1, 1); err == nil {
		t.Fatal("counter underflow accepted")
	}
	if (sqlReceiptRetentionUsage{resultBytes: maxRetainedSQLResultBytes + 1}).valid(1) {
		t.Fatal("corrupt byte counter accepted")
	}
}

func TestSQLReceiptRetentionUsageAdoptedAndExpires(t *testing.T) {
	ctx := context.Background()
	const window = 1024
	source, err := Open(filepath.Join(t.TempDir(), "source.db"), 1, window)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "retained", SQL: "CREATE TABLE retained(id INTEGER)"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Apply(ctx, 1, value); err != nil {
		t.Fatal(err)
	}
	files, _, cleanup, err := source.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	target, err := Open(filepath.Join(t.TempDir(), "target.db"), 1, window)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	if err := target.RestoreCheckpoint(ctx, files); err != nil {
		t.Fatal(err)
	}
	if want := (sqlReceiptRetentionUsage{receipts: 1}); target.sqlReceiptRetention != want {
		t.Fatalf("adopted retention usage=%+v want=%+v", target.sqlReceiptRetention, want)
	}
	barrier := types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{})
	decisions := make([]quepaxa.DecidedValue, 0, window)
	for slot := uint64(2); slot <= window+1; slot++ {
		decisions = append(decisions, quepaxa.DecidedValue{Slot: quepaxa.Slot(slot), Value: barrier})
	}
	if err := target.ApplyBatch(ctx, decisions); err != nil {
		t.Fatalf("expire restored receipt: %v", err)
	}
	if target.sqlReceiptRetention != (sqlReceiptRetentionUsage{}) {
		t.Fatalf("expired retention usage=%+v want zero", target.sqlReceiptRetention)
	}
}

func TestSQLReceiptRetentionEnvelopeChargeBound(t *testing.T) {
	for _, window := range []uint64{types.DefaultIdempotencyWindowSlots, 1_048_576} {
		usage := sqlReceiptRetentionUsage{
			resultBytes: maxRetainedSQLResultBytes,
			receipts:    uint64(types.MaxSQLCommandsPerDecidedValue) * window,
		}
		want := uint64(maxRetainedSQLResultBytes) + uint64(maxSQLReceiptEnvelopeBytes*types.MaxSQLCommandsPerDecidedValue)*window
		if !usage.valid(window) || usage.chargedBytes() != want {
			t.Fatalf("window=%d charge=%d want=%d valid=%v", window, usage.chargedBytes(), want, usage.valid(window))
		}
	}
}

func TestSQLReceiptRetentionCountersAndWindowAreRequired(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retention.db")
	m, err := Open(path, 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.db.Exec(`UPDATE _rhiza_meta SET value = '1' WHERE key = 'sql_receipt_count'`); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path, 1, 1024); err == nil {
		t.Fatal("mismatched retention counter accepted")
	}
	if _, err := Open(path, 1); !errors.Is(err, sqlpolicy.ErrIncompatible) {
		t.Fatalf("window mismatch error=%v", err)
	}
}
