package materializer

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestDurableSQLResultRetentionQuotaRestartAndExpiry(t *testing.T) {
	const window = 1024
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "retention.db")
	m, err := Open(path, 1, window)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.Close() }()

	const payloadBytes = MaxMutationResultBytes - 56
	result, err := encodeSQLResult(types.SQLCommandResult{Statements: []types.SQLStatementResult{{
		RowsAffected: 1,
		Columns:      []string{"payload"},
		Rows:         [][]any{{make([]byte, payloadBytes)}},
	}}})
	if err != nil || len(result) != MaxMutationResultBytes {
		t.Fatalf("fixture result bytes=%d err=%v", len(result), err)
	}
	setup, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "setup", Statements: []types.SQLStatement{
		{SQL: "CREATE TABLE retained (count INTEGER NOT NULL, payload BLOB NOT NULL)"},
		{SQL: "INSERT INTO retained VALUES (0, zeroblob(?))", Args: []any{payloadBytes}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 1, setup); err != nil {
		t.Fatal(err)
	}

	command := func(id string) types.SQLCommand {
		return types.SQLCommand{RequestID: id, Statements: []types.SQLStatement{{
			SQL:      "UPDATE retained SET count=count+1, payload=payload RETURNING payload",
			WantRows: true,
		}}}
	}
	startTime := time.Now()
	start := runtime.MemStats{}
	runtime.ReadMemStats(&start)
	for batch := 0; batch < maxRetainedSQLResultBytes/MaxMutationResultBytes/types.MaxSQLCommandsPerDecidedValue; batch++ {
		commands := make([]types.SQLCommand, types.MaxSQLCommandsPerDecidedValue)
		for i := range commands {
			commands[i] = command("retained-" + strconv.Itoa(batch*types.MaxSQLCommandsPerDecidedValue+i))
		}
		value, err := types.EncodeSQLBatch(commands)
		if err != nil {
			t.Fatal(err)
		}
		if err := m.Apply(ctx, uint64(batch+2), value); err != nil {
			t.Fatalf("apply full batch %d: %v", batch, err)
		}
	}
	if got, want := m.sqlReceiptRetention.resultBytes, uint64(maxRetainedSQLResultBytes); got != want {
		t.Fatalf("retained result bytes=%d want=%d", got, want)
	}
	var pageCount, freePages int64
	if err := m.writer.QueryRow(`PRAGMA page_count`).Scan(&pageCount); err != nil {
		t.Fatal(err)
	}
	if err := m.writer.QueryRow(`PRAGMA freelist_count`).Scan(&freePages); err != nil {
		t.Fatal(err)
	}
	dbInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	walInfo, walErr := os.Stat(path + "-wal")
	var walBytes int64
	if walErr == nil {
		walBytes = walInfo.Size()
	} else if !os.IsNotExist(walErr) {
		t.Fatal(walErr)
	}
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	t.Logf("retention saturation: blob=%d db=%d pages=%d freelist=%d wal=%d alloc_delta=%d apply_elapsed=%s", m.sqlReceiptRetention.resultBytes, dbInfo.Size(), pageCount, freePages, walBytes, after.TotalAlloc-start.TotalAlloc, time.Since(startTime))

	files, _, cleanup, err := m.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	var snapshotBytes int64
	for _, file := range files {
		if file.Role == CheckpointSQLite {
			info, err := os.Stat(file.Path)
			if err != nil {
				t.Fatal(err)
			}
			snapshotBytes = info.Size()
		}
	}
	if snapshotBytes < maxRetainedSQLResultBytes {
		t.Fatalf("checkpoint SQLite snapshot bytes=%d below retained result bytes", snapshotBytes)
	}
	t.Logf("retention checkpoint SQLite snapshot=%d bytes (32 GiB checkpoint ceiling unchanged)", snapshotBytes)
	if err := m.RestoreCheckpoint(ctx, files); err != nil {
		t.Fatalf("restore bounded retention checkpoint: %v", err)
	}
	if m.sqlReceiptRetention.resultBytes != maxRetainedSQLResultBytes {
		t.Fatalf("restored result bytes=%d want=%d", m.sqlReceiptRetention.resultBytes, maxRetainedSQLResultBytes)
	}

	full := command("retention-full")
	advance := types.SQLCommand{RequestID: "no-result-at-cap", SQL: "UPDATE retained SET count=count+1"}
	lastValue, err := types.EncodeSQLBatch([]types.SQLCommand{full, advance})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 6, lastValue); err != nil {
		t.Fatal(err)
	}
	var persisted int
	if err := m.writer.QueryRow(`SELECT COUNT(*) FROM _rhiza_idempotency WHERE kind = ? AND request_id = ?`, types.MutationSQL, full.RequestID).Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	t.Logf("retention-full persisted=%d cache=%v bloom=%v counters=%+v", persisted, m.recentSQLReceipts[full.RequestID], m.sqlReceipts.mightContain(full.RequestID, m.tip), m.sqlReceiptRetention)
	fingerprint, _ := types.SQLFingerprint(full)
	receipt, returned, found, matches, err := m.SQLRequestResultFingerprint(ctx, full.RequestID, fingerprint, true)
	if err != nil || !found || !matches || receipt.Status != types.MutationRejected || receipt.ErrorCode != types.MutationErrorCodeResultRetentionFull || len(returned.Statements) != 0 {
		t.Fatalf("full receipt=%+v result=%+v found=%v matches=%v err=%v", receipt, returned, found, matches, err)
	}
	if got, err := m.QueryResult(ctx, "SELECT count FROM retained", nil); err != nil || got.Rows[0][0] != int64(maxRetainedSQLResultBytes/MaxMutationResultBytes+1) {
		t.Fatalf("rejected mutation rollback/progress result=%v err=%v", got.Rows, err)
	}
	beforeRetryUsage := m.sqlReceiptRetention
	if err := m.Apply(ctx, 7, lastValue); err != nil {
		t.Fatal(err)
	}
	if m.sqlReceiptRetention != beforeRetryUsage {
		t.Fatalf("exact retry changed retention counters: before=%+v after=%+v", beforeRetryUsage, m.sqlReceiptRetention)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m, err = Open(path, 1, window)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.Close() }()
	_, _, found, matches, err = m.SQLRequestResultFingerprint(ctx, full.RequestID, fingerprint, true)
	if err != nil || !found || !matches {
		t.Fatalf("reopened retention-full retry found=%v matches=%v err=%v", found, matches, err)
	}

	barrier := types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{})
	decisions := make([]quepaxa.DecidedValue, 0, 1020)
	for slot := uint64(8); slot <= 1026; slot++ {
		decisions = append(decisions, quepaxa.DecidedValue{Slot: quepaxa.Slot(slot), Value: barrier})
	}
	if err := m.ApplyBatch(ctx, decisions); err != nil {
		t.Fatalf("advance through retry-window expiry: %v", err)
	}
	if got, want := m.sqlReceiptRetention.resultBytes, uint64(3*maxRetainedSQLResultBytes/4); got != want {
		t.Fatalf("expired result bytes=%d want=%d", got, want)
	}
	if _, _, found, _, err := m.SQLRequestResultFingerprint(ctx, "retained-0", [32]byte{}, true); err != nil || found {
		t.Fatalf("expired receipt found=%v err=%v", found, err)
	}
	afterExpiry, err := types.EncodeSQLBatch([]types.SQLCommand{command("after-expiry")})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 1027, afterExpiry); err != nil {
		t.Fatal(err)
	}
	newFingerprint, _ := types.SQLFingerprint(command("after-expiry"))
	newReceipt, _, found, matches, err := m.SQLRequestResultFingerprint(ctx, "after-expiry", newFingerprint, true)
	if err != nil || !found || !matches || newReceipt.Status != types.MutationCommitted {
		t.Fatalf("post-expiry result receipt=%+v found=%v matches=%v err=%v", newReceipt, found, matches, err)
	}
}
