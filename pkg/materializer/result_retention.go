package materializer

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
)

const (
	maxRetainedSQLResultBytes = 256 << 20
	// Deterministic receipt charge; this is not a physical SQLite size guarantee.
	maxSQLReceiptEnvelopeBytes = 256
)

type sqlReceiptRetentionUsage struct {
	resultBytes uint64
	receipts    uint64
}

func (u sqlReceiptRetentionUsage) admits(resultBytes, window uint64) bool {
	return u.valid(window) &&
		u.receipts < uint64(types.MaxSQLCommandsPerDecidedValue)*window &&
		resultBytes <= maxRetainedSQLResultBytes-u.resultBytes
}

func (u *sqlReceiptRetentionUsage) add(resultBytes uint64) {
	u.resultBytes += resultBytes
	u.receipts++
}

func (u *sqlReceiptRetentionUsage) remove(resultBytes, receipts uint64) error {
	if resultBytes > u.resultBytes || receipts > u.receipts {
		return fmt.Errorf("SQL receipt retention counter underflow")
	}
	u.resultBytes -= resultBytes
	u.receipts -= receipts
	return nil
}

func (u sqlReceiptRetentionUsage) valid(window uint64) bool {
	return u.resultBytes <= maxRetainedSQLResultBytes &&
		u.receipts <= uint64(types.MaxSQLCommandsPerDecidedValue)*window
}

func (u sqlReceiptRetentionUsage) chargedBytes() uint64 {
	return u.resultBytes + u.receipts*maxSQLReceiptEnvelopeBytes
}

func (m *Materializer) loadSQLReceiptRetention() error {
	var windowText, bytesText, receiptsText string
	for _, counter := range []struct {
		key  string
		dest *string
	}{{"sql_idempotency_window", &windowText}, {"sql_receipt_result_bytes", &bytesText}, {"sql_receipt_count", &receiptsText}} {
		if err := m.writer.QueryRow(`SELECT value FROM _rhiza_meta WHERE key = ?`, counter.key).Scan(counter.dest); err != nil {
			return fmt.Errorf("load %s: %w", counter.key, err)
		}
	}
	window, err := strconv.ParseUint(windowText, 10, 64)
	if err != nil || window != m.idempotencyWindow {
		return fmt.Errorf("%w: SQL idempotency window does not match stored policy", sqlpolicy.ErrIncompatible)
	}
	resultBytes, err := strconv.ParseUint(bytesText, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid SQL receipt result byte counter")
	}
	receipts, err := strconv.ParseUint(receiptsText, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid SQL receipt count")
	}
	usage := sqlReceiptRetentionUsage{resultBytes: resultBytes, receipts: receipts}
	if !usage.valid(window) {
		return fmt.Errorf("SQL receipt retention counters exceed policy bounds")
	}
	var invalidEnvelopes int64
	if err := m.writer.QueryRow(`SELECT COUNT(*) FROM _rhiza_idempotency WHERE kind = ? AND (length(request_id) = 0 OR length(request_id) > ? OR length(fingerprint) != 32 OR length(error_code) > 64 OR status NOT IN (?, ?) OR (status = ? AND error_code != '') OR (status = ? AND error_code NOT IN (?, ?, ?)) OR (status = ? AND length(sql_result) != 0) OR applied NOT IN (0, 1) OR length(sql_result) > ?)`,
		types.MutationSQL, types.MaxRequestIDBytes, types.MutationCommitted, types.MutationRejected,
		types.MutationCommitted, types.MutationRejected,
		types.MutationErrorCodeExecutionFailed, types.MutationErrorCodePreconditionFailed,
		types.MutationErrorCodeResultRetentionFull, types.MutationRejected,
		MaxMutationResultBytes).Scan(&invalidEnvelopes); err != nil {
		return fmt.Errorf("validate SQL receipt envelopes: %w", err)
	}
	if invalidEnvelopes != 0 {
		return fmt.Errorf("stored SQL receipt envelope exceeds policy bounds")
	}
	var actualBytes, actualReceipts int64
	if err := m.writer.QueryRow(`SELECT COALESCE(SUM(length(sql_result)), 0), COUNT(*) FROM _rhiza_idempotency WHERE kind = ?`, types.MutationSQL).Scan(&actualBytes, &actualReceipts); err != nil {
		return fmt.Errorf("verify SQL receipt retention counters: %w", err)
	}
	if actualBytes < 0 || actualReceipts < 0 || uint64(actualBytes) != resultBytes || uint64(actualReceipts) != receipts {
		return fmt.Errorf("SQL receipt retention counters do not match stored receipts")
	}
	m.sqlReceiptRetention = usage
	return nil
}

func (m *Materializer) persistSQLReceiptRetention(ctx context.Context, tx *sql.Tx) error {
	if !m.sqlReceiptRetention.valid(m.idempotencyWindow) {
		return fmt.Errorf("SQL receipt retention counters exceed policy bounds")
	}
	for _, counter := range []struct{ key, value string }{
		{"sql_receipt_result_bytes", strconv.FormatUint(m.sqlReceiptRetention.resultBytes, 10)},
		{"sql_receipt_count", strconv.FormatUint(m.sqlReceiptRetention.receipts, 10)},
	} {
		result, err := tx.ExecContext(ctx, `UPDATE _rhiza_meta SET value = ? WHERE key = ?`, counter.value, counter.key)
		if err != nil {
			return fmt.Errorf("persist %s: %w", counter.key, err)
		}
		updated, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("verify %s update: %w", counter.key, err)
		}
		if updated != 1 {
			return fmt.Errorf("missing SQL receipt retention counter %s", counter.key)
		}
	}
	return nil
}
