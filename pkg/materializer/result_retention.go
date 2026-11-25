package materializer

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strconv"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
)

// SQLRetentionBarrierTarget reports the first applied slot at which a maximal
// SQL result can fit after expired receipts are pruned by an application value.
// It does not modify receipts or authorize a retry of a rejected request.
func (m *Materializer) SQLRetentionBarrierTarget(ctx context.Context) (uint64, error) {
	return m.sqlRetentionBarrierTarget(ctx, maxRetainedSQLResultBytes, MaxMutationResultBytes)
}

func (m *Materializer) sqlRetentionBarrierTarget(ctx context.Context, maxBytes, headroom uint64) (target uint64, planErr error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.writer == nil {
		return 0, sql.ErrConnDone
	}
	if err := m.fatalHealthLocked(); err != nil {
		return 0, err
	}
	if headroom == 0 || headroom > maxBytes {
		return 0, fmt.Errorf("invalid SQL result retention headroom")
	}
	if p := m.sqlRetentionPlan; p.valid && p.stateTip == m.stateTip && p.max == maxBytes && p.headroom == headroom && m.tip <= p.validThrough && (p.target == 0 || m.tip < p.target) {
		return p.target, nil
	}
	planValidThrough := uint64(math.MaxUint64)
	defer func() {
		if planErr == nil {
			m.sqlRetentionPlan = sqlRetentionBarrierPlan{valid: true, stateTip: m.stateTip, max: maxBytes, headroom: headroom, target: target, validThrough: planValidThrough}
		}
	}()
	if m.sqlReceiptRetention.resultBytes <= maxBytes-headroom {
		return 0, nil
	}
	var floor uint64
	if m.tip >= m.idempotencyWindow {
		floor = m.tip - m.idempotencyWindow + 1
	}
	var latestRejection sql.NullInt64
	if err := m.writer.QueryRowContext(ctx, `SELECT MAX(commit_slot) FROM _rhiza_idempotency WHERE kind = ? AND error_code = ? AND commit_slot >= ?`, types.MutationSQL, types.MutationErrorCodeResultRetentionFull, floor).Scan(&latestRejection); err != nil {
		return 0, fmt.Errorf("inspect SQL result retention pressure: %w", err)
	}
	if !latestRejection.Valid {
		return 0, nil
	}
	if latestRejection.Int64 < 0 || uint64(latestRejection.Int64) > math.MaxUint64-(m.idempotencyWindow-1) {
		return 0, fmt.Errorf("SQL result retention rejection slot overflow")
	}
	planValidThrough = uint64(latestRejection.Int64) + m.idempotencyWindow - 1
	var live int64
	if err := m.writer.QueryRowContext(ctx, `SELECT COALESCE(SUM(length(sql_result)), 0) FROM _rhiza_idempotency WHERE kind = ? AND commit_slot >= ?`, types.MutationSQL, floor).Scan(&live); err != nil {
		return 0, fmt.Errorf("count live SQL result bytes: %w", err)
	}
	if live < 0 || uint64(live) > maxBytes {
		return 0, fmt.Errorf("SQL result retention accounting exceeds limit")
	}
	if uint64(live) <= maxBytes-headroom {
		return 0, nil
	}
	needed := uint64(live) - (maxBytes - headroom)
	rows, err := m.writer.QueryContext(ctx, `SELECT commit_slot, length(sql_result) FROM _rhiza_idempotency WHERE kind = ? AND commit_slot >= ? AND length(sql_result) > 0 ORDER BY commit_slot`, types.MutationSQL, floor)
	if err != nil {
		return 0, fmt.Errorf("inspect live SQL results: %w", err)
	}
	defer rows.Close()
	var reclaimed uint64
	for rows.Next() {
		var slot, size uint64
		if err := rows.Scan(&slot, &size); err != nil {
			return 0, fmt.Errorf("inspect live SQL result: %w", err)
		}
		if size > maxBytes-reclaimed {
			return 0, fmt.Errorf("SQL result retention accounting exceeds limit")
		}
		reclaimed += size
		if reclaimed >= needed {
			if slot > math.MaxUint64-m.idempotencyWindow {
				return 0, fmt.Errorf("SQL result retention slot overflow")
			}
			return slot + m.idempotencyWindow, nil
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate live SQL results: %w", err)
	}
	return 0, fmt.Errorf("unable to calculate SQL result retention target")
}

const (
	maxRetainedSQLResultBytes = 256 << 20
	// Deterministic receipt charge; this is not a physical SQLite size guarantee.
	maxSQLReceiptEnvelopeBytes = 256
)

type sqlRetentionBarrierPlan struct {
	valid                           bool
	stateTip, max, headroom, target uint64
	validThrough                    uint64
}

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
