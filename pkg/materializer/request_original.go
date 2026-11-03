package materializer

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

const requestOriginalDDL = `CREATE TABLE IF NOT EXISTS _rhiza_request_original (
 kind INTEGER NOT NULL, request_id BLOB NOT NULL, commit_slot INTEGER NOT NULL,
 batch_index INTEGER NOT NULL, execution BLOB NOT NULL, PRIMARY KEY(kind, request_id)
) WITHOUT ROWID`

// RequestOriginal is a retained, applied result, not pre-certification acceptance
// or permission to resubmit. All returned payloads are owned by the caller.
type RequestOriginal struct {
	Kind                             types.MutationKind
	RequestID                        string
	Slot                             uint64
	BatchIndex                       uint32
	Fingerprint                      [32]byte
	CanonicalOriginal, ExecutionItem []byte
	SQL                              *types.SQLCommand
	KV                               *types.KVCommand
	Graph                            *types.GraphCommand
	DecisionHash                     [32]byte
	Receipt                          types.MutationReceipt
	SQLResult                        types.SQLCommandResult
}

// OpenManagedSQLKVWithOriginals selects dormant profile 9 explicitly. It never
// upgrades existing profile 7 or 8 state.
func OpenManagedSQLKVWithOriginals(path string, readers int, window ...uint64) (*Materializer, error) {
	return openWithProtocol(path, readers, sqlpolicy.ManagedRequestApplicationProtocol, window...)
}

func insertRequestOriginal(ctx context.Context, tx *sql.Tx, kind types.MutationKind, id string, slot uint64, index uint32, execution []byte) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO _rhiza_request_original(kind,request_id,commit_slot,batch_index,execution) VALUES(?,?,?,?,?)`, kind, id, slot, index, execution); err != nil {
		return err
	}
	return adjustOriginalBytes(ctx, tx, int64(len(execution)))
}

func adjustOriginalBytes(ctx context.Context, tx *sql.Tx, delta int64) error {
	var raw string
	if err := tx.QueryRowContext(ctx, `SELECT value FROM _rhiza_meta WHERE key='request_original_bytes'`).Scan(&raw); err != nil {
		return err
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < 0 || (delta < 0 && n < -delta) || (delta > 0 && n > math.MaxInt64-delta) {
		return fmt.Errorf("invalid retained original byte accounting")
	}
	_, err = tx.ExecContext(ctx, `UPDATE _rhiza_meta SET value=? WHERE key='request_original_bytes'`, strconv.FormatInt(n+delta, 10))
	return err
}

const originalJoin = `SELECT o.kind,o.request_id,o.commit_slot,o.batch_index,o.execution,r.fingerprint,r.commit_slot,r.status,r.error_code,r.rows_affected,r.last_insert_id,r.applied,r.sql_result FROM _rhiza_request_original o LEFT JOIN _rhiza_idempotency r ON r.kind=o.kind AND r.request_id=o.request_id`

func scanRequestOriginal(scanner interface{ Scan(...any) error }, tip, window uint64) (RequestOriginal, error) {
	var out RequestOriginal
	var kind, index, applied int64
	var fingerprint, result []byte
	if err := scanner.Scan(&kind, &out.RequestID, &out.Slot, &index, &out.ExecutionItem, &fingerprint, &out.Receipt.Slot, &out.Receipt.Status, &out.Receipt.ErrorCode, &out.Receipt.RowsAffected, &out.Receipt.LastInsertID, &applied, &result); err != nil {
		return out, err
	}
	if (kind != int64(types.MutationSQL) && kind != int64(types.MutationKV)) || len(out.RequestID) == 0 || len(out.RequestID) > types.MaxRequestIDBytes || out.Slot == 0 || out.Slot > tip || out.Slot != out.Receipt.Slot || out.Slot > math.MaxUint64-window+1 || index < 0 || index >= 64 || len(out.ExecutionItem) == 0 || len(out.ExecutionItem) > quepaxa.MaxReplicatedValueBytes || len(fingerprint) != 32 || (applied != 0 && applied != 1) {
		return RequestOriginal{}, fmt.Errorf("invalid retained original envelope")
	}
	out.Kind, out.BatchIndex = types.MutationKind(kind), uint32(index)
	copy(out.Fingerprint[:], fingerprint)
	out.Receipt.Applied = applied == 1
	out.Receipt.RetryThroughSlot = out.Slot + window - 1
	var canonical []byte
	var actual [32]byte
	var err error
	if out.Kind == types.MutationSQL {
		commands, ok, decodeErr := types.DecodeSQLBatch(types.AssembleSQLBatch([][]byte{out.ExecutionItem}))
		if decodeErr != nil || !ok || len(commands) != 1 || commands[0].Migration != nil {
			return RequestOriginal{}, fmt.Errorf("invalid retained SQL original")
		}
		command := commands[0]
		if err := ValidateSQLCommand(command); err != nil {
			return RequestOriginal{}, err
		}
		if command.RequestID != out.RequestID || applied != 0 || (out.Receipt.Status != types.MutationCommitted && out.Receipt.Status != types.MutationRejected) || (out.Receipt.Status == types.MutationCommitted && out.Receipt.ErrorCode != "") {
			return RequestOriginal{}, fmt.Errorf("invalid retained SQL result")
		}
		if out.Receipt.Status == types.MutationRejected && (out.Receipt.RowsAffected != 0 || out.Receipt.LastInsertID != 0 || len(result) != 0 || (out.Receipt.ErrorCode != types.MutationErrorCodeExecutionFailed && out.Receipt.ErrorCode != types.MutationErrorCodePreconditionFailed && out.Receipt.ErrorCode != types.MutationErrorCodeResultRetentionFull)) {
			return RequestOriginal{}, fmt.Errorf("invalid rejected SQL original")
		}
		canonical, err = types.EncodeSQLBatchItem(command)
		if err == nil {
			actual, err = types.SQLFingerprint(command)
		}
		out.SQL = &command
		out.CanonicalOriginal = bytes.Clone(canonical)
		if err == nil && len(result) != 0 {
			out.SQLResult, err = decodeSQLResult(result)
			wantRows := command.WantRows
			for _, statement := range command.Statements {
				wantRows = wantRows || statement.WantRows
			}
			if !wantRows || len(out.SQLResult.Statements) != max(1, len(command.Statements)) {
				return RequestOriginal{}, fmt.Errorf("retained SQL result differs from original request")
			}
			var affected, last int64
			for _, statement := range out.SQLResult.Statements {
				affected += statement.RowsAffected
				last = statement.LastInsertID
			}
			if affected != out.Receipt.RowsAffected || last != out.Receipt.LastInsertID {
				return RequestOriginal{}, fmt.Errorf("retained SQL result differs from receipt")
			}
		}
	} else {
		value, encodeErr := types.AssembleManagedKVBatch([][]byte{out.ExecutionItem})
		if encodeErr != nil {
			return RequestOriginal{}, encodeErr
		}
		commands, ok, decodeErr := types.DecodeManagedKVBatch(value)
		if decodeErr != nil || !ok || len(commands) != 1 {
			return RequestOriginal{}, fmt.Errorf("invalid retained KV original")
		}
		command := commands[0]
		if command.RequestID != out.RequestID || out.Receipt.Status != types.MutationCommitted || out.Receipt.ErrorCode != "" || out.Receipt.RowsAffected != 0 || out.Receipt.LastInsertID != 0 || len(result) != 0 {
			return RequestOriginal{}, fmt.Errorf("invalid retained KV result")
		}
		canonical, err = types.EncodeManagedKVBatchItem(command)
		if err == nil {
			actual, err = types.ManagedKVFingerprint(command)
		}
		if err == nil {
			out.CanonicalOriginal, _, err = types.EncodeManagedKVOriginal(command)
		}
		out.KV = &command
	}
	if err != nil {
		return RequestOriginal{}, err
	}
	if actual != out.Fingerprint || !bytes.Equal(canonical, out.ExecutionItem) {
		return RequestOriginal{}, fmt.Errorf("retained original differs from receipt or canonical execution")
	}
	return out, nil
}

// RetainedRequestOriginal observes only profile 9's applied projection. A false
// result is unknown/expired, never proof that a request had no effect.
func (m *Materializer) RetainedRequestOriginal(ctx context.Context, kind types.MutationKind, rawID string) (RequestOriginal, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !originalsPolicy(m.materializationPolicy) {
		return RequestOriginal{}, false, sqlpolicy.ErrIncompatible
	}
	if m.writer == nil || len(m.readers) == 0 {
		return RequestOriginal{}, false, sql.ErrConnDone
	}
	if err := m.sqlReceiptReadLocked(); err != nil {
		return RequestOriginal{}, false, err
	}
	if graphProtocol(m.managedProtocol) && kind == types.MutationGraph {
		return m.retainedGraphOriginalLocked(ctx, rawID)
	}
	if (kind != types.MutationSQL && kind != types.MutationKV) || rawID == "" || len(rawID) > types.MaxRequestIDBytes {
		return RequestOriginal{}, false, fmt.Errorf("invalid original lookup")
	}
	tx, err := m.readers[0].BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return RequestOriginal{}, false, err
	}
	defer tx.Rollback()
	if err := checkManagedPolicy(ctx, tx, m.managedProtocol, m.idempotencyWindow); err != nil {
		return RequestOriginal{}, false, err
	}
	out, err := scanRequestOriginal(tx.QueryRowContext(ctx, originalJoin+` WHERE o.kind=? AND o.request_id=?`, kind, rawID), m.tip, m.idempotencyWindow)
	if errors.Is(err, sql.ErrNoRows) {
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM _rhiza_idempotency WHERE kind=? AND request_id=?`, kind, rawID).Scan(&count); err != nil {
			return RequestOriginal{}, false, err
		}
		if count != 0 {
			return RequestOriginal{}, false, fmt.Errorf("retained receipt lacks original")
		}
		return RequestOriginal{}, false, nil
	}
	if err != nil {
		return RequestOriginal{}, false, err
	}
	if m.tip > out.Receipt.RetryThroughSlot {
		return RequestOriginal{}, false, nil
	}
	return out, true, nil
}

func validateRequestOriginals(ctx context.Context, tx *sql.Tx, tip, window uint64) error {
	var receipts, originals int64
	if err := tx.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM _rhiza_idempotency),(SELECT COUNT(*) FROM _rhiza_request_original)`).Scan(&receipts, &originals); err != nil {
		return err
	}
	if receipts != originals || originals < 0 || uint64(originals) > uint64(types.MaxSQLCommandsPerDecidedValue)*window {
		return fmt.Errorf("retained originals and receipts differ")
	}
	var raw string
	var actual int64
	if err := tx.QueryRowContext(ctx, `SELECT (SELECT value FROM _rhiza_meta WHERE key='request_original_bytes'),COALESCE(SUM(length(execution)),0) FROM _rhiza_request_original`).Scan(&raw, &actual); err != nil {
		return err
	}
	charged, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || actual < 0 || charged != actual {
		return fmt.Errorf("invalid retained original byte accounting")
	}
	rows, err := tx.QueryContext(ctx, originalJoin)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if _, err := scanRequestOriginal(rows, tip, window); err != nil {
			return err
		}
	}
	return rows.Err()
}

// InspectManagedRequestFile validates a stable profile 9 file using the same
// complete evidence path as live inspection. It never upgrades source files.
func InspectManagedRequestFile(ctx context.Context, path string, window uint64) (ManagedApplicationState, uint64, error) {
	if window < 1024 || window > 1_048_576 {
		return ManagedApplicationState{}, 0, fmt.Errorf("invalid request receipt window")
	}
	if err := validateManagedArtifacts(path); err != nil {
		return ManagedApplicationState{}, 0, err
	}
	state, tip, err := inspectApplicationFile(ctx, path, window, sqlpolicy.ManagedRequestApplicationProtocol)
	return ManagedApplicationState(state), tip, err
}
