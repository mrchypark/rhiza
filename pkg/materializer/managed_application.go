package materializer

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
)

const managedApplicationPolicy = "application-prune-raw-state/v1"

func policyForProtocol(protocol string) sqlpolicy.MaterializationPolicy {
	if graphProtocol(protocol) {
		return sqlpolicy.ManagedGraphRequestMaterializationPolicy
	}
	if protocol == sqlpolicy.ManagedRequestApplicationProtocol {
		return sqlpolicy.ManagedRequestMaterializationPolicy
	}
	if protocol == types.ManagedApplicationProtocolSQLKV {
		return sqlpolicy.ManagedSQLKVMaterializationPolicy
	}
	return sqlpolicy.LegacyMaterializationPolicy
}

// MaterializationPolicy is fixed by the constructor, never inferred from files.
func (m *Materializer) MaterializationPolicy() sqlpolicy.MaterializationPolicy {
	return m.materializationPolicy
}

func (m *Materializer) validateExecutionPolicy(value []byte) error {
	if graphProtocol(m.managedProtocol) {
		return types.ValidateManagedGraphExecutionPolicy(value)
	}
	if m.managedProtocol != "" {
		return types.ValidateManagedKVExecutionPolicy(value)
	}
	return types.ValidateExecutionPolicy(value)
}

func (m *Materializer) kvFingerprint(command types.KVCommand) ([32]byte, error) {
	if m.managedProtocol != "" {
		return types.ManagedKVFingerprint(command)
	}
	return types.KVFingerprint(command)
}

func validateManagedGraphAbsence(path string) error {
	for _, name := range []string{"latticedb", "latticedb.restore-backup"} {
		if _, err := os.Lstat(filepath.Join(filepath.Dir(path), name)); err == nil {
			return fmt.Errorf("mixed materialization rejects Graph artifacts")
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func validateManagedArtifacts(path string) error {
	return validateManagedArtifactsExcept(path, "")
}

// ValidateManagedRestoreState only inspects existing paths. It never repairs,
// opens a writer, or removes interrupted recovery evidence.
func ValidateManagedRestoreState(path string) error { return validateManagedArtifacts(path) }

func validateManagedArtifactsExcept(path, checkpointSource string) error {
	if err := validateManagedGraphAbsence(path); err != nil {
		return err
	}
	if err := ValidateCleanRestoreState(path); err != nil {
		return err
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if filepath.Join(filepath.Dir(path), name) == checkpointSource && strings.HasPrefix(name, ".rhiza-checkpoint-sqlite-") {
			continue
		}
		if name == filepath.Base(localRestoreJournalPath(path)) || strings.HasPrefix(name, ".rhiza-local-restore-") || strings.HasPrefix(name, ".rhiza-restore-") || strings.HasPrefix(name, ".rhiza-graph-restore-") || strings.HasPrefix(name, ".rhiza-checkpoint-") {
			return fmt.Errorf("mixed materialization found interrupted restore/checkpoint artifact")
		}
	}
	return nil
}

func (m *Materializer) managedKVGetAt(ctx context.Context, key string, now time.Time) ([]byte, bool, uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.sqlReceiptReadLocked(); err != nil {
		return nil, false, 0, err
	}
	if len(m.readers) == 0 {
		return nil, false, 0, sql.ErrConnDone
	}
	var slotText string
	var found bool
	var value []byte
	err := m.readers[0].QueryRowContext(ctx, `SELECT meta.value, kv.key IS NOT NULL, kv.value FROM _rhiza_meta meta LEFT JOIN _rhiza_kv kv ON kv.key=? AND (kv.expires_at_unix_ms=0 OR kv.expires_at_unix_ms>?) WHERE meta.key='applied_slot'`, key, now.UnixMilli()).Scan(&slotText, &found, &value)
	if err != nil {
		return nil, false, 0, err
	}
	slot, err := strconv.ParseUint(slotText, 10, 64)
	return value, found, slot, err
}

func (m *Materializer) managedCheckpointFilesAt(ctx context.Context) ([]CheckpointFile, uint64, func(), error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.sqlReceiptReadLocked(); err != nil {
		return nil, 0, nil, err
	}
	if err := validateManagedGraphAbsence(m.dbPath); err != nil {
		return nil, 0, nil, err
	}
	snapshot, err := m.beginSQLiteSnapshotLocked(ctx)
	if err != nil {
		return nil, 0, nil, err
	}
	defer snapshot.Close()
	file, err := os.CreateTemp(filepath.Dir(m.dbPath), ".rhiza-checkpoint-sqlite-*")
	if err != nil {
		return nil, 0, nil, err
	}
	path := file.Name()
	cleanup := func() { _ = os.Remove(path) }
	if err := file.Close(); err != nil {
		cleanup()
		return nil, 0, nil, err
	}
	if err := os.Remove(path); err != nil {
		cleanup()
		return nil, 0, nil, err
	}
	if err := snapshot.Backup(path); err != nil {
		cleanup()
		return nil, 0, nil, err
	}
	if err := ctx.Err(); err != nil {
		cleanup()
		return nil, 0, nil, err
	}
	if err := snapshot.Close(); err != nil {
		cleanup()
		return nil, 0, nil, err
	}
	if m.materializationPolicy == sqlpolicy.ManagedRequestMaterializationPolicy {
		if _, _, err := inspectApplicationFile(ctx, path, m.idempotencyWindow, m.managedProtocol); err != nil {
			cleanup()
			return nil, 0, nil, err
		}
	}
	return []CheckpointFile{{Role: CheckpointSQLite, Path: path}}, snapshot.index, cleanup, nil
}

func (m *Materializer) restoreManagedCheckpoint(ctx context.Context, files []CheckpointFile) error {
	if len(files) != 1 || files[0].Role != CheckpointSQLite || files[0].Path == "" {
		return fmt.Errorf("mixed checkpoint requires exactly one SQLite source")
	}
	if err := validateManagedArtifactsExcept(m.dbPath, files[0].Path); err != nil {
		return err
	}
	if err := validateManagedArtifactsExcept(files[0].Path, files[0].Path); err != nil {
		return err
	}
	if err := sqlpolicy.CheckSnapshotFileForPolicy(ctx, files[0].Path, m.materializationPolicy); err != nil {
		return err
	}
	if _, _, err := inspectApplicationFile(ctx, files[0].Path, m.idempotencyWindow, m.managedProtocol); err != nil {
		return err
	}
	return m.restoreParts(ctx, snapshotParts{sqlitePath: files[0].Path})
}

type ManagedApplicationState struct {
	Digest     [32]byte
	FreshEmpty bool
}

// OpenManagedSQLKV selects a creation-time policy, never an in-place migration.
func OpenManagedSQLKV(dbPath string, readerCount int, idempotencyWindow ...uint64) (*Materializer, error) {
	return openWithProtocol(dbPath, readerCount, types.ManagedApplicationProtocolSQLKV, idempotencyWindow...)
}

// KVRequestStatusFingerprint observes a receipt, fingerprint and logical retry
// window under one apply guard. It does not certify history or prove durability.
func (m *Materializer) KVRequestStatusFingerprint(ctx context.Context, requestID string, fingerprint [32]byte) (types.MutationReceipt, bool, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.sqlReceiptReadLocked(); err != nil {
		return types.MutationReceipt{}, false, false, err
	}
	if len(m.readers) == 0 || m.readers[0] == nil {
		return types.MutationReceipt{}, false, false, sql.ErrConnDone
	}
	var stored, result []byte
	var slot uint64
	var status types.MutationStatus
	var code string
	var affected, last int64
	var applied int
	err := m.readers[0].QueryRowContext(ctx, receiptQuery(), types.MutationKV, requestID).Scan(&stored, &slot, &status, &code, &affected, &last, &applied, &result)
	if errors.Is(err, sql.ErrNoRows) {
		return types.MutationReceipt{}, false, true, nil
	}
	if err != nil {
		return types.MutationReceipt{}, false, false, err
	}
	if len(stored) != 32 || slot == 0 || slot > m.tip || slot > math.MaxUint64-m.idempotencyWindow+1 || status != types.MutationCommitted || code != "" || affected != 0 || last != 0 || (applied != 0 && applied != 1) || len(result) != 0 {
		return types.MutationReceipt{}, false, false, fmt.Errorf("invalid retained KV receipt")
	}
	retryThrough := slot + m.idempotencyWindow - 1
	if m.tip > retryThrough {
		return types.MutationReceipt{}, false, true, nil
	}
	receipt := types.MutationReceipt{Slot: slot, Status: status, Applied: applied == 1, RetryThroughSlot: retryThrough}
	return receipt, true, bytes.Equal(stored, fingerprint[:]), nil
}

type policyReader interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func checkManagedPolicy(ctx context.Context, reader policyReader, protocol string, window uint64) error {
	var stored string
	err := reader.QueryRowContext(ctx, `SELECT value FROM _rhiza_meta WHERE key='managed_application_protocol'`).Scan(&stored)
	if protocol == "" {
		if errors.Is(err, sql.ErrNoRows) {
			var count int
			if err := reader.QueryRowContext(ctx, `SELECT COUNT(*) FROM _rhiza_meta WHERE key='managed_application_policy'`).Scan(&count); err != nil {
				return err
			}
			if count != 0 {
				return fmt.Errorf("orphan mixed materialization policy")
			}
			return nil
		}
		if err != nil {
			return err
		}
		return fmt.Errorf("managed application policy requires explicit mixed open")
	}
	if err != nil || (protocol != types.ManagedApplicationProtocolSQLKV && !originalsProtocol(protocol)) || stored != protocol {
		return fmt.Errorf("missing or incompatible managed application protocol")
	}
	for _, item := range []struct{ key, want string }{
		{"managed_application_policy", managedApplicationPolicy},
		{"sql_execution_policy", strconv.Itoa(int(policyForProtocol(protocol)))},
		{"sql_idempotency_window", strconv.FormatUint(window, 10)},
	} {
		if err := reader.QueryRowContext(ctx, `SELECT value FROM _rhiza_meta WHERE key=?`, item.key).Scan(&stored); err != nil || stored != item.want {
			return fmt.Errorf("missing or incompatible managed materialization policy %s", item.key)
		}
	}
	return nil
}

func checkManagedPolicyFile(ctx context.Context, path, protocol string, windows ...uint64) (resultErr error) {
	window := uint64(types.DefaultIdempotencyWindowSlots)
	if len(windows) > 0 {
		window = windows[0]
	}
	dsn := (&url.URL{Scheme: "file", Path: filepath.ToSlash(path)}).String() + "?mode=ro"
	db, err := openSQLite(dsn, false)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, db.Close()) }()
	return checkManagedPolicy(ctx, db, protocol, window)
}

func checkManagedPolicySnapshot(ctx context.Context, path, protocol string, window uint64) (resultErr error) {
	dsn := (&url.URL{Scheme: "file", Path: filepath.ToSlash(path)}).String() + "?mode=ro&immutable=1"
	db, err := openSQLite(dsn, false)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, db.Close()) }()
	return checkManagedPolicy(ctx, db, protocol, window)
}

func managedApplicationValue(value []byte) (bool, error) {
	if _, ok, err := types.DecodeGraphBatch(value); err != nil {
		return false, err
	} else if ok {
		return false, fmt.Errorf("Graph is unsupported in mixed materialization")
	}
	if _, ok, err := types.DecodeNotifyCommand(value); err != nil {
		return false, err
	} else if ok {
		return false, fmt.Errorf("Notify is unsupported in mixed materialization")
	}
	if _, ok, err := types.DecodeManagedKVCommand(value); err != nil {
		return false, err
	} else if ok {
		return true, nil
	}
	if _, ok, err := types.DecodeManagedKVBatch(value); err != nil {
		return false, err
	} else if ok {
		return true, nil
	}
	if commands, ok, err := types.DecodeSQLBatch(value); err != nil {
		return false, err
	} else if ok {
		for _, command := range commands {
			if command.Migration != nil {
				return false, fmt.Errorf("migrations are unsupported in mixed materialization")
			}
		}
		return len(commands) > 0, nil
	}
	if ok, err := types.DecodeReadBarrier(value); err != nil {
		return false, err
	} else if ok {
		return false, nil
	}
	if _, ok, err := types.DecodeLeaderSchedule(value); err != nil {
		return false, err
	} else if ok {
		return false, nil
	}
	if _, ok, err := types.DecodeCheckpointSeal(value); err != nil {
		return false, err
	} else if ok {
		return false, nil
	}
	if ok, err := types.DecodeReconfiguration(value); err != nil {
		return false, err
	} else if ok {
		return false, nil
	}
	return false, fmt.Errorf("unknown mixed application value")
}

func (m *Materializer) InspectManagedApplicationState(ctx context.Context) (ManagedApplicationState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.writer == nil {
		return ManagedApplicationState{}, sql.ErrConnDone
	}
	if m.managedProtocol != types.ManagedApplicationProtocolSQLKV && !originalsProtocol(m.managedProtocol) {
		return ManagedApplicationState{}, fmt.Errorf("mixed materialization not selected")
	}
	if err := m.sqlReceiptReadLocked(); err != nil {
		return ManagedApplicationState{}, err
	}
	if graphProtocol(m.managedProtocol) {
		if err := m.graphReadableLocked(); err != nil {
			return ManagedApplicationState{}, err
		}
	} else if err := validateManagedGraphAbsence(m.dbPath); err != nil {
		return ManagedApplicationState{}, err
	}
	tx, err := m.writer.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ManagedApplicationState{}, err
	}
	state, err := inspectApplicationTransaction(ctx, tx, m.stateTip, m.idempotencyWindow, m.managedProtocol)
	return ManagedApplicationState(state), err
}

// InspectManagedApplicationFile requires stable source SQLite/WAL and graph
// files, just like InspectSQLApplicationFile. It does not repair either store.
func InspectManagedApplicationFile(ctx context.Context, path string, window uint64) (ManagedApplicationState, uint64, error) {
	if window < 1024 || window > 1_048_576 {
		return ManagedApplicationState{}, 0, fmt.Errorf("invalid mixed receipt window")
	}
	if err := validateManagedArtifacts(path); err != nil {
		return ManagedApplicationState{}, 0, err
	}
	state, tip, err := inspectApplicationFile(ctx, path, window)
	return ManagedApplicationState(state), tip, err
}

func validateManagedReceipts(ctx context.Context, tx *sql.Tx, tip, window uint64, protocols ...string) error {
	var appliedText, stateText, hashText string
	if err := tx.QueryRowContext(ctx, `SELECT (SELECT value FROM _rhiza_meta WHERE key='applied_slot'),(SELECT value FROM _rhiza_meta WHERE key='state_slot'),(SELECT value FROM _rhiza_meta WHERE key='applied_hash')`).Scan(&appliedText, &stateText, &hashText); err != nil {
		return err
	}
	appliedTip, appliedErr := strconv.ParseUint(appliedText, 10, 64)
	stateTip, stateErr := strconv.ParseUint(stateText, 10, 64)
	hash, hashErr := hex.DecodeString(hashText)
	if appliedErr != nil || stateErr != nil || hashErr != nil || len(hash) != 32 || stateTip != tip || stateTip > appliedTip {
		return fmt.Errorf("invalid mixed snapshot position")
	}
	var unknown int
	query := `SELECT COUNT(*) FROM _rhiza_meta WHERE key NOT IN ('managed_application_protocol','managed_application_policy','sql_execution_policy','sql_idempotency_window','sql_receipt_result_bytes','sql_receipt_count','applied_slot','state_slot','applied_hash')`
	if len(protocols) > 0 && originalsProtocol(protocols[0]) {
		query += ` AND key!='request_original_bytes'`
	}
	if len(protocols) > 0 && graphProtocol(protocols[0]) {
		query += ` AND key NOT IN ('last_graph_slot','graph_history_hash')`
		history, err := sqliteGraphHistory(ctx, tx)
		if err != nil || history.LastGraphSlot > appliedTip {
			return fmt.Errorf("invalid SQLite graph history position")
		}
	}
	if err := tx.QueryRowContext(ctx, query).Scan(&unknown); err != nil {
		return err
	}
	if unknown != 0 {
		return fmt.Errorf("unsupported mixed materialization metadata")
	}
	var migrations int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM _rhiza_migrations`).Scan(&migrations); err != nil {
		return err
	}
	if migrations != 0 {
		return fmt.Errorf("mixed migrations are unsupported")
	}
	rows, err := tx.QueryContext(ctx, `SELECT kind,request_id,fingerprint,commit_slot,status,error_code,rows_affected,last_insert_id,applied,sql_result FROM _rhiza_idempotency`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var receiptCount, resultBytes uint64
	for rows.Next() {
		var kind, slot, affected, last, applied int64
		var id, fingerprint, result []byte
		var status types.MutationStatus
		var code string
		if err := rows.Scan(&kind, &id, &fingerprint, &slot, &status, &code, &affected, &last, &applied, &result); err != nil {
			return err
		}
		if len(id) == 0 || len(id) > types.MaxRequestIDBytes || len(fingerprint) != 32 || slot <= 0 || uint64(slot) > tip || uint64(slot) > math.MaxUint64-window+1 || (applied != 0 && applied != 1) {
			return fmt.Errorf("invalid mixed receipt envelope")
		}
		switch kind {
		case int64(types.MutationKV):
			if status != types.MutationCommitted || code != "" || affected != 0 || last != 0 || len(result) != 0 {
				return fmt.Errorf("invalid mixed KV receipt")
			}
		case int64(types.MutationSQL):
			if applied != 0 || (status != types.MutationCommitted && status != types.MutationRejected) || (status == types.MutationCommitted && code != "") {
				return fmt.Errorf("invalid mixed SQL receipt")
			}
			if status == types.MutationRejected && (affected != 0 || last != 0 || len(result) != 0 || (code != types.MutationErrorCodeExecutionFailed && code != types.MutationErrorCodePreconditionFailed && code != types.MutationErrorCodeResultRetentionFull)) {
				return fmt.Errorf("invalid mixed rejected receipt")
			}
			if len(result) > 0 {
				if _, err := decodeSQLResult(result); err != nil {
					return err
				}
			}
			receiptCount++
			resultBytes += uint64(len(result))
		default:
			return fmt.Errorf("unsupported mixed receipt kind")
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, counter := range []struct {
		key  string
		want uint64
	}{{"sql_receipt_count", receiptCount}, {"sql_receipt_result_bytes", resultBytes}} {
		var value string
		if err := tx.QueryRowContext(ctx, `SELECT value FROM _rhiza_meta WHERE key=?`, counter.key).Scan(&value); err != nil {
			return err
		}
		got, err := strconv.ParseUint(value, 10, 64)
		if err != nil || got != counter.want {
			return fmt.Errorf("invalid mixed retention accounting")
		}
	}
	return nil
}

func applicationObjectMetadata(object sqlEvidenceObject, mixed bool) ([]byte, error) {
	if !mixed {
		return json.Marshal(object)
	}
	// SQLite TEXT may contain invalid UTF-8. Include the original bytes, not
	// encoding/json's replacement characters, for schema identifiers and SQL.
	return json.Marshal(struct {
		Type             string
		Name, Table, SQL []byte
		Rows             []string
	}{object.Type, []byte(object.Name), []byte(object.Table), []byte(object.SQL), nil})
}
