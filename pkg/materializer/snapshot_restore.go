package materializer

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"

	sqlite3driver "github.com/ncruces/go-sqlite3/driver"
)

func (m *Materializer) Snapshot(ctx context.Context) ([]byte, error) {
	data, _, err := m.SnapshotAt(ctx)
	return data, err
}

// SnapshotAt is unavailable for the combined store; use CheckpointFilesAt.
func (m *Materializer) SnapshotAt(ctx context.Context) ([]byte, uint64, error) {
	var data bytes.Buffer
	index, err := m.SnapshotTo(ctx, &data)
	return data.Bytes(), index, err
}

// SnapshotTo is unavailable for the combined store; use CheckpointFilesAt.
func (m *Materializer) SnapshotTo(ctx context.Context, writer io.Writer) (uint64, error) {
	m.mu.RLock()
	closed := m.db == nil
	m.mu.RUnlock()
	if closed {
		return 0, sql.ErrConnDone
	}
	return 0, fmt.Errorf("combined SQL and Graph snapshots require CheckpointFilesAt")
}

func (m *Materializer) beginSQLiteSnapshot(ctx context.Context) (*sqliteSnapshot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.sqlReceiptReadLocked(); err != nil {
		return nil, err
	}
	db := m.db
	return beginSQLiteSnapshot(ctx, db)
}

func (m *Materializer) beginSQLiteReadSnapshot(ctx context.Context) (*sqliteSnapshot, error) {
	m.mu.RLock()
	if err := m.sqlReceiptReadLocked(); err != nil {
		m.mu.RUnlock()
		return nil, err
	}
	var reader *sql.DB
	if len(m.readers) != 0 {
		reader = m.readers[0]
	}
	snapshot, err := beginSQLiteSnapshot(ctx, reader)
	if err != nil || m.managedProtocol == "" {
		m.mu.RUnlock()
	} else {
		snapshot.release = m.mu.RUnlock
	}
	return snapshot, err
}

// beginSQLiteSnapshotLocked is for callers that already hold m.mu.
func (m *Materializer) beginSQLiteSnapshotLocked(ctx context.Context) (*sqliteSnapshot, error) {
	if err := m.sqlReceiptReadLocked(); err != nil {
		return nil, err
	}
	return beginSQLiteSnapshot(ctx, m.db)
}

func beginSQLiteSnapshot(ctx context.Context, db *sql.DB) (*sqliteSnapshot, error) {
	if db == nil {
		return nil, sql.ErrConnDone
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		_ = conn.Close()
		return nil, err
	}
	var value string
	if err := conn.QueryRowContext(ctx, `SELECT value FROM _rhiza_meta WHERE key = 'applied_slot'`).Scan(&value); err != nil {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		_ = conn.Close()
		return nil, err
	}
	index, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		_ = conn.Close()
		return nil, err
	}
	return &sqliteSnapshot{conn: conn, index: index}, nil
}

func (snapshot *sqliteSnapshot) Backup(path string) error {
	destination := (&url.URL{Scheme: "file", Path: filepath.ToSlash(path)}).String()
	return snapshot.conn.Raw(func(driverConn any) error {
		raw, ok := driverConn.(sqlite3driver.Conn)
		if !ok {
			return fmt.Errorf("unexpected SQLite driver connection %T", driverConn)
		}
		return raw.Raw().Backup("main", destination)
	})
}

func (snapshot *sqliteSnapshot) Close() error {
	if snapshot == nil || snapshot.conn == nil {
		return nil
	}
	if snapshot.release != nil {
		defer snapshot.release()
		snapshot.release = nil
	}
	_, rollbackErr := snapshot.conn.ExecContext(context.Background(), "ROLLBACK")
	closeErr := snapshot.conn.Close()
	snapshot.conn = nil
	if rollbackErr != nil {
		return rollbackErr
	}
	return closeErr
}

// CheckpointFilesAt captures fixed-role database files without packaging them.
func (m *Materializer) CheckpointFilesAt(ctx context.Context) ([]CheckpointFile, uint64, func(), error) {
	if m.managedProtocol != "" && !graphProtocol(m.managedProtocol) {
		return m.managedCheckpointFilesAt(ctx)
	}
	dir := filepath.Dir(m.dbPath)
	sqlite, err := os.CreateTemp(dir, ".rhiza-checkpoint-sqlite-*")
	if err != nil {
		return nil, 0, nil, err
	}
	sqlitePath := sqlite.Name()
	_ = sqlite.Close()
	_ = os.Remove(sqlitePath)
	paths := []string{sqlitePath}
	cleanup := func() {
		for _, path := range paths {
			_ = os.Remove(path)
		}
	}
	graph, err := os.CreateTemp(dir, ".rhiza-checkpoint-graph-*")
	if err != nil {
		cleanup()
		return nil, 0, nil, err
	}
	graphPath := graph.Name()
	_ = graph.Close()
	_ = os.Remove(graphPath)
	paths = append(paths, graphPath)
	m.mu.Lock()
	err = m.graphReadableLocked()
	var sqliteSnapshot *sqliteSnapshot
	if err == nil {
		sqliteSnapshot, err = m.beginSQLiteSnapshotLocked(ctx)
	}
	index := uint64(0)
	if err == nil {
		index = sqliteSnapshot.index
	}
	if err == nil && (m.tip != index || m.graphTip() != index) {
		err = fmt.Errorf("checkpoint stores disagree at slot %d", index)
	}
	var graphSnap *graphSnapshot
	if err == nil {
		graphSnap, err = m.beginGraphSnapshot(ctx)
	}
	m.mu.Unlock()
	if err != nil {
		if sqliteSnapshot != nil {
			_ = sqliteSnapshot.Close()
		}
		cleanup()
		return nil, 0, nil, err
	}
	if err = sqliteSnapshot.Backup(sqlitePath); err == nil {
		err = ctx.Err()
	}
	if err == nil {
		err = graphSnap.Backup(graphPath)
	}
	if closeErr := graphSnap.Close(); err == nil {
		err = closeErr
	}
	if closeErr := sqliteSnapshot.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		cleanup()
		return nil, 0, nil, err
	}
	return []CheckpointFile{{Role: CheckpointSQLite, Path: sqlitePath}, {Role: CheckpointGraphData, Path: graphPath}}, index, cleanup, nil
}

// Restore is unavailable for the combined store; use RestoreCheckpoint.
func (m *Materializer) Restore(ctx context.Context, data []byte) error {
	return fmt.Errorf("combined SQL and Graph restore requires fixed-role checkpoint files")
}

// RestoreFile is unavailable for the combined store; use RestoreCheckpoint.
func (m *Materializer) RestoreFile(ctx context.Context, snapshotPath string) error {
	return fmt.Errorf("combined SQL and Graph restore requires fixed-role checkpoint files")
}

// RestoreCheckpoint atomically installs the exact fixed-role checkpoint set.
func (m *Materializer) RestoreCheckpoint(ctx context.Context, files []CheckpointFile) error {
	if m.managedProtocol != "" && !graphProtocol(m.managedProtocol) {
		return m.restoreManagedCheckpoint(ctx, files)
	}
	var parts snapshotParts
	seen := make(map[CheckpointRole]bool, len(files))
	for _, file := range files {
		if seen[file.Role] || file.Path == "" {
			return fmt.Errorf("invalid checkpoint file set")
		}
		seen[file.Role] = true
		switch file.Role {
		case CheckpointSQLite:
			parts.sqlitePath = file.Path
		case CheckpointGraphData:
			parts.graphDir = filepath.Dir(file.Path)
		default:
			return fmt.Errorf("unknown checkpoint role %q", file.Role)
		}
	}
	if parts.sqlitePath == "" || !seen[CheckpointGraphData] {
		return fmt.Errorf("checkpoint requires SQLite and Graph files")
	}
	if err := sqlpolicy.CheckSnapshotFileForPolicy(ctx, parts.sqlitePath, m.materializationPolicy); err != nil {
		return err
	}
	if err := checkManagedPolicySnapshot(ctx, parts.sqlitePath, m.managedProtocol, m.idempotencyWindow); err != nil {
		return err
	}
	root, err := os.MkdirTemp(filepath.Dir(m.dbPath), ".rhiza-graph-restore-*")
	if err != nil {
		return err
	}
	graphDir := filepath.Join(root, "latticedb")
	if err := os.MkdirAll(graphDir, 0o700); err != nil {
		_ = os.RemoveAll(root)
		return err
	}
	source := ""
	for _, file := range files {
		if file.Role == CheckpointGraphData {
			source = file.Path
		}
	}
	if err := copyFile(source, filepath.Join(graphDir, "graph.ltdb")); err != nil {
		_ = os.RemoveAll(root)
		return err
	}
	parts.graphDir = graphDir
	parts.cleanup = func() { _ = os.RemoveAll(root) }
	return m.restoreParts(ctx, parts)
}

func copyFile(sourcePath, targetPath string) error {
	source, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer source.Close()
	target, err := os.OpenFile(targetPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, err = io.Copy(target, source)
	if err == nil {
		err = target.Sync()
	}
	if closeErr := target.Close(); err == nil {
		err = closeErr
	}
	return err
}

func restoreStatePath(dbPath string) string { return dbPath + ".restore-state.json" }

func syncDirectory(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = f.Sync()
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	return err
}

func writeRestoreState(dbPath string, state restoreState) error {
	dir := filepath.Dir(dbPath)
	file, err := os.CreateTemp(dir, ".rhiza-restore-state-*")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	if err = file.Chmod(0o600); err == nil {
		err = json.NewEncoder(file).Encode(state)
	}
	if err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err := os.Rename(name, restoreStatePath(dbPath)); err != nil {
		return err
	}
	return syncDirectory(dir)
}

func recoverRestore(dbPath string) error {
	journal := restoreStatePath(dbPath)
	data, err := os.ReadFile(journal)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var discriminator struct {
		LocalVersion int `json:"local_version"`
	}
	if err := json.Unmarshal(data, &discriminator); err != nil {
		return fmt.Errorf("decode restore journal: %w", err)
	}
	if discriminator.LocalVersion != 0 {
		return fmt.Errorf("Local restore journal requires exact validated WAL-base recovery")
	}
	var state restoreState
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("decode restore journal: %w", err)
	}
	dir := filepath.Dir(dbPath)
	backupPath := dbPath + ".restore-backup"
	graphPath := filepath.Join(dir, "latticedb")
	graphBackupPath := graphPath + ".restore-backup"
	if state.Phase != "committed" {
		if _, err := os.Stat(backupPath); err == nil {
			_ = os.Remove(dbPath)
			if err := os.Rename(backupPath, dbPath); err != nil {
				return fmt.Errorf("rollback SQLite restore: %w", err)
			}
		} else if err != nil && !os.IsNotExist(err) {
			return err
		}
		if state.InstallGraph {
			if state.GraphHadOriginal {
				if _, err := os.Stat(graphBackupPath); err == nil {
					_ = os.RemoveAll(graphPath)
					if err := os.Rename(graphBackupPath, graphPath); err != nil {
						return fmt.Errorf("rollback graph restore: %w", err)
					}
				} else if !os.IsNotExist(err) {
					return err
				}
			} else {
				_ = os.RemoveAll(graphPath)
			}
		}
	}
	_ = os.Remove(backupPath)
	_ = os.RemoveAll(graphBackupPath)
	if err := syncDirectory(dir); err != nil {
		return err
	}
	if err := os.Remove(journal); err != nil && !os.IsNotExist(err) {
		return err
	}
	return syncDirectory(dir)
}

func (m *Materializer) restoreParts(ctx context.Context, parts snapshotParts) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.sqlReceiptReadLocked(); err != nil {
		return err
	}
	dir := filepath.Dir(m.dbPath)
	if parts.cleanup != nil {
		defer parts.cleanup()
	}
	file, err := os.CreateTemp(dir, ".rhiza-restore-*.db")
	if err != nil {
		return err
	}
	tempPath := file.Name()
	defer os.Remove(tempPath)
	if err = file.Chmod(0o600); err == nil {
		var source *os.File
		source, err = os.Open(parts.sqlitePath)
		if err == nil {
			_, err = io.Copy(file, source)
			if closeErr := source.Close(); err == nil {
				err = closeErr
			}
		}
	}
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return fmt.Errorf("write snapshot: %w", err)
	}
	if closeErr != nil {
		return fmt.Errorf("close snapshot: %w", closeErr)
	}
	check, err := openSQLite((&url.URL{Scheme: "file", Path: filepath.ToSlash(tempPath)}).String()+"?mode=ro&immutable=1", false)
	if err != nil {
		return fmt.Errorf("open snapshot: %w", err)
	}
	var status string
	err = check.QueryRowContext(ctx, `PRAGMA quick_check`).Scan(&status)
	check.Close()
	if err != nil || status != "ok" {
		return fmt.Errorf("invalid snapshot: quick_check=%q err=%v", status, err)
	}
	backupPath := m.dbPath + ".restore-backup"
	if err := sqlpolicy.CheckSnapshotFileForPolicy(ctx, tempPath, m.materializationPolicy); err != nil {
		return err
	}
	graphPath := filepath.Join(dir, "latticedb")
	graphBackupPath := graphPath + ".restore-backup"
	if _, err := os.Stat(backupPath); !os.IsNotExist(err) {
		return fmt.Errorf("stale SQLite restore backup exists")
	}
	if _, err := os.Stat(graphBackupPath); !os.IsNotExist(err) {
		return fmt.Errorf("stale graph restore backup exists")
	}
	_, graphStatErr := os.Stat(graphPath)
	state := restoreState{Phase: "prepared", InstallGraph: parts.graphDir != "", GraphHadOriginal: graphStatErr == nil}
	if graphStatErr != nil && !os.IsNotExist(graphStatErr) {
		return graphStatErr
	}
	if err := writeRestoreState(m.dbPath, state); err != nil {
		return fmt.Errorf("prepare restore journal: %w", err)
	}
	rollback := func(cause error) error {
		var recoveryErr, reopenErr error
		if err := recoverRestore(m.dbPath); err != nil {
			recoveryErr = fmt.Errorf("recover restore: %w", err)
		}
		if err := m.reopen(); err != nil {
			reopenErr = fmt.Errorf("reopen materializer: %w", err)
		}
		return errors.Join(cause, recoveryErr, reopenErr)
	}
	if err := m.closeConnections(); err != nil {
		return rollback(err)
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if err := os.Remove(m.dbPath + suffix); err != nil && !os.IsNotExist(err) {
			return rollback(fmt.Errorf("remove SQLite sidecar %s: %w", suffix, err))
		}
	}
	if err := os.Rename(m.dbPath, backupPath); err != nil && !os.IsNotExist(err) {
		return rollback(fmt.Errorf("backup database: %w", err))
	}
	if err := syncDirectory(dir); err != nil {
		return rollback(err)
	}
	state.Phase = "sqlite-backed-up"
	if err := writeRestoreState(m.dbPath, state); err != nil {
		return rollback(err)
	}
	graphBackedUp := false
	graphInstalled := false
	if parts.graphDir != "" {
		if err := os.Rename(graphPath, graphBackupPath); err == nil {
			graphBackedUp = true
		} else if !os.IsNotExist(err) {
			_ = os.Rename(backupPath, m.dbPath)
			return rollback(fmt.Errorf("backup graph database: %w", err))
		}
		if err := os.Rename(parts.graphDir, graphPath); err != nil {
			if graphBackedUp {
				_ = os.Rename(graphBackupPath, graphPath)
			}
			_ = os.Rename(backupPath, m.dbPath)
			return rollback(fmt.Errorf("install graph snapshot: %w", err))
		}
		graphInstalled = true
		if err := syncDirectory(dir); err != nil {
			return rollback(err)
		}
		state.Phase = "graph-installed"
		if err := writeRestoreState(m.dbPath, state); err != nil {
			return rollback(err)
		}
	}
	if err := os.Rename(tempPath, m.dbPath); err != nil {
		if graphInstalled {
			_ = os.RemoveAll(graphPath)
		}
		if graphBackedUp {
			_ = os.Rename(graphBackupPath, graphPath)
		}
		_ = os.Rename(backupPath, m.dbPath)
		return rollback(fmt.Errorf("install snapshot: %w", err))
	}
	if err := syncDirectory(dir); err != nil {
		return rollback(err)
	}
	state.Phase = "sqlite-installed"
	if err := writeRestoreState(m.dbPath, state); err != nil {
		return rollback(err)
	}
	restored, err := openMaterializerPolicy(m.dbPath, m.readersN, m.managedProtocol, m.idempotencyWindow)
	if err == nil && (m.managedProtocol == "" || graphProtocol(m.managedProtocol)) {
		err = restored.validateRestoredSnapshot()
	}
	if err == nil && m.managedProtocol == "" {
		err = ensureGraphNodePropertyIndexes(restored.graph, m.graphNodePropertyIndexes)
		restored.graphNodePropertyIndexes = slices.Clone(m.graphNodePropertyIndexes)
	}
	if err != nil {
		if restored != nil {
			_ = restored.Close()
		}
		os.Remove(m.dbPath)
		if graphInstalled {
			_ = os.RemoveAll(graphPath)
		}
		if graphBackedUp {
			_ = os.Rename(graphBackupPath, graphPath)
		}
		_ = os.Rename(backupPath, m.dbPath)
		return rollback(fmt.Errorf("open restored snapshot: %w", err))
	}
	state.Phase = "committed"
	if err := writeRestoreState(m.dbPath, state); err != nil {
		_ = restored.Close()
		return rollback(fmt.Errorf("commit restore journal: %w", err))
	}
	if err := recoverRestore(m.dbPath); err != nil {
		_ = restored.Close()
		return rollback(fmt.Errorf("finalize restore: %w", err))
	}
	m.adopt(restored)
	return nil
}

func (m *Materializer) reopen() error {
	open := openWithProtocol
	if m.managedProtocol != "" {
		open = openMaterializerPolicy
	}
	reopened, err := open(m.dbPath, m.readersN, m.managedProtocol, m.idempotencyWindow)
	if err != nil {
		return err
	}
	if m.managedProtocol == "" {
		if err := ensureGraphNodePropertyIndexes(reopened.graph, m.graphNodePropertyIndexes); err != nil {
			_ = reopened.Close()
			return err
		}
	}
	reopened.graphNodePropertyIndexes = slices.Clone(m.graphNodePropertyIndexes)
	m.adopt(reopened)
	return nil
}

// adopt requires m.mu's write lock and already-closed previous target handles.
// source must be a distinct temporary materializer exclusively owned by caller.
func (m *Materializer) adopt(source *Materializer) {
	source.stopSQLiteCheckpoints()
	m.db, m.writer, m.readers, m.graph = source.db, source.writer, source.readers, source.graph
	m.tip, m.stateTip, m.tipHash = source.tip, source.stateTip, source.tipHash
	m.recentSQLReceipts = source.recentSQLReceipts
	m.sqlReceipts = source.sqlReceipts
	m.sqlReceiptRetention = source.sqlReceiptRetention
	m.sqlRetentionPlan = sqlRetentionBarrierPlan{}
	m.graphNodePropertyIndexes = slices.Clone(source.graphNodePropertyIndexes)
	source.db, source.writer, source.readers, source.graph = nil, nil, nil, nil
	_ = source.Close()
}

// Tip returns the last applied slot.
