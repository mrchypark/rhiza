package materializer

import (
	"bytes"
	"context"
	"database/sql"
	stdlibdriver "database/sql/driver"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
)

func TestRestoreCheckpointRemovesOwnedSQLiteTempSidecars(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	sourceDir := filepath.Join(root, "source")
	targetDir := filepath.Join(root, "target")
	if err := os.MkdirAll(sourceDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(targetDir, 0o700); err != nil {
		t.Fatal(err)
	}

	sourcePath := filepath.Join(sourceDir, "source.db")
	source, err := Open(sourcePath, 1)
	if err != nil {
		t.Fatal(err)
	}
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{
		RequestID: "temp-sidecar-fixture",
		SQL:       "CREATE TABLE sidecar_fixture (id INTEGER PRIMARY KEY, value TEXT); INSERT INTO sidecar_fixture VALUES (1, 'source')",
	}})
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
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	sourceDBBefore, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	snapshotPath := ""
	for _, file := range files {
		if file.Role == CheckpointSQLite {
			snapshotPath = file.Path
		}
	}
	if snapshotPath == "" {
		t.Fatal("checkpoint fixture omitted its SQLite file")
	}
	snapshotBefore, err := os.ReadFile(snapshotPath)
	if err != nil {
		t.Fatal(err)
	}

	unrelatedPath := filepath.Join(targetDir, "keep-unrelated.tmp")
	const unrelatedContents = "must remain untouched"
	if err := os.WriteFile(unrelatedPath, []byte(unrelatedContents), 0o600); err != nil {
		t.Fatal(err)
	}
	targetPath := filepath.Join(targetDir, "sqlite.db")
	target, err := Open(targetPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	target, err = Open(targetPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := target.RestoreCheckpoint(ctx, files); err != nil {
		_ = target.Close()
		t.Fatalf("restore real SQL and Graph checkpoint: %v", err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}

	snapshotAfter, err := os.ReadFile(snapshotPath)
	if err != nil || !bytes.Equal(snapshotAfter, snapshotBefore) {
		t.Fatalf("restore changed the separate source checkpoint: err=%v", err)
	}
	sourceDBAfter, err := os.ReadFile(sourcePath)
	if err != nil || !bytes.Equal(sourceDBAfter, sourceDBBefore) {
		t.Fatalf("restore changed the separate original database: err=%v", err)
	}
	unrelatedAfter, err := os.ReadFile(unrelatedPath)
	if err != nil || string(unrelatedAfter) != unrelatedContents {
		t.Fatalf("restore changed unrelated target file: contents=%q err=%v", unrelatedAfter, err)
	}
	entries, err := os.ReadDir(targetDir)
	if err != nil {
		t.Fatal(err)
	}
	var leftoverSidecars []string
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, ".rhiza-restore-") && (strings.HasSuffix(name, ".db-shm") || strings.HasSuffix(name, ".db-wal")) {
			leftoverSidecars = append(leftoverSidecars, name)
		}
	}
	if len(leftoverSidecars) != 0 {
		t.Fatalf("RestoreCheckpoint left SQLite sidecars for its owned temporary database: %v", leftoverSidecars)
	}
}

var errInjectedRestoreTempSidecarRemove = errors.New("injected owned restore sidecar remove failure")
var errInjectedOriginalMaterializerClose = errors.New("injected original materializer connection close failure")

func TestRestoreTempSidecarRemoveFailurePreservesOpenOriginal(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "sqlite.db")
	m, err := Open(dbPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	m.stopSQLiteCheckpoints()
	applyRestoreFixtureSQL(t, ctx, m, 1, "base", "CREATE TABLE preserved (id INTEGER PRIMARY KEY, value TEXT)")
	applyRestoreFixtureSQL(t, ctx, m, 2, "base-row", "INSERT INTO preserved VALUES (1, 'base')")
	files, _, cleanup, err := m.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	applyRestoreFixtureSQL(t, ctx, m, 3, "suffix-row", "INSERT INTO preserved VALUES (2, 'suffix')")
	graph, err := types.EncodeGraphCommand(types.GraphCommand{RequestID: "suffix-graph", Cypher: "CREATE (:ClosePreserved {id: 23})"})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 4, graph); err != nil {
		t.Fatal(err)
	}

	databaseBefore, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	walPath := dbPath + "-wal"
	walBefore, err := os.ReadFile(walPath)
	if err != nil || len(walBefore) == 0 {
		t.Fatalf("expected live original SQLite WAL before restore attempt: bytes=%d err=%v", len(walBefore), err)
	}
	unrelatedPath := filepath.Join(dir, "unrelated.temp")
	if err := os.WriteFile(unrelatedPath, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	var failedPath string
	remove := func(path string) error {
		if strings.HasPrefix(filepath.Base(path), ".rhiza-restore-") && strings.HasSuffix(path, ".db-shm") {
			failedPath = path
			return errInjectedRestoreTempSidecarRemove
		}
		return os.Remove(path)
	}
	var sqlitePath, graphDir string
	for _, file := range files {
		switch file.Role {
		case CheckpointSQLite:
			sqlitePath = file.Path
		case CheckpointGraphData:
			graphDir = filepath.Dir(file.Path)
		}
	}
	if err := m.restorePartsUsing(ctx, snapshotParts{sqlitePath: sqlitePath, graphDir: graphDir}, remove); !errors.Is(err, errInjectedRestoreTempSidecarRemove) {
		t.Fatalf("restore error=%v, want injected owned-sidecar cleanup error", err)
	}
	if failedPath == "" {
		t.Fatal("owned SQLite shm removal fault was not reached")
	}
	databaseAfter, err := os.ReadFile(dbPath)
	if err != nil || !bytes.Equal(databaseAfter, databaseBefore) {
		t.Fatalf("pre-install sidecar cleanup failure changed original database: err=%v", err)
	}
	walAfter, err := os.ReadFile(walPath)
	if err != nil || !bytes.Equal(walAfter, walBefore) {
		t.Fatalf("pre-install sidecar cleanup failure changed original WAL: err=%v", err)
	}
	if _, err := os.Stat(dbPath + ".restore-backup"); !os.IsNotExist(err) {
		t.Fatalf("pre-install sidecar cleanup failure started backup rename: %v", err)
	}
	if _, err := os.Stat(restoreStatePath(dbPath)); !os.IsNotExist(err) {
		t.Fatalf("pre-install sidecar cleanup failure wrote restore journal: %v", err)
	}
	if data, err := os.ReadFile(unrelatedPath); err != nil || string(data) != "keep" {
		t.Fatalf("pre-install sidecar cleanup failure changed unrelated temp file: data=%q err=%v", data, err)
	}
	if err := m.Health(ctx); err != nil {
		t.Fatalf("pre-install sidecar cleanup failure closed original materializer: %v", err)
	}
	rows, err := m.QueryResult(ctx, "SELECT value FROM preserved ORDER BY id", nil)
	if err != nil || len(rows.Rows) != 2 || rows.Rows[1][0] != "suffix" {
		t.Fatalf("open original materializer lost post-checkpoint SQL: rows=%+v err=%v", rows.Rows, err)
	}
}

func TestOriginalConnectionCloseFailurePreservesWALAndSkipsRollback(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "sqlite.db")
	m, err := Open(dbPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	m.stopSQLiteCheckpoints()
	defer m.Close()
	applyRestoreFixtureSQL(t, ctx, m, 1, "base", "CREATE TABLE preserved (id INTEGER PRIMARY KEY, value TEXT)")
	applyRestoreFixtureSQL(t, ctx, m, 2, "base-row", "INSERT INTO preserved VALUES (1, 'base')")
	files, _, cleanup, err := m.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	applyRestoreFixtureSQL(t, ctx, m, 3, "suffix-row", "INSERT INTO preserved VALUES (2, 'suffix')")
	graph, err := types.EncodeGraphCommand(types.GraphCommand{RequestID: "suffix-graph", Cypher: "CREATE (:ClosePreserved {id: 29})"})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 4, graph); err != nil {
		t.Fatal(err)
	}
	databaseBefore, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	walPath := dbPath + "-wal"
	walBefore, err := os.ReadFile(walPath)
	if err != nil || len(walBefore) == 0 {
		t.Fatalf("expected live original SQLite WAL before restore attempt: bytes=%d err=%v", len(walBefore), err)
	}

	originalWriter := m.writer
	faultDB := sql.OpenDB(restoreCloseErrorConnector{})
	if err := faultDB.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	m.writer = faultDB
	var sqlitePath, graphDir string
	for _, file := range files {
		switch file.Role {
		case CheckpointSQLite:
			sqlitePath = file.Path
		case CheckpointGraphData:
			graphDir = filepath.Dir(file.Path)
		}
	}
	err = m.restoreParts(ctx, snapshotParts{sqlitePath: sqlitePath, graphDir: graphDir})
	if !errors.Is(err, errInjectedOriginalMaterializerClose) {
		t.Fatalf("restore error=%v, want original connection close failure", err)
	}
	if m.closeErr == nil || m.writer != nil {
		t.Fatalf("failed close did not poison/close the materializer: closeErr=%v writer=%v", m.closeErr, m.writer)
	}
	databaseAfter, err := os.ReadFile(dbPath)
	if err != nil || !bytes.Equal(databaseAfter, databaseBefore) {
		t.Fatalf("original close failure changed database file: err=%v", err)
	}
	walAfter, err := os.ReadFile(walPath)
	if err != nil || !bytes.Equal(walAfter, walBefore) {
		t.Fatalf("original close failure removed or changed original WAL: err=%v", err)
	}
	if _, err := os.Stat(dbPath + ".restore-backup"); !os.IsNotExist(err) {
		t.Fatalf("original close failure started backup rename: %v", err)
	}
	if _, err := os.Stat(restoreStatePath(dbPath)); !os.IsNotExist(err) {
		t.Fatalf("original close failure invoked rollback/recovery journal: %v", err)
	}
	if err := originalWriter.Close(); err != nil {
		t.Fatalf("close retained original writer after assertions: %v", err)
	}
	if err := m.Close(); !errors.Is(err, errInjectedOriginalMaterializerClose) {
		t.Fatalf("poisoned materializer close error=%v", err)
	}
	reopened, err := Open(dbPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	rows, err := reopened.QueryResult(ctx, "SELECT value FROM preserved ORDER BY id", nil)
	if err != nil || len(rows.Rows) != 2 || rows.Rows[1][0] != "suffix" {
		t.Fatalf("original SQL state after refused restore: rows=%+v err=%v", rows.Rows, err)
	}
	graphRows, err := reopened.GraphQuery(ctx, "MATCH (n:ClosePreserved) RETURN n.id", nil)
	if err != nil || len(graphRows.Rows) != 1 || graphRows.Rows[0][0] != int64(29) {
		t.Fatalf("original Graph state after refused restore: rows=%+v err=%v", graphRows.Rows, err)
	}
}

func applyRestoreFixtureSQL(t *testing.T, ctx context.Context, m *Materializer, slot uint64, requestID, statement string) {
	t.Helper()
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: requestID, SQL: statement}})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, slot, value); err != nil {
		t.Fatal(err)
	}
}

type restoreCloseErrorConnector struct{}

func (restoreCloseErrorConnector) Connect(context.Context) (stdlibdriver.Conn, error) {
	return restoreCloseErrorConn{}, nil
}

func (restoreCloseErrorConnector) Driver() stdlibdriver.Driver { return restoreCloseErrorDriver{} }

type restoreCloseErrorDriver struct{}

func (restoreCloseErrorDriver) Open(string) (stdlibdriver.Conn, error) {
	return restoreCloseErrorConn{}, nil
}

type restoreCloseErrorConn struct{}

func (restoreCloseErrorConn) Prepare(string) (stdlibdriver.Stmt, error) {
	return nil, errors.New("prepare is unused")
}

func (restoreCloseErrorConn) Close() error { return errInjectedOriginalMaterializerClose }

func (restoreCloseErrorConn) Begin() (stdlibdriver.Tx, error) {
	return nil, errors.New("transactions are unused")
}

func (restoreCloseErrorConn) Ping(context.Context) error { return nil }
