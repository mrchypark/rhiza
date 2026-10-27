package materializer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	latticedb "github.com/mrchypark/latticedb-go"
	"github.com/mrchypark/rhiza/internal/types"
)

var errInjectedRestoreJournalWrite = errors.New("injected restore journal write failure")

func TestRestoreJournalWriteFailureReopensOriginalMaterializer(t *testing.T) {
	for _, afterPublish := range []bool{false, true} {
		name := "before-rename"
		if afterPublish {
			name = "after-rename-directory-sync-error"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			source, files, cleanup := openRestoreCloseTestSnapshot(t, filepath.Join(dir, "source", "sqlite.db"), "snapshot", 22)
			defer cleanup()
			if err := source.Close(); err != nil {
				t.Fatal(err)
			}

			target, _, targetCleanup := openRestoreCloseTestSnapshot(t, filepath.Join(dir, "target", "sqlite.db"), "original", 11)
			targetCleanup()
			defer target.Close()
			originalInfo, err := os.Stat(target.dbPath)
			if err != nil {
				t.Fatal(err)
			}
			target.restoreStateWriteHook = func(dbPath string, state restoreState) error {
				if state.Phase == "prepared" {
					if afterPublish {
						return writeRestoreStateUsing(dbPath, state, func(string) error {
							return errInjectedRestoreJournalWrite
						})
					}
					return errInjectedRestoreJournalWrite
				}
				return writeRestoreState(dbPath, state)
			}
			if err := target.RestoreCheckpoint(ctx, files); !errors.Is(err, errInjectedRestoreJournalWrite) {
				t.Fatalf("RestoreCheckpoint error=%v, want journal write failure", err)
			}
			if err := target.Health(ctx); err != nil {
				t.Fatalf("materializer remained unusable after journal write failure: %v", err)
			}
			assertRestoreCloseTestState(t, ctx, target, "original", 11)
			if target.Tip() != 3 {
				t.Fatalf("reopened tip=%d, want original tip 3", target.Tip())
			}
			currentInfo, err := os.Stat(target.dbPath)
			if err != nil || !os.SameFile(originalInfo, currentInfo) {
				t.Fatalf("original SQLite database was replaced: err=%v", err)
			}
			for _, path := range []string{
				restoreStatePath(target.dbPath), target.dbPath + ".restore-backup",
				filepath.Join(filepath.Dir(target.dbPath), "latticedb.restore-backup"),
			} {
				if _, err := os.Lstat(path); !os.IsNotExist(err) {
					t.Errorf("restore journal/backup unexpectedly exists at %q: %v", path, err)
				}
			}
			insert, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "continued-sql", SQL: "INSERT INTO restore_close_rows VALUES ('continued')"}})
			if err != nil {
				t.Fatal(err)
			}
			if err := target.Apply(ctx, 4, insert); err != nil {
				t.Fatalf("same-instance SQL write after recovery: %v", err)
			}
			graph, err := types.EncodeGraphCommand(types.GraphCommand{RequestID: "continued-graph", Cypher: `CREATE (:RestoreClose {id: 12})`})
			if err != nil {
				t.Fatal(err)
			}
			if err := target.Apply(ctx, 5, graph); err != nil {
				t.Fatalf("same-instance Graph write after recovery: %v", err)
			}
			if target.Tip() != 5 {
				t.Fatalf("same-instance tip after SQL/Graph writes=%d, want 5", target.Tip())
			}
		})
	}
}

func TestRestoreRefusesActiveGraphSnapshotWithoutClosingOrRenamingOriginal(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	source, files, cleanup := openRestoreCloseTestSnapshot(t, filepath.Join(dir, "source", "sqlite.db"), "snapshot", 22)
	defer cleanup()
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	target, _, targetCleanup := openRestoreCloseTestSnapshot(t, filepath.Join(dir, "target", "sqlite.db"), "original", 11)
	targetCleanup()
	defer target.Close()
	snapshot, err := target.beginGraphSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	originalSQL, err := os.ReadFile(target.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	graphPath := filepath.Join(filepath.Dir(target.dbPath), "latticedb", "graph.ltdb")
	originalGraphInfo, err := os.Stat(graphPath)
	if err != nil {
		t.Fatal(err)
	}
	originalWAL, walErr := os.ReadFile(target.dbPath + "-wal")
	walExisted := walErr == nil
	if walErr != nil && !os.IsNotExist(walErr) {
		t.Fatal(walErr)
	}

	for attempt := 0; attempt < 2; attempt++ {
		err = target.RestoreCheckpoint(ctx, files)
		if !errors.Is(err, latticedb.ErrSnapshotActive) {
			t.Fatalf("RestoreCheckpoint attempt %d error=%v, want active graph snapshot refusal", attempt+1, err)
		}
		select {
		case <-target.graph.streamWake:
			t.Fatal("graph publication wake channel was closed after restore refusal")
		default:
		}
	}
	if got, err := os.ReadFile(target.dbPath); err != nil || string(got) != string(originalSQL) {
		t.Fatalf("original SQLite file changed after graph-close refusal: err=%v", err)
	}
	graphInfo, err := os.Stat(graphPath)
	if err != nil || !os.SameFile(originalGraphInfo, graphInfo) {
		t.Fatalf("original graph storage was replaced after graph-close refusal: err=%v", err)
	}
	if walExisted {
		if got, err := os.ReadFile(target.dbPath + "-wal"); err != nil || string(got) != string(originalWAL) {
			t.Fatalf("original SQLite WAL changed after graph-close refusal: err=%v", err)
		}
	} else if _, err := os.Lstat(target.dbPath + "-wal"); !os.IsNotExist(err) {
		t.Fatalf("graph-close refusal created a SQLite WAL: %v", err)
	}
	for _, path := range []string{
		restoreStatePath(target.dbPath), target.dbPath + ".restore-backup",
		filepath.Join(filepath.Dir(target.dbPath), "latticedb.restore-backup"),
	} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Errorf("restore journal/backup unexpectedly exists at %q: %v", path, err)
		}
	}
	if target.graph == nil || target.graph.db == nil || !target.graph.db.IsOpen() || target.db == nil || target.writer == nil {
		t.Fatalf("graph-close refusal discarded live materializer handles: graph=%v db=%v writer=%v", target.graph, target.db, target.writer)
	}
	if records, _, err := target.GraphReadStream(ctx, "close-regression", 0, 10, 0); err != nil || len(records) != 0 {
		t.Fatalf("initial graph stream read records=%+v err=%v", records, err)
	}
	command, err := types.EncodeGraphCommand(types.GraphCommand{
		RequestID: "after-close-refusal", Cypher: `CREATE (:RestoreClose {id: 99})`,
		Events: []types.GraphStreamEvent{{Stream: "close-regression", Kind: "created", Payload: "after-close-refusal"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := target.Apply(ctx, 4, command); err != nil {
		t.Fatalf("graph write after restore refusal: %v", err)
	}
	streamRecords, _, err := target.GraphReadStream(ctx, "close-regression", 0, 10, 0)
	if err != nil || len(streamRecords) != 1 || streamRecords[0].Payload != "after-close-refusal" {
		t.Fatalf("graph stream after restore refusal records=%+v err=%v", streamRecords, err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := target.RestoreCheckpoint(ctx, files); err != nil {
		t.Fatalf("restore after releasing graph snapshot: %v", err)
	}
	assertRestoreCloseTestState(t, ctx, target, "snapshot", 22)
	if target.Tip() != 3 {
		t.Fatalf("restored tip=%d, want snapshot tip 3", target.Tip())
	}
	if err := target.Close(); err != nil {
		t.Fatalf("final Close after successful restore: %v", err)
	}
	if target.graph != nil || target.db != nil || target.writer != nil {
		t.Fatalf("final Close did not release retained connections: graph=%v db=%v writer=%v", target.graph, target.db, target.writer)
	}
}

func TestCheckpointFilesAtKeepsGraphLeaseAfterSQLiteSnapshotClose(t *testing.T) {
	ctx := context.Background()
	m, _, cleanup := openRestoreCloseTestSnapshot(t, filepath.Join(t.TempDir(), "target", "sqlite.db"), "checkpoint", 31)
	cleanup()
	defer m.Close()
	baselineInUse := m.db.Stats().InUse
	entered, release := make(chan struct{}), make(chan struct{})
	var inUse int
	var graphCloseErr error
	m.checkpointSnapshotCloseHook = func(stage string) {
		if stage != "sqlite-closed-before-graph" {
			return
		}
		inUse = m.db.Stats().InUse
		graphCloseErr = m.graph.db.Close()
		close(entered)
		<-release
	}
	type result struct {
		files   []CheckpointFile
		cleanup func()
		err     error
	}
	done := make(chan result, 1)
	go func() {
		files, _, cleanup, err := m.CheckpointFilesAt(ctx)
		done <- result{files: files, cleanup: cleanup, err: err}
	}()
	<-entered
	if inUse != baselineInUse {
		t.Errorf("SQLite checkpoint connection still borrowed at graph-lease barrier: in-use=%d baseline=%d", inUse, baselineInUse)
	}
	if !errors.Is(graphCloseErr, latticedb.ErrSnapshotActive) {
		t.Errorf("graph Close at SQLite-first barrier=%v, want active graph snapshot refusal", graphCloseErr)
	}
	close(release)
	checkpoint := <-done
	m.checkpointSnapshotCloseHook = nil
	if checkpoint.err != nil {
		t.Fatalf("CheckpointFilesAt: %v", checkpoint.err)
	}
	if checkpoint.cleanup != nil {
		checkpoint.cleanup()
	}
	if len(checkpoint.files) != 2 {
		t.Fatalf("CheckpointFilesAt returned %d files, want 2", len(checkpoint.files))
	}
}

func TestCheckpointFilesAtClosesSQLiteBeforeUnlockOnGraphSnapshotFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m, _, cleanup := openRestoreCloseTestSnapshot(t, filepath.Join(t.TempDir(), "target", "sqlite.db"), "failed-checkpoint", 41)
	cleanup()
	defer m.Close()
	baselineInUse := m.db.Stats().InUse
	graphAttempt, continueGraph := make(chan struct{}), make(chan struct{})
	closeEntered, releaseClose := make(chan struct{}), make(chan struct{})
	var inUse int
	var mutexWasAvailable bool
	m.checkpointSnapshotCloseHook = func(stage string) {
		if stage == "sqlite-snapshot-acquired-before-graph" {
			close(graphAttempt)
			<-continueGraph
			return
		}
		if stage != "snapshot-acquire-error-sqlite-closed" {
			return
		}
		inUse = m.db.Stats().InUse
		mutexWasAvailable = m.mu.TryRLock()
		if mutexWasAvailable {
			m.mu.RUnlock()
		}
		close(closeEntered)
		<-releaseClose
	}
	result := make(chan error, 1)
	go func() {
		_, _, _, err := m.CheckpointFilesAt(ctx)
		result <- err
	}()
	<-graphAttempt
	cancel()
	close(continueGraph)
	<-closeEntered
	if inUse != baselineInUse {
		t.Errorf("SQLite snapshot connection was not returned before unlocking: in-use=%d baseline=%d", inUse, baselineInUse)
	}
	if mutexWasAvailable {
		t.Error("materializer mutex was released before SQLite snapshot close completed")
	}
	close(releaseClose)
	m.checkpointSnapshotCloseHook = nil
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("CheckpointFilesAt error=%v, want canceled graph snapshot acquisition", err)
	}
}

func TestRestorePreservesInstalledFilesWhenRestoredCloseFails(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	source, files, cleanup := openRestoreCloseTestSnapshot(t, filepath.Join(dir, "source", "sqlite.db"), "snapshot", 22)
	defer cleanup()
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	target, _, targetCleanup := openRestoreCloseTestSnapshot(t, filepath.Join(dir, "target", "sqlite.db"), "original", 11)
	targetCleanup()
	defer target.Close()

	var restored *Materializer
	var held *graphSnapshot
	target.restoreOpenedHook = func(opened *Materializer) {
		restored = opened
		var err error
		held, err = opened.beginGraphSnapshot(ctx)
		if err != nil {
			t.Errorf("hold restored graph snapshot: %v", err)
			return
		}
		opened.graph.commitUnknown = true
	}
	err := target.RestoreCheckpoint(ctx, files)
	if !errors.Is(err, ErrGraphCommitUnknown) || !errors.Is(err, latticedb.ErrSnapshotActive) {
		t.Fatalf("RestoreCheckpoint error=%v, want validation and graph-close errors", err)
	}
	if restored == nil || held == nil {
		t.Fatal("restored materializer/snapshot hook was not reached")
	}
	for _, path := range []string{
		target.dbPath,
		target.dbPath + ".restore-backup",
		filepath.Join(filepath.Dir(target.dbPath), "latticedb", "graph.ltdb"),
		filepath.Join(filepath.Dir(target.dbPath), "latticedb.restore-backup", "graph.ltdb"),
	} {
		if _, err := os.Stat(path); err != nil {
			t.Errorf("installed or backup file missing after restored Close failure at %q: %v", path, err)
		}
	}
	journalBytes, err := os.ReadFile(restoreStatePath(target.dbPath))
	if err != nil {
		t.Fatal(err)
	}
	var journal restoreState
	if err := json.Unmarshal(journalBytes, &journal); err != nil || journal.Phase != "sqlite-installed" {
		t.Fatalf("restore journal=%+v err=%v, want retained sqlite-installed phase", journal, err)
	}
	if target.db != nil || target.writer != nil || target.graph != nil {
		t.Fatal("original materializer was unexpectedly reopened after restored Close failure")
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if err := restored.Close(); err != nil {
		t.Fatalf("close restored materializer after releasing test snapshot: %v", err)
	}
}

func openRestoreCloseTestSnapshot(t *testing.T, dbPath, value string, graphID int64) (*Materializer, []CheckpointFile, func()) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o700); err != nil {
		t.Fatal(err)
	}
	m, err := Open(dbPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for slot, sql := range []string{
		"CREATE TABLE restore_close_rows (value TEXT)",
		"INSERT INTO restore_close_rows VALUES ('" + value + "')",
	} {
		command, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: fmt.Sprintf("%s-sql-%d", value, slot+1), SQL: sql}})
		if err != nil {
			_ = m.Close()
			t.Fatal(err)
		}
		if err := m.Apply(ctx, uint64(slot+1), command); err != nil {
			_ = m.Close()
			t.Fatal(err)
		}
	}
	graph, err := types.EncodeGraphCommand(types.GraphCommand{RequestID: value + "-graph", Cypher: "CREATE (:RestoreClose {id: $id})", Args: map[string]any{"id": graphID}})
	if err != nil {
		_ = m.Close()
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 3, graph); err != nil {
		_ = m.Close()
		t.Fatal(err)
	}
	files, _, cleanup, err := m.CheckpointFilesAt(ctx)
	if err != nil {
		_ = m.Close()
		t.Fatal(err)
	}
	return m, files, cleanup
}

func assertRestoreCloseTestState(t *testing.T, ctx context.Context, m *Materializer, value string, graphID int64) {
	t.Helper()
	rows, err := m.QueryResult(ctx, "SELECT value FROM restore_close_rows", nil)
	if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != value {
		t.Fatalf("SQL restore-close state=%+v err=%v, want %q", rows.Rows, err, value)
	}
	graph, err := m.GraphQuery(ctx, "MATCH (n:RestoreClose) RETURN n.id", nil)
	if err != nil || len(graph.Rows) != 1 || graph.Rows[0][0] != graphID {
		t.Fatalf("Graph restore-close state=%+v err=%v, want %d", graph.Rows, err, graphID)
	}
}
