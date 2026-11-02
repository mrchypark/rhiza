package materializer

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ncruces/go-sqlite3"
	sqlite3driver "github.com/ncruces/go-sqlite3/driver"
)

// checkpointBarrier pauses the actual pinned driver during preparation of its
// real checkpoint statement, after database/sql has begun Exec on a connection.
func checkpointBarrier(t *testing.T, db *sql.DB) (<-chan struct{}, func()) {
	t.Helper()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	err = conn.Raw(func(raw any) error {
		return raw.(sqlite3driver.Conn).Raw().SetAuthorizer(func(action sqlite3.AuthorizerActionCode, object, _, _, _ string) sqlite3.AuthorizerReturnCode {
			if action == sqlite3.AUTH_PRAGMA && strings.EqualFold(object, "wal_checkpoint") {
				select {
				case <-entered:
				default:
					close(entered)
				}
				<-release
			}
			return sqlite3.AUTH_OK
		})
	})
	closeErr := conn.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("install checkpoint barrier: %v / %v", err, closeErr)
	}
	return entered, unblock
}

func waitCheckpointSignal(t *testing.T, signal <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not reach channel barrier", label)
	}
}

// This is a current pinned-driver counterexample, not a claim of file damage.
func TestSQLiteCheckpointCloseDoesNotDrainActiveExecution(t *testing.T) {
	db, err := openSQLite("file:"+filepath.Join(t.TempDir(), "checkpoint.db")+"?_pragma=journal_mode(wal)", false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE pending(v); INSERT INTO pending VALUES(1)`); err != nil {
		t.Fatal(err)
	}
	entered, unblock := checkpointBarrier(t, db)
	executed := make(chan error, 1)
	go func() { _, err := db.Exec(`PRAGMA wal_checkpoint(PASSIVE)`); executed <- err }()
	waitCheckpointSignal(t, entered, "real checkpoint Exec")
	closed := make(chan struct{})
	go func() { _ = db.Close(); close(closed) }()
	waitCheckpointSignal(t, closed, "sql.DB.Close with checkpoint still held")
	select {
	case err := <-executed:
		t.Fatalf("checkpoint finished before release: %v", err)
	default:
	}
	unblock()
	select {
	case err := <-executed:
		if err != nil {
			t.Fatalf("actual checkpoint after Close: %v", err)
		}
		t.Log("actual pinned-driver checkpoint remained active after sql.DB.Close returned and completed only after release")
	case <-time.After(5 * time.Second):
		t.Fatal("released checkpoint did not finish")
	}
}

func TestCheckpointRetirementWaitsForActiveExecution(t *testing.T) {
	m, err := Open(filepath.Join(t.TempDir(), "checkpoint.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	entered, unblock := checkpointBarrier(t, m.db)
	t.Cleanup(func() { unblock(); _ = m.Close() })
	waitCheckpointSignal(t, entered, "actual background checkpoint")
	locked, retired := make(chan struct{}), make(chan struct{})
	go func() {
		m.mu.Lock()
		close(locked)
		_ = m.closeConnections()
		close(retired)
		m.mu.Unlock()
	}()
	waitCheckpointSignal(t, locked, "retirement write lock")
	select {
	case <-retired:
		t.Fatal("connections retired while actual old checkpoint Exec was still held")
	case <-time.After(100 * time.Millisecond):
		// The checkpoint is held by a channel, not a timed workload. The bound
		// here only checks that retirement has not crossed that known barrier.
	}
	unblock()
	waitCheckpointSignal(t, retired, "retirement after checkpoint release")
}

func TestAdoptDrainsSourceWorkersBeforeReturning(t *testing.T) {
	target, err := Open(filepath.Join(t.TempDir(), "target.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = target.Close() })
	source, err := Open(filepath.Join(t.TempDir(), "source.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	entered, unblock := checkpointBarrier(t, source.db)
	t.Cleanup(func() { unblock(); _ = source.Close() })
	waitCheckpointSignal(t, entered, "actual source checkpoint")
	// A tracked notification activity pins the existing drain WaitGroup. This
	// tests the lifecycle wait, not application notification delivery semantics.
	notifyStopped, notifyRelease, activityDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseNotify := func() { releaseOnce.Do(func() { close(notifyRelease) }) }
	t.Cleanup(releaseNotify)
	source.notifyWG.Add(1)
	go func() {
		<-source.notifyStop
		close(notifyStopped)
		<-notifyRelease
		close(activityDone)
		source.notifyWG.Done()
	}()
	returned := make(chan struct{})
	go func() {
		target.mu.Lock()
		_ = target.closeConnections()
		target.adopt(source)
		target.mu.Unlock()
		close(returned)
	}()
	waitCheckpointSignal(t, source.walCheckpointStop, "source stop requested")
	select {
	case <-returned:
		t.Fatal("adopt returned with source checkpoint held")
	default:
	}
	unblock()
	waitCheckpointSignal(t, notifyStopped, "source notification drain requested")
	select {
	case <-returned:
		t.Fatal("adopt returned with tracked notification activity held")
	default:
	}
	releaseNotify()
	waitCheckpointSignal(t, returned, "adopt after both source drains")
	select {
	case <-activityDone:
	default:
		t.Fatal("adopt returned before source activity completed")
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if err := target.Health(context.Background()); err != nil {
		t.Fatal(err)
	}
}
