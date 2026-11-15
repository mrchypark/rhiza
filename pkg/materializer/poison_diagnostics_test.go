package materializer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/applydiag"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	sqlite3 "github.com/ncruces/go-sqlite3"
	sqlite3driver "github.com/ncruces/go-sqlite3/driver"
)

type poisonLockCheckingWriter struct {
	m      *Materializer
	mu     sync.Mutex
	lines  []string
	locked bool
}

func TestFirstSQLitePoisonInterruptedWriteKeepsPrimaryAndRollbackCode(t *testing.T) {
	ctx, stop := context.WithTimeout(context.Background(), 15*time.Second)
	defer stop()
	m, err := Open(filepath.Join(t.TempDir(), "state.sqlite"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	schema, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "schema", SQL: "CREATE TABLE probe(id INTEGER PRIMARY KEY)"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 1, schema); err != nil {
		t.Fatal(err)
	}
	started, release, exited := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	applyCtx, cancel := context.WithCancel(ctx)
	workerStarted := false
	defer func() {
		cancel()
		releaseOnce.Do(func() { close(release) })
		if workerStarted {
			select {
			case <-exited:
			case <-ctx.Done():
				t.Error("interrupted apply worker did not exit")
			}
		}
	}()
	conn, err := m.writer.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = conn.Raw(func(raw any) error {
		return raw.(sqlite3driver.Conn).Raw().CreateFunction("gate_interrupt", 0, 0, func(out sqlite3.Context, _ ...sqlite3.Value) {
			close(started)
			<-release
			out.ResultInt(1)
		})
	})
	closeErr := conn.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("function registration err=%v close=%v", err, closeErr)
	}
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "private-request", SQL: "WITH RECURSIVE seq(x) AS (SELECT gate_interrupt() UNION ALL SELECT x+1 FROM seq WHERE x<1000000) INSERT INTO probe SELECT x FROM seq"}})
	if err != nil {
		t.Fatal(err)
	}
	var recorder applydiag.Recorder
	result := make(chan error, 1)
	workerStarted = true
	go func() {
		defer close(exited)
		result <- m.ApplyBatch(applydiag.WithRecorder(applyCtx, &recorder), []quepaxa.DecidedValue{{Slot: 2, Value: value}})
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("SQLite callback not reached", ctx.Err())
	}
	cancel()
	releaseOnce.Do(func() { close(release) })
	select {
	case err = <-result:
		origin := recorder.Origin()
		if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrGraphCommitUnknown) || !origin.Known || origin.Trigger != applydiag.PoisonRollback || origin.PrimaryClass != applydiag.ErrorCanceled || origin.PrimarySite != applydiag.ApplyValue || !origin.FailedSlotKnown || origin.FailedSlot != 2 || origin.CleanupClass != applydiag.ErrorSQLite || !origin.CleanupCodeKnown || origin.CleanupCode != int(sqlite3.ERROR) || m.Tip() != 1 {
			t.Fatalf("interrupted write origin=%+v err=%v tip=%d", origin, err, m.Tip())
		}
	case <-ctx.Done():
		t.Fatal("interrupted apply did not return", ctx.Err())
	}
}

func (w *poisonLockCheckingWriter) Write(p []byte) (int, error) {
	if strings.Contains(string(p), "materializer sqlite poison:") {
		unlocked := w.m.mu.TryLock()
		if unlocked {
			w.m.mu.Unlock()
		}
		w.mu.Lock()
		w.locked = w.locked || !unlocked
		w.lines = append(w.lines, string(p))
		w.mu.Unlock()
	}
	return len(p), nil
}

func TestFirstSQLitePoisonRollbackKeepsOriginalAndLogsOutsideLock(t *testing.T) {
	ctx := context.Background()
	m, err := Open(filepath.Join(t.TempDir(), "state.sqlite"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	for i, stmt := range []string{"CREATE TABLE items(id INTEGER PRIMARY KEY)", "CREATE TRIGGER tr AFTER INSERT ON items WHEN new.id=2 BEGIN INSERT INTO items VALUES(9223372036854775807); SELECT RAISE(ROLLBACK,'private-payload'); END"} {
		value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: fmt.Sprint(i), SQL: stmt}})
		if err != nil {
			t.Fatal(err)
		}
		if err := m.Apply(ctx, uint64(i+1), value); err != nil {
			t.Fatal(err)
		}
	}
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "private-request", SQL: "INSERT INTO items VALUES(2)"}})
	if err != nil {
		t.Fatal(err)
	}
	barrier := types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{})
	var recorder applydiag.Recorder
	observed := applydiag.WithRecorder(ctx, &recorder)
	oldWriter := log.Writer()
	checker := &poisonLockCheckingWriter{m: m}
	log.SetOutput(checker)
	defer log.SetOutput(oldWriter)
	err = m.ApplyBatch(observed, []quepaxa.DecidedValue{{Slot: 3, Value: barrier}, {Slot: 4, Value: value}})
	if !errors.Is(err, ErrGraphCommitUnknown) || !errors.Is(err, sqlite3.ERROR) || m.Tip() != 2 {
		t.Fatalf("rollback failure: %v tip=%d", err, m.Tip())
	}
	origin := recorder.Origin()
	if !origin.Known || origin.Trigger != applydiag.PoisonRollback || origin.PrimarySite != applydiag.ApplyValue || !origin.FailedSlotKnown || origin.FailedSlot != 4 || !origin.TipKnown || origin.TipBeforeReset != 3 || origin.BatchFirst != 3 || origin.BatchLast != 4 || origin.CleanupClass == applydiag.ErrorNone {
		t.Fatalf("incorrect first origin: %+v", origin)
	}
	err = m.ApplyBatch(observed, []quepaxa.DecidedValue{{Slot: 3, Value: barrier}})
	if !errors.Is(err, ErrGraphCommitUnknown) || recorder.Site() != applydiag.Health || recorder.Origin() != origin {
		t.Fatalf("health guard changed origin: %v %+v", err, recorder.Origin())
	}
	var wg sync.WaitGroup
	results := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var rec applydiag.Recorder
			err := m.ApplyBatch(applydiag.WithRecorder(ctx, &rec), []quepaxa.DecidedValue{{Slot: 3, Value: barrier}})
			if !errors.Is(err, ErrGraphCommitUnknown) || rec.Origin() != origin {
				results <- fmt.Errorf("concurrent origin=%+v err=%v", rec.Origin(), err)
			}
		}()
	}
	wg.Wait()
	close(results)
	for err := range results {
		t.Error(err)
	}
	checker.mu.Lock()
	defer checker.mu.Unlock()
	if checker.locked || len(checker.lines) != 1 || len(checker.lines[0]) > 1024 || strings.Contains(checker.lines[0], "private") || !strings.Contains(checker.lines[0], "trigger=rollback") || !strings.Contains(checker.lines[0], "failed_slot=4") {
		t.Fatalf("bad first poison event: locked=%v lines=%q", checker.locked, checker.lines)
	}
}

func TestFirstSQLitePoisonCommitAndUnknownOrigin(t *testing.T) {
	ctx := context.Background()
	m, err := Open(filepath.Join(t.TempDir(), "state.sqlite"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	barrier := types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{})
	cause := errors.New("private-lost-ack")
	m.afterApplyCommitError = func() error { return cause }
	var recorder applydiag.Recorder
	observed := applydiag.WithRecorder(ctx, &recorder)
	err = m.ApplyBatch(observed, []quepaxa.DecidedValue{{Slot: 1, Value: barrier}})
	origin := recorder.Origin()
	if !errors.Is(err, cause) || !errors.Is(err, ErrGraphCommitUnknown) || !origin.Known || origin.Trigger != applydiag.PoisonCommit || origin.PrimarySite != applydiag.Commit || origin.FailedSlotKnown || !origin.TipKnown || origin.TipBeforeReset != 1 || origin.CleanupClass != applydiag.ErrorUnknown {
		t.Fatalf("commit origin=%+v err=%v", origin, err)
	}
	if err := m.ApplyBatch(observed, []quepaxa.DecidedValue{{Slot: 1, Value: barrier}}); !errors.Is(err, ErrGraphCommitUnknown) || recorder.Origin() != origin {
		t.Fatalf("origin overwritten: %+v err=%v", recorder.Origin(), err)
	}
	other, err := Open(filepath.Join(t.TempDir(), "other.sqlite"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Close() })
	other.commitUnknown = true // Simulates an origin not observed in this incarnation.
	if err := other.ApplyBatch(observed, []quepaxa.DecidedValue{{Slot: 1, Value: barrier}}); !errors.Is(err, ErrGraphCommitUnknown) || recorder.Origin().Known {
		t.Fatalf("invented origin: %+v err=%v", recorder.Origin(), err)
	}
}
