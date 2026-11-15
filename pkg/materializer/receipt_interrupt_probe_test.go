//go:build rhiza_local_testhooks

package materializer

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/applydiag"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	sqlite3 "github.com/ncruces/go-sqlite3"
	sqlite3driver "github.com/ncruces/go-sqlite3/driver"
)

func TestReceiptInterruptedTransactionProbe(t *testing.T) {
	if os.Getenv("RHIZA_RECEIPT_PROBE_CHILD") != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestReceiptInterruptedTransactionProbe$", "-test.v")
		cmd.Env = append(os.Environ(), "RHIZA_RECEIPT_PROBE_CHILD=1", "RHIZA_RECEIPT_PROBE_DIR="+t.TempDir())
		out, err := cmd.CombinedOutput()
		if ctx.Err() != nil {
			t.Fatalf("receipt probe child timed out: %v\n%s", ctx.Err(), out)
		}
		if err != nil {
			t.Fatalf("receipt probe child failed: %v\n%s", err, out)
		}
		return
	}
	ctx, stop := context.WithTimeout(context.Background(), 15*time.Second)
	defer stop()
	path := filepath.Join(os.Getenv("RHIZA_RECEIPT_PROBE_DIR"), "state.sqlite")
	m, err := Open(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	encode := func(id, sql string) []byte {
		v, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: id, SQL: sql}})
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	if err := m.Apply(ctx, 1, encode("schema", "CREATE TABLE probe(id INTEGER PRIMARY KEY)")); err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 2, encode("baseline", "INSERT INTO probe VALUES(1)")); err != nil {
		t.Fatal(err)
	}
	started, release, exited := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	var startedOnce sync.Once
	applyCtx, cancel := context.WithCancel(ctx)
	var running bool
	defer func() {
		cancel()
		once.Do(func() { close(release) })
		if running {
			select {
			case <-exited:
			case <-ctx.Done():
				t.Error("apply did not exit")
			}
		}
	}()
	conn, err := m.writer.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Raw(func(raw any) error {
		native := raw.(sqlite3driver.Conn).Raw()
		if err := native.SetAuthorizer(nil); err != nil {
			return err
		}
		return native.CreateFunction("receipt_gate", 0, 0, func(out sqlite3.Context, _ ...sqlite3.Value) {
			startedOnce.Do(func() { close(started) })
			<-release
			out.ResultInt(1)
		})
	}); err != nil {
		t.Fatal(err)
	}
	// Continue with bounded VM work after the gate so cancellation cannot be
	// missed just because the remaining INSERT takes fewer than 1000 opcodes.
	if _, err := conn.ExecContext(ctx, `CREATE TRIGGER receipt_gate_trigger BEFORE INSERT ON _rhiza_idempotency BEGIN
		SELECT receipt_gate();
		SELECT sum(value) FROM (WITH RECURSIVE seq(value) AS (VALUES(1) UNION ALL SELECT value+1 FROM seq WHERE value<100000) SELECT value FROM seq);
	END`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Raw(func(raw any) error { return setSQLAuthorizer(raw.(sqlite3driver.Conn).Raw(), true, false, false) }); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	var recorder applydiag.Recorder
	values := []quepaxa.DecidedValue{{Slot: 3, Value: encode("first", "INSERT INTO probe VALUES(2)")}, {Slot: 4, Value: types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{})}}
	result := make(chan error, 1)
	running = true
	go func() {
		defer close(exited)
		result <- m.ApplyBatch(applydiag.WithRecorder(applyCtx, &recorder), values)
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("receipt trigger not reached")
	}
	cancel()
	once.Do(func() { close(release) })
	var applyErr error
	select {
	case applyErr = <-result:
	case <-ctx.Done():
		t.Fatal("receipt apply did not return")
	}
	<-exited
	running = false
	origin := recorder.Origin()
	if !errors.Is(applyErr, sqlite3.INTERRUPT) || !errors.Is(applyErr, ErrGraphCommitUnknown) || !origin.Known || origin.Trigger != applydiag.PoisonRollback || origin.PrimarySite != applydiag.Receipts || !origin.PrimaryCodeKnown || origin.PrimaryCode != int(sqlite3.INTERRUPT) || !origin.CleanupCodeKnown || origin.CleanupCode != int(sqlite3.ERROR) || origin.BatchFirst != 3 || origin.BatchLast != 4 || !origin.TipKnown || origin.TipBeforeReset != 4 || m.Tip() != 2 {
		t.Fatalf("receipt interruption: err=%v origin=%+v tip=%d", applyErr, origin, m.Tip())
	}
	// The reserved connection is no longer owned here; a later connection's
	// native state cannot witness the failed transaction's error boundary.
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	rows, err := reopened.QueryResult(ctx, "SELECT id FROM probe", nil)
	if err != nil || len(rows.Rows) != 1 || len(rows.Rows[0]) != 1 || rows.Rows[0][0] != int64(1) || reopened.Tip() != 2 || reopened.StateTip() != 2 {
		t.Fatalf("reopen: rows=%+v tip=%d state_tip=%d err=%v", rows, reopened.Tip(), reopened.StateTip(), err)
	}
	if _, found, err := reopened.MutationReceipt(ctx, types.MutationSQL, "first"); err != nil || found {
		t.Fatalf("interrupted receipt found=%t err=%v", found, err)
	}
	if receipt, found, err := reopened.MutationReceipt(ctx, types.MutationSQL, "baseline"); err != nil || !found || receipt.Status != types.MutationCommitted {
		t.Fatalf("baseline receipt=%+v found=%t err=%v", receipt, found, err)
	}
}
