//go:build rhiza_local_testhooks

package materializer

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/applydiag"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/qlog"
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
	wal, err := qlog.Open(filepath.Join(os.Getenv("RHIZA_RECEIPT_PROBE_DIR"), "wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	inputs := [][]byte{encode("schema", "CREATE TABLE probe(id INTEGER PRIMARY KEY)"), encode("baseline", "INSERT INTO probe VALUES(1)"), encode("first", "INSERT INTO probe VALUES(2)"), types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{})}
	for i, input := range inputs {
		if slot, _, err := core.Propose(ctx, input); err != nil || slot != quepaxa.Slot(i+1) {
			t.Fatalf("certify decision %d: slot=%d err=%v", i+1, slot, err)
		}
	}
	if err := core.EnsureDurableThrough(ctx, 4); err != nil {
		t.Fatal(err)
	}
	values, tip, err := core.DecisionsFrom(3, 2)
	if err != nil || tip != 4 || len(values) != 2 || values[0].Slot != 3 || values[1].Slot != 4 || len(values[0].Certificate) == 0 || len(values[1].Certificate) == 0 || !bytes.Equal(values[0].Value, inputs[2]) || !bytes.Equal(values[1].Value, inputs[3]) {
		t.Fatalf("certified suffix unavailable: tip=%d count=%d err=%v", tip, len(values), err)
	}
	if err := m.Apply(ctx, 1, inputs[0]); err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 2, inputs[1]); err != nil {
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
		// This test-only function is deliberately permitted inside the fixture trigger.
		return native.CreateFunction("receipt_gate", 0, sqlite3.INNOCUOUS, func(out sqlite3.Context, _ ...sqlite3.Value) {
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
	result := make(chan error, 1)
	running = true
	go func() {
		defer close(exited)
		result <- m.ApplyBatch(applydiag.WithRecorder(applyCtx, &recorder), values)
	}()
	select {
	case <-started:
	case earlyErr := <-result:
		<-exited
		running = false
		var sqliteCode sqlite3.ErrorCode
		known := errors.As(earlyErr, &sqliteCode)
		detail := "unavailable"
		var sqliteErr *sqlite3.Error
		if errors.As(earlyErr, &sqliteErr) && strings.Contains(sqliteErr.Error(), "unsafe use of receipt_gate()") {
			detail = "unsafe_receipt_gate"
		}
		t.Fatalf("apply returned before receipt gate: success=%t site=%s sqlite_code_known=%t sqlite_code=%d sqlite_fixture_detail=%q canceled=%t deadline=%t unknown=%t", earlyErr == nil, recorder.Site(), known, sqliteCode, detail, errors.Is(earlyErr, context.Canceled), errors.Is(earlyErr, context.DeadlineExceeded), errors.Is(earlyErr, ErrGraphCommitUnknown))
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
	if !errors.Is(applyErr, sqlite3.INTERRUPT) || errors.Is(applyErr, ErrGraphCommitUnknown) || origin.Known || m.Tip() != 2 {
		t.Fatalf("receipt interruption: err=%v origin=%+v tip=%d", applyErr, origin, m.Tip())
	}
	if err := m.Health(ctx); err != nil {
		t.Fatalf("confirmed auto-rollback poisoned materializer: %v", err)
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
	// Remove only the test fixture trigger before replaying the original suffix.
	if _, err := reopened.writer.ExecContext(ctx, "DROP TRIGGER receipt_gate_trigger"); err != nil {
		t.Fatal(err)
	}
	replay, replayTip, err := core.DecisionsFrom(3, 2)
	if err != nil || replayTip != 4 || len(replay) != len(values) {
		t.Fatalf("replay source: tip=%d count=%d err=%v", replayTip, len(replay), err)
	}
	for i := range values {
		if replay[i].Slot != values[i].Slot || !bytes.Equal(replay[i].Value, values[i].Value) || !bytes.Equal(replay[i].Certificate, values[i].Certificate) {
			t.Fatalf("certified replay changed at index %d", i)
		}
	}
	if err := reopened.ApplyBatch(ctx, replay); err != nil {
		t.Fatalf("apply certified replay: %v", err)
	}
	if receipt, found, err := reopened.MutationReceipt(ctx, types.MutationSQL, "first"); err != nil || !found || receipt.Status != types.MutationCommitted || receipt.Slot != 3 {
		t.Fatalf("replayed receipt=%+v found=%t err=%v", receipt, found, err)
	}
	rows, err = reopened.QueryResult(ctx, "SELECT id FROM probe ORDER BY id", nil)
	if err != nil || len(rows.Rows) != 2 || rows.Rows[0][0] != int64(1) || rows.Rows[1][0] != int64(2) || reopened.Tip() != 4 {
		t.Fatalf("replayed state=%+v tip=%d err=%v", rows, reopened.Tip(), err)
	}
}
