package materializer

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/ncruces/go-sqlite3"
	sqlite3driver "github.com/ncruces/go-sqlite3/driver"
)

type cancelAfterGraphTip struct {
	context.Context
	graph  *graphState
	target uint64
	done   chan struct{}
	once   sync.Once
}

func (c *cancelAfterGraphTip) Done() <-chan struct{} { return c.done }

func (c *cancelAfterGraphTip) Err() error {
	reached := false
	if c.graph.mu.TryRLock() {
		reached = c.graph.tip >= c.target
		c.graph.mu.RUnlock()
	}
	if reached {
		c.once.Do(func() { close(c.done) })
	}
	select {
	case <-c.done:
		return context.Canceled
	default:
		return nil
	}
}

func TestGraphControlRollbackPreservesDurableGraphDecision(t *testing.T) {
	m, err := OpenManagedSQLKVGraphWithAppendOnlyOriginals(filepath.Join(t.TempDir(), "sqlite.db"), 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	command := types.GraphCommand{RequestID: "first", Cypher: "CREATE (:Item {id: 'first'})"}
	graph, err := types.EncodeManagedGraphExecution(command)
	if err != nil {
		t.Fatal(err)
	}
	barrier := types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{})
	decisions := []quepaxa.DecidedValue{{Slot: 1, Value: graph}, {Slot: 2, Value: barrier}}
	ctx := &cancelAfterGraphTip{Context: context.Background(), graph: m.graph, target: 2, done: make(chan struct{})}
	if err := m.ApplyBatch(ctx, decisions); err == nil || ctx.Err() != context.Canceled || errors.Is(err, ErrGraphCommitUnknown) {
		t.Fatalf("apply=%v context=%v", err, ctx.Err())
	}
	if m.tip != 0 || m.graph.tip != 1 || m.graph.durableTip != 1 {
		t.Fatalf("sqlite=%d graph=%d durable=%d", m.tip, m.graph.tip, m.graph.durableTip)
	}
	if err := m.ApplyBatch(context.Background(), decisions); err != nil {
		t.Fatalf("exact replay: %v", err)
	}
	if receipt, found, err := m.GraphMutationReceipt(context.Background(), "first"); err != nil || !found || receipt.Slot != 1 {
		t.Fatalf("receipt=%+v found=%t err=%v", receipt, found, err)
	}
	if err := m.ApplyBatch(context.Background(), decisions); err != nil {
		t.Fatalf("duplicate replay: %v", err)
	}
}

func TestSQLiteAutoRollbackDoesNotPoison(t *testing.T) {
	ctx := context.Background()
	m, err := Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	for slot, query := range []string{
		"CREATE TABLE items(id INTEGER PRIMARY KEY)",
		"CREATE TRIGGER rollback_once BEFORE INSERT ON items WHEN new.id = 2 BEGIN SELECT RAISE(ROLLBACK, 'rollback-once'); END",
	} {
		value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: string(rune('a' + slot)), SQL: query}})
		if err != nil {
			t.Fatal(err)
		}
		if err := m.Apply(ctx, uint64(slot+1), value); err != nil {
			t.Fatal(err)
		}
	}
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "rollback", SQL: "INSERT INTO items VALUES(2)"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 3, value); !errors.Is(err, sqlite3.ERROR) || errors.Is(err, ErrGraphCommitUnknown) {
		t.Fatalf("auto-rollback error=%v", err)
	}
	if m.Tip() != 2 {
		t.Fatalf("tip=%d", m.Tip())
	}
	if err := m.Health(ctx); err != nil {
		t.Fatalf("health: %v", err)
	}
	if _, err := m.writer.ExecContext(ctx, "DROP TRIGGER rollback_once"); err != nil {
		t.Fatal(err)
	}
	value, err = types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "after-rollback", SQL: "INSERT INTO items VALUES(3)"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 3, value); err != nil {
		t.Fatalf("apply after confirmed rollback: %v", err)
	}
}

func TestRollbackErrorRemainsCommitUnknown(t *testing.T) {
	ctx := context.Background()
	m, err := Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	conn, err := m.writer.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var old *rollbackWitnessConn
	denied := false
	if err := conn.Raw(func(raw any) error {
		old = raw.(*rollbackWitnessConn)
		return raw.(sqlite3driver.Conn).Raw().SetAuthorizer(func(action sqlite3.AuthorizerActionCode, object, _, _, _ string) sqlite3.AuthorizerReturnCode {
			if !denied && action == sqlite3.AUTH_TRANSACTION && strings.EqualFold(object, "ROLLBACK") {
				denied = true
				return 99
			}
			return sqlite3.AUTH_OK
		})
	}); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	notify, err := types.EncodeNotifyCommand(types.NotifyCommand{RequestID: "rollback", Topic: "test"})
	if err != nil {
		t.Fatal(err)
	}
	barrier := types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{})
	err = m.ApplyBatch(ctx, []quepaxa.DecidedValue{{Slot: 1, Value: notify}, {Slot: 3, Value: barrier}})
	if !denied || !errors.Is(err, ErrGraphCommitUnknown) || !errors.Is(err, sqlite3.ERROR) {
		t.Fatalf("denied=%t error=%v sqlite-error=%t", denied, err, errors.Is(err, sqlite3.ERROR))
	}
	if err := m.Health(ctx); !errors.Is(err, ErrGraphCommitUnknown) {
		t.Fatalf("health=%v", err)
	}
	if old.autoRollbackSkipped() {
		t.Fatal("active transaction was incorrectly treated as auto-rolled-back")
	}
	current, err := m.writer.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	var replacement *rollbackWitnessConn
	if err := current.Raw(func(raw any) error {
		replacement = raw.(*rollbackWitnessConn)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if replacement == old || !replacement.Conn.Raw().GetAutocommit() {
		t.Fatal("failed rollback connection was not replaced")
	}
}

func TestAutoRollbackDiscardWithLeakedStatementReplacesConnection(t *testing.T) {
	ctx := context.Background()
	m, err := Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	for slot, query := range []string{
		"CREATE TABLE items(id INTEGER PRIMARY KEY)",
		"CREATE TRIGGER rollback_once BEFORE INSERT ON items WHEN new.id = 2 BEGIN SELECT RAISE(ROLLBACK, 'rollback-once'); END",
	} {
		value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: string(rune('a' + slot)), SQL: query}})
		if err != nil {
			t.Fatal(err)
		}
		if err := m.Apply(ctx, uint64(slot+1), value); err != nil {
			t.Fatal(err)
		}
	}
	conn, err := m.writer.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var old *rollbackWitnessConn
	var leaked *sqlite3.Stmt
	if err := conn.Raw(func(raw any) error {
		old = raw.(*rollbackWitnessConn)
		var tail string
		var err error
		leaked, tail, err = raw.(sqlite3driver.Conn).Raw().Prepare("SELECT 1")
		if err == nil && tail != "" {
			t.Fatalf("unexpected native statement tail %q", tail)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = leaked.Close()
		_ = old.Conn.Close()
	}()
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "rollback", SQL: "INSERT INTO items VALUES(2)"}})
	if err != nil {
		t.Fatal(err)
	}
	err = m.Apply(ctx, 3, value)
	if !errors.Is(err, sqlite3.ERROR) || errors.Is(err, ErrGraphCommitUnknown) || strings.Contains(err.Error(), "close apply connection") || m.Tip() != 2 {
		t.Fatalf("error=%v tip=%d", err, m.Tip())
	}
	current, err := m.writer.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	var replacement *rollbackWitnessConn
	if err := current.Raw(func(raw any) error {
		replacement = raw.(*rollbackWitnessConn)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if replacement == old {
		t.Fatal("physically failed connection was reused")
	}
	if !old.autoRollbackSkipped() {
		t.Fatal("confirmed native auto-rollback was not witnessed")
	}
}

func TestStatementCloseFailureRetiresConnection(t *testing.T) {
	ctx := context.Background()
	m, err := Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if _, err := m.writer.ExecContext(ctx, "CREATE TABLE items(id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.writer.ExecContext(ctx, "INSERT INTO items VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	conn, err := m.writer.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var old *rollbackWitnessConn
	if err := conn.Raw(func(raw any) error {
		old = raw.(*rollbackWitnessConn)
		old.setStatementCloseError(errors.New("injected statement close failure"))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{SQL: "INSERT INTO items VALUES(1)"}})
	if err != nil {
		t.Fatal(err)
	}
	err = m.Apply(ctx, 1, value)
	if !errors.Is(err, sqlite3.CONSTRAINT) || errors.Is(err, ErrGraphCommitUnknown) {
		t.Fatalf("aborted write error=%v", err)
	}
	current, err := m.writer.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	var replacement *rollbackWitnessConn
	if err := current.Raw(func(raw any) error {
		replacement = raw.(*rollbackWitnessConn)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !old.statementCloseFailed() {
		t.Fatalf("statement-close failure was not consumed: old=%p current=%p", old, replacement)
	}
	if old.autoRollbackSkipped() {
		t.Fatal("statement-close path was masked by automatic rollback")
	}
	if m.Tip() != 0 {
		t.Fatalf("tip=%d", m.Tip())
	}
	if replacement == old || !replacement.Conn.Raw().GetAutocommit() {
		t.Fatalf("statement-close failure connection was not replaced: old=%p current=%p", old, replacement)
	}
}

func TestRollbackWitnessStatementCloseReportsError(t *testing.T) {
	ctx := context.Background()
	db, err := openSQLite(":memory:", true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Raw(func(raw any) error {
		raw.(*rollbackWitnessConn).setStatementCloseError(errors.New("injected statement close failure"))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	if err := stmt.Close(); !strings.Contains(err.Error(), "injected statement close failure") {
		t.Fatalf("statement close error=%v", err)
	}
}

func TestSQLiteCommitConstraintErrorRemainsCommitUnknown(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sqlite.db")
	m, err := Open(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if _, err := m.writer.ExecContext(ctx, "PRAGMA foreign_keys=ON"); err != nil {
		t.Fatal(err)
	}
	for slot, query := range []string{
		"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
		"CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)",
	} {
		value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: string(rune('a' + slot)), SQL: query}})
		if err != nil {
			t.Fatal(err)
		}
		if err := m.Apply(ctx, uint64(slot+1), value); err != nil {
			t.Fatal(err)
		}
	}
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "orphan", SQL: "INSERT INTO child VALUES(1, 99)"}})
	if err != nil {
		t.Fatal(err)
	}
	err = m.Apply(ctx, 3, value)
	if !errors.Is(err, ErrGraphCommitUnknown) || !errors.Is(err, sqlite3.CONSTRAINT) || m.Tip() != 2 {
		t.Fatalf("error=%v tip=%d", err, m.Tip())
	}
	if err := m.Health(ctx); !errors.Is(err, ErrGraphCommitUnknown) {
		t.Fatalf("health=%v", err)
	}
}
