package materializer

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/applydiag"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	sqlite3 "github.com/ncruces/go-sqlite3"
	sqlite3driver "github.com/ncruces/go-sqlite3/driver"
)

type injectedSQLiteCleanupError struct{}

func (injectedSQLiteCleanupError) Error() string { return "injected SQLite cleanup failure" }

func (injectedSQLiteCleanupError) Is(target error) bool {
	code, ok := target.(sqlite3.ErrorCode)
	return ok && code == sqlite3.ERROR
}

func (injectedSQLiteCleanupError) As(target any) bool {
	code, ok := target.(*sqlite3.ExtendedErrorCode)
	if ok {
		*code = sqlite3.ExtendedErrorCode(sqlite3.ERROR)
	}
	return ok
}

func injectRollbackCleanupError(t *testing.T, m *Materializer, cause error) {
	t.Helper()
	ctx := context.Background()
	conn, err := m.writer.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Raw(func(raw any) error {
		raw.(*rollbackWitnessConn).setRollbackCleanupError(cause)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteAutoRollbackSkipsNativeRollback(t *testing.T) {
	ctx := context.Background()
	db, err := openSQLite(":memory:", true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1);
		CREATE TRIGGER rollback_once BEFORE INSERT ON items WHEN new.id = 2 BEGIN SELECT RAISE(ROLLBACK, 'rollback-once'); END`); err != nil {
		t.Fatal(err)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO items VALUES(2)`); err == nil {
		t.Fatal("trigger did not fail the write")
	}
	rollbackSeen := false
	if err := conn.Raw(func(raw any) error {
		return raw.(sqlite3driver.Conn).Raw().SetAuthorizer(func(action sqlite3.AuthorizerActionCode, object, _, _, _ string) sqlite3.AuthorizerReturnCode {
			if action == sqlite3.AUTH_TRANSACTION && strings.EqualFold(object, "ROLLBACK") {
				rollbackSeen = true
				return sqlite3.AuthorizerReturnCode(99)
			}
			return sqlite3.AUTH_OK
		})
	}); err != nil {
		t.Fatal(err)
	}
	var witness *rollbackWitnessConn
	if err := conn.Raw(func(raw any) error {
		witness = raw.(*rollbackWitnessConn)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !witness.armAutoRollback() {
		t.Fatal("automatic rollback was not witnessed")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("redundant rollback was not skipped: %v", err)
	}
	if rollbackSeen {
		t.Fatal("native ROLLBACK was issued")
	}
}

func TestSQLiteActiveTransactionRollbackFailureIsNotSkipped(t *testing.T) {
	ctx := context.Background()
	db, err := openSQLite(":memory:", true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)`); err != nil {
		t.Fatal(err)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO items VALUES(1)`); err == nil {
		t.Fatal("duplicate write succeeded")
	}
	var actions []sqlite3.AuthorizerActionCode
	var objects []string
	if err := conn.Raw(func(raw any) error {
		return raw.(sqlite3driver.Conn).Raw().SetAuthorizer(func(action sqlite3.AuthorizerActionCode, object, _, _, _ string) sqlite3.AuthorizerReturnCode {
			actions = append(actions, action)
			objects = append(objects, object)
			if action == sqlite3.AUTH_TRANSACTION {
				return sqlite3.AuthorizerReturnCode(99)
			}
			return sqlite3.AUTH_OK
		})
	}); err != nil {
		t.Fatal(err)
	}
	var witness *rollbackWitnessConn
	if err := conn.Raw(func(raw any) error {
		witness = raw.(*rollbackWitnessConn)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if witness.armAutoRollback() {
		t.Fatal("active transaction was incorrectly witnessed as rolled back")
	}
	err = tx.Rollback()
	if err == nil {
		t.Fatalf("rollback failure was skipped; actions=%v objects=%q", actions, objects)
	}
}

func TestConfirmedRollbackRetiresConnectionAfterStatementCloseFailure(t *testing.T) {
	ctx := context.Background()
	m, err := Open(t.TempDir()+"/state.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	for slot, command := range []types.SQLCommand{
		{RequestID: "schema", SQL: "CREATE TABLE items(id INTEGER PRIMARY KEY)"},
		{RequestID: "trigger", SQL: "CREATE TRIGGER rollback_once BEFORE INSERT ON items WHEN new.id=2 BEGIN SELECT RAISE(ROLLBACK,'rollback-once'); END"},
	} {
		value, err := types.EncodeSQLBatch([]types.SQLCommand{command})
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
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{SQL: "INSERT INTO items VALUES(2)"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 3, value); err == nil || errors.Is(err, ErrGraphCommitUnknown) {
		t.Fatalf("confirmed rollback with hidden statement-close failure: %v", err)
	}
	if !old.statementCloseFailed() {
		t.Fatal("statement close failure was not injected")
	}
	if err := old.closedError(); err != nil {
		t.Fatalf("retired physical connection close: %v", err)
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
		t.Fatal("retired physical connection was reused")
	}
}

func TestConfirmedRollbackPhysicalCloseFailurePoisonsAndRetiresConnection(t *testing.T) {
	ctx := context.Background()
	m, err := Open(t.TempDir()+"/state.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	for slot, command := range []types.SQLCommand{
		{RequestID: "schema", SQL: "CREATE TABLE items(id INTEGER PRIMARY KEY)"},
		{RequestID: "trigger", SQL: "CREATE TRIGGER rollback_once BEFORE INSERT ON items WHEN new.id=2 BEGIN SELECT RAISE(ROLLBACK,'rollback-once'); END"},
	} {
		value, err := types.EncodeSQLBatch([]types.SQLCommand{command})
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
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{SQL: "INSERT INTO items VALUES(2)"}})
	if err != nil {
		t.Fatal(err)
	}
	var recorder applydiag.Recorder
	err = m.ApplyBatch(applydiag.WithRecorder(ctx, &recorder), []quepaxa.DecidedValue{{Slot: 3, Value: value}})
	origin := recorder.Origin()
	if !errors.Is(err, ErrGraphCommitUnknown) || !errors.Is(err, sqlite3.BUSY) || !strings.Contains(err.Error(), "close apply connection:") || !origin.Known || origin.Trigger != applydiag.PoisonRollback || origin.CleanupClass != applydiag.ErrorSQLite || !origin.CleanupCodeKnown || origin.CleanupCode != int(sqlite3.BUSY) || m.Tip() != 2 {
		t.Fatalf("physical close failure err=%v origin=%+v tip=%d", err, origin, m.Tip())
	}
	if closeErr := old.closedError(); !errors.Is(closeErr, sqlite3.BUSY) {
		t.Fatalf("native close error=%v, want SQLITE_BUSY", closeErr)
	}
	if err := m.Health(ctx); !errors.Is(err, ErrGraphCommitUnknown) {
		t.Fatalf("close failure did not poison materializer: %v", err)
	}
	if err := m.Apply(ctx, 3, value); !errors.Is(err, ErrGraphCommitUnknown) {
		t.Fatalf("same-instance refusal: %v", err)
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
}
