package materializer

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	sqlite3 "github.com/ncruces/go-sqlite3"
)

func TestUserTransactionControlDenied(t *testing.T) {
	ctx := context.Background()
	for _, query := range []string{"BEGIN", "COMMIT", "END", "END TRANSACTION", "end", "eNd TrAnSaCtIoN", "/* leading comment */ END", "-- leading comment\nend transaction", " \t/* first */ -- second\nEnD", "ROLLBACK", "SAVEPOINT user_sp", "RELEASE user_sp", "ROLLBACK TO user_sp", "SAVEPOINT rhiza_command", "RELEASE rhiza_command", "ROLLBACK TO rhiza_command"} {
		t.Run(query, func(t *testing.T) {
			if err := ValidateSQLCommand(types.SQLCommand{SQL: query}); err == nil {
				t.Fatalf("validation accepted %q", query)
			}
			m, err := Open(filepath.Join(t.TempDir(), "test.sqlite"), 1)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = m.Close() })
			conn, err := m.writer.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
				t.Fatal(err)
			}
			defer conn.ExecContext(ctx, "ROLLBACK")
			if _, err := conn.ExecContext(ctx, "SAVEPOINT internal_probe"); err != nil {
				t.Fatal(err)
			}
			stmt, err := conn.PrepareContext(ctx, query)
			if err != nil {
				t.Fatal(err)
			}
			defer stmt.Close()
			restore, err := beginUserSQL(conn, false)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = stmt.ExecContext(ctx); !errors.Is(err, sqlite3.AUTH) {
				t.Errorf("cached %q not denied by authorizer: %v", query, err)
			}
			if userStmt, err := conn.PrepareContext(ctx, query); !errors.Is(err, sqlite3.AUTH) {
				if userStmt != nil {
					_ = userStmt.Close()
				}
				t.Errorf("new %q not denied by authorizer: %v", query, err)
			}
			if err := restore(); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.ExecContext(ctx, "RELEASE internal_probe"); err != nil {
				t.Fatalf("trusted release: %v", err)
			}
		})
	}
}

func TestTransactionEndCannotCommitPartialBatch(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "test.sqlite")
	m, err := Open(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.Close() }()
	schema, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "schema", SQL: "CREATE TABLE probe(id INTEGER PRIMARY KEY)"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 1, schema); err != nil {
		t.Fatal(err)
	}
	command := types.SQLCommand{RequestID: "tx-control", Statements: []types.SQLStatement{{SQL: "INSERT INTO probe VALUES(7)"}, {SQL: "END"}}}
	value, err := types.EncodeSQLBatch([]types.SQLCommand{command})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 2, value); err == nil {
		t.Fatal("transaction control accepted")
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	rows, err := reopened.QueryResult(ctx, "SELECT count(*) FROM probe", nil)
	if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != int64(0) || reopened.Tip() != 1 {
		t.Fatalf("partial write survived: rows=%+v tip=%d err=%v", rows.Rows, reopened.Tip(), err)
	}
	if _, found, err := reopened.MutationReceipt(ctx, types.MutationSQL, "tx-control"); err != nil || found {
		t.Fatalf("invalid transaction left receipt: found=%t err=%v", found, err)
	}
	valid, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "valid", SQL: "INSERT INTO probe VALUES(CASE WHEN 1 THEN 9 ELSE 10 END)"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Apply(ctx, 2, valid); err != nil {
		t.Fatalf("valid CASE after reopen: %v", err)
	}
	if receipt, found, err := reopened.MutationReceipt(ctx, types.MutationSQL, "valid"); err != nil || !found || receipt.Status != types.MutationCommitted {
		t.Fatalf("invalid receipt: receipt=%+v found=%t err=%v", receipt, found, err)
	}
	rows, err = reopened.QueryResult(ctx, "SELECT id FROM probe", nil)
	if err != nil || len(rows.Rows) != 1 || len(rows.Rows[0]) != 1 || rows.Rows[0][0] != int64(9) {
		t.Fatalf("CASE inserted unexpected value: rows=%+v err=%v", rows.Rows, err)
	}
}

func TestInternalSavepointCachedStatementRestored(t *testing.T) {
	ctx := context.Background()
	m, err := Open(filepath.Join(t.TempDir(), "test.sqlite"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	conn, err := m.writer.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		t.Fatal(err)
	}
	defer conn.ExecContext(ctx, "ROLLBACK")
	stmt, err := conn.PrepareContext(ctx, "SAVEPOINT rhiza_command")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	if _, err := stmt.ExecContext(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "RELEASE rhiza_command"); err != nil {
		t.Fatal(err)
	}
	restore, err := beginUserSQL(conn, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stmt.ExecContext(ctx); !errors.Is(err, sqlite3.AUTH) {
		t.Fatalf("user cached savepoint: %v", err)
	}
	if err := restore(); err != nil {
		t.Fatal(err)
	}
	if _, err := stmt.ExecContext(ctx); err != nil {
		t.Fatalf("restored cached savepoint: %v", err)
	}
	if _, err := conn.ExecContext(ctx, "RELEASE rhiza_command"); err != nil {
		t.Fatal(err)
	}
}
