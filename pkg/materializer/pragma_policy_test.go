package materializer

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/ncruces/go-sqlite3"
	"github.com/ncruces/go-sqlite3/driver"
)

func TestWriterRejectsNodeLocalPragma(t *testing.T) {
	ctx := context.Background()
	m, err := Open(t.TempDir()+"/state.db", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	for i, query := range []string{
		"CREATE TABLE probe(value TEXT)",
		"INSERT INTO probe SELECT file FROM pragma_database_list WHERE name='main'",
	} {
		id := fmt.Sprint(i)
		value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: id, SQL: query}})
		if err != nil {
			t.Fatal(err)
		}
		if err = m.Apply(ctx, uint64(i+1), value); err != nil {
			t.Fatal(err)
		}
		receipt, ok, err := m.MutationReceipt(ctx, types.MutationSQL, id)
		if err != nil || !ok {
			t.Fatal(receipt, ok, err)
		}
		want := types.MutationCommitted
		if i == 1 {
			want = types.MutationRejected
		}
		if receipt.Status != want {
			t.Fatalf("%s: status=%s want=%s", query, receipt.Status, want)
		}
	}
}

func TestPragmaScopeRollbackAndLifecycle(t *testing.T) {
	cases := []struct {
		name       string
		setup      []string
		statements []types.SQLStatement
	}{
		{"scalar", nil, []types.SQLStatement{{SQL: "INSERT INTO probe VALUES((SELECT file FROM pragma_database_list WHERE name='main'))"}}},
		{"quoted", nil, []types.SQLStatement{{SQL: "INSERT INTO probe SELECT file FROM main.\"pragma_database_list\" WHERE name='main'"}}},
		{"returning", nil, []types.SQLStatement{{SQL: "SELECT file FROM pragma_database_list", WantRows: true}}},
		{"dml-returning", nil, []types.SQLStatement{{SQL: "INSERT INTO probe VALUES('rolled-back') RETURNING (SELECT file FROM pragma_database_list WHERE name='main')", WantRows: true}}},
		{"output-reference", nil, []types.SQLStatement{{SQL: "SELECT file FROM pragma_database_list WHERE name='main'", WantRows: true}, {SQL: "INSERT INTO probe VALUES(?)", Args: []any{nil}, OutputRefs: []types.SQLStatementOutputRef{{ArgIndex: 0, StatementIndex: 1, ColumnName: "file"}}}}},
		{"table-list", nil, []types.SQLStatement{{SQL: "SELECT * FROM pragma_table_list", WantRows: true}}},
		{"table-info", nil, []types.SQLStatement{{SQL: "SELECT * FROM pragma_table_info('probe')", WantRows: true}}},
		{"ctas", nil, []types.SQLStatement{{SQL: "CREATE TABLE leaked AS SELECT file FROM pragma_database_list"}}},
		{"view", []string{"CREATE VIEW v AS SELECT file FROM pragma_database_list", "CREATE VIEW v2 AS SELECT * FROM v"}, []types.SQLStatement{{SQL: "INSERT INTO probe SELECT * FROM v2"}}},
		{"trigger", []string{"CREATE TABLE source(value)", "CREATE TRIGGER tr AFTER INSERT ON source BEGIN INSERT INTO probe SELECT file FROM pragma_database_list; END"}, []types.SQLStatement{{SQL: "INSERT INTO source VALUES(1)"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			path := t.TempDir() + "/state.db"
			m, err := Open(path, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { m.Close() }()
			slot := uint64(0)
			apply := func(c types.SQLCommand, want types.MutationStatus) {
				t.Helper()
				slot++
				c.RequestID = fmt.Sprint(slot)
				v, e := types.EncodeSQLBatch([]types.SQLCommand{c})
				if e != nil {
					t.Fatal(e)
				}
				if e = m.Apply(ctx, slot, v); e != nil {
					t.Fatal(e)
				}
				r, ok, e := m.MutationReceipt(ctx, types.MutationSQL, c.RequestID)
				if e != nil || !ok || r.Status != want {
					t.Fatal(r, ok, e)
				}
				if want == types.MutationRejected && (r.ErrorCode != types.MutationErrorCodeExecutionFailed || r.RowsAffected != 0 || r.LastInsertID != 0) {
					t.Fatal(r)
				}
			}
			apply(types.SQLCommand{SQL: "CREATE TABLE probe(value TEXT)"}, types.MutationCommitted)
			for _, q := range tc.setup {
				apply(types.SQLCommand{SQL: q}, types.MutationCommitted)
			}
			for phase := 0; phase < 3; phase++ {
				if phase == 1 {
					m.writer.SetMaxIdleConns(0)
				}
				if phase == 2 {
					files, _, cleanup, e := m.CheckpointFilesAt(ctx)
					if e != nil {
						t.Fatal(e)
					}
					if e = m.RestoreCheckpoint(ctx, files); e != nil {
						cleanup()
						t.Fatal(e)
					}
					cleanup()
					if e = m.Close(); e != nil {
						t.Fatal(e)
					}
					m, e = Open(path, 1)
					if e != nil {
						t.Fatal(e)
					}
				}
				statements := append([]types.SQLStatement{{SQL: "INSERT INTO probe VALUES('must-roll-back')"}}, tc.statements...)
				apply(types.SQLCommand{Statements: statements}, types.MutationRejected)
				rows, e := m.QueryResult(ctx, "SELECT count(*) FROM probe", nil)
				if e != nil || rows.Rows[0][0] != int64(0) {
					t.Fatal(rows, e)
				}
				rows, e = m.QueryResult(ctx, "SELECT count(*) FROM sqlite_schema WHERE name='leaked'", nil)
				if e != nil || rows.Rows[0][0] != int64(0) {
					t.Fatal(rows, e)
				}
				rows, e = m.QueryResult(ctx, "SELECT file FROM pragma_database_list WHERE name='main'", nil)
				if e != nil || len(rows.Rows) != 1 {
					t.Fatal(rows, e)
				}
				apply(types.SQLCommand{SQL: "INSERT INTO probe VALUES('safe')"}, types.MutationCommitted)
				apply(types.SQLCommand{SQL: "DELETE FROM probe"}, types.MutationCommitted)
			}
		})
	}
}

func TestPragmaCachedShadowReprepare(t *testing.T) {
	ctx := context.Background()
	m, err := Open(t.TempDir()+"/state.db", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	queries := []string{"CREATE TABLE probe(value TEXT)", "CREATE TABLE pragma_database_list(file TEXT)", "INSERT INTO pragma_database_list VALUES('replicated')", "INSERT INTO probe SELECT file FROM pragma_database_list", "DROP TABLE pragma_database_list", "INSERT INTO probe SELECT file FROM pragma_database_list", "WITH pragma_database_list(file) AS (VALUES('cte')) INSERT INTO probe SELECT file FROM pragma_database_list"}
	commands := make([]types.SQLCommand, len(queries))
	for i, q := range queries {
		commands[i] = types.SQLCommand{RequestID: fmt.Sprint(i), SQL: q}
	}
	value, err := types.EncodeSQLBatch(commands)
	if err != nil {
		t.Fatal(err)
	}
	if err = m.Apply(ctx, 1, value); err != nil {
		t.Fatal(err)
	}
	for i := range queries {
		r, ok, e := m.MutationReceipt(ctx, types.MutationSQL, fmt.Sprint(i))
		want := types.MutationCommitted
		if i == 5 {
			want = types.MutationRejected
		}
		if e != nil || !ok || r.Status != want {
			t.Fatal(i, r, ok, e)
		}
	}
	rows, err := m.QueryResult(ctx, "SELECT value FROM probe ORDER BY value", nil)
	if err != nil || len(rows.Rows) != 2 || rows.Rows[0][0] != "cte" || rows.Rows[1][0] != "replicated" {
		t.Fatal(rows, err)
	}
}

func TestPragmaPolicySetupFailureAbortsAndDiscardsConnection(t *testing.T) {
	ctx := context.Background()
	m, err := Open(t.TempDir()+"/state.db", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	conn, err := m.writer.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Force the real inventory prepare to fail, without a mocked driver or setter.
	err = conn.Raw(func(raw any) error {
		return raw.(driver.Conn).Raw().SetAuthorizer(func(a sqlite3.AuthorizerActionCode, _, _, _, _ string) sqlite3.AuthorizerReturnCode {
			if a == sqlite3.AUTH_PRAGMA {
				return sqlite3.AUTH_DENY
			}
			return sqlite3.AUTH_OK
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = conn.Close(); err != nil {
		t.Fatal(err)
	}
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "retry", SQL: "CREATE TABLE recovered(id INTEGER)"}})
	if err != nil {
		t.Fatal(err)
	}
	if err = m.Apply(ctx, 1, value); !errors.Is(err, errSQLPolicyState) {
		t.Fatal(err)
	}
	if m.Tip() != 0 {
		t.Fatal("advanced tip")
	}
	if r, ok, e := m.MutationReceipt(ctx, types.MutationSQL, "retry"); e != nil || ok {
		t.Fatal(r, ok, e)
	}
	// Success proves the poisoned physical connection was discarded, not pooled.
	if err = m.Apply(ctx, 1, value); err != nil {
		t.Fatal(err)
	}
	if r, ok, e := m.MutationReceipt(ctx, types.MutationSQL, "retry"); e != nil || !ok || r.Status != types.MutationCommitted {
		t.Fatal(r, ok, e)
	}
}

func TestPragmaAllowsNativeAlterValidation(t *testing.T) {
	cases := []struct {
		name, create, alter string
		want                types.MutationStatus
	}{
		{"check", "CREATE TABLE t(id INTEGER)", "ALTER TABLE t ADD COLUMN x INTEGER CHECK(x>=0)", types.MutationCommitted},
		{"strict", "CREATE TABLE t(id INTEGER) STRICT", "ALTER TABLE t ADD COLUMN x INTEGER", types.MutationCommitted},
		{"generated", "CREATE TABLE t(id INTEGER)", "ALTER TABLE t ADD COLUMN x INTEGER GENERATED ALWAYS AS (id+1) VIRTUAL NOT NULL", types.MutationCommitted},
		{"invalid", "CREATE TABLE t(id INTEGER)", "ALTER TABLE t ADD COLUMN x INTEGER DEFAULT -1 CHECK(x>=0)", types.MutationRejected},
	}
	for _, tc := range cases {
		for phase := 0; phase < 3; phase++ {
			t.Run(fmt.Sprintf("%s/%d", tc.name, phase), func(t *testing.T) {
				ctx := context.Background()
				path := t.TempDir() + "/state.db"
				m, err := Open(path, 1)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { m.Close() }()
				slot := uint64(0)
				apply := func(q string, want types.MutationStatus) {
					t.Helper()
					slot++
					id := fmt.Sprint(slot)
					v, e := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: id, SQL: q}})
					if e != nil {
						t.Fatal(e)
					}
					if e = m.Apply(ctx, slot, v); e != nil {
						t.Fatal(e)
					}
					r, ok, e := m.MutationReceipt(ctx, types.MutationSQL, id)
					if e != nil || !ok || r.Status != want {
						t.Fatal(q, r, ok, e)
					}
				}
				apply(tc.create, types.MutationCommitted)
				apply("INSERT INTO t VALUES(1)", types.MutationCommitted)
				if phase == 1 {
					m.writer.SetMaxIdleConns(0)
				}
				if phase == 2 {
					files, _, cleanup, e := m.CheckpointFilesAt(ctx)
					if e != nil {
						t.Fatal(e)
					}
					if e = m.RestoreCheckpoint(ctx, files); e != nil {
						cleanup()
						t.Fatal(e)
					}
					cleanup()
					if e = m.Close(); e != nil {
						t.Fatal(e)
					}
					m, e = Open(path, 1)
					if e != nil {
						t.Fatal(e)
					}
				}
				apply(tc.alter, tc.want)
				apply("SELECT * FROM pragma_quick_check('t')", types.MutationRejected)
				apply("INSERT INTO t(id) VALUES(2)", types.MutationCommitted)
			})
		}
	}
}
