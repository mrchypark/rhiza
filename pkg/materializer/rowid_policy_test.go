package materializer

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/ncruces/go-sqlite3/driver"
)

// An ordinary rowid table containing MaxInt64 enters SQLite's random rowid
// allocation path on its next implicit insert. Keep that state unrepresentable.
func TestSQLRejectsExhaustedRowID(t *testing.T) {
	ctx := context.Background()
	m, err := Open(t.TempDir()+"/state.db", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	for i, query := range []string{
		"CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)",
		"INSERT INTO items VALUES(9223372036854775807, 'sentinel')",
	} {
		id := fmt.Sprint(i)
		value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: id, SQL: query}})
		if err != nil {
			t.Fatal(err)
		}
		if err := m.Apply(ctx, uint64(i+1), value); err != nil {
			t.Fatal(err)
		}
		receipt, ok, err := m.MutationReceipt(ctx, types.MutationSQL, id)
		if err != nil || !ok {
			t.Fatalf("receipt=%+v found=%v err=%v", receipt, ok, err)
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

func TestRowIDPolicyCommandRollback(t *testing.T) {
	zero := int64(0)
	cases := []struct {
		name       string
		setup      []string
		statements []types.SQLStatement
	}{
		{"bound", nil, []types.SQLStatement{{SQL: "INSERT INTO items VALUES(?,2)", Args: []any{int64(9223372036854775807)}}}},
		{"computed", nil, []types.SQLStatement{{SQL: "INSERT INTO items VALUES(9223372036854775806+1,2)"}}},
		{"update", nil, []types.SQLStatement{{SQL: "UPDATE items SET id=9223372036854775807"}}},
		{"upsert", nil, []types.SQLStatement{{SQL: "INSERT INTO items VALUES(1,3) ON CONFLICT(id) DO UPDATE SET id=9223372036854775807"}}},
		{"replace", nil, []types.SQLStatement{{SQL: "INSERT OR REPLACE INTO items VALUES(9223372036854775807,3)"}}},
		{"multirow", nil, []types.SQLStatement{{SQL: "INSERT INTO items VALUES(2,3),(9223372036854775807,4)"}}},
		{"insert-select", nil, []types.SQLStatement{{SQL: "INSERT INTO items SELECT 9223372036854775807,value FROM items"}}},
		{"whole-command", nil, []types.SQLStatement{{SQL: "CREATE TABLE undone(x)"}, {SQL: "INSERT INTO items VALUES(2,4)"}, {SQL: "INSERT INTO items VALUES(9223372036854775807,4)"}}},
		{"returning", nil, []types.SQLStatement{{SQL: "INSERT INTO items VALUES(9223372036854775807,4) RETURNING id", WantRows: true}}},
		{"returning-not-requested", nil, []types.SQLStatement{{SQL: "INSERT INTO items VALUES(9223372036854775807,4) RETURNING id"}}},
		{"returning-precondition", nil, []types.SQLStatement{{SQL: "INSERT INTO items VALUES(9223372036854775807,4) RETURNING id", WantRows: true, ExpectedReturnedRows: &zero}}},
		{"affected-precondition", nil, []types.SQLStatement{{SQL: "INSERT INTO items VALUES(9223372036854775807,4)", ExpectedRowsAffected: &zero}}},
		{"transient-trigger", []string{"CREATE TRIGGER tr AFTER INSERT ON items WHEN new.id=2 BEGIN INSERT INTO items VALUES(9223372036854775807,3); INSERT INTO items(value) VALUES(4); DELETE FROM items WHERE id=9223372036854775807; END"}, []types.SQLStatement{{SQL: "INSERT INTO items VALUES(2,2)"}}},
		{"before-trigger", []string{"CREATE TRIGGER tr BEFORE UPDATE ON items BEGIN INSERT INTO items VALUES(9223372036854775807,3); END"}, []types.SQLStatement{{SQL: "UPDATE items SET value=2 WHERE id=1"}}},
		{"view-trigger", []string{"CREATE VIEW v AS SELECT * FROM items", "CREATE TRIGGER tr INSTEAD OF INSERT ON v BEGIN INSERT INTO items VALUES(9223372036854775807,new.value); END"}, []types.SQLStatement{{SQL: "INSERT INTO v VALUES(2,3)"}}},
		{"late-fail", []string{"CREATE TRIGGER tr AFTER INSERT ON items WHEN new.id=2 BEGIN INSERT INTO items VALUES(9223372036854775807,3); SELECT RAISE(FAIL,'later'); END"}, []types.SQLStatement{{SQL: "INSERT INTO items VALUES(2,2)"}}},
		{"near-limit", []string{"INSERT INTO items VALUES(9223372036854775806,2)"}, []types.SQLStatement{{SQL: "INSERT INTO items(value) VALUES(3)"}}},
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
			apply := func(command types.SQLCommand, want string) types.MutationReceipt {
				t.Helper()
				slot++
				if command.RequestID == "" {
					command.RequestID = fmt.Sprint(slot)
				}
				value, e := types.EncodeSQLBatch([]types.SQLCommand{command})
				if e != nil {
					t.Fatal(e)
				}
				if e = m.Apply(ctx, slot, value); e != nil {
					t.Fatal(e)
				}
				r, ok, e := m.MutationReceipt(ctx, types.MutationSQL, command.RequestID)
				if e != nil || !ok || string(r.Status) != want {
					t.Fatalf("receipt=%+v found=%v err=%v want=%s", r, ok, e, want)
				}
				return r
			}
			apply(types.SQLCommand{SQL: "CREATE TABLE items(id INTEGER PRIMARY KEY,value INTEGER)"}, string(types.MutationCommitted))
			apply(types.SQLCommand{SQL: "INSERT INTO items VALUES(1,1)"}, string(types.MutationCommitted))
			for _, q := range tc.setup {
				apply(types.SQLCommand{SQL: q}, string(types.MutationCommitted))
			}
			before, e := m.QueryResult(ctx, "SELECT id,value FROM items ORDER BY id", nil)
			if e != nil {
				t.Fatal(e)
			}
			command := types.SQLCommand{RequestID: "violation", Statements: tc.statements}
			receipt := apply(command, string(types.MutationRejected))
			if receipt.ErrorCode != types.MutationErrorCodeExecutionFailed || receipt.LastInsertID != 0 || receipt.RowsAffected != 0 {
				t.Fatalf("partial result: %+v", receipt)
			}
			fingerprint, e := types.SQLFingerprint(command)
			if e != nil {
				t.Fatal(e)
			}
			_, result, found, matches, e := m.SQLRequestResultFingerprint(ctx, command.RequestID, fingerprint, true)
			if e != nil || !found || !matches || len(result.Statements) != 0 {
				t.Fatalf("leaked result: %+v %v", result, e)
			}
			if e = m.Close(); e != nil {
				t.Fatal(e)
			}
			m, e = Open(path, 1)
			if e != nil {
				t.Fatal(e)
			}
			again := apply(command, string(types.MutationRejected))
			if again != receipt {
				t.Fatalf("retry changed receipt: %+v %+v", receipt, again)
			}
			after, e := m.QueryResult(ctx, "SELECT id,value FROM items ORDER BY id", nil)
			if e != nil || fmt.Sprint(before) != fmt.Sprint(after) {
				t.Fatalf("state changed: %+v %+v %v", before, after, e)
			}
			apply(types.SQLCommand{SQL: "INSERT INTO items VALUES(3,9)"}, string(types.MutationCommitted))
			if tc.name == "whole-command" {
				if _, e = m.QueryResult(ctx, "SELECT * FROM undone", nil); e == nil {
					t.Fatal("DDL survived rejected command")
				}
			}
		})
	}
}

func TestRowIDPolicyPreservesPayloadsAndSchemaLifecycle(t *testing.T) {
	ctx := context.Background()
	m, err := Open(t.TempDir()+"/state.db", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	for i, q := range []string{
		"CREATE TABLE items(id INTEGER PRIMARY KEY AUTOINCREMENT,value INTEGER)",
		"INSERT INTO items(id,value) VALUES(-1,9223372036854775807),(0,1),(1,1)",
		"INSERT INTO items(value) VALUES(2)",
		"CREATE TABLE wr(id INTEGER PRIMARY KEY) WITHOUT ROWID",
		"INSERT INTO wr VALUES(9223372036854775807)",
		"CREATE TABLE ordinary(id INT PRIMARY KEY)",
		"INSERT INTO ordinary VALUES(9223372036854775807)",
		"CREATE TABLE descending(id INTEGER PRIMARY KEY DESC)",
		"INSERT INTO descending VALUES(9223372036854775807)",
		"CREATE TABLE shadowed(rowid INTEGER,_rowid_ INTEGER,oid INTEGER)",
		"INSERT INTO shadowed VALUES(9223372036854775807,9223372036854775807,9223372036854775807)",
		"CREATE TABLE copied AS SELECT 9223372036854775807 AS id",
		"ALTER TABLE wr RENAME TO renamed",
		"DROP TABLE renamed",
		"CREATE TABLE renamed(id INTEGER PRIMARY KEY)",
		"INSERT INTO renamed VALUES(7)",
		"INSERT OR IGNORE INTO renamed VALUES(7)",
		"ALTER TABLE items RENAME TO auto_renamed",
		"DROP TABLE auto_renamed",
	} {
		id := fmt.Sprint(i)
		v, e := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: id, SQL: q}})
		if e != nil {
			t.Fatal(e)
		}
		if e = m.Apply(ctx, uint64(i+1), v); e != nil {
			t.Fatal(e)
		}
		r, ok, e := m.MutationReceipt(ctx, types.MutationSQL, id)
		if e != nil || !ok || r.Status != types.MutationCommitted {
			t.Fatalf("%s: %+v %v", q, r, e)
		}
	}
	files, _, cleanup, err := m.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if err = m.RestoreCheckpoint(ctx, files); err != nil {
		t.Fatal(err)
	}
	rows, err := m.QueryResult(ctx, "SELECT id FROM copied", nil)
	if err != nil || rows.Rows[0][0] != int64(9223372036854775807) {
		t.Fatal(rows, err)
	}
}

func TestRowIDPolicyLostSavepointFailsApply(t *testing.T) {
	ctx := context.Background()
	m, err := Open(t.TempDir()+"/state.db", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	for i, q := range []string{"CREATE TABLE items(id INTEGER PRIMARY KEY)", "CREATE TRIGGER tr AFTER INSERT ON items WHEN new.id=2 BEGIN INSERT INTO items VALUES(9223372036854775807); SELECT RAISE(ROLLBACK,'later'); END"} {
		v, e := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: fmt.Sprint(i), SQL: q}})
		if e != nil {
			t.Fatal(e)
		}
		if e = m.Apply(ctx, uint64(i+1), v); e != nil {
			t.Fatal(e)
		}
	}
	v, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "lost-savepoint", SQL: "INSERT INTO items VALUES(2)"}})
	if err != nil {
		t.Fatal(err)
	}
	if err = m.Apply(ctx, 3, v); err == nil {
		t.Fatal("lost transaction accepted")
	}
	if m.Tip() != 2 {
		t.Fatal("tip advanced")
	}
	if _, ok, e := m.MutationReceipt(ctx, types.MutationSQL, "lost-savepoint"); e != nil || ok {
		t.Fatal("false receipt", ok, e)
	}
	rows, err := m.QueryResult(ctx, "SELECT COUNT(*) FROM items", nil)
	if err != nil || rows.Rows[0][0] != int64(0) {
		t.Fatal(rows, err)
	}
	good, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "safe", SQL: "INSERT INTO items VALUES(3)"}})
	if err != nil {
		t.Fatal(err)
	}
	if err = m.Apply(ctx, 3, good); err != nil {
		t.Fatal(err)
	}
}

func TestRowIDPolicyRejectsUnsupportedWrites(t *testing.T) {
	ctx := context.Background()
	m, err := Open(t.TempDir()+"/state.db", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	schema, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "schema", SQL: "CREATE TABLE items(id INTEGER PRIMARY KEY AUTOINCREMENT)"}})
	if err != nil {
		t.Fatal(err)
	}
	if err = m.Apply(ctx, 1, schema); err != nil {
		t.Fatal(err)
	}
	for i, q := range []string{
		"CREATE VIRTUAL TABLE docs USING fts5(body)",
		"CREATE VIRTUAL TABLE docs USING fts5(body,content='',columnsize=0)",
		"CREATE VIRTUAL TABLE docs USING fts5(body,content='items')",
		"INSERT INTO sqlite_sequence(name,seq) VALUES('items',9223372036854775807)",
		"UPDATE sqlite_sequence SET rowid=9223372036854775807",
		"DELETE FROM sqlite_sequence",
		"ANALYZE",
	} {
		id := fmt.Sprint(i)
		v, e := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: id, SQL: q}})
		if e != nil {
			t.Fatal(e)
		}
		if e = m.Apply(ctx, uint64(i+2), v); e != nil {
			t.Fatal(e)
		}
		r, ok, e := m.MutationReceipt(ctx, types.MutationSQL, id)
		if e != nil || !ok || r.Status != types.MutationRejected {
			t.Fatalf("%s: %+v %v", q, r, e)
		}
	}
}

func TestRowIDPolicyRejectsVirtualPhysicalState(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/state.db"
	m, err := Open(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	// Simulates unsupported external physical state, not the guarded import API.
	if _, err = m.db.Exec("CREATE VIRTUAL TABLE docs USING fts5(body)"); err != nil {
		t.Fatal(err)
	}
	if err = m.Close(); err != nil {
		t.Fatal(err)
	}
	if other, err := Open(path, 1); !errors.Is(err, sqlpolicy.ErrIncompatible) {
		if other != nil {
			other.Close()
		}
		t.Fatal(err)
	}
	live, err := Open(t.TempDir()+"/live.db", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	if err = live.RestoreCheckpoint(ctx, []CheckpointFile{{Role: CheckpointSQLite, Path: path}, {Role: CheckpointGraphData, Path: "must-not-install"}}); !errors.Is(err, sqlpolicy.ErrIncompatible) {
		t.Fatal(err)
	}
}

func TestRowIDPolicyNativeVirtualWriteAuthorization(t *testing.T) {
	ctx := context.Background()
	m, err := Open(t.TempDir()+"/state.db", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if _, err = m.db.Exec("CREATE VIRTUAL TABLE docs USING fts5(body); CREATE TABLE source(body); CREATE TRIGGER tr AFTER INSERT ON source BEGIN INSERT INTO docs VALUES(new.body); END"); err != nil {
		t.Fatal(err)
	}
	conn, err := m.writer.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err = conn.Raw(func(raw any) error { return setSQLAuthorizer(raw.(driver.Conn).Raw(), true, false, false) }); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{"INSERT INTO docs VALUES('x')", "UPDATE docs SET body='x'", "DELETE FROM docs", "INSERT INTO source VALUES('x')", "DELETE FROM docs_data"} {
		if _, err = conn.ExecContext(ctx, q); err == nil {
			t.Fatalf("allowed %s", q)
		}
	}
}

func TestRowIDPolicyCachedStatementDoesNotLeakLatch(t *testing.T) {
	ctx := context.Background()
	m, err := Open(t.TempDir()+"/state.db", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	schema, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "schema", SQL: "CREATE TABLE items(id INTEGER PRIMARY KEY)"}})
	if err != nil {
		t.Fatal(err)
	}
	if err = m.Apply(ctx, 1, schema); err != nil {
		t.Fatal(err)
	}
	value, err := types.EncodeSQLBatch([]types.SQLCommand{
		{RequestID: "max", SQL: "INSERT INTO items VALUES(?)", Args: []any{int64(9223372036854775807)}},
		{RequestID: "good", SQL: "INSERT INTO items VALUES(?)", Args: []any{int64(5)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = m.Apply(ctx, 2, value); err != nil {
		t.Fatal(err)
	}
	for id, want := range map[string]types.MutationStatus{"max": types.MutationRejected, "good": types.MutationCommitted} {
		r, ok, e := m.MutationReceipt(ctx, types.MutationSQL, id)
		if e != nil || !ok || r.Status != want {
			t.Fatal(r, ok, e)
		}
	}
	rows, err := m.QueryResult(ctx, "SELECT id FROM items", nil)
	if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != int64(5) {
		t.Fatal(rows, err)
	}
}

func TestPolicyCatalogNameDoesNotBreakReopen(t *testing.T) {
	for _, q := range []string{"CREATE TABLE pragma_table_list(id INTEGER PRIMARY KEY)", "CREATE VIEW pragma_table_list AS SELECT 1 AS id"} {
		t.Run(q, func(t *testing.T) {
			ctx := context.Background()
			path := t.TempDir() + "/state.db"
			m, err := Open(path, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { m.Close() }()
			v, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "catalog-name", SQL: q}})
			if err != nil {
				t.Fatal(err)
			}
			if err = m.Apply(ctx, 1, v); err != nil {
				t.Fatal(err)
			}
			receipt, ok, err := m.MutationReceipt(ctx, types.MutationSQL, "catalog-name")
			if err != nil || !ok || receipt.Status != types.MutationCommitted {
				t.Fatal(receipt, ok, err)
			}
			files, _, cleanup, err := m.CheckpointFilesAt(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer cleanup()
			if err = m.RestoreCheckpoint(ctx, files); err != nil {
				t.Fatal(err)
			}
			if err = m.Close(); err != nil {
				t.Fatal(err)
			}
			m, err = Open(path, 1)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = m.QueryResult(ctx, "SELECT id FROM pragma_table_list", nil); err != nil {
				t.Fatal(err)
			}
		})
	}
}
