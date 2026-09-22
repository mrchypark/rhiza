package materializer

import (
	"context"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/ncruces/go-sqlite3/driver"
)

func TestWriterAuthorizerRejectsTempBelowAdmission(t *testing.T) {
	for _, query := range []string{
		"CREATE TEMP TABLE other(id)", "CREATE TEMP VIEW other AS SELECT 1", "CREATE TEMP TRIGGER other AFTER INSERT ON main.items BEGIN SELECT 1; END", "CREATE INDEX temp.other ON scratch(id)",
		"DROP TABLE temp.scratch", "DROP VIEW temp.view1", "DROP TRIGGER temp.trigger1", "DROP INDEX temp.index1",
		"SELECT * FROM scratch", "INSERT INTO scratch VALUES(2)", "UPDATE scratch SET id=3", "DELETE FROM scratch", "ALTER TABLE scratch RENAME TO other",
		"SELECT * FROM sqlite_temp_master", "SELECT * FROM sqlite_temp_schema",
	} {
		t.Run(query, func(t *testing.T) {
			ctx := context.Background()
			db, err := driver.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if _, err = conn.ExecContext(ctx, "CREATE TABLE items(id); CREATE TEMP TABLE scratch(id); CREATE INDEX temp.index1 ON scratch(id); CREATE TEMP VIEW view1 AS SELECT * FROM scratch; CREATE TEMP TRIGGER trigger1 AFTER INSERT ON main.items BEGIN SELECT 1; END"); err != nil {
				t.Fatal(err)
			}
			if err = conn.Raw(func(raw any) error { return setSQLAuthorizer(raw.(driver.Conn).Raw(), true, false, false) }); err != nil {
				t.Fatal(err)
			}
			if _, err = conn.ExecContext(ctx, query); err == nil || !strings.Contains(strings.ToLower(err.Error()), "authoriz") {
				t.Fatalf("expected authorization failure: %v", err)
			}
		})
	}
}

func TestTempPolicyPreservesAlterAndCheckpoint(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/state.db"
	m, err := Open(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { m.Close() }()
	slot := uint64(0)
	apply := func(query string) {
		t.Helper()
		slot++
		value, e := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: query, SQL: query}})
		if e != nil {
			t.Fatal(e)
		}
		if e = m.Apply(ctx, slot, value); e != nil {
			t.Fatal(e)
		}
		receipt, ok, e := m.MutationReceipt(ctx, types.MutationSQL, query)
		if e != nil || !ok || receipt.Status != types.MutationCommitted {
			t.Fatalf("%s receipt=%+v found=%v err=%v", query, receipt, ok, e)
		}
	}
	apply("CREATE TABLE items(id INTEGER PRIMARY KEY,x INTEGER,y INTEGER)")
	apply("INSERT INTO items VALUES(42,5,6)")
	apply("ALTER TABLE items RENAME TO renamed")
	apply("ALTER TABLE renamed RENAME COLUMN x TO z")
	apply("ALTER TABLE renamed DROP COLUMN z")
	apply("CREATE INDEX idx ON renamed(y)")
	apply("INSERT INTO renamed SELECT 43,y FROM renamed ORDER BY y")
	files, _, cleanup, err := m.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if err = m.Close(); err != nil {
		t.Fatal(err)
	}
	m, err = Open(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err = m.RestoreCheckpoint(ctx, files); err != nil {
		t.Fatal(err)
	}
	rows, err := m.QueryResult(ctx, "SELECT DISTINCT y,COUNT(*) FROM renamed GROUP BY y ORDER BY y", nil)
	if err != nil || len(rows.Rows) != 1 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	if _, err = m.writer.ExecContext(ctx, "SELECT * FROM sqlite_temp_master"); err == nil {
		t.Fatal("ALTER scope leaked")
	}
}

func TestAlterScopeClearsOnSuccessAndFailure(t *testing.T) {
	ctx := context.Background()
	m, err := Open(t.TempDir()+"/scope.db", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	for i, query := range []string{"CREATE TABLE items(id INTEGER)", "ALTER TABLE items RENAME TO renamed", "ALTER TABLE missing RENAME TO other"} {
		value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: query, SQL: query}})
		if err != nil {
			t.Fatal(err)
		}
		if err = m.Apply(ctx, uint64(i+1), value); err != nil {
			t.Fatal(err)
		}
		if _, err = m.writer.ExecContext(ctx, "SELECT * FROM sqlite_temp_master"); err == nil {
			t.Fatalf("scope leaked after %s", query)
		}
	}
}
