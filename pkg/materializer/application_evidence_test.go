package materializer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
)

func TestSQLApplicationEvidenceLegacyDigestCompatibility(t *testing.T) {
	m := evidenceMaterializer(t)
	ctx := context.Background()
	for _, statement := range []string{`CREATE TABLE empty (x TEXT)`, `CREATE TABLE typed (n, i, f, s, b)`, `CREATE INDEX typed_i ON typed(i)`, `CREATE VIEW visible AS SELECT i FROM typed`} {
		if _, err := m.writer.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	text := "<>&\u2028\u2029\x00\n\"\\한글"
	for _, id := range []int64{2, 10} {
		if _, err := m.writer.ExecContext(ctx, `INSERT INTO typed(rowid,n,i,f,s,b) VALUES (?,NULL,?,1.5,?,?)`, id, id, text, []byte{0, 255}); err != nil {
			t.Fatal(err)
		}
	}
	var encodedRows []string
	for _, id := range []int64{2, 10} {
		row, err := json.Marshal([][2]string{{"integer", strconv.FormatInt(id, 10)}, {"null", ""}, {"integer", strconv.FormatInt(id, 10)}, {"real", "3ff8000000000000"}, {"text", text}, {"blob", "00ff"}})
		if err != nil {
			t.Fatal(err)
		}
		encodedRows = append(encodedRows, string(row))
	}
	sort.Strings(encodedRows)
	// This is the exact historical encoding, independently assembled in memory.
	objects := []sqlEvidenceObject{
		{Type: "index", Name: "typed_i", Table: "typed", SQL: `CREATE INDEX typed_i ON typed(i)`},
		{Type: "table", Name: "empty", Table: "empty", SQL: `CREATE TABLE empty (x TEXT)`},
		{Type: "table", Name: "typed", Table: "typed", SQL: `CREATE TABLE typed (n, i, f, s, b)`, Rows: encodedRows},
		{Type: "view", Name: "visible", Table: "visible", SQL: `CREATE VIEW visible AS SELECT i FROM typed`},
	}
	canonical, err := json.Marshal(struct {
		Version string
		Objects []sqlEvidenceObject
	}{"rhiza-sql-state-v1", objects})
	if err != nil {
		t.Fatal(err)
	}
	got, err := m.InspectSQLApplicationState(ctx)
	if err != nil || got.FreshEmpty || got.Digest != sha256.Sum256(canonical) {
		t.Fatalf("canonical digest mismatch: %+v %v", got, err)
	}
}

func TestSQLApplicationEvidenceBeyondFormerLimit(t *testing.T) {
	ctx := context.Background()
	m := evidenceMaterializer(t)
	evidenceApply(t, m, 1, "schema", `CREATE TABLE large (v BLOB)`)
	payload := bytes.Repeat([]byte{0xAB}, 128<<10)
	batch, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "large", SQL: `WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<257) INSERT INTO large(v) SELECT ? FROM n`, Args: []any{payload}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 2, batch); err != nil {
		t.Fatal(err)
	}
	var count, size int64
	if err := m.writer.QueryRowContext(ctx, `SELECT COUNT(*),SUM(length(v)) FROM large`).Scan(&count, &size); err != nil {
		t.Fatal(err)
	}
	if count != 257 || size != 257*int64(len(payload)) {
		t.Fatalf("committed payload mismatch: %d %d", count, size)
	}
	got, err := m.InspectSQLApplicationState(ctx)
	if err != nil || got.FreshEmpty {
		t.Fatalf("committed >64MiB canonical evidence must remain inspectable: %+v %v", got, err)
	}
	// Hash the old canonical format independently without collecting large rows.
	h := sha256.New()
	h.Write([]byte(`{"Version":"rhiza-sql-state-v1","Objects":[{"Type":"table","Name":"large","Table":"large","SQL":"CREATE TABLE large (v BLOB)","Rows":[`))
	ids := make([]string, 257)
	for i := range ids {
		ids[i] = strconv.Itoa(i + 1)
	}
	sort.Strings(ids)
	for i, id := range ids {
		if i != 0 {
			h.Write([]byte(","))
		}
		row, err := json.Marshal([][2]string{{"integer", id}, {"blob", hex.EncodeToString(payload)}})
		if err != nil {
			t.Fatal(err)
		}
		quoted, err := json.Marshal(string(row))
		if err != nil {
			t.Fatal(err)
		}
		h.Write(quoted)
	}
	h.Write([]byte(`]}]}`))
	if !bytes.Equal(got.Digest[:], h.Sum(nil)) {
		t.Fatal("large canonical digest changed")
	}
	if err := m.writer.QueryRowContext(ctx, `SELECT COUNT(*),SUM(length(v)) FROM large`).Scan(&count, &size); err != nil || count != 257 || size != 257*int64(len(payload)) {
		t.Fatalf("inspection changed data: %d %d %v", count, size, err)
	}
}

func TestSQLApplicationEvidenceSpoolCleanupAndFailures(t *testing.T) {
	m := evidenceMaterializer(t)
	evidenceApply(t, m, 1, "schema", `CREATE TABLE item (v TEXT)`)
	evidenceApply(t, m, 2, "insert", `INSERT INTO item VALUES ('retained')`)
	root := t.TempDir()
	t.Setenv("TMPDIR", root)
	assertClean := func() {
		t.Helper()
		entries, err := os.ReadDir(root)
		if err != nil || len(entries) != 0 {
			t.Fatalf("private evidence files remain: %v %v", entries, err)
		}
		var value string
		if err := m.writer.QueryRow(`SELECT v FROM item`).Scan(&value); err != nil || value != "retained" {
			t.Fatalf("inspection changed source: %q %v", value, err)
		}
	}
	if _, err := m.InspectSQLApplicationState(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertClean()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if state, err := m.InspectSQLApplicationState(canceled); err == nil || state != (SQLApplicationState{}) {
		t.Fatalf("canceled evidence accepted: %+v %v", state, err)
	}
	assertClean()
	if _, err := m.writer.Exec(`CREATE TABLE aliases (rowid, _rowid_, oid)`); err != nil {
		t.Fatal(err)
	}
	if state, err := m.InspectSQLApplicationState(context.Background()); err == nil || !strings.Contains(err.Error(), "rowid unavailable") || state != (SQLApplicationState{}) {
		t.Fatalf("unsupported row identity accepted: %+v %v", state, err)
	}
	assertClean()
	t.Setenv("TMPDIR", filepath.Join(root, "missing"))
	if state, err := m.InspectSQLApplicationState(context.Background()); err == nil || state != (SQLApplicationState{}) {
		t.Fatalf("unavailable spool accepted: %+v %v", state, err)
	}
	assertClean()
}

func evidenceMaterializer(t *testing.T) *Materializer {
	t.Helper()
	m, err := Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Error(err)
		}
	})
	return m
}

func TestSQLApplicationEvidenceRejectsApplicationHistory(t *testing.T) {
	for _, kind := range []string{"insert-delete", "graph", "migration"} {
		t.Run(kind, func(t *testing.T) {
			m := evidenceMaterializer(t)
			ctx := context.Background()
			switch kind {
			case "insert-delete":
				evidenceApply(t, m, 1, "create", `CREATE TABLE item (id INTEGER)`)
				evidenceApply(t, m, 2, "insert", `INSERT INTO item VALUES (1)`)
				evidenceApply(t, m, 3, "delete", `DELETE FROM item`)
				evidenceApply(t, m, 4, "drop", `DROP TABLE item`)
			case "graph":
				value, err := types.EncodeGraphCommand(types.GraphCommand{RequestID: "graph", Events: []types.GraphStreamEvent{{Stream: "s", Kind: "created", Payload: "x"}}})
				if err != nil {
					t.Fatal(err)
				}
				if err := m.Apply(ctx, 1, value); err != nil {
					t.Fatal(err)
				}
			case "migration":
				value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "migration", Statements: []types.SQLStatement{{SQL: `CREATE TABLE migration_data (id INTEGER)`}}, Migration: &types.SQLMigration{Version: 1, Name: "first", Checksum: strings.Repeat("a", 64)}}})
				if err != nil {
					t.Fatal(err)
				}
				if err := m.Apply(ctx, 1, value); err != nil {
					t.Fatal(err)
				}
			}
			state, err := m.InspectSQLApplicationState(ctx)
			if err != nil || state.FreshEmpty {
				t.Fatalf("history %s state=%+v err=%v", kind, state, err)
			}
		})
	}
}

func TestSQLApplicationEvidenceCanceledAndClosed(t *testing.T) {
	m := evidenceMaterializer(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.InspectSQLApplicationState(ctx); err == nil {
		t.Fatal("canceled inspection succeeded")
	}
}

func TestInspectSQLApplicationFileReadOnlySnapshot(t *testing.T) {
	ctx := context.Background()
	m := evidenceMaterializer(t)
	evidenceApply(t, m, 1, "schema", `CREATE TABLE item (id INTEGER PRIMARY KEY, value TEXT)`)
	evidenceApply(t, m, 2, "insert", `INSERT INTO item(value) VALUES ('expected')`)
	want, err := m.InspectSQLApplicationState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got, tip, err := InspectSQLApplicationFile(ctx, m.dbPath)
	if err != nil || tip != 2 || got.Digest != want.Digest || got.FreshEmpty {
		t.Fatalf("read-only file state=%+v tip=%d err=%v", got, tip, err)
	}
	missingPath := m.dbPath + ".missing"
	if _, _, err := InspectSQLApplicationFile(ctx, missingPath); err == nil {
		t.Fatal("missing database was created or accepted")
	}
	if _, err := os.Stat(missingPath); !os.IsNotExist(err) {
		t.Fatalf("read-only inspection created missing database: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := InspectSQLApplicationFile(canceled, m.dbPath); err == nil {
		t.Fatal("canceled read-only inspection succeeded")
	}
}

func TestInspectSQLApplicationFileNeverRemovesSourceSidecars(t *testing.T) {
	ctx := context.Background()
	m := evidenceMaterializer(t)
	evidenceApply(t, m, 1, "schema", `CREATE TABLE item (id INTEGER PRIMARY KEY)`)
	// Use a closed, standalone DB so this test can place a source-sidecar that
	// SQLite itself is not managing. The inspector must never claim ownership.
	source := filepath.Join(t.TempDir(), "source.sqlite")
	if err := copyEvidenceFile(m.dbPath, source); err != nil {
		t.Fatal(err)
	}
	if err := copyEvidenceFileIfPresent(m.dbPath+"-wal", source+"-wal"); err != nil {
		t.Fatal(err)
	}
	// Make the source copy long enough for the creator goroutine to race the
	// inspector after its private snapshot directory has been created.
	if err := os.Truncate(source, 64<<20); err != nil {
		t.Fatal(err)
	}
	foreignSidecar := source + "-shm"
	if _, err := os.Lstat(foreignSidecar); !os.IsNotExist(err) {
		t.Fatalf("source sidecar unexpectedly exists before race: %v", err)
	}
	entries, err := os.ReadDir(os.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	baseline := make(map[string]bool, len(entries))
	for _, entry := range entries {
		baseline[entry.Name()] = true
	}
	start := make(chan struct{})
	created := make(chan error, 1)
	finish := make(chan struct{})
	go func() {
		<-start
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			current, err := os.ReadDir(os.TempDir())
			if err != nil {
				created <- err
				return
			}
			for _, entry := range current {
				if strings.HasPrefix(entry.Name(), "rhiza-sql-evidence-") && !baseline[entry.Name()] {
					created <- os.WriteFile(foreignSidecar, []byte("created during inspection by another connection"), 0o600)
					<-finish
					return
				}
			}
			time.Sleep(time.Millisecond)
		}
		created <- os.ErrDeadlineExceeded
		<-finish
	}()
	close(start)
	inspected := make(chan error, 1)
	go func() {
		_, _, err := InspectSQLApplicationFile(ctx, source)
		inspected <- err
	}()
	if err := <-created; err != nil {
		close(finish)
		t.Fatal(err)
	}
	before, err := os.ReadFile(foreignSidecar)
	if err != nil {
		close(finish)
		t.Fatal(err)
	}
	inspectErr := <-inspected
	close(finish)
	if inspectErr != nil {
		t.Fatal(inspectErr)
	}
	after, err := os.ReadFile(foreignSidecar)
	if err != nil || string(after) != string(before) {
		t.Fatalf("source sidecar changed or disappeared: before=%q after=%q err=%v", before, after, err)
	}
}

func TestSQLApplicationEvidenceConcurrentApplySnapshot(t *testing.T) {
	ctx := context.Background()
	m := evidenceMaterializer(t)
	evidenceApply(t, m, 1, "schema", `CREATE TABLE item (id INTEGER)`)
	before, err := m.InspectSQLApplicationState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "insert", SQL: `INSERT INTO item VALUES (1),(2)`}})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- m.Apply(ctx, 2, value) }()
	observed, inspectErr := m.InspectSQLApplicationState(ctx)
	applyErr := <-done
	if inspectErr != nil {
		t.Fatal(inspectErr)
	}
	if applyErr != nil {
		t.Fatal(applyErr)
	}
	after, err := m.InspectSQLApplicationState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if before.Digest == after.Digest || (observed.Digest != before.Digest && observed.Digest != after.Digest) {
		t.Fatal("inspection observed a partial state")
	}
}

func evidenceApply(t *testing.T, m *Materializer, slot uint64, id, statement string) {
	t.Helper()
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: id, SQL: statement}})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(context.Background(), slot, value); err != nil {
		t.Fatal(err)
	}
}

func TestSQLApplicationEvidenceEmptyAndHistory(t *testing.T) {
	ctx := context.Background()
	a, b := evidenceMaterializer(t), evidenceMaterializer(t)
	first, err := a.InspectSQLApplicationState(ctx)
	if err != nil || !first.FreshEmpty {
		t.Fatalf("fresh=%+v err=%v", first, err)
	}
	if err := b.Apply(ctx, 1, types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{})); err != nil {
		t.Fatal(err)
	}
	control, err := b.InspectSQLApplicationState(ctx)
	if err != nil || !control.FreshEmpty || first.Digest != control.Digest {
		t.Fatalf("control=%+v err=%v", control, err)
	}
	evidenceApply(t, a, 1, "create", `CREATE TABLE data (id INTEGER)`)
	evidenceApply(t, a, 2, "drop", `DROP TABLE data`)
	after, err := a.InspectSQLApplicationState(ctx)
	if err != nil || after.FreshEmpty || after.Digest != first.Digest {
		t.Fatalf("dropped=%+v err=%v", after, err)
	}
}

func TestSQLApplicationEvidenceCanonicalTypedRows(t *testing.T) {
	ctx := context.Background()
	a, b := evidenceMaterializer(t), evidenceMaterializer(t)
	for _, m := range []*Materializer{a, b} {
		evidenceApply(t, m, 1, "schema", `CREATE TABLE data (n, i, f, s, b)`)
	}
	evidenceApply(t, a, 2, "first", `INSERT INTO data(rowid,n,i,f,s,b) VALUES (1,NULL,1,1.5,'x',X'00FF'),(2,NULL,2,2.5,'y',X'01')`)
	evidenceApply(t, b, 2, "second", `INSERT INTO data(rowid,n,i,f,s,b) VALUES (2,NULL,2,2.5,'y',X'01'),(1,NULL,1,1.5,'x',X'00FF')`)
	x, err := a.InspectSQLApplicationState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	y, err := b.InspectSQLApplicationState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if x.FreshEmpty || y.FreshEmpty || x.Digest != y.Digest {
		t.Fatalf("logical states differ: %+v %+v", x, y)
	}
	evidenceApply(t, b, 3, "change", `UPDATE data SET i='1' WHERE i=1`)
	y, err = b.InspectSQLApplicationState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if x.Digest == y.Digest {
		t.Fatal("integer and text were conflated")
	}
}

func TestSQLApplicationEvidenceRowIDVariants(t *testing.T) {
	for _, tc := range []struct {
		name, schema, first, second string
		equal, reject               bool
	}{
		{"without rowid", `CREATE TABLE t(k TEXT PRIMARY KEY, x TEXT) WITHOUT ROWID`, `INSERT INTO t VALUES('a','x'),('b','y')`, `INSERT INTO t VALUES('b','y'),('a','x')`, true, false},
		{"shadowed alias", `CREATE TABLE t(rowid TEXT, x TEXT)`, `INSERT INTO t(_rowid_,rowid,x) VALUES(1,'r','a')`, `INSERT INTO t(_rowid_,rowid,x) VALUES(2,'r','a')`, false, false},
		{"all aliases shadowed", `CREATE TABLE t(rowid TEXT, _rowid_ TEXT, oid TEXT)`, `INSERT INTO t VALUES('a','b','c')`, `INSERT INTO t VALUES('a','b','c')`, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, b := evidenceMaterializer(t), evidenceMaterializer(t)
			for _, m := range []*Materializer{a, b} {
				evidenceApply(t, m, 1, "schema", tc.schema)
			}
			evidenceApply(t, a, 2, "insert", tc.first)
			evidenceApply(t, b, 2, "insert", tc.second)
			x, ex := a.InspectSQLApplicationState(context.Background())
			y, ey := b.InspectSQLApplicationState(context.Background())
			if tc.reject {
				if ex == nil || ey == nil {
					t.Fatalf("expected rejection: %v %v", ex, ey)
				}
				return
			}
			if ex != nil || ey != nil {
				t.Fatalf("inspection: %v %v", ex, ey)
			}
			if (x.Digest == y.Digest) != tc.equal {
				t.Fatal("unexpected digest equality")
			}
		})
	}
}

func TestSQLApplicationEvidenceRowIdentity(t *testing.T) {
	a, b := evidenceMaterializer(t), evidenceMaterializer(t)
	for _, m := range []*Materializer{a, b} {
		evidenceApply(t, m, 1, "schema", `CREATE TABLE t(x TEXT)`)
	}
	evidenceApply(t, a, 2, "insert", `INSERT INTO t(rowid,x) VALUES(1,'a')`)
	evidenceApply(t, b, 2, "insert", `INSERT INTO t(rowid,x) VALUES(2,'a')`)
	x, err := a.InspectSQLApplicationState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	y, err := b.InspectSQLApplicationState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if x.Digest == y.Digest {
		t.Fatal("different rowids produced the same digest")
	}
}

func TestSQLApplicationEvidenceRejectsUnknownInternal(t *testing.T) {
	for _, statement := range []string{`CREATE TABLE _rhiza_unrecognized(x)`, `DROP INDEX _rhiza_kv_expiry`, `DROP INDEX _rhiza_kv_expiry; CREATE INDEX _rhiza_kv_expiry ON _rhiza_kv(key)`} {
		t.Run(statement, func(t *testing.T) {
			m := evidenceMaterializer(t)
			if _, err := m.writer.Exec(statement); err != nil {
				t.Fatal(err)
			}
			if _, err := m.InspectSQLApplicationState(context.Background()); err == nil {
				t.Fatal("invalid internal schema accepted")
			}
		})
	}
}
