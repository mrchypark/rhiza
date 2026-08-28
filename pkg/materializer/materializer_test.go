//go:build !graph

package materializer

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestNormalizeSQLArgsAcceptsGoIntegers(t *testing.T) {
	values, err := NormalizeSQLArgs([]any{int(1), int32(2), uint64(3), "ok"})
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []any{int64(1), int64(2), int64(3), "ok"} {
		if values[i] != want {
			t.Fatalf("arg %d = %#v, want %#v", i, values[i], want)
		}
	}
	if _, err := NormalizeSQLArgs([]any{uint64(^uint64(0))}); err == nil {
		t.Fatal("overflow accepted")
	}
}

func TestMaterializerCreatesKVExpiryIndex(t *testing.T) {
	m, err := Open(t.TempDir()+"/index.db", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	var name string
	if err := m.db.QueryRow(`SELECT name FROM sqlite_master WHERE type = 'index' AND name = '_rhiza_kv_expiry'`).Scan(&name); err != nil {
		t.Fatal(err)
	}
}

func TestMaterializerApply(t *testing.T) {
	dir, err := os.MkdirTemp("", "materializer-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	dbPath := dir + "/test.db"
	m, err := Open(dbPath, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	// Create table
	err = m.Apply(context.Background(), 1, []byte("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"))
	if err != nil {
		t.Fatalf("apply error: %v", err)
	}

	// Insert data
	err = m.Apply(context.Background(), 2, []byte("INSERT INTO test (id, name) VALUES (1, 'hello')"))
	if err != nil {
		t.Fatalf("apply error: %v", err)
	}

	rows, err := m.Query(context.Background(), "SELECT id, name FROM test WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected one row")
	}

	var id int
	var name string
	err = rows.Scan(&id, &name)
	if err != nil {
		t.Fatalf("query error: %v", err)
	}

	if id != 1 || name != "hello" {
		t.Errorf("expected (1, hello), got (%d, %s)", id, name)
	}
}

func TestMaterializerAppliesSQLBatchAtomically(t *testing.T) {
	m, err := Open(t.TempDir()+"/batch.db", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	value, err := types.EncodeSQLBatch([]types.SQLCommand{
		{RequestID: "schema", SQL: "CREATE TABLE batched (id INTEGER PRIMARY KEY)"},
		{RequestID: "row", SQL: "INSERT INTO batched VALUES (1)"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(context.Background(), 1, value); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := m.queryRow(context.Background(), "SELECT COUNT(*) FROM batched").Scan(&count); err != nil || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
}

func TestMaterializerDeduplicatesRequestID(t *testing.T) {
	m, err := Open(t.TempDir()+"/dedupe.db", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	values := [][]types.SQLCommand{
		{{RequestID: "schema", SQL: "CREATE TABLE dedupe (value INTEGER)"}, {RequestID: "row", SQL: "INSERT INTO dedupe VALUES (1)"}},
		{{RequestID: "row", SQL: "INSERT INTO dedupe VALUES (1)"}},
	}
	for i, commands := range values {
		value, err := types.EncodeSQLBatch(commands)
		if err != nil {
			t.Fatal(err)
		}
		if err := m.Apply(context.Background(), uint64(i+1), value); err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := m.queryRow(context.Background(), "SELECT COUNT(*) FROM dedupe").Scan(&count); err != nil || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	conflict, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "row", SQL: "INSERT INTO dedupe VALUES (2)"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(context.Background(), 3, conflict); err != nil {
		t.Fatal(err)
	}
	if m.Tip() != 3 {
		t.Fatalf("conflicting no-op did not advance tip: %d", m.Tip())
	}
	if err := m.queryRow(context.Background(), "SELECT COUNT(*) FROM dedupe").Scan(&count); err != nil || count != 1 {
		t.Fatalf("count after conflicting retry=%d err=%v", count, err)
	}
}

func TestFailedSQLCommandRecordsResultAndAdvancesTip(t *testing.T) {
	ctx := context.Background()
	m, err := Open(t.TempDir()+"/failure.db", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	commands := [][]types.SQLCommand{
		{{RequestID: "schema", SQL: "CREATE TABLE failures (id INTEGER PRIMARY KEY)"}},
		{{RequestID: "first", SQL: "INSERT INTO failures VALUES (1)"}},
		{{RequestID: "duplicate", SQL: "INSERT INTO failures VALUES (1)"}},
		{{RequestID: "after", SQL: "INSERT INTO failures VALUES (2)"}},
	}
	for i, batch := range commands {
		value, err := types.EncodeSQLBatch(batch)
		if err != nil {
			t.Fatal(err)
		}
		if err := m.Apply(ctx, uint64(i+1), value); err != nil {
			t.Fatal(err)
		}
	}
	receipt, found, err := m.MutationReceipt(ctx, types.MutationSQL, "duplicate")
	if err != nil || !found || receipt.Status != types.MutationRejected || m.Tip() != 4 {
		t.Fatalf("receipt=%+v found=%v tip=%d err=%v", receipt, found, m.Tip(), err)
	}
	var count int
	if err := m.queryRow(ctx, "SELECT COUNT(*) FROM failures").Scan(&count); err != nil || count != 2 {
		t.Fatalf("count=%d err=%v", count, err)
	}
}

func TestIdempotencyReceiptExpiresAtWindowBoundary(t *testing.T) {
	ctx := context.Background()
	m, err := Open(t.TempDir()+"/retention.db", 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "old", SQL: "CREATE TABLE retained (id INTEGER)"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 1, value); err != nil {
		t.Fatal(err)
	}
	var nonce [types.ReadBarrierNonceSize]byte
	for slot := uint64(2); slot <= 1025; slot++ {
		nonce[0] = byte(slot)
		if err := m.Apply(ctx, slot, types.EncodeReadBarrier(nonce)); err != nil {
			t.Fatal(err)
		}
	}
	if _, found, err := m.MutationReceipt(ctx, types.MutationSQL, "old"); err != nil || found {
		t.Fatalf("expired receipt found=%v err=%v", found, err)
	}
}

func TestKVMutationPhysicallyPrunesExpiredRows(t *testing.T) {
	m, err := Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	ctx := context.Background()
	commands := []types.KVCommand{
		{RequestID: "expired", Operation: "put", Key: "expired", Value: []byte("old"), ObservedAtUnixMS: 1, ExpiresAtUnixMS: 10},
		{RequestID: "next", Operation: "put", Key: "next", Value: []byte("new"), ObservedAtUnixMS: 11},
	}
	for i, command := range commands {
		value, err := types.EncodeKVCommand(command)
		if err != nil || m.Apply(ctx, uint64(i+1), value) != nil {
			t.Fatalf("apply %d: encode=%v", i+1, err)
		}
	}
	var count int
	if err := m.writer.QueryRowContext(ctx, `SELECT COUNT(*) FROM _rhiza_kv WHERE key = 'expired'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("expired physical rows=%d err=%v", count, err)
	}
}

func TestKVBatchAppliesAllCommands(t *testing.T) {
	m, err := Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	commands := []types.KVCommand{
		{RequestID: "batch-a", Operation: "put", Key: "a", Value: []byte("one"), ObservedAtUnixMS: 1},
		{RequestID: "batch-b", Operation: "put", Key: "b", Value: []byte("two"), ObservedAtUnixMS: 1},
	}
	items := make([][]byte, len(commands))
	for i := range commands {
		items[i], err = types.EncodeKVBatchItem(commands[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := m.Apply(context.Background(), 1, types.AssembleKVBatch(items)); err != nil {
		t.Fatal(err)
	}
	for _, command := range commands {
		got, found, err := m.KVGet(context.Background(), command.Key, time.UnixMilli(1))
		if err != nil || !found || string(got) != string(command.Value) {
			t.Fatalf("key %q value=%q found=%v err=%v", command.Key, got, found, err)
		}
	}
}

func TestSQLCommandRejectsMutationRows(t *testing.T) {
	command := types.SQLCommand{RequestID: "aggregate", Statements: []types.SQLStatement{
		{SQL: "WITH RECURSIVE seq(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM seq WHERE n<6000) SELECT n FROM seq", WantRows: true},
	}}
	if err := ValidateSQLCommand(command); err == nil {
		t.Fatal("mutation row result was accepted")
	}
}

func TestReplicatedSQLRejectsTailPragmaAndNullByte(t *testing.T) {
	m, err := Open(t.TempDir()+"/sql-boundary.db", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	command := types.SQLCommand{RequestID: "tail", SQL: "CREATE TABLE escaped (id INTEGER); PRAGMA writable_schema=ON"}
	value, err := types.EncodeSQLBatch([]types.SQLCommand{command})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(context.Background(), 1, value); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := m.queryRow(context.Background(), "SELECT COUNT(*) FROM sqlite_master WHERE name='escaped'").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("the first statement executed despite a non-whitespace tail")
	}
	if err := ValidateSQLCommand(types.SQLCommand{SQL: "SELECT 1\x00; PRAGMA writable_schema=ON"}); err == nil {
		t.Fatal("SQL containing a null byte was accepted")
	}
	if err := ValidateSQLCommand(types.SQLCommand{SQL: "/* leading comment */ PRAGMA writable_schema=ON"}); err == nil {
		t.Fatal("comment-prefixed PRAGMA was accepted")
	}
}

func TestMaterializerArgumentsTransactionsResultsAndSnapshot(t *testing.T) {
	ctx := context.Background()
	m, err := Open(t.TempDir()+"/features.db", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	command := types.SQLCommand{RequestID: "tx", Statements: []types.SQLStatement{
		{SQL: "CREATE TABLE features (id INTEGER PRIMARY KEY, name TEXT)"},
		{SQL: "INSERT INTO features(name) VALUES (?)", Args: []any{"safe"}},
	}}
	value, err := types.EncodeSQLBatch([]types.SQLCommand{command})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 1, value); err != nil {
		t.Fatal(err)
	}
	result, err := m.QueryResult(ctx, "SELECT name FROM features", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := result.Rows[0][0]; got != "safe" {
		t.Fatalf("returning name=%v", got)
	}
	snapshot, err := m.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 2, []byte("DELETE FROM features")); err != nil {
		t.Fatal(err)
	}
	if err := m.Restore(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := m.queryRow(ctx, "SELECT COUNT(*) FROM features").Scan(&count); err != nil || count != 1 || m.Tip() != 1 {
		t.Fatalf("restored count=%d tip=%d err=%v", count, m.Tip(), err)
	}
	foreignPath := t.TempDir() + "/foreign.db"
	foreign, err := sql.Open("sqlite3", "file:"+foreignPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := foreign.Exec("CREATE TABLE foreign_data (id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	foreign.Close()
	foreignData, err := os.ReadFile(foreignPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Restore(ctx, foreignData); err == nil {
		t.Fatal("expected foreign snapshot rejection")
	}
	if err := m.queryRow(ctx, "SELECT COUNT(*) FROM features").Scan(&count); err != nil || count != 1 || m.Tip() != 1 {
		t.Fatalf("failed restore damaged original: count=%d tip=%d err=%v", count, m.Tip(), err)
	}
}

func TestQueryResultRejectsOversizedCell(t *testing.T) {
	m, err := Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if _, err := m.QueryResult(context.Background(), "SELECT zeroblob(?)", []any{MaxCellBytes + 1}); err == nil {
		t.Fatal("oversized result cell was accepted")
	}
}

func TestQueryResultBoundsEncodedJSON(t *testing.T) {
	m, err := Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	ctx := context.Background()
	if _, err := m.writer.ExecContext(ctx, "CREATE TABLE encoded_result (value TEXT)"); err != nil {
		t.Fatal(err)
	}
	value := strings.Repeat("\x01", MaxCellBytes)
	for range 3 {
		if _, err := m.writer.ExecContext(ctx, "INSERT INTO encoded_result(value) VALUES (?)", value); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := m.QueryResult(ctx, "SELECT value FROM encoded_result", nil); err == nil {
		t.Fatal("JSON-escaped result exceeded the response budget")
	}
}

func TestMaterializerRejectsNondeterministicWrite(t *testing.T) {
	m, err := Open(t.TempDir()+"/deterministic.db", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.Apply(context.Background(), 1, []byte("CREATE TABLE deterministic (value INTEGER DEFAULT (random()))")); err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(context.Background(), 2, []byte("INSERT INTO deterministic VALUES (random())")); err == nil {
		t.Fatal("expected random() to be rejected")
	}
}

func TestStateTipIgnoresConsensusOnlyDecisions(t *testing.T) {
	m, err := Open(t.TempDir()+"/state-tip.db", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.Apply(context.Background(), 1, []byte("CREATE TABLE state_tip (id INTEGER)")); err != nil {
		t.Fatal(err)
	}
	var nonce [types.ReadBarrierNonceSize]byte
	if err := m.Apply(context.Background(), 2, types.EncodeReadBarrier(nonce)); err != nil {
		t.Fatal(err)
	}
	if m.Tip() != 2 || m.StateTip() != 1 {
		t.Fatalf("tip=%d state_tip=%d, want 2/1", m.Tip(), m.StateTip())
	}
}

func TestMaterializerHiqliteSQLSurface(t *testing.T) {
	ctx := context.Background()
	m, err := Open(t.TempDir()+"/hiqlite-surface.db", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	commands := []types.SQLCommand{
		{RequestID: "schema", Statements: []types.SQLStatement{
			{SQL: `CREATE TABLE feature (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score INTEGER NOT NULL DEFAULT 0, doubled INTEGER GENERATED ALWAYS AS (score * 2) STORED) STRICT`},
			{SQL: `CREATE UNIQUE INDEX feature_name ON feature(lower(name)) WHERE score >= 0`},
			{SQL: `CREATE TABLE audit (feature_id INTEGER, old_score INTEGER, new_score INTEGER)`},
			{SQL: `CREATE TRIGGER feature_audit AFTER UPDATE OF score ON feature BEGIN INSERT INTO audit VALUES (old.id, old.score, new.score); END`},
			{SQL: `CREATE VIRTUAL TABLE feature_search USING fts5(name)`},
		}},
		{RequestID: "insert", SQL: `INSERT INTO feature(name, score) VALUES (?, ?)`, Args: []any{"Ada", int64(3)}},
		{RequestID: "cte-upsert", SQL: `WITH input(name, score) AS (VALUES ('Ada', 5), ('Grace', 7)) INSERT INTO feature(name, score) SELECT name, score FROM input WHERE true ON CONFLICT(lower(name)) WHERE score >= 0 DO UPDATE SET score = excluded.score`},
		{RequestID: "fts", SQL: `INSERT INTO feature_search(name) SELECT name FROM feature`},
	}
	for slot, command := range commands {
		if err := ValidateSQLCommand(command); err != nil {
			t.Fatalf("validate slot %d: %v", slot+1, err)
		}
		value, err := types.EncodeSQLBatch([]types.SQLCommand{command})
		if err != nil {
			t.Fatal(err)
		}
		if err := m.Apply(ctx, uint64(slot+1), value); err != nil {
			t.Fatalf("apply slot %d: %v", slot+1, err)
		}
	}
	result, err := m.QueryResult(ctx, `WITH RECURSIVE seq(n) AS (VALUES(1) UNION ALL SELECT n + 1 FROM seq WHERE n < 3) SELECT json_extract('{"ok":true}', '$.ok'), group_concat(n, '') FROM seq`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := result.Rows[0]; len(got) != 2 || got[0] != int64(1) || got[1] != "123" {
		t.Fatalf("recursive CTE/JSON result=%v types=%T,%T", got, got[0], got[1])
	}
	result, err = m.QueryResult(ctx, `SELECT name, row_number() OVER (ORDER BY score DESC) AS rank FROM feature ORDER BY rank`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := result.Rows; len(got) != 2 || got[0][0] != "Grace" || got[0][1] != int64(1) {
		t.Fatalf("window result=%v", got)
	}
	result, err = m.QueryResult(ctx, `SELECT name FROM feature_search WHERE feature_search MATCH 'Grace'`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := result.Rows; len(got) != 1 || got[0][0] != "Grace" {
		t.Fatalf("FTS5 result=%v", got)
	}
}

func TestMaterializerReadAPIRejectsAttachment(t *testing.T) {
	m, err := Open(t.TempDir()+"/read-boundary.db", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if _, err := m.QueryResult(context.Background(), `ATTACH DATABASE ':memory:' AS other`, nil); err == nil {
		t.Fatal("read API accepted ATTACH")
	}
	if _, err := m.Snapshot(context.Background()); err != nil {
		t.Fatalf("internal snapshot was blocked: %v", err)
	}
}

func BenchmarkCheckpointFilesAt(b *testing.B) {
	m, err := Open(b.TempDir()+"/checkpoint.db", 1)
	if err != nil {
		b.Fatal(err)
	}
	defer m.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _, cleanup, err := m.CheckpointFilesAt(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		cleanup()
	}
}

func TestMaterializerPublishesCommittedNotificationOnce(t *testing.T) {
	m, err := Open(t.TempDir()+"/notify.db", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	ch, cancel := m.Subscribe("jobs")
	defer cancel()
	value, err := types.EncodeNotifyCommand(types.NotifyCommand{RequestID: "notice-1", Topic: "jobs", Payload: []byte("ready")})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(context.Background(), 1, value); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-ch:
		if string(got) != "ready" {
			t.Fatalf("payload=%q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("notification was not delivered")
	}
	if err := m.Apply(context.Background(), 2, value); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ch:
		t.Fatal("duplicate notification was delivered twice")
	default:
	}
}

func TestMaterializerAdvancesPastLeaderSchedule(t *testing.T) {
	m, err := Open(t.TempDir()+"/schedule.db", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	value, err := types.EncodeLeaderSchedule([]types.NodeID{"n2", "n1", "n3"})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(context.Background(), 1, value); err != nil {
		t.Fatal(err)
	}
	if m.Tip() != 1 {
		t.Fatalf("tip=%d, want 1", m.Tip())
	}
}

func TestMaterializerHealth(t *testing.T) {
	dir, err := os.MkdirTemp("", "materializer-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	dbPath := dir + "/test.db"
	m, err := Open(dbPath, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	if err := m.Health(context.Background()); err != nil {
		t.Fatalf("health check failed: %v", err)
	}
}

func TestMaterializerPersistsAppliedSlot(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	m, err := Open(dbPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(context.Background(), 1, []byte("CREATE TABLE durable (id INTEGER PRIMARY KEY)")); err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(context.Background(), 2, []byte("INSERT INTO durable VALUES (1)")); err != nil {
		t.Fatal(err)
	}
	m.Close()

	m, err = Open(dbPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if m.Tip() != 2 {
		t.Fatalf("tip=%d, want 2", m.Tip())
	}
	if err := m.Apply(context.Background(), 2, []byte("INSERT INTO durable VALUES (1)")); err != nil {
		t.Fatalf("duplicate apply: %v", err)
	}
	if err := m.Apply(context.Background(), 2, []byte("INSERT INTO durable VALUES (2)")); err == nil {
		t.Fatal("expected duplicate slot hash conflict")
	}
	if err := m.Apply(context.Background(), 4, []byte("SELECT 1")); err == nil {
		t.Fatal("expected slot gap")
	}
	if err := m.Apply(context.Background(), 3, []byte("not valid SQL")); err == nil {
		t.Fatal("expected SQL error")
	}
	if m.Tip() != 2 {
		t.Fatalf("failed apply advanced tip to %d", m.Tip())
	}
}

func TestValidateTipRejectsRecoveredDecisionConflict(t *testing.T) {
	m, err := Open(t.TempDir()+"/validate-tip.db", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	ctx := context.Background()
	value := []byte("CREATE TABLE validate_tip (id INTEGER)")
	if err := m.Apply(ctx, 1, value); err != nil {
		t.Fatal(err)
	}
	if err := m.ValidateTip(1, value); err != nil {
		t.Fatal(err)
	}
	if err := m.ValidateTip(1, []byte("different")); err == nil {
		t.Fatal("accepted conflicting recovered decision")
	}
}

func TestMaterializerRejectsUnmanagedDatabaseWithoutAppliedSlot(t *testing.T) {
	dbPath := t.TempDir() + "/unmanaged.db"
	db, err := sql.Open("sqlite3", "file:"+dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE unmanaged (id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if _, err := Open(dbPath, 1); err == nil {
		t.Fatal("expected unmanaged database rejection")
	}
}

func TestPublicSQLCannotAccessInternalTables(t *testing.T) {
	m, err := Open(t.TempDir()+"/internal.db", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	ctx := context.Background()
	if err := m.Apply(ctx, 1, []byte(`INSERT INTO _rhiza_meta(key, value) VALUES ('owned', '1')`)); err == nil {
		t.Fatal("replicated SQL wrote an internal table")
	}
	if _, err := m.QueryResult(ctx, `SELECT * FROM _rhiza_meta`, nil); err == nil {
		t.Fatal("public query read an internal table")
	}
}

func TestApplyBatchAdvancesContiguousPage(t *testing.T) {
	m, err := Open(t.TempDir()+"/batch.db", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	decisions := []quepaxa.DecidedValue{
		{Slot: 1, Value: []byte(`CREATE TABLE batch_page (id INTEGER)`)},
		{Slot: 2, Value: []byte(`INSERT INTO batch_page VALUES (1)`)},
	}
	if err := m.ApplyBatch(context.Background(), decisions); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := m.queryRow(context.Background(), `SELECT COUNT(*) FROM batch_page`).Scan(&count); err != nil || count != 1 || m.Tip() != 2 {
		t.Fatalf("count=%d tip=%d err=%v", count, m.Tip(), err)
	}
}

func TestRecoverInterruptedRestore(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "materialized.db")
	graphPath := filepath.Join(dir, "latticedb")
	if err := os.WriteFile(dbPath, []byte("new-sqlite"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dbPath+".restore-backup", []byte("old-sqlite"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(graphPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(graphPath, "value"), []byte("new-graph"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(graphPath+".restore-backup", 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(graphPath+".restore-backup", "value"), []byte("old-graph"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := writeRestoreState(dbPath, restoreState{Phase: "sqlite-installed", InstallGraph: true, GraphHadOriginal: true}); err != nil {
		t.Fatal(err)
	}
	if err := recoverRestore(dbPath); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(dbPath); err != nil || string(data) != "old-sqlite" {
		t.Fatalf("sqlite=%q err=%v", data, err)
	}
	if data, err := os.ReadFile(filepath.Join(graphPath, "value")); err != nil || string(data) != "old-graph" {
		t.Fatalf("graph=%q err=%v", data, err)
	}
}

func TestRecoverCommittedRestoreFinalizes(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "materialized.db")
	if err := os.WriteFile(dbPath, []byte("new-sqlite"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dbPath+".restore-backup", []byte("old-sqlite"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := writeRestoreState(dbPath, restoreState{Phase: "committed"}); err != nil {
		t.Fatal(err)
	}
	if err := recoverRestore(dbPath); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(dbPath); err != nil || string(data) != "new-sqlite" {
		t.Fatalf("sqlite=%q err=%v", data, err)
	}
	if _, err := os.Stat(dbPath + ".restore-backup"); !os.IsNotExist(err) {
		t.Fatalf("backup remains: %v", err)
	}
	if _, err := os.Stat(restoreStatePath(dbPath)); !os.IsNotExist(err) {
		t.Fatalf("journal remains: %v", err)
	}
}
