package rhiza_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/mrchypark/rhiza"
)

func TestApplicationCacheCleanupIsBoundedAndIdempotent(t *testing.T) {
	ctx := context.Background()
	db, err := rhiza.Open(ctx, rhiza.Config{NodeID: "n1", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	exec := func(requestID, sql string, args ...any) rhiza.ExecuteResponse {
		t.Helper()
		response, err := db.Execute(ctx, rhiza.ExecuteRequest{RequestID: requestID, SQL: sql, Args: args})
		if err != nil {
			t.Fatalf("%s: %v", requestID, err)
		}
		if response.Status != rhiza.MutationCommitted {
			t.Fatalf("%s: receipt=%+v", requestID, response.MutationReceipt)
		}
		return response
	}
	exec("cache-schema", `CREATE TABLE app_cache (namespace INTEGER NOT NULL, cache_key TEXT NOT NULL, value BLOB NOT NULL, expires_at INTEGER, PRIMARY KEY(namespace, cache_key))`)
	exec("cache-index", `CREATE INDEX app_cache_expiry ON app_cache(expires_at, namespace, cache_key)`)
	for _, row := range []struct {
		key     string
		expires any
	}{{"old-1", int64(1)}, {"old-2", int64(1)}, {"old-3", int64(2)}, {"fresh", int64(11)}, {"permanent", nil}} {
		exec("cache-"+row.key, `INSERT INTO app_cache(namespace, cache_key, value, expires_at) VALUES (?, ?, ?, ?)`, int64(1), row.key, []byte(row.key), row.expires)
	}
	const cleanupSQL = `DELETE FROM app_cache WHERE rowid IN (SELECT rowid FROM app_cache WHERE expires_at <= ? ORDER BY expires_at, namespace, cache_key LIMIT ?)`
	first := exec("cache-cleanup-pass-1", cleanupSQL, int64(2), int64(2))
	if first.RowsAffected != 2 {
		t.Fatalf("first cleanup rows=%d, want 2", first.RowsAffected)
	}
	retry := exec("cache-cleanup-pass-1", cleanupSQL, int64(2), int64(2))
	if retry.Slot != first.Slot || retry.RowsAffected != first.RowsAffected {
		t.Fatalf("retry=%+v first=%+v", retry, first)
	}
	assertCacheKeys(t, db, ctx, "fresh", "old-3", "permanent")
	second := exec("cache-cleanup-pass-2", cleanupSQL, int64(2), int64(2))
	if second.RowsAffected != 1 {
		t.Fatalf("second cleanup rows=%d, want 1", second.RowsAffected)
	}
	assertCacheKeys(t, db, ctx, "fresh", "permanent")
}

func BenchmarkApplicationCacheCleanup(b *testing.B) {
	for _, backlog := range []int{256, 4096} {
		b.Run("expired_rows_"+strconv.Itoa(backlog), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				ctx := context.Background()
				db, err := rhiza.Open(ctx, rhiza.Config{NodeID: "n1", DataDir: b.TempDir()})
				if err != nil {
					b.Fatal(err)
				}
				setupCacheBenchmark(b, ctx, db, backlog)
				b.StartTimer()
				response, err := db.Execute(ctx, rhiza.ExecuteRequest{RequestID: "cleanup", SQL: `DELETE FROM cache_benchmark WHERE rowid IN (SELECT rowid FROM cache_benchmark WHERE expires_at <= ? ORDER BY expires_at, namespace, cache_key LIMIT ?)`, Args: []any{int64(2), int64(256)}})
				b.StopTimer()
				if err != nil || response.Status != rhiza.MutationCommitted || response.RowsAffected != 256 {
					b.Fatalf("cleanup=%+v err=%v", response, err)
				}
				rows, err := db.Query(ctx, rhiza.QueryRequest{SQL: `SELECT COUNT(*) FROM cache_benchmark WHERE expires_at <= 2`})
				if err != nil || len(rows.Rows) != 1 || len(rows.Rows[0]) != 1 || rows.Rows[0][0] != int64(backlog-256) {
					b.Fatalf("retained=%#v err=%v", rows.Rows, err)
				}
				b.ReportMetric(float64(backlog-256), "expired-retained")
				if err := db.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func setupCacheBenchmark(b testing.TB, ctx context.Context, db *rhiza.DB, rows int) {
	b.Helper()
	response, err := db.Execute(ctx, rhiza.ExecuteRequest{RequestID: "schema", SQL: `CREATE TABLE cache_benchmark (namespace INTEGER NOT NULL, cache_key TEXT NOT NULL, value BLOB NOT NULL, expires_at INTEGER, PRIMARY KEY(namespace, cache_key))`})
	if err != nil || response.Status != rhiza.MutationCommitted {
		b.Fatalf("schema=%+v err=%v", response, err)
	}
	response, err = db.Execute(ctx, rhiza.ExecuteRequest{RequestID: "index", SQL: `CREATE INDEX cache_benchmark_expiry ON cache_benchmark(expires_at, namespace, cache_key)`})
	if err != nil || response.Status != rhiza.MutationCommitted {
		b.Fatalf("index=%+v err=%v", response, err)
	}
	for start := 0; start < rows; start += 64 {
		statements := make([]rhiza.SQLStatement, 0, min(64, rows-start))
		for i := start; i < min(start+64, rows); i++ {
			statements = append(statements, rhiza.SQLStatement{SQL: `INSERT INTO cache_benchmark(namespace, cache_key, value, expires_at) VALUES (?, ?, ?, ?)`, Args: []any{int64(1), fmt.Sprintf("key-%05d", i), []byte("value"), int64(2)}})
		}
		response, err = db.Execute(ctx, rhiza.ExecuteRequest{RequestID: fmt.Sprintf("seed-%d", start), Statements: statements})
		if err != nil || response.Status != rhiza.MutationCommitted {
			b.Fatalf("seed %d: response=%+v err=%v", start, response, err)
		}
	}
}

func assertCacheKeys(t testing.TB, db *rhiza.DB, ctx context.Context, want ...string) {
	t.Helper()
	response, err := db.Query(ctx, rhiza.QueryRequest{SQL: `SELECT cache_key FROM app_cache ORDER BY cache_key`})
	if err != nil || len(response.Rows) != len(want) {
		t.Fatalf("rows=%#v want=%q err=%v", response.Rows, want, err)
	}
	for i, key := range want {
		if response.Rows[i][0] != key {
			t.Fatalf("row %d=%v, want %q", i, response.Rows[i], key)
		}
	}
}
