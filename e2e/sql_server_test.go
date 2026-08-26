package e2e

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

var baseURL = strings.TrimRight(os.Getenv("RHIZA_E2E_URL"), "/")

type queryResponse struct {
	Columns []string `json:"columns"`
	Rows    [][]any  `json:"rows"`
}

func TestSQLServer(t *testing.T) {
	if baseURL == "" {
		t.Skip("set RHIZA_E2E_URL to run the SQL server E2E test")
	}

	res, err := http.Get(baseURL + "/ready")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("ready: %s", res.Status)
	}

	table := fmt.Sprintf("e2e_%d", time.Now().UnixNano())
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	post(t, "/sql/execute", map[string]string{
		"request_id": "schema-" + suffix,
		"sql":        "CREATE TABLE " + table + " (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
	}, nil)
	post(t, "/sql/execute", map[string]string{
		"request_id": "insert-" + suffix,
		"sql":        "INSERT INTO " + table + " (id, name) VALUES (1, 'Ada')",
	}, nil)

	var got queryResponse
	post(t, "/sql/query", map[string]string{
		"sql": "SELECT id, name FROM " + table,
	}, &got)
	if len(got.Rows) != 1 || got.Rows[0][0] != float64(1) || got.Rows[0][1] != "Ada" {
		t.Fatalf("unexpected query result: %+v", got)
	}

	var transaction struct {
		Success bool `json:"success"`
	}
	post(t, "/sql/transaction", map[string]any{
		"request_id": "transaction-" + suffix,
		"statements": []map[string]any{{"sql": "INSERT INTO " + table + " (id, name) VALUES (?, ?) RETURNING id", "args": []any{2, "Grace"}, "want_rows": true}},
	}, &transaction)
	if !transaction.Success {
		t.Fatal("transaction was not committed")
	}
	post(t, "/sql/query", map[string]any{"sql": "SELECT name FROM " + table + " WHERE id = ?", "args": []any{2}, "consistency": "linearizable"}, &got)
	if len(got.Rows) != 1 || got.Rows[0][0] != "Grace" {
		t.Fatalf("bound query result: %+v", got)
	}

	ftsTable := "fts_" + suffix
	post(t, "/sql/transaction", map[string]any{
		"request_id": "surface-" + suffix,
		"statements": []map[string]any{
			{"sql": "CREATE VIRTUAL TABLE " + ftsTable + " USING fts5(name)"},
			{"sql": "INSERT INTO " + ftsTable + "(name) SELECT name FROM " + table},
			{"sql": "WITH RECURSIVE seq(n) AS (VALUES(1) UNION ALL SELECT n + 1 FROM seq WHERE n < 3) SELECT json_array(group_concat(n, '')) FROM seq", "want_rows": true},
		},
	}, &transaction)
	if !transaction.Success {
		t.Fatal("SQLite feature transaction was not committed")
	}
	post(t, "/sql/query", map[string]any{"sql": "SELECT name, row_number() OVER (ORDER BY name) FROM " + ftsTable + " WHERE " + ftsTable + " MATCH ?", "args": []any{"Grace"}}, &got)
	if len(got.Rows) != 1 || got.Rows[0][0] != "Grace" || got.Rows[0][1] != float64(1) {
		t.Fatalf("SQLite FTS/window result: %+v", got)
	}

	key := "e2e-" + suffix
	post(t, "/kv/put", map[string]any{"request_id": "kv-put-" + suffix, "key": key, "value": []byte("value")}, nil)
	var kv struct {
		Found bool   `json:"found"`
		Value []byte `json:"value"`
	}
	post(t, "/kv/get", map[string]any{"key": key, "consistency": "linearizable"}, &kv)
	if !kv.Found || string(kv.Value) != "value" {
		t.Fatalf("KV result: %+v", kv)
	}
	post(t, "/kv/cas", map[string]any{"request_id": "kv-cas-" + suffix, "key": key, "expected": []byte("value"), "expected_exists": true, "value": []byte("changed")}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/notify/subscribe?topic=e2e", nil)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Body.Close()
	reader := bufio.NewReader(stream.Body)
	if line, _ := reader.ReadString('\n'); line != ": connected\n" {
		t.Fatalf("notify connection: %q", line)
	}
	_, _ = reader.ReadString('\n')
	post(t, "/notify/publish", map[string]any{"request_id": "notify-" + suffix, "topic": "e2e", "payload": []byte("ready")}, nil)
	if line, _ := reader.ReadString('\n'); !strings.Contains(line, "cmVhZHk=") {
		t.Fatalf("notify event: %q", line)
	}
}

func BenchmarkSQLServerQueryLocal(b *testing.B) {
	benchmarkRequests(b, []byte(`{"sql":"SELECT 1","consistency":"local"}`), "/sql/query")
}

func BenchmarkSQLServerQueryLinearizable(b *testing.B) {
	benchmarkRequests(b, []byte(`{"sql":"SELECT 1","consistency":"linearizable"}`), "/sql/query")
}

func BenchmarkSQLServerInsert(b *testing.B) {
	if baseURL == "" {
		b.Skip("set RHIZA_E2E_URL to run SQL server benchmarks")
	}
	post(b, "/sql/execute", map[string]string{
		"request_id": "benchmark-schema",
		"sql":        "CREATE TABLE IF NOT EXISTS benchmark_writes (value INTEGER NOT NULL)",
	}, nil)
	var before queryResponse
	post(b, "/sql/query", map[string]string{"sql": "SELECT COUNT(*) FROM benchmark_writes"}, &before)
	beforeCount := int(before.Rows[0][0].(float64))
	prefix := time.Now().UnixNano()
	var requestID atomic.Uint64
	benchmarkDynamicRequests(b, "/sql/execute", func() []byte {
		return []byte(fmt.Sprintf(`{"request_id":"benchmark-%d-%d","sql":"INSERT INTO benchmark_writes(value) VALUES (1)"}`, prefix, requestID.Add(1)))
	})
	b.StopTimer()
	var after queryResponse
	post(b, "/sql/query", map[string]string{"sql": "SELECT COUNT(*) FROM benchmark_writes"}, &after)
	if delta := int(after.Rows[0][0].(float64)) - beforeCount; delta != b.N {
		b.Fatalf("committed rows=%d, requests=%d", delta, b.N)
	}
}

func BenchmarkKVGetLocal(b *testing.B) {
	benchmarkKVGet(b, "local")
}

func BenchmarkKVGetLinearizable(b *testing.B) {
	benchmarkKVGet(b, "linearizable")
}

func benchmarkKVGet(b *testing.B, consistency string) {
	if baseURL == "" {
		b.Skip("set RHIZA_E2E_URL to run KV benchmarks")
	}
	key := fmt.Sprintf("benchmark-kv-%d", time.Now().UnixNano())
	post(b, "/kv/put", map[string]any{"request_id": "seed-" + key, "key": key, "value": []byte("value")}, nil)
	body, _ := json.Marshal(map[string]any{"key": key, "consistency": consistency})
	benchmarkRequests(b, body, "/kv/get")
}

func BenchmarkKVPut(b *testing.B) {
	if baseURL == "" {
		b.Skip("set RHIZA_E2E_URL to run KV benchmarks")
	}
	prefix := time.Now().UnixNano()
	var requestID atomic.Uint64
	benchmarkDynamicRequests(b, "/kv/put", func() []byte {
		id := requestID.Add(1)
		return []byte(fmt.Sprintf(`{"request_id":"kv-%d-%d","key":"kv-%d-%d","value":"dg=="}`, prefix, id, prefix, id))
	})
}

func benchmarkRequests(b *testing.B, body []byte, path string) {
	benchmarkDynamicRequests(b, path, func() []byte { return body })
}

func benchmarkDynamicRequests(b *testing.B, path string, body func() []byte) {
	benchmarkDynamicRequestsAt(b, baseURL, path, body)
}

func benchmarkDynamicRequestsAt(b *testing.B, target, path string, body func() []byte) {
	if target == "" {
		b.Skip("set RHIZA_E2E_URL to run SQL server benchmarks")
	}
	client := &http.Client{Timeout: 10 * time.Second}
	var failures atomic.Uint64
	var firstFailure atomic.Value
	recordFailure := func(message string) {
		if failures.Add(1) == 1 {
			firstFailure.Store(message)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req, err := http.NewRequest(http.MethodPost, target+path, bytes.NewReader(body()))
			if err != nil {
				recordFailure(err.Error())
				continue
			}
			req.Header.Set("Content-Type", "application/json")
			res, err := client.Do(req)
			if err != nil {
				recordFailure(err.Error())
				continue
			}
			responseBody, readErr := io.ReadAll(res.Body)
			res.Body.Close()
			if readErr != nil || res.StatusCode != http.StatusOK {
				recordFailure(fmt.Sprintf("status=%s read=%v body=%s", res.Status, readErr, responseBody))
			}
		}
	})
	if count := failures.Load(); count > 0 {
		b.Fatalf("request failures=%d first=%s", count, firstFailure.Load())
	}
}

func post(tb testing.TB, path string, request, response any) {
	tb.Helper()
	body, err := json.Marshal(request)
	if err != nil {
		tb.Fatal(err)
	}
	res, err := http.Post(baseURL+path, "application/json", bytes.NewReader(body))
	if err != nil {
		tb.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		message, _ := io.ReadAll(res.Body)
		tb.Fatalf("%s: %s: %s", path, res.Status, message)
	}
	if response != nil {
		if err := json.NewDecoder(res.Body).Decode(response); err != nil {
			tb.Fatal(err)
		}
	}
}
