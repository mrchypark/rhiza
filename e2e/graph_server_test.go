package e2e

import (
	"bytes"
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

func TestGraphServer(t *testing.T) {
	graphURL := strings.TrimRight(os.Getenv("RHIZA_GRAPH_E2E_URL"), "/")
	if graphURL == "" {
		t.Skip("set RHIZA_GRAPH_E2E_URL to run the graph server E2E test")
	}
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	adaID, graceID := "ada-"+suffix, "grace-"+suffix
	for _, person := range []struct{ id, name string }{{adaID, "Ada"}, {graceID, "Grace"}} {
		postGraph(t, graphURL, "/graph/execute", map[string]any{
			"request_id": "person-" + person.id,
			"cypher":     `CREATE (:Person {id: $id, name: $name})`,
			"args":       map[string]any{"id": person.id, "name": person.name},
		}, nil)
	}
	postGraph(t, graphURL, "/graph/execute", map[string]any{
		"request_id": "knows-" + suffix,
		"cypher":     `MATCH (from:Person {id: $from}), (to:Person {id: $to}) CREATE (from)-[:Knows {since: $since}]->(to)`,
		"args":       map[string]any{"from": adaID, "to": graceID, "since": 2026},
	}, nil)
	var result queryResponse
	postGraph(t, graphURL, "/graph/query", map[string]any{
		"cypher":      `MATCH (person:Person {id: $id})-[:Knows]->(friend:Person) RETURN friend.name`,
		"args":        map[string]any{"id": adaID},
		"consistency": "linearizable",
	}, &result)
	if len(result.Rows) != 1 || result.Rows[0][0] != "Grace" {
		t.Fatalf("unexpected traversal: %+v", result)
	}
	postGraph(t, graphURL, "/kv/put", map[string]any{"request_id": "kv-" + suffix, "key": "graph-" + suffix, "value": []byte("ok")}, nil)
}

func BenchmarkGraphQueryLocal(b *testing.B) {
	benchmarkGraphRequests(b, []byte(`{"cypher":"MATCH (n) RETURN n LIMIT 1","consistency":"local"}`), "/graph/query")
}

func BenchmarkGraphQueryLinearizable(b *testing.B) {
	benchmarkGraphRequests(b, []byte(`{"cypher":"MATCH (n) RETURN n LIMIT 1","consistency":"linearizable"}`), "/graph/query")
}

func BenchmarkGraphCreate(b *testing.B) {
	target := strings.TrimRight(os.Getenv("RHIZA_GRAPH_E2E_URL"), "/")
	if target == "" {
		b.Skip("set RHIZA_GRAPH_E2E_URL to run graph benchmarks")
	}
	prefix := time.Now().UnixNano()
	var requestID atomic.Uint64
	benchmarkDynamicRequestsAt(b, target, "/graph/execute", func() []byte {
		id := requestID.Add(1)
		return []byte(fmt.Sprintf(`{"request_id":"graph-%d-%d","cypher":"CREATE (:BenchmarkNode {id: $id})","args":{"id":"%d-%d"}}`, prefix, id, prefix, id))
	})
}

func benchmarkGraphRequests(b *testing.B, body []byte, path string) {
	target := strings.TrimRight(os.Getenv("RHIZA_GRAPH_E2E_URL"), "/")
	benchmarkDynamicRequestsAt(b, target, path, func() []byte { return body })
}

func postGraph(tb testing.TB, base, path string, request, response any) {
	tb.Helper()
	body, err := json.Marshal(request)
	if err != nil {
		tb.Fatal(err)
	}
	res, err := http.Post(base+path, "application/json", bytes.NewReader(body))
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
