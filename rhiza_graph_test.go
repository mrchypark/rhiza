package rhiza_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza"
)

func TestEmbeddedGraphGoAPI(t *testing.T) {
	db, err := rhiza.Open(context.Background(), rhiza.Config{NodeID: "n1", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.GraphExecute(context.Background(), rhiza.GraphCommand{RequestID: "schema", Cypher: "CREATE NODE TABLE Tea(id STRING, name STRING, PRIMARY KEY(id))"}); err != nil {
		t.Fatal(err)
	}
	insert := rhiza.GraphCommand{
		RequestID: "insert",
		Cypher:    "CREATE (:Tea {id: '1', name: 'green'})",
		Events: []rhiza.GraphStreamEvent{{
			Stream: "tea-events", Kind: "tea.created", Payload: map[string]any{"id": "1", "name": "green"},
		}},
	}
	if _, err := db.GraphExecute(context.Background(), insert); err != nil {
		t.Fatal(err)
	}
	if _, err := db.GraphExecute(context.Background(), insert); err != nil {
		t.Fatal(err)
	}
	result, err := db.GraphQuery(context.Background(), rhiza.GraphQueryRequest{Cypher: "MATCH (n:Tea) RETURN n.name", Consistency: rhiza.ConsistencyLocal})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("unexpected rows: %#v", result.Rows)
	}
	changes, err := db.GraphChanges(context.Background(), rhiza.GraphStreamReadRequest{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	foundInsert := false
	for _, change := range changes.Records {
		foundInsert = foundInsert || change.Kind == "node.insert"
	}
	if !foundInsert {
		t.Fatalf("missing built-in graph change: %#v", changes.Records)
	}
	events, err := db.GraphStreamRead(context.Background(), rhiza.GraphStreamReadRequest{Stream: "tea-events", Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(events.Records) != 1 || events.Records[0].Kind != "tea.created" || !reflect.DeepEqual(events.Records[0].Payload, map[string]any{"id": "1", "name": "green"}) {
		t.Fatalf("unexpected events: %#v", events.Records)
	}
	request := httptest.NewRequest(http.MethodPost, "/graph/streams/read", strings.NewReader(`{"stream":"tea-events","limit":100}`))
	response := httptest.NewRecorder()
	db.ServeHTTP(response, request)
	var httpEvents rhiza.GraphStreamReadResponse
	if response.Code != http.StatusOK {
		t.Fatalf("stream HTTP status=%d body=%s", response.Code, response.Body.String())
	}
	if err := json.NewDecoder(response.Body).Decode(&httpEvents); err != nil || len(httpEvents.Records) != 1 {
		t.Fatalf("stream HTTP response=%#v err=%v", httpEvents, err)
	}
	if err := db.SetGraphStreamOffset(context.Background(), rhiza.GraphStreamOffsetRequest{RequestID: "offset-1", Stream: "tea-events", Consumer: "worker-a", Sequence: events.Records[0].Sequence}); err != nil {
		t.Fatal(err)
	}
	offset, err := db.GraphStreamOffset(context.Background(), rhiza.GraphStreamOffsetRequest{Stream: "tea-events", Consumer: "worker-a"})
	if err != nil || !offset.Found || offset.Sequence != events.Records[0].Sequence {
		t.Fatalf("unexpected offset: %+v err=%v", offset, err)
	}
	if err := db.TrimGraphStream(context.Background(), rhiza.GraphStreamTrimRequest{RequestID: "trim-1", Stream: "tea-events", ThroughSequence: events.Records[0].Sequence}); err != nil {
		t.Fatal(err)
	}
	events, err = db.GraphStreamRead(context.Background(), rhiza.GraphStreamReadRequest{Stream: "tea-events", Limit: 100})
	if err != nil || len(events.Records) != 0 {
		t.Fatalf("trimmed events: %#v err=%v", events.Records, err)
	}
}

func TestEmbeddedGraphAfterCloseReturnsErrors(t *testing.T) {
	db, err := rhiza.Open(context.Background(), rhiza.Config{NodeID: "n1", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.GraphQuery(context.Background(), rhiza.GraphQueryRequest{Cypher: "MATCH (n) RETURN n", Consistency: rhiza.ConsistencyLocal}); err == nil {
		t.Fatal("GraphQuery succeeded after close")
	}
	if _, err := db.RequestStatus(context.Background(), rhiza.RequestStatusRequest{Kind: "graph", RequestID: "closed"}); err == nil {
		t.Fatal("graph RequestStatus succeeded after close")
	}
}
