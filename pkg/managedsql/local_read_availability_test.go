package managedsql

import (
	"context"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
)

func TestManagedLocalSQLAndKVReadsBypassWriteSerialization(t *testing.T) {
	_, hosts, fixture := mixedHostQuorum(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	h := hosts[0]
	if _, err := h.Execute(ctx, ExecuteRequest{RequestID: "local-read-schema", SQL: "CREATE TABLE local_read(value TEXT)"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Execute(ctx, ExecuteRequest{RequestID: "local-read-row", SQL: "INSERT INTO local_read VALUES ('sql')"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.KVMutate(ctx, "put", network.KVMutationRequest{RequestID: "local-kv-row", Key: "kv", Value: []byte("kv")}); err != nil {
		t.Fatal(err)
	}
	for _, other := range hosts[1:] {
		if err := fixture.closeHost(other); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.Ready(ctx); err != nil {
		t.Fatalf("last voter lost local readiness: %v", err)
	}
	linearCtx, stop := context.WithTimeout(ctx, 500*time.Millisecond)
	if _, err := h.Query(linearCtx, QueryRequest{SQL: "SELECT 1", Consistency: "linearizable"}); err == nil {
		stop()
		t.Fatal("last voter served a linearizable SQL read")
	}
	stop()
	linearCtx, stop = context.WithTimeout(ctx, 500*time.Millisecond)
	if _, err := h.KVGet(linearCtx, network.KVGetRequest{Key: "kv", Consistency: "linearizable"}); err == nil {
		stop()
		t.Fatal("last voter served a linearizable KV read")
	}
	stop()
	writeCtx, stop := context.WithTimeout(ctx, 500*time.Millisecond)
	if _, err := h.Execute(writeCtx, ExecuteRequest{RequestID: "must-not-write", SQL: "INSERT INTO local_read VALUES ('blocked')"}); err == nil {
		stop()
		t.Fatal("last voter acknowledged SQL without quorum")
	}
	stop()
	writeCtx, stop = context.WithTimeout(ctx, 500*time.Millisecond)
	if result, err := h.KVMutate(writeCtx, "put", network.KVMutationRequest{RequestID: "kv-must-not-write", Key: "blocked", Value: []byte("blocked")}); err == nil || result.Slot != 0 {
		stop()
		t.Fatalf("last voter acknowledged KV without quorum: %+v", result)
	}
	stop()

	// User mutations serialize on this lock. A mutation waiting for quorum must
	// not prevent an explicit local read of already certified local data.
	h.mu.Lock()
	defer h.mu.Unlock()
	read := make(chan error, 3)
	go func() {
		rows, err := h.Query(ctx, QueryRequest{SQL: "SELECT value FROM local_read", Consistency: "local"})
		if err == nil && (len(rows.Rows) != 1 || len(rows.Rows[0]) != 1 || rows.Rows[0][0] != "sql") {
			err = network.ErrInvalidRequest
		}
		read <- err
	}()
	go func() {
		value, err := h.KVGet(ctx, network.KVGetRequest{Key: "kv", Consistency: "local"})
		if err == nil && (!value.Found || string(value.Value) != "kv") {
			err = network.ErrInvalidRequest
		}
		read <- err
	}()
	go func() { read <- h.Ready(ctx) }()
	for range 3 {
		select {
		case err := <-read:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("local SQL/KV read waited behind write serialization")
		}
	}
}

func TestManagedLocalGraphReadBypassesWriteSerialization(t *testing.T) {
	configs, fixture := sqlClusterForProtocol(t, sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	hosts := make([]*Host, len(configs))
	for i, cfg := range configs {
		h, err := fixture.open(ctx, cfg)
		if h != nil {
			t.Cleanup(func() { _ = h.Close() })
		}
		if err != nil {
			t.Fatalf("open public Graph Host %d: %v", i, err)
		}
		hosts[i] = h
	}
	h := hosts[0]
	if _, err := h.GraphExecute(ctx, types.GraphCommand{RequestID: "local-graph-row", Cypher: "CREATE (:LocalRead {value: 'graph'})"}); err != nil {
		t.Fatal(err)
	}
	for _, other := range hosts[1:] {
		if err := fixture.closeHost(other); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.Ready(ctx); err != nil {
		t.Fatalf("last Graph voter lost local readiness: %v", err)
	}
	linearCtx, stop := context.WithTimeout(ctx, 500*time.Millisecond)
	if _, err := h.GraphQuery(linearCtx, network.GraphQueryRequest{Cypher: "RETURN 1", Consistency: "linearizable"}); err == nil {
		stop()
		t.Fatal("last Graph voter served a linearizable read")
	}
	stop()
	writeCtx, stop := context.WithTimeout(ctx, 500*time.Millisecond)
	if result, err := h.GraphExecute(writeCtx, types.GraphCommand{RequestID: "graph-must-not-write", Cypher: "CREATE (:MustNotWrite)"}); err == nil || result.Slot != 0 {
		stop()
		t.Fatalf("last Graph voter acknowledged without quorum: %+v", result)
	}
	stop()

	h.mu.Lock()
	defer h.mu.Unlock()
	read := make(chan error, 1)
	go func() {
		rows, err := h.GraphQuery(ctx, network.GraphQueryRequest{Cypher: "MATCH (n:LocalRead) RETURN n.value", Consistency: "local"})
		if err == nil && (len(rows.Rows) != 1 || len(rows.Rows[0]) != 1 || rows.Rows[0][0] != "graph") {
			err = network.ErrInvalidRequest
		}
		read <- err
	}()
	select {
	case err := <-read:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("local Graph read waited behind write serialization")
	}
}
