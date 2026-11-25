//go:build rhiza_local_testhooks

package network

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// The injected lookup miss is not a real expiration: retention is tested separately.
func TestPostSubmitMissingReceiptBeforeProposerReturns(t *testing.T) {
	for _, kind := range []string{"sql", "kv"} {
		t.Run(kind, func(t *testing.T) {
			var armed atomic.Bool
			var calls atomic.Int32
			var submitted atomic.Uint64
			id := kind + "-injected-missing"
			ctx := context.WithValue(context.Background(), postSubmitMissingKey{}, func(gotKind, gotID string) bool {
				return armed.CompareAndSwap(true, false) && gotKind == kind && gotID == id
			})
			core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}}, nil, nil)
			m, err := materializer.Open(t.TempDir()+"/state.sqlite", 1, 1024)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = m.Close() })
			s := NewServer(core, m, "cluster", true, nil)
			t.Cleanup(s.Close)
			if _, err := s.Execute(ctx, ExecuteRequest{RequestID: "setup", SQL: "CREATE TABLE counter (n INTEGER)"}); err != nil {
				t.Fatal(err)
			}
			wrap := func(original func(context.Context, []byte) (quepaxa.Slot, error)) func(context.Context, []byte) (quepaxa.Slot, error) {
				return func(callCtx context.Context, value []byte) (quepaxa.Slot, error) {
					slot, err := original(callCtx, value)
					if err != nil || slot == 0 {
						return slot, err
					}
					calls.Add(1)
					submitted.Store(uint64(slot))
					if err := s.applyDecisions(callCtx, slot); err != nil {
						return slot, err
					}
					armed.Store(true)
					return slot, nil
				}
			}
			req := ExecuteRequest{RequestID: id, SQL: "INSERT INTO counter VALUES (1) RETURNING n", WantRows: true}
			if kind == "sql" {
				s.sqlBatcher.propose = wrap(s.sqlBatcher.propose)
				_, err = s.ExecuteReturning(ctx, req)
			} else {
				s.kvBatcher.propose = wrap(s.kvBatcher.propose)
				_, err = s.KVMutate(ctx, "put", KVMutationRequest{RequestID: id, Key: "key", Value: []byte("value")})
			}
			var unknown *CommitUnknownError
			if !errors.As(err, &unknown) || unknown.RequestID != id || uint64(unknown.Slot) != submitted.Load() || unknown.Slot == 0 || unknown.Cause == nil || errors.Is(err, ErrRequestConflict) {
				t.Fatalf("missing receipt: %v", err)
			}
			if armed.Load() {
				t.Fatal("receipt-miss injection was not consumed")
			}
			assertPostSubmitHTTP(t, err, http.StatusServiceUnavailable, "commit_unknown", id, submitted.Load())
			before := core.Tip()
			if kind == "sql" {
				result, err := s.ResolveExecute(ctx, req)
				if err != nil || result.State != ResolveExecuteRetained || result.Receipt == nil || result.Receipt.Slot != submitted.Load() {
					t.Fatalf("resolve: %+v %v", result, err)
				}
				rows, err := s.Query(ctx, QueryRequest{SQL: "SELECT n FROM counter"})
				if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != int64(1) {
					t.Fatalf("effect: %+v %v", rows, err)
				}
			} else {
				value, err := s.KVGet(ctx, KVGetRequest{Key: "key"})
				if err != nil || !value.Found || string(value.Value) != "value" {
					t.Fatalf("effect: %+v %v", value, err)
				}
			}
			if calls.Load() != 1 || core.Tip() != before {
				t.Fatalf("observation resubmitted: calls=%d tip=%d->%d", calls.Load(), before, core.Tip())
			}
		})
	}
}
