package network

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// The proposer is real: it records, certifies and applies the mutation before
// closing the actual SQLite materializer, not a fake successful submitter.
func TestPostSubmitLookupFailureIsUnknown(t *testing.T) {
	for _, kind := range []string{"sql", "kv"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}}, nil, nil)
			path := t.TempDir() + "/state.sqlite"
			m, err := materializer.Open(path, 1, 1024)
			if err != nil {
				t.Fatal(err)
			}
			s := NewServer(core, m, "cluster", true, nil)
			t.Cleanup(s.Close)
			t.Cleanup(func() { _ = m.Close() })
			if _, err := s.Execute(ctx, ExecuteRequest{RequestID: "setup", SQL: "CREATE TABLE counter (n INTEGER)"}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Execute(ctx, ExecuteRequest{RequestID: "seed", SQL: "INSERT INTO counter VALUES (0)"}); err != nil {
				t.Fatal(err)
			}
			id := kind + "-é/\x00original"
			var submitted atomic.Uint64
			var calls atomic.Int32
			wrap := func(original func(context.Context, []byte) (quepaxa.Slot, error)) func(context.Context, []byte) (quepaxa.Slot, error) {
				return func(ctx context.Context, value []byte) (quepaxa.Slot, error) {
					slot, err := original(ctx, value)
					if err != nil || slot == 0 {
						return slot, err
					}
					calls.Add(1)
					submitted.Store(uint64(slot))
					if err := m.Close(); err != nil {
						return slot, fmt.Errorf("close after real apply: %w", err)
					}
					return slot, nil
				}
			}
			req := ExecuteRequest{RequestID: id, SQL: "UPDATE counter SET n=n+1 RETURNING n", WantRows: true}
			if kind == "sql" {
				s.sqlBatcher.propose = wrap(s.sqlBatcher.propose)
				_, err = s.ExecuteReturning(ctx, req)
			} else {
				s.kvBatcher.propose = wrap(s.kvBatcher.propose)
				_, err = s.KVMutate(ctx, "put", KVMutationRequest{RequestID: id, Key: "key", Value: []byte("value")})
			}
			t.Logf("actual post-submit result: kind=%s slot=%d calls=%d err=%v", kind, submitted.Load(), calls.Load(), err)
			var unknown *CommitUnknownError
			if !errors.As(err, &unknown) || !errors.Is(err, ErrCommitUnknown) || errors.Is(err, ErrRequestConflict) {
				t.Fatalf("post-submit lookup error must be unknown, not conflict: %v", err)
			}
			if unknown.RequestID != id || uint64(unknown.Slot) != submitted.Load() || unknown.Slot == 0 || !errors.Is(err, sql.ErrConnDone) {
				t.Fatalf("lost original identity/slot/cause: %+v", unknown)
			}
			assertPostSubmitHTTP(t, err, http.StatusServiceUnavailable, "commit_unknown", id, submitted.Load())
			s.Close()
			reopened, err := materializer.Open(path, 1, 1024)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reopened.Close() })
			observer := NewServer(core, reopened, "cluster", true, nil)
			t.Cleanup(observer.Close)
			before := core.Tip()
			if kind == "sql" {
				result, err := observer.ResolveExecute(ctx, req)
				if err != nil || result.State != ResolveExecuteRetained || result.Receipt == nil || result.Receipt.Slot != submitted.Load() {
					t.Fatalf("resolve original: %+v, %v", result, err)
				}
				rows, err := observer.Query(ctx, QueryRequest{SQL: "SELECT n FROM counter"})
				if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != int64(1) {
					t.Fatalf("application executed more than once: %+v, %v", rows, err)
				}
			} else {
				status, err := observer.RequestStatus(ctx, RequestStatusRequest{Kind: "kv", RequestID: id})
				if err != nil || status.Receipt == nil || status.Receipt.Slot != submitted.Load() {
					t.Fatalf("retained KV: %+v, %v", status, err)
				}
				value, err := observer.KVGet(ctx, KVGetRequest{Key: "key"})
				if err != nil || !value.Found || string(value.Value) != "value" {
					t.Fatalf("original KV effect: %+v %v", value, err)
				}
			}
			if core.Tip() != before || calls.Load() != 1 {
				t.Fatalf("observation resubmitted: tip %d -> %d, calls %d", before, core.Tip(), calls.Load())
			}
		})
	}
}

func assertPostSubmitHTTP(t *testing.T, err error, status int, code, id string, slot uint64) {
	t.Helper()
	w := httptest.NewRecorder()
	writeAPIError(w, err)
	var body ErrorResponse
	if decodeErr := json.Unmarshal(w.Body.Bytes(), &body); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if w.Code != status || body.Code != code || body.RequestID != id || body.Slot != slot {
		t.Fatalf("HTTP status=%d body=%+v", w.Code, body)
	}
}

func TestPostSubmitActualConflictRemainsConflict(t *testing.T) {
	s, core, req, _ := resolveExecuteFixture(t)
	before := core.Tip()
	req.SQL = "INSERT INTO resolution VALUES (8)"
	_, err := s.Execute(context.Background(), req)
	if !errors.Is(err, ErrRequestConflict) || errors.Is(err, ErrCommitUnknown) {
		t.Fatalf("actual mismatch: %v", err)
	}
	assertPostSubmitHTTP(t, err, http.StatusConflict, "request_conflict", "", 0)
	if core.Tip() != before {
		t.Fatal("conflict submitted a mutation")
	}
}

// A certified mutation may be applied even when its proposer exceeds the real
// batcher deadline before returning; the caller must not infer failure.
func TestPostSubmitRealBatcherDeadlineIsUnknown(t *testing.T) {
	for _, kind := range []string{"sql", "kv"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
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
			var submitted atomic.Uint64
			var calls atomic.Int32
			wrap := func(original func(context.Context, []byte) (quepaxa.Slot, error)) func(context.Context, []byte) (quepaxa.Slot, error) {
				return func(ctx context.Context, value []byte) (quepaxa.Slot, error) {
					slot, err := original(ctx, value)
					if err != nil || slot == 0 {
						return slot, err
					}
					calls.Add(1)
					submitted.Store(uint64(slot))
					if err := s.applyDecisions(ctx, slot); err != nil {
						return slot, err
					}
					<-ctx.Done()
					return slot, ctx.Err()
				}
			}
			id := kind + "-deadline"
			req := ExecuteRequest{RequestID: id, SQL: "INSERT INTO counter VALUES (1) RETURNING n", WantRows: true}
			start := time.Now()
			if kind == "sql" {
				s.sqlBatcher.propose = wrap(s.sqlBatcher.propose)
				_, err = s.ExecuteReturning(ctx, req)
			} else {
				s.kvBatcher.propose = wrap(s.kvBatcher.propose)
				_, err = s.KVMutate(ctx, "put", KVMutationRequest{RequestID: id, Key: "key", Value: []byte("value")})
			}
			if elapsed := time.Since(start); elapsed < 29*time.Second {
				t.Fatalf("batcher deadline expired too early: %v", elapsed)
			}
			var unknown *CommitUnknownError
			if !errors.As(err, &unknown) || !errors.Is(err, context.DeadlineExceeded) || unknown.RequestID != id || uint64(unknown.Slot) != submitted.Load() || unknown.Slot == 0 {
				t.Fatalf("deadline must preserve unknown identity/slot/cause: %v", err)
			}
			assertPostSubmitHTTP(t, err, http.StatusServiceUnavailable, "commit_unknown", id, submitted.Load())
			before := core.Tip()
			if kind == "sql" {
				result, err := s.ResolveExecute(ctx, req)
				if err != nil || result.State != ResolveExecuteRetained || result.Receipt == nil || result.Receipt.Slot != submitted.Load() {
					t.Fatalf("resolve: %+v %v", result, err)
				}
				if len(result.Statements) != 1 || len(result.Statements[0].Rows) != 1 || len(result.Statements[0].Rows[0]) != 1 || result.Statements[0].Rows[0][0] != int64(1) {
					t.Fatalf("resolved RETURNING row: %+v", result.Statements)
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

func TestPostSubmitFalseCASRetainsOriginalReceipt(t *testing.T) {
	ctx := context.Background()
	core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}}, nil, nil)
	m, err := materializer.Open(t.TempDir()+"/state.sqlite", 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	s := NewServer(core, m, "cluster", true, nil)
	t.Cleanup(s.Close)
	req := KVMutationRequest{RequestID: "false-cas", Key: "absent", ExpectedExists: true, Expected: []byte("expected"), Value: []byte("not-applied")}
	first, err := s.KVMutate(ctx, "cas", req)
	if err != nil || first.Applied {
		t.Fatalf("false CAS: %+v %v", first, err)
	}
	before := core.Tip()
	second, err := s.KVMutate(ctx, "cas", req)
	if err != nil || second != first || core.Tip() != before {
		t.Fatalf("retry changed retained receipt/tip: first=%+v second=%+v err=%v", first, second, err)
	}
	value, err := s.KVGet(ctx, KVGetRequest{Key: req.Key})
	if err != nil || value.Found {
		t.Fatalf("false CAS wrote data: %+v %v", value, err)
	}
}
