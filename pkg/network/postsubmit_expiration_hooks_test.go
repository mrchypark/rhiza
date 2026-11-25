//go:build rhiza_local_testhooks

package network

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// Expiration occurs after the batcher has finished, so fixture setup does not
// consume the mutation's internal execution deadline.
func TestPostSubmitCertifiedExpirationOutsideBatcher(t *testing.T) {
	for _, kind := range []string{"sql", "kv"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			const window = 1024
			core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}}, nil, nil)
			m, err := materializer.Open(t.TempDir()+"/state.sqlite", 1, window)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = m.Close() })
			s := NewServer(core, m, "cluster", true, nil)
			t.Cleanup(s.Close)
			if _, err := s.Execute(ctx, ExecuteRequest{RequestID: "setup", SQL: "CREATE TABLE counter (n INTEGER)"}); err != nil {
				t.Fatal(err)
			}
			var originalSlot quepaxa.Slot
			var setupErr error
			calls := 0
			var mutationProposals atomic.Int32
			wrap := func(original func(context.Context, []byte) (quepaxa.Slot, error)) func(context.Context, []byte) (quepaxa.Slot, error) {
				return func(ctx context.Context, value []byte) (quepaxa.Slot, error) {
					mutationProposals.Add(1)
					return original(ctx, value)
				}
			}
			if kind == "sql" {
				s.sqlBatcher.propose = wrap(s.sqlBatcher.propose)
			} else {
				s.kvBatcher.propose = wrap(s.kvBatcher.propose)
			}
			restore := localtesthooks.Set(func(event string) {
				if event != "network:"+kind+"-postsubmit:before-receipt" {
					return
				}
				calls++
				originalSlot = core.Tip()
				barrier := types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{})
				for core.Tip() < originalSlot+window-1 {
					if _, _, err := core.Propose(ctx, barrier); err != nil {
						setupErr = err
						return
					}
				}
				if setupErr = s.applyDecisions(ctx, originalSlot+window-1); setupErr != nil {
					return
				}
				mutationKind := types.MutationSQL
				if kind == "kv" {
					mutationKind = types.MutationKV
				}
				receipt, found, err := m.MutationReceipt(ctx, mutationKind, kind+"-expired")
				if err != nil || !found || receipt.Slot != uint64(originalSlot) {
					setupErr = fmt.Errorf("receipt missing at last retained slot: %+v found=%v: %v", receipt, found, err)
					return
				}
				if core.Tip() < originalSlot+window {
					if _, _, setupErr = core.Propose(ctx, barrier); setupErr != nil {
						return
					}
				}
				setupErr = s.applyDecisions(ctx, core.Tip())
			})
			defer restore()
			id := kind + "-expired"
			req := ExecuteRequest{RequestID: id, SQL: "INSERT INTO counter VALUES (1) RETURNING n", WantRows: true}
			if kind == "sql" {
				_, err = s.ExecuteReturning(ctx, req)
			} else {
				_, err = s.KVMutate(ctx, "put", KVMutationRequest{RequestID: id, Key: "key", Value: []byte("value")})
			}
			if setupErr != nil || calls != 1 || originalSlot == 0 || core.Tip() < originalSlot+window {
				t.Fatalf("expiration setup: calls=%d original=%d tip=%d err=%v", calls, originalSlot, core.Tip(), setupErr)
			}
			var unknown *CommitUnknownError
			if !errors.As(err, &unknown) || unknown.RequestID != id || unknown.Slot != originalSlot || unknown.Cause == nil || unknown.Cause.Error() != fmt.Sprintf("%s mutation receipt is unavailable", map[string]string{"sql": "SQL", "kv": "KV"}[kind]) {
				t.Fatalf("missing receipt must return unknown: %v", err)
			}
			assertPostSubmitHTTP(t, err, http.StatusServiceUnavailable, "commit_unknown", id, uint64(originalSlot))
			status, err := s.RequestStatus(ctx, RequestStatusRequest{Kind: kind, RequestID: id})
			if err != nil || status.Receipt != nil {
				t.Fatalf("expired receipt: %+v %v", status, err)
			}
			before := core.Tip()
			if kind == "sql" {
				result, err := s.ResolveExecute(ctx, req)
				if err != nil || result.State != ResolveExecuteUnknownOrExpired {
					t.Fatalf("expired resolution: %+v %v", result, err)
				}
				rows, err := s.Query(ctx, QueryRequest{SQL: "SELECT n FROM counter"})
				if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != int64(1) {
					t.Fatalf("SQL effect missing or duplicated: %+v %v", rows, err)
				}
			} else {
				value, err := s.KVGet(ctx, KVGetRequest{Key: "key"})
				if err != nil || !value.Found || string(value.Value) != "value" {
					t.Fatalf("KV effect missing: %+v %v", value, err)
				}
			}
			if mutationProposals.Load() != 1 || core.Tip() != before {
				t.Fatalf("observation resubmitted mutation: proposals=%d tip=%d->%d", mutationProposals.Load(), before, core.Tip())
			}
		})
	}
}
