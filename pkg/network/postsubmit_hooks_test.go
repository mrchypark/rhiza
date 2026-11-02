//go:build rhiza_local_testhooks

package network

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestPostSubmitKVSecondLookupFailureIsUnknown(t *testing.T) {
	ctx := context.Background()
	core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}}, nil, nil)
	path := t.TempDir() + "/state.sqlite"
	m, err := materializer.Open(path, 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	s := NewServer(core, m, "cluster", true, nil)
	t.Cleanup(s.Close)
	req := KVMutationRequest{RequestID: "second-é/\x00original", Key: "key", Value: []byte("original-value")}
	var calls atomic.Int32
	original := s.kvBatcher.propose
	s.kvBatcher.propose = func(ctx context.Context, value []byte) (quepaxa.Slot, error) {
		slot, err := original(ctx, value)
		if err == nil && slot != 0 {
			calls.Add(1)
		}
		return slot, err
	}
	var entered int
	var beforeClose types.MutationReceipt
	var closeErr error
	restore := localtesthooks.Set(func(name string) {
		if name != "network:kv-postsubmit:before-receipt" {
			return
		}
		entered++
		var found bool
		beforeClose, found, closeErr = m.MutationReceipt(ctx, types.MutationKV, req.RequestID)
		if closeErr != nil || !found {
			t.Errorf("actual receipt before Close: %+v found=%v err=%v", beforeClose, found, closeErr)
			return
		}
		closeErr = m.Close()
	})
	defer restore()
	_, err = s.KVMutate(ctx, "put", req)
	if entered != 1 || closeErr != nil || calls.Load() != 1 || beforeClose.Slot == 0 {
		t.Fatalf("first lookup boundary not calibrated: entered=%d close=%v calls=%d receipt=%+v", entered, closeErr, calls.Load(), beforeClose)
	}
	t.Logf("actual second receipt lookup failure: slot=%d err=%v", beforeClose.Slot, err)
	var unknown *CommitUnknownError
	if !errors.As(err, &unknown) || !errors.Is(err, sql.ErrConnDone) || errors.Is(err, ErrRequestConflict) || unknown.RequestID != req.RequestID || uint64(unknown.Slot) != beforeClose.Slot || unknown.Cause == nil {
		t.Fatalf("second lookup lost uncertainty/identity/cause: %v", err)
	}
	assertPostSubmitHTTP(t, err, http.StatusServiceUnavailable, "commit_unknown", req.RequestID, beforeClose.Slot)
	s.Close()
	reopened, err := materializer.Open(path, 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	observer := NewServer(core, reopened, "cluster", true, nil)
	t.Cleanup(observer.Close)
	before := core.Tip()
	status, err := observer.RequestStatus(ctx, RequestStatusRequest{Kind: "kv", RequestID: req.RequestID})
	if err != nil || status.Receipt == nil || *status.Receipt != beforeClose {
		t.Fatalf("original retained receipt: %+v %v", status, err)
	}
	value, err := observer.KVGet(ctx, KVGetRequest{Key: req.Key})
	if err != nil || !value.Found || string(value.Value) != string(req.Value) || core.Tip() != before || calls.Load() != 1 {
		t.Fatalf("observation changed original effect/proposal: %+v err=%v", value, err)
	}
}
