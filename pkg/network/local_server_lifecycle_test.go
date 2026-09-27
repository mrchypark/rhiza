package network

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type observedCancelContext struct {
	context.Context
	observed chan struct{}
}

func (c observedCancelContext) Done() <-chan struct{} {
	select {
	case <-c.observed:
	default:
		close(c.observed)
	}
	return c.Context.Done()
}

func newLocalLifecycleServer(t *testing.T) (*Server, *materializer.Materializer, *qlog.WAL) {
	t.Helper()
	wal, err := qlog.Open(t.TempDir() + "/qlog")
	if err != nil {
		t.Fatal(err)
	}
	core, err := quepaxa.New(quepaxa.Config{
		NodeID: "local", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "local"}}},
		WAL: wal, LocalMode: true,
	})
	if err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	server := NewServer(core, material, "cluster", true, nil)
	t.Cleanup(func() {
		server.Close()
		_ = material.Close()
		_ = wal.Close()
	})
	return server, material, wal
}

func TestExplicitLocalProposalOwnsWholeLifecycleAndDedupCancellation(t *testing.T) {
	server, _, _ := newLocalLifecycleServer(t)
	if cap(server.localCap) != 1 {
		t.Fatalf("local capacity=%d, want 1", cap(server.localCap))
	}
	entered, release := make(chan struct{}), make(chan struct{})
	server.SetDurabilityBarrier(func(ctx context.Context, _ quepaxa.Slot) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "lifecycle", SQL: "CREATE TABLE lifecycle(id INTEGER)"}})
	if err != nil {
		t.Fatal(err)
	}
	owner := make(chan error, 1)
	go func() { _, err := server.propose(context.Background(), value); owner <- err }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("proposal did not reach the durability barrier")
	}
	ctx, cancel := context.WithCancel(context.Background())
	observed := make(chan struct{})
	waiter := make(chan error, 1)
	go func() {
		_, err := server.propose(observedCancelContext{Context: ctx, observed: observed}, value)
		waiter <- err
	}()
	select {
	case <-observed:
	case <-time.After(5 * time.Second):
		t.Fatal("same-hash caller did not join the inflight proposal")
	}
	cancel()
	if err := <-waiter; !errors.Is(err, context.Canceled) || !errors.Is(err, ErrCommitUnknown) {
		t.Fatalf("deduplicated caller error=%v, want canceled commit-unknown", err)
	}
	other := types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{1})
	if _, err := server.propose(context.Background(), other); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("overlap error=%v, want ErrOverloaded", err)
	}
	if !server.Ready() {
		t.Fatal("caller cancellation poisoned the server")
	}
	close(release)
	if err := <-owner; err != nil {
		t.Fatal(err)
	}
	if _, err := server.propose(context.Background(), []byte("invalid")); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("invalid proposal error=%v, want ErrInvalidRequest", err)
	}
	if !server.Ready() {
		t.Fatal("deterministic rejection poisoned the server")
	}
}

func TestExplicitLocalDurabilityFailureIsStickyAndPreservesCause(t *testing.T) {
	server, _, _ := newLocalLifecycleServer(t)
	want := errors.New("durability fixture failure")
	server.SetDurabilityBarrier(func(context.Context, quepaxa.Slot) error { return want })
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "failed", SQL: "CREATE TABLE failed(id INTEGER)"}})
	if err != nil {
		t.Fatal(err)
	}
	slot, err := server.propose(context.Background(), value)
	if slot != 1 || !errors.Is(err, ErrCommitUnknown) || !errors.Is(err, want) || errors.Is(err, ErrDurabilityUnavailable) {
		t.Fatalf("slot=%d error=%v; want slot 1, commit unknown, original cause, and no object-store error", slot, err)
	}
	if server.Ready() {
		t.Fatal("failed local lifecycle remained ready")
	}
	if _, err := server.propose(context.Background(), value); !errors.Is(err, ErrNotReady) || errors.Is(err, ErrCommitUnknown) {
		t.Fatalf("same-value retry error=%v, want ErrNotReady", err)
	}
	if _, err := server.propose(context.Background(), types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{2})); !errors.Is(err, ErrNotReady) {
		t.Fatalf("new proposal error=%v, want ErrNotReady", err)
	}
	if _, err := server.RequestStatus(context.Background(), RequestStatusRequest{RequestID: "failed"}); !errors.Is(err, ErrNotReady) {
		t.Fatalf("request status error=%v, want ErrNotReady", err)
	}
	if _, err := server.Query(context.Background(), QueryRequest{SQL: "SELECT 1", Consistency: "linearizable"}); !errors.Is(err, ErrNotReady) {
		t.Fatalf("query error=%v, want ErrNotReady", err)
	}
	if !errors.Is(server.localFailure(), want) {
		t.Fatalf("sticky failure=%v, want original cause", server.localFailure())
	}
	if _, _, err := server.NotifySubscribe("new-after-failure"); !errors.Is(err, ErrNotReady) {
		t.Fatalf("new notification subscription error=%v, want ErrNotReady", err)
	}
}

func TestExplicitLocalFailedPendingRecoveryAtSlotZeroPoisonsServer(t *testing.T) {
	server, _, wal := newLocalLifecycleServer(t)
	// Force pending-record recovery to fail before allocation by closing the WAL
	// after recording a partial local proposal.
	core := server.core
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "pending", SQL: "CREATE TABLE pending(id INTEGER)"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := core.Record(context.Background(), quepaxa.RecordRequest{Slot: 1, Step: 4, ConfigID: 1, Proposal: quepaxa.Proposal{Priority: quepaxa.Priority{1}, ProposerID: "local", Hash: sha256.Sum256(value), Value: value}}); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	slot, err := server.propose(context.Background(), value)
	if slot != 0 || !errors.Is(err, ErrCommitUnknown) {
		t.Fatalf("slot=%d error=%v; want slot-zero commit-unknown", slot, err)
	}
	if server.Ready() {
		t.Fatal("pending recovery failure did not poison readiness")
	}
	if _, err := server.propose(context.Background(), value); !errors.Is(err, ErrNotReady) || errors.Is(err, ErrCommitUnknown) {
		t.Fatalf("retry error=%v; want ErrNotReady", err)
	}
}

func TestExplicitLocalLinearizableReadAppliesPendingDecisionButSkipsAppliedGate(t *testing.T) {
	server, material, _ := newLocalLifecycleServer(t)
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "read-apply", SQL: "CREATE TABLE read_apply(id INTEGER)"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := server.core.Propose(context.Background(), value); err != nil {
		t.Fatal(err)
	}
	if material.Tip() != 0 {
		t.Fatalf("fixture material tip=%d, want unapplied decision", material.Tip())
	}
	if _, err := server.Query(context.Background(), QueryRequest{SQL: "SELECT 1", Consistency: "linearizable"}); err != nil {
		t.Fatal(err)
	}
	if material.Tip() != 1 {
		t.Fatalf("linearizable read did not apply pending decision: tip=%d", material.Tip())
	}
	server.localCap <- struct{}{}
	defer func() { <-server.localCap }()
	if _, err := server.Query(context.Background(), QueryRequest{SQL: "SELECT 1", Consistency: "linearizable"}); err != nil {
		t.Fatalf("already-applied linearizable read required lifecycle permit: %v", err)
	}
}

func TestExplicitLocalReadApplySurvivesCallerCancellation(t *testing.T) {
	server, material, _ := newLocalLifecycleServer(t)
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "read-cancel", SQL: "CREATE TABLE read_cancel(id INTEGER)"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := server.core.Propose(context.Background(), value); err != nil {
		t.Fatal(err)
	}
	server.applyMu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := server.Query(ctx, QueryRequest{SQL: "SELECT 1", Consistency: "linearizable"})
		result <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for len(server.localCap) == 0 {
		if time.Now().After(deadline) {
			server.applyMu.Unlock()
			cancel()
			t.Fatal("implicit apply did not acquire lifecycle ownership")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		server.applyMu.Unlock()
		t.Fatalf("query error=%v, want caller cancellation", err)
	}
	if len(server.localCap) != 1 || !server.Ready() {
		server.applyMu.Unlock()
		t.Fatal("caller cancellation released ownership or poisoned readiness before apply finished")
	}
	server.applyMu.Unlock()
	deadline = time.Now().Add(5 * time.Second)
	for material.Tip() != 1 || len(server.localCap) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("apply did not finish: material tip=%d permit count=%d", material.Tip(), len(server.localCap))
		}
		time.Sleep(time.Millisecond)
	}
	if !server.Ready() {
		t.Fatal("successful apply after caller cancellation poisoned readiness")
	}
}

func TestExplicitLocalReadApplyFailureRemainsSticky(t *testing.T) {
	server, material, _ := newLocalLifecycleServer(t)
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "read-fail", SQL: "CREATE TABLE read_fail(id INTEGER)"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := server.core.Propose(context.Background(), value); err != nil {
		t.Fatal(err)
	}
	if err := material.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Query(context.Background(), QueryRequest{SQL: "SELECT 1", Consistency: "linearizable"}); err == nil {
		t.Fatal("implicit apply unexpectedly succeeded on closed materializer")
	}
	if server.Ready() || server.localFailure() == nil {
		t.Fatalf("failed actual apply did not remain sticky: ready=%t failure=%v", server.Ready(), server.localFailure())
	}
}

func TestExplicitLocalCachedReceiptsHoldLifecycleForEveryMutationAPI(t *testing.T) {
	tests := []struct {
		name string
		call func(*Server, context.Context) (uint64, error)
	}{
		{name: "sql", call: func(s *Server, ctx context.Context) (uint64, error) {
			r, err := s.Execute(ctx, ExecuteRequest{RequestID: "cached-sql", SQL: "CREATE TABLE cached_sql(id INTEGER)"})
			return r.Slot, err
		}},
		{name: "graph", call: func(s *Server, ctx context.Context) (uint64, error) {
			r, err := s.GraphExecute(ctx, types.GraphCommand{RequestID: "cached-graph", Cypher: "CREATE (:CachedGraph)"})
			return r.Slot, err
		}},
		{name: "kv", call: func(s *Server, ctx context.Context) (uint64, error) {
			r, err := s.KVPut(ctx, KVMutationRequest{RequestID: "cached-kv", Key: "cache", Value: []byte("value")})
			return r.Slot, err
		}},
		{name: "notify", call: func(s *Server, ctx context.Context) (uint64, error) {
			r, err := s.NotifyPublish(ctx, types.NotifyCommand{RequestID: "cached-notify", Topic: "cache", Payload: []byte("value")})
			return r.Slot, err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, _, _ := newLocalLifecycleServer(t)
			firstSlot, err := test.call(server, context.Background())
			if err != nil {
				t.Fatal(err)
			}
			entered, release := make(chan struct{}), make(chan struct{})
			server.SetDurabilityBarrier(func(ctx context.Context, _ quepaxa.Slot) error {
				close(entered)
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
			done := make(chan error, 1)
			go func() {
				slot, err := test.call(server, context.Background())
				if err == nil && slot != firstSlot {
					err = errors.New("cached receipt changed slot")
				}
				done <- err
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("cached receipt did not enter durability callback")
			}
			if len(server.localCap) != 1 {
				t.Fatalf("Local lifecycle permit count=%d, want 1", len(server.localCap))
			}
			blocked, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			if _, err := server.Quiesce(blocked); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Quiesce error=%v, want deadline while cached receipt is pending", err)
			}
			if server.Ready() == false {
				t.Fatal("pending cached callback changed readiness")
			}
			close(release)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if !server.Ready() {
				t.Fatal("successful cached callback poisoned readiness")
			}
		})
	}
}

func TestExplicitLocalCachedReceiptCallerCancellationDoesNotCancelLifecycle(t *testing.T) {
	server, _, _ := newLocalLifecycleServer(t)
	req := ExecuteRequest{RequestID: "cached-cancel", SQL: "CREATE TABLE cached_cancel(id INTEGER)"}
	if _, err := server.Execute(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	bounded := make(chan bool, 1)
	server.SetDurabilityBarrier(func(ctx context.Context, _ quepaxa.Slot) error {
		close(entered)
		_, hasDeadline := ctx.Deadline()
		bounded <- hasDeadline
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { _, err := server.Execute(ctx, req); result <- err }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("cached receipt did not enter durability callback")
	}
	if !<-bounded {
		t.Fatal("cached durability callback did not receive a bounded server-owned context")
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) || !errors.Is(err, ErrCommitUnknown) {
		t.Fatalf("caller error=%v, want canceled commit-unknown", err)
	}
	if len(server.localCap) != 1 || !server.Ready() {
		t.Fatal("caller cancellation released lifecycle ownership or poisoned readiness before callback ended")
	}
	close(release)
	deadline := time.After(5 * time.Second)
	for len(server.localCap) != 0 {
		select {
		case <-deadline:
			t.Fatal("lifecycle ownership remained after durability callback completed")
		case <-time.After(time.Millisecond):
		}
	}
	if !server.Ready() {
		t.Fatal("successful callback after caller cancellation poisoned readiness")
	}
}
