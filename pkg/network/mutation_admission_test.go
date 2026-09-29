package network

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestMutationChargeRejectsDeepAndOversizedInput(t *testing.T) {
	tooLarge, err := mutationCharge(types.KVCommand{Value: make([]byte, maxMutationCharge)})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("large charge=%d err=%v", tooLarge, err)
	}
	cycle := map[string]any{}
	cycle["self"] = cycle
	if _, err := mutationCharge(types.GraphCommand{Args: map[string]any{"cycle": cycle}}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("cyclic graph input error=%v", err)
	}
}

func TestMutationAdmissionCountAndAggregateByteCaps(t *testing.T) {
	server := NewServer(nil, nil, "cluster", false, nil)
	defer server.Close()
	leases := make([]*mutationLease, maxAdmittedMutations)
	for i := range leases {
		lease, err := server.admitMutation(1)
		if err != nil {
			t.Fatalf("admission %d: %v", i, err)
		}
		leases[i] = lease
	}
	if _, err := server.admitMutation(1); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("count cap error=%v", err)
	}
	for _, lease := range leases {
		lease.releaseCaller()
	}
	if server.mutationAdmission.count != 0 || server.mutationAdmission.bytes != 0 {
		t.Fatalf("admission leaked: %+v", server.mutationAdmission)
	}
	leases = make([]*mutationLease, maxAdmittedBytes/maxMutationCharge)
	for i := range leases {
		lease, err := server.admitMutation(maxMutationCharge)
		if err != nil {
			t.Fatalf("byte admission %d: %v", i, err)
		}
		leases[i] = lease
	}
	if _, err := server.admitMutation(1); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("aggregate byte cap error=%v", err)
	}
	for _, lease := range leases {
		lease.releaseCaller()
	}
	lease, err := server.admitMutation(maxAdmittedBytes + 1)
	if !errors.Is(err, ErrInvalidRequest) || lease != nil {
		t.Fatalf("per-request cap lease=%v err=%v", lease, err)
	}
}

func TestRequestStripeWaitHonorsCancellationAndShutdown(t *testing.T) {
	server := NewServer(nil, nil, "cluster", false, nil)
	defer server.Close()
	id := "same-id"
	hash := sha256.Sum256([]byte(id))
	stripe := uint16(hash[0])<<4 | uint16(hash[1])>>4
	other := ""
	for i := 0; ; i++ {
		candidate := fmt.Sprintf("stripe-%d", i)
		otherHash := sha256.Sum256([]byte(candidate))
		if uint16(otherHash[0])<<4|uint16(otherHash[1])>>4 == stripe {
			other = candidate
			break
		}
	}
	unlock, err := server.lockRequest(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := server.lockRequest(ctx, other); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stripe wait error=%v", err)
	}
	unlock()
	unlock, err = server.lockRequest(context.Background(), "shutdown")
	if err != nil {
		t.Fatal(err)
	}
	server.proposalStop()
	if _, err := server.lockRequest(context.Background(), "shutdown"); !errors.Is(err, ErrNotReady) {
		t.Fatalf("shutdown wait error=%v", err)
	}
	unlock()
}

func TestMutationLeaseLivesUntilQueuedProposalFinishesAfterCancel(t *testing.T) {
	started, finish, released := make(chan struct{}), make(chan struct{}), make(chan struct{})
	b := newSQLBatcher(func(context.Context, []byte) (quepaxa.Slot, error) {
		close(started)
		<-finish
		return 1, nil
	}, nil)
	defer b.Close()
	lease := &mutationLease{release: func() { close(released) }}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := b.submit(ctx, types.SQLCommand{RequestID: "lease", SQL: "SELECT 1"}, lease)
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("proposal did not start")
	}
	cancel()
	if err := <-done; !errors.Is(err, ErrCommitUnknown) {
		t.Fatalf("canceled submit error=%v", err)
	}
	select {
	case <-released:
		t.Fatal("lease released while proposal was still running")
	default:
	}
	close(finish)
	select {
	case <-released:
	case <-time.After(5 * time.Second):
		t.Fatal("proposal completion leaked admission lease")
	}
}

func TestBlockedStripeRetainsOnlyAdmittedMutationInputs(t *testing.T) {
	server := NewServer(nil, nil, "cluster", true, nil)
	defer server.Close()
	const payloadBytes = 160 << 10
	charge, err := mutationCharge(types.KVCommand{RequestID: "blocked-000000", Operation: "put", Key: "k", Value: make([]byte, payloadBytes)})
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, maxAdmittedBytes/charge)
	stripeFor := func(id string) uint16 { h := sha256.Sum256([]byte(id)); return uint16(h[0])<<4 | uint16(h[1])>>4 }
	found := 0
	for i := 0; found < len(ids); i++ {
		id := fmt.Sprintf("blocked-%06d", i)
		if stripeFor(id) == stripeFor("hold") {
			ids[found] = id
			found++
		}
	}
	unlock, err := server.lockRequest(context.Background(), "hold")
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	ctxs, cancels := make([]context.Context, len(ids)), make([]context.CancelFunc, len(ids))
	done := make(chan error, len(ids))
	values := make([][]byte, len(ids))
	for i := range values {
		values[i] = make([]byte, payloadBytes)
	}
	runtime.GC()
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)
	for i, id := range ids {
		ctxs[i], cancels[i] = context.WithCancel(context.Background())
		go func(ctx context.Context, requestID string, payload []byte) {
			_, err := server.KVPut(ctx, KVMutationRequest{RequestID: requestID, Key: "k", Value: payload})
			done <- err
		}(ctxs[i], id, values[i])
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		server.mutationMu.Lock()
		count := server.mutationAdmission.count
		server.mutationMu.Unlock()
		if count == len(ids) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("admitted %d of %d stripe waiters", count, len(ids))
		}
		time.Sleep(time.Millisecond)
	}
	runtime.GC()
	var blocked runtime.MemStats
	runtime.ReadMemStats(&blocked)
	if retained := blocked.HeapAlloc - baseline.HeapAlloc; retained > maxAdmittedBytes {
		t.Fatalf("blocked engine-retained heap=%d exceeds mutation byte budget %d", retained, maxAdmittedBytes)
	}
	_, err = server.KVPut(context.Background(), KVMutationRequest{RequestID: "blocked-overflow", Key: "k", Value: make([]byte, payloadBytes)})
	if !errors.Is(err, ErrOverloaded) {
		t.Fatalf("over-cap mutation error=%v", err)
	}
	for _, cancel := range cancels {
		cancel()
	}
	for range ids {
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled waiter error=%v", err)
		}
	}
	server.mutationMu.Lock()
	remaining := server.mutationAdmission.count
	server.mutationMu.Unlock()
	if remaining != 0 {
		t.Fatalf("admission reservations leaked after cancel: %d", remaining)
	}
}
