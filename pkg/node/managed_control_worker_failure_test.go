package node

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// This isolates classification of an explicitly completed failed operation.
// It does not claim to reproduce a worker's actual RPC/deadline lifecycle.
func TestManagedControlFailedWorkerDoesNotCancelLiveWaiter(t *testing.T) {
	for _, workerError := range []struct {
		name string
		err  error
	}{{"deadline", context.DeadlineExceeded}, {"canceled", context.Canceled}} {
		t.Run(workerError.name, func(t *testing.T) {
			fixture := newManagedGraphAdmissionRaceFixture(t)
			n := fixture.node
			ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			value, err := types.EncodeManagedKVCommand(types.KVCommand{RequestID: "known-before-worker-failure", Operation: "put", Key: "known", Value: []byte("retained")})
			if err != nil {
				t.Fatal(err)
			}
			slot, _, err := n.core.Propose(ctx, value)
			if err != nil {
				t.Fatal("certify actual prior application", err)
			}
			decision, exists := n.core.CertifiedValue(slot)
			if !exists {
				t.Fatal("actual prior certificate missing")
			}
			if err := n.core.EnsureDurable(slot); err != nil {
				t.Fatal(err)
			}
			if err := n.material.ApplyBatch(ctx, []quepaxa.DecidedValue{decision}); err != nil {
				t.Fatal(err)
			}
			receipt, exists, err := n.material.MutationReceipt(ctx, types.MutationKV, "known-before-worker-failure")
			if err != nil || !exists || receipt.Slot != uint64(slot) || !receipt.Applied {
				t.Fatalf("actual prior receipt missing: %+v %v", receipt, err)
			}
			before, tip, err := n.core.DecisionsFrom(1, 256)
			if err != nil {
				t.Fatal(err)
			}
			prefix, prefixExists := n.core.PrefixHash(tip)
			if !prefixExists {
				t.Fatal("actual prior prefix missing")
			}
			applied, recorded := n.material.Tip(), n.core.RecorderTip()
			done := make(chan struct{})
			close(done)
			op := &managedGraphControlOperation{binding: n.internalApplication.Load(), done: done, err: workerError.err}
			proof, err := n.waitManagedAppendOnlyBarrierProof(ctx, tip+1, op, done)
			if ctx.Err() != nil || proof != 0 || !errors.Is(err, network.ErrNotReady) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("failed worker was attributed to the live waiter: proof=%d err=%v", proof, err)
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			proof, err = n.waitManagedAppendOnlyBarrierProof(canceled, tip+1, op, done)
			if proof != 0 || !errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, network.ErrNotReady) {
				t.Fatalf("waiter's own cancellation lost priority: proof=%d err=%v", proof, err)
			}
			after, afterTip, err := n.core.DecisionsFrom(1, 256)
			afterPrefix, ok := n.core.PrefixHash(tip)
			if err != nil || afterTip != tip || !reflect.DeepEqual(before, after) || !ok || afterPrefix != prefix || n.material.Tip() != applied || n.core.RecorderTip() != recorded {
				t.Fatal("failed wait changed actual certified/applied/recorder history")
			}
			afterReceipt, exists, err := n.material.MutationReceipt(ctx, types.MutationKV, "known-before-worker-failure")
			if err != nil || !exists || !reflect.DeepEqual(receipt, afterReceipt) {
				t.Fatal("failed wait changed prior actual receipt")
			}
			if _, exists, err := n.material.MutationReceipt(ctx, types.MutationKV, "never-submitted"); err != nil || exists {
				t.Fatal("failed wait fabricated a new receipt")
			}
		})
	}
}
