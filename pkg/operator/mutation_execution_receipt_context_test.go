package operator

import (
	"context"
	"errors"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

func TestRemoteMutationExecutionReceiptGetContextHonorsCancellation(t *testing.T) {
	objects := newMemoryEvidenceObjects()
	store, err := NewRemoteMutationExecutionReceiptStore(testAuthorityEvidence(t, objects, "receipt-context-root"))
	if err != nil {
		t.Fatal(err)
	}
	receipt := mutationExecutionReceipt{WALIdentity: "wal", ProducerUID: "producer", Sequence: 1,
		DispatchID: "dispatch", Outcome: fencing.MutationCommitted}
	receipt.ReceiptDigest = receipt.digest()
	name := mutationExecutionReceiptName(receipt)
	if _, err := store.put(receipt); err != nil {
		t.Fatal(err)
	}
	if got, err := store.getContext(context.Background(), name); err != nil || got != receipt {
		t.Fatalf("context read did not return exact validated receipt: got=%+v err=%v", got, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.getContext(ctx, name); !errors.Is(err, context.Canceled) {
		t.Fatalf("receipt read ignored canceled context: %v", err)
	}
}
