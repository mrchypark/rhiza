package network

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestKVUnknownDiagnosticDoesNotExposeCause(t *testing.T) {
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previous) })
	unknown := commitUnknown(156, "private-id", errors.Join(context.DeadlineExceeded, errors.New("private-storage-path")))
	logKVUnknown("receipt_lookup", 156, unknown)
	if got := output.String(); !strings.Contains(got, "MANAGED_KV_UNKNOWN stage=receipt_lookup slot=156 cause=deadline") || strings.Contains(got, "private") {
		t.Fatalf("unexpected diagnostic: %q", got)
	}
	output.Reset()
	logKVUnknown("batch", 0, ErrNotReady)
	if output.Len() != 0 {
		t.Fatal("non-unknown result logged as unknown")
	}
	for _, stage := range []string{"propose", "apply"} {
		output.Reset()
		original := errors.New("private-storage-path")
		b := &mutationBatcher[types.KVCommand]{ctx: context.Background()}
		b.propose = func(context.Context, []byte) (quepaxa.Slot, error) {
			if stage == "propose" {
				return 156, original
			}
			return 156, nil
		}
		b.apply = func(context.Context, quepaxa.Slot) error { return original }
		item := &batchItem{requestID: "private-id", result: make(chan batchResult, 1)}
		b.execute([]*batchItem{item}, []byte("value"))
		var gotUnknown *CommitUnknownError
		if !errors.As((<-item.result).err, &gotUnknown) || gotUnknown.Cause != original {
			t.Fatal("unknown error changed direct Cause identity")
		}
		if got := output.String(); !strings.Contains(got, "stage=batch_"+stage+" slot=156 cause=other") || strings.Contains(got, "private") {
			t.Fatalf("invalid stage: %q", got)
		}
	}
}

func TestBatcherPreservesZeroSlotErrors(t *testing.T) {
	for _, stage := range []string{"propose", "apply"} {
		t.Run(stage, func(t *testing.T) {
			original := errors.New("original error")
			b := &mutationBatcher[struct{}]{ctx: context.Background()}
			b.propose = func(context.Context, []byte) (quepaxa.Slot, error) {
				if stage == "propose" {
					return 0, original
				}
				return 0, nil
			}
			b.apply = func(context.Context, quepaxa.Slot) error { return original }
			item := &batchItem{result: make(chan batchResult, 1)}
			b.execute([]*batchItem{item}, []byte("value"))
			if got := (<-item.result).err; got != original {
				t.Fatalf("zero-slot error identity changed: %v", got)
			}
		})
	}
}

func TestBatcherPreservesUnknownCauseAtZeroSlot(t *testing.T) {
	original := &CommitUnknownError{Cause: context.Canceled}
	b := &mutationBatcher[struct{}]{ctx: context.Background(), propose: func(context.Context, []byte) (quepaxa.Slot, error) { return 0, original }}
	item := &batchItem{result: make(chan batchResult, 1)}
	b.execute([]*batchItem{item}, []byte("value"))
	var got *CommitUnknownError
	if !errors.As((<-item.result).err, &got) || got.Cause != original {
		t.Fatal("zero-slot unknown changed direct Cause identity")
	}
}
