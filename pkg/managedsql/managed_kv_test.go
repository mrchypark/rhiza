package managedsql

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
)

// This is a pure predicate decision table, not evidence that a live certified
// gap was forced. Integration evidence comes from the prepared-learner test.

func TestManagedKVHostOutcomeContextOwnsOriginal(t *testing.T) {
	for _, kind := range []string{"sql", "kv"} {
		t.Run(kind, func(t *testing.T) {
			id := string([]byte{0xff, 0xfe, 0, 0xef, 0xbf, 0xbd})
			original := applicationOriginal{kind: kind, sql: ExecuteRequest{RequestID: id}, kv: types.KVCommand{RequestID: id, Operation: "put", Key: "key"}}
			cause := &network.CommitUnknownError{Slot: 17, RequestID: id, RetryThroughSlot: 1040, Cause: context.Canceled}
			err := applicationOutcomeUnknown(original, types.MutationReceipt{}, cause)
			if !errors.Is(err, network.ErrCommitUnknown) || !errors.Is(err, context.Canceled) || errors.Is(err, ErrKVNotSubmitted) {
				t.Fatalf("lost submitted error chain: %v", err)
			}
			var unknown *network.CommitUnknownError
			if !errors.As(err, &unknown) || unknown.RequestID != id || unknown.Slot != 17 || unknown.RetryThroughSlot != 1040 {
				t.Fatalf("lost original outcome: %+v", unknown)
			}
			metadata, ok := MutationErrorContext(err)
			if !ok || metadata.Kind != kind || !bytes.Equal(metadata.RequestID, []byte(id)) || metadata.Slot != 17 || metadata.RetryThroughSlot != 1040 {
				t.Fatalf("context=%+v", metadata)
			}
			metadata.RequestID[0] = 0
			again, _ := MutationErrorContext(err)
			if !bytes.Equal(again.RequestID, []byte(id)) {
				t.Fatal("caller changed captured original")
			}
		})
	}
	if _, ok := MutationErrorContext(context.Canceled); ok {
		t.Fatal("invented context for unrelated error")
	}
}

func TestManagedKVHostUnknownContextDoesNotInventSlot(t *testing.T) {
	p := applicationOriginal{kind: "kv", kv: types.KVCommand{RequestID: "original"}}
	err := applicationOutcomeUnknown(p, types.MutationReceipt{}, context.DeadlineExceeded)
	metadata, ok := MutationErrorContext(err)
	if !ok || metadata.Slot != 0 || metadata.RetryThroughSlot != 0 || !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, network.ErrCommitUnknown) {
		t.Fatalf("unknown metadata=%+v err=%v", metadata, err)
	}
	before := &KVNotSubmittedError{Stage: SQLStageReservation, RecoveryRequired: true, Cause: context.Canceled}
	if !errors.Is(before, ErrKVNotSubmitted) || !errors.Is(before, context.Canceled) || errors.Is(before, network.ErrCommitUnknown) || errors.Is(before, ErrSQLNotSubmitted) {
		t.Fatalf("misclassified pre-submit: %v", before)
	}
}

func TestManagedKVHostFingerprintIsNotOriginalEquality(t *testing.T) {
	a := types.KVCommand{RequestID: string([]byte{0xff}), Operation: "put", Key: string([]byte{0xfe}), Value: nil, Expected: nil}
	b := a
	b.Value = []byte{}
	b.Expected = []byte{}
	left, err := originalFingerprint(applicationOriginal{kind: "kv", kv: a})
	if err != nil {
		t.Fatal(err)
	}
	right, err := originalFingerprint(applicationOriginal{kind: "kv", kv: b})
	if err != nil || left != right {
		t.Fatalf("fixture no longer covers equivalent fingerprint: %v", err)
	}
	rawA, _, err := types.EncodeManagedKVOriginal(a)
	if err != nil {
		t.Fatal(err)
	}
	rawB, _, err := types.EncodeManagedKVOriginal(b)
	if err != nil || bytes.Equal(rawA, rawB) {
		t.Fatal("canonical originals merged nullable fields", err)
	}
}
