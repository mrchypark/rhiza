package managedsql

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
)

// This is a pure predicate decision table, not evidence that a live certified
// gap was forced. Integration evidence comes from the prepared-learner test.
func TestManagedKVHostResolvedHistoryGuard(t *testing.T) {
	for _, test := range []struct {
		name      string
		kind      string
		receipt   types.MutationReceipt
		certified uint64
		applied   uint64
		recorder  uint64
		wantError bool
	}{
		{"slot-zero", "sql", types.MutationReceipt{Status: types.MutationCommitted}, 3, 3, 3, true},
		{"outside-certified", "sql", types.MutationReceipt{Slot: 4, Status: types.MutationCommitted}, 3, 3, 3, true},
		{"nonterminal", "sql", types.MutationReceipt{Slot: 2}, 3, 3, 3, true},
		{"kv-rejected", "kv", types.MutationReceipt{Slot: 2, Status: types.MutationRejected}, 3, 3, 3, true},
		{"applied-gap", "sql", types.MutationReceipt{Slot: 2, Status: types.MutationCommitted}, 36, 33, 36, true},
		{"recorder-gap", "sql", types.MutationReceipt{Slot: 2, Status: types.MutationCommitted}, 3, 3, 4, true},
		{"sql-rejected", "sql", types.MutationReceipt{Slot: 2, Status: types.MutationRejected}, 3, 3, 3, false},
		{"kv-cas-false", "kv", types.MutationReceipt{Slot: 2, Status: types.MutationCommitted, Applied: false}, 3, 3, 3, false},
		{"sql-committed", "sql", types.MutationReceipt{Slot: 2, Status: types.MutationCommitted}, 3, 3, 3, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := RecoveryResult{Kind: test.kind}
			if test.kind == "sql" {
				result.SQL = &ExecuteResponse{MutationReceipt: test.receipt}
			} else {
				result.KV = &network.KVMutationResponse{MutationReceipt: test.receipt}
			}
			state := node.ManagedRuntimeState{CertifiedTip: test.certified, AppliedTip: test.applied, RecorderTip: test.recorder}
			if err := validateResolvedHistory(applicationOriginal{kind: test.kind}, result, state); (err != nil) != test.wantError {
				t.Fatalf("guard error=%v, want error=%v", err, test.wantError)
			}
		})
	}
}

func TestManagedKVHostPendingAbsentReasonIsNarrow(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want bool
	}{
		{"exact", errPendingAbsent, true},
		{"wrapped-unknown", &network.CommitUnknownError{Cause: errors.Join(errPendingAbsent)}, true},
		{"barrier", errors.Join(errPendingAbsent, context.DeadlineExceeded), false},
		{"poison", errors.Join(errPendingAbsent, errors.New("materializer poisoned")), false},
		{"mismatch", errors.New("pending original differs"), false},
		{"empty", nil, false},
		{"unknown-without-reason", &network.CommitUnknownError{}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := onlyPendingAbsent(test.err); got != test.want {
				t.Fatalf("onlyPendingAbsent(%v)=%v, want %v", test.err, got, test.want)
			}
		})
	}
}

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
	left, err := (applicationOriginal{kind: "kv", kv: a}).identity()
	if err != nil {
		t.Fatal(err)
	}
	right, err := (applicationOriginal{kind: "kv", kv: b}).identity()
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
