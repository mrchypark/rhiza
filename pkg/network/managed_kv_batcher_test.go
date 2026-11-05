package network

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestManagedKVBatcherAssemblyFailureBeforeProposal(t *testing.T) {
	for _, oversize := range []bool{false, true} {
		var proposals atomic.Int32
		original := errors.New("assembly rejected")
		b := newMutationBatcherWithAssembly(func(context.Context, []byte) (quepaxa.Slot, error) { proposals.Add(1); return 1, nil }, nil, func(string) ([]byte, error) { return []byte("x"), nil }, func(items [][]byte) ([]byte, error) {
			if len(items) == 0 {
				t.Error("assembler called to discover envelope")
			}
			if oversize {
				return make([]byte, quepaxa.MaxReplicatedValueBytes+1), nil
			}
			return nil, original
		}, 7, func(s string) string { return s })
		s := &Server{}
		lease, err := s.admitMutation(17)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err = b.submit(ctx, "id", lease)
		cancel()
		b.Close()
		lease.releaseCaller()
		want := original
		if oversize {
			want = ErrInvalidRequest
		}
		var unknown *CommitUnknownError
		if !errors.Is(err, want) || errors.As(err, &unknown) || proposals.Load() != 0 {
			t.Fatalf("error=%v proposals=%d", err, proposals.Load())
		}
		if b.queuedN != 0 || b.queuedByte != 0 || b.inflightB != 0 || len(b.inflight) != 0 || s.mutationAdmission.count != 0 || s.mutationAdmission.bytes != 0 {
			t.Fatal("accounting or lease leak")
		}
	}
}

func TestManagedKVBatcherProposedBytes(t *testing.T) {
	for _, blob := range [][]byte{nil, {}, {255, 0}} {
		command := types.KVCommand{RequestID: string([]byte{255}), Operation: "cas", Key: string([]byte{254}), Value: blob, Expected: bytes.Clone(blob), ExpectedExists: true, TTLMS: 19, ObservedAtUnixMS: 101, ExpiresAtUnixMS: 120}
		before := bytes.Clone(blob)
		var proposed []byte
		b := newManagedKVBatcher(func(_ context.Context, value []byte) (quepaxa.Slot, error) {
			proposed = bytes.Clone(value)
			return 8, nil
		}, nil)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		slot, err := b.submit(ctx, command)
		cancel()
		b.Close()
		if err != nil || slot != 8 {
			t.Fatal(slot, err)
		}
		commands, ok, err := types.DecodeManagedKVBatch(proposed)
		if !ok || err != nil || len(commands) != 1 || !reflect.DeepEqual(commands[0], command) {
			t.Fatalf("actual proposed bytes: %#v %v", commands, err)
		}
		if !bytes.Equal(blob, before) {
			t.Fatal("caller bytes mutated")
		}
		if b.baseSize != 7 {
			t.Fatal("wrong envelope")
		}
	}
	// Existing ordinary constructor remains byte-for-byte legacy.
	c := types.KVCommand{RequestID: "i", Operation: "put", Key: "k"}
	var proposed []byte
	b := newKVBatcher(func(_ context.Context, v []byte) (quepaxa.Slot, error) { proposed = bytes.Clone(v); return 1, nil }, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_, err := b.submit(ctx, c)
	cancel()
	b.Close()
	if err != nil {
		t.Fatal(err)
	}
	item, _ := types.EncodeKVBatchItem(c)
	if !bytes.Equal(proposed, types.AssembleKVBatch([][]byte{item})) {
		t.Fatal("legacy encoding changed")
	}
}

func TestManagedKVBatcherExecutionBoundary(t *testing.T) {
	c := types.KVCommand{RequestID: "i", Operation: "put", Key: "k"}
	item, err := types.EncodeManagedKVBatchItem(c)
	if err != nil {
		t.Fatal(err)
	}
	item = append(item, bytes.Repeat([]byte{' '}, quepaxa.MaxReplicatedValueBytes-7-len(item))...)
	var proposals atomic.Int32
	var actualBytes atomic.Int32
	b := newManagedKVBatcher(func(_ context.Context, value []byte) (quepaxa.Slot, error) {
		proposals.Add(1)
		actualBytes.Store(int32(len(value)))
		if err := types.ValidateManagedKVExecutionPolicy(value); err != nil {
			return 0, err
		}
		return 1, nil
	}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := b.submitEncoded(ctx, c, item); err != nil {
		t.Fatal(err)
	}
	if _, err := b.submitEncoded(ctx, c, append(bytes.Clone(item), ' ')); !errors.Is(err, ErrInvalidRequest) {
		t.Fatal("over-limit item admitted", err)
	}
	b.Close()
	if proposals.Load() != 1 || actualBytes.Load() != 128<<10 {
		t.Fatal("incorrect exact value bound", proposals.Load(), actualBytes.Load())
	}
}

func TestManagedKVBatcherCanceledDispatch(t *testing.T) {
	var proposals atomic.Int32
	b := newManagedKVBatcher(func(context.Context, []byte) (quepaxa.Slot, error) { proposals.Add(1); return 1, nil }, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	item := &batchItem{ctx: ctx, result: make(chan batchResult, 1)}
	b.dispatch([]*batchItem{item}, [][]byte{[]byte("not valid: canceled before assembly")})
	result := <-item.result
	b.Close()
	if !errors.Is(result.err, context.Canceled) || proposals.Load() != 0 {
		t.Fatal(result.err, proposals.Load())
	}
}

func TestManagedKVBatcherCountBoundary(t *testing.T) {
	var proposals atomic.Int32
	b := newManagedKVBatcher(func(_ context.Context, value []byte) (quepaxa.Slot, error) {
		commands, ok, err := types.DecodeManagedKVBatch(value)
		if err != nil || !ok || len(commands) != 64 {
			return 0, ErrInvalidRequest
		}
		proposals.Add(1)
		return 3, nil
	}, nil)
	defer b.Close()
	item, err := types.EncodeManagedKVBatchItem(types.KVCommand{RequestID: "id", Operation: "put", Key: "key"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	makeItems := func(count int) ([]*batchItem, [][]byte) {
		items := make([]*batchItem, count)
		encoded := make([][]byte, count)
		for i := range items {
			items[i] = &batchItem{ctx: ctx, result: make(chan batchResult, 1)}
			encoded[i] = item
		}
		return items, encoded
	}
	items, encoded := makeItems(64)
	b.dispatch(items, encoded)
	for _, item := range items {
		select {
		case result := <-item.result:
			if result.err != nil || result.slot != 3 {
				t.Fatal(result)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	items, encoded = makeItems(65)
	b.dispatch(items, encoded)
	for _, item := range items {
		select {
		case result := <-item.result:
			if result.err == nil {
				t.Fatal("65 accepted")
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if proposals.Load() != 1 {
		t.Fatal("invalid count reached proposal", proposals.Load())
	}
}
