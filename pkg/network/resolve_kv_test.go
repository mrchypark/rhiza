package network

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestResolveKVOriginalResults(t *testing.T) {
	s, m, core := managedKVRealServer(t, 8)
	ctx := context.Background()
	for i, operation := range []string{"put", "delete", "cas", "cas", "delete"} {
		req := KVMutationRequest{RequestID: fmt.Sprint("raw-", i, string([]byte{255})), Key: fmt.Sprint("key-", i, string([]byte{254})), Value: []byte{0, 255}, TTLMS: 1}
		if i == 3 {
			req.ExpectedExists = true
		} // committed CAS false
		if operation == "delete" && i == 1 {
			if _, err := s.KVPut(ctx, KVMutationRequest{RequestID: "seed", Key: req.Key, Value: []byte("old")}); err != nil {
				t.Fatal(err)
			}
		}
		response, err := s.KVMutate(ctx, operation, req)
		if err != nil {
			t.Fatal(err)
		}
		before := core.Tip()
		result, err := s.ResolveKV(ctx, operation, req)
		if err != nil || result.State != ResolveKVRetained || result.Receipt == nil || *result.Receipt != response.MutationReceipt || result.Durability != ResolveKVDurabilityNotConfigured {
			t.Fatalf("%s result=%+v error=%v", operation, result, err)
		}
		if (i == 3 || i == 4) && result.Receipt.Applied {
			t.Fatal("false result lost")
		}
		if operation == "put" {
			decisions, _, err := core.DecisionsFrom(quepaxa.Slot(response.Slot), 1)
			if err != nil {
				t.Fatal(err)
			}
			commands, _, err := types.DecodeManagedKVBatch(decisions[0].Value)
			if err != nil {
				t.Fatal(err)
			}
			if _, found, err := m.KVGet(ctx, req.Key, time.UnixMilli(commands[0].ExpiresAtUnixMS)); err != nil || found {
				t.Fatal("controlled TTL expiry failed", err)
			}
			again, err := s.ResolveKV(ctx, operation, req)
			if err != nil || *again.Receipt != *result.Receipt {
				t.Fatal("TTL changed original resolution", err)
			}
			after, _, err := core.DecisionsFrom(quepaxa.Slot(response.Slot), 1)
			if err != nil || !bytes.Equal(after[0].Value, decisions[0].Value) {
				t.Fatal("resolution refreshed timestamp", err)
			}
		}
		if core.Tip() != before {
			t.Fatal("resolution proposed a mutation")
		}
	}
}

func TestResolveKVExactIntentAndKind(t *testing.T) {
	s, _, core := managedKVRealServer(t, 8)
	ctx := context.Background()
	request := KVMutationRequest{RequestID: "shared", Key: string([]byte{255}), Value: []byte("v"), Expected: []byte("ignored"), ExpectedExists: true, TTLMS: 10}
	kv, err := s.KVPut(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	sqlRequest := ExecuteRequest{RequestID: "shared", SQL: "CREATE TABLE resolver_kind(v)"}
	sql, err := s.Execute(ctx, sqlRequest)
	if err != nil {
		t.Fatal(err)
	}
	before := core.Tip()
	for _, change := range []func(*KVMutationRequest){func(r *KVMutationRequest) { r.Key = string([]byte{254}) }, func(r *KVMutationRequest) { r.Value = []byte("changed") }, func(r *KVMutationRequest) { r.Expected = []byte("changed ignored") }, func(r *KVMutationRequest) { r.ExpectedExists = false }, func(r *KVMutationRequest) { r.TTLMS++ }} {
		changed := request
		change(&changed)
		if _, err := s.ResolveKV(ctx, "put", changed); !errors.Is(err, ErrRequestConflict) {
			t.Fatal("changed intent accepted", err)
		}
	}
	if _, err := s.ResolveKV(ctx, "delete", request); !errors.Is(err, ErrRequestConflict) {
		t.Fatal("changed operation accepted", err)
	}
	result, err := s.ResolveKV(ctx, "put", request)
	if err != nil || result.Receipt.Slot != kv.Slot {
		t.Fatal("KV kind changed", err)
	}
	SQLResult, err := s.ResolveExecute(ctx, sqlRequest)
	if err != nil || SQLResult.Receipt.Slot != sql.Slot || sql.Slot == kv.Slot {
		t.Fatal("SQL/KV kind conflated", err)
	}
	request.RequestID = "missing"
	result, err = s.ResolveKV(ctx, "put", request)
	if err != nil || result.State != ResolveKVUnknownOrExpired || result.Receipt != nil || result.Durability != ResolveKVDurabilityUnknown {
		t.Fatal("missing not unknown", result, err)
	}
	if core.Tip() != before {
		t.Fatal("resolution changed history")
	}
}

func TestResolveKVBarrierAndNoWriteGates(t *testing.T) {
	s, _, core := managedKVRealServer(t, 8)
	req := KVMutationRequest{RequestID: string([]byte{255}), Key: "key", Value: []byte{}}
	expected, err := s.KVPut(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	before := core.Tip()
	unlock, err := s.lockRequest(context.Background(), req.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	// Receipt return can precede worker lease release. Drain the completed
	// writer before configuring a deliberately saturated test admission state.
	s.kvBatcher.Close()
	s.writable = false
	s.ready = func() bool { return false }
	s.mutationMu.Lock()
	s.mutationAdmission = mutationAdmission{count: maxAdmittedMutations, bytes: maxAdmittedBytes}
	s.mutationMu.Unlock()
	var slot quepaxa.Slot
	s.SetDurabilityBarrier(func(_ context.Context, original quepaxa.Slot) error { slot = original; return nil })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := s.ResolveKV(ctx, "put", req)
	if err != nil || result.Durability != ResolveKVDurabilityConfirmed || uint64(slot) != expected.Slot || *result.Receipt != expected.MutationReceipt {
		t.Fatal("resolver waited for write gates", result, err)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	s.SetDurabilityBarrier(func(_ context.Context, original quepaxa.Slot) error { slot = original; cancel2(); return ctx2.Err() })
	result, err = s.ResolveKV(ctx2, "put", req)
	var unknown *CommitUnknownError
	if !errors.Is(err, ErrCommitUnknown) || !errors.Is(err, ErrDurabilityUnavailable) || !errors.As(err, &unknown) || unknown.RequestID != req.RequestID || unknown.Slot != quepaxa.Slot(expected.Slot) || result.State != ResolveKVRetained || result.Durability != ResolveKVDurabilityUnknown || *result.Receipt != expected.MutationReceipt || slot != quepaxa.Slot(expected.Slot) {
		t.Fatal("barrier failure lost original receipt/identity", result, err)
	}
	if core.Tip() != before {
		t.Fatal("barrier resolution submitted application")
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if _, err := s.ResolveKV(canceled, "put", req); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled lookup accepted", err)
	}
	s.proposeMu.Lock()
	s.lifecycleFailure = errors.New("terminal")
	s.proposeMu.Unlock()
	if _, err := s.ResolveKV(context.Background(), "put", req); !errors.Is(err, ErrNotReady) {
		t.Fatal("terminal accepted", err)
	}
}

func TestResolveKVExpiredUnknown(t *testing.T) {
	s, m, core := managedKVRealServer(t, 8)
	req := KVMutationRequest{RequestID: "expire", Key: "key", Value: []byte("v")}
	response, err := s.KVPut(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	barrier := types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{})
	for core.Tip() <= quepaxa.Slot(response.RetryThroughSlot) {
		if _, _, err := core.Propose(context.Background(), barrier); err != nil {
			t.Fatal(err)
		}
	}
	var decisions []quepaxa.DecidedValue
	for slot := quepaxa.Slot(m.Tip() + 1); slot <= core.Tip(); slot++ {
		decision, ok := core.CertifiedValue(slot)
		if !ok {
			t.Fatal("missing actual control decision")
		}
		decisions = append(decisions, decision)
	}
	if err := m.ApplyBatch(context.Background(), decisions); err != nil {
		t.Fatal(err)
	}
	before := core.Tip()
	result, err := s.ResolveKV(context.Background(), "put", req)
	if err != nil || result.State != ResolveKVUnknownOrExpired || result.Receipt != nil || result.Durability != ResolveKVDurabilityUnknown || core.Tip() != before {
		t.Fatal("expired resolved as retained/effect proof", result, err)
	}
}

func TestResolveKVProfileAndValidation(t *testing.T) {
	legacy, _, _ := managedKVRealServer(t, 7)
	if _, err := legacy.ResolveKV(context.Background(), "put", KVMutationRequest{}); !errors.Is(err, sqlpolicy.ErrIncompatible) {
		t.Fatal("legacy profile accepted", err)
	}
	empty, err := NewServerForPolicy(nil, nil, "cluster", false, nil, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer empty.Close()
	if _, err := empty.ResolveKV(context.Background(), "put", KVMutationRequest{}); !errors.Is(err, ErrNotReady) {
		t.Fatal("nil material accepted", err)
	}
	s, _, core := managedKVRealServer(t, 8)
	for _, req := range []KVMutationRequest{{}, {RequestID: "i", Key: "k", TTLMS: -1}, {RequestID: "i", Key: "k", Expected: make([]byte, 1<<20)}} {
		if _, err := s.ResolveKV(context.Background(), "put", req); err == nil {
			t.Fatal("invalid original accepted")
		}
	}
	if _, err := s.ResolveKV(context.Background(), "get", KVMutationRequest{RequestID: "i", Key: "k"}); err == nil {
		t.Fatal("invalid operation accepted")
	}
	if core.Tip() != 0 {
		t.Fatal("invalid resolution proposed")
	}
}
