//go:build cgo

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/managedsql"
	"github.com/mrchypark/rhiza/pkg/network"
)

func TestManagedKVStrictOriginalWire(t *testing.T) {
	for _, operation := range []string{"put", "delete", "cas"} {
		for _, id := range []string{string([]byte{0xff}), string([]byte{0xfe}), "\ufffd"} {
			original := types.KVCommand{RequestID: id, Operation: operation, Key: string([]byte{0xfe}), Value: []byte{}, Expected: nil, ExpectedExists: false, TTLMS: 123}
			raw, _, err := types.EncodeManagedKVOriginal(original)
			if err != nil {
				t.Fatal(err)
			}
			gotOperation, got, err := decodeManagedKVMutation("kv_"+operation, raw)
			if err != nil || gotOperation != operation || got.RequestID != id || got.Key != original.Key || got.Value == nil || len(got.Value) != 0 || got.Expected != nil || got.ExpectedExists || got.TTLMS != 123 {
				t.Fatalf("lost original: %#v %v", got, err)
			}
			if _, _, err := decodeManagedKVMutation("kv_other", raw); err == nil {
				t.Fatal("outer operation mismatch accepted")
			}
		}
	}
	valid := `{"request_id_b64":"/w==","operation":"put","key_b64":"/g==","value":"","expected":null,"expected_exists":false,"ttl_ms":0}`
	for _, raw := range []string{
		strings.Replace(valid, `"ttl_ms":0`, `"ttl_ms":0,"ttl_ms":1`, 1),
		strings.Replace(valid, `,"ttl_ms":0`, "", 1),
		strings.Replace(valid, `"expected_exists":false`, `"expected_exists":null`, 1),
		strings.Replace(valid, `"ttl_ms":0`, `"ttl_ms":null`, 1),
		strings.Replace(valid, `"/w=="`, `"/w"`, 1),
		strings.Replace(valid, `"/w=="`, `"/x=="`, 1),
		strings.Replace(valid, `"value":""`, `"value":[]`, 1),
		strings.Replace(valid, `"ttl_ms":0`, `"ttl_ms":0,"unknown":1`, 1),
		valid + ` {}`,
	} {
		if _, _, err := decodeManagedKVMutation("kv_put", json.RawMessage(raw)); err == nil {
			t.Fatal("invalid original accepted", raw)
		}
	}
}

func TestManagedKVByteReadValidation(t *testing.T) {
	for _, input := range []string{"/w==", "/g==", "77+9"} {
		got, err := managedRawBytes(&input, 64)
		if err != nil || len(got) == 0 {
			t.Fatal(input, err)
		}
	}
	for _, input := range []string{"", "/w", "/x==", "/w==\n", strings.Repeat("A", 100)} {
		if _, err := managedRawBytes(&input, 64); err == nil {
			t.Fatal("invalid canonical identity accepted", input)
		}
	}
	if _, err := managedRawBytes(nil, 64); err == nil {
		t.Fatal("missing identity accepted")
	}
}

func TestManagedKVReadFoundIsAuthoritative(t *testing.T) {
	for _, stored := range [][]byte{nil, {}, []byte("value")} {
		wire := managedKVGetWire(network.KVGetResponse{Found: true, Value: stored, AppliedSlot: 7, ConsensusTip: 8})
		if !wire.Found || wire.Value == nil || !bytes.Equal(wire.Value, stored) || wire.AppliedSlot != 7 || wire.ConsensusTip != 8 {
			t.Fatalf("present value lost: %#v", wire)
		}
		raw, _ := json.Marshal(wire)
		if bytes.Contains(raw, []byte(`"value":null`)) {
			t.Fatal(string(raw))
		}
	}
	wire := managedKVGetWire(network.KVGetResponse{Found: false, Value: []byte("not-authoritative")})
	if wire.Found || wire.Value != nil {
		t.Fatal("absence inferred from bytes", wire)
	}
}

func TestManagedKVExplicitFalseWire(t *testing.T) {
	receipt := types.MutationReceipt{Slot: 17, Status: types.MutationCommitted, Applied: false, RetryThroughSlot: 900}
	mutation := managedKVReceiptWire(receipt)
	status := managedStatusWire("kv", network.RequestStatusResponse{State: "committed", Tip: 18, Receipt: &receipt})
	recovery, err := managedRecoveryWire(managedsql.RecoveryResult{Kind: "kv", RequestID: []byte{0xff}, KV: &network.KVMutationResponse{MutationReceipt: receipt}})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []any{mutation, status, recovery} {
		raw, err := json.Marshal(value)
		if err != nil || !bytes.Contains(raw, []byte(`"applied":false`)) {
			t.Fatalf("false receipt omitted: %s %v", raw, err)
		}
	}
	// Existing SQL/global receipt wire must not silently acquire the new field.
	sqlRaw, _ := json.Marshal(managedStatusWire("sql", network.RequestStatusResponse{Receipt: &receipt}))
	if bytes.Contains(sqlRaw, []byte(`"applied"`)) {
		t.Fatal("legacy SQL wire changed", string(sqlRaw))
	}
	none, err := managedRecoveryWire(managedsql.RecoveryResult{})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(none)
	if string(raw) != `{"kind":"none"}` {
		t.Fatal(string(raw))
	}
	if _, err := managedRecoveryWire(managedsql.RecoveryResult{Kind: "kv", RequestID: []byte{1}}); err == nil {
		t.Fatal("missing exact result accepted")
	}
}

func TestManagedKVErrorSerializationPreservesContext(t *testing.T) {
	value := &ffiError{Code: "commit_unknown", Message: "managed operation rejected", MutationContext: &ffiMutationContext{Kind: "kv", RequestID: []byte{0xff}, Slot: 0, RetryThroughSlot: 900}}
	for _, raw := range [][]byte{marshalFFIError(value), errorJSON(value)} {
		var envelope struct {
			Error struct {
				Code    string             `json:"code"`
				Context ffiMutationContext `json:"mutation_context"`
			} `json:"error"`
		}
		if err := json.Unmarshal(raw, &envelope); err != nil {
			t.Fatal(err)
		}
		if envelope.Error.Code != "commit_unknown" || envelope.Error.Context.Kind != "kv" || !bytes.Equal(envelope.Error.Context.RequestID, []byte{0xff}) || envelope.Error.Context.Slot != 0 || envelope.Error.Context.RetryThroughSlot != 900 {
			t.Fatal(string(raw))
		}
	}
	// This executes the actual managed redaction builder with a real typed
	// outcome, not an injected Host/receipt ledger. Captured Host context itself
	// requires the real Host integration fixture and remains a separate gate.
	unknown := &network.CommitUnknownError{Slot: 17, RequestID: string([]byte{0xff}), Cause: context.Canceled}
	raw := marshalFFIError(managedError(unknown))
	if !bytes.Contains(raw, []byte(`"code":"commit_unknown"`)) || bytes.Contains(raw, []byte(`canceled`)) || bytes.Contains(raw, []byte(`"slot"`)) {
		t.Fatal(string(raw))
	}
	if code := errorCode(errors.Join(unknown, &managedsql.KVNotSubmittedError{RecoveryRequired: true})); code != "commit_unknown" {
		t.Fatal(code)
	}
	for _, test := range []struct {
		required bool
		want     string
	}{{false, "kv_not_submitted"}, {true, "kv_recovery_required"}} {
		if got := errorCode(&managedsql.KVNotSubmittedError{RecoveryRequired: test.required, Cause: context.DeadlineExceeded}); got != test.want {
			t.Fatal(got, test.want)
		}
	}
}
