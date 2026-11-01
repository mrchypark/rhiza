//go:build cgo

package main

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
)

func TestManagedRustInitialWireRequestIDLimit(t *testing.T) {
	// Exact 2ec Rust example construction with the actual replacement UID;
	// timestamp shape is the example's 19-digit nanosecond i64.
	id := "rust-managed-contract-62d3a54a-4b56-4852-88c4-acf6d020e9ac-1793514461000000000-schema"
	request, _ := json.Marshal(map[string]any{"request_id": id, "sql": "CREATE TABLE IF NOT EXISTS rust_managed_contract (id INTEGER PRIMARY KEY, value TEXT NOT NULL)"})
	var decoded network.ExecuteRequest
	if err := decodeStrict(request, &decoded); err != nil {
		t.Fatalf("Rust JSON was rejected by decodeStrict: %v", err)
	}
	if len(id) != 85 || types.MaxRequestIDBytes != 64 {
		t.Fatalf("repro length/limit changed: %d/%d", len(id), types.MaxRequestIDBytes)
	}
	if err := network.ValidateExecuteRequest(decoded); !errors.Is(err, network.ErrInvalidRequest) {
		t.Fatalf("canonical managed validator outcome: %v", err)
	}
	// Actual FFI call through an owned real embedded DB uses that same validator.
	// This establishes request classification, not a fake managed Host startup.
	handle := openTestDB(t)
	wire, _ := json.Marshal(callEnvelope{Operation: "execute", Request: request})
	if code := responseErrorCode(t, goCall(handle, wire, 1000)); code != "invalid_request" {
		t.Fatalf("actual FFI long-ID outcome: %s", code)
	}
	decoded.RequestID = strings.Repeat("a", types.MaxRequestIDBytes)
	if err := network.ValidateExecuteRequest(decoded); err != nil {
		t.Fatalf("same valid SQL/omitted args with a bounded ID rejected: %v", err)
	}
	bounded, _ := json.Marshal(decoded)
	wire, _ = json.Marshal(callEnvelope{Operation: "execute", Request: bounded})
	result := requireData(t, mustResponse(t, goCall(handle, wire, 30000)))
	if !committedFFIReceipt(result) {
		t.Fatal("bounded request did not return committed receipt through actual FFI")
	}
}
