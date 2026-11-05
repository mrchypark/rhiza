//go:build cgo

package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/managedsql"
	"github.com/mrchypark/rhiza/pkg/network"
)

func TestManagedGraphRecoveryWireKeepsFalseAndExclusiveArm(t *testing.T) {
	receipt := types.MutationReceipt{Slot: 17, Status: types.MutationCommitted, Applied: false, RetryThroughSlot: 1040}
	result := managedsql.RecoveryResult{Kind: "graph", RequestID: []byte{255}, Graph: &network.GraphExecuteResponse{MutationReceipt: receipt}}
	wire, err := managedRecoveryWire(result)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(wire)
	if err != nil || !bytes.Contains(raw, []byte(`"graph":{"slot":17,"status":"committed","applied":false`)) || !bytes.Contains(raw, []byte(`"request_id_b64":"/w=="`)) {
		t.Fatal("Graph receipt/identity lost", string(raw), err)
	}
	result.SQL = &managedsql.ExecuteResponse{}
	if _, err := managedRecoveryWire(result); err == nil {
		t.Fatal("multiple result arms accepted")
	}
	result.Kind, result.SQL = "", nil
	if _, err := managedRecoveryWire(result); err == nil {
		t.Fatal("Graph payload accepted as none")
	}
	status, _ := json.Marshal(managedStatusWire("graph", network.RequestStatusResponse{Receipt: &receipt}))
	if !bytes.Contains(status, []byte(`"applied":false`)) {
		t.Fatal("observational status omitted false", string(status))
	}
}

func TestManagedGraphFrameEnvelopeStrict(t *testing.T) {
	for _, input := range []string{
		`{}`, `null`, `[]`, `{"frame_b64":null}`, `{"Frame_b64":"TQ=="}`,
		`{"frame_b64":"TQ==","frame_b64":"TQ=="}`,
		`{"frame_b64":null,"frame_b64":"TQ=="}`,
		`{"frame_b64":"TQ==","frame_b\u00364":"TQ=="}`,
		`{"frame_b64":"TQ==","other":0}`, `{"frame_b64":"TQ=="} {}`,
		`{"frame_b64":"TQ"}`, `{"frame_b64":"TR=="}`, `{"frame_b64":""}`,
	} {
		if _, err := decodeManagedGraphFrame([]byte(input)); err == nil {
			t.Fatal("invalid transport envelope accepted", input)
		}
	}
	for _, input := range []string{`{"frame_b64":"TQ=="}`, `{"frame_b\u00364":"T\u0051=="}`} {
		body, err := decodeManagedGraphFrame([]byte(input))
		if err != nil || !bytes.Equal(body, []byte{'M'}) {
			t.Fatal("transport bytes changed", err)
		}
	}
}

func TestManagedGraphFrameEnvelopeByteLimit(t *testing.T) {
	// Envelope-boundary bytes are deliberately not claimed as a valid frame.
	for _, size := range []int{256 << 10, (256 << 10) + 1} {
		input := fmt.Sprintf(`{"frame_b64":%q}`, base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{'x'}, size)))
		body, err := decodeManagedGraphFrame([]byte(input))
		if size == 256<<10 {
			if err != nil || len(body) != size {
				t.Fatal("exact transport size rejected", err)
			}
		} else if err == nil {
			t.Fatal("oversized transport accepted")
		}
	}
}
