package types

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"math"
	"testing"
)

func TestManagedGraphExecutionCrossSDKGolden(t *testing.T) {
	want, err := hex.DecodeString("5147525801000000204d474f0100000001720000000852455455524e203109ffffffffffffffff0000")
	if err != nil {
		t.Fatal(err)
	}
	got, err := EncodeManagedGraphExecution(GraphCommand{RequestID: "r", Cypher: "RETURN 1"})
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("golden got=%x err=%v", got, err)
	}
}

func TestManagedGraphExecutionIntentAndProjection(t *testing.T) {
	raw := GraphCommand{RequestID: string([]byte{0xff}), Cypher: "RETURN $x", Args: map[string]any{"x": []any{float32(.1), float64(1), math.Copysign(0, -1), json.Number("1e0"), string([]byte{0xfe}), []any(nil)}}}
	frame, err := EncodeManagedGraphExecution(raw)
	if err != nil {
		t.Fatal(err)
	}
	original, ok, err := DecodeManagedGraphExecution(frame)
	if err != nil || !ok || original.RequestID != raw.RequestID {
		t.Fatal("raw original lost", err)
	}
	fp, err := ManagedGraphIntentFingerprint(raw)
	if err != nil {
		t.Fatal(err)
	}
	clone, err := cloneGraphCommandForOrdinaryEncoding(raw)
	if err != nil {
		t.Fatal(err)
	}
	clone.RequestID = ""
	item, err := EncodeGraphBatchItem(clone)
	if err != nil || fp != FingerprintBatchItem("rhiza/managed-graph-intent/v1", item) {
		t.Fatal("intent changed", err)
	}
	e, err := ManagedGraphExecutionProjection(original)
	if err != nil {
		t.Fatal(err)
	}
	args := e.Args["x"].([]any)
	if args[0] != json.Number("0.1") || args[1] != json.Number("1") || args[2] != json.Number("-0") || args[3] != json.Number("1e0") || args[4] != "\ufffd" || len(args[5].([]any)) != 0 {
		t.Fatalf("projection changed: %#v", args)
	}
	if ValidateManagedGraphExecutionPolicy(frame) != nil || ValidateManagedKVExecutionPolicy(frame) == nil || ValidateExecutionPolicy(frame) == nil {
		t.Fatal("profile crossing accepted")
	}
	bad := append(bytes.Clone(frame), 0)
	if _, _, err := DecodeManagedGraphExecution(bad); err == nil {
		t.Fatal("trailing bytes accepted")
	}
}

func TestManagedGraphIntentUsesRawMapBeforeJSONReplacement(t *testing.T) {
	raw := GraphCommand{RequestID: "r", Cypher: "RETURN $x", Args: map[string]any{string([]byte{0xff}): 1, string([]byte{0xfe}): 2, "\ufffd": 3}}
	got, err := ManagedGraphIntentFingerprint(raw)
	if err != nil {
		t.Fatal(err)
	}
	clone, _ := cloneGraphCommandForOrdinaryEncoding(raw)
	clone.RequestID = ""
	legacy, _ := EncodeGraphBatchItem(clone)
	if got != FingerprintBatchItem("rhiza/managed-graph-intent/v1", legacy) {
		t.Fatal("intent computed after lossy map decode")
	}
}
