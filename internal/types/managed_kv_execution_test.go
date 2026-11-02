package types

import (
	"bytes"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestManagedKVExecutionRoundTrip(t *testing.T) {
	var items [][]byte
	var want []KVCommand
	for _, id := range []string{string([]byte{255}), string([]byte{254}), "\ufffd", "a\x00b"} {
		for _, blob := range [][]byte{nil, {}, {0, 255}} {
			c := KVCommand{RequestID: id, Operation: "cas", Key: id, Value: blob, Expected: bytes.Clone(blob), ExpectedExists: true, TTLMS: math.MaxInt64, ObservedAtUnixMS: math.MinInt64, ExpiresAtUnixMS: math.MaxInt64}
			wire, err := EncodeManagedKVCommand(c)
			if err != nil {
				t.Fatal(err)
			}
			got, ok, err := DecodeManagedKVCommand(wire)
			if err != nil || !ok || !reflect.DeepEqual(got, c) {
				t.Fatalf("roundtrip %#v %#v %v", c, got, err)
			}
			if _, ok, _ := DecodeKVCommand(wire); ok {
				t.Fatal("legacy accepted mixed singleton")
			}
			item, err := EncodeManagedKVBatchItem(c)
			if err != nil {
				t.Fatal(err)
			}
			items = append(items, item)
			want = append(want, c)
			if len(got.Value) > 0 {
				before := bytes.Clone(c.Value)
				wireBefore := bytes.Clone(wire)
				got.Value[0] ^= 255
				if !bytes.Equal(c.Value, before) || !bytes.Equal(wire, wireBefore) {
					t.Fatal("aliased input")
				}
			}
		}
	}
	wire, err := AssembleManagedKVBatch(items)
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := DecodeManagedKVBatch(wire)
	if !ok || err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("batch mismatch %v", err)
	}
	if _, ok, _ := DecodeKVBatch(wire); ok {
		t.Fatal("legacy accepted mixed batch")
	}
	legacy, _ := EncodeKVCommand(want[0])
	if _, ok, _ := DecodeManagedKVCommand(legacy); ok {
		t.Fatal("mixed accepted legacy")
	}
	legacyBatch := AssembleKVBatch([][]byte{[]byte(`{"request_id":"i"}`)})
	if _, ok, _ := DecodeManagedKVBatch(legacyBatch); ok {
		t.Fatal("mixed accepted legacy batch")
	}
}

func TestManagedKVExecutionStrict(t *testing.T) {
	item, err := EncodeManagedKVBatchItem(KVCommand{RequestID: "id", Operation: "delete", Key: "key"})
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(item, &fields); err != nil {
		t.Fatal(err)
	}
	for key := range fields {
		for _, replacement := range []string{"missing", "null", "[]"} {
			copyFields := make(map[string]json.RawMessage)
			for k, v := range fields {
				copyFields[k] = v
			}
			if replacement == "missing" {
				delete(copyFields, key)
			} else {
				copyFields[key] = json.RawMessage(replacement)
			}
			bad, _ := json.Marshal(copyFields)
			// The two nullable blobs alone accept null.
			if replacement == "null" && (key == "value" || key == "expected") {
				continue
			}
			if _, err := decodeManagedKVExecution(bad); err == nil {
				t.Fatalf("accepted %s %s", key, replacement)
			}
		}
		bad := append(bytes.Clone(item[:len(item)-1]), ',')
		name, _ := json.Marshal(key)
		bad = append(bad, name...)
		bad = append(bad, ':')
		bad = append(bad, fields[key]...)
		bad = append(bad, '}')
		if _, err := decodeManagedKVExecution(bad); err == nil {
			t.Fatalf("duplicate %s", key)
		}
	}
	for _, bad := range []string{string(item) + "{}", `null`, `[]`, strings.Replace(string(item), `"request_id_b64":"aWQ="`, `"request_id_b64":"aWQ"`, 1), strings.Replace(string(item), `"operation":"delete"`, `"extra":0,"operation":"delete"`, 1)} {
		if _, err := decodeManagedKVExecution([]byte(bad)); err == nil {
			t.Fatalf("accepted %s", bad)
		}
	}
	for _, batch := range [][]byte{[]byte("QKVB\x01[]"), []byte("QKVB\x01null"), append([]byte("QKVB\x01["), append(bytes.Clone(item), []byte(",null]")...)...)} {
		if _, ok, err := DecodeManagedKVBatch(batch); !ok || err == nil {
			t.Fatal("invalid batch accepted")
		}
	}
	items := make([][]byte, 64)
	for i := range items {
		items[i] = item
	}
	wire, err := AssembleManagedKVBatch(items)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok, err := DecodeManagedKVBatch(wire); err != nil || !ok || len(got) != 64 {
		t.Fatal("boundary rejected")
	}
	if _, err := AssembleManagedKVBatch(append(items, item)); err == nil {
		t.Fatal("oversized count accepted")
	}
	tooMany := assembleBatch(managedKVBatchMagic, append(items, item))
	if _, _, err := DecodeManagedKVBatch(tooMany); err == nil {
		t.Fatal("oversized decode accepted")
	}
	if _, err := decodeManagedKVExecution(bytes.Repeat([]byte{' '}, MaxManagedKVExecutionBytes+1)); err == nil {
		t.Fatal("oversized execution accepted")
	}
}

func TestManagedKVFingerprintIntent(t *testing.T) {
	c := KVCommand{RequestID: "id", Operation: "put", Key: string([]byte{255})}
	fp, err := ManagedKVFingerprint(c)
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*KVCommand){func(c *KVCommand) { c.RequestID = "other" }, func(c *KVCommand) { c.ObservedAtUnixMS = 1; c.ExpiresAtUnixMS = 2 }, func(c *KVCommand) { c.Value = []byte{}; c.Expected = []byte{} }} {
		d := c
		change(&d)
		got, err := ManagedKVFingerprint(d)
		if err != nil || got != fp {
			t.Fatal("nonintent changed fingerprint")
		}
	}
	for _, change := range []func(*KVCommand){func(c *KVCommand) { c.Key = string([]byte{254}) }, func(c *KVCommand) { c.Key = "\ufffd" }, func(c *KVCommand) { c.Operation = "delete" }, func(c *KVCommand) { c.Value = []byte{1} }, func(c *KVCommand) { c.Expected = []byte{1} }, func(c *KVCommand) { c.ExpectedExists = true }, func(c *KVCommand) { c.TTLMS = 1 }} {
		d := c
		change(&d)
		got, err := ManagedKVFingerprint(d)
		if err != nil || got == fp {
			t.Fatal("intent fingerprint collision")
		}
	}
	legacy, _ := KVFingerprint(c)
	if legacy == fp {
		t.Fatal("domain not separated")
	}
}

func TestManagedKVExecutionLegacyGolden(t *testing.T) {
	c := KVCommand{RequestID: "i", Operation: "put", Key: "k"}
	wire, err := EncodeKVCommand(c)
	if err != nil || string(wire) != "QKVC\x00"+`{"request_id":"i","operation":"put","key":"k"}` {
		t.Fatalf("legacy wire changed %q", wire)
	}
	item, _ := EncodeKVBatchItem(c)
	batch := AssembleKVBatch([][]byte{item})
	if string(batch) != "QKVB\x00["+string(item)+"]" {
		t.Fatal("legacy batch changed")
	}
	got, _ := KVFingerprint(c)
	want := FingerprintBatchItem("rhiza/kv", []byte(`{"request_id":"","operation":"put","key":"k"}`))
	if got != want {
		t.Fatal("legacy fingerprint changed")
	}
}
