package types

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestManagedKVOriginalLosslessRoundTrip(t *testing.T) {
	identities := []string{"ordinary", " \t\n", string([]byte{0xff, 0, 0xfe, 'x'})}
	blobs := [][]byte{nil, {}, {0, 0xff, 1, 0xfe}}
	for _, operation := range []string{"put", "delete", "cas"} {
		for _, identity := range identities {
			for _, value := range blobs {
				for _, expected := range blobs {
					original := KVCommand{RequestID: identity, Operation: operation, Key: identity, Value: value, Expected: expected, ExpectedExists: true, TTLMS: math.MaxInt64, ObservedAtUnixMS: 17, ExpiresAtUnixMS: 29}
					raw, owned, err := EncodeManagedKVOriginal(original)
					if err != nil {
						t.Fatal(err)
					}
					want := original
					want.ObservedAtUnixMS, want.ExpiresAtUnixMS = 0, 0
					if !reflect.DeepEqual(owned, want) {
						t.Fatalf("encode lost intent: %#v != %#v", owned, want)
					}
					decoded, err := DecodeManagedKVOriginal(raw)
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(decoded, want) {
						t.Fatalf("decode lost intent: %#v != %#v", decoded, want)
					}
					fingerprint, err := KVFingerprint(original)
					if err != nil {
						t.Fatal(err)
					}
					for _, command := range []KVCommand{owned, decoded} {
						got, err := KVFingerprint(command)
						if err != nil || got != fingerprint {
							t.Fatalf("fingerprint changed: %v", err)
						}
					}
					again, _, err := EncodeManagedKVOriginal(decoded)
					if err != nil || !bytes.Equal(raw, again) {
						t.Fatalf("canonical roundtrip changed: %v", err)
					}
					wireID, err := ManagedKVPendingWireID(identity)
					if err != nil || wireID != base64.StdEncoding.EncodeToString([]byte(identity)) {
						t.Fatalf("wire ID changed: %v", err)
					}
				}
			}
		}
	}
}

func TestManagedKVOriginalCanonicalWire(t *testing.T) {
	command := KVCommand{RequestID: "i", Operation: "put", Key: "k", Expected: []byte{}}
	raw, _, err := EncodeManagedKVOriginal(command)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"request_id_b64":"aQ==","operation":"put","key_b64":"aw==","value":null,"expected":"","expected_exists":false,"ttl_ms":0}`
	if string(raw) != want {
		t.Fatalf("wire = %s", raw)
	}
	// JSON escapes and object order are not base64 aliases.
	input := ` {"ttl_ms":0,"expected_exists":false,"expected":"","value":null,"key_b64":"aw==","operation":"put","request_id_b64":"\u0061Q=="} `
	got, err := DecodeManagedKVOriginal(json.RawMessage(input))
	if err != nil || !reflect.DeepEqual(got, command) {
		t.Fatalf("equivalent JSON rejected: %#v, %v", got, err)
	}
}

func TestManagedKVOriginalRejectsMalformedWire(t *testing.T) {
	raw, _, err := EncodeManagedKVOriginal(KVCommand{RequestID: "i", Operation: "put", Key: "k"})
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	for name := range fields {
		t.Run("missing_"+name, func(t *testing.T) {
			copyFields := make(map[string]json.RawMessage)
			for key, value := range fields {
				if key != name {
					copyFields[key] = value
				}
			}
			bad, _ := json.Marshal(copyFields)
			if _, err := DecodeManagedKVOriginal(bad); err == nil {
				t.Fatal("accepted missing field")
			}
		})
		t.Run("duplicate_"+name, func(t *testing.T) {
			bad := append(bytes.Clone(raw[:len(raw)-1]), []byte(`,"`+name+`":`+string(fields[name])+`}`)...)
			if _, err := DecodeManagedKVOriginal(bad); err == nil {
				t.Fatal("accepted duplicate field")
			}
		})
	}
	for _, field := range []string{"request_id_b64", "key_b64", "value", "expected"} {
		for _, bad := range []string{`"!"`, `"aQ"`, `"aR=="`, `"aQ==\n"`, `"_w=="`, `3`, `[]`, `{}`, `true`} {
			changed := bytes.Replace(raw, append([]byte(`"`+field+`":`), fields[field]...), []byte(`"`+field+`":`+bad), 1)
			if _, err := DecodeManagedKVOriginal(changed); err == nil {
				t.Fatalf("accepted %s=%s", field, bad)
			}
		}
	}
	for _, field := range []string{"request_id_b64", "key_b64", "operation", "expected_exists", "ttl_ms"} {
		changed := bytes.Replace(raw, append([]byte(`"`+field+`":`), fields[field]...), []byte(`"`+field+`":null`), 1)
		if _, err := DecodeManagedKVOriginal(changed); err == nil {
			t.Fatalf("accepted null %s", field)
		}
	}
	for _, bad := range []string{"", "null", "[]", "true", "{}", string(raw) + " true", string(raw) + "{}", string(raw[:len(raw)-1]), strings.Replace(string(raw), `"ttl_ms":0`, `"ttl_ms":1.5`, 1), strings.Replace(string(raw), `"ttl_ms":0`, `"ttl_ms":9223372036854775808`, 1), strings.Replace(string(raw), `"expected_exists":false`, `"expected_exists":"false"`, 1), strings.Replace(string(raw), `"operation":"put"`, `"operation":3`, 1), strings.Replace(string(raw), `"ttl_ms":0`, `"unknown":0,"ttl_ms":0`, 1), strings.Replace(string(raw), `"ttl_ms":0`, `"ttl_ms":0,"ttl_\u006ds":0`, 1)} {
		if _, err := DecodeManagedKVOriginal(json.RawMessage(bad)); err == nil {
			t.Fatalf("accepted malformed wire %q", bad)
		}
	}
	oversized := append(bytes.Repeat([]byte(" "), MaxManagedKVOriginalBytes), raw...)
	if _, err := DecodeManagedKVOriginal(oversized); err == nil {
		t.Fatal("accepted oversized raw JSON")
	}
}

func TestManagedKVOriginalInputLimits(t *testing.T) {
	valid := KVCommand{RequestID: "i", Operation: "put", Key: "kkkk"}
	// 192 + 13*(1+3+4) + 5*209656 == 1 MiB exactly.
	boundary := valid
	boundary.Value = make([]byte, 209656)
	raw, _, err := EncodeManagedKVOriginal(boundary)
	if err != nil {
		t.Fatalf("exact charge boundary rejected: %v", err)
	}
	if len(raw) > MaxManagedKVOriginalBytes {
		t.Fatal("canonical cap exceeded")
	}
	if _, err := DecodeManagedKVOriginal(raw); err != nil {
		t.Fatal(err)
	}
	boundary.Value = append(boundary.Value, 0)
	if _, _, err := EncodeManagedKVOriginal(boundary); err == nil {
		t.Fatal("accepted charge overflow")
	}
	// Ignored fields are still retained and charged, including on delete.
	ignored := valid
	ignored.Operation, ignored.ExpectedExists = "delete", true
	ignored.Expected = make([]byte, 209656)
	if _, _, err := EncodeManagedKVOriginal(ignored); err == nil {
		t.Fatal("ignored expected bytes escaped charge")
	}
	for _, mutate := range []func(*KVCommand){
		func(c *KVCommand) { c.RequestID = "" },
		func(c *KVCommand) { c.RequestID = strings.Repeat("i", MaxRequestIDBytes+1) },
		func(c *KVCommand) { c.Key = "" },
		func(c *KVCommand) { c.Key = strings.Repeat("k", 1025) },
		func(c *KVCommand) { c.Operation = "get" },
		func(c *KVCommand) { c.TTLMS = -1 },
		func(c *KVCommand) { c.Value = make([]byte, (1<<20)/5+1) },
		func(c *KVCommand) { c.Expected = make([]byte, (1<<20)/5+1) },
	} {
		bad := valid
		mutate(&bad)
		if _, _, err := EncodeManagedKVOriginal(bad); err == nil {
			t.Fatalf("accepted invalid input %#v", bad)
		}
		wire, err := json.Marshal(managedKVOriginalWire(bad))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodeManagedKVOriginal(wire); err == nil {
			t.Fatal("decoder bypassed logical validation")
		}
	}
	maximum := valid
	maximum.RequestID, maximum.Key, maximum.TTLMS = strings.Repeat("i", MaxRequestIDBytes), strings.Repeat("k", 1024), math.MaxInt64
	if _, _, err := EncodeManagedKVOriginal(maximum); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"", strings.Repeat("i", MaxRequestIDBytes+1)} {
		if _, err := ManagedKVPendingWireID(id); err == nil {
			t.Fatal("invalid wire ID accepted")
		}
	}
}

func TestManagedKVOriginalOwnsInputs(t *testing.T) {
	command := KVCommand{RequestID: "id", Operation: "cas", Key: "key", Value: []byte{1, 2}, Expected: []byte{3, 4}}
	raw, owned, err := EncodeManagedKVOriginal(command)
	if err != nil {
		t.Fatal(err)
	}
	saved := bytes.Clone(raw)
	command.Value[0], command.Expected[0] = 9, 9
	if owned.Value[0] != 1 || owned.Expected[0] != 3 || !bytes.Equal(raw, saved) {
		t.Fatal("caller mutation escaped ownership")
	}
	owned.Value[0], owned.Expected[0] = 8, 8
	if !bytes.Equal(raw, saved) {
		t.Fatal("owned command aliases encoded bytes")
	}
	decoded, err := DecodeManagedKVOriginal(raw)
	if err != nil {
		t.Fatal(err)
	}
	for i := range raw {
		raw[i] = ' '
	}
	if decoded.Value[0] != 1 || decoded.Expected[0] != 3 {
		t.Fatal("decoded blobs alias raw input")
	}
	again, err := DecodeManagedKVOriginal(saved)
	if err != nil {
		t.Fatal(err)
	}
	decoded.Value[0], decoded.Expected[0] = 7, 7
	if again.Value[0] != 1 || again.Expected[0] != 3 {
		t.Fatal("decodes share blobs")
	}
}
