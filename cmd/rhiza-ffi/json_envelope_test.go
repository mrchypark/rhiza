//go:build cgo

package main

import (
	"bytes"
	"encoding/json"
	"math"
	"strings"
	"testing"
)

func legacyMapDataJSON(value any) []byte {
	b, err := json.Marshal(map[string]any{"data": value})
	if err != nil {
		return marshalError("internal", "encode response: "+err.Error())
	}
	return b
}

func TestDataJSONMatchesLegacyMapBytes(t *testing.T) {
	var typedNil *string
	for _, tc := range []struct {
		name  string
		value any
	}{
		{"null", nil}, {"typed-null", typedNil}, {"false", false},
		{"empty-string", ""}, {"empty-array", []any{}}, {"nil-array", []any(nil)},
		{"empty-bytes", []byte{}}, {"binary", []byte{0, 255, 1}},
		{"escaping", "<>&\u2028\u2029\n\"\\"}, {"invalid-utf8", string([]byte{0xff, 0xfe})},
		{"sql-rows", map[string]any{"columns": []string{"a", "b"}, "rows": [][]any{{int64(1), nil}, {true, []byte{0, 255}}, {json.Number("9007199254740993"), "value"}}}},
		{"error-channel", make(chan int)}, {"error-nan", math.NaN()}, {"error-infinity", math.Inf(1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, want := dataJSON(tc.value), legacyMapDataJSON(tc.value)
			if !bytes.Equal(got, want) {
				t.Fatalf("response bytes changed: got %q want %q", got, want)
			}
		})
	}
	for _, tc := range []struct {
		value any
		want  string
	}{{nil, `{"data":null}`}, {false, `{"data":false}`}, {[]any{}, `{"data":[]}`}, {[]byte{}, `{"data":""}`}} {
		if got := string(dataJSON(tc.value)); got != tc.want {
			t.Fatalf("literal wire changed: got %s want %s", got, tc.want)
		}
	}
}

func TestJSONEnvelopeInputSizeLimitUnchanged(t *testing.T) {
	if validInput(make([]byte, maxInputBytes)) != nil || validInput(make([]byte, maxInputBytes+1)) == nil {
		t.Fatal("FFI input size boundary changed")
	}
}

func BenchmarkFFIDataJSONEnvelope(b *testing.B) {
	// Compare production across base/head; legacy_map is the unchanged control.
	for _, tc := range []struct {
		name  string
		value any
	}{
		{"null", nil}, {"receipt", map[string]any{"slot": uint64(42), "status": "committed", "applied": false}},
		{"blob_1k", bytes.Repeat([]byte{0x5a}, 1024)},
		{"sql_rows", map[string]any{"columns": []string{"key", "value"}, "rows": [][]any{{"row", strings.Repeat("x", 1024)}, {int64(2), nil}}}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			for _, encoder := range []struct {
				name   string
				encode func(any) []byte
			}{{"legacy_map", legacyMapDataJSON}, {"production", dataJSON}} {
				b.Run(encoder.name, func(b *testing.B) {
					b.ReportAllocs()
					for range b.N {
						if len(encoder.encode(tc.value)) == 0 {
							b.Fatal("empty response")
						}
					}
				})
			}
		})
	}
}
