package types

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestManagedGraphOriginalRoundTrip(t *testing.T) {
	for _, raw := range []string{string([]byte{0xff}), string([]byte{0xfe}), "\ufffd"} {
		command := GraphCommand{RequestID: raw, Cypher: "RETURN $x", Args: map[string]any{
			raw: []any{nil, true, false, int64(-7), float32(1.25), math.Copysign(0, -1), json.Number("1e0"), raw, []any(nil), []any{}, map[string]any(nil), map[string]any{}},
		}, Events: []GraphStreamEvent{{Stream: "events", Kind: "new", Payload: map[string]any{raw: json.Number("9223372036854775808")}}},
			StreamOffset: &GraphStreamOffsetMutation{Stream: "s", Consumer: "c", Sequence: math.MaxUint64}, StreamTrim: &GraphStreamTrimMutation{Stream: "s", ThroughSequence: math.MaxUint64}}
		body, err := EncodeManagedGraphOriginal(command)
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeManagedGraphOriginal(body)
		if err != nil || !reflect.DeepEqual(got, command) {
			t.Fatalf("roundtrip mismatch: got=%#v err=%v", got, err)
		}
		if math.Float64bits(got.Args[raw].([]any)[5].(float64)) != math.Float64bits(math.Copysign(0, -1)) {
			t.Fatal("negative zero lost")
		}
		again, err := EncodeManagedGraphOriginal(got)
		if err != nil || !bytes.Equal(body, again) {
			t.Fatal("noncanonical roundtrip", err)
		}
		before := append([]byte(nil), body...)
		command.Args[raw] = "changed"
		if !bytes.Equal(body, before) {
			t.Fatal("encoder aliases caller")
		}
		for i := range body {
			body[i] = 0
		}
		if got.RequestID != raw {
			t.Fatal("decoder aliases input")
		}
	}
}

func TestManagedGraphOriginalPresenceAndIntegers(t *testing.T) {
	for _, command := range []GraphCommand{{}, {Args: map[string]any{}, Events: []GraphStreamEvent{}}, {StreamOffset: &GraphStreamOffsetMutation{}, StreamTrim: &GraphStreamTrimMutation{}}} {
		body, err := EncodeManagedGraphOriginal(command)
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeManagedGraphOriginal(body)
		if err != nil || !reflect.DeepEqual(got, command) {
			t.Fatalf("presence lost %#v %v", got, err)
		}
	}
	for _, v := range []any{int(7), int8(7), int16(7), int32(7), int64(7), uint(7), uint8(7), uint16(7), uint32(7), uint64(7)} {
		body, err := EncodeManagedGraphOriginal(GraphCommand{Args: map[string]any{"x": v}})
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeManagedGraphOriginal(body)
		if err != nil || got.Args["x"] != int64(7) {
			t.Fatal("integer normalization", got, err)
		}
	}
}

func TestManagedGraphOriginalRejectsInvalid(t *testing.T) {
	type alias string
	for _, v := range []any{[]byte{}, alias("x"), math.NaN(), math.Inf(1), float32(math.Inf(-1)), uint64(math.MaxUint64), json.Number("01"), json.Number("null"), json.Number("1e999"), json.Number(" 1")} {
		if _, err := EncodeManagedGraphOriginal(GraphCommand{Args: map[string]any{"x": v}}); err == nil {
			t.Fatalf("accepted invalid %T:%v", v, v)
		}
	}
	body, err := EncodeManagedGraphOriginal(GraphCommand{Args: map[string]any{"a": nil, "b": nil}})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(body); i++ {
		if _, err := DecodeManagedGraphOriginal(body[:i]); err == nil {
			t.Fatalf("accepted truncation %d", i)
		}
	}
	for _, bad := range [][]byte{append(append([]byte(nil), body...), 0), bytes.Repeat([]byte{0}, managedGraphOriginalLimit+1), []byte("MGO\x02")} {
		if _, err := DecodeManagedGraphOriginal(bad); err == nil {
			t.Fatal("accepted malformed body")
		}
	}
	// Header, two empty strings, map tag/count, then first raw key.
	duplicate := append([]byte(nil), body...)
	duplicate[27] = 'a'
	if _, err := DecodeManagedGraphOriginal(duplicate); err == nil {
		t.Fatal("accepted duplicate/nonordered key")
	}
	unordered := append([]byte(nil), body...)
	unordered[21], unordered[27] = 'b', 'a'
	if _, err := DecodeManagedGraphOriginal(unordered); err == nil {
		t.Fatal("accepted unsorted map")
	}
	count := append([]byte(nil), body...)
	binary.BigEndian.PutUint32(count[13:17], 65537)
	if _, err := DecodeManagedGraphOriginal(count); err == nil {
		t.Fatal("accepted oversized count")
	}
	unknown := append([]byte(nil), body...)
	unknown[12] = 255
	if _, err := DecodeManagedGraphOriginal(unknown); err == nil {
		t.Fatal("accepted unknown tag")
	}
	var deep any = nil
	for range 33 {
		deep = []any{deep}
	}
	if _, err := EncodeManagedGraphOriginal(GraphCommand{Args: map[string]any{"x": deep}}); err == nil {
		t.Fatal("accepted excessive depth")
	}
	if _, err := EncodeManagedGraphOriginal(GraphCommand{Cypher: strings.Repeat("x", managedGraphOriginalLimit)}); err == nil {
		t.Fatal("accepted excessive body")
	}
}

func TestManagedGraphOriginalFloatFamilyBudget(t *testing.T) {
	// Actual ordinary charge arithmetic: Graph node32; ID/Cypher strings each
	// 32+13*len; Args map node32+container64+entry32+key32+13;
	// list node32+container64+16*n; float nodes32*n; empty Events64.
	const n = 21800
	items := make([]any, n)
	for i := range items {
		items[i] = float64(1)
	}
	command := GraphCommand{RequestID: "r", Cypher: "RETURN $x", Args: map[string]any{"x": items}}
	charge := 32 + (32 + 13) + (32 + 13*len(command.Cypher)) + (32 + 64 + 32 + 32 + 13) + (32 + 64 + 16*n) + 32*n + 64
	if charge > 1<<20 {
		t.Fatalf("fixture exceeds ordinary charge: %d", charge)
	}
	body, err := EncodeManagedGraphOriginal(command)
	if err != nil {
		t.Fatal(err)
	}
	if len(body)*16 > charge*3 || len(body) > managedGraphOriginalLimit {
		t.Fatalf("budget body=%d charge=%d", len(body), charge)
	}
	if _, err := DecodeManagedGraphOriginal(body); err != nil {
		t.Fatal(err)
	}
	legacy, err := EncodeGraphCommand(command)
	if err != nil || len(legacy) > 128<<10 {
		t.Fatalf("legacy representation size=%d err=%v", len(legacy), err)
	}
	// This proves representation/charge arithmetic only, not an actual Cypher
	// engine ACK or a new execution-frame capacity.
}

func TestManagedGraphOriginalExactBodyBudget(t *testing.T) {
	// Pure codec boundary, deliberately not an ordinary admission/ACK fixture.
	// Header4 + ID length4 + Cypher length4 + nil map5 + nil Events4 +
	// two absent operation flags2 = 23 fixed bytes.
	command := GraphCommand{Cypher: strings.Repeat("x", managedGraphOriginalLimit-23)}
	body, err := EncodeManagedGraphOriginal(command)
	if err != nil || len(body) != managedGraphOriginalLimit {
		t.Fatalf("exact budget len=%d err=%v", len(body), err)
	}
	if _, err := DecodeManagedGraphOriginal(body); err != nil {
		t.Fatal(err)
	}
	command.Cypher += "x"
	if _, err := EncodeManagedGraphOriginal(command); err == nil {
		t.Fatal("accepted body overflow")
	}
}

func TestManagedGraphOriginalNumberBudgetBeforeValidation(t *testing.T) {
	// Invalid grammar deliberately distinguishes the budget error from the
	// JSON-validation error: oversized lexemes must never reach validation.
	huge := json.Number(strings.Repeat("x", 4*managedGraphOriginalLimit))
	if _, err := EncodeManagedGraphOriginal(GraphCommand{Args: map[string]any{"n": huge}}); err == nil || !strings.Contains(err.Error(), "number exceeds body budget") {
		t.Fatalf("oversized number did not fail at budget boundary: %v", err)
	}
	if validGraphNumber(string(huge)) {
		t.Fatal("validator accepted oversized number")
	}
	w := graphOriginalWriter{data: make([]byte, managedGraphOriginalLimit-6)}
	w.value(json.Number("10"), 1)
	if w.err == nil || !strings.Contains(w.err.Error(), "number exceeds body budget") || len(w.data) != managedGraphOriginalLimit-6 {
		t.Fatalf("remaining budget checked after append/validation: len=%d err=%v", len(w.data), w.err)
	}
	w = graphOriginalWriter{data: make([]byte, managedGraphOriginalLimit-6)}
	w.value(json.Number("1"), 1)
	if w.err != nil || len(w.data) != managedGraphOriginalLimit {
		t.Fatalf("exact-fit valid number rejected: len=%d err=%v", len(w.data), w.err)
	}
	for _, number := range []json.Number{"0", "-1", "1e0", "1.25", "9223372036854775808"} {
		body, err := EncodeManagedGraphOriginal(GraphCommand{Args: map[string]any{"n": number}})
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeManagedGraphOriginal(body)
		if err != nil || got.Args["n"] != number {
			t.Fatalf("valid number changed: %#v %v", got, err)
		}
	}
}
