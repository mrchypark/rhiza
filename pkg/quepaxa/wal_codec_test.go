package quepaxa

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
)

func TestRecorderEntryFlatBufferRoundTrip(t *testing.T) {
	first := newProposal(Priority{1}, "n1", bytes.Repeat([]byte{0xab}, 512))
	current := newProposal(Priority{2}, "n2", []byte("current"))
	prior := newProposal(Priority{3}, "n3", []byte("prior"))
	want := recorderEntry{Slot: 42, State: ISR{
		Step:             17,
		FirstCurrent:     &first,
		AggregateCurrent: &current,
		AggregatePrior:   &prior,
	}}

	got, err := decodeRecorderEntry(encodeRecorderEntry(want.Slot, want.State))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip mismatch:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestRecorderEntryRejectsCorruptFlatBuffer(t *testing.T) {
	proposal := newProposal(Priority{1}, "n1", []byte("value"))
	payload := encodeRecorderEntry(1, ISR{Step: 4, FirstCurrent: &proposal})
	for _, corrupt := range [][]byte{payload[:len(isrEntryMagic)+4], append([]byte(nil), payload...)} {
		if len(corrupt) == len(payload) {
			corrupt[len(isrEntryMagic)+4] ^= 0xff
		}
		if _, err := decodeRecorderEntry(corrupt); err == nil {
			t.Fatalf("accepted corrupt payload of length %d", len(corrupt))
		}
	}
}

func TestRecorderEntryRejectsNumberedJSONFormat(t *testing.T) {
	proposal := newProposal(Priority{1}, "n1", []byte("old"))
	want := recorderEntry{Slot: 7, State: ISR{Step: 5, AggregateCurrent: &proposal}}
	jsonPayload, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	payload := append([]byte("QISR1\x00"), jsonPayload...)
	if _, err := decodeRecorderEntry(payload); err == nil {
		t.Fatal("accepted numbered recorder WAL format")
	}
}

func TestRecorderEntryFlatBufferAvoidsJSONAmplification(t *testing.T) {
	proposal := newProposal(Priority{1}, "n1", bytes.Repeat([]byte{0xab}, 1024))
	entry := recorderEntry{Slot: 1, State: ISR{
		Step:             4,
		FirstCurrent:     &proposal,
		AggregateCurrent: &proposal,
		AggregatePrior:   &proposal,
	}}
	jsonPayload, err := json.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	flatPayload := encodeRecorderEntry(entry.Slot, entry.State)
	if len(flatPayload)*2 >= len(jsonPayload) {
		t.Fatalf("FlatBuffers payload=%d bytes, JSON=%d bytes", len(flatPayload), len(jsonPayload))
	}
}
