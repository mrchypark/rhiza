package quepaxa

import (
	"bytes"
	"errors"
	"math"
	"strings"
	"testing"
)

func TestDecisionsFromBoundedUsesContiguousPayloadPrefix(t *testing.T) {
	core := &Core{tip: 4, decided: map[Slot]DecidedValue{
		1: {Slot: 1, Value: []byte("a"), Certificate: []byte("12")},
		2: {Slot: 2, Value: []byte("bc"), Certificate: []byte("3")},
		3: {Slot: 3, Value: []byte("oversized"), Certificate: []byte("certificate")},
		4: {Slot: 4, Value: []byte("tail"), Certificate: []byte("must-not-skip")},
	}}
	page, tip, err := core.DecisionsFromBounded(1, 4, 6)
	if err != nil || tip != 4 || len(page) != 2 || page[0].Slot != 1 || page[1].Slot != 2 {
		t.Fatalf("tip=%d page=%+v err=%v, want contiguous 1-2 prefix", tip, page, err)
	}
	if !bytes.Equal(page[0].Value, []byte("a")) || !bytes.Equal(page[0].Certificate, []byte("12")) || !bytes.Equal(page[1].Value, []byte("bc")) || !bytes.Equal(page[1].Certificate, []byte("3")) {
		t.Fatalf("page payloads differ from source: %+v", page)
	}
	page[0].Value[0] = 'z'
	page[0].Certificate[0] = 'z'
	if !bytes.Equal(core.decided[1].Value, []byte("a")) || !bytes.Equal(core.decided[1].Certificate, []byte("12")) {
		t.Fatal("bounded page payload aliases Core-owned decision bytes")
	}
}

func TestDecisionsFromBoundedRejectsFirstOversizeAndInvalidBudgets(t *testing.T) {
	core := &Core{tip: 1, decided: map[Slot]DecidedValue{1: {Slot: 1, Value: []byte("value"), Certificate: []byte("cert")}}}
	if _, _, err := core.DecisionsFromBounded(1, 1, 8); err == nil || !strings.Contains(err.Error(), "decision 1") || !strings.Contains(err.Error(), "8 bytes") {
		t.Fatalf("first oversize error=%v, want slot and budget", err)
	}
	for _, limits := range [][2]int{{0, 1}, {1, 0}, {-1, 1}, {1, -1}} {
		if _, _, err := core.DecisionsFromBounded(1, limits[0], limits[1]); err == nil {
			t.Fatalf("limits %v unexpectedly succeeded", limits)
		}
	}
}

func TestDecisionsFromBoundedCountAndMaximumSlotBoundaries(t *testing.T) {
	decided := make(map[Slot]DecidedValue, 1025)
	for slot := Slot(1); slot <= 1025; slot++ {
		decided[slot] = DecidedValue{Slot: slot, Value: []byte{1}, Certificate: []byte{2}}
	}
	core := &Core{tip: 1025, decided: decided}
	page, tip, err := core.DecisionsFromBounded(1, 1024, 1024*2)
	if err != nil || tip != 1025 || len(page) != 1024 || page[0].Slot != 1 || page[len(page)-1].Slot != 1024 {
		t.Fatalf("tip=%d page slots=%d-%d len=%d err=%v", tip, page[0].Slot, page[len(page)-1].Slot, len(page), err)
	}
	tail, _, err := core.DecisionsFromBounded(1025, 1, 2)
	if err != nil || len(tail) != 1 || tail[0].Slot != 1025 {
		t.Fatalf("tail=%+v err=%v, want slot 1025", tail, err)
	}

	max := Slot(math.MaxUint64)
	maxCore := &Core{tip: max, decided: map[Slot]DecidedValue{max: {Slot: max, Value: []byte{1}, Certificate: []byte{2}}}}
	last, reportedTip, err := maxCore.DecisionsFromBounded(max, 2, 2)
	if err != nil || reportedTip != max || len(last) != 1 || last[0].Slot != max {
		t.Fatalf("maximum slot page=%+v tip=%d err=%v", last, reportedTip, err)
	}
}

func TestDecisionsFromBoundedPreservesCompactionAndGapErrors(t *testing.T) {
	compacted := &Core{tip: 2, floor: 1, decided: map[Slot]DecidedValue{2: {Slot: 2, Value: []byte{1}, Certificate: []byte{1}}}}
	if _, _, err := compacted.DecisionsFromBounded(1, 1, 2); !errors.Is(err, ErrCompacted) {
		t.Fatalf("compacted error=%v, want ErrCompacted", err)
	}
	gap := &Core{tip: 2, decided: map[Slot]DecidedValue{2: {Slot: 2, Value: []byte{1}, Certificate: []byte{1}}}}
	if _, _, err := gap.DecisionsFromBounded(1, 2, 4); err == nil || !strings.Contains(err.Error(), "gap at slot 1") {
		t.Fatalf("gap error=%v, want slot 1 gap", err)
	}
}
