package qlog

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestEntryEncodeDecode(t *testing.T) {
	entry := Entry{
		Slot:    42,
		Type:    EntryProposal,
		Payload: []byte("test payload"),
	}
	copy(entry.Hash[:], []byte("0123456789abcdef0123456789abcdef"))

	// Encode
	data := entry.Encode()

	// Decode
	decoded, _, err := DecodeEntry(data)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if decoded.Slot != entry.Slot {
		t.Errorf("slot mismatch: got %d, want %d", decoded.Slot, entry.Slot)
	}
	if decoded.Type != entry.Type {
		t.Errorf("type mismatch: got %d, want %d", decoded.Type, entry.Type)
	}
	if string(decoded.Payload) != string(entry.Payload) {
		t.Errorf("payload mismatch: got %s, want %s", decoded.Payload, entry.Payload)
	}
}

func TestEntryChecksumCoversPayloadAndRejectsOldLayout(t *testing.T) {
	data := (Entry{Slot: 1, Payload: []byte("payload")}).Encode()
	data[len(data)-1] ^= 1
	if _, _, err := DecodeEntry(data); err == nil {
		t.Fatal("payload corruption passed checksum validation")
	}

	old := (Entry{Slot: 2, Payload: []byte("old")}).Encode()
	old[44] = 0
	if _, _, err := DecodeEntry(old); err == nil {
		t.Fatal("accepted WAL entry without current marker")
	}
}

func TestWALAppendRead(t *testing.T) {
	dir, err := os.MkdirTemp("", "qlog-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	wal, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	// Append entries
	for i := uint64(1); i <= 5; i++ {
		err := wal.Append(Entry{
			Slot:    i,
			Type:    EntryProposal,
			Payload: []byte("test"),
		})
		if err != nil {
			t.Fatalf("append error: %v", err)
		}
	}

	// Read entries
	entries, err := wal.Read()
	if err != nil {
		t.Fatalf("read error: %v", err)
	}

	if len(entries) != 5 {
		t.Errorf("expected 5 entries, got %d", len(entries))
	}

	for i, e := range entries {
		if e.Slot != uint64(i+1) {
			t.Errorf("entry %d: slot mismatch: got %d, want %d", i, e.Slot, i+1)
		}
	}
}

func TestWALReadLargeEntry(t *testing.T) {
	dir := t.TempDir()
	want := Entry{Slot: 1, Type: EntryProposal, Payload: make([]byte, 2*1024*1024)}
	want.Payload[len(want.Payload)-1] = 1

	wal, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	if err := wal.Append(want); err != nil {
		t.Fatal(err)
	}
	got, err := wal.Read()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !bytes.Equal(got[0].Payload, want.Payload) {
		t.Fatal("large WAL entry did not round-trip")
	}
}

func TestWALRolloverRoundTrip(t *testing.T) {
	dir := t.TempDir()
	wal, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	wal.maxSize = 100
	for slot := uint64(1); slot <= 3; slot++ {
		if err := wal.Append(Entry{Slot: slot, Type: EntryProposal, Payload: []byte("rollover")}); err != nil {
			t.Fatal(err)
		}
		if err := wal.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	if len(wal.segments) < 2 {
		t.Fatal("WAL did not roll over")
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	entries, err := reopened.Read()
	if err != nil || len(entries) != 3 {
		t.Fatalf("entries=%d err=%v", len(entries), err)
	}
}

func TestWALRepairsTornTailBeforeAppend(t *testing.T) {
	dir := t.TempDir()
	wal, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for slot := uint64(1); slot <= 2; slot++ {
		if err := wal.Append(Entry{Slot: slot, Type: EntryProposal, Payload: []byte("value")}); err != nil {
			t.Fatal(err)
		}
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "seg_001.log")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, info.Size()-5); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.Append(Entry{Slot: 3, Type: EntryProposal, Payload: []byte("after-repair")}); err != nil {
		t.Fatal(err)
	}
	entries, err := reopened.Read()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].Slot != 1 || entries[1].Slot != 3 {
		t.Fatalf("repaired entries = %#v", entries)
	}
}

func TestWALSortsSegmentIndexesNumerically(t *testing.T) {
	dir := t.TempDir()
	for _, entry := range []Entry{{Slot: 999, Type: EntryProposal}, {Slot: 1000, Type: EntryProposal}} {
		path := filepath.Join(dir, fmt.Sprintf("seg_%03d.log", entry.Slot))
		if err := os.WriteFile(path, entry.Encode(), 0644); err != nil {
			t.Fatal(err)
		}
	}
	wal, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	entries, err := wal.Read()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].Slot != 999 || entries[1].Slot != 1000 || wal.current.index != 1000 {
		t.Fatalf("segments were not sorted numerically: %#v current=%d", entries, wal.current.index)
	}
}

func TestLockFile(t *testing.T) {
	dir, err := os.MkdirTemp("", "qlog-lock-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Acquire lock
	cleanStart, lock, err := Acquire(dir)
	if err != nil {
		t.Fatal(err)
	}

	if !cleanStart {
		t.Error("expected clean start")
	}

	// Release lock
	if err := lock.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "lock.qlog")); err != nil {
		t.Fatalf("stable lock inode missing after release: %v", err)
	}

	// Acquire again
	cleanStart, lock2, err := Acquire(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer lock2.Release()

	if !cleanStart {
		t.Error("expected clean start after release")
	}
}
