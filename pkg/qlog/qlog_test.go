package qlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/thanos-io/objstore"
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

func TestRecoveryInstallsExactObjectStoreSegment(t *testing.T) {
	segment := append(
		Entry{Slot: 1, Type: EntryProposal, Payload: []byte("one")}.Encode(),
		Entry{Slot: 2, Type: EntryDecide, Payload: []byte("two")}.Encode()...,
	)
	hash := sha256.Sum256(segment)
	manifest := Manifest{Segments: []SegmentMeta{{Index: 1, StartSlot: 1, EndSlot: 2, Size: int64(len(segment)), Hash: hash, Synced: true}}, TipSlot: 2}
	manifestData, err := json.Marshal(&manifest)
	if err != nil {
		t.Fatal(err)
	}
	bucket := objstore.NewInMemBucket()
	ctx := context.Background()
	if err := bucket.Upload(ctx, "qlog/segments/seg_001.log", bytes.NewReader(segment)); err != nil {
		t.Fatal(err)
	}
	if err := bucket.Upload(ctx, "qlog/manifest.json", bytes.NewReader(manifestData)); err != nil {
		t.Fatal(err)
	}

	wal, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	tip, err := NewRecovery(wal, NewManifest(), bucket, "").Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := wal.Read()
	if err != nil {
		t.Fatal(err)
	}
	if tip != 2 || len(entries) != 2 || entries[0].Slot != 1 || entries[1].Slot != 2 {
		t.Fatalf("tip=%d entries=%#v", tip, entries)
	}
}

func TestRecoveryExtendsMatchingLocalSegment(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	remote, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for slot := uint64(1); slot <= 2; slot++ {
		if err := remote.Append(Entry{Slot: slot, Type: EntryDecide, Payload: []byte{byte(slot)}}); err != nil {
			t.Fatal(err)
		}
	}
	syncer := NewObjStoreSyncer(remote, NewManifest(), bucket, "", time.Second)
	if err := syncer.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	_ = remote.Close()

	local, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	if err := local.Append(Entry{Slot: 1, Type: EntryDecide, Payload: []byte{1}}); err != nil {
		t.Fatal(err)
	}
	tip, err := NewRecovery(local, NewManifest(), bucket, "").Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := local.Read()
	if err != nil {
		t.Fatal(err)
	}
	if tip != 2 || len(entries) != 2 {
		t.Fatalf("tip=%d entries=%#v", tip, entries)
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
	lock.Release()

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
