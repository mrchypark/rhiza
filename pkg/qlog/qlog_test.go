package qlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/thanos-io/objstore"
)

type failManifestBucket struct {
	objstore.Bucket
	fail bool
}

type countingBucket struct {
	objstore.Bucket
	uploads map[string]int
}

func (b *countingBucket) Upload(ctx context.Context, name string, r io.Reader, options ...objstore.ObjectUploadOption) error {
	b.uploads[name]++
	return b.Bucket.Upload(ctx, name, r, options...)
}

func (b *failManifestBucket) Upload(ctx context.Context, name string, r io.Reader, options ...objstore.ObjectUploadOption) error {
	if b.fail && (strings.Contains(name, "qlog/manifests/") || strings.HasSuffix(name, "manifest.json")) {
		b.fail = false
		return errors.New("manifest unavailable")
	}
	return b.Bucket.Upload(ctx, name, r, options...)
}

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

func TestSegmentSnapshotsSinceReadsOnlyDelta(t *testing.T) {
	wal, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	first := Entry{Slot: 1, Type: EntryDecide, Payload: []byte("one")}
	if err := wal.Append(first); err != nil {
		t.Fatal(err)
	}
	snapshots, err := wal.SegmentSnapshots()
	if err != nil {
		t.Fatal(err)
	}
	known := map[uint32]SegmentMeta{snapshots[0].Meta.Index: snapshots[0].Meta}
	second := Entry{Slot: 2, Type: EntryDecide, Payload: []byte("two")}
	if err := wal.Append(second); err != nil {
		t.Fatal(err)
	}
	delta, err := wal.SegmentSnapshotsSince(known)
	if err != nil {
		t.Fatal(err)
	}
	if len(delta) != 1 || delta[0].Offset != int64(len(first.Encode())) || !bytes.Equal(delta[0].Data, second.Encode()) {
		t.Fatalf("unexpected delta: %+v", delta)
	}
}

func TestRecoveryRejectsMutableObjectStoreManifest(t *testing.T) {
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
	if _, err := NewRecovery(wal, NewManifest(), bucket, "").Recover(ctx); err == nil || !strings.Contains(err.Error(), "unsupported object-store QLog layout") {
		t.Fatalf("expected mutable manifest rejection, got %v", err)
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

func TestSyncThroughRetriesFailedManifestPublication(t *testing.T) {
	ctx := context.Background()
	bucket := &failManifestBucket{Bucket: objstore.NewInMemBucket()}
	wal, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	if err := wal.Append(Entry{Slot: 1, Type: EntryDecide, Payload: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	syncer := NewObjStoreSyncer(wal, NewManifest(), bucket, "", 0)
	if err := syncer.SyncThrough(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if err := wal.Append(Entry{Slot: 2, Type: EntryDecide, Payload: []byte("two")}); err != nil {
		t.Fatal(err)
	}
	bucket.fail = true
	if err := syncer.SyncThrough(ctx, 2); err == nil {
		t.Fatal("expected manifest publication failure")
	}
	old, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if tip, err := NewRecovery(old, NewManifest(), bucket, "").Recover(ctx); err != nil || tip != 1 {
		t.Fatalf("previous published prefix tip=%d err=%v", tip, err)
	}
	_ = old.Close()

	if err := syncer.SyncThrough(ctx, 2); err != nil {
		t.Fatalf("retry failed: %v", err)
	}

	restored, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if tip, err := NewRecovery(restored, NewManifest(), bucket, "").Recover(ctx); err != nil || tip != 2 {
		t.Fatalf("recovery tip=%d err=%v", tip, err)
	}
}

func TestExtentSyncUploadsOnlyAppendedBytesAndCollectsOldManifest(t *testing.T) {
	ctx := context.Background()
	memory := objstore.NewInMemBucket()
	bucket := &countingBucket{Bucket: memory, uploads: make(map[string]int)}
	wal, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	manifest := NewManifest()
	syncer := NewObjStoreSyncer(wal, manifest, bucket, "", 0)
	syncer.chunkSize = 128
	if err := wal.Append(Entry{Slot: 1, Type: EntryDecide, Payload: bytes.Repeat([]byte("a"), 200)}); err != nil {
		t.Fatal(err)
	}
	if err := syncer.SyncThrough(ctx, 1); err != nil {
		t.Fatal(err)
	}
	firstSegments, _, _ := manifest.Snapshot()
	if len(firstSegments) != 1 || firstSegments[0].ExtentCount != 2 {
		t.Fatalf("first segments=%+v", firstSegments)
	}
	firstHead, err := syncer.downloadExtent(ctx, firstSegments[0].ExtentHead)
	if err != nil {
		t.Fatal(err)
	}
	stableKey := extentObjectKey(firstHead.Previous)
	var firstManifest string
	if err := memory.Iter(ctx, "qlog/manifests", func(name string) error { firstManifest = name; return nil }); err != nil {
		t.Fatal(err)
	}

	if err := wal.Append(Entry{Slot: 2, Type: EntryDecide, Payload: bytes.Repeat([]byte("b"), 50)}); err != nil {
		t.Fatal(err)
	}
	if err := syncer.SyncThrough(ctx, 2); err != nil {
		t.Fatal(err)
	}
	if bucket.uploads[stableKey] != 1 {
		t.Fatalf("stable extent uploads=%d, want 1", bucket.uploads[stableKey])
	}
	extentUploads := 0
	for name, count := range bucket.uploads {
		if strings.Contains(name, "qlog/extents/") {
			extentUploads += count
		}
	}
	if extentUploads != 3 {
		t.Fatalf("extent uploads=%d, want two initial plus one append", extentUploads)
	}
	if err := syncer.GarbageCollect(ctx, time.Hour); err != nil {
		t.Fatal(err)
	}
	if exists, _ := memory.Exists(ctx, firstManifest); !exists {
		t.Fatal("GC removed an old manifest inside the grace period")
	}
	mark := syncer.gcMarkKey(firstManifest)
	if err := memory.ChangeLastModified(mark, time.Now().Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := syncer.GarbageCollect(ctx, time.Hour); err != nil {
		t.Fatal(err)
	}
	if exists, _ := memory.Exists(ctx, firstManifest); exists {
		t.Fatal("expired manifest was not deleted")
	}
	if exists, err := memory.Exists(ctx, stableKey); err != nil || !exists {
		t.Fatalf("referenced extent was deleted: exists=%v err=%v", exists, err)
	}

	restored, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if tip, err := NewRecovery(restored, NewManifest(), bucket, "").Recover(ctx); err != nil || tip != 2 {
		t.Fatalf("extent recovery tip=%d err=%v", tip, err)
	}
}

func TestImmutableManifestGenerationCannotRollBack(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	wal, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	syncer := NewObjStoreSyncer(wal, NewManifest(), bucket, "", 0)
	if err := wal.Append(Entry{Slot: 1, Type: EntryDecide, Payload: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	if err := syncer.SyncThrough(ctx, 1); err != nil {
		t.Fatal(err)
	}
	var oldKey string
	if err := bucket.Iter(ctx, "qlog/manifests", func(name string) error { oldKey = name; return nil }); err != nil {
		t.Fatal(err)
	}
	oldReader, err := bucket.Get(ctx, oldKey)
	if err != nil {
		t.Fatal(err)
	}
	oldData, err := io.ReadAll(oldReader)
	oldReader.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err := wal.Append(Entry{Slot: 2, Type: EntryDecide, Payload: []byte("two")}); err != nil {
		t.Fatal(err)
	}
	if err := syncer.SyncThrough(ctx, 2); err != nil {
		t.Fatal(err)
	}
	// A delayed retry of an older publication can only overwrite its immutable key.
	if err := bucket.Upload(ctx, oldKey, bytes.NewReader(oldData)); err != nil {
		t.Fatal(err)
	}
	restored, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if tip, err := NewRecovery(restored, NewManifest(), bucket, "").Recover(ctx); err != nil || tip != 2 {
		t.Fatalf("recovery rolled back: tip=%d err=%v", tip, err)
	}
}

func TestExtentRecoveryRejectsCorruptionAndNumberedManifest(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	wal, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	syncManifest := NewManifest()
	syncer := NewObjStoreSyncer(wal, syncManifest, bucket, "", 0)
	if err := wal.Append(Entry{Slot: 1, Type: EntryDecide, Payload: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	if err := syncer.SyncThrough(ctx, 1); err != nil {
		t.Fatal(err)
	}
	segments, _, _ := syncManifest.Snapshot()
	key := extentObjectKey(segments[0].ExtentHead)
	r, err := bucket.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(r)
	r.Close()
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff
	if err := bucket.Upload(ctx, key, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	corruptWAL, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer corruptWAL.Close()
	if _, err := NewRecovery(corruptWAL, NewManifest(), bucket, "").Recover(ctx); err == nil || !strings.Contains(err.Error(), "extent hash mismatch") {
		t.Fatalf("expected extent integrity error, got %v", err)
	}

	versioned := struct {
		Version    int    `json:"version"`
		Generation uint64 `json:"generation"`
		TipSlot    uint64 `json:"tip_slot"`
	}{Version: 2, Generation: 1, TipSlot: 1}
	unknownData, err := json.Marshal(versioned)
	if err != nil {
		t.Fatal(err)
	}
	unknownHash := sha256.Sum256(unknownData)
	unknownKey := fmt.Sprintf("qlog/manifests/%020d_%x.json", versioned.Generation, unknownHash)
	unknownBucket := objstore.NewInMemBucket()
	if err := unknownBucket.Upload(ctx, unknownKey, bytes.NewReader(unknownData)); err != nil {
		t.Fatal(err)
	}
	unknownWAL, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer unknownWAL.Close()
	if _, err := NewRecovery(unknownWAL, NewManifest(), unknownBucket, "").Recover(ctx); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("expected numbered manifest rejection, got %v", err)
	}
}

func TestManifestGenerationForkFailsClosed(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	for _, tip := range []uint64{1, 2} {
		manifest := &Manifest{Generation: 7, TipSlot: tip}
		data, err := json.Marshal(manifest)
		if err != nil {
			t.Fatal(err)
		}
		hash := sha256.Sum256(data)
		key := fmt.Sprintf("qlog/manifests/%020d_%x.json", manifest.Generation, hash)
		if err := bucket.Upload(ctx, key, bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
	}
	wal, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	err = NewObjStoreSyncer(wal, NewManifest(), bucket, "", 0).LoadManifest(ctx)
	if err == nil || !strings.Contains(err.Error(), "conflicting object-store manifests") {
		t.Fatalf("expected manifest fork error, got %v", err)
	}
}

func TestBackgroundSyncCompactsLongExtentChain(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	wal, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	manifest := NewManifest()
	syncer := NewObjStoreSyncer(wal, manifest, bucket, "", 0)
	for slot := uint64(1); slot <= maxExtentChain+1; slot++ {
		if err := wal.Append(Entry{Slot: slot, Type: EntryDecide, Payload: []byte{byte(slot)}}); err != nil {
			t.Fatal(err)
		}
		if err := syncer.SyncThrough(ctx, slot); err != nil {
			t.Fatal(err)
		}
	}
	before, _, _ := manifest.Snapshot()
	if before[0].ExtentCount <= maxExtentChain {
		t.Fatalf("extent chain did not grow: %d", before[0].ExtentCount)
	}
	if err := syncer.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	after, _, _ := manifest.Snapshot()
	if after[0].ExtentCount >= before[0].ExtentCount {
		t.Fatalf("extent chain was not compacted: before=%d after=%d", before[0].ExtentCount, after[0].ExtentCount)
	}
	restored, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if tip, err := NewRecovery(restored, NewManifest(), bucket, "").Recover(ctx); err != nil || tip != maxExtentChain+1 {
		t.Fatalf("compacted recovery tip=%d err=%v", tip, err)
	}
}

func TestSyncRetriesDirtyManifestWhenTipIsUnchanged(t *testing.T) {
	ctx := context.Background()
	bucket := &failManifestBucket{Bucket: objstore.NewInMemBucket()}
	wal, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	syncer := NewObjStoreSyncer(wal, NewManifest(), bucket, "", 0)
	if err := wal.Append(Entry{Slot: 1, Type: EntryDecide, Payload: []byte("decision")}); err != nil {
		t.Fatal(err)
	}
	if err := syncer.SyncThrough(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if err := wal.Append(Entry{Slot: 1, Type: EntryReceipt, Payload: []byte("receipt")}); err != nil {
		t.Fatal(err)
	}
	bucket.fail = true
	if err := syncer.SyncThrough(ctx, 1); err == nil {
		t.Fatal("expected manifest publication failure")
	}
	if err := syncer.SyncThrough(ctx, 1); err != nil {
		t.Fatal(err)
	}
	restored, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if _, err := NewRecovery(restored, NewManifest(), bucket, "").Recover(ctx); err != nil {
		t.Fatal(err)
	}
	entries, err := restored.Read()
	if err != nil || len(entries) != 2 {
		t.Fatalf("restored entries=%d err=%v", len(entries), err)
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
