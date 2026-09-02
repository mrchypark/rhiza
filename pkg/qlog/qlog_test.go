package qlog

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestWALCapacityFailsClosedAndScanStreams(t *testing.T) {
	wal, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	entry := Entry{Slot: 1, Type: EntryProposal, Payload: []byte("value")}
	if err := wal.SetMaxBytes(int64(len(entry.Encode()))); err != nil {
		t.Fatal(err)
	}
	if err := wal.Append(entry); err != nil {
		t.Fatal(err)
	}
	if err := wal.Append(Entry{Slot: 2, Type: EntryProposal, Payload: []byte("other")}); !errors.Is(err, ErrCapacity) {
		t.Fatalf("append error=%v, want capacity", err)
	}
	var slots []uint64
	if err := wal.Scan(func(entry Entry) error { slots = append(slots, entry.Slot); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(slots) != 1 || slots[0] != 1 {
		t.Fatalf("slots=%v", slots)
	}
}

func TestWALCloseReportsSegmentErrors(t *testing.T) {
	wal, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := wal.current.file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err == nil {
		t.Fatal("WAL close discarded segment errors")
	}
}

func TestWALRolloverRequiresDirectorySync(t *testing.T) {
	dir := t.TempDir()
	wal, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	wantErr := errors.New("directory sync failed")
	wal.maxSize = 1
	wal.syncDir = func(string) error { return wantErr }
	if err := wal.Append(Entry{Slot: 1, Type: EntryProposal, Payload: []byte("value")}); !errors.Is(err, wantErr) {
		t.Fatalf("append error=%v, want %v", err, wantErr)
	}
	if len(wal.segments) != 1 || wal.current.index != 1 {
		t.Fatalf("failed rollover changed active segments: count=%d current=%d", len(wal.segments), wal.current.index)
	}
	if _, err := os.Stat(filepath.Join(dir, "seg_002.log")); !os.IsNotExist(err) {
		t.Fatalf("failed rollover left segment behind: %v", err)
	}
}

func TestWALRolloverPostRenameSyncFailureRemainsReopenable(t *testing.T) {
	dir := t.TempDir()
	wal, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("manifest directory sync failed")
	calls := 0
	wal.maxSize = 1
	wal.syncDir = func(string) error {
		calls++
		if calls == 2 {
			return wantErr
		}
		return nil
	}
	if err := wal.Append(Entry{Slot: 1, Type: EntryProposal, Payload: []byte("value")}); !errors.Is(err, wantErr) {
		t.Fatalf("append error=%v, want %v", err, wantErr)
	}
	wal.syncDir = syncDir
	if err := wal.Append(Entry{Slot: 2, Type: EntryProposal, Payload: []byte("must-not-write")}); !errors.Is(err, wantErr) {
		t.Fatalf("poisoned WAL append error=%v, want %v", err, wantErr)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "manifest_00000000000000000002.bin")); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen after uncertain manifest publish: %v", err)
	}
	defer reopened.Close()
	if reopened.current == nil || reopened.current.index != 1 {
		t.Fatalf("reopened current=%v, want prior segment 1", reopened.current)
	}
	entries, err := reopened.Read()
	if err != nil || len(entries) != 0 {
		t.Fatalf("uncertain manifest exposed later writes: entries=%#v err=%v", entries, err)
	}
}

func TestWALUsesPrivatePermissions(t *testing.T) {
	dir := t.TempDir() + "/qlog"
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	segment := filepath.Join(dir, "seg_001.log")
	if err := os.Chmod(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(segment, 0644); err != nil {
		t.Fatal(err)
	}
	wal, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	for path, want := range map[string]os.FileMode{dir: 0700, segment: 0600} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != want {
			t.Fatalf("%s permissions=%o, want %o", path, got, want)
		}
	}
}

func TestWALSyncTracksDirtyWrites(t *testing.T) {
	wal, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	if wal.dirty {
		t.Fatal("new WAL is dirty")
	}
	if err := wal.Append(Entry{Slot: 1, Type: EntryProposal, Payload: []byte("value")}); err != nil {
		t.Fatal(err)
	}
	if !wal.dirty {
		t.Fatal("append did not mark WAL dirty")
	}
	if err := wal.Sync(); err != nil {
		t.Fatal(err)
	}
	if wal.dirty {
		t.Fatal("successful sync left WAL dirty")
	}
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
	large := Entry{Slot: 1, Type: EntryProposal, Payload: make([]byte, 2*1024*1024)}
	large.Payload[len(large.Payload)-1] = 1
	want := []Entry{large, {Slot: 2, Type: EntryReceipt, Payload: []byte("small")}}

	wal, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	for _, entry := range want {
		if err := wal.Append(entry); err != nil {
			t.Fatal(err)
		}
	}
	got, err := wal.Read()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("read %d entries, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Slot != want[i].Slot || got[i].Type != want[i].Type || !bytes.Equal(got[i].Payload, want[i].Payload) {
			t.Fatalf("entry %d did not round-trip: got=%#v want=%#v", i, got[i], want[i])
		}
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

func TestWALRejectsNoncanonicalManifestWithoutDeletingCommittedState(t *testing.T) {
	dir := t.TempDir()
	wal, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	firstManifest, err := os.ReadFile(filepath.Join(dir, "manifest_00000000000000000001.bin"))
	if err != nil {
		t.Fatal(err)
	}
	wal.maxSize = 1
	if err := wal.Append(Entry{Slot: 1, Type: EntryProposal, Payload: []byte("rollover")}); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	latest := filepath.Join(dir, "manifest_00000000000000000002.bin")
	segment := filepath.Join(dir, "seg_002.log")
	if err := os.WriteFile(filepath.Join(dir, "manifest_1.bin"), firstManifest, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil {
		t.Fatal("noncanonical manifest was accepted")
	}
	for _, name := range []string{latest, segment} {
		if _, err := os.Stat(name); err != nil {
			t.Fatalf("committed WAL file %s was removed: %v", filepath.Base(name), err)
		}
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

func TestWALRejectsManifestlessSegments(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "seg_001.log"), (Entry{Slot: 1, Type: EntryProposal}).Encode(), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil {
		t.Fatal("manifestless WAL layout was accepted")
	}
}

func TestOpenRecoversInitialSegmentCreatedBeforeManifest(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "seg_001.log"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	wal, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := wal.Append(Entry{Slot: 1, Type: EntryProposal, Payload: []byte("value")}); err != nil {
		t.Fatal(err)
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
	if err != nil || len(entries) != 1 || entries[0].Slot != 1 {
		t.Fatalf("entries=%#v err=%v", entries, err)
	}
}

func TestOpenRecoversInitialSegmentAlongsideNoncanonicalFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "seg_001.log"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	note := filepath.Join(dir, "seg_notes.log")
	if err := os.WriteFile(note, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	wal, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	if data, err := os.ReadFile(note); err != nil || string(data) != "keep" {
		t.Fatalf("noncanonical file data=%q err=%v", data, err)
	}
}

func TestOpenRemovesUnreferencedRolloverSegment(t *testing.T) {
	dir := t.TempDir()
	wal, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "seg_002.log"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopened.maxSize = 1
	if err := reopened.Append(Entry{Slot: 1, Type: EntryProposal, Payload: []byte("value")}); err != nil {
		t.Fatal(err)
	}
}

func TestOpenRemovesAbortedCompactionTarget(t *testing.T) {
	dir := t.TempDir()
	wal, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := wal.Append(Entry{Slot: 1, Type: EntryProposal, Payload: []byte("value")}); err != nil {
		t.Fatal(err)
	}
	plan, err := wal.BeginCompaction(Entry{Slot: 1, Type: EntryCheckpoint, Payload: []byte("base")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.Build(); err != nil {
		t.Fatal(err)
	}
	target := plan.targetPath
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("uncommitted compaction target remains: %v", err)
	}
	if err := reopened.Append(Entry{Slot: 2, Type: EntryProposal, Payload: []byte("after-restart")}); err != nil {
		t.Fatal(err)
	}
}

func TestManifestGenerationsRemainBounded(t *testing.T) {
	dir := t.TempDir()
	wal, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	wal.maxSize = 1
	for slot := uint64(1); slot <= 100; slot++ {
		if err := wal.Append(Entry{Slot: slot, Type: EntryProposal, Payload: []byte("value")}); err != nil {
			t.Fatal(err)
		}
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	manifests, err := filepath.Glob(filepath.Join(dir, "manifest_*.bin"))
	if err != nil || len(manifests) != 1 {
		t.Fatalf("manifests=%v err=%v", manifests, err)
	}
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	entries, err := reopened.Read()
	if err != nil || len(entries) != 100 {
		t.Fatalf("entries=%d err=%v", len(entries), err)
	}
}

func TestCommittedManifestCleanupFailureDoesNotFailAppend(t *testing.T) {
	dir := t.TempDir()
	wal, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	oldManifest := filepath.Join(dir, "manifest_00000000000000000001.bin")
	wantErr := errors.New("injected cleanup failure")
	wal.remove = func(name string) error {
		if name == oldManifest {
			return wantErr
		}
		return os.Remove(name)
	}
	wal.maxSize = 1
	if err := wal.Append(Entry{Slot: 1, Type: EntryProposal, Payload: []byte("value")}); err != nil {
		t.Fatalf("durable append reported cleanup failure: %v", err)
	}
	wal.remove = os.Remove
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(oldManifest); err != nil {
		t.Fatalf("old manifest was unexpectedly removed: %v", err)
	}
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	manifests, _ := filepath.Glob(filepath.Join(dir, "manifest_*.bin"))
	if len(manifests) != 1 {
		t.Fatalf("manifest cleanup was not retried: %v", manifests)
	}
}

func TestOpenCleansAtomicWriteTemps(t *testing.T) {
	dir := t.TempDir()
	wal, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{".rhiza-segment-crash", ".rhiza-compact-crash"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("partial"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for _, name := range []string{".rhiza-segment-crash", ".rhiza-compact-crash"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Fatalf("temp %s remains: %v", name, err)
		}
	}
}

func TestWALCompactionRetainsConcurrentTailExactlyOnce(t *testing.T) {
	dir := t.TempDir()
	wal, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for slot := uint64(1); slot <= 3; slot++ {
		if err := wal.Append(Entry{Slot: slot, Type: EntryProposal, Payload: []byte{byte(slot)}}); err != nil {
			t.Fatal(err)
		}
	}
	base := Entry{Slot: 1, Type: EntryCheckpoint, Hash: [32]byte{1}, Payload: []byte("base")}
	plan, err := wal.BeginCompaction(base, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Abort()
	if err := wal.Append(Entry{Slot: 4, Type: EntryReceipt, Payload: []byte("during-build")}); err != nil {
		t.Fatal(err)
	}
	if err := plan.Build(); err != nil {
		t.Fatal(err)
	}
	if err := wal.Append(Entry{Slot: 5, Type: EntryReceipt, Payload: []byte("before-install")}); err != nil {
		t.Fatal(err)
	}
	if err := plan.Commit(); err != nil {
		t.Fatal(err)
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
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 || entries[0].Type != EntryCheckpoint || entries[1].Slot != 4 || entries[2].Slot != 5 {
		t.Fatalf("compacted entries=%#v", entries)
	}
}

func TestWALCompactionPostRenameSyncFailureRemainsReopenable(t *testing.T) {
	dir := t.TempDir()
	wal, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := wal.Append(Entry{Slot: 1, Type: EntryProposal, Payload: []byte("old")}); err != nil {
		t.Fatal(err)
	}
	plan, err := wal.BeginCompaction(Entry{Slot: 1, Type: EntryCheckpoint, Hash: [32]byte{1}, Payload: []byte("base")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Abort()
	if err := plan.Build(); err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("manifest directory sync failed")
	wal.syncDir = func(string) error { return wantErr }
	if err := plan.Commit(); !errors.Is(err, wantErr) {
		t.Fatalf("commit error=%v, want %v", err, wantErr)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen after uncertain compaction manifest: %v", err)
	}
	defer reopened.Close()
	entries, err := reopened.Read()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Type != EntryCheckpoint || entries[0].Slot != 1 {
		t.Fatalf("reopened entries=%#v", entries)
	}
}

func TestWALCompactionSynthesizesRetainedProposalsOnce(t *testing.T) {
	wal, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	first, second := []byte("first"), []byte("second")
	firstHash, secondHash := sha256.Sum256(first), sha256.Sum256(second)
	for _, entry := range []Entry{
		{Hash: secondHash, Type: EntryProposal, Payload: second},
		{Hash: firstHash, Type: EntryProposal, Payload: first},
		{Hash: secondHash, Type: EntryProposal, Payload: second},
		{Slot: 3, Type: EntryReceipt, Payload: []byte("receipt")},
	} {
		if err := wal.Append(entry); err != nil {
			t.Fatal(err)
		}
	}
	plan, err := wal.BeginCompaction(Entry{Slot: 1, Type: EntryCheckpoint, Hash: [32]byte{1}, Payload: []byte("base")}, map[[32]byte][]byte{firstHash: first, secondHash: second})
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Abort()
	if err := plan.Build(); err != nil {
		t.Fatal(err)
	}
	if err := plan.Commit(); err != nil {
		t.Fatal(err)
	}
	entries, err := wal.Read()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 4 || entries[0].Type != EntryCheckpoint || entries[1].Type != EntryProposal || entries[2].Type != EntryProposal || entries[3].Type != EntryReceipt {
		t.Fatalf("compacted entries=%#v", entries)
	}
	if bytes.Compare(entries[1].Hash[:], entries[2].Hash[:]) >= 0 {
		t.Fatalf("retained proposals are not deterministic: %x then %x", entries[1].Hash, entries[2].Hash)
	}
	for _, entry := range entries[1:3] {
		if sha256.Sum256(entry.Payload) != entry.Hash {
			t.Fatalf("retained proposal hash mismatch: %x", entry.Hash)
		}
	}
}

func TestWALRecoveryIgnoresUncommittedCompactionSegment(t *testing.T) {
	dir := t.TempDir()
	wal, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := wal.Append(Entry{Slot: 1, Type: EntryProposal, Payload: []byte("old")}); err != nil {
		t.Fatal(err)
	}
	plan, err := wal.BeginCompaction(Entry{Slot: 1, Type: EntryCheckpoint, Hash: [32]byte{1}, Payload: []byte("base")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.Build(); err != nil {
		plan.Abort()
		t.Fatal(err)
	}
	if err := wal.Append(Entry{Slot: 2, Type: EntryReceipt, Payload: []byte("tail")}); err != nil {
		plan.Abort()
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		plan.Abort()
		t.Fatal(err)
	}
	reopened, err := Open(dir)
	if err != nil {
		plan.Abort()
		t.Fatal(err)
	}
	entries, err := reopened.Read()
	if err != nil {
		reopened.Close()
		plan.Abort()
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].Slot != 1 || entries[1].Slot != 2 {
		t.Fatalf("recovered uncommitted layout=%#v", entries)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	plan.Abort()
}

func TestWALFailsClosedOnCorruptHighestManifest(t *testing.T) {
	dir := t.TempDir()
	wal, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest_99999999999999999999.bin"), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil {
		t.Fatal("corrupt highest WAL manifest was ignored")
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
