package qlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// Segment is a single WAL segment file.
type Segment struct {
	file   *os.File
	index  uint32
	offset int64
	mu     sync.Mutex
}

// WAL is the local write-ahead log for QLog entries.
// It stores proposals, receipts, and decisions for fast recovery.
type WAL struct {
	dir        string
	segments   []*Segment
	current    *Segment
	mu         sync.RWMutex
	compactMu  sync.Mutex
	generation uint64
	maxSize    int64
	maxBytes   int64
	totalBytes int64
	dirty      bool
	syncDir    func(string) error
}

var ErrCapacity = errors.New("WAL capacity reached")

// Bytes returns the current on-disk segment bytes.
func (w *WAL) Bytes() int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.bytesLocked()
}

func (w *WAL) bytesLocked() int64 {
	return w.totalBytes
}

// SetMaxBytes rejects future appends before the WAL exceeds max. Existing
// data above the configured ceiling fails startup instead of filling disk.
func (w *WAL) SetMaxBytes(max int64) error {
	if max <= 0 {
		return fmt.Errorf("WAL capacity must be positive")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if used := w.bytesLocked(); used > max {
		return fmt.Errorf("%w: used=%d max=%d", ErrCapacity, used, max)
	}
	w.maxBytes = max
	return nil
}

const defaultMaxSize = 64 * 1024 * 1024 // 64MB per segment

// Open opens or creates a WAL in the given directory.
func Open(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("create WAL dir: %w", err)
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return nil, fmt.Errorf("secure WAL dir: %w", err)
	}

	w := &WAL{
		dir:     dir,
		maxSize: defaultMaxSize,
		syncDir: syncDir,
	}

	if err := w.loadSegments(); err != nil {
		return nil, fmt.Errorf("load segments: %w", err)
	}

	return w, nil
}

// loadSegments loads existing segments from the directory.
func (w *WAL) loadSegments() error {
	manifests, err := filepath.Glob(filepath.Join(w.dir, "manifest_*.bin"))
	if err != nil {
		return err
	}
	if len(manifests) == 0 {
		legacy, globErr := filepath.Glob(filepath.Join(w.dir, "seg_*.log"))
		if globErr != nil {
			return globErr
		}
		if len(legacy) != 0 {
			return fmt.Errorf("unsupported WAL layout without manifest")
		}
		return w.createSegment(1)
	}
	sort.Strings(manifests)
	latest := manifests[len(manifests)-1]
	data, err := os.ReadFile(latest)
	if err != nil {
		return err
	}
	generation, refs, err := decodeManifest(data)
	if err != nil {
		return fmt.Errorf("decode %s: %w", filepath.Base(latest), err)
	}
	var filenameGeneration uint64
	if _, err := fmt.Sscanf(filepath.Base(latest), "manifest_%d.bin", &filenameGeneration); err != nil || filenameGeneration != generation {
		return fmt.Errorf("WAL manifest generation mismatch")
	}
	for i, ref := range refs {
		path := w.segmentPath(ref.index)
		f, err := os.OpenFile(path, os.O_RDWR, 0600)
		if err != nil {
			w.closeSegments()
			return fmt.Errorf("open manifest segment %d: %w", ref.index, err)
		}
		if err := f.Chmod(0600); err != nil {
			f.Close()
			w.closeSegments()
			return err
		}

		info, err := f.Stat()
		if err != nil {
			f.Close()
			w.closeSegments()
			return err
		}

		if !ref.active && uint64(info.Size()) != ref.length {
			f.Close()
			w.closeSegments()
			return fmt.Errorf("sealed WAL segment %d length=%d, want %d", ref.index, info.Size(), ref.length)
		}
		seg := &Segment{file: f, index: ref.index, offset: info.Size()}
		if _, err := seg.scan(i == len(refs)-1); err != nil {
			f.Close()
			w.closeSegments()
			return fmt.Errorf("scan segment %d: %w", ref.index, err)
		}
		w.segments = append(w.segments, seg)
		w.current = seg
		w.totalBytes += seg.offset
	}

	w.generation = generation
	return nil
}

func (w *WAL) closeSegments() {
	for _, seg := range w.segments {
		_ = seg.file.Close()
	}
	w.segments = nil
	w.current = nil
}

// createSegment creates a new segment file.
func (w *WAL) createSegment(index uint32) error {
	if w.current != nil {
		w.current.mu.Lock()
		err := w.current.file.Sync()
		w.current.mu.Unlock()
		if err != nil {
			return err
		}
		w.dirty = false
	}
	path := filepath.Join(w.dir, fmt.Sprintf("seg_%03d.log", index))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	if err := w.syncDir(w.dir); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return fmt.Errorf("sync WAL directory after creating segment %d: %w", index, err)
	}

	seg := &Segment{
		file:  f,
		index: index,
	}
	w.segments = append(w.segments, seg)
	w.current = seg
	if err := w.publishManifestLocked(w.segments); err != nil {
		w.segments = w.segments[:len(w.segments)-1]
		if len(w.segments) == 0 {
			w.current = nil
		} else {
			w.current = w.segments[len(w.segments)-1]
		}
		_ = f.Close()
		_ = os.Remove(path)
		return err
	}
	return nil
}

func (w *WAL) segmentPath(index uint32) string {
	return filepath.Join(w.dir, fmt.Sprintf("seg_%03d.log", index))
}

func (w *WAL) publishManifestLocked(segments []*Segment) error {
	refs := make([]manifestRef, len(segments))
	for i, seg := range segments {
		refs[i] = manifestRef{index: seg.index, length: uint64(seg.offset), active: i == len(segments)-1}
		if refs[i].active {
			refs[i].length = 0
		}
	}
	generation := w.generation + 1
	data, err := encodeManifest(generation, refs)
	if err != nil {
		return err
	}
	target := filepath.Join(w.dir, fmt.Sprintf("manifest_%020d.bin", generation))
	if err := writeFileAtomically(target, data); err != nil {
		return fmt.Errorf("publish WAL manifest: %w", err)
	}
	w.generation = generation
	return nil
}

// Append appends an entry to the WAL.
func (w *WAL) Append(entry Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data := entry.Encode()
	if w.maxBytes > 0 && int64(len(data)) > w.maxBytes-w.bytesLocked() {
		return fmt.Errorf("%w: used=%d append=%d max=%d", ErrCapacity, w.bytesLocked(), len(data), w.maxBytes)
	}

	// Roll over if needed
	if w.current != nil && w.current.offset+int64(len(data)) > w.maxSize {
		if err := w.createSegment(w.current.index + 1); err != nil {
			return err
		}
	}

	if w.current == nil {
		if err := w.createSegment(1); err != nil {
			return err
		}
	}

	w.current.mu.Lock()
	defer w.current.mu.Unlock()

	n, err := w.current.file.WriteAt(data, w.current.offset)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	w.current.offset += int64(n)
	w.totalBytes += int64(n)
	w.dirty = true

	return nil
}

// Sync flushes the current segment. Older segments are immutable and synced
// before rollover.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.current == nil || !w.dirty {
		return nil
	}
	w.current.mu.Lock()
	defer w.current.mu.Unlock()
	if err := w.current.file.Sync(); err != nil {
		return err
	}
	w.dirty = false
	return nil
}

// Read reads all entries from all segments.
func (w *WAL) Read() ([]Entry, error) {
	var entries []Entry
	err := w.Scan(func(entry Entry) error {
		entries = append(entries, entry)
		return nil
	})
	return entries, err
}

// Scan streams validated entries in segment order without retaining the WAL
// tail in a second aggregate slice.
func (w *WAL) Scan(visit func(Entry) error) error {
	if visit == nil {
		return fmt.Errorf("WAL scan callback is required")
	}
	w.mu.RLock()
	defer w.mu.RUnlock()
	for _, seg := range w.segments {
		seg.mu.Lock()
		err := seg.scanEntries(false, visit)
		seg.mu.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

// readAll reads all entries from a segment.
func (s *Segment) readAll() ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.scan(false)
}

// scan validates every entry. Only the active segment may repair a torn final
// write; immutable older segments fail closed on any truncation or corruption.
func (s *Segment) scan(repairTail bool) ([]Entry, error) {
	var entries []Entry
	err := s.scanEntries(repairTail, func(entry Entry) error {
		entries = append(entries, entry)
		return nil
	})
	return entries, err
}

func (s *Segment) scanEntries(repairTail bool, visit func(Entry) error) error {
	info, err := s.file.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	var offset int64
	memory := newReadArena()
	defer memory.free()
	header := memory.bytes(49)

	for offset < size {
		remaining := size - offset
		if remaining < int64(len(header)) {
			if repairTail {
				if err := s.file.Truncate(offset); err != nil {
					return err
				}
				s.offset = offset
				return nil
			}
			return io.ErrUnexpectedEOF
		}
		if _, err := s.file.ReadAt(header, offset); err != nil {
			return err
		}
		payloadLen, _ := entryPayloadLength(binary.LittleEndian.Uint32(header[41:45]))
		totalLen := int64(len(header)) + int64(payloadLen)
		if totalLen > remaining {
			if repairTail {
				if err := s.file.Truncate(offset); err != nil {
					return err
				}
				s.offset = offset
				return nil
			}
			return io.ErrUnexpectedEOF
		}
		buf := memory.bytes(int(totalLen))
		copy(buf, header)
		if payloadLen > 0 {
			if _, err := s.file.ReadAt(buf[len(header):], offset+int64(len(header))); err != nil {
				return err
			}
		}
		entry, _, err := DecodeEntry(buf)
		if err != nil {
			return err
		}
		if err := visit(entry); err != nil {
			return err
		}
		offset += totalLen
	}
	s.offset = offset
	return nil
}

// RestoreSegment atomically installs an exact WAL segment downloaded from
// object storage. Existing non-empty segments must match byte-for-byte.
func (w *WAL) RestoreSegment(index uint32, data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("empty segment %d", index)
	}
	if err := validateSegmentBytes(data); err != nil {
		return fmt.Errorf("invalid segment %d: %w", index, err)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	for _, seg := range w.segments {
		if seg.index != index {
			continue
		}
		seg.mu.Lock()
		existing := make([]byte, seg.offset)
		_, err := seg.file.ReadAt(existing, 0)
		if err == io.EOF && len(existing) == 0 {
			err = nil
		}
		seg.mu.Unlock()
		if err != nil {
			return err
		}
		if bytes.Equal(existing, data) {
			return nil
		}
		if seg != w.current {
			return fmt.Errorf("sealed segment %d conflicts with local WAL", index)
		}
		if len(existing) != 0 && !bytes.HasPrefix(data, existing) {
			return fmt.Errorf("segment %d conflicts with local WAL", index)
		}
		if w.maxBytes > 0 && int64(len(data)-len(existing)) > w.maxBytes-w.bytesLocked() {
			return fmt.Errorf("%w: restore segment %d", ErrCapacity, index)
		}
		return w.replaceEmptySegment(seg, data)
	}

	if w.current != nil && index <= w.current.index {
		return fmt.Errorf("restored segment %d is not after active segment %d", index, w.current.index)
	}
	if w.maxBytes > 0 && int64(len(data)) > w.maxBytes-w.bytesLocked() {
		return fmt.Errorf("%w: restore segment %d", ErrCapacity, index)
	}
	path := filepath.Join(w.dir, fmt.Sprintf("seg_%03d.log", index))
	if err := writeSegmentAtomically(path, data); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	seg := &Segment{file: f, index: index, offset: int64(len(data))}
	w.segments = append(w.segments, seg)
	w.totalBytes += int64(len(data))
	w.current = seg
	if err := w.publishManifestLocked(w.segments); err != nil {
		w.segments = w.segments[:len(w.segments)-1]
		w.current = w.segments[len(w.segments)-1]
		w.totalBytes -= int64(len(data))
		_ = f.Close()
		_ = os.Remove(path)
		return err
	}
	return nil
}

func (w *WAL) replaceEmptySegment(seg *Segment, data []byte) error {
	seg.mu.Lock()
	defer seg.mu.Unlock()
	path := filepath.Join(w.dir, fmt.Sprintf("seg_%03d.log", seg.index))
	if err := writeSegmentAtomically(path, data); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	if err := seg.file.Close(); err != nil {
		f.Close()
		return err
	}
	w.totalBytes += int64(len(data)) - seg.offset
	seg.file = f
	seg.offset = int64(len(data))
	return nil
}

func writeSegmentAtomically(path string, data []byte) error {
	return writeFileAtomically(path, data)
}

func writeFileAtomically(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".rhiza-segment-*")
	if err != nil {
		return err
	}
	temp := f.Name()
	defer os.Remove(temp)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err := os.Rename(temp, path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func validateSegmentBytes(data []byte) error {
	for offset := 0; offset < len(data); {
		_, used, err := DecodeEntry(data[offset:])
		if err != nil {
			return err
		}
		offset += used
	}
	return nil
}

// Compaction is a fenced WAL rewrite. Entries appended after BeginCompaction
// stay in independent tail segments and are never copied by the rewrite.
type Compaction struct {
	wal             *WAL
	base            Entry
	keepValueHashes map[[32]byte]struct{}
	prefix          []*Segment
	tailIndex       uint32
	targetIndex     uint32
	targetPath      string
	maxBytes        int64
	offset          int64
	built           bool
	done            bool
}

// BeginCompaction seals the current prefix and switches appends to a durable
// tail generation. The caller may release higher-level locks after it returns.
func (w *WAL) BeginCompaction(base Entry, keepValueHashes map[[32]byte]struct{}) (*Compaction, error) {
	if base.Type != EntryCheckpoint || base.Slot == 0 {
		return nil, fmt.Errorf("invalid WAL compaction base")
	}
	w.compactMu.Lock()
	w.mu.Lock()
	if w.current == nil {
		w.mu.Unlock()
		w.compactMu.Unlock()
		return nil, fmt.Errorf("WAL has no active segment")
	}
	if w.current.index > ^uint32(0)-2 {
		w.mu.Unlock()
		w.compactMu.Unlock()
		return nil, fmt.Errorf("WAL segment index exhausted")
	}
	prefix := append([]*Segment(nil), w.segments...)
	targetIndex := w.current.index + 1
	tailIndex := targetIndex + 1
	if err := w.createSegment(tailIndex); err != nil {
		w.mu.Unlock()
		w.compactMu.Unlock()
		return nil, err
	}
	w.mu.Unlock()
	keep := make(map[[32]byte]struct{}, len(keepValueHashes))
	for hash := range keepValueHashes {
		keep[hash] = struct{}{}
	}
	return &Compaction{wal: w, base: base, keepValueHashes: keep, prefix: prefix, tailIndex: tailIndex, targetIndex: targetIndex, targetPath: w.segmentPath(targetIndex), maxBytes: w.maxBytes}, nil
}

// Build writes and syncs the compacted prefix without blocking appends.
func (c *Compaction) Build() error {
	if c == nil || c.done || c.built {
		return fmt.Errorf("invalid WAL compaction state")
	}
	w := c.wal
	temp, err := os.CreateTemp(w.dir, ".rhiza-compact-*")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	offset := int64(0)
	writeEntry := func(entry Entry) error {
		data := entry.Encode()
		if c.maxBytes > 0 && offset+int64(len(data)) > c.maxBytes {
			return fmt.Errorf("%w: compacted WAL requires more than %d bytes", ErrCapacity, c.maxBytes)
		}
		n, writeErr := temp.Write(data)
		if writeErr != nil {
			return writeErr
		}
		if n != len(data) {
			return io.ErrShortWrite
		}
		offset += int64(n)
		return nil
	}
	if err := writeEntry(c.base); err != nil {
		temp.Close()
		return err
	}
	for _, seg := range c.prefix {
		seg.mu.Lock()
		err := seg.scanEntries(false, func(entry Entry) error {
			if entry.Type == EntryProposal {
				if _, ok := c.keepValueHashes[entry.Hash]; !ok {
					return nil
				}
			} else if entry.Slot <= c.base.Slot {
				return nil
			}
			return writeEntry(entry)
		})
		seg.mu.Unlock()
		if err != nil {
			temp.Close()
			return err
		}
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, c.targetPath); err != nil {
		return err
	}
	if err := syncDir(w.dir); err != nil {
		return err
	}
	c.offset = offset
	c.built = true
	return nil
}

// Commit atomically switches the manifest from the old prefix to the built
// prefix plus every post-fence tail segment.
func (c *Compaction) Commit() error {
	if c == nil || c.done || !c.built {
		return fmt.Errorf("invalid WAL compaction state")
	}
	w := c.wal
	file, err := os.OpenFile(c.targetPath, os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	compacted := &Segment{file: file, index: c.targetIndex, offset: c.offset}
	w.mu.Lock()
	if w.current == nil {
		w.mu.Unlock()
		file.Close()
		return fmt.Errorf("WAL has no active segment")
	}
	w.current.mu.Lock()
	if err := w.current.file.Sync(); err != nil {
		w.current.mu.Unlock()
		w.mu.Unlock()
		file.Close()
		return err
	}
	w.current.mu.Unlock()
	w.dirty = false
	tail := make([]*Segment, 0, len(w.segments))
	for _, seg := range w.segments {
		if seg.index >= c.tailIndex {
			tail = append(tail, seg)
		}
	}
	if len(tail) == 0 || tail[len(tail)-1] != w.current {
		w.mu.Unlock()
		file.Close()
		return fmt.Errorf("WAL compaction tail changed")
	}
	next := append([]*Segment{compacted}, tail...)
	var nextBytes int64
	for _, seg := range next {
		nextBytes += seg.offset
	}
	if w.maxBytes > 0 && nextBytes > w.maxBytes {
		w.mu.Unlock()
		file.Close()
		return fmt.Errorf("%w: compacted WAL requires %d bytes", ErrCapacity, nextBytes)
	}
	if err := w.publishManifestLocked(next); err != nil {
		w.mu.Unlock()
		file.Close()
		return err
	}
	old := c.prefix
	w.segments = next
	w.current = next[len(next)-1]
	w.totalBytes = nextBytes
	w.dirty = false
	w.mu.Unlock()
	c.done = true
	w.compactMu.Unlock()
	for _, seg := range old {
		seg.mu.Lock()
		_ = seg.file.Close()
		seg.mu.Unlock()
	}
	for _, seg := range old {
		_ = os.Remove(w.segmentPath(seg.index))
	}
	_ = syncDir(w.dir)
	return nil
}

// Abort discards an uncommitted compacted prefix. The fenced tail remains the
// active WAL generation and contains every concurrent append.
func (c *Compaction) Abort() {
	if c == nil || c.done {
		return
	}
	c.done = true
	_ = os.Remove(c.targetPath)
	c.wal.compactMu.Unlock()
}

// Compact preserves the simple API for restore paths.
func (w *WAL) Compact(base Entry, keepValueHashes map[[32]byte]struct{}) error {
	compaction, err := w.BeginCompaction(base, keepValueHashes)
	if err != nil {
		return err
	}
	defer compaction.Abort()
	if err := compaction.Build(); err != nil {
		return err
	}
	return compaction.Commit()
}

func syncDir(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

// Close closes all segment files.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var err error
	for _, seg := range w.segments {
		seg.mu.Lock()
		err = errors.Join(err, seg.file.Sync(), seg.file.Close())
		seg.mu.Unlock()
	}
	return err
}
