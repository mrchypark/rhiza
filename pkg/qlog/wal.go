package qlog

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
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
	dir      string
	segments []*Segment
	current  *Segment
	mu       sync.RWMutex
	maxSize  int64
}

const defaultMaxSize = 64 * 1024 * 1024 // 64MB per segment

// SegmentSnapshot is an exact, validated WAL segment and its object-store metadata.
type SegmentSnapshot struct {
	Meta   SegmentMeta
	Offset int64
	Data   []byte
}

// Open opens or creates a WAL in the given directory.
func Open(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create WAL dir: %w", err)
	}

	w := &WAL{
		dir:     dir,
		maxSize: defaultMaxSize,
	}

	if err := w.loadSegments(); err != nil {
		return nil, fmt.Errorf("load segments: %w", err)
	}

	return w, nil
}

// loadSegments loads existing segments from the directory.
func (w *WAL) loadSegments() error {
	matches, err := filepath.Glob(filepath.Join(w.dir, "seg_*.log"))
	if err != nil {
		return err
	}

	type segmentPath struct {
		index uint32
		path  string
	}
	paths := make([]segmentPath, 0, len(matches))
	for _, path := range matches {
		var index uint32
		if _, err := fmt.Sscanf(filepath.Base(path), "seg_%d.log", &index); err != nil {
			continue
		}
		paths = append(paths, segmentPath{index: index, path: path})
	}
	sort.Slice(paths, func(i, j int) bool { return paths[i].index < paths[j].index })

	for i, item := range paths {
		f, err := os.OpenFile(item.path, os.O_RDWR, 0644)
		if err != nil {
			w.closeSegments()
			return err
		}

		info, err := f.Stat()
		if err != nil {
			f.Close()
			w.closeSegments()
			return err
		}

		seg := &Segment{
			file:   f,
			index:  item.index,
			offset: info.Size(),
		}
		if _, err := seg.scan(i == len(paths)-1); err != nil {
			f.Close()
			w.closeSegments()
			return fmt.Errorf("scan segment %d: %w", item.index, err)
		}
		w.segments = append(w.segments, seg)
		w.current = seg
	}

	if len(w.segments) == 0 {
		return w.createSegment(1)
	}

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
	}
	path := filepath.Join(w.dir, fmt.Sprintf("seg_%03d.log", index))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	seg := &Segment{
		file:  f,
		index: index,
	}
	w.segments = append(w.segments, seg)
	w.current = seg
	return nil
}

// Append appends an entry to the WAL.
func (w *WAL) Append(entry Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data := entry.Encode()

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

	return nil
}

// Sync flushes the current segment. Older segments are immutable and synced
// before rollover.
func (w *WAL) Sync() error {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.current == nil {
		return nil
	}
	w.current.mu.Lock()
	defer w.current.mu.Unlock()
	return w.current.file.Sync()
}

// Read reads all entries from all segments.
func (w *WAL) Read() ([]Entry, error) {
	w.mu.RLock()
	segments := make([]*Segment, len(w.segments))
	copy(segments, w.segments)
	w.mu.RUnlock()

	var entries []Entry
	for _, seg := range segments {
		segEntries, err := seg.readAll()
		if err != nil {
			return nil, err
		}
		entries = append(entries, segEntries...)
	}
	return entries, nil
}

// SegmentSnapshots returns consistent copies suitable for object-store upload.
func (w *WAL) SegmentSnapshots() ([]SegmentSnapshot, error) {
	return w.SegmentSnapshotsSince(nil)
}

// SegmentSnapshotsSince avoids rereading segments whose validated metadata is unchanged.
func (w *WAL) SegmentSnapshotsSince(validated map[uint32]SegmentMeta) ([]SegmentSnapshot, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	snapshots := make([]SegmentSnapshot, 0, len(w.segments))
	for _, seg := range w.segments {
		seg.mu.Lock()
		known, ok := validated[seg.index]
		if ok && known.Size > seg.offset {
			seg.mu.Unlock()
			return nil, fmt.Errorf("segment %d shrank from %d to %d", seg.index, known.Size, seg.offset)
		}
		if ok && known.Size == seg.offset {
			seg.mu.Unlock()
			snapshots = append(snapshots, SegmentSnapshot{Meta: known})
			continue
		}
		offset := int64(0)
		if ok {
			offset = known.Size
		}
		data := make([]byte, seg.offset-offset)
		_, err := seg.file.ReadAt(data, offset)
		seg.mu.Unlock()
		if err != nil && !(err == io.EOF && len(data) == 0) {
			return nil, err
		}
		if len(data) == 0 {
			continue
		}
		start, end := known.StartSlot, known.EndSlot
		for cursor := 0; cursor < len(data); {
			entry, used, err := DecodeEntry(data[cursor:])
			if err != nil {
				return nil, fmt.Errorf("segment %d: %w", seg.index, err)
			}
			if start == 0 || entry.Slot < start {
				start = entry.Slot
			}
			if entry.Slot > end {
				end = entry.Slot
			}
			cursor += used
		}
		meta := SegmentMeta{Index: seg.index, StartSlot: start, EndSlot: end, Size: offset + int64(len(data))}
		if offset == 0 {
			meta.Hash = sha256.Sum256(data)
		}
		snapshots = append(snapshots, SegmentSnapshot{Meta: meta, Offset: offset, Data: data})
	}
	return snapshots, nil
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
	info, err := s.file.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()
	var offset int64
	var entries []Entry
	memory := newReadArena()
	defer memory.free()
	header := memory.bytes(49)

	for offset < size {
		remaining := size - offset
		if remaining < int64(len(header)) {
			if repairTail {
				if err := s.file.Truncate(offset); err != nil {
					return nil, err
				}
				s.offset = offset
				return entries, nil
			}
			return nil, io.ErrUnexpectedEOF
		}
		if _, err := s.file.ReadAt(header, offset); err != nil {
			return nil, err
		}
		payloadLen := int64(binary.LittleEndian.Uint32(header[41:45]))
		totalLen := int64(len(header)) + payloadLen
		if totalLen > remaining {
			if repairTail {
				if err := s.file.Truncate(offset); err != nil {
					return nil, err
				}
				s.offset = offset
				return entries, nil
			}
			return nil, io.ErrUnexpectedEOF
		}
		buf := memory.bytes(int(totalLen))
		copy(buf, header)
		if payloadLen > 0 {
			if _, err := s.file.ReadAt(buf[len(header):], offset+int64(len(header))); err != nil {
				return nil, err
			}
		}
		entry, _, err := DecodeEntry(buf)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
		offset += totalLen
	}
	s.offset = offset
	return entries, nil
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
		if len(existing) != 0 && !bytes.HasPrefix(data, existing) {
			return fmt.Errorf("segment %d conflicts with local WAL", index)
		}
		return w.replaceEmptySegment(seg, data)
	}

	path := filepath.Join(w.dir, fmt.Sprintf("seg_%03d.log", index))
	if err := writeSegmentAtomically(path, data); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	seg := &Segment{file: f, index: index, offset: int64(len(data))}
	w.segments = append(w.segments, seg)
	sort.Slice(w.segments, func(i, j int) bool { return w.segments[i].index < w.segments[j].index })
	w.current = w.segments[len(w.segments)-1]
	return nil
}

func (w *WAL) replaceEmptySegment(seg *Segment, data []byte) error {
	seg.mu.Lock()
	defer seg.mu.Unlock()
	path := filepath.Join(w.dir, fmt.Sprintf("seg_%03d.log", seg.index))
	if err := seg.file.Close(); err != nil {
		return err
	}
	if err := writeSegmentAtomically(path, data); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	seg.file = f
	seg.offset = int64(len(data))
	return nil
}

func writeSegmentAtomically(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".rhiza-segment-*")
	if err != nil {
		return err
	}
	temp := f.Name()
	defer os.Remove(temp)
	if err := f.Chmod(0644); err == nil {
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
	return os.Rename(temp, path)
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

// Close closes all segment files.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, seg := range w.segments {
		seg.mu.Lock()
		seg.file.Sync()
		seg.file.Close()
		seg.mu.Unlock()
	}
	return nil
}
