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
	dir      string
	segments []*Segment
	current  *Segment
	mu       sync.RWMutex
	maxSize  int64
	dirty    bool
}

// Bytes returns the current on-disk segment bytes.
func (w *WAL) Bytes() int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	var total int64
	for _, segment := range w.segments {
		segment.mu.Lock()
		total += segment.offset
		segment.mu.Unlock()
	}
	return total
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
		f, err := os.OpenFile(item.path, os.O_RDWR, 0600)
		if err != nil {
			w.closeSegments()
			return err
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
		w.dirty = false
	}
	path := filepath.Join(w.dir, fmt.Sprintf("seg_%03d.log", index))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
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
	w.mu.RLock()
	defer w.mu.RUnlock()
	segments := make([]*Segment, len(w.segments))
	copy(segments, w.segments)

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
		payloadLen, _ := entryPayloadLength(binary.LittleEndian.Uint32(header[41:45]))
		totalLen := int64(len(header)) + int64(payloadLen)
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
	f, err := os.OpenFile(path, os.O_RDWR, 0600)
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

// Compact atomically installs a certified base plus the suffix that remains
// necessary for consensus recovery. The new higher-numbered segment is made
// durable before any old segment is removed.
func (w *WAL) Compact(base Entry, keepValueHashes map[[32]byte]struct{}) error {
	if base.Type != EntryCheckpoint || base.Slot == 0 {
		return fmt.Errorf("invalid WAL compaction base")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	var kept []Entry
	for _, seg := range w.segments {
		entries, err := seg.readAll()
		if err != nil {
			return err
		}
		for _, entry := range entries {
			switch {
			case entry.Type == EntryProposal:
				if _, ok := keepValueHashes[entry.Hash]; ok {
					kept = append(kept, entry)
				}
			case entry.Slot > base.Slot:
				kept = append(kept, entry)
			}
		}
	}
	index := uint32(1)
	if w.current != nil {
		index = w.current.index + 1
	}
	temp, err := os.CreateTemp(w.dir, ".rhiza-compact-*")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	offset := int64(0)
	for _, entry := range append([]Entry{base}, kept...) {
		data := entry.Encode()
		n, writeErr := temp.Write(data)
		if writeErr != nil {
			temp.Close()
			return writeErr
		}
		if n != len(data) {
			temp.Close()
			return io.ErrShortWrite
		}
		offset += int64(n)
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	target := filepath.Join(w.dir, fmt.Sprintf("seg_%03d.log", index))
	if err := os.Rename(tempPath, target); err != nil {
		return err
	}
	if err := syncDir(w.dir); err != nil {
		return err
	}
	file, err := os.OpenFile(target, os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	old := append([]*Segment(nil), w.segments...)
	w.segments = []*Segment{{file: file, index: index, offset: offset}}
	w.current = w.segments[0]
	w.dirty = false
	var closeErr error
	for _, seg := range old {
		seg.mu.Lock()
		closeErr = errors.Join(closeErr, seg.file.Close())
		seg.mu.Unlock()
	}
	if closeErr != nil {
		return closeErr
	}
	for _, seg := range old {
		if err := os.Remove(filepath.Join(w.dir, fmt.Sprintf("seg_%03d.log", seg.index))); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return syncDir(w.dir)
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
