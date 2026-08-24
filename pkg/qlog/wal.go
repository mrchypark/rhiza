package qlog

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

	for _, m := range matches {
		var index uint32
		if _, err := fmt.Sscanf(filepath.Base(m), "seg_%d.log", &index); err != nil {
			continue
		}

		f, err := os.OpenFile(m, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return err
		}

		info, err := f.Stat()
		if err != nil {
			f.Close()
			return err
		}

		seg := &Segment{
			file:   f,
			index:  index,
			offset: info.Size(),
		}
		w.segments = append(w.segments, seg)
		w.current = seg
	}

	if len(w.segments) == 0 {
		return w.createSegment(1)
	}

	return nil
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
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
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

// readAll reads all entries from a segment.
func (s *Segment) readAll() ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	var entries []Entry
	memory := newReadArena()
	defer memory.free()
	buf := memory.bytes(1024 * 1024)

	for {
		// Read header first (49 bytes minimum)
		if _, err := io.ReadFull(s.file, buf[:49]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}

		payloadLen := binary.LittleEndian.Uint32(buf[41:45])
		totalLen := 49 + int(payloadLen)

		// Extend buffer if needed
		if totalLen > len(buf) {
			grown := memory.bytes(totalLen)
			copy(grown, buf[:49])
			buf = grown
		}

		// Read payload
		if _, err := io.ReadFull(s.file, buf[49:totalLen]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}

		entry, _, err := DecodeEntry(buf[:totalLen])
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}

	return entries, nil
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
