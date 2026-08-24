package objstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/mrchypark/rhiza/internal/encoding"
	"github.com/mrchypark/rhiza/internal/types"
)

// QLog is an ordered commit log backed by object storage.
// It stores decided values in contiguous segments.
type QLog struct {
	store    *Store
	mu       sync.RWMutex
	manifest *Manifest
}

// Manifest tracks QLog segment metadata.
type Manifest struct {
	Segments []SegmentMeta `json:"segments"`
}

// SegmentMeta describes a single QLog segment.
type SegmentMeta struct {
	StartSlot types.Slot     `json:"start_slot"`
	EndSlot   types.Slot     `json:"end_slot"` // exclusive
	Hash      types.ValueHash `json:"hash"`
	Size      int64          `json:"size"`
	CreatedAt time.Time      `json:"created_at"`
}

// Entry is a single QLog entry.
type Entry struct {
	Slot    types.Slot     `json:"slot"`
	Hash    types.ValueHash `json:"hash"`
	Payload []byte         `json:"payload"`
}

// NewQLog creates a new QLog instance.
func NewQLog(store *Store) *QLog {
	return &QLog{
		store: store,
		manifest: &Manifest{
			Segments: make([]SegmentMeta, 0),
		},
	}
}

// Load reads the manifest from object storage.
func (q *QLog) Load(ctx context.Context) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	r, err := q.store.Get(ctx, "qlog/manifest.json")
	if err != nil {
		if q.store.IsNotFound(err) {
			q.manifest = &Manifest{Segments: make([]SegmentMeta, 0)}
			return nil
		}
		return fmt.Errorf("get manifest: %w", err)
	}
	defer r.Close()

	var m Manifest
	if err := json.NewDecoder(r).Decode(&m); err != nil {
		return fmt.Errorf("decode manifest: %w", err)
	}
	q.manifest = &m
	return nil
}

// Persist writes the manifest to object storage.
func (q *QLog) Persist(ctx context.Context) error {
	q.mu.RLock()
	defer q.mu.RUnlock()

	data, err := json.Marshal(q.manifest)
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}

	return q.store.Upload(ctx, "qlog/manifest.json", bytes.NewReader(data))
}

// Append adds entries to the QLog and returns the committed slot range.
func (q *QLog) Append(ctx context.Context, entries []Entry) (types.SlotRange, error) {
	if len(entries) == 0 {
		return types.SlotRange{}, nil
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	startSlot := entries[0].Slot
	endSlot := entries[len(entries)-1].Slot + 1

	// Convert to encoding entries
	encEntries := make([]encoding.Entry, len(entries))
	for i, e := range entries {
		encEntries[i] = encoding.Entry{
			Slot:    e.Slot,
			Hash:    e.Hash,
			Payload: e.Payload,
		}
	}

	// Upload segment data
	key := fmt.Sprintf("qlog/segments/%d-%d", startSlot, endSlot)
	data, err := encoding.EncodeEntries(encEntries)
	if err != nil {
		return types.SlotRange{}, fmt.Errorf("encode entries: %w", err)
	}

	if err := q.store.Upload(ctx, key, bytes.NewReader(data)); err != nil {
		return types.SlotRange{}, fmt.Errorf("upload segment: %w", err)
	}

	// Update manifest
	q.manifest.Segments = append(q.manifest.Segments, SegmentMeta{
		StartSlot: startSlot,
		EndSlot:   endSlot,
		Size:      int64(len(data)),
		CreatedAt: time.Now(),
	})

	return types.SlotRange{Start: startSlot, End: endSlot}, nil
}

// Read returns entries in the given slot range.
func (q *QLog) Read(ctx context.Context, from, to types.Slot) ([]Entry, error) {
	q.mu.RLock()
	segments := q.manifest.Segments
	q.mu.RUnlock()

	var entries []Entry
	for _, seg := range segments {
		if seg.EndSlot <= from || seg.StartSlot >= to {
			continue
		}

		key := fmt.Sprintf("qlog/segments/%d-%d", seg.StartSlot, seg.EndSlot)
		r, err := q.store.Get(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("get segment: %w", err)
		}

		encEntries, err := encoding.DecodeEntries(r)
		r.Close()
		if err != nil {
			return nil, fmt.Errorf("decode segment: %w", err)
		}

		for _, e := range encEntries {
			if e.Slot >= from && e.Slot < to {
				entries = append(entries, Entry{
					Slot:    e.Slot,
					Hash:    e.Hash,
					Payload: e.Payload,
				})
			}
		}
	}

	return entries, nil
}

// Tip returns the latest committed slot.
func (q *QLog) Tip() types.Slot {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if len(q.manifest.Segments) == 0 {
		return 0
	}
	return q.manifest.Segments[len(q.manifest.Segments)-1].EndSlot
}

// IsNotFound checks if an error indicates a missing object.
func (s *Store) IsNotFound(err error) bool {
	return s.bucket.IsObjNotFoundErr(err)
}
