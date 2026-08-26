package qlog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"sort"
	"sync"
	"time"
)

// SegmentMeta describes a segment file.
type SegmentMeta struct {
	Index       uint32   `json:"index"`
	StartSlot   uint64   `json:"start_slot"`
	EndSlot     uint64   `json:"end_slot"`
	Size        int64    `json:"size"`
	Hash        [32]byte `json:"hash"`
	ExtentHead  [32]byte `json:"extent_head,omitempty"`
	ExtentCount uint32   `json:"extent_count,omitempty"`
	Synced      bool     `json:"synced"` // object storage에 동기화되었는지
}

// Manifest tracks QLog segment metadata.
type Manifest struct {
	Generation uint64        `json:"generation,omitempty"`
	Segments   []SegmentMeta `json:"segments"`
	TipSlot    uint64        `json:"tip_slot"`
	LastSync   time.Time     `json:"last_sync"`
	mu         sync.RWMutex
}

// NewManifest creates a new manifest.
func NewManifest() *Manifest {
	return &Manifest{Segments: make([]SegmentMeta, 0)}
}

// ReplaceSynced installs the exact extent-chain snapshot to publish.
func (m *Manifest) ReplaceSynced(segments []SegmentMeta) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range segments {
		segments[i].Synced = true
	}
	if slices.Equal(m.Segments, segments) {
		return false
	}
	m.Generation++
	m.Segments = append(m.Segments[:0], segments...)
	m.recalculateTip()
	m.LastSync = time.Now()
	return true
}

func (m *Manifest) Snapshot() (segments []SegmentMeta, generation, tip uint64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]SegmentMeta(nil), m.Segments...), m.Generation, m.TipSlot
}

// Load reads the manifest from file.
func (m *Manifest) Load(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	return decodeManifestJSON(data, m)
}

func decodeManifestJSON(data []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("trailing JSON data")
	}
	return nil
}

// Save writes the manifest to file.
func (m *Manifest) Save(path string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0644)
}

// Add adds a segment to the manifest.
func (m *Manifest) Add(seg SegmentMeta) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Segments = append(m.Segments, seg)
	if seg.EndSlot > m.TipSlot {
		m.TipSlot = seg.EndSlot
	}
}

// Unsynced returns segments that haven't been synced to object storage.
func (m *Manifest) Unsynced() []SegmentMeta {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []SegmentMeta
	for _, seg := range m.Segments {
		if !seg.Synced {
			result = append(result, seg)
		}
	}
	return result
}

// MarkSynced marks a segment as synced to object storage.
func (m *Manifest) MarkSynced(index uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := range m.Segments {
		if m.Segments[i].Index == index {
			m.Segments[i].Synced = true
			m.LastSync = time.Now()
			break
		}
	}
}

func (m *Manifest) IsSynced(seg SegmentMeta) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, current := range m.Segments {
		if current.Index == seg.Index {
			return current.Synced && current.Size == seg.Size && current.Hash == seg.Hash
		}
	}
	return false
}

func (m *Manifest) UpsertSynced(seg SegmentMeta) {
	m.mu.Lock()
	defer m.mu.Unlock()
	seg.Synced = true
	for i := range m.Segments {
		if m.Segments[i].Index == seg.Index {
			m.Segments[i] = seg
			m.recalculateTip()
			m.LastSync = time.Now()
			return
		}
	}
	m.Segments = append(m.Segments, seg)
	sort.Slice(m.Segments, func(i, j int) bool { return m.Segments[i].Index < m.Segments[j].Index })
	m.recalculateTip()
	m.LastSync = time.Now()
}

func (m *Manifest) Tip() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.TipSlot
}

func (m *Manifest) recalculateTip() {
	m.TipSlot = 0
	for _, seg := range m.Segments {
		if seg.EndSlot > m.TipSlot {
			m.TipSlot = seg.EndSlot
		}
	}
}
