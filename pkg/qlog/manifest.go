package qlog

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

// SegmentMeta describes a segment file.
type SegmentMeta struct {
	Index    uint32    `json:"index"`
	StartSlot uint64   `json:"start_slot"`
	EndSlot  uint64    `json:"end_slot"`
	Size     int64     `json:"size"`
	Hash     [32]byte  `json:"hash"`
	Synced   bool      `json:"synced"` // object storage에 동기화되었는지
}

// Manifest tracks QLog segment metadata.
type Manifest struct {
	Segments   []SegmentMeta `json:"segments"`
	TipSlot    uint64        `json:"tip_slot"`
	LastSync   time.Time     `json:"last_sync"`
	mu         sync.RWMutex
}

// NewManifest creates a new manifest.
func NewManifest() *Manifest {
	return &Manifest{
		Segments: make([]SegmentMeta, 0),
	}
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

	return json.Unmarshal(data, m)
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
