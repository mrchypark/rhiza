package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

const (
	extentVersion    = 1
	maxExtentSize    = 4 << 20
	maxExtentItems   = 64
	maxExtentObjects = 1 << 20
)

type Extent struct {
	Version     int                    `json:"version"`
	ConfigID    uint                   `json:"config_id"`
	Start       quepaxa.Slot           `json:"start"`
	End         quepaxa.Slot           `json:"end"`
	StartPrefix [32]byte               `json:"start_prefix"`
	EndPrefix   [32]byte               `json:"end_prefix"`
	Decisions   []quepaxa.DecidedValue `json:"decisions"`
}

type source interface {
	DecisionsFrom(quepaxa.Slot, int) ([]quepaxa.DecidedValue, quepaxa.Slot, error)
	PrefixHash(quepaxa.Slot) ([32]byte, bool)
}

type Manager struct {
	bucket   objstore.Bucket
	prefix   string
	configID uint
	mu       sync.Mutex
	extents  []Extent
	tip      quepaxa.Slot
}

func NewManager(bucket objstore.Bucket, prefix string, configID uint) *Manager {
	return &Manager{bucket: bucket, prefix: prefix, configID: configID}
}

func (m *Manager) Load(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var extents []Extent
	count := 0
	if err := m.bucket.Iter(ctx, m.key("archive/extents"), func(name string) error {
		count++
		if count > maxExtentObjects {
			return fmt.Errorf("shared archive exceeds extent limit")
		}
		start, end, hash, ok := parseExtentKey(name)
		if !ok {
			return nil
		}
		extent, err := m.readExtent(ctx, name, hash)
		if err != nil {
			return err
		}
		if extent.Start != start || extent.End != end {
			return fmt.Errorf("archive extent key range mismatch")
		}
		extents = append(extents, extent)
		return nil
	}); err != nil {
		return err
	}
	sort.Slice(extents, func(i, j int) bool {
		if extents[i].Start == extents[j].Start {
			return extents[i].End > extents[j].End
		}
		return extents[i].Start < extents[j].Start
	})
	m.extents = m.extents[:0]
	m.tip = 0
	next := quepaxa.Slot(1)
	var prefix [32]byte
	for _, extent := range extents {
		if extent.Start < next {
			continue
		}
		if extent.Start > next || extent.StartPrefix != prefix {
			break
		}
		m.extents = append(m.extents, extent)
		m.tip, next, prefix = extent.End, extent.End+1, extent.EndPrefix
	}
	return nil
}

func (m *Manager) SyncThrough(ctx context.Context, core source, through quepaxa.Slot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for m.tip < through {
		from := m.tip + 1
		decisions, tip, err := core.DecisionsFrom(from, maxExtentItems)
		if err != nil {
			return err
		}
		if len(decisions) == 0 || decisions[0].Slot != from {
			return fmt.Errorf("shared archive source omitted slot %d", from)
		}
		selected := decisions[:0]
		size := 256
		for _, decision := range decisions {
			next := size + len(decision.Value) + len(decision.Certificate) + 256
			if len(selected) != 0 && next > maxExtentSize {
				break
			}
			if next > maxExtentSize {
				return fmt.Errorf("decision %d exceeds archive extent limit", decision.Slot)
			}
			selected = append(selected, decision)
			size = next
			if decision.Slot >= through {
				break
			}
		}
		end := selected[len(selected)-1].Slot
		var startPrefix [32]byte
		if from > 1 {
			var ok bool
			startPrefix, ok = core.PrefixHash(from - 1)
			if !ok {
				return fmt.Errorf("archive start prefix %d is unavailable", from-1)
			}
		}
		endPrefix, ok := core.PrefixHash(end)
		if !ok {
			return fmt.Errorf("archive end prefix %d is unavailable", end)
		}
		extent := Extent{Version: extentVersion, ConfigID: m.configID, Start: from, End: end, StartPrefix: startPrefix, EndPrefix: endPrefix, Decisions: selected}
		data, err := json.Marshal(extent)
		if err != nil || len(data) > maxExtentSize {
			return fmt.Errorf("encode archive extent: %w", err)
		}
		hash := sha256.Sum256(data)
		if err := m.bucket.Upload(ctx, m.key(extentKey(extent, hash)), bytes.NewReader(data)); err != nil {
			return err
		}
		m.extents = append(m.extents, extent)
		m.tip = end
		if tip < through && end == tip {
			return fmt.Errorf("archive source tip %d is behind required slot %d", tip, through)
		}
	}
	return nil
}

func (m *Manager) DecisionsFrom(from quepaxa.Slot, limit int) ([]quepaxa.DecidedValue, quepaxa.Slot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if limit <= 0 {
		limit = 256
	}
	values := make([]quepaxa.DecidedValue, 0, limit)
	for _, extent := range m.extents {
		if extent.End < from {
			continue
		}
		for _, decision := range extent.Decisions {
			if decision.Slot < from {
				continue
			}
			if decision.Slot != from+quepaxa.Slot(len(values)) {
				return nil, m.tip, fmt.Errorf("shared archive decision gap")
			}
			values = append(values, decision)
			if len(values) == limit {
				return values, m.tip, nil
			}
		}
	}
	return values, m.tip, nil
}

func (m *Manager) Tip() quepaxa.Slot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tip
}

func (m *Manager) readExtent(ctx context.Context, name string, expected [32]byte) (Extent, error) {
	r, err := m.bucket.Get(ctx, name)
	if err != nil {
		return Extent{}, err
	}
	defer r.Close()
	data, err := io.ReadAll(io.LimitReader(r, maxExtentSize+1))
	if err != nil {
		return Extent{}, err
	}
	if len(data) > maxExtentSize || sha256.Sum256(data) != expected {
		return Extent{}, fmt.Errorf("archive extent integrity mismatch")
	}
	var extent Extent
	if err := json.Unmarshal(data, &extent); err != nil {
		return Extent{}, err
	}
	if err := m.validateExtent(extent); err != nil {
		return Extent{}, err
	}
	return extent, nil
}

func (m *Manager) validateExtent(extent Extent) error {
	if extent.Version != extentVersion || extent.ConfigID != m.configID || extent.Start == 0 || extent.End < extent.Start || len(extent.Decisions) == 0 || len(extent.Decisions) > maxExtentItems || int(extent.End-extent.Start+1) != len(extent.Decisions) {
		return fmt.Errorf("invalid archive extent")
	}
	prefix := extent.StartPrefix
	for i, decision := range extent.Decisions {
		if decision.Slot != extent.Start+quepaxa.Slot(i) || sha256.Sum256(decision.Value) != decision.Hash || len(decision.Certificate) == 0 {
			return fmt.Errorf("invalid archived decision")
		}
		prefix = quepaxa.AdvancePrefixHash(prefix, decision.Slot, decision.Hash)
	}
	if prefix != extent.EndPrefix {
		return fmt.Errorf("archive extent prefix mismatch")
	}
	return nil
}

func extentKey(extent Extent, hash [32]byte) string {
	return fmt.Sprintf("archive/extents/%020d_%020d_%x.json", extent.Start, extent.End, hash)
}

func parseExtentKey(name string) (quepaxa.Slot, quepaxa.Slot, [32]byte, bool) {
	parts := strings.Split(strings.TrimSuffix(path.Base(name), ".json"), "_")
	if len(parts) != 3 || len(parts[2]) != 64 {
		return 0, 0, [32]byte{}, false
	}
	start, startErr := strconv.ParseUint(parts[0], 10, 64)
	end, endErr := strconv.ParseUint(parts[1], 10, 64)
	decoded, hashErr := hex.DecodeString(parts[2])
	if startErr != nil || endErr != nil || hashErr != nil || len(decoded) != 32 {
		return 0, 0, [32]byte{}, false
	}
	var hash [32]byte
	copy(hash[:], decoded)
	return quepaxa.Slot(start), quepaxa.Slot(end), hash, true
}

func (m *Manager) key(value string) string {
	if m.prefix == "" {
		return value
	}
	return m.prefix + "/" + value
}
