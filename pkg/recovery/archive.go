package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

const (
	archiveVersion     = 2
	maxExtentSize      = 8 << 20
	maxExtentItems     = 1024
	maxManifestSize    = 4 << 20
	maxHeadSize        = 4 << 10
	archiveGroupDelay  = 2 * time.Millisecond
	archiveSyncTimeout = 5 * time.Minute
	maxPublishRetries  = 8
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

type extentRef struct {
	Start       quepaxa.Slot `json:"start"`
	End         quepaxa.Slot `json:"end"`
	Hash        [32]byte     `json:"hash"`
	StartPrefix [32]byte     `json:"start_prefix"`
	EndPrefix   [32]byte     `json:"end_prefix"`
}

type archiveManifest struct {
	Version  int          `json:"version"`
	ConfigID uint         `json:"config_id"`
	Tip      quepaxa.Slot `json:"tip"`
	Extents  []extentRef  `json:"extents"`
}

type archiveHead struct {
	Version      int          `json:"version"`
	ConfigID     uint         `json:"config_id"`
	Tip          quepaxa.Slot `json:"tip"`
	ManifestHash [32]byte     `json:"manifest_hash"`
}

type source interface {
	DecisionsFrom(quepaxa.Slot, int) ([]quepaxa.DecidedValue, quepaxa.Slot, error)
	PrefixHash(quepaxa.Slot) ([32]byte, bool)
}

type syncBatch struct {
	target quepaxa.Slot
	done   chan struct{}
	err    error
}

type Manager struct {
	bucket      objstore.Bucket
	prefix      string
	configID    uint
	mu          sync.Mutex
	extents     []Extent
	manifest    archiveManifest
	tip         quepaxa.Slot
	headVersion *objstore.ObjectVersion
	cas         bool
	batchMu     sync.Mutex
	batch       *syncBatch
}

func NewManager(bucket objstore.Bucket, prefix string, configID uint) *Manager {
	options := bucket.SupportedObjectUploadOptions()
	return &Manager{bucket: bucket, prefix: prefix, configID: configID, cas: slices.Contains(options, objstore.IfMatch) && slices.Contains(options, objstore.IfNotExists)}
}

func (m *Manager) Load(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.loadLocked(ctx, true)
}

func (m *Manager) loadLocked(ctx context.Context, checkLegacy bool) error {
	name := m.key("archive/latest.json")
	attributes, err := m.bucket.Attributes(ctx, name)
	if err != nil {
		if !m.bucket.IsObjNotFoundErr(err) {
			return err
		}
		if checkLegacy {
			legacy := false
			if err := m.bucket.Iter(ctx, m.key("archive/extents"), func(string) error { legacy = true; return nil }); err != nil {
				return err
			}
			if legacy {
				return fmt.Errorf("legacy shared archive format is not supported; rebuild archive v2 from a certified local log")
			}
		}
		m.extents = nil
		m.manifest = archiveManifest{Version: archiveVersion, ConfigID: m.configID}
		m.tip = 0
		m.headVersion = nil
		return nil
	}
	headData, err := m.readObject(ctx, name, maxHeadSize)
	if err != nil {
		return err
	}
	var head archiveHead
	if err := json.Unmarshal(headData, &head); err != nil {
		return err
	}
	if head.Version != archiveVersion || head.ConfigID != m.configID || head.Tip == 0 || head.ManifestHash == ([32]byte{}) {
		return fmt.Errorf("invalid shared archive head")
	}
	manifestName := m.key(manifestKey(head.Tip, head.ManifestHash))
	manifestData, err := m.readObject(ctx, manifestName, maxManifestSize)
	if err != nil {
		return err
	}
	if sha256.Sum256(manifestData) != head.ManifestHash {
		return fmt.Errorf("shared archive manifest integrity mismatch")
	}
	var manifest archiveManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		return err
	}
	if manifest.Version != archiveVersion || manifest.ConfigID != m.configID || manifest.Tip != head.Tip || len(manifest.Extents) == 0 {
		return fmt.Errorf("invalid shared archive manifest")
	}
	extents := make([]Extent, 0, len(manifest.Extents))
	next := quepaxa.Slot(1)
	var prefix [32]byte
	for _, ref := range manifest.Extents {
		if ref.Start != next || ref.End < ref.Start || ref.StartPrefix != prefix || ref.Hash == ([32]byte{}) {
			return fmt.Errorf("invalid shared archive extent chain")
		}
		extent, err := m.readExtent(ctx, m.key(extentKey(ref)), ref.Hash)
		if err != nil {
			return err
		}
		if extent.Start != ref.Start || extent.End != ref.End || extent.StartPrefix != ref.StartPrefix || extent.EndPrefix != ref.EndPrefix {
			return fmt.Errorf("shared archive extent reference mismatch")
		}
		extents = append(extents, extent)
		next, prefix = ref.End+1, ref.EndPrefix
	}
	if next-1 != manifest.Tip {
		return fmt.Errorf("shared archive manifest tip mismatch")
	}
	m.extents = extents
	m.manifest = manifest
	m.tip = manifest.Tip
	m.headVersion = attributes.Version
	return nil
}

// SyncThrough publishes all decisions through the requested slot. Concurrent
// callers share a bounded delay and one immutable block/manifest publication.
func (m *Manager) SyncThrough(ctx context.Context, core source, through quepaxa.Slot) error {
	if through == 0 || m.Tip() >= through {
		return nil
	}
	m.batchMu.Lock()
	if batch := m.batch; batch != nil {
		if through > batch.target {
			batch.target = through
		}
		m.batchMu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-batch.done:
			if batch.err != nil {
				return batch.err
			}
			return m.SyncThrough(ctx, core, through)
		}
	}
	batch := &syncBatch{target: through, done: make(chan struct{})}
	m.batch = batch
	m.batchMu.Unlock()
	go m.flushBatch(batch, core)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-batch.done:
		return batch.err
	}
}

func (m *Manager) flushBatch(batch *syncBatch, core source) {
	time.Sleep(archiveGroupDelay)
	m.batchMu.Lock()
	target := batch.target
	m.batchMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), archiveSyncTimeout)
	batch.err = m.syncNow(ctx, core, target)
	cancel()
	m.batchMu.Lock()
	if m.batch == batch {
		m.batch = nil
	}
	close(batch.done)
	m.batchMu.Unlock()
}

func (m *Manager) syncNow(ctx context.Context, core source, through quepaxa.Slot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for attempt := 0; attempt < maxPublishRetries; attempt++ {
		if m.tip >= through {
			return nil
		}
		extents := append([]Extent(nil), m.extents...)
		manifest := m.manifest
		manifest.Version, manifest.ConfigID = archiveVersion, m.configID
		manifest.Extents = append([]extentRef(nil), manifest.Extents...)
		from := m.tip + 1
		for from <= through {
			extent, ref, sourceTip, err := m.buildExtent(core, from, through)
			if err != nil {
				return err
			}
			data, err := json.Marshal(extent)
			if err != nil || len(data) > maxExtentSize {
				return fmt.Errorf("encode archive extent: %w", err)
			}
			if err := m.bucket.Upload(ctx, m.key(extentKey(ref)), bytes.NewReader(data)); err != nil {
				return err
			}
			extents = append(extents, extent)
			manifest.Extents = append(manifest.Extents, ref)
			manifest.Tip = ref.End
			from = ref.End + 1
			if sourceTip < through && ref.End == sourceTip {
				return fmt.Errorf("archive source tip %d is behind required slot %d", sourceTip, through)
			}
		}
		manifestData, err := json.Marshal(manifest)
		if err != nil || len(manifestData) > maxManifestSize {
			return fmt.Errorf("encode archive manifest: %w", err)
		}
		manifestHash := sha256.Sum256(manifestData)
		if err := m.bucket.Upload(ctx, m.key(manifestKey(manifest.Tip, manifestHash)), bytes.NewReader(manifestData)); err != nil {
			return err
		}
		if err := m.publishHeadLocked(ctx, manifest, manifestHash); err == nil {
			m.extents, m.manifest, m.tip = extents, manifest, manifest.Tip
			if m.cas {
				return m.loadLocked(ctx, false)
			}
			return nil
		} else if !m.cas {
			return err
		}
		if err := m.loadLocked(ctx, false); err != nil {
			return err
		}
	}
	return fmt.Errorf("shared archive publication conflicted too many times")
}

func (m *Manager) publishHeadLocked(ctx context.Context, manifest archiveManifest, hash [32]byte) error {
	data, err := json.Marshal(archiveHead{Version: archiveVersion, ConfigID: m.configID, Tip: manifest.Tip, ManifestHash: hash})
	if err != nil {
		return err
	}
	var options []objstore.ObjectUploadOption
	if m.cas {
		if m.headVersion == nil {
			options = append(options, objstore.WithIfNotExists())
		} else {
			options = append(options, objstore.WithIfMatch(m.headVersion))
		}
	}
	return m.bucket.Upload(ctx, m.key("archive/latest.json"), bytes.NewReader(data), options...)
}

func (m *Manager) buildExtent(core source, from, through quepaxa.Slot) (Extent, extentRef, quepaxa.Slot, error) {
	decisions, tip, err := core.DecisionsFrom(from, maxExtentItems)
	if err != nil {
		return Extent{}, extentRef{}, tip, err
	}
	if len(decisions) == 0 || decisions[0].Slot != from {
		return Extent{}, extentRef{}, tip, fmt.Errorf("shared archive source omitted slot %d", from)
	}
	selected := decisions[:0]
	size := 256
	for _, decision := range decisions {
		next := size + len(decision.Value) + len(decision.Certificate) + 256
		if len(selected) != 0 && next > maxExtentSize {
			break
		}
		if next > maxExtentSize {
			return Extent{}, extentRef{}, tip, fmt.Errorf("decision %d exceeds archive extent limit", decision.Slot)
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
			return Extent{}, extentRef{}, tip, fmt.Errorf("archive start prefix %d is unavailable", from-1)
		}
	}
	endPrefix, ok := core.PrefixHash(end)
	if !ok {
		return Extent{}, extentRef{}, tip, fmt.Errorf("archive end prefix %d is unavailable", end)
	}
	extent := Extent{Version: archiveVersion, ConfigID: m.configID, Start: from, End: end, StartPrefix: startPrefix, EndPrefix: endPrefix, Decisions: selected}
	data, err := json.Marshal(extent)
	if err != nil {
		return Extent{}, extentRef{}, tip, err
	}
	hash := sha256.Sum256(data)
	return extent, extentRef{Start: from, End: end, Hash: hash, StartPrefix: startPrefix, EndPrefix: endPrefix}, tip, nil
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

// Cleanup removes obsolete immutable publications after the grace period.
// The current manifest and every block it references are always retained.
func (m *Manager) Cleanup(ctx context.Context, grace time.Duration) error {
	if grace < 0 {
		return fmt.Errorf("archive GC grace period must not be negative")
	}
	m.mu.Lock()
	if err := m.loadLocked(ctx, false); err != nil {
		m.mu.Unlock()
		return err
	}
	if compacted := m.compactExtents(); len(compacted) < len(m.extents) {
		manifest := archiveManifest{Version: archiveVersion, ConfigID: m.configID, Tip: m.tip, Extents: make([]extentRef, 0, len(compacted))}
		for _, extent := range compacted {
			data, err := json.Marshal(extent)
			if err != nil || len(data) > maxExtentSize {
				m.mu.Unlock()
				return fmt.Errorf("encode compacted archive extent: %w", err)
			}
			hash := sha256.Sum256(data)
			ref := extentRef{Start: extent.Start, End: extent.End, Hash: hash, StartPrefix: extent.StartPrefix, EndPrefix: extent.EndPrefix}
			if err := m.bucket.Upload(ctx, m.key(extentKey(ref)), bytes.NewReader(data)); err != nil {
				m.mu.Unlock()
				return err
			}
			manifest.Extents = append(manifest.Extents, ref)
		}
		data, err := json.Marshal(manifest)
		if err != nil || len(data) > maxManifestSize {
			m.mu.Unlock()
			return fmt.Errorf("encode compacted archive manifest: %w", err)
		}
		hash := sha256.Sum256(data)
		if err := m.bucket.Upload(ctx, m.key(manifestKey(manifest.Tip, hash)), bytes.NewReader(data)); err != nil {
			m.mu.Unlock()
			return err
		}
		if err := m.publishHeadLocked(ctx, manifest, hash); err != nil {
			m.mu.Unlock()
			return err
		}
		m.extents, m.manifest = compacted, manifest
		if m.cas {
			if err := m.loadLocked(ctx, false); err != nil {
				m.mu.Unlock()
				return err
			}
		}
	}
	keep := make(map[string]struct{}, len(m.manifest.Extents)+1)
	manifestData, err := json.Marshal(m.manifest)
	if err != nil {
		m.mu.Unlock()
		return err
	}
	manifestHash := sha256.Sum256(manifestData)
	keep[m.key(manifestKey(m.manifest.Tip, manifestHash))] = struct{}{}
	for _, ref := range m.manifest.Extents {
		keep[m.key(extentKey(ref))] = struct{}{}
	}
	m.mu.Unlock()
	cutoff := time.Now().Add(-grace)
	for _, dir := range []string{"archive/manifests", "archive/blocks"} {
		if err := m.bucket.Iter(ctx, m.key(dir), func(name string) error {
			if _, ok := keep[name]; ok {
				return nil
			}
			attributes, err := m.bucket.Attributes(ctx, name)
			if err != nil {
				return err
			}
			if attributes.LastModified.After(cutoff) {
				return nil
			}
			if err := m.bucket.Delete(ctx, name); err != nil && !m.bucket.IsObjNotFoundErr(err) {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) compactExtents() []Extent {
	if len(m.extents) < 2 {
		return m.extents
	}
	result := make([]Extent, 0, len(m.extents))
	var current Extent
	var prefix [32]byte
	size := 256
	flush := func() {
		if len(current.Decisions) != 0 {
			result = append(result, current)
		}
	}
	for _, extent := range m.extents {
		for _, decision := range extent.Decisions {
			nextSize := size + len(decision.Value) + len(decision.Certificate) + 256
			if len(current.Decisions) == maxExtentItems || len(current.Decisions) != 0 && nextSize > maxExtentSize {
				flush()
				current, size = Extent{}, 256
			}
			if len(current.Decisions) == 0 {
				current = Extent{Version: archiveVersion, ConfigID: m.configID, Start: decision.Slot, StartPrefix: prefix}
			}
			current.Decisions = append(current.Decisions, decision)
			current.End = decision.Slot
			prefix = quepaxa.AdvancePrefixHash(prefix, decision.Slot, decision.Hash)
			current.EndPrefix = prefix
			size = nextSize
		}
	}
	flush()
	return result
}

func (m *Manager) readExtent(ctx context.Context, name string, expected [32]byte) (Extent, error) {
	data, err := m.readObject(ctx, name, maxExtentSize)
	if err != nil {
		return Extent{}, err
	}
	if sha256.Sum256(data) != expected {
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

func (m *Manager) readObject(ctx context.Context, name string, limit int64) ([]byte, error) {
	r, err := m.bucket.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	data, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("object %s exceeds size limit", name)
	}
	return data, nil
}

func (m *Manager) validateExtent(extent Extent) error {
	if extent.Version != archiveVersion || extent.ConfigID != m.configID || extent.Start == 0 || extent.End < extent.Start || len(extent.Decisions) == 0 || len(extent.Decisions) > maxExtentItems || int(extent.End-extent.Start+1) != len(extent.Decisions) {
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

func extentKey(ref extentRef) string {
	return fmt.Sprintf("archive/blocks/%020d_%020d_%x.json", ref.Start, ref.End, ref.Hash)
}

func manifestKey(tip quepaxa.Slot, hash [32]byte) string {
	return fmt.Sprintf("archive/manifests/%020d_%x.json", tip, hash)
}

func (m *Manager) key(value string) string {
	if m.prefix == "" {
		return value
	}
	return m.prefix + "/" + value
}
