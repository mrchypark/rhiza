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
	maxExtentSize      = 8 << 20
	maxExtentItems     = 1024
	maxHeadSize        = 64 << 10
	archiveGroupDelay  = 2 * time.Millisecond
	archiveSyncTimeout = 5 * time.Minute
	maxPublishRetries  = 8
	maxCachedExtents   = 2
)

type Extent struct {
	ConfigID     uint                   `json:"config_id"`
	Start        quepaxa.Slot           `json:"start"`
	End          quepaxa.Slot           `json:"end"`
	StartPrefix  [32]byte               `json:"start_prefix"`
	EndPrefix    [32]byte               `json:"end_prefix"`
	PreviousHash [32]byte               `json:"previous_hash"`
	Decisions    []quepaxa.DecidedValue `json:"decisions"`
	hash         [32]byte
}

type archiveHead struct {
	ConfigID     uint                    `json:"config_id"`
	Base         quepaxa.Slot            `json:"base"`
	BasePrefix   [32]byte                `json:"base_prefix"`
	BaseSeal     *quepaxa.CheckpointSeal `json:"base_seal,omitempty"`
	BaseDecision *quepaxa.DecidedValue   `json:"base_decision,omitempty"`
	Tip          quepaxa.Slot            `json:"tip"`
	TailHash     [32]byte                `json:"tail_hash"`
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
	bucket   objstore.Bucket
	prefix   string
	configID uint
	mu       sync.Mutex
	extents  []Extent
	cache    map[[32]byte]Extent
	cacheLRU [][32]byte
	head     archiveHead
	tip      quepaxa.Slot
	headCAS  *objstore.ObjectVersion
	cas      bool
	batchMu  sync.Mutex
	batch    *syncBatch
}

func NewManager(bucket objstore.Bucket, prefix string, configID uint) *Manager {
	options := bucket.SupportedObjectUploadOptions()
	return &Manager{bucket: bucket, prefix: prefix, configID: configID, cas: slices.Contains(options, objstore.IfMatch) && slices.Contains(options, objstore.IfNotExists), cache: make(map[[32]byte]Extent)}
}

func (m *Manager) Load(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.loadLocked(ctx)
}

func (m *Manager) loadLocked(ctx context.Context) error {
	name := m.key("archive/latest.json")
	attributes, err := m.bucket.Attributes(ctx, name)
	if err != nil {
		if !m.bucket.IsObjNotFoundErr(err) {
			return err
		}
		unsupported := false
		if err := m.bucket.Iter(ctx, m.key("archive/extents"), func(string) error { unsupported = true; return nil }); err != nil {
			return err
		}
		if unsupported {
			return fmt.Errorf("unsupported shared archive layout")
		}
		m.extents = nil
		m.head = archiveHead{ConfigID: m.configID}
		m.tip = 0
		m.headCAS = nil
		return nil
	}
	if m.headCAS != nil && attributes.Version != nil && *m.headCAS == *attributes.Version {
		return nil
	}
	headData, err := m.readObject(ctx, name, maxHeadSize)
	if err != nil {
		return err
	}
	var head archiveHead
	if err := decodePersistedJSON(headData, &head); err != nil {
		return err
	}
	if head.ConfigID != m.configID || head.Tip < head.Base || (head.Tip > head.Base) != (head.TailHash != [32]byte{}) {
		return fmt.Errorf("invalid shared archive head")
	}
	if head.Base > 0 && (head.BaseSeal == nil || head.BaseDecision == nil || head.BaseSeal.Index != head.Base || head.BaseSeal.PrefixHash != head.BasePrefix) {
		return fmt.Errorf("invalid shared archive recovery base")
	}
	extents := make([]Extent, 0)
	hash, end := head.TailHash, head.Tip
	for end > head.Base {
		if hash == ([32]byte{}) {
			return fmt.Errorf("invalid shared archive block chain")
		}
		extent, err := m.readExtent(ctx, hash)
		if err != nil {
			return err
		}
		if extent.End != end {
			return fmt.Errorf("invalid shared archive block chain")
		}
		ref := extent
		ref.Decisions = nil
		extents = append(extents, ref)
		hash, end = extent.PreviousHash, extent.Start-1
	}
	slices.Reverse(extents)
	if len(extents) != 0 && extents[0].Start <= head.Base {
		extent, err := m.readExtent(ctx, extents[0].hash)
		if err != nil {
			return err
		}
		offset := int(head.Base - extent.Start + 1)
		extent.Decisions = append([]quepaxa.DecidedValue(nil), extent.Decisions[offset:]...)
		extent.Start = head.Base + 1
		extent.StartPrefix = head.BasePrefix
		if err := m.validateExtent(extent); err != nil {
			return fmt.Errorf("invalid shared archive checkpoint boundary: %w", err)
		}
		extents[0].Start = extent.Start
		extents[0].StartPrefix = extent.StartPrefix
	}
	next, prefix := head.Base+1, head.BasePrefix
	for i, extent := range extents {
		if extent.Start != next || extent.StartPrefix != prefix {
			return fmt.Errorf("invalid shared archive block chain")
		}
		next, prefix = extent.End+1, extent.EndPrefix
		extents[i].Decisions = nil
	}
	if next-1 != head.Tip {
		return fmt.Errorf("shared archive head tip mismatch")
	}
	m.extents = extents
	m.head = head
	m.tip = head.Tip
	m.headCAS = attributes.Version
	return nil
}

// CASSupported reports whether the shared mutable head can be published
// without regressing under concurrent writers.
func (m *Manager) CASSupported() bool { return m.cas }

// TrimThrough removes decisions covered by a certified checkpoint while
// retaining the authenticated prefix needed to validate the remaining tail.
func (m *Manager) TrimThrough(ctx context.Context, sealed quepaxa.SealedCheckpoint, decision quepaxa.DecidedValue) error {
	through, prefix := sealed.Index, sealed.PrefixHash
	encoded, err := quepaxa.EncodeCheckpointSeal(sealed.CheckpointSeal)
	if err != nil || !bytes.Equal(encoded, decision.Value) || decision.Slot != sealed.DecisionSlot {
		return fmt.Errorf("archive trim requires the certified checkpoint decision")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for attempt := 0; attempt < maxPublishRetries; attempt++ {
		if through <= m.head.Base {
			return nil
		}
		if through > m.tip {
			return fmt.Errorf("archive tip %d is behind trim slot %d", m.tip, through)
		}
		sealCopy, decisionCopy := sealed.CheckpointSeal, decision
		head := m.head
		head.Base, head.BasePrefix, head.BaseSeal, head.BaseDecision = through, prefix, &sealCopy, &decisionCopy
		extents := make([]Extent, 0, len(m.extents))
		for _, extent := range m.extents {
			if extent.End <= through {
				continue
			}
			if extent.Start <= through {
				extent.Start = through + 1
				extent.StartPrefix = prefix
			}
			extents = append(extents, extent)
		}
		if through == m.tip {
			head.TailHash = [32]byte{}
		}
		if err := m.publishHeadLocked(ctx, head); err == nil {
			m.extents, m.head = extents, head
			if m.cas {
				return m.refreshPublishedHead(ctx, head)
			}
			return nil
		} else if !m.cas {
			return err
		}
		if err := m.loadLocked(ctx); err != nil {
			return err
		}
	}
	return fmt.Errorf("shared archive trim conflicted too many times")
}

func (m *Manager) refreshPublishedHead(ctx context.Context, expected archiveHead) error {
	name := m.key("archive/latest.json")
	attributes, err := m.bucket.Attributes(ctx, name)
	if err != nil {
		return err
	}
	data, err := m.readObject(ctx, name, maxHeadSize)
	if err != nil {
		return err
	}
	var head archiveHead
	if err := decodePersistedJSON(data, &head); err != nil {
		return err
	}
	expectedData, err := json.Marshal(expected)
	if err != nil {
		return err
	}
	if !bytes.Equal(data, expectedData) {
		return m.loadLocked(ctx)
	}
	m.headCAS = attributes.Version
	return nil
}

func (m *Manager) RecoveryBase() (quepaxa.CheckpointSeal, quepaxa.DecidedValue, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.head.BaseSeal == nil || m.head.BaseDecision == nil {
		return quepaxa.CheckpointSeal{}, quepaxa.DecidedValue{}, false
	}
	return *m.head.BaseSeal, *m.head.BaseDecision, true
}

// SyncThrough publishes all decisions through the requested slot. Concurrent
// callers share a bounded delay and one immutable block/head publication.
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
		added := make([]Extent, 0, 1)
		head := m.head
		head.ConfigID = m.configID
		previous := head.TailHash
		from := m.tip + 1
		for from <= through {
			extent, sourceTip, err := m.buildExtent(core, from, through, previous)
			if err != nil {
				return err
			}
			data, err := json.Marshal(extent)
			if err != nil {
				return fmt.Errorf("encode archive extent: %w", err)
			}
			if len(data) > maxExtentSize {
				return fmt.Errorf("archive extent exceeds %d bytes", maxExtentSize)
			}
			if err := m.bucket.Upload(ctx, m.key(extentKey(extent.hash)), bytes.NewReader(data)); err != nil {
				return err
			}
			added = append(added, extent)
			head.Tip, head.TailHash, previous = extent.End, extent.hash, extent.hash
			from = extent.End + 1
			if sourceTip < through && extent.End == sourceTip {
				return fmt.Errorf("archive source tip %d is behind required slot %d", sourceTip, through)
			}
		}
		if err := m.publishHeadLocked(ctx, head); err == nil {
			for _, extent := range added {
				m.rememberExtentLocked(extent)
				extent.Decisions = nil
				m.extents = append(m.extents, extent)
			}
			m.head, m.tip = head, head.Tip
			if m.cas {
				return m.refreshPublishedHead(ctx, head)
			}
			return nil
		} else if !m.cas {
			return err
		}
		if err := m.loadLocked(ctx); err != nil {
			return err
		}
	}
	return fmt.Errorf("shared archive publication conflicted too many times")
}

func (m *Manager) publishHeadLocked(ctx context.Context, head archiveHead) error {
	data, err := json.Marshal(head)
	if err != nil {
		return err
	}
	if len(data) > maxHeadSize {
		return fmt.Errorf("archive head exceeds %d bytes", maxHeadSize)
	}
	var options []objstore.ObjectUploadOption
	if m.cas {
		if m.headCAS == nil {
			options = append(options, objstore.WithIfNotExists())
		} else {
			options = append(options, objstore.WithIfMatch(m.headCAS))
		}
	}
	return m.bucket.Upload(ctx, m.key("archive/latest.json"), bytes.NewReader(data), options...)
}

func (m *Manager) buildExtent(core source, from, through quepaxa.Slot, previous [32]byte) (Extent, quepaxa.Slot, error) {
	decisions, tip, err := core.DecisionsFrom(from, maxExtentItems)
	if err != nil {
		return Extent{}, tip, err
	}
	if len(decisions) == 0 || decisions[0].Slot != from {
		return Extent{}, tip, fmt.Errorf("shared archive source omitted slot %d", from)
	}
	var startPrefix [32]byte
	if from > 1 {
		var ok bool
		startPrefix, ok = core.PrefixHash(from - 1)
		if !ok {
			return Extent{}, tip, fmt.Errorf("archive start prefix %d is unavailable", from-1)
		}
	}
	selected := decisions[:0]
	encodedDecisions := 0
	var endPrefix [32]byte
	for _, decision := range decisions {
		encoded, err := json.Marshal(decision)
		if err != nil {
			return Extent{}, tip, err
		}
		candidatePrefix, ok := core.PrefixHash(decision.Slot)
		if !ok {
			return Extent{}, tip, fmt.Errorf("archive end prefix %d is unavailable", decision.Slot)
		}
		candidate := Extent{ConfigID: m.configID, Start: from, End: decision.Slot, StartPrefix: startPrefix, EndPrefix: candidatePrefix, PreviousHash: previous, Decisions: []quepaxa.DecidedValue{}}
		size, err := extentEncodedSize(candidate, encodedDecisions+len(encoded), len(selected)+1)
		if err != nil {
			return Extent{}, tip, err
		}
		if len(selected) != 0 && size > maxExtentSize {
			break
		}
		if size > maxExtentSize {
			return Extent{}, tip, fmt.Errorf("decision %d exceeds archive extent limit", decision.Slot)
		}
		selected = append(selected, decision)
		encodedDecisions += len(encoded)
		endPrefix = candidatePrefix
		if decision.Slot >= through {
			break
		}
	}
	end := selected[len(selected)-1].Slot
	extent := Extent{ConfigID: m.configID, Start: from, End: end, StartPrefix: startPrefix, EndPrefix: endPrefix, PreviousHash: previous, Decisions: selected}
	data, err := json.Marshal(extent)
	if err != nil {
		return Extent{}, tip, err
	}
	if len(data) > maxExtentSize {
		return Extent{}, tip, fmt.Errorf("archive extent exceeds %d bytes", maxExtentSize)
	}
	hash := sha256.Sum256(data)
	extent.hash = hash
	return extent, tip, nil
}

func extentEncodedSize(extent Extent, decisionsBytes, decisions int) (int, error) {
	extent.Decisions = []quepaxa.DecidedValue{}
	header, err := json.Marshal(extent)
	if err != nil {
		return 0, err
	}
	if decisions > 1 {
		decisionsBytes += decisions - 1
	}
	return len(header) + decisionsBytes, nil
}

func (m *Manager) DecisionsFrom(from quepaxa.Slot, limit int) ([]quepaxa.DecidedValue, quepaxa.Slot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if from <= m.head.Base {
		return nil, m.tip, fmt.Errorf("%w: archive starts after checkpoint %d", quepaxa.ErrCompacted, m.head.Base)
	}
	if limit <= 0 {
		limit = 256
	}
	values := make([]quepaxa.DecidedValue, 0, limit)
	ctx, cancel := context.WithTimeout(context.Background(), archiveSyncTimeout)
	defer cancel()
	for i, ref := range m.extents {
		if ref.End < from {
			continue
		}
		extent, err := m.extentAtLocked(ctx, i)
		if err != nil {
			return nil, m.tip, err
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

// Cleanup removes obsolete immutable blocks after the grace period.
// Every block reachable from the current head is always retained.
func (m *Manager) Cleanup(ctx context.Context, grace time.Duration) error {
	if grace < 0 {
		return fmt.Errorf("archive GC grace period must not be negative")
	}
	m.mu.Lock()
	if err := m.loadLocked(ctx); err != nil {
		m.mu.Unlock()
		return err
	}
	compacted, err := m.compactExtents(ctx)
	if err != nil {
		m.mu.Unlock()
		return err
	}
	if len(compacted) < len(m.extents) {
		head := m.head
		previous := [32]byte{}
		for i := range compacted {
			extent := &compacted[i]
			extent.PreviousHash = previous
			data, err := json.Marshal(extent)
			if err != nil {
				m.mu.Unlock()
				return fmt.Errorf("encode compacted archive extent: %w", err)
			}
			if len(data) > maxExtentSize {
				m.mu.Unlock()
				return fmt.Errorf("compacted archive extent exceeds %d bytes", maxExtentSize)
			}
			hash := sha256.Sum256(data)
			extent.hash = hash
			if err := m.bucket.Upload(ctx, m.key(extentKey(hash)), bytes.NewReader(data)); err != nil {
				m.mu.Unlock()
				return err
			}
			previous = hash
		}
		head.TailHash = previous
		if err := m.publishHeadLocked(ctx, head); err != nil {
			m.mu.Unlock()
			return err
		}
		m.extents, m.head = nil, head
		for _, extent := range compacted {
			m.rememberExtentLocked(extent)
			extent.Decisions = nil
			m.extents = append(m.extents, extent)
		}
		if m.cas {
			if err := m.refreshPublishedHead(ctx, head); err != nil {
				m.mu.Unlock()
				return err
			}
		}
	}
	keep := make(map[string]struct{}, len(m.extents))
	for _, extent := range m.extents {
		keep[m.key(extentKey(extent.hash))] = struct{}{}
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

func (m *Manager) compactExtents(ctx context.Context) ([]Extent, error) {
	if len(m.extents) < 2 {
		return m.extents, nil
	}
	result := make([]Extent, 0, len(m.extents))
	var current Extent
	prefix := m.head.BasePrefix
	encodedDecisions := 0
	flush := func() {
		if len(current.Decisions) != 0 {
			result = append(result, current)
		}
	}
	for i := range m.extents {
		extent, err := m.extentAtLocked(ctx, i)
		if err != nil {
			return nil, err
		}
		for _, decision := range extent.Decisions {
			encoded, err := json.Marshal(decision)
			if err != nil {
				return nil, err
			}
			candidate := current
			if len(candidate.Decisions) == 0 {
				candidate = Extent{ConfigID: m.configID, Start: decision.Slot, StartPrefix: prefix}
			}
			candidate.End = decision.Slot
			candidate.EndPrefix = quepaxa.AdvancePrefixHash(prefix, decision.Slot, decision.Hash)
			nextSize, err := extentEncodedSize(candidate, encodedDecisions+len(encoded), len(current.Decisions)+1)
			if err != nil {
				return nil, err
			}
			if len(current.Decisions) == maxExtentItems || len(current.Decisions) != 0 && nextSize > maxExtentSize {
				flush()
				current, encodedDecisions = Extent{}, 0
			}
			if len(current.Decisions) == 0 {
				current = Extent{ConfigID: m.configID, Start: decision.Slot, StartPrefix: prefix}
			}
			current.Decisions = append(current.Decisions, decision)
			current.End = decision.Slot
			prefix = quepaxa.AdvancePrefixHash(prefix, decision.Slot, decision.Hash)
			current.EndPrefix = prefix
			encodedDecisions += len(encoded)
		}
	}
	flush()
	return result, nil
}

func (m *Manager) extentAtLocked(ctx context.Context, index int) (Extent, error) {
	ref := m.extents[index]
	if extent, ok := m.cache[ref.hash]; ok {
		m.touchExtentLocked(ref.hash)
		return extent, nil
	}
	extent, err := m.readExtent(ctx, ref.hash)
	if err != nil {
		return Extent{}, err
	}
	m.rememberExtentLocked(extent)
	return extent, nil
}

func (m *Manager) rememberExtentLocked(extent Extent) {
	if _, ok := m.cache[extent.hash]; !ok && len(m.cacheLRU) == maxCachedExtents {
		delete(m.cache, m.cacheLRU[0])
		m.cacheLRU = m.cacheLRU[1:]
	}
	m.cache[extent.hash] = extent
	m.touchExtentLocked(extent.hash)
}

func (m *Manager) touchExtentLocked(hash [32]byte) {
	for i, existing := range m.cacheLRU {
		if existing == hash {
			m.cacheLRU = append(m.cacheLRU[:i], m.cacheLRU[i+1:]...)
			break
		}
	}
	m.cacheLRU = append(m.cacheLRU, hash)
}

func (m *Manager) readExtent(ctx context.Context, expected [32]byte) (Extent, error) {
	data, err := m.readObject(ctx, m.key(extentKey(expected)), maxExtentSize)
	if err != nil {
		return Extent{}, err
	}
	if sha256.Sum256(data) != expected {
		return Extent{}, fmt.Errorf("archive extent integrity mismatch")
	}
	var extent Extent
	if err := decodePersistedJSON(data, &extent); err != nil {
		return Extent{}, err
	}
	if err := m.validateExtent(extent); err != nil {
		return Extent{}, err
	}
	extent.hash = expected
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
	if extent.ConfigID != m.configID || extent.Start == 0 || extent.End < extent.Start || len(extent.Decisions) == 0 || len(extent.Decisions) > maxExtentItems || int(extent.End-extent.Start+1) != len(extent.Decisions) {
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

func decodePersistedJSON(data []byte, value any) error {
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

func extentKey(hash [32]byte) string {
	return fmt.Sprintf("archive/blocks/%x.json", hash)
}

func (m *Manager) key(value string) string {
	if m.prefix == "" {
		return value
	}
	return m.prefix + "/" + value
}
