package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
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

var (
	ErrArchiveClosed       = errors.New("archive manager is closed")
	errArchiveStateChanged = errors.New("archive state changed during I/O")
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
	ConfigID     uint `json:"config_id"`
	Generation   uint64
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
	bucket       objstore.Bucket
	prefix       string
	configID     uint
	mu           sync.Mutex
	transitionMu sync.Mutex
	extents      []Extent
	cache        map[[32]byte]Extent
	cacheLRU     [][32]byte
	cacheMu      sync.Mutex
	head         archiveHead
	tip          quepaxa.Slot
	headCAS      *objstore.ObjectVersion
	cas          bool
	batchMu      sync.Mutex
	batch        *syncBatch
	ctx          context.Context
	cancel       context.CancelFunc
	batchWG      sync.WaitGroup
	groupDelay   time.Duration
	closed       bool
	gcMu         sync.Mutex
	readMu       sync.Mutex
	readCond     *sync.Cond
	readers      int
}

func NewManager(bucket objstore.Bucket, prefix string, configID uint) *Manager {
	options := bucket.SupportedObjectUploadOptions()
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{bucket: bucket, prefix: prefix, configID: configID, cas: slices.Contains(options, objstore.IfMatch) && slices.Contains(options, objstore.IfNotExists), cache: make(map[[32]byte]Extent), ctx: ctx, cancel: cancel, groupDelay: archiveGroupDelay}
	m.readCond = sync.NewCond(&m.readMu)
	return m
}

// SetGroupDelay selects the maximum linger used to coalesce archive writes.
func (m *Manager) SetGroupDelay(delay time.Duration) {
	if delay > 0 {
		m.groupDelay = delay
	}
}

func (m *Manager) Close() {
	m.batchMu.Lock()
	if !m.closed {
		m.closed = true
		m.cancel()
	}
	m.batchMu.Unlock()
	m.batchWG.Wait()
}

func (m *Manager) Load(ctx context.Context) error {
	for range maxPublishRetries {
		if err := m.loadLocked(ctx); !errors.Is(err, errArchiveStateChanged) {
			return err
		}
	}
	return fmt.Errorf("shared archive state changed too often")
}

func (m *Manager) loadLocked(ctx context.Context) error {
	m.mu.Lock()
	oldExtents, oldHead, oldCAS := slices.Clone(m.extents), m.head, m.headCAS
	m.mu.Unlock()
	name := m.key("archive/head.bin")
	attributes, headData, unchanged, err := m.readStableHead(ctx, name, oldCAS)
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
		if oldHead.Tip != 0 {
			return fmt.Errorf("shared archive head disappeared")
		}
		m.mu.Lock()
		if !sameNullableObjectVersion(m.headCAS, oldCAS) {
			m.mu.Unlock()
			return errArchiveStateChanged
		}
		m.extents, m.head, m.tip, m.headCAS = nil, archiveHead{ConfigID: m.configID}, 0, nil
		m.mu.Unlock()
		return nil
	}
	if unchanged {
		return nil
	}
	head, err := decodeHead(headData)
	if err != nil {
		return err
	}
	if head.ConfigID != m.configID || head.Generation == 0 || head.Tip < head.Base || (head.Tip > head.Base) != (head.TailHash != [32]byte{}) {
		return fmt.Errorf("invalid shared archive head")
	}
	if head.Base > 0 && (head.BaseSeal == nil || head.BaseDecision == nil || head.BaseSeal.Index != head.Base || head.BaseSeal.PrefixHash != head.BasePrefix) {
		return fmt.Errorf("invalid shared archive recovery base")
	}
	extents := make([]Extent, 0)
	if head.Generation < oldHead.Generation || head.Base < oldHead.Base || head.Tip < oldHead.Tip || oldHead.ConfigID != 0 && head.Base == oldHead.Base && !archiveBaseEqual(head, oldHead) {
		return fmt.Errorf("shared archive head regressed or changed recovery base")
	}
	known := make(map[[32]byte]int, len(oldExtents))
	if head.Base >= oldHead.Base {
		for i, ref := range oldExtents {
			known[ref.hash] = i
		}
	}
	hash, end := head.TailHash, head.Tip
	reused := false
	for end > head.Base {
		if hash == ([32]byte{}) {
			return fmt.Errorf("invalid shared archive block chain")
		}
		if index, ok := known[hash]; ok && oldExtents[index].End == end {
			prefix := make([]Extent, 0, index+1)
			for _, ref := range oldExtents[:index+1] {
				if ref.End > head.Base {
					prefix = append(prefix, ref)
				}
			}
			slices.Reverse(extents)
			extents = append(prefix, extents...)
			end = head.Base
			reused = true
			break
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
	if !reused {
		slices.Reverse(extents)
	}
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
	finalAttributes, err := m.bucket.Attributes(ctx, name)
	if err != nil || !sameObjectVersion(attributes.Version, finalAttributes.Version) {
		if err != nil {
			return err
		}
		return fmt.Errorf("shared archive head changed during chain load")
	}
	m.mu.Lock()
	if !sameNullableObjectVersion(m.headCAS, oldCAS) {
		m.mu.Unlock()
		return errArchiveStateChanged
	}
	m.extents, m.head, m.tip, m.headCAS = extents, head, head.Tip, finalAttributes.Version
	m.mu.Unlock()
	return nil
}

func (m *Manager) readStableHead(ctx context.Context, name string, known *objstore.ObjectVersion) (objstore.ObjectAttributes, []byte, bool, error) {
	for range maxPublishRetries {
		before, err := m.bucket.Attributes(ctx, name)
		if err != nil {
			return objstore.ObjectAttributes{}, nil, false, err
		}
		if sameObjectVersion(known, before.Version) {
			return before, nil, true, nil
		}
		data, err := m.readObject(ctx, name, maxHeadSize)
		if err != nil {
			return objstore.ObjectAttributes{}, nil, false, err
		}
		after, err := m.bucket.Attributes(ctx, name)
		if err != nil {
			return objstore.ObjectAttributes{}, nil, false, err
		}
		if sameObjectVersion(before.Version, after.Version) {
			return after, data, false, nil
		}
	}
	return objstore.ObjectAttributes{}, nil, false, fmt.Errorf("shared archive head did not stabilize")
}

func sameObjectVersion(a, b *objstore.ObjectVersion) bool {
	return a != nil && b != nil && *a == *b
}

func sameNullableObjectVersion(a, b *objstore.ObjectVersion) bool {
	return a == nil && b == nil || sameObjectVersion(a, b)
}

func archiveBaseEqual(a, b archiveHead) bool {
	if a.Base != b.Base || a.BasePrefix != b.BasePrefix {
		return false
	}
	a.Generation, a.Base, a.BasePrefix, a.Tip, a.TailHash = 0, 0, [32]byte{}, 0, [32]byte{}
	b.Generation, b.Base, b.BasePrefix, b.Tip, b.TailHash = 0, 0, [32]byte{}, 0, [32]byte{}
	return archiveHeadsEqual(a, b)
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
	m.transitionMu.Lock()
	defer m.transitionMu.Unlock()
	for attempt := 0; attempt < maxPublishRetries; attempt++ {
		m.mu.Lock()
		head, tip, refs, headCAS := m.head, m.tip, slices.Clone(m.extents), m.headCAS
		m.mu.Unlock()
		if through <= head.Base {
			return nil
		}
		if through > tip {
			return fmt.Errorf("archive tip %d is behind trim slot %d", tip, through)
		}
		sealCopy, decisionCopy := sealed.CheckpointSeal, decision
		head.Base, head.BasePrefix, head.BaseSeal, head.BaseDecision = through, prefix, &sealCopy, &decisionCopy
		head.Generation++
		extents := make([]Extent, 0, len(refs))
		for _, extent := range refs {
			if extent.End <= through {
				continue
			}
			if extent.Start <= through {
				extent.Start = through + 1
				extent.StartPrefix = prefix
			}
			extents = append(extents, extent)
		}
		if through == tip {
			head.TailHash = [32]byte{}
		}
		if err := m.publishHead(ctx, head, headCAS); err == nil {
			if m.cas {
				return m.refreshPublishedHead(ctx, head)
			}
			m.mu.Lock()
			m.extents, m.head = extents, head
			m.mu.Unlock()
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
	if err := m.loadLocked(ctx); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.head.Base < expected.Base || m.head.Tip < expected.Tip {
		return fmt.Errorf("published archive head regressed")
	}
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
	if m.closed {
		m.batchMu.Unlock()
		return ErrArchiveClosed
	}
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
	m.batchWG.Add(1)
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
	defer m.batchWG.Done()
	timer := time.NewTimer(m.groupDelay)
	select {
	case <-m.ctx.Done():
		timer.Stop()
		batch.err = ErrArchiveClosed
	case <-timer.C:
		m.batchMu.Lock()
		target := batch.target
		m.batchMu.Unlock()
		ctx, cancel := context.WithTimeout(m.ctx, archiveSyncTimeout)
		batch.err = m.syncNow(ctx, core, target)
		cancel()
	}
	m.batchMu.Lock()
	if m.batch == batch {
		m.batch = nil
	}
	close(batch.done)
	m.batchMu.Unlock()
}

func (m *Manager) syncNow(ctx context.Context, core source, through quepaxa.Slot) error {
	m.transitionMu.Lock()
	defer m.transitionMu.Unlock()
	for attempt := 0; attempt < maxPublishRetries; attempt++ {
		m.mu.Lock()
		tip, head, refs, headCAS := m.tip, m.head, slices.Clone(m.extents), m.headCAS
		m.mu.Unlock()
		if tip >= through {
			return nil
		}
		added := make([]Extent, 0, 1)
		head.ConfigID = m.configID
		previous := head.TailHash
		from := tip + 1
		for from <= through {
			extent, sourceTip, err := m.buildExtent(core, from, through, previous)
			if err != nil {
				return err
			}
			data, err := encodeExtent(extent)
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
		head.Generation++
		if err := m.publishHead(ctx, head, headCAS); err == nil {
			if m.cas {
				return m.refreshPublishedHead(ctx, head)
			}
			refs = append(refs, added...)
			for _, extent := range added {
				m.rememberExtent(extent)
			}
			for i := range refs {
				refs[i].Decisions = nil
			}
			m.mu.Lock()
			m.extents, m.head, m.tip = refs, head, head.Tip
			m.mu.Unlock()
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

func (m *Manager) publishHead(ctx context.Context, head archiveHead, headCAS *objstore.ObjectVersion) error {
	data, err := encodeHead(head)
	if err != nil {
		return err
	}
	if len(data) > maxHeadSize {
		return fmt.Errorf("archive head exceeds %d bytes", maxHeadSize)
	}
	var options []objstore.ObjectUploadOption
	if m.cas {
		if headCAS == nil {
			options = append(options, objstore.WithIfNotExists())
		} else {
			options = append(options, objstore.WithIfMatch(headCAS))
		}
	}
	return m.bucket.Upload(ctx, m.key("archive/head.bin"), bytes.NewReader(data), options...)
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
		encodedSize := archiveDecisionSize(decision)
		candidatePrefix, ok := core.PrefixHash(decision.Slot)
		if !ok {
			return Extent{}, tip, fmt.Errorf("archive end prefix %d is unavailable", decision.Slot)
		}
		candidate := Extent{ConfigID: m.configID, Start: from, End: decision.Slot, StartPrefix: startPrefix, EndPrefix: candidatePrefix, PreviousHash: previous, Decisions: []quepaxa.DecidedValue{}}
		size, err := extentEncodedSize(candidate, encodedDecisions+encodedSize, len(selected)+1)
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
		encodedDecisions += encodedSize
		endPrefix = candidatePrefix
		if decision.Slot >= through {
			break
		}
	}
	end := selected[len(selected)-1].Slot
	extent := Extent{ConfigID: m.configID, Start: from, End: end, StartPrefix: startPrefix, EndPrefix: endPrefix, PreviousHash: previous, Decisions: selected}
	data, err := encodeExtent(extent)
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
	if decisions <= 0 || decisions > maxExtentItems {
		return 0, fmt.Errorf("invalid archive extent item count")
	}
	return extentHeaderSize + decisionsBytes + archiveCRCSize, nil
}

func (m *Manager) DecisionsFrom(ctx context.Context, from quepaxa.Slot, limit int) ([]quepaxa.DecidedValue, quepaxa.Slot, error) {
	m.mu.Lock()
	if from <= m.head.Base {
		tip, base := m.tip, m.head.Base
		m.mu.Unlock()
		return nil, tip, fmt.Errorf("%w: archive starts after checkpoint %d", quepaxa.ErrCompacted, base)
	}
	m.readMu.Lock()
	m.readers++
	m.readMu.Unlock()
	refs, tip := slices.Clone(m.extents), m.tip
	m.mu.Unlock()
	defer m.endRead()
	if limit <= 0 {
		limit = 256
	}
	values := make([]quepaxa.DecidedValue, 0, limit)
	for _, ref := range refs {
		if ref.End < from {
			continue
		}
		extent, err := m.extentForRef(ctx, ref)
		if err != nil {
			return nil, tip, err
		}
		for _, decision := range extent.Decisions {
			if decision.Slot < from {
				continue
			}
			if decision.Slot != from+quepaxa.Slot(len(values)) {
				return nil, tip, fmt.Errorf("shared archive decision gap")
			}
			values = append(values, decision)
			if len(values) == limit {
				return values, tip, nil
			}
		}
	}
	return values, tip, nil
}

func (m *Manager) endRead() {
	m.readMu.Lock()
	m.readers--
	if m.readers == 0 {
		m.readCond.Broadcast()
	}
	m.readMu.Unlock()
}

func (m *Manager) waitReaders() {
	m.readMu.Lock()
	for m.readers != 0 {
		m.readCond.Wait()
	}
	m.readMu.Unlock()
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
	m.gcMu.Lock()
	defer m.gcMu.Unlock()
	settled := false
	for attempt := 0; attempt < maxPublishRetries; attempt++ {
		if err := m.Load(ctx); err != nil {
			return err
		}
		m.mu.Lock()
		refs, snapshotHead, snapshotCAS := slices.Clone(m.extents), m.head, m.headCAS
		m.mu.Unlock()
		compacted, err := m.compactExtents(ctx, refs, snapshotHead.BasePrefix)
		if err != nil {
			return err
		}
		if len(compacted) >= len(refs) {
			settled = true
			break
		}
		head := snapshotHead
		previous := [32]byte{}
		for i := range compacted {
			extent := &compacted[i]
			extent.PreviousHash = previous
			data, err := encodeExtent(*extent)
			if err != nil {
				return fmt.Errorf("encode compacted archive extent: %w", err)
			}
			if len(data) > maxExtentSize {
				return fmt.Errorf("compacted archive extent exceeds %d bytes", maxExtentSize)
			}
			hash := sha256.Sum256(data)
			extent.hash = hash
			if err := m.bucket.Upload(ctx, m.key(extentKey(hash)), bytes.NewReader(data)); err != nil {
				return err
			}
			previous = hash
		}
		head.TailHash = previous
		head.Generation++
		if err := m.publishHead(ctx, head, snapshotCAS); err != nil {
			if m.cas {
				continue
			}
			return err
		}
		if m.cas {
			if err := m.refreshPublishedHead(ctx, head); err != nil {
				return err
			}
		} else {
			newRefs := make([]Extent, 0, len(compacted))
			for _, extent := range compacted {
				m.rememberExtent(extent)
				extent.Decisions = nil
				newRefs = append(newRefs, extent)
			}
			m.mu.Lock()
			m.extents, m.head = newRefs, head
			m.mu.Unlock()
		}
		verifier := NewManager(m.bucket, m.prefix, m.configID)
		if err := verifier.Load(ctx); err != nil {
			verifier.Close()
			return fmt.Errorf("verify compacted archive: %w", err)
		}
		if verifier.head.Base < head.Base || verifier.head.Tip < head.Tip {
			verifier.Close()
			return fmt.Errorf("compacted archive head regressed during verification")
		}
		verifier.Close()
		settled = true
		break
	}
	if !settled {
		return fmt.Errorf("archive cleanup conflicted too many times")
	}
	m.mu.Lock()
	keep := make(map[string]struct{}, len(m.extents))
	for _, extent := range m.extents {
		keep[m.key(extentKey(extent.hash))] = struct{}{}
	}
	cutoff := time.Now().Add(-grace)
	m.mu.Unlock()
	m.waitReaders()
	for _, dir := range []string{"archive/manifests", "archive/blocks"} {
		if err := m.bucket.Iter(ctx, m.key(dir), func(name string) error {
			marker := m.gcMarkerKey(name)
			if _, ok := keep[name]; ok {
				if err := m.bucket.Delete(ctx, marker); err != nil && !m.bucket.IsObjNotFoundErr(err) {
					return err
				}
				return nil
			}
			attributes, err := m.bucket.Attributes(ctx, marker)
			if err != nil {
				if !m.bucket.IsObjNotFoundErr(err) {
					return err
				}
				err = m.bucket.Upload(ctx, marker, bytes.NewReader([]byte(name)), objstore.WithIfNotExists())
				if err != nil && !m.bucket.IsConditionNotMetErr(err) {
					return err
				}
				return nil
			}
			if attributes.LastModified.After(cutoff) {
				return nil
			}
			if err := m.bucket.Delete(ctx, name); err != nil && !m.bucket.IsObjNotFoundErr(err) {
				return err
			}
			if err := m.bucket.Delete(ctx, marker); err != nil && !m.bucket.IsObjNotFoundErr(err) {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) gcMarkerKey(name string) string {
	hash := sha256.Sum256([]byte(name))
	return m.key(fmt.Sprintf("archive/gc-candidates/%x", hash))
}

func (m *Manager) compactExtents(ctx context.Context, refs []Extent, prefix [32]byte) ([]Extent, error) {
	if len(refs) < 2 {
		return refs, nil
	}
	result := make([]Extent, 0, len(refs))
	var current Extent
	encodedDecisions := 0
	flush := func() {
		if len(current.Decisions) != 0 {
			result = append(result, current)
		}
	}
	for _, ref := range refs {
		extent, err := m.extentForRef(ctx, ref)
		if err != nil {
			return nil, err
		}
		for _, decision := range extent.Decisions {
			encodedSize := archiveDecisionSize(decision)
			candidate := current
			if len(candidate.Decisions) == 0 {
				candidate = Extent{ConfigID: m.configID, Start: decision.Slot, StartPrefix: prefix}
			}
			candidate.End = decision.Slot
			candidate.EndPrefix = quepaxa.AdvancePrefixHash(prefix, decision.Slot, decision.Hash)
			nextSize, err := extentEncodedSize(candidate, encodedDecisions+encodedSize, len(current.Decisions)+1)
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
			encodedDecisions += encodedSize
		}
	}
	flush()
	return result, nil
}

func archiveHeadsEqual(a, b archiveHead) bool {
	aData, _ := encodeHead(a)
	bData, _ := encodeHead(b)
	return bytes.Equal(aData, bData)
}

func (m *Manager) extentAtLocked(ctx context.Context, index int) (Extent, error) {
	return m.extentForRef(ctx, m.extents[index])
}

func (m *Manager) extentForRef(ctx context.Context, ref Extent) (Extent, error) {
	m.cacheMu.Lock()
	extent, ok := m.cache[ref.hash]
	if ok {
		m.touchExtentLocked(ref.hash)
	}
	m.cacheMu.Unlock()
	if !ok {
		var err error
		extent, err = m.readExtent(ctx, ref.hash)
		if err != nil {
			return Extent{}, err
		}
		m.rememberExtent(extent)
	}
	if ref.Start < extent.Start || ref.End != extent.End || ref.EndPrefix != extent.EndPrefix || ref.PreviousHash != extent.PreviousHash {
		return Extent{}, fmt.Errorf("invalid shared archive extent reference")
	}
	if ref.Start > extent.Start {
		offset := int(ref.Start - extent.Start)
		if offset >= len(extent.Decisions) {
			return Extent{}, fmt.Errorf("invalid shared archive extent reference")
		}
		extent.Decisions = slices.Clone(extent.Decisions[offset:])
		extent.Start = ref.Start
		extent.StartPrefix = ref.StartPrefix
	} else if ref.StartPrefix != extent.StartPrefix {
		return Extent{}, fmt.Errorf("invalid shared archive extent reference")
	}
	if err := m.validateExtent(extent); err != nil {
		return Extent{}, err
	}
	return extent, nil
}

func (m *Manager) rememberExtent(extent Extent) {
	m.cacheMu.Lock()
	defer m.cacheMu.Unlock()
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
	extent, err := decodeExtent(data)
	if err != nil {
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

func extentKey(hash [32]byte) string {
	return fmt.Sprintf("archive/blocks/%x.bin", hash)
}

func (m *Manager) key(value string) string {
	if m.prefix == "" {
		return value
	}
	return m.prefix + "/" + value
}
