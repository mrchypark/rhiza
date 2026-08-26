package qlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/thanos-io/objstore"
)

const (
	defaultObjStoreChunkSize = 1 << 20
	maxExtentChain           = 256
	maxExtentChainEntries    = 1 << 20
	maxManifestSize          = 64 << 20
	extentMagic              = "QEXT1"
)

type extent struct {
	Segment  uint32
	Offset   int64
	Previous [32]byte
	Data     []byte
}

// ObjStoreSyncer syncs local WAL to object storage.
type ObjStoreSyncer struct {
	wal                *WAL
	manifest           *Manifest
	bucket             objstore.Bucket
	prefix             string
	interval           time.Duration
	stopCh             chan struct{}
	stopOnce           sync.Once
	syncMu             sync.Mutex
	publishedTip       uint64
	manifestDirty      bool
	stateInitialized   bool
	chunkSize          int
	gcInterval         time.Duration
	gcGrace            time.Duration
	nextGC             time.Time
	currentManifestKey string
	validated          map[uint32]SegmentMeta
}

// NewObjStoreSyncer creates a new object storage syncer.
func NewObjStoreSyncer(wal *WAL, manifest *Manifest, bucket objstore.Bucket, prefix string, interval time.Duration) *ObjStoreSyncer {
	return &ObjStoreSyncer{
		wal: wal, manifest: manifest, bucket: bucket, prefix: prefix, interval: interval,
		stopCh: make(chan struct{}), chunkSize: defaultObjStoreChunkSize,
	}
}

// ConfigureGC enables periodic orphan cleanup during background syncs.
func (s *ObjStoreSyncer) ConfigureGC(interval, grace time.Duration) {
	s.gcInterval, s.gcGrace = interval, grace
}

// Start starts the background sync loop.
func (s *ObjStoreSyncer) Start(ctx context.Context) {
	if s.interval <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.stopCh:
				return
			case <-ticker.C:
				if err := s.Sync(ctx); err != nil {
					log.Printf("object storage sync error: %v", err)
				}
			}
		}
	}()
}

// Stop stops the sync loop.
func (s *ObjStoreSyncer) Stop() { s.stopOnce.Do(func() { close(s.stopCh) }) }

// Sync publishes new bytes and compacts long extent chains off the ACK path.
func (s *ObjStoreSyncer) Sync(ctx context.Context) error {
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	if err := s.syncThroughLocked(ctx, 0, true); err != nil {
		return err
	}
	s.maybeGCLocked(ctx)
	return nil
}

// SyncThrough publishes a manifest that covers through before returning.
func (s *ObjStoreSyncer) SyncThrough(ctx context.Context, through uint64) error {
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	return s.syncThroughLocked(ctx, through, false)
}

func (s *ObjStoreSyncer) syncThroughLocked(ctx context.Context, through uint64, compact bool) error {
	s.initializeStateLocked()
	validated := s.validated
	if compact {
		validated = make(map[uint32]SegmentMeta, len(s.validated))
		for index, segment := range s.validated {
			if segment.ExtentCount <= maxExtentChain {
				validated[index] = segment
			}
		}
	}
	snapshots, err := s.wal.SegmentSnapshotsSince(validated)
	if err != nil {
		return err
	}
	previous, mode, _, _ := s.manifest.Snapshot()
	old := make(map[uint32]SegmentMeta, len(previous))
	for _, segment := range previous {
		old[segment.Index] = segment
	}
	// The in-memory manifest may be one failed PUT ahead of object storage.
	// Retry from the last published snapshot so the missing delta is included.
	if s.manifestDirty {
		old = make(map[uint32]SegmentMeta, len(s.validated))
		for index, segment := range s.validated {
			old[index] = segment
		}
		if len(old) == 0 {
			mode = ""
		} else {
			mode = StorageModeExtentChainV1
		}
	}
	desired := make([]SegmentMeta, 0, len(snapshots))
	for _, snapshot := range snapshots {
		prior, exists := old[snapshot.Meta.Index]
		if len(snapshot.Data) == 0 {
			if !exists || prior != snapshot.Meta {
				return fmt.Errorf("validated segment %d no longer matches published WAL", snapshot.Meta.Index)
			}
			desired = append(desired, prior)
			continue
		}
		start, head, count := int64(0), [32]byte{}, uint32(0)
		if snapshot.Offset > 0 {
			validated, ok := s.validated[snapshot.Meta.Index]
			if !exists || !ok || prior != validated || snapshot.Offset != prior.Size || mode != StorageModeExtentChainV1 {
				return fmt.Errorf("segment %d delta does not extend published WAL", snapshot.Meta.Index)
			}
			start, head, count = snapshot.Offset, prior.ExtentHead, prior.ExtentCount
		} else if exists && mode == StorageModeExtentChainV1 {
			if prior.Size > int64(len(snapshot.Data)) {
				return fmt.Errorf("local segment %d does not extend published WAL", prior.Index)
			}
			if prior.Size > 0 {
				if prior.Hash != ([32]byte{}) && sha256.Sum256(snapshot.Data[:prior.Size]) != prior.Hash {
					return fmt.Errorf("local segment %d does not extend published WAL", prior.Index)
				}
				if prior.Hash == ([32]byte{}) {
					validated, ok := s.validated[prior.Index]
					if ok && validated != prior {
						return fmt.Errorf("validated segment %d no longer matches published WAL", prior.Index)
					} else if !ok {
						if err := s.verifyExtentPrefix(ctx, prior, snapshot.Data[:prior.Size]); err != nil {
							return err
						}
					}
				}
			}
			start, head, count = prior.Size, prior.ExtentHead, prior.ExtentCount
			if compact && count > maxExtentChain {
				start, head, count = 0, [32]byte{}, 0
			}
		}
		if start > 0 && (head == ([32]byte{}) || count == 0) {
			return fmt.Errorf("published segment %d has no extent chain", prior.Index)
		}
		data := snapshot.Data
		if snapshot.Offset == 0 {
			data = data[start:]
		}
		head, count, err = s.uploadExtents(ctx, snapshot.Meta.Index, start, head, count, data)
		if err != nil {
			return err
		}
		meta := snapshot.Meta
		meta.ExtentHead, meta.ExtentCount, meta.Synced = head, count, true
		desired = append(desired, meta)
	}
	if s.manifest.ReplaceSynced(desired) {
		s.manifestDirty = true
	}
	if s.manifestDirty {
		key, err := s.saveManifest(ctx)
		if err != nil {
			return err
		}
		s.currentManifestKey = key
		s.publishedTip = s.manifest.Tip()
		s.manifestDirty = false
	}
	if s.publishedTip < through {
		return fmt.Errorf("published QLog tip %d is behind required slot %d", s.publishedTip, through)
	}
	s.validated = make(map[uint32]SegmentMeta, len(desired))
	for _, segment := range desired {
		s.validated[segment.Index] = segment
	}
	return nil
}

func (s *ObjStoreSyncer) uploadExtents(ctx context.Context, segment uint32, offset int64, head [32]byte, count uint32, data []byte) ([32]byte, uint32, error) {
	for len(data) > 0 {
		size := min(s.chunkSize, len(data))
		encoded := encodeExtent(extent{Segment: segment, Offset: offset, Previous: head, Data: data[:size]})
		hash := sha256.Sum256(encoded)
		if err := s.bucket.Upload(ctx, s.key(extentObjectKey(hash)), bytes.NewReader(encoded)); err != nil {
			return head, count, fmt.Errorf("upload segment %d extent %d: %w", segment, offset, err)
		}
		head, count, offset, data = hash, count+1, offset+int64(size), data[size:]
	}
	return head, count, nil
}

func encodeExtent(value extent) []byte {
	data := make([]byte, 0, len(extentMagic)+4+8+4+32+len(value.Data))
	data = append(data, extentMagic...)
	data = binary.LittleEndian.AppendUint32(data, value.Segment)
	data = binary.LittleEndian.AppendUint64(data, uint64(value.Offset))
	data = binary.LittleEndian.AppendUint32(data, uint32(len(value.Data)))
	data = append(data, value.Previous[:]...)
	return append(data, value.Data...)
}

func decodeExtent(data []byte) (extent, error) {
	header := len(extentMagic) + 4 + 8 + 4 + 32
	if len(data) < header || string(data[:len(extentMagic)]) != extentMagic {
		return extent{}, fmt.Errorf("invalid extent header")
	}
	size := int(binary.LittleEndian.Uint32(data[len(extentMagic)+12 : len(extentMagic)+16]))
	if size <= 0 || size > defaultObjStoreChunkSize || len(data) != header+size {
		return extent{}, fmt.Errorf("invalid extent size")
	}
	value := extent{
		Segment: binary.LittleEndian.Uint32(data[len(extentMagic):]),
		Offset:  int64(binary.LittleEndian.Uint64(data[len(extentMagic)+4:])),
		Data:    data[header:],
	}
	copy(value.Previous[:], data[len(extentMagic)+16:header])
	if value.Offset < 0 {
		return extent{}, fmt.Errorf("invalid extent offset")
	}
	return value, nil
}

func extentObjectKey(hash [32]byte) string { return fmt.Sprintf("qlog/extents/%x.ext", hash) }

func (s *ObjStoreSyncer) saveManifest(ctx context.Context) (string, error) {
	s.manifest.mu.RLock()
	data, err := json.Marshal(s.manifest)
	generation := s.manifest.Generation
	s.manifest.mu.RUnlock()
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(data)
	key := s.key(fmt.Sprintf("qlog/manifests/%020d_%x.json", generation, hash))
	if err := s.bucket.Upload(ctx, key, bytes.NewReader(data)); err != nil {
		return "", err
	}
	return key, nil
}

// LoadManifest loads the highest immutable generation, then falls back to the legacy mutable key.
func (s *ObjStoreSyncer) LoadManifest(ctx context.Context) error {
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	loaded, key, err := s.loadLatestManifest(ctx)
	if err != nil || loaded == nil {
		return err
	}
	s.manifest.mu.Lock()
	s.manifest.Version = loaded.Version
	s.manifest.Generation = loaded.Generation
	s.manifest.StorageMode = loaded.StorageMode
	s.manifest.Segments = append(s.manifest.Segments[:0], loaded.Segments...)
	s.manifest.TipSlot = loaded.TipSlot
	s.manifest.LastSync = loaded.LastSync
	s.manifest.mu.Unlock()
	s.publishedTip, s.currentManifestKey, s.stateInitialized = loaded.TipSlot, key, true
	return nil
}

func (s *ObjStoreSyncer) loadLatestManifest(ctx context.Context) (*Manifest, string, error) {
	var best string
	var bestGeneration uint64
	var forked bool
	if err := s.bucket.Iter(ctx, s.key("qlog/manifests"), func(name string) error {
		generation, _, ok := parseManifestKey(name)
		if !ok {
			return nil
		}
		if best == "" || generation > bestGeneration {
			best, bestGeneration, forked = name, generation, false
		} else if generation == bestGeneration && name != best {
			forked = true
		}
		return nil
	}); err != nil {
		return nil, "", err
	}
	if forked {
		return nil, "", fmt.Errorf("conflicting object-store manifests at generation %d", bestGeneration)
	}
	if best != "" {
		manifest, err := s.readManifest(ctx, best, true)
		return manifest, best, err
	}
	legacy := s.key("qlog/manifest.json")
	manifest, err := s.readManifest(ctx, legacy, false)
	if err != nil && s.bucket.IsObjNotFoundErr(err) {
		return nil, "", nil
	}
	return manifest, legacy, err
}

func (s *ObjStoreSyncer) readManifest(ctx context.Context, key string, verifyKey bool) (*Manifest, error) {
	r, err := s.bucket.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	data, err := io.ReadAll(io.LimitReader(r, maxManifestSize+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxManifestSize {
		return nil, fmt.Errorf("object-store manifest exceeds %d bytes", maxManifestSize)
	}
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, err
	}
	if verifyKey {
		generation, hash, ok := parseManifestKey(key)
		if !ok || generation != manifest.Generation || hash != sha256.Sum256(data) {
			return nil, fmt.Errorf("manifest key integrity mismatch")
		}
	}
	return &manifest, nil
}

func parseManifestKey(name string) (uint64, [32]byte, bool) {
	base := strings.TrimSuffix(path.Base(name), ".json")
	parts := strings.SplitN(base, "_", 2)
	if len(parts) != 2 || len(parts[1]) != 64 {
		return 0, [32]byte{}, false
	}
	generation, err := strconv.ParseUint(parts[0], 10, 64)
	decoded, hashErr := hex.DecodeString(parts[1])
	if err != nil || hashErr != nil || len(decoded) != 32 {
		return 0, [32]byte{}, false
	}
	var hash [32]byte
	copy(hash[:], decoded)
	return generation, hash, true
}

func (s *ObjStoreSyncer) initializeStateLocked() {
	if s.stateInitialized {
		return
	}
	_, _, generation, tip := s.manifest.Snapshot()
	s.publishedTip = tip
	if generation > 0 {
		s.manifest.mu.RLock()
		data, _ := json.Marshal(s.manifest)
		s.manifest.mu.RUnlock()
		s.currentManifestKey = s.key(fmt.Sprintf("qlog/manifests/%020d_%x.json", generation, sha256.Sum256(data)))
	}
	s.stateInitialized = true
}

// DownloadSegment reads a legacy whole-segment object.
func (s *ObjStoreSyncer) DownloadSegment(ctx context.Context, seg SegmentMeta) ([]byte, error) {
	r, err := s.bucket.Get(ctx, s.key(segmentObjectKey(seg)))
	if err != nil && s.bucket.IsObjNotFoundErr(err) {
		r, err = s.bucket.Get(ctx, s.key(fmt.Sprintf("qlog/segments/seg_%03d.log", seg.Index)))
	}
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

func segmentObjectKey(seg SegmentMeta) string {
	return fmt.Sprintf("qlog/segments/seg_%03d_%x.log", seg.Index, seg.Hash)
}

func (s *ObjStoreSyncer) downloadExtent(ctx context.Context, hash [32]byte) (extent, error) {
	r, err := s.bucket.Get(ctx, s.key(extentObjectKey(hash)))
	if err != nil {
		return extent{}, err
	}
	defer r.Close()
	data, err := io.ReadAll(io.LimitReader(r, int64(defaultObjStoreChunkSize+128)))
	if err != nil {
		return extent{}, err
	}
	if sha256.Sum256(data) != hash {
		return extent{}, fmt.Errorf("extent hash mismatch")
	}
	return decodeExtent(data)
}

func (s *ObjStoreSyncer) extentChainKeys(ctx context.Context, segment SegmentMeta) ([]string, error) {
	if segment.Size == 0 {
		return nil, nil
	}
	hash := segment.ExtentHead
	keys := make([]string, 0, segment.ExtentCount)
	for count := uint32(0); hash != ([32]byte{}); count++ {
		if count >= segment.ExtentCount || count >= maxExtentChainEntries {
			return nil, fmt.Errorf("segment %d extent chain exceeds declared count", segment.Index)
		}
		value, err := s.downloadExtent(ctx, hash)
		if err != nil {
			return nil, err
		}
		if value.Segment != segment.Index {
			return nil, fmt.Errorf("extent belongs to segment %d, want %d", value.Segment, segment.Index)
		}
		keys = append(keys, s.key(extentObjectKey(hash)))
		hash = value.Previous
	}
	if uint32(len(keys)) != segment.ExtentCount {
		return nil, fmt.Errorf("segment %d extent count mismatch", segment.Index)
	}
	return keys, nil
}

func (s *ObjStoreSyncer) verifyExtentPrefix(ctx context.Context, segment SegmentMeta, local []byte) error {
	hash, expectedEnd := segment.ExtentHead, segment.Size
	var count uint32
	for hash != ([32]byte{}) {
		if count >= segment.ExtentCount || count >= maxExtentChainEntries {
			return fmt.Errorf("segment %d extent chain exceeds declared count", segment.Index)
		}
		part, err := s.downloadExtent(ctx, hash)
		if err != nil {
			return err
		}
		if part.Segment != segment.Index || part.Offset+int64(len(part.Data)) != expectedEnd ||
			!bytes.Equal(local[part.Offset:expectedEnd], part.Data) {
			return fmt.Errorf("local segment %d does not match published WAL", segment.Index)
		}
		count++
		expectedEnd, hash = part.Offset, part.Previous
	}
	if count != segment.ExtentCount || expectedEnd != 0 {
		return fmt.Errorf("segment %d extent chain is incomplete", segment.Index)
	}
	return nil
}

// GarbageCollect performs persistent two-phase mark-and-sweep.
func (s *ObjStoreSyncer) GarbageCollect(ctx context.Context, grace time.Duration) error {
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	return s.garbageCollectLocked(ctx, time.Now().Add(-grace))
}

func (s *ObjStoreSyncer) maybeGCLocked(ctx context.Context) {
	if s.gcInterval <= 0 || time.Now().Before(s.nextGC) {
		return
	}
	s.nextGC = time.Now().Add(s.gcInterval)
	if err := s.garbageCollectLocked(ctx, time.Now().Add(-s.gcGrace)); err != nil {
		log.Printf("object storage GC error: %v", err)
	}
}

func (s *ObjStoreSyncer) garbageCollectLocked(ctx context.Context, cutoff time.Time) error {
	current, currentKey, err := s.loadLatestManifest(ctx)
	if err != nil || current == nil {
		return err
	}
	referenced := map[string]struct{}{currentKey: {}}
	if current.StorageMode == StorageModeExtentChainV1 {
		for _, segment := range current.Segments {
			keys, err := s.extentChainKeys(ctx, segment)
			if err != nil {
				return err
			}
			for _, key := range keys {
				referenced[key] = struct{}{}
			}
		}
	} else {
		for _, segment := range current.Segments {
			referenced[s.key(segmentObjectKey(segment))] = struct{}{}
			referenced[s.key(fmt.Sprintf("qlog/segments/seg_%03d.log", segment.Index))] = struct{}{}
		}
	}
	marks := make(map[string]struct{})
	if err := s.bucket.Iter(ctx, s.key("qlog/gc"), func(name string) error { marks[name] = struct{}{}; return nil }); err != nil {
		return err
	}
	// Marks measure time since an object became unreachable. Compaction may
	// create them before publication, so a live object must lose its mark.
	for name := range referenced {
		mark := s.gcMarkKey(name)
		if _, ok := marks[mark]; !ok {
			continue
		}
		if err := s.bucket.Delete(ctx, mark); err != nil && !s.bucket.IsObjNotFoundErr(err) {
			return err
		}
		delete(marks, mark)
	}
	var candidates []string
	for _, prefix := range []string{s.key("qlog/manifests"), s.key("qlog/extents"), s.key("qlog/chunks"), s.key("qlog/segments")} {
		if err := s.bucket.Iter(ctx, prefix, func(name string) error {
			if _, ok := referenced[name]; !ok && immutableObjectName(name) {
				candidates = append(candidates, name)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	for _, name := range candidates {
		mark := s.gcMarkKey(name)
		if _, exists := marks[mark]; !exists {
			if err := s.bucket.Upload(ctx, mark, strings.NewReader(name)); err != nil {
				return err
			}
			continue
		}
		attributes, err := s.bucket.Attributes(ctx, mark)
		if err != nil {
			return err
		}
		if attributes.LastModified.IsZero() || attributes.LastModified.After(cutoff) {
			continue
		}
		_, latestKey, err := s.loadLatestManifest(ctx)
		if err != nil || latestKey != currentKey {
			return err
		}
		if err := s.bucket.Delete(ctx, name); err != nil {
			return err
		}
		if err := s.bucket.Delete(ctx, mark); err != nil {
			return err
		}
	}
	return nil
}

func (s *ObjStoreSyncer) gcMarkKey(name string) string {
	return s.key(fmt.Sprintf("qlog/gc/%x.mark", sha256.Sum256([]byte(name))))
}

func immutableObjectName(name string) bool {
	base := path.Base(name)
	if strings.HasSuffix(base, ".json") {
		_, _, ok := parseManifestKey(name)
		return ok
	}
	for _, suffix := range []string{".ext", ".chunk"} {
		if strings.HasSuffix(base, suffix) && len(strings.TrimSuffix(base, suffix)) == 64 {
			return true
		}
	}
	parts := strings.Split(strings.TrimSuffix(base, ".log"), "_")
	return strings.HasSuffix(base, ".log") && len(parts) == 3 && len(parts[2]) == 64
}

func (s *ObjStoreSyncer) key(name string) string {
	if s.prefix == "" {
		return name
	}
	return s.prefix + "/" + name
}
