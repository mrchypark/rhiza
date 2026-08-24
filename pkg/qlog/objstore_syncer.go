package qlog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/thanos-io/objstore"
)

// ObjStoreSyncer syncs local WAL to object storage.
type ObjStoreSyncer struct {
	wal      *WAL
	manifest *Manifest
	bucket   objstore.Bucket
	prefix   string
	interval time.Duration
	stopCh   chan struct{}
	stopOnce sync.Once
}

// NewObjStoreSyncer creates a new object storage syncer.
func NewObjStoreSyncer(wal *WAL, manifest *Manifest, bucket objstore.Bucket, prefix string, interval time.Duration) *ObjStoreSyncer {
	return &ObjStoreSyncer{
		wal:      wal,
		manifest: manifest,
		bucket:   bucket,
		prefix:   prefix,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
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
func (s *ObjStoreSyncer) Stop() {
	s.stopOnce.Do(func() { close(s.stopCh) })
}

// Sync uploads only new or changed segments, then publishes their manifest.
func (s *ObjStoreSyncer) Sync(ctx context.Context) error {
	snapshots, err := s.wal.SegmentSnapshots()
	if err != nil {
		return err
	}
	changed := false
	for _, snapshot := range snapshots {
		if s.manifest.IsSynced(snapshot.Meta) {
			continue
		}
		if err := s.syncSegment(ctx, snapshot.Meta, snapshot.Data); err != nil {
			return fmt.Errorf("sync segment %d: %w", snapshot.Meta.Index, err)
		}
		s.manifest.UpsertSynced(snapshot.Meta)
		changed = true
	}
	if !changed {
		return nil
	}
	return s.saveManifest(ctx)
}

// syncSegment syncs a single segment to object storage.
func (s *ObjStoreSyncer) syncSegment(ctx context.Context, seg SegmentMeta, data []byte) error {
	key := s.key(fmt.Sprintf("qlog/segments/seg_%03d.log", seg.Index))
	return s.bucket.Upload(ctx, key, bytes.NewReader(data))
}

// saveManifest saves the manifest to object storage.
func (s *ObjStoreSyncer) saveManifest(ctx context.Context) error {
	s.manifest.mu.RLock()
	data, err := json.MarshalIndent(s.manifest, "", "  ")
	s.manifest.mu.RUnlock()

	if err != nil {
		return err
	}

	key := s.key("qlog/manifest.json")
	return s.bucket.Upload(ctx, key, bytes.NewReader(data))
}

// LoadManifest loads the manifest from object storage.
func (s *ObjStoreSyncer) LoadManifest(ctx context.Context) error {
	key := s.key("qlog/manifest.json")
	r, err := s.bucket.Get(ctx, key)
	if err != nil {
		if s.bucket.IsObjNotFoundErr(err) {
			return nil // no manifest yet
		}
		return err
	}
	defer r.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	s.manifest.mu.Lock()
	defer s.manifest.mu.Unlock()

	return json.Unmarshal(data, s.manifest)
}

// DownloadSegment downloads a segment from object storage.
func (s *ObjStoreSyncer) DownloadSegment(ctx context.Context, index uint32) ([]byte, error) {
	key := s.key(fmt.Sprintf("qlog/segments/seg_%03d.log", index))
	r, err := s.bucket.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return io.ReadAll(r)
}

// key prepends the prefix to the path.
func (s *ObjStoreSyncer) key(path string) string {
	if s.prefix == "" {
		return path
	}
	return s.prefix + "/" + path
}
