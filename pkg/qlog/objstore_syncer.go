package qlog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
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
				if err := s.sync(ctx); err != nil {
					log.Printf("object storage sync error: %v", err)
				}
			}
		}
	}()
}

// Stop stops the sync loop.
func (s *ObjStoreSyncer) Stop() {
	close(s.stopCh)
}

// sync syncs unsynced segments to object storage.
func (s *ObjStoreSyncer) sync(ctx context.Context) error {
	unsynced := s.manifest.Unsynced()
	if len(unsynced) == 0 {
		return nil
	}

	for _, seg := range unsynced {
		if err := s.syncSegment(ctx, seg); err != nil {
			return fmt.Errorf("sync segment %d: %w", seg.Index, err)
		}
		s.manifest.MarkSynced(seg.Index)
	}

	// Save manifest
	return s.saveManifest(ctx)
}

// syncSegment syncs a single segment to object storage.
func (s *ObjStoreSyncer) syncSegment(ctx context.Context, seg SegmentMeta) error {
	// Read segment from local WAL
	data, err := s.readSegment(seg.Index)
	if err != nil {
		return err
	}

	// Upload to object storage
	key := s.key(fmt.Sprintf("qlog/segments/seg_%03d.log", seg.Index))
	return s.bucket.Upload(ctx, key, bytes.NewReader(data))
}

// readSegment reads a segment file into memory.
func (s *ObjStoreSyncer) readSegment(index uint32) ([]byte, error) {
	s.wal.mu.RLock()
	defer s.wal.mu.RUnlock()

	for _, seg := range s.wal.segments {
		if seg.index == index {
			seg.mu.Lock()
			defer seg.mu.Unlock()

			data := make([]byte, seg.offset)
			if _, err := seg.file.ReadAt(data, 0); err != nil {
				return nil, err
			}
			return data, nil
		}
	}

	return nil, fmt.Errorf("segment %d not found", index)
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
