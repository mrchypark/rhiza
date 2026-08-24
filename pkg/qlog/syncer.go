package qlog

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"time"
)

// ObjectStore is the interface for object storage operations.
type ObjectStore interface {
	Upload(ctx context.Context, path string, r io.Reader) error
	Get(ctx context.Context, path string) (io.ReadCloser, error)
	Exists(ctx context.Context, path string) (bool, error)
}

// Syncer syncs local WAL segments to object storage.
type Syncer struct {
	wal      *WAL
	manifest *Manifest
	store    ObjectStore
	interval time.Duration
	stopCh   chan struct{}
}

// NewSyncer creates a new syncer.
func NewSyncer(wal *WAL, manifest *Manifest, store ObjectStore, interval time.Duration) *Syncer {
	return &Syncer{
		wal:      wal,
		manifest: manifest,
		store:    store,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

// Start starts the background sync loop.
func (s *Syncer) Start(ctx context.Context) {
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
					log.Printf("QLog sync error: %v", err)
				}
			}
		}
	}()
}

// Stop stops the sync loop.
func (s *Syncer) Stop() {
	close(s.stopCh)
}

// sync syncs unsynced segments to object storage.
func (s *Syncer) sync(ctx context.Context) error {
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

	return s.manifest.Save("qlog/manifest.json")
}

// syncSegment syncs a single segment to object storage.
func (s *Syncer) syncSegment(ctx context.Context, seg SegmentMeta) error {
	// Read segment from local WAL
	data, err := s.readSegment(seg.Index)
	if err != nil {
		return err
	}

	// Upload to object storage
	key := fmt.Sprintf("qlog/segments/seg_%03d.log", seg.Index)
	return s.store.Upload(ctx, key, bytes.NewReader(data))
}

// readSegment reads a segment file into memory.
func (s *Syncer) readSegment(index uint32) ([]byte, error) {
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
