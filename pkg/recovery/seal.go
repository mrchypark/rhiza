package recovery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/thanos-io/objstore"
)

type archiveSealIntent struct {
	Version     int    `json:"version"`
	OperationID string `json:"operation_id"`
}

// Seal permanently stops archive publication for prefix. The caller must
// already have fenced old writers; this CAS only prevents them from making a
// stale archive head authoritative after that fence.
func Seal(ctx context.Context, bucket objstore.Bucket, prefix, operationID string) error {
	if bucket == nil || !supportsCAS(bucket) {
		return fmt.Errorf("archive seal requires conditional object writes")
	}
	if operationID == "" {
		return fmt.Errorf("archive seal requires an operation ID")
	}
	m := NewManager(bucket, prefix, 1)
	defer m.Close()
	if err := m.Load(ctx); err != nil {
		return fmt.Errorf("load archive for seal: %w", err)
	}
	m.mu.Lock()
	head, version := m.head, m.headCAS
	m.mu.Unlock()
	if head.Sealed {
		return verifySealIntent(ctx, m, operationID)
	}
	if version == nil || head.Generation == 0 {
		return fmt.Errorf("archive seal requires an authoritative head")
	}
	if err := acquireSealIntent(ctx, m, operationID); err != nil {
		return err
	}

	m.transitionMu.Lock()
	defer m.transitionMu.Unlock()
	var refs []Extent
	for range maxPublishRetries {
		if err := m.Load(ctx); err != nil {
			return err
		}
		m.mu.Lock()
		head, refs, version = m.head, append([]Extent(nil), m.extents...), m.headCAS
		m.mu.Unlock()
		if head.Sealed {
			return verifySealIntent(ctx, m, operationID)
		}
		if version == nil || head.Generation == 0 {
			return fmt.Errorf("archive seal requires an authoritative head")
		}
		head.Sealed = true
		if err := m.publishHead(ctx, head, version); err == nil {
			return m.refreshPublishedHead(ctx, head, version, refs)
		} else if !m.bucket.IsConditionNotMetErr(err) {
			return err
		}
	}
	return fmt.Errorf("archive seal conflicted too many times")
}

func (m *Manager) sealIntentKey() string { return m.key("archive/seal.json") }

func acquireSealIntent(ctx context.Context, m *Manager, operationID string) error {
	want := archiveSealIntent{Version: 1, OperationID: operationID}
	data, err := json.Marshal(want)
	if err != nil {
		return err
	}
	if err := m.bucket.Upload(ctx, m.sealIntentKey(), bytes.NewReader(data), objstore.WithIfNotExists()); err == nil {
		return nil
	} else if !m.bucket.IsConditionNotMetErr(err) {
		return err
	}
	return verifySealIntent(ctx, m, operationID)
}

func verifySealIntent(ctx context.Context, m *Manager, operationID string) error {
	r, err := m.bucket.Get(ctx, m.sealIntentKey())
	if err != nil {
		return fmt.Errorf("sealed archive lacks matching recovery operation: %w", err)
	}
	data, readErr := io.ReadAll(io.LimitReader(r, maxHeadSize+1))
	closeErr := r.Close()
	if readErr != nil || closeErr != nil || len(data) > maxHeadSize {
		return fmt.Errorf("read archive seal intent")
	}
	var got archiveSealIntent
	if err := json.Unmarshal(data, &got); err != nil || got.Version != 1 || got.OperationID == "" || got.OperationID != operationID {
		return fmt.Errorf("archive is sealed by a different recovery operation")
	}
	canonical, _ := json.Marshal(got)
	if !bytes.Equal(canonical, data) {
		return fmt.Errorf("invalid archive seal intent")
	}
	return nil
}
