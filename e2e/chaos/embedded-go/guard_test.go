package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/mrchypark/rhiza/pkg/recoveryanchor"
)

// fakeBackend simulates a Kubernetes ConfigMap with resourceVersion
// increment on every CAS. Implements recoveryanchor.Backend.
type fakeBackend struct {
	mu      sync.Mutex
	data    map[string]string
	version map[string]int
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{data: make(map[string]string), version: make(map[string]int)}
}

func (b *fakeBackend) seed(id string, rec recoveryanchor.Record) {
	b.mu.Lock()
	defer b.mu.Unlock()
	raw, _ := json.Marshal(rec)
	b.data[id] = string(raw)
	b.version[id] = 1
}

func (b *fakeBackend) Read(_ context.Context, id string) (recoveryanchor.Record, string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	raw, ok := b.data[id]
	if !ok {
		return recoveryanchor.Record{}, "", fmt.Errorf("anchor %q not found", id)
	}
	var rec recoveryanchor.Record
	if err := json.Unmarshal([]byte(raw), &rec); err != nil {
		return recoveryanchor.Record{}, "", err
	}
	return rec, fmt.Sprintf("v%d", b.version[id]), nil
}

func (b *fakeBackend) CAS(_ context.Context, id string, version string, rec recoveryanchor.Record) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	cur, ok := b.version[id]
	if !ok {
		return fmt.Errorf("anchor %q not found", id)
	}
	if version != fmt.Sprintf("v%d", cur) {
		return fmt.Errorf("CAS conflict: version mismatch")
	}
	raw, _ := json.Marshal(rec)
	b.data[id] = string(raw)
	b.version[id] = cur + 1
	return nil
}

func srcSID() string { return storageID("s3", "rhiza-minio:9000", "rhiza", "rhiza", "test-cluster") }

func TestGuardReserveClearCycle(t *testing.T) {
	b := newFakeBackend()
	const aid = "test-anchor"
	b.seed(aid, recoveryanchor.Record{
		Version: recoveryanchor.Version1, EvidenceFormat: "sql-epoch-token/v1",
		Binding: recoveryanchor.Binding{ClusterID: "test-cluster", StorageID: srcSID()},
		Evidence: []byte(`{"epoch":1,"token":"initial"}`), Generation: 0})
	guard := newAnchorGuard(b, aid, "test-cluster")
	ctx := context.Background()

	snap, err := guard.reserve(ctx)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if snap.generation != 0 {
		t.Errorf("generation = %d, want 0", snap.generation)
	}
	// Verify PendingWrite=true after reserve.
	rec, _, _ := b.Read(ctx, aid)
	if !rec.PendingWrite {
		t.Fatal("PendingWrite should be true after reserve")
	}

	if err := guard.clear(ctx, snap); err != nil {
		t.Fatalf("clear: %v", err)
	}
	rec, _, _ = b.Read(ctx, aid)
	if rec.PendingWrite {
		t.Fatal("PendingWrite should be false after clear")
	}
}

func TestGuardRejectsStaleBinding(t *testing.T) {
	b := newFakeBackend()
	const aid = "test-anchor"
	b.seed(aid, recoveryanchor.Record{
		Version: recoveryanchor.Version1, EvidenceFormat: "sql-epoch-token/v1",
		Binding: recoveryanchor.Binding{ClusterID: "test-cluster", StorageID: srcSID()},
		Evidence: []byte(`{"epoch":1,"token":"initial"}`), Generation: 0})
	guard := newAnchorGuard(b, aid, "test-cluster")

	snap, _ := guard.reserve(context.Background())

	// Tamper binding (simulates operator committed to different target).
	b.mu.Lock()
	raw, _ := json.Marshal(recoveryanchor.Record{
		Version: recoveryanchor.Version1, EvidenceFormat: "sql-epoch-token/v1",
		Binding: recoveryanchor.Binding{ClusterID: "other", StorageID: "other-sid"},
		Evidence: []byte(`{"epoch":1,"token":"initial"}`), PendingWrite: true, Generation: 0})
	b.data[aid] = string(raw)
	b.version[aid]++
	b.mu.Unlock()

	if err := guard.clear(context.Background(), snap); err == nil {
		t.Fatal("clear should fail with stale binding")
	}
}

func TestGuardRejectsPendingAlready(t *testing.T) {
	b := newFakeBackend()
	const aid = "test-anchor"
	b.seed(aid, recoveryanchor.Record{
		Version: recoveryanchor.Version1, EvidenceFormat: "sql-epoch-token/v1",
		Binding: recoveryanchor.Binding{ClusterID: "test-cluster", StorageID: srcSID()},
		Evidence: []byte(`{"epoch":1,"token":"initial"}`), PendingWrite: true, Generation: 0})
	guard := newAnchorGuard(b, aid, "test-cluster")
	_, err := guard.reserve(context.Background())
	if err == nil {
		t.Fatal("should reject when PendingWrite already true")
	}
}

func TestGuardRejectsWrongStorageID(t *testing.T) {
	b := newFakeBackend()
	const aid = "test-anchor"
	b.seed(aid, recoveryanchor.Record{
		Version: recoveryanchor.Version1, EvidenceFormat: "sql-epoch-token/v1",
		Binding: recoveryanchor.Binding{ClusterID: "test-cluster", StorageID: "wrong"},
		Evidence: []byte(`{"epoch":1,"token":"initial"}`), Generation: 0})
	guard := newAnchorGuard(b, aid, "test-cluster")
	_, err := guard.reserve(context.Background())
	if err == nil {
		t.Fatal("should reject wrong storage ID")
	}
}

func TestGuardRejectsTransition(t *testing.T) {
	b := newFakeBackend()
	const aid = "test-anchor"
	b.seed(aid, recoveryanchor.Record{
		Version: recoveryanchor.Version1, EvidenceFormat: "sql-epoch-token/v1",
		Binding: recoveryanchor.Binding{ClusterID: "test-cluster", StorageID: srcSID()},
		Evidence: []byte(`{"epoch":1,"token":"initial"}`), Generation: 0,
		Transition: &recoveryanchor.Transition{}})
	guard := newAnchorGuard(b, aid, "test-cluster")
	_, err := guard.reserve(context.Background())
	if err == nil {
		t.Fatal("should reject active transition")
	}
}
