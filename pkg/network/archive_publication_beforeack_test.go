package network

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	objmetrics "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

type publicationLeaseAdmissionBucket struct {
	objstore.Bucket
	key     string
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

type controlledDeadlineContext struct {
	context.Context
	done <-chan struct{}
}

func (c controlledDeadlineContext) Done() <-chan struct{} { return c.done }

func (c controlledDeadlineContext) Err() error {
	select {
	case <-c.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}

func (b *publicationLeaseAdmissionBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	if name == b.key {
		b.once.Do(func() { close(b.entered) })
		select {
		case <-b.release:
		case <-ctx.Done():
			return objstore.ObjectAttributes{}, ctx.Err()
		}
	}
	return b.Bucket.Attributes(ctx, name)
}

// TestBeforeAckArchivePublicationLeaseWaitsThroughCallerDeadline exercises the
// real BeforeAck barrier against an occupied recovery publication lease. The
// lease is written at the Manager's private object key so this is not a
// synthetic durability callback: SyncThrough must wait for archive admission.
func TestBeforeAckArchivePublicationLeaseWaitsThroughCallerDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	bucket, err := objmetrics.NewBucket(objmetrics.Config{
		Provider:      objmetrics.ProviderFilesystem,
		FilesystemDir: filepath.Join(t.TempDir(), "objects"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Close()

	const prefix = "before-ack"
	lockKey := prefix + "/archive/PUBLISH_LOCK"
	admissionEntered := make(chan struct{})
	releaseAdmission := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseAdmission) }) }
	probe := &publicationLeaseAdmissionBucket{
		Bucket: bucket, key: lockKey, entered: admissionEntered, release: releaseAdmission,
	}
	archive := recovery.NewManager(probe, prefix, 1)
	defer archive.Close()
	if err := archive.Load(ctx); err != nil {
		t.Fatal(err)
	}

	core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}}, nil, nil)
	material, err := materializer.Open(filepath.Join(t.TempDir(), "state.sqlite"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := NewServer(core, material, "cluster", true, nil)
	defer func() {
		release()
		server.Close()
	}()
	server.SetDurabilityBarrier(func(barrierCtx context.Context, slot quepaxa.Slot) error {
		return archive.SyncThrough(barrierCtx, core, slot)
	})

	lockData, err := json.Marshal(struct {
		OwnerID      string `json:"owner_id"`
		Generation   uint64 `json:"generation"`
		LeaseUntilMS int64  `json:"lease_until_unix_ms"`
	}{
		OwnerID: "test-held-publication-lease", Generation: 1,
		LeaseUntilMS: time.Now().Add(time.Minute).UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := bucket.Upload(ctx, lockKey, strings.NewReader(string(lockData))); err != nil {
		t.Fatalf("occupy publication lease: %v", err)
	}

	req := ExecuteRequest{RequestID: "publication-lease-deadline", SQL: "CREATE TABLE publication_lease_deadline(id INTEGER)"}
	callerDeadline := make(chan struct{})
	callerCtx := controlledDeadlineContext{Context: context.Background(), done: callerDeadline}
	result := make(chan error, 1)
	go func() {
		_, err := server.Execute(callerCtx, req)
		result <- err
	}()
	select {
	case <-admissionEntered:
	case <-ctx.Done():
		t.Fatalf("BeforeAck barrier did not read the occupied publication lease: %v", ctx.Err())
	}
	close(callerDeadline)
	select {
	case err = <-result:
	case <-ctx.Done():
		t.Fatalf("caller did not return after cancellation: %v", ctx.Err())
	}
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrCommitUnknown) {
		t.Fatalf("occupied publication lease error=%v, want canceled commit-unknown", err)
	}
	if errors.Is(err, ErrLocalMaintenanceRefused) {
		t.Fatalf("occupied publication lease became pre-admission refusal: %v", err)
	}
	if receipt, found, receiptErr := material.MutationReceipt(ctx, types.MutationSQL, req.RequestID); receiptErr != nil || !found || receipt.Slot == 0 {
		t.Fatalf("mutation was not applied before uncertain result: receipt=%+v found=%t err=%v", receipt, found, receiptErr)
	}
	if tip := archive.Tip(); tip != 0 {
		t.Fatalf("archive acknowledged while publication lease remained occupied: tip=%d", tip)
	}

	expiredLockData, err := json.Marshal(struct {
		OwnerID      string `json:"owner_id"`
		Generation   uint64 `json:"generation"`
		LeaseUntilMS int64  `json:"lease_until_unix_ms"`
	}{
		OwnerID: "test-held-publication-lease", Generation: 1,
		LeaseUntilMS: time.Now().Add(-time.Minute).UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := bucket.Upload(ctx, lockKey, strings.NewReader(string(expiredLockData))); err != nil {
		t.Fatalf("expire publication lease: %v", err)
	}
	release()
	deadline := time.After(5 * time.Second)
	for len(server.localCap) != 0 {
		select {
		case <-deadline:
			t.Fatal("caller deadline canceled the server-owned BeforeAck lifecycle")
		case <-time.After(time.Millisecond):
		}
	}
	if !server.Ready() {
		t.Fatalf("successful server-owned BeforeAck lifecycle poisoned readiness: %v", server.localFailure())
	}

	readback := recovery.NewManager(bucket, prefix, 1)
	defer readback.Close()
	if err := readback.Load(ctx); err != nil {
		t.Fatalf("archive readback after lease release: %v", err)
	}
	values, tip, err := readback.DecisionsFrom(ctx, 1, 1)
	if err != nil || tip != 1 || len(values) != 1 {
		t.Fatalf("archive readback tip=%d values=%d err=%v", tip, len(values), err)
	}
	decided, ok := core.CertifiedValue(1)
	if !ok || !bytes.Equal(values[0].Value, decided.Value) {
		t.Fatal("archive readback differs from the applied mutation")
	}

	response, err := server.Execute(ctx, req)
	if err != nil || response.Slot != 1 {
		t.Fatalf("cached receipt after verified archive publication: response=%+v err=%v", response, err)
	}
}
