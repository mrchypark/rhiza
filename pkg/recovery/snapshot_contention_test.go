package recovery

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thanos-io/objstore"
)

func TestRecoverySnapshotWaitsForMaintenance(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	holder := NewManager(bucket, "cluster", 1)
	// An abandoned short lease models another startup or maintenance holder.
	if _, err := holder.acquireGCLock(ctx, "other-startup", 75*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	reader := NewManager(bucket, "cluster", 1)
	snapshot, err := reader.BeginRecoverySnapshot(ctx, "startup", time.Second)
	if err != nil {
		t.Fatalf("transient lock contention aborted recovery: %v", err)
	}
	defer snapshot.Close(ctx)
	if snapshot.Tip() != 0 {
		t.Fatalf("empty archive tip = %d", snapshot.Tip())
	}
	// Busy from the work itself (a duplicate live owner) must not be retried.
	if _, err := reader.BeginRecoverySnapshot(ctx, "startup", time.Second); !errors.Is(err, ErrArchiveBusy) {
		t.Fatalf("duplicate pin: %v", err)
	}
}

func TestRecoverySnapshotContentionHonorsDeadline(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(objstore.NewInMemBucket(), "cluster", 1)
	if _, err := manager.acquireGCLock(ctx, "maintenance", time.Minute); err != nil {
		t.Fatal(err)
	}
	for _, callerDeadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "lease-bound", true: "caller-bound"}[callerDeadline], func(t *testing.T) {
			workCtx := ctx
			lease := 20 * time.Millisecond
			if callerDeadline {
				var cancel context.CancelFunc
				workCtx, cancel = context.WithTimeout(ctx, lease)
				defer cancel()
				lease = time.Minute
			}
			snapshot, err := manager.BeginRecoverySnapshot(workCtx, "startup", lease)
			if snapshot != nil || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("snapshot=%v err=%v", snapshot, err)
			}
		})
	}
}
