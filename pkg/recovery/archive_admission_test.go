//go:build rhiza_local_testhooks

package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/thanos-io/objstore"
)

type archiveAdmissionBucket struct {
	objstore.Bucket
	gcKey         string
	headKey       string
	gcUploads     atomic.Int32
	pinUploads    atomic.Int32
	blockHead     bool
	headRead      chan struct{}
	confirmErr    error
	confirmBusy   bool
	releaseErr    error
	commitRelease bool
}

func (b *archiveAdmissionBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if name == b.gcKey && b.confirmBusy && b.gcUploads.Load() > 0 {
		mismatch, err := json.Marshal(archiveGCLock{
			OwnerID:      "different-owner",
			Generation:   1,
			LeaseUntilMS: time.Now().Add(archivePinLease).UnixMilli(),
		})
		if err != nil {
			return nil, err
		}
		return io.NopCloser(strings.NewReader(string(mismatch))), nil
	}
	if name == b.gcKey && b.confirmErr != nil && b.gcUploads.Load() > 0 {
		return nil, b.confirmErr
	}
	if name == b.headKey && b.blockHead {
		select {
		case b.headRead <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return b.Bucket.Get(ctx, name)
}

func (b *archiveAdmissionBucket) Upload(ctx context.Context, name string, r io.Reader, options ...objstore.ObjectUploadOption) error {
	if name == b.gcKey {
		attempt := b.gcUploads.Add(1)
		if attempt == 2 && b.releaseErr != nil && !b.commitRelease {
			return b.releaseErr
		}
		err := b.Bucket.Upload(ctx, name, r, options...)
		if attempt == 2 && b.releaseErr != nil {
			return b.releaseErr
		}
		return err
	}
	if strings.Contains(name, "/archive/recovery-pins/") {
		b.pinUploads.Add(1)
	}
	return b.Bucket.Upload(ctx, name, r, options...)
}

func TestBeginRecoverySnapshotActiveGCLockCancellationPreservesBothCauses(t *testing.T) {
	ctx, base, _, seed := newSealableArchive(t)
	defer seed.Close()
	holder := NewManager(base, "cluster", 1)
	defer holder.Close()
	if _, err := holder.acquireGCLock(ctx, "active-holder", archivePinLease); err != nil {
		t.Fatal(err)
	}

	operationCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	waiter := NewManager(base, "cluster", 1)
	defer waiter.Close()
	traced := localtesthooks.WithArchiveBusyTrace(operationCtx, func(event localtesthooks.ArchiveBusyEvent) {
		if event.Resource == "gc_lock" && event.Branch == "active_live_lease" {
			cancel()
		}
	})
	snapshot, err := waiter.BeginRecoverySnapshot(traced, "active-waiter", time.Minute)
	if snapshot != nil || !errors.Is(err, ErrActiveGCLock) || !errors.Is(err, ErrArchiveBusy) || !errors.Is(err, context.Canceled) {
		t.Fatalf("snapshot=%v error=%v, want active lease and cancellation retained", snapshot, err)
	}
	if waiter.headCAS != nil {
		t.Fatal("active-lease refusal entered the snapshot callback")
	}
	if exists, err := base.Exists(ctx, waiter.recoveryPinKey("active-waiter")); err != nil || exists {
		t.Fatalf("pin exists=%t err=%v, want no pin write", exists, err)
	}
}

func TestBeginRecoverySnapshotConditionalContentionIsNotActiveLease(t *testing.T) {
	ctx, base, _, seed := newSealableArchive(t)
	defer seed.Close()
	holder := NewManager(base, "cluster", 1)
	defer holder.Close()
	lease, err := holder.acquireGCLock(ctx, "initial", archivePinLease)
	if err != nil {
		t.Fatal(err)
	}
	if err := holder.releaseGCLock(ctx, lease); err != nil {
		t.Fatal(err)
	}
	key := holder.gcLockKey()
	bucket := &conditionalArchiveLockBucket{Bucket: base, key: key}
	bucket.before = func(ctx context.Context) error {
		current, err := holder.readArchiveLock(ctx, key)
		if err != nil {
			return err
		}
		current.Generation++
		current.LeaseUntilMS = time.Now().Add(-time.Second).UnixMilli()
		return holder.writeArchiveLock(ctx, key, *current, objstore.WithIfMatch(current.version))
	}
	waiter := NewManager(bucket, "cluster", 1)
	defer waiter.Close()
	operationCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	traced := localtesthooks.WithArchiveBusyTrace(operationCtx, func(event localtesthooks.ArchiveBusyEvent) {
		if event.Branch == "conditional_exhausted" {
			cancel()
		}
	})
	snapshot, err := waiter.BeginRecoverySnapshot(traced, "conditional-waiter", time.Minute)
	var admissionErr *archiveLockAdmissionError
	if snapshot != nil || !errors.Is(err, ErrArchiveBusy) || !errors.Is(err, context.Canceled) || errors.Is(err, ErrActiveGCLock) ||
		!errors.As(err, &admissionErr) || admissionErr.phase != "conditional contention exhausted" || bucket.count != maxPublishRetries {
		t.Fatalf("snapshot=%v error=%v conditional PUTs=%d admission=%+v", snapshot, err, bucket.count, admissionErr)
	}
	if waiter.headCAS != nil {
		t.Fatal("conditional exhaustion entered the snapshot callback")
	}
	if exists, err := base.Exists(ctx, waiter.recoveryPinKey("conditional-waiter")); err != nil || exists {
		t.Fatalf("pin exists=%t err=%v, want no pin write", exists, err)
	}
}

func TestBeginRecoverySnapshotConfirmationUnknownIsNotRetried(t *testing.T) {
	ctx, base, _, seed := newSealableArchive(t)
	defer seed.Close()
	unknownErr := errors.New("confirmation read outcome unknown")
	bucket := &archiveAdmissionBucket{
		Bucket:     base,
		gcKey:      "cluster/archive/GC_LOCK",
		headKey:    "cluster/archive/head.bin",
		confirmErr: unknownErr,
	}
	waiter := NewManager(bucket, "cluster", 1)
	defer waiter.Close()
	snapshot, err := waiter.BeginRecoverySnapshot(ctx, "confirmation-waiter", time.Minute)
	var admissionErr *archiveLockAdmissionError
	if snapshot != nil || errors.Is(err, ErrActiveGCLock) || !errors.Is(err, unknownErr) ||
		!errors.As(err, &admissionErr) || admissionErr.phase != "acquisition confirmation outcome unknown" || admissionErr.retryable {
		t.Fatalf("snapshot=%v error=%v, want unknown confirmation preserved and non-retryable", snapshot, err)
	}
	if got := bucket.gcUploads.Load(); got != 1 {
		t.Fatalf("GC-lock uploads=%d, want one successful acquisition and no whole-operation retry", got)
	}
	if waiter.headCAS != nil {
		t.Fatal("unconfirmed acquisition entered the snapshot callback")
	}
	if exists, err := base.Exists(ctx, waiter.recoveryPinKey("confirmation-waiter")); err != nil || exists {
		t.Fatalf("pin exists=%t err=%v, want no pin write", exists, err)
	}
}

func TestBeginRecoverySnapshotConfirmationMismatchIsNotRetried(t *testing.T) {
	ctx, base, _, seed := newSealableArchive(t)
	defer seed.Close()
	bucket := &archiveAdmissionBucket{
		Bucket:      base,
		gcKey:       "cluster/archive/GC_LOCK",
		headKey:     "cluster/archive/head.bin",
		confirmBusy: true,
	}
	waiter := NewManager(bucket, "cluster", 1)
	defer waiter.Close()
	snapshot, err := waiter.BeginRecoverySnapshot(ctx, "mismatch-waiter", time.Minute)
	var admissionErr *archiveLockAdmissionError
	if snapshot != nil || !errors.Is(err, ErrArchiveBusy) || errors.Is(err, ErrActiveGCLock) ||
		!errors.As(err, &admissionErr) || admissionErr.phase != "acquisition confirmation outcome unknown" || admissionErr.retryable {
		t.Fatalf("snapshot=%v error=%v admission=%+v, want one non-retryable confirmed mismatch", snapshot, err, admissionErr)
	}
	if got := bucket.gcUploads.Load(); got != 1 {
		t.Fatalf("GC-lock uploads=%d, want one successful PUT and no whole-operation retry", got)
	}
	if waiter.headCAS != nil {
		t.Fatal("mismatched acquisition entered the snapshot callback")
	}
	if exists, err := base.Exists(ctx, waiter.recoveryPinKey("mismatch-waiter")); err != nil || exists {
		t.Fatalf("pin exists=%t err=%v, want no pin write", exists, err)
	}
}

func TestBeginRecoverySnapshotCallbackLoadCancellationIsTerminalAndReleases(t *testing.T) {
	ctx, base, _, seed := newSealableArchive(t)
	defer seed.Close()
	readEntered := make(chan struct{}, 1)
	bucket := &archiveAdmissionBucket{
		Bucket:    base,
		gcKey:     "cluster/archive/GC_LOCK",
		headKey:   "cluster/archive/head.bin",
		blockHead: true,
		headRead:  readEntered,
	}
	waiter := NewManager(bucket, "cluster", 1)
	defer waiter.Close()
	operationCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		snapshot *RecoverySnapshot
		err      error
	}
	done := make(chan result, 1)
	go func() {
		snapshot, err := waiter.BeginRecoverySnapshot(operationCtx, "load-waiter", time.Minute)
		done <- result{snapshot: snapshot, err: err}
	}()
	select {
	case <-readEntered:
		cancel()
	case <-time.After(5 * time.Second):
		t.Fatal("callback archive HEAD read was not reached")
	}
	got := <-done
	if got.snapshot != nil || !errors.Is(got.err, context.Canceled) || !strings.Contains(got.err.Error(), "load archive under GC-lock") {
		t.Fatalf("snapshot=%v error=%v, want callback Load cancellation identified", got.snapshot, got.err)
	}
	if uploads := bucket.gcUploads.Load(); uploads != 2 {
		t.Fatalf("GC-lock uploads=%d, want acquisition and fresh-context release", uploads)
	}
	if uploads := bucket.pinUploads.Load(); uploads != 0 {
		t.Fatalf("recovery-pin uploads=%d, want none before successful Load", uploads)
	}
}

func TestArchiveLockPreservesWorkAndReleaseErrors(t *testing.T) {
	ctx, base, _, seed := newSealableArchive(t)
	defer seed.Close()
	workErr := errors.New("archive callback failed")
	releaseErr := errors.New("GC-lock release failed")
	bucket := &archiveAdmissionBucket{Bucket: base, gcKey: "cluster/archive/GC_LOCK", releaseErr: releaseErr}
	manager := NewManager(bucket, "cluster", 1)
	defer manager.Close()
	workCalls := 0
	err := manager.withGCLock(ctx, "dual-error", func(context.Context) error {
		workCalls++
		return workErr
	})
	if workCalls != 1 || !errors.Is(err, workErr) || !errors.Is(err, releaseErr) || !strings.Contains(err.Error(), "archive lock work") || !strings.Contains(err.Error(), "archive lock release") {
		t.Fatalf("work calls=%d error=%v, want both phase errors retained once", workCalls, err)
	}
}

func TestArchiveLockPreservesUnknownCommittedRelease(t *testing.T) {
	ctx, base, _, seed := newSealableArchive(t)
	defer seed.Close()
	unknownErr := errors.New("release committed but acknowledgment lost")
	bucket := &archiveAdmissionBucket{Bucket: base, gcKey: "cluster/archive/GC_LOCK", releaseErr: unknownErr, commitRelease: true}
	manager := NewManager(bucket, "cluster", 1)
	defer manager.Close()
	err := manager.withGCLock(ctx, "unknown-release", func(context.Context) error { return nil })
	if !errors.Is(err, unknownErr) || !strings.Contains(err.Error(), "archive lock release") {
		t.Fatalf("release error=%v, want unknown commit outcome retained", err)
	}
	lock, readErr := manager.readGCLock(ctx)
	if readErr != nil || lock.LeaseUntilMS > time.Now().UnixMilli() {
		t.Fatalf("committed release lease=%+v err=%v, want expired state despite unknown acknowledgment", lock, readErr)
	}
}

func TestArchiveActiveRecoveryPinCleanReleaseRemainsExactSentinel(t *testing.T) {
	ctx, base, core, seed := newSealableArchive(t)
	defer seed.Close()
	sealed, decision := realTrimFixture(t, ctx, core, seed)
	owner := "clean-active-pin"
	pin := archiveRecoveryPin{OwnerID: owner, Token: "token", Base: 0, Tip: 1, TailHash: [32]byte{1}, TailObject: 1, LeaseUntilMS: time.Now().Add(time.Hour).UnixMilli()}
	if err := seed.writeRecoveryPin(ctx, seed.recoveryPinKey(owner), pin); err != nil {
		t.Fatal(err)
	}
	manager := NewManager(base, "cluster", 1)
	defer manager.Close()
	if err := manager.TrimThrough(ctx, sealed, decision); err != ErrActiveRecoveryPin {
		t.Fatalf("clean active-pin refusal=%v, want exact ErrActiveRecoveryPin", err)
	}
}
