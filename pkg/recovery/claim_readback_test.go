package recovery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/thanos-io/objstore"
)

type lockReadbackBucket struct {
	objstore.Bucket
	afterWrite func(context.Context, string) error
	match      string
	onGet      func(context.Context, string) error
	afterGet   bool
	nilVersion bool
	gets       int
}

func (b *lockReadbackBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	b.gets++
	if b.onGet != nil && !b.afterGet {
		if err := b.onGet(ctx, name); err != nil {
			return nil, err
		}
	}
	r, err := b.Bucket.Get(ctx, name)
	if err == nil && b.onGet != nil && b.afterGet {
		if err := b.onGet(ctx, name); err != nil {
			r.Close()
			return nil, err
		}
	}
	return r, err
}

func (b *lockReadbackBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	a, err := b.Bucket.Attributes(ctx, name)
	if b.nilVersion {
		a.Version = nil
	}
	return a, err
}

func (b *lockReadbackBucket) Upload(ctx context.Context, name string, r io.Reader, options ...objstore.ObjectUploadOption) error {
	if err := b.Bucket.Upload(ctx, name, r, options...); err != nil {
		return err
	}
	if b.afterWrite != nil && (strings.HasSuffix(name, "/GC_LOCK") && b.match == "" || b.match != "" && strings.Contains(name, b.match)) {
		hook := b.afterWrite
		b.afterWrite = nil
		return hook(ctx, name)
	}
	return nil
}

func TestGCLockReadbackRejectsSuccessor(t *testing.T) {
	for _, operation := range []string{"acquire", "renew"} {
		t.Run(operation, func(t *testing.T) {
			ctx := context.Background()
			bucket := &lockReadbackBucket{Bucket: objstore.NewInMemBucket()}
			manager := NewManager(bucket, "readback", 1)
			var original *archiveGCLock
			if operation == "renew" {
				var err error
				original, err = manager.acquireGCLock(ctx, "owner", time.Minute)
				if err != nil {
					t.Fatal(err)
				}
			}
			bucket.afterWrite = func(ctx context.Context, name string) error {
				r, err := bucket.Bucket.Get(ctx, name)
				if err != nil {
					return err
				}
				var successor archiveGCLock
				err = json.NewDecoder(r).Decode(&successor)
				r.Close()
				if err != nil {
					return err
				}
				successor.Generation++ // A repeated owner name must not authorize the next lease.
				successor.LeaseUntilMS = time.Now().Add(time.Hour).UnixMilli()
				data, err := json.Marshal(successor)
				if err != nil {
					return err
				}
				return bucket.Bucket.Upload(ctx, name, bytes.NewReader(data))
			}
			var lock *archiveGCLock
			var err error
			if operation == "acquire" {
				lock, err = manager.acquireGCLock(ctx, "owner", time.Minute)
			} else {
				lock, err = manager.renewGCLock(ctx, original, time.Minute)
			}
			if lock != nil || !errors.Is(err, ErrArchiveBusy) {
				t.Fatalf("successor adopted: lock=%+v err=%v", lock, err)
			}
		})
	}
}

func TestRecoveryPinReadbackRejectsSuccessor(t *testing.T) {
	ctx := context.Background()
	bucket := &lockReadbackBucket{Bucket: objstore.NewInMemBucket(), match: "/archive/recovery-pins/"}
	manager := NewManager(bucket, "pin-readback", 1)
	bucket.afterWrite = func(ctx context.Context, name string) error {
		r, err := bucket.Bucket.Get(ctx, name)
		if err != nil {
			return err
		}
		var successor archiveRecoveryPin
		err = json.NewDecoder(r).Decode(&successor)
		r.Close()
		if err != nil {
			return err
		}
		successor.Token = "successor-token"
		successor.LeaseUntilMS = time.Now().Add(time.Hour).UnixMilli()
		data, err := json.Marshal(successor)
		if err != nil {
			return err
		}
		return bucket.Bucket.Upload(ctx, name, bytes.NewReader(data))
	}
	snapshot, err := manager.BeginRecoverySnapshot(ctx, "owner", time.Minute)
	if snapshot != nil || !errors.Is(err, ErrArchiveBusy) {
		t.Fatalf("successor pin adopted: snapshot=%+v err=%v", snapshot, err)
	}
}

func TestStableLockPairsBodyAndVersion(t *testing.T) {
	for _, mode := range []string{"before-get", "after-get", "churn", "nil-version"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			b := &lockReadbackBucket{Bucket: objstore.NewInMemBucket(), afterGet: mode == "after-get", nilVersion: mode == "nil-version"}
			m := NewManager(b, "stable", 1)
			lock := archiveGCLock{OwnerID: "owner", Generation: 1, LeaseUntilMS: time.Now().Add(time.Hour).UnixMilli()}
			if err := m.writeGCLock(ctx, lock); err != nil {
				t.Fatal(err)
			}
			b.onGet = func(ctx context.Context, name string) error {
				if mode != "churn" {
					b.onGet = nil
				}
				lock.Generation++
				data, err := json.Marshal(lock)
				if err != nil {
					return err
				}
				return b.Bucket.Upload(ctx, name, bytes.NewReader(data))
			}
			got, err := m.readGCLock(ctx)
			if mode == "churn" || mode == "nil-version" {
				if err == nil || got != nil || b.gets != maxPublishRetries {
					t.Fatalf("got=%+v gets=%d err=%v", got, b.gets, err)
				}
				return
			}
			a, attrErr := b.Bucket.Attributes(ctx, m.gcLockKey())
			if err != nil || attrErr != nil || got == nil || got.Generation != 2 || got.version == nil || *got.version != *a.Version || b.gets != 2 {
				t.Fatalf("got=%+v current=%v gets=%d err=%v/%v", got, a.Version, b.gets, err, attrErr)
			}
		})
	}
}

func TestArchiveConfirmationRequiresExactLivePayload(t *testing.T) {
	ctx := context.Background()
	m := NewManager(objstore.NewInMemBucket(), "confirm", 1)
	for _, expired := range []bool{false, true} {
		deadline := time.Now().Add(time.Hour).UnixMilli()
		if expired {
			deadline = 1
		}
		lock := archiveGCLock{OwnerID: "owner", Generation: 1, LeaseUntilMS: deadline}
		if err := m.writeGCLock(ctx, lock); err != nil {
			t.Fatal(err)
		}
		got, err := m.confirmGCLock(ctx, lock)
		if expired && (got != nil || !errors.Is(err, ErrArchiveBusy)) || !expired && (got == nil || err != nil) {
			t.Fatalf("got=%+v err=%v", got, err)
		}
		pin := archiveRecoveryPin{OwnerID: "owner", Token: "token", LeaseUntilMS: deadline}
		key := m.recoveryPinKey("owner")
		if err := m.writeRecoveryPin(ctx, key, pin); err != nil {
			t.Fatal(err)
		}
		confirmed, err := m.confirmRecoveryPin(ctx, key, pin)
		if expired && (confirmed != nil || !errors.Is(err, ErrArchiveBusy)) || !expired && (confirmed == nil || err != nil) {
			t.Fatalf("pin=%+v err=%v", confirmed, err)
		}
	}
}

func TestRecoveryPinRejectsChangedTip(t *testing.T) {
	ctx := context.Background()
	m := NewManager(objstore.NewInMemBucket(), "binding", 1)
	pin := archiveRecoveryPin{OwnerID: "owner", Token: "token", Base: 1, Tip: 2, TailHash: [32]byte{1}, TailObject: 7, LeaseUntilMS: time.Now().Add(time.Hour).UnixMilli()}
	key := m.recoveryPinKey(pin.OwnerID)
	snapshot := &RecoverySnapshot{manager: m, pinKey: key, pin: pin}
	pin.Tip = 3
	if err := m.writeRecoveryPin(ctx, key, pin); err != nil {
		t.Fatal(err)
	}
	before, err := m.bucket.Attributes(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Renew(ctx, time.Minute); !errors.Is(err, ErrArchiveBusy) {
		t.Fatalf("renew=%v", err)
	}
	if err := snapshot.Close(ctx); !errors.Is(err, ErrArchiveBusy) {
		t.Fatalf("close=%v", err)
	}
	after, err := m.bucket.Attributes(ctx, key)
	if err != nil || *before.Version != *after.Version {
		t.Fatalf("replacement changed: err=%v", err)
	}
}
