package recovery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thanos-io/objstore"
)

type publicationReleaseTestBucket struct {
	objstore.Bucket
	heads, gets, puts atomic.Int32
	release           func(context.Context, string, io.Reader, ...objstore.ObjectUploadOption) error
}

func (b *publicationReleaseTestBucket) Attributes(ctx context.Context, key string) (objstore.ObjectAttributes, error) {
	b.heads.Add(1)
	return b.Bucket.Attributes(ctx, key)
}

func (b *publicationReleaseTestBucket) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	b.gets.Add(1)
	return b.Bucket.Get(ctx, key)
}

func (b *publicationReleaseTestBucket) Upload(ctx context.Context, key string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	if b.puts.Add(1) == 2 {
		return b.release(ctx, key, r, opts...)
	}
	return b.Bucket.Upload(ctx, key, r, opts...)
}

func TestPublicationReleaseUsesConfirmedVersion(t *testing.T) {
	for _, mode := range []string{"normal", "stale", "lost-response"} {
		t.Run(mode, func(t *testing.T) {
			ctx := t.Context()
			base := objstore.NewInMemBucket()
			bucket := &publicationReleaseTestBucket{Bucket: base}
			bucket.release = func(ctx context.Context, key string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
				if _, ok := ctx.Deadline(); !ok || ctx.Err() != nil {
					t.Fatal("release must have its own live deadline")
				}
				params := objstore.ApplyObjectUploadOptions(opts...)
				if params.Condition == nil || params.IfNotMatch || params.IfNotExists {
					t.Fatal("release must use IfMatch")
				}
				if mode == "normal" {
					return base.Upload(ctx, key, r, opts...)
				}
				data, err := io.ReadAll(r)
				if err != nil {
					return err
				}
				replacement := data
				if mode == "stale" {
					replacement, err = json.Marshal(archiveGCLock{OwnerID: "successor", Generation: 2, LeaseUntilMS: time.Now().Add(time.Minute).UnixMilli()})
					if err != nil {
						return err
					}
				}
				// A successor wins before release, or the first CAS commits but
				// its response is lost and the SDK repeats the same condition.
				if err := base.Upload(ctx, key, bytes.NewReader(replacement), opts...); err != nil {
					return err
				}
				return base.Upload(ctx, key, bytes.NewReader(data), opts...)
			}
			manager := NewManager(bucket, "cluster", 1)
			defer manager.Close()
			calls := 0
			err := manager.withPublicationLock(ctx, "publisher", func(context.Context) error {
				calls++
				return nil
			})
			if mode == "normal" && err != nil || mode != "normal" && !errors.Is(err, ErrArchiveBusy) {
				t.Fatalf("release error in %s = %v", mode, err)
			}
			// One missing Attributes, then acquire and final confirmation
			// each use two Attributes and one GET. Release uses one PUT only.
			if calls != 1 || bucket.heads.Load() != 5 || bucket.gets.Load() != 2 || bucket.puts.Load() != 2 {
				t.Fatalf("work/Attributes/GET/PUT = %d/%d/%d/%d; want 1/5/2/2", calls, bucket.heads.Load(), bucket.gets.Load(), bucket.puts.Load())
			}
			reader := NewManager(base, "cluster", 1)
			defer reader.Close()
			stored, err := reader.readArchiveLock(ctx, manager.publicationLockKey())
			if err != nil {
				t.Fatal(err)
			}
			live := stored.LeaseUntilMS > time.Now().UnixMilli()
			if mode == "stale" {
				if !live || stored.OwnerID != "successor" || stored.Generation != 2 {
					t.Fatalf("successor lock was changed: %+v", stored)
				}
			} else if live || stored.OwnerID != "publisher" || stored.Generation != 1 {
				t.Fatalf("publisher lock was not expired as expected: %+v", stored)
			}
		})
	}
}
