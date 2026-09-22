package checkpoint

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

type claimReadbackBucket struct {
	objstore.Bucket
	afterWrite func(context.Context, string) error
}

func (b *claimReadbackBucket) Upload(ctx context.Context, name string, r io.Reader, options ...objstore.ObjectUploadOption) error {
	if err := b.Bucket.Upload(ctx, name, r, options...); err != nil {
		return err
	}
	if b.afterWrite != nil && strings.HasSuffix(name, "/PUBLISHER") {
		hook := b.afterWrite
		b.afterWrite = nil
		return hook(ctx, name)
	}
	return nil
}

func TestPublisherReadbackRejectsSuccessor(t *testing.T) {
	ctx := context.Background()
	bucket := &claimReadbackBucket{Bucket: objstore.NewInMemBucket()}
	manager := NewManager(bucket, "readback", "", 1)
	bucket.afterWrite = func(ctx context.Context, name string) error {
		r, err := bucket.Bucket.Get(ctx, name)
		if err != nil {
			return err
		}
		var successor PublisherClaim
		err = json.NewDecoder(r).Decode(&successor)
		r.Close()
		if err != nil {
			return err
		}
		// Model a complete lease expiry and takeover while the first writer is paused.
		successor.OwnerID = "successor"
		successor.Generation++
		successor.LeaseUntilMS = time.Now().Add(time.Hour).UnixMilli()
		data, err := json.Marshal(successor)
		if err != nil {
			return err
		}
		return bucket.Bucket.Upload(ctx, name, bytes.NewReader(data))
	}
	claim, err := manager.AcquirePublisherClaim(ctx, "original", 0, time.Minute)
	if !errors.Is(err, ErrPublisherFenced) || claim != nil {
		t.Fatalf("successor adopted: claim=%+v err=%v", claim, err)
	}
}
