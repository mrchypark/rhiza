package rhiza_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/thanos-io/objstore"
)

type embeddedReleaseACKBucket struct {
	objstore.Bucket
	commit bool
}

func (b *embeddedReleaseACKBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	var state struct {
		Token string `json:"token"`
	}
	if !strings.HasSuffix(name, "/local-gc-control") || json.Unmarshal(data, &state) != nil || state.Token != "" {
		return b.Bucket.Upload(ctx, name, bytes.NewReader(data), opts...)
	}
	if b.commit {
		if err := b.Bucket.Upload(ctx, name, bytes.NewReader(data), opts...); err != nil {
			return err
		}
	}
	return errors.New("fixture: release acknowledgement lost")
}

func TestEmbeddedObjectStorageMaintenanceReleaseACK(t *testing.T) {
	for _, commit := range []bool{true, false} {
		name := "committed"
		if !commit {
			name = "not-committed"
		}
		t.Run(name, func(t *testing.T) {
			cfg := embeddedStoreConfig(t)
			base := embeddedBucket(t, cfg)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			free, err := json.Marshal(map[string]any{"version": types.LocalArchiveFormatVersion, "location_hash": "fixture-location", "binding_hash": "fixture-binding", "token": ""})
			if err != nil {
				t.Fatal(err)
			}
			const key = "fixture/local-gc-control"
			if err := base.Upload(ctx, key, bytes.NewReader(free), objstore.WithIfNotExists()); err != nil {
				t.Fatal(err)
			}
			bucket := &embeddedReleaseACKBucket{Bucket: base, commit: commit}
			manager := checkpoint.NewManager(bucket, "fixture", "", 1)
			manager.SetLocalControlBinding("fixture-location", "fixture-binding")
			calls := 0
			err = manager.WithLocalMaintenanceContext(ctx, func(workCtx context.Context) error {
				calls++
				return checkpoint.LocalBucket(bucket).Upload(workCtx, "fixture/ack-data", strings.NewReader("preserved"), objstore.WithIfNotExists())
			})
			if (err == nil) != commit {
				t.Fatalf("committed=%v, result=%v", commit, err)
			}
			if calls != 1 {
				t.Fatalf("executed %d times", calls)
			}
			resolved, err := manager.LocalMaintenanceResolved(ctx)
			if err != nil || resolved != commit {
				t.Fatalf("resolved=%v: %v", resolved, err)
			}
			r, err := base.Get(ctx, "fixture/ack-data")
			if err != nil {
				t.Fatal(err)
			}
			data, readErr := io.ReadAll(r)
			closeErr := r.Close()
			if readErr != nil || closeErr != nil || string(data) != "preserved" {
				t.Fatalf("data=%q, read=%v, close=%v", data, readErr, closeErr)
			}
		})
	}
}
