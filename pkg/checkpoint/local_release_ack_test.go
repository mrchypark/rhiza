package checkpoint

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/thanos-io/objstore"
)

type localReleaseACKBucket struct {
	objstore.Bucket
	mode      string
	releasing atomic.Bool
	finished  chan struct{}
}

func (b *localReleaseACKBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	var control localMaintenanceControl
	if !strings.HasSuffix(name, "/local-gc-control") || json.Unmarshal(data, &control) != nil || control.Token != "" {
		return b.Bucket.Upload(ctx, name, bytes.NewReader(data), opts...)
	}
	b.releasing.Store(true)
	if b.mode == "not-committed" {
		return context.DeadlineExceeded
	}
	if err := b.Bucket.Upload(ctx, name, bytes.NewReader(data), opts...); err != nil {
		return err
	}
	if b.mode == "delayed-ack" {
		defer close(b.finished)
		time.Sleep(6 * time.Second)
	}
	return context.DeadlineExceeded
}

func (b *localReleaseACKBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if b.mode == "unreadable" && b.releasing.Load() {
		return nil, errors.New("release readback unavailable")
	}
	return b.Bucket.Get(ctx, name)
}

func TestLocalMaintenanceReleaseACK(t *testing.T) {
	for _, mode := range []string{"lost-ack", "delayed-ack", "not-committed", "unreadable", "work-error", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				base := objstore.NewInMemBucket()
				bucket := &localReleaseACKBucket{Bucket: base, mode: mode, finished: make(chan struct{})}
				manager := NewManager(bucket, "local", "", 1)
				manager.SetLocalControlBinding("location", "binding")
				free, _ := json.Marshal(localMaintenanceControl{Version: types.LocalArchiveFormatVersion, LocationHash: "location", BindingHash: "binding"})
				var control localMaintenanceControl
				if err := base.Upload(context.Background(), "local/local-gc-control", bytes.NewReader(free)); err != nil {
					t.Fatal(err)
				}
				calls := 0
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				workErr := errors.New("work validation failed")
				err := manager.WithLocalMaintenanceContext(ctx, func(workCtx context.Context) error {
					calls++
					if err := LocalBucket(bucket).Upload(workCtx, "local/ack-data", strings.NewReader("confirmed")); err != nil {
						return err
					}
					if mode == "work-error" {
						return workErr
					}
					if mode == "cancelled" {
						cancel()
					}
					return nil
				})
				if mode == "delayed-ack" {
					<-bucket.finished
				}
				wantSuccess := mode == "lost-ack" || mode == "delayed-ack"
				if (err == nil) != wantSuccess {
					t.Fatalf("success=%v, got %v", wantSuccess, err)
				}
				if mode == "work-error" && !errors.Is(err, workErr) {
					t.Fatalf("lost work error: %v", err)
				}
				if mode == "cancelled" && !errors.Is(err, context.Canceled) {
					t.Fatalf("lost cancellation: %v", err)
				}
				if calls != 1 {
					t.Fatalf("work ran %d times", calls)
				}
				r, err := base.Get(context.Background(), "local/local-gc-control")
				if err != nil {
					t.Fatal(err)
				}
				if err := json.NewDecoder(r).Decode(&control); err != nil {
					t.Fatal(err)
				}
				if err := r.Close(); err != nil {
					t.Fatal(err)
				}
				if (control.Token != "") != (mode == "not-committed") {
					t.Fatal("unexpected held-token state")
				}
				data, err := base.Get(context.Background(), "local/ack-data")
				if err != nil {
					t.Fatal(err)
				}
				got, readErr := io.ReadAll(data)
				closeErr := data.Close()
				if readErr != nil || closeErr != nil || string(got) != "confirmed" {
					t.Fatalf("acknowledged data changed: %q, %v, %v", got, readErr, closeErr)
				}
			})
		})
	}
}
