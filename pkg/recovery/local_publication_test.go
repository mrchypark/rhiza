package recovery

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

type localPublicationBucket struct {
	objstore.Bucket
	changeHeadDuringRead atomic.Bool
	writes               atomic.Int64
	heads                atomic.Int64
	extents              atomic.Int64
}

func (b *localPublicationBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	b.writes.Add(1)
	if strings.HasSuffix(name, "/HEAD") {
		b.heads.Add(1)
	}
	err := b.Bucket.Upload(ctx, name, r, opts...)
	return err
}
func (b *localPublicationBucket) Delete(ctx context.Context, name string) error {
	b.writes.Add(1)
	return b.Bucket.Delete(ctx, name)
}
func (b *localPublicationBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	return b.Bucket.Attributes(ctx, name)
}
func (b *localPublicationBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if strings.Contains(name, "/extents/") {
		b.extents.Add(1)
	}
	r, err := b.Bucket.Get(ctx, name)
	if err == nil && strings.HasSuffix(name, "/HEAD") && b.changeHeadDuringRead.Swap(false) {
		if err := b.Bucket.Upload(ctx, name, strings.NewReader("changed during read")); err != nil {
			_ = r.Close()
			return nil, err
		}
	}
	return r, err
}

func TestLocalFreshnessRequiresStableHeadVersion(t *testing.T) {
	ctx := context.Background()
	bucket := &localPublicationBucket{Bucket: objstore.NewInMemBucket()}
	a, err := CreateLocalArchive(ctx, bucket, "local", localTestBinding(), nil)
	if err != nil {
		t.Fatal(err)
	}
	bucket.changeHeadDuringRead.Store(true)
	if err := a.checkLocalHead(ctx); err == nil || !strings.Contains(err.Error(), "head changed during read") {
		t.Fatalf("unstable HEAD not rejected: %v", err)
	}
}

func TestLocalFreshnessRejectsMetadataCorruption(t *testing.T) {
	for _, object := range []string{"HEAD", "BINDING", "local-gc-control"} {
		t.Run(object, func(t *testing.T) {
			ctx := context.Background()
			bucket := objstore.NewInMemBucket()
			a, err := CreateLocalArchive(ctx, bucket, "local", localTestBinding(), nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := a.GarbageCollect(ctx, 1, 0); err != nil {
				t.Fatal(err)
			}
			if err := bucket.Upload(ctx, "local/"+object, strings.NewReader(`{"corrupt":true}`)); err != nil {
				t.Fatal(err)
			}
			if err := a.checkLocalHead(ctx); err == nil {
				t.Fatal("freshness accepted corrupt metadata")
			}
			if _, err := OpenLocalArchive(ctx, bucket, "local"); err == nil {
				t.Fatal("Open accepted corrupt metadata")
			}
		})
	}
}

func TestLocalPublicationExtentReadsScaleLinearly(t *testing.T) {
	for _, count := range []int{8, 64} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			ctx := context.Background()
			bucket := &localPublicationBucket{Bucket: objstore.NewInMemBucket()}
			a, err := CreateLocalArchive(ctx, bucket, "local", localTestBinding(), nil)
			if err != nil {
				t.Fatal(err)
			}
			source := localArchiveSuffix{prefixes: map[quepaxa.Slot][32]byte{0: {}}}
			bucket.extents.Store(0)
			for i := 1; i <= count; i++ {
				slot := quepaxa.Slot(i)
				value := []byte(fmt.Sprintf("value-%d", i))
				decision := quepaxa.DecidedValue{Slot: slot, Value: value, Hash: sha256.Sum256(value), Certificate: json.RawMessage(`{}`)}
				source.values = append(source.values, decision)
				source.prefixes[slot] = quepaxa.AdvancePrefixHash(source.prefixes[slot-1], slot, decision.Hash)
				source.tip = slot
				if err := a.SyncThrough(ctx, source, slot); err != nil {
					t.Fatal(err)
				}
			}
			if got := bucket.extents.Load(); got != int64(count) {
				t.Fatalf("%d writes read %d extents; want exactly %d publication readbacks", count, got, count)
			}
			bucket.extents.Store(0)
			if err := a.SyncThrough(ctx, source, source.tip); err != nil {
				t.Fatal(err)
			}
			if got := bucket.extents.Load(); got != 0 {
				t.Fatalf("no-op reread %d extents", got)
			}
			if _, err := OpenLocalArchive(ctx, bucket, "local"); err != nil {
				t.Fatal(err)
			}
			if got := bucket.extents.Load(); got != int64(count) {
				t.Fatalf("Open read %d extents; want full reconstruction of %d", got, count)
			}
			key := fmt.Sprintf("local/extents/%x", a.head.Tail)
			if err := bucket.Upload(ctx, key, strings.NewReader("corrupt")); err != nil {
				t.Fatal(err)
			}
			if _, err := OpenLocalArchive(ctx, bucket, "local"); err == nil {
				t.Fatal("Open accepted corrupted extent")
			}
		})
	}
}

// Cancel exactly after the backing store acknowledges HEAD, before Get admission.
type localHeadACKBucket struct {
	objstore.Bucket
	cancel context.CancelFunc
	fault  string
	active bool
}

func (b *localHeadACKBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	err := b.Bucket.Upload(ctx, name, r, opts...)
	if err == nil && strings.HasSuffix(name, "/HEAD") && b.cancel != nil {
		b.cancel()
		b.cancel = nil
		b.active = true
	}
	return err
}

func (b *localHeadACKBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if strings.HasSuffix(name, "/HEAD") && b.active {
		switch b.fault {
		case "slow":
			time.Sleep(3 * time.Second)
		case "corrupt":
			return io.NopCloser(strings.NewReader("corrupt")), nil
		case "timeout":
			<-ctx.Done()
			return nil, ctx.Err()
		}
	}
	return b.Bucket.Get(ctx, name)
}

func TestLocalHeadACKCancellationReadback(t *testing.T) {
	for _, fault := range []string{"", "slow", "corrupt", "timeout"} {
		t.Run(fault, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				bucket := &localHeadACKBucket{Bucket: objstore.NewInMemBucket()}
				a, err := CreateLocalArchive(ctx, bucket, "local", localTestBinding(), nil)
				if err != nil {
					t.Fatal(err)
				}
				old := a.headHash
				oldVersion := a.version
				head := a.head
				request, cancel := context.WithCancel(ctx)
				defer cancel()
				bucket.cancel, bucket.fault = cancel, fault
				err = a.localManager().WithLocalMaintenanceContext(request, func(work context.Context) error {
					return a.publish(work, head)
				})
				if fault == "" || fault == "slow" {
					if !errors.Is(err, context.Canceled) || a.failure != nil || a.head.Version != localArchiveFormatVersion || sameObjectVersion(oldVersion, a.version) {
						t.Fatalf("acknowledged HEAD not retained: err=%v failure=%v head=%+v", err, a.failure, a.head)
					}
					if a.headHash != old {
						t.Fatal("idempotent HEAD publication changed its contents")
					}
					fresh, err := OpenLocalArchive(ctx, bucket, "local")
					if err != nil || fresh.headHash != a.headHash || !sameObjectVersion(fresh.version, a.version) {
						t.Fatalf("verified cache differs from reopened HEAD: %v", err)
					}
				} else {
					if err == nil || a.failure == nil || a.headHash != old {
						t.Fatalf("failed readback accepted: %v failure=%v", err, a.failure)
					}
					if fault == "timeout" && !errors.Is(err, context.DeadlineExceeded) {
						t.Fatal(err)
					}
				}
				if err := a.localManager().WithLocalMaintenanceContext(ctx, func(context.Context) error { return nil }); err != nil {
					t.Fatal("gate not free after acknowledged mutations", err)
				}
				if fault == "" || fault == "slow" {
					// Final flush on the same handle must remain usable; publish seals the intent.
					if err := a.GarbageCollect(ctx, 1, 0); err != nil {
						t.Fatal("final flush", err)
					}
					if _, err := OpenLocalArchive(ctx, bucket, "local"); err != nil {
						t.Fatal("reopen", err)
					}
				}
				synctest.Wait()
			})
		})
	}
}
