package node

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	thanosobjstore "github.com/thanos-io/objstore"
)

var errInjectedPreparedPinClose = errors.New("injected prepared archive pin close failure")
var errInjectedPreparedBucketClose = errors.New("injected prepared bucket close failure")
var errInjectedPreparedRetryGet = errors.New("injected prepared retry archive read refusal")

type preparedPinCloseFaultBucket struct {
	thanosobjstore.Bucket
	mode                string
	archivePinUploads   map[string]int
	failingKey          string
	underlyingWriteDone bool
	entered             chan struct{}
	release             chan struct{}
	closed              chan struct{}
}

func (b *preparedPinCloseFaultBucket) Upload(ctx context.Context, name string, reader io.Reader, options ...thanosobjstore.ObjectUploadOption) error {
	if b.mode != "bucket-close-error" && strings.Contains(name, "/archive/recovery-pins/") {
		if b.archivePinUploads == nil {
			b.archivePinUploads = make(map[string]int)
		}
		b.archivePinUploads[name]++
		if b.archivePinUploads[name] == 2 {
			b.failingKey = name
			if b.mode == "blocked-close" {
				close(b.entered)
				<-b.release
				return b.Bucket.Upload(ctx, name, reader, options...)
			}
			if b.mode == "unknown-after-write" {
				if err := b.Bucket.Upload(ctx, name, reader, options...); err != nil {
					return err
				}
				b.underlyingWriteDone = true
			}
			return errInjectedPreparedPinClose
		}
	}
	return b.Bucket.Upload(ctx, name, reader, options...)
}

func (b *preparedPinCloseFaultBucket) Close() error {
	err := b.Bucket.Close()
	if b.closed != nil {
		close(b.closed)
	}
	if b.mode == "bucket-close-error" {
		return errors.Join(err, errInjectedPreparedBucketClose)
	}
	return err
}

type preparedRestoreDenyReadsBucket struct {
	thanosobjstore.Bucket
	getCalls int
}

func (b *preparedRestoreDenyReadsBucket) Get(context.Context, string) (io.ReadCloser, error) {
	b.getCalls++
	return nil, errInjectedPreparedRetryGet
}

func runPreparedRestoreCloseFailureRegression(t *testing.T, ctx context.Context, config types.ExecutionConfig, archiveDir string) {
	t.Helper()
	for _, mode := range []string{"known-refusal", "unknown-after-write", "bucket-close-error"} {
		t.Run("deferred-pin-close-"+mode, func(t *testing.T) {
			failed := config
			failed.DataDir = t.TempDir()
			bucket := openPreparedRestoreTestBucket(t, archiveDir)
			original := bucket.Bucket
			fault := &preparedPinCloseFaultBucket{Bucket: original, mode: mode}
			bucket.Bucket = fault
			firstErr := prepareRestore(ctx, &failed, bucket)

			marker, markerExists, markerErr := readPreparedRestoreMarker(failed.DataDir)
			leaseUntil := int64(0)
			if fault.failingKey != "" {
				inspectBucket := openPreparedRestoreTestBucket(t, archiveDir)
				reader, err := inspectBucket.Get(ctx, fault.failingKey)
				if err == nil {
					var pin struct {
						LeaseUntilMS int64 `json:"lease_until_unix_ms"`
					}
					data, readErr := io.ReadAll(reader)
					closeErr := reader.Close()
					if readErr == nil && closeErr == nil {
						_ = json.Unmarshal(data, &pin)
						leaseUntil = pin.LeaseUntilMS
					}
				}
				if err := inspectBucket.Close(); err != nil {
					t.Fatal(err)
				}
			}
			observedAtMS := time.Now().UnixMilli()

			retryBucket := openPreparedRestoreTestBucket(t, archiveDir)
			blocker := &preparedRestoreDenyReadsBucket{Bucket: retryBucket.Bucket}
			retryBucket.Bucket = blocker
			retryErr := prepareRestore(ctx, &failed, retryBucket)
			phase := "archive-recovery-pin-close"
			uploadOrdinal := 2
			if mode == "bucket-close-error" {
				phase = "bucket-close"
				uploadOrdinal = 0
			}
			t.Logf("cleanup_failure mode=%s key=%q phase=%s upload_ordinal=%d underlying_write_done=%t api_error=%v marker=%+v marker_exists=%t marker_error=%v pin_lease_until_ms=%d observed_at_ms=%d retry_error=%v retry_remote_gets=%d", mode, fault.failingKey, phase, uploadOrdinal, fault.underlyingWriteDone, firstErr, marker, markerExists, markerErr, leaseUntil, observedAtMS, retryErr, blocker.getCalls)

			wantErr := errInjectedPreparedPinClose
			if mode == "bucket-close-error" {
				wantErr = errInjectedPreparedBucketClose
			}
			if !errors.Is(firstErr, wantErr) {
				t.Errorf("PrepareRestore error=%v, want injected cleanup error %v", firstErr, wantErr)
			}
			if mode != "bucket-close-error" && fault.failingKey == "" {
				t.Errorf("did not intercept the archive recovery pin close upload")
			}
			if mode == "known-refusal" && (fault.underlyingWriteDone || leaseUntil <= observedAtMS) {
				t.Errorf("known pre-write refusal did not retain active pin: underlying_write_done=%t lease_until_ms=%d", fault.underlyingWriteDone, leaseUntil)
			}
			if mode == "unknown-after-write" && (!fault.underlyingWriteDone || leaseUntil > observedAtMS) {
				t.Errorf("unknown-after-write did not leave close mutation applied: underlying_write_done=%t lease_until_ms=%d", fault.underlyingWriteDone, leaseUntil)
			}
			if mode != "bucket-close-error" && fault.archivePinUploads[fault.failingKey] != 2 {
				t.Errorf("pin close was not one-shot: uploads=%d", fault.archivePinUploads[fault.failingKey])
			}
			if mode != "bucket-close-error" && !errors.Is(firstErr, objectstore.ErrMutationOutcomeUnknown) {
				t.Errorf("pin close error did not retain metered mutation uncertainty: %v", firstErr)
			}
			if markerErr != nil || !markerExists || marker.Phase != "preparing" {
				t.Errorf("close failure must leave a resumable preparing marker, not complete: marker=%+v exists=%t err=%v", marker, markerExists, markerErr)
			}
			if blocker.getCalls == 0 || !errors.Is(retryErr, errInjectedPreparedRetryGet) {
				t.Errorf("retry did not reach the remote archive read and preserve its refusal: remote_gets=%d err=%v", blocker.getCalls, retryErr)
			}

			// A distinct, healthy client may resume the preparing DataDir, but it
			// must not erase the uncertainty receipt held by the failed client.
			healthyBucket := openPreparedRestoreTestBucket(t, archiveDir)
			if err := prepareRestore(ctx, &failed, healthyBucket); err != nil {
				t.Fatalf("healthy new client could not resume prepared DataDir: %v", err)
			}
			completed, exists, err := readPreparedRestoreMarker(failed.DataDir)
			if err != nil || !exists || completed.Phase != "complete" || completed.CertifiedTip == 0 {
				t.Fatalf("healthy retry did not publish complete certified restore: marker=%+v exists=%t err=%v", completed, exists, err)
			}
			state, err := materializer.Open(filepath.Join(failed.DataDir, "sqlite.db"), 1)
			if err != nil {
				t.Fatal(err)
			}
			if got := uint64(state.Tip()); got != completed.CertifiedTip {
				_ = state.Close()
				t.Fatalf("healthy retry materialized tip=%d, marker certified tip=%d", got, completed.CertifiedTip)
			}
			rows, err := state.QueryResult(ctx, "SELECT id, value FROM prepared_rows ORDER BY id", nil)
			if err != nil || len(rows.Rows) != 2 || rows.Rows[0][1] != "checkpoint" || rows.Rows[1][1] != "suffix" {
				_ = state.Close()
				t.Fatalf("healthy retry restored SQL rows=%+v err=%v", rows.Rows, err)
			}
			graph, err := state.GraphQuery(ctx, "MATCH (n:PreparedRestore) RETURN n.id", nil)
			if err != nil || len(graph.Rows) != 1 || graph.Rows[0][0] != int64(17) {
				_ = state.Close()
				t.Fatalf("healthy retry restored Graph rows=%+v err=%v", graph.Rows, err)
			}
			if err := state.Close(); err != nil {
				t.Fatal(err)
			}
			if mode != "bucket-close-error" {
				if err := bucket.DrainMutations(context.Background()); !errors.Is(err, objectstore.ErrMutationOutcomeUnknown) {
					t.Errorf("healthy retry cleared failed bucket's sticky mutation uncertainty: %v", err)
				}
			}
		})
	}
}

func runPreparedRestoreCleanupJoinRegression(t *testing.T, ctx context.Context, config types.ExecutionConfig, archiveDir string) {
	t.Helper()
	failed := config
	failed.DataDir = t.TempDir()
	bucket := openPreparedRestoreTestBucket(t, archiveDir)
	fault := &preparedPinCloseFaultBucket{
		Bucket: bucket.Bucket, mode: "blocked-close", entered: make(chan struct{}),
		release: make(chan struct{}), closed: make(chan struct{}),
	}
	bucket.Bucket = fault
	done := make(chan error, 1)
	go func() { done <- prepareRestore(ctx, &failed, bucket) }()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(fault.release) }) }
	defer release()
	select {
	case <-fault.entered:
	case <-ctx.Done():
		release()
		t.Fatal("timed out before pin-close barrier")
	}
	marker, exists, err := readPreparedRestoreMarker(failed.DataDir)
	if err != nil || !exists || marker.Phase != "preparing" {
		release()
		t.Fatalf("marker published complete before cleanup join: marker=%+v exists=%t err=%v", marker, exists, err)
	}
	if _, lock, err := qlog.Acquire(filepath.Join(failed.DataDir, "qlog")); err == nil {
		_ = lock.Release()
		release()
		t.Fatal("qlog lock was released before complete-marker commit")
	}
	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("prepared restore failed after releasing cleanup barrier: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for cleanup join")
	}
	select {
	case <-fault.closed:
	default:
		t.Fatal("complete marker was published before bucket close")
	}
	marker, exists, err = readPreparedRestoreMarker(failed.DataDir)
	if err != nil || !exists || marker.Phase != "complete" {
		t.Fatalf("successful cleanup did not publish complete marker: marker=%+v exists=%t err=%v", marker, exists, err)
	}
}
