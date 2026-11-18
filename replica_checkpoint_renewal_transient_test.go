//go:build rhiza_local_testhooks

package rhiza

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	thanosobjstore "github.com/thanos-io/objstore"
)

type learnerTransientRenewalBucket struct {
	thanosobjstore.Bucket

	mu           sync.Mutex
	armed        bool
	injected     chan struct{}
	injectedOnce sync.Once
}

func (b *learnerTransientRenewalBucket) arm() {
	b.mu.Lock()
	b.armed = true
	b.mu.Unlock()
}

func (b *learnerTransientRenewalBucket) Upload(ctx context.Context, name string, reader io.Reader, options ...thanosobjstore.ObjectUploadOption) error {
	if err := b.Bucket.Upload(ctx, name, reader, options...); err != nil {
		return err
	}
	b.mu.Lock()
	inject := b.armed && strings.Contains(name, "/checkpoint/recovery-pins/")
	if inject {
		b.armed = false
	}
	b.mu.Unlock()
	if inject {
		b.injectedOnce.Do(func() { close(b.injected) })
		return errors.New("injected transient recovery pin renewal failure")
	}
	return nil
}

func TestLearnerAmbiguousPinRenewalDuringCompactionFailsClosed(t *testing.T) {
	oldLease, oldRenew := learnerCheckpointLease, learnerCheckpointRenew
	learnerCheckpointLease, learnerCheckpointRenew = 10*time.Second, 25*time.Millisecond
	defer func() { learnerCheckpointLease, learnerCheckpointRenew = oldLease, oldRenew }()

	ctx := context.Background()
	bucketDir := t.TempDir()
	source := newLearnerCheckpointSource(t, bucketDir)
	r := openTestLearner(t, learnerReplicaConfig(t.TempDir(), bucketDir, "reader"), source)
	source.execute(t, "CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)")
	source.execute(t, "INSERT INTO items VALUES (1, 1)")
	_, sealed := source.publishCheckpoint(t, true)

	baseBucket := r.bucket.Bucket
	bucket := &learnerTransientRenewalBucket{
		Bucket:   baseBucket,
		injected: make(chan struct{}),
	}
	r.bucket.Bucket = bucket
	defer func() { r.bucket.Bucket = baseBucket }()

	compacting := make(chan struct{})
	releaseCompaction := make(chan struct{})
	var compactingOnce, releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseCompaction) }) }
	defer release()
	restoreHooks := localtesthooks.Set(func(name string) {
		if name != "qlog:after-compaction-build-synced-before-manifest" {
			return
		}
		bucket.arm()
		compactingOnce.Do(func() { close(compacting) })
		select {
		case <-releaseCompaction:
		case <-time.After(10 * time.Second):
		}
	})
	defer restoreHooks()

	r.syncMu.Lock()
	r.nextCheckpointProbe = time.Now()
	r.syncMu.Unlock()
	syncResult := make(chan error, 1)
	go func() {
		for {
			err := r.Sync(ctx)
			if err != nil {
				syncResult <- err
				return
			}
			if r.core.CompactionFloor() >= sealed.Index {
				syncResult <- nil
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	select {
	case <-compacting:
	case <-time.After(5 * time.Second):
		t.Fatal("learner did not reach the blocked compaction boundary")
	}
	select {
	case <-bucket.injected:
	case <-time.After(5 * time.Second):
		t.Fatal("learner did not attempt recovery pin renewal while compaction was blocked")
	}
	release()

	var syncErr error
	select {
	case syncErr = <-syncResult:
	case <-time.After(5 * time.Second):
		t.Fatal("learner did not finish compaction after the transient renewal failure")
	}
	status := r.Status()
	failure := r.checkpointFailure.Load()
	// The renewal upload is durable, but its ACK is lost. The current pin API
	// cannot distinguish that outcome from an unsuccessful renewal, so treating
	// it as recoverable would require authoritative lease readback/confirmation.
	// Transient recovery remains unresolved; this test pins the existing policy:
	// an ambiguous renewal during compaction latches until reopen.
	const (
		wantLatchErr = "checkpoint pin lost during WAL compaction: injected transient recovery pin renewal failure"
		wantSyncErr  = "checkpoint floor committed after pin lease loss; reopen required: injected transient recovery pin renewal failure"
	)
	if syncErr == nil || syncErr.Error() != wantSyncErr || r.Ready() || r.core.CompactionFloor() != sealed.Index ||
		failure == nil || failure.err == nil || failure.err.Error() != wantLatchErr || status.LastError != wantSyncErr {
		t.Fatalf("ambiguous renewal did not fail closed: syncErr=%v ready=%t floor=%d want=%d checkpointFailure=%v status=%+v", syncErr, r.Ready(), r.core.CompactionFloor(), sealed.Index, failure, status)
	}
}

func learnerCheckpointPinExpired(t *testing.T, bucketDir string) bool {
	t.Helper()
	dir := filepath.Join(bucketDir, "learner-adoption", "checkpoint", "recovery-pins")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read persisted recovery pins: %v", err)
	}
	now := time.Now().UnixMilli()
	for _, entry := range entries {
		if entry.IsDir() || strings.HasSuffix(entry.Name(), ".tmp") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatalf("read persisted recovery pin: %v", err)
		}
		var record struct {
			LeaseUntilMS int64 `json:"lease_until_unix_ms"`
		}
		if err := json.Unmarshal(data, &record); err != nil {
			t.Fatalf("decode persisted recovery pin: %v", err)
		}
		if record.LeaseUntilMS <= now {
			return true
		}
	}
	return false
}

func TestLearnerCompactionPastPinDeadlineWithoutRenewalAttemptDoesNotClearLatch(t *testing.T) {
	oldLease, oldRenew := learnerCheckpointLease, learnerCheckpointRenew
	learnerCheckpointLease, learnerCheckpointRenew = 300*time.Millisecond, time.Hour
	defer func() { learnerCheckpointLease, learnerCheckpointRenew = oldLease, oldRenew }()

	ctx := context.Background()
	bucketDir := t.TempDir()
	source := newLearnerCheckpointSource(t, bucketDir)
	r := openTestLearner(t, learnerReplicaConfig(t.TempDir(), bucketDir, "reader"), source)
	source.execute(t, "CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)")
	source.execute(t, "INSERT INTO items VALUES (1, 1)")
	_, sealed := source.publishCheckpoint(t, true)

	compacting := make(chan struct{})
	releaseCompaction := make(chan struct{})
	var compactingOnce, releaseOnce sync.Once
	var renewalAttempts atomic.Int64
	release := func() { releaseOnce.Do(func() { close(releaseCompaction) }) }
	defer release()
	restoreHooks := localtesthooks.Set(func(name string) {
		if name == "learner:checkpoint-pin-renewal-started" {
			renewalAttempts.Add(1)
			return
		}
		if name != "qlog:after-compaction-build-synced-before-manifest" {
			return
		}
		compactingOnce.Do(func() { close(compacting) })
		select {
		case <-releaseCompaction:
		case <-time.After(10 * time.Second):
		}
	})
	defer restoreHooks()

	r.syncMu.Lock()
	r.nextCheckpointProbe = time.Now()
	r.syncMu.Unlock()
	syncResult := make(chan error, 1)
	go func() {
		for {
			err := r.Sync(ctx)
			if err != nil {
				syncResult <- err
				return
			}
			if r.core.CompactionFloor() >= sealed.Index {
				syncResult <- nil
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	select {
	case <-compacting:
	case <-time.After(5 * time.Second):
		t.Fatal("learner did not reach the blocked compaction boundary")
	}
	time.Sleep(2 * learnerCheckpointLease)
	if !learnerCheckpointPinExpired(t, bucketDir) {
		t.Fatal("persisted recovery pin did not expire before compaction was released")
	}
	if got := renewalAttempts.Load(); got != 0 {
		t.Fatalf("recovery pin renewal was attempted %d times; the missing post-compaction deadline case was not exercised", got)
	}
	release()

	var syncErr error
	select {
	case syncErr = <-syncResult:
	case <-time.After(5 * time.Second):
		t.Fatal("learner did not finish compaction after the pin deadline")
	}
	status := r.Status()
	failure := r.checkpointFailure.Load()
	// The floor is already committed here: this tests detection and fail-closed
	// reporting, not prevention of the preceding compaction or root deletion.
	const wantErr = "checkpoint floor committed after confirmed renewal deadline expiration; reopen required: confirmed renewal deadline expired after compaction"
	if syncErr == nil || r.Ready() || r.core.CompactionFloor() != sealed.Index || failure == nil {
		t.Fatalf("durable compaction past the persisted pin deadline did not stay latched: syncErr=%v ready=%t floor=%d want=%d checkpointFailure=%v status=%+v", syncErr, r.Ready(), r.core.CompactionFloor(), sealed.Index, failure, status)
	}
	if syncErr.Error() != wantErr || failure.err == nil || failure.err.Error() != wantErr || status.LastError != wantErr {
		t.Fatalf("expiry cause mismatch: syncErr=%v latchErr=%v statusError=%q want=%q", syncErr, failure.err, status.LastError, wantErr)
	}
}
