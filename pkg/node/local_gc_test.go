package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	rhizaobjstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/recovery"
	objstore "github.com/thanos-io/objstore"
)

func TestLocalGCConfigDefaultsAndExplicitCount(t *testing.T) {
	for _, count := range []int{0, 1, 7} {
		config := types.ExecutionConfig{Local: true, CheckpointRetainCount: count}
		if err := validateLocalGCConfig(&config); err != nil {
			t.Fatal(err)
		}
		want := count
		if want == 0 {
			want = 3
		}
		if config.CheckpointRetainCount != want || config.ObjStoreGCInterval != time.Hour || config.ObjStoreGCGracePeriod != 24*time.Hour {
			t.Fatalf("defaults=%+v", config)
		}
	}
}

func TestLocalGCConfigPreservesExplicitGracePeriod(t *testing.T) {
	for _, grace := range []time.Duration{time.Nanosecond, time.Hour, 48 * time.Hour} {
		config := types.ExecutionConfig{Local: true, ObjStoreGCGracePeriod: grace}
		if err := validateLocalGCConfig(&config); err != nil {
			t.Fatal(err)
		}
		if config.ObjStoreGCGracePeriod != grace {
			t.Fatalf("grace=%v, want %v", config.ObjStoreGCGracePeriod, grace)
		}
	}
}

func TestLocalMaintenanceRetryOnlyKnownBusy(t *testing.T) {
	for _, test := range []struct {
		name      string
		err       error
		wantCalls int
	}{
		{"busy", fmt.Errorf("gate: %w", checkpoint.ErrLocalMaintenanceBusy), 3},
		{"HEAD advanced", recovery.ErrLocalHEADAdvanced, 1},
		{"uncertain operation", errors.New("delete acknowledgement lost"), 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				err := retryLocalMaintenance(context.Background(), func() error {
					calls++
					if calls == 3 {
						return nil
					}
					return test.err
				})
				if calls != test.wantCalls {
					t.Fatalf("calls=%d, want %d", calls, test.wantCalls)
				}
				if test.wantCalls == 3 && err != nil || test.wantCalls == 1 && !errors.Is(err, test.err) {
					t.Fatalf("retry error=%v", err)
				}
			})
		})
	}
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- retryLocalMaintenance(ctx, func() error { return checkpoint.ErrLocalMaintenanceBusy }) }()
		synctest.Wait()
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) || !errors.Is(err, checkpoint.ErrLocalMaintenanceBusy) {
			t.Fatalf("cancelled retry=%v", err)
		}
	})
}

func TestLocalBeforeAckRetriesRealMaintenanceContention(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	config := types.ExecutionConfig{Local: true, NodeID: "local", ClusterID: "busy-retry", DataDir: t.TempDir(), ObjStoreProvider: "filesystem", ObjStoreDir: t.TempDir(), ObjStoreSyncInterval: time.Hour, CheckpointInterval: time.Hour}
	n := New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatal(err)
	}
	defer n.Shutdown()
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	manager, err := n.localCheckpointManager(n.localBackup.Binding.Bootstrap.ConfigID)
	if err != nil {
		t.Fatal(err)
	}
	gateDone := make(chan error, 1)
	go func() {
		gateDone <- manager.WithLocalMaintenanceContext(ctx, func(context.Context) error { close(entered); <-release; return nil })
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("gate admission timed out")
	}
	writeDone := make(chan error, 1)
	go func() {
		_, err := n.server.Execute(ctx, network.ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE busy_rows (id INTEGER)"})
		writeDone <- err
	}()
	// The engine returns typed busy after five seconds; the node must continue.
	select {
	case err := <-writeDone:
		t.Fatalf("before-ack stopped while maintenance was busy: %v", err)
	case <-time.After(5500 * time.Millisecond):
	}
	if !n.ready.Load() {
		t.Fatal("known maintenance contention failed the Local lifecycle")
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-gateDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("gate did not drain")
	}
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("before-ack did not resume after maintenance")
	}
	if n.localBackup.Tip() != n.core.Tip() {
		t.Fatal("before-ack returned before the write was archived")
	}
}

func TestLocalBackupSnapshotlessOwnerSurvivesWholeLoss(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	config := types.ExecutionConfig{Local: true, NodeID: "local", ClusterID: "snapshotless", DataDir: t.TempDir(), ObjStoreProvider: "filesystem", ObjStoreDir: t.TempDir(), ObjStoreSyncInterval: time.Hour, CheckpointInterval: time.Hour}
	n := New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = n.Shutdown() }()
	for i, sql := range []string{"CREATE TABLE view_rows (id INTEGER)", "INSERT INTO view_rows VALUES (1)"} {
		if _, err := n.server.Execute(ctx, network.ExecuteRequest{RequestID: fmt.Sprintf("view-%d", i), SQL: sql}); err != nil {
			t.Fatal(err)
		}
	}
	if n.localBackup.Snapshot() != nil {
		t.Fatal("fixture unexpectedly has a snapshot")
	}
	if n.localOwner == nil || !n.localOwner.Held() {
		t.Fatal("Local filesystem node did not retain ownership before whole-DataDir loss")
	}
	uuid := n.localStore.policy.StoreUUID
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(config.DataDir); err != nil {
		t.Fatal(err)
	}
	n = New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatalf("snapshotless whole-loss restore: %v", err)
	}
	if n.localStore.policy.StoreUUID != uuid {
		t.Fatal("snapshotless restore replaced store identity")
	}
	if n.localOwner == nil || !n.localOwner.Held() {
		t.Fatal("snapshotless restore did not retain Local filesystem ownership")
	}
	rows, err := n.server.Query(ctx, network.QueryRequest{SQL: "SELECT id FROM view_rows"})
	if err != nil || len(rows.Rows) != 1 {
		t.Fatalf("snapshotless rows=%v error=%v", rows.Rows, err)
	}
	views := 0
	if err := n.bucket.Iter(ctx, path.Join(n.localBackupPrefix(), "view-pins"), func(name string) error {
		reader, err := n.bucket.Get(ctx, name)
		if err != nil {
			return err
		}
		data, readErr := io.ReadAll(reader)
		closeErr := reader.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return err
		}
		var record struct {
			LeaseUntilMS int64 `json:"lease_until_unix_ms"`
		}
		if err := json.Unmarshal(data, &record); err != nil {
			return err
		}
		if record.LeaseUntilMS > time.Now().UnixMilli() {
			return errors.New("restore view pin still active after Open completed")
		}
		views++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if views != 0 {
		t.Fatalf("owner-backed Local restore left %d remote view pins, want none", views)
	}
}

type localPinTestLease struct {
	renewFunc func(context.Context) error
	closeFunc func() error
}

func (p localPinTestLease) Renew(ctx context.Context, _ time.Duration) error { return p.renewFunc(ctx) }
func (p localPinTestLease) Close(context.Context) error                      { return p.closeFunc() }

func TestLocalRestorePinConfirmHasBoundedRenewal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pins := newLocalRestorePins(context.Background())
		defer pins.close()
		pins.pin = &localRestorePin{pin: localPinTestLease{renewFunc: func(ctx context.Context) error {
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("confirmation renewal has no deadline")
			}
			<-ctx.Done()
			return ctx.Err()
		}, closeFunc: func() error { return nil }}}
		started := time.Now()
		if err := pins.confirm(); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("confirmation error=%v", err)
		}
		if time.Since(started) != localRestorePinRenewBudget || localRestorePinRenewBudget != 20*time.Second {
			t.Fatal("confirmation exceeded the 20-second renewal budget")
		}
		if !errors.Is(context.Cause(pins.ctx), context.DeadlineExceeded) {
			t.Fatalf("restore did not abort: %v", context.Cause(pins.ctx))
		}
	})
}

func TestLocalRestoreRenewalTimingAndMutexBudget(t *testing.T) {
	if localRestorePinRenewInterval != 20*time.Second || localRestorePinRenewInterval+localRestorePinRenewBudget+30*time.Second >= localRestorePinLease-localRestorePinLease/4 {
		t.Fatal("renewal timing consumes the minimum lease margin")
	}
	synctest.Test(t, func(t *testing.T) {
		pins := newLocalRestorePins(context.Background())
		defer pins.close()
		entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		calls := 0
		guard := &localRestorePin{pin: localPinTestLease{renewFunc: func(context.Context) error {
			calls++
			close(entered)
			<-release
			return nil
		}, closeFunc: func() error { return nil }}}
		pins.pin = guard
		go func() { done <- guard.renew(context.Background()) }()
		<-entered
		started := time.Now()
		if err := pins.confirm(); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("mutex wait ignored renewal deadline: %v", err)
		}
		if time.Since(started) != localRestorePinRenewBudget || calls != 1 {
			t.Fatal("renewal started another RPC after its lock-wait budget")
		}
		close(release)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

type localGCCancelledDeleteBucket struct {
	objstore.Bucket
	target string
	cancel context.CancelFunc
}

func (b *localGCCancelledDeleteBucket) Delete(ctx context.Context, name string) error {
	if name == b.target {
		b.cancel()
		return context.Canceled
	}
	return b.Bucket.Delete(ctx, name)
}

func TestLocalGCLogsUncertainDeleteDespiteCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	config := types.ExecutionConfig{Local: true, NodeID: "local", ClusterID: "gc-log", DataDir: t.TempDir(), ObjStoreProvider: "filesystem", ObjStoreDir: t.TempDir(), ObjStoreSyncInterval: time.Hour, CheckpointInterval: time.Hour, ObjStoreGCGracePeriod: time.Nanosecond}
	n := New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		// Avoid a final sync against this deliberately poisoned disposable gate.
		n.localBackupReady = false
		_ = n.Shutdown()
	}()
	if _, err := n.server.Execute(ctx, network.ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE log_rows (id INTEGER)"}); err != nil {
		t.Fatal(err)
	}
	if err := n.reclaimLocalCheckpoint(ctx); err != nil {
		t.Fatal(err)
	}
	target := path.Join(n.localBackupPrefix(), "extents", fmt.Sprintf("%064x", 101))
	if err := n.bucket.Upload(ctx, target, bytes.NewReader([]byte("orphan extent"))); err != nil {
		t.Fatal(err)
	}
	gcCtx, stopGC := context.WithCancel(ctx)
	defer stopGC()
	raw := n.bucket.Bucket
	n.bucket.Bucket = &localGCCancelledDeleteBucket{Bucket: raw, target: target, cancel: stopGC}
	var output bytes.Buffer
	oldOutput := log.Writer()
	log.SetOutput(&output)
	defer log.SetOutput(oldOutput)
	n.collectLocalBackup(gcCtx)
	if gcCtx.Err() == nil {
		t.Fatal("GC was not cancelled")
	}
	if !strings.Contains(output.String(), "Local maintenance remains held after uncertain work") || !strings.Contains(output.String(), "context canceled") {
		t.Fatalf("lost held-gate diagnostic during cancellation: %s", output.String())
	}
	reader, err := raw.Get(ctx, path.Join(n.localBackupPrefix(), "local-gc-control"))
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	var control struct {
		Token string `json:"token"`
	}
	if readErr != nil || closeErr != nil || json.Unmarshal(data, &control) != nil || control.Token == "" {
		t.Fatalf("uncertain gate was reset: %s", data)
	}
}

func TestLocalRestorePinLossCancelsWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lost := errors.New("pin fenced")
		closed := false
		pins := newLocalRestorePins(context.Background())
		started := time.Now()
		guard := &localRestorePin{pin: localPinTestLease{renewFunc: func(ctx context.Context) error {
			deadline, ok := ctx.Deadline()
			if time.Since(started) != localRestorePinRenewInterval || !ok || deadline.Sub(time.Now()) != localRestorePinRenewBudget {
				t.Error("watcher did not use the 20-second interval and budget")
			}
			return lost
		}, closeFunc: func() error { closed = true; return nil }}}
		pins.pin = guard
		pins.watch(guard)
		time.Sleep(localRestorePinLease / 2)
		if !errors.Is(context.Cause(pins.ctx), lost) {
			t.Fatalf("restore continued after renewal loss: %v", context.Cause(pins.ctx))
		}
		if err := pins.close(); err != nil || !closed {
			t.Fatalf("pin cleanup: closed=%v error=%v", closed, err)
		}
	})
}

func TestLocalRestorePinCloseDrainsRenewal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		closed := false
		pins := newLocalRestorePins(context.Background())
		guard := &localRestorePin{pin: localPinTestLease{renewFunc: func(ctx context.Context) error {
			close(entered)
			<-release
			return ctx.Err()
		}, closeFunc: func() error { closed = true; return nil }}}
		pins.pin = guard
		pins.watch(guard)
		time.Sleep(localRestorePinLease / 2)
		<-entered
		done := make(chan error, 1)
		go func() { done <- pins.close() }()
		synctest.Wait()
		if closed {
			t.Fatal("pin released before outstanding renewal drained")
		}
		close(release)
		if err := <-done; err != nil || !closed {
			t.Fatalf("pin cleanup: closed=%v error=%v", closed, err)
		}
	})
}

type localGCDrainBucket struct {
	objstore.Bucket
	target      string
	entered     chan struct{}
	release     chan struct{}
	drained     chan struct{}
	once        sync.Once
	closedEarly atomic.Bool
}

func (b *localGCDrainBucket) Delete(ctx context.Context, name string) error {
	if name == b.target {
		b.once.Do(func() { close(b.entered) })
		<-b.release
		defer close(b.drained)
	}
	return b.Bucket.Delete(ctx, name)
}

func (b *localGCDrainBucket) Close() error {
	select {
	case <-b.entered:
		select {
		case <-b.drained:
		default:
			b.closedEarly.Store(true)
		}
	default:
	}
	return b.Bucket.Close()
}

func TestLocalGCInterruptionDrainsBeforeGateReleaseAndClose(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(fmt.Sprintf("shutdown=%v", shutdown), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			config := types.ExecutionConfig{Local: true, NodeID: "local", ClusterID: "gc-drain", DataDir: t.TempDir(), ObjStoreProvider: "filesystem", ObjStoreDir: t.TempDir(), ObjStoreSyncInterval: time.Hour, CheckpointInterval: time.Hour, ObjStoreGCGracePeriod: time.Nanosecond}
			n := New(&config)
			if err := n.Open(ctx); err != nil {
				t.Fatal(err)
			}
			defer n.Shutdown()
			if _, err := n.server.Execute(ctx, network.ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE gc_rows (id INTEGER)"}); err != nil {
				t.Fatal(err)
			}
			if err := n.reclaimLocalCheckpoint(ctx); err != nil {
				t.Fatal(err)
			}
			target := path.Join(n.localBackupPrefix(), "extents", fmt.Sprintf("%064x", 99))
			if err := n.bucket.Upload(ctx, target, bytes.NewReader([]byte("orphan extent"))); err != nil {
				t.Fatal(err)
			}
			raw := n.bucket.Bucket
			boundary := &localGCDrainBucket{Bucket: raw, target: target, entered: make(chan struct{}), release: make(chan struct{}), drained: make(chan struct{})}
			n.bucket.Bucket = boundary
			var release sync.Once
			defer release.Do(func() { close(boundary.release) })
			gcCtx, stopGC := context.WithCancel(ctx)
			defer stopGC()
			done := make(chan struct{})
			n.wg.Add(1)
			go func() { defer n.wg.Done(); n.collectLocalBackup(gcCtx); close(done) }()
			select {
			case <-boundary.entered:
			case <-ctx.Done():
				t.Fatal("GC did not dispatch orphan deletion")
			}
			var shutdownDone chan error
			if shutdown {
				shutdownDone = make(chan error, 1)
				go func() { shutdownDone <- n.Shutdown() }()
			} else {
				stopGC()
			}
			select {
			case <-done:
				t.Fatal("GC returned before its admitted deletion drained")
			case <-time.After(30 * time.Millisecond):
			}
			reader, err := raw.Get(ctx, path.Join(n.localBackupPrefix(), "local-gc-control"))
			if err != nil {
				t.Fatal(err)
			}
			data, readErr := io.ReadAll(reader)
			closeErr := reader.Close()
			var control struct {
				Token string `json:"token"`
			}
			if readErr != nil || closeErr != nil || json.Unmarshal(data, &control) != nil || control.Token == "" {
				t.Fatalf("gate released before deletion resolved: %s", data)
			}
			release.Do(func() { close(boundary.release) })
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("GC did not drain")
			}
			if shutdown {
				select {
				case err := <-shutdownDone:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal("shutdown did not drain")
				}
			}
			if boundary.closedEarly.Load() {
				t.Fatal("bucket closed before deletion drained")
			}
			var inspector *rhizaobjstore.MeteredBucket
			inspectionBucket := raw
			if shutdown {
				inspector, err = rhizaobjstore.NewBucket(rhizaobjstore.Config{Provider: rhizaobjstore.ProviderFilesystem, FilesystemDir: config.ObjStoreDir})
				if err != nil {
					t.Fatal(err)
				}
				defer inspector.Close()
				inspectionBucket = inspector.Bucket
			}
			reader, err = inspectionBucket.Get(ctx, path.Join(n.localBackupPrefix(), "local-gc-control"))
			if err != nil {
				t.Fatal(err)
			}
			data, readErr = io.ReadAll(reader)
			closeErr = reader.Close()
			if readErr != nil || closeErr != nil || json.Unmarshal(data, &control) != nil || control.Token != "" {
				t.Fatalf("gate not released after deletion drained: %s", data)
			}
		})
	}
}

func TestLocalGCUsesRetainCountAfterCheckpointAndPeriodicInterval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	config := types.ExecutionConfig{Local: true, NodeID: "local", ClusterID: "gc-policy", DataDir: t.TempDir(), ObjStoreProvider: "filesystem", ObjStoreDir: t.TempDir(), ObjStoreSyncInterval: time.Hour, CheckpointInterval: time.Hour, CheckpointRetainCount: 1, ObjStoreGCGracePeriod: time.Nanosecond}
	n := New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = n.Shutdown() }()
	if _, err := n.server.Execute(ctx, network.ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE policy_rows (id INTEGER)"}); err != nil {
		t.Fatal(err)
	}
	if err := n.reclaimLocalCheckpoint(ctx); err != nil {
		t.Fatal(err)
	}
	old := n.localBackup.Snapshot()
	if _, err := n.server.Execute(ctx, network.ExecuteRequest{RequestID: "insert", SQL: "INSERT INTO policy_rows VALUES (1)"}); err != nil {
		t.Fatal(err)
	}
	if err := n.reclaimLocalCheckpoint(ctx); err != nil {
		t.Fatal(err)
	}
	manager, err := n.localCheckpointManager(old.Seal.ConfigID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.OpenRoot(ctx, uint64(old.Seal.Index), old.PortableRoot); err == nil {
		t.Fatal("post-checkpoint GC ignored retain count")
	}
	current := n.localBackup.Snapshot()
	if _, err := manager.OpenRoot(ctx, uint64(current.Seal.Index), current.PortableRoot); err != nil {
		t.Fatalf("current snapshot was collected: %v", err)
	}
	target := path.Join(n.localBackupPrefix(), "extents", fmt.Sprintf("%064x", 100))
	if err := n.bucket.Upload(ctx, target, bytes.NewReader([]byte("periodic orphan"))); err != nil {
		t.Fatal(err)
	}
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	config.ObjStoreGCInterval = 20 * time.Millisecond
	n = New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatal(err)
	}
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	for {
		exists, err := n.bucket.Exists(ctx, target)
		if err != nil {
			t.Fatal(err)
		}
		if !exists {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("periodic GC did not collect orphan")
		case <-poll.C:
		}
	}
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(config.DataDir); err != nil {
		t.Fatal(err)
	}
	config.ObjStoreGCInterval = time.Hour
	n = New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatalf("whole-loss restore after GC: %v", err)
	}
	rows, err := n.server.Query(ctx, network.QueryRequest{SQL: "SELECT id FROM policy_rows"})
	if err != nil || len(rows.Rows) != 1 {
		t.Fatalf("GC whole-loss rows=%v error=%v", rows.Rows, err)
	}
}

func TestLocalGCPolicyReopenPreservesIdentityAndPinnedSnapshots(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	config := types.ExecutionConfig{Local: true, NodeID: "local", ClusterID: "gc-reopen", DataDir: t.TempDir(), ObjStoreProvider: "filesystem", ObjStoreDir: t.TempDir(), ObjStoreSyncInterval: time.Hour, CheckpointInterval: time.Hour, ObjStoreGCGracePeriod: time.Hour}
	n := New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = n.Shutdown() }()
	var snapshots []recovery.LocalSnapshot
	for i, sql := range []string{"CREATE TABLE reopen_rows (id INTEGER)", "INSERT INTO reopen_rows VALUES (1)", "INSERT INTO reopen_rows VALUES (2)"} {
		if _, err := n.server.Execute(ctx, network.ExecuteRequest{RequestID: fmt.Sprintf("reopen-%d", i), SQL: sql}); err != nil {
			t.Fatal(err)
		}
		if err := n.reclaimLocalCheckpoint(ctx); err != nil {
			t.Fatal(err)
		}
		snapshots = append(snapshots, *n.localBackup.Snapshot())
	}
	policy, digest := n.localStore.policy, n.localStore.configDigest
	binding, tip := n.localBackup.Binding, n.localBackup.Tip()
	// Pin through the current owner. A second Local Node cannot own this same
	// filesystem root concurrently; Local filesystem startup uses its lifetime
	// owner rather than a second process's remote view pin.
	pin, err := n.localBackup.PinRestore(ctx, snapshots[0], "policy-reopen", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	config.CheckpointRetainCount = 1
	config.CheckpointInterval = 2 * time.Hour
	config.ObjStoreGCInterval = 20 * time.Millisecond
	config.ObjStoreGCGracePeriod = time.Nanosecond
	if err := n.localBackup.GarbageCollect(ctx, config.CheckpointRetainCount, config.ObjStoreGCGracePeriod); err != nil {
		t.Fatal(err)
	}
	manager, err := n.localCheckpointManager(binding.Bootstrap.ConfigID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.OpenRoot(ctx, uint64(snapshots[1].Seal.Index), snapshots[1].PortableRoot); err == nil {
		t.Fatal("reduced retention kept the unpinned middle snapshot")
	}
	for _, snapshot := range []recovery.LocalSnapshot{snapshots[0], snapshots[2]} {
		if _, err := manager.OpenRoot(ctx, uint64(snapshot.Seal.Index), snapshot.PortableRoot); err != nil {
			t.Fatalf("GC removed pinned or current snapshot while owner was live: %v", err)
		}
	}
	if err := pin.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.OpenRoot(ctx, uint64(snapshots[0].Seal.Index), snapshots[0].PortableRoot); err != nil {
		t.Fatalf("pin close removed the old root before GC: %v", err)
	}
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	n = New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatalf("reopen after changing operational policy: %v", err)
	}
	if n.localStore.policy != policy || n.localStore.configDigest != digest || !reflect.DeepEqual(n.localBackup.Binding, binding) || n.localBackup.Tip() != tip || !reflect.DeepEqual(*n.localBackup.Snapshot(), snapshots[2]) {
		t.Fatal("operational policy changed identity, archive binding, or current backup")
	}
	manager, err = n.localCheckpointManager(binding.Bootstrap.ConfigID)
	if err != nil {
		t.Fatal(err)
	}
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	for {
		_, err := manager.OpenRoot(ctx, uint64(snapshots[0].Seal.Index), snapshots[0].PortableRoot)
		if err != nil && n.bucket.IsObjNotFoundErr(err) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("reopened GC schedule did not collect the released old snapshot")
		case <-poll.C:
		}
	}
	if _, err := manager.OpenRoot(ctx, uint64(snapshots[2].Seal.Index), snapshots[2].PortableRoot); err != nil {
		t.Fatalf("GC removed current snapshot after reopen: %v", err)
	}
	rows, err := n.server.Query(ctx, network.QueryRequest{SQL: "SELECT id FROM reopen_rows ORDER BY id"})
	if err != nil || len(rows.Rows) != 2 {
		t.Fatalf("reopened data=%v error=%v", rows.Rows, err)
	}
}

func TestLocalGCRestorePinLeaseProtectsRootAcrossNodeReopen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	config := types.ExecutionConfig{Local: true, NodeID: "local", ClusterID: "gc-pin-reopen", DataDir: t.TempDir(), ObjStoreProvider: "filesystem", ObjStoreDir: t.TempDir(), ObjStoreSyncInterval: time.Hour, CheckpointInterval: time.Hour, CheckpointRetainCount: 3, ObjStoreGCInterval: time.Hour, ObjStoreGCGracePeriod: time.Hour}
	n1 := New(&config)
	if err := n1.Open(ctx); err != nil {
		t.Fatal(err)
	}
	n1Open := true
	defer func() {
		if n1Open {
			if err := n1.Shutdown(); err != nil {
				t.Errorf("close first Local owner: %v", err)
			}
		}
	}()

	var snapshots []recovery.LocalSnapshot
	for i, sql := range []string{"CREATE TABLE reopen_pin_rows (id INTEGER)", "INSERT INTO reopen_pin_rows VALUES (1)", "INSERT INTO reopen_pin_rows VALUES (2)"} {
		ack, err := n1.server.Execute(ctx, network.ExecuteRequest{RequestID: fmt.Sprintf("pin-reopen-%d", i), SQL: sql})
		if err != nil || ack.Status != types.MutationCommitted || ack.Slot == 0 {
			t.Fatalf("before-ack mutation=%d receipt=%+v err=%v", i, ack.MutationReceipt, err)
		}
		if uint64(n1.localBackup.Tip()) < ack.Slot || n1.localBackup.Tip() != n1.core.Tip() {
			t.Fatalf("before-ack mutation=%d archive tip=%d core tip=%d ACK slot=%d", i, n1.localBackup.Tip(), n1.core.Tip(), ack.Slot)
		}
		if err := n1.reclaimLocalCheckpoint(ctx); err != nil {
			t.Fatal(err)
		}
		snapshots = append(snapshots, *n1.localBackup.Snapshot())
	}

	const pinLease = 5 * time.Second
	pin, err := n1.localBackup.PinRestore(ctx, snapshots[0], "gc-pin-across-reopen", pinLease)
	if err != nil {
		t.Fatal(err)
	}
	_ = pin // The durable lease intentionally outlives n1; never call this stale handle after shutdown.
	viewPrefix := path.Join(n1.localBackupPrefix(), "view-pins")
	var pinKey string
	var leaseUntilMS int64
	viewPinCount := 0
	if err := n1.bucket.Iter(ctx, viewPrefix, func(name string) error {
		reader, err := n1.bucket.Get(ctx, name)
		if err != nil {
			return err
		}
		data, readErr := io.ReadAll(reader)
		closeErr := reader.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return err
		}
		var record struct {
			LeaseUntilMS int64 `json:"lease_until_unix_ms"`
		}
		if err := json.Unmarshal(data, &record); err != nil {
			return err
		}
		pinKey, leaseUntilMS = name, record.LeaseUntilMS
		viewPinCount++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if viewPinCount != 1 || leaseUntilMS <= time.Now().Add(time.Second).UnixMilli() {
		t.Fatalf("persisted view pins=%d deadline=%s, want one lease with >1s remaining", viewPinCount, time.UnixMilli(leaseUntilMS))
	}
	leaseDeadline := time.UnixMilli(leaseUntilMS)
	if err := n1.Shutdown(); err != nil {
		t.Fatal(err)
	}
	n1Open = false
	pin = nil

	// Read the durable record through a fresh, unowned inspector after n1 has
	// released its lifetime owner. Its original deadline must survive shutdown.
	inspector, err := rhizaobjstore.NewBucket(rhizaobjstore.Config{Provider: rhizaobjstore.ProviderFilesystem, FilesystemDir: config.ObjStoreDir})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := inspector.Bucket.Get(ctx, pinKey)
	if err != nil {
		if closeErr := inspector.Close(); closeErr != nil {
			t.Fatalf("read durable pin after owner shutdown: %v; close inspector: %v", err, closeErr)
		}
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if err := errors.Join(readErr, closeErr, inspector.Close()); err != nil {
		t.Fatal(err)
	}
	var persisted struct {
		LeaseUntilMS int64 `json:"lease_until_unix_ms"`
	}
	if err := json.Unmarshal(data, &persisted); err != nil || persisted.LeaseUntilMS != leaseUntilMS {
		t.Fatalf("view pin after owner shutdown=%s err=%v, want unchanged deadline %s", data, err, leaseDeadline)
	}

	config.CheckpointRetainCount = 1
	config.ObjStoreGCInterval = 20 * time.Millisecond
	config.ObjStoreGCGracePeriod = time.Nanosecond
	n2 := New(&config)
	if err := n2.Open(ctx); err != nil {
		t.Fatalf("reopen Local owner while restore pin is live: %v", err)
	}
	n2Open := true
	defer func() {
		if n2Open {
			if err := n2.Shutdown(); err != nil {
				t.Errorf("close second Local owner: %v", err)
			}
		}
	}()
	if time.Until(leaseDeadline) < time.Second {
		t.Fatalf("fixture pin lease nearly expired before second owner opened: deadline=%s remaining=%s", leaseDeadline, time.Until(leaseDeadline))
	}
	manager, err := n2.localCheckpointManager(n2.localBackup.Binding.Bootstrap.ConfigID)
	if err != nil {
		t.Fatal(err)
	}
	if err := n2.localBackup.GarbageCollect(ctx, config.CheckpointRetainCount, config.ObjStoreGCGracePeriod); err != nil {
		t.Fatalf("GC while durable restore lease is active: %v", err)
	}
	for _, snapshot := range []recovery.LocalSnapshot{snapshots[0], snapshots[2]} {
		if _, err := manager.OpenRoot(ctx, uint64(snapshot.Seal.Index), snapshot.PortableRoot); err != nil {
			t.Fatalf("GC removed pinned or current snapshot before lease deadline %s: %v", leaseDeadline, err)
		}
	}
	if time.Now().After(leaseDeadline) {
		t.Fatalf("persisted lease expired during active-root assertion: deadline=%s", leaseDeadline)
	}

	wait := time.Until(leaseDeadline) + 25*time.Millisecond
	timer := time.NewTimer(wait)
	select {
	case <-ctx.Done():
		timer.Stop()
		t.Fatalf("waiting for natural restore-pin expiry: %v", ctx.Err())
	case <-timer.C:
	}
	if time.Now().Before(leaseDeadline) {
		t.Fatalf("restore-pin expiry wait ended before stored deadline %s", leaseDeadline)
	}
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	for {
		_, err := manager.OpenRoot(ctx, uint64(snapshots[0].Seal.Index), snapshots[0].PortableRoot)
		if err != nil && n2.bucket.IsObjNotFoundErr(err) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("scheduled GC did not collect the expired-pin root")
		case <-poll.C:
		}
	}
	if _, err := manager.OpenRoot(ctx, uint64(snapshots[2].Seal.Index), snapshots[2].PortableRoot); err != nil {
		t.Fatalf("expired-pin GC removed current root: %v", err)
	}
	rows, err := n2.server.Query(ctx, network.QueryRequest{SQL: "SELECT id FROM reopen_pin_rows ORDER BY id"})
	if err != nil || len(rows.Rows) != 2 {
		t.Fatalf("rows after lease expiry and GC=%v error=%v", rows.Rows, err)
	}
	if err := n2.Shutdown(); err != nil {
		t.Fatal(err)
	}
	n2Open = false
}

func TestLocalBackupPeriodicCheckpointSkipsIdleSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	config := types.ExecutionConfig{Local: true, NodeID: "local", ClusterID: "idle-gc", DataDir: t.TempDir(), ObjStoreProvider: "filesystem", ObjStoreDir: t.TempDir(), ObjStoreSyncInterval: time.Hour, CheckpointInterval: 20 * time.Millisecond}
	n := New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatal(err)
	}
	defer n.Shutdown()
	if _, err := n.server.Execute(ctx, network.ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE idle_rows (id INTEGER PRIMARY KEY)"}); err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	for n.localBackup.Snapshot() == nil {
		select {
		case <-deadline.C:
			t.Fatal("periodic checkpoint did not publish a snapshot")
		case <-poll.C:
		}
	}
	before := n.localBackup.Snapshot()
	time.Sleep(120 * time.Millisecond)
	after := n.localBackup.Snapshot()
	if before.Decision.Slot != after.Decision.Slot || before.PortableRoot != after.PortableRoot {
		t.Fatalf("idle checkpoint advanced snapshot from %d to %d", before.Decision.Slot, after.Decision.Slot)
	}
}

func TestLocalGCConfigPreservesNonLocalDefaults(t *testing.T) {
	config := types.ExecutionConfig{}
	before := config
	if err := validateLocalGCConfig(&config); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(config, before) {
		t.Fatalf("non-Local configuration changed: %+v", config)
	}
}

func TestLocalGCConfigInvalidBeforeIO(t *testing.T) {
	for _, test := range []struct {
		name string
		set  func(*types.ExecutionConfig)
		want string
	}{
		{"count", func(c *types.ExecutionConfig) { c.CheckpointRetainCount = -1 }, "retain count"},
		{"checkpoint interval", func(c *types.ExecutionConfig) { c.CheckpointInterval = -time.Second }, "checkpoint interval"},
		{"tail", func(c *types.ExecutionConfig) { c.CheckpointTailBytes = -1 }, "tail bytes"},
		{"GC interval", func(c *types.ExecutionConfig) { c.ObjStoreGCInterval = -time.Second }, "GC durations"},
		{"grace", func(c *types.ExecutionConfig) { c.ObjStoreGCGracePeriod = -time.Second }, "GC durations"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			config := types.ExecutionConfig{Local: true, NodeID: "local", ClusterID: "gc", DataDir: filepath.Join(root, "data"), ObjStoreProvider: "filesystem", ObjStoreDir: filepath.Join(root, "objects")}
			test.set(&config)
			if err := New(&config).Open(context.Background()); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Open error=%v, want %q", err, test.want)
			}
			entries, err := os.ReadDir(root)
			if err != nil || len(entries) != 0 {
				t.Fatalf("validation performed filesystem I/O: entries=%v error=%v", entries, err)
			}
		})
	}
}
