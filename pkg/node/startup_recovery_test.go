package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	objmetrics "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/objstore/versityfixture"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

type noCASBucket struct{ objstore.Bucket }

func (noCASBucket) SupportedObjectUploadOptions() []objstore.ObjectUploadOptionType { return nil }

func TestStartupRecoveryGuardReloadsFirstArchiveHead(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	stale := recovery.NewManager(bucket, "cluster", 1)
	if err := stale.Load(ctx); err != nil {
		t.Fatal(err)
	}

	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{
		NodeID:  "n1",
		Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}},
		WAL:     wal,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, []byte("first archive decision")); err != nil {
		t.Fatal(err)
	}
	writer := recovery.NewManager(bucket, "cluster", 1)
	if err := writer.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}

	guard, err := newStartupRecoveryGuard(ctx, stale, "startup-test")
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Close()
	snapshot := guard.Snapshot()
	if snapshot == nil || snapshot.Tip() != core.Tip() {
		t.Fatalf("snapshot tip=%v, want %d after stale empty load", snapshot, core.Tip())
	}
	values, _, err := snapshot.DecisionsFrom(guard.Context(), 1, 1)
	if err != nil || len(values) != 1 {
		t.Fatalf("snapshot values=%d err=%v", len(values), err)
	}
}

func TestStartupRecoveryGuardSkipsSingleNodeArchiveWithoutCAS(t *testing.T) {
	guard, err := newStartupRecoveryGuard(context.Background(), recovery.NewManager(noCASBucket{objstore.NewInMemBucket()}, "cluster", 1), "startup-test")
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Close()
	if guard.Snapshot() != nil {
		t.Fatal("non-CAS archive unexpectedly created a shared recovery pin")
	}
}

func TestStartupRecoveryPinRefusesForeignLivePublisher(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	manager := checkpoint.NewManager(bucket, "startup-refusal", t.TempDir(), 1)
	file := filepath.Join(t.TempDir(), "sqlite.db")
	policySnapshot(t, file, "certified-root")
	initial, err := manager.AcquirePublisherClaim(ctx, "initial", 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	root, err := manager.CreateFiles(ctx, initial, []checkpoint.Source{{Role: checkpoint.RoleSQLite, Path: file}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.ReleasePublisherClaim(ctx, initial); err != nil {
		t.Fatal(err)
	}
	foreign, err := manager.AcquirePublisherClaim(ctx, "foreign-live-publisher", root.Index, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.ReleasePublisherClaim(ctx, foreign)
	key := "startup-refusal/checkpoint/PUBLISHER"
	before, version := startupClaimBytes(t, ctx, bucket, key)
	var observations []checkpoint.PublisherBusyObservation
	observed := checkpoint.WithPublisherBusyObserver(ctx, func(event checkpoint.PublisherBusyObservation) { observations = append(observations, event) })
	guard, err := newStartupRecoveryGuard(observed, nil, "startup-test")
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Close()
	if _, err := manager.PinRecoveryRoot(guard.Context(), root, guard.Owner(), startupRecoveryLease); !errors.Is(err, checkpoint.ErrPublisherBusy) || !errors.Is(err, checkpoint.ErrRecoveryAdmissionBusy) {
		t.Fatalf("pin refusal=%v", err)
	}
	assertStartupPublisherRefusal(t, observations, foreign.Generation)
	after, afterVersion := startupClaimBytes(t, ctx, bucket, key)
	if !bytes.Equal(before, after) || !reflect.DeepEqual(version, afterVersion) || guard.root != nil {
		t.Fatal("refused pin changed the live claim or installed a recovery pin")
	}
}

// The sealed before-fix version proves immediate refusal. This version releases
// the verified foreign claim at an explicit barrier and requires real Open to
// retain the fenced pin and recover an exact floor plus certified SQL suffix.
func TestNodeOpenWaitsForForeignPublisherBeforeWarmRestoreS3(t *testing.T) {
	binary := os.Getenv("RHIZA_VERSITYGW_BIN")
	if binary == "" {
		t.Skip("set RHIZA_VERSITYGW_BIN to the pinned local Versity binary")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	gateway, err := versityfixture.Start(ctx, binary, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, stop := context.WithTimeout(context.Background(), 7*time.Second)
		defer stop()
		if err := gateway.Close(closeCtx); err != nil {
			t.Errorf("reap local gateway: %v", err)
		}
	}()
	client, _, err := gateway.NewS3Client()
	if err != nil {
		t.Fatal(err)
	}
	for _, atFloor := range []bool{false, true} {
		t.Run(fmt.Sprintf("materializer_at_floor=%t", atFloor), func(t *testing.T) {
			bucketName := fmt.Sprintf("rhiza-startup-%d", time.Now().UnixNano())
			if err := client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{Region: "us-east-1"}); err != nil {
				t.Fatal(err)
			}
			const clusterID = "startup-recovery-refusal"
			config := &types.ExecutionConfig{
				DataDir: t.TempDir(), ClusterID: clusterID, NodeID: "n1",
				BindAddr: "127.0.0.1:0", PeerAddr: "127.0.0.1:0", PeerToken: "startup-peer",
				Members:          []quepaxa.Member{peerMember(clusterID, "n1", "startup-peer")},
				ObjStoreProvider: "s3", ObjStoreEndpoint: strings.TrimPrefix(gateway.Endpoint, "http://"),
				ObjStoreBucket: bucketName, ObjStorePrefix: "startup", ObjStoreRegion: "us-east-1", ObjStoreInsecure: true,
				ObjStoreAccessKey: gateway.AccessKey, ObjStoreSecretKey: gateway.SecretKey,
				ObjStoreDurability:   types.ObjectStoreDurabilityBeforeAck,
				ObjStoreSyncInterval: time.Hour, CheckpointInterval: time.Hour,
			}
			first := New(config)
			defer first.Shutdown()
			if err := first.Open(ctx); err != nil {
				t.Fatal(err)
			}
			first.checkpointer.Stop()
			if _, err := first.server.Execute(ctx, network.ExecuteRequest{RequestID: "base", SQL: "CREATE TABLE restart_proof (id INTEGER PRIMARY KEY, value TEXT)"}); err != nil {
				t.Fatal(err)
			}
			if err := first.checkpointer.CheckpointOnShutdown(ctx, first.material.StateTip()); err != nil {
				t.Fatal(err)
			}
			root := first.checkpoints.Latest()
			seal, sealed, err := first.core.LatestCheckpointSeal()
			if err != nil || !sealed || root == nil || uint64(seal.Index) != root.Index || seal.RootHash != root.RootHash || first.core.CompactionFloor() != seal.Index {
				t.Fatalf("certified floor root=%v sealed=%t seal=%d floor=%d err=%v", root, sealed, seal.Index, first.core.CompactionFloor(), err)
			}
			if _, err := first.server.Execute(ctx, network.ExecuteRequest{RequestID: "suffix", SQL: "INSERT INTO restart_proof VALUES (1, 'certified-suffix')"}); err != nil {
				t.Fatal(err)
			}
			identity, through := first.core.WALIdentity(), first.core.Tip()
			if err := first.Shutdown(); err != nil {
				t.Fatal(err)
			}
			bucket, err := objmetrics.NewBucket(objmetrics.Config{
				Provider: objmetrics.ProviderS3, Endpoint: config.ObjStoreEndpoint, Bucket: bucketName,
				Region: config.ObjStoreRegion, Insecure: true, AccessKey: gateway.AccessKey, SecretKey: gateway.SecretKey,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer bucket.Close()
			prefix := path.Join(config.ObjStorePrefix, clusterID)
			manager := checkpoint.NewManager(bucket, prefix, t.TempDir(), 1)
			if err := manager.Load(ctx); err != nil {
				t.Fatal(err)
			}
			if atFloor {
				files, err := manager.DownloadRootFiles(ctx, root.Index, root.RootHash, t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				material, err := materializer.Open(filepath.Join(config.DataDir, "sqlite.db"), 1)
				if err != nil {
					t.Fatal(err)
				}
				var restore []materializer.CheckpointFile
				for _, file := range files {
					restore = append(restore, materializer.CheckpointFile{Role: materializer.CheckpointRole(file.Role), Path: file.Path})
				}
				restoreErr := material.RestoreCheckpoint(ctx, restore)
				tip := material.Tip()
				closeErr := material.Close()
				if restoreErr != nil || closeErr != nil || tip != root.Index {
					t.Fatalf("exact floor materializer tip=%d root=%d restore=%v close=%v", tip, root.Index, restoreErr, closeErr)
				}
			}
			foreign, err := manager.AcquirePublisherClaim(ctx, "foreign-live-publisher", root.Index, 2*time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			released := false
			defer func() {
				if !released {
					_ = manager.ReleasePublisherClaim(ctx, foreign)
				}
			}()
			key := path.Join(prefix, "checkpoint/PUBLISHER")
			before, version := startupClaimBytes(t, ctx, bucket, key)
			currentKey := path.Join(prefix, "checkpoint/CURRENT")
			currentBefore, currentVersion := startupClaimBytes(t, ctx, bucket, currentKey)
			cancelCtx, stopCanceled := context.WithCancel(ctx)
			canceledAttempts := 0
			cancelObserved := checkpoint.WithPublisherBusyObserver(cancelCtx, func(event checkpoint.PublisherBusyObservation) {
				if event.Source == "publisher_claim_active" {
					canceledAttempts++
					stopCanceled()
				}
			})
			canceledNode := New(config)
			cancelErr := canceledNode.Open(cancelObserved)
			stopCanceled()
			if !errors.Is(cancelErr, context.Canceled) || canceledAttempts != 1 || canceledNode.Ready() {
				t.Fatalf("canceled Open err=%v attempts=%d ready=%t", cancelErr, canceledAttempts, canceledNode.Ready())
			}
			if err := canceledNode.Shutdown(); err != nil {
				t.Fatal(err)
			}
			var observations []checkpoint.PublisherBusyObservation
			blocked := make(chan struct{}, 1)
			observed := checkpoint.WithPublisherBusyObserver(ctx, func(event checkpoint.PublisherBusyObservation) {
				if event.Source == "publisher_claim_active" {
					observations = append(observations, event)
					select {
					case blocked <- struct{}{}:
					default:
					}
				}
			})
			restarted := New(config)
			openCtx, stopOpen := context.WithCancel(observed)
			openDone := make(chan error, 1)
			joined := false
			defer func() {
				stopOpen()
				if !joined {
					<-openDone
				}
				if err := restarted.Shutdown(); err != nil {
					t.Errorf("reap restarted node: %v", err)
				}
			}()
			go func() { openDone <- restarted.Open(openCtx) }()
			select {
			case <-blocked:
			case err := <-openDone:
				joined = true
				t.Fatalf("Open returned before controlled claim release: %v", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			after, afterVersion := startupClaimBytes(t, ctx, bucket, key)
			currentAfter, currentAfterVersion := startupClaimBytes(t, ctx, bucket, currentKey)
			if !bytes.Equal(before, after) || !reflect.DeepEqual(version, afterVersion) || restarted.Ready() {
				t.Fatal("blocked Open changed publisher evidence or became ready")
			}
			if !bytes.Equal(currentBefore, currentAfter) || !reflect.DeepEqual(currentVersion, currentAfterVersion) {
				t.Fatal("failed Open changed certified CURRENT")
			}
			// Production never releases the foreign claim; only this test holder does.
			if err := manager.ReleasePublisherClaim(ctx, foreign); err != nil {
				t.Fatal(err)
			}
			released = true
			err = <-openDone
			joined = true
			if err != nil {
				t.Fatalf("Open after controlled claim release: %v", err)
			}
			assertStartupPublisherRefusal(t, observations[:1], foreign.Generation)
			rows, err := restarted.server.Query(ctx, network.QueryRequest{SQL: "SELECT value FROM restart_proof WHERE id=1", Consistency: "linearizable"})
			if err != nil || len(rows.Rows) != 1 || len(rows.Rows[0]) != 1 || rows.Rows[0][0] != "certified-suffix" || restarted.core.WALIdentity() != identity || restarted.core.Tip() < through {
				t.Fatalf("identity/suffix recovery rows=%v err=%v tip=%d through=%d identity_equal=%t", rows.Rows, err, restarted.core.Tip(), through, restarted.core.WALIdentity() == identity)
			}
			t.Logf("RECOVERY at_floor=%t root_index=%d original_tip=%d admission_refusals=%d foreign_claim_unchanged_before_release=true SQL_suffix=true WAL_identity_equal=true", atFloor, root.Index, through, len(observations))
		})
	}
}

func startupPinFixture(t *testing.T, bucket objstore.Bucket) (*checkpoint.Manager, *checkpoint.Checkpoint) {
	t.Helper()
	manager := checkpoint.NewManager(bucket, "startup-bound", t.TempDir(), 1)
	file := filepath.Join(t.TempDir(), "sqlite.db")
	policySnapshot(t, file, "certified-root")
	claim, err := manager.AcquirePublisherClaim(context.Background(), "initial", 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	root, err := manager.CreateFiles(context.Background(), claim, []checkpoint.Source{{Role: checkpoint.RoleSQLite, Path: file}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.ReleasePublisherClaim(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	return manager, root
}

func TestStartupRecoveryAdmissionCancellation(t *testing.T) {
	for _, mode := range []string{"caller-cancel", "caller-deadline", "renewal-error"} {
		t.Run(mode, func(t *testing.T) {
			bucket := objstore.NewInMemBucket()
			manager, root := startupPinFixture(t, bucket)
			foreign, err := manager.AcquirePublisherClaim(context.Background(), "foreign-live-publisher", root.Index, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			defer manager.ReleasePublisherClaim(context.Background(), foreign)
			before, version := startupClaimBytes(t, context.Background(), bucket, "startup-bound/checkpoint/PUBLISHER")
			ctx, cancel := context.WithCancel(context.Background())
			if mode == "caller-deadline" {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
			}
			defer cancel()
			guard, err := newStartupRecoveryGuard(ctx, nil, "startup-test")
			if err != nil {
				t.Fatal(err)
			}
			defer guard.Close()
			renewErr := errors.New("test recovery snapshot renewal failed")
			attempts := 0
			observed := checkpoint.WithPublisherBusyObserver(ctx, func(event checkpoint.PublisherBusyObservation) {
				if event.Source != "publisher_claim_active" {
					return
				}
				attempts++
				switch mode {
				case "caller-cancel":
					cancel()
				case "renewal-error":
					// Exercise the renewal loop's existing error/cancel handoff.
					guard.mu.Lock()
					guard.err = renewErr
					guard.mu.Unlock()
					guard.cancel()
				}
			})
			_, err = guard.PinRoot(observed, manager, root, guard.Owner())
			want := error(context.Canceled)
			if mode == "caller-deadline" {
				want = context.DeadlineExceeded
			} else if mode == "renewal-error" {
				want = renewErr
			}
			if !errors.Is(err, want) || attempts != 1 || guard.root != nil {
				t.Fatalf("admission error=%v want=%v attempts=%d root=%v", err, want, attempts, guard.root)
			}
			guard.Close()
			select {
			case <-guard.done:
			default:
				t.Fatal("renewal worker was not joined")
			}
			after, afterVersion := startupClaimBytes(t, context.Background(), bucket, "startup-bound/checkpoint/PUBLISHER")
			if !bytes.Equal(before, after) || !reflect.DeepEqual(version, afterVersion) {
				t.Fatal("canceled admission mutated foreign claim")
			}
		})
	}
}

type startupPinFailureBucket struct {
	objstore.Bucket
	failAt string
	fail   error
	calls  int
}

func (b *startupPinFailureBucket) Attributes(ctx context.Context, key string) (objstore.ObjectAttributes, error) {
	if (b.failAt == "root" || b.failAt == "root-missing") && strings.Contains(key, "/checkpoint/roots/") {
		b.calls++
		if b.failAt == "root-missing" {
			return b.Bucket.Attributes(ctx, key+".missing")
		}
		return objstore.ObjectAttributes{}, b.fail
	}
	return b.Bucket.Attributes(ctx, key)
}

func (b *startupPinFailureBucket) Upload(ctx context.Context, key string, r io.Reader, options ...objstore.ObjectUploadOption) error {
	if b.failAt == "claim-cas" && strings.HasSuffix(key, "/checkpoint/PUBLISHER") {
		b.calls++
		data, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		var foreign checkpoint.PublisherClaim
		if err := json.Unmarshal(data, &foreign); err != nil {
			return err
		}
		foreign.OwnerID, foreign.Purpose = "foreign-live-publisher", "publisher"
		foreign.ReservedIndex = 1
		foreign.Generation++
		foreignData, err := json.Marshal(foreign)
		if err != nil {
			return err
		}
		if err := b.Bucket.Upload(ctx, key, bytes.NewReader(foreignData)); err != nil {
			return err
		}
		// The contender's original CAS loses; the next read sees the live holder.
		return b.Bucket.Upload(ctx, key, bytes.NewReader(data), options...)
	}
	if b.failAt == "pin-cas" && strings.Contains(key, "/checkpoint/recovery-pins/") {
		b.calls++
		return b.fail
	}
	if b.failAt == "upload" && strings.Contains(key, "/checkpoint/recovery-pins/") {
		b.calls++
		// Model an accepted write with an unknown acknowledgement.
		if err := b.Bucket.Upload(ctx, key, r, options...); err != nil {
			return err
		}
		return b.fail
	}
	return b.Bucket.Upload(ctx, key, r, options...)
}

func (b *startupPinFailureBucket) IsConditionNotMetErr(err error) bool {
	return b.failAt == "pin-cas" && errors.Is(err, b.fail) || b.Bucket.IsConditionNotMetErr(err)
}

func (b *startupPinFailureBucket) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if b.failAt == "readback" && strings.Contains(key, "/checkpoint/recovery-pins/") {
		if exists, err := b.Bucket.Exists(ctx, key); err != nil || exists {
			b.calls++
			return nil, b.fail
		}
	}
	return b.Bucket.Get(ctx, key)
}

func TestStartupRecoveryAdmissionDoesNotRetryPinFailures(t *testing.T) {
	for _, mode := range []string{"own-pin", "root", "root-missing", "upload", "readback", "pin-cas", "claim-cas"} {
		t.Run(mode, func(t *testing.T) {
			bucket := &startupPinFailureBucket{Bucket: objstore.NewInMemBucket()}
			manager, root := startupPinFixture(t, bucket)
			guard, err := newStartupRecoveryGuard(context.Background(), nil, "startup-test")
			if err != nil {
				t.Fatal(err)
			}
			defer guard.Close()
			failure := errors.New("test provider failure")
			if mode == "own-pin" {
				pin, err := manager.PinRecoveryRoot(context.Background(), root, guard.Owner(), time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				defer pin.Close(context.Background())
				failure = checkpoint.ErrPublisherBusy
			} else {
				bucket.failAt, bucket.fail = mode, failure
			}
			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer cancel()
			_, err = guard.PinRoot(ctx, manager, root, guard.Owner())
			if mode == "root-missing" {
				if !bucket.IsObjNotFoundErr(err) {
					t.Fatalf("missing immutable root was not preserved: %v", err)
				}
				failure = err
			} else if mode == "pin-cas" || mode == "claim-cas" {
				failure = checkpoint.ErrPublisherBusy
			}
			if !errors.Is(err, failure) || errors.Is(err, checkpoint.ErrRecoveryAdmissionBusy) || errors.Is(err, context.DeadlineExceeded) || guard.root != nil {
				t.Fatalf("pin failure retried or changed: %v", err)
			}
			wantCalls := 1
			if mode == "pin-cas" {
				wantCalls = 4 // Existing manager CAS attempts, not startup retries.
			}
			if mode != "own-pin" && bucket.calls != wantCalls {
				t.Fatalf("provider failure calls=%d want%d", bucket.calls, wantCalls)
			}
		})
	}
}

func startupClaimBytes(t *testing.T, ctx context.Context, bucket objstore.Bucket, key string) ([]byte, *objstore.ObjectVersion) {
	t.Helper()
	reader, err := bucket.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("claim read=%v close=%v", readErr, closeErr)
	}
	attributes, err := bucket.Attributes(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	return data, attributes.Version
}

func assertStartupPublisherRefusal(t *testing.T, observations []checkpoint.PublisherBusyObservation, generation uint64) {
	t.Helper()
	if len(observations) != 1 {
		t.Fatalf("refusal count=%d, want exactly1", len(observations))
	}
	event := observations[0]
	if event.Source != "publisher_claim_active" || event.RequestedPurpose != "maintenance" || event.Purpose != "publisher" || event.OwnerID != "foreign-live-publisher" || event.Generation != generation || !event.VersionPresent || event.LeaseRemainingMillis <= 0 {
		t.Fatalf("wrong refusal branch=%+v", event)
	}
}
