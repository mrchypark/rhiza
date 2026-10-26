package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/ncruces/go-sqlite3/driver"
	"io"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/thanos-io/objstore"
)

func TestLocalGCProtocolDowngradeAndReadOnlyOpen(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	archive, err := CreateLocalArchive(ctx, bucket, "local", localTestBinding(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if exists, _ := bucket.Exists(ctx, "local/local-gc-control"); exists {
		t.Fatal("read-only open created control")
	}
	old, _ := json.Marshal(archive.head)
	if err := archive.GarbageCollect(ctx, 0, 0); err != nil {
		t.Fatal(err)
	}
	if archive.head.Version != 2 {
		t.Fatal("protocol not activated")
	}
	if _, err := OpenLocalArchive(ctx, bucket, "local"); err != nil {
		t.Fatal(err)
	}
	if err := bucket.Upload(ctx, "local/HEAD", bytes.NewReader(old)); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenLocalArchive(ctx, bucket, "local"); err == nil {
		t.Fatal("downgrade accepted")
	}
}

func TestLocalGCRejectsStateAdvance(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	first, err := CreateLocalArchive(ctx, bucket, "local", localTestBinding(), nil)
	if err != nil {
		t.Fatal(err)
	}
	stale, err := OpenLocalArchive(ctx, bucket, "local")
	if err != nil {
		t.Fatal(err)
	}
	if err := first.GarbageCollect(ctx, 1, 0); err != nil {
		t.Fatal(err)
	}
	if err := stale.GarbageCollect(ctx, 1, 0); err == nil {
		t.Fatal("stale HEAD admitted")
	}
}

func TestLocalGCRejectsPolicyBeforeStorage(t *testing.T) {
	archive := &LocalArchive{}
	if err := archive.GarbageCollect(context.Background(), -1, 0); err == nil {
		t.Fatal("negative keep accepted")
	}
	if err := archive.GarbageCollect(context.Background(), 1, -1); err == nil {
		t.Fatal("negative grace accepted")
	}
}

func TestLocalGCCatalogRetentionAndRestorePin(t *testing.T) {
	ctx := context.Background()
	bucket := &localViewAdmissionDelayBucket{Bucket: objstore.NewInMemBucket()}
	binding := localTestBinding()
	manager := checkpoint.NewManager(bucket, "local", "", 1)
	var snapshots []LocalSnapshot
	for i := uint64(1); i <= 4; i++ {
		filename := filepath.Join(t.TempDir(), "snapshot.db")
		db, err := driver.Open(filename)
		if err != nil {
			t.Fatal(err)
		}
		_, err = db.Exec(`CREATE TABLE _rhiza_meta(key TEXT PRIMARY KEY, value TEXT); INSERT INTO _rhiza_meta VALUES ('sql_execution_policy','` + sqlpolicy.Marker() + `'); CREATE TABLE payload(value INTEGER)`)
		if err == nil {
			_, err = db.Exec(`INSERT INTO payload VALUES (?)`, i)
		}
		closeErr := db.Close()
		if err != nil {
			t.Fatal(err)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
		root, err := manager.CreateLocalFiles(ctx, []checkpoint.Source{{Role: checkpoint.RoleSQLite, Path: filename}}, i)
		if err != nil {
			t.Fatal(err)
		}
		seal := quepaxa.CheckpointSeal{ConfigID: 1, Index: quepaxa.Slot(i), RootHash: sha256.Sum256([]byte(fmt.Sprint(i))), StateHash: root.Hash, PrefixHash: sha256.Sum256([]byte("prefix"))}
		seal.NextLeaderOrder = []quepaxa.NodeID{"local"}
		if len(snapshots) != 0 {
			previous := snapshots[len(snapshots)-1]
			seal.PrefixHash = quepaxa.AdvancePrefixHash(previous.Seal.PrefixHash, previous.Decision.Slot, previous.Decision.Hash)
		}
		encoded, err := quepaxa.EncodeCheckpointSeal(seal)
		if err != nil {
			t.Fatal(err)
		}
		snapshot := LocalSnapshot{Seal: seal, PortableRoot: root.RootHash, Descriptor: json.RawMessage(`{}`), Decision: quepaxa.DecidedValue{Slot: quepaxa.Slot(i + 1), Value: encoded, Hash: sha256.Sum256(encoded), Certificate: json.RawMessage(`{}`)}}
		if err := StoreLocalSnapshot(ctx, bucket, "local", snapshot); err != nil {
			t.Fatal(err)
		}
		snapshots = append(snapshots, snapshot)
	}
	current := snapshots[3]
	archive, err := CreateLocalArchive(ctx, bucket, "local", binding, &snapshots[0])
	if err != nil {
		t.Fatal(err)
	}
	extent := Extent{ConfigID: 1, Start: 2, End: 3, StartPrefix: snapshots[0].Seal.PrefixHash, Decisions: []quepaxa.DecidedValue{snapshots[0].Decision, snapshots[1].Decision}}
	extent.EndPrefix = quepaxa.AdvancePrefixHash(snapshots[1].Seal.PrefixHash, snapshots[1].Decision.Slot, snapshots[1].Decision.Hash)
	encoded, err := encodeExtent(extent)
	if err != nil {
		t.Fatal(err)
	}
	extentHash := sha256.Sum256(encoded)
	extentName := fmt.Sprintf("local/extents/%x", extentHash)
	if err := bucket.Upload(ctx, extentName, bytes.NewReader(encoded)); err != nil {
		t.Fatal(err)
	}
	head := archive.head
	head.Tip, head.Prefix, head.Tail = 3, extent.EndPrefix, extentHash
	if err := archive.publish(ctx, head); err != nil {
		t.Fatal(err)
	}
	archive.extents = []Extent{extent}
	pin, err := archive.PinRestore(ctx, snapshots[0], "reader", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(pin.parts) != 2 {
		t.Fatalf("captured suffix closure pins=%d, want A+B", len(pin.parts))
	}
	// Rebase after admission. B remains required by the reader's captured view.
	head = archive.head
	head.Version, head.Catalog = 2, snapshots
	head.Origin, head.Snapshot = &current, &current
	head.Tip, head.Prefix, head.Tail = current.Seal.Index, current.Seal.PrefixHash, [32]byte{}
	if err := archive.publish(ctx, head); err != nil {
		t.Fatal(err)
	}
	archive.extents = nil
	orphan := fmt.Sprintf("local/extents/%x", sha256.Sum256([]byte("abandoned")))
	if err := bucket.Upload(ctx, orphan, bytes.NewReader([]byte("abandoned"))); err != nil {
		t.Fatal(err)
	}
	if err := archive.GarbageCollect(ctx, 1, 0); err != nil {
		t.Fatal(err)
	}
	if exists, _ := bucket.Exists(ctx, orphan); !exists {
		t.Fatal("archive extent removed during pinned restore")
	}
	if _, err := manager.OpenRoot(ctx, 3, snapshots[2].PortableRoot); err == nil {
		t.Fatal("old committed snapshot retained")
	}
	for _, index := range []int{0, 1, 3} {
		root, err := manager.OpenRoot(ctx, uint64(index+1), snapshots[index].PortableRoot)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := manager.DownloadAndVerifyRootFiles(ctx, root, t.TempDir()); err != nil {
			t.Fatal(err)
		}
	}
	if err := pin.Renew(ctx, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := pin.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := archive.GarbageCollect(ctx, 1, 0); err != nil {
		t.Fatal(err)
	}
	if exists, _ := bucket.Exists(ctx, orphan); exists {
		t.Fatal("old archive extent retained after restore close")
	}
	if _, err := manager.OpenRoot(ctx, 1, snapshots[0].PortableRoot); err == nil {
		t.Fatal("unpinned retired snapshot retained")
	}
	if _, err := archive.PinRestore(ctx, snapshots[0], "late-reader", time.Hour); err == nil {
		t.Fatal("deleted root pinned")
	}
	t.Run("renew-rechecks-roots-after-slow-view-update", func(t *testing.T) {
		renewal, err := archive.PinView(ctx, "slow-renewal", time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		// This read occurs after all root renewals and before the fresh view
		// deadline is assigned, so the root must expire while the view stays live.
		bucket.delayMS.Store(750)
		if err := renewal.Renew(ctx, 250*time.Millisecond); !errors.Is(err, checkpoint.ErrPublisherFenced) {
			t.Fatalf("expired root hidden by fresh view renewal: %v", err)
		}
		record, _, err := archive.readLocalView(ctx, renewal.key)
		if err != nil {
			t.Fatal(err)
		}
		if record.LeaseUntilMS <= time.Now().UnixMilli() {
			t.Fatal("regression did not isolate root expiry from view expiry")
		}
		if err := renewal.Close(ctx); err != nil {
			t.Fatal(err)
		}
	})
	for _, renew := range []bool{false, true} {
		for _, delay := range []int64{249, 251} {
			t.Run(fmt.Sprintf("root-margin-renew=%v-delay=%d", renew, delay), func(t *testing.T) {
				isolated := &localViewAdmissionDelayBucket{Bucket: objstore.NewInMemBucket()}
				if err := bucket.Iter(ctx, "local/", func(name string) error {
					if !strings.HasPrefix(name, "local/checkpoint/roots/") && !strings.HasPrefix(name, "local/checkpoint/blocks/") && !strings.HasPrefix(name, "local/snapshots/") {
						return nil
					}
					reader, err := bucket.Get(ctx, name)
					if err != nil {
						return err
					}
					defer reader.Close()
					return isolated.Upload(ctx, name, reader)
				}, objstore.WithRecursiveIter()); err != nil {
					t.Fatal(err)
				}
				fresh, err := CreateLocalArchive(ctx, isolated, "local", binding, &current)
				if err != nil {
					t.Fatal(err)
				}
				synctest.Test(t, func(t *testing.T) {
					const lease = time.Second
					owner := fmt.Sprintf("root-margin-%v-%d", renew, delay)
					var candidate *LocalRestorePin
					var err error
					if renew {
						candidate, err = fresh.PinView(ctx, owner, time.Hour)
						if err != nil {
							t.Fatal(err)
						}
					}
					isolated.delayMS.Store(delay)
					if renew {
						err = candidate.Renew(ctx, lease)
					} else {
						candidate, err = fresh.PinView(ctx, owner, lease)
					}
					if delay == 251 {
						if !errors.Is(err, checkpoint.ErrPublisherFenced) {
							t.Fatalf("near-expiry root accepted: %v", err)
						}
						if !renew {
							active, cleanupErr := fresh.localManager().LocalRecoveryActive(ctx)
							if candidate != nil || cleanupErr != nil || active {
								t.Fatalf("rejected admission left a root pin: pin=%v active=%v err=%v", candidate, active, cleanupErr)
							}
						}
						if renew {
							deadline, checkErr := checkpoint.LocalRecoveryPinDeadline(ctx, candidate.parts)
							if checkErr != nil || !deadline.After(time.Now()) {
								t.Fatalf("root was expired, not near expiry: %v %v", deadline, checkErr)
							}
							record, _, checkErr := fresh.readLocalView(ctx, candidate.key)
							if checkErr != nil || record.LeaseUntilMS <= time.Now().UnixMilli() {
								t.Fatal("view was not live", checkErr)
							}
						}
					} else if err != nil {
						t.Fatalf("sufficient root margin rejected: %v", err)
					}
					if candidate != nil {
						if err := candidate.Close(ctx); err != nil {
							t.Fatal(err)
						}
					}
				})
			})
		}
	}
	bucket.delayMS.Store(200)
	if expired, err := archive.PinView(ctx, "slow-admission", 100*time.Millisecond); !errors.Is(err, checkpoint.ErrPublisherFenced) || expired != nil {
		t.Fatalf("expired early root pin admitted while fresh view lease was registered: pin=%v err=%v", expired, err)
	}
}

func TestLocalViewMarginAdmissionAndRenewal(t *testing.T) {
	for _, renew := range []bool{false, true} {
		for _, delay := range []time.Duration{249 * time.Millisecond, 251 * time.Millisecond} {
			t.Run(fmt.Sprintf("renew=%v-delay=%v", renew, delay), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx := context.Background()
					bucket := &localViewFinalReadDelayBucket{Bucket: objstore.NewInMemBucket()}
					archive, err := CreateLocalArchive(ctx, bucket, "local", localTestBinding(), nil)
					if err != nil {
						t.Fatal(err)
					}
					var pin *LocalRestorePin
					if renew {
						pin, err = archive.PinView(ctx, "view-margin", time.Hour)
						if err != nil {
							t.Fatal(err)
						}
					}
					bucket.delay = delay
					if renew {
						err = pin.Renew(ctx, time.Second)
					} else {
						pin, err = archive.PinView(ctx, "view-margin", time.Second)
					}
					if delay == 251*time.Millisecond {
						if !errors.Is(err, checkpoint.ErrPublisherFenced) {
							t.Fatalf("near-expiry view accepted: %v", err)
						}
						if renew {
							record, _, checkErr := archive.readLocalView(ctx, pin.key)
							if checkErr != nil || record.LeaseUntilMS <= time.Now().UnixMilli() {
								t.Fatal("view was expired, not near expiry", checkErr)
							}
						}
					} else if err != nil {
						t.Fatalf("sufficient view margin rejected: %v", err)
					}
					if pin != nil {
						if err := pin.Close(ctx); err != nil {
							t.Fatal(err)
						}
					}
				})
			})
		}
	}
}

type localViewFinalReadDelayBucket struct {
	objstore.Bucket
	delay        time.Duration
	armed        bool
	releaseDelay time.Duration
}

func (b *localViewFinalReadDelayBucket) Upload(ctx context.Context, name string, reader io.Reader, options ...objstore.ObjectUploadOption) error {
	var release bool
	if b.releaseDelay != 0 && strings.HasSuffix(name, "/local-gc-control") {
		data, err := io.ReadAll(reader)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(data)
		var control struct {
			Token string `json:"token"`
		}
		if err := json.Unmarshal(data, &control); err != nil {
			return err
		}
		release = control.Token == ""
	}
	if err := b.Bucket.Upload(ctx, name, reader, options...); err != nil {
		return err
	}
	if b.delay != 0 && strings.Contains(name, "/view-pins/") {
		b.armed = true
	}
	if release {
		delay := b.releaseDelay
		b.releaseDelay = 0
		time.Sleep(delay)
	}
	return nil
}

func TestLocalViewMarginCheckedAfterGateRelease(t *testing.T) {
	for _, delay := range []time.Duration{249 * time.Millisecond, 251 * time.Millisecond} {
		t.Run(delay.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				bucket := &localViewFinalReadDelayBucket{Bucket: objstore.NewInMemBucket()}
				archive, err := CreateLocalArchive(ctx, bucket, "local", localTestBinding(), nil)
				if err != nil {
					t.Fatal(err)
				}
				bucket.releaseDelay = delay
				pin, err := archive.PinView(ctx, "release-margin", time.Second)
				if delay == 251*time.Millisecond {
					if !errors.Is(err, checkpoint.ErrPublisherFenced) || pin != nil {
						t.Fatalf("gate-release latency consumed margin: pin=%v err=%v", pin, err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
				if pin != nil {
					if err := pin.Close(ctx); err != nil {
						t.Fatal(err)
					}
				}
				if err := archive.localManager().WithLocalMaintenanceContext(ctx, func(context.Context) error { return nil }); err != nil {
					t.Fatalf("rejection left a held gate: %v", err)
				}
			})
		})
	}
}

func (b *localViewFinalReadDelayBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	if b.armed && b.delay != 0 && strings.Contains(name, "/view-pins/") {
		if exists, _ := b.Bucket.Exists(ctx, name); exists {
			delay := b.delay
			b.delay = 0
			time.Sleep(delay)
		}
	}
	return b.Bucket.Attributes(ctx, name)
}

func TestLocalArchiveBusyAndPreDispatchCancelDoNotPoison(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	archive, err := CreateLocalArchive(ctx, bucket, "local", localTestBinding(), nil)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- archive.localManager().WithLocalMaintenanceContext(ctx, func(context.Context) error { close(entered); <-release; return nil })
	}()
	<-entered
	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	err = archive.SyncThrough(waitCtx, nil, 0)
	cancel()
	close(release)
	if holderErr := <-done; holderErr != nil {
		t.Fatal(holderErr)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if archive.failure != nil {
		t.Fatal("busy admission poisoned writer", archive.failure)
	}
	cancelled, stop := context.WithCancel(ctx)
	err = archive.localManager().WithLocalMaintenanceContext(cancelled, func(workCtx context.Context) error {
		stop()
		return archive.publish(workCtx, archive.head)
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if archive.failure != nil {
		t.Fatal("pre-dispatch cancel poisoned writer", archive.failure)
	}
	if err := archive.SyncThrough(ctx, localArchiveSuffix{prefixes: map[quepaxa.Slot][32]byte{0: {}}}, 0); err != nil {
		t.Fatal("stream did not resume", err)
	}
}

func TestLocalPinViewWithoutSnapshotProtectsTail(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	archive, err := CreateLocalArchive(ctx, bucket, "local", localTestBinding(), nil)
	if err != nil {
		t.Fatal(err)
	}
	value := []byte("tail-only-write")
	decision := quepaxa.DecidedValue{Slot: 1, Value: value, Hash: sha256.Sum256(value), Certificate: json.RawMessage(`{}`)}
	extent := Extent{ConfigID: 1, Start: 1, End: 1, Decisions: []quepaxa.DecidedValue{decision}, EndPrefix: quepaxa.AdvancePrefixHash([32]byte{}, 1, decision.Hash)}
	encoded, err := encodeExtent(extent)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(encoded)
	tailKey := fmt.Sprintf("local/extents/%x", hash)
	if err := bucket.Upload(ctx, tailKey, bytes.NewReader(encoded)); err != nil {
		t.Fatal(err)
	}
	head := archive.head
	head.Tip, head.Prefix, head.Tail = 1, extent.EndPrefix, hash
	if err := archive.publish(ctx, head); err != nil {
		t.Fatal(err)
	}
	archive.extents = []Extent{extent}
	pin, err := archive.PinView(ctx, "tail-reader", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(pin.parts) != 0 || pin.archive == nil {
		t.Fatal("snapshotless view requires its own lease, not a fake root")
	}
	orphan := fmt.Sprintf("local/extents/%x", sha256.Sum256([]byte("old-tail")))
	if err := bucket.Upload(ctx, orphan, bytes.NewReader([]byte("old-tail"))); err != nil {
		t.Fatal(err)
	}
	if err := archive.GarbageCollect(ctx, 1, 0); err != nil {
		t.Fatal(err)
	}
	if exists, _ := bucket.Exists(ctx, orphan); !exists {
		t.Fatal("active snapshotless view did not block extent sweep")
	}
	if err := pin.Renew(ctx, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := pin.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := archive.GarbageCollect(ctx, 1, 0); err != nil {
		t.Fatal(err)
	}
	if exists, _ := bucket.Exists(ctx, orphan); exists {
		t.Fatal("closed view blocked extent sweep")
	}
	if exists, _ := bucket.Exists(ctx, tailKey); !exists {
		t.Fatal("current tail removed")
	}
	if err := pin.Renew(ctx, time.Hour); !errors.Is(err, checkpoint.ErrPublisherFenced) {
		t.Fatal("closed view renewed", err)
	}
}

func TestLocalViewExpiredLeaseCannotRenewOrCloseReplacement(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	archive, err := CreateLocalArchive(ctx, bucket, "local", localTestBinding(), nil)
	if err != nil {
		t.Fatal(err)
	}
	old, err := archive.PinView(ctx, "reader", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	record, version, err := archive.readLocalView(ctx, old.key)
	if err != nil {
		t.Fatal(err)
	}
	record.LeaseUntilMS = time.Now().Add(-time.Hour).UnixMilli()
	if err := archive.writeLocalView(ctx, old.key, record, objstore.WithIfMatch(version)); err != nil {
		t.Fatal(err)
	}
	if err := old.Renew(ctx, time.Hour); !errors.Is(err, checkpoint.ErrPublisherFenced) {
		t.Fatal("expired lease renewed", err)
	}
	replacement, err := archive.PinView(ctx, "reader", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := old.Close(ctx); !errors.Is(err, checkpoint.ErrPublisherFenced) {
		t.Fatal("stale close accepted", err)
	}
	if err := replacement.Renew(ctx, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := replacement.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

type localViewAdmissionDelayBucket struct {
	objstore.Bucket
	delayMS atomic.Int64
}

func (b *localViewAdmissionDelayBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	if strings.Contains(name, "/view-pins/") {
		if delay := b.delayMS.Swap(0); delay > 0 {
			time.Sleep(time.Duration(delay) * time.Millisecond)
		}
	}
	return b.Bucket.Attributes(ctx, name)
}

type localExtentInjectedListing struct {
	objstore.Bucket
	extra string
}

func (b localExtentInjectedListing) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	if err := b.Bucket.Iter(ctx, dir, f, options...); err != nil {
		return err
	}
	if dir == "local/extents" {
		return f(b.extra)
	}
	return nil
}
func TestLocalGCRejectsNoncanonicalExtentKeys(t *testing.T) {
	for _, parent := range []string{"local/extents/unknown-subdir", "local/extents-sibling", "foreign/extents"} {
		t.Run(parent, func(t *testing.T) {
			ctx := context.Background()
			raw := objstore.NewInMemBucket()
			name := path.Join(parent, fmt.Sprintf("%x", sha256.Sum256([]byte("unknown"))))
			bucket := localExtentInjectedListing{Bucket: raw, extra: name}
			archive, err := CreateLocalArchive(ctx, bucket, "local", localTestBinding(), nil)
			if err != nil {
				t.Fatal(err)
			}
			orphan := fmt.Sprintf("local/extents/%x", sha256.Sum256([]byte("orphan")))
			for _, key := range []string{name, orphan} {
				if err := raw.Upload(ctx, key, strings.NewReader("preserve")); err != nil {
					t.Fatal(err)
				}
			}
			if err := archive.GarbageCollect(ctx, 1, 0); err == nil {
				t.Fatal("noncanonical extent accepted")
			}
			for _, key := range []string{name, orphan} {
				if exists, _ := raw.Exists(ctx, key); !exists {
					t.Fatal("deleted before canonical validation", key)
				}
			}
		})
	}
}

type localCatalogPruneReadFailure struct {
	objstore.Bucket
	fail                  atomic.Bool
	cancelAfterDescriptor atomic.Pointer[context.CancelFunc]
}

func (b *localCatalogPruneReadFailure) Delete(ctx context.Context, name string) error {
	err := b.Bucket.Delete(ctx, name)
	if err == nil && strings.Contains(name, "/snapshots/") {
		if cancel := b.cancelAfterDescriptor.Swap(nil); cancel != nil {
			(*cancel)()
		}
	}
	return err
}

func (b *localCatalogPruneReadFailure) Exists(ctx context.Context, name string) (bool, error) {
	if strings.Contains(name, "/checkpoint/roots/") && b.fail.CompareAndSwap(true, false) {
		return false, errors.New("prune existence read failed after acknowledged deletes")
	}
	return b.Bucket.Exists(ctx, name)
}
func TestLocalGCCatalogBoundedAcrossCompletedRetirements(t *testing.T) {
	ctx := context.Background()
	bucket := &localCatalogPruneReadFailure{Bucket: objstore.NewInMemBucket()}
	archive, err := CreateLocalArchive(ctx, bucket, "local", localTestBinding(), nil)
	if err != nil {
		t.Fatal(err)
	}
	filename := filepath.Join(t.TempDir(), "snapshot.db")
	db, err := driver.Open(filename)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE _rhiza_meta(key TEXT PRIMARY KEY, value TEXT); INSERT INTO _rhiza_meta VALUES ('sql_execution_policy','` + sqlpolicy.Marker() + `')`)
	closeErr := db.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	manager := checkpoint.NewManager(bucket, "local", "", 1)
	unknownMetadata := fmt.Sprintf("local/snapshots/%x.json", sha256.Sum256([]byte("prepared-or-legacy")))
	if err := bucket.Upload(ctx, unknownMetadata, strings.NewReader(`{}`)); err != nil {
		t.Fatal(err)
	}
	firstSize := 0
	for cycle := uint64(1); cycle <= 40; cycle++ {
		root, err := manager.CreateLocalFiles(ctx, []checkpoint.Source{{Role: checkpoint.RoleSQLite, Path: filename}}, cycle)
		if err != nil {
			t.Fatal(err)
		}
		seal := quepaxa.CheckpointSeal{ConfigID: 1, Index: quepaxa.Slot(cycle), RootHash: sha256.Sum256([]byte(fmt.Sprint(cycle))), StateHash: root.Hash, PrefixHash: sha256.Sum256([]byte("prefix")), NextLeaderOrder: []quepaxa.NodeID{"local"}}
		encoded, err := quepaxa.EncodeCheckpointSeal(seal)
		if err != nil {
			t.Fatal(err)
		}
		snapshot := LocalSnapshot{Seal: seal, PortableRoot: root.RootHash, Descriptor: json.RawMessage(`{}`), Decision: quepaxa.DecidedValue{Slot: quepaxa.Slot(cycle + 1), Value: encoded, Hash: sha256.Sum256(encoded), Certificate: json.RawMessage(`{}`)}}
		if err := StoreLocalSnapshot(ctx, bucket, "local", snapshot); err != nil {
			t.Fatal(err)
		}
		head := archive.head
		head.Version, head.Tip, head.Prefix = 2, seal.Index, seal.PrefixHash
		head.Origin, head.Snapshot = &snapshot, &snapshot
		head.Catalog = append(append([]LocalSnapshot(nil), head.Catalog...), snapshot)
		if err := archive.publish(ctx, head); err != nil {
			t.Fatal(err)
		}
		if cycle == 2 {
			if err := archive.GarbageCollect(ctx, 1, 24*time.Hour); err != nil {
				t.Fatal(err)
			}
			if len(archive.head.Catalog) != 2 {
				t.Fatal("grace-protected retirement eligibility lost")
			}
			bucket.fail.Store(true)
			if err := archive.GarbageCollect(ctx, 1, 0); err == nil {
				t.Fatal("expected post-delete prune read failure")
			}
			if len(archive.head.Catalog) != 2 {
				t.Fatal("catalog pruned without definitive absence")
			}
		}
		if cycle == 3 {
			cancelled, cancel := context.WithCancel(ctx)
			bucket.cancelAfterDescriptor.Store(&cancel)
			err := archive.GarbageCollect(cancelled, 1, 0)
			cancel()
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("post-descriptor ACK cancellation: %v", err)
			}
			if len(archive.head.Catalog) != 2 {
				t.Fatal("catalog changed before acknowledged HEAD prune")
			}
		}
		if err := archive.GarbageCollect(ctx, 1, 0); err != nil {
			t.Fatalf("cycle %d: %v", cycle, err)
		}
		if len(archive.head.Catalog) != 1 {
			t.Fatalf("cycle %d catalog=%d", cycle, len(archive.head.Catalog))
		}
		encodedHead, err := archive.read(ctx, "HEAD", maxLocalHeadSize)
		if err != nil {
			t.Fatal(err)
		}
		if firstSize == 0 {
			firstSize = len(encodedHead)
		}
		if len(encodedHead) > firstSize+2048 {
			t.Fatalf("HEAD grew with lifetime retirements: first=%d now=%d", firstSize, len(encodedHead))
		}
	}
	metadataCount := 0
	if err := bucket.Iter(ctx, "local/snapshots", func(name string) error { metadataCount++; return nil }, objstore.WithRecursiveIter()); err != nil {
		t.Fatal(err)
	}
	if metadataCount != 2 {
		t.Fatalf("snapshot descriptors=%d, want current plus preserved unknown", metadataCount)
	}
	if present, _ := bucket.Exists(ctx, unknownMetadata); !present {
		t.Fatal("unknown/prepared descriptor deleted")
	}
	if _, err := OpenLocalArchive(ctx, bucket, "local"); err != nil {
		t.Fatal(err)
	}
}

type localCountHeadPublication struct {
	objstore.Bucket
	uploads atomic.Int64
}

func (b *localCountHeadPublication) Upload(ctx context.Context, name string, r io.Reader, options ...objstore.ObjectUploadOption) error {
	b.uploads.Add(1)
	return b.Bucket.Upload(ctx, name, r, options...)
}
func TestLocalOversizedHeadRejectedBeforePublicationIO(t *testing.T) {
	ctx := context.Background()
	bucket := &localCountHeadPublication{Bucket: objstore.NewInMemBucket()}
	archive, err := CreateLocalArchive(ctx, bucket, "local", localTestBinding(), nil)
	if err != nil {
		t.Fatal(err)
	}
	before, err := archive.read(ctx, "HEAD", maxLocalHeadSize)
	if err != nil {
		t.Fatal(err)
	}
	bucket.uploads.Store(0)
	oversized := archive.head
	oversized.Version = 2
	oversized.Catalog = make([]LocalSnapshot, 512)
	descriptor := json.RawMessage(`"` + strings.Repeat("x", (16<<10)-2) + `"`)
	for i := range oversized.Catalog {
		oversized.Catalog[i].Descriptor = descriptor
	}
	if err := archive.publish(ctx, oversized); err == nil {
		t.Fatal("oversized HEAD accepted")
	}
	if got := bucket.uploads.Load(); got != 0 {
		t.Fatalf("oversize rejection issued %d uploads", got)
	}
	if archive.failure != nil {
		t.Fatal("known pre-I/O rejection poisoned archive", archive.failure)
	}
	after, err := archive.read(ctx, "HEAD", maxLocalHeadSize)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("oversize rejection changed current HEAD")
	}
}
