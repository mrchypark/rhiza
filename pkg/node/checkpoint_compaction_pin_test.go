package node

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

type currentUploadCounter struct {
	objstore.Bucket
	currentUploads atomic.Int64
}

// claimReadBarrier returns the already-read second version of a stable lease
// read only after the replacement claim has been written.
type claimReadBarrier struct {
	objstore.Bucket
	armed   atomic.Bool
	reads   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

type prefixWaitContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
	armed   atomic.Bool
}

func (c *prefixWaitContext) Done() <-chan struct{} {
	if c.armed.Load() {
		c.once.Do(func() { close(c.entered) })
	}
	return c.Context.Done()
}

func (b *claimReadBarrier) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	attr, err := b.Bucket.Attributes(ctx, name)
	if b.armed.Load() && strings.HasSuffix(name, "/checkpoint/PUBLISHER") && b.reads.Add(1) == 2 {
		b.armed.Store(false)
		close(b.entered)
		select {
		case <-b.release:
		case <-ctx.Done():
			return objstore.ObjectAttributes{}, ctx.Err()
		}
	}
	return attr, err
}

// holdGCCandidateScan pauses a real Cleanup only after its publication lease
// has been released. Its GC lease remains live while a checkpoint publishes.
type holdGCCandidateScan struct {
	objstore.Bucket
	entered chan struct{}
	release chan struct{}
	paused  atomic.Bool
}

func (b *holdGCCandidateScan) IterWithAttributes(ctx context.Context, dir string, f func(objstore.IterObjectAttributes) error, opts ...objstore.IterOption) error {
	if strings.HasSuffix(dir, "/archive/gc-candidates") && b.paused.CompareAndSwap(false, true) {
		close(b.entered)
		select {
		case <-b.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return b.Bucket.IterWithAttributes(ctx, dir, f, opts...)
}

func (b *currentUploadCounter) Upload(ctx context.Context, name string, value io.Reader, options ...objstore.ObjectUploadOption) error {
	if err := b.Bucket.Upload(ctx, name, value, options...); err != nil {
		return err
	}
	if strings.HasSuffix(name, "/checkpoint/CURRENT") {
		b.currentUploads.Add(1)
	}
	return nil
}

func TestPeerCheckpointWaitsForAuthenticatedPrefixBeforePublication(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	bucket := &currentUploadCounter{Bucket: objstore.NewInMemBucket()}
	checkpoints := checkpoint.NewManager(bucket, "peer-lag", t.TempDir(), 1)
	archive := recovery.NewManager(bucket, "peer-lag", 1)
	defer archive.Close()
	cluster := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}
	openCore := func() (*quepaxa.Core, *qlog.WAL) {
		t.Helper()
		wal, err := qlog.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: cluster, WAL: wal})
		if err != nil {
			wal.Close()
			t.Fatal(err)
		}
		return core, wal
	}
	source, sourceWAL := openCore()
	defer sourceWAL.Close()
	peer, peerWAL := openCore()
	defer peerWAL.Close()
	command, err := types.EncodeKVCommand(types.KVCommand{RequestID: "peer-lag", Operation: "put", Key: "key", Value: []byte("value")})
	if err != nil {
		t.Fatal(err)
	}
	slot, _, err := source.Propose(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	certified, ok := source.CertifiedValue(slot)
	if !ok {
		t.Fatal("source has no authentic certified value")
	}
	material, err := materializer.Open(filepath.Join(t.TempDir(), "material.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	if err := material.Apply(ctx, uint64(slot), command); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "sqlite.db")
	policySnapshot(t, file, "peer-lag")
	claim, err := checkpoints.AcquirePublisherClaim(ctx, "n1", 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	root, err := checkpoints.CreateFiles(ctx, claim, []checkpoint.Source{{Role: checkpoint.RoleSQLite, Path: file}}, uint64(slot))
	if err != nil {
		t.Fatal(err)
	}
	claim, err = checkpoints.BindPublisherClaim(ctx, claim, uint64(slot), root.RootHash, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer checkpoints.ReleasePublisherClaim(context.Background(), claim)
	prefix, ok := source.PrefixHash(slot)
	if !ok {
		t.Fatal("certified source prefix absent")
	}
	next, following, err := source.CheckpointLeaderOrders(slot)
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{ConfigID: 1, Index: slot, RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following}
	peer.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
		return checkpoints.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
	})
	n := &Node{core: peer, checkpoints: checkpoints, archive: archive, material: material}

	canceled, stop := context.WithCancel(ctx)
	canceledResult := make(chan error, 1)
	go func() { canceledResult <- n.preparePeerCheckpoint(canceled, "n1", seal) }()
	stop()
	if err := <-canceledResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled prefix wait returned %v", err)
	}
	if err := peer.RequirePreparedCheckpoint(seal); err == nil || bucket.currentUploads.Load() != 0 {
		t.Fatal("canceled wait prepared or published the checkpoint")
	}

	prepared := make(chan error, 1)
	go func() { prepared <- n.preparePeerCheckpoint(ctx, "n1", seal) }()
	if peer.Tip() >= slot {
		t.Fatal("peer was not behind before catch-up")
	}
	if err := peer.AcceptCertifiedValue(certified); err != nil {
		t.Fatalf("authentic peer catch-up: %v", err)
	}
	if err := <-prepared; err != nil {
		t.Fatalf("Node peer callback after certified catch-up: %v", err)
	}
	encoded, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := peer.Propose(ctx, encoded); err != nil {
		t.Fatal(err)
	}
	if err := n.publishCertifiedCheckpoint(ctx, root); err != nil {
		t.Fatalf("certified publication: %v", err)
	}
	if got := bucket.currentUploads.Load(); got != 1 {
		t.Fatalf("CURRENT uploads=%d, want one", got)
	}
	fresh := checkpoint.NewManager(bucket, "peer-lag", t.TempDir(), 1)
	if err := fresh.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if latest := fresh.Latest(); latest == nil || latest.Index != uint64(slot) || latest.RootHash != root.RootHash || latest.Hash != root.Hash {
		t.Fatalf("independent CURRENT differs: %+v", latest)
	}
	if err := fresh.Verify(ctx, uint64(slot), root.RootHash, root.Hash); err != nil {
		t.Fatal(err)
	}
	reader := recovery.NewManager(bucket, "peer-lag", 1)
	defer reader.Close()
	pin, err := reader.BeginRecoverySnapshot(ctx, "independent-reader", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Close(ctx)
	decisions, _, err := pin.DecisionsFrom(ctx, slot, 1)
	if err != nil || len(decisions) != 1 || !bytes.Equal(decisions[0].Value, command) {
		t.Fatalf("independent archived decision count=%d error=%v", len(decisions), err)
	}
}

func TestPeerCheckpointRejectsClaimReplacedDuringPrefixWait(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	bucket := &claimReadBarrier{Bucket: objstore.NewInMemBucket(), entered: make(chan struct{}), release: make(chan struct{})}
	defer func() {
		select {
		case <-bucket.release:
		default:
			close(bucket.release)
		}
	}()
	manager := checkpoint.NewManager(bucket, "replaced-claim", t.TempDir(), 1)
	cluster := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}
	newCore := func() (*quepaxa.Core, *qlog.WAL) {
		t.Helper()
		wal, err := qlog.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: cluster, WAL: wal})
		if err != nil {
			wal.Close()
			t.Fatal(err)
		}
		return core, wal
	}
	source, sourceWAL := newCore()
	defer sourceWAL.Close()
	peer, peerWAL := newCore()
	defer peerWAL.Close()
	slot, _, err := source.Propose(ctx, []byte("certified decision"))
	if err != nil {
		t.Fatal(err)
	}
	certified, ok := source.CertifiedValue(slot)
	if !ok {
		t.Fatal("certified source decision absent")
	}
	file := filepath.Join(t.TempDir(), "sqlite.db")
	policySnapshot(t, file, "replaced-claim")
	claim, err := manager.AcquirePublisherClaim(ctx, "n1", 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	root, err := manager.CreateFiles(ctx, claim, []checkpoint.Source{{Role: checkpoint.RoleSQLite, Path: file}}, uint64(slot))
	if err != nil {
		t.Fatal(err)
	}
	claim, err = manager.BindPublisherClaim(ctx, claim, uint64(slot), root.RootHash, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	prefix, _ := source.PrefixHash(slot)
	next, following, err := source.CheckpointLeaderOrders(slot)
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{ConfigID: 1, Index: slot, RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following}
	peer.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
		return manager.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
	})
	n := &Node{core: peer, checkpoints: manager}
	bucket.armed.Store(true)
	callCtx := &prefixWaitContext{Context: ctx, entered: make(chan struct{})}
	result := make(chan error, 1)
	go func() { result <- n.preparePeerCheckpoint(callCtx, "n1", seal) }()
	select {
	case <-bucket.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := manager.ReleasePublisherClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	replacement, err := manager.AcquirePublisherClaim(ctx, "replacement", 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.ReleasePublisherClaim(context.Background(), replacement)
	callCtx.armed.Store(true)
	close(bucket.release)
	select {
	case <-callCtx.entered:
	case err := <-result:
		t.Fatalf("claim replacement returned before prefix wait: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := peer.AcceptCertifiedValue(certified); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, checkpoint.ErrPublisherFenced) {
		t.Fatalf("replaced claim returned %v, want fenced", err)
	}
	if err := peer.RequirePreparedCheckpoint(seal); err == nil {
		t.Fatal("replaced claim prepared a checkpoint")
	}
	fresh := checkpoint.NewManager(bucket, "replaced-claim", t.TempDir(), 1)
	if err := fresh.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if fresh.Latest() != nil {
		t.Fatal("replaced claim published CURRENT")
	}
}

func TestCertifiedCheckpointDefersOnlyLivePinCompaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	bucket := &currentUploadCounter{Bucket: objstore.NewInMemBucket()}
	archive := recovery.NewManager(bucket, "cluster", 1)
	defer archive.Close()
	checkpoints := checkpoint.NewManager(bucket, "cluster", t.TempDir(), 1)
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{
		NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, WAL: wal,
	})
	if err != nil {
		t.Fatal(err)
	}
	material, err := materializer.Open(filepath.Join(t.TempDir(), "material.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	command, err := types.EncodeKVCommand(types.KVCommand{RequestID: "pin-compaction", Operation: "put", Key: "key", Value: []byte("value")})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, command); err != nil {
		t.Fatal(err)
	}
	if err := material.Apply(ctx, 1, command); err != nil {
		t.Fatal(err)
	}
	if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	reader := recovery.NewManager(bucket, "cluster", 1)
	defer reader.Close()
	pin, err := reader.BeginRecoverySnapshot(ctx, "recovering-reader", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Close(ctx)
	n := &Node{archive: archive, checkpoints: checkpoints, core: core, material: material}
	core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
		return checkpoints.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
	})
	auto := checkpoint.NewAutoCheckpointer(checkpoints, material, 1, 0)
	auto.ConfigurePublisher("publisher", func() uint64 { return 0 }, nil)
	var published *checkpoint.Checkpoint
	publicationCalls := 0
	auto.ConfigurePublication(nil, func(ctx context.Context, root *checkpoint.Checkpoint) error {
		publicationCalls++
		published = root
		prefix, ok := core.PrefixHash(1)
		if !ok {
			return errors.New("missing checkpoint prefix")
		}
		next, following, err := core.CheckpointLeaderOrders(1)
		if err != nil {
			return err
		}
		seal := quepaxa.CheckpointSeal{
			ConfigID: 1, Index: 1, RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix,
			NextLeaderOrder: next, FollowingLeaderOrder: following,
		}
		if err := core.PrepareCheckpoint(ctx, seal); err != nil {
			return err
		}
		encoded, err := quepaxa.EncodeCheckpointSeal(seal)
		if err != nil {
			return err
		}
		if _, _, err := core.Propose(ctx, encoded); err != nil {
			return err
		}
		if err := n.publishCertifiedCheckpoint(ctx, root); err != nil {
			return err
		}
		return n.compactCertifiedCheckpoint(ctx)
	})
	if err := auto.CheckpointOnShutdown(ctx, 1); err != nil {
		t.Fatalf("real checkpoint publication with a live reader: %v", err)
	}
	if publicationCalls != 1 || published == nil {
		t.Fatalf("publication calls=%d root=%v, want one certified publication", publicationCalls, published)
	}
	if floor := core.CompactionFloor(); floor != 0 {
		t.Fatalf("floor advanced under live recovery pin: %d", floor)
	}
	if got := bucket.currentUploads.Load(); got != 1 {
		t.Fatalf("certified CURRENT uploads=%d, want one", got)
	}
	fresh := checkpoint.NewManager(bucket, "cluster", t.TempDir(), 1)
	if err := fresh.Load(ctx); err != nil {
		t.Fatalf("fresh durable CURRENT read under live pin: %v", err)
	}
	if latest := fresh.Latest(); latest == nil || latest.Index != published.Index || latest.RootHash != published.RootHash || latest.Hash != published.Hash {
		t.Fatalf("fresh durable CURRENT differs under live pin: %+v", latest)
	}
	if err := fresh.Verify(ctx, published.Index, published.RootHash, published.Hash); err != nil {
		t.Fatalf("fresh certified checkpoint verification under live pin: %v", err)
	}
	decisions, _, err := pin.DecisionsFrom(ctx, 1, 1)
	if err != nil || len(decisions) != 1 || !bytes.Equal(decisions[0].Value, command) {
		t.Fatalf("protected snapshot decision: count=%d error=%v", len(decisions), err)
	}
	other := checkpoint.NewManager(bucket, "cluster", t.TempDir(), 1)
	nextClaim, err := other.AcquirePublisherClaim(ctx, "next-publisher", 1, time.Minute)
	if err != nil {
		t.Fatalf("completed checkpoint retained its publisher claim: %v", err)
	}
	if err := other.ReleasePublisherClaim(ctx, nextClaim); err != nil {
		t.Fatalf("release next publisher claim: %v", err)
	}
	if err := pin.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := n.compactCertifiedCheckpoint(ctx); err != nil {
		t.Fatalf("compaction after confirmed reader close: %v", err)
	}
	if floor := core.CompactionFloor(); floor != 1 {
		t.Fatalf("floor after reader close=%d, want 1", floor)
	}
	freshAfter := checkpoint.NewManager(bucket, "cluster", t.TempDir(), 1)
	if err := freshAfter.Load(ctx); err != nil {
		t.Fatalf("fresh durable CURRENT read after deferred compaction: %v", err)
	}
	if latest := freshAfter.Latest(); latest == nil || latest.Index != published.Index || latest.RootHash != published.RootHash || latest.Hash != published.Hash {
		t.Fatalf("fresh durable CURRENT changed during deferred compaction: %+v", latest)
	}
	if got := bucket.currentUploads.Load(); got != 1 || publicationCalls != 1 {
		t.Fatalf("deferred compaction republished CURRENT: uploads=%d callbacks=%d", got, publicationCalls)
	}
}

func TestCertifiedCheckpointDefersOnlyHeldGCLockCompaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	held := &holdGCCandidateScan{Bucket: objstore.NewInMemBucket(), entered: make(chan struct{}), release: make(chan struct{})}
	bucket := &currentUploadCounter{Bucket: held}
	archive := recovery.NewManager(bucket, "cluster", 1)
	defer archive.Close()
	checkpoints := checkpoint.NewManager(bucket, "cluster", t.TempDir(), 1)
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	material, err := materializer.Open(filepath.Join(t.TempDir(), "material.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	command, err := types.EncodeKVCommand(types.KVCommand{RequestID: "gc-lock-compaction", Operation: "put", Key: "key", Value: []byte("value")})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, command); err != nil {
		t.Fatal(err)
	}
	if err := material.Apply(ctx, 1, command); err != nil {
		t.Fatal(err)
	}
	if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}

	holder := recovery.NewManager(bucket, "cluster", 1)
	defer holder.Close()
	gcDone := make(chan error, 1)
	go func() { gcDone <- holder.Cleanup(ctx, 24*time.Hour) }()
	select {
	case <-held.entered:
	case <-ctx.Done():
		t.Fatalf("Cleanup did not reach post-publication GC boundary: %v", ctx.Err())
	}
	released := false
	defer func() {
		if !released {
			close(held.release)
			<-gcDone
		}
	}()

	n := &Node{archive: archive, checkpoints: checkpoints, core: core, material: material}
	core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
		return checkpoints.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
	})
	auto := checkpoint.NewAutoCheckpointer(checkpoints, material, 1, 0)
	auto.ConfigurePublisher("publisher", func() uint64 { return 0 }, nil)
	var published *checkpoint.Checkpoint
	publicationCalls := 0
	auto.ConfigurePublication(nil, func(ctx context.Context, root *checkpoint.Checkpoint) error {
		publicationCalls++
		published = root
		prefix, ok := core.PrefixHash(1)
		if !ok {
			return errors.New("missing checkpoint prefix")
		}
		next, following, err := core.CheckpointLeaderOrders(1)
		if err != nil {
			return err
		}
		seal := quepaxa.CheckpointSeal{ConfigID: 1, Index: 1, RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following}
		if err := core.PrepareCheckpoint(ctx, seal); err != nil {
			return err
		}
		encoded, err := quepaxa.EncodeCheckpointSeal(seal)
		if err != nil {
			return err
		}
		if _, _, err := core.Propose(ctx, encoded); err != nil {
			return err
		}
		if err := n.publishCertifiedCheckpoint(ctx, root); err != nil {
			return err
		}
		return n.compactCertifiedCheckpoint(ctx)
	})
	if err := auto.CheckpointOnShutdown(ctx, 1); err != nil {
		t.Fatalf("certified publication under live GC lease: %v", err)
	}
	if publicationCalls != 1 || published == nil || bucket.currentUploads.Load() != 1 {
		t.Fatalf("publication calls=%d root=%v CURRENT uploads=%d", publicationCalls, published, bucket.currentUploads.Load())
	}
	if core.CompactionFloor() != 0 {
		t.Fatalf("floor advanced while GC lease held: %d", core.CompactionFloor())
	}
	fresh := checkpoint.NewManager(bucket, "cluster", t.TempDir(), 1)
	if err := fresh.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if latest := fresh.Latest(); latest == nil || latest.Index != published.Index || latest.RootHash != published.RootHash || latest.Hash != published.Hash {
		t.Fatalf("fresh CURRENT differs under GC lease: %+v", latest)
	}
	if err := fresh.Verify(ctx, published.Index, published.RootHash, published.Hash); err != nil {
		t.Fatal(err)
	}
	reader := recovery.NewManager(bucket, "cluster", 1)
	defer reader.Close()
	if err := reader.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if reader.Tip() < 1 {
		t.Fatalf("archive tip under GC lease=%d", reader.Tip())
	}
	decisions, _, err := reader.DecisionsFrom(ctx, 1, 1)
	if err != nil || len(decisions) != 1 || !bytes.Equal(decisions[0].Value, command) {
		t.Fatalf("retained archive decision under GC lease: count=%d err=%v", len(decisions), err)
	}
	close(held.release)
	released = true
	if err := <-gcDone; err != nil {
		t.Fatalf("held Cleanup after release: %v", err)
	}
	if err := n.compactCertifiedCheckpoint(ctx); err != nil {
		t.Fatalf("compaction after GC release: %v", err)
	}
	if core.CompactionFloor() != 1 {
		t.Fatalf("floor after GC release=%d", core.CompactionFloor())
	}
	freshAfter := checkpoint.NewManager(bucket, "cluster", t.TempDir(), 1)
	if err := freshAfter.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if latest := freshAfter.Latest(); latest == nil || latest.Index != published.Index || latest.RootHash != published.RootHash || latest.Hash != published.Hash {
		t.Fatalf("CURRENT changed during deferred compaction: %+v", latest)
	}
	if err := freshAfter.Verify(ctx, published.Index, published.RootHash, published.Hash); err != nil {
		t.Fatalf("fresh checkpoint verification after GC release: %v", err)
	}
	if bucket.currentUploads.Load() != 1 || publicationCalls != 1 {
		t.Fatalf("deferred compaction republished: uploads=%d callbacks=%d", bucket.currentUploads.Load(), publicationCalls)
	}
}
