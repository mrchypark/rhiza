package rhiza

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	thanosobjstore "github.com/thanos-io/objstore"
)

type learnerCheckpointSource struct {
	core        *quepaxa.Core
	material    *materializer.Materializer
	wal         *qlog.WAL
	checkpoints *checkpoint.Manager
	archive     *recovery.Manager
	bucket      *objstore.MeteredBucket
}

func newLearnerCheckpointSource(t *testing.T, bucketDir string) *learnerCheckpointSource {
	t.Helper()
	ctx := context.Background()
	bucket, err := objstore.NewBucket(objstore.Config{Provider: objstore.ProviderFilesystem, FilesystemDir: bucketDir})
	if err != nil {
		t.Fatal(err)
	}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		bucket.Close()
		t.Fatal(err)
	}
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, WAL: wal})
	if err != nil {
		wal.Close()
		bucket.Close()
		t.Fatal(err)
	}
	material, err := materializer.Open(filepath.Join(t.TempDir(), "source.sqlite"), 1)
	if err != nil {
		wal.Close()
		bucket.Close()
		t.Fatal(err)
	}
	checkpoints := checkpoint.NewManager(bucket, "learner-adoption", t.TempDir(), 1)
	archive := recovery.NewManager(bucket, "learner-adoption", 1)
	if err := checkpoints.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if err := archive.Load(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		archive.Close()
		_ = material.Close()
		_ = wal.Close()
		_ = bucket.Close()
	})
	return &learnerCheckpointSource{core: core, material: material, wal: wal, checkpoints: checkpoints, archive: archive, bucket: bucket}
}

func (s *learnerCheckpointSource) execute(t *testing.T, sql string) {
	t.Helper()
	ctx := context.Background()
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: fmt.Sprintf("source-%d", s.core.Tip()+1), SQL: sql}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.core.Propose(ctx, value); err != nil {
		t.Fatal(err)
	}
	decision, ok := s.core.CertifiedValue(s.core.Tip())
	if !ok {
		t.Fatalf("missing source decision at %d", s.core.Tip())
	}
	if err := s.material.ApplyBatch(ctx, []quepaxa.DecidedValue{decision}); err != nil {
		t.Fatal(err)
	}
}

func (s *learnerCheckpointSource) publishCheckpoint(t *testing.T, trimArchive bool) (*checkpoint.Checkpoint, quepaxa.SealedCheckpoint) {
	t.Helper()
	ctx := context.Background()
	files, index, cleanup, err := s.material.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if index == 0 || quepaxa.Slot(index) > s.core.Tip() {
		t.Fatalf("invalid checkpoint index %d at core tip %d", index, s.core.Tip())
	}
	claim, err := s.checkpoints.AcquirePublisherClaim(ctx, "n1", index-1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	root, err := s.checkpoints.CreateFiles(ctx, claim, []checkpoint.Source{
		{Role: checkpoint.RoleSQLite, Path: files[0].Path},
		{Role: checkpoint.RoleGraphData, Path: files[1].Path},
	}, index)
	if err != nil {
		_ = s.checkpoints.ReleasePublisherClaim(ctx, claim)
		t.Fatal(err)
	}
	prefix, ok := s.core.PrefixHash(quepaxa.Slot(index))
	if !ok {
		t.Fatal("checkpoint prefix unavailable")
	}
	next, following, err := s.core.CheckpointLeaderOrders(quepaxa.Slot(index))
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{ConfigID: s.core.ConfigIDForSlot(quepaxa.Slot(index)), Index: quepaxa.Slot(index),
		RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next,
		FollowingLeaderOrder: following, GenerationAnchorHash: s.core.GenerationAnchorHash()}
	s.core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
		return s.checkpoints.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
	})
	if err := s.core.PrepareCheckpoint(ctx, seal); err != nil {
		_ = s.checkpoints.ReleasePublisherClaim(ctx, claim)
		t.Fatal(err)
	}
	value, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		_ = s.checkpoints.ReleasePublisherClaim(ctx, claim)
		t.Fatal(err)
	}
	if _, _, err := s.core.Propose(ctx, value); err != nil {
		_ = s.checkpoints.ReleasePublisherClaim(ctx, claim)
		t.Fatal(err)
	}
	decision, ok := s.core.CertifiedValue(s.core.Tip())
	if !ok {
		_ = s.checkpoints.ReleasePublisherClaim(ctx, claim)
		t.Fatal("checkpoint seal decision missing")
	}
	if err := s.material.ApplyBatch(ctx, []quepaxa.DecidedValue{decision}); err != nil {
		_ = s.checkpoints.ReleasePublisherClaim(ctx, claim)
		t.Fatal(err)
	}
	if err := s.archive.SyncThrough(ctx, s.core, s.core.Tip()); err != nil {
		_ = s.checkpoints.ReleasePublisherClaim(ctx, claim)
		t.Fatal(err)
	}
	sealed, ok, err := s.core.LatestCheckpointSeal()
	if err != nil || !ok {
		_ = s.checkpoints.ReleasePublisherClaim(ctx, claim)
		t.Fatalf("checkpoint seal missing: %v", err)
	}
	if err := s.checkpoints.PromoteCertifiedCurrent(ctx, root); err != nil {
		_ = s.checkpoints.ReleasePublisherClaim(ctx, claim)
		t.Fatal(err)
	}
	if trimArchive {
		if err := s.archive.TrimThrough(ctx, sealed, decision); err != nil {
			_ = s.checkpoints.ReleasePublisherClaim(ctx, claim)
			t.Fatal(err)
		}
	}
	if err := s.checkpoints.ReleasePublisherClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	return root, sealed
}

func learnerReplicaConfig(dataDir, bucketDir, replicaID string) ReplicaConfig {
	return ReplicaConfig{ClusterID: "learner-adoption", ReplicaID: replicaID, DataDir: dataDir,
		Members:    []ReplicaMember{{ID: "n1", PeerURL: "quic://127.0.0.1:1", PublicKey: [32]byte(network.PeerPublicKey("learner-adoption", "n1", "voter"))}},
		AdminToken: "test-admin", ObjStoreProvider: "filesystem", ObjStoreDir: bucketDir, SyncInterval: time.Hour}
}

func openTestLearner(t *testing.T, config ReplicaConfig, source *learnerCheckpointSource) *ReadReplica {
	t.Helper()
	r, err := OpenReadReplica(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	r.mode = ReplicaModeLearner
	r.statusMu.Lock()
	r.status.Mode = ReplicaModeLearner
	r.statusMu.Unlock()
	r.fetch = func(_ context.Context, _ quepaxa.NodeID, from quepaxa.Slot, limit int) (network.DecisionsResponse, error) {
		values, tip, err := source.core.DecisionsFrom(from, limit)
		return network.DecisionsResponse{Tip: tip, Decisions: values}, err
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func syncLearnerToFloor(t *testing.T, r *ReadReplica, want quepaxa.Slot) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		r.syncMu.Lock()
		r.nextCheckpointProbe = time.Now()
		r.syncMu.Unlock()
		if err := r.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}
		if r.core.CompactionFloor() >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("learner did not adopt checkpoint floor %d (tip=%d floor=%d status=%+v)", want, r.core.Tip(), r.core.CompactionFloor(), r.Status())
}

func TestLearnerAdoptsPublishedArchiveRecoveryBase(t *testing.T) {
	ctx := context.Background()
	bucketDir := t.TempDir()
	source := newLearnerCheckpointSource(t, bucketDir)
	r := openTestLearner(t, learnerReplicaConfig(t.TempDir(), bucketDir, "reader"), source)
	source.execute(t, "CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)")
	source.execute(t, "INSERT INTO items VALUES (1, 1)")
	_, sealed := source.publishCheckpoint(t, true)
	r.syncMu.Lock()
	r.nextCheckpointProbe = time.Now()
	r.syncMu.Unlock()
	syncLearnerToFloor(t, r, sealed.Index)
	if r.core.Tip() < sealed.DecisionSlot || r.material.Tip() < uint64(sealed.DecisionSlot) {
		t.Fatalf("learner compacted without applying seal decision: core=%d material=%d decision=%d", r.core.Tip(), r.material.Tip(), sealed.DecisionSlot)
	}
	if status := r.Status(); status.Source != "peer:n1" || status.SourceTip < uint64(sealed.DecisionSlot) {
		t.Fatalf("peer progress was not preserved during checkpoint adoption: %+v", status)
	}
	rows, err := r.Query(ctx, QueryRequest{SQL: "SELECT value FROM items WHERE id=1"})
	if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != int64(1) {
		t.Fatalf("checkpoint changed applied data: rows=%#v err=%v", rows.Rows, err)
	}
	beforeSuccessor := r.ObjectStoreStats()
	source.execute(t, "UPDATE items SET value=2 WHERE id=1")
	_, successor := source.publishCheckpoint(t, true)
	syncLearnerToFloor(t, r, successor.Index)
	afterSuccessor := r.ObjectStoreStats()
	if status := r.Status(); status.Source != "peer:n1" || status.LastError != "" || r.core.CompactionFloor() != successor.Index {
		t.Fatalf("repeated healthy-peer adoption status=%+v floor=%d want=%d", status, r.core.CompactionFloor(), successor.Index)
	}
	if afterSuccessor.Gets <= beforeSuccessor.Gets {
		t.Fatalf("successor candidate reused prior verifier content cache: GETs before=%d after=%d", beforeSuccessor.Gets, afterSuccessor.Gets)
	}
}

func TestLearnerPeerOnlySealDoesNotAuthorizeAdoptionOrFrequentProbes(t *testing.T) {
	ctx := context.Background()
	bucketDir := t.TempDir()
	source := newLearnerCheckpointSource(t, bucketDir)
	r := openTestLearner(t, learnerReplicaConfig(t.TempDir(), bucketDir, "reader"), source)
	source.execute(t, "CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)")
	source.execute(t, "INSERT INTO items VALUES (1, 1)")
	_, sealed := source.publishCheckpoint(t, false)
	r.syncMu.Lock()
	r.nextCheckpointProbe = time.Now()
	r.syncMu.Unlock()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := r.Sync(ctx); err != nil {
			t.Fatal(err)
		}
		r.syncMu.Lock()
		finished := r.checkpointJob == nil
		r.syncMu.Unlock()
		if finished {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if r.core.CompactionFloor() != 0 || r.core.Tip() < sealed.DecisionSlot || !r.Ready() {
		t.Fatalf("peer-only seal changed authority/readiness: floor=%d tip=%d ready=%t", r.core.CompactionFloor(), r.core.Tip(), r.Ready())
	}
	before := r.ObjectStoreStats()
	for range 2000 {
		if err := r.Sync(ctx); err != nil {
			t.Fatal(err)
		}
	}
	after := r.ObjectStoreStats()
	if after.Gets != before.Gets || after.Lists != before.Lists || after.Uploads != before.Uploads || after.Deletes != before.Deletes {
		t.Fatalf("frequent Sync calls repeated checkpoint probes: before=%+v after=%+v", before, after)
	}
}

type learnerCheckpointBlockingBucket struct {
	thanosobjstore.Bucket
	once        sync.Once
	releaseOnce sync.Once
	entered     chan struct{}
	release     chan struct{}
}

type learnerCheckpointOneShotBucket struct {
	thanosobjstore.Bucket
	once    sync.Once
	entered chan struct{}
}

func (b *learnerCheckpointOneShotBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	var blockedErr error
	if strings.Contains(name, "/checkpoint/blocks/") {
		b.once.Do(func() {
			close(b.entered)
			<-ctx.Done()
			blockedErr = ctx.Err()
		})
	}
	if blockedErr != nil {
		return nil, blockedErr
	}
	return b.Bucket.Get(ctx, name)
}

func (b *learnerCheckpointBlockingBucket) unblock() {
	b.releaseOnce.Do(func() { close(b.release) })
}

func (b *learnerCheckpointBlockingBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if strings.Contains(name, "/checkpoint/blocks/") {
		b.once.Do(func() { close(b.entered) })
		select {
		case <-b.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return b.Bucket.Get(ctx, name)
}

func TestLearnerPeerCatchupContinuesDuringCheckpointVerification(t *testing.T) {
	ctx := context.Background()
	bucketDir := t.TempDir()
	source := newLearnerCheckpointSource(t, bucketDir)
	r := openTestLearner(t, learnerReplicaConfig(t.TempDir(), bucketDir, "reader"), source)
	source.execute(t, "CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)")
	source.execute(t, "INSERT INTO items VALUES (1, 1)")
	_, sealed := source.publishCheckpoint(t, true)
	baseBucket := r.bucket.Bucket
	blocker := &learnerCheckpointBlockingBucket{Bucket: baseBucket, entered: make(chan struct{}), release: make(chan struct{})}
	r.bucket.Bucket = blocker
	t.Cleanup(func() { blocker.unblock(); r.bucket.Bucket = baseBucket })
	r.syncMu.Lock()
	r.nextCheckpointProbe = time.Now()
	r.syncMu.Unlock()
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-blocker.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("checkpoint verification did not reach its deterministic block read")
	}
	oldTip := r.core.Tip()
	source.execute(t, "UPDATE items SET value=2 WHERE id=1")
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if r.core.Tip() <= oldTip || r.material.Tip() != uint64(r.core.Tip()) {
		t.Fatalf("peer catch-up stalled behind checkpoint verification: old=%d core=%d material=%d", oldTip, r.core.Tip(), r.material.Tip())
	}
	blocker.unblock()
	syncLearnerToFloor(t, r, sealed.Index)
	if status := r.Status(); status.Source != "peer:n1" || status.SourceTip < uint64(r.core.Tip()) {
		t.Fatalf("peer source status lost after verification: %+v", status)
	}
}

func TestLearnerFallbackCancelsVerificationAndCloseJoinsIt(t *testing.T) {
	for _, test := range []struct {
		name     string
		fallback bool
	}{
		{name: "object_store_fallback", fallback: true},
		{name: "close", fallback: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			bucketDir := t.TempDir()
			source := newLearnerCheckpointSource(t, bucketDir)
			r := openTestLearner(t, learnerReplicaConfig(t.TempDir(), bucketDir, "reader"), source)
			source.execute(t, "CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)")
			source.execute(t, "INSERT INTO items VALUES (1, 1)")
			_, sealed := source.publishCheckpoint(t, true)
			baseBucket := r.bucket.Bucket
			blocker := &learnerCheckpointOneShotBucket{Bucket: baseBucket, entered: make(chan struct{})}
			r.bucket.Bucket = blocker
			t.Cleanup(func() { r.bucket.Bucket = baseBucket })
			r.syncMu.Lock()
			r.nextCheckpointProbe = time.Now()
			r.syncMu.Unlock()
			if err := r.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case <-blocker.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("verification did not reach its deterministic block read")
			}
			if test.fallback {
				r.fetch = func(context.Context, quepaxa.NodeID, quepaxa.Slot, int) (network.DecisionsResponse, error) {
					return network.DecisionsResponse{}, errors.New("injected peer failure")
				}
				if err := r.Sync(ctx); err != nil {
					t.Fatalf("object-store fallback after verification cancellation: %v", err)
				}
				if !r.Ready() || r.core.CompactionFloor() < sealed.Index {
					t.Fatalf("fallback did not complete cleanly: ready=%t floor=%d want>=%d", r.Ready(), r.core.CompactionFloor(), sealed.Index)
				}
				return
			}
			closed := make(chan error, 1)
			go func() { closed <- r.Close() }()
			select {
			case err := <-closed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not cancel and join checkpoint verification")
			}
			if r.Ready() {
				t.Fatal("closed learner remained ready")
			}
		})
	}
}

func TestLearnerWaitsForLocallyAppliedSealDecision(t *testing.T) {
	ctx := context.Background()
	bucketDir := t.TempDir()
	source := newLearnerCheckpointSource(t, bucketDir)
	r := openTestLearner(t, learnerReplicaConfig(t.TempDir(), bucketDir, "reader"), source)
	source.execute(t, "CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)")
	source.execute(t, "INSERT INTO items VALUES (1, 1)")
	_, sealed := source.publishCheckpoint(t, true)
	var peerTip quepaxa.Slot = sealed.Index
	r.fetch = func(_ context.Context, _ quepaxa.NodeID, from quepaxa.Slot, limit int) (network.DecisionsResponse, error) {
		values, _, err := source.core.DecisionsFrom(from, limit)
		if err != nil {
			return network.DecisionsResponse{}, err
		}
		filtered := values[:0]
		for _, value := range values {
			if value.Slot <= peerTip {
				filtered = append(filtered, value)
			}
		}
		return network.DecisionsResponse{Tip: peerTip, Decisions: filtered}, nil
	}
	r.syncMu.Lock()
	r.nextCheckpointProbe = time.Now()
	r.syncMu.Unlock()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := r.Sync(ctx); err != nil {
			t.Fatal(err)
		}
		r.syncMu.Lock()
		verified := r.checkpointJob != nil && r.checkpointJob.candidate != nil
		r.syncMu.Unlock()
		if verified {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if r.core.Tip() != sealed.Index || r.material.Tip() != uint64(sealed.Index) || r.core.CompactionFloor() != 0 {
		t.Fatalf("learner changed Core/material before applying seal decision: core=%d material=%d floor=%d seal=%d", r.core.Tip(), r.material.Tip(), r.core.CompactionFloor(), sealed.Index)
	}
	peerTip = sealed.DecisionSlot
	syncLearnerToFloor(t, r, sealed.Index)
	if r.core.Tip() < sealed.DecisionSlot || r.material.Tip() < uint64(sealed.DecisionSlot) {
		t.Fatalf("learner adopted before seal decision application: core=%d material=%d decision=%d", r.core.Tip(), r.material.Tip(), sealed.DecisionSlot)
	}
}

func TestLearnerRestartUsesPublishedSuccessorAfterCheckpointGC(t *testing.T) {
	ctx := context.Background()
	bucketDir := t.TempDir()
	source := newLearnerCheckpointSource(t, bucketDir)
	lagging := openTestLearner(t, learnerReplicaConfig(t.TempDir(), bucketDir, "lagging"), source)
	ahead := openTestLearner(t, learnerReplicaConfig(t.TempDir(), bucketDir, "ahead"), source)
	source.execute(t, "CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)")
	source.execute(t, "INSERT INTO items VALUES (1, 1)")
	firstRoot, first := source.publishCheckpoint(t, true)
	lagging.syncMu.Lock()
	lagging.nextCheckpointProbe = time.Now()
	lagging.syncMu.Unlock()
	syncLearnerToFloor(t, lagging, first.Index)
	ahead.syncMu.Lock()
	ahead.nextCheckpointProbe = time.Now()
	ahead.syncMu.Unlock()
	syncLearnerToFloor(t, ahead, first.Index)

	source.execute(t, "UPDATE items SET value=2 WHERE id=1")
	secondRoot, second := source.publishCheckpoint(t, true)
	if second.Index <= first.Index {
		t.Fatal("successor checkpoint did not advance")
	}
	// One replica is below the successor seal index and the other catches the
	// seal decision before restart, exercising both exact-base recovery branches.
	ahead.syncMu.Lock()
	ahead.nextCheckpointProbe = time.Now().Add(time.Hour)
	ahead.syncMu.Unlock()
	if err := ahead.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if ahead.core.Tip() < second.DecisionSlot {
		t.Fatalf("ahead replica did not apply successor seal: tip=%d want>=%d", ahead.core.Tip(), second.DecisionSlot)
	}
	if lagging.core.Tip() >= second.Index {
		t.Fatalf("lagging replica unexpectedly reached successor base: tip=%d successor=%d", lagging.core.Tip(), second.Index)
	}
	if err := lagging.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ahead.Close(); err != nil {
		t.Fatal(err)
	}
	removeReplicaMaterializedFiles(t, lagging.config.DataDir)
	removeReplicaMaterializedFiles(t, ahead.config.DataDir)

	if err := source.checkpoints.GarbageCollectFrom(ctx, nil, 1, uint64(second.Index), 0); err != nil {
		t.Fatal(err)
	}
	// A follow-up sweep removes content blocks whose predecessor root was
	// removed by the preceding pass.
	if err := source.checkpoints.GarbageCollectFrom(ctx, nil, 1, uint64(second.Index), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := source.checkpoints.OpenRoot(ctx, firstRoot.Index, firstRoot.RootHash); err == nil {
		t.Fatal("actual checkpoint GC retained obsolete predecessor root")
	}
	if _, err := source.checkpoints.OpenRoot(ctx, secondRoot.Index, secondRoot.RootHash); err != nil {
		t.Fatalf("successor recovery root unavailable after GC: %v", err)
	}
	removedBlock := false
	for _, file := range firstRoot.Files {
		for _, block := range file.Blocks {
			shared := false
			for _, nextFile := range secondRoot.Files {
				for _, nextBlock := range nextFile.Blocks {
					shared = shared || (block.Hash == nextBlock.Hash && block.Generation == nextBlock.Generation)
				}
			}
			if shared {
				continue
			}
			key := fmt.Sprintf("learner-adoption/checkpoint/blocks/%s.block", block.Hash)
			if block.Generation != 0 {
				key = fmt.Sprintf("learner-adoption/checkpoint/blocks/%s_%020d.block", block.Hash, block.Generation)
			}
			if _, err := source.bucket.Attributes(ctx, key); err == nil {
				t.Fatalf("actual checkpoint GC retained unreferenced predecessor block %s", key)
			} else if !source.bucket.IsObjNotFoundErr(err) {
				t.Fatalf("check predecessor block %s: %v", key, err)
			}
			removedBlock = true
			break
		}
		if removedBlock {
			break
		}
	}
	if !removedBlock {
		t.Fatal("test checkpoints had no predecessor-only content block to verify GC")
	}

	for _, tc := range []struct {
		name string
		id   string
		want uint64
	}{
		{name: "successor_above_local_tip", id: "lagging", want: 2},
		{name: "successor_at_or_below_local_tip", id: "ahead", want: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := map[string]string{"lagging": lagging.config.DataDir, "ahead": ahead.config.DataDir}[tc.id]
			config := learnerReplicaConfig(dataDir, bucketDir, tc.id)
			reopened, err := OpenLearner(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if reopened.core.CompactionFloor() != second.Index || reopened.core.Tip() < second.DecisionSlot || reopened.material.Tip() < uint64(second.DecisionSlot) {
				t.Fatalf("successor recovery floor=%d tip=%d material=%d, want floor=%d decision=%d", reopened.core.CompactionFloor(), reopened.core.Tip(), reopened.material.Tip(), second.Index, second.DecisionSlot)
			}
			rows, err := reopened.Query(ctx, QueryRequest{SQL: "SELECT value FROM items WHERE id=1"})
			if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != int64(tc.want) {
				t.Fatalf("successor snapshot/replay data=%#v err=%v", rows.Rows, err)
			}
		})
	}
}

func removeReplicaMaterializedFiles(t *testing.T, dataDir string) {
	t.Helper()
	for _, name := range []string{"sqlite.db", "sqlite.db-wal", "sqlite.db-shm"} {
		if err := os.Remove(filepath.Join(dataDir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
	if err := os.RemoveAll(filepath.Join(dataDir, "latticedb")); err != nil {
		t.Fatal(err)
	}
}
