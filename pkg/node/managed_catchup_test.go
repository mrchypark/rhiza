package node

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

type managedCatchupBucket struct {
	objstore.Bucket
	writes atomic.Int64
}

func TestManagedPublisherPublishesExactCheckpointWithoutTrimmingArchive(t *testing.T) {
	tests := []struct {
		name        string
		policy      sqlpolicy.MaterializationPolicy
		protocol    string
		validate    func([]byte) error
		clusterName string
	}{
		{
			name: "request", policy: sqlpolicy.ManagedRequestMaterializationPolicy,
			protocol: sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, validate: types.ValidateManagedKVExecutionPolicy,
			clusterName: "managed-request-catchup-publication",
		},
		{
			name: "graph", policy: sqlpolicy.ManagedGraphRequestMaterializationPolicy,
			protocol: sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol, validate: types.ValidateManagedGraphExecutionPolicy,
			clusterName: "managed-graph-catchup-publication",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			fixture := newManagedAppendOnlyAdmissionFixture(t, test.clusterName, test.policy, test.protocol, test.validate)
			n := fixture.node
			fixture.transport.releaseAll()
			n.managedGraphInitialRootDone = make(chan struct{})
			beforePublication := fixture.bucket.mutationSnapshot()
			if err := n.publishManagedAppendOnlyCheckpoint(ctx); err != nil {
				t.Fatal(err)
			}
			sealed, ok, err := n.core.LatestManagedCheckpointSeal()
			if err != nil || !ok {
				t.Fatalf("certified QCKP unavailable: ok=%t err=%v", ok, err)
			}
			locator, exists, err := n.checkpoints.LoadManagedAppendOnlyLocator(ctx, test.policy)
			decision, certified := n.core.CertifiedValue(sealed.DecisionSlot)
			if err != nil || !exists || !certified || locator.Policy != int(test.policy) || locator.Index != uint64(sealed.Index) || locator.RootHash != sealed.RootHash || locator.SealDecisionSlot != uint64(sealed.DecisionSlot) || locator.SealDecisionHash != decision.Hash {
				t.Fatalf("CURRENT does not identify the exact published QCKP: locator=%+v exists=%t decision=%+v certified=%t seal=%+v err=%v", locator, exists, decision, certified, sealed, err)
			}
			if floor := n.core.CompactionFloor(); floor != 0 {
				t.Fatalf("small managed WAL was compacted before the local threshold: floor=%d index=%d", floor, sealed.Index)
			}
			if n.archive.Tip() != sealed.DecisionSlot {
				t.Fatalf("publisher changed archived tip: got=%d want=%d", n.archive.Tip(), sealed.DecisionSlot)
			}
			if base, _, ok := n.archive.RecoveryBase(); ok {
				t.Fatalf("production publisher trimmed archive base: %+v", base)
			}
			for _, mutation := range fixture.bucket.mutationSnapshot()[len(beforePublication):] {
				if mutation.operation == "delete" {
					t.Fatalf("production publisher destructively mutated archive: key=%s", mutation.key)
				}
			}
			decision, ok = n.core.CertifiedValue(sealed.DecisionSlot)
			if !ok {
				t.Fatal("published QCKP certificate unavailable")
			}
			if err := n.archive.TrimThrough(ctx, sealed, decision); err != nil {
				t.Fatal(err)
			}
			if base, _, ok := n.archive.RecoveryBase(); !ok || base.Index != sealed.Index {
				t.Fatalf("manual recovery floor unavailable: base=%+v ok=%t", base, ok)
			}

			beforeCatchup := fixture.bucket.mutationSnapshot()
			below := &Node{config: n.config, managedStartup: n.managedStartup, archive: n.archive, core: newEmptyManagedAppendOnlyCore(t, fixture)}
			below.ready.Store(true)
			err = below.catchUpArchive(ctx)
			if err == nil || !strings.Contains(err.Error(), "managed checkpoint catch-up is unavailable") || below.ready.Load() {
				t.Fatalf("below-floor catch-up did not fail closed: err=%v ready=%t", err, below.ready.Load())
			}
			if after := fixture.bucket.mutationSnapshot(); !reflect.DeepEqual(after, beforeCatchup) {
				t.Fatalf("below-floor catch-up mutated storage: before=%v after=%v", beforeCatchup, after)
			}
		})
	}
}

func TestManagedLocalWALCompactionThreshold(t *testing.T) {
	const mib = int64(1 << 20)
	for _, tc := range []struct {
		name        string
		used, limit int64
		want        bool
	}{
		{name: "empty", limit: 4 << 30},
		{name: "below cap", used: 255 * mib, limit: 4 << 30},
		{name: "at cap", used: 256 * mib, limit: 4 << 30, want: true},
		{name: "half of small limit", used: mib, limit: 2 * mib, want: true},
		{name: "unbounded", used: 1 << 40},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := managedLocalWALCompactionDue(tc.used, tc.limit); got != tc.want {
				t.Fatalf("due(%d,%d)=%t want %t", tc.used, tc.limit, got, tc.want)
			}
		})
	}
}

func newEmptyManagedAppendOnlyCore(t *testing.T, fixture *managedGraphAppendOnlyAdmissionFixture) *quepaxa.Core {
	t.Helper()
	n := fixture.node
	nonce, registered := n.wal.Identity()
	if !registered || len(nonce) != 32 {
		t.Fatal("admitted fixture WAL identity unavailable")
	}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	if err := wal.BindIdentity(nonce); err != nil {
		t.Fatal(err)
	}
	var walIdentity [32]byte
	copy(walIdentity[:], nonce)
	core, err := quepaxa.New(quepaxa.Config{
		NodeID: n.config.NodeID, Cluster: n.core.CurrentCluster(), WAL: wal, Transport: fixture.transport, ValidateValue: fixture.validate,
		ManagedAppendOnly: &quepaxa.ManagedAppendOnlyConfig{Binding: quepaxa.ManagedAppendOnlyBinding{
			ApplicationProtocol: fixture.protocol, BootstrapPlanDigest: n.managedStartup.Identity.BootstrapPlanDigest,
			MemberConfigDigest: n.managedStartup.Identity.Member.ConfigDigest, Generation: 0, WALIdentity: walIdentity,
		}, Verify: n.verifyManagedAppendOnlyCandidate},
	})
	if err != nil {
		t.Fatal(err)
	}
	core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
		return n.checkpoints.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
	})
	return core
}

func TestManagedCatchupArchiveBaseTrigger(t *testing.T) {
	ctx := context.Background()
	bucket := &managedCatchupBucket{Bucket: objstore.NewInMemBucket()}
	archive := recovery.NewManager(bucket, "managed", 1)
	t.Cleanup(archive.Close)
	newCore := func() *quepaxa.Core {
		wal, err := qlog.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, WAL: wal})
		if err != nil {
			t.Fatal(err)
		}
		return core
	}
	source := newCore()
	if _, _, err := source.Propose(ctx, []byte("decision")); err != nil {
		t.Fatal(err)
	}
	prefix, ok := source.PrefixHash(1)
	if !ok {
		t.Fatal("missing certified prefix")
	}
	next, following, err := source.CheckpointLeaderOrders(1)
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{ConfigID: 1, Index: 1, RootHash: [32]byte{1}, StateHash: [32]byte{2}, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following}
	source.SetCheckpointValidator(func(context.Context, quepaxa.CheckpointSeal) error { return nil })
	if err := source.PrepareCheckpoint(ctx, seal); err != nil {
		t.Fatal(err)
	}
	value, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	slot, _, err := source.Propose(ctx, value)
	if err != nil {
		t.Fatal(err)
	}
	decision, ok := source.CertifiedValue(slot)
	if !ok {
		t.Fatal("missing certified seal")
	}
	if err := archive.SyncThrough(ctx, source, source.Tip()); err != nil {
		t.Fatal(err)
	}
	if err := archive.TrimThrough(ctx, quepaxa.SealedCheckpoint{CheckpointSeal: seal, DecisionSlot: slot}, decision); err != nil {
		t.Fatal(err)
	}
	if base, _, ok := archive.RecoveryBase(); !ok || base.Index != 1 {
		t.Fatal("archive base not established")
	}
	bucket.writes.Store(0)
	n := &Node{config: &types.ExecutionConfig{NodeID: "n1"}, managedStartup: &ManagedStartupConfig{}, archive: archive, core: newCore(), checkpoints: checkpoint.NewManager(bucket, "managed", t.TempDir(), 1), material: &materializer.Materializer{}, server: &network.Server{}}
	n.ready.Store(true)
	err = n.catchUpArchive(ctx)
	if err == nil || !strings.Contains(err.Error(), "managed checkpoint catch-up is unavailable") || n.ready.Load() || bucket.writes.Load() != 0 {
		t.Fatalf("archive catch-up err=%v ready=%v writes=%d", err, n.ready.Load(), bucket.writes.Load())
	}
	if err := source.CompactThrough(seal.Index, seal.RootHash); err != nil {
		t.Fatal(err)
	}
	peerMaterial, err := materializer.Open(filepath.Join(t.TempDir(), "peer.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peerMaterial.Close() })
	server := network.NewServer(source, peerMaterial, "cluster", true, nil)
	t.Cleanup(server.Close)
	members := []quepaxa.Member{peerMember("cluster", "n1", "n1-token"), peerMember("cluster", "n2", "n2-token")}
	peer, err := network.StartPeerServer(ctx, "127.0.0.1:0", server, members, "n1-token", "admin")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	members[0].PeerURL = "quic://" + peer.Addr()
	transport := network.NewTransport("cluster", "n2", &quepaxa.Cluster{ConfigID: 1, Members: members}, "n2-token")
	transport.SetAdminToken("admin")
	t.Cleanup(func() { _ = transport.Close() })
	if _, err := transport.FetchDecisions(ctx, "n1", 1, 128); !errors.Is(err, quepaxa.ErrCompacted) {
		t.Fatalf("peer did not report compacted: %v", err)
	}
	localMaterial, err := materializer.Open(filepath.Join(t.TempDir(), "local.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = localMaterial.Close() })
	n.material = localMaterial
	n.ready.Store(true)
	bucket.writes.Store(0)
	err = n.catchUpPeer(ctx, transport, "n1")
	if err == nil || !strings.Contains(err.Error(), "managed checkpoint catch-up is unavailable") || n.ready.Load() || bucket.writes.Load() != 0 {
		t.Fatalf("peer catch-up err=%v ready=%v writes=%d", err, n.ready.Load(), bucket.writes.Load())
	}
}

func (b *managedCatchupBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	b.writes.Add(1)
	return b.Bucket.Upload(ctx, name, r, opts...)
}

func TestManagedCatchupBlocksArchiveRestore(t *testing.T) {
	bucket := &managedCatchupBucket{Bucket: objstore.NewInMemBucket()}
	archive := recovery.NewManager(bucket, "managed", 1)
	t.Cleanup(archive.Close)
	n := &Node{config: &types.ExecutionConfig{NodeID: "node"}, managedStartup: &ManagedStartupConfig{}, archive: archive,
		checkpoints: checkpoint.NewManager(bucket, "managed", t.TempDir(), 1), core: &quepaxa.Core{}, material: &materializer.Materializer{}, server: &network.Server{}}
	n.ready.Store(true)
	err := n.restoreArchiveCatchUp(context.Background())
	if err == nil || !strings.Contains(err.Error(), "managed checkpoint catch-up is unavailable") {
		t.Fatalf("managed restore error: %v", err)
	}
	if n.ready.Load() || bucket.writes.Load() != 0 {
		t.Fatalf("ready=%v writes=%d", n.ready.Load(), bucket.writes.Load())
	}
	n.managedStartup = nil
	if err := n.restoreArchiveCatchUp(context.Background()); err == nil || !strings.Contains(err.Error(), "shared archive has no checkpoint recovery base") {
		t.Fatalf("ordinary restore error: %v", err)
	}
	if bucket.writes.Load() == 0 {
		t.Fatal("ordinary restore did not reach archive snapshot")
	}
}
