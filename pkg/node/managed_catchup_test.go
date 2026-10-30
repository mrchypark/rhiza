package node

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

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
