package node

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
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

func (b *currentUploadCounter) Upload(ctx context.Context, name string, value io.Reader, options ...objstore.ObjectUploadOption) error {
	if err := b.Bucket.Upload(ctx, name, value, options...); err != nil {
		return err
	}
	if strings.HasSuffix(name, "/checkpoint/CURRENT") {
		b.currentUploads.Add(1)
	}
	return nil
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
