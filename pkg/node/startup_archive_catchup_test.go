package node

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

func TestStartupArchiveCheckIsOneShotForCapturedVoterConfig(t *testing.T) {
	config := quepaxa.Cluster{ConfigID: 3, Members: []quepaxa.Member{{ID: "voter"}, {ID: "peer"}}}
	node := &Node{archive: recovery.NewManager(objstore.NewInMemBucket(), "startup-check", recovery.ArchiveConfigID)}
	node.initArchiveCatchUpHints()

	node.queueStartupArchiveCheck(config)
	config.Members[1].ID = "mutated"
	node.queueStartupArchiveCheck(config)

	captured, ok := node.takeStartupArchiveCheck()
	if !ok || captured.ConfigID != 3 || captured.Members[1].ID != "peer" {
		t.Fatalf("startup archive check captured=%+v ok=%t", captured, ok)
	}
	if _, ok := node.takeStartupArchiveCheck(); ok {
		t.Fatal("startup archive check was queued more than once")
	}
}

func TestStartupArchiveCatchUpRefreshesAndAppliesCertifiedPrefix(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	cluster := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "voter"}}}

	sourceWAL, err := qlog.Open(filepath.Join(t.TempDir(), "source"))
	if err != nil {
		t.Fatal(err)
	}
	defer sourceWAL.Close()
	source, err := quepaxa.New(quepaxa.Config{NodeID: "voter", Cluster: cluster, WAL: sourceWAL})
	if err != nil {
		t.Fatal(err)
	}
	publisher := recovery.NewManager(bucket, "startup-catchup", recovery.ArchiveConfigID)
	defer publisher.Close()
	if err := publisher.Load(ctx); err != nil {
		t.Fatal(err)
	}
	reader := recovery.NewManager(bucket, "startup-catchup", recovery.ArchiveConfigID)
	defer reader.Close()
	if err := reader.Load(ctx); err != nil {
		t.Fatal(err)
	}

	value := types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{1})
	if _, _, err := source.Propose(ctx, value); err != nil {
		t.Fatal(err)
	}
	if err := publisher.SyncThrough(ctx, source, source.Tip()); err != nil {
		t.Fatal(err)
	}

	targetWAL, err := qlog.Open(filepath.Join(t.TempDir(), "target"))
	if err != nil {
		t.Fatal(err)
	}
	defer targetWAL.Close()
	target, err := quepaxa.New(quepaxa.Config{NodeID: "voter", Cluster: cluster, WAL: targetWAL})
	if err != nil {
		t.Fatal(err)
	}
	material, err := materializer.Open(filepath.Join(t.TempDir(), "state.sqlite"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	node := &Node{
		config:   &types.ExecutionConfig{EnableReconfiguration: true},
		core:     target,
		archive:  reader,
		material: material,
	}

	if err := node.catchUpStartupArchive(ctx, cluster); err != nil {
		t.Fatalf("fixed startup archive catch-up: %v", err)
	}
	if got := target.Tip(); got != 1 {
		t.Fatalf("certified tip=%d, want 1", got)
	}
	if got := material.Tip(); got != 1 {
		t.Fatalf("materialized tip=%d, want 1", got)
	}
}
