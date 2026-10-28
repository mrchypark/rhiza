package node

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	objmetrics "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

func TestInitialBootstrapBarrierPublishesAndReadsBackExactDecision(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dataDir := t.TempDir()
	wal, err := qlog.Open(filepath.Join(dataDir, "qlog"))
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
	material, err := materializer.Open(filepath.Join(dataDir, "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	bucket, err := objmetrics.NewBucket(objmetrics.Config{Provider: objmetrics.ProviderFilesystem, FilesystemDir: filepath.Join(dataDir, "objects")})
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Close()
	archive := recovery.NewManager(bucket, "prefix/cluster", 1)
	defer archive.Close()
	if err := archive.Load(ctx); err != nil {
		t.Fatal(err)
	}
	server := network.NewServer(core, material, "cluster", true, nil, func() bool { return true })
	defer server.Close()
	n := &Node{
		config:  &types.ExecutionConfig{ClusterID: "cluster", NodeID: "n1", ObjStorePrefix: "prefix"},
		core:    core,
		server:  server,
		archive: archive,
		bucket:  bucket,
	}

	var nonce [types.ReadBarrierNonceSize]byte
	nonce[0] = 42
	proof, err := n.persistBootstrapBarrier(ctx, nonce)
	if err != nil {
		t.Fatal(err)
	}
	if proof != "1" || core.Tip() != 1 {
		t.Fatalf("barrier proof=%q core tip=%d, want slot 1", proof, core.Tip())
	}
	decision, ok := core.CertifiedValue(1)
	if !ok || len(decision.Certificate) == 0 {
		t.Fatal("local barrier decision is not certified")
	}
	readback := recovery.NewManager(bucket, "prefix/cluster", 1)
	defer readback.Close()
	if err := readback.Load(ctx); err != nil {
		t.Fatal(err)
	}
	values, tip, err := readback.DecisionsFrom(ctx, 1, 1)
	if err != nil || tip != 1 || len(values) != 1 || values[0].Slot != decision.Slot || values[0].Hash != decision.Hash ||
		string(values[0].Value) != string(decision.Value) || string(values[0].Certificate) != string(decision.Certificate) {
		t.Fatalf("archive decision readback=%+v tip=%d err=%v", values, tip, err)
	}

	proof, err = n.persistBootstrapBarrier(ctx, nonce)
	if err != nil || proof != "1" || core.Tip() != 1 {
		t.Fatalf("retry proof=%q tip=%d err=%v; exact barrier should be reused", proof, core.Tip(), err)
	}
}

func TestInitialBootstrapBarrierRequiresArchiveAndRefusesMutationWithoutIt(t *testing.T) {
	dataDir := t.TempDir()
	wal, err := qlog.Open(filepath.Join(dataDir, "qlog"))
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
	material, err := materializer.Open(filepath.Join(dataDir, "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := network.NewServer(core, material, "cluster", true, nil, func() bool { return false })
	defer server.Close()
	n := &Node{config: &types.ExecutionConfig{ClusterID: "cluster", NodeID: "n1"}, core: core, server: server}
	var nonce [types.ReadBarrierNonceSize]byte
	if _, err := n.persistBootstrapBarrier(context.Background(), nonce); err == nil {
		t.Fatal("barrier succeeded without a shared archive")
	}
	if core.Tip() != 0 {
		t.Fatalf("refused barrier changed local tip to %d", core.Tip())
	}
}

func TestInitialBootstrapBarrierNonceBindsOperation(t *testing.T) {
	first := fencing.BootstrapBarrierNonce("operation-a")
	retry := fencing.BootstrapBarrierNonce("operation-a")
	if first != retry {
		t.Fatalf("same operation retry nonce differs: %x %x", first, retry)
	}
	otherOperation := fencing.BootstrapBarrierNonce("operation-b")
	if first == otherOperation {
		t.Fatalf("operation change did not change nonce: %x %x", first, otherOperation)
	}
}
