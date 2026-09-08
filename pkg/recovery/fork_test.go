package recovery

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

func TestForkCopiesVerifiedGenerationAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	members := []quepaxa.Member{{ID: "n1"}}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: members}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, []byte("before checkpoint")); err != nil {
		t.Fatal(err)
	}
	checkpoints := checkpoint.NewManager(bucket, "source", t.TempDir(), 1)
	file := filepath.Join(t.TempDir(), "sqlite.db")
	if err := os.WriteFile(file, []byte("checkpoint"), 0o600); err != nil {
		t.Fatal(err)
	}
	claim, err := checkpoints.AcquirePublisherClaim(ctx, "test", 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	root, err := checkpoints.CreateFiles(ctx, claim, []checkpoint.Source{{Role: checkpoint.RoleSQLite, Path: file}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkpoints.ReleasePublisherClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	prefix, ok := core.PrefixHash(1)
	if !ok {
		t.Fatal("missing prefix")
	}
	next, following, err := core.CheckpointLeaderOrders(1)
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{ConfigID: 1, Index: 1, RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following}
	core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
		return checkpoints.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
	})
	if err := core.PrepareCheckpoint(ctx, seal); err != nil {
		t.Fatal(err)
	}
	encoded, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, encoded); err != nil {
		t.Fatal(err)
	}
	archive := NewManager(bucket, "source", 1)
	if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	sealed, ok, err := core.LatestCheckpointSeal()
	if err != nil || !ok {
		t.Fatalf("seal=%v err=%v", ok, err)
	}
	decision, ok := core.CertifiedValue(sealed.DecisionSlot)
	if !ok {
		t.Fatal("missing seal decision")
	}
	if err := archive.TrimThrough(ctx, sealed, decision); err != nil {
		t.Fatal(err)
	}
	if err := Seal(ctx, bucket, "source", "op-1"); err != nil {
		t.Fatal(err)
	}

	options := ForkOptions{SourcePrefix: "source", TargetPrefix: "target", Members: members, OperationID: "op-1"}
	result, err := Fork(ctx, bucket, options)
	if err != nil {
		t.Fatal(err)
	}
	again, err := Fork(ctx, bucket, options)
	if err != nil || again != result {
		t.Fatalf("retry result=%+v err=%v", again, err)
	}
	target := NewManager(bucket, "target", 1)
	if err := target.Load(ctx); err != nil {
		t.Fatalf("load target: %v", err)
	}
	if target.Tip() != quepaxa.Slot(result.Tip) {
		t.Fatalf("target tip=%d result=%d", target.Tip(), result.Tip)
	}
	targetCheckpoint := checkpoint.NewManager(bucket, "target", "", 1)
	if err := targetCheckpoint.Load(ctx); err != nil || targetCheckpoint.Latest() == nil {
		t.Fatalf("target checkpoint err=%v", err)
	}
	if _, err := Fork(ctx, bucket, ForkOptions{SourcePrefix: "source", TargetPrefix: "target", Members: members, OperationID: "other"}); err == nil {
		t.Fatal("different operation reused target")
	}
	if _, err := Fork(ctx, bucket, ForkOptions{SourcePrefix: "source", TargetPrefix: "source", Members: members, OperationID: "bad"}); err == nil {
		t.Fatal("same source/target accepted")
	}
	if err := bucket.Delete(ctx, "target/archive/head.bin"); err != nil {
		t.Fatal(err)
	}
	if _, err := Fork(ctx, bucket, options); err == nil {
		t.Fatal("cached fork result accepted a tampered target")
	}
}

func TestForkRequiresSealedSourceAndIsolatedPrefixes(t *testing.T) {
	ctx, bucket, _, _ := newSealableArchive(t)
	members := []quepaxa.Member{{ID: "n1"}}
	if _, err := Fork(ctx, bucket, ForkOptions{SourcePrefix: "cluster", TargetPrefix: "target", Members: members, OperationID: "op"}); err == nil {
		t.Fatal("unsealed source accepted")
	}
	for _, options := range []ForkOptions{
		{SourcePrefix: "source", TargetPrefix: "source/target", Members: members, OperationID: "overlap"},
		{SourcePrefix: "../source", TargetPrefix: "target", Members: members, OperationID: "traversal"},
	} {
		if _, err := Fork(ctx, objstore.NewInMemBucket(), options); err == nil {
			t.Fatalf("unsafe prefixes accepted: %+v", options)
		}
	}
}

func TestForkRejectsMissingRecoveryBase(t *testing.T) {
	_, err := Fork(context.Background(), objstore.NewInMemBucket(), ForkOptions{SourcePrefix: "source", TargetPrefix: "target", Members: []quepaxa.Member{{ID: "n1"}}, OperationID: "op"})
	if err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("missing source err=%v", err)
	}
}

func TestForkCopiesUncompactedCertifiedSuffix(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	members := []quepaxa.Member{{ID: "n1", Token: "secret"}}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: members}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, []byte("uncompacted")); err != nil {
		t.Fatal(err)
	}
	archive := NewManager(bucket, "source", 1)
	if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	if err := Seal(ctx, bucket, "source", "op"); err != nil {
		t.Fatal(err)
	}
	result, err := Fork(ctx, bucket, ForkOptions{SourcePrefix: "source", TargetPrefix: "target", Members: members, OperationID: "op"})
	if err != nil || result.Tip != 1 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	target := NewManager(bucket, "target", 1)
	if err := target.Load(ctx); err != nil || target.Tip() != 1 {
		t.Fatalf("target tip=%d err=%v", target.Tip(), err)
	}
	if target.head.Sealed {
		t.Fatal("fork copied source seal into target")
	}
	if _, _, err := core.Propose(ctx, []byte("target continues")); err != nil {
		t.Fatal(err)
	}
	if err := target.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatalf("fork target remained sealed: %v", err)
	}
	r, err := bucket.Get(ctx, "target/fork/intent.json")
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil || bytes.Contains(data, []byte("secret")) {
		t.Fatalf("intent leaked token: %s", data)
	}
}

func TestForkRejectsMalformedStoredResult(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	// A zero-tip result is malformed even when it is syntactically valid JSON.
	if err := bucket.Upload(ctx, "target/fork/intent.json", bytes.NewReader([]byte(`{"version":1,"operation_id":"op","source_prefix":"source","target_prefix":"target","member_ids":["n1"],"manifest_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`)), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	if err := bucket.Upload(ctx, "target/fork/result.json", bytes.NewReader([]byte(`{"tip":0,"prefix_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","manifest_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`)), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	if _, err := Fork(ctx, bucket, ForkOptions{SourcePrefix: "source", TargetPrefix: "target", Members: []quepaxa.Member{{ID: "n1"}}, OperationID: "op"}); err == nil {
		t.Fatal("malformed result accepted")
	}
}
