package recovery

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

// Dynamic source certificates remain bound to their original bootstrap
// identities. Fork must reject them until recovery creates a target-generation
// trust anchor instead of replaying the source control lineage as the target.
func TestForkRejectsMembershipCheckpointWithoutTargetTrustAnchor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	bucket := objstore.NewInMemBucket()
	members := []quepaxa.Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}
	transport := newArchiveTestCluster(t, members)
	core := transport.cores["n1"]

	if _, err := core.BeginReconfiguration(ctx, quepaxa.Cluster{ConfigID: 2, Members: members[:2]}); err != nil {
		t.Fatal(err)
	}
	if err := core.FinishReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, []byte("after membership")); err != nil {
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
	root, err := checkpoints.CreateFiles(ctx, claim, []checkpoint.Source{{Role: checkpoint.RoleSQLite, Path: file}}, uint64(core.Tip()))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkpoints.ReleasePublisherClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	index := core.Tip()
	prefix, ok := core.PrefixHash(index)
	if !ok {
		t.Fatal("missing checkpoint prefix")
	}
	next, following, err := core.CheckpointLeaderOrders(index)
	if err != nil {
		t.Fatal(err)
	}
	history, err := core.CheckpointMembership(index)
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{ConfigID: core.ConfigID(), Index: index, RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following, Membership: &history}
	for _, id := range []quepaxa.NodeID{"n1", "n2"} {
		member := transport.cores[id]
		member.SetCheckpointValidator(func(ctx context.Context, candidate quepaxa.CheckpointSeal) error {
			return checkpoints.Verify(ctx, uint64(candidate.Index), candidate.RootHash, candidate.StateHash)
		})
		if err := member.PrepareCheckpoint(ctx, seal); err != nil {
			t.Fatal(err)
		}
	}
	encoded, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, encoded); err != nil {
		t.Fatal(err)
	}

	archive := NewManager(bucket, "source", 1)
	defer archive.Close()
	if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	sealed, ok, err := core.LatestCheckpointSeal()
	if err != nil || !ok {
		t.Fatalf("seal=%v err=%v", ok, err)
	}
	decision, ok := core.CertifiedValue(sealed.DecisionSlot)
	if !ok {
		t.Fatal("missing checkpoint decision")
	}
	if err := archive.TrimThrough(ctx, sealed, decision); err != nil {
		t.Fatal(err)
	}
	if err := Seal(ctx, bucket, "source", "op"); err != nil {
		t.Fatal(err)
	}

	if _, err := Fork(ctx, bucket, ForkOptions{SourcePrefix: "source", TargetPrefix: "target", Members: members, OperationID: "op"}); err == nil {
		t.Fatal("fork accepted source membership lineage without target trust anchor")
	} else if !strings.Contains(err.Error(), "fixed-membership checkpoint includes membership history") {
		t.Fatalf("fork rejected membership source for the wrong reason: %v", err)
	}
}
