package managedbootstrap

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

// This is a connected checkpoint verifier test, not successful REMOVE repair,
// Kubernetes owner validation, Host activation or a provider qualification.
func TestManagedKVRepairControlUsesVerifiedProfile(t *testing.T) {
	ctx := context.Background()
	member := quepaxa.Member{ID: "member"}
	copy(member.PublicKey[:], network.PeerPublicKey("cluster", "member", "token"))
	cluster := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{member}}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: member.ID, Cluster: cluster, WAL: wal, EnableReconfiguration: true})
	if err != nil {
		t.Fatal(err)
	}
	m, err := materializer.OpenManagedSQLKV(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	propose := func(value []byte) quepaxa.DecidedValue {
		t.Helper()
		slot, _, err := core.Propose(ctx, value)
		if err != nil {
			t.Fatal(err)
		}
		decision, ok := core.CertifiedValue(slot)
		if !ok || len(decision.Certificate) == 0 {
			t.Fatal("missing actual certified decision")
		}
		if err := m.ApplyBatch(ctx, []quepaxa.DecidedValue{decision}); err != nil {
			t.Fatal(err)
		}
		return decision
	}
	value, err := types.EncodeManagedKVCommand(types.KVCommand{RequestID: "id", Operation: "put", Key: string([]byte{0xff}), Value: []byte("kept")})
	if err != nil {
		t.Fatal(err)
	}
	propose(value)
	files, tip, cleanup, err := m.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	bucket := objstore.NewInMemBucket()
	cp, err := checkpoint.NewManagerForPolicy(bucket, "repair", "", sqlpolicy.ManagedSQLKVMaterializationPolicy, 1)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := cp.AcquirePublisherClaim(ctx, "test", 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer cp.ReleasePublisherClaim(ctx, claim)
	root, err := cp.CreateFiles(ctx, claim, []checkpoint.Source{{Role: checkpoint.RoleSQLite, Path: files[0].Path}}, tip)
	if err != nil {
		t.Fatal(err)
	}
	prefix, ok := core.PrefixHash(core.Tip())
	if !ok {
		t.Fatal("missing prefix")
	}
	next, following, err := core.CheckpointLeaderOrders(core.Tip())
	if err != nil {
		t.Fatal(err)
	}
	membership, err := core.CheckpointMembership(core.Tip())
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{ConfigID: 1, Index: core.Tip(), RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following, Membership: &membership}
	core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
		return cp.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
	})
	if err := core.PrepareCheckpoint(ctx, seal); err != nil {
		t.Fatal(err)
	}
	encoded, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	decision := propose(encoded)
	archive := recovery.NewManager(bucket, "repair", 1)
	defer archive.Close()
	if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	sealed, ok, err := core.LatestCheckpointSeal()
	if err != nil || !ok {
		t.Fatal("missing seal", err)
	}
	if err := archive.TrimThrough(ctx, sealed, decision); err != nil {
		t.Fatal(err)
	}
	view, err := recovery.OpenReadOnlyViewForPolicy(ctx, bucket, "repair", 1, cluster, 8, types.DefaultIdempotencyWindowSlots)
	if err != nil {
		t.Fatal(err)
	}
	capture := operator.V1Alpha2RepairHandoff{SourcePrefix: "repair", HeadHash: fmt.Sprintf("%x", view.HeadHash()), Survivors: cluster}
	err = VerifyRepairControl(ctx, bucket, cluster, view, capture)
	if err == nil || !strings.Contains(err.Error(), "fixed source does not prove captured REMOVE target") {
		t.Fatalf("mixed checkpoint did not reach independent REMOVE check: %v", err)
	}
}
