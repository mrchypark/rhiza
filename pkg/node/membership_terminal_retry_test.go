package node

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type terminalLossTransport struct {
	*membershipOperationTransport
	fail     atomic.Bool
	attempts atomic.Int64
}

func (t *terminalLossTransport) SendDecision(ctx context.Context, d quepaxa.Decision) error {
	if t.cores["a"].CurrentCluster().ConfigID == 2 {
		t.attempts.Add(1)
		if t.fail.Load() {
			return errors.New("terminal delivery lost")
		}
	}
	return t.membershipOperationTransport.SendDecision(ctx, d)
}

func terminalRetryFixture(t *testing.T, admission func(context.Context, quepaxa.Cluster, quepaxa.Slot, [32]byte) error) (*Node, *objectstore.MeteredBucket, *terminalLossTransport, map[quepaxa.NodeID]string) {
	t.Helper()
	ctx := context.Background()
	bucket, err := objectstore.NewBucket(objectstore.Config{Provider: objectstore.ProviderFilesystem, FilesystemDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bucket.Close() })
	members := []quepaxa.Member{peerMember("cluster", "a", "a-token"), peerMember("cluster", "b", "b-token"), peerMember("cluster", "c", "c-token")}
	wals := map[quepaxa.NodeID]*qlog.WAL{}
	ids := map[quepaxa.NodeID]string{}
	for _, member := range members {
		cfg := &types.ExecutionConfig{DataDir: t.TempDir(), ClusterID: "cluster", NodeID: member.ID, Members: members[:2]}
		if member.ID == "c" {
			cfg.Learner = &quepaxa.Member{ID: "c", PublicKey: member.PublicKey}
		}
		state, e := loadVoterIdentity(cfg)
		if e != nil {
			t.Fatal(e)
		}
		wal, e := qlog.Open(filepath.Join(cfg.DataDir, "qlog"))
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { _ = wal.Close() })
		if e = ensureVoterIdentity(ctx, cfg, bucket, wal, state, false); e != nil {
			t.Fatal(e)
		}
		state, e = loadVoterIdentity(cfg)
		if e != nil || state.identity == nil {
			t.Fatal(e)
		}
		wals[member.ID] = wal
		ids[member.ID] = state.identity.Nonce
	}
	for i := range members {
		members[i].WALIdentity = ids[members[i].ID]
	}
	base := &membershipOperationTransport{cores: map[quepaxa.NodeID]*quepaxa.Core{}, disabled: map[quepaxa.NodeID]bool{}, ids: []quepaxa.NodeID{"a", "b", "c"}}
	tr := &terminalLossTransport{membershipOperationTransport: base}
	for _, m := range members {
		core, e := quepaxa.New(quepaxa.Config{NodeID: m.ID, Cluster: quepaxa.Cluster{ConfigID: 1, Members: members}, WAL: wals[m.ID], Transport: tr, EnableReconfiguration: true, ReconfigurationAdmission: admission})
		if e != nil {
			t.Fatal(e)
		}
		base.cores[m.ID] = core
	}
	n := &Node{config: &types.ExecutionConfig{NodeID: "a", ClusterID: "cluster", Members: members, EnableReconfiguration: true}, core: base.cores["a"], bucket: bucket}
	return n, bucket, tr, ids
}

func TestMembershipTerminalRetryRequiresDelivery(t *testing.T) {
	ctx := context.Background()
	n, _, tr, ids := terminalRetryFixture(t, nil)
	tr.fail.Store(true)
	request := network.MembershipChange{OperationID: "remove-c", ClusterID: "cluster", ExpectedConfigID: 1, Remove: "c", Fence: &network.MembershipFence{NodeID: "c", WALIdentity: ids["c"], WorkloadUID: "w", Confirmed: true, Evidence: "test"}}
	if err := n.changeMembership(ctx, request); err == nil || n.core.ConfigID() != 2 {
		t.Fatalf("terminal-loss setup failed: %v", err)
	}
	before := tr.attempts.Load()
	if err := n.changeMembership(ctx, request); err == nil || tr.attempts.Load() <= before {
		t.Fatalf("retry falsely completed without terminal delivery: %v", err)
	}
	tr.fail.Store(false)
	if err := n.changeMembership(ctx, request); err != nil {
		t.Fatalf("retry after restored delivery: %v", err)
	}
}

func TestMembershipRetryRejectsEarlierAbortedSameTarget(t *testing.T) {
	ctx := context.Background()
	var allow atomic.Bool
	n, bucket, _, _ := terminalRetryFixture(t, func(context.Context, quepaxa.Cluster, quepaxa.Slot, [32]byte) error {
		if !allow.Load() {
			return errors.New("learner unavailable")
		}
		return nil
	})
	learner := registerMembershipLearner(t, bucket, "fresh")
	old := network.MembershipChange{OperationID: "first", ClusterID: "cluster", ExpectedConfigID: 1, Add: &learner}
	if err := n.changeMembership(ctx, old); err == nil {
		t.Fatal("first addition unexpectedly succeeded")
	}
	if err := n.abortMembership(ctx, old); err != nil {
		t.Fatal(err)
	}
	abort, _, ok := n.core.LastReconfigurationAbort()
	if !ok || abort == 0 {
		t.Fatal("missing genuine abort")
	}
	later := old
	later.OperationID = "second"
	later.ExpectedAbortSlot = abort
	allow.Store(true)
	if err := n.changeMembership(ctx, later); err != nil {
		t.Fatalf("later addition: %v", err)
	}
	if err := n.changeMembership(ctx, old); !errors.Is(err, network.ErrRequestConflict) {
		t.Fatalf("old aborted operation matched later terminal: %v", err)
	}
	if err := n.changeMembership(ctx, later); err != nil {
		t.Fatalf("exact later retry: %v", err)
	}
}
