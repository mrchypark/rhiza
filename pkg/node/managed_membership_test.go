package node

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

type membershipAuthorityChecker struct {
	calls        int
	reject       bool
	rejectFrozen bool
	n            *Node
}

// This isolates the private dispatch/completion boundary using a genuine
// registered learner Core/WAL. The Host test separately exercises real QUIC
// learner verification and the complete REMOVE-first handoff authority path.
func TestManagedMembershipAddTerminalRetryRequiresDelivery(t *testing.T) {
	ctx := context.Background()
	var learner *quepaxa.Core
	var nonce string
	n, bucket, transport, identities := terminalRetryFixture(t, func(_ context.Context, _ quepaxa.Cluster, through quepaxa.Slot, prefix [32]byte) error {
		if learner == nil || learner.WALIdentity() != nonce {
			return errors.New("actual learner unavailable")
		}
		actual, err := learner.DurablePrefix(through)
		if err != nil || actual != prefix {
			return errors.New("actual learner durable prefix differs")
		}
		return nil
	})
	member := peerMember("cluster", "fresh", "fresh-token")
	member.PeerURL = "https://fresh"
	config := &types.ExecutionConfig{DataDir: t.TempDir(), ClusterID: "cluster", NodeID: member.ID, Members: n.config.Members[:2], Learner: &member, ObjStoreProvider: "filesystem", ObjStoreDir: "unused"}
	state, err := loadVoterIdentity(config)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := qlog.Open(filepath.Join(config.DataDir, "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	if err := ensureVoterIdentity(ctx, config, bucket, wal, state, false); err != nil {
		t.Fatal(err)
	}
	state, err = loadVoterIdentity(config)
	if err != nil || state.identity == nil {
		t.Fatal("actual learner registration missing")
	}
	nonce = state.identity.Nonce
	member.WALIdentity = nonce
	learner, err = quepaxa.NewLearner(quepaxa.Config{NodeID: member.ID, Cluster: n.core.CurrentCluster(), WAL: wal, Transport: transport, EnableReconfiguration: true})
	if err != nil {
		t.Fatal(err)
	}
	transport.cores[member.ID] = learner
	transport.ids = append(transport.ids, member.ID)
	n.config.ObjStoreProvider, n.config.ObjStoreDir = "filesystem", "unused"
	source, _, err := n.preparedBindingDigests()
	if err != nil {
		t.Fatal(err)
	}
	n.managedStartup = &ManagedStartupConfig{Checker: &membershipAuthorityChecker{}, Identity: ManagedStartupIdentity{PhysicalBinding: source, Member: recovery.PreparedStartupIdentity{NodeID: "a", SourceBinding: source, ClusterUID: "uid", DeploymentUID: "deployment"}}}
	n.opened.Store(true)
	n.server = network.NewServer(n.core, nil, "cluster", true, nil)
	n.server.SetMembershipChange(n.changeMembership)
	t.Cleanup(n.server.Close)
	r := ManagedMembershipRequest{RepairOperationID: "add-response-loss", Action: ManagedMembershipAdd, ExpectedConfigID: 1, OldNodeID: "c", OldWALIdentity: identities["c"], Addition: &member, HandoffHeadHash: strings.Repeat("a", 64)}
	transport.fail.Store(true)
	if err := n.ChangeManagedMembership(ctx, r); err == nil || n.core.ConfigID() != 2 || learner.IsVoter() {
		t.Fatalf("actual terminal-loss setup: %v", err)
	}
	tip := n.core.Tip()
	terminal, ok := n.core.CertifiedValue(tip)
	if !ok {
		t.Fatal("locally certified terminal missing")
	}
	attempts := transport.attempts.Load()
	if err := n.ChangeManagedMembership(ctx, r); err == nil || transport.attempts.Load() <= attempts || learner.IsVoter() {
		t.Fatalf("retry falsely succeeded without terminal delivery: %v", err)
	}
	transport.fail.Store(false)
	if err := n.ChangeManagedMembership(ctx, r); err != nil {
		t.Fatalf("retry restored actual terminal delivery: %v", err)
	}
	after, ok := learner.CertifiedValue(tip)
	if !ok || !learner.IsVoter() || n.core.Tip() != tip || after.Hash != terminal.Hash || !bytes.Equal(after.Certificate, terminal.Certificate) {
		t.Fatal("retry did not complete exact retained terminal at actual learner")
	}
}

func (*membershipAuthorityChecker) ValidateManagedStartup(context.Context, ManagedStartupIdentity) error {
	return nil
}
func (*membershipAuthorityChecker) ValidateManagedAdmission(context.Context, ManagedStartupIdentity, ManagedRuntimeState) error {
	return nil
}
func (c *membershipAuthorityChecker) ValidateManagedMembership(_ context.Context, _ ManagedStartupIdentity, r ManagedMembershipRequest, current quepaxa.Cluster, _ quepaxa.Slot) error {
	c.calls++
	if c.rejectFrozen {
		if _, ok := c.n.core.PendingReconfiguration(); ok {
			return network.ErrNotReady
		}
	}
	if c.reject || r.ExpectedConfigID != 1 || (current.ConfigID != 1 && current.ConfigID != 2) {
		return network.ErrNotReady
	}
	return nil
}

func TestManagedMembershipPrivateRemoveAndRetry(t *testing.T) {
	ctx := context.Background()
	n, identities, bucket, transport := newMembershipOperationNode(t)
	source, _, err := n.preparedBindingDigests()
	if err != nil {
		t.Fatal(err)
	}
	checker := &membershipAuthorityChecker{}
	n.managedStartup = &ManagedStartupConfig{Checker: checker, Identity: ManagedStartupIdentity{PhysicalBinding: source, Member: recovery.PreparedStartupIdentity{NodeID: "a", SourceBinding: source, ClusterUID: "uid", DeploymentUID: "deployment"}}}
	n.opened.Store(true)
	n.server = network.NewServer(n.core, nil, "cluster", true, nil)
	n.server.SetMembershipChange(n.changeMembership)
	n.server.SetMembershipAbort(n.abortMembership)
	t.Cleanup(n.server.Close)
	r := ManagedMembershipRequest{RepairOperationID: "repair", Action: ManagedMembershipRemove, ExpectedConfigID: 1, OldNodeID: "c", OldWALIdentity: identities["c"]}
	if err := n.server.ChangeMembership(ctx, r.change("cluster")); !errors.Is(err, network.ErrNotReady) {
		t.Fatalf("raw call: %v", err)
	}
	forged := r.change("cluster")
	forged.Fence = &network.MembershipFence{NodeID: "c", WALIdentity: identities["c"], WorkloadUID: "old", Confirmed: true, Evidence: "not-managed-authority"}
	if err := n.server.ChangeMembership(ctx, forged); !errors.Is(err, network.ErrNotReady) {
		t.Fatalf("legacy fence bypassed managed checker: %v", err)
	}
	n.managedStartup.Checker = nil
	if err := n.ChangeManagedMembership(ctx, r); !errors.Is(err, network.ErrNotReady) {
		t.Fatalf("missing checker: %v", err)
	}
	n.managedStartup.Checker = checker
	assertNoMembershipOperation(t, ctx, n, bucket, 1)
	checker.reject = true
	if err := n.ChangeManagedMembership(ctx, r); !errors.Is(err, network.ErrNotReady) {
		t.Fatalf("closed authority rejection: %v", err)
	}
	checker.reject = false
	wrong := r
	wrong.OldWALIdentity = "not-registered"
	if err := n.ChangeManagedMembership(ctx, wrong); err == nil {
		t.Fatal("wrong registered WAL accepted")
	}
	assertNoMembershipOperation(t, ctx, n, bucket, 1)
	transport.disable("c")
	checker.n, checker.rejectFrozen = n, true
	if err := n.ChangeManagedMembership(ctx, r); !errors.Is(err, network.ErrNotReady) {
		t.Fatalf("injected post-freeze rejection: %v", err)
	}
	if _, ok := n.core.PendingReconfiguration(); !ok {
		t.Fatal("real durable freeze missing")
	}
	checker.rejectFrozen = false
	other := r
	other.RepairOperationID = "other"
	if err := n.ChangeManagedMembership(ctx, other); err == nil {
		t.Fatal("different operation resumed freeze")
	}
	wrongTarget := r
	wrongTarget.OldNodeID, wrongTarget.OldWALIdentity = "b", identities["b"]
	if err := n.ChangeManagedMembership(ctx, wrongTarget); !errors.Is(err, network.ErrRequestConflict) {
		t.Fatalf("different target resumed freeze: %v", err)
	}
	if err := n.ChangeManagedMembership(ctx, r); err != nil {
		t.Fatal(err)
	}
	if got := n.core.CurrentCluster(); got.ConfigID != 2 || len(got.Members) != 2 {
		t.Fatalf("REMOVE result: %+v", got)
	}
	for _, oldID := range []quepaxa.NodeID{"b", "c"} {
		neighbor := r
		neighbor.RepairOperationID = "neighbor"
		neighbor.OldNodeID = oldID
		neighbor.OldWALIdentity = identities[oldID]
		change := neighbor.change("cluster")
		isolated := context.WithValue(ctx, managedMembershipPermitKey{}, managedMembershipPermit{request: neighbor, change: change})
		// Genuine C-removal history is not authority for another operation or
		// B-removal. Exercise the capture boundary independently of dispatch.
		if _, err := n.captureManagedRepairSource(isolated, neighbor); err == nil {
			t.Fatalf("captured neighboring operation/REMOVE target %s", oldID)
		}
	}
	if transport.cores["c"].CurrentCluster().ConfigID != 1 {
		t.Fatal("test must not imply physical old-process fencing")
	}
	tip := n.core.Tip()
	if err := n.ChangeManagedMembership(ctx, r); err != nil {
		t.Fatalf("terminal retry: %v", err)
	}
	if n.core.Tip() != tip || checker.calls < 4 {
		t.Fatalf("retry tip/checks: %d/%d", n.core.Tip(), checker.calls)
	}
	assertDurableArchivedMembership(t, ctx, n, bucket, tip)
}
