//go:build rhiza_local_testhooks

package node

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

func TestWaitManagedAppendOnlyControlProofPreservesReconfigurationDrain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	protocol := sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol
	clusterID := types.ClusterID("managed-control-drain-preserve")
	members := make([]quepaxa.Member, 0, 3)
	for i, id := range []quepaxa.NodeID{"a", "b", "c"} {
		member := peerMember(string(clusterID), id, string(id)+"-token")
		member.PeerURL = "quic://" + string(id) + ":9090"
		member.WALIdentity = hex.EncodeToString([]byte(strings.Repeat(fmt.Sprintf("%d", i), 32)))
		members = append(members, member)
	}
	transport := &managedTransitionTransport{cores: map[quepaxa.NodeID]*quepaxa.Core{}, wals: map[quepaxa.NodeID]*qlog.WAL{}}
	var local *Node
	for _, member := range members {
		wal, err := qlog.Open(filepath.Join(t.TempDir(), "wal"))
		if err != nil {
			t.Fatal(err)
		}
		identity, err := hex.DecodeString(member.WALIdentity)
		if err != nil {
			t.Fatal(err)
		}
		if err := wal.BindIdentity(identity); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		config := &types.ExecutionConfig{
			NodeID: member.ID, ClusterID: clusterID, Members: members, DataDir: t.TempDir(),
			ObjStorePrefix: "managed-control-drain-preserve", ObjStoreProvider: "memory", ObjStoreBucket: "test", ObjStoreRegion: "local",
			EnableReconfiguration: true,
		}
		n := New(config)
		n.managedStartup = &ManagedStartupConfig{expectedPolicy: sqlpolicy.ManagedRequestMaterializationPolicy, Identity: ManagedStartupIdentity{
			ApplicationProtocol: protocol, EvidenceFormat: protocol,
			Member: recovery.PreparedStartupIdentity{ClusterUID: string(clusterID), NodeID: string(member.ID)},
		}}
		source, memberDigest, err := n.preparedBindingDigestsForProtocol(protocol)
		if err != nil {
			t.Fatal(err)
		}
		planDigest, err := n.ManagedBootstrapPlanDigestForProtocol(source, protocol, protocol)
		if err != nil {
			t.Fatal(err)
		}
		n.managedStartup.Identity.PhysicalBinding = source
		n.managedStartup.Identity.BootstrapPlanDigest = planDigest
		n.managedStartup.Identity.Member.SourceBinding = source
		n.managedStartup.Identity.Member.ConfigDigest = memberDigest
		var walIdentity [32]byte
		copy(walIdentity[:], identity)
		core, err := quepaxa.New(quepaxa.Config{
			NodeID: member.ID, Cluster: quepaxa.Cluster{ConfigID: 1, Members: members}, WAL: wal, Transport: transport, EnableReconfiguration: true,
			ValidateValue: types.ValidateManagedKVExecutionPolicy,
			ManagedAppendOnly: &quepaxa.ManagedAppendOnlyConfig{
				Binding: quepaxa.ManagedAppendOnlyBinding{ApplicationProtocol: protocol, BootstrapPlanDigest: planDigest, MemberConfigDigest: memberDigest, WALIdentity: walIdentity},
				Verify:  n.verifyManagedAppendOnlyCandidate,
			},
			ReconfigurationAdmission: func(context.Context, quepaxa.Cluster, quepaxa.Slot, [32]byte) error { return nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		n.core, n.wal = core, wal
		transport.cores[member.ID], transport.wals[member.ID] = core, wal
		if member.ID == "a" {
			local = n
		}
	}
	n := local
	mat, err := materializer.OpenManagedSQLKVWithAppendOnlyOriginals(filepath.Join(t.TempDir(), "sqlite.db"), 1, types.DefaultIdempotencyWindowSlots)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mat.Close() })
	n.material = mat
	n.opened.Store(true)
	n.managedPeerVerified.Store(true)
	if err := n.installInternalApplicationBinding(ctx); err != nil {
		t.Fatal(err)
	}
	server, err := network.NewServerForManagedProfile(n.core, mat, clusterID, true, nil, network.ManagedPeerProfile{
		ApplicationProtocol: protocol, BootstrapPlanDigest: n.managedStartup.Identity.BootstrapPlanDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	n.server = server
	t.Cleanup(server.Close)
	n.managedInitiallyAdmitted.Store(true)

	a, b := transport.cores["a"], transport.cores["b"]
	// Record slot 1 on all voters while dropping its decision delivery to a so a enters
	// waitManagedAppendOnlyControlProof with RecorderTip > CertifiedTip and CanParticipateReadIndex == true.
	transport.mu.Lock()
	transport.dropTerminalTo, transport.dropTerminalFrom = a.NodeID(), 1
	transport.mu.Unlock()
	if _, _, err := b.Propose(ctx, types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{1})); err != nil {
		t.Fatal(err)
	}
	if a.Tip() != 0 || a.RecorderTip() != 1 || !a.CanParticipateReadIndex() {
		t.Fatalf("unexpected pre-race state on a: tip=%d recorder=%d canReadIndex=%t", a.Tip(), a.RecorderTip(), a.CanParticipateReadIndex())
	}

	var hookOnce sync.Once
	var hookErr atomic.Value
	target := quepaxa.Cluster{ConfigID: 2, Members: members[:2]}
	restore := localtesthooks.SetProposalEntry(func(value []byte) {
		if barrier, err := types.DecodeReadBarrier(value); err != nil || !barrier {
			return
		}
		hookOnce.Do(func() {
			transport.mu.Lock()
			transport.dropTerminalTo, transport.dropTerminalFrom = "", 0
			transport.mu.Unlock()
			dec, ok := b.CertifiedValue(1)
			if !ok {
				hookErr.Store(errors.New("missing certified slot 1 on b"))
				return
			}
			if err := a.AcceptCertifiedValues([]quepaxa.DecidedValue{dec}); err != nil {
				hookErr.Store(err)
				return
			}
			if _, err := b.BeginReconfiguration(ctx, target); err != nil {
				hookErr.Store(err)
				return
			}
		})
	})
	defer restore()

	_, err = n.waitManagedAppendOnlyControlProof(ctx, n.internalApplication.Load())
	if stored := hookErr.Load(); stored != nil {
		t.Fatalf("proposal entry race setup failed: %v", stored)
	}
	if !errors.Is(err, quepaxa.ErrReconfigurationDrain) || !errors.Is(err, network.ErrNotReady) {
		t.Fatalf("waitManagedAppendOnlyControlProof err=%v, want both ErrReconfigurationDrain and ErrNotReady", err)
	}
}
