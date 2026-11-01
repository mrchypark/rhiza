package node

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type managedPeerReadinessChecker struct {
	deny                 atomic.Bool
	runtime              *Node
	learnedWhileNotReady atomic.Bool
}

func (c *managedPeerReadinessChecker) ValidateManagedStartup(context.Context, ManagedStartupIdentity) error {
	return nil
}

func (c *managedPeerReadinessChecker) ValidateManagedAdmission(context.Context, ManagedStartupIdentity, ManagedRuntimeState) error {
	return errors.New("application admission intentionally unavailable")
}

func (c *managedPeerReadinessChecker) ValidateManagedPeerLearn(context.Context, ManagedStartupIdentity) error {
	if c.deny.Load() {
		return errors.New("live peer authority unavailable")
	}
	if c.runtime != nil {
		if c.runtime.ready.Load() {
			return errors.New("test did not close client readiness before peer-learning gate")
		}
		c.learnedWhileNotReady.Store(true)
	}
	return nil
}

func TestManagedPeerLearnReadinessIndependentOfClientCatchUp(t *testing.T) {
	checker := &managedPeerReadinessChecker{}
	n := New(&types.ExecutionConfig{NodeID: "managed", ClusterID: "cluster", DataDir: t.TempDir(), PeerAddr: "127.0.0.1:0"})
	n.managedStartup = &ManagedStartupConfig{Checker: checker}
	if err := n.open(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := n.Shutdown(); err != nil {
			t.Error(err)
		}
	})
	if n.core == nil || n.wal == nil || n.material == nil || n.peerControl == nil {
		t.Fatal("test did not open a real managed peer runtime")
	}

	// Model the verified post-startup state, then clear only client/catch-up
	// readiness as a compacted peer does. No quorum or SQL admission is granted.
	n.managedPeerVerified.Store(true)
	n.ready.Store(false)
	if !n.managedLearnReady() {
		t.Fatal("verified peer with live authority could not learn while client catch-up is pending")
	}
	if n.Ready() {
		t.Fatal("peer-learning readiness opened client/SQL readiness")
	}

	checker.deny.Store(true)
	if n.managedLearnReady() {
		t.Fatal("peer learning bypassed unavailable live authority")
	}
	checker.deny.Store(false)
	n.managedPeerVerified.Store(false)
	if n.managedLearnReady() {
		t.Fatal("peer learning bypassed managed runtime verification")
	}
	n.managedPeerVerified.Store(true)
	n.opened.Store(false)
	if n.managedLearnReady() {
		t.Fatal("peer learning bypassed opened-runtime requirement")
	}
}

func TestManagedQUICLearnAcceptsCertificateWhileClientCatchUpPending(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	peerAddr := func() string {
		t.Helper()
		conn, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := conn.LocalAddr().String()
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
		return addr
	}
	targetAddr, sourceAddr := peerAddr(), peerAddr()
	clusterID := types.ClusterID("managed-peer-readiness")
	members := []types.NodeConfig{
		peerMember(string(clusterID), "target", "target-peer-token"),
		peerMember(string(clusterID), "source", "source-peer-token"),
	}
	members[0].PeerURL = "quic://" + targetAddr
	members[1].PeerURL = "quic://" + sourceAddr

	var targetChecker *managedPeerReadinessChecker
	makeNode := func(id, token, addr string, managed bool) *Node {
		t.Helper()
		config := &types.ExecutionConfig{NodeID: types.NodeID(id), ClusterID: clusterID, DataDir: t.TempDir(), PeerAddr: addr, PeerToken: token, Members: members}
		n := New(config)
		var checker *managedPeerReadinessChecker
		if managed {
			checker = &managedPeerReadinessChecker{runtime: n}
			targetChecker = checker
			n.managedStartup = &ManagedStartupConfig{Checker: checker}
		}
		cleanup := func() {
			if err := n.Shutdown(); err != nil {
				t.Errorf("shutdown %s test node: %v", id, err)
			}
		}
		wal, err := qlog.Open(filepath.Join(config.DataDir, "qlog"))
		if err != nil {
			t.Fatal(err)
		}
		n.wal = wal
		transport := network.NewTransport(clusterID, quepaxa.NodeID(id), &quepaxa.Cluster{ConfigID: 1, Members: members}, token)
		n.transport = transport
		core, err := quepaxa.New(quepaxa.Config{NodeID: quepaxa.NodeID(id), Cluster: quepaxa.Cluster{ConfigID: 1, Members: members}, WAL: wal, Transport: transport})
		if err != nil {
			cleanup()
			t.Fatal(err)
		}
		n.core = core
		material, err := materializer.Open(filepath.Join(config.DataDir, "sqlite.db"), 1)
		if err != nil {
			cleanup()
			t.Fatal(err)
		}
		n.material = material
		server := network.NewServer(core, material, clusterID, false, transport, n.ready.Load)
		n.peerControl = server
		if managed {
			// Installed before listener publication; the test wrapper closes only
			// client readiness immediately before invoking the production gate.
			server.SetLearnReady(func() bool {
				n.ready.Store(false)
				return n.managedLearnReady()
			})
		} else {
			n.server = server
		}
		n.opened.Store(true)
		if managed {
			n.managedPeerVerified.Store(true)
		}
		peer, err := network.StartPeerServer(ctx, addr, server, members, token, "")
		if err != nil {
			cleanup()
			t.Fatal(err)
		}
		n.peer = peer
		t.Cleanup(cleanup)
		return n
	}
	target := makeNode("target", "target-peer-token", targetAddr, true)
	source := makeNode("source", "source-peer-token", sourceAddr, false)

	value, err := types.EncodeKVCommand(types.KVCommand{RequestID: "learn-while-catching-up", Operation: "put", Key: "key", Value: []byte("value")})
	if err != nil {
		t.Fatal(err)
	}
	slot, _, err := source.core.Propose(ctx, value)
	if err != nil {
		t.Fatalf("real two-voter proposal/QUIC Learned failed: %v", err)
	}
	if !targetChecker.learnedWhileNotReady.Load() {
		t.Fatal("target did not validate live authority while client readiness was false")
	}
	if target.core.Tip() < slot || target.material.Tip() < uint64(slot) {
		t.Fatalf("target did not learn/apply certified slot %d: core=%d material=%d", slot, target.core.Tip(), target.material.Tip())
	}
	if target.Ready() {
		t.Fatal("managed peer learning opened client/SQL readiness")
	}
}
