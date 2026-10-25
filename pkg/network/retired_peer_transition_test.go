package network

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/quic-go/quic-go"
	"github.com/thanos-io/objstore"
)

func TestRetiredPeerTransitionRejectsCachedIdentityAndAdmitsNewIdentity(t *testing.T) {
	const clusterID types.ClusterID = "retired-peer-transition"
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	ids := []quepaxa.NodeID{"a", "b", "c"}
	tokens := map[quepaxa.NodeID]string{"a": "token-a", "b": "token-b", "c": "token-c"}
	config := quepaxa.Cluster{ConfigID: 1}
	packets := make([]net.PacketConn, 0, len(ids))
	listeners := make([]*quic.Transport, 0, len(ids))
	for _, id := range ids {
		packet, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		packets = append(packets, packet)
		t.Cleanup(func() { _ = packet.Close() })
		listeners = append(listeners, &quic.Transport{Conn: packet})
		member := testMember(clusterID, id, tokens[id])
		member.PeerURL = "quic://" + packet.LocalAddr().String()
		config.Members = append(config.Members, member)
	}
	for _, listener := range listeners {
		t.Cleanup(func() { _ = listener.Close() })
	}

	cores := make(map[quepaxa.NodeID]*quepaxa.Core, len(ids))
	servers := make(map[quepaxa.NodeID]*Server, len(ids))
	transports := make(map[quepaxa.NodeID]*Transport, len(ids))
	peerServers := make(map[quepaxa.NodeID]*PeerServer, len(ids)+1)
	var dCore *quepaxa.Core
	var learnerTransport *Transport
	for _, member := range config.Members {
		transport := NewTransport(clusterID, member.ID, &config, tokens[member.ID])
		transports[member.ID] = transport
		t.Cleanup(func() { _ = transport.Close() })
		wal, err := qlog.Open(t.TempDir() + "/qlog")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		core, err := quepaxa.New(quepaxa.Config{
			NodeID: member.ID, Cluster: config, WAL: wal, Transport: transport,
			EnableReconfiguration: true,
			ReconfigurationAdmission: func(admissionCtx context.Context, admission quepaxa.ReconfigurationAdmission) error {
				if admission.Kind != quepaxa.ReconfigurationAdmissionLearner {
					return nil
				}
				if dCore == nil || learnerTransport == nil {
					return errors.New("learner admission ran before the test learner was ready")
				}
				page, err := learnerTransport.FetchDecisions(admissionCtx, "a", dCore.Tip()+1, 128)
				if err != nil {
					return fmt.Errorf("sync learner through admission prefix %d: %w", admission.Through, err)
				}
				if page.Tip < admission.Through {
					return fmt.Errorf("learner source tip %d is below admission prefix %d", page.Tip, admission.Through)
				}
				if len(page.Decisions) > 0 {
					if err := dCore.AcceptCertifiedValues(page.Decisions); err != nil {
						return fmt.Errorf("apply certified learner prefix through %d: %w", admission.Through, err)
					}
				}
				if dCore.Tip() < admission.Through {
					return fmt.Errorf("learner durable tip %d is below admission prefix %d", dCore.Tip(), admission.Through)
				}
				durablePrefix, err := dCore.DurablePrefix(admission.Through)
				if err != nil {
					return fmt.Errorf("read learner durable prefix through %d: %w", admission.Through, err)
				}
				if durablePrefix != admission.PrefixHash {
					return fmt.Errorf("learner durable prefix mismatch through %d: got %x, want %x", admission.Through, durablePrefix, admission.PrefixHash)
				}
				return nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		transport.BindCore(core)
		cores[member.ID] = core
		material, err := materializer.Open(filepath.Join(t.TempDir(), string(member.ID)+".sqlite"), 1)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = material.Close() })
		servers[member.ID] = NewServer(core, material, clusterID, true, transport)
		t.Cleanup(servers[member.ID].Close)
	}
	for i, member := range config.Members {
		peer, err := StartPeerServerOnTransport(ctx, listeners[i], servers[member.ID], config.Members, tokens[member.ID], "admin-token")
		if err != nil {
			t.Fatal(err)
		}
		peerServers[member.ID] = peer
		t.Cleanup(func() { _ = peer.Close() })
	}

	// C establishes and keeps a real QUIC/TLS connection to A under the old
	// committed membership before its removal.
	oldClient := NewTransport(clusterID, "c", &config, tokens["c"])
	t.Cleanup(func() { _ = oldClient.Close() })
	if _, err := oldClient.ReadTip(ctx, "a"); err != nil {
		t.Fatalf("old voter could not establish its connection: %v", err)
	}
	oldConn := cachedPeerConnection(oldClient, "a")
	if oldConn == nil || !oldConn.ConnectionState().TLS.HandshakeComplete {
		t.Fatal("old voter connection was not established")
	}

	withoutC := quepaxa.Cluster{ConfigID: 2, Members: append([]quepaxa.Member(nil), config.Members[:2]...)}
	removeFreeze, err := cores["a"].BeginReconfiguration(ctx, withoutC)
	if err != nil {
		t.Fatalf("begin REMOVE: %v", err)
	}
	removeEvents := make(chan string, 32)
	removeHook := installRetiredPeerTestHook(func(event string) {
		if !strings.Contains(event, "network:terminal-decision:") {
			return
		}
		select {
		case removeEvents <- event:
		default:
		}
	})
	removeErr := cores["a"].FinishReconfigurationAt(ctx, withoutC, 0)
	removeHook()
	if removeErr != nil {
		t.Fatalf("commit durable REMOVE: %v; sender=a operation=OperationLearned target_peers=[b,c] freeze_slot=%d terminal_events=%v peer_states=%s",
			removeErr, removeFreeze, drainRetiredPeerEvents(removeEvents), retiredPeerStateSnapshot(cores, []quepaxa.NodeID{"a", "b", "c"}, removeFreeze))
	}
	if got := cores["a"].CurrentCluster(); got.ConfigID != withoutC.ConfigID || !sameNetworkMembers(got, withoutC) {
		t.Fatalf("REMOVE did not activate at receiver A: %+v", got)
	}

	// A delayed voter record names a slot certified by the historical config.
	// It must still fail on the already-open connection because A also checks
	// the active configuration for every RPC.
	delayed := &peerfb.RequestT{
		Operation: peerfb.OperationRecord, ClusterId: string(clusterID), SenderId: "c", ConfigId: 1,
		Record: &peerfb.RecordRequestT{Slot: 1, Step: 4, Proposal: &peerfb.ProposalT{Priority: make([]byte, 32), ProposerId: "c", Hash: make([]byte, 32)}},
	}
	if _, err := oldClient.call(ctx, "a", delayed); err == nil || !errors.Is(err, errPeerRejected) || !strings.Contains(err.Error(), "authentication failed") {
		t.Fatalf("cached retired identity RPC error=%v, want peer authentication rejection", err)
	}
	if got := cachedPeerConnection(oldClient, "a"); got != oldConn || !oldConn.ConnectionState().TLS.HandshakeComplete {
		t.Fatal("test did not reuse the original live QUIC/TLS connection")
	}

	// The same logical ID cannot be resurrected, even with a new member record.
	reuseC := quepaxa.Cluster{ConfigID: 3, Members: append([]quepaxa.Member(nil), withoutC.Members...)}
	retiredC := config.Members[2]
	retiredC.PublicKey = quepaxa.PublicKey(PeerPublicKey(clusterID, "c", "token-c-v2"))
	retiredC.WALIdentity = strings.Repeat("c", 64)
	reuseC.Members = append(reuseC.Members, retiredC)
	if err := cores["a"].ValidateReconfigurationTarget(reuseC); err == nil || !strings.Contains(err.Error(), "retired") {
		t.Fatalf("re-adding retired node ID error=%v, want retired-ID rejection", err)
	}

	// D is a distinct logical identity. Before the committed ADD, its token is
	// not an admitted voter credential at A.
	dToken := "token-d"
	dMember := testMember(clusterID, "d", dToken)
	dPacket, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dPacket.Close() })
	dListener := &quic.Transport{Conn: dPacket}
	t.Cleanup(func() { _ = dListener.Close() })
	dMember.PeerURL = "quic://" + dPacket.LocalAddr().String()
	peerIdentities := make([]PeerIdentity, 0, len(config.Members))
	for _, member := range config.Members {
		identity, err := NewPeerIdentity(clusterID, member)
		if err != nil {
			t.Fatal(err)
		}
		peerIdentities = append(peerIdentities, identity)
	}
	learnerTransport = NewLearnerTransport(clusterID, "d", config.ConfigID, peerIdentities, "admin-token")
	t.Cleanup(func() { _ = learnerTransport.Close() })
	dWal, err := qlog.Open(t.TempDir() + "/qlog")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dWal.Close() })
	dCore, err = quepaxa.NewLearner(quepaxa.Config{NodeID: "d", Cluster: config, WAL: dWal, Transport: learnerTransport, EnableReconfiguration: true})
	if err != nil {
		t.Fatal(err)
	}
	learnerTransport.BindCore(dCore)
	dMember.WALIdentity = dCore.WALIdentity()
	dMaterial, err := materializer.Open(filepath.Join(t.TempDir(), "d.sqlite"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dMaterial.Close() })
	dServer := NewServer(dCore, dMaterial, clusterID, true, learnerTransport)
	t.Cleanup(dServer.Close)
	dPeer, err := StartPeerServerOnTransport(ctx, dListener, dServer, append(append([]quepaxa.Member(nil), config.Members...), dMember), dToken, "admin-token")
	if err != nil {
		t.Fatal(err)
	}
	peerServers["d"] = dPeer
	t.Cleanup(func() { _ = dPeer.Close() })
	dBeforeAdd := NewTransport(clusterID, "d", &withoutC, dToken)
	t.Cleanup(func() { _ = dBeforeAdd.Close() })
	if _, err := dBeforeAdd.ReadTip(ctx, "a"); err == nil || !errors.Is(err, errPeerRejected) {
		t.Fatalf("new identity before committed ADD error=%v, want rejection", err)
	}
	// A stale ConfigID cannot use peer Sync to learn a retired transition.
	// The replacement obtains the exact committed REMOVE from the verified
	// archive path, then its bound transport can Sync at the resulting current
	// configuration.
	if _, err := learnerTransport.FetchDecisions(ctx, "a", dCore.Tip()+1, 128); err == nil || !strings.Contains(err.Error(), "cluster identity mismatch") {
		t.Fatalf("stale-config learner Sync error=%v, want rejection", err)
	}
	if dCore.Tip() != 0 || dCore.CurrentCluster().ConfigID != config.ConfigID {
		t.Fatalf("rejected stale Sync advanced learner: tip=%d config=%d", dCore.Tip(), dCore.CurrentCluster().ConfigID)
	}
	archiveBucket := objstore.NewInMemBucket()
	publisher := recovery.NewManager(archiveBucket, string(clusterID), recovery.ArchiveConfigID)
	defer publisher.Close()
	reader := recovery.NewManager(archiveBucket, string(clusterID), recovery.ArchiveConfigID)
	defer reader.Close()
	applyCertifiedArchive := func(target *quepaxa.Core) {
		t.Helper()
		through := cores["a"].Tip()
		if err := publisher.SyncThrough(ctx, cores["a"], through); err != nil {
			t.Fatalf("publish certified membership history through %d: %v", through, err)
		}
		if err := reader.Load(ctx); err != nil {
			t.Fatalf("load certified membership history: %v", err)
		}
		values, tip, err := reader.DecisionsFrom(ctx, target.Tip()+1, 128)
		if err != nil || tip < through {
			t.Fatalf("read certified membership suffix: values=%d tip=%d want>=%d err=%v", len(values), tip, through, err)
		}
		if len(values) > 0 {
			if err := target.AcceptCertifiedValues(values); err != nil {
				t.Fatalf("apply certified membership suffix: %v", err)
			}
			if err := target.EnsureDurableThrough(ctx, through); err != nil {
				t.Fatalf("persist certified membership suffix through %d: %v", through, err)
			}
		}
	}
	applyCertifiedArchive(cores["b"])
	if got := cores["b"].CurrentCluster(); got.ConfigID != withoutC.ConfigID || !sameNetworkMembers(got, withoutC) || !cores["b"].IsVoter() {
		t.Fatalf("remaining voter did not apply archived REMOVE: voter=%t cluster=%+v", cores["b"].IsVoter(), got)
	}
	applyCertifiedArchive(dCore)
	if got := dCore.CurrentCluster(); got.ConfigID != withoutC.ConfigID || !sameNetworkMembers(got, withoutC) || dCore.IsVoter() {
		t.Fatalf("replacement learner did not apply archived REMOVE and remain non-voting: voter=%t cluster=%+v", dCore.IsVoter(), got)
	}
	removeSyncFrom := dCore.Tip() + 1
	currentSync, err := learnerTransport.FetchDecisions(ctx, "a", removeSyncFrom, 128)
	if err != nil || currentSync.ConfigID != withoutC.ConfigID || len(currentSync.Decisions) != 0 {
		t.Fatalf("bound current-config Sync after archived REMOVE: sender=d operation=OperationSync target=a from=%d response config=%d decisions=%d err=%v peer_states=%s",
			removeSyncFrom, currentSync.ConfigID, len(currentSync.Decisions), err,
			retiredPeerStateSnapshot(map[quepaxa.NodeID]*quepaxa.Core{"a": cores["a"], "b": cores["b"], "c": cores["c"], "d": dCore}, []quepaxa.NodeID{"a", "b", "c", "d"}, removeSyncFrom))
	}
	if got := dCore.CurrentCluster(); got.ConfigID != withoutC.ConfigID || !sameNetworkMembers(got, withoutC) || dCore.IsVoter() {
		t.Fatalf("replacement learner did not remain non-voting at post-REMOVE config: voter=%t cluster=%+v", dCore.IsVoter(), got)
	}
	if response, err := transports["b"].FetchDecisions(ctx, "a", cores["b"].Tip()+1, 128); err != nil || response.ConfigID != withoutC.ConfigID {
		t.Fatalf("remaining voter current-config Sync after REMOVE: response config=%d err=%v", response.ConfigID, err)
	}

	withD := quepaxa.Cluster{ConfigID: 3, Members: append([]quepaxa.Member(nil), withoutC.Members...)}
	withD.Members = append(withD.Members, dMember)
	if _, err := cores["a"].BeginReconfiguration(ctx, withD); err != nil {
		t.Fatalf("begin ADD: %v", err)
	}
	terminalEvents := make(chan string, 32)
	restoreHook := installRetiredPeerTestHook(func(event string) {
		if !strings.Contains(event, "network:terminal-decision:") && !strings.Contains(event, "network:learned:phase=handler-result:") {
			return
		}
		select {
		case terminalEvents <- event:
		default:
		}
	})
	if err := cores["a"].FinishReconfigurationAt(ctx, withD, 0); err != nil {
		restoreHook()
		slot := cores["a"].Tip()
		requestConfig := cores["a"].ClusterForSlot(slot)
		request := &peerfb.RequestT{
			Operation: peerfb.OperationLearned, ClusterId: string(clusterID), SenderId: "a",
			ConfigId: uint64(requestConfig.ConfigID), Decision: &peerfb.DecisionT{Slot: uint64(slot)},
		}
		var peerDiagnostics []string
		for _, id := range []quepaxa.NodeID{"b", "d"} {
			peer := peerServers[id]
			if peer == nil {
				continue
			}
			received, configErr := peer.configurationFor(request)
			current := peer.server.core.CurrentCluster()
			historicalMember, historicalOK := peerMember(received, "a")
			activeMember, activeOK := peerMember(current, "a")
			key := quepaxa.PublicKey(PeerPublicKey(clusterID, "a", tokens["a"]))
			peerDiagnostics = append(peerDiagnostics, fmt.Sprintf(
				"peer=%s operation=%d request_config=%d slot=%d receiver_slot_config=%d receiver_current_config=%d config_error=%v sender_historical=%t sender_active=%t sender_key_historical=%t sender_key_active=%t",
				id, request.Operation, request.ConfigId, slot, received.ConfigID, current.ConfigID, configErr,
				historicalOK, activeOK, historicalOK && historicalMember.PublicKey == key, activeOK && activeMember.PublicKey == key,
			))
		}
		var observedEvents []string
		for {
			select {
			case event := <-terminalEvents:
				observedEvents = append(observedEvents, event)
			default:
				t.Fatalf("commit durable ADD: %v; terminal RPC diagnostics: %v; per-peer events: %v", err, peerDiagnostics, observedEvents)
			}
		}
	}
	restoreHook()
	if got := cores["a"].CurrentCluster(); got.ConfigID != withD.ConfigID || !sameNetworkMembers(got, withD) {
		t.Fatalf("ADD did not activate at receiver A: %+v", got)
	}
	applyCertifiedArchive(dCore)
	if !dCore.IsVoter() || dCore.CurrentCluster().ConfigID != withD.ConfigID {
		t.Fatalf("new logical identity did not apply committed ADD: voter=%t cluster=%+v", dCore.IsVoter(), dCore.CurrentCluster())
	}
	addSyncFrom := dCore.Tip() + 1
	currentSync, err = learnerTransport.FetchDecisions(ctx, "a", addSyncFrom, 128)
	if err != nil || currentSync.ConfigID != withD.ConfigID || len(currentSync.Decisions) != 0 {
		t.Fatalf("bound current-config Sync after archived ADD: sender=d operation=OperationSync target=a from=%d response config=%d decisions=%d err=%v peer_states=%s",
			addSyncFrom, currentSync.ConfigID, len(currentSync.Decisions), err,
			retiredPeerStateSnapshot(map[quepaxa.NodeID]*quepaxa.Core{"a": cores["a"], "b": cores["b"], "c": cores["c"], "d": dCore}, []quepaxa.NodeID{"a", "b", "c", "d"}, addSyncFrom))
	}
	dAfterAdd := NewTransport(clusterID, "d", &withD, dToken)
	t.Cleanup(func() { _ = dAfterAdd.Close() })
	if _, err := dAfterAdd.ReadTip(ctx, "a"); err != nil {
		t.Fatalf("new logical identity rejected after committed ADD: %v", err)
	}
	if _, err := oldClient.call(ctx, "a", delayed); err == nil || !errors.Is(err, errPeerRejected) || !strings.Contains(err.Error(), "authentication failed") {
		t.Fatalf("old identity RPC after new generation error=%v, want rejection", err)
	}
}

func cachedPeerConnection(transport *Transport, id quepaxa.NodeID) *quic.Conn {
	peer := transport.peers[id]
	if peer == nil {
		return nil
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	return peer.conn
}

func sameNetworkMembers(left, right quepaxa.Cluster) bool {
	if left.ConfigID != right.ConfigID || len(left.Members) != len(right.Members) {
		return false
	}
	for i := range left.Members {
		if left.Members[i] != right.Members[i] {
			return false
		}
	}
	return true
}

func drainRetiredPeerEvents(events <-chan string) []string {
	var drained []string
	for {
		select {
		case event := <-events:
			drained = append(drained, event)
		default:
			return drained
		}
	}
}

func retiredPeerStateSnapshot(cores map[quepaxa.NodeID]*quepaxa.Core, peerIDs []quepaxa.NodeID, slot quepaxa.Slot) string {
	states := make([]string, 0, len(peerIDs))
	for _, id := range peerIDs {
		core := cores[id]
		if core == nil {
			states = append(states, fmt.Sprintf("peer=%s unavailable", id))
			continue
		}
		current := core.CurrentCluster()
		hasRetiredMember := false
		for _, member := range current.Members {
			if member.ID == "c" {
				hasRetiredMember = true
				break
			}
		}
		pending, hasPending := core.PendingReconfiguration()
		pendingConfigID := uint(0)
		if hasPending {
			pendingConfigID = pending.ConfigID
		}
		states = append(states, fmt.Sprintf(
			"peer=%s tip=%d current_config=%d snapshot_slot_config=%d voter=%t pending=%t pending_config=%d c_member_active=%t",
			id, core.Tip(), current.ConfigID, core.ConfigIDForSlot(slot), core.IsVoter(), hasPending, pendingConfigID, hasRetiredMember,
		))
	}
	return strings.Join(states, "; ")
}
