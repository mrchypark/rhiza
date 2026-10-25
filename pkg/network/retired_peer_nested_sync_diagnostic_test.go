//go:build rhiza_local_testhooks

package network

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/quic-go/quic-go"
	"github.com/thanos-io/objstore"
)

func TestRetiredPeerNestedSyncDiagnostic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const clusterID types.ClusterID = "nested-sync-diagnostic"
	ids := []quepaxa.NodeID{"a", "b", "c"}
	tokens := map[quepaxa.NodeID]string{"a": "nested-a", "b": "nested-b", "c": "nested-c"}
	config := quepaxa.Cluster{ConfigID: 1}
	listeners := make(map[quepaxa.NodeID]*quic.Transport, len(ids))
	cores := make(map[quepaxa.NodeID]*quepaxa.Core, len(ids))
	servers := make(map[quepaxa.NodeID]*Server, len(ids))
	transports := make(map[quepaxa.NodeID]*Transport, len(ids))
	peerServers := make(map[quepaxa.NodeID]*PeerServer, len(ids))

	for _, id := range ids {
		packet, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = packet.Close() })
		listeners[id] = &quic.Transport{Conn: packet}
		t.Cleanup(func() { _ = listeners[id].Close() })
		member := testMember(clusterID, id, tokens[id])
		member.PeerURL = "quic://" + packet.LocalAddr().String()
		config.Members = append(config.Members, member)
	}

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
		})
		if err != nil {
			t.Fatal(err)
		}
		transport.BindCore(core)
		cores[member.ID] = core
		material, err := materializer.Open(t.TempDir()+"/materializer.sqlite", 1)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = material.Close() })
		servers[member.ID] = NewServer(core, material, clusterID, true, transport)
		t.Cleanup(servers[member.ID].Close)
	}
	for _, member := range config.Members {
		peer, err := StartPeerServerOnTransport(ctx, listeners[member.ID], servers[member.ID], config.Members, tokens[member.ID], "admin")
		if err != nil {
			t.Fatal(err)
		}
		peerServers[member.ID] = peer
		t.Cleanup(func() { _ = peer.Close() })
	}

	events := make(chan string, 32)
	catchupTip := make(chan quepaxa.Slot, 1)
	var captureNested atomic.Bool
	restore := localtesthooks.Set(func(event string) {
		if !captureNested.Load() {
			return
		}
		if strings.Contains(event, "network:learned-control:phase=catchup-start:node=b:sender=a:") {
			select {
			case catchupTip <- cores["b"].Tip():
			default:
			}
		}
		if strings.Contains(event, "network:learned-control:phase=catchup-start:node=b:sender=a:") ||
			strings.Contains(event, "network:learned-control:phase=catchup-failed:node=b:sender=a:") ||
			strings.Contains(event, "network:learned:phase=handler-result:node=b:sender=a:") ||
			strings.HasPrefix(event, "network:peer-request-rejected:operation=sync:") {
			select {
			case events <- event:
			default:
			}
		}
	})
	defer restore()

	next := quepaxa.Cluster{ConfigID: 2, Members: []quepaxa.Member{config.Members[0], config.Members[2]}}
	freeze, err := cores["a"].BeginReconfiguration(ctx, next)
	if err != nil {
		t.Fatalf("begin REMOVE B: %v", err)
	}
	if cores["b"].Tip() != freeze {
		t.Fatalf("B did not learn the REMOVE freeze before the injected gap: tip=%d freeze=%d", cores["b"].Tip(), freeze)
	}
	if err := cores["a"].FinishReconfigurationAt(ctx, next, 0); err != nil {
		t.Fatalf("commit REMOVE with the three-voter fixture: %v", err)
	}
	for _, id := range []quepaxa.NodeID{"a", "c"} {
		got := cores[id].CurrentCluster()
		if got.ConfigID != 2 || !sameNetworkMembers(got, next) {
			t.Fatalf("survivor %s has not applied config 2 while B is held: %+v", id, got)
		}
	}
	terminalSlot := freeze + 16
	if err := peerServers["b"].Close(); err != nil {
		t.Fatalf("stop B peer listener before restoring the stale prefix: %v", err)
	}
	// Reopen B from the real certified prefix through the REMOVE freeze. This
	// models a stale learner restart without inventing a terminal or changing
	// the production admission/authentication path.
	staleTransport := NewTransport(clusterID, "b", &config, tokens["b"])
	transports["b"] = staleTransport
	t.Cleanup(func() { _ = staleTransport.Close() })
	staleWAL, err := qlog.Open(t.TempDir() + "/stale-b-qlog")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = staleWAL.Close() })
	staleCore, err := quepaxa.New(quepaxa.Config{
		NodeID: "b", Cluster: config, WAL: staleWAL, Transport: staleTransport,
		EnableReconfiguration: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	staleTransport.BindCore(staleCore)
	prefix, _, err := cores["a"].DecisionsFrom(1, int(freeze))
	if err != nil || len(prefix) == 0 || quepaxa.Slot(len(prefix)) != freeze {
		t.Fatalf("read real certified prefix through freeze %d: decisions=%d err=%v", freeze, len(prefix), err)
	}
	if err := staleCore.AcceptCertifiedValues(prefix); err != nil {
		t.Fatalf("restore stale B from certified prefix: %v", err)
	}
	if err := staleCore.EnsureDurableThrough(ctx, freeze); err != nil {
		t.Fatalf("persist stale B certified prefix: %v", err)
	}
	staleMaterial, err := materializer.Open(t.TempDir()+"/stale-b-materializer.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = staleMaterial.Close() })
	staleServer := NewServer(staleCore, staleMaterial, clusterID, true, staleTransport)
	servers["b"], cores["b"] = staleServer, staleCore
	t.Cleanup(staleServer.Close)
	peer, err := StartPeerServerOnTransport(ctx, listeners["b"], staleServer, config.Members, tokens["b"], "admin")
	if err != nil {
		t.Fatalf("start stale B peer listener from the certified prefix: %v", err)
	}
	peerServers["b"] = peer
	t.Cleanup(func() { _ = peer.Close() })
	captureNested.Store(true)

	terminal, ok := cores["a"].CertifiedValue(terminalSlot)
	if !ok {
		t.Fatalf("survivor A lacks certified REMOVE terminal at slot %d", terminalSlot)
	}
	var decision quepaxa.Decision
	if err := json.Unmarshal(terminal.Certificate, &decision); err != nil {
		t.Fatalf("decode committed terminal certificate: %v", err)
	}
	decision.Proposal.Value = terminal.Value
	if decision.Slot != terminalSlot || decision.Proposal.Hash != terminal.Hash {
		t.Fatalf("terminal certificate does not bind committed value: slot=%d hash_match=%t", decision.Slot, decision.Proposal.Hash == terminal.Hash)
	}
	isControl, err := quepaxa.DecodeReconfiguration(terminal.Value)
	if err != nil || !isControl {
		t.Fatalf("committed terminal is not a reconfiguration control: isControl=%t err=%v", isControl, err)
	}
	request := transports["a"].request(peerfb.OperationLearned)
	request.Decision = decisionToWire(decision)
	_, outerErr := transports["a"].call(ctx, "b", request)
	if outerErr == nil || !strings.Contains(outerErr.Error(), "cluster identity mismatch") {
		t.Fatalf("stale B outer Learned result=%v, want nested Sync cluster mismatch", outerErr)
	}
	catchupStart := waitNestedSyncEvent(t, ctx, events, "network:learned-control:phase=catchup-start:node=b:sender=a:")
	select {
	case tip := <-catchupTip:
		if tip != freeze {
			t.Fatalf("nested catch-up began with B tip=%d, want frozen missing prefix at %d", tip, freeze)
		}
	case <-ctx.Done():
		t.Fatalf("missing B tip snapshot at nested catch-up start: %v", ctx.Err())
	}
	rejectedSync := waitNestedSyncEvent(t, ctx, events, "network:peer-request-rejected:operation=sync:")
	catchupFailed := waitNestedSyncEvent(t, ctx, events, "network:learned-control:phase=catchup-failed:node=b:sender=a:")
	handlerResult := waitNestedSyncEvent(t, ctx, events, "network:learned:phase=handler-result:node=b:sender=a:")
	terminalSlotText := strconv.FormatUint(uint64(terminalSlot), 10)
	for _, event := range []string{catchupStart, catchupFailed, handlerResult} {
		if !strings.Contains(event, fmt.Sprintf(":slot=%s", terminalSlotText)) {
			t.Fatalf("nested outer Learned phase does not match terminal slot %s: %s", terminalSlotText, event)
		}
	}
	for _, field := range []string{
		"request_config=1", "selected_config=2", "slot=0", "from=2",
		"lookup_ok=true", "cluster_match=true", "config_match=false", "rejection=cluster_or_config",
	} {
		if !strings.Contains(rejectedSync, field) {
			t.Fatalf("nested Sync rejection missing %q: %s", field, rejectedSync)
		}
	}
	if !strings.Contains(catchupFailed, "cluster identity mismatch") || !strings.Contains(handlerResult, "cluster identity mismatch") {
		t.Fatalf("outer Learned did not fail specifically at nested Sync: catchup=%s handler=%s", catchupFailed, handlerResult)
	}
	if strings.Contains(handlerResult, "error=\"\"") {
		t.Fatalf("outer Learned handler unexpectedly succeeded after the nested Sync rejection: %s", handlerResult)
	}
	if cores["b"].CurrentCluster().ConfigID != 1 || cores["b"].Tip() >= freeze+16 {
		t.Fatalf("diagnostic unexpectedly applied the terminal at B: config=%d tip=%d", cores["b"].CurrentCluster().ConfigID, cores["b"].Tip())
	}
}

func TestRetiredPeerArchiveRecoveryAllowsRetainedVoterCurrentSync(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const clusterID types.ClusterID = "retained-voter-archive-recovery"
	ids := []quepaxa.NodeID{"a", "b", "c"}
	tokens := map[quepaxa.NodeID]string{"a": "archive-a", "b": "archive-b", "c": "archive-c"}
	config := quepaxa.Cluster{ConfigID: 1}
	listeners := make(map[quepaxa.NodeID]*quic.Transport, len(ids))
	cores := make(map[quepaxa.NodeID]*quepaxa.Core, len(ids))
	servers := make(map[quepaxa.NodeID]*Server, len(ids))
	transports := make(map[quepaxa.NodeID]*Transport, len(ids))
	peerServers := make(map[quepaxa.NodeID]*PeerServer, len(ids))

	for _, id := range ids {
		packet, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = packet.Close() })
		listener := &quic.Transport{Conn: packet}
		listeners[id] = listener
		t.Cleanup(func() { _ = listener.Close() })
		member := testMember(clusterID, id, tokens[id])
		member.PeerURL = "quic://" + packet.LocalAddr().String()
		config.Members = append(config.Members, member)
	}

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
		})
		if err != nil {
			t.Fatalf("create voter %s: %v", member.ID, err)
		}
		transport.BindCore(core)
		cores[member.ID] = core
		material, err := materializer.Open(t.TempDir()+"/materializer.sqlite", 1)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = material.Close() })
		server := NewServer(core, material, clusterID, true, transport)
		servers[member.ID] = server
		t.Cleanup(server.Close)
	}
	for _, member := range config.Members {
		peer, err := StartPeerServerOnTransport(ctx, listeners[member.ID], servers[member.ID], config.Members, tokens[member.ID], "admin")
		if err != nil {
			t.Fatalf("start voter %s peer: %v", member.ID, err)
		}
		peerServers[member.ID] = peer
		t.Cleanup(func() { _ = peer.Close() })
	}

	// Remove C, leaving B as a voter in the certified target configuration.
	target := quepaxa.Cluster{ConfigID: 2, Members: []quepaxa.Member{config.Members[0], config.Members[1]}}
	freeze, err := cores["a"].BeginReconfiguration(ctx, target)
	if err != nil {
		t.Fatalf("begin REMOVE C: %v", err)
	}
	if cores["b"].Tip() != freeze {
		t.Fatalf("retained voter B did not learn freeze before terminal: tip=%d freeze=%d", cores["b"].Tip(), freeze)
	}
	if err := cores["a"].FinishReconfigurationAt(ctx, target, 0); err != nil {
		t.Fatalf("commit REMOVE C: %v", err)
	}
	terminalSlot := freeze + 16
	terminal, ok := cores["a"].CertifiedValue(terminalSlot)
	if !ok {
		t.Fatalf("survivor A lacks certified REMOVE terminal at fixed slot %d", terminalSlot)
	}
	control, err := quepaxa.DecodeReconfiguration(terminal.Value)
	if err != nil || !control {
		t.Fatalf("slot %d is not the certified REMOVE control: control=%t err=%v", terminalSlot, control, err)
	}
	if got := cores["a"].CurrentCluster(); got.ConfigID != target.ConfigID || !sameNetworkMembers(got, target) {
		t.Fatalf("REMOVE target was not committed at survivor A: %+v", got)
	}

	// Recreate B from its certified prefix at the freeze. This is a stale but
	// retained voter: its old-config Sync must fail until certified archive
	// recovery applies the exact suffix through the fixed terminal.
	if err := peerServers["b"].Close(); err != nil {
		t.Fatalf("stop B before restoring its stale certified prefix: %v", err)
	}
	staleTransport := NewTransport(clusterID, "b", &config, tokens["b"])
	transports["b"] = staleTransport
	t.Cleanup(func() { _ = staleTransport.Close() })
	staleWAL, err := qlog.Open(t.TempDir() + "/stale-b-qlog")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = staleWAL.Close() })
	staleCore, err := quepaxa.New(quepaxa.Config{
		NodeID: "b", Cluster: config, WAL: staleWAL, Transport: staleTransport,
		EnableReconfiguration: true,
	})
	if err != nil {
		t.Fatalf("create stale retained voter B: %v", err)
	}
	staleTransport.BindCore(staleCore)
	cores["b"] = staleCore
	prefix, _, err := cores["a"].DecisionsFrom(1, int(freeze))
	if err != nil || len(prefix) == 0 || quepaxa.Slot(len(prefix)) != freeze {
		t.Fatalf("read certified prefix through freeze %d: decisions=%d err=%v", freeze, len(prefix), err)
	}
	if err := staleCore.AcceptCertifiedValues(prefix); err != nil {
		t.Fatalf("restore stale B prefix: %v", err)
	}
	if err := staleCore.EnsureDurableThrough(ctx, freeze); err != nil {
		t.Fatalf("persist stale B prefix through freeze %d: %v", freeze, err)
	}
	staleMaterial, err := materializer.Open(t.TempDir()+"/stale-b-materializer.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = staleMaterial.Close() })
	staleServer := NewServer(staleCore, staleMaterial, clusterID, true, staleTransport)
	servers["b"] = staleServer
	t.Cleanup(staleServer.Close)
	peer, err := StartPeerServerOnTransport(ctx, listeners["b"], staleServer, config.Members, tokens["b"], "admin")
	if err != nil {
		t.Fatalf("restart stale B peer: %v", err)
	}
	peerServers["b"] = peer
	t.Cleanup(func() { _ = peer.Close() })
	if staleCore.CurrentCluster().ConfigID != 1 || staleCore.Tip() != freeze {
		t.Fatalf("stale B fixture is not exactly at the freeze: config=%d tip=%d freeze=%d", staleCore.CurrentCluster().ConfigID, staleCore.Tip(), freeze)
	}
	if _, err := staleTransport.FetchDecisions(ctx, "a", freeze+1, 1); err == nil || !strings.Contains(err.Error(), "cluster identity mismatch") {
		t.Fatalf("pre-archive stale B Sync error=%v, want cluster identity mismatch", err)
	}

	archiveBucket := objstore.NewInMemBucket()
	publisher := recovery.NewManager(archiveBucket, string(clusterID), recovery.ArchiveConfigID)
	defer publisher.Close()
	reader := recovery.NewManager(archiveBucket, string(clusterID), recovery.ArchiveConfigID)
	defer reader.Close()
	if err := publisher.SyncThrough(ctx, cores["a"], terminalSlot); err != nil {
		t.Fatalf("publish certified archive through fixed terminal %d: %v", terminalSlot, err)
	}
	if err := reader.Load(ctx); err != nil {
		t.Fatalf("load certified archive: %v", err)
	}
	suffix, archiveTip, err := reader.DecisionsFrom(ctx, freeze+1, int(terminalSlot-freeze))
	if err != nil || archiveTip < terminalSlot || len(suffix) != int(terminalSlot-freeze) || suffix[len(suffix)-1].Slot != terminalSlot {
		t.Fatalf("read certified suffix through fixed terminal %d: decisions=%d tip=%d err=%v", terminalSlot, len(suffix), archiveTip, err)
	}
	if err := staleCore.AcceptCertifiedValues(suffix); err != nil {
		t.Fatalf("apply certified archive suffix through terminal %d: %v", terminalSlot, err)
	}
	if err := staleCore.EnsureDurableThrough(ctx, terminalSlot); err != nil {
		t.Fatalf("persist certified archive suffix through terminal %d: %v", terminalSlot, err)
	}
	if staleCore.Tip() != terminalSlot {
		t.Fatalf("B durable tip=%d, want fixed terminal %d", staleCore.Tip(), terminalSlot)
	}
	if got := staleCore.CurrentCluster(); got.ConfigID != target.ConfigID || !sameNetworkMembers(got, target) || !staleCore.IsVoter() {
		t.Fatalf("certified archive did not retain B in the REMOVE target: voter=%t cluster=%+v", staleCore.IsVoter(), got)
	}

	// The same bound transport resolves B's now-current config and performs a
	// real Sync against A; no fresh config-2 transport is substituted here.
	response, err := staleTransport.FetchDecisions(ctx, "a", terminalSlot+1, 1)
	if err != nil || response.ConfigID != target.ConfigID || response.ClusterID != clusterID || response.ProposerID != "a" || response.Tip < terminalSlot || len(response.Decisions) != 0 {
		t.Fatalf("same bound transport current-config Sync: config=%d cluster=%q proposer=%q tip=%d decisions=%d err=%v",
			response.ConfigID, response.ClusterID, response.ProposerID, response.Tip, len(response.Decisions), err)
	}

	// The removed C identity remains unauthorized even after B's archive
	// recovery succeeded; only the certified retained membership authorizes B.
	retiredTransport := NewTransport(clusterID, "c", &config, tokens["c"])
	t.Cleanup(func() { _ = retiredTransport.Close() })
	if _, err := retiredTransport.FetchDecisions(ctx, "a", terminalSlot+1, 1); err == nil || !strings.Contains(err.Error(), "cluster identity mismatch") {
		t.Fatalf("retired C identity Sync error=%v, want cluster identity mismatch", err)
	}
	currentConfigRetiredTransport := NewTransport(clusterID, "c", &target, tokens["c"])
	t.Cleanup(func() { _ = currentConfigRetiredTransport.Close() })
	if _, err := currentConfigRetiredTransport.FetchDecisions(ctx, "a", terminalSlot+1, 1); err == nil || !strings.Contains(err.Error(), "peer authentication failed") {
		t.Fatalf("retired C current-config Sync error=%v, want peer authentication failure", err)
	}
}

func waitNestedSyncEvent(t *testing.T, ctx context.Context, events <-chan string, prefix string) string {
	t.Helper()
	for {
		select {
		case event := <-events:
			if strings.Contains(event, prefix) {
				return event
			}
		case <-ctx.Done():
			t.Fatalf("did not observe %q: %v", prefix, ctx.Err())
		}
	}
}
