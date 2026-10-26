package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/quic-go/quic-go"
)

func TestNodePendingReconfigurationDoesNotRejectValidPeerLearn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const clusterID = "pending-reconfiguration-readiness"
	tokens := map[quepaxa.NodeID]string{"a": "readiness-a", "b": "readiness-b", "c": "readiness-c"}
	ids := []quepaxa.NodeID{"a", "b", "c"}
	members := make([]quepaxa.Member, 0, len(ids))
	var bPacket net.PacketConn
	for _, id := range ids {
		packet, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		member := peerMember(clusterID, id, tokens[id])
		member.PeerURL = "quic://" + packet.LocalAddr().String()
		identity := sha256.Sum256([]byte("pending-reconfiguration-wal:" + string(id)))
		member.WALIdentity = hex.EncodeToString(identity[:])
		members = append(members, member)
		if id == "b" {
			bPacket = packet
		} else if err := packet.Close(); err != nil {
			t.Fatalf("close unused %s peer socket: %v", id, err)
		}
	}
	listenerTransport := &quic.Transport{Conn: bPacket}
	t.Cleanup(func() { _ = listenerTransport.Close() })
	initial := quepaxa.Cluster{ConfigID: 1, Members: members}
	baseTransport := &membershipOperationTransport{
		cores:    make(map[quepaxa.NodeID]*quepaxa.Core),
		disabled: make(map[quepaxa.NodeID]bool),
	}
	consensusTransport := &bridgeTerminalWithholdingTransport{
		membershipOperationTransport: baseTransport,
		withheld:                     make(map[quepaxa.NodeID]bool),
		freezeRecordWithheld:         "b",
	}
	for _, member := range members {
		wal, err := qlog.Open(filepath.Join(t.TempDir(), "qlog"))
		if err != nil {
			t.Fatalf("open %s WAL: %v", member.ID, err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		identity, err := hex.DecodeString(member.WALIdentity)
		if err != nil {
			t.Fatalf("decode %s WAL identity: %v", member.ID, err)
		}
		if err := wal.BindIdentity(identity); err != nil {
			t.Fatalf("bind %s WAL identity: %v", member.ID, err)
		}
		core, err := quepaxa.New(quepaxa.Config{
			NodeID: member.ID, Cluster: initial, WAL: wal, Transport: consensusTransport,
			EnableReconfiguration: true,
		})
		if err != nil {
			t.Fatalf("create voter %s: %v", member.ID, err)
		}
		baseTransport.cores[member.ID] = core
		baseTransport.ids = append(baseTransport.ids, member.ID)
	}
	consensusTransport.freezeRecordSlot = baseTransport.cores["a"].Tip() + 1

	node := &Node{
		config: &types.ExecutionConfig{ClusterID: types.ClusterID(clusterID), EnableReconfiguration: true},
		core:   consensusTransport.cores["b"],
	}
	node.opened.Store(true)
	node.fenceSession = newActiveVoterSessionForLearnTest(t, clusterID, "b")
	node.fenceWriterActive.Store(true)
	node.observeCatchUpRound(nil, &initial)
	if !node.Ready() || !node.peerLearnReady() {
		t.Fatal("voter B did not begin with public and peer-learning readiness")
	}

	bMaterial, err := materializer.Open(filepath.Join(t.TempDir(), "b.sqlite"), 1)
	if err != nil {
		t.Fatalf("open voter B materializer: %v", err)
	}
	t.Cleanup(func() { _ = bMaterial.Close() })
	bServer := network.NewServer(node.core, bMaterial, clusterID, true, nil, node.Ready)
	bServer.SetPeerLearnReady(node.peerLearnReady)
	bServer.SetPeerLearnDecisionAllowed(node.peerLearnDecisionAllowed)
	peer, err := network.StartPeerServerOnTransport(ctx, listenerTransport, bServer, members, tokens["b"], "readiness-admin")
	if err != nil {
		t.Fatalf("start voter B peer server: %v", err)
	}
	t.Cleanup(func() {
		_ = peer.Close()
		bServer.Close()
	})

	target := quepaxa.Cluster{ConfigID: 2, Members: []quepaxa.Member{members[0], members[2]}}
	// Keep the sender transport pointed only at B so recheck assertions cannot
	// be satisfied early by C's quorum response.
	retainedCluster := quepaxa.Cluster{ConfigID: initial.ConfigID, Members: []quepaxa.Member{members[0], members[1]}}
	sourceTransport := network.NewTransport(clusterID, "a", &retainedCluster, tokens["a"])
	t.Cleanup(func() { _ = sourceTransport.Close() })
	freeze, err := consensusTransport.cores["a"].BeginReconfiguration(ctx, target)
	if err != nil {
		t.Fatalf("begin removal: %v", err)
	}
	if node.Ready() {
		t.Fatal("public readiness remained open after the freeze")
	}
	if got := node.core.CurrentCluster(); got.ConfigID != initial.ConfigID {
		t.Fatalf("freeze unexpectedly changed current membership: config=%d", got.ConfigID)
	}
	if pending, ok := node.core.PendingReconfiguration(); !ok || pending.ConfigID != target.ConfigID {
		t.Fatalf("retained voter pending target=%+v present=%t, want config %d", pending, ok, target.ConfigID)
	}
	if got := node.core.Tip(); got != freeze {
		t.Fatalf("retained voter tip=%d after freeze, want %d", got, freeze)
	}
	if node.peerLearnReady() {
		t.Fatal("peer Learn readiness remained open after the freeze")
	}
	freezeValue, ok := consensusTransport.cores["a"].CertifiedValue(freeze)
	if !ok {
		t.Fatalf("source has no certified freeze at slot %d", freeze)
	}
	var freezeDecision quepaxa.Decision
	if err := json.Unmarshal(freezeValue.Certificate, &freezeDecision); err != nil {
		t.Fatalf("decode freeze certificate: %v", err)
	}
	freezeDecision.Proposal.Value = append([]byte(nil), freezeValue.Value...)
	if err := sourceTransport.SendDecision(ctx, freezeDecision); err != nil {
		t.Fatalf("exact freeze retransmission was rejected while readiness was closed: %v", err)
	}

	baseTransport.disable("b")
	if err := consensusTransport.cores["a"].FinishReconfiguration(ctx); err != nil {
		t.Fatalf("finish removal without the stale retained voter: %v", err)
	}
	drain, ok := consensusTransport.cores["a"].CertifiedValue(freeze + 1)
	if !ok {
		t.Fatalf("source has no certified drain decision at slot %d", freeze+1)
	}
	if control, err := quepaxa.DecodeReconfiguration(drain.Value); err != nil || control {
		t.Fatalf("selected Learned value is not an ordinary drain decision: control=%t err=%v", control, err)
	}
	var decision quepaxa.Decision
	if err := json.Unmarshal(drain.Certificate, &decision); err != nil {
		t.Fatalf("decode certified drain decision: %v", err)
	}
	decision.Proposal.Value = append([]byte(nil), drain.Value...)
	if decision.Slot != freeze+1 || decision.Proposal.Hash != drain.Hash {
		t.Fatalf("drain certificate does not bind slot/value: slot=%d hash_match=%t", decision.Slot, decision.Proposal.Hash == drain.Hash)
	}
	wrongKeyConfig := quepaxa.Cluster{ConfigID: initial.ConfigID, Members: []quepaxa.Member{members[1]}}
	wrongKeyTransport := network.NewTransport(clusterID, "a", &wrongKeyConfig, "incorrect-peer-key")
	t.Cleanup(func() { _ = wrongKeyTransport.Close() })
	if err := wrongKeyTransport.SendDecision(ctx, decision); err == nil || !strings.Contains(err.Error(), "peer authentication failed") {
		t.Fatalf("wrong-key sender Learn error=%v, want authenticated rejection", err)
	}
	if got := node.core.Tip(); got != freeze {
		t.Fatalf("wrong-key sender advanced retained voter tip to %d, want %d", got, freeze)
	}
	sparseDrain, ok := consensusTransport.cores["a"].CertifiedValue(freeze + 2)
	if !ok {
		t.Fatalf("source has no certified sparse drain decision at slot %d", freeze+2)
	}
	var sparseDecision quepaxa.Decision
	if err := json.Unmarshal(sparseDrain.Certificate, &sparseDecision); err != nil {
		t.Fatalf("decode sparse drain certificate: %v", err)
	}
	sparseDecision.Proposal.Value = append([]byte(nil), sparseDrain.Value...)
	if !node.peerLearnDecisionAllowed(sparseDecision) {
		t.Fatal("bounded sparse old-config drain decision was rejected before its missing prefix arrived")
	}
	wrongFreeze := decision
	wrongFreeze.Slot = freeze
	if node.peerLearnDecisionAllowed(wrongFreeze) {
		t.Fatal("pending-Learn exception admitted a drain value in the locally frozen slot")
	}
	wrongConfig := decision
	wrongConfig.ConfigID++
	if node.peerLearnDecisionAllowed(wrongConfig) {
		t.Fatal("pending-Learn exception admitted a decision from the wrong configuration")
	}
	wrongHash := decision
	wrongHash.Proposal.Hash[0] ^= 0xff
	if node.peerLearnDecisionAllowed(wrongHash) {
		t.Fatal("pending-Learn exception admitted a proposal with the wrong hash")
	}
	noFreezeCore := newPendingLearnTestCore(t, "b", initial, consensusTransport)
	noFreezeNode := &Node{config: node.config, core: noFreezeCore}
	noFreezeNode.opened.Store(true)
	if noFreezeNode.peerLearnDecisionAllowed(sparseDecision) {
		t.Fatal("pending-Learn exception admitted a decision without a locally durable freeze")
	}

	// Address only B so a successful response proves that its authenticated
	// PeerServer accepted this exact certified Learn, rather than reaching a
	// quorum through another recipient.
	badCertificate := decision
	badCertificate.Summaries = append([]quepaxa.Summary(nil), decision.Summaries...)
	badCertificate.Summaries[0].RecorderID = "not-a-recorder"
	if err := sourceTransport.SendDecision(ctx, badCertificate); err == nil {
		t.Fatal("invalid certificate was accepted while the pending-Learn exception was active")
	}
	if got := node.core.Tip(); got != freeze {
		t.Fatalf("invalid certificate advanced retained voter tip to %d, want %d", got, freeze)
	}
	if err := sourceTransport.SendDecision(ctx, decision); err != nil {
		t.Fatalf("valid certified drain Learn was rejected while public readiness was closed: %v", err)
	}
	if got := node.core.Tip(); got != freeze+1 {
		t.Fatalf("retained voter tip=%d after accepted drain Learn, want %d", got, freeze+1)
	}
	if node.Ready() {
		t.Fatal("public readiness opened before the reconfiguration terminal")
	}
	terminalSlot := consensusTransport.cores["a"].Tip()
	if terminalSlot <= freeze+1 {
		t.Fatalf("source terminal slot=%d does not follow the drained prefix after freeze %d", terminalSlot, freeze)
	}
	wrongSlot := decision
	wrongSlot.Slot = terminalSlot
	if node.peerLearnDecisionAllowed(wrongSlot) {
		t.Fatal("pending-Learn exception admitted a drain decision at the terminal slot")
	}
	for slot := freeze + 2; slot < terminalSlot-1; slot++ {
		value, exists := consensusTransport.cores["a"].CertifiedValue(slot)
		if !exists {
			t.Fatalf("source has no certified drain decision at slot %d", slot)
		}
		var next quepaxa.Decision
		if err := json.Unmarshal(value.Certificate, &next); err != nil {
			t.Fatalf("decode drain certificate at slot %d: %v", slot, err)
		}
		next.Proposal.Value = append([]byte(nil), value.Value...)
		if err := sourceTransport.SendDecision(ctx, next); err != nil {
			t.Fatalf("valid drain Learn at slot %d was rejected while readiness was closed: %v", slot, err)
		}
	}
	terminal, exists := consensusTransport.cores["a"].CertifiedValue(terminalSlot)
	if !exists {
		t.Fatalf("source has no certified terminal at slot %d", terminalSlot)
	}
	var terminalDecision quepaxa.Decision
	if err := json.Unmarshal(terminal.Certificate, &terminalDecision); err != nil {
		t.Fatalf("decode terminal certificate: %v", err)
	}
	terminalDecision.Proposal.Value = append([]byte(nil), terminal.Value...)
	wrongTerminalConfig := terminalDecision
	wrongTerminalConfig.ConfigID++
	if node.peerLearnDecisionAllowed(wrongTerminalConfig) {
		t.Fatal("pending-Learn exception admitted terminal from the wrong configuration")
	}
	wrongTerminalHash := terminalDecision
	wrongTerminalHash.Proposal.Hash[0] ^= 0xff
	if node.peerLearnDecisionAllowed(wrongTerminalHash) {
		t.Fatal("pending-Learn exception admitted terminal with the wrong value hash")
	}
	type terminalControl struct {
		Terminal      bool                   `json:"terminal,omitempty"`
		Abort         bool                   `json:"abort,omitempty"`
		Freeze        quepaxa.Slot           `json:"freeze"`
		TerminalSlot  quepaxa.Slot           `json:"terminal_slot"`
		Target        quepaxa.Cluster        `json:"target"`
		PrefixHash    [32]byte               `json:"prefix_hash"`
		EngineRemoval *quepaxa.EngineRemoval `json:"engine_removal,omitempty"`
	}
	var changedControl terminalControl
	if err := json.Unmarshal(terminal.Value[len("QRCF\x00"):], &changedControl); err != nil {
		t.Fatalf("decode terminal control for negative case: %v", err)
	}
	changedControl.Abort = true
	abortPayload, err := json.Marshal(changedControl)
	if err != nil {
		t.Fatal(err)
	}
	abortRemoval := terminalDecision
	abortRemoval.Proposal.Value = append([]byte("QRCF\x00"), abortPayload...)
	abortRemoval.Proposal.Hash = quepaxa.ValueHash(sha256.Sum256(abortRemoval.Proposal.Value))
	if node.peerLearnDecisionAllowed(abortRemoval) {
		t.Fatal("pending-Learn exception admitted an abort of ordinary removal")
	}
	changedControl.Abort = false
	changedControl.Target.ConfigID++
	changedPayload, err := json.Marshal(changedControl)
	if err != nil {
		t.Fatalf("encode wrong-target terminal: %v", err)
	}
	wrongTarget := terminalDecision
	wrongTarget.Proposal.Value = append([]byte("QRCF\x00"), changedPayload...)
	wrongTarget.Proposal.Hash = quepaxa.ValueHash(sha256.Sum256(wrongTarget.Proposal.Value))
	if node.peerLearnDecisionAllowed(wrongTarget) {
		t.Fatal("pending-Learn exception admitted terminal for a different target")
	}
	if !node.peerLearnDecisionAllowed(terminalDecision) {
		t.Fatal("exact locally frozen terminal was not admitted while readiness was closed")
	}

	// Hold terminal catch-up while the final prefix and exact terminal arrive
	// independently. The handler must retain the admission key captured before
	// catch-up, then allow only the exact terminal that became durably installed.
	catchupStarted := make(chan quepaxa.TerminalGapKey, 1)
	releaseCatchup := make(chan struct{})
	bServer.SetArchiveCatchUpContinuation(func(callCtx context.Context, key quepaxa.TerminalGapKey) error {
		catchupStarted <- key
		select {
		case <-releaseCatchup:
			return nil
		case <-callCtx.Done():
			return callCtx.Err()
		}
	})
	terminalResult := make(chan error, 1)
	go func() { terminalResult <- sourceTransport.SendDecision(ctx, terminalDecision) }()
	var capturedKey quepaxa.TerminalGapKey
	select {
	case capturedKey = <-catchupStarted:
		if capturedKey.Freeze != freeze || capturedKey.Terminal != terminalSlot {
			t.Fatalf("terminal catch-up captured key=%+v, want freeze=%d terminal=%d", capturedKey, freeze, terminalSlot)
		}
	case <-ctx.Done():
		t.Fatalf("terminal Learn did not enter blocked catch-up: %v", ctx.Err())
	}
	// A second, independently valid reconfiguration from the same old
	// membership cannot replace the captured round while its terminal is still
	// pending. Core serializes rounds and requires round A's terminal/abort to
	// be installed before round B can begin.
	competingTarget := quepaxa.Cluster{ConfigID: initial.ConfigID + 1, Members: append([]quepaxa.Member(nil), initial.Members...)}
	competingTarget.Members = append(competingTarget.Members, quepaxa.Member{ID: "d", WALIdentity: bridgeWALIdentity("d")})
	if err := node.core.ValidateReconfigurationTarget(competingTarget); err != nil {
		t.Fatalf("competing round B target is not independently valid: %v", err)
	}
	if _, err := node.core.BeginReconfiguration(ctx, competingTarget); err == nil || err.Error() != "reconfiguration is already frozen" {
		t.Fatalf("round B replaced pending round A: begin error=%v", err)
	}
	if !node.core.PendingTerminalGap(capturedKey) {
		t.Fatal("failed round B attempt changed the captured round A key")
	}
	lastPrefix, exists := consensusTransport.cores["a"].CertifiedValue(terminalSlot - 1)
	if !exists {
		t.Fatal("source has no certified final prefix before terminal")
	}
	lastPrefixValue := quepaxa.DecidedValue{Slot: terminalSlot - 1, Hash: lastPrefix.Hash, Value: append([]byte(nil), lastPrefix.Value...), Certificate: append([]byte(nil), lastPrefix.Certificate...)}
	if err := node.core.AcceptCertifiedHints([]quepaxa.DecidedValue{lastPrefixValue, terminal}); err != nil {
		t.Fatalf("install exact final prefix and terminal during blocked catch-up: %v", err)
	}
	close(releaseCatchup)
	if err := <-terminalResult; err != nil {
		t.Fatalf("exact concurrently installed terminal Learn was rejected after catch-up: %v", err)
	}
	bServer.SetArchiveCatchUpContinuation(nil)
	if got := node.core.Tip(); got != terminalSlot {
		t.Fatalf("retained voter tip=%d after terminal Learn, want %d", got, terminalSlot)
	}
	got := node.core.CurrentCluster()
	if got.ConfigID != target.ConfigID || len(got.Members) != len(target.Members) {
		t.Fatalf("retained voter membership after terminal=%+v, want target %+v", got, target)
	}
	for index, member := range target.Members {
		if got.Members[index].ID != member.ID || got.Members[index].WALIdentity != member.WALIdentity {
			t.Fatalf("retained voter member[%d]=%+v, want %+v", index, got.Members[index], member)
		}
	}
	if node.core.IsVoter() {
		t.Fatal("removed receiver remained a voter after installing the terminal")
	}
	if node.Ready() {
		t.Fatal("public readiness opened without a fresh current-cluster catch-up observation")
	}
	if err := sourceTransport.SendDecision(ctx, terminalDecision); err != nil {
		t.Fatalf("exact installed terminal retry was rejected while readiness was closed: %v", err)
	}

	assertRecheckDenies := func(name string, change func()) {
		t.Helper()
		var calls atomic.Int32
		bServer.SetPeerLearnDecisionAllowed(func(candidate quepaxa.Decision) bool {
			if calls.Add(1) == 2 {
				change()
			}
			return node.peerLearnDecisionAllowed(candidate)
		})
		err := sourceTransport.SendDecision(ctx, terminalDecision)
		if err == nil {
			t.Fatalf("terminal retry was accepted after %s changed during catch-up", name)
		}
		if got := calls.Load(); got != 2 {
			t.Fatalf("%s recheck calls=%d, want initial and pre-accept checks", name, got)
		}
		if got := node.core.Tip(); got != terminalSlot {
			t.Fatalf("terminal retry changed tip after %s: got %d want %d", name, got, terminalSlot)
		}
		bServer.SetPeerLearnDecisionAllowed(node.peerLearnDecisionAllowed)
	}
	assertRecheckDenies("shutdown", func() { node.terminalRequested.Store(true) })
	node.terminalRequested.Store(false)

	assertRecheckDenies("fence writer loss", func() { node.fenceWriterActive.Store(false) })

}

func newPendingLearnTestCore(t *testing.T, nodeID quepaxa.NodeID, cluster quepaxa.Cluster, transport quepaxa.Transport) *quepaxa.Core {
	t.Helper()
	wal, err := qlog.Open(filepath.Join(t.TempDir(), string(nodeID)+".qlog"))
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range cluster.Members {
		if member.ID != nodeID || member.WALIdentity == "" {
			continue
		}
		identity, err := hex.DecodeString(member.WALIdentity)
		if err != nil {
			t.Fatal(err)
		}
		if err := wal.BindIdentity(identity); err != nil {
			t.Fatal(err)
		}
		break
	}
	t.Cleanup(func() { _ = wal.Close() })
	core, err := quepaxa.New(quepaxa.Config{NodeID: nodeID, Cluster: cluster, WAL: wal, Transport: transport, EnableReconfiguration: true})
	if err != nil {
		t.Fatal(err)
	}
	return core
}

func newActiveVoterSessionForLearnTest(t *testing.T, clusterID, nodeID string) *fencing.Session {
	t.Helper()
	t.Setenv("RHIZA_POD_NAME", nodeID)
	socket := shortFenceTestSocket(t)
	identity := fencing.Identity{SourceClusterID: clusterID, NodeID: nodeID, PodName: nodeID, PodUID: "pod-" + nodeID, RunID: "run-" + nodeID}
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/startup" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(fencing.StartupGrant{Identity: identity, Role: fencing.StartupRoleVoterActive}); err != nil {
			t.Errorf("write active voter grant: %v", err)
		}
	})}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		_ = server.Close()
		_ = listener.Close()
	})
	session, err := fencing.Connect(t.Context(), socket, "test-ticket", clusterID, nodeID)
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func TestPeerLearnAdmissionRemainsClosedAfterStickyLifecycleFailure(t *testing.T) {
	wal, err := qlog.Open(filepath.Join(t.TempDir(), "local.qlog"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	if err := wal.SetMaxBytes(4 << 20); err != nil {
		t.Fatal(err)
	}
	core, err := quepaxa.New(quepaxa.Config{
		NodeID: "local", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "local"}}},
		WAL: wal, LocalMode: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	material, err := materializer.Open(filepath.Join(t.TempDir(), "local.sqlite"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := network.NewServer(core, material, "cluster", true, nil, func() bool { return true })
	defer server.Close()
	server.SetPeerLearnReady(func() bool { return true })
	// Local lifecycle failure is intentionally a no-op for HA servers. Exercise
	// a failure during admission on the Local server where the gate applies.
	server.SetPeerLearnDecisionAllowed(func(quepaxa.Decision) bool {
		server.FailLocalLifecycle(errors.New("injected sticky lifecycle failure"))
		return true
	})
	if server.PeerLearnDecisionAllowed(quepaxa.Decision{}) {
		t.Fatal("peer Learn admission ignored lifecycle failure during its predicate")
	}
	if server.PeerLearnReady() || server.PeerLearnDecisionAllowed(quepaxa.Decision{}) {
		t.Fatal("peer Learn admission remained open after sticky lifecycle failure")
	}
}
