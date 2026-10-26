package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	thanosobjstore "github.com/thanos-io/objstore"
)

func TestNodeLearnedTerminalGapAutomaticallyConsumesPublishedArchive(t *testing.T) {
	ctx := context.Background()
	const clusterID = "archive-terminal-hint"
	const adminToken = "archive-terminal-admin"
	tokens := map[quepaxa.NodeID]string{"a": "archive-terminal-a", "b": "archive-terminal-b", "c": "archive-terminal-c"}
	aAddr := archiveTerminalPeerAddress(t)
	bAddr := archiveTerminalPeerAddress(t)
	cAddr := archiveTerminalPeerAddress(t)
	members := []quepaxa.Member{
		peerMember(clusterID, "a", tokens["a"]),
		peerMember(clusterID, "b", tokens["b"]),
		peerMember(clusterID, "c", tokens["c"]),
	}
	for i := range members {
		switch members[i].ID {
		case "a":
			members[i].PeerURL = "quic://" + aAddr
		case "b":
			members[i].PeerURL = "quic://" + bAddr
		case "c":
			members[i].PeerURL = "quic://" + cAddr
		}
	}
	wals := make(map[quepaxa.NodeID]*qlog.WAL, len(members))
	for i := range members {
		id := members[i].ID
		identity := sha256.Sum256([]byte("archive-terminal-wal:" + string(id)))
		members[i].WALIdentity = hex.EncodeToString(identity[:])
		wal, err := qlog.Open(filepath.Join(t.TempDir(), "qlog"))
		if err != nil {
			t.Fatalf("open %s WAL: %v", id, err)
		}
		if err := wal.BindIdentity(identity[:]); err != nil {
			t.Fatalf("bind %s WAL identity: %v", id, err)
		}
		wals[id] = wal
		t.Cleanup(func() { _ = wal.Close() })
	}
	initial := quepaxa.Cluster{ConfigID: 1, Members: members}
	base := &membershipOperationTransport{cores: make(map[quepaxa.NodeID]*quepaxa.Core), disabled: make(map[quepaxa.NodeID]bool)}
	transport := &bridgeTerminalWithholdingTransport{
		membershipOperationTransport: base,
		withheld:                     make(map[quepaxa.NodeID]bool),
		freezeRecordWithheld:         "b",
	}
	for _, member := range members {
		core, err := quepaxa.New(quepaxa.Config{
			NodeID: member.ID, Cluster: initial, WAL: wals[member.ID],
			Transport:             transport,
			EnableReconfiguration: true,
		})
		if err != nil {
			t.Fatalf("create voter %s: %v", member.ID, err)
		}
		base.cores[member.ID] = core
		base.ids = append(base.ids, member.ID)
	}
	for _, id := range []quepaxa.NodeID{"a", "b", "c"} {
		if got := base.cores[id].CurrentCluster().ConfigID; got != initial.ConfigID {
			t.Fatalf("voter %s config=%d, want %d", id, got, initial.ConfigID)
		}
	}

	target := quepaxa.Cluster{ConfigID: 2, Members: append([]quepaxa.Member(nil), members[:2]...)}
	transport.freezeRecordSlot = base.cores["a"].Tip() + 1
	if _, err := base.cores["a"].BeginReconfiguration(ctx, target); err != nil {
		t.Fatalf("begin removal: %v", err)
	}
	freeze := base.cores["a"].Tip()
	if got := base.cores["b"].Tip(); got != freeze {
		t.Fatalf("retained voter tip=%d after freeze, want %d", got, freeze)
	}
	base.mu.Lock()
	transport.withheld["b"] = true
	base.mu.Unlock()
	if err := base.cores["a"].FinishReconfiguration(ctx); err != nil {
		t.Fatalf("finish removal while retained voter misses terminal: %v", err)
	}
	terminal := base.cores["a"].Tip()
	if terminal-freeze > 16 {
		t.Fatalf("terminal gap=%d, exceeds bounded drain", terminal-freeze)
	}
	decided, ok := base.cores["a"].CertifiedValue(terminal)
	if !ok {
		t.Fatalf("terminal slot %d was not locally certified", terminal)
	}
	isControl, err := quepaxa.DecodeReconfiguration(decided.Value)
	if err != nil || !isControl || base.cores["a"].CurrentCluster().ConfigID != target.ConfigID {
		t.Fatalf("local terminal control=%t current_config=%d err=%v", isControl, base.cores["a"].CurrentCluster().ConfigID, err)
	}

	archiveBucket := thanosobjstore.NewInMemBucket()
	publisher := recovery.NewManager(archiveBucket, string(clusterID), recovery.ArchiveConfigID)
	defer publisher.Close()
	if err := publisher.Load(ctx); err != nil {
		t.Fatalf("load publisher: %v", err)
	}
	reader := recovery.NewManager(archiveBucket, string(clusterID), recovery.ArchiveConfigID)
	defer reader.Close()
	if err := reader.Load(ctx); err != nil {
		t.Fatalf("load initially unpublished reader: %v", err)
	}
	if reader.Tip() >= terminal {
		t.Fatalf("test reader was not stale before publication: tip=%d terminal=%d", reader.Tip(), terminal)
	}
	peerMembers := append([]quepaxa.Member(nil), members...)
	deadA := "quic://" + archiveTerminalClosedAddress(t)
	bFetchCluster := initial
	bFetchCluster.Members = append([]quepaxa.Member(nil), peerMembers...)
	for i := range bFetchCluster.Members {
		if bFetchCluster.Members[i].ID == "a" {
			bFetchCluster.Members[i].PeerURL = deadA
		}
	}
	bFetchTransport := network.NewTransport(clusterID, "b", &bFetchCluster, tokens["b"])
	bMaterial, err := materializer.Open(filepath.Join(t.TempDir(), "b.sqlite"), 1)
	if err != nil {
		t.Fatalf("open retained voter materializer: %v", err)
	}
	t.Cleanup(func() { _ = bMaterial.Close() })
	cMaterial, err := materializer.Open(filepath.Join(t.TempDir(), "c.sqlite"), 1)
	if err != nil {
		t.Fatalf("open removed voter materializer: %v", err)
	}
	t.Cleanup(func() { _ = cMaterial.Close() })
	bServer := network.NewServer(base.cores["b"], bMaterial, clusterID, true, bFetchTransport, func() bool { return true })
	bServer.SetPeerLearnReady(func() bool { return true })
	node := &Node{
		config:  &types.ExecutionConfig{ClusterID: types.ClusterID(clusterID), ObjStoreSyncInterval: time.Hour},
		core:    base.cores["b"],
		archive: reader,
	}
	node.initArchiveCatchUpHints()
	node.installArchiveCatchUpHint(bServer, base.cores["b"])
	peer, err := network.StartPeerServer(ctx, bAddr, bServer, peerMembers, tokens["b"], adminToken)
	if err != nil {
		bFetchTransport.Close()
		t.Fatalf("start retained voter peer server: %v", err)
	}
	cServer := network.NewServer(base.cores["c"], cMaterial, clusterID, true, nil, func() bool { return true })
	cPeer, err := network.StartPeerServer(ctx, cAddr, cServer, peerMembers, tokens["c"], adminToken)
	if err != nil {
		_ = peer.Close()
		bFetchTransport.Close()
		t.Fatalf("start removed voter peer server: %v", err)
	}
	defer func() {
		_ = peer.Close()
		_ = cPeer.Close()
		bServer.Close()
		cServer.Close()
		_ = bFetchTransport.Close()
	}()
	var decision quepaxa.Decision
	if err := json.Unmarshal(decided.Certificate, &decision); err != nil {
		t.Fatalf("decode locally certified terminal for peer transport: %v", err)
	}
	decision.ConfigID = initial.ConfigID
	decision.Proposal.Value = append([]byte(nil), decided.Value...)
	sourceTransport := network.NewTransport(clusterID, "a", &initial, tokens["a"])
	sourceTransport.BindCore(base.cores["a"])
	defer sourceTransport.Close()
	sendCtx, cancelSend := context.WithTimeout(ctx, 4*time.Second)
	sendErr := sourceTransport.SendDecision(sendCtx, decision)
	cancelSend()
	if sendErr == nil {
		t.Fatal("stale retained voter unexpectedly acknowledged terminal without prefix catch-up")
	}
	if got := base.cores["b"].Tip(); got != freeze {
		t.Fatalf("retained voter tip=%d after rejected terminal, want frozen prefix %d", got, freeze)
	}
	if key, ok := base.cores["b"].KnownTerminalGap(decision); !ok || key.Terminal != terminal {
		t.Fatalf("retained voter terminal gap=%+v present=%t, want terminal %d", key, ok, terminal)
	}
	node.archiveHintMu.Lock()
	pendingHint := node.archiveHintPending
	node.archiveHintMu.Unlock()
	if pendingHint.Terminal != terminal {
		t.Fatalf("queued archive hint=%+v, want terminal %d", pendingHint, terminal)
	}
	if got := reader.Tip(); got >= terminal {
		t.Fatalf("reader archive tip=%d before publication, want stale prefix below terminal %d", got, terminal)
	}
	if err := publisher.SyncThrough(ctx, base.cores["a"], terminal); err != nil {
		t.Fatalf("publish certified terminal after rejected request: %v", err)
	}
	if got := reader.Tip(); got >= terminal {
		t.Fatalf("reader archive tip=%d changed before asynchronous worker, want stale prefix below terminal %d", got, terminal)
	}
	workerCtx, stopWorker := context.WithCancel(ctx)
	node.startArchiveWorker(workerCtx, base.cores["b"])
	defer func() {
		stopWorker()
		node.wg.Wait()
	}()

	applyCtx, cancelApply := context.WithTimeout(ctx, 5*time.Second)
	defer cancelApply()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for base.cores["b"].Tip() < terminal {
		select {
		case <-applyCtx.Done():
			t.Fatalf("automatic archive hint did not apply terminal: tip=%d terminal=%d: %v", base.cores["b"].Tip(), terminal, applyCtx.Err())
		case <-ticker.C:
		}
	}
	got := base.cores["b"].CurrentCluster()
	if got.ConfigID != target.ConfigID || !containsMember(got.Members, "b") || containsMember(got.Members, "c") {
		t.Fatalf("retained voter membership after automatic archive catch-up=%+v, want target %+v", got, target)
	}
}

func archiveTerminalPeerAddress(t *testing.T) string {
	t.Helper()
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := conn.LocalAddr().String()
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func archiveTerminalClosedAddress(t *testing.T) string {
	t.Helper()
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := conn.LocalAddr().String()
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}
