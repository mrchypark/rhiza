package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
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

func TestNodePublishesFixedPrefixBeforeOrdinaryRemovalTerminal(t *testing.T) {
	assertNodePublishesFixedPrefixBeforeRemovalTerminal(t, false)
}

func TestNodePublishesFixedPrefixBeforeEngineRemovalTerminal(t *testing.T) {
	assertNodePublishesFixedPrefixBeforeRemovalTerminal(t, true)
}

func TestNodeLearnedTerminalUsesSynchronousPublishedPrefix(t *testing.T) {
	ctx := context.Background()
	const clusterID = "reconfiguration-archive-continuation"
	tokens := map[quepaxa.NodeID]string{"a": "bridge-a", "b": "bridge-b", "c": "bridge-c", "d": "bridge-d", "e": "bridge-e"}
	addresses := map[quepaxa.NodeID]string{
		"a": archiveTerminalPeerAddress(t),
		"b": archiveTerminalPeerAddress(t),
		"c": archiveTerminalPeerAddress(t),
		"d": archiveTerminalPeerAddress(t),
		"e": archiveTerminalPeerAddress(t),
	}
	members := []quepaxa.Member{
		{ID: "a", PublicKey: peerMember(clusterID, "a", tokens["a"]).PublicKey, PeerURL: "quic://" + addresses["a"], WALIdentity: bridgeWALIdentity("a")},
		{ID: "b", PublicKey: peerMember(clusterID, "b", tokens["b"]).PublicKey, PeerURL: "quic://" + addresses["b"], WALIdentity: bridgeWALIdentity("b")},
		{ID: "c", PublicKey: peerMember(clusterID, "c", tokens["c"]).PublicKey, PeerURL: "quic://" + addresses["c"], WALIdentity: bridgeWALIdentity("c")},
		{ID: "d", PublicKey: peerMember(clusterID, "d", tokens["d"]).PublicKey, PeerURL: "quic://" + addresses["d"], WALIdentity: bridgeWALIdentity("d")},
		{ID: "e", PublicKey: peerMember(clusterID, "e", tokens["e"]).PublicKey, PeerURL: "quic://" + addresses["e"], WALIdentity: bridgeWALIdentity("e")},
	}
	initial := quepaxa.Cluster{ConfigID: 1, Members: members}
	baseTransport := &membershipOperationTransport{cores: make(map[quepaxa.NodeID]*quepaxa.Core), disabled: make(map[quepaxa.NodeID]bool)}
	transport := &bridgeTerminalWithholdingTransport{membershipOperationTransport: baseTransport, withheld: make(map[quepaxa.NodeID]bool), freezeRecordWithheld: "b"}
	bucket := thanosobjstore.NewInMemBucket()
	publisher := recovery.NewManager(bucket, clusterID, recovery.ArchiveConfigID)
	if err := publisher.Load(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(publisher.Close)
	reader := recovery.NewManager(bucket, clusterID, recovery.ArchiveConfigID)
	if err := reader.Load(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reader.Close)
	publisherNode := &Node{archive: publisher}
	for _, member := range members {
		wal, err := qlog.Open(filepath.Join(t.TempDir(), string(member.ID), "qlog"))
		if err != nil {
			t.Fatalf("open %s WAL: %v", member.ID, err)
		}
		identity, err := hex.DecodeString(member.WALIdentity)
		if err != nil {
			t.Fatal(err)
		}
		if err := wal.BindIdentity(identity); err != nil {
			t.Fatalf("bind %s WAL: %v", member.ID, err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		core, err := quepaxa.New(quepaxa.Config{
			NodeID: member.ID, Cluster: initial, WAL: wal, Transport: transport, EnableReconfiguration: true,
			ReconfigurationPrefixPublication: func(ctx context.Context, prefix quepaxa.ReconfigurationPrefix) error {
				return publisherNode.publishReconfigurationPrefix(ctx, prefix)
			},
		})
		if err != nil {
			t.Fatalf("create %s core: %v", member.ID, err)
		}
		baseTransport.cores[member.ID] = core
		baseTransport.ids = append(baseTransport.ids, member.ID)
		if member.ID == "a" {
			publisherNode.core = core
		}
	}
	transport.freezeRecordSlot = transport.cores["a"].Tip() + 1
	target := quepaxa.Cluster{ConfigID: 2, Members: append([]quepaxa.Member(nil), members[:4]...)}
	baseTransport.disable("e")
	if _, err := transport.cores["a"].BeginReconfigurationAt(ctx, target, 0); err != nil {
		t.Fatalf("begin removal: %v", err)
	}
	freeze := transport.cores["a"].Tip()
	if got := transport.cores["b"].Tip(); got != freeze {
		t.Fatalf("retained voter tip=%d after freeze, want %d", got, freeze)
	}
	baseTransport.mu.Lock()
	transport.withheld["b"] = true
	baseTransport.mu.Unlock()
	if err := transport.cores["a"].FinishReconfigurationAt(ctx, target, 0); err != nil {
		t.Fatalf("finish removal and publish old prefix: %v", err)
	}
	terminal := transport.cores["a"].Tip()
	if got, want := publisher.Tip(), terminal-1; got != want {
		t.Fatalf("published archive tip=%d, want terminal prefix %d", got, want)
	}
	if got := reader.Tip(); got >= terminal-1 {
		t.Fatalf("reader unexpectedly observed archive update before Learned: tip=%d terminal=%d", got, terminal)
	}
	decided, ok := transport.cores["a"].CertifiedValue(terminal)
	if !ok {
		t.Fatalf("terminal %d is not certified", terminal)
	}
	var decision quepaxa.Decision
	if err := json.Unmarshal(decided.Certificate, &decision); err != nil {
		t.Fatalf("decode terminal decision: %v", err)
	}
	decision.Proposal.Value = append([]byte(nil), decided.Value...)
	if key, ok := transport.cores["b"].KnownTerminalGap(decision); !ok || key.Terminal != terminal {
		bCore := transport.cores["b"]
		startupKey, startupOK := bCore.StartupTerminalGapSnapshot()
		bCluster := bCore.CurrentCluster()
		_, freezeHashOK := bCore.PrefixHash(freeze)
		t.Fatalf("retained voter has no matching terminal gap: known_key=%+v known_ok=%t startup_key=%+v startup_ok=%t b_tip=%d freeze=%d terminal=%d decision_config=%d b_config=%d freeze_prefix_ok=%t",
			key, ok, startupKey, startupOK, bCore.Tip(), freeze, terminal, decision.ConfigID, bCluster.ConfigID, freezeHashOK)
	}
	deadA := "quic://" + archiveTerminalClosedAddress(t)
	bFetchCluster := initial
	bFetchCluster.Members = append([]quepaxa.Member(nil), members...)
	for i := range bFetchCluster.Members {
		if bFetchCluster.Members[i].ID == "a" {
			bFetchCluster.Members[i].PeerURL = deadA
		}
	}
	bFetchTransport := network.NewTransport(clusterID, "b", &bFetchCluster, tokens["b"])
	bFetchTransport.BindCore(transport.cores["b"])
	t.Cleanup(func() { _ = bFetchTransport.Close() })
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
	dMaterial, err := materializer.Open(filepath.Join(t.TempDir(), "d.sqlite"), 1)
	if err != nil {
		t.Fatalf("open retained voter materializer: %v", err)
	}
	t.Cleanup(func() { _ = dMaterial.Close() })
	bServer := network.NewServer(transport.cores["b"], bMaterial, clusterID, true, bFetchTransport, func() bool { return true })
	t.Cleanup(bServer.Close)
	bServer.SetPeerLearnReady(func() bool { return true })
	readerNode := &Node{
		config: &types.ExecutionConfig{ClusterID: types.ClusterID(clusterID), ObjStoreSyncInterval: time.Hour},
		core:   transport.cores["b"], archive: reader,
	}
	readerNode.initArchiveCatchUpHints()
	readerNode.installArchiveCatchUpHint(bServer, transport.cores["b"])
	bPeer, err := network.StartPeerServer(ctx, addresses["b"], bServer, members, tokens["b"], "bridge-admin")
	if err != nil {
		t.Fatalf("start retained voter peer: %v", err)
	}
	t.Cleanup(func() { _ = bPeer.Close() })
	cServer := network.NewServer(transport.cores["c"], cMaterial, clusterID, true, nil, func() bool { return true })
	t.Cleanup(cServer.Close)
	cServer.SetPeerLearnReady(func() bool { return true })
	cPeer, err := network.StartPeerServer(ctx, addresses["c"], cServer, members, tokens["c"], "bridge-admin")
	if err != nil {
		t.Fatalf("start removed voter peer: %v", err)
	}
	t.Cleanup(func() { _ = cPeer.Close() })
	dServer := network.NewServer(transport.cores["d"], dMaterial, clusterID, true, nil, func() bool { return true })
	dServerClosed := false
	t.Cleanup(func() {
		if !dServerClosed {
			dServer.Close()
		}
	})
	dServer.SetPeerLearnReady(func() bool { return true })
	dPeer, err := network.StartPeerServer(ctx, addresses["d"], dServer, members, tokens["d"], "bridge-admin")
	if err != nil {
		t.Fatalf("start retained voter peer: %v", err)
	}
	dPeerClosed := false
	t.Cleanup(func() {
		if !dPeerClosed {
			_ = dPeer.Close()
		}
	})
	// D helped certify the terminal, but is unavailable for its delivery. This
	// makes B's archive-continuation response necessary to reach both quorum
	// thresholds rather than allowing the sender to succeed despite B failing.
	if err := dPeer.Close(); err != nil {
		t.Fatalf("stop non-B terminal acknowledger: %v", err)
	}
	dPeerClosed = true
	dServer.Close()
	dServerClosed = true
	sourceTransport := network.NewTransport(clusterID, "a", &target, tokens["a"])
	sourceTransport.BindCore(transport.cores["a"])
	defer sourceTransport.Close()
	sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err = sourceTransport.SendDecision(sendCtx, decision)
	cancel()
	if err != nil {
		t.Fatalf("send terminal through archive continuation: %v", err)
	}
	if got := transport.cores["b"].Tip(); got != terminal {
		t.Fatalf("retained voter tip=%d after terminal ACK, want %d", got, terminal)
	}
	if key, ok := transport.cores["b"].KnownTerminalGap(decision); ok {
		t.Fatalf("terminal gap remained after acceptance: %+v", key)
	}
	if got := reader.Tip(); got != terminal-1 {
		t.Fatalf("receiver archive tip=%d, want prefix only through %d", got, terminal-1)
	}
	if got := transport.cores["b"].CurrentCluster(); got.ConfigID != target.ConfigID || containsMember(got.Members, "e") {
		t.Fatalf("retained voter config=%+v, want removal target %+v", got, target)
	}
}

type bridgeTerminalWithholdingTransport struct {
	*membershipOperationTransport
	withheld             map[quepaxa.NodeID]bool
	freezeRecordWithheld quepaxa.NodeID
	freezeRecordSlot     quepaxa.Slot
}

func (t *bridgeTerminalWithholdingTransport) SendRecord(ctx context.Context, to quepaxa.NodeID, request quepaxa.RecordRequest) (quepaxa.Summary, error) {
	if to == t.freezeRecordWithheld && request.Slot == t.freezeRecordSlot {
		return quepaxa.Summary{}, fmt.Errorf("peer unavailable for freeze record")
	}
	summary, err := t.membershipOperationTransport.SendRecord(ctx, to, request)
	if err != nil {
		return summary, fmt.Errorf("record peer %s slot %d: %w", to, request.Slot, err)
	}
	return summary, nil
}

func (t *bridgeTerminalWithholdingTransport) SendDecision(_ context.Context, decision quepaxa.Decision) error {
	t.mu.RLock()
	cores := make([]*quepaxa.Core, 0, len(t.ids))
	for _, id := range t.ids {
		if !t.disabled[id] && !t.withheld[id] {
			cores = append(cores, t.cores[id])
		}
	}
	t.mu.RUnlock()
	for _, core := range cores {
		if err := core.AcceptDecision(decision); err != nil {
			return err
		}
	}
	return nil
}

func assertNodePublishesFixedPrefixBeforeRemovalTerminal(t *testing.T, engineOwned bool) {
	t.Helper()
	ctx := context.Background()
	initialMembers := []quepaxa.Member{
		{ID: "a", WALIdentity: bridgeWALIdentity("a")},
		{ID: "b", WALIdentity: bridgeWALIdentity("b")},
		{ID: "c", WALIdentity: bridgeWALIdentity("c")},
	}
	cluster := quepaxa.Cluster{ConfigID: 1, Members: initialMembers}
	transport := &membershipOperationTransport{cores: make(map[quepaxa.NodeID]*quepaxa.Core)}
	bucket := thanosobjstore.NewInMemBucket()
	archive := recovery.NewManager(bucket, "bridge-test", recovery.ArchiveConfigID)
	if err := archive.Load(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(archive.Close)
	node := &Node{archive: archive}
	for _, member := range initialMembers {
		wal, err := qlog.Open(filepath.Join(t.TempDir(), "qlog"))
		if err != nil {
			t.Fatalf("open %s WAL: %v", member.ID, err)
		}
		identity, err := hex.DecodeString(member.WALIdentity)
		if err != nil {
			t.Fatal(err)
		}
		if err := wal.BindIdentity(identity); err != nil {
			t.Fatalf("bind %s WAL identity: %v", member.ID, err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		config := quepaxa.Config{
			NodeID: member.ID, Cluster: cluster, WAL: wal, Transport: transport,
			EnableReconfiguration: true, EngineOwnedRemovals: engineOwned,
			ReconfigurationAdmission: func(context.Context, quepaxa.ReconfigurationAdmission) error { return nil },
		}
		if member.ID == "a" {
			config.ReconfigurationPrefixPublication = node.publishReconfigurationPrefix
		}
		core, err := quepaxa.New(config)
		if err != nil {
			t.Fatalf("create %s core: %v", member.ID, err)
		}
		transport.cores[member.ID] = core
		transport.ids = append(transport.ids, member.ID)
		if member.ID == "a" {
			node.core = core
		}
	}
	target := quepaxa.Cluster{ConfigID: 2, Members: append([]quepaxa.Member(nil), initialMembers[:2]...)}
	var terminal quepaxa.Slot
	if engineOwned {
		operation, err := node.core.BeginEngineOwnedRemovalAt(ctx, target, "bridge-engine-removal", 0)
		if err != nil {
			t.Fatalf("begin engine removal: %v", err)
		}
		operation.StorageEffectsExcludedHash = quepaxa.ValueHash{1}
		if err := node.core.FinishEngineOwnedRemovalAt(ctx, operation); err != nil {
			t.Fatalf("finish engine removal: %v", err)
		}
		terminal = node.core.Tip()
	} else {
		if _, err := node.core.BeginReconfigurationAt(ctx, target, 0); err != nil {
			t.Fatalf("begin ordinary removal: %v", err)
		}
		if err := node.core.FinishReconfigurationAt(ctx, target, 0); err != nil {
			t.Fatalf("finish ordinary removal: %v", err)
		}
		terminal = node.core.Tip()
	}
	if terminal <= 1 {
		t.Fatalf("terminal slot=%d, want a non-empty prefix", terminal)
	}
	if got, want := archive.Tip(), terminal-1; got != want {
		t.Fatalf("archive tip=%d after finish, want fixed pre-terminal prefix %d", got, want)
	}
	if _, ok := node.core.CertifiedValue(terminal); !ok {
		t.Fatalf("terminal slot %d was not certified after prefix publication", terminal)
	}
	if got := node.core.Tip(); got != terminal {
		t.Fatalf("core tip=%d after terminal publication, want %d", got, terminal)
	}
}

func bridgeWALIdentity(id string) string {
	hash := sha256.Sum256([]byte("reconfiguration-archive-bridge:" + id))
	return hex.EncodeToString(hash[:])
}
