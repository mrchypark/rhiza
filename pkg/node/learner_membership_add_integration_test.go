package node

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

func TestNodeChangeMembershipAuthenticatesLearnerBeforeADD(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	bucket, err := objectstore.NewBucket(objectstore.Config{Provider: objectstore.ProviderFilesystem, FilesystemDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	const clusterID = "cluster"
	const adminToken = "membership-add-admin-token"
	tokens := map[quepaxa.NodeID]string{"a": "membership-add-a-token", "b": "membership-add-b-token"}
	bootstrap := []quepaxa.Member{
		peerMember(clusterID, "a", tokens["a"]),
		peerMember(clusterID, "b", tokens["b"]),
	}
	for i := range bootstrap {
		bootstrap[i].PeerURL = "quic://" + learnerArchiveCatchupAddress(t)
	}
	initial := quepaxa.Cluster{ConfigID: 1, Members: append([]quepaxa.Member(nil), bootstrap...)}

	consensusTransport := &membershipOperationTransport{cores: make(map[quepaxa.NodeID]*quepaxa.Core), disabled: make(map[quepaxa.NodeID]bool)}
	voterWALs := make([]*qlog.WAL, 0, len(bootstrap))
	for i := range bootstrap {
		member := &bootstrap[i]
		config := &types.ExecutionConfig{
			DataDir: t.TempDir(), ClusterID: clusterID, NodeID: types.NodeID(member.ID),
			ObjStoreProvider: "filesystem", ObjStoreDir: "unused", Members: append([]quepaxa.Member(nil), bootstrap...),
		}
		state, err := loadVoterIdentity(config)
		if err != nil {
			t.Fatal(err)
		}
		wal, err := qlog.Open(filepath.Join(config.DataDir, "qlog"))
		if err != nil {
			t.Fatal(err)
		}
		voterWALs = append(voterWALs, wal)
		t.Cleanup(func() { _ = wal.Close() })
		if err := ensureVoterIdentity(ctx, config, bucket, wal, state, false); err != nil {
			t.Fatalf("register voter %s: %v", member.ID, err)
		}
		state, err = loadVoterIdentity(config)
		if err != nil || state.identity == nil {
			t.Fatalf("reload voter %s identity: state=%+v err=%v", member.ID, state, err)
		}
		member.WALIdentity = state.identity.Nonce
		consensusTransport.ids = append(consensusTransport.ids, member.ID)
	}
	// The immutable membership registration was written from the bootstrap
	// descriptors before their WAL identities were attached, as in startup.
	// Consensus carries the bound identities from this point onward.
	initial.Members = append([]quepaxa.Member(nil), bootstrap...)
	consensusTransport.cores = make(map[quepaxa.NodeID]*quepaxa.Core)
	var nodeA *Node
	for i, member := range bootstrap {
		core, err := quepaxa.New(quepaxa.Config{
			NodeID: member.ID, Cluster: initial, WAL: voterWALs[i], Transport: consensusTransport,
			EnableReconfiguration: true,
			ReconfigurationAdmission: func(admissionCtx context.Context, admission quepaxa.ReconfigurationAdmission) error {
				if nodeA == nil {
					return network.ErrNotReady
				}
				return nodeA.verifyMembershipAdmission(admissionCtx, admission)
			},
		})
		if err != nil {
			t.Fatalf("create voter %s: %v", member.ID, err)
		}
		consensusTransport.cores[member.ID] = core
	}

	archive := recovery.NewManager(bucket, clusterID, recovery.ArchiveConfigID)
	if err := archive.Load(ctx); err != nil {
		t.Fatalf("load decision archive: %v", err)
	}
	t.Cleanup(func() { archive.Close() })
	peerTransport := network.NewTransport(clusterID, "a", &initial, tokens["a"])
	peerTransport.BindCore(consensusTransport.cores["a"])
	t.Cleanup(func() { _ = peerTransport.Close() })
	nodeConfig := &types.ExecutionConfig{
		ClusterID: clusterID, NodeID: "a", ObjStorePrefix: "", EnableReconfiguration: true,
		AdminToken: adminToken, Members: append([]quepaxa.Member(nil), bootstrap...),
	}
	nodeA = &Node{config: nodeConfig, core: consensusTransport.cores["a"], bucket: bucket, archive: archive, transport: peerTransport}

	// Expose the current voters over real authenticated QUIC so the learner's
	// VerifyPrefix handler can fill the frozen prefix before admission.
	peerServers := make([]*network.PeerServer, 0, len(bootstrap))
	servers := make([]*network.Server, 0, len(bootstrap))
	for _, member := range bootstrap {
		server := network.NewServer(consensusTransport.cores[member.ID], nil, clusterID, false, nil)
		servers = append(servers, server)
		peer, err := network.StartPeerServer(ctx, member.PeerURL[len("quic://"):], server, bootstrap, tokens[member.ID], adminToken)
		if err != nil {
			t.Fatalf("start voter %s peer server: %v", member.ID, err)
		}
		peerServers = append(peerServers, peer)
	}
	t.Cleanup(func() {
		for _, peer := range peerServers {
			_ = peer.Close()
		}
		for _, server := range servers {
			server.Close()
		}
	})

	if _, _, err := nodeA.core.Propose(ctx, types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{1, 2, 3})); err != nil {
		t.Fatalf("commit durable-prefix baseline: %v", err)
	}
	through := nodeA.core.Tip()
	wantPrefix, err := nodeA.core.DurablePrefix(through)
	if err != nil {
		t.Fatalf("durable voter prefix through %d: %v", through, err)
	}

	learnerToken := "membership-add-fresh-token"
	learnerPublic := peerMember(clusterID, "fresh", learnerToken).PublicKey
	learnerConfig := &types.ExecutionConfig{
		DataDir: t.TempDir(), ClusterID: clusterID, NodeID: "fresh", ObjStoreProvider: "filesystem", ObjStoreDir: "unused",
		Members: append([]quepaxa.Member(nil), bootstrap...), Learner: &quepaxa.Member{ID: "fresh", PublicKey: learnerPublic},
	}
	localIdentity, err := loadVoterIdentity(learnerConfig)
	if err != nil {
		t.Fatal(err)
	}
	learnerWAL, err := qlog.Open(filepath.Join(learnerConfig.DataDir, "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = learnerWAL.Close() })
	if err := ensureVoterIdentity(ctx, learnerConfig, bucket, learnerWAL, localIdentity, false); err != nil {
		t.Fatalf("register fresh learner identity: %v", err)
	}
	localIdentity, err = loadVoterIdentity(learnerConfig)
	if err != nil || localIdentity.identity == nil {
		t.Fatalf("reload learner identity: state=%+v err=%v", localIdentity, err)
	}
	learner := quepaxa.Member{
		ID: "fresh", PublicKey: learnerPublic, WALIdentity: localIdentity.identity.Nonce,
		PeerURL: "quic://" + learnerArchiveCatchupAddress(t),
	}
	learnerConfig.Learner = &learner
	learnerTransport := network.NewTransport(clusterID, learner.ID, &initial, learnerToken)
	learnerTransport.SetAdminToken(adminToken)
	t.Cleanup(func() { _ = learnerTransport.Close() })
	learnerCore, err := quepaxa.NewLearner(quepaxa.Config{
		NodeID: learner.ID, Cluster: initial, WAL: learnerWAL, Transport: learnerTransport, EnableReconfiguration: true,
	})
	if err != nil {
		t.Fatalf("create fresh learner core: %v", err)
	}
	learnerTransport.BindCore(learnerCore)
	learnerServer := network.NewServer(learnerCore, nil, clusterID, false, learnerTransport)
	learnerPeer, err := network.StartLearnerPeerServer(ctx, learner.PeerURL[len("quic://"):], learnerServer, bootstrap, learnerToken, adminToken, learner)
	if err != nil {
		t.Fatalf("start learner's authenticated peer server: %v", err)
	}
	t.Cleanup(func() {
		_ = learnerPeer.Close()
		learnerServer.Close()
	})
	learner.PeerURL = "quic://" + learnerPeer.Addr()

	request := network.MembershipChange{
		OperationID: "add-fresh-authenticated", ClusterID: clusterID, ExpectedConfigID: initial.ConfigID, Add: &learner,
	}
	if err := nodeA.changeMembership(ctx, request); err != nil {
		t.Fatalf("Node.changeMembership with real VerifyLearner admission: %v", err)
	}
	if got := nodeA.core.CurrentCluster(); got.ConfigID != initial.ConfigID+1 || len(got.Members) != 3 || got.Members[2] != learner {
		t.Fatalf("ADD was not committed with the authenticated learner identity: %+v", got)
	}
	if got, err := learnerCore.DurablePrefix(through); err != nil || got != wantPrefix {
		t.Fatalf("VerifyLearner did not establish exact durable prefix: got=%x want=%x err=%v", got, wantPrefix, err)
	}
	if learnerCore.IsVoter() {
		t.Fatal("learner became a voter before applying the committed ADD")
	}

	archiveReader := recovery.NewManager(bucket, clusterID, recovery.ArchiveConfigID)
	if err := archiveReader.Load(ctx); err != nil {
		t.Fatalf("load committed ADD archive: %v", err)
	}
	t.Cleanup(func() { archiveReader.Close() })
	learnerMaterial, err := materializer.Open(filepath.Join(t.TempDir(), "learner.sqlite"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = learnerMaterial.Close() })
	learnerNode := &Node{config: learnerConfig, core: learnerCore, archive: archiveReader, material: learnerMaterial}
	if err := learnerNode.catchUpArchive(ctx); err != nil {
		t.Fatalf("apply certified ADD to learner: %v", err)
	}
	if !learnerCore.IsVoter() || learnerCore.CurrentCluster().ConfigID != initial.ConfigID+1 {
		t.Fatalf("learner did not become voter after certified ADD: voter=%t cluster=%+v", learnerCore.IsVoter(), learnerCore.CurrentCluster())
	}
}
