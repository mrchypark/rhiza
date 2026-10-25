package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	thanosobjstore "github.com/thanos-io/objstore"
)

func TestLearnerCatchUpRefreshesArchiveAfterMissedMembershipPublication(t *testing.T) {
	ctx := context.Background()
	fixture := newLearnerArchiveCatchupFixture(t, true)
	defer fixture.close()

	if err := fixture.learner.catchUpMembership(ctx, fixture.learnerTransport, &fixture.initial); err == nil {
		t.Fatal("learner synchronized stale peers before the membership transition was published")
	}
	if got := fixture.learner.core.CurrentCluster().ConfigID; got != fixture.initial.ConfigID {
		t.Fatalf("learner applied unpublished membership: config=%d, want %d", got, fixture.initial.ConfigID)
	}

	through := fixture.voters["a"].Tip()
	if err := fixture.publisher.SyncThrough(ctx, fixture.voters["a"], through); err != nil {
		t.Fatalf("publish certified membership after delay: %v", err)
	}
	if err := fixture.learner.catchUpMembership(ctx, fixture.learnerTransport, &fixture.initial); err != nil {
		t.Fatalf("same learner did not refresh archive and reselect current peers: %v", err)
	}
	current := fixture.learner.core.CurrentCluster()
	if current.ConfigID != fixture.target.ConfigID || !reflect.DeepEqual(current.Members, fixture.target.Members) {
		t.Fatalf("learner current membership=%+v, want certified target %+v", current, fixture.target)
	}
	if fixture.learner.core.Tip() != through || fixture.learner.material.Tip() != uint64(through) {
		t.Fatalf("learner did not apply certified prefix: core=%d material=%d want=%d", fixture.learner.core.Tip(), fixture.learner.material.Tip(), through)
	}
}

func TestLearnerCatchUpRejectsInvalidCertifiedArchiveHistory(t *testing.T) {
	ctx := context.Background()
	fixture := newLearnerArchiveCatchupFixture(t, false)
	defer fixture.close()

	if _, _, err := fixture.voters["a"].Propose(ctx, []byte("not an execution policy")); err != nil {
		t.Fatalf("prepare certified but invalid execution history: %v", err)
	}
	through := fixture.voters["a"].Tip()
	if err := fixture.publisher.SyncThrough(ctx, fixture.voters["a"], through); err != nil {
		t.Fatalf("publish invalid test history: %v", err)
	}
	if err := fixture.learner.catchUpMembership(ctx, fixture.learnerTransport, &fixture.initial); err == nil {
		t.Fatal("learner accepted archive history that violates execution policy")
	}
	if got := fixture.learner.core.CurrentCluster().ConfigID; got != fixture.initial.ConfigID {
		t.Fatalf("invalid archive changed learner configuration to %d", got)
	}
	if got := fixture.learner.core.Tip(); got != 0 {
		t.Fatalf("invalid archive advanced learner tip to %d", got)
	}
}

type learnerArchiveCatchupFixture struct {
	initial          quepaxa.Cluster
	target           quepaxa.Cluster
	voters           map[quepaxa.NodeID]*quepaxa.Core
	transport        *membershipOperationTransport
	learner          *Node
	learnerTransport *network.Transport
	publisher        *recovery.Manager
	peerServers      []*network.PeerServer
	servers          []*network.Server
	materials        []*materializer.Materializer
	wals             []*qlog.WAL
}

func newLearnerArchiveCatchupFixture(t *testing.T, advanceMembership bool) *learnerArchiveCatchupFixture {
	t.Helper()
	ctx := context.Background()
	const clusterID = "learner-archive-catchup"
	const adminToken = "learner-archive-admin-token"
	ids := []quepaxa.NodeID{"a", "b", "c"}
	tokens := map[quepaxa.NodeID]string{"a": "learner-archive-a", "b": "learner-archive-b", "c": "learner-archive-c"}
	members := make([]quepaxa.Member, len(ids))
	addresses := make(map[quepaxa.NodeID]string, len(ids))
	for i, id := range ids {
		members[i] = peerMember(clusterID, id, tokens[id])
		members[i].PeerURL = "quic://" + learnerArchiveCatchupAddress(t)
		addresses[id] = members[i].PeerURL
	}

	transport := &membershipOperationTransport{cores: make(map[quepaxa.NodeID]*quepaxa.Core), disabled: make(map[quepaxa.NodeID]bool)}
	fixture := &learnerArchiveCatchupFixture{
		voters: transport.cores, transport: transport, wals: make([]*qlog.WAL, 0, len(ids)+1),
	}
	complete := false
	defer func() {
		if !complete {
			fixture.close()
		}
	}()
	voterWALs := make(map[quepaxa.NodeID]*qlog.WAL, len(ids))
	for _, id := range ids {
		wal, err := qlog.Open(filepath.Join(t.TempDir(), "qlog"))
		if err != nil {
			t.Fatal(err)
		}
		fixture.wals = append(fixture.wals, wal)
		identity := sha256.Sum256([]byte("learner-archive-voter:" + string(id)))
		if err := wal.BindIdentity(identity[:]); err != nil {
			t.Fatalf("bind voter %s WAL identity: %v", id, err)
		}
		for i := range members {
			if members[i].ID == id {
				members[i].WALIdentity = hex.EncodeToString(identity[:])
				break
			}
		}
		voterWALs[id] = wal
		transport.ids = append(transport.ids, id)
	}
	fixture.initial = quepaxa.Cluster{ConfigID: 1, Members: append([]quepaxa.Member(nil), members...)}
	for _, id := range ids {
		core, err := quepaxa.New(quepaxa.Config{NodeID: id, Cluster: quepaxa.Cluster{ConfigID: 1, Members: members}, WAL: voterWALs[id], Transport: transport, EnableReconfiguration: true})
		if err != nil {
			t.Fatalf("create voter %s: %v", id, err)
		}
		transport.cores[id] = core
	}

	if advanceMembership {
		fixture.target = quepaxa.Cluster{ConfigID: 2, Members: append([]quepaxa.Member(nil), members[:2]...)}
		if _, err := fixture.voters["a"].BeginReconfiguration(ctx, fixture.target); err != nil {
			t.Fatalf("begin membership transition: %v", err)
		}
		if err := fixture.voters["a"].FinishReconfiguration(ctx); err != nil {
			t.Fatalf("finish membership transition: %v", err)
		}
	} else {
		fixture.target = fixture.initial
	}

	bucket := thanosobjstore.NewInMemBucket()
	fixture.publisher = recovery.NewManager(bucket, "cluster", recovery.ArchiveConfigID)
	t.Cleanup(func() { fixture.publisher.Close() })
	reader := recovery.NewManager(bucket, "cluster", recovery.ArchiveConfigID)
	t.Cleanup(func() { reader.Close() })
	if err := reader.Load(ctx); err != nil {
		t.Fatalf("load initially unpublished archive: %v", err)
	}

	learnerWAL, err := qlog.Open(filepath.Join(t.TempDir(), "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	fixture.wals = append(fixture.wals, learnerWAL)
	learnerID := quepaxa.NodeID("learner")
	learnerMember := peerMember(clusterID, learnerID, "learner-archive-local-token")
	learnerTransport := network.NewTransport(clusterID, learnerID, &fixture.initial, "learner-archive-local-token")
	learnerTransport.SetAdminToken(adminToken)
	fixture.learnerTransport = learnerTransport
	learnerCore, err := quepaxa.NewLearner(quepaxa.Config{NodeID: learnerID, Cluster: fixture.initial, WAL: learnerWAL, Transport: learnerTransport, EnableReconfiguration: true})
	if err != nil {
		t.Fatalf("create learner core: %v", err)
	}
	learnerMaterial, err := materializer.Open(filepath.Join(t.TempDir(), "learner.sqlite"), 1)
	if err != nil {
		t.Fatal(err)
	}
	fixture.materials = append(fixture.materials, learnerMaterial)
	learnerTransport.BindCore(learnerCore)
	fixture.learner = &Node{
		config: &types.ExecutionConfig{ClusterID: types.ClusterID(clusterID), NodeID: types.NodeID(learnerID), EnableReconfiguration: true,
			AdminToken: adminToken, Members: append([]quepaxa.Member(nil), members...), Learner: &learnerMember},
		core: learnerCore, material: learnerMaterial, archive: reader,
	}

	if advanceMembership {
		for _, id := range []quepaxa.NodeID{"a", "b"} {
			material, err := materializer.Open(filepath.Join(t.TempDir(), fmt.Sprintf("%s.sqlite", id)), 1)
			if err != nil {
				t.Fatal(err)
			}
			fixture.materials = append(fixture.materials, material)
			server := network.NewServer(fixture.voters[id], material, types.ClusterID(clusterID), false, nil)
			fixture.servers = append(fixture.servers, server)
			peer, err := network.StartPeerServer(ctx, addresses[id][len("quic://"):], server, fixture.target.Members, tokens[id], adminToken)
			if err != nil {
				t.Fatalf("start current voter %s peer server: %v", id, err)
			}
			fixture.peerServers = append(fixture.peerServers, peer)
		}
	}
	complete = true
	return fixture
}

func (f *learnerArchiveCatchupFixture) close() {
	if f.learnerTransport != nil {
		_ = f.learnerTransport.Close()
	}
	for _, peer := range f.peerServers {
		_ = peer.Close()
	}
	for _, server := range f.servers {
		server.Close()
	}
	for _, material := range f.materials {
		_ = material.Close()
	}
	for _, wal := range f.wals {
		_ = wal.Close()
	}
}

func learnerArchiveCatchupAddress(t *testing.T) string {
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
