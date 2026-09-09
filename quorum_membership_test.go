package rhiza_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// Exercise the public Core and real authenticated QUIC path together. This
// deliberately does not use Node's archive or claim no-PVC automatic recovery.
func TestQuorumMembershipQUIC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const admin = "membership-test-admin"
	members := make([]quepaxa.Member, 4)
	reserved := make([]net.PacketConn, len(members))
	t.Cleanup(func() {
		for _, listener := range reserved {
			if listener != nil {
				_ = listener.Close()
			}
		}
	})
	for i := range members {
		listener, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		members[i] = quepaxa.Member{ID: quepaxa.NodeID(fmt.Sprintf("v%d", i)), PeerURL: "quic://" + listener.LocalAddr().String(), Token: fmt.Sprintf("membership-test-voter-%d", i)}
		reserved[i] = listener
	}
	genesis := quepaxa.Cluster{ConfigID: 1, Members: members[:3]}
	type peer struct {
		core      *quepaxa.Core
		transport *network.Transport
		server    *network.Server
		listener  *network.PeerServer
		material  *materializer.Materializer
	}
	peers := make([]*peer, 4)
	var learnerMu sync.Mutex
	syncLearner := func(through quepaxa.Slot) error {
		learner := peers[3]
		for learner.core.Tip() < through {
			batch, err := learner.transport.FetchDecisions(ctx, members[0].ID, learner.core.Tip()+1, 128)
			if err != nil {
				return err
			}
			if len(batch.Decisions) == 0 {
				return fmt.Errorf("learner prefix has a gap")
			}
			if err := learner.core.AcceptCertifiedValues(batch.Decisions); err != nil {
				return err
			}
			if err := learner.material.ApplyBatch(ctx, batch.Decisions); err != nil {
				return err
			}
		}
		return nil
	}
	start := func(index int, learner bool) {
		dir := t.TempDir()
		wal, err := qlog.Open(filepath.Join(dir, "wal"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		transport := network.NewTransport("membership-test", members[index].ID, &genesis, admin)
		t.Cleanup(func() { _ = transport.Close() })
		config := quepaxa.Config{NodeID: members[index].ID, Cluster: genesis, WAL: wal, Transport: transport, EnableReconfiguration: true}
		config.ReconfigurationAdmission = func(callCtx context.Context, _ quepaxa.Cluster, through quepaxa.Slot, prefix [32]byte) error {
			learnerMu.Lock()
			defer learnerMu.Unlock()
			if peers[3] == nil {
				return fmt.Errorf("learner is absent")
			}
			if err := syncLearner(through); err != nil {
				return err
			}
			return transport.VerifyLearner(callCtx, members[3], through, prefix)
		}
		var core *quepaxa.Core
		if learner {
			core, err = quepaxa.NewLearner(config)
		} else {
			core, err = quepaxa.New(config)
		}
		if err != nil {
			t.Fatal(err)
		}
		transport.BindCore(core)
		if learner {
			members[index].WALIdentity = core.WALIdentity()
		}
		material, err := materializer.Open(filepath.Join(dir, "db.sqlite"), 2)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = material.Close() })
		server := network.NewServer(core, material, "membership-test", true, transport)
		t.Cleanup(server.Close)
		addr := members[index].PeerURL[len("quic://"):]
		if reserved[index] != nil {
			if err := reserved[index].Close(); err != nil {
				t.Fatal(err)
			}
			reserved[index] = nil
		}
		var listener *network.PeerServer
		if learner {
			listener, err = network.StartLearnerPeerServer(ctx, addr, server, genesis.Members, admin, members[index])
		} else {
			listener, err = network.StartPeerServer(ctx, addr, server, genesis.Members, admin)
		}
		if err != nil {
			t.Fatal(err)
		}
		p := &peer{core: core, transport: transport, material: material, server: server, listener: listener}
		peers[index] = p
		t.Cleanup(func() {
			if p.listener != nil {
				_ = p.listener.Close()
			}
		})
	}
	for i := 0; i < 3; i++ {
		start(i, false)
	}
	execute := func(index int, id, sql string) {
		t.Helper()
		if _, err := peers[index].server.Execute(ctx, network.ExecuteRequest{RequestID: id, SQL: sql}); err != nil {
			t.Fatalf("%s: %v", id, err)
		}
	}
	execute(0, "create", "CREATE TABLE recovered (id INTEGER PRIMARY KEY)")
	execute(0, "before", "INSERT INTO recovered VALUES (1)")
	for i := 1; i <= 64; i++ {
		var nonce [quepaxa.ReadBarrierNonceSize]byte
		nonce[0] = byte(i)
		if _, _, err := peers[0].core.ProposeCertified(ctx, quepaxa.EncodeReadBarrier(nonce)); err != nil {
			t.Fatalf("certified pipeline stalled at write %d: %v", i, err)
		}
	}
	if err := peers[2].listener.Close(); err != nil {
		t.Fatal(err)
	}
	peers[2].listener = nil
	survivors := quepaxa.Cluster{ConfigID: 2, Members: members[:2]}
	if _, err := peers[0].core.BeginReconfiguration(ctx, survivors); err != nil {
		t.Fatal(err)
	}
	if err := peers[0].core.FinishReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	execute(0, "degraded", "INSERT INTO recovered VALUES (2)")
	start(3, true)
	if err := syncLearner(peers[0].core.Tip()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := peers[3].core.Propose(ctx, []byte("INSERT INTO recovered VALUES (99)")); err == nil {
		t.Fatal("unpromoted learner proposed a write")
	}
	if err := peers[0].transport.VerifyLearner(ctx, members[3], peers[0].core.Tip(), quepaxa.ValueHash{1}); err == nil {
		t.Fatal("learner accepted an incorrect prefix")
	}
	prefix, ok := peers[0].core.PrefixHash(peers[0].core.Tip())
	if !ok {
		t.Fatal("current prefix is unavailable")
	}
	wrongIncarnation := members[3]
	first := byte('0')
	if wrongIncarnation.WALIdentity[0] == first {
		first = '1'
	}
	wrongIncarnation.WALIdentity = string(first) + wrongIncarnation.WALIdentity[1:]
	if err := peers[0].transport.VerifyLearner(ctx, wrongIncarnation, peers[0].core.Tip(), prefix); err == nil {
		t.Fatal("learner accepted an incorrect WAL identity")
	}
	replacement := quepaxa.Cluster{ConfigID: 3, Members: []quepaxa.Member{members[0], members[1], members[3]}}
	if _, err := peers[0].core.BeginReconfiguration(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	if err := peers[0].core.FinishReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	if err := syncLearner(peers[0].core.Tip()); err != nil {
		t.Fatal(err)
	}
	if err := peers[1].listener.Close(); err != nil {
		t.Fatal(err)
	}
	peers[1].listener = nil
	execute(0, "replacement", "INSERT INTO recovered VALUES (3)")
	result, err := peers[0].server.Query(ctx, network.QueryRequest{SQL: "SELECT id FROM recovered ORDER BY id"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 3 {
		t.Fatalf("rows=%v", result.Rows)
	}
	if peers[3].core.CurrentCluster().ConfigID != 3 {
		t.Fatal("learner did not learn promotion")
	}
	staleCtx, staleCancel := context.WithTimeout(ctx, time.Second)
	defer staleCancel()
	if _, err := peers[2].transport.ReadTip(staleCtx, members[0].ID); err == nil {
		t.Fatal("removed voter retained quorum read authority")
	}
	if err := peers[3].listener.Close(); err != nil {
		t.Fatal(err)
	}
	peers[3].listener = nil
	start(3, true) // Same peer ID/token, but a freshly created WAL incarnation.
	if err := syncLearner(peers[0].core.Tip()); err != nil {
		t.Fatal(err)
	}
	if peers[3].core.IsVoter() {
		t.Fatal("fresh WAL reused the old incarnation's promotion")
	}
	probe := network.NewTransport("membership-test", members[0].ID, &replacement, admin)
	t.Cleanup(func() { _ = probe.Close() })
	probe.BindCore(peers[0].core)
	if batch, err := probe.FetchDecisions(ctx, members[3].ID, 1, 1); err != nil || len(batch.Decisions) != 1 {
		t.Fatalf("fresh peer is not serving its restored history: %v", err)
	}
	value := []byte("stale-incarnation-vote")
	var priority quepaxa.Priority
	for i := range priority {
		priority[i] = 0xff
	}
	_, err = probe.SendRecord(ctx, members[3].ID, quepaxa.RecordRequest{
		Slot: peers[0].core.Tip() + 1, Step: 4, ConfigID: 3,
		Proposal: quepaxa.Proposal{Priority: priority, ProposerID: members[0].ID, Hash: sha256.Sum256(value), Value: value},
	})
	if err == nil {
		t.Fatal("fresh WAL voted through QUIC using the old incarnation's promotion")
	}
}
