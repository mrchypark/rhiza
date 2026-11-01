package network

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestLearnerPromotionGateBlocksOnlyAdmissionPrefix(t *testing.T) {
	config, values := catchUpValues(t, 2)
	source := startCatchUpPeer(t, "a", values, nil)
	config.Members[0] = source.member
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	pull := adminTransport("cluster", "learner", &config, "admin")
	defer pull.Close()
	core, err := quepaxa.NewLearner(quepaxa.Config{NodeID: "learner", Cluster: config, WAL: wal, Transport: pull, EnableReconfiguration: true})
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer(core, nil, "cluster", false, pull)
	defer server.Close()
	ready := make(chan struct{})
	learner := testMember("cluster", "learner", "learner-token")
	learner.WALIdentity = core.WALIdentity()
	peer, err := StartLearnerPeerServerWithPromotionGate(context.Background(), "127.0.0.1:0", server, config.Members, "learner-token", "admin", learner, ready)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	learner.PeerURL = "quic://" + peer.Addr()
	probe := NewTransport("cluster", "a", &config, "a-token")
	defer probe.Close()
	var prefix quepaxa.ValueHash
	for _, value := range values {
		prefix = quepaxa.AdvancePrefixHash(prefix, value.Slot, value.Hash)
	}
	blocked, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	err = probe.VerifyLearner(blocked, learner, 2, prefix)
	cancel()
	if err == nil || core.Tip() != 0 {
		t.Fatalf("VerifyPrefix bypassed promotion gate: tip=%d err=%v", core.Tip(), err)
	}
	close(ready)
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if err := probe.VerifyLearner(ctx, learner, 2, prefix); err != nil {
		t.Fatalf("VerifyPrefix did not resume after durable gate: %v", err)
	}
	if core.Tip() != 2 {
		t.Fatalf("learner did not persist verified prefix after gate: tip=%d", core.Tip())
	}
}

func TestCertifiedLearnReadinessDoesNotOpenClientAdmission(t *testing.T) {
	server := &Server{ready: func() bool { return false }}
	if server.readyToLearn() {
		t.Fatal("default learning bypassed ordinary readiness")
	}
	allowed := false
	server.SetLearnReady(func() bool { return allowed })
	if server.readyToLearn() {
		t.Fatal("closed peer authority admitted learning")
	}
	allowed = true
	if !server.readyToLearn() || server.Ready() {
		t.Fatal("certified delivery did not remain separate from client admission")
	}
}

func TestActualRecordedProposalLearnsCertificateBeforeSQLReadiness(t *testing.T) {
	cluster, values := catchUpValues(t, 1)
	var decision quepaxa.Decision
	if err := json.Unmarshal(values[0].Certificate, &decision); err != nil {
		t.Fatal(err)
	}
	decision.Proposal.Value = values[0].Value
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	core, err := quepaxa.New(quepaxa.Config{NodeID: "a", Cluster: cluster, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := core.Record(ctx, quepaxa.RecordRequest{ConfigID: cluster.ConfigID, Slot: decision.Slot, Step: decision.Step, Proposal: decision.Proposal}); err != nil {
		t.Fatal(err)
	}
	if core.RecorderTip() != 1 || core.Tip() != 0 {
		t.Fatal("fixture did not retain an undecided real record")
	}
	material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = material.Close() })
	server := NewServer(core, material, "cluster", false, nil, func() bool { return core.RecorderTip() <= core.Tip() })
	t.Cleanup(server.Close)
	server.SetLearnReady(func() bool { return true })
	peer, err := StartPeerServer(ctx, "127.0.0.1:0", server, cluster.Members, "a-token", "admin")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	cluster.Members[0].PeerURL = "quic://" + peer.Addr()
	probe := NewTransport("cluster", "a", &cluster, "a-token")
	t.Cleanup(func() { _ = probe.Close() })
	if server.Ready() {
		t.Fatal("unresolved recorder opened SQL readiness")
	}
	request := probe.request(peerfb.OperationLearned)
	request.Decision = decisionToWire(decision)
	if _, err := probe.callQuorum(ctx, "a", request); err != nil {
		t.Fatalf("valid certificate delivery blocked by undecided record: %v", err)
	}
	if core.Tip() != 1 || core.RecorderTip() != 1 {
		t.Fatal("certificate did not resolve original recorded slot")
	}
	if material.Tip() != 1 {
		t.Fatal("certified original slot was not replayed")
	}
	if err := core.EnsureDurableThrough(ctx, 1); err != nil {
		t.Fatal(err)
	}
}
