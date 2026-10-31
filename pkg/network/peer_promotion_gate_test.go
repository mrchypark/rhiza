package network

import (
	"context"
	"testing"
	"time"

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
