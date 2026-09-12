package network

import (
	"context"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestVerifyLearnerFetchesAndPersistsAdmissionPrefix(t *testing.T) {
	config, values := catchUpValues(t, 8)
	source := startCatchUpPeer(t, "a", values, nil)
	config.Members[0] = source.member
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	pull := NewTransport("cluster", "learner", &config, "admin")
	defer pull.Close()
	core, err := quepaxa.NewLearner(quepaxa.Config{NodeID: "learner", Cluster: config, WAL: wal, Transport: pull, EnableReconfiguration: true})
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer(core, nil, "cluster", false, pull)
	defer server.Close()
	learner := quepaxa.Member{ID: "learner", Token: "learner-token", WALIdentity: core.WALIdentity()}
	peer, err := StartLearnerPeerServer(ctx, "127.0.0.1:0", server, config.Members, "admin", learner)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	learner.PeerURL = "quic://" + peer.Addr()
	probe := NewTransport("cluster", "a", &config, "admin")
	defer probe.Close()
	var prefix quepaxa.ValueHash
	for _, value := range values {
		prefix = quepaxa.AdvancePrefixHash(prefix, value.Slot, value.Hash)
	}
	if core.Tip() != 0 {
		t.Fatal("learner unexpectedly caught up")
	}
	if err := probe.VerifyLearner(ctx, learner, 8, prefix); err != nil {
		t.Fatal(err)
	}
	got, err := core.DurablePrefix(8)
	if err != nil || got != prefix {
		t.Fatalf("admitted without durable prefix: %x %v", got, err)
	}
	if core.IsVoter() {
		t.Fatal("prefix check granted voting authority")
	}
	wrong := learner
	wrong.WALIdentity = "0000000000000000000000000000000000000000000000000000000000000000"
	if err := probe.VerifyLearner(ctx, wrong, 8, prefix); err == nil {
		t.Fatal("accepted wrong incarnation")
	}
	if err := probe.VerifyLearner(ctx, learner, 8, quepaxa.ValueHash{9}); err == nil {
		t.Fatal("accepted wrong prefix")
	}
}
