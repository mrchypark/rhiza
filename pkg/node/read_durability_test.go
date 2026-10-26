package node

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestManagedReadBarrierActivatesWhenLearnerBecomesVoter(t *testing.T) {
	var voter atomic.Bool
	var calls atomic.Int32
	barrier := voterReadDurabilityBarrier(voter.Load, func(_ context.Context, slot quepaxa.Slot) error {
		if slot != 7 {
			t.Errorf("barrier slot=%d, want 7", slot)
		}
		calls.Add(1)
		return nil
	})

	if err := barrier(context.Background(), 7); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("learner read unexpectedly waited on voter durability")
	}

	// The same registered callback observes role changes; promotion does not
	// require mutating a live HTTP handler or restarting the node.
	voter.Store(true)
	if err := barrier(context.Background(), 7); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("promoted voter durability calls=%d, want 1", calls.Load())
	}
}

func TestManagedReadBarrierTracksCommittedCorePromotion(t *testing.T) {
	ctx := context.Background()
	sourceMember := peerMember("cluster", "source", "source-token")
	initial := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{sourceMember}}

	learnerWAL, err := qlog.Open(t.TempDir() + "/learner-qlog")
	if err != nil {
		t.Fatal(err)
	}
	defer learnerWAL.Close()
	learner, err := quepaxa.NewLearner(quepaxa.Config{
		NodeID: "learner", Cluster: initial, WAL: learnerWAL, EnableReconfiguration: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	sourceWAL, err := qlog.Open(t.TempDir() + "/source-qlog")
	if err != nil {
		t.Fatal(err)
	}
	defer sourceWAL.Close()
	source, err := quepaxa.New(quepaxa.Config{
		NodeID: "source", Cluster: initial, WAL: sourceWAL, EnableReconfiguration: true,
		ReconfigurationAdmission: func(context.Context, quepaxa.ReconfigurationAdmission) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}

	target := quepaxa.Cluster{ConfigID: 2, Members: []quepaxa.Member{
		sourceMember,
		{ID: "learner", PublicKey: peerMember("cluster", "learner", "learner-token").PublicKey, PeerURL: "https://learner", WALIdentity: learner.WALIdentity()},
	}}
	if _, err := source.BeginReconfiguration(ctx, target); err != nil {
		t.Fatalf("begin learner promotion: %v", err)
	}
	if err := source.FinishReconfiguration(ctx); err != nil {
		t.Fatalf("commit learner promotion: %v", err)
	}
	decisions, _, err := source.DecisionsFrom(1, 128)
	if err != nil {
		t.Fatal(err)
	}

	material, err := materializer.Open(t.TempDir()+"/learner.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := network.NewServer(learner, material, "cluster", true, nil)
	defer server.Close()
	var calls int
	server.SetReadDurabilityBarrier(voterReadDurabilityBarrier(learner.IsVoter, func(context.Context, quepaxa.Slot) error {
		calls++
		return nil
	}))
	if learner.IsVoter() {
		t.Fatal("new learner started as voter")
	}
	if _, err := server.Query(ctx, network.QueryRequest{SQL: "SELECT 1", Consistency: "local"}); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatal("learner callback unexpectedly entered voter durability barrier")
	}

	if err := learner.AcceptCertifiedValues(decisions); err != nil {
		t.Fatalf("apply committed promotion to live learner: %v", err)
	}
	if !learner.IsVoter() {
		t.Fatalf("learner did not become voter after committed configuration: %+v", learner.CurrentCluster())
	}
	if _, err := server.Query(ctx, network.QueryRequest{SQL: "SELECT 1", Consistency: "local"}); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("promoted core durability calls=%d, want 1", calls)
	}
}
