package managedsql

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestActualManagedPeerLearnRequiresLiveAuthority(t *testing.T) {
	configs, hosts, fixture := openSQLQuorum(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := hosts[0].Execute(ctx, ExecuteRequest{RequestID: "peer-learn-create", SQL: "CREATE TABLE peer_learn_test(value)"}); err != nil {
		t.Fatal(err)
	}
	cfg := configs[0].Runtime
	cluster := quepaxa.Cluster{ConfigID: 1, Members: cfg.Members}
	transport := network.NewTransport(cfg.ClusterID, cfg.NodeID, &cluster, cfg.PeerToken)
	t.Cleanup(func() { _ = transport.Close() })
	page, err := transport.FetchDecisions(ctx, cfg.Members[1].ID, 1, 1)
	if err != nil || len(page.Decisions) != 1 {
		t.Fatalf("actual certified decision: %v", err)
	}
	value := page.Decisions[0]
	var decision quepaxa.Decision
	if err := json.Unmarshal(value.Certificate, &decision); err != nil {
		t.Fatal(err)
	}
	decision.Proposal.Value = value.Value
	if err := transport.SendDecision(ctx, decision); err != nil {
		t.Fatalf("valid duplicate certificate rejected before authority change: %v", err)
	}
	before, err := hosts[1].node.ManagedState()
	if err != nil {
		t.Fatal(err)
	}
	fixture.mu.Lock()
	saved := append(json.RawMessage(nil), fixture.obj["status"]...)
	var status map[string]json.RawMessage
	if err := json.Unmarshal(saved, &status); err != nil {
		fixture.mu.Unlock()
		t.Fatal(err)
	}
	delete(status, "authority")
	fixture.obj["status"], err = json.Marshal(status)
	fixture.mu.Unlock()
	defer func() { fixture.mu.Lock(); fixture.obj["status"] = saved; fixture.mu.Unlock() }()
	if err != nil {
		t.Fatal(err)
	}
	if err := transport.SendDecision(ctx, decision); err == nil {
		t.Fatal("actual QUIC Learned listener accepted a certificate without live authority")
	}
	after, err := hosts[1].node.ManagedState()
	if err != nil || after.CertifiedTip != before.CertifiedTip || after.AppliedTip != before.AppliedTip {
		t.Fatalf("rejected decision changed runtime state: before=%+v after=%+v err=%v", before, after, err)
	}
}
