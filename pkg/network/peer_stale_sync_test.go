package network

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type peerAckTransport struct{}

func (peerAckTransport) SendRecord(_ context.Context, to quepaxa.NodeID, request quepaxa.RecordRequest) (quepaxa.Summary, error) {
	proposal := request.Proposal
	return quepaxa.Summary{RecorderID: to, ReconfigurationID: request.ReconfigurationID, Step: request.Step, FirstCurrent: &proposal, AggregatePrior: &proposal}, nil
}
func (peerAckTransport) SendDecision(context.Context, quepaxa.Decision) error { return nil }
func (peerAckTransport) ReadTip(context.Context, quepaxa.NodeID) (quepaxa.Slot, error) {
	return 0, nil
}
func (peerAckTransport) StageValue(context.Context, quepaxa.NodeID, quepaxa.ValueHash, []byte) error {
	return nil
}
func (peerAckTransport) FetchValue(context.Context, quepaxa.NodeID, quepaxa.ValueHash) ([]byte, error) {
	return nil, errors.New("value unavailable")
}

func TestStaleConfigSyncRequiresUnchangedCurrentVoterIdentity(t *testing.T) {
	ctx := context.Background()
	const cluster = "stale-sync"
	members := []quepaxa.Member{testMember(cluster, "a", "a-token"), testMember(cluster, "b", "b-token"), testMember(cluster, "c", "c-token")}
	wal, err := qlog.Open(t.TempDir() + "/qlog")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	core, err := quepaxa.New(quepaxa.Config{
		NodeID: "a", Cluster: quepaxa.Cluster{ConfigID: 1, Members: members}, WAL: wal,
		Transport: peerAckTransport{}, EnableReconfiguration: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	target := quepaxa.Cluster{ConfigID: 2, Members: append([]quepaxa.Member(nil), members[:2]...)}
	if _, err := core.BeginReconfiguration(ctx, target); err != nil {
		t.Fatalf("begin removal: %v", err)
	}
	if err := core.FinishReconfigurationAt(ctx, target, 0); err != nil {
		t.Fatalf("finish removal: %v", err)
	}
	if core.CurrentCluster().ConfigID != 2 || core.ClusterForSlot(1).ConfigID != 1 {
		t.Fatalf("fixture did not retain historical and current configurations: current=%d slot1=%d", core.CurrentCluster().ConfigID, core.ClusterForSlot(1).ConfigID)
	}
	peer := &PeerServer{server: NewServer(core, nil, cluster, true, nil), adminToken: "admin"}
	defer peer.server.Close()

	request := func(operation peerfb.Operation, sender string) *peerfb.RequestT {
		return &peerfb.RequestT{Operation: operation, ClusterId: cluster, ConfigId: 1, SenderId: sender, AdminToken: "admin", From: 1, Limit: 1}
	}
	for _, tc := range []struct {
		name      string
		operation peerfb.Operation
		sender    string
		token     string
		wantErr   string
	}{
		{name: "unchanged current voter", operation: peerfb.OperationSync, sender: "a", token: "a-token"},
		{name: "retired voter C cannot use admin token for stale sync", operation: peerfb.OperationSync, sender: "c", token: "c-token", wantErr: "identity mismatch"},
		{name: "nonmember D cannot use admin token for stale sync", operation: peerfb.OperationSync, sender: "d", token: "d-token", wantErr: "identity mismatch"},
		{name: "changed current voter key rejected", operation: peerfb.OperationSync, sender: "a", token: "a-token-v2", wantErr: "identity mismatch"},
		{name: "stale non-read request rejected", operation: peerfb.OperationReadIndex, sender: "a", token: "a-token", wantErr: "identity mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := request(tc.operation, tc.sender)
			response, err := peer.handle(ctx, PeerPublicKey(cluster, quepaxa.NodeID(tc.sender), tc.token), req)
			if tc.wantErr == "" && err != nil || tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("error=%v, want %q", err, tc.wantErr)
			}
			if tc.wantErr == "" && (response == nil || response.ConfigId != req.ConfigId) {
				t.Fatalf("response=%+v, want requested historical config %d", response, req.ConfigId)
			}
		})
	}
}
