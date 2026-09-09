package network

import (
	"context"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestPeerAuthenticationMatrix(t *testing.T) {
	members := []quepaxa.Member{{ID: "a", Token: "a-token"}, {ID: "b", Token: "b-token"}}
	core := mustCore(t, "a", members, nil, nil)
	server := NewServer(core, nil, "cluster", true, nil)
	defer server.Close()
	peer := &PeerServer{server: server, token: "admin"}
	for _, tc := range []struct {
		name, sender, token, cluster string
		config                       uint64
		op                           peerfb.Operation
		want                         string
	}{
		{"voter", "b", "b-token", "cluster", 0, peerfb.OperationSync, ""},
		{"bad-token", "b", "wrong", "cluster", 0, peerfb.OperationSync, "authentication failed"},
		{"unknown", "removed", "b-token", "cluster", 0, peerfb.OperationSync, "authentication failed"},
		{"learner", "learner", "admin", "cluster", 0, peerfb.OperationSync, ""},
		{"learner-write", "learner", "admin", "cluster", 0, peerfb.OperationRecord, "authentication failed"},
		{"learner-impersonation", "b", "admin", "cluster", 0, peerfb.OperationRecord, "authentication failed"},
		{"wrong-cluster", "b", "b-token", "other", 0, peerfb.OperationSync, "identity mismatch"},
		{"wrong-config", "b", "b-token", "cluster", 99, peerfb.OperationSync, "identity mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := peer.handle(context.Background(), nil, &peerfb.RequestT{Operation: tc.op, ClusterId: tc.cluster, ConfigId: tc.config, SenderId: tc.sender, Token: tc.token, From: 1, Limit: 1})
			if tc.want == "" && err != nil || tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("error=%v, want %q", err, tc.want)
			}
		})
	}
	config := quepaxa.Cluster{Members: append(members, quepaxa.Member{ID: "a", Token: "last"})}
	for _, id := range []quepaxa.NodeID{"a", "b", "unknown"} {
		want, wantOK := config.MemberSet()[id]
		got, gotOK := peerMember(config, id)
		if got != want || gotOK != wantOK {
			t.Fatalf("lookup changed MemberSet semantics for %s", id)
		}
	}
}
