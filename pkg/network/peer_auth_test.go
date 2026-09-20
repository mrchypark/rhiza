package network

import (
	"context"
	"crypto/ed25519"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestPeerAuthenticationMatrix(t *testing.T) {
	members := []quepaxa.Member{testMember("cluster", "a", "a-token"), testMember("cluster", "b", "b-token")}
	core := mustCore(t, "a", members, nil, nil)
	server := NewServer(core, nil, "cluster", true, nil)
	defer server.Close()
	peer := &PeerServer{server: server, adminToken: "admin"}
	for _, tc := range []struct {
		name, sender, adminToken, cluster string
		certificateKey                    string
		config                            uint64
		op                                peerfb.Operation
		want                              string
	}{
		{name: "voter", sender: "b", certificateKey: "b-token", cluster: "cluster", op: peerfb.OperationSync},
		{name: "bad-key", sender: "b", certificateKey: "wrong", cluster: "cluster", op: peerfb.OperationSync, want: "authentication failed"},
		{name: "unknown", sender: "removed", certificateKey: "b-token", cluster: "cluster", op: peerfb.OperationSync, want: "authentication failed"},
		{name: "no-certificate", sender: "b", cluster: "cluster", op: peerfb.OperationSync, want: "authentication failed"},
		{name: "admin-sync", sender: "learner", adminToken: "admin", cluster: "cluster", op: peerfb.OperationSync},
		{name: "admin-write", sender: "learner", adminToken: "admin", cluster: "cluster", op: peerfb.OperationRecord, want: "authentication failed"},
		{name: "admin-impersonation", sender: "b", adminToken: "admin", cluster: "cluster", op: peerfb.OperationRecord, want: "authentication failed"},
		{name: "wrong-cluster", sender: "b", certificateKey: "b-token", cluster: "other", op: peerfb.OperationSync, want: "identity mismatch"},
		{name: "wrong-config", sender: "b", certificateKey: "b-token", cluster: "cluster", config: 99, op: peerfb.OperationSync, want: "identity mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var certificateKey ed25519.PublicKey
			if tc.certificateKey != "" {
				certificateKey = PeerPublicKey("cluster", quepaxa.NodeID(tc.sender), tc.certificateKey)
			}
			_, err := peer.handle(context.Background(), certificateKey, &peerfb.RequestT{Operation: tc.op, ClusterId: tc.cluster, ConfigId: tc.config, SenderId: tc.sender, AdminToken: tc.adminToken, From: 1, Limit: 1})
			if tc.want == "" && err != nil || tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("error=%v, want %q", err, tc.want)
			}
		})
	}
	config := quepaxa.Cluster{Members: append(members, quepaxa.Member{ID: "a"})}
	for _, id := range []quepaxa.NodeID{"a", "b", "unknown"} {
		want, wantOK := config.MemberSet()[id]
		got, gotOK := peerMember(config, id)
		if got != want || gotOK != wantOK {
			t.Fatalf("lookup changed MemberSet semantics for %s", id)
		}
	}
}
