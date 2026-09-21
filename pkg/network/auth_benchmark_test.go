package network

import (
	"context"
	"fmt"
	"testing"

	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func BenchmarkPeerAuthentication(b *testing.B) {
	for _, count := range []int{3, 16} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			members := make([]quepaxa.Member, count)
			for i := range members {
				members[i] = testMember("cluster", quepaxa.NodeID(fmt.Sprint(i)), fmt.Sprintf("token-%d", i))
			}
			core := mustCore(b, members[0].ID, members, nil, nil)
			server := NewServer(core, nil, "cluster", true, nil)
			defer server.Close()
			peer := &PeerServer{server: server, adminToken: "admin"}
			certificateKey := PeerPublicKey("cluster", members[0].ID, "token-0")
			request := &peerfb.RequestT{Operation: peerfb.OperationSync, ClusterId: "cluster", ConfigId: uint64(core.ConfigID()), SenderId: string(members[0].ID), From: 1, Limit: 1}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := peer.handle(context.Background(), certificateKey, request); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
