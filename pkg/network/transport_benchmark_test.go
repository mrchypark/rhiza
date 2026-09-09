package network

import (
	"context"
	"testing"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func BenchmarkBoundTransportQUICReadTip(b *testing.B) {
	members := []quepaxa.Member{{ID: "a", Token: "a-token"}, {ID: "b", Token: "b-token"}, {ID: "c", Token: "c-token"}}
	source := mustCore(b, "a", members, nil, nil)
	server := NewServer(source, nil, "cluster", true, nil)
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	peer, err := StartPeerServer(ctx, "127.0.0.1:0", server, members, "admin")
	if err != nil {
		b.Fatal(err)
	}
	defer peer.Close()
	members[0].PeerURL = "quic://" + peer.Addr()
	core := mustCore(b, "b", members, nil, nil)
	config := core.CurrentCluster()
	transport := NewTransport("cluster", "b", &config, "admin")
	defer transport.Close()
	transport.BindCore(core)
	for range 10 {
		if _, err := transport.ReadTip(ctx, "a"); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := transport.ReadTip(ctx, "a"); err != nil {
			b.Fatal(err)
		}
	}
}
