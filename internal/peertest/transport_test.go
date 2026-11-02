package peertest

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
)

func TestTransportBindingIsExactAndPreservesParent(t *testing.T) {
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	tr := &quic.Transport{Conn: conn}
	t.Cleanup(func() { _ = tr.Close() })
	type key struct{}
	deadline := time.Now().Add(time.Minute)
	parent, cancel := context.WithDeadline(context.WithValue(context.Background(), key{}, "retained"), deadline)
	t.Cleanup(cancel)
	address := conn.LocalAddr().String()
	ctx := WithTransport(parent, "cluster", "node", address, tr)
	if got, present, err := Transport(ctx, "cluster", "node", address); err != nil || !present || got != tr {
		t.Fatalf("binding: %v", err)
	}
	if got, _ := ctx.Deadline(); got != deadline || ctx.Value(key{}) != "retained" {
		t.Fatal("parent context changed")
	}
	for _, args := range [][3]string{{"wrong", "node", address}, {"cluster", "wrong", address}, {"cluster", "node", "127.0.0.1:1"}, {"cluster", "node", "localhost:1"}, {"cluster", "node", "0.0.0.0:1"}, {"cluster", "node", "127.0.0.1:0"}} {
		if _, present, err := Transport(ctx, args[0], args[1], args[2]); err == nil || !present {
			t.Fatal("mismatch silently accepted")
		}
	}
	if _, present, err := Transport(WithTransport(parent, "cluster", "node", address, nil), "cluster", "node", address); err == nil || !present {
		t.Fatal("nil binding accepted")
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if _, present, err := Transport(ctx, "cluster", "node", address); err == nil || !present {
		t.Fatal("closed socket accepted")
	}
	cancel()
	if ctx.Err() != context.Canceled {
		t.Fatal("parent cancellation lost")
	}
	if tr, present, err := Transport(context.Background(), "cluster", "node", address); err != nil || present || tr != nil {
		t.Fatal("uninjected context changed")
	}
}
