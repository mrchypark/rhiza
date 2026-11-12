package network

import (
	"context"
	"crypto/tls"
	"io"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
)

func TestPeerQUICConfigRetiresIdleConnectionAndRedials(t *testing.T) {
	const idleTimeout = 250 * time.Millisecond
	certificate, err := peerCertificate("cluster", "n1", "secret")
	if err != nil {
		t.Fatal(err)
	}
	listener, err := quic.ListenAddr("127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{certificate},
		NextProtos:   []string{peerALPN},
		MinVersion:   tls.VersionTLS13,
	}, newPeerQUICConfig(idleTimeout))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer func() {
		cancel()
		_ = listener.Close()
	}()

	accepted := make(chan *quic.Conn, 2)
	go func() {
		for {
			conn, err := listener.Accept(ctx)
			if err != nil {
				return
			}
			accepted <- conn
		}
	}()
	dial := func() *quic.Conn {
		t.Helper()
		conn, err := quic.DialAddr(ctx, listener.Addr().String(), &tls.Config{
			InsecureSkipVerify: true,
			NextProtos:         []string{peerALPN},
			MinVersion:         tls.VersionTLS13,
		}, newPeerQUICConfig(idleTimeout))
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-conn.HandshakeComplete():
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		return conn
	}

	first := dial()
	firstServer := <-accepted
	select {
	case <-first.Context().Done():
	case <-firstServer.Context().Done():
	case <-time.After(2 * time.Second):
		t.Fatal("idle peer connection remained open")
	}

	second := dial()
	defer second.CloseWithError(0, "test complete")
	secondServer := <-accepted
	defer secondServer.CloseWithError(0, "test complete")
	stream, err := second.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte{0x5a}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	serverStream, err := secondServer.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 1)
	if _, err := io.ReadFull(serverStream, got); err != nil || got[0] != 0x5a {
		t.Fatalf("redialed stream payload=%x error=%v", got, err)
	}
	if _, err := serverStream.Write([]byte{0xa5}); err != nil {
		t.Fatal(err)
	}
	if err := serverStream.Close(); err != nil {
		t.Fatal(err)
	}
	got = make([]byte, 1)
	if _, err := io.ReadFull(stream, got); err != nil || got[0] != 0xa5 {
		t.Fatalf("redialed stream response=%x error=%v", got, err)
	}
}
