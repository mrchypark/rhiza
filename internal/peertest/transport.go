// Package peertest binds a fixture-owned UDP transport to one peer context.
package peertest

import (
	"context"
	"errors"
	"net"
	"net/netip"

	"github.com/quic-go/quic-go"
)

type transportKey struct{}
type binding struct {
	cluster, node, endpoint string
	transport               *quic.Transport
}

// WithTransport preserves the parent's cancellation, deadline and values.
// The fixture owns the transport and socket through its final cleanup.
func WithTransport(ctx context.Context, cluster, node, endpoint string, transport *quic.Transport) context.Context {
	return context.WithValue(ctx, transportKey{}, binding{cluster, node, endpoint, transport})
}

// Transport returns an explicit binding error rather than falling back to bind.
func Transport(ctx context.Context, cluster, node, endpoint string) (*quic.Transport, bool, error) {
	b, present := ctx.Value(transportKey{}).(binding)
	if !present {
		return nil, false, nil
	}
	invalid := errors.New("peer fixture transport binding mismatch")
	address, err := netip.ParseAddrPort(endpoint)
	if err != nil || !address.Addr().IsLoopback() || address.Addr().Zone() != "" || address.Port() == 0 || cluster == "" || node == "" || b.cluster != cluster || b.node != node || b.endpoint != endpoint || b.transport == nil || b.transport.Conn == nil {
		return nil, true, invalid
	}
	actual, err := netip.ParseAddrPort(b.transport.Conn.LocalAddr().String())
	if err != nil || actual != address || b.transport.Conn.LocalAddr().Network() != "udp" {
		return nil, true, invalid
	}
	udp, ok := b.transport.Conn.(*net.UDPConn)
	if !ok {
		return nil, true, invalid
	}
	raw, err := udp.SyscallConn()
	if err != nil {
		return nil, true, invalid
	}
	if err := raw.Control(func(uintptr) {}); err != nil {
		return nil, true, invalid
	}
	return b.transport, true, nil
}
