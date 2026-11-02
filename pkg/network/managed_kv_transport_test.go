package network

import (
	"context"
	"crypto/tls"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/quic-go/quic-go"
)

// A real pinned QUIC endpoint exercises transport construction/handshake only;
// it is not the still-unmodified production mixed PeerServer or a receipt store.
func managedKVTransportEndpoint(t *testing.T, alpn string) (quepaxa.Member, <-chan peerfb.Operation) {
	t.Helper()
	member := testMember("cluster", "b", "b-token")
	cert, err := peerCertificate("cluster", "b", "b-token")
	if err != nil {
		t.Fatal(err)
	}
	listener, err := quic.ListenAddr("127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{alpn}, MinVersion: tls.VersionTLS13, ClientAuth: tls.RequestClientCert}, nil)
	if err != nil {
		t.Fatal(err)
	}
	member.PeerURL = "quic://" + listener.Addr().String()
	member.WALIdentity = strings.Repeat("01", 32)
	ctx, cancel := context.WithCancel(context.Background())
	operations := make(chan peerfb.Operation, 16)
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			conn, err := listener.Accept(ctx)
			if err != nil {
				return
			}
			wg.Go(func() {
				defer conn.CloseWithError(0, "test cleanup")
				for {
					stream, err := conn.AcceptStream(ctx)
					if err != nil {
						return
					}
					data, err := readPeerFrame(stream)
					if err != nil {
						stream.CancelRead(0)
						_ = stream.Close()
						return
					}
					req, err := decodePeerRequest(data)
					if err != nil {
						stream.CancelRead(0)
						_ = stream.Close()
						return
					}
					operations <- req.Operation
					response := &peerfb.ResponseT{ClusterId: "cluster", ProposerId: "b", ConfigId: req.ConfigId, Tip: req.From}
					_ = writePeerFrame(stream, encodePeerResponse(response))
					stream.CancelRead(0)
					_ = stream.Close()
				}
			})
		}
	})
	t.Cleanup(func() { cancel(); _ = listener.Close(); wg.Wait() })
	return member, operations
}

func TestManagedKVTransportPolicy(t *testing.T) {
	for _, bad := range []sqlpolicy.MaterializationPolicy{0, -1, 9} {
		if got, err := NewTransportForPolicy("cluster", "a", nil, "", bad); got != nil || !errors.Is(err, sqlpolicy.ErrIncompatible) {
			t.Fatal("unknown transport profile", got, err)
		}
		if got, err := NewLearnerTransportForPolicy("cluster", "a", 1, nil, "", bad); got != nil || !errors.Is(err, sqlpolicy.ErrIncompatible) {
			t.Fatal("unknown learner profile", got, err)
		}
	}
	config := quepaxa.Cluster{ConfigID: 1}
	legacy := NewTransport("cluster", "a", &config, "")
	defer legacy.Close()
	if legacy.expectedPolicy != 7 || len(legacy.tls.NextProtos) != 1 || legacy.tls.NextProtos[0] != peerALPN {
		t.Fatal("legacy transport changed")
	}
	mixed, err := NewTransportForPolicy("cluster", "a", &config, "", 8)
	if err != nil {
		t.Fatal(err)
	}
	defer mixed.Close()
	dynamic, err := mixed.transportFor(quepaxa.Cluster{ConfigID: 2})
	if err != nil {
		t.Fatal(err)
	}
	learner, err := NewLearnerTransportForPolicy("cluster", "l", 1, nil, "admin", 8)
	if err != nil {
		t.Fatal(err)
	}
	defer learner.Close()
	for _, tr := range []*Transport{mixed, dynamic, learner} {
		if tr.expectedPolicy != 8 || len(tr.tls.NextProtos) != 1 || tr.tls.NextProtos[0] != managedKVPeerALPN || tr.tls.MinVersion != tls.VersionTLS13 {
			t.Fatal("mixed profile/ALPN drift")
		}
	}
}

func TestManagedKVTransportHandshakeAndDerivedPaths(t *testing.T) {
	for _, policy := range []sqlpolicy.MaterializationPolicy{7, 8, sqlpolicy.ManagedRequestMaterializationPolicy} {
		alpn, _ := peerALPNForPolicy(policy)
		member, ops := managedKVTransportEndpoint(t, alpn)
		a := testMember("cluster", "a", "a-token")
		config := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{a, member}}
		tr, err := NewTransportForPolicy("cluster", "a", &config, "a-token", policy)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = tr.Close() })
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if _, err := tr.ReadTip(ctx, "b"); err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
		if <-ops != peerfb.OperationReadIndex {
			t.Fatal("wrong operation")
		}
		// Exercise the actual temporary union transport, not just a field clone.
		ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
		if err := tr.sendTerminalDecision(ctx, quepaxa.Decision{Slot: 1, ConfigID: 1}, config, config); err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
		if <-ops != peerfb.OperationLearned {
			t.Fatal("terminal union path not reached")
		}
		// Exercise actual VerifyLearner's separately constructed probe.
		origin := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{a}}
		probeParent, err := NewTransportForPolicy("cluster", "a", &origin, "a-token", policy)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
		err = probeParent.VerifyLearner(ctx, member, 3, quepaxa.ValueHash{1})
		cancel()
		_ = probeParent.Close()
		if err != nil {
			t.Fatal(err)
		}
		if <-ops != peerfb.OperationVerifyPrefix {
			t.Fatal("learner probe path not reached")
		}
		for _, other := range []sqlpolicy.MaterializationPolicy{7, 8, sqlpolicy.ManagedRequestMaterializationPolicy} {
			if other == policy {
				continue
			}
			mismatch, err := NewTransportForPolicy("cluster", "a", &config, "a-token", other)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel = context.WithTimeout(context.Background(), time.Second)
			_, err = mismatch.ReadTip(ctx, "b")
			cancel()
			_ = mismatch.Close()
			if err == nil {
				t.Fatal("mismatched ALPN connected")
			}
		}
	}
}
