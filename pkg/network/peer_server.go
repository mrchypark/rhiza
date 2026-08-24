package network

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/quic-go/quic-go"
)

// PeerServer owns the private QUIC listener. Public HTTP remains a separate adapter.
type PeerServer struct {
	listener *quic.EarlyListener
	server   *Server
	members  map[quepaxa.NodeID]quepaxa.Member
	token    string
}

func StartPeerServer(ctx context.Context, addr string, server *Server, members []quepaxa.Member, token string) (*PeerServer, error) {
	certificate, err := ephemeralCertificate()
	if err != nil {
		return nil, err
	}
	listener, err := quic.ListenAddrEarly(addr, &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{peerALPN}, Certificates: []tls.Certificate{certificate}}, &quic.Config{
		HandshakeIdleTimeout: 5 * time.Second, MaxIdleTimeout: 30 * time.Second, KeepAlivePeriod: 10 * time.Second,
		MaxIncomingStreams: 256, MaxIncomingUniStreams: -1, Allow0RTT: true,
	})
	if err != nil {
		return nil, err
	}
	peer := &PeerServer{listener: listener, server: server, members: make(map[quepaxa.NodeID]quepaxa.Member, len(members)), token: token}
	for _, member := range members {
		peer.members[member.ID] = member
	}
	go peer.serve(ctx)
	return peer, nil
}

func (s *PeerServer) Close() error { return s.listener.Close() }

func (s *PeerServer) Addr() string { return s.listener.Addr().String() }

func (s *PeerServer) serve(ctx context.Context) {
	go func() {
		<-ctx.Done()
		_ = s.Close()
	}()
	for {
		conn, err := s.listener.Accept(ctx)
		if err != nil {
			return
		}
		go s.serveConnection(ctx, conn)
	}
}

func (s *PeerServer) serveConnection(ctx context.Context, conn *quic.Conn) {
	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		go s.serveStream(conn, stream)
	}
}

func (s *PeerServer) serveStream(conn *quic.Conn, stream *quic.Stream) {
	defer stream.CancelRead(0)
	_ = stream.SetDeadline(time.Now().Add(30 * time.Second))
	response := &peerfb.ResponseT{}
	data, err := readPeerFrame(stream)
	if err == nil {
		var request *peerfb.RequestT
		request, err = decodePeerRequest(data)
		if err == nil {
			response, err = s.handle(stream.Context(), conn, request)
		}
	}
	if err != nil {
		response = &peerfb.ResponseT{Error: err.Error()}
		if errors.Is(err, quepaxa.ErrQuorumUnavailable) {
			response.ErrorCode = 1
		}
	}
	if writeErr := writePeerFrame(stream, encodePeerResponse(response)); writeErr != nil {
		stream.CancelWrite(1)
		return
	}
	_ = stream.Close()
}

func (s *PeerServer) handle(ctx context.Context, conn *quic.Conn, request *peerfb.RequestT) (*peerfb.ResponseT, error) {
	if request.ClusterId != string(s.server.cluster) || request.ConfigId != uint64(s.server.core.ConfigID()) {
		return nil, fmt.Errorf("cluster identity mismatch")
	}
	member, ok := s.members[quepaxa.NodeID(request.SenderId)]
	if !ok {
		return nil, fmt.Errorf("unknown peer %q", request.SenderId)
	}
	expectedToken := member.Token
	if expectedToken == "" {
		expectedToken = s.token
	}
	if request.Token != expectedToken {
		return nil, fmt.Errorf("peer authentication failed")
	}
	switch request.Operation {
	case peerfb.OperationRecord:
		if request.Record == nil {
			return nil, fmt.Errorf("record is required")
		}
		proposal, err := proposalFromWire(request.Record.Proposal)
		if err != nil {
			return nil, err
		}
		summary, err := s.server.core.Record(ctx, quepaxa.RecordRequest{Slot: quepaxa.Slot(request.Record.Slot), Step: quepaxa.Step(request.Record.Step), Proposal: proposal})
		if err != nil {
			return nil, err
		}
		return &peerfb.ResponseT{Summary: summaryToWire(summary)}, nil
	case peerfb.OperationPropose:
		if !conn.ConnectionState().TLS.HandshakeComplete {
			return nil, fmt.Errorf("propose is not accepted as replayable early data")
		}
		if len(request.Value) == 0 {
			return nil, fmt.Errorf("value is required")
		}
		decision, err := s.server.proposeLocal(ctx, request.Value)
		if err != nil {
			return nil, err
		}
		return &peerfb.ResponseT{Decided: decidedToWire(decision)}, nil
	case peerfb.OperationLearned:
		decision, err := decisionFromWire(request.Decision)
		if err != nil {
			return nil, err
		}
		if err := s.server.core.AcceptDecision(decision); err != nil {
			return nil, err
		}
		if err := s.server.applyDecisions(ctx, decision.Slot); err != nil {
			return nil, err
		}
		return &peerfb.ResponseT{}, nil
	case peerfb.OperationDecisions:
		if request.From == 0 || request.Limit == 0 || request.Limit > 256 {
			return nil, fmt.Errorf("invalid decisions range")
		}
		decisions, tip, err := s.server.core.DecisionsFrom(quepaxa.Slot(request.From), int(request.Limit))
		if err != nil {
			return nil, err
		}
		wireDecisions := make([]*peerfb.DecidedValueT, len(decisions))
		for i := range decisions {
			wireDecisions[i] = decidedToWire(decisions[i])
		}
		return &peerfb.ResponseT{ClusterId: string(s.server.cluster), ProposerId: string(s.server.core.NodeID()), ConfigId: uint64(s.server.core.ConfigID()), Tip: uint64(tip), Decisions: wireDecisions}, nil
	default:
		return nil, fmt.Errorf("unknown peer operation %d", request.Operation)
	}
}

func ephemeralCertificate() (tls.Certificate, error) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: "rhiza-peer"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: privateKey}, nil
}
