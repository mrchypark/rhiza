package network

import (
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/quic-go/quic-go"
)

const (
	maxPeerConnections = 64
	maxPeerStreams     = 1024
	peerErrorQuorum    = 1
	peerErrorCompacted = 2
	peerErrorRetryable = 3
)

// PeerServer owns the private QUIC listener. Public HTTP remains a separate adapter.
type PeerServer struct {
	listener    *quic.EarlyListener
	server      *Server
	members     map[quepaxa.NodeID]quepaxa.Member
	token       string
	connections chan struct{}
	streams     chan struct{}
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

func StartPeerServer(ctx context.Context, addr string, server *Server, members []quepaxa.Member, token string) (*PeerServer, error) {
	return startPeerServer(ctx, server, members, token, func(tlsConfig *tls.Config, config *quic.Config) (*quic.EarlyListener, error) {
		return quic.ListenAddrEarly(addr, tlsConfig, config)
	})
}

// StartPeerServerOnTransport serves on an already-bound QUIC transport. The
// caller owns the transport and its UDP socket; closing this PeerServer leaves
// them available for a subsequent listener without releasing the port.
func StartPeerServerOnTransport(ctx context.Context, transport *quic.Transport, server *Server, members []quepaxa.Member, token string) (*PeerServer, error) {
	if transport == nil {
		return nil, fmt.Errorf("peer QUIC transport is required")
	}
	return startPeerServer(ctx, server, members, token, transport.ListenEarly)
}

func startPeerServer(ctx context.Context, server *Server, members []quepaxa.Member, token string, listen func(*tls.Config, *quic.Config) (*quic.EarlyListener, error)) (*PeerServer, error) {
	identityToken := token
	for _, member := range members {
		if len(members) > 1 && member.Token == "" {
			return nil, fmt.Errorf("voter token is required for %q", member.ID)
		}
		if token != "" && member.Token != "" && subtle.ConstantTimeCompare([]byte(token), []byte(member.Token)) == 1 {
			return nil, fmt.Errorf("admin token must differ from voter token for %q", member.ID)
		}
		if member.ID == server.core.NodeID() && member.Token != "" {
			identityToken = member.Token
		}
	}
	if identityToken == "" && len(members) > 1 {
		return nil, fmt.Errorf("peer identity token is required")
	}
	certificate, err := peerCertificate(server.cluster, server.core.NodeID(), identityToken)
	if err != nil {
		return nil, err
	}
	listener, err := listen(&tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{peerALPN}, Certificates: []tls.Certificate{certificate}}, &quic.Config{
		HandshakeIdleTimeout: 5 * time.Second, MaxIdleTimeout: 30 * time.Second, KeepAlivePeriod: 10 * time.Second,
		MaxIncomingStreams: 256, MaxIncomingUniStreams: -1, Allow0RTT: true,
	})
	if err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	peer := &PeerServer{
		listener: listener, server: server, members: make(map[quepaxa.NodeID]quepaxa.Member, len(members)), token: token,
		connections: make(chan struct{}, maxPeerConnections), streams: make(chan struct{}, maxPeerStreams), cancel: cancel,
	}
	for _, member := range members {
		peer.members[member.ID] = member
	}
	peer.wg.Add(1)
	go func() { defer peer.wg.Done(); peer.serve(runCtx) }()
	return peer, nil
}

// StartLearnerPeerServer gives an unpromoted learner its own token-bound TLS
// identity. The learner is not admitted as a voter until its core applies the
// reconfiguration, so its token cannot authenticate voting RPCs meanwhile.
func StartLearnerPeerServer(ctx context.Context, addr string, server *Server, voters []quepaxa.Member, token string, learner quepaxa.Member) (*PeerServer, error) {
	if learner.ID == "" || learner.Token == "" {
		return nil, fmt.Errorf("learner ID and token are required")
	}
	if learner.ID != server.core.NodeID() {
		return nil, fmt.Errorf("learner ID must match the local node")
	}
	for _, voter := range voters {
		if voter.ID == learner.ID {
			return nil, fmt.Errorf("learner is already a voter")
		}
	}
	members := append(append([]quepaxa.Member(nil), voters...), learner)
	return StartPeerServer(ctx, addr, server, members, token)
}

func (s *PeerServer) Close() error {
	s.cancel()
	err := s.listener.Close()
	s.wg.Wait()
	return err
}

func (s *PeerServer) Addr() string { return s.listener.Addr().String() }

func (s *PeerServer) serve(ctx context.Context) {
	for {
		conn, err := s.listener.Accept(ctx)
		if err != nil {
			return
		}
		select {
		case s.connections <- struct{}{}:
			s.wg.Add(1)
			go func() {
				defer func() { <-s.connections; s.wg.Done() }()
				s.serveConnection(ctx, conn)
			}()
		default:
			_ = conn.CloseWithError(1, "peer connection limit reached")
		}
	}
}

func (s *PeerServer) serveConnection(ctx context.Context, conn *quic.Conn) {
	defer conn.CloseWithError(0, "shutdown")
	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		select {
		case s.streams <- struct{}{}:
			s.wg.Add(1)
			go func() {
				defer func() { <-s.streams; s.wg.Done() }()
				s.serveStream(conn, stream)
			}()
		default:
			stream.CancelRead(1)
			stream.CancelWrite(1)
		}
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
			if request.Operation == peerfb.OperationPrepareCheckpoint {
				_ = stream.SetDeadline(time.Now().Add(checkpointPrepareTimeout))
			}
			response, err = s.handle(stream.Context(), conn, request)
		}
	}
	if err != nil {
		response = &peerfb.ResponseT{Error: err.Error()}
		switch {
		case errors.Is(err, quepaxa.ErrQuorumUnavailable):
			response.ErrorCode = peerErrorQuorum
		case errors.Is(err, quepaxa.ErrCompacted):
			response.ErrorCode = peerErrorCompacted
		case errors.Is(err, ErrNotReady), errors.Is(err, ErrOverloaded):
			response.ErrorCode = peerErrorRetryable
		}
	}
	if writeErr := writePeerFrame(stream, encodePeerResponse(response)); writeErr != nil {
		stream.CancelWrite(1)
		return
	}
	_ = stream.Close()
}

func peerMember(config quepaxa.Cluster, id quepaxa.NodeID) (quepaxa.Member, bool) {
	for i := len(config.Members) - 1; i >= 0; i-- {
		if config.Members[i].ID == id {
			return config.Members[i], true
		}
	}
	return quepaxa.Member{}, false
}

func (s *PeerServer) handle(ctx context.Context, conn *quic.Conn, request *peerfb.RequestT) (*peerfb.ResponseT, error) {
	config, err := s.configurationFor(request)
	if err != nil || request.ClusterId != string(s.server.cluster) || request.ConfigId != uint64(config.ConfigID) {
		return nil, fmt.Errorf("cluster identity mismatch")
	}
	historical, historicalOK := peerMember(config, quepaxa.NodeID(request.SenderId))
	current := s.server.core.CurrentCluster()
	active, activeOK := peerMember(current, quepaxa.NodeID(request.SenderId))
	// A historical envelope does not keep a removed voter authorized after the
	// boundary. Existing voters need credentials valid in both configurations.
	voter := historicalOK && activeOK && historical.Token != "" && active.Token != "" &&
		subtle.ConstantTimeCompare([]byte(request.Token), []byte(historical.Token)) == 1 &&
		subtle.ConstantTimeCompare([]byte(request.Token), []byte(active.Token)) == 1
	learner := s.token != "" && subtle.ConstantTimeCompare([]byte(request.Token), []byte(s.token)) == 1
	// Non-voting learners may only pull already-certified decisions. They use
	// the cluster admin token and never enter the fixed voter membership.
	if !voter && !(learner && request.Operation == peerfb.OperationSync) {
		return nil, fmt.Errorf("peer authentication failed")
	}
	if !allows0RTT(request.Operation) && (conn == nil || !conn.ConnectionState().TLS.HandshakeComplete) {
		return nil, fmt.Errorf("%s is not accepted as replayable early data", request.Operation)
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
		if len(request.Hash) != 0 && len(request.Hash) != sha256.Size {
			return nil, fmt.Errorf("invalid reconfiguration ID")
		}
		record := quepaxa.RecordRequest{Slot: quepaxa.Slot(request.Record.Slot), Step: quepaxa.Step(request.Record.Step), ConfigID: uint(request.ConfigId), Proposal: proposal}
		copy(record.ReconfigurationID[:], request.Hash)
		if through := s.server.core.RecordCatchUpThrough(record); through > s.server.core.Tip() {
			source := quepaxa.NodeID(request.SenderId)
			if s.server.transport == nil || source == s.server.core.NodeID() {
				return nil, fmt.Errorf("record prefix is unavailable")
			}
			if err := s.server.catchUpFrom(ctx, source, through, false); err != nil {
				return nil, err
			}
		}
		summary, err := s.server.core.Record(ctx, record)
		if err != nil {
			return nil, err
		}
		return &peerfb.ResponseT{Summary: summaryToWire(summary)}, nil
	case peerfb.OperationLearned:
		if !s.server.ready() {
			return nil, ErrNotReady
		}
		decision, err := decisionFromWire(request.Decision)
		if err != nil {
			return nil, err
		}
		if control, err := quepaxa.DecodeReconfiguration(decision.Proposal.Value); err != nil {
			return nil, err
		} else if control && decision.Slot > 1 {
			if s.server.core.Tip() < decision.Slot-1 {
				if s.server.transport == nil {
					return nil, fmt.Errorf("reconfiguration control prefix is unavailable")
				}
				if err := s.server.catchUpFrom(ctx, quepaxa.NodeID(request.SenderId), decision.Slot-1, true); err != nil {
					return nil, err
				}
			}
			if err := s.server.core.WaitTip(ctx, decision.Slot-1); err != nil {
				return nil, err
			}
		}
		if decisionHasRecorder(decision, s.server.core.NodeID()) {
			err = s.server.core.AcceptDecisionHint(decision)
		} else {
			err = s.server.core.AcceptDecision(decision)
		}
		if err != nil {
			return nil, err
		}
		if err := s.server.applyDecisions(ctx, decision.Slot); err != nil {
			return nil, err
		}
		return &peerfb.ResponseT{}, nil
	case peerfb.OperationSync:
		if request.From == 0 || request.Limit == 0 || request.Limit > 128 {
			return nil, fmt.Errorf("invalid decisions range")
		}
		decisions, tip, err := s.server.core.DecisionsFrom(quepaxa.Slot(request.From), int(request.Limit))
		if err != nil {
			return nil, err
		}
		response := &peerfb.ResponseT{ClusterId: string(s.server.cluster), ProposerId: string(s.server.core.NodeID()), ConfigId: request.ConfigId, Tip: uint64(tip)}
		wireDecisions := make([]*peerfb.DecidedValueT, len(decisions))
		for i := range decisions {
			wireDecisions[i] = decidedToWire(decisions[i])
		}
		low, high, fit := 1, len(wireDecisions), 0
		for low <= high {
			mid := low + (high-low)/2
			response.Decisions = wireDecisions[:mid]
			if len(encodePeerResponse(response)) <= maxPeerFrame {
				fit = mid
				low = mid + 1
			} else {
				high = mid - 1
			}
		}
		if len(wireDecisions) > 0 && fit == 0 {
			return nil, fmt.Errorf("decision %d exceeds peer frame limit", decisions[0].Slot)
		}
		response.Decisions = wireDecisions[:fit]
		return response, nil
	case peerfb.OperationReadIndex:
		if !s.server.core.CanParticipateReadIndex() {
			return nil, quepaxa.ErrQuorumUnavailable
		}
		if !s.server.ready() {
			return nil, ErrNotReady
		}
		return &peerfb.ResponseT{ClusterId: string(s.server.cluster), ProposerId: string(s.server.core.NodeID()), ConfigId: uint64(s.server.core.ConfigID()), Tip: uint64(s.server.core.Tip())}, nil
	case peerfb.OperationStageValue:
		if len(request.Hash) != sha256.Size {
			return nil, fmt.Errorf("invalid value hash")
		}
		var hash quepaxa.ValueHash
		copy(hash[:], request.Hash)
		if err := s.server.core.StageValue(hash, request.Value); err != nil {
			return nil, err
		}
		return &peerfb.ResponseT{}, nil
	case peerfb.OperationFetchValue:
		if len(request.Hash) != sha256.Size {
			return nil, fmt.Errorf("invalid value hash")
		}
		var hash quepaxa.ValueHash
		copy(hash[:], request.Hash)
		value, ok := s.server.core.Value(hash)
		if !ok {
			return nil, fmt.Errorf("value is unavailable")
		}
		return &peerfb.ResponseT{Value: value}, nil
	case peerfb.OperationVerifyPrefix:
		if request.From == 0 || len(request.Hash) != sha256.Size || len(request.Value) != sha256.Size {
			return nil, fmt.Errorf("invalid durable-prefix request")
		}
		if !matchesWALIdentity(s.server.core.WALIdentity(), request.Value) {
			return nil, fmt.Errorf("learner WAL identity mismatch")
		}
		var expected quepaxa.ValueHash
		copy(expected[:], request.Hash)
		actual, err := s.server.core.DurablePrefix(quepaxa.Slot(request.From))
		if err != nil {
			return nil, err
		}
		if actual != expected {
			return nil, fmt.Errorf("durable prefix mismatch")
		}
		return &peerfb.ResponseT{ClusterId: string(s.server.cluster), ProposerId: string(s.server.core.NodeID()), ConfigId: request.ConfigId, Tip: request.From}, nil
	case peerfb.OperationPrepareCheckpoint:
		seal, checkpoint, err := quepaxa.DecodeCheckpointSeal(request.Value)
		if err != nil || !checkpoint {
			if err == nil {
				err = fmt.Errorf("checkpoint seal is required")
			}
			return nil, err
		}
		if err := s.server.prepareCheckpoint(ctx, quepaxa.NodeID(request.SenderId), seal); err != nil {
			return nil, err
		}
		return &peerfb.ResponseT{}, nil
	default:
		return nil, fmt.Errorf("unknown peer operation %d", request.Operation)
	}
}

func (s *PeerServer) configurationFor(request *peerfb.RequestT) (quepaxa.Cluster, error) {
	switch request.Operation {
	case peerfb.OperationRecord:
		if request.Record != nil {
			return s.server.core.ClusterForSlot(quepaxa.Slot(request.Record.Slot)), nil
		}
	case peerfb.OperationLearned:
		if request.Decision != nil {
			return s.server.core.ClusterForSlot(quepaxa.Slot(request.Decision.Slot)), nil
		}
	case peerfb.OperationSync:
		current := s.server.core.CurrentCluster()
		if request.ConfigId == uint64(current.ConfigID) {
			return current, nil
		}
		if request.From != 0 {
			return s.server.core.ClusterForSlot(quepaxa.Slot(request.From)), nil
		}
	case peerfb.OperationPrepareCheckpoint:
		seal, checkpoint, err := quepaxa.DecodeCheckpointSeal(request.Value)
		if err != nil {
			return quepaxa.Cluster{}, err
		}
		if checkpoint {
			return s.server.core.ClusterForSlot(seal.Index), nil
		}
	}
	return s.server.core.CurrentCluster(), nil
}

func peerPublicKey(clusterID types.ClusterID, nodeID quepaxa.NodeID, token string) ed25519.PublicKey {
	return peerPrivateKey(clusterID, nodeID, token).Public().(ed25519.PublicKey)
}

func peerPrivateKey(clusterID types.ClusterID, nodeID quepaxa.NodeID, token string) ed25519.PrivateKey {
	mac := hmac.New(sha256.New, []byte(token))
	mac.Write([]byte("rhiza-peer-certificate\x00"))
	mac.Write([]byte(clusterID))
	mac.Write([]byte{0})
	mac.Write([]byte(nodeID))
	return ed25519.NewKeyFromSeed(mac.Sum(nil))
}

func peerCertificate(clusterID types.ClusterID, nodeID quepaxa.NodeID, token string) (tls.Certificate, error) {
	privateKey := peerPrivateKey(clusterID, nodeID, token)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	serialHash := sha256.Sum256(publicKey)
	serial := new(big.Int).SetBytes(serialHash[:20])
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: string(nodeID)}, DNSNames: []string{string(nodeID)},
		NotBefore: time.Unix(0, 0), NotAfter: time.Date(2125, 1, 1, 0, 0, 0, 0, time.UTC),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: privateKey}, nil
}
