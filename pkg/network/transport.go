package network

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sync"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/quic-go/quic-go"
)

const peerALPN = "rhiza-peer"
const peerRPCTimeout = 5 * time.Second
const quorumRPCTimeout = 500 * time.Millisecond
const checkpointPrepareTimeout = 5 * time.Minute

type peerConnection struct {
	mu     sync.Mutex
	gate   chan struct{}
	conn   *quic.Conn
	active map[*quic.Conn]int
}

// Transport sends private peer RPCs over persistent raw QUIC connections.
type Transport struct {
	members   map[quepaxa.NodeID]quepaxa.Member
	peerKeys  map[quepaxa.NodeID]ed25519.PublicKey
	clusterID types.ClusterID
	configID  uint
	localID   quepaxa.NodeID
	token     string
	fallback  string
	tls       *tls.Config
	quic      *quic.Config
	peers     map[quepaxa.NodeID]*peerConnection
}

func NewTransport(clusterID types.ClusterID, localID quepaxa.NodeID, config *quepaxa.Cluster, token string) *Transport {
	return newTransport(clusterID, localID, config, token, nil)
}

// PeerIdentity is the token-free endpoint and pinned TLS identity of a voter.
type PeerIdentity struct {
	ID        quepaxa.NodeID
	PeerURL   string
	PublicKey [ed25519.PublicKeySize]byte
}

// NewPeerIdentity derives the public identity a learner may retain.
func NewPeerIdentity(clusterID types.ClusterID, member quepaxa.Member) (PeerIdentity, error) {
	peerURL := member.PeerURL
	if peerURL == "" {
		peerURL = member.URL
	}
	if clusterID == "" || member.ID == "" || peerURL == "" || member.Token == "" {
		return PeerIdentity{}, fmt.Errorf("cluster ID, voter ID, peer URL, and voter token are required")
	}
	return PeerIdentity{ID: member.ID, PeerURL: peerURL, PublicKey: [ed25519.PublicKeySize]byte(peerPublicKey(clusterID, member.ID, member.Token))}, nil
}

// NewLearnerTransport creates a read-only transport without retaining voter tokens.
func NewLearnerTransport(clusterID types.ClusterID, localID quepaxa.NodeID, configID uint, peers []PeerIdentity, token string) *Transport {
	members := make([]quepaxa.Member, 0, len(peers))
	keys := make(map[quepaxa.NodeID]ed25519.PublicKey, len(peers))
	for _, peer := range peers {
		members = append(members, quepaxa.Member{ID: peer.ID, PeerURL: peer.PeerURL})
		keys[peer.ID] = append(ed25519.PublicKey(nil), peer.PublicKey[:]...)
	}
	return newTransport(clusterID, localID, &quepaxa.Cluster{ConfigID: configID, Members: members}, token, keys)
}

func newTransport(clusterID types.ClusterID, localID quepaxa.NodeID, config *quepaxa.Cluster, token string, keys map[quepaxa.NodeID]ed25519.PublicKey) *Transport {
	peers := make(map[quepaxa.NodeID]*peerConnection, len(config.Members))
	for _, member := range config.Members {
		peers[member.ID] = &peerConnection{gate: make(chan struct{}, 1), active: make(map[*quic.Conn]int)}
	}
	localToken := token
	if member, ok := config.MemberSet()[localID]; ok && member.Token != "" {
		localToken = member.Token
	}
	return &Transport{
		members: config.MemberSet(), clusterID: clusterID, configID: config.ConfigID,
		localID: localID, token: localToken, fallback: token, peers: peers, peerKeys: keys,
		tls: &tls.Config{
			MinVersion: tls.VersionTLS13, NextProtos: []string{peerALPN},
			ClientSessionCache: tls.NewLRUClientSessionCache(len(config.Members)),
		},
		quic: &quic.Config{HandshakeIdleTimeout: 5 * time.Second, MaxIdleTimeout: 30 * time.Second, KeepAlivePeriod: 10 * time.Second, MaxIncomingStreams: 256},
	}
}

func (t *Transport) request(operation peerfb.Operation) *peerfb.RequestT {
	return &peerfb.RequestT{Operation: operation, ClusterId: string(t.clusterID), SenderId: string(t.localID), ConfigId: uint64(t.configID), Token: t.token}
}

func memberQUICAddr(member quepaxa.Member) (string, error) {
	raw := member.PeerURL
	if raw == "" {
		raw = member.URL
	}
	endpoint, err := url.Parse(raw)
	if err != nil || endpoint.Host == "" {
		return "", fmt.Errorf("invalid peer URL %q", raw)
	}
	return endpoint.Host, nil
}

func (t *Transport) connection(ctx context.Context, to quepaxa.NodeID, waitHandshake bool) (*quic.Conn, error) {
	member, ok := t.members[to]
	if !ok {
		return nil, fmt.Errorf("unknown node: %s", to)
	}
	peer := t.peers[to]
	select {
	case peer.gate <- struct{}{}:
		defer func() { <-peer.gate }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if peer.conn == nil || peer.conn.Context().Err() != nil {
		addr, err := memberQUICAddr(member)
		if err != nil {
			return nil, err
		}
		tlsConfig := t.tls.Clone()
		tlsConfig.ServerName = string(to)
		identityToken := member.Token
		if identityToken == "" {
			identityToken = t.fallback
		}
		if identityToken == "" && len(t.members) > 1 {
			return nil, fmt.Errorf("peer identity token is required for %s", to)
		}
		expectedKey := t.peerKeys[to]
		if len(expectedKey) == 0 {
			expectedKey = peerPublicKey(t.clusterID, to, identityToken)
		}
		tlsConfig.InsecureSkipVerify = true // Exact token-bound Ed25519 key pin is verified below.
		tlsConfig.VerifyConnection = func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) != 1 {
				return fmt.Errorf("peer %s presented %d certificates", to, len(state.PeerCertificates))
			}
			key, ok := state.PeerCertificates[0].PublicKey.(ed25519.PublicKey)
			if !ok || subtle.ConstantTimeCompare(key, expectedKey) != 1 {
				return fmt.Errorf("peer %s certificate identity mismatch", to)
			}
			return nil
		}
		peer.conn, err = quic.DialAddrEarly(ctx, addr, tlsConfig, t.quic)
		if err != nil {
			peer.conn = nil
			return nil, err
		}
	}
	if waitHandshake {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-peer.conn.HandshakeComplete():
		}
	}
	conn := peer.conn
	peer.active[conn]++
	return conn, nil
}

func (t *Transport) call(ctx context.Context, to quepaxa.NodeID, request *peerfb.RequestT, waitHandshake bool) (*peerfb.ResponseT, error) {
	return t.callWithTimeout(ctx, to, request, waitHandshake, peerRPCTimeout)
}

func (t *Transport) callWithTimeout(ctx context.Context, to quepaxa.NodeID, request *peerfb.RequestT, waitHandshake bool, timeout time.Duration) (*peerfb.ResponseT, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	waitHandshake = waitHandshake || !allows0RTT(request.Operation)
	for retried0RTT := false; ; {
		conn, err := t.connection(ctx, to, waitHandshake)
		if err != nil {
			return nil, err
		}
		response, err := t.callConnection(ctx, conn, request)
		if errors.Is(err, quic.Err0RTTRejected) && !retried0RTT {
			t.release(to, conn)
			// The early stream was discarded, not executed. Promote this same
			// connection to 1-RTT and replay the request once within its original
			// deadline. This prevents a peer restart from consuming one whole
			// periodic catch-up round (or a remaining quorum attempt).
			if _, nextErr := conn.NextConnection(ctx); nextErr != nil {
				t.invalidate(to, conn)
				return nil, nextErr
			}
			retried0RTT = true
			continue
		}
		if err != nil && response == nil {
			t.invalidate(to, conn)
		}
		t.release(to, conn)
		if response != nil {
			switch response.ErrorCode {
			case peerErrorQuorum:
				return nil, quepaxa.ErrQuorumUnavailable
			case peerErrorCompacted:
				return nil, quepaxa.ErrCompacted
			}
		}
		if err != nil {
			return nil, err
		}
		return response, nil
	}
}

func allows0RTT(operation peerfb.Operation) bool {
	switch operation {
	case peerfb.OperationSync, peerfb.OperationReadIndex, peerfb.OperationFetchValue:
		return true
	default:
		return false
	}
}

func (t *Transport) callConnection(ctx context.Context, conn *quic.Conn, request *peerfb.RequestT) (*peerfb.ResponseT, error) {
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	defer stream.CancelRead(0)
	if deadline, ok := ctx.Deadline(); ok {
		_ = stream.SetDeadline(deadline)
	}
	if err := writePeerFrame(stream, encodePeerRequest(request)); err != nil {
		stream.CancelWrite(1)
		return nil, err
	}
	if err := stream.Close(); err != nil {
		return nil, err
	}
	data, err := readPeerFrame(stream)
	if err != nil {
		return nil, err
	}
	return decodePeerResponse(data)
}

// PrepareCheckpoint waits for a durable verified quorum before the small seal
// value enters normal consensus.
func (t *Transport) PrepareCheckpoint(ctx context.Context, seal quepaxa.CheckpointSeal) error {
	value, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		return err
	}
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, len(t.members)-1)
	pending := 0
	for _, member := range t.members {
		if member.ID == t.localID {
			continue
		}
		pending++
		go func(member quepaxa.Member) {
			request := t.request(peerfb.OperationPrepareCheckpoint)
			request.Value = value
			_, err := t.callWithTimeout(callCtx, member.ID, request, false, checkpointPrepareTimeout)
			results <- err
		}(member)
	}
	successes := 1
	if successes >= len(t.members)/2+1 {
		return nil
	}
	var firstErr error
	for range pending {
		if err := <-results; err != nil {
			if firstErr == nil {
				firstErr = err
			}
		} else {
			successes++
			if successes >= len(t.members)/2+1 {
				return nil
			}
		}
	}
	if firstErr != nil {
		return fmt.Errorf("%w: prepare checkpoint: %v", quepaxa.ErrQuorumUnavailable, firstErr)
	}
	return quepaxa.ErrQuorumUnavailable
}

func (t *Transport) invalidate(to quepaxa.NodeID, conn *quic.Conn) {
	peer := t.peers[to]
	peer.mu.Lock()
	if peer.conn == conn {
		peer.conn = nil
	}
	idle := peer.active[conn] == 0
	peer.mu.Unlock()
	if idle {
		_ = conn.CloseWithError(0, "reconnect")
	}
}

func (t *Transport) release(to quepaxa.NodeID, conn *quic.Conn) {
	peer := t.peers[to]
	peer.mu.Lock()
	peer.active[conn]--
	remaining := peer.active[conn]
	if remaining == 0 {
		delete(peer.active, conn)
	}
	stale := peer.conn != conn
	peer.mu.Unlock()
	if remaining == 0 && stale {
		_ = conn.CloseWithError(0, "reconnect")
	}
}

func (t *Transport) FetchDecisions(ctx context.Context, source quepaxa.NodeID, from quepaxa.Slot, limit int) (DecisionsResponse, error) {
	req := t.request(peerfb.OperationSync)
	req.From, req.Limit = uint64(from), uint32(limit)
	response, err := t.call(ctx, source, req, false)
	if err != nil {
		return DecisionsResponse{}, err
	}
	result := DecisionsResponse{ClusterID: types.ClusterID(response.ClusterId), ProposerID: quepaxa.NodeID(response.ProposerId), ConfigID: uint(response.ConfigId), Tip: quepaxa.Slot(response.Tip), Decisions: make([]quepaxa.DecidedValue, len(response.Decisions))}
	for i := range response.Decisions {
		result.Decisions[i], err = decidedFromWire(response.Decisions[i])
		if err != nil {
			return DecisionsResponse{}, err
		}
	}
	if result.ClusterID != t.clusterID || result.ProposerID != source || result.ConfigID != t.configID {
		return DecisionsResponse{}, fmt.Errorf("catch-up source identity mismatch")
	}
	return result, nil
}

func (t *Transport) SendRecord(ctx context.Context, to quepaxa.NodeID, request quepaxa.RecordRequest) (quepaxa.Summary, error) {
	req := t.request(peerfb.OperationRecord)
	req.Record = &peerfb.RecordRequestT{Slot: uint64(request.Slot), Step: uint64(request.Step), Proposal: proposalToWire(request.Proposal)}
	response, err := t.callWithTimeout(ctx, to, req, false, quorumRPCTimeout)
	if err != nil {
		return quepaxa.Summary{}, err
	}
	summary, err := summaryFromWire(response.Summary)
	if err == nil && summary.RecorderID != to {
		err = fmt.Errorf("recorder identity mismatch: want %s got %s", to, summary.RecorderID)
	}
	return summary, err
}

func (t *Transport) Propose(ctx context.Context, to quepaxa.NodeID, value []byte) (quepaxa.DecidedValue, error) {
	req := t.request(peerfb.OperationPropose)
	req.Value = value
	// Every ingress also starts its local proposer. A remote proposer is only a
	// hedge, so a dead peer must not hold the client path for the general RPC
	// timeout while the local and other remote proposers can still make quorum.
	response, err := t.callWithTimeout(ctx, to, req, true, quorumRPCTimeout)
	if err != nil {
		return quepaxa.DecidedValue{}, err
	}
	return decidedFromWire(response.Decided)
}

func (t *Transport) SendDecision(ctx context.Context, decision quepaxa.Decision) error {
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, len(t.members)-1)
	pending := 0
	for _, member := range t.members {
		if member.ID == t.localID {
			continue
		}
		pending++
		go func(member quepaxa.Member) {
			req := t.request(peerfb.OperationLearned)
			req.Decision = decisionToWire(decision)
			_, err := t.callWithTimeout(callCtx, member.ID, req, false, quorumRPCTimeout)
			results <- err
		}(member)
	}
	successes := 1 // local learner
	var firstErr error
	for range pending {
		if err := <-results; err != nil {
			if firstErr == nil {
				firstErr = err
			}
		} else {
			successes++
			if successes >= len(t.members)/2+1 {
				return nil
			}
		}
	}
	if firstErr != nil {
		return firstErr
	}
	return quepaxa.ErrQuorumUnavailable
}

func decisionHasRecorder(decision quepaxa.Decision, id quepaxa.NodeID) bool {
	for _, summary := range decision.Summaries {
		if summary.RecorderID == id {
			return true
		}
	}
	return false
}

func (t *Transport) ReadTip(ctx context.Context, to quepaxa.NodeID) (quepaxa.Slot, error) {
	response, err := t.callWithTimeout(ctx, to, t.request(peerfb.OperationReadIndex), false, quorumRPCTimeout)
	if err != nil {
		return 0, err
	}
	if response.ClusterId != string(t.clusterID) || response.ProposerId != string(to) || response.ConfigId != uint64(t.configID) {
		return 0, fmt.Errorf("read-index source identity mismatch")
	}
	return quepaxa.Slot(response.Tip), nil
}

func (t *Transport) StageValue(ctx context.Context, to quepaxa.NodeID, hash quepaxa.ValueHash, value []byte) error {
	request := t.request(peerfb.OperationStageValue)
	request.Hash = append([]byte(nil), hash[:]...)
	request.Value = append([]byte(nil), value...)
	_, err := t.call(ctx, to, request, false)
	return err
}

func (t *Transport) FetchValue(ctx context.Context, from quepaxa.NodeID, hash quepaxa.ValueHash) ([]byte, error) {
	request := t.request(peerfb.OperationFetchValue)
	request.Hash = append([]byte(nil), hash[:]...)
	response, err := t.call(ctx, from, request, false)
	if err != nil {
		return nil, err
	}
	if sha256.Sum256(response.Value) != hash {
		return nil, fmt.Errorf("fetched value hash mismatch")
	}
	return append([]byte(nil), response.Value...), nil
}

func (t *Transport) Close() error {
	for _, peer := range t.peers {
		peer.mu.Lock()
		if peer.conn != nil {
			_ = peer.conn.CloseWithError(0, "shutdown")
			peer.conn = nil
		}
		for conn := range peer.active {
			_ = conn.CloseWithError(0, "shutdown")
		}
		clear(peer.active)
		peer.mu.Unlock()
	}
	return nil
}

func writePeerFrame(w io.Writer, data []byte) error {
	if len(data) == 0 || len(data) > maxPeerFrame {
		return fmt.Errorf("peer frame too large: %d", len(data))
	}
	frame := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(frame, uint32(len(data)))
	copy(frame[4:], data)
	for len(frame) > 0 {
		n, err := w.Write(frame)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		frame = frame[n:]
	}
	return nil
}

func readPeerFrame(r io.Reader) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(header[:])
	if size == 0 || size > maxPeerFrame {
		return nil, fmt.Errorf("invalid peer frame size %d", size)
	}
	data := make([]byte, size)
	_, err := io.ReadFull(r, data)
	return data, err
}

var _ quepaxa.Transport = (*Transport)(nil)
