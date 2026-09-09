package network

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/quic-go/quic-go"
)

const peerALPN = "rhiza-peer"
const peerRPCTimeout = 5 * time.Second
const checkpointPrepareTimeout = 5 * time.Minute

var errPeerRejected = errors.New("peer rejected request")
var errTransportClosed = errors.New("peer transport is closed")

type peerConnection struct {
	mu     sync.Mutex
	gate   chan struct{}
	conn   *quic.Conn
	active map[*quic.Conn]int
}

type clusterResolver interface {
	CurrentCluster() quepaxa.Cluster
	ClusterForSlot(quepaxa.Slot) quepaxa.Cluster
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

	resolverMu sync.RWMutex
	resolver   clusterResolver
	bound      atomic.Bool
	dynamicMu  sync.Mutex
	dynamic    map[uint]*Transport
	closed     bool
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
		quic: &quic.Config{HandshakeIdleTimeout: 5 * time.Second, MaxIdleTimeout: 30 * time.Second, KeepAlivePeriod: 10 * time.Second, MaxIncomingStreams: 256}, dynamic: make(map[uint]*Transport),
	}
}

// BindCore makes requests resolve the immutable configuration for their slot.
// It is called once during startup before peer RPCs begin.
func (t *Transport) BindCore(core clusterResolver) {
	t.resolverMu.Lock()
	t.resolver = core
	t.resolverMu.Unlock()
	t.bound.Store(core != nil)
}

func (t *Transport) resolverSnapshot() clusterResolver {
	if !t.bound.Load() {
		return nil
	}
	t.resolverMu.RLock()
	defer t.resolverMu.RUnlock()
	return t.resolver
}

func (t *Transport) clusterForSlot(slot quepaxa.Slot) quepaxa.Cluster {
	if core := t.resolverSnapshot(); core != nil {
		return core.ClusterForSlot(slot)
	}
	return quepaxa.Cluster{ConfigID: t.configID, Members: membersSlice(t.members)}
}

func (t *Transport) currentCluster() quepaxa.Cluster {
	if core := t.resolverSnapshot(); core != nil {
		return core.CurrentCluster()
	}
	return quepaxa.Cluster{ConfigID: t.configID, Members: membersSlice(t.members)}
}

func membersSlice(members map[quepaxa.NodeID]quepaxa.Member) []quepaxa.Member {
	result := make([]quepaxa.Member, 0, len(members))
	for _, member := range members {
		result = append(result, member)
	}
	return result
}

func (t *Transport) transportFor(config quepaxa.Cluster) (*Transport, error) {
	t.dynamicMu.Lock()
	defer t.dynamicMu.Unlock()
	if t.closed {
		return nil, errTransportClosed
	}
	if peer := t.dynamic[config.ConfigID]; peer != nil {
		return peer, nil
	}
	// Each immutable configuration owns its pools and TLS session cache. A
	// changed voter token therefore cannot reuse a previous configuration's
	// connection or 0-RTT ticket.
	peer := newTransport(t.clusterID, t.localID, &config, t.fallback, nil)
	t.dynamic[config.ConfigID] = peer
	return peer, nil
}

func (t *Transport) transportForSlot(slot quepaxa.Slot) (*Transport, error) {
	if core := t.resolverSnapshot(); core != nil {
		return t.transportFor(core.ClusterForSlot(slot))
	}
	return t, nil
}

func (t *Transport) transportForCurrent() (*Transport, error) {
	if core := t.resolverSnapshot(); core != nil {
		return t.transportFor(core.CurrentCluster())
	}
	return t, nil
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
	return t.callContext(ctx, to, request, waitHandshake)
}

// callQuorum sends one asynchronous quorum attempt. A failed recorder does not
// need an in-phase retry: the enclosing quorum completes from other replies.
func (t *Transport) callQuorum(ctx context.Context, to quepaxa.NodeID, request *peerfb.RequestT, waitHandshake bool) (*peerfb.ResponseT, error) {
	return t.callContext(ctx, to, request, waitHandshake)
}

func (t *Transport) callContext(ctx context.Context, to quepaxa.NodeID, request *peerfb.RequestT, waitHandshake bool) (*peerfb.ResponseT, error) {
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
			case peerErrorRetryable:
				return nil, err
			}
		}
		if err != nil {
			if response != nil {
				return nil, fmt.Errorf("%w: %v", errPeerRejected, err)
			}
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
		var streamErr *quic.StreamError
		if !errors.As(context.Cause(stream.Context()), &streamErr) || !streamErr.Remote || streamErr.ErrorCode != 0 {
			return nil, err
		}
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
	peer, err := t.transportForSlot(seal.Index)
	if err != nil {
		return err
	}
	if peer != t {
		return peer.PrepareCheckpoint(ctx, seal)
	}
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
	// Sync is a read-only transfer. Route to a current voter even when the
	// requested page begins before that voter was admitted; Core validates every
	// returned certificate against the configuration for its own slot.
	peer, err := t.transportForCurrent()
	if err != nil {
		return DecisionsResponse{}, err
	}
	if peer != t {
		return peer.FetchDecisions(ctx, source, from, limit)
	}
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
	peer, err := t.transportForSlot(request.Slot)
	if err != nil {
		return quepaxa.Summary{}, err
	}
	if peer != t {
		return peer.SendRecord(ctx, to, request)
	}
	if request.ConfigID != 0 && request.ConfigID != t.configID {
		return quepaxa.Summary{}, fmt.Errorf("record configuration mismatch")
	}
	req := t.request(peerfb.OperationRecord)
	req.Record = &peerfb.RecordRequestT{Slot: uint64(request.Slot), Step: uint64(request.Step), Proposal: proposalToWire(request.Proposal)}
	if request.ReconfigurationID != (quepaxa.ValueHash{}) {
		req.Hash = append([]byte(nil), request.ReconfigurationID[:]...)
	}
	response, err := t.callQuorum(ctx, to, req, false)
	if err != nil {
		return quepaxa.Summary{}, err
	}
	summary, err := summaryFromWire(response.Summary)
	if err == nil && summary.RecorderID != to {
		err = fmt.Errorf("recorder identity mismatch: want %s got %s", to, summary.RecorderID)
	}
	return summary, err
}

func (t *Transport) SendDecision(ctx context.Context, decision quepaxa.Decision) error {
	if core := t.resolverSnapshot(); core != nil {
		old, next := core.ClusterForSlot(decision.Slot), core.ClusterForSlot(decision.Slot+1)
		if old.ConfigID != next.ConfigID {
			return t.sendTerminalDecision(ctx, decision, old, next)
		}
		if control, err := quepaxa.DecodeReconfiguration(decision.Proposal.Value); err != nil {
			return err
		} else if control {
			peer, err := t.transportForSlot(decision.Slot)
			if err != nil {
				return err
			}
			return peer.sendFreezeDecision(ctx, decision)
		}
	}
	peer, err := t.transportForSlot(decision.Slot)
	if err != nil {
		return err
	}
	if peer != t {
		return peer.SendDecision(ctx, decision)
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
			req := t.request(peerfb.OperationLearned)
			req.Decision = decisionToWire(decision)
			_, err := t.callQuorum(callCtx, member.ID, req, false)
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

// sendFreezeDecision requires every recorder named by the freeze certificate.
// A generic quorum could otherwise omit a safe recorder that must learn the
// freeze before it can reject the terminal record under the old configuration.
func (t *Transport) sendFreezeDecision(ctx context.Context, decision quepaxa.Decision) error {
	required := make(map[quepaxa.NodeID]bool, len(decision.Summaries))
	for _, summary := range decision.Summaries {
		if _, ok := t.members[summary.RecorderID]; !ok {
			return fmt.Errorf("freeze recorder %q is not a voter", summary.RecorderID)
		}
		required[summary.RecorderID] = summary.RecorderID == t.localID
	}
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		id  quepaxa.NodeID
		err error
	}
	results := make(chan result, len(t.members)-1)
	pending := 0
	for _, member := range t.members {
		if member.ID == t.localID {
			continue
		}
		pending++
		go func(member quepaxa.Member) {
			req := t.request(peerfb.OperationLearned)
			req.Decision = decisionToWire(decision)
			_, err := t.callQuorum(callCtx, member.ID, req, false)
			results <- result{id: member.ID, err: err}
		}(member)
	}
	successes := 1
	var firstErr error
	for range pending {
		result := <-results
		if result.err != nil {
			if firstErr == nil {
				firstErr = result.err
			}
			if _, needed := required[result.id]; needed {
				return result.err
			}
			continue
		}
		successes++
		if _, needed := required[result.id]; needed {
			required[result.id] = true
		}
		allRequired := true
		for _, acknowledged := range required {
			if !acknowledged {
				allRequired = false
				break
			}
		}
		if allRequired && successes >= len(t.members)/2+1 {
			return nil
		}
	}
	if firstErr != nil {
		return firstErr
	}
	return quepaxa.ErrQuorumUnavailable
}

// sendTerminalDecision retains the old decision envelope while requiring its
// durable installation by the next configuration. A newly added voter must
// acknowledge explicitly; an AB quorum cannot activate a replacement D that
// has not durably learned the terminal record.
func (t *Transport) sendTerminalDecision(ctx context.Context, decision quepaxa.Decision, old, next quepaxa.Cluster) error {
	oldMembers, nextMembers := old.MemberSet(), next.MemberSet()
	if decision.ConfigID != 0 && decision.ConfigID != old.ConfigID {
		return fmt.Errorf("terminal decision configuration mismatch")
	}
	union := old
	union.Members = append([]quepaxa.Member(nil), old.Members...)
	for _, member := range next.Members {
		if _, ok := oldMembers[member.ID]; !ok {
			union.Members = append(union.Members, member)
		}
	}
	peer := newTransport(t.clusterID, t.localID, &union, t.fallback, nil)
	defer peer.Close()
	type result struct {
		id  quepaxa.NodeID
		err error
	}
	results := make(chan result, len(union.Members)-1)
	pending := 0
	for _, member := range union.Members {
		if member.ID == t.localID {
			continue
		}
		pending++
		go func(member quepaxa.Member) {
			req := peer.request(peerfb.OperationLearned)
			req.Decision = decisionToWire(decision)
			_, err := peer.callQuorum(ctx, member.ID, req, false)
			results <- result{id: member.ID, err: err}
		}(member)
	}
	oldAcks, nextAcks := 0, 0
	if _, ok := oldMembers[t.localID]; ok {
		oldAcks++
	}
	if _, ok := nextMembers[t.localID]; ok {
		nextAcks++
	}
	added := make(map[quepaxa.NodeID]bool)
	for id := range nextMembers {
		if _, ok := oldMembers[id]; !ok {
			added[id] = false
		}
	}
	var firstErr error
	for range pending {
		result := <-results
		if result.err != nil {
			if firstErr == nil {
				firstErr = result.err
			}
			if _, required := added[result.id]; required {
				return result.err
			}
			continue
		}
		if _, ok := oldMembers[result.id]; ok {
			oldAcks++
		}
		if _, ok := nextMembers[result.id]; ok {
			nextAcks++
		}
		if _, required := added[result.id]; required {
			added[result.id] = true
		}
		allAdded := true
		for _, acknowledged := range added {
			if !acknowledged {
				allAdded = false
				break
			}
		}
		if allAdded && oldAcks >= old.QuorumSize() && nextAcks >= next.QuorumSize() {
			return nil
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
	peer, err := t.transportForCurrent()
	if err != nil {
		return 0, err
	}
	if peer != t {
		return peer.ReadTip(ctx, to)
	}
	response, err := t.callQuorum(ctx, to, t.request(peerfb.OperationReadIndex), false)
	if err != nil {
		return 0, err
	}
	if response.ClusterId != string(t.clusterID) || response.ProposerId != string(to) || response.ConfigId != uint64(t.configID) {
		return 0, fmt.Errorf("read-index source identity mismatch")
	}
	return quepaxa.Slot(response.Tip), nil
}

func (t *Transport) StageValue(ctx context.Context, to quepaxa.NodeID, hash quepaxa.ValueHash, value []byte) error {
	peer, err := t.transportForCurrent()
	if err != nil {
		return err
	}
	if peer != t {
		return peer.StageValue(ctx, to, hash, value)
	}
	request := t.request(peerfb.OperationStageValue)
	request.Hash = append([]byte(nil), hash[:]...)
	request.Value = append([]byte(nil), value...)
	_, err = t.call(ctx, to, request, false)
	return err
}

func (t *Transport) FetchValue(ctx context.Context, from quepaxa.NodeID, hash quepaxa.ValueHash) ([]byte, error) {
	peer, err := t.transportForCurrent()
	if err != nil {
		return nil, err
	}
	if peer != t {
		return peer.FetchValue(ctx, from, hash)
	}
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

// VerifyLearner asks the candidate's own QUIC endpoint to prove that its WAL
// durably contains the exact certified prefix. It is an admission check only;
// it neither copies data nor changes membership.
func (t *Transport) VerifyLearner(ctx context.Context, learner quepaxa.Member, through quepaxa.Slot, prefix quepaxa.ValueHash) error {
	if learner.ID == "" || learner.Token == "" || through == 0 {
		return fmt.Errorf("learner identity and prefix are required")
	}
	identity, err := decodeWALIdentity(learner.WALIdentity)
	if err != nil {
		return fmt.Errorf("invalid learner WAL identity: %w", err)
	}
	config := t.currentCluster()
	if _, ok := config.MemberSet()[t.localID]; !ok {
		return fmt.Errorf("local node is not an active voter")
	}
	if _, ok := config.MemberSet()[learner.ID]; ok {
		return fmt.Errorf("learner is already an active voter")
	}
	probeConfig := config
	probeConfig.Members = append(append([]quepaxa.Member(nil), config.Members...), learner)
	probe := newTransport(t.clusterID, t.localID, &probeConfig, t.fallback, nil)
	defer probe.Close()
	request := probe.request(peerfb.OperationVerifyPrefix)
	request.ConfigId = uint64(config.ConfigID)
	request.From, request.Hash = uint64(through), append([]byte(nil), prefix[:]...)
	request.Value = append([]byte(nil), identity[:]...)
	response, err := probe.call(ctx, learner.ID, request, false)
	if err != nil {
		return err
	}
	if response.ClusterId != string(t.clusterID) || response.ProposerId != string(learner.ID) || response.ConfigId != uint64(config.ConfigID) || response.Tip != uint64(through) {
		return fmt.Errorf("learner durable-prefix identity mismatch")
	}
	return nil
}

func decodeWALIdentity(value string) ([sha256.Size]byte, error) {
	var identity [sha256.Size]byte
	if len(value) != 2*len(identity) {
		return identity, fmt.Errorf("must be %d hex characters", 2*len(identity))
	}
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return identity, err
	}
	if hex.EncodeToString(decoded) != value {
		return identity, fmt.Errorf("must be canonical lowercase hex")
	}
	copy(identity[:], decoded)
	return identity, nil
}

func matchesWALIdentity(actual string, expected []byte) bool {
	identity, err := decodeWALIdentity(actual)
	return err == nil && len(expected) == len(identity) && subtle.ConstantTimeCompare(expected, identity[:]) == 1
}

func (t *Transport) Close() error {
	t.dynamicMu.Lock()
	t.closed = true
	peers := make([]*Transport, 0, len(t.dynamic))
	for _, peer := range t.dynamic {
		peers = append(peers, peer)
	}
	clear(t.dynamic)
	t.dynamicMu.Unlock()
	for _, peer := range peers {
		_ = peer.Close()
	}
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
