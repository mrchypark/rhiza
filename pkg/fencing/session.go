package fencing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"
)

// Session authenticates one application child to its local supervisor. The
// supervisor, not the application, reserves the durable incarnation before spawn.
type Session struct {
	client   *http.Client
	ticket   string
	mu       sync.Mutex
	identity Identity
}

// Connect must complete before opening a node or performing storage mutations.
func Connect(ctx context.Context, socketPath, ticket, sourceClusterID, nodeID string) (*Session, error) {
	if socketPath == "" || ticket == "" || sourceClusterID == "" || nodeID == "" {
		return nil, fmt.Errorf("fencing: complete startup configuration is required")
	}
	s := &Session{ticket: ticket, client: &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
		}},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
	var identity Identity
	if err := s.call(ctx, "/v1/startup", Identity{SourceClusterID: sourceClusterID, NodeID: nodeID}, &identity); err != nil {
		return nil, err
	}
	if identity.SourceClusterID != sourceClusterID || identity.NodeID != nodeID || identity.PodUID == "" || identity.RunID == "" || identity.WALIdentity != "" {
		return nil, fmt.Errorf("fencing: invalid startup identity")
	}
	s.identity = identity
	return s, nil
}

func (s *Session) Identity() Identity {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.identity
}

// BindWAL binds once, after opening the WAL but before starting any writer.
func (s *Session) BindWAL(ctx context.Context, walIdentity string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if walIdentity == "" || (s.identity.WALIdentity != "" && s.identity.WALIdentity != walIdentity) {
		return fmt.Errorf("fencing: WAL identity cannot change")
	}
	bound := s.identity
	bound.WALIdentity = walIdentity
	if err := s.call(ctx, "/v1/bind", bound, nil); err != nil {
		return err
	}
	s.identity = bound
	return nil
}

// WaitStop waits for an immutable terminal request. A pending response is not
// authorization to restart or reopen the node.
func (s *Session) WaitStop(ctx context.Context) (StopRequest, error) {
	identity := s.Identity()
	var request StopRequest
	if err := s.call(ctx, "/v1/stop", identity, &request); err != nil {
		return StopRequest{}, err
	}
	if request.Identity != identity || request.OperationID == "" || request.RequestHash == "" {
		return StopRequest{}, fmt.Errorf("fencing: stop request identity mismatch")
	}
	return request, nil
}

func (s *Session) AcknowledgeDrain(ctx context.Context, receipt DrainReceipt) error {
	if receipt.Request.Identity != s.Identity() || receipt.Request.OperationID == "" || receipt.Request.RequestHash == "" {
		return fmt.Errorf("fencing: drain identity mismatch")
	}
	return s.call(ctx, "/v1/drained", receipt, nil)
}

// RequestPeerClosure asks the bound supervisor to submit one exact voter-slot
// closure. The local session identity remains the caller; target identity is a
// separate field and cannot be substituted for the caller.
func (s *Session) RequestPeerClosure(ctx context.Context, closure PeerClosureRequest) (PeerClosureReceipt, error) {
	return s.peerClosureCall(ctx, "/v1/peer-closure/request", "peer-closure-request", closure)
}

// ReadPeerClosure reads only an already persisted exact operation. A missing
// record is reported as ErrNotFound and must never trigger request creation.
func (s *Session) ReadPeerClosure(ctx context.Context, closure PeerClosureRequest) (PeerClosureReceipt, error) {
	return s.peerClosureCall(ctx, "/v1/peer-closure/status", "peer-closure-status", closure)
}

func (s *Session) peerClosureCall(ctx context.Context, path, action string, closure PeerClosureRequest) (PeerClosureReceipt, error) {
	caller := s.Identity()
	if err := closure.Validate(caller); err != nil {
		return PeerClosureReceipt{}, err
	}
	message := AuthorityMessage{Action: action, Identity: caller, PeerClosure: &closure}
	var receipt PeerClosureReceipt
	if err := s.call(ctx, path, message, &receipt); err != nil {
		return PeerClosureReceipt{}, err
	}
	if err := validatePeerClosureReceipt(caller, closure, receipt); err != nil {
		return PeerClosureReceipt{}, err
	}
	return receipt, nil
}

func (s *Session) call(ctx context.Context, path string, value, out any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://supervisor"+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+s.ticket)
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusAccepted:
		return ErrPending
	case http.StatusGone:
		return ErrClosed
	case http.StatusNotFound:
		if path == "/v1/peer-closure/status" {
			return ErrNotFound
		}
	}
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
			return ErrUnavailable
		}
		return fmt.Errorf("fencing: supervisor returned HTTP %d", resp.StatusCode)
	}
	if out == nil {
		return nil
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 65537))
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if len(data) > 65536 {
		return fmt.Errorf("fencing: supervisor response too large")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("fencing: trailing supervisor response")
	}
	return nil
}
