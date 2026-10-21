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
	binding  *BindReceipt
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
	var receipt BindReceipt
	if err := s.call(ctx, "/v1/bind", bound, &receipt); err != nil {
		return err
	}
	if err := receipt.Validate(bound); err != nil {
		return err
	}
	if s.binding != nil && !s.binding.SameBinding(receipt) {
		return fmt.Errorf("fencing: source producer binding changed")
	}
	copy := receipt.clone()
	s.binding = &copy
	s.identity = bound
	return nil
}

// MutationBinding returns an immutable copy of the validated journal binding.
// Presence is not a dispatch right or proof that a source retry file is durable.
func (s *Session) MutationBinding() (ProducerBinding, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.binding == nil || s.binding.Producer == nil {
		return ProducerBinding{}, false
	}
	return *s.binding.Producer, true
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
