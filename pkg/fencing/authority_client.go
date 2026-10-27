package fencing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// AuthorityMessage is sent only over authenticated TLS. The server authenticates
// the bound service account token independently of the claimed identity.
type AuthorityMessage struct {
	Action      string              `json:"action"`
	Identity    Identity            `json:"identity"`
	Drain       *DrainReceipt       `json:"drain,omitempty"`
	Request     *StopRequest        `json:"request,omitempty"`
	PeerClosure *PeerClosureRequest `json:"peerClosure,omitempty"`
}

type AuthorityClient struct {
	URL       string
	TokenFile string
	Client    *http.Client
}

var _ PeerClosureAuthority = (*AuthorityClient)(nil)

func (a *AuthorityClient) call(ctx context.Context, message AuthorityMessage, result any) error {
	return a.callURL(ctx, a.URL, message, result)
}

func (a *AuthorityClient) callPeerClosure(ctx context.Context, endpointPath string, message AuthorityMessage, result any) error {
	u, err := url.Parse(a.URL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("fencing: HTTPS authority required")
	}
	if !strings.HasSuffix(u.Path, "/v1/supervisor") {
		return fmt.Errorf("fencing: configured authority URL must end in /v1/supervisor")
	}
	u.Path = strings.TrimSuffix(u.Path, "/v1/supervisor") + endpointPath
	u.RawPath = ""
	return a.callURL(ctx, u.String(), message, result)
}

func (a *AuthorityClient) callURL(ctx context.Context, endpoint string, message AuthorityMessage, result any) error {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("fencing: HTTPS authority required")
	}
	token, err := os.ReadFile(a.TokenFile)
	if err != nil {
		return err
	}
	if len(bytes.TrimSpace(token)) == 0 {
		return fmt.Errorf("fencing: empty authority token")
	}
	body, err := json.Marshal(message)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	req.Header.Set("Content-Type", "application/json")
	if a.Client == nil {
		return fmt.Errorf("fencing: bounded TLS client required")
	}
	// Never forward a projected token to a redirect destination.
	client := *a.Client
	if client.Timeout <= 0 || client.Timeout > 30*time.Second {
		client.Timeout = 30 * time.Second
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusAccepted {
		return ErrPending
	}
	if resp.StatusCode == http.StatusGone {
		return ErrClosed
	}
	if resp.StatusCode == http.StatusNotFound && message.Action == "peer-closure-status" {
		return ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
			return ErrUnavailable
		}
		return fmt.Errorf("fencing: authority HTTP %d", resp.StatusCode)
	}
	if result == nil {
		return nil
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 65537))
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if len(data) > 65536 {
		return fmt.Errorf("fencing: authority response too large")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(result); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("fencing: trailing authority response")
	}
	return nil
}

func (a *AuthorityClient) Register(ctx context.Context, id Identity) error {
	return a.call(ctx, AuthorityMessage{Action: "register", Identity: id}, nil)
}
func (a *AuthorityClient) Bind(ctx context.Context, id Identity) error {
	return a.call(ctx, AuthorityMessage{Action: "bind", Identity: id}, nil)
}
func (a *AuthorityClient) Stop(ctx context.Context, id Identity) (StopRequest, error) {
	var request StopRequest
	err := a.call(ctx, AuthorityMessage{Action: "stop", Identity: id}, &request)
	return request, err
}
func (a *AuthorityClient) Drain(ctx context.Context, r DrainReceipt) error {
	return a.call(ctx, AuthorityMessage{Action: "drain", Identity: r.Request.Identity, Drain: &r}, nil)
}
func (a *AuthorityClient) Exit(ctx context.Context, r StopRequest) error {
	return a.call(ctx, AuthorityMessage{Action: "exit", Identity: r.Identity, Request: &r}, nil)
}

func (a *AuthorityClient) NeverBoundExit(ctx context.Context, r StopRequest) error {
	return a.call(ctx, AuthorityMessage{Action: "never-bound-exit", Identity: r.Identity, Request: &r}, nil)
}

func (a *AuthorityClient) RequestPeerClosure(ctx context.Context, caller Identity, closure PeerClosureRequest) (PeerClosureReceipt, error) {
	if err := closure.Validate(caller); err != nil {
		return PeerClosureReceipt{}, err
	}
	var receipt PeerClosureReceipt
	err := a.callPeerClosure(ctx, "/v1/peer-closure/request", AuthorityMessage{Action: "peer-closure-request", Identity: caller, PeerClosure: &closure}, &receipt)
	if err != nil {
		return PeerClosureReceipt{}, err
	}
	if err := validatePeerClosureReceipt(caller, closure, receipt); err != nil {
		return PeerClosureReceipt{}, err
	}
	return receipt, nil
}

func (a *AuthorityClient) ReadPeerClosure(ctx context.Context, caller Identity, closure PeerClosureRequest) (PeerClosureReceipt, error) {
	if err := closure.Validate(caller); err != nil {
		return PeerClosureReceipt{}, err
	}
	var receipt PeerClosureReceipt
	err := a.callPeerClosure(ctx, "/v1/peer-closure/status", AuthorityMessage{Action: "peer-closure-status", Identity: caller, PeerClosure: &closure}, &receipt)
	if err != nil {
		return PeerClosureReceipt{}, err
	}
	if err := validatePeerClosureReceipt(caller, closure, receipt); err != nil {
		return PeerClosureReceipt{}, err
	}
	return receipt, nil
}

func validatePeerClosureReceipt(caller Identity, request PeerClosureRequest, receipt PeerClosureReceipt) error {
	if err := request.Validate(caller); err != nil {
		return err
	}
	if receipt.ExpectedAbortSlot == nil || receipt.OperationID == "" || receipt.SourceClusterID != caller.SourceClusterID || receipt.BindingUID == "" || receipt.StatefulSetUID == "" || receipt.RequestHash == "" || receipt.ProofID == "" ||
		receipt.ExpectedConfigID != request.ExpectedConfigID || *receipt.ExpectedAbortSlot != *request.ExpectedAbortSlot || receipt.TargetNodeID != request.TargetNodeID || receipt.TargetWALIdentity != request.TargetWALIdentity ||
		!receipt.ProcessesTerminated || !receipt.RecreationBlocked || !receipt.StorageQuiesced || len(receipt.Targets) == 0 {
		return fmt.Errorf("fencing: peer-closure receipt does not match request")
	}
	covered := false
	for _, target := range receipt.Targets {
		if target.NodeID != request.TargetNodeID || target.WALIdentity != request.TargetWALIdentity || target.Pod != request.TargetNodeID || target.PodUID == "" {
			return fmt.Errorf("fencing: peer-closure receipt target differs")
		}
		covered = true
	}
	if !covered {
		return fmt.Errorf("fencing: peer-closure receipt omitted target")
	}
	return nil
}
