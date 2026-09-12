package operator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// ErrFencePending is returned when the fencing service accepts
// the request (HTTP 202) and the proof is not yet ready.
var ErrFencePending = errors.New("fencing: operation pending")

// FenceTarget records an observed incarnation. Backends must fence the requested
// voter or generation authority, including recreation, not just this Pod UID.
type FenceTarget struct {
	NodeID      string `json:"nodeID"`
	Pod         string `json:"pod"`
	PodUID      string `json:"podUID"`
	WALIdentity string `json:"walIdentity"`
}

// Scope is "Generation" or "Voter".
type Scope string

const (
	ScopeGeneration Scope = "Generation"
	ScopeVoter      Scope = "Voter"
)

type FenceRequest struct {
	LogicalID       string        `json:"logicalID"`
	BindingUID      string        `json:"bindingUID"`
	Namespace       string        `json:"namespace"`
	StatefulSet     string        `json:"statefulSet"`
	StatefulSetUID  string        `json:"statefulSetUID"`
	SourceClusterID string        `json:"sourceClusterID"`
	OperationID     string        `json:"operationID"`
	Scope           Scope         `json:"scope"`
	Targets         []FenceTarget `json:"targets"`
}

type FenceProof struct {
	OperationID         string `json:"operationID"`
	SourceClusterID     string `json:"sourceClusterID"`
	BindingUID          string `json:"bindingUID"`
	RequestHash         string `json:"requestHash"`
	ProofID             string `json:"proofID"`
	ProcessesTerminated bool   `json:"processesTerminated"`
	RecreationBlocked   bool   `json:"recreationBlocked"`
	StorageQuiesced     bool   `json:"storageQuiesced"`
}

// Fencer executes an immutable operation owned by the Operator. A pending or
// uncertain isolation must return an error, never a successful partial proof.
type Fencer interface {
	Fence(context.Context, FenceRequest) (*FenceProof, error)
}

// FencingClient is the HTTPS backend; it does not own recovery decisions.
type FencingClient struct {
	URL       string
	TokenFile string
	Client    *http.Client
}

const maxResponseBytes = 64 * 1024

const maxFenceTimeout = 10 * time.Second

func requestHash(req *FenceRequest) (string, error) {
	b, err := json.Marshal(req)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return fmt.Sprintf("%x", h), nil
}

func validateURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || raw == "" {
		return fmt.Errorf("fencing: invalid endpoint")
	}
	if u.Scheme != "https" {
		return fmt.Errorf("fencing: endpoint must use https")
	}
	if u.Host == "" {
		return fmt.Errorf("fencing: endpoint host is empty")
	}
	if u.User != nil {
		return fmt.Errorf("fencing: endpoint must not contain credentials")
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("fencing: endpoint must not contain query or fragment")
	}
	return nil
}

func cloneClient(c *http.Client) *http.Client {
	if c == nil {
		return &http.Client{
			Timeout: maxFenceTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}
	cc := *c
	cc.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	if cc.Timeout == 0 || cc.Timeout > maxFenceTimeout {
		cc.Timeout = maxFenceTimeout
	}
	return &cc
}

func validateRequest(req *FenceRequest) error {
	if req.OperationID == "" {
		return fmt.Errorf("fencing: operationID is required")
	}
	if req.SourceClusterID == "" {
		return fmt.Errorf("fencing: sourceClusterID is required")
	}
	if req.BindingUID == "" {
		return fmt.Errorf("fencing: bindingUID is required")
	}
	if req.Scope != ScopeGeneration && req.Scope != ScopeVoter {
		return fmt.Errorf("fencing: invalid scope %q", req.Scope)
	}
	return nil
}

func readToken(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("fencing: read token")
	}
	tok := strings.TrimSpace(string(b))
	if tok == "" {
		return "", fmt.Errorf("fencing: empty token")
	}
	return tok, nil
}

func (fc *FencingClient) Fence(ctx context.Context, req FenceRequest) (*FenceProof, error) {
	if err := validateURL(fc.URL); err != nil {
		return nil, err
	}
	if err := validateRequest(&req); err != nil {
		return nil, err
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("fencing: marshal request: %w", err)
	}
	c := cloneClient(fc.Client)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, fc.URL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("fencing: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Idempotency-Key", req.OperationID)
	token, err := readToken(fc.TokenFile)
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Authorization", "Bearer "+token)
	resp, err := c.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("fencing: request failed")
	}
	defer resp.Body.Close()
	limited := io.LimitReader(resp.Body, maxResponseBytes+1)
	respBody, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("fencing: read response: %w", err)
	}
	if int64(len(respBody)) > maxResponseBytes {
		return nil, fmt.Errorf("fencing: response exceeds %d bytes", maxResponseBytes)
	}
	if resp.StatusCode == http.StatusAccepted {
		return nil, ErrFencePending
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fencing: HTTP %d", resp.StatusCode)
	}
	var proof FenceProof
	dec := json.NewDecoder(bytes.NewReader(respBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&proof); err != nil {
		return nil, fmt.Errorf("fencing: decode proof: %w", err)
	}
	if dec.Decode(new(any)) != io.EOF {
		return nil, fmt.Errorf("fencing: trailing data after proof")
	}
	if err := validateFenceProof(req, &proof); err != nil {
		return nil, err
	}
	return &proof, nil
}

// Validate independently of the backend before authorizing recovery effects.
func validateFenceProof(req FenceRequest, proof *FenceProof) error {
	if proof == nil {
		return fmt.Errorf("fencing: missing proof")
	}
	hash, err := requestHash(&req)
	if err != nil {
		return err
	}
	if proof.OperationID != req.OperationID {
		return fmt.Errorf("fencing: proof operationID mismatch")
	}
	if proof.SourceClusterID != req.SourceClusterID {
		return fmt.Errorf("fencing: proof sourceClusterID mismatch")
	}
	if proof.BindingUID != req.BindingUID {
		return fmt.Errorf("fencing: proof bindingUID mismatch")
	}
	if proof.RequestHash != hash {
		return fmt.Errorf("fencing: proof requestHash mismatch")
	}
	if proof.ProofID == "" {
		return fmt.Errorf("fencing: proof has empty proofID")
	}
	if !proof.ProcessesTerminated || !proof.RecreationBlocked || !proof.StorageQuiesced {
		return fmt.Errorf("fencing: proof booleans incomplete")
	}
	return nil
}

func (c *Controller) fence(ctx context.Context, req FenceRequest) (*FenceProof, error) {
	if c.Fencer == nil {
		return nil, fmt.Errorf("fencing: backend is not configured")
	}
	if err := validateRequest(&req); err != nil {
		return nil, err
	}
	proof, err := c.Fencer.Fence(ctx, req)
	if err != nil {
		return nil, err
	}
	if err := validateFenceProof(req, proof); err != nil {
		return nil, err
	}
	return proof, nil
}
