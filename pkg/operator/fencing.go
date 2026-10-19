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
//
// NodeName, NodeUID, ContainerID and ImageID are the immutable runtime identity
// observed before the fault. They stay optional for the cooperative path and are
// required by independent evidence: a Pod name, or a lookup performed after the
// fault, is never substituted for the incarnation the operator captured.
type FenceTarget struct {
	NodeID      string `json:"nodeID"`
	Pod         string `json:"pod"`
	PodUID      string `json:"podUID"`
	WALIdentity string `json:"walIdentity"`
	NodeName    string `json:"nodeName,omitempty"`
	NodeUID     string `json:"nodeUID,omitempty"`
	ContainerID string `json:"containerID,omitempty"`
	ImageID     string `json:"imageID,omitempty"`
}

// capturedFenceIdentity adds the immutable runtime identity observed in a Pod to
// an already authenticated target. A Pod that never reported its container
// identity keeps empty fields and stays ineligible for independent evidence.
func capturedFenceIdentity(target FenceTarget, pod object) FenceTarget {
	target.NodeName = str(nested(pod, "spec", "nodeName"))
	if statuses := list(nested(pod, "status", "containerStatuses")); len(statuses) == 1 {
		status := asObject(statuses[0])
		target.ContainerID = str(status["containerID"])
		target.ImageID = str(status["imageID"])
	}
	return target
}

// Scope is "Generation" or "Voter".
type Scope string

const (
	ScopeGeneration Scope = "Generation"
	ScopeVoter      Scope = "Voter"
)

// fenceTargetCapacity mirrors RhizaFence.spec.targets.maxItems in
// deploy/operator/fence-crd.yaml. One fence must name every voter plus the
// retained learner history, so a larger scope cannot be executed at all.
const fenceTargetCapacity = 64

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
	// Independent is the structured evidence an external attestor produced for
	// this exact closed operation. It is additive: it never replaces the
	// cooperative ledger, and the authority attaches it only after validating
	// every binding against that ledger.
	Independent *IndependentEvidence `json:"independent,omitempty"`
}

// independentEvidenceVersion is the only supported evidence schema.
const independentEvidenceVersion = 1

// Independent runtime states. API absence, a DELETE response and an unreachable
// runtime are deliberately not representable: none of them is termination proof.
const (
	RuntimeStateExited     = "exited"
	RuntimeStateTerminated = "terminated"
)

// IndependentEvidence is one external attestor's structured proof for a closed
// fencing operation. Every field binds the exact request, durable ledger,
// immutable runtime incarnation, recreation barrier and storage scope the
// authority observed; anything else must stay pending.
type IndependentEvidence struct {
	Version     int               `json:"version"`
	Provider    string            `json:"provider"`
	Mode        string            `json:"mode"`
	ObservedAt  string            `json:"observedAt"`
	RequestHash string            `json:"requestHash"`
	LedgerHash  string            `json:"ledgerHash"`
	BarrierID   string            `json:"barrierID"`
	Barrier     EvidenceBarrier   `json:"barrier"`
	Runtimes    []EvidenceRuntime `json:"runtimes"`
	Storage     EvidenceStorage   `json:"storage"`
}

// EvidenceBarrier proves the durable recreation barrier was still refusing the
// closed incarnation after termination, together with the exact Pod inventory
// that existed while it refused.
type EvidenceBarrier struct {
	Kind              string   `json:"kind"`
	ClosedOperationID string   `json:"closedOperationID"`
	ClosedRequestHash string   `json:"closedRequestHash"`
	CurrentPodUIDs    []string `json:"currentPodUIDs"`
	DeniedAt          string   `json:"deniedAt"`
}

// EvidenceRuntime is the terminal state of exactly one fenced Pod incarnation.
type EvidenceRuntime struct {
	NodeName        string   `json:"nodeName"`
	NodeUID         string   `json:"nodeUID"`
	InstanceID      string   `json:"instanceID"`
	PodName         string   `json:"podName"`
	PodUID          string   `json:"podUID"`
	ContainerIDs    []string `json:"containerIDs"`
	ImageIDs        []string `json:"imageIDs"`
	RunIDs          []string `json:"runIDs"`
	WALIdentities   []string `json:"walIdentities"`
	State           string   `json:"state"`
	RuntimeEvidence string   `json:"runtimeEvidence"`
}

// EvidenceStorage is an explicitly labelled stability observation, not a clean
// engine drain: it states that no writer from the closed scope remains and that
// the final object state did not change across the quiet window.
type EvidenceStorage struct {
	Provider           string `json:"provider"`
	Bucket             string `json:"bucket"`
	Prefix             string `json:"prefix"`
	QuietWindowSeconds int    `json:"quietWindowSeconds"`
	HeadGeneration     string `json:"headGeneration"`
	CurrentGeneration  string `json:"currentGeneration"`
	InventoryDigest    string `json:"inventoryDigest"`
	Stable             bool   `json:"stable"`
	ObservedFrom       string `json:"observedFrom"`
	ObservedTo         string `json:"observedTo"`
}

// AttestationRequest is the exact scope the authority sends after it has durably
// closed the ledger. It carries no credentials and no free-form authority.
type AttestationRequest struct {
	Request        FenceRequest          `json:"request"`
	RequestHash    string                `json:"requestHash"`
	LedgerHash     string                `json:"ledgerHash"`
	StatefulSetUID string                `json:"statefulSetUID"`
	Runs           []AttestationRun      `json:"runs"`
	CurrentPodUIDs []string              `json:"currentPodUIDs"`
	StorageScope   AttestationStoreScope `json:"storageScope"`
}

// AttestationRun is one closed ledger run the attestor must account for.
type AttestationRun struct {
	RunID       string `json:"runID"`
	NodeID      string `json:"nodeID"`
	PodUID      string `json:"podUID"`
	WALIdentity string `json:"walIdentity"`
}

// AttestationStoreScope is the exact object-store location of the fenced source
// generation, derived from the workload configuration rather than a global guess.
type AttestationStoreScope struct {
	Provider string `json:"provider"`
	Bucket   string `json:"bucket"`
	Prefix   string `json:"prefix"`
}

// AttestationResponse is the attestor's proof identity fields plus the
// independent evidence they are bound to. The authority, never the attestor,
// writes the resulting RhizaFence status.
type AttestationResponse struct {
	Proof       FenceProof          `json:"proof"`
	Independent IndependentEvidence `json:"independent"`
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
	return fenceCapacityError(len(req.Targets))
}

// fenceCapacityError rejects a scope the Kubernetes fencing backend must refuse.
func fenceCapacityError(targets int) error {
	if targets <= fenceTargetCapacity {
		return nil
	}
	return fmt.Errorf("fencing: %d targets exceed the fence capacity of %d", targets, fenceTargetCapacity)
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
