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

	"github.com/mrchypark/rhiza/pkg/fencing"
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
	LogicalID        string                   `json:"logicalID"`
	BindingUID       string                   `json:"bindingUID"`
	Namespace        string                   `json:"namespace"`
	StatefulSet      string                   `json:"statefulSet"`
	StatefulSetUID   string                   `json:"statefulSetUID"`
	SourceClusterID  string                   `json:"sourceClusterID"`
	OperationID      string                   `json:"operationID"`
	Scope            Scope                    `json:"scope"`
	Targets          []FenceTarget            `json:"targets"`
	BootstrapGenesis *BootstrapGenesisBinding `json:"bootstrapGenesis,omitempty"`
}

// BootstrapGenesisBinding is present only for a fencer-authorized recovery of
// an automatic bootstrap blocked on its first canonical HEAD mutation.
type BootstrapGenesisBinding struct {
	Version         int    `json:"version"`
	CandidateDigest string `json:"candidateDigest"`
	ClusterName     string `json:"clusterName"`
	ClusterUID      string `json:"clusterUID"`
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
	Independent   *IndependentEvidence   `json:"independent,omitempty"`
	IndependentV2 *IndependentEvidenceV2 `json:"independentV2,omitempty"`
}

// IndependentEvidenceV2 refers to evidence verified and retained by the authority,
// not stability observations supplied by a workload or external HTTP response.
type IndependentEvidenceV2 struct {
	Version        int                         `json:"version"`
	RequestHash    string                      `json:"requestHash"`
	LedgerHash     string                      `json:"ledgerHash"`
	StorageScope   AttestationStoreScope       `json:"storageScope"`
	CurrentPodUIDs []string                    `json:"currentPodUIDs"`
	Terminals      []fencing.TerminalReference `json:"terminals"`
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
// engine drain: it binds the exact object-store location and two observations of
// the same two named objects. archive/head.bin and checkpoint/CURRENT are
// separate objects with independent per-object generation counters, so the
// invariant is that both of their generations, both metagenerations and the
// inventory digest are unchanged across the quiet window.
type EvidenceStorage struct {
	Provider               string          `json:"provider"`
	Endpoint               string          `json:"endpoint,omitempty"`
	Bucket                 string          `json:"bucket"`
	Prefix                 string          `json:"prefix"`
	Account                string          `json:"account,omitempty"`
	ConnectionStringDigest string          `json:"connectionStringDigest,omitempty"`
	QuietWindowSeconds     int             `json:"quietWindowSeconds"`
	Before                 StorageSnapshot `json:"before"`
	After                  StorageSnapshot `json:"after"`
	Stable                 bool            `json:"stable"`
}

// StorageSnapshot is one observation of the exact storage state. ObservedAt is
// the instant the provider read it, never a value the authority assumed.
type StorageSnapshot struct {
	ObservedAt            string `json:"observedAt"`
	HeadGeneration        string `json:"headGeneration"`
	HeadMetageneration    string `json:"headMetageneration"`
	CurrentGeneration     string `json:"currentGeneration"`
	CurrentMetageneration string `json:"currentMetageneration"`
	InventoryDigest       string `json:"inventoryDigest"`
}

// scope is the exact location identity this observation claims. It is compared
// against the workload's own scope, so a different store that merely shares a
// provider, bucket and prefix cannot satisfy the binding.
func (s EvidenceStorage) scope() AttestationStoreScope {
	return AttestationStoreScope{Provider: s.Provider, Endpoint: s.Endpoint, Bucket: s.Bucket, Prefix: s.Prefix, Account: s.Account, ConnectionStringDigest: s.ConnectionStringDigest}
}

// state is the same snapshot without its clock. Only the observed storage state
// has to be identical across the window; the clock is what measures the window.
func (s StorageSnapshot) state() StorageSnapshot {
	s.ObservedAt = ""
	return s
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
// It carries every non-secret field StoreIdentity compares, because the same
// provider, bucket and prefix can name different archives behind a custom
// endpoint or a different account.
type AttestationStoreScope struct {
	Provider               string `json:"provider"`
	Endpoint               string `json:"endpoint,omitempty"`
	Bucket                 string `json:"bucket"`
	Prefix                 string `json:"prefix"`
	Account                string `json:"account,omitempty"`
	ConnectionStringDigest string `json:"connectionStringDigest,omitempty"`
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

// ArchiveForkGenesisRegistrar binds a verified successor fork to the
// canonical archive authority. Cooperative backends may omit this capability;
// configured HTTPS authorities implement it and fail closed if unsupported.
type ArchiveForkGenesisRegistrar interface {
	RegisterArchiveForkGenesis(context.Context, ArchiveForkGenesisRequest) error
}

// ArchiveEmptyRecoveryRegistrar authorizes or confirms a fresh empty target.
// It reports committed only after the target's real archive genesis receipt is
// canonical; authorized alone permits activation but never completion.
type ArchiveEmptyRecoveryRegistrar interface {
	RegisterArchiveEmptyRecovery(context.Context, ArchiveEmptyRecoveryRequest) (ArchiveEmptyRecoveryResponse, error)
}

// FencingClient is the HTTPS backend; it does not own recovery decisions.
type FencingClient struct {
	URL       string
	TokenFile string
	Client    *http.Client
}

// A 64-run v2 proof carries immutable references, not just three booleans.
// Keep the bound finite while accommodating the complete supported cohort.
const maxResponseBytes = 512 * 1024

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
	if req.BootstrapGenesis != nil {
		binding := req.BootstrapGenesis
		if binding.Version != 1 || !validEvidenceDigest(binding.CandidateDigest) || binding.ClusterName == "" || len(binding.ClusterName) > 253 || binding.ClusterUID == "" || len(binding.ClusterUID) > 256 {
			return fmt.Errorf("fencing: bootstrap genesis binding is invalid")
		}
		if req.Scope != ScopeGeneration || len(req.Targets) == 0 {
			return fmt.Errorf("fencing: bootstrap genesis requires a generation-wide captured cohort")
		}
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

// RegisterArchiveForkGenesis submits only the canonical identifiers. The
// authority independently reads and verifies the fork evidence before binding
// it; the controller never supplies a caller-asserted proof.
func (fc *FencingClient) RegisterArchiveForkGenesis(ctx context.Context, request ArchiveForkGenesisRequest) error {
	if err := validateURL(fc.URL); err != nil {
		return err
	}
	if request.StatefulSetUID == "" || request.TargetClusterID == "" || request.OperationID == "" || request.SourceFenceOperationID == "" {
		return fmt.Errorf("fencing: archive fork genesis identifiers are required")
	}
	if request.EvidenceKind == ManualTerminalEvidenceKind {
		if request.RecoveryMode != RecoveryModeFrozenSource || request.ManualMembershipBindingDigest != "" ||
			!validEvidenceDigest(request.ManualRecoveryChallengeDigest) || !validEvidenceDigest(request.ManualTerminalAttestationDigest) {
			return fmt.Errorf("fencing: manual terminal archive fork bindings are incomplete")
		}
	} else if request.EvidenceKind != "" || request.ManualRecoveryChallengeDigest != "" || request.ManualTerminalAttestationDigest != "" {
		return fmt.Errorf("fencing: archive fork carries an unsupported terminal evidence binding")
	}
	body, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("fencing: marshal archive fork genesis request: %w", err)
	}
	endpoint, err := url.Parse(fc.URL)
	if err != nil {
		return fmt.Errorf("fencing: invalid endpoint")
	}
	endpoint.Path = "/v1/archive-genesis/fork/register"
	endpoint.RawPath = ""
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("fencing: build archive fork genesis request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Idempotency-Key", request.OperationID)
	token, err := readToken(fc.TokenFile)
	if err != nil {
		return err
	}
	httpReq.Header.Set("Authorization", "Bearer "+token)
	resp, err := cloneClient(fc.Client).Do(httpReq)
	if err != nil {
		return fmt.Errorf("fencing: archive fork genesis request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusAccepted {
		return ErrFencePending
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("fencing: archive fork genesis HTTP %d", resp.StatusCode)
	}
	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return fmt.Errorf("fencing: read archive fork genesis response: %w", err)
	}
	if int64(len(responseBody)) > maxResponseBytes {
		return fmt.Errorf("fencing: archive fork genesis response exceeds %d bytes", maxResponseBytes)
	}
	var result struct {
		Profile           string `json:"profile"`
		GenesisRegistered bool   `json:"genesisRegistered"`
	}
	decoder := json.NewDecoder(bytes.NewReader(responseBody))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil || decoder.Decode(new(any)) != io.EOF {
		return fmt.Errorf("fencing: invalid archive fork genesis profile response")
	}
	switch {
	case result.Profile == "independent" && result.GenesisRegistered:
		return nil
	case result.Profile == "cooperative" && !result.GenesisRegistered:
		return nil
	default:
		return fmt.Errorf("fencing: archive fork genesis profile response is inconsistent")
	}
}

// RegisterArchiveEmptyRecovery submits identifiers and the exact policy
// snapshot. The authority reads the stored fence, source archive, and target
// membership itself; the controller does not provide a caller-asserted proof.
func (fc *FencingClient) RegisterArchiveEmptyRecovery(ctx context.Context, request ArchiveEmptyRecoveryRequest) (ArchiveEmptyRecoveryResponse, error) {
	if err := validateURL(fc.URL); err != nil {
		return ArchiveEmptyRecoveryResponse{}, err
	}
	if request.RecoveryName == "" || request.StatefulSetUID == "" || request.TargetClusterID == "" || request.OperationID == "" ||
		request.SourceFenceOperationID == "" || !validEvidenceDigest(request.TargetMembershipHash) ||
		(request.TargetDurability != "async" && request.TargetDurability != "before-ack") ||
		!validEvidenceDigest(request.PolicyDigest) || request.SourceFenceOperationID == request.OperationID {
		return ArchiveEmptyRecoveryResponse{}, fmt.Errorf("fencing: empty recovery request identity or policy is invalid")
	}
	body, err := json.Marshal(request)
	if err != nil {
		return ArchiveEmptyRecoveryResponse{}, fmt.Errorf("fencing: marshal empty recovery request: %w", err)
	}
	endpoint, err := url.Parse(fc.URL)
	if err != nil {
		return ArchiveEmptyRecoveryResponse{}, fmt.Errorf("fencing: invalid endpoint")
	}
	endpoint.Path = "/v1/archive-genesis/empty-recovery/register"
	endpoint.RawPath = ""
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return ArchiveEmptyRecoveryResponse{}, fmt.Errorf("fencing: build empty recovery request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Idempotency-Key", request.OperationID)
	token, err := readToken(fc.TokenFile)
	if err != nil {
		return ArchiveEmptyRecoveryResponse{}, err
	}
	httpReq.Header.Set("Authorization", "Bearer "+token)
	resp, err := cloneClient(fc.Client).Do(httpReq)
	if err != nil {
		return ArchiveEmptyRecoveryResponse{}, fmt.Errorf("fencing: empty recovery request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusAccepted {
		return ArchiveEmptyRecoveryResponse{}, ErrFencePending
	}
	if resp.StatusCode != http.StatusOK {
		return ArchiveEmptyRecoveryResponse{}, fmt.Errorf("fencing: empty recovery HTTP %d", resp.StatusCode)
	}
	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return ArchiveEmptyRecoveryResponse{}, fmt.Errorf("fencing: read empty recovery response: %w", err)
	}
	if int64(len(responseBody)) > maxResponseBytes {
		return ArchiveEmptyRecoveryResponse{}, fmt.Errorf("fencing: empty recovery response exceeds %d bytes", maxResponseBytes)
	}
	var result ArchiveEmptyRecoveryResponse
	decoder := json.NewDecoder(bytes.NewReader(responseBody))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil || decoder.Decode(new(any)) != io.EOF {
		return ArchiveEmptyRecoveryResponse{}, fmt.Errorf("fencing: invalid empty recovery response")
	}
	if (result.State != fencing.EmptyRecoveryAuthorized && result.State != fencing.EmptyRecoveryCommitted) || result.Authorization.Validate() != nil {
		return ArchiveEmptyRecoveryResponse{}, fmt.Errorf("fencing: empty recovery response is not a valid authorization")
	}
	authorization := result.Authorization
	if authorization.StatefulSetUID != request.StatefulSetUID || authorization.TargetClusterID != request.TargetClusterID ||
		authorization.OperationID != request.OperationID || authorization.SourceFenceOperationID != request.SourceFenceOperationID ||
		authorization.TargetMembershipHash != request.TargetMembershipHash || authorization.TargetDurability != request.TargetDurability ||
		authorization.AllowDataLoss != request.AllowDataLoss || authorization.PolicyDigest != emptyRecoveryPolicyDigest(request.PolicyDigest, authorization.SourceDurability, request.TargetDurability, request.AllowDataLoss) {
		return ArchiveEmptyRecoveryResponse{}, fmt.Errorf("fencing: empty recovery authorization differs from request")
	}
	return result, nil
}

func emptyRecoveryPolicyDigest(specDigest, sourceDurability, targetDurability string, allowDataLoss bool) string {
	return hashJSON([]any{"rhiza-empty-recovery-policy-v1", specDigest, sourceDurability, targetDurability, allowDataLoss})
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
	if proof.Independent != nil {
		return fmt.Errorf("fencing: quiet-window independent evidence v1 cannot authorize recovery")
	}
	if strings.HasPrefix(proof.ProofID, "independent-v2-") && proof.IndependentV2 == nil {
		return fmt.Errorf("fencing: independent v2 evidence was omitted or pruned")
	}
	if proof.IndependentV2 != nil {
		if proof.ProofID != "independent-v2-"+hashJSON(proof.IndependentV2) {
			return fmt.Errorf("fencing: independent v2 proof identity differs from its evidence")
		}
		if err := validateIndependentV2(req, proof.IndependentV2); err != nil {
			return err
		}
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
