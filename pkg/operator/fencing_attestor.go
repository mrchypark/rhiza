package operator

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"time"
)

// minIndependentQuietWindowSeconds is the shortest storage stability window the
// authority accepts. It must exceed the maximum supported object-store batch
// delay plus one SDK request timeout, otherwise a stopped writer could still be
// buffered. The attestor, not this floor, owns the real window.
const minIndependentQuietWindowSeconds = 30

// attestationClockSkew is the only tolerance allowed on the snapshot clock, so
// a future-dated quiet window cannot authorize recovery before it elapsed.
const attestationClockSkew = 2 * time.Minute

// maxAttestationBytes bounds one attestation response. It stays above the
// largest evidence the published CRD schema permits (asserted by
// TestFenceCRDSchemaFitsTheAttestationTransport) and below the Kubernetes object
// limit, so a proof the schema allows is never rejected by the transport alone.
const maxAttestationBytes = 512 * 1024

// FenceAttestor returns independent evidence for one durably closed fencing
// operation. It is an external trust boundary: the authority keeps the fence
// pending unless the returned evidence binds the exact request, ledger, runtime
// identity, recreation barrier and storage scope it observed. A not-yet-ready
// attestation returns ErrFencePending.
type FenceAttestor interface {
	Attest(context.Context, AttestationRequest) (*AttestationResponse, error)
}

// HTTPSAttestor is the thin adapter to the separately operated attestor service.
// The real provider (VM/power, later node/CRI) lives behind that service, so no
// cloud SDK or cluster credential is linked into the operator.
type HTTPSAttestor struct {
	URL       string
	TokenFile string
	Client    *http.Client
}

// NewHTTPSAttestor builds the adapter from the RHIZA_FENCE_ATTESTOR_* settings.
// A partially configured trust relationship must fail startup instead of
// silently disabling independent evidence.
func NewHTTPSAttestor(rawURL, tokenFile, caFile string) (*HTTPSAttestor, error) {
	if err := validateURL(rawURL); err != nil {
		return nil, err
	}
	if tokenFile == "" {
		return nil, fmt.Errorf("fencing: attestor token file is required")
	}
	// The attestor CA is required and replaces the system pool: an unpinned
	// system root would silently accept any host instead of the reviewed one.
	if caFile == "" {
		return nil, fmt.Errorf("fencing: attestor CA file is required")
	}
	roots := x509.NewCertPool()
	data, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("fencing: read attestor CA")
	}
	if !roots.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("fencing: attestor CA contains no certificate")
	}
	client := &http.Client{Timeout: maxFenceTimeout, Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}}
	return &HTTPSAttestor{URL: rawURL, TokenFile: tokenFile, Client: client}, nil
}

// attestationKey scopes idempotency to one exact attested snapshot. A ledger or
// Pod inventory that changed since the previous call is a different attestation,
// so it must not replay a cached response or collide with a changed payload.
func attestationKey(req AttestationRequest) string {
	sum := hashJSON(req)
	return fmt.Sprintf("%s-%s", req.Request.OperationID, sum[:32])
}

// Attest posts one bound request. The attestor must answer 202 until its quiet
// window is complete; the authority re-invokes it with the same idempotency key
// on the next reconcile instead of holding the request open.
func (a *HTTPSAttestor) Attest(ctx context.Context, req AttestationRequest) (*AttestationResponse, error) {
	if a == nil {
		return nil, fmt.Errorf("fencing: attestor is not configured")
	}
	if err := validateURL(a.URL); err != nil {
		return nil, err
	}
	if req.RequestHash == "" || req.LedgerHash == "" || req.Request.OperationID == "" {
		return nil, fmt.Errorf("fencing: incomplete attestation request")
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("fencing: marshal attestation request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.URL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("fencing: build attestation request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Idempotency-Key", attestationKey(req))
	token, err := readToken(a.TokenFile)
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Authorization", "Bearer "+token)
	resp, err := cloneClient(a.Client).Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("fencing: attestation request failed")
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxAttestationBytes+1))
	if err != nil {
		return nil, fmt.Errorf("fencing: read attestation response: %w", err)
	}
	if len(respBody) > maxAttestationBytes {
		return nil, fmt.Errorf("fencing: attestation response exceeds %d bytes", maxAttestationBytes)
	}
	if resp.StatusCode == http.StatusAccepted {
		return nil, ErrFencePending
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fencing: attestation HTTP %d", resp.StatusCode)
	}
	var out AttestationResponse
	dec := json.NewDecoder(bytes.NewReader(respBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&out); err != nil {
		return nil, fmt.Errorf("fencing: decode attestation: %w", err)
	}
	if dec.Decode(new(any)) != io.EOF {
		return nil, fmt.Errorf("fencing: trailing data after attestation")
	}
	return &out, nil
}

// independentScope is the exact closed scope the authority requires the
// evidence to restate. It is derived from the durable ledger, the immutable
// request targets and the observed Pod inventory, never from the response.
type independentScope struct {
	request        FenceRequest
	requestHash    string
	ledgerHash     string
	currentPodUIDs []string
	targets        []FenceTarget
	runs           []AttestationRun
	storage        AttestationStoreScope
}

// buildIndependentScope bounds the exact runs this operation closed: the named
// voter slots for a voter fence, every run for a generation fence. It refuses to
// describe an incarnation that was never captured, so missing container, image
// or run coverage stays pending instead of being guessed from a Pod name.
func buildIndependentScope(state fenceLedger, request FenceRequest, nodes []string, requestHash, ledgerHash string, current []string, storage AttestationStoreScope) (independentScope, error) {
	scope := independentScope{request: request, requestHash: requestHash, ledgerHash: ledgerHash, currentPodUIDs: append([]string(nil), current...), targets: append([]FenceTarget(nil), request.Targets...), storage: storage}
	for _, target := range scope.targets {
		if target.NodeName == "" || target.ContainerID == "" || target.ImageID == "" || target.PodUID == "" {
			return scope, fmt.Errorf("fencing: target %q has no captured runtime identity", target.NodeID)
		}
	}
	wanted := map[string]bool{}
	for _, node := range nodes {
		wanted[node] = true
	}
	for runID, run := range state.Ledger.Runs {
		if len(wanted) > 0 && !wanted[run.Identity.NodeID] {
			continue
		}
		covered := false
		for _, target := range scope.targets {
			if target.NodeID == run.Identity.NodeID && target.PodUID == run.Identity.PodUID && target.WALIdentity == run.Identity.WALIdentity {
				covered = true
			}
		}
		if !covered {
			return scope, fmt.Errorf("fencing: closed ledger run %q is outside the captured target scope", runID)
		}
		scope.runs = append(scope.runs, AttestationRun{RunID: runID, NodeID: run.Identity.NodeID, PodUID: run.Identity.PodUID, WALIdentity: run.Identity.WALIdentity})
	}
	if len(scope.runs) == 0 {
		return scope, fmt.Errorf("fencing: closed ledger has no runs in scope")
	}
	sort.Slice(scope.runs, func(i, j int) bool { return scope.runs[i].RunID < scope.runs[j].RunID })
	sort.Strings(scope.currentPodUIDs)
	return scope, nil
}

// attestationRequest is the wire request for this exact scope.
func (s independentScope) attestationRequest() AttestationRequest {
	current := append([]string(nil), s.currentPodUIDs...)
	return AttestationRequest{Request: s.request, RequestHash: s.requestHash, LedgerHash: s.ledgerHash, StatefulSetUID: s.request.StatefulSetUID, Runs: append([]AttestationRun(nil), s.runs...), CurrentPodUIDs: current, StorageScope: s.storage}
}

// validateIndependent accepts a proof only when its identity fields and every
// evidence binding match the closed scope the authority itself observed.
func validateIndependent(scope independentScope, resp *AttestationResponse) (*FenceProof, error) {
	if resp == nil {
		return nil, fmt.Errorf("fencing: missing attestation")
	}
	proof := resp.Proof
	if proof.Independent != nil {
		return nil, fmt.Errorf("fencing: attestor must not attach evidence")
	}
	if err := validateFenceProof(scope.request, &proof); err != nil {
		return nil, err
	}
	if err := validateIndependentEvidence(scope, &resp.Independent); err != nil {
		return nil, err
	}
	proof.Independent = &resp.Independent
	return &proof, nil
}

func validateIndependentEvidence(scope independentScope, ev *IndependentEvidence) error {
	if ev == nil {
		return fmt.Errorf("fencing: missing independent evidence")
	}
	if ev.Version != independentEvidenceVersion {
		return fmt.Errorf("fencing: unsupported independent evidence version %d", ev.Version)
	}
	if ev.Provider == "" || ev.Mode == "" || ev.ObservedAt == "" || ev.BarrierID == "" {
		return fmt.Errorf("fencing: independent evidence is incomplete")
	}
	if ev.RequestHash != scope.requestHash || ev.LedgerHash != scope.ledgerHash {
		return fmt.Errorf("fencing: independent evidence is bound to another request or ledger")
	}
	if ev.Barrier.Kind == "" || ev.Barrier.DeniedAt == "" || ev.Barrier.ClosedOperationID != scope.request.OperationID || ev.Barrier.ClosedRequestHash != scope.requestHash {
		return fmt.Errorf("fencing: independent barrier evidence differs from the closed operation")
	}
	if !sameStrings(ev.Barrier.CurrentPodUIDs, scope.currentPodUIDs) {
		return fmt.Errorf("fencing: independent barrier names a different Pod inventory")
	}
	if err := validateEvidenceRuntimes(scope, ev.Runtimes); err != nil {
		return err
	}
	return validateEvidenceStorage(scope.storage, &ev.Storage)
}

// validateEvidenceRuntimes requires exactly one terminal runtime per fenced Pod
// incarnation, restating the container, image, run and WAL identity the operator
// captured before the fault.
func validateEvidenceRuntimes(scope independentScope, runtimes []EvidenceRuntime) error {
	wanted := map[string][]AttestationRun{}
	for _, run := range scope.runs {
		wanted[run.PodUID] = append(wanted[run.PodUID], run)
	}
	if len(runtimes) != len(wanted) {
		return fmt.Errorf("fencing: independent evidence covers %d of %d fenced incarnations", len(runtimes), len(wanted))
	}
	seen := map[string]bool{}
	for _, rt := range runtimes {
		runs, known := wanted[rt.PodUID]
		if !known || rt.PodUID == "" || seen[rt.PodUID] {
			return fmt.Errorf("fencing: independent evidence names an unknown or duplicate incarnation")
		}
		seen[rt.PodUID] = true
		var target *FenceTarget
		for i := range scope.targets {
			if scope.targets[i].PodUID == rt.PodUID {
				target = &scope.targets[i]
			}
		}
		if target == nil {
			return fmt.Errorf("fencing: independent evidence has no captured target")
		}
		if rt.PodName != target.Pod || rt.NodeName == "" || rt.NodeName != target.NodeName || rt.NodeUID == "" || rt.InstanceID == "" {
			return fmt.Errorf("fencing: independent runtime identity differs from the captured incarnation")
		}
		if target.NodeUID != "" && rt.NodeUID != target.NodeUID {
			return fmt.Errorf("fencing: independent node UID differs from the captured incarnation")
		}
		if !sameStrings(rt.ContainerIDs, []string{target.ContainerID}) || !sameStrings(rt.ImageIDs, []string{target.ImageID}) {
			return fmt.Errorf("fencing: independent container or image identity differs from the captured incarnation")
		}
		var runIDs, wals []string
		for _, run := range runs {
			runIDs, wals = append(runIDs, run.RunID), append(wals, run.WALIdentity)
		}
		if !sameStrings(rt.RunIDs, runIDs) || !sameStrings(rt.WALIdentities, wals) {
			return fmt.Errorf("fencing: independent run or WAL coverage differs from the closed ledger")
		}
		if rt.State != RuntimeStateExited && rt.State != RuntimeStateTerminated {
			return fmt.Errorf("fencing: runtime state %q is not terminal", rt.State)
		}
		if rt.RuntimeEvidence == "" {
			return fmt.Errorf("fencing: independent runtime evidence is empty")
		}
	}
	return nil
}

// validateEvidenceStorage accepts only the exact workload location and a stable
// quiet-window observation. It proves writer absence and object stability; it is
// never a clean engine drain.
func validateEvidenceStorage(want AttestationStoreScope, got *EvidenceStorage) error {
	if want.Provider == "" || want.Bucket == "" || want.Prefix == "" {
		return fmt.Errorf("fencing: workload object-store scope is unknown")
	}
	if got.scope() != want {
		return fmt.Errorf("fencing: storage evidence is bound to another location")
	}
	if got.QuietWindowSeconds < minIndependentQuietWindowSeconds {
		return fmt.Errorf("fencing: storage quiet window is too short")
	}
	for _, snapshot := range []StorageSnapshot{got.Before, got.After} {
		if snapshot.ObservedAt == "" || snapshot.HeadGeneration == "" || snapshot.HeadMetageneration == "" || snapshot.CurrentGeneration == "" || snapshot.CurrentMetageneration == "" || snapshot.InventoryDigest == "" {
			return fmt.Errorf("fencing: storage snapshot is incomplete")
		}
	}
	if !got.Stable || got.After.state() != got.Before.state() {
		return fmt.Errorf("fencing: storage state changed across the quiet window")
	}
	from, fromErr := time.Parse(time.RFC3339, got.Before.ObservedAt)
	to, toErr := time.Parse(time.RFC3339, got.After.ObservedAt)
	if fromErr != nil || toErr != nil || to.Sub(from) < time.Duration(got.QuietWindowSeconds)*time.Second {
		return fmt.Errorf("fencing: storage evidence does not span its quiet window")
	}
	if to.After(time.Now().Add(attestationClockSkew)) {
		return fmt.Errorf("fencing: storage evidence observes a future window")
	}
	return nil
}

// sameStrings compares two string multisets. Order is not significant.
func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	x, y := append([]string(nil), a...), append([]string(nil), b...)
	sort.Strings(x)
	sort.Strings(y)
	for i := range x {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}
