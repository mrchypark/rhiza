package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

const frozenSourcePreflightPath = "/v1/archive-genesis/frozen-source/preflight"

type FrozenSourcePreflightRequest struct {
	RecoveryName string `json:"recoveryName"`
	RecoveryUID  string `json:"recoveryUID"`
}

type FrozenSourcePreflightResponse struct {
	Identity             recovery.FrozenSourceIdentity `json:"identity"`
	ClosureReady         bool                          `json:"closureReady"`
	ProofDigest          string                        `json:"proofDigest,omitempty"`
	ClosedRootDigest     string                        `json:"closedRootDigest,omitempty"`
	EmptyGenesisEligible bool                          `json:"emptyGenesisEligible,omitempty"`
}

type FrozenSourcePreflighter interface {
	FrozenSourcePreflight(context.Context, FrozenSourcePreflightRequest) (FrozenSourcePreflightResponse, error)
}

func (fc *FencingClient) FrozenSourcePreflight(ctx context.Context, request FrozenSourcePreflightRequest) (FrozenSourcePreflightResponse, error) {
	if err := validateURL(fc.URL); err != nil {
		return FrozenSourcePreflightResponse{}, err
	}
	if request.RecoveryName == "" || request.RecoveryUID == "" {
		return FrozenSourcePreflightResponse{}, fmt.Errorf("fencing: frozen source recovery identity is required")
	}
	body, err := json.Marshal(request)
	if err != nil {
		return FrozenSourcePreflightResponse{}, err
	}
	endpoint, err := url.Parse(fc.URL)
	if err != nil {
		return FrozenSourcePreflightResponse{}, fmt.Errorf("fencing: invalid endpoint")
	}
	endpoint.Path = frozenSourcePreflightPath
	endpoint.RawPath = ""
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return FrozenSourcePreflightResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	token, err := readToken(fc.TokenFile)
	if err != nil {
		return FrozenSourcePreflightResponse{}, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := cloneClient(fc.Client).Do(req)
	if err != nil {
		return FrozenSourcePreflightResponse{}, fmt.Errorf("fencing: frozen source preflight request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusServiceUnavailable {
		return FrozenSourcePreflightResponse{}, ErrFencePending
	}
	if resp.StatusCode != http.StatusOK {
		return FrozenSourcePreflightResponse{}, fmt.Errorf("fencing: frozen source preflight HTTP %d", resp.StatusCode)
	}
	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil || len(responseBody) > maxResponseBytes {
		return FrozenSourcePreflightResponse{}, errors.Join(err, fmt.Errorf("fencing: invalid frozen source preflight response size"))
	}
	var result FrozenSourcePreflightResponse
	decoder := json.NewDecoder(bytes.NewReader(responseBody))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil || decoder.Decode(new(any)) != io.EOF {
		return FrozenSourcePreflightResponse{}, fmt.Errorf("fencing: invalid frozen source preflight response")
	}
	if _, err := recovery.FrozenTargetID(result.Identity); err != nil ||
		(result.ClosureReady && (!validEvidenceDigest(result.ProofDigest) || !validEvidenceDigest(result.ClosedRootDigest))) ||
		(!result.ClosureReady && (result.ProofDigest != "" || result.ClosedRootDigest != "")) {
		return FrozenSourcePreflightResponse{}, fmt.Errorf("fencing: inconsistent frozen source preflight response")
	}
	return result, nil
}

func (e *FencingExecutor) handleFrozenSourcePreflight(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != frozenSourcePreflightPath || r.URL.RawQuery != "" {
		http.NotFound(w, r)
		return
	}
	if err := e.authenticateController(r.Context(), r.Header.Get("Authorization")); err != nil {
		if transientFenceAuthority(err) {
			http.Error(w, "authority unavailable", http.StatusServiceUnavailable)
		} else {
			http.Error(w, "controller identity rejected", http.StatusForbidden)
		}
		return
	}
	var request FrozenSourcePreflightRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil || decoder.Decode(new(any)) != io.EOF || request.RecoveryName == "" || request.RecoveryUID == "" {
		http.Error(w, "invalid frozen source preflight request", http.StatusBadRequest)
		return
	}
	result, err := e.frozenSourcePreflight(r.Context(), request)
	if err != nil {
		if transientFenceAuthority(err) || errors.Is(err, fencing.ErrPending) {
			http.Error(w, "frozen source preflight pending", http.StatusServiceUnavailable)
		} else {
			http.Error(w, "frozen source preflight rejected", http.StatusConflict)
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

func (e *FencingExecutor) frozenSourcePreflight(ctx context.Context, request FrozenSourcePreflightRequest) (FrozenSourcePreflightResponse, error) {
	if e == nil || e.Kube == nil || e.AuthorityState == nil {
		return FrozenSourcePreflightResponse{}, fmt.Errorf("fencing: canonical frozen source authority is unavailable")
	}
	var persisted Resource
	recoveryPath := "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/" + url.PathEscape(e.Kube.Namespace) + "/rhizarecoveries/" + url.PathEscape(request.RecoveryName)
	if err := e.Kube.Get(ctx, recoveryPath, &persisted); err != nil {
		return FrozenSourcePreflightResponse{}, err
	}
	if str(persisted.Metadata["name"]) != request.RecoveryName || str(persisted.Metadata["uid"]) != request.RecoveryUID ||
		str(persisted.Metadata["namespace"]) != e.Kube.Namespace || persisted.Metadata["deletionTimestamp"] != nil ||
		persisted.Status.RecoveryMode != RecoveryModeFrozenSource || persisted.Status.StatefulSetUID == "" ||
		persisted.Status.StatefulSetUID != persisted.Spec.Fence.StatefulSetUID || persisted.Spec.Fence.RecoveryID != persisted.Spec.RecoveryID ||
		persisted.Spec.Fence.ClusterID != persisted.Spec.SourceClusterID || persisted.Status.Source != persisted.Spec.SourceClusterID ||
		persisted.Spec.RecoveryID == "" || persisted.Spec.SourceClusterID == "" || persisted.Status.RecoveryID != persisted.Spec.RecoveryID ||
		persisted.Status.SpecHash != immutableSpecHash(persisted.Spec) ||
		(persisted.Status.FrozenEmptyGenesisPending && (persisted.Status.TargetBaseKind != "empty" || !persisted.Spec.AllowDataLoss || persisted.Status.SourceDurability != "async")) {
		return FrozenSourcePreflightResponse{}, fmt.Errorf("fencing: persisted frozen recovery identity is incomplete or changed")
	}
	uid, sourceID, operationID := persisted.Status.StatefulSetUID, persisted.Spec.SourceClusterID, persisted.Spec.RecoveryID
	state, _, err := e.AuthorityState.ReadRoot(ctx, uid, sourceID)
	if err != nil {
		return FrozenSourcePreflightResponse{}, err
	}
	if state.StatefulSetUID != uid || state.Ledger.SourceClusterID != sourceID || !state.Ledger.RequireMutationJournal {
		return FrozenSourcePreflightResponse{}, fmt.Errorf("fencing: canonical source root identity or profile differs")
	}
	if err := e.validateMutationJournalProfile(state); err != nil {
		return FrozenSourcePreflightResponse{}, err
	}
	sts, err := e.archiveForkStatefulSet(ctx, uid)
	if err != nil {
		return FrozenSourcePreflightResponse{}, err
	}
	storeScope, err := e.storageScope(ctx, sts, sourceID)
	if err != nil {
		return FrozenSourcePreflightResponse{}, err
	}
	archiveScope := fencing.ArchiveScope{Provider: storeScope.Provider, Bucket: storeScope.Bucket, Prefix: storeScope.Prefix, ClusterID: sourceID}
	scopeHash, err := archiveScope.Hash()
	if err != nil || storeScope.Provider != "gcs" || state.Ledger.ArchiveScopeHash != scopeHash {
		return FrozenSourcePreflightResponse{}, errors.Join(err, fmt.Errorf("fencing: canonical source archive scope differs"))
	}
	result := FrozenSourcePreflightResponse{Identity: recovery.FrozenSourceIdentity{ScopeHash: scopeHash, ClusterID: sourceID, StatefulSetUID: uid}}
	if persisted.Status.TargetBaseKind == "empty" {
		closedDigest := ""
		if state.Ledger.ClosedBy != "" {
			if state.Ledger.ClosedBy != operationID {
				return FrozenSourcePreflightResponse{}, fmt.Errorf("fencing: source root is closed by another operation")
			}
			closedDigest = e.ledgerHash(state)
		}
		if err := e.validatePendingFrozenEmptyGenesis(ctx, state, archiveScope, uid, sourceID, operationID, closedDigest); err != nil {
			return FrozenSourcePreflightResponse{}, err
		}
		result.EmptyGenesisEligible = true
	}
	if state.Ledger.ClosedBy == "" {
		return result, nil
	}
	if state.Ledger.ClosedBy != operationID {
		return FrozenSourcePreflightResponse{}, fmt.Errorf("fencing: source root is closed by another operation")
	}
	_, bindingUID, err := sourceRootBinding(state)
	if err != nil {
		return FrozenSourcePreflightResponse{}, err
	}
	fenceRequest := ArchiveForkGenesisRequest{StatefulSetUID: uid, SourceFenceOperationID: operationID}
	fence, err := e.readCanonicalSourceFence(ctx, fenceRequest, sourceID, bindingUID, state)
	if err != nil {
		return FrozenSourcePreflightResponse{}, err
	}
	if fence.Spec.OperationID != operationID || fence.Spec.SourceClusterID != sourceID || fence.Spec.StatefulSetUID != uid || fence.Spec.Scope != ScopeGeneration || fence.Status.Proof == nil || fence.Status.Proof.IndependentV2 == nil {
		return FrozenSourcePreflightResponse{}, fencing.ErrPending
	}
	requestDigest, err := requestHash(&fence.Spec)
	if err != nil || state.Ledger.ClosedHash != requestDigest {
		return FrozenSourcePreflightResponse{}, fmt.Errorf("fencing: source fence does not match the closed root")
	}
	if err := validateFenceProof(fence.Spec, fence.Status.Proof); err != nil {
		return FrozenSourcePreflightResponse{}, err
	}
	proof := fence.Status.Proof.IndependentV2
	if err := validateIndependentV2(fence.Spec, proof); err != nil || proof.LedgerHash != e.ledgerHash(state) || proof.StorageScope != storeScope {
		return FrozenSourcePreflightResponse{}, errors.Join(err, fmt.Errorf("fencing: independent proof is not bound to the source root and scope"))
	}
	if err := verifyIndependentProofCoverage(state, fence.Spec, proof); err != nil {
		return FrozenSourcePreflightResponse{}, err
	}
	currentUIDs, err := e.currentInventory(ctx, uid, nil)
	if err != nil {
		return FrozenSourcePreflightResponse{}, err
	}
	sort.Strings(currentUIDs)
	proofUIDs := append([]string(nil), proof.CurrentPodUIDs...)
	sort.Strings(proofUIDs)
	if !sameStrings(currentUIDs, proofUIDs) {
		return FrozenSourcePreflightResponse{}, fmt.Errorf("fencing: source Pod inventory changed since independent proof")
	}
	result.ClosureReady = true
	result.ProofDigest = hashJSON(fence.Status.Proof)
	result.ClosedRootDigest = e.ledgerHash(state)
	if persisted.Status.TargetBaseKind == "empty" {
		if err := e.validatePendingFrozenEmptyGenesis(ctx, state, archiveScope, uid, sourceID, operationID, result.ClosedRootDigest); err != nil {
			return FrozenSourcePreflightResponse{}, err
		}
	}
	return result, nil
}

func (e *FencingExecutor) validatePendingFrozenEmptyGenesis(ctx context.Context, state fenceLedger, scope fencing.ArchiveScope, uid, sourceID, recoveryID, closedRootDigest string) error {
	if state.StatefulSetUID != uid || (state.Ledger.ClosedBy != "" && state.Ledger.ClosedBy != recoveryID) || !state.Ledger.RequireMutationJournal ||
		state.Ledger.ArchiveGenesisState != fencing.ArchiveGenesisCommitted || state.Ledger.ArchiveGenesisRef == nil || state.Ledger.ArchiveGenesisRef.Mutation == nil {
		return fmt.Errorf("canonical source root does not bind an admitted genesis reference")
	}
	if closedRootDigest != "" && (state.Ledger.ClosedBy != recoveryID || e.ledgerHash(state) != closedRootDigest) {
		return fmt.Errorf("canonical closed root changed after its independent proof")
	}
	if err := e.validateMutationJournalProfile(state); err != nil {
		return err
	}
	ref := state.Ledger.ArchiveGenesisRef
	if err := ref.ValidateForScope(scope); err != nil {
		return err
	}
	claim, _, err := e.AuthorityState.ReadArchiveScopeClaim(ctx, scope)
	if err != nil || claim.OwnerStatefulSetUID != uid || claim.OwnerSourceClusterID != sourceID || claim.GenesisRef == nil || !sameArchiveGenesisRef(claim.GenesisRef, ref) {
		return errors.Join(err, fmt.Errorf("source archive claim does not bind the root genesis reference"))
	}
	mutation := ref.Mutation
	runtime, run, err := e.archiveGenesisProducerRun(ctx, state, mutation.ProducerUID)
	if err != nil || run.Runtime == nil || *run.Runtime != runtime || run.MutationProducerUID != mutation.ProducerUID || run.MutationProfile != fencing.MutationHeldExecutionProfile ||
		run.Identity.WALIdentity == "" || run.MutationScopeHash != mutation.Intent.StoreScopeHash || runtime.StatefulSetUID != uid || runtime.SourceClusterID != sourceID {
		return errors.Join(err, fmt.Errorf("genesis candidate is not bound to the held source runtime"))
	}
	producer, producerUID, _, err := e.AuthorityState.ReadProducer(ctx, runtime)
	phaseMismatch := (state.Ledger.ClosedBy == "" && (producer.Phase != fencing.MutationOpen || producer.CloseOperationID != "")) ||
		(state.Ledger.ClosedBy != "" && (producer.Phase != fencing.MutationClosed || producer.CloseOperationID != recoveryID || producer.ClosedThrough != producer.LastAdmitted))
	if err != nil || producerUID != mutation.ProducerUID || phaseMismatch || producer.CompactedThrough != 0 || producer.LastAdmitted != uint64(len(producer.Entries)) {
		return errors.Join(err, fmt.Errorf("genesis producer has incomplete or compacted source history"))
	}
	var winnerSeen bool
	for runID, sourceRun := range state.Ledger.Runs {
		if sourceRun.MutationProducerUID == "" {
			continue
		}
		if sourceRun.Runtime == nil || sourceRun.Runtime.StatefulSetUID != uid || sourceRun.Runtime.SourceClusterID != sourceID || sourceRun.MutationProfile != fencing.MutationHeldExecutionProfile ||
			sourceRun.MutationScopeHash != mutation.Intent.StoreScopeHash || sourceRun.Identity.WALIdentity == "" {
			return fmt.Errorf("source mutation producer has no exact source runtime binding")
		}
		other, otherUID, _, err := e.AuthorityState.ReadProducer(ctx, *sourceRun.Runtime)
		otherPhaseMismatch := (state.Ledger.ClosedBy == "" && (other.Phase != fencing.MutationOpen || other.CloseOperationID != "")) ||
			(state.Ledger.ClosedBy != "" && (other.Phase != fencing.MutationClosed || other.CloseOperationID != recoveryID || other.ClosedThrough != other.LastAdmitted))
		if err != nil || otherUID != sourceRun.MutationProducerUID || other.Runtime != *sourceRun.Runtime || other.WALIdentity != sourceRun.Identity.WALIdentity || otherPhaseMismatch ||
			other.CompactedThrough != 0 || other.LastAdmitted != uint64(len(other.Entries)) {
			return errors.Join(err, fmt.Errorf("source contains incomplete or compacted mutation history"))
		}
		for sequence, entry := range other.Entries {
			isWinner := runID == runtime.RunID && otherUID == mutation.ProducerUID && sequence == mutation.Sequence && entry.Intent == mutation.Intent
			if isWinner {
				if winnerSeen {
					return fmt.Errorf("source contains duplicate archive genesis winner entries")
				}
				winnerSeen = true
				if err := validateFrozenEmptyGenesisWinner(other, otherUID, sequence, entry, state.Ledger.ClosedBy == recoveryID, recoveryID); err != nil {
					return err
				}
				continue
			}
			if err := e.validateFrozenEmptyGenesisLoser(ctx, *sourceRun.Runtime, sourceRun, otherUID, sequence, entry, *ref); err != nil {
				return fmt.Errorf("source contains non-winner mutation history: %w", err)
			}
		}
	}
	if !winnerSeen {
		return fmt.Errorf("source root winner is absent from its mutation producer")
	}
	emptyHead, err := recovery.EmptyArchiveHeadBytes()
	if err != nil || mutation.Intent.PayloadSize != int64(len(emptyHead)) || mutation.Intent.PayloadDigest != authorityStateDigest(emptyHead) {
		return errors.Join(err, fmt.Errorf("genesis intent does not bind canonical empty HEAD bytes"))
	}
	return nil
}

func validateFrozenEmptyGenesisWinner(producer fencing.MutationProducer, producerUID string, sequence uint64, entry fencing.MutationEntry, sourceClosed bool, recoveryID string) error {
	if !isArchiveGenesisMutation(entry.Intent) {
		return fmt.Errorf("source root winner is not an archive HEAD create")
	}
	if !sourceClosed || entry.GuardedLogicalResolution == nil {
		if entry.Resolution != nil || entry.CopyCompletion != nil || entry.GuardedLogicalResolution != nil || len(entry.Dispatches) != 1 {
			return fmt.Errorf("pending genesis has a terminal or ambiguous provider result")
		}
		for _, dispatch := range entry.Dispatches {
			if !entry.DispatchClosed || entry.Preparation == nil || !fencing.IsHeldCreateEligible(entry.Intent, *entry.Preparation) || dispatch.ReceiptDigest != "" {
				return fmt.Errorf("pending genesis dispatch is not the exact unresolved held create")
			}
		}
		return nil
	}
	guarded := entry.GuardedLogicalResolution
	if producer.CloseOperationID != recoveryID || producer.Phase != fencing.MutationClosed || entry.Preparation == nil || !fencing.IsHeldCreateEligible(entry.Intent, *entry.Preparation) || !entry.DispatchClosed || len(entry.Dispatches) != 1 ||
		entry.Resolution == nil || entry.Resolution.Outcome != fencing.MutationCommitted || entry.Resolution.AllCopiesTerminal || entry.Resolution.PayloadDigest != entry.Intent.PayloadDigest || entry.CopyCompletion != nil || guarded == nil {
		return fmt.Errorf("closed genesis lacks its nonterminal guarded logical resolution")
	}
	if err := producer.ValidateGuardedLogicalResolution(producerUID, sequence, entry); err != nil {
		return fmt.Errorf("closed genesis guarded resolution is invalid: %w", err)
	}
	if entry.Resolution.ReceiptDigest != guarded.GuardDigest || entry.Resolution.PreparationHash != guarded.PreparationHash {
		return fmt.Errorf("closed genesis resolution differs from its exact guard evidence")
	}
	return nil
}

func (e *FencingExecutor) validateFrozenEmptyGenesisLoser(ctx context.Context, runtime fencing.RuntimeBinding, run fencing.Run, producerUID string, sequence uint64, entry fencing.MutationEntry, winner fencing.ArchiveGenesisRef) error {
	if winner.Mutation == nil || entry.Intent != winner.Mutation.Intent || !isArchiveGenesisMutation(entry.Intent) || entry.Preparation != nil || len(entry.Dispatches) != 0 || !entry.DispatchClosed || entry.CopyCompletion != nil || entry.GuardedLogicalResolution != nil ||
		entry.Resolution == nil || entry.Resolution.Outcome != fencing.MutationRejected || !entry.Resolution.AllCopiesTerminal || !mutationDigestValidOperator(entry.Resolution.ReceiptDigest) {
		return fmt.Errorf("entry is not an exact unprepared terminal genesis loser")
	}
	evidence, err := e.authorityEvidenceFor(runtime)
	if err != nil {
		return err
	}
	reader, _, err := evidence.Read(ctx, "archive-genesis-cancellations/"+entry.Resolution.ReceiptDigest)
	if err != nil {
		return err
	}
	body, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if err := errors.Join(readErr, closeErr); err != nil || authorityStateDigest(body) != entry.Resolution.ReceiptDigest {
		return errors.Join(err, fmt.Errorf("genesis loser cancellation evidence digest differs"))
	}
	expected := archiveGenesisCancellationProof{Version: 1, Runtime: runtime, WALIdentity: run.Identity.WALIdentity, ProducerUID: producerUID, Sequence: sequence, Intent: entry.Intent, WinningGenesis: winner}
	var proof archiveGenesisCancellationProof
	if err := json.Unmarshal(body, &proof); err != nil {
		return err
	}
	canonical, err := json.Marshal(expected)
	if err != nil || !bytes.Equal(body, canonical) || proof.Version != expected.Version || proof.Runtime != expected.Runtime || proof.WALIdentity != expected.WALIdentity ||
		proof.ProducerUID != expected.ProducerUID || proof.Sequence != expected.Sequence || proof.Intent != expected.Intent || !sameArchiveGenesisRef(&proof.WinningGenesis, &expected.WinningGenesis) {
		return errors.Join(err, fmt.Errorf("genesis loser cancellation does not bind the exact source winner"))
	}
	return nil
}
