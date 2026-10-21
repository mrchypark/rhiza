package operator

import (
	"context"
	"fmt"
	"net/url"
	"slices"
	"sort"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

func validateIndependentV2(request FenceRequest, evidence *IndependentEvidenceV2) error {
	hash, err := requestHash(&request)
	if err != nil {
		return err
	}
	if evidence.Version != 2 || evidence.RequestHash != hash || !validEvidenceDigest(evidence.LedgerHash) ||
		evidence.StorageScope.Provider == "" || evidence.StorageScope.Provider == "filesystem" || evidence.StorageScope.Bucket == "" || evidence.StorageScope.Prefix == "" ||
		len(evidence.Terminals) == 0 || len(evidence.Terminals) > fenceTargetCapacity || len(evidence.CurrentPodUIDs) > fenceTargetCapacity {
		return fmt.Errorf("fencing: independent v2 coverage is incomplete")
	}
	seen := map[string]bool{}
	pods := map[string]bool{}
	for _, pod := range evidence.CurrentPodUIDs {
		if pod == "" || len(pod) > 256 || pods[pod] {
			return fmt.Errorf("fencing: independent v2 Pod inventory differs")
		}
		pods[pod] = true
	}
	for _, ref := range evidence.Terminals {
		id := ref.Request.Identity
		if ref.EvidenceVersion != 2 || id.SourceClusterID != request.SourceClusterID || id.NodeID == "" || id.PodUID == "" || id.RunID == "" ||
			seen[id.RunID] || ref.Request.OperationID == "" || ref.ProducerUID == "" || ref.ProofName == "" || ref.ProofUID == "" ||
			!validEvidenceDigest(ref.Request.RequestHash) || !validEvidenceDigest(ref.RuntimeHash) || !validEvidenceDigest(ref.ProofDigest) ||
			!validEvidenceDigest(ref.RuntimeReceiptDigest) || !validEvidenceDigest(ref.MutationReceiptDigest) || ref.StorageScopeDigest != hashJSON(evidence.StorageScope) {
			return fmt.Errorf("fencing: independent v2 terminal reference differs")
		}
		for _, value := range []string{id.SourceClusterID, id.NodeID, id.PodUID, ref.Request.OperationID} {
			if len(value) > 256 {
				return fmt.Errorf("fencing: independent v2 identity exceeds capacity")
			}
		}
		if len(id.RunID) > 128 || len(id.WALIdentity) > 128 || len(ref.ProducerUID) > 128 || len(ref.ProofName) > 63 || len(ref.ProofUID) > 128 {
			return fmt.Errorf("fencing: independent v2 reference exceeds capacity")
		}
		seen[id.RunID] = true
		if request.Scope == ScopeVoter {
			matched := false
			for _, target := range request.Targets {
				matched = matched || target.NodeID == id.NodeID
			}
			if !matched {
				return fmt.Errorf("fencing: independent v2 covers another voter")
			}
		}
	}
	for _, target := range request.Targets {
		covered := false
		for _, ref := range evidence.Terminals {
			id := ref.Request.Identity
			covered = covered || id.NodeID == target.NodeID && (target.PodUID == "" || id.PodUID == target.PodUID) && (target.WALIdentity == "" || id.WALIdentity == target.WALIdentity)
		}
		if !covered {
			return fmt.Errorf("fencing: independent v2 omits a captured target")
		}
	}
	return nil
}

func validEvidenceDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, c := range value {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// Derive the entire frozen cohort, not just currently captured Pod targets.
// Historical incarnations may have disappeared from Kubernetes long ago.
func independentClosedRuns(state fenceLedger, request FenceRequest, nodes []string) ([]fencing.Run, error) {
	l := state.Ledger
	hash, err := requestHash(&request)
	if err != nil {
		return nil, err
	}
	if !l.RequireRuntimeBinding || !l.RequireMutationJournal || state.StatefulSetUID != request.StatefulSetUID || l.SourceClusterID != request.SourceClusterID {
		return nil, fmt.Errorf("fencing: independent v2 enrollment profile is unavailable")
	}
	if len(nodes) == 0 {
		if l.ClosedBy != request.OperationID || l.ClosedHash != hash {
			return nil, fmt.Errorf("fencing: generation cohort is not frozen by this operation")
		}
	} else {
		for _, node := range nodes {
			closed, ok := l.ClosedNodes[node]
			if !ok || closed.OperationID != request.OperationID || closed.RequestHash != hash {
				return nil, fmt.Errorf("fencing: voter cohort is not frozen by this operation")
			}
		}
	}
	var runs []fencing.Run
	for runID, run := range l.Runs {
		if len(nodes) != 0 && !slices.Contains(nodes, run.Identity.NodeID) {
			continue
		}
		if runID != run.Identity.RunID || run.Identity.SourceClusterID != l.SourceClusterID || run.Runtime == nil || run.Runtime.Validate(run.Identity) != nil ||
			run.Runtime.StatefulSetUID != state.StatefulSetUID || run.Runtime.BindingUID != request.BindingUID || run.Stop == nil || run.Stop.Identity != run.Identity ||
			run.Stop.OperationID == "" || !validEvidenceDigest(run.Stop.RequestHash) {
			return nil, fmt.Errorf("fencing: closed historical run lacks its own enrollment or stop binding")
		}
		runs = append(runs, run)
	}
	if len(runs) == 0 || len(runs) > fenceTargetCapacity {
		return nil, fmt.Errorf("fencing: independent v2 cohort is empty or exceeds capacity")
	}
	sort.Slice(runs, func(i, j int) bool { return runs[i].Identity.RunID < runs[j].Identity.RunID })
	return runs, nil
}

// Independent completion requires retained, verified evidence for the complete
// frozen cohort. API absence, a DELETE response and quiet reads cannot replace it.
func (e *FencingExecutor) independentFence(ctx context.Context, endpoint string, record *fenceResource, request FenceRequest, sts object, state fenceLedger, hash string, nodes, current []string) error {
	runs, err := independentClosedRuns(state, request, nodes)
	if err != nil || e.VerifyRuntimeEvidence == nil {
		return ErrFencePending
	}
	storage, err := e.storageScope(ctx, sts, request.SourceClusterID)
	if err != nil {
		return err
	}
	// Retain each original termination first. This can span reconciles or
	// executor restarts; a never-bound producer is included, never skipped.
	for _, run := range runs {
		if run.IndependentTerminal != nil {
			continue
		}
		if e.CollectRuntimeEvidence == nil {
			return ErrFencePending
		}
		// Producer admission must already be closed before the runtime provider
		// acts. This also pins a missing never-bound producer's actual UID.
		if _, err := e.reserveMutationProducer(ctx, *run.Runtime); err != nil {
			return err
		}
		receipt, err := e.CollectRuntimeEvidence(ctx, *run.Runtime, *run.Stop)
		if err != nil {
			return err
		}
		if _, err := e.retainIndependentTerminal(ctx, *run.Runtime, storage, receipt); err != nil {
			return err
		}
	}
	state, err = e.read(ctx, state.StatefulSetUID, state.Ledger.SourceClusterID)
	if err != nil {
		return err
	}
	runs, err = independentClosedRuns(state, request, nodes)
	if err != nil {
		return ErrFencePending
	}
	evidence := &IndependentEvidenceV2{Version: 2, RequestHash: hash, LedgerHash: e.ledgerHash(state), StorageScope: storage,
		CurrentPodUIDs: append([]string{}, current...)}
	sort.Strings(evidence.CurrentPodUIDs)
	for _, run := range runs {
		verificationID := hashJSON([]any{"compose-independent-terminal", hash, storage, run.Identity.RunID})
		_, ref, err := e.readIndependentTerminal(ctx, *run.Runtime, storage, verificationID)
		if err != nil {
			return err
		}
		evidence.Terminals = append(evidence.Terminals, ref)
	}
	// Root references may have changed while evidence was being loaded. Neither
	// inventory changes nor a different root snapshot may reuse this proof.
	rechecked, err := e.read(ctx, state.StatefulSetUID, state.Ledger.SourceClusterID)
	if err != nil {
		return err
	}
	again, err := e.currentInventory(ctx, request.StatefulSetUID, nodes)
	if err != nil {
		return err
	}
	if e.ledgerHash(rechecked) != evidence.LedgerHash || !sameStrings(again, current) {
		return ErrFencePending
	}
	proof := &FenceProof{OperationID: request.OperationID, SourceClusterID: request.SourceClusterID, BindingUID: request.BindingUID,
		RequestHash: hash, ProofID: "independent-v2-" + hashJSON(evidence), ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true,
		IndependentV2: evidence}
	if err := validateFenceProof(request, proof); err != nil {
		return err
	}
	record.Status.Proof = proof
	return e.Kube.Put(ctx, endpoint+"/"+url.PathEscape(str(record.Metadata["name"]))+"/status", record, nil)
}
