package operator

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

// promoteSupervisorLearner is the sole pending-to-voter transition. The
// current core may have applied the ADD, but only this authenticated route can
// turn that local fact into a producer: canonical child completion rechecks
// both exact membership requests, immutable credential UID/resourceVersion,
// committed configuration, and the executor's durability/read-barrier ACK.
func (e *FencingExecutor) promoteSupervisorLearner(ctx context.Context, uid string, id fencing.Identity) (fencing.BindReceipt, error) {
	state, err := e.read(ctx, uid, id.SourceClusterID)
	if err != nil {
		return fencing.BindReceipt{}, err
	}
	run, ok := state.Ledger.Runs[id.RunID]
	if !ok || run.Identity != id || run.Identity.WALIdentity == "" || run.Stop != nil || run.Runtime == nil || !state.Ledger.RequireMutationJournal {
		return fencing.BindReceipt{}, fencing.ErrClosed
	}
	roleBeforeRetry := fencing.RunRole(run)
	resumingPromotion := roleBeforeRetry == fencing.StartupRoleVoterActive && run.Promotion != nil
	if roleBeforeRetry != fencing.StartupRoleLearnerPendingRemove && !resumingPromotion {
		return fencing.BindReceipt{}, fencing.ErrClosed
	}
	repair, repairRef, repairKey, engineOwned, err := e.currentEngineRepairCandidate(ctx, uid, id)
	if err != nil {
		return fencing.BindReceipt{}, err
	}
	if run.EngineRepairProvenance != nil && !engineOwned {
		// Engine-repair enrollment provenance is an authority-owned classifier.
		// A missing root reference cannot turn that Run into a generic learner.
		return fencing.BindReceipt{}, fencing.ErrClosed
	}
	if engineOwned {
		if repair.Phase != fencing.RepairAdding && repair.Phase != fencing.RepairActive {
			return fencing.BindReceipt{}, fencing.ErrClosed
		}
		if repair.Phase == fencing.RepairActive && !resumingPromotion {
			return fencing.BindReceipt{}, fencing.ErrClosed
		}
	} else {
		role, roleErr := e.resolveStartupRole(ctx, uid, id)
		if roleErr != nil || role != fencing.StartupRoleVoterActive {
			return fencing.BindReceipt{}, errors.Join(fencing.ErrPending, roleErr)
		}
	}
	proofHash, err := e.verifiedLearnerPromotionHash(ctx, uid, id)
	if err != nil {
		return fencing.BindReceipt{}, errors.Join(fencing.ErrPending, err)
	}
	if resumingPromotion && (run.Promotion.OperationID != proofHash.operationID || run.Promotion.EvidenceHash != proofHash.digest) {
		return fencing.BindReceipt{}, fencing.ErrClosed
	}
	if _, err := e.change(ctx, uid, id.SourceClusterID, func(l *fencing.Ledger) error {
		if engineOwned {
			if err := validateEngineRepairCandidateCAS(l, repair, repairRef, repairKey, id, *run.Runtime, engineRepairPromotionCAS, proofHash); err != nil {
				return err
			}
		}
		return l.PromoteLearner(id, proofHash.operationID, proofHash.digest)
	}); err != nil {
		return fencing.BindReceipt{}, err
	}
	return e.finishPromotedSupervisorLearner(ctx, uid, id, proofHash)
}

// finishPromotedSupervisorLearner resumes only the exact durable promotion
// after canonical role/proof validation by promoteSupervisorLearner. It is
// deliberately safe to retry across producer reservation, archive-scope, or
// WAL-binding failures; none of those failures rolls back the canonical role.
func (e *FencingExecutor) finishPromotedSupervisorLearner(ctx context.Context, uid string, id fencing.Identity, proofHash learnerPromotionHash) (fencing.BindReceipt, error) {
	current, err := e.read(ctx, uid, id.SourceClusterID)
	if err != nil {
		return fencing.BindReceipt{}, err
	}
	currentRun, ok := current.Ledger.Runs[id.RunID]
	_, voterClosed := current.Ledger.ClosedNodes[id.NodeID]
	if !ok || currentRun.Identity != id || id.WALIdentity == "" || currentRun.Stop != nil || current.Ledger.ClosedBy != "" || voterClosed ||
		!current.Ledger.RequireMutationJournal || fencing.RunRole(currentRun) != fencing.StartupRoleVoterActive || currentRun.Runtime == nil || currentRun.Promotion == nil ||
		currentRun.Promotion.OperationID != proofHash.operationID || currentRun.Promotion.EvidenceHash != proofHash.digest {
		return fencing.BindReceipt{}, fencing.ErrClosed
	}
	if _, err := e.reserveMutationProducer(ctx, *currentRun.Runtime); err != nil {
		return fencing.BindReceipt{}, err
	}
	if err := e.claimArchiveScopeForRegistration(ctx, uid, id, *currentRun.Runtime); err != nil {
		return fencing.BindReceipt{}, err
	}
	return e.bindSupervisorWAL(ctx, uid, id)
}

type learnerPromotionHash struct{ operationID, digest string }

func (e *FencingExecutor) verifiedLearnerPromotionHash(ctx context.Context, uid string, id fencing.Identity) (learnerPromotionHash, error) {
	if repair, _, _, found, err := e.currentEngineRepairCandidate(ctx, uid, id); err != nil {
		return learnerPromotionHash{}, err
	} else if found {
		if repair.Phase == fencing.RepairActive && repair.CurrentCandidateIdentity() != id && sameEngineRepairCandidateRun(repair.CurrentCandidateIdentity(), id) {
			state, readErr := e.read(ctx, uid, id.SourceClusterID)
			if readErr != nil {
				return learnerPromotionHash{}, readErr
			}
			run, ok := state.Ledger.Runs[id.RunID]
			if !ok || run.Identity != id || run.Runtime == nil || run.Promotion == nil ||
				run.Promotion.OperationID != engineRepairAddOperationID(repair.Binding.OperationID) || !validEvidenceDigest(run.Promotion.EvidenceHash) {
				return learnerPromotionHash{}, fencing.ErrClosed
			}
			if err := e.verifyEngineRepairActiveRestartCurrent(ctx, uid, repair, id, *run.Runtime); err != nil {
				return learnerPromotionHash{}, err
			}
			return learnerPromotionHash{operationID: run.Promotion.OperationID, digest: run.Promotion.EvidenceHash}, nil
		}
		candidateRuntime := repair.CurrentCandidateRuntime()
		if (repair.Phase != fencing.RepairAdding && repair.Phase != fencing.RepairActive) || candidateRuntime == nil || candidateRuntime.Validate(id) != nil || !engineRepairRuntimeMatches(repair, *candidateRuntime) {
			return learnerPromotionHash{}, fmt.Errorf("fencing: engine repair candidate runtime differs from its canonical enrollment")
		}
		return e.engineRepairCandidateActivationEvidence(ctx, *candidateRuntime, repair)
	}
	state, err := e.read(ctx, uid, id.SourceClusterID)
	if err != nil {
		return learnerPromotionHash{}, err
	}
	if run, ok := state.Ledger.Runs[id.RunID]; ok {
		if run.EngineRepairProvenance != nil || run.Promotion != nil && strings.HasPrefix(run.Promotion.OperationID, "engine-repair-add-") {
			return learnerPromotionHash{}, fencing.ErrClosed
		}
	}
	role, err := e.resolveStartupRole(ctx, uid, id)
	if err != nil || role != fencing.StartupRoleVoterActive {
		return learnerPromotionHash{}, errors.Join(fencing.ErrPending, err)
	}
	var pod object
	if err := e.Kube.Get(ctx, e.controller().corePath("pods", id.PodName), &pod); err != nil {
		return learnerPromotionHash{}, err
	}
	var roots struct {
		Items []ClusterResource `json:"items"`
	}
	if err := e.Kube.Get(ctx, "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/"+url.PathEscape(e.Kube.Namespace)+"/rhizaclusters", &roots); err != nil {
		return learnerPromotionHash{}, err
	}
	statefulSetName := ""
	for _, ref := range list(nested(pod, "metadata", "ownerReferences")) {
		owner := asObject(ref)
		if str(owner["kind"]) == "StatefulSet" && str(owner["uid"]) == uid {
			statefulSetName = str(owner["name"])
		}
	}
	if statefulSetName == "" {
		return learnerPromotionHash{}, fmt.Errorf("fencing: learner StatefulSet owner is unavailable")
	}
	var root *ClusterResource
	for i := range roots.Items {
		candidate := &roots.Items[i]
		if str(candidate.Metadata["uid"]) == str(nested(pod, "metadata", "annotations", fencingRootAnnotation)) && candidate.Spec.Automatic && candidate.Spec.StatefulSet == statefulSetName && candidate.Spec.AdoptClusterID == id.SourceClusterID {
			if root != nil {
				return learnerPromotionHash{}, fmt.Errorf("fencing: automatic learner root is ambiguous")
			}
			root = candidate
		}
	}
	if root == nil {
		return learnerPromotionHash{}, fmt.Errorf("fencing: automatic learner root is unavailable")
	}
	c := &Controller{Kube: e.Kube, Bucket: e.ArchiveBucket, Prefix: e.AutomaticControlPrefix}
	autoState, _, err := c.loadAuto(ctx, c.autoKey(root))
	if err != nil || autoState.Intent == nil || autoState.Intent.RecoveryName == "" {
		return learnerPromotionHash{}, fmt.Errorf("fencing: automatic learner control is unavailable")
	}
	var child Resource
	if err := e.Kube.Get(ctx, c.resourcePath(autoState.Intent.RecoveryName), &child); err != nil {
		return learnerPromotionHash{}, err
	}
	if child.Status.Membership == nil || child.Status.Membership.Add == nil || child.Status.Membership.RemoveRequest == nil {
		return learnerPromotionHash{}, fencing.ErrPending
	}
	return learnerPromotionHash{operationID: autoState.Intent.Request.OperationID, digest: hashJSON([]any{
		autoState.BindingUID, autoState.BindingHash, autoState.StatefulSetUID, autoState.ActiveClusterID,
		autoState.Intent.Request, autoState.Intent.Proof, autoState.Intent.RecoveryUID, autoState.Intent.SpecHash,
		str(child.Metadata["uid"]), hashJSON(child.Spec), child.Status.Membership,
		child.Status.TargetIdentities,
	})}, nil
}

func (e *FencingExecutor) bindSupervisorWAL(ctx context.Context, uid string, id fencing.Identity) (fencing.BindReceipt, error) {
	state, err := e.read(ctx, uid, id.SourceClusterID)
	if err != nil {
		return fencing.BindReceipt{}, err
	}
	if err := e.validateMutationJournalProfile(state); err != nil {
		return fencing.BindReceipt{}, err
	}
	if run, ok := state.Ledger.Runs[id.RunID]; ok && fencing.RunRole(run) == fencing.StartupRoleLearnerPendingRemove {
		role, roleErr := e.resolveStartupRole(ctx, uid, id)
		if roleErr != nil || role != fencing.StartupRoleLearnerPendingRemove {
			return fencing.BindReceipt{}, errors.Join(fencing.ErrPending, roleErr)
		}
		changed, bindErr := e.change(ctx, uid, id.SourceClusterID, func(l *fencing.Ledger) error { return l.Bind(id) })
		if bindErr != nil {
			return fencing.BindReceipt{}, bindErr
		}
		current, ok := changed.Ledger.Runs[id.RunID]
		if !ok || current.Identity != id || current.Stop != nil || fencing.RunRole(current) != fencing.StartupRoleLearnerPendingRemove || current.MutationProducerUID != "" {
			return fencing.BindReceipt{}, fencing.ErrClosed
		}
		return fencing.BindReceipt{Identity: id, Role: fencing.StartupRoleLearnerPendingRemove}, nil
	}
	scopeHash := ""
	stageIncarnation := ""
	if state.Ledger.RequireMutationJournal {
		var err error
		run := state.Ledger.Runs[id.RunID]
		if run.Runtime == nil {
			return fencing.BindReceipt{}, fmt.Errorf("fencing: enrolled runtime is missing")
		}
		scopeHash, stageIncarnation, err = e.mutationJournalBindingInputs(ctx, uid, id, *run.Runtime)
		if err != nil {
			return fencing.BindReceipt{}, err
		}
	}
	return e.bindWALAtScopeWithStage(ctx, uid, id, scopeHash, stageIncarnation)
}

// mutationJournalBindingInputs derives the immutable storage and stage
// assignments from the authenticated Pod/StatefulSet and enrolled runtime.
// The caller persists these values before producer activation.
func (e *FencingExecutor) mutationJournalBindingInputs(ctx context.Context, uid string, id fencing.Identity, runtime fencing.RuntimeBinding) (string, string, error) {
	if ctx == nil || e == nil || e.Kube == nil || uid == "" || id.SourceClusterID == "" || id.PodUID == "" || id.WALIdentity == "" {
		return "", "", fmt.Errorf("fencing: mutation binding identity is incomplete")
	}
	if runtime.StatefulSetUID != uid || runtime.Validate(id) != nil {
		return "", "", fmt.Errorf("fencing: mutation binding runtime differs from the exact active identity")
	}
	podName := id.PodName
	if podName == "" {
		podName = id.NodeID
	}
	var pod object
	if err := e.Kube.Get(ctx, e.controller().corePath("pods", podName), &pod); err != nil {
		return "", "", err
	}
	if str(nested(pod, "metadata", "uid")) != id.PodUID || nested(pod, "metadata", "deletionTimestamp") != nil {
		return "", "", fmt.Errorf("fencing: bind Pod incarnation differs")
	}
	ownerName := ""
	for _, ref := range list(nested(pod, "metadata", "ownerReferences")) {
		owner := asObject(ref)
		if str(owner["kind"]) == "StatefulSet" && str(owner["uid"]) == uid {
			if ownerName != "" {
				return "", "", fmt.Errorf("fencing: bind workload owner is ambiguous")
			}
			ownerName = str(owner["name"])
		}
	}
	if ownerName == "" {
		return "", "", fmt.Errorf("fencing: bind workload owner differs")
	}
	var sts object
	if err := e.Kube.Get(ctx, e.controller().stsPath(ownerName), &sts); err != nil {
		return "", "", err
	}
	if str(nested(sts, "metadata", "uid")) != uid || nested(sts, "metadata", "deletionTimestamp") != nil {
		return "", "", fmt.Errorf("fencing: bind workload incarnation differs")
	}
	scope, err := e.storageScope(ctx, sts, id.SourceClusterID)
	if err != nil {
		return "", "", err
	}
	// Compare actual Pod configuration too; a changed template cannot silently
	// redirect a still-running source incarnation to a different store.
	podView := object{"spec": object{"template": object{"spec": pod["spec"]}}}
	podScope, err := e.storageScope(ctx, podView, id.SourceClusterID)
	if err != nil || podScope != scope || scope.Provider != "gcs" {
		return "", "", fmt.Errorf("fencing: source/profile storage scope differs")
	}
	stages, err := e.sourceStagesFor(ctx, runtime)
	if err != nil {
		return "", "", err
	}
	return hashJSON(scope), stages.incarnation, nil
}

// pinMutationJournalBeforeGrant reserves the exact producer and binds the
// root's storage, profile, stage, and known WAL before an active restart grant
// is returned. The producer remains Reserved; only the later child WAL bind
// activates admission.
func (e *FencingExecutor) pinMutationJournalBeforeGrant(ctx context.Context, uid string, id fencing.Identity, runtime fencing.RuntimeBinding) error {
	current, err := e.read(ctx, uid, id.SourceClusterID)
	if err != nil {
		return err
	}
	if err := e.validateMutationJournalProfile(current); err != nil {
		return err
	}
	if !current.Ledger.RequireMutationJournal {
		return nil
	}
	run, ok := current.Ledger.Runs[id.RunID]
	if !ok || run.Identity != id || run.Runtime == nil || *run.Runtime != runtime ||
		run.Promotion == nil || run.EngineRepairProvenance == nil || run.Stop != nil || current.Ledger.ClosedBy != "" {
		return fencing.ErrClosed
	}
	if _, closed := current.Ledger.ClosedNodes[id.NodeID]; closed {
		return fencing.ErrClosed
	}
	scopeHash, stageIncarnation, err := e.mutationJournalBindingInputs(ctx, uid, id, runtime)
	if err != nil {
		return err
	}
	if _, err := e.reserveMutationProducer(ctx, runtime); err != nil {
		return err
	}
	if _, err := e.changeMutationProducer(ctx, runtime, func(producer *fencing.MutationProducer) error {
		return producer.BindWAL(id)
	}); err != nil {
		return err
	}
	profile := e.sourceMutationProfile()
	var repair fencing.EngineRepairRecord
	var repairReference fencing.EngineRepairReference
	repairKey := ""
	var repairProof learnerPromotionHash
	engineOwned := false
	if run.Promotion != nil {
		repair, repairReference, repairKey, repairProof, engineOwned, err = e.verifyEngineRepairPromotion(ctx, uid, id, run)
		if err != nil {
			return err
		}
	}
	bound, err := e.change(ctx, uid, id.SourceClusterID, func(ledger *fencing.Ledger) error {
		if engineOwned {
			if err := validateEngineRepairCandidateCAS(ledger, repair, repairReference, repairKey, id, runtime, engineRepairBindCAS, repairProof); err != nil {
				return err
			}
		}
		if !ledger.RequireMutationJournal {
			return fmt.Errorf("fencing: authority root no longer requires mutation journal")
		}
		if err := ledger.BindMutationScope(id, scopeHash, profile); err != nil {
			return err
		}
		return ledger.BindMutationStage(id, stageIncarnation)
	})
	if err != nil {
		return err
	}
	confirmed, err := e.read(ctx, uid, id.SourceClusterID)
	if err != nil {
		return err
	}
	confirmedRun, ok := confirmed.Ledger.Runs[id.RunID]
	boundRun, boundOK := bound.Ledger.Runs[id.RunID]
	producer, producerUID, _, producerErr := e.AuthorityState.ReadProducer(ctx, runtime)
	if producerErr != nil {
		return producerErr
	}
	if confirmed.Ledger.ClosedBy != "" || confirmed.Ledger.ClosedHash != "" || !ok || confirmedRun.Identity != id ||
		confirmedRun.Runtime == nil || *confirmedRun.Runtime != runtime || confirmedRun.MutationProducerUID == "" ||
		confirmedRun.MutationScopeHash != scopeHash || confirmedRun.MutationProfile != profile ||
		confirmedRun.MutationStageIncarnation != stageIncarnation || producerUID != confirmedRun.MutationProducerUID ||
		producer.Runtime != runtime || producer.WALIdentity != id.WALIdentity || producer.Phase != fencing.MutationReserved ||
		producer.LastAdmitted != 0 || len(producer.Entries) != 0 || !boundOK || boundRun.Identity != id {
		return fmt.Errorf("fencing: active restart producer binding was not durably pinned before grant")
	}
	return nil
}

// scopeHash is derived only by the authenticated server from the owned workload,
// never accepted from the supervisor's request body. Tests use synthetic scopes.
func (e *FencingExecutor) bindWALAtScope(ctx context.Context, uid string, id fencing.Identity, scopeHash string) (fencing.BindReceipt, error) {
	return e.bindWALAtScopeWithStage(ctx, uid, id, scopeHash, "")
}

func (e *FencingExecutor) bindWALAtScopeWithStage(ctx context.Context, uid string, id fencing.Identity, scopeHash, stageIncarnation string) (fencing.BindReceipt, error) {
	currentRoot, err := e.read(ctx, uid, id.SourceClusterID)
	if err != nil {
		return fencing.BindReceipt{}, err
	}
	if err := e.validateMutationJournalProfile(currentRoot); err != nil {
		return fencing.BindReceipt{}, err
	}
	profile := e.sourceMutationProfile()
	if existing, ok := currentRoot.Ledger.Runs[id.RunID]; ok && existing.MutationProfile != "" {
		if !fencing.MutationProfileSupported(existing.MutationProfile) {
			return fencing.BindReceipt{}, fmt.Errorf("fencing: existing source mutation profile is unsupported")
		}
		if profile == fencing.MutationHeldExecutionProfile && existing.MutationProfile != profile {
			return fencing.BindReceipt{}, fmt.Errorf("fencing: configured held-create profile cannot attach to an existing source mutation profile")
		}
		profile = existing.MutationProfile
	}
	var repair fencing.EngineRepairRecord
	var repairReference fencing.EngineRepairReference
	repairKey := ""
	var repairProof learnerPromotionHash
	engineOwned := false
	if run, ok := currentRoot.Ledger.Runs[id.RunID]; ok && run.Promotion != nil {
		var verifyErr error
		repair, repairReference, repairKey, repairProof, engineOwned, verifyErr = e.verifyEngineRepairPromotion(ctx, uid, id, run)
		if verifyErr != nil {
			return fencing.BindReceipt{}, verifyErr
		}
	}
	state, err := e.change(ctx, uid, id.SourceClusterID, func(l *fencing.Ledger) error {
		if engineOwned {
			candidateRun, ok := l.Runs[id.RunID]
			if !ok || candidateRun.Runtime == nil {
				return fencing.ErrClosed
			}
			// Active restarts preserve the immutable original candidate runtime in
			// the repair record. Bind authorization must use the current Run's
			// runtime from this CAS snapshot, which the current root reference and
			// Run provenance pin to the new incarnation.
			if err := validateEngineRepairCandidateCAS(l, repair, repairReference, repairKey, id, *candidateRun.Runtime, engineRepairBindCAS, repairProof); err != nil {
				return err
			}
		}
		if e.RequireMutationJournal && !l.RequireMutationJournal {
			return fmt.Errorf("fencing: authority root does not require mutation journal")
		}
		if l.RequireMutationJournal {
			if err := l.BindMutationScope(id, scopeHash, profile); err != nil {
				return err
			}
			if stageIncarnation != "" {
				if err := l.BindMutationStage(id, stageIncarnation); err != nil {
					return err
				}
			} else if l.Runs[id.RunID].MutationStageIncarnation != "" {
				return fmt.Errorf("fencing: bound mutation stage store is unavailable")
			}
		} else if scopeHash != "" {
			return fmt.Errorf("fencing: mutation binding profile changed")
		}
		return l.Bind(id)
	})
	if err != nil {
		return fencing.BindReceipt{}, err
	}
	run := state.Ledger.Runs[id.RunID]
	if state.Ledger.RequireMutationJournal {
		if run.Runtime == nil {
			return fencing.BindReceipt{}, fmt.Errorf("fencing: bound runtime is missing")
		}
		producer, err := e.changeMutationProducer(ctx, *run.Runtime, func(p *fencing.MutationProducer) error { return p.Activate(id) })
		if err != nil {
			return fencing.BindReceipt{}, err
		}
		if producer.Phase != fencing.MutationOpen || producer.WALIdentity != id.WALIdentity {
			return fencing.BindReceipt{}, fencing.ErrClosed
		}
	}
	confirmed, err := e.read(ctx, uid, id.SourceClusterID)
	if err != nil {
		return fencing.BindReceipt{}, err
	}
	current, ok := confirmed.Ledger.Runs[id.RunID]
	_, voterClosed := confirmed.Ledger.ClosedNodes[id.NodeID]
	if !ok || current.Identity != id || current.Stop != nil || confirmed.Ledger.ClosedBy != "" || voterClosed {
		return fencing.BindReceipt{}, fencing.ErrClosed
	}
	if confirmed.Ledger.RequireMutationJournal != state.Ledger.RequireMutationJournal {
		return fencing.BindReceipt{}, fmt.Errorf("fencing: binding profile changed during activation")
	}
	receipt := fencing.BindReceipt{Identity: id, Role: fencing.RunRole(current)}
	if confirmed.Ledger.RequireMutationJournal {
		if current.Runtime == nil || run.Runtime == nil || *current.Runtime != *run.Runtime || current.MutationProducerUID != run.MutationProducerUID || current.MutationScopeHash != scopeHash || current.MutationProfile != profile || current.MutationStageIncarnation != stageIncarnation {
			return fencing.BindReceipt{}, fmt.Errorf("fencing: producer binding changed during activation")
		}
		receipt.Producer = &fencing.ProducerBinding{ContractVersion: fencing.MutationIntentVersion,
			Runtime: *current.Runtime, ProducerUID: current.MutationProducerUID,
			StoreScopeHash: current.MutationScopeHash, ExecutionProfile: current.MutationProfile,
			StageIncarnation: current.MutationStageIncarnation}
	}
	if (e.RequireMutationJournal || confirmed.Ledger.RequireMutationJournal) && receipt.Producer == nil {
		return fencing.BindReceipt{}, fmt.Errorf("fencing: required mutation-journal binding has no validated producer")
	}
	return receipt, receipt.Validate(id)
}

func (e *FencingExecutor) sourceMutationProfile() string {
	if e != nil && e.MutationProfile != "" {
		return e.MutationProfile
	}
	return fencing.MutationExecutionProfile
}
