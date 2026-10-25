package operator

import (
	"context"
	"fmt"
	"strings"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

type engineRepairCandidateCASStage uint8

const (
	engineRepairPromotionCAS engineRepairCandidateCASStage = iota + 1
	engineRepairBindCAS
	engineRepairProducerPinCAS
)

// currentEngineRepairCandidate returns one authority-root-pinned snapshot for
// this physical candidate. The snapshot is captured before a transition and
// compared again inside its root CAS; a newer continuation cannot inherit the
// older RunID's promotion or producer binding.
func (e *FencingExecutor) currentEngineRepairCandidate(ctx context.Context, uid string, id fencing.Identity) (fencing.EngineRepairRecord, fencing.EngineRepairReference, string, bool, error) {
	record, found, err := e.engineRepairRecordForRuntime(ctx, uid, id)
	if err != nil || !found {
		return fencing.EngineRepairRecord{}, fencing.EngineRepairReference{}, "", found, err
	}
	key, err := record.Binding.EngineRepairGenerationKey()
	if err != nil {
		return fencing.EngineRepairRecord{}, fencing.EngineRepairReference{}, "", true, err
	}
	state, err := e.read(ctx, uid, id.SourceClusterID)
	if err != nil {
		return fencing.EngineRepairRecord{}, fencing.EngineRepairReference{}, "", true, err
	}
	want, ok := state.Ledger.EngineRepairs[key]
	if !ok || want.Validate() != nil || want.Binding != record.Binding || want.OperationID != record.Binding.OperationID ||
		want.RecordDigest != record.Digest() || want.IdentityDigest != record.IdentityDigest() || want.CandidateIdentity != id {
		return fencing.EngineRepairRecord{}, fencing.EngineRepairReference{}, "", true, fencing.ErrClosed
	}
	return record, want, key, true, nil
}

// validateEngineRepairCandidateCAS is pure so every authorization check runs
// against the exact ledger value that will be conditionally written.
func validateEngineRepairCandidateCAS(l *fencing.Ledger, record fencing.EngineRepairRecord, expected fencing.EngineRepairReference, key string, id fencing.Identity, runtime fencing.RuntimeBinding, stage engineRepairCandidateCASStage, proof learnerPromotionHash) error {
	if l == nil || !l.RequireRuntimeBinding || !l.RequireMutationJournal || key == "" ||
		record.Phase != fencing.RepairAdding && record.Phase != fencing.RepairActive ||
		l.EngineRepairs[key] != expected || expected.Validate() != nil || expected.Binding != record.Binding ||
		expected.OperationID != record.Binding.OperationID || expected.RecordDigest != record.Digest() || expected.IdentityDigest != record.IdentityDigest() || expected.CandidateIdentity != id ||
		record.Binding.StatefulSetUID != runtime.StatefulSetUID || record.Binding.SourceClusterID != id.SourceClusterID ||
		(record.CurrentCandidateIdentity() != id && (!sameEngineRepairCandidateRun(record.CurrentCandidateIdentity(), id) || record.Phase != fencing.RepairActive)) ||
		(record.CurrentCandidateIdentity() == id && (record.CurrentCandidateRuntime() == nil || *record.CurrentCandidateRuntime() != runtime || record.CurrentCandidateRuntimeHash() != fencing.RuntimeBindingDigest(runtime))) ||
		runtime.Validate(id) != nil {
		return fencing.ErrClosed
	}
	run, ok := l.Runs[id.RunID]
	if !ok || run.Identity != id || run.Runtime == nil || *run.Runtime != runtime || run.Stop != nil || run.Exited || run.NeverBoundExit != nil ||
		l.ClosedBy != "" || l.ClosedHash != "" || run.EngineRepairProvenance == nil ||
		run.EngineRepairProvenance.OperationID != record.Binding.OperationID ||
		run.EngineRepairProvenance.IdentityDigest != record.IdentityDigest() ||
		!sameEngineRepairCandidateRun(run.EngineRepairProvenance.CandidateIdentity, id) || run.EngineRepairProvenance.ValidateForRun(run) != nil {
		return fencing.ErrClosed
	}
	_, closed := l.ClosedNodes[id.NodeID]
	if closed {
		return fencing.ErrClosed
	}
	addOperation := engineRepairAddOperationID(record.Binding.OperationID)
	switch stage {
	case engineRepairPromotionCAS:
		if id.WALIdentity == "" || !validEvidenceDigest(proof.digest) || proof.operationID != addOperation {
			return fencing.ErrClosed
		}
		if fencing.RunRole(run) == fencing.StartupRoleLearnerPendingRemove {
			if run.Promotion != nil || record.Phase != fencing.RepairAdding {
				return fencing.ErrClosed
			}
			return nil
		}
		if fencing.RunRole(run) == fencing.StartupRoleVoterActive && run.Promotion != nil &&
			run.Promotion.OperationID == proof.operationID && run.Promotion.EvidenceHash == proof.digest {
			return nil
		}
		return fencing.ErrClosed
	case engineRepairBindCAS, engineRepairProducerPinCAS:
		if fencing.RunRole(run) != fencing.StartupRoleVoterActive || run.Promotion == nil ||
			run.Promotion.OperationID != addOperation || !validEvidenceDigest(run.Promotion.EvidenceHash) {
			return fencing.ErrClosed
		}
		if proof.operationID != addOperation || !validEvidenceDigest(proof.digest) ||
			run.Promotion.OperationID != proof.operationID || run.Promotion.EvidenceHash != proof.digest {
			return fencing.ErrClosed
		}
		if stage == engineRepairProducerPinCAS && run.MutationProducerUID != "" {
			return fencing.ErrClosed
		}
		return nil
	default:
		return fmt.Errorf("fencing: unknown engine repair candidate CAS stage")
	}
}

func (e *FencingExecutor) verifyEngineRepairPromotion(ctx context.Context, uid string, id fencing.Identity, run fencing.Run) (fencing.EngineRepairRecord, fencing.EngineRepairReference, string, learnerPromotionHash, bool, error) {
	record, reference, key, found, err := e.currentEngineRepairCandidate(ctx, uid, id)
	if err != nil || !found {
		if err == nil && (run.EngineRepairProvenance != nil || run.Promotion != nil && strings.HasPrefix(run.Promotion.OperationID, "engine-repair-add-")) {
			return record, reference, key, learnerPromotionHash{}, true, fencing.ErrClosed
		}
		return record, reference, key, learnerPromotionHash{}, found, err
	}
	provenance := run.EngineRepairProvenance
	if run.Runtime == nil || run.Identity != id || provenance == nil ||
		provenance.OperationID != record.Binding.OperationID || provenance.IdentityDigest != record.IdentityDigest() ||
		!sameEngineRepairCandidateRun(provenance.CandidateIdentity, id) || provenance.ValidateForRun(run) != nil {
		return record, reference, key, learnerPromotionHash{}, true, fencing.ErrClosed
	}
	if record.Phase == fencing.RepairActive && record.CurrentCandidateIdentity() != id {
		if err := e.verifyEngineRepairActiveRestartCurrent(ctx, uid, record, id, *run.Runtime); err != nil {
			return record, reference, key, learnerPromotionHash{}, true, err
		}
		if run.Promotion == nil || run.Promotion.OperationID != engineRepairAddOperationID(record.Binding.OperationID) || !validEvidenceDigest(run.Promotion.EvidenceHash) {
			return record, reference, key, learnerPromotionHash{}, true, fencing.ErrClosed
		}
		return record, reference, key, learnerPromotionHash{operationID: run.Promotion.OperationID, digest: run.Promotion.EvidenceHash}, true, nil
	}
	proof, err := e.engineRepairCandidateActivationEvidence(ctx, *run.Runtime, record)
	if err != nil {
		return record, reference, key, learnerPromotionHash{}, true, err
	}
	if proof.operationID != engineRepairAddOperationID(record.Binding.OperationID) || !validEvidenceDigest(proof.digest) {
		return record, reference, key, learnerPromotionHash{}, true, fencing.ErrClosed
	}
	return record, reference, key, proof, true, nil
}
