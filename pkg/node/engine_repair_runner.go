package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// engineRepairRunnerAuthority is intentionally the narrow consumer contract
// for the authenticated fencing session. Reads and phase changes must resolve
// through the canonical root reference; CloseEngineRepairStorageEffects takes
// only an operation ID and independently re-reads the record and freeze.
type engineRepairRunnerAuthority interface {
	CurrentEngineRepair(context.Context) (fencing.EngineRepairRecord, bool, error)
	ListPendingEngineRepairs(context.Context) ([]fencing.PendingEngineRepairRequest, error)
	AdoptPendingEngineRepair(context.Context, string, string, fencing.EngineRepairRecord) (fencing.EngineRepairRecord, error)
	CompareAndSwapEngineRepair(context.Context, string, fencing.EngineRepairRecord) (fencing.EngineRepairRecord, error)
	CloseEngineRepairStorageEffects(context.Context, string) (fencing.StorageEffectsExcluded, error)
	ConfirmEngineRepair(context.Context, string) (fencing.EngineRepairRecord, error)
}

// startEngineRepairRunner runs the same canonical operation on every active
// voter. The root CAS and QuePaxa's exact-value rules serialize progress; this
// worker does not elect a leader or acquire a time-based lease.
func (n *Node) startEngineRepairRunner(ctx context.Context) {
	if n == nil || n.config == nil || !n.config.EngineOwnedRemovals || n.fenceSession == nil || ctx == nil {
		return
	}
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		for round := uint64(0); ; round++ {
			delay := syncInterval(n.config.NodeID, round)
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			if err := n.advanceEngineRepair(ctx); err != nil && ctx.Err() == nil {
				log.Printf("engine-owned recovery remains pending: %v", err)
			}
		}
	}()
}

// advanceEngineRepair executes at most one durable phase transition. Each
// retry re-reads the canonical repair record and consensus state so a phase
// label alone can never authorize a subsequent action.
func (n *Node) advanceEngineRepair(ctx context.Context) error {
	if n == nil || ctx == nil || n.fenceSession == nil || n.core == nil || n.transport == nil ||
		n.config == nil || !n.config.EngineOwnedRemovals || !n.config.EnableReconfiguration || !n.core.IsVoter() ||
		n.fenceSession.StartupRole() != fencing.StartupRoleVoterActive {
		return nil
	}
	authority, ok := any(n.fenceSession).(engineRepairRunnerAuthority)
	if !ok {
		return fmt.Errorf("authenticated engine repair authority is unavailable")
	}
	repair, found, err := authority.CurrentEngineRepair(ctx)
	if err != nil {
		return err
	}
	if !found || repair.Phase == fencing.RepairActive {
		// Active records remain durable history, but must not hide a later
		// pending repair. The authority remains the only source of pending work.
		pendingRepair, pendingFound, adoptErr := n.adoptPendingEngineRepair(ctx, authority)
		if adoptErr != nil {
			return adoptErr
		}
		if pendingFound {
			repair, found = pendingRepair, true
		}
	}
	if !found {
		return nil
	}
	if err := repair.Validate(); err != nil {
		return fmt.Errorf("canonical engine repair record is invalid: %w", err)
	}
	if repair.Binding.SourceClusterID != string(n.config.ClusterID) || repair.Phase == fencing.RepairGenerationClosing {
		return nil
	}

	switch repair.Phase {
	case fencing.RepairCandidatePrepared:
		latest, found, err := authority.CurrentEngineRepair(ctx)
		if err != nil {
			return err
		}
		if !found || latest.Digest() != repair.Digest() || latest.Phase != fencing.RepairCandidatePrepared {
			return fmt.Errorf("engine repair reservation changed before REMOVE freeze")
		}
		operation, _, err := n.freezeEngineRepairRemoval(ctx, repair)
		if err != nil {
			return err
		}
		if err := n.persistMembership(ctx); err != nil {
			return fmt.Errorf("archive engine-owned REMOVE freeze: %w", err)
		}
		next := repair
		next.Binding.FreezeSlot = uint64(operation.Freeze)
		next.Binding.FreezeHash = fmt.Sprintf("%x", operation.FreezeHash[:])
		next.RemovalOperation = &operation
		next.Phase = fencing.RepairRemoveFrozen
		_, err = authority.CompareAndSwapEngineRepair(ctx, repair.Digest(), next)
		return err
	case fencing.RepairRemoveFrozen:
		if repair.RemovalOperation == nil {
			return fmt.Errorf("canonical frozen repair lacks its exact REMOVE operation")
		}
		if err := engineRepairOperationFromFreeze(repair, *repair.RemovalOperation); err != nil {
			return err
		}
		if _, err := n.EngineRemovalFreezeEvidence(ctx, *repair.RemovalOperation); err != nil {
			return err
		}
		next := repair
		next.Phase = fencing.RepairStorageClosing
		_, err := authority.CompareAndSwapEngineRepair(ctx, repair.Digest(), next)
		return err
	case fencing.RepairStorageClosing:
		if repair.RemovalOperation == nil {
			return fmt.Errorf("storage-closing repair lacks its exact REMOVE operation")
		}
		var proof fencing.StorageEffectsExcluded
		proofReady := false
		if repair.StorageProofDigest != "" {
			proof, err = authority.CloseEngineRepairStorageEffects(ctx, repair.Binding.OperationID)
			if err != nil {
				return err
			}
			if err := proof.Validate(repair); err != nil || proof.ProofDigest != repair.StorageProofDigest {
				return fmt.Errorf("canonical storage proof differs from the pinned engine repair evidence")
			}
			proofReady = true
			completed, terminalErr := n.resumeEngineRepairTerminal(ctx, authority, repair, proof)
			if terminalErr != nil {
				return terminalErr
			}
			if completed {
				return nil
			}
		}
		if _, err := n.EngineRemovalFreezeEvidence(ctx, *repair.RemovalOperation); err != nil {
			return err
		}
		if !proofReady {
			proof, err = authority.CloseEngineRepairStorageEffects(ctx, repair.Binding.OperationID)
			if err != nil {
				return err
			}
			if err := proof.Validate(repair); err != nil {
				return fmt.Errorf("storage authority returned invalid engine repair evidence: %w", err)
			}
		}
		// The authority persists the proof digest as part of settlement. Never
		// CAS from the stale pre-close record: another survivor may already have
		// advanced the same canonical operation while this request was running.
		latest, found, err := authority.CurrentEngineRepair(ctx)
		if err != nil {
			return err
		}
		if !found || latest.IdentityDigest() != repair.IdentityDigest() || latest.Binding.OperationID != repair.Binding.OperationID {
			return fmt.Errorf("canonical engine repair changed during storage closure")
		}
		if latest.StorageProofDigest != "" && latest.StorageProofDigest != proof.ProofDigest {
			return fmt.Errorf("canonical engine repair storage proof changed")
		}
		if latest.Phase != fencing.RepairStorageClosing {
			// A DR close or another survivor's later phase wins. The old repair
			// must not resume membership changes after that root-CAS transition.
			return nil
		}
		if latest.StorageProofDigest == "" {
			next := latest
			next.StorageProofDigest = proof.ProofDigest
			_, err := authority.CompareAndSwapEngineRepair(ctx, latest.Digest(), next)
			return err
		}
		if err := proof.Validate(latest); err != nil {
			return fmt.Errorf("canonical engine repair does not match settled storage evidence: %w", err)
		}
		terminalHash, err := n.completeEngineRepairRemoval(ctx, latest, proof)
		if err != nil {
			return err
		}
		next := latest
		next.RemoveTerminalDigest = terminalHash
		next.Phase = fencing.RepairRemoved
		_, err = authority.CompareAndSwapEngineRepair(ctx, latest.Digest(), next)
		return err
	case fencing.RepairRemoved:
		if err := n.verifyEngineRepairRemoveReadback(ctx, repair, repair.RemoveTerminalDigest); err != nil {
			return err
		}
		request, err := n.engineRepairAddRequest(repair)
		if err != nil {
			return err
		}
		requestHash, err := engineRepairRequestDigest(request)
		if err != nil {
			return err
		}
		next := repair
		next.AddRequestHash = requestHash
		next.Phase = fencing.RepairAdding
		_, err = authority.CompareAndSwapEngineRepair(ctx, repair.Digest(), next)
		return err
	case fencing.RepairAdding:
		if err := n.completeEngineRepairAdd(ctx, repair); err != nil {
			return err
		}
		_, err := authority.ConfirmEngineRepair(ctx, repair.Binding.OperationID)
		return err
	case fencing.RepairActive:
		if err := n.verifyEngineRepairRemoveReadback(ctx, repair, repair.RemoveTerminalDigest); err != nil {
			return err
		}
		request, err := n.engineRepairAddRequest(repair)
		if err != nil {
			return err
		}
		target, err := engineRepairAddTarget(repair)
		if err != nil {
			return err
		}
		return n.verifyEngineRepairAddReadback(ctx, repair, request, target)
	default:
		return fmt.Errorf("canonical engine repair has an unsupported phase")
	}
}

// resumeEngineRepairTerminal handles the crash window where the exact REMOVE
// terminal is already durably decided but the canonical repair phase CAS to
// Removed was lost. It requires both original-quorum terminal evidence and
// the same locally retained certified bytes before it advances the record.
func (n *Node) resumeEngineRepairTerminal(ctx context.Context, authority engineRepairRunnerAuthority,
	repair fencing.EngineRepairRecord, storageProof fencing.StorageEffectsExcluded) (bool, error) {
	if n == nil || ctx == nil || authority == nil || repair.RemovalOperation == nil ||
		repair.Phase != fencing.RepairStorageClosing || repair.StorageProofDigest == "" ||
		storageProof.ProofDigest != repair.StorageProofDigest {
		return false, fmt.Errorf("engine repair terminal resume lacks its exact storage proof")
	}
	if err := storageProof.Validate(repair); err != nil {
		return false, fmt.Errorf("engine repair terminal resume storage proof is invalid: %w", err)
	}
	operation := *repair.RemovalOperation
	storageHash, err := repairProofValueHash(storageProof.ProofDigest)
	if err != nil {
		return false, err
	}
	operation.StorageEffectsExcludedHash = storageHash
	terminalProof, err := n.EngineRemovalTerminalEvidence(ctx, operation)
	if errors.Is(err, quepaxa.ErrQuorumUnavailable) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	terminalHash := fmt.Sprintf("%x", terminalProof.Snapshot.Decision.Hash[:])
	// The REMOVE freeze's persisted operation intentionally has no storage
	// proof hash; the terminal value binds the hash separately. Confirm the
	// local history against the original operation while quorum evidence above
	// verifies the terminal operation containing the proof hash.
	localHash, err := n.confirmEngineRemovalTerminal(ctx, repair, *repair.RemovalOperation)
	if err != nil {
		return false, fmt.Errorf("durable engine repair terminal is not locally applied and read back: %w", err)
	}
	if localHash != terminalHash {
		return false, fmt.Errorf("original-quorum terminal hash differs from local REMOVE readback")
	}
	latest, found, err := authority.CurrentEngineRepair(ctx)
	if err != nil {
		return false, err
	}
	if !found || latest.IdentityDigest() != repair.IdentityDigest() || latest.Binding.OperationID != repair.Binding.OperationID ||
		latest.Phase != fencing.RepairStorageClosing || latest.StorageProofDigest != storageProof.ProofDigest ||
		latest.RemovalOperation == nil || !fencing.SameEngineRemovalOperation(*latest.RemovalOperation, *repair.RemovalOperation) {
		return false, fmt.Errorf("canonical engine repair changed during terminal recovery")
	}
	if err := latest.Validate(); err != nil {
		return false, fmt.Errorf("canonical engine repair is invalid during terminal recovery: %w", err)
	}
	next := latest
	next.RemoveTerminalDigest = terminalHash
	next.Phase = fencing.RepairRemoved
	if _, err := authority.CompareAndSwapEngineRepair(ctx, latest.Digest(), next); err != nil {
		return false, err
	}
	return true, nil
}

// adoptPendingEngineRepair consumes only a canonical request returned by the
// authenticated authority. The authority attaches the complete root run
// inventory during its adoption CAS; this Node proposal deliberately carries
// no Incarnations or candidate runtime evidence.
func (n *Node) adoptPendingEngineRepair(ctx context.Context, authority engineRepairRunnerAuthority) (fencing.EngineRepairRecord, bool, error) {
	if n == nil || ctx == nil || authority == nil || n.core == nil || n.fenceSession == nil || n.config == nil ||
		!n.config.EngineOwnedRemovals || !n.core.IsVoter() || n.fenceSession.StartupRole() != fencing.StartupRoleVoterActive {
		return fencing.EngineRepairRecord{}, false, fmt.Errorf("pending engine repair adoption requires an active survivor")
	}
	pending, err := authority.ListPendingEngineRepairs(ctx)
	if err != nil {
		return fencing.EngineRepairRecord{}, false, err
	}
	if len(pending) == 0 {
		return fencing.EngineRepairRecord{}, false, nil
	}
	if len(pending) != 1 {
		return fencing.EngineRepairRecord{}, false, fmt.Errorf("multiple pending engine repair requests require explicit canonical selection")
	}
	request := pending[0]
	current := n.core.CurrentCluster()
	abortSlot, _, _ := n.core.LastReconfigurationAbort()
	identity := n.fenceSession.Identity()
	oldMember, oldFound := current.MemberSet()[quepaxa.NodeID(request.OldMemberID)]
	if !oldFound || oldMember.WALIdentity == "" || request.OldWALIdentity != "" && request.OldWALIdentity != oldMember.WALIdentity {
		return fencing.EngineRepairRecord{}, false, fmt.Errorf("pending engine repair old member differs from current certified membership")
	}
	removeRequest := network.MembershipChange{OperationID: request.OperationID, ClusterID: request.SourceClusterID,
		ExpectedConfigID: current.ConfigID, ExpectedAbortSlot: abortSlot, Remove: quepaxa.NodeID(request.OldMemberID)}
	removeRequestHash, err := engineRepairRequestDigest(removeRequest)
	if err != nil {
		return fencing.EngineRepairRecord{}, false, err
	}
	repair := fencing.EngineRepairRecord{
		Version: fencing.EngineRepairRecordVersion,
		Binding: fencing.EngineRemovalBinding{
			Version: fencing.EngineRemovalBindingVersion, Mode: fencing.EngineRemovalMode,
			OperationID: request.OperationID, RequestHash: removeRequestHash,
			SourceClusterID: request.SourceClusterID, StatefulSetUID: request.StatefulSetUID,
			OldConfigID: uint64(current.ConfigID), ExpectedAbortSlot: uint64(abortSlot),
			OldMemberID: request.OldMemberID, OldWALIdentity: oldMember.WALIdentity,
			TargetMemberID: request.CandidateIdentity.NodeID,
		},
		BootstrapRequestDigest: request.RequestDigest,
		SourceConfig:           current, SourceAdminCredential: request.SourceAdminCredential,
		CandidateIdentity: request.CandidateIdentity, CandidateMember: request.CandidateMember,
		CandidateCredential:   request.CandidateCredential,
		CandidateMemberDigest: fencing.EngineRepairMemberDigest(request.CandidateMember),
		RemoveRequestHash:     removeRequestHash, Phase: fencing.RepairCandidatePrepared,
	}
	if err := validatePendingEngineRepairSnapshot(request, repair, current, uint64(abortSlot), identity, n.fenceSession.StartupRole()); err != nil {
		return fencing.EngineRepairRecord{}, false, err
	}
	if err := repair.ValidateAdoptionProposal(); err != nil {
		return fencing.EngineRepairRecord{}, false, fmt.Errorf("invalid engine repair adoption proposal: %w", err)
	}
	adopted, err := authority.AdoptPendingEngineRepair(ctx, request.PendingID, request.RequestDigest, repair)
	if err != nil {
		return fencing.EngineRepairRecord{}, false, err
	}
	if adopted.Validate() != nil || !request.MatchesRecord(adopted) || adopted.Phase != fencing.RepairCandidatePrepared ||
		len(adopted.Incarnations) == 0 || adopted.CandidateRuntimeBinding != nil || adopted.CandidateRuntimeHash != "" {
		return fencing.EngineRepairRecord{}, false, fmt.Errorf("authority returned an invalid adopted engine repair reservation")
	}
	return adopted, true, nil
}

func (n *Node) completeEngineRepairRemoval(ctx context.Context, repair fencing.EngineRepairRecord, proof fencing.StorageEffectsExcluded) (string, error) {
	if repair.RemovalOperation == nil || repair.StorageProofDigest != proof.ProofDigest {
		return "", fmt.Errorf("engine repair storage proof is not the canonical repair proof")
	}
	if err := proof.Validate(repair); err != nil {
		return "", err
	}
	operation := *repair.RemovalOperation
	if err := engineRepairOperationFromFreeze(repair, operation); err != nil {
		return "", err
	}
	if _, err := n.EngineRemovalFreezeEvidence(ctx, operation); err != nil {
		return "", fmt.Errorf("revalidate original-quorum REMOVE freeze: %w", err)
	}
	proofHash, err := repairProofValueHash(proof.ProofDigest)
	if err != nil {
		return "", err
	}
	operation.StorageEffectsExcludedHash = proofHash

	current := n.core.CurrentCluster()
	if sameRepairCluster(current, operation.Target) {
		return n.confirmEngineRemovalTerminal(ctx, repair, operation)
	}
	if !sameRepairCluster(current, repair.SourceConfig) {
		return "", fmt.Errorf("engine repair cannot finish REMOVE from an unexpected membership")
	}
	pending, ok := n.core.PendingReconfiguration()
	if !ok || !sameRepairCluster(pending, operation.Target) {
		return "", fmt.Errorf("exact engine-owned REMOVE freeze is no longer pending")
	}
	if err := n.core.FinishEngineOwnedRemovalAt(ctx, operation); err != nil {
		return "", fmt.Errorf("finish engine-owned REMOVE after storage closure: %w", err)
	}
	if !sameRepairCluster(n.core.CurrentCluster(), operation.Target) {
		return "", fmt.Errorf("engine-owned REMOVE terminal did not install its exact target")
	}
	return n.confirmEngineRemovalTerminal(ctx, repair, operation)
}

func (n *Node) confirmEngineRemovalTerminal(ctx context.Context, repair fencing.EngineRepairRecord, operation quepaxa.EngineRemoval) (string, error) {
	freeze, ok := n.core.CertifiedValue(operation.Freeze)
	if !ok || freeze.Hash != operation.FreezeHash || freeze.Slot != operation.Freeze {
		return "", fmt.Errorf("engine repair freeze is not present in certified history")
	}
	decoded, isFreeze, err := quepaxa.DecodeEngineRemovalFreeze(freeze.Value)
	if err != nil || !isFreeze {
		return "", fmt.Errorf("certified engine repair freeze is malformed")
	}
	wantFreeze := operation
	wantFreeze.FreezeHash = quepaxa.ValueHash{}
	wantFreeze.StorageEffectsExcludedHash = quepaxa.ValueHash{}
	if !fencing.SameEngineRemovalOperation(decoded.Operation, wantFreeze) ||
		decoded.Freeze != operation.Freeze || !sameRepairCluster(decoded.Target, operation.Target) ||
		!sameRepairCluster(n.core.ClusterForSlot(operation.Freeze), repair.SourceConfig) {
		return "", fmt.Errorf("certified engine repair freeze differs from the canonical operation")
	}
	terminal, ok := n.core.CertifiedValue(operation.Freeze + 16)
	if !ok || terminal.Slot != operation.Freeze+16 || !sameRepairCluster(n.core.CurrentCluster(), operation.Target) ||
		!sameRepairCluster(n.core.ClusterForSlot(operation.Freeze+17), operation.Target) {
		return "", fmt.Errorf("engine repair REMOVE terminal is not applied in certified membership")
	}
	terminalHash := fmt.Sprintf("%x", terminal.Hash[:])
	if err := n.verifyEngineRepairRemoveReadback(ctx, repair, terminalHash); err != nil {
		return "", err
	}
	return terminalHash, nil
}

func (n *Node) verifyEngineRepairRemoveReadback(ctx context.Context, repair fencing.EngineRepairRecord, terminalDigest string) error {
	if n == nil || ctx == nil || n.core == nil || n.archive == nil || repair.RemovalOperation == nil || repair.StorageProofDigest == "" ||
		terminalDigest == "" || repair.RemoveTerminalDigest != "" && terminalDigest != repair.RemoveTerminalDigest {
		return fmt.Errorf("engine repair REMOVE readback is incomplete")
	}
	operation := *repair.RemovalOperation
	if err := engineRepairOperationFromFreeze(repair, operation); err != nil {
		return err
	}
	if err := n.persistMembership(ctx); err != nil {
		return fmt.Errorf("archive engine repair REMOVE history: %w", err)
	}
	history, err := n.core.MembershipHistory()
	if err != nil {
		return fmt.Errorf("read durable engine repair membership history: %w", err)
	}
	if !sameRepairCluster(n.core.ClusterForSlot(operation.Freeze), repair.SourceConfig) ||
		!sameRepairCluster(n.core.ClusterForSlot(operation.Freeze+17), operation.Target) {
		return fmt.Errorf("engine repair REMOVE history differs from its pinned source or survivor target")
	}
	terminalHash, err := repairProofValueHash(terminalDigest)
	if err != nil {
		return err
	}
	matches := 0
	for _, transition := range history.Transitions {
		if transition.Freeze.Slot != operation.Freeze {
			continue
		}
		matches++
		if transition.Freeze.Hash != operation.FreezeHash || transition.Terminal.Slot != operation.Freeze+16 || transition.Terminal.Hash != terminalHash {
			return fmt.Errorf("durable engine repair REMOVE terminal differs from its canonical digest")
		}
		freeze, isFreeze, err := quepaxa.DecodeEngineRemovalFreeze(transition.Freeze.Value)
		if err != nil || !isFreeze {
			return fmt.Errorf("durable engine repair REMOVE freeze is malformed")
		}
		want := operation
		want.FreezeHash = quepaxa.ValueHash{}
		want.StorageEffectsExcludedHash = quepaxa.ValueHash{}
		if !fencing.SameEngineRemovalOperation(freeze.Operation, want) || !sameRepairCluster(freeze.Target, operation.Target) {
			return fmt.Errorf("durable engine repair REMOVE freeze differs from its exact operation")
		}
	}
	if matches != 1 {
		return fmt.Errorf("durable membership history does not contain one exact engine repair REMOVE")
	}
	return nil
}

func repairProofValueHash(digest string) (quepaxa.ValueHash, error) {
	var result quepaxa.ValueHash
	decoded, err := hex.DecodeString(digest)
	if err != nil || len(decoded) != len(result) {
		return result, fmt.Errorf("engine repair storage proof digest is malformed")
	}
	copy(result[:], decoded)
	return result, nil
}

func (n *Node) engineRepairAddRequest(repair fencing.EngineRepairRecord) (network.MembershipChange, error) {
	if n == nil || n.core == nil {
		return network.MembershipChange{}, fmt.Errorf("engine repair ADD source is unavailable")
	}
	abortSlot, _, _ := n.core.LastReconfigurationAbort()
	return engineRepairAddRequestFor(repair, n.core.CurrentCluster(), uint64(abortSlot))
}

func engineRepairAddRequestFor(repair fencing.EngineRepairRecord, current quepaxa.Cluster, abortSlot uint64) (network.MembershipChange, error) {
	if repair.RemovalOperation == nil || repair.StorageProofDigest == "" || repair.RemoveTerminalDigest == "" ||
		repair.Binding.OperationID == "" || repair.Binding.SourceClusterID == "" {
		return network.MembershipChange{}, fmt.Errorf("engine repair ADD precedes exact REMOVE closure")
	}
	removeTarget := repair.RemovalOperation.Target
	target := quepaxa.Cluster{ConfigID: removeTarget.ConfigID + 1,
		Members: append(append([]quepaxa.Member(nil), removeTarget.Members...), repair.CandidateMember)}
	if _, exists := removeTarget.MemberSet()[repair.CandidateMember.ID]; exists {
		return network.MembershipChange{}, fmt.Errorf("engine repair candidate is already in the REMOVE target")
	}
	if !sameRepairCluster(current, removeTarget) && !sameRepairCluster(current, target) {
		return network.MembershipChange{}, fmt.Errorf("engine repair ADD source differs from certified REMOVE or ADD target")
	}
	// REMOVE installs a successor configuration with a fresh abort history.
	// Keep the source configuration's abort slot in Binding, but use revision
	// zero for the first ADD in the successor configuration.
	if abortSlot != 0 {
		return network.MembershipChange{}, fmt.Errorf("engine repair ADD successor abort revision is not zero")
	}
	return network.MembershipChange{OperationID: engineRepairAddOperationID(repair.Binding.OperationID), ClusterID: repair.Binding.SourceClusterID,
		ExpectedConfigID: removeTarget.ConfigID, ExpectedAbortSlot: quepaxa.Slot(abortSlot), Add: &repair.CandidateMember}, nil
}

func engineRepairAddOperationID(operationID string) string {
	return fmt.Sprintf("engine-repair-add-%x", sha256.Sum256([]byte(operationID)))
}

func engineRepairRequestDigest(request network.MembershipChange) (string, error) {
	data, err := json.Marshal(request)
	if err != nil || len(data) == 0 || len(data) > 64<<10 {
		return "", fmt.Errorf("engine repair ADD request cannot be encoded")
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func engineRepairAddTarget(repair fencing.EngineRepairRecord) (quepaxa.Cluster, error) {
	if repair.RemovalOperation == nil || repair.CandidateMember.ID == "" ||
		repair.AddRequestHash == "" {
		return quepaxa.Cluster{}, fmt.Errorf("engine repair ADD target is not pinned")
	}
	current := repair.RemovalOperation.Target
	if current.MemberSet()[repair.CandidateMember.ID].ID != "" {
		return quepaxa.Cluster{}, fmt.Errorf("engine repair candidate is already in the REMOVE target")
	}
	target := quepaxa.Cluster{ConfigID: current.ConfigID + 1, Members: append([]quepaxa.Member(nil), current.Members...)}
	target.Members = append(target.Members, repair.CandidateMember)
	return target, nil
}

func (n *Node) completeEngineRepairAdd(ctx context.Context, repair fencing.EngineRepairRecord) error {
	if err := n.verifyEngineRepairRemoveReadback(ctx, repair, repair.RemoveTerminalDigest); err != nil {
		return err
	}
	request, err := n.engineRepairAddRequest(repair)
	if err != nil {
		return err
	}
	hash, err := engineRepairRequestDigest(request)
	if err != nil {
		return err
	}
	if hash != repair.AddRequestHash {
		return fmt.Errorf("engine repair ADD request differs from its canonical digest")
	}
	target, err := engineRepairAddTarget(repair)
	if err != nil {
		return err
	}
	current := n.core.CurrentCluster()
	if sameRepairCluster(current, target) {
		return n.verifyEngineRepairAddReadback(ctx, repair, request, target)
	}
	if !sameRepairCluster(current, repair.RemovalOperation.Target) {
		return fmt.Errorf("engine repair ADD cannot start from an unexpected membership")
	}
	through := n.core.Tip()
	prefix, ok := n.core.PrefixHash(through)
	if !ok || prefix == (quepaxa.ValueHash{}) {
		return fmt.Errorf("engine repair ADD certified prefix is unavailable")
	}
	if err := n.transport.VerifyLearner(ctx, repair.CandidateMember, through, prefix); err != nil {
		return fmt.Errorf("engine repair candidate has not caught up through REMOVE: %w", err)
	}
	if err := n.core.ValidateReconfigurationTarget(target); err != nil {
		return err
	}
	if err := n.changeMembership(ctx, request); err != nil {
		return fmt.Errorf("apply exact journaled engine repair ADD: %w", err)
	}
	if !sameRepairCluster(n.core.CurrentCluster(), target) {
		return fmt.Errorf("engine repair ADD terminal did not install its exact candidate")
	}
	return n.verifyEngineRepairAddReadback(ctx, repair, request, target)
}

// verifyEngineRepairAddReadback requires the exact request journal and its
// durable certified membership transition before the caller may ask the
// authority to activate the candidate. A local CurrentCluster alone is not a
// terminal receipt.
func (n *Node) verifyEngineRepairAddReadback(ctx context.Context, repair fencing.EngineRepairRecord, request network.MembershipChange, target quepaxa.Cluster) error {
	expectedTarget, err := engineRepairAddTarget(repair)
	if err != nil {
		return err
	}
	if n == nil || ctx == nil || n.core == nil || n.bucket == nil || repair.RemovalOperation == nil ||
		request.Add == nil || request.OperationID != engineRepairAddOperationID(repair.Binding.OperationID) ||
		request.ExpectedConfigID != repair.RemovalOperation.Target.ConfigID || request.ExpectedAbortSlot != 0 ||
		request.ClusterID != repair.Binding.SourceClusterID || !sameRepairCluster(target, expectedTarget) ||
		!sameRepairCluster(n.core.CurrentCluster(), target) {
		return fmt.Errorf("engine repair ADD readback request or applied target is incomplete")
	}
	hash, err := engineRepairRequestDigest(request)
	if err != nil || hash != repair.AddRequestHash {
		return fmt.Errorf("engine repair ADD readback request differs from its pinned digest")
	}
	data, err := readVoterRegistration(ctx, n.bucket, n.membershipOperationKey(request))
	if err != nil {
		return fmt.Errorf("read durable engine repair ADD request: %w", err)
	}
	var operation membershipOperation
	if len(data) == 0 || json.Unmarshal(data, &operation) != nil || !reflect.DeepEqual(operation.Request, request) || !sameRepairCluster(operation.Target, target) {
		return fmt.Errorf("durable engine repair ADD request differs from its exact operation")
	}
	if err := n.persistMembership(ctx); err != nil {
		return fmt.Errorf("archive engine repair ADD history: %w", err)
	}
	history, err := n.core.MembershipHistory()
	if err != nil {
		return fmt.Errorf("read durable engine repair ADD history: %w", err)
	}
	matches := 0
	for _, transition := range history.Transitions {
		if transition.Freeze.Slot == 0 || transition.Terminal.Slot != transition.Freeze.Slot+16 {
			continue
		}
		if !sameRepairCluster(n.core.ClusterForSlot(transition.Freeze.Slot), repair.RemovalOperation.Target) ||
			!sameRepairCluster(n.core.ClusterForSlot(transition.Freeze.Slot+17), target) {
			continue
		}
		matches++
	}
	if matches != 1 {
		return fmt.Errorf("durable membership history does not contain one exact engine repair ADD terminal")
	}
	return nil
}

// prepareEngineRepairCandidate verifies the candidate against the certified
// source prefix before the REMOVE freeze is proposed. It intentionally does
// not advance the canonical repair record: that operation requires the
// authenticated authority session, which is supplied by the runner caller.
func (n *Node) prepareEngineRepairCandidate(ctx context.Context, repair fencing.EngineRepairRecord) error {
	if n == nil || ctx == nil || n.config == nil || n.core == nil || n.transport == nil ||
		!n.config.EngineOwnedRemovals || !n.config.EnableReconfiguration || !n.core.IsVoter() {
		return fmt.Errorf("engine repair candidate preparation is unavailable")
	}
	if err := validateEngineRepairCandidateRuntime(repair); err != nil {
		return err
	}
	current := n.core.CurrentCluster()
	abortSlot, _, _ := n.core.LastReconfigurationAbort()
	if n.fenceSession == nil || n.fenceSession.StartupRole() != fencing.StartupRoleVoterActive {
		return fmt.Errorf("engine repair requires an authenticated active survivor")
	}
	if err := validateEngineRepairSourceSnapshot(repair, current, uint64(abortSlot), n.fenceSession.Identity(), n.fenceSession.StartupRole()); err != nil {
		return err
	}
	if pending, exists := n.core.PendingReconfiguration(); exists {
		target, err := engineRepairRemovalTarget(current, repair)
		if err != nil || !sameRepairCluster(pending, target) {
			return fmt.Errorf("engine repair cannot prepare a candidate while another membership transition is pending")
		}
	}

	through := n.core.Tip()
	prefix, ok := n.core.PrefixHash(through)
	if !ok || prefix == (quepaxa.ValueHash{}) {
		return fmt.Errorf("engine repair certified source prefix is unavailable")
	}
	if err := n.transport.VerifyLearner(ctx, repair.CandidateMember, through, prefix); err != nil {
		return fmt.Errorf("engine repair candidate has not caught up to the certified source prefix: %w", err)
	}
	return nil
}

// validateEngineRepairCandidateRuntime keeps CandidatePrepared non-executable
// until the trusted runtime agent has enrolled the current candidate
// incarnation. The original reservation remains immutable in the record.
func validateEngineRepairCandidateRuntime(repair fencing.EngineRepairRecord) error {
	identity := repair.CurrentCandidateIdentity()
	runtime := repair.CurrentCandidateRuntime()
	if runtime == nil || runtime.Validate(identity) != nil ||
		repair.CurrentCandidateRuntimeHash() != fencing.RuntimeBindingDigest(*runtime) {
		return fmt.Errorf("engine repair candidate is not yet enrolled in the trusted runtime")
	}
	return nil
}

// validateEngineRepairSourceSnapshot is the pure adoption/freeze boundary for
// a survivor. A pending request is not authorization by itself: the caller
// must already be the registered active runtime, and the exact original
// member/WAL and abort revision must still match the pinned source snapshot.
// Canonical pending-request lookup and reservation remain authority duties.
func validateEngineRepairSourceSnapshot(repair fencing.EngineRepairRecord, current quepaxa.Cluster, abortSlot uint64,
	caller fencing.Identity, role fencing.StartupRole) error {
	if err := repair.Validate(); err != nil {
		return fmt.Errorf("engine repair reservation is invalid: %w", err)
	}
	if repair.Phase != fencing.RepairCandidatePrepared || repair.RemovalOperation != nil ||
		repair.Binding.FreezeSlot != 0 || repair.Binding.FreezeHash != "" {
		return fmt.Errorf("engine repair candidate is not in its pre-freeze phase")
	}
	if current.ConfigID != uint(repair.Binding.OldConfigID) || !sameRepairCluster(current, repair.SourceConfig) ||
		abortSlot != repair.Binding.ExpectedAbortSlot {
		return fmt.Errorf("engine repair source config or abort revision is no longer current")
	}
	if role != fencing.StartupRoleVoterActive || caller.SourceClusterID != repair.Binding.SourceClusterID ||
		caller.NodeID == "" || caller.NodeID == repair.Binding.OldMemberID || caller.PodUID == "" || caller.RunID == "" || caller.WALIdentity == "" {
		return fmt.Errorf("engine repair caller is not a registered active survivor")
	}
	old, oldFound := current.MemberSet()[quepaxa.NodeID(repair.Binding.OldMemberID)]
	survivor, survivorFound := current.MemberSet()[quepaxa.NodeID(caller.NodeID)]
	if !oldFound || old.WALIdentity != repair.Binding.OldWALIdentity || !survivorFound || survivor.WALIdentity != caller.WALIdentity ||
		repair.CandidateMember.ID == old.ID || repair.CandidateMember.WALIdentity == old.WALIdentity {
		return fmt.Errorf("engine repair old member, survivor, or candidate identity differs from the pinned source")
	}
	if _, exists := current.MemberSet()[repair.CandidateMember.ID]; exists {
		return fmt.Errorf("engine repair candidate is already a source voter")
	}
	return nil
}

// validatePendingEngineRepairSnapshot ties a canonical physical request to
// the exact source-config snapshot selected by a surviving voter. It does not
// create authority: callers must obtain the request from the canonical root
// and pass it to the restricted adoption API for the root CAS.
func validatePendingEngineRepairSnapshot(pending fencing.PendingEngineRepairRequest, repair fencing.EngineRepairRecord,
	current quepaxa.Cluster, abortSlot uint64, caller fencing.Identity, role fencing.StartupRole) error {
	if err := pending.Validate(); err != nil {
		return fmt.Errorf("engine repair adoption differs from its canonical pending request: %w", err)
	}
	if !pending.MatchesRecord(repair) {
		return fmt.Errorf("engine repair adoption differs from its canonical pending request")
	}
	if pending.StatefulSetUID != repair.Binding.StatefulSetUID || pending.OperationID != repair.Binding.OperationID ||
		pending.RequestDigest != repair.BootstrapRequestDigest || pending.BootstrapIdentity.PodName != pending.CandidateIdentity.PodName {
		return fmt.Errorf("engine repair adoption request identity differs from its canonical reservation")
	}
	name := pending.BootstrapIdentity.PodName
	separator := strings.LastIndexByte(name, '-')
	if separator < 1 {
		return fmt.Errorf("engine repair request Pod name has no StatefulSet ordinal")
	}
	ordinal, err := strconv.Atoi(name[separator+1:])
	if err != nil || ordinal != pending.StableOrdinal {
		return fmt.Errorf("engine repair request Pod ordinal differs from its stable slot")
	}
	return validateEngineRepairSourceSnapshot(repair, current, abortSlot, caller, role)
}

// freezeEngineRepairRemoval is the only transition from CandidatePrepared to
// an engine-owned REMOVE. Candidate prefix verification happens before the
// call to BeginEngineOwnedRemovalAt; after the call, the exact decided freeze
// is re-read from the original voter quorum before the caller persists it.
func (n *Node) freezeEngineRepairRemoval(ctx context.Context, repair fencing.EngineRepairRecord) (quepaxa.EngineRemoval, network.EngineRemovalFreezeProof, error) {
	var emptyOperation quepaxa.EngineRemoval
	var emptyProof network.EngineRemovalFreezeProof
	if err := n.prepareEngineRepairCandidate(ctx, repair); err != nil {
		return emptyOperation, emptyProof, err
	}
	target, err := engineRepairRemovalTarget(n.core.CurrentCluster(), repair)
	if err != nil {
		return emptyOperation, emptyProof, err
	}
	operation, err := n.core.BeginEngineOwnedRemovalAt(ctx, target, repair.Binding.OperationID, quepaxa.Slot(repair.Binding.ExpectedAbortSlot))
	if err != nil {
		return emptyOperation, emptyProof, fmt.Errorf("begin exact engine-owned REMOVE: %w", err)
	}
	proof, err := n.EngineRemovalFreezeEvidence(ctx, operation)
	if err != nil {
		return emptyOperation, emptyProof, fmt.Errorf("verify durable original-quorum REMOVE freeze: %w", err)
	}
	return operation, proof, nil
}

// engineRepairRemovalTarget derives only the exact one-member removal target
// bound by the canonical reservation. It never inserts the replacement into
// the REMOVE target and never treats record phase as evidence of a decision.
func engineRepairRemovalTarget(source quepaxa.Cluster, repair fencing.EngineRepairRecord) (quepaxa.Cluster, error) {
	if err := repair.Validate(); err != nil {
		return quepaxa.Cluster{}, fmt.Errorf("engine repair reservation is invalid: %w", err)
	}
	if !sameRepairCluster(source, repair.SourceConfig) || source.ConfigID != uint(repair.Binding.OldConfigID) {
		return quepaxa.Cluster{}, fmt.Errorf("engine repair source configuration differs from the reservation")
	}
	old, found := source.MemberSet()[quepaxa.NodeID(repair.Binding.OldMemberID)]
	if !found || old.WALIdentity != repair.Binding.OldWALIdentity || len(source.Members) < 2 {
		return quepaxa.Cluster{}, fmt.Errorf("engine repair old voter identity is not in the pinned source configuration")
	}
	target := quepaxa.Cluster{ConfigID: source.ConfigID + 1, Members: make([]quepaxa.Member, 0, len(source.Members)-1)}
	for _, member := range source.Members {
		if member.ID != old.ID {
			target.Members = append(target.Members, member)
		}
	}
	if target.MemberSet()[repair.CandidateMember.ID].ID != "" || len(target.Members) != len(source.Members)-1 {
		return quepaxa.Cluster{}, fmt.Errorf("engine repair REMOVE target is not the exact original survivor set")
	}
	return target, nil
}

func sameRepairCluster(left, right quepaxa.Cluster) bool {
	if left.ConfigID != right.ConfigID || len(left.Members) != len(right.Members) {
		return false
	}
	leftMembers, rightMembers := left.MemberSet(), right.MemberSet()
	if len(leftMembers) != len(rightMembers) {
		return false
	}
	for id, member := range leftMembers {
		if rightMembers[id] != member {
			return false
		}
	}
	return true
}

// engineRepairOperationFromFreeze compares a locally retained freeze with the
// exact canonical operation. The proof itself must still be fetched and
// verified through EngineRemovalFreezeEvidence before storage closure.
func engineRepairOperationFromFreeze(repair fencing.EngineRepairRecord, operation quepaxa.EngineRemoval) error {
	if err := repair.Validate(); err != nil {
		return fmt.Errorf("engine repair reservation is invalid: %w", err)
	}
	if repair.RemovalOperation == nil || !fencing.SameEngineRemovalOperation(*repair.RemovalOperation, operation) ||
		repair.Binding.FreezeSlot != uint64(operation.Freeze) || repair.Binding.FreezeHash != fmt.Sprintf("%x", operation.FreezeHash[:]) {
		return network.ErrRequestConflict
	}
	return nil
}
