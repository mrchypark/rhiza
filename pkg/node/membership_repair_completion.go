package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func (n *Node) completeMembershipRepairTerminal(ctx context.Context, request network.MembershipRepairCompletionRequest) (network.MembershipRepairCompletionResponse, error) {
	var response network.MembershipRepairCompletionResponse
	if err := network.ValidateMembershipRepairCompletionRequest(request); err != nil {
		return response, err
	}
	if n == nil || n.config == nil || n.core == nil || n.archive == nil || n.bucket == nil || n.fenceSession == nil || !n.ready.Load() || !n.core.IsVoter() {
		return response, fmt.Errorf("membership repair completion requires a ready supervised voter")
	}
	n.membershipMu.Lock()
	defer n.membershipMu.Unlock()
	identity := n.fenceSession.Identity()
	if identity.Validate() != nil || identity.SourceClusterID != request.SourceClusterID || identity.NodeID != request.ExecutorNodeID ||
		identity.PodUID != request.ExecutorPodUID || identity.RunID != request.ExecutorRunID || identity.WALIdentity != request.ExecutorWALIdentity ||
		identity.WALIdentity == "" || string(n.config.ClusterID) != request.SourceClusterID || string(n.config.NodeID) != identity.NodeID {
		return response, fmt.Errorf("membership repair completion executor differs from the bound voter runtime")
	}
	operation, source, terminal, err := n.validateMembershipRepairCompletionJournal(ctx, request)
	if err != nil {
		return response, err
	}
	if len(source.Members) < 2 {
		return response, fmt.Errorf("membership repair ADD source has fewer than two voters")
	}
	current := n.core.CurrentCluster()
	if !membershipHasSameVoterKey(source, current, quepaxa.NodeID(identity.NodeID), identity.WALIdentity) {
		return response, fmt.Errorf("membership repair executor is not a current pre-ADD survivor with the same key and WAL")
	}

	// CompleteDecision is intentionally repeated even when this Node's local
	// membership journal says the ADD is done. Only its successful return proves
	// the historical terminal was dispatched and the added member ACKed it.
	completed, err := n.core.CompleteDecision(ctx, quepaxa.Slot(request.TerminalSlot))
	if err != nil {
		return response, fmt.Errorf("complete exact membership ADD terminal: %w", err)
	}
	if completed.Slot != quepaxa.Slot(request.TerminalSlot) || hex.EncodeToString(completed.Hash[:]) != request.TerminalHash ||
		!reflect.DeepEqual(completed, terminal) {
		return response, fmt.Errorf("completed membership terminal differs from challenged certificate")
	}
	if err := n.core.EnsureDurableThrough(ctx, quepaxa.Slot(request.TerminalSlot)); err != nil {
		return response, fmt.Errorf("persist completed membership ADD terminal: %w", err)
	}
	if err := n.archive.SyncThrough(ctx, n.core, quepaxa.Slot(request.TerminalSlot)); err != nil {
		return response, fmt.Errorf("archive completed membership ADD terminal: %w", err)
	}
	if err := n.verifyMembershipRepairArchiveReadback(ctx, quepaxa.Slot(request.TerminalSlot), terminal); err != nil {
		return response, err
	}
	if err := n.revalidateMembershipRepairCompletionState(ctx, request, operation, source); err != nil {
		return response, err
	}
	digest, err := network.MembershipRepairCompletionRequestDigest(request)
	if err != nil {
		return response, err
	}
	response = network.MembershipRepairCompletionResponse{
		Version: request.Version, Action: request.Action, Challenge: request.Challenge, AttemptRevision: request.AttemptRevision,
		ScopeDigest: request.ScopeDigest, RequestDigest: digest,
		ExecutorNodeID: identity.NodeID, ExecutorPodUID: identity.PodUID, ExecutorRunID: identity.RunID,
		ExecutorWALIdentity: identity.WALIdentity, TerminalSlot: request.TerminalSlot, TerminalHash: request.TerminalHash, Outcome: "completed",
	}
	return response, nil
}

func (n *Node) validateMembershipRepairCompletionJournal(ctx context.Context, request network.MembershipRepairCompletionRequest) (membershipOperation, quepaxa.Cluster, quepaxa.DecidedValue, error) {
	var operation membershipOperation
	if request.SourceClusterID != string(n.config.ClusterID) {
		return operation, quepaxa.Cluster{}, quepaxa.DecidedValue{}, network.ErrRequestConflict
	}
	change := network.MembershipChange{
		ClusterID: request.SourceClusterID, ExpectedConfigID: request.SourceConfigID,
		ExpectedAbortSlot: *request.AbortSlot,
	}
	data, err := readVoterRegistration(ctx, n.bucket, n.membershipOperationKey(change))
	if err != nil {
		return operation, quepaxa.Cluster{}, quepaxa.DecidedValue{}, err
	}
	if len(data) == 0 || json.Unmarshal(data, &operation) != nil || operation.Request.OperationID != request.JournalOperationID ||
		operation.Request.ClusterID != request.SourceClusterID || operation.Request.ExpectedConfigID != request.SourceConfigID ||
		operation.Request.ExpectedAbortSlot != *request.AbortSlot || operation.Request.Remove != "" || operation.Request.Add == nil ||
		operation.Request.Fence != nil || *operation.Request.Add != request.CandidateMember || !reflect.DeepEqual(operation.Target, request.Target) {
		return operation, quepaxa.Cluster{}, quepaxa.DecidedValue{}, fmt.Errorf("membership repair completion journal differs from exact challenged ADD")
	}
	encodedRequest, err := json.Marshal(operation.Request)
	if err != nil {
		return operation, quepaxa.Cluster{}, quepaxa.DecidedValue{}, err
	}
	requestHash := sha256.Sum256(encodedRequest)
	if hex.EncodeToString(requestHash[:]) != request.JournalRequestHash {
		return operation, quepaxa.Cluster{}, quepaxa.DecidedValue{}, fmt.Errorf("membership repair completion journal request hash differs")
	}
	phase := fencing.MembershipRepairPhase{
		Kind: fencing.MembershipRepairAdd, CandidatePodUID: request.CandidatePodUID,
		Change: operation.Request, AbortSlot: request.AbortSlot,
	}
	phaseHash, err := fencing.MembershipRepairPhaseHash(phase)
	if err != nil || phaseHash != request.PhaseHash {
		return operation, quepaxa.Cluster{}, quepaxa.DecidedValue{}, fmt.Errorf("membership repair completion phase hash differs from exact journal")
	}
	history, err := n.core.MembershipHistory()
	if err != nil {
		return operation, quepaxa.Cluster{}, quepaxa.DecidedValue{}, fmt.Errorf("read certified membership history: %w", err)
	}
	var terminal *quepaxa.DecidedValue
	for i := range history.Transitions {
		transition := &history.Transitions[i]
		if uint64(transition.Freeze.Slot) != request.FreezeSlot || hex.EncodeToString(transition.Freeze.Hash[:]) != request.FreezeHash ||
			uint64(transition.Terminal.Slot) != request.TerminalSlot || hex.EncodeToString(transition.Terminal.Hash[:]) != request.TerminalHash {
			continue
		}
		source := n.core.ClusterForSlot(transition.Terminal.Slot)
		target := n.core.ClusterForSlot(transition.Terminal.Slot + 1)
		if source.ConfigID != request.SourceConfigID || !reflect.DeepEqual(target, request.Target) {
			return operation, quepaxa.Cluster{}, quepaxa.DecidedValue{}, fmt.Errorf("certified ADD transition differs from challenged source or target")
		}
		copy := transition.Terminal
		terminal = &copy
	}
	if terminal == nil {
		return operation, quepaxa.Cluster{}, quepaxa.DecidedValue{}, fmt.Errorf("challenged ADD freeze and terminal are absent from certified membership history")
	}
	source := n.core.ClusterForSlot(terminal.Slot)
	if source.ConfigID != request.SourceConfigID || len(source.Members) < 2 || clusterHasMember(source, request.CandidateMember.ID) || !membershipTargetHasExactMember(request.Target, request.CandidateMember) {
		return operation, quepaxa.Cluster{}, quepaxa.DecidedValue{}, fmt.Errorf("challenged ADD source geometry or target is invalid")
	}
	return operation, source, *terminal, nil
}

func membershipTargetHasExactMember(target quepaxa.Cluster, expected quepaxa.Member) bool {
	for _, member := range target.Members {
		if member.ID == expected.ID {
			return member == expected
		}
	}
	return false
}

func (n *Node) revalidateMembershipRepairCompletionState(ctx context.Context, request network.MembershipRepairCompletionRequest, expectedOperation membershipOperation, source quepaxa.Cluster) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	identity := n.fenceSession.Identity()
	current := n.core.CurrentCluster()
	if !n.ready.Load() || !n.core.IsVoter() || identity.Validate() != nil || identity.NodeID != request.ExecutorNodeID || identity.PodUID != request.ExecutorPodUID ||
		identity.RunID != request.ExecutorRunID || identity.WALIdentity != request.ExecutorWALIdentity || !membershipHasSameVoterKey(source, current, quepaxa.NodeID(identity.NodeID), identity.WALIdentity) {
		return fmt.Errorf("membership repair completion executor binding changed during terminal delivery")
	}
	operation, gotSource, _, err := n.validateMembershipRepairCompletionJournal(ctx, request)
	if err != nil || !reflect.DeepEqual(operation, expectedOperation) || !reflect.DeepEqual(gotSource, source) {
		if err == nil {
			err = fmt.Errorf("membership repair journal or source changed during terminal delivery")
		}
		return err
	}
	return nil
}

func membershipHasSameVoterKey(source, current quepaxa.Cluster, id quepaxa.NodeID, wal string) bool {
	var sourceMember, currentMember *quepaxa.Member
	for i := range source.Members {
		if source.Members[i].ID == id {
			sourceMember = &source.Members[i]
			break
		}
	}
	for i := range current.Members {
		if current.Members[i].ID == id {
			currentMember = &current.Members[i]
			break
		}
	}
	return sourceMember != nil && currentMember != nil && sourceMember.PublicKey == currentMember.PublicKey && sourceMember.WALIdentity == wal && currentMember.WALIdentity == wal
}
