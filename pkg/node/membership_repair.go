package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"reflect"
	"strings"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
)

const membershipRepairPollInterval = time.Second

// startMembershipRepairWorker gives each open Node one local resume worker.
// The authority's immutable ledger and the existing membership journal make
// retries from other surviving voters safe and idempotent.
func (n *Node) startMembershipRepairWorker(ctx context.Context) {
	if n.fenceSession == nil || !n.config.EnableReconfiguration {
		return
	}
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		ticker := time.NewTicker(membershipRepairPollInterval)
		defer ticker.Stop()
		for {
			if err := n.resumeMembershipRepair(ctx); err != nil && ctx.Err() == nil {
				log.Printf("membership repair remains pending: %v", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (n *Node) resumeMembershipRepair(ctx context.Context) error {
	if n.fenceSession == nil || n.core == nil || !n.ready.Load() || !n.core.IsVoter() {
		return nil
	}
	status, err := n.fenceSession.ReadMembershipRepair(ctx)
	if err != nil {
		return err
	}
	if status.Repair == nil {
		view, err := n.reserveObservedMembershipRepair(ctx, nil)
		if err != nil || view == nil {
			return err
		}
		return n.resumeMembershipRepairView(ctx, *view)
	}
	return n.resumeMembershipRepairView(ctx, *status.Repair)
}

func (n *Node) resumeMembershipRepairView(ctx context.Context, view fencing.MembershipRepairView) error {
	identity := n.fenceSession.Identity()
	if err := view.ValidateFor(identity); err != nil {
		return err
	}
	if len(view.Progress.Candidates) == 0 {
		reserved, err := n.reserveObservedMembershipRepair(ctx, &view)
		if err != nil || reserved == nil {
			return err
		}
		view = *reserved
		if len(view.Progress.Candidates) == 0 {
			return nil
		}
	}
	candidate, err := membershipRepairCandidate(view)
	if err != nil {
		return err
	}
	switch view.Progress.Stage {
	case fencing.MembershipRepairCandidateBound:
		if candidate.Member == nil || identity.NodeID == view.Authorization.OldIdentity.NodeID {
			return nil
		}
		qualification, err := n.fenceSession.QualifyMembershipRepair(ctx, fencing.MembershipRepairQualificationRequest{
			OperationID: view.Authorization.OperationID, CandidatePodUID: candidate.Assignment.PodUID,
		})
		if err != nil {
			return fmt.Errorf("qualify candidate before old-voter closure: %w", err)
		}
		if qualification.Preclosure == nil || qualification.Preclosure.CandidateMember != *candidate.Member || qualification.Preclosure.CandidateBindRef != fencing.BootstrapAssignmentDigest(candidate.Assignment) {
			return fmt.Errorf("candidate preclosure proof differs from retained binding")
		}
		if err := n.requestMembershipRepairClosure(ctx, view); err != nil {
			return err
		}
		prepared, err := n.fenceSession.PrepareMembershipRepairPhase(ctx, fencing.MembershipRepairPhasePrepareRequest{
			OperationID: view.Authorization.OperationID, Kind: fencing.MembershipRepairRemove,
		})
		if err != nil {
			return fmt.Errorf("prepare retained REMOVE phase after closure: %w", err)
		}
		if prepared.Phase.CandidatePodUID != candidate.Assignment.PodUID || prepared.Phase.Validate(view.Authorization, *candidate) != nil {
			return fmt.Errorf("prepared REMOVE phase differs from retained candidate")
		}
		return nil
	case fencing.MembershipRepairOldClosed:
		phase, ok := retainedRepairPhase(view, candidate.Assignment.PodUID, fencing.MembershipRepairRemove)
		if !ok {
			prepared, err := n.fenceSession.PrepareMembershipRepairPhase(ctx, fencing.MembershipRepairPhasePrepareRequest{
				OperationID: view.Authorization.OperationID, Kind: fencing.MembershipRepairRemove,
			})
			if err != nil {
				return fmt.Errorf("prepare retained REMOVE phase: %w", err)
			}
			phase = prepared.Phase
			if phase.CandidatePodUID != candidate.Assignment.PodUID || phase.Validate(view.Authorization, *candidate) != nil {
				return fmt.Errorf("prepared REMOVE phase differs from retained candidate")
			}
		}
		if err := n.changeMembership(ctx, phase.Change); err != nil {
			return err
		}
		return n.completeMembershipRepairPhase(ctx, view, phase)
	case fencing.MembershipRepairRemoved, fencing.MembershipRepairAdded:
		phase, ok := retainedRepairPhase(view, candidate.Assignment.PodUID, fencing.MembershipRepairAdd)
		if !ok {
			prepared, err := n.fenceSession.PrepareMembershipRepairPhase(ctx, fencing.MembershipRepairPhasePrepareRequest{
				OperationID: view.Authorization.OperationID, Kind: fencing.MembershipRepairAdd,
			})
			if err != nil {
				return fmt.Errorf("prepare retained ADD phase: %w", err)
			}
			phase = prepared.Phase
			if phase.CandidatePodUID != candidate.Assignment.PodUID || phase.Validate(view.Authorization, *candidate) != nil {
				return fmt.Errorf("prepared ADD phase differs from retained candidate")
			}
		}
		if err := n.changeMembership(ctx, phase.Change); err != nil {
			return err
		}
		return n.completeMembershipRepairPhase(ctx, view, phase)
	default:
		return nil
	}
}

func (n *Node) completeMembershipRepairPhase(ctx context.Context, view fencing.MembershipRepairView, phase fencing.MembershipRepairPhase) error {
	data, err := readVoterRegistration(ctx, n.bucket, n.membershipOperationKey(phase.Change))
	if err != nil {
		return err
	}
	var operation membershipOperation
	if len(data) == 0 || json.Unmarshal(data, &operation) != nil || !reflect.DeepEqual(operation.Request, phase.Change) || operation.Target.ConfigID != phase.Change.ExpectedConfigID+1 {
		return fmt.Errorf("completed membership phase lacks its exact retained journal")
	}
	terminal, ok, err := membershipTargetTerminal(n.core, operation.Target)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("membership phase target has no completed certified transition")
	}
	requestBytes, err := json.Marshal(operation.Request)
	if err != nil {
		return err
	}
	requestDigest := sha256.Sum256(requestBytes)
	observe := fencing.MembershipRepairPhaseObserveRequest{
		OperationID: view.Authorization.OperationID, PhaseHash: phase.Hash,
		JournalOperationID: operation.Request.OperationID, JournalRequestHash: hex.EncodeToString(requestDigest[:]),
		Target: operation.Target, TerminalSlot: uint64(terminal),
	}
	if _, err := n.fenceSession.ObserveMembershipRepairPhase(ctx, observe); err != nil {
		return fmt.Errorf("observe certified membership phase: %w", err)
	}
	if _, err := n.fenceSession.CompleteMembershipRepairPhase(ctx, observe); err != nil {
		return fmt.Errorf("complete certified membership phase: %w", err)
	}
	return nil
}

func membershipRepairCandidate(view fencing.MembershipRepairView) (*fencing.MembershipRepairCandidate, error) {
	targetPodName := fencing.PodNameForIdentity(view.Authorization.OldIdentity)
	var selected *fencing.MembershipRepairCandidate
	for i := range view.Progress.Candidates {
		candidate := &view.Progress.Candidates[i]
		if candidate.Assignment.PodName != targetPodName {
			continue
		}
		if selected != nil {
			return nil, fmt.Errorf("%w: multiple candidate incarnations for retained slot", network.ErrRequestConflict)
		}
		selected = candidate
	}
	if selected == nil {
		return nil, fmt.Errorf("%w: repair candidate does not match retained slot", network.ErrRequestConflict)
	}
	return selected, nil
}

// reserveObservedMembershipRepair uses only stable Pod-name hints from the
// immutable public bootstrap profile. The authority supplies and rechecks the
// old identity, replacement UID, and exact source round before admission.
func (n *Node) reserveObservedMembershipRepair(ctx context.Context, retained *fencing.MembershipRepairView) (*fencing.MembershipRepairView, error) {
	var targetNames []string
	if retained != nil {
		targetNames = []string{fencing.PodNameForIdentity(retained.Authorization.OldIdentity)}
	} else {
		if n.config == nil {
			return nil, nil
		}
		for _, member := range n.config.Members {
			if member.ID != "" && member.URL != "" && member.PeerURL != "" && string(member.ID) == hostName(member.URL) && string(member.ID) == hostName(member.PeerURL) {
				targetNames = append(targetNames, string(member.ID))
			}
		}
	}
	if len(targetNames) == 0 {
		return nil, nil
	}
	var selected *fencing.MembershipRepairSourceObservation
	for _, targetName := range targetNames {
		status, err := n.fenceSession.ReadMembershipRepairForTarget(ctx, targetName)
		if err != nil {
			if errors.Is(err, fencing.ErrPending) && retained == nil {
				continue
			}
			return nil, err
		}
		if status.Repair != nil {
			if retained != nil && !reflect.DeepEqual(status.Repair.Authorization, retained.Authorization) {
				return nil, network.ErrRequestConflict
			}
			view := *status.Repair
			selected = nil
			if len(view.Progress.Candidates) > 0 || view.Progress.Stage != fencing.MembershipRepairAdmitted {
				return &view, nil
			}
			// The retained admitted request is still the authority for the
			// exact round; this query only prompts its idempotent advancement.
			return n.reserveMembershipRepairView(ctx, view)
		}
		observation := status.Observation
		if observation == nil {
			continue
		}
		if selected != nil {
			return nil, fmt.Errorf("%w: multiple source slots report replacement candidates", network.ErrRequestConflict)
		}
		copy := *observation
		selected = &copy
	}
	if selected == nil {
		return nil, nil
	}
	current := n.core.CurrentCluster()
	if uint64(current.ConfigID) != selected.ConfigID {
		return nil, network.ErrRequestConflict
	}
	membershipBytes, err := json.Marshal(current.Members)
	if err != nil {
		return nil, err
	}
	membershipHash := sha256.Sum256(membershipBytes)
	if hex.EncodeToString(membershipHash[:]) != selected.MembershipHash {
		return nil, network.ErrRequestConflict
	}
	revision, _, aborted := n.core.LastReconfigurationAbort()
	if !aborted {
		revision = 0
	}
	if uint64(revision) != selected.AbortSlot {
		return nil, network.ErrRequestConflict
	}
	abortSlot := selected.AbortSlot
	reservation, err := n.fenceSession.ReserveMembershipRepair(ctx, fencing.MembershipRepairReserveRequest{
		ExpectedConfigID: selected.ConfigID, ExpectedAbortSlot: &abortSlot,
		TargetNodeID: selected.TargetNodeID, TargetWALIdentity: selected.TargetWALIdentity,
	})
	if err != nil {
		return nil, err
	}
	return reservation.Repair, nil
}

func (n *Node) reserveMembershipRepairView(ctx context.Context, view fencing.MembershipRepairView) (*fencing.MembershipRepairView, error) {
	authorization := view.Authorization
	if authorization.ExpectedAbortSlot == nil {
		return nil, network.ErrRequestConflict
	}
	abortSlot := uint64(*authorization.ExpectedAbortSlot)
	request := fencing.MembershipRepairReserveRequest{
		ExpectedConfigID: uint64(authorization.Membership.ConfigID), ExpectedAbortSlot: &abortSlot,
		TargetNodeID: authorization.OldIdentity.NodeID, TargetWALIdentity: authorization.OldIdentity.WALIdentity,
	}
	reserved, err := n.fenceSession.ReserveMembershipRepair(ctx, request)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(reserved.Repair.Authorization, authorization) {
		return nil, network.ErrRequestConflict
	}
	return reserved.Repair, nil
}

func hostName(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" {
		return ""
	}
	return strings.SplitN(u.Hostname(), ".", 2)[0]
}

func retainedRepairPhase(view fencing.MembershipRepairView, candidatePodUID string, kind fencing.MembershipRepairPhaseKind) (fencing.MembershipRepairPhase, bool) {
	var selected fencing.MembershipRepairPhase
	found := false
	for _, phase := range view.Progress.Phases {
		if phase.CandidatePodUID != candidatePodUID || phase.Kind != kind {
			continue
		}
		if found {
			return fencing.MembershipRepairPhase{}, false
		}
		selected, found = phase, true
	}
	return selected, found
}

// authorizeMembershipRepairMutation is called before either membership
// journal is read or written. Once an active repair exists, only its exact
// retained phase may use the existing consensus mutation lifecycle.
func (n *Node) authorizeMembershipRepairMutation(ctx context.Context, request network.MembershipChange) error {
	if n.fenceSession == nil {
		return nil
	}
	status, err := n.fenceSession.ReadMembershipRepair(ctx)
	if err != nil {
		return err
	}
	if status.Repair == nil {
		return nil
	}
	view := *status.Repair
	if err := view.ValidateFor(n.fenceSession.Identity()); err != nil {
		return err
	}
	var matched *fencing.MembershipRepairPhase
	for i := range view.Progress.Phases {
		phase := &view.Progress.Phases[i]
		if !reflect.DeepEqual(phase.Change, request) {
			continue
		}
		if matched != nil {
			return fmt.Errorf("%w: ambiguous retained repair phase", network.ErrRequestConflict)
		}
		matched = phase
	}
	if matched == nil {
		return network.ErrRequestConflict
	}
	return n.fenceSession.AuthorizeMembershipPhase(ctx, *matched)
}

func (n *Node) requestMembershipRepairClosure(ctx context.Context, view fencing.MembershipRepairView) error {
	n.membershipMu.Lock()
	defer n.membershipMu.Unlock()
	current := n.core.CurrentCluster()
	if !reflect.DeepEqual(current, view.Authorization.Membership) {
		return network.ErrRequestConflict
	}
	if _, pending := n.core.PendingReconfiguration(); pending {
		return network.ErrRequestConflict
	}
	revision, _, aborted := n.core.LastReconfigurationAbort()
	if !aborted {
		revision = 0
	}
	abortSlot := uint64(revision)
	request := fencing.PeerClosureRequest{
		ExpectedConfigID:  uint64(view.Authorization.Membership.ConfigID),
		ExpectedAbortSlot: &abortSlot,
		TargetNodeID:      view.Authorization.OldIdentity.NodeID,
		TargetWALIdentity: view.Authorization.OldIdentity.WALIdentity,
	}
	_, err := n.fenceSession.ReadPeerClosure(ctx, request)
	if err == fencing.ErrNotFound {
		_, err = n.fenceSession.RequestPeerClosure(ctx, request)
	}
	return err
}
