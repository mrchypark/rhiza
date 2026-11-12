package managedsql

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"slices"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type repairResumeStage string

const (
	repairResumeRegistration repairResumeStage = "registration"
	repairResumeMembership   repairResumeStage = "membership_add"
	repairResumeVerification repairResumeStage = "verification"
	repairResumeCompletion   repairResumeStage = "completion"
)

func managedRepairCauseClass(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, network.ErrNotReady):
		return "not_ready"
	case errors.Is(err, network.ErrInvalidRequest):
		return "invalid_request"
	case errors.Is(err, network.ErrRequestConflict):
		return "request_conflict"
	case errors.Is(err, quepaxa.ErrQuorumUnavailable):
		return "quorum_unavailable"
	default:
		return "other"
	}
}

func repairResumeFailure(stage repairResumeStage, err error) error {
	if err == nil {
		return nil
	}
	class := managedRepairCauseClass(err)
	log.Printf("MANAGED_REPAIR_RESUME_FAILURE stage=%s cause=%s", stage, class)
	// Keep the original public error value, message, and errors.Is behavior.
	return err
}

// ReplaceVoter resumes only the exact closed replacement intent supplied by
// the caller. It never proves physical death, executes pending SQL, or creates
// a replacement identity; the Pod identity is validated by the status API.
func (h *Host) ReplaceVoter(ctx context.Context, plan operator.V1Alpha2RepairPlan) error {
	h.mu.Lock()
	closed := h.closed
	h.mu.Unlock()
	if closed {
		return fmt.Errorf("SQL host closed")
	}
	if err := validateRepairSequenceProfile(plan.Sequence, h.cfg.Plan.ApplicationProtocol); err != nil {
		return err
	}
	if err := h.validateRepairPlanRuntime(ctx, plan); err != nil {
		return err
	}
	a, err := h.live(ctx)
	if err != nil {
		return err
	}
	if plan.Sequence == operator.V1Alpha2RepairAdd {
		return h.enrollVoter(ctx, a, plan)
	}
	if plan.Sequence == operator.V1Alpha2RepairLeave {
		return fmt.Errorf("managed leave must use the explicit leave coordinator")
	}
	return h.replaceVoterAddFirst(ctx, a, plan)
}

func validateRepairSequenceProfile(sequence operator.V1Alpha2RepairSequence, applicationProtocol string) error {
	if !sqlpolicy.IsManagedAppendOnlyApplicationProtocol(applicationProtocol) {
		return fmt.Errorf("managed append-only application protocol required; profiles 7/8 are retired")
	}
	if sequence != operator.V1Alpha2RepairAddThenRemove && sequence != operator.V1Alpha2RepairAdd && sequence != operator.V1Alpha2RepairLeave {
		return fmt.Errorf("append-only replacement requires ADD-first")
	}
	return nil
}

func (h *Host) replaceVoterAddFirst(ctx context.Context, a operator.V1Alpha2AdmissionSnapshot, plan operator.V1Alpha2RepairPlan) error {
	existing, exists := a.Repair()
	if exists && !reflect.DeepEqual(existing.Plan, plan) && existing.State == "Closed" {
		return fmt.Errorf("another exact repair is already closed")
	}
	if exists && reflect.DeepEqual(existing.Plan, plan) && existing.State == "Completed" {
		return h.verifyCompletedAddFirstRepair(ctx, existing)
	}
	if exists && reflect.DeepEqual(existing.Plan, plan) && existing.State != "Closed" {
		return fmt.Errorf("ADD-first repair is not resumable")
	}
	if !a.SQLReservationsClosed() {
		if err := h.validateRepairStartState(ctx, plan); err != nil {
			return err
		}
		closed, observation, err := operator.BeginV1Alpha2Repair(ctx, h.cfg.Store, a, a.Authority(), plan)
		if err != nil {
			return err
		}
		if observation != operator.V1Alpha2CASApplied || !closed.SQLReservationsClosed() {
			return fmt.Errorf("repair close CAS outcome %s; refusing to claim ownership", observation)
		}
		a = closed
	}
	r, ok := a.Repair()
	if !ok || r.State != "Closed" || !reflect.DeepEqual(r.Plan, plan) {
		return fmt.Errorf("exact closed ADD-first repair required")
	}
	if r.PreparedHandoff == nil {
		if _, err := h.CaptureRepairHandoff(ctx, plan.OperationID); err != nil {
			return repairResumeFailure(repairResumeVerification, fmt.Errorf("capture fixed ADD-first H: %w", err))
		}
		latest, err := h.live(ctx)
		if err != nil {
			return err
		}
		a = latest
		r, ok = a.Repair()
		if !ok || r.State != "Closed" || !reflect.DeepEqual(r.Plan, plan) || r.PreparedHandoff == nil {
			return fmt.Errorf("ADD-first H was not retained in the exact repair")
		}
	}
	handoff := *r.PreparedHandoff
	if handoff.PreparedConfig == nil || uint64(handoff.PreparedConfig.ConfigID) != plan.ExpectedConfigID {
		return fmt.Errorf("exact ADD-first prepared source required")
	}
	registration, err := h.node.ManagedRepairSnapshot(ctx, plan.Replacement.NodeID, plan.Replacement.PublicKey)
	if err != nil {
		return err
	}
	if registration.LearnerWALIdentity == "" {
		registration.LearnerWALIdentity, err = h.waitRegisteredRepairLearner(ctx, plan.Replacement.NodeID, plan.Replacement.PublicKey)
		if err != nil {
			return repairResumeFailure(repairResumeRegistration, err)
		}
	}
	addRequest, err := managedRepairAddRequest(r, &handoff, registration.LearnerWALIdentity)
	if err != nil {
		return err
	}
	if r.AddCompletion == nil {
		// If a prior attempt committed ADD but failed before its status CAS, the
		// live configuration is already C+1. Verify that terminal instead of
		// issuing a second membership operation.
		switch registration.Current.ConfigID {
		case handoff.PreparedConfig.ConfigID:
			if err := h.AddRepairVoter(ctx, plan.OperationID, registration.LearnerWALIdentity); err != nil {
				return repairResumeFailure(repairResumeMembership, fmt.Errorf("add registered learner: %w", err))
			}
		case handoff.PreparedConfig.ConfigID + 1, handoff.PreparedConfig.ConfigID + 2:
		default:
			return fmt.Errorf("ADD-first membership advanced to an unexpected configuration")
		}
	}
	addProof, err := h.node.VerifyManagedRepairAddition(ctx, addRequest)
	if err != nil {
		return repairResumeFailure(repairResumeVerification, err)
	}
	addCompletion, err := managedRepairAdditionCompletion(plan, handoff, addProof)
	if err != nil {
		return repairResumeFailure(repairResumeVerification, err)
	}
	if r.AddCompletion == nil {
		latest, err := h.live(ctx)
		if err != nil {
			return repairResumeFailure(repairResumeCompletion, err)
		}
		latestRepair, ok := latest.Repair()
		if !ok || latestRepair.State != "Closed" || !latest.SQLReservationsClosed() || !reflect.DeepEqual(latestRepair.Plan, plan) || !reflect.DeepEqual(latestRepair.PreparedHandoff, &handoff) || latestRepair.AddCompletion != nil {
			return fmt.Errorf("ADD-first intent changed before ADD proof CAS")
		}
		added, observation, err := operator.RecordV1Alpha2RepairAddition(ctx, h.cfg.Store, latest, latest.Authority(), addCompletion)
		if err != nil {
			return repairResumeFailure(repairResumeCompletion, err)
		}
		if observation != operator.V1Alpha2CASApplied {
			return fmt.Errorf("ADD terminal CAS outcome %s; refusing to continue", observation)
		}
		r, ok = added.Repair()
		if !ok || r.AddCompletion == nil || !reflect.DeepEqual(*r.AddCompletion, addCompletion) {
			return fmt.Errorf("ADD terminal proof readback mismatch")
		}
	} else if !reflect.DeepEqual(*r.AddCompletion, addCompletion) {
		return fmt.Errorf("retained ADD terminal differs from actual Core proof")
	}
	registration, err = h.node.ManagedRepairSnapshot(ctx, plan.Replacement.NodeID, plan.Replacement.PublicKey)
	if err != nil {
		return repairResumeFailure(repairResumeVerification, err)
	}
	if registration.LearnerWALIdentity != addRequest.Addition.WALIdentity {
		return fmt.Errorf("replacement registration changed after verified ADD terminal")
	}
	if registration.Current.ConfigID == handoff.PreparedConfig.ConfigID+1 {
		if err := h.RemoveRepairVoter(ctx, plan.OperationID); err != nil {
			return repairResumeFailure(repairResumeMembership, fmt.Errorf("remove old voter after recorded ADD: %w", err))
		}
	} else if registration.Current.ConfigID != handoff.PreparedConfig.ConfigID+2 {
		return fmt.Errorf("ADD-first repair did not reach the exact post-ADD or post-REMOVE configuration")
	}
	removeRequest := node.ManagedMembershipRequest{RepairOperationID: plan.OperationID, Action: node.ManagedMembershipRemove, ExpectedConfigID: uint(addCompletion.ConfigID), ExpectedAbortSlot: quepaxa.Slot(plan.ExpectedAbortSlot), OldNodeID: plan.OldNodeID, OldWALIdentity: plan.OldWALIdentity}
	removeProof, err := h.node.CaptureManagedRepairSource(ctx, removeRequest)
	if err != nil {
		return repairResumeFailure(repairResumeVerification, fmt.Errorf("verify actual REMOVE terminal: %w", err))
	}
	finalProof := node.ManagedRepairCompletion{Current: removeProof.Survivors, Terminal: removeProof.RemoveTerminal}
	completion, err := repairCompletion(plan, finalProof)
	if err != nil {
		return repairResumeFailure(repairResumeVerification, err)
	}
	if completion.Slot <= addCompletion.Slot {
		return fmt.Errorf("verified REMOVE terminal does not follow the recorded ADD terminal")
	}
	latest, err := h.live(ctx)
	if err != nil {
		return repairResumeFailure(repairResumeCompletion, err)
	}
	latestRepair, ok := latest.Repair()
	if !ok || latestRepair.State != "Closed" || !latest.SQLReservationsClosed() || !reflect.DeepEqual(latestRepair.Plan, plan) || !reflect.DeepEqual(latestRepair.PreparedHandoff, &handoff) || latestRepair.AddCompletion == nil || !reflect.DeepEqual(*latestRepair.AddCompletion, addCompletion) {
		return fmt.Errorf("exact ADD-first proof changed before REMOVE completion")
	}
	finished, observation, err := operator.FinishV1Alpha2Repair(ctx, h.cfg.Store, latest, latest.Authority(), completion)
	if err != nil {
		return repairResumeFailure(repairResumeCompletion, err)
	}
	if observation != operator.V1Alpha2CASApplied {
		return fmt.Errorf("repair completion CAS outcome %s; refusing to report completion", observation)
	}
	finishedRepair, ok := finished.Repair()
	if !ok || finishedRepair.State != "Completed" || finishedRepair.AddCompletion == nil || !reflect.DeepEqual(*finishedRepair.AddCompletion, addCompletion) || finishedRepair.Completion == nil || !reflect.DeepEqual(*finishedRepair.Completion, completion) {
		return fmt.Errorf("ADD-first repair completion readback mismatch")
	}
	return nil
}

// enrollVoter adds exactly one learner and keeps every current voter. The
// admitted learner proof is verified by QuePaxa before AddRepairVoter can
// commit the new configuration; status completion requires the retained
// terminal and target configuration as well.
func (h *Host) enrollVoter(ctx context.Context, a operator.V1Alpha2AdmissionSnapshot, plan operator.V1Alpha2RepairPlan) error {
	existing, exists := a.Repair()
	if exists && !reflect.DeepEqual(existing.Plan, plan) && existing.State == "Closed" {
		return fmt.Errorf("another exact membership operation is already closed")
	}
	if exists && reflect.DeepEqual(existing.Plan, plan) && existing.State == "Completed" {
		return h.verifyCompletedEnrollment(ctx, existing)
	}
	if exists && reflect.DeepEqual(existing.Plan, plan) && existing.State != "Closed" {
		return fmt.Errorf("add-only enrollment is not resumable")
	}
	if !a.SQLReservationsClosed() {
		if err := h.validateRepairStartState(ctx, plan); err != nil {
			return err
		}
		closed, observation, err := operator.BeginV1Alpha2Repair(ctx, h.cfg.Store, a, a.Authority(), plan)
		if err != nil {
			return err
		}
		if observation != operator.V1Alpha2CASApplied || !closed.SQLReservationsClosed() {
			return fmt.Errorf("enrollment close CAS outcome %s; refusing to claim ownership", observation)
		}
		a = closed
	}
	r, ok := a.Repair()
	if !ok || r.State != "Closed" || !reflect.DeepEqual(r.Plan, plan) {
		return fmt.Errorf("exact closed add-only enrollment required")
	}
	if r.PreparedHandoff == nil {
		if _, err := h.CaptureRepairHandoff(ctx, plan.OperationID); err != nil {
			return repairResumeFailure(repairResumeVerification, fmt.Errorf("capture fixed enrollment source: %w", err))
		}
		latest, err := h.live(ctx)
		if err != nil {
			return err
		}
		a = latest
		r, ok = a.Repair()
		if !ok || r.State != "Closed" || !reflect.DeepEqual(r.Plan, plan) || r.PreparedHandoff == nil {
			return fmt.Errorf("enrollment source was not retained in the exact operation")
		}
	}
	handoff := *r.PreparedHandoff
	if handoff.PreparedConfig == nil || uint64(handoff.PreparedConfig.ConfigID) != plan.ExpectedConfigID {
		return fmt.Errorf("exact enrollment prepared source required")
	}
	registration, err := h.node.ManagedRepairSnapshot(ctx, plan.Replacement.NodeID, plan.Replacement.PublicKey)
	if err != nil {
		return err
	}
	if registration.LearnerWALIdentity == "" {
		registration.LearnerWALIdentity, err = h.waitRegisteredRepairLearner(ctx, plan.Replacement.NodeID, plan.Replacement.PublicKey)
		if err != nil {
			return repairResumeFailure(repairResumeRegistration, err)
		}
	}
	request, err := managedRepairAddRequest(r, &handoff, registration.LearnerWALIdentity)
	if err != nil {
		return err
	}
	if r.Completion == nil {
		switch registration.Current.ConfigID {
		case handoff.PreparedConfig.ConfigID:
			if err := h.AddRepairVoter(ctx, plan.OperationID, registration.LearnerWALIdentity); err != nil {
				return repairResumeFailure(repairResumeMembership, fmt.Errorf("add enrolled learner: %w", err))
			}
		case handoff.PreparedConfig.ConfigID + 1:
		default:
			return fmt.Errorf("add-only enrollment advanced to an unexpected configuration")
		}
		proof, err := h.node.VerifyManagedRepairCompletion(ctx, request)
		if err != nil {
			return repairResumeFailure(repairResumeVerification, err)
		}
		completion, err := managedRepairAdditionCompletion(plan, handoff, proof)
		if err != nil {
			return repairResumeFailure(repairResumeVerification, err)
		}
		latest, err := h.live(ctx)
		if err != nil {
			return repairResumeFailure(repairResumeCompletion, err)
		}
		latestRepair, ok := latest.Repair()
		if !ok || latestRepair.State != "Closed" || latestRepair.Completion != nil || !reflect.DeepEqual(latestRepair.Plan, plan) || !reflect.DeepEqual(latestRepair.PreparedHandoff, &handoff) {
			return fmt.Errorf("enrollment intent changed before completion CAS")
		}
		finished, observation, err := operator.FinishV1Alpha2Repair(ctx, h.cfg.Store, latest, latest.Authority(), completion)
		if err != nil {
			return repairResumeFailure(repairResumeCompletion, err)
		}
		if observation != operator.V1Alpha2CASApplied {
			return fmt.Errorf("enrollment completion CAS outcome %s; refusing to report completion", observation)
		}
		r, ok = finished.Repair()
		if !ok || r.State != "Completed" || r.Completion == nil || !reflect.DeepEqual(*r.Completion, completion) {
			return fmt.Errorf("enrollment completion readback mismatch")
		}
		return nil
	}
	proof, err := h.node.VerifyManagedRepairCompletion(ctx, request)
	if err != nil {
		return repairResumeFailure(repairResumeVerification, err)
	}
	completion, err := managedRepairAdditionCompletion(plan, handoff, proof)
	if err != nil || !reflect.DeepEqual(completion, *r.Completion) {
		return errors.Join(fmt.Errorf("enrollment completion differs from actual retained terminal"), err)
	}
	return nil
}

func (h *Host) verifyCompletedEnrollment(ctx context.Context, r operator.V1Alpha2RepairRecord) error {
	if r.Plan.Sequence != operator.V1Alpha2RepairAdd || r.PreparedHandoff == nil || r.Completion == nil || r.Completion.Kind != "Terminal" {
		return fmt.Errorf("completed enrollment lacks its prepared source or terminal")
	}
	snapshot, err := h.node.ManagedRepairSnapshot(ctx, r.Plan.Replacement.NodeID, r.Plan.Replacement.PublicKey)
	if err != nil || snapshot.LearnerWALIdentity == "" {
		return errors.Join(fmt.Errorf("completed enrollment registration unavailable"), err)
	}
	request, err := managedRepairAddRequest(r, r.PreparedHandoff, snapshot.LearnerWALIdentity)
	if err != nil {
		return err
	}
	proof, err := h.node.VerifyManagedRepairCompletion(ctx, request)
	if err != nil {
		return err
	}
	completion, err := managedRepairAdditionCompletion(r.Plan, *r.PreparedHandoff, proof)
	if err != nil || !reflect.DeepEqual(completion, *r.Completion) {
		return errors.Join(fmt.Errorf("completed enrollment differs from actual retained terminal"), err)
	}
	return nil
}

func (h *Host) verifyCompletedAddFirstRepair(ctx context.Context, r operator.V1Alpha2RepairRecord) error {
	if r.Plan.Sequence != operator.V1Alpha2RepairAddThenRemove || r.PreparedHandoff == nil || r.AddCompletion == nil || r.Completion == nil || r.Completion.Kind != "Terminal" {
		return fmt.Errorf("completed ADD-first repair lacks exact ADD/REMOVE proofs")
	}
	snapshot, err := h.node.ManagedRepairSnapshot(ctx, r.Plan.Replacement.NodeID, r.Plan.Replacement.PublicKey)
	if err != nil || snapshot.LearnerWALIdentity == "" {
		return errors.Join(fmt.Errorf("completed replacement registration unavailable"), err)
	}
	request, err := managedRepairAddRequest(r, r.PreparedHandoff, snapshot.LearnerWALIdentity)
	if err != nil {
		return err
	}
	addProof, err := h.node.VerifyManagedRepairAddition(ctx, request)
	if err != nil {
		return err
	}
	addition, err := managedRepairAdditionCompletion(r.Plan, *r.PreparedHandoff, addProof)
	if err != nil || !reflect.DeepEqual(addition, *r.AddCompletion) {
		return errors.Join(fmt.Errorf("completed ADD proof differs from actual retained terminal"), err)
	}
	removeRequest := node.ManagedMembershipRequest{RepairOperationID: r.Plan.OperationID, Action: node.ManagedMembershipRemove, ExpectedConfigID: uint(r.AddCompletion.ConfigID), ExpectedAbortSlot: quepaxa.Slot(r.Plan.ExpectedAbortSlot), OldNodeID: r.Plan.OldNodeID, OldWALIdentity: r.Plan.OldWALIdentity}
	removeProof, err := h.node.VerifyManagedRepairRemoval(ctx, removeRequest)
	if err != nil {
		return err
	}
	completion, err := repairCompletion(r.Plan, node.ManagedRepairCompletion{Current: removeProof.Survivors, Terminal: removeProof.RemoveTerminal})
	if err != nil || completion.Slot <= r.AddCompletion.Slot || !reflect.DeepEqual(completion, *r.Completion) {
		return errors.Join(fmt.Errorf("completed REMOVE proof differs from actual retained terminal"), err)
	}
	return nil
}

func (h *Host) validateRepairPlanRuntime(ctx context.Context, plan operator.V1Alpha2RepairPlan) error {
	if plan.OperationID == "" || plan.ClusterUID != h.cfg.Ref.UID || plan.Generation != h.cfg.Plan.Generation || plan.DeploymentUID != h.cfg.Plan.DeploymentUID || plan.StorageID != h.cfg.Plan.StorageID || plan.BootstrapPlanDigest != h.cfg.Plan.BootstrapPlanDigest {
		return fmt.Errorf("repair plan differs from managed host binding")
	}
	snapshot, err := h.node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil {
		return err
	}
	if snapshot.LocalWALIdentity == "" {
		return fmt.Errorf("managed voter WAL identity unavailable")
	}
	return nil
}

func (h *Host) validateRepairStartState(ctx context.Context, plan operator.V1Alpha2RepairPlan) error {
	snapshot, err := h.node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil {
		return err
	}
	if uint64(snapshot.Current.ConfigID) != plan.ExpectedConfigID || uint64(snapshot.AbortSlot) != plan.ExpectedAbortSlot {
		return fmt.Errorf("repair plan no longer matches current membership revision")
	}
	oldFound := false
	var oldMember quepaxa.Member
	ids := make([]types.NodeID, 0, len(snapshot.Current.Members))
	for _, member := range snapshot.Current.Members {
		if member.ID == plan.OldNodeID {
			oldFound = true
			oldMember = member
			if plan.Sequence != operator.V1Alpha2RepairAdd {
				continue
			}
		}
		ids = append(ids, types.NodeID(member.ID))
	}
	if !oldFound || plan.OldWALIdentity == "" {
		return fmt.Errorf("old voter or its expected WAL identity is missing from repair intent")
	}
	registered, err := h.node.ManagedVoterWALIdentity(ctx, snapshot.Current.ConfigID, snapshot.AbortSlot, oldMember)
	if err != nil || registered != plan.OldWALIdentity {
		return errors.Join(fmt.Errorf("repair intent does not bind the exact current old-voter WAL identity"), err)
	}
	ids = append(ids, plan.Replacement.NodeID)
	slices.Sort(ids)
	if !slices.Equal(ids, plan.FinalVoterIDs) {
		return fmt.Errorf("repair target differs from actual current membership")
	}
	return nil
}

func (h *Host) waitRegisteredRepairLearner(ctx context.Context, learnerID quepaxa.NodeID, key quepaxa.PublicKey) (string, error) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		snapshot, err := h.node.ManagedRepairSnapshot(ctx, learnerID, key)
		if err != nil {
			return "", fmt.Errorf("read actual learner registration: %w", err)
		}
		if snapshot.LearnerWALIdentity != "" {
			return snapshot.LearnerWALIdentity, nil
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
		}
	}
}

func managedRepairAddRequest(record operator.V1Alpha2RepairRecord, handoff *operator.V1Alpha2RepairHandoff, identity string) (node.ManagedMembershipRequest, error) {
	if handoff == nil || identity == "" || record.Plan.OperationID == "" {
		return node.ManagedMembershipRequest{}, fmt.Errorf("exact handoff and registered learner identity required")
	}
	member := quepaxa.Member{ID: record.Plan.Replacement.NodeID, PublicKey: record.Plan.Replacement.PublicKey, PeerURL: record.Plan.Replacement.PeerURL, WALIdentity: identity}
	expectedConfig := handoff.Survivors.ConfigID
	expectedAbort := quepaxa.Slot(0)
	if record.Plan.Sequence == operator.V1Alpha2RepairAddThenRemove || record.Plan.Sequence == operator.V1Alpha2RepairAdd {
		if handoff.PreparedConfig == nil || uint64(handoff.PreparedConfig.ConfigID) != record.Plan.ExpectedConfigID {
			return node.ManagedMembershipRequest{}, fmt.Errorf("exact ADD-first prepared source required")
		}
		expectedConfig = handoff.PreparedConfig.ConfigID
		expectedAbort = quepaxa.Slot(record.Plan.ExpectedAbortSlot)
	}
	return node.ManagedMembershipRequest{RepairOperationID: record.Plan.OperationID, Action: node.ManagedMembershipAdd, AddOnly: record.Plan.Sequence == operator.V1Alpha2RepairAdd, ExpectedConfigID: expectedConfig, ExpectedAbortSlot: expectedAbort, OldNodeID: record.Plan.OldNodeID, OldWALIdentity: record.Plan.OldWALIdentity, Addition: &member, HandoffHeadHash: handoff.HeadHash}, nil
}

func managedRepairAdditionCompletion(plan operator.V1Alpha2RepairPlan, handoff operator.V1Alpha2RepairHandoff, proof node.ManagedRepairCompletion) (operator.V1Alpha2RepairCompletion, error) {
	if handoff.PreparedConfig == nil || proof.Current.ConfigID != uint(handoff.PreparedConfig.ConfigID+1) || proof.Terminal.Slot <= quepaxa.Slot(handoff.PreparedSlot) {
		return operator.V1Alpha2RepairCompletion{}, fmt.Errorf("verified ADD terminal differs from exact prepared source")
	}
	ids := make([]types.NodeID, 0, len(proof.Current.Members))
	for _, member := range proof.Current.Members {
		ids = append(ids, types.NodeID(member.ID))
	}
	slices.Sort(ids)
	expected := make([]types.NodeID, 0, len(handoff.PreparedConfig.Members)+1)
	for _, member := range handoff.PreparedConfig.Members {
		expected = append(expected, types.NodeID(member.ID))
	}
	expected = append(expected, plan.Replacement.NodeID)
	slices.Sort(expected)
	if !slices.Equal(ids, expected) {
		return operator.V1Alpha2RepairCompletion{}, fmt.Errorf("verified ADD terminal does not bind the exact intermediate roster")
	}
	return operator.V1Alpha2RepairCompletion{OperationID: plan.OperationID, PlanDigest: plan.Digest(), ExpectedConfigID: plan.ExpectedConfigID, ExpectedAbortSlot: plan.ExpectedAbortSlot, Kind: "Terminal", ConfigID: uint64(proof.Current.ConfigID), Slot: uint64(proof.Terminal.Slot), Hash: fmt.Sprintf("%x", proof.Terminal.Hash), FinalVoterIDs: ids}, nil
}

func repairCompletion(plan operator.V1Alpha2RepairPlan, proof node.ManagedRepairCompletion) (operator.V1Alpha2RepairCompletion, error) {
	ids := make([]types.NodeID, 0, len(proof.Current.Members))
	for _, member := range proof.Current.Members {
		ids = append(ids, types.NodeID(member.ID))
	}
	slices.Sort(ids)
	if uint64(proof.Current.ConfigID) != plan.ExpectedConfigID+2 || !slices.Equal(ids, plan.FinalVoterIDs) || proof.Terminal.Slot <= quepaxa.Slot(plan.ExpectedAbortSlot) {
		return operator.V1Alpha2RepairCompletion{}, fmt.Errorf("verified runtime terminal does not match exact final repair target")
	}
	return operator.V1Alpha2RepairCompletion{OperationID: plan.OperationID, PlanDigest: plan.Digest(), ExpectedConfigID: plan.ExpectedConfigID, ExpectedAbortSlot: plan.ExpectedAbortSlot, Kind: "Terminal", ConfigID: uint64(proof.Current.ConfigID), Slot: uint64(proof.Terminal.Slot), Hash: fmt.Sprintf("%x", proof.Terminal.Hash), FinalVoterIDs: ids}, nil
}
