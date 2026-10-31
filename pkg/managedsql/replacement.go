package managedsql

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

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
	if err := h.validateRepairPlanRuntime(ctx, plan); err != nil {
		return err
	}
	a, err := h.live(ctx)
	if err != nil {
		return err
	}
	if existing, ok := a.Repair(); ok {
		if !reflect.DeepEqual(existing.Plan, plan) {
			if existing.State == "Closed" {
				return fmt.Errorf("another exact repair is already closed")
			}
		} else if existing.State == "Completed" && existing.Completion != nil {
			snapshot, err := h.node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
			if err != nil {
				return err
			}
			identity := ""
			for _, member := range snapshot.Current.Members {
				if member.ID == plan.Replacement.NodeID {
					identity = member.WALIdentity
					break
				}
			}
			request, err := managedRepairAddRequest(existing, existing.PreparedHandoff, identity)
			if err != nil {
				return err
			}
			proof, err := h.node.VerifyManagedRepairCompletion(ctx, request)
			if err != nil {
				return err
			}
			return matchRepairCompletion(plan, proof, *existing.Completion)
		} else if existing.State != "Closed" {
			return fmt.Errorf("repair is not resumable")
		}
	}
	if !a.SQLReservationsClosed() {
		if err := h.validateRepairStartState(ctx, plan); err != nil {
			return err
		}
		closedSnapshot, observation, err := operator.BeginV1Alpha2Repair(ctx, h.cfg.Store, a, a.Authority(), plan)
		if err != nil {
			return err
		}
		if observation != operator.V1Alpha2CASApplied {
			return fmt.Errorf("repair close CAS outcome %s; refusing to claim ownership", observation)
		}
		if !closedSnapshot.SQLReservationsClosed() {
			return fmt.Errorf("repair close did not persist the reservation gate")
		}
	}
	if err := h.RemoveRepairVoter(ctx, plan.OperationID); err != nil {
		return fmt.Errorf("remove old voter: %w", err)
	}
	handoff, err := h.CaptureRepairHandoff(ctx, plan.OperationID)
	if err != nil {
		return fmt.Errorf("capture post-REMOVE handoff: %w", err)
	}
	learnerIdentity, err := h.waitRegisteredRepairLearner(ctx, plan.Replacement.NodeID, plan.Replacement.PublicKey)
	if err != nil {
		return err
	}
	if err := h.AddRepairVoter(ctx, plan.OperationID, learnerIdentity); err != nil {
		return fmt.Errorf("add registered learner: %w", err)
	}
	record := operator.V1Alpha2RepairRecord{Plan: plan, PreparedHandoff: &handoff}
	request, err := managedRepairAddRequest(record, &handoff, learnerIdentity)
	if err != nil {
		return err
	}
	proof, err := h.node.VerifyManagedRepairCompletion(ctx, request)
	if err != nil {
		return err
	}
	completion, err := repairCompletion(plan, proof)
	if err != nil {
		return err
	}
	latest, err := h.live(ctx)
	if err != nil {
		return err
	}
	closedRepair, ok := latest.Repair()
	if !ok || closedRepair.State != "Closed" || !latest.SQLReservationsClosed() || !reflect.DeepEqual(closedRepair.Plan, plan) || closedRepair.PreparedHandoff == nil || !reflect.DeepEqual(*closedRepair.PreparedHandoff, handoff) {
		return fmt.Errorf("exact live closed repair changed before completion")
	}
	finished, observation, err := operator.FinishV1Alpha2Repair(ctx, h.cfg.Store, latest, latest.Authority(), completion)
	if err != nil {
		return err
	}
	if observation != operator.V1Alpha2CASApplied {
		return fmt.Errorf("repair completion CAS outcome %s; refusing to report completion", observation)
	}
	finishedRepair, ok := finished.Repair()
	if !ok || finishedRepair.State != "Completed" || finishedRepair.Completion == nil || !reflect.DeepEqual(*finishedRepair.Completion, completion) {
		return fmt.Errorf("repair completion readback mismatch")
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
	ids := make([]types.NodeID, 0, len(snapshot.Current.Members))
	for _, member := range snapshot.Current.Members {
		if member.ID == plan.OldNodeID {
			oldFound = true
			continue
		}
		ids = append(ids, types.NodeID(member.ID))
	}
	if !oldFound || plan.OldWALIdentity == "" {
		return fmt.Errorf("old voter or its expected WAL identity is missing from repair intent")
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
	return node.ManagedMembershipRequest{RepairOperationID: record.Plan.OperationID, Action: node.ManagedMembershipAdd, ExpectedConfigID: handoff.Survivors.ConfigID, OldNodeID: record.Plan.OldNodeID, OldWALIdentity: record.Plan.OldWALIdentity, Addition: &member, HandoffHeadHash: handoff.HeadHash}, nil
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

func matchRepairCompletion(plan operator.V1Alpha2RepairPlan, proof node.ManagedRepairCompletion, recorded operator.V1Alpha2RepairCompletion) error {
	actual, err := repairCompletion(plan, proof)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(actual, recorded) {
		return fmt.Errorf("completed repair status differs from actual retained terminal")
	}
	return nil
}
