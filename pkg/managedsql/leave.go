package managedsql

import (
	"context"
	"crypto/sha256"
	"fmt"
	"reflect"
	"slices"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type LeaveVoterRequest struct {
	NodeID quepaxa.NodeID `json:"node_id"`
}

// LeaveVoter removes exactly one named voter after spec.voterCount was
// explicitly reduced by one. The closed repair record is the replay journal.
func (h *Host) LeaveVoter(ctx context.Context, request LeaveVoterRequest) error {
	if request.NodeID == "" {
		return fmt.Errorf("%w: voter ID required", network.ErrInvalidRequest)
	}
	h.mu.Lock()
	closed := h.closed
	h.mu.Unlock()
	if closed {
		return fmt.Errorf("SQL host closed")
	}
	a, err := h.live(ctx)
	if err != nil {
		return err
	}
	if r, ok := a.Repair(); ok && r.Plan.Sequence == operator.V1Alpha2RepairLeave && r.Plan.OldNodeID == types.NodeID(request.NodeID) && r.State == "Completed" {
		return h.resumeLeave(ctx, r)
	}
	if _, pending := a.Pending(); pending {
		return fmt.Errorf("pending application write blocks managed leave")
	}
	target, err := a.TargetVoterCount()
	if err != nil {
		return err
	}
	if r, ok := a.Repair(); ok && r.Plan.Sequence == operator.V1Alpha2RepairLeave && r.Plan.OldNodeID == types.NodeID(request.NodeID) {
		return h.resumeLeave(ctx, r)
	}
	if a.SQLReservationsClosed() {
		return fmt.Errorf("another repair blocks managed leave")
	}
	r, err := h.beginLeave(ctx, a, request, target)
	if err != nil {
		return err
	}
	return h.resumeLeave(ctx, r)
}

func (h *Host) beginLeave(ctx context.Context, a operator.V1Alpha2AdmissionSnapshot, request LeaveVoterRequest, target int) (operator.V1Alpha2RepairRecord, error) {
	snapshot, err := h.node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil {
		return operator.V1Alpha2RepairRecord{}, err
	}
	if len(snapshot.Current.Members) <= 3 || target != len(snapshot.Current.Members)-1 {
		return operator.V1Alpha2RepairRecord{}, fmt.Errorf("managed leave requires an exact one-voter desired-count reduction")
	}
	var retired *quepaxa.Member
	for i := range snapshot.Current.Members {
		if snapshot.Current.Members[i].ID == request.NodeID {
			retired = &snapshot.Current.Members[i]
			break
		}
	}
	if retired == nil {
		return operator.V1Alpha2RepairRecord{}, fmt.Errorf("%w: voter is not in the current configuration", network.ErrInvalidRequest)
	}
	if retired.ID == h.cfg.Runtime.NodeID {
		return operator.V1Alpha2RepairRecord{}, fmt.Errorf("%w: a voter cannot leave through its own runtime", network.ErrInvalidRequest)
	}
	wal, err := h.node.ManagedVoterWALIdentity(ctx, snapshot.Current.ConfigID, snapshot.AbortSlot, *retired)
	if err != nil {
		return operator.V1Alpha2RepairRecord{}, err
	}
	final := make([]types.NodeID, 0, len(snapshot.Current.Members)-1)
	for _, member := range snapshot.Current.Members {
		if member.ID != retired.ID {
			final = append(final, types.NodeID(member.ID))
		}
	}
	slices.Sort(final)
	plan := operator.V1Alpha2RepairPlan{Sequence: operator.V1Alpha2RepairLeave, OperationID: managedLeaveOperationID(h.identity, snapshot.Current.ConfigID, snapshot.AbortSlot, retired.ID, target), ClusterUID: h.cfg.Ref.UID, Generation: h.cfg.Plan.Generation, DeploymentUID: h.cfg.Plan.DeploymentUID, StorageID: h.cfg.Plan.StorageID, ExpectedConfigID: uint64(snapshot.Current.ConfigID), ExpectedAbortSlot: uint64(snapshot.AbortSlot), OldNodeID: types.NodeID(retired.ID), OldWALIdentity: wal, Replacement: operator.V1Alpha2RepairMember{PodName: string(retired.ID), PodUID: string(retired.ID), NodeID: types.NodeID(retired.ID), PublicKey: retired.PublicKey, PeerURL: retired.PeerURL}, BootstrapPlanDigest: h.cfg.Plan.BootstrapPlanDigest, FinalVoterIDs: final}
	closedSnapshot, observation, err := operator.BeginV1Alpha2Repair(ctx, h.cfg.Store, a, h.authority, plan)
	if err != nil {
		return operator.V1Alpha2RepairRecord{}, err
	}
	if observation != operator.V1Alpha2CASApplied {
		return operator.V1Alpha2RepairRecord{}, fmt.Errorf("leave intent CAS outcome %s", observation)
	}
	r, ok := closedSnapshot.Repair()
	if !ok || !reflect.DeepEqual(r.Plan, plan) {
		return operator.V1Alpha2RepairRecord{}, fmt.Errorf("leave intent readback mismatch")
	}
	return r, nil
}

func managedLeaveOperationID(identity node.ManagedStartupIdentity, configID uint, abortSlot quepaxa.Slot, retired quepaxa.NodeID, target int) string {
	payload := fmt.Sprintf("rhiza-managed-leave-v1\x00%s\x00%d\x00%s\x00%s\x00%s\x00%d\x00%d\x00%s\x00%d", identity.Member.ClusterUID, identity.Member.Generation, identity.Member.DeploymentUID, identity.StorageID, identity.Member.NodeID, configID, abortSlot, retired, target)
	return fmt.Sprintf("leave-%x", sha256.Sum256([]byte(payload)))
}

func (h *Host) resumeLeave(ctx context.Context, r operator.V1Alpha2RepairRecord) error {
	if r.Plan.Sequence != operator.V1Alpha2RepairLeave {
		return fmt.Errorf("exact managed leave record required")
	}
	a, err := h.live(ctx)
	if err != nil {
		return err
	}
	if r.State == "Completed" {
		return h.verifyCompletedLeave(ctx, r)
	}
	if r.State != "Closed" || !a.SQLReservationsClosed() {
		return fmt.Errorf("exact closed managed leave record required")
	}
	if err := h.RemoveRepairVoter(ctx, r.Plan.OperationID); err != nil {
		return err
	}
	if r.PreparedHandoff == nil {
		if _, err := h.CaptureRepairHandoff(ctx, r.Plan.OperationID); err != nil {
			return err
		}
		latest, err := h.live(ctx)
		if err != nil {
			return err
		}
		r, _ = latest.Repair()
		if r.PreparedHandoff == nil {
			return fmt.Errorf("leave handoff was not retained")
		}
	}
	request := node.ManagedMembershipRequest{RepairOperationID: r.Plan.OperationID, Action: node.ManagedMembershipRemove, ExpectedConfigID: uint(r.Plan.ExpectedConfigID), ExpectedAbortSlot: quepaxa.Slot(r.Plan.ExpectedAbortSlot), OldNodeID: quepaxa.NodeID(r.Plan.OldNodeID), OldWALIdentity: r.Plan.OldWALIdentity}
	proof, err := h.node.VerifyManagedRepairRemoval(ctx, request)
	if err != nil {
		return err
	}
	completion := operator.V1Alpha2RepairCompletion{OperationID: r.Plan.OperationID, PlanDigest: r.Plan.Digest(), ExpectedConfigID: r.Plan.ExpectedConfigID, ExpectedAbortSlot: r.Plan.ExpectedAbortSlot, Kind: "Terminal", ConfigID: uint64(proof.Survivors.ConfigID), Slot: uint64(proof.RemoveTerminal.Slot), Hash: fmt.Sprintf("%x", proof.RemoveTerminal.Hash), FinalVoterIDs: slices.Clone(r.Plan.FinalVoterIDs)}
	latest, err := h.live(ctx)
	if err != nil {
		return err
	}
	finished, observation, err := operator.FinishV1Alpha2Repair(ctx, h.cfg.Store, latest, h.authority, completion)
	if err != nil {
		return err
	}
	if observation != operator.V1Alpha2CASApplied {
		return fmt.Errorf("leave completion CAS outcome %s", observation)
	}
	record, ok := finished.Repair()
	if !ok || record.State != "Completed" || record.Completion == nil || !reflect.DeepEqual(*record.Completion, completion) {
		return fmt.Errorf("leave completion readback mismatch")
	}
	return nil
}

func (h *Host) verifyCompletedLeave(ctx context.Context, r operator.V1Alpha2RepairRecord) error {
	if r.Completion == nil || r.Completion.Kind != "Terminal" || !slices.Equal(r.Completion.FinalVoterIDs, r.Plan.FinalVoterIDs) {
		return fmt.Errorf("completed leave proof is invalid")
	}
	request := node.ManagedMembershipRequest{RepairOperationID: r.Plan.OperationID, Action: node.ManagedMembershipRemove, ExpectedConfigID: uint(r.Plan.ExpectedConfigID), ExpectedAbortSlot: quepaxa.Slot(r.Plan.ExpectedAbortSlot), OldNodeID: quepaxa.NodeID(r.Plan.OldNodeID), OldWALIdentity: r.Plan.OldWALIdentity}
	proof, err := h.node.VerifyManagedRepairRemoval(ctx, request)
	if err != nil {
		return err
	}
	ids := make([]types.NodeID, 0, len(proof.Survivors.Members))
	for _, member := range proof.Survivors.Members {
		ids = append(ids, types.NodeID(member.ID))
	}
	slices.Sort(ids)
	if !slices.Equal(ids, r.Plan.FinalVoterIDs) || proof.RemoveTerminal.Slot != quepaxa.Slot(r.Completion.Slot) || fmt.Sprintf("%x", proof.RemoveTerminal.Hash) != r.Completion.Hash {
		return fmt.Errorf("completed leave differs from certified REMOVE terminal")
	}
	return nil
}

func (h *Host) validateManagedLeave(r operator.V1Alpha2RepairRecord, request node.ManagedMembershipRequest, current quepaxa.Cluster, abortSlot quepaxa.Slot) error {
	p := r.Plan
	if request.Action != node.ManagedMembershipRemove || request.AddOnly || request.Addition != nil || uint64(request.ExpectedConfigID) != p.ExpectedConfigID || uint64(request.ExpectedAbortSlot) != p.ExpectedAbortSlot || request.OldNodeID != quepaxa.NodeID(p.OldNodeID) || request.OldWALIdentity != p.OldWALIdentity || uint64(abortSlot) != p.ExpectedAbortSlot {
		return fmt.Errorf("managed leave request differs from closed intent")
	}
	ids := make([]types.NodeID, 0, len(current.Members))
	found := false
	for _, member := range current.Members {
		if member.ID == quepaxa.NodeID(p.OldNodeID) {
			found = true
			if member.WALIdentity != "" && member.WALIdentity != p.OldWALIdentity {
				return fmt.Errorf("managed leave voter WAL identity changed")
			}
			continue
		}
		ids = append(ids, types.NodeID(member.ID))
	}
	slices.Sort(ids)
	if current.ConfigID == uint(p.ExpectedConfigID) {
		if !found || len(p.FinalVoterIDs) < 3 || !slices.Equal(ids, p.FinalVoterIDs) {
			return fmt.Errorf("managed leave source configuration changed")
		}
		return nil
	}
	if current.ConfigID == uint(p.ExpectedConfigID+1) && !found {
		if len(current.Members) != len(p.FinalVoterIDs) {
			return fmt.Errorf("managed leave target roster changed")
		}
		got := make([]types.NodeID, 0, len(current.Members))
		for _, member := range current.Members {
			got = append(got, types.NodeID(member.ID))
		}
		slices.Sort(got)
		if !slices.Equal(got, p.FinalVoterIDs) {
			return fmt.Errorf("managed leave target roster changed")
		}
		return nil
	}
	return fmt.Errorf("managed leave configuration/revision changed")
}
