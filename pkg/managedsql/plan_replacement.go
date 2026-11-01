package managedsql

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// PlanVoterReplacement prefers the exact unfinished intent. Discovery cannot
// retarget it, including when its candidate disappeared. Missing Pods authorize
// consensus replacement only, not a physical-death or storage-I/O claim.
func (h *Host) PlanVoterReplacement(ctx context.Context, replacement operator.V1Alpha2RepairPodRequest, presentIDs []quepaxa.NodeID) (*operator.V1Alpha2RepairPlan, error) {
	a, err := h.live(ctx)
	if err != nil {
		return nil, err
	}
	if r, ok := a.Repair(); ok && r.State == "Closed" {
		if _, err := operator.ValidateV1Alpha2RepairPodBinding(ctx, h.cfg.Store, h.cfg.Ref, r.Plan); err != nil {
			return nil, fmt.Errorf("closed repair candidate unavailable; retargeting forbidden: %w", err)
		}
		return &r.Plan, nil
	}
	snapshot, err := h.node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil {
		return nil, err
	}
	seen := map[quepaxa.NodeID]bool{}
	for _, id := range presentIDs {
		if id == "" || seen[id] {
			return nil, fmt.Errorf("invalid present member observations")
		}
		seen[id] = true
	}
	var missing []quepaxa.Member
	for _, m := range snapshot.Current.Members {
		if !seen[m.ID] {
			missing = append(missing, m)
		}
		if m.ID == replacement.NodeID {
			return nil, nil
		}
	}
	if len(missing) == 0 {
		return nil, nil
	}
	if len(missing) != 1 || missing[0].ID == h.cfg.Runtime.NodeID {
		return nil, fmt.Errorf("replacement requires exactly one other missing current voter")
	}
	old := missing[0]
	nonce, err := h.node.ManagedVoterWALIdentity(ctx, snapshot.Current.ConfigID, snapshot.AbortSlot, old)
	if err != nil {
		return nil, err
	}
	p := operator.V1Alpha2RepairPlan{ClusterUID: h.cfg.Ref.UID, Generation: h.cfg.Plan.Generation, DeploymentUID: h.cfg.Plan.DeploymentUID, StorageID: h.cfg.Plan.StorageID, ExpectedConfigID: uint64(snapshot.Current.ConfigID), ExpectedAbortSlot: uint64(snapshot.AbortSlot), OldNodeID: old.ID, OldWALIdentity: nonce, Replacement: replacement, BootstrapPlanDigest: h.cfg.Plan.BootstrapPlanDigest}
	for _, member := range snapshot.Current.Members {
		if member.ID != old.ID {
			p.FinalVoterIDs = append(p.FinalVoterIDs, types.NodeID(member.ID))
		}
	}
	p.FinalVoterIDs = append(p.FinalVoterIDs, replacement.NodeID)
	slices.Sort(p.FinalVoterIDs)
	encoded, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	p.OperationID = fmt.Sprintf("replace-%x", sha256.Sum256(encoded))
	if _, err := operator.ValidateV1Alpha2RepairPodBinding(ctx, h.cfg.Store, h.cfg.Ref, p); err != nil {
		return nil, err
	}
	latest, err := h.live(ctx)
	if err != nil {
		return nil, err
	}
	if latest.SQLReservationsClosed() {
		return nil, fmt.Errorf("another repair closed during planning")
	}
	current, err := h.node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(current.Current, snapshot.Current) || current.AbortSlot != snapshot.AbortSlot {
		return nil, fmt.Errorf("membership changed during replacement planning")
	}
	return &p, nil
}
