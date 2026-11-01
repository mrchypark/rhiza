package managedsql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"reflect"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

const repairOriginMaxBytes = (128 << 10) + 4096

type managedRepairOriginFile struct {
	Version int                               `json:"version"`
	Repair  operator.V1Alpha2RepairRecord     `json:"repair"`
	Binding operator.V1Alpha2RepairPodBinding `json:"binding"`
}

// ValidateManagedRepairOrigin implements Node's private origin-checker boundary.
// The byte slice is the original repair.json; no separate origin ledger is
// accepted or created by the runtime.
func (h *Host) ValidateManagedRepairOrigin(ctx context.Context, raw []byte) error {
	if h == nil || h.cfg.Store == nil || len(raw) == 0 || len(raw) > repairOriginMaxBytes {
		return fmt.Errorf("invalid managed repair origin")
	}
	var origin managedRepairOriginFile
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&origin) != nil || d.Decode(new(any)) != io.EOF || origin.Version != 1 {
		return fmt.Errorf("invalid managed repair origin encoding")
	}
	plan, handoff := origin.Repair.Plan, origin.Repair.PreparedHandoff
	if origin.Repair.State != "Closed" || origin.Repair.Completion != nil || handoff == nil {
		return fmt.Errorf("repair origin is not the immutable closed handoff")
	}
	if plan.ClusterUID != h.cfg.Ref.UID || plan.Generation != h.cfg.Plan.Generation || plan.DeploymentUID != h.cfg.Plan.DeploymentUID || plan.StorageID != h.cfg.Plan.StorageID || plan.BootstrapPlanDigest != h.cfg.Plan.BootstrapPlanDigest || plan.Replacement.PodUID != h.cfg.PodUID || plan.Replacement.NodeID != h.cfg.Runtime.NodeID || h.cfg.Runtime.Learner == nil || *h.cfg.Runtime.Learner != planMember(plan) {
		return fmt.Errorf("repair origin differs from managed learner identity")
	}
	if plan.Replacement.PublicKey != quepaxa.PublicKey(network.PeerPublicKey(h.cfg.Runtime.ClusterID, h.cfg.Runtime.NodeID, h.cfg.Runtime.PeerToken)) {
		return fmt.Errorf("repair origin public key differs from configured peer identity")
	}
	if handoff.PlanDigest != plan.Digest() || handoff.PhysicalBinding != h.identity.PhysicalBinding || handoff.SourceRoot != h.cfg.StorageRoot || handoff.HeadHash != fmt.Sprintf("%x", sha256.Sum256(handoff.Head)) || len(handoff.Head) == 0 || handoff.RemoveTerminalSlot <= plan.ExpectedAbortSlot || handoff.Survivors.ConfigID != uint(plan.ExpectedConfigID+1) {
		return fmt.Errorf("repair origin handoff digest or physical binding mismatch")
	}
	if terminalHash, err := hex.DecodeString(handoff.RemoveTerminalHash); err != nil || len(terminalHash) != sha256.Size {
		return fmt.Errorf("invalid repair origin REMOVE terminal hash")
	}
	if h.cfg.Prepared != nil && !bytes.Equal(handoff.Head, h.cfg.Prepared.View().CapturedHead()) {
		return fmt.Errorf("prepared source HEAD differs from immutable repair origin")
	}
	namespace, err := node.ManagedNamespaceFor(h.cfg.StorageRoot, h.cfg.Ref.UID, h.cfg.Plan.Generation, h.cfg.Plan.AssignmentID, string(h.cfg.Runtime.ClusterID))
	if err != nil || handoff.SourcePrefix != namespace.FinalPrefix {
		return fmt.Errorf("repair origin source namespace mismatch")
	}
	binding, err := operator.ValidateV1Alpha2RepairPodBinding(ctx, h.cfg.Store, h.cfg.Ref, plan)
	if err != nil {
		return fmt.Errorf("verify current repair Pod owner chain: %w", err)
	}
	if origin.Binding != binding || binding.PodUID != h.cfg.PodUID || binding.ClusterUID != h.cfg.Ref.UID || binding.Generation != h.cfg.Plan.Generation || binding.DeploymentUID != h.cfg.Plan.DeploymentUID {
		return fmt.Errorf("live repair Pod owner chain differs from immutable origin")
	}
	if h.cfg.Prepared != nil {
		admission, err := operator.LoadV1Alpha2Admission(ctx, h.cfg.Store, h.cfg.Ref)
		if err != nil {
			return err
		}
		current, ok := admission.Repair()
		if !ok || current.State != "Closed" || !reflect.DeepEqual(current.Plan, plan) || current.PreparedHandoff == nil || !reflect.DeepEqual(*current.PreparedHandoff, *handoff) {
			return fmt.Errorf("prepared learner no longer matches the closed repair intent")
		}
	}
	if err := h.ValidateManagedStartup(ctx, h.identity); err != nil {
		return fmt.Errorf("repair origin current authority changed: %w", err)
	}
	return nil
}

func planMember(plan operator.V1Alpha2RepairPlan) quepaxa.Member {
	return quepaxa.Member{ID: plan.Replacement.NodeID, PublicKey: plan.Replacement.PublicKey, PeerURL: plan.Replacement.PeerURL}
}
