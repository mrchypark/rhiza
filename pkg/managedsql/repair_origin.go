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
	if handoff.PlanDigest != plan.Digest() || handoff.PhysicalBinding != h.identity.PhysicalBinding || handoff.SourceRoot != h.cfg.StorageRoot || handoff.HeadHash != fmt.Sprintf("%x", sha256.Sum256(handoff.Head)) || len(handoff.Head) == 0 {
		return fmt.Errorf("repair origin handoff digest or physical binding mismatch")
	}
	switch plan.Sequence {
	case "", operator.V1Alpha2RepairRemoveThenAdd:
		if handoff.RemoveTerminalSlot <= plan.ExpectedAbortSlot || handoff.Survivors.ConfigID != uint(plan.ExpectedConfigID+1) || handoff.PreparedConfig != nil || handoff.PreparedSlot != 0 || handoff.PreparedHash != "" {
			return fmt.Errorf("repair origin handoff digest or physical binding mismatch")
		}
		if terminalHash, err := hex.DecodeString(handoff.RemoveTerminalHash); err != nil || len(terminalHash) != sha256.Size {
			return fmt.Errorf("invalid repair origin REMOVE terminal hash")
		}
		if h.cfg.Prepared != nil && !bytes.Equal(handoff.Head, h.cfg.Prepared.View().CapturedHead()) {
			return fmt.Errorf("prepared source HEAD differs from immutable repair origin")
		}
	case operator.V1Alpha2RepairAddThenRemove, operator.V1Alpha2RepairAdd:
		if handoff.PreparedConfig == nil || uint64(handoff.PreparedConfig.ConfigID) != plan.ExpectedConfigID || handoff.PreparedSlot <= plan.ExpectedAbortSlot || handoff.RemoveTerminalSlot != 0 || handoff.RemoveTerminalHash != "" || handoff.Survivors.ConfigID != 0 || len(handoff.Survivors.Members) != 0 {
			return fmt.Errorf("invalid ADD-first prepared source in repair origin")
		}
		preparedHash, err := hex.DecodeString(handoff.PreparedHash)
		if err != nil || len(preparedHash) != sha256.Size || fmt.Sprintf("%x", preparedHash) != handoff.PreparedHash {
			return fmt.Errorf("invalid ADD-first prepared prefix hash in repair origin")
		}
		if h.cfg.Prepared != nil {
			view := h.cfg.Prepared.View()
			if view == nil {
				return fmt.Errorf("ADD-first prepared source differs from immutable repair origin")
			}
			prefixHash := view.PrefixHash()
			if uint64(view.Tip()) != handoff.PreparedSlot || !bytes.Equal(prefixHash[:], preparedHash) || !bytes.Equal(handoff.Head, view.CapturedHead()) {
				return fmt.Errorf("ADD-first prepared source differs from immutable repair origin")
			}
		}
		// With no Prepared view this is a retained same-Pod restart. Node's
		// preflight separately requires a valid persisted adoption whose exact
		// origin digest matches these immutable H bytes before opening runtime state.
	default:
		return fmt.Errorf("unsupported repair origin sequence")
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
