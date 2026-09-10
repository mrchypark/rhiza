package node

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"reflect"
	"slices"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type membershipOperation struct {
	Request network.MembershipChange `json:"request"`
	Target  quepaxa.Cluster          `json:"target"`
}

func (n *Node) changeMembership(ctx context.Context, request network.MembershipChange) error {
	n.membershipMu.Lock()
	defer n.membershipMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if !n.config.EnableReconfiguration || n.bucket == nil || !n.core.IsVoter() {
		return network.ErrNotReady
	}
	if request.OperationID == "" || len(request.OperationID) > 256 || request.ClusterID != string(n.config.ClusterID) || request.ExpectedConfigID == 0 || request.ExpectedConfigID == ^uint(0) || (request.Remove == "") == (request.Add == nil) {
		return network.ErrInvalidRequest
	}
	key := n.membershipOperationKey(request)
	stored, err := readVoterRegistration(ctx, n.bucket, key)
	if err != nil {
		return err
	}
	operation := membershipOperation{Request: request}
	current := n.core.CurrentCluster()
	if stored != nil {
		if err := json.Unmarshal(stored, &operation); err != nil {
			return err
		}
		if !reflect.DeepEqual(operation.Request, request) {
			return network.ErrRequestConflict
		}
	} else {
		if current.ConfigID != request.ExpectedConfigID {
			return network.ErrRequestConflict
		}
		if revision, _, _ := n.core.LastReconfigurationAbort(); revision != request.ExpectedAbortSlot {
			return network.ErrRequestConflict
		}
		operation.Target = quepaxa.Cluster{ConfigID: current.ConfigID + 1}
		found := false
		for _, member := range current.Members {
			if member.ID == request.Remove {
				found = true
				continue
			}
			if request.Add != nil && member.ID == request.Add.ID {
				return network.ErrInvalidRequest
			}
			operation.Target.Members = append(operation.Target.Members, member)
		}
		if request.Add != nil {
			if request.Fence != nil || request.Add.ID == "" || request.Add.Token == "" || request.Add.Token == n.config.AdminToken || request.Add.WALIdentity == "" || request.Add.PeerURL == "" {
				return network.ErrInvalidRequest
			}
			operation.Target.Members = append(operation.Target.Members, *request.Add)
			registrationKey := path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID), "voters", fmt.Sprintf("%x.json", sha256.Sum256([]byte(request.Add.ID))))
			data, err := readVoterRegistration(ctx, n.bucket, registrationKey)
			if err != nil {
				return err
			}
			var registration voterIdentity
			if len(data) == 0 || json.Unmarshal(data, &registration) != nil || registration.Node != string(request.Add.ID) || registration.Cluster != request.ClusterID || registration.Nonce != request.Add.WALIdentity || registration.LearnerTokenHash != fmt.Sprintf("%x", sha256.Sum256([]byte(request.Add.Token))) {
				return network.ErrInvalidRequest
			}
		} else {
			if !found || len(operation.Target.Members) == 0 || request.Remove == n.config.NodeID {
				return network.ErrInvalidRequest
			}
			if err := n.validateMembershipFence(ctx, request); err != nil {
				return err
			}
		}
		if err := n.core.ValidateReconfigurationTarget(operation.Target); err != nil {
			return fmt.Errorf("%w: %v", network.ErrInvalidRequest, err)
		}
		if pending, ok := n.core.PendingReconfiguration(); ok && !reflect.DeepEqual(pending, operation.Target) {
			return network.ErrRequestConflict
		}
		encoded, err := json.Marshal(operation)
		if err != nil {
			return err
		}
		if len(encoded) >= 64<<10 {
			return network.ErrInvalidRequest
		}
		if err := matchVoterRegistration(ctx, n.bucket, key, encoded, nil); err != nil {
			if errors.Is(err, ErrVoterStateLost) {
				return network.ErrRequestConflict
			}
			return err
		}
	}
	if !reflect.DeepEqual(current, operation.Target) {
		if current.ConfigID != request.ExpectedConfigID {
			return network.ErrRequestConflict
		}
		if revision, _, _ := n.core.LastReconfigurationAbort(); revision != request.ExpectedAbortSlot {
			return network.ErrRequestConflict
		}
		if pending, ok := n.core.PendingReconfiguration(); ok {
			if !reflect.DeepEqual(pending, operation.Target) {
				return network.ErrRequestConflict
			}
		} else if _, err := n.core.BeginReconfigurationAt(ctx, operation.Target, request.ExpectedAbortSlot); err != nil {
			return err
		}
		if err := n.core.FinishReconfigurationAt(ctx, operation.Target, request.ExpectedAbortSlot); err != nil {
			return err
		}
		if !reflect.DeepEqual(n.core.CurrentCluster(), operation.Target) {
			return network.ErrRequestConflict
		}
	}
	return n.persistMembership(ctx)
}

func (n *Node) membershipOperationKey(request network.MembershipChange) string {
	name := fmt.Sprintf("%d.json", request.ExpectedConfigID)
	if request.ExpectedAbortSlot != 0 {
		name = fmt.Sprintf("%d-abort-%d.json", request.ExpectedConfigID, request.ExpectedAbortSlot)
	}
	return path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID), "membership", name)
}

func (n *Node) abortMembership(ctx context.Context, request network.MembershipChange) error {
	n.membershipMu.Lock()
	defer n.membershipMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if !n.config.EnableReconfiguration || n.bucket == nil || !n.core.IsVoter() {
		return network.ErrNotReady
	}
	if request.OperationID == "" || request.Remove != "" || request.ClusterID != string(n.config.ClusterID) || request.ExpectedConfigID != n.core.ConfigID() {
		return network.ErrInvalidRequest
	}
	data, err := readVoterRegistration(ctx, n.bucket, n.membershipOperationKey(request))
	if err != nil {
		return err
	}
	var operation membershipOperation
	if data == nil || json.Unmarshal(data, &operation) != nil || operation.Request.Add == nil || operation.Request.OperationID != request.OperationID || operation.Request.ClusterID != request.ClusterID || operation.Request.ExpectedConfigID != request.ExpectedConfigID || operation.Request.ExpectedAbortSlot != request.ExpectedAbortSlot {
		return network.ErrRequestConflict
	}
	if request.Add != nil && !reflect.DeepEqual(operation.Request, request) {
		return network.ErrRequestConflict
	}
	revision, target, aborted := n.core.LastReconfigurationAbort()
	if aborted && revision > request.ExpectedAbortSlot && reflect.DeepEqual(target, operation.Target) {
		return n.persistMembership(ctx)
	}
	if revision != request.ExpectedAbortSlot {
		return network.ErrRequestConflict
	}
	pending, ok := n.core.PendingReconfiguration()
	if !ok || !reflect.DeepEqual(pending, operation.Target) {
		return network.ErrRequestConflict
	}
	if err := n.core.AbortReconfigurationAt(ctx, operation.Target, request.ExpectedAbortSlot); err != nil {
		return err
	}
	revision, target, aborted = n.core.LastReconfigurationAbort()
	if !aborted || revision <= request.ExpectedAbortSlot || !reflect.DeepEqual(target, operation.Target) {
		return network.ErrRequestConflict
	}
	return n.persistMembership(ctx)
}

func (n *Node) persistMembership(ctx context.Context) error {
	through := n.core.Tip()
	if err := n.core.EnsureDurableThrough(ctx, through); err != nil {
		return err
	}
	if n.archive != nil {
		return n.archive.SyncThrough(ctx, n.core, through)
	}
	return nil
}

func (n *Node) validateMembershipFence(ctx context.Context, request network.MembershipChange) error {
	fence := request.Fence
	if fence == nil || !fence.Confirmed || fence.NodeID != request.Remove || fence.WorkloadUID == "" || fence.WALIdentity == "" || fence.Evidence == "" {
		return network.ErrInvalidRequest
	}
	key := path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID), "voters", fmt.Sprintf("%x.json", sha256.Sum256([]byte(request.Remove))))
	data, err := readVoterRegistration(ctx, n.bucket, key)
	if err != nil {
		return err
	}
	var registration voterIdentity
	if len(data) == 0 || json.Unmarshal(data, &registration) != nil || registration.Node != string(request.Remove) || registration.Cluster != request.ClusterID || registration.Nonce != fence.WALIdentity {
		return network.ErrInvalidRequest
	}
	return nil
}

// Admission must use the configuration that owns the drain, even if another
// goroutine has already learned its terminal while this recorder was verifying.
func (n *Node) verifyMembershipAdmission(ctx context.Context, target quepaxa.Cluster, through quepaxa.Slot, prefix [32]byte) error {
	previous := n.core.ClusterForSlot(through)
	for _, candidate := range target.Members {
		if !slices.ContainsFunc(previous.Members, func(member quepaxa.Member) bool { return member.ID == candidate.ID }) {
			if err := n.transport.VerifyLearner(ctx, candidate, through, prefix); err != nil {
				return err
			}
		}
	}
	return nil
}
