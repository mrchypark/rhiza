package node

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path"
	"reflect"
	"strings"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

type ManagedMembershipAction string

const (
	ManagedMembershipRemove ManagedMembershipAction = "Remove"
	ManagedMembershipAdd    ManagedMembershipAction = "Add"
)

// ManagedMembershipRequest authorizes consensus membership only. It is not an
// external fencing attestation and contains no replacement startup/WAL fiction.
type ManagedMembershipRequest struct {
	RepairOperationID string
	Action            ManagedMembershipAction
	ExpectedConfigID  uint
	ExpectedAbortSlot quepaxa.Slot
	OldNodeID         quepaxa.NodeID
	OldWALIdentity    string
	Addition          *quepaxa.Member
	HandoffHeadHash   string
}

type ManagedRepairSource struct {
	Head           []byte
	HeadHash       [32]byte
	RemoveTerminal quepaxa.DecidedValue
	Survivors      quepaxa.Cluster
}

// CaptureManagedRepairSource requires the exact completed REMOVE journal and
// verifies immutable archived objects against the original trusted bootstrap.
func (n *Node) CaptureManagedRepairSource(ctx context.Context, r ManagedMembershipRequest) (ManagedRepairSource, error) {
	if r.Action != ManagedMembershipRemove {
		return ManagedRepairSource{}, network.ErrInvalidRequest
	}
	var result ManagedRepairSource
	err := n.runManagedMembership(ctx, r, &result)
	return result, err
}

// Called only within changeMembership's lock and Server proposal lifecycle.
func (n *Node) captureManagedRepairSource(ctx context.Context, r ManagedMembershipRequest) (ManagedRepairSource, error) {
	if n.archive == nil || n.core.CurrentCluster().ConfigID != r.ExpectedConfigID+1 {
		return ManagedRepairSource{}, network.ErrNotReady
	}
	change := r.change(string(n.config.ClusterID))
	if err := n.validateManagedMembership(ctx, change, false); err != nil {
		return ManagedRepairSource{}, err
	}
	// Consensus certifies the control target, not an application operation ID.
	// Bind that certificate to the exact immutable operation journal first.
	data, err := readVoterRegistration(ctx, n.bucket, n.membershipOperationKey(change))
	if err != nil {
		return ManagedRepairSource{}, err
	}
	var operation membershipOperation
	if len(data) == 0 || json.Unmarshal(data, &operation) != nil || !reflect.DeepEqual(operation.Request, change) || operation.Request.Remove != r.OldNodeID || operation.Request.Add != nil || !operation.ManagedExcludedRead || !reflect.DeepEqual(operation.Target, n.core.CurrentCluster()) {
		return ManagedRepairSource{}, network.ErrRequestConflict
	}
	terminal, err := n.retainedMembershipTerminal(operation)
	if err != nil {
		return ManagedRepairSource{}, err
	}
	source := n.core.ClusterForSlot(terminal.Slot)
	expected := quepaxa.Cluster{ConfigID: r.ExpectedConfigID + 1}
	foundOld := false
	for _, member := range source.Members {
		if member.ID == r.OldNodeID {
			foundOld = true
		} else {
			expected.Members = append(expected.Members, member)
		}
	}
	if !foundOld || !reflect.DeepEqual(expected, operation.Target) {
		return ManagedRepairSource{}, network.ErrRequestConflict
	}
	if err := n.archive.SyncThrough(ctx, n.core, n.core.Tip()); err != nil {
		return ManagedRepairSource{}, err
	}
	prefix := path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID))
	bootstrap := quepaxa.Cluster{ConfigID: 1, Members: n.config.Members}
	view, err := recovery.OpenReadOnlyView(ctx, n.bucket, prefix, 1, bootstrap)
	if err != nil {
		return ManagedRepairSource{}, err
	}
	view, err = recovery.OpenReadOnlyViewAtHead(ctx, n.bucket, prefix, 1, bootstrap, view.CapturedHead())
	if err != nil {
		return ManagedRepairSource{}, err
	}
	found := false
	if seal, _, ok := view.RecoveryBase(); ok && seal.Membership != nil {
		for _, transition := range seal.Membership.Transitions {
			decision := transition.Terminal
			if decision.Slot == terminal.Slot && decision.Hash == terminal.Hash && reflect.DeepEqual(decision.Value, terminal.Value) && reflect.DeepEqual(decision.Certificate, terminal.Certificate) {
				found = true
			}
		}
	}
	for _, decision := range view.Decisions() {
		if decision.Slot == terminal.Slot {
			found = decision.Hash == terminal.Hash && reflect.DeepEqual(decision.Value, terminal.Value) && reflect.DeepEqual(decision.Certificate, terminal.Certificate)
		}
	}
	if !found {
		return ManagedRepairSource{}, fmt.Errorf("captured HEAD does not retain exact REMOVE terminal")
	}
	if err := n.validateManagedMembership(ctx, change, false); err != nil {
		return ManagedRepairSource{}, err
	}
	return ManagedRepairSource{Head: view.CapturedHead(), HeadHash: view.HeadHash(), RemoveTerminal: terminal, Survivors: n.core.CurrentCluster()}, nil
}

// ManagedMembershipChecker is a live private-host authority check. It must not
// require new SQL admission: pending resolution and control remain independent.
type ManagedMembershipChecker interface {
	ValidateManagedMembership(context.Context, ManagedStartupIdentity, ManagedMembershipRequest, quepaxa.Cluster, quepaxa.Slot) error
}

type managedMembershipPermitKey struct{}
type managedMembershipPermit struct {
	request ManagedMembershipRequest
	change  network.MembershipChange
	capture *ManagedRepairSource
}

func (r ManagedMembershipRequest) change(cluster string) network.MembershipChange {
	phase := "remove"
	if r.Action == ManagedMembershipAdd {
		phase = "add"
	}
	digest := sha256.Sum256([]byte("rhiza-managed-membership-v1\x00" + r.RepairOperationID + "\x00" + phase))
	c := network.MembershipChange{OperationID: fmt.Sprintf("managed-%x", digest), ClusterID: cluster, ExpectedConfigID: r.ExpectedConfigID, ExpectedAbortSlot: r.ExpectedAbortSlot}
	if r.Action == ManagedMembershipRemove {
		c.Remove = r.OldNodeID
	} else if r.Action == ManagedMembershipAdd {
		c.Add = r.Addition
	}
	return c
}

// ChangeManagedMembership reuses the server/WAL lifecycle. ADD requires the
// actual registered learner and a live host-validated fixed source handoff.
func (n *Node) ChangeManagedMembership(ctx context.Context, r ManagedMembershipRequest) error {
	return n.runManagedMembership(ctx, r, nil)
}

func (n *Node) runManagedMembership(ctx context.Context, r ManagedMembershipRequest, capture *ManagedRepairSource) error {
	if n == nil {
		return network.ErrNotReady
	}
	n.shutdownMu.Lock()
	defer n.shutdownMu.Unlock()
	if r.Action != ManagedMembershipRemove && r.Action != ManagedMembershipAdd {
		return network.ErrInvalidRequest
	}
	if r.Addition != nil {
		member := *r.Addition
		r.Addition = &member
	}
	if r.Action == ManagedMembershipAdd && (r.Addition == nil || r.Addition.WALIdentity == "" || r.HandoffHeadHash == "") || r.Action == ManagedMembershipRemove && r.Addition != nil {
		return network.ErrInvalidRequest
	}
	if n.config == nil || n.managedStartup == nil || n.server == nil || n.core == nil || !n.opened.Load() {
		return network.ErrNotReady
	}
	if _, ok := n.managedStartup.Checker.(ManagedMembershipChecker); !ok {
		return network.ErrNotReady
	}
	if r.RepairOperationID == "" || len(r.RepairOperationID) > 256 || strings.TrimSpace(r.RepairOperationID) != r.RepairOperationID || r.OldNodeID == "" || r.OldWALIdentity == "" || len(r.OldWALIdentity) > 256 || r.ExpectedConfigID == 0 || r.ExpectedConfigID == ^uint(0) {
		return network.ErrInvalidRequest
	}
	change := r.change(string(n.config.ClusterID))
	ctx = context.WithValue(ctx, managedMembershipPermitKey{}, managedMembershipPermit{request: r, change: change, capture: capture})
	return n.server.ChangeMembership(ctx, change)
}

// A package-private capability routes this operation through Server's existing
// lifecycle. Raw API/HTTP calls cannot manufacture it or skip live validation.
func (n *Node) validateManagedMembership(ctx context.Context, change network.MembershipChange, abort bool) error {
	permit, ok := ctx.Value(managedMembershipPermitKey{}).(managedMembershipPermit)
	if n.managedStartup == nil {
		if ok {
			return network.ErrNotReady
		}
		return nil
	}
	if n.config == nil || n.core == nil || n.bucket == nil {
		return network.ErrNotReady
	}
	if !ok || abort || change.Fence != nil || !reflect.DeepEqual(permit.change, change) {
		return network.ErrNotReady
	}
	checker, ok := n.managedStartup.Checker.(ManagedMembershipChecker)
	if !ok || !n.opened.Load() {
		return network.ErrNotReady
	}
	i := n.managedStartup.Identity
	source, _, err := n.preparedBindingDigests()
	if err != nil || source == "" || source != i.PhysicalBinding || source != i.Member.SourceBinding || i.Member.NodeID != string(n.config.NodeID) || i.Member.ClusterUID == "" || i.Member.DeploymentUID == "" || i.Member.Generation < 0 {
		return fmt.Errorf("managed membership namespace/identity changed")
	}
	revision, _, _ := n.core.LastReconfigurationAbort()
	if err := checker.ValidateManagedMembership(ctx, i, permit.request, n.core.CurrentCluster(), revision); err != nil {
		return err
	}
	if permit.request.OldNodeID == n.config.NodeID {
		return network.ErrInvalidRequest
	}
	if permit.request.Action == ManagedMembershipAdd {
		if permit.request.Addition == nil {
			return network.ErrInvalidRequest
		}
		if err := n.validateRegisteredVoterWAL(ctx, permit.request.Addition.ID, permit.request.Addition.WALIdentity); err != nil {
			return err
		}
	}
	return n.validateRegisteredVoterWAL(ctx, permit.request.OldNodeID, permit.request.OldWALIdentity)
}

func (n *Node) validateRemovalAuthority(ctx context.Context, change network.MembershipChange) error {
	if n.managedStartup == nil {
		return n.validateMembershipFence(ctx, change)
	}
	return n.validateManagedMembership(ctx, change, false)
}
