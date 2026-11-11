package node

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log"
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
	// AddOnly enrolls one voter without removing OldNodeID. OldNodeID is then
	// a deterministic current-voter witness for the old configuration quorum.
	AddOnly           bool
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
	PreparedConfig *quepaxa.Cluster
	PreparedSlot   quepaxa.Slot
	PreparedHash   [32]byte
}

// ManagedRepairPreparationChecker authorizes only the read-only source capture
// that precedes an ADD-first repair. It is separate from the membership
// mutator capability and cannot authorize a consensus change.
type ManagedRepairPreparationChecker interface {
	ValidateManagedRepairPreparation(context.Context, ManagedStartupIdentity, ManagedMembershipRequest, quepaxa.Cluster, quepaxa.Slot) error
}

// CaptureManagedPreparedRepairSource pins the current certified application
// prefix before an ADD-first membership change. The returned archive head is
// fixed evidence, not an authorization to mutate membership.
func (n *Node) CaptureManagedPreparedRepairSource(ctx context.Context, r ManagedMembershipRequest) (ManagedRepairSource, error) {
	if n == nil || r.Action != ManagedMembershipAdd || r.Addition != nil || r.HandoffHeadHash != "" || r.RepairOperationID == "" {
		return ManagedRepairSource{}, network.ErrInvalidRequest
	}
	n.shutdownMu.Lock()
	defer n.shutdownMu.Unlock()
	if err := n.managedRepairReady(); err != nil {
		return ManagedRepairSource{}, err
	}
	checker, ok := n.managedStartup.Checker.(ManagedRepairPreparationChecker)
	if !ok {
		return ManagedRepairSource{}, network.ErrNotReady
	}
	current := n.core.CurrentCluster()
	current.Members = append([]quepaxa.Member(nil), current.Members...)
	abortSlot, _, _ := n.core.LastReconfigurationAbort()
	_, pending := n.core.PendingReconfiguration()
	if n.archive == nil || current.ConfigID != r.ExpectedConfigID || abortSlot != r.ExpectedAbortSlot || pending {
		return ManagedRepairSource{}, fmt.Errorf("prepared repair source config/revision changed: %w", network.ErrRequestConflict)
	}
	var oldMember quepaxa.Member
	oldFound := false
	for _, member := range current.Members {
		if member.ID == r.OldNodeID {
			oldMember, oldFound = member, true
			break
		}
	}
	if !oldFound || r.OldWALIdentity == "" {
		return ManagedRepairSource{}, fmt.Errorf("prepared repair source voter missing: %w", network.ErrRequestConflict)
	}
	registered, err := n.managedVoterWALIdentityLocked(ctx, current.ConfigID, abortSlot, oldMember)
	if err != nil {
		return ManagedRepairSource{}, err
	}
	if registered != r.OldWALIdentity {
		return ManagedRepairSource{}, fmt.Errorf("prepared repair source WAL changed: %w", network.ErrRequestConflict)
	}
	var readIndex quepaxa.Slot
	if r.AddOnly {
		readIndex, _, err = n.core.ReadIndex(ctx)
	} else {
		readIndex, _, err = n.core.ReadIndexExcluding(ctx, r.OldNodeID)
	}
	if err != nil {
		return ManagedRepairSource{}, fmt.Errorf("verify current membership quorum: %w", err)
	}
	if err := n.core.WaitTip(ctx, readIndex); err != nil {
		return ManagedRepairSource{}, fmt.Errorf("wait for current ADD-first quorum tip: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return ManagedRepairSource{}, err
	}
	currentAfterQuorum := n.core.CurrentCluster()
	currentAfterQuorum.Members = append([]quepaxa.Member(nil), currentAfterQuorum.Members...)
	abortAfterQuorum, _, _ := n.core.LastReconfigurationAbort()
	_, pendingAfterQuorum := n.core.PendingReconfiguration()
	if !reflect.DeepEqual(currentAfterQuorum, current) || abortAfterQuorum != abortSlot || pendingAfterQuorum {
		return ManagedRepairSource{}, fmt.Errorf("prepared repair source changed after quorum read: %w", network.ErrRequestConflict)
	}
	if err := checker.ValidateManagedRepairPreparation(ctx, n.managedStartup.Identity, r, current, abortSlot); err != nil {
		return ManagedRepairSource{}, err
	}
	registered, err = n.managedVoterWALIdentityLocked(ctx, current.ConfigID, abortSlot, oldMember)
	if err != nil {
		return ManagedRepairSource{}, err
	}
	if registered != r.OldWALIdentity {
		return ManagedRepairSource{}, fmt.Errorf("prepared repair source WAL changed after quorum read: %w", network.ErrRequestConflict)
	}
	tip := n.core.Tip()
	if tip <= r.ExpectedAbortSlot {
		return ManagedRepairSource{}, fmt.Errorf("prepared repair source tip precedes enrollment revision: %w", network.ErrRequestConflict)
	}
	if err := n.archive.SyncThrough(ctx, n.core, tip); err != nil {
		return ManagedRepairSource{}, fmt.Errorf("sync fixed ADD-first source: %w", err)
	}
	prefix := path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID))
	bootstrap := quepaxa.Cluster{ConfigID: 1, Members: n.config.Members}
	var view *recovery.ReadOnlyView
	if isManagedAppendOnlyProtocol(n.managedApplicationProtocol()) {
		view, err = recovery.OpenReadOnlyViewForApplicationProtocol(ctx, n.bucket, prefix, 1, bootstrap, n.materializationPolicy(), n.receiptWindow(), n.managedApplicationProtocol(), n.config.EnableReconfiguration)
	} else {
		view, err = recovery.OpenReadOnlyViewForPolicyWithReconfiguration(ctx, n.bucket, prefix, 1, bootstrap, n.materializationPolicy(), n.receiptWindow(), n.config.EnableReconfiguration)
	}
	if err != nil {
		return ManagedRepairSource{}, fmt.Errorf("verify fixed ADD-first source: %w", err)
	}
	if err := checker.ValidateManagedRepairPreparation(ctx, n.managedStartup.Identity, r, current, abortSlot); err != nil {
		return ManagedRepairSource{}, err
	}
	corePrefix, prefixOK := n.core.PrefixHash(tip)
	if view.Tip() != tip || !prefixOK || view.PrefixHash() != corePrefix {
		return ManagedRepairSource{}, fmt.Errorf("prepared repair source prefix differs from live source: %w", network.ErrRequestConflict)
	}
	currentAfterArchive := n.core.CurrentCluster()
	currentAfterArchive.Members = append([]quepaxa.Member(nil), currentAfterArchive.Members...)
	abortAfterArchive, _, _ := n.core.LastReconfigurationAbort()
	_, pendingAfterArchive := n.core.PendingReconfiguration()
	registered, registrationErr := n.managedVoterWALIdentityLocked(ctx, current.ConfigID, abortSlot, oldMember)
	if registrationErr != nil {
		return ManagedRepairSource{}, registrationErr
	}
	if !reflect.DeepEqual(currentAfterArchive, current) || abortAfterArchive != abortSlot || pendingAfterArchive || n.core.Tip() < tip || registered != r.OldWALIdentity {
		return ManagedRepairSource{}, fmt.Errorf("prepared repair source changed after archive sync: %w", network.ErrRequestConflict)
	}
	preparedConfig := current
	return ManagedRepairSource{Head: view.CapturedHead(), HeadHash: view.HeadHash(), Survivors: current, PreparedConfig: &preparedConfig, PreparedSlot: tip, PreparedHash: view.PrefixHash()}, nil
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

// VerifyManagedRepairAddition revalidates the durable ADD journal and its
// completed terminal after a later REMOVE may have advanced the live roster.
// The returned Current is the exact ADD target, not the newer live cluster.
func (n *Node) VerifyManagedRepairAddition(ctx context.Context, request ManagedMembershipRequest) (ManagedRepairCompletion, error) {
	if n == nil || request.Action != ManagedMembershipAdd || request.Addition == nil || request.Addition.WALIdentity == "" || request.HandoffHeadHash == "" {
		return ManagedRepairCompletion{}, network.ErrInvalidRequest
	}
	n.shutdownMu.Lock()
	defer n.shutdownMu.Unlock()
	if err := n.managedRepairReady(); err != nil {
		return ManagedRepairCompletion{}, err
	}
	if err := ctx.Err(); err != nil {
		return ManagedRepairCompletion{}, err
	}
	change := request.change(string(n.config.ClusterID))
	data, err := readVoterRegistration(ctx, n.bucket, n.membershipOperationKey(change))
	if err != nil {
		return ManagedRepairCompletion{}, fmt.Errorf("read managed ADD journal: %w", err)
	}
	var operation membershipOperation
	if len(data) == 0 || json.Unmarshal(data, &operation) != nil || !reflect.DeepEqual(operation.Request, change) || operation.Request.Add == nil || *operation.Request.Add != *request.Addition {
		return ManagedRepairCompletion{}, network.ErrRequestConflict
	}
	if operation.Target.ConfigID != request.ExpectedConfigID+1 {
		return ManagedRepairCompletion{}, network.ErrRequestConflict
	}
	terminal, err := n.retainedMembershipTerminal(operation)
	if err != nil {
		return ManagedRepairCompletion{}, err
	}
	// The terminal decision is certified by the pre-transition configuration;
	// the target configuration becomes active at the following slot.
	if !reflect.DeepEqual(n.core.ClusterForSlot(terminal.Slot+1), operation.Target) {
		return ManagedRepairCompletion{}, network.ErrRequestConflict
	}
	current := n.core.CurrentCluster()
	if !reflect.DeepEqual(current, operation.Target) {
		if current.ConfigID != operation.Target.ConfigID+1 || !repairTargetAfterRemoval(current, operation.Target, request.OldNodeID) {
			return ManagedRepairCompletion{}, network.ErrRequestConflict
		}
	}
	completed, err := n.core.CompleteDecision(ctx, terminal.Slot)
	if err != nil {
		return ManagedRepairCompletion{}, fmt.Errorf("managed ADD terminal completion: %w", err)
	}
	if completed.Slot != terminal.Slot || completed.Hash != terminal.Hash || !reflect.DeepEqual(completed.Value, terminal.Value) || !reflect.DeepEqual(completed.Certificate, terminal.Certificate) || !reflect.DeepEqual(n.core.ClusterForSlot(completed.Slot+1), operation.Target) {
		return ManagedRepairCompletion{}, fmt.Errorf("managed ADD terminal or historical target changed")
	}
	operationTarget := operation.Target
	operationTarget.Members = append([]quepaxa.Member(nil), operationTarget.Members...)
	return ManagedRepairCompletion{Current: operationTarget, Terminal: completed}, nil
}

// VerifyManagedRepairRemoval revalidates the exact final REMOVE operation and
// terminal after its status completion CAS, without reopening the closed gate.
func (n *Node) VerifyManagedRepairRemoval(ctx context.Context, request ManagedMembershipRequest) (ManagedRepairSource, error) {
	if n == nil || request.Action != ManagedMembershipRemove || request.Addition != nil || request.RepairOperationID == "" {
		return ManagedRepairSource{}, network.ErrInvalidRequest
	}
	n.shutdownMu.Lock()
	defer n.shutdownMu.Unlock()
	if err := n.managedRepairReady(); err != nil {
		return ManagedRepairSource{}, err
	}
	if err := ctx.Err(); err != nil {
		return ManagedRepairSource{}, err
	}
	change := request.change(string(n.config.ClusterID))
	data, err := readVoterRegistration(ctx, n.bucket, n.membershipOperationKey(change))
	if err != nil {
		return ManagedRepairSource{}, fmt.Errorf("read managed REMOVE journal: %w", err)
	}
	var operation membershipOperation
	if len(data) == 0 || json.Unmarshal(data, &operation) != nil || !reflect.DeepEqual(operation.Request, change) || operation.Request.Remove != request.OldNodeID || operation.Request.Add != nil {
		return ManagedRepairSource{}, network.ErrRequestConflict
	}
	if operation.Target.ConfigID != request.ExpectedConfigID+1 || !reflect.DeepEqual(n.core.CurrentCluster(), operation.Target) {
		return ManagedRepairSource{}, network.ErrRequestConflict
	}
	terminal, err := n.retainedMembershipTerminal(operation)
	if err != nil {
		return ManagedRepairSource{}, err
	}
	// Match the target at its first effective slot, not the terminal decision
	// which is still certified under the prior configuration.
	if !reflect.DeepEqual(n.core.ClusterForSlot(terminal.Slot+1), operation.Target) {
		return ManagedRepairSource{}, network.ErrRequestConflict
	}
	completed, err := n.core.CompleteDecision(ctx, terminal.Slot)
	if err != nil {
		return ManagedRepairSource{}, fmt.Errorf("managed REMOVE terminal completion: %w", err)
	}
	if completed.Slot != terminal.Slot || completed.Hash != terminal.Hash || !reflect.DeepEqual(completed.Value, terminal.Value) || !reflect.DeepEqual(completed.Certificate, terminal.Certificate) || !reflect.DeepEqual(n.core.CurrentCluster(), operation.Target) {
		return ManagedRepairSource{}, fmt.Errorf("managed REMOVE terminal or target changed")
	}
	return ManagedRepairSource{RemoveTerminal: completed, Survivors: operation.Target}, nil
}

func repairTargetAfterRemoval(current, added quepaxa.Cluster, oldID quepaxa.NodeID) bool {
	if current.ConfigID != added.ConfigID+1 {
		return false
	}
	expected := quepaxa.Cluster{ConfigID: current.ConfigID}
	foundOld := false
	for _, member := range added.Members {
		if member.ID == oldID {
			foundOld = true
			continue
		}
		expected.Members = append(expected.Members, member)
	}
	return foundOld && reflect.DeepEqual(current, expected)
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
	var view *recovery.ReadOnlyView
	if isManagedAppendOnlyProtocol(n.managedApplicationProtocol()) {
		view, err = recovery.OpenReadOnlyViewForApplicationProtocol(ctx, n.bucket, prefix, 1, bootstrap, n.materializationPolicy(), n.receiptWindow(), n.managedApplicationProtocol(), n.config.EnableReconfiguration)
	} else {
		view, err = recovery.OpenReadOnlyViewForPolicyWithReconfiguration(ctx, n.bucket, prefix, 1, bootstrap, n.materializationPolicy(), n.receiptWindow(), n.config.EnableReconfiguration)
	}
	if err != nil {
		return ManagedRepairSource{}, err
	}
	if isManagedAppendOnlyProtocol(n.managedApplicationProtocol()) {
		view, err = recovery.OpenReadOnlyViewAtHeadForApplicationProtocol(ctx, n.bucket, prefix, 1, bootstrap, view.CapturedHead(), n.materializationPolicy(), n.receiptWindow(), n.managedApplicationProtocol(), n.config.EnableReconfiguration)
	} else {
		view, err = recovery.OpenReadOnlyViewAtHeadForPolicyWithReconfiguration(ctx, n.bucket, prefix, 1, bootstrap, view.CapturedHead(), n.materializationPolicy(), n.receiptWindow(), n.config.EnableReconfiguration)
	}
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
	if r.AddOnly && r.Action != ManagedMembershipAdd {
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
	err := n.server.ChangeMembership(ctx, change)
	if err != nil && r.Action == ManagedMembershipAdd {
		log.Printf("MANAGED_MEMBERSHIP_FAILURE action=add cause=%s", managedMembershipCauseClass(err))
	}
	return err
}

func managedMembershipCauseClass(err error) string {
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
	if permit.request.OldNodeID == n.config.NodeID && !permit.request.AddOnly {
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
