package operator

import (
	"context"
	"encoding/base64"
	"fmt"
	"path"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

// activeGenerationBinding is the independently resolved trust scope used by
// ordinary membership repair on either the initial or an adopted DR generation.
type activeGenerationBinding struct {
	RootUID           string
	StatefulSetUID    string
	ClusterID         string
	LedgerUID         string
	OperationID       string
	RequestHash       string
	AuthorizationHash string
	CredentialName    string
	CredentialUID     string
	MembershipHash    string
	ProfileHash       string
	StoreIdentityHash string
	Durability        string
	Origin            string
	RecoveryName      string
	RecoveryUID       string
	RecoverySpecHash  string
	DRApproval        *fencing.BootstrapDRApproval
}

func sameActiveGenerationBinding(a, b activeGenerationBinding) bool {
	return a.RootUID == b.RootUID && a.StatefulSetUID == b.StatefulSetUID && a.ClusterID == b.ClusterID && a.LedgerUID == b.LedgerUID &&
		a.OperationID == b.OperationID && a.RequestHash == b.RequestHash && a.AuthorizationHash == b.AuthorizationHash &&
		a.CredentialName == b.CredentialName && a.CredentialUID == b.CredentialUID && a.MembershipHash == b.MembershipHash &&
		a.ProfileHash == b.ProfileHash && a.StoreIdentityHash == b.StoreIdentityHash && a.Durability == b.Durability && a.Origin == b.Origin &&
		a.RecoveryName == b.RecoveryName && a.RecoveryUID == b.RecoveryUID && a.RecoverySpecHash == b.RecoverySpecHash
}

// resolveActiveGenerationBinding joins the current root/workload projection to
// either the immutable initial bootstrap pin or the exact completed DR child
// retained by automatic control. It deliberately does not scan ledgers for a
// plausible DR operation.
func (e *FencingExecutor) resolveActiveGenerationBinding(ctx context.Context, binding peerClosureBinding, source string) (activeGenerationBinding, error) {
	var resolved activeGenerationBinding
	if source == "" || source != binding.root.Status.ActiveClusterID {
		return resolved, fmt.Errorf("active generation source %q differs from root status %q", source, binding.root.Status.ActiveClusterID)
	}
	if err := e.controller().storeConfigMatches(binding.environment); err != nil {
		return resolved, fmt.Errorf("active generation workload storage differs from configured operator storage")
	}
	state, _, err := e.controller().loadAuto(ctx, e.controller().autoKey(&binding.root))
	if err != nil || state.BindingUID != binding.rootUID || state.StatefulSetUID != binding.workloadUID || state.ActiveClusterID != source {
		return resolved, fmt.Errorf("active generation automatic control does not match")
	}
	resolved.RootUID, resolved.StatefulSetUID, resolved.ClusterID = binding.rootUID, binding.workloadUID, source
	if source == binding.root.Spec.AdoptClusterID {
		pin, err := e.initBootstrapPin(ctx, binding, source)
		if err != nil {
			return activeGenerationBinding{}, err
		}
		resolved.LedgerUID, resolved.OperationID, resolved.RequestHash = pin.LedgerUID, pin.OperationID, pin.RequestHash
		resolved.AuthorizationHash, resolved.CredentialName, resolved.CredentialUID = pin.AuthorizationHash, pin.CredentialName, pin.CredentialUID
		resolved.MembershipHash, resolved.ProfileHash = pin.MembershipHash, pin.ProfileHash
		resolved.StoreIdentityHash = fencing.BootstrapStoreIdentityHash(StoreIdentity(binding.environment))
		resolved.Durability = binding.environment["RHIZA_OBJSTORE_DURABILITY"]
		if resolved.Durability == "" {
			resolved.Durability = "async"
		}
		resolved.Origin = "initial"
		return resolved, nil
	}
	completed := state.Completed
	if completed == nil || completed.ClusterID != source || completed.RecoveryName == "" || completed.RecoveryUID == "" || completed.SpecHash == "" {
		return activeGenerationBinding{}, fmt.Errorf("active DR completion pointer is missing or differs")
	}
	var child Resource
	if err := e.Kube.Get(ctx, e.controller().resourcePath(completed.RecoveryName), &child); err != nil {
		return activeGenerationBinding{}, fmt.Errorf("read retained DR child failed")
	}
	if str(child.Metadata["name"]) != completed.RecoveryName || str(child.Metadata["uid"]) != completed.RecoveryUID || hashJSON(child.Spec) != completed.SpecHash ||
		str(nested(child.Metadata, "annotations", automaticOwner)) != binding.rootUID || str(nested(child.Metadata, "annotations", automaticLogical)) != binding.root.Spec.LogicalID ||
		child.Status.Stage != "Complete" || child.Status.Target != source || child.Status.StatefulSetUID != binding.workloadUID || child.Status.SpecHash != immutableSpecHash(child.Spec) ||
		child.Status.Source == "" || child.Status.Source != child.Spec.SourceClusterID || child.Status.RecoveryID != child.Spec.RecoveryID || child.Status.DRSuccessorPin == nil || child.Status.DRSuccessorPin.LedgerUID == "" || child.Status.DRSuccessorPin.AuthorizationHash == "" || child.Spec.AnchorID == "" {
		return activeGenerationBinding{}, fmt.Errorf("retained DR child identity, completion, or pin differs")
	}
	ledger, ledgerUID, _, err := e.readWithUID(ctx, binding.workloadUID, source)
	if err != nil || ledgerUID != child.Status.DRSuccessorPin.LedgerUID || ledger.Ledger.Bootstrap == nil || ledger.Ledger.Bootstrap.RootUID != binding.rootUID || ledger.Ledger.Bootstrap.StatefulSetUID != binding.workloadUID || ledger.Ledger.SourceClusterID != source {
		return activeGenerationBinding{}, fmt.Errorf("active DR ledger identity differs")
	}
	var selected *fencing.BootstrapOperation
	for id, operation := range ledger.Ledger.Bootstrap.Operations {
		if operation.OperationID != id || operation.Purpose != fencing.BootstrapDRSuccessor || operation.RootUID != binding.rootUID || operation.StatefulSetUID != binding.workloadUID || operation.SourceClusterID != source || operation.AuthorizationHash != child.Status.DRSuccessorPin.AuthorizationHash {
			continue
		}
		if selected != nil {
			return activeGenerationBinding{}, fmt.Errorf("active DR generation has ambiguous retained successor operations")
		}
		copy := operation
		selected = &copy
	}
	if selected == nil || selected.OperationID == "" || selected.AuthorizationHash != child.Status.DRSuccessorPin.AuthorizationHash {
		return activeGenerationBinding{}, fmt.Errorf("active DR successor operation does not match the retained child pin")
	}
	var assignment *fencing.BootstrapAssignment
	for uid, candidate := range selected.Assignments {
		if candidate.Validate() != nil || uid != candidate.PodUID || candidate.Purpose != fencing.BootstrapDRSuccessor || candidate.OperationID != selected.OperationID || candidate.RequestHash != selected.RequestHash || candidate.AuthorizationHash != selected.AuthorizationHash || candidate.RootUID != binding.rootUID || candidate.StatefulSetUID != binding.workloadUID || candidate.SourceClusterID != source || candidate.LedgerUID != ledgerUID || fencing.BootstrapDRAuthorizationHash(candidate) != selected.AuthorizationHash {
			return activeGenerationBinding{}, fmt.Errorf("active DR assignment differs from retained operation")
		}
		if assignment == nil {
			copy := candidate
			assignment = &copy
		} else if assignment.CredentialName != candidate.CredentialName || assignment.CredentialUID != candidate.CredentialUID || assignment.MembershipHash != candidate.MembershipHash || assignment.ProfileHash != candidate.ProfileHash || assignment.StoreIdentityHash != candidate.StoreIdentityHash || assignment.Durability != candidate.Durability {
			return activeGenerationBinding{}, fmt.Errorf("active DR assignments disagree on generation profile")
		}
	}
	if assignment == nil || child.Status.SecretName != assignment.CredentialName || child.Status.AnchorRequest == nil || child.Status.AnchorReceipt == nil || assignment.DRApproval.Request != *child.Status.AnchorRequest || assignment.DRApproval.Receipt != *child.Status.AnchorReceipt {
		return activeGenerationBinding{}, fmt.Errorf("active DR child status does not retain the exact approval and credentials")
	}
	request := *child.Status.AnchorRequest
	if request.AnchorID != child.Spec.AnchorID || request.OperationID != e.controller().operationID(&child) || request.SourceClusterID != child.Spec.SourceClusterID || request.TargetClusterID != source || request.StatefulSetUID != binding.workloadUID ||
		path.Clean(request.SourcePrefix) != path.Join(e.Prefix, child.Spec.SourceClusterID) || path.Clean(request.TargetPrefix) != path.Join(e.Prefix, source) || selected.OperationID != drSuccessorOperationID(request.OperationID, binding.workloadUID, source) {
		return activeGenerationBinding{}, fmt.Errorf("active DR child and retained anchor request provenance differ")
	}
	wantAnchor, anchorErr := expectedAnchorBinding(e.AnchorBindingConfig, e.AnchorStorageID, source)
	if e.Anchor == nil || anchorErr != nil || child.Status.AnchorReceipt.Target != wantAnchor || child.Status.AnchorRequest.TargetClusterID != source || child.Status.AnchorRequest.StatefulSetUID != binding.workloadUID {
		return activeGenerationBinding{}, fmt.Errorf("active DR admission target differs from configured storage binding")
	}
	if err := e.Anchor.VerifyAdmission(ctx, *child.Status.AnchorRequest, *child.Status.AnchorReceipt, wantAnchor, child.Status.AnchorReceipt.Generation); err != nil {
		return activeGenerationBinding{}, fmt.Errorf("active DR admission is not current: %w", err)
	}
	var secret object
	if err := e.Kube.Get(ctx, e.controller().corePath("secrets", assignment.CredentialName), &secret); err != nil || str(nested(secret, "metadata", "uid")) != assignment.CredentialUID || secret["immutable"] != true || str(nested(secret, "metadata", "annotations", ownerAnnotation)) != e.controller().operationID(&child) {
		return activeGenerationBinding{}, fmt.Errorf("active DR credential identity differs")
	}
	membersData, err := base64.StdEncoding.DecodeString(str(nested(secret, "data", "members")))
	if err != nil {
		return activeGenerationBinding{}, fencing.ErrPending
	}
	members, err := recoveryMembers(string(membersData))
	if err != nil || fencing.BootstrapMembersHash(members) != assignment.MembershipHash {
		return activeGenerationBinding{}, fmt.Errorf("active DR credential membership differs: err=%v", err)
	}
	var sts object
	if err := e.Kube.Get(ctx, e.controller().stsPath(binding.root.Spec.StatefulSet), &sts); err != nil || str(nested(sts, "metadata", "uid")) != binding.workloadUID || prepareProfileHash(sts) != assignment.ProfileHash {
		return activeGenerationBinding{}, fmt.Errorf("active DR StatefulSet profile differs")
	}
	if assignment.StoreIdentityHash != fencing.BootstrapStoreIdentityHash(StoreIdentity(binding.environment)) || assignment.Durability != child.Spec.Durability {
		return activeGenerationBinding{}, fmt.Errorf("active DR storage or durability differs")
	}
	expectedMembership := recovery.NewMembershipRecord(source, members, assignment.Durability)
	if request.TargetMembership != expectedMembership || selected.RequestHash != hashJSON(struct {
		OperationID, RecoveryUID, StatefulSetUID, TargetClusterID  string
		CredentialName, CredentialUID, MembershipHash, ProfileHash string
	}{selected.OperationID, completed.RecoveryUID, binding.workloadUID, source, assignment.CredentialName, assignment.CredentialUID, assignment.MembershipHash, assignment.ProfileHash}) {
		return activeGenerationBinding{}, fmt.Errorf("active DR authorization request hash or target membership differs")
	}
	resolved.LedgerUID, resolved.OperationID, resolved.RequestHash = ledgerUID, selected.OperationID, selected.RequestHash
	resolved.AuthorizationHash, resolved.CredentialName, resolved.CredentialUID = selected.AuthorizationHash, assignment.CredentialName, assignment.CredentialUID
	resolved.MembershipHash, resolved.ProfileHash, resolved.StoreIdentityHash = assignment.MembershipHash, assignment.ProfileHash, assignment.StoreIdentityHash
	resolved.Durability, resolved.Origin = assignment.Durability, "dr"
	resolved.RecoveryName, resolved.RecoveryUID, resolved.RecoverySpecHash = completed.RecoveryName, completed.RecoveryUID, completed.SpecHash
	approval := assignment.DRApproval
	resolved.DRApproval = &approval
	return resolved, nil
}

// verifyDRSuccessorChildPin binds init/runtime authorization to either the
// exact in-progress recovery intent (before root adoption changes) or the
// retained completed generation. It never chooses a child by scanning.
func (e *FencingExecutor) verifyDRSuccessorChildPin(ctx context.Context, rootUID, stsUID, source string, assignment fencing.BootstrapAssignment, ledgerUID string) error {
	var roots struct {
		Items []ClusterResource `json:"items"`
	}
	if err := e.Kube.Get(ctx, e.controller().clusterPath(""), &roots); err != nil {
		return err
	}
	for i := range roots.Items {
		root := &roots.Items[i]
		if str(root.Metadata["uid"]) != rootUID || !root.Spec.Automatic || root.Metadata["deletionTimestamp"] != nil || root.Spec.AdoptClusterID == source {
			continue
		}
		state, _, err := e.controller().loadAuto(ctx, e.controller().autoKey(root))
		if err != nil || state.BindingUID != rootUID || state.StatefulSetUID != stsUID {
			return fencing.ErrPending
		}
		var name, uid, specHash string
		inProgress := false
		if state.Intent != nil && state.Intent.Failed == "" && state.Intent.RecoveryName != "" && state.Intent.RecoveryUID != "" {
			name, uid, specHash = state.Intent.RecoveryName, state.Intent.RecoveryUID, state.Intent.SpecHash
			inProgress = true
		} else if state.Completed != nil && state.Completed.ClusterID == source {
			name, uid, specHash = state.Completed.RecoveryName, state.Completed.RecoveryUID, state.Completed.SpecHash
		}
		if name == "" || uid == "" || specHash == "" {
			return fencing.ErrPending
		}
		var child Resource
		if err := e.Kube.Get(ctx, e.controller().resourcePath(name), &child); err != nil || str(child.Metadata["uid"]) != uid || hashJSON(child.Spec) != specHash || child.Spec.AnchorID == "" ||
			str(nested(child.Metadata, "annotations", automaticOwner)) != rootUID || str(nested(child.Metadata, "annotations", automaticLogical)) != root.Spec.LogicalID ||
			child.Status.StatefulSetUID != stsUID || child.Status.Target != source || child.Status.DRSuccessorPin == nil ||
			child.Status.DRSuccessorPin.LedgerUID != ledgerUID || child.Status.DRSuccessorPin.AuthorizationHash != assignment.AuthorizationHash {
			return fencing.ErrPending
		}
		if inProgress && (state.ActiveClusterID != child.Spec.SourceClusterID || child.Status.Source != child.Spec.SourceClusterID || state.Intent.Request.SourceClusterID != child.Spec.SourceClusterID || child.Status.Target != source) {
			return fencing.ErrPending
		}
		return nil
	}
	return fencing.ErrPending
}

// retainDRSuccessorPin performs a single status CAS. If the response is lost,
// it accepts only the identical pin already committed to the same child UID and
// specification; it never chooses a new ledger incarnation.
func (c *Controller) retainDRSuccessorPin(ctx context.Context, child *Resource, ledgerUID, authorizationHash string) error {
	if child == nil || str(child.Metadata["uid"]) == "" || resourceName(child) == "" || ledgerUID == "" || authorizationHash == "" {
		return fmt.Errorf("DR successor pin identity is incomplete")
	}
	want := DRSuccessorPin{LedgerUID: ledgerUID, AuthorizationHash: authorizationHash}
	if old := child.Status.DRSuccessorPin; old != nil {
		if *old != want {
			return fmt.Errorf("DR successor child pin changed")
		}
		return nil
	}
	childUID, specHash := str(child.Metadata["uid"]), hashJSON(child.Spec)
	path := c.resourcePath(resourceName(child))
	readExact := func() (Resource, error) {
		var current Resource
		if err := c.Kube.Get(ctx, path, &current); err != nil {
			return Resource{}, err
		}
		if str(current.Metadata["uid"]) != childUID || hashJSON(current.Spec) != specHash ||
			str(nested(current.Metadata, "annotations", automaticOwner)) != str(nested(child.Metadata, "annotations", automaticOwner)) ||
			str(nested(current.Metadata, "annotations", automaticLogical)) != str(nested(child.Metadata, "annotations", automaticLogical)) {
			return Resource{}, fmt.Errorf("DR successor child identity changed before pin")
		}
		return current, nil
	}
	current, err := readExact()
	if err != nil {
		return fmt.Errorf("read exact DR successor before pin: %w", err)
	}
	if old := current.Status.DRSuccessorPin; old != nil {
		if *old != want {
			return fmt.Errorf("DR successor child has a conflicting ledger pin")
		}
		child.Status.DRSuccessorPin = old
		return nil
	}
	current.Status.DRSuccessorPin = &want
	var updated Resource
	putErr := c.Kube.Put(ctx, path+"/status", &current, &updated)
	if putErr == nil && str(updated.Metadata["uid"]) == childUID && hashJSON(updated.Spec) == specHash && updated.Status.DRSuccessorPin != nil && *updated.Status.DRSuccessorPin == want {
		child.Status.DRSuccessorPin = updated.Status.DRSuccessorPin
		return nil
	}
	// A timeout or conflict is not permission to re-pin. Read the exact resource
	// once and accept only the same write if the API committed it.
	observed, readErr := readExact()
	if readErr == nil && observed.Status.DRSuccessorPin != nil && *observed.Status.DRSuccessorPin == want {
		child.Status.DRSuccessorPin = observed.Status.DRSuccessorPin
		return nil
	}
	if putErr != nil {
		return fmt.Errorf("retain DR successor child pin: %w", putErr)
	}
	if readErr != nil {
		return fmt.Errorf("DR successor pin write was not confirmed: %w", readErr)
	}
	return fmt.Errorf("DR successor pin write did not retain the exact ledger identity")
}
