package operator

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/mrchypark/rhiza/pkg/recoveryanchor"
)

const prepareInitContainerName = "rhiza-prepare"

func (e *FencingExecutor) bootstrapCaller(ctx context.Context, authenticatedName string, id fencing.Identity, operationID string) (object, string, string, error) {
	if operationID == "" || id.PodUID == "" || id.SourceClusterID == "" || id.NodeID == "" {
		return nil, "", "", fmt.Errorf("fencing: incomplete bootstrap caller identity")
	}
	var pod object
	if err := e.Kube.Get(ctx, e.controller().corePath("pods", authenticatedName), &pod); err != nil {
		return nil, "", "", err
	}
	if str(nested(pod, "metadata", "name")) != authenticatedName || str(nested(pod, "metadata", "uid")) != id.PodUID || str(nested(pod, "metadata", "annotations", fencingPrepareOperationAnnotation)) != operationID {
		return nil, "", "", fmt.Errorf("fencing: init operation is not bound to the authenticated Pod")
	}
	if id.PodName != "" && id.PodName != authenticatedName {
		return nil, "", "", fmt.Errorf("fencing: bootstrap Pod name differs")
	}
	var stsName, stsUID string
	for _, ref := range list(nested(pod, "metadata", "ownerReferences")) {
		owner := asObject(ref)
		if str(owner["apiVersion"]) == "apps/v1" && str(owner["kind"]) == "StatefulSet" && owner["controller"] == true {
			stsName, stsUID = str(owner["name"]), str(owner["uid"])
			break
		}
	}
	rootUID := str(nested(pod, "metadata", "annotations", fencingRootAnnotation))
	if stsName == "" || stsUID == "" || rootUID == "" {
		return nil, "", "", fmt.Errorf("fencing: managed bootstrap root is unavailable")
	}
	var roots struct {
		Items []ClusterResource `json:"items"`
	}
	if err := e.Kube.Get(ctx, e.controller().clusterPath(""), &roots); err != nil {
		return nil, "", "", err
	}
	rootFound := false
	for _, root := range roots.Items {
		if str(root.Metadata["uid"]) == rootUID && root.Spec.StatefulSet == stsName && root.Spec.Automatic && root.Metadata["deletionTimestamp"] == nil {
			rootFound = true
			break
		}
	}
	if !rootFound {
		return nil, "", "", fmt.Errorf("fencing: bootstrap root binding is unavailable or changed")
	}
	return pod, stsUID, rootUID, nil
}

func (e *FencingExecutor) bootstrapAssignment(ctx context.Context, stsUID, rootUID string, id fencing.Identity, operationID string) (fencing.BootstrapAssignment, fenceLedger, string, error) {
	pin, err := e.bootstrapTrust(ctx, stsUID, rootUID, id.SourceClusterID, operationID)
	if err != nil {
		return fencing.BootstrapAssignment{}, fenceLedger{}, "", err
	}
	state, ledgerUID, _, err := e.readWithUID(ctx, stsUID, id.SourceClusterID)
	if err != nil {
		return fencing.BootstrapAssignment{}, state, "", err
	}
	if err := validateBootstrapLedgerOperation(state.Ledger, pin); err != nil {
		return fencing.BootstrapAssignment{}, state, ledgerUID, err
	}
	status, err := state.Ledger.BootstrapStatus(operationID, id.PodUID)
	if err != nil || status.Assignment == nil {
		return fencing.BootstrapAssignment{}, state, ledgerUID, fencing.ErrPending
	}
	a := *status.Assignment
	if a.RootUID != rootUID || a.StatefulSetUID != stsUID || a.SourceClusterID != id.SourceClusterID || a.PodName != fencing.PodNameForIdentity(id) || a.NodeID != id.NodeID || a.PodUID != id.PodUID || a.LedgerUID != ledgerUID || a.OperationID != pin.OperationID || a.RequestHash != pin.RequestHash || a.AuthorizationHash != pin.AuthorizationHash || a.CredentialName != pin.CredentialName || a.CredentialUID != pin.CredentialUID || a.MembershipHash != pin.MembershipHash || a.ProfileHash != pin.ProfileHash {
		return fencing.BootstrapAssignment{}, state, ledgerUID, fmt.Errorf("fencing: retained bootstrap assignment differs from authenticated Pod")
	}
	return a, state, ledgerUID, nil
}

func validateBootstrapLedgerOperation(ledger fencing.Ledger, pin bootstrapLedgerPin) error {
	if ledger.Bootstrap == nil || ledger.Bootstrap.RootUID != pin.RootUID || ledger.Bootstrap.StatefulSetUID != pin.StatefulSetUID {
		return fencing.ErrPending
	}
	operation, ok := ledger.Bootstrap.Operations[pin.OperationID]
	if !ok || operation.OperationID != pin.OperationID || operation.RequestHash != pin.RequestHash ||
		operation.AuthorizationHash != pin.AuthorizationHash || operation.Purpose != fencing.BootstrapInitial ||
		operation.RootUID != pin.RootUID || operation.StatefulSetUID != pin.StatefulSetUID || operation.SourceClusterID != pin.SourceClusterID {
		return fencing.ErrPending
	}
	for uid, assignment := range operation.Assignments {
		if assignment.Validate() != nil || uid != assignment.PodUID || assignment.OperationID != pin.OperationID ||
			assignment.RequestHash != pin.RequestHash || assignment.AuthorizationHash != pin.AuthorizationHash ||
			assignment.Purpose != operation.Purpose || assignment.RootUID != pin.RootUID ||
			assignment.StatefulSetUID != pin.StatefulSetUID || assignment.SourceClusterID != pin.SourceClusterID ||
			assignment.NodeID != assignment.PodName || assignment.CredentialName != pin.CredentialName ||
			assignment.CredentialUID != pin.CredentialUID || assignment.MembershipHash != pin.MembershipHash ||
			assignment.ProfileHash != pin.ProfileHash || assignment.LedgerUID != pin.LedgerUID {
			return fencing.ErrPending
		}
	}
	for attemptID, attempt := range operation.Attempts {
		assignment, ok := operation.Assignments[attempt.Assignment.PodUID]
		if !ok || attemptID == "" || attempt.AttemptID != attemptID || attempt.Assignment != assignment || attempt.Scope.ValidateFor(assignment) != nil || len(attempt.Outcomes) > 1 {
			return fencing.ErrPending
		}
		for _, outcome := range attempt.Outcomes {
			if outcome.ValidateForAssignment(assignment) != nil {
				return fencing.ErrPending
			}
		}
		if attempt.Terminal != nil && (attempt.Terminal.ContainerID != attempt.Scope.ContainerID || attempt.Terminal.ImageID != attempt.Scope.ImageID || attempt.Terminal.Reason == "") {
			return fencing.ErrPending
		}
	}
	return nil
}

// drSuccessorAssignment performs a fresh authority read for a DR assignment.
// Unlike initial bootstrap, DR authorization is the exact committed recovery
// anchor request/receipt pair carried by the assignment. The fencer's storage
// ID is separately configured; it is never derived from the archive prefix or
// the opaque StoreIdentity digest.
func (e *FencingExecutor) drSuccessorAssignment(ctx context.Context, stsUID, rootUID string, id fencing.Identity, operationID string) (fencing.BootstrapAssignment, fenceLedger, string, error) {
	if e.Anchor == nil || e.AnchorStorageID == "" || id.SourceClusterID == "" {
		return fencing.BootstrapAssignment{}, fenceLedger{}, "", fencing.ErrPending
	}
	state, ledgerUID, _, err := e.readWithUID(ctx, stsUID, id.SourceClusterID)
	if err != nil || ledgerUID == "" || state.Ledger.Bootstrap == nil || state.Ledger.Bootstrap.RootUID != rootUID || state.Ledger.Bootstrap.StatefulSetUID != stsUID {
		return fencing.BootstrapAssignment{}, fenceLedger{}, "", fencing.ErrPending
	}
	operation, ok := state.Ledger.Bootstrap.Operations[operationID]
	if !ok || operation.Purpose != fencing.BootstrapDRSuccessor || operation.OperationID != operationID || operation.RootUID != rootUID || operation.StatefulSetUID != stsUID || operation.SourceClusterID != id.SourceClusterID || operation.AdmissionClosed || operation.Complete {
		return fencing.BootstrapAssignment{}, state, ledgerUID, fencing.ErrPending
	}
	assignment, ok := operation.Assignments[id.PodUID]
	if !ok || assignment.Validate() != nil || assignment.Purpose != fencing.BootstrapDRSuccessor || assignment.OperationID != operationID || assignment.RootUID != rootUID || assignment.StatefulSetUID != stsUID || assignment.SourceClusterID != id.SourceClusterID || assignment.PodName != fencing.PodNameForIdentity(id) || assignment.PodUID != id.PodUID || assignment.NodeID != id.NodeID || assignment.LedgerUID != ledgerUID || assignment.RequestHash != operation.RequestHash || assignment.AuthorizationHash != operation.AuthorizationHash || fencing.BootstrapDRAuthorizationHash(assignment) != operation.AuthorizationHash {
		return fencing.BootstrapAssignment{}, state, ledgerUID, fencing.ErrPending
	}
	request, receipt := assignment.DRApproval.Request, assignment.DRApproval.Receipt
	if request.TargetClusterID != id.SourceClusterID || receipt.Target.ClusterID != id.SourceClusterID {
		return fencing.BootstrapAssignment{}, state, ledgerUID, fencing.ErrPending
	}
	want := recoveryanchor.Binding{ClusterID: id.SourceClusterID, StorageID: e.AnchorStorageID}
	if err := e.Anchor.VerifyAdmission(ctx, request, receipt, want, receipt.Generation); err != nil {
		return fencing.BootstrapAssignment{}, state, ledgerUID, fmt.Errorf("fencing: DR successor anchor admission is not current: %w", err)
	}
	var secret object
	if err := e.Kube.Get(ctx, e.controller().corePath("secrets", assignment.CredentialName), &secret); err != nil || str(nested(secret, "metadata", "uid")) != assignment.CredentialUID || secret["immutable"] != true || str(nested(secret, "metadata", "annotations", ownerAnnotation)) != request.OperationID {
		return fencing.BootstrapAssignment{}, state, ledgerUID, fencing.ErrPending
	}
	membersData, err := base64.StdEncoding.DecodeString(str(nested(secret, "data", "members")))
	if err != nil {
		return fencing.BootstrapAssignment{}, state, ledgerUID, fencing.ErrPending
	}
	members, err := recoveryMembers(string(membersData))
	if err != nil || hashJSON(members) != assignment.MembershipHash {
		return fencing.BootstrapAssignment{}, state, ledgerUID, fencing.ErrPending
	}
	return assignment, state, ledgerUID, nil
}

func (e *FencingExecutor) bootstrapTrust(ctx context.Context, stsUID, rootUID, source, operationID string) (bootstrapLedgerPin, error) {
	if e.Bucket == nil {
		return bootstrapLedgerPin{}, fencing.ErrPending
	}
	var roots struct {
		Items []ClusterResource `json:"items"`
	}
	if err := e.Kube.Get(ctx, e.controller().clusterPath(""), &roots); err != nil {
		return bootstrapLedgerPin{}, err
	}
	var root *ClusterResource
	for i := range roots.Items {
		item := &roots.Items[i]
		if str(item.Metadata["uid"]) == rootUID && item.Spec.Automatic && item.Metadata["deletionTimestamp"] == nil {
			root = item
			break
		}
	}
	if root == nil {
		return bootstrapLedgerPin{}, fencing.ErrPending
	}
	normalizeClusterSpec(root)
	if root.Spec.AdoptClusterID != source || root.Spec.StatefulSet == "" {
		return bootstrapLedgerPin{}, fencing.ErrPending
	}
	var sts object
	if err := e.Kube.Get(ctx, e.controller().stsPath(root.Spec.StatefulSet), &sts); err != nil {
		return bootstrapLedgerPin{}, err
	}
	if str(nested(sts, "metadata", "uid")) != stsUID || str(nested(sts, "metadata", "deletionTimestamp")) != "" {
		return bootstrapLedgerPin{}, fencing.ErrPending
	}
	expected := expectedBootstrapCandidate(root, sts)
	if operationID != bootstrapOperationID(rootUID, stsUID, expected) {
		return bootstrapLedgerPin{}, fencing.ErrPending
	}
	reader, err := e.Bucket.Get(ctx, e.controller().bootstrapCandidateKey(root))
	if err != nil {
		return bootstrapLedgerPin{}, fencing.ErrPending
	}
	data, readErr := io.ReadAll(io.LimitReader(reader, 64<<10))
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || len(data) == 64<<10 || !bytes.Equal(data, jsonBytes(expected)) {
		return bootstrapLedgerPin{}, fencing.ErrPending
	}
	pin, err := e.controller().readBootstrapLedgerPin(ctx, root)
	if err != nil || pin.OperationID != operationID || pin.RootUID != rootUID || pin.StatefulSetUID != stsUID || pin.SourceClusterID != source || pin.RequestHash != hashJSON(expected) || pin.ProfileHash != prepareProfileHash(sts) || pin.LedgerUID == "" {
		return bootstrapLedgerPin{}, fencing.ErrPending
	}
	wantAuthorization := hashJSON(struct {
		RootUID, StatefulSetUID, SourceClusterID, CredentialName, CredentialUID, MembershipHash, ProfileHash string
	}{rootUID, stsUID, source, pin.CredentialName, pin.CredentialUID, pin.MembershipHash, pin.ProfileHash})
	if pin.AuthorizationHash != wantAuthorization || str(nested(sts, "spec", "podManagementPolicy")) != "Parallel" {
		return bootstrapLedgerPin{}, fencing.ErrPending
	}
	var secret object
	if err := e.Kube.Get(ctx, e.controller().corePath("secrets", pin.CredentialName), &secret); err != nil || str(nested(secret, "metadata", "uid")) != pin.CredentialUID || secret["immutable"] != true {
		return bootstrapLedgerPin{}, fencing.ErrPending
	}
	owner := &Resource{Metadata: object{"uid": rootUID}, Spec: Spec{RecoveryID: root.Spec.LogicalID}, Status: Status{Target: source}}
	if str(nested(secret, "metadata", "annotations", ownerAnnotation)) != e.controller().operationID(owner) {
		return bootstrapLedgerPin{}, fencing.ErrPending
	}
	membersData, err := base64.StdEncoding.DecodeString(str(nested(secret, "data", "members")))
	if err != nil {
		return bootstrapLedgerPin{}, fencing.ErrPending
	}
	members, err := recoveryMembers(string(membersData))
	if err != nil || hashJSON(members) != pin.MembershipHash {
		return bootstrapLedgerPin{}, fencing.ErrPending
	}
	return pin, nil
}

func (e *FencingExecutor) initBootstrapPin(ctx context.Context, binding peerClosureBinding, source string) (bootstrapLedgerPin, error) {
	var sts object
	if err := e.Kube.Get(ctx, e.controller().stsPath(binding.root.Spec.StatefulSet), &sts); err != nil {
		return bootstrapLedgerPin{}, err
	}
	stsUID := str(nested(sts, "metadata", "uid"))
	if stsUID != binding.workloadUID || nested(sts, "metadata", "deletionTimestamp") != nil || source != binding.root.Spec.AdoptClusterID {
		return bootstrapLedgerPin{}, fencing.ErrPending
	}
	operationID := bootstrapOperationID(binding.rootUID, stsUID, expectedBootstrapCandidate(&binding.root, sts))
	pin, err := e.bootstrapTrust(ctx, stsUID, binding.rootUID, source, operationID)
	if err != nil {
		return bootstrapLedgerPin{}, err
	}
	state, ledgerUID, _, err := e.readWithUID(ctx, stsUID, source)
	if err != nil || ledgerUID != pin.LedgerUID || state.Ledger.Bootstrap == nil {
		return bootstrapLedgerPin{}, fencing.ErrPending
	}
	operation, ok := state.Ledger.Bootstrap.Operations[pin.OperationID]
	if !ok || operation.Purpose != fencing.BootstrapInitial || !operation.Complete || !operation.AdmissionClosed {
		return bootstrapLedgerPin{}, fencing.ErrPending
	}
	return pin, nil
}

func (e *FencingExecutor) bootstrapStatus(ctx context.Context, _ string, podName string, id fencing.Identity, request fencing.BootstrapStatusRequest) (fencing.BootstrapStatusResult, error) {
	if request.OperationID == "" {
		return e.bootstrapLearnerStatus(ctx, podName, id)
	}
	_, stsUID, rootUID, err := e.bootstrapCaller(ctx, podName, id, request.OperationID)
	if err != nil {
		return fencing.BootstrapStatusResult{}, err
	}
	if state, _, _, readErr := e.readWithUID(ctx, stsUID, id.SourceClusterID); readErr == nil && state.Ledger.Bootstrap != nil {
		if operation, ok := state.Ledger.Bootstrap.Operations[request.OperationID]; ok && operation.Purpose == fencing.BootstrapDRSuccessor {
			assignment, retained, _, assignErr := e.drSuccessorAssignment(ctx, stsUID, rootUID, id, request.OperationID)
			if assignErr != nil {
				return fencing.BootstrapStatusResult{}, assignErr
			}
			status, statusErr := retained.Ledger.BootstrapStatus(request.OperationID, id.PodUID)
			if statusErr != nil || status.Assignment == nil || *status.Assignment != assignment {
				return fencing.BootstrapStatusResult{}, fencing.ErrPending
			}
			return status, nil
		}
	}
	pin, err := e.bootstrapTrust(ctx, stsUID, rootUID, id.SourceClusterID, request.OperationID)
	if err != nil {
		return fencing.BootstrapStatusResult{}, err
	}
	state, _, _, err := e.readWithUID(ctx, stsUID, id.SourceClusterID)
	if err != nil {
		return fencing.BootstrapStatusResult{}, err
	}
	if err := validateBootstrapLedgerOperation(state.Ledger, pin); err != nil {
		return fencing.BootstrapStatusResult{}, err
	}
	result, err := state.Ledger.BootstrapStatus(request.OperationID, id.PodUID)
	if err != nil {
		return fencing.BootstrapStatusResult{}, err
	}
	if result.Assignment != nil {
		a := result.Assignment
		if a.RootUID != rootUID || a.StatefulSetUID != stsUID || a.SourceClusterID != id.SourceClusterID || a.PodName != podName || a.NodeID != id.NodeID || a.PodUID != id.PodUID {
			return fencing.BootstrapStatusResult{}, fmt.Errorf("fencing: retained bootstrap assignment differs from authenticated Pod")
		}
	}
	return result, nil
}

func (e *FencingExecutor) bootstrapRuntimeAdmission(ctx context.Context, podName string, id fencing.Identity, request fencing.BootstrapStatusRequest) (fencing.BootstrapRuntimeAdmissionResult, error) {
	if request.OperationID == "" {
		return fencing.BootstrapRuntimeAdmissionResult{}, fencing.ErrPending
	}
	_, stsUID, rootUID, err := e.bootstrapCaller(ctx, podName, id, request.OperationID)
	if err != nil {
		return fencing.BootstrapRuntimeAdmissionResult{}, err
	}
	assignment, _, ledgerUID, err := e.drSuccessorAssignment(ctx, stsUID, rootUID, id, request.OperationID)
	if err != nil || assignment.Purpose != fencing.BootstrapDRSuccessor {
		return fencing.BootstrapRuntimeAdmissionResult{}, fencing.ErrPending
	}
	if err := e.recordObservedInitTerminal(ctx, id, stsUID); err != nil {
		return fencing.BootstrapRuntimeAdmissionResult{}, err
	}
	// The terminal observer mutates the pinned ledger; reread both the fresh
	// anchor admission and that exact ledger UID before exposing runtime identity.
	current, _, currentLedgerUID, err := e.drSuccessorAssignment(ctx, stsUID, rootUID, id, request.OperationID)
	if err != nil || currentLedgerUID != ledgerUID || current != assignment {
		return fencing.BootstrapRuntimeAdmissionResult{}, fencing.ErrPending
	}
	state, stateLedgerUID, _, err := e.readWithUID(ctx, stsUID, id.SourceClusterID)
	if err != nil || stateLedgerUID != ledgerUID || state.Ledger.Bootstrap == nil {
		return fencing.BootstrapRuntimeAdmissionResult{}, fencing.ErrPending
	}
	status, err := state.Ledger.BootstrapStatus(request.OperationID, id.PodUID)
	if err != nil || status.Assignment == nil || *status.Assignment != assignment {
		return fencing.BootstrapRuntimeAdmissionResult{}, fencing.ErrPending
	}
	identity := fencing.Identity{SourceClusterID: id.SourceClusterID, NodeID: assignment.NodeID, PodUID: assignment.PodUID, RunID: "prebind-runtime-admission"}
	if !state.Ledger.BootstrapInitReady(identity) {
		return fencing.BootstrapRuntimeAdmissionResult{}, fencing.ErrPending
	}
	return fencing.BootstrapRuntimeAdmissionResult{Assignment: assignment}, nil
}

func (e *FencingExecutor) prepareInitCaller(ctx context.Context, podName string, id fencing.Identity) (object, peerClosureBinding, bootstrapLedgerPin, fenceLedger, error) {
	if podName == "" || id.PodUID == "" || id.SourceClusterID == "" || id.NodeID == "" || (id.PodName != "" && id.PodName != podName) {
		return nil, peerClosureBinding{}, bootstrapLedgerPin{}, fenceLedger{}, fmt.Errorf("fencing: incomplete prepare init identity")
	}
	if id.NodeID != podName {
		canonical, err := fencing.ReplacementNodeID(id.PodUID)
		if err != nil || id.NodeID != canonical {
			return nil, peerClosureBinding{}, bootstrapLedgerPin{}, fenceLedger{}, fmt.Errorf("fencing: prepare init identity is not bound to its Pod UID")
		}
	}
	var pod object
	if err := e.Kube.Get(ctx, e.controller().corePath("pods", podName), &pod); err != nil {
		return nil, peerClosureBinding{}, bootstrapLedgerPin{}, fenceLedger{}, err
	}
	if str(nested(pod, "metadata", "name")) != podName || str(nested(pod, "metadata", "uid")) != id.PodUID || nested(pod, "metadata", "deletionTimestamp") != nil {
		return nil, peerClosureBinding{}, bootstrapLedgerPin{}, fenceLedger{}, fencing.ErrPending
	}
	if err := validatePreparePod(pod, id.PodUID); err != nil {
		return nil, peerClosureBinding{}, bootstrapLedgerPin{}, fenceLedger{}, err
	}
	refs := list(nested(pod, "metadata", "ownerReferences"))
	if len(refs) != 1 {
		return nil, peerClosureBinding{}, bootstrapLedgerPin{}, fenceLedger{}, fencing.ErrPending
	}
	owner := asObject(refs[0])
	stsName, stsUID := str(owner["name"]), str(owner["uid"])
	rootUID := str(nested(pod, "metadata", "annotations", fencingRootAnnotation))
	if owner["apiVersion"] != "apps/v1" || owner["kind"] != "StatefulSet" || owner["controller"] != true || stsName == "" || stsUID == "" || rootUID == "" {
		return nil, peerClosureBinding{}, bootstrapLedgerPin{}, fenceLedger{}, fmt.Errorf("fencing: prepare init requires the exact StatefulSet controller and root")
	}
	var roots struct {
		Items []ClusterResource `json:"items"`
	}
	if err := e.Kube.Get(ctx, e.controller().clusterPath(""), &roots); err != nil {
		return nil, peerClosureBinding{}, bootstrapLedgerPin{}, fenceLedger{}, err
	}
	var root *ClusterResource
	for i := range roots.Items {
		candidate := &roots.Items[i]
		if str(candidate.Metadata["uid"]) == rootUID && candidate.Spec.StatefulSet == stsName && candidate.Spec.Automatic && candidate.Status.ActiveClusterID == id.SourceClusterID && candidate.Metadata["deletionTimestamp"] == nil {
			if root != nil {
				return nil, peerClosureBinding{}, bootstrapLedgerPin{}, fenceLedger{}, fencing.ErrPending
			}
			root = candidate
		}
	}
	if root == nil || root.Spec.AdoptClusterID != id.SourceClusterID {
		return nil, peerClosureBinding{}, bootstrapLedgerPin{}, fenceLedger{}, fencing.ErrPending
	}
	var sts object
	if err := e.Kube.Get(ctx, e.controller().stsPath(stsName), &sts); err != nil {
		return nil, peerClosureBinding{}, bootstrapLedgerPin{}, fenceLedger{}, err
	}
	if str(nested(sts, "metadata", "uid")) != stsUID || nested(sts, "metadata", "deletionTimestamp") != nil {
		return nil, peerClosureBinding{}, bootstrapLedgerPin{}, fenceLedger{}, fencing.ErrPending
	}
	operationID := str(nested(pod, "metadata", "annotations", fencingPrepareOperationAnnotation))
	if operationID != bootstrapOperationID(rootUID, stsUID, expectedBootstrapCandidate(root, sts)) {
		return nil, peerClosureBinding{}, bootstrapLedgerPin{}, fenceLedger{}, fencing.ErrPending
	}
	pin, err := e.bootstrapTrust(ctx, stsUID, rootUID, id.SourceClusterID, operationID)
	if err != nil {
		return nil, peerClosureBinding{}, bootstrapLedgerPin{}, fenceLedger{}, err
	}
	var main object
	for _, raw := range list(nested(pod, "spec", "containers")) {
		if str(asObject(raw)["name"]) == root.Spec.Container {
			main = asObject(raw)
			break
		}
	}
	if main == nil {
		return nil, peerClosureBinding{}, bootstrapLedgerPin{}, fenceLedger{}, fencing.ErrPending
	}
	env, err := e.controller().environment(ctx, main)
	if err != nil || env["RHIZA_CLUSTER_ID"] != id.SourceClusterID || e.controller().storeConfigMatches(env) != nil {
		return nil, peerClosureBinding{}, bootstrapLedgerPin{}, fenceLedger{}, fencing.ErrPending
	}
	state, ledgerUID, _, err := e.readWithUID(ctx, stsUID, id.SourceClusterID)
	if err != nil || ledgerUID != pin.LedgerUID {
		return nil, peerClosureBinding{}, bootstrapLedgerPin{}, fenceLedger{}, fencing.ErrPending
	}
	identity := id
	identity.PodName = podName
	binding, err := e.peerClosureBindingWithPrepareInit(ctx, identity, stsUID, true)
	if err != nil || binding.rootUID != rootUID {
		return nil, peerClosureBinding{}, bootstrapLedgerPin{}, fenceLedger{}, fencing.ErrPending
	}
	return pod, binding, pin, state, nil
}

func (e *FencingExecutor) bootstrapLearnerStatus(ctx context.Context, podName string, id fencing.Identity) (fencing.BootstrapStatusResult, error) {
	_, binding, pin, state, err := e.prepareInitCaller(ctx, podName, id)
	if err != nil {
		// A clean initial learner has no bootstrap authorization or repair
		// record yet. Return a read-only unassigned result only after rechecking
		// its complete managed workload scope and proving that the retained
		// ledger contains neither kind of authorization history.
		if status, ok := e.unassignedInitialStatus(ctx, podName, id); ok {
			return status, nil
		}
		return fencing.BootstrapStatusResult{}, err
	}
	if validateBootstrapLedgerOperation(state.Ledger, pin) != nil {
		return fencing.BootstrapStatusResult{}, fencing.ErrPending
	}
	status, err := state.Ledger.BootstrapLearnerStatus(id.PodUID)
	if (err != nil || status.Assignment == nil) && state.Ledger.MembershipRepair == nil {
		return fencing.BootstrapStatusResult{AssignmentSetHash: fencing.BootstrapAssignmentSetHash(nil)}, nil
	}
	if err != nil || status.Assignment == nil {
		return fencing.BootstrapStatusResult{}, fencing.ErrPending
	}
	assignment := *status.Assignment
	authorization := state.Ledger.MembershipRepair.Authorization
	if authorization.ExpectedAbortSlot == nil || authorization.BootstrapMembershipHash != pin.MembershipHash || authorization.ProfileHash != pin.ProfileHash || authorization.StoreIdentityHash != fencing.BootstrapStoreIdentityHash(StoreIdentity(binding.environment)) || authorization.Durability != bootstrapDurability(binding.environment) || membershipRepairRequestHash(authorization) != authorization.RequestHash || membershipRepairAuthorizationHash(authorization) != authorization.AuthorizationHash || assignment.Purpose != fencing.BootstrapLearner || assignment.OperationID != membershipRepairAssignmentOperationID(authorization.OperationID, id.PodUID) || assignment.RequestHash != authorization.RequestHash || assignment.AuthorizationHash != authorization.AuthorizationHash || assignment.RootUID != pin.RootUID || assignment.StatefulSetUID != pin.StatefulSetUID || assignment.LedgerUID != pin.LedgerUID || assignment.SourceClusterID != pin.SourceClusterID || assignment.PodName != podName || assignment.NodeID != mustReplacementNodeID(id.PodUID) || assignment.MembershipHash != pin.MembershipHash || assignment.ProfileHash != pin.ProfileHash || assignment.StoreIdentityHash != fencing.BootstrapStoreIdentityHash(StoreIdentity(binding.environment)) || assignment.Durability != bootstrapDurability(binding.environment) {
		return fencing.BootstrapStatusResult{}, fencing.ErrPending
	}
	credentialIdentity := id
	credentialIdentity.NodeID = assignment.NodeID
	credentialIdentity.PodName = assignment.PodName
	credentialIdentity.RunID = ""
	credentialIdentity.WALIdentity = ""
	credentialIdentity.CredentialName = assignment.CredentialName
	credentialIdentity.CredentialUID = assignment.CredentialUID
	registration, err := e.incarnationCredentialForInit(ctx, credentialIdentity, assignment.StatefulSetUID, credentialIdentity, false)
	if err != nil || !bootstrapPublicMemberMatches(assignment.PublicMember, registration.Learner) {
		return fencing.BootstrapStatusResult{}, fencing.ErrPending
	}
	if err := e.verifyMembershipRepairArchive(ctx, binding, fencing.MembershipRepairView{Authorization: authorization, Progress: state.Ledger.MembershipRepair.Progress}); err != nil {
		return fencing.BootstrapStatusResult{}, err
	}
	return status, nil
}

// unassignedInitialStatus authorizes no mutation. It lets a valid mandatory
// init caller distinguish a genuinely fresh, unassigned initial candidate
// from unavailable or ambiguous authority state. Existing bootstrap or repair
// records, including records for another Pod incarnation, always fail closed.
func (e *FencingExecutor) unassignedInitialStatus(ctx context.Context, podName string, id fencing.Identity) (fencing.BootstrapStatusResult, bool) {
	if podName == "" || id.PodName != podName || id.PodUID == "" || id.SourceClusterID == "" || id.RunID != "" || id.WALIdentity != "" || fencing.ValidatePodBinding(id.NodeID, podName, id.PodUID) != nil {
		return fencing.BootstrapStatusResult{}, false
	}
	var pod object
	if err := e.Kube.Get(ctx, e.controller().corePath("pods", podName), &pod); err != nil || str(nested(pod, "metadata", "name")) != podName || str(nested(pod, "metadata", "uid")) != id.PodUID || nested(pod, "metadata", "deletionTimestamp") != nil {
		return fencing.BootstrapStatusResult{}, false
	}
	if err := validatePreparePod(pod, id.PodUID); err != nil {
		return fencing.BootstrapStatusResult{}, false
	}
	refs := list(nested(pod, "metadata", "ownerReferences"))
	if len(refs) != 1 {
		return fencing.BootstrapStatusResult{}, false
	}
	owner := asObject(refs[0])
	stsName, stsUID := str(owner["name"]), str(owner["uid"])
	rootUID := str(nested(pod, "metadata", "annotations", fencingRootAnnotation))
	if owner["apiVersion"] != "apps/v1" || owner["kind"] != "StatefulSet" || owner["controller"] != true || stsName == "" || stsUID == "" || rootUID == "" {
		return fencing.BootstrapStatusResult{}, false
	}
	binding, err := e.peerClosureBindingWithPrepareInit(ctx, id, stsUID, true)
	if err != nil || binding.rootUID != rootUID || binding.root.Spec.AdoptClusterID != id.SourceClusterID || e.controller().storeConfigMatches(binding.environment) != nil {
		return fencing.BootstrapStatusResult{}, false
	}
	var sts object
	if err := e.Kube.Get(ctx, e.controller().stsPath(stsName), &sts); err != nil || str(nested(sts, "metadata", "uid")) != stsUID || nested(sts, "metadata", "deletionTimestamp") != nil {
		return fencing.BootstrapStatusResult{}, false
	}
	operationID := str(nested(pod, "metadata", "annotations", fencingPrepareOperationAnnotation))
	if operationID == "" || operationID != bootstrapOperationID(rootUID, stsUID, expectedBootstrapCandidate(&binding.root, sts)) {
		return fencing.BootstrapStatusResult{}, false
	}
	if e.Bucket == nil || !bootstrapObjectAbsent(ctx, e.Bucket, e.controller().bootstrapCandidateKey(&binding.root)) || !bootstrapObjectAbsent(ctx, e.Bucket, e.controller().bootstrapLedgerPinKey(&binding.root)) {
		return fencing.BootstrapStatusResult{}, false
	}
	state, ledgerUID, _, err := e.readWithUID(ctx, stsUID, id.SourceClusterID)
	if err != nil || ledgerUID == "" || state.Ledger.Bootstrap != nil || state.Ledger.MembershipRepair != nil || len(state.Ledger.MembershipRepairHistory) != 0 {
		return fencing.BootstrapStatusResult{}, false
	}
	return fencing.BootstrapStatusResult{AssignmentSetHash: fencing.BootstrapAssignmentSetHash(nil)}, true
}

func bootstrapObjectAbsent(ctx context.Context, bucket interface {
	Get(context.Context, string) (io.ReadCloser, error)
	IsObjNotFoundErr(error) bool
}, key string) bool {
	reader, err := bucket.Get(ctx, key)
	if err != nil {
		return bucket.IsObjNotFoundErr(err)
	}
	_ = reader.Close()
	return false
}

func mustReplacementNodeID(podUID string) string {
	value, _ := fencing.ReplacementNodeID(podUID)
	return value
}

func bootstrapDurability(env map[string]string) string {
	if env["RHIZA_OBJSTORE_DURABILITY"] == "" {
		return "async"
	}
	return env["RHIZA_OBJSTORE_DURABILITY"]
}

func bootstrapPublicMemberMatches(want fencing.BootstrapPublicMember, data []byte) bool {
	var member quepaxa.Member
	if json.Unmarshal(data, &member) != nil {
		return false
	}
	return want.ID == string(member.ID) && want.URL == member.URL && want.PeerURL == member.PeerURL && want.LogURL == member.LogURL && want.PublicKey == fmt.Sprintf("%x", member.PublicKey[:]) && want.WALIdentity == member.WALIdentity
}

func (e *FencingExecutor) initExecutionScope(ctx context.Context, pod object, assignment fencing.BootstrapAssignment) (fencing.InitExecutionScope, error) {
	var status object
	for _, raw := range list(nested(pod, "status", "initContainerStatuses")) {
		candidate := asObject(raw)
		if str(candidate["name"]) == prepareInitContainerName {
			status = candidate
			break
		}
	}
	if status == nil || nested(status, "state", "running") == nil {
		return fencing.InitExecutionScope{}, fencing.ErrPending
	}
	containerID, imageID := str(status["containerID"]), str(status["imageID"])
	nodeName := str(nested(pod, "spec", "nodeName"))
	if containerID == "" || imageID == "" || nodeName == "" {
		return fencing.InitExecutionScope{}, fencing.ErrPending
	}
	var node object
	if err := e.Kube.Get(ctx, e.controller().corePath("nodes", nodeName), &node); err != nil {
		return fencing.InitExecutionScope{}, err
	}
	scope := fencing.InitExecutionScope{PodName: assignment.PodName, PodUID: assignment.PodUID, ContainerName: prepareInitContainerName, ContainerID: containerID, ImageID: imageID, NodeName: nodeName, NodeUID: str(nested(node, "metadata", "uid"))}
	if err := scope.ValidateFor(assignment); err != nil {
		return fencing.InitExecutionScope{}, err
	}
	return scope, nil
}

func (e *FencingExecutor) beginInit(ctx context.Context, _ string, podName string, id fencing.Identity, request fencing.BeginInitRequest) (fencing.InitAttempt, error) {
	if request.AttemptID == "" || request.Assignment.OperationID == "" {
		return fencing.InitAttempt{}, fmt.Errorf("fencing: incomplete init begin request")
	}
	if request.Assignment.Purpose == fencing.BootstrapLearner {
		return e.beginMembershipRepairInit(ctx, podName, id, request)
	}
	pod, stsUID, rootUID, err := e.bootstrapCaller(ctx, podName, id, request.Assignment.OperationID)
	if err != nil {
		return fencing.InitAttempt{}, err
	}
	if request.Assignment.Purpose == fencing.BootstrapDRSuccessor {
		assignment, _, ledgerUID, assignErr := e.drSuccessorAssignment(ctx, stsUID, rootUID, id, request.Assignment.OperationID)
		if assignErr != nil || assignment != request.Assignment {
			return fencing.InitAttempt{}, fencing.ErrPending
		}
		scope, scopeErr := e.initExecutionScope(ctx, pod, assignment)
		if scopeErr != nil {
			return fencing.InitAttempt{}, scopeErr
		}
		var attempt fencing.InitAttempt
		_, err = e.changeExisting(ctx, stsUID, id.SourceClusterID, ledgerUID, nil, func(l *fencing.Ledger) error {
			var beginErr error
			attempt, beginErr = l.BeginInit(assignment, request.AttemptID, scope)
			return beginErr
		})
		return attempt, err
	}
	assignment, _, ledgerUID, err := e.bootstrapAssignment(ctx, stsUID, rootUID, id, request.Assignment.OperationID)
	if err != nil || assignment != request.Assignment {
		return fencing.InitAttempt{}, fencing.ErrPending
	}
	scope, err := e.initExecutionScope(ctx, pod, assignment)
	if err != nil {
		return fencing.InitAttempt{}, err
	}
	var attempt fencing.InitAttempt
	updated, err := e.changeExisting(ctx, stsUID, id.SourceClusterID, ledgerUID, nil, func(l *fencing.Ledger) error {
		var beginErr error
		attempt, beginErr = l.BeginInit(assignment, request.AttemptID, scope)
		return beginErr
	})
	if err != nil {
		return fencing.InitAttempt{}, err
	}
	if updated.StatefulSetUID != stsUID {
		return fencing.InitAttempt{}, fmt.Errorf("fencing: init ledger binding changed")
	}
	return attempt, nil
}

func (e *FencingExecutor) recordInitOutcome(ctx context.Context, _ string, podName string, id fencing.Identity, request fencing.RecordInitOutcomeRequest) error {
	if err := request.Outcome.ValidateForAssignment(request.Assignment); err != nil || request.AttemptID == "" {
		return fmt.Errorf("fencing: invalid init outcome request")
	}
	if request.Assignment.Purpose == fencing.BootstrapLearner {
		return e.recordMembershipRepairInitOutcome(ctx, podName, id, request)
	}
	pod, stsUID, rootUID, err := e.bootstrapCaller(ctx, podName, id, request.Assignment.OperationID)
	if err != nil {
		return err
	}
	if request.Assignment.Purpose == fencing.BootstrapDRSuccessor {
		assignment, _, ledgerUID, assignErr := e.drSuccessorAssignment(ctx, stsUID, rootUID, id, request.Assignment.OperationID)
		if assignErr != nil || assignment != request.Assignment {
			return fencing.ErrPending
		}
		var found bool
		state, _, _, err := e.readWithUID(ctx, stsUID, id.SourceClusterID)
		if err != nil {
			return err
		}
		for _, attempt := range state.Ledger.Bootstrap.Operations[assignment.OperationID].Attempts {
			if attempt.AttemptID == request.AttemptID && attempt.Assignment == assignment {
				scope, scopeErr := e.initExecutionScope(ctx, pod, assignment)
				if scopeErr != nil || attempt.Scope != scope {
					return fencing.ErrPending
				}
				found = true
				break
			}
		}
		if !found {
			return fencing.ErrPending
		}
		_, err = e.changeExisting(ctx, stsUID, id.SourceClusterID, ledgerUID, nil, func(l *fencing.Ledger) error {
			return l.RecordInitOutcome(assignment, request.AttemptID, request.Outcome)
		})
		return err
	}
	assignment, state, ledgerUID, err := e.bootstrapAssignment(ctx, stsUID, rootUID, id, request.Assignment.OperationID)
	if err != nil || assignment != request.Assignment {
		return fencing.ErrPending
	}
	var found bool
	for _, attempt := range state.Ledger.Bootstrap.Operations[assignment.OperationID].Attempts {
		if attempt.AttemptID == request.AttemptID && attempt.Assignment == assignment {
			scope, scopeErr := e.initExecutionScope(ctx, pod, assignment)
			if scopeErr != nil || attempt.Scope != scope {
				return fencing.ErrPending
			}
			found = true
			break
		}
	}
	if !found {
		return fencing.ErrPending
	}
	_, err = e.changeExisting(ctx, stsUID, id.SourceClusterID, ledgerUID, nil, func(l *fencing.Ledger) error {
		return l.RecordInitOutcome(assignment, request.AttemptID, request.Outcome)
	})
	return err
}

func (e *FencingExecutor) beginMembershipRepairInit(ctx context.Context, podName string, id fencing.Identity, request fencing.BeginInitRequest) (fencing.InitAttempt, error) {
	pod, binding, pin, state, err := e.prepareInitCaller(ctx, podName, id)
	if err != nil {
		return fencing.InitAttempt{}, err
	}
	status, err := e.bootstrapLearnerStatus(ctx, podName, id)
	if err != nil || status.Assignment == nil || *status.Assignment != request.Assignment {
		return fencing.InitAttempt{}, fencing.ErrPending
	}
	scope, err := e.initExecutionScope(ctx, pod, request.Assignment)
	if err != nil {
		return fencing.InitAttempt{}, err
	}
	var attempt fencing.InitAttempt
	updated, err := e.changeExisting(ctx, binding.workloadUID, id.SourceClusterID, pin.LedgerUID, nil, func(ledger *fencing.Ledger) error {
		var beginErr error
		attempt, beginErr = ledger.BeginMembershipRepairInit(request.Assignment, request.AttemptID, scope)
		return beginErr
	})
	if err != nil {
		return fencing.InitAttempt{}, err
	}
	if updated.StatefulSetUID != state.StatefulSetUID || attempt.Assignment != request.Assignment {
		return fencing.InitAttempt{}, fencing.ErrPending
	}
	return attempt, nil
}

func (e *FencingExecutor) recordMembershipRepairInitOutcome(ctx context.Context, podName string, id fencing.Identity, request fencing.RecordInitOutcomeRequest) error {
	pod, binding, pin, _, err := e.prepareInitCaller(ctx, podName, id)
	if err != nil {
		return err
	}
	status, err := e.bootstrapLearnerStatus(ctx, podName, id)
	if err != nil || status.Assignment == nil || *status.Assignment != request.Assignment {
		return fencing.ErrPending
	}
	var attempt *fencing.InitAttempt
	for i := range status.Attempts {
		if status.Attempts[i].AttemptID == request.AttemptID {
			if attempt != nil {
				return fencing.ErrPending
			}
			copy := status.Attempts[i]
			attempt = &copy
		}
	}
	if attempt == nil {
		return fencing.ErrPending
	}
	scope, err := e.initExecutionScope(ctx, pod, request.Assignment)
	if err != nil || attempt.Scope != scope {
		return fencing.ErrPending
	}
	_, err = e.changeExisting(ctx, binding.workloadUID, id.SourceClusterID, pin.LedgerUID, nil, func(ledger *fencing.Ledger) error {
		return ledger.RecordMembershipRepairInitOutcome(request.Assignment, request.AttemptID, request.Outcome)
	})
	return err
}

func (e *FencingExecutor) recordObservedInitTerminal(ctx context.Context, id fencing.Identity, stsUID string) error {
	podName := fencing.PodNameForIdentity(id)
	var pod object
	if err := e.Kube.Get(ctx, e.controller().corePath("pods", podName), &pod); err != nil {
		return err
	}
	operationID := str(nested(pod, "metadata", "annotations", fencingPrepareOperationAnnotation))
	if operationID == "" {
		state, err := e.read(ctx, stsUID, id.SourceClusterID)
		if err == nil && state.Ledger.Bootstrap == nil {
			return nil
		}
		return fencing.ErrPending
	}
	_, actualSTS, rootUID, err := e.bootstrapCaller(ctx, podName, id, operationID)
	if err != nil {
		return err
	}
	if actualSTS != stsUID {
		return fmt.Errorf("fencing: observed DR init StatefulSet UID %q differs from expected %q: %w", actualSTS, stsUID, fencing.ErrPending)
	}
	state, _, _, readErr := e.readWithUID(ctx, actualSTS, id.SourceClusterID)
	if strings.HasPrefix(operationID, "dr-successor-") && readErr != nil {
		return fmt.Errorf("fencing: DR init ledger is unavailable: %w", readErr)
	}
	if readErr == nil && state.Ledger.Bootstrap != nil {
		if operation, ok := state.Ledger.Bootstrap.Operations[operationID]; ok && operation.Purpose == fencing.BootstrapDRSuccessor {
			assignment, retained, ledgerUID, assignErr := e.drSuccessorAssignment(ctx, actualSTS, rootUID, id, operationID)
			if assignErr != nil {
				return assignErr
			}
			status, statusErr := retained.Ledger.BootstrapStatus(operationID, id.PodUID)
			if statusErr != nil || status.Assignment == nil || *status.Assignment != assignment {
				return fmt.Errorf("fencing: DR init terminal assignment is not retained: %w", fencing.ErrPending)
			}
			var found *fencing.InitAttempt
			for i := range status.Attempts {
				if status.Attempts[i].Assignment == assignment {
					if found != nil {
						return fmt.Errorf("fencing: multiple DR init attempts are retained: %w", fencing.ErrPending)
					}
					copy := status.Attempts[i]
					found = &copy
				}
			}
			if found == nil {
				return fmt.Errorf("fencing: DR init attempt is not retained: %w", fencing.ErrPending)
			}
			var terminalStatus object
			for _, raw := range list(nested(pod, "status", "initContainerStatuses")) {
				candidate := asObject(raw)
				if str(candidate["name"]) == prepareInitContainerName {
					terminalStatus = asObject(nested(candidate, "state", "terminated"))
					break
				}
			}
			if terminalStatus == nil {
				return fmt.Errorf("fencing: DR init terminal state is unavailable: %w", fencing.ErrPending)
			}
			terminal := fencing.InitTerminalEvidence{ContainerID: str(terminalStatus["containerID"]), ImageID: str(terminalStatus["imageID"]), ExitCode: int32(number(terminalStatus["exitCode"])), Reason: str(terminalStatus["reason"])}
			if terminal.ContainerID != found.Scope.ContainerID || terminal.ImageID != found.Scope.ImageID || terminal.Reason == "" {
				return fmt.Errorf("fencing: observed DR init terminal differs from live attempt scope: %w", fencing.ErrPending)
			}
			_, err = e.changeExisting(ctx, actualSTS, id.SourceClusterID, ledgerUID, nil, func(l *fencing.Ledger) error {
				return l.RecordInitTerminal(assignment, found.AttemptID, terminal)
			})
			if err != nil {
				return fmt.Errorf("fencing: retain DR init terminal: %w", err)
			}
			return nil
		}
	}
	if strings.HasPrefix(operationID, "dr-successor-") {
		return fmt.Errorf("fencing: DR init operation is not retained: %w", fencing.ErrPending)
	}
	pin, err := e.bootstrapTrust(ctx, actualSTS, rootUID, id.SourceClusterID, operationID)
	if err != nil {
		return err
	}
	state, ledgerUID, _, err := e.readWithUID(ctx, actualSTS, id.SourceClusterID)
	if err != nil || ledgerUID != pin.LedgerUID || validateBootstrapLedgerOperation(state.Ledger, pin) != nil {
		return fencing.ErrPending
	}
	bootstrapStatus, err := state.Ledger.BootstrapStatus(operationID, id.PodUID)
	if err != nil {
		return err
	}
	var assignment fencing.BootstrapAssignment
	var attempt fencing.InitAttempt
	if bootstrapStatus.Assignment != nil {
		assignment, _, _, err = e.bootstrapAssignment(ctx, actualSTS, rootUID, id, operationID)
		if err != nil {
			return err
		}
		found := false
		for _, candidate := range state.Ledger.Bootstrap.Operations[operationID].Attempts {
			if candidate.Assignment == assignment {
				if found {
					return fmt.Errorf("fencing: multiple init attempts exist for one Pod assignment")
				}
				attempt, found = candidate, true
			}
		}
		if !found {
			return fencing.ErrPending
		}
	} else {
		status, statusErr := e.bootstrapLearnerStatus(ctx, podName, id)
		if statusErr != nil || status.Assignment == nil {
			return fencing.ErrPending
		}
		assignment = *status.Assignment
		for _, candidate := range status.Attempts {
			if candidate.Assignment == assignment {
				if attempt.AttemptID != "" {
					return fmt.Errorf("fencing: multiple learner init attempts exist for one Pod assignment")
				}
				attempt = candidate
			}
		}
		if attempt.AttemptID == "" {
			return fencing.ErrPending
		}
	}
	if attempt.Scope.ContainerName != prepareInitContainerName {
		return fencing.ErrPending
	}
	var terminalStatus object
	for _, raw := range list(nested(pod, "status", "initContainerStatuses")) {
		status := asObject(raw)
		if str(status["name"]) == prepareInitContainerName {
			terminalStatus = asObject(nested(status, "state", "terminated"))
			break
		}
	}
	if terminalStatus == nil {
		return fencing.ErrPending
	}
	terminal := fencing.InitTerminalEvidence{ContainerID: str(terminalStatus["containerID"]), ImageID: str(terminalStatus["imageID"]), ExitCode: int32(number(terminalStatus["exitCode"])), Reason: str(terminalStatus["reason"])}
	if terminal.ContainerID != attempt.Scope.ContainerID || terminal.ImageID != attempt.Scope.ImageID || terminal.Reason == "" {
		return fencing.ErrPending
	}
	_, err = e.changeExisting(ctx, actualSTS, id.SourceClusterID, ledgerUID, nil, func(l *fencing.Ledger) error {
		if assignment.Purpose == fencing.BootstrapLearner {
			return l.RecordMembershipRepairInitTerminal(assignment, attempt.AttemptID, terminal)
		}
		return l.RecordInitTerminal(assignment, attempt.AttemptID, terminal)
	})
	return err
}

func (e *FencingExecutor) registerBootstrapAssignment(ctx context.Context, candidate fencing.Identity, stsUID string) (fencing.Registration, error) {
	if candidate.PodName == "" || candidate.NodeID != candidate.PodName || candidate.WALIdentity != "" || candidate.RunID == "" {
		return fencing.Registration{}, fmt.Errorf("fencing: initial bootstrap registration identity differs")
	}
	var pod object
	if err := e.Kube.Get(ctx, e.controller().corePath("pods", candidate.PodName), &pod); err != nil {
		return fencing.Registration{}, err
	}
	operationID := str(nested(pod, "metadata", "annotations", fencingPrepareOperationAnnotation))
	if operationID == "" {
		return fencing.Registration{}, fencing.ErrPending
	}
	_, actualSTS, rootUID, err := e.bootstrapCaller(ctx, candidate.PodName, candidate, operationID)
	if err != nil || actualSTS != stsUID {
		return fencing.Registration{}, fencing.ErrPending
	}
	if state, _, _, readErr := e.readWithUID(ctx, stsUID, candidate.SourceClusterID); readErr == nil && state.Ledger.Bootstrap != nil {
		if operation, ok := state.Ledger.Bootstrap.Operations[operationID]; ok && operation.Purpose == fencing.BootstrapDRSuccessor {
			assignment, _, ledgerUID, assignErr := e.drSuccessorAssignment(ctx, stsUID, rootUID, candidate, operationID)
			if assignErr != nil || assignment.NodeID != candidate.NodeID || assignment.PodName != candidate.PodName || assignment.PodUID != candidate.PodUID ||
				(candidate.CredentialName != "" && candidate.CredentialName != assignment.CredentialName) ||
				(candidate.CredentialUID != "" && candidate.CredentialUID != assignment.CredentialUID) || candidate.WALIdentity != "" {
				return fencing.Registration{}, fencing.ErrPending
			}
			identity := candidate
			// DR restores the existing static voter ID. The bootstrap credential
			// is checked from the immutable Secret above; runtime ledger identities
			// retain the legacy static-member shape (no PodName/credential fields).
			identity.PodName, identity.CredentialName, identity.CredentialUID = "", "", ""
			// Re-read the approved anchor immediately before the ledger mutation.
			// The CAS callback then verifies that the exact assignment and its
			// completed, authority-observed materialization are still retained.
			current, _, currentLedgerUID, currentErr := e.drSuccessorAssignment(ctx, stsUID, rootUID, identity, operationID)
			if currentErr != nil || currentLedgerUID != ledgerUID || current != assignment {
				return fencing.Registration{}, fencing.ErrPending
			}
			_, err := e.changeExisting(ctx, stsUID, candidate.SourceClusterID, ledgerUID, func(l *fencing.Ledger) error {
				if l.Bootstrap == nil {
					return fencing.ErrPending
				}
				retained, ok := l.Bootstrap.Operations[operationID]
				if !ok || retained.Purpose != fencing.BootstrapDRSuccessor || retained.AdmissionClosed || retained.Complete || retained.Assignments[candidate.PodUID] != assignment || !l.BootstrapInitReady(identity) {
					return fencing.ErrPending
				}
				return nil
			}, func(l *fencing.Ledger) error { return l.Register(identity) })
			if err != nil {
				return fencing.Registration{}, err
			}
			return fencing.Registration{Identity: identity}, nil
		}
	}
	assignment, _, ledgerUID, err := e.bootstrapAssignment(ctx, stsUID, rootUID, candidate, operationID)
	if err != nil || assignment.Purpose != fencing.BootstrapInitial || assignment.NodeID != candidate.NodeID {
		return fencing.Registration{}, fencing.ErrPending
	}
	stsName := ""
	for _, raw := range list(nested(pod, "metadata", "ownerReferences")) {
		owner := asObject(raw)
		if owner["controller"] == true && owner["kind"] == "StatefulSet" && owner["uid"] == stsUID {
			stsName = str(owner["name"])
			break
		}
	}
	if stsName == "" {
		return fencing.Registration{}, fencing.ErrPending
	}
	root := ClusterResource{Metadata: map[string]any{"uid": rootUID}, Spec: ClusterSpec{StatefulSet: stsName, AdoptClusterID: candidate.SourceClusterID}}
	// The pinned secret and ledger UID are both re-read before the first WAL
	// registration; neither a replaced Secret nor a replacement ledger adopts it.
	pin, err := e.controller().readBootstrapLedgerPin(ctx, &root)
	if err != nil || pin.LedgerUID != ledgerUID || pin.OperationID != operationID || pin.CredentialUID != assignment.CredentialUID || pin.CredentialName != assignment.CredentialName || pin.MembershipHash != assignment.MembershipHash || pin.ProfileHash != assignment.ProfileHash {
		return fencing.Registration{}, fencing.ErrPending
	}
	var secret object
	if err := e.Kube.Get(ctx, e.controller().corePath("secrets", assignment.CredentialName), &secret); err != nil || str(nested(secret, "metadata", "uid")) != assignment.CredentialUID || secret["immutable"] != true {
		return fencing.Registration{}, fencing.ErrPending
	}
	identity := candidate
	identity.PodName = "" // Initial voters retain their original name-based NodeID.
	identity.CredentialName, identity.CredentialUID = "", ""
	_, err = e.changeExisting(ctx, stsUID, candidate.SourceClusterID, ledgerUID, func(l *fencing.Ledger) error {
		if !l.BootstrapInitReady(identity) {
			return fencing.ErrPending
		}
		return nil
	}, func(l *fencing.Ledger) error { return l.Register(identity) })
	if err != nil {
		return fencing.Registration{}, err
	}
	return fencing.Registration{Identity: identity}, nil
}

func (e *FencingExecutor) bindRuntime(ctx context.Context, stsUID string, identity fencing.Identity) error {
	podName := fencing.PodNameForIdentity(identity)
	var pod object
	if err := e.Kube.Get(ctx, e.controller().corePath("pods", podName), &pod); err != nil {
		return err
	}
	operationID := str(nested(pod, "metadata", "annotations", fencingPrepareOperationAnnotation))
	if operationID == "" {
		_, err := e.change(ctx, stsUID, identity.SourceClusterID, func(l *fencing.Ledger) error { return l.Bind(identity) })
		return err
	}
	_, actualSTS, rootUID, err := e.bootstrapCaller(ctx, podName, identity, operationID)
	if err != nil || actualSTS != stsUID {
		return fencing.ErrPending
	}
	if state, _, _, readErr := e.readWithUID(ctx, stsUID, identity.SourceClusterID); readErr == nil && state.Ledger.Bootstrap != nil {
		if operation, ok := state.Ledger.Bootstrap.Operations[operationID]; ok && operation.Purpose == fencing.BootstrapDRSuccessor {
			assignment, _, ledgerUID, assignErr := e.drSuccessorAssignment(ctx, stsUID, rootUID, identity, operationID)
			if assignErr != nil || (identity.CredentialName != "" && identity.CredentialName != assignment.CredentialName) || (identity.CredentialUID != "" && identity.CredentialUID != assignment.CredentialUID) {
				return fencing.ErrPending
			}
			_, err = e.changeExisting(ctx, stsUID, identity.SourceClusterID, ledgerUID, func(l *fencing.Ledger) error {
				if !l.BootstrapInitReady(identity) {
					return fencing.ErrPending
				}
				return nil
			}, func(l *fencing.Ledger) error { return l.Bind(identity) })
			return err
		}
	}
	pin, err := e.bootstrapTrust(ctx, stsUID, rootUID, identity.SourceClusterID, operationID)
	if err != nil {
		return fencing.ErrPending
	}
	state, ledgerUID, _, err := e.readWithUID(ctx, stsUID, identity.SourceClusterID)
	if err != nil || ledgerUID != pin.LedgerUID || validateBootstrapLedgerOperation(state.Ledger, pin) != nil {
		return fencing.ErrPending
	}
	initialStatus, err := state.Ledger.BootstrapStatus(operationID, identity.PodUID)
	if err != nil {
		return fencing.ErrPending
	}
	if initialStatus.Assignment != nil {
		assignment, _, actualLedgerUID, assignmentErr := e.bootstrapAssignment(ctx, stsUID, rootUID, identity, operationID)
		if assignmentErr != nil || actualLedgerUID != ledgerUID || assignment.NodeID != identity.NodeID || (identity.CredentialName != "" && identity.CredentialName != assignment.CredentialName) || (identity.CredentialUID != "" && identity.CredentialUID != assignment.CredentialUID) {
			return fencing.ErrPending
		}
		_, err = e.changeExisting(ctx, stsUID, identity.SourceClusterID, ledgerUID, func(l *fencing.Ledger) error {
			if !l.BootstrapInitReady(identity) {
				return fencing.ErrPending
			}
			return nil
		}, func(l *fencing.Ledger) error { return l.Bind(identity) })
		return err
	}
	learnerStatus, err := e.bootstrapLearnerStatus(ctx, podName, identity)
	if err != nil || learnerStatus.Assignment == nil {
		return fencing.ErrPending
	}
	assignment := *learnerStatus.Assignment
	if assignment.NodeID != identity.NodeID || assignment.PodUID != identity.PodUID || assignment.PodName != podName || assignment.SourceClusterID != identity.SourceClusterID || assignment.CredentialName != identity.CredentialName || assignment.CredentialUID != identity.CredentialUID {
		return fencing.ErrPending
	}
	_, err = e.changeExisting(ctx, stsUID, identity.SourceClusterID, ledgerUID, func(l *fencing.Ledger) error {
		if !l.BootstrapInitReady(identity) {
			return fencing.ErrPending
		}
		if l.MembershipRepair == nil {
			return fencing.ErrPending
		}
		for _, candidate := range l.MembershipRepair.Progress.Candidates {
			if candidate.Assignment.PodUID == identity.PodUID && candidate.Assignment == assignment {
				return nil
			}
		}
		return fencing.ErrPending
	}, func(l *fencing.Ledger) error { return l.Bind(identity) })
	return err
}

func (e *FencingExecutor) completeBootstrap(ctx context.Context, stsUID string, runtime fencing.Identity, completion fencing.BootstrapCompletion) error {
	if completion.OperationID == "" || completion.AuthorizationHash == "" || completion.AssignmentSetHash == "" || completion.CertifiedBarrierRef == "" || runtime.WALIdentity == "" {
		return fmt.Errorf("fencing: incomplete bootstrap completion request")
	}
	if _, err := fencing.ParseBootstrapBarrierSlot(completion.CertifiedBarrierRef); err != nil {
		return err
	}
	podName := fencing.PodNameForIdentity(runtime)
	var pod object
	if err := e.Kube.Get(ctx, e.controller().corePath("pods", podName), &pod); err != nil {
		return err
	}
	_, actualSTS, rootUID, err := e.bootstrapCaller(ctx, podName, runtime, completion.OperationID)
	if err != nil || actualSTS != stsUID {
		return fencing.ErrPending
	}
	assignment, state, ledgerUID, err := e.bootstrapAssignment(ctx, stsUID, rootUID, runtime, completion.OperationID)
	if err != nil || assignment.Purpose != fencing.BootstrapInitial {
		return fencing.ErrPending
	}
	status, err := state.Ledger.BootstrapStatus(completion.OperationID, runtime.PodUID)
	if err != nil || status.AssignmentSetHash != completion.AssignmentSetHash {
		return fencing.ErrPending
	}
	operation := state.Ledger.Bootstrap.Operations[completion.OperationID]
	if operation.AuthorizationHash != completion.AuthorizationHash {
		return fencing.ErrPending
	}
	var assignments []fencing.BootstrapAssignment
	nodes := make([]string, 0, len(operation.Assignments))
	for _, item := range operation.Assignments {
		if item.OperationID != completion.OperationID || item.RequestHash != operation.RequestHash || item.AuthorizationHash != operation.AuthorizationHash || item.RootUID != rootUID || item.StatefulSetUID != stsUID || item.SourceClusterID != runtime.SourceClusterID || item.LedgerUID == "" {
			return fencing.ErrPending
		}
		assignments = append(assignments, item)
		nodes = append(nodes, item.NodeID)
	}
	if fencing.BootstrapAssignmentSetHash(assignments) != completion.AssignmentSetHash || !state.Ledger.BootstrapInitComplete(nodes) {
		return fencing.ErrPending
	}
	run, ok := state.Ledger.Runs[runtime.RunID]
	if !ok || run.Identity != runtime || run.Identity.WALIdentity == "" {
		return fencing.ErrPending
	}
	if operation.Complete {
		// A response-lost retry for this exact, already completed operation does
		// not need the history to remain un-compacted. The immutable operation,
		// authorization, assignment-set digest, and bound runtime still match.
		return nil
	}
	if operation.AdmissionClosed || e.Bucket == nil {
		return fencing.ErrPending
	}

	// Re-read the current managed root, workload, immutable credential, and
	// source membership before treating the barrier reference as a lookup hint.
	var roots struct {
		Items []ClusterResource `json:"items"`
	}
	if err := e.Kube.Get(ctx, e.controller().clusterPath(""), &roots); err != nil {
		return err
	}
	var root *ClusterResource
	for i := range roots.Items {
		if str(roots.Items[i].Metadata["uid"]) == rootUID {
			root = &roots.Items[i]
			break
		}
	}
	if root == nil || root.Metadata["deletionTimestamp"] != nil {
		return fencing.ErrPending
	}
	normalizeClusterSpec(root)
	if !root.Spec.Automatic || root.Spec.StatefulSet == "" || root.Spec.AdoptClusterID != runtime.SourceClusterID {
		return fencing.ErrPending
	}
	var sts object
	if err := e.Kube.Get(ctx, e.controller().stsPath(root.Spec.StatefulSet), &sts); err != nil {
		return err
	}
	if str(nested(sts, "metadata", "uid")) != stsUID || nested(sts, "metadata", "deletionTimestamp") != nil {
		return fencing.ErrPending
	}
	mainContainer, err := container(sts, root.Spec.Container)
	if err != nil {
		return err
	}
	env, err := e.controller().environment(ctx, mainContainer)
	if err != nil || env["RHIZA_CLUSTER_ID"] != runtime.SourceClusterID {
		return fencing.ErrPending
	}
	storeIdentity := StoreIdentity(env)
	if len(e.StoreIdentity) == 0 || len(storeIdentity) != len(e.StoreIdentity) {
		return fencing.ErrPending
	}
	for key, expected := range e.StoreIdentity {
		if storeIdentity[key] != expected {
			return fencing.ErrPending
		}
	}
	enableReconfiguration, err := reconfigurationEnabled(env)
	if err != nil || !enableReconfiguration {
		return fencing.ErrPending
	}
	pin, err := e.bootstrapTrust(ctx, stsUID, rootUID, runtime.SourceClusterID, completion.OperationID)
	if err != nil || pin.CredentialName != assignment.CredentialName || pin.CredentialUID != assignment.CredentialUID || pin.MembershipHash != assignment.MembershipHash {
		return fencing.ErrPending
	}
	var secret object
	if err := e.Kube.Get(ctx, e.controller().corePath("secrets", pin.CredentialName), &secret); err != nil ||
		str(nested(secret, "metadata", "uid")) != pin.CredentialUID || secret["immutable"] != true {
		return fencing.ErrPending
	}
	encodedMembers, err := base64.StdEncoding.DecodeString(str(nested(secret, "data", "members")))
	if err != nil {
		return fencing.ErrPending
	}
	members, err := recoveryMembers(string(encodedMembers))
	if err != nil || hashJSON(members) != pin.MembershipHash {
		return fencing.ErrPending
	}
	memberBound := false
	for _, member := range members {
		if string(member.ID) == runtime.NodeID {
			memberBound = true
			break
		}
	}
	if !memberBound {
		return fencing.ErrPending
	}
	durability := env["RHIZA_OBJSTORE_DURABILITY"]
	if durability == "" {
		durability = "async"
	}
	if durability != "async" && durability != "before-ack" {
		return fencing.ErrPending
	}
	archivePrefix := path.Join(e.Prefix, runtime.SourceClusterID)
	registered, err := e.controller().sourceMembership(ctx, archivePrefix)
	if err != nil || registered != recovery.NewMembershipRecord(runtime.SourceClusterID, members, durability) {
		return fencing.ErrPending
	}
	initial := quepaxa.Cluster{ConfigID: 1, Members: members}
	archive := recovery.NewManager(e.Bucket, archivePrefix, 1)
	defer archive.Close()
	slot, err := fencing.ParseBootstrapBarrierSlot(completion.CertifiedBarrierRef)
	if err != nil {
		return err
	}
	decision, barrierMembership, err := archive.VerifyCertifiedBarrier(ctx, initial, enableReconfiguration, quepaxa.Slot(slot), fencing.BootstrapBarrierNonce(completion.OperationID))
	if err != nil || decision.Slot != quepaxa.Slot(slot) || barrierMembership.ConfigID != initial.ConfigID || string(jsonBytes(barrierMembership.Members)) != string(jsonBytes(initial.Members)) {
		return fencing.ErrPending
	}
	currentPin, err := e.bootstrapTrust(ctx, stsUID, rootUID, runtime.SourceClusterID, completion.OperationID)
	if err != nil || currentPin != pin {
		return fencing.ErrPending
	}
	currentMembership, err := e.controller().sourceMembership(ctx, archivePrefix)
	if err != nil || currentMembership != recovery.NewMembershipRecord(runtime.SourceClusterID, members, durability) {
		return fencing.ErrPending
	}

	// The proof is accepted only after replay/readback, then persisted by the
	// same retained-ledger UID and a CAS that rechecks the immutable operation.
	_, err = e.changeExisting(ctx, stsUID, runtime.SourceClusterID, ledgerUID, func(l *fencing.Ledger) error {
		if l.Bootstrap == nil || l.Bootstrap.RootUID != rootUID || l.Bootstrap.StatefulSetUID != stsUID {
			return fencing.ErrPending
		}
		current, ok := l.Bootstrap.Operations[completion.OperationID]
		if !ok || current.AuthorizationHash != completion.AuthorizationHash || current.AdmissionClosed || current.Complete {
			return fencing.ErrPending
		}
		currentAssignments := make([]fencing.BootstrapAssignment, 0, len(current.Assignments))
		currentNodes := make([]string, 0, len(current.Assignments))
		for _, item := range current.Assignments {
			currentAssignments = append(currentAssignments, item)
			currentNodes = append(currentNodes, item.NodeID)
		}
		if fencing.BootstrapAssignmentSetHash(currentAssignments) != completion.AssignmentSetHash || !l.BootstrapInitComplete(currentNodes) {
			return fencing.ErrPending
		}
		return nil
	}, func(l *fencing.Ledger) error {
		current := l.Bootstrap.Operations[completion.OperationID]
		current.Complete = true
		current.AdmissionClosed = true
		l.Bootstrap.Operations[completion.OperationID] = current
		return nil
	})
	return err
}

func validatePreparePod(pod object, podUID string) error {
	spec := asObject(pod["spec"])
	if spec["hostPID"] == true || spec["shareProcessNamespace"] == true || len(list(spec["initContainers"])) != 1 || len(list(spec["containers"])) != 1 || len(list(spec["ephemeralContainers"])) != 0 {
		return fmt.Errorf("fencing: fixed prepare init requires one supervised main container and private PID namespace")
	}
	main := asObject(list(spec["containers"])[0])
	init := asObject(list(spec["initContainers"])[0])
	if str(init["name"]) != prepareInitContainerName || init["image"] != main["image"] || !stringListEqual(init["command"], []string{"/usr/local/bin/rhiza"}) || !stringListEqual(init["args"], []string{"--prepare-restore"}) || init["envFrom"] != nil || main["envFrom"] != nil || init["lifecycle"] != nil || str(init["restartPolicy"]) == "Always" {
		return fmt.Errorf("fencing: prepare init profile differs")
	}
	base := object{}
	for key, value := range pod {
		base[key] = value
	}
	baseSpec := object{}
	for key, value := range spec {
		baseSpec[key] = value
	}
	baseSpec["initContainers"] = []any{}
	base["spec"] = baseSpec
	if err := validateSupervisedPod(base); err != nil {
		return err
	}
	if str(nested(pod, "metadata", "annotations", fencingPrepareOperationAnnotation)) == "" {
		return fmt.Errorf("fencing: prepare operation annotation is required")
	}
	mainEnv := make(map[string]object)
	for _, raw := range list(main["env"]) {
		entry := asObject(raw)
		name := str(entry["name"])
		if name == "" || mainEnv[name] != nil {
			return fmt.Errorf("fencing: supervised container has duplicate environment")
		}
		mainEnv[name] = entry
	}
	env := make(map[string]object)
	for _, raw := range list(init["env"]) {
		entry := asObject(raw)
		name := str(entry["name"])
		if name == "" || env[name] != nil || strings.Contains(name, "ADMIN") || strings.Contains(name, "PEER_TOKEN") ||
			!prepareInitEnvironmentName(name) && name != "POD_UID" && name != "RHIZA_PREPARED_POD_UID" && name != "POD_NAME" && name != "RHIZA_PREPARE_OPERATION_ID" {
			return fmt.Errorf("fencing: prepare init has duplicate or unsupported environment")
		}
		env[name] = entry
	}
	for name, entry := range env {
		if !prepareInitEnvironmentName(name) {
			continue
		}
		mainEntry := mainEnv[name]
		if mainEntry == nil || !bytes.Equal(jsonBytes(entry), jsonBytes(mainEntry)) {
			return fmt.Errorf("fencing: prepare init environment %s differs from the supervised container", name)
		}
	}
	for name := range mainEnv {
		if prepareInitEnvironmentName(name) && env[name] == nil {
			return fmt.Errorf("fencing: prepare init omits supervised configuration %s", name)
		}
	}
	for name, fieldPath := range map[string]string{"POD_UID": "metadata.uid", "RHIZA_PREPARED_POD_UID": "metadata.uid", "POD_NAME": "metadata.name", "RHIZA_PREPARE_OPERATION_ID": "metadata.annotations['" + fencingPrepareOperationAnnotation + "']"} {
		if str(nested(env[name], "valueFrom", "fieldRef", "fieldPath")) != fieldPath || env[name]["value"] != nil {
			return fmt.Errorf("fencing: prepare init downward API binding %s differs", name)
		}
	}
	if podUID == "" || str(nested(pod, "metadata", "uid")) != podUID {
		return fmt.Errorf("fencing: prepare init Pod UID differs")
	}
	for _, name := range []string{"RHIZA_CLUSTER_ID", "RHIZA_NODE_ID", "RHIZA_CLUSTER_MEMBERS", "RHIZA_DATA_DIR", "RHIZA_ENABLE_RECONFIGURATION", "FENCER_AUTHORITY_URL", "FENCER_CA_FILE"} {
		if env[name] == nil {
			return fmt.Errorf("fencing: prepare init environment %s is missing", name)
		}
	}
	if str(env["RHIZA_ENABLE_RECONFIGURATION"]["value"]) != "true" {
		return fmt.Errorf("fencing: prepare init requires reconfiguration to be enabled")
	}
	if env["RHIZA_CLUSTER_MEMBERS"]["valueFrom"] == nil || nested(env["RHIZA_CLUSTER_MEMBERS"], "valueFrom", "secretKeyRef", "key") != "members" || env["RHIZA_CLUSTER_MEMBERS"]["value"] != nil {
		return fmt.Errorf("fencing: prepare init public membership must use its explicit Secret key")
	}
	for _, name := range []string{"FENCER_AUTHORITY_URL", "FENCER_CA_FILE"} {
		if str(env[name]["value"]) == "" || env[name]["value"] != mainEnv[name]["value"] || env[name]["valueFrom"] != nil {
			return fmt.Errorf("fencing: prepare init %s differs from the supervised container", name)
		}
	}
	if !strings.HasPrefix(str(env["FENCER_AUTHORITY_URL"]["value"]), "https://") || !strings.HasSuffix(str(env["FENCER_AUTHORITY_URL"]["value"]), "/v1/supervisor") {
		return fmt.Errorf("fencing: prepare init requires the configured HTTPS authority")
	}
	volumes := make(map[string]object)
	for _, raw := range list(spec["volumes"]) {
		volume := asObject(raw)
		name := str(volume["name"])
		if name == "" || volumes[name] != nil || volume["hostPath"] != nil || (volume["secret"] != nil && name != "fence-token") {
			return fmt.Errorf("fencing: prepare init has an unsupported volume")
		}
		volumes[name] = volume
	}
	tokenSources := list(nested(volumes["fence-token"], "projected", "sources"))
	tokenFound := false
	for _, raw := range tokenSources {
		token := asObject(asObject(raw)["serviceAccountToken"])
		if token != nil && str(token["path"]) == "token" && str(token["audience"]) == "rhiza-fencer" {
			tokenFound = true
		}
	}
	if !tokenFound || volumes["fence-ca"] == nil || volumes["fence-ca"]["configMap"] == nil {
		return fmt.Errorf("fencing: prepare init authority token and CA volumes are required")
	}
	mainMounts := make(map[string]object)
	initMounts := make(map[string]object)
	for _, raw := range list(init["volumeMounts"]) {
		mount := asObject(raw)
		name := str(mount["name"])
		if name == "" || str(mount["mountPath"]) == "" || volumes[name] == nil || initMounts[name] != nil {
			return fmt.Errorf("fencing: prepare init mount is incomplete")
		}
		initMounts[name] = mount
	}
	for _, raw := range list(main["volumeMounts"]) {
		mount := asObject(raw)
		name := str(mount["name"])
		if name != "" && volumes[name] != nil && (volumes[name]["emptyDir"] != nil || name == "fence-token" || name == "fence-ca") {
			mainMounts[name] = mount
		}
	}
	for name, mount := range mainMounts {
		got := initMounts[name]
		if got == nil || got["mountPath"] != mount["mountPath"] || got["subPath"] != mount["subPath"] || got["readOnly"] != mount["readOnly"] {
			return fmt.Errorf("fencing: prepare init mount %s differs from the supervised container", name)
		}
	}
	for _, name := range []string{"fence-token", "fence-ca"} {
		if initMounts[name] == nil {
			return fmt.Errorf("fencing: prepare init mount %s is required", name)
		}
	}
	return nil
}

func stringListEqual(value any, expected []string) bool {
	actual := list(value)
	if len(actual) != len(expected) {
		return false
	}
	for i, item := range actual {
		if str(item) != expected[i] {
			return false
		}
	}
	return true
}

func podPrepareProfileHash(pod object) string {
	spec := asObject(pod["spec"])
	containers, init := list(spec["containers"]), list(spec["initContainers"])
	if len(containers) != 1 || len(init) != 1 {
		return ""
	}
	return hashJSON(object{"image": asObject(containers[0])["image"], "init": init[0], "volumes": spec["volumes"]})
}

func (c *Controller) readBootstrapLedgerPin(ctx context.Context, r *ClusterResource) (bootstrapLedgerPin, error) {
	var pin bootstrapLedgerPin
	reader, err := c.Bucket.Get(ctx, c.bootstrapLedgerPinKey(r))
	if err != nil {
		return pin, fmt.Errorf("bootstrap ledger pin is unavailable")
	}
	data, readErr := io.ReadAll(io.LimitReader(reader, 64<<10))
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || len(data) == 64<<10 || json.Unmarshal(data, &pin) != nil || !bytes.Equal(data, jsonBytes(pin)) || pin.Version != 1 || pin.OperationID == "" || pin.RequestHash == "" || pin.AuthorizationHash == "" || pin.RootUID != str(r.Metadata["uid"]) || pin.SourceClusterID != r.Spec.AdoptClusterID || pin.StatefulSetUID == "" || pin.CredentialName == "" || pin.CredentialUID == "" || pin.MembershipHash == "" || pin.ProfileHash == "" || pin.LedgerUID == "" {
		return bootstrapLedgerPin{}, fmt.Errorf("bootstrap ledger pin is invalid")
	}
	return pin, nil
}

func (c *Controller) assignBootstrapPods(ctx context.Context, r *ClusterResource, sts object, pods []object) error {
	if c.Bucket == nil {
		return fmt.Errorf("bootstrap assignment store is unavailable")
	}
	pin, err := c.readBootstrapLedgerPin(ctx, r)
	if err != nil {
		return err
	}
	if pin.StatefulSetUID != str(nested(sts, "metadata", "uid")) || pin.OperationID != bootstrapOperationID(pin.RootUID, pin.StatefulSetUID, expectedBootstrapCandidate(r, sts)) || pin.ProfileHash != prepareProfileHash(sts) || str(nested(sts, "spec", "podManagementPolicy")) != "Parallel" {
		return fmt.Errorf("bootstrap ledger pin differs from the workload template")
	}
	if len(pods) != r.Spec.VoterCount {
		return fencing.ErrPending
	}
	var secret object
	if err := c.Kube.Get(ctx, c.corePath("secrets", pin.CredentialName), &secret); err != nil {
		return fmt.Errorf("pinned bootstrap credential is unavailable")
	}
	owner := &Resource{Metadata: object{"uid": str(r.Metadata["uid"])}, Spec: Spec{RecoveryID: r.Spec.LogicalID}, Status: Status{Target: r.Spec.AdoptClusterID}}
	if str(nested(secret, "metadata", "uid")) != pin.CredentialUID || secret["immutable"] != true || str(nested(secret, "metadata", "annotations", ownerAnnotation)) != c.operationID(owner) {
		return fmt.Errorf("pinned bootstrap credential identity changed")
	}
	encoded, err := base64.StdEncoding.DecodeString(str(nested(secret, "data", "members")))
	if err != nil {
		return fmt.Errorf("pinned bootstrap membership is invalid")
	}
	members, err := recoveryMembers(string(encoded))
	if err != nil || len(members) != r.Spec.VoterCount || hashJSON(members) != pin.MembershipHash {
		return fmt.Errorf("pinned bootstrap membership changed")
	}
	voters, err := derivedVoters(r, sts)
	if err != nil || len(voters) != r.Spec.VoterCount {
		return fmt.Errorf("bootstrap voter inventory is invalid")
	}
	want := make(map[string]quepaxa.NodeID, len(voters))
	for _, voter := range voters {
		want[voter.Pod] = voter.NodeID
	}
	assignments := make([]fencing.BootstrapAssignment, 0, len(pods))
	seen := make(map[string]bool, len(pods))
	for _, pod := range pods {
		podName, podUID := str(nested(pod, "metadata", "name")), str(nested(pod, "metadata", "uid"))
		nodeID, exists := want[podName]
		if !exists || seen[podName] || podUID == "" || nested(pod, "metadata", "deletionTimestamp") != nil || str(nested(pod, "metadata", "annotations", fencingPrepareOperationAnnotation)) != pin.OperationID || str(nested(pod, "metadata", "annotations", fencingRootAnnotation)) != pin.RootUID {
			return fmt.Errorf("bootstrap Pod inventory differs from its retained voter assignment")
		}
		ownerOK := false
		for _, raw := range list(nested(pod, "metadata", "ownerReferences")) {
			ref := asObject(raw)
			if ref["controller"] == true && ref["apiVersion"] == "apps/v1" && ref["kind"] == "StatefulSet" && ref["name"] == r.Spec.StatefulSet && ref["uid"] == pin.StatefulSetUID {
				ownerOK = true
			}
		}
		if !ownerOK || validatePreparePod(pod, podUID) != nil || podPrepareProfileHash(pod) != pin.ProfileHash {
			return fmt.Errorf("bootstrap Pod profile or controller owner differs")
		}
		init := asObject(list(nested(pod, "spec", "initContainers"))[0])
		initEnv := make(map[string]object)
		for _, raw := range list(init["env"]) {
			entry := asObject(raw)
			initEnv[str(entry["name"])] = entry
		}
		if str(initEnv["RHIZA_CLUSTER_ID"]["value"]) != pin.SourceClusterID || str(nested(initEnv["RHIZA_NODE_ID"], "valueFrom", "fieldRef", "fieldPath")) != "metadata.name" || str(nested(initEnv["RHIZA_CLUSTER_MEMBERS"], "valueFrom", "secretKeyRef", "name")) != pin.CredentialName {
			return fmt.Errorf("bootstrap Pod public identity inputs differ from the pinned assignment")
		}
		assignments = append(assignments, fencing.BootstrapAssignment{OperationID: pin.OperationID, RequestHash: pin.RequestHash, AuthorizationHash: pin.AuthorizationHash, Purpose: fencing.BootstrapInitial, RootUID: pin.RootUID, StatefulSetUID: pin.StatefulSetUID, SourceClusterID: pin.SourceClusterID, PodName: podName, PodUID: podUID, NodeID: string(nodeID), CredentialName: pin.CredentialName, CredentialUID: pin.CredentialUID, MembershipHash: pin.MembershipHash, ProfileHash: pin.ProfileHash, LedgerUID: pin.LedgerUID})
		seen[podName] = true
	}
	if len(assignments) != r.Spec.VoterCount {
		return fencing.ErrPending
	}
	executor := &FencingExecutor{Kube: c.Kube}
	_, err = executor.changeExisting(ctx, pin.StatefulSetUID, pin.SourceClusterID, pin.LedgerUID, nil, func(l *fencing.Ledger) error {
		if l.Bootstrap == nil {
			return fmt.Errorf("bootstrap history is missing")
		}
		op, ok := l.Bootstrap.Operations[pin.OperationID]
		if !ok || op.RequestHash != pin.RequestHash || op.AuthorizationHash != pin.AuthorizationHash || op.Purpose != fencing.BootstrapInitial || op.RootUID != pin.RootUID || op.StatefulSetUID != pin.StatefulSetUID || op.SourceClusterID != pin.SourceClusterID {
			return fmt.Errorf("bootstrap operation differs from pinned authority")
		}
		for _, assignment := range assignments {
			if err := l.AssignBootstrap(assignment); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}
