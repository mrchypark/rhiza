package operator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func drSuccessorOperationID(recoveryOperationID, statefulSetUID, target string) string {
	data := []byte(recoveryOperationID + "\x00" + statefulSetUID + "\x00" + target)
	digest := sha256.Sum256(data)
	return "dr-successor-" + hex.EncodeToString(digest[:12])
}

// prepareDRSuccessor creates the operation before the StatefulSet is scaled.
// A later reconciliation calls it with pods present to bind their actual UIDs.
// It never assigns from status/read paths and never invents run or WAL identity.
func (c *Controller) prepareDRSuccessor(ctx context.Context, r *Resource, sts object, members []quepaxa.Member, assignPods bool) (string, error) {
	wantBinding, bindingErr := expectedAnchorBinding(c.AnchorBindingConfig, c.AnchorBinding.StorageID, r.Status.Target)
	if r.Spec.AnchorID == "" || r.Status.AnchorRequest == nil || r.Status.AnchorReceipt == nil || c.Anchor == nil || bindingErr != nil {
		return "", fmt.Errorf("DR successor requires an exact anchor approval and configured target storage binding")
	}
	rootUID := str(nested(sts, "spec", "template", "metadata", "annotations", fencingRootAnnotation))
	if rootUID == "" || str(nested(sts, "metadata", "uid")) != r.Status.StatefulSetUID {
		return "", fmt.Errorf("DR successor requires the retained root UID and StatefulSet UID")
	}
	request, receipt := *r.Status.AnchorRequest, *r.Status.AnchorReceipt
	if request.OperationID != c.operationID(r) || request.StatefulSetUID != r.Status.StatefulSetUID || request.TargetClusterID != r.Status.Target || receipt.Target != wantBinding {
		return "", fmt.Errorf("DR successor anchor approval differs from the reserved target")
	}
	if err := c.Anchor.VerifyAdmission(ctx, request, receipt, wantBinding, receipt.Generation); err != nil {
		return "", fmt.Errorf("DR successor anchor approval is not current: %w", err)
	}
	var secret object
	if err := c.Kube.Get(ctx, c.corePath("secrets", r.Status.SecretName), &secret); err != nil {
		return "", fmt.Errorf("DR successor credentials are unavailable")
	}
	credentialName, credentialUID := r.Status.SecretName, str(nested(secret, "metadata", "uid"))
	membershipHash := fencing.BootstrapMembersHash(members)
	profileHash := prepareProfileHash(sts)
	storeHash := fencing.BootstrapStoreIdentityHash(c.StoreIdentity)
	durability := r.Spec.Durability
	if credentialUID == "" || secret["immutable"] != true || str(nested(secret, "metadata", "annotations", ownerAnnotation)) != c.operationID(r) || profileHash == "" || membershipHash == "" || storeHash == "" {
		return "", fmt.Errorf("DR successor credential, profile, or store binding is incomplete")
	}
	operationID := drSuccessorOperationID(c.operationID(r), r.Status.StatefulSetUID, r.Status.Target)
	requestHash := hashJSON(struct {
		OperationID, RecoveryUID, StatefulSetUID, TargetClusterID  string
		CredentialName, CredentialUID, MembershipHash, ProfileHash string
	}{operationID, str(r.Metadata["uid"]), r.Status.StatefulSetUID, r.Status.Target, credentialName, credentialUID, membershipHash, profileHash})
	approval := fencing.BootstrapDRApproval{Request: request, Receipt: receipt}
	base := fencing.BootstrapAssignment{
		OperationID: operationID, RequestHash: requestHash, Purpose: fencing.BootstrapDRSuccessor,
		RootUID: rootUID, StatefulSetUID: r.Status.StatefulSetUID, SourceClusterID: r.Status.Target,
		CredentialName: credentialName, CredentialUID: credentialUID, MembershipHash: membershipHash,
		ProfileHash: profileHash, StoreIdentityHash: storeHash, Durability: durability, DRApproval: approval,
	}
	base.AuthorizationHash = fencing.BootstrapDRAuthorizationHash(base)
	op := fencing.BootstrapOperation{OperationID: operationID, RequestHash: requestHash, AuthorizationHash: base.AuthorizationHash, Purpose: fencing.BootstrapDRSuccessor, RootUID: rootUID, StatefulSetUID: r.Status.StatefulSetUID, SourceClusterID: r.Status.Target}
	executor := &FencingExecutor{Kube: c.Kube, Bucket: c.Bucket, Prefix: c.Prefix}
	_, err := executor.change(ctx, r.Status.StatefulSetUID, r.Status.Target, func(l *fencing.Ledger) error {
		return l.InstallBootstrapOperation(op)
	})
	if err != nil {
		return "", fmt.Errorf("retain DR successor operation: %w", err)
	}
	state, ledgerUID, _, err := executor.readWithUID(ctx, r.Status.StatefulSetUID, r.Status.Target)
	if err != nil || ledgerUID == "" || state.Ledger.Bootstrap == nil {
		return "", fmt.Errorf("DR successor ledger identity is unavailable")
	}
	operation, ok := state.Ledger.Bootstrap.Operations[operationID]
	if !ok || operation.OperationID != op.OperationID || operation.RequestHash != op.RequestHash || operation.AuthorizationHash != op.AuthorizationHash || operation.Purpose != op.Purpose || operation.RootUID != op.RootUID || operation.StatefulSetUID != op.StatefulSetUID || operation.SourceClusterID != op.SourceClusterID {
		return "", fmt.Errorf("retained DR successor operation differs")
	}
	if !assignPods {
		return operationID, nil
	}
	pods, err := c.ownedPods(ctx, r.Status.StatefulSetUID)
	if err != nil {
		return "", err
	}
	byID := make(map[string]quepaxa.Member, len(members))
	for _, member := range members {
		byID[string(member.ID)] = member
	}
	for _, pod := range pods {
		podName, podUID := str(nested(pod, "metadata", "name")), str(nested(pod, "metadata", "uid"))
		if podName == "" || podUID == "" || validatePreparePod(pod, podUID) != nil || str(nested(pod, "metadata", "annotations", fencingRootAnnotation)) != rootUID || str(nested(pod, "metadata", "annotations", fencingPrepareOperationAnnotation)) != operationID {
			return "", fmt.Errorf("DR successor Pod identity or init profile differs")
		}
		ownerRefs := list(nested(pod, "metadata", "ownerReferences"))
		if len(ownerRefs) != 1 || str(asObject(ownerRefs[0])["uid"]) != r.Status.StatefulSetUID || asObject(ownerRefs[0])["controller"] != true {
			return "", fmt.Errorf("DR successor Pod is not controlled by the exact StatefulSet")
		}
		member, ok := byID[podName]
		if !ok {
			return "", fmt.Errorf("DR successor member identity does not match the Pod's configured static voter identity")
		}
		assignment := base
		assignment.PodName, assignment.PodUID, assignment.NodeID = podName, podUID, string(member.ID)
		assignment.LedgerUID = ledgerUID
		if err := assignment.Validate(); err != nil {
			return "", err
		}
		_, err := executor.changeExisting(ctx, r.Status.StatefulSetUID, r.Status.Target, ledgerUID, nil, func(l *fencing.Ledger) error {
			return l.AssignBootstrap(assignment)
		})
		if err != nil {
			return "", fmt.Errorf("assign DR successor Pod %s: %w", podName, err)
		}
	}
	return operationID, nil
}
