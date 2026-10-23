package operator

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// resolveStartupGrant derives logical identity and learner configuration from
// the canonical automatic-control record. The request remains bound to the
// TokenReview-authenticated physical Pod; only this authority response can map
// it to a fresh logical voter identity.
func (e *FencingExecutor) resolveStartupGrant(ctx context.Context, statefulSetUID string, request fencing.Identity) (fencing.StartupGrant, error) {
	physical := request.PodName
	if physical == "" {
		physical = request.NodeID
	}
	if physical == "" || request.PodUID == "" {
		return fencing.StartupGrant{}, fmt.Errorf("fencing: startup physical identity is incomplete")
	}
	request.PodName = physical
	grant, assigned, err := e.resolveSameOrdinalStartupGrant(ctx, statefulSetUID, request)
	if err != nil {
		return fencing.StartupGrant{}, err
	}
	if assigned {
		return grant, nil
	}
	role, err := e.resolveLegacyStartupRole(ctx, statefulSetUID, request)
	if err != nil {
		return fencing.StartupGrant{}, err
	}
	return fencing.StartupGrant{Identity: request, Role: role}, nil
}

// resolveSameOrdinalStartupGrant returns assigned=true once the canonical
// intent identifies this physical replacement, including while its child is
// not yet journaled. Such a candidate must remain pending rather than falling
// through to the old physical voter identity.
func (e *FencingExecutor) resolveSameOrdinalStartupGrant(ctx context.Context, statefulSetUID string, request fencing.Identity) (fencing.StartupGrant, bool, error) {
	if e == nil || e.Kube == nil || e.ArchiveBucket == nil {
		return fencing.StartupGrant{}, false, nil
	}
	physical := request.PodName
	var pod object
	if err := e.Kube.Get(ctx, e.controller().corePath("pods", physical), &pod); err != nil {
		return fencing.StartupGrant{}, false, err
	}
	if str(nested(pod, "metadata", "uid")) != request.PodUID || str(nested(pod, "metadata", "name")) != physical || nested(pod, "metadata", "deletionTimestamp") != nil {
		return fencing.StartupGrant{}, false, fmt.Errorf("fencing: startup Pod incarnation changed")
	}
	rootUID := str(nested(pod, "metadata", "annotations", fencingRootAnnotation))
	if rootUID == "" {
		return fencing.StartupGrant{}, false, nil
	}
	var roots struct {
		Items []ClusterResource `json:"items"`
	}
	rootPath := "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/" + url.PathEscape(e.Kube.Namespace) + "/rhizaclusters"
	if err := e.Kube.Get(ctx, rootPath, &roots); err != nil {
		return fencing.StartupGrant{}, false, err
	}
	var root *ClusterResource
	for i := range roots.Items {
		candidate := &roots.Items[i]
		if str(candidate.Metadata["uid"]) != rootUID || candidate.Metadata["deletionTimestamp"] != nil {
			continue
		}
		if root != nil {
			return fencing.StartupGrant{}, false, fmt.Errorf("fencing: startup root binding is ambiguous")
		}
		root = candidate
	}
	if root == nil {
		return fencing.StartupGrant{}, false, fmt.Errorf("fencing: startup root binding is unavailable")
	}
	if !root.Spec.Automatic || root.Spec.StatefulSet == "" || root.Spec.AdoptClusterID != request.SourceClusterID || root.Spec.LogicalID == "" {
		return fencing.StartupGrant{}, false, nil
	}
	ownerFound := false
	for _, ref := range list(nested(pod, "metadata", "ownerReferences")) {
		owner := asObject(ref)
		if owner["kind"] == "StatefulSet" && owner["controller"] == true && owner["name"] == root.Spec.StatefulSet && owner["uid"] == statefulSetUID {
			if ownerFound {
				return fencing.StartupGrant{}, false, fmt.Errorf("fencing: startup StatefulSet ownership is ambiguous")
			}
			ownerFound = true
		}
	}
	if !ownerFound {
		return fencing.StartupGrant{}, false, fmt.Errorf("fencing: startup StatefulSet owner differs")
	}
	if str(nested(pod, "metadata", "annotations", fencingProtocolAnnotation)) != "1" || str(nested(pod, "metadata", "annotations", fencingRootAnnotation)) != rootUID {
		return fencing.StartupGrant{}, false, fmt.Errorf("fencing: startup workload binding differs")
	}
	if err := validateSupervisedPod(pod); err != nil {
		return fencing.StartupGrant{}, false, err
	}
	c := &Controller{Kube: e.Kube, Bucket: e.ArchiveBucket, Prefix: e.AutomaticControlPrefix}
	state, _, err := c.loadAuto(ctx, c.autoKey(root))
	if err != nil {
		return fencing.StartupGrant{}, false, fmt.Errorf("fencing: canonical startup control unavailable: %w", err)
	}
	if state.BindingUID != rootUID || state.BindingHash != autoBinding(root) || state.StatefulSetUID != statefulSetUID || state.ActiveClusterID != request.SourceClusterID {
		return fencing.StartupGrant{}, false, fmt.Errorf("fencing: canonical startup binding differs")
	}
	i := state.Intent
	completed := false
	if i == nil && state.Completed != nil && state.Completed.LearnerIntent != nil {
		i = state.Completed.LearnerIntent
		completed = true
	}
	if i == nil || i.Failed == "" || i.Proof == nil {
		return fencing.StartupGrant{}, false, nil
	}
	var failed FenceTarget
	if completed {
		if len(i.Request.Targets) != 1 {
			return fencing.StartupGrant{}, false, fmt.Errorf("fencing: completed replacement request is ambiguous")
		}
		failed = i.Request.Targets[0]
	} else {
		var known bool
		failed, known = state.Identities[i.Failed]
		if !known {
			if i.LearnerPodName == physical && i.LearnerUID == request.PodUID {
				return fencing.StartupGrant{}, true, fmt.Errorf("fencing: same-ordinal closed identity is absent from canonical control")
			}
			return fencing.StartupGrant{}, false, nil
		}
	}
	if failed.NodeID != string(i.Failed) || failed.Pod != physical || failed.PodUID == request.PodUID {
		if i.LearnerPodName == physical && i.LearnerUID == request.PodUID {
			return fencing.StartupGrant{}, true, fmt.Errorf("fencing: same-ordinal assignment no longer matches its closed identity")
		}
		return fencing.StartupGrant{}, false, nil
	}
	// A replacement of a closed voter ordinal is a candidate immediately. If
	// assignment data is incomplete or the controller is still preparing it,
	// reject as pending instead of allowing the closed physical identity back.
	if i.LearnerPodName == "" && i.LearnerUID == "" {
		return fencing.StartupGrant{}, true, fencing.ErrPending
	}
	if i.Aborting || i.LearnerName == "" || i.LearnerPodName != physical || i.LearnerUID != request.PodUID || i.LearnerSecretName == "" ||
		i.LearnerSecretUID == "" || i.LearnerSecretRV == "" || i.LearnerSecretHash == "" || i.RecoveryName == "" || i.RecoveryUID == "" || i.SpecHash == "" || i.LearnerAssignmentID == "" {
		return fencing.StartupGrant{}, true, fencing.ErrPending
	}
	if request.NodeID != physical && request.NodeID != i.LearnerName {
		return fencing.StartupGrant{}, true, fmt.Errorf("fencing: startup logical identity differs from assignment")
	}
	if i.Request.BindingUID != state.BindingUID || i.Request.SourceClusterID != state.ActiveClusterID || i.Request.StatefulSetUID != state.StatefulSetUID || i.Request.Scope != ScopeVoter || len(i.Request.Targets) != 1 || i.Request.Targets[0] != failed || validateFenceProof(i.Request, i.Proof) != nil {
		return fencing.StartupGrant{}, true, fmt.Errorf("fencing: startup removal proof differs from canonical cohort")
	}
	if err := e.validateCanonicalLearnerFence(ctx, i.Request, i.Proof, failed); err != nil {
		return fencing.StartupGrant{}, true, err
	}
	assignment, err := sameOrdinalAssignmentDigest(state, i)
	if err != nil || assignment != i.LearnerAssignmentID {
		return fencing.StartupGrant{}, true, fmt.Errorf("fencing: startup assignment digest differs")
	}

	var sts object
	if err := e.Kube.Get(ctx, c.stsPath(root.Spec.StatefulSet), &sts); err != nil {
		return fencing.StartupGrant{}, true, err
	}
	if str(nested(sts, "metadata", "uid")) != statefulSetUID {
		return fencing.StartupGrant{}, true, fmt.Errorf("fencing: startup StatefulSet identity changed")
	}
	var child Resource
	if err := e.Kube.Get(ctx, c.resourcePath(i.RecoveryName), &child); err != nil {
		return fencing.StartupGrant{}, true, err
	}
	if str(child.Metadata["uid"]) != i.RecoveryUID || hashJSON(child.Spec) != i.SpecHash || child.Spec.Membership == nil || child.Spec.Membership.AbortAddition || child.Spec.Membership.OperationID != i.RecoveryName || child.Spec.Membership.Remove != i.Failed || child.Spec.Membership.ReplacementPod != physical || child.Spec.Membership.ReplacementSecret != i.LearnerSecretName || string(child.Spec.Membership.Fence.NodeID) != string(i.Failed) || child.Spec.Membership.Fence.WALIdentity != failed.WALIdentity || child.Spec.Membership.Fence.WorkloadUID != failed.PodUID || !child.Spec.Membership.Fence.Confirmed || child.Spec.Membership.Fence.Evidence != i.Proof.ProofID || child.Spec.Fence.Evidence != i.Proof.ProofID {
		return fencing.StartupGrant{}, true, fmt.Errorf("fencing: startup recovery child differs from assignment")
	}
	role := fencing.StartupRoleLearnerPendingRemove
	if child.Status.Phase == "Complete" || child.Status.Membership != nil && child.Status.Membership.Phase == "Complete" {
		verifyState := *state
		verifyState.Intent = i
		var installed installedGeneration
		if err := c.verifyAutomaticCompletion(ctx, root, &verifyState, &child, sts, &installed); err != nil {
			return fencing.StartupGrant{}, true, fmt.Errorf("fencing: completed startup assignment is not canonical: %w", err)
		}
		role = fencing.StartupRoleVoterActive
	}
	if completed && (role != fencing.StartupRoleVoterActive || state.Completed.RecoveryName != i.RecoveryName || state.Completed.RecoveryUID != i.RecoveryUID || state.Completed.SpecHash != i.SpecHash || state.Completed.FenceProofID != i.Proof.ProofID || state.Completed.FenceRequestHash != i.Proof.RequestHash || state.Completed.ClusterID != state.ActiveClusterID) {
		return fencing.StartupGrant{}, true, fmt.Errorf("fencing: completed same-ordinal assignment receipt differs")
	}

	var secret object
	if err := e.Kube.Get(ctx, e.controller().corePath("secrets", i.LearnerSecretName), &secret); err != nil {
		return fencing.StartupGrant{}, true, err
	}
	if secret["type"] != "Opaque" || secret["immutable"] != true || str(nested(secret, "metadata", "name")) != i.LearnerSecretName || str(nested(secret, "metadata", "namespace")) != e.Kube.Namespace || str(nested(secret, "metadata", "uid")) != i.LearnerSecretUID || str(nested(secret, "metadata", "resourceVersion")) != i.LearnerSecretRV || str(nested(secret, "metadata", "annotations", ownerAnnotation)) != i.Request.OperationID || hashJSON(secret["data"]) != i.LearnerSecretHash {
		return fencing.StartupGrant{}, true, fmt.Errorf("fencing: startup learner credential differs from assignment")
	}
	if len(asObject(secret["data"])) != 2 || nested(secret, "data", "member") == nil || nested(secret, "data", "RHIZA_PEER_TOKEN") == nil {
		return fencing.StartupGrant{}, true, fmt.Errorf("fencing: startup learner credential payload differs")
	}
	memberBytes, err := base64.StdEncoding.DecodeString(str(nested(secret, "data", "member")))
	if err != nil {
		return fencing.StartupGrant{}, true, fmt.Errorf("fencing: startup member credential encoding is invalid")
	}
	var member quepaxa.Member
	decoder := json.NewDecoder(bytes.NewReader(memberBytes))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&member) != nil || string(member.ID) != i.LearnerName || member.PublicKey == (quepaxa.PublicKey{}) {
		return fencing.StartupGrant{}, true, fmt.Errorf("fencing: startup member credential differs")
	}
	token, err := base64.StdEncoding.DecodeString(str(nested(secret, "data", "RHIZA_PEER_TOKEN")))
	if err != nil || len(token) == 0 || quepaxa.PublicKey(network.PeerPublicKey(types.ClusterID(state.ActiveClusterID), member.ID, string(token))) != member.PublicKey {
		return fencing.StartupGrant{}, true, fmt.Errorf("fencing: startup peer credential differs")
	}
	ctr, err := container(sts, root.Spec.Container)
	if err != nil {
		return fencing.StartupGrant{}, true, err
	}
	httpPort, peerPort, err := validateLearnerPorts(ctr)
	if err != nil || member.URL != fmt.Sprintf("http://%s:%d", physical, httpPort) || member.PeerURL != fmt.Sprintf("quic://%s:%d", physical, peerPort) {
		return fencing.StartupGrant{}, true, fmt.Errorf("fencing: startup member endpoint differs from the assigned Pod")
	}
	memberJSON, err := json.Marshal(member)
	if err != nil {
		return fencing.StartupGrant{}, true, err
	}
	identity := request
	identity.NodeID = i.LearnerName
	identity.PodName = physical
	identity.AssignmentID = i.LearnerAssignmentID
	identity.WALIdentity = ""
	env := map[string]string{"RHIZA_NODE_ID": i.LearnerName, "RHIZA_LEARNER": string(memberJSON), "RHIZA_PEER_TOKEN": string(token), "RHIZA_PEER_TOKENS": ""}
	if role == fencing.StartupRoleVoterActive {
		// The template still describes the removed voter. Rebuild the committed
		// membership using the frozen removal request and verified new credential.
		source, envErr := c.environment(ctx, ctr)
		if envErr != nil {
			return fencing.StartupGrant{}, true, envErr
		}
		members, membersErr := recoveryMembers(source["RHIZA_CLUSTER_MEMBERS"])
		if membersErr != nil {
			return fencing.StartupGrant{}, true, membersErr
		}
		filtered := make([]quepaxa.Member, 0, len(members))
		removed := false
		for _, existing := range members {
			if existing.ID == i.Failed {
				removed = true
				continue
			}
			if existing.ID == member.ID {
				return fencing.StartupGrant{}, true, fmt.Errorf("fencing: replacement identity already appears in template membership")
			}
			filtered = append(filtered, existing)
		}
		if !removed || len(filtered) < 2 {
			return fencing.StartupGrant{}, true, fmt.Errorf("fencing: committed replacement cohort differs from source template")
		}
		filtered = append(filtered, member)
		membersJSON, marshalErr := json.Marshal(filtered)
		if marshalErr != nil {
			return fencing.StartupGrant{}, true, marshalErr
		}
		env["RHIZA_LEARNER"] = ""
		env["RHIZA_CLUSTER_MEMBERS"] = string(membersJSON)
	}
	return fencing.StartupGrant{Identity: identity, Role: role, Environment: env}, true, nil
}
