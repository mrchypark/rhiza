package operator

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

var errSameOrdinalLearnerCapacity = errors.New("same-ordinal learner capacity unavailable")

// prepareSameOrdinalLearner reserves a new logical identity for the exact
// StatefulSet Pod incarnation that replaced a fenced voter. The Pod remains
// physically owned and named by the StatefulSet; only its consensus identity
// and immutable peer credential are new. It does not create or mutate the Pod.
func (c *Controller) prepareSameOrdinalLearner(ctx context.Context, state *autoControl, sts object, containerName string) (bool, error) {
	i := state.Intent
	if i == nil || i.Failed == "" || i.Proof == nil || i.LearnerUID != "" {
		return false, nil
	}
	old, ok := state.Identities[i.Failed]
	if !ok || old.NodeID != string(i.Failed) || old.Pod == "" || old.PodUID == "" || old.WALIdentity == "" {
		return false, fmt.Errorf("failed voter incarnation is not durably identified")
	}
	if err := validateFenceProof(i.Request, i.Proof); err != nil || i.Request.Scope != ScopeVoter || len(i.Request.Targets) != 1 || i.Request.Targets[0] != old {
		return false, fmt.Errorf("same-ordinal candidate is not bound to the exact old-voter fence")
	}
	var pod object
	if err := c.Kube.Get(ctx, c.corePath("pods", old.Pod), &pod); err != nil {
		if apiNotFound(err) {
			return false, nil
		}
		return false, err
	}
	uid := str(nested(pod, "metadata", "uid"))
	if uid == old.PodUID || uid == "" || nested(pod, "metadata", "deletionTimestamp") != nil {
		return false, nil
	}
	if str(nested(pod, "metadata", "name")) != old.Pod || str(nested(sts, "metadata", "uid")) != state.StatefulSetUID {
		return false, fmt.Errorf("recreated voter Pod or StatefulSet identity differs")
	}
	controllerOwner := false
	for _, raw := range list(nested(pod, "metadata", "ownerReferences")) {
		owner := asObject(raw)
		if owner["controller"] == true && owner["kind"] == "StatefulSet" && owner["uid"] == state.StatefulSetUID && owner["name"] == nested(sts, "metadata", "name") {
			if controllerOwner {
				return false, fmt.Errorf("recreated voter Pod has ambiguous StatefulSet ownership")
			}
			controllerOwner = true
		}
	}
	if !controllerOwner || str(nested(pod, "metadata", "annotations", fencingProtocolAnnotation)) != "1" || str(nested(pod, "metadata", "annotations", fencingRootAnnotation)) != state.BindingUID {
		return false, fmt.Errorf("recreated voter Pod is not bound to the supervised workload")
	}
	if err := validateSupervisedPod(pod); err != nil {
		return false, err
	}
	logicalID := "inc-" + shortID(string(i.Failed)+":"+i.Request.OperationID+":"+uid)
	for id := range state.Identities {
		if string(id) == logicalID {
			return false, fmt.Errorf("fresh logical identity collides with the installed cohort")
		}
	}
	for _, entry := range state.Learners {
		if entry.Target.NodeID != logicalID && entry.Target.PodUID != uid {
			continue
		}
		if entry.SourceClusterID != state.ActiveClusterID || entry.OperationID != i.Request.OperationID ||
			entry.Target.NodeID != logicalID || entry.Target.Pod != old.Pod || entry.Target.PodUID != uid || entry.Retirement == nil || i.LearnerAttempt == 0 {
			return false, fmt.Errorf("fresh logical identity collides with retained learner history")
		}
		retirementRequest := entry.Retirement.Request
		retirementProof := &entry.Retirement.Proof
		retirementOperationMatches := false
		for priorAttempt := 0; priorAttempt < i.LearnerAttempt && priorAttempt < 3; priorAttempt++ {
			if retirementRequest.OperationID == fmt.Sprintf("%s-learner-fence-%d", i.RecoveryName, priorAttempt) {
				retirementOperationMatches = true
				break
			}
		}
		if retirementRequest.LogicalID != i.Request.LogicalID || retirementRequest.BindingUID != state.BindingUID ||
			retirementRequest.Namespace != c.Kube.Namespace || retirementRequest.StatefulSet != i.Request.StatefulSet ||
			retirementRequest.StatefulSetUID != state.StatefulSetUID || retirementRequest.SourceClusterID != entry.SourceClusterID ||
			!retirementOperationMatches ||
			retirementRequest.Scope != ScopeVoter || len(retirementRequest.Targets) != 1 || retirementRequest.Targets[0] != entry.Target ||
			validateFenceProof(retirementRequest, retirementProof) != nil || proofCoversLearner(retirementRequest, retirementProof, entry.Target) != nil {
			return false, fmt.Errorf("fresh logical identity collides with unproven learner retirement history")
		}
		return false, nil
	}
	if i.LearnerName != "" && i.LearnerName != old.Pod && i.LearnerPodName == "" {
		var standalone object
		if err := c.Kube.Get(ctx, c.corePath("pods", i.LearnerName), &standalone); err == nil {
			// A standalone learner may already have been created before the
			// control CAS response. Never retarget that immutable attempt.
			return false, nil
		} else if !apiNotFound(err) {
			return false, err
		}
	}
	ctr, err := container(sts, containerName)
	if err != nil {
		return false, fmt.Errorf("recreated voter container is unavailable: %w", err)
	}
	env, err := c.environment(ctx, ctr)
	if err != nil {
		return false, err
	}
	if err := noPVC(sts, ctr, env); err != nil {
		return false, err
	}
	httpPort, peerPort, err := validateLearnerPorts(ctr)
	if err != nil {
		return false, err
	}
	candidate := *state
	candidateIntent := *i
	candidateIntent.LearnerName = logicalID
	candidate.Intent = &candidateIntent
	if err := fenceCapacityError(worstCaseGenerationTargets(&candidate)); err != nil {
		return false, fmt.Errorf("%w: %v", errSameOrdinalLearnerCapacity, err)
	}
	if len(state.Learners) >= maxLearnerHistory {
		return false, fmt.Errorf("%w: learner history is full (%d) and canonical fencing status cannot compact it", errSameOrdinalLearnerCapacity, maxLearnerHistory)
	}
	secretName := sameOrdinalSecretName(state.BindingUID, uid)
	httpURL := fmt.Sprintf("http://%s:%d", old.Pod, httpPort)
	peerURL := fmt.Sprintf("quic://%s:%d", old.Pod, peerPort)

	var secret object
	err = c.Kube.Get(ctx, c.corePath("secrets", secretName), &secret)
	if err != nil && !apiNotFound(err) {
		return false, err
	}
	if apiNotFound(err) {
		token, tokenErr := randomToken()
		if tokenErr != nil {
			return false, tokenErr
		}
		member := quepaxa.Member{ID: quepaxa.NodeID(logicalID), URL: httpURL, PeerURL: peerURL,
			PublicKey: quepaxa.PublicKey(network.PeerPublicKey(types.ClusterID(state.ActiveClusterID), quepaxa.NodeID(logicalID), token))}
		secret = object{"apiVersion": "v1", "kind": "Secret", "type": "Opaque", "immutable": true,
			"metadata": object{"name": secretName, "annotations": object{ownerAnnotation: i.Request.OperationID}},
			"data":     object{"member": base64.StdEncoding.EncodeToString(jsonBytes(member)), "RHIZA_PEER_TOKEN": base64.StdEncoding.EncodeToString([]byte(token))}}
		if err := c.Kube.Post(ctx, c.corePath("secrets", ""), secret, nil); err != nil {
			return false, fmt.Errorf("create same-ordinal learner credential: %w", err)
		}
		if err := c.Kube.Get(ctx, c.corePath("secrets", secretName), &secret); err != nil {
			return false, err
		}
	}
	if secret["immutable"] != true || str(nested(secret, "metadata", "annotations", ownerAnnotation)) != i.Request.OperationID {
		return false, fmt.Errorf("same-ordinal learner credential is mutable or has a different owner")
	}
	data := asObject(secret["data"])
	if len(data) != 2 || data["member"] == nil || data["RHIZA_PEER_TOKEN"] == nil {
		return false, fmt.Errorf("same-ordinal learner credential has an unsupported payload")
	}
	memberBytes, err := base64.StdEncoding.DecodeString(str(nested(secret, "data", "member")))
	if err != nil {
		return false, fmt.Errorf("same-ordinal learner member encoding is invalid")
	}
	var member quepaxa.Member
	decoder := json.NewDecoder(bytes.NewReader(memberBytes))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&member) != nil || decoder.Decode(new(any)) != io.EOF || string(member.ID) != logicalID || member.URL != httpURL || member.PeerURL != peerURL || member.PublicKey == (quepaxa.PublicKey{}) {
		return false, fmt.Errorf("same-ordinal learner credential differs from its reserved identity")
	}
	token, err := base64.StdEncoding.DecodeString(str(nested(secret, "data", "RHIZA_PEER_TOKEN")))
	if err != nil || quepaxa.PublicKey(network.PeerPublicKey(types.ClusterID(state.ActiveClusterID), quepaxa.NodeID(logicalID), string(token))) != member.PublicKey {
		return false, fmt.Errorf("same-ordinal learner peer credential is unavailable")
	}
	secretUID, secretRV := str(nested(secret, "metadata", "uid")), str(nested(secret, "metadata", "resourceVersion"))
	if secretUID == "" || secretRV == "" {
		return false, fmt.Errorf("same-ordinal learner credential identity is unavailable")
	}
	secretHash := hashJSON(secret["data"])
	i.LearnerName, i.LearnerPodName, i.LearnerUID = logicalID, old.Pod, uid
	i.LearnerSecretName, i.LearnerSecretUID, i.LearnerSecretRV, i.LearnerSecretHash = secretName, secretUID, secretRV, secretHash
	return true, nil
}

func sameOrdinalSecretName(bindingUID, podUID string) string {
	return "rhiza-inc-" + shortID(bindingUID+":"+podUID)
}

func learnerPhysicalPod(i *autoIntent) string {
	if i == nil {
		return ""
	}
	if i.LearnerPodName != "" {
		return i.LearnerPodName
	}
	return i.LearnerName
}

func learnerCredentialName(i *autoIntent) string {
	if i == nil {
		return ""
	}
	if i.LearnerSecretName != "" {
		return i.LearnerSecretName
	}
	return i.LearnerName + "-credentials"
}

func sameOrdinalAssignmentDigest(state *autoControl, intent *autoIntent) (string, error) {
	if state == nil || intent == nil || intent.LearnerPodName == "" || intent.LearnerName == "" || intent.LearnerUID == "" ||
		intent.LearnerSecretName == "" || intent.LearnerSecretUID == "" || intent.LearnerSecretRV == "" || intent.LearnerSecretHash == "" ||
		intent.RecoveryUID == "" || intent.SpecHash == "" || intent.Proof == nil {
		return "", fmt.Errorf("same-ordinal learner assignment is incomplete")
	}
	if err := validateFenceProof(intent.Request, intent.Proof); err != nil || intent.Request.StatefulSetUID != state.StatefulSetUID || intent.Request.BindingUID != state.BindingUID || intent.Request.SourceClusterID != state.ActiveClusterID {
		return "", fmt.Errorf("same-ordinal learner assignment is not bound to canonical recovery")
	}
	return hashJSON([]any{
		"rhiza-same-ordinal-assignment-v1", state.BindingUID, state.BindingHash, state.StatefulSetUID, state.ActiveClusterID,
		intent.Request, intent.Proof, intent.Failed, intent.LearnerName, intent.LearnerPodName, intent.LearnerUID,
		intent.LearnerSecretName, intent.LearnerSecretUID, intent.LearnerSecretRV, intent.LearnerSecretHash,
		intent.RecoveryName, intent.RecoveryUID, intent.SpecHash,
	}), nil
}
