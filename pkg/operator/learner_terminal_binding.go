package operator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type learnerBindingScope struct {
	rootUID, statefulSetUID, sourceClusterID string
	podName, podUID, nodeID                  string
}

// resolveLearnerBindingScope validates the immutable root, workload, Pod and
// source identity shared by live learner admission and retired cleanup.
func (e *FencingExecutor) resolveLearnerBindingScope(ctx context.Context, pod object, stsName string) (learnerBindingScope, error) {
	name := str(nested(pod, "metadata", "name"))
	podUID := str(nested(pod, "metadata", "uid"))
	operation := str(nested(pod, "metadata", "annotations", ownerAnnotation))
	rootUID := str(nested(pod, "metadata", "annotations", fencingRootAnnotation))
	if name == "" || podUID == "" || operation == "" || rootUID == "" || str(nested(pod, "metadata", "labels", "rhiza.mrchypark.dev/automatic-learner")) != name {
		return learnerBindingScope{}, fmt.Errorf("fencing: learner binding missing")
	}
	var roots struct {
		Items []ClusterResource `json:"items"`
	}
	if err := e.Kube.Get(ctx, "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/"+url.PathEscape(e.Kube.Namespace)+"/rhizaclusters", &roots); err != nil {
		return learnerBindingScope{}, err
	}
	var boundRoot *ClusterResource
	for _, candidate := range roots.Items {
		if str(candidate.Metadata["uid"]) != rootUID || candidate.Spec.StatefulSet != stsName || candidate.Metadata["deletionTimestamp"] != nil {
			continue
		}
		if boundRoot != nil {
			return learnerBindingScope{}, fmt.Errorf("fencing: learner root binding is ambiguous")
		}
		copy := candidate
		boundRoot = &copy
	}
	if boundRoot == nil || boundRoot.Spec.Container == "" {
		return learnerBindingScope{}, fmt.Errorf("fencing: learner root binding differs")
	}
	var sts object
	if err := e.Kube.Get(ctx, e.controller().stsPath(stsName), &sts); err != nil {
		return learnerBindingScope{}, err
	}
	workloadUID := str(nested(sts, "metadata", "uid"))
	if str(nested(sts, "metadata", "name")) != stsName || workloadUID == "" || nested(sts, "metadata", "deletionTimestamp") != nil {
		return learnerBindingScope{}, fmt.Errorf("fencing: learner StatefulSet binding is unavailable")
	}
	refs := list(nested(pod, "metadata", "ownerReferences"))
	if len(refs) != 1 {
		return learnerBindingScope{}, fmt.Errorf("fencing: learner StatefulSet owner differs")
	}
	owner := asObject(refs[0])
	if owner["apiVersion"] != "apps/v1" || owner["kind"] != "StatefulSet" || owner["name"] != stsName || owner["uid"] != workloadUID || owner["controller"] != false {
		return learnerBindingScope{}, fmt.Errorf("fencing: learner StatefulSet owner differs")
	}
	var ctr object
	for _, raw := range list(nested(pod, "spec", "containers")) {
		candidate := asObject(raw)
		if str(candidate["name"]) == boundRoot.Spec.Container {
			ctr = candidate
			break
		}
	}
	if ctr == nil {
		return learnerBindingScope{}, fmt.Errorf("fencing: learner database container is unavailable")
	}
	env, err := e.controller().environment(ctx, ctr)
	if err != nil || env["RHIZA_CLUSTER_ID"] == "" {
		return learnerBindingScope{}, fmt.Errorf("fencing: learner source identity is unavailable")
	}
	nodeID, err := fencing.ReplacementNodeID(podUID)
	if err != nil {
		return learnerBindingScope{}, err
	}
	return learnerBindingScope{rootUID: rootUID, statefulSetUID: workloadUID, sourceClusterID: env["RHIZA_CLUSTER_ID"], podName: name, podUID: podUID, nodeID: nodeID}, nil
}

func (e *FencingExecutor) validateLearnerCredential(ctx context.Context, scope learnerBindingScope, identity fencing.Identity) error {
	if identity.Validate() != nil || identity.SourceClusterID != scope.sourceClusterID || identity.NodeID != scope.nodeID || fencing.PodNameForIdentity(identity) != scope.podName || identity.PodUID != scope.podUID || identity.CredentialName == "" || identity.CredentialUID == "" {
		return fmt.Errorf("fencing: learner credential identity differs")
	}
	credentialName := e.incarnationSecretName(scope.rootUID, scope.statefulSetUID, identity)
	if identity.CredentialName != credentialName {
		return fmt.Errorf("fencing: learner credential name differs from its registered identity")
	}
	var secret object
	if err := e.Kube.Get(ctx, e.controller().corePath("secrets", credentialName), &secret); err != nil {
		return err
	}
	if secret["immutable"] != true || secret["type"] != "Opaque" || nested(secret, "metadata", "deletionTimestamp") != nil || str(nested(secret, "metadata", "name")) != credentialName || str(nested(secret, "metadata", "namespace")) != e.Kube.Namespace || str(nested(secret, "metadata", "uid")) != identity.CredentialUID || str(nested(secret, "metadata", "annotations", fencingRootAnnotation)) != scope.rootUID || str(nested(secret, "metadata", "annotations", fencingBindingAnnotation)) != hashJSON([]string{scope.rootUID, scope.statefulSetUID, identity.SourceClusterID, scope.podName, scope.podUID, scope.nodeID}) {
		return fmt.Errorf("fencing: learner credentials changed")
	}
	data, err := base64.StdEncoding.DecodeString(str(nested(secret, "data", "member")))
	if err != nil {
		return err
	}
	var member quepaxa.Member
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&member) != nil || decoder.Decode(new(any)) != io.EOF || string(member.ID) != scope.nodeID || member.PublicKey == (quepaxa.PublicKey{}) {
		return fmt.Errorf("fencing: learner member differs")
	}
	token, err := base64.StdEncoding.DecodeString(str(nested(secret, "data", "RHIZA_PEER_TOKEN")))
	if err != nil || len(token) == 0 || member.PublicKey != quepaxa.PublicKey(network.PeerPublicKey(types.ClusterID(identity.SourceClusterID), member.ID, string(token))) {
		return fmt.Errorf("fencing: learner member key differs from its registered peer token")
	}
	return nil
}

// validateLearnerTerminalBinding is cleanup-only. It checks the exact retained
// historical assignment and closed ledger; it never grants live registration.
func (c *Controller) validateLearnerTerminalBinding(ctx context.Context, pod object, stsName string, state *autoControl, entry learnerIncarnation, target FenceTarget, request FenceRequest, proof *FenceProof) error {
	if state == nil || entry.LedgerUID == "" || entry.SourceClusterID == "" || entry.OperationID == "" || entry.CredentialName == "" || entry.CredentialUID == "" || request.Scope != ScopeGeneration || request.BindingUID != state.BindingUID || request.StatefulSetUID != state.StatefulSetUID || request.StatefulSet != stsName || request.SourceClusterID != entry.SourceClusterID || request.OperationID == "" || request.BindingUID == "" {
		return fmt.Errorf("learner cleanup lacks the exact retained terminal binding")
	}
	if target != entry.Target || !managedLearnerFenceTargetMatches(target, entry.Target.Pod, entry.Target.PodUID) || str(nested(pod, "metadata", "name")) != entry.Target.Pod || str(nested(pod, "metadata", "uid")) != entry.Target.PodUID || nested(pod, "metadata", "annotations", fencingRootAnnotation) != state.BindingUID || nested(pod, "metadata", "annotations", ownerAnnotation) != entry.OperationID || nested(pod, "metadata", "annotations", fencingCredentialAnnotation) != entry.CredentialUID || nested(pod, "metadata", "labels", "rhiza.mrchypark.dev/automatic-learner") != entry.Target.Pod {
		return fmt.Errorf("learner cleanup Pod differs from its retained creation assignment")
	}
	if err := validateFenceProof(request, proof); err != nil {
		return err
	}
	if err := validateSupervisedPod(pod); err != nil {
		return err
	}
	executor := &FencingExecutor{Kube: c.Kube}
	scope, err := executor.resolveLearnerBindingScope(ctx, pod, stsName)
	if err != nil {
		return err
	}
	if scope.rootUID != state.BindingUID || scope.statefulSetUID != state.StatefulSetUID || scope.sourceClusterID != entry.SourceClusterID || scope.podName != entry.Target.Pod || scope.podUID != entry.Target.PodUID || scope.nodeID != entry.Target.NodeID {
		return fmt.Errorf("learner cleanup immutable scope differs")
	}
	ledger, ledgerUID, ledgerObject, err := executor.readWithUID(ctx, scope.statefulSetUID, scope.sourceClusterID)
	if err != nil {
		return fmt.Errorf("fencing: learner cleanup ledger unavailable: %w", err)
	}
	verifiedUID, err := executor.registrationLedgerObjectUID(ledgerObject, scope.statefulSetUID, scope.sourceClusterID)
	if err != nil || verifiedUID != ledgerUID || ledgerUID != entry.LedgerUID {
		return fmt.Errorf("fencing: learner cleanup ledger identity changed")
	}
	requestHash, err := requestHash(&request)
	if err != nil {
		return err
	}
	if ledger.Ledger.ClosedBy != request.OperationID || ledger.Ledger.ClosedHash != requestHash {
		return fmt.Errorf("fencing: learner cleanup admission is not closed by the exact generation request")
	}
	if err := verifyLearnerTerminalRuns(ledger.Ledger, entry, &request, proof); err != nil {
		return err
	}
	if entry.Target.WALIdentity == "" {
		if proof.ProofID != "cooperative-"+hashJSON(ledger) || !ledger.Ledger.CompleteNodes(request.OperationID, requestHash, nil, []string{entry.Target.PodUID}) {
			return fmt.Errorf("never-bound cleanup lacks exact complete ledger proof")
		}
	}
	return nil
}

func verifyLearnerTerminalRuns(ledger fencing.Ledger, entry learnerIncarnation, generation *FenceRequest, generationProof *FenceProof) error {
	if ledger.SourceClusterID != entry.SourceClusterID || entry.Target.NodeID == "" || entry.Target.Pod == "" || entry.Target.PodUID == "" || entry.CredentialName == "" || entry.CredentialUID == "" || entry.LedgerUID == "" {
		return fmt.Errorf("learner terminal binding is incomplete")
	}
	globalClosed := ledger.ClosedBy != "" && ledger.ClosedHash != ""
	if generation != nil {
		hash, err := requestHash(generation)
		if err != nil || ledger.ClosedBy != generation.OperationID || ledger.ClosedHash != hash {
			return fmt.Errorf("fencing: learner cleanup generation closure differs")
		}
	} else if !globalClosed {
		closure, ok := ledger.ClosedNodes[entry.Target.NodeID]
		if !ok || closure.OperationID == "" || closure.RequestHash == "" {
			return fmt.Errorf("fencing: learner admission closure is absent")
		}
	}
	var retirementHash string
	var retirementValid bool
	if entry.Retirement != nil && learnerTargetCovered(entry.Target, entry.Retirement.Request.Targets) && validateFenceProof(entry.Retirement.Request, &entry.Retirement.Proof) == nil {
		retirementHash, _ = requestHash(&entry.Retirement.Request)
		retirementValid = retirementHash != ""
	}
	closure, hasNodeClosure := ledger.ClosedNodes[entry.Target.NodeID]
	seen := false
	for _, run := range ledger.Runs {
		identity := run.Identity
		if identity.PodUID != entry.Target.PodUID && identity.NodeID != entry.Target.NodeID {
			continue
		}
		seen = true
		if err := identity.Validate(); err != nil || identity.SourceClusterID != entry.SourceClusterID || identity.NodeID != entry.Target.NodeID || fencing.PodNameForIdentity(identity) != entry.Target.Pod || identity.PodUID != entry.Target.PodUID || identity.CredentialName != entry.CredentialName || identity.CredentialUID != entry.CredentialUID || run.Stop == nil || run.Stop.OperationID == "" || run.Stop.RequestHash == "" || run.Stop.Identity != identity {
			return fmt.Errorf("learner run identity or stop receipt differs")
		}
		stopAuthorized := globalClosed && run.Stop.OperationID == ledger.ClosedBy && run.Stop.RequestHash == ledger.ClosedHash
		if generation != nil {
			hash, _ := requestHash(generation)
			stopAuthorized = stopAuthorized || (run.Stop.OperationID == generation.OperationID && run.Stop.RequestHash == hash)
		}
		if hasNodeClosure && run.Stop.OperationID == closure.OperationID && run.Stop.RequestHash == closure.RequestHash {
			stopAuthorized = true
		}
		if retirementValid && run.Stop.OperationID == entry.Retirement.Request.OperationID && run.Stop.RequestHash == retirementHash {
			for _, target := range entry.Retirement.Request.Targets {
				if target == entry.Target {
					stopAuthorized = true
					break
				}
			}
		}
		if !stopAuthorized {
			return fmt.Errorf("learner stop receipt has no retained cleanup authorization")
		}
		if entry.Target.WALIdentity == "" {
			if identity.WALIdentity != "" || run.NeverBoundExit == nil || *run.NeverBoundExit != *run.Stop || run.Drain != nil || !run.Exited {
				return fmt.Errorf("learner never-bound exit is not proven")
			}
		} else if identity.WALIdentity != entry.Target.WALIdentity || !run.Exited || run.NeverBoundExit != nil || run.Drain == nil || run.Drain.Request != *run.Stop || !run.Drain.Clean() {
			return fmt.Errorf("learner termination proof is incomplete")
		}
	}
	if !seen {
		return fmt.Errorf("learner run is absent from the fencing ledger")
	}
	return nil
}
