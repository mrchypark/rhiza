package operator

import (
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

// resolveStaticVoterStartupGrant restores only the two startup values a static
// cooperative voter needs. The Pod, its exact StatefulSet owner, the pinned
// RhizaCluster, and the current StatefulSet template are the authority for the
// source configuration. Node still independently validates canonical archive
// membership before opening voter state.
func (e *FencingExecutor) resolveStaticVoterStartupGrant(ctx context.Context, statefulSetUID string, request fencing.Identity) (fencing.StartupGrant, bool, error) {
	if e == nil || e.Kube == nil {
		return fencing.StartupGrant{}, false, nil
	}
	physical := request.PodName
	if physical == "" {
		physical = request.NodeID
	}
	if physical == "" || request.NodeID != physical || request.PodUID == "" || request.AssignmentID != "" || request.WALIdentity != "" || statefulSetUID == "" {
		return fencing.StartupGrant{}, false, nil
	}
	var pod object
	if err := e.Kube.Get(ctx, e.controller().corePath("pods", physical), &pod); err != nil {
		return fencing.StartupGrant{}, false, err
	}
	if str(nested(pod, "metadata", "uid")) != request.PodUID || str(nested(pod, "metadata", "name")) != physical || nested(pod, "metadata", "deletionTimestamp") != nil {
		return fencing.StartupGrant{}, false, fmt.Errorf("fencing: static startup Pod incarnation changed")
	}
	rootUID := str(nested(pod, "metadata", "annotations", fencingRootAnnotation))
	if str(nested(pod, "metadata", "annotations", fencingProtocolAnnotation)) != "1" {
		return fencing.StartupGrant{}, false, fmt.Errorf("fencing: static startup workload is not enrolled")
	}
	if e.AuthorityProfile == fencing.LogicalWriterProfileV1 {
		if err := validateLogicalWriterPod(pod); err != nil {
			return fencing.StartupGrant{}, false, err
		}
	} else if err := validateSupervisedPod(pod); err != nil {
		return fencing.StartupGrant{}, false, err
	}
	// The registration handler has already applied initializeRoot's configured
	// cooperative authority policy. Do not read it again here: legacy first
	// registration intentionally permits a missing root for genesis.
	rootStatefulSet := ""
	containerName := "rhiza"
	if rootUID != "" {
		rootPath := "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/" + url.PathEscape(e.Kube.Namespace) + "/rhizaclusters"
		var roots struct {
			Items []ClusterResource `json:"items"`
		}
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
				return fencing.StartupGrant{}, false, fmt.Errorf("fencing: static startup root binding is ambiguous")
			}
			root = candidate
		}
		if root == nil || root.Spec.StatefulSet == "" || root.Spec.AdoptClusterID != request.SourceClusterID {
			return fencing.StartupGrant{}, false, fmt.Errorf("fencing: static startup root binding differs")
		}
		rootStatefulSet = root.Spec.StatefulSet
		if root.Spec.Container != "" {
			containerName = root.Spec.Container
		}
	}

	ownerName := ""
	for _, item := range list(nested(pod, "metadata", "ownerReferences")) {
		owner := asObject(item)
		if owner["apiVersion"] != "apps/v1" || owner["kind"] != "StatefulSet" || owner["controller"] != true {
			continue
		}
		if ownerName != "" {
			return fencing.StartupGrant{}, false, fmt.Errorf("fencing: static startup StatefulSet owner is ambiguous")
		}
		ownerName = str(owner["name"])
		if (rootStatefulSet != "" && ownerName != rootStatefulSet) || str(owner["uid"]) != statefulSetUID {
			return fencing.StartupGrant{}, false, fmt.Errorf("fencing: static startup StatefulSet owner differs")
		}
	}
	if ownerName == "" {
		return fencing.StartupGrant{}, false, fmt.Errorf("fencing: static startup StatefulSet owner is missing")
	}

	var sts object
	if err := e.Kube.Get(ctx, e.controller().stsPath(ownerName), &sts); err != nil {
		return fencing.StartupGrant{}, false, err
	}
	if str(nested(sts, "metadata", "uid")) != statefulSetUID || str(nested(sts, "metadata", "name")) != ownerName || nested(sts, "metadata", "deletionTimestamp") != nil {
		return fencing.StartupGrant{}, false, fmt.Errorf("fencing: static startup StatefulSet changed")
	}
	if str(nested(sts, "spec", "template", "metadata", "annotations", fencingProtocolAnnotation)) != "1" || str(nested(sts, "spec", "template", "metadata", "annotations", fencingRootAnnotation)) != rootUID {
		return fencing.StartupGrant{}, false, fmt.Errorf("fencing: static startup template binding differs")
	}
	if e.AuthorityProfile == fencing.LogicalWriterProfileV1 {
		if err := validateLogicalWriterTemplate(pod, sts); err != nil {
			return fencing.StartupGrant{}, false, err
		}
	}
	templateContainer, err := container(sts, containerName)
	if err != nil {
		return fencing.StartupGrant{}, false, err
	}
	podContainer, err := namedPodContainer(pod, str(templateContainer["name"]))
	if err != nil {
		return fencing.StartupGrant{}, false, err
	}
	controller := e.controller()
	templateEnv, err := controller.environment(ctx, templateContainer)
	if err != nil {
		return fencing.StartupGrant{}, false, fmt.Errorf("fencing: static startup template environment unavailable: %w", err)
	}
	podEnv, err := controller.environment(ctx, podContainer)
	if err != nil {
		return fencing.StartupGrant{}, false, fmt.Errorf("fencing: static startup Pod environment unavailable: %w", err)
	}
	templateStore := StoreIdentity(templateEnv)
	podStore := StoreIdentity(podEnv)
	for key, value := range templateStore {
		if podStore[key] != value {
			return fencing.StartupGrant{}, false, fmt.Errorf("fencing: static startup Pod differs from current template store identity")
		}
	}
	if e.AuthorityProfile == fencing.LogicalWriterProfileV1 {
		if err := noPVC(sts, podContainer, podEnv); err != nil {
			return fencing.StartupGrant{}, false, fmt.Errorf("fencing: logical workload violates the no-PVC data contract: %w", err)
		}
	}
	for _, key := range []string{"RHIZA_CLUSTER_ID", "RHIZA_CLUSTER_MEMBERS"} {
		if templateEnv[key] == "" || podEnv[key] != templateEnv[key] {
			return fencing.StartupGrant{}, false, fmt.Errorf("fencing: static startup Pod differs from current template")
		}
	}
	for _, key := range []string{"RHIZA_PEER_TOKENS", "RHIZA_PEER_TOKEN"} {
		if podEnv[key] != templateEnv[key] {
			return fencing.StartupGrant{}, false, fmt.Errorf("fencing: static startup Pod differs from current template")
		}
	}
	if templateEnv["RHIZA_CLUSTER_ID"] != request.SourceClusterID {
		return fencing.StartupGrant{}, false, fmt.Errorf("fencing: static startup source cluster differs")
	}

	var ownToken string
	if templateEnv["RHIZA_PEER_TOKENS"] != "" {
		if templateEnv["RHIZA_PEER_TOKEN"] != "" {
			return fencing.StartupGrant{}, false, fmt.Errorf("fencing: static startup peer token forms conflict")
		}
		tokens, secretBacked, err := secretBackedEnvironmentValue(ctx, controller, templateContainer, "RHIZA_PEER_TOKENS")
		if err != nil {
			return fencing.StartupGrant{}, false, err
		}
		if !secretBacked || tokens != templateEnv["RHIZA_PEER_TOKENS"] {
			return fencing.StartupGrant{}, false, fmt.Errorf("fencing: static startup peer token source differs")
		}
		var peerTokens map[string]string
		if err := json.Unmarshal([]byte(tokens), &peerTokens); err != nil || len(peerTokens) == 0 {
			return fencing.StartupGrant{}, false, fmt.Errorf("fencing: static startup peer tokens are invalid")
		}
		ownToken = peerTokens[physical]
	} else if templateEnv["RHIZA_PEER_TOKEN"] != "" {
		var secretBacked bool
		ownToken, secretBacked, err = secretBackedEnvironmentValue(ctx, controller, templateContainer, "RHIZA_PEER_TOKEN")
		if err != nil {
			return fencing.StartupGrant{}, false, err
		}
		if !secretBacked || ownToken != templateEnv["RHIZA_PEER_TOKEN"] {
			return fencing.StartupGrant{}, false, fmt.Errorf("fencing: static startup peer token source differs")
		}
	} else {
		return fencing.StartupGrant{}, false, fmt.Errorf("fencing: static startup peer token is unavailable")
	}
	if ownToken == "" {
		return fencing.StartupGrant{}, false, fmt.Errorf("fencing: static startup peer token is missing")
	}
	members, err := recoveryMembers(templateEnv["RHIZA_CLUSTER_MEMBERS"])
	if err != nil {
		return fencing.StartupGrant{}, false, fmt.Errorf("fencing: static startup membership is invalid: %w", err)
	}
	var ownMember *quepaxa.Member
	for i := range members {
		if string(members[i].ID) == physical {
			if ownMember != nil {
				return fencing.StartupGrant{}, false, fmt.Errorf("fencing: static startup membership duplicates this voter")
			}
			ownMember = &members[i]
		}
	}
	if ownMember == nil || quepaxa.PublicKey(network.PeerPublicKey(types.ClusterID(request.SourceClusterID), quepaxa.NodeID(physical), ownToken)) != ownMember.PublicKey {
		return fencing.StartupGrant{}, false, fmt.Errorf("fencing: static startup peer token does not match voter identity")
	}
	canonicalMembers, err := json.Marshal(members)
	if err != nil {
		return fencing.StartupGrant{}, false, err
	}
	grant := fencing.StartupGrant{
		Identity: request,
		Role:     fencing.StartupRoleVoterActive,
		Environment: map[string]string{
			"RHIZA_CLUSTER_MEMBERS": string(canonicalMembers),
			"RHIZA_PEER_TOKEN":      ownToken,
		},
	}
	if e.AuthorityProfile == fencing.LogicalWriterProfileV1 {
		if e.AuthorityState == nil || !e.RequireMutationJournal {
			return fencing.StartupGrant{}, false, fmt.Errorf("fencing: logical startup authority is not configured")
		}
		scope, err := e.storageScope(ctx, sts, request.SourceClusterID)
		if err != nil {
			return fencing.StartupGrant{}, false, err
		}
		binding := fencing.LogicalWriterBindingV1{Version: fencing.LogicalWriterVersionV1, Profile: fencing.LogicalWriterProfileV1,
			SourceClusterID: request.SourceClusterID, NodeID: request.NodeID, RunID: request.RunID,
			ProducerUID:    "logical-producer-" + hashJSON([]any{"rhiza-logical-producer-v1", statefulSetUID, request.SourceClusterID, request.NodeID, request.RunID, hashJSON(scope)})[:40],
			StoreScopeHash: hashJSON(scope), Role: fencing.StartupRoleVoterActive}
		grant.AuthorityProfile = fencing.LogicalWriterProfileV1
		grant.LogicalBinding = &binding
	}
	return grant, true, nil
}

func namedPodContainer(pod object, name string) (object, error) {
	for _, item := range list(nested(pod, "spec", "containers")) {
		candidate := asObject(item)
		if str(candidate["name"]) == name {
			return candidate, nil
		}
	}
	return nil, fmt.Errorf("fencing: static startup Pod container differs")
}

// secretBackedEnvironmentValue resolves a single effective environment value
// only when its winning source is a Secret. It follows environment's envFrom
// and env precedence so ConfigMap or literal overrides cannot masquerade as
// Secret-backed credentials.
func secretBackedEnvironmentValue(ctx context.Context, c *Controller, container object, name string) (string, bool, error) {
	var value string
	secretBacked := false
	for _, item := range list(container["envFrom"]) {
		entry := asObject(item)
		prefix := str(entry["prefix"])
		for _, kind := range []string{"configMapRef", "secretRef"} {
			ref := asObject(entry[kind])
			if ref == nil {
				continue
			}
			resource := "configmaps"
			isSecret := kind == "secretRef"
			if isSecret {
				resource = "secrets"
			}
			var source object
			if err := c.Kube.Get(ctx, c.corePath(resource, str(ref["name"])), &source); err != nil {
				return "", false, err
			}
			for key, raw := range asObject(source["data"]) {
				if prefix+key != name {
					continue
				}
				value = str(raw)
				secretBacked = isSecret
				if isSecret {
					decoded, err := base64.StdEncoding.DecodeString(value)
					if err != nil {
						return "", false, fmt.Errorf("fencing: static startup peer token Secret is malformed")
					}
					value = string(decoded)
				}
			}
		}
	}
	for _, item := range list(container["env"]) {
		entry := asObject(item)
		if str(entry["name"]) != name {
			continue
		}
		if raw, ok := entry["value"]; ok {
			value, secretBacked = str(raw), false
			continue
		}
		from := asObject(entry["valueFrom"])
		for _, kind := range []string{"secretKeyRef", "configMapKeyRef"} {
			ref := asObject(from[kind])
			if ref == nil {
				continue
			}
			resource := "configmaps"
			isSecret := kind == "secretKeyRef"
			if isSecret {
				resource = "secrets"
			}
			var source object
			if err := c.Kube.Get(ctx, c.corePath(resource, str(ref["name"])), &source); err != nil {
				return "", false, err
			}
			raw, ok := asObject(source["data"])[str(ref["key"])]
			if !ok {
				if isSecret {
					return "", false, fmt.Errorf("fencing: static startup peer token Secret key is unavailable")
				}
				return "", false, fmt.Errorf("fencing: static startup token ConfigMap key is unavailable")
			}
			value, secretBacked = str(raw), isSecret
			if isSecret {
				decoded, err := base64.StdEncoding.DecodeString(value)
				if err != nil {
					return "", false, fmt.Errorf("fencing: static startup peer token Secret is malformed")
				}
				value = string(decoded)
			}
		}
	}
	return value, secretBacked, nil
}
