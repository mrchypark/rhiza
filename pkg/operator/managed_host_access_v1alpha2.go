package operator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
)

const (
	// V1Alpha2ManagedHostServiceAccount is retained only to migrate deployments
	// created before managed hosts received an isolated ServiceAccount.
	V1Alpha2ManagedHostServiceAccount = "rhiza-managed-host"
	managedHostAccessManagedBy        = "rhiza-operator"
	managedHostAPIGroup               = "rhiza.mrchypark.dev"
)

var managedHostServiceAccountPattern = regexp.MustCompile(`^rhiza-managed-host-[0-9a-f]{16}$`)

// V1Alpha2ManagedHostServiceAccountName returns the immutable, UID-scoped
// identity used by one managed cluster. The cluster name is intentionally not
// trusted as an RBAC subject boundary because a deleted name can be recreated.
func V1Alpha2ManagedHostServiceAccountName(ref V1Alpha2ClusterRef) (string, error) {
	if !validNamespace(ref.Namespace) || len(ref.Name) == 0 || len(ref.Name) > 253 || !v1alpha2Name.MatchString(ref.Name) || !v1alpha2NonBlank.MatchString(ref.UID) {
		return "", fmt.Errorf("managed host access requires exact cluster identity")
	}
	digest := sha256.Sum256([]byte(ref.Namespace + "\x00" + ref.Name + "\x00" + ref.UID))
	return "rhiza-managed-host-" + hex.EncodeToString(digest[:8]), nil
}

func managedHostRoleBindingName(serviceAccount string) string {
	return serviceAccount + "-binding"
}

func managedHostLabels(ref V1Alpha2ClusterRef) map[string]string {
	return map[string]string{
		"app.kubernetes.io/managed-by":         managedHostAccessManagedBy,
		"rhiza.mrchypark.dev/cluster-name":     managedHostClusterNameLabel(ref.Name),
		"rhiza.mrchypark.dev/cluster-uid":      ref.UID,
		"rhiza.mrchypark.dev/access-contract":  "v1alpha2-host-v1",
		"rhiza.mrchypark.dev/access-lifecycle": "retain-until-manual-cleanup",
	}
}

func managedHostClusterNameLabel(name string) string {
	if len(name) <= 63 {
		return name
	}
	digest := sha256.Sum256([]byte(name))
	return "sha256-" + hex.EncodeToString(digest[:8])
}

func managedHostRoleRules(ref V1Alpha2ClusterRef, deployment string) []any {
	return []any{
		map[string]any{"apiGroups": []string{managedHostAPIGroup}, "resources": []string{"rhizaclusters"}, "resourceNames": []string{ref.Name}, "verbs": []string{"get"}},
		map[string]any{"apiGroups": []string{managedHostAPIGroup}, "resources": []string{"rhizaclusters/status"}, "resourceNames": []string{ref.Name}, "verbs": []string{"get", "update"}},
		// RBAC cannot restrict list by label or get by owner chain. Hosts use a
		// cluster-UID selector and validate Pod -> ReplicaSet -> Deployment.
		map[string]any{"apiGroups": []string{""}, "resources": []string{"pods"}, "verbs": []string{"get", "list"}},
		map[string]any{"apiGroups": []string{"apps"}, "resources": []string{"deployments"}, "resourceNames": []string{deployment}, "verbs": []string{"get"}},
		map[string]any{"apiGroups": []string{"apps"}, "resources": []string{"replicasets"}, "verbs": []string{"get"}},
	}
}

type managedHostAccessObject struct {
	path string
	kind string
	name string
	body map[string]any
}

func managedHostAccessObjects(ref V1Alpha2ClusterRef, name, deployment, binding string) []managedHostAccessObject {
	return []managedHostAccessObject{
		{"/api/v1/namespaces/" + ref.Namespace + "/serviceaccounts", "ServiceAccount", name, map[string]any{
			"apiVersion": "v1", "kind": "ServiceAccount",
			"metadata": map[string]any{"name": name, "namespace": ref.Namespace, "labels": managedHostLabels(ref)},
		}},
		{"/apis/rbac.authorization.k8s.io/v1/namespaces/" + ref.Namespace + "/roles", "Role", name, map[string]any{
			"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role",
			"metadata": map[string]any{"name": name, "namespace": ref.Namespace, "labels": managedHostLabels(ref)},
			"rules":    managedHostRoleRules(ref, deployment),
		}},
		{"/apis/rbac.authorization.k8s.io/v1/namespaces/" + ref.Namespace + "/rolebindings", "RoleBinding", binding, map[string]any{
			"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
			"metadata": map[string]any{"name": binding, "namespace": ref.Namespace, "labels": managedHostLabels(ref)},
			"roleRef":  map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": name},
			"subjects": []any{map[string]any{"kind": "ServiceAccount", "name": name, "namespace": ref.Namespace}},
		}},
	}
}

func ensureV1Alpha2ManagedHostAccess(ctx context.Context, kube *Kubernetes, ref V1Alpha2ClusterRef) (string, error) {
	if kube == nil || kube.Namespace != ref.Namespace {
		return "", fmt.Errorf("managed host access requires same-namespace Kubernetes client")
	}
	name, err := V1Alpha2ManagedHostServiceAccountName(ref)
	if err != nil {
		return "", err
	}
	deployment, err := V1Alpha2GenesisDeploymentName(ref.UID)
	if err != nil {
		return "", err
	}
	binding := managedHostRoleBindingName(name)
	if err := rejectLegacyManagedHostAccess(ctx, kube, ref); err != nil {
		return "", err
	}
	allowed, err := liveManagedHostServiceAccounts(ctx, kube, ref.Namespace)
	if err != nil {
		return "", err
	}
	if allowed[name].UID == "" {
		allowed, err = liveManagedHostServiceAccounts(ctx, kube, ref.Namespace)
		if err != nil || allowed[name].UID == "" {
			return "", fmt.Errorf("current cluster is absent from the live managed host identity set")
		}
	}
	if staleErr := rejectStaleManagedHostAccess(ctx, kube, ref, name, allowed); staleErr != nil {
		allowed, err = liveManagedHostServiceAccounts(ctx, kube, ref.Namespace)
		if err != nil || allowed[name].UID == "" {
			return "", staleErr
		}
		if err = rejectStaleManagedHostAccess(ctx, kube, ref, name, allowed); err != nil {
			return "", err
		}
	}
	if err := verifyLiveManagedHostServiceAccounts(ctx, kube, ref.Namespace, allowed); err != nil {
		return "", err
	}
	objects := managedHostAccessObjects(ref, name, deployment, binding)
	for _, obj := range objects {
		var live map[string]json.RawMessage
		err := kube.Get(ctx, obj.path+"/"+obj.name, &live)
		var apiErr *APIError
		if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
			continue
		}
		if err != nil {
			return "", fmt.Errorf("preflight managed host %s: %w", obj.kind, err)
		}
		if err := validateManagedHostAccessObject(live, obj.kind, obj.name, ref, obj.body); err != nil {
			return "", err
		}
	}
	if err := verifyV1Alpha2ManagedHostClusterIdentity(ctx, kube, ref); err != nil {
		return "", err
	}
	for _, obj := range objects {
		// shortcut: RBAC cannot constrain create by name or body; add admission
		// policy when protection against a compromised operator is in scope.
		var created map[string]json.RawMessage
		err := kube.Post(ctx, obj.path, obj.body, &created)
		var apiErr *APIError
		if err == nil {
			if err := validateManagedHostAccessObject(created, obj.kind, obj.name, ref, obj.body); err != nil {
				return "", err
			}
		} else {
			if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusConflict {
				return "", fmt.Errorf("create managed host %s: %w", obj.kind, err)
			}
			var live map[string]json.RawMessage
			if err := kube.Get(ctx, obj.path+"/"+obj.name, &live); err != nil {
				return "", fmt.Errorf("load managed host %s: %w", obj.kind, err)
			}
			if err := validateManagedHostAccessObject(live, obj.kind, obj.name, ref, obj.body); err != nil {
				return "", err
			}
		}
		if obj.kind == "Role" {
			if err := verifyV1Alpha2ManagedHostClusterIdentity(ctx, kube, ref); err != nil {
				return "", err
			}
		}
	}
	if err := verifyV1Alpha2ManagedHostClusterIdentity(ctx, kube, ref); err != nil {
		return "", err
	}
	// shortcut: create is not atomic with CR identity; admission is required
	// to eliminate the final deletion/recreation race after RoleBinding create.
	return name, nil
}

func rejectLegacyManagedHostAccess(ctx context.Context, kube *Kubernetes, ref V1Alpha2ClusterRef) error {
	base := "/api/v1/namespaces/" + ref.Namespace
	for _, object := range []string{
		base + "/serviceaccounts/" + V1Alpha2ManagedHostServiceAccount,
		"/apis/rbac.authorization.k8s.io/v1/namespaces/" + ref.Namespace + "/roles/" + V1Alpha2ManagedHostServiceAccount,
		"/apis/rbac.authorization.k8s.io/v1/namespaces/" + ref.Namespace + "/rolebindings/" + V1Alpha2ManagedHostServiceAccount,
	} {
		var live map[string]json.RawMessage
		err := kube.Get(ctx, object, &live)
		var apiErr *APIError
		if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
			continue
		}
		if err != nil {
			return fmt.Errorf("inspect legacy managed host access: %w", err)
		}
		return fmt.Errorf("legacy shared managed host access in namespace %q requires manual cleanup", ref.Namespace)
	}
	return nil
}

// Live CR UIDs define the only reserved access names, so unlabeled remnants
// fail closed without guessing a deleted cluster or deleting shared RBAC.
func liveManagedHostServiceAccounts(ctx context.Context, kube *Kubernetes, namespace string) (map[string]V1Alpha2ClusterRef, error) {
	base := "/apis/" + V1Alpha2APIVersion + "/namespaces/" + namespace + "/rhizaclusters"
	allowed := map[string]V1Alpha2ClusterRef{}
	seenUID := map[string]bool{}
	seenToken := map[string]bool{}
	token, version := "", ""
	for {
		path := base
		if token != "" {
			path += "?continue=" + url.QueryEscape(token)
		}
		var page struct {
			APIVersion, Kind string
			Metadata         struct{ ResourceVersion, Continue string }
			Items            []map[string]json.RawMessage
		}
		if err := kube.Get(ctx, path, &page); err != nil {
			return nil, fmt.Errorf("list live managed host identities: %w", err)
		}
		if page.APIVersion != V1Alpha2APIVersion || page.Kind != "RhizaClusterList" || !v1alpha2NonBlank.MatchString(page.Metadata.ResourceVersion) || version != "" && version != page.Metadata.ResourceVersion || page.Items == nil {
			return nil, fmt.Errorf("invalid live managed host identity list")
		}
		version = page.Metadata.ResourceVersion
		for _, item := range page.Items {
			meta, err := jsonObject(item["metadata"])
			if err != nil {
				return nil, fmt.Errorf("invalid live managed host identity metadata")
			}
			var ref V1Alpha2ClusterRef
			_ = json.Unmarshal(meta["name"], &ref.Name)
			_ = json.Unmarshal(meta["namespace"], &ref.Namespace)
			_ = json.Unmarshal(meta["uid"], &ref.UID)
			name, err := V1Alpha2ManagedHostServiceAccountName(ref)
			if err != nil || ref.Namespace != namespace || seenUID[ref.UID] || allowed[name].UID != "" {
				return nil, fmt.Errorf("invalid or repeated live managed host identity")
			}
			seenUID[ref.UID] = true
			allowed[name] = ref
		}
		token = page.Metadata.Continue
		if token == "" {
			return allowed, nil
		}
		if seenToken[token] {
			return nil, fmt.Errorf("repeated live managed host identity continuation")
		}
		seenToken[token] = true
	}
}

func verifyLiveManagedHostServiceAccounts(ctx context.Context, kube *Kubernetes, namespace string, allowed map[string]V1Alpha2ClusterRef) error {
	for _, cluster := range allowed {
		if err := verifyV1Alpha2ManagedHostClusterIdentity(ctx, kube, cluster); err != nil {
			return fmt.Errorf("live managed host identity changed: %w", err)
		}
	}
	return nil
}

func verifyV1Alpha2ManagedHostClusterIdentity(ctx context.Context, kube *Kubernetes, ref V1Alpha2ClusterRef) error {
	path := "/apis/" + V1Alpha2APIVersion + "/namespaces/" + ref.Namespace + "/rhizaclusters/" + ref.Name
	var live struct {
		APIVersion, Kind string
		Metadata         v1alpha2BootstrapMetadata
	}
	if err := kube.Get(ctx, path, &live); err != nil {
		return fmt.Errorf("load live cluster identity: %w", err)
	}
	if live.APIVersion != V1Alpha2APIVersion || live.Kind != "RhizaCluster" || live.Metadata.Name != ref.Name || live.Metadata.Namespace != ref.Namespace || live.Metadata.UID != ref.UID || live.Metadata.ResourceVersion == "" || live.Metadata.DeletionTimestamp != nil {
		return fmt.Errorf("live cluster identity differs from the access snapshot")
	}
	return nil
}

func rejectStaleManagedHostAccess(ctx context.Context, kube *Kubernetes, ref V1Alpha2ClusterRef, expected string, allowed map[string]V1Alpha2ClusterRef) error {
	base := "/apis/rbac.authorization.k8s.io/v1/namespaces/" + ref.Namespace
	roles, err := listManagedHostAccess(ctx, kube, base+"/roles", "rbac.authorization.k8s.io/v1", "RoleList")
	if err != nil {
		return err
	}
	for _, role := range roles {
		name, clusterUID, labels, err := managedHostAccessMetadata(role)
		if err != nil {
			return err
		}
		if managedHostServiceAccountPattern.MatchString(name) && allowed[name].UID == "" || labels["rhiza.mrchypark.dev/cluster-name"] == managedHostClusterNameLabel(ref.Name) && clusterUID != ref.UID || name != expected && managedHostRoleTargetsCluster(role, ref.Name) {
			return fmt.Errorf("stale managed host Role for cluster name %q requires manual cleanup", ref.Name)
		}
	}
	for _, list := range []struct {
		path, apiVersion, kind string
	}{
		{"/api/v1/namespaces/" + ref.Namespace + "/serviceaccounts", "v1", "ServiceAccountList"},
		{base + "/rolebindings", "rbac.authorization.k8s.io/v1", "RoleBindingList"},
	} {
		items, err := listManagedHostAccess(ctx, kube, list.path, list.apiVersion, list.kind)
		if err != nil {
			return err
		}
		for _, item := range items {
			name, clusterUID, labels, err := managedHostAccessMetadata(item)
			if err != nil {
				return err
			}
			stale := labels["rhiza.mrchypark.dev/cluster-name"] == managedHostClusterNameLabel(ref.Name) && clusterUID != ref.UID
			if list.kind == "ServiceAccountList" {
				stale = stale || managedHostServiceAccountPattern.MatchString(name) && allowed[name].UID == ""
			} else {
				stale = stale || managedHostBindingUsesUnknownAccess(item, ref.Namespace, allowed)
			}
			if stale {
				return fmt.Errorf("stale managed host access for cluster name %q requires manual cleanup", ref.Name)
			}
		}
	}
	return nil
}

func managedHostBindingUsesUnknownAccess(binding map[string]json.RawMessage, namespace string, allowed map[string]V1Alpha2ClusterRef) bool {
	name, _, _, _ := managedHostAccessMetadata(binding)
	if serviceAccount := managedHostBindingName(name); serviceAccount != "" && allowed[serviceAccount].UID == "" {
		return true
	}
	var roleRef struct {
		Kind, Name string
	}
	_ = json.Unmarshal(binding["roleRef"], &roleRef)
	if managedHostServiceAccountPattern.MatchString(roleRef.Name) && (roleRef.Kind != "Role" || allowed[roleRef.Name].UID == "") {
		return true
	}
	var subjects []struct {
		Kind, Name, Namespace string
	}
	_ = json.Unmarshal(binding["subjects"], &subjects)
	for _, subject := range subjects {
		if subject.Kind == "ServiceAccount" && subject.Namespace == namespace && managedHostServiceAccountPattern.MatchString(subject.Name) && allowed[subject.Name].UID == "" {
			return true
		}
	}
	return false
}

func managedHostBindingName(bindingName string) string {
	serviceAccount := strings.TrimSuffix(bindingName, "-binding")
	if serviceAccount != bindingName && managedHostServiceAccountPattern.MatchString(serviceAccount) {
		return serviceAccount
	}
	return ""
}

func listManagedHostAccess(ctx context.Context, kube *Kubernetes, path, apiVersion, kind string) ([]map[string]json.RawMessage, error) {
	var list struct {
		APIVersion, Kind string
		Items            []map[string]json.RawMessage
	}
	if err := kube.Get(ctx, path, &list); err != nil {
		return nil, fmt.Errorf("list managed host %s: %w", kind, err)
	}
	if list.APIVersion != apiVersion || list.Kind != kind || list.Items == nil {
		return nil, fmt.Errorf("invalid managed host %s", kind)
	}
	return list.Items, nil
}

func managedHostAccessMetadata(obj map[string]json.RawMessage) (string, string, map[string]string, error) {
	meta, err := jsonObject(obj["metadata"])
	if err != nil {
		return "", "", nil, fmt.Errorf("invalid managed host access metadata")
	}
	var name string
	var labels map[string]string
	_ = json.Unmarshal(meta["name"], &name)
	_ = json.Unmarshal(meta["labels"], &labels)
	return name, labels["rhiza.mrchypark.dev/cluster-uid"], labels, nil
}

func managedHostRoleTargetsCluster(role map[string]json.RawMessage, clusterName string) bool {
	var rules []struct {
		APIGroups     []string `json:"apiGroups"`
		Resources     []string `json:"resources"`
		ResourceNames []string `json:"resourceNames"`
	}
	if json.Unmarshal(role["rules"], &rules) != nil {
		return false
	}
	for _, rule := range rules {
		if !managedHostRuleMatches(rule.APIGroups, managedHostAPIGroup) {
			continue
		}
		for _, resource := range rule.Resources {
			if resource != "*" && resource != "rhizaclusters/status" {
				continue
			}
			for _, name := range rule.ResourceNames {
				if name == clusterName {
					return true
				}
			}
		}
	}
	return false
}

func managedHostRuleMatches(values []string, expected string) bool {
	for _, value := range values {
		if value == "*" || value == expected {
			return true
		}
	}
	return false
}

func validateManagedHostAccessObject(obj map[string]json.RawMessage, kind, name string, ref V1Alpha2ClusterRef, desired map[string]any) error {
	if obj == nil {
		return fmt.Errorf("managed host %s response is empty", kind)
	}
	var gotKind string
	_ = json.Unmarshal(obj["kind"], &gotKind)
	meta, err := jsonObject(obj["metadata"])
	if err != nil {
		return fmt.Errorf("managed host %s metadata is invalid", kind)
	}
	var gotName, gotNamespace, uid string
	_ = json.Unmarshal(meta["name"], &gotName)
	_ = json.Unmarshal(meta["namespace"], &gotNamespace)
	_ = json.Unmarshal(meta["uid"], &uid)
	if gotKind != kind || gotName != name || gotNamespace != ref.Namespace || uid == "" {
		return fmt.Errorf("managed host %s identity is invalid", kind)
	}
	if raw, ok := meta["deletionTimestamp"]; ok && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return fmt.Errorf("managed host %s is deleting", kind)
	}
	for key, want := range map[string]string{
		"app.kubernetes.io/managed-by":         managedHostAccessManagedBy,
		"rhiza.mrchypark.dev/cluster-name":     managedHostClusterNameLabel(ref.Name),
		"rhiza.mrchypark.dev/cluster-uid":      ref.UID,
		"rhiza.mrchypark.dev/access-contract":  "v1alpha2-host-v1",
		"rhiza.mrchypark.dev/access-lifecycle": "retain-until-manual-cleanup",
	} {
		var labels map[string]string
		if json.Unmarshal(meta["labels"], &labels) != nil || labels[key] != want {
			return fmt.Errorf("managed host %s label %s differs", kind, key)
		}
	}
	if raw, exists := meta["ownerReferences"]; exists && !bytes.Equal(bytes.TrimSpace(raw), []byte("[]")) && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return fmt.Errorf("managed host %s must not be garbage-collected before its Pods", kind)
	}
	for _, key := range []string{"rules", "roleRef", "subjects"} {
		if _, exists := desired[key]; exists {
			want, _ := json.Marshal(desired[key])
			if !sameJSON(obj[key], want) {
				return fmt.Errorf("managed host %s %s differs", kind, key)
			}
		}
	}
	return nil
}

func managedHostOwnerMatches(raw json.RawMessage, ref V1Alpha2ClusterRef) bool {
	var owners []struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Name       string `json:"name"`
		UID        string `json:"uid"`
	}
	if json.Unmarshal(raw, &owners) != nil || len(owners) != 1 {
		return false
	}
	o := owners[0]
	return o.APIVersion == V1Alpha2APIVersion && o.Kind == "RhizaCluster" && o.Name == ref.Name && o.UID == ref.UID
}

func validateV1Alpha2ManagedHostDeployment(ctx context.Context, kube *Kubernetes, ref V1Alpha2ClusterRef) error {
	name, err := V1Alpha2ManagedHostServiceAccountName(ref)
	if err != nil {
		return err
	}
	depName, err := V1Alpha2GenesisDeploymentName(ref.UID)
	if err != nil {
		return err
	}
	apiPath := "/apis/apps/v1/namespaces/" + ref.Namespace + "/deployments/" + depName
	var obj map[string]json.RawMessage
	if err := kube.Get(ctx, apiPath, &obj); err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
			return nil
		}
		return fmt.Errorf("load managed Deployment for host account validation: %w", err)
	}
	meta, err := jsonObject(obj["metadata"])
	if err != nil || !managedHostOwnerMatches(meta["ownerReferences"], ref) {
		return fmt.Errorf("managed Deployment owner is invalid")
	}
	spec, err := jsonObject(obj["spec"])
	if err != nil {
		return fmt.Errorf("managed Deployment spec is invalid")
	}
	template, err := jsonObject(spec["template"])
	if err != nil {
		return fmt.Errorf("managed Deployment template is invalid")
	}
	pod, err := jsonObject(template["spec"])
	if err != nil {
		return fmt.Errorf("managed Deployment Pod spec is invalid")
	}
	var current string
	_ = json.Unmarshal(pod["serviceAccountName"], &current)
	var deprecated string
	_ = json.Unmarshal(pod["serviceAccount"], &deprecated)
	if current != name || deprecated != "" && deprecated != name || !sameJSON(pod["automountServiceAccountToken"], json.RawMessage("true")) {
		return fmt.Errorf("managed Deployment uses a service account outside the per-cluster contract; manual migration required")
	}
	return nil
}
