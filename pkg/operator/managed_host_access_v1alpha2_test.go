package operator

import (
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
)

type managedHostAccessAPI struct {
	t        *testing.T
	mu       sync.Mutex
	objects  map[string]map[string]json.RawMessage
	clusters []V1Alpha2ClusterRef
	creates  []string
	puts     []string
}

func newManagedHostAccessAPI(t *testing.T, clusters ...V1Alpha2ClusterRef) *managedHostAccessAPI {
	if len(clusters) == 0 {
		clusters = []V1Alpha2ClusterRef{phase2Ref}
	}
	return &managedHostAccessAPI{t: t, objects: map[string]map[string]json.RawMessage{}, clusters: clusters}
}

func managedHostClusterListJSON(clusters []V1Alpha2ClusterRef) string {
	items := make([]map[string]any, 0, len(clusters))
	for _, ref := range clusters {
		items = append(items, map[string]any{"metadata": map[string]any{"name": ref.Name, "namespace": ref.Namespace, "uid": ref.UID, "resourceVersion": "1"}})
	}
	return string(mustManagedJSON(map[string]any{"apiVersion": "rhiza.mrchypark.dev/v1alpha2", "kind": "RhizaClusterList", "metadata": map[string]any{"resourceVersion": "1"}, "items": items}))
}

func addManagedHostAccessObjects(api *managedHostAccessAPI, ref V1Alpha2ClusterRef) {
	serviceAccount, _ := V1Alpha2ManagedHostServiceAccountName(ref)
	deployment, _ := V1Alpha2GenesisDeploymentName(ref.UID)
	api.objects["/api/v1/namespaces/ns/serviceaccounts/"+serviceAccount] = map[string]json.RawMessage{
		"kind": json.RawMessage(`"ServiceAccount"`), "metadata": mustManagedJSON(map[string]any{"name": serviceAccount, "namespace": "ns", "uid": "sa-" + ref.UID}),
	}
	api.objects["/apis/rbac.authorization.k8s.io/v1/namespaces/ns/roles/"+serviceAccount] = map[string]json.RawMessage{
		"kind": json.RawMessage(`"Role"`), "metadata": mustManagedJSON(map[string]any{"name": serviceAccount, "namespace": "ns", "uid": "role-" + ref.UID}), "rules": mustManagedJSON(managedHostRoleRules(ref, deployment)),
	}
	api.objects["/apis/rbac.authorization.k8s.io/v1/namespaces/ns/rolebindings/"+managedHostRoleBindingName(serviceAccount)] = map[string]json.RawMessage{
		"kind": json.RawMessage(`"RoleBinding"`), "metadata": mustManagedJSON(map[string]any{"name": managedHostRoleBindingName(serviceAccount), "namespace": "ns", "uid": "binding-" + ref.UID}),
		"roleRef":  mustManagedJSON(map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": serviceAccount}),
		"subjects": mustManagedJSON([]any{map[string]any{"kind": "ServiceAccount", "name": serviceAccount, "namespace": "ns"}}),
	}
}

func (a *managedHostAccessAPI) roundTrip(r *http.Request) (*http.Response, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if r.URL.Path == "/apis/rhiza.mrchypark.dev/v1alpha2/namespaces/ns/rhizaclusters" && r.Method == http.MethodGet {
		return phase2Response(http.StatusOK, managedHostClusterListJSON(a.clusters)), true, nil
	}
	if strings.HasPrefix(r.URL.Path, "/apis/rhiza.mrchypark.dev/v1alpha2/namespaces/ns/rhizaclusters/") && r.Method == http.MethodGet {
		name := strings.TrimPrefix(r.URL.Path, "/apis/rhiza.mrchypark.dev/v1alpha2/namespaces/ns/rhizaclusters/")
		for _, ref := range a.clusters {
			if ref.Name == name {
				obj := map[string]any{"apiVersion": "rhiza.mrchypark.dev/v1alpha2", "kind": "RhizaCluster", "metadata": map[string]any{"name": ref.Name, "namespace": ref.Namespace, "uid": ref.UID, "resourceVersion": "1"}}
				return phase2Response(http.StatusOK, string(mustManagedJSON(obj))), true, nil
			}
		}
		return phase2Response(http.StatusNotFound, `{}`), true, nil
	}
	collections := []struct {
		path, kind, listKind string
	}{
		{"/api/v1/namespaces/ns/serviceaccounts", "ServiceAccount", "ServiceAccountList"},
		{"/apis/rbac.authorization.k8s.io/v1/namespaces/ns/roles", "Role", "RoleList"},
		{"/apis/rbac.authorization.k8s.io/v1/namespaces/ns/rolebindings", "RoleBinding", "RoleBindingList"},
	}
	for _, collection := range collections {
		if r.URL.Path == collection.path && r.Method == http.MethodGet {
			items := make([]map[string]json.RawMessage, 0)
			for path, obj := range a.objects {
				if strings.HasPrefix(path, collection.path+"/") {
					items = append(items, obj)
				}
			}
			apiVersion := "rbac.authorization.k8s.io/v1"
			if collection.kind == "ServiceAccount" {
				apiVersion = "v1"
			}
			return phase2Response(http.StatusOK, string(mustManagedJSON(map[string]any{"apiVersion": apiVersion, "kind": collection.listKind, "metadata": map[string]any{"resourceVersion": "1"}, "items": items}))), true, nil
		}
		if r.URL.Path == collection.path && r.Method == http.MethodPost {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				return nil, true, err
			}
			var obj map[string]json.RawMessage
			if err := json.Unmarshal(body, &obj); err != nil {
				return nil, true, err
			}
			meta, _ := jsonObject(obj["metadata"])
			var name string
			if json.Unmarshal(meta["name"], &name) != nil || name == "" {
				return nil, true, json.Unmarshal([]byte(`{}`), new(any))
			}
			key := collection.path + "/" + name
			if _, exists := a.objects[key]; exists {
				return phase2Response(http.StatusConflict, `{}`), true, nil
			}
			meta["uid"], _ = json.Marshal("uid-" + name)
			meta["resourceVersion"], _ = json.Marshal("1")
			obj["metadata"], _ = json.Marshal(meta)
			a.objects[key] = obj
			a.creates = append(a.creates, key)
			return phase2Response(http.StatusCreated, string(mustManagedJSON(obj))), true, nil
		}
		if strings.HasPrefix(r.URL.Path, collection.path+"/") && r.Method == http.MethodGet {
			obj, ok := a.objects[r.URL.Path]
			if !ok {
				return phase2Response(http.StatusNotFound, `{}`), true, nil
			}
			return phase2Response(http.StatusOK, string(mustManagedJSON(obj))), true, nil
		}
	}
	return nil, false, nil
}

func TestEnsureManagedHostAccessRejectsUnlabeledLegacySharedObjects(t *testing.T) {
	for _, tc := range []struct {
		name, path, kind string
	}{
		{"service account", "/api/v1/namespaces/ns/serviceaccounts/" + V1Alpha2ManagedHostServiceAccount, "ServiceAccount"},
		{"role", "/apis/rbac.authorization.k8s.io/v1/namespaces/ns/roles/" + V1Alpha2ManagedHostServiceAccount, "Role"},
		{"role binding", "/apis/rbac.authorization.k8s.io/v1/namespaces/ns/rolebindings/" + V1Alpha2ManagedHostServiceAccount, "RoleBinding"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := newManagedHostAccessAPI(t)
			api.objects[tc.path] = map[string]json.RawMessage{
				"kind":     json.RawMessage(`"` + tc.kind + `"`),
				"metadata": mustManagedJSON(map[string]any{"name": V1Alpha2ManagedHostServiceAccount, "namespace": "ns", "uid": "legacy"}),
			}
			kube := phase2Store(t, func(r *http.Request) (*http.Response, error) {
				if response, handled, err := api.roundTrip(r); handled {
					return response, err
				}
				t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
				return nil, nil
			}).Kube
			ref := V1Alpha2ClusterRef{Name: "cluster", Namespace: "ns", UID: "uid"}
			if _, err := ensureV1Alpha2ManagedHostAccess(t.Context(), kube, ref); err == nil || !strings.Contains(err.Error(), "manual cleanup") {
				t.Fatalf("legacy access was accepted: %v", err)
			}
			if len(api.creates) != 0 {
				t.Fatal("legacy access reached resource creation")
			}
		})
	}
}

func TestEnsureManagedHostAccessRejectsStaleServiceAccountAndBinding(t *testing.T) {
	old := V1Alpha2ClusterRef{Name: "cluster", Namespace: "ns", UID: "old-uid"}
	current := V1Alpha2ClusterRef{Name: "cluster", Namespace: "ns", UID: "new-uid"}
	for _, kind := range []string{"ServiceAccount", "RoleBinding"} {
		t.Run(kind, func(t *testing.T) {
			api := newManagedHostAccessAPI(t, current)
			oldName, _ := V1Alpha2ManagedHostServiceAccountName(old)
			objectName := oldName
			path := "/api/v1/namespaces/ns/serviceaccounts/" + oldName
			if kind == "RoleBinding" {
				objectName = managedHostRoleBindingName(oldName)
				path = "/apis/rbac.authorization.k8s.io/v1/namespaces/ns/rolebindings/" + objectName
			}
			api.objects[path] = map[string]json.RawMessage{
				"kind":     json.RawMessage(`"` + kind + `"`),
				"metadata": mustManagedJSON(map[string]any{"name": objectName, "namespace": "ns", "uid": "stale"}),
			}
			kube := phase2Store(t, func(r *http.Request) (*http.Response, error) {
				if response, handled, err := api.roundTrip(r); handled {
					return response, err
				}
				t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
				return nil, nil
			}).Kube
			if _, err := ensureV1Alpha2ManagedHostAccess(t.Context(), kube, current); err == nil || !strings.Contains(err.Error(), "manual cleanup") {
				t.Fatalf("stale %s was accepted: %v", kind, err)
			}
			if len(api.creates) != 0 {
				t.Fatalf("stale %s reached resource creation", kind)
			}
		})
	}
}

func TestEnsureManagedHostAccessAllowsAnotherLiveClusterIdentity(t *testing.T) {
	current := V1Alpha2ClusterRef{Name: "current", Namespace: "ns", UID: "current-uid"}
	other := V1Alpha2ClusterRef{Name: "other", Namespace: "ns", UID: "other-uid"}
	api := newManagedHostAccessAPI(t, current, other)
	addManagedHostAccessObjects(api, other)
	kube := phase2Store(t, func(r *http.Request) (*http.Response, error) {
		if response, handled, err := api.roundTrip(r); handled {
			return response, err
		}
		t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		return nil, nil
	}).Kube
	if _, err := ensureV1Alpha2ManagedHostAccess(t.Context(), kube, current); err != nil {
		t.Fatal(err)
	}
	if len(api.creates) != 3 {
		t.Fatalf("valid other-cluster identity blocked current provisioning: creates=%v", api.creates)
	}
}

func TestEnsureManagedHostAccessRefreshesLiveIdentitiesOnStaleCandidate(t *testing.T) {
	current := V1Alpha2ClusterRef{Name: "current", Namespace: "ns", UID: "current-uid"}
	other := V1Alpha2ClusterRef{Name: "other", Namespace: "ns", UID: "other-uid"}
	api := newManagedHostAccessAPI(t, current, other)
	addManagedHostAccessObjects(api, other)
	listCalls := 0
	kube := phase2Store(t, func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/apis/rhiza.mrchypark.dev/v1alpha2/namespaces/ns/rhizaclusters" && r.Method == http.MethodGet {
			listCalls++
			if listCalls == 1 {
				return phase2Response(http.StatusOK, managedHostClusterListJSON([]V1Alpha2ClusterRef{current})), nil
			}
		}
		if response, handled, err := api.roundTrip(r); handled {
			return response, err
		}
		t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		return nil, nil
	}).Kube
	if _, err := ensureV1Alpha2ManagedHostAccess(t.Context(), kube, current); err != nil {
		t.Fatal(err)
	}
	if listCalls < 2 || len(api.creates) != 3 {
		t.Fatalf("concurrent live identity was not refreshed: lists=%d creates=%v", listCalls, api.creates)
	}
}

func TestEnsureManagedHostAccessRejectsIdentityDeletedAfterList(t *testing.T) {
	current := V1Alpha2ClusterRef{Name: "current", Namespace: "ns", UID: "current-uid"}
	deleted := V1Alpha2ClusterRef{Name: "deleted", Namespace: "ns", UID: "deleted-uid"}
	deletedSA, _ := V1Alpha2ManagedHostServiceAccountName(deleted)
	api := newManagedHostAccessAPI(t, current)
	api.objects["/api/v1/namespaces/ns/serviceaccounts/"+deletedSA] = map[string]json.RawMessage{
		"kind": json.RawMessage(`"ServiceAccount"`), "metadata": mustManagedJSON(map[string]any{"name": deletedSA, "namespace": "ns", "uid": "stale-sa"}),
	}
	kube := phase2Store(t, func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/apis/rhiza.mrchypark.dev/v1alpha2/namespaces/ns/rhizaclusters" && r.Method == http.MethodGet {
			return phase2Response(http.StatusOK, managedHostClusterListJSON([]V1Alpha2ClusterRef{current, deleted})), nil
		}
		if response, handled, err := api.roundTrip(r); handled {
			return response, err
		}
		t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		return nil, nil
	}).Kube
	if _, err := ensureV1Alpha2ManagedHostAccess(t.Context(), kube, current); err == nil || !strings.Contains(err.Error(), "identity changed") {
		t.Fatalf("snapshot-deleted identity was accepted: %v", err)
	}
	if len(api.creates) != 0 {
		t.Fatal("snapshot-deleted identity reached provisioning")
	}
}

func TestEnsureManagedHostAccessChecksClusterIdentityAfterCreation(t *testing.T) {
	current := V1Alpha2ClusterRef{Name: "current", Namespace: "ns", UID: "current-uid"}
	api := newManagedHostAccessAPI(t, current)
	clusterGets := 0
	kube := phase2Store(t, func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/apis/rhiza.mrchypark.dev/v1alpha2/namespaces/ns/rhizaclusters/current" && r.Method == http.MethodGet {
			clusterGets++
			if clusterGets == 3 {
				return phase2Response(http.StatusNotFound, `{}`), nil
			}
		}
		if response, handled, err := api.roundTrip(r); handled {
			return response, err
		}
		t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		return nil, nil
	}).Kube
	if _, err := ensureV1Alpha2ManagedHostAccess(t.Context(), kube, current); err == nil {
		t.Fatal("post-create cluster replacement was accepted")
	}
	if len(api.creates) != 2 {
		t.Fatalf("expected detection before RoleBinding creation, creates=%v", api.creates)
	}
	serviceAccount, _ := V1Alpha2ManagedHostServiceAccountName(current)
	if _, exists := api.objects["/apis/rbac.authorization.k8s.io/v1/namespaces/ns/rolebindings/"+managedHostRoleBindingName(serviceAccount)]; exists {
		t.Fatal("RoleBinding was created after the cluster identity changed")
	}
}

func TestEnsureManagedHostAccessRejectsUnlabeledBindingByRoleRefOrSubject(t *testing.T) {
	stale := V1Alpha2ClusterRef{Name: "cluster", Namespace: "ns", UID: "old-uid"}
	current := V1Alpha2ClusterRef{Name: "cluster", Namespace: "ns", UID: "new-uid"}
	staleName, _ := V1Alpha2ManagedHostServiceAccountName(stale)
	for _, tc := range []struct {
		name    string
		role    string
		subject string
	}{
		{"roleRef", staleName, "default"},
		{"subject", "unrelated-role", staleName},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := newManagedHostAccessAPI(t, current)
			bindingName := "worker-binding"
			api.objects["/apis/rbac.authorization.k8s.io/v1/namespaces/ns/rolebindings/"+bindingName] = map[string]json.RawMessage{
				"kind": json.RawMessage(`"RoleBinding"`), "metadata": mustManagedJSON(map[string]any{"name": bindingName, "namespace": "ns", "uid": "binding"}),
				"roleRef":  mustManagedJSON(map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": tc.role}),
				"subjects": mustManagedJSON([]any{map[string]any{"kind": "ServiceAccount", "name": tc.subject, "namespace": "ns"}}),
			}
			kube := phase2Store(t, func(r *http.Request) (*http.Response, error) {
				if response, handled, err := api.roundTrip(r); handled {
					return response, err
				}
				t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
				return nil, nil
			}).Kube
			if _, err := ensureV1Alpha2ManagedHostAccess(t.Context(), kube, current); err == nil || !strings.Contains(err.Error(), "manual cleanup") {
				t.Fatalf("unlabeled stale binding roleRef/subject was accepted: %v", err)
			}
			if len(api.creates) != 0 {
				t.Fatal("stale binding reached provisioning")
			}
		})
	}
}

func TestManagedHostServiceAccountIsUIDScopedAndStable(t *testing.T) {
	ref := V1Alpha2ClusterRef{Name: "cluster", Namespace: "ns", UID: "uid-one"}
	first, err := V1Alpha2ManagedHostServiceAccountName(ref)
	if err != nil {
		t.Fatal(err)
	}
	second, _ := V1Alpha2ManagedHostServiceAccountName(ref)
	if first != second || len(first) > 253 || !v1alpha2Name.MatchString(first) {
		t.Fatalf("unstable or invalid ServiceAccount name %q", first)
	}
	ref.UID = "uid-two"
	recreated, _ := V1Alpha2ManagedHostServiceAccountName(ref)
	if first == recreated {
		t.Fatal("recreated CR reused the old ServiceAccount name")
	}
}

func TestManagedHostLabelsHandleLongClusterName(t *testing.T) {
	short := V1Alpha2ClusterRef{Name: "short", Namespace: "ns", UID: "uid"}
	if got := managedHostLabels(short)["rhiza.mrchypark.dev/cluster-name"]; got != short.Name {
		t.Fatalf("short cluster name label changed: %q", got)
	}
	long := short
	long.Name = strings.Repeat("a", 200)
	labels := managedHostLabels(long)
	got := labels["rhiza.mrchypark.dev/cluster-name"]
	if len(got) > 63 || got == long.Name || got != managedHostClusterNameLabel(long.Name) {
		t.Fatalf("unsafe long cluster name label %q", got)
	}
	for key, value := range labels {
		if len(value) > 63 {
			t.Fatalf("label %s exceeds Kubernetes limit: %q", key, value)
		}
	}
}

func TestEnsureManagedHostAccessCreatesExactUIDScopedResources(t *testing.T) {
	api := newManagedHostAccessAPI(t)
	kube := phase2Store(t, func(r *http.Request) (*http.Response, error) {
		if response, handled, err := api.roundTrip(r); handled {
			return response, err
		}
		t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		return nil, nil
	}).Kube
	ref := V1Alpha2ClusterRef{Name: "cluster", Namespace: "ns", UID: "uid"}
	name, err := ensureV1Alpha2ManagedHostAccess(t.Context(), kube, ref)
	if err != nil {
		t.Fatal(err)
	}
	wantCreates := []string{
		"/api/v1/namespaces/ns/serviceaccounts/" + name,
		"/apis/rbac.authorization.k8s.io/v1/namespaces/ns/roles/" + name,
		"/apis/rbac.authorization.k8s.io/v1/namespaces/ns/rolebindings/" + name + "-binding",
	}
	if !reflect.DeepEqual(api.creates, wantCreates) {
		t.Fatalf("create order = %v, want %v", api.creates, wantCreates)
	}
	if _, err := ensureV1Alpha2ManagedHostAccess(t.Context(), kube, ref); err != nil {
		t.Fatal(err)
	}
	if len(api.creates) != 3 || len(api.puts) != 0 {
		t.Fatal("idempotent reconcile changed resources")
	}
	role := api.objects["/apis/rbac.authorization.k8s.io/v1/namespaces/ns/roles/"+name]
	var rules []map[string]any
	if json.Unmarshal(role["rules"], &rules) != nil || len(rules) != 5 {
		t.Fatal("host Role has unexpected rules")
	}
	for _, rule := range rules[:2] {
		if !reflect.DeepEqual(rule["resourceNames"], []any{ref.Name}) {
			t.Fatalf("host Role is not cluster-name scoped: %v", rule)
		}
	}
	var deploymentRule map[string]any
	for _, rule := range rules[2:] {
		resources := rule["resources"].([]any)
		if len(resources) == 1 && resources[0] == "deployments" {
			deploymentRule = rule
		}
	}
	deploymentName, _ := V1Alpha2GenesisDeploymentName(ref.UID)
	if deploymentRule == nil || !reflect.DeepEqual(deploymentRule["resourceNames"], []any{deploymentName}) {
		t.Fatal("host Deployment get is not name-scoped")
	}
	meta, _ := jsonObject(role["metadata"])
	if raw, ok := meta["ownerReferences"]; ok && string(raw) != "[]" && string(raw) != "null" {
		t.Fatal("host Role is owner-garbage-collected before terminating Pods")
	}
	var labels map[string]string
	_ = json.Unmarshal(meta["labels"], &labels)
	if labels["rhiza.mrchypark.dev/access-lifecycle"] != "retain-until-manual-cleanup" {
		t.Fatal("host Role lacks explicit cleanup lifecycle")
	}
}

func TestEnsureManagedHostAccessRejectsStaleOrExpandedRole(t *testing.T) {
	api := newManagedHostAccessAPI(t)
	kube := phase2Store(t, func(r *http.Request) (*http.Response, error) {
		if response, handled, err := api.roundTrip(r); handled {
			return response, err
		}
		t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		return nil, nil
	}).Kube
	ref := V1Alpha2ClusterRef{Name: "cluster", Namespace: "ns", UID: "uid"}
	name, _ := ensureV1Alpha2ManagedHostAccess(t.Context(), kube, ref)
	rolePath := "/apis/rbac.authorization.k8s.io/v1/namespaces/ns/roles/" + name
	role := api.objects[rolePath]
	role["rules"], _ = json.Marshal([]any{map[string]any{"apiGroups": []any{"rhiza.mrchypark.dev"}, "resources": []any{"rhizaclusters/status"}, "verbs": []any{"get", "update"}}})
	if _, err := ensureV1Alpha2ManagedHostAccess(t.Context(), kube, ref); err == nil {
		t.Fatal("expanded pre-existing Role was adopted")
	}
}

func TestManagedHostDeploymentRequiresPerClusterServiceAccount(t *testing.T) {
	ref := V1Alpha2ClusterRef{Name: "cluster", Namespace: "ns", UID: "uid"}
	target, _ := V1Alpha2ManagedHostServiceAccountName(ref)
	deploymentName, _ := V1Alpha2GenesisDeploymentName(ref.UID)
	for _, tc := range []struct {
		name, current string
		wantError     bool
	}{
		{name: "current", current: target},
		{name: "legacy", current: V1Alpha2ManagedHostServiceAccount, wantError: true},
		{name: "arbitrary", current: "some-other-service-account", wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deployment := map[string]any{
				"apiVersion": "apps/v1", "kind": "Deployment",
				"metadata": map[string]any{"name": deploymentName, "namespace": ref.Namespace, "uid": "deployment", "resourceVersion": "1", "ownerReferences": []any{map[string]any{"apiVersion": V1Alpha2APIVersion, "kind": "RhizaCluster", "name": ref.Name, "uid": ref.UID, "controller": true}}},
				"spec":     map[string]any{"template": map[string]any{"metadata": map[string]any{}, "spec": map[string]any{"serviceAccountName": tc.current, "serviceAccount": tc.current, "automountServiceAccountToken": true}}},
			}
			kube := phase2Store(t, func(r *http.Request) (*http.Response, error) {
				if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/deployments/"+deploymentName) {
					return phase2Response(http.StatusOK, string(mustManagedJSON(deployment))), nil
				}
				t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
				return nil, nil
			}).Kube
			err := validateV1Alpha2ManagedHostDeployment(t.Context(), kube, ref)
			if (err != nil) != tc.wantError {
				t.Fatal(err)
			}
		})
	}
}

func TestRecreatedClusterNameRejectsStalePerClusterRole(t *testing.T) {
	current := V1Alpha2ClusterRef{Name: "cluster", Namespace: "ns", UID: "new-uid"}
	api := newManagedHostAccessAPI(t, current)
	kube := phase2Store(t, func(r *http.Request) (*http.Response, error) {
		if response, handled, err := api.roundTrip(r); handled {
			return response, err
		}
		t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		return nil, nil
	}).Kube
	old := V1Alpha2ClusterRef{Name: "cluster", Namespace: "ns", UID: "old-uid"}
	oldName, _ := V1Alpha2ManagedHostServiceAccountName(old)
	api.objects["/apis/rbac.authorization.k8s.io/v1/namespaces/ns/roles/"+oldName] = map[string]json.RawMessage{
		"apiVersion": json.RawMessage(`"rbac.authorization.k8s.io/v1"`),
		"kind":       json.RawMessage(`"Role"`),
		"metadata":   mustManagedJSON(map[string]any{"name": oldName, "namespace": "ns", "uid": "role-uid", "labels": managedHostLabels(old)}),
		"rules":      mustManagedJSON([]any{}),
	}
	if _, err := ensureV1Alpha2ManagedHostAccess(t.Context(), kube, current); err == nil || !strings.Contains(err.Error(), "manual cleanup") {
		t.Fatalf("stale recreated CR access was accepted: %v", err)
	}
}

func TestRecreatedClusterNameRejectsUnlabeledRoleTargetingItsStatus(t *testing.T) {
	current := V1Alpha2ClusterRef{Name: "cluster", Namespace: "ns", UID: "new-uid"}
	api := newManagedHostAccessAPI(t, current)
	kube := phase2Store(t, func(r *http.Request) (*http.Response, error) {
		if response, handled, err := api.roundTrip(r); handled {
			return response, err
		}
		t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		return nil, nil
	}).Kube
	old := V1Alpha2ClusterRef{Name: "cluster", Namespace: "ns", UID: "old-uid"}
	oldName, _ := V1Alpha2ManagedHostServiceAccountName(old)
	deployment, _ := V1Alpha2GenesisDeploymentName(old.UID)
	api.objects["/apis/rbac.authorization.k8s.io/v1/namespaces/ns/roles/"+oldName] = map[string]json.RawMessage{
		"kind":     json.RawMessage(`"Role"`),
		"metadata": mustManagedJSON(map[string]any{"name": oldName, "namespace": "ns", "uid": "role-uid"}),
		"rules":    mustManagedJSON(managedHostRoleRules(old, deployment)),
	}
	if _, err := ensureV1Alpha2ManagedHostAccess(t.Context(), kube, current); err == nil || !strings.Contains(err.Error(), "manual cleanup") {
		t.Fatalf("unlabeled stale Role was accepted: %v", err)
	}
}
