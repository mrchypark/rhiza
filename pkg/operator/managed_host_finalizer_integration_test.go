package operator

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// TestManagedHostFinalizerAPIServerContract exercises the administrator-owned
// lifecycle boundary against a real API server. The contract runner owns the
// disposable cluster; this test only removes resources it can clean safely.
func TestManagedHostFinalizerAPIServerContract(t *testing.T) {
	server, caPath := os.Getenv("RHIZA_CONTRACT_SERVER"), os.Getenv("RHIZA_CONTRACT_CA")
	unauthorizedToken, kubeconfig := os.Getenv("RHIZA_CONTRACT_TOKEN"), os.Getenv("RHIZA_CONTRACT_KUBECONFIG")
	contextName, namespace := os.Getenv("RHIZA_CONTRACT_CONTEXT"), os.Getenv("RHIZA_CONTRACT_NAMESPACE")
	if server == "" || caPath == "" || unauthorizedToken == "" || kubeconfig == "" || contextName == "" || namespace == "" {
		t.Skip("API server managed-host finalizer contract environment not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	kubectlRaw := func(input string, args ...string) ([]byte, error) {
		t.Helper()
		args = append([]string{"--kubeconfig", kubeconfig, "--context", contextName, "--namespace", namespace, "--request-timeout=30s"}, args...)
		cmd := exec.CommandContext(ctx, "kubectl", args...)
		cmd.Stdin = strings.NewReader(input)
		return cmd.CombinedOutput()
	}
	kubectl := func(input string, args ...string) []byte {
		t.Helper()
		out, err := kubectlRaw(input, args...)
		if err != nil {
			t.Fatalf("kubectl: %v: %s", err, out)
		}
		return out
	}
	cleanupKubectl := func(input string, args ...string) {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		args = append([]string{"--kubeconfig", kubeconfig, "--context", contextName, "--namespace", namespace, "--request-timeout=10s"}, args...)
		cmd := exec.CommandContext(cleanupCtx, "kubectl", args...)
		cmd.Stdin = strings.NewReader(input)
		_, _ = cmd.CombinedOutput()
	}

	ca, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatal("invalid contract CA")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	t.Cleanup(transport.CloseIdleConnections)
	host := &Kubernetes{BaseURL: server, Namespace: namespace, TokenFile: unauthorizedToken, Client: &http.Client{Transport: transport}}

	nonce := time.Now().UnixNano()
	name := fmt.Sprintf("managed-finalizer-%d", nonce)
	clusterSpec := fmt.Sprintf(`{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaCluster","metadata":{"name":%q,"namespace":%q},"spec":{"logicalID":"test","genesisIntent":"Create","applicationProtocol":"rhiza-managed-request-append-only/v1","voterCount":3,"workload":{"podTemplate":{"spec":{"containers":[{"name":"app","image":"example.invalid/app"}]}}}}}`, name, namespace)
	kubectl(clusterSpec, "create", "-f", "-")
	var cluster map[string]json.RawMessage
	if err := json.Unmarshal(kubectl("", "get", "rhizacluster", name, "-o", "json"), &cluster); err != nil {
		t.Fatal(err)
	}
	meta, err := jsonObject(cluster["metadata"])
	if err != nil {
		t.Fatal(err)
	}
	var uid string
	if json.Unmarshal(meta["uid"], &uid) != nil || uid == "" {
		t.Fatal("created cluster has no UID")
	}
	ref := V1Alpha2ClusterRef{Name: name, Namespace: namespace, UID: uid}

	deploymentName, err := V1Alpha2GenesisDeploymentName(uid)
	if err != nil {
		t.Fatal(err)
	}
	deployment := fmt.Sprintf(`{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":%q,"namespace":%q,"labels":{"rhiza.mrchypark.dev/cluster-uid":%q,"rhiza.mrchypark.dev/generation":"0"},"ownerReferences":[{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaCluster","name":%q,"uid":%q,"controller":true}]},"spec":{"replicas":0,"selector":{"matchLabels":{"app":%q}},"template":{"metadata":{"labels":{"app":%q}},"spec":{"containers":[{"name":"app","image":"example.invalid/app"}]}}}}`, deploymentName, namespace, uid, name, uid, name, name)
	kubectl(deployment, "create", "-f", "-")
	t.Cleanup(func() {
		cleanupKubectl("", "delete", "deployment", deploymentName, "--ignore-not-found", "--wait=false")
	})

	bootstrap, digest, err := testBootstrapPlan(ref)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap.EvidenceFormat = sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol
	members := make([]quepaxa.Member, len(bootstrap.Members))
	for i, member := range bootstrap.Members {
		members[i] = member.Member
	}
	digest, err = types.ManagedBootstrapPlanDigestForProtocol(string(bootstrap.ClusterID), members, bootstrap.PhysicalBinding, bootstrap.EvidenceFormat, bootstrap.Durability, bootstrap.Reconfiguration, bootstrap.EvidenceFormat)
	if err != nil {
		t.Fatal(err)
	}
	var liveDeployment struct {
		Metadata struct {
			UID string `json:"uid"`
		} `json:"metadata"`
	}
	if err := host.Get(ctx, "/apis/apps/v1/namespaces/"+namespace+"/deployments/"+deploymentName, &liveDeployment); err != nil {
		t.Fatal(err)
	}
	init, err := NewV1Alpha2ApplicationInitPlan(ref, "test", liveDeployment.Metadata.UID, name+"-assignment", bootstrap.PhysicalBinding, digest, bootstrap.EvidenceFormat)
	if err != nil {
		t.Fatal(err)
	}
	init.ApplicationProtocol = sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol
	store := &V1Alpha2StatusStore{Kube: host}
	if _, err := InitializeV1Alpha2AuthorityWithBootstrap(ctx, store, ref, deploymentName, init, bootstrap); err != nil {
		t.Fatal(err)
	}
	unused, err := LoadV1Alpha2ApplicationInit(ctx, store, ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, observation, err := CommitV1Alpha2ApplicationInit(ctx, store, unused, init, json.RawMessage(`{"value":"initial"}`)); err != nil || observation != V1Alpha2CASApplied {
		t.Fatalf("consume initialization: %s %v", observation, err)
	}

	admission, err := LoadV1Alpha2Admission(ctx, store, ref)
	if err != nil {
		t.Fatal(err)
	}
	authority := admission.Authority()
	final := make([]types.NodeID, 0, len(members))
	for _, member := range members {
		final = append(final, types.NodeID(member.ID))
	}
	slices.Sort(final)
	old := types.NodeID("retired-voter")
	preparing := V1Alpha2RepairRecord{State: "Preparing", Plan: V1Alpha2RepairPlan{
		Sequence: V1Alpha2RepairLeave, OperationID: name + "-prepare", ClusterUID: ref.UID,
		Generation: authority.Generation, DeploymentUID: authority.Binding.DeploymentUID, StorageID: authority.Binding.StorageID,
		ExpectedConfigID: 1, ExpectedAbortSlot: 0, OldNodeID: old, OldWALIdentity: strings.Repeat("a", 64),
		Replacement:         V1Alpha2RepairMember{PodName: string(old), PodUID: string(old), NodeID: old, PublicKey: members[0].PublicKey, PeerURL: "quic://10.0.0.9:9090"},
		BootstrapPlanDigest: init.BootstrapPlanDigest, FinalVoterIDs: final,
	}}
	status, err := jsonObject(admission.status.object["status"])
	if err != nil {
		t.Fatal(err)
	}
	status["repair"], _ = json.Marshal(preparing)
	admission.status.object["status"], _ = json.Marshal(status)
	if err := host.Put(ctx, "/apis/rhiza.mrchypark.dev/v1alpha2/namespaces/"+namespace+"/rhizaclusters/"+name+"/status", admission.status.object, nil); err != nil {
		t.Fatalf("seed Preparing repair through the API: %v", err)
	}
	prepared, err := LoadV1Alpha2Admission(ctx, store, ref)
	if err != nil {
		t.Fatal(err)
	}
	preparingStatus := bytes.Clone(prepared.status.object["status"])
	if record, ok := prepared.Repair(); !ok || record.State != "Preparing" {
		t.Fatalf("Preparing repair was not retained: %+v", record)
	}

	accessName := fmt.Sprintf("managed-finalizer-%d", nonce)
	deniedName := accessName + "-denied"
	accessYAML := fmt.Sprintf(`apiVersion: v1
kind: ServiceAccount
metadata:
  name: %s
  namespace: %s
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: %s
  namespace: %s
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: %s
  namespace: %s
rules:
- apiGroups: ["rhiza.mrchypark.dev"]
  resources: ["rhizaclusters"]
  verbs: ["get", "update", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: %s
  namespace: %s
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: %s
subjects:
- kind: ServiceAccount
  name: %s
  namespace: %s
`, accessName, namespace, deniedName, namespace, accessName, namespace, accessName, namespace, accessName, accessName, namespace)
	kubectl(accessYAML, "apply", "-f", "-")
	t.Cleanup(func() {
		for _, object := range []string{"rolebinding/" + accessName, "role/" + accessName, "serviceaccount/" + accessName, "serviceaccount/" + deniedName} {
			cleanupKubectl("", "delete", object, "--ignore-not-found", "--wait=false")
		}
	})
	accessTokenPath := t.TempDir() + "/managed-finalizer.token"
	if err := os.WriteFile(accessTokenPath, bytes.TrimSpace(kubectl("", "create", "token", accessName, "--duration=10m")), 0600); err != nil {
		t.Fatal(err)
	}
	authorized := &Kubernetes{BaseURL: server, Namespace: namespace, TokenFile: accessTokenPath, Client: &http.Client{Transport: transport}}
	deniedTokenPath := t.TempDir() + "/managed-finalizer-denied.token"
	if err := os.WriteFile(deniedTokenPath, bytes.TrimSpace(kubectl("", "create", "token", deniedName, "--duration=10m")), 0600); err != nil {
		t.Fatal(err)
	}
	denied := &Kubernetes{BaseURL: server, Namespace: namespace, TokenFile: deniedTokenPath, Client: &http.Client{Transport: transport}}

	const otherFinalizer = "example.invalid/keep"
	kubectl("", "patch", "rhizacluster", name, "--type=merge", "-p", `{"metadata":{"finalizers":["example.invalid/keep"]}}`)
	deleting, err := reconcileV1Alpha2ManagedHostClusterLifecycle(ctx, authorized, ref)
	if err != nil || deleting {
		t.Fatalf("install managed-host finalizer: deleting=%t err=%v", deleting, err)
	}
	record, err := loadManagedHostClusterRecord(ctx, authorized, ref)
	if err != nil || !slices.Equal(record.finalizers, []string{otherFinalizer, V1Alpha2ManagedHostCleanupFinalizer}) {
		t.Fatalf("installed finalizers=%v err=%v", record.finalizers, err)
	}
	afterInstall, err := LoadV1Alpha2Admission(ctx, store, ref)
	if err != nil || !sameJSON(afterInstall.status.object["status"], preparingStatus) {
		t.Fatalf("finalizer install changed Preparing status: %v", err)
	}

	var live map[string]json.RawMessage
	if err := authorized.Get(ctx, managedHostClusterPath(ref), &live); err != nil {
		t.Fatal(err)
	}
	liveMeta, _ := jsonObject(live["metadata"])
	var unauthorizedFinalizers []string
	_ = json.Unmarshal(liveMeta["finalizers"], &unauthorizedFinalizers)
	unauthorizedFinalizers = append(unauthorizedFinalizers, "example.invalid/unauthorized")
	liveMeta["finalizers"], _ = json.Marshal(unauthorizedFinalizers)
	live["metadata"], _ = json.Marshal(liveMeta)
	err = denied.Put(ctx, managedHostClusterPath(ref), live, nil)
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusForbidden {
		t.Fatalf("unauthorized finalizer update was not forbidden: %v", err)
	}
	unchanged, err := loadManagedHostClusterRecord(ctx, authorized, ref)
	if err != nil || !slices.Equal(unchanged.finalizers, record.finalizers) {
		t.Fatalf("unauthorized update changed finalizers: %v %v", unchanged.finalizers, err)
	}

	kubectl("", "delete", "rhizacluster", name, "--wait=false")
	var deletionTime, resourceVersion string
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if err := host.Get(ctx, managedHostClusterPath(ref), &live); err != nil {
			t.Fatal(err)
		}
		liveMeta, _ = jsonObject(live["metadata"])
		_ = json.Unmarshal(liveMeta["deletionTimestamp"], &deletionTime)
		_ = json.Unmarshal(liveMeta["resourceVersion"], &resourceVersion)
		if deletionTime != "" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if deletionTime == "" {
		t.Fatal("cluster deletion was not blocked by its finalizers")
	}
	var finalizers []string
	_ = json.Unmarshal(liveMeta["finalizers"], &finalizers)
	if !slices.Equal(finalizers, []string{otherFinalizer, V1Alpha2ManagedHostCleanupFinalizer}) {
		t.Fatalf("deletion changed finalizers: %v", finalizers)
	}
	deleting, err = reconcileV1Alpha2ManagedHostClusterLifecycle(ctx, authorized, ref)
	if err != nil || !deleting {
		t.Fatalf("deleting lifecycle: deleting=%t err=%v", deleting, err)
	}
	var deletingObject map[string]json.RawMessage
	if err := authorized.Get(ctx, managedHostClusterPath(ref), &deletingObject); err != nil {
		t.Fatal(err)
	}
	deletingStatus, err := jsonObject(deletingObject["status"])
	if err != nil {
		t.Fatal(err)
	}
	var deletingRepair V1Alpha2RepairRecord
	if decodeV1Alpha2(deletingStatus["repair"], &deletingRepair) != nil || deletingRepair.State != "Preparing" || !sameJSON(json.RawMessage(deletingObject["status"]), preparingStatus) {
		t.Fatalf("deletion changed Preparing status: %+v", deletingRepair)
	}

	finalizerPatch := func(remove, wantRevision, wantState string) []byte {
		index := slices.Index(finalizers, remove)
		if index < 0 {
			t.Fatalf("finalizer %q is absent", remove)
		}
		remaining := slices.Delete(slices.Clone(finalizers), index, index+1)
		ops := []any{
			map[string]any{"op": "test", "path": "/metadata/uid", "value": uid},
			map[string]any{"op": "test", "path": "/metadata/resourceVersion", "value": wantRevision},
			map[string]any{"op": "test", "path": "/metadata/deletionTimestamp", "value": deletionTime},
			map[string]any{"op": "test", "path": "/metadata/finalizers", "value": finalizers},
			map[string]any{"op": "test", "path": fmt.Sprintf("/metadata/finalizers/%d", index), "value": remove},
			map[string]any{"op": "test", "path": "/status/repair/state", "value": wantState},
			map[string]any{"op": "replace", "path": "/metadata/finalizers", "value": remaining},
		}
		data, _ := json.Marshal(ops)
		return data
	}
	stale := finalizerPatch(otherFinalizer, "stale-resource-version", "Preparing")
	if out, err := kubectlRaw(string(stale), "patch", "rhizacluster", name, "--type=json", "-p", string(stale)); err == nil {
		t.Fatalf("stale conditional cleanup was not rejected: %v %s", err, out)
	}
	var afterStale map[string]json.RawMessage
	if err := authorized.Get(ctx, managedHostClusterPath(ref), &afterStale); err != nil {
		t.Fatal(err)
	}
	afterStaleMeta, _ := jsonObject(afterStale["metadata"])
	var afterStaleRV string
	var afterStaleFinalizers []string
	_ = json.Unmarshal(afterStaleMeta["resourceVersion"], &afterStaleRV)
	_ = json.Unmarshal(afterStaleMeta["finalizers"], &afterStaleFinalizers)
	if afterStaleRV != resourceVersion || !slices.Equal(afterStaleFinalizers, finalizers) {
		t.Fatalf("rejected stale cleanup changed the object: rv=%s finalizers=%v", afterStaleRV, afterStaleFinalizers)
	}
	incomplete := finalizerPatch(V1Alpha2ManagedHostCleanupFinalizer, resourceVersion, "Completed")
	if out, err := kubectlRaw(string(incomplete), "patch", "rhizacluster", name, "--type=json", "-p", string(incomplete)); err == nil {
		t.Fatalf("incomplete Preparing release was not rejected: %v %s", err, out)
	}
	var afterIncomplete map[string]json.RawMessage
	if err := authorized.Get(ctx, managedHostClusterPath(ref), &afterIncomplete); err != nil {
		t.Fatal(err)
	}
	afterIncompleteMeta, _ := jsonObject(afterIncomplete["metadata"])
	var afterIncompleteRV string
	var afterIncompleteFinalizers []string
	_ = json.Unmarshal(afterIncompleteMeta["resourceVersion"], &afterIncompleteRV)
	_ = json.Unmarshal(afterIncompleteMeta["finalizers"], &afterIncompleteFinalizers)
	if afterIncompleteRV != resourceVersion || !slices.Equal(afterIncompleteFinalizers, finalizers) {
		t.Fatalf("rejected incomplete release changed the object: rv=%s finalizers=%v", afterIncompleteRV, afterIncompleteFinalizers)
	}
	approvedOther := finalizerPatch(otherFinalizer, resourceVersion, "Preparing")
	kubectl(string(approvedOther), "patch", "rhizacluster", name, "--type=json", "-p", string(approvedOther))
	afterRelease, err := loadManagedHostClusterRecord(ctx, authorized, ref)
	if err != nil || !slices.Equal(afterRelease.finalizers, []string{V1Alpha2ManagedHostCleanupFinalizer}) {
		t.Fatalf("exact unrelated-finalizer cleanup failed: %v %v", afterRelease.finalizers, err)
	}
	var finalObject map[string]json.RawMessage
	if err := authorized.Get(ctx, managedHostClusterPath(ref), &finalObject); err != nil {
		t.Fatal(err)
	}
	finalStatus, err := jsonObject(finalObject["status"])
	if err != nil {
		t.Fatal(err)
	}
	var finalRepair V1Alpha2RepairRecord
	if decodeV1Alpha2(finalStatus["repair"], &finalRepair) != nil || finalRepair.State != "Preparing" || !sameJSON(json.RawMessage(finalObject["status"]), preparingStatus) {
		t.Fatalf("cleanup changed incomplete Preparing ownership: %+v", finalRepair)
	}
	deleting, err = reconcileV1Alpha2ManagedHostClusterLifecycle(ctx, authorized, ref)
	if err != nil || !deleting {
		t.Fatalf("final managed-host lifecycle changed: deleting=%t err=%v", deleting, err)
	}
}
