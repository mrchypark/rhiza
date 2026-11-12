package operator

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"
	"time"
)

// The harness seeds authority as an administrative fixture, not as genesis
// evidence or permission to activate a recovery.
func TestV1Alpha2StatusAPIServerContract(t *testing.T) {
	server := os.Getenv("RHIZA_CONTRACT_SERVER")
	if server == "" {
		t.Skip("API server contract environment not configured")
	}
	getenv := func(key string) string {
		t.Helper()
		value := os.Getenv(key)
		if value == "" {
			t.Fatalf("%s is required", key)
		}
		return value
	}
	ca, err := os.ReadFile(getenv("RHIZA_CONTRACT_CA"))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatal("invalid contract CA")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	t.Cleanup(transport.CloseIdleConnections)
	ns := getenv("RHIZA_CONTRACT_NAMESPACE")
	store := &V1Alpha2StatusStore{Kube: &Kubernetes{BaseURL: server, Namespace: ns, TokenFile: getenv("RHIZA_CONTRACT_TOKEN"), Client: &http.Client{Transport: transport}}}
	ctx := context.Background()
	ref := V1Alpha2ClusterRef{Name: "status-contract", Namespace: ns, UID: getenv("RHIZA_CONTRACT_STATUS_UID")}
	old, err := store.Load(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	queue := json.RawMessage(`{"test":"winner"}`)
	updated, err := store.compareAndSwapQueueObserved(ctx, old, queue)
	if err != nil {
		t.Fatal(err)
	}
	if updated.ResourceVersion() == old.ResourceVersion() || !sameJSON(updated.authorityRaw, old.authorityRaw) || !sameJSON(updated.Queue(), queue) {
		t.Fatal("status update changed authority or failed to persist queue and revision")
	}
	var before, after map[string]json.RawMessage
	if err := json.Unmarshal(old.object["status"], &before); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(updated.object["status"], &after); err != nil {
		t.Fatal(err)
	}
	if !sameJSON(before["other"], after["other"]) {
		t.Fatal("unrelated status changed")
	}
	if _, err := store.compareAndSwapQueueObserved(ctx, old, json.RawMessage(`{"test":"loser"}`)); !errors.Is(err, ErrV1Alpha2StatusConflict) {
		t.Fatalf("stale update: %v", err)
	}
	live, err := store.Load(ctx, ref)
	if err != nil || !sameJSON(live.Queue(), queue) {
		t.Fatalf("winner overwritten: %v", err)
	}
	missing := V1Alpha2ClusterRef{Name: "missing-authority", Namespace: ns, UID: getenv("RHIZA_CONTRACT_MISSING_UID")}
	if _, err := store.Load(ctx, missing); err == nil {
		t.Fatal("missing authority accepted")
	}
	var missingObject map[string]json.RawMessage
	if err := store.Kube.Get(ctx, "/apis/rhiza.mrchypark.dev/v1alpha2/namespaces/"+ns+"/rhizaclusters/missing-authority", &missingObject); err != nil {
		t.Fatal(err)
	}
	if len(missingObject["status"]) != 0 && string(missingObject["status"]) != "{}" && string(missingObject["status"]) != "null" {
		t.Fatal("missing authority was bootstrapped")
	}
	queueRef := V1Alpha2ClusterRef{Name: "queue-contract", Namespace: ns, UID: getenv("RHIZA_CONTRACT_QUEUE_UID")}
	var list struct {
		Items []struct {
			Metadata struct{ Name, UID, CreationTimestamp string }
		}
	}
	if err := store.Kube.Get(ctx, "/apis/rhiza.mrchypark.dev/v1alpha2/namespaces/"+ns+"/rhizarecoveries", &list); err != nil {
		t.Fatal(err)
	}
	var candidates []struct{ Name, UID, CreationTimestamp string }
	for _, item := range list.Items {
		if item.Metadata.Name == "queue-first" || item.Metadata.Name == "queue-second" {
			candidates = append(candidates, item.Metadata)
		}
	}
	if len(candidates) != 2 {
		t.Fatalf("expected two protected requests, got %d", len(candidates))
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].CreationTimestamp == candidates[j].CreationTimestamp {
			return candidates[i].UID < candidates[j].UID
		}
		return candidates[i].CreationTimestamp < candidates[j].CreationTimestamp
	})
	claim, err := ClaimV1Alpha2Recovery(ctx, store, queueRef)
	if err != nil || claim.RequestUID != candidates[0].UID {
		t.Fatalf("claim %+v: %v", claim, err)
	}
	again, err := ClaimV1Alpha2Recovery(ctx, store, queueRef)
	if err != nil || again.OperationID != claim.OperationID || again.RequestUID != claim.RequestUID {
		t.Fatalf("resume %+v: %v", again, err)
	}
}

func TestV1Alpha2ReplicaScaleAPIServerContract(t *testing.T) {
	if os.Getenv("RHIZA_CONTRACT_KUBECONFIG") == "" {
		t.Skip("API server contract kubeconfig not configured")
	}
	required := func(key string) string {
		t.Helper()
		value := os.Getenv(key)
		if value == "" {
			t.Fatalf("%s is required", key)
		}
		return value
	}
	ns := required("RHIZA_CONTRACT_NAMESPACE")
	kubectl := func(input string, args ...string) ([]byte, error) {
		args = append([]string{"--kubeconfig", required("RHIZA_CONTRACT_KUBECONFIG"), "--context", required("RHIZA_CONTRACT_CONTEXT"), "--namespace", ns, "--request-timeout=30s"}, args...)
		cmd := exec.Command("kubectl", args...)
		if input != "" {
			cmd.Stdin = strings.NewReader(input)
		}
		return cmd.CombinedOutput()
	}
	cluster := func(name string, replicas *int32) string {
		spec := map[string]any{"logicalID": "external-scale", "voterCount": 3, "workload": map[string]any{"podTemplate": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": "example.invalid/app"}}}}}}
		if replicas != nil {
			spec["replicas"] = *replicas
		}
		return string(mustManagedJSON(map[string]any{"apiVersion": V1Alpha2APIVersion, "kind": "RhizaCluster", "metadata": map[string]any{"name": name, "namespace": ns}, "spec": spec}))
	}
	create := func(name string, replicas *int32) map[string]any {
		out, err := kubectl(cluster(name, replicas), "create", "-f", "-", "-o", "json")
		if err != nil {
			t.Fatalf("create %s: %v: %s", name, err, out)
		}
		var object map[string]any
		if err := json.Unmarshal(out, &object); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if out, err := kubectl("", "delete", "rhizacluster", name, "--wait=false"); err != nil && !strings.Contains(string(out), "NotFound") {
				t.Errorf("cleanup %s: %v: %s", name, err, out)
			}
		})
		return object
	}
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	invalid := "replica-scale-invalid-" + suffix
	if out, err := kubectl(cluster(invalid, phase1Int32(4)), "create", "-f", "-"); err == nil || !strings.Contains(strings.ToLower(string(out)), "initial replicas must equal votercount") {
		t.Fatalf("invalid initial replicas accepted: %v: %s", err, out)
	}
	pre := "replica-scale-pre-" + suffix
	create(pre, phase1Int32(3))
	if out, err := kubectl("", "scale", "rhizacluster/"+pre, "--replicas=5"); err == nil || !strings.Contains(strings.ToLower(string(out)), "initial replicas must equal votercount") {
		t.Fatalf("pre-genesis scale proposal accepted: %v: %s", err, out)
	}
	name := "replica-scale-" + suffix
	created := create(name, phase1Int32(3))
	uid := created["metadata"].(map[string]any)["uid"].(string)
	ref := V1Alpha2ClusterRef{Name: name, Namespace: ns, UID: uid}
	selector := "rhiza.mrchypark.dev/cluster-uid=" + uid + ",rhiza.mrchypark.dev/generation=0"
	bootstrap, digest, err := testBootstrapPlan(ref)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := NewV1Alpha2ApplicationInitPlan(ref, "external-scale", "deployment", "scale-assignment", bootstrap.PhysicalBinding, digest, bootstrap.EvidenceFormat)
	if err != nil {
		t.Fatal(err)
	}
	genesis := v1alpha2Genesis{OwnerUID: uid, LogicalID: "external-scale", DeploymentUID: "deployment", StorageID: plan.StorageID, IntentHash: plan.IntentHash}
	status := string(mustManagedJSON(map[string]any{"status": map[string]any{"replicas": 3, "selector": selector, "genesis": genesis, "bootstrapPlan": bootstrap, "applicationInit": V1Alpha2ApplicationInitAssignment{Plan: plan, State: "Unused"}, "authority": map[string]any{"everApplicationReserved": false, "owner": map[string]any{"namespace": ns, "name": name, "uid": uid, "logicalID": "external-scale"}, "generation": 0, "binding": map[string]any{"deploymentUID": "deployment", "storageID": plan.StorageID}, "pendingWrite": false}}}))
	if out, err := kubectl(status, "patch", "rhizacluster", name, "--subresource=status", "--type=merge", "-p", status); err != nil {
		t.Fatalf("seed scale status: %v: %s", err, out)
	}
	scalePath := "/apis/rhiza.mrchypark.dev/v1alpha2/namespaces/" + ns + "/rhizaclusters/" + name + "/scale"
	out, err := kubectl("", "get", "--raw", scalePath)
	if err != nil {
		t.Fatalf("get scale: %v: %s", err, out)
	}
	var scale struct {
		Spec struct {
			Replicas int32 `json:"replicas"`
		} `json:"spec"`
		Status struct {
			Replicas int32  `json:"replicas"`
			Selector string `json:"selector"`
		} `json:"status"`
	}
	if json.Unmarshal(out, &scale) != nil || scale.Spec.Replicas != 3 || scale.Status.Replicas != 3 || scale.Status.Selector != selector {
		t.Fatalf("scale paths: %s", out)
	}
	if out, err := kubectl("", "scale", "rhizacluster/"+name, "--replicas=5"); err != nil {
		t.Fatalf("post-genesis scale proposal: %v: %s", err, out)
	}
	for _, replicas := range []int{2, 65} {
		if out, err := kubectl("", "scale", "rhizacluster/"+name, fmt.Sprintf("--replicas=%d", replicas)); err == nil {
			t.Fatalf("scale bound %d accepted: %s", replicas, out)
		}
	}
	step := V1Alpha2ReplicaStep{Generation: 0, DeploymentUID: "deployment", DeploymentResourceVersion: "91", ConfigID: 5, From: 3, To: 4, ExpiresAt: time.Now().Add(time.Hour).UTC()}
	stepPatch := string(mustManagedJSON(map[string]any{"status": map[string]any{"replicaStep": step}}))
	if out, err := kubectl(stepPatch, "patch", "rhizacluster", name, "--subresource=status", "--type=merge", "-p", stepPatch); err != nil {
		t.Fatalf("seed pending step: %v: %s", err, out)
	}
	if out, err := kubectl("", "scale", "rhizacluster/"+name, "--replicas=6"); err != nil {
		t.Fatalf("scale with pending step: %v: %s", err, out)
	}
	var after map[string]any
	out, err = kubectl("", "get", "rhizacluster", name, "-o", "json")
	if err != nil || json.Unmarshal(out, &after) != nil {
		t.Fatalf("get proposal-only result: %v: %s", err, out)
	}
	afterSpec := after["spec"].(map[string]any)
	afterStatus := after["status"].(map[string]any)
	if afterSpec["replicas"] != float64(6) || afterSpec["voterCount"] != float64(3) || afterSpec["logicalID"] != "external-scale" {
		t.Fatalf("scale changed more than the proposal: %v", afterSpec)
	}
	if !sameJSON(mustManagedJSON(afterStatus["replicaStep"]), mustManagedJSON(step)) || !sameJSON(mustManagedJSON(afterStatus["bootstrapPlan"]), mustManagedJSON(bootstrap)) || !sameJSON(mustManagedJSON(afterStatus["genesis"]), mustManagedJSON(genesis)) {
		t.Fatal("scale reinterpreted durable membership or bootstrap state")
	}
	manual := "replica-scale-manual-" + suffix
	create(manual, nil)
	manualScale := "/apis/rhiza.mrchypark.dev/v1alpha2/namespaces/" + ns + "/rhizaclusters/" + manual + "/scale"
	if out, err := kubectl("", "get", "--raw", manualScale); err == nil {
		t.Fatalf("manual cluster unexpectedly exposed usable scale: %s", out)
	}
}
