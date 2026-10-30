package operator

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestV1Alpha2GenesisAPIServerContract(t *testing.T) {
	server := os.Getenv("RHIZA_CONTRACT_SERVER")
	if server == "" {
		t.Skip("API server contract environment not configured")
	}
	required := func(key string) string {
		t.Helper()
		v := os.Getenv(key)
		if v == "" {
			t.Fatalf("%s is required", key)
		}
		return v
	}
	ns, uid := required("RHIZA_CONTRACT_NAMESPACE"), required("RHIZA_CONTRACT_GENESIS_UID")
	name, err := V1Alpha2GenesisDeploymentName(uid)
	if err != nil {
		t.Fatal(err)
	}
	kubectl := func(input string, args ...string) {
		t.Helper()
		args = append([]string{"--kubeconfig", required("RHIZA_CONTRACT_KUBECONFIG"), "--context", required("RHIZA_CONTRACT_CONTEXT"), "--namespace", ns, "--request-timeout=30s"}, args...)
		cmd := exec.Command("kubectl", args...)
		cmd.Stdin = strings.NewReader(input)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("kubectl: %v: %s", err, out)
		}
	}
	rejectStatusPatch := func(patch, message string) {
		t.Helper()
		args := []string{"--kubeconfig", required("RHIZA_CONTRACT_KUBECONFIG"), "--context", required("RHIZA_CONTRACT_CONTEXT"), "--namespace", ns, "--request-timeout=30s", "patch", "rhizacluster", "genesis-contract", "--subresource=status", "--type=merge", "-p", patch}
		out, err := exec.Command("kubectl", args...).CombinedOutput()
		if err == nil || !strings.Contains(string(out), message) {
			t.Fatalf("expected status rejection %q: %v %s", message, err, out)
		}
	}
	deployment := fmt.Sprintf(`{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":%q,"namespace":%q,"labels":{"rhiza.mrchypark.dev/cluster-uid":%q,"rhiza.mrchypark.dev/generation":"0"},"ownerReferences":[{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaCluster","name":"genesis-contract","uid":%q,"controller":true}]},"spec":{"replicas":0,"selector":{"matchLabels":{"app":"genesis-contract"}},"template":{"metadata":{"labels":{"app":"genesis-contract"}},"spec":{"containers":[{"name":"app","image":"example.invalid/app"}]}}}}`, name, ns, uid, uid)
	kubectl(deployment, "create", "-f", "-")
	ca, err := os.ReadFile(required("RHIZA_CONTRACT_CA"))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatal("invalid contract CA")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	t.Cleanup(transport.CloseIdleConnections)
	store := &V1Alpha2StatusStore{Kube: &Kubernetes{BaseURL: server, Namespace: ns, TokenFile: required("RHIZA_CONTRACT_TOKEN"), Client: &http.Client{Transport: transport}}}
	ctx := context.Background()
	ref := V1Alpha2ClusterRef{Name: "genesis-contract", Namespace: ns, UID: uid}
	var liveDeployment struct {
		Metadata struct {
			UID string `json:"uid"`
		} `json:"metadata"`
	}
	if err := store.Kube.Get(ctx, "/apis/apps/v1/namespaces/"+ns+"/deployments/"+name, &liveDeployment); err != nil {
		t.Fatal(err)
	}
	bootstrap, digest, err := testBootstrapPlan(ref)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := NewV1Alpha2ApplicationInitPlan(ref, "test", liveDeployment.Metadata.UID, "genesis-contract-assignment", bootstrap.PhysicalBinding, digest, bootstrap.EvidenceFormat)
	if err != nil {
		t.Fatal(err)
	}
	genesis := v1alpha2Genesis{OwnerUID: uid, LogicalID: "test", DeploymentUID: liveDeployment.Metadata.UID, StorageID: plan.StorageID, IntentHash: plan.IntentHash}
	bad, err := json.Marshal(map[string]any{"status": map[string]any{"genesis": genesis, "authority": map[string]any{"owner": map[string]string{"namespace": ns, "name": ref.Name, "uid": uid, "logicalID": "test"}, "generation": 0, "binding": map[string]string{"deploymentUID": liveDeployment.Metadata.UID, "storageID": plan.StorageID}, "pendingWrite": false, "everApplicationReserved": false}}})
	if err != nil {
		t.Fatal(err)
	}
	rejectStatusPatch(string(bad), "new genesis requires an unused initialization assignment")
	first, err := InitializeV1Alpha2AuthorityWithBootstrap(ctx, store, ref, name, plan, bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	if first.Authority().Generation != 0 || first.Authority().Binding.DeploymentUID == "" {
		t.Fatal("missing initial authority binding")
	}
	again, err := InitializeV1Alpha2Authority(ctx, store, ref, name)
	if err != nil || again.ResourceVersion() != first.ResourceVersion() || !sameJSON(first.object["status"], again.object["status"]) {
		t.Fatalf("repeat changed initialization: %v", err)
	}
	kubectl("", "patch", "rhizacluster", ref.Name, "--type=merge", "-p", `{"spec":{"automaticRecovery":"A"}}`)
	loaded, err := store.Load(ctx, ref)
	if err != nil || !sameJSON(first.object["status"], loaded.object["status"]) {
		t.Fatalf("mutable policy invalidated genesis: %v", err)
	}
	unused, err := LoadV1Alpha2ApplicationInit(ctx, store, ref)
	if err != nil || unused.Assignment().State != "Unused" {
		t.Fatalf("load unused assignment: %v", err)
	}
	consumed, observation, err := CommitV1Alpha2ApplicationInit(ctx, store, unused, plan, json.RawMessage(`{"value":"initial"}`))
	if err != nil || observation != V1Alpha2CASApplied || consumed.Assignment().State != "Consumed" {
		t.Fatalf("consume assignment: %s %v", observation, err)
	}
	rejectStatusPatch(`{"status":{"applicationInit":null}}`, "initialization assignment cannot be removed")
	rejectStatusPatch(`{"status":{"bootstrapPlan":null}}`, "bootstrap plan is immutable")
	rejectStatusPatch(`{"status":{"bootstrapPlan":{"evidenceFormat":"changed"}}}`, "bootstrap plan is immutable")
	rejectStatusPatch(`{"status":{"applicationInit":{"state":"Unused","initialEvidenceDigest":null}}}`, "consumed initialization cannot be changed or rearmed")
	kubectl("", "patch", "rhizacluster", ref.Name, "--subresource=status", "--type=merge", "-p", `{"status":{"authority":{"evidence":{"value":"advanced"}}}}`)
	kubectl("", "patch", "rhizacluster", ref.Name, "--subresource=status", "--type=merge", "-p", `{"status":{"authority":{"everApplicationReserved":true}}}`)
	rejectStatusPatch(`{"status":{"authority":{"everApplicationReserved":false}}}`, "application reservation history cannot be reset")
	rejectStatusPatch(`{"status":{"authority":{"everApplicationReserved":null}}}`, "everApplicationReserved")
	advanced, err := LoadV1Alpha2ApplicationInit(ctx, store, ref)
	if err != nil || advanced.Assignment() != consumed.Assignment() {
		t.Fatalf("initial assignment changed after evidence advance: %v", err)
	}
	wrong := ref
	wrong.UID = uid + "-other"
	if _, err := InitializeV1Alpha2Authority(ctx, store, wrong, name); err == nil {
		t.Fatal("different UID accepted")
	}
}
