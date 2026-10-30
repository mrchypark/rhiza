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

func TestV1Alpha2InitializerAPIServerContract(t *testing.T) {
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
	ns := required("RHIZA_CONTRACT_NAMESPACE")
	kubectl := func(input string, args ...string) []byte {
		t.Helper()
		args = append([]string{"--kubeconfig", required("RHIZA_CONTRACT_KUBECONFIG"), "--context", required("RHIZA_CONTRACT_CONTEXT"), "--namespace", ns, "--request-timeout=30s"}, args...)
		cmd := exec.Command("kubectl", args...)
		cmd.Stdin = strings.NewReader(input)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("kubectl: %v: %s", err, out)
		}
		return out
	}
	cluster := fmt.Sprintf(`{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaCluster","metadata":{"name":"initializer-contract","namespace":%q},"spec":{"logicalID":"initializer","genesisIntent":"Create","voterCount":3,"workload":{"podTemplate":{"spec":{"containers":[{"name":"app","image":"example.invalid/app"}]}}}}}`, ns)
	var created struct {
		Metadata struct {
			UID string `json:"uid"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(kubectl(cluster, "create", "-f", "-", "-o", "json"), &created); err != nil || created.Metadata.UID == "" {
		t.Fatalf("invalid created cluster: %v", err)
	}
	ref := V1Alpha2ClusterRef{Name: "initializer-contract", Namespace: ns, UID: created.Metadata.UID}
	name, err := V1Alpha2GenesisDeploymentName(ref.UID)
	if err != nil {
		t.Fatal(err)
	}
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
	kube := &Kubernetes{BaseURL: server, Namespace: ns, TokenFile: required("RHIZA_CONTRACT_TOKEN"), Client: &http.Client{Transport: transport}}
	ctx := context.Background()
	first, err := ReconcileV1Alpha2Initialization(ctx, kube, ref, phase9Plan)
	if err != nil {
		t.Fatal(err)
	}
	var deploy struct {
		Metadata struct {
			UID, ResourceVersion string
			OwnerReferences      []struct {
				UID        string
				Controller bool
			}
		} `json:"metadata"`
		Spec struct {
			Replicas *int32 `json:"replicas"`
		} `json:"spec"`
	}
	firstDeployment := kubectl("", "get", "deployment", name, "-o", "json")
	if err := json.Unmarshal(firstDeployment, &deploy); err != nil {
		t.Fatal(err)
	}
	owners := 0
	for _, owner := range deploy.Metadata.OwnerReferences {
		if owner.Controller && owner.UID == ref.UID {
			owners++
		}
	}
	if owners != 1 || deploy.Metadata.UID == "" || deploy.Spec.Replicas == nil || *deploy.Spec.Replicas != 0 || first.Authority().Generation != 0 || first.Authority().Binding.DeploymentUID != deploy.Metadata.UID {
		t.Fatal("deployment and initial authority do not match")
	}
	status, err := jsonObject(first.object["status"])
	if err != nil {
		t.Fatal(err)
	}
	var receipt v1alpha2Genesis
	if err := json.Unmarshal(status["genesis"], &receipt); err != nil || receipt.DeploymentUID != deploy.Metadata.UID || receipt.OwnerUID != ref.UID {
		t.Fatalf("receipt mismatch: %v", err)
	}
	again, err := ReconcileV1Alpha2Initialization(ctx, kube, ref, phase9Plan)
	if err != nil || again.ResourceVersion() != first.ResourceVersion() || !sameJSON(again.object["status"], first.object["status"]) {
		t.Fatalf("repeat changed initialization: %v", err)
	}
	var after struct {
		Metadata struct{ UID, ResourceVersion string } `json:"metadata"`
		Spec     struct {
			Replicas *int32 `json:"replicas"`
		} `json:"spec"`
	}
	afterDeployment := kubectl("", "get", "deployment", name, "-o", "json")
	if err := json.Unmarshal(afterDeployment, &after); err != nil {
		t.Fatal(err)
	}
	if after.Metadata.UID != deploy.Metadata.UID || after.Spec.Replicas == nil || *after.Spec.Replicas != 0 {
		t.Fatal("repeat changed deployment")
	}
	var firstObject, afterObject map[string]json.RawMessage
	if json.Unmarshal(firstDeployment, &firstObject) != nil || json.Unmarshal(afterDeployment, &afterObject) != nil || !sameJSON(firstObject["spec"], afterObject["spec"]) {
		t.Fatal("repeat changed deployment spec")
	}
	var sets struct {
		Items []struct {
			Metadata struct {
				UID             string
				OwnerReferences []struct{ UID string }
			}
		}
	}
	if err := json.Unmarshal(kubectl("", "get", "replicasets", "-o", "json"), &sets); err != nil {
		t.Fatal(err)
	}
	owned := map[string]bool{deploy.Metadata.UID: true}
	for _, set := range sets.Items {
		for _, owner := range set.Metadata.OwnerReferences {
			if owner.UID == deploy.Metadata.UID {
				owned[set.Metadata.UID] = true
			}
		}
	}
	var pods struct {
		Items []struct {
			Metadata struct{ OwnerReferences []struct{ UID string } }
		}
	}
	if err := json.Unmarshal(kubectl("", "get", "pods", "-o", "json"), &pods); err != nil {
		t.Fatal(err)
	}
	for _, pod := range pods.Items {
		for _, owner := range pod.Metadata.OwnerReferences {
			if owned[owner.UID] {
				t.Fatal("unexpected deployment-owned pod")
			}
		}
	}
}
