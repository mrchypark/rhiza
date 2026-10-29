package operator

import (
	"context"
	"crypto/tls"
	"crypto/x509"
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
	first, err := InitializeV1Alpha2Authority(ctx, store, ref, name)
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
	wrong := ref
	wrong.UID = uid + "-other"
	if _, err := InitializeV1Alpha2Authority(ctx, store, wrong, name); err == nil {
		t.Fatal("different UID accepted")
	}
}
