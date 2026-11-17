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
	"strings"
	"testing"
	"time"
)

func TestManagedHostAccessAPIServerContract(t *testing.T) {
	server, caPath, tokenPath := os.Getenv("RHIZA_CONTRACT_SERVER"), os.Getenv("RHIZA_CONTRACT_CA"), os.Getenv("RHIZA_CONTRACT_TOKEN")
	kubeconfig, contextName, namespace := os.Getenv("RHIZA_CONTRACT_KUBECONFIG"), os.Getenv("RHIZA_CONTRACT_CONTEXT"), os.Getenv("RHIZA_CONTRACT_NAMESPACE")
	if server == "" || caPath == "" || tokenPath == "" || kubeconfig == "" || contextName == "" || namespace == "" {
		t.Skip("API server service-account contract environment not configured")
	}
	kubectl := func(input string, args ...string) []byte {
		t.Helper()
		args = append([]string{"--kubeconfig", kubeconfig, "--context", contextName, "--namespace", namespace, "--request-timeout=30s"}, args...)
		cmd := exec.Command("kubectl", args...)
		cmd.Stdin = strings.NewReader(input)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("kubectl: %v: %s", err, out)
		}
		return out
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
	kube := &Kubernetes{BaseURL: server, Namespace: namespace, TokenFile: tokenPath, Client: &http.Client{Transport: transport}}
	name := fmt.Sprintf("access-contract-%d", time.Now().UnixNano())
	deleteCluster := func(clusterName string) {
		_, _ = exec.Command("kubectl", "--kubeconfig", kubeconfig, "--context", contextName, "-n", namespace, "delete", "rhizacluster", clusterName, "--ignore-not-found", "--wait=true", "--timeout=30s").CombinedOutput()
	}
	deleteAccess := func(serviceAccount string) {
		for _, object := range []string{"rolebinding/" + serviceAccount + "-binding", "role/" + serviceAccount, "serviceaccount/" + serviceAccount} {
			_, _ = exec.Command("kubectl", "--kubeconfig", kubeconfig, "--context", contextName, "-n", namespace, "delete", object, "--ignore-not-found", "--wait=false").CombinedOutput()
		}
	}
	create := func(clusterName string) V1Alpha2ClusterRef {
		clusterSpec := fmt.Sprintf(`{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaCluster","metadata":{"name":%q,"namespace":%q},"spec":{"logicalID":%q,"genesisIntent":"Create","applicationProtocol":"rhiza-managed-request-append-only/v1","voterCount":3,"workload":{"podTemplate":{"spec":{"containers":[{"name":"app","image":"example.invalid/app"}]}}}}}`, clusterName, namespace, clusterName)
		kubectl(clusterSpec, "create", "-f", "-")
		var cluster V1Alpha2Cluster
		if err := json.Unmarshal(kubectl("", "get", "rhizacluster", clusterName, "-o", "json"), &cluster); err != nil {
			t.Fatal(err)
		}
		var uid string
		_ = json.Unmarshal(cluster.Metadata["uid"], &uid)
		return V1Alpha2ClusterRef{Name: clusterName, Namespace: namespace, UID: uid}
	}
	old := create(name)
	t.Cleanup(func() { deleteCluster(name) })
	oldSA, _ := V1Alpha2ManagedHostServiceAccountName(old)
	t.Cleanup(func() { deleteAccess(oldSA) })
	oldSA, err = ensureV1Alpha2ManagedHostAccess(context.Background(), kube, old)
	if err != nil {
		t.Fatal(err)
	}
	deleteCluster(name)
	newRef := create(name)
	newSACandidate, _ := V1Alpha2ManagedHostServiceAccountName(newRef)
	t.Cleanup(func() { deleteAccess(newSACandidate) })
	if newRef.UID == old.UID {
		t.Fatal("recreated cluster retained its UID")
	}
	_, err = ensureV1Alpha2ManagedHostAccess(context.Background(), kube, newRef)
	if err == nil || !strings.Contains(err.Error(), "manual cleanup") {
		t.Fatalf("same-name recreation did not fail on stale access: %v", err)
	}
	for _, object := range []string{"rolebinding/" + oldSA + "-binding", "role/" + oldSA, "serviceaccount/" + oldSA} {
		kubectl("", "delete", object, "--ignore-not-found", "--wait=true", "--timeout=30s")
	}
	newSA, err := ensureV1Alpha2ManagedHostAccess(context.Background(), kube, newRef)
	if err != nil {
		t.Fatal(err)
	}
	if newSA == oldSA {
		t.Fatal("recreated cluster reused the previous access identity")
	}
	foreignName := name + "-foreign"
	foreign := create(foreignName)
	t.Cleanup(func() { deleteCluster(foreignName) })
	hostTokenPath := t.TempDir() + "/host.token"
	if err := os.WriteFile(hostTokenPath, bytes.TrimSpace(kubectl("", "create", "token", newSA, "--duration=10m")), 0600); err != nil {
		t.Fatal(err)
	}
	host := &Kubernetes{BaseURL: server, Namespace: namespace, TokenFile: hostTokenPath, Client: kube.Client}
	var own, other map[string]json.RawMessage
	base := "/apis/rhiza.mrchypark.dev/v1alpha2/namespaces/" + namespace + "/rhizaclusters/"
	if err := kube.Get(context.Background(), base+newRef.Name, &own); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(context.Background(), base+foreign.Name, &other); err != nil {
		t.Fatal(err)
	}
	if err := host.Put(context.Background(), base+newRef.Name+"/status", own, nil); err != nil {
		t.Fatalf("host could not update its own status: %v", err)
	}
	err = host.Put(context.Background(), base+foreign.Name+"/status", other, nil)
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusForbidden {
		t.Fatalf("host status update crossed CR boundary: %v", err)
	}
	var role map[string]json.RawMessage
	if err := kube.Get(context.Background(), "/apis/rbac.authorization.k8s.io/v1/namespaces/"+namespace+"/roles/"+newSA, &role); err != nil {
		t.Fatal(err)
	}
	meta, _ := jsonObject(role["metadata"])
	if raw, ok := meta["ownerReferences"]; ok && string(raw) != "[]" && string(raw) != "null" {
		t.Fatal("per-cluster Role has premature owner-reference GC")
	}
}
