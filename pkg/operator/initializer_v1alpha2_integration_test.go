package operator

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
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

	// This contract exercises real API-server ownership/defaulting/CAS with
	// controlled public status, not kubelet execution of the reference image.
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	profile := phase9RuntimeProfile()
	profile.ProfileSecretName = "initializer-contract-runtime"
	profileJSON, _ := json.Marshal(profile)
	secret := map[string]any{"apiVersion": "v1", "kind": "Secret", "metadata": map[string]any{"name": profile.ProfileSecretName, "namespace": ns}, "data": map[string][]byte{V1Alpha2ManagedRuntimeProfileKey: profileJSON}}
	kubectl(string(mustManagedJSON(secret)), "create", "-f", "-")
	t.Cleanup(func() {
		args := []string{"--kubeconfig", required("RHIZA_CONTRACT_KUBECONFIG"), "--context", required("RHIZA_CONTRACT_CONTEXT"), "--namespace", ns, "--request-timeout=10s", "delete", "secret", profile.ProfileSecretName, "--wait=false"}
		if out, err := exec.Command("kubectl", args...).CombinedOutput(); err != nil {
			t.Errorf("cleanup owned profile Secret: %v %s", err, out)
		}
	})
	if _, err := ReconcileV1Alpha2ManagedInitialization(ctx, kube, ref, profile); !errors.Is(err, ErrV1Alpha2BootstrapPending) {
		t.Fatalf("expected gated pending candidate: %v", err)
	}
	var deploy struct {
		Metadata struct {
			UID, ResourceVersion string
			OwnerReferences      []struct {
				UID        string
				Controller bool
			}
		}
		Spec struct {
			Replicas *int
			Template struct {
				Spec struct {
					InitContainers []json.RawMessage
					Containers     []map[string]json.RawMessage
				}
			}
		}
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
	if owners != 1 || deploy.Metadata.UID == "" || deploy.Spec.Replicas == nil || *deploy.Spec.Replicas != 3 || len(deploy.Spec.Template.Spec.InitContainers) != 2 || len(deploy.Spec.Template.Spec.Containers) != 1 {
		t.Fatal("fixed managed candidate ownership/template mismatch")
	}
	main := deploy.Spec.Template.Spec.Containers[0]
	if !sameJSON(main["command"], json.RawMessage("[\"/usr/local/bin/rhiza-ha-init\"]")) || len(main["readinessProbe"]) == 0 || len(main["livenessProbe"]) != 0 {
		t.Fatal("managed main contract missing")
	}
	var first V1Alpha2StatusSnapshot
	var lastErr error
	for ctx.Err() == nil {
		var page struct {
			Items []struct {
				Metadata struct {
					Name, UID       string
					OwnerReferences []struct {
						APIVersion, Kind, Name, UID string
						Controller                  bool
					}
				}
				Status struct{ PodIP string }
			}
		}
		path := "/api/v1/namespaces/" + ns + "/pods?labelSelector=" + url.QueryEscape("rhiza.mrchypark.dev/cluster-uid="+ref.UID)
		if err := kube.Get(ctx, path, &page); err != nil {
			t.Fatal(err)
		}
		if len(page.Items) == 3 {
			for i, pod := range page.Items {
				if pod.Metadata.UID == "" || len(pod.Metadata.OwnerReferences) != 1 || pod.Metadata.OwnerReferences[0].Kind != "ReplicaSet" || !pod.Metadata.OwnerReferences[0].Controller {
					t.Fatal("Pod lacks exact controller identity")
				}
				key := make([]byte, 32)
				key[0] = byte(i + 1)
				message := string(mustManagedJSON(map[string]any{"version": 1, "publicKey": base64.StdEncoding.EncodeToString(key)}))
				ip := pod.Status.PodIP
				if ip == "" {
					ip = fmt.Sprintf("192.0.2.%d", i+1)
				}
				status := map[string]any{"phase": "Pending", "podIP": ip, "podIPs": []any{map[string]any{"ip": ip}}, "initContainerStatuses": []any{
					map[string]any{"name": V1Alpha2PrepareIdentityContainer, "image": profile.InitImage, "imageID": "", "ready": false, "restartCount": 0, "state": map[string]any{"terminated": map[string]any{"exitCode": 0, "message": message}}},
					map[string]any{"name": V1Alpha2AwaitBootstrapContainer, "image": profile.InitImage, "imageID": "", "ready": false, "restartCount": 0, "state": map[string]any{"running": map[string]any{"startedAt": "2026-01-01T00:00:00Z"}}},
				}, "containerStatuses": []any{}}
				patch := []any{map[string]any{"op": "test", "path": "/metadata/uid", "value": pod.Metadata.UID}, map[string]any{"op": "replace", "path": "/status", "value": status}}
				kubectl("", "patch", "pod", pod.Metadata.Name, "--subresource=status", "--type=json", "-p", string(mustManagedJSON(patch)))
			}
			first, lastErr = ReconcileV1Alpha2ManagedInitialization(ctx, kube, ref, profile)
			if lastErr == nil {
				break
			}
			if !errors.Is(lastErr, ErrV1Alpha2BootstrapPending) {
				t.Fatal(lastErr)
			}
		}
		select {
		case <-ctx.Done():
		case <-time.After(100 * time.Millisecond):
		}
	}
	if lastErr != nil || first.ResourceVersion() == "" {
		t.Fatalf("controlled identity bootstrap did not commit: %v context=%v", lastErr, ctx.Err())
	}
	if first.Authority().Generation != 0 || first.Authority().Binding.DeploymentUID != deploy.Metadata.UID {
		t.Fatal("initial authority does not match actual Deployment")
	}
	status, err := jsonObject(first.object["status"])
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"genesis", "authority", "applicationInit", "bootstrapPlan"} {
		if len(status[key]) == 0 {
			t.Fatalf("atomic genesis omitted %s", key)
		}
	}
	var receipt v1alpha2Genesis
	if json.Unmarshal(status["genesis"], &receipt) != nil || receipt.DeploymentUID != deploy.Metadata.UID || receipt.OwnerUID != ref.UID {
		t.Fatal("receipt mismatch")
	}
	plan, err := first.BootstrapPlan()
	if err != nil || len(plan.Members) != 3 {
		t.Fatalf("full public bootstrap plan: %v", err)
	}
	again, err := ReconcileV1Alpha2ManagedInitialization(ctx, kube, ref, profile)
	if err != nil || again.ResourceVersion() != first.ResourceVersion() || !sameJSON(again.object["status"], first.object["status"]) {
		t.Fatalf("repeat changed initialization: %v", err)
	}
	afterDeployment := kubectl("", "get", "deployment", name, "-o", "json")
	var before, after map[string]json.RawMessage
	if json.Unmarshal(firstDeployment, &before) != nil || json.Unmarshal(afterDeployment, &after) != nil || !sameJSON(before["spec"], after["spec"]) {
		t.Fatal("repeat changed candidate spec")
	}
	var afterMeta struct{ UID string }
	if json.Unmarshal(after["metadata"], &afterMeta) != nil || afterMeta.UID != deploy.Metadata.UID {
		t.Fatal("repeat replaced Deployment")
	}
}
