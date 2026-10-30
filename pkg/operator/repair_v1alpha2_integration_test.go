package operator

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// This exercises API-server schema transitions, not consensus proof. The
// genesis contract creates and consumes the assignment before this test.
func TestV1Alpha2RepairAPIServerContract(t *testing.T) {
	server := os.Getenv("RHIZA_CONTRACT_SERVER")
	if server == "" {
		t.Skip("API server contract environment not configured")
	}
	required := func(k string) string {
		t.Helper()
		v := os.Getenv(k)
		if v == "" {
			t.Fatalf("%s is required", k)
		}
		return v
	}
	ns := required("RHIZA_CONTRACT_NAMESPACE")
	ref := V1Alpha2ClusterRef{Name: "genesis-contract", Namespace: ns, UID: required("RHIZA_CONTRACT_GENESIS_UID")}
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
	a, err := LoadV1Alpha2Admission(ctx, store, ref)
	if err != nil {
		t.Fatal(err)
	}
	init, err := LoadV1Alpha2ApplicationInit(ctx, store, ref)
	if err != nil {
		t.Fatal(err)
	}
	if init.Assignment().State != "Consumed" {
		t.Fatal("genesis fixture not consumed")
	}
	request := V1Alpha2WriteRequest{Kind: "fixture", ID: "repair-pending", Fingerprint: "repair-pending-fingerprint"}
	pending, obs, err := ReserveV1Alpha2Write(ctx, store, a, a.Authority(), a.Evidence(), request)
	if err != nil || obs != V1Alpha2CASApplied {
		t.Fatalf("reserve: %s %v", obs, err)
	}
	var key quepaxa.PublicKey
	key[0] = 1
	kubectl := func(input string, args ...string) string {
		t.Helper()
		args = append([]string{"--kubeconfig", required("RHIZA_CONTRACT_KUBECONFIG"), "--context", required("RHIZA_CONTRACT_CONTEXT"), "--namespace", ns, "--request-timeout=30s"}, args...)
		cmd := exec.Command("kubectl", args...)
		cmd.Stdin = strings.NewReader(input)
		out, e := cmd.CombinedOutput()
		if e != nil {
			t.Fatalf("kubectl: %v: %s", e, out)
		}
		return string(out)
	}
	depName, err := V1Alpha2GenesisDeploymentName(ref.UID)
	if err != nil {
		t.Fatal(err)
	}
	deploymentPatch, err := json.Marshal(map[string]any{"spec": map[string]any{"replicas": 1, "template": map[string]any{"spec": map[string]any{"nodeName": "repair-contract-no-such-node", "initContainers": []any{map[string]any{"name": V1Alpha2PrepareIdentityContainer, "image": "example.invalid/init"}}}}}})
	if err != nil {
		t.Fatal(err)
	}
	kubectl("", "patch", "deployment", depName, "--type=merge", "-p", string(deploymentPatch))
	var podName string
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		podName = kubectl("", "get", "pods", "-l", "app=genesis-contract", "-o", "jsonpath={.items[*].metadata.name}")
		if podName != "" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if podName == "" {
		t.Fatal("Deployment did not create its fixture Pod")
	}
	podUID := kubectl("", "get", "pod", podName, "-o", "jsonpath={.metadata.uid}")
	nodeID, err := types.ManagedNodeID(podUID)
	if err != nil {
		t.Fatal(err)
	}
	report, err := json.Marshal(map[string]any{"version": 1, "publicKey": key})
	if err != nil {
		t.Fatal(err)
	}
	status, err := json.Marshal(map[string]any{"status": map[string]any{"podIP": "10.0.0.1", "initContainerStatuses": []any{map[string]any{"name": V1Alpha2PrepareIdentityContainer, "state": map[string]any{"terminated": map[string]any{"exitCode": 0, "message": string(report)}}, "ready": true, "restartCount": 0, "image": "example.invalid/init", "imageID": "fixture"}}}})
	if err != nil {
		t.Fatal(err)
	}
	kubectl("", "patch", "pod", podName, "--subresource=status", "--type=merge", "-p", string(status))
	voters := []types.NodeID{nodeID, "survivor"}
	plan := V1Alpha2RepairPlan{OperationID: "repair-cel", ClusterUID: ref.UID, Generation: a.Authority().Generation, DeploymentUID: a.Authority().Binding.DeploymentUID, StorageID: a.Authority().Binding.StorageID, ExpectedConfigID: 1, OldNodeID: "old", OldWALIdentity: "registered-nonce", BootstrapPlanDigest: init.Assignment().Plan.BootstrapPlanDigest, FinalVoterIDs: voters, Replacement: V1Alpha2RepairMember{PodName: podName, PodUID: podUID, NodeID: nodeID, PublicKey: key, PeerURL: "quic://10.0.0.1:9090"}}
	patch := func(body any, want string) {
		t.Helper()
		b, e := json.Marshal(map[string]any{"status": body})
		if e != nil {
			t.Fatal(e)
		}
		args := []string{"--kubeconfig", required("RHIZA_CONTRACT_KUBECONFIG"), "--context", required("RHIZA_CONTRACT_CONTEXT"), "--namespace", ns, "--request-timeout=30s", "patch", "rhizacluster", ref.Name, "--subresource=status", "--type=merge", "-p", string(b)}
		out, e := exec.Command("kubectl", args...).CombinedOutput()
		if want == "" && e != nil {
			t.Fatalf("status patch: %v: %s", e, out)
		}
		if want != "" && (e == nil || !strings.Contains(string(out), want)) {
			t.Fatalf("expected %q: %v: %s", want, e, out)
		}
	}
	closed, obs, err := BeginV1Alpha2Repair(ctx, store, pending, pending.Authority(), plan)
	if err != nil || obs != V1Alpha2CASApplied || !closed.SQLReservationsClosed() {
		t.Fatalf("close: %s %v", obs, err)
	}
	patch(map[string]any{"repair": map[string]any{"plan": map[string]any{"oldWALIdentity": "changed"}}}, "repair intent is immutable per operation")
	patch(map[string]any{"authority": map[string]any{"binding": map[string]any{"storageID": "changed"}}}, "closed repair must match authority")
	patch(map[string]any{"authority": map[string]any{"transition": map[string]any{"id": "other"}}}, "closed repair must match authority")
	patch(map[string]any{"repair": map[string]any{"state": "Completed"}}, "repair state and completion must agree")
	patch(map[string]any{"repair": map[string]any{"plan": map[string]any{"operationID": "replacement"}}}, "replacement repair requires a terminal predecessor")
	observed, err := LoadV1Alpha2Admission(ctx, store, ref)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := observed.Pending()
	if !ok || got != request || !sameJSON(observed.Evidence(), pending.Evidence()) {
		t.Fatal("closing repair lost pending request or evidence")
	}
	final, obs, err := FinalizeV1Alpha2Write(ctx, store, observed, observed.Authority(), request, V1Alpha2WriteCommitted, json.RawMessage(`{"host":"after-repair"}`))
	if err != nil || obs != V1Alpha2CASApplied {
		t.Fatalf("pending finalization: %s %v", obs, err)
	}
	if _, ok := final.Pending(); ok || !sameJSON(final.Evidence(), json.RawMessage(`{"host":"after-repair"}`)) {
		t.Fatal("finalization lost evidence")
	}
	patch(map[string]any{"authority": map[string]any{"pendingWrite": true, "pendingRequest": map[string]any{"kind": "fixture", "id": "new", "fingerprint": "new"}}}, "closed repair forbids new application reservations")
	completion := V1Alpha2RepairCompletion{OperationID: plan.OperationID, PlanDigest: plan.Digest(), ExpectedConfigID: plan.ExpectedConfigID, ExpectedAbortSlot: plan.ExpectedAbortSlot, Kind: "Terminal", ConfigID: 2, Slot: 1, Hash: strings.Repeat("b", 64), FinalVoterIDs: voters}
	patch(map[string]any{"repair": map[string]any{"state": "Completed", "completion": completion}}, "REMOVE alone is not final repair completion")
	completion.ConfigID = 3
	finished, obs, err := FinishV1Alpha2Repair(ctx, store, final, final.Authority(), completion)
	if err != nil || obs != V1Alpha2CASApplied || finished.SQLReservationsClosed() {
		t.Fatalf("finish: %s %v", obs, err)
	}
	loaded, err := LoadV1Alpha2Admission(ctx, store, ref)
	if err != nil {
		t.Fatalf("load terminal: %v", err)
	}
	record, ok := loaded.Repair()
	if !ok || record.State != "Completed" || record.Completion == nil || record.Completion.PlanDigest != plan.Digest() {
		t.Fatal("reader did not accept exact completion")
	}
	patch(map[string]any{"repair": map[string]any{"completion": map[string]any{"hash": strings.Repeat("c", 64)}}}, "terminal repair is immutable per operation")
}
