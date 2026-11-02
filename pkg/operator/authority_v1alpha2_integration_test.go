package operator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// The harness seeds host evidence administratively; it is not a DB outcome.
func TestV1Alpha2AuthorityAPIServerContract(t *testing.T) {
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
	ns := required("RHIZA_CONTRACT_NAMESPACE")
	store := &V1Alpha2StatusStore{Kube: &Kubernetes{BaseURL: server, Namespace: ns, TokenFile: required("RHIZA_CONTRACT_TOKEN"), Client: &http.Client{Transport: transport}}}
	ctx := context.Background()
	ref := V1Alpha2ClusterRef{Name: "authority-contract", Namespace: ns, UID: required("RHIZA_CONTRACT_AUTHORITY_UID")}
	first, err := LoadV1Alpha2Admission(ctx, store, ref)
	if err != nil {
		t.Fatal(err)
	}
	request := V1Alpha2WriteRequest{Kind: "fixture", ID: "request-1", Fingerprint: "fingerprint-1"}
	wrong := ref
	wrong.UID += "-other"
	if _, err := LoadV1Alpha2Admission(ctx, store, wrong); err == nil {
		t.Fatal("wrong UID accepted")
	}
	reserved, outcome, err := ReserveV1Alpha2Write(ctx, store, first, first.Authority(), first.Evidence(), request)
	if err != nil || outcome != V1Alpha2CASApplied || reserved.status.ResourceVersion() == first.status.ResourceVersion() {
		t.Fatalf("reserve: %s %v", outcome, err)
	}
	if _, _, err := ReserveV1Alpha2Write(ctx, store, first, first.Authority(), first.Evidence(), request); !errors.Is(err, ErrV1Alpha2StatusConflict) {
		t.Fatalf("stale reservation: %v", err)
	}
	observed, err := LoadV1Alpha2Admission(ctx, store, ref)
	if err != nil {
		t.Fatal(err)
	}
	if got, pending := observed.Pending(); !pending || got != request {
		t.Fatalf("pending request: %+v %t", got, pending)
	}
	other := request
	other.ID = "other"
	if _, _, err := FinalizeV1Alpha2Write(ctx, store, observed, observed.Authority(), other, V1Alpha2WriteCommitted, json.RawMessage(`{"host":"after"}`)); err == nil {
		t.Fatal("different request finalized pending write")
	}
	finished, outcome, err := FinalizeV1Alpha2Write(ctx, store, observed, observed.Authority(), request, V1Alpha2WriteCommitted, json.RawMessage(`{"host":"after"}`))
	if err != nil || outcome != V1Alpha2CASApplied || finished.status.ResourceVersion() == observed.status.ResourceVersion() {
		t.Fatalf("finalize: %s %v", outcome, err)
	}
	if _, pending := finished.Pending(); pending || !sameJSON(finished.Evidence(), json.RawMessage(`{"host":"after"}`)) {
		t.Fatal("finalized state differs")
	}
	beforeStatus, err := jsonObject(first.status.object["status"])
	if err != nil {
		t.Fatal(err)
	}
	afterStatus, err := jsonObject(finished.status.object["status"])
	if err != nil {
		t.Fatal(err)
	}
	beforeAuth, err := jsonObject(beforeStatus["authority"])
	if err != nil {
		t.Fatal(err)
	}
	afterAuth, err := jsonObject(afterStatus["authority"])
	if err != nil {
		t.Fatal(err)
	}
	if !sameJSON(beforeStatus["other"], afterStatus["other"]) || !sameJSON(beforeAuth["receipt"], afterAuth["receipt"]) || first.Authority() != finished.Authority() {
		t.Fatal("unrelated status or authority binding changed")
	}
}

// This qualifies API storage/CEL and normal shared status writes, not runtime
// REMOVE/ADD certificates. Administrative fixture evidence is never a DB ACK.
func TestV1Alpha2MixedBudgetAPIServerContract(t *testing.T) {
	if os.Getenv("RHIZA_CONTRACT_SERVER") == "" {
		if os.Getenv("RHIZA_CONTRACT_REQUIRED") == "1" {
			t.Fatal("required API server missing")
		}
		t.Skip("API server contract environment not configured")
	}
	required := func(key string) string {
		t.Helper()
		v := os.Getenv(key)
		if v == "" {
			t.Fatalf("%s required", key)
		}
		return v
	}
	ns := required("RHIZA_CONTRACT_NAMESPACE")
	kubectl := func(input string, args ...string) []byte {
		t.Helper()
		args = append([]string{"--kubeconfig", required("RHIZA_CONTRACT_KUBECONFIG"), "--context", required("RHIZA_CONTRACT_CONTEXT"), "-n", ns, "--request-timeout=30s"}, args...)
		cmd := exec.Command("kubectl", args...)
		cmd.Stdin = strings.NewReader(input)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("kubectl %v: %s", err, out)
		}
		return out
	}
	ca, err := os.ReadFile(required("RHIZA_CONTRACT_CA"))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatal("invalid CA")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	t.Cleanup(transport.CloseIdleConnections)
	store := &V1Alpha2StatusStore{Kube: &Kubernetes{BaseURL: required("RHIZA_CONTRACT_SERVER"), Namespace: ns, TokenFile: required("RHIZA_CONTRACT_TOKEN"), Client: &http.Client{Transport: transport}}}
	ctx := context.Background()
	for _, count := range []int{3, 64} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			name := fmt.Sprintf("mixed-budget-%d", count)
			cluster := map[string]any{"apiVersion": V1Alpha2APIVersion, "kind": "RhizaCluster", "metadata": map[string]any{"name": name, "namespace": ns, "annotations": map[string]string{"fixture": strings.Repeat("m", 48<<10)}}, "spec": map[string]any{"logicalID": "test", "genesisIntent": "Create", "applicationProtocol": types.ManagedApplicationProtocolSQLKV, "voterCount": count, "workload": map[string]any{"podTemplate": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": "example.invalid/app"}}}}}}}
			raw, _ := json.Marshal(cluster)
			var created V1Alpha2Cluster
			if err := json.Unmarshal(kubectl(string(raw), "create", "-f", "-", "-o", "json"), &created); err != nil {
				t.Fatal(err)
			}
			var uid string
			_ = json.Unmarshal(created.Metadata["uid"], &uid)
			ref := V1Alpha2ClusterRef{Name: name, Namespace: ns, UID: uid}
			clusterID, _ := types.ManagedClusterID(uid, 0)
			bootstrap := V1Alpha2BootstrapPlan{ClusterID: clusterID, PhysicalBinding: strings.Repeat("a", 64), EvidenceFormat: "fixture-mixed/v1", Durability: types.ObjectStoreDurabilityBeforeAck}
			var members []quepaxa.Member
			for i := 0; i < count; i++ {
				podUID := fmt.Sprintf("00000000-0000-0000-0000-%012d", i)
				id, _ := types.ManagedNodeID(podUID)
				var key quepaxa.PublicKey
				key[0] = byte(i + 1)
				m := quepaxa.Member{ID: id, PublicKey: key, PeerURL: fmt.Sprintf("quic://10.0.0.%d:9090", i+1)}
				members = append(members, m)
				bootstrap.Members = append(bootstrap.Members, V1Alpha2BootstrapMember{PodUID: podUID, Member: m})
			}
			digest, err := types.ManagedBootstrapPlanDigest(string(clusterID), members, bootstrap.PhysicalBinding, bootstrap.EvidenceFormat, bootstrap.Durability, false)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := NewV1Alpha2ApplicationInitPlan(ref, "test", "fixture-deployment", "fixture-assignment", bootstrap.PhysicalBinding, digest, bootstrap.EvidenceFormat)
			if err != nil {
				t.Fatal(err)
			}
			plan.ApplicationProtocol = types.ManagedApplicationProtocolSQLKV
			genesis := v1alpha2Genesis{OwnerUID: uid, LogicalID: "test", DeploymentUID: plan.DeploymentUID, StorageID: plan.StorageID, IntentHash: plan.IntentHash}
			status := map[string]any{"genesis": genesis, "bootstrapPlan": bootstrap, "applicationInit": V1Alpha2ApplicationInitAssignment{Plan: plan, State: "Unused"}, "authority": map[string]any{"owner": map[string]string{"namespace": ns, "name": name, "uid": uid, "logicalID": "test"}, "generation": 0, "binding": map[string]string{"deploymentUID": plan.DeploymentUID, "storageID": plan.StorageID}, "pendingWrite": false, "everApplicationReserved": false}, "fixtureExtra": strings.Repeat("x", 60<<10)}
			statusRaw, _ := json.Marshal(map[string]any{"status": status})
			kubectl("", "patch", "rhizacluster", name, "--subresource=status", "--type=merge", "-p", string(statusRaw))
			initial, err := LoadV1Alpha2ApplicationInit(ctx, store, ref)
			if err != nil {
				t.Fatal(err)
			}
			evidence, _ := json.Marshal(map[string]string{"fixture": strings.Repeat("e", (64<<10)-32)})
			if _, obs, err := CommitV1Alpha2ApplicationInit(ctx, store, initial, plan, evidence); err != nil || obs != V1Alpha2CASApplied {
				t.Fatalf("consume %s %v", obs, err)
			}
			old, err := LoadV1Alpha2Admission(ctx, store, ref)
			if err != nil {
				t.Fatal(err)
			}
			// Exactly the ordinary pure admission charge maximum, not a reduced blob cap.
			command := types.KVCommand{RequestID: string([]byte{0xff}), Operation: "put", Key: string([]byte{0xfe, 'k', 'k', 'k'}), Value: make([]byte, 209656), Expected: []byte{}}
			reserved, obs, err := ReserveV1Alpha2KVWrite(ctx, store, old, old.Authority(), old.Evidence(), command)
			if err != nil || obs != V1Alpha2CASApplied {
				t.Fatalf("max reserve %s %v", obs, err)
			}
			actual, present, err := reserved.PendingKV()
			if err != nil || !present || actual.RequestID != command.RequestID || actual.Key != command.Key || actual.Expected == nil {
				t.Fatalf("API original pruning: %v", err)
			}
			check := func(s V1Alpha2AdmissionSnapshot) {
				t.Helper()
				if err := validateManagedMixedProfile(s.status.object); err != nil {
					t.Fatal(err)
				}
				b, _ := json.Marshal(s.status.object)
				t.Logf("voters=%d bytes=%d projected-base-cap=%d metadata-ceiling=%d", count, len(b), managedMixedProjectedBaseLimit, managedMixedMetadataCeiling)
			}
			check(reserved)
			request, _ := reserved.Pending()
			// Raw /status must reject union and marker changes, not merely client validation.
			reject := func(change func(map[string]json.RawMessage, map[string]json.RawMessage)) {
				t.Helper()
				var object map[string]json.RawMessage
				b, _ := json.Marshal(reserved.status.object)
				_ = json.Unmarshal(b, &object)
				s, _ := jsonObject(object["status"])
				a, _ := jsonObject(s["authority"])
				change(s, a)
				s["authority"], _ = json.Marshal(a)
				object["status"], _ = json.Marshal(s)
				var response map[string]json.RawMessage
				err := store.Kube.Put(ctx, "/apis/"+V1Alpha2APIVersion+"/namespaces/"+ns+"/rhizaclusters/"+name+"/status", object, &response)
				var api *APIError
				if !errors.As(err, &api) || api.StatusCode != 422 {
					t.Fatalf("expected CEL422, got %v", err)
				}
			}
			reject(func(_ map[string]json.RawMessage, a map[string]json.RawMessage) {
				p, _ := jsonObject(a["pendingRequest"])
				p["originalSQL"] = json.RawMessage(`{}`)
				a["pendingRequest"], _ = json.Marshal(p)
			})
			reject(func(_ map[string]json.RawMessage, a map[string]json.RawMessage) {
				a["generation"] = json.RawMessage("1")
			})
			reject(func(_ map[string]json.RawMessage, a map[string]json.RawMessage) {
				a["binding"] = json.RawMessage(`{"deploymentUID":"other","storageID":"other"}`)
			})
			reject(func(s map[string]json.RawMessage, _ map[string]json.RawMessage) {
				var assignment V1Alpha2ApplicationInitAssignment
				_ = json.Unmarshal(s["applicationInit"], &assignment)
				assignment.Plan.ApplicationProtocol = ""
				s["applicationInit"], _ = json.Marshal(assignment)
			})
			// This is a schema/budget fixture, not authorization from a real candidate Pod.
			growth, _ := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": map[string]string{"fixture": strings.Repeat("m", 70<<10)}}})
			kubectl("", "patch", "rhizacluster", name, "--type=merge", "-p", string(growth))
			reserved, err = LoadV1Alpha2Admission(ctx, store, ref)
			if err != nil {
				t.Fatalf("metadata growth blocked pending repair: %v", err)
			}
			check(reserved)
			replacementUID := "11111111-0000-0000-0000-000000000000"
			replacementID, _ := types.ManagedNodeID(replacementUID)
			var key quepaxa.PublicKey
			key[0] = 100
			ids := []types.NodeID{replacementID}
			survivors := quepaxa.Cluster{ConfigID: 2, Members: members[1:]}
			for _, m := range survivors.Members {
				ids = append(ids, m.ID)
			}
			slices.Sort(ids)
			repair := V1Alpha2RepairPlan{OperationID: "mixed-budget-repair", ClusterUID: uid, Generation: 0, DeploymentUID: plan.DeploymentUID, StorageID: plan.StorageID, ExpectedConfigID: 1, OldNodeID: members[0].ID, OldWALIdentity: "fixture-wal", Replacement: V1Alpha2RepairMember{PodName: "fixture-candidate", PodUID: replacementUID, NodeID: replacementID, PublicKey: key, PeerURL: "quic://10.1.0.1:9090"}, BootstrapPlanDigest: digest, FinalVoterIDs: ids}
			if err := validateV1Alpha2RepairPlan(reserved.status, repair); err != nil {
				t.Fatal(err)
			}
			closed, obs, err := updateV1Alpha2AdmissionStatus(ctx, store, reserved, func(s, a map[string]json.RawMessage) {
				s["repair"], _ = json.Marshal(V1Alpha2RepairRecord{Plan: repair, State: "Closed"})
			})
			if err != nil || obs != V1Alpha2CASApplied {
				t.Fatalf("close shared status writer %s %v", obs, err)
			}
			check(closed)
			head := bytes.Repeat([]byte("h"), V1Alpha2RepairHeadLimit)
			root := "fixture-root"
			_, prefix, err := objectstore.ManagedNamespacePrefixes(root, uid, 0, plan.AssignmentID, string(clusterID))
			if err != nil {
				t.Fatal(err)
			}
			handoff := V1Alpha2RepairHandoff{PlanDigest: repair.Digest(), SourceRoot: root, SourcePrefix: prefix, PhysicalBinding: plan.PhysicalBinding, Head: head, HeadHash: fmt.Sprintf("%x", sha256.Sum256(head)), RemoveTerminalSlot: 1, RemoveTerminalHash: strings.Repeat("b", 64), Survivors: survivors}
			if err := validateRepairHandoff(closed.status, repair, handoff); err != nil {
				t.Fatal(err)
			}
			// The production capture helper additionally requires an actual prepared
			// Pod. This API budget fixture has no Pod/consensus authorization claim.
			captured, obs, err := updateV1Alpha2AdmissionStatus(ctx, store, closed, func(status, _ map[string]json.RawMessage) {
				status["repair"], _ = json.Marshal(V1Alpha2RepairRecord{Plan: repair, State: "Closed", PreparedHandoff: &handoff})
			})
			if err != nil || obs != V1Alpha2CASApplied {
				t.Fatalf("handoff %s %v", obs, err)
			}
			check(captured)
			completion := V1Alpha2RepairCompletion{OperationID: repair.OperationID, PlanDigest: repair.Digest(), ExpectedConfigID: 1, Kind: "Terminal", ConfigID: 3, Slot: 2, Hash: strings.Repeat("c", 64), FinalVoterIDs: ids}
			finished, obs, err := FinishV1Alpha2Repair(ctx, store, captured, captured.Authority(), completion)
			if err != nil || obs != V1Alpha2CASApplied {
				t.Fatalf("completion %s %v", obs, err)
			}
			check(finished)
			final, obs, err := FinalizeV1Alpha2Write(ctx, store, finished, finished.Authority(), request, V1Alpha2WriteCommitted, evidence)
			if err != nil || obs != V1Alpha2CASApplied {
				t.Fatalf("finalize %s %v", obs, err)
			}
			check(final)
			// Required nullable blobs must remain distinct after both a direct
			// response and a fresh API read, not only the maximum nonnull fixture.
			nullable := types.KVCommand{RequestID: "nullable", Operation: "cas", Key: "nullable", Value: nil, Expected: []byte{}, ExpectedExists: true}
			nullReserved, obs, err := ReserveV1Alpha2KVWrite(ctx, store, final, final.Authority(), final.Evidence(), nullable)
			if err != nil || obs != V1Alpha2CASApplied {
				t.Fatalf("nullable reserve %s %v", obs, err)
			}
			for _, snapshot := range []V1Alpha2AdmissionSnapshot{nullReserved, func() V1Alpha2AdmissionSnapshot {
				s, err := LoadV1Alpha2Admission(ctx, store, ref)
				if err != nil {
					t.Fatal(err)
				}
				return s
			}()} {
				got, found, err := snapshot.PendingKV()
				if err != nil || !found || got.Value != nil || got.Expected == nil || len(got.Expected) != 0 {
					t.Fatalf("nullable API intent changed: found=%v error=%v", found, err)
				}
				fingerprint, _ := types.KVFingerprint(got)
				want, _ := types.KVFingerprint(nullable)
				if fingerprint != want || got.RequestID != nullable.RequestID {
					t.Fatal("nullable original identity changed")
				}
			}
			nullRequest, _ := nullReserved.Pending()
			final, obs, err = FinalizeV1Alpha2Write(ctx, store, nullReserved, nullReserved.Authority(), nullRequest, V1Alpha2WriteCommitted, evidence)
			if err != nil || obs != V1Alpha2CASApplied {
				t.Fatalf("nullable finalize %s %v", obs, err)
			}
			if dir := os.Getenv("RHIZA_CONTRACT_LOG_DIR"); dir != "" {
				b, _ := json.Marshal(final.status.object)
				if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("mixed-old-binary-%d.json", count)), b, 0600); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
