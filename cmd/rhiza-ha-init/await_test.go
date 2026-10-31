package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/managedsql"
	nodepkg "github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func mustRepairJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestAwaitCommittedPlanAndLiveGuards(t *testing.T) {
	ref := operator.V1Alpha2ClusterRef{Name: "cluster", Namespace: "ns", UID: "uid"}
	dir := t.TempDir()
	identity := filepath.Join(dir, "identity")
	output := filepath.Join(dir, "handoff")
	cluster, _ := types.ManagedClusterID(ref.UID, 0)
	node, _ := types.ManagedNodeID("pod-1")
	token, err := prepareIdentityFile(identity, string(cluster), string(node))
	if err != nil {
		t.Fatal(err)
	}
	plan := operator.V1Alpha2BootstrapPlan{ClusterID: cluster, PhysicalBinding: strings.Repeat("a", 64), EvidenceFormat: managedsql.EvidenceFormat, Durability: types.ObjectStoreDurabilityBeforeAck}
	var members []quepaxa.Member
	for i := 1; i <= 3; i++ {
		pod := fmt.Sprintf("pod-%d", i)
		id, _ := types.ManagedNodeID(pod)
		key := rhiza.PeerPublicKey(string(cluster), string(id), fmt.Sprintf("token-%d", i))
		if i == 1 {
			key = rhiza.PeerPublicKey(string(cluster), string(id), token)
		}
		m := quepaxa.Member{ID: id, PeerURL: fmt.Sprintf("quic://127.0.0.%d:1234", i), PublicKey: key}
		members = append(members, m)
		plan.Members = append(plan.Members, operator.V1Alpha2BootstrapMember{PodUID: pod, Member: m})
	}
	namespace, err := nodepkg.ManagedNamespaceFor("root", ref.UID, 0, "assignment", string(cluster))
	if err != nil {
		t.Fatal(err)
	}
	plan.PhysicalBinding, err = types.ManagedStorageBinding(types.ExecutionConfig{ClusterID: cluster, ObjStorePrefix: namespace.ConfigPrefix})
	if err != nil {
		t.Fatal(err)
	}
	digest, err := types.ManagedBootstrapPlanDigest(string(cluster), members, plan.PhysicalBinding, plan.EvidenceFormat, plan.Durability, false)
	if err != nil {
		t.Fatal(err)
	}
	assignment, err := operator.NewV1Alpha2ApplicationInitPlan(ref, "logical", "deployment", "assignment", plan.PhysicalBinding, digest, plan.EvidenceFormat)
	if err != nil {
		t.Fatal(err)
	}
	depName, err := operator.V1Alpha2GenesisDeploymentName(ref.UID)
	if err != nil {
		t.Fatal(err)
	}
	clusterJSON := []byte(`{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaCluster","metadata":{"name":"cluster","namespace":"ns","uid":"uid","resourceVersion":"1"},"spec":{"logicalID":"logical","genesisIntent":"Create","voterCount":3,"workload":{"podTemplate":{"spec":{"containers":[{"name":"app","image":"image"}]}}}},"status":{}}`)
	dep := fmt.Sprintf(`{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":%q,"namespace":"ns","uid":"deployment","resourceVersion":"1","labels":{"rhiza.mrchypark.dev/cluster-uid":"uid","rhiza.mrchypark.dev/generation":"0"},"ownerReferences":[{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaCluster","name":"cluster","uid":"uid","controller":true}]}}`, depName)
	rs := fmt.Sprintf(`{"apiVersion":"apps/v1","kind":"ReplicaSet","metadata":{"name":"rs","namespace":"ns","uid":"rsuid","resourceVersion":"1","ownerReferences":[{"apiVersion":"apps/v1","kind":"Deployment","name":%q,"uid":"deployment","controller":true}]}}`, depName)
	pod := `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"pod-name","namespace":"ns","uid":"pod-1","resourceVersion":"1","ownerReferences":[{"apiVersion":"apps/v1","kind":"ReplicaSet","name":"rs","uid":"rsuid","controller":true}]},"status":{"podIP":"127.0.0.1"}}`
	var mu sync.Mutex
	rv := 1
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/rhizaclusters/"):
			if r.Method == http.MethodPut {
				clusterJSON, _ = io.ReadAll(r.Body)
				var obj map[string]any
				_ = json.Unmarshal(clusterJSON, &obj)
				rv++
				obj["metadata"].(map[string]any)["resourceVersion"] = fmt.Sprint(rv)
				clusterJSON, _ = json.Marshal(obj)
			}
			_, _ = w.Write(clusterJSON)
		case strings.Contains(r.URL.Path, "/deployments/"):
			_, _ = io.WriteString(w, dep)
		case strings.Contains(r.URL.Path, "/replicasets/"):
			_, _ = io.WriteString(w, rs)
		case strings.Contains(r.URL.Path, "/pods/"):
			_, _ = io.WriteString(w, pod)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(s.Close)
	tokenFile := filepath.Join(dir, "api-token")
	if err := os.WriteFile(tokenFile, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	k := &operator.Kubernetes{BaseURL: s.URL, Namespace: "ns", TokenFile: tokenFile, Client: s.Client()}
	store := &operator.V1Alpha2StatusStore{Kube: k}
	if _, err := operator.InitializeV1Alpha2AuthorityWithBootstrap(context.Background(), store, ref, depName, assignment, plan); err != nil {
		t.Fatal(err)
	}
	initial, err := operator.LoadV1Alpha2ApplicationInit(context.Background(), store, ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := operator.CommitV1Alpha2ApplicationInit(context.Background(), store, initial, assignment, json.RawMessage(`{"digest":"initial"}`)); err != nil {
		t.Fatal(err)
	}
	if err := awaitBootstrap(context.Background(), k, ref, 0, "pod-name", "pod-1", identity, output); err != nil {
		t.Fatalf("consumed gate: %v", err)
	}
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	var got operator.V1Alpha2BootstrapPlan
	if err := json.Unmarshal(data, &got); err != nil || len(got.Members) != 3 || got.Members[0].PodUID != "pod-1" || bytes.Contains(data, []byte(token)) {
		t.Fatal("invalid or secret-bearing handoff")
	}
	profile := operator.V1Alpha2RuntimeProfile{PeerPort: 1234, EvidenceFormat: managedsql.EvidenceFormat, Storage: types.ExecutionConfig{ObjStorePrefix: "root", AdminToken: "private-admin"}}
	cfg, err := managedConfig(context.Background(), k, ref, "pod-name", "pod-1", identity, output, profile)
	if err != nil || cfg.StorageRoot != "root" || cfg.Runtime.NodeID != node || cfg.Runtime.PeerToken != token || cfg.Runtime.PeerAddr != "127.0.0.1:1234" || len(cfg.Runtime.Members) != 3 || cfg.Runtime.ObjStorePrefix == "root" {
		t.Fatalf("bad managed configuration: %v", err)
	}
	profile.EvidenceFormat = "wrong"
	if _, err := managedConfig(context.Background(), k, ref, "pod-name", "pod-1", identity, output, profile); err == nil {
		t.Fatal("mismatched evidence profile accepted")
	}
	profile.EvidenceFormat = managedsql.EvidenceFormat
	var advanced map[string]any
	if err := json.Unmarshal(clusterJSON, &advanced); err != nil {
		t.Fatal(err)
	}
	advanced["status"].(map[string]any)["authority"].(map[string]any)["evidence"] = map[string]any{"digest": "advanced"}
	clusterJSON, err = json.Marshal(advanced)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := checkGate(context.Background(), k, ref, 0, "pod-name", "pod-1", identity); err != nil {
		t.Fatalf("consumed advanced evidence rejected: %v", err)
	}
	replacementNode, _ := types.ManagedNodeID("replacement")
	replacementFile := filepath.Join(dir, "replacement")
	if _, err := prepareIdentityFile(replacementFile, string(cluster), string(replacementNode)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	replacementOutput := filepath.Join(dir, "replacement-handoff")
	err = awaitBootstrap(ctx, k, ref, 0, "pod-name", "replacement", replacementFile, replacementOutput)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("replacement did not remain gated: %v", err)
	}
	if _, err := os.Stat(replacementOutput); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("replacement received handoff")
	}
	t.Run("repair captured gate", func(t *testing.T) {
		oldPod, oldCluster := pod, bytes.Clone(clusterJSON)
		defer func() { pod, clusterJSON = oldPod, oldCluster }()
		replacementIdentity := filepath.Join(dir, "replacement-identity")
		replacementID, _ := types.ManagedNodeID("replacement-uid")
		replacementToken, err := prepareIdentityFile(replacementIdentity, string(cluster), string(replacementID))
		if err != nil {
			t.Fatal(err)
		}
		key := rhiza.PeerPublicKey(string(cluster), string(replacementID), replacementToken)
		report, _ := json.Marshal(map[string]any{"version": 1, "publicKey": key})
		var livePod map[string]any
		_ = json.Unmarshal([]byte(pod), &livePod)
		livePod["metadata"].(map[string]any)["uid"] = "replacement-uid"
		livePod["status"].(map[string]any)["initContainerStatuses"] = []any{map[string]any{"name": operator.V1Alpha2PrepareIdentityContainer, "state": map[string]any{"terminated": map[string]any{"exitCode": 0, "message": string(report)}}}}
		pod = string(mustRepairJSON(t, livePod))
		a, err := operator.LoadV1Alpha2Admission(context.Background(), store, ref)
		if err != nil {
			t.Fatal(err)
		}
		intent := operator.V1Alpha2RepairPlan{OperationID: "repair-gate", ClusterUID: ref.UID, Generation: 0, DeploymentUID: assignment.DeploymentUID, StorageID: assignment.StorageID, ExpectedConfigID: 1, OldNodeID: members[0].ID, OldWALIdentity: "trusted-runtime-registered-wal", Replacement: operator.V1Alpha2RepairMember{PodName: "pod-name", PodUID: "replacement-uid", NodeID: replacementID, PublicKey: key, PeerURL: "quic://127.0.0.1:1234"}, BootstrapPlanDigest: assignment.BootstrapPlanDigest, FinalVoterIDs: []types.NodeID{members[1].ID, members[2].ID, replacementID}}
		slices.Sort(intent.FinalVoterIDs)
		closed, _, err := operator.BeginV1Alpha2Repair(context.Background(), store, a, a.Authority(), intent)
		if err != nil {
			t.Fatal(err)
		}
		// Gate fixture only: these bytes are not presented as a certificate.
		head := []byte("gate-only-fixture")
		namespace, err := nodepkg.ManagedNamespaceFor("root", ref.UID, 0, assignment.AssignmentID, string(cluster))
		if err != nil {
			t.Fatal(err)
		}
		capture := operator.V1Alpha2RepairHandoff{PlanDigest: intent.Digest(), SourceRoot: "root", SourcePrefix: namespace.FinalPrefix, PhysicalBinding: assignment.PhysicalBinding, Head: head, HeadHash: fmt.Sprintf("%x", sha256.Sum256(head)), RemoveTerminalSlot: 17, RemoveTerminalHash: strings.Repeat("b", 64), Survivors: quepaxa.Cluster{ConfigID: 2, Members: members[1:]}}
		if _, _, err := operator.CaptureV1Alpha2RepairHandoff(context.Background(), store, closed, closed.Authority(), capture); err != nil {
			t.Fatal(err)
		}
		bootstrapOutput, repairOutput := filepath.Join(dir, "original-bootstrap.json"), filepath.Join(dir, "repair.json")
		if err := awaitBootstrapWithRepair(context.Background(), k, ref, 0, "pod-name", "replacement-uid", replacementIdentity, bootstrapOutput, repairOutput); err != nil {
			t.Fatal(err)
		}
		var original operator.V1Alpha2BootstrapPlan
		if err := readGateFile(bootstrapOutput, &original, 65536, false); err != nil || !reflect.DeepEqual(original, plan) {
			t.Fatal("repair mutated original bootstrap plan")
		}
		var saved repairGateFile
		if err := readGateFile(repairOutput, &saved, repairFileLimit, true); err != nil || saved.Repair.Plan.OperationID != intent.OperationID {
			t.Fatalf("repair handoff: %v", err)
		}
		data, _ := os.ReadFile(repairOutput)
		if bytes.Contains(data, []byte(replacementToken)) {
			t.Fatal("repair file exposed private token")
		}
		if saved.Binding.PodUID != "replacement-uid" || saved.Binding.ReplicaSetUID == "" || saved.Binding.PodIP != "127.0.0.1" {
			t.Fatal("origin omitted live owner-chain binding")
		}
		// This HTTP gate fixture establishes no consensus proof. A later valid
		// status shape must not retarget D's local origin to the E replacement.
		beforeRollover := bytes.Clone(clusterJSON)
		later := saved.Repair
		later.Plan.OperationID = "later-repair-E"
		later.Plan.ExpectedConfigID = 3
		later.Plan.OldNodeID = members[1].ID
		later.Plan.Replacement.PodUID = "later-E"
		later.Plan.Replacement.PodName = "later-e"
		later.Plan.Replacement.NodeID, _ = types.ManagedNodeID("later-E")
		later.Plan.Replacement.PublicKey = rhiza.PeerPublicKey(string(cluster), string(later.Plan.Replacement.NodeID), "separate-E-key")
		later.Plan.FinalVoterIDs = []types.NodeID{members[2].ID, replacementID, later.Plan.Replacement.NodeID}
		slices.Sort(later.Plan.FinalVoterIDs)
		later.PreparedHandoff = nil
		var rollover map[string]any
		if err := json.Unmarshal(clusterJSON, &rollover); err != nil {
			t.Fatal(err)
		}
		rollover["status"].(map[string]any)["repair"] = later
		clusterJSON = mustRepairJSON(t, rollover)
		if _, err := checkSavedRepairGate(context.Background(), k, ref, 0, "pod-name", "replacement-uid", replacementIdentity, saved); err != nil {
			t.Fatalf("historical D origin rejected after E rollover: %v", err)
		}
		liveE, err := operator.LoadV1Alpha2Admission(context.Background(), store, ref)
		if err != nil || !liveE.SQLReservationsClosed() {
			t.Fatalf("historical gate reopened E SQL reservations: %v", err)
		}
		wrongBinding := saved
		wrongBinding.Binding.ReplicaSetUID = "foreign"
		if _, err := checkSavedRepairGate(context.Background(), k, ref, 0, "pod-name", "replacement-uid", replacementIdentity, wrongBinding); err == nil {
			t.Fatal("historical origin ignored owner-chain change")
		}
		if err := awaitBootstrapWithRepair(context.Background(), k, ref, 0, "pod-name", "replacement-uid", replacementIdentity, bootstrapOutput, repairOutput); err != nil {
			t.Fatal(err)
		}
		afterRollover, _ := os.ReadFile(repairOutput)
		if !bytes.Equal(data, afterRollover) {
			t.Fatal("later repair replaced immutable origin")
		}
		var afterBootstrap operator.V1Alpha2BootstrapPlan
		if err := readGateFile(bootstrapOutput, &afterBootstrap, 65536, false); err != nil || !reflect.DeepEqual(afterBootstrap, original) {
			t.Fatal("historical await rewrote bootstrap")
		}
		if _, _, err := managedRepairConfig(context.Background(), k, ref, "pod-name", "replacement-uid", replacementIdentity, bootstrapOutput, repairOutput, profile); err == nil || !strings.Contains(err.Error(), "historical repair requires existing adoption and WAL") {
			t.Fatalf("historical origin without adoption did not fail at no-backfill boundary: %v", err)
		}
		if _, err := checkSavedRepairGate(context.Background(), k, ref, 0, "pod-name", "new-uid", replacementIdentity, saved); err == nil {
			t.Fatal("historical origin authorized new Pod UID")
		}
		clusterJSON = beforeRollover
		if _, err := checkGate(context.Background(), k, ref, 0, "pod-name", "replacement-uid", replacementIdentity); !errors.Is(err, errLearnerAdmissionPending) {
			t.Fatal("replacement became an original bootstrap member")
		}
		saved.Repair.PreparedHandoff.Head[0] ^= 1
		if err := writeGateFile(replacementIdentity, repairOutput, saved); err != nil {
			t.Fatal(err)
		}
		if _, cleanup, err := managedRepairConfig(context.Background(), k, ref, "pod-name", "replacement-uid", replacementIdentity, bootstrapOutput, repairOutput, profile); err == nil {
			_ = cleanup()
			t.Fatal("tampered repair bytes accepted")
		}
		corruptBytes, _ := os.ReadFile(repairOutput)
		if err := awaitBootstrapWithRepair(context.Background(), k, ref, 0, "pod-name", "replacement-uid", replacementIdentity, bootstrapOutput, repairOutput); err == nil {
			t.Fatal("await accepted corrupted existing origin")
		}
		preserved, _ := os.ReadFile(repairOutput)
		if !bytes.Equal(corruptBytes, preserved) {
			t.Fatal("await repaired or replaced corrupted origin")
		}
		pod = strings.Replace(pod, `"uid":"replacement-uid"`, `"uid":"foreign"`, 1)
		if _, _, err := checkRepairGate(context.Background(), k, ref, 0, "pod-name", "replacement-uid", replacementIdentity, false); err == nil {
			t.Fatal("repair gate accepted foreign live Pod")
		}
	})
	for _, tc := range []struct {
		name   string
		mutate func()
	}{
		{"uid", func() { pod = strings.Replace(pod, `"uid":"pod-1"`, `"uid":"other"`, 1) }},
		{"ip", func() { pod = strings.Replace(pod, `127.0.0.1`, `127.0.0.9`, 1) }},
		{"owner", func() { rs = strings.Replace(rs, `"uid":"deployment"`, `"uid":"foreign"`, 1) }},
		{"missing authority", func() {
			var obj map[string]any
			_ = json.Unmarshal(clusterJSON, &obj)
			delete(obj["status"].(map[string]any), "authority")
			clusterJSON, _ = json.Marshal(obj)
		}},
		{"key", func() { _ = os.Remove(identity); _, _ = prepareIdentityFile(identity, string(cluster), string(node)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			oldPod, oldRS, oldCluster := pod, rs, bytes.Clone(clusterJSON)
			before, _ := os.ReadFile(identity)
			tc.mutate()
			if _, err := checkGate(context.Background(), k, ref, 0, "pod-name", "pod-1", identity); err == nil {
				t.Fatal("guard accepted mismatch")
			}
			pod, rs, clusterJSON = oldPod, oldRS, oldCluster
			if err := os.WriteFile(identity, before, 0600); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestHandoffRejectsIdentityAliases(t *testing.T) {
	dir := t.TempDir()
	identity := filepath.Join(dir, "identity")
	if err := os.WriteFile(identity, []byte("private"), 0600); err != nil {
		t.Fatal(err)
	}
	aliasDir := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(dir, aliasDir); err != nil {
		t.Fatal(err)
	}
	for _, output := range []string{identity, filepath.Join(aliasDir, "identity")} {
		if err := validateHandoffPath(identity, output); err == nil {
			t.Fatal("identity alias accepted")
		}
	}
	hardlink := filepath.Join(dir, "hardlink")
	if err := os.Link(identity, hardlink); err != nil {
		t.Fatal(err)
	}
	if err := validateHandoffPath(identity, hardlink); err == nil {
		t.Fatal("hardlink accepted")
	}
	if err := validateHandoffPath(identity, filepath.Join(dir, "handoff")); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(identity)
	if err != nil || string(data) != "private" {
		t.Fatal("identity changed")
	}
}

func TestBootstrapProbeStrictPending(t *testing.T) {
	base := `{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaCluster","metadata":{"name":"cluster","namespace":"ns","uid":"uid","resourceVersion":"1"},"spec":{"logicalID":"logical","genesisIntent":"Create","voterCount":3,"workload":{"podTemplate":{"spec":{"containers":[{"name":"app","image":"image"}]}}}},"status":{}}`
	for _, tc := range []struct {
		name, body string
		pending    bool
	}{
		{"new", base, true}, {"partial", strings.Replace(base, `"status":{}`, `"status":{"genesis":{}}`, 1), false}, {"null", strings.Replace(base, `"status":{}`, `"status":null`, 1), false}, {"wrong uid", strings.Replace(base, `"uid":"uid"`, `"uid":"other"`, 1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(tc.body)) }))
			t.Cleanup(s.Close)
			token := filepath.Join(t.TempDir(), "token")
			if err := os.WriteFile(token, []byte("test"), 0600); err != nil {
				t.Fatal(err)
			}
			k := &operator.Kubernetes{BaseURL: s.URL, Namespace: "ns", Client: s.Client(), TokenFile: token}
			err := probeBootstrap(context.Background(), k, operator.V1Alpha2ClusterRef{Name: "cluster", Namespace: "ns", UID: "uid"})
			if errors.Is(err, errBootstrapPending) != tc.pending || err == nil {
				t.Fatalf("unexpected result: %v", err)
			}
		})
	}
}

func TestReadIdentityDoesNotCreate(t *testing.T) {
	name := filepath.Join(t.TempDir(), "absent")
	if _, err := readIdentityFile(name, "cluster", "node"); err == nil {
		t.Fatal("missing identity accepted")
	}
	if _, err := os.Stat(name); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("read created identity")
	}
}

func TestAwaitBootstrapCancellationWhilePending(t *testing.T) {
	body := `{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaCluster","metadata":{"name":"cluster","namespace":"ns","uid":"uid","resourceVersion":"1"},"spec":{"logicalID":"logical","genesisIntent":"Create","voterCount":3,"workload":{"podTemplate":{"spec":{"containers":[{"name":"app","image":"image"}]}}}},"status":{}}`
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body)) }))
	t.Cleanup(s.Close)
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenFile, []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	k := &operator.Kubernetes{BaseURL: s.URL, Namespace: "ns", Client: s.Client(), TokenFile: tokenFile}
	cluster, _ := types.ManagedClusterID("uid", 0)
	node, _ := types.ManagedNodeID("pod")
	identity := filepath.Join(dir, "identity")
	if _, err := prepareIdentityFile(identity, string(cluster), string(node)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := awaitBootstrap(ctx, k, operator.V1Alpha2ClusterRef{Name: "cluster", Namespace: "ns", UID: "uid"}, 0, "pod-name", "pod", identity, filepath.Join(dir, "handoff"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected cancellation: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "handoff")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("pending gate published handoff")
	}
}
