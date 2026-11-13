//go:build rhiza_local_testhooks

package managedsql

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func replicaObservedPods(configs []Config, members []quepaxa.Member) []map[string]any {
	pods := make([]map[string]any, 0, len(configs))
	for i := range configs {
		endpoint, _ := url.Parse(members[i].PeerURL)
		message, _ := json.Marshal(map[string]any{"version": 1, "publicKey": members[i].PublicKey})
		pods = append(pods, map[string]any{
			"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": fmt.Sprintf("pod-%d", i), "namespace": "ns", "uid": fmt.Sprintf("pod-%d", i), "resourceVersion": "1", "ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "rs", "uid": "rsuid", "controller": true}}},
			"status": map[string]any{
				"podIP":                 net.ParseIP(endpoint.Hostname()).String(),
				"initContainerStatuses": []any{map[string]any{"name": operator.V1Alpha2PrepareIdentityContainer, "state": map[string]any{"terminated": map[string]any{"exitCode": 0, "message": string(message)}}}},
				"containerStatuses":     []any{map[string]any{"restartCount": 0, "state": map[string]any{"running": map[string]any{}}}},
			},
		})
	}
	return pods
}

func installReplicaObservation(t *testing.T, store *operator.V1Alpha2StatusStore, ref operator.V1Alpha2ClusterRef, pods []map[string]any, replicas int) {
	t.Helper()
	kube := *store.Kube
	client := *kube.Client
	base := client.Transport
	depName, err := operator.V1Alpha2GenesisDeploymentName(ref.UID)
	if err != nil {
		t.Fatal(err)
	}
	labels := map[string]any{"rhiza.mrchypark.dev/cluster-uid": ref.UID, "rhiza.mrchypark.dev/generation": "0"}
	client.Transport = repairGateTransport(func(r *http.Request) (*http.Response, error) {
		switch {
		case strings.Contains(r.URL.Path, "/pods") && r.URL.Query().Get("labelSelector") != "":
			return repairGateJSON(map[string]any{"apiVersion": "v1", "kind": "PodList", "metadata": map[string]any{"resourceVersion": "1", "continue": ""}, "items": pods}), nil
		case strings.Contains(r.URL.Path, "/pods/"):
			name := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
			for _, pod := range pods {
				if pod["metadata"].(map[string]any)["name"] == name {
					return repairGateJSON(pod), nil
				}
			}
			return &http.Response{StatusCode: http.StatusNotFound, Header: make(http.Header), Body: http.NoBody}, nil
		case strings.Contains(r.URL.Path, "/replicasets/"):
			return repairGateJSON(map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "metadata": map[string]any{"name": "rs", "namespace": ref.Namespace, "uid": "rsuid", "resourceVersion": "1", "ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": depName, "uid": "deployment", "controller": true}}}}), nil
		case strings.Contains(r.URL.Path, "/deployments/") && strings.HasSuffix(r.URL.Path, "/scale"):
			return repairGateJSON(map[string]any{"apiVersion": "autoscaling/v1", "kind": "Scale", "metadata": map[string]any{"name": depName, "namespace": ref.Namespace, "uid": "deployment", "resourceVersion": "1"}, "spec": map[string]any{"replicas": replicas}, "status": map[string]any{"replicas": replicas}}), nil
		case strings.Contains(r.URL.Path, "/deployments/"):
			return repairGateJSON(map[string]any{
				"apiVersion": "apps/v1", "kind": "Deployment",
				"metadata": map[string]any{"name": depName, "namespace": ref.Namespace, "uid": "deployment", "resourceVersion": "1", "generation": 1, "labels": labels, "ownerReferences": []any{map[string]any{"apiVersion": operator.V1Alpha2APIVersion, "kind": "RhizaCluster", "name": ref.Name, "uid": ref.UID, "controller": true}}},
				"spec":     map[string]any{"replicas": replicas, "selector": map[string]any{"matchLabels": labels}, "template": map[string]any{"metadata": map[string]any{"labels": labels}}},
				"status":   map[string]any{"replicas": replicas, "readyReplicas": replicas, "updatedReplicas": replicas, "observedGeneration": 1},
			}), nil
		default:
			return base.RoundTrip(r)
		}
	})
	kube.Client = &client
	store.Kube = &kube
}

func setManagedReplicas(t *testing.T, fixture *sqlStatusFixture, replicas int) {
	t.Helper()
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	var spec map[string]json.RawMessage
	if json.Unmarshal(fixture.obj["spec"], &spec) != nil {
		t.Fatal("invalid fixture spec")
	}
	spec["replicas"], _ = json.Marshal(replicas)
	fixture.obj["spec"], _ = json.Marshal(spec)
}

func TestManagedExternalDownStepGatesPublicLeaveAndOrdinaryReplacement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	configs, fixture, hosts := managedLeaveFixture(t)
	setManagedReplicas(t, fixture, 3)
	a, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := hosts[0].node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil {
		t.Fatal(err)
	}
	step := &operator.V1Alpha2ReplicaStep{
		Generation: a.Authority().Generation, DeploymentUID: a.Authority().Binding.DeploymentUID,
		DeploymentResourceVersion: "host-test-rv", ConfigID: uint64(snapshot.Current.ConfigID),
		From: 4, To: 3, ExpiresAt: time.Now().Add(time.Minute),
	}
	if _, outcome, err := operator.CommitV1Alpha2ReplicaStep(ctx, configs[0].Store, a, a.Authority(), step); err != nil || outcome != operator.V1Alpha2CASApplied {
		t.Fatalf("commit down step: outcome=%s err=%v", outcome, err)
	}
	if err := hosts[0].LeaveVoter(ctx, LeaveVoterRequest{NodeID: configs[3].Runtime.NodeID}); err == nil || !strings.Contains(err.Error(), "fresh Host certification") {
		t.Fatalf("public leave accepted external mode: %v", err)
	}
	if _, err := hosts[0].PlanVoterReplacement(ctx, operator.V1Alpha2RepairPodRequest{}, nil); err == nil || !strings.Contains(err.Error(), "fenced") {
		t.Fatalf("ordinary replacement was not fenced during down step: %v", err)
	}
	after, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	current, present, err := after.ReplicaStep()
	if err != nil || !present || current.Generation != step.Generation || current.DeploymentUID != step.DeploymentUID || current.DeploymentResourceVersion != step.DeploymentResourceVersion || current.ConfigID != step.ConfigID || current.From != step.From || current.To != step.To || !current.ExpiresAt.Equal(step.ExpiresAt) {
		t.Fatalf("gate changed approved step: %+v present=%t err=%v", current, present, err)
	}
	proof := replicaLeaveProof{Step: current, Cluster: snapshot.Current, Missing: configs[3].Runtime.NodeID}
	for _, member := range snapshot.Current.Members {
		if member.ID != configs[3].Runtime.NodeID {
			proof.Survivors = append(proof.Survivors, member.ID)
		}
	}
	if err := validateReplicaLeaveProof(after, LeaveVoterRequest{NodeID: configs[3].Runtime.NodeID}, proof); err != nil {
		t.Fatalf("actual missing voter proof failed: %v", err)
	}
	proof.Missing = configs[2].Runtime.NodeID
	if err := validateReplicaLeaveProof(after, LeaveVoterRequest{NodeID: configs[2].Runtime.NodeID}, proof); err == nil {
		t.Fatal("predicted or changed victim proof was accepted")
	}
	if err := validateReplicaRepairEntry(after, operator.V1Alpha2RepairAdd); err == nil {
		t.Fatal("ADD was accepted during a down step")
	}
	if err := validateReplicaRepairEntry(after, operator.V1Alpha2RepairAddThenRemove); err == nil {
		t.Fatal("ordinary replacement was accepted during a down step")
	}
}

func TestManagedExternalNoStepHealthFailurePreservesOrdinaryRepair(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	configs, fixture, hosts := managedLeaveFixture(t)
	setManagedReplicas(t, fixture, 5)

	store := configs[0].Store
	kube := *store.Kube
	client := *kube.Client
	base := client.Transport
	pods := replicaObservedPods(configs[:3], configs[0].Runtime.Members)
	depName, _ := operator.V1Alpha2GenesisDeploymentName(configs[0].Ref.UID)
	client.Transport = repairGateTransport(func(r *http.Request) (*http.Response, error) {
		switch {
		case strings.Contains(r.URL.Path, "/pods") && r.URL.Query().Get("labelSelector") != "":
			return repairGateJSON(map[string]any{"apiVersion": "v1", "kind": "PodList", "metadata": map[string]any{"resourceVersion": "1", "continue": ""}, "items": pods}), nil
		case strings.Contains(r.URL.Path, "/pods/"):
			name := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
			for _, pod := range pods {
				if pod["metadata"].(map[string]any)["name"] == name {
					return repairGateJSON(pod), nil
				}
			}
			return &http.Response{StatusCode: http.StatusNotFound, Header: make(http.Header), Body: http.NoBody}, nil
		case strings.Contains(r.URL.Path, "/replicasets/"):
			return repairGateJSON(map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "metadata": map[string]any{"name": "rs", "namespace": "ns", "uid": "rsuid", "resourceVersion": "1", "ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": depName, "uid": "deployment", "controller": true}}}}), nil
		default:
			return base.RoundTrip(r)
		}
	})
	kube.Client = &client
	store.Kube = &kube

	stage, err := hosts[0].ReconcileExternalReplicas(ctx)
	if err != nil || stage != "ordinary-repair" {
		t.Fatalf("missing-voter scale preflight did not preserve ordinary repair: stage=%s err=%v", stage, err)
	}
}

func TestManagedExternalDownStepWithoutChallengeRefreshesOnlyBookkeeping(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	configs, fixture, hosts := managedLeaveFixture(t)
	setManagedReplicas(t, fixture, 3)
	a, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := hosts[0].node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil {
		t.Fatal(err)
	}
	step := &operator.V1Alpha2ReplicaStep{
		Generation: a.Authority().Generation, DeploymentUID: a.Authority().Binding.DeploymentUID,
		DeploymentResourceVersion: "old-rv", ConfigID: uint64(snapshot.Current.ConfigID),
		From: 4, To: 3, ExpiresAt: time.Now().Add(150 * time.Millisecond),
	}
	if _, outcome, err := operator.CommitV1Alpha2ReplicaStep(ctx, configs[0].Store, a, a.Authority(), step); err != nil || outcome != operator.V1Alpha2CASApplied {
		t.Fatalf("commit expiring down step: outcome=%s err=%v", outcome, err)
	}
	time.Sleep(200 * time.Millisecond)
	installReplicaObservation(t, configs[0].Store, configs[0].Ref, replicaObservedPods(configs, configs[0].Runtime.Members), 4)
	stage, err := hosts[0].ReconcileExternalReplicas(ctx)
	if err != nil || stage != "refreshed-unproven" {
		t.Fatalf("unextended down step was not refreshed as bookkeeping only: stage=%s err=%v", stage, err)
	}
	after, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	current, present, err := after.ReplicaStep()
	if err != nil || !present || current.CheckID != "" || current.DeploymentResourceVersion != "1" || !current.ExpiresAt.After(time.Now()) {
		t.Fatalf("bookkeeping refresh authorized or failed to renew step: %+v present=%t err=%v", current, present, err)
	}
	if record, ok := after.Repair(); ok {
		t.Fatalf("unproven refresh started repair: %+v", record)
	}
}

func TestManagedExternalClosedLeaveResumeRequiresActualRecordedVictim(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	configs, fixture, hosts := managedLeaveFixture(t)
	setManagedReplicas(t, fixture, 3)
	a, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := hosts[0].node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil {
		t.Fatal(err)
	}
	step := &operator.V1Alpha2ReplicaStep{
		Generation: a.Authority().Generation, DeploymentUID: a.Authority().Binding.DeploymentUID,
		DeploymentResourceVersion: "host-test-rv", ConfigID: uint64(snapshot.Current.ConfigID),
		From: 4, To: 3, ExpiresAt: time.Now().Add(time.Minute),
	}
	if _, outcome, err := operator.CommitV1Alpha2ReplicaStep(ctx, configs[0].Store, a, a.Authority(), step); err != nil || outcome != operator.V1Alpha2CASApplied {
		t.Fatalf("commit down step: outcome=%s err=%v", outcome, err)
	}
	a, err = operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hosts[0].beginLeave(ctx, a, LeaveVoterRequest{NodeID: configs[2].Runtime.NodeID}, 3); err != nil {
		t.Fatal(err)
	}
	installReplicaObservation(t, configs[0].Store, configs[0].Ref, replicaObservedPods(configs[:3], configs[0].Runtime.Members), 3)
	if stage, err := hosts[0].ReconcileExternalReplicas(ctx); err == nil || !strings.Contains(err.Error(), "single actual removed voter") {
		t.Fatalf("wrong actual victim resumed closed intent: stage=%s err=%v", stage, err)
	}
	after, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	record, ok := after.Repair()
	if !ok || record.State != "Closed" || record.Plan.OldNodeID != configs[2].Runtime.NodeID {
		t.Fatalf("closed leave was retargeted or mutated: %+v", record)
	}
	current, err := hosts[0].node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil || current.Current.ConfigID != snapshot.Current.ConfigID {
		t.Fatalf("wrong-victim resume changed membership: %+v err=%v", current.Current, err)
	}
}
