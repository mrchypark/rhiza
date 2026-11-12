package managedsql

import (
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func replicaTestMember(id quepaxa.NodeID) quepaxa.Member {
	return quepaxa.Member{ID: id, PublicKey: quepaxa.PublicKey{byte(id[0])}, PeerURL: "quic://127.0.0.1:" + string(id)}
}

func replicaTestPod(member quepaxa.Member, candidate bool) operator.V1Alpha2ManagedPodObservation {
	return operator.V1Alpha2ManagedPodObservation{
		Member:    operator.V1Alpha2RepairPodRequest{PodName: "pod-" + string(member.ID), PodUID: string(member.ID), NodeID: member.ID, PublicKey: member.PublicKey, PeerURL: member.PeerURL},
		Candidate: candidate,
	}
}

func TestReplicaPodRosterMatchesExactCertifiedIdentity(t *testing.T) {
	cluster := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{replicaTestMember("a"), replicaTestMember("b"), replicaTestMember("c")}}
	pods := []operator.V1Alpha2ManagedPodObservation{replicaTestPod(cluster.Members[2], false), replicaTestPod(cluster.Members[0], false), replicaTestPod(cluster.Members[1], false)}
	if err := replicaFullPods(cluster, pods); err != nil {
		t.Fatal(err)
	}

	changed := append([]operator.V1Alpha2ManagedPodObservation(nil), pods...)
	changed[0].Member.PublicKey = quepaxa.PublicKey{9}
	if err := replicaFullPods(cluster, changed); err == nil {
		t.Fatal("changed Pod public key was accepted")
	}
	notRunning := append([]operator.V1Alpha2ManagedPodObservation(nil), pods...)
	notRunning[0].Candidate = true
	if err := replicaFullPods(cluster, notRunning); err == nil {
		t.Fatal("not-yet-running current Pod was accepted as healthy")
	}
}

func TestReplicaUpAndDownSelectActualSinglePod(t *testing.T) {
	cluster := quepaxa.Cluster{ConfigID: 4, Members: []quepaxa.Member{replicaTestMember("a"), replicaTestMember("b"), replicaTestMember("c"), replicaTestMember("d")}}
	current := []operator.V1Alpha2ManagedPodObservation{
		replicaTestPod(cluster.Members[0], false), replicaTestPod(cluster.Members[1], false), replicaTestPod(cluster.Members[2], false), replicaTestPod(cluster.Members[3], false),
	}
	newMember := replicaTestMember("e")
	up, err := replicaUpReplacement(cluster, append(append([]operator.V1Alpha2ManagedPodObservation(nil), current...), replicaTestPod(newMember, true)))
	if err != nil || up.NodeID != newMember.ID {
		t.Fatalf("actual new Pod was not selected: %+v %v", up, err)
	}
	if _, err := replicaUpReplacement(cluster, append(current, replicaTestPod(newMember, false), replicaTestPod(replicaTestMember("f"), true))); err == nil {
		t.Fatal("ambiguous up-step Pod set was accepted")
	}

	downPods := []operator.V1Alpha2ManagedPodObservation{current[3], current[0], current[2]}
	missing, _, err := replicaDownRemoval(cluster, downPods)
	if err != nil || missing.ID != cluster.Members[1].ID {
		t.Fatalf("actual removed voter was not derived: %+v %v", missing, err)
	}
	if _, _, err := replicaDownRemoval(cluster, downPods[:2]); err == nil {
		t.Fatal("multi-voter disappearance was accepted as a down step")
	}
}

func TestReplicaDeploymentStableRequiresObservedUpdatedReadyCounts(t *testing.T) {
	stable := operator.V1Alpha2ReplicaObservation{DesiredReplicas: 4, ObservedReplicas: 4, UpdatedReplicas: 4, ReadyReplicas: 4, DeploymentGeneration: 7, ObservedDeploymentGeneration: 7}
	if !replicaDeploymentStable(stable, 4) {
		t.Fatal("stable Deployment status was rejected")
	}
	for _, change := range []func(*operator.V1Alpha2ReplicaObservation){
		func(o *operator.V1Alpha2ReplicaObservation) { o.ObservedDeploymentGeneration-- },
		func(o *operator.V1Alpha2ReplicaObservation) { o.ObservedReplicas-- },
		func(o *operator.V1Alpha2ReplicaObservation) { o.UpdatedReplicas-- },
		func(o *operator.V1Alpha2ReplicaObservation) { o.ReadyReplicas-- },
	} {
		candidate := stable
		change(&candidate)
		if replicaDeploymentStable(candidate, 4) {
			t.Fatalf("unstable Deployment status was accepted: %+v", candidate)
		}
	}
}

func TestReplicaChallengeUsesCurrentDeploymentRevisionAndBoundsDeadline(t *testing.T) {
	now := time.Now()
	step := operator.V1Alpha2ReplicaStep{Generation: 2, DeploymentUID: "deployment", DeploymentResourceVersion: "old", ConfigID: 7, From: 5, To: 4}
	observation := operator.V1Alpha2ReplicaObservation{DesiredReplicas: 5, ObservedReplicas: 5, UpdatedReplicas: 5, ReadyReplicas: 5, DeploymentGeneration: 3, ObservedDeploymentGeneration: 3, DeploymentResourceVersion: "current"}
	check := operator.V1Alpha2ReplicaCheck{ID: "check", DeploymentResourceVersion: "current", ExpiresAt: now.Add(2 * time.Second)}
	if err := validateReplicaChallenge(check, observation, step.From, now); err != nil {
		t.Fatalf("fresh challenge was rejected because the durable step RV was old: %v", err)
	}
	if deadline := replicaProofDeadline(check, now); !deadline.Equal(check.ExpiresAt) {
		t.Fatalf("proof deadline %s exceeded challenge %s", deadline, check.ExpiresAt)
	}
	changed := observation
	changed.DeploymentResourceVersion = "different"
	if err := validateReplicaChallenge(check, changed, step.From, now); err == nil {
		t.Fatal("challenge for another Deployment revision was accepted")
	}
	expired := check
	expired.ExpiresAt = now
	if err := validateReplicaChallenge(expired, observation, step.From, now); err == nil {
		t.Fatal("expired challenge was accepted")
	}
	proven := step
	proven.CheckID = check.ID
	proven.DeploymentResourceVersion = check.DeploymentResourceVersion
	if sameReplicaStep(step, proven) {
		t.Fatal("proof-bearing step was treated as unproven")
	}
}
