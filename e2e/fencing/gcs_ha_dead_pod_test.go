package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/fencing/cri"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestGCSVoterRemoveAckCheckpointRequiresCommittedRemoveAndNoAdd(t *testing.T) {
	source := gcsRecoverySource{ClusterID: "cluster", StatefulSet: "rhiza", StatefulSetUID: "sts-uid", Targets: []operator.FenceTarget{
		{NodeID: "n0", Pod: "rhiza-0", PodUID: "old-pod", WALIdentity: "old-wal"},
		{NodeID: "n1", Pod: "rhiza-1", PodUID: "pod-1", WALIdentity: "wal-1"},
		{NodeID: "n2", Pod: "rhiza-2", PodUID: "pod-2", WALIdentity: "wal-2"},
	}}
	target, observer := source.Targets[0], source.Targets[1]
	request := operator.FenceRequest{OperationID: "recovery-op", SourceClusterID: source.ClusterID}
	proof := operator.FenceProof{ProofID: "proof-id"}
	remove := network.MembershipChange{OperationID: request.OperationID, ClusterID: source.ClusterID, ExpectedConfigID: 1, Remove: quepaxa.NodeID(target.NodeID),
		Fence: &network.MembershipFence{NodeID: quepaxa.NodeID(target.NodeID), WALIdentity: target.WALIdentity, WorkloadUID: target.PodUID, Confirmed: true, Evidence: proof.ProofID}}
	recovery := operator.Resource{Metadata: map[string]any{"uid": "recovery-uid"},
		Spec: operator.Spec{StatefulSet: source.StatefulSet, SourceClusterID: source.ClusterID, RecoveryID: request.OperationID,
			Fence: operator.Fence{RecoveryID: request.OperationID, ClusterID: source.ClusterID, StatefulSetUID: source.StatefulSetUID, Confirmed: true, Evidence: proof.ProofID},
			Membership: &operator.MembershipSpec{OperationID: request.OperationID, Remove: quepaxa.NodeID(target.NodeID), RemovedPod: target.Pod,
				ReplacementPod: target.Pod, ReplacementSecret: "replacement-secret", Fence: operator.MembershipFence{NodeID: quepaxa.NodeID(target.NodeID),
					WALIdentity: target.WALIdentity, WorkloadUID: target.PodUID, Confirmed: true, Evidence: proof.ProofID},
				VoterPods: []operator.VoterPod{{NodeID: quepaxa.NodeID("n1"), Pod: "rhiza-1"}, {NodeID: quepaxa.NodeID("n2"), Pod: "rhiza-2"}}}},
		Status: operator.Status{Phase: "Removing", Message: gcsVoterRemoveAcceptedMessage, Source: source.ClusterID, StatefulSetUID: source.StatefulSetUID,
			Membership: &operator.MembershipStatus{Phase: "Removing", RemoveRequest: &remove}}}
	status := network.MembershipStatus{NodeID: quepaxa.NodeID(observer.NodeID), ClusterID: source.ClusterID, ConfigID: 2,
		Voters: []quepaxa.NodeID{"n1", "n2"}, WALIdentity: observer.WALIdentity, Voting: true}
	if !gcsVoterRemoveAckCheckpointMatches(recovery, source, target, request, proof, observer, status) {
		t.Fatal("rejected canonical post-REMOVE, pre-ADD checkpoint")
	}
	tests := []struct {
		name   string
		mutate func(*operator.Resource, *network.MembershipStatus)
	}{
		{"ADD already journaled", func(r *operator.Resource, _ *network.MembershipStatus) {
			r.Status.Membership.Add = &operator.MembershipAddJournal{OperationID: "add-op"}
		}},
		{"wrong status message", func(r *operator.Resource, _ *network.MembershipStatus) {
			r.Status.Message = "remove request journaled before dispatch"
		}},
		{"wrong recovery phase", func(r *operator.Resource, _ *network.MembershipStatus) { r.Status.Phase = "Adding" }},
		{"wrong recovery identity", func(r *operator.Resource, _ *network.MembershipStatus) { r.Metadata["uid"] = "other-recovery" }},
		{"wrong Recovery membership binding", func(r *operator.Resource, _ *network.MembershipStatus) { r.Spec.Membership.Remove = "other-node" }},
		{"wrong captured survivor set", func(r *operator.Resource, _ *network.MembershipStatus) {
			r.Spec.Membership.VoterPods[0].Pod = "other-pod"
		}},
		{"wrong Recovery fence binding", func(r *operator.Resource, _ *network.MembershipStatus) { r.Spec.Fence.Evidence = "other-proof" }},
		{"remove not committed", func(_ *operator.Resource, s *network.MembershipStatus) { s.ConfigID = 1 }},
		{"membership still pending", func(_ *operator.Resource, s *network.MembershipStatus) { s.Pending = true }},
		{"old voter still present", func(_ *operator.Resource, s *network.MembershipStatus) { s.Voters = []quepaxa.NodeID{"n0", "n1"} }},
		{"wrong remove proof", func(r *operator.Resource, _ *network.MembershipStatus) {
			r.Status.Membership.RemoveRequest.Fence.Evidence = "other-proof"
		}},
		{"wrong observer identity", func(_ *operator.Resource, s *network.MembershipStatus) { s.WALIdentity = "other-wal" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var changed operator.Resource
			if err := json.Unmarshal(encoded(recovery), &changed); err != nil {
				t.Fatal(err)
			}
			changedStatus := status
			changedStatus.Voters = append([]quepaxa.NodeID(nil), status.Voters...)
			test.mutate(&changed, &changedStatus)
			if gcsVoterRemoveAckCheckpointMatches(changed, source, target, request, proof, observer, changedStatus) {
				t.Fatal("accepted an incomplete or advanced REMOVE checkpoint")
			}
		})
	}
}

func TestGCSDeadPodQualificationRequiresExactPassiveTerminalReceipt(t *testing.T) {
	runtime := fencing.RuntimeBinding{AuthorityID: "authority", NamespaceUID: "namespace-uid", BindingUID: "binding-uid",
		StatefulSetUID: "sts-uid", SourceClusterID: "cluster", NodeID: "node-0", PodUID: "pod-old", RunID: "run-old",
		NodeName: "node", NodeUID: "node-uid", MachineID: "machine", BootID: "boot", RuntimeID: "runtime", SandboxID: "sandbox",
		ContainerID: "containerd://container", ImageID: "image@sha256:abc", CapturedScopeDigest: strings.Repeat("9", 64)}
	target := operator.FenceTarget{NodeID: runtime.NodeID, Pod: "rhiza-voter-0", PodUID: runtime.PodUID, WALIdentity: "wal"}
	request := fencing.StopRequest{OperationID: "exact-stop", RequestHash: strings.Repeat("b", 64), Identity: fencing.Identity{
		SourceClusterID: runtime.SourceClusterID, NodeID: runtime.NodeID, PodUID: runtime.PodUID, RunID: runtime.RunID, WALIdentity: target.WALIdentity}}
	ref := fencing.TerminalReference{EvidenceVersion: 2, Request: request, RuntimeHash: fencing.RuntimeBindingDigest(runtime), ProducerUID: "producer"}
	receipt := cri.PassiveReceipt{Mode: "passive-kernel-v1", Runtime: runtime, Request: request, Observation: cri.PassiveObservation{
		Version: 1, RuntimeHash: fencing.RuntimeScopeBindingDigest(runtime), ScopeDigest: runtime.CapturedScopeDigest,
		TargetPIDExited: true, TargetCgroupGone: true, SandboxPIDExited: true, SandboxCgroupGone: true, ObservedAt: "2026-10-23T00:00:00Z"}}
	if err := validateGCSDeadPodPassiveReceipt(encoded(receipt), ref, target, runtime, "producer"); err != nil {
		t.Fatalf("rejected exact passive terminal receipt: %v", err)
	}

	t.Run("normal CRI receipt is not passive qualification", func(t *testing.T) {
		fallback := cri.StopObservation{Runtime: runtime, Request: request, ContainerExited: true, SandboxNotReady: true, CgroupEmpty: true, ObservedAt: receipt.Observation.ObservedAt}
		if err := validateGCSDeadPodPassiveReceipt(encoded(fallback), ref, target, runtime, "producer"); err == nil {
			t.Fatal("accepted the normal CRI fallback receipt as passive dead-Pod proof")
		}
	})
	t.Run("wrong request", func(t *testing.T) {
		wrong := receipt
		wrong.Request.OperationID = "other-stop"
		if err := validateGCSDeadPodPassiveReceipt(encoded(wrong), ref, target, runtime, "producer"); err == nil {
			t.Fatal("accepted a passive receipt for another stop request")
		}
	})
	t.Run("wrong incarnation", func(t *testing.T) {
		wrong := receipt
		wrong.Runtime.PodUID = "pod-new"
		if err := validateGCSDeadPodPassiveReceipt(encoded(wrong), ref, target, runtime, "producer"); err == nil {
			t.Fatal("accepted a passive receipt for another Pod incarnation")
		}
	})
}

func TestGCSDeadPodPassiveProfilePreflightRequiresInstalledQualifiedRollout(t *testing.T) {
	profile, daemonSet, pods, source := gcsDeadPodPassiveProfileFixture()
	if err := validateGCSDeadPodPassiveAgentProfile(profile, daemonSet, pods, source); err != nil {
		t.Fatalf("rejected installed qualified runtime-agent rollout: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(obj, obj, obj)
	}{
		{"unset executable approval", func(profile, _, _ obj) { profile["data"].(obj)["sandbox-executable-sha256"] = "" }},
		{"invalid executable approval", func(profile, _, _ obj) { profile["data"].(obj)["sandbox-executable-sha256"] = strings.Repeat("A", 64) }},
		{"wrong approved argv", func(profile, _, _ obj) { profile["data"].(obj)["sandbox-args-json"] = `["/other"]` }},
		{"digest does not opt in to passive capability", func(_, ds, _ obj) {
			ds["spec"].(obj)["template"].(obj)["metadata"].(obj)["annotations"].(obj)["rhiza.mrchypark.dev/runtime-proof-profile"] = nil
		}},
		{"ConfigMap changed without rollout", func(profile, _, _ obj) { profile["data"].(obj)["sandbox-executable-sha256"] = strings.Repeat("b", 64) }},
		{"wrong ConfigMap identity", func(profile, _, _ obj) { profile["metadata"].(obj)["uid"] = "" }},
		{"wrong ConfigMap key reference", func(_, ds, _ obj) {
			ds["spec"].(obj)["template"].(obj)["spec"].(obj)["containers"].([]any)[0].(obj)["env"].([]any)[0].(obj)["valueFrom"].(obj)["configMapKeyRef"].(obj)["key"] = "other"
		}},
		{"missing command binding", func(_, ds, _ obj) {
			args := ds["spec"].(obj)["template"].(obj)["spec"].(obj)["containers"].([]any)[0].(obj)["args"].([]any)
			ds["spec"].(obj)["template"].(obj)["spec"].(obj)["containers"].([]any)[0].(obj)["args"] = args[1:]
		}},
		{"stale observed generation", func(_, ds, _ obj) { ds["status"].(obj)["observedGeneration"] = float64(1) }},
		{"incomplete rollout", func(_, ds, _ obj) { ds["status"].(obj)["updatedNumberScheduled"] = float64(1) }},
		{"wrong template profile annotation", func(_, ds, _ obj) {
			ds["spec"].(obj)["template"].(obj)["metadata"].(obj)["annotations"].(obj)["rhiza.dev/sandbox-executable-sha256"] = strings.Repeat("b", 64)
		}},
		{"wrong Pod profile annotation", func(_, _, pods obj) {
			pods["items"].([]any)[0].(obj)["metadata"].(obj)["annotations"].(obj)["rhiza.dev/sandbox-executable-sha256"] = strings.Repeat("b", 64)
		}},
		{"Pod missing passive profile annotation", func(_, _, pods obj) {
			delete(pods["items"].([]any)[0].(obj)["metadata"].(obj)["annotations"].(obj), "rhiza.mrchypark.dev/runtime-proof-profile")
		}},
		{"host PID namespace disabled", func(_, ds, _ obj) { ds["spec"].(obj)["template"].(obj)["spec"].(obj)["hostPID"] = false }},
		{"default capless profile", func(_, ds, _ obj) {
			delete(ds["spec"].(obj)["template"].(obj)["spec"].(obj)["containers"].([]any)[0].(obj), "securityContext")
		}},
		{"extra acquired capability", func(_, ds, _ obj) {
			ds["spec"].(obj)["template"].(obj)["spec"].(obj)["containers"].([]any)[0].(obj)["securityContext"].(obj)["capabilities"].(obj)["add"] = []any{"SYS_PTRACE", "SYS_ADMIN"}
		}},
		{"extra sidecar", func(_, ds, _ obj) {
			spec := ds["spec"].(obj)["template"].(obj)["spec"].(obj)
			spec["containers"] = append(spec["containers"].([]any), obj{"name": "unverified-sidecar"})
		}},
		{"ephemeral container", func(_, _, pods obj) {
			pods["items"].([]any)[0].(obj)["spec"].(obj)["ephemeralContainers"] = []any{obj{"name": "debug"}}
		}},
		{"privileged agent", func(_, ds, _ obj) {
			ds["spec"].(obj)["template"].(obj)["spec"].(obj)["containers"].([]any)[0].(obj)["securityContext"].(obj)["privileged"] = true
		}},
		{"changed captured source agent UID", func(_, _, pods obj) { pods["items"].([]any)[0].(obj)["metadata"].(obj)["uid"] = "replacement-agent" }},
		{"source-node agent not ready", func(_, _, pods obj) { pods["items"].([]any)[0].(obj)["status"].(obj)["conditions"] = []any{} }},
		{"agent container not ready", func(_, _, pods obj) {
			pods["items"].([]any)[0].(obj)["status"].(obj)["containerStatuses"].([]any)[0].(obj)["ready"] = false
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			profileCopy := encodedClone(profile)
			daemonSetCopy := encodedClone(daemonSet)
			podsCopy := encodedClone(pods)
			test.mutate(profileCopy, daemonSetCopy, podsCopy)
			if err := validateGCSDeadPodPassiveAgentProfile(profileCopy, daemonSetCopy, podsCopy, source); err == nil {
				t.Fatal("accepted an unqualified or incomplete installed runtime-agent profile")
			}
		})
	}
}

func TestGCSDeadPodProfilePreflightRunsBeforePodChaos(t *testing.T) {
	source, err := os.ReadFile("gcs_ha_dead_pod.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	preflight := strings.Index(text, "verifyGCSDeadPodPassiveAgentProfile(ctx, source)")
	fault := strings.Index(text, "createGCSDeadPodFault(ctx, source, target)")
	if preflight < 0 || fault < 0 || preflight >= fault {
		t.Fatal("passive profile preflight must precede dead-Pod Chaos creation")
	}
}

func gcsDeadPodPassiveProfileFixture() (obj, obj, obj, gcsRecoverySource) {
	const profileName, daemonSetUID = "rhiza-runtime-agent-profile", "agent-ds-uid"
	profile := obj{"metadata": obj{"name": profileName, "namespace": "run-ns", "uid": "profile-uid", "resourceVersion": "profile-rv"},
		"data": obj{"sandbox-executable-sha256": strings.Repeat("a", 64), "sandbox-args-json": `["/pause"]`}}
	container := obj{"name": "runtime-agent", "args": []any{"--sandbox-executable-sha256=$(SANDBOX_EXECUTABLE_SHA256)", "--sandbox-args-json=$(SANDBOX_ARGS_JSON)"},
		"securityContext": obj{"privileged": false, "allowPrivilegeEscalation": false,
			"capabilities": obj{"drop": []any{"ALL"}, "add": []any{"SYS_PTRACE"}}},
		"env": []any{
			obj{"name": "SANDBOX_EXECUTABLE_SHA256", "valueFrom": obj{"configMapKeyRef": obj{"name": profileName, "key": "sandbox-executable-sha256"}}},
			obj{"name": "SANDBOX_ARGS_JSON", "valueFrom": obj{"configMapKeyRef": obj{"name": profileName, "key": "sandbox-args-json"}}},
		}}
	selector := obj{"app.kubernetes.io/name": runtimeAgentDSName}
	daemonSet := obj{"metadata": obj{"name": runtimeAgentDSName, "namespace": "run-ns", "uid": daemonSetUID, "generation": float64(7)},
		"spec": obj{"selector": obj{"matchLabels": selector}, "template": obj{"metadata": obj{"annotations": obj{
			"rhiza.dev/sandbox-executable-sha256": strings.Repeat("a", 64), "rhiza.mrchypark.dev/runtime-proof-profile": "passive-kernel-v1"}},
			"spec": obj{"hostPID": true, "containers": []any{container}}}},
		"status": obj{"observedGeneration": float64(7), "desiredNumberScheduled": float64(2), "currentNumberScheduled": float64(2),
			"updatedNumberScheduled": float64(2), "numberReady": float64(2), "numberAvailable": float64(2)}}
	makePod := func(name, uid, node string) obj {
		return obj{"metadata": obj{"name": name, "uid": uid, "labels": obj{"app.kubernetes.io/name": runtimeAgentDSName},
			"annotations": obj{"rhiza.dev/sandbox-executable-sha256": strings.Repeat("a", 64),
				"rhiza.mrchypark.dev/runtime-proof-profile": "passive-kernel-v1"},
			"ownerReferences": []any{obj{"apiVersion": "apps/v1", "kind": "DaemonSet", "name": runtimeAgentDSName, "uid": daemonSetUID, "controller": true}}},
			"spec": obj{"nodeName": node, "hostPID": true, "containers": []any{encodedClone(container)}},
			"status": obj{"conditions": []any{obj{"type": "Ready", "status": "True"}},
				"containerStatuses": []any{obj{"name": "runtime-agent", "ready": true}}}}
	}
	pods := obj{"items": []any{makePod("agent-a", "agent-a-uid", "node-a"), makePod("agent-b", "agent-b-uid", "node-b")}}
	source := gcsRecoverySource{Namespace: "run-ns", AgentDaemonUID: daemonSetUID,
		AgentPods: []gcsControlPodIdentity{{Name: "agent-a", UID: "agent-a-uid", OwnerUID: daemonSetUID}, {Name: "agent-b", UID: "agent-b-uid", OwnerUID: daemonSetUID}},
		Targets:   []operator.FenceTarget{{NodeName: "node-a"}, {NodeName: "node-b"}}}
	return profile, daemonSet, pods, source
}

func TestGCSDeadPodChaosObjectUsesExactAdmittedSelector(t *testing.T) {
	body, err := gcsDeadPodChaosObject("run-deadbeef", "deadbeef", "rhiza-voter-0")
	if err != nil {
		t.Fatal(err)
	}
	if nested(body, "kind") != "PodChaos" || nested(body, "metadata", "name") != "gcs-dead-pod-deadbeef" ||
		nested(body, "spec", "action") != "pod-kill" || nested(body, "spec", "mode") != "one" || nested(body, "spec", "duration") != "10s" {
		t.Fatalf("unexpected dead-Pod Chaos contract: %v", body)
	}
	selector := nested(body, "spec", "selector")
	if !jsonSemanticEqual(nested(selector, "namespaces"), []any{"run-deadbeef"}) || nested(selector, "pods") != nil {
		t.Fatalf("selector is not bound to the one exact run Pod: %v", selector)
	}
	labels, ok := nested(selector, "labelSelectors").(obj)
	if !ok || len(labels) != 3 || labels["statefulset.kubernetes.io/pod-name"] != "rhiza-voter-0" || labels["app.kubernetes.io/part-of"] != "rhiza-chaos" || labels[gcsChaosRunLabel] != "deadbeef" {
		t.Fatalf("selector differs from admitted run-owned voter profile: %v", labels)
	}
	spec, _ := nested(body, "spec").(obj)
	if _, ok := spec["containerNames"]; ok {
		t.Fatal("PodChaos must kill the whole Pod, not only a container")
	}
	created := obj{}
	if err := json.Unmarshal(encoded(obj{"apiVersion": nested(body, "apiVersion"), "kind": nested(body, "kind"),
		"metadata": obj{"name": nested(body, "metadata", "name"), "namespace": nested(body, "metadata", "namespace"),
			"uid": "chaos-uid", "labels": obj{gcsChaosRunLabel: "deadbeef"}}, "spec": nested(body, "spec")}), &created); err != nil {
		t.Fatal(err)
	}
	if !gcsDeadPodObjectMatches(created, body) {
		t.Fatal("rejected exact created PodChaos object")
	}
	created["spec"].(obj)["duration"] = "180s"
	if gcsDeadPodObjectMatches(created, body) {
		t.Fatal("accepted a mutated PodChaos spec")
	}
}

func TestGCSDeadPodChaosRejectsInvalidRunOrPodNames(t *testing.T) {
	for _, input := range [][3]string{{"run-deadbeef", "bad!beef", "rhiza-voter-0"}, {"run-deadbeef", "deadbeef", "rhiza-voter-3"}, {"run-deadbeef", "deadbeeG", "rhiza-voter-0"}} {
		if _, err := gcsDeadPodChaosObject(input[0], input[1], input[2]); err == nil {
			t.Fatalf("accepted invalid Chaos identity %q %q %q", input[0], input[1], input[2])
		}
	}
}

func TestVerifyIndependentVoterProofBindsExactGCSIncarnation(t *testing.T) {
	source := gcsRecoverySource{Namespace: "run-deadbeef", LogicalID: "logical", StatefulSet: "rhiza-voter", StatefulSetUID: "sts-uid",
		BindingUID: "binding-uid", ClusterID: "cluster", Bucket: "archive", Prefix: "rhiza/cluster"}
	runtime := fencing.RuntimeBinding{AuthorityID: "authority", NamespaceUID: "namespace-uid", BindingUID: source.BindingUID,
		StatefulSetUID: source.StatefulSetUID, SourceClusterID: source.ClusterID, NodeID: "rhiza-voter-0", PodUID: "old-pod", RunID: "exact-run",
		NodeName: "node", NodeUID: "node-uid", MachineID: "machine", BootID: "boot", RuntimeID: "runtime", SandboxID: "sandbox",
		ContainerID: "containerd://container", ImageID: "image@sha256:abc", CapturedScopeDigest: strings.Repeat("9", 64)}
	producerUID := "exact-producer"
	target := operator.FenceTarget{NodeID: "rhiza-voter-0", Pod: "rhiza-voter-0", PodUID: "old-pod", WALIdentity: "wal",
		NodeName: "node", NodeUID: "node-uid", ContainerID: "containerd://container", ImageID: "image@sha256:abc"}
	request := operator.FenceRequest{LogicalID: source.LogicalID, BindingUID: source.BindingUID, Namespace: source.Namespace,
		StatefulSet: source.StatefulSet, StatefulSetUID: source.StatefulSetUID, SourceClusterID: source.ClusterID,
		OperationID: "auto-recovery", Scope: operator.ScopeVoter, Targets: []operator.FenceTarget{target}}
	scope := operator.AttestationStoreScope{Provider: "gcs", Bucket: source.Bucket, Prefix: source.Prefix}
	scopeDigest := fmt.Sprintf("%x", sha256.Sum256(encoded(scope)))
	evidence := &operator.IndependentEvidenceV2{Version: 2, RequestHash: fmt.Sprintf("%x", sha256.Sum256(encoded(request))),
		LedgerHash: strings.Repeat("a", 64), StorageScope: scope, CurrentPodUIDs: []string{"old-pod", "survivor-1", "survivor-2"},
		Terminals: []fencing.TerminalReference{{EvidenceVersion: 2,
			Request: fencing.StopRequest{OperationID: "stop-old", RequestHash: strings.Repeat("b", 64), Identity: fencing.Identity{
				SourceClusterID: source.ClusterID, NodeID: target.NodeID, PodUID: target.PodUID, RunID: runtime.RunID, WALIdentity: target.WALIdentity}},
			RuntimeHash: fencing.RuntimeBindingDigest(runtime), ProducerUID: producerUID, ProofName: "proof", ProofUID: "proof-uid",
			ProofDigest: strings.Repeat("d", 64), RuntimeReceiptDigest: strings.Repeat("e", 64), MutationReceiptDigest: strings.Repeat("f", 64), StorageScopeDigest: scopeDigest}},
	}
	proof := operator.FenceProof{OperationID: request.OperationID, SourceClusterID: source.ClusterID, BindingUID: source.BindingUID,
		RequestHash: evidence.RequestHash, ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true, IndependentV2: evidence}
	proof.ProofID = "independent-v2-" + fmt.Sprintf("%x", sha256.Sum256(encoded(evidence)))
	item := obj{"metadata": obj{"name": "fence", "uid": "fence-uid"}, "spec": request, "status": obj{"proof": proof}}
	if _, _, err := verifyIndependentVoterProof(item, source, target, runtime, producerUID); err != nil {
		t.Fatalf("valid exact voter proof rejected: %v", err)
	}

	badRequest := request
	badRequest.Scope = operator.ScopeGeneration
	badItem := obj{"spec": badRequest, "status": obj{"proof": proof}}
	if _, _, err := verifyIndependentVoterProof(badItem, source, target, runtime, producerUID); err == nil {
		t.Fatal("accepted a non-Voter scope")
	}
	for name, mutate := range map[string]func(*fencing.TerminalReference){
		"different enrolled run":   func(ref *fencing.TerminalReference) { ref.Request.Identity.RunID = "historical-run" },
		"different runtime digest": func(ref *fencing.TerminalReference) { ref.RuntimeHash = strings.Repeat("1", 64) },
		"different producer":       func(ref *fencing.TerminalReference) { ref.ProducerUID = "historical-producer" },
	} {
		t.Run(name, func(t *testing.T) {
			badEvidence := *evidence
			badEvidence.Terminals = append([]fencing.TerminalReference(nil), evidence.Terminals...)
			mutate(&badEvidence.Terminals[0])
			badProof := proof
			badProof.IndependentV2 = &badEvidence
			badProof.ProofID = "independent-v2-" + fmt.Sprintf("%x", sha256.Sum256(encoded(&badEvidence)))
			badItem := obj{"spec": request, "status": obj{"proof": badProof}}
			if _, _, err := verifyIndependentVoterProof(badItem, source, target, runtime, producerUID); err == nil {
				t.Fatal("accepted historical terminal with the same Pod UID and WAL")
			}
		})
	}
}
