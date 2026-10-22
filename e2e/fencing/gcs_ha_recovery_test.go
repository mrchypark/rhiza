package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/operator"
)

func TestGCSRecoveryMatchesApprovedWorkloadSelector(t *testing.T) {
	selector := map[string]any{"app": "rhiza-voter", "app.kubernetes.io/part-of": "rhiza-chaos", "chaos.rhiza.io/run": "deadbeef"}
	if !labelsMatch(map[string]any{"app": "rhiza-voter", "app.kubernetes.io/part-of": "rhiza-chaos", "chaos.rhiza.io/run": "deadbeef", "statefulset.kubernetes.io/pod-name": "rhiza-voter-2"}, selector) {
		t.Fatal("rejected matching run-owned StatefulSet selector")
	}
	if labelsMatch(map[string]any{"app": "rhiza-voter", "app.kubernetes.io/part-of": "rhiza-chaos", "chaos.rhiza.io/run": "other"}, selector) {
		t.Fatal("accepted voter from another run")
	}
}

func TestGCSRecoveryRequiresNoPVCWorkload(t *testing.T) {
	if !gcsWorkloadHasNoPVC(obj{"spec": obj{"template": obj{"spec": obj{"volumes": []any{obj{"name": "data", "emptyDir": obj{}}}}}}}) {
		t.Fatal("rejected no-PVC emptyDir workload")
	}
	for _, workload := range []obj{
		{"spec": obj{"volumeClaimTemplates": []any{obj{"metadata": obj{"name": "data"}}}, "template": obj{"spec": obj{}}}},
		{"spec": obj{"template": obj{"spec": obj{"volumes": []any{obj{"name": "data", "persistentVolumeClaim": obj{"claimName": "data"}}}}}}},
		{"spec": obj{"template": obj{"spec": obj{"volumes": []any{obj{"name": "data", "hostPath": obj{"path": "/var/lib/rhiza"}}}}}}},
		{"spec": obj{"template": obj{"spec": obj{"volumes": []any{obj{"name": "data", "csi": obj{"driver": "example.invalid/driver"}}}}}}},
	} {
		if gcsWorkloadHasNoPVC(workload) {
			t.Fatalf("accepted PVC-backed workload: %v", workload)
		}
	}
}

func TestGCSFenceSpecComparisonDecodesKubernetesMap(t *testing.T) {
	expected := operator.FenceRequest{LogicalID: "logical", BindingUID: "binding", Namespace: "run-ns", StatefulSet: "rhiza-voter", StatefulSetUID: "sts-uid", SourceClusterID: "generation-1", OperationID: "fence-1", Scope: operator.ScopeGeneration, Targets: []operator.FenceTarget{{NodeID: "rhiza-voter-0", Pod: "rhiza-voter-0", PodUID: "pod-uid", WALIdentity: "wal"}}}
	var apiSpec any
	if err := json.Unmarshal(encoded(expected), &apiSpec); err != nil {
		t.Fatal(err)
	}
	if !sameFenceRequestSpec(apiSpec, expected) {
		t.Fatal("rejected unchanged Kubernetes map representation of a fence request")
	}
	apiSpec.(map[string]any)["operationID"] = "replacement"
	if sameFenceRequestSpec(apiSpec, expected) {
		t.Fatal("accepted a changed operation ID in the Kubernetes fence spec")
	}
}

func TestGCSProofWaitIsBoundToTheExactPendingFenceResourceAndOperation(t *testing.T) {
	request := operator.FenceRequest{
		LogicalID: "logical", BindingUID: "binding", Namespace: "run-ns", StatefulSet: "rhiza-voter", StatefulSetUID: "sts-uid",
		SourceClusterID: "generation-1", OperationID: "fence-exact", Scope: operator.ScopeGeneration,
		Targets: []operator.FenceTarget{{NodeID: "voter-0", Pod: "voter-0", PodUID: "pod-uid", WALIdentity: "wal"}},
	}
	pending := pendingGCSFence{Name: "fence-resource", UID: "fence-uid", Request: request}
	item := obj{"metadata": obj{"name": pending.Name, "uid": pending.UID}, "spec": request}
	if got, err := exactPendingGCSFenceRequest(item, pending); err != nil || !sameFenceRequest(got, request) {
		t.Fatalf("rejected the exact pending fence resource: %v", err)
	}

	otherOperation := request
	otherOperation.OperationID = "another-operation-same-cohort"
	item["spec"] = otherOperation
	if _, err := exactPendingGCSFenceRequest(item, pending); err == nil {
		t.Fatal("accepted another operation for the same source cohort")
	}
	item["spec"] = request
	item["metadata"] = obj{"name": pending.Name, "uid": "replacement-resource-uid"}
	if _, err := exactPendingGCSFenceRequest(item, pending); err == nil {
		t.Fatal("accepted a same-name replacement RhizaFence resource")
	}
}

func TestScopedProofUsesCapturedSourceGeneration(t *testing.T) {
	request := operator.FenceRequest{LogicalID: "fencing-e2e", BindingUID: "binding", Namespace: namespace, StatefulSet: "rhiza", StatefulSetUID: "sts", SourceClusterID: "gcs-source", OperationID: "fence", Scope: operator.ScopeGeneration}
	proof := operator.FenceProof{OperationID: "fence", SourceClusterID: "gcs-source", BindingUID: "binding"}
	item := obj{"spec": request, "status": obj{"proof": proof}}
	if err := verifyScopedProof(nil, item, "sts", "binding", nil, operator.ScopeGeneration, nil, t.TempDir(), "different-source"); err == nil {
		t.Fatal("accepted a proof for a source generation other than the captured source")
	}
}

func TestVerifyIndependentGenerationProofBindsGCSAndCapturedIncarnations(t *testing.T) {
	target := operator.FenceTarget{NodeID: "rhiza-0", Pod: "rhiza-0", PodUID: "source-pod-uid", WALIdentity: "source-wal"}
	request := operator.FenceRequest{LogicalID: "fencing-e2e", BindingUID: "binding-uid", Namespace: namespace, StatefulSet: "rhiza", StatefulSetUID: "sts-uid", SourceClusterID: "source-generation", OperationID: "fence-op", Scope: operator.ScopeGeneration, Targets: []operator.FenceTarget{target}}
	scope := operator.AttestationStoreScope{Provider: "gcs", Bucket: "qualification-bucket", Prefix: "rhiza/source-generation"}
	scopeDigest := fmt.Sprintf("%x", sha256.Sum256(encoded(scope)))
	evidence := &operator.IndependentEvidenceV2{
		Version: 2, RequestHash: fmt.Sprintf("%x", sha256.Sum256(encoded(request))), LedgerHash: strings.Repeat("a", 64), StorageScope: scope,
		Terminals: []fencing.TerminalReference{{EvidenceVersion: 2, Request: fencing.StopRequest{OperationID: "source-stop", RequestHash: strings.Repeat("b", 64), Identity: fencing.Identity{SourceClusterID: request.SourceClusterID, NodeID: target.NodeID, PodUID: target.PodUID, RunID: "run-1", WALIdentity: target.WALIdentity}}, RuntimeHash: strings.Repeat("c", 64), ProducerUID: "producer-uid", ProofName: "proof-name", ProofUID: "proof-uid", ProofDigest: strings.Repeat("d", 64), RuntimeReceiptDigest: strings.Repeat("e", 64), MutationReceiptDigest: strings.Repeat("f", 64), StorageScopeDigest: scopeDigest}},
	}
	proof := operator.FenceProof{OperationID: request.OperationID, SourceClusterID: request.SourceClusterID, BindingUID: request.BindingUID, RequestHash: evidence.RequestHash,
		ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true, IndependentV2: evidence}
	proof.ProofID = "independent-v2-" + fmt.Sprintf("%x", sha256.Sum256(encoded(evidence)))
	item := obj{"spec": request, "status": obj{"proof": proof}}
	if _, _, err := verifyIndependentGenerationProof(item, request.SourceClusterID, request.StatefulSetUID, request.BindingUID, []operator.FenceTarget{target}, scope.Bucket, scope.Prefix, request.Namespace, request.StatefulSet, request.LogicalID); err != nil {
		t.Fatalf("valid proof rejected: %v", err)
	}

	badProof := proof
	badEvidence := *evidence
	badEvidence.StorageScope.Bucket = "other-bucket"
	badProof.IndependentV2 = &badEvidence
	badItem := obj{"spec": request, "status": obj{"proof": badProof}}
	if _, _, err := verifyIndependentGenerationProof(badItem, request.SourceClusterID, request.StatefulSetUID, request.BindingUID, []operator.FenceTarget{target}, scope.Bucket, scope.Prefix, request.Namespace, request.StatefulSet, request.LogicalID); err == nil {
		t.Fatal("accepted proof for another GCS bucket")
	}

	badEvidence = *evidence
	badEvidence.Terminals = append([]fencing.TerminalReference(nil), evidence.Terminals...)
	badEvidence.Terminals[0].Request.Identity.PodUID = "replacement-pod-uid"
	badProof = proof
	badProof.IndependentV2 = &badEvidence
	badItem = obj{"spec": request, "status": obj{"proof": badProof}}
	if _, _, err := verifyIndependentGenerationProof(badItem, request.SourceClusterID, request.StatefulSetUID, request.BindingUID, []operator.FenceTarget{target}, scope.Bucket, scope.Prefix, request.Namespace, request.StatefulSet, request.LogicalID); err == nil {
		t.Fatal("accepted terminal for a replacement Pod UID")
	}
}

func TestRecoveryLineageRequiresSameStatefulSetAndFreshIncarnations(t *testing.T) {
	previous := operator.Resource{Status: operator.Status{Phase: "Complete", Stage: "Complete", StatefulSetUID: "same-sts", Target: "generation-2", TargetIdentities: []operator.FenceTarget{{NodeID: "rhiza-0", Pod: "rhiza-0", PodUID: "old-pod", WALIdentity: "old-wal"}}}}
	next := operator.Resource{Spec: operator.Spec{SourceClusterID: "generation-2"}, Status: operator.Status{Phase: "Complete", Stage: "Complete", StatefulSetUID: "same-sts", Source: "generation-2", Target: "generation-3"}}
	previousFence := operator.FenceRequest{OperationID: "fence-2"}
	nextFence := operator.FenceRequest{OperationID: "fence-3", SourceClusterID: "generation-2", Targets: []operator.FenceTarget{{NodeID: "rhiza-0", Pod: "rhiza-0", PodUID: "new-pod", WALIdentity: "new-wal"}}}
	if err := verifyRecoveryLineage(previous, next, previousFence, nextFence); err != nil {
		t.Fatalf("valid same-StatefulSet lineage rejected: %v", err)
	}
	nextFence.Targets[0].PodUID = "old-pod"
	if err := verifyRecoveryLineage(previous, next, previousFence, nextFence); err == nil {
		t.Fatal("accepted a previous-generation Pod UID in the next fence")
	}
	nextFence.Targets[0].PodUID = "new-pod"
	next.Status.StatefulSetUID = "replacement-sts"
	if err := verifyRecoveryLineage(previous, next, previousFence, nextFence); err == nil {
		t.Fatal("rejected same workload lineage requirement was not enforced")
	}
}

func TestAsyncZeroRecoveryRequiresExplicitLossPolicyAndExactFenceProof(t *testing.T) {
	request := operator.FenceRequest{OperationID: "fence-op", SourceClusterID: "source", StatefulSetUID: "source-sts", BindingUID: "binding"}
	proof := operator.FenceProof{OperationID: request.OperationID, SourceClusterID: request.SourceClusterID, BindingUID: request.BindingUID, ProofID: "exact-proof", IndependentV2: &operator.IndependentEvidenceV2{StorageScope: operator.AttestationStoreScope{Provider: "gcs"}}}
	resource := operator.Resource{Spec: operator.Spec{SourceClusterID: request.SourceClusterID, RecoveryID: request.OperationID, Durability: "async", AllowDataLoss: true, Fence: operator.Fence{RecoveryID: request.OperationID, ClusterID: request.SourceClusterID, StatefulSetUID: request.StatefulSetUID, Confirmed: true, Evidence: proof.ProofID}}, Status: operator.Status{Phase: "Complete", Stage: "Complete", TargetBaseKind: "empty", SourceDurability: "async"}}
	if err := verifyAsyncEmptyRecovery(resource, request, proof); err != nil {
		t.Fatalf("valid explicit async zero recovery rejected: %v", err)
	}
	resource.Spec.AllowDataLoss = false
	if err := verifyAsyncEmptyRecovery(resource, request, proof); err == nil {
		t.Fatal("accepted async zero recovery without explicit allowDataLoss")
	}
	resource.Spec.AllowDataLoss = true
	for name, mutate := range map[string]func(*operator.Resource, *operator.FenceProof){
		"wrong proof ID":          func(_ *operator.Resource, p *operator.FenceProof) { p.ProofID = "other-proof" },
		"wrong recovery fence ID": func(r *operator.Resource, _ *operator.FenceProof) { r.Spec.Fence.RecoveryID = "other-operation" },
		"unconfirmed fence":       func(r *operator.Resource, _ *operator.FenceProof) { r.Spec.Fence.Confirmed = false },
	} {
		t.Run(name, func(t *testing.T) {
			badResource, badProof := resource, proof
			mutate(&badResource, &badProof)
			if err := verifyAsyncEmptyRecovery(badResource, request, badProof); err == nil {
				t.Fatal("accepted recovery not linked to the exact captured proof")
			}
		})
	}
}

func TestImageIDMatchesPinnedDigest(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	pinned := "registry.example/rhiza/operator@" + digest
	for _, imageID := range []string{
		"containerd://" + digest,
		"docker-pullable://registry.example/rhiza/operator@" + digest,
	} {
		if !imageIDMatchesPinned(imageID, pinned) {
			t.Fatalf("rejected matching deployed image ID %q", imageID)
		}
	}
	for _, imageID := range []string{
		"",
		"containerd://sha256:" + strings.Repeat("b", 64),
		"registry.example/rhiza/operator:latest",
	} {
		if imageIDMatchesPinned(imageID, pinned) {
			t.Fatalf("accepted missing or mismatched deployed image digest %q", imageID)
		}
	}
	if imageIDMatchesPinned("containerd://"+digest, "registry.example/rhiza/operator:latest") {
		t.Fatal("accepted a mutable pinned image reference")
	}
}

func TestGCSControlPodSnapshotRejectsReplacementAndOwnerChange(t *testing.T) {
	captured := gcsControlPodIdentity{Name: "rhiza-runtime-agent-node-a", UID: "pod-uid-a", OwnerUID: "daemonset-uid-a"}
	if !gcsControlPodSnapshotMatches(captured, captured.Name, captured.UID, captured.OwnerUID, "") {
		t.Fatal("rejected unchanged captured DaemonSet Pod identity")
	}
	if gcsControlPodSnapshotMatches(captured, captured.Name, "replacement-pod-uid", captured.OwnerUID, "") {
		t.Fatal("accepted replacement Pod UID under the captured name")
	}
	if gcsControlPodSnapshotMatches(captured, captured.Name, captured.UID, "replacement-daemonset-uid", "") {
		t.Fatal("accepted a different owning DaemonSet UID")
	}
	operator := gcsControlPodIdentity{Name: "rhiza-operator-abc", UID: "operator-pod-uid", OwnerUID: "replicaset-uid", ParentOwnerUID: "deployment-uid"}
	if gcsControlPodSnapshotMatches(operator, operator.Name, operator.UID, operator.OwnerUID, "replacement-deployment-uid") {
		t.Fatal("accepted a different owning operator Deployment UID")
	}
}
