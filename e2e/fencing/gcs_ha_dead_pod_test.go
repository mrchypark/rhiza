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
