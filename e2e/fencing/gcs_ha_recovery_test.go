package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/thanos-io/objstore"
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

func TestGCSAPIServerValidationRejectionRequiresHTTP422Invalid(t *testing.T) {
	valid := []byte(`{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"Invalid","code":422,"message":"RhizaFence.status.proof is invalid","details":{"causes":[{"reason":"FieldValueInvalid","field":"status.proof.independentV2.terminals[0].producerUID","message":"must be non-empty"}]}}`)
	if diagnostic, ok := gcsAPIServerValidationRejection(http.StatusUnprocessableEntity, valid); !ok || !strings.Contains(diagnostic, "producerUID") {
		t.Fatalf("did not retain bounded API validation detail: ok=%t diagnostic=%q", ok, diagnostic)
	}
	for _, tc := range []struct {
		name string
		code int
		body []byte
	}{
		{name: "forbidden", code: http.StatusForbidden, body: []byte(`{"reason":"Forbidden","code":403}`)},
		{name: "conflict", code: http.StatusConflict, body: []byte(`{"reason":"Conflict","code":409}`)},
		{name: "server error", code: http.StatusInternalServerError, body: []byte(`{"reason":"InternalError","code":500}`)},
		{name: "422 non-validation reason", code: http.StatusUnprocessableEntity, body: []byte(`{"reason":"Failure","code":422}`)},
		{name: "mismatched status code", code: http.StatusUnprocessableEntity, body: []byte(`{"reason":"Invalid","code":403}`)},
		{name: "malformed response", code: http.StatusUnprocessableEntity, body: []byte(`not-json`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if diagnostic, ok := gcsAPIServerValidationRejection(tc.code, tc.body); ok {
				t.Fatalf("treated non-validation failure as schema rejection: %q", diagnostic)
			}
		})
	}
}

func TestExplicitRecoveryProfileRequiresAutomaticModeOffAndNoAutomaticLoss(t *testing.T) {
	for _, tc := range []struct {
		name      string
		spec      obj
		automatic string
		want      bool
	}{
		{name: "explicit no policy", spec: obj{"automatic": false}, automatic: "false", want: true},
		{name: "explicit safe legacy policy", spec: obj{"automatic": false, "policy": obj{"allowDataLoss": false}}, automatic: "false", want: true},
		{name: "cluster automatic", spec: obj{"automatic": true}, automatic: "false"},
		{name: "operator automatic", spec: obj{"automatic": false}, automatic: "true"},
		{name: "automatic loss policy", spec: obj{"automatic": false, "policy": obj{"allowDataLoss": true}}, automatic: "false"},
		{name: "missing cluster mode", spec: obj{}, automatic: "false"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := explicitGCSRecoveryProfileSafe(tc.spec, tc.automatic); got != tc.want {
				t.Fatalf("explicit profile validity = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestExplicitRecoveryMutationsRequireFreshRunOwnedResources(t *testing.T) {
	previousRunID := qualificationRunID
	t.Cleanup(func() { qualificationRunID = previousRunID })
	created, err := time.Parse(time.RFC3339Nano, "2026-10-23T00:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	qualificationRunID = "deadbeef"
	valid := obj{"metadata": obj{
		"namespace": "run-deadbeef", "uid": "resource-uid", "creationTimestamp": "2026-10-23T00:00:01Z",
		"labels": obj{"chaos.rhiza.io/run": "deadbeef"},
	}}
	if err := requireRunOwnedResource(valid, "fixture", "run-deadbeef", created); err != nil {
		t.Fatalf("rejected fresh run-owned resource: %v", err)
	}
	for name, mutate := range map[string]func(obj){
		"wrong namespace": func(resource obj) { resource["metadata"].(obj)["namespace"] = "other" },
		"missing UID":     func(resource obj) { resource["metadata"].(obj)["uid"] = "" },
		"preexisting": func(resource obj) {
			resource["metadata"].(obj)["creationTimestamp"] = "2026-10-22T23:59:59Z"
		},
		"foreign run label": func(resource obj) {
			resource["metadata"].(obj)["labels"].(obj)["chaos.rhiza.io/run"] = "feedface"
		},
	} {
		t.Run(name, func(t *testing.T) {
			resource := obj{"metadata": obj{
				"namespace": "run-deadbeef", "uid": "resource-uid", "creationTimestamp": "2026-10-23T00:00:01Z",
				"labels": obj{"chaos.rhiza.io/run": "deadbeef"},
			}}
			mutate(resource)
			if err := requireRunOwnedResource(resource, "fixture", "run-deadbeef", created); err == nil {
				t.Fatal("accepted a resource not proven to belong to this fresh run")
			}
		})
	}
}

func TestExplicitRecoveryIdentityRejectsUntrustedRunIDs(t *testing.T) {
	for _, runID := range []string{"", "deadbeef-extra", "DEADBEEF", "deadbeeg", "deadbee"} {
		if _, _, err := explicitRecoveryIdentity(runID, "first"); err == nil {
			t.Fatalf("accepted malformed run ID %q", runID)
		}
	}
	if _, _, err := explicitRecoveryIdentity("deadbeef", "unexpected"); err == nil {
		t.Fatal("accepted an unknown explicit recovery phase")
	}
}

func TestExplicitRecoveryHasNoSuccessorReservationUntilFenceProof(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	if err := verifyNoGCSReservationAt(ctx, bucket, "rhiza/run-prefix"); err != nil {
		t.Fatalf("accepted fresh source with no successor reservation: %v", err)
	}
	if err := bucket.Upload(ctx, "rhiza/run-prefix/recovery/successor.json", strings.NewReader(`{"target":"successor"}`)); err != nil {
		t.Fatal(err)
	}
	if err := verifyNoGCSReservationAt(ctx, bucket, "rhiza/run-prefix"); err == nil {
		t.Fatal("accepted a successor reservation before the external fence proof")
	}
}

func TestExplicitRecoveryManifestStartsUnconfirmedAndBindsCapturedFence(t *testing.T) {
	source := gcsRecoverySource{
		Namespace: "run-deadbeef", LogicalID: "logical", StatefulSet: "rhiza-voter", Container: "rhiza",
		ClusterID: "source-cluster", StatefulSetUID: "sts-uid", BindingUID: "binding-uid",
		Targets: []operator.FenceTarget{{NodeID: "rhiza-voter-0", Pod: "rhiza-voter-0", PodUID: "pod-0", WALIdentity: "wal-0"}},
	}
	manifest, name, recoveryID, err := explicitRecoveryManifest(source, "deadbeef", "first", "before-ack", false)
	if err != nil {
		t.Fatal(err)
	}
	if name != "rhiza-manual-deadbeef-first" || recoveryID != "manual-deadbeef-first" ||
		text(nested(manifest, "metadata", "labels", "chaos.rhiza.io/run")) != "deadbeef" ||
		nested(manifest, "spec", "allowDataLoss") != false || nested(manifest, "spec", "fence", "confirmed") != false ||
		nested(manifest, "spec", "fence", "recoveryID") != recoveryID || nested(manifest, "spec", "fence", "clusterID") != source.ClusterID ||
		nested(manifest, "spec", "fence", "statefulSetUID") != source.StatefulSetUID {
		t.Fatalf("explicit resource is not an unconfirmed, run-owned request: %v", manifest)
	}
	request := explicitGenerationFenceRequest(source, recoveryID)
	if err := validateExplicitGenerationFenceRequest(request, source, recoveryID); err != nil {
		t.Fatalf("fencer request is not bound to the captured source incarnation: %v", err)
	}
	for name, mutate := range map[string]func(*operator.FenceRequest){
		"different operation": func(request *operator.FenceRequest) { request.OperationID = "other-operation" },
		"different binding":   func(request *operator.FenceRequest) { request.BindingUID = "other-binding" },
		"different generation": func(request *operator.FenceRequest) {
			request.SourceClusterID = "other-generation"
		},
		"different StatefulSet UID": func(request *operator.FenceRequest) { request.StatefulSetUID = "other-sts" },
		"different cohort": func(request *operator.FenceRequest) {
			request.Targets = []operator.FenceTarget{{NodeID: "other", Pod: "other", PodUID: "other-pod", WALIdentity: "other-wal"}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			wrong := request
			wrong.Targets = append([]operator.FenceTarget(nil), request.Targets...)
			mutate(&wrong)
			if err := validateExplicitGenerationFenceRequest(wrong, source, recoveryID); err == nil {
				t.Fatal("accepted a fence not bound to the captured source request")
			}
		})
	}
	asyncManifest, _, _, err := explicitRecoveryManifest(source, "deadbeef", "async-empty", "async", true)
	if err != nil || nested(asyncManifest, "spec", "allowDataLoss") != true {
		t.Fatalf("explicit async loss must be declared on the recovery resource: %v", err)
	}
	if _, _, _, err := explicitRecoveryManifest(source, "deadbeef", "async-empty", "async", false); err == nil {
		t.Fatal("accepted explicit async recovery without resource-scoped allowDataLoss")
	}
}

func TestExplicitRecoveryConfirmationBindsUIDSourcePolicyAndResourceVersion(t *testing.T) {
	source := gcsRecoverySource{Namespace: "run-deadbeef", StatefulSet: "rhiza-voter", Container: "rhiza",
		ClusterID: "source-cluster", StatefulSetUID: "sts-uid"}
	manifest, name, recoveryID, err := explicitRecoveryManifest(source, "deadbeef", "async-empty", "async", true)
	if err != nil {
		t.Fatal(err)
	}
	recovery := explicitGCSRecoveryRef{Name: name, UID: "recovery-uid", Namespace: source.Namespace, RunLabel: "deadbeef",
		OperationID: recoveryID, SourceClusterID: source.ClusterID, StatefulSet: source.StatefulSet, Container: source.Container,
		Durability: "async", AllowDataLoss: true}
	base := obj{
		"metadata": obj{"name": name, "namespace": source.Namespace, "uid": recovery.UID, "resourceVersion": "17", "labels": obj{"chaos.rhiza.io/run": "deadbeef"}},
		"spec":     nested(manifest, "spec"), "status": obj{"phase": "AwaitingFence"},
	}
	if err := validateExplicitRecoveryResource(base, source, recovery); err != nil {
		t.Fatalf("rejected captured explicit recovery: %v", err)
	}
	if err := validateExplicitRecoveryResource([]any{"malformed response"}, source, recovery); err == nil {
		t.Fatal("accepted a non-object recovery API response")
	}
	for name, mutate := range map[string]func(obj){
		"replacement UID": func(item obj) { nested(item, "metadata").(obj)["uid"] = "replacement-uid" },
		"changed source":  func(item obj) { nested(item, "spec").(obj)["sourceClusterID"] = "other-source" },
		"changed policy":  func(item obj) { nested(item, "spec").(obj)["allowDataLoss"] = false },
		"changed target durability": func(item obj) {
			nested(item, "spec").(obj)["durability"] = "before-ack"
		},
	} {
		t.Run(name, func(t *testing.T) {
			item := cloneExplicitRecoveryResource(t, base)
			mutate(item)
			if err := validateExplicitRecoveryResource(item, source, recovery); err == nil {
				t.Fatal("accepted a changed explicit-recovery identity or policy")
			}
		})
	}
	patch, err := explicitRecoveryConfirmationPatch(base, recovery, "proof-id")
	if err != nil {
		t.Fatal(err)
	}
	versionTestFound := false
	for _, operation := range patch {
		if operation["op"] == "test" && operation["path"] == "/metadata/resourceVersion" && operation["value"] == "17" {
			versionTestFound = true
		}
	}
	if !versionTestFound {
		t.Fatal("confirmation patch did not atomically test the exact resourceVersion read with the recovery")
	}
	base["metadata"].(obj)["resourceVersion"] = "18"
	newPatch, err := explicitRecoveryConfirmationPatch(base, recovery, "proof-id")
	if err != nil {
		t.Fatal(err)
	}
	if newPatch[1]["value"] != "18" {
		t.Fatalf("fresh confirmation patch did not bind its current resourceVersion: %v", newPatch[1])
	}
	delete(base["metadata"].(obj), "resourceVersion")
	if _, err := explicitRecoveryConfirmationPatch(base, recovery, "proof-id"); err == nil {
		t.Fatal("built a confirmation patch without a resourceVersion precondition")
	}
}

func cloneExplicitRecoveryResource(t *testing.T, resource obj) obj {
	t.Helper()
	var copy obj
	if err := json.Unmarshal(encoded(resource), &copy); err != nil {
		t.Fatal(err)
	}
	return copy
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
	item["metadata"] = obj{"name": "replacement-resource-name", "uid": pending.UID}
	if _, err := exactPendingGCSFenceRequest(item, pending); err == nil {
		t.Fatal("accepted a renamed fence resource instead of the captured API name")
	}
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
		Terminals: []fencing.TerminalReference{
			{EvidenceVersion: 2, Request: fencing.StopRequest{OperationID: "source-stop", RequestHash: strings.Repeat("b", 64), Identity: fencing.Identity{SourceClusterID: request.SourceClusterID, NodeID: target.NodeID, PodUID: target.PodUID, RunID: "run-1", WALIdentity: target.WALIdentity}}, RuntimeHash: strings.Repeat("c", 64), ProducerUID: "producer-uid", ProofName: "proof-name", ProofUID: "proof-uid", ProofDigest: strings.Repeat("d", 64), RuntimeReceiptDigest: strings.Repeat("e", 64), MutationReceiptDigest: strings.Repeat("f", 64), StorageScopeDigest: scopeDigest},
			{EvidenceVersion: 3, Request: fencing.StopRequest{OperationID: "pending-stop", RequestHash: strings.Repeat("1", 64), Identity: fencing.Identity{SourceClusterID: request.SourceClusterID, NodeID: "learner-1", PodName: "rhiza-1", PodUID: "learner-pod-uid", RunID: "learner-run", AssignmentID: strings.Repeat("2", 64)}}, RuntimeHash: strings.Repeat("3", 64), ProofName: "pending-proof", ProofUID: "pending-proof-uid", ProofDigest: strings.Repeat("4", 64), RuntimeReceiptDigest: strings.Repeat("5", 64), StorageScopeDigest: scopeDigest},
		},
	}
	proof := operator.FenceProof{OperationID: request.OperationID, SourceClusterID: request.SourceClusterID, BindingUID: request.BindingUID, RequestHash: evidence.RequestHash,
		ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true, IndependentV2: evidence}
	proof.ProofID = "independent-v2-" + fmt.Sprintf("%x", sha256.Sum256(encoded(evidence)))
	item := obj{"spec": request, "status": obj{"proof": proof}}
	if _, _, err := verifyIndependentGenerationProof(item, request.SourceClusterID, request.StatefulSetUID, request.BindingUID, []operator.FenceTarget{target}, scope.Bucket, scope.Prefix, request.Namespace, request.StatefulSet, request.LogicalID); err != nil {
		t.Fatalf("valid mixed V2/v3 proof rejected: %v", err)
	}
	apiItem := obj{"metadata": obj{"name": "fence", "uid": "fence-uid"}, "spec": request, "status": obj{"proof": proof}}
	pending := pendingGCSFence{Name: "fence", UID: "fence-uid", Request: request}
	source := gcsRecoverySource{Namespace: request.Namespace, LogicalID: request.LogicalID, StatefulSet: request.StatefulSet,
		ClusterID: request.SourceClusterID, StatefulSetUID: request.StatefulSetUID, BindingUID: request.BindingUID,
		Bucket: scope.Bucket, Prefix: scope.Prefix, Targets: []operator.FenceTarget{target}}
	if err := verifyGCSFenceProofReadback(apiItem, apiItem, pending, source); err != nil {
		t.Fatalf("unchanged API status readback rejected: %v", err)
	}
	changedReadback := obj{}
	if err := json.Unmarshal(encoded(apiItem), &changedReadback); err != nil {
		t.Fatal(err)
	}
	nested(changedReadback, "status", "proof", "independentV2", "terminals").([]any)[0].(map[string]any)["proofDigest"] = strings.Repeat("9", 64)
	if err := verifyGCSFenceProofReadback(apiItem, changedReadback, pending, source); err == nil {
		t.Fatal("accepted an API status readback with a changed immutable terminal proof digest")
	}

	v3Only := *evidence
	v3Only.Terminals = append([]fencing.TerminalReference(nil), evidence.Terminals...)
	v3Only.Terminals[0].EvidenceVersion = 3
	v3Only.Terminals[0].ProducerUID = ""
	v3Only.Terminals[0].MutationReceiptDigest = ""
	v3OnlyProof := proof
	v3OnlyProof.IndependentV2 = &v3Only
	v3OnlyProof.ProofID = "independent-v2-" + fmt.Sprintf("%x", sha256.Sum256(encoded(&v3Only)))
	if _, _, err := verifyIndependentGenerationProof(obj{"spec": request, "status": obj{"proof": v3OnlyProof}}, request.SourceClusterID, request.StatefulSetUID, request.BindingUID, []operator.FenceTarget{target}, scope.Bucket, scope.Prefix, request.Namespace, request.StatefulSet, request.LogicalID); err == nil {
		t.Fatal("accepted a V3 pending learner reference as coverage for a captured active voter")
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

func TestIndependentGenerationProofMatchesCompleteCanonicalTerminalSet(t *testing.T) {
	target := operator.FenceTarget{NodeID: "rhiza-0", Pod: "rhiza-0", PodUID: "source-pod-uid", WALIdentity: "source-wal"}
	request := operator.FenceRequest{LogicalID: "fencing-e2e", BindingUID: "binding-uid", Namespace: namespace, StatefulSet: "rhiza", StatefulSetUID: "sts-uid", SourceClusterID: "source-generation", OperationID: "fence-op", Scope: operator.ScopeGeneration, Targets: []operator.FenceTarget{target}}
	scope := operator.AttestationStoreScope{Provider: "gcs", Bucket: "qualification-bucket", Prefix: "rhiza/source-generation"}
	scopeDigest := fmt.Sprintf("%x", sha256.Sum256(encoded(scope)))
	requestHash := fmt.Sprintf("%x", sha256.Sum256(encoded(request)))
	terminals := []fencing.TerminalReference{
		{EvidenceVersion: 2, Request: fencing.StopRequest{OperationID: "source-stop", RequestHash: strings.Repeat("b", 64), Identity: fencing.Identity{SourceClusterID: request.SourceClusterID, NodeID: target.NodeID, PodUID: target.PodUID, RunID: "run-1", WALIdentity: target.WALIdentity}}, RuntimeHash: strings.Repeat("c", 64), ProducerUID: "producer-uid", ProofName: "proof-name", ProofUID: "proof-uid", ProofDigest: strings.Repeat("d", 64), RuntimeReceiptDigest: strings.Repeat("e", 64), MutationReceiptDigest: strings.Repeat("f", 64), StorageScopeDigest: scopeDigest},
		{EvidenceVersion: 3, Request: fencing.StopRequest{OperationID: "pending-stop", RequestHash: strings.Repeat("1", 64), Identity: fencing.Identity{SourceClusterID: request.SourceClusterID, NodeID: "learner-1", PodName: "rhiza-1", PodUID: "learner-pod-uid", RunID: "learner-run", AssignmentID: strings.Repeat("2", 64)}}, RuntimeHash: strings.Repeat("3", 64), ProofName: "pending-proof", ProofUID: "pending-proof-uid", ProofDigest: strings.Repeat("4", 64), RuntimeReceiptDigest: strings.Repeat("5", 64), StorageScopeDigest: scopeDigest},
	}
	root := fenceLedgerState{StatefulSetUID: request.StatefulSetUID, Ledger: fencing.Ledger{SourceClusterID: request.SourceClusterID, ClosedBy: request.OperationID, ClosedHash: requestHash, RequireRuntimeBinding: true, RequireMutationJournal: true, Runs: map[string]fencing.Run{}}}
	for i := range terminals {
		ref := &terminals[i]
		id := ref.Request.Identity
		runtime := fencing.RuntimeBinding{AuthorityID: "authority", NamespaceUID: "namespace-uid", BindingUID: request.BindingUID, StatefulSetUID: request.StatefulSetUID,
			SourceClusterID: request.SourceClusterID, NodeID: id.NodeID, PodName: id.PodName, PodUID: id.PodUID, RunID: id.RunID, AssignmentID: id.AssignmentID,
			NodeName: "node", NodeUID: "node-uid", MachineID: "machine", BootID: "boot", RuntimeID: "runtime", SandboxID: "sandbox", ContainerID: "container", ImageID: "image"}
		ref.RuntimeHash = fencing.RuntimeBindingDigest(runtime)
		run := fencing.Run{Identity: id, Role: fencing.StartupRoleVoterActive, Stop: &ref.Request, Runtime: &runtime, IndependentTerminal: ref}
		if ref.EvidenceVersion == 3 {
			run.Role = fencing.StartupRoleLearnerPendingRemove
		} else {
			run.MutationProducerUID = ref.ProducerUID
		}
		root.Ledger.Runs[id.RunID] = run
	}
	ledgerHash := fmt.Sprintf("%x", sha256.Sum256(encoded([]any{request.Namespace, root})))
	evidence := &operator.IndependentEvidenceV2{Version: 2, RequestHash: requestHash, LedgerHash: ledgerHash, StorageScope: scope, Terminals: terminals}
	proof := operator.FenceProof{OperationID: request.OperationID, SourceClusterID: request.SourceClusterID, BindingUID: request.BindingUID, RequestHash: requestHash, ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true, IndependentV2: evidence}
	proof.ProofID = "independent-v2-" + fmt.Sprintf("%x", sha256.Sum256(encoded(evidence)))
	item := obj{"spec": request, "status": obj{"proof": proof}}
	verify := func(candidateItem any, candidate fenceLedgerState) error {
		_, _, err := verifyIndependentGenerationProofAgainstRoot(candidateItem, request.SourceClusterID, request.StatefulSetUID, request.BindingUID,
			[]operator.FenceTarget{target}, scope.Bucket, scope.Prefix, candidate, request.Namespace, request.StatefulSet, request.LogicalID)
		return err
	}
	if err := verify(item, root); err != nil {
		t.Fatalf("complete canonical V2/V3 terminal set rejected: %v", err)
	}
	cloneRoot := func(source fenceLedgerState) fenceLedgerState {
		cloned := source
		cloned.Ledger.Runs = make(map[string]fencing.Run, len(source.Ledger.Runs))
		for runID, run := range source.Ledger.Runs {
			cloned.Ledger.Runs[runID] = run
		}
		return cloned
	}
	lateDrain := cloneRoot(root)
	voterRun := lateDrain.Ledger.Runs["run-1"]
	voterStop := *voterRun.Stop
	if err := lateDrain.Ledger.RecordDrain(fencing.DrainReceipt{Request: voterStop, WritersJoined: true}); err != nil {
		t.Fatalf("record lawful late drain: %v", err)
	}
	if err := lateDrain.Ledger.RecordExit(voterStop); err != nil {
		t.Fatalf("record lawful late exit: %v", err)
	}
	lateDrainHash := fmt.Sprintf("%x", sha256.Sum256(encoded([]any{request.Namespace, lateDrain})))
	if lateDrainHash == evidence.LedgerHash {
		t.Fatal("late drain/exit did not change the current root metadata hash")
	}
	if err := verify(item, lateDrain); err != nil {
		t.Fatalf("unchanged issued proof rejected after lawful late drain/exit: %v", err)
	}
	lateNeverBoundExit := cloneRoot(root)
	learnerRun := lateNeverBoundExit.Ledger.Runs["learner-run"]
	if err := lateNeverBoundExit.Ledger.RecordNeverBoundExit(*learnerRun.Stop); err != nil {
		t.Fatalf("record lawful late never-bound exit: %v", err)
	}
	lateNeverBoundHash := fmt.Sprintf("%x", sha256.Sum256(encoded([]any{request.Namespace, lateNeverBoundExit})))
	if lateNeverBoundHash == evidence.LedgerHash {
		t.Fatal("late never-bound exit did not change the current root metadata hash")
	}
	if err := verify(item, lateNeverBoundExit); err != nil {
		t.Fatalf("unchanged issued proof rejected after lawful late never-bound exit: %v", err)
	}

	missingPending := root
	missingPending.Ledger.Runs = map[string]fencing.Run{"run-1": root.Ledger.Runs["run-1"]}
	itemForRoot := func(candidate fenceLedgerState, terminals []fencing.TerminalReference) any {
		candidateEvidence := *evidence
		candidateEvidence.Terminals = append([]fencing.TerminalReference(nil), terminals...)
		candidateEvidence.LedgerHash = fmt.Sprintf("%x", sha256.Sum256(encoded([]any{request.Namespace, candidate})))
		candidateProof := proof
		candidateProof.IndependentV2 = &candidateEvidence
		candidateProof.ProofID = "independent-v2-" + fmt.Sprintf("%x", sha256.Sum256(encoded(&candidateEvidence)))
		return obj{"spec": request, "status": obj{"proof": candidateProof}}
	}
	if err := verify(itemForRoot(missingPending, terminals), missingPending); err == nil {
		t.Fatal("accepted proof whose V3 pending terminal is absent from canonical root")
	}

	changedRoot := root
	changedRoot.Ledger.Runs = map[string]fencing.Run{}
	for runID, run := range root.Ledger.Runs {
		changedRoot.Ledger.Runs[runID] = run
	}
	changedRun := changedRoot.Ledger.Runs["run-1"]
	changedRef := *changedRun.IndependentTerminal
	changedRef.ProofDigest = strings.Repeat("9", 64)
	changedRun.IndependentTerminal = &changedRef
	changedRoot.Ledger.Runs["run-1"] = changedRun
	if err := verify(itemForRoot(changedRoot, terminals), changedRoot); err == nil {
		t.Fatal("accepted a terminal whose canonical immutable proof digest differs")
	}

	extraRoot := root
	extraRoot.Ledger.Runs = map[string]fencing.Run{}
	for runID, run := range root.Ledger.Runs {
		extraRoot.Ledger.Runs[runID] = run
	}
	extraRun := root.Ledger.Runs["run-1"]
	extraRef := *extraRun.IndependentTerminal
	extraRuntime := *extraRun.Runtime
	extraRef.Request.Identity.NodeID = "rhiza-extra"
	extraRef.Request.Identity.PodUID = "extra-pod-uid"
	extraRef.Request.Identity.RunID = "extra-run"
	extraRef.Request.Identity.WALIdentity = "extra-wal"
	extraRef.RuntimeHash = ""
	extraRuntime.NodeID = extraRef.Request.Identity.NodeID
	extraRuntime.PodUID = extraRef.Request.Identity.PodUID
	extraRuntime.RunID = extraRef.Request.Identity.RunID
	extraRef.RuntimeHash = fencing.RuntimeBindingDigest(extraRuntime)
	extraRun.Identity = extraRef.Request.Identity
	extraRun.Stop = &extraRef.Request
	extraRun.Runtime = &extraRuntime
	extraRun.IndependentTerminal = &extraRef
	extraRoot.Ledger.Runs["extra-run"] = extraRun
	if err := verify(itemForRoot(extraRoot, terminals), extraRoot); err == nil {
		t.Fatal("accepted a canonical terminal omitted from the proof reference set")
	}

	duplicateTerminals := append(append([]fencing.TerminalReference(nil), terminals...), terminals[0])
	if err := verify(itemForRoot(root, duplicateTerminals), root); err == nil {
		t.Fatal("accepted duplicate proof terminal references")
	}
}

func TestCleanupUncertainSchemaNamespaceCreate(t *testing.T) {
	const (
		runID = "deadbeef"
		name  = "rhiza-fence-schema-deadbeef"
		token = "private-create-marker"
		uid   = "created-namespace-uid"
	)
	createErr := errors.New("kubectl create command failed: timeout")
	owned := obj{"metadata": obj{"name": name, "uid": uid,
		"labels": obj{"chaos.rhiza.io/run": runID, schemaNamespaceCreateTokenLabel: token}}}

	t.Run("commit then command error cleans exact UID", func(t *testing.T) {
		readCalls, deleteCalls := 0, 0
		gotUID := ""
		err := cleanupUncertainSchemaNamespaceCreate(context.Background(), name, runID, token, createErr,
			func(_ context.Context, gotName string) (obj, error) {
				readCalls++
				if gotName != name {
					t.Fatalf("readback name = %q, want exact %q", gotName, name)
				}
				return owned, nil
			},
			func(_ context.Context, gotName, gotRunID, targetUID string) error {
				deleteCalls++
				if gotName != name || gotRunID != runID {
					t.Fatalf("cleanup identity = %q/%q, want %q/%q", gotName, gotRunID, name, runID)
				}
				gotUID = targetUID
				return nil
			})
		if !errors.Is(err, createErr) || readCalls != 1 || deleteCalls != 1 || gotUID != uid {
			t.Fatalf("uncertain create cleanup err=%v reads=%d deletes=%d uid=%q", err, readCalls, deleteCalls, gotUID)
		}
	})

	t.Run("committed create cleans with canceled operation context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		readCalls, deleteCalls := 0, 0
		gotUID := ""
		err := cleanupUncertainSchemaNamespaceCreate(ctx, name, runID, token, createErr,
			func(cleanupCtx context.Context, gotName string) (obj, error) {
				readCalls++
				if gotName != name {
					t.Fatalf("readback name = %q, want exact %q", gotName, name)
				}
				if cleanupCtx.Err() != nil {
					return nil, cleanupCtx.Err()
				}
				if _, ok := cleanupCtx.Deadline(); !ok {
					t.Fatal("readback context has no bounded cleanup deadline")
				}
				return owned, nil
			},
			func(cleanupCtx context.Context, gotName, gotRunID, targetUID string) error {
				deleteCalls++
				if cleanupCtx.Err() != nil {
					return cleanupCtx.Err()
				}
				if gotName != name || gotRunID != runID {
					t.Fatalf("cleanup identity = %q/%q, want %q/%q", gotName, gotRunID, name, runID)
				}
				gotUID = targetUID
				return nil
			})
		if !errors.Is(err, createErr) || readCalls != 1 || deleteCalls != 1 || gotUID != uid {
			t.Fatalf("canceled operation context blocked uncertain-create cleanup: err=%v reads=%d deletes=%d uid=%q", err, readCalls, deleteCalls, gotUID)
		}
	})

	t.Run("AlreadyExists is never adopted or deleted", func(t *testing.T) {
		readCalls, deleteCalls := 0, 0
		alreadyExists := errors.New("kubectl create: Error from server (AlreadyExists): namespaces already exists")
		err := cleanupUncertainSchemaNamespaceCreate(context.Background(), name, runID, token, alreadyExists,
			func(context.Context, string) (obj, error) { readCalls++; return owned, nil },
			func(context.Context, string, string, string) error { deleteCalls++; return nil })
		if !errors.Is(err, alreadyExists) || readCalls != 0 || deleteCalls != 0 {
			t.Fatalf("existing namespace was inspected/adopted: err=%v reads=%d deletes=%d", err, readCalls, deleteCalls)
		}
	})

	t.Run("ownership mismatch is left untouched", func(t *testing.T) {
		readCalls, deleteCalls := 0, 0
		mismatch := obj{"metadata": obj{"name": name, "uid": uid,
			"labels": obj{"chaos.rhiza.io/run": runID, schemaNamespaceCreateTokenLabel: "different-marker"}}}
		err := cleanupUncertainSchemaNamespaceCreate(context.Background(), name, runID, token, createErr,
			func(context.Context, string) (obj, error) { readCalls++; return mismatch, nil },
			func(context.Context, string, string, string) error { deleteCalls++; return nil })
		if !errors.Is(err, createErr) || !strings.Contains(err.Error(), "may remain") || readCalls != 1 || deleteCalls != 0 {
			t.Fatalf("ownership mismatch cleanup result err=%v reads=%d deletes=%d", err, readCalls, deleteCalls)
		}
	})

	t.Run("unreadable readback is not reported clean", func(t *testing.T) {
		readCalls, deleteCalls := 0, 0
		readErr := errors.New("API unavailable")
		err := cleanupUncertainSchemaNamespaceCreate(context.Background(), name, runID, token, createErr,
			func(context.Context, string) (obj, error) { readCalls++; return nil, readErr },
			func(context.Context, string, string, string) error { deleteCalls++; return nil })
		if !errors.Is(err, createErr) || !errors.Is(err, readErr) || !strings.Contains(err.Error(), "may remain") || readCalls != 1 || deleteCalls != 0 {
			t.Fatalf("unreadable readback cleanup result err=%v reads=%d deletes=%d", err, readCalls, deleteCalls)
		}
	})
}

func TestGCSGenerationTerminalReferenceKeepsV2StrictAndV3ProducerFree(t *testing.T) {
	scopeDigest := strings.Repeat("a", 64)
	base := fencing.TerminalReference{
		EvidenceVersion: 3,
		Request: fencing.StopRequest{OperationID: "stop", RequestHash: strings.Repeat("b", 64), Identity: fencing.Identity{
			SourceClusterID: "source", NodeID: "learner-0", PodName: "rhiza-0", PodUID: "pod-uid", RunID: "learner-run", AssignmentID: strings.Repeat("c", 64),
		}},
		RuntimeHash: strings.Repeat("d", 64), ProofName: "proof", ProofUID: "proof-uid", ProofDigest: strings.Repeat("e", 64),
		RuntimeReceiptDigest: strings.Repeat("f", 64), StorageScopeDigest: scopeDigest,
	}
	if err := validateGCSGenerationTerminalReference(base, "source", scopeDigest); err != nil {
		t.Fatalf("valid closed-pending V3 reference rejected: %v", err)
	}
	v2 := base
	v2.EvidenceVersion = 2
	v2.Request.Identity.WALIdentity = "wal"
	v2.ProducerUID = "producer-uid"
	v2.MutationReceiptDigest = strings.Repeat("1", 64)
	if err := validateGCSGenerationTerminalReference(v2, "source", scopeDigest); err != nil {
		t.Fatalf("valid producer-backed V2 reference rejected: %v", err)
	}
	for _, tc := range []struct {
		name   string
		base   fencing.TerminalReference
		mutate func(*fencing.TerminalReference)
	}{
		{name: "V2 without producer", base: v2, mutate: func(ref *fencing.TerminalReference) { ref.ProducerUID = "" }},
		{name: "V2 without mutation digest", base: v2, mutate: func(ref *fencing.TerminalReference) { ref.MutationReceiptDigest = "" }},
		{name: "V2 without WAL identity", base: v2, mutate: func(ref *fencing.TerminalReference) { ref.Request.Identity.WALIdentity = "" }},
		{name: "V3 with producer UID", base: base, mutate: func(ref *fencing.TerminalReference) { ref.ProducerUID = "producer-uid" }},
		{name: "V3 with mutation digest", base: base, mutate: func(ref *fencing.TerminalReference) { ref.MutationReceiptDigest = strings.Repeat("1", 64) }},
		{name: "V3 with unsupported assignment digest", base: base, mutate: func(ref *fencing.TerminalReference) { ref.Request.Identity.AssignmentID = "not-a-digest" }},
		{name: "unsupported evidence version", base: base, mutate: func(ref *fencing.TerminalReference) { ref.EvidenceVersion = 4 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ref := tc.base
			tc.mutate(&ref)
			if err := validateGCSGenerationTerminalReference(ref, "source", scopeDigest); err == nil {
				t.Fatal("accepted reference outside its version-specific shape")
			}
		})
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
	if !gcsControlPodSnapshotMatches(captured, captured.Name, captured.UID, "", captured.OwnerUID, "") {
		t.Fatal("rejected unchanged captured DaemonSet Pod identity")
	}
	if gcsControlPodSnapshotMatches(captured, captured.Name, "replacement-pod-uid", "", captured.OwnerUID, "") {
		t.Fatal("accepted replacement Pod UID under the captured name")
	}
	if gcsControlPodSnapshotMatches(captured, captured.Name, captured.UID, "", "replacement-daemonset-uid", "") {
		t.Fatal("accepted a different owning DaemonSet UID")
	}
	operator := gcsControlPodIdentity{Name: "rhiza-operator-abc", UID: "operator-pod-uid", OwnerName: "replicaset-name", OwnerUID: "replicaset-uid", ParentOwnerUID: "deployment-uid"}
	if gcsControlPodSnapshotMatches(operator, operator.Name, operator.UID, operator.OwnerName, operator.OwnerUID, "replacement-deployment-uid") {
		t.Fatal("accepted a different owning operator Deployment UID")
	}
}

func TestGCSOperatorRestartCapturesNewPodIdentityInSameDeployment(t *testing.T) {
	old := gcsControlPodIdentity{Name: "rhiza-operator-u1", UID: "operator-pod-u1", OwnerName: "operator-rs-u1", OwnerUID: "rs-u1", ParentOwnerUID: "deployment-u1"}
	current := gcsControlPodIdentity{Name: "rhiza-operator-u2", UID: "operator-pod-u2", OwnerName: "operator-rs-u2", OwnerUID: "rs-u2", ParentOwnerUID: "deployment-u1"}
	if err := validateGCSOperatorRestartIdentity(old, current); err != nil {
		t.Fatalf("rejected new operator Pod under the same Deployment: %v", err)
	}
	newTarget := []gcsControlPodIdentity{current}
	if !gcsFaultSourceMatches(&gcsChaosFault{sourceUIDs: map[string]string{current.Name: current.UID}}, newTarget) {
		t.Fatal("replacement operator fault target did not use the new Pod UID")
	}
	if gcsFaultSourceMatches(&gcsChaosFault{sourceUIDs: map[string]string{old.Name: old.UID}}, newTarget) {
		t.Fatal("accepted the retired operator Pod UID as the replacement fault target")
	}

	wrongDeployment := current
	wrongDeployment.ParentOwnerUID = "other-deployment"
	if err := validateGCSOperatorRestartIdentity(old, wrongDeployment); err == nil {
		t.Fatal("accepted a replacement operator Pod from another Deployment")
	}
	samePod := current
	samePod.UID = old.UID
	if err := validateGCSOperatorRestartIdentity(old, samePod); err == nil {
		t.Fatal("accepted the old operator Pod UID after restart")
	}
}

func TestGCSOperatorSnapshotUsesReplicaSetOwnerNameAndUID(t *testing.T) {
	dir := t.TempDir()
	argsPath := filepath.Join(dir, "kubectl-args")
	kubectlPath := filepath.Join(dir, "kubectl")
	const fakeKubectl = `#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$KUBECTL_ARGS_LOG"
case "$6:$7" in
  pod:rhiza-operator-abc)
    printf '%s' '{"metadata":{"name":"rhiza-operator-abc","uid":"operator-pod-uid","ownerReferences":[{"controller":true,"kind":"ReplicaSet","name":"rs-name","uid":"rs-uid"}]}}'
    ;;
  replicaset:rs-name)
    printf '%s' '{"metadata":{"name":"rs-name","uid":"rs-uid","ownerReferences":[{"controller":true,"kind":"Deployment","name":"rhiza-operator","uid":"deployment-uid"}]}}'
    ;;
  deployment:rhiza-operator)
    printf '%s' '{"metadata":{"name":"rhiza-operator","uid":"deployment-uid"}}'
    ;;
  *)
    printf 'unexpected kubectl resource: %s/%s\n' "$6" "$7" >&2
    exit 2
    ;;
esac
`
	if err := os.WriteFile(kubectlPath, []byte(fakeKubectl), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("KUBECTL_ARGS_LOG", argsPath)

	expected := gcsControlPodIdentity{
		Name: "rhiza-operator-abc", UID: "operator-pod-uid", OwnerName: "rs-name", OwnerUID: "rs-uid", ParentOwnerUID: "deployment-uid",
	}
	if err := verifyGCSControlPodSnapshots(context.Background(), []gcsControlPodIdentity{expected}); err != nil {
		t.Fatalf("verify exact operator owner chain: %v", err)
	}
	args, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(args), "get replicaset rs-name -o json") || strings.Contains(string(args), "get replicaset rs-uid -o json") {
		t.Fatalf("ReplicaSet lookup did not use captured owner name: %s", args)
	}
}

func TestGCSRecoveryAfterProofRejectsMismatchedOperationProofTargetAndRows(t *testing.T) {
	source := gcsRecoverySource{ClusterID: "source", BindingUID: "binding", BindingName: "cluster", LogicalID: "logical",
		Namespace: "qualification", StatefulSet: "rhiza", StatefulSetUID: "sts", Bucket: "bucket", Prefix: "source-prefix", Durability: "before-ack"}
	request := operator.FenceRequest{OperationID: "generation-op", Scope: operator.ScopeGeneration, SourceClusterID: source.ClusterID,
		LogicalID: source.LogicalID, Namespace: source.Namespace, StatefulSet: source.StatefulSet,
		BindingUID: source.BindingUID, StatefulSetUID: source.StatefulSetUID}
	proof := operator.FenceProof{OperationID: request.OperationID, SourceClusterID: request.SourceClusterID, BindingUID: request.BindingUID,
		RequestHash: digestJSON(request), ProofID: "proof-uid", IndependentV2: &operator.IndependentEvidenceV2{Version: 2, RequestHash: digestJSON(request)}}
	pending := pendingGCSFence{Name: "fence-resource-abc123", UID: "fence-uid", Request: request}
	const expectedRows = `[[1,"baseline"],[2,"after-remove"]]`
	if err := validateGCSRecoveryAfterProofInputs(source, pending, request, proof, "fork", expectedRows); err != nil {
		t.Fatalf("rejected exact validated recovery tuple: %v", err)
	}

	wrongOperation := pending
	wrongOperation.Request.OperationID = "other-generation-op"
	if err := validateGCSRecoveryAfterProofInputs(source, wrongOperation, request, proof, "fork", expectedRows); err == nil {
		t.Fatal("accepted recovery tied to a different operation")
	}
	wrongProof := proof
	wrongProof.RequestHash = strings.Repeat("f", 64)
	if err := validateGCSRecoveryAfterProofInputs(source, pending, request, wrongProof, "fork", expectedRows); err == nil {
		t.Fatal("accepted proof not bound to the exact request hash")
	}

	recovery := operator.Resource{Status: operator.Status{Phase: "Complete", Stage: "Complete", Target: "target-cluster"}}
	root := obj{"metadata": obj{"name": source.BindingName, "namespace": source.Namespace, "uid": source.BindingUID}, "status": obj{"phase": "Healthy", "activeClusterID": "target-cluster"}}
	if err := validateGCSRecoveryTargetAdoption(root, recovery); err != nil {
		t.Fatalf("rejected exact completed target adoption: %v", err)
	}
	wrongTargetRoot := obj{"metadata": obj{"name": source.BindingName, "namespace": source.Namespace, "uid": source.BindingUID}, "status": obj{"phase": "Healthy", "activeClusterID": "other-target"}}
	if err := validateGCSRecoveryTargetAdoption(wrongTargetRoot, recovery); err == nil {
		t.Fatal("accepted root adoption of a different recovery target")
	}

	rows := []any{[]any{1, "baseline"}, []any{2, "after-remove"}}
	if !gcsRecoveryRowsMatch(rows, expectedRows) {
		t.Fatal("rejected exact source ACK rows on the recovered target")
	}
	if gcsRecoveryRowsMatch([]any{[]any{1, "baseline"}}, expectedRows) {
		t.Fatal("accepted recovered target missing the post-REMOVE source ACK")
	}
}

func TestGCSRecoveryAfterProofWaitsForStabilizingRootOnCapturedBinding(t *testing.T) {
	request := operator.FenceRequest{
		LogicalID: "logical", BindingUID: "binding-uid", Namespace: "run-ns", StatefulSet: "rhiza-voter", StatefulSetUID: "sts-uid",
		SourceClusterID: "source-generation", OperationID: "auto-operation", Scope: operator.ScopeGeneration,
		Targets: []operator.FenceTarget{{NodeID: "voter-0", Pod: "voter-0", PodUID: "source-pod-uid", WALIdentity: "source-wal"}},
	}
	proof := operator.FenceProof{OperationID: request.OperationID, SourceClusterID: request.SourceClusterID, BindingUID: request.BindingUID,
		RequestHash: digestJSON(request), ProofID: "generation-proof", IndependentV2: &operator.IndependentEvidenceV2{Version: 2, RequestHash: digestJSON(request)}}
	// The distinct Kubernetes fence name is covered separately; keep this tail
	// focused on valid parent observations after the recovery child completes.
	pending := pendingGCSFence{Name: request.OperationID, UID: "fence-resource-uid", Request: request}
	target := operator.FenceTarget{NodeID: "target-voter-0", Pod: "target-voter-0", PodUID: "target-pod-uid", WALIdentity: "target-wal"}
	recovery := operator.Resource{
		Metadata: map[string]any{"name": "recovery-resource-name", "namespace": request.Namespace, "uid": "recovery-resource-uid"},
		Spec: operator.Spec{StatefulSet: request.StatefulSet, SourceClusterID: request.SourceClusterID, Durability: "before-ack", RecoveryID: request.OperationID,
			Fence: operator.Fence{RecoveryID: request.OperationID, ClusterID: request.SourceClusterID, StatefulSetUID: request.StatefulSetUID, Confirmed: true, Evidence: proof.ProofID}},
		Status: operator.Status{Phase: "Complete", Stage: "Complete", Source: request.SourceClusterID, Target: "target-generation", StatefulSetUID: request.StatefulSetUID,
			SourceDurability: "before-ack", TargetBaseKind: "fork", SecretName: "target-admin", TargetIdentities: []operator.FenceTarget{target}},
	}
	root := func(phase, activeCluster, activeRecovery string) obj {
		return obj{"metadata": obj{"name": "captured-cluster", "namespace": "run-ns", "uid": "binding-uid"},
			"status": obj{"phase": phase, "activeClusterID": activeCluster, "activeRecovery": activeRecovery}}
	}
	tests := []struct {
		name   string
		states []obj
	}{
		{name: "first observation already healthy", states: []obj{root("Healthy", "target-generation", "")}},
		{name: "stabilizing then healthy", states: []obj{root("Stabilizing", "target-generation", ""), root("Healthy", "target-generation", "")}},
		{name: "recovering directly to healthy", states: []obj{root("Recovering", "source-generation", "auto-operation"), root("Healthy", "target-generation", "")}},
		{name: "full transition observed", states: []obj{root("Recovering", "source-generation", "auto-operation"), root("Stabilizing", "target-generation", ""), root("Healthy", "target-generation", "")}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			argsPath := filepath.Join(dir, "kubectl-args")
			readCountPath := filepath.Join(dir, "root-read-count")
			recoveryPath := filepath.Join(dir, "recoveries.json")
			rootStatesPath := filepath.Join(dir, "root-states.jsonl")
			portForwardReadyPath := filepath.Join(dir, "target-port-forward-ready")
			recoveryList, err := json.Marshal(obj{"items": []operator.Resource{recovery}})
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(recoveryPath, recoveryList, 0600); err != nil {
				t.Fatal(err)
			}
			var rootStateLines []string
			for _, state := range tc.states {
				encodedRoot, err := json.Marshal(state)
				if err != nil {
					t.Fatal(err)
				}
				rootStateLines = append(rootStateLines, string(encodedRoot))
			}
			if err := os.WriteFile(rootStatesPath, []byte(strings.Join(rootStateLines, "\n")+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			const fakeKubectl = `#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$KUBECTL_ARGS_LOG"
case "$*" in
  *'get rhizarecoveries -o json'*) cat "$RECOVERY_LIST" ;;
  *'get rhizacluster captured-cluster -o json'*)
    count=0
    [ ! -f "$ROOT_READ_COUNT" ] || count=$(cat "$ROOT_READ_COUNT")
    count=$((count + 1))
    printf '%s\n' "$count" > "$ROOT_READ_COUNT"
    sed -n "${count}p" "$ROOT_STATES"
    ;;
  *'get secret target-admin -o json'*) printf '%s\n' '{"data":{"admin":"c2VjcmV0"}}' ;;
  *'get pod target-voter-0 -o json'*) printf '%s\n' '{"metadata":{"name":"target-voter-0","uid":"target-pod-uid"},"status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}]}}' ;;
	  *'port-forward pod/target-voter-0 '*) printf '%s\n' ready > "$PORT_FORWARD_READY"; exit 0 ;;
  *) printf 'unexpected kubectl request: %s\n' "$*" >&2; exit 2 ;;
esac
`
			kubectlPath := filepath.Join(dir, "kubectl")
			if err := os.WriteFile(kubectlPath, []byte(fakeKubectl), 0700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
			t.Setenv("KUBECTL_ARGS_LOG", argsPath)
			t.Setenv("RECOVERY_LIST", recoveryPath)
			t.Setenv("ROOT_READ_COUNT", readCountPath)
			t.Setenv("ROOT_STATES", rootStatesPath)
			t.Setenv("PORT_FORWARD_READY", portForwardReadyPath)

			source := gcsRecoverySource{Namespace: request.Namespace, BindingName: "captured-cluster", LogicalID: request.LogicalID,
				StatefulSet: request.StatefulSet, ClusterID: request.SourceClusterID, StatefulSetUID: request.StatefulSetUID, BindingUID: request.BindingUID,
				Bucket: "qualification-bucket", Prefix: "run/source", Durability: "before-ack", Targets: request.Targets}
			watchdogCtx, cancelWatchdog := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelWatchdog()
			ctx, cancel := context.WithCancel(watchdogCtx)
			defer cancel()
			portForwardReached := make(chan struct{})
			go func() {
				ticker := time.NewTicker(10 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-watchdogCtx.Done():
						return
					case <-ticker.C:
						if _, err := os.Stat(portForwardReadyPath); err == nil {
							close(portForwardReached)
							cancel()
							return
						}
					}
				}
			}()
			_, err = waitForGCSRecoveryAfterProof(ctx, source, pending, request, proof, "fork", `[[1,"baseline"]]`, t.TempDir(), nil)
			select {
			case <-portForwardReached:
			case <-watchdogCtx.Done():
				t.Fatalf("bounded watchdog expired before target port-forward readiness: %v", err)
			}
			if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "port-forward target-voter-0") {
				t.Fatalf("expected the readiness sentinel to cancel after reaching target port-forward, got %v", err)
			}
			readCount, readErr := os.ReadFile(readCountPath)
			if readErr != nil || strings.TrimSpace(string(readCount)) != fmt.Sprint(len(tc.states)) {
				t.Fatalf("expected %d parent observations, count=%q err=%v", len(tc.states), readCount, readErr)
			}
			kubectlArgs, argsErr := os.ReadFile(argsPath)
			if argsErr != nil {
				t.Fatalf("could not read fake kubectl call trace: %v", argsErr)
			}
			trace := string(kubectlArgs)
			for _, step := range []string{"get rhizarecoveries -o json", "get rhizacluster captured-cluster -o json", "get secret target-admin -o json", "get pod target-voter-0 -o json", "port-forward pod/target-voter-0"} {
				if !strings.Contains(trace, step) {
					t.Fatalf("recovery tail did not reach expected readiness step %q; trace=%s", step, trace)
				}
			}
		})
	}
}

func TestGCSRecoveryRootAdoptionRejectsIdentityAndLineageDrift(t *testing.T) {
	source := gcsRecoverySource{Namespace: "run-ns", BindingName: "captured-cluster", BindingUID: "binding-uid",
		ClusterID: "source-generation", StatefulSet: "rhiza-voter", StatefulSetUID: "sts-uid"}
	recovery := operator.Resource{Metadata: map[string]any{"name": "recovery-name", "namespace": source.Namespace, "uid": "recovery-uid"},
		Spec:   operator.Spec{StatefulSet: source.StatefulSet, RecoveryID: "auto-operation", SourceClusterID: source.ClusterID},
		Status: operator.Status{Source: source.ClusterID, StatefulSetUID: "sts-uid", Target: "target-generation"}}
	root := func(name, namespace, uid, phase, activeCluster, activeRecovery string) obj {
		return obj{"metadata": obj{"name": name, "namespace": namespace, "uid": uid},
			"status": obj{"phase": phase, "activeClusterID": activeCluster, "activeRecovery": activeRecovery}}
	}
	targetObserved, done, err := observeGCSRecoveryRoot(source, recovery, false,
		root(source.BindingName, source.Namespace, source.BindingUID, "Recovering", source.ClusterID, recovery.Spec.RecoveryID))
	if err != nil || done || targetObserved {
		t.Fatalf("rejected exact source recovery checkpoint: targetObserved=%t done=%t err=%v", targetObserved, done, err)
	}
	targetObserved, done, err = observeGCSRecoveryRoot(source, recovery, targetObserved,
		root(source.BindingName, source.Namespace, source.BindingUID, "Stabilizing", recovery.Status.Target, ""))
	if err != nil || done || !targetObserved {
		t.Fatalf("rejected exact target stabilizing checkpoint: targetObserved=%t done=%t err=%v", targetObserved, done, err)
	}
	if targetObserved, done, err = observeGCSRecoveryRoot(source, recovery, targetObserved,
		root(source.BindingName, source.Namespace, source.BindingUID, "Healthy", recovery.Status.Target, "")); err != nil || !done {
		t.Fatalf("rejected exact target Healthy checkpoint: targetObserved=%t done=%t err=%v", targetObserved, done, err)
	}

	for _, tc := range []struct {
		name           string
		targetObserved bool
		root           obj
	}{
		{name: "binding UID changed", root: root(source.BindingName, source.Namespace, "replacement-binding-uid", "Recovering", source.ClusterID, recovery.Spec.RecoveryID)},
		{name: "binding name changed", root: root("replacement-cluster", source.Namespace, source.BindingUID, "Recovering", source.ClusterID, recovery.Spec.RecoveryID)},
		{name: "wrong operation", root: root(source.BindingName, source.Namespace, source.BindingUID, "Recovering", source.ClusterID, "other-operation")},
		{name: "third target", root: root(source.BindingName, source.Namespace, source.BindingUID, "Stabilizing", "third-generation", "")},
		{name: "source regression", targetObserved: true, root: root(source.BindingName, source.Namespace, source.BindingUID, "Recovering", source.ClusterID, recovery.Spec.RecoveryID)},
		{name: "different active recovery", targetObserved: true, root: root(source.BindingName, source.Namespace, source.BindingUID, "Stabilizing", recovery.Status.Target, "other-operation")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := observeGCSRecoveryRoot(source, recovery, tc.targetObserved, tc.root); err == nil {
				t.Fatal("accepted a changed binding or recovery lineage")
			}
		})
	}
}

func TestGCSRecoveryTargetAdoptionWaitUsesExistingDeadline(t *testing.T) {
	dir := t.TempDir()
	readCountPath := filepath.Join(dir, "root-read-count")
	const fakeKubectl = `#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$KUBECTL_ARGS_LOG"
count=0
[ ! -f "$ROOT_READ_COUNT" ] || count=$(cat "$ROOT_READ_COUNT")
count=$((count + 1))
printf '%s\n' "$count" > "$ROOT_READ_COUNT"
printf '%s\n' '{"metadata":{"name":"captured-cluster","namespace":"run-ns","uid":"binding-uid"},"status":{"phase":"Recovering","activeClusterID":"source-generation","activeRecovery":"auto-operation"}}'
`
	kubectlPath := filepath.Join(dir, "kubectl")
	if err := os.WriteFile(kubectlPath, []byte(fakeKubectl), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("KUBECTL_ARGS_LOG", filepath.Join(dir, "kubectl-args"))
	t.Setenv("ROOT_READ_COUNT", readCountPath)
	source := gcsRecoverySource{Namespace: "run-ns", BindingName: "captured-cluster", BindingUID: "binding-uid",
		ClusterID: "source-generation", StatefulSet: "rhiza-voter", StatefulSetUID: "sts-uid"}
	recovery := operator.Resource{Metadata: map[string]any{"name": "recovery-name", "namespace": source.Namespace, "uid": "recovery-uid"},
		Spec:   operator.Spec{StatefulSet: source.StatefulSet, RecoveryID: "auto-operation", SourceClusterID: source.ClusterID},
		Status: operator.Status{Source: source.ClusterID, StatefulSetUID: source.StatefulSetUID, Target: "target-generation"}}
	ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
	defer cancel()
	err := waitForGCSRecoveryTargetAdoption(ctx, source, recovery)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected the existing context deadline to stop the recovery wait, got %v", err)
	}
	readCount, readErr := os.ReadFile(readCountPath)
	if readErr != nil || strings.TrimSpace(string(readCount)) == "" {
		t.Fatalf("deadline wait did not perform a root observation: count=%q err=%v", readCount, readErr)
	}
}

func TestGCSRecoveryFailurePreservesForwardCleanupError(t *testing.T) {
	logFile, err := os.CreateTemp(t.TempDir(), "recovery-port-forward-log-*")
	if err != nil {
		t.Fatal(err)
	}
	if err := logFile.Close(); err != nil {
		t.Fatal(err)
	}

	operationErr := errors.New("membership verification failed")
	cleanupErr := errors.New("port-forward wait failed")
	forward := &forward{cmd: &exec.Cmd{Process: &os.Process{Pid: -1}}, done: make(chan error, 1), log: logFile}
	forward.done <- cleanupErr

	err = joinForwardCloseError(forward, operationErr)
	if !errors.Is(err, operationErr) || !errors.Is(err, cleanupErr) ||
		!strings.Contains(err.Error(), "kill port-forward process") || !strings.Contains(err.Error(), "close port-forward log file") {
		t.Fatalf("recovery failure did not preserve its operation and cleanup errors: %v", err)
	}
}

func TestMixedQualificationManifestPinsRemoveGateToConfiguredSource(t *testing.T) {
	const key = "RHIZA_QUALIFICATION_REMOVE_GATE_SOURCE"
	t.Setenv(key, "")
	statefulSet := func() obj {
		for _, item := range manifests()["items"].([]obj) {
			if item["kind"] == "StatefulSet" && nested(item, "metadata", "name") == "rhiza" {
				return item
			}
		}
		t.Fatal("fixture omitted source StatefulSet")
		return nil
	}
	appEnv := func(sts obj) []any {
		values, _ := nested(sts, "spec", "template", "spec", "containers").([]obj)
		if len(values) != 1 {
			t.Fatal("fixture source container shape changed")
		}
		env, _ := values[0]["env"].([]obj)
		result := make([]any, len(env))
		for i := range env {
			result[i] = env[i]
		}
		return result
	}
	for _, entry := range appEnv(statefulSet()) {
		if nested(entry, "name") == key {
			t.Fatal("default fixture unexpectedly enables REMOVE response gate")
		}
	}
	t.Setenv(key, "chaos-source")
	var found bool
	for _, entry := range appEnv(statefulSet()) {
		if nested(entry, "name") == key && nested(entry, "value") == "chaos-source" {
			found = true
		}
	}
	if !found {
		t.Fatal("mixed fixture did not pin the source-only REMOVE gate identity")
	}
}
