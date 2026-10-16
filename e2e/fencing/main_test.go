package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/mrchypark/rhiza/pkg/operator"
)

func TestFixtureHoldsWritersUntilSupervisorAndStoreReady(t *testing.T) {
	items := manifests()["items"].([]obj)
	var sts, job, operator bool
	for _, item := range items {
		if nested(item, "metadata", "namespace") != namespace {
			t.Fatal("fixture escaped owned namespace")
		}
		switch item["kind"] {
		case "StatefulSet":
			sts = true
			if nested(item, "spec", "updateStrategy", "type") != "OnDelete" {
				t.Fatal("qualification changed the recovery OnDelete strategy")
			}
			if nested(item, "spec", "replicas") != 0 {
				t.Fatal("writer launched before canonical supervised patch")
			}
		case "Job":
			job = true
			if nested(item, "spec", "suspend") != true {
				t.Fatal("bucket setup races store readiness")
			}
		case "Deployment":
			if nested(item, "metadata", "name") == "rhiza-operator" {
				operator = true
				if nested(item, "spec", "replicas") != 0 {
					t.Fatal("operator launched before voters")
				}
			}
		}
	}
	if !sts || !job || !operator {
		t.Fatal("fixture omitted lifecycle resource")
	}
}

func TestGenerationProofRejectsUnrelatedAuthorityBeforeReadingLedger(t *testing.T) {
	request := operator.FenceRequest{LogicalID: "fencing-e2e", BindingUID: "binding", Namespace: namespace, StatefulSet: "rhiza", StatefulSetUID: "sts", SourceClusterID: "chaos-source", OperationID: "operation", Scope: operator.ScopeGeneration}
	proof := operator.FenceProof{OperationID: request.OperationID, SourceClusterID: request.SourceClusterID, BindingUID: request.BindingUID, RequestHash: fmt.Sprintf("%x", sha256.Sum256(encoded(request))), ProofID: "cooperative-proof", ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true}
	for _, change := range []func(*operator.FenceRequest, *operator.FenceProof){
		func(r *operator.FenceRequest, _ *operator.FenceProof) { r.SourceClusterID = "other" },
		func(r *operator.FenceRequest, _ *operator.FenceProof) { r.StatefulSetUID = "other" },
		func(r *operator.FenceRequest, _ *operator.FenceProof) { r.BindingUID = "other" },
		func(_ *operator.FenceRequest, p *operator.FenceProof) { p.OperationID = "other" },
		func(_ *operator.FenceRequest, p *operator.FenceProof) { p.RequestHash = "other" },
		func(_ *operator.FenceRequest, p *operator.FenceProof) { p.StorageQuiesced = false },
	} {
		r, p := request, proof
		change(&r, &p)
		if err := verifyGenerationProof(context.Background(), obj{"spec": r, "status": obj{"proof": p}}, "sts", "binding", []string{"old"}, t.TempDir()); err == nil {
			t.Fatal("unrelated/incomplete proof accepted")
		}
	}
}
