package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/operator"
)

func TestImmutableImageResolutionDescriptors(t *testing.T) {
	config := "sha256:" + strings.Repeat("a", 64)
	for _, reference := range []string{config, "containerd://" + config, "docker.io/library/import-date@" + config} {
		if got, err := immutableDigest(reference); err != nil || got != config {
			t.Fatalf("immutable reference %q: %q, %v", reference, got, err)
		}
	}
	for _, reference := range []string{"image:local", "sha256:abc", "sha256:" + strings.Repeat("z", 64)} {
		if _, err := immutableDigest(reference); err == nil {
			t.Fatalf("accepted mutable or invalid reference %q", reference)
		}
	}
	for _, tc := range []struct {
		body  string
		valid bool
	}{
		{`{"config":{"digest":"` + config + `"}}`, true},
		{`{"manifests":[{"digest":"` + config + `"}]}`, true},
		{`{"manifests":[{"digest":"` + config + `"},{"digest":"` + config + `"}]}`, false},
		{`{"config":{"digest":"image:local"}}`, false},
		{`{}`, false},
	} {
		body := []byte(tc.body)
		d := fmt.Sprintf("sha256:%x", sha256.Sum256(body))
		got, err := imageDescriptor(d, body)
		if tc.valid && (err != nil || got != config) || !tc.valid && err == nil {
			t.Fatalf("descriptor %s: %q, %v", tc.body, got, err)
		}
		if _, err := imageDescriptor(config, body); err == nil {
			t.Fatal("accepted content that did not hash to the requested digest")
		}
	}
}

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
