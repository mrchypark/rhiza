package operator

import (
	"context"
	"fmt"
)

// KubernetesFencer submits immutable requests to a namespaced execution backend.
// Only the executor may write status; absent or incomplete evidence remains pending.
// The executor must establish real runtime, admission and storage barriers. Pod
// deletion or Kubernetes object disappearance alone is never sufficient evidence.
type KubernetesFencer struct{ Kube *Kubernetes }

type fenceResource struct {
	APIVersion string       `json:"apiVersion"`
	Kind       string       `json:"kind"`
	Metadata   object       `json:"metadata"`
	Spec       FenceRequest `json:"spec"`
	Status     struct {
		Proof *FenceProof `json:"proof,omitempty"`
	} `json:"status,omitempty"`
}

func (f *KubernetesFencer) Fence(ctx context.Context, req FenceRequest) (*FenceProof, error) {
	if f.Kube == nil || req.Namespace != f.Kube.Namespace {
		return nil, fmt.Errorf("fencing: Kubernetes namespace mismatch")
	}
	if err := validateRequest(&req); err != nil {
		return nil, err
	}
	hash, err := requestHash(&req)
	if err != nil {
		return nil, err
	}
	// Key only by bound operation: changing the target cannot create a second request.
	name := "fence-" + hashJSON([]string{req.BindingUID, req.OperationID})[:40]
	endpoint := "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/" + f.Kube.Namespace + "/rhizafences"
	var record fenceResource
	err = f.Kube.Get(ctx, endpoint+"/"+name, &record)
	if apiNotFound(err) {
		record = fenceResource{APIVersion: "rhiza.mrchypark.dev/v1alpha1", Kind: "RhizaFence", Metadata: object{"name": name, "namespace": req.Namespace}, Spec: req}
		if err := f.Kube.Post(ctx, endpoint, &record, nil); err != nil {
			return nil, err
		}
		return nil, ErrFencePending
	}
	if err != nil {
		return nil, err
	}
	recordedHash, err := requestHash(&record.Spec)
	if err != nil || recordedHash != hash || record.Metadata["deletionTimestamp"] != nil {
		return nil, fmt.Errorf("fencing: immutable Kubernetes operation differs or is deleting")
	}
	if record.Status.Proof == nil {
		return nil, ErrFencePending
	}
	if err := validateFenceProof(req, record.Status.Proof); err != nil {
		return nil, err
	}
	return record.Status.Proof, nil
}
