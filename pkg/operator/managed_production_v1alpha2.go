package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
)

// ReconcileV1Alpha2ManagedProduction installs production guardrails that do
// not participate in membership. It is idempotent and never rewrites a
// divergence silently.
func ReconcileV1Alpha2ManagedProduction(ctx context.Context, kube *Kubernetes, ref V1Alpha2ClusterRef) error {
	store := &V1Alpha2StatusStore{Kube: kube}
	snapshot, err := store.Load(ctx, ref)
	if err != nil {
		return err
	}
	if _, err := snapshot.BootstrapPlan(); err != nil {
		return err
	}
	rawCluster, err := json.Marshal(snapshot.object)
	if err != nil {
		return err
	}
	c, err := DecodeV1Alpha2Cluster(rawCluster)
	if err != nil {
		return err
	}
	name, err := V1Alpha2GenesisDeploymentName(ref.UID)
	if err != nil {
		return err
	}
	labels := map[string]string{"rhiza.mrchypark.dev/cluster-uid": ref.UID, "rhiza.mrchypark.dev/generation": "0"}
	owner := []any(nil)
	policy := c.Spec.DeletionPolicy
	if policy == "" {
		policy = "Orphan"
	}
	if policy == "Delete" {
		owner = []any{map[string]any{"apiVersion": V1Alpha2APIVersion, "kind": "RhizaCluster", "name": ref.Name, "uid": ref.UID, "controller": true}}
	}
	pdbName := name + "-pdb"
	desired := map[string]any{
		"apiVersion": "policy/v1", "kind": "PodDisruptionBudget",
		"metadata": map[string]any{"name": pdbName, "namespace": ref.Namespace, "labels": labels, "ownerReferences": owner},
		"spec":     map[string]any{"maxUnavailable": 1, "selector": map[string]any{"matchLabels": labels}},
	}
	base := "/apis/policy/v1/namespaces/" + ref.Namespace + "/poddisruptionbudgets"
	postErr := kube.Post(ctx, base, desired, nil)
	if postErr != nil {
		var apiErr *APIError
		if !errors.As(postErr, &apiErr) || apiErr.StatusCode != http.StatusConflict {
			return fmt.Errorf("create managed PodDisruptionBudget: %w", postErr)
		}
	}
	var live map[string]json.RawMessage
	if err := kube.Get(ctx, base+"/"+pdbName, &live); err != nil {
		return fmt.Errorf("read managed PodDisruptionBudget: %w", err)
	}
	var got struct {
		APIVersion, Kind string
		Metadata         struct {
			Name, Namespace, UID string
			Labels               map[string]string
			OwnerReferences      []v1alpha2BootstrapOwner
			DeletionTimestamp    *string
		}
		Spec map[string]json.RawMessage
	}
	b, _ := json.Marshal(live)
	if json.Unmarshal(b, &got) != nil || got.APIVersion != "policy/v1" || got.Kind != "PodDisruptionBudget" || got.Metadata.Name != pdbName || got.Metadata.Namespace != ref.Namespace || got.Metadata.UID == "" || got.Metadata.DeletionTimestamp != nil || !reflect.DeepEqual(got.Metadata.Labels, labels) {
		return fmt.Errorf("managed PodDisruptionBudget identity mismatch")
	}
	if policy == "Delete" && !bootstrapOwner(v1alpha2BootstrapMetadata{OwnerReferences: got.Metadata.OwnerReferences}, V1Alpha2APIVersion, "RhizaCluster", ref.Name, ref.UID) {
		return fmt.Errorf("managed PodDisruptionBudget owner mismatch")
	}
	if policy == "Orphan" && len(got.Metadata.OwnerReferences) != 0 {
		return fmt.Errorf("managed PodDisruptionBudget is not orphaned")
	}
	selector, _ := json.Marshal(map[string]any{"matchLabels": labels})
	if !sameJSON(got.Spec["maxUnavailable"], json.RawMessage("1")) || len(bytes.TrimSpace(got.Spec["minAvailable"])) != 0 || !sameJSON(got.Spec["selector"], selector) {
		return fmt.Errorf("managed PodDisruptionBudget contract mismatch")
	}
	return nil
}
