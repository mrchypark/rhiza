package operator

import (
	"context"
	"encoding/json"
	"fmt"
)

// ReconcileV1Alpha2ManagedImageUpgrade applies one workload image generation
// only while membership is idle. The Deployment creates one surging Pod; the
// existing managed replacement loop performs the certified ADD-first handoff.
func ReconcileV1Alpha2ManagedImageUpgrade(ctx context.Context, kube *Kubernetes, ref V1Alpha2ClusterRef, profile V1Alpha2RuntimeProfile) error {
	store := &V1Alpha2StatusStore{Kube: kube}
	snapshot, err := store.Load(ctx, ref)
	if err != nil {
		return err
	}
	if _, err := snapshot.BootstrapPlan(); err != nil {
		return err
	}
	admission, err := LoadV1Alpha2Admission(ctx, store, ref)
	if err != nil {
		return err
	}
	_, pending := admission.Pending()
	_, repairing := admission.Repair()
	_, scaling, _ := admission.ReplicaStep()
	if pending || hasV1Alpha2Transition(admission.Transition()) || repairing || scaling {
		return nil
	}
	rawCluster, err := json.Marshal(snapshot.object)
	if err != nil {
		return err
	}
	cluster, err := DecodeV1Alpha2Cluster(rawCluster)
	if err != nil {
		return err
	}
	name, err := V1Alpha2GenesisDeploymentName(ref.UID)
	if err != nil {
		return err
	}
	base := "/apis/apps/v1/namespaces/" + ref.Namespace + "/deployments/" + name
	var live map[string]json.RawMessage
	if err := kube.Get(ctx, base, &live); err != nil {
		return err
	}
	var dep struct {
		APIVersion, Kind string
		Metadata         v1alpha2BootstrapMetadata
		Spec             struct {
			Replicas *int32 `json:"replicas"`
			Template struct {
				Spec struct {
					Containers []V1Alpha2Container `json:"containers"`
				} `json:"spec"`
			} `json:"template"`
		} `json:"spec"`
		Status struct {
			Replicas           int32 `json:"replicas"`
			ReadyReplicas      int32 `json:"readyReplicas"`
			UpdatedReplicas    int32 `json:"updatedReplicas"`
			ObservedGeneration int64 `json:"observedGeneration"`
		} `json:"status"`
	}
	b, _ := json.Marshal(live)
	if json.Unmarshal(b, &dep) != nil || dep.APIVersion != "apps/v1" || dep.Kind != "Deployment" || dep.Metadata.Name != name || dep.Metadata.Namespace != ref.Namespace || dep.Metadata.UID != admission.Authority().Binding.DeploymentUID || dep.Metadata.DeletionTimestamp != nil || !bootstrapOwner(dep.Metadata, V1Alpha2APIVersion, "RhizaCluster", ref.Name, ref.UID) || dep.Spec.Replicas == nil {
		return fmt.Errorf("managed image upgrade Deployment identity mismatch")
	}
	if dep.Status.ObservedGeneration != dep.Metadata.Generation || dep.Status.ReadyReplicas != *dep.Spec.Replicas || dep.Status.UpdatedReplicas != *dep.Spec.Replicas {
		return nil
	}
	if len(dep.Spec.Template.Spec.Containers) != len(cluster.Spec.Workload.PodTemplate.Spec.Containers) {
		return fmt.Errorf("managed image upgrade container set changed")
	}
	changed := false
	for i, c := range dep.Spec.Template.Spec.Containers {
		want := cluster.Spec.Workload.PodTemplate.Spec.Containers[i]
		if c.Name != want.Name {
			return fmt.Errorf("managed image upgrade container identity changed")
		}
		changed = changed || c.Image != want.Image
	}
	if !changed {
		return nil
	}
	podSpec, err := V1Alpha2ManagedSQLPodSpec(ref, profile, cluster.Spec.Workload.PodTemplate.Spec.Containers)
	if err != nil {
		return err
	}
	var next map[string]any
	if json.Unmarshal(b, &next) != nil {
		return fmt.Errorf("invalid managed image upgrade Deployment")
	}
	spec := asObject(next["spec"])
	spec["strategy"] = map[string]any{"type": "RollingUpdate", "rollingUpdate": map[string]any{"maxSurge": 1, "maxUnavailable": 0}}
	asObject(spec["template"])["spec"] = podSpec
	meta := asObject(next["metadata"])
	delete(meta, "annotations")
	delete(meta, "generation")
	delete(meta, "managedFields")
	delete(meta, "creationTimestamp")
	var written map[string]json.RawMessage
	if err := kube.Put(ctx, base, next, &written); err != nil {
		return err
	}
	writtenMeta, _ := json.Marshal(written["metadata"])
	var wm v1alpha2BootstrapMetadata
	var writtenSpec map[string]json.RawMessage
	if json.Unmarshal(writtenMeta, &wm) != nil || wm.UID != dep.Metadata.UID || wm.ResourceVersion == dep.Metadata.ResourceVersion || json.Unmarshal(written["spec"], &writtenSpec) != nil || !sameJSON(writtenSpec["strategy"], json.RawMessage(`{"type":"RollingUpdate","rollingUpdate":{"maxSurge":1,"maxUnavailable":0}}`)) {
		return fmt.Errorf("managed image upgrade readback mismatch")
	}
	return nil
}
