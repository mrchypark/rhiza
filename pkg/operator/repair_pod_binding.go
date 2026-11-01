package operator

import (
	"context"
	"fmt"
	"net"
	"net/url"
)

// V1Alpha2RepairPodBinding is the live controller identity established by a
// repair Pod's owner chain. Resource versions are not identity: routine
// controller updates do not change the owner chain.
type V1Alpha2RepairPodBinding struct {
	Namespace      string `json:"namespace"`
	PodName        string `json:"podName"`
	PodUID         string `json:"podUID"`
	PodIP          string `json:"podIP"`
	ReplicaSetName string `json:"replicaSetName"`
	ReplicaSetUID  string `json:"replicaSetUID"`
	DeploymentUID  string `json:"deploymentUID"`
	ClusterUID     string `json:"clusterUID"`
	Generation     int64  `json:"generation"`
}

// ValidateV1Alpha2RepairPodBinding retains the full existing Pod/key/endpoint
// and Pod→ReplicaSet→Deployment→RhizaCluster validation, returning the exact
// live owner identities for immutable repair-origin comparison.
func ValidateV1Alpha2RepairPodBinding(ctx context.Context, store *V1Alpha2StatusStore, ref V1Alpha2ClusterRef, p V1Alpha2RepairPlan) (V1Alpha2RepairPodBinding, error) {
	var zero V1Alpha2RepairPodBinding
	if err := validateV1Alpha2RepairPodIdentity(ctx, store, ref, p); err != nil {
		return zero, err
	}
	var pod struct {
		APIVersion, Kind string
		Metadata         v1alpha2BootstrapMetadata
		Status           struct{ PodIP string }
	}
	if err := store.Kube.Get(ctx, "/api/v1/namespaces/"+ref.Namespace+"/pods/"+url.PathEscape(p.Replacement.PodName), &pod); err != nil {
		return zero, err
	}
	u, _ := url.Parse(p.Replacement.PeerURL)
	if pod.APIVersion != "v1" || pod.Kind != "Pod" || pod.Metadata.Namespace != ref.Namespace || pod.Metadata.Name != p.Replacement.PodName || pod.Metadata.UID != p.Replacement.PodUID || pod.Metadata.ResourceVersion == "" || pod.Metadata.DeletionTimestamp != nil || net.ParseIP(pod.Status.PodIP) == nil || !net.ParseIP(pod.Status.PodIP).Equal(net.ParseIP(u.Hostname())) {
		return zero, fmt.Errorf("repair Pod binding changed")
	}
	var rsOwner *v1alpha2BootstrapOwner
	for i := range pod.Metadata.OwnerReferences {
		o := &pod.Metadata.OwnerReferences[i]
		if o.Controller != nil && *o.Controller {
			if rsOwner != nil {
				return zero, fmt.Errorf("ambiguous repair Pod owner")
			}
			rsOwner = o
		}
	}
	if rsOwner == nil || rsOwner.APIVersion != "apps/v1" || rsOwner.Kind != "ReplicaSet" || rsOwner.UID == "" {
		return zero, fmt.Errorf("invalid repair Pod owner")
	}
	var rs struct {
		APIVersion, Kind string
		Metadata         v1alpha2BootstrapMetadata
	}
	base := "/apis/apps/v1/namespaces/" + ref.Namespace
	if err := store.Kube.Get(ctx, base+"/replicasets/"+url.PathEscape(rsOwner.Name), &rs); err != nil {
		return zero, err
	}
	if rs.APIVersion != "apps/v1" || rs.Kind != "ReplicaSet" || rs.Metadata.Name != rsOwner.Name || rs.Metadata.ResourceVersion == "" || rs.Metadata.UID != rsOwner.UID || rs.Metadata.Namespace != ref.Namespace || rs.Metadata.DeletionTimestamp != nil {
		return zero, fmt.Errorf("repair ReplicaSet binding changed")
	}
	var deploymentOwner *v1alpha2BootstrapOwner
	for i := range rs.Metadata.OwnerReferences {
		o := &rs.Metadata.OwnerReferences[i]
		if o.Controller != nil && *o.Controller {
			if deploymentOwner != nil {
				return zero, fmt.Errorf("ambiguous repair ReplicaSet owner")
			}
			deploymentOwner = o
		}
	}
	if deploymentOwner == nil || deploymentOwner.APIVersion != "apps/v1" || deploymentOwner.Kind != "Deployment" || deploymentOwner.Name == "" || deploymentOwner.UID != p.DeploymentUID {
		return zero, fmt.Errorf("repair Deployment owner changed")
	}
	var deployment struct {
		APIVersion, Kind string
		Metadata         v1alpha2BootstrapMetadata
	}
	if err := store.Kube.Get(ctx, base+"/deployments/"+url.PathEscape(deploymentOwner.Name), &deployment); err != nil {
		return zero, err
	}
	if deployment.APIVersion != "apps/v1" || deployment.Kind != "Deployment" || deployment.Metadata.Name != deploymentOwner.Name || deployment.Metadata.ResourceVersion == "" || deployment.Metadata.UID != p.DeploymentUID || deployment.Metadata.Namespace != ref.Namespace || deployment.Metadata.DeletionTimestamp != nil || !bootstrapOwner(deployment.Metadata, V1Alpha2APIVersion, "RhizaCluster", ref.Name, ref.UID) {
		return zero, fmt.Errorf("repair Deployment binding changed")
	}
	return V1Alpha2RepairPodBinding{Namespace: ref.Namespace, PodName: p.Replacement.PodName, PodUID: pod.Metadata.UID, PodIP: pod.Status.PodIP, ReplicaSetName: rsOwner.Name, ReplicaSetUID: rsOwner.UID, DeploymentUID: p.DeploymentUID, ClusterUID: ref.UID, Generation: p.Generation}, nil
}

// validateV1Alpha2RepairPodIdentity accepts the stable init identity after a
// same-Pod restart. The initial repair authorization's stricter main-container
// gate remains in repair_v1alpha2.go.
func validateV1Alpha2RepairPodIdentity(ctx context.Context, store *V1Alpha2StatusStore, ref V1Alpha2ClusterRef, p V1Alpha2RepairPlan) error {
	var pod struct {
		APIVersion, Kind string
		Metadata         v1alpha2BootstrapMetadata
		Status           struct {
			PodIP                 string
			InitContainerStatuses []struct {
				Name  string
				State struct {
					Terminated *struct {
						ExitCode int
						Message  string
					}
				}
			}
		}
	}
	if err := store.Kube.Get(ctx, "/api/v1/namespaces/"+ref.Namespace+"/pods/"+url.PathEscape(p.Replacement.PodName), &pod); err != nil {
		return err
	}
	u, _ := url.Parse(p.Replacement.PeerURL)
	if pod.APIVersion != "v1" || pod.Kind != "Pod" || pod.Metadata.Namespace != ref.Namespace || pod.Metadata.Name != p.Replacement.PodName || pod.Metadata.UID != p.Replacement.PodUID || pod.Metadata.ResourceVersion == "" || pod.Metadata.DeletionTimestamp != nil || net.ParseIP(pod.Status.PodIP) == nil || !net.ParseIP(pod.Status.PodIP).Equal(net.ParseIP(u.Hostname())) {
		return fmt.Errorf("repair Pod identity or endpoint mismatch")
	}
	if len(pod.Status.InitContainerStatuses) == 0 || pod.Status.InitContainerStatuses[0].Name != V1Alpha2PrepareIdentityContainer || pod.Status.InitContainerStatuses[0].State.Terminated == nil || pod.Status.InitContainerStatuses[0].State.Terminated.ExitCode != 0 {
		return fmt.Errorf("repair Pod identity is not prepared")
	}
	key, err := DecodeV1Alpha2BootstrapReport(pod.Status.InitContainerStatuses[0].State.Terminated.Message)
	if err != nil || key != p.Replacement.PublicKey {
		return fmt.Errorf("repair Pod public key mismatch")
	}
	var owner *v1alpha2BootstrapOwner
	for i := range pod.Metadata.OwnerReferences {
		o := &pod.Metadata.OwnerReferences[i]
		if o.Controller != nil && *o.Controller {
			if owner != nil {
				return fmt.Errorf("ambiguous repair Pod owner")
			}
			owner = o
		}
	}
	if owner == nil || owner.APIVersion != "apps/v1" || owner.Kind != "ReplicaSet" || owner.Name == "" || owner.UID == "" {
		return fmt.Errorf("invalid repair Pod owner")
	}
	var rs struct {
		APIVersion, Kind string
		Metadata         v1alpha2BootstrapMetadata
	}
	base := "/apis/apps/v1/namespaces/" + ref.Namespace
	if err := store.Kube.Get(ctx, base+"/replicasets/"+url.PathEscape(owner.Name), &rs); err != nil {
		return err
	}
	if rs.APIVersion != "apps/v1" || rs.Kind != "ReplicaSet" || rs.Metadata.Name != owner.Name || rs.Metadata.ResourceVersion == "" || rs.Metadata.UID != owner.UID || rs.Metadata.Namespace != ref.Namespace || rs.Metadata.DeletionTimestamp != nil {
		return fmt.Errorf("invalid repair ReplicaSet")
	}
	var deploymentOwner *v1alpha2BootstrapOwner
	for i := range rs.Metadata.OwnerReferences {
		o := &rs.Metadata.OwnerReferences[i]
		if o.Controller != nil && *o.Controller {
			if deploymentOwner != nil {
				return fmt.Errorf("ambiguous repair Deployment owner")
			}
			deploymentOwner = o
		}
	}
	if deploymentOwner == nil || deploymentOwner.APIVersion != "apps/v1" || deploymentOwner.Kind != "Deployment" || deploymentOwner.UID != p.DeploymentUID || deploymentOwner.Name == "" {
		return fmt.Errorf("invalid repair Deployment owner")
	}
	var dep struct {
		APIVersion, Kind string
		Metadata         v1alpha2BootstrapMetadata
	}
	if err := store.Kube.Get(ctx, base+"/deployments/"+url.PathEscape(deploymentOwner.Name), &dep); err != nil {
		return err
	}
	if dep.APIVersion != "apps/v1" || dep.Kind != "Deployment" || dep.Metadata.Name != deploymentOwner.Name || dep.Metadata.ResourceVersion == "" || dep.Metadata.UID != p.DeploymentUID || dep.Metadata.Namespace != ref.Namespace || dep.Metadata.DeletionTimestamp != nil || !bootstrapOwner(dep.Metadata, V1Alpha2APIVersion, "RhizaCluster", ref.Name, ref.UID) {
		return fmt.Errorf("invalid repair Deployment binding")
	}
	return nil
}
