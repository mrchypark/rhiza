package operator

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strconv"

	"github.com/mrchypark/rhiza/internal/types"
)

type V1Alpha2RepairPodRequest = V1Alpha2RepairMember

type V1Alpha2ManagedPodObservation struct {
	Member V1Alpha2RepairPodRequest
	// Candidate means the mandatory identity init completed but main never ran.
	Candidate bool
}

// ObserveV1Alpha2ManagedPods discovers only the installed Deployment's Pods.
// Absence is consensus replacement input, never physical-death evidence.
func ObserveV1Alpha2ManagedPods(ctx context.Context, store *V1Alpha2StatusStore, ref V1Alpha2ClusterRef, peerPort int) ([]V1Alpha2ManagedPodObservation, error) {
	if store == nil || store.Kube == nil || peerPort < 1 || peerPort > 65535 {
		return nil, fmt.Errorf("invalid managed discovery binding")
	}
	initial, err := LoadV1Alpha2ApplicationInit(ctx, store, ref)
	if err != nil {
		return nil, err
	}
	p := initial.Assignment().Plan
	base := "/api/v1/namespaces/" + url.PathEscape(ref.Namespace) + "/pods"
	query := url.Values{"labelSelector": {"rhiza.mrchypark.dev/cluster-uid=" + ref.UID}, "limit": {"128"}}
	version := ""
	seen := map[string]bool{}
	var result []V1Alpha2ManagedPodObservation
	for pages := 0; ; pages++ {
		if pages >= 16 {
			return nil, fmt.Errorf("managed Pod discovery exceeds bounded page budget")
		}
		var page struct {
			APIVersion, Kind string
			Metadata         struct{ ResourceVersion, Continue string }
			Items            []struct{ Metadata v1alpha2BootstrapMetadata }
		}
		if err := store.Kube.Get(ctx, base+"?"+query.Encode(), &page); err != nil {
			return nil, err
		}
		if page.APIVersion != "v1" || page.Kind != "PodList" || page.Metadata.ResourceVersion == "" || version != "" && version != page.Metadata.ResourceVersion || page.Items == nil {
			return nil, fmt.Errorf("invalid managed Pod list")
		}
		version = page.Metadata.ResourceVersion
		for _, item := range page.Items {
			if item.Metadata.Name == "" || item.Metadata.UID == "" || item.Metadata.Namespace != ref.Namespace || seen[item.Metadata.UID] {
				return nil, fmt.Errorf("invalid or repeated managed Pod list identity")
			}
			seen[item.Metadata.UID] = true
			// Native list items can omit TypeMeta; require it on the direct GET.
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
					ContainerStatuses []struct {
						RestartCount int
						State        struct{ Running, Terminated *struct{} }
					}
				}
			}
			if err := store.Kube.Get(ctx, base+"/"+url.PathEscape(item.Metadata.Name), &pod); err != nil {
				return nil, err
			}
			if pod.APIVersion != "v1" || pod.Kind != "Pod" || pod.Metadata.Name != item.Metadata.Name || pod.Metadata.UID != item.Metadata.UID || pod.Metadata.Namespace != ref.Namespace || pod.Metadata.ResourceVersion == "" {
				return nil, fmt.Errorf("managed Pod changed during discovery")
			}
			if pod.Metadata.DeletionTimestamp != nil {
				continue
			}
			if net.ParseIP(pod.Status.PodIP) == nil || len(pod.Status.InitContainerStatuses) == 0 || pod.Status.InitContainerStatuses[0].Name != V1Alpha2PrepareIdentityContainer || pod.Status.InitContainerStatuses[0].State.Terminated == nil || pod.Status.InitContainerStatuses[0].State.Terminated.ExitCode != 0 {
				return nil, fmt.Errorf("managed Pod identity is not available")
			}
			key, err := DecodeV1Alpha2BootstrapReport(pod.Status.InitContainerStatuses[0].State.Terminated.Message)
			if err != nil {
				return nil, err
			}
			id, err := types.ManagedNodeID(pod.Metadata.UID)
			if err != nil {
				return nil, err
			}
			member := V1Alpha2RepairPodRequest{PodName: pod.Metadata.Name, PodUID: pod.Metadata.UID, NodeID: id, PublicKey: key, PeerURL: "quic://" + net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(peerPort))}
			bindingPlan := V1Alpha2RepairPlan{ClusterUID: ref.UID, Generation: p.Generation, DeploymentUID: p.DeploymentUID, Replacement: member}
			if _, err := ValidateV1Alpha2RepairPodBinding(ctx, store, ref, bindingPlan); err != nil {
				return nil, err
			}
			candidate := true
			for _, main := range pod.Status.ContainerStatuses {
				if main.RestartCount != 0 || main.State.Running != nil || main.State.Terminated != nil {
					candidate = false
				}
			}
			result = append(result, V1Alpha2ManagedPodObservation{Member: member, Candidate: candidate})
		}
		if page.Metadata.Continue == "" {
			break
		}
		query.Set("continue", page.Metadata.Continue)
	}
	slices.SortFunc(result, func(a, b V1Alpha2ManagedPodObservation) int {
		if a.Member.NodeID < b.Member.NodeID {
			return -1
		}
		if a.Member.NodeID > b.Member.NodeID {
			return 1
		}
		return 0
	})
	return result, nil
}
