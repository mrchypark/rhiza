package operator

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/fencing/runtimeagent"
)

// RuntimeAgentClient targets one daemonset pod by the authenticated Pod's
// NodeName. TLS still verifies the configured runtime-agent DNS identity.
type RuntimeAgentClient struct {
	Kube        *Kubernetes
	AuthorityID string
	TokenFile   string
	CA          []byte
	ServerName  string
	Port        string
}

func (a *RuntimeAgentClient) agent(ctx context.Context, nodeName string) (*runtimeagent.Client, error) {
	if a == nil || a.Kube == nil || a.AuthorityID == "" || a.TokenFile == "" || len(a.CA) == 0 || a.ServerName == "" || a.Port == "" || nodeName == "" {
		return nil, fmt.Errorf("fencing: complete node runtime-agent configuration required")
	}
	var node struct {
		Status struct {
			Addresses []struct {
				Type    string `json:"type"`
				Address string `json:"address"`
			} `json:"addresses"`
		} `json:"status"`
	}
	if err := a.Kube.Get(ctx, "/api/v1/nodes/"+url.PathEscape(nodeName), &node); err != nil {
		return nil, err
	}
	var address string
	for _, candidate := range node.Status.Addresses {
		if candidate.Type == "InternalIP" && net.ParseIP(candidate.Address) != nil {
			address = candidate.Address
			break
		}
	}
	if address == "" {
		return nil, fmt.Errorf("fencing: node has no valid internal IP")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(a.CA) {
		return nil, fmt.Errorf("fencing: runtime-agent CA has no certificate")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: a.ServerName}}
	return &runtimeagent.Client{BaseURL: "https://" + net.JoinHostPort(address, a.Port), TokenFile: a.TokenFile, HTTP: &http.Client{Transport: transport, Timeout: 30 * time.Second}}, nil
}

func (a *RuntimeAgentClient) Enroll(ctx context.Context, binding fencing.RuntimeBinding) (fencing.RuntimeBinding, error) {
	client, err := a.agent(ctx, binding.NodeName)
	if err != nil {
		return fencing.RuntimeBinding{}, err
	}
	return client.Enroll(ctx, binding)
}

func (a *RuntimeAgentClient) Stop(ctx context.Context, binding fencing.RuntimeBinding, request fencing.StopRequest) ([]byte, error) {
	client, err := a.agent(ctx, binding.NodeName)
	if err != nil {
		return nil, err
	}
	return client.Stop(ctx, binding, request)
}

func (a *RuntimeAgentClient) Verify(ctx context.Context, binding fencing.RuntimeBinding, request fencing.StopRequest, receipt []byte) error {
	client, err := a.agent(ctx, binding.NodeName)
	if err != nil {
		return err
	}
	return client.Verify(ctx, binding, request, receipt)
}

func (a *RuntimeAgentClient) enrollmentBase(ctx context.Context, namespace string, statefulSetUID string, identity fencing.Identity) (fencing.RuntimeBinding, error) {
	if a == nil || a.Kube == nil || a.AuthorityID == "" || namespace != a.Kube.Namespace {
		return fencing.RuntimeBinding{}, fmt.Errorf("fencing: runtime enrollment authority unavailable")
	}
	podName := identity.PodName
	if podName == "" {
		podName = identity.NodeID
	}
	var pod object
	if err := a.Kube.Get(ctx, (&Controller{Kube: a.Kube}).corePath("pods", podName), &pod); err != nil {
		return fencing.RuntimeBinding{}, err
	}
	if str(nested(pod, "metadata", "uid")) != identity.PodUID || nested(pod, "metadata", "deletionTimestamp") != nil || str(nested(pod, "spec", "nodeName")) == "" || str(nested(pod, "metadata", "annotations", fencingRootAnnotation)) == "" {
		return fencing.RuntimeBinding{}, fmt.Errorf("fencing: runtime enrollment Pod incarnation or root binding differs")
	}
	if err := validateSupervisedPod(pod); err != nil {
		return fencing.RuntimeBinding{}, err
	}
	containers := list(nested(pod, "status", "containerStatuses"))
	if len(containers) != 1 {
		return fencing.RuntimeBinding{}, fmt.Errorf("fencing: one live CRI container is required")
	}
	container := asObject(containers[0])
	containerID, imageID := str(container["containerID"]), str(container["imageID"])
	if str(container["name"]) != "rhiza" || !strings.HasPrefix(containerID, "containerd://") || len(strings.TrimPrefix(containerID, "containerd://")) < 32 || !strings.Contains(imageID, "@sha256:") {
		return fencing.RuntimeBinding{}, fmt.Errorf("fencing: Kubernetes container/image identity is incomplete")
	}
	var ns object
	if err := a.Kube.Get(ctx, "/api/v1/namespaces/"+url.PathEscape(namespace), &ns); err != nil {
		return fencing.RuntimeBinding{}, err
	}
	namespaceUID := str(nested(ns, "metadata", "uid"))
	if namespaceUID == "" {
		return fencing.RuntimeBinding{}, fmt.Errorf("fencing: namespace UID is unavailable")
	}
	return fencing.RuntimeBinding{AuthorityID: a.AuthorityID, NamespaceUID: namespaceUID, BindingUID: str(nested(pod, "metadata", "annotations", fencingRootAnnotation)),
		StatefulSetUID: statefulSetUID, SourceClusterID: identity.SourceClusterID, NodeID: identity.NodeID, PodName: podName, PodUID: identity.PodUID, RunID: identity.RunID, AssignmentID: identity.AssignmentID,
		NodeName: str(nested(pod, "spec", "nodeName")), ContainerID: containerID, ImageID: imageID}, nil
}
