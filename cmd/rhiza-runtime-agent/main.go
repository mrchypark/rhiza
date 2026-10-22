// rhiza-runtime-agent is the qualified node-local containerd/cgroup-v2 agent.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing/cri"
	"github.com/mrchypark/rhiza/pkg/fencing/runtimeagent"
	"github.com/mrchypark/rhiza/pkg/operator"
)

const runtimeAgentAudience = "rhiza-runtime-agent"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

func run(ctx context.Context, args []string) error {
	f := flag.NewFlagSet("rhiza-runtime-agent", flag.ContinueOnError)
	namespace := f.String("namespace", os.Getenv("POD_NAMESPACE"), "agent namespace")
	listen := f.String("listen", ":9445", "node-local HTTPS listener")
	cert := f.String("tls-cert", "/tls/tls.crt", "agent TLS certificate")
	key := f.String("tls-key", "/tls/tls.key", "agent TLS private key")
	stateDir := f.String("state-dir", "/var/lib/rhiza-runtime-agent", "durable node-local enrollment and receipt directory")
	socket := f.String("cri-socket", "/run/containerd/containerd.sock", "fixed local containerd CRI socket")
	containerName := f.String("container-name", "rhiza", "single enrolled database container name")
	runtimeVersion := f.String("containerd-version", "", "exact qualified containerd version")
	runtimeType := f.String("containerd-runtime-type", "io.containerd.runc.v2", "qualified containerd runtime type")
	profileID := f.String("cgroup-profile-id", "", "administrator-qualified private cgroup-v2 profile ID")
	kernelRelease := f.String("kernel-release", "", "exact qualified Linux kernel release")
	authorityUID := f.String("authority-deployment-uid", os.Getenv("AUTHORITY_DEPLOYMENT_UID"), "pinned immutable rhiza-fencer Deployment UID")
	grace := f.Int64("stop-grace-seconds", 3, "bounded CRI stop grace period")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || *namespace == "" || *runtimeVersion == "" || *runtimeType == "" || *profileID == "" || *kernelRelease == "" || *authorityUID == "" || *grace < 0 || *grace > 10 {
		return fmt.Errorf("runtime agent requires namespace, pinned authority Deployment UID, exact containerd/kernel versions, qualified cgroup profile, and valid grace period")
	}
	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		return fmt.Errorf("NODE_NAME is required")
	}
	kube, err := operator.NewInCluster(*namespace)
	if err != nil {
		return err
	}
	var node struct {
		Metadata struct {
			UID string `json:"uid"`
		} `json:"metadata"`
	}
	if err := kube.Get(ctx, "/api/v1/nodes/"+nodeName, &node); err != nil {
		return fmt.Errorf("read authenticated node UID: %w", err)
	}
	if node.Metadata.UID == "" {
		return fmt.Errorf("authenticated node UID is empty")
	}
	readHost := func(path string) (string, error) {
		data, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		value := strings.TrimSpace(string(data))
		if value == "" {
			return "", fmt.Errorf("host identity file %s is empty", path)
		}
		return value, nil
	}
	host := func(context.Context) (cri.HostIdentity, error) {
		machineID, err := readHost("/host/etc/machine-id")
		if err != nil {
			return cri.HostIdentity{}, err
		}
		bootID, err := readHost("/host/proc/sys/kernel/random/boot_id")
		if err != nil {
			return cri.HostIdentity{}, err
		}
		release, err := readHost("/host/proc/sys/kernel/osrelease")
		if err != nil {
			return cri.HostIdentity{}, err
		}
		return cri.HostIdentity{NodeName: nodeName, NodeUID: node.Metadata.UID, MachineID: machineID, BootID: bootID, KernelRelease: release}, nil
	}
	provider, err := cri.Open(*socket, cri.Profile{Namespace: *namespace, ContainerName: *containerName, RuntimeName: "containerd", RuntimeVersion: *runtimeVersion,
		RuntimeAPIVersion: "v1", ContainerdRuntimeType: *runtimeType, CgroupProfileID: *profileID, KernelRelease: *kernelRelease, GraceSeconds: *grace}, host)
	if err != nil {
		return err
	}
	defer provider.Close()
	service, err := runtimeagent.New(runtimeagent.Config{AuthorityID: "rhiza-fencer/" + *namespace, AuthorityNamespace: *namespace, AuthorityServiceAccount: "rhiza-fencer", AuthorityWorkloadUID: *authorityUID, Audience: runtimeAgentAudience,
		Directory: *stateDir, Auth: tokenReviewer{Kube: kube, Namespace: *namespace, ServiceAccount: "rhiza-fencer", DeploymentUID: *authorityUID}, Backend: &runtimeagent.CRIBackend{Provider: provider, ProcRoot: "/host/proc", CgroupRoot: "/host/sys/fs/cgroup"}})
	if err != nil {
		return err
	}
	server := &http.Server{Addr: *listen, Handler: service.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 35 * time.Second, WriteTimeout: 35 * time.Second,
		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}}
	defer server.Close()
	serving := make(chan error, 1)
	go func() { serving <- server.ListenAndServeTLS(*cert, *key) }()
	select {
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
		return ctx.Err()
	case err := <-serving:
		return err
	}
}

type tokenReviewer struct {
	Kube           *operator.Kubernetes
	Namespace      string
	ServiceAccount string
	DeploymentUID  string
}

func (t tokenReviewer) Authenticate(ctx context.Context, token string) (runtimeagent.Principal, error) {
	var result struct {
		Status struct {
			Authenticated bool     `json:"authenticated"`
			Audiences     []string `json:"audiences"`
			User          struct {
				Username string              `json:"username"`
				Extra    map[string][]string `json:"extra"`
			} `json:"user"`
		} `json:"status"`
	}
	if err := t.Kube.Post(ctx, "/apis/authentication.k8s.io/v1/tokenreviews", map[string]any{"apiVersion": "authentication.k8s.io/v1", "kind": "TokenReview",
		"spec": map[string]any{"token": token, "audiences": []string{runtimeAgentAudience}}}, &result); err != nil {
		return runtimeagent.Principal{}, err
	}
	if !result.Status.Authenticated || len(result.Status.Audiences) != 1 || result.Status.Audiences[0] != runtimeAgentAudience || result.Status.User.Username != "system:serviceaccount:"+t.Namespace+":"+t.ServiceAccount {
		return runtimeagent.Principal{}, fmt.Errorf("runtime authority token is unauthenticated, wrong audience, or wrong service account")
	}
	values := result.Status.User.Extra["authentication.kubernetes.io/pod-uid"]
	names := result.Status.User.Extra["authentication.kubernetes.io/pod-name"]
	if len(values) != 1 || values[0] == "" || len(names) != 1 || names[0] == "" {
		return runtimeagent.Principal{}, fmt.Errorf("runtime authority token lacks bound Pod name and UID")
	}
	var pod struct {
		Metadata struct {
			UID               string           `json:"uid"`
			DeletionTimestamp string           `json:"deletionTimestamp"`
			OwnerReferences   []ownerReference `json:"ownerReferences"`
		} `json:"metadata"`
		Spec struct {
			ServiceAccountName string `json:"serviceAccountName"`
		} `json:"spec"`
	}
	if err := t.Kube.Get(ctx, "/api/v1/namespaces/"+url.PathEscape(t.Namespace)+"/pods/"+url.PathEscape(names[0]), &pod); err != nil {
		return runtimeagent.Principal{}, err
	}
	if pod.Metadata.UID != values[0] || pod.Metadata.DeletionTimestamp != "" || pod.Spec.ServiceAccountName != t.ServiceAccount {
		return runtimeagent.Principal{}, fmt.Errorf("bound authority Pod is not live or differs")
	}
	rsOwner, ok := soleController(pod.Metadata.OwnerReferences, "ReplicaSet")
	if !ok {
		return runtimeagent.Principal{}, fmt.Errorf("authority Pod is not controlled by a ReplicaSet")
	}
	var rs struct {
		Metadata struct {
			UID             string           `json:"uid"`
			OwnerReferences []ownerReference `json:"ownerReferences"`
		} `json:"metadata"`
	}
	if err := t.Kube.Get(ctx, "/apis/apps/v1/namespaces/"+url.PathEscape(t.Namespace)+"/replicasets/"+url.PathEscape(rsOwner.Name), &rs); err != nil {
		return runtimeagent.Principal{}, err
	}
	if rs.Metadata.UID != rsOwner.UID {
		return runtimeagent.Principal{}, fmt.Errorf("authority ReplicaSet UID differs")
	}
	deploymentOwner, ok := soleController(rs.Metadata.OwnerReferences, "Deployment")
	if !ok || deploymentOwner.Name != "rhiza-fencer" || deploymentOwner.UID != t.DeploymentUID {
		return runtimeagent.Principal{}, fmt.Errorf("authority ReplicaSet is not owned by the pinned Deployment")
	}
	var deployment struct {
		Metadata struct {
			UID string `json:"uid"`
		} `json:"metadata"`
	}
	if err := t.Kube.Get(ctx, "/apis/apps/v1/namespaces/"+url.PathEscape(t.Namespace)+"/deployments/rhiza-fencer", &deployment); err != nil {
		return runtimeagent.Principal{}, err
	}
	if deployment.Metadata.UID != t.DeploymentUID {
		return runtimeagent.Principal{}, fmt.Errorf("authority Deployment UID differs")
	}
	return runtimeagent.Principal{Username: result.Status.User.Username, PodUID: values[0], WorkloadUID: deployment.Metadata.UID, Audiences: result.Status.Audiences}, nil
}

type ownerReference struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	UID        string `json:"uid"`
	Controller *bool  `json:"controller"`
}

func soleController(refs []ownerReference, kind string) (ownerReference, bool) {
	var found ownerReference
	count := 0
	for _, ref := range refs {
		if ref.Controller != nil && *ref.Controller {
			found, count = ref, count+1
		}
	}
	return found, count == 1 && found.Kind == kind && found.Name != "" && found.UID != ""
}
