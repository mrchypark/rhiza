// Package cri implements the local, exact-target CRI stop primitive. It is not
// an authenticated node-agent endpoint or a durable terminal-receipt authority.
package cri

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

var ErrUnverified = errors.New("fencing: CRI execution scope is unverified")

// HostIdentity must come from the host and authenticated node enrollment, never
// request parameters or the node-agent container's own machine/boot identity.
type HostIdentity struct {
	NodeName      string
	NodeUID       string
	MachineID     string
	BootID        string
	KernelRelease string
}

// Profile pins a runtime/kernel combination for subsequent qualification. Merely
// setting these strings does not qualify the combination or its process isolation.
type Profile struct {
	Namespace         string
	ContainerName     string
	RuntimeName       string
	RuntimeVersion    string
	RuntimeAPIVersion string
	KernelRelease     string
	GraceSeconds      int64
}

// False flags mean not verified, not proof that a scope is alive. This is a
// transport observation, not a durable or independently authenticated receipt.
type StopObservation struct {
	Runtime                   fencing.RuntimeBinding `json:"runtime"`
	Request                   fencing.StopRequest    `json:"request"`
	ContainerStopAcknowledged bool                   `json:"containerStopAcknowledged"`
	SandboxStopAcknowledged   bool                   `json:"sandboxStopAcknowledged"`
	ContainerExited           bool                   `json:"containerExited"`
	SandboxNotReady           bool                   `json:"sandboxNotReady"`
	ObservedAt                string                 `json:"observedAt,omitempty"`
}

type Provider struct {
	client  runtimeapi.RuntimeServiceClient
	conn    *grpc.ClientConn
	profile Profile
	host    func(context.Context) (HostIdentity, error)
}

// RuntimeIdentity binds enrollment to the exact CRI runtime/API versions, not
// just a runtime name that a later upgrade can silently reuse.
func RuntimeIdentity(name, version, api string) string {
	data, _ := json.Marshal([3]string{name, version, api})
	digest := sha256.Sum256(data)
	return "cri-v1-" + hex.EncodeToString(digest[:])
}

// Open uses a single administrator-configured local socket, never a socket or
// destination supplied by a termination request. No TCP, shell or PID API exists.
func Open(socket string, profile Profile, host func(context.Context) (HostIdentity, error)) (*Provider, error) {
	if !filepath.IsAbs(socket) || host == nil || profile.Namespace == "" || profile.ContainerName == "" ||
		profile.RuntimeName == "" || profile.RuntimeVersion == "" || profile.RuntimeAPIVersion != "v1" || profile.KernelRelease == "" ||
		profile.GraceSeconds < 0 || profile.GraceSeconds > 10 {
		return nil, fmt.Errorf("%w: incomplete local runtime profile", ErrUnverified)
	}
	conn, err := grpc.NewClient("passthrough:///local-cri",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "unix", socket)
		}),
		grpc.WithDisableRetry(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1024*1024), grpc.MaxCallSendMsgSize(64*1024)))
	if err != nil {
		return nil, err
	}
	return &Provider{client: runtimeapi.NewRuntimeServiceClient(conn), conn: conn, profile: profile, host: host}, nil
}

func (p *Provider) Close() error {
	if p == nil || p.conn == nil {
		return nil
	}
	return p.conn.Close()
}

func (p *Provider) verifyHost(ctx context.Context, binding fencing.RuntimeBinding) error {
	host, err := p.host(ctx)
	if err != nil {
		return fmt.Errorf("%w: read host identity: %w", ErrUnverified, err)
	}
	if host.NodeName != binding.NodeName || host.NodeUID != binding.NodeUID || host.MachineID != binding.MachineID || host.BootID != binding.BootID || host.KernelRelease != p.profile.KernelRelease {
		return fmt.Errorf("%w: enrolled node, machine, boot or kernel differs", ErrUnverified)
	}
	return nil
}

func (p *Provider) verifyVersion(ctx context.Context, binding fencing.RuntimeBinding) error {
	version, err := p.client.Version(ctx, &runtimeapi.VersionRequest{Version: "0.1.0"})
	if err != nil {
		return fmt.Errorf("%w: runtime version: %w", ErrUnverified, err)
	}
	if version == nil || version.RuntimeName != p.profile.RuntimeName || version.RuntimeVersion != p.profile.RuntimeVersion || version.RuntimeApiVersion != p.profile.RuntimeAPIVersion ||
		binding.RuntimeID != RuntimeIdentity(version.RuntimeName, version.RuntimeVersion, version.RuntimeApiVersion) {
		return fmt.Errorf("%w: enrolled runtime/API version differs", ErrUnverified)
	}
	return nil
}

func (p *Provider) inspect(ctx context.Context, binding fencing.RuntimeBinding, containerID string, stopped bool) error {
	sandbox, err := p.client.PodSandboxStatus(ctx, &runtimeapi.PodSandboxStatusRequest{PodSandboxId: binding.SandboxID})
	if err != nil {
		return fmt.Errorf("%w: sandbox status: %w", ErrUnverified, err)
	}
	if sandbox == nil || sandbox.Status == nil || sandbox.Status.Id != binding.SandboxID || sandbox.Status.Metadata == nil ||
		sandbox.Status.Metadata.Uid != binding.PodUID || sandbox.Status.Metadata.Namespace != p.profile.Namespace || sandbox.Status.CreatedAt <= 0 ||
		(sandbox.Status.State != runtimeapi.PodSandboxState_SANDBOX_READY && sandbox.Status.State != runtimeapi.PodSandboxState_SANDBOX_NOTREADY) ||
		sandbox.Status.Linux == nil || sandbox.Status.Linux.Namespaces == nil || sandbox.Status.Linux.Namespaces.Options == nil {
		return fmt.Errorf("%w: sandbox enrollment or namespace metadata differs", ErrUnverified)
	}
	ns := sandbox.Status.Linux.Namespaces.Options
	if ns.Pid != runtimeapi.NamespaceMode_CONTAINER || ns.Network != runtimeapi.NamespaceMode_POD || ns.Ipc != runtimeapi.NamespaceMode_POD || ns.TargetId != "" {
		return fmt.Errorf("%w: private execution namespaces are required", ErrUnverified)
	}
	container, err := p.client.ContainerStatus(ctx, &runtimeapi.ContainerStatusRequest{ContainerId: containerID})
	if err != nil {
		return fmt.Errorf("%w: container status: %w", ErrUnverified, err)
	}
	if container == nil || container.Status == nil || container.Status.Id != containerID || container.Status.Metadata == nil ||
		container.Status.Metadata.Name != p.profile.ContainerName || container.Status.ImageRef != binding.ImageID || container.Status.CreatedAt <= 0 || !knownContainerState(container.Status.State) {
		return fmt.Errorf("%w: container enrollment differs", ErrUnverified)
	}
	containers, err := p.client.ListContainers(ctx, &runtimeapi.ListContainersRequest{Filter: &runtimeapi.ContainerFilter{PodSandboxId: binding.SandboxID}})
	if err != nil {
		return fmt.Errorf("%w: sandbox container inventory: %w", ErrUnverified, err)
	}
	if containers == nil {
		return fmt.Errorf("%w: sandbox inventory is missing", ErrUnverified)
	}
	seen := map[string]bool{}
	found := false
	for _, entry := range containers.Containers {
		if entry == nil || entry.Id == "" || seen[entry.Id] || entry.PodSandboxId != binding.SandboxID || entry.Metadata == nil || entry.Metadata.Name != p.profile.ContainerName || !knownContainerState(entry.State) {
			return fmt.Errorf("%w: sandbox has unknown or duplicate container scope", ErrUnverified)
		}
		seen[entry.Id] = true
		if entry.Id == containerID {
			found = true
			if entry.ImageRef != binding.ImageID {
				return fmt.Errorf("%w: target container inventory differs", ErrUnverified)
			}
		}
		if stopped && entry.State != runtimeapi.ContainerState_CONTAINER_EXITED {
			return fmt.Errorf("%w: sandbox still has nonterminal execution scope", ErrUnverified)
		}
	}
	if !found {
		return fmt.Errorf("%w: target container is outside the captured sandbox", ErrUnverified)
	}
	if stopped && (container.Status.State != runtimeapi.ContainerState_CONTAINER_EXITED || container.Status.FinishedAt <= 0 || sandbox.Status.State != runtimeapi.PodSandboxState_SANDBOX_NOTREADY) {
		return fmt.Errorf("%w: runtime has not reached the terminal state", ErrUnverified)
	}
	return nil
}

func knownContainerState(state runtimeapi.ContainerState) bool {
	return state == runtimeapi.ContainerState_CONTAINER_CREATED || state == runtimeapi.ContainerState_CONTAINER_RUNNING || state == runtimeapi.ContainerState_CONTAINER_EXITED
}

// Stop performs the CRI primitive only. Even a successful observation needs
// authenticated enrollment, durable receipt persistence/verification, qualified
// descendant containment and return prevention before it can authorize fencing.
// A partial observation on error preserves acknowledged boundaries, not success.
func (p *Provider) Stop(ctx context.Context, binding fencing.RuntimeBinding, request fencing.StopRequest) (StopObservation, error) {
	observation := StopObservation{Runtime: binding, Request: request}
	if p == nil || p.client == nil || p.host == nil {
		return observation, fmt.Errorf("%w: local runtime provider is not configured", ErrUnverified)
	}
	decoded, err := hex.DecodeString(request.RequestHash)
	if binding.Validate(request.Identity) != nil || request.OperationID == "" || err != nil || len(decoded) != sha256.Size {
		return observation, fmt.Errorf("%w: invalid enrolled stop request", ErrUnverified)
	}
	containerID := strings.TrimPrefix(binding.ContainerID, p.profile.RuntimeName+"://")
	if containerID == "" || strings.Contains(containerID, "://") {
		return observation, fmt.Errorf("%w: runtime container ID differs", ErrUnverified)
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := p.verifyHost(ctx, binding); err != nil {
		return observation, err
	}
	if err := p.verifyVersion(ctx, binding); err != nil {
		return observation, err
	}
	if err := p.inspect(ctx, binding, containerID, false); err != nil {
		return observation, err
	}
	if err := p.verifyHost(ctx, binding); err != nil {
		return observation, err
	}
	ack, err := p.client.StopContainer(ctx, &runtimeapi.StopContainerRequest{ContainerId: containerID, Timeout: p.profile.GraceSeconds})
	if err != nil {
		return observation, fmt.Errorf("%w: container stop acknowledgement is uncertain: %w", ErrUnverified, err)
	}
	if ack == nil {
		return observation, fmt.Errorf("%w: container stop acknowledgement is missing", ErrUnverified)
	}
	observation.ContainerStopAcknowledged = true
	if err := p.verifyHost(ctx, binding); err != nil {
		return observation, err
	}
	sandboxAck, err := p.client.StopPodSandbox(ctx, &runtimeapi.StopPodSandboxRequest{PodSandboxId: binding.SandboxID})
	if err != nil {
		return observation, fmt.Errorf("%w: sandbox stop acknowledgement is uncertain: %w", ErrUnverified, err)
	}
	if sandboxAck == nil {
		return observation, fmt.Errorf("%w: sandbox stop acknowledgement is missing", ErrUnverified)
	}
	observation.SandboxStopAcknowledged = true
	if err := p.inspect(ctx, binding, containerID, true); err != nil {
		return observation, err
	}
	if err := p.verifyVersion(ctx, binding); err != nil {
		return observation, err
	}
	if err := p.verifyHost(ctx, binding); err != nil {
		return observation, err
	}
	observation.ContainerExited, observation.SandboxNotReady = true, true
	observation.ObservedAt = time.Now().UTC().Format(time.RFC3339Nano)
	return observation, nil
}
