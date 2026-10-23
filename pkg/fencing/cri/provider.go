// Package cri implements the local, exact-target CRI stop primitive. It is not
// an authenticated node-agent endpoint or a durable terminal-receipt authority.
package cri

import (
	"context"
	"crypto/sha256"
	"crypto/sha512"
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
	Namespace             string
	ContainerName         string
	RuntimeName           string
	RuntimeVersion        string
	RuntimeAPIVersion     string
	ContainerdRuntimeType string
	// CgroupProfileID names an administrator-qualified private cgroup-v2
	// profile; a non-empty value is an assertion, not proof of no-escape.
	CgroupProfileID string
	KernelRelease   string
	GraceSeconds    int64
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
	CgroupScope               CgroupScope            `json:"cgroupScope,omitempty"`
	CgroupEmpty               bool                   `json:"cgroupEmpty,omitempty"`
	RecoveredTerminalState    bool                   `json:"recoveredTerminalState,omitempty"`
	ObservedAt                string                 `json:"observedAt,omitempty"`
}

type Provider struct {
	client      runtimeapi.RuntimeServiceClient
	imageClient runtimeapi.ImageServiceClient
	conn        *grpc.ClientConn
	profile     Profile
	host        func(context.Context) (HostIdentity, error)
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
	return &Provider{client: runtimeapi.NewRuntimeServiceClient(conn), imageClient: runtimeapi.NewImageServiceClient(conn), conn: conn, profile: profile, host: host}, nil
}

func validOCIDigest(value string) bool {
	algorithm, encoded, ok := strings.Cut(value, ":")
	if !ok {
		return false
	}
	wantLength := 0
	switch algorithm {
	case "sha256":
		wantLength = sha256.Size * 2
	case "sha384":
		wantLength = sha512.Size384 * 2
	case "sha512":
		wantLength = sha512.Size * 2
	default:
		return false
	}
	if len(encoded) != wantLength {
		return false
	}
	for _, char := range encoded {
		if !('0' <= char && char <= '9') && !('a' <= char && char <= 'f') {
			return false
		}
	}
	return true
}

func validOCIRepoDigest(value string) bool {
	repository, digest, ok := strings.Cut(value, "@")
	if !ok || repository == "" || strings.Contains(digest, "@") || !validOCIDigest(digest) || strings.ContainsAny(repository, " \t\r\n") {
		return false
	}
	lastSlash := strings.LastIndexByte(repository, '/')
	lastComponent := repository[lastSlash+1:]
	return lastComponent != "" && !strings.Contains(lastComponent, ":")
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

// EnrollBinding fills node/runtime/sandbox identity from this node and its
// pinned CRI, then captures the populated private cgroup before the supervisor
// starts its application child. Kubernetes-supplied container/image IDs are
// treated as expected identities and must match CRI exactly.
func (p *Provider) EnrollBinding(ctx context.Context, base fencing.RuntimeBinding, procRoot, cgroupRoot string) (fencing.RuntimeBinding, CgroupScope, error) {
	binding := base
	identity := fencing.Identity{SourceClusterID: base.SourceClusterID, NodeID: base.NodeID, PodUID: base.PodUID, RunID: base.RunID}
	if p == nil || p.client == nil || p.host == nil || p.profile.RuntimeName != "containerd" || p.profile.ContainerdRuntimeType == "" || p.profile.CgroupProfileID == "" ||
		base.AuthorityID == "" || base.NamespaceUID == "" || base.BindingUID == "" || base.StatefulSetUID == "" || base.NodeName == "" || base.ContainerID == "" || base.ImageID == "" ||
		identity.SourceClusterID == "" || identity.NodeID == "" || identity.PodUID == "" || identity.RunID == "" {
		return fencing.RuntimeBinding{}, CgroupScope{}, fmt.Errorf("%w: incomplete qualified runtime enrollment", ErrUnverified)
	}
	if p.profile.RuntimeAPIVersion != "v1" || !filepath.IsAbs(procRoot) || !filepath.IsAbs(cgroupRoot) {
		return fencing.RuntimeBinding{}, CgroupScope{}, fmt.Errorf("%w: unsupported runtime enrollment profile", ErrUnverified)
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	host, err := p.host(ctx)
	if err != nil || host.NodeName != base.NodeName || host.NodeUID == "" || host.MachineID == "" || host.BootID == "" || host.KernelRelease != p.profile.KernelRelease {
		return fencing.RuntimeBinding{}, CgroupScope{}, fmt.Errorf("%w: local node or boot identity differs", ErrUnverified)
	}
	binding.NodeUID, binding.MachineID, binding.BootID = host.NodeUID, host.MachineID, host.BootID
	version, err := p.client.Version(ctx, &runtimeapi.VersionRequest{Version: "0.1.0"})
	if err != nil || version == nil || version.RuntimeName != p.profile.RuntimeName || version.RuntimeVersion != p.profile.RuntimeVersion || version.RuntimeApiVersion != p.profile.RuntimeAPIVersion {
		return fencing.RuntimeBinding{}, CgroupScope{}, fmt.Errorf("%w: runtime version is not the qualified profile", ErrUnverified)
	}
	binding.RuntimeID = RuntimeIdentity(version.RuntimeName, version.RuntimeVersion, version.RuntimeApiVersion)
	containerID, ok := strings.CutPrefix(base.ContainerID, "containerd://")
	if !ok || containerID == "" || strings.Contains(containerID, "://") {
		return fencing.RuntimeBinding{}, CgroupScope{}, fmt.Errorf("%w: container ID is not containerd-qualified", ErrUnverified)
	}
	status, err := p.client.ContainerStatus(ctx, &runtimeapi.ContainerStatusRequest{ContainerId: containerID, Verbose: true})
	if err != nil || status == nil || status.Status == nil || status.Status.Id != containerID || status.Status.Metadata == nil || status.Status.Metadata.Name != p.profile.ContainerName ||
		status.Status.ImageRef != base.ImageID || status.Status.State != runtimeapi.ContainerState_CONTAINER_RUNNING {
		return fencing.RuntimeBinding{}, CgroupScope{}, fmt.Errorf("%w: pre-child container metadata differs", ErrUnverified)
	}
	criIdentity, err := containerdVerboseRuntimeIdentity(status.Info, "", p.profile.ContainerdRuntimeType)
	if err != nil {
		return fencing.RuntimeBinding{}, CgroupScope{}, err
	}
	binding.SandboxID = criIdentity.SandboxID
	if err := p.verifyHost(ctx, binding); err != nil {
		return fencing.RuntimeBinding{}, CgroupScope{}, err
	}
	if err := p.verifyVersion(ctx, binding); err != nil {
		return fencing.RuntimeBinding{}, CgroupScope{}, err
	}
	if err := p.inspect(ctx, binding, containerID, false); err != nil {
		return fencing.RuntimeBinding{}, CgroupScope{}, err
	}
	scope, err := captureCgroupScope(procRoot, cgroupRoot, criIdentity.PID, criIdentity.CgroupsPath, binding, p.profile.CgroupProfileID)
	if err != nil {
		return fencing.RuntimeBinding{}, CgroupScope{}, err
	}
	secondPID, secondPath, err := p.containerdPID(ctx, binding, containerID)
	if err != nil || secondPID != criIdentity.PID || secondPath != criIdentity.CgroupsPath || scope.VerifyProcess(procRoot, cgroupRoot) != nil {
		return fencing.RuntimeBinding{}, CgroupScope{}, fmt.Errorf("%w: process changed during pre-child capture", ErrUnverified)
	}
	if err := p.inspect(ctx, binding, containerID, false); err != nil {
		return fencing.RuntimeBinding{}, CgroupScope{}, err
	}
	if err := p.verifyHost(ctx, binding); err != nil {
		return fencing.RuntimeBinding{}, CgroupScope{}, err
	}
	if err := p.verifyVersion(ctx, binding); err != nil {
		return fencing.RuntimeBinding{}, CgroupScope{}, err
	}
	if err := binding.Validate(identity); err != nil {
		return fencing.RuntimeBinding{}, CgroupScope{}, err
	}
	return binding, scope, nil
}

// CaptureCgroupScope maps a containerd verbose-status PID to its live host
// cgroup. The exact containerd runtime/API version and runtime type must be
// pinned in Profile; CRI's generic Info map is not accepted for other runtimes.
func (p *Provider) CaptureCgroupScope(ctx context.Context, binding fencing.RuntimeBinding, procRoot, cgroupRoot string) (CgroupScope, error) {
	identity := fencing.Identity{SourceClusterID: binding.SourceClusterID, NodeID: binding.NodeID, PodUID: binding.PodUID, RunID: binding.RunID}
	if p == nil || p.client == nil || p.host == nil || p.profile.RuntimeName != "containerd" || p.profile.ContainerdRuntimeType == "" || p.profile.CgroupProfileID == "" || binding.Validate(identity) != nil {
		return CgroupScope{}, fmt.Errorf("%w: qualified containerd cgroup capture is not configured", ErrUnverified)
	}
	containerID := strings.TrimPrefix(binding.ContainerID, p.profile.RuntimeName+"://")
	if containerID == "" || strings.Contains(containerID, "://") {
		return CgroupScope{}, fmt.Errorf("%w: runtime container ID differs", ErrUnverified)
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := p.verifyHost(ctx, binding); err != nil {
		return CgroupScope{}, err
	}
	if err := p.verifyVersion(ctx, binding); err != nil {
		return CgroupScope{}, err
	}
	if err := p.inspect(ctx, binding, containerID, false); err != nil {
		return CgroupScope{}, err
	}
	pid, cgroupsPath, err := p.containerdPID(ctx, binding, containerID)
	if err != nil {
		return CgroupScope{}, err
	}
	scope, err := captureCgroupScope(procRoot, cgroupRoot, pid, cgroupsPath, binding, p.profile.CgroupProfileID)
	if err != nil {
		return CgroupScope{}, err
	}
	secondPID, secondPath, err := p.containerdPID(ctx, binding, containerID)
	if err != nil || secondPID != pid || secondPath != cgroupsPath || scope.VerifyProcess(procRoot, cgroupRoot) != nil {
		return CgroupScope{}, fmt.Errorf("%w: containerd PID changed during cgroup capture", ErrUnverified)
	}
	if err := p.inspect(ctx, binding, containerID, false); err != nil {
		return CgroupScope{}, err
	}
	if err := p.verifyHost(ctx, binding); err != nil {
		return CgroupScope{}, err
	}
	if err := p.verifyVersion(ctx, binding); err != nil {
		return CgroupScope{}, err
	}
	return scope, nil
}

func (p *Provider) containerdPID(ctx context.Context, binding fencing.RuntimeBinding, containerID string) (uint32, string, error) {
	response, err := p.client.ContainerStatus(ctx, &runtimeapi.ContainerStatusRequest{ContainerId: containerID, Verbose: true})
	if err != nil || response == nil || response.Status == nil || response.Status.Id != containerID || response.Status.Metadata == nil ||
		response.Status.Metadata.Name != p.profile.ContainerName || response.Status.ImageRef != binding.ImageID || response.Status.State != runtimeapi.ContainerState_CONTAINER_RUNNING {
		return 0, "", fmt.Errorf("%w: verbose container status identity differs", ErrUnverified)
	}
	identity, err := containerdVerboseRuntimeIdentity(response.Info, binding.SandboxID, p.profile.ContainerdRuntimeType)
	if err != nil {
		return 0, "", err
	}
	return identity.PID, identity.CgroupsPath, nil
}

func (p *Provider) inspect(ctx context.Context, binding fencing.RuntimeBinding, containerID string, stopped bool) error {
	return p.inspectState(ctx, binding, containerID, stopped, 0, false)
}

// inspectState can distinguish the crash window after the exact container has
// exited but before its captured sandbox has been stopped.
func (p *Provider) inspectState(ctx context.Context, binding fencing.RuntimeBinding, containerID string, stopped bool, sandboxState runtimeapi.PodSandboxState, checkSandboxState bool) error {
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
		container.Status.Metadata.Name != p.profile.ContainerName || container.Status.ImageRef != binding.ImageID ||
		container.Status.CreatedAt <= 0 || !knownContainerState(container.Status.State) {
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
			if err := p.verifyInventoryImage(ctx, entry, container.Status, binding.ImageID); err != nil {
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
	if stopped && (container.Status.State != runtimeapi.ContainerState_CONTAINER_EXITED || container.Status.FinishedAt <= 0 ||
		(!checkSandboxState && sandbox.Status.State != runtimeapi.PodSandboxState_SANDBOX_NOTREADY)) {
		return fmt.Errorf("%w: runtime has not reached the terminal state", ErrUnverified)
	}
	if checkSandboxState && sandbox.Status.State != sandboxState {
		return fmt.Errorf("%w: sandbox is not in the expected partial-stop state", ErrUnverified)
	}
	return nil
}

// verifyInventoryImage keeps the Pod-bound digest check on ContainerStatus
// while handling CRI implementations that omit ImageId in both responses.
func (p *Provider) verifyInventoryImage(ctx context.Context, listed *runtimeapi.Container, status *runtimeapi.ContainerStatus, boundImageRef string) error {
	if listed.ImageId != "" && status.ImageId != "" {
		if listed.ImageId != status.ImageId {
			return ErrUnverified
		}
		return nil
	}
	if listed.ImageId != "" || status.ImageId != "" || !validOCIDigest(listed.ImageRef) || !validOCIRepoDigest(boundImageRef) || p.imageClient == nil {
		return ErrUnverified
	}
	response, err := p.imageClient.ImageStatus(ctx, &runtimeapi.ImageStatusRequest{Image: &runtimeapi.ImageSpec{Image: listed.ImageRef}})
	if err != nil || response == nil || response.Image == nil || response.Image.Id != listed.ImageRef {
		return ErrUnverified
	}
	for _, repoDigest := range response.Image.RepoDigests {
		if repoDigest == boundImageRef {
			return nil
		}
	}
	return ErrUnverified
}

func knownContainerState(state runtimeapi.ContainerState) bool {
	return state == runtimeapi.ContainerState_CONTAINER_CREATED || state == runtimeapi.ContainerState_CONTAINER_RUNNING || state == runtimeapi.ContainerState_CONTAINER_EXITED
}

// Stop performs the CRI primitive only. Even a successful observation needs
// authenticated enrollment, durable receipt persistence/verification, qualified
// descendant containment and return prevention before it can authorize fencing.
// A partial observation on error preserves acknowledged boundaries, not success.
func (p *Provider) Stop(ctx context.Context, binding fencing.RuntimeBinding, request fencing.StopRequest) (StopObservation, error) {
	return p.stop(ctx, binding, request, nil, nil)
}

// stop leaves a container-stopped/sandbox-ready scope pending unless its exact
// CRI inventory is terminal before the sandbox stop is attempted.
func (p *Provider) stop(ctx context.Context, binding fencing.RuntimeBinding, request fencing.StopRequest, beforeContainerStop, beforeSandboxStop func(context.Context) error) (StopObservation, error) {
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
	if beforeContainerStop != nil {
		if err := beforeContainerStop(ctx); err != nil {
			return observation, err
		}
	}
	ack, err := p.client.StopContainer(ctx, &runtimeapi.StopContainerRequest{ContainerId: containerID, Timeout: p.profile.GraceSeconds})
	if err != nil {
		return observation, fmt.Errorf("%w: container stop acknowledgement is uncertain: %w", ErrUnverified, err)
	}
	if ack == nil {
		return observation, fmt.Errorf("%w: container stop acknowledgement is missing", ErrUnverified)
	}
	observation.ContainerStopAcknowledged = true
	if err := p.inspectState(ctx, binding, containerID, true, runtimeapi.PodSandboxState_SANDBOX_READY, true); err != nil {
		return observation, err
	}
	if beforeSandboxStop != nil {
		if err := beforeSandboxStop(ctx); err != nil {
			return observation, err
		}
	}
	if err := p.verifyVersion(ctx, binding); err != nil {
		return observation, err
	}
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

// StopInCgroup is the qualified Linux path: it requires the exact enrolled
// cgroup inode to be populated before stopping and empty afterward. CgroupScope
// must have been captured from this container's host PID before child startup
// and persisted by the authenticated agent under the immutable binding.
func (p *Provider) StopInCgroup(ctx context.Context, binding fencing.RuntimeBinding, request fencing.StopRequest, procRoot, cgroupRoot string, scope CgroupScope) (StopObservation, error) {
	observation := StopObservation{Runtime: binding, Request: request}
	if p == nil || p.profile.CgroupProfileID == "" || binding.Validate(request.Identity) != nil || scope.RuntimeHash != fencing.RuntimeScopeBindingDigest(binding) || scope.ProfileID != p.profile.CgroupProfileID {
		return observation, fmt.Errorf("%w: enrolled cgroup belongs to another runtime binding", ErrUnverified)
	}
	beforeContainerStop := func(ctx context.Context) error {
		containerID := strings.TrimPrefix(binding.ContainerID, p.profile.RuntimeName+"://")
		pid, cgroupsPath, err := p.containerdPID(ctx, binding, containerID)
		if err != nil || pid != scope.PID {
			return fmt.Errorf("%w: CRI process identity differs from captured scope", ErrUnverified)
		}
		mappedPath, err := containerdCgroupsPath(cgroupsPath, containerID)
		if err != nil || mappedPath != scope.Path {
			return fmt.Errorf("%w: CRI cgroups path differs from captured scope", ErrUnverified)
		}
		return scope.VerifyProcess(procRoot, cgroupRoot)
	}
	observation, err := p.stop(ctx, binding, request, beforeContainerStop, func(context.Context) error { return scope.VerifyEmpty(cgroupRoot) })
	if err != nil {
		return observation, err
	}
	if err := scope.VerifyEmpty(cgroupRoot); err != nil {
		return observation, err
	}
	observation.CgroupScope, observation.CgroupEmpty = scope, true
	observation.ObservedAt = time.Now().UTC().Format(time.RFC3339Nano)
	return observation, nil
}

// ResumeStopInCgroup resumes only a previously durable exact stop intent. It
// either reissues Stop for the same PID/starttime and cgroup, or reconstructs
// a receipt from complete exact CRI terminal metadata plus the same empty
// cgroup inode. Missing CRI metadata, a changed process, or partial terminal
// observations remain unverified.
func (p *Provider) ResumeStopInCgroup(ctx context.Context, binding fencing.RuntimeBinding, request fencing.StopRequest, procRoot, cgroupRoot string, scope CgroupScope) (StopObservation, error) {
	observation := StopObservation{Runtime: binding, Request: request, CgroupScope: scope}
	digest, digestErr := hex.DecodeString(request.RequestHash)
	if p == nil || p.client == nil || p.host == nil || p.profile.CgroupProfileID == "" || binding.Validate(request.Identity) != nil || request.OperationID == "" || digestErr != nil || len(digest) != sha256.Size ||
		scope.RuntimeHash != fencing.RuntimeScopeBindingDigest(binding) || scope.ProfileID != p.profile.CgroupProfileID {
		return observation, fmt.Errorf("%w: resumed stop intent differs from captured scope", ErrUnverified)
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := p.verifyHost(ctx, binding); err != nil {
		return observation, err
	}
	if err := p.verifyVersion(ctx, binding); err != nil {
		return observation, err
	}
	containerID := strings.TrimPrefix(binding.ContainerID, p.profile.RuntimeName+"://")
	if containerID == "" || strings.Contains(containerID, "://") {
		return observation, fmt.Errorf("%w: runtime container ID differs", ErrUnverified)
	}
	if p.inspect(ctx, binding, containerID, true) == nil && scope.VerifyEmpty(cgroupRoot) == nil {
		if err := p.verifyHost(ctx, binding); err != nil {
			return observation, err
		}
		if err := p.verifyVersion(ctx, binding); err != nil {
			return observation, err
		}
		observation.ContainerExited, observation.SandboxNotReady = true, true
		observation.CgroupEmpty, observation.RecoveredTerminalState = true, true
		observation.ObservedAt = time.Now().UTC().Format(time.RFC3339Nano)
		return observation, nil
	}
	// A durable exact intent may have stopped this container before a crash
	// interrupted the second CRI effect. Finish only that enrolled sandbox after
	// rechecking its UID, container inventory, runtime and captured empty cgroup.
	if p.inspectState(ctx, binding, containerID, true, runtimeapi.PodSandboxState_SANDBOX_READY, true) == nil && scope.VerifyEmpty(cgroupRoot) == nil {
		if err := p.verifyVersion(ctx, binding); err != nil {
			return observation, err
		}
		if err := p.verifyHost(ctx, binding); err != nil {
			return observation, err
		}
		ack, err := p.client.StopPodSandbox(ctx, &runtimeapi.StopPodSandboxRequest{PodSandboxId: binding.SandboxID})
		if err != nil || ack == nil {
			return observation, fmt.Errorf("%w: resumed sandbox stop is unconfirmed: %v", ErrUnverified, err)
		}
		if err := p.inspect(ctx, binding, containerID, true); err != nil {
			return observation, err
		}
		if err := scope.VerifyEmpty(cgroupRoot); err != nil {
			return observation, err
		}
		if err := p.verifyVersion(ctx, binding); err != nil {
			return observation, err
		}
		if err := p.verifyHost(ctx, binding); err != nil {
			return observation, err
		}
		observation.ContainerExited, observation.SandboxNotReady = true, true
		observation.CgroupEmpty, observation.RecoveredTerminalState = true, true
		observation.ObservedAt = time.Now().UTC().Format(time.RFC3339Nano)
		return observation, nil
	}
	// A nonterminal retry may reissue the exact CRI operation only while the
	// originally captured process incarnation still occupies the pinned scope.
	if err := scope.VerifyProcess(procRoot, cgroupRoot); err != nil {
		return observation, err
	}
	return p.StopInCgroup(ctx, binding, request, procRoot, cgroupRoot, scope)
}
