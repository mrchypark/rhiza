package cri

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type fakeRuntime struct {
	runtimeapi.UnimplementedRuntimeServiceServer
	runtimeapi.UnimplementedImageServiceServer
	mu                      sync.Mutex
	version                 *runtimeapi.VersionResponse
	sandbox                 *runtimeapi.PodSandboxStatus
	container               *runtimeapi.ContainerStatus
	verboseInfo             map[string]string
	containers              []*runtimeapi.Container
	imageStatus             *runtimeapi.ImageStatusResponse
	imageStatusError        codes.Code
	imageStatusRequests     []string
	host                    HostIdentity
	stopContainers          int
	stopSandboxes           int
	versionCalls            int
	listCalls               int
	changeVersionAfterStop  bool
	changeVersionOnListCall int
	statusError             codes.Code
	stopError               codes.Code
	sandboxError            codes.Code
	sandboxErrorOnce        bool
	stillRunning            bool
	descendantRunning       bool
	metadataGone            bool
	blockStop               bool
	changeHostAfterStop     bool
	directory               string
	scopeEventsFile         string
	stopEntered             chan struct{}
	stopEnteredOnce         sync.Once
}

func (f *fakeRuntime) Version(context.Context, *runtimeapi.VersionRequest) (*runtimeapi.VersionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.versionCalls++
	return proto.Clone(f.version).(*runtimeapi.VersionResponse), nil
}

func (f *fakeRuntime) PodSandboxStatus(_ context.Context, request *runtimeapi.PodSandboxStatusRequest) (*runtimeapi.PodSandboxStatusResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.statusError != codes.OK {
		return nil, status.Error(f.statusError, "test sandbox unavailable")
	}
	if f.metadataGone && f.stopSandboxes != 0 {
		return nil, status.Error(codes.NotFound, "test metadata removed")
	}
	if request.PodSandboxId != "sandbox-0" {
		return nil, status.Error(codes.NotFound, "wrong sandbox ID")
	}
	return &runtimeapi.PodSandboxStatusResponse{Status: proto.Clone(f.sandbox).(*runtimeapi.PodSandboxStatus)}, nil
}

func (f *fakeRuntime) ContainerStatus(_ context.Context, request *runtimeapi.ContainerStatusRequest) (*runtimeapi.ContainerStatusResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if request.ContainerId != "container-0" {
		return nil, status.Error(codes.NotFound, "wrong container ID")
	}
	response := &runtimeapi.ContainerStatusResponse{Status: proto.Clone(f.container).(*runtimeapi.ContainerStatus)}
	if request.Verbose {
		response.Info = make(map[string]string, len(f.verboseInfo))
		for key, value := range f.verboseInfo {
			response.Info[key] = value
		}
	}
	return response, nil
}

func (f *fakeRuntime) ListContainers(_ context.Context, request *runtimeapi.ListContainersRequest) (*runtimeapi.ListContainersResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if request.Filter == nil || request.Filter.PodSandboxId != "sandbox-0" {
		return nil, status.Error(codes.InvalidArgument, "unscoped list is forbidden")
	}
	result := &runtimeapi.ListContainersResponse{}
	for _, container := range f.containers {
		result.Containers = append(result.Containers, proto.Clone(container).(*runtimeapi.Container))
	}
	f.listCalls++
	if f.changeVersionOnListCall == f.listCalls {
		f.version.RuntimeVersion = "changed-version"
	}
	return result, nil
}

func (f *fakeRuntime) ImageStatus(_ context.Context, request *runtimeapi.ImageStatusRequest) (*runtimeapi.ImageStatusResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if request.Image == nil || request.Image.Image == "" {
		return nil, status.Error(codes.InvalidArgument, "unqualified image lookup")
	}
	f.imageStatusRequests = append(f.imageStatusRequests, request.Image.Image)
	if f.imageStatusError != codes.OK {
		return nil, status.Error(f.imageStatusError, "test image status unavailable")
	}
	if f.imageStatus == nil {
		return &runtimeapi.ImageStatusResponse{}, nil
	}
	return proto.Clone(f.imageStatus).(*runtimeapi.ImageStatusResponse), nil
}

func (f *fakeRuntime) StopContainer(ctx context.Context, request *runtimeapi.StopContainerRequest) (*runtimeapi.StopContainerResponse, error) {
	f.mu.Lock()
	f.stopContainers++
	if request.ContainerId != "container-0" || request.Timeout != 3 {
		f.mu.Unlock()
		return nil, status.Error(codes.InvalidArgument, "wrong stop target or grace")
	}
	if f.blockStop {
		f.stopEnteredOnce.Do(func() { close(f.stopEntered) })
		f.mu.Unlock()
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	defer f.mu.Unlock()
	if !f.stillRunning {
		f.container.State, f.container.FinishedAt = runtimeapi.ContainerState_CONTAINER_EXITED, 20
		if len(f.containers) != 0 {
			f.containers[0].State = runtimeapi.ContainerState_CONTAINER_EXITED
		}
		if f.scopeEventsFile != "" && !f.descendantRunning {
			if err := os.WriteFile(f.scopeEventsFile, []byte("populated 0\nfrozen 0\n"), 0600); err != nil {
				return nil, status.Error(codes.Internal, "test cgroup update failed")
			}
		}
	}
	if f.changeVersionAfterStop {
		f.version.RuntimeVersion = "changed-version"
	}
	if f.stopError != codes.OK {
		return nil, status.Error(f.stopError, "test stop result unknown")
	}
	return &runtimeapi.StopContainerResponse{}, nil
}

func (f *fakeRuntime) StopPodSandbox(_ context.Context, request *runtimeapi.StopPodSandboxRequest) (*runtimeapi.StopPodSandboxResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopSandboxes++
	if request.PodSandboxId != "sandbox-0" {
		return nil, status.Error(codes.InvalidArgument, "wrong stop sandbox")
	}
	if f.sandboxError != codes.OK {
		err := status.Error(f.sandboxError, "test sandbox result unknown")
		if f.sandboxErrorOnce {
			f.sandboxError, f.sandboxErrorOnce = codes.OK, false
		}
		return nil, err
	}
	f.sandbox.State = runtimeapi.PodSandboxState_SANDBOX_NOTREADY
	if f.scopeEventsFile != "" && !f.stillRunning && !f.descendantRunning {
		if err := os.WriteFile(f.scopeEventsFile, []byte("populated 0\nfrozen 0\n"), 0600); err != nil {
			return nil, status.Error(codes.Internal, "test cgroup update failed")
		}
	}
	for i, entry := range f.containers {
		if !f.stillRunning && !(f.descendantRunning && i != 0) {
			entry.State = runtimeapi.ContainerState_CONTAINER_EXITED
		}
	}
	return &runtimeapi.StopPodSandboxResponse{}, nil
}

func newRuntimeFixture(t *testing.T) (*Provider, *fakeRuntime, fencing.RuntimeBinding, fencing.StopRequest) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("local CRI profile requires a Unix socket")
	}
	nodeImageID := "sha256:" + strings.Repeat("a", 64)
	boundImageRef := "registry.example/app@sha256:" + strings.Repeat("b", 64)
	f := &fakeRuntime{
		stopEntered: make(chan struct{}),
		version:     &runtimeapi.VersionResponse{RuntimeName: "containerd", RuntimeVersion: "test-version", RuntimeApiVersion: "v1"},
		host:        HostIdentity{NodeName: "node-0", NodeUID: "node-uid", MachineID: "machine", BootID: "boot", KernelRelease: "test-kernel"},
		sandbox: &runtimeapi.PodSandboxStatus{Id: "sandbox-0", CreatedAt: 1, State: runtimeapi.PodSandboxState_SANDBOX_READY,
			Metadata: &runtimeapi.PodSandboxMetadata{Name: "database", Namespace: "test", Uid: "pod-uid"},
			Linux:    &runtimeapi.LinuxPodSandboxStatus{Namespaces: &runtimeapi.Namespace{Options: &runtimeapi.NamespaceOption{Pid: runtimeapi.NamespaceMode_CONTAINER, Network: runtimeapi.NamespaceMode_POD, Ipc: runtimeapi.NamespaceMode_POD}}}},
		container:   &runtimeapi.ContainerStatus{Id: "container-0", CreatedAt: 2, State: runtimeapi.ContainerState_CONTAINER_RUNNING, ImageRef: boundImageRef, ImageId: nodeImageID, Metadata: &runtimeapi.ContainerMetadata{Name: "database"}},
		containers:  []*runtimeapi.Container{{Id: "container-0", PodSandboxId: "sandbox-0", State: runtimeapi.ContainerState_CONTAINER_RUNNING, ImageRef: nodeImageID, ImageId: nodeImageID, Metadata: &runtimeapi.ContainerMetadata{Name: "database"}}},
		imageStatus: &runtimeapi.ImageStatusResponse{Image: &runtimeapi.Image{Id: nodeImageID, RepoDigests: []string{boundImageRef}}},
	}
	// Keep Unix paths short on macOS; cleanup owns this exact new directory.
	directory, err := os.MkdirTemp("/tmp", "rhiza-cri-")
	if err != nil {
		t.Fatal(err)
	}
	f.directory = directory
	t.Cleanup(func() {
		if err := os.RemoveAll(directory); err != nil {
			t.Error(err)
		}
	})
	socket := filepath.Join(directory, "runtime.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	runtimeapi.RegisterRuntimeServiceServer(server, f)
	runtimeapi.RegisterImageServiceServer(server, f)
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-done })
	profile := Profile{Namespace: "test", ContainerName: "database", RuntimeName: "containerd", RuntimeVersion: "test-version", RuntimeAPIVersion: "v1", ContainerdRuntimeType: "io.containerd.runc.v2", CgroupProfileID: "qualified-test-profile", KernelRelease: "test-kernel", GraceSeconds: 3}
	p, err := Open(socket, profile, func(context.Context) (HostIdentity, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		host := f.host
		if f.changeHostAfterStop && f.stopContainers != 0 {
			host.BootID = "new-boot"
		}
		return host, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	binding := fencing.RuntimeBinding{AuthorityID: "authority", NamespaceUID: "namespace-uid", BindingUID: "binding-uid", StatefulSetUID: "sts-uid",
		SourceClusterID: "source", NodeID: "voter-0", PodUID: "pod-uid", RunID: "run-0", NodeName: f.host.NodeName, NodeUID: f.host.NodeUID,
		MachineID: f.host.MachineID, BootID: f.host.BootID, RuntimeID: RuntimeIdentity(profile.RuntimeName, profile.RuntimeVersion, profile.RuntimeAPIVersion),
		SandboxID: "sandbox-0", ContainerID: "containerd://container-0", ImageID: boundImageRef}
	request := fencing.StopRequest{OperationID: "closed-operation", RequestHash: strings.Repeat("a", 64), Identity: fencing.Identity{
		SourceClusterID: binding.SourceClusterID, NodeID: binding.NodeID, PodUID: binding.PodUID, RunID: binding.RunID, WALIdentity: "wal"}}
	return p, f, binding, request
}

func writeCgroupProcs(t *testing.T, dir string, pids ...uint32) {
	t.Helper()
	var contents strings.Builder
	for _, pid := range pids {
		contents.WriteString(strconv.FormatUint(uint64(pid), 10))
		contents.WriteByte('\n')
	}
	if err := os.WriteFile(filepath.Join(dir, "cgroup.procs"), []byte(contents.String()), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestCRIExactStopUsesRealUnixGRPCAndVerifiesScope(t *testing.T) {
	p, f, binding, request := newRuntimeFixture(t)
	observation, err := p.Stop(t.Context(), binding, request)
	if err != nil || !observation.ContainerStopAcknowledged || !observation.SandboxStopAcknowledged || !observation.ContainerExited || !observation.SandboxNotReady || observation.ObservedAt == "" {
		t.Fatalf("exact stop observation incomplete: %+v %v", observation, err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stopContainers != 1 || f.stopSandboxes != 1 || f.versionCalls != 3 {
		t.Fatalf("unexpected execution scope: container=%d sandbox=%d versions=%d", f.stopContainers, f.stopSandboxes, f.versionCalls)
	}
}

func TestCRIInventoryAcceptsDifferentImageRefDomainsWithMatchingImageID(t *testing.T) {
	p, _, binding, request := newRuntimeFixture(t)
	// containerd 2.3 reports the config digest in ListContainers.ImageRef and
	// the manifest-list digest in ContainerStatus.ImageRef, with matching IDs.

	observation, err := p.Stop(t.Context(), binding, request)
	if err != nil || !observation.ContainerExited || !observation.SandboxNotReady {
		t.Fatalf("different ImageRef domains with equal ImageId were rejected: %+v %v", observation, err)
	}
}

func TestCRIImageStatusFallbackMapsPlatformDigestToBoundRepoDigest(t *testing.T) {
	p, f, binding, request := newRuntimeFixture(t)
	f.mu.Lock()
	f.containers[0].ImageId, f.container.ImageId = "", ""
	listImageRef := f.containers[0].ImageRef
	f.mu.Unlock()

	observation, err := p.Stop(t.Context(), binding, request)
	if err != nil || !observation.ContainerExited || !observation.SandboxNotReady {
		t.Fatalf("containerd 2.2 ImageStatus mapping was rejected: %+v %v", observation, err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.imageStatusRequests) == 0 {
		t.Fatal("ImageStatus fallback was not used when both ImageId fields were absent")
	}
	for _, requested := range f.imageStatusRequests {
		if requested != listImageRef {
			t.Fatal("ImageStatus fallback did not use the exact inventory digest")
		}
	}
}

func TestCRIInventoryRequiresConsistentImageIDPresenceAndValue(t *testing.T) {
	for _, change := range []string{"list mismatch", "status mismatch", "list missing", "status missing"} {
		t.Run(change, func(t *testing.T) {
			p, f, binding, request := newRuntimeFixture(t)
			f.mu.Lock()
			switch change {
			case "list mismatch":
				f.containers[0].ImageId = "sha256:other-config"
			case "status mismatch":
				f.container.ImageId = "sha256:other-config"
			case "list missing":
				f.containers[0].ImageId = ""
			case "status missing":
				f.container.ImageId = ""
			}
			f.mu.Unlock()

			observation, err := p.Stop(t.Context(), binding, request)
			if !errors.Is(err, ErrUnverified) || observation.ContainerStopAcknowledged || observation.SandboxStopAcknowledged {
				t.Fatalf("invalid inventory ImageId was not rejected before stop: %+v %v", observation, err)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.stopContainers != 0 || f.stopSandboxes != 0 {
				t.Fatal("invalid inventory ImageId received a termination command")
			}
			if len(f.imageStatusRequests) != 0 {
				t.Fatal("mixed or mismatched ImageId fields incorrectly used the fallback")
			}
		})
	}
}

func TestCRIImageStatusFallbackRejectsUnverifiedMappings(t *testing.T) {
	for _, change := range []string{"tag reference", "malformed digest", "unsupported algorithm", "tag-bound status", "ImageStatus error", "unresolved image", "wrong image ID", "missing exact repo digest"} {
		t.Run(change, func(t *testing.T) {
			p, f, binding, request := newRuntimeFixture(t)
			f.mu.Lock()
			f.containers[0].ImageId, f.container.ImageId = "", ""
			imageRef := f.containers[0].ImageRef
			switch change {
			case "tag reference":
				f.containers[0].ImageRef = "registry.example/app:latest"
			case "malformed digest":
				f.containers[0].ImageRef = "sha256:bad"
			case "unsupported algorithm":
				f.containers[0].ImageRef = "sha1:" + strings.Repeat("d", 40)
			case "tag-bound status":
				binding.ImageID = "registry.example/app:latest"
				f.container.ImageRef = binding.ImageID
				f.imageStatus.Image.RepoDigests = []string{binding.ImageID}
			case "ImageStatus error":
				f.imageStatusError = codes.Unavailable
			case "unresolved image":
				f.imageStatus = &runtimeapi.ImageStatusResponse{}
			case "wrong image ID":
				f.imageStatus.Image.Id = "sha256:" + strings.Repeat("c", 64)
			case "missing exact repo digest":
				f.imageStatus.Image.RepoDigests = []string{"registry.other/app@sha256:" + strings.Repeat("b", 64)}
			}
			f.mu.Unlock()

			observation, err := p.Stop(t.Context(), binding, request)
			if !errors.Is(err, ErrUnverified) || observation.ContainerStopAcknowledged || observation.SandboxStopAcknowledged {
				t.Fatalf("unverified ImageStatus mapping was accepted: %+v %v", observation, err)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.stopContainers != 0 || f.stopSandboxes != 0 {
				t.Fatal("unverified ImageStatus mapping received a termination command")
			}
			if change == "tag reference" || change == "malformed digest" || change == "unsupported algorithm" || change == "tag-bound status" {
				if len(f.imageStatusRequests) != 0 {
					t.Fatal("tag-like reference reached ImageStatus fallback")
				}
			} else if len(f.imageStatusRequests) != 1 || f.imageStatusRequests[0] != imageRef {
				t.Fatal("ImageStatus fallback did not use the exact inventory digest")
			}
		})
	}
}

func TestCRIStopRejectsRuntimeVersionChangeBeforeSandboxStop(t *testing.T) {
	p, f, binding, request := newRuntimeFixture(t)
	f.mu.Lock()
	f.changeVersionAfterStop = true
	f.mu.Unlock()
	observation, err := p.Stop(t.Context(), binding, request)
	if !errors.Is(err, ErrUnverified) || !observation.ContainerStopAcknowledged || observation.SandboxStopAcknowledged {
		t.Fatalf("runtime change did not leave the exact partial stop pending: %+v %v", observation, err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stopSandboxes != 0 {
		t.Fatalf("changed runtime received sandbox stop: %d", f.stopSandboxes)
	}
}

func TestCRIIdentityMismatchesIssueZeroStopCommands(t *testing.T) {
	for _, change := range []string{"node UID", "machine", "boot", "kernel", "runtime", "runtime version", "sandbox", "pod", "namespace", "image", "binding image", "container", "host PID", "unknown namespace mode", "unknown container state", "other application", "wrong membership", "bad request"} {
		t.Run(change, func(t *testing.T) {
			p, f, binding, request := newRuntimeFixture(t)
			f.mu.Lock()
			switch change {
			case "node UID":
				binding.NodeUID = "new-node"
			case "machine":
				binding.MachineID = "new-machine"
			case "boot":
				binding.BootID = "new-boot"
			case "kernel":
				f.host.KernelRelease = "new-kernel"
			case "runtime":
				binding.RuntimeID = "other-runtime"
			case "runtime version":
				f.version.RuntimeVersion = "unqualified-version"
			case "sandbox":
				f.sandbox.Id = "other-sandbox"
			case "pod":
				f.sandbox.Metadata.Uid = "other-pod"
			case "namespace":
				f.sandbox.Metadata.Namespace = "other"
			case "image":
				f.container.ImageRef = "other-image"
			case "binding image":
				binding.ImageID = "other-image"
			case "container":
				f.container.Id = "other-container"
			case "host PID":
				f.sandbox.Linux.Namespaces.Options.Pid = runtimeapi.NamespaceMode_NODE
			case "unknown namespace mode":
				f.sandbox.Linux.Namespaces.Options.Network = runtimeapi.NamespaceMode(99)
			case "unknown container state":
				f.container.State = runtimeapi.ContainerState(99)
			case "other application":
				f.containers = append(f.containers, &runtimeapi.Container{Id: "unrelated", PodSandboxId: "sandbox-0", Metadata: &runtimeapi.ContainerMetadata{Name: "other"}})
			case "wrong membership":
				f.containers[0].PodSandboxId = "other-sandbox"
			case "bad request":
				request.RequestHash = "not-a-digest"
			}
			f.mu.Unlock()
			observation, err := p.Stop(t.Context(), binding, request)
			if !errors.Is(err, ErrUnverified) || observation.ContainerStopAcknowledged || observation.SandboxStopAcknowledged || observation.ObservedAt != "" {
				t.Fatalf("identity mismatch was not rejected: %+v %v", observation, err)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.stopContainers != 0 || f.stopSandboxes != 0 {
				t.Fatal("mismatched incarnation received a termination command")
			}
		})
	}
}

func TestCRIAcknowledgementAloneIsNotTerminalEvidence(t *testing.T) {
	for _, condition := range []string{"container alive", "another container alive", "metadata disappeared", "container timeout", "sandbox timeout", "boot changed"} {
		t.Run(condition, func(t *testing.T) {
			p, f, binding, request := newRuntimeFixture(t)
			f.mu.Lock()
			switch condition {
			case "container alive":
				f.stillRunning = true
			case "another container alive":
				f.descendantRunning = true
				f.containers = append(f.containers, &runtimeapi.Container{Id: "another-runtime", PodSandboxId: "sandbox-0", Metadata: &runtimeapi.ContainerMetadata{Name: "database"}, State: runtimeapi.ContainerState_CONTAINER_RUNNING})
			case "metadata disappeared":
				f.metadataGone = true
			case "container timeout":
				f.stopError = codes.DeadlineExceeded
			case "sandbox timeout":
				f.sandboxError = codes.DeadlineExceeded
			case "boot changed":
				f.changeHostAfterStop = true
			}
			f.mu.Unlock()
			observation, err := p.Stop(t.Context(), binding, request)
			if !errors.Is(err, ErrUnverified) || observation.ContainerExited || observation.SandboxNotReady || observation.ObservedAt != "" {
				t.Fatalf("incomplete stop became terminal evidence: %+v %v", observation, err)
			}
			if condition != "container timeout" && !observation.ContainerStopAcknowledged {
				t.Fatal("partial container stop acknowledgement was discarded")
			}
		})
	}
}

func TestCRIQualifiedStopRequiresSameCgroupToBecomeEmpty(t *testing.T) {
	p, f, binding, request := newRuntimeFixture(t)
	f.verboseInfo = map[string]string{"info": containerdInfoFixture}
	root := t.TempDir()
	proc := filepath.Join(root, "proc")
	cgroupRoot := filepath.Join(root, "cgroup")
	cgroup := filepath.Join(cgroupRoot, "kubepods", "pod-0", "container-0")
	if err := os.MkdirAll(cgroup, 0700); err != nil {
		t.Fatal(err)
	}
	writeProcIdentity(t, proc, 123, 789, "kubepods/pod-0")
	events := filepath.Join(cgroup, "cgroup.events")
	if err := os.WriteFile(events, []byte("populated 1\nfrozen 0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	writeCgroupProcs(t, cgroup, 123)
	scope, err := captureCgroupScope(proc, cgroupRoot, 123, "/kubepods/pod-0/container-0", binding, p.profile.CgroupProfileID)
	if err != nil {
		t.Fatal(err)
	}
	moved := filepath.Join(cgroupRoot, "kubepods", "other-pod")
	if err := os.MkdirAll(moved, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(moved, "cgroup.events"), []byte("populated 1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	writeCgroupProcs(t, cgroup)
	writeCgroupProcs(t, moved, 123)
	if err := os.WriteFile(filepath.Join(proc, "123", "cgroup"), []byte("0::/kubepods/other-pod\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := p.StopInCgroup(t.Context(), binding, request, proc, cgroupRoot, scope); !errors.Is(err, ErrUnverified) {
		t.Fatalf("process moved to another cgroup was accepted: %v", err)
	}
	f.mu.Lock()
	if f.stopContainers != 0 || f.stopSandboxes != 0 {
		f.mu.Unlock()
		t.Fatal("moved process received a CRI stop command")
	}
	f.mu.Unlock()
	writeCgroupProcs(t, cgroup, 123)
	f.mu.Lock()
	f.scopeEventsFile = events
	f.mu.Unlock()
	observation, err := p.StopInCgroup(t.Context(), binding, request, proc, cgroupRoot, scope)
	if err != nil || !observation.ContainerExited || observation.ObservedAt == "" {
		t.Fatalf("qualified execution scope was not verified: %+v %v", observation, err)
	}
	f.mu.Lock()
	stopContainers, stopSandboxes := f.stopContainers, f.stopSandboxes
	f.mu.Unlock()
	recovered, err := p.ResumeStopInCgroup(t.Context(), binding, request, proc, cgroupRoot, scope)
	if err != nil || !recovered.RecoveredTerminalState || !recovered.CgroupEmpty || recovered.CgroupScope != scope ||
		recovered.ContainerStopAcknowledged || recovered.SandboxStopAcknowledged || !recovered.ContainerExited || !recovered.SandboxNotReady {
		t.Fatalf("complete exact terminal state was not recoverable without repeating CRI stop: %+v %v", recovered, err)
	}
	f.mu.Lock()
	if f.stopContainers != stopContainers || f.stopSandboxes != stopSandboxes {
		f.mu.Unlock()
		t.Fatal("terminal receipt recovery repeated a CRI side effect")
	}
	f.metadataGone = true
	f.mu.Unlock()
	if _, err := p.ResumeStopInCgroup(t.Context(), binding, request, proc, cgroupRoot, scope); !errors.Is(err, ErrUnverified) {
		t.Fatalf("missing CRI terminal metadata was treated as proof: %v", err)
	}
	f.mu.Lock()
	if f.stopContainers != stopContainers || f.stopSandboxes != stopSandboxes {
		f.mu.Unlock()
		t.Fatal("metadata loss triggered a new stop call")
	}
	f.metadataGone = false
	f.mu.Unlock()
	otherBinding := binding
	otherBinding.RunID = "other-run"
	otherRequest := request
	otherRequest.Identity.RunID = otherBinding.RunID
	if _, err := p.StopInCgroup(t.Context(), otherBinding, otherRequest, proc, cgroupRoot, scope); !errors.Is(err, ErrUnverified) {
		t.Fatalf("cgroup scope from another run was accepted: %v", err)
	}
	f.mu.Lock()
	f.stillRunning = true
	f.scopeEventsFile = ""
	f.mu.Unlock()
	// A new cgroup identity is rejected before any second CRI stop attempt.
	if err := os.MkdirAll(filepath.Join(cgroupRoot, "replacement"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cgroupRoot, "replacement", "cgroup.events"), []byte("populated 1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(cgroup, filepath.Join(cgroupRoot, "old")); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(cgroupRoot, "replacement"), cgroup); err != nil {
		t.Fatal(err)
	}
	if _, err := p.StopInCgroup(t.Context(), binding, request, proc, cgroupRoot, scope); !errors.Is(err, ErrUnverified) {
		t.Fatalf("changed cgroup identity was accepted: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stopContainers != 1 || f.stopSandboxes != 1 {
		t.Fatalf("changed cgroup identity reached CRI: containers=%d sandboxes=%d", f.stopContainers, f.stopSandboxes)
	}
}

func TestCRIQualifiedStopRejectsVerboseCgroupPathChangeBeforeStop(t *testing.T) {
	p, f, binding, request := newRuntimeFixture(t)
	root := t.TempDir()
	proc, cgroupRoot := filepath.Join(root, "proc"), filepath.Join(root, "cgroup")
	cgroup := filepath.Join(cgroupRoot, "kubepods", "pod-0", "container-0")
	if err := os.MkdirAll(cgroup, 0700); err != nil {
		t.Fatal(err)
	}
	writeProcIdentity(t, proc, 123, 789, "../peer")
	writeCgroupProcs(t, cgroup, 123)
	if err := os.WriteFile(filepath.Join(cgroup, "cgroup.events"), []byte("populated 1\nfrozen 0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.verboseInfo = map[string]string{"info": containerdInfoFixture}
	f.mu.Unlock()
	scope, err := p.CaptureCgroupScope(t.Context(), binding, proc, cgroupRoot)
	if err != nil {
		t.Fatalf("capture trusted runtime cgroup scope: %v", err)
	}
	f.mu.Lock()
	f.verboseInfo = map[string]string{"info": strings.Replace(containerdInfoFixture, "/kubepods/pod-0/container-0", "/kubepods/pod-other/container-0", 1)}
	f.mu.Unlock()
	if _, err := p.StopInCgroup(t.Context(), binding, request, proc, cgroupRoot, scope); !errors.Is(err, ErrUnverified) {
		t.Fatalf("changed runtime cgroups path was accepted: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stopContainers != 0 || f.stopSandboxes != 0 {
		t.Fatalf("changed runtime cgroups path reached CRI stop: containers=%d sandboxes=%d", f.stopContainers, f.stopSandboxes)
	}
}

func TestCRIResumeCompletesExactContainerStoppedSandboxReadyCrashWindow(t *testing.T) {
	p, f, binding, request := newRuntimeFixture(t)
	f.verboseInfo = map[string]string{"info": containerdInfoFixture}
	root := t.TempDir()
	proc, cgroupRoot := filepath.Join(root, "proc"), filepath.Join(root, "cgroup")
	cgroup := filepath.Join(cgroupRoot, "kubepods", "pod-0", "container-0")
	if err := os.MkdirAll(cgroup, 0700); err != nil {
		t.Fatal(err)
	}
	writeProcIdentity(t, proc, 123, 789, "kubepods/pod-0")
	events := filepath.Join(cgroup, "cgroup.events")
	if err := os.WriteFile(events, []byte("populated 1\nfrozen 0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	writeCgroupProcs(t, cgroup, 123)
	scope, err := captureCgroupScope(proc, cgroupRoot, 123, "/kubepods/pod-0/container-0", binding, p.profile.CgroupProfileID)
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.scopeEventsFile, f.sandboxError, f.sandboxErrorOnce = events, codes.Unavailable, true
	f.mu.Unlock()
	partial, err := p.StopInCgroup(t.Context(), binding, request, proc, cgroupRoot, scope)
	if !errors.Is(err, ErrUnverified) || !partial.ContainerStopAcknowledged || partial.SandboxStopAcknowledged {
		t.Fatalf("sandbox-stop interruption did not preserve the partial observation: %+v %v", partial, err)
	}
	if err := os.RemoveAll(filepath.Join(proc, "123")); err != nil {
		t.Fatal(err)
	}
	recovered, err := p.ResumeStopInCgroup(t.Context(), binding, request, proc, cgroupRoot, scope)
	if err != nil || !recovered.RecoveredTerminalState || !recovered.ContainerExited || !recovered.SandboxNotReady || !recovered.CgroupEmpty {
		t.Fatalf("exact partial stop was not completed after restart: %+v %v", recovered, err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stopContainers != 1 || f.stopSandboxes != 2 {
		t.Fatalf("resume repeated container stop or missed sandbox stop: container=%d sandbox=%d", f.stopContainers, f.stopSandboxes)
	}
}

func TestCRIResumeRejectsRuntimeVersionChangeBeforeSandboxStop(t *testing.T) {
	p, f, binding, request := newRuntimeFixture(t)
	root := t.TempDir()
	proc, cgroupRoot := filepath.Join(root, "proc"), filepath.Join(root, "cgroup")
	cgroup := filepath.Join(cgroupRoot, "kubepods", "pod-0", "container-0")
	if err := os.MkdirAll(cgroup, 0700); err != nil {
		t.Fatal(err)
	}
	writeProcIdentity(t, proc, 123, 789, "kubepods/pod-0")
	events := filepath.Join(cgroup, "cgroup.events")
	if err := os.WriteFile(events, []byte("populated 1\nfrozen 0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	writeCgroupProcs(t, cgroup, 123)
	scope, err := captureCgroupScope(proc, cgroupRoot, 123, "/kubepods/pod-0/container-0", binding, p.profile.CgroupProfileID)
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.scopeEventsFile, f.sandboxError = events, codes.Unavailable
	f.mu.Unlock()
	if _, err := p.StopInCgroup(t.Context(), binding, request, proc, cgroupRoot, scope); !errors.Is(err, ErrUnverified) {
		t.Fatalf("sandbox-stop interruption was not left pending: %v", err)
	}
	if err := os.RemoveAll(filepath.Join(proc, "123")); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.changeVersionOnListCall = f.listCalls + 2 // terminal retry inspect, then partial-state inspect
	sandboxStops := f.stopSandboxes
	f.mu.Unlock()
	if _, err := p.ResumeStopInCgroup(t.Context(), binding, request, proc, cgroupRoot, scope); !errors.Is(err, ErrUnverified) {
		t.Fatalf("runtime change during partial-state verification was accepted: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stopSandboxes != sandboxStops {
		t.Fatalf("changed runtime received resumed sandbox stop: before=%d after=%d", sandboxStops, f.stopSandboxes)
	}
}

func TestCRIResumeRejectsWrongSandboxIdentityInContainerStoppedWindow(t *testing.T) {
	p, f, binding, request := newRuntimeFixture(t)
	root := t.TempDir()
	proc, cgroupRoot := filepath.Join(root, "proc"), filepath.Join(root, "cgroup")
	cgroup := filepath.Join(cgroupRoot, "kubepods", "pod-0", "container-0")
	if err := os.MkdirAll(cgroup, 0700); err != nil {
		t.Fatal(err)
	}
	writeProcIdentity(t, proc, 123, 789, "kubepods/pod-0")
	events := filepath.Join(cgroup, "cgroup.events")
	if err := os.WriteFile(events, []byte("populated 1\nfrozen 0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	writeCgroupProcs(t, cgroup, 123)
	scope, err := captureCgroupScope(proc, cgroupRoot, 123, "/kubepods/pod-0/container-0", binding, p.profile.CgroupProfileID)
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.scopeEventsFile, f.sandboxError = events, codes.Unavailable
	f.mu.Unlock()
	if _, err := p.StopInCgroup(t.Context(), binding, request, proc, cgroupRoot, scope); !errors.Is(err, ErrUnverified) {
		t.Fatalf("interrupted sandbox stop was not left pending: %v", err)
	}
	if err := os.RemoveAll(filepath.Join(proc, "123")); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.sandbox.Metadata.Uid = "another-pod"
	sandboxStops := f.stopSandboxes
	f.mu.Unlock()
	if _, err := p.ResumeStopInCgroup(t.Context(), binding, request, proc, cgroupRoot, scope); !errors.Is(err, ErrUnverified) {
		t.Fatalf("wrong sandbox identity was accepted: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stopSandboxes != sandboxStops {
		t.Fatal("wrong sandbox identity received a stop command")
	}
}

func TestCRIResumeRejectsNewLiveContainerInContainerStoppedWindow(t *testing.T) {
	p, f, binding, request := newRuntimeFixture(t)
	root := t.TempDir()
	proc, cgroupRoot := filepath.Join(root, "proc"), filepath.Join(root, "cgroup")
	cgroup := filepath.Join(cgroupRoot, "kubepods", "pod-0", "container-0")
	if err := os.MkdirAll(cgroup, 0700); err != nil {
		t.Fatal(err)
	}
	writeProcIdentity(t, proc, 123, 789, "kubepods/pod-0")
	events := filepath.Join(cgroup, "cgroup.events")
	if err := os.WriteFile(events, []byte("populated 1\nfrozen 0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	writeCgroupProcs(t, cgroup, 123)
	scope, err := captureCgroupScope(proc, cgroupRoot, 123, "/kubepods/pod-0/container-0", binding, p.profile.CgroupProfileID)
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.scopeEventsFile, f.sandboxError = events, codes.Unavailable
	f.mu.Unlock()
	if _, err := p.StopInCgroup(t.Context(), binding, request, proc, cgroupRoot, scope); !errors.Is(err, ErrUnverified) {
		t.Fatalf("interrupted sandbox stop was not left pending: %v", err)
	}
	if err := os.RemoveAll(filepath.Join(proc, "123")); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.containers = append(f.containers, &runtimeapi.Container{Id: "container-replacement", PodSandboxId: binding.SandboxID,
		State: runtimeapi.ContainerState_CONTAINER_RUNNING, ImageRef: binding.ImageID, Metadata: &runtimeapi.ContainerMetadata{Name: "database"}})
	sandboxStops := f.stopSandboxes
	f.mu.Unlock()
	if _, err := p.ResumeStopInCgroup(t.Context(), binding, request, proc, cgroupRoot, scope); !errors.Is(err, ErrUnverified) {
		t.Fatalf("new live sandbox container was accepted: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stopSandboxes != sandboxStops {
		t.Fatal("sandbox with a new live container received a stop command")
	}
}

func TestCRIContainerdEnrollmentCapturesPIDOnlyForExactBinding(t *testing.T) {
	p, f, binding, _ := newRuntimeFixture(t)
	f.mu.Lock()
	f.verboseInfo = map[string]string{"info": containerdInfoFixture}
	f.mu.Unlock()
	root := t.TempDir()
	proc := filepath.Join(root, "proc")
	cgroupRoot := filepath.Join(root, "cgroup")
	cgroup := filepath.Join(cgroupRoot, "kubepods", "pod-0", "container-0")
	if err := os.MkdirAll(cgroup, 0700); err != nil {
		t.Fatal(err)
	}
	writeProcIdentity(t, proc, 123, 789, "kubepods/pod-0")
	writeCgroupProcs(t, cgroup, 123)
	if err := os.WriteFile(filepath.Join(cgroup, "cgroup.events"), []byte("populated 1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	scope, err := p.CaptureCgroupScope(t.Context(), binding, proc, cgroupRoot)
	if err != nil || scope.PID != 123 || scope.StartTime != 789 || scope.RuntimeHash != fencing.RuntimeScopeBindingDigest(binding) {
		t.Fatalf("containerd enrollment scope=%+v, %v", scope, err)
	}
	f.mu.Lock()
	f.verboseInfo = map[string]string{"info": `{"sandboxID":"another-sandbox","pid":123,"removing":false,"snapshotKey":"snapshot","snapshotter":"overlayfs","runtimeType":"io.containerd.runc.v2","runtimeOptions":null,"config":{},"runtimeSpec":{}}`}
	f.mu.Unlock()
	if _, err := p.CaptureCgroupScope(t.Context(), binding, proc, cgroupRoot); !errors.Is(err, ErrUnverified) {
		t.Fatalf("cross-sandbox containerd PID was accepted: %v", err)
	}
}

func TestCRIEnrollmentBuildsFullBindingAndCapturesBeforeReturn(t *testing.T) {
	p, f, base, _ := newRuntimeFixture(t)
	f.mu.Lock()
	f.verboseInfo = map[string]string{"info": containerdInfoFixture}
	f.mu.Unlock()
	base.NodeUID, base.MachineID, base.BootID, base.RuntimeID, base.SandboxID = "", "", "", "", ""
	root := t.TempDir()
	proc, cgroupRoot := filepath.Join(root, "proc"), filepath.Join(root, "cgroup")
	cgroup := filepath.Join(cgroupRoot, "kubepods", "pod-0", "container-0")
	if err := os.MkdirAll(cgroup, 0700); err != nil {
		t.Fatal(err)
	}
	writeProcIdentity(t, proc, 123, 789, "kubepods/pod-0")
	writeCgroupProcs(t, cgroup, 123)
	if err := os.WriteFile(filepath.Join(cgroup, "cgroup.events"), []byte("populated 1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	binding, scope, err := p.EnrollBinding(t.Context(), base, proc, cgroupRoot)
	if err != nil {
		t.Fatalf("pre-child runtime enrollment failed: %v", err)
	}
	if binding.NodeUID != f.host.NodeUID || binding.MachineID != f.host.MachineID || binding.BootID != f.host.BootID || binding.RuntimeID != RuntimeIdentity("containerd", "test-version", "v1") || binding.SandboxID != "sandbox-0" || binding.ContainerID != base.ContainerID || binding.ImageID != base.ImageID {
		t.Fatalf("runtime facts were not bound from the host and CRI: %+v", binding)
	}
	if scope.PID != 123 || scope.StartTime != 789 || scope.RuntimeHash != fencing.RuntimeScopeBindingDigest(binding) || scope.ProfileID != "qualified-test-profile" {
		t.Fatalf("captured scope does not bind full runtime enrollment: %+v", scope)
	}
	base.ImageID = "other-image@sha256:" + strings.Repeat("0", 64)
	if _, _, err := p.EnrollBinding(t.Context(), base, proc, cgroupRoot); !errors.Is(err, ErrUnverified) {
		t.Fatalf("mismatched Kubernetes image accepted: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stopContainers != 0 || f.stopSandboxes != 0 {
		t.Fatal("enrollment unexpectedly stopped CRI processes")
	}
}

func TestCRINotFoundBeforeCaptureIsUncertainty(t *testing.T) {
	p, f, binding, request := newRuntimeFixture(t)
	f.mu.Lock()
	f.statusError = codes.NotFound
	f.mu.Unlock()
	observation, err := p.Stop(t.Context(), binding, request)
	if !errors.Is(err, ErrUnverified) || observation.ObservedAt != "" {
		t.Fatal("NotFound was interpreted as process termination")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stopContainers != 0 || f.stopSandboxes != 0 {
		t.Fatal("unknown runtime received stop commands")
	}
}

func TestCRICancellationDoesNotTriggerAnotherAttempt(t *testing.T) {
	p, f, binding, request := newRuntimeFixture(t)
	f.mu.Lock()
	f.blockStop = true
	f.mu.Unlock()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	type result struct {
		observation StopObservation
		err         error
	}
	completed := make(chan result, 1)
	go func() { observation, err := p.Stop(ctx, binding, request); completed <- result{observation, err} }()
	select {
	case <-f.stopEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("stop RPC was not reached before cancellation")
	}
	cancel()
	var outcome result
	select {
	case outcome = <-completed:
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled stop did not return")
	}
	observation, err := outcome.observation, outcome.err
	if !errors.Is(err, ErrUnverified) || observation.ContainerStopAcknowledged || observation.ObservedAt != "" {
		t.Fatalf("deadline was reported as acknowledged termination: %+v %v", observation, err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stopContainers != 1 || f.stopSandboxes != 0 {
		t.Fatalf("deadline retried/expanded scope: containers=%d sandboxes=%d", f.stopContainers, f.stopSandboxes)
	}
}

func TestCRIRejectsUnsupportedConfigurationWithoutConnecting(t *testing.T) {
	for _, kind := range []string{"TCP socket", "relative socket", "namespace", "kernel", "API", "negative grace", "excessive grace", "host reader"} {
		t.Run(kind, func(t *testing.T) {
			profile := Profile{Namespace: "test", ContainerName: "database", RuntimeName: "containerd", RuntimeVersion: "test-version", RuntimeAPIVersion: "v1", KernelRelease: "kernel", GraceSeconds: 3}
			socket := "/not-opened/runtime.sock"
			host := func(context.Context) (HostIdentity, error) {
				t.Fatal("invalid configuration read the host")
				return HostIdentity{}, nil
			}
			switch kind {
			case "TCP socket":
				socket = "https://runtime.example.invalid"
			case "relative socket":
				socket = "runtime.sock"
			case "namespace":
				profile.Namespace = ""
			case "kernel":
				profile.KernelRelease = ""
			case "API":
				profile.RuntimeAPIVersion = "unknown"
			case "negative grace":
				profile.GraceSeconds = -1
			case "excessive grace":
				profile.GraceSeconds = 11
			case "host reader":
				host = nil
			}
			p, err := Open(socket, profile, host)
			if err == nil {
				_ = p.Close()
				t.Fatal("unsupported configuration accepted")
			}
		})
	}
	var zero Provider
	if _, err := zero.Stop(t.Context(), fencing.RuntimeBinding{}, fencing.StopRequest{}); !errors.Is(err, ErrUnverified) {
		t.Fatal("zero provider did not fail closed")
	}
}

func TestCRIRuntimeIdentityBindsEveryTupleField(t *testing.T) {
	want := RuntimeIdentity("containerd", "v1", "v1")
	if want != RuntimeIdentity("containerd", "v1", "v1") || want == RuntimeIdentity("containerd", "v2", "v1") || want == RuntimeIdentity("containerd", "v1", "v2") || RuntimeIdentity("a/b", "c", "d") == RuntimeIdentity("a", "b/c", "d") {
		t.Fatal("runtime identity is unstable or aliases another version tuple")
	}
}

func TestCRIOwnedSocketServerAndDirectoryAreCleaned(t *testing.T) {
	var directory string
	t.Run("owned runtime", func(t *testing.T) {
		p, f, binding, request := newRuntimeFixture(t)
		directory = f.directory
		if _, err := p.Stop(t.Context(), binding, request); err != nil {
			t.Fatal(err)
		}
	})
	if directory == "" {
		t.Skip("Unix runtime fixture is unavailable")
	}
	if _, err := os.Stat(directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("owned runtime directory remains after cleanup: %v", err)
	}
}
