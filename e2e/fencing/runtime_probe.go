package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/fencing/cri"
	"github.com/mrchypark/rhiza/pkg/fencing/runtimeagent"
	"github.com/mrchypark/rhiza/pkg/operator"
)

const (
	runtimeProbePodName = "runtime-agent-component-probe"
	runtimeProbeSTName  = "runtime-agent-component-anchor"
	runtimeProbeLabel   = "rhiza.mrchypark.dev/runtime-agent-component-probe"
	runtimeAgentDSName  = "rhiza-runtime-agent"
)

type runtimeProfileEvidence struct {
	Qualification     string `json:"qualification"`
	KindNodeImage     string `json:"kindNodeImage"`
	RuntimeName       string `json:"runtimeName"`
	RuntimeVersion    string `json:"runtimeVersion"`
	RuntimeAPIVersion string `json:"runtimeAPIVersion"`
	RuntimeType       string `json:"runtimeType"`
	KernelRelease     string `json:"kernelRelease"`
	CgroupFilesystem  string `json:"cgroupFilesystem"`
	CgroupProfileID   string `json:"cgroupProfileID"`
}

type runtimeComponentEvidence struct {
	Qualification             string `json:"qualification"`
	Outcome                   string `json:"outcome"`
	FailureStage              string `json:"failureStage,omitempty"`
	KindNodeImage             string `json:"kindNodeImage,omitempty"`
	NodeName                  string `json:"nodeName,omitempty"`
	RuntimeVersion            string `json:"runtimeVersion,omitempty"`
	RuntimeType               string `json:"runtimeType,omitempty"`
	KernelRelease             string `json:"kernelRelease,omitempty"`
	CgroupFilesystem          string `json:"cgroupFilesystem,omitempty"`
	CgroupProfileID           string `json:"cgroupProfileID,omitempty"`
	PodUIDSHA256              string `json:"podUIDSHA256,omitempty"`
	ContainerIDSHA256         string `json:"containerIDSHA256,omitempty"`
	RuntimeBindingID          string `json:"runtimeBindingID,omitempty"`
	EnrollmentCaptured        bool   `json:"enrollmentCaptured"`
	EnrollmentDiagnosticStage string `json:"enrollmentDiagnosticStage,omitempty"`
	EnrollmentDiagnosticCode  string `json:"enrollmentDiagnosticCode,omitempty"`
	StopDiagnosticStage       string `json:"stopDiagnosticStage,omitempty"`
	StopDiagnosticCode        string `json:"stopDiagnosticCode,omitempty"`
	ContainerStopAck          bool   `json:"containerStopAcknowledged"`
	SandboxStopAck            bool   `json:"sandboxStopAcknowledged"`
	ContainerExited           bool   `json:"containerExited"`
	SandboxNotReady           bool   `json:"sandboxNotReady"`
	CgroupEmpty               bool   `json:"cgroupEmpty"`
	ReceiptVerified           bool   `json:"receiptVerified"`
	AgentRestarted            bool   `json:"agentRestarted"`
	ReceiptRecovered          bool   `json:"receiptRecovered"`
	ReceiptSHA256             string `json:"receiptSHA256,omitempty"`
	NoReplacementObserved     bool   `json:"noReplacementObserved"`
	CleanupConfirmed          bool   `json:"cleanupConfirmed"`
	StartedAt                 string `json:"startedAt"`
	CompletedAt               string `json:"completedAt,omitempty"`
}

type runtimeProbeInvocation struct {
	Action  string                 `json:"action"`
	Binding fencing.RuntimeBinding `json:"binding"`
	Request fencing.StopRequest    `json:"request"`
	Receipt string                 `json:"receipt,omitempty"`
}

type runtimeProbeReply struct {
	Binding       fencing.RuntimeBinding `json:"binding"`
	Request       fencing.StopRequest    `json:"request"`
	Receipt       string                 `json:"receipt,omitempty"`
	ReceiptSHA256 string                 `json:"receiptSHA256"`
	Observation   cri.StopObservation    `json:"observation"`
	Verified      bool                   `json:"verified"`
	Progress      *runtimeProbeProgress  `json:"progress,omitempty"`
}

type runtimeProbeProgress struct {
	EnrollmentCaptured bool   `json:"enrollmentCaptured"`
	EnrollmentStage    string `json:"enrollmentStage,omitempty"`
	EnrollmentCode     string `json:"enrollmentCode,omitempty"`
	StopStage          string `json:"stopStage,omitempty"`
	StopCode           string `json:"stopCode,omitempty"`
	Pending            bool   `json:"pending,omitempty"`
}

// runtimeAgentRequest runs inside the real authority Pod so TokenReview sees
// its projected, audience-bound ServiceAccount token and pinned Deployment.
func runtimeAgentRequest(ctx context.Context) error {
	data, err := io.ReadAll(io.LimitReader(os.Stdin, 64<<10))
	if err != nil || len(data) == 0 || len(data) >= 64<<10 {
		return fmt.Errorf("runtime-agent probe request unavailable")
	}
	var request runtimeProbeInvocation
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&request); err != nil || d.Decode(new(any)) != io.EOF {
		return fmt.Errorf("runtime-agent probe request invalid")
	}
	if request.Action != "stop" && request.Action != "resume" && request.Action != "retry-stop" {
		return fmt.Errorf("runtime-agent probe action invalid")
	}
	kube, err := operator.NewInCluster(namespace)
	if err != nil {
		return fmt.Errorf("configure in-cluster probe client: %w", err)
	}
	ca, err := os.ReadFile("/runtime-agent-ca/ca.crt")
	if err != nil {
		return fmt.Errorf("read runtime-agent trust: %w", err)
	}
	client := &operator.RuntimeAgentClient{Kube: kube, AuthorityID: "rhiza-fencer/" + namespace,
		TokenFile: "/var/run/secrets/rhiza-fencer/runtime-agent-token", CA: ca, ServerName: "runtime-agent", Port: "9445"}
	enrollmentCaptured := false
	if request.Action == "stop" {
		request.Binding, err = client.Enroll(ctx, request.Binding)
		if err != nil {
			var diagnostic *runtimeagent.EnrollmentDiagnosticError
			if errors.As(err, &diagnostic) && runtimeagent.ValidEnrollmentDiagnostic(diagnostic.Stage, diagnostic.Code) {
				return json.NewEncoder(os.Stdout).Encode(runtimeProbeReply{Progress: &runtimeProbeProgress{EnrollmentStage: diagnostic.Stage, EnrollmentCode: diagnostic.Code}})
			}
			return fmt.Errorf("runtime-agent enrollment failed: %w", err)
		}
		enrollmentCaptured = true
	} else if request.Action == "resume" {
		decoded, decodeErr := base64.StdEncoding.DecodeString(request.Receipt)
		if decodeErr != nil || len(decoded) == 0 || !json.Valid(decoded) {
			return fmt.Errorf("runtime-agent receipt input invalid")
		}
	}
	receipt, err := client.Stop(ctx, request.Binding, request.Request)
	if err != nil {
		var stopFailure *cri.StopDiagnosticError
		if request.Action == "stop" && errors.Is(err, fencing.ErrPending) && errors.As(err, &stopFailure) &&
			cri.ValidStopDiagnostic(stopFailure.Stage, stopFailure.Code) {
			return json.NewEncoder(os.Stdout).Encode(runtimeProbeReply{Progress: &runtimeProbeProgress{EnrollmentCaptured: enrollmentCaptured,
				StopStage: stopFailure.Stage, StopCode: stopFailure.Code, Pending: true}})
		}
		return fmt.Errorf("runtime-agent exact stop remains unverified: %w", err)
	}
	if request.Action == "resume" {
		want, _ := base64.StdEncoding.DecodeString(request.Receipt)
		if !bytes.Equal(receipt, want) {
			return fmt.Errorf("runtime-agent durable receipt changed after restart")
		}
	}
	if err := client.Verify(ctx, request.Binding, request.Request, receipt); err != nil {
		return fmt.Errorf("runtime-agent durable receipt verification failed: %w", err)
	}
	var observation cri.StopObservation
	if err := json.Unmarshal(receipt, &observation); err != nil || observation.Runtime != request.Binding || observation.Request != request.Request ||
		!observation.ContainerExited || !observation.SandboxNotReady || !observation.CgroupEmpty || observation.ObservedAt == "" ||
		!runtimeProbeCgroupScopeMatches(request.Binding, observation.CgroupScope, "") {
		return fmt.Errorf("runtime-agent receipt lacks exact terminal container, sandbox and cgroup evidence")
	}
	if request.Action == "stop" && (!observation.ContainerStopAcknowledged || !observation.SandboxStopAcknowledged) {
		return fmt.Errorf("runtime-agent initial receipt is not a completed stop")
	}
	digest := sha256.Sum256(receipt)
	reply := runtimeProbeReply{Binding: request.Binding, Request: request.Request, ReceiptSHA256: hex.EncodeToString(digest[:]), Observation: observation, Verified: true,
		Progress: &runtimeProbeProgress{EnrollmentCaptured: enrollmentCaptured}}
	if request.Action == "stop" {
		reply.Receipt = base64.StdEncoding.EncodeToString(receipt)
	}
	return json.NewEncoder(os.Stdout).Encode(reply)
}

func runtimeProbeCgroupScopeMatches(binding fencing.RuntimeBinding, scope cri.CgroupScope, profileID string) bool {
	return scope.Path != "" && scope.Ino != 0 && scope.PID != 0 && scope.StartTime != 0 && scope.ProfileID != "" &&
		scope.RuntimeHash == fencing.RuntimeScopeBindingDigest(binding) && (profileID == "" || scope.ProfileID == profileID)
}

func runtimeProbeWorkload(statefulSetUID string) obj {
	return obj{"apiVersion": "v1", "kind": "Pod", "metadata": obj{"name": runtimeProbePodName, "namespace": namespace,
		"labels": obj{runtimeProbeLabel: "true"}, "annotations": obj{"rhiza.mrchypark.dev/fencing-root-uid": "runtime-agent-component-only"},
		"ownerReferences": []obj{{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": runtimeProbeSTName, "uid": statefulSetUID, "controller": false, "blockOwnerDeletion": false}}},
		"spec": obj{"restartPolicy": "Never", "terminationGracePeriodSeconds": 1, "automountServiceAccountToken": false,
			"containers": []obj{{"name": "rhiza", "image": "rhiza-fencing-host:local", "command": []string{"/bin/sh", "-c", "exec sleep 600"},
				"securityContext": obj{"runAsUser": 65532, "allowPrivilegeEscalation": false, "readOnlyRootFilesystem": true, "capabilities": obj{"drop": []string{"ALL"}}}}}}}
}

func runtimeProbeAnchor() obj {
	labels := obj{"app": "runtime-agent-component-anchor"}
	return resource("apps/v1", "StatefulSet", runtimeProbeSTName, obj{"replicas": 0, "serviceName": runtimeProbeSTName,
		"selector": obj{"matchLabels": labels}, "template": obj{"metadata": obj{"labels": labels}, "spec": obj{"containers": []obj{{"name": "rhiza", "image": "rhiza-fencing-host:local", "command": []string{"/bin/sh", "-c", "exec sleep 600"}}}}}})
}

func runtimeComponentProbe(parent context.Context) (resultErr error) {
	started := time.Now().UTC()
	out := os.Getenv("FENCING_E2E_OUT")
	if out == "" {
		return fmt.Errorf("FENCING_E2E_OUT is required")
	}
	evidence := runtimeComponentEvidence{Qualification: "runtime-agent-component-only", Outcome: "failed", StartedAt: started.Format(time.RFC3339Nano)}
	stage := "profile"
	var podUID, anchorUID string
	var podCreateAttempted, anchorCreateAttempted bool
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		var cleanupErr error
		if podUID != "" {
			cleanupErr = errors.Join(cleanupErr, deleteProbeObject(cleanupCtx, "pod", runtimeProbePodName, podUID))
		} else if podCreateAttempted {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("probe Pod UID was not pinned; refusing name-only cleanup"))
		}
		if anchorUID != "" {
			cleanupErr = errors.Join(cleanupErr, deleteProbeObject(cleanupCtx, "statefulset", runtimeProbeSTName, anchorUID))
		} else if anchorCreateAttempted {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("probe anchor UID was not pinned; refusing name-only cleanup"))
		}
		evidence.CleanupConfirmed = cleanupErr == nil && anchorCreateAttempted && anchorUID != "" && (!podCreateAttempted || podUID != "")
		resultErr = errors.Join(resultErr, cleanupErr)
		if resultErr == nil {
			evidence.Outcome = "passed"
			evidence.CompletedAt = time.Now().UTC().Format(time.RFC3339Nano)
		} else {
			evidence.FailureStage = stage
		}
		data, err := json.Marshal(evidence)
		if err == nil {
			err = os.WriteFile(filepath.Join(out, "runtime-component-probe.json"), append(data, '\n'), 0600)
		}
		resultErr = errors.Join(resultErr, err)
	}()

	var profile runtimeProfileEvidence
	profileData, err := os.ReadFile(filepath.Join(out, "runtime-profile.json"))
	if err != nil || json.Unmarshal(profileData, &profile) != nil || profile.Qualification != "runtime-agent-component-only" ||
		profile.RuntimeName != "containerd" || profile.RuntimeVersion == "" || profile.RuntimeAPIVersion != "v1" || profile.RuntimeType == "" ||
		profile.KernelRelease == "" || profile.CgroupFilesystem != "cgroup2fs" || profile.CgroupProfileID == "" || profile.KindNodeImage == "" {
		return fmt.Errorf("runtime component profile is missing or unsupported")
	}
	evidence.KindNodeImage, evidence.RuntimeVersion, evidence.RuntimeType = profile.KindNodeImage, profile.RuntimeVersion, profile.RuntimeType
	evidence.KernelRelease, evidence.CgroupFilesystem, evidence.CgroupProfileID = profile.KernelRelease, profile.CgroupFilesystem, profile.CgroupProfileID
	ctx, cancel := context.WithTimeout(parent, 5*time.Minute)
	defer cancel()
	stage = "agent-ready"
	var anchor struct {
		Metadata struct {
			UID string `json:"uid"`
		} `json:"metadata"`
	}
	anchorCreateAttempted = true
	if data, err := commandWithInput(ctx, encoded(runtimeProbeAnchor()), "kubectl", "--context", kubeContext, "-n", namespace, "apply", "-f", "-", "-o", "json"); err != nil {
		return fmt.Errorf("create non-controller probe anchor: %w", err)
	} else if err := json.Unmarshal(data, &anchor); err != nil || anchor.Metadata.UID == "" {
		return fmt.Errorf("probe anchor UID unavailable")
	}
	anchorUID = anchor.Metadata.UID
	podCreateAttempted = true
	if _, err := commandWithInput(ctx, encoded(runtimeProbeWorkload(anchorUID)), "kubectl", "--context", kubeContext, "-n", namespace, "apply", "-f", "-"); err != nil {
		return fmt.Errorf("create no-restart probe workload: %w", err)
	}
	createdPod, err := get(ctx, "pod", runtimeProbePodName)
	if err != nil {
		return fmt.Errorf("read created no-restart probe workload: %w", err)
	}
	podUID = text(nested(createdPod, "metadata", "uid"))
	if podUID == "" {
		return fmt.Errorf("created probe workload UID unavailable")
	}
	stage = "workload-ready"
	if err := wait(ctx, "runtime probe Pod Ready", func() (bool, error) {
		pod, err := get(ctx, "pod", runtimeProbePodName)
		if err != nil {
			return false, err
		}
		if text(nested(pod, "metadata", "uid")) != podUID {
			return false, fmt.Errorf("probe workload UID changed")
		}
		if text(nested(pod, "metadata", "deletionTimestamp")) != "" {
			return false, fmt.Errorf("probe workload is terminating")
		}
		owners := runtimeProbeList(nested(pod, "metadata", "ownerReferences"))
		if len(owners) != 1 || nested(owners[0], "uid") != anchorUID || nested(owners[0], "controller") != false || nested(pod, "spec", "restartPolicy") != "Never" {
			return false, fmt.Errorf("probe workload recreation barrier differs")
		}
		containers := runtimeProbeList(nested(pod, "status", "containerStatuses"))
		if text(nested(pod, "status", "phase")) != "Running" || len(containers) != 1 || nested(containers[0], "ready") != true || nested(containers[0], "started") != true {
			return false, nil
		}
		return true, nil
	}); err != nil {
		return err
	}
	pod, err := get(ctx, "pod", runtimeProbePodName)
	if err != nil {
		return err
	}
	if text(nested(pod, "metadata", "uid")) != podUID {
		return fmt.Errorf("probe workload UID changed before enrollment")
	}
	if len(runtimeProbeList(nested(pod, "metadata", "ownerReferences"))) != 1 {
		return fmt.Errorf("probe workload owner identity missing")
	}
	containerStatuses := runtimeProbeList(nested(pod, "status", "containerStatuses"))
	if len(containerStatuses) != 1 {
		return fmt.Errorf("probe workload container status is ambiguous")
	}
	container := containerStatuses[0]
	containerID, imageID := text(nested(container, "containerID")), text(nested(container, "imageID"))
	if !strings.HasPrefix(containerID, "containerd://") || !strings.Contains(imageID, "@sha256:") {
		return fmt.Errorf("probe CRI image/container identity unavailable")
	}
	uid, nodeName := text(nested(pod, "metadata", "uid")), text(nested(pod, "spec", "nodeName"))
	if uid == "" || nodeName == "" {
		return fmt.Errorf("probe Pod UID or node missing")
	}
	var ns struct {
		Metadata struct {
			UID string `json:"uid"`
		} `json:"metadata"`
	}
	if data, err := kubectl(ctx, "get", "namespace", namespace, "-o", "json"); err != nil || json.Unmarshal(data, &ns) != nil || ns.Metadata.UID == "" {
		return fmt.Errorf("probe namespace UID unavailable")
	}
	anchorLive, err := get(ctx, "statefulset", runtimeProbeSTName)
	if err != nil || text(nested(anchorLive, "metadata", "uid")) != anchorUID || nested(anchorLive, "spec", "replicas") != float64(0) ||
		nested(anchorLive, "spec", "selector", "matchLabels", "app") == nested(pod, "metadata", "labels", "app") {
		return fmt.Errorf("probe anchor UID or recreation barrier changed")
	}
	identity := fencing.Identity{SourceClusterID: "runtime-agent-component", NodeID: runtimeProbePodName, PodUID: uid, RunID: "probe-" + uid}
	bindingUID := text(nested(pod, "metadata", "annotations", "rhiza.mrchypark.dev/fencing-root-uid"))
	binding := fencing.RuntimeBinding{AuthorityID: "rhiza-fencer/" + namespace, NamespaceUID: ns.Metadata.UID, BindingUID: bindingUID, StatefulSetUID: anchorUID,
		SourceClusterID: identity.SourceClusterID, NodeID: identity.NodeID, PodUID: identity.PodUID, RunID: identity.RunID, NodeName: nodeName,
		ContainerID: containerID, ImageID: imageID}
	requestHash := sha256.Sum256(encoded(obj{"binding": binding, "identity": identity, "operation": "runtime-agent-component-probe"}))
	stopRequest := fencing.StopRequest{OperationID: "runtime-agent-component-" + uid, RequestHash: hex.EncodeToString(requestHash[:]), Identity: identity}
	evidence.NodeName = nodeName
	evidence.PodUIDSHA256, evidence.ContainerIDSHA256 = digestText(uid), digestText(containerID)
	if stateful := nested(pod, "metadata", "ownerReferences"); stateful == nil {
		return fmt.Errorf("probe workload lacks owner pin")
	}
	stage = "authenticated-stop"
	first, err := callRuntimeAgent(ctx, runtimeProbeInvocation{Action: "stop", Binding: binding, Request: stopRequest})
	if err != nil {
		return err
	}
	if first.Progress != nil && (first.Progress.EnrollmentStage != "" || first.Progress.EnrollmentCode != "") {
		stage = "enrollment"
	}
	if err := applyRuntimeProbeProgress(&evidence, first.Progress); err != nil {
		return err
	}
	if first.Binding.RuntimeID != cri.RuntimeIdentity(profile.RuntimeName, profile.RuntimeVersion, profile.RuntimeAPIVersion) {
		return fmt.Errorf("runtime enrollment differs from captured CRI profile")
	}
	if first.Binding.Validate(identity) != nil || first.Binding.NodeName != nodeName || first.Binding.ContainerID != containerID || first.Binding.ImageID != imageID ||
		first.Request != stopRequest || !first.Verified || first.Receipt == "" || first.ReceiptSHA256 == "" {
		return fmt.Errorf("runtime agent enrollment or receipt tuple differs")
	}
	receipt, err := base64.StdEncoding.DecodeString(first.Receipt)
	if err != nil || len(receipt) == 0 || digestText(string(receipt)) != first.ReceiptSHA256 {
		return fmt.Errorf("runtime agent receipt digest differs")
	}
	if first.Observation.Runtime != first.Binding || first.Observation.Request != stopRequest || !first.Observation.ContainerStopAcknowledged || !first.Observation.SandboxStopAcknowledged ||
		!first.Observation.ContainerExited || !first.Observation.SandboxNotReady || !first.Observation.CgroupEmpty ||
		!runtimeProbeCgroupScopeMatches(first.Binding, first.Observation.CgroupScope, profile.CgroupProfileID) {
		return fmt.Errorf("runtime agent did not return actual exact terminal scope evidence")
	}
	evidence.EnrollmentCaptured, evidence.ContainerStopAck, evidence.SandboxStopAck = true, true, true
	evidence.ContainerExited, evidence.SandboxNotReady, evidence.CgroupEmpty, evidence.ReceiptVerified = true, true, true, true
	evidence.RuntimeBindingID, evidence.ReceiptSHA256 = first.Binding.RuntimeID, first.ReceiptSHA256
	stage = "target-no-replacement"
	if err := wait(ctx, "no-restart probe target terminal", func() (bool, error) {
		current, err := get(ctx, "pod", runtimeProbePodName)
		if err != nil {
			return false, err
		}
		if text(nested(current, "metadata", "uid")) != uid {
			return false, fmt.Errorf("probe target UID changed")
		}
		if text(nested(current, "status", "phase")) != "Failed" {
			return false, nil
		}
		return true, nil
	}); err != nil {
		return err
	}
	items, err := get(ctx, "pods", "")
	if err != nil {
		return err
	}
	matching := 0
	for _, item := range runtimeProbeList(nested(items, "items")) {
		if nested(item, "metadata", "labels", runtimeProbeLabel) == "true" {
			matching++
			if nested(item, "metadata", "uid") != uid {
				return fmt.Errorf("replacement probe Pod appeared")
			}
		}
	}
	if matching != 1 {
		return fmt.Errorf("no-restart barrier observed %d matching Pods", matching)
	}
	evidence.NoReplacementObserved = true
	stage = "agent-restart"
	oldAgent, err := runtimeAgentPod(ctx, nodeName)
	daemonSet, dsErr := get(ctx, "daemonset", runtimeAgentDSName)
	daemonSetUID := text(nested(daemonSet, "metadata", "uid"))
	if dsErr != nil || daemonSetUID == "" || oldAgent != nil && !runtimeAgentOwnedBy(oldAgent, nodeName, daemonSetUID) {
		return fmt.Errorf("runtime-agent DaemonSet or old Pod owner identity unavailable")
	}
	if err != nil {
		return err
	}
	oldAgentUID := text(nested(oldAgent, "metadata", "uid"))
	if oldAgentUID == "" {
		return fmt.Errorf("runtime-agent Pod UID unavailable")
	}
	if err := deleteObjectWithUID(ctx, "pods", text(nested(oldAgent, "metadata", "name")), oldAgentUID); err != nil {
		return fmt.Errorf("delete owned runtime-agent Pod for restart: %w", err)
	}
	if err := wait(ctx, "runtime-agent Pod replacement", func() (bool, error) {
		pods, err := get(ctx, "pods", "")
		if err != nil {
			return false, err
		}
		for _, item := range runtimeProbeList(nested(pods, "items")) {
			if nested(item, "metadata", "labels", "app.kubernetes.io/name") == runtimeAgentDSName && nested(item, "spec", "nodeName") == nodeName && nested(item, "metadata", "uid") != oldAgentUID && runtimeAgentOwnedBy(runtimeProbeObject(item), nodeName, daemonSetUID) && podReady(item) {
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		return err
	}
	newAgent, err := runtimeAgentPod(ctx, nodeName)
	currentDaemonSet, currentDSErr := get(ctx, "daemonset", runtimeAgentDSName)
	if err != nil || currentDSErr != nil || text(nested(currentDaemonSet, "metadata", "uid")) != daemonSetUID ||
		nested(newAgent, "metadata", "uid") == oldAgentUID || !runtimeAgentOwnedBy(newAgent, nodeName, daemonSetUID) {
		return fmt.Errorf("runtime-agent Pod did not restart on the pinned node")
	}
	evidence.AgentRestarted = true
	stage = "receipt-recovery"
	for _, changed := range []fencing.StopRequest{
		{OperationID: stopRequest.OperationID + "-changed", RequestHash: stopRequest.RequestHash, Identity: stopRequest.Identity},
		{OperationID: stopRequest.OperationID, RequestHash: changedRequestHash(stopRequest.RequestHash), Identity: stopRequest.Identity},
	} {
		if _, err := callRuntimeAgent(ctx, runtimeProbeInvocation{Action: "retry-stop", Binding: first.Binding, Request: changed}); err == nil {
			return fmt.Errorf("runtime agent accepted a changed operation/request tuple")
		}
	}
	resumed, err := callRuntimeAgent(ctx, runtimeProbeInvocation{Action: "resume", Binding: first.Binding, Request: stopRequest, Receipt: first.Receipt})
	if err != nil {
		return err
	}
	if resumed.Binding != first.Binding || resumed.Request != stopRequest || resumed.ReceiptSHA256 != first.ReceiptSHA256 || !resumed.Verified ||
		!resumed.Observation.ContainerExited || !resumed.Observation.SandboxNotReady || !resumed.Observation.CgroupEmpty || resumed.Observation.CgroupScope != first.Observation.CgroupScope {
		return fmt.Errorf("restarted runtime agent did not recover the exact durable receipt")
	}
	evidence.ReceiptRecovered = true
	stage = "cleanup"
	return nil
}

func applyRuntimeProbeProgress(evidence *runtimeComponentEvidence, progress *runtimeProbeProgress) error {
	if evidence == nil || progress == nil {
		return fmt.Errorf("runtime-agent stop progress is unavailable")
	}
	if progress.EnrollmentStage != "" || progress.EnrollmentCode != "" {
		if !runtimeagent.ValidEnrollmentDiagnostic(progress.EnrollmentStage, progress.EnrollmentCode) || progress.EnrollmentCaptured {
			return fmt.Errorf("runtime-agent enrollment diagnostic category is invalid")
		}
		evidence.EnrollmentDiagnosticStage, evidence.EnrollmentDiagnosticCode = progress.EnrollmentStage, progress.EnrollmentCode
		return fmt.Errorf("runtime-agent enrollment rejected at %s/%s", progress.EnrollmentStage, progress.EnrollmentCode)
	}
	evidence.EnrollmentCaptured = progress.EnrollmentCaptured
	if progress.StopStage != "" || progress.StopCode != "" {
		if !cri.ValidStopDiagnostic(progress.StopStage, progress.StopCode) {
			return fmt.Errorf("runtime-agent stop diagnostic category is invalid")
		}
		evidence.StopDiagnosticStage, evidence.StopDiagnosticCode = progress.StopStage, progress.StopCode
	}
	if progress.Pending {
		if progress.StopStage == "" || progress.StopCode == "" {
			return fmt.Errorf("runtime-agent pending response lacks a stop category")
		}
		return fmt.Errorf("runtime-agent stop pending at %s/%s", progress.StopStage, progress.StopCode)
	}
	return nil
}

func callRuntimeAgent(ctx context.Context, request runtimeProbeInvocation) (runtimeProbeReply, error) {
	data, err := commandWithInput(ctx, encoded(request), "kubectl", "--context", kubeContext, "-n", namespace, "exec", "-i", "deployment/rhiza-fencer", "-c", "authority", "--", "/usr/local/bin/fencing-e2e", "runtime-agent-request")
	if err != nil {
		return runtimeProbeReply{}, fmt.Errorf("runtime-agent component call failed: %w", err)
	}
	var result runtimeProbeReply
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&result); err != nil || d.Decode(new(any)) != io.EOF {
		return runtimeProbeReply{}, fmt.Errorf("runtime-agent component response invalid")
	}
	return result, nil
}

func changedRequestHash(hash string) string {
	if len(hash) == 0 {
		return "0"
	}
	if hash[0] == '0' {
		return "1"
	}
	return "0" + hash[1:]
}

func deleteProbeObject(ctx context.Context, kind, name, uid string) error {
	if uid == "" {
		return fmt.Errorf("refusing name-only cleanup for %s/%s", kind, name)
	}
	resource := map[string]string{"pod": "pods", "statefulset": "statefulsets"}[kind]
	if resource == "" {
		return fmt.Errorf("unsupported probe cleanup kind %q", kind)
	}
	deleteErr := deleteObjectWithUID(ctx, resource, name, uid)
	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if err := wait(waitCtx, "probe "+kind+" UID cleanup", func() (bool, error) {
		current, err := get(waitCtx, kind, name)
		if err != nil {
			if strings.Contains(err.Error(), "(NotFound)") {
				return true, nil
			}
			return false, err
		}
		return probeUIDGone(uid, current), nil
	}); err != nil {
		return errors.Join(deleteErr, fmt.Errorf("confirm probe %s UID cleanup: %w", kind, err))
	}
	return nil
}

// deleteObjectWithUID uses Kubernetes DeleteOptions rather than a kubectl
// delete flag: kubectl does not support --preconditions on all pinned versions.
func deleteObjectWithUID(ctx context.Context, resource, name, uid string) error {
	return deleteObjectWithPreconditions(ctx, resource, name, uid, "", nil)
}

func deleteObjectWithUIDAndResourceVersion(ctx context.Context, resource, name, uid, resourceVersion string) error {
	if resourceVersion == "" {
		return fmt.Errorf("refusing resourceVersion-preconditioned delete without resourceVersion")
	}
	return deleteObjectWithPreconditions(ctx, resource, name, uid, resourceVersion, nil)
}

func deleteObjectWithUIDAndGracePeriod(ctx context.Context, resource, name, uid string, gracePeriodSeconds int64) error {
	if gracePeriodSeconds < 0 {
		return fmt.Errorf("refusing UID-preconditioned delete with negative grace period")
	}
	return deleteObjectWithPreconditions(ctx, resource, name, uid, "", &gracePeriodSeconds)
}

type kubernetesDeleteOptions struct {
	APIVersion         string `json:"apiVersion"`
	Kind               string `json:"kind"`
	GracePeriodSeconds *int64 `json:"gracePeriodSeconds,omitempty"`
	Preconditions      struct {
		UID             string  `json:"uid"`
		ResourceVersion *string `json:"resourceVersion,omitempty"`
	} `json:"preconditions"`
}

func buildKubernetesDeleteOptions(uid, resourceVersion string, gracePeriodSeconds *int64) (kubernetesDeleteOptions, error) {
	if uid == "" || (gracePeriodSeconds != nil && *gracePeriodSeconds < 0) {
		return kubernetesDeleteOptions{}, fmt.Errorf("invalid UID-preconditioned delete options")
	}
	options := kubernetesDeleteOptions{APIVersion: "v1", Kind: "DeleteOptions", GracePeriodSeconds: gracePeriodSeconds}
	options.Preconditions.UID = uid
	if resourceVersion != "" {
		options.Preconditions.ResourceVersion = &resourceVersion
	}
	return options, nil
}

func deleteObjectWithPreconditions(ctx context.Context, resource, name, uid, resourceVersion string, gracePeriodSeconds *int64) error {
	if name == "" || uid == "" {
		return fmt.Errorf("refusing UID-preconditioned delete without name and UID")
	}
	path, err := probeDeletePath(namespace, resource, name)
	if err != nil {
		return err
	}

	proxyDir, err := os.MkdirTemp("/tmp", "rhiza-kube-proxy-")
	if err != nil {
		return fmt.Errorf("create private Kubernetes proxy directory: %w", err)
	}
	defer os.RemoveAll(proxyDir)
	socketPath := filepath.Join(proxyDir, "proxy.sock")
	cmd := exec.CommandContext(ctx, "kubectl", "--context", kubeContext, "proxy", "--unix-socket", socketPath, "--api-prefix=/")
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start Kubernetes API proxy: %w", err)
	}
	proxyDone := make(chan struct{})
	var proxyErr error
	go func() {
		proxyErr = cmd.Wait()
		close(proxyDone)
	}()
	defer func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		<-proxyDone
	}()

	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	baseURL := "http://localhost"
	readyDeadline := time.NewTimer(10 * time.Second)
	defer readyDeadline.Stop()
	for {
		select {
		case <-proxyDone:
			return fmt.Errorf("Kubernetes API proxy exited before readiness: %v", proxyErr)
		default:
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/version", nil)
		if err != nil {
			return err
		}
		response, requestErr := client.Do(request)
		if requestErr == nil {
			_ = response.Body.Close()
			if response.StatusCode >= 200 && response.StatusCode < 300 {
				break
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for Kubernetes API proxy: %w", ctx.Err())
		case <-readyDeadline.C:
			return fmt.Errorf("Kubernetes API proxy did not become ready")
		case <-time.After(50 * time.Millisecond):
		}
	}

	deleteOptions, err := buildKubernetesDeleteOptions(uid, resourceVersion, gracePeriodSeconds)
	if err != nil {
		return err
	}
	body, err := json.Marshal(deleteOptions)
	if err != nil {
		return fmt.Errorf("encode Kubernetes UID delete options: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodDelete, baseURL+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("construct Kubernetes UID delete request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Origin", baseURL)
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("send Kubernetes UID-preconditioned delete: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("Kubernetes UID-preconditioned delete returned HTTP %d", response.StatusCode)
	}
	return nil
}

func probeDeletePath(namespace, resource, name string) (string, error) {
	apiPrefix := map[string]string{"pods": "/api/v1", "configmaps": "/api/v1", "statefulsets": "/apis/apps/v1"}[resource]
	if apiPrefix == "" {
		return "", fmt.Errorf("unsupported UID-preconditioned delete resource %q", resource)
	}
	return apiPrefix + "/namespaces/" + url.PathEscape(namespace) + "/" + resource + "/" + url.PathEscape(name), nil
}

func probeUIDGone(uid string, current obj) bool {
	currentUID := text(nested(current, "metadata", "uid"))
	return uid != "" && currentUID != "" && currentUID != uid
}

func commandWithInput(ctx context.Context, input []byte, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdin = bytes.NewReader(input)
	data, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("%s command failed: %w: %s", name, err, data)
	}
	return data, nil
}

func digestText(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func podReady(pod any) bool {
	conditions := runtimeProbeList(nested(pod, "status", "conditions"))
	for _, condition := range conditions {
		if nested(condition, "type") == "Ready" && nested(condition, "status") == "True" {
			return true
		}
	}
	return false
}

func runtimeAgentPod(ctx context.Context, nodeName string) (obj, error) {
	pods, err := get(ctx, "pods", "")
	if err != nil {
		return nil, err
	}
	var found obj
	for _, item := range runtimeProbeList(nested(pods, "items")) {
		if nested(item, "metadata", "labels", "app.kubernetes.io/name") != runtimeAgentDSName || nested(item, "spec", "nodeName") != nodeName || !podReady(item) {
			continue
		}
		if found != nil {
			return nil, fmt.Errorf("multiple ready runtime-agent Pods on node")
		}
		found = runtimeProbeObject(item)
	}
	if found == nil {
		return nil, fmt.Errorf("ready runtime-agent Pod unavailable on node")
	}
	return found, nil
}

func runtimeAgentOwnedBy(pod obj, nodeName, daemonSetUID string) bool {
	if pod == nil || daemonSetUID == "" || nested(pod, "spec", "nodeName") != nodeName || text(nested(pod, "metadata", "deletionTimestamp")) != "" {
		return false
	}
	owners := runtimeProbeList(nested(pod, "metadata", "ownerReferences"))
	return len(owners) == 1 && nested(owners[0], "apiVersion") == "apps/v1" && nested(owners[0], "kind") == "DaemonSet" &&
		nested(owners[0], "name") == runtimeAgentDSName && nested(owners[0], "uid") == daemonSetUID && nested(owners[0], "controller") == true
}

func runtimeProbeList(value any) []any {
	items, _ := value.([]any)
	return items
}

func runtimeProbeObject(value any) obj {
	object, _ := value.(map[string]any)
	return object
}
