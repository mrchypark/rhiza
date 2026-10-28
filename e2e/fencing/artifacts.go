package main

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recoveryanchor"
)

func publicRuntime(runtime obj) obj {
	return obj{"status": obj{"id": nested(runtime, "status", "id"), "imageRef": nested(runtime, "status", "imageRef"), "labels": obj{"io.kubernetes.pod.uid": nested(runtime, "status", "labels", "io.kubernetes.pod.uid")}}}
}

func publicCRIImage(cri obj) obj {
	return obj{"status": obj{"id": nested(cri, "status", "id"), "repoDigests": nested(cri, "status", "repoDigests")}}
}

type descriptorView struct {
	Config struct {
		Digest string `json:"digest"`
	} `json:"config"`
	Manifests []struct {
		Digest    string `json:"digest"`
		Size      int64  `json:"size"`
		MediaType string `json:"mediaType"`
		Platform  struct {
			OS           string `json:"os"`
			Architecture string `json:"architecture"`
		} `json:"platform"`
	} `json:"manifests,omitempty"`
}

func publicDescriptor(data []byte) descriptorView {
	var view descriptorView
	_ = json.Unmarshal(data, &view)
	return view
}

func sanitizeRuntimeDiagnostics(data []byte) ([]byte, error) {
	var view runtimeDiagnosticsView
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&view); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("trailing runtime diagnostics")
	}
	if _, err := time.Parse(time.RFC3339Nano, view.CapturedAt); err != nil || view.PodCount != len(view.Pods) || view.PodCount > 16 {
		return nil, fmt.Errorf("invalid runtime diagnostics summary")
	}
	allowedSignals := map[string]bool{"error": true, "failed": true, "refused": true, "ready": true, "listening": true, "bootstrap": true, "membership": true, "restore": true}
	allowedConditions := map[string]bool{"Ready": true, "Initialized": true, "PodScheduled": true, "ContainersReady": true, "PodReadyToStartContainers": true}
	allowedReasons := map[string]bool{"CrashLoopBackOff": true, "ContainerCreating": true, "CreateContainerConfigError": true, "CreateContainerError": true, "ContainerCannotRun": true, "RunContainerError": true, "ErrImagePull": true, "ImagePullBackOff": true, "InvalidImageName": true, "OOMKilled": true, "Completed": true, "Error": true, "PodCompleted": true, "PodInitializing": true, "NodeLost": true, "Evicted": true, "DeadlineExceeded": true, "Shutdown": true, "StartError": true, "Preempting": true, "Unschedulable": true, "ContainersNotReady": true, "PodReadyToStartContainers": true}
	for i := range view.Pods {
		pod := &view.Pods[i]
		if pod.Name != "rhiza-0" && pod.Name != "rhiza-1" && pod.Name != "rhiza-2" || !validPodUID(pod.UID) || len(pod.Containers) > 16 || len(pod.Conditions) > 32 || len(pod.Logs) > 32 {
			return nil, fmt.Errorf("invalid runtime Pod identity or bounds")
		}
		if pod.NodeName != kindNode {
			pod.NodeName = ""
		}
		if pod.Phase != "Pending" && pod.Phase != "Running" && pod.Phase != "Succeeded" && pod.Phase != "Failed" && pod.Phase != "Unknown" {
			pod.Phase = ""
		}
		if pod.CreatedAt != "" {
			if _, err := time.Parse(time.RFC3339Nano, pod.CreatedAt); err != nil {
				pod.CreatedAt = ""
			}
		}
		filteredConditions := pod.Conditions[:0]
		for _, condition := range pod.Conditions {
			if !allowedConditions[condition.Type] || condition.Status != "True" && condition.Status != "False" && condition.Status != "Unknown" {
				continue
			}
			if !allowedReasons[condition.Reason] {
				condition.Reason = ""
			}
			filteredConditions = append(filteredConditions, condition)
		}
		pod.Conditions = filteredConditions
		for j := range pod.Containers {
			container := &pod.Containers[j]
			if container.Name != "rhiza" && container.Name != "rhiza-prepare" || container.State != "" && container.State != "waiting" && container.State != "running" && container.State != "terminated" {
				return nil, fmt.Errorf("invalid runtime container status")
			}
			if !allowedReasons[container.Reason] {
				container.Reason = ""
			}
		}
		for j := range pod.Logs {
			log := &pod.Logs[j]
			if log.Container != "rhiza" && log.Container != "rhiza-prepare" || log.LineCount < 0 || log.LineCount > 200 {
				return nil, fmt.Errorf("invalid runtime log summary")
			}
			if log.Available {
				if len(log.SHA256) != 64 {
					return nil, fmt.Errorf("invalid runtime log digest")
				}
				if _, err := hex.DecodeString(log.SHA256); err != nil {
					return nil, err
				}
			} else if log.SHA256 != "" || log.LineCount != 0 || len(log.Signals) != 0 {
				return nil, fmt.Errorf("unavailable runtime log has content")
			}
			for _, signal := range log.Signals {
				if !allowedSignals[signal] {
					return nil, fmt.Errorf("invalid runtime log signal")
				}
			}
		}
	}
	return json.Marshal(view)
}

func validPodUID(value string) bool {
	if len(value) != 36 {
		return false
	}
	for i, char := range value {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if char != '-' {
				return false
			}
			continue
		}
		if char >= 'a' && char <= 'f' || char >= '0' && char <= '9' {
			continue
		}
		return false
	}
	return true
}

type imageView struct {
	Pod        string `json:"pod"`
	UID        string `json:"uid"`
	Role       string `json:"role"`
	PodImageID string `json:"podImageID"`
	Container  struct {
		Status struct {
			ID       string `json:"id"`
			ImageRef string `json:"imageRef"`
			Labels   struct {
				PodUID string `json:"io.kubernetes.pod.uid"`
			} `json:"labels"`
		} `json:"status"`
	} `json:"container"`
	Resolution []struct {
		Digest          string         `json:"digest"`
		CRILookupFailed bool           `json:"criLookupFailed,omitempty"`
		ContentSHA256   string         `json:"contentSHA256,omitempty"`
		Descriptor      descriptorView `json:"descriptor"`
		CRI             struct {
			Status struct {
				ID          string   `json:"id"`
				RepoDigests []string `json:"repoDigests"`
			} `json:"status"`
		} `json:"cri"`
	} `json:"resolution"`
	BuiltConfigID  string   `json:"builtConfigID"`
	CRIConfigID    string   `json:"criConfigID"`
	CRIRepoDigests []string `json:"criRepoDigests"`
}

type proofView struct {
	Request            operator.FenceRequest `json:"request"`
	Proof              operator.FenceProof   `json:"proof"`
	Ledger             fenceLedgerState      `json:"ledger"`
	OldUIDs            []string              `json:"oldUIDs"`
	LedgerVerification string                `json:"ledgerVerification"`
}

type eventView struct {
	Event                          string                 `json:"event"`
	Time                           string                 `json:"time"`
	FenceOperationID               string                 `json:"fenceOperationID,omitempty"`
	AnchorOperationID              string                 `json:"anchorOperationID,omitempty"`
	TargetClusterID                string                 `json:"targetClusterID,omitempty"`
	AnchorStage                    string                 `json:"stage,omitempty"`
	AnchorPhase                    string                 `json:"phase,omitempty"`
	AnchorRecordState              string                 `json:"anchorRecordState,omitempty"`
	CorruptBytesSHA256             string                 `json:"corruptBytesSHA256,omitempty"`
	AnchorArtifactSHA256           string                 `json:"anchorArtifactSHA256,omitempty"`
	PreRuntimeAssignmentObserved   bool                   `json:"preRuntimeAssignmentObserved,omitempty"`
	Rows                           json.RawMessage        `json:"rows,omitempty"`
	Voters                         int                    `json:"voters,omitempty"`
	ConfigID                       uint                   `json:"configID,omitempty"`
	Targets                        []operator.FenceTarget `json:"targets,omitempty"`
	OldUIDs                        []string               `json:"oldUIDs,omitempty"`
	Target                         *operator.FenceTarget  `json:"target,omitempty"`
	Request                        *operator.FenceRequest `json:"request,omitempty"`
	Proof                          *operator.FenceProof   `json:"proof,omitempty"`
	AppliedSlot                    uint64                 `json:"appliedSlot,omitempty"`
	ConsensusTip                   uint64                 `json:"consensusTip,omitempty"`
	SourceACKRows                  int                    `json:"sourceACKRows,omitempty"`
	OnlineVoterReplacement         bool                   `json:"onlineVoterReplacement,omitempty"`
	NewGenerationWrite             bool                   `json:"newGenerationWrite,omitempty"`
	OldRuntimesGone                bool                   `json:"oldRuntimesGone,omitempty"`
	IndependentArchiveTip          uint64                 `json:"independentArchiveTip,omitempty"`
	IndependentArchiveStart        uint64                 `json:"independentArchiveStart,omitempty"`
	IndependentArchiveThrough      uint64                 `json:"independentArchiveThrough,omitempty"`
	IndependentArchivePrefixSHA256 string                 `json:"independentArchivePrefixSHA256,omitempty"`
	ArchivedRequestIDs             []string               `json:"archivedRequestIDs,omitempty"`
	AbsentPeerProbeRequestIDs      []string               `json:"absentPeerProbeRequestIDs,omitempty"`
	AbsentPeerProbeMarkers         []string               `json:"absentPeerProbeMarkers,omitempty"`
	Executor                       string                 `json:"executor,omitempty"`
	DRTargetClusterID              string                 `json:"drTargetClusterID,omitempty"`
	DRWriteEpoch                   int                    `json:"drWriteEpoch,omitempty"`
	DRReadbackSHA256               string                 `json:"drReadbackSHA256,omitempty"`
	DRAssignmentsObserved          bool                   `json:"drAssignmentsObserved,omitempty"`
	ContainerID                    string                 `json:"containerID,omitempty"`
	ImageID                        string                 `json:"imageID,omitempty"`
	PID                            string                 `json:"pid,omitempty"`
	OldIdentity                    *oldPeerIdentity       `json:"oldIdentity,omitempty"`
	PeerProbe                      *peerProbeResult       `json:"peerProbe,omitempty"`
	Fault                          *struct {
		OperatorPod string               `json:"operatorPod"`
		OperatorUID string               `json:"operatorUID"`
		ContainerID string               `json:"containerID"`
		Target      operator.FenceTarget `json:"target"`
		Removed     bool                 `json:"removed"`
	} `json:"fault,omitempty"`
}

// verifyAnchorGateEvidence validates the bounded event packet emitted by the
// cooperative runtime. It does not infer success from a PASS record alone:
// each missing/pending/corrupt gate must precede exact restoration and the
// same retained operation must later receive its runtime ACK.
func verifyAnchorGateEvidence(eventsPath, qualificationPath, sourceHeadPath, expectedSourceHead string) (obj, error) {
	file, err := os.Open(eventsPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	scan := bufio.NewScanner(file)
	scan.Buffer(make([]byte, 4096), 1<<20)
	var gates []eventView
	var gateEventIndexes []int
	var ack, pass *eventView
	var ackIndex, passIndex int
	var ackCount, passCount int
	eventIndex := 0
	for scan.Scan() {
		var event eventView
		decoder := json.NewDecoder(bytes.NewReader(scan.Bytes()))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&event); err != nil {
			return nil, fmt.Errorf("decode bounded event: %w", err)
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			return nil, fmt.Errorf("trailing event data")
		}
		switch event.Event {
		case "anchor_gate_missing_record", "anchor_gate_pending_write", "anchor_gate_corrupt_target", "anchor_gate_restored_exact_target":
			gates = append(gates, event)
			gateEventIndexes = append(gateEventIndexes, eventIndex)
		case "anchor_gates_restored":
			gates = append(gates, event)
			gateEventIndexes = append(gateEventIndexes, eventIndex)
		case "dr_successor_runtime_ack":
			copy := event
			ack = &copy
			ackIndex = eventIndex
			ackCount++
		case "PASS":
			copy := event
			pass = &copy
			passIndex = eventIndex
			passCount++
		}
		eventIndex++
	}
	if err := scan.Err(); err != nil {
		return nil, err
	}
	if len(gates) != 5 || ack == nil || pass == nil || ackCount != 1 || passCount != 1 {
		return nil, fmt.Errorf("anchor gate sequence or final runtime evidence is incomplete")
	}
	if ackIndex <= gateEventIndexes[4] || passIndex <= ackIndex {
		return nil, fmt.Errorf("final runtime ACK and PASS must follow all anchor gates in order")
	}
	first := gates[0]
	if first.Event != "anchor_gate_missing_record" || first.AnchorRecordState != "missing" ||
		gates[1].Event != "anchor_gate_pending_write" || gates[1].AnchorRecordState != "pending_write" ||
		gates[2].Event != "anchor_gate_corrupt_target" || gates[2].AnchorRecordState != "corrupt" ||
		gates[3].Event != "anchor_gate_restored_exact_target" || gates[3].AnchorRecordState != "restored_exact" ||
		gates[4].Event != "anchor_gates_restored" || !gates[4].PreRuntimeAssignmentObserved {
		return nil, fmt.Errorf("anchor gates are missing or out of order")
	}
	if first.FenceOperationID == "" || first.AnchorOperationID == "" || first.TargetClusterID == "" {
		return nil, fmt.Errorf("anchor gate identity is incomplete")
	}
	for i, gate := range gates {
		if gate.FenceOperationID != first.FenceOperationID || gate.AnchorOperationID != first.AnchorOperationID || gate.TargetClusterID != first.TargetClusterID {
			return nil, fmt.Errorf("anchor gate identity changed at step %d", i)
		}
		if i < 3 && (gate.AnchorStage != "Sealed" || gate.AnchorPhase != "Blocked") {
			return nil, fmt.Errorf("anchor gate %d was not observed sealed and blocked", i)
		}
	}
	if len(gates[2].CorruptBytesSHA256) != 64 || len(gates[3].AnchorArtifactSHA256) != 64 {
		return nil, fmt.Errorf("anchor corruption/restoration digest is absent")
	}
	if _, err := hex.DecodeString(gates[2].CorruptBytesSHA256); err != nil {
		return nil, fmt.Errorf("invalid corrupted-anchor digest")
	}
	if _, err := hex.DecodeString(gates[3].AnchorArtifactSHA256); err != nil {
		return nil, fmt.Errorf("invalid restored-anchor digest")
	}
	if ack.DRTargetClusterID != first.TargetClusterID || ack.DRWriteEpoch != 2 || !ack.DRAssignmentsObserved || len(ack.DRReadbackSHA256) != 64 {
		return nil, fmt.Errorf("final runtime ACK is not bound to the recovered target")
	}
	if _, err := hex.DecodeString(ack.DRReadbackSHA256); err != nil {
		return nil, fmt.Errorf("invalid final runtime readback digest")
	}
	qualificationBytes, err := os.ReadFile(qualificationPath)
	if err != nil {
		return nil, err
	}
	if _, err := safeJSON("dr-qualification.json", qualificationBytes); err != nil {
		return nil, fmt.Errorf("qualification artifact is not safe or valid: %w", err)
	}
	var qualification struct {
		Version                      int                    `json:"version"`
		TargetClusterID              string                 `json:"targetClusterID"`
		AnchorID                     string                 `json:"anchorID"`
		RecoveryName                 string                 `json:"recoveryName"`
		RecoveryUID                  string                 `json:"recoveryUID"`
		OperationID                  string                 `json:"operationID"`
		RequestHash                  string                 `json:"requestHash"`
		Receipt                      recoveryanchor.Receipt `json:"receipt"`
		Assignments                  []drAssignmentView     `json:"assignments"`
		TargetVoters                 int                    `json:"targetVoters"`
		InitSucceeded                bool                   `json:"initSucceeded"`
		PreRuntimeAssignmentObserved bool                   `json:"preRuntimeAssignmentObserved"`
		RuntimeWALBound              bool                   `json:"runtimeWALBound"`
	}
	qualificationDecoder := json.NewDecoder(bytes.NewReader(qualificationBytes))
	qualificationDecoder.DisallowUnknownFields()
	if err := qualificationDecoder.Decode(&qualification); err != nil {
		return nil, fmt.Errorf("decode bounded DR qualification: %w", err)
	}
	if err := qualificationDecoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("trailing DR qualification data")
	}
	if qualification.Version != 1 || qualification.AnchorID != recoveryAnchorID || qualification.RecoveryName == "" ||
		!validPodUID(qualification.RecoveryUID) || qualification.OperationID != qualification.RecoveryUID+":"+qualification.RecoveryName ||
		qualification.OperationID != first.AnchorOperationID || qualification.TargetClusterID != first.TargetClusterID ||
		qualification.OperationID != qualification.Receipt.OperationID || qualification.RequestHash == "" ||
		qualification.RequestHash != qualification.Receipt.RequestHash || qualification.Receipt.AnchorID != recoveryAnchorID ||
		qualification.Receipt.Target.ClusterID != first.TargetClusterID || qualification.Receipt.Generation == 0 ||
		qualification.TargetVoters != 3 || !qualification.InitSucceeded || !qualification.PreRuntimeAssignmentObserved ||
		!qualification.RuntimeWALBound || len(qualification.Assignments) != 3 {
		return nil, fmt.Errorf("DR qualification does not prove the exact anchor operation and target")
	}
	if _, err := hex.DecodeString(qualification.RequestHash); err != nil {
		return nil, fmt.Errorf("invalid DR qualification request digest")
	}
	if !pass.OldRuntimesGone || !pass.NewGenerationWrite || !pass.OnlineVoterReplacement {
		return nil, fmt.Errorf("final PASS record lacks replacement, termination, or write evidence")
	}
	sourceHeadBytes, err := os.ReadFile(sourceHeadPath)
	if err != nil {
		return nil, err
	}
	sourceHead := strings.TrimSpace(string(sourceHeadBytes))
	if len(expectedSourceHead) != 40 || sourceHead != expectedSourceHead {
		return nil, fmt.Errorf("cooperative runtime evidence source revision does not match this workflow revision")
	}
	if _, err := hex.DecodeString(sourceHead); err != nil {
		return nil, fmt.Errorf("invalid cooperative runtime source revision")
	}
	return obj{"fenceOperationID": first.FenceOperationID, "anchorOperationID": first.AnchorOperationID, "targetClusterID": first.TargetClusterID, "sourceHead": sourceHead, "gateCount": len(gates), "qualificationBound": true, "runtimeAck": true, "oldRuntimesGone": pass.OldRuntimesGone, "newGenerationWrite": pass.NewGenerationWrite}, nil
}

type runtimeDiagnosticsView struct {
	CapturedAt string           `json:"capturedAt"`
	PodCount   int              `json:"podCount"`
	Pods       []runtimePodView `json:"pods"`
}

type runtimePodView struct {
	Name       string                  `json:"name"`
	UID        string                  `json:"uid"`
	NodeName   string                  `json:"nodeName,omitempty"`
	CreatedAt  string                  `json:"createdAt,omitempty"`
	Phase      string                  `json:"phase,omitempty"`
	Conditions []runtimeConditionView  `json:"conditions,omitempty"`
	Containers []runtimeContainerView  `json:"containers,omitempty"`
	Logs       []runtimeLogSummaryView `json:"logs,omitempty"`
}

type runtimeConditionView struct {
	Type   string `json:"type"`
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
}

type runtimeContainerView struct {
	Name         string `json:"name"`
	Ready        bool   `json:"ready"`
	RestartCount int64  `json:"restartCount"`
	State        string `json:"state,omitempty"`
	Reason       string `json:"reason,omitempty"`
	ExitCode     *int64 `json:"exitCode,omitempty"`
}

// Log text is deliberately not published: even application errors can contain
// credentials or object-store URLs. These bounded summaries establish which
// logs were available and their broad startup/failure signals without exposing
// their contents.
type runtimeLogSummaryView struct {
	Container string   `json:"container"`
	Available bool     `json:"available"`
	LineCount int      `json:"lineCount"`
	SHA256    string   `json:"sha256"`
	Signals   []string `json:"signals,omitempty"`
}

type drAssignmentView struct {
	Pod           string `json:"pod"`
	PodUID        string `json:"podUID"`
	NodeID        string `json:"nodeID"`
	CredentialUID string `json:"credentialUID"`
	InitExitCode  int    `json:"initExitCode"`
}

func safeJSON(name string, data []byte) ([]byte, error) {
	var value any
	switch {
	case strings.HasSuffix(name, "-image-evidence.json"):
		value = &[]imageView{}
	case name == "validated-voter-proof.json" || name == "validated-generation-proof.json":
		value = &proofView{}
	case name == "online-active-ledger.json":
		value = &fenceLedgerState{}
	case name == "resources-summary.json":
		var source struct {
			Items []obj `json:"items"`
		}
		if err := json.Unmarshal(data, &source); err != nil {
			return nil, err
		}
		var summary []obj
		for _, item := range source.Items {
			summary = append(summary, obj{"kind": item["kind"], "name": nested(item, "metadata", "name"), "uid": nested(item, "metadata", "uid"), "namespace": nested(item, "metadata", "namespace"), "deletionTimestamp": nested(item, "metadata", "deletionTimestamp"), "phase": nested(item, "status", "phase"), "stage": nested(item, "status", "stage"), "activeClusterID": nested(item, "status", "activeClusterID")})
		}
		return encoded(summary), nil
	case name == "runtime-diagnostics.json":
		return sanitizeRuntimeDiagnostics(data)
	case name == "learner-authenticated-binding.json":
		value = &struct {
			PodUID         string                        `json:"podUID"`
			NodeID         string                        `json:"nodeID"`
			WALIdentity    string                        `json:"walIdentity"`
			RootUID        string                        `json:"rootUID"`
			StatefulSetUID string                        `json:"statefulSetUID"`
			CredentialUID  string                        `json:"credentialUID"`
			Member         quepaxa.Member                `json:"member"`
			Addition       operator.MembershipAddJournal `json:"addition"`
		}{}
	case name == "online-completed-membership.json":
		var source struct {
			Recovery     operator.Resource      `json:"recovery"`
			Targets      []operator.FenceTarget `json:"targets"`
			Observations []struct {
				PodUID     string                   `json:"podUID"`
				Pod        string                   `json:"pod"`
				Membership network.MembershipStatus `json:"membership"`
			} `json:"observations"`
		}
		if err := json.Unmarshal(data, &source); err != nil {
			return nil, err
		}
		source.Recovery.Metadata = obj{"name": source.Recovery.Metadata["name"], "uid": source.Recovery.Metadata["uid"], "namespace": source.Recovery.Metadata["namespace"]}
		return encoded(source), nil
	case name == "dr-qualification.json":
		var source struct {
			Version                      int                    `json:"version"`
			TargetClusterID              string                 `json:"targetClusterID"`
			AnchorID                     string                 `json:"anchorID"`
			RecoveryName                 string                 `json:"recoveryName"`
			RecoveryUID                  string                 `json:"recoveryUID"`
			OperationID                  string                 `json:"operationID"`
			RequestHash                  string                 `json:"requestHash"`
			Receipt                      recoveryanchor.Receipt `json:"receipt"`
			Assignments                  []drAssignmentView     `json:"assignments"`
			TargetVoters                 int                    `json:"targetVoters"`
			InitSucceeded                bool                   `json:"initSucceeded"`
			PreRuntimeAssignmentObserved bool                   `json:"preRuntimeAssignmentObserved"`
			RuntimeWALBound              bool                   `json:"runtimeWALBound"`
		}
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&source); err != nil {
			return nil, err
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			return nil, fmt.Errorf("trailing DR qualification data")
		}
		if source.Version != 1 || source.TargetClusterID == "" || source.AnchorID != recoveryAnchorID || source.RecoveryName == "" || !validPodUID(source.RecoveryUID) || source.OperationID == "" || len(source.RequestHash) != 64 || source.Receipt.AnchorID != source.AnchorID || source.Receipt.OperationID != source.OperationID || source.Receipt.RequestHash != source.RequestHash || source.Receipt.Target.ClusterID != source.TargetClusterID || source.Receipt.Generation == 0 || source.TargetVoters != 3 || !source.InitSucceeded || !source.PreRuntimeAssignmentObserved || !source.RuntimeWALBound || len(source.Assignments) != 3 {
			return nil, fmt.Errorf("invalid DR qualification summary")
		}
		seen := map[string]bool{}
		for _, assignment := range source.Assignments {
			if assignment.Pod != "rhiza-0" && assignment.Pod != "rhiza-1" && assignment.Pod != "rhiza-2" || !validPodUID(assignment.PodUID) || assignment.NodeID == "" || seen[assignment.PodUID] || assignment.CredentialUID == "" || assignment.InitExitCode != 0 {
				return nil, fmt.Errorf("invalid DR assignment summary")
			}
			seen[assignment.PodUID] = true
		}
		return encoded(source), nil
	default:
		return nil, fmt.Errorf("artifact type is not approved")
	}
	if err := json.Unmarshal(data, value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

// Only explicit evidence types enter the public packet; raw diagnostics stay on the disposable runner.
func safeArtifacts(raw string) error {
	if raw == "" {
		return fmt.Errorf("FENCING_E2E_OUT is required")
	}
	public := filepath.Join(raw, "public")
	if err := os.Mkdir(public, 0700); err != nil {
		return err
	}
	entries, err := os.ReadDir(raw)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() {
			continue
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("artifact symlinks are unsupported")
		}
		approved := strings.HasSuffix(name, "-image-evidence.json") || name == "validated-voter-proof.json" || name == "validated-generation-proof.json" || name == "online-active-ledger.json" || name == "learner-authenticated-binding.json" || name == "online-completed-membership.json" || name == "dr-qualification.json" || name == "terminal.json" || name == "runtime-diagnostics.json"
		textFile := name == "source-head.txt" || name == "image-ids.txt" || name == "actual-exit.txt" || name == "pre-cleanup-exit.txt"
		if !approved && !textFile && name != "events.jsonl" && name != "cleanup.log" {
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() || info.Size() > 16<<20 {
			return fmt.Errorf("artifact is unavailable or oversized")
		}
		data, err := os.ReadFile(filepath.Join(raw, name))
		if err != nil {
			return err
		}
		switch {
		case approved:
			if name == "terminal.json" {
				name = "resources-summary.json"
			}
			data, err = safeJSON(name, data)
		case name == "events.jsonl":
			var result bytes.Buffer
			scan := bufio.NewScanner(bytes.NewReader(data))
			scan.Buffer(make([]byte, 4096), 1<<20)
			for scan.Scan() {
				var event eventView
				if err = json.Unmarshal(scan.Bytes(), &event); err != nil {
					return err
				}
				if len(event.Rows) > 0 && string(event.Rows) != "1" && string(event.Rows) != "2" && string(event.Rows) != "3" && string(event.Rows) != `[[1,"acknowledged"],[2,"quorum-survived"],[3,"learner-promoted"],[4,"recovered"]]` {
					return fmt.Errorf("unexpected public readback values")
				}
				if event.Event == "dr_successor_runtime_ack" {
					if len(event.Rows) != 0 || event.DRTargetClusterID == "" || event.DRWriteEpoch != 2 || !event.DRAssignmentsObserved || len(event.DRReadbackSHA256) != 64 {
						return fmt.Errorf("invalid bounded DR runtime ACK summary")
					}
					if _, err := hex.DecodeString(event.DRReadbackSHA256); err != nil {
						return fmt.Errorf("invalid DR readback digest")
					}
				}
				if err = json.NewEncoder(&result).Encode(event); err != nil {
					return err
				}
			}
			if err = scan.Err(); err != nil {
				return err
			}
			data = result.Bytes()
		case name == "cleanup.log":
			data = encoded(obj{"ownedKindDeleted": bytes.Contains(data, []byte(`Deleted nodes: ["rhiza-generic-fencing-control-plane"]`))})
		case textFile:
			lines := strings.Fields(string(data))
			if len(lines) == 0 || name != "image-ids.txt" && len(lines) != 1 {
				return fmt.Errorf("missing or ambiguous public identity receipt")
			}
			for i, line := range lines {
				if name == "actual-exit.txt" || name == "pre-cleanup-exit.txt" {
					if line != "0" && line != "1" && line != "130" && line != "143" {
						return fmt.Errorf("invalid public exit receipt")
					}
					continue
				}
				if name == "source-head.txt" {
					if len(line) != 40 {
						return fmt.Errorf("invalid public source SHA")
					}
					if _, e := hex.DecodeString(line); e != nil {
						return e
					}
					continue
				}
				canonical, e := immutableDigest(line)
				if e != nil {
					return e
				}
				lines[i] = canonical
			}
			data = []byte(strings.Join(lines, "\n") + "\n")
		}
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(public, name), data, 0600); err != nil {
			return err
		}
	}
	for _, name := range []string{"actual-exit.txt", "source-head.txt", "image-ids.txt"} {
		if _, err := os.Stat(filepath.Join(public, name)); err != nil {
			return fmt.Errorf("required public receipt missing: %s", name)
		}
	}
	return nil
}
