package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
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
	Event                  string                 `json:"event"`
	Time                   string                 `json:"time"`
	Rows                   json.RawMessage        `json:"rows,omitempty"`
	Voters                 int                    `json:"voters,omitempty"`
	ConfigID               uint                   `json:"configID,omitempty"`
	Targets                []operator.FenceTarget `json:"targets,omitempty"`
	OldUIDs                []string               `json:"oldUIDs,omitempty"`
	Target                 *operator.FenceTarget  `json:"target,omitempty"`
	Request                *operator.FenceRequest `json:"request,omitempty"`
	Proof                  *operator.FenceProof   `json:"proof,omitempty"`
	AppliedSlot            uint64                 `json:"appliedSlot,omitempty"`
	ConsensusTip           uint64                 `json:"consensusTip,omitempty"`
	SourceACKRows          int                    `json:"sourceACKRows,omitempty"`
	OnlineVoterReplacement bool                   `json:"onlineVoterReplacement,omitempty"`
	NewGenerationWrite     bool                   `json:"newGenerationWrite,omitempty"`
	OldRuntimesGone        bool                   `json:"oldRuntimesGone,omitempty"`
	Executor               string                 `json:"executor,omitempty"`
	ContainerID            string                 `json:"containerID,omitempty"`
	ImageID                string                 `json:"imageID,omitempty"`
	PID                    string                 `json:"pid,omitempty"`
	Fault                  *struct {
		OperatorPod string               `json:"operatorPod"`
		OperatorUID string               `json:"operatorUID"`
		ContainerID string               `json:"containerID"`
		Target      operator.FenceTarget `json:"target"`
		Removed     bool                 `json:"removed"`
	} `json:"fault,omitempty"`
}

type fencerDiagnosticsView struct {
	Context            string `json:"context"`
	Namespace          string `json:"namespace"`
	PodsCaptured       bool   `json:"podsCaptured"`
	EventsCaptured     bool   `json:"eventsCaptured"`
	DeploymentCaptured bool   `json:"deploymentCaptured"`
	Logs               struct {
		Captured bool   `json:"captured"`
		SHA256   string `json:"sha256"`
		Bytes    int64  `json:"bytes"`
		Lines    int64  `json:"lines"`
	} `json:"logs"`
	RuntimeAgent struct {
		PodName string `json:"podName"`
		PodUID  string `json:"podUID"`
		Logs    struct {
			Captured bool   `json:"captured"`
			SHA256   string `json:"sha256"`
			Bytes    int64  `json:"bytes"`
			Lines    int64  `json:"lines"`
		} `json:"logs"`
		Diagnostics          []runtimeAgentEnrollmentDiagnostic `json:"diagnostics"`
		DiagnosticsTruncated bool                               `json:"diagnosticsTruncated,omitempty"`
	} `json:"runtimeAgent"`
}

type runtimeAgentEnrollmentDiagnostic struct {
	Stage string `json:"stage"`
	Code  string `json:"code"`
}

const maxRuntimeAgentDiagnostics = 20

var allowedRuntimeAgentDiagnosticPairs = map[string]struct{}{
	"request_validation/authority_mismatch":                    {},
	"state_read/binding_conflict":                              {},
	"state_read/state_unavailable":                             {},
	"cri_enroll/binding_incomplete":                            {},
	"cri_enroll/profile_unsupported":                           {},
	"cri_enroll/host_identity_unavailable":                     {},
	"cri_enroll/host_identity_mismatch":                        {},
	"cri_enroll/runtime_api_unavailable":                       {},
	"cri_enroll/runtime_profile_mismatch":                      {},
	"cri_enroll/runtime_binding_invalid":                       {},
	"cri_enroll/container_id_unqualified":                      {},
	"cri_enroll/container_metadata_mismatch":                   {},
	"cri_enroll/containerd_verbose_info_missing":               {},
	"cri_enroll/containerd_verbose_info_invalid":               {},
	"cri_enroll/containerd_verbose_field_duplicate":            {},
	"cri_enroll/containerd_verbose_field_missing":              {},
	"cri_enroll/containerd_verbose_schema_mismatch":            {},
	"cri_enroll/containerd_verbose_identity_mismatch":          {},
	"cri_enroll/containerd_pid_changed_during_capture":         {},
	"cri_enroll/sandbox_status_unavailable":                    {},
	"cri_enroll/sandbox_metadata_mismatch":                     {},
	"cri_enroll/execution_namespace_mismatch":                  {},
	"cri_enroll/container_status_unavailable":                  {},
	"cri_enroll/sandbox_inventory_unavailable":                 {},
	"cri_enroll/sandbox_inventory_missing":                     {},
	"cri_enroll/sandbox_inventory_ambiguous":                   {},
	"cri_enroll/target_container_metadata_mismatch":            {},
	"cri_enroll/target_container_outside_sandbox":              {},
	"cri_enroll/cgroup_capture_configuration_invalid":          {},
	"cri_enroll/process_identity_unavailable":                  {},
	"cri_enroll/process_cgroup_unavailable":                    {},
	"cri_enroll/process_cgroup_path_root":                      {},
	"cri_enroll/process_cgroup_path_absolute":                  {},
	"cri_enroll/process_cgroup_path_dot":                       {},
	"cri_enroll/process_cgroup_path_noncanonical":              {},
	"cri_enroll/process_cgroup_path_traversal":                 {},
	"cri_enroll/process_cgroup_path_invalid":                   {},
	"cri_enroll/process_cgroup_path_missing":                   {},
	"cri_enroll/cgroup_namespace_all_same":                     {},
	"cri_enroll/cgroup_namespace_agent_host_same_target_other": {},
	"cri_enroll/cgroup_namespace_agent_target_same_host_other": {},
	"cri_enroll/cgroup_namespace_host_target_same_agent_other": {},
	"cri_enroll/cgroup_namespace_all_different":                {},
	"cri_enroll/cgroup_namespace_agent_unavailable":            {},
	"cri_enroll/cgroup_namespace_host_unavailable":             {},
	"cri_enroll/cgroup_namespace_target_unavailable":           {},
	"cri_enroll/cgroup_namespace_multiple_unavailable":         {},
	"cri_enroll/cgroup_root_unavailable":                       {},
	"cri_enroll/cgroup_scope_unavailable":                      {},
	"cri_enroll/cgroup_identity_unavailable":                   {},
	"cri_enroll/cgroup_not_populated":                          {},
	"cri_enroll/process_changed_during_capture":                {},
	"cri_enroll/cri_scope_unverified":                          {},
	"cri_enroll/backend_error":                                 {},
	"backend_result_validation/binding_invalid":                {},
	"backend_result_validation/binding_mismatch":               {},
	"backend_result_validation/scope_invalid":                  {},
	"backend_result_validation/scope_digest_mismatch":          {},
	"state_persist/state_write_failed":                         {},
}

type fencerPodsView struct {
	Pods []struct {
		Name       string `json:"name"`
		UID        string `json:"uid"`
		Phase      string `json:"phase"`
		Conditions []struct {
			Type   string `json:"type"`
			Status string `json:"status"`
			Reason string `json:"reason,omitempty"`
		} `json:"conditions"`
		Containers []struct {
			Name         string `json:"name"`
			Ready        bool   `json:"ready"`
			RestartCount int32  `json:"restartCount"`
			State        struct {
				Waiting struct {
					Reason string `json:"reason,omitempty"`
				} `json:"waiting"`
				Terminated struct {
					Reason     string `json:"reason,omitempty"`
					ExitCode   *int32 `json:"exitCode,omitempty"`
					FinishedAt string `json:"finishedAt,omitempty"`
				} `json:"terminated"`
			} `json:"state"`
			LastState struct {
				Terminated struct {
					Reason     string `json:"reason,omitempty"`
					ExitCode   *int32 `json:"exitCode,omitempty"`
					FinishedAt string `json:"finishedAt,omitempty"`
				} `json:"terminated"`
			} `json:"lastState"`
		} `json:"containers"`
	} `json:"pods"`
}

type fencerEventsView struct {
	Warnings []struct {
		Type           string `json:"type"`
		Reason         string `json:"reason"`
		Count          int32  `json:"count"`
		LastTimestamp  string `json:"lastTimestamp"`
		InvolvedObject struct {
			Kind string `json:"kind"`
			Name string `json:"name"`
		} `json:"involvedObject"`
		Source struct {
			Component string `json:"component"`
		} `json:"source"`
	} `json:"warnings"`
}

type fencerDeploymentView struct {
	Name                string `json:"name"`
	UID                 string `json:"uid"`
	Generation          int64  `json:"generation"`
	ObservedGeneration  int64  `json:"observedGeneration"`
	Replicas            int32  `json:"replicas"`
	UpdatedReplicas     int32  `json:"updatedReplicas"`
	ReadyReplicas       int32  `json:"readyReplicas"`
	AvailableReplicas   int32  `json:"availableReplicas"`
	UnavailableReplicas int32  `json:"unavailableReplicas"`
	Conditions          []struct {
		Type               string `json:"type"`
		Status             string `json:"status"`
		Reason             string `json:"reason,omitempty"`
		LastUpdateTime     string `json:"lastUpdateTime,omitempty"`
		LastTransitionTime string `json:"lastTransitionTime,omitempty"`
	} `json:"conditions"`
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
	case name == "runtime-profile.json":
		value = &runtimeProfileEvidence{}
	case name == "runtime-component-probe.json":
		value = &runtimeComponentEvidence{}
	case name == "fencer-diagnostics.json":
		var view fencerDiagnosticsView
		if err := json.Unmarshal(data, &view); err != nil {
			return nil, err
		}
		if err := validateRuntimeAgentDiagnostics(view); err != nil {
			return nil, err
		}
		return json.Marshal(view)
	case name == "fencer-pods.json":
		value = &fencerPodsView{}
	case name == "fencer-events.json":
		value = &fencerEventsView{}
	case name == "fencer-deployment.json":
		value = &fencerDeploymentView{}
	default:
		return nil, fmt.Errorf("artifact type is not approved")
	}
	if err := json.Unmarshal(data, value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

func validateRuntimeAgentDiagnostics(view fencerDiagnosticsView) error {
	agent := view.RuntimeAgent
	if len(agent.Diagnostics) > maxRuntimeAgentDiagnostics || agent.Logs.Bytes < 0 || agent.Logs.Bytes > 65536 || agent.Logs.Lines < 0 || agent.Logs.Lines > 200 {
		return fmt.Errorf("runtime-agent diagnostic receipt exceeds bounds")
	}
	if agent.Logs.Captured {
		if agent.PodName == "" || agent.PodUID == "" || len(agent.Logs.SHA256) != 64 {
			return fmt.Errorf("runtime-agent diagnostic identity is incomplete")
		}
		if _, err := hex.DecodeString(agent.Logs.SHA256); err != nil || strings.ToLower(agent.Logs.SHA256) != agent.Logs.SHA256 {
			return fmt.Errorf("runtime-agent diagnostic log digest is invalid")
		}
	} else if len(agent.Diagnostics) != 0 || agent.DiagnosticsTruncated {
		return fmt.Errorf("runtime-agent diagnostics lack a captured log")
	}
	if agent.DiagnosticsTruncated && len(agent.Diagnostics) != maxRuntimeAgentDiagnostics {
		return fmt.Errorf("runtime-agent diagnostic truncation marker is inconsistent")
	}
	for _, diagnostic := range agent.Diagnostics {
		if _, ok := allowedRuntimeAgentDiagnosticPairs[diagnostic.Stage+"/"+diagnostic.Code]; !ok {
			return fmt.Errorf("runtime-agent diagnostic code is not allowlisted")
		}
	}
	return nil
}

func projectRuntimeAgentDiagnostics(raw string, data []byte) ([]byte, error) {
	var view fencerDiagnosticsView
	if err := json.Unmarshal(data, &view); err != nil {
		return nil, err
	}
	view.RuntimeAgent.Diagnostics = []runtimeAgentEnrollmentDiagnostic{}
	if !view.RuntimeAgent.Logs.Captured {
		return json.Marshal(view)
	}
	path := filepath.Join(raw, "private-fencer-diagnostics", "runtime-agent-current.log")
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > 65536 {
		return nil, fmt.Errorf("runtime-agent private log is unsafe or oversized")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	logData, err := io.ReadAll(io.LimitReader(file, 65537))
	if err != nil || len(logData) > 65536 || bytes.Count(logData, []byte("\n")) > 200 {
		return nil, fmt.Errorf("runtime-agent private log exceeds bounds")
	}
	digest := sha256.Sum256(logData)
	if int64(len(logData)) != view.RuntimeAgent.Logs.Bytes || int64(bytes.Count(logData, []byte("\n"))) != view.RuntimeAgent.Logs.Lines || hex.EncodeToString(digest[:]) != view.RuntimeAgent.Logs.SHA256 {
		return nil, fmt.Errorf("runtime-agent private log does not match its public receipt")
	}
	const marker = "runtime-agent enrollment rejected "
	for _, line := range strings.Split(string(logData), "\n") {
		at := strings.Index(line, marker)
		if at < 0 {
			continue
		}
		fields := strings.Fields(line[at+len(marker):])
		if len(fields) != 2 || !strings.HasPrefix(fields[0], "stage=") || !strings.HasPrefix(fields[1], "code=") {
			continue
		}
		diagnostic := runtimeAgentEnrollmentDiagnostic{Stage: strings.TrimPrefix(fields[0], "stage="), Code: strings.TrimPrefix(fields[1], "code=")}
		if _, ok := allowedRuntimeAgentDiagnosticPairs[diagnostic.Stage+"/"+diagnostic.Code]; !ok {
			continue
		}
		if len(view.RuntimeAgent.Diagnostics) == maxRuntimeAgentDiagnostics {
			view.RuntimeAgent.DiagnosticsTruncated = true
			continue
		}
		view.RuntimeAgent.Diagnostics = append(view.RuntimeAgent.Diagnostics, diagnostic)
	}
	return json.Marshal(view)
}

type gcsAsyncEmptyPublicSummary struct {
	Qualification               string `json:"qualification"`
	Outcome                     string `json:"outcome"`
	Provider                    string `json:"provider"`
	NoPVC                       bool   `json:"noPVC"`
	SourceDurability            string `json:"sourceDurability"`
	AllowDataLoss               bool   `json:"allowDataLoss"`
	SourceAcknowledged          bool   `json:"sourceAcknowledged"`
	SourceAckLostAtEmptyBase    bool   `json:"sourceAckLostAtEmptyBase"`
	PersistedZeroTipBeforeFence bool   `json:"persistedZeroTipBeforeFence"`
	SameHeadBeforeProof         bool   `json:"sameHeadBeforeProof"`
	SealedZeroTipAfterFence     bool   `json:"sealedZeroTipAfterFence"`
	EmptyBaseRecoveryCommitted  bool   `json:"emptyBaseRecoveryCommitted"`
	TargetQuorumVerified        bool   `json:"targetQuorumVerified"`
	EmptyReadbackVerified       bool   `json:"emptyReadbackVerified"`
	TargetAckReadbackVerified   bool   `json:"targetAckReadbackVerified"`
	AckToSeal                   string `json:"ackToSeal,omitempty"`
	PrivateEvidenceSHA256       string `json:"privateEvidenceSHA256,omitempty"`
	PrivateEventsSHA256         string `json:"privateEventsSHA256,omitempty"`
}

const (
	gcsAsyncEmptyEvidenceName   = "gcs-async-empty-evidence.json"
	gcsAsyncEmptyEventsName     = "gcs-async-empty-events.jsonl"
	gcsAsyncEmptySummaryName    = "gcs-async-empty-summary.json"
	maxPrivateQualificationFile = 16 << 20
)

func projectGCSAsyncEmptyEvidence(raw, public string, failedRun bool) (bool, error) {
	entries, err := os.ReadDir(raw)
	if err != nil {
		return false, err
	}
	known := map[string]bool{gcsAsyncEmptyEvidenceName: false, gcsAsyncEmptyEventsName: false}
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, "gcs-async-empty-") {
			if _, ok := known[name]; !ok {
				return false, fmt.Errorf("async-empty private artifact name is not allowlisted")
			}
			known[name] = true
		}
	}
	if !known[gcsAsyncEmptyEvidenceName] && !known[gcsAsyncEmptyEventsName] {
		return false, nil
	}
	if !known[gcsAsyncEmptyEventsName] {
		return false, fmt.Errorf("async-empty evidence exists without its private event record")
	}

	events, err := readBoundedPrivateArtifact(filepath.Join(raw, gcsAsyncEmptyEventsName), true)
	if err != nil {
		return false, err
	}
	eventsHash := sha256.Sum256(events)
	summary := gcsAsyncEmptyPublicSummary{
		Qualification:       "gcs-async-empty-tip-recovery",
		Outcome:             "failed",
		PrivateEventsSHA256: hex.EncodeToString(eventsHash[:]),
	}
	if !known[gcsAsyncEmptyEvidenceName] {
		if !failedRun {
			return false, fmt.Errorf("successful async-empty run is missing its private evidence record")
		}
		return true, writeGCSAsyncEmptySummary(public, summary)
	}
	evidence, err := readBoundedPrivateArtifact(filepath.Join(raw, gcsAsyncEmptyEvidenceName), false)
	if err != nil {
		return false, err
	}
	if err := validateGCSAsyncEmptyPrivateEvidence(evidence, &summary); err != nil {
		return false, err
	}
	evidenceHash := sha256.Sum256(evidence)
	summary.PrivateEvidenceSHA256 = hex.EncodeToString(evidenceHash[:])
	if !failedRun {
		summary.Outcome = "passed"
	}
	return true, writeGCSAsyncEmptySummary(public, summary)
}

func readBoundedPrivateArtifact(name string, allowEmpty bool) ([]byte, error) {
	info, err := os.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || (!allowEmpty && info.Size() <= 0) || info.Size() > maxPrivateQualificationFile {
		return nil, fmt.Errorf("private async-empty artifact is unsafe or oversized")
	}
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxPrivateQualificationFile+1))
	if err != nil || (!allowEmpty && len(data) == 0) || len(data) > maxPrivateQualificationFile {
		return nil, fmt.Errorf("private async-empty artifact could not be read within bounds")
	}
	return data, nil
}

func validateGCSAsyncEmptyPrivateEvidence(data []byte, summary *gcsAsyncEmptyPublicSummary) error {
	fields, err := exactJSONFields(data,
		"qualification", "profile", "sourceAck", "sourceHeadBeforeFence", "sourceHeadBeforeProof", "sealedSourceHead",
		"fence", "proof", "recovery", "sourceAckLost", "targetQuorumVerified", "emptyReadbackVerified",
		"targetAckReadbackVerified", "targetEmptyRows", "targetAck", "ackToSeal", "proofSource")
	if err != nil {
		return err
	}
	var qualification string
	var profile struct {
		Provider         string `json:"provider"`
		Bucket           string `json:"bucket"`
		NoPVC            bool   `json:"noPVC"`
		SourceDurability string `json:"sourceDurability"`
		AllowDataLoss    bool   `json:"allowDataLoss"`
	}
	if err := json.Unmarshal(fields["qualification"], &qualification); err != nil || qualification != "gcs-async-empty-tip-recovery" {
		return fmt.Errorf("private async-empty evidence has an unknown qualification")
	}
	if _, err := exactJSONFields(fields["profile"], "provider", "bucket", "noPVC", "sourceDurability", "allowDataLoss"); err != nil {
		return err
	}
	if err := json.Unmarshal(fields["profile"], &profile); err != nil || profile.Provider != "gcs" || profile.Bucket == "" || !profile.NoPVC || profile.SourceDurability != "async" || !profile.AllowDataLoss {
		return fmt.Errorf("private async-empty evidence profile is incomplete")
	}
	var sourceAck, targetAck gcsAcknowledgement
	if _, err := exactJSONFields(fields["sourceAck"], "requestID", "slot", "rows"); err != nil {
		return err
	}
	if _, err := exactJSONFields(fields["targetAck"], "requestID", "slot", "rows"); err != nil {
		return err
	}
	if err := json.Unmarshal(fields["sourceAck"], &sourceAck); err != nil || sourceAck.RequestID == "" || sourceAck.Slot == 0 || sourceAck.Rows != `[[1,"gcs-async-ack-lost"]]` {
		return fmt.Errorf("private async-empty source ACK receipt is incomplete")
	}
	if err := json.Unmarshal(fields["targetAck"], &targetAck); err != nil || targetAck.RequestID == "" || targetAck.Slot == 0 || targetAck.Rows != `[[2,"gcs-async-target-ack"]]` {
		return fmt.Errorf("private async-empty target ACK readback is incomplete")
	}
	var before, beforeProof, sealed gcsArchiveHeadEvidence
	for name, head := range map[string]*gcsArchiveHeadEvidence{"sourceHeadBeforeFence": &before, "sourceHeadBeforeProof": &beforeProof, "sealedSourceHead": &sealed} {
		if _, err := exactJSONFields(fields[name], "versionType", "version", "tip", "sealed", "headSHA256"); err != nil {
			return err
		}
		if err := json.Unmarshal(fields[name], head); err != nil {
			return fmt.Errorf("private async-empty source HEAD evidence is invalid")
		}
	}
	if err := validateInitialZeroHead(before); err != nil {
		return err
	}
	if err := validateUnchangedZeroHead(before, beforeProof); err != nil {
		return err
	}
	if err := validateSealedZeroHead(before, sealed); err != nil {
		return err
	}
	var fence operator.FenceRequest
	var proof operator.FenceProof
	var recovery operator.Resource
	if err := json.Unmarshal(fields["fence"], &fence); err != nil || fence.OperationID == "" || fence.SourceClusterID == "" {
		return fmt.Errorf("private async-empty generation fence identity is incomplete")
	}
	if err := json.Unmarshal(fields["proof"], &proof); err != nil || proof.OperationID != fence.OperationID || proof.SourceClusterID != fence.SourceClusterID || proof.BindingUID != fence.BindingUID {
		return fmt.Errorf("private async-empty generation proof differs from its request")
	}
	if err := json.Unmarshal(fields["recovery"], &recovery); err != nil || recovery.Spec.RecoveryID != fence.OperationID || recovery.Spec.SourceClusterID != fence.SourceClusterID {
		return fmt.Errorf("private async-empty recovery differs from its source fence")
	}
	if err := recoveryFenceMatchesProof(recovery, fence, proof); err != nil {
		return err
	}
	if err := verifyAsyncEmptyRecovery(recovery, fence, proof); err != nil {
		return err
	}
	var sourceAckLost, targetQuorum, emptyReadback, targetReadback bool
	if err := json.Unmarshal(fields["sourceAckLost"], &sourceAckLost); err != nil || !sourceAckLost {
		return fmt.Errorf("private async-empty evidence does not prove the source ACK was lost")
	}
	if err := json.Unmarshal(fields["targetQuorumVerified"], &targetQuorum); err != nil || !targetQuorum {
		return fmt.Errorf("private async-empty evidence does not prove target quorum")
	}
	if err := json.Unmarshal(fields["emptyReadbackVerified"], &emptyReadback); err != nil || !emptyReadback {
		return fmt.Errorf("private async-empty evidence does not prove the empty-base readback")
	}
	if err := json.Unmarshal(fields["targetAckReadbackVerified"], &targetReadback); err != nil || !targetReadback {
		return fmt.Errorf("private async-empty evidence does not prove the target ACK readback")
	}
	var targetEmptyRows, proofSource, ackToSeal string
	if err := json.Unmarshal(fields["targetEmptyRows"], &targetEmptyRows); err != nil || targetEmptyRows != "[]" {
		return fmt.Errorf("private async-empty target readback is not empty")
	}
	if err := json.Unmarshal(fields["proofSource"], &proofSource); err != nil || proofSource != "RhizaFence status plus direct read-only GCS HEAD read; the generation fence proof is validated against captured workload identities" {
		return fmt.Errorf("private async-empty proof source is not recognized")
	}
	if err := json.Unmarshal(fields["ackToSeal"], &ackToSeal); err != nil {
		return fmt.Errorf("private async-empty ACK-to-seal duration is invalid")
	}
	duration, err := time.ParseDuration(ackToSeal)
	if err != nil || duration < 0 || duration > gcsAsyncEmptyMaxAckToSeal {
		return fmt.Errorf("private async-empty ACK-to-seal duration is outside its bound")
	}

	summary.Qualification = qualification
	summary.Provider = profile.Provider
	summary.NoPVC = profile.NoPVC
	summary.SourceDurability = profile.SourceDurability
	summary.AllowDataLoss = profile.AllowDataLoss
	summary.SourceAcknowledged = true
	summary.SourceAckLostAtEmptyBase = sourceAckLost
	summary.PersistedZeroTipBeforeFence = true
	summary.SameHeadBeforeProof = true
	summary.SealedZeroTipAfterFence = true
	summary.EmptyBaseRecoveryCommitted = true
	summary.TargetQuorumVerified = targetQuorum
	summary.EmptyReadbackVerified = emptyReadback
	summary.TargetAckReadbackVerified = targetReadback
	summary.AckToSeal = ackToSeal
	return nil
}

func exactJSONFields(data []byte, expected ...string) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil || fields == nil {
		return nil, fmt.Errorf("private async-empty evidence object is invalid")
	}
	wanted := make(map[string]bool, len(expected))
	for _, name := range expected {
		wanted[name] = true
	}
	if len(fields) != len(wanted) {
		return nil, fmt.Errorf("private async-empty evidence has an unexpected field set")
	}
	for name := range fields {
		if !wanted[name] {
			return nil, fmt.Errorf("private async-empty evidence contains an unknown field")
		}
	}
	return fields, nil
}

func writeGCSAsyncEmptySummary(public string, summary gcsAsyncEmptyPublicSummary) error {
	if err := os.WriteFile(filepath.Join(public, gcsAsyncEmptySummaryName), encoded(summary), 0600); err != nil {
		return err
	}
	return nil
}

type gcsAckLossPublicSummary struct {
	Qualification         string `json:"qualification"`
	Outcome               string `json:"outcome"`
	Provider              string `json:"provider"`
	NoPVC                 bool   `json:"noPVC"`
	FaultArmed            bool   `json:"faultArmed"`
	FaultInjected         bool   `json:"faultInjected"`
	CanonicalReceipt      bool   `json:"canonicalReceiptVerified"`
	HeldBytes             bool   `json:"heldBytesVerified"`
	FirstRecovery         bool   `json:"firstRecoveryVerified"`
	SecondRecovery        bool   `json:"secondRecoveryVerified"`
	PrivateEvidenceSHA256 string `json:"privateEvidenceSHA256,omitempty"`
	PrivateEventsSHA256   string `json:"privateEventsSHA256,omitempty"`
	PrivateReceiptSHA256  string `json:"privateReceiptSHA256,omitempty"`
}

const (
	gcsAckLossEvidenceName = "gcs-ack-loss-evidence.json"
	gcsAckLossEventsName   = "gcs-ack-loss-events.jsonl"
	gcsAckLossReceiptName  = "gcs-ack-loss-receipt.json"
	gcsAckLossSummaryName  = "gcs-ack-loss-summary.json"
)

// projectGCSAckLossEvidence publishes only a typed summary and hashes. The
// receipt, runtime identity, object key, bucket and prefix remain private.
func projectGCSAckLossEvidence(raw, public string, failedRun bool) (bool, error) {
	entries, err := os.ReadDir(raw)
	if err != nil {
		return false, err
	}
	known := map[string]bool{gcsAckLossEvidenceName: false, gcsAckLossEventsName: false, gcsAckLossReceiptName: false}
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, "gcs-ack-loss-") {
			if _, ok := known[name]; !ok {
				return false, fmt.Errorf("ACK-loss private artifact name is not allowlisted")
			}
			known[name] = true
		}
	}
	if !known[gcsAckLossEvidenceName] && !known[gcsAckLossEventsName] {
		return false, nil
	}
	if !known[gcsAckLossEventsName] {
		return false, fmt.Errorf("ACK-loss private evidence exists without its event record")
	}
	events, err := readBoundedPrivateArtifact(filepath.Join(raw, gcsAckLossEventsName), true)
	if err != nil {
		return false, err
	}
	counts, err := validateAckLossPrivateEvents(events)
	if err != nil {
		return false, err
	}
	eventsHash := sha256.Sum256(events)
	summary := gcsAckLossPublicSummary{
		Qualification:       "gcs-held-create-ack-loss-recovery",
		Outcome:             "failed",
		Provider:            "gcs",
		NoPVC:               true,
		FaultArmed:          counts["ack_loss_fault_armed"] == 1,
		PrivateEventsSHA256: hex.EncodeToString(eventsHash[:]),
	}
	if known[gcsAckLossReceiptName] {
		receipt, err := readBoundedPrivateArtifact(filepath.Join(raw, gcsAckLossReceiptName), false)
		if err != nil {
			return false, err
		}
		receiptHash := sha256.Sum256(receipt)
		summary.PrivateReceiptSHA256 = hex.EncodeToString(receiptHash[:])
	}
	if !known[gcsAckLossEvidenceName] {
		if !failedRun {
			return false, fmt.Errorf("successful ACK-loss qualification is missing private evidence")
		}
		return true, writeGCSAckLossSummary(public, summary)
	}
	evidence, err := readBoundedPrivateArtifact(filepath.Join(raw, gcsAckLossEvidenceName), false)
	if err != nil {
		return false, err
	}
	var private gcsAckLossPrivateEvidence
	decoder := json.NewDecoder(bytes.NewReader(evidence))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&private); err != nil {
		return false, fmt.Errorf("decode private ACK-loss evidence")
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return false, fmt.Errorf("private ACK-loss evidence has trailing data")
	}
	if err := validateGCSAckLossPrivateEvidence(private, counts); err != nil {
		return false, err
	}
	summary.FaultInjected = private.FaultReceipt.Injected
	summary.CanonicalReceipt = true
	summary.HeldBytes = true
	summary.FirstRecovery = true
	summary.SecondRecovery = true
	evidenceHash := sha256.Sum256(evidence)
	summary.PrivateEvidenceSHA256 = hex.EncodeToString(evidenceHash[:])
	if !failedRun {
		if !summary.FaultInjected || !summary.CanonicalReceipt || !summary.HeldBytes || !summary.FirstRecovery || !summary.SecondRecovery {
			return false, fmt.Errorf("successful ACK-loss run lacks a complete private evidence sequence")
		}
		summary.Outcome = "passed"
	}
	return true, writeGCSAckLossSummary(public, summary)
}

func validateAckLossPrivateEvents(data []byte) (map[string]int, error) {
	counts := make(map[string]int)
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 4096), 1<<20)
	for scanner.Scan() {
		var event struct {
			Event string          `json:"event"`
			Time  string          `json:"time"`
			Data  json.RawMessage `json:"data"`
		}
		decoder := json.NewDecoder(bytes.NewReader(scanner.Bytes()))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&event); err != nil || event.Event == "" || event.Time == "" || len(event.Data) == 0 {
			return nil, fmt.Errorf("private ACK-loss event record is invalid")
		}
		if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("private ACK-loss event has trailing data")
		}
		if _, err := time.Parse(time.RFC3339Nano, event.Time); err != nil {
			return nil, fmt.Errorf("private ACK-loss event timestamp is invalid")
		}
		counts[event.Event]++
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read private ACK-loss events: %w", err)
	}
	for _, name := range []string{"ack_loss_fault_armed", "ack_loss_canonical_and_object_verified", "ack_loss_two_recoveries_complete"} {
		if counts[name] > 1 {
			return nil, fmt.Errorf("private ACK-loss event sequence is ambiguous")
		}
	}
	return counts, nil
}

func validateGCSAckLossPrivateEvidence(private gcsAckLossPrivateEvidence, events map[string]int) error {
	receipt, observation := private.FaultReceipt, private.CanonicalObservation
	if private.Version != 1 || !receipt.Injected || receipt.WireRequests != 1 || receipt.OperationID == "" ||
		receipt.Runtime != observation.Runtime || receipt.DispatchID != observation.DispatchID || receipt.Sequence != observation.Sequence ||
		receipt.Intent.OperationID != receipt.OperationID || receipt.Intent.PayloadDigest != receipt.PayloadDigest || receipt.Intent.PayloadSize != receipt.PayloadSize ||
		observation.Guarded.DispatchID != receipt.DispatchID || observation.Guarded.ObjectGeneration != private.Object.Generation ||
		observation.Guarded.ObjectMetageneration != private.Object.Metageneration || observation.Guarded.PayloadDigest != private.Object.PayloadSHA256 ||
		observation.Guarded.ObjectSize != private.Object.Size || !private.Object.TemporaryHold ||
		private.BaselineAck.RequestID == "" || private.BaselineAck.Slot == 0 || private.BaselineAck.Rows != `[[1,"gcs-ack-loss-baseline"]]` ||
		private.SecondAck.RequestID == "" || private.SecondAck.Slot == 0 || private.SecondAck.Rows != `[[2,"gcs-ack-loss-successor"]]` {
		return fmt.Errorf("private ACK-loss evidence does not bind the exact fault, object, and acknowledged rows")
	}
	if events["ack_loss_fault_armed"] != 1 || events["ack_loss_canonical_and_object_verified"] != 1 || events["ack_loss_two_recoveries_complete"] != 1 {
		return fmt.Errorf("private ACK-loss event sequence is incomplete")
	}
	var first, second struct {
		Source   gcsRecoverySource     `json:"source"`
		Fence    operator.FenceRequest `json:"fence"`
		Proof    operator.FenceProof   `json:"proof"`
		Recovery operator.Resource     `json:"recovery"`
	}
	decodeRecovery := func(data obj, target any) error {
		encoded, err := json.Marshal(data)
		if err != nil {
			return err
		}
		decoder := json.NewDecoder(bytes.NewReader(encoded))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(target); err != nil {
			return err
		}
		return nil
	}
	if err := decodeRecovery(private.FirstRecovery, &first); err != nil {
		return fmt.Errorf("decode private first recovery evidence")
	}
	if err := decodeRecovery(private.SecondRecovery, &second); err != nil {
		return fmt.Errorf("decode private second recovery evidence")
	}
	if err := validateGCSHAAckReceipt(first.Source, receipt); err != nil {
		return fmt.Errorf("private ACK-loss receipt does not match the first captured source: %w", err)
	}
	guarded := observation.Guarded
	guardDigest, digestErr := hex.DecodeString(guarded.GuardDigest)
	if guarded.Version != fencing.MutationGuardedLogicalResolutionVersion || guarded.Runtime != receipt.Runtime ||
		guarded.WALIdentity != receipt.SourceIdentity.WALIdentity || guarded.ProducerUID != receipt.ProducerUID || guarded.Sequence != receipt.Sequence ||
		guarded.Intent != receipt.Intent || guarded.Preparation != receipt.Preparation || guarded.DispatchID != receipt.DispatchID ||
		guarded.Profile != fencing.MutationHeldExecutionProfile || guarded.GuardRootIdentity != gcsHAAuthorityRootIdentity(receipt.Runtime.StatefulSetUID, receipt.Runtime.SourceClusterID) ||
		guarded.GuardVersion != fencing.MutationGuardedLogicalResolutionVersion || guarded.GuardRef == "" || guarded.PreparationHash == "" ||
		digestErr != nil || len(guardDigest) != sha256.Size || observation.GuardSHA256 != guarded.GuardDigest ||
		guarded.ObjectGeneration != private.Object.Generation || guarded.ObjectMetageneration != private.Object.Metageneration ||
		guarded.ObjectSize != receipt.PayloadSize || guarded.ObjectSize != private.Object.Size || guarded.PayloadDigest != receipt.PayloadDigest ||
		guarded.PayloadDigest != private.Object.PayloadSHA256 || !private.Object.TemporaryHold {
		return fmt.Errorf("private ACK-loss guard does not match the exact receipt, source root, and held object tuple")
	}
	for _, item := range []struct {
		source   gcsRecoverySource
		fence    operator.FenceRequest
		proof    operator.FenceProof
		recovery operator.Resource
	}{{first.Source, first.Fence, first.Proof, first.Recovery}, {second.Source, second.Fence, second.Proof, second.Recovery}} {
		if item.source.ClusterID == "" || item.fence.OperationID == "" || item.proof.OperationID != item.fence.OperationID ||
			item.proof.SourceClusterID != item.fence.SourceClusterID || item.recovery.Status.Phase != "Complete" ||
			item.recovery.Status.Stage != "Complete" || item.recovery.Spec.RecoveryID != item.fence.OperationID ||
			item.recovery.Spec.SourceClusterID != item.fence.SourceClusterID || recoveryFenceMatchesProof(item.recovery, item.fence, item.proof) != nil {
			return fmt.Errorf("private ACK-loss recovery evidence is not a complete fence-linked recovery")
		}
	}
	return verifyRecoveryLineage(first.Recovery, second.Recovery, first.Fence, second.Fence)
}

func writeGCSAckLossSummary(public string, summary gcsAckLossPublicSummary) error {
	return os.WriteFile(filepath.Join(public, gcsAckLossSummaryName), encoded(summary), 0600)
}

// Only explicit evidence types enter the public packet; raw diagnostics stay on the disposable runner.
func safeArtifacts(raw string) error {
	if raw == "" {
		return fmt.Errorf("FENCING_E2E_OUT is required")
	}
	exitData, err := os.ReadFile(filepath.Join(raw, "actual-exit.txt"))
	if err != nil {
		return err
	}
	failedRun := strings.TrimSpace(string(exitData)) != "0"
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
		approved := strings.HasSuffix(name, "-image-evidence.json") || name == "validated-voter-proof.json" || name == "validated-generation-proof.json" || name == "online-active-ledger.json" || name == "learner-authenticated-binding.json" || name == "online-completed-membership.json" || name == "runtime-profile.json" || name == "runtime-component-probe.json" || name == "fencer-diagnostics.json" || name == "fencer-pods.json" || name == "fencer-events.json" || name == "fencer-deployment.json" || name == "terminal.json"
		textFile := name == "source-head.txt" || name == "image-ids.txt" || name == "actual-exit.txt"
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
		// A failed run may stop before a redirected diagnostic JSON file gets
		// any bytes. Preserve the actual exit receipt and the usable diagnostics;
		// do not turn an early qualification failure into an artifact parse error.
		if failedRun && approved && len(data) == 0 {
			continue
		}
		switch {
		case approved:
			if name == "terminal.json" {
				name = "resources-summary.json"
			}
			if name == "fencer-diagnostics.json" {
				data, err = projectRuntimeAgentDiagnostics(raw, data)
				if err != nil {
					return err
				}
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
				if name == "actual-exit.txt" {
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
	asyncEmptySeen, err := projectGCSAsyncEmptyEvidence(raw, public, failedRun)
	if err != nil {
		return err
	}
	ackLossSeen, err := projectGCSAckLossEvidence(raw, public, failedRun)
	if err != nil {
		return err
	}
	required := []string{"actual-exit.txt", "source-head.txt", "image-ids.txt"}
	if !failedRun {
		required = append(required, "runtime-profile.json", "runtime-component-probe.json")
		if asyncEmptySeen {
			required = append(required, gcsAsyncEmptySummaryName)
		}
		if ackLossSeen {
			required = append(required, gcsAckLossSummaryName)
		}
	} else {
		for _, name := range []string{"fencer-diagnostics.json", "fencer-pods.json", "fencer-events.json", "fencer-deployment.json"} {
			if _, statErr := os.Stat(filepath.Join(public, name)); statErr == nil {
				required = append(required, name)
			}
		}
	}
	for _, name := range required {
		if _, err := os.Stat(filepath.Join(public, name)); err != nil {
			return fmt.Errorf("required public receipt missing: %s", name)
		}
	}
	return nil
}
