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
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/fencing/runtimeagent"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

func publicRuntime(runtime obj) obj {
	return obj{"status": obj{"id": nested(runtime, "status", "id"), "imageRef": nested(runtime, "status", "imageRef"), "labels": obj{"io.kubernetes.pod.uid": nested(runtime, "status", "labels", "io.kubernetes.pod.uid")}}}
}

func publicCRIImage(cri obj) obj {
	return obj{"status": obj{"id": nested(cri, "status", "id"), "repoDigests": nested(cri, "status", "repoDigests")}}
}

func publicOnlineVoterDiagnostics(data []byte) ([]byte, error) {
	var source obj
	if err := json.Unmarshal(data, &source); err != nil {
		return nil, err
	}
	view := onlineVoterDiagnosticsView{
		RecoveryFound:                  source["recoveryFound"] == true,
		PodsQuerySucceeded:             source["podsQuerySucceeded"] == true,
		EventsQuerySucceeded:           source["eventsQuerySucceeded"] == true,
		RecoveryQuerySucceeded:         source["recoveryQuerySucceeded"] == true,
		ParentQuerySucceeded:           source["parentQuerySucceeded"] == true,
		ParentRecoveryMatchesOperation: source["parentRecoveryMatchesOperation"] == true,
		Conditions:                     []onlineLearnerConditionView{},
		Containers:                     []onlineLearnerContainerView{}, Events: []onlineLearnerEventView{}, Secrets: []onlineLearnerSecretView{},
	}
	if source["recoveryFound"] != true && source["recoveryFound"] != false {
		return nil, fmt.Errorf("online voter recovery observation is invalid")
	}
	if source["parentQuerySucceeded"] != true && source["parentQuerySucceeded"] != false {
		return nil, fmt.Errorf("online voter parent observation is invalid")
	}
	for _, key := range []string{"podsQuerySucceeded", "eventsQuerySucceeded", "recoveryQuerySucceeded"} {
		if source[key] != true && source[key] != false {
			return nil, fmt.Errorf("online voter %s observation is invalid", key)
		}
	}
	view.PodsQuerySucceeded = source["podsQuerySucceeded"] == true
	view.EventsQuerySucceeded = source["eventsQuerySucceeded"] == true
	view.RecoveryQuerySucceeded = source["recoveryQuerySucceeded"] == true
	recovery := source["recovery"]
	if view.RecoveryFound {
		if recovery == nil {
			return nil, fmt.Errorf("online voter recovery observation is incomplete")
		}
		view.RecoveryPhase = safeKubePhase(text(nested(recovery, "status", "phase")))
		view.RecoveryStage = safeKubePhase(text(nested(recovery, "status", "stage")))
		view.MembershipPhase = safeKubePhase(text(nested(recovery, "status", "membership", "phase")))
	} else if recovery != nil {
		return nil, fmt.Errorf("online voter recovery observation contradicts its presence flag")
	}
	parent := source["parent"]
	if view.ParentQuerySucceeded && parent == nil {
		return nil, fmt.Errorf("online voter parent observation is incomplete")
	}
	if view.ParentQuerySucceeded {
		view.ParentPhase = safeKubePhase(text(nested(parent, "status", "phase")))
	}
	membership := source["membershipObservation"]
	if membership != nil {
		for _, key := range []string{"checked", "available", "authRejected", "nodeMatchesAdd", "walMatchesAdd", "clusterMatchesActive", "voting", "pending"} {
			if nested(membership, key) != true && nested(membership, key) != false {
				return nil, fmt.Errorf("online voter membership observation is invalid")
			}
		}
		view.MembershipChecked = nested(membership, "checked") == true
		view.MembershipAvailable = nested(membership, "available") == true
		view.MembershipAuthRejected = nested(membership, "authRejected") == true
		view.MembershipNodeMatchesAdd = nested(membership, "nodeMatchesAdd") == true
		view.MembershipWALMatchesAdd = nested(membership, "walMatchesAdd") == true
		view.MembershipClusterMatchesActive = nested(membership, "clusterMatchesActive") == true
		view.MembershipVoting = nested(membership, "voting") == true
		view.MembershipPending = nested(membership, "pending") == true
	}
	secretObservations, _ := source["secretObservations"].([]any)
	if source["secretObservationsTruncated"] != true && source["secretObservationsTruncated"] != false {
		return nil, fmt.Errorf("online voter secret observation completeness is invalid")
	}
	view.SecretObservationsTruncated = source["secretObservationsTruncated"] == true
	view.SecretReferences = len(secretObservations)
	view.SecretKeysPresent = len(secretObservations) > 0
	for _, secret := range secretObservations {
		if nested(secret, "querySucceeded") != true {
			view.SecretQueryFailures++
		}
		if nested(secret, "found") == true {
			view.SecretsFound++
			view.Secrets = append(view.Secrets, onlineLearnerSecretView{
				UID: safeKubernetesUID(text(nested(secret, "uid"))), Found: true,
				QuerySucceeded: nested(secret, "querySucceeded") == true,
				Immutable:      nested(secret, "immutable") == true, KeysPresent: nested(secret, "keysPresent") == true,
			})
		} else {
			view.SecretKeysPresent = false
			view.Secrets = append(view.Secrets, onlineLearnerSecretView{Found: false, QuerySucceeded: nested(secret, "querySucceeded") == true})
			continue
		}
		if nested(secret, "immutable") == true && nested(secret, "ownerMatchesOperation") == true {
			view.ImmutableSecrets++
		} else {
			view.SecretKeysPresent = false
		}
		if nested(secret, "keysPresent") != true {
			view.SecretKeysPresent = false
		}
	}
	learner := source["learner"]
	view.LearnerFound = learner != nil
	if learner != nil {
		view.LearnerMatchesFaultTargetUID = text(nested(learner, "metadata", "uid")) != "" && nested(learner, "metadata", "uid") == source["faultTargetPodUID"]
		view.LearnerMatchesFaultTargetIP = text(nested(learner, "status", "podIP")) != "" && nested(learner, "status", "podIP") == source["faultTargetIP"]
		view.LearnerPhase = safePodPhase(text(nested(learner, "status", "phase")))
		conditions, _ := nested(learner, "status", "conditions").([]any)
		if len(conditions) > 12 {
			conditions = conditions[:12]
		}
		for _, condition := range conditions {
			typ := text(nested(condition, "type"))
			if typ != "Ready" && typ != "ContainersReady" && typ != "Initialized" && typ != "PodScheduled" {
				continue
			}
			status := text(nested(condition, "status"))
			if status != "True" && status != "False" && status != "Unknown" {
				status = "Other"
			}
			view.Conditions = append(view.Conditions, onlineLearnerConditionView{Type: typ, Status: status, Reason: safeContainerReason(text(nested(condition, "reason")))})
		}
		statuses, _ := nested(learner, "status", "containerStatuses").([]any)
		if len(statuses) > 8 {
			statuses = statuses[:8]
		}
		for _, status := range statuses {
			view.Containers = append(view.Containers, onlineLearnerContainerView{
				Ready:                nested(status, "ready") == true,
				RestartCount:         boundedRestartCount(nested(status, "restartCount")),
				WaitingReason:        safeContainerReason(text(nested(status, "state", "waiting", "reason"))),
				TerminatedReason:     safeContainerReason(text(nested(status, "state", "terminated", "reason"))),
				ExitCode:             safeExitCode(nested(status, "state", "terminated", "exitCode")),
				LastTerminatedReason: safeContainerReason(text(nested(status, "lastState", "terminated", "reason"))),
				LastExitCode:         safeExitCode(nested(status, "lastState", "terminated", "exitCode")),
			})
		}
	}
	events, _ := source["events"].([]any)
	for _, event := range events {
		if text(nested(event, "type")) != "Warning" {
			continue
		}
		reason := safeEventReason(text(nested(event, "reason")))
		if reason == "" {
			continue
		}
		view.Events = append(view.Events, onlineLearnerEventView{
			Reason:  reason,
			Message: classifyLearnerEventMessage(reason, text(nested(event, "message"))),
			Count:   boundedRestartCount(nested(event, "count")),
		})
	}
	if len(view.Events) > 30 {
		view.Events = view.Events[len(view.Events)-30:]
	}
	return json.Marshal(view)
}

func projectOnlineVoterDiagnostics(raw string, data []byte) ([]byte, error) {
	projected, err := publicOnlineVoterDiagnostics(data)
	if err != nil {
		return nil, err
	}
	var view onlineVoterDiagnosticsView
	if err := json.Unmarshal(projected, &view); err != nil {
		return nil, err
	}
	dir := filepath.Join(raw, "private-online-voter-diagnostics")
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return projected, nil
	}
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		name := entry.Name()
		period := ""
		switch {
		case strings.HasPrefix(name, "learner-current-") && strings.HasSuffix(name, ".log"):
			period = "current"
		case strings.HasPrefix(name, "learner-previous-") && strings.HasSuffix(name, ".log"):
			period = "previous"
		default:
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() || entry.Type()&os.ModeSymlink != 0 || info.Size() > 32<<10 {
			continue
		}
		file, err := os.Open(filepath.Join(dir, name))
		if err != nil {
			continue
		}
		contents, readErr := io.ReadAll(io.LimitReader(file, (32<<10)+1))
		closeErr := file.Close()
		if readErr != nil || closeErr != nil || len(contents) > 32<<10 {
			continue
		}
		digest := sha256.Sum256(contents)
		failures := classifyVoterStartupLog(contents)
		stages, stagesTruncated := classifyVoterStartupStages(contents)
		lower := strings.ToLower(string(contents))
		if strings.Contains(lower, "fatal") && !hasString(failures, "fatal_error") {
			failures = append(failures, "fatal_error")
		}
		if strings.Contains(lower, "panic:") && !hasString(failures, "panic") {
			failures = append(failures, "panic")
		}
		startupFailure := view.LearnerPhase != "Running"
		for _, container := range view.Containers {
			startupFailure = startupFailure || !container.Ready || container.WaitingReason != "" || container.TerminatedReason != ""
		}
		if startupFailure && len(failures) == 0 && len(bytes.TrimSpace(contents)) > 0 {
			failures = []string{"unknown_startup_error"}
		}
		if startupFailure && len(stages) == 0 && len(bytes.TrimSpace(contents)) > 0 {
			stages = []string{"node_open/outer_wrapper_unparsed"}
		}
		view.Logs = append(view.Logs, onlineLearnerLogView{
			Period: period, Captured: true, Bytes: len(contents), SHA256: hex.EncodeToString(digest[:]),
			Failures: failures, Stages: stages, Truncated: stagesTruncated,
		})
	}
	return json.Marshal(view)
}

func hasString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func safeKubePhase(value string) string {
	switch value {
	case "Pending", "Running", "Succeeded", "Failed", "Blocked", "Healthy", "Degraded", "Complete", "Completed", "Fencing", "Recovering", "Stabilizing", "Creating", "Adding", "Removing", "Aborting", "Aborted":
		return value
	default:
		if value == "" {
			return ""
		}
		return "Other"
	}
}

func safePodPhase(value string) string {
	switch value {
	case "Pending", "Running", "Succeeded", "Failed":
		return value
	default:
		if value == "" {
			return ""
		}
		return "Other"
	}
}

func safeKubernetesUID(value string) string {
	if len(value) != 36 {
		return ""
	}
	for i, c := range value {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return ""
			}
			continue
		}
		if !strings.ContainsRune("0123456789abcdef", c) {
			return ""
		}
	}
	return value
}

func safeContainerReason(value string) string {
	switch value {
	case "CrashLoopBackOff", "ImagePullBackOff", "ErrImagePull", "CreateContainerConfigError", "CreateContainerError", "ContainerCreating", "RunContainerError", "OOMKilled", "Error", "Completed", "ContainerCannotRun", "DeadlineExceeded", "StartError":
		return value
	default:
		if value == "" {
			return ""
		}
		return "Other"
	}
}

func safeEventReason(value string) string {
	switch value {
	case "BackOff", "Unhealthy", "Failed", "FailedMount", "FailedCreate", "FailedScheduling", "NetworkNotReady", "Killing", "Pulled", "Started":
		return value
	default:
		return ""
	}
}

func classifyLearnerEventMessage(reason, message string) string {
	message = strings.ToLower(message)
	switch {
	case reason == "BackOff" && strings.Contains(message, "back-off restarting failed container"):
		return "container restart backoff"
	case reason == "Unhealthy" && strings.Contains(message, "connection refused"):
		return "probe connection refused"
	case reason == "Unhealthy" && strings.Contains(message, "http probe failed with statuscode:"):
		if code := eventHTTPStatusCode(message); code != "" {
			return "HTTP probe failed, status " + code
		}
		return "readiness probe failed"
	case reason == "Unhealthy" && strings.Contains(message, "context deadline exceeded"):
		return "probe deadline exceeded"
	case reason == "Unhealthy" && strings.Contains(message, "readiness probe failed"):
		return "readiness probe failed"
	case reason == "Unhealthy" && strings.Contains(message, "liveness probe failed"):
		return "liveness probe failed"
	case reason == "Unhealthy" && strings.Contains(message, "startup probe failed"):
		return "startup probe failed"
	case reason == "Failed" && strings.Contains(message, "failed to pull image"):
		return "image pull failed"
	case reason == "Failed" && strings.Contains(message, "failed to create pod sandbox"):
		return "pod sandbox creation failed"
	default:
		return ""
	}
}

func eventHTTPStatusCode(message string) string {
	return safeHTTPStatusCode(message, "http probe failed with statuscode:")
}

func safeHTTPStatusCode(message, marker string) string {
	index := strings.Index(strings.ToLower(message), strings.ToLower(marker))
	if index < 0 {
		return ""
	}
	value := strings.TrimSpace(message[index+len(marker):])
	if len(value) > 3 {
		value = value[:3]
	}
	code, err := strconv.Atoi(value)
	if err != nil || code < 100 || code > 599 {
		return ""
	}
	return strconv.Itoa(code)
}

func boundedRestartCount(value any) int32 {
	count, ok := value.(float64)
	if !ok || count < 0 || count > 1_000_000 {
		return 0
	}
	return int32(count)
}

func safeExitCode(value any) *int32 {
	code, ok := value.(float64)
	if !ok || code < -255 || code > 255 {
		return nil
	}
	result := int32(code)
	return &result
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
		Captured            bool                            `json:"captured"`
		SHA256              string                          `json:"sha256"`
		Bytes               int64                           `json:"bytes"`
		Lines               int64                           `json:"lines"`
		Rejections          []supervisorRejectionDiagnostic `json:"rejections,omitempty"`
		RejectionsTruncated bool                            `json:"rejectionsTruncated,omitempty"`
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

type supervisorRejectionDiagnostic struct {
	Stage  string `json:"stage"`
	Action string `json:"action"`
	Code   string `json:"code"`
}

const maxRuntimeAgentDiagnostics = 20
const maxSupervisorRejectionDiagnostics = 20

var allowedRuntimeAgentDiagnosticPairs = map[string]struct{}{
	"authorization/token_missing":                              {},
	"authorization/token_review_failed":                        {},
	"authorization/principal_mismatch":                         {},
	"authorization/audience_mismatch":                          {},
	"client/config_invalid":                                    {},
	"client/node_lookup_failed":                                {},
	"client/node_address_unavailable":                          {},
	"client/trust_invalid":                                     {},
	"client/token_unavailable":                                 {},
	"client/token_empty":                                       {},
	"client/request_invalid":                                   {},
	"transport/unavailable":                                    {},
	"transport/context_canceled":                               {},
	"transport/deadline_exceeded":                              {},
	"client/context_canceled":                                  {},
	"client/deadline_exceeded":                                 {},
	"response/http_rejected":                                   {},
	"response/body_unavailable":                                {},
	"response/body_oversized":                                  {},
	"response/context_canceled":                                {},
	"response/deadline_exceeded":                               {},
	"response/decode_invalid":                                  {},
	"response/trailing_data":                                   {},
	"binding/invalid":                                          {},
	"binding/mismatch":                                         {},
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

// These are fixed source errors emitted by the register transition path. The
// raw error remains private; only an exact known prefix is projected.
var supervisorRejectionPrefixes = []struct {
	message string
	code    string
}{
	{"fencing: standalone learner Pod incarnation changed", "standalone_learner_pod_incarnation_changed"},
	{"fencing: standalone learner workload binding differs", "standalone_learner_workload_binding_mismatch"},
	{"fencing: standalone learner incarnation is not current in canonical history", "standalone_learner_history_not_current"},
	{"fencing: standalone learner incarnation is being replaced", "standalone_learner_being_replaced"},
	{"fencing: static startup Pod incarnation changed", "static_startup_pod_incarnation_changed"},
	{"fencing: static startup root binding", "static_startup_root_binding_rejected"},
	{"fencing: static startup StatefulSet", "static_startup_statefulset_binding_rejected"},
	{"fencing: static startup template binding", "static_startup_template_binding_rejected"},
	{"fencing: static startup template environment", "static_startup_environment_rejected"},
	{"fencing: static startup Pod environment", "static_startup_environment_rejected"},
	{"fencing: static startup Pod differs", "static_startup_environment_rejected"},
	{"fencing: static startup source cluster", "static_startup_source_mismatch"},
	{"fencing: static startup peer token", "static_startup_credential_rejected"},
	{"fencing: static startup token ConfigMap", "static_startup_credential_rejected"},
	{"fencing: static startup membership", "static_startup_membership_rejected"},
	{"fencing: static startup Pod container", "static_startup_container_rejected"},
	{"fencing: startup physical identity is incomplete", "startup_physical_identity_incomplete"},
	{"fencing: startup Pod incarnation changed", "startup_pod_incarnation_changed"},
	{"fencing: startup root binding is ambiguous", "startup_root_ambiguous"},
	{"fencing: startup root binding is unavailable", "startup_root_unavailable"},
	{"fencing: startup StatefulSet ownership is ambiguous", "startup_statefulset_owner_ambiguous"},
	{"fencing: startup StatefulSet owner differs", "startup_statefulset_owner_mismatch"},
	{"fencing: startup workload binding differs", "startup_workload_binding_mismatch"},
	{"fencing: workload is not enrolled", "workload_not_enrolled"},
	{"fencing: dedicated private PID namespace required", "workload_pid_namespace_invalid"},
	{"fencing: untracked lifecycle processes are unsupported", "workload_lifecycle_unsupported"},
	{"fencing: exec probes may create untracked processes", "workload_exec_probe_unsupported"},
	{"fencing: procfs must remain kernel-provided", "workload_procfs_invalid"},
	{"fencing: supervised launch required", "workload_supervised_launch_missing"},
	{"fencing: default procfs required", "workload_procfs_profile_invalid"},
	{"fencing: unprivileged supervisor required", "workload_privilege_profile_invalid"},
	{"fencing: capabilities must be dropped", "workload_capability_profile_invalid"},
	{"fencing: host mounts unsupported", "workload_host_mount_unsupported"},
	{"fencing: canonical startup control unavailable:", "startup_control_unavailable"},
	{"fencing: canonical startup binding differs", "startup_control_binding_mismatch"},
	{"fencing: completed replacement request is ambiguous", "replacement_request_ambiguous"},
	{"fencing: same-ordinal closed identity is absent from canonical control", "replacement_closed_identity_missing"},
	{"fencing: same-ordinal assignment no longer matches its closed identity", "replacement_closed_identity_mismatch"},
	{"fencing: startup logical identity differs from assignment", "replacement_logical_identity_mismatch"},
	{"fencing: startup removal proof differs from canonical cohort", "replacement_removal_proof_mismatch"},
	{"fencing: startup assignment digest differs", "replacement_assignment_digest_mismatch"},
	{"fencing: startup StatefulSet identity changed", "replacement_statefulset_identity_changed"},
	{"fencing: startup recovery child differs from assignment", "replacement_recovery_child_mismatch"},
	{"fencing: completed startup assignment is not canonical:", "replacement_completion_not_canonical"},
	{"fencing: completed same-ordinal assignment receipt differs", "replacement_completion_receipt_mismatch"},
	{"fencing: startup learner credential differs from assignment", "replacement_credential_mismatch"},
	{"fencing: startup learner credential payload differs", "replacement_credential_payload_mismatch"},
	{"fencing: startup member credential encoding is invalid", "replacement_member_encoding_invalid"},
	{"fencing: startup member credential differs", "replacement_member_mismatch"},
	{"fencing: startup peer credential differs", "replacement_peer_credential_mismatch"},
	{"fencing: startup member endpoint differs from the assigned Pod", "replacement_member_endpoint_mismatch"},
	{"fencing: replacement identity already appears in template membership", "replacement_identity_in_template"},
	{"fencing: committed replacement cohort differs from source template", "replacement_cohort_mismatch"},
	{"fencing: authority root does not require mutation journal", "registration_journal_policy_mismatch"},
	{"fencing: authority root requires mutation journal without runtime binding", "registration_runtime_binding_policy_mismatch"},
	{"fencing: runtime agent is required for mutation-journal registration", "registration_runtime_agent_required"},
	{"fencing: runtime agent is required", "registration_runtime_agent_required"},
	{"fencing: required archive genesis registration lacks runtime binding", "registration_archive_runtime_binding_missing"},
}

var registerStageRejectionPrefixes = []struct {
	message string
	code    string
}{
	{"fencing: register root-init rejected:", "register_root_init_rejected"},
	{"fencing: register startup-grant rejected:", "register_startup_grant_rejected"},
	{"fencing: register ledger-register rejected:", "register_ledger_register_rejected"},
	{"fencing: register producer-reserve rejected:", "register_producer_reserve_rejected"},
	{"fencing: register archive-scope rejected:", "register_archive_scope_rejected"},
}

var authenticationErrorPrefixes = []struct {
	message string
	code    string
}{
	{"fencing: bound token required", "authentication_token_missing"},
	{"fencing: unauthenticated token", "authentication_token_unauthenticated"},
	{"fencing: token audience differs", "authentication_audience_mismatch"},
	{"fencing: token Pod UID differs", "authentication_pod_uid_mismatch"},
	{"fencing: authenticated physical Pod name differs", "authentication_pod_name_mismatch"},
	{"fencing: Pod incarnation differs", "authentication_pod_incarnation_mismatch"},
	{"fencing: token namespace differs", "authentication_namespace_mismatch"},
	{"fencing: StatefulSet controller required", "authentication_statefulset_controller_missing"},
	{"fencing: workload UID differs", "authentication_workload_uid_mismatch"},
	{"fencing: learner binding missing", "authentication_learner_binding_invalid"},
	{"fencing: learner root binding differs", "authentication_learner_binding_invalid"},
	{"fencing: learner credentials changed", "authentication_learner_binding_invalid"},
	{"fencing: learner member differs", "authentication_learner_binding_invalid"},
	{"fencing: Pod source differs", "authentication_source_mismatch"},
	{"fencing: workload is not enrolled", "workload_not_enrolled"},
	{"fencing: dedicated private PID namespace required", "workload_pid_namespace_invalid"},
	{"fencing: untracked lifecycle processes are unsupported", "workload_lifecycle_unsupported"},
	{"fencing: exec probes may create untracked processes", "workload_exec_probe_unsupported"},
	{"fencing: procfs must remain kernel-provided", "workload_procfs_invalid"},
	{"fencing: supervised launch required", "workload_supervised_launch_missing"},
	{"fencing: default procfs required", "workload_procfs_profile_invalid"},
	{"fencing: unprivileged supervisor required", "workload_privilege_profile_invalid"},
	{"fencing: capabilities must be dropped", "workload_capability_profile_invalid"},
	{"fencing: host mounts unsupported", "workload_host_mount_unsupported"},
}

var runtimeEnrollmentErrorPrefixes = []struct {
	message string
	code    string
}{
	{"fencing: runtime enrollment authority unavailable", "runtime_enrollment_authority_unavailable"},
	{"fencing: runtime enrollment Pod incarnation or root binding differs", "runtime_enrollment_pod_binding_mismatch"},
	{"fencing: one live CRI container is required", "runtime_enrollment_container_unavailable"},
	{"fencing: Kubernetes container/image identity is incomplete", "runtime_enrollment_container_image_identity_incomplete"},
	{"fencing: namespace UID is unavailable", "runtime_enrollment_namespace_uid_unavailable"},
	{"fencing: workload is not enrolled", "workload_not_enrolled"},
	{"fencing: dedicated private PID namespace required", "workload_pid_namespace_invalid"},
	{"fencing: untracked lifecycle processes are unsupported", "workload_lifecycle_unsupported"},
	{"fencing: exec probes may create untracked processes", "workload_exec_probe_unsupported"},
	{"fencing: procfs must remain kernel-provided", "workload_procfs_invalid"},
	{"fencing: supervised launch required", "workload_supervised_launch_missing"},
	{"fencing: default procfs required", "workload_procfs_profile_invalid"},
	{"fencing: unprivileged supervisor required", "workload_privilege_profile_invalid"},
	{"fencing: capabilities must be dropped", "workload_capability_profile_invalid"},
	{"fencing: host mounts unsupported", "workload_host_mount_unsupported"},
}

var allowedSupervisorRejectionCodes = func() map[string]struct{} {
	codes := make(map[string]struct{}, len(supervisorRejectionPrefixes)+len(registerStageRejectionPrefixes)+len(allowedRuntimeAgentDiagnosticPairs)+16)
	for _, item := range supervisorRejectionPrefixes {
		codes[item.code] = struct{}{}
	}
	for _, item := range registerStageRejectionPrefixes {
		codes[item.code] = struct{}{}
	}
	for pair := range allowedRuntimeAgentDiagnosticPairs {
		codes["runtimeagent_"+strings.ReplaceAll(pair, "/", "_")] = struct{}{}
	}
	for _, code := range []string{
		"authentication_guard_unknown", "authentication_token_missing", "authentication_token_unauthenticated",
		"authentication_audience_mismatch", "authentication_pod_uid_mismatch", "authentication_pod_name_mismatch",
		"authentication_pod_incarnation_mismatch", "authentication_namespace_mismatch", "authentication_statefulset_controller_missing",
		"authentication_workload_uid_mismatch", "authentication_learner_binding_invalid", "authentication_source_mismatch",
		"runtime_enrollment_guard_unknown", "runtime_enrollment_authority_unavailable", "runtime_enrollment_pod_binding_mismatch",
		"runtime_enrollment_container_unavailable", "runtime_enrollment_container_image_identity_incomplete",
		"runtime_enrollment_namespace_uid_unavailable", "workload_not_enrolled", "workload_pid_namespace_invalid",
		"workload_lifecycle_unsupported", "workload_exec_probe_unsupported", "workload_procfs_invalid",
		"workload_supervised_launch_missing", "workload_procfs_profile_invalid", "workload_privilege_profile_invalid",
		"workload_capability_profile_invalid", "workload_host_mount_unsupported",
	} {
		codes[code] = struct{}{}
	}
	codes["register_guard_unknown"] = struct{}{}
	return codes
}()

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

// onlineVoterDiagnosticsView is an allowlist projection of the private
// operation-bound learner snapshot. It intentionally excludes names, UIDs,
// addresses, resource specs, event messages, and raw container logs.
type onlineVoterDiagnosticsView struct {
	RecoveryFound                  bool                         `json:"recoveryFound"`
	PodsQuerySucceeded             bool                         `json:"podsQuerySucceeded"`
	EventsQuerySucceeded           bool                         `json:"eventsQuerySucceeded"`
	RecoveryQuerySucceeded         bool                         `json:"recoveryQuerySucceeded"`
	RecoveryPhase                  string                       `json:"recoveryPhase,omitempty"`
	RecoveryStage                  string                       `json:"recoveryStage,omitempty"`
	MembershipPhase                string                       `json:"membershipPhase,omitempty"`
	ParentQuerySucceeded           bool                         `json:"parentQuerySucceeded"`
	ParentPhase                    string                       `json:"parentPhase,omitempty"`
	ParentRecoveryMatchesOperation bool                         `json:"parentRecoveryMatchesOperation"`
	LearnerMatchesFaultTargetUID   bool                         `json:"learnerMatchesFaultTargetUID"`
	LearnerMatchesFaultTargetIP    bool                         `json:"learnerMatchesFaultTargetIP"`
	LearnerFound                   bool                         `json:"learnerFound"`
	LearnerPhase                   string                       `json:"learnerPhase,omitempty"`
	SecretReferences               int                          `json:"secretReferences"`
	SecretObservationsTruncated    bool                         `json:"secretObservationsTruncated"`
	SecretQueryFailures            int                          `json:"secretQueryFailures"`
	SecretsFound                   int                          `json:"secretsFound"`
	ImmutableSecrets               int                          `json:"immutableSecrets"`
	SecretKeysPresent              bool                         `json:"secretKeysPresent"`
	Secrets                        []onlineLearnerSecretView    `json:"secrets"`
	MembershipChecked              bool                         `json:"membershipChecked"`
	MembershipAvailable            bool                         `json:"membershipAvailable"`
	MembershipAuthRejected         bool                         `json:"membershipAuthRejected"`
	MembershipNodeMatchesAdd       bool                         `json:"membershipNodeMatchesAdd"`
	MembershipWALMatchesAdd        bool                         `json:"membershipWALMatchesAdd"`
	MembershipClusterMatchesActive bool                         `json:"membershipClusterMatchesActive"`
	MembershipVoting               bool                         `json:"membershipVoting"`
	MembershipPending              bool                         `json:"membershipPending"`
	Conditions                     []onlineLearnerConditionView `json:"conditions"`
	Containers                     []onlineLearnerContainerView `json:"containers"`
	Events                         []onlineLearnerEventView     `json:"events"`
	Logs                           []onlineLearnerLogView       `json:"logs"`
}

type onlineLearnerSecretView struct {
	UID            string `json:"uid,omitempty"`
	Found          bool   `json:"found"`
	QuerySucceeded bool   `json:"querySucceeded"`
	Immutable      bool   `json:"immutable"`
	KeysPresent    bool   `json:"keysPresent"`
}

type onlineLearnerConditionView struct {
	Type   string `json:"type"`
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
}

type onlineLearnerContainerView struct {
	Ready                bool   `json:"ready"`
	RestartCount         int32  `json:"restartCount"`
	WaitingReason        string `json:"waitingReason,omitempty"`
	TerminatedReason     string `json:"terminatedReason,omitempty"`
	ExitCode             *int32 `json:"exitCode,omitempty"`
	LastTerminatedReason string `json:"lastTerminatedReason,omitempty"`
	LastExitCode         *int32 `json:"lastExitCode,omitempty"`
}

type onlineLearnerEventView struct {
	Reason  string `json:"reason"`
	Message string `json:"message,omitempty"`
	Count   int32  `json:"count,omitempty"`
}

type onlineLearnerLogView struct {
	Period    string   `json:"period"`
	Captured  bool     `json:"captured"`
	Bytes     int      `json:"bytes"`
	SHA256    string   `json:"sha256,omitempty"`
	Failures  []string `json:"failures,omitempty"`
	Stages    []string `json:"stages,omitempty"`
	Truncated bool     `json:"truncated,omitempty"`
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

type voterStartupPrivateView struct {
	StatefulSetIdentity string `json:"statefulSetIdentity"`
	StatefulSetUID      string `json:"statefulSetUID"`
	Voters              []struct {
		Ordinal      string `json:"ordinal"`
		PodUID       string `json:"podUID"`
		Identity     string `json:"identity"`
		LogsCaptured bool   `json:"logsCaptured"`
		Status       struct {
			Phase      string `json:"phase"`
			Conditions []struct {
				Type   string `json:"type"`
				Status string `json:"status"`
				Reason string `json:"reason"`
			} `json:"conditions"`
			Containers []struct {
				Ready        bool  `json:"ready"`
				RestartCount int32 `json:"restartCount"`
				State        struct {
					Waiting struct {
						Reason string `json:"reason"`
					} `json:"waiting"`
					Terminated struct {
						Reason   string `json:"reason"`
						ExitCode *int32 `json:"exitCode"`
						Signal   *int32 `json:"signal"`
					} `json:"terminated"`
				} `json:"state"`
				LastState struct {
					Terminated struct {
						Reason   string `json:"reason"`
						ExitCode *int32 `json:"exitCode"`
						Signal   *int32 `json:"signal"`
					} `json:"terminated"`
				} `json:"lastState"`
			} `json:"containers"`
		} `json:"status"`
	} `json:"voters"`
}

type voterStartupPublicView struct {
	StatefulSetPinned bool                      `json:"statefulSetPinned"`
	Voters            []voterStartupPublicEntry `json:"voters"`
}

type voterStartupPublicEntry struct {
	Ordinal     string               `json:"ordinal"`
	Identity    string               `json:"identity"`
	Phase       string               `json:"phase"`
	Conditions  []voterConditionView `json:"conditions"`
	Containers  []voterContainerView `json:"containers"`
	CurrentLog  voterLogView         `json:"currentLog"`
	PreviousLog voterLogView         `json:"previousLog"`
}

type voterConditionView struct {
	Type   string `json:"type"`
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
}

type voterContainerView struct {
	Ready        bool   `json:"ready"`
	RestartCount int32  `json:"restartCount"`
	Waiting      string `json:"waiting,omitempty"`
	Terminated   string `json:"terminated,omitempty"`
	ExitCode     *int32 `json:"exitCode,omitempty"`
	Signal       *int32 `json:"signal,omitempty"`
	LastReason   string `json:"lastReason,omitempty"`
	LastExitCode *int32 `json:"lastExitCode,omitempty"`
	LastSignal   *int32 `json:"lastSignal,omitempty"`
}

type voterLogView struct {
	Captured               bool     `json:"captured"`
	Bytes                  int      `json:"bytes"`
	SHA256                 string   `json:"sha256,omitempty"`
	Startup                []string `json:"startupCategories,omitempty"`
	StartupStages          []string `json:"startupStages,omitempty"`
	StartupStagesTruncated bool     `json:"startupStagesTruncated,omitempty"`
}

const maxVoterStartupStages = 8

var allowedVoterPhases = map[string]struct{}{"Pending": {}, "Running": {}, "Succeeded": {}, "Failed": {}, "Unknown": {}}
var allowedVoterConditionTypes = map[string]struct{}{"Ready": {}, "PodScheduled": {}, "Initialized": {}, "ContainersReady": {}}
var allowedVoterStatuses = map[string]struct{}{"True": {}, "False": {}, "Unknown": {}}
var allowedVoterReasons = map[string]struct{}{
	"CrashLoopBackOff": {}, "ImagePullBackOff": {}, "ErrImagePull": {}, "CreateContainerConfigError": {},
	"CreateContainerError": {}, "RunContainerError": {}, "ContainerCannotRun": {}, "OOMKilled": {},
	"Error": {}, "Completed": {}, "ContainerCreating": {}, "PodInitializing": {}, "PodFailed": {},
	"ContainersNotReady": {}, "Unschedulable": {}, "FailedScheduling": {}, "FailedMount": {},
	"FailedAttachVolume": {}, "FailedCreatePodSandBox": {}, "MinimumReplicasUnavailable": {},
}

var voterStartupPrefixes = []struct{ prefix, category string }{
	{"fencing: supervisor must be PID 1 in a private process namespace", "supervisor_pid_namespace_invalid"},
	{"fencing: incomplete supervisor configuration", "supervisor_config_incomplete"},
	{"fencing: child exited without a terminal receipt:", "supervisor_child_exit_unreceipted"},
	{"fork/exec ", "supervisor_child_exec_failed"},
	{"exec: ", "supervisor_child_exec_failed"},
	{"invalid RHIZA_", "config_invalid"},
	{"conflicting RHIZA_", "config_conflict"},
	{"fencing:", "supervisor_admission_failed"},
	{"acquire lock:", "wal_lock_failed"},
	{"open WAL:", "wal_open_failed"},
	{"open object store:", "object_store_open_failed"},
	{"load checkpoint manifest:", "checkpoint_load_failed"},
	{"pin startup recovery evidence:", "recovery_evidence_pin_failed"},
	{"recover shared archive:", "archive_recovery_failed"},
	{"load shared decision archive:", "archive_recovery_failed"},
	{"open materializer:", "materializer_open_failed"},
	{"rebuild materializer:", "materializer_rebuild_failed"},
	{"configure local graph indexes:", "materializer_config_failed"},
	{"create QuePaxa core:", "consensus_core_create_failed"},
	{"learner WAL identity differs", "wal_identity_mismatch"},
	{"restore generation anchor:", "recovery_restore_failed"},
	{"restore checkpoint recovery base:", "recovery_restore_failed"},
	{"recover certified log through ", "recovery_replay_failed"},
	{"replay local decisions:", "recovery_replay_failed"},
	{"renew startup recovery evidence:", "recovery_evidence_renew_failed"},
	{"listen peer QUIC:", "peer_listener_failed"},
	{"open rhiza:", "node_open_failed"},
	{"open learner:", "replica_open_failed"},
	{"open object-store:", "replica_open_failed"},
	{"configure learner:", "role_config_failed"},
	{"configure object-store:", "role_config_failed"},
	{"listen on ", "http_listener_failed"},
}

// These fixed source wrappers identify where Node.Open failed without
// publishing the nested error, which may contain private paths or credentials.
var voterStartupStagePrefixes = []struct{ prefix, stageCode string }{
	{"fencing: pending-removal startup requires an explicit nonvoting learner configuration", "startup_grant/role_rejected"},
	{"fencing: bind receipt", "wal_binding/receipt_rejected"},
	{"fencing: incomplete mutation producer binding", "wal_binding/producer_incomplete"},
	{"fencing: bind role differs from startup grant", "wal_binding/role_mismatch"},
	{"fencing: WAL identity cannot change", "wal_binding/identity_changed"},
	{"fencing: authenticated WAL binding differs", "wal_binding/identity_mismatch"},
	{"fencing: source producer binding changed", "wal_binding/producer_changed"},
	{"fencing: runtime enrollment identity differs", "wal_binding/runtime_identity_mismatch"},
	{"fencing: runtime enrollment is incomplete", "wal_binding/runtime_incomplete"},
	{"fencing: runtime cgroup scope digest is invalid", "wal_binding/runtime_scope_invalid"},
	{"supervised WAL binding requires an authenticated fencing session", "wal_binding/session_missing"},
	{"fencing: supervisor returned HTTP ", "supervisor_call/rejected"},
	{"fencing: authority temporarily unavailable", "supervisor_call/unavailable"},
	{"fencing: evidence pending", "supervisor_call/pending"},
	{"fencing: incarnation terminally fenced", "supervisor_call/closed"},
	{"fencing: supervisor response too large", "supervisor_call/response_too_large"},
	{"fencing: trailing supervisor response", "supervisor_call/response_trailing_data"},
	{"voter state continuity cannot be established: invalid local identity", "voter_identity/local_identity_invalid"},
	{"voter state continuity cannot be established: local identity or WAL manifest", "voter_identity/local_state_invalid"},
	{"voter state continuity cannot be established: authority-assigned restart", "voter_identity/assigned_restart_invalid"},
	{"voter state continuity cannot be established: WAL identity differs", "voter_identity/wal_identity_mismatch"},
	{"voter state continuity cannot be established: bound WAL has no local identity", "voter_identity/local_identity_missing"},
	{"voter state continuity cannot be established: voter configuration changed", "voter_identity/configuration_mismatch"},
	{"voter state continuity cannot be established: registered voter requires its shared object store", "voter_identity/object_store_missing"},
	{"voter state continuity cannot be established: membership cannot be changed during startup", "voter_identity/membership_changed"},
	{"voter state continuity cannot be established: registered WAL binding is absent", "voter_identity/wal_binding_missing"},
	{"voter state continuity cannot be established: immutable remote registration is missing", "voter_identity/remote_registration_missing"},
	{"voter state continuity cannot be established: enrollment requires the original WAL", "voter_identity/original_wal_missing"},
	{"voter state continuity cannot be established: immutable voter registration differs", "voter_identity/registration_mismatch"},
	{"voter state continuity cannot be established: invalid remote registration", "voter_identity/remote_registration_invalid"},
	{"voter state continuity cannot be established", "voter_identity/state_lost"},
	{"existing voter requires offline enrollment", "voter_identity/offline_enrollment_required"},
	{"read voter registration:", "voter_identity/remote_read_failed"},
	{"register voter:", "voter_identity/remote_write_failed"},
	{"voter registration requires conditional object writes", "voter_identity/conditional_write_unsupported"},
	{"fencing: pending learner identity registration is unavailable", "voter_identity/learner_registration_unavailable"},
	{"load shared decision archive:", "archive/load_failed"},
	{"open WAL:", "wal/open_failed"},
	{"open object store:", "object_store/open_failed"},
	{"prepare source retry directory:", "source_mutation/retry_directory_failed"},
	{"open source mutation client:", "source_mutation/client_open_failed"},
	{"supervised source mutations currently require GCS", "source_mutation/provider_unsupported"},
	{"fencing: validated mutation binding disappeared", "source_mutation/binding_missing"},
	{"read canonical archive genesis:", "archive_genesis/read_failed"},
	{"confirm canonical archive genesis:", "archive_genesis/confirmation_failed"},
	{"create canonical archive genesis:", "archive_genesis/create_failed"},
	{"load canonical archive genesis HEAD:", "archive_genesis/head_load_failed"},
	{"fencing: archive genesis", "archive_genesis/validation_failed"},
	{"fencing: canonical archive", "archive_genesis/validation_failed"},
	{"fencing: canonical empty recovery", "archive_genesis/empty_recovery_validation_failed"},
	{"fencing: verified archive fork", "archive_genesis/fork_validation_failed"},
	{"fencing: invalid archive fork lineage", "archive_genesis/fork_lineage_invalid"},
	{"fencing: archive fork", "archive_genesis/fork_validation_failed"},
	{"acquire lock:", "node_open/lock_failed"},
	{"load checkpoint manifest:", "recovery/checkpoint_manifest_failed"},
	{"pin startup recovery evidence:", "recovery/evidence_pin_failed"},
	{"recover shared archive:", "recovery/archive_failed"},
	{"open materializer:", "materializer/open_failed"},
	{"rebuild materializer:", "materializer/rebuild_failed"},
	{"configure local graph indexes:", "materializer/configure_failed"},
	{"create QuePaxa core:", "consensus/core_create_failed"},
	{"restore generation anchor:", "recovery/generation_anchor_failed"},
	{"restore checkpoint recovery base:", "recovery/checkpoint_base_failed"},
	{"recover certified log through ", "recovery/replay_failed"},
	{"replay local decisions:", "recovery/replay_failed"},
	{"renew startup recovery evidence:", "recovery/evidence_renew_failed"},
	{"listen peer QUIC:", "peer_listener/open_failed"},
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
		var evidence runtimeComponentEvidence
		if err := json.Unmarshal(data, &evidence); err != nil {
			return nil, err
		}
		if (evidence.EnrollmentDiagnosticStage != "" || evidence.EnrollmentDiagnosticCode != "") &&
			(!runtimeagent.ValidEnrollmentDiagnostic(evidence.EnrollmentDiagnosticStage, evidence.EnrollmentDiagnosticCode) || evidence.EnrollmentCaptured) {
			return nil, fmt.Errorf("runtime-agent enrollment diagnostic is invalid")
		}
		return json.Marshal(evidence)
	case name == "fencer-diagnostics.json":
		var view fencerDiagnosticsView
		if err := json.Unmarshal(data, &view); err != nil {
			return nil, err
		}
		if err := validateSupervisorRejections(view); err != nil {
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
	case name == "online-voter-diagnostics.json":
		return publicOnlineVoterDiagnostics(data)
	case name == "fencer-deployment.json":
		value = &fencerDeploymentView{}
	case name == "voter-startup-private.json":
		return nil, fmt.Errorf("private voter diagnostics require identity-bound projection")
	default:
		return nil, fmt.Errorf("artifact type is not approved")
	}
	if err := json.Unmarshal(data, value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

func projectVoterStartupDiagnostics(raw string, data []byte) ([]byte, error) {
	var source voterStartupPrivateView
	if err := json.Unmarshal(data, &source); err != nil {
		return nil, err
	}
	if source.StatefulSetIdentity != "pinned" && source.StatefulSetIdentity != "unavailable" {
		return nil, fmt.Errorf("voter diagnostic identity status is invalid")
	}
	if source.StatefulSetIdentity == "pinned" && (source.StatefulSetUID == "" || len(source.Voters) != 3) {
		return nil, fmt.Errorf("pinned voter diagnostic identity is incomplete")
	}
	if source.StatefulSetIdentity == "unavailable" && (source.StatefulSetUID != "" || len(source.Voters) != 0) {
		return nil, fmt.Errorf("unavailable voter diagnostic identity is inconsistent")
	}
	public := voterStartupPublicView{StatefulSetPinned: source.StatefulSetIdentity == "pinned", Voters: make([]voterStartupPublicEntry, 0, len(source.Voters))}
	seen := map[string]bool{}
	for _, voter := range source.Voters {
		if voter.Ordinal != "0" && voter.Ordinal != "1" && voter.Ordinal != "2" || seen[voter.Ordinal] {
			return nil, fmt.Errorf("voter diagnostic ordinal is invalid")
		}
		seen[voter.Ordinal] = true
		if voter.Identity != "verified" && voter.Identity != "identity_drift" && voter.Identity != "unavailable" {
			return nil, fmt.Errorf("voter diagnostic identity status is invalid")
		}
		entry := voterStartupPublicEntry{Ordinal: voter.Ordinal, Identity: voter.Identity, Phase: "Unknown"}
		if _, ok := allowedVoterPhases[voter.Status.Phase]; ok {
			entry.Phase = voter.Status.Phase
		}
		for _, condition := range voter.Status.Conditions {
			if _, ok := allowedVoterConditionTypes[condition.Type]; !ok {
				continue
			}
			if _, ok := allowedVoterStatuses[condition.Status]; !ok {
				continue
			}
			reason := ""
			if _, ok := allowedVoterReasons[condition.Reason]; ok {
				reason = condition.Reason
			}
			entry.Conditions = append(entry.Conditions, voterConditionView{Type: condition.Type, Status: condition.Status, Reason: reason})
		}
		for _, container := range voter.Status.Containers {
			view := voterContainerView{Ready: container.Ready, RestartCount: container.RestartCount,
				Waiting: allowedVoterReason(container.State.Waiting.Reason), Terminated: allowedVoterReason(container.State.Terminated.Reason),
				ExitCode: container.State.Terminated.ExitCode, Signal: container.State.Terminated.Signal,
				LastReason: allowedVoterReason(container.LastState.Terminated.Reason), LastExitCode: container.LastState.Terminated.ExitCode,
				LastSignal: container.LastState.Terminated.Signal}
			entry.Containers = append(entry.Containers, view)
		}
		if voter.Identity == "verified" && voter.LogsCaptured {
			privateDir := filepath.Join(raw, "private-voter-diagnostics", "rhiza-"+voter.Ordinal)
			entry.CurrentLog = readVoterLog(privateDir, "current", entry.Phase != "Running" || !allVoterContainersReady(voter.Status.Containers))
			entry.PreviousLog = readVoterLog(privateDir, "previous", entry.Phase != "Running" || !allVoterContainersReady(voter.Status.Containers))
		}
		public.Voters = append(public.Voters, entry)
	}
	return json.Marshal(public)
}

func allowedVoterReason(reason string) string {
	if _, ok := allowedVoterReasons[reason]; ok {
		return reason
	}
	return ""
}

func allVoterContainersReady(containers []struct {
	Ready        bool  `json:"ready"`
	RestartCount int32 `json:"restartCount"`
	State        struct {
		Waiting struct {
			Reason string `json:"reason"`
		} `json:"waiting"`
		Terminated struct {
			Reason   string `json:"reason"`
			ExitCode *int32 `json:"exitCode"`
			Signal   *int32 `json:"signal"`
		} `json:"terminated"`
	} `json:"state"`
	LastState struct {
		Terminated struct {
			Reason   string `json:"reason"`
			ExitCode *int32 `json:"exitCode"`
			Signal   *int32 `json:"signal"`
		} `json:"terminated"`
	} `json:"lastState"`
}) bool {
	if len(containers) != 1 {
		return false
	}
	return containers[0].Ready
}

func readVoterLog(dir, name string, startupFailure bool) voterLogView {
	var view voterLogView
	captured, err := os.ReadFile(filepath.Join(dir, name+".captured"))
	if err != nil || strings.TrimSpace(string(captured)) != "true" {
		return view
	}
	logPath := filepath.Join(dir, name+".log")
	info, err := os.Lstat(logPath)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > 16384 {
		return view
	}
	file, err := os.Open(logPath)
	if err != nil {
		return view
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 16385))
	if err != nil || len(data) > 16384 || bytes.Count(data, []byte("\n")) > 80 {
		return view
	}
	digest := sha256.Sum256(data)
	view.Captured = true
	view.Bytes = len(data)
	view.SHA256 = hex.EncodeToString(digest[:])
	view.Startup = classifyVoterStartupLog(data)
	view.StartupStages, view.StartupStagesTruncated = classifyVoterStartupStages(data)
	if startupFailure && len(view.Startup) == 0 && len(bytes.TrimSpace(data)) != 0 {
		view.Startup = []string{"unknown_startup_error"}
	}
	if startupFailure && len(view.StartupStages) == 0 && len(bytes.TrimSpace(data)) != 0 {
		view.StartupStages = []string{"node_open/outer_wrapper_unparsed"}
	}
	return view
}

func classifyVoterStartupLog(data []byte) []string {
	seen := map[string]bool{}
	for _, line := range strings.Split(string(data), "\n") {
		for _, item := range voterStartupPrefixes {
			if strings.HasPrefix(line, item.prefix) || strings.Contains(line, item.prefix) {
				seen[item.category] = true
			}
		}
	}
	var categories []string
	for _, item := range voterStartupPrefixes {
		if seen[item.category] {
			categories = append(categories, item.category)
		}
	}
	return categories
}

func classifyVoterStartupStages(data []byte) ([]string, bool) {
	seen := map[string]bool{}
	var stages []string
	truncated := false
	for _, line := range strings.Split(string(data), "\n") {
		if len(line) < 20 {
			continue
		}
		if _, err := time.Parse("2006/01/02 15:04:05 ", line[:20]); err != nil {
			continue
		}
		logText := line[20:]
		var supervisorStage string
		switch {
		case strings.HasPrefix(logText, "fencing: supervisor must be PID 1 in a private process namespace"):
			supervisorStage = "supervisor/pid_namespace_invalid"
		case strings.HasPrefix(logText, "fencing: incomplete supervisor configuration"):
			supervisorStage = "supervisor/config_incomplete"
		case strings.HasPrefix(logText, "fencing: supervisor returned HTTP "):
			supervisorStage = "supervisor/authority_rejected"
			if code := safeHTTPStatusCode(logText, "fencing: supervisor returned HTTP "); code != "" {
				supervisorStage += "_" + code
			}
		case strings.HasPrefix(logText, "fencing: child exited without a terminal receipt:"):
			supervisorStage = "supervisor/child_exit_unreceipted"
		case strings.HasPrefix(logText, "fork/exec ") || strings.HasPrefix(logText, "exec: "):
			supervisorStage = "supervisor/child_exec_failed"
		}
		if supervisorStage != "" {
			if !seen[supervisorStage] {
				seen[supervisorStage] = true
				if len(stages) < maxVoterStartupStages {
					stages = append(stages, supervisorStage)
				} else {
					truncated = true
				}
			}
			continue
		}
		if !strings.HasPrefix(logText, "open rhiza: ") {
			continue
		}
		errText := strings.TrimPrefix(logText, "open rhiza: ")
		unknownStage := "node_open/inner_error_unknown"
		if strings.HasPrefix(errText, "node open phase ") {
			phase, nested, ok := strings.Cut(strings.TrimPrefix(errText, "node open phase "), ": ")
			phaseCode := "node_open/phase_unknown"
			if ok {
				switch phase {
				case "configuration", "fencing", "local_state", "wal", "identity_storage", "archive", "materializer", "consensus", "recovery", "network", "services":
					phaseCode = "node_open/phase_" + phase
				}
				errText = nested
			} else {
				errText = ""
			}
			if !seen[phaseCode] {
				seen[phaseCode] = true
				if len(stages) < maxVoterStartupStages {
					stages = append(stages, phaseCode)
				} else {
					truncated = true
				}
			}
		}
		if strings.HasPrefix(errText, "connect fencing startup gate: ") {
			errText = strings.TrimPrefix(errText, "connect fencing startup gate: ")
			stageCode := "startup_grant/connect_failed"
			switch {
			case strings.HasPrefix(errText, "fencing: invalid startup grant:"):
				stageCode = "startup_grant/response_invalid"
			case strings.HasPrefix(errText, "fencing: startup grant logical identity differs from child configuration"):
				stageCode = "startup_grant/identity_mismatch"
			case strings.HasPrefix(errText, "fencing: supervisor returned HTTP "):
				stageCode = "startup_grant/rejected"
				if code := safeHTTPStatusCode(errText, "fencing: supervisor returned HTTP "); code != "" {
					stageCode += "_" + code
				}
			}
			if !seen[stageCode] {
				seen[stageCode] = true
				if len(stages) < maxVoterStartupStages {
					stages = append(stages, stageCode)
				} else {
					truncated = true
				}
			}
			continue
		}
		if strings.HasPrefix(errText, "ensure voter identity: ") {
			errText = strings.TrimPrefix(errText, "ensure voter identity: ")
			unknownStage = "voter_identity/guard_unknown"
		}
		if strings.HasPrefix(errText, "load voter identity: ") {
			errText = strings.TrimPrefix(errText, "load voter identity: ")
			unknownStage = "voter_identity/load_unknown"
		}
		if strings.HasPrefix(errText, "load shared decision archive: ") {
			errText = strings.TrimPrefix(errText, "load shared decision archive: ")
			unknownStage = "archive/guard_unknown"
		}
		if strings.HasPrefix(errText, "voter state continuity cannot be established: voter ") &&
			strings.HasSuffix(errText, " is registered but its local identity is absent") {
			const stageCode = "voter_identity/registration_without_local_identity"
			if !seen[stageCode] {
				seen[stageCode] = true
				if len(stages) < maxVoterStartupStages {
					stages = append(stages, stageCode)
				} else {
					truncated = true
				}
			}
			continue
		}
		matched := false
		for _, item := range voterStartupStagePrefixes {
			if !strings.HasPrefix(errText, item.prefix) {
				continue
			}
			matched = true
			if seen[item.stageCode] {
				break
			}
			seen[item.stageCode] = true
			if len(stages) < maxVoterStartupStages {
				stages = append(stages, item.stageCode)
			} else {
				truncated = true
			}
			break
		}
		if !matched {
			// A valid outer wrapper with an unknown nested error is useful to
			// distinguish from a pre-Node.Open supervisor registration failure.
			if !seen[unknownStage] {
				seen[unknownStage] = true
				if len(stages) < maxVoterStartupStages {
					stages = append(stages, unknownStage)
				} else {
					truncated = true
				}
			}
		}
	}
	return stages, truncated
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

func validateSupervisorRejections(view fencerDiagnosticsView) error {
	logs := view.Logs
	if len(logs.Rejections) > maxSupervisorRejectionDiagnostics {
		return fmt.Errorf("supervisor rejection diagnostics exceed bounds")
	}
	if logs.RejectionsTruncated && len(logs.Rejections) != maxSupervisorRejectionDiagnostics {
		return fmt.Errorf("supervisor rejection truncation marker is inconsistent")
	}
	if !logs.Captured && len(logs.Rejections) != 0 {
		return fmt.Errorf("supervisor rejection diagnostics lack captured logs")
	}
	for _, diagnostic := range logs.Rejections {
		if diagnostic.Action != "register" {
			return fmt.Errorf("supervisor rejection action is not allowlisted")
		}
		if diagnostic.Stage != "authentication" && diagnostic.Stage != "runtime_enrollment" && diagnostic.Stage != "transition" {
			return fmt.Errorf("supervisor rejection stage is not allowlisted")
		}
		if _, ok := allowedSupervisorRejectionCodes[diagnostic.Code]; !ok {
			return fmt.Errorf("supervisor rejection code is not allowlisted")
		}
	}
	return nil
}

func classifySupervisorRejection(line string) (supervisorRejectionDiagnostic, bool) {
	markers := []struct {
		marker string
		stage  string
	}{
		{"fencing supervisor authentication rejected:", "authentication"},
		{"runtime enrollment pending:", "runtime_enrollment"},
		{"fencing supervisor transition rejected:", "transition"},
	}
	for _, marker := range markers {
		at := strings.Index(line, marker.marker)
		if at < 0 || !validSupervisorLogPrefix(line[:at]) {
			continue
		}
		rest := line[at+len(marker.marker):]
		header, message, ok := strings.Cut(rest, " error=")
		if !ok {
			return supervisorRejectionDiagnostic{}, false
		}
		fields := strings.Fields(header)
		action := "register"
		if marker.stage == "runtime_enrollment" {
			// This marker is emitted only in the authenticated register branch.
			if len(fields) != 2 || !strings.HasPrefix(fields[0], "podUID=") || strings.TrimPrefix(fields[0], "podUID=") == "" || !strings.HasPrefix(fields[1], "runID=") || strings.TrimPrefix(fields[1], "runID=") == "" {
				return supervisorRejectionDiagnostic{}, false
			}
		} else if len(fields) != 2 || fields[0] != "action=register" || !strings.HasPrefix(fields[1], "podUID=") || strings.TrimPrefix(fields[1], "podUID=") == "" {
			return supervisorRejectionDiagnostic{}, false
		}
		code := classifySupervisorRejectionCode(marker.stage, message)
		return supervisorRejectionDiagnostic{Stage: marker.stage, Action: action, Code: code}, true
	}
	return supervisorRejectionDiagnostic{}, false
}

func classifySupervisorRejectionCode(stage, message string) string {
	if stage == "runtime_enrollment" {
		const typed = "fencing: runtime enrollment rejected at "
		if strings.HasPrefix(message, typed) {
			pair := strings.TrimPrefix(message, typed)
			pair = strings.TrimSpace(pair)
			diagnosticStage, diagnosticCode, ok := strings.Cut(pair, "/")
			if ok && runtimeagent.ValidEnrollmentDiagnostic(diagnosticStage, diagnosticCode) {
				return "runtimeagent_" + diagnosticStage + "_" + diagnosticCode
			}
		}
		for _, item := range runtimeEnrollmentErrorPrefixes {
			if strings.HasPrefix(message, item.message) {
				return item.code
			}
		}
		return "runtime_enrollment_guard_unknown"
	}
	if stage == "authentication" {
		for _, item := range authenticationErrorPrefixes {
			if strings.HasPrefix(message, item.message) {
				return item.code
			}
		}
		return "authentication_guard_unknown"
	}
	for _, item := range registerStageRejectionPrefixes {
		if strings.HasPrefix(message, item.message) {
			reason := strings.TrimSpace(strings.TrimPrefix(message, item.message))
			for _, known := range supervisorRejectionPrefixes {
				if strings.HasPrefix(reason, known.message) {
					return known.code
				}
			}
			return item.code
		}
	}
	for _, item := range supervisorRejectionPrefixes {
		if strings.HasPrefix(message, item.message) {
			return item.code
		}
	}
	return "register_guard_unknown"
}

func validSupervisorLogPrefix(prefix string) bool {
	if strings.HasPrefix(prefix, "[") {
		end := strings.Index(prefix, "] ")
		if end < 0 {
			return false
		}
		prefix = prefix[end+2:]
	}
	_, err := time.Parse("2006/01/02 15:04:05 ", prefix)
	return err == nil
}

func projectSupervisorRejections(raw string, view *fencerDiagnosticsView) error {
	view.Logs.Rejections = []supervisorRejectionDiagnostic{}
	view.Logs.RejectionsTruncated = false
	if !view.Logs.Captured {
		return nil
	}
	path := filepath.Join(raw, "private-fencer-diagnostics", "authority-current.log")
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > 65536 {
		return fmt.Errorf("fencer private log is unsafe or oversized")
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	logData, err := io.ReadAll(io.LimitReader(file, 65537))
	if err != nil || len(logData) > 65536 || bytes.Count(logData, []byte("\n")) > 200 {
		return fmt.Errorf("fencer private log exceeds bounds")
	}
	digest := sha256.Sum256(logData)
	if int64(len(logData)) != view.Logs.Bytes || int64(bytes.Count(logData, []byte("\n"))) != view.Logs.Lines || hex.EncodeToString(digest[:]) != view.Logs.SHA256 {
		return fmt.Errorf("fencer private log does not match its public receipt")
	}
	for _, line := range strings.Split(string(logData), "\n") {
		diagnostic, ok := classifySupervisorRejection(line)
		if !ok {
			continue
		}
		if len(view.Logs.Rejections) < maxSupervisorRejectionDiagnostics {
			view.Logs.Rejections = append(view.Logs.Rejections, diagnostic)
		} else {
			view.Logs.RejectionsTruncated = true
		}
	}
	return nil
}

func projectRuntimeAgentDiagnostics(raw string, data []byte) ([]byte, error) {
	var view fencerDiagnosticsView
	if err := json.Unmarshal(data, &view); err != nil {
		return nil, err
	}
	if err := projectSupervisorRejections(raw, &view); err != nil {
		return nil, err
	}
	view.RuntimeAgent.Diagnostics = []runtimeAgentEnrollmentDiagnostic{}
	if !view.RuntimeAgent.Logs.Captured {
		if err := validateSupervisorRejections(view); err != nil {
			return nil, err
		}
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
	if err := validateSupervisorRejections(view); err != nil {
		return nil, err
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

type gcsAsyncNonemptyPublicSummary struct {
	Qualification             string `json:"qualification"`
	Outcome                   string `json:"outcome"`
	Provider                  string `json:"provider"`
	NoPVC                     bool   `json:"noPVC"`
	SourceDurability          string `json:"sourceDurability"`
	AllowDataLoss             bool   `json:"allowDataLoss"`
	SourceAckCount            int    `json:"sourceAckCount"`
	LastSourceAckSlot         uint64 `json:"lastSourceAckSlot"`
	PersistedNonemptyTip      bool   `json:"persistedNonemptyTip"`
	SealedNonemptyTip         bool   `json:"sealedNonemptyTip"`
	RecoveredArchivedTip      uint64 `json:"recoveredArchivedTip"`
	RecoveredRowsVerified     bool   `json:"recoveredRowsVerified"`
	TargetQuorumVerified      bool   `json:"targetQuorumVerified"`
	TargetAckReadbackVerified bool   `json:"targetAckReadbackVerified"`
	PrivateEvidenceSHA256     string `json:"privateEvidenceSHA256,omitempty"`
	PrivateEventsSHA256       string `json:"privateEventsSHA256,omitempty"`
}

const (
	gcsAsyncNonemptyEvidenceName = "gcs-async-nonempty-evidence.json"
	gcsAsyncNonemptyEventsName   = "gcs-async-nonempty-events.jsonl"
	gcsAsyncNonemptySummaryName  = "gcs-async-nonempty-summary.json"
)

const (
	gcsManualTerminalQualification = "gcs-manual-terminal"
	gcsManualTerminalEvidenceName  = "gcs-manual-terminal-evidence.json"
	gcsManualTerminalChallengeName = "gcs-manual-terminal-challenge.json"
	gcsManualTerminalEventsName    = "gcs-manual-terminal-events.jsonl"
	gcsManualTerminalSummaryName   = "gcs-manual-terminal-summary.json"
)

const (
	gcsMissingMembershipNegativeQualification = "gcs-missing-membership-negative"
	gcsMissingMembershipPositiveQualification = "gcs-missing-membership-positive"
	gcsMissingMembershipNegativeEvidenceName  = "gcs-missing-membership-negative-evidence.json"
	gcsMissingMembershipPositiveEvidenceName  = "gcs-missing-membership-positive-evidence.json"
	gcsMissingMembershipEventsName            = "gcs-missing-membership-events.jsonl"
	gcsMissingMembershipSummaryName           = "gcs-missing-membership-summary.json"
)

const (
	gcsDeadPodQualification = "gcs-dead-pod"
	gcsDeadPodEvidenceName  = "gcs-dead-pod-evidence.json"
	gcsDeadPodEventsName    = "gcs-dead-pod-events.jsonl"
	gcsDeadPodSummaryName   = "gcs-dead-pod-summary.json"
)

type gcsDeadPodPublicSummary struct {
	Qualification                   string `json:"qualification"`
	Outcome                         string `json:"outcome"`
	Provider                        string `json:"provider"`
	SourceDurability                string `json:"sourceDurability"`
	CapturedVoterCount              int    `json:"capturedVoterCount"`
	SameOrdinalReplacementVerified  bool   `json:"sameOrdinalReplacementVerified"`
	ClosedIdentityExcluded          bool   `json:"closedIdentityExcluded"`
	PostRecoveryAckReadbackVerified bool   `json:"postRecoveryAckReadbackVerified"`
	ActiveChildRestartVerified      bool   `json:"activeChildRestartVerified"`
	PostRestartAckReadbackVerified  bool   `json:"postRestartAckReadbackVerified"`
	PrivateEvidenceSHA256           string `json:"privateEvidenceSHA256,omitempty"`
	PrivateEventsSHA256             string `json:"privateEventsSHA256,omitempty"`
}

type gcsDeadPodPrivateEvidence struct {
	Qualification   string                `json:"qualification"`
	Status          string                `json:"status"`
	Source          gcsRecoverySource     `json:"source"`
	OldIncarnation  operator.FenceTarget  `json:"oldIncarnation"`
	FenceRequest    operator.FenceRequest `json:"fenceRequest"`
	FenceProof      operator.FenceProof   `json:"independentV2Proof"`
	Recovery        operator.Resource     `json:"recovery"`
	Replacement     operator.FenceTarget  `json:"replacement"`
	BaselineAck     gcsAcknowledgement    `json:"baselineAck"`
	PostRecoveryAck gcsAcknowledgement    `json:"postRecoveryAck"`
	ActiveRestart   gcsDeadPodRestartView `json:"activeRestart"`
	PostRestartAck  gcsAcknowledgement    `json:"postRestartAck"`
	PostRestartRows json.RawMessage       `json:"postRestartRows"`
}

type gcsDeadPodRestartView struct {
	PodUID            string                   `json:"podUID"`
	NodeID            string                   `json:"nodeID"`
	WALIdentity       string                   `json:"walIdentity"`
	PreviousContainer string                   `json:"previousContainerID"`
	CurrentContainer  string                   `json:"currentContainerID"`
	PreviousRun       string                   `json:"previousRunID"`
	CurrentRun        string                   `json:"currentRunID"`
	Membership        network.MembershipStatus `json:"membership"`
}

func projectGCSDeadPodEvidence(raw, public, qualification string, failedRun bool) (bool, error) {
	entries, err := os.ReadDir(raw)
	if err != nil {
		return false, err
	}
	known := map[string]bool{gcsDeadPodEvidenceName: false, gcsDeadPodEventsName: false}
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, "gcs-dead-pod-") {
			if _, ok := known[name]; !ok {
				return false, fmt.Errorf("dead-Pod private artifact name is not allowlisted")
			}
			known[name] = true
		}
	}
	if !known[gcsDeadPodEvidenceName] && !known[gcsDeadPodEventsName] {
		return false, nil
	}
	if qualification != gcsDeadPodQualification {
		return false, fmt.Errorf("dead-Pod evidence requires its explicit artifact qualification selector")
	}
	if !known[gcsDeadPodEventsName] {
		return false, fmt.Errorf("dead-Pod evidence exists without its private event record")
	}
	events, err := readBoundedPrivateArtifact(filepath.Join(raw, gcsDeadPodEventsName), false)
	if err != nil {
		return false, err
	}
	eventNames := map[string]bool{}
	scan := bufio.NewScanner(bytes.NewReader(events))
	scan.Buffer(make([]byte, 4096), 1<<20)
	for scan.Scan() {
		var event struct {
			Event string `json:"event"`
		}
		if err := json.Unmarshal(scan.Bytes(), &event); err != nil || event.Event == "" {
			return false, fmt.Errorf("private dead-Pod event record is invalid")
		}
		eventNames[event.Event] = true
	}
	if err := scan.Err(); err != nil {
		return false, fmt.Errorf("private dead-Pod event record exceeds its scan limit")
	}
	eventsHash := sha256.Sum256(events)
	summary := gcsDeadPodPublicSummary{
		Qualification:       "gcs-single-voter-dead-pod",
		Outcome:             "failed",
		Provider:            "gcs",
		SourceDurability:    "before-ack",
		PrivateEventsSHA256: hex.EncodeToString(eventsHash[:]),
	}
	if !known[gcsDeadPodEvidenceName] {
		if !failedRun {
			return false, fmt.Errorf("successful dead-Pod run is missing its private evidence")
		}
		return true, writeGCSDeadPodSummary(public, summary)
	}
	evidenceBytes, err := readBoundedPrivateArtifact(filepath.Join(raw, gcsDeadPodEvidenceName), false)
	if err != nil {
		return false, err
	}
	var evidence gcsDeadPodPrivateEvidence
	decoder := json.NewDecoder(bytes.NewReader(evidenceBytes))
	if err := decoder.Decode(&evidence); err != nil || decoder.Decode(new(any)) != io.EOF {
		return false, fmt.Errorf("private dead-Pod evidence is invalid")
	}
	if err := validateGCSDeadPodPrivateEvidence(evidence, eventNames); err != nil {
		return false, err
	}
	evidenceHash := sha256.Sum256(evidenceBytes)
	summary.CapturedVoterCount = len(evidence.Source.Targets)
	summary.SameOrdinalReplacementVerified = true
	summary.ClosedIdentityExcluded = true
	summary.PostRecoveryAckReadbackVerified = true
	summary.ActiveChildRestartVerified = true
	summary.PostRestartAckReadbackVerified = true
	summary.PrivateEvidenceSHA256 = hex.EncodeToString(evidenceHash[:])
	if !failedRun {
		summary.Outcome = "passed"
	}
	return true, writeGCSDeadPodSummary(public, summary)
}

func validateGCSDeadPodPrivateEvidence(e gcsDeadPodPrivateEvidence, eventNames map[string]bool) error {
	old, replacement := e.OldIncarnation, e.Replacement
	if e.Qualification != "gcs-noPVC-single-voter-dead-pod" || e.Status != "qualified" ||
		e.Source.Bucket == "" || e.Source.Prefix == "" || e.Source.StatefulSetUID == "" || e.Source.ClusterID == "" ||
		e.Source.Durability != "before-ack" || len(e.Source.Targets) != 3 ||
		old.Pod == "" || old.PodUID == "" || old.NodeID == "" || old.WALIdentity == "" ||
		replacement.Pod != old.Pod || replacement.PodUID == "" || replacement.PodUID == old.PodUID ||
		replacement.NodeID == "" || replacement.NodeID == old.NodeID || replacement.WALIdentity == "" || replacement.WALIdentity == old.WALIdentity {
		return fmt.Errorf("private dead-Pod replacement evidence is incomplete")
	}
	if !gcsDeadPodAckMatches(e.BaselineAck, 1, "gcs-dead-pod-baseline") ||
		!gcsDeadPodAckMatches(e.PostRecoveryAck, 2, "gcs-dead-pod-post-recovery") ||
		!gcsDeadPodAckMatches(e.PostRestartAck, 3, "gcs-dead-pod-post-restart") ||
		string(e.PostRestartRows) != `[[1,"gcs-dead-pod-baseline"],[2,"gcs-dead-pod-post-recovery"],[3,"gcs-dead-pod-post-restart"]]` {
		return fmt.Errorf("private dead-Pod ACK/readback evidence differs from the expected rows")
	}
	if !gcsVoterFenceRequestMatches(e.FenceRequest, e.Source, old) ||
		recoveryFenceMatchesProof(e.Recovery, e.FenceRequest, e.FenceProof) != nil ||
		e.Recovery.Status.Source != e.Source.ClusterID || e.Recovery.Status.Target != e.Source.ClusterID ||
		e.Recovery.Status.StatefulSetUID != e.Source.StatefulSetUID || e.Recovery.Status.Phase != "Complete" || e.Recovery.Status.Stage != "Complete" ||
		e.Recovery.Spec.Membership == nil || e.Recovery.Spec.Membership.ReplacementPod != old.Pod ||
		e.Recovery.Status.Membership == nil || e.Recovery.Status.Membership.Phase != "Complete" ||
		e.Recovery.Status.Membership.Add == nil || e.Recovery.Status.Membership.Add.MemberID != quepaxa.NodeID(replacement.NodeID) ||
		e.Recovery.Status.Membership.Add.WALIdentity != replacement.WALIdentity ||
		e.ActiveRestart.PodUID != replacement.PodUID || e.ActiveRestart.NodeID != replacement.NodeID ||
		e.ActiveRestart.WALIdentity != replacement.WALIdentity || e.ActiveRestart.PreviousContainer == "" ||
		e.ActiveRestart.CurrentContainer == "" || e.ActiveRestart.CurrentContainer == e.ActiveRestart.PreviousContainer ||
		e.ActiveRestart.PreviousRun == "" || e.ActiveRestart.CurrentRun == "" || e.ActiveRestart.CurrentRun == e.ActiveRestart.PreviousRun {
		return fmt.Errorf("private dead-Pod fence or active-restart evidence is incomplete")
	}
	membership := e.ActiveRestart.Membership
	if membership.ClusterID != e.Source.ClusterID || string(membership.NodeID) != replacement.NodeID ||
		membership.WALIdentity != replacement.WALIdentity || membership.Pending || !membership.Voting || len(membership.Voters) != 3 {
		return fmt.Errorf("private dead-Pod restarted membership differs from the committed replacement")
	}
	oldIncluded := false
	newIncluded := false
	for _, voter := range membership.Voters {
		oldIncluded = oldIncluded || string(voter) == old.NodeID
		newIncluded = newIncluded || string(voter) == replacement.NodeID
	}
	if oldIncluded || !newIncluded || !eventNames["old_logical_identity_closed"] ||
		!eventNames["post_recovery_ack_durable"] || !eventNames["same_ordinal_active_restart_verified"] {
		return fmt.Errorf("private dead-Pod event or old-identity exclusion evidence is incomplete")
	}
	oldCaptured := false
	for _, sourceTarget := range e.Source.Targets {
		if sourceTarget.PodUID == old.PodUID {
			if sourceTarget.Pod != old.Pod || sourceTarget.NodeID != old.NodeID || sourceTarget.WALIdentity != old.WALIdentity {
				return fmt.Errorf("private dead-Pod old incarnation differs from captured source")
			}
			oldCaptured = true
			continue
		}
		found := false
		for _, voter := range membership.Voters {
			found = found || string(voter) == sourceTarget.NodeID
		}
		if !found {
			return fmt.Errorf("private dead-Pod membership omits a surviving captured voter")
		}
	}
	if !oldCaptured {
		return fmt.Errorf("private dead-Pod source omits the captured old incarnation")
	}
	return nil
}

func gcsDeadPodAckMatches(ack gcsAcknowledgement, id int, value string) bool {
	return ack.RequestID == fmt.Sprintf("gcs-ha-ack-%d", id) && ack.Slot != 0 &&
		ack.Rows == fmt.Sprintf("[[%d,%q]]", id, value)
}

func writeGCSDeadPodSummary(public string, summary gcsDeadPodPublicSummary) error {
	return os.WriteFile(filepath.Join(public, gcsDeadPodSummaryName), encoded(summary), 0600)
}

type gcsMissingMembershipPublicSummary struct {
	Qualification                string `json:"qualification"`
	Outcome                      string `json:"outcome"`
	Provider                     string `json:"provider"`
	Case                         string `json:"case"`
	NoPVC                        bool   `json:"noPVC,omitempty"`
	SourceDurability             string `json:"sourceDurability,omitempty"`
	AllowDataLoss                bool   `json:"allowDataLoss,omitempty"`
	AllowMissingSourceMembership bool   `json:"allowMissingSourceMembership,omitempty"`
	SourceMembershipAbsent       bool   `json:"sourceMembershipAbsent"`
	TargetMembershipVerified     bool   `json:"targetMembershipVerified,omitempty"`
	RecoveredRowsVerified        bool   `json:"recoveredRowsVerified,omitempty"`
	TargetQuorumVerified         bool   `json:"targetQuorumVerified,omitempty"`
	PrivateEvidenceSHA256        string `json:"privateEvidenceSHA256,omitempty"`
	PrivateEventsSHA256          string `json:"privateEventsSHA256,omitempty"`
}

func projectGCSMissingMembershipEvidence(raw, public, qualification string, failedRun bool) (bool, error) {
	entries, err := os.ReadDir(raw)
	if err != nil {
		return false, err
	}
	if qualification != gcsMissingMembershipNegativeQualification && qualification != gcsMissingMembershipPositiveQualification {
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), "gcs-missing-membership-") {
				return false, fmt.Errorf("missing-membership evidence requires its explicit case selector")
			}
		}
		return false, nil
	}
	caseName := "negative"
	evidenceName := gcsMissingMembershipNegativeEvidenceName
	if qualification == gcsMissingMembershipPositiveQualification {
		caseName = "positive"
		evidenceName = gcsMissingMembershipPositiveEvidenceName
	}
	seenEvidence, seenEvents := false, false
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "gcs-missing-membership-") {
			continue
		}
		if name == evidenceName && !entry.IsDir() {
			seenEvidence = true
		} else if name == gcsMissingMembershipEventsName && !entry.IsDir() {
			seenEvents = true
		} else {
			return false, fmt.Errorf("missing-membership private artifact name is not allowlisted")
		}
	}
	if !seenEvidence && !seenEvents {
		return false, nil
	}
	if !seenEvents {
		return false, fmt.Errorf("missing-membership evidence exists without its private event record")
	}
	events, err := readBoundedPrivateArtifact(filepath.Join(raw, gcsMissingMembershipEventsName), true)
	if err != nil {
		return false, err
	}
	eventsHash := sha256.Sum256(events)
	summary := gcsMissingMembershipPublicSummary{Qualification: qualification, Outcome: "failed", Provider: "gcs", Case: caseName,
		PrivateEventsSHA256: hex.EncodeToString(eventsHash[:])}
	if !seenEvidence {
		if !failedRun {
			return false, fmt.Errorf("successful missing-membership run is missing its private evidence record")
		}
		return true, os.WriteFile(filepath.Join(public, gcsMissingMembershipSummaryName), encoded(summary), 0600)
	}
	evidence, err := readBoundedPrivateArtifact(filepath.Join(raw, evidenceName), false)
	if err != nil {
		return false, err
	}
	if caseName == "negative" {
		if err := validateGCSMissingMembershipNegativeEvidence(evidence); err != nil {
			return false, err
		}
		summary.SourceMembershipAbsent = true
	} else {
		if err := validateGCSMissingMembershipPositiveEvidence(evidence); err != nil {
			return false, err
		}
		summary.NoPVC, summary.SourceMembershipAbsent = true, true
		summary.SourceDurability, summary.AllowDataLoss, summary.AllowMissingSourceMembership = "async", true, true
		summary.TargetMembershipVerified, summary.RecoveredRowsVerified, summary.TargetQuorumVerified = true, true, true
	}
	evidenceHash := sha256.Sum256(evidence)
	summary.PrivateEvidenceSHA256 = hex.EncodeToString(evidenceHash[:])
	if !failedRun {
		summary.Outcome = "passed"
	}
	return true, os.WriteFile(filepath.Join(public, gcsMissingMembershipSummaryName), encoded(summary), 0600)
}

func validateGCSMissingMembershipNegativeEvidence(data []byte) error {
	fields, err := exactJSONFields(data, "qualification", "case", "sourceClusterID", "statefulSetUID", "recoveryUID", "phase", "sourceMembershipAbsent", "sourceSuccessorAbsent", "targetScopeUnclaimed", "missingMembershipOptInAbsent")
	if err != nil {
		return err
	}
	var got struct {
		Qualification   string `json:"qualification"`
		Case            string `json:"case"`
		RecoveryUID     string `json:"recoveryUID"`
		Phase           string `json:"phase"`
		SourceAbsent    bool   `json:"sourceMembershipAbsent"`
		SuccessorAbsent bool   `json:"sourceSuccessorAbsent"`
		TargetUnclaimed bool   `json:"targetScopeUnclaimed"`
		OptInAbsent     bool   `json:"missingMembershipOptInAbsent"`
	}
	if json.Unmarshal(data, &got) != nil || got.Qualification != "gcs-missing-membership-no-opt-in-rejection" || got.Case != "negative" || got.RecoveryUID == "" || got.Phase != "Blocked" || !got.SourceAbsent || !got.SuccessorAbsent || !got.TargetUnclaimed || !got.OptInAbsent {
		return fmt.Errorf("private missing-membership negative evidence is invalid")
	}
	for _, key := range []string{"sourceClusterID", "statefulSetUID"} {
		var value string
		if json.Unmarshal(fields[key], &value) != nil || value == "" {
			return fmt.Errorf("private missing-membership source identity is incomplete")
		}
	}
	return nil
}

func validateGCSMissingMembershipPositiveEvidence(data []byte) error {
	fields, err := exactJSONFields(data, "qualification", "case", "profile", "sourceAcknowledgements", "sourceRows", "sourceHeadBeforeFence", "sealedSourceHead", "fence", "proof", "sourceIdentity", "recovery", "sourceMembershipAbsent", "sourceMembers", "targetCredentials", "targetMembership", "manualMembershipBindingDigest", "targetMembershipVerified", "recoveredRows", "targetQuorumVerified", "targetAck")
	if err != nil {
		return err
	}
	var qualification, caseName, sourceRows, recoveredRows, bindingDigest string
	var profile struct {
		Provider      string `json:"provider"`
		NoPVC         bool   `json:"noPVC"`
		Durability    string `json:"sourceDurability"`
		AllowDataLoss bool   `json:"allowDataLoss"`
		AllowMissing  bool   `json:"allowMissingSourceMembership"`
	}
	if json.Unmarshal(fields["qualification"], &qualification) != nil || qualification != "gcs-manual-missing-membership-nonempty-fork" ||
		json.Unmarshal(fields["case"], &caseName) != nil || caseName != "positive" ||
		json.Unmarshal(fields["profile"], &profile) != nil || profile.Provider != "gcs" || !profile.NoPVC || profile.Durability != "async" || !profile.AllowDataLoss || !profile.AllowMissing ||
		json.Unmarshal(fields["sourceRows"], &sourceRows) != nil || json.Unmarshal(fields["recoveredRows"], &recoveredRows) != nil || sourceRows == "" || sourceRows != recoveredRows ||
		json.Unmarshal(fields["manualMembershipBindingDigest"], &bindingDigest) != nil || len(bindingDigest) != 64 {
		return fmt.Errorf("private missing-membership positive evidence is invalid")
	}
	if _, err := hex.DecodeString(bindingDigest); err != nil {
		return fmt.Errorf("private missing-membership binding digest is invalid")
	}
	var sourceAcks []gcsAcknowledgement
	if json.Unmarshal(fields["sourceAcknowledgements"], &sourceAcks) != nil || len(sourceAcks) != 3 {
		return fmt.Errorf("private missing-membership source ACK set is incomplete")
	}
	lastSlot := uint64(0)
	for i, ack := range sourceAcks {
		wantRow := fmt.Sprintf("[[%d,%q]]", i+1, fmt.Sprintf("gcs-missing-membership-source-%d", i+1))
		if ack.RequestID != fmt.Sprintf("gcs-ha-ack-%d", i+1) || ack.Slot <= lastSlot || ack.Rows != wantRow {
			return fmt.Errorf("private missing-membership source ACK differs from expected rows")
		}
		lastSlot = ack.Slot
	}
	var before, sealed gcsArchiveHeadEvidence
	if json.Unmarshal(fields["sourceHeadBeforeFence"], &before) != nil || json.Unmarshal(fields["sealedSourceHead"], &sealed) != nil || validateSealedNonemptyHead(before, sealed) != nil || before.Tip < lastSlot {
		return fmt.Errorf("private missing-membership archived source tip is invalid")
	}
	var sourceIdentity struct {
		Namespace      string                 `json:"namespace"`
		StatefulSet    string                 `json:"statefulSet"`
		Container      string                 `json:"container"`
		StatefulSetUID string                 `json:"statefulSetUID"`
		BindingUID     string                 `json:"bindingUID"`
		LogicalID      string                 `json:"logicalID"`
		ClusterID      string                 `json:"clusterID"`
		Bucket         string                 `json:"bucket"`
		Prefix         string                 `json:"prefix"`
		Targets        []operator.FenceTarget `json:"targets"`
	}
	if _, err := exactJSONFields(fields["sourceIdentity"], "namespace", "statefulSet", "container", "statefulSetUID", "bindingUID", "logicalID", "clusterID", "bucket", "prefix", "targets"); err != nil ||
		json.Unmarshal(fields["sourceIdentity"], &sourceIdentity) != nil || sourceIdentity.Namespace == "" || sourceIdentity.StatefulSet == "" || sourceIdentity.Container == "" || sourceIdentity.StatefulSetUID == "" || sourceIdentity.BindingUID == "" || sourceIdentity.LogicalID == "" || sourceIdentity.ClusterID == "" || sourceIdentity.Bucket == "" || sourceIdentity.Prefix == "" || len(sourceIdentity.Targets) != 3 {
		return fmt.Errorf("private missing-membership source identity is incomplete")
	}
	scope := fencing.ArchiveScope{Provider: "gcs", Bucket: sourceIdentity.Bucket, Prefix: sourceIdentity.Prefix, ClusterID: sourceIdentity.ClusterID}
	scopeHash, err := scope.Hash()
	if err != nil {
		return fmt.Errorf("private missing-membership archive scope is invalid")
	}
	targetID, err := recovery.FrozenTargetID(recovery.FrozenSourceIdentity{ScopeHash: scopeHash, ClusterID: sourceIdentity.ClusterID, StatefulSetUID: sourceIdentity.StatefulSetUID})
	if err != nil {
		return fmt.Errorf("private missing-membership target identity is invalid")
	}
	var fence operator.FenceRequest
	if json.Unmarshal(fields["fence"], &fence) != nil || fence.Scope != operator.ScopeGeneration || fence.OperationID == "" || fence.Namespace != sourceIdentity.Namespace ||
		fence.StatefulSet != sourceIdentity.StatefulSet || fence.StatefulSetUID != sourceIdentity.StatefulSetUID || fence.BindingUID != sourceIdentity.BindingUID ||
		fence.LogicalID != sourceIdentity.LogicalID || fence.SourceClusterID != sourceIdentity.ClusterID || !sameTargets(fence.Targets, sourceIdentity.Targets) {
		return fmt.Errorf("private missing-membership fence is not bound to the captured source cohort")
	}
	var proof operator.FenceProof
	if json.Unmarshal(fields["proof"], &proof) != nil || proof.IndependentV2 == nil || proof.Independent != nil {
		return fmt.Errorf("private missing-membership independent generation proof is incomplete")
	}
	fenceItem := obj{"spec": fence, "status": obj{"proof": proof}}
	if _, _, err := verifyIndependentGenerationProof(fenceItem, sourceIdentity.ClusterID, sourceIdentity.StatefulSetUID,
		sourceIdentity.BindingUID, sourceIdentity.Targets, sourceIdentity.Bucket, sourceIdentity.Prefix,
		sourceIdentity.Namespace, sourceIdentity.StatefulSet, sourceIdentity.LogicalID); err != nil {
		return fmt.Errorf("private missing-membership IndependentV2 proof is invalid: %w", err)
	}
	var recoveryResource operator.Resource
	if json.Unmarshal(fields["recovery"], &recoveryResource) != nil || recoveryResource.Metadata["uid"] == nil || text(recoveryResource.Metadata["uid"]) == "" ||
		text(recoveryResource.Metadata["namespace"]) != sourceIdentity.Namespace || text(nested(recoveryResource.Metadata, "labels", "chaos.rhiza.io/run")) != qualificationRunID ||
		recoveryResource.Spec.Durability != "async" || !recoveryResource.Spec.AllowDataLoss || !recoveryResource.Spec.AllowMissingSourceMembership ||
		recoveryResource.Spec.SourceClusterID != sourceIdentity.ClusterID || recoveryResource.Spec.StatefulSet != sourceIdentity.StatefulSet || recoveryResource.Spec.Container != sourceIdentity.Container ||
		recoveryResource.Spec.RecoveryID != fence.OperationID || recoveryResource.Status.Phase != "Complete" || recoveryResource.Status.Stage != "Complete" ||
		recoveryResource.Status.Source != sourceIdentity.ClusterID || recoveryResource.Status.SourceDurability != "async" || recoveryResource.Status.StatefulSetUID != sourceIdentity.StatefulSetUID ||
		recoveryResource.Status.Target != targetID || recoveryResource.Status.TargetBaseKind != "fork" || recoveryResource.Status.RecoveredTip != before.Tip || recoveryResource.Status.ManifestHash == "" ||
		recoveryResource.Status.ManualMembershipBindingDigest != bindingDigest || recoveryFenceMatchesProof(recoveryResource, fence, proof) != nil {
		return fmt.Errorf("private missing-membership recovery does not match its canonical fork binding")
	}
	var sourceMembers []quepaxa.Member
	var targetMembership recovery.MembershipRecord
	if json.Unmarshal(fields["sourceMembers"], &sourceMembers) != nil || len(sourceMembers) != 3 || json.Unmarshal(fields["targetMembership"], &targetMembership) != nil {
		return fmt.Errorf("private missing-membership membership evidence is incomplete")
	}
	wantMembers := make(map[string]string, len(sourceIdentity.Targets))
	for _, target := range sourceIdentity.Targets {
		if target.NodeID == "" || target.Pod == "" || target.PodUID == "" || target.WALIdentity == "" || wantMembers[target.NodeID] != "" {
			return fmt.Errorf("private missing-membership captured target identities are invalid")
		}
		wantMembers[target.NodeID] = target.WALIdentity
	}
	seenMembers := make(map[string]bool, len(sourceMembers))
	for _, member := range sourceMembers {
		id := string(member.ID)
		if id == "" || seenMembers[id] || wantMembers[id] == "" || member.WALIdentity != wantMembers[id] || member.URL == "" || member.LogURL == "" {
			return fmt.Errorf("private missing-membership source cohort differs from captured identities")
		}
		seenMembers[id] = true
	}
	if len(seenMembers) != len(wantMembers) {
		return fmt.Errorf("private missing-membership source membership omits a captured voter")
	}
	var targetCredentials struct {
		SecretName       string           `json:"secretName"`
		SecretUID        string           `json:"secretUID"`
		ResourceVersion  string           `json:"resourceVersion"`
		OwnerOperationID string           `json:"ownerOperationID"`
		Immutable        bool             `json:"immutable"`
		MembersSHA256    string           `json:"membersSHA256"`
		Members          []quepaxa.Member `json:"members"`
	}
	if _, err := exactJSONFields(fields["targetCredentials"], "secretName", "secretUID", "resourceVersion", "ownerOperationID", "immutable", "membersSHA256", "members"); err != nil ||
		json.Unmarshal(fields["targetCredentials"], &targetCredentials) != nil || targetCredentials.SecretUID == "" || targetCredentials.ResourceVersion == "" ||
		targetCredentials.OwnerOperationID != text(recoveryResource.Metadata["uid"])+":"+recoveryResource.Spec.RecoveryID ||
		targetCredentials.SecretName != recoveryResource.Status.SecretName || targetCredentials.SecretName != "rhiza-recovery-"+gcsShortID(targetID) ||
		!targetCredentials.Immutable || len(targetCredentials.Members) != 3 {
		return fmt.Errorf("private missing-membership target credential identity is incomplete")
	}
	canonicalTargetMembers := encoded(targetCredentials.Members)
	targetMembersHash := sha256.Sum256(canonicalTargetMembers)
	if targetCredentials.MembersSHA256 != hex.EncodeToString(targetMembersHash[:]) {
		return fmt.Errorf("private missing-membership target credential membership digest is invalid")
	}
	targetSeen := make(map[string]bool, len(targetCredentials.Members))
	for _, member := range targetCredentials.Members {
		id := string(member.ID)
		var sourceMember *quepaxa.Member
		for i := range sourceMembers {
			if string(sourceMembers[i].ID) == id {
				sourceMember = &sourceMembers[i]
				break
			}
		}
		if sourceMember == nil || targetSeen[id] || member.WALIdentity != sourceMember.WALIdentity || member.URL != sourceMember.URL ||
			member.PeerURL != sourceMember.PeerURL || member.LogURL != sourceMember.LogURL || member.PublicKey == (quepaxa.PublicKey{}) || member.PublicKey == sourceMember.PublicKey {
			return fmt.Errorf("private missing-membership target credentials are not fresh keys for the exact source cohort")
		}
		targetSeen[id] = true
	}
	if len(targetSeen) != 3 || targetMembership != recovery.NewMembershipRecord(targetID, targetCredentials.Members, "async") {
		return fmt.Errorf("private missing-membership target membership is not bound to the exact fresh credential cohort")
	}
	var absent, targetMembershipVerified, targetQuorum bool
	if json.Unmarshal(fields["sourceMembershipAbsent"], &absent) != nil || !absent || json.Unmarshal(fields["targetMembershipVerified"], &targetMembershipVerified) != nil || !targetMembershipVerified ||
		json.Unmarshal(fields["targetQuorumVerified"], &targetQuorum) != nil || !targetQuorum {
		return fmt.Errorf("private missing-membership source/target assertions are incomplete")
	}
	var targetAck gcsAcknowledgement
	if json.Unmarshal(fields["targetAck"], &targetAck) != nil || targetAck.RequestID != "gcs-ha-ack-4" || targetAck.Slot <= before.Tip || targetAck.Rows != `[[4,"gcs-missing-membership-target"]]` {
		return fmt.Errorf("private missing-membership target ACK is invalid")
	}
	return nil
}

func projectGCSAsyncNonemptyEvidence(raw, public string, failedRun bool) (bool, error) {
	entries, err := os.ReadDir(raw)
	if err != nil {
		return false, err
	}
	known := map[string]bool{gcsAsyncNonemptyEvidenceName: false, gcsAsyncNonemptyEventsName: false}
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, "gcs-async-nonempty-") {
			if _, ok := known[name]; !ok {
				return false, fmt.Errorf("async-nonempty private artifact name is not allowlisted")
			}
			known[name] = true
		}
	}
	if !known[gcsAsyncNonemptyEvidenceName] && !known[gcsAsyncNonemptyEventsName] {
		return false, nil
	}
	if !known[gcsAsyncNonemptyEventsName] {
		return false, fmt.Errorf("async-nonempty evidence exists without its private event record")
	}
	events, err := readBoundedPrivateArtifact(filepath.Join(raw, gcsAsyncNonemptyEventsName), true)
	if err != nil {
		return false, err
	}
	eventsHash := sha256.Sum256(events)
	summary := gcsAsyncNonemptyPublicSummary{
		Qualification:       "gcs-async-nonempty-tip-recovery",
		Outcome:             "failed",
		PrivateEventsSHA256: hex.EncodeToString(eventsHash[:]),
	}
	if !known[gcsAsyncNonemptyEvidenceName] {
		if !failedRun {
			return false, fmt.Errorf("successful async-nonempty run is missing its private evidence record")
		}
		return true, writeGCSAsyncNonemptySummary(public, summary)
	}
	evidence, err := readBoundedPrivateArtifact(filepath.Join(raw, gcsAsyncNonemptyEvidenceName), false)
	if err != nil {
		return false, err
	}
	if err := validateGCSAsyncNonemptyPrivateEvidence(evidence, &summary); err != nil {
		return false, err
	}
	evidenceHash := sha256.Sum256(evidence)
	summary.PrivateEvidenceSHA256 = hex.EncodeToString(evidenceHash[:])
	if !failedRun {
		summary.Outcome = "passed"
	}
	return true, writeGCSAsyncNonemptySummary(public, summary)
}

type gcsManualTerminalPublicSummary struct {
	Qualification          string `json:"qualification"`
	Outcome                string `json:"outcome"`
	Provider               string `json:"provider"`
	NoPVC                  bool   `json:"noPVC"`
	SourceDurability       string `json:"sourceDurability"`
	AllowDataLoss          bool   `json:"allowDataLoss"`
	SourceAcknowledged     bool   `json:"sourceAcknowledged"`
	NonemptySourceSealed   bool   `json:"nonemptySourceSealed"`
	IndependentV2Absent    bool   `json:"independentV2Absent"`
	ChallengeObserved      bool   `json:"challengeObserved"`
	IndependentApproval    bool   `json:"independentApprovalObserved"`
	NoPreapprovalTarget    bool   `json:"noPreapprovalTargetClaim"`
	RecoveryCommitted      bool   `json:"recoveryCommitted"`
	TargetReadbackVerified bool   `json:"targetReadbackVerified"`
	PrivateEvidenceSHA256  string `json:"privateEvidenceSHA256,omitempty"`
	PrivateEventsSHA256    string `json:"privateEventsSHA256,omitempty"`
}

type gcsManualTerminalHistoricalRun struct {
	Identity       fencing.Identity        `json:"identity"`
	Runtime        *fencing.RuntimeBinding `json:"runtime"`
	runtimePresent bool
}

func (run *gcsManualTerminalHistoricalRun) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	if _, ok := fields["runtime"]; !ok {
		return fmt.Errorf("manual-terminal historical run omits runtime status")
	}
	type historicalRunAlias gcsManualTerminalHistoricalRun
	var decoded historicalRunAlias
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		return err
	}
	run.Identity = decoded.Identity
	run.Runtime = decoded.Runtime
	run.runtimePresent = true
	return nil
}

// This packet is bounded private review context, not independent termination proof.
type gcsManualTerminalVerifiedContext struct {
	RootDigest             string                           `json:"rootDigest"`
	FenceRequest           operator.FenceRequest            `json:"fenceRequest"`
	FenceRequestHash       string                           `json:"fenceRequestHash"`
	SourceDataScope        fencing.ArchiveScope             `json:"sourceDataScope"`
	SourceControlPrefix    string                           `json:"sourceControlPrefix"`
	SuccessorClaimKey      string                           `json:"successorClaimKey"`
	TargetDataScope        fencing.ArchiveScope             `json:"targetDataScope"`
	TargetControlKeys      []string                         `json:"targetControlKeys"`
	UnclaimedObservedAt    time.Time                        `json:"unclaimedObservedAt"`
	HistoricalRunInventory []gcsManualTerminalHistoricalRun `json:"historicalRunInventory"`
	HistoricalRunCount     int                              `json:"historicalRunCount"`
}

type gcsManualTerminalChallengeRecord struct {
	Challenge       operator.ManualRecoveryChallenge `json:"challenge"`
	ChallengeDigest string                           `json:"challengeDigest"`
	AttestationName string                           `json:"attestationName"`
	VerifiedContext gcsManualTerminalVerifiedContext `json:"verifiedContext"`
}

func projectGCSManualTerminalEvidence(raw, public string, failedRun bool) (bool, error) {
	entries, err := os.ReadDir(raw)
	if err != nil {
		return false, err
	}
	known := map[string]bool{gcsManualTerminalEvidenceName: false, gcsManualTerminalChallengeName: false, gcsManualTerminalEventsName: false}
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, "gcs-manual-terminal-") {
			if _, ok := known[name]; !ok {
				return false, fmt.Errorf("manual-terminal private artifact name is not allowlisted")
			}
			known[name] = true
		}
	}
	seen := known[gcsManualTerminalEvidenceName] || known[gcsManualTerminalChallengeName] || known[gcsManualTerminalEventsName]
	if !seen {
		return false, nil
	}
	if !known[gcsManualTerminalEventsName] {
		return false, fmt.Errorf("manual-terminal evidence exists without its private event record")
	}
	events, err := readBoundedPrivateArtifact(filepath.Join(raw, gcsManualTerminalEventsName), true)
	if err != nil {
		return false, err
	}
	eventsHash := sha256.Sum256(events)
	summary := gcsManualTerminalPublicSummary{Qualification: gcsManualTerminalQualification, Outcome: "failed", PrivateEventsSHA256: hex.EncodeToString(eventsHash[:])}
	challengeDigest := ""
	if known[gcsManualTerminalChallengeName] {
		challenge, err := readBoundedPrivateArtifact(filepath.Join(raw, gcsManualTerminalChallengeName), false)
		if err != nil {
			return false, err
		}
		var challengeRecord gcsManualTerminalChallengeRecord
		decoder := json.NewDecoder(bytes.NewReader(challenge))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&challengeRecord); err != nil || decoder.Decode(new(any)) != io.EOF {
			return false, fmt.Errorf("private manual-terminal challenge is invalid")
		}
		digest, err := operator.ManualRecoveryChallengeDigest(challengeRecord.Challenge)
		attestationName, nameErr := operator.ManualRecoveryAttestationName(challengeRecord.Challenge.RecoveryUID)
		if err != nil || nameErr != nil || digest != challengeRecord.ChallengeDigest || attestationName != challengeRecord.AttestationName ||
			challengeRecord.Challenge.TargetBaseKind != "fork" || challengeRecord.Challenge.TargetClusterID == "" ||
			validateGCSManualTerminalVerifiedContext(challengeRecord) != nil {
			return false, fmt.Errorf("private manual-terminal challenge binding is invalid")
		}
		challengeDigest = challengeRecord.ChallengeDigest
		summary.ChallengeObserved = true
	}
	if !known[gcsManualTerminalEvidenceName] {
		if !failedRun {
			return false, fmt.Errorf("successful manual-terminal run is missing private evidence")
		}
		return true, writeGCSManualTerminalSummary(public, summary)
	}
	evidence, err := readBoundedPrivateArtifact(filepath.Join(raw, gcsManualTerminalEvidenceName), false)
	if err != nil {
		return false, err
	}
	if !summary.ChallengeObserved {
		return false, fmt.Errorf("manual-terminal evidence is missing its separately captured challenge")
	}
	if err := validateGCSManualTerminalPrivateEvidence(evidence, challengeDigest, &summary); err != nil {
		return false, err
	}
	evidenceHash := sha256.Sum256(evidence)
	summary.PrivateEvidenceSHA256 = hex.EncodeToString(evidenceHash[:])
	if !failedRun {
		summary.Outcome = "passed"
	}
	return true, writeGCSManualTerminalSummary(public, summary)
}

func validateGCSManualTerminalVerifiedContext(record gcsManualTerminalChallengeRecord) error {
	context := record.VerifiedContext
	challenge := record.Challenge
	if context.RootDigest == "" || context.RootDigest != challenge.RootDigest || context.FenceRequestHash == "" ||
		context.FenceRequestHash != challenge.FenceRequestHash || context.HistoricalRunCount <= 0 ||
		context.HistoricalRunCount != len(context.HistoricalRunInventory) || context.UnclaimedObservedAt.IsZero() ||
		context.UnclaimedObservedAt.Before(challenge.IssuedAt) || context.UnclaimedObservedAt.After(challenge.ExpiresAt) {
		return fmt.Errorf("manual-terminal verified review context is incomplete")
	}
	requestDigest := sha256.Sum256(encoded(context.FenceRequest))
	if hex.EncodeToString(requestDigest[:]) != context.FenceRequestHash ||
		context.FenceRequest.Scope != operator.ScopeGeneration || context.FenceRequest.OperationID != challenge.OperationID ||
		context.FenceRequest.SourceClusterID != challenge.SourceClusterID || context.FenceRequest.StatefulSetUID != challenge.StatefulSetUID ||
		context.FenceRequest.StatefulSet != challenge.StatefulSetName || context.FenceRequest.Namespace != challenge.RecoveryNamespace ||
		context.FenceRequest.BootstrapGenesis != nil {
		return fmt.Errorf("manual-terminal request context differs from its challenge")
	}
	sourceScope := context.SourceDataScope
	sourceScopeHash, err := sourceScope.Hash()
	if err != nil || sourceScope.Provider != "gcs" || sourceScope.ClusterID != challenge.SourceClusterID ||
		sourceScopeHash != challenge.StorageScopeDigest {
		return fmt.Errorf("manual-terminal source scope context differs from its challenge")
	}
	targetPrefix := path.Join(path.Dir(sourceScope.Prefix), challenge.TargetClusterID)
	if context.TargetDataScope.Provider != "gcs" || context.TargetDataScope.Bucket != sourceScope.Bucket ||
		context.TargetDataScope.Prefix != targetPrefix || context.TargetDataScope.ClusterID != challenge.TargetClusterID ||
		context.SourceControlPrefix != path.Join(path.Dir(sourceScope.Prefix), "control") || context.SuccessorClaimKey == "" {
		return fmt.Errorf("manual-terminal target scope context is invalid")
	}
	claimKey, err := manualTerminalSuccessorClaimKey(gcsRecoverySource{
		Bucket: sourceScope.Bucket, Prefix: sourceScope.Prefix, ClusterID: sourceScope.ClusterID,
		StatefulSetUID: challenge.StatefulSetUID,
	}, context.SourceControlPrefix)
	if err != nil || claimKey != context.SuccessorClaimKey {
		return fmt.Errorf("manual-terminal successor claim context is invalid")
	}
	targetKeys, err := manualTerminalTargetControlKeys(gcsRecoverySource{Bucket: sourceScope.Bucket, StatefulSetUID: challenge.StatefulSetUID},
		context.SourceControlPrefix, targetPrefix, challenge.TargetClusterID)
	if err != nil || len(targetKeys) != len(context.TargetControlKeys) {
		return fmt.Errorf("manual-terminal target CONTROL scope context is invalid")
	}
	for i := range targetKeys {
		if targetKeys[i] != context.TargetControlKeys[i] {
			return fmt.Errorf("manual-terminal target CONTROL scope context is invalid")
		}
	}
	previousRunID := ""
	for _, run := range context.HistoricalRunInventory {
		if !run.runtimePresent || run.Identity.SourceClusterID == "" || run.Identity.SourceClusterID != challenge.SourceClusterID ||
			run.Identity.NodeID == "" || run.Identity.PodUID == "" || run.Identity.RunID == "" ||
			(run.Identity.PodName != "" && run.Identity.PodName != run.Identity.NodeID && run.Identity.AssignmentID == "") ||
			(run.Identity.AssignmentID != "" && (run.Identity.PodName == "" || run.Identity.PodName == run.Identity.NodeID)) ||
			(previousRunID != "" && run.Identity.RunID <= previousRunID) {
			return fmt.Errorf("manual-terminal historical run inventory is incomplete or unordered")
		}
		if run.Runtime != nil {
			if err := run.Runtime.Validate(run.Identity); err != nil {
				return fmt.Errorf("manual-terminal historical run runtime differs from identity: %w", err)
			}
		}
		previousRunID = run.Identity.RunID
	}
	return nil
}

func validateGCSManualTerminalPrivateEvidence(data []byte, expectedChallengeDigest string, summary *gcsManualTerminalPublicSummary) error {
	fields, err := exactJSONFields(data, "qualification", "profile", "sourceAck", "sourceHeadBeforeFence", "sealedSourceHead", "fence", "fenceProof", "challenge", "challengeDigest", "independentApproval", "approvalDigest", "preApprovalTargetUnclaimed", "recovery", "targetQuorumVerified", "recoveredRows", "targetAck", "independentV2Absent")
	if err != nil {
		return err
	}
	var qualification string
	if err := json.Unmarshal(fields["qualification"], &qualification); err != nil || qualification != gcsManualTerminalQualification {
		return fmt.Errorf("private manual-terminal qualification is unknown")
	}
	var profile struct {
		Provider         string `json:"provider"`
		NoPVC            bool   `json:"noPVC"`
		SourceDurability string `json:"sourceDurability"`
		AllowDataLoss    bool   `json:"allowDataLoss"`
	}
	if _, err := exactJSONFields(fields["profile"], "provider", "noPVC", "sourceDurability", "allowDataLoss"); err != nil {
		return err
	}
	if err := json.Unmarshal(fields["profile"], &profile); err != nil || profile.Provider != "gcs" || !profile.NoPVC ||
		(profile.SourceDurability != "async" && profile.SourceDurability != "before-ack") || profile.AllowDataLoss != (profile.SourceDurability == "async") {
		return fmt.Errorf("private manual-terminal GCS profile is invalid")
	}
	var sourceAck, targetAck gcsAcknowledgement
	if _, err := exactJSONFields(fields["sourceAck"], "requestID", "slot", "rows"); err != nil {
		return err
	}
	if _, err := exactJSONFields(fields["targetAck"], "requestID", "slot", "rows"); err != nil {
		return err
	}
	if err := json.Unmarshal(fields["sourceAck"], &sourceAck); err != nil || sourceAck.RequestID != "gcs-ha-ack-1" || sourceAck.Slot == 0 || sourceAck.Rows != `[[1,"gcs-manual-terminal-source"]]` {
		return fmt.Errorf("private manual-terminal source ACK is invalid")
	}
	var before, sealed gcsArchiveHeadEvidence
	for name, head := range map[string]*gcsArchiveHeadEvidence{"sourceHeadBeforeFence": &before, "sealedSourceHead": &sealed} {
		if _, err := exactJSONFields(fields[name], "versionType", "version", "tip", "sealed", "headSHA256"); err != nil {
			return err
		}
		if err := json.Unmarshal(fields[name], head); err != nil {
			return err
		}
	}
	if before.Tip < sourceAck.Slot {
		return fmt.Errorf("private manual-terminal source HEAD omits the acknowledged row")
	}
	if err := validateSealedNonemptyHead(before, sealed); err != nil {
		return err
	}
	var fence operator.FenceRequest
	if err := json.Unmarshal(fields["fence"], &fence); err != nil || fence.Scope != operator.ScopeGeneration || fence.OperationID == "" || fence.SourceClusterID == "" {
		return fmt.Errorf("private manual-terminal generation fence is invalid")
	}
	var proof operator.FenceProof
	if string(fields["fenceProof"]) == "null" || json.Unmarshal(fields["fenceProof"], &proof) != nil {
		return fmt.Errorf("private manual-terminal generation fence proof is missing")
	}
	fenceHash := sha256.Sum256(encoded(fence))
	if proof.OperationID != fence.OperationID || proof.SourceClusterID != fence.SourceClusterID || proof.BindingUID != fence.BindingUID ||
		proof.RequestHash != hex.EncodeToString(fenceHash[:]) || proof.Independent != nil || proof.IndependentV2 != nil {
		return fmt.Errorf("private manual-terminal fence proof is invalid or contains independent evidence")
	}
	var challenge operator.ManualRecoveryChallenge
	if err := json.Unmarshal(fields["challenge"], &challenge); err != nil {
		return err
	}
	var challengeDigest, approvalDigest string
	if err := json.Unmarshal(fields["challengeDigest"], &challengeDigest); err != nil {
		return err
	}
	calculatedChallenge, err := operator.ManualRecoveryChallengeDigest(challenge)
	if err != nil || calculatedChallenge != challengeDigest || challengeDigest != expectedChallengeDigest || challenge.TargetBaseKind != "fork" ||
		challenge.TargetClusterID == fence.SourceClusterID || challenge.RecoveryID != fence.OperationID || challenge.OperationID != fence.OperationID ||
		challenge.SourceClusterID != fence.SourceClusterID || challenge.StatefulSetUID != fence.StatefulSetUID ||
		challenge.FenceRequestHash != hex.EncodeToString(fenceHash[:]) {
		return fmt.Errorf("private manual-terminal challenge is invalid")
	}
	var approval operator.ManualRecoveryAttestationObject
	if err := json.Unmarshal(fields["independentApproval"], &approval); err != nil {
		return err
	}
	if err := json.Unmarshal(fields["approvalDigest"], &approvalDigest); err != nil {
		return err
	}
	approvalBytes, err := json.Marshal(approval)
	if err != nil {
		return err
	}
	approvalHash := sha256.Sum256(approvalBytes)
	if hex.EncodeToString(approvalHash[:]) != approvalDigest || operator.ValidateManualRecoveryAttestation(challenge, approval, challenge.ManualRecoveryBinding, approval.UID, time.Now().UTC()) != nil {
		return fmt.Errorf("private manual-terminal independent approval is invalid")
	}
	var unclaimed, quorum, noIndependentV2 bool
	if err := json.Unmarshal(fields["preApprovalTargetUnclaimed"], &unclaimed); err != nil || !unclaimed {
		return fmt.Errorf("private manual-terminal evidence does not prove target was unclaimed before approval")
	}
	if err := json.Unmarshal(fields["targetQuorumVerified"], &quorum); err != nil || !quorum {
		return fmt.Errorf("private manual-terminal recovery lacks target quorum readback")
	}
	if err := json.Unmarshal(fields["independentV2Absent"], &noIndependentV2); err != nil || !noIndependentV2 {
		return fmt.Errorf("private manual-terminal evidence is not an unproven-generation path")
	}
	var recovery operator.Resource
	if err := json.Unmarshal(fields["recovery"], &recovery); err != nil || recovery.Status.Phase != "Complete" || recovery.Status.Stage != "Complete" ||
		recovery.Status.Target != challenge.TargetClusterID || recovery.Status.TargetBaseKind != "fork" || recovery.Status.ManualRecoveryChallengeDigest != challengeDigest ||
		recovery.Status.ManualAttestationDigest != approvalDigest || recovery.Spec.RecoveryID != fence.OperationID ||
		recovery.Spec.SourceClusterID != fence.SourceClusterID || recovery.Spec.StatefulSet != fence.StatefulSet ||
		recovery.Spec.Durability != profile.SourceDurability || recovery.Spec.AllowDataLoss != profile.AllowDataLoss ||
		recovery.Spec.Fence.RecoveryID != fence.OperationID || recovery.Spec.Fence.ClusterID != fence.SourceClusterID ||
		recovery.Spec.Fence.StatefulSetUID != fence.StatefulSetUID || !recovery.Spec.Fence.Confirmed ||
		text(recovery.Metadata["name"]) != challenge.RecoveryName || text(recovery.Metadata["namespace"]) != challenge.RecoveryNamespace ||
		text(recovery.Metadata["uid"]) != challenge.RecoveryUID ||
		recovery.Status.Source != fence.SourceClusterID || recovery.Status.StatefulSetUID != fence.StatefulSetUID {
		return fmt.Errorf("private manual-terminal recovery differs from the exact approval")
	}
	var recoveredRows string
	if err := json.Unmarshal(fields["recoveredRows"], &recoveredRows); err != nil || recoveredRows != sourceAck.Rows {
		return fmt.Errorf("private manual-terminal readback differs from the acknowledged source row")
	}
	if err := json.Unmarshal(fields["targetAck"], &targetAck); err != nil || targetAck.RequestID != "gcs-ha-ack-2" || targetAck.Slot <= before.Tip || targetAck.Rows != `[[2,"gcs-manual-terminal-target"]]` {
		return fmt.Errorf("private manual-terminal target ACK/readback is invalid")
	}
	summary.Provider, summary.NoPVC = profile.Provider, profile.NoPVC
	summary.SourceDurability, summary.AllowDataLoss = profile.SourceDurability, profile.AllowDataLoss
	summary.SourceAcknowledged, summary.NonemptySourceSealed = true, true
	summary.IndependentV2Absent, summary.ChallengeObserved = true, true
	summary.IndependentApproval, summary.NoPreapprovalTarget = true, true
	summary.RecoveryCommitted, summary.TargetReadbackVerified = true, true
	return nil
}

func writeGCSManualTerminalSummary(public string, summary gcsManualTerminalPublicSummary) error {
	return os.WriteFile(filepath.Join(public, gcsManualTerminalSummaryName), encoded(summary), 0600)
}

func validateGCSAsyncNonemptyPrivateEvidence(data []byte, summary *gcsAsyncNonemptyPublicSummary) error {
	fields, err := exactJSONFields(data,
		"qualification", "profile", "sourceAcknowledgements", "sourceRows", "sourceHeadBeforeFence", "sealedSourceHead",
		"fence", "proof", "recovery", "recoveredRows", "recoveredTip", "targetQuorumVerified", "targetAck", "proofSource")
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
	if err := json.Unmarshal(fields["qualification"], &qualification); err != nil || qualification != "gcs-async-nonempty-tip-recovery" {
		return fmt.Errorf("private async-nonempty evidence has an unknown qualification")
	}
	if _, err := exactJSONFields(fields["profile"], "provider", "bucket", "noPVC", "sourceDurability", "allowDataLoss"); err != nil {
		return err
	}
	if err := json.Unmarshal(fields["profile"], &profile); err != nil || profile.Provider != "gcs" || profile.Bucket == "" || !profile.NoPVC || profile.SourceDurability != "async" || !profile.AllowDataLoss {
		return fmt.Errorf("private async-nonempty evidence profile is incomplete")
	}
	var sourceAckFields []json.RawMessage
	if err := json.Unmarshal(fields["sourceAcknowledgements"], &sourceAckFields); err != nil || len(sourceAckFields) != 3 {
		return fmt.Errorf("private async-nonempty source ACK receipts are incomplete")
	}
	rows := make([]string, 0, len(sourceAckFields))
	var lastAckSlot uint64
	for i, rawAck := range sourceAckFields {
		if _, err := exactJSONFields(rawAck, "requestID", "slot", "rows"); err != nil {
			return err
		}
		var ack gcsAcknowledgement
		if err := json.Unmarshal(rawAck, &ack); err != nil {
			return fmt.Errorf("private async-nonempty source ACK receipt is invalid")
		}
		row := fmt.Sprintf("[%d,\"gcs-async-archive-%d\"]", i+1, i+1)
		if ack.RequestID != fmt.Sprintf("gcs-ha-ack-%d", i+1) || ack.Slot == 0 || ack.Slot <= lastAckSlot || ack.Rows != "["+row+"]" {
			return fmt.Errorf("private async-nonempty source ACK receipt differs from the captured rows")
		}
		lastAckSlot = ack.Slot
		rows = append(rows, row)
	}
	expectedRows := "[" + strings.Join(rows, ",") + "]"
	var sourceRows, recoveredRows string
	if err := json.Unmarshal(fields["sourceRows"], &sourceRows); err != nil || sourceRows != expectedRows {
		return fmt.Errorf("private async-nonempty source row set is invalid")
	}
	if err := json.Unmarshal(fields["recoveredRows"], &recoveredRows); err != nil || recoveredRows != sourceRows {
		return fmt.Errorf("private async-nonempty recovered rows differ from source ACKs")
	}
	var before, sealed gcsArchiveHeadEvidence
	for name, head := range map[string]*gcsArchiveHeadEvidence{"sourceHeadBeforeFence": &before, "sealedSourceHead": &sealed} {
		if _, err := exactJSONFields(fields[name], "versionType", "version", "tip", "sealed", "headSHA256"); err != nil {
			return err
		}
		if err := json.Unmarshal(fields[name], head); err != nil {
			return fmt.Errorf("private async-nonempty source HEAD evidence is invalid")
		}
		if len(head.HeadSHA256) != 64 || strings.ToLower(head.HeadSHA256) != head.HeadSHA256 {
			return fmt.Errorf("private async-nonempty source HEAD digest is invalid")
		}
		if _, err := hex.DecodeString(head.HeadSHA256); err != nil {
			return fmt.Errorf("private async-nonempty source HEAD digest is invalid")
		}
		generation, err := strconv.ParseUint(head.Version, 10, 64)
		if err != nil || generation == 0 {
			return fmt.Errorf("private async-nonempty GCS generation is invalid")
		}
	}
	if before.Tip < lastAckSlot {
		return fmt.Errorf("private async-nonempty HEAD does not contain the acknowledged source rows")
	}
	if err := validateSealedNonemptyHead(before, sealed); err != nil {
		return err
	}
	var fence operator.FenceRequest
	var proof operator.FenceProof
	var recovery operator.Resource
	if err := json.Unmarshal(fields["fence"], &fence); err != nil {
		return fmt.Errorf("private async-nonempty generation fence is invalid")
	}
	if err := json.Unmarshal(fields["proof"], &proof); err != nil {
		return fmt.Errorf("private async-nonempty generation proof is invalid")
	}
	if proof.IndependentV2 == nil {
		return fmt.Errorf("private async-nonempty generation proof is not IndependentV2")
	}
	if _, _, err := verifyIndependentGenerationProof(obj{"spec": fence, "status": obj{"proof": proof}}, fence.SourceClusterID, fence.StatefulSetUID,
		fence.BindingUID, fence.Targets, profile.Bucket, proof.IndependentV2.StorageScope.Prefix, fence.Namespace, fence.StatefulSet, fence.LogicalID); err != nil {
		return fmt.Errorf("private async-nonempty generation proof is invalid: %w", err)
	}
	if err := json.Unmarshal(fields["recovery"], &recovery); err != nil || recovery.Spec.Durability != "async" || !recovery.Spec.AllowDataLoss ||
		recovery.Status.Phase != "Complete" || recovery.Status.Stage != "Complete" || recovery.Status.TargetBaseKind != "fork" ||
		recovery.Status.Source != fence.SourceClusterID || recovery.Status.StatefulSetUID != fence.StatefulSetUID || recovery.Status.Target == "" ||
		recovery.Status.SourceDurability != "async" || recovery.Status.RecoveredTip != before.Tip || recovery.Status.ManifestHash == "" {
		return fmt.Errorf("private async-nonempty recovery differs from the exact sealed archived tip")
	}
	if err := recoveryFenceMatchesProof(recovery, fence, proof); err != nil {
		return err
	}
	var recoveredTip uint64
	if err := json.Unmarshal(fields["recoveredTip"], &recoveredTip); err != nil || recoveredTip != sealed.Tip {
		return fmt.Errorf("private async-nonempty recovered tip differs from the sealed source")
	}
	var targetQuorum bool
	if err := json.Unmarshal(fields["targetQuorumVerified"], &targetQuorum); err != nil || !targetQuorum {
		return fmt.Errorf("private async-nonempty evidence does not prove target quorum")
	}
	var targetAck gcsAcknowledgement
	if _, err := exactJSONFields(fields["targetAck"], "requestID", "slot", "rows"); err != nil {
		return err
	}
	if err := json.Unmarshal(fields["targetAck"], &targetAck); err != nil || targetAck.RequestID != "gcs-ha-ack-4" || targetAck.Slot <= recoveredTip || targetAck.Rows != `[[4,"gcs-async-target-ack"]]` {
		return fmt.Errorf("private async-nonempty target ACK readback is incomplete")
	}
	var proofSource string
	if err := json.Unmarshal(fields["proofSource"], &proofSource); err != nil || proofSource != "RhizaFence status plus direct versioned GCS HEAD reads; recovered rows were read linearly from the promoted target" {
		return fmt.Errorf("private async-nonempty proof source is not recognized")
	}

	summary.Qualification = qualification
	summary.Provider = profile.Provider
	summary.NoPVC = profile.NoPVC
	summary.SourceDurability = profile.SourceDurability
	summary.AllowDataLoss = profile.AllowDataLoss
	summary.SourceAckCount = len(sourceAckFields)
	summary.LastSourceAckSlot = lastAckSlot
	summary.PersistedNonemptyTip = true
	summary.SealedNonemptyTip = true
	summary.RecoveredArchivedTip = recoveredTip
	summary.RecoveredRowsVerified = true
	summary.TargetQuorumVerified = targetQuorum
	summary.TargetAckReadbackVerified = true
	return nil
}

func writeGCSAsyncNonemptySummary(public string, summary gcsAsyncNonemptyPublicSummary) error {
	return os.WriteFile(filepath.Join(public, gcsAsyncNonemptySummaryName), encoded(summary), 0600)
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
	qualification := os.Getenv("FENCING_E2E_QUALIFICATION")
	if qualification != "" && qualification != "gcs-async-nonempty" && qualification != gcsManualTerminalQualification &&
		qualification != gcsMissingMembershipNegativeQualification && qualification != gcsMissingMembershipPositiveQualification &&
		qualification != gcsDeadPodQualification {
		return fmt.Errorf("artifact qualification selector is unsupported")
	}
	exitData, err := os.ReadFile(filepath.Join(raw, "actual-exit.txt"))
	if err != nil {
		return err
	}
	exitText := strings.TrimSpace(string(exitData))
	exitCode, err := strconv.ParseUint(exitText, 10, 8)
	if err != nil || strconv.FormatUint(exitCode, 10) != exitText {
		return fmt.Errorf("invalid public exit receipt")
	}
	failedRun := exitCode != 0
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
		if (qualification == "gcs-async-nonempty" || qualification == gcsManualTerminalQualification || qualification == gcsDeadPodQualification ||
			qualification == gcsMissingMembershipNegativeQualification || qualification == gcsMissingMembershipPositiveQualification) &&
			(name == "runtime-profile.json" || name == "runtime-component-probe.json") {
			// These receipts describe the disposable Kind component probe, not
			// the independently qualified GCS scenario.
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
			if name == "image-ids.txt" && (qualification == "gcs-async-nonempty" || qualification == gcsDeadPodQualification) && len(lines) != 2 {
				return fmt.Errorf("selected GCS qualification requires both immutable workload image references")
			}
			for i, line := range lines {
				if name == "actual-exit.txt" {
					continue
				}
				if name == "source-head.txt" {
					if len(line) != 40 || strings.ToLower(line) != line {
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
	asyncNonemptySeen, err := projectGCSAsyncNonemptyEvidence(raw, public, failedRun)
	if err != nil {
		return err
	}
	if asyncNonemptySeen && qualification != "gcs-async-nonempty" {
		return fmt.Errorf("async-nonempty evidence requires its explicit artifact qualification selector")
	}
	if qualification == "gcs-async-nonempty" && !failedRun && !asyncNonemptySeen {
		return fmt.Errorf("successful async-nonempty artifact set is missing its required scenario evidence")
	}
	manualTerminalSeen, err := projectGCSManualTerminalEvidence(raw, public, failedRun)
	if err != nil {
		return err
	}
	if manualTerminalSeen && qualification != gcsManualTerminalQualification {
		return fmt.Errorf("manual-terminal evidence requires its explicit artifact qualification selector")
	}
	if qualification == gcsManualTerminalQualification && !failedRun && !manualTerminalSeen {
		return fmt.Errorf("successful manual-terminal artifact set is missing its required scenario evidence")
	}
	missingMembershipSeen, err := projectGCSMissingMembershipEvidence(raw, public, qualification, failedRun)
	if err != nil {
		return err
	}
	if missingMembershipSeen && qualification != gcsMissingMembershipNegativeQualification && qualification != gcsMissingMembershipPositiveQualification {
		return fmt.Errorf("missing-membership evidence requires its explicit case selector")
	}
	if (qualification == gcsMissingMembershipNegativeQualification || qualification == gcsMissingMembershipPositiveQualification) && !failedRun && !missingMembershipSeen {
		return fmt.Errorf("successful missing-membership artifact set is missing its required case evidence")
	}
	deadPodSeen, err := projectGCSDeadPodEvidence(raw, public, qualification, failedRun)
	if err != nil {
		return err
	}
	if deadPodSeen && qualification != gcsDeadPodQualification {
		return fmt.Errorf("dead-Pod evidence requires its explicit artifact qualification selector")
	}
	if qualification == gcsDeadPodQualification && !failedRun && !deadPodSeen {
		return fmt.Errorf("successful dead-Pod artifact set is missing its required scenario evidence")
	}
	if privateData, readErr := os.ReadFile(filepath.Join(raw, "voter-startup-private.json")); readErr == nil {
		projected, projectErr := projectVoterStartupDiagnostics(raw, privateData)
		if projectErr != nil {
			return projectErr
		}
		if err := os.WriteFile(filepath.Join(public, "voter-startup-diagnostics.json"), projected, 0600); err != nil {
			return err
		}
	} else if !os.IsNotExist(readErr) {
		return fmt.Errorf("private voter diagnostics are unavailable")
	}
	privateOnlineDir := filepath.Join(raw, "private-online-voter-diagnostics")
	if info, statErr := os.Lstat(privateOnlineDir); statErr == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("private online voter diagnostics directory is unsafe")
		}
		privatePath := filepath.Join(privateOnlineDir, "online-voter-diagnostics.json")
		if info, statErr = os.Lstat(privatePath); statErr == nil {
			if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > 16<<20 {
				return fmt.Errorf("private online voter diagnostics are unsafe or oversized")
			}
			privateData, readErr := os.ReadFile(privatePath)
			if readErr != nil {
				return fmt.Errorf("private online voter diagnostics are unavailable")
			}
			projected, projectErr := projectOnlineVoterDiagnostics(raw, privateData)
			if projectErr != nil {
				return projectErr
			}
			if err := os.WriteFile(filepath.Join(public, "online-voter-diagnostics.json"), projected, 0600); err != nil {
				return err
			}
		} else if !os.IsNotExist(statErr) {
			return fmt.Errorf("private online voter diagnostics are unavailable")
		}
	} else if !os.IsNotExist(statErr) {
		return fmt.Errorf("private online voter diagnostics are unavailable")
	}
	ackLossSeen, err := projectGCSAckLossEvidence(raw, public, failedRun)
	if err != nil {
		return err
	}
	required := []string{"actual-exit.txt", "source-head.txt", "image-ids.txt"}
	if !failedRun {
		if qualification != "gcs-async-nonempty" && qualification != gcsManualTerminalQualification && qualification != gcsDeadPodQualification &&
			qualification != gcsMissingMembershipNegativeQualification && qualification != gcsMissingMembershipPositiveQualification {
			required = append(required, "runtime-profile.json", "runtime-component-probe.json")
		}
		if asyncEmptySeen {
			required = append(required, gcsAsyncEmptySummaryName)
		}
		if asyncNonemptySeen {
			required = append(required, gcsAsyncNonemptySummaryName)
		}
		if manualTerminalSeen {
			required = append(required, gcsManualTerminalSummaryName)
		}
		if missingMembershipSeen {
			required = append(required, gcsMissingMembershipSummaryName)
		}
		if deadPodSeen {
			required = append(required, gcsDeadPodSummaryName)
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
		if _, statErr := os.Stat(filepath.Join(public, "voter-startup-diagnostics.json")); statErr == nil {
			required = append(required, "voter-startup-diagnostics.json")
		}
	}
	for _, name := range required {
		if _, err := os.Stat(filepath.Join(public, name)); err != nil {
			return fmt.Errorf("required public receipt missing: %s", name)
		}
	}
	return nil
}
