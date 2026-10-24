package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

const gcsMixedRemoveGateSource = "RHIZA_QUALIFICATION_REMOVE_GATE_SOURCE"

const gcsStartupDenialMaxLogBytes = 1 << 20

var gcsSupervisorRunIDPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)
var errGCSStartupDenialPending = errors.New("startup denial event not observed yet")

type gcsStartupDenialEvent struct {
	Event              string `json:"event"`
	Action             string `json:"action"`
	Result             string `json:"result"`
	HTTPStatus         int    `json:"httpStatus"`
	Stage              string `json:"stage"`
	Namespace          string `json:"namespace,omitempty"`
	StatefulSetUID     string `json:"statefulSetUID,omitempty"`
	PodUID             string `json:"podUID,omitempty"`
	SourceClusterID    string `json:"sourceClusterID,omitempty"`
	AttemptedRunID     string `json:"attemptedRunID,omitempty"`
	RequestedNodeID    string `json:"requestedNodeID,omitempty"`
	GrantResolved      bool   `json:"grantResolved"`
	ResolvedNodeID     string `json:"resolvedNodeID,omitempty"`
	AssignmentID       string `json:"assignmentID,omitempty"`
	RuntimeContainerID string `json:"runtimeContainerID,omitempty"`
	OmittedReason      string `json:"omittedReason,omitempty"`
}

type gcsStartupDenialExpectation struct {
	Namespace, StatefulSetUID, PodUID, SourceClusterID string
	RequestedNodeID, OldRunID                          string
	CanonicalRunIDs                                    []string
}

type gcsMixedProofWaitResult struct {
	Request  operator.FenceRequest
	Proof    operator.FenceProof
	Observed any
	Err      error
}

type gcsMixedDenialWaitResult struct {
	Log                   []byte
	ErrClosedRow          string
	TerminatedContainerID string
	Event                 gcsStartupDenialEvent
	PostflightLogs        []byte
	Err                   error
}

type gcsAuthorityLogSource struct {
	PodName, PodUID, DeploymentUID string
	ContainerName, ContainerID     string
	Image, ImageID                 string
	RestartCount                   int
}

func captureGCSAuthorityLogSource(ctx context.Context, source gcsRecoverySource) (gcsAuthorityLogSource, error) {
	var result gcsAuthorityLogSource
	deployment, err := get(ctx, "deployment", "rhiza-fencer")
	if err != nil {
		return result, fmt.Errorf("read authority Deployment identity")
	}
	deploymentUID := text(nested(deployment, "metadata", "uid"))
	if deploymentUID == "" || text(nested(deployment, "metadata", "namespace")) != source.Namespace || nested(deployment, "spec", "replicas") != float64(1) {
		return result, fmt.Errorf("authority Deployment identity is not uniquely pinned")
	}
	pod, err := get(ctx, "pod", source.FencerPod)
	if err != nil || !ackLossPodOwnedBy(ctx, pod, deploymentUID) {
		return result, fmt.Errorf("authority log Pod is not owned by the pinned Deployment")
	}
	image := os.Getenv("RHIZA_OPERATOR_IMAGE")
	if _, err := immutableDigest(image); err != nil {
		return result, fmt.Errorf("authority image is not digest pinned")
	}
	if _, err := verifyGCSPodImage(pod, "authority", image); err != nil {
		return result, fmt.Errorf("authority log Pod image is not pinned: %w", err)
	}
	status := namedContainerStatus(pod, "authority")
	containerID, imageID := text(nested(status, "containerID")), text(nested(status, "imageID"))
	if status == nil || nested(status, "ready") != true || containerID == "" || imageID == "" {
		return result, fmt.Errorf("authority log container is not ready and uniquely identifiable")
	}
	return gcsAuthorityLogSource{PodName: source.FencerPod, PodUID: text(nested(pod, "metadata", "uid")), DeploymentUID: deploymentUID,
		ContainerName: "authority", ContainerID: containerID, Image: image, ImageID: imageID,
		RestartCount: int(numberOrZero(nested(status, "restartCount")))}, nil
}

func readGCSAuthorityLogs(ctx context.Context, source gcsRecoverySource, pinned gcsAuthorityLogSource) ([]byte, error) {
	current, err := captureGCSAuthorityLogSource(ctx, source)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, fmt.Errorf("authority log source capture canceled: %w", contextErr)
		}
		return nil, fmt.Errorf("authority log source capture failed: %w", err)
	}
	if current != pinned {
		return nil, fmt.Errorf("authority log source identity changed")
	}
	logs, err := command(ctx, "kubectl", "--context", kubeContext, "-n", source.Namespace, "logs", pinned.PodName,
		"-c", pinned.ContainerName, "--tail=256", "--limit-bytes=1048576")
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, fmt.Errorf("authority log read canceled: %w", contextErr)
		}
		return nil, fmt.Errorf("authority log read failed: %w", err)
	}
	if len(logs) > gcsStartupDenialMaxLogBytes {
		return nil, fmt.Errorf("authority log read exceeded its bound")
	}
	return logs, nil
}

func persistGCSStartupDenialEvidence(out, row, learnerErrClosedRow string, event gcsStartupDenialEvent, source gcsAuthorityLogSource, learnerRuntimeID string,
	rootBefore, rootAfter fenceLedgerState, beforeVersion, afterVersion *objstore.ObjectVersion) error {
	dir := filepath.Join(out, "private-startup-denial")
	if err := os.Mkdir(dir, 0700); err != nil {
		return fmt.Errorf("create private startup denial evidence directory: %w", err)
	}
	rowPath := filepath.Join(dir, "authority-denial-row.jsonl")
	if err := writeGCSPrivateEvidence(rowPath, []byte(row+"\n")); err != nil {
		return err
	}
	if err := writeGCSPrivateEvidence(filepath.Join(dir, "learner-errclosed-row.log"), []byte(learnerErrClosedRow+"\n")); err != nil {
		return err
	}
	if beforeVersion == nil || afterVersion == nil || beforeVersion.Value == "" || afterVersion.Value == "" {
		return fmt.Errorf("canonical root readback omitted version evidence")
	}
	beforeBytes, afterBytes := encoded(rootBefore), encoded(rootAfter)
	beforeDigest, afterDigest := sha256.Sum256(beforeBytes), sha256.Sum256(afterBytes)
	projection, err := json.MarshalIndent(obj{"event": event, "authority": source, "learnerTerminatedContainerID": learnerRuntimeID,
		"rootBefore": obj{"statefulSetUID": rootBefore.StatefulSetUID, "sourceClusterID": rootBefore.Ledger.SourceClusterID,
			"versionType": beforeVersion.Type, "version": beforeVersion.Value, "digest": hex.EncodeToString(beforeDigest[:])},
		"rootAfter": obj{"statefulSetUID": rootAfter.StatefulSetUID, "sourceClusterID": rootAfter.Ledger.SourceClusterID,
			"versionType": afterVersion.Type, "version": afterVersion.Value, "digest": hex.EncodeToString(afterDigest[:])}}, "", "  ")
	if err != nil {
		return err
	}
	return writeGCSPrivateEvidence(filepath.Join(dir, "correlation-tuple.json"), append(projection, '\n'))
}

func writeGCSPrivateEvidence(path string, content []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("create private startup denial evidence: %w", err)
	}
	if _, err := file.Write(content); err != nil {
		_ = file.Close()
		return fmt.Errorf("write private startup denial evidence")
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync private startup denial evidence")
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close private startup denial evidence")
	}
	readback, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(readback, content) {
		return fmt.Errorf("private startup denial evidence readback mismatch")
	}
	return nil
}

func selectGCSClientErrClosedRow(logs []byte) (string, error) {
	if len(logs) == 0 || len(logs) > gcsStartupDenialMaxLogBytes {
		return "", fmt.Errorf("previous learner logs are empty or exceed their bound")
	}
	var selected string
	for _, raw := range bytes.Split(logs, []byte{'\n'}) {
		if !bytes.Contains(raw, []byte(fencing.ErrClosed.Error())) {
			continue
		}
		line := strings.TrimSuffix(string(raw), "\r")
		if len(line) > 4096 || len(line) < 20 || selected != "" {
			return "", fmt.Errorf("previous learner logs contain an oversized or ambiguous ErrClosed row")
		}
		if _, err := time.Parse("2006/01/02 15:04:05 ", line[:20]); err != nil || strings.TrimSpace(line[20:]) != fencing.ErrClosed.Error() {
			return "", fmt.Errorf("ErrClosed was not emitted as the exact supervisor client result")
		}
		selected = line
	}
	if selected == "" {
		return "", fmt.Errorf("previous learner logs contain no exact ErrClosed observation")
	}
	return selected, nil
}

type gcsMixedPendingTargets struct {
	OriginalVoters               []operator.FenceTarget
	RemovedVoter                 operator.FenceTarget
	RetainedRemovedVoterTerminal fencing.TerminalReference
	CurrentSurvivors             []operator.FenceTarget
	PendingLearner               operator.FenceTarget
	PendingLearnerEvidence       operator.FenceTarget
	PendingLearnerIdentity       fencing.Identity
	ExpectedGenerationTargets    []operator.FenceTarget
}

type gcsMixedVoterRun struct {
	Target   operator.FenceTarget
	Identity fencing.Identity
	Runtime  fencing.RuntimeBinding
	Producer string
}

type gcsMixedGateSnapshot struct {
	Enabled             bool     `json:"enabled"`
	SourceClusterID     string   `json:"source_cluster_id"`
	CurrentClusterID    string   `json:"current_cluster_id"`
	LocalNodeID         string   `json:"local_node_id"`
	ConfiguredVoters    []string `json:"configured_voters"`
	SuccessfulRemoves   uint64   `json:"successful_removes"`
	BlockedSuccesses    uint64   `json:"blocked_successes"`
	LastRequestSHA256   string   `json:"last_request_sha256"`
	LastResponseSHA256  string   `json:"last_response_sha256"`
	LastHandlerStatus   int      `json:"last_handler_status"`
	UnexpectedAdditions uint64   `json:"unexpected_additions"`
	CaptureFailures     uint64   `json:"capture_failures"`
	IdentityViolations  uint64   `json:"identity_violations"`
}

type gcsMixedPendingCheckpoint struct {
	Recovery              operator.Resource
	RecoveryUID           string
	RemoveRequest         network.MembershipChange
	Learner               operator.FenceTarget
	LearnerID             fencing.Identity
	LearnerRun            fencing.Run
	LearnerRequestTarget  operator.FenceTarget
	LearnerControlVersion objstore.ObjectVersion
	LearnerControlDigest  string
	InitialControlIntent  gcsMixedAutoIntentProjection
	CredentialUID         string
	CredentialRV          string
	CredentialHash        string
	SurvivorStatus        []network.MembershipStatus
	Gate                  []gcsMixedGateSnapshot
}

const gcsMixedAutoControlMaxBytes = 1 << 20

type gcsMixedAutoControlReader interface {
	Attributes(context.Context, string) (objstore.ObjectAttributes, error)
	Get(context.Context, string) (io.ReadCloser, error)
}

type gcsMixedAutoIntentProjection struct {
	Supersedes          string                `json:"supersedes,omitempty"`
	LearnerUID          string                `json:"learnerUID,omitempty"`
	LearnerWAL          string                `json:"learnerWAL,omitempty"`
	LearnerMissingSince time.Time             `json:"learnerMissingSince"`
	LearnerAttempt      int                   `json:"learnerAttempt"`
	Aborting            bool                  `json:"aborting"`
	RetirementSince     time.Time             `json:"retirementSince,omitempty"`
	PreviousSpecHash    string                `json:"previousSpecHash,omitempty"`
	NoQuorumSince       time.Time             `json:"noQuorumSince"`
	Request             operator.FenceRequest `json:"request"`
	Policy              operator.AutoPolicy   `json:"policy"`
	Durability          string                `json:"durability"`
	Voters              []operator.VoterPod   `json:"voters"`
	Failed              string                `json:"failed,omitempty"`
	Proof               *operator.FenceProof  `json:"proof,omitempty"`
	RecoveryName        string                `json:"recoveryName"`
	RecoveryUID         string                `json:"recoveryUID,omitempty"`
	SpecHash            string                `json:"specHash,omitempty"`
	LearnerName         string                `json:"learnerName,omitempty"`
	LearnerPodName      string                `json:"learnerPodName,omitempty"`
	LearnerSecretName   string                `json:"learnerSecretName,omitempty"`
	LearnerSecretUID    string                `json:"learnerSecretUID,omitempty"`
	LearnerSecretRV     string                `json:"learnerSecretResourceVersion,omitempty"`
	LearnerSecretHash   string                `json:"learnerSecretHash,omitempty"`
	LearnerAssignmentID string                `json:"learnerAssignmentID,omitempty"`
}

type gcsMixedAutoControlProjection struct {
	Version           int                                `json:"version"`
	BindingUID        string                             `json:"bindingUID"`
	BindingHash       string                             `json:"bindingHash"`
	BindingVersion    int                                `json:"bindingVersion,omitempty"`
	LegacyBindingHash string                             `json:"legacyBindingHash,omitempty"`
	StatefulSetUID    string                             `json:"statefulSetUID"`
	ActiveClusterID   string                             `json:"activeClusterID"`
	Voters            []operator.VoterPod                `json:"voters"`
	Identities        map[string]operator.FenceTarget    `json:"identities"`
	SuspectedSince    time.Time                          `json:"suspectedSince"`
	Completions       []time.Time                        `json:"completions,omitempty"`
	Sequence          uint64                             `json:"sequence"`
	Intent            *gcsMixedAutoIntentProjection      `json:"intent,omitempty"`
	BootstrapGenesis  json.RawMessage                    `json:"bootstrapGenesis,omitempty"`
	Completed         json.RawMessage                    `json:"completed,omitempty"`
	Learners          []gcsMixedLearnerHistoryProjection `json:"learners,omitempty"`
}

type gcsMixedLearnerHistoryProjection struct {
	SourceClusterID string               `json:"sourceClusterID"`
	OperationID     string               `json:"operationID"`
	CredentialName  string               `json:"credentialName,omitempty"`
	CredentialUID   string               `json:"credentialUID"`
	Target          operator.FenceTarget `json:"target"`
	Retirement      json.RawMessage      `json:"retirement,omitempty"`
}

type gcsMixedAutoControlExpectation struct {
	Source       gcsRecoverySource
	Checkpoint   gcsMixedPendingCheckpoint
	FenceRequest operator.FenceRequest
}

func validateGCSMixedAutoControl(data []byte, expected gcsMixedAutoControlExpectation) (string, error) {
	if len(data) == 0 || len(data) > gcsMixedAutoControlMaxBytes {
		return "", fmt.Errorf("automatic control snapshot is empty or oversized")
	}
	var control gcsMixedAutoControlProjection
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&control); err != nil {
		return "", fmt.Errorf("decode bounded automatic control projection: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return "", fmt.Errorf("automatic control snapshot has trailing data")
	}
	i, c, s := control.Intent, expected.Checkpoint, expected.Source
	if c.Recovery.Spec.Membership == nil {
		return "", fmt.Errorf("pending recovery has no membership replacement spec")
	}
	if control.Version != 1 || control.BindingUID != s.BindingUID || control.StatefulSetUID != s.StatefulSetUID ||
		control.ActiveClusterID != s.ClusterID || i == nil || i.Aborting || digestJSON(i.Request) != digestJSON(expected.FenceRequest) ||
		i.RecoveryName != text(c.Recovery.Metadata["name"]) || i.RecoveryUID != c.RecoveryUID ||
		i.SpecHash != digestJSON(c.Recovery.Spec) || i.LearnerName != c.LearnerID.NodeID ||
		i.LearnerPodName != c.Learner.Pod || i.LearnerUID != c.Learner.PodUID ||
		i.LearnerSecretName != c.Recovery.Spec.Membership.ReplacementSecret ||
		i.LearnerSecretUID != c.CredentialUID || i.LearnerSecretRV != c.CredentialRV ||
		i.LearnerSecretHash != c.CredentialHash || i.LearnerAssignmentID != c.LearnerID.AssignmentID {
		return "", fmt.Errorf("automatic control no longer matches the exact non-aborting pending learner intent")
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func readGCSMixedAutoControl(ctx context.Context, bucket gcsMixedAutoControlReader, key string, expected gcsMixedAutoControlExpectation) (objstore.ObjectVersion, string, error) {
	version, data, err := readGCSMixedAutoControlBytes(ctx, bucket, key)
	if err != nil {
		return objstore.ObjectVersion{}, "", err
	}
	digest, err := validateGCSMixedAutoControl(data, expected)
	if err != nil {
		return objstore.ObjectVersion{}, "", err
	}
	return version, digest, nil
}

func readGCSMixedAutoControlBytes(ctx context.Context, bucket gcsMixedAutoControlReader, key string) (objstore.ObjectVersion, []byte, error) {
	before, err := bucket.Attributes(ctx, key)
	if err != nil {
		return objstore.ObjectVersion{}, nil, fmt.Errorf("read automatic control version before snapshot: %w", err)
	}
	if before.Version == nil || before.Version.Value == "" {
		return objstore.ObjectVersion{}, nil, fmt.Errorf("automatic control requires a versioned object before snapshot")
	}
	r, err := bucket.Get(ctx, key)
	if err != nil {
		return objstore.ObjectVersion{}, nil, fmt.Errorf("read automatic control snapshot: %w", err)
	}
	data, readErr := io.ReadAll(io.LimitReader(r, gcsMixedAutoControlMaxBytes+1))
	closeErr := r.Close()
	if readErr != nil || closeErr != nil || len(data) > gcsMixedAutoControlMaxBytes {
		return objstore.ObjectVersion{}, nil, fmt.Errorf("automatic control snapshot read/close failed or exceeded limit")
	}
	after, err := bucket.Attributes(ctx, key)
	if err != nil {
		return objstore.ObjectVersion{}, nil, fmt.Errorf("read automatic control version after snapshot: %w", err)
	}
	if after.Version == nil || after.Version.Value == "" || *before.Version != *after.Version {
		return objstore.ObjectVersion{}, nil, fmt.Errorf("automatic control changed during version-pinned read")
	}
	return *after.Version, data, nil
}

func verifyGCSMixedEscalatedControl(ctx context.Context, bucket gcsMixedAutoControlReader, controlKey, basePrefix string,
	source gcsRecoverySource, checkpoint gcsMixedPendingCheckpoint, initial gcsMixedAutoIntentProjection,
	oldRequest, generationRequest operator.FenceRequest) (objstore.ObjectVersion, string, string, string, string, error) {
	var zero objstore.ObjectVersion
	version, currentBytes, err := readGCSMixedAutoControlBytes(ctx, bucket, controlKey)
	if err != nil {
		return zero, "", "", "", "", err
	}
	var current gcsMixedAutoControlProjection
	if err := decodeGCSMixedAutoControl(currentBytes, &current); err != nil {
		return zero, "", "", "", "", err
	}
	currentDigest := sha256.Sum256(currentBytes)
	intent := current.Intent
	if current.Version != 1 || current.BindingUID != source.BindingUID || current.StatefulSetUID != source.StatefulSetUID ||
		current.ActiveClusterID != source.ClusterID || intent == nil || intent.Aborting || intent.Supersedes == "" ||
		!sameFenceRequest(intent.Request, generationRequest) || generationRequest.Scope != operator.ScopeGeneration ||
		intent.RecoveryName != generationRequest.OperationID || intent.RecoveryUID != "" || intent.SpecHash != "" ||
		intent.Failed != "" || intent.Proof != nil || intent.LearnerName != "" || intent.LearnerPodName != "" ||
		intent.LearnerUID != "" || intent.LearnerSecretName != "" || intent.LearnerSecretUID != "" ||
		intent.LearnerSecretRV != "" || intent.LearnerSecretHash != "" || intent.LearnerAssignmentID != "" ||
		intent.Policy != initial.Policy || intent.Durability != initial.Durability || !reflect.DeepEqual(intent.Voters, initial.Voters) {
		return zero, "", "", "", "", fmt.Errorf("current automatic CONTROL is not the exact superseding generation intent")
	}
	if initial.Aborting || initial.Request.Scope != operator.ScopeVoter || initial.RecoveryName == "" ||
		initial.RecoveryUID != checkpoint.RecoveryUID || initial.SpecHash != digestJSON(checkpoint.Recovery.Spec) ||
		initial.LearnerName != checkpoint.LearnerID.NodeID || initial.LearnerPodName != checkpoint.Learner.Pod ||
		initial.LearnerUID != checkpoint.Learner.PodUID || initial.LearnerSecretUID != checkpoint.CredentialUID ||
		initial.LearnerSecretRV != checkpoint.CredentialRV || initial.LearnerSecretHash != checkpoint.CredentialHash ||
		initial.LearnerAssignmentID != checkpoint.LearnerID.AssignmentID ||
		!sameFenceRequest(initial.Request, oldRequest) {
		return zero, "", "", "", "", fmt.Errorf("captured old automatic intent does not match the exact learner/REMOVE checkpoint")
	}
	historyMatches := 0
	for _, entry := range current.Learners {
		if entry.SourceClusterID != source.ClusterID ||
			(entry.Target.NodeID != checkpoint.LearnerID.NodeID && entry.Target.PodUID != checkpoint.Learner.PodUID) {
			continue
		}
		historyMatches++
		if entry.OperationID != oldRequest.OperationID || entry.CredentialUID != checkpoint.CredentialUID ||
			(entry.CredentialName != "" && entry.CredentialName != checkpoint.Recovery.Spec.Membership.ReplacementSecret) ||
			entry.Target != checkpoint.LearnerRequestTarget || len(entry.Retirement) != 0 {
			return zero, "", "", "", "", fmt.Errorf("generation CONTROL changed the exact historical learner request/credential identity")
		}
	}
	if historyMatches != 1 {
		return zero, "", "", "", "", fmt.Errorf("generation CONTROL omitted or duplicated the exact historical learner incarnation")
	}
	oldNamePrefix := initial.RecoveryName + "-"
	if !strings.HasPrefix(intent.Supersedes, oldNamePrefix) || strings.Contains(strings.TrimPrefix(intent.Supersedes, oldNamePrefix), "/") {
		return zero, "", "", "", "", fmt.Errorf("generation Supersedes does not identify the exact prior recovery intent")
	}
	oldKey := path.Join(basePrefix, "automatic", source.Namespace, source.LogicalID, "superseded", intent.Supersedes+".json")
	oldVersion, oldBytes, err := readGCSMixedAutoControlBytes(ctx, bucket, oldKey)
	if err != nil {
		return zero, "", "", "", "", fmt.Errorf("read immutable prior automatic intent: %w", err)
	}
	oldHash := sha256.Sum256(oldBytes)
	if hex.EncodeToString(oldHash[:]) != strings.TrimPrefix(intent.Supersedes, oldNamePrefix) {
		return zero, "", "", "", "", fmt.Errorf("immutable Supersedes object bytes do not match the production content-addressed name")
	}
	var old gcsMixedAutoIntentProjection
	if err := decodeGCSMixedAutoIntent(oldBytes, &old); err != nil {
		return zero, "", "", "", "", err
	}
	initialComparable, oldComparable := initial, old
	initialComparable.NoQuorumSince = time.Time{}
	oldComparable.NoQuorumSince = time.Time{}
	if old.NoQuorumSince.IsZero() || old.Aborting || old.Supersedes != "" ||
		!reflect.DeepEqual(oldComparable, initialComparable) || old.NoQuorumSince.Before(initial.NoQuorumSince) {
		return zero, "", "", "", "", fmt.Errorf("immutable superseded intent changed historical identity/workflow beyond NoQuorumSince progression")
	}
	return version, hex.EncodeToString(currentDigest[:]), oldVersion.Value, intent.Supersedes, hex.EncodeToString(oldHash[:]), nil
}

func decodeGCSMixedAutoControl(data []byte, control *gcsMixedAutoControlProjection) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(control); err != nil {
		return fmt.Errorf("decode bounded automatic CONTROL: %w", err)
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return fmt.Errorf("automatic CONTROL has trailing JSON data")
	}
	return nil
}

func decodeGCSMixedAutoIntent(data []byte, intent *gcsMixedAutoIntentProjection) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(intent); err != nil {
		return fmt.Errorf("decode complete immutable superseded intent: %w", err)
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return fmt.Errorf("immutable superseded intent has trailing JSON data")
	}
	return nil
}

// waitForGCSMixedProofAndDenial joins the two independent qualification
// observations. A failed observation cancels its sibling and still joins it,
// so the caller never proceeds to cleanup while an owned waiter is running.
func waitForGCSMixedProofAndDenial(ctx context.Context,
	proofWait func(context.Context) gcsMixedProofWaitResult,
	denialWait func(context.Context) gcsMixedDenialWaitResult) (gcsMixedProofWaitResult, gcsMixedDenialWaitResult, error) {
	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	proofResults := make(chan gcsMixedProofWaitResult, 1)
	denialResults := make(chan gcsMixedDenialWaitResult, 1)
	go func() { proofResults <- proofWait(waitCtx) }()
	go func() { denialResults <- denialWait(waitCtx) }()

	var proof gcsMixedProofWaitResult
	var denial gcsMixedDenialWaitResult
	gotProof, gotDenial := false, false
	var firstErr error
	for !gotProof || !gotDenial {
		select {
		case result := <-proofResults:
			proof, gotProof = result, true
			if result.Err != nil && firstErr == nil {
				firstErr = fmt.Errorf("canonical mixed generation proof: %w", result.Err)
				cancel()
			}
		case result := <-denialResults:
			denial, gotDenial = result, true
			if result.Err != nil && firstErr == nil {
				firstErr = fmt.Errorf("natural learner startup denial: %w", result.Err)
				cancel()
			}
		}
	}
	return proof, denial, firstErr
}

func closeGCSMixedEventFile(file *os.File) error {
	if file == nil {
		return fmt.Errorf("mixed qualification event file is unavailable")
	}
	var closeErrors []error
	if err := file.Sync(); err != nil {
		closeErrors = append(closeErrors, fmt.Errorf("sync mixed qualification event ledger: %w", err))
	}
	if err := file.Close(); err != nil {
		closeErrors = append(closeErrors, fmt.Errorf("close mixed qualification event ledger: %w", err))
	}
	return errors.Join(closeErrors...)
}

func gcsMixedOperatorControlLocation(ctx context.Context, source gcsRecoverySource) (string, string, error) {
	deployment, err := get(ctx, "deployment", "rhiza-operator")
	if err != nil {
		return "", "", fmt.Errorf("read operator Deployment for learner-history projection")
	}
	container, err := boundContainer(deployment, "operator")
	if err != nil {
		return "", "", err
	}
	bucket, err := effectiveGCSSetting(ctx, container, "RHIZA_OBJSTORE_BUCKET")
	if err != nil || bucket != source.Bucket {
		return "", "", fmt.Errorf("operator control bucket differs from captured source")
	}
	basePrefix, err := effectiveGCSSetting(ctx, container, "RHIZA_OBJSTORE_PREFIX")
	if err != nil || strings.Trim(basePrefix, "/") != basePrefix || path.Clean(basePrefix) != basePrefix || path.Join(basePrefix, source.ClusterID) != source.Prefix {
		return "", "", fmt.Errorf("operator control prefix is not bound to captured source")
	}
	return path.Join(basePrefix, "automatic", source.Namespace, source.LogicalID, "control.json"), basePrefix, nil
}

func readGCSMixedLearnerRequestTarget(ctx context.Context, bucket gcsMixedAutoControlReader, key string, expected gcsMixedAutoControlExpectation,
	runtime fencing.RuntimeBinding) (operator.FenceTarget, objstore.ObjectVersion, string, error) {
	var zero operator.FenceTarget
	before, err := bucket.Attributes(ctx, key)
	if err != nil || before.Version == nil || before.Version.Value == "" {
		return zero, objstore.ObjectVersion{}, "", fmt.Errorf("learner history requires a versioned canonical control snapshot")
	}
	r, err := bucket.Get(ctx, key)
	if err != nil {
		return zero, objstore.ObjectVersion{}, "", fmt.Errorf("read canonical learner history")
	}
	data, readErr := io.ReadAll(io.LimitReader(r, gcsMixedAutoControlMaxBytes+1))
	closeErr := r.Close()
	if readErr != nil || closeErr != nil || len(data) == 0 || len(data) > gcsMixedAutoControlMaxBytes {
		return zero, objstore.ObjectVersion{}, "", fmt.Errorf("canonical learner history read/close failed or exceeded its bound")
	}
	after, err := bucket.Attributes(ctx, key)
	if err != nil || after.Version == nil || after.Version.Value == "" || *before.Version != *after.Version {
		return zero, objstore.ObjectVersion{}, "", fmt.Errorf("canonical learner history changed during version-pinned read")
	}
	if _, err := validateGCSMixedAutoControl(data, expected); err != nil {
		return zero, objstore.ObjectVersion{}, "", err
	}
	var control gcsMixedAutoControlProjection
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&control); err != nil {
		return zero, objstore.ObjectVersion{}, "", fmt.Errorf("decode canonical learner history")
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return zero, objstore.ObjectVersion{}, "", fmt.Errorf("canonical learner history has trailing data")
	}
	checkpoint := expected.Checkpoint
	evidence := checkpoint.Learner
	if runtime.Validate(checkpoint.LearnerID) != nil || evidence.NodeID != checkpoint.LearnerID.NodeID || evidence.Pod != checkpoint.LearnerID.PodName ||
		evidence.PodUID != checkpoint.LearnerID.PodUID || evidence.WALIdentity != checkpoint.LearnerID.WALIdentity ||
		evidence.NodeName != runtime.NodeName || evidence.NodeUID != runtime.NodeUID || evidence.ContainerID != runtime.ContainerID || evidence.ImageID != runtime.ImageID {
		return zero, objstore.ObjectVersion{}, "", fmt.Errorf("canonical learner runtime binding is invalid")
	}
	var matched *gcsMixedLearnerHistoryProjection
	for index := range control.Learners {
		entry := &control.Learners[index]
		if entry.SourceClusterID != expected.Source.ClusterID ||
			(entry.Target.NodeID != checkpoint.LearnerID.NodeID && entry.Target.PodUID != checkpoint.LearnerID.PodUID) {
			continue
		}
		if matched != nil {
			return zero, objstore.ObjectVersion{}, "", fmt.Errorf("canonical learner history has duplicate identity entries")
		}
		matched = entry
	}
	if matched == nil || matched.OperationID != expected.FenceRequest.OperationID || matched.CredentialUID != checkpoint.CredentialUID ||
		(matched.CredentialName != "" && matched.CredentialName != checkpoint.Recovery.Spec.Membership.ReplacementSecret) {
		return zero, objstore.ObjectVersion{}, "", fmt.Errorf("canonical learner history does not match the exact pending learner assignment")
	}
	projection, identity := matched.Target, checkpoint.LearnerID
	if projection.NodeID != identity.NodeID || projection.Pod != identity.PodName || projection.PodUID != identity.PodUID || projection.WALIdentity != identity.WALIdentity ||
		(projection.NodeName != "" && projection.NodeName != runtime.NodeName) ||
		(projection.NodeUID != "" && projection.NodeUID != runtime.NodeUID) ||
		(projection.ContainerID != "" && projection.ContainerID != runtime.ContainerID) ||
		(projection.ImageID != "" && projection.ImageID != runtime.ImageID) {
		return zero, objstore.ObjectVersion{}, "", fmt.Errorf("canonical request target conflicts with learner identity or runtime evidence")
	}
	digest := sha256.Sum256(data)
	return projection, *after.Version, hex.EncodeToString(digest[:]), nil
}

// parseGCSStartupDenialDelta accepts exactly one new, bounded authority event.
// The preflight bytes must remain an exact prefix of the postflight stream;
// rotation/truncation is ambiguous and therefore fails closed.
func parseGCSStartupDenialDelta(before, after []byte, expected gcsStartupDenialExpectation) (gcsStartupDenialEvent, string, error) {
	var zero gcsStartupDenialEvent
	if len(before) > gcsStartupDenialMaxLogBytes || len(after) > gcsStartupDenialMaxLogBytes || len(after) < len(before) ||
		!bytes.HasPrefix(after, before) {
		return zero, "", fmt.Errorf("authority log preflight is missing or postflight log rotated/truncated")
	}
	delta := after[len(before):]
	if len(delta) == 0 {
		return zero, "", errGCSStartupDenialPending
	}
	if delta[len(delta)-1] != '\n' {
		return zero, "", fmt.Errorf("authority log delta is incomplete")
	}
	var selectedEvent gcsStartupDenialEvent
	var selected string
	for _, raw := range bytes.Split(delta, []byte{'\n'}) {
		if len(raw) == 0 {
			continue
		}
		if len(raw) > 4116 {
			return zero, "", fmt.Errorf("authority log row exceeds bounded parser limit")
		}
		line := string(raw)
		marker := strings.Index(line, `"event":"startup_registration_denial"`)
		if marker < 0 {
			continue
		}
		start := strings.Index(line, "{")
		if start < 0 || marker < start {
			return zero, "", fmt.Errorf("denial marker is not in a structured log row")
		}
		prefix := line[:start]
		if _, err := time.Parse("2006/01/02 15:04:05 ", prefix); err != nil {
			return zero, "", fmt.Errorf("denial row lacks the expected timestamped logger prefix")
		}
		payload := []byte(line[start:])
		if len(payload)+1 > 4096 {
			return zero, "", fmt.Errorf("denial event exceeds production line bound")
		}
		var found gcsStartupDenialEvent
		decoder := json.NewDecoder(bytes.NewReader(payload))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&found); err != nil {
			return zero, "", fmt.Errorf("decode denial event: %w", err)
		}
		var extra any
		if err := decoder.Decode(&extra); err != io.EOF {
			return zero, "", fmt.Errorf("denial event has trailing JSON data")
		}
		canonical, err := json.Marshal(found)
		if err != nil || !bytes.Equal(canonical, payload) {
			return zero, "", fmt.Errorf("denial event is not the canonical typed encoding")
		}
		// Other voter Pods can emit ordinary closed-generation denials in the
		// same authority log interval. Parse them strictly, then scope the
		// candidate count to the exact learner incarnation.
		if found.PodUID == "" {
			return zero, "", fmt.Errorf("denial event omitted PodUID before candidate filtering")
		}
		if found.PodUID != expected.PodUID {
			continue
		}
		if selected != "" {
			return zero, "", fmt.Errorf("authority log delta contains multiple denial events for the learner PodUID")
		}
		selected = line
		selectedEvent = found
	}
	if selected == "" {
		return zero, "", errGCSStartupDenialPending
	}
	if err := validateGCSStartupDenial(selectedEvent, expected); err != nil {
		return zero, "", err
	}
	return selectedEvent, selected, nil
}

func validateGCSStartupDenial(event gcsStartupDenialEvent, expected gcsStartupDenialExpectation) error {
	for _, value := range []string{event.Namespace, event.StatefulSetUID, event.PodUID, event.SourceClusterID,
		event.AttemptedRunID, event.RequestedNodeID} {
		if value == "" || len(value) > 256 || strings.ContainsAny(value, "\r\n") {
			return fmt.Errorf("startup denial identity is empty, oversized, or contains controls")
		}
	}
	if event.Event != "startup_registration_denial" || event.Action != "register" || event.Result != "closed" || event.HTTPStatus != http.StatusGone ||
		event.Stage != "startup-grant" || event.OmittedReason != "" || event.GrantResolved ||
		event.ResolvedNodeID != "" || event.AssignmentID != "" || event.RuntimeContainerID != "" ||
		!gcsSupervisorRunIDPattern.MatchString(event.AttemptedRunID) || event.AttemptedRunID == expected.OldRunID ||
		event.Namespace != expected.Namespace || event.StatefulSetUID != expected.StatefulSetUID || event.PodUID != expected.PodUID ||
		event.SourceClusterID != expected.SourceClusterID || event.RequestedNodeID != expected.RequestedNodeID ||
		containsString(expected.CanonicalRunIDs, event.AttemptedRunID) {
		return fmt.Errorf("startup denial event does not match the pinned early-grant closed-generation identity")
	}
	return nil
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func newGCSMixedPendingTargets(original []operator.FenceTarget, removed, learnerEvidence, learnerRequest operator.FenceTarget,
	learnerIdentity fencing.Identity, learnerRuntime fencing.RuntimeBinding) (gcsMixedPendingTargets, error) {
	var result gcsMixedPendingTargets
	if len(original) != 3 || !completeGCSMixedTarget(removed) || !completeGCSMixedTarget(learnerEvidence) ||
		learnerIdentity.SourceClusterID == "" || learnerIdentity.NodeID != learnerEvidence.NodeID || learnerIdentity.PodUID != learnerEvidence.PodUID ||
		learnerIdentity.RunID == "" || learnerIdentity.PodName != learnerEvidence.Pod || learnerIdentity.WALIdentity != learnerEvidence.WALIdentity ||
		learnerIdentity.AssignmentID == "" || learnerRuntime.Validate(learnerIdentity) != nil ||
		learnerRuntime.NodeName != learnerEvidence.NodeName || learnerRuntime.NodeUID != learnerEvidence.NodeUID ||
		learnerRuntime.ContainerID != learnerEvidence.ContainerID || learnerRuntime.ImageID != learnerEvidence.ImageID ||
		learnerRequest.NodeID != learnerIdentity.NodeID || learnerRequest.Pod != learnerIdentity.PodName ||
		learnerRequest.PodUID != learnerIdentity.PodUID || learnerRequest.WALIdentity != learnerIdentity.WALIdentity ||
		(learnerRequest.NodeName != "" && learnerRequest.NodeName != learnerRuntime.NodeName) ||
		(learnerRequest.NodeUID != "" && learnerRequest.NodeUID != learnerRuntime.NodeUID) ||
		(learnerRequest.ContainerID != "" && learnerRequest.ContainerID != learnerRuntime.ContainerID) ||
		(learnerRequest.ImageID != "" && learnerRequest.ImageID != learnerRuntime.ImageID) {
		return result, fmt.Errorf("mixed generation expectation requires three source voters and one canonical assigned learner")
	}
	seenNode, seenPod, seenUID := map[string]bool{}, map[string]bool{}, map[string]bool{}
	removedFound := false
	for _, target := range original {
		if !completeGCSMixedTarget(target) || seenNode[target.NodeID] || seenPod[target.Pod] || seenUID[target.PodUID] {
			return result, fmt.Errorf("mixed source voter targets are incomplete or duplicated")
		}
		seenNode[target.NodeID], seenPod[target.Pod], seenUID[target.PodUID] = true, true, true
		if target == removed {
			removedFound = true
		}
	}
	sameOrdinalPod := learnerRequest.Pod == removed.Pod
	if !removedFound || seenNode[learnerRequest.NodeID] || seenUID[learnerRequest.PodUID] || (seenPod[learnerRequest.Pod] && !sameOrdinalPod) ||
		(sameOrdinalPod && (learnerRequest.NodeID == removed.NodeID || learnerRequest.PodUID == removed.PodUID)) {
		return result, fmt.Errorf("mixed removed voter or pending learner is outside the exact source cohort")
	}
	result.OriginalVoters = append([]operator.FenceTarget(nil), original...)
	result.RemovedVoter = removed
	result.PendingLearner = learnerRequest
	result.PendingLearnerEvidence = learnerEvidence
	result.PendingLearnerIdentity = learnerIdentity
	result.ExpectedGenerationTargets = append([]operator.FenceTarget(nil), original...)
	result.ExpectedGenerationTargets = append(result.ExpectedGenerationTargets, learnerRequest)
	for _, target := range original {
		if target != removed {
			result.CurrentSurvivors = append(result.CurrentSurvivors, target)
		}
	}
	return result, nil
}

func completeGCSMixedTarget(target operator.FenceTarget) bool {
	return target.NodeID != "" && target.Pod != "" && target.PodUID != "" && target.WALIdentity != "" &&
		target.NodeName != "" && target.NodeUID != "" && target.ContainerID != "" && target.ImageID != ""
}

// captureGCSMixedPendingTarget keeps the production request projection separate
// from its runtime evidence source: the Node UID comes from the canonical run
// binding and is cross-checked against the live Node object, while the Pod's
// current container and image must still match that same binding.
func captureGCSMixedPendingTarget(identity fencing.Identity, runtime fencing.RuntimeBinding, pod, node obj) (operator.FenceTarget, error) {
	var zero operator.FenceTarget
	if runtime.Validate(identity) != nil || identity.NodeID == "" || identity.PodName == "" || identity.PodUID == "" || identity.WALIdentity == "" || identity.AssignmentID == "" ||
		runtime.NodeName == "" || runtime.NodeUID == "" || text(nested(node, "metadata", "name")) != runtime.NodeName || text(nested(node, "metadata", "uid")) != runtime.NodeUID ||
		text(nested(pod, "metadata", "name")) != identity.PodName || text(nested(pod, "metadata", "uid")) != identity.PodUID ||
		text(nested(pod, "spec", "nodeName")) != runtime.NodeName {
		return zero, fmt.Errorf("canonical learner runtime, Pod, and Node identities do not agree")
	}
	target := capturedTarget(operator.FenceTarget{NodeID: identity.NodeID, Pod: identity.PodName, PodUID: identity.PodUID,
		WALIdentity: identity.WALIdentity, NodeUID: runtime.NodeUID}, pod)
	if !completeGCSMixedTarget(target) || target.NodeName != runtime.NodeName || target.NodeUID != runtime.NodeUID ||
		target.ContainerID != runtime.ContainerID || target.ImageID != runtime.ImageID {
		return zero, fmt.Errorf("captured learner request projection differs from its canonical runtime binding")
	}
	return target, nil
}

func verifyGCSMixedPendingGenerationProof(item any, source gcsRecoverySource, expected gcsMixedPendingTargets, voterRuns []gcsMixedVoterRun,
	root fenceLedgerState) (operator.FenceRequest, operator.FenceProof, error) {
	var zeroRequest operator.FenceRequest
	var zeroProof operator.FenceProof
	if len(voterRuns) != 3 {
		return zeroRequest, zeroProof, fmt.Errorf("mixed proof requires exactly three captured source voter runs")
	}
	request, proof, err := verifyIndependentGenerationProofAgainstRootWithV2Targets(item, source.ClusterID, source.StatefulSetUID,
		source.BindingUID, expected.ExpectedGenerationTargets, expected.OriginalVoters, source.Bucket, source.Prefix, root,
		source.Namespace, source.StatefulSet, source.LogicalID)
	if err != nil {
		return request, proof, err
	}
	terminals := proof.IndependentV2.Terminals
	if len(terminals) != 4 {
		return request, proof, fmt.Errorf("mixed proof terminal set is not exactly three voter V2 runs plus one pending learner V3 run")
	}
	seenRuns := make(map[string]bool, len(terminals))
	for _, run := range voterRuns {
		if run.Identity.SourceClusterID != source.ClusterID || run.Identity.NodeID != run.Target.NodeID || run.Identity.PodUID != run.Target.PodUID ||
			run.Identity.WALIdentity != run.Target.WALIdentity || run.Runtime.Validate(run.Identity) != nil || run.Producer == "" {
			return request, proof, fmt.Errorf("mixed V2 expectation is not bound to the captured source voter runtime")
		}
		found := false
		for _, ref := range terminals {
			if ref.Request.Identity.RunID != run.Identity.RunID {
				continue
			}
			retainedRemoved := run.Target == expected.RemovedVoter
			requestMatches := ref.Request.OperationID == request.OperationID && ref.Request.RequestHash == digestJSON(request)
			if retainedRemoved {
				requestMatches = expected.RetainedRemovedVoterTerminal.Request.Identity.RunID != "" && ref == expected.RetainedRemovedVoterTerminal
			}
			if found || ref.EvidenceVersion != 2 || !requestMatches || ref.Request.Identity != run.Identity ||
				ref.RuntimeHash != fencing.RuntimeBindingDigest(run.Runtime) || ref.ProducerUID != run.Producer || ref.MutationReceiptDigest == "" {
				return request, proof, fmt.Errorf("mixed generation proof changed a captured active-voter V2 terminal")
			}
			found = true
			seenRuns[run.Identity.RunID] = true
		}
		if !found {
			return request, proof, fmt.Errorf("mixed generation proof omits a captured active-voter V2 run")
		}
	}
	learner := expected.PendingLearnerIdentity
	foundLearner := false
	for _, ref := range terminals {
		if ref.Request.Identity.RunID != learner.RunID {
			continue
		}
		if foundLearner || ref.EvidenceVersion != 3 || ref.Request.OperationID != request.OperationID || ref.Request.Identity != learner ||
			ref.ProducerUID != "" || ref.MutationReceiptDigest != "" {
			return request, proof, fmt.Errorf("mixed generation proof learner terminal is not the exact no-producer pending V3 run")
		}
		foundLearner = true
		seenRuns[learner.RunID] = true
	}
	if !foundLearner || len(seenRuns) != 4 {
		return request, proof, fmt.Errorf("mixed generation proof does not contain exactly the four expected run identities")
	}
	return request, proof, nil
}

// verifyGCSMixedDeniedRootSafety allows unrelated root CAS metadata to move,
// while pinning the generation closure and every run/terminal safety record.
func verifyGCSMixedDeniedRootSafety(before, after fenceLedgerState, request operator.FenceRequest, proof operator.FenceProof,
	expected gcsMixedPendingTargets, attemptedRunID string) error {
	oldLearner := expected.PendingLearnerIdentity.RunID
	retainedVoter := expected.RetainedRemovedVoterTerminal
	requestHash := digestJSON(request)
	if before.StatefulSetUID != request.StatefulSetUID || after.StatefulSetUID != request.StatefulSetUID ||
		before.Ledger.SourceClusterID != request.SourceClusterID || after.Ledger.SourceClusterID != request.SourceClusterID ||
		before.Ledger.ClosedBy != request.OperationID || after.Ledger.ClosedBy != request.OperationID ||
		before.Ledger.ClosedHash != requestHash || after.Ledger.ClosedHash != requestHash ||
		!before.Ledger.RequireRuntimeBinding || !after.Ledger.RequireRuntimeBinding ||
		!before.Ledger.RequireMutationJournal || !after.Ledger.RequireMutationJournal ||
		!reflect.DeepEqual(before.Ledger.ClosedNodes, after.Ledger.ClosedNodes) {
		return fmt.Errorf("canonical root changed its exact generation closure or fencing profile")
	}
	if proof.OperationID != request.OperationID || proof.RequestHash != requestHash || proof.IndependentV2 == nil ||
		proof.IndependentV2.RequestHash != requestHash || len(proof.IndependentV2.Terminals) != 4 {
		return fmt.Errorf("mixed proof does not pin exactly four terminals for the expected generation request")
	}
	refs := make(map[string]fencing.TerminalReference, len(proof.IndependentV2.Terminals))
	learnerRefs := 0
	retainedVoterRefs := 0
	retainedIdentity := retainedVoter.Request.Identity
	if retainedVoter.Request.OperationID == "" || retainedVoter.Request.RequestHash == "" ||
		retainedIdentity.SourceClusterID != request.SourceClusterID || retainedIdentity.NodeID != expected.RemovedVoter.NodeID ||
		retainedIdentity.PodName != expected.RemovedVoter.Pod || retainedIdentity.PodUID != expected.RemovedVoter.PodUID ||
		retainedIdentity.WALIdentity != expected.RemovedVoter.WALIdentity || retainedIdentity.RunID == "" ||
		retainedVoter.Request.OperationID == request.OperationID || retainedVoter.Request.RequestHash == requestHash {
		return fmt.Errorf("removed voter prior terminal reference is incomplete")
	}
	for _, ref := range proof.IndependentV2.Terminals {
		runID := ref.Request.Identity.RunID
		if runID == "" {
			return fmt.Errorf("mixed terminal reference is not bound to the expected generation request")
		}
		if _, duplicate := refs[runID]; duplicate {
			return fmt.Errorf("mixed proof contains duplicate terminal run identities")
		}
		refs[runID] = ref
		if runID == retainedVoter.Request.Identity.RunID {
			retainedVoterRefs++
			if retainedVoterRefs != 1 || retainedVoter.Request.Identity == (fencing.Identity{}) || ref != retainedVoter ||
				ref.EvidenceVersion != 2 || ref.ProducerUID == "" || ref.MutationReceiptDigest == "" {
				return fmt.Errorf("removed voter terminal is not the exact retained prior V2 reference")
			}
		} else if ref.Request.OperationID != request.OperationID || ref.Request.RequestHash != requestHash {
			return fmt.Errorf("newly closed terminal reference is not bound to the generation request")
		}
		if runID == oldLearner {
			learnerRefs++
			if ref.EvidenceVersion != 3 || ref.ProducerUID != "" || ref.MutationReceiptDigest != "" {
				return fmt.Errorf("original learner terminal no longer has its exact pending V3 shape")
			}
		} else if ref.EvidenceVersion != 2 || ref.ProducerUID == "" || ref.MutationReceiptDigest == "" {
			return fmt.Errorf("source voter terminal no longer has its producer-backed V2 shape")
		}
	}
	if learnerRefs != 1 || retainedVoterRefs != 1 || len(before.Ledger.Runs) != len(refs) || len(after.Ledger.Runs) != len(refs) {
		return fmt.Errorf("canonical root run set differs from the exact four terminal references")
	}
	if _, exists := refs[attemptedRunID]; attemptedRunID == "" || exists {
		return fmt.Errorf("denied startup RunID is empty or already present in the proof")
	}
	for runID, ref := range refs {
		beforeRun, beforeOK := before.Ledger.Runs[runID]
		afterRun, afterOK := after.Ledger.Runs[runID]
		if !beforeOK || !afterOK || !gcsMixedRunHasOnlyValidLateDrainExit(before.Ledger, runID, afterRun) || beforeRun.Identity != ref.Request.Identity ||
			beforeRun.Stop == nil || *beforeRun.Stop != ref.Request || afterRun.IndependentTerminal == nil || *afterRun.IndependentTerminal != ref ||
			(beforeRun.IndependentTerminal != nil && *beforeRun.IndependentTerminal != ref) {
			return fmt.Errorf("canonical run or terminal reference changed for run %q", runID)
		}
		if runID == oldLearner && (fencing.RunRole(beforeRun) != fencing.StartupRoleLearnerPendingRemove || beforeRun.Promotion != nil ||
			beforeRun.MutationProducerUID != "" || beforeRun.MutationScopeHash != "" || beforeRun.MutationProfile != "" || beforeRun.MutationStageIncarnation != "") {
			return fmt.Errorf("original learner is no longer the closed no-producer pending run")
		}
	}
	return nil
}

func waitForGCSMixedClosedCheckpoint(ctx context.Context, store *operator.AuthorityStateStore, source gcsRecoverySource,
	request operator.FenceRequest, expected gcsMixedPendingTargets, voterRuns []gcsMixedVoterRun) (fenceLedgerState, *objstore.ObjectVersion, error) {
	var captured fenceLedgerState
	var capturedVersion *objstore.ObjectVersion
	err := wait(ctx, "exact canonical mixed generation closure before runtime-agent restoration", func() (bool, error) {
		root, version, err := store.ReadRoot(ctx, source.StatefulSetUID, source.ClusterID)
		if err != nil {
			return false, err
		}
		state := fenceLedgerState{StatefulSetUID: root.StatefulSetUID, Ledger: root.Ledger}
		if state.StatefulSetUID != source.StatefulSetUID || state.Ledger.SourceClusterID != source.ClusterID ||
			state.Ledger.ClosedBy != request.OperationID || state.Ledger.ClosedHash != digestJSON(request) ||
			!state.Ledger.RequireRuntimeBinding || !state.Ledger.RequireMutationJournal {
			return false, nil
		}
		if len(state.Ledger.Runs) != 4 || len(voterRuns) != 3 {
			return false, fmt.Errorf("canonical closure run set is not exactly the three source voters plus pending learner")
		}
		for _, voter := range voterRuns {
			run, ok := state.Ledger.Runs[voter.Identity.RunID]
			if !ok || run.Identity != voter.Identity || run.Runtime == nil || run.Runtime.Validate(run.Identity) != nil ||
				run.Promotion != nil || run.MutationProducerUID != voter.Producer || run.Stop == nil {
				return false, fmt.Errorf("canonical closure changed a captured source voter run")
			}
			if voter.Target == expected.RemovedVoter {
				if run.IndependentTerminal == nil || *run.IndependentTerminal != expected.RetainedRemovedVoterTerminal || *run.Stop != expected.RetainedRemovedVoterTerminal.Request {
					return false, fmt.Errorf("canonical closure changed the retained prior removed-voter terminal")
				}
			} else if run.Stop.OperationID != request.OperationID || run.Stop.RequestHash != digestJSON(request) {
				return false, fmt.Errorf("surviving voter run lacks the exact generation close request")
			}
		}
		learner, ok := state.Ledger.Runs[expected.PendingLearnerIdentity.RunID]
		if !ok || learner.Identity != expected.PendingLearnerIdentity || learner.Runtime == nil ||
			learner.Runtime.Validate(learner.Identity) != nil || learner.Stop == nil || learner.Stop.OperationID != request.OperationID ||
			learner.Stop.RequestHash != digestJSON(request) || fencing.RunRole(learner) != fencing.StartupRoleLearnerPendingRemove ||
			learner.Promotion != nil || learner.MutationProducerUID != "" || learner.MutationScopeHash != "" || learner.MutationProfile != "" ||
			learner.MutationStageIncarnation != "" || learner.IndependentTerminal != nil {
			return false, fmt.Errorf("canonical closure changed original no-producer pending learner run")
		}
		captured, capturedVersion = state, version
		return true, nil
	})
	return captured, capturedVersion, err
}

func waitForGCSMixedNaturalDeniedStartup(ctx context.Context, source gcsRecoverySource, authoritySource gcsAuthorityLogSource,
	preflightLogs []byte, expected gcsStartupDenialExpectation, previousRestarts float64, previousContainerID, expectedImageID, expectedPodSpecHash string) (
	[]byte, string, string, gcsStartupDenialEvent, []byte, error) {
	var clientLogs []byte
	var errClosedRow string
	var terminatedContainerID string
	var denialEvent gcsStartupDenialEvent
	var denialRow string
	var postflightLogs []byte
	err := wait(ctx, "one natural same-PodUID early grant denial and ErrClosed restart", func() (bool, error) {
		current, err := get(ctx, "pod", expectedNodePodName(expected))
		if err != nil {
			return false, nil
		}
		if text(nested(current, "metadata", "uid")) != expected.PodUID || !gcsPodOwnedBy(current, source.StatefulSetUID) {
			return false, fmt.Errorf("natural closed-generation startup changed physical Pod identity")
		}
		if digestJSON(nested(current, "spec")) != expectedPodSpecHash {
			return false, fmt.Errorf("natural closed-generation startup changed the supervised Pod launch specification")
		}
		status := namedContainerStatus(current, source.Container)
		if status == nil {
			return false, fmt.Errorf("natural learner container status disappeared")
		}
		restarts := numberOrZero(nested(status, "restartCount"))
		// One natural C0 shutdown may be followed by the single denied C1
		// startup, after which kubelet can already have started C2. The
		// --previous container must be C1 and there must be no third restart.
		if restarts > previousRestarts+2 {
			return false, fmt.Errorf("learner Pod had an unexplained restart beyond the one natural transition and one denied attempt")
		}
		logs, err := readGCSAuthorityLogs(ctx, source, authoritySource)
		if err != nil {
			return false, err
		}
		candidate, row, parseErr := parseGCSStartupDenialDelta(preflightLogs, logs, expected)
		if parseErr != nil && !errors.Is(parseErr, errGCSStartupDenialPending) {
			return false, parseErr
		}
		postflightLogs = logs
		if parseErr == nil {
			denialEvent, denialRow = candidate, row
		}
		if restarts < previousRestarts+1 || denialRow == "" {
			return false, nil
		}
		if text(nested(status, "containerID")) == "" || text(nested(status, "containerID")) == previousContainerID ||
			text(nested(status, "imageID")) != expectedImageID {
			return false, fmt.Errorf("natural C1/C2 lifecycle changed image or did not advance beyond C0")
		}
		terminatedContainerID = text(nested(status, "lastState", "terminated", "containerID"))
		if terminatedContainerID == "" || terminatedContainerID == previousContainerID {
			return false, nil
		}
		logsFromC1, err := command(ctx, "kubectl", "--context", kubeContext, "-n", source.Namespace, "logs", expectedNodePodName(expected),
			"-c", source.Container, "--previous")
		if err != nil {
			return false, nil
		}
		closedRow, err := selectGCSClientErrClosedRow(logsFromC1)
		if err != nil {
			return false, nil
		}
		stable, err := get(ctx, "pod", expectedNodePodName(expected))
		stableStatus := namedContainerStatus(stable, source.Container)
		if err != nil || text(nested(stable, "metadata", "uid")) != expected.PodUID ||
			digestJSON(nested(stable, "spec")) != expectedPodSpecHash ||
			numberOrZero(nested(stableStatus, "restartCount")) != restarts ||
			text(nested(stableStatus, "lastState", "terminated", "containerID")) != terminatedContainerID ||
			text(nested(stableStatus, "imageID")) != expectedImageID {
			return false, nil
		}
		clientLogs, errClosedRow = logsFromC1, closedRow
		return true, nil
	})
	return clientLogs, errClosedRow, terminatedContainerID, denialEvent, postflightLogs, err
}

func expectedNodePodName(expected gcsStartupDenialExpectation) string {
	return expected.RequestedNodeID
}

// gcsMixedRunHasOnlyValidLateDrainExit accepts only the monotonic cooperative
// ledger transition methods after a terminal reference has been persisted.
func gcsMixedRunHasOnlyValidLateDrainExit(before fencing.Ledger, runID string, after fencing.Run) bool {
	original, ok := before.Runs[runID]
	if !ok || original.Identity != after.Identity || original.Role != after.Role || !sameStopRequest(original.Stop, after.Stop) ||
		!sameStopRequest(original.NeverBoundExit, after.NeverBoundExit) || !reflect.DeepEqual(original.Runtime, after.Runtime) ||
		original.MutationProducerUID != after.MutationProducerUID || original.MutationScopeHash != after.MutationScopeHash ||
		original.MutationProfile != after.MutationProfile || original.MutationStageIncarnation != after.MutationStageIncarnation ||
		!reflect.DeepEqual(original.Promotion, after.Promotion) ||
		(original.IndependentTerminal != nil && !reflect.DeepEqual(original.IndependentTerminal, after.IndependentTerminal)) {
		return false
	}
	if original.Exited && !after.Exited || original.Drain != nil && !reflect.DeepEqual(original.Drain, after.Drain) {
		return false
	}
	if reflect.DeepEqual(original.Drain, after.Drain) && original.Exited == after.Exited {
		observed := after
		observed.IndependentTerminal = original.IndependentTerminal
		return reflect.DeepEqual(original, observed)
	}
	if after.NeverBoundExit != nil || after.Stop == nil {
		return false
	}
	ledger := fencing.Ledger{Runs: make(map[string]fencing.Run, len(before.Runs))}
	for id, run := range before.Runs {
		ledger.Runs[id] = run
	}
	if original.Drain == nil && after.Drain != nil {
		if err := ledger.RecordDrain(*after.Drain); err != nil {
			return false
		}
	}
	current := ledger.Runs[runID]
	if !current.Exited && after.Exited {
		if err := ledger.RecordExit(*after.Stop); err != nil {
			return false
		}
	}
	observed := after
	observed.IndependentTerminal = ledger.Runs[runID].IndependentTerminal
	return reflect.DeepEqual(ledger.Runs[runID], observed)
}

func sameStopRequest(a, b *fencing.StopRequest) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}

func hasGCSMixedRootVersionEvidence(before, after *objstore.ObjectVersion) bool {
	validType := func(value objstore.ObjectVersionType) bool {
		return value == objstore.Generation || value == objstore.ETag
	}
	return before != nil && after != nil && validType(before.Type) && before.Value != "" && validType(after.Type) && after.Value != ""
}

func gcsMixedGateSnapshotForTarget(ctx context.Context, target operator.FenceTarget, out string) (snapshot gcsMixedGateSnapshot, resultErr error) {
	forward, err := startForward(ctx, target.Pod, out)
	if err != nil {
		return snapshot, err
	}
	defer func() { resultErr = errors.Join(resultErr, forward.close()) }()
	data, err := call(ctx, forward, "/qualification/remove-gate", nil, "")
	if err != nil {
		return snapshot, err
	}
	if err := json.Unmarshal(encoded(data), &snapshot); err != nil {
		return snapshot, fmt.Errorf("decode fixture REMOVE gate telemetry: %w", err)
	}
	return snapshot, nil
}

func verifyGCSMixedGatePreflight(ctx context.Context, source gcsRecoverySource, out string) ([]gcsMixedGateSnapshot, error) {
	if os.Getenv(gcsMixedRemoveGateSource) != source.ClusterID || len(source.Targets) != 3 {
		return nil, fmt.Errorf("mixed qualification requires the gate env pinned to the exact three-voter source")
	}
	wantVoters := make([]string, len(source.Targets))
	for i, target := range source.Targets {
		wantVoters[i] = target.NodeID
	}
	var snapshots []gcsMixedGateSnapshot
	for _, target := range source.Targets {
		snapshot, err := gcsMixedGateSnapshotForTarget(ctx, target, out)
		if err != nil {
			return nil, fmt.Errorf("read source voter %s REMOVE gate before fault: %w", target.NodeID, err)
		}
		if !snapshot.Enabled || snapshot.SourceClusterID != source.ClusterID || snapshot.CurrentClusterID != source.ClusterID ||
			snapshot.LocalNodeID != target.NodeID || !sameStringSet(snapshot.ConfiguredVoters, wantVoters) ||
			snapshot.SuccessfulRemoves != 0 || snapshot.BlockedSuccesses != 0 || !gcsMixedGateHasNoViolations(snapshot) {
			return nil, fmt.Errorf("source voter %s REMOVE gate telemetry is not a clean exact-source preflight", target.NodeID)
		}
		snapshots = append(snapshots, snapshot)
	}
	return snapshots, nil
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[string]bool, len(a))
	for _, value := range a {
		if value == "" || seen[value] {
			return false
		}
		seen[value] = true
	}
	for _, value := range b {
		if !seen[value] {
			return false
		}
	}
	return true
}

func gcsMixedGateHasNoViolations(snapshot gcsMixedGateSnapshot) bool {
	return snapshot.UnexpectedAdditions == 0 && snapshot.CaptureFailures == 0 && snapshot.IdentityViolations == 0
}

func verifyGCSMixedGateWithheldRemove(ctx context.Context, source gcsRecoverySource, targets []operator.FenceTarget, remove any, out string) ([]gcsMixedGateSnapshot, error) {
	requestDigestBytes := sha256.Sum256(encoded(remove))
	wantRequestDigest := hex.EncodeToString(requestDigestBytes[:])
	var snapshots []gcsMixedGateSnapshot
	blocked := 0
	for _, target := range targets {
		snapshot, err := gcsMixedGateSnapshotForTarget(ctx, target, out)
		if err != nil {
			return nil, err
		}
		if snapshot.SourceClusterID != source.ClusterID || snapshot.CurrentClusterID != source.ClusterID || !snapshot.Enabled ||
			snapshot.LocalNodeID != target.NodeID || !gcsMixedGateHasNoViolations(snapshot) {
			return nil, fmt.Errorf("source voter %s gate telemetry has a violation or wrong generation", target.NodeID)
		}
		if snapshot.SuccessfulRemoves > 0 {
			if snapshot.LastRequestSHA256 != wantRequestDigest || snapshot.LastHandlerStatus != http.StatusOK ||
				snapshot.BlockedSuccesses != snapshot.SuccessfulRemoves || !validSHA256(snapshot.LastResponseSHA256) {
				return nil, fmt.Errorf("source voter %s gate observed a REMOVE other than the exact successful withheld request", target.NodeID)
			}
			blocked++
		} else if snapshot.BlockedSuccesses != 0 {
			return nil, fmt.Errorf("source voter %s gate counted a blocked response without a successful REMOVE", target.NodeID)
		}
		snapshots = append(snapshots, snapshot)
	}
	if blocked == 0 {
		return nil, fmt.Errorf("no original source endpoint recorded the exact successful REMOVE whose response was withheld")
	}
	return snapshots, nil
}

func verifyGCSMixedLearnerProducerAbsent(ctx context.Context, bucket objstore.Bucket, prefix string, run fencing.Run) error {
	if bucket == nil || prefix == "" || run.Runtime == nil || run.MutationProducerUID != "" || run.MutationScopeHash != "" ||
		run.MutationProfile != "" || run.MutationStageIncarnation != "" {
		return fmt.Errorf("pending learner canonical run already claims source mutation authority")
	}
	digest := sha256.Sum256(encoded([]any{"rhiza-mutation-producer-v1", *run.Runtime}))
	name := path.Join(prefix, "rhiza-authority/v1/producers", hex.EncodeToString(digest[:])+".json")
	_, err := bucket.Attributes(ctx, name)
	if err == nil {
		return fmt.Errorf("pending learner deterministic producer object exists")
	}
	if !bucket.IsObjNotFoundErr(err) {
		return fmt.Errorf("pending learner producer absence is unconfirmed: %w", err)
	}
	return nil
}

func waitForGCSMixedPendingCheckpoint(ctx context.Context, source gcsRecoverySource, removed operator.FenceTarget,
	voterRequest operator.FenceRequest, voterProof operator.FenceProof, stateStore *operator.AuthorityStateStore,
	readBucket objstore.Bucket, authorityPrefix string, survivorForwards []*forward, adminToken, out string) (gcsMixedPendingCheckpoint, error) {
	var checkpoint gcsMixedPendingCheckpoint
	err := wait(ctx, "canonical pending learner after successful REMOVE whose response was withheld", func() (bool, error) {
		childObject, err := get(ctx, "rhizarecovery", voterRequest.OperationID)
		if err != nil {
			return false, nil
		}
		var child operator.Resource
		if err := json.Unmarshal(encoded(childObject), &child); err != nil {
			return false, err
		}
		childUID := text(child.Metadata["uid"])
		generation, _ := child.Metadata["generation"].(float64)
		if childUID == "" || text(child.Metadata["name"]) != voterRequest.OperationID || text(child.Metadata["namespace"]) != source.Namespace ||
			text(nested(child.Metadata, "annotations", "rhiza.mrchypark.dev/automatic-owner")) != source.BindingUID ||
			text(nested(child.Metadata, "annotations", "rhiza.mrchypark.dev/automatic-logical")) != source.LogicalID || generation < 1 ||
			child.Spec.SourceClusterID != source.ClusterID || child.Spec.RecoveryID != voterRequest.OperationID || child.Spec.StatefulSet != source.StatefulSet ||
			child.Status.StatefulSetUID != source.StatefulSetUID || child.Status.Source != source.ClusterID || child.Status.ObservedGeneration != int64(generation) ||
			child.Status.Membership == nil || child.Status.Membership.Phase != "Removing" || child.Status.Membership.Add != nil || child.Status.Membership.RemoveRequest == nil ||
			child.Spec.Membership == nil || child.Spec.Membership.AbortAddition || child.Spec.Membership.OperationID != voterRequest.OperationID ||
			child.Spec.Membership.Remove != networkNodeID(removed.NodeID) || child.Spec.Membership.ReplacementPod == "" || child.Spec.Membership.ReplacementSecret == "" {
			return false, nil
		}
		remove := *child.Status.Membership.RemoveRequest
		if remove.OperationID != voterRequest.OperationID || remove.ClusterID != source.ClusterID || remove.Remove != networkNodeID(removed.NodeID) ||
			remove.ExpectedConfigID == 0 || remove.Add != nil || remove.Fence == nil || string(remove.Fence.NodeID) != removed.NodeID ||
			remove.Fence.WALIdentity != removed.WALIdentity || remove.Fence.WorkloadUID != removed.PodUID || !remove.Fence.Confirmed || remove.Fence.Evidence != voterProof.ProofID ||
			child.Spec.Membership.Fence.Evidence != voterProof.ProofID || child.Spec.Membership.Fence.WorkloadUID != removed.PodUID ||
			len(child.Spec.Membership.VoterPods) != 2 {
			return false, nil
		}
		if !gcsMixedSpecSurvivorsMatch(child.Spec.Membership.VoterPods, source.Targets, removed) {
			return false, fmt.Errorf("pending REMOVE child changed the exact two-survivor membership scope")
		}
		statuses := make([]network.MembershipStatus, 0, len(survivorForwards))
		for _, f := range survivorForwards {
			status, err := readMembershipWithToken(ctx, f, adminToken)
			if err != nil {
				return false, nil
			}
			if status.ClusterID != source.ClusterID || status.Pending || !status.Voting || len(status.Voters) != 2 || status.ConfigID != remove.ExpectedConfigID+1 ||
				!gcsMixedMembershipHasExactly(status, source.Targets, removed) {
				return false, fmt.Errorf("authenticated source survivor membership does not prove the exact completed REMOVE")
			}
			statuses = append(statuses, status)
		}
		root, _, err := stateStore.ReadRoot(ctx, source.StatefulSetUID, source.ClusterID)
		if err != nil {
			return false, err
		}
		var learnerRun fencing.Run
		for _, run := range root.Ledger.Runs {
			if fencing.RunRole(run) != fencing.StartupRoleLearnerPendingRemove || run.Stop != nil || run.Promotion != nil {
				continue
			}
			if learnerRun.Identity.RunID != "" {
				return false, fmt.Errorf("canonical source root has multiple open pending learner runs")
			}
			learnerRun = run
		}
		id := learnerRun.Identity
		runtime := learnerRun.Runtime
		if id.SourceClusterID != source.ClusterID || id.RunID == "" || id.PodName == "" || id.PodUID == "" || id.WALIdentity == "" || id.AssignmentID == "" ||
			runtime == nil || runtime.Validate(id) != nil || runtime.NamespaceUID != source.NamespaceUID || runtime.BindingUID != source.BindingUID || runtime.StatefulSetUID != source.StatefulSetUID ||
			learnerRun.MutationProducerUID != "" || learnerRun.MutationScopeHash != "" || learnerRun.MutationProfile != "" || learnerRun.MutationStageIncarnation != "" ||
			id.PodName != child.Spec.Membership.ReplacementPod {
			return false, nil
		}
		if err := verifyGCSMixedLearnerProducerAbsent(ctx, readBucket, authorityPrefix, learnerRun); err != nil {
			return false, err
		}
		pod, err := get(ctx, "pod", id.PodName)
		if err != nil || text(nested(pod, "metadata", "uid")) != id.PodUID || !gcsPodOwnedBy(pod, source.StatefulSetUID) ||
			nested(pod, "metadata", "deletionTimestamp") != nil || text(nested(pod, "metadata", "annotations", "rhiza.mrchypark.dev/fencing-root-uid")) != source.BindingUID ||
			text(nested(pod, "metadata", "annotations", "rhiza.mrchypark.dev/recovery-owner")) != voterRequest.OperationID {
			return false, fmt.Errorf("canonical pending learner Pod no longer matches its source, UID, or recovery owner")
		}
		credentialUID := text(nested(pod, "metadata", "annotations", "rhiza.mrchypark.dev/fencing-credential-uid"))
		secret, err := get(ctx, "secret", child.Spec.Membership.ReplacementSecret)
		credentialRV := text(nested(secret, "metadata", "resourceVersion"))
		if err != nil || nested(secret, "immutable") != true || credentialUID == "" || credentialUID != text(nested(secret, "metadata", "uid")) || credentialRV == "" ||
			text(nested(secret, "metadata", "annotations", "rhiza.mrchypark.dev/automatic-owner")) != voterRequest.OperationID {
			return false, fmt.Errorf("pending learner does not retain its exact immutable credential UID and resourceVersion")
		}
		learnerForward, err := startForward(ctx, id.PodName, out)
		if err != nil {
			return false, nil
		}
		learnerStatus, statusErr := readMembershipWithToken(ctx, learnerForward, adminToken)
		if closeErr := learnerForward.close(); closeErr != nil {
			return false, fmt.Errorf("close pending learner port-forward: %w", closeErr)
		}
		if statusErr != nil || learnerStatus.ClusterID != source.ClusterID || string(learnerStatus.NodeID) != id.NodeID || learnerStatus.WALIdentity != id.WALIdentity ||
			!learnerStatus.Pending || learnerStatus.Voting {
			return false, nil
		}
		node, err := get(ctx, "node", runtime.NodeName)
		if err != nil {
			return false, fmt.Errorf("canonical pending learner Node identity is unavailable")
		}
		learner, err := captureGCSMixedPendingTarget(id, *runtime, pod, node)
		if err != nil {
			return false, fmt.Errorf("pending learner lacks a complete canonical runtime target projection: %w", err)
		}
		gate, err := verifyGCSMixedGateWithheldRemove(ctx, source, survivorTargets(source.Targets, removed), remove, out)
		if err != nil {
			return false, nil
		}
		checkpoint = gcsMixedPendingCheckpoint{Recovery: child, RecoveryUID: childUID, RemoveRequest: remove, Learner: learner,
			LearnerID: id, LearnerRun: learnerRun, CredentialUID: credentialUID, CredentialRV: credentialRV,
			CredentialHash: digestJSON(secret["data"]), SurvivorStatus: statuses, Gate: gate}
		return true, nil
	})
	return checkpoint, err
}

func networkNodeID(value string) quepaxa.NodeID { return quepaxa.NodeID(value) }

func gcsMixedSpecSurvivorsMatch(actual []operator.VoterPod, original []operator.FenceTarget, removed operator.FenceTarget) bool {
	if len(actual) != 2 {
		return false
	}
	want := make(map[string]string, 2)
	for _, target := range original {
		if target != removed {
			want[target.NodeID] = target.Pod
		}
	}
	for _, voter := range actual {
		if want[string(voter.NodeID)] != voter.Pod {
			return false
		}
		delete(want, string(voter.NodeID))
	}
	return len(want) == 0
}

func gcsMixedMembershipHasExactly(status network.MembershipStatus, original []operator.FenceTarget, removed operator.FenceTarget) bool {
	want := make(map[string]bool, 2)
	for _, target := range original {
		if target != removed {
			want[target.NodeID] = true
		}
	}
	for _, voter := range status.Voters {
		if !want[string(voter)] {
			return false
		}
		delete(want, string(voter))
	}
	return len(want) == 0
}

func verifyGCSMixedCheckpointContainerID(checkpointContainerID, currentContainerID string) error {
	if checkpointContainerID == "" {
		return fmt.Errorf("canonical learner checkpoint has no container ID")
	}
	if currentContainerID == "" || currentContainerID != checkpointContainerID {
		return fmt.Errorf("current learner container does not match the canonical checkpoint")
	}
	return nil
}

func survivorTargets(original []operator.FenceTarget, removed operator.FenceTarget) []operator.FenceTarget {
	var survivors []operator.FenceTarget
	for _, target := range original {
		if target != removed {
			survivors = append(survivors, target)
		}
	}
	return survivors
}

func validSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func waitForGCSMixedPendingFence(ctx context.Context, source gcsRecoverySource, expected []operator.FenceTarget) (pendingGCSFence, error) {
	var pending pendingGCSFence
	err := wait(ctx, "exact four-target mixed generation fence request without proof", func() (bool, error) {
		list, err := get(ctx, "rhizafences", "")
		if err != nil {
			return false, err
		}
		items, _ := list["items"].([]any)
		var matches []pendingGCSFence
		for _, item := range items {
			var request operator.FenceRequest
			if err := json.Unmarshal(encoded(nested(item, "spec")), &request); err != nil {
				return false, err
			}
			if request.Scope != operator.ScopeGeneration || request.SourceClusterID != source.ClusterID || request.LogicalID != source.LogicalID ||
				request.BindingUID != source.BindingUID || request.Namespace != source.Namespace || request.StatefulSet != source.StatefulSet ||
				request.StatefulSetUID != source.StatefulSetUID || request.OperationID == "" || !sameTargets(request.Targets, expected) {
				continue
			}
			if nested(item, "status", "proof") != nil {
				return false, fmt.Errorf("mixed generation proof arrived before partition checkpoint")
			}
			name, uid := text(nested(item, "metadata", "name")), text(nested(item, "metadata", "uid"))
			if name == "" || uid == "" {
				return false, fmt.Errorf("mixed generation fence lacks stable API identity")
			}
			matches = append(matches, pendingGCSFence{Name: name, UID: uid, Request: request})
		}
		if len(matches) > 1 {
			return false, fmt.Errorf("multiple exact four-target mixed generation fences exist")
		}
		if len(matches) == 1 {
			pending = matches[0]
			return true, nil
		}
		return false, nil
	})
	return pending, err
}

func waitForGCSMixedFenceProof(ctx context.Context, source gcsRecoverySource, pending pendingGCSFence,
	targets gcsMixedPendingTargets, voterRuns []gcsMixedVoterRun, stateStore *operator.AuthorityStateStore) (operator.FenceRequest, operator.FenceProof, any, error) {
	var request operator.FenceRequest
	var proof operator.FenceProof
	var observed any
	err := wait(ctx, "canonical mixed three-V2-plus-one-V3 generation proof", func() (bool, error) {
		item, err := get(ctx, "rhizafence", pending.Name)
		if err != nil {
			return false, err
		}
		if !pendingFenceIdentityMatches(item, pending) {
			return false, fmt.Errorf("mixed generation fence API identity changed")
		}
		if _, err := exactPendingGCSFenceRequest(item, pending); err != nil {
			return false, fmt.Errorf("mixed generation fence request changed after observation: %w", err)
		}
		if nested(item, "status", "proof") == nil {
			return false, nil
		}
		var listItem any
		list, err := get(ctx, "rhizafences", "")
		if err != nil {
			return false, err
		}
		for _, candidate := range array(nested(list, "items")) {
			if text(nested(candidate, "metadata", "uid")) == pending.UID {
				listItem = candidate
				break
			}
		}
		if listItem == nil {
			return false, fmt.Errorf("mixed generation proof is absent from API list readback")
		}
		root, _, err := stateStore.ReadRoot(ctx, source.StatefulSetUID, source.ClusterID)
		if err != nil {
			return false, err
		}
		request, proof, err = verifyGCSMixedPendingGenerationProof(item, source, targets, voterRuns,
			fenceLedgerState{StatefulSetUID: root.StatefulSetUID, Ledger: root.Ledger})
		if err != nil {
			return false, err
		}
		var readbackRequest operator.FenceRequest
		var readbackProof operator.FenceProof
		if err := json.Unmarshal(encoded(nested(listItem, "spec")), &readbackRequest); err != nil {
			return false, err
		}
		if err := json.Unmarshal(encoded(nested(listItem, "status", "proof")), &readbackProof); err != nil {
			return false, err
		}
		if text(nested(listItem, "metadata", "uid")) != pending.UID || !sameFenceRequest(readbackRequest, request) ||
			!sameJSON(nested(listItem, "status", "proof"), nested(item, "status", "proof")) || readbackProof.ProofID != proof.ProofID {
			return false, fmt.Errorf("mixed generation proof API list readback changed request or immutable proof")
		}
		observed = item
		return true, nil
	})
	return request, proof, observed, err
}

func array(value any) []any {
	items, _ := value.([]any)
	return items
}

func gcsChaosFaultMatchesTargets(fault *gcsChaosFault, targets []operator.FenceTarget) bool {
	if fault == nil || len(fault.sourceUIDs) != len(targets) {
		return false
	}
	for _, target := range targets {
		if target.Pod == "" || target.PodUID == "" || fault.sourceUIDs[target.Pod] != target.PodUID {
			return false
		}
	}
	return true
}

func qualifyGCSMixedPending(ctx context.Context) (resultErr error) {
	out := strings.TrimSpace(os.Getenv("FENCING_E2E_OUT"))
	if out == "" {
		out = "gcs-ha-mixed-pending-results"
	}
	if err := os.MkdirAll(out, 0700); err != nil {
		return err
	}
	if info, err := os.Lstat(out); err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("mixed qualification result directory must be private")
	}
	events, err := os.OpenFile(filepath.Join(out, "gcs-mixed-pending-events.jsonl"), os.O_CREATE|os.O_EXCL|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, closeGCSMixedEventFile(events)) }()
	event := func(name string, data any) error {
		return json.NewEncoder(events).Encode(obj{"event": name, "time": time.Now().UTC(), "data": data})
	}
	source, err := captureGCSRecoverySource(ctx, out)
	if err != nil {
		return err
	}
	if source.Durability != "before-ack" || len(source.Targets) != 3 {
		return fmt.Errorf("mixed qualification requires exactly three before-ACK source voters")
	}
	gate, err := verifyGCSMixedGatePreflight(ctx, source, out)
	if err != nil {
		return err
	}
	if err := event("source_gate_preflight", gate); err != nil {
		return err
	}
	if err := verifyGCSDeadPodPassiveAgentProfile(ctx, source); err != nil {
		return fmt.Errorf("passive source-agent preflight: %w", err)
	}
	adminToken, err := sourceAdminToken(ctx, source.StatefulSet, source.Container)
	if err != nil {
		return err
	}
	observer, err := startForward(ctx, source.Targets[1].Pod, out)
	if err != nil {
		return err
	}
	observerClosed := false
	defer func() {
		if !observerClosed {
			resultErr = errors.Join(resultErr, observer.close())
		}
	}()
	if _, err := call(ctx, observer, "/sql/execute", obj{"request_id": "gcs-mixed-pending-create-table", "sql": "CREATE TABLE fencing_items (id INTEGER PRIMARY KEY, value TEXT)"}, adminToken); err != nil {
		return fmt.Errorf("create mixed qualification baseline table: %w", err)
	}
	baseline, err := writeGCSAcknowledgedRow(ctx, observer, adminToken, 1, "gcs-mixed-pending-baseline")
	if err != nil {
		return fmt.Errorf("establish source ACK before REMOVE: %w", err)
	}
	observerCloseErr := observer.close()
	observerClosed = true
	if observerCloseErr != nil {
		return fmt.Errorf("close baseline observer after its final source ACK: %w", observerCloseErr)
	}
	deployment, err := get(ctx, "deployment", "rhiza-fencer")
	if err != nil {
		return err
	}
	readScope, err := newGCSAckLossReadScope(ctx, source, deployment)
	if err != nil {
		return err
	}
	stateStore, evidenceStore, gcsClient, readBucket, closeStores, err := openGCSAckLossAuthorityStores(ctx, source, readScope)
	if err != nil {
		return err
	}
	defer func() {
		if gcsClient != nil {
			resultErr = errors.Join(resultErr, gcsClient.Close())
		}
		if closeStores != nil {
			resultErr = errors.Join(resultErr, closeStores())
		}
	}()
	root, _, err := stateStore.ReadRoot(ctx, source.StatefulSetUID, source.ClusterID)
	if err != nil || root.StatefulSetUID != source.StatefulSetUID || root.Ledger.SourceClusterID != source.ClusterID {
		if err != nil {
			return fmt.Errorf("read exact canonical source root before REMOVE: %w", err)
		}
		return fmt.Errorf("canonical source root identity changed before REMOVE")
	}
	var voterRuns []gcsMixedVoterRun
	for _, target := range source.Targets {
		var matched *fencing.Run
		for _, run := range root.Ledger.Runs {
			if run.Identity.PodUID != target.PodUID || run.Runtime == nil || run.Stop != nil || fencing.RunRole(run) != fencing.StartupRoleVoterActive {
				continue
			}
			if matched != nil {
				return fmt.Errorf("source voter Pod UID %s has multiple active canonical runs", target.PodUID)
			}
			copy := run
			matched = &copy
		}
		if matched == nil || matched.Runtime.Validate(matched.Identity) != nil || matched.Identity.NodeID != target.NodeID || matched.Identity.WALIdentity != target.WALIdentity {
			return fmt.Errorf("source voter %s lacks its exact active canonical runtime", target.NodeID)
		}
		_, producer, _, err := stateStore.ReadProducer(ctx, *matched.Runtime)
		if err != nil || producer == "" || producer != matched.MutationProducerUID {
			return fmt.Errorf("source voter %s lacks its exact canonical producer: %w", target.NodeID, err)
		}
		voterRuns = append(voterRuns, gcsMixedVoterRun{Target: target, Identity: matched.Identity, Runtime: *matched.Runtime, Producer: producer})
	}
	if err := event("source_bound", obj{"source": source, "voterRuns": voterRuns, "baselineAck": baseline, "authorityPrefix": readScope.Prefix}); err != nil {
		return err
	}
	removed := source.Targets[0]
	if err := requireNoPriorGCSVoterFence(ctx, source, removed); err != nil {
		return err
	}
	runtime, producerUID, err := captureGCSAckLossRuntime(ctx, stateStore, source, removed.PodUID)
	if err != nil {
		return err
	}
	faultStarted := time.Now().UTC().Truncate(time.Second)
	fault, err := createGCSDeadPodFault(ctx, source, removed)
	if err != nil {
		return err
	}
	defer func() {
		if fault != nil {
			cleanupCtx, cancel := gcsMixedCleanupContext(ctx, 45*time.Second)
			defer cancel()
			resultErr = errors.Join(resultErr, fault.Restore(cleanupCtx))
		}
	}()
	if err := fault.WaitInjected(ctx); err != nil {
		return err
	}
	if err := waitForGCSVoterFence(ctx, source, removed, runtime, producerUID, faultStarted); err != nil {
		return err
	}
	voterRequest, voterProof, fenceObject, err := captureGCSVoterFence(ctx, source, removed, runtime, producerUID, faultStarted)
	if err != nil {
		return err
	}
	ref, err := findGCSDeadPodTerminalRef(voterProof, source, removed, runtime, producerUID)
	if err != nil {
		return err
	}
	passiveReceipt, err := verifyGCSDeadPodPassiveTerminal(ctx, stateStore, evidenceStore, source, removed, runtime, producerUID, ref)
	if err != nil {
		return fmt.Errorf("source voter passive terminal evidence: %w", err)
	}
	if err := event("source_voter_fenced", obj{"request": voterRequest, "proof": voterProof, "resource": fenceObject, "passiveReceipt": passiveReceipt}); err != nil {
		return err
	}
	var survivorForwards []*forward
	survivorForwardsClosed := false
	defer func() {
		if !survivorForwardsClosed {
			for _, forward := range survivorForwards {
				resultErr = errors.Join(resultErr, forward.close())
			}
		}
	}()
	for _, target := range survivorTargets(source.Targets, removed) {
		f, err := startForward(ctx, target.Pod, out)
		if err != nil {
			return err
		}
		survivorForwards = append(survivorForwards, f)
	}
	checkpoint, err := waitForGCSMixedPendingCheckpoint(ctx, source, removed, voterRequest, voterProof, stateStore,
		readBucket, readScope.Prefix, survivorForwards, adminToken, out)
	if err != nil {
		return fmt.Errorf("exact pending-learner REMOVE checkpoint: %w", err)
	}
	preEscalationControlKey, controlBasePrefix, err := gcsMixedOperatorControlLocation(ctx, source)
	if err != nil {
		return err
	}
	learnerRequestTarget, learnerControlVersion, learnerControlDigest, err := readGCSMixedLearnerRequestTarget(ctx, readBucket, preEscalationControlKey,
		gcsMixedAutoControlExpectation{Source: source, Checkpoint: checkpoint, FenceRequest: voterRequest}, *checkpoint.LearnerRun.Runtime)
	if err != nil {
		return fmt.Errorf("pre-escalation canonical learner request projection: %w", err)
	}
	checkpoint.LearnerRequestTarget = learnerRequestTarget
	checkpoint.LearnerControlVersion = learnerControlVersion
	checkpoint.LearnerControlDigest = learnerControlDigest
	initialControlVersion, initialControlBytes, err := readGCSMixedAutoControlBytes(ctx, readBucket, preEscalationControlKey)
	if err != nil || initialControlVersion != learnerControlVersion {
		return fmt.Errorf("pre-escalation CONTROL changed before its complete intent snapshot: %w", err)
	}
	if _, err := validateGCSMixedAutoControl(initialControlBytes, gcsMixedAutoControlExpectation{Source: source, Checkpoint: checkpoint, FenceRequest: voterRequest}); err != nil {
		return fmt.Errorf("pre-escalation complete CONTROL intent is not exact: %w", err)
	}
	var initialControl gcsMixedAutoControlProjection
	if err := decodeGCSMixedAutoControl(initialControlBytes, &initialControl); err != nil || initialControl.Intent == nil {
		return fmt.Errorf("pre-escalation CONTROL omitted a complete intent: %w", err)
	}
	initialControlDigest := sha256.Sum256(initialControlBytes)
	if hex.EncodeToString(initialControlDigest[:]) != learnerControlDigest {
		return fmt.Errorf("learner history and complete pre-escalation CONTROL snapshots disagree")
	}
	checkpoint.InitialControlIntent = *initialControl.Intent
	if err := event("canonical_learner_request_projection", obj{"key": preEscalationControlKey, "basePrefix": controlBasePrefix,
		"versionType": learnerControlVersion.Type, "version": learnerControlVersion.Value, "sha256": learnerControlDigest,
		"target": learnerRequestTarget, "claim": "version-pinned pre-escalation CONTROL learner-history request projection"}); err != nil {
		return err
	}
	if err := event("mixed_pending_learner_checkpoint", checkpoint); err != nil {
		return err
	}
	replacementOperator, err := restartGCSOperatorAtRemoveCheckpointAndCapture(ctx, source)
	if err != nil {
		return fmt.Errorf("restart operator at stable REMOVE checkpoint: %w", err)
	}
	source.OperatorIdentity = replacementOperator
	postRestart, err := waitForGCSMixedPendingCheckpoint(ctx, source, removed, voterRequest, voterProof, stateStore,
		readBucket, readScope.Prefix, survivorForwards, adminToken, out)
	if err != nil || postRestart.RecoveryUID != checkpoint.RecoveryUID || postRestart.LearnerID != checkpoint.LearnerID ||
		postRestart.CredentialUID != checkpoint.CredentialUID || postRestart.CredentialRV != checkpoint.CredentialRV {
		return fmt.Errorf("operator restart did not preserve the exact pending learner REMOVE checkpoint: %w", err)
	}
	postAck, err := writeGCSAcknowledgedRow(ctx, survivorForwards[0], adminToken, 2, "gcs-mixed-pending-after-remove")
	if err != nil {
		return fmt.Errorf("two-voter quorum ACK after REMOVE checkpoint: %w", err)
	}
	rows, err := call(ctx, survivorForwards[0], "/sql/query", obj{"sql": "SELECT id,value FROM fencing_items ORDER BY id", "consistency": "linearizable"}, adminToken)
	if err != nil || string(encoded(rows["rows"])) != `[[1,"gcs-mixed-pending-baseline"],[2,"gcs-mixed-pending-after-remove"]]` {
		return fmt.Errorf("two-voter post-REMOVE linearizable readback differs: %v", err)
	}
	if err := event("two_voter_ack_after_remove", obj{"ack": postAck, "rows": rows["rows"], "checkpointUID": checkpoint.RecoveryUID}); err != nil {
		return err
	}
	expected, err := newGCSMixedPendingTargets(source.Targets, removed, checkpoint.Learner, checkpoint.LearnerRequestTarget,
		checkpoint.LearnerID, *checkpoint.LearnerRun.Runtime)
	if err != nil {
		return err
	}
	expected.RetainedRemovedVoterTerminal = ref
	combinedTargets := append(append([]operator.FenceTarget(nil), source.Targets...), checkpoint.Learner)
	_, mixedAgentPods, err := captureGCSRuntimeAgentPods(ctx, combinedTargets)
	if err != nil {
		return fmt.Errorf("capture runtime-agent snapshot including actual learner node: %w", err)
	}
	for _, original := range source.AgentPods {
		found := false
		for _, current := range mixedAgentPods {
			if current.Name == original.Name && current.UID == original.UID && current.OwnerUID == original.OwnerUID {
				found = true
			}
		}
		if !found {
			return fmt.Errorf("original source runtime-agent snapshot changed before mixed partition: %s", original.Name)
		}
	}
	var chaosFaults []*gcsChaosFault
	defer func() {
		cleanupCtx, cancel := gcsMixedCleanupContext(ctx, 45*time.Second)
		defer cancel()
		for i := len(chaosFaults) - 1; i >= 0; i-- {
			if chaosFaults[i] != nil {
				resultErr = errors.Join(resultErr, chaosFaults[i].Restore(cleanupCtx))
			}
		}
	}()
	chaosCtx, cancelChaos := context.WithTimeout(ctx, 165*time.Second)
	defer cancelChaos()
	if err := verifyGCSControlPodSnapshots(chaosCtx, mixedAgentPods); err != nil {
		return fmt.Errorf("mixed runtime-agent cohort changed before partition: %w", err)
	}
	agentFault, err := createGCSControlLoss(chaosCtx, source.Namespace, qualificationRunID, "mixed-pending", "runtime-agent", mixedAgentPods)
	if agentFault != nil {
		chaosFaults = append(chaosFaults, agentFault)
	}
	if err != nil {
		return err
	}
	if err := agentFault.WaitInjected(chaosCtx); err != nil {
		return err
	}
	cohortFault, err := partitionGCSVoterCohort(chaosCtx, source.Namespace, qualificationRunID, "mixed-pending", expected.CurrentSurvivors)
	if cohortFault != nil {
		chaosFaults = append(chaosFaults, cohortFault)
	}
	if err != nil {
		return err
	}
	if err := cohortFault.WaitInjected(chaosCtx); err != nil {
		return err
	}
	if !gcsChaosFaultMatchesTargets(cohortFault, expected.CurrentSurvivors) {
		return fmt.Errorf("two-survivor partition source identities differ from the captured active endpoints")
	}
	if err := event("current_two_survivors_partitioned", obj{"agentFault": gcsChaosEvidence(agentFault), "cohortFault": gcsChaosEvidence(cohortFault), "survivors": expected.CurrentSurvivors}); err != nil {
		return err
	}
	if err := wait(chaosCtx, "one current survivor observes lost quorum", func() (bool, error) {
		data, err := call(chaosCtx, survivorForwards[0], "/recovery/status", nil, adminToken)
		if err != nil {
			return false, err
		}
		var status network.VoterRecoveryStatus
		if err := json.Unmarshal(encoded(data), &status); err != nil {
			return false, err
		}
		if status.ClusterID != source.ClusterID {
			return false, fmt.Errorf("partition observer changed source generation")
		}
		return !status.Quorum, nil
	}); err != nil {
		return err
	}
	var survivorCloseErr error
	for _, forward := range survivorForwards {
		survivorCloseErr = errors.Join(survivorCloseErr, forward.close())
	}
	survivorForwardsClosed = true
	if survivorCloseErr != nil {
		return fmt.Errorf("close source survivor forwards after last quorum-loss observation and before generation terminalization: %w", survivorCloseErr)
	}
	pending, err := waitForGCSMixedPendingFence(chaosCtx, source, expected.ExpectedGenerationTargets)
	if err != nil {
		return fmt.Errorf("exact four-target mixed generation request: %w", err)
	}
	postPartitionGate, err := verifyGCSMixedGateWithheldRemove(chaosCtx, source, expected.CurrentSurvivors, checkpoint.RemoveRequest, out)
	if err != nil {
		return fmt.Errorf("mixed generation partition gate telemetry contains an ADD or unrelated REMOVE: %w", err)
	}
	if err := event("mixed_partition_remove_gate_recheck", obj{"snapshots": postPartitionGate,
		"claim": "two-survivor endpoints preserved exactly the withheld original REMOVE response and recorded no ADD"}); err != nil {
		return err
	}
	if err := agentFault.RecheckInjected(chaosCtx); err != nil {
		return err
	}
	operatorSnapshot := []gcsControlPodIdentity{source.OperatorIdentity}
	if err := verifyGCSControlPodSnapshots(chaosCtx, operatorSnapshot); err != nil {
		return err
	}
	operatorFault, err := createGCSControlLoss(chaosCtx, source.Namespace, qualificationRunID, "mixed-pending", "operator", operatorSnapshot)
	if operatorFault != nil {
		chaosFaults = append(chaosFaults, operatorFault)
	}
	if err != nil {
		return err
	}
	if err := operatorFault.WaitInjected(chaosCtx); err != nil {
		return err
	}
	if err := operatorFault.RecheckInjected(chaosCtx); err != nil {
		return err
	}
	if err := cohortFault.RecheckInjected(chaosCtx); err != nil {
		return err
	}
	generationControlVersion, generationControlDigest, supersededVersion, supersedesName, supersededDigest, err := verifyGCSMixedEscalatedControl(chaosCtx, readBucket,
		preEscalationControlKey, controlBasePrefix, source, checkpoint, checkpoint.InitialControlIntent, voterRequest, pending.Request)
	if err != nil {
		return fmt.Errorf("post-escalation canonical CONTROL/Supersedes proof: %w", err)
	}
	if err := event("automatic_escalation_control_and_superseded", obj{"controlKey": preEscalationControlKey,
		"controlVersionType": generationControlVersion.Type, "controlVersion": generationControlVersion.Value,
		"controlSHA256": generationControlDigest, "supersededVersion": supersededVersion,
		"supersedes": supersedesName, "supersededSHA256": supersededDigest,
		"claim": "version-pinned generation CONTROL and full-byte historical Supersedes verification"}); err != nil {
		return err
	}
	authoritySource, err := captureGCSAuthorityLogSource(chaosCtx, source)
	if err != nil {
		return fmt.Errorf("pin authority log source before terminalization: %w", err)
	}
	preflightLogs, err := readGCSAuthorityLogs(chaosCtx, source, authoritySource)
	if err != nil {
		return fmt.Errorf("authority log access preflight failed before terminalization: %w", err)
	}
	learnerPod, err := get(chaosCtx, "pod", checkpoint.Learner.Pod)
	if err != nil || text(nested(learnerPod, "metadata", "uid")) != checkpoint.Learner.PodUID || !gcsPodOwnedBy(learnerPod, source.StatefulSetUID) {
		return fmt.Errorf("learner Pod identity changed before natural closed-generation restart: %v", err)
	}
	status := namedContainerStatus(learnerPod, source.Container)
	if status == nil || text(nested(status, "imageID")) != checkpoint.Learner.ImageID {
		return fmt.Errorf("learner C0 lifecycle identity is not pinned before terminalization")
	}
	previousContainerID := text(nested(status, "containerID"))
	if err := verifyGCSMixedCheckpointContainerID(checkpoint.Learner.ContainerID, previousContainerID); err != nil {
		return fmt.Errorf("learner C0 lifecycle identity is not pinned before terminalization: %w", err)
	}
	previousRestarts := numberOrZero(nested(status, "restartCount"))
	learnerPodSpecHash := digestJSON(nested(learnerPod, "spec"))
	rootBeforeDeny, rootBeforeVersion, err := waitForGCSMixedClosedCheckpoint(chaosCtx, stateStore, source, pending.Request, expected, voterRuns)
	if err != nil {
		return fmt.Errorf("canonical generation closure checkpoint before agent restoration: %w", err)
	}
	if rootBeforeVersion == nil || rootBeforeVersion.Value == "" {
		return fmt.Errorf("canonical closure checkpoint omitted CAS version evidence")
	}
	if err := event("mixed_generation_closed_before_agent_restore", obj{"closedBy": rootBeforeDeny.Ledger.ClosedBy,
		"closedHash": rootBeforeDeny.Ledger.ClosedHash, "versionType": rootBeforeVersion.Type, "version": rootBeforeVersion.Value,
		"runCount": len(rootBeforeDeny.Ledger.Runs), "claim": "exact global closure and four captured run identities before runtime termination"}); err != nil {
		return err
	}
	if err := agentFault.Restore(chaosCtx); err != nil {
		return err
	}
	for i, current := range chaosFaults {
		if current == agentFault {
			chaosFaults[i] = nil
		}
	}
	if err := wait(chaosCtx, "runtime-agent recovers while two voters remain partitioned", func() (bool, error) {
		return true, verifyGCSControlPodSnapshots(chaosCtx, mixedAgentPods)
	}); err != nil {
		return err
	}
	denialExpected := gcsStartupDenialExpectation{Namespace: source.Namespace, StatefulSetUID: source.StatefulSetUID,
		PodUID: checkpoint.Learner.PodUID, SourceClusterID: source.ClusterID, RequestedNodeID: checkpoint.Learner.Pod,
		OldRunID: checkpoint.LearnerID.RunID, CanonicalRunIDs: []string{checkpoint.LearnerID.RunID}}
	for _, voter := range voterRuns {
		denialExpected.CanonicalRunIDs = append(denialExpected.CanonicalRunIDs, voter.Identity.RunID)
	}
	proofResult, denialResult, err := waitForGCSMixedProofAndDenial(chaosCtx,
		func(waitCtx context.Context) gcsMixedProofWaitResult {
			request, proof, observed, err := waitForGCSMixedFenceProof(waitCtx, source, pending, expected, voterRuns, stateStore)
			return gcsMixedProofWaitResult{Request: request, Proof: proof, Observed: observed, Err: err}
		},
		func(waitCtx context.Context) gcsMixedDenialWaitResult {
			log, errClosedRow, terminatedContainerID, event, postflightLogs, err := waitForGCSMixedNaturalDeniedStartup(
				waitCtx, source, authoritySource, preflightLogs, denialExpected, previousRestarts, previousContainerID, checkpoint.Learner.ImageID, learnerPodSpecHash)
			return gcsMixedDenialWaitResult{Log: log, ErrClosedRow: errClosedRow, TerminatedContainerID: terminatedContainerID,
				Event: event, PostflightLogs: postflightLogs, Err: err}
		})
	if err != nil {
		return err
	}
	denialLog, denialErrClosedRow, terminatedContainerID, denialEvent, postflightLogs := denialResult.Log, denialResult.ErrClosedRow,
		denialResult.TerminatedContainerID, denialResult.Event, denialResult.PostflightLogs
	request, proof, observed := proofResult.Request, proofResult.Proof, proofResult.Observed
	postflightLogs, err = readGCSAuthorityLogs(chaosCtx, source, authoritySource)
	if err != nil {
		return fmt.Errorf("final authority log candidate-count readback failed: %w", err)
	}
	denialEvent, denialRow, err := parseGCSStartupDenialDelta(preflightLogs, postflightLogs, denialExpected)
	if err != nil {
		return fmt.Errorf("final exact learner denial-event count changed: %w", err)
	}
	readback, err := get(chaosCtx, "rhizafence", pending.Name)
	if err != nil || !pendingFenceIdentityMatches(readback, pending) {
		return fmt.Errorf("mixed proof exact-name API readback changed fence identity: %v", err)
	}
	rootAfterProof, rootAfterVersion, err := stateStore.ReadRoot(chaosCtx, source.StatefulSetUID, source.ClusterID)
	if err != nil {
		return fmt.Errorf("canonical root readback after mixed proof: %w", err)
	}
	proofRequest, proofValue, err := verifyGCSMixedPendingGenerationProof(readback, source, expected, voterRuns,
		fenceLedgerState{StatefulSetUID: rootAfterProof.StatefulSetUID, Ledger: rootAfterProof.Ledger})
	if err != nil || !sameFenceRequest(request, proofRequest) || proof.ProofID != proofValue.ProofID {
		return fmt.Errorf("mixed proof readback changed the original request/proof: %v", err)
	}
	if err := event("mixed_generation_proof_api_readback", obj{"observed": observed, "request": request, "proof": proof}); err != nil {
		return err
	}
	rootBeforeState := fenceLedgerState{StatefulSetUID: rootBeforeDeny.StatefulSetUID, Ledger: rootBeforeDeny.Ledger}
	rootAfterState := fenceLedgerState{StatefulSetUID: rootAfterProof.StatefulSetUID, Ledger: rootAfterProof.Ledger}
	if err := verifyGCSMixedDeniedRootSafety(rootBeforeState, rootAfterState, request, proof, expected, denialEvent.AttemptedRunID); err != nil {
		return fmt.Errorf("canonical closure, terminal references, or denied-attempt safety changed: %w", err)
	}
	if rootAfterVersion == nil || !hasGCSMixedRootVersionEvidence(rootBeforeVersion, rootAfterVersion) {
		return fmt.Errorf("canonical closure/proof readback omitted CAS version evidence")
	}
	if _, exists := rootAfterProof.Ledger.Runs[denialEvent.AttemptedRunID]; exists {
		return fmt.Errorf("denied supervisor RunID became a canonical run")
	}
	oldLearnerRun, exists := rootAfterProof.Ledger.Runs[checkpoint.LearnerID.RunID]
	if !exists || oldLearnerRun.Promotion != nil || oldLearnerRun.MutationProducerUID != "" || oldLearnerRun.MutationScopeHash != "" ||
		oldLearnerRun.MutationProfile != "" || oldLearnerRun.MutationStageIncarnation != "" {
		return fmt.Errorf("post-denial canonical learner admission or producer state changed")
	}
	if err := verifyGCSMixedLearnerProducerAbsent(chaosCtx, readBucket, readScope.Prefix, oldLearnerRun); err != nil {
		return fmt.Errorf("post-denial deterministic learner producer readback is not NotFound: %w", err)
	}
	denialLogDigest := sha256.Sum256(denialLog)
	if err := persistGCSStartupDenialEvidence(out, denialRow, denialErrClosedRow, denialEvent, authoritySource, terminatedContainerID,
		rootBeforeState, rootAfterState, rootBeforeVersion, rootAfterVersion); err != nil {
		return fmt.Errorf("preserve correlated startup denial evidence: %w", err)
	}
	if err := event("closed_generation_learner_restart_denied", obj{"podUID": checkpoint.Learner.PodUID, "oldRunID": checkpoint.LearnerID.RunID,
		"attemptRunID": denialEvent.AttemptedRunID, "stage": denialEvent.Stage, "grantResolved": denialEvent.GrantResolved,
		"c0ContainerID": previousContainerID, "c1TerminatedContainerID": terminatedContainerID,
		"previousLogSHA256": hex.EncodeToString(denialLogDigest[:]), "result": fencing.ErrClosed.Error()}); err != nil {
		return err
	}
	if err := operatorFault.Restore(chaosCtx); err != nil {
		return err
	}
	for i, current := range chaosFaults {
		if current == operatorFault {
			chaosFaults[i] = nil
		}
	}
	if err := cohortFault.Restore(chaosCtx); err != nil {
		return err
	}
	for i, current := range chaosFaults {
		if current == cohortFault {
			chaosFaults[i] = nil
		}
	}
	if fault != nil {
		if err := fault.Restore(chaosCtx); err != nil {
			return fmt.Errorf("restore owned source PodChaos after proof and denial: %w", err)
		}
		fault = nil
	}
	if err := event("mixed_generation_proof_and_denial_complete", obj{"fence": pending, "request": request, "proof": proof,
		"attemptRunID": denialEvent.AttemptedRunID, "claim": "proof and natural early denial both captured before fault restoration"}); err != nil {
		return err
	}
	const expectedTwoRows = "[[1,\"gcs-mixed-pending-baseline\"],[2,\"gcs-mixed-pending-after-remove\"]]"
	outcome, err := waitForGCSRecoveryAfterProof(chaosCtx, source, pending, request, proof, "fork", expectedTwoRows, out, nil)
	if err != nil {
		return fmt.Errorf("automatic mixed target completion/readback: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, outcome.Forward.close()) }()
	thirdAck, err := writeGCSAcknowledgedRow(chaosCtx, outcome.Forward, outcome.Admin, 3, "gcs-mixed-pending-new-target")
	if err != nil {
		return fmt.Errorf("new target ACK after automatic mixed recovery: %w", err)
	}
	finalRows, err := call(chaosCtx, outcome.Forward, "/sql/query", obj{"sql": "SELECT id,value FROM fencing_items ORDER BY id", "consistency": "linearizable"}, outcome.Admin)
	if err != nil || string(encoded(finalRows["rows"])) != "[[1,\"gcs-mixed-pending-baseline\"],[2,\"gcs-mixed-pending-after-remove\"],[3,\"gcs-mixed-pending-new-target\"]]" {
		return fmt.Errorf("recovered target final three-row linearizable readback differs: %v", err)
	}
	if err := event("mixed_target_third_ack_and_readback", obj{"ack": thirdAck, "rows": finalRows["rows"], "target": outcome.Recovery.Status.Target}); err != nil {
		return err
	}
	rootFinal, finalVersion, err := stateStore.ReadRoot(chaosCtx, source.StatefulSetUID, source.ClusterID)
	if err != nil || finalVersion == nil || finalVersion.Value == "" {
		return fmt.Errorf("final canonical source root closure readback failed")
	}
	if err := verifyGCSMixedDeniedRootSafety(rootBeforeState, fenceLedgerState{StatefulSetUID: rootFinal.StatefulSetUID, Ledger: rootFinal.Ledger},
		request, proof, expected, denialEvent.AttemptedRunID); err != nil {
		return fmt.Errorf("final canonical closure/reference/no-admission readback changed: %w", err)
	}
	if _, exists := rootFinal.Ledger.Runs[denialEvent.AttemptedRunID]; exists {
		return fmt.Errorf("denied attempt was admitted after target recovery")
	}
	if err := event("mixed_final_canonical_readback", obj{"closedBy": rootFinal.Ledger.ClosedBy, "closedHash": rootFinal.Ledger.ClosedHash,
		"versionType": finalVersion.Type, "version": finalVersion.Value, "attemptRunIDAbsent": true}); err != nil {
		return err
	}
	return nil
}
