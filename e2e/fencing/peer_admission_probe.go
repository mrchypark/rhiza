package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

const (
	peerAdmissionProbeMaxInput  = 256 << 10
	peerAdmissionProbeMaxOutput = 1024
	peerAdmissionProbeTimeout   = 30 * time.Second
	peerAdmissionProbeBinary    = "/usr/local/bin/fencing-e2e"
)

type peerAdmissionCredential struct {
	Member        quepaxa.Member `json:"member"`
	Token         string         `json:"token"`
	PodUID        string         `json:"podUID"`
	SecretName    string         `json:"secretName"`
	SecretUID     string         `json:"secretUID"`
	ContainerID   string         `json:"containerID"`
	CredentialUID string         `json:"credentialUID"`
}

type peerAdmissionProbeRequest struct {
	ClusterID string                    `json:"clusterID"`
	ConfigID  uint                      `json:"configID"`
	Current   []peerAdmissionCredential `json:"current"`
	Previous  []peerAdmissionCredential `json:"previous"`
}

type peerAdmissionProbeResult struct {
	Schema             string `json:"schema"`
	ConfigID           uint   `json:"configID"`
	CurrentSuccesses   int    `json:"currentSuccesses"`
	PreviousRejections int    `json:"previousRejections"`
}

func runPeerAdmissionProbeCommand() error {
	ctx, cancel := context.WithTimeout(context.Background(), peerAdmissionProbeTimeout)
	defer cancel()
	request, err := decodePeerAdmissionProbeRequest(os.Stdin)
	if err != nil {
		return err
	}
	result, err := runPeerAdmissionProbe(ctx, request)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}

func decodePeerAdmissionProbeRequest(r io.Reader) (peerAdmissionProbeRequest, error) {
	var request peerAdmissionProbeRequest
	raw, err := io.ReadAll(io.LimitReader(r, peerAdmissionProbeMaxInput+1))
	if err != nil || len(raw) > peerAdmissionProbeMaxInput {
		return request, fmt.Errorf("peer-admission probe request is oversized or unreadable")
	}
	if err := rejectDuplicateJSONKeys(raw); err != nil {
		return request, fmt.Errorf("invalid peer-admission probe request")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return request, fmt.Errorf("invalid peer-admission probe request")
	}
	if decoder.InputOffset() > peerAdmissionProbeMaxInput {
		return request, fmt.Errorf("peer-admission probe request is oversized")
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return request, fmt.Errorf("peer-admission probe request must contain one JSON value")
	}
	return request, nil
}

func rejectDuplicateJSONKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	var consume func(json.Token) error
	consume = func(token json.Token) error {
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := make(map[string]struct{})
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok {
					return fmt.Errorf("object key is not a string")
				}
				if _, exists := seen[key]; exists {
					return fmt.Errorf("duplicate object key")
				}
				seen[key] = struct{}{}
				value, err := decoder.Token()
				if err != nil {
					return err
				}
				if err := consume(value); err != nil {
					return err
				}
			}
			closing, err := decoder.Token()
			if err != nil || closing != json.Delim('}') {
				return fmt.Errorf("unterminated object")
			}
		case '[':
			for decoder.More() {
				value, err := decoder.Token()
				if err != nil {
					return err
				}
				if err := consume(value); err != nil {
					return err
				}
			}
			closing, err := decoder.Token()
			if err != nil || closing != json.Delim(']') {
				return fmt.Errorf("unterminated array")
			}
		default:
			return fmt.Errorf("unexpected delimiter")
		}
		return nil
	}
	first, err := decoder.Token()
	if err != nil {
		return err
	}
	if err := consume(first); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("trailing JSON value")
	}
	return nil
}

func runPeerAdmissionProbe(ctx context.Context, request peerAdmissionProbeRequest) (peerAdmissionProbeResult, error) {
	result := peerAdmissionProbeResult{Schema: "rhiza-peer-admission-v1", ConfigID: request.ConfigID}
	if request.ClusterID == "" || request.ConfigID == 0 || len(request.Current) != 3 || len(request.Previous) == 0 {
		return result, fmt.Errorf("peer-admission probe requires one three-voter snapshot and prior identities")
	}
	members := make([]quepaxa.Member, 0, len(request.Current))
	activeKeys := make(map[string]bool, len(request.Current))
	currentByID := make(map[quepaxa.NodeID]bool, len(request.Current))
	previousByID := make(map[quepaxa.NodeID]bool, len(request.Previous))
	for _, credential := range request.Current {
		if err := validatePeerAdmissionCredential(request.ClusterID, credential); err != nil {
			return result, fmt.Errorf("current peer identity binding is invalid")
		}
		id := credential.Member.ID
		if currentByID[id] {
			return result, fmt.Errorf("current membership contains duplicate peer identities")
		}
		currentByID[id] = true
		activeKeys[string(credential.Member.PublicKey[:])] = true
		members = append(members, credential.Member)
	}
	for _, credential := range request.Previous {
		if currentByID[credential.Member.ID] {
			return result, fmt.Errorf("previous peer identity overlaps current membership")
		}
		if previousByID[credential.Member.ID] {
			return result, fmt.Errorf("previous membership contains duplicate peer identities")
		}
		previousByID[credential.Member.ID] = true
		if err := validatePeerAdmissionCredential(request.ClusterID, credential); err != nil {
			return result, fmt.Errorf("previous peer identity binding is invalid")
		}
		for _, active := range request.Current {
			derived := network.PeerPublicKey(types.ClusterID(request.ClusterID), active.Member.ID, credential.Token)
			if activeKeys[string(derived)] {
				return result, fmt.Errorf("previous credential aliases a current voter identity")
			}
		}
	}
	cluster := &quepaxa.Cluster{ConfigID: request.ConfigID, Members: members}
	for _, sender := range request.Current {
		transport := network.NewTransport(types.ClusterID(request.ClusterID), sender.Member.ID, cluster, sender.Token)
		for _, receiver := range request.Current {
			callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			_, err := transport.ReadTip(callCtx, receiver.Member.ID)
			cancel()
			if err != nil {
				_ = transport.Close()
				return result, fmt.Errorf("current voter peer-admission positive control failed")
			}
			result.CurrentSuccesses++
		}
		if err := transport.Close(); err != nil {
			return result, fmt.Errorf("current voter peer transport cleanup failed")
		}
	}
	for _, sender := range request.Previous {
		transport := network.NewTransport(types.ClusterID(request.ClusterID), sender.Member.ID, cluster, sender.Token)
		for _, receiver := range request.Current {
			callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			_, err := transport.ReadTip(callCtx, receiver.Member.ID)
			cancel()
			if !isExactPeerAuthenticationDenial(err) {
				_ = transport.Close()
				return result, fmt.Errorf("previous voter credential did not receive the exact peer-authentication denial (classification %s)", peerAdmissionErrorClass(err))
			}
			result.PreviousRejections++
		}
		if err := transport.Close(); err != nil {
			return result, fmt.Errorf("previous voter peer transport cleanup failed")
		}
	}
	return result, nil
}

func peerAdmissionErrorClass(err error) string {
	if err == nil {
		return "accepted"
	}
	message := err.Error()
	switch {
	case isExactPeerAuthenticationDenial(err):
		return "peer_authentication_denied"
	case strings.Contains(message, "cluster identity mismatch"):
		return "cluster_identity_mismatch"
	case strings.Contains(message, "read-index source identity mismatch"):
		return "response_identity_mismatch"
	default:
		return "other_transport_or_protocol_error"
	}
}

func isExactPeerAuthenticationDenial(err error) bool {
	if err == nil {
		return false
	}
	// Transport wraps a decoded peer response in a phase error, then wraps
	// that in its private peer-rejection sentinel. Match that exact known
	// wrapper shape and denial payload; suffix/prefix-only matching can mistake
	// a timeout or unrelated protocol error for an authentication decision.
	sentinel := errors.Unwrap(err)
	if sentinel == nil || sentinel.Error() != "peer rejected request" {
		return false
	}
	const (
		prefix = "peer rejected request: peer RPC phase=response-decode elapsed="
		middle = " cause_type=*errors.errorString: peer rejected request: peer authentication failed"
	)
	message := err.Error()
	if !strings.HasPrefix(message, prefix) || !strings.HasSuffix(message, middle) {
		return false
	}
	elapsed := strings.TrimSuffix(strings.TrimPrefix(message, prefix), middle)
	if elapsed == "" {
		return false
	}
	_, parseErr := time.ParseDuration(elapsed)
	return parseErr == nil
}

func validatePeerAdmissionCredential(clusterID string, credential peerAdmissionCredential) error {
	member := credential.Member
	addr := member.PeerURL
	if addr == "" {
		addr = member.URL
	}
	parsed, err := net.ResolveUDPAddr("udp", strings.TrimPrefix(addr, "quic://"))
	if err != nil || parsed == nil || clusterID == "" || member.ID == "" || member.PublicKey == (quepaxa.PublicKey{}) ||
		member.WALIdentity == "" || credential.Token == "" || credential.PodUID == "" || credential.SecretName == "" ||
		credential.SecretUID == "" || credential.ContainerID == "" || credential.CredentialUID == "" {
		return fmt.Errorf("incomplete identity")
	}
	derived := network.PeerPublicKey(types.ClusterID(clusterID), member.ID, credential.Token)
	if !bytes.Equal(derived, member.PublicKey[:]) {
		return fmt.Errorf("credential mismatch")
	}
	return nil
}

type peerProbePodPin struct {
	Name          string
	UID           string
	Deployment    string
	ReplicaSet    string
	ReplicaSetUID string
	ContainerID   string
	ImageID       string
	RestartCount  float64
}

func capturePeerProbePodPin(ctx context.Context, owned runOwnedOperator, name string) (peerProbePodPin, error) {
	var pin peerProbePodPin
	if owned.Name != "rhiza-fencer" || owned.UID == "" || len(owned.Pods) != 1 || owned.Pods[0].Name != name {
		return pin, fmt.Errorf("peer probe requires the captured single run-owned fencer Pod")
	}
	pod, err := get(ctx, "pod", name)
	if err != nil || text(nested(pod, "metadata", "uid")) != owned.Pods[0].UID || !podReady(pod) || nested(pod, "metadata", "deletionTimestamp") != nil {
		return pin, fmt.Errorf("captured fencer Pod is not the same Ready incarnation: %v", err)
	}
	deployment, err := get(ctx, "deployment", owned.Name)
	if err != nil || text(nested(deployment, "metadata", "uid")) != owned.UID || integer(nested(deployment, "spec", "replicas")) != 1 ||
		!deploymentSelectorMatches(deployment, owned.Selector) || !peerProbeLabelsMatch(nested(pod, "metadata", "labels"), owned.Selector) {
		return pin, fmt.Errorf("captured fencer Deployment identity or Pod selector changed")
	}
	sets, err := get(ctx, "replicasets", "")
	if err != nil {
		return pin, fmt.Errorf("captured fencer ReplicaSets are unavailable")
	}
	items, _ := sets["items"].([]any)
	rsName, rsUID, err := peerProbePodReplicaSet(pod, items, owned.Name, owned.UID)
	if err != nil {
		return pin, err
	}
	container := namedContainerStatus(pod, "authority")
	if container == nil || nested(container, "ready") != true {
		return pin, fmt.Errorf("captured fencer authority container is not Ready")
	}
	containerID, imageID := text(nested(container, "containerID")), text(nested(container, "imageID"))
	if containerID == "" || imageID == "" {
		return pin, fmt.Errorf("captured fencer authority container identity is incomplete")
	}
	return peerProbePodPin{Name: name, UID: owned.Pods[0].UID, Deployment: owned.UID, ReplicaSet: rsName, ReplicaSetUID: rsUID, ContainerID: containerID, ImageID: imageID,
		RestartCount: numberOrZero(nested(container, "restartCount"))}, nil
}

func peerProbeLabelsMatch(value any, required map[string]string) bool {
	labels, _ := value.(map[string]any)
	if len(required) == 0 {
		return false
	}
	for key, want := range required {
		if text(labels[key]) != want {
			return false
		}
	}
	return true
}

func peerProbePodReplicaSet(pod any, replicaSets []any, deploymentName, deploymentUID string) (string, string, error) {
	owner, ok := peerProbeControllerOwner(pod)
	if !ok || text(nested(owner, "kind")) != "ReplicaSet" {
		return "", "", fmt.Errorf("captured fencer Pod has no unique ReplicaSet controller")
	}
	podRSName, podRSUID := text(nested(owner, "name")), text(nested(owner, "uid"))
	if podRSName == "" || podRSUID == "" {
		return "", "", fmt.Errorf("captured fencer Pod ReplicaSet identity is incomplete")
	}
	var found bool
	for _, rs := range replicaSets {
		if text(nested(rs, "metadata", "name")) != podRSName || text(nested(rs, "metadata", "uid")) != podRSUID {
			continue
		}
		if found {
			return "", "", fmt.Errorf("captured fencer Pod ReplicaSet identity is ambiguous")
		}
		found = true
		if !peerProbeReplicaSetOwnedByDeployment(rs, deploymentName, deploymentUID) {
			return "", "", fmt.Errorf("captured fencer ReplicaSet is not controlled by the captured Deployment")
		}
	}
	if !found {
		return "", "", fmt.Errorf("captured fencer Pod ReplicaSet is unavailable")
	}
	return podRSName, podRSUID, nil
}

func peerProbeControllerOwner(resource any) (any, bool) {
	var controller any
	for _, owner := range ownerRefs(resource) {
		if nested(owner, "controller") != true {
			continue
		}
		if controller != nil {
			return nil, false
		}
		controller = owner
	}
	return controller, controller != nil
}

func peerProbeReplicaSetOwnedByDeployment(rs any, deploymentName, deploymentUID string) bool {
	if deploymentName == "" || deploymentUID == "" || text(nested(rs, "metadata", "name")) == "" || text(nested(rs, "metadata", "uid")) == "" {
		return false
	}
	owner, ok := peerProbeControllerOwner(rs)
	return ok && text(nested(owner, "kind")) == "Deployment" && text(nested(owner, "name")) == deploymentName && text(nested(owner, "uid")) == deploymentUID
}

func verifyPeerProbePodPin(ctx context.Context, before peerProbePodPin, owned runOwnedOperator) error {
	after, err := capturePeerProbePodPin(ctx, owned, before.Name)
	if err != nil {
		return err
	}
	if after != before {
		return fmt.Errorf("fencer Pod or authority container changed during peer-admission probe")
	}
	return nil
}

func executePeerAdmissionProbe(ctx context.Context, owned runOwnedOperator, request peerAdmissionProbeRequest) (peerAdmissionProbeResult, error) {
	var result peerAdmissionProbeResult
	if ctx == nil || ctx.Err() != nil {
		return result, fmt.Errorf("peer-admission probe context is unavailable")
	}
	if len(owned.Pods) != 1 {
		return result, fmt.Errorf("peer-admission probe requires exactly one captured fencer Pod")
	}
	if err := verifyRunOwnedDeploymentReady(ctx, owned); err != nil {
		return result, fmt.Errorf("run-owned fencer is not Ready before peer-admission probe")
	}
	before, err := capturePeerProbePodPin(ctx, owned, owned.Pods[0].Name)
	if err != nil {
		return result, err
	}
	input, err := json.Marshal(request)
	if err != nil || len(input) > peerAdmissionProbeMaxInput {
		return result, fmt.Errorf("peer-admission probe request is invalid or oversized")
	}
	commandCtx, cancel := context.WithTimeout(ctx, peerAdmissionProbeTimeout)
	defer cancel()
	cmd := exec.CommandContext(commandCtx, "kubectl", "--context", kubeContext, "-n", namespace, "exec", "-i", owned.Pods[0].Name,
		"-c", "authority", "--", peerAdmissionProbeBinary, "peer-admission-probe")
	cmd.Stdin = bytes.NewReader(input)
	var output boundedPeerProbeOutput
	cmd.Stdout = &output
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		return result, fmt.Errorf("pinned fencer peer-admission probe command failed")
	}
	if err := verifyPeerProbePodPin(ctx, before, owned); err != nil {
		return result, err
	}
	if err := verifyRunOwnedDeploymentReady(ctx, owned); err != nil {
		return result, fmt.Errorf("run-owned fencer changed during peer-admission probe")
	}
	if err := rejectDuplicateJSONKeys(output.Bytes()); err != nil {
		return peerAdmissionProbeResult{}, fmt.Errorf("pinned fencer returned duplicate JSON keys")
	}
	decoder := json.NewDecoder(bytes.NewReader(output.Bytes()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil || result.Schema != "rhiza-peer-admission-v1" || result.ConfigID != request.ConfigID ||
		result.CurrentSuccesses != len(request.Current)*len(request.Current) || result.PreviousRejections != len(request.Previous)*len(request.Current) {
		return peerAdmissionProbeResult{}, fmt.Errorf("pinned fencer returned an invalid peer-admission result")
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return peerAdmissionProbeResult{}, fmt.Errorf("pinned fencer returned trailing peer-admission output")
	}
	return result, nil
}

type boundedPeerProbeOutput struct{ bytes.Buffer }

func (output *boundedPeerProbeOutput) Write(p []byte) (int, error) {
	if output.Len()+len(p) > peerAdmissionProbeMaxOutput {
		return 0, fmt.Errorf("peer-admission probe output limit exceeded")
	}
	return output.Buffer.Write(p)
}

func peerAdmissionCredentialsForTargets(ctx context.Context, source gcsRecoverySource, targets []operator.FenceTarget) ([]peerAdmissionCredential, error) {
	if len(targets) != 3 {
		return nil, fmt.Errorf("peer-admission snapshot requires three captured voter targets")
	}
	credentials := make([]peerAdmissionCredential, 0, len(targets))
	var canonicalMembers []quepaxa.Member
	for _, target := range targets {
		pod, err := get(ctx, "pod", target.Pod)
		if err != nil || text(nested(pod, "metadata", "uid")) != target.PodUID || !gcsPodOwnedBy(pod, source.StatefulSetUID) || nested(pod, "metadata", "deletionTimestamp") != nil {
			return nil, fmt.Errorf("peer-admission voter Pod is not the captured live StatefulSet incarnation")
		}
		membersValue, membersName, membersUID, err := podCredentialValue(ctx, pod, source.Container, "RHIZA_CLUSTER_MEMBERS")
		if err != nil {
			return nil, fmt.Errorf("peer-admission voter membership credential is unavailable")
		}
		var members []quepaxa.Member
		if err := json.Unmarshal([]byte(membersValue), &members); err != nil || len(members) != 3 {
			return nil, fmt.Errorf("peer-admission voter membership credential is invalid")
		}
		if canonicalMembers == nil {
			canonicalMembers = members
		} else if !samePeerAdmissionMembers(canonicalMembers, members) {
			return nil, fmt.Errorf("peer-admission voter Pods do not share one public membership snapshot")
		}
		member, ok := peerAdmissionMember(canonicalMembers, quepaxa.NodeID(target.NodeID))
		if !ok {
			return nil, fmt.Errorf("peer-admission snapshot is missing a captured voter identity")
		}
		member.WALIdentity = target.WALIdentity
		token, tokenName, tokenUID, err := podPeerToken(ctx, pod, source.Container, member.ID)
		if err != nil {
			return nil, fmt.Errorf("peer-admission voter token is unavailable")
		}
		credentialUID := text(nested(pod, "metadata", "annotations", "rhiza.mrchypark.dev/fencing-credential-uid"))
		if credentialUID == "" || tokenUID != credentialUID || membersName == "" || membersUID == "" || tokenName == "" {
			return nil, fmt.Errorf("peer-admission credential UID is not bound to the Pod")
		}
		container := namedContainerStatus(pod, source.Container)
		containerID := text(nested(container, "containerID"))
		credential := peerAdmissionCredential{Member: member, Token: token, PodUID: target.PodUID, SecretName: tokenName,
			SecretUID: tokenUID, ContainerID: containerID, CredentialUID: credentialUID}
		if err := validatePeerAdmissionCredential(source.ClusterID, credential); err != nil {
			return nil, fmt.Errorf("peer-admission voter credential does not match its public identity")
		}
		credentials = append(credentials, credential)
	}
	sort.Slice(credentials, func(i, j int) bool { return credentials[i].Member.ID < credentials[j].Member.ID })
	return credentials, nil
}

var errPeerCredentialMissing = errors.New("credential Secret reference is missing")

func podCredentialValue(ctx context.Context, pod obj, containerName, envName string) (value, secretName, secretUID string, err error) {
	containerList, _ := nested(pod, "spec", "containers").([]any)
	var app any
	for _, candidate := range containerList {
		if nested(candidate, "name") == containerName {
			if app != nil {
				return "", "", "", fmt.Errorf("ambiguous application container")
			}
			app = candidate
		}
	}
	if app == nil {
		return "", "", "", fmt.Errorf("application container is missing")
	}
	var key string
	found := false
	envs, _ := nested(app, "env").([]any)
	for _, entry := range envs {
		if nested(entry, "name") == envName {
			if found || nested(entry, "value") != nil {
				return "", "", "", fmt.Errorf("credential is not secret-backed")
			}
			key = text(nested(entry, "valueFrom", "secretKeyRef", "key"))
			secretName = text(nested(entry, "valueFrom", "secretKeyRef", "name"))
			found = true
			if key == "" || secretName == "" {
				return "", "", "", fmt.Errorf("credential Secret reference is malformed")
			}
		}
	}
	if key == "" {
		refs, _ := nested(app, "envFrom").([]any)
		for _, ref := range refs {
			if nested(ref, "configMapRef") != nil {
				return "", "", "", fmt.Errorf("credential envFrom ConfigMap is unsupported")
			}
			prefix := text(nested(ref, "prefix"))
			if !strings.HasPrefix(envName, prefix) {
				continue
			}
			dataKey := strings.TrimPrefix(envName, prefix)
			name := text(nested(ref, "secretRef", "name"))
			if name == "" {
				continue
			}
			secret, getErr := get(ctx, "secret", name)
			if getErr != nil {
				return "", "", "", getErr
			}
			_, ok := nested(secret, "data", dataKey).(string)
			if !ok {
				continue
			}
			if key != "" {
				return "", "", "", fmt.Errorf("competing credential Secret sources")
			}
			key, secretName = dataKey, name
		}
	}
	if key == "" || secretName == "" {
		return "", "", "", errPeerCredentialMissing
	}
	secret, getErr := get(ctx, "secret", secretName)
	if getErr != nil || secret["immutable"] != true || text(nested(secret, "metadata", "namespace")) != namespace {
		return "", "", "", fmt.Errorf("credential Secret is not immutable in the captured namespace")
	}
	raw, decodeErr := base64.StdEncoding.DecodeString(text(nested(secret, "data", key)))
	if decodeErr != nil || len(raw) == 0 || len(raw) > peerAdmissionProbeMaxInput {
		return "", "", "", fmt.Errorf("credential Secret value is invalid")
	}
	return string(raw), secretName, text(nested(secret, "metadata", "uid")), nil
}

func podPeerToken(ctx context.Context, pod obj, containerName string, nodeID quepaxa.NodeID) (token, secretName, secretUID string, err error) {
	if value, name, uid, readErr := podCredentialValue(ctx, pod, containerName, "RHIZA_PEER_TOKEN"); readErr == nil {
		return value, name, uid, nil
	} else if !errors.Is(readErr, errPeerCredentialMissing) {
		return "", "", "", readErr
	}
	value, name, uid, err := podCredentialValue(ctx, pod, containerName, "RHIZA_PEER_TOKENS")
	if err != nil {
		return "", "", "", err
	}
	var tokens map[string]string
	if json.Unmarshal([]byte(value), &tokens) != nil || len(tokens) == 0 || tokens[string(nodeID)] == "" {
		return "", "", "", fmt.Errorf("token map does not contain this voter")
	}
	return tokens[string(nodeID)], name, uid, nil
}

func peerAdmissionMember(members []quepaxa.Member, id quepaxa.NodeID) (quepaxa.Member, bool) {
	var found quepaxa.Member
	count := 0
	for _, member := range members {
		if member.ID == id {
			found, count = member, count+1
		}
	}
	return found, count == 1
}

func samePeerAdmissionMembers(a, b []quepaxa.Member) bool {
	if len(a) != len(b) {
		return false
	}
	left := append([]quepaxa.Member(nil), a...)
	right := append([]quepaxa.Member(nil), b...)
	sort.Slice(left, func(i, j int) bool { return left[i].ID < left[j].ID })
	sort.Slice(right, func(i, j int) bool { return right[i].ID < right[j].ID })
	for i := range left {
		if left[i].ID != right[i].ID || left[i].PublicKey != right[i].PublicKey ||
			left[i].PeerURL != right[i].PeerURL || left[i].URL != right[i].URL {
			return false
		}
	}
	return true
}

func credentialFromSecret(ctx context.Context, name, expectedUID, podUID, nodeID, clusterID string, member quepaxa.Member, key string) (peerAdmissionCredential, error) {
	var credential peerAdmissionCredential
	secret, err := get(ctx, "secret", name)
	if err != nil || secret["immutable"] != true || text(nested(secret, "metadata", "namespace")) != namespace ||
		text(nested(secret, "metadata", "uid")) != expectedUID {
		return credential, fmt.Errorf("peer credential Secret is not the captured immutable object")
	}
	raw, err := base64.StdEncoding.DecodeString(text(nested(secret, "data", key)))
	if err != nil || len(raw) == 0 || len(raw) > 4096 {
		return credential, fmt.Errorf("peer credential is unavailable")
	}
	token := string(raw)
	credential = peerAdmissionCredential{Member: member, Token: token, PodUID: podUID, SecretName: name, SecretUID: expectedUID,
		CredentialUID: expectedUID}
	if member.ID != quepaxa.NodeID(nodeID) || validatePeerAdmissionCredential(clusterID, credential) != nil {
		return peerAdmissionCredential{}, fmt.Errorf("peer credential does not match the captured voter")
	}
	return credential, nil
}
