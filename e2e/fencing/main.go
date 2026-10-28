// Command fencing-e2e drives a disposable Kind qualification of the real
// cooperative executor. Runtime inspection below is a fault-injector oracle;
// it is never supplied to the executor as a fencing proof.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	objmetrics "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/mrchypark/rhiza/pkg/recoveryanchor"
)

const namespace = "rhiza-fencing-e2e"
const kubeContext = "kind-rhiza-generic-fencing"
const kindNode = "rhiza-generic-fencing-control-plane"
const chaosName = "rhiza-initial-replacement"
const chaosTargetLabel = "rhiza.mrchypark.dev/chaos-target"
const sourceArchiveConfigID uint = 1
const recoveryAnchorID = "fencing-e2e"
const recoveryAnchorConfigMap = "rhiza-anchor-fencing-e2e"

type obj = map[string]any

func main() {
	if len(os.Args) == 2 && os.Args[1] == "capture-runtime-diagnostics" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		fatal(captureRuntimeDiagnostics(ctx, os.Getenv("FENCING_E2E_OUT")))
		return
	}
	if len(os.Args) == 3 && os.Args[1] == "peer-probe" {
		fatal(runPeerProbe(os.Args[2]))
		return
	}
	if len(os.Args) != 2 {
		fatal(fmt.Errorf("usage: fencing-e2e manifests|wait-source|run|capture-runtime-diagnostics|peer-probe <control|stale>"))
	}
	if os.Args[1] == "manifests" {
		fatal(json.NewEncoder(os.Stdout).Encode(manifests()))
		return
	}
	if os.Args[1] == "wait-source" {
		ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
		defer cancel()
		fatal(waitSource(ctx))
		return
	}
	if os.Args[1] == "safe-artifacts" {
		fatal(safeArtifacts(os.Getenv("FENCING_E2E_OUT")))
		return
	}
	if os.Args[1] != "run" {
		fatal(fmt.Errorf("unknown command"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	fatal(qualify(ctx))
}

func captureRuntimeDiagnostics(ctx context.Context, out string) error {
	if out == "" {
		return fmt.Errorf("FENCING_E2E_OUT is required")
	}
	data, err := kubectl(ctx, "get", "pods", "-l", "app.kubernetes.io/name=rhiza", "-o", "json")
	if err != nil {
		return err
	}
	var response obj
	if err := json.Unmarshal(data, &response); err != nil {
		return fmt.Errorf("decode source Pod diagnostics: %w", err)
	}
	items, _ := response["items"].([]any)
	snapshot := runtimeDiagnosticsView{CapturedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	for _, raw := range items {
		name := text(nested(raw, "metadata", "name"))
		uid := text(nested(raw, "metadata", "uid"))
		if name == "" || uid == "" {
			return fmt.Errorf("source Pod diagnostics lack identity")
		}
		pod := runtimePodView{
			Name:      name,
			UID:       uid,
			NodeName:  text(nested(raw, "spec", "nodeName")),
			CreatedAt: text(nested(raw, "metadata", "creationTimestamp")),
			Phase:     text(nested(raw, "status", "phase")),
		}
		for _, condition := range listAny(nested(raw, "status", "conditions")) {
			pod.Conditions = append(pod.Conditions, runtimeConditionView{
				Type: text(nested(condition, "type")), Status: text(nested(condition, "status")), Reason: text(nested(condition, "reason")),
			})
		}
		for _, field := range []string{"initContainerStatuses", "containerStatuses"} {
			for _, status := range listAny(nested(raw, "status", field)) {
				container := runtimeContainerView{
					Name: text(nested(status, "name")), Ready: nested(status, "ready") == true,
				}
				if count, ok := nested(status, "restartCount").(float64); ok {
					container.RestartCount = int64(count)
				}
				for _, state := range []string{"waiting", "running", "terminated"} {
					if value := nested(status, "state", state); value != nil {
						container.State = state
						container.Reason = text(nested(value, "reason"))
						if code, ok := nested(value, "exitCode").(float64); ok {
							exitCode := int64(code)
							container.ExitCode = &exitCode
						}
						break
					}
				}
				pod.Containers = append(pod.Containers, container)
			}
		}
		// Retain only bounded, non-content summaries; raw log text never enters
		// the public evidence packet.
		for _, container := range pod.Containers {
			logData, logErr := kubectl(ctx, "logs", name, "-c", container.Name, "--tail=200", "--limit-bytes=65536")
			if logErr != nil {
				pod.Logs = append(pod.Logs, runtimeLogSummaryView{Container: container.Name})
				continue
			}
			lines := 0
			if len(bytes.TrimSpace(logData)) > 0 {
				lines = bytes.Count(logData, []byte{'\n'}) + 1
			}
			lower := strings.ToLower(string(logData))
			signals := make([]string, 0, 6)
			for _, signal := range []string{"error", "failed", "refused", "ready", "listening", "bootstrap", "membership", "restore"} {
				if strings.Contains(lower, signal) {
					signals = append(signals, signal)
				}
			}
			pod.Logs = append(pod.Logs, runtimeLogSummaryView{
				Container: container.Name, Available: true, LineCount: lines, SHA256: fmt.Sprintf("%x", sha256.Sum256(logData)), Signals: signals,
			})
		}
		snapshot.Pods = append(snapshot.Pods, pod)
	}
	snapshot.PodCount = len(snapshot.Pods)
	encodedSnapshot, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "runtime-diagnostics.json"), encodedSnapshot, 0600); err != nil {
		return err
	}
	return nil
}

type peerProbeResult struct {
	Mode                 string `json:"mode"`
	ClusterID            string `json:"clusterID"`
	SenderNodeID         string `json:"senderNodeID"`
	SenderPublicKey      string `json:"senderPublicKey"`
	DestinationNodeID    string `json:"destinationNodeID"`
	DestinationPeerURL   string `json:"destinationPeerURL"`
	DestinationPublicKey string `json:"destinationPublicKey"`
	ConfigID             uint   `json:"configID"`
	BaselineTip          uint64 `json:"baselineTip,omitempty"`
	ReadTipVerified      bool   `json:"readTipVerified,omitempty"`
	RequestID            string `json:"requestID"`
	ValueSHA256          string `json:"valueSHA256"`
	MarkerSHA256         string `json:"markerSHA256"`
	Accepted             bool   `json:"accepted"`
	ErrorPhase           string `json:"errorPhase,omitempty"`
	ErrorElapsed         string `json:"errorElapsed,omitempty"`
	ErrorCauseType       string `json:"errorCauseType,omitempty"`
	ProbeJobName         string `json:"probeJobName,omitempty"`
	ProbeJobUID          string `json:"probeJobUID,omitempty"`
	ProbeSecretName      string `json:"probeSecretName,omitempty"`
	ProbeSecretUID       string `json:"probeSecretUID,omitempty"`
	CleanupVerified      bool   `json:"cleanupVerified"`
}

type oldPeerIdentity struct {
	PodName              string `json:"podName"`
	PodUID               string `json:"podUID"`
	NodeName             string `json:"nodeName"`
	CreatedAt            string `json:"createdAt"`
	NodeID               string `json:"nodeID"`
	WALIdentity          string `json:"walIdentity"`
	ConfigID             uint   `json:"configID"`
	PublicKey            string `json:"publicKey"`
	CredentialSecretName string `json:"credentialSecretName"`
	CredentialSecretUID  string `json:"credentialSecretUID"`
}

// runPeerProbe is a credential-minimal, actual-transport probe. It never opens
// a node data directory or starts a Rhiza process; only a scalar peer token and
// public member records are supplied by the run-owned Job.
func runPeerProbe(mode string) error {
	if mode != "control" && mode != "stale" {
		return fmt.Errorf("peer probe mode must be control or stale")
	}
	clusterID := os.Getenv("FENCING_CLUSTER_ID")
	senderID := quepaxa.NodeID(os.Getenv("FENCING_SENDER_NODE_ID"))
	destinationID := quepaxa.NodeID(os.Getenv("FENCING_DESTINATION_NODE_ID"))
	token := os.Getenv("FENCING_PEER_TOKEN")
	requestID := os.Getenv("FENCING_REQUEST_ID")
	marker := os.Getenv("FENCING_MARKER")
	configID, err := strconv.ParseUint(os.Getenv("FENCING_CONFIG_ID"), 10, 64)
	if err != nil || configID == 0 || clusterID == "" || senderID == "" || destinationID == "" || token == "" || requestID == "" || marker == "" {
		return fmt.Errorf("peer probe identity or request input is incomplete")
	}
	var members []quepaxa.Member
	if err := json.Unmarshal([]byte(os.Getenv("FENCING_PUBLIC_MEMBERS")), &members); err != nil || len(members) == 0 {
		return fmt.Errorf("peer probe public membership is malformed")
	}
	var sender, destination quepaxa.Member
	for _, member := range members {
		if member.ID == senderID {
			sender = member
		}
		if member.ID == destinationID {
			destination = member
		}
	}
	if sender.ID == "" || destination.ID == "" || destination.PublicKey == (quepaxa.PublicKey{}) || destination.PeerURL == "" && destination.URL == "" {
		return fmt.Errorf("peer probe sender or destination public identity is missing")
	}
	publicKey := network.PeerPublicKey(types.ClusterID(clusterID), senderID, token)
	if !bytes.Equal(publicKey, sender.PublicKey[:]) {
		return fmt.Errorf("peer probe token does not match captured sender public identity")
	}
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: requestID, SQL: "SELECT '" + strings.ReplaceAll(marker, "'", "''") + "'"}})
	if err != nil {
		return fmt.Errorf("encode peer probe value: %w", err)
	}
	hash := sha256.Sum256(value)
	config := &quepaxa.Cluster{ConfigID: uint(configID), Members: []quepaxa.Member{destination}}
	transport := network.NewTransport(types.ClusterID(clusterID), senderID, config, token)
	defer transport.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result := peerProbeResult{
		Mode: mode, ClusterID: clusterID, SenderNodeID: string(senderID),
		SenderPublicKey: hex.EncodeToString(publicKey), DestinationNodeID: string(destinationID),
		DestinationPeerURL: destination.PeerURL, DestinationPublicKey: hex.EncodeToString(destination.PublicKey[:]),
		ConfigID: uint(configID), RequestID: requestID, ValueSHA256: hex.EncodeToString(hash[:]), MarkerSHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(marker))),
	}
	if result.DestinationPeerURL == "" {
		result.DestinationPeerURL = destination.URL
	}
	if mode == "control" {
		tip, err := transport.ReadTip(ctx, destinationID)
		if err != nil {
			return fmt.Errorf("valid pre-fault ReadTip control failed: %w", err)
		}
		result.BaselineTip, result.ReadTipVerified = uint64(tip), true
	}
	err = transport.StageValue(ctx, destinationID, quepaxa.ValueHash(hash), value)
	if mode == "control" {
		if err != nil {
			return fmt.Errorf("valid pre-fault StageValue control failed: %w", err)
		}
		result.Accepted = true
	} else {
		phase, elapsed, cause, rejected := peerAuthenticationRejection(err)
		if !rejected {
			if err == nil {
				return fmt.Errorf("retired peer StageValue unexpectedly succeeded")
			}
			return fmt.Errorf("retired peer StageValue returned an unrecognized rejection")
		}
		result.ErrorPhase, result.ErrorElapsed, result.ErrorCauseType = phase, elapsed.String(), cause
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}

func peerAuthenticationRejection(err error) (phase string, elapsed time.Duration, causeType string, ok bool) {
	if err == nil {
		return "", 0, "", false
	}
	const prefix = "peer rejected request: peer RPC phase=response-decode elapsed="
	const separator = " cause_type=*errors.errorString: "
	const cause = "peer rejected request: peer authentication failed"
	message := err.Error()
	if !strings.HasPrefix(message, prefix) {
		return "", 0, "", false
	}
	durationText, suffix, found := strings.Cut(strings.TrimPrefix(message, prefix), separator)
	if !found || suffix != cause {
		return "", 0, "", false
	}
	parsed, parseErr := time.ParseDuration(durationText)
	if parseErr != nil || parsed < 0 || parsed > 10*time.Second || parsed.String() != durationText {
		return "", 0, "", false
	}
	return "response-decode", parsed, "*errors.errorString", true
}

func peerProbeDestination(status network.MembershipStatus, members []quepaxa.Member) (quepaxa.Member, error) {
	for _, voter := range status.Voters {
		if voter == status.NodeID {
			continue
		}
		for _, member := range members {
			if member.ID == voter && member.PublicKey != (quepaxa.PublicKey{}) && (member.PeerURL != "" || member.URL != "") {
				return member, nil
			}
		}
	}
	return quepaxa.Member{}, fmt.Errorf("no surviving bootstrap member has a public peer endpoint")
}

func containsNode(nodes []quepaxa.NodeID, wanted quepaxa.NodeID) bool {
	for _, node := range nodes {
		if node == wanted {
			return true
		}
	}
	return false
}

func membershipForNode(ctx context.Context, nodeID quepaxa.NodeID, podNames []string, adminToken, out string) (network.MembershipStatus, error) {
	var lastErr error
	for _, podName := range podNames {
		forward, err := startForward(ctx, podName, out)
		if err != nil {
			lastErr = err
			continue
		}
		status, statusErr := readMembership(ctx, forward, adminToken)
		forward.close()
		if statusErr != nil {
			lastErr = statusErr
			continue
		}
		if status.NodeID == nodeID {
			return status, nil
		}
	}
	if lastErr != nil {
		return network.MembershipStatus{}, lastErr
	}
	return network.MembershipStatus{}, fmt.Errorf("no live Pod reports expected survivor NodeID %q", nodeID)
}

func waitSource(ctx context.Context) error {
	return wait(ctx, "three source-generation voters Ready", func() (bool, error) {
		config, err := get(ctx, "configmap", "rhiza-config")
		if err != nil {
			return false, err
		}
		if nested(config, "data", "RHIZA_CLUSTER_ID") != "chaos-source" {
			return false, fmt.Errorf("source generation config differs")
		}
		if nested(config, "data", "RHIZA_CLUSTER_MEMBERS") != "" {
			return false, fmt.Errorf("fresh source fixture must begin with empty membership")
		}
		sts, err := get(ctx, "statefulset", "rhiza")
		if err != nil {
			return false, err
		}
		uid := text(nested(sts, "metadata", "uid"))
		if uid == "" {
			return false, fmt.Errorf("source StatefulSet UID missing")
		}
		if nested(sts, "spec", "replicas") != float64(3) || nested(sts, "status", "readyReplicas") != float64(3) {
			return false, nil
		}
		if _, err := generatedAdminToken(ctx, sts); err != nil {
			return false, err
		}
		pods, err := get(ctx, "pods", "")
		if err != nil {
			return false, err
		}
		items, _ := pods["items"].([]any)
		ready := map[string]bool{}
		for _, pod := range items {
			if nested(pod, "metadata", "deletionTimestamp") != nil || nested(pod, "metadata", "labels", "app.kubernetes.io/name") != "rhiza" {
				continue
			}
			owners, _ := nested(pod, "metadata", "ownerReferences").([]any)
			owned := false
			for _, owner := range owners {
				if nested(owner, "kind") == "StatefulSet" && nested(owner, "uid") == uid && nested(owner, "controller") == true {
					owned = true
				}
			}
			if !owned {
				continue
			}
			containers, _ := nested(pod, "spec", "containers").([]any)
			if len(containers) != 1 {
				return false, fmt.Errorf("source voter is not a dedicated container")
			}
			from, _ := nested(containers[0], "envFrom").([]any)
			sourceConfig := false
			for _, entry := range from {
				if nested(entry, "configMapRef", "name") == "rhiza-config" {
					sourceConfig = true
				}
			}
			if !sourceConfig {
				return false, fmt.Errorf("source voter generation config missing")
			}
			env, _ := nested(containers[0], "env").([]any)
			for _, entry := range env {
				if nested(entry, "name") == "RHIZA_CLUSTER_ID" && nested(entry, "value") != "chaos-source" {
					return false, fmt.Errorf("source voter generation overridden")
				}
			}
			name := text(nested(pod, "metadata", "name"))
			if name != "rhiza-0" && name != "rhiza-1" && name != "rhiza-2" {
				return false, fmt.Errorf("unexpected source voter Pod")
			}
			conditions, _ := nested(pod, "status", "conditions").([]any)
			for _, condition := range conditions {
				if nested(condition, "type") == "Ready" && nested(condition, "status") == "True" {
					ready[name] = true
				}
			}
		}
		return len(ready) == 3, nil
	})
}

func fatal(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func encoded(value any) []byte {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return data
}
func metadata(name string) obj { return obj{"name": name, "namespace": namespace} }
func resource(api, kind, name string, spec obj) obj {
	return obj{"apiVersion": api, "kind": kind, "metadata": metadata(name), "spec": spec}
}
func field(name, path string) obj {
	return obj{"name": name, "valueFrom": obj{"fieldRef": obj{"fieldPath": path}}}
}
func value(name, val string) obj { return obj{"name": name, "value": val} }
func selector(name string) obj   { return obj{"app.kubernetes.io/name": name} }

func recoveryAnchorStorageID(cluster string) string {
	identity := [5]string{"s3", "rhiza-minio:9000", "rhiza", "rhiza", cluster}
	sum := sha256.Sum256(encoded(identity))
	return fmt.Sprintf("%x", sum[:])
}

func initialRecoveryAnchorRecord() recoveryanchor.Record {
	return recoveryanchor.Record{
		Version: 1, EvidenceFormat: "sql-epoch-token/v1",
		Binding:  recoveryanchor.Binding{ClusterID: "chaos-source", StorageID: recoveryAnchorStorageID("chaos-source")},
		Evidence: encoded(obj{"epoch": 1, "token": "initial"}),
	}
}

func manifests() obj {
	config := obj{"RHIZA_CLUSTER_ID": "chaos-source", "RHIZA_BIND_ADDR": "0.0.0.0:8080", "RHIZA_PEER_ADDR": "0.0.0.0:9090", "RHIZA_DATA_DIR": "/data", "RHIZA_ENABLE_RECONFIGURATION": "true", "RHIZA_CHECKPOINT_INTERVAL": "0", "RHIZA_OBJSTORE_SYNC_INTERVAL": "1s", "RHIZA_OBJSTORE_GC_INTERVAL": "0", "RHIZA_OBJSTORE_PROVIDER": "s3", "RHIZA_OBJSTORE_PREFIX": "rhiza", "RHIZA_OBJSTORE_DURABILITY": "before-ack", "RHIZA_CLUSTER_MEMBERS": ""}
	configMap := obj{"apiVersion": "v1", "kind": "ConfigMap", "metadata": metadata("rhiza-config"), "data": config}
	storeCredentials := obj{"apiVersion": "v1", "kind": "Secret", "metadata": metadata("rhiza-object-store"), "stringData": obj{"RHIZA_OBJSTORE_ENDPOINT": "rhiza-minio:9000", "RHIZA_OBJSTORE_BUCKET": "rhiza", "RHIZA_OBJSTORE_INSECURE": "true", "RHIZA_OBJSTORE_ACCESS_KEY": "fencing-local", "RHIZA_OBJSTORE_SECRET_KEY": "fencing-local-secret", "RHIZA_OBJSTORE_REGION": "us-east-1"}}
	store := resource("apps/v1", "Deployment", "rhiza-minio", obj{"replicas": 1, "selector": obj{"matchLabels": selector("rhiza-minio")}, "template": obj{"metadata": obj{"labels": selector("rhiza-minio")}, "spec": obj{"containers": []obj{{"name": "minio", "image": "ghcr.io/versity/versitygw@sha256:30292fc2eeacc67a36993b01f7a7a5e3361a19cced0e80c1d71cfa2a4b0a2499", "args": []string{"--port", ":9000", "--health", "/_/health", "posix", "/data"}, "env": []obj{value("ROOT_ACCESS_KEY", "fencing-local"), value("ROOT_SECRET_KEY", "fencing-local-secret")}, "ports": []obj{{"name": "s3", "containerPort": 9000}}, "readinessProbe": obj{"httpGet": obj{"path": "/_/health", "port": "s3"}}, "volumeMounts": []obj{{"name": "data", "mountPath": "/data"}}}}, "volumes": []obj{{"name": "data", "emptyDir": obj{}}}}}})
	storeService := resource("v1", "Service", "rhiza-minio", obj{"selector": selector("rhiza-minio"), "ports": []obj{{"port": 9000, "targetPort": "s3"}}})
	bucketJob := resource("batch/v1", "Job", "rhiza-create-bucket", obj{"backoffLimit": 0, "activeDeadlineSeconds": 120, "template": obj{"spec": obj{"restartPolicy": "Never", "containers": []obj{{"name": "create", "image": "amazon/aws-cli@sha256:83f8ffe939569070c5b66d22231862ab78718766d9d8e4c44ca84dd0be5569a5", "args": []string{"--endpoint-url=http://rhiza-minio:9000", "s3api", "create-bucket", "--bucket", "rhiza"}, "env": []obj{value("AWS_ACCESS_KEY_ID", "fencing-local"), value("AWS_SECRET_ACCESS_KEY", "fencing-local-secret"), value("AWS_DEFAULT_REGION", "us-east-1"), value("AWS_EC2_METADATA_DISABLED", "true")}}}}}})
	headless := resource("v1", "Service", "rhiza-headless", obj{"clusterIP": "None", "publishNotReadyAddresses": true, "selector": selector("rhiza"), "ports": []obj{{"name": "http", "port": 8080, "targetPort": "http"}, {"name": "peer-quic", "port": 9090, "targetPort": "peer-quic", "protocol": "UDP"}}})
	envFrom := []obj{{"configMapRef": obj{"name": "rhiza-config"}}, {"secretRef": obj{"name": "rhiza-object-store"}}}
	sts := resource("apps/v1", "StatefulSet", "rhiza", obj{"replicas": 0, "serviceName": "rhiza-headless", "podManagementPolicy": "Parallel", "updateStrategy": obj{"type": "OnDelete"}, "selector": obj{"matchLabels": selector("rhiza")}, "template": obj{"metadata": obj{"labels": selector("rhiza")}, "spec": obj{"securityContext": obj{"runAsNonRoot": true, "fsGroup": 65532}, "containers": []obj{{"name": "rhiza", "image": "rhiza-fencing-host:local", "envFrom": envFrom, "env": []obj{field("RHIZA_NODE_ID", "metadata.name"), value("RHIZA_RECOVERY_ANCHOR_ID", recoveryAnchorID)}, "ports": []obj{{"name": "http", "containerPort": 8080}, {"name": "peer-quic", "containerPort": 9090, "protocol": "UDP"}}, "volumeMounts": []obj{{"name": "data", "mountPath": "/data"}}, "readinessProbe": obj{"httpGet": obj{"path": "/ready", "port": "http"}, "periodSeconds": 2}, "resources": obj{"requests": obj{"cpu": "250m", "memory": "256Mi"}, "limits": obj{"memory": "2Gi"}}}}, "volumes": []obj{{"name": "data", "emptyDir": obj{}}}}}})
	workload := nested(sts, "spec", "template", "spec").(obj)
	nested(sts, "spec", "template", "metadata").(obj)["annotations"] = obj{
		"rhiza.mrchypark.dev/cooperative-fencing": "1",
	}
	workload["serviceAccountName"] = "rhiza-supervised"
	workload["automountServiceAccountToken"] = false
	workload["initContainers"] = []obj{{
		"name": "rhiza-prepare", "image": "rhiza-fencing-host:local",
		"command": []string{"/usr/local/bin/rhiza"}, "args": []string{"--prepare-restore"},
		"envFrom": []obj{{"configMapRef": obj{"name": "rhiza-config"}}, {"secretRef": obj{"name": "rhiza-object-store"}}},
		"env": []obj{
			// The operator must replace this sentinel with the actual immutable
			// bootstrap Secret's public members key before it scales any Pod.
			{"name": "RHIZA_CLUSTER_MEMBERS", "valueFrom": obj{"secretKeyRef": obj{"name": "rhiza-bootstrap-pending", "key": "members"}}},
			field("RHIZA_NODE_ID", "metadata.name"), field("POD_NAME", "metadata.name"),
			field("POD_UID", "metadata.uid"), field("RHIZA_PREPARED_POD_UID", "metadata.uid"),
			field("RHIZA_PREPARE_OPERATION_ID", "metadata.annotations['rhiza.mrchypark.dev/prepare-operation-id']"),
			value("FENCER_AUTHORITY_URL", "https://rhiza-fencer:9444/v1/prepare-restore"),
			value("FENCER_CA_FILE", "/fencer-ca/ca.crt"),
		},
		"volumeMounts": []obj{
			{"name": "data", "mountPath": "/data"},
			{"name": "temporary", "mountPath": "/tmp"},
			{"name": "fence-token", "mountPath": "/var/run/secrets/rhiza-fencer", "readOnly": true},
			{"name": "fence-ca", "mountPath": "/fencer-ca", "readOnly": true},
		},
		"securityContext": obj{"allowPrivilegeEscalation": false, "readOnlyRootFilesystem": true, "capabilities": obj{"drop": []string{"ALL"}}},
	}}
	workload["hostPID"], workload["shareProcessNamespace"] = false, false
	application := workload["containers"].([]obj)[0]
	application["securityContext"] = obj{"allowPrivilegeEscalation": false, "readOnlyRootFilesystem": true, "capabilities": obj{"drop": []string{"ALL"}}}
	opEnv := []obj{field("POD_NAMESPACE", "metadata.namespace"), value("RHIZA_AUTOMATIC_RECOVERY", "true"), value("RHIZA_FENCER_BACKEND", "kubernetes"),
		value("RHIZA_RECOVERY_ANCHOR_URL", "https://anchor-service."+namespace+".svc:9191"),
		value("RHIZA_RECOVERY_ANCHOR_TOKEN_FILE", "/anchor/token"), value("RHIZA_RECOVERY_ANCHOR_CA_FILE", "/anchor/ca.crt")}
	op := resource("apps/v1", "Deployment", "rhiza-operator", obj{"replicas": 0, "selector": obj{"matchLabels": selector("rhiza-operator")}, "template": obj{"metadata": obj{"labels": selector("rhiza-operator")}, "spec": obj{"serviceAccountName": "rhiza-operator", "securityContext": obj{"runAsNonRoot": true, "fsGroup": 65532}, "containers": []obj{{"name": "operator", "image": "rhiza-fencing-operator:local", "args": []string{"--namespace=$(POD_NAMESPACE)", "--poll-interval=1s"}, "env": opEnv, "envFrom": []obj{envFrom[0], envFrom[1]}, "volumeMounts": []obj{{"name": "tmp", "mountPath": "/tmp"}, {"name": "anchor", "mountPath": "/anchor", "readOnly": true}}}}, "volumes": []obj{{"name": "tmp", "emptyDir": obj{}}, {"name": "anchor", "secret": obj{"secretName": "anchor-token"}}}}}})
	anchorConfig := obj{"apiVersion": "v1", "kind": "ConfigMap", "metadata": metadata(recoveryAnchorConfigMap), "data": obj{"anchor.json": string(encoded(initialRecoveryAnchorRecord()))}}
	anchorAccount := obj{"apiVersion": "v1", "kind": "ServiceAccount", "metadata": metadata("anchor-service")}
	anchorRole := obj{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role", "metadata": metadata("anchor-service"), "rules": []obj{{"apiGroups": []string{""}, "resources": []string{"configmaps"}, "resourceNames": []string{recoveryAnchorConfigMap}, "verbs": []string{"get", "update"}}}}
	anchorRoleBinding := obj{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding", "metadata": metadata("anchor-service"), "subjects": []obj{{"kind": "ServiceAccount", "name": "anchor-service", "namespace": namespace}}, "roleRef": obj{"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "anchor-service"}}
	appAnchorRole := obj{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role", "metadata": metadata("anchor-app-guard"), "rules": []obj{{"apiGroups": []string{""}, "resources": []string{"configmaps"}, "resourceNames": []string{recoveryAnchorConfigMap}, "verbs": []string{"get", "update"}}}}
	appAnchorRoleBinding := obj{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding", "metadata": metadata("anchor-app-guard"), "subjects": []obj{{"kind": "ServiceAccount", "name": "rhiza-supervised", "namespace": namespace}}, "roleRef": obj{"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "anchor-app-guard"}}
	anchorDeployment := resource("apps/v1", "Deployment", "anchor-service", obj{"replicas": 1, "selector": obj{"matchLabels": obj{"app": "anchor-service"}}, "template": obj{"metadata": obj{"labels": obj{"app": "anchor-service"}}, "spec": obj{"serviceAccountName": "anchor-service", "securityContext": obj{"runAsNonRoot": true, "fsGroup": 65532}, "containers": []obj{{"name": "anchor", "image": "rhiza-anchor-service:local", "command": []string{"/usr/local/bin/rhiza-entrypoint"}, "ports": []obj{{"name": "https", "containerPort": 9191}}, "env": []obj{value("RHIZA_NAMESPACE", namespace), value("ANCHOR_TOKEN_FILE", "/token/token"), value("ANCHOR_TLS_CERT", "/certs/tls.crt"), value("ANCHOR_TLS_KEY", "/certs/tls.key")}, "envFrom": envFrom, "volumeMounts": []obj{{"name": "certs", "mountPath": "/certs", "readOnly": true}, {"name": "token", "mountPath": "/token", "readOnly": true}}}}, "volumes": []obj{{"name": "certs", "secret": obj{"secretName": "anchor-tls"}}, {"name": "token", "secret": obj{"secretName": "anchor-token"}}}}}})
	anchorService := resource("v1", "Service", "anchor-service", obj{"selector": obj{"app": "anchor-service"}, "ports": []obj{{"name": "https", "port": 9191, "targetPort": 9191}}})
	binding := resource("rhiza.mrchypark.dev/v1alpha1", "RhizaCluster", "rhiza", obj{"logicalID": "fencing-e2e", "statefulSet": "rhiza", "container": "rhiza", "adoptClusterID": "chaos-source", "automatic": true, "bootstrap": true, "anchorID": recoveryAnchorID, "voterCount": 3, "policy": obj{"failureGraceSeconds": 15, "cooldownSeconds": 3, "maxRecoveriesPerDay": 3, "allowDataLoss": false}})
	bucketJob["spec"].(obj)["suspend"] = true
	return obj{"apiVersion": "v1", "kind": "List", "items": []obj{configMap, storeCredentials, store, storeService, bucketJob, headless, sts, op, binding, anchorConfig, anchorAccount, anchorRole, anchorRoleBinding, appAnchorRole, appAnchorRoleBinding, anchorDeployment, anchorService}}
}

// generatedAdminToken follows the same Secret reference written by the real
// bootstrap controller. It deliberately does not know or predict the Secret
// name, and never emits credential bytes into qualification artifacts.
func generatedAdminToken(ctx context.Context, sts obj) (string, error) {
	containers, _ := nested(sts, "spec", "template", "spec", "containers").([]any)
	if len(containers) != 1 {
		return "", fmt.Errorf("source StatefulSet must have one runtime container")
	}
	var secretName string
	refs := map[string]string{}
	env, _ := nested(containers[0], "env").([]any)
	for _, entry := range env {
		name := text(nested(entry, "name"))
		if name == "RHIZA_ADMIN_TOKEN" || name == "RHIZA_CLUSTER_MEMBERS" || name == "RHIZA_PEER_TOKENS" {
			refs[name] = text(nested(entry, "valueFrom", "secretKeyRef", "name"))
		}
		if name == "RHIZA_ADMIN_TOKEN" {
			secretName = refs[name]
		}
	}
	if secretName == "" {
		return "", fmt.Errorf("operator-generated admin Secret reference missing")
	}
	for _, name := range []string{"RHIZA_CLUSTER_MEMBERS", "RHIZA_PEER_TOKENS"} {
		if refs[name] != secretName {
			return "", fmt.Errorf("operator-generated %s Secret reference differs", name)
		}
	}
	secret, err := get(ctx, "secret", secretName)
	if err != nil {
		return "", err
	}
	if nested(secret, "immutable") != true {
		return "", fmt.Errorf("operator-generated credential Secret is not immutable")
	}
	decode := func(key string) ([]byte, error) {
		return base64.StdEncoding.DecodeString(text(nested(secret, "data", key)))
	}
	raw, err := decode("admin")
	if err != nil || len(raw) == 0 {
		return "", fmt.Errorf("operator-generated admin credential is missing or malformed")
	}
	membersData, err := decode("members")
	if err != nil {
		return "", fmt.Errorf("operator-generated membership is missing or malformed")
	}
	var members []obj
	if err := json.Unmarshal(membersData, &members); err != nil || len(members) != 3 {
		return "", fmt.Errorf("operator-generated membership does not contain three voters")
	}
	peerTokens, err := decode("peer_tokens")
	if err != nil {
		return "", fmt.Errorf("operator-generated peer credentials are missing or malformed")
	}
	var tokens map[string]string
	if err := json.Unmarshal(peerTokens, &tokens); err != nil || len(tokens) != 3 {
		return "", fmt.Errorf("operator-generated peer credential map does not contain three voters")
	}
	return string(raw), nil
}

type peerBootstrapCredential struct {
	secretName string
	secretUID  string
	token      string
	members    []quepaxa.Member
}

func generatedPeerCredential(ctx context.Context, sts obj, nodeID quepaxa.NodeID) (peerBootstrapCredential, error) {
	containers, _ := nested(sts, "spec", "template", "spec", "containers").([]any)
	if len(containers) != 1 {
		return peerBootstrapCredential{}, fmt.Errorf("source StatefulSet must have one runtime container")
	}
	refs := map[string]string{}
	for _, entry := range listAny(nested(containers[0], "env")) {
		name := text(nested(entry, "name"))
		if name == "RHIZA_ADMIN_TOKEN" || name == "RHIZA_CLUSTER_MEMBERS" || name == "RHIZA_PEER_TOKENS" {
			refs[name] = text(nested(entry, "valueFrom", "secretKeyRef", "name"))
		}
	}
	secretName := refs["RHIZA_ADMIN_TOKEN"]
	if secretName == "" || refs["RHIZA_CLUSTER_MEMBERS"] != secretName || refs["RHIZA_PEER_TOKENS"] != secretName {
		return peerBootstrapCredential{}, fmt.Errorf("operator-generated immutable peer credential Secret references differ")
	}
	secret, err := get(ctx, "secret", secretName)
	if err != nil {
		return peerBootstrapCredential{}, err
	}
	if nested(secret, "immutable") != true || text(nested(secret, "metadata", "uid")) == "" {
		return peerBootstrapCredential{}, fmt.Errorf("operator-generated peer credential Secret is not immutable or lacks UID")
	}
	decode := func(key string) ([]byte, error) {
		return base64.StdEncoding.DecodeString(text(nested(secret, "data", key)))
	}
	memberBytes, err := decode("members")
	if err != nil {
		return peerBootstrapCredential{}, fmt.Errorf("operator-generated public membership is malformed")
	}
	var members []quepaxa.Member
	if err := json.Unmarshal(memberBytes, &members); err != nil || len(members) != 3 {
		return peerBootstrapCredential{}, fmt.Errorf("operator-generated public membership does not contain three voters")
	}
	tokenBytes, err := decode("peer_tokens")
	if err != nil {
		return peerBootstrapCredential{}, fmt.Errorf("operator-generated peer credential map is malformed")
	}
	var tokens map[string]string
	if err := json.Unmarshal(tokenBytes, &tokens); err != nil || len(tokens) != len(members) {
		return peerBootstrapCredential{}, fmt.Errorf("operator-generated peer credential map does not match membership")
	}
	token := tokens[string(nodeID)]
	if token == "" {
		return peerBootstrapCredential{}, fmt.Errorf("operator-generated credential for old node is missing")
	}
	found := false
	for _, member := range members {
		if member.ID == nodeID && member.PublicKey == quepaxa.PublicKey(network.PeerPublicKey(types.ClusterID("chaos-source"), nodeID, token)) {
			found = true
		}
	}
	if !found {
		return peerBootstrapCredential{}, fmt.Errorf("old peer token does not match immutable public member key")
	}
	return peerBootstrapCredential{secretName: secretName, secretUID: text(nested(secret, "metadata", "uid")), token: token, members: members}, nil
}

func randomHex(size int) (string, error) {
	if size <= 0 || size > 64 {
		return "", fmt.Errorf("invalid random identifier size")
	}
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		return "", err
	}
	return hex.EncodeToString(data), nil
}

func runPeerProbeJob(ctx context.Context, mode string, bootstrapSecret string, credential peerBootstrapCredential, senderID, destinationID quepaxa.NodeID, configID uint, requestID, marker, out string) (result peerProbeResult, resultErr error) {
	if mode != "control" && mode != "stale" || bootstrapSecret == "" || credential.token == "" || senderID == "" || destinationID == "" || configID == 0 || requestID == "" || marker == "" {
		return peerProbeResult{}, fmt.Errorf("peer probe Job input is incomplete")
	}
	suffix, err := randomHex(12)
	if err != nil {
		return peerProbeResult{}, err
	}
	jobName, secretName := "peer-probe-"+mode+"-"+suffix, "peer-probe-token-"+suffix
	jobCreated, secretCreated := false, false
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		var cleanupErr error
		if jobCreated {
			_, err := kubectl(cleanupCtx, "delete", "job", jobName, "--cascade=foreground", "--ignore-not-found=true", "--wait=true", "--timeout=30s")
			cleanupErr = errors.Join(cleanupErr, err, ensureKubernetesAbsent(cleanupCtx, "job", jobName), ensureProbePodsAbsent(cleanupCtx, jobName))
		}
		if secretCreated {
			_, err := kubectl(cleanupCtx, "delete", "secret", secretName, "--ignore-not-found=true", "--wait=true", "--timeout=30s")
			cleanupErr = errors.Join(cleanupErr, err, ensureKubernetesAbsent(cleanupCtx, "secret", secretName))
		}
		result.CleanupVerified = cleanupErr == nil
		resultErr = errors.Join(resultErr, cleanupErr)
	}()
	secret := obj{"apiVersion": "v1", "kind": "Secret", "metadata": obj{"name": secretName, "namespace": namespace, "labels": obj{"app.kubernetes.io/managed-by": "fencing-e2e", "rhiza.mrchypark.dev/probe-mode": mode, "rhiza.mrchypark.dev/probe-run": suffix}}, "type": "Opaque", "immutable": true, "data": obj{"peer-token": base64.StdEncoding.EncodeToString([]byte(credential.token))}}
	if err := ensureKubernetesAbsent(ctx, "secret", secretName); err != nil {
		return peerProbeResult{}, fmt.Errorf("refusing to reuse scalar peer Secret name: %w", err)
	}
	if _, err := createKubernetesJSON(ctx, "secret", secret); err != nil {
		secretCreated = runOwnedKubernetesResource(ctx, "secret", secretName, suffix)
		return peerProbeResult{}, fmt.Errorf("create scalar old-peer probe Secret: %w", err)
	}
	secretCreated = true
	createdSecret, err := get(ctx, "secret", secretName)
	if err != nil || nested(createdSecret, "immutable") != true || text(nested(createdSecret, "metadata", "uid")) == "" {
		return peerProbeResult{}, fmt.Errorf("created scalar peer Secret is not verifiably immutable: %v", err)
	}
	result.ProbeSecretName = secretName
	result.ProbeSecretUID = text(nested(createdSecret, "metadata", "uid"))
	job := peerProbeJobResource(jobName, secretName, bootstrapSecret, mode, suffix, senderID, destinationID, configID, requestID, marker)
	if err := ensureKubernetesAbsent(ctx, "job", jobName); err != nil {
		return peerProbeResult{}, fmt.Errorf("refusing to reuse peer probe Job name: %w", err)
	}
	if _, err := createKubernetesJSON(ctx, "job", job); err != nil {
		jobCreated = runOwnedKubernetesResource(ctx, "job", jobName, suffix)
		return peerProbeResult{}, fmt.Errorf("create run-owned %s probe Job: %w", mode, err)
	}
	jobCreated = true
	createdJob, err := get(ctx, "job", jobName)
	if err != nil || text(nested(createdJob, "metadata", "uid")) == "" {
		return peerProbeResult{}, fmt.Errorf("created %s probe Job UID unavailable: %v", mode, err)
	}
	result.ProbeJobName = jobName
	result.ProbeJobUID = text(nested(createdJob, "metadata", "uid"))
	var logs []byte
	err = wait(ctx, mode+" peer probe Job completion", func() (bool, error) {
		current, err := get(ctx, "job", jobName)
		if err != nil {
			return false, err
		}
		for _, condition := range listAny(nested(current, "status", "conditions")) {
			if nested(condition, "type") == "Failed" && nested(condition, "status") == "True" {
				logs, _ = kubectl(ctx, "logs", "job/"+jobName, "--tail=200", "--limit-bytes=65536")
				podData, _ := kubectl(ctx, "get", "pods", "-l", "job-name="+jobName, "-o", "json")
				var pods obj
				_ = json.Unmarshal(podData, &pods)
				return false, peerProbeJobFailure(mode, jobName, current, pods)
			}
			if nested(condition, "type") == "Complete" && nested(condition, "status") == "True" {
				logs, err = kubectl(ctx, "logs", "job/"+jobName, "--tail=200", "--limit-bytes=65536")
				if err != nil {
					return false, fmt.Errorf("peer probe Job result logs unavailable")
				}
				return true, nil
			}
		}
		return false, nil
	})
	if len(logs) > 0 {
		_ = os.WriteFile(filepath.Join(out, "peer-probe-"+mode+".log"), logs, 0600)
	}
	if err != nil {
		return peerProbeResult{}, err
	}
	if err := json.Unmarshal(bytes.TrimSpace(logs), &result); err != nil {
		return peerProbeResult{}, fmt.Errorf("peer probe result is not one JSON record")
	}
	if result.Mode != mode || result.SenderNodeID != string(senderID) || result.DestinationNodeID != string(destinationID) || result.ConfigID != configID || result.RequestID != requestID || result.Accepted != (mode == "control") {
		return peerProbeResult{}, fmt.Errorf("peer probe result does not match the run request")
	}
	if result.ReadTipVerified != (mode == "control") {
		return peerProbeResult{}, fmt.Errorf("peer probe ReadTip control evidence does not match the mode")
	}
	result.ProbeJobName = jobName
	result.ProbeJobUID = text(nested(createdJob, "metadata", "uid"))
	result.ProbeSecretName = secretName
	result.ProbeSecretUID = text(nested(createdSecret, "metadata", "uid"))
	return result, nil
}

func peerProbeJobFailure(mode, jobName string, job, pods obj) error {
	classification := "failed"
	for _, condition := range listAny(nested(job, "status", "conditions")) {
		if nested(condition, "type") != "Failed" || nested(condition, "status") != "True" {
			continue
		}
		switch nested(condition, "reason") {
		case "BackoffLimitExceeded":
			classification = "backoff_limit_exceeded"
		case "DeadlineExceeded":
			classification = "deadline_exceeded"
		}
		break
	}
	exitCode := "unknown"
	for _, pod := range listAny(pods["items"]) {
		if nested(pod, "metadata", "labels", "job-name") != jobName {
			continue
		}
		for _, status := range listAny(nested(pod, "status", "containerStatuses")) {
			if nested(status, "name") != "peer-probe" {
				continue
			}
			code, ok := nested(status, "state", "terminated", "exitCode").(float64)
			if ok && code >= 0 && code <= 255 && float64(int64(code)) == code {
				exitCode = strconv.FormatInt(int64(code), 10)
			}
			break
		}
		if exitCode != "unknown" {
			break
		}
	}
	return fmt.Errorf("peer probe Job failed: mode=%s classification=%s exitCode=%s", mode, classification, exitCode)
}

func peerProbeJobResource(jobName, tokenSecret, bootstrapSecret, mode, runID string, senderID, destinationID quepaxa.NodeID, configID uint, requestID, marker string) obj {
	job := resource("batch/v1", "Job", jobName, obj{
		"backoffLimit": 0, "activeDeadlineSeconds": 60,
		"template": obj{"metadata": obj{"labels": obj{"app.kubernetes.io/managed-by": "fencing-e2e", "rhiza.mrchypark.dev/probe-mode": mode, "rhiza.mrchypark.dev/probe-run": runID}}, "spec": obj{
			"restartPolicy": "Never", "automountServiceAccountToken": false,
			"securityContext": obj{"runAsNonRoot": true, "runAsUser": 65532, "runAsGroup": 65532},
			"containers": []obj{{"name": "peer-probe", "image": "rhiza-fencing-host:local", "imagePullPolicy": "IfNotPresent", "command": []string{"/usr/local/bin/fencing-e2e"}, "args": []string{"peer-probe", mode}, "env": []obj{
				value("FENCING_CLUSTER_ID", "chaos-source"), value("FENCING_SENDER_NODE_ID", string(senderID)), value("FENCING_DESTINATION_NODE_ID", string(destinationID)),
				value("FENCING_CONFIG_ID", strconv.FormatUint(uint64(configID), 10)), value("FENCING_REQUEST_ID", requestID), value("FENCING_MARKER", marker),
				{"name": "FENCING_PEER_TOKEN", "valueFrom": obj{"secretKeyRef": obj{"name": tokenSecret, "key": "peer-token"}}},
				{"name": "FENCING_PUBLIC_MEMBERS", "valueFrom": obj{"secretKeyRef": obj{"name": bootstrapSecret, "key": "members"}}},
			}, "resources": obj{"requests": obj{"cpu": "25m", "memory": "32Mi"}, "limits": obj{"memory": "128Mi"}}}},
		}},
	})
	job["metadata"].(obj)["labels"] = obj{"app.kubernetes.io/managed-by": "fencing-e2e", "rhiza.mrchypark.dev/probe-mode": mode, "rhiza.mrchypark.dev/probe-run": runID}
	return job
}

func createKubernetesJSON(ctx context.Context, resource string, value any) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, "kubectl", "--context", kubeContext, "-n", namespace, "create", "-f", "-")
	cmd.Stdin = bytes.NewReader(data)
	output, err := cmd.CombinedOutput()
	if err != nil {
		if resource == "secret" {
			return nil, fmt.Errorf("kubectl create %s failed: %w", resource, err)
		}
		return nil, fmt.Errorf("kubectl create %s failed: %w: %s", resource, err, output)
	}
	return output, nil
}

func runOwnedKubernetesResource(ctx context.Context, kind, name, runID string) bool {
	data, err := kubectl(ctx, "get", kind, name, "-o", "json", "--ignore-not-found=true")
	if err != nil || len(bytes.TrimSpace(data)) == 0 {
		return false
	}
	var resource obj
	if json.Unmarshal(data, &resource) != nil {
		return false
	}
	return text(nested(resource, "metadata", "labels", "rhiza.mrchypark.dev/probe-run")) == runID
}

func ensureKubernetesAbsent(ctx context.Context, kind, name string) error {
	data, err := kubectl(ctx, "get", kind, name, "-o", "name", "--ignore-not-found=true")
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(data)) != "" {
		return fmt.Errorf("run-owned %s %s remains after cleanup", kind, name)
	}
	return nil
}

func ensureProbePodsAbsent(ctx context.Context, jobName string) error {
	data, err := kubectl(ctx, "get", "pods", "-l", "job-name="+jobName, "-o", "json")
	if err != nil {
		return err
	}
	var pods obj
	if err := json.Unmarshal(data, &pods); err != nil {
		return err
	}
	if len(listAny(pods["items"])) != 0 {
		return fmt.Errorf("run-owned peer probe Pod remains after cleanup")
	}
	return nil
}

func podChaosForTarget(targetUID string) (obj, error) {
	if targetUID == "" {
		return nil, fmt.Errorf("PodChaos target UID is required")
	}
	digest := sha256.Sum256([]byte(targetUID))
	selectorValue := hex.EncodeToString(digest[:8])
	return obj{
		"apiVersion": "chaos-mesh.org/v1alpha1", "kind": "PodChaos",
		"metadata": obj{"name": chaosName, "namespace": namespace},
		"spec": obj{
			"action": "pod-kill", "mode": "one", "duration": "45s",
			"selector": obj{"namespaces": []string{namespace}, "labelSelectors": obj{chaosTargetLabel: selectorValue}},
		},
	}, nil
}

func replaceInitialPodWithChaos(ctx context.Context, oldMembership network.MembershipStatus, adminToken, out string, event func(string, obj)) (resultErr error) {
	if err := requireOperatorStopped(ctx); err != nil {
		return err
	}
	oldPod, err := get(ctx, "pod", "rhiza-0")
	if err != nil {
		return err
	}
	oldUID := text(nested(oldPod, "metadata", "uid"))
	if oldUID == "" || oldMembership.NodeID == "" {
		return fmt.Errorf("initial replacement target lacks exact Pod or Node identity")
	}
	chaos, err := podChaosForTarget(oldUID)
	if err != nil {
		return err
	}
	labelValue := text(nested(chaos, "spec", "selector", "labelSelectors", chaosTargetLabel))
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		cleanupErr := errors.Join(removePodChaos(cleanupCtx), removeChaosTargetLabel(cleanupCtx, labelValue))
		if cleanupErr == nil {
			event("podchaos_cleanup_verified", obj{"name": chaosName, "targetPodUID": oldUID})
		} else {
			event("podchaos_cleanup_failed", obj{"name": chaosName, "targetPodUID": oldUID, "error": cleanupErr.Error()})
		}
		resultErr = errors.Join(resultErr, cleanupErr)
	}()
	if _, err := kubectl(ctx, "label", "pod", "rhiza-0", chaosTargetLabel+"="+labelValue, "--overwrite=false"); err != nil {
		return fmt.Errorf("mark exact PodChaos target: %w", err)
	}
	if _, err := applyKubernetesJSON(ctx, "podchaos", chaos); err != nil {
		return fmt.Errorf("create run-scoped PodChaos: %w", err)
	}
	event("podchaos_requested", obj{"name": chaosName, "targetPod": "rhiza-0", "targetPodUID": oldUID, "targetNodeID": oldMembership.NodeID, "duration": "45s", "labelSelector": labelValue})
	if err := wait(ctx, "PodChaos reports exact target injected", func() (bool, error) {
		current, err := get(ctx, "podchaos", chaosName)
		if err != nil {
			return false, err
		}
		namespaces, _ := nested(current, "spec", "selector", "namespaces").([]any)
		if nested(current, "spec", "action") != "pod-kill" || nested(current, "spec", "mode") != "one" || nested(current, "spec", "duration") != "45s" || len(namespaces) != 1 || namespaces[0] != namespace || text(nested(current, "spec", "selector", "labelSelectors", chaosTargetLabel)) != labelValue {
			return false, fmt.Errorf("PodChaos selector changed from the exact run target")
		}
		conditions, _ := nested(current, "status", "conditions").([]any)
		for _, condition := range conditions {
			if nested(condition, "type") == "AllInjected" && nested(condition, "status") == "True" {
				event("podchaos_injected", obj{"name": chaosName, "resourceUID": nested(current, "metadata", "uid"), "targetPodUID": oldUID, "condition": condition})
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		return err
	}
	var replacement obj
	if err := wait(ctx, "same-ordinal Pod recreated with successful restore init", func() (bool, error) {
		pods, err := get(ctx, "pods", "")
		if err != nil {
			return false, err
		}
		items, _ := pods["items"].([]any)
		for _, item := range items {
			if text(nested(item, "metadata", "name")) != "rhiza-0" || nested(item, "metadata", "deletionTimestamp") != nil {
				continue
			}
			uid := text(nested(item, "metadata", "uid"))
			if uid == "" || uid == oldUID {
				return false, nil
			}
			ready, err := preparePodReady(item)
			if err != nil {
				return false, err
			}
			if !ready {
				return false, nil
			}
			replacement = item.(obj)
			return true, nil
		}
		return false, nil
	}); err != nil {
		return err
	}
	newUID := text(nested(replacement, "metadata", "uid"))
	newForward, err := startForward(ctx, "rhiza-0", out)
	if err != nil {
		return err
	}
	forwards := []*forward{newForward}
	defer func() {
		for _, f := range forwards {
			f.close()
		}
	}()
	for _, name := range []string{"rhiza-1", "rhiza-2"} {
		f, err := startForward(ctx, name, out)
		if err != nil {
			return err
		}
		forwards = append(forwards, f)
	}
	var currentMembership network.MembershipStatus
	var voterStatuses []network.MembershipStatus
	newNodeID := quepaxa.NodeID("pod-" + newUID)
	wantVoters, err := replacementVoterSet(oldMembership.Voters, oldMembership.NodeID, newNodeID)
	if err != nil {
		return fmt.Errorf("initial membership cannot define one-Pod replacement: %w", err)
	}
	if err := wait(ctx, "canonical three-voter membership after init replacement", func() (bool, error) {
		statuses := make([]network.MembershipStatus, 0, len(forwards))
		for _, f := range forwards {
			status, err := readMembership(ctx, f, adminToken)
			if err != nil {
				return false, nil
			}
			statuses = append(statuses, status)
		}
		first := statuses[0]
		if first.ClusterID != "chaos-source" || first.NodeID != newNodeID || first.WALIdentity == "" || !first.Voting || first.Pending || first.ConfigID == 0 || len(first.Voters) != 3 {
			return false, nil
		}
		if !sameNodeSet(first.Voters, wantVoters) {
			return false, nil
		}
		seenNodes := make(map[quepaxa.NodeID]bool, len(statuses))
		for _, status := range statuses {
			if status.ClusterID != first.ClusterID || status.ConfigID != first.ConfigID || status.WALIdentity == "" || !status.Voting || status.Pending || len(status.Voters) != len(wantVoters) || !sameNodeSet(status.Voters, wantVoters) || !wantVoters[status.NodeID] {
				return false, nil
			}
			if seenNodes[status.NodeID] {
				return false, fmt.Errorf("two live Pod incarnations report the same NodeID")
			}
			seenNodes[status.NodeID] = true
		}
		cluster, err := get(ctx, "rhizacluster", "rhiza")
		if err != nil || nested(cluster, "status", "phase") != "Healthy" || nested(cluster, "status", "activeClusterID") != "chaos-source" {
			return false, nil
		}
		currentMembership, voterStatuses = first, statuses
		return true, nil
	}); err != nil {
		return err
	}
	if err := requireOperatorStopped(ctx); err != nil {
		return err
	}
	event("podchaos_replacement_recovered", obj{"oldPodUID": oldUID, "newPodUID": newUID, "oldNodeID": oldMembership.NodeID, "newNodeID": currentMembership.NodeID, "clusterID": currentMembership.ClusterID, "configID": currentMembership.ConfigID, "voters": currentMembership.Voters, "voterStatuses": voterStatuses, "initOperationID": nested(replacement, "metadata", "annotations", "rhiza.mrchypark.dev/prepare-operation-id")})
	return nil
}

func sameNodeSet(got []quepaxa.NodeID, want map[quepaxa.NodeID]bool) bool {
	seen := make(map[quepaxa.NodeID]bool, len(got))
	for _, id := range got {
		if !want[id] || seen[id] {
			return false
		}
		seen[id] = true
	}
	return len(seen) == len(want)
}

func replacementVoterSet(old []quepaxa.NodeID, removed, added quepaxa.NodeID) (map[quepaxa.NodeID]bool, error) {
	if removed == "" || added == "" || removed == added {
		return nil, fmt.Errorf("removed and added NodeID must be distinct and nonempty")
	}
	want := make(map[quepaxa.NodeID]bool, len(old))
	seen := make(map[quepaxa.NodeID]bool, len(old))
	foundRemoved := false
	for _, id := range old {
		if id == "" || seen[id] {
			return nil, fmt.Errorf("old voter set contains empty or duplicate NodeID")
		}
		seen[id] = true
		want[id] = true
		if id == removed {
			foundRemoved = true
			delete(want, id)
		}
	}
	if !foundRemoved || want[added] {
		return nil, fmt.Errorf("old voter set does not authorize one replacement")
	}
	want[added] = true
	return want, nil
}

func preparePodReady(pod any) (bool, error) {
	initContainers, _ := nested(pod, "spec", "initContainers").([]any)
	if len(initContainers) != 1 || nested(initContainers[0], "name") != "rhiza-prepare" || nested(initContainers[0], "command") == nil {
		return false, fmt.Errorf("replacement Pod does not use the required restore init profile")
	}
	command, args := stringsFromAny(nested(initContainers[0], "command")), stringsFromAny(nested(initContainers[0], "args"))
	if strings.Join(command, " ") != "/usr/local/bin/rhiza" || strings.Join(args, " ") != "--prepare-restore" {
		return false, fmt.Errorf("replacement Pod restore init command differs")
	}
	appContainers, _ := nested(pod, "spec", "containers").([]any)
	if len(appContainers) != 1 || nested(appContainers[0], "name") != "rhiza" {
		return false, fmt.Errorf("replacement Pod application container differs")
	}
	if nested(initContainers[0], "image") != nested(appContainers[0], "image") {
		return false, fmt.Errorf("restore init and application images differ")
	}
	initEnv, _ := nested(initContainers[0], "env").([]any)
	memberSecret, podNameField, podUIDField, preparedUIDField, prepareOperation, authorityURL, caFile := "", false, false, false, false, false, false
	for _, raw := range initEnv {
		name := text(nested(raw, "name"))
		if name == "RHIZA_ADMIN_TOKEN" || name == "RHIZA_PEER_TOKEN" || name == "RHIZA_PEER_TOKENS" {
			return false, fmt.Errorf("restore init received runtime credentials")
		}
		if name == "RHIZA_CLUSTER_MEMBERS" {
			memberSecret = text(nested(raw, "valueFrom", "secretKeyRef", "name"))
			if nested(raw, "valueFrom", "secretKeyRef", "key") != "members" {
				return false, fmt.Errorf("restore init membership reference is not public members key")
			}
		}
		fieldPath := text(nested(raw, "valueFrom", "fieldRef", "fieldPath"))
		switch name {
		case "POD_NAME":
			podNameField = fieldPath == "metadata.name"
		case "POD_UID":
			podUIDField = fieldPath == "metadata.uid"
		case "RHIZA_PREPARED_POD_UID":
			preparedUIDField = fieldPath == "metadata.uid"
		case "RHIZA_PREPARE_OPERATION_ID":
			prepareOperation = fieldPath == "metadata.annotations['rhiza.mrchypark.dev/prepare-operation-id']"
		case "FENCER_AUTHORITY_URL":
			authorityURL = nested(raw, "value") == "https://rhiza-fencer:9444/v1/prepare-restore"
		case "FENCER_CA_FILE":
			caFile = nested(raw, "value") == "/fencer-ca/ca.crt"
		}
	}
	if memberSecret == "" || memberSecret == "rhiza-bootstrap-pending" || !podNameField || !podUIDField || !preparedUIDField || !prepareOperation || !authorityURL || !caFile {
		return false, fmt.Errorf("operator did not bind restore init to the generated public membership Secret")
	}
	configMapEnvFrom, objectStoreEnvFrom := false, false
	for _, raw := range listAny(nested(initContainers[0], "envFrom")) {
		if nested(raw, "configMapRef", "name") == "rhiza-config" {
			configMapEnvFrom = true
		}
		if nested(raw, "secretRef") != nil && nested(raw, "secretRef", "name") != "rhiza-object-store" {
			return false, fmt.Errorf("restore init imports an unexpected Secret")
		}
		if nested(raw, "secretRef", "name") == "rhiza-object-store" {
			objectStoreEnvFrom = true
		}
	}
	if !configMapEnvFrom || !objectStoreEnvFrom {
		return false, fmt.Errorf("restore init lacks public configuration or object-store credentials")
	}
	initMounts, _ := nested(initContainers[0], "volumeMounts").([]any)
	dataMounted := func(mounts []any) bool {
		for _, mount := range mounts {
			if nested(mount, "mountPath") == "/data" && nested(mount, "name") == "data" {
				return true
			}
		}
		return false
	}
	appMounts, _ := nested(appContainers[0], "volumeMounts").([]any)
	tokenMounted, caMounted := false, false
	for _, mount := range initMounts {
		if nested(mount, "name") == "fence-token" && nested(mount, "mountPath") == "/var/run/secrets/rhiza-fencer" && nested(mount, "readOnly") == true {
			tokenMounted = true
		}
		if nested(mount, "name") == "fence-ca" && nested(mount, "mountPath") == "/fencer-ca" && nested(mount, "readOnly") == true {
			caMounted = true
		}
	}
	if !dataMounted(initMounts) || !dataMounted(appMounts) || !tokenMounted || !caMounted {
		return false, fmt.Errorf("restore init and app do not share the same /data volume")
	}
	volumes, _ := nested(pod, "spec", "volumes").([]any)
	dataIsEmptyDir := false
	for _, volume := range volumes {
		if nested(volume, "name") == "data" && nested(volume, "emptyDir") != nil {
			dataIsEmptyDir = true
		}
		if nested(volume, "persistentVolumeClaim") != nil {
			return false, fmt.Errorf("NoPVC qualification unexpectedly uses persistent storage")
		}
	}
	tokenProjection, caConfig := false, false
	for _, volume := range volumes {
		if nested(volume, "name") == "fence-token" {
			for _, source := range listAny(nested(volume, "projected", "sources")) {
				if nested(source, "serviceAccountToken", "path") == "token" && nested(source, "serviceAccountToken", "audience") == "rhiza-fencer" && nested(source, "serviceAccountToken", "expirationSeconds") == float64(600) {
					tokenProjection = true
				}
			}
		}
		if nested(volume, "name") == "fence-ca" && nested(volume, "configMap", "name") == "rhiza-fencer-ca" {
			caConfig = true
		}
	}
	if !dataIsEmptyDir || !tokenProjection || !caConfig {
		return false, fmt.Errorf("restore data volume is not emptyDir")
	}
	operationID := text(nested(pod, "metadata", "annotations", "rhiza.mrchypark.dev/prepare-operation-id"))
	if operationID == "" {
		return false, fmt.Errorf("replacement Pod lacks trusted prepare operation annotation")
	}
	statuses, _ := nested(pod, "status", "initContainerStatuses").([]any)
	var initFinished time.Time
	for _, status := range statuses {
		if nested(status, "name") != "rhiza-prepare" {
			continue
		}
		if nested(status, "state", "terminated") == nil {
			return false, nil
		}
		if nested(status, "state", "terminated", "exitCode") != float64(0) {
			return false, fmt.Errorf("restore init container failed")
		}
		initFinished, _ = time.Parse(time.RFC3339Nano, text(nested(status, "state", "terminated", "finishedAt")))
	}
	if initFinished.IsZero() {
		return false, nil
	}
	appStatuses, _ := nested(pod, "status", "containerStatuses").([]any)
	if len(appStatuses) != 1 || nested(appStatuses[0], "name") != "rhiza" || nested(appStatuses[0], "state", "running") == nil {
		return false, nil
	}
	appStarted, _ := time.Parse(time.RFC3339Nano, text(nested(appStatuses[0], "state", "running", "startedAt")))
	if appStarted.IsZero() || appStarted.Before(initFinished) {
		return false, fmt.Errorf("application started before successful restore init completion")
	}
	conditions, _ := nested(pod, "status", "conditions").([]any)
	for _, condition := range conditions {
		if nested(condition, "type") == "Ready" && nested(condition, "status") == "True" {
			return true, nil
		}
	}
	return false, nil
}

func listAny(value any) []any {
	items, _ := value.([]any)
	return items
}

func stringsFromAny(value any) []string {
	switch values := value.(type) {
	case []string:
		return values
	case []any:
		result := make([]string, 0, len(values))
		for _, item := range values {
			text, ok := item.(string)
			if !ok {
				return nil
			}
			result = append(result, text)
		}
		return result
	default:
		return nil
	}
}

func applyKubernetesJSON(ctx context.Context, resource string, value any) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, "kubectl", "--context", kubeContext, "-n", namespace, "apply", "-f", "-")
	cmd.Stdin = bytes.NewReader(data)
	output, err := cmd.CombinedOutput()
	if err != nil {
		if resource == "secret" {
			return nil, fmt.Errorf("kubectl apply %s failed: %w", resource, err)
		}
		return nil, fmt.Errorf("kubectl apply %s failed: %w: %s", resource, err, output)
	}
	return output, nil
}

func removePodChaos(ctx context.Context) error {
	if _, err := kubectl(ctx, "delete", "podchaos", chaosName, "--ignore-not-found=true", "--wait=true", "--timeout=30s"); err != nil {
		return err
	}
	data, err := kubectl(ctx, "get", "podchaos", chaosName, "-o", "json", "--ignore-not-found=true")
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(data)) != "" {
		return fmt.Errorf("PodChaos %s remains after cleanup", chaosName)
	}
	return nil
}

func removeChaosTargetLabel(ctx context.Context, value string) error {
	pods, err := get(ctx, "pods", "")
	if err != nil {
		return err
	}
	items, _ := pods["items"].([]any)
	for _, pod := range items {
		if nested(pod, "metadata", "name") == "rhiza-0" && nested(pod, "metadata", "labels", chaosTargetLabel) == value {
			if _, err := kubectl(ctx, "label", "pod", "rhiza-0", chaosTargetLabel+"-"); err != nil {
				return fmt.Errorf("remove owned target label: %w", err)
			}
		}
	}
	return nil
}

func scaleOperator(ctx context.Context, replicas int) error {
	if replicas != 0 && replicas != 1 {
		return fmt.Errorf("fixture operator replica count %d is not allowed", replicas)
	}
	if _, err := kubectl(ctx, "scale", "deployment", "rhiza-operator", fmt.Sprintf("--replicas=%d", replicas)); err != nil {
		return err
	}
	if replicas == 0 {
		return wait(ctx, "test operator fully stopped", func() (bool, error) {
			return operatorStopped(ctx)
		})
	}
	if _, err := kubectl(ctx, "rollout", "status", "deployment/rhiza-operator", "--timeout=120s"); err != nil {
		return err
	}
	return nil
}

func operatorStopped(ctx context.Context) (bool, error) {
	deployment, err := get(ctx, "deployment", "rhiza-operator")
	if err != nil {
		return false, err
	}
	pods, err := get(ctx, "pods", "")
	if err != nil {
		return false, err
	}
	return operatorStoppedSnapshot(deployment, pods), nil
}

func operatorStoppedSnapshot(deployment, pods obj) bool {
	if nested(deployment, "spec", "replicas") != float64(0) {
		return false
	}
	if replicas := nested(deployment, "status", "replicas"); replicas != nil && replicas != float64(0) {
		return false
	}
	items, _ := pods["items"].([]any)
	for _, pod := range items {
		if nested(pod, "metadata", "labels", "app.kubernetes.io/name") != "rhiza-operator" {
			continue
		}
		phase := text(nested(pod, "status", "phase"))
		if phase != "Succeeded" && phase != "Failed" {
			return false
		}
	}
	return true
}

func requireOperatorStopped(ctx context.Context) error {
	stopped, err := operatorStopped(ctx)
	if err != nil {
		return err
	}
	if !stopped {
		return fmt.Errorf("test operator was not stopped for Node-driven replacement")
	}
	return nil
}

func command(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	data, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("%s command failed: %w: %s", name, err, data)
	}
	return data, nil
}
func kubectl(ctx context.Context, args ...string) ([]byte, error) {
	return command(ctx, "kubectl", append([]string{"--context", kubeContext, "-n", namespace}, args...)...)
}
func get(ctx context.Context, kind, name string) (obj, error) {
	args := []string{"get", kind}
	if name != "" {
		args = append(args, name)
	}
	data, err := kubectl(ctx, append(args, "-o", "json")...)
	if err != nil {
		return nil, err
	}
	var out obj
	err = json.Unmarshal(data, &out)
	return out, err
}
func nested(value any, keys ...string) any {
	for _, key := range keys {
		m, ok := value.(map[string]any)
		if !ok {
			return nil
		}
		value = m[key]
	}
	return value
}
func text(value any) string { s, _ := value.(string); return s }
func wait(ctx context.Context, description string, check func() (bool, error)) error {
	for {
		ok, err := check()
		if err != nil {
			return fmt.Errorf("%s: %w", description, err)
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: %w", description, ctx.Err())
		case <-time.After(time.Second):
		}
	}
}

type forward struct {
	cmd  *exec.Cmd
	port int
	done chan error
	log  *os.File
}

func (f *forward) close() { _ = f.cmd.Process.Kill(); <-f.done; _ = f.log.Close() }
func startForward(ctx context.Context, pod, out string) (*forward, error) {
	return startPortForward(ctx, "pod/"+pod, 8080, pod, out)
}

func startServiceForward(ctx context.Context, service string, remotePort int, out string) (*forward, error) {
	return startPortForward(ctx, "service/"+service, remotePort, service, out)
}

func startPortForward(ctx context.Context, target string, remotePort int, logName, out string) (*forward, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	log, err := os.CreateTemp(out, strings.ReplaceAll(logName, "/", "-")+"-portforward-*.log")
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, "kubectl", "--context", kubeContext, "-n", namespace, "port-forward", target, fmt.Sprintf("%d:%d", port, remotePort), "--address=127.0.0.1")
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		_ = log.Close()
		return nil, err
	}
	f := &forward{cmd: cmd, port: port, done: make(chan error, 1), log: log}
	go func() { f.done <- cmd.Wait() }()
	ready, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	err = wait(ready, "port-forward "+target, func() (bool, error) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 100*time.Millisecond)
		if err != nil {
			return false, nil
		}
		_ = conn.Close()
		return true, nil
	})
	if err != nil {
		f.close()
		return nil, err
	}
	return f, nil
}
func call(ctx context.Context, f *forward, path string, body any, token string) (obj, error) {
	method := http.MethodGet
	var reader io.Reader
	if body != nil {
		method = http.MethodPost
		reader = bytes.NewReader(encoded(body))
	}
	req, err := http.NewRequestWithContext(ctx, method, fmt.Sprintf("http://127.0.0.1:%d%s", f.port, path), reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("%s returned HTTP %d", path, resp.StatusCode)
	}
	var value obj
	err = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&value)
	if err == nil && text(value["error_code"]) != "" {
		return nil, fmt.Errorf("%s application error %s", path, text(value["error_code"]))
	}
	return value, err
}

func qualify(ctx context.Context) (resultErr error) {
	out := os.Getenv("FENCING_E2E_OUT")
	if out == "" {
		out = "generic-fencing-results"
	}
	if err := os.MkdirAll(out, 0700); err != nil {
		return err
	}
	events, err := os.Create(filepath.Join(out, "events.jsonl"))
	if err != nil {
		return err
	}
	defer events.Close()
	event := func(kind string, data obj) {
		data["event"] = kind
		data["time"] = time.Now().UTC()
		_ = json.NewEncoder(events).Encode(data)
	}
	var observationFaults []*observationFault
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, fault := range observationFaults {
			cleanupErr := fault.remove(cleanupCtx)
			resultErr = errors.Join(resultErr, cleanupErr)
			event("operator_observation_fault_cleanup", obj{"fault": fault, "error": fmt.Sprint(cleanupErr)})
		}
	}()
	defer func() {
		collectCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, kind := range []string{"pods", "rhizafences", "rhizaclusters", "rhizarecoveries", "configmaps", "events"} {
			if data, err := kubectl(collectCtx, "get", kind, "-o", "json"); err == nil {
				_ = os.WriteFile(filepath.Join(out, kind+".json"), data, 0600)
			}
		}
	}()
	if err := wait(ctx, "healthy source", func() (bool, error) {
		c, err := get(ctx, "rhizacluster", "rhiza")
		if err != nil {
			return false, err
		}
		return nested(c, "status", "phase") == "Healthy", nil
	}); err != nil {
		return err
	}
	sts, err := get(ctx, "statefulset", "rhiza")
	if err != nil {
		return err
	}
	binding, err := get(ctx, "rhizacluster", "rhiza")
	if err != nil {
		return err
	}
	stsUID, bindingUID := text(nested(sts, "metadata", "uid")), text(nested(binding, "metadata", "uid"))
	if stsUID == "" || bindingUID == "" {
		return fmt.Errorf("missing source workload/binding UID")
	}
	if err := verifyScratch(sts); err != nil {
		return err
	}
	if err := verifyImages(ctx, out, "source"); err != nil {
		return err
	}
	adminToken, err := generatedAdminToken(ctx, sts)
	if err != nil {
		return err
	}
	if err := scaleOperator(ctx, 0); err != nil {
		return fmt.Errorf("stop only the test operator after bootstrap: %w", err)
	}
	event("operator_paused_for_node_driven_replacement", obj{"operatorReplicas": 0, "authorityRetained": true, "chaosMeshRetained": true})
	operatorRestored := false
	defer func() {
		if operatorRestored {
			return
		}
		restoreCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		restoreErr := scaleOperator(restoreCtx, 1)
		resultErr = errors.Join(resultErr, restoreErr)
	}()
	oldForward, err := startForward(ctx, "rhiza-0", out)
	if err != nil {
		return err
	}
	oldMembership, err := readMembership(ctx, oldForward, adminToken)
	oldForward.close()
	if err != nil || oldMembership.NodeID == "" || !oldMembership.Voting || oldMembership.Pending {
		return fmt.Errorf("initial voter identity unavailable before Chaos Mesh replacement: %v", err)
	}
	oldPod, err := get(ctx, "pod", "rhiza-0")
	if err != nil {
		return err
	}
	credential, err := generatedPeerCredential(ctx, sts, oldMembership.NodeID)
	if err != nil {
		return err
	}
	destination, err := peerProbeDestination(oldMembership, credential.members)
	if err != nil {
		return err
	}
	var oldMember quepaxa.Member
	for _, member := range credential.members {
		if member.ID == oldMembership.NodeID {
			oldMember = member
		}
	}
	if oldMember.ID == "" || text(nested(oldPod, "metadata", "uid")) == "" || oldMembership.WALIdentity == "" {
		return fmt.Errorf("old peer identity is not bound to its live Pod and WAL")
	}
	oldIdentity := oldPeerIdentity{
		PodName: text(nested(oldPod, "metadata", "name")), PodUID: text(nested(oldPod, "metadata", "uid")),
		NodeName: text(nested(oldPod, "spec", "nodeName")), CreatedAt: text(nested(oldPod, "metadata", "creationTimestamp")),
		NodeID: string(oldMembership.NodeID), WALIdentity: oldMembership.WALIdentity, ConfigID: oldMembership.ConfigID,
		PublicKey: hex.EncodeToString(oldMember.PublicKey[:]), CredentialSecretName: credential.secretName, CredentialSecretUID: credential.secretUID,
	}
	probeRunID, err := randomHex(10)
	if err != nil {
		return err
	}
	controlRequestID, staleRequestID := "peer-control-"+probeRunID, "peer-stale-"+probeRunID
	controlMarker, staleMarker := "peer-control-marker-"+probeRunID, "peer-stale-marker-"+probeRunID
	controlProbe, err := runPeerProbeJob(ctx, "control", credential.secretName, credential, oldMembership.NodeID, destination.ID, oldMembership.ConfigID, controlRequestID, controlMarker, out)
	if err != nil {
		return fmt.Errorf("pre-fault actual-transport peer StageValue control: %w", err)
	}
	if !controlProbe.Accepted || !controlProbe.CleanupVerified {
		return fmt.Errorf("pre-fault peer StageValue control or run-owned cleanup was not verified")
	}
	if controlProbe.SenderPublicKey != oldIdentity.PublicKey || controlProbe.DestinationPublicKey != hex.EncodeToString(destination.PublicKey[:]) {
		return fmt.Errorf("pre-fault control did not use the captured old and survivor public identities")
	}
	event("retired_peer_probe_control", obj{"oldIdentity": oldIdentity, "peerProbe": controlProbe})
	if err := replaceInitialPodWithChaos(ctx, oldMembership, adminToken, out, event); err != nil {
		return err
	}
	postFaultMembership, statusErr := membershipForNode(ctx, destination.ID, []string{"rhiza-1", "rhiza-2"}, adminToken, out)
	if statusErr != nil || postFaultMembership.ClusterID != "chaos-source" || postFaultMembership.ConfigID <= oldMembership.ConfigID {
		return fmt.Errorf("post-fault survivor membership could not be bound to the replaced incarnation: %v", statusErr)
	}
	if !containsNode(postFaultMembership.Voters, destination.ID) || containsNode(postFaultMembership.Voters, oldMembership.NodeID) {
		return fmt.Errorf("post-fault membership did not retain the probe target while removing the old identity")
	}
	staleProbe, err := runPeerProbeJob(ctx, "stale", credential.secretName, credential, oldMembership.NodeID, destination.ID, postFaultMembership.ConfigID, staleRequestID, staleMarker, out)
	if err != nil {
		return fmt.Errorf("post-fault retired peer actual-transport StageValue: %w", err)
	}
	if staleProbe.Accepted || staleProbe.ErrorPhase != "response-decode" || staleProbe.ErrorCauseType != "*errors.errorString" || !staleProbe.CleanupVerified {
		return fmt.Errorf("post-fault peer authentication rejection or run-owned cleanup was not verified")
	}
	if staleProbe.SenderPublicKey != oldIdentity.PublicKey || staleProbe.DestinationPublicKey != controlProbe.DestinationPublicKey {
		return fmt.Errorf("post-fault rejection did not use the same captured identities")
	}
	event("retired_peer_probe_rejected", obj{"oldIdentity": oldIdentity, "peerProbe": staleProbe})
	probeRequestIDs := []string{controlRequestID, staleRequestID}
	probeMarkers := []string{controlMarker, staleMarker}
	var oldUIDs []string
	f, err := startForward(ctx, "rhiza-0", out)
	if err != nil {
		return err
	}
	defer f.close()
	if _, err = call(ctx, f, "/sql/execute", obj{"request_id": "fencing-create", "sql": "CREATE TABLE fencing_items (id INTEGER PRIMARY KEY, value TEXT)"}, adminToken); err != nil {
		return err
	}
	if _, err = call(ctx, f, "/sql/execute", obj{"request_id": "fencing-ack", "sql": "INSERT INTO fencing_items VALUES (1, 'acknowledged')"}, adminToken); err != nil {
		return err
	}
	if _, err = call(ctx, f, "/sql/execute", obj{"request_id": "anchor-evidence-create", "sql": "CREATE TABLE trust_state (id INTEGER PRIMARY KEY, epoch INTEGER NOT NULL, token TEXT NOT NULL)"}, adminToken); err != nil {
		return err
	}
	if _, err = call(ctx, f, "/sql/execute", obj{"request_id": "anchor-evidence-seed", "sql": "INSERT INTO trust_state VALUES (1, 1, 'initial')"}, adminToken); err != nil {
		return err
	}
	if _, err = call(ctx, f, "/graph/execute", obj{"request_id": "fencing-graph-ack", "cypher": "CREATE (:FencingItem {id: 1, value: 'acknowledged'})"}, adminToken); err != nil {
		return err
	}
	if err := checkRows(ctx, f, adminToken, `[[1,"acknowledged"]]`); err != nil {
		return fmt.Errorf("SQL ACK readback while operator is stopped: %w", err)
	}
	if err := checkGraphRows(ctx, f, adminToken, `[[1,"acknowledged"]]`); err != nil {
		return fmt.Errorf("Graph ACK readback while operator is stopped: %w", err)
	}
	status, err := readMembership(ctx, f, adminToken)
	if err != nil {
		return fmt.Errorf("read source identity for independent archive check: %w", err)
	}
	archive, err := verifyDurableArchive(ctx, out, sourceArchiveIdentity(status.ConfigID), []string{"fencing-ack", "fencing-graph-ack"}, probeRequestIDs, probeMarkers)
	if err != nil {
		return fmt.Errorf("independent Versity S3 ACK evidence while operator is stopped: %w", err)
	}
	if err := requireOperatorStopped(ctx); err != nil {
		return err
	}
	event("source_acknowledged", obj{"rows": 1, "operatorReplicas": 0, "independentArchiveTip": archive.Tip, "independentArchiveStart": archive.Start, "independentArchiveThrough": archive.Through, "independentArchivePrefixSHA256": archive.PrefixSHA256, "archivedRequestIDs": []string{"fencing-ack", "fencing-graph-ack"}, "absentPeerProbeRequestIDs": probeRequestIDs, "absentPeerProbeMarkers": []string{fmt.Sprintf("%x", sha256.Sum256([]byte(controlMarker))), fmt.Sprintf("%x", sha256.Sum256([]byte(staleMarker)))}})
	event("source_graph_acknowledged", obj{"rows": 1, "operatorReplicas": 0})
	if err := scaleOperator(ctx, 1); err != nil {
		return fmt.Errorf("restore operator after Node-driven replacement/readback: %w", err)
	}
	operatorRestored = true
	event("operator_restored_after_node_driven_replacement", obj{"operatorReplicas": 1})
	incarnations := map[string]string{}
	var initialUIDs []string
	for i := 0; i < 3; i++ {
		pod := fmt.Sprintf("rhiza-%d", i)
		p, err := get(ctx, "pod", pod)
		if err != nil {
			return err
		}
		incarnations[pod] = incarnationSnapshot(p)
		initialUIDs = append(initialUIDs, text(nested(p, "metadata", "uid")))
	}
	if _, err = kubectl(ctx, "scale", "deployment", "rhiza-fencer", "--replicas=0"); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(5 * time.Second):
	}
	if _, err = kubectl(ctx, "scale", "deployment", "rhiza-fencer", "--replicas=1"); err != nil {
		return err
	}
	if _, err = kubectl(ctx, "rollout", "status", "deployment/rhiza-fencer", "--timeout=120s"); err != nil {
		return err
	}
	if _, err = call(ctx, f, "/sql/query", obj{"sql": "SELECT id FROM fencing_items", "consistency": "linearizable"}, adminToken); err != nil {
		return fmt.Errorf("authority restart stopped healthy engine: %w", err)
	}
	for pod, before := range incarnations {
		p, err := get(ctx, "pod", pod)
		if err != nil {
			return err
		}
		after := incarnationSnapshot(p)
		if before != after {
			return fmt.Errorf("source incarnation changed during authority restart: %s", pod)
		}
	}
	event("authority_restart_preserved_incarnations", obj{"voters": 3})
	if err := verifyImages(ctx, out, "authority-restarted"); err != nil {
		return err
	}
	onlineCtx, onlineCancel := context.WithTimeout(ctx, 6*time.Minute)
	current, err := qualifyOnline(onlineCtx, f, adminToken, stsUID, bindingUID, out, event, &observationFaults)
	onlineCancel()
	if err != nil {
		return fmt.Errorf("online voter qualification: %w", err)
	}
	for _, target := range current {
		oldUIDs = append(oldUIDs, target.PodUID)
		partition, err := partitionPeer(ctx, target)
		if err != nil {
			return err
		}
		event("generation_peer_partition", partition)
		fault, err := blockOperatorObservation(ctx, target)
		if fault != nil {
			observationFaults = append(observationFaults, fault)
		}
		if err != nil {
			return err
		}
		event("generation_operator_observation_fault", obj{"fault": fault})
	}
	if err := wait(ctx, "generic generation fence proof", func() (bool, error) {
		list, err := get(ctx, "rhizafences", "")
		if err != nil {
			return false, err
		}
		items, _ := list["items"].([]any)
		for _, item := range items {
			if nested(item, "spec", "scope") != "Generation" || nested(item, "status", "proof") == nil {
				continue
			}
			return true, verifyGenerationProof(ctx, item, stsUID, bindingUID, oldUIDs, out, current...)
		}
		return false, nil
	}); err != nil {
		return err
	}
	event("generic_generation_proof", obj{"oldUIDs": oldUIDs})
	preRuntime, err := observeDRAssignmentsBeforeRuntime(ctx, stsUID, out)
	if err != nil {
		return fmt.Errorf("observe DR assignments before runtime binding: %w", err)
	}
	if err := os.WriteFile(filepath.Join(out, "dr-pre-runtime-assignment.json"), encoded(preRuntime), 0600); err != nil {
		return err
	}
	if err := wait(ctx, "new generation healthy", func() (bool, error) {
		c, err := get(ctx, "rhizacluster", "rhiza")
		if err != nil {
			return false, err
		}
		return nested(c, "status", "phase") == "Healthy" && nested(c, "status", "activeClusterID") != "chaos-source" && text(nested(c, "status", "activeClusterID")) != "", nil
	}); err != nil {
		return err
	}
	if err := verifyImages(ctx, out, "recovered"); err != nil {
		return err
	}
	newForward, err := startForward(ctx, "rhiza-0", out)
	if err != nil {
		return err
	}
	defer newForward.close()
	secret, err := get(ctx, "statefulset", "rhiza")
	if err != nil {
		return err
	}
	containers, _ := nested(secret, "spec", "template", "spec", "containers").([]any)
	if len(containers) != 1 {
		return fmt.Errorf("unexpected target container count")
	}
	env, _ := nested(containers[0], "env").([]any)
	secretName := ""
	for _, v := range env {
		if nested(v, "name") == "RHIZA_ADMIN_TOKEN" {
			secretName = text(nested(v, "valueFrom", "secretKeyRef", "name"))
		}
	}
	if secretName == "" {
		return fmt.Errorf("target admin Secret missing")
	}
	secret, err = get(ctx, "secret", secretName)
	if err != nil {
		return err
	}
	raw, err := base64.StdEncoding.DecodeString(text(nested(secret, "data", "admin")))
	if err != nil {
		return err
	}
	targetAdmin := string(raw)
	root, err := get(ctx, "rhizacluster", "rhiza")
	if err != nil {
		return err
	}
	targetClusterID := text(nested(root, "status", "activeClusterID"))
	drProof, err := verifyDRSuccessor(ctx, stsUID, targetClusterID, targetAdmin, out)
	if err != nil {
		return fmt.Errorf("approved DR successor qualification: %w", err)
	}
	if err := os.WriteFile(filepath.Join(out, "dr-qualification.json"), encoded(drProof), 0600); err != nil {
		return err
	}
	trustRows, err := call(ctx, newForward, "/sql/query", obj{"sql": "SELECT epoch,token FROM trust_state WHERE id = 1", "consistency": "linearizable"}, targetAdmin)
	if err != nil || string(encoded(trustRows["rows"])) != `[[1,"initial"]]` {
		return fmt.Errorf("approved DR trust_state readback differs: %v", err)
	}
	if _, err = call(ctx, newForward, "/sql/execute", obj{"request_id": "dr-runtime-bound-write", "sql": "UPDATE trust_state SET epoch = 2, token = 'dr-runtime-bound' WHERE id = 1"}, targetAdmin); err != nil {
		return fmt.Errorf("DR runtime-bound write: %w", err)
	}
	trustReadback, err := call(ctx, newForward, "/sql/query", obj{"sql": "SELECT epoch,token FROM trust_state WHERE id = 1", "consistency": "linearizable"}, targetAdmin)
	if err != nil || string(encoded(trustReadback["rows"])) != `[[2,"dr-runtime-bound"]]` {
		return fmt.Errorf("DR runtime-bound linearizable ACK readback differs: %v", err)
	}
	event("dr_successor_runtime_ack", obj{"drTargetClusterID": targetClusterID, "drWriteEpoch": 2, "drReadbackSHA256": fmt.Sprintf("%x", sha256.Sum256(encoded(trustReadback["rows"]))), "drAssignmentsObserved": drProof["preRuntimeAssignmentObserved"], "linearizable": true, "assignmentCount": len(drProof["assignments"].([]obj))})
	rows, err := call(ctx, newForward, "/sql/query", obj{"sql": "SELECT id,value FROM fencing_items ORDER BY id", "consistency": "linearizable"}, targetAdmin)
	if err != nil {
		return err
	}
	values, _ := rows["rows"].([]any)
	if len(values) != 3 {
		return fmt.Errorf("ACK row count differs")
	}
	if string(encoded(values)) != `[[1,"acknowledged"],[2,"quorum-survived"],[3,"learner-promoted"]]` {
		return fmt.Errorf("ACK row differs")
	}
	if _, err = call(ctx, newForward, "/sql/execute", obj{"request_id": "fencing-new-generation-write", "sql": "INSERT INTO fencing_items VALUES (4, 'recovered')"}, targetAdmin); err != nil {
		return err
	}
	readback, err := call(ctx, newForward, "/sql/query", obj{"sql": "SELECT id,value FROM fencing_items ORDER BY id", "consistency": "linearizable"}, targetAdmin)
	if err != nil {
		return err
	}
	if string(encoded(readback["rows"])) != `[[1,"acknowledged"],[2,"quorum-survived"],[3,"learner-promoted"],[4,"recovered"]]` {
		return fmt.Errorf("new-generation linearizable readback differs")
	}
	event("new_generation_linearizable_readback", obj{"rows": readback["rows"], "appliedSlot": readback["applied_slot"], "consensusTip": readback["consensus_tip"]})
	data, err := command(ctx, "docker", "exec", kindNode, "crictl", "ps", "-a", "-o", "json")
	if err != nil {
		return err
	}
	var runtime obj
	if err = json.Unmarshal(data, &runtime); err != nil {
		return err
	}
	runtimes, _ := runtime["containers"].([]any)
	retiredUIDs := uniqueUIDs(oldIdentity.PodUID, oldUIDs, initialUIDs)
	for _, container := range runtimes {
		if nested(container, "state") != "CONTAINER_RUNNING" {
			continue
		}
		uid := text(nested(container, "labels", "io.kubernetes.pod.uid"))
		for _, old := range retiredUIDs {
			if uid == old {
				return fmt.Errorf("old runtime remains live after proof")
			}
		}
	}
	event("PASS", obj{"sourceACKRows": 3, "onlineVoterReplacement": true, "newGenerationWrite": true, "oldRuntimesGone": true, "executor": "generic-cooperative"})
	return nil
}

type drPreRuntimeAssignment struct {
	PodName        string `json:"podName"`
	PodUID         string `json:"podUID"`
	NodeID         string `json:"nodeID"`
	CredentialUID  string `json:"credentialUID"`
	LedgerUID      string `json:"ledgerUID"`
	MembershipHash string `json:"membershipHash"`
}

type drPreRuntimeObservation struct {
	Version             int                      `json:"version"`
	CapturedAt          string                   `json:"capturedAt"`
	TargetClusterID     string                   `json:"targetClusterID"`
	OperationID         string                   `json:"operationID"`
	RequestHash         string                   `json:"requestHash"`
	Receipt             recoveryanchor.Receipt   `json:"receipt"`
	Assignments         []drPreRuntimeAssignment `json:"assignments"`
	ExpectedAssignments []drPreRuntimeAssignment `json:"-"`
}

func observeDRAssignmentsBeforeRuntime(ctx context.Context, sourceStatefulSetUID, out string) (drPreRuntimeObservation, error) {
	var accumulated drPreRuntimeObservation
	fenceValidated := false
	for {
		observation, _, err := readDRPreRuntimeObservation(ctx, sourceStatefulSetUID, out, !fenceValidated)
		if err != nil {
			return drPreRuntimeObservation{}, err
		}
		if len(observation.ExpectedAssignments) == 3 {
			fenceValidated = true
		}
		accumulated, err = mergeDRPreRuntimeObservation(accumulated, observation)
		if err != nil {
			return drPreRuntimeObservation{}, err
		}
		if len(accumulated.ExpectedAssignments) == 3 && sameDRPreRuntimeAssignments(accumulated.ExpectedAssignments, accumulated.Assignments) {
			return accumulated, nil
		}
		select {
		case <-ctx.Done():
			return drPreRuntimeObservation{}, fmt.Errorf("no live pre-runtime assignment observation before timeout: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func mergeDRPreRuntimeObservation(previous, next drPreRuntimeObservation) (drPreRuntimeObservation, error) {
	if next.TargetClusterID == "" {
		return previous, nil
	}
	if previous.TargetClusterID != "" && (previous.Version != next.Version || previous.TargetClusterID != next.TargetClusterID || previous.OperationID != next.OperationID || previous.RequestHash != next.RequestHash || previous.Receipt != next.Receipt) {
		return drPreRuntimeObservation{}, fmt.Errorf("DR pre-runtime approval changed during observation")
	}
	if previous.TargetClusterID == "" {
		previous.Version, previous.CapturedAt = next.Version, next.CapturedAt
		previous.TargetClusterID, previous.OperationID = next.TargetClusterID, next.OperationID
		previous.RequestHash, previous.Receipt = next.RequestHash, next.Receipt
	}
	if len(next.ExpectedAssignments) > 0 {
		for _, old := range previous.ExpectedAssignments {
			found := false
			for _, current := range next.ExpectedAssignments {
				if old.PodName == current.PodName {
					if old != current {
						return drPreRuntimeObservation{}, fmt.Errorf("DR assignment identity or credential changed for previously observed Pod %s", old.PodName)
					}
					found = true
				}
			}
			if !found {
				return drPreRuntimeObservation{}, fmt.Errorf("retained DR assignment disappeared for previously observed Pod %s", old.PodName)
			}
		}
		previous.ExpectedAssignments = append([]drPreRuntimeAssignment(nil), next.ExpectedAssignments...)
	}
	for _, observed := range next.Assignments {
		found := false
		for _, prior := range previous.Assignments {
			if prior.PodName == observed.PodName {
				if prior != observed {
					return drPreRuntimeObservation{}, fmt.Errorf("pre-runtime Pod identity changed for %s", observed.PodName)
				}
				found = true
			}
		}
		if !found {
			previous.Assignments = append(previous.Assignments, observed)
		}
	}
	sort.Slice(previous.ExpectedAssignments, func(i, j int) bool {
		return previous.ExpectedAssignments[i].PodName < previous.ExpectedAssignments[j].PodName
	})
	sort.Slice(previous.Assignments, func(i, j int) bool { return previous.Assignments[i].PodName < previous.Assignments[j].PodName })
	return previous, nil
}

func sameDRPreRuntimeAssignments(expected, observed []drPreRuntimeAssignment) bool {
	if len(expected) != len(observed) {
		return false
	}
	byName := make(map[string]drPreRuntimeAssignment, len(observed))
	for _, assignment := range observed {
		if _, exists := byName[assignment.PodName]; exists {
			return false
		}
		byName[assignment.PodName] = assignment
	}
	for _, assignment := range expected {
		if byName[assignment.PodName] != assignment {
			return false
		}
	}
	return true
}

func readDRPreRuntimeObservation(ctx context.Context, sourceStatefulSetUID, out string, validateFence bool) (drPreRuntimeObservation, bool, error) {
	root, err := get(ctx, "rhizacluster", "rhiza")
	if err != nil {
		return drPreRuntimeObservation{}, false, nil
	}
	rootUID := text(nested(root, "metadata", "uid"))
	recoveries, err := get(ctx, "rhizarecoveries", "")
	if err != nil {
		return drPreRuntimeObservation{}, false, nil
	}
	items, _ := recoveries["items"].([]any)
	var child obj
	for _, raw := range items {
		item, _ := raw.(map[string]any)
		if nested(item, "spec", "anchorID") != recoveryAnchorID || nested(item, "spec", "sourceClusterID") != "chaos-source" || nested(item, "status", "statefulSetUID") != sourceStatefulSetUID {
			continue
		}
		if child != nil {
			return drPreRuntimeObservation{}, false, fmt.Errorf("multiple anchored recoveries match the source workload")
		}
		child = item
	}
	if child == nil || nested(child, "status", "target") == nil || nested(child, "status", "anchorRequest") == nil || nested(child, "status", "anchorReceipt") == nil || rootUID == "" {
		return drPreRuntimeObservation{}, false, nil
	}
	target := text(nested(child, "status", "target"))
	var request recoveryanchor.Request
	var receipt recoveryanchor.Receipt
	if err := json.Unmarshal(encoded(nested(child, "status", "anchorRequest")), &request); err != nil {
		return drPreRuntimeObservation{}, false, err
	}
	if err := json.Unmarshal(encoded(nested(child, "status", "anchorReceipt")), &receipt); err != nil {
		return drPreRuntimeObservation{}, false, err
	}
	requestHash, err := recoveryanchor.RequestHash(request)
	if err != nil || requestHash != receipt.RequestHash || request.TargetClusterID != target || request.StatefulSetUID != sourceStatefulSetUID || request.AnchorID != recoveryAnchorID || receipt.Target.ClusterID != target || receipt.Generation == 0 {
		return drPreRuntimeObservation{}, false, fmt.Errorf("pre-runtime anchor approval pair differs from its recovery target")
	}
	secretName := text(nested(child, "status", "secretName"))
	if secretName == "" {
		return drPreRuntimeObservation{}, false, nil
	}
	secret, err := get(ctx, "secret", secretName)
	if err != nil {
		return drPreRuntimeObservation{}, false, nil
	}
	if nested(secret, "immutable") != true {
		return drPreRuntimeObservation{}, false, fmt.Errorf("target credential Secret is not immutable")
	}
	credentialUID := text(nested(secret, "metadata", "uid"))
	encodedMembers, err := base64.StdEncoding.DecodeString(text(nested(secret, "data", "members")))
	if err != nil || credentialUID == "" {
		return drPreRuntimeObservation{}, false, fmt.Errorf("immutable target credential membership is unavailable")
	}
	var members []quepaxa.Member
	if err := json.Unmarshal(encodedMembers, &members); err != nil || len(members) != 3 || recovery.NewMembershipRecord(target, members, text(nested(child, "spec", "durability"))) != request.TargetMembership {
		return drPreRuntimeObservation{}, false, fmt.Errorf("approved target membership differs from immutable credential members")
	}
	memberByID := make(map[string]bool, len(members))
	for _, member := range members {
		memberByID[string(member.ID)] = true
	}
	ledgerName := "rhiza-fence-ledger-" + fixtureHashJSON([]string{sourceStatefulSetUID, target})[:40]
	ledgerConfig, err := get(ctx, "configmap", ledgerName)
	if err != nil {
		return drPreRuntimeObservation{}, false, nil
	}
	ledgerUID := text(nested(ledgerConfig, "metadata", "uid"))
	var ledger obj
	if err := json.Unmarshal([]byte(text(nested(ledgerConfig, "data", "ledger.json"))), &ledger); err != nil || ledgerUID == "" || nested(ledger, "statefulSetUID") != sourceStatefulSetUID || nested(ledger, "ledger", "sourceClusterID") != target || nested(ledger, "ledger", "bootstrap", "rootUID") != rootUID {
		return drPreRuntimeObservation{}, false, fmt.Errorf("DR bootstrap ledger is not bound to the retained workload")
	}
	operations, _ := nested(ledger, "ledger", "bootstrap", "operations").(map[string]any)
	var selected obj
	var operationID string
	for id, raw := range operations {
		operation, _ := raw.(map[string]any)
		if nested(operation, "purpose") == "dr-successor" {
			if selected != nil {
				return drPreRuntimeObservation{}, false, fmt.Errorf("multiple DR successor operations were retained")
			}
			selected, operationID = operation, id
		}
	}
	if selected == nil || operationID == "" || nested(selected, "sourceClusterID") != target || nested(selected, "rootUID") != rootUID || nested(selected, "statefulSetUID") != sourceStatefulSetUID {
		return drPreRuntimeObservation{}, false, nil
	}
	assignments, _ := nested(selected, "assignments").(map[string]any)
	if len(assignments) > 3 {
		return drPreRuntimeObservation{}, false, fmt.Errorf("DR successor retained more than three assignments")
	}
	if validateFence && len(assignments) == 3 {
		if err := validateDRRecoveryFence(ctx, child, request, sourceStatefulSetUID, rootUID, out); err != nil {
			return drPreRuntimeObservation{}, false, err
		}
	}
	if len(assignments) == 0 {
		return drPreRuntimeObservation{Version: 1, CapturedAt: time.Now().UTC().Format(time.RFC3339Nano), TargetClusterID: target, OperationID: operationID, RequestHash: requestHash, Receipt: receipt}, false, nil
	}
	runs, _ := nested(ledger, "ledger", "runs").(map[string]any)
	observation := drPreRuntimeObservation{Version: 1, CapturedAt: time.Now().UTC().Format(time.RFC3339Nano), TargetClusterID: target, OperationID: operationID, RequestHash: requestHash, Receipt: receipt, Assignments: make([]drPreRuntimeAssignment, 0, 3), ExpectedAssignments: make([]drPreRuntimeAssignment, 0, len(assignments))}
	seenNodes := map[string]bool{}
	for _, raw := range assignments {
		assignment, _ := raw.(map[string]any)
		approval := nested(assignment, "drApproval")
		podName, podUID, nodeID := text(nested(assignment, "podName")), text(nested(assignment, "podUID")), text(nested(assignment, "nodeID"))
		candidate := drPreRuntimeAssignment{PodName: podName, PodUID: podUID, NodeID: nodeID, CredentialUID: credentialUID, LedgerUID: ledgerUID, MembershipHash: fixtureHashJSON(members)}
		if nested(assignment, "purpose") != "dr-successor" || nested(assignment, "operationID") != operationID || nested(assignment, "rootUID") != rootUID || nested(assignment, "statefulSetUID") != sourceStatefulSetUID || nested(assignment, "sourceClusterID") != target || nested(assignment, "ledgerUID") != ledgerUID || nested(assignment, "credentialName") != secretName || nested(assignment, "credentialUID") != credentialUID || nested(assignment, "membershipHash") != fixtureHashJSON(members) || nested(approval, "request") == nil || nested(approval, "receipt") == nil || !bytes.Equal(encoded(nested(approval, "request")), encoded(request)) || !bytes.Equal(encoded(nested(approval, "receipt")), encoded(receipt)) || !memberByID[nodeID] || nodeID != podName || seenNodes[nodeID] || podName == "" || podUID == "" {
			return drPreRuntimeObservation{}, false, fmt.Errorf("pending-init assignment does not match approval, credential membership, or immutable UIDs")
		}
		seenNodes[nodeID] = true
		observation.ExpectedAssignments = append(observation.ExpectedAssignments, candidate)
		pod, err := get(ctx, "pod", podName)
		if err != nil {
			continue
		}
		if text(nested(pod, "metadata", "uid")) != podUID {
			return drPreRuntimeObservation{}, false, fmt.Errorf("assigned Pod %s was replaced before pre-runtime observation", podName)
		}
		if nested(pod, "metadata", "deletionTimestamp") != nil || !podControlledByStatefulSet(pod, sourceStatefulSetUID) {
			return drPreRuntimeObservation{}, false, fmt.Errorf("assigned Pod %s is deleting or no longer controlled by the exact StatefulSet", podName)
		}
		initStatuses, _ := nested(pod, "status", "initContainerStatuses").([]any)
		if len(initStatuses) != 1 || nested(initStatuses[0], "name") != "rhiza-prepare" || nested(initStatuses[0], "state", "terminated") != nil || nested(initStatuses[0], "state", "waiting") == nil && nested(initStatuses[0], "state", "running") == nil {
			continue
		}
		if len(listAny(nested(pod, "status", "containerStatuses"))) != 0 {
			continue
		}
		runtimeBound := false
		for _, run := range runs {
			identity := nested(run, "identity")
			if nested(identity, "podUID") == podUID || nested(identity, "nodeID") == nodeID {
				runtimeBound = true
				break
			}
		}
		if !runtimeBound {
			observation.Assignments = append(observation.Assignments, candidate)
		}
	}
	sort.Slice(observation.ExpectedAssignments, func(i, j int) bool {
		return observation.ExpectedAssignments[i].PodName < observation.ExpectedAssignments[j].PodName
	})
	sort.Slice(observation.Assignments, func(i, j int) bool { return observation.Assignments[i].PodName < observation.Assignments[j].PodName })
	return observation, len(assignments) == 3 && sameDRPreRuntimeAssignments(observation.ExpectedAssignments, observation.Assignments), nil
}

func validateDRRecoveryFence(ctx context.Context, child obj, request recoveryanchor.Request, sourceStatefulSetUID, rootUID, out string) error {
	fence := nested(child, "spec", "fence")
	var exactFence operator.Fence
	if err := json.Unmarshal(encoded(fence), &exactFence); err != nil {
		return fmt.Errorf("decode exact recovery fence: %w", err)
	}
	if request.FenceHash != fixtureHashJSON(exactFence) || !exactFence.Confirmed || exactFence.ClusterID != request.SourceClusterID || exactFence.StatefulSetUID != sourceStatefulSetUID {
		return fmt.Errorf("approved request does not bind the exact recovery fence")
	}
	proofBytes, err := os.ReadFile(filepath.Join(out, "validated-generation-proof.json"))
	if err != nil {
		return fmt.Errorf("independently validated generation proof is unavailable: %w", err)
	}
	var generationProof proofView
	if err := json.Unmarshal(proofBytes, &generationProof); err != nil || generationProof.Request.Scope != operator.ScopeGeneration || generationProof.Request.SourceClusterID != request.SourceClusterID || generationProof.Request.StatefulSetUID != sourceStatefulSetUID || generationProof.Proof.ProofID == "" || generationProof.Proof.ProofID != text(nested(fence, "evidence")) {
		return fmt.Errorf("recovery fence differs from independently validated generation proof")
	}
	proofItem := obj{"spec": generationProof.Request, "status": obj{"proof": generationProof.Proof}}
	if err := verifyGenerationProof(ctx, proofItem, sourceStatefulSetUID, rootUID, generationProof.OldUIDs, out); err != nil {
		return fmt.Errorf("generation proof revalidation: %w", err)
	}
	return nil
}

func podControlledByStatefulSet(pod obj, statefulSetUID string) bool {
	owners, _ := nested(pod, "metadata", "ownerReferences").([]any)
	if len(owners) != 1 {
		return false
	}
	owner, _ := owners[0].(map[string]any)
	return nested(owner, "kind") == "StatefulSet" && nested(owner, "uid") == statefulSetUID && nested(owner, "controller") == true
}

func verifyDRSuccessor(ctx context.Context, sourceStatefulSetUID, targetClusterID, targetAdmin string, out string) (obj, error) {
	if targetClusterID == "" || targetClusterID == "chaos-source" {
		return nil, fmt.Errorf("active target cluster is not a new generation")
	}
	root, err := get(ctx, "rhizacluster", "rhiza")
	if err != nil {
		return nil, err
	}
	rootUID := text(nested(root, "metadata", "uid"))
	if rootUID == "" || nested(root, "spec", "anchorID") != recoveryAnchorID {
		return nil, fmt.Errorf("root binding UID or anchor ID is missing")
	}
	recoveries, err := get(ctx, "rhizarecoveries", "")
	if err != nil {
		return nil, err
	}
	items, _ := recoveries["items"].([]any)
	var child obj
	for _, raw := range items {
		item, _ := raw.(map[string]any)
		if nested(item, "spec", "anchorID") == recoveryAnchorID && nested(item, "status", "target") == targetClusterID {
			if child != nil {
				return nil, fmt.Errorf("multiple anchored recoveries claim the active target")
			}
			child = item
		}
	}
	if child == nil || nested(child, "status", "stage") != "Complete" || nested(child, "status", "statefulSetUID") != sourceStatefulSetUID {
		return nil, fmt.Errorf("completed anchored recovery for the active target is missing")
	}
	requestData, receiptData := nested(child, "status", "anchorRequest"), nested(child, "status", "anchorReceipt")
	if requestData == nil || receiptData == nil {
		return nil, fmt.Errorf("recovery status does not retain both approval components")
	}
	var request recoveryanchor.Request
	var receipt recoveryanchor.Receipt
	if err := json.Unmarshal(encoded(requestData), &request); err != nil {
		return nil, fmt.Errorf("decode retained request: %w", err)
	}
	if err := json.Unmarshal(encoded(receiptData), &receipt); err != nil {
		return nil, fmt.Errorf("decode retained receipt: %w", err)
	}
	requestHash, err := recoveryanchor.RequestHash(request)
	if err != nil || requestHash != receipt.RequestHash || request.AnchorID != recoveryAnchorID || request.TargetClusterID != targetClusterID || request.StatefulSetUID != sourceStatefulSetUID || receipt.AnchorID != recoveryAnchorID || receipt.OperationID != request.OperationID || receipt.Generation == 0 || receipt.Target.ClusterID != targetClusterID {
		return nil, fmt.Errorf("retained approval pair is incomplete or does not match the completed target")
	}
	if request.Fork.Tip == 0 || request.Fork.ManifestHash == "" || request.Fork.PrefixHash == "" || request.TargetMembership.Cluster != targetClusterID {
		return nil, fmt.Errorf("approved recovery fork or membership is incomplete")
	}
	if err := validateDRRecoveryFence(ctx, child, request, sourceStatefulSetUID, rootUID, out); err != nil {
		return nil, err
	}

	anchorConfig, err := get(ctx, "configmap", recoveryAnchorConfigMap)
	if err != nil {
		return nil, err
	}
	var anchorRecord recoveryanchor.Record
	if err := json.Unmarshal([]byte(text(nested(anchorConfig, "data", "anchor.json"))), &anchorRecord); err != nil {
		return nil, fmt.Errorf("decode committed anchor record: %w", err)
	}
	if anchorRecord.Version != recoveryanchor.Version1 || anchorRecord.PendingWrite || anchorRecord.Transition != nil || anchorRecord.LastTransition == nil || *anchorRecord.LastTransition != receipt || anchorRecord.Generation != receipt.Generation || anchorRecord.Binding != receipt.Target {
		return nil, fmt.Errorf("anchor backend is not at the exact committed approval")
	}

	ledgerName := "rhiza-fence-ledger-" + fixtureHashJSON([]string{sourceStatefulSetUID, targetClusterID})[:40]
	ledgerConfig, err := get(ctx, "configmap", ledgerName)
	if err != nil {
		return nil, err
	}
	var ledger obj
	if err := json.Unmarshal([]byte(text(nested(ledgerConfig, "data", "ledger.json"))), &ledger); err != nil {
		return nil, fmt.Errorf("decode retained fencing ledger: %w", err)
	}
	if nested(ledger, "statefulSetUID") != sourceStatefulSetUID || nested(ledger, "ledger", "sourceClusterID") != targetClusterID || nested(ledger, "ledger", "bootstrap", "rootUID") != rootUID || nested(ledger, "ledger", "bootstrap", "statefulSetUID") != sourceStatefulSetUID {
		return nil, fmt.Errorf("bootstrap ledger binding differs from the active workload")
	}
	operations, _ := nested(ledger, "ledger", "bootstrap", "operations").(map[string]any)
	var selected map[string]any
	var selectedID string
	for operationID, raw := range operations {
		operation, _ := raw.(map[string]any)
		if nested(operation, "purpose") != "dr-successor" {
			continue
		}
		if selected != nil {
			return nil, fmt.Errorf("multiple DR successor operations are retained for one recovery")
		}
		selected, selectedID = operation, operationID
	}
	if selected == nil || selectedID == "" || nested(selected, "operationID") != selectedID || nested(selected, "sourceClusterID") != targetClusterID || nested(selected, "rootUID") != rootUID || nested(selected, "statefulSetUID") != sourceStatefulSetUID {
		return nil, fmt.Errorf("retained DR successor operation is missing or misbound")
	}
	assignments, _ := nested(selected, "assignments").(map[string]any)
	if len(assignments) != 3 {
		return nil, fmt.Errorf("DR successor operation has %d assignments, want three", len(assignments))
	}
	secretName := text(nested(child, "status", "secretName"))
	if secretName == "" {
		return nil, fmt.Errorf("completed recovery target credential Secret is missing")
	}
	credential, err := get(ctx, "secret", secretName)
	if err != nil {
		return nil, err
	}
	credentialUID := text(nested(credential, "metadata", "uid"))
	if credentialUID == "" {
		return nil, fmt.Errorf("target credential UID is missing")
	}
	memberIDs := make(map[string]bool, 3)
	currentAssignments := make(map[string]drPreRuntimeAssignment, 3)
	for _, raw := range assignments {
		assignment, _ := raw.(map[string]any)
		approval := nested(assignment, "drApproval")
		if nested(assignment, "purpose") != "dr-successor" || nested(assignment, "operationID") != selectedID || nested(assignment, "rootUID") != rootUID || nested(assignment, "statefulSetUID") != sourceStatefulSetUID || nested(assignment, "sourceClusterID") != targetClusterID || nested(assignment, "ledgerUID") != text(nested(ledgerConfig, "metadata", "uid")) || nested(assignment, "credentialName") != secretName || nested(assignment, "credentialUID") != credentialUID || nested(approval, "request") == nil || nested(approval, "receipt") == nil || !bytes.Equal(encoded(nested(approval, "request")), encoded(request)) || !bytes.Equal(encoded(nested(approval, "receipt")), encoded(receipt)) {
			return nil, fmt.Errorf("DR successor assignment differs from retained approval or immutable resources")
		}
		podName, podUID, nodeID := text(nested(assignment, "podName")), text(nested(assignment, "podUID")), text(nested(assignment, "nodeID"))
		if podName == "" || podUID == "" || nodeID == "" || memberIDs[nodeID] {
			return nil, fmt.Errorf("DR assignment has an absent or duplicate Pod/member identity")
		}
		currentAssignments[podUID] = drPreRuntimeAssignment{PodName: podName, PodUID: podUID, NodeID: nodeID, CredentialUID: credentialUID, LedgerUID: text(nested(assignment, "ledgerUID")), MembershipHash: text(nested(assignment, "membershipHash"))}
		pod, err := get(ctx, "pod", podName)
		if err != nil {
			return nil, err
		}
		if text(nested(pod, "metadata", "uid")) != podUID || nested(pod, "metadata", "deletionTimestamp") != nil || !podControlledByStatefulSet(pod, sourceStatefulSetUID) {
			return nil, fmt.Errorf("DR assignment no longer names the exact live Pod UID")
		}
		initStatuses, _ := nested(pod, "status", "initContainerStatuses").([]any)
		if len(initStatuses) != 1 || nested(initStatuses[0], "name") != "rhiza-prepare" || nested(initStatuses[0], "state", "terminated", "exitCode") != float64(0) || nested(initStatuses[0], "state", "terminated", "reason") != "Completed" {
			return nil, fmt.Errorf("DR Pod init did not terminate successfully")
		}
		containers, _ := nested(pod, "status", "containerStatuses").([]any)
		if len(containers) != 1 || nested(containers[0], "state", "running") == nil {
			return nil, fmt.Errorf("DR runtime container is not running after init")
		}
		forward, err := startForward(ctx, podName, out)
		if err != nil {
			return nil, err
		}
		membership, statusErr := readMembership(ctx, forward, targetAdmin)
		forward.close()
		if statusErr != nil || membership.ClusterID != targetClusterID || string(membership.NodeID) != nodeID || !membership.Voting || membership.Pending || membership.WALIdentity == "" {
			return nil, fmt.Errorf("DR assignment did not bind to its actual runtime voter/WAL: %v", statusErr)
		}
		memberIDs[nodeID] = true
	}
	for _, raw := range assignments {
		assignment := raw.(map[string]any)
		podName := text(nested(assignment, "podName"))
		forward, err := startForward(ctx, podName, out)
		if err != nil {
			return nil, err
		}
		membership, statusErr := readMembership(ctx, forward, targetAdmin)
		forward.close()
		if statusErr != nil || len(membership.Voters) != 3 {
			return nil, fmt.Errorf("target voter quorum is not visible from every assigned runtime: %v", statusErr)
		}
		for _, voter := range membership.Voters {
			if !memberIDs[string(voter)] {
				return nil, fmt.Errorf("target membership contains an unassigned voter")
			}
		}
	}
	proofAssignments := make([]obj, 0, len(assignments))
	for _, raw := range assignments {
		assignment := raw.(map[string]any)
		proofAssignments = append(proofAssignments, obj{"pod": assignment["podName"], "podUID": assignment["podUID"], "nodeID": assignment["nodeID"], "credentialUID": assignment["credentialUID"], "initExitCode": float64(0)})
	}
	preRuntimeBytes, err := os.ReadFile(filepath.Join(out, "dr-pre-runtime-assignment.json"))
	if err != nil {
		return nil, fmt.Errorf("pre-runtime assignment observation is missing: %w", err)
	}
	var preRuntime drPreRuntimeObservation
	if err := json.Unmarshal(preRuntimeBytes, &preRuntime); err != nil || preRuntime.TargetClusterID != targetClusterID || preRuntime.OperationID != request.OperationID || preRuntime.RequestHash != requestHash || preRuntime.Receipt != receipt || len(preRuntime.Assignments) != 3 {
		return nil, fmt.Errorf("pre-runtime assignment observation differs from the approved recovery")
	}
	for _, observed := range preRuntime.Assignments {
		if currentAssignments[observed.PodUID] != observed {
			return nil, fmt.Errorf("observed pre-runtime assignment does not match its completed runtime, ledger, or credential")
		}
	}
	return obj{"version": 1, "targetClusterID": targetClusterID, "anchorID": recoveryAnchorID, "recoveryName": nested(child, "metadata", "name"), "recoveryUID": nested(child, "metadata", "uid"), "operationID": request.OperationID, "requestHash": requestHash, "receipt": receipt, "assignments": proofAssignments, "targetVoters": 3, "initSucceeded": true, "preRuntimeAssignmentObserved": true, "runtimeWALBound": true}, nil
}

func fixtureHashJSON(value any) string {
	return fmt.Sprintf("%x", sha256.Sum256(encoded(value)))
}

// Archive identity is a fixed storage namespace, not consensus membership
// configuration. Membership changes advance ConfigID without rotating the
// source archive identity.
func sourceArchiveIdentity(_ uint) uint { return sourceArchiveConfigID }

func uniqueUIDs(captured string, groups ...[]string) []string {
	seen := make(map[string]struct{}, 1)
	result := make([]string, 0, 1)
	add := func(uid string) {
		if uid == "" {
			return
		}
		if _, ok := seen[uid]; ok {
			return
		}
		seen[uid] = struct{}{}
		result = append(result, uid)
	}
	add(captured)
	for _, group := range groups {
		for _, uid := range group {
			add(uid)
		}
	}
	return result
}

func verifyScratch(sts obj) error {
	spec := nested(sts, "spec", "template", "spec")
	containers, _ := nested(spec, "containers").([]any)
	if len(containers) != 1 || nested(containers[0], "securityContext", "readOnlyRootFilesystem") != true || nested(containers[0], "securityContext", "allowPrivilegeEscalation") != false || !disabledPodOption(spec, "hostPID") || !disabledPodOption(spec, "shareProcessNamespace") {
		return fmt.Errorf("applied supervised workload security differs")
	}
	mounts, _ := nested(containers[0], "volumeMounts").([]any)
	volumes, _ := nested(spec, "volumes").([]any)
	for _, mount := range mounts {
		if nested(mount, "mountPath") != "/tmp" || nested(mount, "readOnly") == true {
			continue
		}
		for _, volume := range volumes {
			if nested(volume, "name") == nested(mount, "name") && nested(volume, "emptyDir") != nil {
				return nil
			}
		}
	}
	return fmt.Errorf("applied checkpoint restore lacks writable /tmp emptyDir")
}

func disabledPodOption(spec any, key string) bool {
	fields, ok := spec.(map[string]any)
	if !ok {
		return false
	}
	value, present := fields[key]
	// Kubernetes omits default-false Pod fields when serializing API objects.
	return !present || value == false
}

func readMembership(ctx context.Context, f *forward, adminToken string) (network.MembershipStatus, error) {
	data, err := call(ctx, f, "/membership/status", nil, adminToken)
	var status network.MembershipStatus
	if err == nil {
		err = json.Unmarshal(encoded(data), &status)
	}
	return status, err
}

func partitionPeer(ctx context.Context, target operator.FenceTarget) (obj, error) {
	pod, err := get(ctx, "pod", target.Pod)
	if err != nil {
		return nil, err
	}
	if text(nested(pod, "metadata", "uid")) != target.PodUID || target.PodUID == "" || target.NodeID == "" || target.WALIdentity == "" {
		return nil, fmt.Errorf("partition target incarnation changed")
	}
	statuses, _ := nested(pod, "status", "containerStatuses").([]any)
	if len(statuses) != 1 {
		return nil, fmt.Errorf("partition requires dedicated single container")
	}
	cid := strings.TrimPrefix(text(nested(statuses[0], "containerID")), "containerd://")
	data, err := command(ctx, "docker", "exec", kindNode, "crictl", "inspect", "-o", "json", cid)
	if err != nil {
		return nil, err
	}
	var runtime obj
	if err := json.Unmarshal(data, &runtime); err != nil {
		return nil, err
	}
	pid := fmt.Sprint(nested(runtime, "info", "pid"))
	if pid == "<nil>" || pid == "0" || nested(runtime, "status", "labels", "io.kubernetes.pod.uid") != target.PodUID || nested(runtime, "status", "id") != cid || nested(runtime, "status", "state") != "CONTAINER_RUNNING" {
		return nil, fmt.Errorf("partition runtime does not bind live target")
	}
	if _, err := command(ctx, "docker", "exec", kindNode, "nsenter", "-t", pid, "-n", "iptables", "-I", "INPUT", "-p", "udp", "--dport", "9090", "-j", "DROP"); err != nil {
		return nil, err
	}
	return obj{"target": target, "containerID": cid, "imageID": nested(statuses[0], "imageID"), "pid": pid}, nil
}

type observationFault struct {
	OperatorPod string               `json:"operatorPod"`
	OperatorUID string               `json:"operatorUID"`
	OperatorIP  string               `json:"operatorIP"`
	ContainerID string               `json:"containerID"`
	PID         string               `json:"pid"`
	Target      operator.FenceTarget `json:"target"`
	TargetIP    string               `json:"targetIP"`
	Rule        []string             `json:"rule"`
	Removed     bool                 `json:"removed"`
}

func observationRule(source, destination, uid string) ([]string, error) {
	src, dst := net.ParseIP(source), net.ParseIP(destination)
	if src == nil || dst == nil || src.To4() == nil || dst.To4() == nil || !src.IsGlobalUnicast() || !dst.IsGlobalUnicast() || src.Equal(dst) || uid == "" || strings.ContainsAny(uid, " \t\n") {
		return nil, fmt.Errorf("observation fault requires distinct exact IPv4 Pod addresses and target UID")
	}
	return []string{"OUTPUT", "-s", src.String(), "-d", dst.String(), "-p", "tcp", "--dport", "8080", "-m", "comment", "--comment", "rhiza-fencing-e2e-" + uid, "-j", "DROP"}, nil
}

func blockOperatorObservation(ctx context.Context, target operator.FenceTarget) (*observationFault, error) {
	targetPod, err := get(ctx, "pod", target.Pod)
	if err != nil {
		return nil, err
	}
	if nested(targetPod, "metadata", "uid") != target.PodUID {
		return nil, fmt.Errorf("observation target Pod incarnation changed")
	}
	list, err := get(ctx, "pods", "")
	if err != nil {
		return nil, err
	}
	items, _ := list["items"].([]any)
	var operatorPod any
	for _, pod := range items {
		if nested(pod, "metadata", "labels", "app.kubernetes.io/name") == "rhiza-operator" && nested(pod, "metadata", "deletionTimestamp") == nil && nested(pod, "status", "phase") == "Running" {
			if operatorPod != nil {
				return nil, fmt.Errorf("observation fault requires one exact operator Pod")
			}
			operatorPod = pod
		}
	}
	statuses, _ := nested(operatorPod, "status", "containerStatuses").([]any)
	if len(statuses) != 1 {
		return nil, fmt.Errorf("observation fault requires dedicated operator container")
	}
	fault := &observationFault{OperatorPod: text(nested(operatorPod, "metadata", "name")), OperatorUID: text(nested(operatorPod, "metadata", "uid")), OperatorIP: text(nested(operatorPod, "status", "podIP")), ContainerID: strings.TrimPrefix(text(nested(statuses[0], "containerID")), "containerd://"), Target: target, TargetIP: text(nested(targetPod, "status", "podIP"))}
	fault.Rule, err = observationRule(fault.OperatorIP, fault.TargetIP, target.PodUID)
	if err != nil {
		return nil, err
	}
	fault.PID, err = fault.runtimePID(ctx)
	if err != nil {
		return nil, err
	}
	_, err = command(ctx, "docker", fault.args("-I")...)
	if err != nil {
		return fault, err // A lost command result may still have installed the rule.
	}
	if _, err := command(ctx, "docker", fault.args("-C")...); err != nil {
		return fault, fmt.Errorf("owned observation rule installation unconfirmed: %w", err)
	}
	return fault, nil
}

func (f *observationFault) args(operation string) []string {
	return append([]string{"exec", kindNode, "nsenter", "-t", f.PID, "-n", "iptables", operation}, f.Rule...)
}

func (f *observationFault) runtimePID(ctx context.Context) (string, error) {
	data, err := command(ctx, "docker", "exec", kindNode, "crictl", "inspect", "-o", "json", f.ContainerID)
	if err != nil {
		return "", err
	}
	var runtime obj
	if err := json.Unmarshal(data, &runtime); err != nil {
		return "", err
	}
	pid := fmt.Sprint(nested(runtime, "info", "pid"))
	if pid == "<nil>" || pid == "0" || nested(runtime, "status", "id") != f.ContainerID || nested(runtime, "status", "labels", "io.kubernetes.pod.uid") != f.OperatorUID || nested(runtime, "status", "state") != "CONTAINER_RUNNING" {
		return "", fmt.Errorf("operator observation-fault runtime identity changed")
	}
	return pid, nil
}

func (f *observationFault) remove(ctx context.Context) error {
	if f.Removed {
		return nil
	}
	pid, err := f.runtimePID(ctx)
	if err != nil || pid != f.PID {
		return fmt.Errorf("cannot safely remove owned observation rule: %v", err)
	}
	if _, err := command(ctx, "docker", f.args("-D")...); err != nil {
		return err
	}
	data, err := exec.CommandContext(ctx, "docker", f.args("-C")...).CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 {
		return fmt.Errorf("owned observation rule absence unconfirmed: %v: %s", err, data)
	}
	f.Removed = true
	return nil
}

func qualifyOnline(ctx context.Context, survivor *forward, adminToken, stsUID, bindingUID, out string, event func(string, obj), faults *[]*observationFault) ([]operator.FenceTarget, error) {
	failedPod, err := get(ctx, "pod", "rhiza-2")
	if err != nil {
		return nil, err
	}
	failedForward, err := startForward(ctx, "rhiza-2", out)
	if err != nil {
		return nil, err
	}
	failedStatus, err := readMembership(ctx, failedForward, adminToken)
	failedForward.close()
	if err != nil || failedStatus.NodeID != "rhiza-2" || failedStatus.ClusterID != "chaos-source" || !failedStatus.Voting || failedStatus.Pending || failedStatus.WALIdentity == "" {
		return nil, fmt.Errorf("failed voter identity unavailable: %v", err)
	}
	failed := capturedTarget(operator.FenceTarget{NodeID: string(failedStatus.NodeID), Pod: "rhiza-2", PodUID: text(nested(failedPod, "metadata", "uid")), WALIdentity: failedStatus.WALIdentity}, failedPod)
	partition, err := partitionPeer(ctx, failed)
	if err != nil {
		return nil, err
	}
	event("online_peer_partition", partition)
	fault, err := blockOperatorObservation(ctx, failed)
	if fault != nil {
		*faults = append(*faults, fault)
	}
	if err != nil {
		return nil, err
	}
	event("online_operator_observation_fault", obj{"fault": fault})
	if _, err := call(ctx, survivor, "/sql/execute", obj{"request_id": "fencing-surviving-quorum", "sql": "INSERT INTO fencing_items VALUES (2, 'quorum-survived')"}, adminToken); err != nil {
		return nil, err
	}
	if err := checkRows(ctx, survivor, adminToken, `[[1,"acknowledged"],[2,"quorum-survived"]]`); err != nil {
		return nil, err
	}
	event("surviving_quorum_ack_readback", obj{"rows": 2})
	var voterRequest operator.FenceRequest
	var voterProof operator.FenceProof
	if err := wait(ctx, "exact online voter drain/exit proof", func() (bool, error) {
		list, err := get(ctx, "rhizafences", "")
		if err != nil {
			return false, err
		}
		items, _ := list["items"].([]any)
		for _, item := range items {
			if nested(item, "spec", "scope") != "Voter" || nested(item, "status", "proof") == nil {
				continue
			}
			if err := verifyScopedProof(ctx, item, stsUID, bindingUID, []string{failed.PodUID}, operator.ScopeVoter, []operator.FenceTarget{failed}, out); err != nil {
				return false, err
			}
			if err := json.Unmarshal(encoded(nested(item, "spec")), &voterRequest); err != nil {
				return false, err
			}
			return true, json.Unmarshal(encoded(nested(item, "status", "proof")), &voterProof)
		}
		return false, nil
	}); err != nil {
		return nil, err
	}
	event("generic_voter_proof", obj{"request": voterRequest, "proof": voterProof})
	var child operator.Resource
	if err := wait(ctx, "online membership promotion and healthy parent", func() (bool, error) {
		resources, err := get(ctx, "rhizarecoveries", "")
		if err != nil {
			return false, err
		}
		items, _ := resources["items"].([]any)
		var resource any
		for _, item := range items {
			if nested(item, "metadata", "name") == voterRequest.OperationID {
				resource = item
			}
		}
		if resource == nil {
			return false, nil
		}
		if err := json.Unmarshal(encoded(resource), &child); err != nil {
			return false, err
		}
		if child.Status.Phase != "Complete" || child.Status.Membership == nil || child.Status.Membership.Phase != "Complete" {
			return false, nil
		}
		root, err := get(ctx, "rhizacluster", "rhiza")
		if err != nil {
			return false, err
		}
		return nested(root, "status", "phase") == "Healthy" && nested(root, "status", "activeClusterID") == "chaos-source", nil
	}); err != nil {
		return nil, err
	}
	m := child.Spec.Membership
	if m == nil || m.Remove != "rhiza-2" || m.RemovedPod != failed.Pod || m.Fence.WorkloadUID != failed.PodUID || m.Fence.WALIdentity != failed.WALIdentity || m.Fence.Evidence != voterProof.ProofID || !m.Fence.Confirmed || m.ReplacementPod == "" || child.Status.Membership.Add == nil || child.Status.Membership.RemoveRequest == nil || text(nested(child.Metadata, "uid")) == "" || text(nested(child.Metadata, "annotations", "rhiza.mrchypark.dev/automatic-owner")) != bindingUID || text(nested(child.Metadata, "annotations", "rhiza.mrchypark.dev/automatic-logical")) != "fencing-e2e" {
		return nil, fmt.Errorf("completed membership differs from exact voter fence")
	}
	remove := child.Status.Membership.RemoveRequest
	operationPrefix := text(child.Metadata["uid"]) + ":" + m.OperationID + ":"
	if m.OperationID != voterRequest.OperationID || remove.OperationID != operationPrefix+"remove" || remove.ClusterID != "chaos-source" || string(remove.Remove) != failed.NodeID || remove.Fence == nil || remove.Fence.Evidence != voterProof.ProofID || remove.Fence.WALIdentity != failed.WALIdentity || remove.Fence.WorkloadUID != failed.PodUID || child.Status.Membership.Add.OperationID != operationPrefix+"add" {
		return nil, fmt.Errorf("membership remove/add journals differ from fenced operation")
	}
	refs := append(append([]operator.VoterPod(nil), m.VoterPods...), operator.VoterPod{NodeID: child.Status.Membership.Add.MemberID, Pod: m.ReplacementPod})
	if err := validateReplacementRefs(refs, failed.NodeID, m.ReplacementPod); err != nil {
		return nil, err
	}
	var targets []operator.FenceTarget
	var observations []obj
	var configID uint
	for _, ref := range refs {
		pod, err := get(ctx, "pod", ref.Pod)
		if err != nil {
			return nil, err
		}
		f, err := startForward(ctx, ref.Pod, out)
		if err != nil {
			return nil, err
		}
		status, statusErr := readMembership(ctx, f, adminToken)
		f.close()
		if statusErr != nil || !membershipMatches(status, refs, string(ref.NodeID)) || configID != 0 && status.ConfigID != configID {
			return nil, fmt.Errorf("promoted voter %s membership differs: %v", ref.NodeID, statusErr)
		}
		configID = status.ConfigID
		targets = append(targets, capturedTarget(operator.FenceTarget{NodeID: string(ref.NodeID), Pod: ref.Pod, PodUID: text(nested(pod, "metadata", "uid")), WALIdentity: status.WALIdentity}, pod))
		observations = append(observations, obj{"podUID": nested(pod, "metadata", "uid"), "pod": ref.Pod, "membership": status})
		if ref.Pod == m.ReplacementPod {
			if err := verifyLearnerBinding(ctx, pod, child, status, stsUID, bindingUID, voterRequest.OperationID, out); err != nil {
				return nil, err
			}
		}
	}
	if err := verifyRegisteredTargets(ctx, targets, stsUID, out); err != nil {
		return nil, err
	}
	if err := verifyImages(ctx, out, "learner-promoted", targets...); err != nil {
		return nil, err
	}
	learnerForward, err := startForward(ctx, m.ReplacementPod, out)
	if err != nil {
		return nil, err
	}
	defer learnerForward.close()
	if _, err := call(ctx, learnerForward, "/sql/execute", obj{"request_id": "fencing-learner-promoted", "sql": "INSERT INTO fencing_items VALUES (3, 'learner-promoted')"}, adminToken); err != nil {
		return nil, err
	}
	if err := checkRows(ctx, learnerForward, adminToken, `[[1,"acknowledged"],[2,"quorum-survived"],[3,"learner-promoted"]]`); err != nil {
		return nil, err
	}
	if err := checkGraphRows(ctx, learnerForward, adminToken, `[[1,"acknowledged"]]`); err != nil {
		return nil, fmt.Errorf("Graph state did not survive learner replacement: %w", err)
	}
	if _, err := call(ctx, learnerForward, "/graph/execute", obj{"request_id": "fencing-graph-learner-promoted", "cypher": "CREATE (:FencingItem {id: 2, value: 'learner-promoted'})"}, adminToken); err != nil {
		return nil, err
	}
	if err := checkGraphRows(ctx, learnerForward, adminToken, `[[1,"acknowledged"],[2,"learner-promoted"]]`); err != nil {
		return nil, err
	}
	event("online_replacement_linearizable_readback", obj{"targets": targets, "rows": 3, "configID": configID})
	if err := fault.remove(ctx); err != nil {
		return nil, err
	}
	event("online_operator_observation_fault_removed", obj{"fault": fault})
	return targets, os.WriteFile(filepath.Join(out, "online-completed-membership.json"), encoded(obj{"recovery": child, "targets": targets, "observations": observations}), 0600)
}

func checkRows(ctx context.Context, f *forward, token, expected string) error {
	response, err := call(ctx, f, "/sql/query", obj{"sql": "SELECT id,value FROM fencing_items ORDER BY id", "consistency": "linearizable"}, token)
	if err != nil {
		return err
	}
	if string(encoded(response["rows"])) != expected {
		return fmt.Errorf("linearizable acknowledged values differ")
	}
	return nil
}

func checkGraphRows(ctx context.Context, f *forward, token, expected string) error {
	response, err := call(ctx, f, "/graph/query", obj{"cypher": "MATCH (n:FencingItem) RETURN n.id, n.value ORDER BY n.id", "consistency": "linearizable"}, token)
	if err != nil {
		return err
	}
	if string(encoded(response["rows"])) != expected {
		return fmt.Errorf("linearizable acknowledged Graph values differ")
	}
	return nil
}

type durableArchiveEvidence struct {
	Tip          quepaxa.Slot
	Start        quepaxa.Slot
	Through      quepaxa.Slot
	PrefixSHA256 string
}

// verifyDurableArchive opens a separate SDK client against VersityGW, checks
// acknowledged application commands, and rejects probe values if they ever
// became a committed decision. A missing staged value is not proof of no WAL
// effect; this check is limited to the independently inspected archive range.
func verifyDurableArchive(ctx context.Context, out string, configID uint, requestIDs, forbiddenIDs, forbiddenMarkers []string) (durableArchiveEvidence, error) {
	forward, err := startServiceForward(ctx, "rhiza-minio", 9000, out)
	if err != nil {
		return durableArchiveEvidence{}, err
	}
	defer forward.close()
	bucket, err := objmetrics.NewBucket(objmetrics.Config{
		Provider: objmetrics.ProviderS3, Endpoint: fmt.Sprintf("http://127.0.0.1:%d", forward.port),
		Bucket: "rhiza", Region: "us-east-1", Insecure: true,
		AccessKey: "fencing-local", SecretKey: "fencing-local-secret",
	})
	if err != nil {
		return durableArchiveEvidence{}, fmt.Errorf("open independent Versity S3 SDK client: %w", err)
	}
	defer bucket.Close()
	archive := recovery.NewManager(bucket, "rhiza/chaos-source", configID)
	defer archive.Close()
	seen := make(map[string]bool, len(requestIDs))
	var durableTip quepaxa.Slot
	err = wait(ctx, "independent object-store archive contains acknowledged SQL and Graph commands", func() (bool, error) {
		if err := archive.Load(ctx); err != nil {
			return false, err
		}
		durableTip = archive.Tip()
		if durableTip == 0 || durableTip > 100000 {
			return false, fmt.Errorf("independent archive tip %d is outside the fixture bound", durableTip)
		}
		decisions, _, err := archive.DecisionsFrom(ctx, 1, int(durableTip))
		if err != nil {
			return false, err
		}
		clear(seen)
		for _, decision := range decisions {
			if commands, ok, err := types.DecodeSQLBatch(decision.Value); err != nil {
				return false, fmt.Errorf("decode archived SQL decision at slot %d: %w", decision.Slot, err)
			} else if ok {
				for _, command := range commands {
					seen[command.RequestID] = true
				}
			}
			if commands, ok, err := types.DecodeGraphBatch(decision.Value); err != nil {
				return false, fmt.Errorf("decode archived Graph decision at slot %d: %w", decision.Slot, err)
			} else if ok {
				for _, command := range commands {
					seen[command.RequestID] = true
				}
			}
		}
		for _, requestID := range requestIDs {
			if !seen[requestID] {
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		return durableArchiveEvidence{}, err
	}
	decisions, observedTip, err := archive.DecisionsFrom(ctx, 1, int(durableTip))
	if err != nil {
		return durableArchiveEvidence{}, fmt.Errorf("read independent archive range: %w", err)
	}
	if observedTip != durableTip || len(decisions) != int(durableTip) {
		return durableArchiveEvidence{}, fmt.Errorf("independent archive range is incomplete")
	}
	forbidden := make(map[string]bool, len(forbiddenIDs))
	for _, id := range forbiddenIDs {
		forbidden[id] = true
	}
	var prefix [32]byte
	for index, decision := range decisions {
		if decision.Slot != quepaxa.Slot(index+1) {
			return durableArchiveEvidence{}, fmt.Errorf("independent archive range has a slot gap")
		}
		prefix = quepaxa.AdvancePrefixHash(prefix, decision.Slot, decision.Hash)
		sqlCommands, isSQL, decodeErr := types.DecodeSQLBatch(decision.Value)
		if decodeErr != nil {
			return durableArchiveEvidence{}, fmt.Errorf("decode independent SQL decision: %w", decodeErr)
		}
		if isSQL {
			for _, command := range sqlCommands {
				if forbidden[command.RequestID] {
					return durableArchiveEvidence{}, fmt.Errorf("peer probe request unexpectedly appears in independent archive")
				}
			}
		}
		graphCommands, isGraph, decodeErr := types.DecodeGraphBatch(decision.Value)
		if decodeErr != nil {
			return durableArchiveEvidence{}, fmt.Errorf("decode independent Graph decision: %w", decodeErr)
		}
		if isGraph {
			for _, command := range graphCommands {
				if forbidden[command.RequestID] {
					return durableArchiveEvidence{}, fmt.Errorf("peer probe request unexpectedly appears in independent archive")
				}
			}
		}
		for _, marker := range forbiddenMarkers {
			if marker != "" && bytes.Contains(decision.Value, []byte(marker)) {
				return durableArchiveEvidence{}, fmt.Errorf("peer probe marker unexpectedly appears in independent archive")
			}
		}
	}
	return durableArchiveEvidence{Tip: durableTip, Start: 1, Through: durableTip, PrefixSHA256: hex.EncodeToString(prefix[:])}, nil
}

func validateReplacementRefs(refs []operator.VoterPod, removed, learner string) error {
	seen, pods := map[string]bool{}, map[string]bool{}
	for _, ref := range refs {
		id := string(ref.NodeID)
		if id == "" || ref.Pod == "" || id == removed || seen[id] || pods[ref.Pod] {
			return fmt.Errorf("replacement membership mapping is invalid")
		}
		seen[id], pods[ref.Pod] = true, true
	}
	if len(refs) != 3 || !seen[learner] || !pods[learner] || !seen["rhiza-0"] || !seen["rhiza-1"] {
		return fmt.Errorf("replacement must retain both survivors and promote the authenticated learner")
	}
	return nil
}

func sameTargets(actual, expected []operator.FenceTarget) bool {
	if len(actual) != len(expected) {
		return false
	}
	seen := map[string]operator.FenceTarget{}
	for _, target := range actual {
		if _, duplicate := seen[target.NodeID]; duplicate || target.NodeID == "" || target.Pod == "" || target.PodUID == "" || target.WALIdentity == "" {
			return false
		}
		seen[target.NodeID] = target
	}
	for _, target := range expected {
		if seen[target.NodeID] != target {
			return false
		}
		delete(seen, target.NodeID)
	}
	return true
}

// capturedTarget mirrors the operator's immutable identity capture, so the
// expected fence target can be compared with the published one exactly.
func capturedTarget(target operator.FenceTarget, pod obj) operator.FenceTarget {
	target.NodeName = text(nested(pod, "spec", "nodeName"))
	if statuses, ok := nested(pod, "status", "containerStatuses").([]any); ok && len(statuses) == 1 {
		status, _ := statuses[0].(map[string]any)
		target.ContainerID = text(status["containerID"])
		target.ImageID = text(status["imageID"])
	}
	return target
}

func membershipMatches(status network.MembershipStatus, refs []operator.VoterPod, nodeID string) bool {
	if string(status.NodeID) != nodeID || status.ClusterID != "chaos-source" || status.WALIdentity == "" || !status.Voting || status.Pending || status.ConfigID == 0 || len(status.Voters) != len(refs) {
		return false
	}
	seen := map[quepaxa.NodeID]bool{}
	for _, id := range status.Voters {
		if seen[id] {
			return false
		}
		seen[id] = true
	}
	for _, ref := range refs {
		if !seen[ref.NodeID] {
			return false
		}
	}
	return true
}

func verifyLearnerBinding(ctx context.Context, pod obj, child operator.Resource, status network.MembershipStatus, stsUID, bindingUID, operation, out string) error {
	m, add := child.Spec.Membership, child.Status.Membership.Add
	name := text(nested(pod, "metadata", "name"))
	if name != m.ReplacementPod || string(add.MemberID) != name || status.WALIdentity != add.WALIdentity || text(nested(pod, "metadata", "uid")) == "" || nested(pod, "metadata", "labels", "rhiza.mrchypark.dev/automatic-learner") != name || nested(pod, "metadata", "annotations", "rhiza.mrchypark.dev/fencing-root-uid") != bindingUID || nested(pod, "metadata", "annotations", "rhiza.mrchypark.dev/recovery-owner") != operation {
		return fmt.Errorf("promoted learner Pod/WAL/root identity differs")
	}
	owners, _ := nested(pod, "metadata", "ownerReferences").([]any)
	owned := false
	for _, owner := range owners {
		owned = owned || nested(owner, "kind") == "StatefulSet" && nested(owner, "uid") == stsUID
	}
	if !owned {
		return fmt.Errorf("promoted learner lacks exact StatefulSet owner")
	}
	secret, err := get(ctx, "secret", m.ReplacementSecret)
	if err != nil {
		return err
	}
	if secret["immutable"] != true || nested(secret, "metadata", "uid") != add.SecretUID || nested(secret, "metadata", "resourceVersion") != add.SecretResourceVer || nested(pod, "metadata", "annotations", "rhiza.mrchypark.dev/fencing-credential-uid") != add.SecretUID || nested(secret, "metadata", "annotations", "rhiza.mrchypark.dev/recovery-owner") != operation {
		return fmt.Errorf("promoted learner immutable credential binding differs")
	}
	raw, err := base64.StdEncoding.DecodeString(text(nested(secret, "data", "member")))
	if err != nil {
		return err
	}
	var member quepaxa.Member
	if err := json.Unmarshal(raw, &member); err != nil {
		return err
	}
	member.WALIdentity = status.WALIdentity
	request := network.MembershipChange{OperationID: add.OperationID, ClusterID: "chaos-source", ExpectedConfigID: add.ExpectedConfigID, ExpectedAbortSlot: add.ExpectedAbortSlot, Add: &member}
	if member.ID != status.NodeID || member.PublicKey == (quepaxa.PublicKey{}) || fmt.Sprintf("%x", sha256.Sum256(encoded(request))) != add.RequestHash {
		return fmt.Errorf("promoted learner member differs from committed addition")
	}
	return os.WriteFile(filepath.Join(out, "learner-authenticated-binding.json"), encoded(obj{"podUID": nested(pod, "metadata", "uid"), "nodeID": status.NodeID, "walIdentity": status.WALIdentity, "rootUID": bindingUID, "statefulSetUID": stsUID, "credentialUID": add.SecretUID, "member": member, "addition": add}), 0600)
}

type fenceLedgerState struct {
	StatefulSetUID string         `json:"statefulSetUID"`
	Ledger         fencing.Ledger `json:"ledger"`
}

func verifyRegisteredTargets(ctx context.Context, targets []operator.FenceTarget, stsUID, out string) error {
	maps, err := get(ctx, "configmaps", "")
	if err != nil {
		return err
	}
	items, _ := maps["items"].([]any)
	for _, item := range items {
		data := text(nested(item, "data", "ledger.json"))
		if data == "" {
			continue
		}
		var state fenceLedgerState
		if err := json.Unmarshal([]byte(data), &state); err != nil {
			return err
		}
		if state.StatefulSetUID != stsUID || state.Ledger.SourceClusterID != "chaos-source" {
			continue
		}
		for _, target := range targets {
			found := false
			for _, run := range state.Ledger.Runs {
				if run.Identity.NodeID == target.NodeID && run.Identity.PodUID == target.PodUID && run.Identity.WALIdentity == target.WALIdentity && run.Stop == nil && !run.Exited {
					found = true
				}
			}
			if !found {
				return fmt.Errorf("promoted current voter lacks active WAL-bound startup registration: %s", target.NodeID)
			}
		}
		return os.WriteFile(filepath.Join(out, "online-active-ledger.json"), encoded(state), 0600)
	}
	return fmt.Errorf("active source ledger unavailable")
}

func digest(value string) string {
	value = strings.TrimPrefix(strings.TrimPrefix(value, "docker-pullable://"), "containerd://")
	if _, d, ok := strings.Cut(value, "@"); ok {
		return d
	}
	return value
}

func immutableDigest(value string) (string, error) {
	d := digest(value)
	if !strings.HasPrefix(d, "sha256:") || len(d) != len("sha256:")+64 {
		return "", fmt.Errorf("image reference has no immutable SHA256 digest: %q", value)
	}
	if _, err := hex.DecodeString(strings.TrimPrefix(d, "sha256:")); err != nil {
		return "", err
	}
	return d, nil
}

// Kind can discard the archive's import repository name while retaining its
// immutable content. Follow only hash-verified descriptors, never a mutable tag.
func imageConfig(ctx context.Context, reference string) (obj, []obj, error) {
	return resolveImageConfig(ctx, reference, command)
}

func resolveImageConfig(ctx context.Context, reference string, run func(context.Context, string, ...string) ([]byte, error)) (obj, []obj, error) {
	d, err := immutableDigest(reference)
	if err != nil {
		return nil, nil, err
	}
	var chain []obj
	for range 4 {
		data, lookupErr := run(ctx, "docker", "exec", kindNode, "crictl", "inspecti", "-o", "json", d)
		if lookupErr == nil {
			var cri obj
			if err := json.Unmarshal(data, &cri); err != nil {
				return nil, chain, err
			}
			if _, err := immutableDigest(text(nested(cri, "status", "id"))); err != nil {
				return nil, chain, err
			}
			chain = append(chain, obj{"digest": d, "cri": publicCRIImage(cri)})
			return cri, chain, nil
		}
		content, err := run(ctx, "docker", "exec", kindNode, "ctr", "-n", "k8s.io", "content", "get", d)
		if err != nil {
			return nil, chain, fmt.Errorf("resolve immutable image %s: CRI: %v; content: %w", d, lookupErr, err)
		}
		next, err := imageDescriptor(d, content)
		chain = append(chain, obj{"digest": d, "criLookupFailed": true, "contentSHA256": fmt.Sprintf("%x", sha256.Sum256(content)), "descriptor": publicDescriptor(content)})
		if err != nil {
			return nil, chain, err
		}
		d = next
	}
	return nil, chain, fmt.Errorf("image descriptor chain exceeds qualification bound")
}

func imageDescriptor(d string, content []byte) (string, error) {
	if "sha256:"+fmt.Sprintf("%x", sha256.Sum256(content)) != d {
		return "", fmt.Errorf("image content digest differs from %s", d)
	}
	var descriptor struct {
		Config struct {
			Digest string `json:"digest"`
		} `json:"config"`
		Manifests []struct {
			Digest      string            `json:"digest"`
			Size        int64             `json:"size"`
			MediaType   string            `json:"mediaType"`
			Platform    obj               `json:"platform"`
			Annotations map[string]string `json:"annotations"`
		} `json:"manifests"`
	}
	if err := json.Unmarshal(content, &descriptor); err != nil {
		return "", err
	}
	if descriptor.Config.Digest != "" && len(descriptor.Manifests) == 0 {
		return immutableDigest(descriptor.Config.Digest)
	}
	if descriptor.Config.Digest == "" && len(descriptor.Manifests) > 0 {
		var attestations []string
		runnable := descriptor.Manifests[:0]
		for _, manifest := range descriptor.Manifests {
			if text(manifest.Platform["os"]) == "unknown" || text(manifest.Platform["architecture"]) == "unknown" || manifest.Annotations["vnd.docker.reference.type"] == "attestation-manifest" {
				if len(manifest.Platform) != 2 || text(manifest.Platform["os"]) != "unknown" || text(manifest.Platform["architecture"]) != "unknown" || manifest.Size <= 0 || manifest.MediaType != "application/vnd.oci.image.manifest.v1+json" || manifest.Annotations["vnd.docker.reference.type"] != "attestation-manifest" {
					return "", fmt.Errorf("invalid attestation in immutable image index %s", d)
				}
				if _, err := immutableDigest(manifest.Digest); err != nil {
					return "", err
				}
				attestations = append(attestations, manifest.Annotations["vnd.docker.reference.digest"])
				continue
			}
			runnable = append(runnable, manifest)
		}
		if len(runnable) == 0 {
			return "", fmt.Errorf("no runnable manifest in immutable image index %s", d)
		}
		descriptor.Manifests = runnable
		first := descriptor.Manifests[0]
		resolved, err := immutableDigest(first.Digest)
		if err != nil {
			return "", err
		}
		for _, reference := range attestations {
			if reference != resolved {
				return "", fmt.Errorf("attestation references another immutable image in %s", d)
			}
		}
		knownMediaType := first.MediaType == "application/vnd.oci.image.manifest.v1+json" || first.MediaType == "application/vnd.oci.image.index.v1+json" || first.MediaType == "application/vnd.docker.distribution.manifest.v2+json" || first.MediaType == "application/vnd.docker.distribution.manifest.list.v2+json"
		for _, alias := range descriptor.Manifests[1:] {
			// Docker archives may index two tag aliases of the same manifest.
			// Ignore only annotations, not conflicting content or platform metadata.
			if first.Size <= 0 || !knownMediaType || alias.Digest != resolved || alias.Size != first.Size || alias.MediaType != first.MediaType || !bytes.Equal(encoded(alias.Platform), encoded(first.Platform)) {
				return "", fmt.Errorf("ambiguous immutable image index %s", d)
			}
		}
		return resolved, nil
	}
	return "", fmt.Errorf("ambiguous or unsupported immutable image descriptor %s", d)
}

func incarnationSnapshot(pod obj) string {
	statuses, _ := nested(pod, "status", "containerStatuses").([]any)
	var processes []obj
	for _, status := range statuses {
		processes = append(processes, obj{"containerID": nested(status, "containerID"), "restartCount": nested(status, "restartCount")})
	}
	return string(encoded(obj{"uid": nested(pod, "metadata", "uid"), "processes": processes}))
}

func verifyImages(ctx context.Context, out, phase string, current ...operator.FenceTarget) error {
	pods, err := get(ctx, "pods", "")
	if err != nil {
		return err
	}
	items, _ := pods["items"].([]any)
	counts := map[string]int{}
	wanted := map[string]string{}
	for _, target := range current {
		if target.Pod == "" || target.PodUID == "" || wanted[target.Pod] != "" {
			return fmt.Errorf("invalid current image inventory")
		}
		wanted[target.Pod] = target.PodUID
	}
	var evidence []obj
	for _, pod := range items {
		if nested(pod, "metadata", "deletionTimestamp") != nil || nested(pod, "status", "phase") != "Running" {
			continue
		}
		role := text(nested(pod, "metadata", "labels", "app.kubernetes.io/name"))
		name := text(nested(pod, "metadata", "name"))
		if uid := wanted[name]; uid != "" {
			if nested(pod, "metadata", "uid") != uid {
				return fmt.Errorf("qualified voter image incarnation changed")
			}
			role = "rhiza"
		} else if len(wanted) > 0 && role == "rhiza" {
			continue // Retired source slots are not current voting engines.
		}
		image := ""
		switch role {
		case "rhiza":
			image = "rhiza-fencing-host:local"
		case "rhiza-operator", "rhiza-fencer":
			image = "rhiza-fencing-operator:local"
		default:
			continue
		}
		statuses, _ := nested(pod, "status", "containerStatuses").([]any)
		if len(statuses) != 1 {
			return fmt.Errorf("%s has no single running image", role)
		}
		podImage := text(nested(statuses[0], "imageID"))
		if podImage == "" {
			return fmt.Errorf("%s imageID missing", role)
		}
		local, err := command(ctx, "docker", "image", "inspect", image)
		if err != nil {
			return err
		}
		var built []obj
		if err = json.Unmarshal(local, &built); err != nil || len(built) != 1 {
			return fmt.Errorf("invalid built image inspection")
		}
		containerID := strings.TrimPrefix(text(nested(statuses[0], "containerID")), "containerd://")
		runtimeData, err := command(ctx, "docker", "exec", kindNode, "crictl", "inspect", "-o", "json", containerID)
		if err != nil {
			return err
		}
		var runtime obj
		if err := json.Unmarshal(runtimeData, &runtime); err != nil {
			return err
		}
		if text(nested(runtime, "status", "id")) != containerID || nested(runtime, "status", "labels", "io.kubernetes.pod.uid") != nested(pod, "metadata", "uid") || digest(text(nested(runtime, "status", "imageRef"))) != digest(podImage) {
			return fmt.Errorf("%s runtime image does not bind the observed Pod incarnation", role)
		}
		cri, chain, resolveErr := imageConfig(ctx, podImage)
		configID := text(nested(cri, "status", "id"))
		builtID := text(built[0]["Id"])
		builtCRI, builtChain, builtErr := imageConfig(ctx, builtID)
		builtConfigID := text(nested(builtCRI, "status", "id"))
		repos, _ := nested(cri, "status", "repoDigests").([]any)
		evidence = append(evidence, obj{"pod": nested(pod, "metadata", "name"), "uid": nested(pod, "metadata", "uid"), "role": role, "podImageID": podImage, "container": publicRuntime(runtime), "resolution": chain, "builtImageID": builtID, "builtResolution": builtChain, "builtConfigID": builtConfigID, "criConfigID": configID, "criRepoDigests": repos})
		if err := os.WriteFile(filepath.Join(out, phase+"-image-evidence.json"), encoded(evidence), 0600); err != nil {
			return err
		}
		if resolveErr != nil {
			return resolveErr
		}
		if builtErr != nil {
			return builtErr
		}
		if configID == "" || digest(configID) != digest(builtConfigID) {
			return fmt.Errorf("%s running image configuration differs from built image", role)
		}
		counts[role]++
	}
	if counts["rhiza"] != 3 || counts["rhiza-operator"] != 1 || counts["rhiza-fencer"] != 1 {
		return fmt.Errorf("qualification image inventory incomplete: %v", counts)
	}
	return os.WriteFile(filepath.Join(out, phase+"-image-evidence.json"), encoded(evidence), 0600)
}

func verifyGenerationProof(ctx context.Context, item any, stsUID, bindingUID string, oldUIDs []string, out string, targets ...operator.FenceTarget) error {
	return verifyScopedProof(ctx, item, stsUID, bindingUID, oldUIDs, operator.ScopeGeneration, targets, out)
}

func verifyScopedProof(ctx context.Context, item any, stsUID, bindingUID string, oldUIDs []string, scope operator.Scope, targets []operator.FenceTarget, out string) error {
	var request operator.FenceRequest
	var proof operator.FenceProof
	if err := json.Unmarshal(encoded(nested(item, "spec")), &request); err != nil {
		return err
	}
	if err := json.Unmarshal(encoded(nested(item, "status", "proof")), &proof); err != nil {
		return err
	}
	if request.SourceClusterID != "chaos-source" || request.StatefulSet != "rhiza" || request.StatefulSetUID != stsUID || request.BindingUID != bindingUID || request.Namespace != namespace || request.LogicalID != "fencing-e2e" || request.OperationID == "" || request.Scope != scope {
		return fmt.Errorf("generation fence request identity differs")
	}
	var nodes []string
	if scope == operator.ScopeVoter {
		if len(targets) != 1 || string(encoded(request.Targets)) != string(encoded(targets)) {
			return fmt.Errorf("voter fence target differs from observed exact incarnation")
		}
		nodes = []string{targets[0].NodeID}
	} else if len(targets) > 0 && !sameTargets(request.Targets, targets) {
		return fmt.Errorf("generation fence does not bind all current voters including the learner")
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(encoded(request)))
	if proof.OperationID != request.OperationID || proof.SourceClusterID != request.SourceClusterID || proof.BindingUID != request.BindingUID || proof.RequestHash != hash || proof.ProofID == "" || !proof.ProcessesTerminated || !proof.RecreationBlocked || !proof.StorageQuiesced {
		return fmt.Errorf("generation proof binding differs")
	}
	maps, err := get(ctx, "configmaps", "")
	if err != nil {
		return err
	}
	items, _ := maps["items"].([]any)
	for _, item := range items {
		data := text(nested(item, "data", "ledger.json"))
		if data == "" {
			continue
		}
		var state fenceLedgerState
		if err := json.Unmarshal([]byte(data), &state); err != nil {
			return err
		}
		if state.StatefulSetUID != stsUID || state.Ledger.SourceClusterID != request.SourceClusterID {
			continue
		}
		if !state.Ledger.CompleteNodes(request.OperationID, hash, nodes, oldUIDs) {
			return fmt.Errorf("generation ledger has no clean drain/exit for all source incarnations")
		}
		for _, run := range state.Ledger.Runs {
			for _, uid := range oldUIDs {
				if run.Identity.PodUID == uid && (run.Stop == nil || run.Stop.OperationID != request.OperationID || run.Stop.RequestHash != hash) {
					return fmt.Errorf("source incarnation drained for a different operation")
				}
			}
		}
		if err := verifyLedgerProof(state, request, proof); err != nil {
			return err
		}
		filename := "validated-voter-proof.json"
		if scope == operator.ScopeGeneration {
			filename = "validated-generation-proof.json"
			if err := verifyPriorVoterStops(out, state.Ledger); err != nil {
				return err
			}
		}
		provenance := "exact-current-ledger-hash"
		if scope == operator.ScopeVoter {
			provenance = "historical-issuer-proof-id; current-ledger-exact-terminal-targets; issuance-snapshot-not-reconstructed"
		}
		return os.WriteFile(filepath.Join(out, filename), encoded(obj{"request": request, "proof": proof, "ledger": state, "oldUIDs": oldUIDs, "ledgerVerification": provenance}), 0600)
	}
	return fmt.Errorf("generation proof has no matching durable ledger")
}

func verifyLedgerProof(state fenceLedgerState, request operator.FenceRequest, proof operator.FenceProof) error {
	hash := fmt.Sprintf("%x", sha256.Sum256(encoded(request)))
	if state.StatefulSetUID != request.StatefulSetUID || state.Ledger.SourceClusterID != request.SourceClusterID || proof.OperationID != request.OperationID || proof.SourceClusterID != request.SourceClusterID || proof.BindingUID != request.BindingUID || proof.RequestHash != hash || !proof.ProcessesTerminated || !proof.RecreationBlocked || !proof.StorageQuiesced {
		return fmt.Errorf("ledger proof identity/request binding differs")
	}
	if request.Scope == operator.ScopeGeneration {
		if proof.ProofID != "cooperative-"+fmt.Sprintf("%x", sha256.Sum256(encoded(state))) {
			return fmt.Errorf("generation proof does not bind observed closed ledger")
		}
		return nil
	}
	// This is an issuer-produced historical proof, not a hash of today's ledger:
	// learner registration advances unrelated runs after the one-time issuance.
	if request.Scope != operator.ScopeVoter || !strings.HasPrefix(proof.ProofID, "cooperative-") {
		return fmt.Errorf("unsupported historical voter proof")
	}
	if _, err := immutableDigest("sha256:" + strings.TrimPrefix(proof.ProofID, "cooperative-")); err != nil {
		return err
	}
	if len(request.Targets) != 1 {
		return fmt.Errorf("online voter proof requires one exact target")
	}
	target := request.Targets[0]
	if target.NodeID == "" || target.PodUID == "" || target.WALIdentity == "" || !state.Ledger.CompleteNodes(request.OperationID, hash, []string{target.NodeID}, []string{target.PodUID}) {
		return fmt.Errorf("historical voter proof lacks clean completed target authority")
	}
	found := false
	for _, run := range state.Ledger.Runs {
		if run.Identity.NodeID != target.NodeID {
			continue
		}
		if run.Stop == nil || run.Stop.OperationID != request.OperationID || run.Stop.RequestHash != hash {
			return fmt.Errorf("terminal voter run belongs to a different operation")
		}
		if run.Identity.PodUID == target.PodUID && run.Identity.WALIdentity == target.WALIdentity && run.Drain != nil && run.Drain.Request == *run.Stop && run.Drain.Clean() && run.Exited {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("historical voter proof target UID/WAL receipt differs")
	}
	return nil
}

func verifyPriorVoterStops(out string, current fencing.Ledger) error {
	data, err := os.ReadFile(filepath.Join(out, "validated-voter-proof.json"))
	if err != nil {
		return fmt.Errorf("generation has no preserved online voter proof: %w", err)
	}
	var prior struct {
		Request operator.FenceRequest `json:"request"`
		Ledger  fenceLedgerState      `json:"ledger"`
	}
	if err := json.Unmarshal(data, &prior); err != nil {
		return err
	}
	matched := false
	for id, old := range prior.Ledger.Ledger.Runs {
		if old.Stop == nil || old.Stop.OperationID != prior.Request.OperationID {
			continue
		}
		if !bytes.Equal(encoded(old), encoded(current.Runs[id])) {
			return fmt.Errorf("prior terminal voter evidence changed during generation fencing")
		}
		matched = true
	}
	if !matched {
		return fmt.Errorf("preserved online proof has no immutable stopped voter")
	}
	return nil
}
