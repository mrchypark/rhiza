// Command fencing-e2e drives the cooperative executor and a separate
// runtime-agent component probe in disposable Kind. Runtime inspection in the
// cooperative scenario is a fault-injector oracle, never executor proof.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
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
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/mrchypark/rhiza"
	internalobjstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

var namespace = "rhiza-fencing-e2e"
var kubeContext = "kind-rhiza-generic-fencing"
var qualificationRunID string

const kindNode = "rhiza-generic-fencing-control-plane"
const admin = "fencing-test-admin"
const gcsHAScenarioEnv = "RHIZA_GCS_HA_SCENARIO"
const gcsHAMixedPendingScenario = "mixed-pending"
const gcsHAMixedPendingSource = "chaos-source"
const gcsMixedWorkDeadlineEnv = "FENCING_E2E_MIXED_WORK_DEADLINE_UNIX"
const gcsMixedHardDeadlineEnv = "FENCING_E2E_MIXED_HARD_DEADLINE_UNIX"

const gcsMixedHardLimit = 55 * time.Minute
const gcsMixedDefaultCleanupReserve = 3 * time.Minute

type gcsMixedHardDeadlineKey struct{}

type obj = map[string]any

func main() {
	if len(os.Args) != 2 {
		fatal(fmt.Errorf("usage: fencing-e2e manifests|local-cooperative-store-secret|wait-source|run|run-gcs-ha|run-gcs-ha-explicit|run-gcs-ha-ordinary-recovery|run-gcs-ha-async-empty|run-gcs-ha-async-empty-explicit|run-gcs-ha-async-nonempty|run-gcs-ha-manual-terminal|run-gcs-ha-missing-membership|run-gcs-ha-ack-loss|run-gcs-ha-dead-pod|run-gcs-ha-mixed-pending|prepare-gcs-ha-initial-head-ack-loss|cleanup-gcs-ha-initial-head-ack-loss|run-gcs-ha-initial-head-ack-loss|runtime-probe|runtime-agent-request|peer-admission-probe"))
	}
	if os.Args[1] == "peer-admission-probe" {
		fatal(runPeerAdmissionProbeCommand())
		return
	}
	if isGCSMixedPendingSelected(os.Args[1], strings.TrimSpace(os.Getenv(gcsHAScenarioEnv))) {
		fatal(runGCSMixedPendingCommand(configureGCSMixedPending, qualifyGCSMixedPending))
		return
	}
	if err := configureGCSMixedPendingSelection(); err != nil {
		fatal(err)
	}
	if os.Args[1] == "prepare-gcs-ha-initial-head-ack-loss" {
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
		defer cancel()
		if err := configureGCSHA(ctx); err != nil {
			fatal(err)
		}
		fatal(prepareGCSInitialHeadAckLoss(ctx))
		return
	}
	if os.Args[1] == "cleanup-gcs-ha-initial-head-ack-loss" {
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
		defer cancel()
		if err := configureGCSHACleanup(ctx); err != nil {
			fatal(err)
		}
		fatal(restoreGCSInitialHeadAckLoss(ctx))
		return
	}
	if os.Args[1] == "run-gcs-ha-initial-head-ack-loss" {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
		defer cancel()
		fatal(configureGCSHA(ctx))
		fatal(qualifyGCSInitialHeadAckLoss(ctx))
		return
	}
	if os.Args[1] == "runtime-agent-request" {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		fatal(runtimeAgentRequest(ctx))
		return
	}
	if os.Args[1] == "runtime-probe" {
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
		defer cancel()
		fatal(runtimeComponentProbe(ctx))
		return
	}
	if os.Args[1] == "manifests" {
		fatal(json.NewEncoder(os.Stdout).Encode(manifests()))
		return
	}
	if os.Args[1] == "local-cooperative-store-secret" {
		fatal(json.NewEncoder(os.Stdout).Encode(localCooperativeStoreSecret()))
		return
	}
	if os.Args[1] == "wait-source" {
		ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
		defer cancel()
		fatal(waitSource(ctx))
		return
	}
	if os.Args[1] == "run-gcs-ha" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()
		fatal(configureGCSHA(ctx))
		fatal(qualifyGCSHA(ctx))
		return
	}
	if os.Args[1] == "run-gcs-ha-explicit" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()
		fatal(configureGCSHA(ctx))
		fatal(qualifyGCSHAExplicit(ctx))
		return
	}
	if os.Args[1] == "run-gcs-ha-ordinary-recovery" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()
		fatal(configureGCSHA(ctx))
		fatal(qualifyGCSOrdinaryRecovery(ctx))
		return
	}
	if os.Args[1] == "run-gcs-ha-async-empty" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()
		fatal(configureGCSHA(ctx))
		fatal(qualifyGCSAsyncEmpty(ctx))
		return
	}
	if os.Args[1] == "run-gcs-ha-async-empty-explicit" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()
		fatal(configureGCSHA(ctx))
		fatal(qualifyGCSAsyncEmptyExplicit(ctx))
		return
	}
	if os.Args[1] == "run-gcs-ha-async-nonempty" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()
		fatal(configureGCSHA(ctx))
		fatal(qualifyGCSAsyncNonempty(ctx))
		return
	}
	if os.Args[1] == "run-gcs-ha-missing-membership" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()
		if os.Getenv("RHIZA_GCS_MISSING_MEMBERSHIP") != "yes" {
			fatal(fmt.Errorf("missing-membership GCS qualification is default-off; set RHIZA_GCS_MISSING_MEMBERSHIP=yes explicitly"))
		}
		fatal(configureGCSHA(ctx))
		fatal(qualifyGCSMissingMembership(ctx))
		return
	}
	if os.Args[1] == "run-gcs-ha-manual-terminal" {
		ctx, cancel := context.WithTimeout(context.Background(), 55*time.Minute)
		defer cancel()
		if os.Getenv("RHIZA_GCS_MANUAL_TERMINAL") != "yes" {
			fatal(fmt.Errorf("manual terminal GCS qualification is default-off; set RHIZA_GCS_MANUAL_TERMINAL=yes explicitly"))
		}
		fatal(configureGCSHA(ctx))
		fatal(qualifyGCSManualTerminal(ctx))
		return
	}
	if os.Args[1] == "run-gcs-ha-ack-loss" {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
		defer cancel()
		fatal(configureGCSHA(ctx))
		fatal(qualifyGCSHAAckLoss(ctx))
		return
	}
	if os.Args[1] == "run-gcs-ha-dead-pod" {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
		defer cancel()
		fatal(configureGCSHA(ctx))
		fatal(qualifyGCSDeadPod(ctx))
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

func configureGCSMixedPending(ctx context.Context) error {
	if err := configureGCSMixedPendingSelection(); err != nil {
		return err
	}
	return configureGCSHA(ctx)
}

func isGCSMixedPendingSelected(command, scenario string) bool {
	return command == "run-gcs-ha-mixed-pending" || (command == "run" && scenario != "")
}

// runGCSMixedPendingCommand owns the mixed scenario's complete signal and
// deadline lifetime. Callers must decide whether to exit only after it returns
// so qualification defers have completed before fatal/os.Exit is reached.
func runGCSMixedPendingCommand(configure, qualify func(context.Context) error) error {
	signalCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	workDeadline, hardDeadline, err := gcsMixedDeadlines(time.Now(), os.Getenv(gcsMixedWorkDeadlineEnv), os.Getenv(gcsMixedHardDeadlineEnv))
	if err != nil {
		return err
	}
	rootCtx, cancelHard := context.WithDeadline(signalCtx, hardDeadline)
	defer cancelHard()
	rootCtx = context.WithValue(rootCtx, gcsMixedHardDeadlineKey{}, hardDeadline)
	workCtx, cancelWork := context.WithDeadline(rootCtx, workDeadline)
	defer cancelWork()

	if configure == nil || qualify == nil {
		return fmt.Errorf("mixed qualification callbacks are unavailable")
	}
	if err := configure(workCtx); err != nil {
		return fmt.Errorf("configure mixed qualification: %w", err)
	}
	if err := workCtx.Err(); err != nil {
		return fmt.Errorf("mixed qualification work window ended before qualification: %w", err)
	}
	if err := qualify(workCtx); err != nil {
		return fmt.Errorf("mixed qualification failed: %w", err)
	}
	if err := signalCtx.Err(); err != nil {
		return fmt.Errorf("mixed qualification interrupted: %w", err)
	}
	if err := workCtx.Err(); err != nil {
		return fmt.Errorf("mixed qualification exceeded its work deadline: %w", err)
	}
	return nil
}

// gcsMixedDeadlines accepts the wrapper's single absolute work/hard horizon.
// Direct invocations get the same bounded ceiling and an explicit cleanup
// reserve; the shell may shorten either deadline but may never extend 55 min.
func gcsMixedDeadlines(now time.Time, workValue, hardValue string) (time.Time, time.Time, error) {
	if (workValue == "") != (hardValue == "") {
		return time.Time{}, time.Time{}, fmt.Errorf("%s and %s must be set together", gcsMixedWorkDeadlineEnv, gcsMixedHardDeadlineEnv)
	}
	var workDeadline, hardDeadline time.Time
	if workValue == "" {
		hardDeadline = now.Add(gcsMixedHardLimit)
		workDeadline = hardDeadline.Add(-gcsMixedDefaultCleanupReserve)
	} else {
		var err error
		workEpoch, err := strconv.ParseInt(workValue, 10, 64)
		if err != nil || workEpoch <= 0 {
			return time.Time{}, time.Time{}, fmt.Errorf("%s must be a positive Unix epoch second", gcsMixedWorkDeadlineEnv)
		}
		hardEpoch, err := strconv.ParseInt(hardValue, 10, 64)
		if err != nil || hardEpoch <= 0 {
			return time.Time{}, time.Time{}, fmt.Errorf("%s must be a positive Unix epoch second", gcsMixedHardDeadlineEnv)
		}
		workDeadline = time.Unix(workEpoch, 0)
		hardDeadline = time.Unix(hardEpoch, 0)
	}
	if !workDeadline.After(now) || !hardDeadline.After(workDeadline) {
		return time.Time{}, time.Time{}, fmt.Errorf("mixed work and hard deadlines must be future times with work ending first")
	}
	if hardDeadline.Sub(now) > gcsMixedHardLimit {
		return time.Time{}, time.Time{}, fmt.Errorf("mixed hard deadline exceeds the 55-minute maximum")
	}
	return workDeadline, hardDeadline, nil
}

// gcsMixedCleanupContext detaches fault restoration from canceled work while
// retaining the command's absolute hard stop. Each existing 45-second restore
// group can use this helper independently without extending the global horizon.
func gcsMixedCleanupContext(workCtx context.Context, maximum time.Duration) (context.Context, context.CancelFunc) {
	deadline := time.Now().Add(maximum)
	if hardDeadline, ok := workCtx.Value(gcsMixedHardDeadlineKey{}).(time.Time); ok && hardDeadline.Before(deadline) {
		deadline = hardDeadline
	}
	return context.WithDeadline(context.Background(), deadline)
}

func configureGCSMixedPendingSelection() error {
	scenario := strings.TrimSpace(os.Getenv(gcsHAScenarioEnv))
	if scenario == "" {
		return nil
	}
	if scenario != gcsHAMixedPendingScenario {
		return fmt.Errorf("unsupported %s %q", gcsHAScenarioEnv, scenario)
	}
	configuredSource := strings.TrimSpace(os.Getenv(gcsMixedRemoveGateSource))
	if configuredSource != "" && configuredSource != gcsHAMixedPendingSource {
		return fmt.Errorf("%s conflicts with the dedicated mixed fixture source", gcsMixedRemoveGateSource)
	}
	return os.Setenv(gcsMixedRemoveGateSource, gcsHAMixedPendingSource)
}

func configureGCSHA(ctx context.Context) error {
	return configureGCSHAContext(ctx, true)
}

func configureGCSHACleanup(ctx context.Context) error {
	return configureGCSHAContext(ctx, false)
}

func configureGCSHAContext(ctx context.Context, requireNoOwnedChaos bool) error {
	contextName := os.Getenv("RHIZA_KUBE_CONTEXT")
	ns := os.Getenv("RHIZA_NAMESPACE")
	runID := os.Getenv("RHIZA_RUN_ID")
	if contextName == "" || ns == "" || len(runID) != 8 {
		return fmt.Errorf("run-gcs-ha requires RHIZA_KUBE_CONTEXT, RHIZA_NAMESPACE, and an eight-character RHIZA_RUN_ID")
	}
	for _, r := range runID {
		if !(r >= 'a' && r <= 'f' || r >= '0' && r <= '9') {
			return fmt.Errorf("RHIZA_RUN_ID must be eight lowercase hexadecimal characters")
		}
	}
	for _, r := range ns {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-') {
			return fmt.Errorf("RHIZA_NAMESPACE is not a Kubernetes DNS label")
		}
	}
	namespace, kubeContext, qualificationRunID = ns, contextName, runID
	data, err := command(ctx, "kubectl", "--context", kubeContext, "get", "namespace", namespace, "-o", "json")
	if err != nil {
		return fmt.Errorf("read run namespace identity: %w", err)
	}
	var nsObject obj
	if err := json.Unmarshal(data, &nsObject); err != nil {
		return err
	}
	if nested(nsObject, "metadata", "labels", "chaos.rhiza.io/run") != runID || nested(nsObject, "metadata", "annotations", "chaos-mesh.org/inject") != "enabled" {
		return fmt.Errorf("namespace is not the fresh run-owned Chaos Mesh injection scope")
	}
	canPatch, err := command(ctx, "kubectl", "--context", kubeContext, "auth", "can-i", "patch", "pods", "--namespace", namespace)
	if err != nil || strings.TrimSpace(string(canPatch)) != "yes" {
		return fmt.Errorf("run identity lacks Pod patch permission required to UID-pin control-plane chaos: %v (%s)", err, strings.TrimSpace(string(canPatch)))
	}
	if requireNoOwnedChaos {
		for _, kind := range []string{"networkchaos", "podchaos"} {
			items, err := get(ctx, kind, "")
			if err != nil {
				return err
			}
			resources, _ := items["items"].([]any)
			for _, item := range resources {
				if nested(item, "metadata", "labels", "chaos.rhiza.io/run") == runID {
					return fmt.Errorf("fresh GCS recovery scope already contains run-owned %s %s", kind, nested(item, "metadata", "name"))
				}
			}
		}
	} else {
		for _, check := range [][2]string{{"patch", "deployment/rhiza-fencer"}, {"delete", "configmap/rhiza-fencer-ack-loss"}} {
			answer, err := command(ctx, "kubectl", "--context", kubeContext, "auth", "can-i", check[0], check[1], "--namespace", namespace)
			if err != nil || strings.TrimSpace(string(answer)) != "yes" {
				return fmt.Errorf("cleanup identity lacks %s permission required to restore the run-owned fencer configuration", check[0])
			}
		}
	}
	return nil
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
func digestJSON(value any) string {
	sum := sha256.Sum256(encoded(value))
	return fmt.Sprintf("%x", sum)
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

func manifests() obj {
	var members []obj
	tokens := map[string]string{}
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("rhiza-%d", i)
		token := "fencing-peer-" + id
		tokens[id] = token
		members = append(members, obj{"node_id": id, "url": "http://" + id + ".rhiza-headless:8080", "peer_url": "quic://" + id + ".rhiza-headless:9090", "public_key": rhiza.PeerPublicKey("chaos-source", id, token)})
	}
	config := obj{"RHIZA_CLUSTER_ID": "chaos-source", "RHIZA_BIND_ADDR": "0.0.0.0:8080", "RHIZA_PEER_ADDR": "0.0.0.0:9090", "RHIZA_DATA_DIR": "/data", "RHIZA_ENABLE_RECONFIGURATION": "true", "RHIZA_CHECKPOINT_INTERVAL": "0", "RHIZA_OBJSTORE_SYNC_INTERVAL": "1s", "RHIZA_OBJSTORE_GC_INTERVAL": "0", "RHIZA_OBJSTORE_PROVIDER": "s3", "RHIZA_OBJSTORE_ENDPOINT": "rhiza-versitygw:9000", "RHIZA_OBJSTORE_BUCKET": "rhiza", "RHIZA_OBJSTORE_INSECURE": "true", "RHIZA_OBJSTORE_REGION": "us-east-1", "RHIZA_OBJSTORE_PREFIX": "rhiza", "RHIZA_OBJSTORE_DURABILITY": "before-ack", "RHIZA_CLUSTER_MEMBERS": string(encoded(members))}
	configMap := obj{"apiVersion": "v1", "kind": "ConfigMap", "metadata": metadata("rhiza-config"), "data": config}
	credentials := obj{"apiVersion": "v1", "kind": "Secret", "metadata": metadata("rhiza-peer-credentials"), "stringData": obj{"RHIZA_ADMIN_TOKEN": admin, "RHIZA_PEER_TOKENS": string(encoded(tokens))}}
	store := resource("apps/v1", "Deployment", "rhiza-versitygw", obj{"replicas": 1, "selector": obj{"matchLabels": selector("rhiza-versitygw")}, "template": obj{"metadata": obj{"labels": selector("rhiza-versitygw")}, "spec": obj{"containers": []obj{{"name": "versitygw", "image": "ghcr.io/versity/versitygw@sha256:30292fc2eeacc67a36993b01f7a7a5e3361a19cced0e80c1d71cfa2a4b0a2499", "args": []string{"--port", ":9000", "--health", "/_/health", "posix", "/data"}, "env": []obj{value("ROOT_ACCESS_KEY", "fencing-local"), value("ROOT_SECRET_KEY", "fencing-local-secret")}, "ports": []obj{{"name": "s3", "containerPort": 9000}}, "readinessProbe": obj{"httpGet": obj{"path": "/_/health", "port": "s3"}}, "volumeMounts": []obj{{"name": "data", "mountPath": "/data"}}}}, "volumes": []obj{{"name": "data", "emptyDir": obj{}}}}}})
	storeService := resource("v1", "Service", "rhiza-versitygw", obj{"selector": selector("rhiza-versitygw"), "ports": []obj{{"port": 9000, "targetPort": "s3"}}})
	bucketJob := resource("batch/v1", "Job", "rhiza-create-bucket", obj{"backoffLimit": 0, "activeDeadlineSeconds": 120, "template": obj{"spec": obj{"restartPolicy": "Never", "containers": []obj{{"name": "create", "image": "amazon/aws-cli@sha256:83f8ffe939569070c5b66d22231862ab78718766d9d8e4c44ca84dd0be5569a5", "args": []string{"--endpoint-url=http://rhiza-versitygw:9000", "s3api", "create-bucket", "--bucket", "rhiza"}, "env": []obj{value("AWS_ACCESS_KEY_ID", "fencing-local"), value("AWS_SECRET_ACCESS_KEY", "fencing-local-secret"), value("AWS_DEFAULT_REGION", "us-east-1"), value("AWS_EC2_METADATA_DISABLED", "true")}}}}}})
	headless := resource("v1", "Service", "rhiza-headless", obj{"clusterIP": "None", "publishNotReadyAddresses": true, "selector": selector("rhiza"), "ports": []obj{{"name": "http", "port": 8080, "targetPort": "http"}, {"name": "peer-quic", "port": 9090, "targetPort": "peer-quic", "protocol": "UDP"}}})
	envFrom := []obj{{"configMapRef": obj{"name": "rhiza-config"}}, {"secretRef": obj{"name": "rhiza-peer-credentials"}}}
	sts := resource("apps/v1", "StatefulSet", "rhiza", obj{"replicas": 0, "serviceName": "rhiza-headless", "podManagementPolicy": "Parallel", "updateStrategy": obj{"type": "OnDelete"}, "selector": obj{"matchLabels": selector("rhiza")}, "template": obj{"metadata": obj{"labels": selector("rhiza")}, "spec": obj{"securityContext": obj{"runAsNonRoot": true, "fsGroup": 65532}, "containers": []obj{{"name": "rhiza", "image": "rhiza-fencing-host:local", "envFrom": envFrom, "env": []obj{field("RHIZA_NODE_ID", "metadata.name")}, "ports": []obj{{"name": "http", "containerPort": 8080}, {"name": "peer-quic", "containerPort": 9090, "protocol": "UDP"}}, "volumeMounts": []obj{{"name": "data", "mountPath": "/data"}}, "readinessProbe": obj{"httpGet": obj{"path": "/ready", "port": "http"}, "periodSeconds": 2}, "resources": obj{"requests": obj{"cpu": "250m", "memory": "256Mi"}, "limits": obj{"memory": "2Gi"}}}}, "volumes": []obj{{"name": "data", "emptyDir": obj{}}}}}})
	workload := nested(sts, "spec", "template", "spec").(obj)
	workload["hostPID"], workload["shareProcessNamespace"] = false, false
	application := workload["containers"].([]obj)[0]
	// The dedicated mixed REMOVE qualification opts in before the source
	// StatefulSet is first applied. Keeping the source ID in the Pod template
	// also makes the embedded fixture's gate self-disable on successor
	// generations without a rollout or mutable runtime switch.
	if source := os.Getenv("RHIZA_QUALIFICATION_REMOVE_GATE_SOURCE"); source != "" {
		application["env"] = append(application["env"].([]obj), value("RHIZA_QUALIFICATION_REMOVE_GATE_SOURCE", source))
	}
	application["securityContext"] = obj{"allowPrivilegeEscalation": false, "readOnlyRootFilesystem": true, "capabilities": obj{"drop": []string{"ALL"}}}
	opEnv := []obj{field("POD_NAMESPACE", "metadata.namespace"), value("RHIZA_AUTOMATIC_RECOVERY", "true"), value("RHIZA_FENCER_BACKEND", "kubernetes")}
	op := resource("apps/v1", "Deployment", "rhiza-operator", obj{"replicas": 0, "selector": obj{"matchLabels": selector("rhiza-operator")}, "template": obj{"metadata": obj{"labels": selector("rhiza-operator")}, "spec": obj{"serviceAccountName": "rhiza-operator", "securityContext": obj{"runAsNonRoot": true, "fsGroup": 65532}, "containers": []obj{{"name": "operator", "image": "rhiza-fencing-operator:local", "args": []string{"--namespace=$(POD_NAMESPACE)", "--poll-interval=1s"}, "env": opEnv, "envFrom": []obj{envFrom[0]}, "volumeMounts": []obj{{"name": "tmp", "mountPath": "/tmp"}}}}, "volumes": []obj{{"name": "tmp", "emptyDir": obj{}}}}}})
	binding := resource("rhiza.mrchypark.dev/v1alpha1", "RhizaCluster", "rhiza", obj{"logicalID": "fencing-e2e", "statefulSet": "rhiza", "container": "rhiza", "adoptClusterID": "chaos-source", "automatic": true, "voterCount": len(members), "policy": obj{"failureGraceSeconds": 15, "cooldownSeconds": 3, "maxRecoveriesPerDay": 3, "allowDataLoss": false}})
	bucketJob["spec"].(obj)["suspend"] = true
	return obj{"apiVersion": "v1", "kind": "List", "items": []obj{configMap, credentials, store, storeService, bucketJob, headless, sts, op, binding}}
}

func localCooperativeStoreSecret() obj {
	return obj{"apiVersion": "v1", "kind": "Secret", "metadata": metadata("rhiza-object-store"), "stringData": obj{"RHIZA_OBJSTORE_ACCESS_KEY": "fencing-local", "RHIZA_OBJSTORE_SECRET_KEY": "fencing-local-secret"}}
}

func command(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	data, err := cmd.CombinedOutput()
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			err = errors.Join(err, ctxErr)
		}
		return nil, fmt.Errorf("%s command failed: %w: %s", name, err, data)
	}
	return data, nil
}

// startFencerForward opens only the run-owned fencer Service and keeps the
// kubectl subprocess log separate from the short-lived controller token.
func startFencerForward(ctx context.Context, out string) (*forward, error) {
	return startServiceForward(ctx, out, "rhiza-fencer", 9444)
}

func startServiceForward(ctx context.Context, out, service string, servicePort int) (*forward, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	log, err := os.CreateTemp(out, service+"-portforward-*.log")
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, "kubectl", "--context", kubeContext, "-n", namespace, "port-forward", "service/"+service, fmt.Sprintf("%d:%d", port, servicePort), "--address=127.0.0.1")
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		_ = log.Close()
		return nil, err
	}
	f := &forward{cmd: cmd, port: port, done: make(chan error, 1), log: log}
	go func() { f.done <- cmd.Wait() }()
	ready, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := wait(ready, "port-forward "+service, func() (bool, error) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 100*time.Millisecond)
		if err != nil {
			return false, nil
		}
		_ = conn.Close()
		return true, nil
	}); err != nil {
		return nil, joinForwardCloseError(f, err)
	}
	return f, nil
}

// requestGenerationFence uses a TokenRequest token only in memory. It never
// places credentials in argv, files, events, or subprocess logs.
func requestGenerationFence(ctx context.Context, source gcsRecoverySource, request operator.FenceRequest, out string) error {
	if err := validateExplicitGenerationFenceRequest(request, source, request.OperationID); err != nil {
		return err
	}
	allowed, err := kubectl(ctx, "auth", "can-i", "create", "serviceaccounts/token")
	if err != nil || strings.TrimSpace(string(allowed)) != "yes" {
		return fmt.Errorf("run identity cannot mint the required short-lived controller token")
	}
	cmd := exec.CommandContext(ctx, "kubectl", "--context", kubeContext, "-n", namespace, "create", "token", "rhiza-operator", "--audience=rhiza-fencer", "--duration=10m")
	tokenBytes, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("mint short-lived rhiza-fencer audience token: %w", err)
	}
	defer func() {
		for i := range tokenBytes {
			tokenBytes[i] = 0
		}
	}()
	token := strings.TrimSpace(string(tokenBytes))
	if token == "" || strings.ContainsAny(token, "\r\n \t") {
		return fmt.Errorf("TokenRequest returned an invalid controller token")
	}
	caConfig, err := get(ctx, "configmap", "rhiza-fencer-ca")
	if err != nil {
		return fmt.Errorf("read fencer CA bundle: %w", err)
	}
	caPEM := text(nested(caConfig, "data", "ca.crt"))
	roots := x509.NewCertPool()
	if caPEM == "" || !roots.AppendCertsFromPEM([]byte(caPEM)) {
		return fmt.Errorf("deployed fencer CA bundle is invalid")
	}
	f, err := startFencerForward(ctx, out)
	if err != nil {
		return err
	}
	defer f.close()
	body, err := json.Marshal(request)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("https://127.0.0.1:%d/v1/fences", f.port), bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Idempotency-Key", request.OperationID)
	httpReq.Header.Set("Authorization", "Bearer "+token)
	client := &http.Client{Timeout: 15 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, ServerName: "rhiza-fencer"}}}
	defer client.CloseIdleConnections()
	response, err := client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("submit exact explicit generation fence request")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		return fmt.Errorf("explicit generation fence did not remain pending during runtime-agent isolation (HTTP %d)", response.StatusCode)
	}
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	if err != nil || len(responseBody) >= 64<<10 {
		return fmt.Errorf("read bounded pending generation fence response")
	}
	return nil
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

type runOwnedPod struct {
	Name string
	UID  string
}

type runOwnedOperator struct {
	Name            string
	UID             string
	ResourceVersion string
	Generation      int64
	Replicas        int64
	Selector        map[string]string
	Pods            []runOwnedPod
}

func captureRunOwnedOperator(ctx context.Context) (runOwnedOperator, error) {
	return captureRunOwnedDeployment(ctx, "rhiza-operator")
}

func captureRunOwnedDeployment(ctx context.Context, name string) (runOwnedOperator, error) {
	var owned runOwnedOperator
	deployment, err := get(ctx, "deployment", name)
	if err != nil {
		return owned, err
	}
	owned.Name = name
	owned.UID = text(nested(deployment, "metadata", "uid"))
	owned.ResourceVersion = text(nested(deployment, "metadata", "resourceVersion"))
	owned.Generation = integer(nested(deployment, "metadata", "generation"))
	owned.Replicas = integer(nested(deployment, "spec", "replicas"))
	selector, _ := nested(deployment, "spec", "selector", "matchLabels").(map[string]any)
	owned.Selector = make(map[string]string, len(selector))
	for key, value := range selector {
		label := text(value)
		if key == "" || label == "" {
			return owned, fmt.Errorf("run-owned operator Deployment selector is incomplete")
		}
		owned.Selector[key] = label
	}
	if owned.Name == "" || owned.UID == "" || owned.ResourceVersion == "" || owned.Generation < 1 || owned.Replicas < 1 || len(owned.Selector) == 0 {
		return owned, fmt.Errorf("run-owned operator Deployment identity or desired state is incomplete")
	}
	pods, err := kubectl(ctx, "get", "pods", "-l", labelSelector(owned.Selector), "-o", "json")
	if err != nil {
		return owned, err
	}
	var list obj
	if err := json.Unmarshal(pods, &list); err != nil {
		return owned, err
	}
	items, _ := list["items"].([]any)
	for _, item := range items {
		podName := text(nested(item, "metadata", "name"))
		uid := text(nested(item, "metadata", "uid"))
		if podName == "" || uid == "" {
			return owned, fmt.Errorf("selected %s Pod lacks identity", name)
		}
		owned.Pods = append(owned.Pods, runOwnedPod{Name: podName, UID: uid})
	}
	if int64(len(owned.Pods)) != owned.Replicas {
		return owned, fmt.Errorf("%s Pod set differs from run-owned Deployment desired replicas", name)
	}
	return owned, nil
}

func (owned runOwnedOperator) setReplicas(ctx context.Context, replicas int64) error {
	if owned.UID == "" || owned.ResourceVersion == "" || replicas < 0 {
		return fmt.Errorf("run-owned operator scale identity is incomplete")
	}
	patch, _ := json.Marshal([]obj{
		{"op": "test", "path": "/metadata/uid", "value": owned.UID},
		{"op": "test", "path": "/metadata/resourceVersion", "value": owned.ResourceVersion},
		{"op": "replace", "path": "/spec/replicas", "value": replicas},
	})
	_, err := kubectl(ctx, "patch", "deployment", owned.Name, "--type=json", "-p", string(patch))
	return err
}

func refreshRunOwnedDeploymentResourceVersion(ctx context.Context, owned *runOwnedOperator) error {
	current, err := get(ctx, "deployment", owned.Name)
	if err != nil {
		return err
	}
	if text(nested(current, "metadata", "uid")) != owned.UID || !deploymentSelectorMatches(current, owned.Selector) ||
		integer(nested(current, "spec", "replicas")) != owned.Replicas {
		return fmt.Errorf("run-owned %s Deployment changed before the UID/RV-guarded scale", owned.Name)
	}
	resourceVersion := text(nested(current, "metadata", "resourceVersion"))
	if resourceVersion == "" {
		return fmt.Errorf("run-owned %s Deployment resourceVersion is unavailable", owned.Name)
	}
	owned.ResourceVersion = resourceVersion
	return nil
}

func labelSelector(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+labels[key])
	}
	return strings.Join(parts, ",")
}

func deploymentSelectorMatches(deployment obj, selector map[string]string) bool {
	actual, _ := nested(deployment, "spec", "selector", "matchLabels").(map[string]any)
	if len(actual) != len(selector) {
		return false
	}
	for key, want := range selector {
		if text(actual[key]) != want {
			return false
		}
	}
	return true
}

func waitRunOwnedOperatorAbsent(ctx context.Context, operator, fencer runOwnedOperator) error {
	if operator.Name != "rhiza-operator" || fencer.Name != "rhiza-fencer" || operator.UID == "" || fencer.UID == "" {
		return fmt.Errorf("controller-absent check lacks exact run-owned deployment identities")
	}
	if err := wait(ctx, "run-owned operator observed at zero replicas", func() (bool, error) {
		current, err := get(ctx, "deployment", operator.Name)
		if err != nil {
			return false, err
		}
		if text(nested(current, "metadata", "uid")) != operator.UID || !deploymentSelectorMatches(current, operator.Selector) || integer(nested(current, "spec", "replicas")) != 0 ||
			integer(nested(current, "status", "observedGeneration")) < integer(nested(current, "metadata", "generation")) {
			return false, nil
		}
		fencerCurrent, err := get(ctx, "deployment", fencer.Name)
		if err != nil {
			return false, err
		}
		if text(nested(fencerCurrent, "metadata", "uid")) != fencer.UID || !deploymentSelectorMatches(fencerCurrent, fencer.Selector) || integer(nested(fencerCurrent, "spec", "replicas")) != fencer.Replicas ||
			integer(nested(fencerCurrent, "status", "observedGeneration")) < integer(nested(fencerCurrent, "metadata", "generation")) ||
			integer(nested(fencerCurrent, "status", "replicas")) != fencer.Replicas ||
			integer(nested(fencerCurrent, "status", "readyReplicas")) != fencer.Replicas || integer(nested(fencerCurrent, "status", "unavailableReplicas")) != 0 {
			return false, fmt.Errorf("fencer Deployment changed or is not Ready while operator is absent")
		}
		for _, pod := range fencer.Pods {
			currentPod, err := get(ctx, "pod", pod.Name)
			if err != nil || text(nested(currentPod, "metadata", "uid")) != pod.UID {
				return false, fmt.Errorf("run-owned fencer Pod identity changed while operator is absent")
			}
		}
		for _, pod := range operator.Pods {
			_, err := get(ctx, "pod", pod.Name)
			if err == nil {
				return false, nil
			}
			if !strings.Contains(err.Error(), "NotFound") {
				return false, fmt.Errorf("direct GET of operator Pod %s failed: %w", pod.Name, err)
			}
		}
		return true, nil
	}); err != nil {
		return err
	}
	return nil
}

func restoreRunOwnedOperator(ctx context.Context, owned runOwnedOperator) error {
	current, err := get(ctx, "deployment", owned.Name)
	if err != nil {
		return err
	}
	if text(nested(current, "metadata", "uid")) != owned.UID || !deploymentSelectorMatches(current, owned.Selector) {
		return fmt.Errorf("operator Deployment changed; refusing to restore a non-owned resource")
	}
	currentReplicas := integer(nested(current, "spec", "replicas"))
	if currentReplicas != 0 && currentReplicas != owned.Replicas {
		return fmt.Errorf("operator Deployment replica count changed; refusing to overwrite another actor's update")
	}
	if currentReplicas == owned.Replicas {
		return waitRunOwnedDeploymentReady(ctx, owned)
	}
	owned.ResourceVersion = text(nested(current, "metadata", "resourceVersion"))
	if err := owned.setReplicas(ctx, owned.Replicas); err != nil {
		return err
	}
	return waitRunOwnedDeploymentReady(ctx, owned)
}

func waitRunOwnedDeploymentReady(ctx context.Context, owned runOwnedOperator) error {
	return wait(ctx, "run-owned operator Deployment restored and Ready", func() (bool, error) {
		restored, err := get(ctx, "deployment", owned.Name)
		if err != nil {
			return false, err
		}
		if text(nested(restored, "metadata", "uid")) != owned.UID || !deploymentSelectorMatches(restored, owned.Selector) ||
			integer(nested(restored, "spec", "replicas")) != owned.Replicas {
			return false, fmt.Errorf("run-owned operator Deployment identity or replica count changed during restore")
		}
		return integer(nested(restored, "status", "observedGeneration")) >= integer(nested(restored, "metadata", "generation")) &&
			integer(nested(restored, "status", "readyReplicas")) == owned.Replicas && integer(nested(restored, "status", "unavailableReplicas")) == 0, nil
	})
}

func verifyRunOwnedDeploymentReady(ctx context.Context, owned runOwnedOperator) error {
	current, err := get(ctx, "deployment", owned.Name)
	if err != nil {
		return err
	}
	replicas := integer(nested(current, "spec", "replicas"))
	if text(nested(current, "metadata", "uid")) != owned.UID || !deploymentSelectorMatches(current, owned.Selector) || replicas != owned.Replicas ||
		integer(nested(current, "status", "observedGeneration")) < integer(nested(current, "metadata", "generation")) ||
		integer(nested(current, "status", "replicas")) != replicas || integer(nested(current, "status", "readyReplicas")) != replicas ||
		integer(nested(current, "status", "unavailableReplicas")) != 0 {
		return fmt.Errorf("run-owned %s Deployment is not Ready at its captured UID and replica count", owned.Name)
	}
	for _, pod := range owned.Pods {
		currentPod, err := get(ctx, "pod", pod.Name)
		if err != nil || text(nested(currentPod, "metadata", "uid")) != pod.UID || !podReady(currentPod) {
			return fmt.Errorf("run-owned %s Pod %s is not Ready at its captured UID: %v", owned.Name, pod.Name, err)
		}
	}
	return nil
}

type forward struct {
	cmd      *exec.Cmd
	port     int
	done     chan error
	log      *os.File
	closeOne sync.Once
	closeErr error
}

func (f *forward) close() error {
	if f == nil {
		return nil
	}
	f.closeOne.Do(func() {
		var closeErrors []error
		killed := false
		if f.cmd == nil || f.cmd.Process == nil {
			closeErrors = append(closeErrors, fmt.Errorf("port-forward process identity is unavailable"))
		} else if err := f.cmd.Process.Kill(); err == nil {
			killed = true
		} else if !errors.Is(err, os.ErrProcessDone) {
			closeErrors = append(closeErrors, fmt.Errorf("kill port-forward process: %w", err))
		}
		if f.done == nil {
			closeErrors = append(closeErrors, fmt.Errorf("port-forward wait channel is unavailable"))
		} else if waitErr, ok := <-f.done; !ok {
			closeErrors = append(closeErrors, fmt.Errorf("port-forward wait channel closed without a result"))
		} else if waitErr != nil {
			var exitErr *exec.ExitError
			if !killed || !errors.As(waitErr, &exitErr) || exitErr.ExitCode() != -1 {
				closeErrors = append(closeErrors, fmt.Errorf("wait for port-forward process: %w", waitErr))
			}
		}
		if f.log == nil {
			closeErrors = append(closeErrors, fmt.Errorf("port-forward log file is unavailable"))
		} else if err := f.log.Close(); err != nil {
			closeErrors = append(closeErrors, fmt.Errorf("close port-forward log file: %w", err))
		}
		f.closeErr = errors.Join(closeErrors...)
	})
	return f.closeErr
}

const fencerDefaultAuthorityStatePrefix = "rhiza-authority-state"

// openQualificationLedgerReader opens the disposable fixture's canonical S3
// bucket through a run-owned loopback port-forward. The cluster DNS endpoint
// from rhiza-config is only resolvable inside Kind, not by this driver process.
func openQualificationLedgerReader(ctx context.Context, out string) (proofLedgerReader, func() error, error) {
	config, err := localFixtureObjectStoreConfig()
	if err != nil {
		return nil, nil, err
	}
	prefix, err := qualificationAuthorityStatePrefix(ctx)
	if err != nil {
		return nil, nil, err
	}
	forward, err := startServiceForward(ctx, out, "rhiza-versitygw", 9000)
	if err != nil {
		return nil, nil, err
	}
	config.Endpoint = fmt.Sprintf("127.0.0.1:%d", forward.port)
	bucket, err := internalobjstore.NewBucket(config)
	if err != nil {
		return nil, nil, joinForwardCloseError(forward, err)
	}
	store, err := operator.NewAuthorityStateStore(bucket, prefix)
	if err != nil {
		return nil, nil, errors.Join(err, closeQualificationLedgerResources(bucket.Close, forward))
	}
	// This fixture deploys the cooperative fencer with its legacy flag enabled,
	// but its configured canonical ObjectStorage always takes precedence.
	reader := configuredProofLedgerReader(store, true, readLegacyConfigMapLedger)
	return reader, func() error {
		return closeQualificationLedgerResources(bucket.Close, forward)
	}, nil
}

func closeQualificationLedgerResources(closeBucket func() error, forward *forward) error {
	var bucketErr error
	if closeBucket != nil {
		if err := closeBucket(); err != nil {
			bucketErr = fmt.Errorf("close canonical authority bucket: %w", err)
		}
	}
	return errors.Join(bucketErr, joinForwardCloseError(forward, nil))
}

func joinForwardCloseError(forward *forward, operationErr error) error {
	if forward == nil {
		return operationErr
	}
	return errors.Join(operationErr, forward.close())
}

func localFixtureObjectStoreConfig() (internalobjstore.Config, error) {
	items, _ := manifests()["items"].([]obj)
	var data obj
	for _, item := range items {
		if nested(item, "kind") == "ConfigMap" && nested(item, "metadata", "name") == "rhiza-config" {
			data, _ = item["data"].(obj)
			break
		}
	}
	secret, _ := localCooperativeStoreSecret()["stringData"].(obj)
	if len(data) == 0 || text(data["RHIZA_OBJSTORE_PROVIDER"]) != "s3" || text(data["RHIZA_OBJSTORE_BUCKET"]) == "" {
		return internalobjstore.Config{}, fmt.Errorf("local qualification fixture has no canonical S3 configuration")
	}
	if text(secret["RHIZA_OBJSTORE_ACCESS_KEY"]) == "" || text(secret["RHIZA_OBJSTORE_SECRET_KEY"]) == "" {
		return internalobjstore.Config{}, fmt.Errorf("local qualification fixture has incomplete S3 credentials")
	}
	return internalobjstore.Config{
		Provider:     internalobjstore.Provider(text(data["RHIZA_OBJSTORE_PROVIDER"])),
		Prefix:       text(data["RHIZA_OBJSTORE_PREFIX"]),
		Endpoint:     text(data["RHIZA_OBJSTORE_ENDPOINT"]),
		Bucket:       text(data["RHIZA_OBJSTORE_BUCKET"]),
		Insecure:     text(data["RHIZA_OBJSTORE_INSECURE"]) == "true",
		AccessKey:    text(secret["RHIZA_OBJSTORE_ACCESS_KEY"]),
		SecretKey:    text(secret["RHIZA_OBJSTORE_SECRET_KEY"]),
		SessionToken: text(secret["RHIZA_OBJSTORE_SESSION_TOKEN"]),
	}, nil
}

func qualificationAuthorityStatePrefix(ctx context.Context) (string, error) {
	deployment, err := get(ctx, "deployment", "rhiza-fencer")
	if err != nil {
		return "", err
	}
	return fencerAuthorityStatePrefix(deployment)
}

func fencerAuthorityStatePrefix(deployment obj) (string, error) {
	containers, _ := nested(deployment, "spec", "template", "spec", "containers").([]any)
	var args []any
	var env []any
	var envFrom []any
	foundAuthority := false
	for _, container := range containers {
		if nested(container, "name") != "authority" {
			continue
		}
		foundAuthority = true
		args, _ = nested(container, "args").([]any)
		env, _ = nested(container, "env").([]any)
		envFrom, _ = nested(container, "envFrom").([]any)
	}
	if !foundAuthority {
		return "", fmt.Errorf("fencer authority container is unavailable")
	}
	configMap, secret := false, false
	for _, source := range envFrom {
		if name := text(nested(source, "configMapRef", "name")); name != "" {
			if name != "rhiza-config" {
				return "", fmt.Errorf("fencer authority has an unexpected configuration source")
			}
			configMap = true
		}
		if name := text(nested(source, "secretRef", "name")); name != "" {
			if name != "rhiza-object-store" {
				return "", fmt.Errorf("fencer authority has an unexpected credential source")
			}
			secret = true
		}
	}
	if !configMap || !secret {
		return "", fmt.Errorf("fencer authority is not using the local canonical-store fixture")
	}
	envPrefix := ""
	for _, entry := range env {
		if nested(entry, "name") != "FENCER_AUTHORITY_STATE_PREFIX" {
			continue
		}
		if nested(entry, "valueFrom") != nil {
			return "", fmt.Errorf("fencer authority prefix uses an unresolved environment reference")
		}
		envPrefix = text(nested(entry, "value"))
	}
	flagPrefix := ""
	flagSet := false
	for i := 0; i < len(args); i++ {
		arg := text(args[i])
		if value, found := strings.CutPrefix(arg, "--authority-state-prefix="); found {
			flagPrefix = value
			flagSet = true
		} else if arg == "--authority-state-prefix" {
			if i+1 >= len(args) {
				return "", fmt.Errorf("fencer authority prefix flag has no value")
			}
			i++
			flagPrefix = text(args[i])
			flagSet = true
		}
	}
	if flagSet && flagPrefix != "" {
		return flagPrefix, nil
	}
	if flagSet {
		return fencerDefaultAuthorityStatePrefix, nil
	}
	if envPrefix != "" {
		return envPrefix, nil
	}
	return fencerDefaultAuthorityStatePrefix, nil
}

type proofLedgerReader func(context.Context, string, string) (fenceLedgerState, error)

func configuredProofLedgerReader(canonical *operator.AuthorityStateStore, legacyCooperative bool, legacy proofLedgerReader) proofLedgerReader {
	if canonical != nil {
		return func(ctx context.Context, stsUID, sourceID string) (fenceLedgerState, error) {
			root, _, err := canonical.ReadRoot(ctx, stsUID, sourceID)
			if err != nil {
				return fenceLedgerState{}, err
			}
			return fenceLedgerState{StatefulSetUID: root.StatefulSetUID, Ledger: root.Ledger}, nil
		}
	}
	if legacyCooperative && legacy != nil {
		return legacy
	}
	return func(context.Context, string, string) (fenceLedgerState, error) {
		return fenceLedgerState{}, fmt.Errorf("canonical authority state is not configured")
	}
}

func readLegacyConfigMapLedger(ctx context.Context, stsUID, sourceID string) (fenceLedgerState, error) {
	maps, err := get(ctx, "configmaps", "")
	if err != nil {
		return fenceLedgerState{}, err
	}
	items, _ := maps["items"].([]any)
	for _, item := range items {
		data := text(nested(item, "data", "ledger.json"))
		if data == "" {
			continue
		}
		var state fenceLedgerState
		if err := json.Unmarshal([]byte(data), &state); err != nil {
			return fenceLedgerState{}, err
		}
		if state.StatefulSetUID == stsUID && state.Ledger.SourceClusterID == sourceID {
			return state, nil
		}
	}
	return fenceLedgerState{}, fmt.Errorf("legacy cooperative source ledger unavailable")
}

func startForward(ctx context.Context, pod, out string) (*forward, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	log, err := os.CreateTemp(out, pod+"-portforward-*.log")
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, "kubectl", "--context", kubeContext, "-n", namespace, "port-forward", "pod/"+pod, fmt.Sprintf("%d:8080", port), "--address=127.0.0.1")
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		_ = log.Close()
		return nil, err
	}
	f := &forward{cmd: cmd, port: port, done: make(chan error, 1), log: log}
	go func() { f.done <- cmd.Wait() }()
	ready, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	err = wait(ready, "port-forward "+pod, func() (bool, error) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 100*time.Millisecond)
		if err != nil {
			return false, nil
		}
		_ = conn.Close()
		return true, nil
	})
	if err != nil {
		return nil, joinForwardCloseError(f, err)
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
		captureErr := captureVoterStartupDiagnostics("", nil, out, "bootstrap")
		return errors.Join(err, captureErr)
	}
	ledgerReader, closeLedgerReader, err := openQualificationLedgerReader(ctx, out)
	if err != nil {
		return fmt.Errorf("open canonical fencing ledger reader: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, closeLedgerReader()) }()
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
	var oldUIDs []string
	f, err := startForward(ctx, "rhiza-0", out)
	if err != nil {
		return err
	}
	defer f.close()
	if _, err = call(ctx, f, "/sql/execute", obj{"request_id": "fencing-create", "sql": "CREATE TABLE fencing_items (id INTEGER PRIMARY KEY, value TEXT)"}, admin); err != nil {
		return err
	}
	if _, err = call(ctx, f, "/sql/execute", obj{"request_id": "fencing-ack", "sql": "INSERT INTO fencing_items VALUES (1, 'acknowledged')"}, admin); err != nil {
		return err
	}
	event("source_acknowledged", obj{"rows": 1})
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
	if _, err = call(ctx, f, "/sql/query", obj{"sql": "SELECT id FROM fencing_items", "consistency": "linearizable"}, admin); err != nil {
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
	current, err := qualifyOnline(onlineCtx, f, stsUID, bindingUID, out, event, &observationFaults, ledgerReader)
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
			return true, verifyGenerationProofWithReader(ctx, item, stsUID, bindingUID, oldUIDs, out, ledgerReader, current...)
		}
		return false, nil
	}); err != nil {
		return err
	}
	event("generic_generation_proof", obj{"oldUIDs": oldUIDs})
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
	for _, container := range runtimes {
		if nested(container, "state") != "CONTAINER_RUNNING" {
			continue
		}
		uid := text(nested(container, "labels", "io.kubernetes.pod.uid"))
		for _, old := range append(append([]string(nil), oldUIDs...), initialUIDs...) {
			if uid == old {
				return fmt.Errorf("old runtime remains live after proof")
			}
		}
	}
	event("PASS", obj{"sourceACKRows": 3, "onlineVoterReplacement": true, "newGenerationWrite": true, "oldRuntimesGone": true, "executor": "generic-cooperative"})
	return nil
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

func readMembership(ctx context.Context, f *forward) (network.MembershipStatus, error) {
	data, err := call(ctx, f, "/membership/status", nil, admin)
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

func qualifyOnline(ctx context.Context, survivor *forward, stsUID, bindingUID, out string, event func(string, obj), faults *[]*observationFault, ledgerReader proofLedgerReader) ([]operator.FenceTarget, error) {
	failedPod, err := get(ctx, "pod", "rhiza-2")
	if err != nil {
		return nil, err
	}
	failedForward, err := startForward(ctx, "rhiza-2", out)
	if err != nil {
		return nil, err
	}
	failedStatus, err := readMembership(ctx, failedForward)
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
	if _, err := call(ctx, survivor, "/sql/execute", obj{"request_id": "fencing-surviving-quorum", "sql": "INSERT INTO fencing_items VALUES (2, 'quorum-survived')"}, admin); err != nil {
		return nil, err
	}
	if err := checkRows(ctx, survivor, admin, `[[1,"acknowledged"],[2,"quorum-survived"]]`); err != nil {
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
			if err := verifyScopedProofWithReader(ctx, item, stsUID, bindingUID, []string{failed.PodUID}, operator.ScopeVoter, []operator.FenceTarget{failed}, out, ledgerReader); err != nil {
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
		captureErr := captureOnlineVoterDiagnostics(voterRequest.OperationID, fault, out)
		return nil, errors.Join(err, captureErr)
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
		status, statusErr := readMembership(ctx, f)
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
	if err := verifyRegisteredTargets(ctx, targets, stsUID, out, ledgerReader); err != nil {
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
	if _, err := call(ctx, learnerForward, "/sql/execute", obj{"request_id": "fencing-learner-promoted", "sql": "INSERT INTO fencing_items VALUES (3, 'learner-promoted')"}, admin); err != nil {
		return nil, err
	}
	if err := checkRows(ctx, learnerForward, admin, `[[1,"acknowledged"],[2,"quorum-survived"],[3,"learner-promoted"]]`); err != nil {
		return nil, err
	}
	event("online_replacement_linearizable_readback", obj{"targets": targets, "rows": 3, "configID": configID})
	if err := fault.remove(ctx); err != nil {
		return nil, err
	}
	event("online_operator_observation_fault_removed", obj{"fault": fault})
	return targets, os.WriteFile(filepath.Join(out, "online-completed-membership.json"), encoded(obj{"recovery": child, "targets": targets, "observations": observations}), 0600)
}

// captureOnlineVoterDiagnostics preserves bounded private state for the exact
// automatic learner involved in a failed online promotion. It is diagnostic
// only: it does not change readiness or retry behavior.
func captureOnlineVoterDiagnostics(operationID string, fault *observationFault, out string) error {
	return captureVoterStartupDiagnostics(operationID, fault, out, "online")
}

// captureVoterStartupDiagnostics is a bounded, read-only failure snapshot. It
// captures logs before optional API and membership probes so a short-lived
// failed container is not lost to later diagnostic work. An empty operationID
// means bootstrap observation and is never replaced with a synthetic ID.
func captureVoterStartupDiagnostics(operationID string, fault *observationFault, out, mode string) error {
	if out == "" {
		return nil
	}
	dir := filepath.Join(out, "private-online-voter-diagnostics")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("diagnostic directory creation failed")
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return fmt.Errorf("diagnostic directory protection failed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pods, err := get(ctx, "pods", "")
	var items []any
	if err == nil {
		items, _ = pods["items"].([]any)
	}
	var learner any
	for _, pod := range items {
		name := text(nested(pod, "metadata", "name"))
		owner := text(nested(pod, "metadata", "annotations", "rhiza.mrchypark.dev/recovery-owner"))
		matches := false
		if mode == "online" && operationID != "" {
			matches = owner == operationID || name == operationID+"-learner"
		} else if mode == "bootstrap" {
			matches = podOwnedByName(pod, "rhiza")
		}
		if matches {
			if learner == nil || nested(pod, "metadata", "deletionTimestamp") == nil {
				learner = pod
			}
		}
	}
	logCaptures, logErr := captureVoterContainerLogs(ctx, dir, learner)
	root, rootErr := get(ctx, "rhizacluster", "rhiza")
	if rootErr != nil {
		root = nil
	}
	sts, stsErr := get(ctx, "statefulset", "rhiza")
	if stsErr != nil {
		sts = nil
	}
	stsUID := text(nested(sts, "metadata", "uid"))
	var captureErr error
	if err != nil {
		captureErr = errors.Join(captureErr, fmt.Errorf("Pod inventory diagnostic query failed"))
	}
	if rootErr != nil {
		captureErr = errors.Join(captureErr, fmt.Errorf("root diagnostic query failed"))
	}
	if stsErr != nil {
		captureErr = errors.Join(captureErr, fmt.Errorf("StatefulSet diagnostic query failed"))
	}
	captureErr = errors.Join(captureErr, logErr)
	var events any
	eventsQuerySucceeded := false
	if allEvents, eventErr := get(ctx, "events", ""); eventErr == nil {
		eventsQuerySucceeded = true
		items, _ := allEvents["items"].([]any)
		learnerUID := text(nested(learner, "metadata", "uid"))
		events = eventsForLearnerUID(items, learnerUID)
	} else {
		captureErr = errors.Join(captureErr, fmt.Errorf("event diagnostic query failed"))
	}
	recoveries, recoveryErr := get(ctx, "rhizarecoveries", "")
	var recovery any
	if recoveryErr == nil {
		items, _ := recoveries["items"].([]any)
		for _, item := range items {
			if nested(item, "metadata", "name") == operationID {
				recovery = item
				break
			}
		}
	} else {
		captureErr = errors.Join(captureErr, fmt.Errorf("recovery diagnostic query failed"))
	}
	secretObservations, secretObservationsTruncated := learnerSecretObservations(ctx, learner, recovery, operationID)
	parent, parentErr := root, rootErr
	private := obj{
		"mode": mode, "operationID": operationID, "learner": privateLearnerDiagnosticView(learner), "events": events,
		"podsQuerySucceeded": err == nil, "eventsQuerySucceeded": eventsQuerySucceeded, "recoveryQuerySucceeded": recoveryErr == nil,
		"secretObservations": secretObservations, "secretObservationsTruncated": secretObservationsTruncated,
		"membershipObservation": inspectLearnerMembership(ctx, learner, recovery, parent, out),
		"recoveryFound":         recovery != nil, "recovery": privateRecoveryDiagnosticView(recovery), "parent": privateParentDiagnosticView(parent),
		"parentQuerySucceeded":           parentErr == nil,
		"parentRecoveryMatchesOperation": operationID != "" && parentErr == nil && nested(parent, "status", "activeRecovery") == operationID,
		"capture": obj{
			"rootQuerySucceeded": rootErr == nil, "root": privateRootDiagnosticView(root),
			"statefulSetQuerySucceeded": stsErr == nil, "statefulSet": privateStatefulSetDiagnosticView(sts),
			"podIdentityBound": diagnosticPodIdentityBound(learner, mode, operationID, stsUID),
			"logs":             logCaptures,
		},
		"faultTargetPodUID": func() string {
			if fault == nil {
				return ""
			}
			return fault.Target.PodUID
		}(),
		"faultTargetIP": func() string {
			if fault == nil {
				return ""
			}
			return fault.TargetIP
		}(),
		"learnerIP": nested(learner, "status", "podIP"),
	}
	if writeErr := os.WriteFile(filepath.Join(dir, "online-voter-diagnostics.json"), encoded(private), 0600); writeErr != nil {
		return errors.Join(captureErr, fmt.Errorf("diagnostic snapshot write failed"))
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return errors.Join(captureErr, ctxErr)
	}
	return captureErr
}

func podOwnedByUID(pod any, uid string) bool {
	if uid == "" {
		return false
	}
	for _, owner := range anySlice(nested(pod, "metadata", "ownerReferences")) {
		if nested(owner, "uid") == uid && nested(owner, "kind") == "StatefulSet" {
			return true
		}
	}
	return false
}

func podOwnedByName(pod any, name string) bool {
	if name == "" {
		return false
	}
	for _, owner := range anySlice(nested(pod, "metadata", "ownerReferences")) {
		if nested(owner, "uid") != "" && nested(owner, "kind") == "StatefulSet" && nested(owner, "name") == name {
			return true
		}
	}
	return false
}

func diagnosticPodIdentityBound(pod any, mode, operationID, stsUID string) bool {
	if pod == nil || text(nested(pod, "metadata", "uid")) == "" {
		return false
	}
	if mode == "bootstrap" {
		return podOwnedByUID(pod, stsUID)
	}
	name := text(nested(pod, "metadata", "name"))
	owner := text(nested(pod, "metadata", "annotations", "rhiza.mrchypark.dev/recovery-owner"))
	return operationID != "" && (owner == operationID || name == operationID+"-learner")
}

func captureVoterContainerLogs(ctx context.Context, dir string, pod any) ([]any, error) {
	if pod == nil {
		return []any{}, nil
	}
	name := text(nested(pod, "metadata", "name"))
	uid := text(nested(pod, "metadata", "uid"))
	if name == "" || uid == "" {
		return []any{}, fmt.Errorf("diagnostic pod identity incomplete")
	}
	var records []any
	var captureErr error
	statuses := map[string]any{}
	for _, status := range anySlice(nested(pod, "status", "containerStatuses")) {
		statuses[text(nested(status, "name"))] = status
	}
	for _, container := range anySlice(nested(pod, "spec", "containers")) {
		containerName := text(nested(container, "name"))
		if containerName == "" || strings.ContainsAny(containerName, "/\\\n\x00") {
			continue
		}
		for _, previous := range []bool{false, true} {
			period := "current"
			statusCode, captured, truncated := "capture_failed", false, false
			status := statuses[containerName]
			containerID := text(nested(status, "containerID"))
			restartCount := integer(nested(status, "restartCount"))
			if previous {
				period = "previous"
				if status == nil || integer(nested(status, "restartCount")) == 0 {
					statusCode = "not_available"
					records = append(records, obj{"period": period, "container": containerName, "status": statusCode, "captured": false, "bytes": 0, "truncated": false})
					continue
				}
			}
			currentPod, queryErr := get(ctx, "pod", name)
			if queryErr != nil || text(nested(currentPod, "metadata", "uid")) != uid || !diagnosticContainerIncarnationMatches(currentPod, containerName, containerID, restartCount) {
				statusCode = "identity_changed"
				if queryErr != nil {
					statusCode = diagnosticErrorCode(ctx, queryErr, false)
				}
				captureErr = errors.Join(captureErr, fmt.Errorf("%s log target identity was not confirmed", period))
				records = append(records, obj{"period": period, "container": containerName, "status": statusCode, "captured": false,
					"bytes": 0, "podUID": uid, "truncated": false})
				continue
			}
			args := []string{"logs", name, "-c", containerName, "--tail=100", "--limit-bytes=32768"}
			if previous {
				args = append(args, "--previous")
			}
			data, err := kubectl(ctx, args...)
			if err != nil {
				statusCode = diagnosticErrorCode(ctx, err, previous)
				captureErr = errors.Join(captureErr, fmt.Errorf("%s log capture %s failed", period, statusCode))
			} else {
				latestPod, identityErr := get(ctx, "pod", name)
				if identityErr != nil || text(nested(latestPod, "metadata", "uid")) != uid || !diagnosticContainerIncarnationMatches(latestPod, containerName, containerID, restartCount) {
					statusCode = "identity_changed"
					if identityErr != nil {
						statusCode = diagnosticErrorCode(ctx, identityErr, false)
					}
					captureErr = errors.Join(captureErr, fmt.Errorf("%s log target identity changed during capture", period))
					records = append(records, obj{"period": period, "container": containerName, "status": statusCode, "captured": false,
						"bytes": 0, "podUID": uid, "truncated": false})
					continue
				}
				statusCode, captured = "captured", true
				truncated = len(data) >= 32<<10
				path := filepath.Join(dir, "learner-"+period+"-"+containerName+".log")
				if err := os.WriteFile(path, data, 0600); err != nil {
					statusCode, captured = "write_failed", false
					captureErr = errors.Join(captureErr, fmt.Errorf("%s log evidence write failed", period))
				}
			}
			digest := sha256.Sum256(data)
			records = append(records, obj{"period": period, "container": containerName, "status": statusCode, "captured": captured,
				"bytes": len(data), "sha256": hex.EncodeToString(digest[:]), "truncated": truncated,
				"podUID": uid, "containerID": containerID, "restartCount": restartCount, "containerTerminationReason": nested(status, "state", "terminated", "reason"),
				"containerExitCode":     nested(status, "state", "terminated", "exitCode"),
				"lastTerminationReason": nested(status, "lastState", "terminated", "reason"), "lastExitCode": nested(status, "lastState", "terminated", "exitCode")})
		}
	}
	return records, captureErr
}

func diagnosticContainerIncarnationMatches(pod any, name, containerID string, restartCount int64) bool {
	if containerID == "" {
		return false
	}
	for _, status := range anySlice(nested(pod, "status", "containerStatuses")) {
		if text(nested(status, "name")) == name {
			return text(nested(status, "containerID")) == containerID && integer(nested(status, "restartCount")) == restartCount
		}
	}
	return false
}

func diagnosticErrorCode(ctx context.Context, err error, previous bool) string {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	if errors.Is(ctx.Err(), context.Canceled) || errors.Is(err, context.Canceled) {
		return "canceled"
	}
	lower := strings.ToLower(err.Error())
	if previous && strings.Contains(lower, "previous terminated container") && strings.Contains(lower, "not found") {
		return "previous_missing"
	}
	if strings.Contains(lower, "notfound") || strings.Contains(lower, "not found") {
		return "api_not_found"
	}
	return "command_failed"
}

func inspectLearnerMembership(ctx context.Context, learner, recovery, parent any, out string) obj {
	result := obj{"checked": false, "available": false, "authRejected": false, "nodeMatchesAdd": false, "walMatchesAdd": false,
		"clusterMatchesActive": false, "voting": false, "pending": false}
	if !learnerPodReady(learner) {
		return result
	}
	name := text(nested(learner, "metadata", "name"))
	if name == "" {
		return result
	}
	result["checked"] = true
	checkCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	forward, err := startForward(checkCtx, name, out)
	if err != nil {
		return result
	}
	status, err := readMembership(checkCtx, forward)
	logPath := forward.log.Name()
	forward.close()
	_ = os.Remove(logPath)
	if err != nil {
		unauthorized := strings.Contains(err.Error(), "HTTP 401") || strings.Contains(err.Error(), "HTTP 403")
		result["authRejected"] = unauthorized
		return result
	}
	add := nested(recovery, "status", "membership", "add")
	result["available"] = true
	result["nodeMatchesAdd"] = add != nil && string(status.NodeID) == text(nested(add, "memberID"))
	result["walMatchesAdd"] = add != nil && status.WALIdentity == text(nested(add, "walIdentity"))
	result["clusterMatchesActive"] = text(nested(parent, "status", "activeClusterID")) != "" && status.ClusterID == text(nested(parent, "status", "activeClusterID"))
	result["voting"], result["pending"], result["status"] = status.Voting, status.Pending, status
	return result
}

func learnerPodReady(pod any) bool {
	statuses := anySlice(nested(pod, "status", "containerStatuses"))
	if len(statuses) == 0 {
		return false
	}
	for _, status := range statuses {
		if nested(status, "ready") != true {
			return false
		}
	}
	for _, condition := range anySlice(nested(pod, "status", "conditions")) {
		if nested(condition, "type") == "Ready" {
			return nested(condition, "status") == "True"
		}
	}
	return false
}

func privateLearnerDiagnosticView(pod any) any {
	if pod == nil {
		return nil
	}
	var conditions, statuses, containers []any
	for _, item := range anySlice(nested(pod, "status", "conditions")) {
		conditions = append(conditions, obj{"type": nested(item, "type"), "status": nested(item, "status"), "reason": nested(item, "reason")})
	}
	for _, item := range anySlice(nested(pod, "status", "containerStatuses")) {
		statuses = append(statuses, obj{
			"ready": nested(item, "ready"), "restartCount": nested(item, "restartCount"),
			"state":     obj{"waiting": obj{"reason": nested(item, "state", "waiting", "reason")}, "terminated": obj{"reason": nested(item, "state", "terminated", "reason"), "exitCode": nested(item, "state", "terminated", "exitCode")}},
			"lastState": obj{"terminated": obj{"reason": nested(item, "lastState", "terminated", "reason"), "exitCode": nested(item, "lastState", "terminated", "exitCode")}},
		})
	}
	for _, item := range anySlice(nested(pod, "spec", "containers")) {
		var secretRefs []any
		for _, env := range anySlice(nested(item, "env")) {
			ref := nested(env, "valueFrom", "secretKeyRef")
			if text(nested(ref, "name")) != "" {
				secretRefs = append(secretRefs, obj{"name": nested(ref, "name"), "key": nested(ref, "key")})
			}
		}
		containers = append(containers, obj{"name": nested(item, "name"), "secretRefs": secretRefs})
	}
	return obj{
		"metadata": obj{"name": nested(pod, "metadata", "name"), "uid": nested(pod, "metadata", "uid"), "ownerReferences": nested(pod, "metadata", "ownerReferences"), "annotations": obj{"recoveryOwner": nested(pod, "metadata", "annotations", "rhiza.mrchypark.dev/recovery-owner")}},
		"status":   obj{"phase": nested(pod, "status", "phase"), "podIP": nested(pod, "status", "podIP"), "conditions": conditions, "containerStatuses": statuses},
		"spec":     obj{"containers": containers},
	}
}

func privateRootDiagnosticView(root any) any {
	if root == nil {
		return nil
	}
	return obj{"metadata": obj{"uid": nested(root, "metadata", "uid"), "generation": nested(root, "metadata", "generation")},
		"status": obj{"observedGeneration": nested(root, "status", "observedGeneration"), "phase": nested(root, "status", "phase"), "message": nested(root, "status", "message")}}
}

func privateStatefulSetDiagnosticView(sts any) any {
	if sts == nil {
		return nil
	}
	return obj{"metadata": obj{"uid": nested(sts, "metadata", "uid"), "generation": nested(sts, "metadata", "generation"), "ownerReferences": nested(sts, "metadata", "ownerReferences")},
		"spec":   obj{"template": obj{"spec": obj{"containers": statefulContainerShape(sts)}}},
		"status": obj{"observedGeneration": nested(sts, "status", "observedGeneration"), "replicas": nested(sts, "status", "replicas"), "readyReplicas": nested(sts, "status", "readyReplicas"), "currentReplicas": nested(sts, "status", "currentReplicas"), "updatedReplicas": nested(sts, "status", "updatedReplicas")}}
}

func statefulContainerShape(sts any) []any {
	var result []any
	for _, container := range anySlice(nested(sts, "spec", "template", "spec", "containers")) {
		var ports []any
		for _, port := range anySlice(nested(container, "ports")) {
			ports = append(ports, obj{"name": nested(port, "name"), "containerPort": nested(port, "containerPort"), "protocol": nested(port, "protocol")})
		}
		result = append(result, obj{"name": nested(container, "name"), "ports": ports, "image": nested(container, "image"), "supervisedLaunchShape": supervisedLaunchShape(container)})
	}
	return result
}

func supervisedLaunchShape(container any) string {
	command, args := anySlice(nested(container, "command")), anySlice(nested(container, "args"))
	if len(command) == 0 && len(args) == 0 {
		return "missing"
	}
	if len(command) == 1 && command[0] == "/usr/local/bin/rhiza-fencer" && len(args) == 3 && args[0] == "supervise" && args[1] == "--" && args[2] == "/usr/local/bin/rhiza" {
		return "expected"
	}
	const fixtureGate = `if [ -n "${RHIZA_LEARNER:-}" ]; then while [ ! -f /data/.chaos-start ]; do sleep 0.2; done; fi; exec /usr/local/bin/rhiza-entrypoint`
	if len(command) == 1 && command[0] == "/usr/local/bin/rhiza-fencer" && len(args) == 5 &&
		args[0] == "supervise" && args[1] == "--" && args[2] == "/bin/sh" && args[3] == "-ec" && args[4] == fixtureGate {
		return "expected"
	}
	return "mismatch"
}

func integer(value any) int64 {
	switch number := value.(type) {
	case int:
		return int64(number)
	case int32:
		return int64(number)
	case int64:
		return number
	case float64:
		return int64(number)
	default:
		return 0
	}
}

func privateRecoveryDiagnosticView(recovery any) any {
	if recovery == nil {
		return nil
	}
	return obj{
		"metadata": obj{"name": nested(recovery, "metadata", "name"), "uid": nested(recovery, "metadata", "uid")},
		"spec":     obj{"membership": obj{"replacementPod": nested(recovery, "spec", "membership", "replacementPod"), "replacementSecret": nested(recovery, "spec", "membership", "replacementSecret")}},
		"status":   obj{"phase": nested(recovery, "status", "phase"), "stage": nested(recovery, "status", "stage"), "message": nested(recovery, "status", "message"), "membership": obj{"phase": nested(recovery, "status", "membership", "phase"), "reason": nested(recovery, "status", "membership", "reason"), "add": obj{"memberID": nested(recovery, "status", "membership", "add", "memberID"), "walIdentity": nested(recovery, "status", "membership", "add", "walIdentity")}}},
	}
}

func privateParentDiagnosticView(parent any) any {
	if parent == nil {
		return nil
	}
	return obj{"status": obj{"phase": nested(parent, "status", "phase"), "message": nested(parent, "status", "message"), "activeRecovery": nested(parent, "status", "activeRecovery"), "activeClusterID": nested(parent, "status", "activeClusterID")}}
}

func learnerSecretObservations(ctx context.Context, pod, recovery any, operationID string) ([]any, bool) {
	refs := map[string]map[string]bool{}
	for _, container := range anySlice(nested(pod, "spec", "containers")) {
		for _, env := range anySlice(nested(container, "env")) {
			ref := nested(env, "valueFrom", "secretKeyRef")
			name := text(nested(ref, "name"))
			if name != "" {
				if refs[name] == nil {
					refs[name] = map[string]bool{}
				}
				if key := text(nested(ref, "key")); key != "" {
					refs[name][key] = true
				}
			}
		}
	}
	for _, volume := range anySlice(nested(pod, "spec", "volumes")) {
		secret := nested(volume, "secret")
		name := text(nested(secret, "secretName"))
		if name == "" {
			continue
		}
		if refs[name] == nil {
			refs[name] = map[string]bool{}
		}
		for _, item := range anySlice(nested(secret, "items")) {
			if key := text(nested(item, "key")); key != "" {
				refs[name][key] = true
			}
		}
	}
	if name := text(nested(recovery, "spec", "membership", "replacementSecret")); name != "" {
		if refs[name] == nil {
			refs[name] = map[string]bool{}
		}
	}
	names := make([]string, 0, len(refs))
	for name := range refs {
		names = append(names, name)
	}
	sort.Strings(names)
	truncated := len(names) > 8
	if truncated {
		names = names[:8]
	}
	var observations []any
	for _, name := range names {
		required := refs[name]
		secret, err := get(ctx, "secret", name)
		if err != nil {
			observations = append(observations, obj{"name": name, "found": false, "querySucceeded": isKubeNotFound(err), "keysPresent": false})
			continue
		}
		data, _ := nested(secret, "data").(map[string]any)
		keysPresent := len(data) > 0
		for key := range required {
			if _, ok := data[key]; !ok {
				keysPresent = false
			}
		}
		observations = append(observations, obj{"name": name, "uid": nested(secret, "metadata", "uid"), "found": true,
			"querySucceeded":        true,
			"immutable":             nested(secret, "immutable") == true,
			"ownerMatchesOperation": nested(secret, "metadata", "annotations", "rhiza.mrchypark.dev/recovery-owner") == operationID,
			"keysPresent":           keysPresent,
		})
	}
	return observations, truncated
}

func isKubeNotFound(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "(notfound)") || strings.Contains(message, "not found")
}

func anySlice(value any) []any {
	items, _ := value.([]any)
	return items
}

func eventsForLearnerUID(events []any, learnerUID string) []any {
	if learnerUID == "" {
		return []any{}
	}
	matched := make([]any, 0)
	for _, event := range events {
		if nested(event, "involvedObject", "uid") == learnerUID {
			matched = append(matched, event)
		}
	}
	return matched
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
	if child.Spec.Membership == nil || child.Status.Membership == nil || child.Status.Membership.Add == nil {
		return fmt.Errorf("promoted learner lacks a durable membership addition record")
	}
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
	request := network.MembershipChange{OperationID: add.OperationID, ClusterID: child.Spec.SourceClusterID, ExpectedConfigID: add.ExpectedConfigID, ExpectedAbortSlot: add.ExpectedAbortSlot, Add: &member}
	if member.ID != status.NodeID || member.PublicKey == (quepaxa.PublicKey{}) || fmt.Sprintf("%x", sha256.Sum256(encoded(request))) != add.RequestHash {
		return fmt.Errorf("promoted learner member differs from committed addition")
	}
	return os.WriteFile(filepath.Join(out, "learner-authenticated-binding.json"), encoded(obj{"podUID": nested(pod, "metadata", "uid"), "nodeID": status.NodeID, "walIdentity": status.WALIdentity, "rootUID": bindingUID, "statefulSetUID": stsUID, "credentialUID": add.SecretUID, "member": member, "addition": add}), 0600)
}

type fenceLedgerState struct {
	StatefulSetUID string         `json:"statefulSetUID"`
	Ledger         fencing.Ledger `json:"ledger"`
}

func verifyRegisteredTargets(ctx context.Context, targets []operator.FenceTarget, stsUID, out string, ledgerReader proofLedgerReader) error {
	state, err := ledgerReader(ctx, stsUID, "chaos-source")
	if err != nil {
		return err
	}
	if state.StatefulSetUID != stsUID || state.Ledger.SourceClusterID != "chaos-source" {
		return fmt.Errorf("active source ledger identity differs")
	}
	for _, target := range targets {
		if target.NodeID == "" || target.Pod == "" || target.PodUID == "" || target.WALIdentity == "" {
			return fmt.Errorf("captured active voter identity is incomplete")
		}
		found := false
		for _, run := range state.Ledger.Runs {
			if run.Identity.NodeID == target.NodeID && run.Identity.PodUID == target.PodUID && run.Identity.WALIdentity == target.WALIdentity && run.Stop == nil && !run.Exited {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("promoted current voter lacks active WAL-bound startup registration: %s", target.NodeID)
		}
	}
	return os.WriteFile(filepath.Join(out, "online-active-ledger.json"), encoded(state), 0600)
}

func digest(value string) string {
	value = strings.TrimPrefix(strings.TrimPrefix(value, "docker-pullable://"), "containerd://")
	if _, d, ok := strings.Cut(value, "@"); ok {
		return d
	}
	return value
}

func imageConfigMatches(criConfigID, builtConfigID string) bool {
	return criConfigID != "" && builtConfigID != "" && digest(criConfigID) == digest(builtConfigID)
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

type imagePlatform struct {
	OS           string
	Architecture string
	Variant      string
}

func kindNodePlatform(ctx context.Context) (imagePlatform, error) {
	data, err := command(ctx, "docker", "exec", kindNode, "uname", "-s", "-m")
	if err != nil {
		return imagePlatform{}, fmt.Errorf("read Kind node platform: %w", err)
	}
	fields := strings.Fields(string(data))
	if len(fields) != 2 {
		return imagePlatform{}, fmt.Errorf("invalid Kind node platform")
	}
	platform := imagePlatform{OS: strings.ToLower(fields[0])}
	switch strings.ToLower(fields[1]) {
	case "x86_64", "x64":
		platform.Architecture = "amd64"
	case "aarch64", "arm64":
		platform.Architecture = "arm64"
	case "armv7l":
		platform.Architecture = "arm"
		platform.Variant = "v7"
	case "armv6l":
		platform.Architecture = "arm"
		platform.Variant = "v6"
	case "i386", "i486", "i586", "i686", "x86":
		platform.Architecture = "386"
	case "ppc64le", "s390x", "riscv64":
		platform.Architecture = strings.ToLower(fields[1])
	default:
		return imagePlatform{}, fmt.Errorf("unsupported Kind node architecture %q", fields[1])
	}
	if platform.OS == "" {
		return imagePlatform{}, fmt.Errorf("invalid Kind node operating system")
	}
	return platform, nil
}

// Kind can discard the archive's import repository name while retaining its
// immutable content. Follow only hash-verified descriptors, never a mutable tag.
func imageConfig(ctx context.Context, reference string, platform imagePlatform) (obj, []obj, error) {
	d, err := immutableDigest(reference)
	if err != nil {
		return nil, nil, err
	}
	var chain []obj
	for range 4 {
		data, lookupErr := command(ctx, "docker", "exec", kindNode, "crictl", "inspecti", "-o", "json", d)
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
		content, err := command(ctx, "docker", "exec", kindNode, "ctr", "-n", "k8s.io", "content", "get", d)
		if err != nil {
			return nil, chain, fmt.Errorf("resolve immutable image %s: CRI: %v; content: %w", d, lookupErr, err)
		}
		next, err := imageDescriptor(d, content, platform)
		chain = append(chain, obj{"digest": d, "criLookupFailed": true, "contentSHA256": fmt.Sprintf("%x", sha256.Sum256(content)), "descriptor": publicDescriptor(content)})
		if err != nil {
			return nil, chain, err
		}
		d = next
	}
	return nil, chain, fmt.Errorf("image descriptor chain exceeds qualification bound")
}

func imageDescriptor(d string, content []byte, target imagePlatform) (string, error) {
	if "sha256:"+fmt.Sprintf("%x", sha256.Sum256(content)) != d {
		return "", fmt.Errorf("image content digest differs from %s", d)
	}
	var descriptor struct {
		Config struct {
			Digest string `json:"digest"`
		} `json:"config"`
		Manifests []struct {
			Digest    string `json:"digest"`
			Size      int64  `json:"size"`
			MediaType string `json:"mediaType"`
			Platform  obj    `json:"platform"`
		} `json:"manifests"`
	}
	if err := json.Unmarshal(content, &descriptor); err != nil {
		return "", err
	}
	if descriptor.Config.Digest != "" && len(descriptor.Manifests) == 0 {
		return immutableDigest(descriptor.Config.Digest)
	}
	if descriptor.Config.Digest == "" && len(descriptor.Manifests) > 0 {
		knownMediaType := func(mediaType string) bool {
			return mediaType == "application/vnd.oci.image.manifest.v1+json" || mediaType == "application/vnd.oci.image.index.v1+json" || mediaType == "application/vnd.docker.distribution.manifest.v2+json" || mediaType == "application/vnd.docker.distribution.manifest.list.v2+json"
		}
		for _, manifest := range descriptor.Manifests {
			if _, err := immutableDigest(manifest.Digest); err != nil {
				return "", err
			}
			if len(descriptor.Manifests) > 1 && (manifest.Size <= 0 || !knownMediaType(manifest.MediaType)) {
				return "", fmt.Errorf("ambiguous immutable image index %s", d)
			}
		}
		first := descriptor.Manifests[0]
		aliases := true
		for _, alias := range descriptor.Manifests[1:] {
			if alias.Digest != first.Digest || alias.Size != first.Size || alias.MediaType != first.MediaType || !bytes.Equal(encoded(alias.Platform), encoded(first.Platform)) {
				aliases = false
				break
			}
		}
		platformMatches := func(platform obj) bool {
			platformOS, _ := platform["os"].(string)
			platformArchitecture, _ := platform["architecture"].(string)
			platformVariant, _ := platform["variant"].(string)
			return target.OS != "" && target.Architecture != "" && strings.EqualFold(platformOS, target.OS) && strings.EqualFold(platformArchitecture, target.Architecture) && strings.EqualFold(platformVariant, target.Variant)
		}
		platformUnspecified := func(platform obj) bool {
			if len(platform) == 0 {
				return true
			}
			platformOS, osOK := platform["os"].(string)
			platformArchitecture, architectureOK := platform["architecture"].(string)
			platformVariant, variantOK := platform["variant"].(string)
			if _, hasVariant := platform["variant"]; hasVariant && (!variantOK || platformVariant != "") {
				return false
			}
			return osOK && architectureOK && platformOS == "" && platformArchitecture == ""
		}
		if target.OS == "" || target.Architecture == "" {
			return "", fmt.Errorf("target platform is required to resolve immutable image index %s", d)
		}
		if aliases && (platformUnspecified(first.Platform) || platformMatches(first.Platform)) {
			return immutableDigest(first.Digest)
		}
		var selected string
		for _, manifest := range descriptor.Manifests {
			if platformMatches(manifest.Platform) || len(descriptor.Manifests) == 1 && platformUnspecified(manifest.Platform) {
				if selected != "" {
					return "", fmt.Errorf("ambiguous immutable image index %s for platform %s/%s/%s", d, target.OS, target.Architecture, target.Variant)
				}
				selected = manifest.Digest
			}
		}
		if selected == "" {
			return "", fmt.Errorf("immutable image index %s has no manifest for platform %s/%s/%s", d, target.OS, target.Architecture, target.Variant)
		}
		return immutableDigest(selected)
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
	platform, err := kindNodePlatform(ctx)
	if err != nil {
		return err
	}
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
		cri, chain, resolveErr := imageConfig(ctx, podImage, platform)
		configID := text(nested(cri, "status", "id"))
		builtID := text(built[0]["Id"])
		builtCRI, builtChain, builtResolveErr := imageConfig(ctx, builtID, platform)
		builtConfigID := text(nested(builtCRI, "status", "id"))
		repos, _ := nested(cri, "status", "repoDigests").([]any)
		evidence = append(evidence, obj{"pod": nested(pod, "metadata", "name"), "uid": nested(pod, "metadata", "uid"), "role": role, "podImageID": podImage, "container": publicRuntime(runtime), "resolution": chain, "builtImageID": builtID, "builtResolution": builtChain, "builtConfigID": builtConfigID, "criConfigID": configID, "criRepoDigests": repos})
		if err := os.WriteFile(filepath.Join(out, phase+"-image-evidence.json"), encoded(evidence), 0600); err != nil {
			return err
		}
		if resolveErr != nil {
			return resolveErr
		}
		if builtResolveErr != nil {
			return builtResolveErr
		}
		if !imageConfigMatches(configID, builtConfigID) {
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

func verifyGenerationProofWithReader(ctx context.Context, item any, stsUID, bindingUID string, oldUIDs []string, out string, ledgerReader proofLedgerReader, targets ...operator.FenceTarget) error {
	return verifyScopedProofWithReader(ctx, item, stsUID, bindingUID, oldUIDs, operator.ScopeGeneration, targets, out, ledgerReader)
}

func verifyScopedProof(ctx context.Context, item any, stsUID, bindingUID string, oldUIDs []string, scope operator.Scope, targets []operator.FenceTarget, out string, expectedSource ...string) error {
	return verifyScopedProofWithReader(ctx, item, stsUID, bindingUID, oldUIDs, scope, targets, out, nil, expectedSource...)
}

func verifyScopedProofWithReader(ctx context.Context, item any, stsUID, bindingUID string, oldUIDs []string, scope operator.Scope, targets []operator.FenceTarget, out string, ledgerReader proofLedgerReader, expectedSource ...string) error {
	var request operator.FenceRequest
	var proof operator.FenceProof
	if err := json.Unmarshal(encoded(nested(item, "spec")), &request); err != nil {
		return err
	}
	if err := json.Unmarshal(encoded(nested(item, "status", "proof")), &proof); err != nil {
		return err
	}
	sourceID := "chaos-source"
	if len(expectedSource) > 0 {
		sourceID = expectedSource[0]
	}
	if sourceID == "" || request.SourceClusterID != sourceID || request.StatefulSet != "rhiza" || request.StatefulSetUID != stsUID || request.BindingUID != bindingUID || request.Namespace != namespace || request.LogicalID != "fencing-e2e" || request.OperationID == "" || request.Scope != scope {
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
	if ledgerReader == nil {
		return fmt.Errorf("canonical proof ledger reader is unavailable")
	}
	state, err := ledgerReader(ctx, stsUID, request.SourceClusterID)
	if err != nil {
		return err
	}
	if state.StatefulSetUID != stsUID || state.Ledger.SourceClusterID != request.SourceClusterID {
		return fmt.Errorf("generation ledger identity differs")
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
