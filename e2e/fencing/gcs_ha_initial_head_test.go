package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	internalobjstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
	"google.golang.org/api/option"
)

func TestPrepareInitialHeadFaultBindsFreshNamespaceAndDataScopeBeforeSourceExists(t *testing.T) {
	tmp := t.TempDir()
	previousNamespace, previousContext, previousRun := namespace, kubeContext, qualificationRunID
	namespace, kubeContext, qualificationRunID = "rhiza-ha-run-a1b2c3d4", "ied-test", "a1b2c3d4"
	t.Cleanup(func() { namespace, kubeContext, qualificationRunID = previousNamespace, previousContext, previousRun })
	t.Setenv("RHIZA_GCS_INITIAL_HEAD_ACK_LOSS", "yes")
	t.Setenv("FENCING_E2E_OUT", filepath.Join(tmp, "private"))
	t.Setenv("RHIZA_CLUSTER_ID", "source-cluster")
	t.Setenv("RHIZA_GCS_BUCKET", "qualification-bucket")
	t.Setenv("RHIZA_GCS_PREFIX", "runs/a1b2c3d4")
	if err := os.Mkdir(os.Getenv("FENCING_E2E_OUT"), 0700); err != nil {
		t.Fatal(err)
	}

	config := operator.GCSMutationAckLossConfig{
		Version: 1, Selector: operator.GCSMutationAckLossSelectorInitialArchiveHead,
		Bucket: "qualification-bucket", Prefix: "runs/a1b2c3d4/data/source-cluster",
		Bootstrap: &operator.GCSMutationAckLossBootstrapBinding{NamespaceUID: "namespace-uid-fresh", SourceClusterID: "source-cluster"},
	}
	configBytes, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	configMapResponse := map[string]any{
		"metadata": map[string]any{"name": gcsAckLossConfigMapName, "namespace": namespace, "uid": "configmap-uid-fresh",
			"labels": map[string]any{"chaos.rhiza.io/run": qualificationRunID}},
		"immutable": true, "data": map[string]any{"config.json": string(configBytes)},
	}
	writeJSON := func(name string, value any) string {
		t.Helper()
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(tmp, name)
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	namespaceJSON := writeJSON("namespace.json", map[string]any{"metadata": map[string]any{"name": namespace, "uid": "namespace-uid-fresh"}})
	rhizaConfigJSON := writeJSON("rhiza-config.json", map[string]any{"data": map[string]any{
		"RHIZA_OBJSTORE_BUCKET": "qualification-bucket", "RHIZA_OBJSTORE_PREFIX": "runs/a1b2c3d4",
		"RHIZA_CLUSTER_ID": "source-cluster", "FENCER_AUTHORITY_STATE_PREFIX": "runs/a1b2c3d4/control",
	}})
	deployment := map[string]any{
		"metadata": map[string]any{"name": "rhiza-fencer", "namespace": namespace, "uid": "fencer-deployment-uid",
			"labels": map[string]any{"chaos.rhiza.io/run": qualificationRunID}},
		"spec": map[string]any{"replicas": float64(1), "template": map[string]any{"spec": map[string]any{
			"containers": []any{map[string]any{"name": "authority", "args": []any{"serve"}, "volumeMounts": []any{},
				"envFrom": []any{map[string]any{"configMapRef": map[string]any{"name": "rhiza-config"}}},
				"env": []any{
					map[string]any{"name": "FENCER_AUTHORITY_STATE_PREFIX", "value": "runs/a1b2c3d4/control"},
					map[string]any{"name": "FENCER_INDEPENDENT_MUTATION_PROFILE", "value": "true"},
					map[string]any{"name": "FENCER_SOURCE_WRITER_USE_ADC", "value": "true"},
				}},
			}, "volumes": []any{}}}},
	}
	deploymentJSON := writeJSON("deployment.json", deployment)
	oldPod := initialHeadTestPod("rhiza-fencer-old", "fencer-pod-old", qualificationRunID)
	newPod := initialHeadTestPod("rhiza-fencer-new", "fencer-pod-new", qualificationRunID)
	oldPodsJSON := writeJSON("pods-old.json", map[string]any{"items": []any{oldPod}})
	newPodsJSON := writeJSON("pods-new.json", map[string]any{"items": []any{newPod}})
	oldPodJSON := writeJSON("pod-old.json", oldPod)
	replicaSetJSON := writeJSON("replicaset.json", map[string]any{
		"metadata": map[string]any{"name": "rhiza-fencer-rs", "uid": "fencer-replicaset-uid", "ownerReferences": []any{
			map[string]any{"kind": "Deployment", "name": "rhiza-fencer", "uid": "fencer-deployment-uid", "controller": true},
		}},
	})
	configMapJSON := writeJSON("ack-loss-configmap.json", configMapResponse)
	manifestCopy := filepath.Join(tmp, "created-configmap.json")
	logPath := filepath.Join(tmp, "kubectl.log")
	seenPods := filepath.Join(tmp, "seen-pods")
	script := fmt.Sprintf(`#!/bin/sh
set -eu
printf '%%s\n' "$*" >> %q
if [ "$5" = "create" ] && [ "$6" = "-f" ]; then
  cp "$7" %q
  : > %q
  exit 0
fi
if [ "$5" = "patch" ] || [ "$5" = "rollout" ]; then exit 0; fi
if [ "$5" != "get" ]; then echo "unexpected kubectl operation" >&2; exit 90; fi
kind=$6
name=${7:-}
case "$kind:$name" in
  namespace:rhiza-ha-run-a1b2c3d4) cat %q ;;
  deployment:rhiza-fencer) cat %q ;;
  pods:)
    if [ -f %q ]; then cat %q; else : > %q; cat %q; fi
    ;;
  pod:rhiza-fencer-old) cat %q ;;
  pod:rhiza-fencer-new) cat %q ;;
  replicaset:rhiza-fencer-rs) cat %q ;;
  configmap:rhiza-config) cat %q ;;
  configmap:%s)
    if [ -f %q ]; then cat %q; else echo 'Error from server (NotFound)' >&2; exit 1; fi
    ;;
  statefulset:*|rhizacluster:*) echo "source workload queried before bootstrap" >&2; exit 91 ;;
  *) echo "unexpected kubectl get: $kind $name" >&2; exit 92 ;;
esac
`, logPath, manifestCopy, filepath.Join(tmp, "config-created"), namespaceJSON, deploymentJSON,
		seenPods, newPodsJSON, seenPods, oldPodsJSON, oldPodJSON, newPodsJSON, replicaSetJSON, rhizaConfigJSON,
		gcsAckLossConfigMapName, filepath.Join(tmp, "config-created"), configMapJSON)
	kubectlPath := filepath.Join(tmp, "kubectl")
	if err := os.WriteFile(kubectlPath, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", tmp+string(os.PathListSeparator)+os.Getenv("PATH"))

	if err := prepareGCSInitialHeadAckLoss(context.Background()); err != nil {
		t.Fatalf("pre-apply initial HEAD fault setup failed with only the fencer Pod present: %v", err)
	}

	manifestBytes, err := os.ReadFile(manifestCopy)
	if err != nil {
		t.Fatal("initial-HEAD ConfigMap was not created")
	}
	var manifest map[string]any
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest["kind"] != "ConfigMap" || manifest["immutable"] != true || nested(manifest, "metadata", "namespace") != namespace ||
		nested(manifest, "metadata", "labels", "chaos.rhiza.io/run") != qualificationRunID {
		t.Fatalf("fault config is not immutable and run-owned: %#v", manifest)
	}
	var parsed operator.GCSMutationAckLossConfig
	if err := json.Unmarshal([]byte(text(nested(manifest, "data", "config.json"))), &parsed); err != nil {
		t.Fatal(err)
	}
	if parsed.Selector != operator.GCSMutationAckLossSelectorInitialArchiveHead || parsed.Bucket != "qualification-bucket" ||
		parsed.Prefix != "runs/a1b2c3d4/data/source-cluster" || parsed.Bootstrap == nil ||
		parsed.Bootstrap.NamespaceUID != "namespace-uid-fresh" || parsed.Bootstrap.SourceClusterID != "source-cluster" ||
		parsed.Identity != (fencing.Identity{}) || parsed.Runtime != (fencing.RuntimeBinding{}) {
		t.Fatalf("initial fault config is not bound to the fresh namespace and DATA scope: %+v", parsed)
	}
	if strings.Contains(parsed.Prefix, "/control") || parsed.Prefix == "runs/a1b2c3d4/control" {
		t.Fatal("fault selector must target the source DATA prefix, not the authority CONTROL prefix")
	}
	privateState, err := readGCSInitialHeadInstallState(os.Getenv("FENCING_E2E_OUT"))
	if err != nil || privateState.DeploymentUID != "fencer-deployment-uid" || privateState.ConfigMapUID != "configmap-uid-fresh" {
		t.Fatalf("private restore record lost exact applied resource identities: state=%+v err=%v", privateState, err)
	}
	stateInfo, err := os.Stat(filepath.Join(os.Getenv("FENCING_E2E_OUT"), gcsInitialHeadInstallStateName))
	if err != nil || stateInfo.Mode().Perm() != 0600 {
		t.Fatalf("fencer restore record must remain private: info=%v err=%v", stateInfo, err)
	}
	logBytes, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	log := string(logBytes)
	if strings.Contains(log, "get statefulset") || strings.Contains(log, "get rhizacluster") {
		t.Fatalf("pre-apply arming must not require source workload or healthy membership: %s", log)
	}
	if !strings.Contains(log, "patch deployment rhiza-fencer --type=json") {
		t.Fatalf("pre-apply setup never installed the one-shot fencer config: %s", log)
	}
	if !strings.Contains(log, `"op":"test","path":"/metadata/uid","value":"fencer-deployment-uid"`) {
		t.Fatalf("pre-apply fencer mutation was not guarded by the captured Deployment UID: %s", log)
	}
	if !strings.Contains(log, `"op":"test","path":"/metadata/labels/chaos.rhiza.io~1run","value":"a1b2c3d4"`) {
		t.Fatalf("pre-apply fencer mutation was not guarded by the exact run label: %s", log)
	}
	container := nested(deployment, "spec", "template", "spec", "containers").([]any)[0].(map[string]any)
	for _, entry := range container["env"].([]any) {
		value := entry.(map[string]any)
		if value["name"] == "FENCER_AUTHORITY_STATE_PREFIX" {
			value["value"] = "runs/other/control"
		}
	}
	wrongControl, err := json.Marshal(deployment)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(deploymentJSON, wrongControl, 0600); err != nil {
		t.Fatal(err)
	}
	if err := prepareGCSInitialHeadAckLoss(context.Background()); err == nil || !strings.Contains(err.Error(), "CONTROL prefix") {
		t.Fatalf("pre-apply accepted a fencer configured outside the exact run CONTROL prefix: %v", err)
	}
	afterBytes, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(afterBytes), "create -f") != 1 || strings.Count(string(afterBytes), "patch deployment rhiza-fencer") != 1 {
		t.Fatalf("wrong CONTROL prefix reached a second ConfigMap/Deployment mutation: %s", afterBytes)
	}
}

func TestInitialHeadInstallRejectsStaleNamespaceBindingBeforeKubernetesWrites(t *testing.T) {
	tmp := t.TempDir()
	previousNamespace := namespace
	namespace = "rhiza-ha-run-a1b2c3d4"
	t.Cleanup(func() { namespace = previousNamespace })
	logPath := filepath.Join(tmp, "kubectl.log")
	kubectlPath := filepath.Join(tmp, "kubectl")
	if err := os.WriteFile(kubectlPath, []byte(fmt.Sprintf("#!/bin/sh\nprintf 'called\\n' >> %q\nexit 0\n", logPath)), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", tmp+string(os.PathListSeparator)+os.Getenv("PATH"))
	config := operator.GCSMutationAckLossConfig{
		Version: 1, Selector: operator.GCSMutationAckLossSelectorInitialArchiveHead,
		Bucket: "qualification-bucket", Prefix: "runs/a1b2c3d4/data/source-cluster",
		Bootstrap: &operator.GCSMutationAckLossBootstrapBinding{NamespaceUID: "stale-namespace-uid", SourceClusterID: "source-cluster"},
	}
	source := gcsRecoverySource{Namespace: namespace, NamespaceUID: "current-namespace-uid", ClusterID: "source-cluster",
		Bucket: "qualification-bucket", Prefix: config.Prefix, FencerPod: "run-owned-fencer"}
	if _, _, err := installGCSAckLossConfig(context.Background(), source, config, tmp); err == nil ||
		!strings.Contains(err.Error(), "pre-apply namespace/source binding") {
		t.Fatalf("installer did not reject the stale namespace UID at the binding check: %v", err)
	}
	if _, err := os.Stat(logPath); !os.IsNotExist(err) {
		t.Fatalf("stale namespace binding reached Kubernetes before rejection: %v", err)
	}
}

func TestPrepareInitialHeadFaultRejectsClusterIDEscapingRunDataPrefix(t *testing.T) {
	tmp := t.TempDir()
	previousNamespace, previousContext, previousRun := namespace, kubeContext, qualificationRunID
	namespace, kubeContext, qualificationRunID = "rhiza-ha-run-a1b2c3d4", "ied-test", "a1b2c3d4"
	t.Cleanup(func() { namespace, kubeContext, qualificationRunID = previousNamespace, previousContext, previousRun })
	privateDir := filepath.Join(tmp, "private")
	if err := os.Mkdir(privateDir, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RHIZA_GCS_INITIAL_HEAD_ACK_LOSS", "yes")
	t.Setenv("FENCING_E2E_OUT", privateDir)
	t.Setenv("RHIZA_CLUSTER_ID", "../control")
	t.Setenv("RHIZA_GCS_BUCKET", "qualification-bucket")
	t.Setenv("RHIZA_GCS_PREFIX", "runs/a1b2c3d4")
	namespacePath := filepath.Join(tmp, "namespace.json")
	if err := os.WriteFile(namespacePath, []byte(`{"metadata":{"name":"rhiza-ha-run-a1b2c3d4","uid":"fresh-namespace-uid"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(tmp, "kubectl.log")
	kubectlPath := filepath.Join(tmp, "kubectl")
	script := fmt.Sprintf(`#!/bin/sh
set -eu
printf '%%s\n' "$*" >> %q
if [ "$5" = "get" ] && [ "$6" = "namespace" ]; then cat %q; exit 0; fi
echo "unexpected pre-apply Kubernetes access" >&2
exit 91
`, logPath, namespacePath)
	if err := os.WriteFile(kubectlPath, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", tmp+string(os.PathListSeparator)+os.Getenv("PATH"))
	if err := prepareGCSInitialHeadAckLoss(context.Background()); err == nil {
		t.Fatal("pre-apply accepted a cluster ID that escapes the run DATA prefix")
	}
	logBytes, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(logBytes), "deployment rhiza-fencer") || strings.Contains(string(logBytes), "create -f") || strings.Contains(string(logBytes), "patch deployment") {
		t.Fatalf("invalid source identity reached fencer mutation before rejection: %s", logBytes)
	}
}

func TestInitialHeadFencerRequiresRunDerivedControlPrefix(t *testing.T) {
	tmp := t.TempDir()
	previousNamespace, previousContext := namespace, kubeContext
	namespace, kubeContext = "rhiza-ha-run-a1b2c3d4", "ied-test"
	t.Cleanup(func() { namespace, kubeContext = previousNamespace, previousContext })
	previousRun := qualificationRunID
	qualificationRunID = "a1b2c3d4"
	t.Cleanup(func() { qualificationRunID = previousRun })
	t.Setenv("RHIZA_GCS_INITIAL_HEAD_ACK_LOSS", "yes")
	t.Setenv("FENCING_E2E_OUT", tmp)
	t.Setenv("RHIZA_CLUSTER_ID", "source-cluster")
	t.Setenv("RHIZA_GCS_BUCKET", "qualification-bucket")
	t.Setenv("RHIZA_GCS_PREFIX", "runs/a1b2c3d4")
	writeJSON := func(name string, value any) string {
		t.Helper()
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(tmp, name)
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	namespaceJSON := writeJSON("namespace.json", map[string]any{"metadata": map[string]any{"uid": "namespace-uid"}})
	deploymentJSON := writeJSON("deployment.json", map[string]any{
		"metadata": map[string]any{"namespace": namespace, "uid": "fencer-deployment-uid", "labels": map[string]any{"chaos.rhiza.io/run": qualificationRunID}},
		"spec": map[string]any{"replicas": float64(1), "template": map[string]any{"spec": map[string]any{
			"containers": []any{map[string]any{"name": "authority", "args": []any{"serve"}, "volumeMounts": []any{},
				"envFrom": []any{map[string]any{"configMapRef": map[string]any{"name": "rhiza-config"}}},
				"env": []any{
					map[string]any{"name": "FENCER_AUTHORITY_STATE_PREFIX", "value": "runs/other/control"},
					map[string]any{"name": "FENCER_INDEPENDENT_MUTATION_PROFILE", "value": "true"},
					map[string]any{"name": "FENCER_SOURCE_WRITER_USE_ADC", "value": "true"},
				}},
			}, "volumes": []any{}}}},
	})
	podsJSON := writeJSON("pods.json", map[string]any{"items": []any{initialHeadTestPod("fencer", "fencer-pod-uid", qualificationRunID)}})
	replicaSetJSON := writeJSON("replicaset.json", map[string]any{"metadata": map[string]any{"uid": "fencer-replicaset-uid", "ownerReferences": []any{
		map[string]any{"kind": "Deployment", "name": "rhiza-fencer", "uid": "fencer-deployment-uid", "controller": true},
	}}})
	configMapJSON := writeJSON("rhiza-config.json", map[string]any{"data": map[string]any{
		"RHIZA_OBJSTORE_BUCKET": "qualification-bucket", "RHIZA_OBJSTORE_PREFIX": "runs/a1b2c3d4", "RHIZA_CLUSTER_ID": "source-cluster",
	}})
	logPath := filepath.Join(tmp, "kubectl.log")
	kubectlPath := filepath.Join(tmp, "kubectl")
	script := fmt.Sprintf(`#!/bin/sh
set -eu
printf '%%s\n' "$*" >> %q
if [ "$5" != "get" ]; then echo "unexpected Kubernetes mutation" >&2; exit 91; fi
case "$6:$7" in
  namespace:rhiza-ha-run-a1b2c3d4) cat %q ;;
  deployment:rhiza-fencer) cat %q ;;
  pods:) cat %q ;;
  replicaset:rhiza-fencer-rs) cat %q ;;
  configmap:rhiza-config) cat %q ;;
  *) echo "unexpected Kubernetes read" >&2; exit 92 ;;
esac
`, logPath, namespaceJSON, deploymentJSON, podsJSON, replicaSetJSON, configMapJSON)
	if err := os.WriteFile(kubectlPath, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", tmp+string(os.PathListSeparator)+os.Getenv("PATH"))
	if err := prepareGCSInitialHeadAckLoss(context.Background()); err == nil || !strings.Contains(err.Error(), "CONTROL prefix") {
		t.Fatalf("pre-apply accepted CONTROL state outside this run's derived CONTROL prefix: %v", err)
	}
	logBytes, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(logBytes), "create -f") || strings.Contains(string(logBytes), "patch deployment") {
		t.Fatalf("wrong CONTROL prefix reached ConfigMap or Deployment mutation: %s", logBytes)
	}
}

func TestInitialHeadRestoreRefusesStaleDeploymentAndConfigMapUIDs(t *testing.T) {
	tmp := t.TempDir()
	previousNamespace, previousContext, previousRun := namespace, kubeContext, qualificationRunID
	namespace, kubeContext, qualificationRunID = "rhiza-ha-run-a1b2c3d4", "ied-test", "a1b2c3d4"
	t.Cleanup(func() { namespace, kubeContext, qualificationRunID = previousNamespace, previousContext, previousRun })
	deploymentJSON, err := json.Marshal(map[string]any{
		"metadata": map[string]any{"namespace": namespace, "uid": "replacement-deployment", "labels": map[string]any{"chaos.rhiza.io/run": qualificationRunID}},
	})
	if err != nil {
		t.Fatal(err)
	}
	configMapJSON, err := json.Marshal(map[string]any{
		"metadata": map[string]any{"namespace": namespace, "uid": "replacement-config", "labels": map[string]any{"chaos.rhiza.io/run": qualificationRunID}},
	})
	if err != nil {
		t.Fatal(err)
	}
	deploymentPath, configMapPath := filepath.Join(tmp, "deployment.json"), filepath.Join(tmp, "configmap.json")
	if err := os.WriteFile(deploymentPath, deploymentJSON, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configMapPath, configMapJSON, 0600); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(tmp, "kubectl.log")
	script := fmt.Sprintf(`#!/bin/sh
set -eu
printf '%%s\n' "$*" >> %q
if [ "$5" = "get" ] && [ "$6" = "deployment" ]; then cat %q; exit 0; fi
if [ "$5" = "get" ] && [ "$6" = "configmap" ]; then cat %q; exit 0; fi
echo "unexpected kubectl command" >&2
exit 90
`, logPath, deploymentPath, configMapPath)
	kubectlPath := filepath.Join(tmp, "kubectl")
	if err := os.WriteFile(kubectlPath, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", tmp+string(os.PathListSeparator)+os.Getenv("PATH"))

	if err := restoreAckLossDeployment(context.Background(), "original-deployment", 0, nil, nil, nil, nil, nil, nil); err == nil {
		t.Fatal("cleanup accepted a replacement Deployment UID")
	}
	if err := deleteAckLossConfigMap(context.Background(), "original-configmap"); err == nil {
		t.Fatal("cleanup accepted a replacement ConfigMap UID")
	}
	logBytes, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(logBytes), "patch deployment") || strings.Contains(string(logBytes), "delete configmap") {
		t.Fatalf("stale resource identities must not be patched or deleted: %s", logBytes)
	}
}

func TestInitialHeadCleanupRestoresOnlyCapturedTemplateAndUIDs(t *testing.T) {
	tmp := t.TempDir()
	previousNamespace, previousContext, previousRun := namespace, kubeContext, qualificationRunID
	namespace, kubeContext, qualificationRunID = "rhiza-ha-run-a1b2c3d4", "ied-test", "a1b2c3d4"
	t.Cleanup(func() { namespace, kubeContext, qualificationRunID = previousNamespace, previousContext, previousRun })
	out := filepath.Join(tmp, "private")
	if err := os.Mkdir(out, 0700); err != nil {
		t.Fatal(err)
	}
	originalArgs := []any{"serve"}
	originalMounts := []any{}
	originalVolumes := []any{}
	patchedArgs := []any{"serve", "--gcs-ack-loss-config-file=" + gcsAckLossConfigPath}
	patchedMounts := []any{map[string]any{"name": gcsAckLossVolumeName, "mountPath": "/var/run/rhiza/gcs-ack-loss", "readOnly": true}}
	patchedVolumes := []any{map[string]any{"name": gcsAckLossVolumeName, "configMap": map[string]any{"name": gcsAckLossConfigMapName, "items": []any{map[string]any{"key": "config.json", "path": "config.json"}}}}}
	originalTemplate := initialHeadCleanupPodTemplate(originalArgs, originalMounts, originalVolumes, "metrics:v1")
	patchedTemplate := initialHeadCleanupPodTemplate(patchedArgs, patchedMounts, patchedVolumes, "metrics:v1")
	configBytes := []byte(`{"version":1,"selector":"initial-archive-head"}`)
	state := gcsInitialHeadInstallState{
		Version: 2, DeploymentUID: "original-deployment-uid", ConfigMapUID: "original-configmap-uid", ConfigDigest: gcsInitialHeadDigest(configBytes),
		NamespaceUID: "namespace-uid", SourceCluster: "source-cluster", Bucket: "qualification-bucket",
		Prefix: "runs/a1b2c3d4/data/source-cluster", ControlPrefix: "runs/a1b2c3d4/control", Container: 0,
		Args: originalArgs, Mounts: originalMounts, Volumes: originalVolumes,
		PatchedArgs: patchedArgs, PatchedMounts: patchedMounts, PatchedVolume: patchedVolumes,
		PodTemplate: originalTemplate, PatchedTemplate: patchedTemplate,
	}
	stateBytes, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(out, gcsInitialHeadInstallStateName)
	if err := os.WriteFile(statePath, stateBytes, 0600); err != nil {
		t.Fatal(err)
	}
	deploymentPath, configMapPath := filepath.Join(tmp, "deployment.json"), filepath.Join(tmp, "configmap.json")
	deployment := initialHeadCleanupDeployment(namespace, "original-deployment-uid", qualificationRunID, patchedTemplate)
	configMap := map[string]any{"metadata": map[string]any{"name": gcsAckLossConfigMapName, "namespace": namespace, "uid": "original-configmap-uid", "resourceVersion": "17",
		"labels": map[string]any{"chaos.rhiza.io/run": qualificationRunID}}, "immutable": true,
		"data": map[string]any{"config.json": string(configBytes)}}
	for name, value := range map[string]any{deploymentPath: deployment, configMapPath: configMap} {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(name, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	logPath := filepath.Join(tmp, "kubectl.log")
	deletedPath := filepath.Join(tmp, "configmap-deleted")
	deleteBodyPath := filepath.Join(tmp, "delete-options.jsonl")
	writeInitialHeadCleanupKubectl(t, tmp, logPath, deploymentPath, configMapPath, "original-configmap-uid")
	t.Setenv("RHIZA_TEST_KUBE_PROXY_DELETE_BODY", deleteBodyPath)
	t.Setenv("RHIZA_TEST_KUBE_PROXY_EXPECTED_RV", "17")
	t.Setenv("RHIZA_TEST_KUBE_PROXY_DELETE_MARKER", deletedPath)
	t.Setenv("PATH", tmp+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("FENCING_E2E_OUT", out)

	if err := restoreGCSInitialHeadAckLoss(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(statePath); !os.IsNotExist(err) {
		t.Fatalf("successful cleanup did not remove its private restore state: %v", err)
	}
	if _, err := os.Stat(deletedPath); err != nil {
		t.Fatalf("cleanup did not delete the exact run-owned ConfigMap UID: %v", err)
	}
	logBytes, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	log := string(logBytes)
	if !strings.Contains(log, `"op":"test","path":"/metadata/uid","value":"original-deployment-uid"`) ||
		!strings.Contains(log, `"op":"test","path":"/metadata/labels/chaos.rhiza.io~1run","value":"a1b2c3d4"`) ||
		!strings.Contains(log, `"op":"test","path":"/spec/template","value":`) ||
		!strings.Contains(log, `"op":"replace","path":"/spec/template/spec/containers/0/args","value":["serve"]`) ||
		!strings.Contains(log, `"op":"replace","path":"/spec/template/spec/containers/0/volumeMounts","value":[]`) ||
		!strings.Contains(log, `"op":"replace","path":"/spec/template/spec/volumes","value":[]`) ||
		!strings.Contains(log, `--preconditions=uid=original-configmap-uid`) {
		t.Fatalf("cleanup did not restore the exact saved template under UID preconditions: %s", log)
	}
}

func TestInitialHeadCleanupRetainsRestoreRecordUntilAbsentConfigAndTemplateAreVerified(t *testing.T) {
	tmp := t.TempDir()
	previousNamespace, previousContext, previousRun := namespace, kubeContext, qualificationRunID
	namespace, kubeContext, qualificationRunID = "rhiza-ha-run-a1b2c3d4", "ied-test", "a1b2c3d4"
	t.Cleanup(func() { namespace, kubeContext, qualificationRunID = previousNamespace, previousContext, previousRun })
	out := filepath.Join(tmp, "private")
	if err := os.Mkdir(out, 0700); err != nil {
		t.Fatal(err)
	}
	originalArgs := []any{"serve"}
	originalMounts, originalVolumes := []any{}, []any{}
	patchedArgs := []any{"serve", "--gcs-ack-loss-config-file=" + gcsAckLossConfigPath}
	patchedMounts := []any{map[string]any{"name": gcsAckLossVolumeName, "mountPath": filepath.Dir(gcsAckLossConfigPath), "readOnly": true}}
	patchedVolumes := []any{map[string]any{"name": gcsAckLossVolumeName, "configMap": map[string]any{"name": gcsAckLossConfigMapName, "items": []any{map[string]any{"key": "config.json", "path": "config.json"}}}}}
	originalTemplate := initialHeadCleanupPodTemplate(originalArgs, originalMounts, originalVolumes, "metrics:v1")
	patchedTemplate := initialHeadCleanupPodTemplate(patchedArgs, patchedMounts, patchedVolumes, "metrics:v1")
	configBytes := []byte(`{"version":1,"selector":"initial-archive-head"}`)
	state := gcsInitialHeadInstallState{
		Version: 2, DeploymentUID: "deployment-uid", ConfigMapUID: "matching-run-configmap-uid", ConfigDigest: gcsInitialHeadDigest(configBytes),
		NamespaceUID: "namespace-uid", SourceCluster: "source-cluster", Bucket: "qualification-bucket",
		Prefix: "runs/a1b2c3d4/data/source-cluster", ControlPrefix: "runs/a1b2c3d4/control", Container: 0,
		Args: originalArgs, Mounts: originalMounts, Volumes: originalVolumes,
		PatchedArgs: patchedArgs, PatchedMounts: patchedMounts, PatchedVolume: patchedVolumes,
		PodTemplate: originalTemplate, PatchedTemplate: patchedTemplate,
	}
	stateBytes, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(out, gcsInitialHeadInstallStateName)
	if err := os.WriteFile(statePath, stateBytes, 0600); err != nil {
		t.Fatal(err)
	}
	deploymentPath := filepath.Join(tmp, "deployment.json")
	writeDeployment := func(template obj) {
		t.Helper()
		deployment := initialHeadCleanupDeployment(namespace, state.DeploymentUID, qualificationRunID, template)
		data, err := json.Marshal(deployment)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(deploymentPath, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	configMapPath := filepath.Join(tmp, "configmap.json")
	writeMutableConfigMap := func() {
		t.Helper()
		configMap := map[string]any{
			"metadata": map[string]any{
				"name": gcsAckLossConfigMapName, "namespace": namespace, "uid": "matching-run-configmap-uid",
				"labels": map[string]any{"chaos.rhiza.io/run": qualificationRunID},
			},
			"immutable": false,
			"data":      map[string]any{"config.json": string(configBytes)},
		}
		data, err := json.Marshal(configMap)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(configMapPath, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	// Simulate an interrupted/ambiguous patch with a template matching neither
	// the saved original nor the exact qualification template.
	writeDeployment(initialHeadCleanupPodTemplate([]any{"serve", "unexpected-arg"}, patchedMounts, patchedVolumes, "metrics:v1"))
	logPath := filepath.Join(tmp, "kubectl.log")
	deleteBodyPath := filepath.Join(tmp, "delete-options.jsonl")
	deletedPath := filepath.Join(tmp, "configmap-deleted")
	writeInitialHeadCleanupKubectl(t, tmp, logPath, deploymentPath, configMapPath, state.ConfigMapUID)
	t.Setenv("RHIZA_TEST_KUBE_PROXY_DELETE_BODY", deleteBodyPath)
	t.Setenv("RHIZA_TEST_KUBE_PROXY_EXPECTED_RV", "18")
	t.Setenv("RHIZA_TEST_KUBE_PROXY_DELETE_MARKER", deletedPath)
	t.Setenv("PATH", tmp+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("FENCING_E2E_OUT", out)

	if err := restoreGCSInitialHeadAckLoss(context.Background()); err == nil {
		t.Fatal("cleanup accepted an ambiguous Deployment template when the ConfigMap was already absent")
	}
	if _, err := os.Stat(statePath); err != nil {
		t.Fatalf("ambiguous cleanup discarded its private restore record: %v", err)
	}
	logBytes, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(logBytes), "patch deployment") || strings.Contains(string(logBytes), "delete configmap") {
		t.Fatalf("ambiguous cleanup mutated Kubernetes resources: %s", logBytes)
	}

	// An unrelated sidecar change must not be overwritten by cleanup, even
	// when the fencer's own args, mounts, and volumes still match the patch.
	ownedConfigMap := map[string]any{
		"metadata": map[string]any{
			"name": gcsAckLossConfigMapName, "namespace": namespace, "uid": state.ConfigMapUID,
			"labels": map[string]any{"chaos.rhiza.io/run": qualificationRunID},
		},
		"immutable": true,
		"data":      map[string]any{"config.json": string(configBytes)},
	}
	ownedData, err := json.Marshal(ownedConfigMap)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configMapPath, ownedData, 0600); err != nil {
		t.Fatal(err)
	}
	driftedTemplate := initialHeadCleanupPodTemplate(patchedArgs, patchedMounts, patchedVolumes, "metrics:v2")
	writeDeployment(driftedTemplate)
	if err := restoreGCSInitialHeadAckLoss(context.Background()); err == nil {
		t.Fatal("cleanup accepted unrelated PodTemplate sidecar drift")
	}
	if _, err := os.Stat(statePath); err != nil {
		t.Fatalf("sidecar drift discarded its private restore record: %v", err)
	}
	logBytes, err = os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(logBytes), "patch deployment") || strings.Contains(string(logBytes), "delete configmap") {
		t.Fatalf("sidecar drift caused mutation or ConfigMap deletion: %s", logBytes)
	}
	if _, err := os.Stat(filepath.Join(tmp, "configmap-deleted")); !os.IsNotExist(err) {
		t.Fatalf("sidecar drift deleted the owned ConfigMap: %v", err)
	}

	// Mutable matching contents must not be adopted as cleanup ownership.
	writeDeployment(state.PodTemplate)
	writeMutableConfigMap()
	if err := restoreGCSInitialHeadAckLoss(context.Background()); err == nil {
		t.Fatal("cleanup adopted a mutable same-run ConfigMap with matching contents")
	}
	if _, err := os.Stat(statePath); err != nil {
		t.Fatalf("mutable ConfigMap cleanup discarded its private restore record: %v", err)
	}
	logBytes, err = os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(logBytes), "patch deployment") || strings.Contains(string(logBytes), "delete configmap") {
		t.Fatalf("cleanup deleted a mutable ConfigMap or changed the restored template: %s", logBytes)
	}
	if _, err := os.Stat(filepath.Join(tmp, "configmap-deleted")); !os.IsNotExist(err) {
		t.Fatalf("mutable ConfigMap was deleted despite lacking immutable ownership proof: %v", err)
	}
	writeOwnedConfigMapVersion := func(resourceVersion string) {
		t.Helper()
		configMap := map[string]any{
			"metadata": map[string]any{
				"name": gcsAckLossConfigMapName, "namespace": namespace, "uid": state.ConfigMapUID,
				"resourceVersion": resourceVersion, "labels": map[string]any{"chaos.rhiza.io/run": qualificationRunID},
			},
			"immutable": true,
			"data":      map[string]any{"config.json": string(configBytes)},
		}
		data, err := json.Marshal(configMap)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(configMapPath, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	writeOwnedConfigMapVersion("17")
	if err := restoreGCSInitialHeadAckLoss(context.Background()); err == nil || !strings.Contains(err.Error(), "HTTP 409") {
		t.Fatalf("cleanup did not fail closed on a stale ConfigMap resourceVersion: %v", err)
	}
	if _, err := os.Stat(statePath); err != nil {
		t.Fatalf("stale-version rejection discarded the private restore record: %v", err)
	}
	if _, err := os.Stat(deletedPath); !os.IsNotExist(err) {
		t.Fatalf("stale-version rejection marked ConfigMap deleted: %v", err)
	}

	writeOwnedConfigMapVersion("18")
	if err := restoreGCSInitialHeadAckLoss(context.Background()); err != nil {
		t.Fatalf("retry with freshly read resourceVersion failed: %v", err)
	}
	if _, err := os.Stat(statePath); !os.IsNotExist(err) {
		t.Fatalf("verified cleanup did not remove its private restore state: %v", err)
	}
	if _, err := os.Stat(deletedPath); err != nil {
		t.Fatalf("resourceVersion-bound cleanup did not record ConfigMap deletion: %v", err)
	}
	deleteBodies, err := os.ReadFile(deleteBodyPath)
	if err != nil {
		t.Fatal(err)
	}
	var requests []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(deleteBodies)), "\n") {
		var request map[string]any
		if err := json.Unmarshal([]byte(line), &request); err != nil {
			t.Fatal(err)
		}
		requests = append(requests, request)
	}
	if len(requests) != 2 {
		t.Fatalf("cleanup sent %d delete requests, want stale attempt plus retry: %s", len(requests), deleteBodies)
	}
	for i, wantVersion := range []string{"17", "18"} {
		preconditions := nested(requests[i], "preconditions")
		if requests[i]["apiVersion"] != "v1" || requests[i]["kind"] != "DeleteOptions" ||
			nested(preconditions, "uid") != state.ConfigMapUID || nested(preconditions, "resourceVersion") != wantVersion {
			t.Fatalf("delete request %d lacks exact UID/resourceVersion preconditions: %#v", i, requests[i])
		}
	}
}

func initialHeadCleanupPodTemplate(args, mounts, volumes []any, metricsImage string) obj {
	return obj{
		"metadata": obj{
			"labels":      obj{"app": "rhiza-fencer"},
			"annotations": obj{"fixture.rhiza.io/owner": "initial-head-test"},
		},
		"spec": obj{
			"serviceAccountName": "rhiza-fencer",
			"containers": []any{
				obj{"name": "authority", "image": "rhiza-fencer:test", "args": args, "volumeMounts": mounts},
				obj{"name": "metrics", "image": metricsImage, "args": []any{"serve-metrics"}, "ports": []any{obj{"name": "metrics", "containerPort": 9090}}},
			},
			"volumes":      volumes,
			"nodeSelector": obj{"rhiza.io/role": "control"},
		},
	}
}

func initialHeadCleanupDeployment(namespace, uid, runID string, template obj) obj {
	return obj{
		"metadata": obj{"name": "rhiza-fencer", "namespace": namespace, "uid": uid,
			"labels": obj{"chaos.rhiza.io/run": runID}},
		"spec": obj{"template": template},
	}
}

func writeInitialHeadCleanupKubectl(t *testing.T, dir, logPath, deploymentPath, configMapPath, expectedConfigMapUID string) {
	t.Helper()
	t.Setenv("RHIZA_TEST_KUBE_PROXY_BINARY", os.Args[0])
	t.Setenv("RHIZA_TEST_KUBE_PROXY_NAMESPACE", namespace)
	t.Setenv("RHIZA_TEST_KUBE_PROXY_EXPECTED_UID", expectedConfigMapUID)
	script := fmt.Sprintf(`#!/bin/sh
set -eu
printf '%%s\n' "$*" >> %q
if [ "$5" = "get" ] && [ "$6" = "deployment" ]; then cat %q; exit 0; fi
if [ "$5" = "get" ] && [ "$6" = "configmap" ]; then
  if [ -f %q ]; then cat %q; exit 0; fi
  echo 'Error from server (NotFound)' >&2
  exit 1
fi
if [ "$5" = "rollout" ] && [ "$6" = "status" ]; then exit 0; fi
if [ "$3" = "proxy" ]; then
  export RHIZA_TEST_KUBE_PROXY_HELPER=1
  export RHIZA_TEST_KUBE_PROXY_SOCKET="$5"
  exec "$RHIZA_TEST_KUBE_PROXY_BINARY" -test.run='^TestInitialHeadCleanupKubectlProxyHelper$'
fi
echo "unexpected kubectl operation" >&2
exit 90
`, logPath, deploymentPath, configMapPath, configMapPath)
	if err := os.WriteFile(filepath.Join(dir, "kubectl"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
}

func TestInitialHeadCleanupKubectlProxyHelper(t *testing.T) {
	if os.Getenv("RHIZA_TEST_KUBE_PROXY_HELPER") != "1" {
		return
	}
	listener, err := net.Listen("unix", os.Getenv("RHIZA_TEST_KUBE_PROXY_SOCKET"))
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/version", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	deletePath := "/api/v1/namespaces/" + os.Getenv("RHIZA_TEST_KUBE_PROXY_NAMESPACE") + "/configmaps/" + gcsAckLossConfigMapName
	mux.HandleFunc(deletePath, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read DeleteOptions", http.StatusBadRequest)
			return
		}
		bodyFile, err := os.OpenFile(os.Getenv("RHIZA_TEST_KUBE_PROXY_DELETE_BODY"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			http.Error(w, "record DeleteOptions", http.StatusInternalServerError)
			return
		}
		_, writeErr := bodyFile.Write(append(body, '\n'))
		closeErr := bodyFile.Close()
		if writeErr != nil || closeErr != nil {
			http.Error(w, "record DeleteOptions", http.StatusInternalServerError)
			return
		}
		var options struct {
			APIVersion    string `json:"apiVersion"`
			Kind          string `json:"kind"`
			Preconditions struct {
				UID             string  `json:"uid"`
				ResourceVersion *string `json:"resourceVersion"`
			} `json:"preconditions"`
		}
		if err := json.Unmarshal(body, &options); err != nil || options.APIVersion != "v1" || options.Kind != "DeleteOptions" ||
			options.Preconditions.UID != os.Getenv("RHIZA_TEST_KUBE_PROXY_EXPECTED_UID") || options.Preconditions.ResourceVersion == nil {
			http.Error(w, "missing exact delete preconditions", http.StatusBadRequest)
			return
		}
		if *options.Preconditions.ResourceVersion != os.Getenv("RHIZA_TEST_KUBE_PROXY_EXPECTED_RV") {
			http.Error(w, "stale resourceVersion", http.StatusConflict)
			return
		}
		if marker := os.Getenv("RHIZA_TEST_KUBE_PROXY_DELETE_MARKER"); marker != "" {
			if err := os.WriteFile(marker, []byte("deleted"), 0600); err != nil {
				http.Error(w, "record delete", http.StatusInternalServerError)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	})
	if err := http.Serve(listener, mux); err != nil && !errors.Is(err, http.ErrServerClosed) {
		t.Fatal(err)
	}
}

func TestReadOnlyAuthorityBucketRejectsAllMutationCapabilities(t *testing.T) {
	ctx := context.Background()
	reader := readOnlyAuthorityBucket{}
	if err := reader.Upload(ctx, "control/root.json", strings.NewReader("overwrite")); !errors.Is(err, errGCSAuthorityObserverReadOnly) {
		t.Fatalf("authority observer Upload error = %v, want read-only rejection", err)
	}
	if err := reader.Delete(ctx, "control/root.json"); !errors.Is(err, errGCSAuthorityObserverReadOnly) {
		t.Fatalf("authority observer Delete error = %v, want read-only rejection", err)
	}
	if internalobjstore.SupportsVersionedDelete(reader) {
		t.Fatal("authority observer exposed underlying versioned-delete capability")
	}
	version := objstore.ObjectVersion{Type: objstore.Generation, Value: "123"}
	if err := internalobjstore.DeleteVersion(ctx, reader, "control/root.json", version); !errors.Is(err, internalobjstore.ErrVersionedDeleteUnsupported) {
		t.Fatalf("authority observer DeleteVersion error = %v, want unsupported capability", err)
	}
	if err := internalobjstore.DeleteSelectedVersion(ctx, reader, "control/root.json", &version); !errors.Is(err, errGCSAuthorityObserverReadOnly) {
		t.Fatalf("authority observer selected-version deletion error = %v, want read-only rejection", err)
	}
}

func initialHeadTestPod(name, uid, runID string) map[string]any {
	return map[string]any{
		"metadata": map[string]any{"name": name, "uid": uid, "namespace": "rhiza-ha-run-a1b2c3d4",
			"labels":          map[string]any{"app": "rhiza-fencer", "chaos.rhiza.io/run": runID},
			"ownerReferences": []any{map[string]any{"kind": "ReplicaSet", "name": "rhiza-fencer-rs", "uid": "fencer-replicaset-uid", "controller": true}}},
		"status": map[string]any{"phase": "Running", "conditions": []any{map[string]any{"type": "Ready", "status": "True"}}},
	}
}

func TestVerifyGCSInitialHeadClosedControlRequiresExactRequestAndProof(t *testing.T) {
	source := gcsRecoverySource{Namespace: "run-ns", ClusterID: "source", StatefulSet: "rhiza", StatefulSetUID: "sts-uid", BindingUID: "binding-uid", LogicalID: "logical"}
	request := operator.FenceRequest{
		Namespace: source.Namespace, SourceClusterID: source.ClusterID, StatefulSet: source.StatefulSet,
		StatefulSetUID: source.StatefulSetUID, BindingUID: source.BindingUID, LogicalID: source.LogicalID,
		OperationID: "fence-operation", Scope: operator.ScopeGeneration,
		BootstrapGenesis: &operator.BootstrapGenesisBinding{Version: 1, CandidateDigest: strings.Repeat("a", 64), ClusterName: "cluster", ClusterUID: "cluster-uid"},
	}
	requestDigest := gcsInitialHeadDigest(encoded(request))
	state := obj{"statefulSetUID": source.StatefulSetUID, "ledger": obj{
		"sourceClusterID": source.ClusterID, "requireRuntimeBinding": true, "requireMutationJournal": true,
		"closedBy": request.OperationID, "closedHash": requestDigest,
	}}
	proof := operator.FenceProof{OperationID: request.OperationID, SourceClusterID: source.ClusterID, BindingUID: source.BindingUID,
		RequestHash: requestDigest, ProofID: "initial-head-proof", ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true,
		IndependentV2: &operator.IndependentEvidenceV2{
			Version: 2, RequestHash: requestDigest, LedgerHash: gcsInitialHeadDigest(encoded([]any{source.Namespace, state})),
		}}
	if _, err := verifyGCSInitialHeadClosedControl(state, source, request, proof); err != nil {
		t.Fatalf("exact closed CONTROL root and proof were rejected: %v", err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(any, operator.FenceRequest, operator.FenceProof) (any, operator.FenceRequest, operator.FenceProof)
	}{
		{name: "wrong closed operation", mutate: func(state any, req operator.FenceRequest, proof operator.FenceProof) (any, operator.FenceRequest, operator.FenceProof) {
			clone := obj{"statefulSetUID": source.StatefulSetUID, "ledger": obj{
				"sourceClusterID": source.ClusterID, "requireRuntimeBinding": true, "requireMutationJournal": true,
				"closedBy": "another-operation", "closedHash": requestDigest,
			}}
			return clone, req, proof
		}},
		{name: "wrong IndependentV2 request hash", mutate: func(state any, req operator.FenceRequest, proof operator.FenceProof) (any, operator.FenceRequest, operator.FenceProof) {
			proof.IndependentV2.RequestHash = strings.Repeat("b", 64)
			return state, req, proof
		}},
		{name: "wrong IndependentV2 closed-root hash", mutate: func(state any, req operator.FenceRequest, proof operator.FenceProof) (any, operator.FenceRequest, operator.FenceProof) {
			proof.IndependentV2.LedgerHash = strings.Repeat("c", 64)
			return state, req, proof
		}},
		{name: "different source binding", mutate: func(state any, req operator.FenceRequest, proof operator.FenceProof) (any, operator.FenceRequest, operator.FenceProof) {
			req.BindingUID = "other-binding"
			return state, req, proof
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			badProof := proof
			independent := *proof.IndependentV2
			badProof.IndependentV2 = &independent
			badState, badRequest, badProof := tc.mutate(state, request, badProof)
			if _, err := verifyGCSInitialHeadClosedControl(badState, source, badRequest, badProof); err == nil {
				t.Fatal("mismatched source root or proof was accepted")
			}
		})
	}
}

func TestIsInitialHeadMutationRequiresCanonicalCreateOnlyBytes(t *testing.T) {
	head, err := recovery.EmptyArchiveHeadBytes()
	if err != nil {
		t.Fatal(err)
	}
	valid := fencing.MutationIntent{Kind: "upload", Key: "archive/head.bin", Condition: fencing.MutationCondition{Mode: fencing.MutationCreateOnly},
		PayloadSize: int64(len(head)), PayloadDigest: gcsInitialHeadDigest(head)}
	if !isInitialHeadMutation(valid) {
		t.Fatal("canonical create-only empty HEAD intent was rejected")
	}
	for _, mutate := range []func(*fencing.MutationIntent){
		func(i *fencing.MutationIntent) { i.Key = "archive/other.bin" },
		func(i *fencing.MutationIntent) { i.Kind = "delete" },
		func(i *fencing.MutationIntent) { i.Condition.Mode = fencing.MutationUnconditional },
		func(i *fencing.MutationIntent) { i.PayloadDigest = strings.Repeat("f", 64) },
		func(i *fencing.MutationIntent) { i.PayloadSize++ },
	} {
		changed := valid
		mutate(&changed)
		if isInitialHeadMutation(changed) {
			t.Fatalf("noncanonical or non-create-only genesis intent was accepted: %+v", changed)
		}
	}
}

func TestVerifyGCSInitialHeadObjectRequiresExactHeldGenerationAndCanonicalBytes(t *testing.T) {
	want, err := recovery.EmptyArchiveHeadBytes()
	if err != nil {
		t.Fatal(err)
	}
	digest := gcsInitialHeadDigest(want)
	for _, tc := range []struct {
		name           string
		generation     string
		metageneration string
		held           bool
		operationID    string
		payloadDigest  string
		media          []byte
		wantError      bool
	}{
		{name: "exact canonical held generation", generation: "41", metageneration: "3", held: true, operationID: "head-operation", payloadDigest: digest, media: want},
		{name: "wrong generation", generation: "42", metageneration: "3", held: true, operationID: "head-operation", payloadDigest: digest, media: want, wantError: true},
		{name: "wrong metageneration", generation: "41", metageneration: "4", held: true, operationID: "head-operation", payloadDigest: digest, media: want, wantError: true},
		{name: "not held", generation: "41", metageneration: "3", held: false, operationID: "head-operation", payloadDigest: digest, media: want, wantError: true},
		{name: "wrong operation", generation: "41", metageneration: "3", held: true, operationID: "other-operation", payloadDigest: digest, media: want, wantError: true},
		{name: "wrong payload digest", generation: "41", metageneration: "3", held: true, operationID: "head-operation", payloadDigest: strings.Repeat("f", 64), media: want, wantError: true},
		{name: "wrong bytes with matching size", generation: "41", metageneration: "3", held: true, operationID: "head-operation", payloadDigest: digest, media: append([]byte(nil), want...), wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			media := tc.media
			if tc.name == "wrong bytes with matching size" {
				media = append([]byte(nil), media...)
				media[len(media)-1] ^= 1
			}
			var mediaReads, metadataReads int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Query().Get("generation") != "41" {
					http.Error(w, "unexpected generation query", http.StatusBadRequest)
					return
				}
				if r.URL.Path == "/head-bucket/run/data/source/archive/head.bin" {
					mediaReads++
					_, _ = w.Write(media)
					return
				}
				if !strings.Contains(r.URL.Path, "/b/head-bucket/o/") || !strings.HasSuffix(r.URL.Path, "run/data/source/archive/head.bin") {
					http.Error(w, "unexpected GCS object request", http.StatusBadRequest)
					return
				}
				metadataReads++
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"kind": "storage#object", "bucket": "head-bucket", "name": "run/data/source/archive/head.bin",
					"generation": tc.generation, "metageneration": tc.metageneration, "size": strconv.Itoa(len(want)),
					"temporaryHold": tc.held,
					"metadata":      map[string]string{"rhiza-operation-id": tc.operationID, "rhiza-payload-sha256": tc.payloadDigest},
				})
			}))
			defer server.Close()
			client, err := storage.NewClient(context.Background(), option.WithEndpoint(server.URL+"/storage/v1/"),
				option.WithHTTPClient(server.Client()), option.WithoutAuthentication())
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			got, err := verifyGCSInitialHeadObject(context.Background(), client, "head-bucket", "run/data/source", 41, 3, "head-operation")
			if tc.wantError {
				if err == nil {
					t.Fatalf("invalid initial HEAD generation was accepted: %+v", got)
				}
				return
			}
			if err != nil || got.Generation != 41 || got.Metageneration != 3 || got.Size != int64(len(want)) || got.PayloadSHA256 != digest || !got.TemporaryHold {
				t.Fatalf("exact initial HEAD generation was not verified: evidence=%+v err=%v", got, err)
			}
			if mediaReads != 1 || metadataReads != 2 {
				t.Fatalf("expected one exact media read and two versioned metadata reads, got media=%d metadata=%d", mediaReads, metadataReads)
			}
		})
	}
}

func TestWaitForGCSInitialHeadRecoveryDoesNotAcceptPendingFence(t *testing.T) {
	previousNamespace, previousContext, previousRun := namespace, kubeContext, qualificationRunID
	namespace, kubeContext, qualificationRunID = "rhiza-ha-run", "ied-test", "run-a1b2"
	t.Cleanup(func() { namespace, kubeContext, qualificationRunID = previousNamespace, previousContext, previousRun })
	tmp := t.TempDir()
	request := operator.FenceRequest{Namespace: namespace, SourceClusterID: "source", StatefulSet: "rhiza", StatefulSetUID: "sts-uid",
		BindingUID: "binding-uid", LogicalID: "logical", OperationID: "fence-operation", Scope: operator.ScopeGeneration,
		Targets:          []operator.FenceTarget{{NodeID: "voter-0", Pod: "rhiza-voter-0", PodUID: "pod-uid"}},
		BootstrapGenesis: &operator.BootstrapGenesisBinding{Version: 1, CandidateDigest: strings.Repeat("a", 64), ClusterName: "cluster", ClusterUID: "cluster-uid"}}
	fence := map[string]any{"metadata": map[string]any{"namespace": namespace, "uid": "fence-uid", "labels": map[string]any{"chaos.rhiza.io/run": qualificationRunID}},
		"spec": request}
	fenceList, err := json.Marshal(map[string]any{"items": []any{fence}})
	if err != nil {
		t.Fatal(err)
	}
	fencePath := filepath.Join(tmp, "fences.json")
	if err := os.WriteFile(fencePath, fenceList, 0600); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(tmp, "kubectl.log")
	kubectlPath := filepath.Join(tmp, "kubectl")
	script := fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' \"$*\" >> %q\nif [ \"$5\" = get ] && [ \"$6\" = rhizafences ]; then cat %q; exit 0; fi\nexit 91\n", logPath, fencePath)
	if err := os.WriteFile(kubectlPath, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", tmp+string(os.PathListSeparator)+os.Getenv("PATH"))
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	source := gcsRecoverySource{Namespace: namespace, ClusterID: "source", StatefulSet: "rhiza", StatefulSetUID: "sts-uid", BindingUID: "binding-uid", LogicalID: "logical"}
	gotRequest, gotProof, gotRecovery, _, err := waitForGCSInitialHeadRecovery(ctx, source, obj{"metadata": obj{"name": "cluster", "uid": "cluster-uid"}}, nil)
	if !errors.Is(err, context.DeadlineExceeded) || gotRequest.OperationID != request.OperationID || gotProof.OperationID != "" || gotRecovery.Status.Phase != "" {
		t.Fatalf("pending fence was treated as completed recovery: request=%+v proof=%+v recovery=%+v err=%v", gotRequest, gotProof, gotRecovery, err)
	}
	logBytes, err := os.ReadFile(logPath)
	if err != nil || !strings.Contains(string(logBytes), "get rhizafences") || strings.Contains(string(logBytes), "get rhizarecoveries") {
		t.Fatalf("pending fence advanced past canonical proof/status gating: log=%s err=%v", logBytes, err)
	}
}

func TestVerifyGCSInitialHeadTargetGenesisRequiresCanonicalClaims(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	t.Cleanup(func() { _ = bucket.Close() })
	store, err := operator.NewAuthorityStateStore(bucket, "control")
	if err != nil {
		t.Fatal(err)
	}
	source := gcsRecoverySource{Bucket: "head-bucket", Prefix: "run/data/source", ClusterID: "source", StatefulSetUID: "sts-uid"}
	request := operator.FenceRequest{OperationID: "fence-operation"}
	resource := operator.Resource{Spec: operator.Spec{RecoveryID: "recovery-operation"}}
	if _, err := verifyGCSInitialHeadTargetGenesis(ctx, store, nil, source, "run/data/target", "target", request,
		operator.FenceProof{}, resource, "fence-uid", "", gcsAckLossObjectEvidence{}); err == nil {
		t.Fatal("target genesis without a canonical source/target claim and authorization was accepted")
	}
}

func TestVerifyGCSInitialHeadTargetReadBarrierRejectsIncompleteRecovery(t *testing.T) {
	for _, resource := range []operator.Resource{
		{},
		{Status: operator.Status{SecretName: "target-admin"}},
		{Status: operator.Status{TargetIdentities: []operator.FenceTarget{{Pod: "target-0", PodUID: "pod-uid"}}}},
	} {
		if _, _, err := verifyGCSInitialHeadTargetReadBarrier(context.Background(), resource, gcsRecoverySource{}, t.TempDir()); err == nil {
			t.Fatalf("incomplete target recovery status was accepted: %+v", resource.Status)
		}
	}
}
