package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

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
  namespace:) cat %q ;;
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
  namespace:*) cat %q ;;
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
	state := gcsInitialHeadInstallState{
		Version: 1, DeploymentUID: "original-deployment-uid", ConfigMapUID: "original-configmap-uid", ConfigDigest: "digest",
		NamespaceUID: "namespace-uid", SourceCluster: "source-cluster", Bucket: "qualification-bucket",
		Prefix: "runs/a1b2c3d4/data/source-cluster", ControlPrefix: "runs/a1b2c3d4/control", Container: 0,
		Args: originalArgs, Mounts: originalMounts, Volumes: originalVolumes,
		PatchedArgs: patchedArgs, PatchedMounts: patchedMounts, PatchedVolume: patchedVolumes,
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
	deployment := map[string]any{
		"metadata": map[string]any{"name": "rhiza-fencer", "namespace": namespace, "uid": "original-deployment-uid",
			"labels": map[string]any{"chaos.rhiza.io/run": qualificationRunID}},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
			"containers": []any{map[string]any{"name": "authority", "args": patchedArgs, "volumeMounts": patchedMounts}},
			"volumes":    patchedVolumes,
		}}},
	}
	configMap := map[string]any{"metadata": map[string]any{"namespace": namespace, "uid": "original-configmap-uid",
		"labels": map[string]any{"chaos.rhiza.io/run": qualificationRunID}}}
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
	script := fmt.Sprintf(`#!/bin/sh
set -eu
printf '%%s\n' "$*" >> %q
if [ "$5" = "get" ] && [ "$6" = "deployment" ]; then cat %q; exit 0; fi
if [ "$5" = "get" ] && [ "$6" = "configmap" ]; then cat %q; exit 0; fi
if [ "$5" = "patch" ] && [ "$6" = "deployment" ]; then exit 0; fi
if [ "$5" = "rollout" ] && [ "$6" = "status" ]; then exit 0; fi
if [ "$5" = "delete" ] && [ "$6" = "configmap" ] && [ "$9" = "--preconditions=uid=original-configmap-uid" ]; then : > %q; exit 0; fi
echo "unexpected kubectl command" >&2
exit 90
`, logPath, deploymentPath, configMapPath, deletedPath)
	kubectlPath := filepath.Join(tmp, "kubectl")
	if err := os.WriteFile(kubectlPath, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
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
	configBytes := []byte(`{"version":1,"selector":"initial-archive-head"}`)
	state := gcsInitialHeadInstallState{
		Version: 1, DeploymentUID: "deployment-uid", ConfigDigest: gcsInitialHeadDigest(configBytes),
		NamespaceUID: "namespace-uid", SourceCluster: "source-cluster", Bucket: "qualification-bucket",
		Prefix: "runs/a1b2c3d4/data/source-cluster", ControlPrefix: "runs/a1b2c3d4/control", Container: 0,
		Args: originalArgs, Mounts: originalMounts, Volumes: originalVolumes,
		PatchedArgs: patchedArgs, PatchedMounts: patchedMounts, PatchedVolume: patchedVolumes,
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
	writeDeployment := func(args, mounts, volumes []any) {
		t.Helper()
		deployment := map[string]any{
			"metadata": map[string]any{"name": "rhiza-fencer", "namespace": namespace, "uid": state.DeploymentUID,
				"labels": map[string]any{"chaos.rhiza.io/run": qualificationRunID}},
			"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
				"containers": []any{map[string]any{"name": "authority", "args": args, "volumeMounts": mounts}}, "volumes": volumes,
			}}},
		}
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
	writeDeployment([]any{"serve", "unexpected-arg"}, patchedMounts, patchedVolumes)
	logPath := filepath.Join(tmp, "kubectl.log")
	kubectlPath := filepath.Join(tmp, "kubectl")
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
if [ "$5" = "delete" ] && [ "$6" = "configmap" ]; then : > %q; exit 0; fi
echo "unexpected kubectl operation" >&2
exit 90
`, logPath, deploymentPath, configMapPath, configMapPath, filepath.Join(tmp, "configmap-deleted"))
	if err := os.WriteFile(kubectlPath, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
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

	// Mutable matching contents must not be adopted as cleanup ownership.
	writeDeployment(originalArgs, originalMounts, originalVolumes)
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
	if err := os.Remove(configMapPath); err != nil {
		t.Fatal(err)
	}
	if err := restoreGCSInitialHeadAckLoss(context.Background()); err != nil {
		t.Fatalf("retry with verified original template and absent ConfigMap failed: %v", err)
	}
	if _, err := os.Stat(statePath); !os.IsNotExist(err) {
		t.Fatalf("verified cleanup did not remove its private restore record: %v", err)
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
		IndependentV2: &operator.IndependentEvidenceV2{
			RequestHash: requestDigest, LedgerHash: gcsInitialHeadDigest(encoded([]any{source.Namespace, state})),
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
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	source := gcsRecoverySource{Namespace: namespace, ClusterID: "source", StatefulSet: "rhiza", StatefulSetUID: "sts-uid", BindingUID: "binding-uid", LogicalID: "logical"}
	gotRequest, gotProof, gotRecovery, _, err := waitForGCSInitialHeadRecovery(ctx, source, obj{"metadata": obj{"name": "cluster", "uid": "cluster-uid"}}, nil)
	if err == nil || gotRequest.OperationID != request.OperationID || gotProof.OperationID != "" || gotRecovery.Status.Phase != "" {
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
