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
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

const gcsInitialHeadInstallStateName = "gcs-initial-head-install-state.json"

type gcsInitialHeadInstallState struct {
	Version       int    `json:"version"`
	DeploymentUID string `json:"deploymentUID"`
	ConfigMapUID  string `json:"configMapUID"`
	ConfigDigest  string `json:"configDigest"`
	FencerPod     string `json:"fencerPod"`
	FencerPodUID  string `json:"fencerPodUID"`
	NamespaceUID  string `json:"namespaceUID"`
	SourceCluster string `json:"sourceClusterID"`
	Bucket        string `json:"bucket"`
	Prefix        string `json:"prefix"`
	ControlPrefix string `json:"controlPrefix"`
	Container     int    `json:"container"`
	Args          []any  `json:"args"`
	Mounts        []any  `json:"mounts"`
	Volumes       []any  `json:"volumes"`
	PatchedArgs   []any  `json:"patchedArgs"`
	PatchedMounts []any  `json:"patchedMounts"`
	PatchedVolume []any  `json:"patchedVolumes"`
}

// prepareGCSInitialHeadAckLoss arms the fencer while the StatefulSet is not
// yet created. The immutable ConfigMap contains no credentials. Exact runtime,
// producer, and genesis tuple selection happens later from canonical CONTROL.
func prepareGCSInitialHeadAckLoss(ctx context.Context) (resultErr error) {
	if os.Getenv("RHIZA_GCS_INITIAL_HEAD_ACK_LOSS") != "yes" {
		return fmt.Errorf("initial archive HEAD ACK-loss qualification is opt-in; set RHIZA_GCS_INITIAL_HEAD_ACK_LOSS=yes")
	}
	out := strings.TrimSpace(os.Getenv("FENCING_E2E_OUT"))
	if out == "" {
		return fmt.Errorf("initial archive HEAD ACK-loss qualification requires private FENCING_E2E_OUT")
	}
	namespaceObject, err := get(ctx, "namespace", namespace)
	if err != nil {
		return fmt.Errorf("read initial-HEAD qualification namespace: %w", err)
	}
	namespaceUID := text(nested(namespaceObject, "metadata", "uid"))
	clusterID := strings.TrimSpace(os.Getenv("RHIZA_CLUSTER_ID"))
	bucket := strings.TrimSpace(os.Getenv("RHIZA_GCS_BUCKET"))
	parentPrefix := strings.TrimSpace(os.Getenv("RHIZA_GCS_PREFIX"))
	if namespaceUID == "" || clusterID == "" || !safeGCSPathComponent(clusterID) || bucket == "" || parentPrefix == "" ||
		strings.Trim(parentPrefix, "/") != parentPrefix || path.Clean(parentPrefix) != parentPrefix || strings.Contains(parentPrefix, "\\") {
		return fmt.Errorf("initial archive HEAD ACK-loss qualification source scope is incomplete")
	}
	basePrefix := path.Join(parentPrefix, "data")
	sourcePrefix := path.Join(basePrefix, clusterID)
	if sourcePrefix != path.Join(parentPrefix, "data", clusterID) || !strings.HasPrefix(sourcePrefix, basePrefix+"/") {
		return fmt.Errorf("initial archive HEAD ACK-loss source prefix is invalid")
	}
	controlPrefix := path.Join(parentPrefix, "control")
	config := operator.GCSMutationAckLossConfig{
		Version: 1, Selector: operator.GCSMutationAckLossSelectorInitialArchiveHead,
		Bucket: bucket, Prefix: sourcePrefix,
		Bootstrap: &operator.GCSMutationAckLossBootstrapBinding{NamespaceUID: namespaceUID, SourceClusterID: clusterID},
	}
	deployment, err := get(ctx, "deployment", "rhiza-fencer")
	if err != nil {
		return err
	}
	deploymentUID := text(nested(deployment, "metadata", "uid"))
	if deploymentUID == "" || text(nested(deployment, "metadata", "namespace")) != namespace ||
		text(nested(deployment, "metadata", "labels", "chaos.rhiza.io/run")) != qualificationRunID || nested(deployment, "spec", "replicas") != float64(1) {
		return fmt.Errorf("refusing to arm a fencer Deployment outside the fresh qualification run")
	}
	pods, err := get(ctx, "pods", "")
	if err != nil {
		return err
	}
	items, ok := pods["items"].([]any)
	if !ok {
		return fmt.Errorf("fencer Pod inventory is malformed")
	}
	fencerPod := ""
	for _, pod := range items {
		if text(nested(pod, "metadata", "labels", "app")) != "rhiza-fencer" ||
			text(nested(pod, "metadata", "labels", "chaos.rhiza.io/run")) != qualificationRunID || !ackLossPodOwnedBy(ctx, pod, deploymentUID) {
			continue
		}
		if fencerPod != "" {
			return fmt.Errorf("run-owned fencer Pod is ambiguous before bootstrap")
		}
		fencerPod = text(nested(pod, "metadata", "name"))
	}
	if fencerPod == "" {
		return fmt.Errorf("run-owned fencer Pod is unavailable before bootstrap")
	}
	source := gcsRecoverySource{Namespace: namespace, NamespaceUID: namespaceUID, ConfigMap: "rhiza-config", ClusterID: clusterID,
		Bucket: bucket, Prefix: sourcePrefix, FencerPod: fencerPod}
	if err := validateAckLossFencerStorageConfig(ctx, deployment, source); err != nil {
		return err
	}
	container, err := boundContainer(deployment, "authority")
	if err != nil {
		return err
	}
	if configured, err := effectiveGCSSetting(ctx, container, "FENCER_AUTHORITY_STATE_PREFIX"); err != nil || configured != controlPrefix {
		return fmt.Errorf("fencer CONTROL prefix does not exactly match the fresh run scope")
	}
	containerIndex, args, mounts, volumes, err := ackLossDeploymentTemplate(deployment)
	if err != nil {
		return err
	}
	patchedArgs := append(append([]any(nil), args...), "--gcs-ack-loss-config-file="+gcsAckLossConfigPath)
	patchedMounts := append(append([]any(nil), mounts...), obj{"name": gcsAckLossVolumeName, "mountPath": "/var/run/rhiza/gcs-ack-loss", "readOnly": true})
	patchedVolumes := append(append([]any(nil), volumes...), obj{"name": gcsAckLossVolumeName, "configMap": obj{"name": gcsAckLossConfigMapName, "items": []any{obj{"key": "config.json", "path": "config.json"}}}})
	configBytes, err := json.Marshal(config)
	if err != nil {
		return err
	}
	state := gcsInitialHeadInstallState{Version: 1, DeploymentUID: deploymentUID, ConfigDigest: gcsInitialHeadDigest(configBytes),
		NamespaceUID: namespaceUID, SourceCluster: clusterID, Bucket: bucket, Prefix: sourcePrefix, ControlPrefix: controlPrefix,
		Container: containerIndex, Args: args, Mounts: mounts, Volumes: volumes,
		PatchedArgs: patchedArgs, PatchedMounts: patchedMounts, PatchedVolume: patchedVolumes}
	statePath := path.Join(out, gcsInitialHeadInstallStateName)
	encodedState, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err := writeNewPrivateAckLossFile(statePath, encodedState); err != nil {
		return fmt.Errorf("persist private fencer restore state before mutation: %w", err)
	}
	fencerPod, cleanup, err := installGCSAckLossConfig(ctx, source, config, out)
	if err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
		defer cancel()
		return errors.Join(err, restoreGCSInitialHeadAckLoss(cleanupCtx))
	}
	keepArmed := false
	defer func() {
		if keepArmed {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
		defer cancel()
		cleanupErr := cleanup(cleanupCtx)
		resultErr = errors.Join(resultErr, cleanupErr)
		if cleanupErr == nil {
			resultErr = errors.Join(resultErr, restoreGCSInitialHeadAckLoss(cleanupCtx))
		}
	}()
	configMap, err := get(ctx, "configmap", gcsAckLossConfigMapName)
	if err != nil {
		return fmt.Errorf("read installed initial-HEAD fault ConfigMap: %w", err)
	}
	state.ConfigMapUID = text(nested(configMap, "metadata", "uid"))
	state.FencerPod = fencerPod
	currentFencerPod, err := get(ctx, "pod", fencerPod)
	if err != nil {
		return fmt.Errorf("read rolled-out initial-HEAD fencer Pod: %w", err)
	}
	state.FencerPodUID = text(nested(currentFencerPod, "metadata", "uid"))
	if state.ConfigMapUID == "" || state.FencerPodUID == "" || text(nested(configMap, "metadata", "labels", "chaos.rhiza.io/run")) != qualificationRunID {
		return fmt.Errorf("initial-HEAD fault ConfigMap has no immutable UID")
	}
	encodedState, err = json.Marshal(state)
	if err == nil {
		err = replacePrivateAckLossFile(statePath, encodedState)
	}
	if err != nil {
		return fmt.Errorf("persist private initial-HEAD fencer restore state: %w", err)
	}
	keepArmed = true
	return nil
}

func readGCSInitialHeadInstallState(out string) (gcsInitialHeadInstallState, error) {
	var state gcsInitialHeadInstallState
	name := path.Join(out, gcsInitialHeadInstallStateName)
	info, err := os.Lstat(name)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return state, fmt.Errorf("private initial-HEAD fencer restore state is missing or unsafe")
	}
	data, err := os.ReadFile(name)
	if err != nil {
		return state, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&state); err != nil || decoder.Decode(new(any)) != io.EOF || state.Version != 1 || state.DeploymentUID == "" || state.ConfigDigest == "" || state.NamespaceUID == "" || state.SourceCluster == "" || state.Bucket == "" || state.Prefix == "" || state.ControlPrefix == "" {
		return gcsInitialHeadInstallState{}, fmt.Errorf("private initial-HEAD fencer restore state is malformed")
	}
	return state, nil
}

func restoreGCSInitialHeadAckLoss(ctx context.Context) error {
	out := strings.TrimSpace(os.Getenv("FENCING_E2E_OUT"))
	if out == "" {
		return fmt.Errorf("initial archive HEAD ACK-loss cleanup requires private FENCING_E2E_OUT")
	}
	state, err := readGCSInitialHeadInstallState(out)
	if err != nil {
		return err
	}
	deployment, err := get(ctx, "deployment", "rhiza-fencer")
	if err != nil || text(nested(deployment, "metadata", "uid")) != state.DeploymentUID || text(nested(deployment, "metadata", "labels", "chaos.rhiza.io/run")) != qualificationRunID {
		return fmt.Errorf("initial-HEAD fencer Deployment identity changed before cleanup")
	}
	currentIndex, currentArgs, currentMounts, currentVolumes, templateErr := ackLossDeploymentTemplate(deployment)
	if templateErr != nil || currentIndex != state.Container {
		return fmt.Errorf("read current initial-HEAD fencer template: %w", templateErr)
	}
	switch {
	case sameJSON(currentArgs, state.PatchedArgs) && sameJSON(currentMounts, state.PatchedMounts) && sameJSON(currentVolumes, state.PatchedVolume):
		if err := restoreAckLossDeployment(ctx, state.DeploymentUID, state.Container, state.Args, state.Mounts, state.Volumes,
			state.PatchedArgs, state.PatchedMounts, state.PatchedVolume); err != nil {
			return fmt.Errorf("restore pre-bootstrap fencer template: %w", err)
		}
	case sameJSON(currentArgs, state.Args) && sameJSON(currentMounts, state.Mounts) && sameJSON(currentVolumes, state.Volumes):
		// Crash before the patch left the original template in place.
	default:
		return fmt.Errorf("fencer template differs from both the saved original and qualification versions")
	}
	if _, err := kubectl(ctx, "rollout", "status", "deployment/rhiza-fencer", "--timeout=120s"); err != nil {
		return fmt.Errorf("wait for original fencer after bootstrap fault cleanup: %w", err)
	}
	configUID := state.ConfigMapUID
	if configUID == "" {
		configMap, err := get(ctx, "configmap", gcsAckLossConfigMapName)
		if isAckLossNotFound(err) {
			// The original Deployment template and rollout were verified above;
			// an absent ConfigMap is the expected outcome of a completed partial rollback.
			configUID = ""
		} else if err != nil || text(nested(configMap, "metadata", "namespace")) != namespace || text(nested(configMap, "metadata", "labels", "chaos.rhiza.io/run")) != qualificationRunID ||
			gcsInitialHeadDigest([]byte(text(nested(configMap, "data", "config.json")))) != state.ConfigDigest {
			return fmt.Errorf("cannot safely identify an interrupted initial-HEAD ConfigMap")
		} else {
			configUID = text(nested(configMap, "metadata", "uid"))
			if configUID == "" {
				return fmt.Errorf("cannot safely identify an interrupted initial-HEAD ConfigMap UID")
			}
		}
	}
	if configUID != "" {
		if err := deleteAckLossConfigMapExact(ctx, configUID, state.ConfigDigest); err != nil {
			return err
		}
	}
	if err := os.Remove(path.Join(out, gcsInitialHeadInstallStateName)); err != nil {
		return fmt.Errorf("remove private initial-HEAD restore record: %w", err)
	}
	return nil
}

func safeGCSPathComponent(value string) bool {
	return value != "" && value != "." && value != ".." && path.Base(value) == value && !strings.ContainsAny(value, `/\\`)
}

func gcsInitialHeadDigest(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func replacePrivateAckLossFile(name string, data []byte) error {
	info, err := os.Lstat(name)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("private initial-HEAD state is missing or unsafe")
	}
	dir := filepath.Dir(name)
	temporary, err := os.CreateTemp(dir, ".gcs-initial-head-state-*")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryName, name)
}

type gcsInitialHeadPrivateEvidence struct {
	Version              int                                `json:"version"`
	Source               obj                                `json:"source"`
	FaultReceipt         operator.GCSMutationAckLossReceipt `json:"faultReceipt"`
	CanonicalObservation gcsHAAckObservation                `json:"canonicalObservation"`
	SourceHead           gcsAckLossObjectEvidence           `json:"sourceHead"`
	Fence                operator.FenceRequest              `json:"fence"`
	Proof                operator.FenceProof                `json:"proof"`
	Recovery             operator.Resource                  `json:"recovery"`
	TargetHead           gcsAckLossObjectEvidence           `json:"targetHead"`
	TargetAck            gcsAcknowledgement                 `json:"targetAck"`
	TargetReadback       string                             `json:"targetReadback"`
}

// qualifyGCSInitialHeadAckLoss collects the startup-only outcome after the
// pre-apply fault was armed. It never asks a source voter for healthy
// membership: source identity and closure come from canonical CONTROL state.
func qualifyGCSInitialHeadAckLoss(ctx context.Context) (resultErr error) {
	out := strings.TrimSpace(os.Getenv("FENCING_E2E_OUT"))
	if out == "" {
		return fmt.Errorf("initial-HEAD collector requires private FENCING_E2E_OUT")
	}
	if err := os.MkdirAll(out, 0700); err != nil {
		return err
	}
	if info, err := os.Lstat(out); err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("initial-HEAD evidence directory must be private")
	}
	state, err := readGCSInitialHeadInstallState(out)
	if err != nil {
		return err
	}
	if state.ConfigMapUID == "" || state.FencerPod == "" || state.FencerPodUID == "" {
		return fmt.Errorf("pre-apply fencer arming did not finish with immutable rollout identities")
	}
	ns, err := get(ctx, "namespace", namespace)
	if err != nil || text(nested(ns, "metadata", "uid")) != state.NamespaceUID || text(nested(ns, "metadata", "labels", "chaos.rhiza.io/run")) != qualificationRunID {
		return fmt.Errorf("initial-HEAD run namespace identity changed after pre-arm")
	}
	cluster, err := captureGCSInitialHeadCluster(ctx, state)
	if err != nil {
		return err
	}
	stsName := text(nested(cluster, "spec", "statefulSet"))
	logicalID := text(nested(cluster, "spec", "logicalID"))
	container := text(nested(cluster, "spec", "container"))
	if stsName == "" || logicalID == "" || container == "" {
		return fmt.Errorf("bootstrap RhizaCluster lacks a complete workload identity")
	}
	sts, err := get(ctx, "statefulset", stsName)
	if err != nil {
		return fmt.Errorf("read initial-HEAD StatefulSet identity: %w", err)
	}
	stsUID := text(nested(sts, "metadata", "uid"))
	if stsUID == "" || !gcsWorkloadHasNoPVC(sts) {
		return fmt.Errorf("initial-HEAD source StatefulSet identity or no-PVC contract is invalid")
	}
	app, err := boundContainer(sts, container)
	if err != nil {
		return err
	}
	workloadConfig, _, err := sourceConfig(ctx, app)
	if err != nil || workloadConfig["RHIZA_OBJSTORE_PROVIDER"] != "gcs" || workloadConfig["RHIZA_OBJSTORE_BUCKET"] != state.Bucket ||
		workloadConfig["RHIZA_OBJSTORE_ENDPOINT"] != "" || workloadConfig["STORAGE_EMULATOR_HOST"] != "" ||
		workloadConfig["RHIZA_OBJSTORE_DURABILITY"] != "async" || path.Join(workloadConfig["RHIZA_OBJSTORE_PREFIX"], state.SourceCluster) != state.Prefix {
		return fmt.Errorf("live workload GCS DATA scope differs from the pre-armed initial-HEAD scope")
	}
	source := gcsRecoverySource{Namespace: namespace, NamespaceUID: state.NamespaceUID, BindingName: text(nested(cluster, "metadata", "name")),
		LogicalID: logicalID, StatefulSet: stsName, Container: container, ConfigMap: "rhiza-config", ClusterID: state.SourceCluster,
		StatefulSetUID: stsUID, BindingUID: text(nested(cluster, "metadata", "uid")), Bucket: state.Bucket,
		Prefix: state.Prefix, Durability: workloadConfig["RHIZA_OBJSTORE_DURABILITY"], FencerPod: state.FencerPod}
	if source.BindingUID == "" || text(nested(cluster, "spec", "adoptClusterID")) != source.ClusterID ||
		nested(cluster, "spec", "bootstrap") != true || nested(cluster, "spec", "automatic") != true ||
		nested(cluster, "spec", "policy", "allowDataLoss") != true {
		return fmt.Errorf("initial-HEAD cluster is not the exact automatic async data-loss bootstrap")
	}
	if err := verifyGCSInitialHeadFencerPod(ctx, state); err != nil {
		return err
	}
	fencerDeployment, err := get(ctx, "deployment", "rhiza-fencer")
	if err != nil {
		return err
	}
	readScope, err := newGCSAckLossReadScope(ctx, source, fencerDeployment)
	if err != nil {
		return err
	}
	if readScope.Prefix != state.ControlPrefix || path.Join(strings.TrimSuffix(state.Prefix, "/"+state.SourceCluster), "control") != state.ControlPrefix {
		return fmt.Errorf("read-only observer CONTROL prefix differs from the exact run scope")
	}
	stateStore, evidenceStore, gcsClient, _, closeStores, err := openGCSAckLossAuthorityStores(ctx, source, readScope)
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

	receipt, err := waitForGCSAckLossReceipt(ctx, state.FencerPod, out)
	if err != nil {
		return fmt.Errorf("startup fencer did not emit the exact ACK-loss receipt: %w", err)
	}
	if receipt.Selector != operator.GCSMutationAckLossSelectorInitialArchiveHead || receipt.Runtime.NamespaceUID != state.NamespaceUID ||
		receipt.Runtime.SourceClusterID != state.SourceCluster || receipt.Runtime.StatefulSetUID != stsUID || receipt.Runtime.BindingUID != source.BindingUID ||
		receipt.ObjectKey != "archive/head.bin" || !isInitialHeadMutation(receipt.Intent) {
		return fmt.Errorf("fencer receipt does not identify the pre-armed canonical initial HEAD dispatch")
	}
	observation, err := observeGCSAckLossReceipt(ctx, stateStore, evidenceStore, source, receipt)
	if err != nil {
		return fmt.Errorf("read-only observer did not verify the canonical CONTROL producer and guard: %w", err)
	}
	fence, proof, recovery, fenceUID, err := waitForGCSInitialHeadRecovery(ctx, source, cluster, stateStore)
	if err != nil {
		return err
	}
	root, _, err := stateStore.ReadRoot(ctx, stsUID, source.ClusterID)
	if err != nil {
		return fmt.Errorf("read closed initial-HEAD CONTROL root: %w", err)
	}
	sourceRootDigest, err := verifyGCSInitialHeadClosedControl(root, source, fence, proof)
	if err != nil {
		return err
	}
	sourceHead, err := verifyGCSInitialHeadObject(ctx, gcsClient, source.Bucket, source.Prefix, observation.Guarded.ObjectGeneration,
		observation.Guarded.ObjectMetageneration, receipt.OperationID)
	if err != nil {
		return fmt.Errorf("independent read-only source HEAD verification failed: %w", err)
	}
	if err := verifyGCSInitialHeadSourceGenesis(ctx, stateStore, source, observation); err != nil {
		return err
	}
	target := recovery.Status.Target
	if !safeGCSPathComponent(target) || target == source.ClusterID {
		return fmt.Errorf("completed bootstrap recovery did not select a distinct target generation")
	}
	targetPrefix := path.Join(path.Dir(source.Prefix), target)
	targetHead, err := verifyGCSInitialHeadTargetGenesis(ctx, stateStore, gcsClient, source, targetPrefix, target, fence, proof, recovery, fenceUID, sourceRootDigest, sourceHead)
	if err != nil {
		return err
	}
	ack, readback, err := verifyGCSInitialHeadTargetReadBarrier(ctx, recovery, source, out)
	if err != nil {
		return err
	}
	finalSourceHead, err := verifyGCSInitialHeadObject(ctx, gcsClient, source.Bucket, source.Prefix, sourceHead.Generation, sourceHead.Metageneration, receipt.OperationID)
	if err != nil {
		return fmt.Errorf("re-read exact source canonical HEAD after target barrier: %w", err)
	}
	if finalSourceHead != sourceHead {
		return fmt.Errorf("source canonical HEAD evidence changed after target recovery barrier")
	}
	evidence := gcsInitialHeadPrivateEvidence{Version: 1,
		Source: obj{"namespaceUID": source.NamespaceUID, "clusterID": source.ClusterID, "statefulSetUID": source.StatefulSetUID, "bindingUID": source.BindingUID,
			"bucket": source.Bucket, "prefix": source.Prefix},
		FaultReceipt: receipt, CanonicalObservation: observation, SourceHead: sourceHead, Fence: fence, Proof: proof,
		Recovery: recovery, TargetHead: targetHead, TargetAck: ack, TargetReadback: readback}
	data, err := json.Marshal(evidence)
	if err != nil || len(data) > maxAckLossReceiptBytes*8 {
		return fmt.Errorf("encode bounded private initial-HEAD qualification evidence")
	}
	if err := writeNewPrivateAckLossFile(filepath.Join(out, "gcs-initial-head-qualification-private.json"), data); err != nil {
		return err
	}
	if err := restoreGCSInitialHeadAckLoss(ctx); err != nil {
		return fmt.Errorf("restore one-shot fencer after initial-HEAD qualification: %w", err)
	}
	return nil
}

func captureGCSInitialHeadCluster(ctx context.Context, state gcsInitialHeadInstallState) (obj, error) {
	var found obj
	list, err := get(ctx, "rhizaclusters", "")
	if err != nil {
		return nil, err
	}
	items, ok := list["items"].([]any)
	if !ok {
		return nil, fmt.Errorf("bootstrap RhizaCluster inventory is malformed")
	}
	for _, item := range items {
		if text(nested(item, "metadata", "namespace")) != namespace || text(nested(item, "metadata", "labels", "chaos.rhiza.io/run")) != qualificationRunID {
			continue
		}
		if found != nil {
			return nil, fmt.Errorf("multiple run-owned RhizaClusters make startup source identity ambiguous")
		}
		candidate, ok := item.(map[string]any)
		if !ok || text(nested(candidate, "metadata", "uid")) == "" || text(nested(candidate, "spec", "adoptClusterID")) != state.SourceCluster {
			return nil, fmt.Errorf("run-owned RhizaCluster does not match the pre-armed source generation")
		}
		found = candidate
	}
	if found == nil {
		return nil, fmt.Errorf("run-owned initial bootstrap RhizaCluster was not found")
	}
	return found, nil
}

func verifyGCSInitialHeadFencerPod(ctx context.Context, state gcsInitialHeadInstallState) error {
	pod, err := get(ctx, "pod", state.FencerPod)
	if err != nil || text(nested(pod, "metadata", "uid")) != state.FencerPodUID ||
		text(nested(pod, "metadata", "namespace")) != namespace || text(nested(pod, "metadata", "labels", "chaos.rhiza.io/run")) != qualificationRunID {
		return fmt.Errorf("pre-armed fencer Pod identity changed before receipt collection")
	}
	deployment, err := get(ctx, "deployment", "rhiza-fencer")
	if err != nil || text(nested(deployment, "metadata", "uid")) != state.DeploymentUID || !ackLossPodOwnedBy(ctx, pod, state.DeploymentUID) {
		return fmt.Errorf("pre-armed fencer Pod is not owned by its exact run Deployment")
	}
	configMap, err := get(ctx, "configmap", gcsAckLossConfigMapName)
	if err != nil || text(nested(configMap, "metadata", "uid")) != state.ConfigMapUID || text(nested(configMap, "metadata", "labels", "chaos.rhiza.io/run")) != qualificationRunID ||
		gcsInitialHeadDigest([]byte(text(nested(configMap, "data", "config.json")))) != state.ConfigDigest {
		return fmt.Errorf("pre-armed immutable fencer ConfigMap identity or contents changed")
	}
	return nil
}

func waitForGCSInitialHeadRecovery(ctx context.Context, source gcsRecoverySource, cluster obj, stateStore *operator.AuthorityStateStore) (operator.FenceRequest, operator.FenceProof, operator.Resource, string, error) {
	var request operator.FenceRequest
	var proof operator.FenceProof
	var recoveryResource operator.Resource
	var fenceUID string
	err := wait(ctx, "canonical initial-HEAD closure, automatic recovery, and target completion", func() (bool, error) {
		fenceList, err := get(ctx, "rhizafences", "")
		if err != nil {
			return false, err
		}
		items, ok := fenceList["items"].([]any)
		if !ok {
			return false, fmt.Errorf("RhizaFence inventory is malformed")
		}
		var matched any
		for _, item := range items {
			var candidate operator.FenceRequest
			if err := json.Unmarshal(encoded(nested(item, "spec")), &candidate); err != nil {
				return false, err
			}
			binding := candidate.BootstrapGenesis
			if binding == nil || candidate.Namespace != source.Namespace || candidate.SourceClusterID != source.ClusterID ||
				candidate.StatefulSet != source.StatefulSet || candidate.StatefulSetUID != source.StatefulSetUID || candidate.BindingUID != source.BindingUID ||
				candidate.LogicalID != source.LogicalID || binding.ClusterName != text(nested(cluster, "metadata", "name")) || binding.ClusterUID != text(nested(cluster, "metadata", "uid")) {
				continue
			}
			if matched != nil {
				return false, fmt.Errorf("multiple generation fences match the initial bootstrap source")
			}
			matched = item
			request = candidate
		}
		if matched == nil {
			return false, nil
		}
		fenceUID = text(nested(matched, "metadata", "uid"))
		if text(nested(matched, "metadata", "namespace")) != source.Namespace || text(nested(matched, "metadata", "labels", "chaos.rhiza.io/run")) != qualificationRunID ||
			fenceUID == "" || request.Scope != operator.ScopeGeneration || request.OperationID == "" || len(request.Targets) == 0 || len(request.Targets) > 64 {
			return false, fmt.Errorf("initial bootstrap fence identity or target set is invalid")
		}
		if nested(matched, "status", "proof") == nil {
			return false, nil
		}
		if err := json.Unmarshal(encoded(nested(matched, "status", "proof")), &proof); err != nil {
			return false, err
		}
		state, _, err := stateStore.ReadRoot(ctx, source.StatefulSetUID, source.ClusterID)
		if err != nil {
			return false, fmt.Errorf("read canonical closed bootstrap CONTROL root: %w", err)
		}
		if _, err := verifyGCSInitialHeadClosedControl(state, source, request, proof); err != nil {
			return false, err
		}
		if _, _, err := verifyIndependentGenerationProof(matched, source.ClusterID, source.StatefulSetUID, source.BindingUID,
			request.Targets, source.Bucket, source.Prefix, source.Namespace, source.StatefulSet, source.LogicalID); err != nil {
			return false, err
		}
		recoveryList, err := get(ctx, "rhizarecoveries", "")
		if err != nil {
			return false, err
		}
		recoveries, ok := recoveryList["items"].([]any)
		if !ok {
			return false, fmt.Errorf("RhizaRecovery inventory is malformed")
		}
		matches := 0
		for _, item := range recoveries {
			if text(nested(item, "metadata", "namespace")) != source.Namespace || text(nested(item, "spec", "sourceClusterID")) != source.ClusterID || text(nested(item, "spec", "recoveryID")) != request.OperationID {
				continue
			}
			matches++
			if err := json.Unmarshal(encoded(item), &recoveryResource); err != nil {
				return false, err
			}
		}
		if matches > 1 {
			return false, fmt.Errorf("multiple automatic recoveries match the canonical bootstrap fence")
		}
		if matches == 0 || recoveryResource.Status.Phase != "Complete" || recoveryResource.Status.Stage != "Complete" {
			return false, nil
		}
		if recoveryResource.Spec.SourceClusterID != source.ClusterID || recoveryResource.Spec.RecoveryID != request.OperationID || !recoveryResource.Spec.AllowDataLoss || recoveryResource.Spec.Durability != "async" ||
			recoveryResource.Status.Source != source.ClusterID || recoveryResource.Status.StatefulSetUID != source.StatefulSetUID || recoveryResource.Status.Target == "" || recoveryResource.Status.Target == source.ClusterID ||
			recoveryResource.Status.TargetBaseKind != "empty" || recoveryResource.Status.RecoveredTip != 0 || recoveryResource.Status.ManifestHash != "" || recoveryResource.Status.SourceDurability != "async" {
			return false, fmt.Errorf("completed initial-HEAD recovery violates the explicit async empty-base policy")
		}
		if err := recoveryFenceMatchesProof(recoveryResource, request, proof); err != nil {
			return false, err
		}
		if err := verifyAsyncEmptyRecovery(recoveryResource, request, proof); err != nil {
			return false, err
		}
		active, err := get(ctx, "rhizacluster", text(nested(cluster, "metadata", "name")))
		if err != nil || text(nested(active, "metadata", "uid")) != source.BindingUID || nested(active, "status", "phase") != "Healthy" || nested(active, "status", "activeClusterID") != recoveryResource.Status.Target {
			return false, nil
		}
		return true, nil
	})
	return request, proof, recoveryResource, fenceUID, err
}

func verifyGCSInitialHeadClosedControl(state any, source gcsRecoverySource, request operator.FenceRequest, proof operator.FenceProof) (string, error) {
	if request.BootstrapGenesis == nil || request.Namespace != source.Namespace || request.SourceClusterID != source.ClusterID || request.StatefulSet != source.StatefulSet ||
		request.StatefulSetUID != source.StatefulSetUID || request.BindingUID != source.BindingUID || request.LogicalID != source.LogicalID || request.Scope != operator.ScopeGeneration ||
		request.BootstrapGenesis.Version != 1 || request.BootstrapGenesis.ClusterName == "" || request.BootstrapGenesis.ClusterUID == "" || request.BootstrapGenesis.CandidateDigest == "" {
		return "", fmt.Errorf("bootstrap fence request differs from the pre-armed source identity")
	}
	requestDigest := gcsInitialHeadDigest(encoded(request))
	var root struct {
		StatefulSetUID string         `json:"statefulSetUID"`
		Ledger         fencing.Ledger `json:"ledger"`
	}
	if err := json.Unmarshal(encoded(state), &root); err != nil {
		return "", fmt.Errorf("decode protected source CONTROL root: %w", err)
	}
	ledger := root.Ledger
	if root.StatefulSetUID != source.StatefulSetUID || ledger.SourceClusterID != source.ClusterID ||
		!ledger.RequireRuntimeBinding || !ledger.RequireMutationJournal || ledger.ClosedBy != request.OperationID || ledger.ClosedHash != requestDigest {
		return "", fmt.Errorf("protected source CONTROL root is not closed by the exact generation fence request")
	}
	ledgerDigest := gcsInitialHeadDigest(encoded([]any{source.Namespace, state}))
	if proof.IndependentV2 == nil || proof.IndependentV2.LedgerHash != ledgerDigest || proof.IndependentV2.RequestHash != requestDigest ||
		proof.OperationID != request.OperationID || proof.SourceClusterID != source.ClusterID || proof.BindingUID != source.BindingUID || proof.RequestHash != requestDigest ||
		!proof.ProcessesTerminated || !proof.RecreationBlocked || !proof.StorageQuiesced {
		return "", fmt.Errorf("IndependentV2 proof is not bound to the exact protected closed CONTROL root")
	}
	rootDigest := gcsInitialHeadDigest(encoded([]any{"rhiza-empty-recovery-closed-root-v1", source.Namespace, root.StatefulSetUID,
		ledger.SourceClusterID, ledger.ClosedBy, ledger.ClosedHash, ledger.RequireMutationJournal, ledger.ArchiveGenesisState,
		ledger.ArchiveScopeHash, ledger.ArchiveGenesisRef}))
	return rootDigest, nil
}

func isInitialHeadMutation(intent fencing.MutationIntent) bool {
	head, err := recovery.EmptyArchiveHeadBytes()
	return err == nil && intent.Kind == "upload" && intent.Key == "archive/head.bin" && intent.Condition.Mode == fencing.MutationCreateOnly &&
		intent.PayloadSize == int64(len(head)) && intent.PayloadDigest == gcsInitialHeadDigest(head)
}

func verifyGCSInitialHeadObject(ctx context.Context, client *storage.Client, bucket, prefix string, generation, metageneration int64, operationID string) (gcsAckLossObjectEvidence, error) {
	var evidence gcsAckLossObjectEvidence
	if client == nil || bucket == "" || prefix == "" || operationID == "" {
		return evidence, fmt.Errorf("exact initial HEAD GCS read scope is incomplete")
	}
	key := path.Join(prefix, "archive/head.bin")
	base := client.Bucket(bucket).Object(key).Retryer(storage.WithPolicy(storage.RetryNever))
	var attrs *storage.ObjectAttrs
	var err error
	if generation > 0 {
		attrs, err = base.Generation(generation).Attrs(ctx)
	} else {
		attrs, err = base.Attrs(ctx)
	}
	if err != nil || attrs == nil || attrs.Generation <= 0 || attrs.Metageneration <= 0 || generation > 0 && attrs.Generation != generation || metageneration > 0 && attrs.Metageneration != metageneration ||
		!attrs.TemporaryHold || attrs.Metadata["rhiza-operation-id"] != operationID {
		return evidence, fmt.Errorf("initial HEAD is not the exact held GCS generation bound by CONTROL")
	}
	object := base.Generation(attrs.Generation)
	if generation > 0 {
		object = base.Generation(generation)
	}
	reader, err := object.NewReader(ctx)
	if err != nil {
		return evidence, fmt.Errorf("read exact generation of initial archive HEAD")
	}
	data, readErr := io.ReadAll(io.LimitReader(reader, 1<<20))
	closeErr := reader.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return evidence, fmt.Errorf("read exact initial archive HEAD bytes: %w", err)
	}
	want, err := recovery.EmptyArchiveHeadBytes()
	if err != nil || !bytes.Equal(data, want) {
		return evidence, fmt.Errorf("initial archive HEAD bytes are not the canonical empty HEAD")
	}
	digest := gcsInitialHeadDigest(data)
	if attrs.Size != int64(len(data)) || attrs.Metadata["rhiza-payload-sha256"] != digest {
		return evidence, fmt.Errorf("initial HEAD attributes do not bind the exact canonical payload")
	}
	finalAttrs, err := object.Attrs(ctx)
	if err != nil || finalAttrs.Generation != attrs.Generation || finalAttrs.Metageneration != attrs.Metageneration || !finalAttrs.TemporaryHold || finalAttrs.Size != attrs.Size ||
		finalAttrs.Metadata["rhiza-operation-id"] != operationID || finalAttrs.Metadata["rhiza-payload-sha256"] != digest {
		return evidence, fmt.Errorf("initial HEAD generation changed during independent read")
	}
	return gcsAckLossObjectEvidence{Generation: attrs.Generation, Metageneration: attrs.Metageneration, Size: attrs.Size, PayloadSHA256: digest, TemporaryHold: true}, nil
}

func verifyGCSInitialHeadSourceGenesis(ctx context.Context, store *operator.AuthorityStateStore, source gcsRecoverySource, observation gcsHAAckObservation) error {
	scope := fencing.ArchiveScope{Provider: "gcs", Bucket: source.Bucket, Prefix: source.Prefix, ClusterID: source.ClusterID}
	scopeHash, err := scope.Hash()
	if err != nil {
		return err
	}
	claim, _, err := store.ReadArchiveScopeClaim(ctx, scope)
	if err != nil || claim.GenesisRef == nil || claim.GenesisRef.Mutation == nil || claim.OwnerStatefulSetUID != source.StatefulSetUID || claim.OwnerSourceClusterID != source.ClusterID ||
		claim.GenesisRef.ScopeHash != scopeHash || claim.GenesisRef.ValidateForScope(scope) != nil ||
		claim.GenesisRef.Mutation.ProducerUID != observation.ProducerUID || claim.GenesisRef.Mutation.Sequence != observation.Sequence || claim.GenesisRef.Mutation.Intent != observation.Guarded.Intent ||
		claim.GenesisRef.Mutation.Intent.OperationID != observation.Guarded.Intent.OperationID || !isInitialHeadMutation(claim.GenesisRef.Mutation.Intent) {
		return errors.Join(err, fmt.Errorf("source canonical genesis claim does not match the guarded startup dispatch"))
	}
	root, _, err := store.ReadRoot(ctx, source.StatefulSetUID, source.ClusterID)
	if err != nil || root.Ledger.ArchiveGenesisRef == nil || gcsInitialHeadDigest(encoded(root.Ledger.ArchiveGenesisRef)) != gcsInitialHeadDigest(encoded(claim.GenesisRef)) {
		return errors.Join(err, fmt.Errorf("source canonical genesis reference differs between root and stable scope claim"))
	}
	return nil
}

func verifyGCSInitialHeadTargetGenesis(ctx context.Context, store *operator.AuthorityStateStore, client *storage.Client, source gcsRecoverySource, targetPrefix, targetID string, fence operator.FenceRequest, proof operator.FenceProof, resource operator.Resource, fenceUID, sourceRootDigest string, sourceHead gcsAckLossObjectEvidence) (gcsAckLossObjectEvidence, error) {
	var zero gcsAckLossObjectEvidence
	sourceScope := fencing.ArchiveScope{Provider: "gcs", Bucket: source.Bucket, Prefix: source.Prefix, ClusterID: source.ClusterID}
	targetScope := fencing.ArchiveScope{Provider: "gcs", Bucket: source.Bucket, Prefix: targetPrefix, ClusterID: targetID}
	sourceClaim, _, err := store.ReadArchiveScopeClaim(ctx, sourceScope)
	if err != nil || sourceClaim.GenesisRef == nil {
		return zero, errors.Join(err, fmt.Errorf("source canonical genesis reference is unavailable"))
	}
	targetClaim, _, err := store.ReadArchiveScopeClaim(ctx, targetScope)
	if err != nil || targetClaim.GenesisRef == nil || targetClaim.GenesisRef.Mutation == nil || targetClaim.EmptyRecoveryAuthorization == nil {
		return zero, errors.Join(err, fmt.Errorf("target CONTROL scope has no committed genesis mutation and empty-recovery authorization"))
	}
	if proof.IndependentV2 == nil {
		return zero, fmt.Errorf("target genesis has no independent source closure proof")
	}
	authorization := targetClaim.EmptyRecoveryAuthorization
	sourceHash, sourceErr := sourceScope.Hash()
	targetHash, targetErr := targetScope.Hash()
	if sourceErr != nil || targetErr != nil || targetClaim.OwnerStatefulSetUID != source.StatefulSetUID || targetClaim.OwnerSourceClusterID != targetID ||
		authorization.StatefulSetUID != source.StatefulSetUID || authorization.TargetClusterID != targetID || authorization.OperationID != resource.Spec.RecoveryID ||
		authorization.SourceFenceOperationID != fence.OperationID || authorization.SourceTip != 0 || authorization.SourceScopeHash != sourceHash || authorization.TargetScopeHash != targetHash ||
		authorization.SourceGenesisRefDigest != gcsInitialHeadDigest(encoded(sourceClaim.GenesisRef)) || authorization.SourceRootDigest != sourceRootDigest ||
		authorization.RecoveryMode != operator.RecoveryModeFrozenSource || authorization.SourcePrefix != source.Prefix || authorization.BucketName != source.Bucket ||
		authorization.SourceDurability != "async" || !authorization.AllowDataLoss || authorization.TargetDurability != "async" ||
		authorization.ConfigID != targetClaim.ConfigID || targetClaim.GenesisRef.ConfigID != targetClaim.ConfigID || targetClaim.GenesisRef.ValidateForScope(targetScope) != nil ||
		!isInitialHeadMutation(targetClaim.GenesisRef.Mutation.Intent) {
		return zero, fmt.Errorf("target empty-recovery authorization or canonical genesis mutation differs from the closed source decision")
	}
	if err := authorization.Validate(); err != nil {
		return zero, fmt.Errorf("target empty-recovery authorization is malformed: %w", err)
	}
	head, err := recovery.EmptyArchiveHeadBytes()
	if err != nil {
		return zero, err
	}
	if authorization.TargetEmptyHeadDigest != gcsInitialHeadDigest(head) || targetClaim.GenesisRef.Mutation.Intent.PayloadDigest != authorization.TargetEmptyHeadDigest {
		return zero, fmt.Errorf("target genesis does not commit the canonical empty archive HEAD")
	}
	if fenceUID == "" || sourceRootDigest == "" || authorization.SourceFenceDigest != gcsInitialHeadDigest(encoded([]any{"rhiza-empty-recovery-source-fence-v1", fenceUID, fence, proof})) ||
		authorization.SourceHeadDigest != sourceHead.PayloadSHA256 || authorization.SourceHeadVersion.Type != objstore.Generation || authorization.SourceHeadVersion.Value != fmt.Sprint(sourceHead.Generation) {
		return zero, fmt.Errorf("target authorization does not bind the exact closed fence and held source HEAD generation")
	}
	evidence, err := verifyGCSInitialHeadObject(ctx, client, source.Bucket, targetPrefix, 0, 0, targetClaim.GenesisRef.Mutation.Intent.OperationID)
	if err != nil {
		return zero, fmt.Errorf("independent read-only target genesis verification failed: %w", err)
	}
	return evidence, nil
}

func verifyGCSInitialHeadTargetReadBarrier(ctx context.Context, resource operator.Resource, source gcsRecoverySource, out string) (gcsAcknowledgement, string, error) {
	var ack gcsAcknowledgement
	if resource.Status.SecretName == "" || len(resource.Status.TargetIdentities) == 0 {
		return ack, "", fmt.Errorf("completed bootstrap recovery lacks a target admin Secret or voter")
	}
	secret, err := get(ctx, "secret", resource.Status.SecretName)
	if err != nil {
		return ack, "", err
	}
	adminToken, err := base64.StdEncoding.DecodeString(text(nested(secret, "data", "admin")))
	if err != nil || len(adminToken) == 0 {
		return ack, "", fmt.Errorf("target admin credential is unavailable")
	}
	target := resource.Status.TargetIdentities[0]
	if target.Pod == "" || target.PodUID == "" {
		return ack, "", fmt.Errorf("completed bootstrap recovery has no concrete target voter")
	}
	pod, err := get(ctx, "pod", target.Pod)
	if err != nil || text(nested(pod, "metadata", "uid")) != target.PodUID || !gcsPodOwnedBy(pod, source.StatefulSetUID) {
		return ack, "", fmt.Errorf("target read-barrier Pod is not the exact recovery-owned incarnation")
	}
	ready := false
	conditions, _ := nested(pod, "status", "conditions").([]any)
	for _, condition := range conditions {
		ready = ready || nested(condition, "type") == "Ready" && nested(condition, "status") == "True"
	}
	if !ready {
		return ack, "", fmt.Errorf("target voter is not Ready for the application read barrier")
	}
	forward, err := startForward(ctx, target.Pod, out)
	if err != nil {
		return ack, "", err
	}
	defer forward.close()
	status, err := readMembershipWithToken(ctx, forward, string(adminToken))
	if err != nil || status.ClusterID != resource.Status.Target || status.Pending || !status.Voting {
		return ack, "", fmt.Errorf("target voter did not pass authenticated membership: %v", err)
	}
	data, err := call(ctx, forward, "/recovery/status", nil, string(adminToken))
	if err != nil {
		return ack, "", fmt.Errorf("read target quorum status: %w", err)
	}
	var quorum network.VoterRecoveryStatus
	if err := json.Unmarshal(encoded(data), &quorum); err != nil || quorum.ClusterID != resource.Status.Target || !quorum.Quorum {
		return ack, "", fmt.Errorf("target did not pass authenticated quorum recovery status")
	}
	if _, err := call(ctx, forward, "/sql/execute", obj{"request_id": "gcs-initial-head-target-create-table", "sql": "CREATE TABLE fencing_items (id INTEGER PRIMARY KEY, value TEXT)"}, string(adminToken)); err != nil {
		return ack, "", fmt.Errorf("initialize target read-barrier table: %w", err)
	}
	ack, err = writeGCSAcknowledgedRow(ctx, forward, string(adminToken), 1, "gcs-initial-head-target")
	if err != nil {
		return ack, "", fmt.Errorf("target write-and-read barrier failed: %w", err)
	}
	rows, err := call(ctx, forward, "/sql/query", obj{"sql": "SELECT id,value FROM fencing_items ORDER BY id", "consistency": "linearizable"}, string(adminToken))
	if err != nil || string(encoded(rows["rows"])) != `[[1,"gcs-initial-head-target"]]` {
		return ack, "", fmt.Errorf("target linearizable application read barrier failed: %v", err)
	}
	return ack, string(encoded(rows["rows"])), nil
}
