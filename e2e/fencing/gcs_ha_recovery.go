package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/operator"
)

type gcsRecoverySource struct {
	Namespace        string
	BindingName      string
	LogicalID        string
	StatefulSet      string
	Container        string
	ConfigMap        string
	OperatorPod      string
	OperatorIdentity gcsControlPodIdentity
	FencerPod        string
	AgentDaemonUID   string
	AgentPods        []gcsControlPodIdentity
	InitialRestart   map[string]int
	ClusterID        string
	StatefulSetUID   string
	BindingUID       string
	Bucket           string
	Endpoint         string
	Prefix           string
	Targets          []operator.FenceTarget
}

type gcsControlPodIdentity struct {
	Name           string `json:"name"`
	UID            string `json:"uid"`
	OwnerUID       string `json:"ownerUID"`
	ParentOwnerUID string `json:"parentOwnerUID,omitempty"`
}

type gcsRecoveryOutcome struct {
	Source    gcsRecoverySource
	Fence     operator.FenceRequest
	Proof     operator.FenceProof
	Recovery  operator.Resource
	OldReturn obj
	Forward   *forward
	Admin     string
}

func qualifyGCSHA(ctx context.Context) (resultErr error) {
	out := os.Getenv("FENCING_E2E_OUT")
	if out == "" {
		out = "gcs-ha-recovery-results"
	}
	if err := os.MkdirAll(out, 0700); err != nil {
		return err
	}
	events, err := os.OpenFile(filepath.Join(out, "gcs-ha-events.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer events.Close()
	event := func(name string, data any) {
		_ = json.NewEncoder(events).Encode(obj{"event": name, "time": time.Now().UTC(), "data": data})
	}
	var faults []*gcsChaosFault
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for i := len(faults) - 1; i >= 0; i-- {
			if err := faults[i].Restore(cleanupCtx); err != nil {
				resultErr = errors.Join(resultErr, fmt.Errorf("restore owned GCS Chaos fault: %w", err))
			}
			event("chaos_cleanup", gcsChaosEvidence(faults[i]))
		}
	}()

	firstSource, err := captureGCSRecoverySource(ctx, out)
	if err != nil {
		return err
	}
	adminToken, err := sourceAdminToken(ctx, firstSource.StatefulSet, firstSource.Container)
	if err != nil {
		return err
	}
	firstForward, err := startForward(ctx, firstSource.Targets[0].Pod, out)
	if err != nil {
		return err
	}
	if _, err := call(ctx, firstForward, "/sql/execute", obj{"request_id": "gcs-ha-create-table", "sql": "CREATE TABLE fencing_items (id INTEGER PRIMARY KEY, value TEXT)"}, adminToken); err != nil {
		firstForward.close()
		return fmt.Errorf("create GCS qualification table: %w", err)
	}
	ack1, err := writeGCSAcknowledgedRow(ctx, firstForward, adminToken, 1, "gcs-ack-1")
	firstForward.close()
	if err != nil {
		return err
	}
	rows1 := `[[1,"gcs-ack-1"]]`
	event("source_ack", obj{"source": firstSource.ClusterID, "ack": ack1})
	firstOutcome, err := gcsLoseQuorumAndRecover(ctx, firstSource, adminToken, rows1, "first", out, event, &faults)
	if err != nil {
		return fmt.Errorf("first GCS recovery: %w", err)
	}
	defer firstOutcome.Forward.close()
	event("first_recovery_complete", obj{"source": firstSource.ClusterID, "target": firstOutcome.Recovery.Status.Target, "proofID": firstOutcome.Proof.ProofID})

	secondSource, err := captureGCSRecoverySource(ctx, out)
	if err != nil {
		return fmt.Errorf("capture first recovered generation: %w", err)
	}
	if secondSource.ClusterID != firstOutcome.Recovery.Status.Target || secondSource.ClusterID == firstSource.ClusterID {
		return fmt.Errorf("second source is not the distinct first recovery target")
	}
	ack2, err := writeGCSAcknowledgedRow(ctx, firstOutcome.Forward, firstOutcome.Admin, 2, "gcs-ack-2")
	if err != nil {
		return fmt.Errorf("second-generation acknowledged row: %w", err)
	}
	rows2 := `[[1,"gcs-ack-1"],[2,"gcs-ack-2"]]`
	event("second_source_ack", obj{"source": secondSource.ClusterID, "ack": ack2})
	secondOutcome, err := gcsLoseQuorumAndRecover(ctx, secondSource, firstOutcome.Admin, rows2, "second", out, event, &faults)
	if err != nil {
		return fmt.Errorf("second GCS recovery: %w", err)
	}
	defer secondOutcome.Forward.close()
	if err := verifyRecoveryLineage(firstOutcome.Recovery, secondOutcome.Recovery, firstOutcome.Fence, secondOutcome.Fence); err != nil {
		return err
	}
	event("second_recovery_lineage_complete", obj{"source": secondOutcome.Recovery.Status.Source, "target": secondOutcome.Recovery.Status.Target, "proofID": secondOutcome.Proof.ProofID})
	oldReturn := obj{"status": "qualified", "generations": []obj{firstOutcome.OldReturn, secondOutcome.OldReturn}}
	if err := os.WriteFile(filepath.Join(out, "gcs-old-pod-return.json"), encoded(oldReturn), 0600); err != nil {
		return err
	}
	evidence := obj{
		"qualification":    "gcs-two-recovery-noPVC",
		"profile":          obj{"provider": "gcs", "bucket": firstSource.Bucket, "noPVC": true},
		"acknowledgements": []gcsAcknowledgement{ack1, ack2},
		"recoveries": []obj{
			gcsRecoveryEvidence(firstOutcome),
			gcsRecoveryEvidence(secondOutcome),
		},
		"proofSource":  "RhizaFence.status.proof read through the authenticated Kubernetes API; remote terminal payloads were not independently fetched by this driver",
		"oldPodReturn": oldReturn,
	}
	if err := os.WriteFile(filepath.Join(out, "gcs-ha-evidence.json"), encoded(evidence), 0600); err != nil {
		return err
	}
	event("old_pod_return_qualified", oldReturn)
	return nil
}

func gcsRecoveryEvidence(outcome *gcsRecoveryOutcome) obj {
	return obj{"source": outcome.Source, "fence": outcome.Fence, "proof": outcome.Proof, "recovery": outcome.Recovery}
}

func sourceAdminToken(ctx context.Context, statefulSet, container string) (string, error) {
	sts, err := get(ctx, "statefulset", statefulSet)
	if err != nil {
		return "", err
	}
	containers, _ := nested(sts, "spec", "template", "spec", "containers").([]any)
	var appContainer any
	for _, candidate := range containers {
		if nested(candidate, "name") == container {
			appContainer = candidate
		}
	}
	if appContainer == nil {
		return "", fmt.Errorf("source admin credential lookup cannot find bound application container")
	}
	env, _ := nested(appContainer, "env").([]any)
	for _, value := range env {
		if nested(value, "name") != "RHIZA_ADMIN_TOKEN" {
			continue
		}
		name := text(nested(value, "valueFrom", "secretKeyRef", "name"))
		key := text(nested(value, "valueFrom", "secretKeyRef", "key"))
		if name != "" && key != "" {
			secret, err := get(ctx, "secret", name)
			if err != nil {
				return "", err
			}
			if token, err := base64.StdEncoding.DecodeString(text(nested(secret, "data", key))); err == nil && len(token) != 0 {
				return string(token), nil
			}
		}
	}
	refs, _ := nested(appContainer, "envFrom").([]any)
	for _, ref := range refs {
		name := text(nested(ref, "secretRef", "name"))
		if name == "" || nested(ref, "secretRef", "name") == "" {
			continue
		}
		secret, err := get(ctx, "secret", name)
		if err != nil {
			return "", err
		}
		for _, key := range []string{"RHIZA_ADMIN_TOKEN", "admin"} {
			if token, err := base64.StdEncoding.DecodeString(text(nested(secret, "data", key))); err == nil && len(token) != 0 {
				return string(token), nil
			}
		}
	}
	return "", fmt.Errorf("source workload has no readable admin Secret")
}

func gcsLoseQuorumAndRecover(ctx context.Context, source gcsRecoverySource, token, expectedRows, phase, out string, event func(string, any), faults *[]*gcsChaosFault) (*gcsRecoveryOutcome, error) {
	chaosCtx, cancelChaos := context.WithTimeout(ctx, 165*time.Second)
	defer cancelChaos()
	if len(source.Targets) < 3 || len(source.Targets) > 64 || len(source.AgentPods) == 0 || source.OperatorPod == "" {
		return nil, fmt.Errorf("quorum-loss scenario lacks a supported voter cohort or exact control Pods")
	}
	if err := verifyGCSControlPodSnapshots(chaosCtx, source.AgentPods); err != nil {
		return nil, fmt.Errorf("runtime-agent cohort changed since source capture: %w", err)
	}
	agentFault, err := createGCSControlLoss(chaosCtx, namespace, qualificationRunID, phase, "runtime-agent", source.AgentPods)
	if agentFault != nil {
		*faults = append(*faults, agentFault)
	}
	if err != nil {
		return nil, fmt.Errorf("isolate source runtime-agent cohort: %w", err)
	}
	if !gcsFaultSourceMatches(agentFault, source.AgentPods) {
		return nil, fmt.Errorf("runtime-agent NetworkChaos source UIDs differ from captured DaemonSet Pod snapshot")
	}
	if err := agentFault.WaitInjected(chaosCtx); err != nil {
		return nil, fmt.Errorf("runtime-agent cohort isolation was not injected: %w", err)
	}
	event("runtime_agent_network_isolated", gcsChaosEvidence(agentFault))
	cohortFault, err := partitionGCSVoterCohort(chaosCtx, namespace, qualificationRunID, phase, source.Targets)
	if cohortFault != nil {
		*faults = append(*faults, cohortFault)
	}
	if err != nil {
		return nil, fmt.Errorf("partition exact source voter cohort: %w", err)
	}
	if err := cohortFault.WaitInjected(chaosCtx); err != nil {
		return nil, fmt.Errorf("source voter full-mesh partition was not injected: %w", err)
	}
	event("source_voter_full_mesh_partitioned", gcsChaosEvidence(cohortFault))
	observer, err := startForward(chaosCtx, source.Targets[0].Pod, out)
	if err != nil {
		return nil, err
	}
	err = wait(chaosCtx, "source voter reports no quorum", func() (bool, error) {
		data, err := call(chaosCtx, observer, "/recovery/status", nil, token)
		if err != nil {
			return false, err
		}
		var status network.VoterRecoveryStatus
		if err := json.Unmarshal(encoded(data), &status); err != nil {
			return false, err
		}
		if status.ClusterID != source.ClusterID {
			return false, fmt.Errorf("quorum observer changed source generation")
		}
		return !status.Quorum, nil
	})
	observer.close()
	if err != nil {
		return nil, err
	}
	event("source_quorum_lost", obj{"source": source.ClusterID, "isolatedPodUIDs": podUIDs(source.Targets)})
	pending, err := waitForPendingGCSFence(chaosCtx, source)
	if err != nil {
		return nil, err
	}
	if err := agentFault.RecheckInjected(chaosCtx); err != nil {
		return nil, fmt.Errorf("runtime-agent partition expired before pending fence observation: %w", err)
	}
	if err := cohortFault.RecheckInjected(chaosCtx); err != nil {
		return nil, fmt.Errorf("voter partition expired before pending fence observation: %w", err)
	}
	event("generation_fence_pending", pending)
	operatorSnapshot := []gcsControlPodIdentity{source.OperatorIdentity}
	if err := verifyGCSControlPodSnapshots(chaosCtx, operatorSnapshot); err != nil {
		return nil, fmt.Errorf("operator Pod changed since source capture: %w", err)
	}
	operatorFault, err := createGCSControlLoss(chaosCtx, namespace, qualificationRunID, phase, "operator", operatorSnapshot)
	if operatorFault != nil {
		*faults = append(*faults, operatorFault)
	}
	if err != nil {
		return nil, fmt.Errorf("isolate operator Pod after fence request persisted: %w", err)
	}
	if !gcsFaultSourceMatches(operatorFault, operatorSnapshot) {
		return nil, fmt.Errorf("operator NetworkChaos source UID differs from captured Deployment Pod snapshot")
	}
	if err := operatorFault.WaitInjected(chaosCtx); err != nil {
		return nil, fmt.Errorf("operator network isolation was not injected: %w", err)
	}
	if err := confirmPendingGCSFence(chaosCtx, source, pending); err != nil {
		return nil, fmt.Errorf("operator isolation raced fence publication: %w", err)
	}
	if err := operatorFault.RecheckInjected(chaosCtx); err != nil {
		return nil, fmt.Errorf("operator partition expired before proof observation: %w", err)
	}
	if err := cohortFault.RecheckInjected(chaosCtx); err != nil {
		return nil, fmt.Errorf("voter partition expired before proof observation: %w", err)
	}
	event("operator_network_isolated", gcsChaosEvidence(operatorFault))
	if err := agentFault.Restore(chaosCtx); err != nil {
		return nil, fmt.Errorf("restore source runtime-agent network: %w", err)
	}
	if err := waitForGCSFenceProof(chaosCtx, source, pending); err != nil {
		return nil, err
	}
	if err := operatorFault.RecheckInjected(chaosCtx); err != nil {
		return nil, fmt.Errorf("operator partition expired before closed-generation return observation: %w", err)
	}
	if err := cohortFault.RecheckInjected(chaosCtx); err != nil {
		return nil, fmt.Errorf("voter partition expired before closed-generation return observation: %w", err)
	}
	event("generation_fence_proven", obj{"fenceName": pending.Name, "operationID": pending.Request.OperationID})
	oldReturn, err := waitForOldGenerationReturn(chaosCtx, source, out)
	if err != nil {
		return nil, err
	}
	if err := operatorFault.RecheckInjected(chaosCtx); err != nil {
		return nil, fmt.Errorf("operator partition expired during closed-generation return observation: %w", err)
	}
	if err := cohortFault.RecheckInjected(chaosCtx); err != nil {
		return nil, fmt.Errorf("voter partition expired during closed-generation return observation: %w", err)
	}
	event("old_pod_return_refused", obj{"source": source.ClusterID, "podUIDs": podUIDs(source.Targets), "oracle": "same Pod UID restarted, unready, previous container log reports fencing: incarnation terminally fenced"})
	if err := cohortFault.Restore(chaosCtx); err != nil {
		return nil, fmt.Errorf("restore voter partition after closed-generation return observation: %w", err)
	}
	if err := operatorFault.Restore(chaosCtx); err != nil {
		return nil, fmt.Errorf("restore operator control after return observation: %w", err)
	}
	outcome, err := waitForGCSRecovery(ctx, source, "fork", expectedRows, out)
	if err != nil {
		return nil, err
	}
	outcome.OldReturn = oldReturn
	return outcome, nil
}

type pendingGCSFence struct {
	Name    string                `json:"name"`
	Request operator.FenceRequest `json:"request"`
}

func podUIDs(targets []operator.FenceTarget) []string {
	uids := make([]string, 0, len(targets))
	for _, target := range targets {
		uids = append(uids, target.PodUID)
	}
	return uids
}

func waitForPendingGCSFence(ctx context.Context, source gcsRecoverySource) (pendingGCSFence, error) {
	var pending pendingGCSFence
	err := wait(ctx, "exact source generation fence request without proof", func() (bool, error) {
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
			if request.Scope != operator.ScopeGeneration || request.SourceClusterID != source.ClusterID || request.StatefulSet != source.StatefulSet || request.StatefulSetUID != source.StatefulSetUID || request.BindingUID != source.BindingUID || request.Namespace != source.Namespace || request.LogicalID != source.LogicalID || request.OperationID == "" || !sameTargets(request.Targets, source.Targets) {
				continue
			}
			if nested(item, "status", "proof") != nil {
				return false, fmt.Errorf("matching generation fence proof arrived before operator isolation")
			}
			matches = append(matches, pendingGCSFence{Name: text(nested(item, "metadata", "name")), Request: request})
		}
		if len(matches) > 1 {
			return false, fmt.Errorf("multiple exact generation fence requests exist")
		}
		if len(matches) == 1 {
			if matches[0].Name == "" {
				return false, fmt.Errorf("pending generation fence has no resource name")
			}
			pending = matches[0]
			return true, nil
		}
		return false, nil
	})
	return pending, err
}

func confirmPendingGCSFence(ctx context.Context, source gcsRecoverySource, pending pendingGCSFence) error {
	item, err := get(ctx, "rhizafence", pending.Name)
	if err != nil {
		return err
	}
	var request operator.FenceRequest
	if err := json.Unmarshal(encoded(nested(item, "spec")), &request); err != nil {
		return err
	}
	if !sameFenceRequest(request, pending.Request) || request.SourceClusterID != source.ClusterID || !sameTargets(request.Targets, source.Targets) || nested(item, "status", "proof") != nil {
		return fmt.Errorf("exact fence request is no longer pending without proof")
	}
	return nil
}

func waitForGCSFenceProof(ctx context.Context, source gcsRecoverySource, pending pendingGCSFence) error {
	return wait(ctx, "authenticated exact-scope GCS generation fence proof", func() (bool, error) {
		item, err := get(ctx, "rhizafence", pending.Name)
		if err != nil {
			return false, err
		}
		if !sameFenceRequestSpec(nested(item, "spec"), pending.Request) {
			return false, fmt.Errorf("generation fence request mutated after creation")
		}
		if nested(item, "status", "proof") == nil {
			return false, nil
		}
		_, _, err = verifyIndependentGenerationProof(item, source.ClusterID, source.StatefulSetUID, source.BindingUID, source.Targets, source.Bucket, source.Prefix, source.Namespace, source.StatefulSet, source.LogicalID)
		return err == nil, err
	})
}

func waitForOldGenerationReturn(ctx context.Context, source gcsRecoverySource, out string) (obj, error) {
	type observation struct {
		Pod       string `json:"pod"`
		PodUID    string `json:"podUID"`
		Restarted bool   `json:"restarted"`
		Unready   bool   `json:"unready"`
		Closed    bool   `json:"closedGenerationFatal"`
	}
	observations := make([]observation, len(source.Targets))
	err := wait(ctx, "same-PodUID restart rejects closed source generation", func() (bool, error) {
		for i, target := range source.Targets {
			pod, err := get(ctx, "pod", target.Pod)
			if err != nil {
				return false, err
			}
			uid := text(nested(pod, "metadata", "uid"))
			if uid != target.PodUID {
				return false, fmt.Errorf("source pod %s was replaced instead of restarted in place", target.Pod)
			}
			status := namedContainerStatus(pod, source.Container)
			if status == nil {
				return false, fmt.Errorf("source pod %s lost the bound application container status", target.Pod)
			}
			restarts := int(numberOrZero(nested(status, "restartCount")))
			ready := false
			conditions, _ := nested(pod, "status", "conditions").([]any)
			for _, condition := range conditions {
				ready = ready || nested(condition, "type") == "Ready" && nested(condition, "status") == "True"
			}
			obs := observation{Pod: target.Pod, PodUID: uid, Restarted: restarts > source.InitialRestart[uid], Unready: !ready}
			if !obs.Restarted || !obs.Unready {
				observations[i] = obs
				return false, nil
			}
			logs, err := command(ctx, "kubectl", "--context", kubeContext, "-n", namespace, "logs", target.Pod, "-c", source.Container, "--previous")
			if err != nil {
				return false, err
			}
			obs.Closed = strings.Contains(string(logs), "fencing: incarnation terminally fenced")
			observations[i] = obs
			if !obs.Closed {
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	if err := verifyGCSPinnedImages(ctx, source, "old-return-"+source.ClusterID, out); err != nil {
		return nil, fmt.Errorf("verify pinned images after old-Pod restart: %w", err)
	}
	evidence := obj{"status": "qualified", "source": source.ClusterID, "observations": observations}
	path := filepath.Join(out, "gcs-old-pod-return-"+source.ClusterID+".json")
	if err := os.WriteFile(path, encoded(evidence), 0600); err != nil {
		return nil, err
	}
	return evidence, nil
}

func namedContainerStatus(pod any, name string) any {
	statuses, _ := nested(pod, "status", "containerStatuses").([]any)
	for _, status := range statuses {
		if nested(status, "name") == name {
			return status
		}
	}
	return nil
}

func sameFenceRequest(actual, expected operator.FenceRequest) bool {
	return string(encoded(actual)) == string(encoded(expected))
}

func sameFenceRequestSpec(spec any, expected operator.FenceRequest) bool {
	var actual operator.FenceRequest
	if err := json.Unmarshal(encoded(spec), &actual); err != nil {
		return false
	}
	return sameFenceRequest(actual, expected)
}

func captureGCSRecoverySource(ctx context.Context, out string) (gcsRecoverySource, error) {
	var source gcsRecoverySource
	bindings, err := get(ctx, "rhizaclusters", "")
	if err != nil {
		return source, err
	}
	items, _ := bindings["items"].([]any)
	var binding any
	for _, candidate := range items {
		active := text(nested(candidate, "status", "activeClusterID"))
		if active == "" || nested(candidate, "status", "phase") != "Healthy" {
			continue
		}
		if binding != nil {
			return source, fmt.Errorf("more than one healthy active RhizaCluster exists in the run namespace")
		}
		binding = candidate
	}
	if binding == nil {
		return source, fmt.Errorf("no healthy active RhizaCluster exists in the run namespace")
	}
	source.Namespace = text(nested(binding, "metadata", "namespace"))
	if source.Namespace == "" {
		source.Namespace = namespace
	}
	source.BindingName = text(nested(binding, "metadata", "name"))
	source.LogicalID = text(nested(binding, "spec", "logicalID"))
	source.StatefulSet = text(nested(binding, "spec", "statefulSet"))
	source.Container = text(nested(binding, "spec", "container"))
	if source.BindingName == "" || source.LogicalID == "" || source.StatefulSet == "" || source.Container == "" {
		return source, fmt.Errorf("active RhizaCluster does not identify its logical workload")
	}
	sts, err := get(ctx, "statefulset", source.StatefulSet)
	if err != nil {
		return source, err
	}
	appContainer, err := boundContainer(sts, source.Container)
	if err != nil {
		return source, err
	}
	config, configMapName, err := sourceConfig(ctx, appContainer)
	if err != nil {
		return source, err
	}
	source.ConfigMap = configMapName
	configuredClusterID := config["RHIZA_CLUSTER_ID"]
	source.Bucket = config["RHIZA_OBJSTORE_BUCKET"]
	source.Endpoint = config["RHIZA_OBJSTORE_ENDPOINT"]
	basePrefix := strings.Trim(path.Clean(config["RHIZA_OBJSTORE_PREFIX"]), "/")
	if configuredClusterID == "" || source.Bucket == "" || basePrefix == "." || source.Endpoint != "" || config["RHIZA_OBJSTORE_PROVIDER"] != "gcs" || config["STORAGE_EMULATOR_HOST"] != "" {
		return source, fmt.Errorf("source is not configured for an explicit GCS object-store profile")
	}
	source.ClusterID = text(nested(binding, "status", "activeClusterID"))
	if source.ClusterID == "" || configuredClusterID != source.ClusterID || text(nested(binding, "status", "phase")) != "Healthy" {
		return source, fmt.Errorf("active RhizaCluster source differs from the configured workload generation")
	}
	source.Prefix = path.Join(basePrefix, source.ClusterID)
	source.StatefulSetUID = text(nested(sts, "metadata", "uid"))
	source.BindingUID = text(nested(binding, "metadata", "uid"))
	if source.StatefulSetUID == "" || source.BindingUID == "" || !podUsesConfigMap(nested(sts, "spec", "template"), source.ConfigMap) || !gcsWorkloadHasNoPVC(sts) {
		return source, fmt.Errorf("source workload and RhizaCluster identity are not bound")
	}
	replicasValue, ok := nested(sts, "spec", "replicas").(float64)
	if !ok {
		return source, fmt.Errorf("source voter count is unavailable")
	}
	replicas := int(replicasValue)
	if replicasValue != float64(replicas) || replicas < 1 || replicas > 64 {
		return source, fmt.Errorf("source voter count is outside the independent fence target limit")
	}
	pods, err := get(ctx, "pods", "")
	if err != nil {
		return source, err
	}
	selector, _ := nested(sts, "spec", "selector", "matchLabels").(map[string]any)
	if len(selector) == 0 {
		return source, fmt.Errorf("StatefulSet has no bound pod selector")
	}
	allPods, _ := pods["items"].([]any)
	source.InitialRestart = make(map[string]int)
	for _, pod := range allPods {
		if !labelsMatch(nested(pod, "metadata", "labels"), selector) || !gcsPodOwnedBy(pod, source.StatefulSetUID) {
			continue
		}
		name := text(nested(pod, "metadata", "name"))
		uid := text(nested(pod, "metadata", "uid"))
		if uid == "" || nested(pod, "metadata", "deletionTimestamp") != nil || !podUsesConfigMap(pod, source.ConfigMap) {
			return source, fmt.Errorf("source voter %s has no live Pod UID", name)
		}
		f, err := startForward(ctx, name, out)
		if err != nil {
			return source, err
		}
		status, statusErr := readMembership(ctx, f)
		f.close()
		if statusErr != nil || status.ClusterID != source.ClusterID || status.NodeID == "" || !status.Voting || status.Pending || status.WALIdentity == "" {
			return source, fmt.Errorf("source voter %s membership is not authenticated: %v", name, statusErr)
		}
		podObject, ok := pod.(obj)
		if !ok {
			return source, fmt.Errorf("source voter %s Pod record has an unexpected type", name)
		}
		target := capturedTarget(operator.FenceTarget{NodeID: name, Pod: name, PodUID: uid, WALIdentity: status.WALIdentity}, podObject)
		if target.NodeName == "" || target.ContainerID == "" || target.ImageID == "" {
			return source, fmt.Errorf("source voter %s runtime identity is incomplete", name)
		}
		source.Targets = append(source.Targets, target)
		statuses, _ := nested(pod, "status", "containerStatuses").([]any)
		if len(statuses) == 0 {
			return source, fmt.Errorf("source voter %s has no container restart baseline", name)
		}
		appStatus := namedContainerStatus(pod, source.Container)
		if appStatus == nil {
			return source, fmt.Errorf("source voter %s has no restart baseline for bound container %s", name, source.Container)
		}
		source.InitialRestart[uid] = int(numberOrZero(nested(appStatus, "restartCount")))
	}
	if len(source.Targets) != replicas {
		return source, fmt.Errorf("live StatefulSet voter cohort does not match its replica count")
	}
	source.OperatorIdentity, err = captureGCSOperatorPodIdentity(ctx)
	if err != nil {
		return source, err
	}
	source.OperatorPod = source.OperatorIdentity.Name
	if err := verifyGCSOperatorHTTPFencer(ctx, source.OperatorPod); err != nil {
		return source, err
	}
	source.FencerPod, err = captureGCSFencerPod(ctx)
	if err != nil {
		return source, err
	}
	source.AgentDaemonUID, source.AgentPods, err = captureGCSRuntimeAgentPods(ctx, source.Targets)
	if err != nil {
		return source, err
	}
	if err := verifyGCSPinnedImages(ctx, source, "source", out); err != nil {
		return source, err
	}
	return source, nil
}

func numberOrZero(value any) float64 {
	n, _ := value.(float64)
	return n
}

func ownerRefs(resource any) []any {
	owners, _ := nested(resource, "metadata", "ownerReferences").([]any)
	return owners
}

func gcsFaultSourceMatches(fault *gcsChaosFault, expected []gcsControlPodIdentity) bool {
	if fault == nil || len(fault.sourceUIDs) != len(expected) {
		return false
	}
	for _, identity := range expected {
		if fault.sourceUIDs[identity.Name] != identity.UID || identity.Name == "" || identity.UID == "" || identity.OwnerUID == "" {
			return false
		}
	}
	return true
}

func gcsControlPodSnapshotMatches(expected gcsControlPodIdentity, currentName, currentUID, currentOwnerUID, currentParentOwnerUID string) bool {
	return expected.Name == currentName && expected.UID != "" && expected.UID == currentUID && expected.OwnerUID != "" &&
		expected.OwnerUID == currentOwnerUID && expected.ParentOwnerUID == currentParentOwnerUID
}

func verifyGCSControlPodSnapshots(ctx context.Context, expected []gcsControlPodIdentity) error {
	if len(expected) == 0 {
		return fmt.Errorf("captured control Pod identity set is empty")
	}
	for _, identity := range expected {
		pod, err := get(ctx, "pod", identity.Name)
		if err != nil {
			return err
		}
		if text(nested(pod, "metadata", "uid")) != identity.UID || nested(pod, "metadata", "deletionTimestamp") != nil {
			return fmt.Errorf("control Pod %s incarnation changed", identity.Name)
		}
		ownerUID, ownerKind := "", ""
		for _, owner := range ownerRefs(pod) {
			if nested(owner, "controller") == true {
				ownerUID, ownerKind = text(nested(owner, "uid")), text(nested(owner, "kind"))
				break
			}
		}
		parentUID := ""
		if identity.ParentOwnerUID != "" {
			if ownerKind != "ReplicaSet" {
				return fmt.Errorf("operator Pod %s no longer has its captured ReplicaSet owner", identity.Name)
			}
			rs, err := get(ctx, "replicaset", identity.OwnerUID)
			if err != nil {
				return err
			}
			if text(nested(rs, "metadata", "uid")) != identity.OwnerUID {
				return fmt.Errorf("operator Pod %s ReplicaSet owner changed", identity.Name)
			}
			for _, owner := range ownerRefs(rs) {
				if nested(owner, "controller") == true && nested(owner, "kind") == "Deployment" {
					parentUID = text(nested(owner, "uid"))
				}
			}
		}
		if !gcsControlPodSnapshotMatches(identity, identity.Name, text(nested(pod, "metadata", "uid")), ownerUID, parentUID) {
			return fmt.Errorf("control Pod %s owner identity changed", identity.Name)
		}
	}
	return nil
}

func captureGCSOperatorPodIdentity(ctx context.Context) (gcsControlPodIdentity, error) {
	var result gcsControlPodIdentity
	deployment, err := get(ctx, "deployment", "rhiza-operator")
	if err != nil {
		return result, err
	}
	deploymentUID := text(nested(deployment, "metadata", "uid"))
	selector, _ := nested(deployment, "spec", "selector", "matchLabels").(map[string]any)
	if deploymentUID == "" || len(selector) == 0 || nested(deployment, "spec", "replicas") != float64(1) {
		return result, fmt.Errorf("operator Deployment must have one run-owned replica and exact selector")
	}
	sets, err := get(ctx, "replicasets", "")
	if err != nil {
		return result, err
	}
	items, _ := sets["items"].([]any)
	ownedRS := map[string]bool{}
	for _, rs := range items {
		for _, owner := range ownerRefs(rs) {
			if nested(owner, "kind") == "Deployment" && nested(owner, "uid") == deploymentUID && nested(owner, "controller") == true {
				ownedRS[text(nested(rs, "metadata", "uid"))] = true
			}
		}
	}
	pods, err := get(ctx, "pods", "")
	if err != nil {
		return result, err
	}
	var found []gcsControlPodIdentity
	allPods, _ := pods["items"].([]any)
	for _, pod := range allPods {
		if !labelsMatch(nested(pod, "metadata", "labels"), selector) || nested(pod, "metadata", "deletionTimestamp") != nil {
			continue
		}
		for _, owner := range ownerRefs(pod) {
			if nested(owner, "kind") == "ReplicaSet" && ownedRS[text(nested(owner, "uid"))] && nested(owner, "controller") == true {
				found = append(found, gcsControlPodIdentity{Name: text(nested(pod, "metadata", "name")), UID: text(nested(pod, "metadata", "uid")), OwnerUID: text(nested(owner, "uid")), ParentOwnerUID: deploymentUID})
			}
		}
	}
	if len(found) != 1 || found[0].Name == "" || found[0].UID == "" {
		return result, fmt.Errorf("operator Pod is not uniquely bound to the run-owned Deployment")
	}
	return found[0], nil
}

func verifyGCSOperatorHTTPFencer(ctx context.Context, podName string) error {
	pod, err := get(ctx, "pod", podName)
	if err != nil {
		return err
	}
	containers, _ := nested(pod, "spec", "containers").([]any)
	var operatorContainer any
	for _, container := range containers {
		if nested(container, "name") == "operator" {
			if operatorContainer != nil {
				return fmt.Errorf("operator Pod has ambiguous operator containers")
			}
			operatorContainer = container
		}
	}
	if operatorContainer == nil {
		return fmt.Errorf("operator Pod has no deployed operator container")
	}
	values := make(map[string]string)
	if err := mergeGCSOperatorEnvFrom(ctx, operatorContainer, values); err != nil {
		return err
	}
	env, _ := nested(operatorContainer, "env").([]any)
	for _, item := range env {
		name := text(nested(item, "name"))
		if name != "RHIZA_FENCER_BACKEND" && name != "RHIZA_FENCER_URL" && name != "RHIZA_FENCER_TOKEN_FILE" && name != "RHIZA_FENCER_CA_FILE" {
			continue
		}
		if value := nested(item, "value"); value != nil {
			stringValue, ok := value.(string)
			if !ok {
				return fmt.Errorf("operator %s environment value is not a string", name)
			}
			values[name] = stringValue
			continue
		}
		if key := text(nested(item, "valueFrom", "configMapKeyRef", "key")); key != "" {
			configMapName := text(nested(item, "valueFrom", "configMapKeyRef", "name"))
			configMap, err := get(ctx, "configmap", configMapName)
			if err != nil {
				return err
			}
			values[name] = text(nested(configMap, "data", key))
			continue
		}
		if key := text(nested(item, "valueFrom", "secretKeyRef", "key")); key != "" {
			secretName := text(nested(item, "valueFrom", "secretKeyRef", "name"))
			secret, err := get(ctx, "secret", secretName)
			if err != nil {
				return err
			}
			decoded, err := base64.StdEncoding.DecodeString(text(nested(secret, "data", key)))
			if err != nil {
				return fmt.Errorf("decode deployed operator %s environment reference: %w", name, err)
			}
			values[name] = string(decoded)
			continue
		}
		return fmt.Errorf("operator %s environment uses an unresolvable value source", name)
	}
	backend := strings.TrimSpace(values["RHIZA_FENCER_BACKEND"])
	if backend == "kubernetes" {
		return fmt.Errorf("GCS control-isolation scenario requires operator HTTP fencing; deployed operator explicitly selects Kubernetes")
	}
	if backend != "" && backend != "http" {
		return fmt.Errorf("deployed operator has an unsupported fencing backend %q", backend)
	}
	if strings.TrimSpace(values["RHIZA_FENCER_URL"]) == "" || strings.TrimSpace(values["RHIZA_FENCER_TOKEN_FILE"]) == "" {
		return fmt.Errorf("deployed operator does not have an effective HTTP fencer URL and token file")
	}
	return nil
}

func mergeGCSOperatorEnvFrom(ctx context.Context, container any, values map[string]string) error {
	refs, _ := nested(container, "envFrom").([]any)
	for _, ref := range refs {
		prefix := text(nested(ref, "prefix"))
		var data map[string]any
		if name := text(nested(ref, "configMapRef", "name")); name != "" {
			configMap, err := get(ctx, "configmap", name)
			if err != nil {
				if nested(ref, "configMapRef", "optional") == true {
					continue
				}
				return err
			}
			data, _ = nested(configMap, "data").(map[string]any)
		} else if name := text(nested(ref, "secretRef", "name")); name != "" {
			secret, err := get(ctx, "secret", name)
			if err != nil {
				if nested(ref, "secretRef", "optional") == true {
					continue
				}
				return err
			}
			encodedData, _ := nested(secret, "data").(map[string]any)
			data = make(map[string]any, len(encodedData))
			for key, value := range encodedData {
				decoded, err := base64.StdEncoding.DecodeString(text(value))
				if err != nil {
					return fmt.Errorf("decode operator envFrom Secret value: %w", err)
				}
				data[key] = string(decoded)
			}
		} else {
			return fmt.Errorf("operator envFrom has no resolvable ConfigMap or Secret source")
		}
		for _, key := range []string{"RHIZA_FENCER_BACKEND", "RHIZA_FENCER_URL", "RHIZA_FENCER_TOKEN_FILE", "RHIZA_FENCER_CA_FILE"} {
			if value, exists := data[key]; exists {
				values[prefix+key] = text(value)
			}
		}
	}
	return nil
}

func captureGCSFencerPod(ctx context.Context) (string, error) {
	deployment, err := get(ctx, "deployment", "rhiza-fencer")
	if err != nil {
		return "", err
	}
	deploymentUID := text(nested(deployment, "metadata", "uid"))
	selector, _ := nested(deployment, "spec", "selector", "matchLabels").(map[string]any)
	if deploymentUID == "" || len(selector) == 0 || nested(deployment, "spec", "replicas") != float64(1) {
		return "", fmt.Errorf("fencer Deployment must have one run-owned replica and exact selector")
	}
	sets, err := get(ctx, "replicasets", "")
	if err != nil {
		return "", err
	}
	items, _ := sets["items"].([]any)
	ownedRS := map[string]bool{}
	for _, rs := range items {
		for _, owner := range ownerRefs(rs) {
			if nested(owner, "kind") == "Deployment" && nested(owner, "uid") == deploymentUID && nested(owner, "controller") == true {
				ownedRS[text(nested(rs, "metadata", "uid"))] = true
			}
		}
	}
	pods, err := get(ctx, "pods", "")
	if err != nil {
		return "", err
	}
	var names []string
	allPods, _ := pods["items"].([]any)
	for _, pod := range allPods {
		if !labelsMatch(nested(pod, "metadata", "labels"), selector) || nested(pod, "metadata", "deletionTimestamp") != nil {
			continue
		}
		for _, owner := range ownerRefs(pod) {
			if nested(owner, "kind") == "ReplicaSet" && ownedRS[text(nested(owner, "uid"))] && nested(owner, "controller") == true {
				names = append(names, text(nested(pod, "metadata", "name")))
			}
		}
	}
	if len(names) != 1 || names[0] == "" {
		return "", fmt.Errorf("fencer Pod is not uniquely bound to the run-owned Deployment")
	}
	return names[0], nil
}

func captureGCSRuntimeAgentPods(ctx context.Context, targets []operator.FenceTarget) (string, []gcsControlPodIdentity, error) {
	daemonSet, err := get(ctx, "daemonset", "rhiza-runtime-agent")
	if err != nil {
		return "", nil, err
	}
	daemonSetUID := text(nested(daemonSet, "metadata", "uid"))
	selector, _ := nested(daemonSet, "spec", "selector", "matchLabels").(map[string]any)
	if daemonSetUID == "" || len(selector) == 0 {
		return "", nil, fmt.Errorf("runtime-agent DaemonSet identity is incomplete")
	}
	pods, err := get(ctx, "pods", "")
	if err != nil {
		return "", nil, err
	}
	byNode := make(map[string]gcsControlPodIdentity)
	allPods, _ := pods["items"].([]any)
	for _, pod := range allPods {
		if !labelsMatch(nested(pod, "metadata", "labels"), selector) || nested(pod, "metadata", "deletionTimestamp") != nil {
			continue
		}
		owned := false
		for _, owner := range ownerRefs(pod) {
			owned = owned || nested(owner, "kind") == "DaemonSet" && nested(owner, "uid") == daemonSetUID && nested(owner, "controller") == true
		}
		if !owned {
			continue
		}
		node := text(nested(pod, "spec", "nodeName"))
		name := text(nested(pod, "metadata", "name"))
		_, alreadyCaptured := byNode[node]
		if node == "" || name == "" || alreadyCaptured {
			return "", nil, fmt.Errorf("runtime-agent Pod identity is ambiguous on source node %s", node)
		}
		uid := text(nested(pod, "metadata", "uid"))
		if uid == "" {
			return "", nil, fmt.Errorf("runtime-agent Pod %s has no UID", name)
		}
		byNode[node] = gcsControlPodIdentity{Name: name, UID: uid, OwnerUID: daemonSetUID}
	}
	seenNodes := map[string]bool{}
	var identities []gcsControlPodIdentity
	for _, target := range targets {
		if target.NodeName == "" || seenNodes[target.NodeName] {
			continue
		}
		seenNodes[target.NodeName] = true
		identity := byNode[target.NodeName]
		if identity.Name == "" {
			return "", nil, fmt.Errorf("no runtime-agent Pod on captured voter node %s", target.NodeName)
		}
		identities = append(identities, identity)
	}
	if len(identities) == 0 {
		return "", nil, fmt.Errorf("source voter cohort has no runtime-agent Pod")
	}
	return daemonSetUID, identities, nil
}

func imageIDMatchesPinned(imageID, pinned string) bool {
	expected, err := immutableDigest(pinned)
	if err != nil || imageID == "" {
		return false
	}
	actual, err := immutableDigest(digest(imageID))
	return err == nil && actual == expected
}

func gcsChaosEvidence(fault *gcsChaosFault) obj {
	if fault == nil {
		return obj{"status": "missing"}
	}
	return obj{
		"name":       fault.name,
		"uid":        fault.uid,
		"namespace":  fault.namespace,
		"runID":      fault.runID,
		"mode":       fault.mode,
		"selector":   fault.selector,
		"target":     fault.target,
		"sourceUIDs": fault.sourceUIDs,
		"targetUIDs": fault.targetUIDs,
	}
}

func verifyGCSPodImage(pod any, containerName, pinned string) (obj, error) {
	name := text(nested(pod, "metadata", "name"))
	uid := text(nested(pod, "metadata", "uid"))
	if name == "" || uid == "" || nested(pod, "status", "phase") != "Running" || nested(pod, "metadata", "deletionTimestamp") != nil {
		return nil, fmt.Errorf("image evidence Pod is not a live Running incarnation")
	}
	containers, _ := nested(pod, "spec", "containers").([]any)
	var spec any
	for _, candidate := range containers {
		if nested(candidate, "name") == containerName {
			if spec != nil {
				return nil, fmt.Errorf("image evidence container is ambiguous")
			}
			spec = candidate
		}
	}
	if spec == nil || nested(spec, "image") != pinned {
		return nil, fmt.Errorf("Pod %s does not declare its exact pinned %s image", name, containerName)
	}
	status := namedContainerStatus(pod, containerName)
	imageID := text(nested(status, "imageID"))
	if status == nil || !imageIDMatchesPinned(imageID, pinned) {
		return nil, fmt.Errorf("Pod %s observed imageID does not match the pinned %s digest", name, containerName)
	}
	return obj{"pod": name, "uid": uid, "container": containerName, "image": pinned, "imageID": imageID}, nil
}

func verifyGCSPinnedImages(ctx context.Context, source gcsRecoverySource, phase, out string) error {
	appImage, operatorImage := os.Getenv("RHIZA_APP_IMAGE"), os.Getenv("RHIZA_OPERATOR_IMAGE")
	if _, err := immutableDigest(appImage); err != nil {
		return fmt.Errorf("RHIZA_APP_IMAGE must be digest-pinned: %w", err)
	}
	if _, err := immutableDigest(operatorImage); err != nil {
		return fmt.Errorf("RHIZA_OPERATOR_IMAGE must be digest-pinned: %w", err)
	}
	pods, err := get(ctx, "pods", "")
	if err != nil {
		return err
	}
	items, _ := pods["items"].([]any)
	byName := make(map[string]any, len(items))
	for _, pod := range items {
		byName[text(nested(pod, "metadata", "name"))] = pod
	}
	evidence := obj{"phase": phase, "runID": qualificationRunID, "sourceClusterID": source.ClusterID, "statefulSetUID": source.StatefulSetUID}
	var voterEvidence []obj
	for _, target := range source.Targets {
		pod := byName[target.Pod]
		if text(nested(pod, "metadata", "uid")) != target.PodUID {
			return fmt.Errorf("pinned-image voter Pod %s changed UID", target.Pod)
		}
		entry, err := verifyGCSPodImage(pod, source.Container, appImage)
		if err != nil {
			return err
		}
		voterEvidence = append(voterEvidence, entry)
	}
	evidence["voters"] = voterEvidence
	for _, rolePod := range []struct{ role, pod, container string }{{"operator", source.OperatorPod, "operator"}, {"fencer", source.FencerPod, "authority"}} {
		pod := byName[rolePod.pod]
		entry, err := verifyGCSPodImage(pod, rolePod.container, operatorImage)
		if err != nil {
			return fmt.Errorf("%s image inventory: %w", rolePod.role, err)
		}
		evidence[rolePod.role] = entry
	}
	daemonSet, err := get(ctx, "daemonset", "rhiza-runtime-agent")
	if err != nil {
		return err
	}
	if text(nested(daemonSet, "metadata", "uid")) != source.AgentDaemonUID || source.AgentDaemonUID == "" {
		return fmt.Errorf("runtime-agent DaemonSet UID changed after source capture")
	}
	if _, err := verifyGCSPodImage(obj{"metadata": obj{"name": "runtime-agent-template", "uid": source.AgentDaemonUID}, "status": obj{"phase": "Running"}, "spec": nested(daemonSet, "spec", "template", "spec")}, "runtime-agent", operatorImage); err != nil {
		return fmt.Errorf("runtime-agent DaemonSet image template: %w", err)
	}
	selector, _ := nested(daemonSet, "spec", "selector", "matchLabels").(map[string]any)
	owned := 0
	ready := 0
	var agentEvidence []obj
	for _, pod := range items {
		isOwned := false
		for _, owner := range ownerRefs(pod) {
			isOwned = isOwned || nested(owner, "kind") == "DaemonSet" && nested(owner, "uid") == source.AgentDaemonUID && nested(owner, "controller") == true
		}
		if !isOwned {
			continue
		}
		owned++
		if !labelsMatch(nested(pod, "metadata", "labels"), selector) {
			return fmt.Errorf("runtime-agent owned Pod is outside its DaemonSet selector")
		}
		entry, err := verifyGCSPodImage(pod, "runtime-agent", operatorImage)
		if err != nil {
			return fmt.Errorf("runtime-agent owned Pod image: %w", err)
		}
		conditions, _ := nested(pod, "status", "conditions").([]any)
		for _, condition := range conditions {
			ready = ready + btoi(nested(condition, "type") == "Ready" && nested(condition, "status") == "True")
		}
		agentEvidence = append(agentEvidence, entry)
	}
	desired := int(numberOrZero(nested(daemonSet, "status", "desiredNumberScheduled")))
	if desired == 0 || owned != desired || ready != desired {
		return fmt.Errorf("runtime-agent DaemonSet owned-pod inventory is incomplete: desired=%d owned=%d ready=%d", desired, owned, ready)
	}
	evidence["runtimeAgent"] = obj{"daemonSetUID": source.AgentDaemonUID, "desiredPods": desired, "pods": agentEvidence}
	return os.WriteFile(filepath.Join(out, "gcs-image-evidence-"+phase+".json"), encoded(evidence), 0600)
}

func btoi(value bool) int {
	if value {
		return 1
	}
	return 0
}

type gcsAcknowledgement struct {
	RequestID string `json:"requestID"`
	Slot      uint64 `json:"slot"`
	Rows      string `json:"rows"`
}

func writeGCSAcknowledgedRow(ctx context.Context, f *forward, token string, id int, value string) (gcsAcknowledgement, error) {
	var ack gcsAcknowledgement
	if id < 1 || value == "" || strings.ContainsAny(value, "'\r\n") {
		return ack, fmt.Errorf("acknowledged row identity is invalid")
	}
	requestID := fmt.Sprintf("gcs-ha-ack-%d", id)
	if _, err := call(ctx, f, "/sql/execute", obj{"request_id": requestID, "sql": fmt.Sprintf("INSERT INTO fencing_items VALUES (%d, '%s')", id, value)}, token); err != nil {
		return ack, err
	}
	var receipt struct {
		State   string `json:"state"`
		Receipt *struct {
			Slot   uint64 `json:"slot"`
			Status string `json:"status"`
		} `json:"receipt"`
	}
	status, err := call(ctx, f, "/request/status", obj{"kind": "sql", "request_id": requestID}, token)
	if err != nil {
		return ack, err
	}
	if err := json.Unmarshal(encoded(status), &receipt); err != nil || receipt.State != "committed" || receipt.Receipt == nil || receipt.Receipt.Status != "committed" || receipt.Receipt.Slot == 0 {
		return ack, fmt.Errorf("ACK request status lacks the exact committed receipt")
	}
	rows, err := call(ctx, f, "/sql/query", obj{"sql": fmt.Sprintf("SELECT id,value FROM fencing_items WHERE id = %d", id), "consistency": "linearizable"}, token)
	if err != nil {
		return ack, err
	}
	if string(encoded(rows["rows"])) != fmt.Sprintf("[[%d,%q]]", id, value) {
		return ack, fmt.Errorf("GCS scenario ACK row %d was not read back", id)
	}
	ack = gcsAcknowledgement{RequestID: requestID, Slot: receipt.Receipt.Slot, Rows: string(encoded(rows["rows"]))}
	return ack, nil
}

func boundContainer(statefulSet any, name string) (any, error) {
	containers, _ := nested(statefulSet, "spec", "template", "spec", "containers").([]any)
	var found any
	for _, candidate := range containers {
		if nested(candidate, "name") == name {
			if found != nil {
				return nil, fmt.Errorf("bound workload container name is ambiguous")
			}
			found = candidate
		}
	}
	if found == nil {
		return nil, fmt.Errorf("bound application container is absent from StatefulSet")
	}
	return found, nil
}

func sourceConfig(ctx context.Context, container any) (map[string]string, string, error) {
	config := make(map[string]string)
	refs, _ := nested(container, "envFrom").([]any)
	var profileConfigMap string
	for _, ref := range refs {
		name := text(nested(ref, "configMapRef", "name"))
		if name == "" {
			continue
		}
		cm, err := get(ctx, "configmap", name)
		if err != nil {
			return nil, "", err
		}
		data, _ := nested(cm, "data").(map[string]any)
		prefix := text(nested(ref, "prefix"))
		for key, value := range data {
			if _, ok := value.(string); !ok {
				continue
			}
			name := prefix + key
			if isGCSProfileKey(name) {
				config[name] = value.(string)
			}
		}
		if data["RHIZA_OBJSTORE_PROVIDER"] != nil {
			if profileConfigMap != "" && profileConfigMap != name {
				return nil, "", fmt.Errorf("multiple ConfigMaps define the object-store provider")
			}
			profileConfigMap = name
		}
	}
	env, _ := nested(container, "env").([]any)
	for _, entry := range env {
		key := text(nested(entry, "name"))
		if !isGCSProfileKey(key) {
			continue
		}
		if value, ok := nested(entry, "value").(string); ok && value != "" {
			config[key] = value
			continue
		}
		refName := text(nested(entry, "valueFrom", "configMapKeyRef", "name"))
		refKey := text(nested(entry, "valueFrom", "configMapKeyRef", "key"))
		if refName == "" || refKey == "" {
			continue
		}
		cm, err := get(ctx, "configmap", refName)
		if err != nil {
			return nil, "", err
		}
		if value := text(nested(cm, "data", refKey)); value != "" {
			config[key] = value
		}
		if key == "RHIZA_OBJSTORE_PROVIDER" {
			if profileConfigMap != "" && profileConfigMap != refName {
				return nil, "", fmt.Errorf("multiple ConfigMaps define the object-store provider")
			}
			profileConfigMap = refName
		}
	}
	if config["RHIZA_CLUSTER_ID"] == "" {
		return nil, "", fmt.Errorf("source workload environment does not set RHIZA_CLUSTER_ID")
	}
	return config, profileConfigMap, nil
}

func isGCSProfileKey(key string) bool {
	switch key {
	case "RHIZA_CLUSTER_ID", "RHIZA_OBJSTORE_PROVIDER", "RHIZA_OBJSTORE_BUCKET", "RHIZA_OBJSTORE_ENDPOINT", "RHIZA_OBJSTORE_PREFIX", "STORAGE_EMULATOR_HOST":
		return true
	default:
		return false
	}
}

func podUsesConfigMap(workload any, configMap string) bool {
	if configMap == "" {
		return false
	}
	spec := nested(workload, "spec", "template", "spec")
	if spec == nil {
		spec = nested(workload, "spec")
	}
	containers, _ := nested(spec, "containers").([]any)
	if len(containers) == 0 {
		return false
	}
	for _, container := range containers {
		from, _ := nested(container, "envFrom").([]any)
		for _, item := range from {
			if nested(item, "configMapRef", "name") == configMap {
				return true
			}
		}
		env, _ := nested(container, "env").([]any)
		for _, item := range env {
			if nested(item, "valueFrom", "configMapKeyRef", "name") == configMap {
				return true
			}
		}
	}
	return false
}

func labelsMatch(labelsValue any, required map[string]any) bool {
	labels, _ := labelsValue.(map[string]any)
	for key, value := range required {
		if labels[key] != value {
			return false
		}
	}
	return len(required) != 0
}

func gcsPodOwnedBy(pod any, stsUID string) bool {
	owners, _ := nested(pod, "metadata", "ownerReferences").([]any)
	for _, owner := range owners {
		if nested(owner, "kind") == "StatefulSet" && nested(owner, "uid") == stsUID && nested(owner, "controller") == true {
			return true
		}
	}
	return false
}

func gcsWorkloadHasNoPVC(sts any) bool {
	claims, _ := nested(sts, "spec", "volumeClaimTemplates").([]any)
	if len(claims) != 0 {
		return false
	}
	volumes, _ := nested(sts, "spec", "template", "spec", "volumes").([]any)
	allowedEphemeral := map[string]bool{"emptyDir": true, "configMap": true, "secret": true, "projected": true, "downwardAPI": true}
	for _, volume := range volumes {
		var sources []string
		for _, source := range []string{"emptyDir", "configMap", "secret", "projected", "downwardAPI", "persistentVolumeClaim", "hostPath", "csi", "ephemeral", "nfs", "local", "awsElasticBlockStore", "azureDisk", "azureFile", "cephfs", "cinder", "fc", "flexVolume", "flocker", "gcePersistentDisk", "glusterfs", "iscsi", "photonPersistentDisk", "portworxVolume", "quobyte", "rbd", "scaleIO", "storageos", "vsphereVolume"} {
			if nested(volume, source) != nil {
				sources = append(sources, source)
			}
		}
		if len(sources) != 1 || !allowedEphemeral[sources[0]] {
			return false
		}
	}
	return true
}

func waitForGCSRecovery(ctx context.Context, source gcsRecoverySource, expectedBase, expectedRows, out string) (*gcsRecoveryOutcome, error) {
	if source.ClusterID == "" || source.StatefulSetUID == "" || source.BindingUID == "" || source.Bucket == "" || source.Prefix == "" || len(source.Targets) == 0 || expectedBase != "fork" && expectedBase != "empty" || expectedRows == "" {
		return nil, fmt.Errorf("GCS recovery observation contract is incomplete")
	}
	outcome := &gcsRecoveryOutcome{Source: source}
	if err := wait(ctx, "IndependentV2 generation fence", func() (bool, error) {
		list, err := get(ctx, "rhizafences", "")
		if err != nil {
			return false, err
		}
		items, _ := list["items"].([]any)
		for _, item := range items {
			if nested(item, "spec", "scope") != string(operator.ScopeGeneration) || nested(item, "spec", "sourceClusterID") != source.ClusterID || nested(item, "spec", "statefulSetUID") != source.StatefulSetUID || nested(item, "spec", "bindingUID") != source.BindingUID || nested(item, "status", "proof") == nil {
				continue
			}
			var candidate operator.FenceRequest
			if err := json.Unmarshal(encoded(nested(item, "spec")), &candidate); err != nil {
				return false, err
			}
			if !sameTargets(candidate.Targets, source.Targets) {
				continue
			}
			request, proof, err := verifyIndependentGenerationProof(item, source.ClusterID, source.StatefulSetUID, source.BindingUID, source.Targets, source.Bucket, source.Prefix, source.Namespace, source.StatefulSet, source.LogicalID)
			if err != nil {
				return false, err
			}
			outcome.Fence, outcome.Proof = request, proof
			return true, nil
		}
		return false, nil
	}); err != nil {
		return nil, err
	}
	if err := wait(ctx, "committed recovered generation", func() (bool, error) {
		list, err := get(ctx, "rhizarecoveries", "")
		if err != nil {
			return false, err
		}
		items, _ := list["items"].([]any)
		for _, item := range items {
			if nested(item, "spec", "sourceClusterID") != source.ClusterID || nested(item, "spec", "recoveryID") != outcome.Fence.OperationID {
				continue
			}
			if err := json.Unmarshal(encoded(item), &outcome.Recovery); err != nil {
				return false, err
			}
			if outcome.Recovery.Status.Phase != "Complete" || outcome.Recovery.Status.Stage != "Complete" {
				return false, nil
			}
			if outcome.Recovery.Status.Source != source.ClusterID || outcome.Recovery.Status.StatefulSetUID != source.StatefulSetUID || outcome.Recovery.Status.TargetBaseKind != expectedBase || (expectedBase == "empty" && (outcome.Recovery.Status.RecoveredTip != 0 || outcome.Recovery.Status.ManifestHash != "")) {
				return false, fmt.Errorf("completed recovery differs from the captured source or expected archive base")
			}
			return true, nil
		}
		return false, nil
	}); err != nil {
		return nil, err
	}
	root, err := get(ctx, "rhizacluster", source.BindingName)
	if err != nil {
		return nil, err
	}
	if nested(root, "status", "phase") != "Healthy" || nested(root, "status", "activeClusterID") != outcome.Recovery.Status.Target {
		return nil, fmt.Errorf("controller root has not adopted the completed recovered target")
	}
	secretName := outcome.Recovery.Status.SecretName
	if secretName == "" {
		return nil, fmt.Errorf("completed recovery has no target admin Secret")
	}
	secret, err := get(ctx, "secret", secretName)
	if err != nil {
		return nil, err
	}
	adminBytes, err := base64.StdEncoding.DecodeString(text(nested(secret, "data", "admin")))
	if err != nil || len(adminBytes) == 0 {
		return nil, fmt.Errorf("recovered target admin credential is unavailable")
	}
	outcome.Admin = string(adminBytes)
	if len(outcome.Recovery.Status.TargetIdentities) == 0 || outcome.Recovery.Status.TargetIdentities[0].Pod == "" {
		return nil, fmt.Errorf("completed recovery has no concrete target voter identity")
	}
	outcome.Forward, err = startForward(ctx, outcome.Recovery.Status.TargetIdentities[0].Pod, out)
	if err != nil {
		return nil, err
	}
	status, err := readMembershipWithToken(ctx, outcome.Forward, outcome.Admin)
	if err != nil || status.ClusterID != outcome.Recovery.Status.Target || status.Pending || !status.Voting {
		outcome.Forward.close()
		return nil, fmt.Errorf("recovered voter did not pass authenticated membership and quorum read: %v", err)
	}
	rows, err := call(ctx, outcome.Forward, "/sql/query", obj{"sql": "SELECT id,value FROM fencing_items ORDER BY id", "consistency": "linearizable"}, outcome.Admin)
	if err != nil || string(encoded(rows["rows"])) != expectedRows {
		outcome.Forward.close()
		return nil, fmt.Errorf("recovered linearizable SQL readback differs: %v", err)
	}
	return outcome, nil
}

func readMembershipWithToken(ctx context.Context, f *forward, token string) (network.MembershipStatus, error) {
	data, err := call(ctx, f, "/membership/status", nil, token)
	var status network.MembershipStatus
	if err == nil {
		err = json.Unmarshal(encoded(data), &status)
	}
	return status, err
}

func verifyRecoveryLineage(previous, next operator.Resource, previousFence, nextFence operator.FenceRequest) error {
	if previous.Status.Phase != "Complete" || previous.Status.Stage != "Complete" || next.Status.Phase != "Complete" || next.Status.Stage != "Complete" ||
		previous.Status.StatefulSetUID == "" || previous.Status.StatefulSetUID != next.Status.StatefulSetUID ||
		previous.Status.Target == "" || next.Spec.SourceClusterID != previous.Status.Target || next.Status.Source != previous.Status.Target ||
		previousFence.OperationID == "" || nextFence.OperationID == "" || previousFence.OperationID == nextFence.OperationID ||
		previous.Status.Target == next.Status.Target || nextFence.SourceClusterID != previous.Status.Target {
		return fmt.Errorf("second recovery does not continue the completed target lineage")
	}
	if len(previous.Status.TargetIdentities) == 0 || len(nextFence.Targets) == 0 {
		return fmt.Errorf("second recovery lacks observed target incarnation identities")
	}
	previousUIDs := map[string]bool{}
	for _, identity := range previous.Status.TargetIdentities {
		if identity.PodUID == "" || identity.WALIdentity == "" {
			return fmt.Errorf("completed target identity is incomplete")
		}
		previousUIDs[identity.PodUID] = true
	}
	for _, target := range nextFence.Targets {
		if target.PodUID == "" || target.WALIdentity == "" || previousUIDs[target.PodUID] {
			return fmt.Errorf("second recovery reused a prior target Pod identity or omitted WAL identity")
		}
	}
	return nil
}

func verifyAsyncEmptyRecovery(resource operator.Resource, proof operator.FenceProof) error {
	if resource.Status.Phase != "Complete" || resource.Status.Stage != "Complete" || resource.Status.TargetBaseKind != "empty" ||
		resource.Status.SourceDurability != "async" || resource.Spec.Durability != "async" || !resource.Spec.AllowDataLoss ||
		resource.Status.RecoveredTip != 0 || resource.Status.ManifestHash != "" || proof.IndependentV2 == nil || proof.IndependentV2.StorageScope.Provider != "gcs" {
		return fmt.Errorf("async zero-tip recovery lacks explicit loss authorization or committed empty GCS lineage")
	}
	return nil
}

// verifyIndependentGenerationProof validates the issuer-published V2 metadata
// and its captured scope/target references; it does not fetch remote payloads.
func verifyIndependentGenerationProof(item any, sourceID, stsUID, bindingUID string, targets []operator.FenceTarget, bucket, prefix string, requestScope ...string) (operator.FenceRequest, operator.FenceProof, error) {
	var request operator.FenceRequest
	var proof operator.FenceProof
	if err := json.Unmarshal(encoded(nested(item, "spec")), &request); err != nil {
		return request, proof, err
	}
	if err := json.Unmarshal(encoded(nested(item, "status", "proof")), &proof); err != nil {
		return request, proof, err
	}
	statefulSet, requestNamespace, logicalID := "", "", ""
	if len(requestScope) == 3 {
		requestNamespace, statefulSet, logicalID = requestScope[0], requestScope[1], requestScope[2]
	}
	if request.SourceClusterID != sourceID || request.StatefulSet != statefulSet || request.StatefulSetUID != stsUID || request.BindingUID != bindingUID || request.Namespace != requestNamespace || request.LogicalID != logicalID || request.Scope != operator.ScopeGeneration || request.OperationID == "" || !sameTargets(request.Targets, targets) {
		return request, proof, fmt.Errorf("independent generation fence request differs from captured source cohort")
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(encoded(request)))
	evidence := proof.IndependentV2
	if evidence == nil || evidence.Version != 2 || evidence.RequestHash != hash || evidence.LedgerHash == "" || evidence.StorageScope.Provider != "gcs" || evidence.StorageScope.Bucket != bucket || evidence.StorageScope.Prefix != prefix || proof.ProofID != "independent-v2-"+fmt.Sprintf("%x", sha256.Sum256(encoded(evidence))) ||
		proof.Independent != nil || proof.OperationID != request.OperationID || proof.SourceClusterID != sourceID || proof.BindingUID != bindingUID || proof.RequestHash != hash || !proof.ProcessesTerminated || !proof.RecreationBlocked || !proof.StorageQuiesced || len(evidence.Terminals) == 0 || len(evidence.Terminals) > 64 || len(evidence.CurrentPodUIDs) > 64 {
		return request, proof, fmt.Errorf("fence does not contain a complete GCS IndependentV2 proof")
	}
	if _, err := immutableDigest("sha256:" + evidence.LedgerHash); err != nil {
		return request, proof, fmt.Errorf("IndependentV2 ledger reference is invalid: %w", err)
	}
	scopeDigest := fmt.Sprintf("%x", sha256.Sum256(encoded(evidence.StorageScope)))
	currentUIDs := map[string]bool{}
	for _, uid := range evidence.CurrentPodUIDs {
		if uid == "" || currentUIDs[uid] {
			return request, proof, fmt.Errorf("IndependentV2 current Pod inventory is invalid")
		}
		currentUIDs[uid] = true
	}
	for _, ref := range evidence.Terminals {
		if ref.EvidenceVersion != 2 || ref.Request.OperationID == "" || ref.Request.Identity.SourceClusterID != sourceID || ref.Request.Identity.NodeID == "" || ref.Request.Identity.PodUID == "" || ref.Request.Identity.RunID == "" || ref.Request.Identity.WALIdentity == "" || ref.ProducerUID == "" || ref.ProofName == "" || ref.ProofUID == "" || ref.StorageScopeDigest != scopeDigest {
			return request, proof, fmt.Errorf("IndependentV2 terminal reference is incomplete")
		}
		for _, digest := range []string{ref.Request.RequestHash, ref.RuntimeHash, ref.ProofDigest, ref.RuntimeReceiptDigest, ref.MutationReceiptDigest} {
			if _, err := immutableDigest("sha256:" + digest); err != nil {
				return request, proof, fmt.Errorf("IndependentV2 terminal digest is invalid")
			}
		}
	}
	for _, target := range targets {
		covered := false
		for _, ref := range evidence.Terminals {
			id := ref.Request.Identity
			if ref.Request.OperationID != "" && id.SourceClusterID == sourceID && id.NodeID == target.NodeID && id.PodUID == target.PodUID && id.WALIdentity == target.WALIdentity && ref.EvidenceVersion == 2 && ref.StorageScopeDigest == scopeDigest {
				covered = true
				break
			}
		}
		if !covered {
			return request, proof, fmt.Errorf("IndependentV2 terminals omit captured source incarnation %s", target.NodeID)
		}
	}
	return request, proof, nil
}
