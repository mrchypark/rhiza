package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/fencing/cri"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

// qualifyGCSDeadPod exercises automatic single-voter replacement on an
// explicitly GCS-backed, no-PVC workload. It is deliberately separate from
// the generation-wide ACK-loss qualification.
func qualifyGCSDeadPod(ctx context.Context) (resultErr error) {
	out := os.Getenv("FENCING_E2E_OUT")
	if out == "" {
		out = "gcs-ha-dead-pod-results"
	}
	if err := os.MkdirAll(out, 0700); err != nil {
		return err
	}
	if info, err := os.Lstat(out); err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("GCS dead-Pod result directory must be private")
	}
	events, err := os.OpenFile(filepath.Join(out, "gcs-dead-pod-events.jsonl"), os.O_CREATE|os.O_EXCL|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	var eventErr error
	event := func(name string, data any) {
		if eventErr == nil {
			_, eventErr = fmt.Fprintln(events, string(encoded(obj{"event": name, "time": time.Now().UTC(), "data": data})))
		}
	}
	var fault *gcsDeadPodFault
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		if fault != nil {
			if err := fault.Restore(cleanupCtx); err != nil {
				resultErr = errors.Join(resultErr, fmt.Errorf("restore exact GCS dead-Pod Chaos UID: %w", err))
			}
			event("pod_chaos_cleanup", obj{"name": fault.name, "uid": fault.uid})
		}
		if err := events.Close(); err != nil {
			resultErr = errors.Join(resultErr, err)
		}
		if eventErr != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("write dead-Pod event record: %w", eventErr))
		}
	}()

	source, err := captureGCSRecoverySource(ctx, out)
	if err != nil {
		return err
	}
	if source.Durability != "before-ack" || len(source.Targets) != 3 {
		return fmt.Errorf("GCS dead-Pod qualification requires three voters and before-ACK durability")
	}
	if err := verifyGCSDeadPodPassiveAgentProfile(ctx, source); err != nil {
		return fmt.Errorf("passive dead-Pod profile preflight (before fault injection): %w", err)
	}
	binding, err := get(ctx, "rhizacluster", source.BindingName)
	if err != nil {
		return err
	}
	sts, err := get(ctx, "statefulset", source.StatefulSet)
	if err != nil {
		return err
	}
	appContainer, err := boundContainer(sts, source.Container)
	if err != nil {
		return err
	}
	automatic, err := effectiveGCSSetting(ctx, appContainer, "RHIZA_AUTOMATIC_RECOVERY")
	if err != nil || nested(binding, "spec", "automatic") != true || nested(binding, "spec", "policy", "allowDataLoss") == true || automatic != "true" {
		return fmt.Errorf("GCS dead-Pod qualification requires safe automatic voter recovery")
	}
	adminToken, err := sourceAdminToken(ctx, source.StatefulSet, source.Container)
	if err != nil {
		return err
	}
	forward, err := startForward(ctx, source.Targets[1].Pod, out)
	if err != nil {
		return err
	}
	defer func() {
		if forward != nil {
			forward.close()
		}
	}()
	if _, err := call(ctx, forward, "/sql/execute", obj{"request_id": "gcs-dead-pod-create-table", "sql": "CREATE TABLE fencing_items (id INTEGER PRIMARY KEY, value TEXT)"}, adminToken); err != nil {
		return fmt.Errorf("create dead-Pod baseline table: %w", err)
	}
	baseline, err := writeGCSAcknowledgedRow(ctx, forward, adminToken, 1, "gcs-dead-pod-baseline")
	if err != nil {
		return fmt.Errorf("establish read-back baseline before Pod kill: %w", err)
	}

	// Capture the canonical source-side runtime tuple before the fault. PID and
	// cgroup details remain inside the existing authority/provider path.
	fencerDeployment, err := get(ctx, "deployment", "rhiza-fencer")
	if err != nil {
		return err
	}
	readScope, err := newGCSAckLossReadScope(ctx, source, fencerDeployment)
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
	if _, _, err := stateStore.ReadRoot(ctx, source.StatefulSetUID, source.ClusterID); err != nil {
		return fmt.Errorf("authenticated read-only source authority cannot read canonical root")
	}
	if _, err := readBucket.Attributes(ctx, path.Join(source.Prefix, "archive/head.bin")); err != nil {
		return fmt.Errorf("authenticated read-only source authority cannot read canonical archive HEAD")
	}
	target := source.Targets[0]
	oldPod, err := get(ctx, "pod", target.Pod)
	if err != nil || text(nested(oldPod, "metadata", "uid")) != target.PodUID {
		return fmt.Errorf("captured voter Pod changed before same-ordinal recovery: %v", err)
	}
	oldCredentialUID := text(nested(oldPod, "metadata", "annotations", "rhiza.mrchypark.dev/fencing-credential-uid"))
	if oldCredentialUID == "" {
		return fmt.Errorf("captured voter has no immutable peer-credential UID")
	}
	if err := requireNoPriorGCSVoterFence(ctx, source, target); err != nil {
		return err
	}
	runtime, producerUID, err := captureGCSAckLossRuntime(ctx, stateStore, source, target.PodUID)
	if err != nil {
		return err
	}
	if runtime.NamespaceUID != source.NamespaceUID || runtime.BindingUID != source.BindingUID || runtime.StatefulSetUID != source.StatefulSetUID ||
		runtime.SourceClusterID != source.ClusterID || runtime.PodUID != target.PodUID || runtime.NodeID != target.NodeID ||
		runtime.NodeName != target.NodeName || runtime.NodeUID != target.NodeUID || runtime.ContainerID != target.ContainerID || runtime.ImageID != target.ImageID {
		return fmt.Errorf("canonical runtime enrollment differs from the exact captured Pod and container identity")
	}
	if _, err := immutableDigest("sha256:" + runtime.CapturedScopeDigest); err != nil {
		return fmt.Errorf("captured source runtime has no valid scope digest")
	}
	event("source_bound", obj{"source": source.ClusterID, "namespaceUID": source.NamespaceUID, "statefulSetUID": source.StatefulSetUID,
		"bindingUID": source.BindingUID, "target": target, "runtimeScopeDigest": runtime.CapturedScopeDigest, "producerUID": producerUID, "baselineAck": baseline})

	faultStarted := time.Now().UTC().Truncate(time.Second)
	fault, err = createGCSDeadPodFault(ctx, source, target)
	if err != nil {
		return err
	}
	if err := fault.WaitInjected(ctx); err != nil {
		return err
	}
	event("exact_voter_pod_kill_injected", obj{"chaosUID": fault.uid, "pod": target.Pod, "podUID": target.PodUID, "podChaos": fault.injected})
	if err := waitForGCSVoterFence(ctx, source, target, runtime, producerUID, faultStarted); err != nil {
		return fmt.Errorf("exact voter IndependentV2 proof: %w", err)
	}
	request, proof, fenceObject, err := captureGCSVoterFence(ctx, source, target, runtime, producerUID, faultStarted)
	if err != nil {
		return err
	}
	terminalRef, err := findGCSDeadPodTerminalRef(proof, source, target, runtime, producerUID)
	if err != nil {
		return err
	}
	passiveReceipt, err := verifyGCSDeadPodPassiveTerminal(ctx, stateStore, evidenceStore, source, target, runtime, producerUID, terminalRef)
	if err != nil {
		return fmt.Errorf("exact passive dead-Pod terminal receipt: %w", err)
	}
	event("passive_terminal_receipt_verified", passiveReceipt)
	event("voter_fence_proven", obj{"fence": fenceObject, "request": request, "proof": proof})

	checkpoint, err := waitForGCSVoterRemoveAckBeforeAdd(ctx, source, target, request, proof, forward, adminToken)
	if err != nil {
		return err
	}
	// Re-read immediately before deleting the captured Pod. The controller may
	// advance while this driver polls; if ADD is already journaled, fail rather
	// than claiming an operator restart in the REMOVE-before-ADD window.
	latestCheckpoint, err := waitForGCSVoterRemoveAckBeforeAdd(ctx, source, target, request, proof, forward, adminToken)
	if err != nil {
		return err
	}
	if text(latestCheckpoint.Metadata["uid"]) != text(checkpoint.Metadata["uid"]) || latestCheckpoint.Status.Membership == nil ||
		checkpoint.Status.Membership == nil || latestCheckpoint.Status.Membership.RemoveRequest == nil || checkpoint.Status.Membership.RemoveRequest == nil ||
		!bytes.Equal(encoded(latestCheckpoint.Status.Membership.RemoveRequest), encoded(checkpoint.Status.Membership.RemoveRequest)) {
		return fmt.Errorf("canonical REMOVE checkpoint changed before operator restart")
	}
	checkpoint = latestCheckpoint
	event("durable_remove_before_add_checkpoint", obj{"recoveryUID": text(checkpoint.Metadata["uid"]),
		"removeRequest": checkpoint.Status.Membership.RemoveRequest, "operatorUID": source.OperatorIdentity.UID})
	if err := restartGCSOperatorAtRemoveCheckpoint(ctx, source); err != nil {
		return fmt.Errorf("restart exact operator Pod between durable REMOVE and ADD: %w", err)
	}
	event("operator_restarted_between_remove_and_add", obj{"recoveryUID": text(checkpoint.Metadata["uid"]),
		"previousOperatorUID": source.OperatorIdentity.UID})
	postRestartCheckpoint, err := waitForGCSVoterRemoveAckBeforeAdd(ctx, source, target, request, proof, forward, adminToken)
	if err != nil {
		return fmt.Errorf("operator restart did not preserve the REMOVE-before-ADD checkpoint: %w", err)
	}
	if text(postRestartCheckpoint.Metadata["uid"]) != text(checkpoint.Metadata["uid"]) || postRestartCheckpoint.Status.Membership == nil ||
		postRestartCheckpoint.Status.Membership.Add != nil || postRestartCheckpoint.Status.Membership.Phase != "Removing" ||
		postRestartCheckpoint.Status.Membership.RemoveRequest == nil || checkpoint.Status.Membership == nil || checkpoint.Status.Membership.RemoveRequest == nil ||
		!bytes.Equal(encoded(postRestartCheckpoint.Status.Membership.RemoveRequest), encoded(checkpoint.Status.Membership.RemoveRequest)) {
		return fmt.Errorf("operator restart changed the canonical Recovery or advanced beyond exact durable REMOVE")
	}
	// Re-read the same Recovery after restart. Its durable REMOVE request must
	// survive unchanged; only then may the controller publish and dispatch ADD.
	recovery, err := waitForGCSVoterRecovery(ctx, source, target, request, proof)
	if err != nil {
		return err
	}
	if text(checkpoint.Metadata["uid"]) == "" || text(recovery.Metadata["uid"]) != text(checkpoint.Metadata["uid"]) ||
		checkpoint.Status.Membership == nil || checkpoint.Status.Membership.RemoveRequest == nil || recovery.Status.Membership == nil ||
		recovery.Status.Membership.RemoveRequest == nil || !bytes.Equal(encoded(checkpoint.Status.Membership.RemoveRequest), encoded(recovery.Status.Membership.RemoveRequest)) {
		return fmt.Errorf("operator restart changed the canonical Recovery or its exact durable REMOVE request")
	}
	event("single_voter_recovery_complete", recovery)
	if err := fault.Restore(ctx); err != nil {
		return err
	}
	fault = nil

	// Confirm the source-authorized replacement uses this exact ordinal with a
	// fresh Pod UID, logical NodeID, WAL, and immutable credential.
	replacement, err := verifyGCSVoterMembership(ctx, source, target, oldCredentialUID, recovery, forward, adminToken, out)
	if err != nil {
		return err
	}
	event("old_logical_identity_closed", obj{"oldNodeID": target.NodeID, "oldWALIdentity": target.WALIdentity,
		"oldCredentialUID": oldCredentialUID, "replacementNodeID": replacement.NodeID, "replacementWALIdentity": replacement.WALIdentity,
		"replacementCredentialUID": recovery.Status.Membership.Add.SecretUID, "oldNodeAbsent": true})
	postAck, err := writeGCSAcknowledgedRow(ctx, forward, adminToken, 2, "gcs-dead-pod-post-recovery")
	if err != nil {
		return fmt.Errorf("post-recovery ACK and linearizable readback: %w", err)
	}
	rows, err := call(ctx, forward, "/sql/query", obj{"sql": "SELECT id,value FROM fencing_items ORDER BY id", "consistency": "linearizable"}, adminToken)
	if err != nil || string(encoded(rows["rows"])) != `[[1,"gcs-dead-pod-baseline"],[2,"gcs-dead-pod-post-recovery"]]` {
		return fmt.Errorf("post-recovery linearizable readback differs: %v", err)
	}
	event("post_recovery_ack_durable", obj{"ack": postAck, "rows": rows["rows"]})

	// Restart only the supervised application child inside the same admitted
	// Pod. The exact Pod UID and WAL must survive; the container/runtime run must
	// be new and the replacement must reauthenticate as its assigned voter.
	beforeRestartPod, err := get(ctx, "pod", replacement.Pod)
	if err != nil || text(nested(beforeRestartPod, "metadata", "uid")) != replacement.PodUID {
		return fmt.Errorf("same-ordinal voter changed before active restart: %v", err)
	}
	beforeRestartStatus := namedContainerStatus(beforeRestartPod, source.Container)
	if beforeRestartStatus == nil {
		return fmt.Errorf("same-ordinal active container status is unavailable before restart")
	}
	beforeContainerID := text(nested(beforeRestartStatus, "containerID"))
	beforeRestartCount := numberOrZero(nested(beforeRestartStatus, "restartCount"))
	beforeRunID, err := currentGCSRuntimeRunID(ctx, stateStore, source, replacement, beforeContainerID)
	if err != nil {
		return fmt.Errorf("capture active same-ordinal runtime run before restart: %w", err)
	}
	if err := restartGCSDeadPodChild(ctx, source, replacement, beforeRestartCount, beforeContainerID); err != nil {
		return fmt.Errorf("restart same-ordinal active voter child: %w", err)
	}
	forward.close()
	forward, err = startForward(ctx, replacement.Pod, out)
	if err != nil {
		return err
	}
	postRestart, err := waitForGCSActiveSameOrdinalRestart(ctx, stateStore, source, replacement, recovery.Status.TargetIdentities, target.NodeID,
		forward, adminToken, beforeRestartCount, beforeContainerID, beforeRunID)
	if err != nil {
		return err
	}
	ackAfterRestart, err := writeGCSAcknowledgedRow(ctx, forward, adminToken, 3, "gcs-dead-pod-post-restart")
	if err != nil {
		return fmt.Errorf("post-restart ACK and linearizable readback: %w", err)
	}
	rows, err = call(ctx, forward, "/sql/query", obj{"sql": "SELECT id,value FROM fencing_items ORDER BY id", "consistency": "linearizable"}, adminToken)
	if err != nil || string(encoded(rows["rows"])) != `[[1,"gcs-dead-pod-baseline"],[2,"gcs-dead-pod-post-recovery"],[3,"gcs-dead-pod-post-restart"]]` {
		return fmt.Errorf("post-restart linearizable readback differs: %v", err)
	}
	event("same_ordinal_active_restart_verified", obj{"identity": replacement, "restart": postRestart, "ack": ackAfterRestart, "rows": rows["rows"]})
	evidence := obj{"qualification": "gcs-noPVC-single-voter-dead-pod", "status": "qualified",
		"scope":  "one exact captured voter; same StatefulSet ordinal promoted under a fresh logical identity and verified across an active child restart",
		"source": source, "oldIncarnation": target, "runtimeScopeDigest": runtime.CapturedScopeDigest,
		"baselineAck": baseline, "fenceRequest": request, "independentV2Proof": proof,
		"recovery": recovery, "replacement": replacement, "postRecoveryAck": postAck, "activeRestart": postRestart,
		"postRestartAck": ackAfterRestart, "postRestartRows": rows["rows"]}
	if err := os.WriteFile(filepath.Join(out, "gcs-dead-pod-evidence.json"), encoded(evidence), 0600); err != nil {
		return err
	}
	return nil
}

const gcsVoterRemoveAcceptedMessage = "remove request accepted; waiting for configuration confirmation"

func waitForGCSVoterRemoveAckBeforeAdd(ctx context.Context, source gcsRecoverySource, target operator.FenceTarget,
	request operator.FenceRequest, proof operator.FenceProof, forward *forward, token string) (operator.Resource, error) {
	var checkpoint operator.Resource
	err := wait(ctx, "durable voter REMOVE acknowledged before ADD journal", func() (bool, error) {
		list, err := get(ctx, "rhizarecoveries", "")
		if err != nil {
			return false, err
		}
		items, _ := list["items"].([]any)
		matches := 0
		for _, item := range items {
			if nested(item, "spec", "sourceClusterID") != source.ClusterID || nested(item, "spec", "recoveryID") != request.OperationID {
				continue
			}
			matches++
			if err := json.Unmarshal(encoded(item), &checkpoint); err != nil {
				return false, err
			}
		}
		if matches > 1 {
			return false, fmt.Errorf("multiple Recovery objects match the exact voter operation")
		}
		if matches != 1 {
			return false, nil
		}
		// Resolve the deterministic automatic-child name directly as a second
		// API read. The UID returned for that exact, owner-bound object is the
		// expected identity for the checkpoint matcher; a nonempty UID from a
		// list item is not sufficient.
		identity, err := get(ctx, "rhizarecovery", request.OperationID)
		if err != nil {
			return false, err
		}
		expectedRecoveryUID := text(nested(identity, "metadata", "uid"))
		if text(nested(identity, "metadata", "name")) != request.OperationID ||
			text(nested(identity, "metadata", "namespace")) != source.Namespace || expectedRecoveryUID == "" ||
			nested(identity, "metadata", "annotations", "rhiza.mrchypark.dev/automatic-owner") != source.BindingUID ||
			nested(identity, "metadata", "annotations", "rhiza.mrchypark.dev/automatic-logical") != source.LogicalID ||
			text(checkpoint.Metadata["uid"]) != expectedRecoveryUID {
			return false, fmt.Errorf("Recovery checkpoint UID differs from the exact owner-bound automatic child")
		}
		membership, err := readMembershipWithToken(ctx, forward, token)
		if err != nil {
			return false, nil
		}
		if gcsVoterRemoveAckCheckpointMatches(checkpoint, expectedRecoveryUID, source, target, request, proof, source.Targets[1], membership) {
			return true, nil
		}
		if checkpoint.Status.Membership != nil && (checkpoint.Status.Membership.Add != nil || checkpoint.Status.Membership.Phase == "Adding" || checkpoint.Status.Membership.Phase == "Complete") {
			return false, fmt.Errorf("durable REMOVE-before-ADD checkpoint was missed; ADD is already journaled")
		}
		return false, nil
	})
	return checkpoint, err
}

func gcsVoterRemoveAckCheckpointMatches(recovery operator.Resource, expectedRecoveryUID string, source gcsRecoverySource, target operator.FenceTarget,
	request operator.FenceRequest, proof operator.FenceProof, observer operator.FenceTarget, membership network.MembershipStatus) bool {
	if expectedRecoveryUID == "" || text(recovery.Metadata["uid"]) != expectedRecoveryUID || recovery.Spec.SourceClusterID != source.ClusterID || recovery.Spec.RecoveryID != request.OperationID ||
		recovery.Spec.StatefulSet != source.StatefulSet || recovery.Status.StatefulSetUID != source.StatefulSetUID || recovery.Status.Source != source.ClusterID ||
		recovery.Status.Phase != "Removing" || recovery.Status.Message != gcsVoterRemoveAcceptedMessage || recovery.Status.Membership == nil ||
		recovery.Status.Membership.Phase != "Removing" || recovery.Status.Membership.Add != nil || recovery.Status.Membership.RemoveRequest == nil {
		return false
	}
	if recovery.Spec.Fence.RecoveryID != request.OperationID || recovery.Spec.Fence.ClusterID != source.ClusterID ||
		recovery.Spec.Fence.StatefulSetUID != source.StatefulSetUID || !recovery.Spec.Fence.Confirmed || recovery.Spec.Fence.Evidence != proof.ProofID {
		return false
	}
	specMembership := recovery.Spec.Membership
	if specMembership == nil || specMembership.OperationID != request.OperationID || specMembership.Remove != quepaxa.NodeID(target.NodeID) ||
		specMembership.RemovedPod != target.Pod || specMembership.ReplacementPod != target.Pod || specMembership.ReplacementSecret == "" ||
		specMembership.Fence.NodeID != quepaxa.NodeID(target.NodeID) || specMembership.Fence.WALIdentity != target.WALIdentity ||
		specMembership.Fence.WorkloadUID != target.PodUID || !specMembership.Fence.Confirmed || specMembership.Fence.Evidence != proof.ProofID ||
		len(specMembership.VoterPods) != 2 {
		return false
	}
	wantPods := map[string]string{}
	for _, captured := range source.Targets {
		if captured.PodUID != target.PodUID {
			wantPods[captured.NodeID] = captured.Pod
		}
	}
	if len(wantPods) != 2 {
		return false
	}
	for _, voter := range specMembership.VoterPods {
		if wantPods[string(voter.NodeID)] != voter.Pod {
			return false
		}
		delete(wantPods, string(voter.NodeID))
	}
	if len(wantPods) != 0 {
		return false
	}
	remove := recovery.Status.Membership.RemoveRequest
	if remove.OperationID != request.OperationID || remove.ClusterID != source.ClusterID || remove.ExpectedConfigID == 0 || remove.Remove != quepaxa.NodeID(target.NodeID) ||
		remove.Fence == nil || string(remove.Fence.NodeID) != target.NodeID || remove.Fence.WALIdentity != target.WALIdentity ||
		remove.Fence.WorkloadUID != target.PodUID || !remove.Fence.Confirmed || remove.Fence.Evidence != proof.ProofID {
		return false
	}
	if membership.ClusterID != source.ClusterID || membership.NodeID != quepaxa.NodeID(observer.NodeID) || membership.WALIdentity != observer.WALIdentity ||
		!membership.Voting || membership.Pending || membership.AbortSlot != 0 || membership.ConfigID != remove.ExpectedConfigID+1 || len(membership.Voters) != 2 {
		return false
	}
	want := map[string]bool{}
	for _, captured := range source.Targets {
		if captured.PodUID != target.PodUID {
			want[captured.NodeID] = true
		}
	}
	if len(want) != 2 {
		return false
	}
	for _, voter := range membership.Voters {
		if !want[string(voter)] {
			return false
		}
		delete(want, string(voter))
	}
	return len(want) == 0
}

func restartGCSOperatorAtRemoveCheckpoint(ctx context.Context, source gcsRecoverySource) error {
	if source.OperatorPod == "" || source.OperatorIdentity.Name != source.OperatorPod || source.OperatorIdentity.UID == "" ||
		source.OperatorIdentity.ParentOwnerUID == "" {
		return fmt.Errorf("captured run-owned operator Pod identity is incomplete")
	}
	if err := verifyGCSControlPodSnapshots(ctx, []gcsControlPodIdentity{source.OperatorIdentity}); err != nil {
		return fmt.Errorf("operator Pod changed before restart: %w", err)
	}
	if err := deleteObjectWithUIDAndGracePeriod(ctx, "pods", source.OperatorIdentity.Name, source.OperatorIdentity.UID, 0); err != nil {
		return fmt.Errorf("delete captured test operator Pod: %w", err)
	}
	var restarted gcsControlPodIdentity
	err := wait(ctx, "replacement run-owned operator Pod after exact deletion", func() (bool, error) {
		current, err := captureGCSOperatorPodIdentity(ctx)
		if err != nil {
			return false, nil
		}
		if current.ParentOwnerUID != source.OperatorIdentity.ParentOwnerUID {
			return false, fmt.Errorf("operator replacement escaped the captured Deployment")
		}
		if current.UID == source.OperatorIdentity.UID {
			return false, nil
		}
		pod, err := get(ctx, "pod", current.Name)
		if err != nil || !podReady(pod) {
			return false, nil
		}
		if err := verifyGCSOperatorHTTPFencer(ctx, current.Name); err != nil {
			return false, err
		}
		restarted = current
		return true, nil
	})
	if err != nil {
		return err
	}
	if restarted.UID == "" || restarted.UID == source.OperatorIdentity.UID {
		return fmt.Errorf("operator restart did not produce a fresh Pod UID")
	}
	return nil
}

func verifyGCSDeadPodPassiveAgentProfile(ctx context.Context, source gcsRecoverySource) error {
	profile, err := get(ctx, "configmap", "rhiza-runtime-agent-profile")
	if err != nil {
		return fmt.Errorf("read installed runtime-agent profile: %w", err)
	}
	daemonSet, err := get(ctx, "daemonset", runtimeAgentDSName)
	if err != nil {
		return fmt.Errorf("read installed runtime-agent DaemonSet: %w", err)
	}
	pods, err := get(ctx, "pods", "")
	if err != nil {
		return fmt.Errorf("read runtime-agent Pods: %w", err)
	}
	return validateGCSDeadPodPassiveAgentProfile(profile, daemonSet, pods, source)
}

func validateGCSDeadPodPassiveAgentProfile(profile, daemonSet, pods obj, source gcsRecoverySource) error {
	const profileName = "rhiza-runtime-agent-profile"
	if text(nested(profile, "metadata", "name")) != profileName || text(nested(profile, "metadata", "namespace")) != source.Namespace ||
		text(nested(profile, "metadata", "uid")) == "" || text(nested(profile, "metadata", "resourceVersion")) == "" {
		return fmt.Errorf("installed runtime-agent profile identity is incomplete")
	}
	sandboxDigest := text(nested(profile, "data", "sandbox-executable-sha256"))
	if len(sandboxDigest) != 64 || strings.ToLower(sandboxDigest) != sandboxDigest {
		return fmt.Errorf("installed runtime-agent profile lacks an approved lowercase SHA-256 pause executable digest")
	}
	if _, err := immutableDigest("sha256:" + sandboxDigest); err != nil {
		return fmt.Errorf("installed runtime-agent pause executable digest is invalid")
	}
	if text(nested(profile, "data", "sandbox-args-json")) != `["/pause"]` {
		return fmt.Errorf("installed runtime-agent profile does not pin the exact passive pause argv")
	}

	daemonSetUID := text(nested(daemonSet, "metadata", "uid"))
	generation := numberOrZero(nested(daemonSet, "metadata", "generation"))
	if text(nested(daemonSet, "metadata", "name")) != runtimeAgentDSName || text(nested(daemonSet, "metadata", "namespace")) != source.Namespace ||
		daemonSetUID == "" || source.AgentDaemonUID == "" || daemonSetUID != source.AgentDaemonUID || generation <= 0 ||
		numberOrZero(nested(daemonSet, "status", "observedGeneration")) != generation {
		return fmt.Errorf("runtime-agent DaemonSet identity or observed generation is stale")
	}
	desired := int(numberOrZero(nested(daemonSet, "status", "desiredNumberScheduled")))
	if desired <= 0 || int(numberOrZero(nested(daemonSet, "status", "currentNumberScheduled"))) != desired ||
		int(numberOrZero(nested(daemonSet, "status", "updatedNumberScheduled"))) != desired ||
		int(numberOrZero(nested(daemonSet, "status", "numberReady"))) != desired ||
		int(numberOrZero(nested(daemonSet, "status", "numberAvailable"))) != desired {
		return fmt.Errorf("runtime-agent DaemonSet rollout is incomplete")
	}
	if text(nested(daemonSet, "spec", "template", "metadata", "annotations", "rhiza.dev/sandbox-executable-sha256")) != sandboxDigest {
		return fmt.Errorf("runtime-agent DaemonSet template is not bound to the installed pause executable profile")
	}
	if text(nested(daemonSet, "spec", "template", "metadata", "annotations", "rhiza.mrchypark.dev/runtime-proof-profile")) != "passive-kernel-v1" ||
		!runtimeAgentPassivePodSpecOK(nested(daemonSet, "spec", "template", "spec")) {
		return fmt.Errorf("runtime-agent DaemonSet is not using the exact opt-in passive-kernel privilege profile")
	}
	selector, _ := nested(daemonSet, "spec", "selector", "matchLabels").(map[string]any)
	if len(selector) == 0 {
		return fmt.Errorf("runtime-agent DaemonSet selector is missing")
	}

	containers := runtimeProbeList(nested(daemonSet, "spec", "template", "spec", "containers"))
	var agent obj
	for _, candidate := range containers {
		if nested(candidate, "name") == "runtime-agent" {
			if agent != nil {
				return fmt.Errorf("runtime-agent DaemonSet container is ambiguous")
			}
			agent = runtimeProbeObject(candidate)
		}
	}
	if agent == nil || !runtimeAgentProfileConfigMapBinding(agent, profileName) ||
		!runtimeAgentProfileArgBinding(agent, "--sandbox-executable-sha256=$(SANDBOX_EXECUTABLE_SHA256)") ||
		!runtimeAgentProfileArgBinding(agent, "--sandbox-args-json=$(SANDBOX_ARGS_JSON)") {
		return fmt.Errorf("runtime-agent DaemonSet does not bind the exact installed passive profile")
	}

	wantNodes := make(map[string]bool)
	expectedAgents := make(map[string]gcsControlPodIdentity)
	agentIndex := 0
	for _, target := range source.Targets {
		if target.NodeName == "" {
			return fmt.Errorf("captured voter has no source node identity")
		}
		if wantNodes[target.NodeName] {
			continue
		}
		wantNodes[target.NodeName] = true
		if agentIndex >= len(source.AgentPods) {
			return fmt.Errorf("captured runtime-agent identities do not cover every source node")
		}
		identity := source.AgentPods[agentIndex]
		if identity.Name == "" || identity.UID == "" || identity.OwnerUID != daemonSetUID {
			return fmt.Errorf("captured runtime-agent identity is incomplete")
		}
		expectedAgents[target.NodeName] = identity
		agentIndex++
	}
	if len(wantNodes) == 0 {
		return fmt.Errorf("captured voter cohort has no source nodes")
	}
	if len(expectedAgents) != len(wantNodes) || len(source.AgentPods) != agentIndex {
		return fmt.Errorf("captured runtime-agent identities do not cover every source node")
	}
	readyByNode := make(map[string]int, len(wantNodes))
	owned := 0
	for _, pod := range runtimeProbeList(nested(pods, "items")) {
		isOwned := false
		owners := ownerRefs(pod)
		for _, owner := range owners {
			isOwned = isOwned || nested(owner, "apiVersion") == "apps/v1" && nested(owner, "kind") == "DaemonSet" &&
				nested(owner, "name") == runtimeAgentDSName && nested(owner, "uid") == daemonSetUID && nested(owner, "controller") == true
		}
		if !isOwned {
			continue
		}
		if len(owners) != 1 {
			return fmt.Errorf("runtime-agent Pod ownership is ambiguous")
		}
		owned++
		if !labelsMatch(nested(pod, "metadata", "labels"), selector) || text(nested(pod, "metadata", "deletionTimestamp")) != "" ||
			text(nested(pod, "metadata", "annotations", "rhiza.dev/sandbox-executable-sha256")) != sandboxDigest || !podReady(pod) ||
			text(nested(pod, "metadata", "annotations", "rhiza.mrchypark.dev/runtime-proof-profile")) != "passive-kernel-v1" ||
			!runtimeAgentPassivePodSpecOK(nested(pod, "spec")) ||
			nested(namedContainerStatus(pod, "runtime-agent"), "ready") != true {
			return fmt.Errorf("runtime-agent DaemonSet has an unready or profile-mismatched Pod")
		}
		node := text(nested(pod, "spec", "nodeName"))
		if wantNodes[node] {
			expected := expectedAgents[node]
			if text(nested(pod, "metadata", "name")) != expected.Name || text(nested(pod, "metadata", "uid")) != expected.UID {
				return fmt.Errorf("source-node runtime-agent Pod identity changed after capture")
			}
			readyByNode[node]++
		}
	}
	if owned != desired {
		return fmt.Errorf("runtime-agent DaemonSet Pod inventory differs from its desired rollout")
	}
	for node := range wantNodes {
		if readyByNode[node] != 1 {
			return fmt.Errorf("source node does not have exactly one Ready runtime-agent Pod")
		}
	}
	return nil
}

func runtimeAgentPassivePodSpecOK(raw any) bool {
	spec := runtimeProbeObject(raw)
	return nested(spec, "hostPID") == true && len(runtimeProbeList(nested(spec, "initContainers"))) == 0 &&
		len(runtimeProbeList(nested(spec, "ephemeralContainers"))) == 0 &&
		runtimeAgentPassiveSecurityContextOK(nested(spec, "containers"))
}

func runtimeAgentPassiveSecurityContextOK(raw any) bool {
	containers := runtimeProbeList(raw)
	if len(containers) != 1 {
		return false
	}
	var agent obj
	for _, candidate := range containers {
		container := runtimeProbeObject(candidate)
		if text(nested(container, "name")) != "runtime-agent" {
			continue
		}
		if agent != nil {
			return false
		}
		agent = container
	}
	if agent == nil {
		return false
	}
	security := runtimeProbeObject(nested(agent, "securityContext"))
	capabilities := runtimeProbeObject(nested(security, "capabilities"))
	added := runtimeProbeList(capabilities["add"])
	dropped := runtimeProbeList(capabilities["drop"])
	return nested(security, "privileged") == false && nested(security, "allowPrivilegeEscalation") == false &&
		len(added) == 1 && added[0] == "SYS_PTRACE" && len(dropped) == 1 && dropped[0] == "ALL"
}

func runtimeAgentProfileConfigMapBinding(container obj, profileName string) bool {
	foundSHA, foundArgs := false, false
	for _, entry := range runtimeProbeList(nested(container, "env")) {
		name := text(nested(entry, "name"))
		key := ""
		switch name {
		case "SANDBOX_EXECUTABLE_SHA256":
			key = "sandbox-executable-sha256"
		case "SANDBOX_ARGS_JSON":
			key = "sandbox-args-json"
		default:
			continue
		}
		if text(nested(entry, "valueFrom", "configMapKeyRef", "name")) != profileName ||
			text(nested(entry, "valueFrom", "configMapKeyRef", "key")) != key || nested(entry, "valueFrom", "configMapKeyRef", "optional") == true {
			return false
		}
		if name == "SANDBOX_EXECUTABLE_SHA256" {
			if foundSHA {
				return false
			}
			foundSHA = true
		} else {
			if foundArgs {
				return false
			}
			foundArgs = true
		}
	}
	return foundSHA && foundArgs
}

func runtimeAgentProfileArgBinding(container obj, expected string) bool {
	count := 0
	for _, argument := range runtimeProbeList(nested(container, "args")) {
		if argument == expected {
			count++
		}
	}
	return count == 1
}

type gcsDeadPodTerminalEvidence struct {
	Version        int                            `json:"version"`
	Request        fencing.StopRequest            `json:"request"`
	Runtime        fencing.RuntimeBinding         `json:"runtime"`
	ProducerUID    string                         `json:"producerUID"`
	Storage        operator.AttestationStoreScope `json:"storage"`
	RuntimeReceipt []byte                         `json:"runtimeReceipt"`
	Producer       fencing.MutationProducer       `json:"producer"`
}

func findGCSDeadPodTerminalRef(proof operator.FenceProof, source gcsRecoverySource, target operator.FenceTarget, runtime fencing.RuntimeBinding, producerUID string) (fencing.TerminalReference, error) {
	if proof.IndependentV2 == nil {
		return fencing.TerminalReference{}, fmt.Errorf("voter fence has no IndependentV2 terminal references")
	}
	var found *fencing.TerminalReference
	for i := range proof.IndependentV2.Terminals {
		ref := &proof.IndependentV2.Terminals[i]
		id := ref.Request.Identity
		if ref.EvidenceVersion != 2 || id.SourceClusterID != source.ClusterID || id.NodeID != target.NodeID || id.PodUID != target.PodUID ||
			id.RunID != runtime.RunID || id.WALIdentity != target.WALIdentity || ref.RuntimeHash != fencing.RuntimeBindingDigest(runtime) || ref.ProducerUID != producerUID {
			continue
		}
		if found != nil {
			return fencing.TerminalReference{}, fmt.Errorf("multiple canonical terminal references match the captured voter")
		}
		found = ref
	}
	if found == nil {
		return fencing.TerminalReference{}, fmt.Errorf("IndependentV2 proof omits the exact captured terminal reference")
	}
	return *found, nil
}

func verifyGCSDeadPodPassiveTerminal(ctx context.Context, stateStore *operator.AuthorityStateStore, evidenceStore *operator.AuthorityEvidenceStore,
	source gcsRecoverySource, target operator.FenceTarget, runtime fencing.RuntimeBinding, producerUID string, ref fencing.TerminalReference) (obj, error) {
	if ctx == nil || stateStore == nil || evidenceStore == nil || producerUID == "" || ref.Request.OperationID == "" {
		return nil, fmt.Errorf("canonical terminal stores and exact stop request are required")
	}
	root, _, err := stateStore.ReadRoot(ctx, runtime.StatefulSetUID, runtime.SourceClusterID)
	if err != nil {
		return nil, fmt.Errorf("read canonical root for dead-Pod terminal: %w", err)
	}
	run, ok := root.Ledger.Runs[runtime.RunID]
	if !ok || root.StatefulSetUID != source.StatefulSetUID || root.Ledger.SourceClusterID != source.ClusterID ||
		run.Identity != ref.Request.Identity || run.Runtime == nil || *run.Runtime != runtime || run.Stop == nil || *run.Stop != ref.Request ||
		run.IndependentTerminal == nil || *run.IndependentTerminal != ref || run.MutationProducerUID != producerUID {
		return nil, fmt.Errorf("canonical root does not bind the exact closed runtime, request, producer, and terminal reference")
	}
	wantRoot := gcsHAAuthorityRootIdentity(runtime.StatefulSetUID, runtime.SourceClusterID)
	if evidenceStore.Identity() != wantRoot {
		return nil, fmt.Errorf("terminal evidence store is not scoped to the canonical source root")
	}
	producer, gotProducerUID, _, err := stateStore.ReadProducer(ctx, runtime)
	if err != nil || gotProducerUID != producerUID || producer.Runtime != runtime || producer.WALIdentity != target.WALIdentity ||
		producer.CloseOperationID != ref.Request.OperationID || producer.CloseRequestHash != ref.Request.RequestHash || !producer.BarrierComplete() {
		return nil, fmt.Errorf("canonical mutation producer is not durably closed for the exact terminal request: %v", err)
	}
	if len(ref.ProofName) != len("rhiza-terminal-")+40 || !strings.HasPrefix(ref.ProofName, "rhiza-terminal-") {
		return nil, fmt.Errorf("canonical terminal evidence object name is invalid")
	}
	object, version, err := evidenceStore.Read(ctx, path.Join("terminal", ref.ProofName))
	if err != nil {
		return nil, fmt.Errorf("read canonical terminal evidence object: %w", err)
	}
	data, readErr := io.ReadAll(io.LimitReader(object, 700001))
	closeErr := object.Close()
	if err := errors.Join(readErr, closeErr); err != nil || len(data) > 700000 || version == nil || version.Value == "" ||
		(version.Type != objstore.Generation && version.Type != objstore.ETag) {
		return nil, fmt.Errorf("canonical terminal evidence object is incomplete: %v", err)
	}
	versionID := fmt.Sprintf("%x", sha256.Sum256(encoded([]any{"rhiza-terminal-object-version-v1", version.Type, version.Value})))
	if versionID != ref.ProofUID {
		return nil, fmt.Errorf("canonical terminal evidence object version differs from its reference")
	}
	var evidence gcsDeadPodTerminalEvidence
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&evidence) != nil || decoder.Decode(new(any)) != io.EOF || !bytes.Equal(encoded(evidence), data) ||
		evidence.Version != 2 || evidence.Request != ref.Request || evidence.Runtime != runtime || evidence.ProducerUID != producerUID ||
		evidence.Storage != gcsHAAckSourceScope(source) || !bytes.Equal(encoded(evidence.Producer), encoded(producer)) || evidence.Producer.Validate() != nil || !evidence.Producer.BarrierComplete() {
		return nil, fmt.Errorf("canonical terminal evidence contents differ from the exact closed source run")
	}
	proofDigest := fmt.Sprintf("%x", sha256.Sum256(encoded(evidence)))
	mutationDigest := fmt.Sprintf("%x", sha256.Sum256(encoded(evidence.Producer)))
	storageDigest := fmt.Sprintf("%x", sha256.Sum256(encoded(evidence.Storage)))
	receiptDigest := fmt.Sprintf("%x", sha256.Sum256(evidence.RuntimeReceipt))
	if proofDigest != ref.ProofDigest || mutationDigest != ref.MutationReceiptDigest || storageDigest != ref.StorageScopeDigest || receiptDigest != ref.RuntimeReceiptDigest {
		return nil, fmt.Errorf("canonical terminal evidence digest differs from its IndependentV2 reference")
	}
	if err := validateGCSDeadPodPassiveReceipt(evidence.RuntimeReceipt, ref, target, runtime, producerUID); err != nil {
		return nil, err
	}
	return obj{"mode": "passive-kernel-v1", "receiptSHA256": receiptDigest, "scopeDigest": runtime.CapturedScopeDigest}, nil
}

func validateGCSDeadPodPassiveReceipt(data []byte, ref fencing.TerminalReference, target operator.FenceTarget, runtime fencing.RuntimeBinding, producerUID string) error {
	identity := fencing.Identity{SourceClusterID: runtime.SourceClusterID, NodeID: target.NodeID, PodUID: target.PodUID, RunID: runtime.RunID, WALIdentity: target.WALIdentity}
	_, requestHashErr := immutableDigest("sha256:" + ref.Request.RequestHash)
	if len(data) == 0 || len(data) > 65536 || producerUID == "" || ref.EvidenceVersion != 2 || ref.Request.Identity != identity ||
		runtime.Validate(identity) != nil || ref.RuntimeHash != fencing.RuntimeBindingDigest(runtime) || ref.ProducerUID != producerUID ||
		ref.Request.OperationID == "" || requestHashErr != nil {
		return fmt.Errorf("terminal receipt reference is not bound to the exact captured run")
	}
	var receipt cri.PassiveReceipt
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&receipt) != nil || decoder.Decode(new(any)) != io.EOF || !bytes.Equal(encoded(receipt), data) ||
		receipt.Mode != "passive-kernel-v1" || receipt.Runtime != runtime || receipt.Request != ref.Request ||
		receipt.Observation.Version != 1 || receipt.Observation.RuntimeHash != fencing.RuntimeScopeBindingDigest(runtime) ||
		receipt.Observation.ScopeDigest != runtime.CapturedScopeDigest || !receipt.Observation.TargetPIDExited ||
		!receipt.Observation.TargetCgroupGone || !receipt.Observation.SandboxPIDExited || !receipt.Observation.SandboxCgroupGone {
		return fmt.Errorf("terminal receipt is not an exact passive-kernel observation")
	}
	if _, err := time.Parse(time.RFC3339Nano, receipt.Observation.ObservedAt); err != nil {
		return fmt.Errorf("passive terminal receipt observation time is invalid")
	}
	return nil
}

type gcsDeadPodFault struct {
	contextName, namespace, name, uid, pod, podUID, runID string
	body                                                  obj
	injected                                              obj
}

func createGCSDeadPodFault(ctx context.Context, source gcsRecoverySource, target operator.FenceTarget) (*gcsDeadPodFault, error) {
	contextName := strings.TrimSpace(os.Getenv("RHIZA_KUBE_CONTEXT"))
	if contextName == "" || !validGCSChaosPodName(source.Namespace) || len(target.PodUID) == 0 {
		return nil, fmt.Errorf("exact GCS dead-Pod fault scope is incomplete")
	}
	pod, err := getGCSChaosPod(ctx, contextName, source.Namespace, target.Pod)
	if err != nil || text(nested(pod, "metadata", "uid")) != target.PodUID || !podReady(pod) ||
		nested(pod, "metadata", "labels", gcsChaosRunLabel) != qualificationRunID ||
		nested(pod, "metadata", "labels", "app.kubernetes.io/part-of") != "rhiza-chaos" ||
		nested(pod, "metadata", "labels", "statefulset.kubernetes.io/pod-name") != target.Pod {
		return nil, fmt.Errorf("captured voter Pod identity changed before fault: %v", err)
	}
	runID := qualificationRunID
	body, err := gcsDeadPodChaosObject(source.Namespace, runID, target.Pod)
	if err != nil {
		return nil, err
	}
	name := text(nested(body, "metadata", "name"))
	data, err := commandWithInput(ctx, encoded(body), "kubectl", "--context", contextName, "-n", source.Namespace, "create", "-f", "-", "-o", "json")
	var created obj
	if err == nil {
		err = json.Unmarshal(data, &created)
	}
	if err != nil || !gcsDeadPodObjectMatches(created, body) {
		current, getErr := getGCSPodChaosObject(ctx, contextName, source.Namespace, name)
		if getErr != nil || !gcsDeadPodObjectMatches(current, body) {
			return nil, fmt.Errorf("PodChaos create outcome is not proven; create=%v read=%v", err, getErr)
		}
		created = current
	}
	uid := text(nested(created, "metadata", "uid"))
	if uid == "" {
		return nil, fmt.Errorf("PodChaos creation lacks exact UID")
	}
	return &gcsDeadPodFault{contextName: contextName, namespace: source.Namespace, name: name, uid: uid, pod: target.Pod, podUID: target.PodUID, runID: runID, body: body}, nil
}

func gcsDeadPodChaosObject(namespace, runID, pod string) (obj, error) {
	if !validGCSChaosPodName(namespace) || !validGCSChaosPodName(pod) || len(runID) != 8 {
		return nil, fmt.Errorf("dead-Pod Chaos selector identity is invalid")
	}
	if pod != "rhiza-voter-0" && pod != "rhiza-voter-1" && pod != "rhiza-voter-2" {
		return nil, fmt.Errorf("dead-Pod Chaos target is outside the admitted voter set")
	}
	for _, r := range runID {
		if !(r >= 'a' && r <= 'f' || r >= '0' && r <= '9') {
			return nil, fmt.Errorf("dead-Pod Chaos run ID is invalid")
		}
	}
	name := "gcs-dead-pod-" + runID
	selector := obj{"namespaces": []string{namespace}, "labelSelectors": obj{
		gcsChaosRunLabel: runID, "app.kubernetes.io/part-of": "rhiza-chaos", "statefulset.kubernetes.io/pod-name": pod}}
	return obj{"apiVersion": "chaos-mesh.org/v1alpha1", "kind": "PodChaos",
		"metadata": obj{"name": name, "namespace": namespace, "labels": obj{gcsChaosRunLabel: runID, "rhiza.dev/fault-phase": "dead-pod", "rhiza.dev/fault-role": "source-voter"}},
		"spec":     obj{"action": "pod-kill", "mode": "one", "duration": "10s", "selector": selector}}, nil
}

func gcsDeadPodObjectMatches(current, expected obj) bool {
	return nested(current, "apiVersion") == nested(expected, "apiVersion") && nested(current, "kind") == "PodChaos" &&
		text(nested(current, "metadata", "uid")) != "" && nested(current, "metadata", "name") == nested(expected, "metadata", "name") &&
		nested(current, "metadata", "namespace") == nested(expected, "metadata", "namespace") &&
		nested(current, "metadata", "labels", gcsChaosRunLabel) == nested(expected, "metadata", "labels", gcsChaosRunLabel) &&
		jsonSemanticEqual(nested(current, "spec"), nested(expected, "spec"))
}

func getGCSPodChaosObject(ctx context.Context, contextName, namespace, name string) (obj, error) {
	data, err := command(ctx, "kubectl", "--context", contextName, "-n", namespace, "get", "podchaos", name, "--ignore-not-found", "-o", "json")
	if err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return obj{}, nil
	}
	var result obj
	if err := json.Unmarshal(data, &result); err != nil || text(nested(result, "metadata", "uid")) == "" {
		return nil, fmt.Errorf("PodChaos response is invalid")
	}
	return result, nil
}

func (f *gcsDeadPodFault) WaitInjected(ctx context.Context) error {
	return wait(ctx, "exact GCS PodChaos injection", func() (bool, error) {
		current, err := getGCSPodChaosObject(ctx, f.contextName, f.namespace, f.name)
		if err != nil {
			return false, err
		}
		if text(nested(current, "metadata", "uid")) != f.uid || !gcsDeadPodObjectMatches(current, f.body) {
			return false, fmt.Errorf("PodChaos identity or exact selector changed")
		}
		conditions, _ := nested(current, "status", "conditions").([]any)
		injected, recovered := 0, 0
		for _, condition := range conditions {
			if nested(condition, "status") != "True" {
				continue
			}
			switch nested(condition, "type") {
			case "AllInjected":
				injected++
			case "AllRecovered":
				recovered++
			}
		}
		if recovered != 0 || injected > 1 {
			return false, fmt.Errorf("PodChaos recovered or reported duplicate injection")
		}
		if injected != 1 {
			return false, nil
		}
		pod, err := get(ctx, "pod", f.pod)
		if err == nil {
			if text(nested(pod, "metadata", "uid")) == f.podUID {
				return false, nil
			}
			f.injected = current
			return true, nil
		}
		if strings.Contains(strings.ToLower(err.Error()), "notfound") || strings.Contains(strings.ToLower(err.Error()), "not found") {
			f.injected = current
			return true, nil
		}
		return false, err
	})
}

func (f *gcsDeadPodFault) Restore(ctx context.Context) error {
	if f == nil || f.uid == "" || f.name == "" || !validGCSChaosPodName(f.namespace) {
		return fmt.Errorf("refusing PodChaos cleanup without exact UID")
	}
	if err := deleteGCSChaosUID(ctx, f.contextName, f.namespace, "podchaos", f.name, f.uid); err != nil {
		return err
	}
	return wait(ctx, "exact PodChaos UID cleanup", func() (bool, error) {
		current, err := getGCSPodChaosObject(ctx, f.contextName, f.namespace, f.name)
		if err != nil {
			return false, err
		}
		uid := text(nested(current, "metadata", "uid"))
		if uid == "" {
			return true, nil
		}
		if uid != f.uid {
			return false, fmt.Errorf("PodChaos name was reused; refusing to claim exact UID cleanup")
		}
		return false, nil
	})
}

func waitForGCSVoterFence(ctx context.Context, source gcsRecoverySource, target operator.FenceTarget, runtime fencing.RuntimeBinding, producerUID string, after time.Time) error {
	return wait(ctx, "authenticated exact single-voter IndependentV2 proof", func() (bool, error) {
		list, err := get(ctx, "rhizafences", "")
		if err != nil {
			return false, err
		}
		items, _ := list["items"].([]any)
		var matched any
		for _, item := range items {
			var request operator.FenceRequest
			if err := json.Unmarshal(encoded(nested(item, "spec")), &request); err != nil {
				return false, err
			}
			if !gcsVoterFenceRequestMatches(request, source, target) || !gcsDeadPodResourceIsFresh(item, after) {
				continue
			}
			if matched != nil {
				return false, fmt.Errorf("multiple voter fences match the captured incarnation")
			}
			matched = item
		}
		if matched == nil || nested(matched, "status", "proof") == nil {
			return false, nil
		}
		_, _, err = verifyIndependentVoterProof(matched, source, target, runtime, producerUID)
		return err == nil, err
	})
}

func gcsVoterFenceRequestMatches(request operator.FenceRequest, source gcsRecoverySource, target operator.FenceTarget) bool {
	return request.SourceClusterID == source.ClusterID && request.StatefulSet == source.StatefulSet && request.StatefulSetUID == source.StatefulSetUID &&
		request.BindingUID == source.BindingUID && request.Namespace == source.Namespace && request.LogicalID == source.LogicalID &&
		request.Scope == operator.ScopeVoter && request.OperationID != "" && len(request.Targets) == 1 && request.Targets[0] == target
}

func requireNoPriorGCSVoterFence(ctx context.Context, source gcsRecoverySource, target operator.FenceTarget) error {
	list, err := get(ctx, "rhizafences", "")
	if err != nil {
		return err
	}
	items, _ := list["items"].([]any)
	for _, item := range items {
		var request operator.FenceRequest
		if err := json.Unmarshal(encoded(nested(item, "spec")), &request); err != nil {
			return err
		}
		if gcsVoterFenceRequestMatches(request, source, target) {
			return fmt.Errorf("a prior exact Voter fence already exists for the captured source incarnation")
		}
	}
	return nil
}

func captureGCSVoterFence(ctx context.Context, source gcsRecoverySource, target operator.FenceTarget, runtime fencing.RuntimeBinding, producerUID string, after time.Time) (operator.FenceRequest, operator.FenceProof, obj, error) {
	list, err := get(ctx, "rhizafences", "")
	if err != nil {
		return operator.FenceRequest{}, operator.FenceProof{}, nil, err
	}
	items, _ := list["items"].([]any)
	var found obj
	for _, item := range items {
		var request operator.FenceRequest
		if json.Unmarshal(encoded(nested(item, "spec")), &request) != nil || !gcsVoterFenceRequestMatches(request, source, target) ||
			!gcsDeadPodResourceIsFresh(item, after) || nested(item, "status", "proof") == nil {
			continue
		}
		if found != nil {
			return operator.FenceRequest{}, operator.FenceProof{}, nil, fmt.Errorf("multiple matching voter fence resources")
		}
		found, _ = item.(obj)
	}
	if found == nil {
		return operator.FenceRequest{}, operator.FenceProof{}, nil, fmt.Errorf("exact voter fence proof disappeared")
	}
	request, proof, err := verifyIndependentVoterProof(found, source, target, runtime, producerUID)
	return request, proof, found, err
}

func gcsDeadPodResourceIsFresh(item any, after time.Time) bool {
	created, err := time.Parse(time.RFC3339Nano, text(nested(item, "metadata", "creationTimestamp")))
	return err == nil && !created.Before(after)
}

func verifyIndependentVoterProof(item any, source gcsRecoverySource, target operator.FenceTarget, runtime fencing.RuntimeBinding, producerUID string) (operator.FenceRequest, operator.FenceProof, error) {
	var request operator.FenceRequest
	var proof operator.FenceProof
	if err := json.Unmarshal(encoded(nested(item, "spec")), &request); err != nil {
		return request, proof, err
	}
	if !gcsVoterFenceRequestMatches(request, source, target) || runtime.SourceClusterID != source.ClusterID || runtime.NodeID != target.NodeID ||
		runtime.PodUID != target.PodUID || producerUID == "" {
		return request, proof, fmt.Errorf("voter fence differs from exact captured source incarnation")
	}
	if err := json.Unmarshal(encoded(nested(item, "status", "proof")), &proof); err != nil {
		return request, proof, err
	}
	evidence := proof.IndependentV2
	hash := fmt.Sprintf("%x", sha256.Sum256(encoded(request)))
	if evidence == nil || evidence.Version != 2 || evidence.RequestHash != hash || evidence.LedgerHash == "" ||
		evidence.StorageScope.Provider != "gcs" || evidence.StorageScope.Bucket != source.Bucket || evidence.StorageScope.Prefix != source.Prefix ||
		proof.ProofID != "independent-v2-"+fmt.Sprintf("%x", sha256.Sum256(encoded(evidence))) || proof.Independent != nil ||
		proof.OperationID != request.OperationID || proof.SourceClusterID != source.ClusterID || proof.BindingUID != source.BindingUID ||
		proof.RequestHash != hash || !proof.ProcessesTerminated || !proof.RecreationBlocked || !proof.StorageQuiesced ||
		len(evidence.Terminals) == 0 || len(evidence.Terminals) > 64 || len(evidence.CurrentPodUIDs) > 64 {
		return request, proof, fmt.Errorf("voter fence lacks a complete GCS IndependentV2 proof")
	}
	if _, err := immutableDigest("sha256:" + evidence.LedgerHash); err != nil {
		return request, proof, fmt.Errorf("IndependentV2 voter ledger digest is invalid")
	}
	scopeDigest := fmt.Sprintf("%x", sha256.Sum256(encoded(evidence.StorageScope)))
	currentUIDs := map[string]bool{}
	for _, uid := range evidence.CurrentPodUIDs {
		if uid == "" || currentUIDs[uid] {
			return request, proof, fmt.Errorf("IndependentV2 voter current Pod inventory is invalid")
		}
		currentUIDs[uid] = true
	}
	terminal := false
	for _, ref := range evidence.Terminals {
		id := ref.Request.Identity
		if err := validateGCSGenerationTerminalReference(ref, source.ClusterID, scopeDigest); err != nil {
			return request, proof, fmt.Errorf("IndependentV2 voter terminal reference is incomplete")
		}
		if id.SourceClusterID == source.ClusterID && id.NodeID == target.NodeID && id.PodUID == target.PodUID &&
			id.WALIdentity == target.WALIdentity && id.RunID == runtime.RunID && ref.EvidenceVersion == 2 &&
			ref.RuntimeHash == fencing.RuntimeBindingDigest(runtime) && ref.ProducerUID == producerUID {
			terminal = true
		}
	}
	if !terminal {
		return request, proof, fmt.Errorf("IndependentV2 voter proof omits captured dead incarnation")
	}
	return request, proof, nil
}

func waitForGCSVoterRecovery(ctx context.Context, source gcsRecoverySource, target operator.FenceTarget, request operator.FenceRequest, proof operator.FenceProof) (operator.Resource, error) {
	var recovery operator.Resource
	err := wait(ctx, "completed automatic single-voter recovery", func() (bool, error) {
		list, err := get(ctx, "rhizarecoveries", "")
		if err != nil {
			return false, err
		}
		items, _ := list["items"].([]any)
		matches := 0
		for _, item := range items {
			if nested(item, "spec", "sourceClusterID") != source.ClusterID || nested(item, "spec", "recoveryID") != request.OperationID {
				continue
			}
			matches++
			if err := json.Unmarshal(encoded(item), &recovery); err != nil {
				return false, err
			}
		}
		if matches > 1 {
			return false, fmt.Errorf("multiple recovery children match the voter fence")
		}
		if matches != 1 || recovery.Status.Phase != "Complete" || recovery.Status.Stage != "Complete" {
			return false, nil
		}
		if recovery.Spec.SourceClusterID != source.ClusterID || recovery.Spec.RecoveryID != request.OperationID ||
			recovery.Status.Source != source.ClusterID || recovery.Status.Target != source.ClusterID || recovery.Status.StatefulSetUID != source.StatefulSetUID ||
			!recovery.Spec.Fence.Confirmed || recovery.Spec.Fence.Evidence != proof.ProofID || recovery.Spec.Fence.RecoveryID != request.OperationID ||
			recovery.Spec.Fence.ClusterID != source.ClusterID || recovery.Spec.Fence.StatefulSetUID != source.StatefulSetUID {
			return false, fmt.Errorf("completed voter recovery does not bind exact source proof")
		}
		if recovery.Spec.Membership == nil || string(recovery.Spec.Membership.Remove) != target.NodeID || recovery.Spec.Membership.RemovedPod != target.Pod ||
			recovery.Spec.Membership.OperationID != request.OperationID || recovery.Spec.Membership.ReplacementPod != target.Pod || recovery.Spec.Membership.ReplacementSecret == "" ||
			recovery.Status.Membership == nil || recovery.Status.Membership.Phase != "Complete" || recovery.Status.Membership.RemoveRequest == nil || recovery.Status.Membership.Add == nil {
			return false, fmt.Errorf("recovery child does not remove the captured voter")
		}
		remove := recovery.Status.Membership.RemoveRequest
		if remove.OperationID != request.OperationID || remove.ClusterID != source.ClusterID || string(remove.Remove) != target.NodeID ||
			remove.Fence == nil || string(remove.Fence.NodeID) != target.NodeID || remove.Fence.WALIdentity != target.WALIdentity ||
			remove.Fence.WorkloadUID != target.PodUID || !remove.Fence.Confirmed || remove.Fence.Evidence != proof.ProofID {
			return false, fmt.Errorf("durable voter removal journal differs from the captured fence proof")
		}
		if len(recovery.Spec.Membership.VoterPods) != 2 {
			return false, fmt.Errorf("automatic replacement did not bind exactly the two surviving voters")
		}
		add := recovery.Status.Membership.Add
		if add.OperationID == "" || add.MemberID == "" || add.MemberID == quepaxa.NodeID(target.NodeID) || add.WALIdentity == "" ||
			add.RequestHash == "" || add.SecretUID == "" || add.SecretResourceVer == "" {
			return false, fmt.Errorf("same-ordinal voter addition lacks a fresh logical identity, WAL, request, or credential binding")
		}
		remaining := map[string]string{}
		for _, voter := range recovery.Spec.Membership.VoterPods {
			remaining[string(voter.NodeID)] = voter.Pod
		}
		for _, captured := range source.Targets {
			if captured.PodUID == target.PodUID {
				continue
			}
			if remaining[captured.NodeID] != captured.Pod {
				return false, fmt.Errorf("automatic replacement changed a surviving voter reference")
			}
			delete(remaining, captured.NodeID)
		}
		if len(remaining) != 0 {
			return false, fmt.Errorf("automatic replacement includes an uncaptured surviving voter")
		}
		return true, nil
	})
	return recovery, err
}

func verifyGCSVoterMembership(ctx context.Context, source gcsRecoverySource, old operator.FenceTarget, oldCredentialUID string, recovery operator.Resource, forward *forward, token, out string) (operator.FenceTarget, error) {
	status, err := readMembershipWithToken(ctx, forward, token)
	if err != nil || status.ClusterID != source.ClusterID || string(status.NodeID) != source.Targets[1].NodeID || status.WALIdentity != source.Targets[1].WALIdentity ||
		status.Pending || !status.Voting || len(status.Voters) != 3 {
		return operator.FenceTarget{}, fmt.Errorf("authenticated source membership did not converge to three voters: %v", err)
	}
	identities := recovery.Status.TargetIdentities
	if len(identities) != 3 {
		return operator.FenceTarget{}, fmt.Errorf("completed voter recovery lacks exactly three target identities")
	}
	byUID := map[string]operator.FenceTarget{}
	byNodeID := map[string]bool{}
	var learner operator.FenceTarget
	for _, identity := range identities {
		if identity.NodeID == "" || identity.Pod == "" || identity.PodUID == "" || identity.WALIdentity == "" || byUID[identity.PodUID].PodUID != "" || byNodeID[identity.NodeID] {
			return operator.FenceTarget{}, fmt.Errorf("recovered membership identity set is incomplete or duplicated")
		}
		byUID[identity.PodUID] = identity
		byNodeID[identity.NodeID] = true
		if identity.PodUID != old.PodUID && identity.NodeID != old.NodeID {
			if learner.PodUID != "" {
				return operator.FenceTarget{}, fmt.Errorf("recovered membership has multiple replacement identities")
			}
			learner = identity
		}
	}
	for _, target := range source.Targets {
		if target.PodUID == old.PodUID {
			continue
		}
		if _, ok := byUID[target.PodUID]; !ok {
			return operator.FenceTarget{}, fmt.Errorf("surviving voter incarnation is absent from completed membership")
		}
	}
	if learner.PodUID == "" {
		return operator.FenceTarget{}, fmt.Errorf("recovery did not identify exactly one fresh logical replacement")
	}
	if learner.Pod != old.Pod || learner.PodUID == old.PodUID || learner.NodeID == old.NodeID || learner.WALIdentity == old.WALIdentity {
		return operator.FenceTarget{}, fmt.Errorf("replacement did not preserve the exact ordinal with fresh Pod, logical, and WAL identities")
	}
	if recovery.Spec.Membership == nil || recovery.Spec.Membership.ReplacementPod != old.Pod || recovery.Spec.Membership.ReplacementSecret == "" || recovery.Status.Membership == nil || recovery.Status.Membership.Add == nil ||
		recovery.Status.Membership.Add.MemberID != quepaxa.NodeID(learner.NodeID) || recovery.Status.Membership.Add.WALIdentity != learner.WALIdentity {
		return operator.FenceTarget{}, fmt.Errorf("durable ADD journal does not bind the same-ordinal replacement")
	}
	for _, voter := range status.Voters {
		if string(voter) == old.NodeID {
			return operator.FenceTarget{}, fmt.Errorf("closed old logical identity remains in authenticated membership")
		}
	}
	seenVoters := map[string]bool{}
	for _, voter := range status.Voters {
		id := string(voter)
		if !byNodeID[id] || seenVoters[id] {
			return operator.FenceTarget{}, fmt.Errorf("authenticated current membership differs from completed recovery identities")
		}
		seenVoters[id] = true
	}
	if len(seenVoters) != len(byNodeID) {
		return operator.FenceTarget{}, fmt.Errorf("authenticated current membership omits a completed voter identity")
	}
	for _, original := range source.Targets {
		if original.PodUID == old.PodUID {
			continue
		}
		pod, err := get(ctx, "pod", original.Pod)
		if err != nil || text(nested(pod, "metadata", "uid")) != original.PodUID || !podReady(pod) {
			return operator.FenceTarget{}, fmt.Errorf("surviving captured voter Pod identity changed: %v", err)
		}
		identity, ok := byUID[original.PodUID]
		if !ok || identity.NodeID != original.NodeID || identity.Pod != original.Pod || identity.WALIdentity != original.WALIdentity {
			return operator.FenceTarget{}, fmt.Errorf("surviving voter changed its captured Node, Pod, or WAL identity")
		}
	}
	pod, err := get(ctx, "pod", learner.Pod)
	if err != nil || text(nested(pod, "metadata", "uid")) != learner.PodUID || !podReady(pod) {
		return operator.FenceTarget{}, fmt.Errorf("same-ordinal replacement Pod identity or readiness is not confirmed: %v", err)
	}
	learnerForward, err := startForward(ctx, learner.Pod, out)
	if err != nil {
		return operator.FenceTarget{}, err
	}
	learnerStatus, err := readMembershipWithToken(ctx, learnerForward, token)
	learnerForward.close()
	if err != nil || learnerStatus.ClusterID != source.ClusterID || string(learnerStatus.NodeID) != learner.NodeID || learnerStatus.WALIdentity != learner.WALIdentity || learnerStatus.Pending || !learnerStatus.Voting || len(learnerStatus.Voters) != 3 {
		return operator.FenceTarget{}, fmt.Errorf("same-ordinal replacement did not authenticate as the committed active voter: %v", err)
	}
	add := recovery.Status.Membership.Add
	secret, err := get(ctx, "secret", recovery.Spec.Membership.ReplacementSecret)
	if err != nil || secret["type"] != "Opaque" || secret["immutable"] != true || text(nested(secret, "metadata", "name")) != recovery.Spec.Membership.ReplacementSecret ||
		text(nested(secret, "metadata", "namespace")) != source.Namespace || text(nested(secret, "metadata", "uid")) != add.SecretUID || text(nested(secret, "metadata", "resourceVersion")) != add.SecretResourceVer ||
		text(nested(secret, "metadata", "annotations", "rhiza.mrchypark.dev/automatic-owner")) != recovery.Spec.RecoveryID || add.SecretUID == oldCredentialUID {
		return operator.FenceTarget{}, fmt.Errorf("same-ordinal replacement credential is not fresh, immutable, and bound to the committed ADD: %v", err)
	}
	if text(nested(pod, "metadata", "annotations", "rhiza.mrchypark.dev/fencing-root-uid")) != source.BindingUID ||
		text(nested(pod, "metadata", "annotations", "rhiza.mrchypark.dev/cooperative-fencing")) != "1" ||
		text(nested(pod, "metadata", "annotations", "rhiza.mrchypark.dev/fencing-credential-uid")) != add.SecretUID {
		return operator.FenceTarget{}, fmt.Errorf("same-ordinal replacement Pod is not bound to the captured root and committed credential")
	}
	memberBytes, err := base64.StdEncoding.DecodeString(text(nested(secret, "data", "member")))
	if err != nil {
		return operator.FenceTarget{}, fmt.Errorf("same-ordinal member credential is invalid")
	}
	var member quepaxa.Member
	if err := json.Unmarshal(memberBytes, &member); err != nil || string(member.ID) != learner.NodeID {
		return operator.FenceTarget{}, fmt.Errorf("same-ordinal immutable member credential differs from committed logical identity")
	}
	member.WALIdentity = learner.WALIdentity
	request := network.MembershipChange{OperationID: add.OperationID, ClusterID: source.ClusterID, ExpectedConfigID: add.ExpectedConfigID, ExpectedAbortSlot: add.ExpectedAbortSlot, Add: &member}
	if fmt.Sprintf("%x", sha256.Sum256(encoded(request))) != add.RequestHash {
		return operator.FenceTarget{}, fmt.Errorf("same-ordinal credential does not reproduce the committed ADD request hash")
	}
	peerToken, err := base64.StdEncoding.DecodeString(text(nested(secret, "data", "RHIZA_PEER_TOKEN")))
	if err != nil || len(peerToken) == 0 || !bytes.Equal(network.PeerPublicKey(types.ClusterID(source.ClusterID), member.ID, string(peerToken)), member.PublicKey[:]) {
		return operator.FenceTarget{}, fmt.Errorf("same-ordinal peer token does not match the committed public identity")
	}
	if err := os.WriteFile(filepath.Join(out, "same-ordinal-authenticated-binding.json"), encoded(obj{
		"pod": learner.Pod, "podUID": learner.PodUID, "oldPodUID": old.PodUID, "nodeID": learner.NodeID, "oldNodeID": old.NodeID,
		"walIdentity": learner.WALIdentity, "oldWALIdentity": old.WALIdentity, "credentialUID": add.SecretUID,
		"oldCredentialUID": oldCredentialUID, "addition": add, "membership": learnerStatus,
	}), 0600); err != nil {
		return operator.FenceTarget{}, err
	}
	return learner, nil
}

func restartGCSDeadPodChild(ctx context.Context, source gcsRecoverySource, target operator.FenceTarget, restartCount float64, containerID string) error {
	if target.Pod == "" || target.PodUID == "" || target.NodeID == "" || target.WALIdentity == "" || restartCount < 0 || containerID == "" {
		return fmt.Errorf("same-ordinal restart binding is incomplete")
	}
	pod, err := get(ctx, "pod", target.Pod)
	if err != nil || text(nested(pod, "metadata", "uid")) != target.PodUID || !gcsPodOwnedBy(pod, source.StatefulSetUID) {
		return fmt.Errorf("same-ordinal Pod identity changed before restart: %v", err)
	}
	status := namedContainerStatus(pod, source.Container)
	if status == nil || text(nested(status, "containerID")) != containerID || numberOrZero(nested(status, "restartCount")) != restartCount || nested(status, "ready") != true {
		return fmt.Errorf("same-ordinal active container changed before restart")
	}
	// The supervised workload has the fencer as PID 1 and the Rhiza child as
	// PID 2. Check the process identity before signaling; never kill PID 1 or
	// select a process by an untrusted name substring.
	_, err = command(ctx, "kubectl", "--context", kubeContext, "-n", namespace, "exec", target.Pod, "-c", source.Container,
		"--", "/bin/sh", "-ec", `test "$(cat /proc/2/comm)" = rhiza; kill -TERM 2`)
	if err != nil {
		// A successful restart below is the authoritative result: kubectl exec
		// may lose its stream while the container is restarting.
		return nil
	}
	return nil
}

func currentGCSRuntimeRunID(ctx context.Context, store *operator.AuthorityStateStore, source gcsRecoverySource, target operator.FenceTarget, containerID string) (string, error) {
	if store == nil || containerID == "" {
		return "", fmt.Errorf("runtime authority or container identity is unavailable")
	}
	state, _, err := store.ReadRoot(ctx, source.StatefulSetUID, source.ClusterID)
	if err != nil {
		return "", err
	}
	var runID string
	for id, run := range state.Ledger.Runs {
		if run.Identity.PodUID != target.PodUID || run.Identity.NodeID != target.NodeID || run.Identity.WALIdentity != target.WALIdentity ||
			run.Runtime == nil || run.Runtime.ContainerID != containerID || run.Stop != nil || run.Exited {
			continue
		}
		if runID != "" {
			return "", fmt.Errorf("multiple active canonical runs match the exact replacement container")
		}
		runID = id
	}
	if runID == "" {
		return "", fmt.Errorf("no active canonical run matches the exact replacement container")
	}
	return runID, nil
}

func waitForGCSActiveSameOrdinalRestart(ctx context.Context, store *operator.AuthorityStateStore, source gcsRecoverySource, target operator.FenceTarget,
	committedIdentities []operator.FenceTarget, closedNodeID string, forward *forward, token string, previousRestarts float64, previousContainerID, previousRunID string) (obj, error) {
	var evidence obj
	err := wait(ctx, "same-ordinal active child restart with retained Pod and WAL identity", func() (bool, error) {
		pod, err := get(ctx, "pod", target.Pod)
		if err != nil {
			return false, nil
		}
		if text(nested(pod, "metadata", "uid")) != target.PodUID || !gcsPodOwnedBy(pod, source.StatefulSetUID) {
			return false, fmt.Errorf("same-ordinal active restart changed physical Pod identity")
		}
		status := namedContainerStatus(pod, source.Container)
		if status == nil || numberOrZero(nested(status, "restartCount")) <= previousRestarts || text(nested(status, "containerID")) == "" || text(nested(status, "containerID")) == previousContainerID || nested(status, "ready") != true || !podReady(pod) {
			return false, nil
		}
		membership, err := readMembershipWithToken(ctx, forward, token)
		if err != nil {
			return false, nil
		}
		if membership.ClusterID != source.ClusterID || string(membership.NodeID) != target.NodeID || membership.WALIdentity != target.WALIdentity ||
			membership.Pending || !membership.Voting || !gcsMembershipMatchesCommittedIdentities(membership, committedIdentities, closedNodeID) {
			return false, fmt.Errorf("restarted child did not resume the exact committed voter set without the closed logical identity")
		}
		runID, err := currentGCSRuntimeRunID(ctx, store, source, target, text(nested(status, "containerID")))
		if err != nil {
			return false, nil
		}
		if runID == previousRunID {
			return false, fmt.Errorf("active container restart reused the prior supervisor run ID")
		}
		evidence = obj{"pod": target.Pod, "podUID": target.PodUID, "nodeID": target.NodeID, "walIdentity": target.WALIdentity,
			"previousContainerID": previousContainerID, "currentContainerID": nested(status, "containerID"),
			"previousRestartCount": previousRestarts, "currentRestartCount": numberOrZero(nested(status, "restartCount")), "previousRunID": previousRunID, "currentRunID": runID,
			"membership": membership}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return evidence, nil
}

func gcsMembershipMatchesCommittedIdentities(status network.MembershipStatus, identities []operator.FenceTarget, closedNodeID string) bool {
	if closedNodeID == "" || len(identities) == 0 || len(status.Voters) != len(identities) {
		return false
	}
	want := make(map[string]bool, len(identities))
	for _, identity := range identities {
		if identity.NodeID == "" || identity.NodeID == closedNodeID || want[identity.NodeID] {
			return false
		}
		want[identity.NodeID] = true
	}
	for _, voter := range status.Voters {
		id := string(voter)
		if id == closedNodeID || !want[id] {
			return false
		}
		delete(want, id)
	}
	return len(want) == 0
}
