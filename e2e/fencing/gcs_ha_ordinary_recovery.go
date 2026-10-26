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
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

func qualifyGCSOrdinaryRecovery(ctx context.Context) (resultErr error) {
	root := os.Getenv("FENCING_E2E_OUT")
	if root == "" {
		root = "gcs-ha-ordinary-recovery-results"
	}
	out := filepath.Join(root, "ordinary-recovery")
	if err := os.MkdirAll(root, 0700); err != nil {
		return err
	}
	if err := os.Mkdir(out, 0700); err != nil {
		return fmt.Errorf("create private ordinary-recovery evidence directory: %w", err)
	}

	var evidence obj
	var forwards []*forward
	var operatorOwned runOwnedOperator
	operatorCaptured, operatorScaleAttempted := false, false
	defer func() {
		for i := len(forwards) - 1; i >= 0; i-- {
			if err := forwards[i].close(); err != nil {
				resultErr = errors.Join(resultErr, fmt.Errorf("close ordinary-recovery port-forward: %w", err))
			}
		}
		if operatorCaptured && operatorScaleAttempted {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			if err := restoreRunOwnedOperator(cleanupCtx, operatorOwned); err != nil {
				resultErr = errors.Join(resultErr, fmt.Errorf("restore exact run-owned operator Deployment: %w", err))
			}
			cancel()
		}
		if evidence != nil {
			status := "qualified"
			if resultErr != nil {
				status = "failed"
			}
			evidence["status"] = status
			data := encoded(evidence)
			file, err := os.OpenFile(filepath.Join(out, "evidence.json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			if err == nil {
				_, err = file.Write(data)
				if closeErr := file.Close(); closeErr != nil {
					err = errors.Join(err, closeErr)
				}
			}
			if err != nil {
				resultErr = errors.Join(resultErr, fmt.Errorf("write private ordinary-recovery evidence: %w", err))
			}
		}
	}()

	appendEvent := func(name string, data any) {
		if evidence == nil {
			evidence = obj{"qualification": "gcs-ordinary-two-consecutive-voter-replacements", "events": []any{}}
		}
		events, _ := evidence["events"].([]any)
		evidence["events"] = append(events, obj{"event": name, "time": time.Now().UTC(), "data": data})
	}

	source, err := captureGCSRecoverySource(ctx, out)
	if err != nil {
		return err
	}
	if source.Durability != "before-ack" || len(source.Targets) != 3 {
		return fmt.Errorf("ordinary recovery requires the explicit GCS before-ACK profile and exactly three voters")
	}
	workload, err := get(ctx, "statefulset", source.StatefulSet)
	if err != nil {
		return fmt.Errorf("read live ordinary-recovery StatefulSet launch: %w", err)
	}
	if err := requireGCSOrdinarySupervisedLaunch(workload, source); err != nil {
		return err
	}
	for _, permission := range [][2]string{{"delete", "pods"}, {"patch", "deployment/rhiza-operator"}} {
		answer, err := command(ctx, "kubectl", "--context", kubeContext, "auth", "can-i", permission[0], permission[1], "--namespace", namespace)
		if err != nil || strings.TrimSpace(string(answer)) != "yes" {
			return fmt.Errorf("ordinary recovery requires existing %s permission for %s: %v", permission[0], permission[1], err)
		}
	}
	appendEvent("source_bound", obj{"clusterID": source.ClusterID, "statefulSetUID": source.StatefulSetUID, "bindingUID": source.BindingUID,
		"bucket": source.Bucket, "prefix": source.Prefix, "targets": source.Targets})
	appendEvent("supervised_fencer_launch_verified", obj{"statefulSetUID": source.StatefulSetUID, "container": source.Container, "shape": "expected"})

	operatorOwned, err = captureRunOwnedOperator(ctx)
	if err != nil {
		return err
	}
	operatorCaptured = true
	fencerOwned, err := captureRunOwnedDeployment(ctx, "rhiza-fencer")
	if err != nil {
		return err
	}
	if err := verifyRunOwnedDeploymentReady(ctx, operatorOwned); err != nil {
		return err
	}
	if err := verifyRunOwnedDeploymentReady(ctx, fencerOwned); err != nil {
		return err
	}
	appendEvent("controllers_ready", obj{"operatorUID": operatorOwned.UID, "operatorReplicas": operatorOwned.Replicas,
		"fencerUID": fencerOwned.UID, "fencerReplicas": fencerOwned.Replicas,
		"operatorPods": operatorOwned.Pods, "fencerPods": fencerOwned.Pods})

	token, err := sourceAdminToken(ctx, source.StatefulSet, source.Container)
	if err != nil {
		return err
	}
	observerForward, err := startForward(ctx, source.Targets[1].Pod, out)
	if err != nil {
		return err
	}
	forwards = append(forwards, observerForward)
	baselineMembership, err := readMembershipWithToken(ctx, observerForward, token)
	if err != nil || !gcsOrdinaryBaselineMembershipReady(source, source.Targets[1], baselineMembership) {
		return fmt.Errorf("ordinary-recovery baseline is not a healthy three-voter membership: %v", err)
	}
	if _, err := call(ctx, observerForward, "/sql/execute", obj{"request_id": "gcs-ordinary-create-table", "sql": "CREATE TABLE fencing_items (id INTEGER PRIMARY KEY, value TEXT)"}, token); err != nil {
		return fmt.Errorf("create ordinary-recovery SQL baseline: %w", err)
	}
	baselineAck, err := writeGCSAcknowledgedRow(ctx, observerForward, token, 1, "gcs-ordinary-baseline")
	if err != nil {
		return fmt.Errorf("ordinary-recovery baseline ACK/readback: %w", err)
	}
	baselineGraphCommand := obj{"request_id": "gcs-ordinary-graph-baseline",
		"cypher": "CREATE (:GCSOrdinaryRecovery {id: 'baseline', value: 'gcs-ordinary-graph-baseline'})"}
	baselineGraphAck, err := call(ctx, observerForward, "/graph/execute", baselineGraphCommand, token)
	if err != nil {
		return fmt.Errorf("ordinary-recovery baseline Graph ACK: %w", err)
	}
	baselineGraphSlot, err := gcsOrdinaryMutationReceiptSlot(baselineGraphAck)
	if err != nil {
		return fmt.Errorf("ordinary-recovery baseline Graph receipt: %w", err)
	}
	baselineGraphRetry, err := call(ctx, observerForward, "/graph/execute", baselineGraphCommand, token)
	if err != nil {
		return fmt.Errorf("ordinary-recovery baseline Graph retry: %w", err)
	}
	baselineGraphRetrySlot, err := gcsOrdinaryMutationReceiptSlot(baselineGraphRetry)
	baselineGraphStatus, statusErr := call(ctx, observerForward, "/request/status", obj{"kind": "graph", "request_id": baselineGraphCommand["request_id"]}, token)
	if err != nil || statusErr != nil || baselineGraphRetrySlot != baselineGraphSlot || !gcsOrdinaryMutationRetryMatches(baselineGraphStatus, baselineGraphSlot) {
		return fmt.Errorf("ordinary-recovery baseline Graph retry changed its ACK: %v", errors.Join(err, statusErr))
	}
	if err := verifyGCSOrdinaryGraphValue(ctx, observerForward, token, "baseline", "gcs-ordinary-graph-baseline"); err != nil {
		return fmt.Errorf("ordinary-recovery baseline Graph readback: %w", err)
	}
	baselineKVCommand := obj{"request_id": "gcs-ordinary-kv-baseline", "key": "gcs-ordinary-recovery/baseline", "value": []byte("gcs-ordinary-kv-baseline")}
	baselineKVAck, err := call(ctx, observerForward, "/kv/put", baselineKVCommand, token)
	if err != nil {
		return fmt.Errorf("ordinary-recovery baseline KV ACK: %w", err)
	}
	baselineKVSlot, err := gcsOrdinaryMutationReceiptSlot(baselineKVAck)
	if err != nil {
		return fmt.Errorf("ordinary-recovery baseline KV receipt: %w", err)
	}
	baselineKVRetry, err := call(ctx, observerForward, "/kv/put", baselineKVCommand, token)
	if err != nil {
		return fmt.Errorf("ordinary-recovery baseline KV retry: %w", err)
	}
	baselineKVRetrySlot, err := gcsOrdinaryMutationReceiptSlot(baselineKVRetry)
	baselineKVStatus, statusErr := call(ctx, observerForward, "/request/status", obj{"kind": "kv", "request_id": baselineKVCommand["request_id"]}, token)
	if err != nil || statusErr != nil || baselineKVRetrySlot != baselineKVSlot || !gcsOrdinaryMutationRetryMatches(baselineKVStatus, baselineKVSlot) {
		return fmt.Errorf("ordinary-recovery baseline KV retry changed its ACK: %v", errors.Join(err, statusErr))
	}
	if err := verifyGCSOrdinaryKVValue(ctx, observerForward, token, "gcs-ordinary-recovery/baseline", []byte("gcs-ordinary-kv-baseline")); err != nil {
		return fmt.Errorf("ordinary-recovery baseline KV readback: %w", err)
	}

	target := source.Targets[0]
	oldPod, err := get(ctx, "pod", target.Pod)
	if err != nil || text(nested(oldPod, "metadata", "uid")) != target.PodUID || !gcsPodOwnedBy(oldPod, source.StatefulSetUID) {
		return fmt.Errorf("ordinary-recovery target changed before controller shutdown: %v", err)
	}
	oldCredentialUID := text(nested(oldPod, "metadata", "annotations", "rhiza.mrchypark.dev/fencing-credential-uid"))
	if oldCredentialUID == "" {
		return fmt.Errorf("ordinary-recovery target has no captured credential UID")
	}
	appendEvent("baseline_acknowledged", obj{"ack": baselineAck, "membership": baselineMembership,
		"oldTarget": target, "oldCredentialUID": oldCredentialUID})

	if err := refreshRunOwnedDeploymentResourceVersion(ctx, &operatorOwned); err != nil {
		return err
	}
	operatorScaleAttempted = true
	if err := operatorOwned.setReplicas(ctx, 0); err != nil {
		return fmt.Errorf("scale exact run-owned operator Deployment to zero: %w", err)
	}
	if err := waitRunOwnedOperatorAbsent(ctx, operatorOwned, fencerOwned); err != nil {
		return err
	}
	appendEvent("operator_absent_fencer_ready", obj{"operatorUID": operatorOwned.UID, "fencerUID": fencerOwned.UID})

	secondSurvivorForward, err := startForward(ctx, source.Targets[2].Pod, out)
	if err != nil {
		return err
	}
	forwards = append(forwards, secondSurvivorForward)
	currentSource := source
	currentSource.Targets = append([]operator.FenceTarget(nil), source.Targets...)
	currentTarget := target
	currentCredentialUID := oldCredentialUID
	priorIncarnations := []operator.FenceTarget{target}
	currentPeerCredentials, err := peerAdmissionCredentialsForTargets(ctx, currentSource, currentSource.Targets)
	if err != nil {
		return fmt.Errorf("ordinary-recovery initial peer credential bindings: %w", err)
	}
	var priorPeerCredentials []peerAdmissionCredential
	var checkpointBeforeSecondRecovery gcsOrdinaryCheckpointEvidence
	for incident := 1; incident <= 2; incident++ {
		if err := waitRunOwnedOperatorAbsent(ctx, operatorOwned, fencerOwned); err != nil {
			return fmt.Errorf("controller is not absent before ordinary voter recovery incident %d: %w", incident, err)
		}
		recoveryTarget, firstSurvivorStatus, secondSurvivorStatus, err := waitForGCSOrdinaryRecoveryTarget(ctx, source.ClusterID,
			currentSource.Targets[1].NodeID, observerForward, currentSource.Targets[2].NodeID, secondSurvivorForward, token)
		if err != nil {
			return fmt.Errorf("ordinary-recovery incident %d stable survivor recovery target: %w", incident, err)
		}
		appendEvent("pre_recovery_survivor_status_target", obj{
			"incident": incident, "target": recoveryTarget,
			"survivors": []network.VoterRecoveryStatus{firstSurvivorStatus, secondSurvivorStatus},
		})
		currentOldPod, err := get(ctx, "pod", currentTarget.Pod)
		if err != nil || text(nested(currentOldPod, "metadata", "uid")) != currentTarget.PodUID {
			return fmt.Errorf("ordinary-recovery incident %d target changed before UID-preconditioned deletion: %v", incident, err)
		}
		if _, err := kubectl(ctx, "delete", "pod", currentTarget.Pod, "--preconditions=uid="+currentTarget.PodUID, "--wait=true", "--timeout=180s"); err != nil {
			return fmt.Errorf("ordinary-recovery incident %d delete only the captured voter Pod UID: %w", incident, err)
		}
		newPod, err := waitForGCSOrdinaryReplacement(ctx, currentSource, currentTarget, currentCredentialUID)
		if err != nil {
			return fmt.Errorf("ordinary-recovery incident %d replacement Pod: %w", incident, err)
		}
		replacementForward, err := startForward(ctx, currentTarget.Pod, out)
		if err != nil {
			return err
		}
		forwards = append(forwards, replacementForward)
		newStatus, err := waitForGCSOrdinaryVoter(ctx, currentSource, currentTarget, currentCredentialUID, baselineMembership.ConfigID, replacementForward, token, newPod)
		if err != nil {
			return fmt.Errorf("ordinary-recovery incident %d voter activation: %w", incident, err)
		}
		if !gcsOrdinaryIncarnationIsFresh(newStatus, priorIncarnations) {
			return fmt.Errorf("ordinary-recovery incident %d reused a prior logical identity or WAL", incident)
		}
		recoveryStatusEvidence, err := waitForGCSOrdinaryCurrentRecoveryStatus(ctx, source.ClusterID, recoveryTarget,
			string(newStatus.NodeID), replacementForward, currentSource.Targets[1].NodeID, observerForward,
			currentSource.Targets[2].NodeID, secondSurvivorForward, token)
		if err != nil {
			return fmt.Errorf("ordinary-recovery incident %d replacement archive/current-tip recovery status: %w", incident, err)
		}
		// The recovery-status API reports readiness and tip heights, not the
		// source used for each replayed slot. This records those facts against a
		// fixed pre-delete target; it is not an archive-source claim by itself.
		appendEvent("replacement_status_at_fixed_archive_and_current_tip", obj{"incident": incident, "evidence": recoveryStatusEvidence})
		replacement := capturedTarget(operator.FenceTarget{NodeID: string(newStatus.NodeID), Pod: currentTarget.Pod,
			PodUID: text(nested(newPod, "metadata", "uid")), WALIdentity: newStatus.WALIdentity}, newPod)
		currentSource.Targets[0] = replacement
		var removedCredential *peerAdmissionCredential
		for i := range currentPeerCredentials {
			if currentPeerCredentials[i].Member.ID == quepaxa.NodeID(currentTarget.NodeID) {
				copy := currentPeerCredentials[i]
				removedCredential = &copy
				break
			}
		}
		if removedCredential == nil {
			return fmt.Errorf("ordinary-recovery incident %d lacks the exact prior peer credential for the removed voter", incident)
		}
		priorPeerCredentials = append(priorPeerCredentials, *removedCredential)
		currentPeerCredentials, err = peerAdmissionCredentialsForTargets(ctx, currentSource, currentSource.Targets)
		if err != nil {
			return fmt.Errorf("ordinary-recovery incident %d current peer credentials: %w", incident, err)
		}
		probeResult, err := executePeerAdmissionProbe(ctx, fencerOwned, peerAdmissionProbeRequest{
			ClusterID: source.ClusterID, ConfigID: newStatus.ConfigID, Current: currentPeerCredentials, Previous: priorPeerCredentials,
		})
		if err != nil {
			return fmt.Errorf("ordinary-recovery incident %d stale peer admission probe: %w", incident, err)
		}
		appendEvent("stale_peer_admission_verified", obj{"incident": incident, "configID": probeResult.ConfigID,
			"currentReadTipSuccesses": probeResult.CurrentSuccesses, "previousPeerAuthDenials": probeResult.PreviousRejections})
		for _, voter := range []struct {
			forward *forward
			target  operator.FenceTarget
		}{{observerForward, currentSource.Targets[1]}, {secondSurvivorForward, currentSource.Targets[2]}, {replacementForward, replacement}} {
			membership, err := readMembershipWithToken(ctx, voter.forward, token)
			if err != nil || !gcsOrdinaryServingMembershipMatches(currentSource, currentTarget, replacement, baselineMembership.ConfigID, voter.target, membership) {
				return fmt.Errorf("ordinary-recovery incident %d voter did not expose the same fresh three-voter membership: %v", incident, err)
			}
		}
		appendEvent("fresh_same_ordinal_voter_serving", obj{"incident": incident, "oldTarget": currentTarget, "replacement": replacement, "membership": newStatus,
			"newCredentialUID": text(nested(newPod, "metadata", "annotations", "rhiza.mrchypark.dev/fencing-credential-uid"))})
		if err := waitRunOwnedOperatorAbsent(ctx, operatorOwned, fencerOwned); err != nil {
			return fmt.Errorf("controller state changed during ordinary voter recovery incident %d: %w", incident, err)
		}

		rowID := incident + 1
		rowValue := fmt.Sprintf("gcs-ordinary-post-recovery-%d", incident)
		postAck, err := writeGCSAcknowledgedRow(ctx, replacementForward, token, rowID, rowValue)
		if err != nil {
			return fmt.Errorf("ordinary-recovery incident %d continued-write ACK/readback: %w", incident, err)
		}
		requestSQL := fmt.Sprintf("INSERT INTO fencing_items VALUES (%d, '%s')", rowID, rowValue)
		retry, err := call(ctx, replacementForward, "/sql/execute", obj{"request_id": postAck.RequestID, "sql": requestSQL}, token)
		if err != nil {
			return fmt.Errorf("ordinary-recovery incident %d exact continued-write retry: %w", incident, err)
		}
		retryStatus, err := call(ctx, replacementForward, "/request/status", obj{"kind": "sql", "request_id": postAck.RequestID}, token)
		if err != nil || text(nested(retryStatus, "state")) != "committed" || integer(nested(retryStatus, "receipt", "slot")) != int64(postAck.Slot) {
			return fmt.Errorf("ordinary-recovery incident %d retry did not preserve its committed ACK: %v", incident, err)
		}
		wantRows := `[[1,"gcs-ordinary-baseline"],[2,"gcs-ordinary-post-recovery-1"]]`
		if incident == 2 {
			wantRows = `[[1,"gcs-ordinary-baseline"],[2,"gcs-ordinary-post-recovery-1"],[3,"gcs-ordinary-post-recovery-2"]]`
		}
		for _, voterForward := range []*forward{observerForward, secondSurvivorForward, replacementForward} {
			rows, err := call(ctx, voterForward, "/sql/query", obj{"sql": "SELECT id,value FROM fencing_items ORDER BY id", "consistency": "linearizable"}, token)
			if err != nil || string(encoded(rows["rows"])) != wantRows {
				return fmt.Errorf("ordinary-recovery incident %d linearizable SQL readback differs across voters: %v", incident, err)
			}
		}
		appendEvent("continued_write_ack_retry_and_three_voter_readback", obj{"incident": incident, "ack": postAck, "retry": retry, "expectedRows": wantRows})

		graphID := fmt.Sprintf("ordinary-%d", incident)
		graphValue := fmt.Sprintf("gcs-ordinary-graph-%d", incident)
		graphCommand := obj{"request_id": fmt.Sprintf("gcs-ordinary-graph-%d", incident),
			"cypher": fmt.Sprintf("CREATE (:GCSOrdinaryRecovery {id: '%s', value: '%s'})", graphID, graphValue)}
		graphAck, err := call(ctx, replacementForward, "/graph/execute", graphCommand, token)
		if err != nil {
			return fmt.Errorf("ordinary-recovery incident %d Graph ACK: %w", incident, err)
		}
		graphSlot, err := gcsOrdinaryMutationReceiptSlot(graphAck)
		if err != nil {
			return fmt.Errorf("ordinary-recovery incident %d Graph receipt: %w", incident, err)
		}
		graphStatus, err := call(ctx, replacementForward, "/request/status", obj{"kind": "graph", "request_id": graphCommand["request_id"]}, token)
		if err != nil || !gcsOrdinaryMutationRetryMatches(graphStatus, graphSlot) {
			return fmt.Errorf("ordinary-recovery incident %d Graph ACK status differs: %v", incident, err)
		}
		graphRetry, err := call(ctx, replacementForward, "/graph/execute", graphCommand, token)
		if err != nil {
			return fmt.Errorf("ordinary-recovery incident %d exact Graph retry: %w", incident, err)
		}
		graphRetrySlot, retryErr := gcsOrdinaryMutationReceiptSlot(graphRetry)
		graphRetryStatus, statusErr := call(ctx, replacementForward, "/request/status", obj{"kind": "graph", "request_id": graphCommand["request_id"]}, token)
		if retryErr != nil || graphRetrySlot != graphSlot || statusErr != nil || !gcsOrdinaryMutationRetryMatches(graphRetryStatus, graphSlot) {
			return fmt.Errorf("ordinary-recovery incident %d Graph retry changed its ACK: %v", incident, errors.Join(retryErr, statusErr))
		}
		graphQuery := obj{"cypher": fmt.Sprintf("MATCH (n:GCSOrdinaryRecovery {id: '%s'}) RETURN n.value", graphID), "consistency": "linearizable"}
		for _, voterForward := range []*forward{observerForward, secondSurvivorForward, replacementForward} {
			readback, err := call(ctx, voterForward, "/graph/query", graphQuery, token)
			if err != nil || string(encoded(readback["rows"])) != fmt.Sprintf("[[%q]]", graphValue) {
				return fmt.Errorf("ordinary-recovery incident %d Graph readback differs across voters: %v", incident, err)
			}
			if err := verifyGCSOrdinaryGraphValue(ctx, voterForward, token, "baseline", "gcs-ordinary-graph-baseline"); err != nil {
				return fmt.Errorf("ordinary-recovery incident %d baseline Graph readback differs across voters: %w", incident, err)
			}
		}

		kvKey := fmt.Sprintf("gcs-ordinary-recovery/%d", incident)
		kvValue := []byte(fmt.Sprintf("gcs-ordinary-kv-%d", incident))
		kvCommand := obj{"request_id": fmt.Sprintf("gcs-ordinary-kv-%d", incident), "key": kvKey, "value": kvValue}
		kvAck, err := call(ctx, replacementForward, "/kv/put", kvCommand, token)
		if err != nil {
			return fmt.Errorf("ordinary-recovery incident %d KV ACK: %w", incident, err)
		}
		kvSlot, err := gcsOrdinaryMutationReceiptSlot(kvAck)
		if err != nil {
			return fmt.Errorf("ordinary-recovery incident %d KV receipt: %w", incident, err)
		}
		kvStatus, err := call(ctx, replacementForward, "/request/status", obj{"kind": "kv", "request_id": kvCommand["request_id"]}, token)
		if err != nil || !gcsOrdinaryMutationRetryMatches(kvStatus, kvSlot) {
			return fmt.Errorf("ordinary-recovery incident %d KV ACK status differs: %v", incident, err)
		}
		kvRetry, err := call(ctx, replacementForward, "/kv/put", kvCommand, token)
		if err != nil {
			return fmt.Errorf("ordinary-recovery incident %d exact KV retry: %w", incident, err)
		}
		kvRetrySlot, retryErr := gcsOrdinaryMutationReceiptSlot(kvRetry)
		kvRetryStatus, statusErr := call(ctx, replacementForward, "/request/status", obj{"kind": "kv", "request_id": kvCommand["request_id"]}, token)
		if retryErr != nil || kvRetrySlot != kvSlot || statusErr != nil || !gcsOrdinaryMutationRetryMatches(kvRetryStatus, kvSlot) {
			return fmt.Errorf("ordinary-recovery incident %d KV retry changed its ACK: %v", incident, errors.Join(retryErr, statusErr))
		}
		kvQuery := obj{"key": kvKey, "consistency": "linearizable"}
		for _, voterForward := range []*forward{observerForward, secondSurvivorForward, replacementForward} {
			readback, err := call(ctx, voterForward, "/kv/get", kvQuery, token)
			if err != nil || nested(readback, "found") != true || nested(readback, "value") != base64.StdEncoding.EncodeToString(kvValue) {
				return fmt.Errorf("ordinary-recovery incident %d KV readback differs across voters: %v", incident, err)
			}
			if err := verifyGCSOrdinaryKVValue(ctx, voterForward, token, "gcs-ordinary-recovery/baseline", []byte("gcs-ordinary-kv-baseline")); err != nil {
				return fmt.Errorf("ordinary-recovery incident %d baseline KV readback differs across voters: %w", incident, err)
			}
		}
		appendEvent("continued_graph_kv_ack_retry_and_three_voter_readback", obj{"incident": incident,
			"graphRequestID": graphCommand["request_id"], "graphSlot": graphSlot, "graphRetry": graphRetry,
			"kvRequestID": kvCommand["request_id"], "kvSlot": kvSlot, "kvRetry": kvRetry})

		requiredCheckpointIndex := uint64(max(int64(postAck.Slot), graphSlot, kvSlot))
		if incident == 1 {
			checkpointBeforeSecondRecovery, err = waitForGCSOrdinaryCheckpoint(ctx, source, requiredCheckpointIndex, newStatus.ConfigID)
			if err != nil {
				return fmt.Errorf("ordinary-recovery first incarnation checkpoint: %w", err)
			}
			appendEvent("first_recovery_persisted_checkpoint", checkpointBeforeSecondRecovery)
		} else {
			checkpointAfterSecondRecovery, err := waitForGCSOrdinaryCheckpoint(ctx, source, requiredCheckpointIndex, newStatus.ConfigID)
			if err != nil {
				return fmt.Errorf("ordinary-recovery second incarnation checkpoint: %w", err)
			}
			if !gcsOrdinaryCheckpointAdvanced(checkpointBeforeSecondRecovery, checkpointAfterSecondRecovery, requiredCheckpointIndex) {
				return fmt.Errorf("ordinary-recovery persisted checkpoint did not advance past the second recovery writes")
			}
			appendEvent("second_recovery_persisted_checkpoint_advanced", obj{
				"before": checkpointBeforeSecondRecovery, "after": checkpointAfterSecondRecovery,
				"requiredAppliedIndex": requiredCheckpointIndex})
		}

		priorIncarnations = append(priorIncarnations, replacement)
		currentTarget = replacement
		currentCredentialUID = text(nested(newPod, "metadata", "annotations", "rhiza.mrchypark.dev/fencing-credential-uid"))
		if incident < 2 {
			if err := replacementForward.close(); err != nil {
				return fmt.Errorf("close first recovered voter observer before the next replacement: %w", err)
			}
		}
	}
	return nil
}

func requireGCSOrdinarySupervisedLaunch(statefulSet any, source gcsRecoverySource) error {
	if source.StatefulSet == "" || source.Container == "" || source.StatefulSetUID == "" {
		return fmt.Errorf("ordinary recovery source identity is incomplete for supervised launch verification")
	}
	if text(nested(statefulSet, "metadata", "name")) != source.StatefulSet ||
		text(nested(statefulSet, "metadata", "uid")) != source.StatefulSetUID {
		return fmt.Errorf("ordinary recovery StatefulSet identity changed before supervised launch verification")
	}
	container, err := boundContainer(statefulSet, source.Container)
	if err != nil {
		return fmt.Errorf("ordinary recovery bound application container: %w", err)
	}
	if nested(container, "name") != source.Container || supervisedLaunchShape(container) != "expected" {
		return fmt.Errorf("ordinary recovery requires the bound application container to run under the supervised fencer")
	}
	return nil
}

type gcsOrdinaryCheckpointEvidence struct {
	ConfigID           uint   `json:"configID"`
	SealConfigID       uint   `json:"sealConfigID"`
	SealDecisionSlot   uint64 `json:"sealDecisionSlot"`
	Generation         uint64 `json:"generation"`
	Index              uint64 `json:"index"`
	Hash               string `json:"hash"`
	RootHash           string `json:"rootHash"`
	PrefixHash         string `json:"prefixHash"`
	MembershipVersions int    `json:"membershipVersions"`
}

type gcsOrdinaryRecoveryTarget struct {
	CertifiedTip uint64 `json:"certifiedTip"`
	AppliedTip   uint64 `json:"appliedTip"`
	ArchiveTip   uint64 `json:"archiveTip"`
}

type gcsOrdinaryRecoveryStatusEvidence struct {
	Target      gcsOrdinaryRecoveryTarget     `json:"target"`
	CurrentTip  uint64                        `json:"currentTip"`
	Replacement network.VoterRecoveryStatus   `json:"replacement"`
	Survivors   []network.VoterRecoveryStatus `json:"survivors"`
}

func readGCSOrdinaryRecoveryStatus(ctx context.Context, f *forward, token string) (network.VoterRecoveryStatus, error) {
	data, err := call(ctx, f, "/recovery/status", nil, token)
	if err != nil {
		return network.VoterRecoveryStatus{}, err
	}
	var status network.VoterRecoveryStatus
	if err := json.Unmarshal(encoded(data), &status); err != nil {
		return network.VoterRecoveryStatus{}, err
	}
	return status, nil
}

func gcsOrdinaryRecoveryStatusMatches(status network.VoterRecoveryStatus, clusterID, nodeID string, target gcsOrdinaryRecoveryTarget) bool {
	return status.ClusterID == clusterID && status.NodeID == nodeID && status.Durability == "before-ack" &&
		status.Ready && status.Quorum && status.CertifiedTip >= target.CertifiedTip &&
		status.AppliedTip >= target.AppliedTip && status.ArchiveTip >= target.ArchiveTip
}

func gcsOrdinaryRecoveryTargetFromSurvivors(clusterID, firstNodeID string, first network.VoterRecoveryStatus,
	secondNodeID string, second network.VoterRecoveryStatus) (gcsOrdinaryRecoveryTarget, bool) {
	if !gcsOrdinaryRecoveryStatusMatches(first, clusterID, firstNodeID, gcsOrdinaryRecoveryTarget{}) ||
		!gcsOrdinaryRecoveryStatusMatches(second, clusterID, secondNodeID, gcsOrdinaryRecoveryTarget{}) ||
		first.CertifiedTip == 0 || first.CertifiedTip != second.CertifiedTip ||
		first.AppliedTip < first.CertifiedTip || second.AppliedTip < second.CertifiedTip ||
		(first.ArchiveTip == 0 && second.ArchiveTip == 0) {
		return gcsOrdinaryRecoveryTarget{}, false
	}
	archiveTip := max(first.ArchiveTip, second.ArchiveTip)
	return gcsOrdinaryRecoveryTarget{CertifiedTip: first.CertifiedTip, AppliedTip: first.CertifiedTip, ArchiveTip: archiveTip}, true
}

func gcsOrdinaryRecoveryStatusesAtCurrentTip(statuses ...network.VoterRecoveryStatus) (uint64, bool) {
	if len(statuses) == 0 {
		return 0, false
	}
	var currentTip uint64
	for _, status := range statuses {
		if !status.Ready || !status.Quorum || status.CertifiedTip > currentTip {
			currentTip = status.CertifiedTip
		}
	}
	for _, status := range statuses {
		if !status.Ready || !status.Quorum || status.CertifiedTip != currentTip || status.AppliedTip < currentTip {
			return 0, false
		}
	}
	return currentTip, currentTip > 0
}

func waitForGCSOrdinaryRecoveryTarget(ctx context.Context, clusterID, firstNodeID string, firstForward *forward,
	secondNodeID string, secondForward *forward, token string) (gcsOrdinaryRecoveryTarget, network.VoterRecoveryStatus,
	network.VoterRecoveryStatus, error) {
	var target gcsOrdinaryRecoveryTarget
	var firstStatus, secondStatus network.VoterRecoveryStatus
	err := wait(ctx, "stable ready survivor certified/applied/archive tip before replacement", func() (bool, error) {
		first, err := readGCSOrdinaryRecoveryStatus(ctx, firstForward, token)
		if err != nil {
			return false, nil
		}
		second, err := readGCSOrdinaryRecoveryStatus(ctx, secondForward, token)
		if err != nil {
			return false, nil
		}
		var ok bool
		target, ok = gcsOrdinaryRecoveryTargetFromSurvivors(clusterID, firstNodeID, first, secondNodeID, second)
		if !ok {
			return false, nil
		}
		firstStatus, secondStatus = first, second
		return true, nil
	})
	return target, firstStatus, secondStatus, err
}

func waitForGCSOrdinaryCurrentRecoveryStatus(ctx context.Context, clusterID string, target gcsOrdinaryRecoveryTarget,
	replacementNodeID string, replacementForward *forward, firstSurvivorNodeID string, firstSurvivorForward *forward,
	secondSurvivorNodeID string, secondSurvivorForward *forward, token string) (gcsOrdinaryRecoveryStatusEvidence, error) {
	var evidence gcsOrdinaryRecoveryStatusEvidence
	err := wait(ctx, "replacement ready, quorum-backed, and current at fixed archive target", func() (bool, error) {
		replacement, err := readGCSOrdinaryRecoveryStatus(ctx, replacementForward, token)
		if err != nil {
			return false, nil
		}
		first, err := readGCSOrdinaryRecoveryStatus(ctx, firstSurvivorForward, token)
		if err != nil {
			return false, nil
		}
		second, err := readGCSOrdinaryRecoveryStatus(ctx, secondSurvivorForward, token)
		if err != nil {
			return false, nil
		}
		if !gcsOrdinaryRecoveryStatusMatches(replacement, clusterID, replacementNodeID, target) ||
			!gcsOrdinaryRecoveryStatusMatches(first, clusterID, firstSurvivorNodeID, gcsOrdinaryRecoveryTarget{CertifiedTip: target.CertifiedTip, AppliedTip: target.AppliedTip}) ||
			!gcsOrdinaryRecoveryStatusMatches(second, clusterID, secondSurvivorNodeID, gcsOrdinaryRecoveryTarget{CertifiedTip: target.CertifiedTip, AppliedTip: target.AppliedTip}) {
			return false, nil
		}
		currentTip, ok := gcsOrdinaryRecoveryStatusesAtCurrentTip(replacement, first, second)
		if !ok {
			return false, nil
		}
		evidence = gcsOrdinaryRecoveryStatusEvidence{Target: target, CurrentTip: currentTip,
			Replacement: replacement, Survivors: []network.VoterRecoveryStatus{first, second}}
		return true, nil
	})
	return evidence, err
}

func waitForGCSOrdinaryCheckpoint(ctx context.Context, source gcsRecoverySource, minimumIndex uint64, expectedSealConfigID uint) (gcsOrdinaryCheckpointEvidence, error) {
	var evidence gcsOrdinaryCheckpointEvidence
	if minimumIndex == 0 || expectedSealConfigID == 0 {
		return evidence, fmt.Errorf("checkpoint target index and certified configuration are required")
	}
	if err := wait(ctx, "persisted certified GCS checkpoint through ordinary-recovery writes", func() (bool, error) {
		bucket, err := newGCSArchiveReader(source)
		if err != nil {
			return false, err
		}
		defer bucket.Close()
		manager := checkpoint.NewManager(bucket, source.Prefix, "", 1)
		if err := manager.Load(ctx); err != nil {
			return false, err
		}
		root := manager.Latest()
		if root == nil || root.Index < minimumIndex {
			return false, nil
		}
		archive := recovery.NewManager(bucket, source.Prefix, recovery.ArchiveConfigID)
		defer archive.Close()
		if err := archive.Load(ctx); err != nil {
			return false, err
		}
		seal, decisionSlot, prefix, versions, found, err := gcsOrdinaryCertifiedCheckpoint(ctx, archive, root)
		if err != nil {
			return false, err
		}
		if !found {
			return false, nil
		}
		if !gcsOrdinaryCheckpointSealMatches(root, seal, decisionSlot, expectedSealConfigID, prefix) {
			return false, fmt.Errorf("persisted checkpoint root differs from the certified post-reconfiguration seal")
		}
		evidence = gcsOrdinaryCheckpointEvidence{
			ConfigID: root.ConfigID, SealConfigID: seal.ConfigID, SealDecisionSlot: uint64(decisionSlot),
			Generation: root.Generation, Index: root.Index, Hash: hex.EncodeToString(root.Hash[:]),
			RootHash: hex.EncodeToString(root.RootHash[:]), PrefixHash: hex.EncodeToString(seal.PrefixHash[:]),
			MembershipVersions: versions,
		}
		if evidence.ConfigID != 1 || evidence.Generation == 0 || evidence.Hash == "" || evidence.RootHash == "" || evidence.PrefixHash == "" {
			return false, fmt.Errorf("persisted checkpoint identity is incomplete")
		}
		return true, nil
	}); err != nil {
		return gcsOrdinaryCheckpointEvidence{}, err
	}
	return evidence, nil
}

func gcsOrdinaryCheckpointAdvanced(before, after gcsOrdinaryCheckpointEvidence, minimumIndex uint64) bool {
	return before.ConfigID == 1 && after.ConfigID == before.ConfigID &&
		before.SealConfigID > 0 && after.SealConfigID > before.SealConfigID && after.MembershipVersions > before.MembershipVersions &&
		before.SealDecisionSlot > before.Index && after.SealDecisionSlot > after.Index && after.SealDecisionSlot > before.SealDecisionSlot &&
		before.Generation > 0 && after.Generation > before.Generation &&
		before.Index > 0 && after.Index >= minimumIndex && after.Index > before.Index &&
		before.Hash != "" && after.Hash != "" && before.Hash != after.Hash &&
		before.RootHash != "" && after.RootHash != "" && before.RootHash != after.RootHash &&
		before.PrefixHash != "" && after.PrefixHash != "" && before.PrefixHash != after.PrefixHash
}

func gcsOrdinaryCertifiedCheckpoint(ctx context.Context, archive *recovery.Manager, root *checkpoint.Checkpoint) (quepaxa.CheckpointSeal, quepaxa.Slot, [32]byte, int, bool, error) {
	var empty quepaxa.CheckpointSeal
	var zero [32]byte
	if archive == nil || root == nil || root.Index == 0 {
		return empty, 0, zero, 0, false, fmt.Errorf("checkpoint archive evidence is incomplete")
	}
	baseSeal, baseDecision, hasBase := archive.RecoveryBase()
	from := quepaxa.Slot(1)
	prefix := zero
	prefixSlot := quepaxa.Slot(0)
	var seal *quepaxa.CheckpointSeal
	var sealDecisionSlot quepaxa.Slot
	if hasBase {
		encoded, err := quepaxa.EncodeCheckpointSeal(baseSeal)
		if err != nil || !bytes.Equal(encoded, baseDecision.Value) || baseDecision.Hash != sha256.Sum256(baseDecision.Value) ||
			len(baseDecision.Certificate) == 0 || baseDecision.Slot <= baseSeal.Index || baseSeal.Index > quepaxa.Slot(root.Index) {
			return empty, 0, zero, 0, false, fmt.Errorf("archive recovery base checkpoint decision is malformed")
		}
		prefix, prefixSlot = baseSeal.PrefixHash, baseSeal.Index
		from = baseSeal.Index + 1
		if uint64(baseSeal.Index) == root.Index {
			copy := baseSeal
			seal, sealDecisionSlot = &copy, baseDecision.Slot
		}
	}
	if root.Index < uint64(prefixSlot) {
		return empty, 0, zero, 0, false, fmt.Errorf("checkpoint root predates the authenticated archive base")
	}
	tip := archive.Tip()
	for from <= tip {
		decisions, _, err := archive.DecisionsFrom(ctx, from, 256)
		if err != nil {
			return empty, 0, zero, 0, false, err
		}
		if len(decisions) == 0 {
			break
		}
		for _, decision := range decisions {
			if decision.Certificate == nil || len(decision.Certificate) == 0 || sha256.Sum256(decision.Value) != decision.Hash {
				return empty, 0, zero, 0, false, fmt.Errorf("archived checkpoint decision lacks its immutable certified value")
			}
			if decision.Slot == prefixSlot+1 && decision.Slot <= quepaxa.Slot(root.Index) {
				prefix = quepaxa.AdvancePrefixHash(prefix, decision.Slot, decision.Hash)
				prefixSlot = decision.Slot
			} else if decision.Slot <= quepaxa.Slot(root.Index) {
				return empty, 0, zero, 0, false, fmt.Errorf("archive prefix has a gap before checkpoint index %d", root.Index)
			}
			candidate, isCheckpoint, decodeErr := quepaxa.DecodeCheckpointSeal(decision.Value)
			if decodeErr != nil {
				return empty, 0, zero, 0, false, decodeErr
			}
			if isCheckpoint && uint64(candidate.Index) == root.Index {
				if seal != nil {
					prior, priorErr := quepaxa.EncodeCheckpointSeal(*seal)
					current, currentErr := quepaxa.EncodeCheckpointSeal(candidate)
					if priorErr != nil || currentErr != nil || !bytes.Equal(prior, current) || sealDecisionSlot != decision.Slot {
						return empty, 0, zero, 0, false, fmt.Errorf("archive contains conflicting checkpoint seals for index %d", root.Index)
					}
					continue
				}
				seal, sealDecisionSlot = &candidate, decision.Slot
			}
		}
		from = decisions[len(decisions)-1].Slot + 1
	}
	if prefixSlot != quepaxa.Slot(root.Index) || seal == nil {
		return empty, 0, zero, 0, false, nil
	}
	versions := 0
	if seal.Membership != nil {
		versions = len(seal.Membership.Transitions)
	}
	return *seal, sealDecisionSlot, prefix, versions, true, nil
}

func gcsOrdinaryCheckpointSealMatches(root *checkpoint.Checkpoint, seal quepaxa.CheckpointSeal, decisionSlot quepaxa.Slot, expectedConfigID uint, prefix [32]byte) bool {
	if root == nil || root.ConfigID != 1 || expectedConfigID == 0 || seal.ConfigID != expectedConfigID ||
		seal.Index == 0 || uint64(seal.Index) != root.Index || seal.RootHash != root.RootHash || seal.StateHash != root.Hash ||
		seal.PrefixHash == ([32]byte{}) || seal.PrefixHash != prefix || decisionSlot <= seal.Index {
		return false
	}
	if seal.ConfigID == 1 {
		return seal.Membership == nil
	}
	return seal.Membership != nil && seal.Membership.Genesis.ConfigID == 1 && len(seal.Membership.Transitions) > 0 &&
		seal.Membership.Genesis.ConfigID+uint(len(seal.Membership.Transitions)) == seal.ConfigID
}

func gcsOrdinaryMutationReceiptSlot(response obj) (int64, error) {
	slot := integer(response["slot"])
	if text(response["status"]) != "committed" || slot <= 0 {
		return 0, fmt.Errorf("mutation response lacks a committed nonzero receipt")
	}
	return slot, nil
}

func gcsOrdinaryMutationRetryMatches(response obj, slot int64) bool {
	return slot > 0 && text(response["state"]) == "committed" && text(nested(response, "receipt", "status")) == "committed" && integer(nested(response, "receipt", "slot")) == slot
}

func verifyGCSOrdinaryGraphValue(ctx context.Context, f *forward, token, id, value string) error {
	result, err := call(ctx, f, "/graph/query", obj{"cypher": fmt.Sprintf("MATCH (n:GCSOrdinaryRecovery {id: '%s'}) RETURN n.value", id), "consistency": "linearizable"}, token)
	if err != nil {
		return err
	}
	if string(encoded(result["rows"])) != fmt.Sprintf("[[%q]]", value) {
		return fmt.Errorf("Graph value %q was not uniquely read back", id)
	}
	return nil
}

func verifyGCSOrdinaryKVValue(ctx context.Context, f *forward, token, key string, value []byte) error {
	result, err := call(ctx, f, "/kv/get", obj{"key": key, "consistency": "linearizable"}, token)
	if err != nil {
		return err
	}
	if nested(result, "found") != true || nested(result, "value") != base64.StdEncoding.EncodeToString(value) {
		return fmt.Errorf("KV value %q was not read back", key)
	}
	return nil
}

func gcsOrdinaryIncarnationIsFresh(status network.MembershipStatus, prior []operator.FenceTarget) bool {
	if status.NodeID == "" || status.WALIdentity == "" {
		return false
	}
	for _, incarnation := range prior {
		if string(status.NodeID) == incarnation.NodeID || status.WALIdentity == incarnation.WALIdentity {
			return false
		}
	}
	return true
}

func waitForGCSOrdinaryReplacement(ctx context.Context, source gcsRecoverySource, old operator.FenceTarget, oldCredentialUID string) (obj, error) {
	var pod obj
	err := wait(ctx, "fresh same-ordinal voter Pod after operator shutdown", func() (bool, error) {
		current, err := get(ctx, "pod", old.Pod)
		if err != nil {
			if strings.Contains(err.Error(), "NotFound") {
				return false, nil
			}
			return false, err
		}
		uid := text(nested(current, "metadata", "uid"))
		container := namedContainerStatus(current, source.Container)
		containerID := text(nested(container, "containerID"))
		credentialUID := text(nested(current, "metadata", "annotations", "rhiza.mrchypark.dev/fencing-credential-uid"))
		if uid == "" || uid == old.PodUID || !gcsPodOwnedBy(current, source.StatefulSetUID) || nested(current, "metadata", "deletionTimestamp") != nil ||
			!podReady(current) || container == nil || nested(container, "ready") != true || containerID == "" || (old.ContainerID != "" && containerID == old.ContainerID) ||
			credentialUID == "" || credentialUID == oldCredentialUID {
			return false, nil
		}
		sts, err := get(ctx, "statefulset", source.StatefulSet)
		if err != nil || text(nested(sts, "metadata", "uid")) != source.StatefulSetUID || integer(nested(sts, "spec", "replicas")) != 3 {
			return false, fmt.Errorf("source StatefulSet identity or three-replica intent changed during ordinary recovery: %v", err)
		}
		pod = current
		return true, nil
	})
	return pod, err
}

func waitForGCSOrdinaryVoter(ctx context.Context, source gcsRecoverySource, old operator.FenceTarget, oldCredentialUID string, configID uint, f *forward, token string, pod obj) (network.MembershipStatus, error) {
	var status network.MembershipStatus
	err := wait(ctx, "authenticated fresh voter membership after ordinary replacement", func() (bool, error) {
		current, err := get(ctx, "pod", old.Pod)
		if err != nil || text(nested(current, "metadata", "uid")) != text(nested(pod, "metadata", "uid")) ||
			text(nested(current, "metadata", "annotations", "rhiza.mrchypark.dev/fencing-credential-uid")) == oldCredentialUID {
			return false, nil
		}
		container := namedContainerStatus(current, source.Container)
		if nested(current, "metadata", "deletionTimestamp") != nil || !podReady(current) || container == nil || nested(container, "ready") != true ||
			text(nested(container, "containerID")) != text(nested(namedContainerStatus(pod, source.Container), "containerID")) {
			return false, fmt.Errorf("replacement Pod or container identity changed before membership activation")
		}
		observed, err := readMembershipWithToken(ctx, f, token)
		if err != nil {
			return false, nil
		}
		if !gcsOrdinaryMembershipReady(source, old, configID, observed) {
			return false, nil
		}
		status = observed
		return true, nil
	})
	return status, err
}

func gcsOrdinaryBaselineMembershipReady(source gcsRecoverySource, observer operator.FenceTarget, status network.MembershipStatus) bool {
	if status.ClusterID != source.ClusterID || string(status.NodeID) != observer.NodeID || status.WALIdentity != observer.WALIdentity ||
		status.ConfigID == 0 || status.Pending || !status.Voting || len(status.Voters) != len(source.Targets) {
		return false
	}
	want := make(map[string]bool, len(source.Targets))
	for _, target := range source.Targets {
		if target.NodeID == "" || want[target.NodeID] {
			return false
		}
		want[target.NodeID] = true
	}
	for _, voter := range status.Voters {
		id := string(voter)
		if !want[id] {
			return false
		}
		delete(want, id)
	}
	return len(want) == 0
}

func gcsOrdinaryMembershipReady(source gcsRecoverySource, old operator.FenceTarget, configID uint, status network.MembershipStatus) bool {
	return status.ClusterID == source.ClusterID && status.ConfigID > configID && !status.Pending && status.Voting &&
		status.NodeID != "" && string(status.NodeID) != old.NodeID && status.WALIdentity != "" &&
		status.WALIdentity != old.WALIdentity && len(status.Voters) == 3 && gcsOrdinaryVotersIncludeSurvivors(status, source.Targets, old)
}

func gcsOrdinaryServingMembershipMatches(source gcsRecoverySource, old, replacement operator.FenceTarget, configID uint,
	target operator.FenceTarget, status network.MembershipStatus) bool {
	if status.ClusterID != source.ClusterID || status.ConfigID <= configID || status.Pending || !status.Voting ||
		string(status.NodeID) != target.NodeID || status.WALIdentity != target.WALIdentity {
		return false
	}
	return gcsOrdinaryVoterSetMatches(status, []string{source.Targets[1].NodeID, source.Targets[2].NodeID, replacement.NodeID})
}

func gcsOrdinaryVotersIncludeSurvivors(status network.MembershipStatus, targets []operator.FenceTarget, old operator.FenceTarget) bool {
	want := make([]string, 0, 3)
	for _, target := range targets {
		if target.PodUID != old.PodUID {
			want = append(want, target.NodeID)
		}
	}
	if len(want) != 2 {
		return false
	}
	want = append(want, string(status.NodeID))
	return gcsOrdinaryVoterSetMatches(status, want)
}

func gcsOrdinaryVoterSetMatches(status network.MembershipStatus, expected []string) bool {
	if len(expected) != len(status.Voters) {
		return false
	}
	want := make(map[string]bool, len(expected))
	for _, id := range expected {
		if id == "" || want[id] {
			return false
		}
		want[id] = true
	}
	for _, voter := range status.Voters {
		id := string(voter)
		if !want[id] {
			return false
		}
		delete(want, id)
	}
	return len(want) == 0
}
