package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/thanos-io/objstore"
)

const gcsManualTerminalApprovalWait = 50 * time.Minute

func qualifyGCSManualTerminal(ctx context.Context) (resultErr error) {
	if os.Getenv("RHIZA_GCS_MANUAL_TERMINAL") != "yes" {
		return fmt.Errorf("manual terminal GCS qualification is default-off; set RHIZA_GCS_MANUAL_TERMINAL=yes explicitly")
	}
	out := strings.TrimSpace(os.Getenv("FENCING_E2E_OUT"))
	if out == "" {
		out = "gcs-manual-terminal-results"
	}
	if err := os.MkdirAll(out, 0700); err != nil {
		return err
	}
	if info, err := os.Lstat(out); err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("manual terminal result directory must be private")
	}
	events, err := os.OpenFile(filepath.Join(out, gcsManualTerminalEventsName), os.O_CREATE|os.O_EXCL|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	var eventErr error
	event := func(name string, value any) {
		if eventErr == nil {
			_, eventErr = fmt.Fprintln(events, string(encoded(obj{"event": name, "time": time.Now().UTC(), "data": value})))
		}
	}
	var faults []*gcsChaosFault
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		for i := len(faults) - 1; i >= 0; i-- {
			if err := faults[i].Restore(cleanupCtx); err != nil {
				resultErr = errors.Join(resultErr, fmt.Errorf("restore owned manual-terminal GCS fault: %w", err))
			}
			event("chaos_cleanup", gcsChaosEvidence(faults[i]))
		}
		if err := events.Close(); err != nil {
			resultErr = errors.Join(resultErr, err)
		}
		if eventErr != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("write manual-terminal event record: %w", eventErr))
		}
	}()

	source, err := captureGCSRecoverySource(ctx, out)
	if err != nil {
		return err
	}
	if len(source.Targets) != 3 || source.Durability != "async" && source.Durability != "before-ack" {
		return fmt.Errorf("manual terminal qualification requires a three-voter GCS source with supported durability")
	}
	allowDataLoss := source.Durability == "async"
	if err := requireExplicitGCSRecoveryProfile(ctx, source, allowDataLoss); err != nil {
		return err
	}
	archive, err := newGCSArchiveReader(source)
	if err != nil {
		return err
	}
	defer archive.Close()
	adminToken, err := sourceAdminToken(ctx, source.StatefulSet, source.Container)
	if err != nil {
		return err
	}
	forward, err := startForward(ctx, source.Targets[0].Pod, out)
	if err != nil {
		return err
	}
	if _, err := call(ctx, forward, "/sql/execute", obj{"request_id": "gcs-manual-terminal-create-table", "sql": "CREATE TABLE fencing_items (id INTEGER PRIMARY KEY, value TEXT)"}, adminToken); err != nil {
		forward.close()
		return fmt.Errorf("create manual-terminal source table: %w", err)
	}
	sourceAck, err := writeGCSAcknowledgedRow(ctx, forward, adminToken, 1, "gcs-manual-terminal-source")
	forward.close()
	if err != nil {
		return fmt.Errorf("acknowledge manual-terminal source row: %w", err)
	}
	var before gcsArchiveHeadEvidence
	archiveCtx, cancelArchive := context.WithTimeout(ctx, gcsAsyncNonemptyMaxHeadWait)
	err = wait(archiveCtx, "persist manual-terminal source row in GCS", func() (bool, error) {
		head, err := readGCSArchiveHead(archiveCtx, archive, source.Prefix, false)
		if err != nil {
			return false, err
		}
		if head.Sealed {
			return false, fmt.Errorf("manual-terminal source archive was already sealed")
		}
		if head.Tip < sourceAck.Slot {
			return false, nil
		}
		before = head
		return true, nil
	})
	cancelArchive()
	if err != nil {
		return fmt.Errorf("capture nonempty source archive before manual terminal recovery: %w", err)
	}
	if before.Tip == 0 || before.Version == "" || before.HeadSHA256 == "" {
		return fmt.Errorf("manual-terminal source archive is not a persisted nonempty GCS tip")
	}
	event("source_archive_captured", obj{"ack": sourceAck, "head": before})

	if err := verifyGCSControlPodSnapshots(ctx, source.AgentPods); err != nil {
		return fmt.Errorf("runtime-agent identities changed before manual terminal isolation: %w", err)
	}
	agentFault, err := createGCSControlLoss(ctx, source.Namespace, qualificationRunID, "manual-terminal", "runtime-agent", source.AgentPods)
	if agentFault != nil {
		faults = append(faults, agentFault)
	}
	if err != nil {
		return fmt.Errorf("isolate the captured runtime-agent cohort for manual terminal review: %w", err)
	}
	if !gcsFaultSourceMatches(agentFault, source.AgentPods) {
		return fmt.Errorf("manual-terminal runtime-agent fault differs from captured DaemonSet Pod identities")
	}
	if err := agentFault.WaitInjected(ctx); err != nil {
		return fmt.Errorf("manual-terminal runtime-agent isolation was not injected: %w", err)
	}
	cohortFault, err := partitionGCSVoterCohort(ctx, source.Namespace, qualificationRunID, "manual-terminal", source.Targets)
	if cohortFault != nil {
		faults = append(faults, cohortFault)
	}
	if err != nil {
		return fmt.Errorf("partition the exact source voter cohort: %w", err)
	}
	if err := cohortFault.WaitInjected(ctx); err != nil {
		return fmt.Errorf("manual-terminal voter partition was not injected: %w", err)
	}
	event("source_cohort_isolated_for_manual_review", obj{"agentFault": gcsChaosEvidence(agentFault), "voterFault": gcsChaosEvidence(cohortFault)})

	request, name, recoveryID, err := explicitRecoveryManifest(source, qualificationRunID, "manual-terminal", source.Durability, allowDataLoss)
	if err != nil {
		return err
	}
	spec := nested(request, "spec").(obj)
	fenceSpec := nested(spec, "fence").(obj)
	// This selects the frozen-source workflow; only the independent CRD approval
	// and canonical preflight can authorize registration or create a target claim.
	fenceSpec["confirmed"] = true
	fenceSpec["evidence"] = "manual-terminal-administrator-approval-required"
	if _, err := commandWithInput(ctx, encoded(request), "kubectl", "--context", kubeContext, "-n", source.Namespace, "create", "-f", "-"); err != nil {
		return fmt.Errorf("create run-owned manual-terminal Recovery: %w", err)
	}
	recoveryUID, err := waitForManualTerminalRecovery(ctx, source, name, recoveryID, allowDataLoss)
	if err != nil {
		return err
	}
	if err := requestGenerationFence(ctx, source, explicitGenerationFenceRequest(source, recoveryID), out); err != nil {
		return fmt.Errorf("request exact manual-terminal source generation fence: %w", err)
	}
	pending, err := waitForPendingGCSFenceOperation(ctx, source, recoveryID)
	if err != nil {
		return err
	}
	if err := validateExplicitGenerationFenceRequest(pending.Request, source, recoveryID); err != nil {
		return err
	}

	challenge, challengeDigest, err := waitForManualTerminalChallenge(ctx, source, name, recoveryUID, pending)
	if err != nil {
		return err
	}
	challengeName, err := operator.ManualRecoveryAttestationName(recoveryUID)
	if err != nil {
		return err
	}
	challengePath := filepath.Join(out, gcsManualTerminalChallengeName)
	if err := writeNewPrivateAckLossFile(challengePath, encoded(obj{"challenge": challenge, "challengeDigest": challengeDigest, "attestationName": challengeName})); err != nil {
		return err
	}
	event("independent_manual_approval_required", obj{"challengeDigest": challengeDigest, "attestationName": challengeName, "expiresAt": challenge.ExpiresAt})

	observerDeployment, err := get(ctx, "deployment", "rhiza-fencer")
	if err != nil {
		return err
	}
	readScope, err := newGCSAckLossReadScope(ctx, source, observerDeployment)
	if err != nil {
		return err
	}
	stateStore, _, authorityClient, controlBucket, closeStores, err := openGCSAckLossAuthorityStores(ctx, source, readScope)
	if err != nil {
		return err
	}
	if authorityClient != nil {
		defer func() { resultErr = errors.Join(resultErr, authorityClient.Close()) }()
	}
	if closeStores != nil {
		defer func() { resultErr = errors.Join(resultErr, closeStores()) }()
	}
	controlPrefix := readScope.Prefix
	if controlPrefix != path.Join(strings.TrimSuffix(source.Prefix, "/"+source.ClusterID), "control") {
		return fmt.Errorf("manual-terminal read-only CONTROL scope differs from the source binding")
	}
	sourceRoot, _, err := stateStore.ReadRoot(ctx, source.StatefulSetUID, source.ClusterID)
	if err != nil || sourceRoot.Ledger.ClosedBy != recoveryID {
		return errors.Join(err, fmt.Errorf("manual-terminal source CONTROL root is not closed by the exact fence operation"))
	}
	rootDigest := sha256.Sum256(encoded([]any{source.Namespace, sourceRoot}))
	if hex.EncodeToString(rootDigest[:]) != challenge.RootDigest {
		return fmt.Errorf("manual-terminal challenge is not bound to the read-only closed CONTROL root")
	}
	fenceDigest := sha256.Sum256(encoded(pending.Request))
	if hex.EncodeToString(fenceDigest[:]) != challenge.FenceRequestHash {
		return fmt.Errorf("manual-terminal challenge is not bound to the exact generation fence request")
	}
	successorClaimKey, err := manualTerminalSuccessorClaimKey(source, controlPrefix)
	if err != nil {
		return err
	}
	targetPrefix := path.Join(path.Dir(source.Prefix), challenge.TargetClusterID)
	if err := verifyManualTerminalUnclaimed(ctx, archive, controlBucket, source, challenge, successorClaimKey, targetPrefix, controlPrefix); err != nil {
		return err
	}
	if err := waitForManualTerminalApproval(ctx, challenge, challengeDigest, challengeName, archive, controlBucket, source,
		successorClaimKey, targetPrefix, controlPrefix); err != nil {
		return err
	}
	approval, approvalDigest, err := readAndValidateManualTerminalApproval(ctx, source.Namespace, challengeName, challenge, challengeDigest)
	if err != nil {
		return err
	}
	event("independent_manual_approval_observed", obj{"approvalUID": approval.UID, "approvalDigest": approvalDigest})

	recovery, err := waitForManualTerminalRecoveryComplete(ctx, source, name, recoveryUID, recoveryID, challenge, challengeDigest, approvalDigest)
	if err != nil {
		return err
	}
	claimExists, err := controlBucket.Exists(ctx, successorClaimKey)
	if err != nil || !claimExists {
		return errors.Join(err, fmt.Errorf("canonical manual successor claim was not committed after approval"))
	}
	sealedHead, err := readGCSArchiveHead(ctx, archive, source.Prefix, false)
	if err != nil {
		return fmt.Errorf("read sealed manual-terminal source archive: %w", err)
	}
	if err := validateSealedNonemptyHead(before, sealedHead); err != nil {
		return err
	}
	if len(recovery.Status.TargetIdentities) == 0 || recovery.Status.TargetIdentities[0].Pod == "" {
		return fmt.Errorf("manual-terminal recovery has no target voter identity")
	}
	targetAdmin, err := recoveryAdminToken(ctx, source.Namespace, recovery.Status.SecretName)
	if err != nil {
		return err
	}
	targetForward, err := startForward(ctx, recovery.Status.TargetIdentities[0].Pod, out)
	if err != nil {
		return err
	}
	defer targetForward.close()
	membership, err := readMembershipWithToken(ctx, targetForward, targetAdmin)
	if err != nil || membership.ClusterID != challenge.TargetClusterID || membership.Pending || !membership.Voting {
		return fmt.Errorf("manual-terminal target did not pass authenticated voter membership: %v", err)
	}
	rows := `[[1,"gcs-manual-terminal-source"]]`
	readback, err := call(ctx, targetForward, "/sql/query", obj{"sql": "SELECT id,value FROM fencing_items ORDER BY id", "consistency": "linearizable"}, targetAdmin)
	if err != nil || string(encoded(readback["rows"])) != rows {
		return fmt.Errorf("manual-terminal target readback differs from the acknowledged source row: %v", err)
	}
	targetAck, err := writeGCSAcknowledgedRow(ctx, targetForward, targetAdmin, 2, "gcs-manual-terminal-target")
	if err != nil {
		return fmt.Errorf("manual-terminal target ACK/readback failed: %w", err)
	}
	proofItem, err := get(ctx, "rhizafence", pending.Name)
	if err != nil {
		return err
	}
	if nested(proofItem, "status", "proof", "independentV2") != nil {
		return fmt.Errorf("manual-terminal run gained IndependentV2 proof and cannot be reported as manual evidence")
	}
	evidence := obj{
		"qualification": gcsManualTerminalQualification,
		"profile":       obj{"provider": "gcs", "noPVC": true, "sourceDurability": source.Durability, "allowDataLoss": allowDataLoss},
		"sourceAck":     sourceAck, "sourceHeadBeforeFence": before, "sealedSourceHead": sealedHead,
		"fence": pending.Request, "fenceProof": nested(proofItem, "status", "proof"), "challenge": challenge,
		"challengeDigest": challengeDigest, "independentApproval": approval, "approvalDigest": approvalDigest,
		"preApprovalTargetUnclaimed": true, "recovery": recovery, "targetQuorumVerified": true,
		"recoveredRows": string(encoded(readback["rows"])), "targetAck": targetAck,
		"independentV2Absent": true,
	}
	if err := writeNewPrivateAckLossFile(filepath.Join(out, gcsManualTerminalEvidenceName), encoded(evidence)); err != nil {
		return err
	}
	event("manual_terminal_recovery_readback_complete", obj{"source": source.ClusterID, "target": challenge.TargetClusterID})
	return nil
}

func waitForManualTerminalRecovery(ctx context.Context, source gcsRecoverySource, name, recoveryID string, allowDataLoss bool) (string, error) {
	var uid string
	err := wait(ctx, "manual-terminal Recovery awaits its exact generation fence", func() (bool, error) {
		resource, err := get(ctx, "rhizarecovery", name)
		if err != nil {
			return false, err
		}
		if text(nested(resource, "metadata", "namespace")) != source.Namespace ||
			text(nested(resource, "metadata", "labels", "chaos.rhiza.io/run")) != qualificationRunID ||
			nested(resource, "spec", "sourceClusterID") != source.ClusterID || nested(resource, "spec", "recoveryID") != recoveryID ||
			nested(resource, "spec", "statefulSet") != source.StatefulSet || nested(resource, "spec", "container") != source.Container ||
			nested(resource, "spec", "durability") != source.Durability || nested(resource, "spec", "allowDataLoss") != allowDataLoss ||
			nested(resource, "spec", "fence", "confirmed") != true || nested(resource, "spec", "fence", "evidence") != "manual-terminal-administrator-approval-required" ||
			text(nested(resource, "status", "target")) != "" {
			return false, fmt.Errorf("manual-terminal Recovery changed identity or claimed a target before approval")
		}
		uid = text(nested(resource, "metadata", "uid"))
		return uid != "", nil
	})
	return uid, err
}

func waitForManualTerminalChallenge(ctx context.Context, source gcsRecoverySource, name, recoveryUID string, pending pendingGCSFence) (operator.ManualRecoveryChallenge, string, error) {
	var challenge operator.ManualRecoveryChallenge
	var digest string
	err := wait(ctx, "manual-terminal controller challenge without IndependentV2 proof", func() (bool, error) {
		fence, err := get(ctx, "rhizafence", pending.Name)
		if err != nil {
			return false, err
		}
		request, err := exactPendingGCSFenceRequest(fence, pending)
		if err != nil || validateExplicitGenerationFenceRequest(request, source, pending.Request.OperationID) != nil {
			return false, fmt.Errorf("manual-terminal generation fence changed before approval")
		}
		if nested(fence, "status", "proof", "independentV2") != nil {
			return false, fmt.Errorf("independent proof arrived; refusing to route a proven generation through manual terminal approval")
		}
		resource, err := get(ctx, "rhizarecovery", name)
		if err != nil {
			return false, err
		}
		if text(nested(resource, "metadata", "uid")) != recoveryUID || text(nested(resource, "status", "target")) != "" {
			return false, fmt.Errorf("manual-terminal Recovery identity changed or a target was claimed before approval")
		}
		if nested(resource, "status", "manualRecoveryChallenge") == nil {
			return false, nil
		}
		if err := json.Unmarshal(encoded(nested(resource, "status", "manualRecoveryChallenge")), &challenge); err != nil {
			return false, err
		}
		digest = text(nested(resource, "status", "manualRecoveryChallengeDigest"))
		calculated, err := operator.ManualRecoveryChallengeDigest(challenge)
		if err != nil || calculated != digest || challenge.RecoveryUID != recoveryUID || challenge.RecoveryName != name ||
			challenge.SourceClusterID != source.ClusterID || challenge.StatefulSetUID != source.StatefulSetUID ||
			challenge.FenceRequestHash == "" || challenge.TargetBaseKind != "fork" || challenge.TargetClusterID == "" || challenge.TargetClusterID == source.ClusterID {
			return false, fmt.Errorf("manual-terminal challenge does not bind the exact GCS recovery and nonempty source")
		}
		return true, nil
	})
	return challenge, digest, err
}

func waitForManualTerminalApproval(ctx context.Context, challenge operator.ManualRecoveryChallenge, digest, fenceName string,
	archiveBucket objstore.Bucket, controlBucket interface {
		Exists(context.Context, string) (bool, error)
	}, source gcsRecoverySource, claimKey, targetPrefix, controlPrefix string) error {
	remaining := time.Until(challenge.ExpiresAt)
	if remaining <= 0 {
		return fmt.Errorf("manual-terminal approval challenge expired before approval wait")
	}
	if remaining > gcsManualTerminalApprovalWait {
		remaining = gcsManualTerminalApprovalWait
	}
	waitCtx, cancel := context.WithTimeout(ctx, remaining)
	defer cancel()
	return wait(waitCtx, "independent manual-terminal administrator approval", func() (bool, error) {
		resource, err := get(waitCtx, "rhizarecovery", challenge.RecoveryName)
		if err != nil {
			return false, err
		}
		if text(nested(resource, "metadata", "uid")) != challenge.RecoveryUID || text(nested(resource, "status", "target")) != "" ||
			text(nested(resource, "status", "manualRecoveryChallengeDigest")) != digest {
			return false, fmt.Errorf("manual-terminal Recovery changed or claimed a target before independent approval")
		}
		if err := verifyManualTerminalUnclaimedWithBuckets(waitCtx, archiveBucket, controlBucket, source, challenge, claimKey, targetPrefix, controlPrefix); err != nil {
			return false, err
		}
		fence, err := get(waitCtx, "rhizafences", "")
		if err != nil {
			return false, err
		}
		items, _ := fence["items"].([]any)
		for _, item := range items {
			if text(nested(item, "metadata", "name")) == fenceName && nested(item, "status", "proof", "independentV2") != nil {
				return false, fmt.Errorf("IndependentV2 proof appeared before manual approval")
			}
		}
		attestationName, err := operator.ManualRecoveryAttestationName(challenge.RecoveryUID)
		if err != nil {
			return false, err
		}
		_, err = kubectl(waitCtx, "get", "rhizamanualrecoveryattestation", attestationName, "-o", "json")
		if err == nil {
			return true, nil
		}
		if strings.Contains(strings.ToLower(err.Error()), "not found") {
			return false, nil
		}
		return false, err
	})
}

func readAndValidateManualTerminalApproval(ctx context.Context, namespace, name string, challenge operator.ManualRecoveryChallenge, challengeDigest string) (operator.ManualRecoveryAttestationObject, string, error) {
	var approval operator.ManualRecoveryAttestationObject
	data, err := kubectl(ctx, "get", "rhizamanualrecoveryattestation", name, "-o", "json")
	if err != nil {
		return approval, "", err
	}
	var resource obj
	if err := json.Unmarshal(data, &resource); err != nil {
		return approval, "", err
	}
	created, err := time.Parse(time.RFC3339Nano, text(nested(resource, "metadata", "creationTimestamp")))
	if err != nil {
		return approval, "", fmt.Errorf("manual-terminal approval creation time is invalid")
	}
	if err := json.Unmarshal(encoded(nested(resource, "spec")), &approval.Spec); err != nil {
		return approval, "", err
	}
	approval.Namespace = text(nested(resource, "metadata", "namespace"))
	approval.Name = text(nested(resource, "metadata", "name"))
	approval.UID = text(nested(resource, "metadata", "uid"))
	approval.CreatedAt = created
	if approval.Namespace != namespace || approval.Name != name || approval.UID == "" {
		return approval, "", fmt.Errorf("manual-terminal approval API identity is incomplete")
	}
	if err := operator.ValidateManualRecoveryAttestation(challenge, approval, challenge.ManualRecoveryBinding, "", time.Now().UTC()); err != nil {
		return approval, "", fmt.Errorf("independent manual-terminal approval does not match its challenge: %w", err)
	}
	data, err = json.Marshal(approval)
	if err != nil {
		return approval, "", err
	}
	digest := sha256.Sum256(data)
	return approval, hex.EncodeToString(digest[:]), nil
}

func waitForManualTerminalRecoveryComplete(ctx context.Context, source gcsRecoverySource, name, uid, recoveryID string, challenge operator.ManualRecoveryChallenge, challengeDigest, approvalDigest string) (operator.Resource, error) {
	var recovery operator.Resource
	err := wait(ctx, "manual-terminal frozen-source recovery completion", func() (bool, error) {
		resource, err := get(ctx, "rhizarecovery", name)
		if err != nil {
			return false, err
		}
		if err := json.Unmarshal(encoded(resource), &recovery); err != nil {
			return false, err
		}
		if text(nested(resource, "metadata", "uid")) != uid || recovery.Spec.RecoveryID != recoveryID || recovery.Spec.SourceClusterID != source.ClusterID ||
			recovery.Status.ManualRecoveryChallengeDigest != challengeDigest || recovery.Status.ManualAttestationDigest != approvalDigest ||
			recovery.Status.Target != challenge.TargetClusterID || recovery.Status.TargetBaseKind != "fork" {
			return false, fmt.Errorf("manual-terminal recovery differs from the independently approved challenge")
		}
		if recovery.Status.Phase == "Failed" || recovery.Status.Phase == "Blocked" {
			return false, fmt.Errorf("manual-terminal recovery stopped in phase %q", recovery.Status.Phase)
		}
		if recovery.Status.Phase != "Complete" || recovery.Status.Stage != "Complete" {
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return operator.Resource{}, err
	}
	root, err := get(ctx, "rhizacluster", source.BindingName)
	if err != nil || nested(root, "status", "activeClusterID") != challenge.TargetClusterID || nested(root, "status", "phase") != "Healthy" {
		return operator.Resource{}, fmt.Errorf("manual-terminal controller root did not adopt the approved target: %v", err)
	}
	return recovery, nil
}

func recoveryAdminToken(ctx context.Context, namespace, secretName string) (string, error) {
	if secretName == "" {
		return "", fmt.Errorf("manual-terminal recovery has no target admin Secret")
	}
	secret, err := kubectl(ctx, "get", "secret", secretName, "-o", "json")
	if err != nil {
		return "", err
	}
	var resource obj
	if err := json.Unmarshal(secret, &resource); err != nil {
		return "", err
	}
	if text(nested(resource, "metadata", "namespace")) != namespace {
		return "", fmt.Errorf("manual-terminal target admin Secret is outside the run namespace")
	}
	admin, err := base64.StdEncoding.DecodeString(text(nested(resource, "data", "admin")))
	if err != nil || len(admin) == 0 {
		return "", fmt.Errorf("manual-terminal target admin credential is unavailable")
	}
	return string(admin), nil
}

func manualTerminalSuccessorClaimKey(source gcsRecoverySource, controlPrefix string) (string, error) {
	scope := fencing.ArchiveScope{Provider: "gcs", Bucket: source.Bucket, Prefix: source.Prefix, ClusterID: source.ClusterID}
	scopeHash, err := scope.Hash()
	if err != nil {
		return "", err
	}
	identity := sha256.Sum256(encoded([]any{"rhiza-source-successor-v1", scopeHash, source.ClusterID, source.StatefulSetUID}))
	return path.Join(controlPrefix, "rhiza-authority/v1/source-successors", hex.EncodeToString(identity[:])+".json"), nil
}

func verifyManualTerminalUnclaimed(ctx context.Context, archiveBucket objstore.Bucket, controlBucket interface {
	Exists(context.Context, string) (bool, error)
}, source gcsRecoverySource, challenge operator.ManualRecoveryChallenge, claimKey, targetPrefix, controlPrefix string) error {
	return verifyManualTerminalUnclaimedWithBuckets(ctx, archiveBucket, controlBucket, source, challenge, claimKey, targetPrefix, controlPrefix)
}

func verifyManualTerminalUnclaimedWithBuckets(ctx context.Context, archiveBucket objstore.Bucket, controlBucket interface {
	Exists(context.Context, string) (bool, error)
}, source gcsRecoverySource, challenge operator.ManualRecoveryChallenge, claimKey, targetPrefix, controlPrefix string) error {
	if archiveBucket == nil || controlBucket == nil || challenge.TargetClusterID == "" || challenge.TargetClusterID == source.ClusterID {
		return fmt.Errorf("manual-terminal unclaimed-source or target identity is incomplete")
	}
	if exists, err := archiveBucket.Exists(ctx, path.Join(source.Prefix, "recovery/successor.json")); err != nil || exists {
		return errors.Join(err, fmt.Errorf("manual-terminal source archive already has a successor reservation"))
	}
	if exists, err := controlBucket.Exists(ctx, claimKey); err != nil || exists {
		return errors.Join(err, fmt.Errorf("manual-terminal source successor claim exists before administrator approval"))
	}
	keys, err := manualTerminalTargetControlKeys(source, controlPrefix, targetPrefix, challenge.TargetClusterID)
	if err != nil {
		return fmt.Errorf("derive manual-terminal target CONTROL identities: %w", err)
	}
	for _, key := range keys {
		if exists, err := controlBucket.Exists(ctx, key); err != nil || exists {
			return errors.Join(err, fmt.Errorf("manual-terminal target CONTROL resource exists before administrator approval"))
		}
	}
	found := false
	if err := archiveBucket.Iter(ctx, strings.TrimSuffix(targetPrefix, "/")+"/", func(string) error {
		found = true
		return nil
	}); err != nil {
		return fmt.Errorf("verify manual-terminal target DATA prefix before approval: %w", err)
	}
	if found {
		return fmt.Errorf("manual-terminal target DATA resources exist before administrator approval")
	}
	return nil
}

func manualTerminalTargetControlKeys(source gcsRecoverySource, controlPrefix, targetPrefix, targetID string) ([]string, error) {
	rootIdentity := sha256.Sum256(encoded([]string{"rhiza-authority-root-v1", source.StatefulSetUID, targetID}))
	rootID := hex.EncodeToString(rootIdentity[:])
	keys := []string{
		path.Join(controlPrefix, "rhiza-authority/v1/roots", rootID+".json"),
		path.Join(controlPrefix, "rhiza-authority/v1/root-genesis", rootID+".json"),
	}
	scope := fencing.ArchiveScope{Provider: "gcs", Bucket: source.Bucket, Prefix: targetPrefix, ClusterID: targetID}
	scopeHash, err := scope.Hash()
	if err != nil {
		return nil, err
	}
	claimIdentity := sha256.Sum256(encoded([]string{"rhiza-archive-scope-claim-v1", scopeHash}))
	keys = append(keys, path.Join(controlPrefix, "rhiza-authority/v1/archive-scope-claims", hex.EncodeToString(claimIdentity[:])+".json"))
	return keys, nil
}
