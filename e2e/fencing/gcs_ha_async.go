package main

import (
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
	"strconv"
	"strings"
	"time"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

const (
	gcsAsyncEmptySyncInterval       = 24 * time.Hour
	gcsAsyncEmptyCheckpointInterval = 24 * time.Hour
	gcsAsyncEmptyMaxAckToSeal       = 30 * time.Minute
	gcsAsyncNonemptyMaxSyncInterval = 30 * time.Second
	gcsAsyncNonemptyMaxHeadWait     = 2 * time.Minute
	gcsArchiveHeadReadLimit         = 1 << 20
)

type gcsAsyncEmptyOutcome struct {
	Source           gcsRecoverySource
	Fence            operator.FenceRequest
	Proof            operator.FenceProof
	Recovery         operator.Resource
	SourceAck        gcsAcknowledgement
	TargetAck        gcsAcknowledgement
	AckToSeal        time.Duration
	BeforeFenceHead  gcsArchiveHeadEvidence
	PreProofHead     gcsArchiveHeadEvidence
	SealedSourceHead gcsArchiveHeadEvidence
	TargetRows       string
	Admin            string
}

type gcsArchiveHeadEvidence struct {
	VersionType int    `json:"versionType"`
	Version     string `json:"version"`
	Tip         uint64 `json:"tip"`
	Sealed      bool   `json:"sealed"`
	HeadSHA256  string `json:"headSHA256"`
}

func qualifyGCSAsyncEmpty(ctx context.Context) (resultErr error) {
	return qualifyGCSAsyncEmptyWithMode(ctx, false)
}

func qualifyGCSAsyncEmptyExplicit(ctx context.Context) error {
	return qualifyGCSAsyncEmptyWithMode(ctx, true)
}

// qualifyGCSAsyncNonempty exercises full-quorum loss after the asynchronous
// source has persisted a nonempty archive tip. Async recovery is explicitly
// authorized by the source's predeclared allowDataLoss policy.
func qualifyGCSAsyncNonempty(ctx context.Context) (resultErr error) {
	out := os.Getenv("FENCING_E2E_OUT")
	if out == "" {
		out = "gcs-async-nonempty-results"
	}
	if err := os.MkdirAll(out, 0700); err != nil {
		return err
	}
	events, err := os.OpenFile(filepath.Join(out, "gcs-async-nonempty-events.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
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
				resultErr = errors.Join(resultErr, fmt.Errorf("restore owned async-nonempty GCS fault: %w", err))
			}
			event("chaos_cleanup", gcsChaosEvidence(faults[i]))
		}
	}()

	source, err := captureGCSRecoverySource(ctx, out)
	if err != nil {
		return err
	}
	if err := requireGCSAsyncNonemptyProfile(ctx, source); err != nil {
		return err
	}
	adminToken, err := sourceAdminToken(ctx, source.StatefulSet, source.Container)
	if err != nil {
		return err
	}
	archive, err := newGCSArchiveReader(source)
	if err != nil {
		return err
	}
	forward, err := startForward(ctx, source.Targets[0].Pod, out)
	if err != nil {
		return err
	}
	if _, err := call(ctx, forward, "/sql/execute", obj{"request_id": "gcs-async-nonempty-create-table", "sql": "CREATE TABLE fencing_items (id INTEGER PRIMARY KEY, value TEXT)"}, adminToken); err != nil {
		forward.close()
		return fmt.Errorf("create async-nonempty source table: %w", err)
	}
	var sourceAcks []gcsAcknowledgement
	var rows []string
	for id := 1; id <= 3; id++ {
		value := fmt.Sprintf("gcs-async-archive-%d", id)
		ack, err := writeGCSAcknowledgedRow(ctx, forward, adminToken, id, value)
		if err != nil {
			forward.close()
			return fmt.Errorf("acknowledge source archive row %d: %w", id, err)
		}
		sourceAcks = append(sourceAcks, ack)
		rows = append(rows, fmt.Sprintf("[%d,%q]", id, value))
	}
	forward.close()
	expectedRows := "[" + strings.Join(rows, ",") + "]"
	lastAckSlot := sourceAcks[len(sourceAcks)-1].Slot
	var beforeFence gcsArchiveHeadEvidence
	archiveCtx, cancelArchive := context.WithTimeout(ctx, gcsAsyncNonemptyMaxHeadWait)
	err = wait(archiveCtx, "nonempty async GCS archive tip", func() (bool, error) {
		head, err := readGCSArchiveHead(archiveCtx, archive, source.Prefix, false)
		if err != nil {
			return false, err
		}
		if head.Sealed {
			return false, fmt.Errorf("source archive was sealed before the generation fence")
		}
		if head.Tip < lastAckSlot {
			return false, nil
		}
		beforeFence = head
		return true, nil
	})
	cancelArchive()
	if err != nil {
		return fmt.Errorf("wait for acknowledged rows to reach the async source archive: %w", err)
	}
	if beforeFence.Tip == 0 || beforeFence.Version == "" || beforeFence.HeadSHA256 == "" {
		return fmt.Errorf("captured async source archive is not a versioned nonempty tip")
	}
	event("source_nonempty_head_before_fence", obj{"head": beforeFence, "rows": expectedRows, "acks": sourceAcks})

	outcome, err := gcsLoseQuorumAndRecover(ctx, source, adminToken, expectedRows, "async-nonempty", false, out, event, &faults)
	if err != nil {
		return err
	}
	defer outcome.Forward.close()
	sealedHead, err := readGCSArchiveHead(ctx, archive, source.Prefix, false)
	if err != nil {
		return fmt.Errorf("read sealed nonempty source archive HEAD: %w", err)
	}
	if err := validateSealedNonemptyHead(beforeFence, sealedHead); err != nil {
		return err
	}
	recovery := outcome.Recovery
	if recovery.Spec.Durability != "async" || !recovery.Spec.AllowDataLoss || recovery.Status.SourceDurability != "async" ||
		recovery.Status.TargetBaseKind != "fork" || recovery.Status.RecoveredTip != beforeFence.Tip || recovery.Status.ManifestHash == "" {
		return fmt.Errorf("recovery did not adopt the exact nonempty sealed async source tip with its explicit data-loss gate")
	}
	recovered, err := call(ctx, outcome.Forward, "/sql/query", obj{"sql": "SELECT id,value FROM fencing_items ORDER BY id", "consistency": "linearizable"}, outcome.Admin)
	if err != nil || string(encoded(recovered["rows"])) != expectedRows {
		return fmt.Errorf("recovered target rows differ from the captured nonempty archive rows: %v", err)
	}
	recoveredRows := string(encoded(recovered["rows"]))
	targetAck, err := writeGCSAcknowledgedRow(ctx, outcome.Forward, outcome.Admin, 4, "gcs-async-target-ack")
	if err != nil {
		return fmt.Errorf("acknowledge and read back a target row after async fork recovery: %w", err)
	}
	evidence := obj{
		"qualification":          "gcs-async-nonempty-tip-recovery",
		"profile":                obj{"provider": "gcs", "bucket": source.Bucket, "noPVC": true, "sourceDurability": "async", "allowDataLoss": true},
		"sourceAcknowledgements": sourceAcks, "sourceRows": expectedRows,
		"sourceHeadBeforeFence": beforeFence, "sealedSourceHead": sealedHead,
		"fence": outcome.Fence, "proof": outcome.Proof, "recovery": recovery,
		"recoveredRows": recoveredRows, "recoveredTip": recovery.Status.RecoveredTip,
		"targetQuorumVerified": true,
		"targetAck":            targetAck,
		"proofSource":          "RhizaFence status plus direct versioned GCS HEAD reads; recovered rows were read linearly from the promoted target",
	}
	if err := os.WriteFile(filepath.Join(out, "gcs-async-nonempty-evidence.json"), encoded(evidence), 0600); err != nil {
		return err
	}
	event("async_nonempty_tip_recovery_qualified", obj{"source": source.ClusterID, "target": recovery.Status.Target, "tip": sealedHead.Tip})
	return nil
}

func qualifyGCSAsyncEmptyWithMode(ctx context.Context, explicit bool) (resultErr error) {
	out := os.Getenv("FENCING_E2E_OUT")
	if out == "" {
		out = "gcs-async-empty-results"
	}
	if err := os.MkdirAll(out, 0700); err != nil {
		return err
	}
	events, err := os.OpenFile(filepath.Join(out, "gcs-async-empty-events.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
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
				resultErr = errors.Join(resultErr, fmt.Errorf("restore owned async-empty GCS fault: %w", err))
			}
			event("chaos_cleanup", gcsChaosEvidence(faults[i]))
		}
	}()

	source, err := captureGCSRecoverySource(ctx, out)
	if err != nil {
		return err
	}
	if explicit {
		source, err = prepareExplicitGCSRecoverySource(ctx, source, out, true)
		if err != nil {
			return err
		}
		if err := requireGCSAsyncEmptyExplicitProfile(ctx, source); err != nil {
			return err
		}
	} else {
		if err := requireGCSAsyncEmptyProfile(ctx, source); err != nil {
			return err
		}
	}
	adminToken, err := sourceAdminToken(ctx, source.StatefulSet, source.Container)
	if err != nil {
		return err
	}
	archive, err := newGCSArchiveReader(source)
	if err != nil {
		return err
	}
	forward, err := startForward(ctx, source.Targets[0].Pod, out)
	if err != nil {
		return err
	}
	if _, err := call(ctx, forward, "/sql/execute", obj{"request_id": "gcs-async-empty-create-table", "sql": "CREATE TABLE fencing_items (id INTEGER PRIMARY KEY, value TEXT)"}, adminToken); err != nil {
		forward.close()
		return fmt.Errorf("create async-empty source table: %w", err)
	}
	ackStarted := time.Now()
	sourceAck, err := writeGCSAcknowledgedRow(ctx, forward, adminToken, 1, "gcs-async-ack-lost")
	forward.close()
	if err != nil {
		return fmt.Errorf("acknowledge source row before empty-tip recovery: %w", err)
	}
	event("source_ack", obj{"generation": source.ClusterID, "ack": sourceAck})

	beforeFence, err := readGCSArchiveHead(ctx, archive, source.Prefix, true)
	if err != nil {
		return fmt.Errorf("verify persisted zero-tip source HEAD after ACK and before fence: %w", err)
	}
	event("source_head_before_fence", beforeFence)

	outcome, err := gcsLoseQuorumAndRecoverAsyncEmpty(ctx, source, adminToken, archive, beforeFence, ackStarted, explicit, out, event, &faults)
	if err != nil {
		return err
	}
	outcome.SourceAck = sourceAck
	outcome.BeforeFenceHead = beforeFence
	outcome.Source = source
	event("source_sealed_zero_tip", obj{"head": outcome.SealedSourceHead, "ackToSeal": outcome.AckToSeal.String()})

	if err := verifyAsyncEmptyRecovery(outcome.Recovery, outcome.Fence, outcome.Proof); err != nil {
		return fmt.Errorf("completed recovery did not prove the explicit async empty-base contract: %w", err)
	}
	forward, err = startForward(ctx, outcome.Recovery.Status.TargetIdentities[0].Pod, out)
	if err != nil {
		return err
	}
	defer forward.close()
	status, err := readMembershipWithToken(ctx, forward, outcome.Admin)
	if err != nil || status.ClusterID != outcome.Recovery.Status.Target || status.Pending || !status.Voting {
		return fmt.Errorf("empty-base target did not pass authenticated quorum membership: %v", err)
	}
	data, err := call(ctx, forward, "/recovery/status", nil, outcome.Admin)
	if err != nil {
		return fmt.Errorf("read empty-base target quorum status: %w", err)
	}
	var targetRecoveryStatus network.VoterRecoveryStatus
	if err := json.Unmarshal(encoded(data), &targetRecoveryStatus); err != nil || targetRecoveryStatus.ClusterID != outcome.Recovery.Status.Target || !targetRecoveryStatus.Quorum {
		return fmt.Errorf("empty-base target did not pass authenticated recovery quorum status: %v", err)
	}
	targetQuorumVerified := true
	if _, err := call(ctx, forward, "/sql/execute", obj{"request_id": "gcs-async-empty-target-create-table", "sql": "CREATE TABLE fencing_items (id INTEGER PRIMARY KEY, value TEXT)"}, outcome.Admin); err != nil {
		return fmt.Errorf("create target table after empty-base recovery: %w", err)
	}
	oldRows, err := call(ctx, forward, "/sql/query", obj{"sql": "SELECT id,value FROM fencing_items ORDER BY id", "consistency": "linearizable"}, outcome.Admin)
	if err != nil || string(encoded(oldRows["rows"])) != "[]" {
		return fmt.Errorf("acknowledged source row unexpectedly appeared in the empty-base target: %v", err)
	}
	outcome.TargetRows = string(encoded(oldRows["rows"]))
	emptyReadbackVerified := true
	targetAck, err := writeGCSAcknowledgedRow(ctx, forward, outcome.Admin, 2, "gcs-async-target-ack")
	if err != nil {
		return fmt.Errorf("acknowledge and read back target row after empty-base recovery: %w", err)
	}
	outcome.TargetAck = targetAck
	event("target_ack_and_linearizable_readback", obj{"generation": outcome.Recovery.Status.Target, "ack": targetAck})

	evidence := obj{
		"qualification": "gcs-async-empty-tip-recovery",
		"profile":       obj{"provider": "gcs", "bucket": source.Bucket, "noPVC": true, "sourceDurability": "async", "allowDataLoss": true},
		"sourceAck":     sourceAck, "sourceHeadBeforeFence": beforeFence, "sourceHeadBeforeProof": outcome.PreProofHead, "sealedSourceHead": outcome.SealedSourceHead,
		"fence": outcome.Fence, "proof": outcome.Proof, "recovery": outcome.Recovery,
		"sourceAckLost": outcome.TargetRows == "[]", "targetQuorumVerified": targetQuorumVerified,
		"emptyReadbackVerified": emptyReadbackVerified, "targetAckReadbackVerified": true,
		"targetEmptyRows": outcome.TargetRows, "targetAck": targetAck, "ackToSeal": outcome.AckToSeal.String(),
		"proofSource": "RhizaFence status plus direct read-only GCS HEAD read; the generation fence proof is validated against captured workload identities",
	}
	if err := os.WriteFile(filepath.Join(out, "gcs-async-empty-evidence.json"), encoded(evidence), 0600); err != nil {
		return err
	}
	event("async_empty_recovery_qualified", obj{"source": source.ClusterID, "target": outcome.Recovery.Status.Target, "sourceAckLost": true})
	return nil
}

func requireGCSAsyncEmptyProfile(ctx context.Context, source gcsRecoverySource) error {
	if err := requireGCSAsyncEmptySourceProfile(ctx, source); err != nil {
		return err
	}
	binding, err := get(ctx, "rhizacluster", source.BindingName)
	if err != nil {
		return err
	}
	if !gcsAsyncEmptyPolicyEnabled(nested(binding, "spec")) {
		return fmt.Errorf("async-empty automatic recovery requires predeclared allowDataLoss policy")
	}
	operatorDeployment, err := get(ctx, "deployment", "rhiza-operator")
	if err != nil {
		return err
	}
	opContainer, err := boundContainer(operatorDeployment, "operator")
	if err != nil {
		return err
	}
	automatic, err := effectiveGCSSetting(ctx, opContainer, "RHIZA_AUTOMATIC_RECOVERY")
	if err != nil || automatic != "true" {
		return fmt.Errorf("async-empty qualification requires the automatic recovery controller to be enabled")
	}
	return nil
}

func requireGCSAsyncEmptyExplicitProfile(ctx context.Context, source gcsRecoverySource) error {
	if err := requireGCSAsyncEmptySourceProfile(ctx, source); err != nil {
		return err
	}
	return requireExplicitGCSRecoveryProfile(ctx, source, true)
}

func requireGCSAsyncNonemptyProfile(ctx context.Context, source gcsRecoverySource) error {
	if err := requireGCSAsyncNonemptySourceProfile(ctx, source); err != nil {
		return err
	}
	binding, err := get(ctx, "rhizacluster", source.BindingName)
	if err != nil {
		return err
	}
	if !gcsAsyncEmptyPolicyEnabled(nested(binding, "spec")) {
		return fmt.Errorf("async nonempty recovery requires predeclared allowDataLoss policy")
	}
	operatorDeployment, err := get(ctx, "deployment", "rhiza-operator")
	if err != nil {
		return err
	}
	opContainer, err := boundContainer(operatorDeployment, "operator")
	if err != nil {
		return err
	}
	automatic, err := effectiveGCSSetting(ctx, opContainer, "RHIZA_AUTOMATIC_RECOVERY")
	if err != nil || automatic != "true" {
		return fmt.Errorf("async nonempty qualification requires the automatic recovery controller to be enabled")
	}
	return nil
}

func requireGCSAsyncNonemptySourceProfile(ctx context.Context, source gcsRecoverySource) error {
	sts, err := get(ctx, "statefulset", source.StatefulSet)
	if err != nil {
		return err
	}
	container, err := boundContainer(sts, source.Container)
	if err != nil {
		return err
	}
	durability, err := effectiveGCSSetting(ctx, container, "RHIZA_OBJSTORE_DURABILITY")
	if err != nil || durability != "async" {
		return fmt.Errorf("async nonempty qualification requires a verified async source durability setting")
	}
	syncText, err := effectiveGCSSetting(ctx, container, "RHIZA_OBJSTORE_SYNC_INTERVAL")
	if err != nil {
		return fmt.Errorf("read source object-store sync interval: %w", err)
	}
	checkpointText, err := effectiveGCSSetting(ctx, container, "RHIZA_CHECKPOINT_INTERVAL")
	if err != nil {
		return fmt.Errorf("read source checkpoint interval: %w", err)
	}
	tailText, err := effectiveGCSOptionalSetting(ctx, container, "RHIZA_CHECKPOINT_TAIL_BYTES")
	if err != nil {
		return fmt.Errorf("read source checkpoint tail limit: %w", err)
	}
	if tailText == "" {
		tailText = "536870912"
	}
	syncInterval, err := time.ParseDuration(syncText)
	if err != nil || syncInterval <= 0 || syncInterval > gcsAsyncNonemptyMaxSyncInterval {
		return fmt.Errorf("async nonempty qualification requires a positive bounded object-store sync interval")
	}
	checkpointInterval, err := time.ParseDuration(checkpointText)
	if err != nil || checkpointInterval <= 0 || checkpointInterval > gcsAsyncNonemptyMaxHeadWait {
		return fmt.Errorf("async nonempty qualification requires a positive bounded checkpoint interval")
	}
	tailBytes, err := strconv.ParseInt(tailText, 10, 64)
	if err != nil || tailBytes <= 0 || tailBytes > 2<<30 {
		return fmt.Errorf("async nonempty qualification requires an explicit valid checkpoint tail limit")
	}
	return nil
}

func requireGCSAsyncEmptySourceProfile(ctx context.Context, source gcsRecoverySource) error {
	sts, err := get(ctx, "statefulset", source.StatefulSet)
	if err != nil {
		return err
	}
	container, err := boundContainer(sts, source.Container)
	if err != nil {
		return err
	}
	durability, err := effectiveGCSSetting(ctx, container, "RHIZA_OBJSTORE_DURABILITY")
	if err != nil || durability != "async" {
		return fmt.Errorf("async-empty qualification requires a verified async source durability setting")
	}
	syncText, err := effectiveGCSSetting(ctx, container, "RHIZA_OBJSTORE_SYNC_INTERVAL")
	if err != nil {
		return fmt.Errorf("read source object-store sync interval: %w", err)
	}
	checkpointText, err := effectiveGCSSetting(ctx, container, "RHIZA_CHECKPOINT_INTERVAL")
	if err != nil {
		return fmt.Errorf("read source checkpoint interval: %w", err)
	}
	tailText, err := effectiveGCSOptionalSetting(ctx, container, "RHIZA_CHECKPOINT_TAIL_BYTES")
	if err != nil {
		return fmt.Errorf("read source checkpoint tail limit: %w", err)
	}
	if tailText == "" {
		// This is the production parser's default, not a qualification-only override.
		tailText = "536870912"
	}
	if err := validateGCSAsyncEmptyCadence(syncText, checkpointText, tailText); err != nil {
		return err
	}
	return nil
}

func validateGCSAsyncEmptyCadence(syncText, checkpointText, tailText string) error {
	syncInterval, err := time.ParseDuration(syncText)
	if err != nil || syncInterval < gcsAsyncEmptySyncInterval {
		return fmt.Errorf("async-empty qualification requires RHIZA_OBJSTORE_SYNC_INTERVAL >= %s", gcsAsyncEmptySyncInterval)
	}
	checkpointInterval, err := time.ParseDuration(checkpointText)
	if err != nil || checkpointInterval < gcsAsyncEmptyCheckpointInterval {
		return fmt.Errorf("async-empty qualification requires RHIZA_CHECKPOINT_INTERVAL >= %s", gcsAsyncEmptyCheckpointInterval)
	}
	tailBytes, err := strconv.ParseInt(tailText, 10, 64)
	if err != nil || tailBytes <= 0 || tailBytes > 2<<30 {
		return fmt.Errorf("async-empty qualification requires an explicit valid checkpoint tail limit")
	}
	return nil
}

func gcsAsyncEmptyPolicyEnabled(spec any) bool {
	return nested(spec, "automatic") == true && nested(spec, "policy", "allowDataLoss") == true
}

func effectiveGCSSetting(ctx context.Context, container any, key string) (string, error) {
	setting, err := effectiveGCSOptionalSetting(ctx, container, key)
	if err != nil {
		return "", err
	}
	if setting == "" {
		return "", fmt.Errorf("source setting %s is not explicitly configured", key)
	}
	return setting, nil
}

func effectiveGCSOptionalSetting(ctx context.Context, container any, key string) (string, error) {
	setting := ""
	from, _ := nested(container, "envFrom").([]any)
	for _, ref := range from {
		prefix := text(nested(ref, "prefix"))
		if name := text(nested(ref, "configMapRef", "name")); name != "" {
			config, err := get(ctx, "configmap", name)
			if err != nil {
				return "", err
			}
			if value := text(nested(config, "data", strings.TrimPrefix(key, prefix))); value != "" && prefix+strings.TrimPrefix(key, prefix) == key {
				setting = value
			}
		} else if name := text(nested(ref, "secretRef", "name")); name != "" {
			secret, err := get(ctx, "secret", name)
			if err != nil {
				return "", err
			}
			if prefix+strings.TrimPrefix(key, prefix) == key {
				if value := text(nested(secret, "data", key)); value != "" {
					decoded, err := base64.StdEncoding.DecodeString(value)
					if err != nil {
						return "", fmt.Errorf("decode source setting %s: %w", key, err)
					}
					setting = string(decoded)
				}
			}
		} else {
			return "", fmt.Errorf("source envFrom has an unsupported value source")
		}
	}
	env, _ := nested(container, "env").([]any)
	for _, entry := range env {
		if text(nested(entry, "name")) != key {
			continue
		}
		if raw, ok := nested(entry, "value").(string); ok {
			setting = raw
			continue
		}
		if name, refKey := text(nested(entry, "valueFrom", "configMapKeyRef", "name")), text(nested(entry, "valueFrom", "configMapKeyRef", "key")); name != "" && refKey != "" {
			config, err := get(ctx, "configmap", name)
			if err != nil {
				return "", err
			}
			setting = text(nested(config, "data", refKey))
			continue
		}
		if name, refKey := text(nested(entry, "valueFrom", "secretKeyRef", "name")), text(nested(entry, "valueFrom", "secretKeyRef", "key")); name != "" && refKey != "" {
			secret, err := get(ctx, "secret", name)
			if err != nil {
				return "", err
			}
			encodedValue := text(nested(secret, "data", refKey))
			decoded, err := base64.StdEncoding.DecodeString(encodedValue)
			if err != nil {
				return "", fmt.Errorf("decode source setting %s: %w", key, err)
			}
			setting = string(decoded)
			continue
		}
		return "", fmt.Errorf("source setting %s has an unsupported value source", key)
	}
	return setting, nil
}

func newGCSArchiveReader(source gcsRecoverySource) (objstore.Bucket, error) {
	if source.Bucket == "" || source.Prefix == "" {
		return nil, fmt.Errorf("GCS archive reader scope is incomplete")
	}
	bucket, err := objectstore.NewBucket(objectstore.Config{Provider: objectstore.ProviderGCS, Bucket: source.Bucket})
	if err != nil {
		return nil, fmt.Errorf("open read-only GCS archive client: %w", err)
	}
	return bucket, nil
}

func readGCSArchiveHead(ctx context.Context, bucket objstore.Bucket, prefix string, requireInitial bool) (gcsArchiveHeadEvidence, error) {
	var evidence gcsArchiveHeadEvidence
	if bucket == nil || prefix == "" {
		return evidence, fmt.Errorf("GCS archive HEAD scope is incomplete")
	}
	key := path.Join(prefix, "archive/head.bin")
	first, err := bucket.Attributes(ctx, key)
	if err != nil {
		return evidence, err
	}
	reader, err := bucket.Get(ctx, key)
	if err != nil {
		return evidence, err
	}
	data, readErr := io.ReadAll(io.LimitReader(reader, gcsArchiveHeadReadLimit+1))
	closeErr := reader.Close()
	if readErr != nil {
		return evidence, fmt.Errorf("read bounded persisted GCS archive HEAD: %w", readErr)
	}
	if closeErr != nil {
		return evidence, fmt.Errorf("close persisted GCS archive HEAD: %w", closeErr)
	}
	if len(data) == 0 || len(data) > gcsArchiveHeadReadLimit {
		return evidence, fmt.Errorf("persisted GCS archive HEAD has invalid size %d", len(data))
	}
	second, err := bucket.Attributes(ctx, key)
	if err != nil {
		return evidence, err
	}
	if first.Version == nil || second.Version == nil || *first.Version != *second.Version || second.Version.Type != objstore.Generation || second.Version.Value == "" {
		return evidence, fmt.Errorf("persisted GCS archive HEAD changed during versioned read")
	}
	observedVersion := *second.Version
	manager := recovery.NewManager(bucket, prefix, recovery.ArchiveConfigID)
	defer manager.Close()
	if err := manager.Load(ctx); err != nil {
		return evidence, err
	}
	version, exists := manager.HeadVersion()
	if !exists || version != observedVersion {
		return evidence, fmt.Errorf("archive manager HEAD version differs from direct GCS object version")
	}
	if requireInitial {
		if err := recovery.ValidateEmptyArchiveHead(data, recovery.ArchiveConfigID); err != nil {
			return evidence, err
		}
	}
	digest := sha256.Sum256(data)
	evidence = gcsArchiveHeadEvidence{VersionType: int(observedVersion.Type), Version: observedVersion.Value, Tip: uint64(manager.Tip()), Sealed: manager.Sealed(), HeadSHA256: hex.EncodeToString(digest[:])}
	if requireInitial {
		if err := validateInitialZeroHead(evidence); err != nil {
			return evidence, err
		}
	}
	return evidence, nil
}

func validateSealedZeroHead(before, after gcsArchiveHeadEvidence) error {
	if before.Version == "" || before.VersionType != int(objstore.Generation) || before.Tip != 0 || before.Sealed || before.HeadSHA256 == "" ||
		after.Version == "" || after.VersionType != int(objstore.Generation) || after.Version == before.Version || after.Tip != 0 || !after.Sealed || after.HeadSHA256 == "" || after.HeadSHA256 == before.HeadSHA256 {
		return fmt.Errorf("source generation was not sealed at a new persisted zero-tip GCS HEAD version")
	}
	return nil
}

func validateSealedNonemptyHead(before, after gcsArchiveHeadEvidence) error {
	if before.Version == "" || before.VersionType != int(objstore.Generation) || before.Tip == 0 || before.Sealed || before.HeadSHA256 == "" ||
		after.Version == "" || after.VersionType != int(objstore.Generation) || after.Version == before.Version || after.Tip != before.Tip || !after.Sealed || after.HeadSHA256 == "" || after.HeadSHA256 == before.HeadSHA256 {
		return fmt.Errorf("source generation did not seal the captured nonempty persisted GCS HEAD tip")
	}
	return nil
}

func validateUnchangedZeroHead(before, after gcsArchiveHeadEvidence) error {
	if err := validateInitialZeroHead(before); err != nil {
		return err
	}
	if err := validateInitialZeroHead(after); err != nil {
		return err
	}
	if before.VersionType != after.VersionType || before.Version != after.Version || before.HeadSHA256 != after.HeadSHA256 {
		return fmt.Errorf("source empty HEAD changed after ACK and before the independent fence proof")
	}
	return nil
}

func validateInitialZeroHead(head gcsArchiveHeadEvidence) error {
	if head.Version == "" || head.VersionType != int(objstore.Generation) || head.Tip != 0 || head.Sealed || head.HeadSHA256 == "" {
		return fmt.Errorf("source HEAD is not a versioned unsealed zero-tip genesis")
	}
	return nil
}

func gcsLoseQuorumAndRecoverAsyncEmpty(ctx context.Context, source gcsRecoverySource, token string, archive objstore.Bucket, initialHead gcsArchiveHeadEvidence, ackStarted time.Time, explicit bool, out string, event func(string, any), faults *[]*gcsChaosFault) (*gcsAsyncEmptyOutcome, error) {
	chaosCtx, cancel := context.WithTimeout(ctx, 165*time.Second)
	defer cancel()
	if len(source.Targets) < 3 || len(source.Targets) > 64 || len(source.AgentPods) == 0 || source.OperatorPod == "" {
		return nil, fmt.Errorf("async-empty scenario lacks a supported voter cohort or exact control Pods")
	}
	if err := verifyGCSControlPodSnapshots(chaosCtx, source.AgentPods); err != nil {
		return nil, err
	}
	agentFault, err := createGCSControlLoss(chaosCtx, namespace, qualificationRunID, "async-empty", "runtime-agent", source.AgentPods)
	if agentFault != nil {
		*faults = append(*faults, agentFault)
	}
	if err != nil {
		return nil, err
	}
	if !gcsFaultSourceMatches(agentFault, source.AgentPods) {
		return nil, fmt.Errorf("runtime-agent isolation differs from captured DaemonSet Pod identities")
	}
	if err := agentFault.WaitInjected(chaosCtx); err != nil {
		return nil, err
	}
	cohortFault, err := partitionGCSVoterCohort(chaosCtx, namespace, qualificationRunID, "async-empty", source.Targets)
	if cohortFault != nil {
		*faults = append(*faults, cohortFault)
	}
	if err != nil {
		return nil, err
	}
	if err := cohortFault.WaitInjected(chaosCtx); err != nil {
		return nil, err
	}
	observer, err := startForward(chaosCtx, source.Targets[0].Pod, out)
	if err != nil {
		return nil, err
	}
	err = wait(chaosCtx, "async-empty source quorum loss", func() (bool, error) {
		data, err := call(chaosCtx, observer, "/recovery/status", nil, token)
		if err != nil {
			return false, err
		}
		var status network.VoterRecoveryStatus
		if err := json.Unmarshal(encoded(data), &status); err != nil {
			return false, err
		}
		if status.ClusterID != source.ClusterID {
			return false, fmt.Errorf("async-empty observer changed source generation")
		}
		return !status.Quorum, nil
	})
	observer.close()
	if err != nil {
		return nil, err
	}
	var pending pendingGCSFence
	var explicitRecovery explicitGCSRecoveryRef
	if explicit {
		explicitRecovery, err = createExplicitGCSRecovery(chaosCtx, source, "async-empty", "async", true)
		if err != nil {
			return nil, err
		}
		if err := verifyNoGCSReservation(chaosCtx, source); err != nil {
			return nil, err
		}
		if err := requestGenerationFence(chaosCtx, source, explicitGenerationFenceRequest(source, explicitRecovery.OperationID), out); err != nil {
			return nil, err
		}
		pending, err = waitForPendingGCSFenceOperation(chaosCtx, source, explicitRecovery.OperationID)
	} else {
		pending, err = waitForPendingGCSFence(chaosCtx, source)
	}
	if err != nil {
		return nil, err
	}
	if explicit {
		if _, err := waitForExplicitGCSRecoveryAwaitingFence(chaosCtx, source, explicitRecovery.Name, pending.Request.OperationID, explicitRecovery.UID, explicitRecovery.AllowDataLoss); err != nil {
			return nil, err
		}
		if err := verifyNoGCSReservation(chaosCtx, source); err != nil {
			return nil, err
		}
	}
	preProofHead, err := readGCSArchiveHead(chaosCtx, archive, source.Prefix, true)
	if err != nil {
		return nil, fmt.Errorf("read authoritative empty source HEAD before fence proof: %w", err)
	}
	if err := validateUnchangedZeroHead(initialHead, preProofHead); err != nil {
		return nil, err
	}
	event("source_empty_head_before_fence_proof", preProofHead)
	if err := agentFault.RecheckInjected(chaosCtx); err != nil {
		return nil, err
	}
	if err := cohortFault.RecheckInjected(chaosCtx); err != nil {
		return nil, err
	}
	operatorSnapshot := []gcsControlPodIdentity{source.OperatorIdentity}
	if err := verifyGCSControlPodSnapshots(chaosCtx, operatorSnapshot); err != nil {
		return nil, err
	}
	operatorFault, err := createGCSControlLoss(chaosCtx, namespace, qualificationRunID, "async-empty", "operator", operatorSnapshot)
	if operatorFault != nil {
		*faults = append(*faults, operatorFault)
	}
	if err != nil {
		return nil, err
	}
	if !gcsFaultSourceMatches(operatorFault, operatorSnapshot) {
		return nil, fmt.Errorf("operator isolation differs from captured Deployment Pod identity")
	}
	if err := operatorFault.WaitInjected(chaosCtx); err != nil {
		return nil, err
	}
	if err := confirmPendingGCSFence(chaosCtx, source, pending); err != nil {
		return nil, err
	}
	if err := operatorFault.RecheckInjected(chaosCtx); err != nil {
		return nil, err
	}
	if err := agentFault.Restore(chaosCtx); err != nil {
		return nil, err
	}
	if err := waitForGCSFenceProof(chaosCtx, source, pending); err != nil {
		return nil, err
	}
	if explicit {
		if err := confirmExplicitGCSRecoveryFence(chaosCtx, source, pending, explicitRecovery); err != nil {
			return nil, err
		}
	}
	sealedHead, err := readGCSArchiveHead(chaosCtx, archive, source.Prefix, false)
	if err != nil {
		return nil, fmt.Errorf("verify sealed source HEAD after generation fence: %w", err)
	}
	if err := validateSealedZeroHead(initialHead, sealedHead); err != nil {
		return nil, err
	}
	ackToSeal := time.Since(ackStarted)
	if ackToSeal > gcsAsyncEmptyMaxAckToSeal {
		return nil, fmt.Errorf("async empty recovery exceeded the bounded ACK-to-seal window")
	}
	if err := operatorFault.RecheckInjected(chaosCtx); err != nil {
		return nil, err
	}
	if err := cohortFault.Restore(chaosCtx); err != nil {
		return nil, err
	}
	if err := operatorFault.Restore(chaosCtx); err != nil {
		return nil, err
	}
	event("async_empty_generation_fence_proven", obj{"fenceName": pending.Name, "operationID": pending.Request.OperationID})
	var expectedRecovery *explicitGCSRecoveryRef
	if explicit {
		expectedRecovery = &explicitRecovery
	}
	recoveryOutcome, err := waitForAsyncEmptyGCSRecovery(ctx, source, pending, out, expectedRecovery)
	if err != nil {
		return nil, err
	}
	return &gcsAsyncEmptyOutcome{Source: source, Fence: pending.Request, Proof: recoveryOutcome.Proof, Recovery: recoveryOutcome.Recovery, PreProofHead: preProofHead, SealedSourceHead: sealedHead, AckToSeal: ackToSeal, Admin: recoveryOutcome.Admin}, nil
}

func waitForAsyncEmptyGCSRecovery(ctx context.Context, source gcsRecoverySource, pending pendingGCSFence, out string, explicitRecovery *explicitGCSRecoveryRef) (*gcsRecoveryOutcome, error) {
	if pending.Name == "" || pending.UID == "" || pending.Request.OperationID == "" || pending.Request.SourceClusterID != source.ClusterID || !sameTargets(pending.Request.Targets, source.Targets) {
		return nil, fmt.Errorf("async-empty recovery is not bound to the exact pending fence")
	}
	outcome := &gcsRecoveryOutcome{Source: source}
	if err := wait(ctx, "authenticated async-empty IndependentV2 fence", func() (bool, error) {
		list, err := get(ctx, "rhizafences", "")
		if err != nil {
			return false, err
		}
		items, _ := list["items"].([]any)
		for _, item := range items {
			if text(nested(item, "metadata", "name")) != pending.Name {
				continue
			}
			candidate, err := exactPendingGCSFenceRequest(item, pending)
			if err != nil {
				return false, fmt.Errorf("async-empty pending fence changed: %w", err)
			}
			if candidate.Scope != operator.ScopeGeneration || candidate.SourceClusterID != source.ClusterID || !sameTargets(candidate.Targets, source.Targets) {
				return false, fmt.Errorf("async-empty pending fence no longer matches the captured source cohort")
			}
			if nested(item, "status", "proof") == nil {
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
	if err := wait(ctx, "completed explicit async-empty recovery", func() (bool, error) {
		list, err := get(ctx, "rhizarecoveries", "")
		if err != nil {
			return false, err
		}
		items, _ := list["items"].([]any)
		matches := 0
		for _, item := range items {
			if explicitRecovery != nil {
				if nested(item, "metadata", "name") != explicitRecovery.Name && nested(item, "spec", "recoveryID") != explicitRecovery.OperationID {
					continue
				}
				if err := validateExplicitRecoveryResource(item, source, *explicitRecovery); err != nil {
					return false, err
				}
			} else if nested(item, "spec", "sourceClusterID") != source.ClusterID || nested(item, "spec", "recoveryID") != pending.Request.OperationID {
				continue
			}
			matches++
			if err := json.Unmarshal(encoded(item), &outcome.Recovery); err != nil {
				return false, err
			}
		}
		if matches > 1 {
			return false, fmt.Errorf("multiple recoveries claim the async-empty fence operation")
		}
		if matches == 0 || outcome.Recovery.Status.Phase != "Complete" || outcome.Recovery.Status.Stage != "Complete" {
			return false, nil
		}
		if explicitRecovery != nil && (outcome.Recovery.Metadata["uid"] != explicitRecovery.UID || outcome.Recovery.Metadata["name"] != explicitRecovery.Name ||
			outcome.Recovery.Metadata["namespace"] != explicitRecovery.Namespace) {
			return false, fmt.Errorf("completed async recovery does not match the captured resource identity")
		}
		if outcome.Recovery.Spec.SourceClusterID != source.ClusterID || outcome.Recovery.Spec.RecoveryID != pending.Request.OperationID ||
			outcome.Recovery.Status.Source != source.ClusterID || outcome.Recovery.Status.StatefulSetUID != source.StatefulSetUID ||
			outcome.Recovery.Status.SourceDurability != "async" || outcome.Recovery.Spec.Durability != "async" || !outcome.Recovery.Spec.AllowDataLoss ||
			outcome.Recovery.Status.TargetBaseKind != "empty" || outcome.Recovery.Status.RecoveredTip != 0 || outcome.Recovery.Status.ManifestHash != "" ||
			outcome.Recovery.Status.Target == "" || outcome.Recovery.Status.RecoveryID != outcome.Recovery.Spec.RecoveryID {
			return false, fmt.Errorf("completed recovery differs from explicit async empty-base contract")
		}
		if err := recoveryFenceMatchesProof(outcome.Recovery, outcome.Fence, outcome.Proof); err != nil {
			return false, err
		}
		return true, nil
	}); err != nil {
		return nil, err
	}
	root, err := get(ctx, "rhizacluster", source.BindingName)
	if err != nil {
		return nil, err
	}
	if nested(root, "status", "phase") != "Healthy" || nested(root, "status", "activeClusterID") != outcome.Recovery.Status.Target {
		return nil, fmt.Errorf("controller root did not adopt the async-empty recovery target")
	}
	secretName := outcome.Recovery.Status.SecretName
	if secretName == "" {
		return nil, fmt.Errorf("completed async-empty recovery has no target admin Secret")
	}
	secret, err := get(ctx, "secret", secretName)
	if err != nil {
		return nil, err
	}
	admin, err := base64.StdEncoding.DecodeString(text(nested(secret, "data", "admin")))
	if err != nil || len(admin) == 0 {
		return nil, fmt.Errorf("async-empty target admin credential is unavailable")
	}
	outcome.Admin = string(admin)
	if len(outcome.Recovery.Status.TargetIdentities) == 0 || outcome.Recovery.Status.TargetIdentities[0].Pod == "" {
		return nil, fmt.Errorf("completed async-empty recovery has no concrete target voter")
	}
	return outcome, nil
}
