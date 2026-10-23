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

	"cloud.google.com/go/storage"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

// qualifyGCSMissingMembership runs one half of the default-off missing-source-
// membership qualification. Negative and positive cases must use separate
// fresh namespaces and GCS prefixes because the fence operation is immutable.
func qualifyGCSMissingMembership(ctx context.Context) (resultErr error) {
	if os.Getenv("RHIZA_GCS_MISSING_MEMBERSHIP") != "yes" {
		return fmt.Errorf("missing-membership GCS qualification is default-off")
	}
	caseName := os.Getenv("RHIZA_GCS_MISSING_MEMBERSHIP_CASE")
	if caseName != "negative" && caseName != "positive" {
		return fmt.Errorf("missing-membership case must be negative or positive")
	}
	out := strings.TrimSpace(os.Getenv("FENCING_E2E_OUT"))
	if out == "" {
		out = "gcs-missing-membership-" + caseName + "-results"
	}
	if err := os.MkdirAll(out, 0700); err != nil {
		return err
	}
	if info, err := os.Lstat(out); err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("missing-membership result directory must be private")
	}
	events, err := os.OpenFile(filepath.Join(out, "gcs-missing-membership-events.jsonl"), os.O_CREATE|os.O_EXCL|os.O_APPEND|os.O_WRONLY, 0600)
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
				resultErr = errors.Join(resultErr, fmt.Errorf("restore owned missing-membership GCS fault: %w", err))
			}
			event("chaos_cleanup", gcsChaosEvidence(faults[i]))
		}
		if err := events.Close(); err != nil {
			resultErr = errors.Join(resultErr, err)
		}
		if eventErr != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("write missing-membership event record: %w", eventErr))
		}
	}()

	source, err := captureGCSRecoverySource(ctx, out)
	if err != nil {
		return err
	}
	if len(source.Targets) != 3 || source.Durability != "async" {
		return fmt.Errorf("missing-membership qualification requires a three-voter async GCS source")
	}
	source, err = prepareExplicitGCSRecoverySource(ctx, source, out, true)
	if err != nil {
		return err
	}
	if err := requireGCSAsyncNonemptySourceProfile(ctx, source); err != nil {
		return err
	}
	if err := requireExplicitGCSRecoveryProfile(ctx, source, true); err != nil {
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
	defer archive.Close()
	if err := removeExactGCSMembershipForQualification(ctx, source, archive); err != nil {
		return err
	}
	if err := requireGCSMembershipAbsent(ctx, archive, source.Prefix); err != nil {
		return err
	}
	if caseName == "negative" {
		return qualifyGCSMissingMembershipNegative(ctx, source, archive, out, event)
	}
	return qualifyGCSMissingMembershipPositive(ctx, source, archive, adminToken, out, event, &faults)
}

func removeExactGCSMembershipForQualification(ctx context.Context, source gcsRecoverySource, bucket objstore.Bucket) error {
	key := path.Join(source.Prefix, "voters/membership.json")
	before, err := bucket.Attributes(ctx, key)
	if err != nil {
		if bucket.IsObjNotFoundErr(err) {
			return nil
		}
		return fmt.Errorf("read source membership generation before test-scoped removal")
	}
	if before.Version == nil || before.Version.Type != objstore.Generation {
		return fmt.Errorf("source membership has no conditional GCS generation; refusing removal")
	}
	generation, err := strconv.ParseInt(before.Version.Value, 10, 64)
	if err != nil || generation <= 0 {
		return fmt.Errorf("source membership generation is invalid; refusing removal")
	}
	reader, err := bucket.Get(ctx, key)
	if err != nil {
		if bucket.IsObjNotFoundErr(err) {
			return nil
		}
		return fmt.Errorf("read source membership before test-scoped removal: %w", err)
	}
	data, readErr := io.ReadAll(io.LimitReader(reader, 64<<10))
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || len(data) >= 64<<10 {
		return errors.Join(readErr, closeErr, fmt.Errorf("source membership object is unreadable or oversized"))
	}
	want, err := gcsMissingMembershipSourceRecord(ctx, source)
	if err != nil {
		return err
	}
	var existing recovery.MembershipRecord
	if json.Unmarshal(data, &existing) != nil || !bytesEqual(data, encoded(existing)) || existing != want {
		return fmt.Errorf("source membership is not the exact canonical async record for this run")
	}
	after, err := bucket.Attributes(ctx, key)
	if err != nil || after.Version == nil || *after.Version != *before.Version {
		return errors.Join(err, fmt.Errorf("source membership changed while checking the run-owned record"))
	}
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("open GCS client for exact-generation test-scoped removal")
	}
	defer client.Close()
	conditional := client.Bucket(source.Bucket).Object(key).Generation(generation).If(storage.Conditions{GenerationMatch: generation})
	if err := conditional.Delete(ctx); err != nil {
		return fmt.Errorf("remove only the run-owned source membership object for this qualification: %w", err)
	}
	return requireGCSMembershipAbsent(ctx, bucket, source.Prefix)
}

func requireGCSMembershipAbsent(ctx context.Context, bucket objstore.Bucket, prefix string) error {
	reader, err := bucket.Get(ctx, path.Join(prefix, "voters/membership.json"))
	if err != nil {
		if bucket.IsObjNotFoundErr(err) {
			return nil
		}
		return fmt.Errorf("source membership absence is not verifiable")
	}
	_ = reader.Close()
	return fmt.Errorf("source membership reappeared during the qualification")
}

func gcsMissingMembershipMembers(ctx context.Context, source gcsRecoverySource) ([]quepaxa.Member, error) {
	sts, err := get(ctx, "statefulset", source.StatefulSet)
	if err != nil {
		return nil, err
	}
	container, err := boundContainer(sts, source.Container)
	if err != nil {
		return nil, err
	}
	var raw string
	envEntries, _ := nested(container, "env").([]any)
	for _, entry := range envEntries {
		if text(nested(entry, "name")) != "RHIZA_CLUSTER_MEMBERS" {
			continue
		}
		if raw != "" {
			return nil, fmt.Errorf("source cluster-member environment is ambiguous")
		}
		if value := text(nested(entry, "value")); value != "" {
			raw = value
			continue
		}
		secretName := text(nested(entry, "valueFrom", "secretKeyRef", "name"))
		secretKey := text(nested(entry, "valueFrom", "secretKeyRef", "key"))
		if secretName == "" || secretKey != "RHIZA_CLUSTER_MEMBERS" {
			return nil, fmt.Errorf("source cluster-member environment is not bound to its expected Secret key")
		}
		secret, err := get(ctx, "secret", secretName)
		if err != nil {
			return nil, err
		}
		encodedValue := text(nested(secret, "data", secretKey))
		decoded, err := base64.StdEncoding.DecodeString(encodedValue)
		if err != nil || len(decoded) == 0 || len(decoded) > 64<<10 {
			return nil, fmt.Errorf("source cluster-member Secret value is unavailable or invalid")
		}
		raw = string(decoded)
	}
	if raw == "" {
		return nil, fmt.Errorf("source workload has no RHIZA_CLUSTER_MEMBERS binding")
	}
	var members []quepaxa.Member
	if err := json.Unmarshal([]byte(raw), &members); err != nil || len(members) != len(source.Targets) {
		return nil, fmt.Errorf("source cluster-member binding does not describe the captured voter cohort")
	}
	want := make(map[string]string, len(source.Targets))
	for _, target := range source.Targets {
		want[target.NodeID] = target.WALIdentity
	}
	seen := make(map[string]bool, len(members))
	for _, member := range members {
		id := string(member.ID)
		if id == "" || seen[id] || want[id] == "" || member.WALIdentity != want[id] || member.URL == "" || member.LogURL == "" {
			return nil, fmt.Errorf("source cluster-member binding differs from the authenticated voter identities")
		}
		seen[id] = true
	}
	if len(seen) != len(want) {
		return nil, fmt.Errorf("source cluster-member binding omits a captured voter")
	}
	return members, nil
}

func gcsMissingMembershipSourceRecord(ctx context.Context, source gcsRecoverySource) (recovery.MembershipRecord, error) {
	members, err := gcsMissingMembershipMembers(ctx, source)
	if err != nil {
		return recovery.MembershipRecord{}, err
	}
	return recovery.NewMembershipRecord(source.ClusterID, members, "async"), nil
}

func qualifyGCSMissingMembershipNegative(ctx context.Context, source gcsRecoverySource, archive objstore.Bucket, out string, event func(string, any)) error {
	request, ref, err := missingMembershipRecoveryManifest(source, false)
	if err != nil {
		return err
	}
	if _, err := commandWithInput(ctx, encoded(request), "kubectl", "--context", kubeContext, "-n", source.Namespace, "create", "-f", "-"); err != nil {
		return fmt.Errorf("create no-opt-in negative Recovery: %w", err)
	}
	if err := waitForMissingMembershipBlocked(ctx, source, &ref); err != nil {
		return err
	}
	if err := verifyMissingMembershipNoClaims(ctx, source, archive); err != nil {
		return err
	}
	if err := requireGCSMembershipAbsent(ctx, archive, source.Prefix); err != nil {
		return err
	}
	event("missing_membership_without_opt_in_blocked_before_claim", obj{"case": "negative", "recoveryUID": ref.UID, "sourceClusterID": source.ClusterID})
	return os.WriteFile(filepath.Join(out, "gcs-missing-membership-negative-evidence.json"), encoded(obj{
		"qualification": "gcs-missing-membership-no-opt-in-rejection", "case": "negative",
		"sourceClusterID": source.ClusterID, "statefulSetUID": source.StatefulSetUID,
		"recoveryUID": ref.UID, "phase": "Blocked", "sourceMembershipAbsent": true,
		"sourceSuccessorAbsent": true, "targetScopeUnclaimed": true, "missingMembershipOptInAbsent": true,
	}), 0600)
}

func qualifyGCSMissingMembershipPositive(ctx context.Context, source gcsRecoverySource, archive objstore.Bucket, adminToken, out string, event func(string, any), faults *[]*gcsChaosFault) (resultErr error) {
	forward, err := startForward(ctx, source.Targets[0].Pod, out)
	if err != nil {
		return err
	}
	if _, err := call(ctx, forward, "/sql/execute", obj{"request_id": "gcs-missing-membership-create-table", "sql": "CREATE TABLE fencing_items (id INTEGER PRIMARY KEY, value TEXT)"}, adminToken); err != nil {
		forward.close()
		return fmt.Errorf("create source table for missing-membership recovery: %w", err)
	}
	var acks []gcsAcknowledgement
	var rowValues []string
	for id := 1; id <= 3; id++ {
		value := fmt.Sprintf("gcs-missing-membership-source-%d", id)
		ack, err := writeGCSAcknowledgedRow(ctx, forward, adminToken, id, value)
		if err != nil {
			forward.close()
			return fmt.Errorf("acknowledge source row %d: %w", id, err)
		}
		acks = append(acks, ack)
		rowValues = append(rowValues, fmt.Sprintf("[%d,%q]", id, value))
	}
	forward.close()
	expectedRows := "[" + strings.Join(rowValues, ",") + "]"
	lastAck := acks[len(acks)-1].Slot
	var beforeFence gcsArchiveHeadEvidence
	archiveCtx, cancelArchive := context.WithTimeout(ctx, gcsAsyncNonemptyMaxHeadWait)
	err = wait(archiveCtx, "missing-membership nonempty async source tip", func() (bool, error) {
		head, err := readGCSArchiveHead(archiveCtx, archive, source.Prefix, false)
		if err != nil {
			return false, err
		}
		if head.Sealed {
			return false, fmt.Errorf("source HEAD sealed before its generation fence")
		}
		if head.Tip < lastAck {
			return false, nil
		}
		beforeFence = head
		return true, nil
	})
	cancelArchive()
	if err != nil || beforeFence.Tip == 0 || beforeFence.Version == "" || beforeFence.HeadSHA256 == "" {
		return errors.Join(err, fmt.Errorf("missing-membership source lacks its acknowledged nonempty archived tip"))
	}
	if err := requireGCSMembershipAbsent(ctx, archive, source.Prefix); err != nil {
		return err
	}
	request, recoveryRef, err := missingMembershipRecoveryManifest(source, true)
	if err != nil {
		return err
	}
	if _, err := commandWithInput(ctx, encoded(request), "kubectl", "--context", kubeContext, "-n", source.Namespace, "create", "-f", "-"); err != nil {
		return fmt.Errorf("create opt-in missing-membership Recovery: %w", err)
	}
	uid, err := waitForExplicitGCSRecoveryAwaitingFence(ctx, source, recoveryRef.Name, recoveryRef.OperationID, "", true)
	if err != nil {
		return err
	}
	recoveryRef.UID = uid
	outcome, err := gcsLoseQuorumAndRecoverMissingMembership(ctx, source, archive, adminToken, expectedRows, &recoveryRef, out, event, faults)
	if err != nil {
		return err
	}
	defer outcome.Forward.close()
	sealedHead, err := readGCSArchiveHead(ctx, archive, source.Prefix, false)
	if err != nil {
		return err
	}
	if err := validateSealedNonemptyHead(beforeFence, sealedHead); err != nil {
		return err
	}
	if err := requireGCSMembershipAbsent(ctx, archive, source.Prefix); err != nil {
		return err
	}
	if outcome.Recovery.Spec.AllowDataLoss != true || !outcome.Recovery.Spec.AllowMissingSourceMembership ||
		outcome.Recovery.Spec.Durability != "async" || outcome.Recovery.Status.SourceDurability != "async" ||
		outcome.Recovery.Status.TargetBaseKind != "fork" || outcome.Recovery.Status.RecoveredTip != beforeFence.Tip ||
		outcome.Recovery.Status.ManifestHash == "" || outcome.Recovery.Status.ManualMembershipBindingDigest == "" {
		return fmt.Errorf("completed recovery lacks its exact async missing-membership binding and nonempty fork")
	}
	sourceMembers, err := gcsMissingMembershipMembers(ctx, source)
	if err != nil {
		return err
	}
	targetMembers, credentialEvidence, err := gcsMissingMembershipTargetCredentials(ctx, source, outcome.Recovery, sourceMembers)
	if err != nil {
		return err
	}
	targetMembership, err := verifyGCSMissingMembershipTarget(ctx, source, archive, outcome.Recovery, targetMembers)
	if err != nil {
		return err
	}
	recovered, err := call(ctx, outcome.Forward, "/sql/query", obj{"sql": "SELECT id,value FROM fencing_items ORDER BY id", "consistency": "linearizable"}, outcome.Admin)
	if err != nil || string(encoded(recovered["rows"])) != expectedRows {
		return fmt.Errorf("recovered target read barrier differs from acknowledged source rows: %v", err)
	}
	targetAck, err := writeGCSAcknowledgedRow(ctx, outcome.Forward, outcome.Admin, 4, "gcs-missing-membership-target")
	if err != nil {
		return fmt.Errorf("target quorum write/readback after missing-membership recovery: %w", err)
	}
	if err := requireGCSMembershipAbsent(ctx, archive, source.Prefix); err != nil {
		return err
	}
	event("missing_membership_opt_in_recovered_target", obj{"source": source.ClusterID, "target": outcome.Recovery.Status.Target, "tip": sealedHead.Tip})
	evidence := obj{
		"qualification": "gcs-manual-missing-membership-nonempty-fork", "case": "positive",
		"profile":                obj{"provider": "gcs", "noPVC": true, "sourceDurability": "async", "allowDataLoss": true, "allowMissingSourceMembership": true},
		"sourceAcknowledgements": acks, "sourceRows": expectedRows, "sourceHeadBeforeFence": beforeFence,
		"sealedSourceHead": sealedHead, "fence": outcome.Fence, "proof": outcome.Proof,
		"sourceIdentity": obj{"namespace": source.Namespace, "statefulSet": source.StatefulSet, "statefulSetUID": source.StatefulSetUID,
			"container": source.Container, "bindingUID": source.BindingUID, "logicalID": source.LogicalID, "clusterID": source.ClusterID, "bucket": source.Bucket,
			"prefix": source.Prefix, "targets": source.Targets},
		"recovery": outcome.Recovery, "sourceMembershipAbsent": true,
		"sourceMembers": sourceMembers, "targetCredentials": credentialEvidence, "targetMembership": targetMembership,
		"manualMembershipBindingDigest": outcome.Recovery.Status.ManualMembershipBindingDigest,
		"targetMembershipVerified":      true, "recoveredRows": string(encoded(recovered["rows"])),
		"targetQuorumVerified": true, "targetAck": targetAck,
	}
	if err := os.WriteFile(filepath.Join(out, "gcs-missing-membership-positive-evidence.json"), encoded(evidence), 0600); err != nil {
		return err
	}
	return nil
}

func missingMembershipRecoveryManifest(source gcsRecoverySource, allow bool) (obj, explicitGCSRecoveryRef, error) {
	phase := "missing-membership-negative"
	if allow {
		phase = "missing-membership-positive"
	}
	name := "rhiza-manual-" + qualificationRunID + "-" + phase
	recoveryID := "manual-" + qualificationRunID + "-" + phase
	request := obj{
		"apiVersion": "rhiza.mrchypark.dev/v1alpha1", "kind": "RhizaRecovery",
		"metadata": obj{"name": name, "namespace": source.Namespace, "labels": obj{"chaos.rhiza.io/run": qualificationRunID}},
		"spec": obj{
			"statefulSet": source.StatefulSet, "container": source.Container,
			"sourceClusterID": source.ClusterID, "durability": "async", "allowDataLoss": true,
			"allowMissingSourceMembership": allow, "recoveryID": recoveryID,
			"fence": obj{"recoveryID": recoveryID, "clusterID": source.ClusterID, "statefulSetUID": source.StatefulSetUID, "confirmed": false},
		},
	}
	return request, explicitGCSRecoveryRef{Name: name, Namespace: source.Namespace, RunLabel: qualificationRunID,
		OperationID: recoveryID, SourceClusterID: source.ClusterID, StatefulSet: source.StatefulSet,
		Container: source.Container, Durability: "async", AllowDataLoss: true}, nil
}

func waitForMissingMembershipBlocked(ctx context.Context, source gcsRecoverySource, ref *explicitGCSRecoveryRef) error {
	return wait(ctx, "no-opt-in missing-membership Recovery rejection", func() (bool, error) {
		resource, err := get(ctx, "rhizarecovery", ref.Name)
		if err != nil {
			return false, err
		}
		if text(nested(resource, "metadata", "namespace")) != ref.Namespace ||
			text(nested(resource, "metadata", "labels", "chaos.rhiza.io/run")) != ref.RunLabel ||
			nested(resource, "spec", "recoveryID") != ref.OperationID || nested(resource, "spec", "sourceClusterID") != source.ClusterID ||
			nested(resource, "spec", "allowMissingSourceMembership") == true || nested(resource, "status", "target") != nil {
			return false, fmt.Errorf("negative Recovery identity changed or carried the opt-in")
		}
		if nested(resource, "status", "phase") != "Blocked" {
			return false, nil
		}
		if !strings.Contains(text(nested(resource, "status", "message")), "immutable source membership is missing outside") {
			return false, fmt.Errorf("negative Recovery blocked for an unexpected reason")
		}
		uid := text(nested(resource, "metadata", "uid"))
		if uid == "" {
			return false, fmt.Errorf("blocked negative Recovery has no immutable UID")
		}
		ref.UID = uid
		return true, nil
	})
}

func verifyMissingMembershipNoClaims(ctx context.Context, source gcsRecoverySource, archive objstore.Bucket) error {
	if err := verifyNoGCSReservationAt(ctx, archive, source.Prefix); err != nil {
		return err
	}
	targetID, targetPrefix, controlPrefix, err := missingMembershipTargetScopes(source)
	if err != nil {
		return err
	}
	deployment, err := get(ctx, "deployment", "rhiza-fencer")
	if err != nil {
		return err
	}
	readScope, err := newGCSAckLossReadScope(ctx, source, deployment)
	if err != nil {
		return err
	}
	if readScope.Prefix != controlPrefix {
		return fmt.Errorf("missing-membership read-only CONTROL scope differs from the exact run scope")
	}
	_, _, client, controlBucket, _, err := openGCSAckLossAuthorityStores(ctx, source, readScope)
	if err != nil {
		return err
	}
	if client != nil {
		defer client.Close()
	}
	claimKey, err := manualTerminalSuccessorClaimKey(source, controlPrefix)
	if err != nil {
		return err
	}
	if err := verifyManualTerminalUnclaimedWithBuckets(ctx, archive, controlBucket, source,
		operator.ManualRecoveryChallenge{TargetClusterID: targetID}, claimKey, targetPrefix, controlPrefix); err != nil {
		return err
	}
	return nil
}

func missingMembershipTargetScopes(source gcsRecoverySource) (string, string, string, error) {
	scope := fencing.ArchiveScope{Provider: "gcs", Bucket: source.Bucket, Prefix: source.Prefix, ClusterID: source.ClusterID}
	scopeHash, err := scope.Hash()
	if err != nil {
		return "", "", "", err
	}
	targetID, err := recovery.FrozenTargetID(recovery.FrozenSourceIdentity{ScopeHash: scopeHash, ClusterID: source.ClusterID, StatefulSetUID: source.StatefulSetUID})
	if err != nil {
		return "", "", "", err
	}
	return targetID, path.Join(path.Dir(source.Prefix), targetID), path.Join(path.Dir(source.Prefix), "control"), nil
}

func verifyMissingMembershipRecoverySpec(ctx context.Context, ref explicitGCSRecoveryRef, allow bool) error {
	resource, err := get(ctx, "rhizarecovery", ref.Name)
	if err != nil {
		return err
	}
	if text(nested(resource, "metadata", "name")) != ref.Name || text(nested(resource, "metadata", "namespace")) != ref.Namespace ||
		text(nested(resource, "metadata", "uid")) != ref.UID || text(nested(resource, "metadata", "labels", "chaos.rhiza.io/run")) != ref.RunLabel ||
		nested(resource, "spec", "statefulSet") != ref.StatefulSet || nested(resource, "spec", "container") != ref.Container ||
		nested(resource, "spec", "sourceClusterID") != ref.SourceClusterID || nested(resource, "spec", "recoveryID") != ref.OperationID ||
		nested(resource, "spec", "allowMissingSourceMembership") != allow || nested(resource, "spec", "allowDataLoss") != true ||
		nested(resource, "spec", "durability") != "async" {
		return fmt.Errorf("Recovery does not preserve the exact missing-membership and async-loss contract")
	}
	return nil
}

func gcsMissingMembershipTargetCredentials(ctx context.Context, source gcsRecoverySource, child operator.Resource, sourceMembers []quepaxa.Member) ([]quepaxa.Member, obj, error) {
	secretName := child.Status.SecretName
	operationID := text(nested(child.Metadata, "uid")) + ":" + child.Spec.RecoveryID
	targetID, _, _, err := missingMembershipTargetScopes(source)
	if err != nil || child.Status.Target != targetID || operationID == ":" || secretName == "" ||
		secretName != "rhiza-recovery-"+gcsShortID(targetID) {
		return nil, nil, fmt.Errorf("target credentials do not match the exact recovery identity")
	}
	secret, err := get(ctx, "secret", secretName)
	if err != nil {
		return nil, nil, fmt.Errorf("read immutable target credential identity: %w", err)
	}
	if text(nested(secret, "metadata", "name")) != secretName || text(nested(secret, "metadata", "namespace")) != source.Namespace ||
		text(nested(secret, "metadata", "uid")) == "" || text(nested(secret, "metadata", "resourceVersion")) == "" ||
		text(nested(secret, "metadata", "annotations", "rhiza.mrchypark.dev/recovery-owner")) != operationID ||
		secret["type"] != "Opaque" || secret["immutable"] != true {
		return nil, nil, fmt.Errorf("target credential Secret is not immutable and owned by this recovery")
	}
	encodedMembers, err := base64.StdEncoding.DecodeString(text(nested(secret, "data", "members")))
	if err != nil || len(encodedMembers) == 0 || len(encodedMembers) > 64<<10 {
		return nil, nil, fmt.Errorf("target credential membership is unavailable")
	}
	var targetMembers []quepaxa.Member
	decoder := json.NewDecoder(strings.NewReader(string(encodedMembers)))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&targetMembers) != nil || len(targetMembers) != len(sourceMembers) || !bytesEqual(encodedMembers, encoded(targetMembers)) {
		return nil, nil, fmt.Errorf("target credential membership is not canonical")
	}
	sourceByID := make(map[quepaxa.NodeID]quepaxa.Member, len(sourceMembers))
	for _, member := range sourceMembers {
		sourceByID[member.ID] = member
	}
	seen := make(map[quepaxa.NodeID]bool, len(targetMembers))
	for _, member := range targetMembers {
		old, ok := sourceByID[member.ID]
		if !ok || seen[member.ID] || member.PublicKey == (quepaxa.PublicKey{}) || member.PublicKey == old.PublicKey ||
			member.URL != old.URL || member.PeerURL != old.PeerURL || member.LogURL != old.LogURL || member.WALIdentity != old.WALIdentity {
			return nil, nil, fmt.Errorf("target credential membership is not the exact fresh-key replacement cohort")
		}
		seen[member.ID] = true
	}
	if len(seen) != len(sourceByID) {
		return nil, nil, fmt.Errorf("target credential membership omits a source voter")
	}
	sum := sha256.Sum256(encoded(targetMembers))
	evidence := obj{"secretName": secretName, "secretUID": text(nested(secret, "metadata", "uid")),
		"resourceVersion": text(nested(secret, "metadata", "resourceVersion")), "ownerOperationID": operationID,
		"immutable": true, "membersSHA256": hex.EncodeToString(sum[:]), "members": targetMembers}
	return targetMembers, evidence, nil
}

func gcsShortID(value string) string {
	sum := sha256.Sum256(encoded(value))
	return hex.EncodeToString(sum[:])[:24]
}

func verifyGCSMissingMembershipTarget(ctx context.Context, source gcsRecoverySource, archive objstore.Bucket, child operator.Resource, members []quepaxa.Member) (recovery.MembershipRecord, error) {
	targetID, targetPrefix, _, err := missingMembershipTargetScopes(source)
	if err != nil {
		return recovery.MembershipRecord{}, err
	}
	if child.Status.Target != targetID {
		return recovery.MembershipRecord{}, fmt.Errorf("missing-membership target is not deterministically derived from the source scope")
	}
	reader, err := archive.Get(ctx, path.Join(targetPrefix, "voters/membership.json"))
	if err != nil {
		return recovery.MembershipRecord{}, fmt.Errorf("read exact target membership after recovery: %w", err)
	}
	data, readErr := io.ReadAll(io.LimitReader(reader, 64<<10))
	closeErr := reader.Close()
	var membership recovery.MembershipRecord
	want := recovery.NewMembershipRecord(targetID, members, "async")
	if readErr != nil || closeErr != nil || len(data) >= 64<<10 || json.Unmarshal(data, &membership) != nil || !bytesEqual(data, encoded(membership)) || membership != want {
		return recovery.MembershipRecord{}, errors.Join(readErr, closeErr, fmt.Errorf("target membership is not the canonical async record for the deterministic target"))
	}
	return membership, nil
}

func gcsLoseQuorumAndRecoverMissingMembership(ctx context.Context, source gcsRecoverySource, archive objstore.Bucket, token, expectedRows string, recoveryRef *explicitGCSRecoveryRef, out string, event func(string, any), faults *[]*gcsChaosFault) (*gcsRecoveryOutcome, error) {
	chaosCtx, cancelChaos := context.WithTimeout(ctx, 165*time.Second)
	defer cancelChaos()
	if len(source.Targets) != 3 || len(source.AgentPods) == 0 || source.OperatorPod == "" {
		return nil, fmt.Errorf("missing-membership scenario lacks the exact source/control cohort")
	}
	if err := verifyGCSControlPodSnapshots(chaosCtx, source.AgentPods); err != nil {
		return nil, err
	}
	agentFault, err := createGCSControlLoss(chaosCtx, namespace, qualificationRunID, "missing-membership", "runtime-agent", source.AgentPods)
	if agentFault != nil {
		*faults = append(*faults, agentFault)
	}
	if err != nil || !gcsFaultSourceMatches(agentFault, source.AgentPods) {
		return nil, errors.Join(err, fmt.Errorf("runtime-agent fault differs from the captured source cohort"))
	}
	if err := agentFault.WaitInjected(chaosCtx); err != nil {
		return nil, err
	}
	cohortFault, err := partitionGCSVoterCohort(chaosCtx, namespace, qualificationRunID, "missing-membership", source.Targets)
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
	err = wait(chaosCtx, "missing-membership source quorum loss", func() (bool, error) {
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
	if err := verifyNoGCSReservation(chaosCtx, source); err != nil {
		return nil, err
	}
	if err := requestGenerationFence(chaosCtx, source, explicitGenerationFenceRequest(source, recoveryRef.OperationID), out); err != nil {
		return nil, err
	}
	pending, err := waitForPendingGCSFenceOperation(chaosCtx, source, recoveryRef.OperationID)
	if err != nil {
		return nil, err
	}
	if err := validateExplicitGenerationFenceRequest(pending.Request, source, recoveryRef.OperationID); err != nil {
		return nil, err
	}
	recoveryUID, err := waitForExplicitGCSRecoveryAwaitingFence(chaosCtx, source, recoveryRef.Name, recoveryRef.OperationID, "", true)
	if err != nil {
		return nil, err
	}
	recoveryRef.UID = recoveryUID
	if err := verifyMissingMembershipRecoverySpec(chaosCtx, *recoveryRef, true); err != nil {
		return nil, err
	}
	if err := verifyMissingMembershipNoClaims(chaosCtx, source, archive); err != nil {
		return nil, err
	}
	if err := requireGCSMembershipAbsent(chaosCtx, archive, source.Prefix); err != nil {
		return nil, err
	}
	operatorSnapshot := []gcsControlPodIdentity{source.OperatorIdentity}
	if err := verifyGCSControlPodSnapshots(chaosCtx, operatorSnapshot); err != nil {
		return nil, err
	}
	operatorFault, err := createGCSControlLoss(chaosCtx, namespace, qualificationRunID, "missing-membership", "operator", operatorSnapshot)
	if operatorFault != nil {
		*faults = append(*faults, operatorFault)
	}
	if err != nil || !gcsFaultSourceMatches(operatorFault, operatorSnapshot) {
		return nil, errors.Join(err, fmt.Errorf("operator fault differs from the captured Pod identity"))
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
	if err := cohortFault.RecheckInjected(chaosCtx); err != nil {
		return nil, err
	}
	if err := agentFault.Restore(chaosCtx); err != nil {
		return nil, err
	}
	if err := waitForGCSFenceProof(chaosCtx, source, pending); err != nil {
		return nil, err
	}
	if err := confirmExplicitGCSRecoveryFence(chaosCtx, source, pending, *recoveryRef); err != nil {
		return nil, err
	}
	if err := operatorFault.RecheckInjected(chaosCtx); err != nil {
		return nil, err
	}
	if err := cohortFault.RecheckInjected(chaosCtx); err != nil {
		return nil, err
	}
	oldReturn, err := waitForOldGenerationReturn(chaosCtx, source, out)
	if err != nil {
		return nil, err
	}
	if err := cohortFault.Restore(chaosCtx); err != nil {
		return nil, err
	}
	if err := operatorFault.Restore(chaosCtx); err != nil {
		return nil, err
	}
	outcome, err := waitForGCSRecovery(ctx, source, pending, "fork", expectedRows, out, recoveryRef)
	if err != nil {
		return nil, err
	}
	outcome.OldReturn = oldReturn
	return outcome, nil
}

func bytesEqual(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
