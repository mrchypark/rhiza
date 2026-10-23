package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	internalobjstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

type frozenRegistrarTrackingBucket struct {
	objstore.Bucket
	sourcePrefix string
	writes       []string
}

func (b *frozenRegistrarTrackingBucket) BucketAttrs(context.Context) (*storage.BucketAttrs, error) {
	return &storage.BucketAttrs{Name: "archive", MetaGeneration: 1}, nil
}

func (b *frozenRegistrarTrackingBucket) Upload(ctx context.Context, name string, body io.Reader, options ...objstore.ObjectUploadOption) error {
	if strings.HasPrefix(name, b.sourcePrefix+"/") {
		b.writes = append(b.writes, "upload:"+name)
		return fmt.Errorf("frozen source upload attempted")
	}
	return b.Bucket.Upload(ctx, name, body, options...)
}

func (b *frozenRegistrarTrackingBucket) Delete(ctx context.Context, name string) error {
	if strings.HasPrefix(name, b.sourcePrefix+"/") {
		b.writes = append(b.writes, "delete:"+name)
		return fmt.Errorf("frozen source delete attempted")
	}
	return b.Bucket.Delete(ctx, name)
}

func (b *frozenRegistrarTrackingBucket) DeleteVersion(ctx context.Context, name string, version objstore.ObjectVersion) error {
	if strings.HasPrefix(name, b.sourcePrefix+"/") {
		b.writes = append(b.writes, "delete-version:"+name)
		return fmt.Errorf("frozen source versioned delete attempted")
	}
	deleter, ok := b.Bucket.(interface {
		DeleteVersion(context.Context, string, objstore.ObjectVersion) error
	})
	if !ok {
		return fmt.Errorf("underlying bucket does not support versioned delete")
	}
	return deleter.DeleteVersion(ctx, name, version)
}

func TestFrozenArchiveForkRegistrationCommitsAndRetriesWithoutSourceWrites(t *testing.T) {
	f := newArchiveGenesisFixture(t)
	ctx := t.Context()
	const sourceID, sourcePrefix, fenceOperation = "source", "root/source", "source-fence"
	if f.e.AuthorityState.bucket == f.e.ArchiveBucket {
		t.Fatal("test fixture must keep canonical CONTROL state separate from DATA archive objects")
	}
	sourceMembers := []quepaxa.Member{forkTestMember(t, "frozen-source-voter")}
	targetMembers := []quepaxa.Member{forkTestMember(t, "frozen-target-voter")}

	container := list(nested(f.f.authStatefulSet, "spec", "template", "spec", "containers"))[0]
	env := list(asObject(container)["env"])
	env = replaceEnv(env, "RHIZA_OBJSTORE_DURABILITY", object{"value": "async"})
	env = replaceEnv(env, "RHIZA_CLUSTER_MEMBERS", object{"value": string(jsonBytes(sourceMembers))})
	asObject(container)["env"] = env
	sts := f.f.authStatefulSet
	stsContainer := list(nested(sts, "spec", "template", "spec", "containers"))[0]
	asObject(stsContainer)["env"] = env
	f.f.authStatefulSet = sts
	f.f.mu.Lock()
	f.f.objects["/apis/apps/v1/namespaces/test/statefulsets"] = object{"items": []any{sts}}
	pod := independentPod(f.id.NodeID, f.id.PodUID)
	asObject(pod["metadata"])["ownerReferences"] = []any{object{"kind": "StatefulSet", "name": "rhiza", "uid": f.runtime.StatefulSetUID, "controller": true}}
	asObject(list(nested(pod, "spec", "containers"))[0])["env"] = env
	asObject(pod["status"])["containerStatuses"] = []any{object{"containerID": "containerd://source", "imageID": "sha256:source"}}
	f.f.objects["/api/v1/namespaces/test/pods"] = object{"items": []any{pod}}
	f.f.mu.Unlock()

	if err := f.e.ArchiveBucket.Upload(ctx, path.Join(sourcePrefix, "voters/membership.json"), bytes.NewReader(jsonBytes(recovery.NewMembershipRecord(sourceID, sourceMembers, "async"))), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	commitArchiveGenesisFixture(t, f)
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	core, err := quepaxa.New(quepaxa.Config{NodeID: sourceMembers[0].ID, Cluster: quepaxa.Cluster{ConfigID: recovery.ArchiveConfigID, Members: sourceMembers}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	decision, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "frozen-registration-seed", SQL: "CREATE TABLE frozen_source (value TEXT)"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, decision); err != nil {
		t.Fatal(err)
	}
	manager := recovery.NewManager(f.e.ArchiveBucket, sourcePrefix, recovery.ArchiveConfigID)
	if err := manager.SyncThrough(ctx, core, core.Tip()); err != nil {
		manager.Close()
		t.Fatal(err)
	}
	manager.Close()

	f.e.CollectRuntimeEvidence = func(context.Context, fencing.RuntimeBinding, fencing.StopRequest) ([]byte, error) {
		return []byte("frozen-registration-terminal"), nil
	}
	f.e.VerifyRuntimeEvidence = func(_ context.Context, _ fencing.RuntimeBinding, _ fencing.StopRequest, receipt []byte) error {
		if string(receipt) != "frozen-registration-terminal" {
			return errors.New("unexpected test runtime evidence")
		}
		return nil
	}
	fenceRequest := FenceRequest{LogicalID: "frozen-registration", BindingUID: f.runtime.BindingUID, Namespace: "test", StatefulSet: "rhiza",
		StatefulSetUID: f.runtime.StatefulSetUID, SourceClusterID: sourceID, OperationID: fenceOperation, Scope: ScopeGeneration}
	fence := &fenceResource{Metadata: object{"name": "source-fence", "namespace": "test"}, Spec: fenceRequest}
	fenceCollection := "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizafences"
	if err := f.e.reconcileFence(ctx, fenceCollection, fence); err != nil {
		t.Fatalf("close frozen source generation: %v", err)
	}
	fenceName := "fence-" + hashJSON([]string{f.runtime.BindingUID, fenceOperation})[:40]
	fence.Metadata["name"], fence.Metadata["namespace"], fence.Metadata["uid"], fence.Metadata["resourceVersion"] = fenceName, "test", "source-fence-uid", "1"
	fenceObjectData, err := json.Marshal(fence)
	if err != nil {
		t.Fatal(err)
	}
	var fenceObject object
	if err := json.Unmarshal(fenceObjectData, &fenceObject); err != nil {
		t.Fatal(err)
	}
	f.f.mu.Lock()
	f.f.objects[fenceCollection+"/"+fenceName] = fenceObject
	f.f.objects[fenceCollection] = object{"items": []any{fenceObject}}
	f.f.mu.Unlock()

	recoveryUID := "recovery-uid"
	operationID := recoveryUID + ":" + fenceOperation
	targetScope := fencing.ArchiveScope{Provider: "gcs", Bucket: "archive", Prefix: sourcePrefix, ClusterID: sourceID}
	identityHash, err := targetScope.Hash()
	if err != nil {
		t.Fatal(err)
	}
	targetID, err := recovery.FrozenTargetID(recovery.FrozenSourceIdentity{ScopeHash: identityHash, ClusterID: sourceID, StatefulSetUID: f.runtime.StatefulSetUID})
	if err != nil {
		t.Fatal(err)
	}
	targetPrefix := path.Join("root", targetID)
	resourceSpec := Spec{StatefulSet: "rhiza", SourceClusterID: sourceID, RecoveryID: fenceOperation, Durability: "async", AllowDataLoss: true}
	resource := Resource{APIVersion: "rhiza.mrchypark.dev/v1alpha1", Kind: "RhizaRecovery",
		Metadata: object{"name": "rhiza-recovery", "uid": recoveryUID, "namespace": "test"}, Spec: resourceSpec,
		Status: Status{StatefulSetUID: f.runtime.StatefulSetUID, Target: targetID, TargetBaseKind: "fork", RecoveryMode: RecoveryModeFrozenSource, Stage: "FrozenForked"}}
	// The DATA archive and fencer CONTROL state use separate buckets in this
	// fixture. Obtain proof material from the fencer side; never infer authority
	// state from the DATA archive.
	identity := recovery.FrozenSourceIdentity{ScopeHash: identityHash, ClusterID: sourceID, StatefulSetUID: f.runtime.StatefulSetUID}
	root, _, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, sourceID)
	if err != nil {
		t.Fatalf("read canonical CONTROL root: %v", err)
	}
	_, bindingUID, err := sourceRootBinding(root)
	if err != nil {
		t.Fatal(err)
	}
	canonicalFence, err := f.e.readCanonicalSourceFence(ctx, ArchiveForkGenesisRequest{StatefulSetUID: f.runtime.StatefulSetUID,
		TargetClusterID: targetID, OperationID: operationID, SourceFenceOperationID: fenceOperation}, sourceID, bindingUID, root)
	if err != nil {
		t.Fatalf("read canonical CONTROL fence proof: %v", err)
	}
	proofDigest, rootDigest := hashJSON(canonicalFence.Status.Proof), f.e.ledgerHash(root)
	tracked := &frozenRegistrarTrackingBucket{Bucket: f.e.ArchiveBucket, sourcePrefix: sourcePrefix}
	f.e.ArchiveBucket = tracked
	var reader internalobjstore.BucketAttrsReader = tracked
	environment, err := captureFrozenBucketEnvironment(ctx, reader, "archive", sourcePrefix)
	if err != nil {
		t.Fatal(err)
	}
	resource.Status.FrozenBucketEnvironment = &environment
	resourceData, err := json.Marshal(resource)
	if err != nil {
		t.Fatal(err)
	}
	var resourceObject object
	if err := json.Unmarshal(resourceData, &resourceObject); err != nil {
		t.Fatal(err)
	}
	f.f.mu.Lock()
	f.f.objects[f.e.controller().resourcePath("rhiza-recovery")] = resourceObject
	f.f.mu.Unlock()

	targetMembership := recovery.NewMembershipRecord(targetID, targetMembers, "async")
	options := recovery.FrozenForkOptions{
		ForkOptions: recovery.ForkOptions{SourcePrefix: sourcePrefix, TargetPrefix: targetPrefix,
			SourceBootstrap: quepaxa.Cluster{ConfigID: recovery.ArchiveConfigID, Members: sourceMembers},
			TargetMembers:   targetMembers, TargetMembership: targetMembership, OperationID: operationID},
		SourceIdentity: identity, FenceProofDigest: proofDigest, ClosedRootDigest: rootDigest,
		BucketEnvironment: environment, VerifyEnvironment: func(ctx context.Context, expected recovery.FrozenBucketEnvironment) error {
			return f.e.verifyFrozenBucketEnvironment(ctx, "archive", sourcePrefix, expected)
		},
	}
	fork, err := recovery.ForkFrozenSource(ctx, tracked, options)
	if err != nil {
		t.Fatalf("materialize frozen v3 source fork: %v", err)
	}
	intentData, err := readBoundedArchiveObject(ctx, tracked, path.Join(targetPrefix, "fork/intent.json"), 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	var forkIntent archiveForkIntentRecord
	if err := decodeCanonicalArchiveJSON(intentData, &forkIntent); err != nil || forkIntent.Version != 3 || forkIntent.FrozenSource == nil || forkIntent.FrozenSource.Frontier.Tip == 0 {
		t.Fatalf("target does not contain a nonempty frozen v3 intent: intent=%+v err=%v", forkIntent, err)
	}
	if err := tracked.Upload(ctx, path.Join(targetPrefix, "voters/membership.json"), bytes.NewReader(jsonBytes(targetMembership)), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	lineage := archiveForkLineageRecord{Version: 1, OperationID: operationID, Source: sourceID, SourceDurability: "async",
		SourceMembership: sourceFingerprint(sourceID, "async", sourceMembers, sourcePrefix), TargetMembership: targetMembership, Fork: fork.ForkResult}
	lineageData := jsonBytes(lineage)
	lineageKey := path.Join(targetPrefix, "recovery/generation.json")
	if err := tracked.Upload(ctx, lineageKey, bytes.NewReader(lineageData), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}

	request := ArchiveForkGenesisRequest{RecoveryName: "rhiza-recovery", StatefulSetUID: f.runtime.StatefulSetUID,
		TargetClusterID: targetID, OperationID: operationID, SourceFenceOperationID: fenceOperation,
		RecoveryMode: RecoveryModeFrozenSource, BucketEnvironment: &environment}
	if err := tracked.Delete(ctx, lineageKey); err != nil {
		t.Fatalf("remove lineage to simulate interrupted registration prerequisites: %v", err)
	}
	if err := f.e.RegisterArchiveForkGenesis(ctx, request); err == nil {
		t.Fatal("registration succeeded without the exact fork lineage")
	}
	sourceScope := fencing.ArchiveScope{Provider: "gcs", Bucket: "archive", Prefix: sourcePrefix, ClusterID: sourceID}
	claim, err := f.e.AuthorityState.readSourceSuccessorClaim(ctx, sourceScope, f.runtime.StatefulSetUID)
	if err != nil || claim.RecoveryMode != RecoveryModeFrozenSource || claim.TargetBaseKind != "fork" || claim.TargetClusterID != targetID || claim.OperationID != operationID || claim.FenceOperationID != fenceOperation {
		t.Fatalf("failed registration did not retain the exact source winner claim: claim=%+v err=%v", claim, err)
	}
	if _, _, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, targetID); !errors.Is(err, errAuthorityStateNotFound) {
		t.Fatalf("failed registration created target authority before artifacts verified: %v", err)
	}
	if err := tracked.Upload(ctx, lineageKey, bytes.NewReader(lineageData), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	// The fork result and lineage are decoded from separate canonical objects.
	// Their FrozenSource pointers must therefore differ even when every field
	// is identical; registration must compare the binding value, not addresses.
	resultData, err := readBoundedArchiveObject(ctx, tracked, path.Join(targetPrefix, "fork/result.json"), 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	var decodedResult recovery.ForkResult
	if err := decodeCanonicalArchiveJSON(resultData, &decodedResult); err != nil {
		t.Fatal(err)
	}
	decodedLineageData, err := readBoundedArchiveObject(ctx, tracked, lineageKey, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	var decodedLineage archiveForkLineageRecord
	if err := decodeCanonicalArchiveJSON(decodedLineageData, &decodedLineage); err != nil {
		t.Fatal(err)
	}
	if decodedResult.FrozenSource == nil || decodedLineage.Fork.FrozenSource == nil || decodedResult.FrozenSource == decodedLineage.Fork.FrozenSource {
		t.Fatal("fork result and lineage must contain separately allocated frozen bindings")
	}
	if hashJSON(decodedResult.FrozenSource) != hashJSON(decodedLineage.Fork.FrozenSource) {
		t.Fatal("independently decoded frozen bindings should have equal values")
	}

	// A separately allocated but tampered binding must be rejected by the real
	// registration path, while the original exact lineage remains retryable.
	tamperedBinding := *decodedLineage.Fork.FrozenSource
	tamperedBinding.FenceProofDigest = strings.Repeat("0", 64)
	decodedLineage.Fork.FrozenSource = &tamperedBinding
	tamperedLineageData := jsonBytes(decodedLineage)
	lineageAttrs, err := tracked.Attributes(ctx, lineageKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := tracked.Upload(ctx, lineageKey, bytes.NewReader(tamperedLineageData), objstore.WithIfMatch(lineageAttrs.Version)); err != nil {
		t.Fatal(err)
	}
	if err := f.e.RegisterArchiveForkGenesis(ctx, request); err == nil || !strings.Contains(err.Error(), "fork lineage differs from verified artifacts") {
		t.Fatalf("registration accepted a lineage with a different frozen binding: %v", err)
	}
	lineageAttrs, err = tracked.Attributes(ctx, lineageKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := tracked.Upload(ctx, lineageKey, bytes.NewReader(lineageData), objstore.WithIfMatch(lineageAttrs.Version)); err != nil {
		t.Fatal(err)
	}
	targetScopeClaimKey := fencing.ArchiveScope{Provider: "gcs", Bucket: "archive", Prefix: targetPrefix, ClusterID: targetID}
	wantRef, err := f.e.verifyArchiveForkArtifacts(ctx, request, sourceID, sourcePrefix, targetPrefix, targetScopeClaimKey, sourceMembers, "async", &fork.Source)
	if err != nil {
		t.Fatalf("verify frozen target fork artifacts before registration: %v", err)
	}
	if err := f.e.RegisterArchiveForkGenesis(ctx, request); err != nil {
		t.Fatalf("exact retry after the source claim and interrupted registration failed: %v", err)
	}
	if err := f.e.RegisterArchiveForkGenesis(ctx, request); err != nil {
		t.Fatalf("exact retry after committed registration failed: %v", err)
	}
	root, err = f.e.read(ctx, f.runtime.StatefulSetUID, targetID)
	if err != nil || root.Ledger.ArchiveGenesisState != fencing.ArchiveGenesisCommitted || !sameArchiveGenesisRef(root.Ledger.ArchiveGenesisRef, &wantRef) {
		t.Fatalf("frozen target root did not commit its verified fork reference: root=%+v err=%v", root.Ledger, err)
	}
	targetScopeClaim, _, err := f.e.AuthorityState.ReadArchiveScopeClaim(ctx, targetScopeClaimKey)
	if err != nil || targetScopeClaim.GenesisRef == nil || !sameArchiveGenesisRef(root.Ledger.ArchiveGenesisRef, targetScopeClaim.GenesisRef) {
		t.Fatalf("target scope claim does not join the committed fork reference: claim=%+v err=%v", targetScopeClaim, err)
	}
	if len(tracked.writes) != 0 {
		t.Fatalf("frozen fork or registrar attempted source data writes: %v", tracked.writes)
	}
}
