package operator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
	"google.golang.org/api/option"
)

func TestEmptyRecoveryCommittedRetryAcceptsEvolvedTargetHead(t *testing.T) {
	f := newArchiveGenesisFixture(t)
	ctx := t.Context()
	scope, targetScopeHash, err := f.e.archiveGenesisScope(ctx, f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	sourceScopeHash, err := (fencing.ArchiveScope{Provider: "gcs", Bucket: "archive", Prefix: "root/retired-source", ClusterID: "retired-source"}).Hash()
	if err != nil {
		t.Fatal(err)
	}
	if sourceScopeHash == targetScopeHash {
		t.Fatal("test source and target scopes unexpectedly match")
	}
	authorization := fencing.ArchiveEmptyRecoveryAuthorization{
		Version: fencing.ArchiveGenesisVersion, StatefulSetUID: f.runtime.StatefulSetUID,
		TargetClusterID: f.runtime.SourceClusterID, OperationID: "recovery-op", SourceFenceOperationID: "fence-op",
		ConfigID: recovery.ArchiveConfigID, SourceTip: 0, SourceScopeHash: sourceScopeHash, TargetScopeHash: targetScopeHash,
		SourceGenesisRefDigest: strings.Repeat("1", 64), SourceHeadDigest: strings.Repeat("2", 64),
		SourceHeadVersion:     objstore.ObjectVersion{Type: objstore.ETag, Value: "sealed-source-head-etag"},
		TargetEmptyHeadDigest: f.intent.PayloadDigest, SourceRootDigest: strings.Repeat("3", 64),
		SourceFenceDigest: strings.Repeat("4", 64), SourceMembershipHash: strings.Repeat("5", 64),
		SourceDurability: "async", TargetMembershipHash: strings.Repeat("6", 64), TargetDurability: "async",
		AllowDataLoss: true, PolicyDigest: strings.Repeat("7", 64),
	}
	if err := authorization.Validate(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.e.change(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID, func(ledger *fencing.Ledger) error {
		ledger.ArchiveGenesisState = fencing.ArchiveGenesisEmptyRecoveryPending
		ledger.ArchiveScopeHash = targetScopeHash
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := f.e.AuthorityState.SetArchiveScopeEmptyRecoveryAuthorization(ctx, scope, authorization); err != nil {
		t.Fatal(err)
	}
	if err := f.e.AuthorityState.SetRootEmptyRecoveryAuthorization(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID, authorization); err != nil {
		t.Fatal(err)
	}
	commitArchiveGenesisFixture(t, f)

	// This advances the target's valid HEAD without changing the immutable
	// empty-genesis mutation receipt or its separate source authorization.
	manager := recovery.NewManager(f.e.ArchiveBucket, scope.Prefix, recovery.ArchiveConfigID)
	if err := manager.Load(ctx); err != nil {
		t.Fatal(err)
	}
	prefixHash := sha256.Sum256([]byte("target-base-prefix"))
	anchorHash := sha256.Sum256([]byte("target-lineage-anchor"))
	if err := manager.InitializeFencedGeneration(ctx, 1, prefixHash, anchorHash); err != nil {
		manager.Close()
		t.Fatalf("advance target archive HEAD: %v", err)
	}
	manager.Close()
	response, found, err := f.e.readCommittedEmptyRecovery(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID, scope, authorization)
	if err != nil || !found || response.State != fencing.EmptyRecoveryCommitted {
		t.Fatalf("committed retry did not recognize the evolved target HEAD: found=%v response=%+v err=%v", found, response, err)
	}
}

func TestEmptyRecoveryClosedRootBindingIsExactAndScopeBound(t *testing.T) {
	e := &FencingExecutor{Kube: &Kubernetes{Namespace: "ns"}}
	state := fenceLedger{StatefulSetUID: "sts-uid", Ledger: fencing.Ledger{
		SourceClusterID: "source", ClosedBy: "fence-op", ClosedHash: strings.Repeat("a", 64),
		RequireMutationJournal: true, ArchiveGenesisState: fencing.ArchiveGenesisCommitted,
		ArchiveScopeHash: strings.Repeat("b", 64),
		ArchiveGenesisRef: &fencing.ArchiveGenesisRef{Version: fencing.ArchiveGenesisVersion, ConfigID: recovery.ArchiveConfigID,
			ScopeHash: strings.Repeat("c", 64), Mutation: &fencing.ArchiveGenesisMutation{ProducerUID: "producer", Sequence: 1}},
	}}
	proofHash := e.ledgerHash(state)
	if err := verifyEmptyRecoveryClosedRootBinding(e, state, proofHash); err != nil {
		t.Fatalf("exact closed root proof was rejected: %v", err)
	}
	if err := verifyEmptyRecoveryClosedRootBinding(e, state, strings.Repeat("d", 64)); err == nil {
		t.Fatal("accepted proof hash from another canonical root")
	}
	wrongRoot := state
	wrongRoot.Ledger.SourceClusterID = "replacement-source"
	if err := verifyEmptyRecoveryClosedRootBinding(e, wrongRoot, proofHash); err == nil {
		t.Fatal("accepted old proof after source root identity changed")
	}
	stable := emptyRecoveryClosedRootDigest(e.Kube.Namespace, state)
	if stable == emptyRecoveryClosedRootDigest(e.Kube.Namespace, wrongRoot) {
		t.Fatal("stable closed-root identity omitted the source cluster")
	}
	// Kubernetes metadata is not part of the canonical authority root; changing
	// unrelated object metadata cannot change the digest inputs above.
	e.Kube.Namespace = "other-ns"
	if stable == emptyRecoveryClosedRootDigest(e.Kube.Namespace, state) {
		t.Fatal("closed-root identity omitted its authority namespace")
	}
}

func TestLegacySuccessorReservationAcceptsControllerMapEncodingOnly(t *testing.T) {
	want := archiveForkReservation{Version: 1, OperationID: "recovery-uid:fence-op", Target: "rhiza-r-target",
		Source: "source", SourceMembership: strings.Repeat("a", 64), StatefulSetUID: "sts-uid", SpecHash: strings.Repeat("b", 64), TargetBaseKind: "empty"}
	wire := jsonBytes(object{"version": want.Version, "operation_id": want.OperationID, "target": want.Target,
		"source": want.Source, "source_membership": want.SourceMembership, "statefulset_uid": want.StatefulSetUID,
		"spec_hash": want.SpecHash, "target_base_kind": want.TargetBaseKind})
	var got archiveForkReservation
	if err := decodeLegacySuccessorReservation(wire, &got); err != nil || got != want {
		t.Fatalf("controller map-encoded reservation did not round-trip: got=%+v err=%v", got, err)
	}

	unknown := object{"version": want.Version, "operation_id": want.OperationID, "target": want.Target,
		"source": want.Source, "source_membership": want.SourceMembership, "statefulset_uid": want.StatefulSetUID,
		"spec_hash": want.SpecHash, "target_base_kind": want.TargetBaseKind, "unexpected": true}
	if err := decodeLegacySuccessorReservation(jsonBytes(unknown), &got); err == nil {
		t.Fatal("accepted a legacy successor reservation with an unknown field")
	}
}

func TestEmptyRecoveryVerifiesRealSealedSourceAndClosedRoot(t *testing.T) {
	f := newArchiveGenesisFixture(t)
	e, ctx := f.e, t.Context()
	sourceMembers := []quepaxa.Member{forkTestMember(t, "empty-source")}
	container := list(nested(f.f.authStatefulSet, "spec", "template", "spec", "containers"))[0]
	env := list(asObject(container)["env"])
	env = append(env,
		object{"name": "RHIZA_OBJSTORE_DURABILITY", "value": "async"},
		object{"name": "RHIZA_CLUSTER_MEMBERS", "value": string(jsonBytes(sourceMembers))},
	)
	asObject(container)["env"] = env
	f.f.mu.Lock()
	f.f.objects["/apis/apps/v1/namespaces/test/statefulsets"] = object{"items": []any{f.f.authStatefulSet}}
	pod := independentPod(f.id.NodeID, f.id.PodUID)
	asObject(pod["metadata"])["ownerReferences"] = []any{object{"kind": "StatefulSet", "name": "rhiza", "uid": f.runtime.StatefulSetUID, "controller": true}}
	asObject(pod["status"])["containerStatuses"] = []any{object{"containerID": "containerd://container", "imageID": "sha256:image"}}
	f.f.objects["/api/v1/namespaces/test/pods"] = object{"items": []any{pod}}
	f.f.mu.Unlock()
	if err := f.e.ArchiveBucket.Upload(ctx, "root/source/voters/membership.json", bytes.NewReader(jsonBytes(recovery.NewMembershipRecord("source", sourceMembers, "async"))), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	commitArchiveGenesisFixture(t, f)
	if err := recovery.Seal(ctx, f.e.ArchiveBucket, "root/source", "source-fence"); err != nil {
		t.Fatal(err)
	}
	e.CollectRuntimeEvidence = func(context.Context, fencing.RuntimeBinding, fencing.StopRequest) ([]byte, error) {
		return []byte("verified-runtime-terminal"), nil
	}
	e.VerifyRuntimeEvidence = func(_ context.Context, _ fencing.RuntimeBinding, _ fencing.StopRequest, receipt []byte) error {
		if string(receipt) != "verified-runtime-terminal" {
			return errors.New("unexpected runtime terminal")
		}
		return nil
	}
	fenceRequest := FenceRequest{LogicalID: "empty-recovery-test", BindingUID: f.runtime.BindingUID, Namespace: "test", StatefulSet: "rhiza",
		StatefulSetUID: f.runtime.StatefulSetUID, SourceClusterID: "source", OperationID: "source-fence", Scope: ScopeGeneration}
	fence := &fenceResource{Metadata: object{"name": "source-fence", "namespace": "test"}, Spec: fenceRequest}
	fenceCollection := "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizafences"
	if err := e.reconcileFence(ctx, fenceCollection, fence); err != nil {
		t.Fatalf("produce source generation proof: %v", err)
	}
	fenceName := "fence-" + hashJSON([]string{f.runtime.BindingUID, fenceRequest.OperationID})[:40]
	fence.Metadata["name"] = fenceName
	fence.Metadata["namespace"] = "test"
	fence.Metadata["uid"] = "source-fence-uid"
	fence.Metadata["resourceVersion"] = "1"
	fence.Metadata["annotations"] = object{"benign": "post-close metadata"}
	fenceData, err := json.Marshal(fence)
	if err != nil {
		t.Fatal(err)
	}
	var fenceObject object
	if err := json.Unmarshal(fenceData, &fenceObject); err != nil {
		t.Fatal(err)
	}
	f.f.mu.Lock()
	f.f.objects[fenceCollection+"/"+fenceName] = fenceObject
	f.f.mu.Unlock()
	state, err := e.read(ctx, f.runtime.StatefulSetUID, "source")
	if err != nil {
		t.Fatal(err)
	}
	request := ArchiveEmptyRecoveryRequest{RecoveryName: "rhiza-recovery", StatefulSetUID: f.runtime.StatefulSetUID,
		TargetClusterID: "", OperationID: "", SourceFenceOperationID: "source-fence",
		TargetDurability: "async", AllowDataLoss: true}
	recoveryResource := Resource{APIVersion: "rhiza.mrchypark.dev/v1alpha1", Kind: "RhizaRecovery",
		Metadata: object{"name": request.RecoveryName, "uid": "recovery-uid", "namespace": "test"},
		Spec:     Spec{StatefulSet: "rhiza", SourceClusterID: "source", Durability: "async", RecoveryID: "source-fence", AllowDataLoss: true},
		Status:   Status{StatefulSetUID: f.runtime.StatefulSetUID, TargetBaseKind: "empty"}}
	request.OperationID = "recovery-uid:source-fence"
	request.TargetClusterID = "rhiza-r-" + shortID(request.OperationID)
	request.PolicyDigest = hashJSON(recoveryResource.Spec)
	recoveryResource.Status.Target = request.TargetClusterID
	recoveryResource.Status.SourceMembership = sourceFingerprint("source", "async", sourceMembers, "root/source")
	recoveryResource.Status.SpecHash = immutableSpecHash(recoveryResource.Spec)
	// Legacy empty recovery requires the same immutable reservation written by
	// the controller before it admits recovery side effects.
	reservation := e.controller().reservation(&recoveryResource)
	if err := e.ArchiveBucket.Upload(ctx, path.Join("root/source", "recovery/successor.json"), bytes.NewReader(jsonBytes(reservation)), objstore.WithIfNotExists()); err != nil {
		t.Fatalf("seed the controller's exact source successor reservation: %v", err)
	}
	targetMembers := []quepaxa.Member{forkTestMember(t, "empty-target")}
	targetMembership := recovery.NewMembershipRecord(request.TargetClusterID, targetMembers, "async")
	targetMembershipBytes := jsonBytes(targetMembership)
	request.TargetMembershipHash = authorityStateDigest(targetMembershipBytes)
	targetPrefix := path.Join("root", request.TargetClusterID)
	if err := e.ArchiveBucket.Upload(ctx, path.Join(targetPrefix, "voters/membership.json"), bytes.NewReader(targetMembershipBytes), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	resourceBytes, err := json.Marshal(recoveryResource)
	if err != nil {
		t.Fatal(err)
	}
	var resourceObject object
	if err := json.Unmarshal(resourceBytes, &resourceObject); err != nil {
		t.Fatal(err)
	}
	f.f.mu.Lock()
	f.f.objects[e.controller().resourcePath(request.RecoveryName)] = resourceObject
	f.f.mu.Unlock()
	sts, err := e.archiveForkStatefulSet(ctx, f.runtime.StatefulSetUID)
	if err != nil {
		t.Fatal(err)
	}
	verifiedFence, sourceVersion, sealedHead, err := e.verifyEmptyRecoverySource(ctx, request, sts, "source", "root/source", state, StoreIdentity(map[string]string{
		"RHIZA_OBJSTORE_PROVIDER": "gcs", "RHIZA_OBJSTORE_BUCKET": "archive", "RHIZA_OBJSTORE_PREFIX": "root",
	}))
	if err != nil || verifiedFence.Status.Proof == nil || sourceVersion.Value == "" || len(sealedHead) == 0 {
		t.Fatalf("sealed source verification failed: version=%v err=%v", sourceVersion, err)
	}
	if err := verifyEmptyRecoveryClosedRootBinding(e, state, verifiedFence.Status.Proof.IndependentV2.LedgerHash); err != nil {
		t.Fatalf("closed source root did not bind to proof: %v", err)
	}
	wrongRoot := state
	wrongRoot.Ledger.SourceClusterID = "wrong-source"
	if err := verifyEmptyRecoveryClosedRootBinding(e, wrongRoot, verifiedFence.Status.Proof.IndependentV2.LedgerHash); err == nil {
		t.Fatal("accepted a source proof against another canonical root")
	}
	registered, err := e.RegisterArchiveEmptyRecovery(ctx, request)
	if err != nil || registered.State != fencing.EmptyRecoveryAuthorized {
		t.Fatalf("register verified empty source recovery: response=%+v err=%v", registered, err)
	}
	if registered.Authorization.RecoveryMode != "" || registered.Authorization.SourcePrefix != "" || registered.Authorization.BucketName != "" ||
		registered.Authorization.BucketMetageneration != "" || registered.Authorization.LifecyclePolicyDigest != "" {
		t.Fatalf("legacy empty authorization unexpectedly contains frozen-source environment: %+v", registered.Authorization)
	}
	// ResourceVersion and annotations may change after source closure. Exact
	// retries use the derived policy digest; the earlier raw spec-digest form is
	// intentionally not upgraded because its authorization failed client validation.
	fence.Metadata["resourceVersion"] = "2"
	fence.Metadata["annotations"] = object{"benign": "updated after close"}
	fenceData, err = json.Marshal(fence)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(fenceData, &fenceObject); err != nil {
		t.Fatal(err)
	}
	f.f.mu.Lock()
	f.f.objects[fenceCollection+"/"+fenceName] = fenceObject
	f.f.mu.Unlock()
	retry, err := e.RegisterArchiveEmptyRecovery(ctx, request)
	if err != nil || retry.State != fencing.EmptyRecoveryAuthorized || retry.Authorization != registered.Authorization {
		t.Fatalf("metadata-only source fence update changed authorization: response=%+v err=%v", retry, err)
	}

	// Simulate the externally managed StatefulSet moving to the target profile
	// before its first protected mutation. The registrar retry below must use
	// the persisted source authorization, not mistake this live target for the
	// retired source.
	currentSTSEnv := list(nested(f.f.authStatefulSet, "spec", "template", "spec", "containers"))[0]
	currentEnv := replaceEnv(list(asObject(currentSTSEnv)["env"]), "RHIZA_CLUSTER_ID", object{"value": request.TargetClusterID})
	currentEnv = replaceEnv(currentEnv, "RHIZA_CLUSTER_MEMBERS", object{"value": string(jsonBytes(targetMembers))})
	asObject(currentSTSEnv)["env"] = currentEnv
	authPodContainer := list(nested(f.f.authPod, "spec", "containers"))[0]
	asObject(authPodContainer)["env"] = currentEnv
	f.f.mu.Lock()
	f.f.objects["/apis/apps/v1/namespaces/test/statefulsets"] = object{"items": []any{f.f.authStatefulSet}}
	f.f.mu.Unlock()

	// Join the target root with a distinct producer, stage the same canonical
	// empty HEAD, and drive the ordinary mutation path to a terminal receipt.
	targetRuntime := f.runtime
	targetRuntime.SourceClusterID = request.TargetClusterID
	targetID := f.id
	targetID.SourceClusterID = request.TargetClusterID
	targetID.WALIdentity = ""
	if _, err := e.change(ctx, targetRuntime.StatefulSetUID, targetRuntime.SourceClusterID, func(ledger *fencing.Ledger) error {
		return ledger.RegisterEnrolled(targetID, targetRuntime)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.reserveMutationProducer(ctx, targetRuntime); err != nil {
		t.Fatal(err)
	}
	targetStages, err := e.sourceStagesFor(ctx, targetRuntime)
	if err != nil {
		t.Fatal(err)
	}
	targetStoreScope, err := e.sourceMutationStorageScope(ctx, targetRuntime)
	if err != nil {
		t.Fatal(err)
	}
	targetStoreScopeHash := hashJSON(targetStoreScope)
	targetID.WALIdentity = "wal-empty-target"
	if _, err := e.change(ctx, targetRuntime.StatefulSetUID, targetRuntime.SourceClusterID, func(ledger *fencing.Ledger) error {
		if err := ledger.BindMutationScope(targetID, targetStoreScopeHash, fencing.MutationExecutionProfile); err != nil {
			return err
		}
		if err := ledger.BindMutationStage(targetID, targetStages.incarnation); err != nil {
			return err
		}
		return ledger.Bind(targetID)
	}); err != nil {
		t.Fatal(err)
	}
	targetRun, err := e.enrolledMutationRun(ctx, targetRuntime)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.changeMutationProducer(ctx, targetRuntime, func(producer *fencing.MutationProducer) error { return producer.Activate(targetID) }); err != nil {
		t.Fatal(err)
	}
	targetPayload, err := recovery.EmptyArchiveHeadBytes()
	if err != nil {
		t.Fatal(err)
	}
	targetDraft := stageStoreIntent()
	targetDraft.StoreScopeHash = targetStoreScopeHash
	targetDraft.Key = "archive/head.bin"
	ticket := strings.Repeat("8", 64)
	if err := targetStages.Reserve(targetRun.MutationProducerUID, ticket, targetDraft); err != nil {
		t.Fatal(err)
	}
	targetSealed, err := targetStages.Seal(targetRun.MutationProducerUID, ticket, hashBytes(targetPayload), int64(len(targetPayload)), bytes.NewReader(targetPayload))
	if err != nil {
		t.Fatal(err)
	}
	targetIntent := targetDraft
	targetIntent.OperationID = fencing.MutationRequestID(targetRun.MutationProducerUID, 1)
	targetIntent.PayloadRef, targetIntent.PayloadDigest, targetIntent.PayloadSize = targetSealed.Ref, targetSealed.Digest, targetSealed.Size
	commitArchiveGenesisMutation(t, e, targetID, targetRuntime, targetRun.MutationProducerUID, targetIntent, targetPayload, targetStoreScope)

	// Registration now resolves the persisted authorization and committed
	// receipt without treating the successor as the retired source or requiring
	// its former target HEAD bytes after the current HEAD has evolved.
	committed, err := e.RegisterArchiveEmptyRecovery(ctx, request)
	if err != nil || committed.State != fencing.EmptyRecoveryCommitted {
		t.Fatalf("requery committed empty genesis after target activation: response=%+v err=%v", committed, err)
	}
	manager := recovery.NewManager(e.ArchiveBucket, targetStoreScope.Prefix, recovery.ArchiveConfigID)
	if err := manager.Load(ctx); err != nil {
		t.Fatal(err)
	}
	prefixHash := sha256.Sum256([]byte("successor-prefix"))
	anchorHash := sha256.Sum256([]byte("successor-anchor"))
	if err := manager.InitializeFencedGeneration(ctx, 1, prefixHash, anchorHash); err != nil {
		manager.Close()
		t.Fatal(err)
	}
	manager.Close()
	committed, err = e.RegisterArchiveEmptyRecovery(ctx, request)
	if err != nil || committed.State != fencing.EmptyRecoveryCommitted {
		t.Fatalf("requery committed authorization after evolved target HEAD: response=%+v err=%v", committed, err)
	}
}

func TestArchiveGenesisAdmissionOnlyAllowsExactCommittedTuple(t *testing.T) {
	intent := fencing.MutationIntent{
		Kind: "upload", Key: "archive/head.bin",
		Condition:   fencing.MutationCondition{Mode: fencing.MutationCreateOnly},
		OperationID: fencing.MutationRequestID("producer", 7),
	}
	ref := &fencing.ArchiveGenesisRef{Mutation: &fencing.ArchiveGenesisMutation{
		ProducerUID: "producer", Sequence: 7, Intent: intent,
	}}
	if err := validateArchiveGenesisAdmission(ref, "producer", 7, intent); err != nil {
		t.Fatalf("exact genesis retry was rejected: %v", err)
	}
	for _, tc := range []struct {
		name     string
		producer string
		sequence uint64
		intent   fencing.MutationIntent
	}{
		{name: "different producer", producer: "replacement", sequence: 7, intent: intent},
		{name: "different sequence", producer: "producer", sequence: 8, intent: intent},
		{name: "different payload tuple", producer: "producer", sequence: 7, intent: func() fencing.MutationIntent {
			changed := intent
			changed.PayloadDigest = "changed"
			return changed
		}()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateArchiveGenesisAdmission(ref, tc.producer, tc.sequence, tc.intent); err == nil {
				t.Fatal("different head create-only tuple was admitted after genesis")
			}
		})
	}
	ordinary := intent
	ordinary.Condition = fencing.MutationCondition{Mode: fencing.MutationMatchGeneration, Generation: 4}
	if err := validateArchiveGenesisAdmission(ref, "producer", 8, ordinary); err != nil {
		t.Fatalf("ordinary archive-head generation update was incorrectly blocked: %v", err)
	}
}

type archiveGenesisFixture struct {
	e          *FencingExecutor
	f          *mutationStoreFixture
	id         fencing.Identity
	runtime    fencing.RuntimeBinding
	producer   string
	stages     *SourceStageStore
	intent     fencing.MutationIntent
	payload    []byte
	storeScope AttestationStoreScope
}

func newArchiveGenesisFixture(t *testing.T) archiveGenesisFixture {
	t.Helper()
	e, f, id, runtime := newMutationStoreFixture(t)
	e.RequireMutationJournal = true
	if _, err := e.change(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID, func(l *fencing.Ledger) error {
		l.ArchiveGenesisState = fencing.ArchiveGenesisRegistrationPending
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	e.ArchiveBucket = objstore.NewInMemBucket()
	archiveBucket := e.ArchiveBucket
	t.Cleanup(func() { _ = archiveBucket.Close() })
	if _, err := e.reserveMutationProducer(t.Context(), runtime); err != nil {
		t.Fatal(err)
	}
	storeScope := AttestationStoreScope{Provider: "gcs", Bucket: "archive", Prefix: "root/source"}
	storeScopeHash := hashJSON(storeScope)
	stages, err := e.sourceStagesFor(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	id.WALIdentity = "wal-archive-genesis"
	if _, err := e.change(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID, func(l *fencing.Ledger) error {
		if err := l.BindMutationScope(id, storeScopeHash, fencing.MutationExecutionProfile); err != nil {
			return err
		}
		if err := l.BindMutationStage(id, stages.incarnation); err != nil {
			return err
		}
		return l.Bind(id)
	}); err != nil {
		t.Fatal(err)
	}
	_, err = e.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error { return p.Activate(id) })
	if err != nil {
		t.Fatal(err)
	}
	run, err := e.enrolledMutationRun(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	env := []any{
		object{"name": "RHIZA_CLUSTER_ID", "value": id.SourceClusterID},
		object{"name": "RHIZA_OBJSTORE_PROVIDER", "value": "gcs"},
		object{"name": "RHIZA_OBJSTORE_BUCKET", "value": "archive"},
		object{"name": "RHIZA_OBJSTORE_PREFIX", "value": "root"},
	}
	pod := supervisedTestPod()
	asObject(pod["metadata"])["name"] = id.NodeID
	asObject(pod["metadata"])["uid"] = id.PodUID
	asObject(pod["metadata"])["ownerReferences"] = []any{object{"kind": "StatefulSet", "name": "rhiza", "uid": runtime.StatefulSetUID, "controller": true}}
	asObject(list(nested(pod, "spec", "containers"))[0])["env"] = env
	sts := object{"metadata": object{"uid": runtime.StatefulSetUID}, "spec": object{"template": object{"spec": object{"containers": []any{object{"name": "rhiza", "env": env}}}}}}
	f.mu.Lock()
	f.authPod, f.authStatefulSet = pod, sts
	f.objects["/apis/apps/v1/namespaces/test/statefulsets"] = object{"items": []any{sts}}
	f.mu.Unlock()
	if err := e.claimArchiveScopeForRegistration(t.Context(), runtime.StatefulSetUID, id, runtime); err != nil {
		t.Fatal(err)
	}
	payload, err := recovery.EmptyArchiveHeadBytes()
	if err != nil {
		t.Fatal(err)
	}
	draft := stageStoreIntent()
	draft.StoreScopeHash = storeScopeHash
	draft.Key = "archive/head.bin"
	ticket := strings.Repeat("7", 64)
	if err := stages.Reserve(run.MutationProducerUID, ticket, draft); err != nil {
		t.Fatal(err)
	}
	sealed, err := stages.Seal(run.MutationProducerUID, ticket, hashBytes(payload), int64(len(payload)), bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	intent := draft
	intent.OperationID = fencing.MutationRequestID(run.MutationProducerUID, 1)
	intent.PayloadRef, intent.PayloadDigest, intent.PayloadSize = sealed.Ref, sealed.Digest, sealed.Size
	return archiveGenesisFixture{e: e, f: f, id: id, runtime: runtime, producer: run.MutationProducerUID, stages: stages, intent: intent, payload: payload, storeScope: storeScope}
}

func (f archiveGenesisFixture) admit(t *testing.T, sequence uint64, intent fencing.MutationIntent) {
	t.Helper()
	_, err := f.e.changeMutationProducer(t.Context(), f.runtime, func(p *fencing.MutationProducer) error { return p.Admit(sequence, intent) })
	if err != nil {
		t.Fatal(err)
	}
}

func (f archiveGenesisFixture) setStorage(t *testing.T, endpoint string, client *http.Client) {
	t.Helper()
	storageClient, err := storage.NewClient(t.Context(), option.WithEndpoint(endpoint+"/storage/v1/"), option.WithHTTPClient(client), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = storageClient.Close() })
	f.e.SourceMutationStorage = storageClient
}

func commitArchiveGenesisFixture(t *testing.T, f archiveGenesisFixture) {
	t.Helper()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodPost || r.URL.Query().Get("ifGenerationMatch") != "0" || r.URL.Query().Get("name") != "root/source/archive/head.bin" {
			t.Errorf("unexpected bootstrap GCS request: %s %s", r.Method, r.URL.String())
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		body, err := readGCSMultipartMedia(r)
		if err != nil {
			t.Errorf("read bootstrap GCS media: %v", err)
			http.Error(w, "invalid GCS multipart upload", http.StatusBadRequest)
			return
		}
		if !bytes.Equal(body, f.payload) {
			t.Errorf("bootstrap GCS media differs from the staged empty HEAD bytes")
			http.Error(w, "unexpected GCS media", http.StatusBadRequest)
			return
		}
		if err := f.e.ArchiveBucket.Upload(r.Context(), path.Join(f.storeScope.Prefix, "archive/head.bin"), bytes.NewReader(body), objstore.WithIfNotExists()); err != nil {
			t.Errorf("mirror target HEAD into archive verification bucket: %v", err)
			http.Error(w, "archive mirror failed", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"kind": "storage#object", "bucket": "archive", "name": "root/source/archive/head.bin",
			"generation": "21", "metageneration": "1", "size": strconv.Itoa(len(f.payload)),
			"metadata": map[string]string{"rhiza-operation-id": f.intent.OperationID, "rhiza-payload-sha256": f.intent.PayloadDigest},
		})
	}))
	t.Cleanup(server.Close)
	f.setStorage(t, server.URL, server.Client())
	if err := f.e.executeSourceMutationRequest(t.Context(), "Bearer test", f.id, sourceMutationRequest{
		ProducerUID: f.producer, Sequence: 1, Intent: f.intent,
	}, true); err != nil {
		t.Fatalf("fixture failed to commit its real archive genesis mutation: %v", err)
	}
	root, err := f.e.read(t.Context(), f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
	if err != nil || root.Ledger.ArchiveGenesisState != fencing.ArchiveGenesisCommitted || root.Ledger.ArchiveGenesisRef == nil {
		t.Fatalf("fixture archive genesis did not become committed: %+v err=%v", root.Ledger, err)
	}
}

func readGCSMultipartMedia(r *http.Request) ([]byte, error) {
	mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		return nil, err
	}
	if mediaType != "multipart/related" || params["boundary"] == "" {
		return nil, fmt.Errorf("unexpected GCS upload content type %q", mediaType)
	}
	reader := multipart.NewReader(r.Body, params["boundary"])
	var parts [][]byte
	for {
		part, err := reader.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		body, err := io.ReadAll(part)
		closeErr := part.Close()
		if err := errors.Join(err, closeErr); err != nil {
			return nil, err
		}
		parts = append(parts, body)
	}
	if len(parts) != 2 {
		return nil, fmt.Errorf("GCS multipart upload has %d parts, want metadata and media", len(parts))
	}
	return parts[1], nil
}

func commitArchiveGenesisMutation(t *testing.T, e *FencingExecutor, id fencing.Identity, runtime fencing.RuntimeBinding, producer string, intent fencing.MutationIntent, payload []byte, scope AttestationStoreScope) {
	t.Helper()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodPost || r.URL.Query().Get("ifGenerationMatch") != "0" || r.URL.Query().Get("name") != path.Join(scope.Prefix, intent.Key) {
			t.Errorf("unexpected staged genesis request: %s %s", r.Method, r.URL.String())
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		body, err := readGCSMultipartMedia(r)
		if err != nil || !bytes.Equal(body, payload) {
			t.Errorf("genesis upload bytes differ from staged payload: %v", err)
			http.Error(w, "payload differs", http.StatusBadRequest)
			return
		}
		if err := e.ArchiveBucket.Upload(r.Context(), path.Join(scope.Prefix, intent.Key), bytes.NewReader(body), objstore.WithIfNotExists()); err != nil {
			t.Errorf("mirror target HEAD into archive verification bucket: %v", err)
			http.Error(w, "archive mirror failed", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"kind": "storage#object", "bucket": scope.Bucket, "name": path.Join(scope.Prefix, intent.Key),
			"generation": "22", "metageneration": "1", "size": strconv.Itoa(len(payload)),
			"metadata": map[string]string{"rhiza-operation-id": intent.OperationID, "rhiza-payload-sha256": intent.PayloadDigest},
		})
	}))
	t.Cleanup(server.Close)
	storageClient, err := storage.NewClient(t.Context(), option.WithEndpoint(server.URL+"/storage/v1/"), option.WithHTTPClient(server.Client()), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = storageClient.Close() })
	e.SourceMutationStorage = storageClient
	if err := e.executeSourceMutationRequest(t.Context(), "Bearer test", id, sourceMutationRequest{
		ProducerUID: producer, Sequence: 1, Intent: intent,
	}, true); err != nil {
		t.Fatalf("commit staged empty archive genesis: %v", err)
	}
	root, err := e.read(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID)
	if err != nil || root.Ledger.ArchiveGenesisState != fencing.ArchiveGenesisCommitted || root.Ledger.ArchiveGenesisRef == nil {
		t.Fatalf("target archive genesis did not become committed: %+v err=%v", root.Ledger, err)
	}
}

func postArchiveGenesisStatus(t *testing.T, e *FencingExecutor, id fencing.Identity) (fencing.ArchiveGenesisStatus, int) {
	t.Helper()
	body, err := json.Marshal(struct {
		Identity fencing.Identity                    `json:"identity"`
		Genesis  fencing.ArchiveGenesisStatusRequest `json:"archiveGenesis"`
	}{Identity: id, Genesis: fencing.ArchiveGenesisStatusRequest{ArchiveKey: "archive/head.bin", ConfigID: recovery.ArchiveConfigID}})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/source-mutations/genesis-status", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer test")
	recorder := httptest.NewRecorder()
	e.handleSourceMutation(recorder, req)
	var status fencing.ArchiveGenesisStatus
	if recorder.Code == http.StatusOK {
		if err := json.Unmarshal(recorder.Body.Bytes(), &status); err != nil {
			t.Fatal(err)
		}
	}
	return status, recorder.Code
}

func TestArchiveGenesisStatusResumesAdmittedTupleAndCommitsRefsBeforeDispatch(t *testing.T) {
	var executor *FencingExecutor
	var runtime fencing.RuntimeBinding
	var uploads atomic.Int32
	payload := recoveryEmptyHeadBytes(t)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodPost || r.URL.Query().Get("ifGenerationMatch") != "0" || r.URL.Query().Get("name") != "root/source/archive/head.bin" {
			t.Errorf("unexpected genesis upload request: %s %s", r.Method, r.URL.String())
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		if executor == nil {
			t.Error("genesis request raced fixture setup")
			return
		}
		scope, _, err := executor.archiveGenesisScope(r.Context(), runtime)
		if err != nil {
			t.Errorf("derive scope before provider dispatch: %v", err)
		}
		claim, _, err := executor.AuthorityState.ReadArchiveScopeClaim(r.Context(), scope)
		if err != nil || claim.GenesisRef == nil || claim.GenesisRef.Mutation == nil {
			t.Errorf("provider dispatch occurred before durable scope claim: %+v %v", claim, err)
			http.Error(w, "missing genesis claim", http.StatusInternalServerError)
			return
		}
		root, err := executor.read(r.Context(), claim.OwnerStatefulSetUID, claim.OwnerSourceClusterID)
		if err != nil || root.Ledger.ArchiveGenesisRef == nil || !sameArchiveGenesisRef(claim.GenesisRef, root.Ledger.ArchiveGenesisRef) {
			t.Errorf("provider dispatch occurred before matching root reference: %+v %v", root.Ledger.ArchiveGenesisRef, err)
		}
		producer, _, _, err := executor.AuthorityState.ReadProducer(r.Context(), runtime)
		if err != nil || producer.Entries[claim.GenesisRef.Mutation.Sequence].Intent != claim.GenesisRef.Mutation.Intent {
			t.Errorf("provider dispatch occurred without the exact admitted tuple: %+v %v", producer, err)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Contains(body, payload) {
			t.Errorf("GCS body did not contain exact empty HEAD: %v", err)
		}
		uploads.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"kind": "storage#object", "bucket": "archive", "name": "root/source/archive/head.bin",
			"generation": "21", "metageneration": "1", "size": strconv.Itoa(len(payload)),
			"metadata": map[string]string{"rhiza-operation-id": claim.GenesisRef.Mutation.Intent.OperationID, "rhiza-payload-sha256": claim.GenesisRef.Mutation.Intent.PayloadDigest},
		})
	}))
	t.Cleanup(server.Close)
	f := newArchiveGenesisFixture(t)
	executor = f.e
	runtime = f.runtime
	f.setStorage(t, server.URL, server.Client())
	f.admit(t, 1, f.intent)
	if _, err := f.e.changeMutationProducer(t.Context(), f.runtime, func(p *fencing.MutationProducer) error {
		return p.Close("test-closed-before-genesis-ref", strings.Repeat("c", 64))
	}); err != nil {
		t.Fatal(err)
	}
	status, code := postArchiveGenesisStatus(t, f.e, f.id)
	if code != http.StatusOK || status.Ref == nil || status.Mutation == nil || status.Mutation.Entry == nil || status.Mutation.Entry.Resolution == nil || status.Mutation.Entry.Resolution.Outcome != fencing.MutationCommitted {
		t.Fatalf("status did not resume and verify the admitted genesis tuple: code=%d status=%+v", code, status)
	}
	if uploads.Load() != 1 {
		t.Fatalf("status issued %d provider writes, want exactly one", uploads.Load())
	}
}

func TestArchiveGenesisLosingAdmittedTupleIsDurablyCancelledBeforeDispatch(t *testing.T) {
	var executor *FencingExecutor
	var runtime fencing.RuntimeBinding
	var first fencing.MutationIntent
	payload := recoveryEmptyHeadBytes(t)
	var uploads atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodPost || r.URL.Query().Get("ifGenerationMatch") != "0" || r.URL.Query().Get("name") != "root/source/archive/head.bin" {
			t.Errorf("unexpected winner upload request: %s %s", r.Method, r.URL.String())
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		if executor == nil {
			t.Error("genesis request raced fixture setup")
			return
		}
		scope, _, err := executor.archiveGenesisScope(r.Context(), runtime)
		claim, _, readErr := executor.AuthorityState.ReadArchiveScopeClaim(r.Context(), scope)
		if err != nil || readErr != nil || claim.GenesisRef == nil || claim.GenesisRef.Mutation == nil || claim.GenesisRef.Mutation.Intent != first {
			t.Errorf("provider dispatched before exact winning ref was durable: %+v %v %v", claim, err, readErr)
			http.Error(w, "missing genesis claim", http.StatusInternalServerError)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Contains(body, payload) {
			t.Errorf("winner upload omitted sealed empty HEAD: %v", err)
		}
		uploads.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"kind": "storage#object", "bucket": "archive", "name": "root/source/archive/head.bin",
			"generation": "21", "metageneration": "1", "size": strconv.Itoa(len(payload)),
			"metadata": map[string]string{"rhiza-operation-id": first.OperationID, "rhiza-payload-sha256": first.PayloadDigest},
		})
	}))
	t.Cleanup(server.Close)
	f := newArchiveGenesisFixture(t)
	executor = f.e
	runtime = f.runtime
	f.setStorage(t, server.URL, server.Client())
	first = f.intent
	if err := f.e.executeSourceMutationRequest(t.Context(), "Bearer test", f.id, sourceMutationRequest{ProducerUID: f.producer, Sequence: 1, Intent: first}, true); err != nil {
		t.Fatalf("normal admission did not commit and execute its exact genesis tuple: %v", err)
	}
	if uploads.Load() != 1 {
		t.Fatalf("winning candidate issued %d uploads, want one", uploads.Load())
	}
	second := f.intent
	second.OperationID = fencing.MutationRequestID(f.producer, 2)
	second.OwnerStep.Step = "genesis-candidate-two"
	ticket := strings.Repeat("8", 64)
	draft := second
	draft.OperationID, draft.PayloadRef, draft.PayloadDigest, draft.PayloadSize = "", "", "", 0
	if err := f.stages.Reserve(f.producer, ticket, draft); err != nil {
		t.Fatal(err)
	}
	sealed, err := f.stages.Seal(f.producer, ticket, hashBytes(f.payload), int64(len(f.payload)), bytes.NewReader(f.payload))
	if err != nil {
		t.Fatal(err)
	}
	second.PayloadRef, second.PayloadDigest, second.PayloadSize = sealed.Ref, sealed.Digest, sealed.Size
	f.admit(t, 2, second)
	err = f.e.executeSourceMutationRequest(t.Context(), "Bearer test", f.id, sourceMutationRequest{ProducerUID: f.producer, Sequence: 2, Intent: second}, true)
	if !errors.Is(err, errArchiveGenesisClaimed) {
		t.Fatalf("losing admitted tuple was not rejected: %v", err)
	}
	producer, _, _, err := f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	entry := producer.Entries[2]
	if entry.Intent != second || entry.Preparation != nil || len(entry.Dispatches) != 0 || entry.Resolution == nil || entry.Resolution.Outcome != fencing.MutationRejected || !entry.Resolution.AllCopiesTerminal || !entry.DispatchClosed {
		t.Fatalf("losing candidate was not terminally excluded without dispatch: %+v", entry)
	}
	if winner := producer.Entries[1]; winner.Resolution == nil || winner.Resolution.Outcome != fencing.MutationCommitted || len(winner.Dispatches) != 1 {
		t.Fatalf("claim winner did not retain its sole committed dispatch: %+v", winner)
	}
	proofKey := "archive-genesis-cancellations/" + entry.Resolution.ReceiptDigest
	evidence, err := f.e.authorityEvidenceFor(f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	reader, _, err := evidence.Read(t.Context(), proofKey)
	if err != nil {
		t.Fatalf("losing candidate cancellation receipt is not durable: %v", err)
	}
	body, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		t.Fatal(err)
	}
	var proof archiveGenesisCancellationProof
	if err := json.Unmarshal(body, &proof); err != nil || proof.ProducerUID != f.producer || proof.Sequence != 2 || proof.Intent != second || proof.WinningGenesis.Mutation == nil || proof.WinningGenesis.Mutation.Sequence != 1 {
		t.Fatalf("cancellation receipt does not bind exact loser and winner: %+v %v", proof, err)
	}
}

func recoveryEmptyHeadBytes(t *testing.T) []byte {
	t.Helper()
	data, err := recovery.EmptyArchiveHeadBytes()
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestArchiveGenesisStatusDoesNotAdmitMissingOrMismatchedReferencedTuple(t *testing.T) {
	for _, tc := range []struct {
		name     string
		sequence uint64
	}{
		{name: "missing tuple", sequence: 1},
		{name: "mismatched tuple", sequence: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) }))
			t.Cleanup(server.Close)
			f := newArchiveGenesisFixture(t)
			f.setStorage(t, server.URL, server.Client())
			scope, scopeHash, err := f.e.archiveGenesisScope(t.Context(), f.runtime)
			if err != nil {
				t.Fatal(err)
			}
			intent := f.intent
			intent.OperationID = fencing.MutationRequestID(f.producer, tc.sequence)
			ref := fencing.ArchiveGenesisRef{Version: fencing.ArchiveGenesisVersion, ConfigID: recovery.ArchiveConfigID, ScopeHash: scopeHash,
				Mutation: &fencing.ArchiveGenesisMutation{ProducerUID: f.producer, Sequence: tc.sequence, Intent: intent}}
			if err := f.e.commitArchiveGenesisRef(t.Context(), f.runtime.StatefulSetUID, f.runtime.SourceClusterID, scope, ref); err != nil {
				t.Fatal(err)
			}
			_, code := postArchiveGenesisStatus(t, f.e, f.id)
			if code != http.StatusAccepted {
				t.Fatalf("unverified journal tuple was not kept pending: status=%d", code)
			}
			producer, _, _, err := f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
			if err != nil || producer.LastAdmitted != 0 || len(producer.Entries) != 0 {
				t.Fatalf("status synthesized an admission for the referenced tuple: last=%d entries=%d err=%v", producer.LastAdmitted, len(producer.Entries), err)
			}
		})
	}
}

func TestArchiveGenesisStatusDoesNotCallHeadlessForkFreshEmpty(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.Contains(r.URL.EscapedPath(), "fork%2Fintent.json") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"kind":"storage#object","bucket":"archive","name":"root/source/fork/intent.json","generation":"7","metageneration":"1","size":"1"}`)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(server.Close)
	f := newArchiveGenesisFixture(t)
	if err := f.e.ArchiveBucket.Upload(t.Context(), "root/source/fork/intent.json", strings.NewReader("{}")); err != nil {
		t.Fatal(err)
	}
	f.setStorage(t, server.URL, server.Client())
	_, code := postArchiveGenesisStatus(t, f.e, f.id)
	if code != http.StatusConflict {
		t.Fatalf("headless fork was advertised as a fresh empty archive: status=%d", code)
	}
}
