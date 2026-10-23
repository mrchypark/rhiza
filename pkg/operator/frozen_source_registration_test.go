package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

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
	sourcePrefix                 string
	writes                       []string
	failGetName                  string
	failGetCount                 int
	failSourceGetAfterForkIntent bool
	forkIntentName               string
	forkIntentWritten            bool
	failSourceGetInjected        bool
}

func TestManualTerminalFrozenForkRegistersWithoutIndependentV2(t *testing.T) {
	for _, tc := range []struct {
		name       string
		durability string
		allowLoss  bool
	}{
		{name: "async requires explicit data-loss consent", durability: "async", allowLoss: true},
		{name: "before-ack needs no data-loss claim", durability: "before-ack", allowLoss: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runManualTerminalFrozenForkRegistration(t, tc.durability, tc.allowLoss)
		})
	}
}

func runManualTerminalFrozenForkRegistration(t *testing.T, durability string, allowLoss bool) {
	f := newArchiveGenesisFixture(t)
	ctx := t.Context()
	now := time.Date(2026, 10, 24, 0, 0, 0, 0, time.UTC)
	f.e.manualRecoveryNow = func() time.Time { return now }
	const sourceID, sourcePrefix, fenceOperation = "source", "root/source", "manual-terminal-fence"
	sourceMembers := []quepaxa.Member{forkTestMember(t, f.id.NodeID), forkTestMember(t, "manual-terminal-voter-1"), forkTestMember(t, "manual-terminal-voter-2")}
	targets := configureManualFrozenForkCohort(t, f, sourceMembers, durability)
	commitArchiveGenesisFixture(t, f)
	if err := f.e.ArchiveBucket.Upload(ctx, path.Join(sourcePrefix, "voters/membership.json"), bytes.NewReader(jsonBytes(recovery.NewMembershipRecord(sourceID, sourceMembers, durability))), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}

	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	core, err := quepaxa.New(quepaxa.Config{NodeID: sourceMembers[0].ID,
		Cluster: quepaxa.Cluster{ConfigID: recovery.ArchiveConfigID, Members: sourceMembers}, WAL: wal, Transport: testTransport{}})
	if err != nil {
		t.Fatal(err)
	}
	decision, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "manual-terminal-source-data", SQL: "CREATE TABLE manual_terminal (value TEXT)"}})
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
		return []byte("manual-terminal-stop"), nil
	}
	f.e.VerifyRuntimeEvidence = func(_ context.Context, _ fencing.RuntimeBinding, _ fencing.StopRequest, receipt []byte) error {
		if string(receipt) != "manual-terminal-stop" {
			return errors.New("unexpected test runtime evidence")
		}
		return nil
	}
	fenceRequest := FenceRequest{LogicalID: "manual-terminal", BindingUID: f.runtime.BindingUID, Namespace: "test", StatefulSet: "rhiza",
		StatefulSetUID: f.runtime.StatefulSetUID, SourceClusterID: sourceID, OperationID: fenceOperation, Scope: ScopeGeneration, Targets: targets}
	fenceName := "fence-" + hashJSON([]string{f.runtime.BindingUID, fenceOperation})[:40]
	fence := &fenceResource{APIVersion: "rhiza.mrchypark.dev/v1alpha1", Kind: "RhizaFence",
		Metadata: object{"name": fenceName, "namespace": "test", "uid": "manual-terminal-fence-uid", "resourceVersion": "1"}, Spec: fenceRequest}
	fenceCollection := "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizafences"
	if err := f.e.reconcileFence(ctx, fenceCollection, fence); err != nil {
		t.Fatalf("close source CONTROL generation: %v", err)
	}
	if fence.Status.Proof == nil || fence.Status.Proof.IndependentV2 == nil {
		t.Fatal("fixture did not create the independent proof that will be removed for this test")
	}
	// Model an unavailable/pruned terminal receipt while retaining the canonical
	// fence request and the closed CONTROL run/stop history.
	fence.Status.Proof = nil
	fenceData := jsonBytes(fence)
	var fenceObject object
	if err := json.Unmarshal(fenceData, &fenceObject); err != nil {
		t.Fatal(err)
	}
	f.f.mu.Lock()
	f.f.objects[fenceCollection+"/"+fenceName] = fenceObject
	f.f.objects[fenceCollection] = object{"items": []any{fenceObject}}
	f.f.mu.Unlock()

	sourceScope := fencing.ArchiveScope{Provider: "gcs", Bucket: "archive", Prefix: sourcePrefix, ClusterID: sourceID}
	scopeHash, err := sourceScope.Hash()
	if err != nil {
		t.Fatal(err)
	}
	targetID, err := recovery.FrozenTargetID(recovery.FrozenSourceIdentity{ScopeHash: scopeHash, ClusterID: sourceID, StatefulSetUID: f.runtime.StatefulSetUID})
	if err != nil {
		t.Fatal(err)
	}
	tracked := &frozenRegistrarTrackingBucket{Bucket: f.e.ArchiveBucket, sourcePrefix: sourcePrefix}
	f.e.ArchiveBucket = tracked
	var attrsReader internalobjstore.BucketAttrsReader = tracked
	environment, err := captureFrozenBucketEnvironment(ctx, attrsReader, "archive", sourcePrefix)
	if err != nil {
		t.Fatal(err)
	}
	recoveryOperation := "manual-recovery-uid:" + fenceOperation
	resourceSpec := Spec{StatefulSet: "rhiza", SourceClusterID: sourceID, RecoveryID: fenceOperation, Durability: durability, AllowDataLoss: allowLoss,
		Fence: Fence{RecoveryID: fenceOperation, ClusterID: sourceID, StatefulSetUID: f.runtime.StatefulSetUID, Confirmed: true,
			Evidence: "manual-terminal-administrator-approval-required"}}
	resource := Resource{APIVersion: "rhiza.mrchypark.dev/v1alpha1", Kind: "RhizaRecovery",
		Metadata: object{"name": "manual-terminal-recovery", "uid": "manual-recovery-uid", "namespace": "test"}, Spec: resourceSpec,
		Status: Status{}}
	persistRecovery := func() {
		data := jsonBytes(resource)
		var encoded object
		if err := json.Unmarshal(data, &encoded); err != nil {
			t.Fatal(err)
		}
		f.f.mu.Lock()
		f.f.objects[f.e.controller().resourcePath(resourceName(&resource))] = encoded
		f.f.mu.Unlock()
	}
	persistRecovery()

	server := httptest.NewTLSServer(f.e.Handler())
	t.Cleanup(server.Close)
	client := &FencingClient{URL: server.URL, TokenFile: f.e.Kube.TokenFile, Client: server.Client()}
	sts, err := f.e.archiveForkStatefulSet(ctx, f.runtime.StatefulSetUID)
	if err != nil {
		t.Fatal(err)
	}
	ctr, err := container(sts, resource.Spec.Container)
	if err != nil {
		t.Fatal(err)
	}
	env, err := f.e.controller().environment(ctx, ctr)
	if err != nil {
		t.Fatal(err)
	}
	controller := &Controller{Fencer: client, Kube: f.e.Kube, Bucket: tracked, Prefix: "root", StoreIdentity: StoreIdentity(env)}
	for step := 0; step < 5 && resource.Status.ManualRecoveryChallenge == nil; step++ {
		if err := controller.reconcile(ctx, &resource); err != nil {
			t.Fatalf("controller manual preflight step %d: %v", step, err)
		}
		var persisted Resource
		if err := f.e.Kube.Get(ctx, f.e.controller().resourcePath(resourceName(&resource)), &persisted); err != nil {
			t.Fatalf("read persisted manual preflight state after step %d: %v", step, err)
		}
		resource = persisted
	}
	if resource.Status.ManualRecoveryChallenge == nil || resource.Status.ManualRecoveryChallengeDigest == "" ||
		resource.Status.Target != "" || resource.Status.Stage != "" || resource.Status.FrozenBucketEnvironment == nil {
		t.Fatalf("controller must persist a challenge and lifecycle snapshot without claiming/creating the target: status=%+v", resource.Status)
	}
	assertManualTerminalSourceWorkload(t, ctx, f.e, resource)
	challenge := *resource.Status.ManualRecoveryChallenge
	if challenge.TargetClusterID != targetID || challenge.TargetBaseKind != "fork" {
		t.Fatalf("challenge did not bind the deterministic proposed target: challenge=%+v expected=%q", challenge, targetID)
	}
	if exists, err := tracked.Exists(ctx, path.Join("root", targetID, "voters/membership.json")); err != nil || exists {
		t.Fatalf("target resources appeared before independent approval: exists=%t err=%v", exists, err)
	}
	if _, err := f.e.AuthorityState.readSourceSuccessorClaim(ctx, sourceScope, f.runtime.StatefulSetUID); !errors.Is(err, errAuthorityStateNotFound) {
		t.Fatalf("target successor claim must be absent before independent approval: %v", err)
	}
	preflightRequest := FrozenSourcePreflightRequest{RecoveryName: resourceName(&resource), RecoveryUID: str(resource.Metadata["uid"]),
		ManualRecoveryChallengeDigest: resource.Status.ManualRecoveryChallengeDigest}
	attestationName, err := ManualRecoveryAttestationName(challenge.RecoveryUID)
	if err != nil {
		t.Fatal(err)
	}
	approval := ManualRecoveryAttestationObject{
		Namespace: challenge.RecoveryNamespace, Name: attestationName, UID: "manual-approval-object-uid", CreatedAt: challenge.IssuedAt,
		Spec: ManualRecoveryAttestationSpec{Version: manualAttestationVersion, ChallengeDigest: resource.Status.ManualRecoveryChallengeDigest,
			ManualRecoveryBinding: challenge.ManualRecoveryBinding,
			Assertions: ManualRecoveryAssertions{AllHistoricalRunsTerminated: true, RecreationBlocked: true,
				OldPeerAndClientWriteAccessBlocked: true, OldArchiveWriterAccessRevoked: true, StorageQuiesced: true},
			Attester: "incident-review", EvidenceRefs: []string{"runbook://manual-terminal"}},
	}
	f.f.mu.Lock()
	f.f.objects[manualRecoveryAttestationPath(approval.Namespace, approval.Name)] = manualAttestationAPIObject(approval)
	f.f.mu.Unlock()

	for step := 0; step < 5 && resource.Status.Target == ""; step++ {
		if err := controller.reconcile(ctx, &resource); err != nil {
			t.Fatalf("controller approved manual preflight step %d: %v", step, err)
		}
		var persisted Resource
		if err := f.e.Kube.Get(ctx, f.e.controller().resourcePath(resourceName(&resource)), &persisted); err != nil {
			t.Fatalf("read persisted approved manual preflight state after step %d: %v", step, err)
		}
		resource = persisted
	}
	if resource.Status.Target != targetID || resource.Status.ManualRecoveryChallengeDigest == "" || resource.Status.ManualAttestationDigest == "" || resource.Status.Stage != "FrozenForked" {
		t.Fatalf("controller did not claim exactly the approved canonical proposal: status=%+v", resource.Status)
	}
	approved, err := f.e.frozenSourcePreflight(ctx, preflightRequest)
	if err != nil || !approved.ClosureReady || approved.EvidenceKind != ManualTerminalEvidenceKind || approved.ProofDigest != "" ||
		approved.ManualRecoveryChallengeDigest != resource.Status.ManualRecoveryChallengeDigest || !validEvidenceDigest(approved.ManualTerminalAttestationDigest) ||
		approved.ManualTerminalAttestationDigest != resource.Status.ManualAttestationDigest {
		t.Fatalf("manual terminal approval response=%+v err=%v", approved, err)
	}
	acceptedRecord, err := f.e.readManualRecoveryApproval(ctx, challenge.RecoveryUID)
	if err != nil || acceptedRecord.AcceptedAt.IsZero() || acceptedRecord.AcceptedAt.Before(challenge.IssuedAt) || !acceptedRecord.AcceptedAt.Before(challenge.ExpiresAt) {
		t.Fatalf("independent approval was not durably accepted before materialization: record=%+v err=%v", acceptedRecord, err)
	}
	persistRecovery()

	targetMembers := []quepaxa.Member{forkTestMember(t, string(sourceMembers[0].ID)), forkTestMember(t, string(sourceMembers[1].ID)), forkTestMember(t, string(sourceMembers[2].ID))}
	targetMembership := recovery.NewMembershipRecord(targetID, targetMembers, durability)
	options := recovery.FrozenForkOptions{
		ForkOptions: recovery.ForkOptions{SourcePrefix: sourcePrefix, TargetPrefix: path.Join("root", targetID),
			SourceBootstrap: quepaxa.Cluster{ConfigID: recovery.ArchiveConfigID, Members: sourceMembers},
			TargetMembers:   targetMembers, TargetMembership: targetMembership, OperationID: recoveryOperation},
		SourceIdentity: recovery.FrozenSourceIdentity{ScopeHash: scopeHash, ClusterID: sourceID, StatefulSetUID: f.runtime.StatefulSetUID},
		EvidenceKind:   ManualTerminalEvidenceKind, ManualAttestationDigest: approved.ManualTerminalAttestationDigest,
		ClosedRootDigest: approved.ClosedRootDigest, BucketEnvironment: environment,
		VerifyEnvironment: func(ctx context.Context, expected recovery.FrozenBucketEnvironment) error {
			return f.e.verifyFrozenBucketEnvironment(ctx, "archive", sourcePrefix, expected)
		},
	}
	tracked.forkIntentName = path.Join("root", targetID, "fork/intent.json")
	tracked.failSourceGetAfterForkIntent = true
	fork, err := recovery.ForkFrozenSource(ctx, tracked, options)
	if err == nil {
		t.Fatal("injected pre-claim frozen-source materialization failure was not surfaced")
	}
	if exists, err := tracked.Exists(ctx, path.Join("root", targetID, "fork/intent.json")); err != nil || !exists {
		t.Fatalf("failed materialization did not retain its exact target intent: exists=%t err=%v", exists, err)
	}
	if _, err := f.e.AuthorityState.readSourceSuccessorClaim(ctx, sourceScope, f.runtime.StatefulSetUID); !errors.Is(err, errAuthorityStateNotFound) {
		t.Fatalf("pre-claim materialization failure unexpectedly consumed the source successor claim: %v", err)
	}
	now = challenge.ExpiresAt.Add(time.Second)
	approvedAfterExpiry, err := f.e.frozenSourcePreflight(ctx, preflightRequest)
	if err != nil || !approvedAfterExpiry.ClosureReady || approvedAfterExpiry.ManualTerminalAttestationDigest != approved.ManualTerminalAttestationDigest {
		t.Fatalf("exact accepted approval did not resume after expiry before source claim: response=%+v err=%v", approvedAfterExpiry, err)
	}
	fork, err = recovery.ForkFrozenSource(ctx, tracked, options)
	if err != nil {
		t.Fatalf("manual frozen-source fork retry failed after approval expiry: %v", err)
	}
	if err := tracked.Upload(ctx, path.Join("root", targetID, "voters/membership.json"), bytes.NewReader(jsonBytes(targetMembership)), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	lineage := archiveForkLineageRecord{Version: 1, OperationID: recoveryOperation, Source: sourceID, SourceDurability: durability,
		SourceMembership: sourceFingerprint(sourceID, durability, sourceMembers, sourcePrefix), TargetMembership: targetMembership, Fork: fork.ForkResult}
	if err := tracked.Upload(ctx, path.Join("root", targetID, "recovery/generation.json"), bytes.NewReader(jsonBytes(lineage)), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	request := ArchiveForkGenesisRequest{RecoveryName: resourceName(&resource), StatefulSetUID: f.runtime.StatefulSetUID,
		TargetClusterID: targetID, OperationID: recoveryOperation, SourceFenceOperationID: fenceOperation,
		RecoveryMode: RecoveryModeFrozenSource, BucketEnvironment: &environment, EvidenceKind: ManualTerminalEvidenceKind,
		ManualRecoveryChallengeDigest: approved.ManualRecoveryChallengeDigest, ManualTerminalAttestationDigest: approved.ManualTerminalAttestationDigest}
	tracked.failGetName = recovery.GenerationAnchorKey(path.Join("root", targetID))
	tracked.failGetCount = 1
	if err := f.e.RegisterArchiveForkGenesis(ctx, request); err == nil || !strings.Contains(err.Error(), "injected post-claim") {
		t.Fatalf("expected an injected artifact failure after the successor claim, got %v", err)
	}
	claim, err := f.e.AuthorityState.readSourceSuccessorClaim(ctx, sourceScope, f.runtime.StatefulSetUID)
	if err != nil || claim.EvidenceKind != ManualTerminalEvidenceKind || claim.ManualRecoveryChallengeDigest != approved.ManualRecoveryChallengeDigest ||
		claim.ManualTerminalAttestationDigest != approved.ManualTerminalAttestationDigest {
		t.Fatalf("post-claim failure did not retain exact manual evidence: claim=%+v err=%v", claim, err)
	}
	closedRoot, _, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, sourceID)
	if err != nil {
		t.Fatal(err)
	}
	changedRoot := closedRoot
	changedRoot.Ledger.ClosedHash += "-changed"
	if matches, err := f.e.exactManualTerminalSuccessorClaim(ctx, request, sourceScope, changedRoot, *fence, "manual-recovery-uid"); err != nil || matches {
		t.Fatalf("post-claim expiry replay matched a changed canonical root: matches=%t err=%v", matches, err)
	}
	wrongApproval := request
	wrongApproval.ManualTerminalAttestationDigest = strings.Repeat("0", 64)
	if err := f.e.RegisterArchiveForkGenesis(ctx, wrongApproval); err == nil {
		t.Fatal("expired manual approval accepted a different attestation digest")
	}
	wrongRecovery := request
	wrongRecovery.OperationID = "different-recovery-uid:" + fenceOperation
	if err := f.e.RegisterArchiveForkGenesis(ctx, wrongRecovery); err == nil {
		t.Fatal("expired manual approval accepted a different Recovery UID")
	}
	resource.Spec.AllowDataLoss = !allowLoss
	persistRecovery()
	if err := f.e.RegisterArchiveForkGenesis(ctx, request); err == nil {
		t.Fatal("expired manual approval accepted a changed Recovery spec")
	}
	resource.Spec.AllowDataLoss = allowLoss
	persistRecovery()
	if err := f.e.RegisterArchiveForkGenesis(ctx, request); err != nil {
		t.Fatalf("exact claimed manual registration did not resume after challenge expiry: %v", err)
	}
	targetRoot, err := f.e.read(ctx, f.runtime.StatefulSetUID, targetID)
	if err != nil || targetRoot.Ledger.ArchiveGenesisState != fencing.ArchiveGenesisCommitted || targetRoot.Ledger.ArchiveGenesisRef == nil || targetRoot.Ledger.ArchiveGenesisRef.Fork == nil ||
		targetRoot.Ledger.ArchiveGenesisRef.Fork.IntentDigest == "" {
		t.Fatalf("manual registration did not commit the verified fork reference: root=%+v err=%v", targetRoot.Ledger, err)
	}
	intentData, err := readBoundedArchiveObject(ctx, tracked, path.Join("root", targetID, "fork/intent.json"), 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	var intent archiveForkIntentRecord
	if err := decodeCanonicalArchiveJSON(intentData, &intent); err != nil || intent.FrozenSource == nil ||
		intent.FrozenSource.EvidenceKind != ManualTerminalEvidenceKind || intent.FrozenSource.FenceProofDigest != "" ||
		intent.FrozenSource.ManualAttestationDigest != approved.ManualTerminalAttestationDigest {
		t.Fatalf("fork intent did not persist the distinct manual terminal binding: intent=%+v err=%v", intent, err)
	}
	claim, err = f.e.AuthorityState.readSourceSuccessorClaim(ctx, sourceScope, f.runtime.StatefulSetUID)
	if err != nil || claim.EvidenceKind != ManualTerminalEvidenceKind || claim.ManualRecoveryChallengeDigest != approved.ManualRecoveryChallengeDigest ||
		claim.ManualTerminalAttestationDigest != approved.ManualTerminalAttestationDigest {
		t.Fatalf("canonical successor claim omitted manual terminal evidence: claim=%+v err=%v", claim, err)
	}
}

func configureManualFrozenForkCohort(t *testing.T, f archiveGenesisFixture, members []quepaxa.Member, durability string) []FenceTarget {
	t.Helper()
	if len(members) < 3 || string(members[0].ID) != f.id.NodeID {
		t.Fatal("manual fork fixture must include the enrolled source runtime and at least two additional voters")
	}
	ctx := t.Context()
	scope := AttestationStoreScope{Provider: "gcs", Bucket: "archive", Prefix: "root/source"}
	f.e.ArchiveStoreIdentity = StoreIdentity(map[string]string{
		"RHIZA_OBJSTORE_PROVIDER": "gcs", "RHIZA_OBJSTORE_BUCKET": "archive", "RHIZA_OBJSTORE_PREFIX": "root",
	})
	env := []any{
		object{"name": "RHIZA_CLUSTER_ID", "value": f.id.SourceClusterID},
		object{"name": "RHIZA_OBJSTORE_PROVIDER", "value": "gcs"},
		object{"name": "RHIZA_OBJSTORE_BUCKET", "value": "archive"},
		object{"name": "RHIZA_OBJSTORE_PREFIX", "value": "root"},
		object{"name": "RHIZA_OBJSTORE_DURABILITY", "value": durability},
		object{"name": "RHIZA_ENABLE_RECONFIGURATION", "value": "true"},
		object{"name": "RHIZA_CLUSTER_MEMBERS", "value": string(jsonBytes(members))},
	}
	sts := f.f.authStatefulSet
	if sts == nil {
		sts = object{"metadata": object{"name": "rhiza", "namespace": "test", "uid": f.runtime.StatefulSetUID},
			"spec": object{"template": object{"spec": object{"containers": []any{object{"name": "rhiza"}}}}}}
	}
	container := asObject(list(nested(sts, "spec", "template", "spec", "containers"))[0])
	container["env"] = env
	container["volumeMounts"] = []any{object{"name": "data", "mountPath": "/data"}}
	var targets []FenceTarget
	pods := make([]any, 0, len(members))
	for i, member := range members {
		runtime := f.runtime
		identity := f.id
		if i > 0 {
			nodeID := string(member.ID)
			runtime.NodeID, runtime.PodUID, runtime.RunID = nodeID, "manual-fork-pod-"+strconv.Itoa(i), "manual-fork-run-"+strconv.Itoa(i)
			runtime.NodeName, runtime.NodeUID = "manual-fork-node-"+strconv.Itoa(i), "manual-fork-node-uid-"+strconv.Itoa(i)
			runtime.MachineID, runtime.BootID = "manual-fork-machine-"+strconv.Itoa(i), "manual-fork-boot-"+strconv.Itoa(i)
			runtime.RuntimeID, runtime.SandboxID = "manual-fork-runtime-"+strconv.Itoa(i), "manual-fork-sandbox-"+strconv.Itoa(i)
			runtime.ContainerID, runtime.ImageID = "containerd://manual-fork-"+strconv.Itoa(i), "sha256:manual-fork-"+strconv.Itoa(i)
			identity = fencing.Identity{SourceClusterID: runtime.SourceClusterID, NodeID: runtime.NodeID, PodUID: runtime.PodUID, RunID: runtime.RunID}
			if _, err := f.e.change(ctx, runtime.StatefulSetUID, runtime.SourceClusterID, func(ledger *fencing.Ledger) error {
				return ledger.RegisterEnrolled(identity, runtime)
			}); err != nil {
				t.Fatalf("register manual fork source runtime %d: %v", i, err)
			}
			if _, err := f.e.reserveMutationProducer(ctx, runtime); err != nil {
				t.Fatalf("reserve manual fork producer %d: %v", i, err)
			}
			stages, err := f.e.sourceStagesFor(ctx, runtime)
			if err != nil {
				t.Fatalf("create manual fork stage store %d: %v", i, err)
			}
			identity.WALIdentity = "manual-fork-wal-" + strconv.Itoa(i)
			if _, err := f.e.change(ctx, runtime.StatefulSetUID, runtime.SourceClusterID, func(ledger *fencing.Ledger) error {
				if err := ledger.BindMutationScope(identity, hashJSON(scope), fencing.MutationHeldExecutionProfile); err != nil {
					return err
				}
				if err := ledger.BindMutationStage(identity, stages.incarnation); err != nil {
					return err
				}
				return ledger.Bind(identity)
			}); err != nil {
				t.Fatalf("bind manual fork WAL %d: %v", i, err)
			}
			if _, err := f.e.changeMutationProducer(ctx, runtime, func(producer *fencing.MutationProducer) error {
				return producer.Activate(identity)
			}); err != nil {
				t.Fatalf("activate manual fork producer %d: %v", i, err)
			}
		}
		targets = append(targets, FenceTarget{NodeID: runtime.NodeID, Pod: runtime.NodeID, PodUID: runtime.PodUID,
			WALIdentity: identity.WALIdentity, NodeName: runtime.NodeName, NodeUID: runtime.NodeUID,
			ContainerID: runtime.ContainerID, ImageID: runtime.ImageID})
		pod := independentPod(runtime.NodeID, runtime.PodUID)
		asObject(pod["metadata"])["namespace"] = "test"
		asObject(pod["metadata"])["ownerReferences"] = []any{object{"kind": "StatefulSet", "name": "rhiza", "uid": runtime.StatefulSetUID, "controller": true}}
		asObject(pod["spec"])["nodeName"] = runtime.NodeName
		asObject(list(nested(pod, "spec", "containers"))[0])["env"] = env
		asObject(list(nested(pod, "status", "containerStatuses"))[0])["containerID"] = runtime.ContainerID
		asObject(list(nested(pod, "status", "containerStatuses"))[0])["imageID"] = runtime.ImageID
		pods = append(pods, pod)
	}
	asObject(nested(sts, "spec"))["replicas"] = float64(len(members))
	asObject(nested(sts, "spec"))["volumeClaimTemplates"] = []any{}
	asObject(nested(sts, "spec", "template", "spec"))["volumes"] = []any{object{"name": "data", "emptyDir": object{}}}
	f.f.authStatefulSet = sts
	f.f.mu.Lock()
	f.f.objects["/apis/apps/v1/namespaces/test/statefulsets"] = object{"items": []any{sts}}
	f.f.objects["/apis/apps/v1/namespaces/test/statefulsets/rhiza"] = sts
	f.f.objects["/api/v1/namespaces/test/pods"] = object{"items": pods}
	for i := range members {
		runtime := f.runtime
		if i > 0 {
			runtime.NodeName = "manual-fork-node-" + strconv.Itoa(i)
			runtime.NodeUID = "manual-fork-node-uid-" + strconv.Itoa(i)
		}
		f.f.objects["/api/v1/nodes/"+runtime.NodeName] = object{"metadata": object{"name": runtime.NodeName, "uid": runtime.NodeUID}}
	}
	f.f.authPod, f.f.authPodName, f.f.authPodUID = asObject(pods[0]), f.runtime.NodeID, f.runtime.PodUID
	f.f.mu.Unlock()
	return targets
}

// Assert the same live API lookup and source-workload predicates used by
// manualTerminalPreflight before testing its challenge/approval lifecycle.
// Error labels are synthetic; do not print workload environment values.
func assertManualTerminalSourceWorkload(t *testing.T, ctx context.Context, e *FencingExecutor, resource Resource) {
	t.Helper()
	if e == nil || e.Kube == nil {
		t.Fatal("manual terminal fixture source workload API client is unavailable")
	}
	sts, err := e.archiveForkStatefulSet(ctx, resource.Status.StatefulSetUID)
	if err != nil {
		t.Fatalf("manual terminal fixture API StatefulSet lookup failed: %v", err)
	}
	ctr, err := container(sts, resource.Spec.Container)
	if err != nil {
		t.Fatalf("manual terminal fixture database-container selection failed: %v", err)
	}
	env, err := e.controller().environment(ctx, ctr)
	if err != nil {
		t.Fatalf("manual terminal fixture environment resolution failed: %v", err)
	}
	if err := noPVC(sts, ctr, env); err != nil {
		t.Fatalf("manual terminal fixture no-PVC/data-volume contract failed: %v", err)
	}
	if err := e.archiveStoreConfigMatches(env); err != nil {
		t.Fatalf("manual terminal fixture archive identity contract failed: %v", err)
	}
	if resource.Status.FrozenBucketEnvironment == nil {
		t.Fatal("manual terminal fixture persisted frozen archive environment is missing")
	}
}

func (b *frozenRegistrarTrackingBucket) BucketAttrs(context.Context) (*storage.BucketAttrs, error) {
	return &storage.BucketAttrs{Name: "archive", MetaGeneration: 1}, nil
}

func (b *frozenRegistrarTrackingBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if name == b.failGetName && b.failGetCount > 0 {
		b.failGetCount--
		return nil, errors.New("injected post-claim artifact failure")
	}
	if b.failSourceGetAfterForkIntent && b.forkIntentWritten && !b.failSourceGetInjected && strings.HasPrefix(name, b.sourcePrefix+"/") {
		b.failSourceGetInjected = true
		return nil, errors.New("injected frozen-source materialization read failure")
	}
	return b.Bucket.Get(ctx, name)
}

func (b *frozenRegistrarTrackingBucket) Upload(ctx context.Context, name string, body io.Reader, options ...objstore.ObjectUploadOption) error {
	if strings.HasPrefix(name, b.sourcePrefix+"/") {
		b.writes = append(b.writes, "upload:"+name)
		return fmt.Errorf("frozen source upload attempted")
	}
	if err := b.Bucket.Upload(ctx, name, body, options...); err != nil {
		return err
	}
	if name == b.forkIntentName {
		b.forkIntentWritten = true
	}
	return nil
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
	sourceMembers := []quepaxa.Member{forkTestMember(t, f.id.NodeID), forkTestMember(t, "frozen-source-voter-1"), forkTestMember(t, "frozen-source-voter-2")}
	targetMembers := []quepaxa.Member{forkTestMember(t, "frozen-target-voter")}
	targets := configureManualFrozenForkCohort(t, f, sourceMembers, "async")
	commitArchiveGenesisFixture(t, f)
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	core, err := quepaxa.New(quepaxa.Config{NodeID: sourceMembers[0].ID, Cluster: quepaxa.Cluster{ConfigID: recovery.ArchiveConfigID, Members: sourceMembers}, WAL: wal, Transport: testTransport{}})
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
		StatefulSetUID: f.runtime.StatefulSetUID, SourceClusterID: sourceID, OperationID: fenceOperation, Scope: ScopeGeneration, Targets: targets}
	fence := &fenceResource{Metadata: object{"name": "source-fence", "namespace": "test"}, Spec: fenceRequest}
	fenceCollection := "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizafences"
	if err := f.e.reconcileFence(ctx, fenceCollection, fence); err != nil {
		t.Fatalf("close frozen source generation: %v", err)
	}
	if fence.Status.Proof == nil || fence.Status.Proof.IndependentV2 == nil ||
		len(fence.Status.Proof.IndependentV2.Terminals) != len(sourceMembers) {
		t.Fatalf("manual fork fixture did not close the complete CONTROL cohort: proof=%+v", fence.Status.Proof)
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
	resourceSpec := Spec{StatefulSet: "rhiza", SourceClusterID: sourceID, RecoveryID: fenceOperation, Durability: "async", AllowDataLoss: true,
		AllowMissingSourceMembership: true}
	resource := Resource{APIVersion: "rhiza.mrchypark.dev/v1alpha1", Kind: "RhizaRecovery",
		Metadata: object{"name": "rhiza-recovery", "uid": recoveryUID, "namespace": "test"}, Spec: resourceSpec,
		Status: Status{RecoveryID: fenceOperation, SpecHash: immutableSpecHash(resourceSpec), Source: sourceID, SourceDurability: "async",
			StatefulSetUID: f.runtime.StatefulSetUID, Target: targetID, TargetBaseKind: "fork", RecoveryMode: RecoveryModeFrozenSource, Stage: "FrozenForked"}}
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
	var sourceContainer object
	for _, candidate := range list(nested(f.f.authStatefulSet, "spec", "template", "spec", "containers")) {
		if str(asObject(candidate)["name"]) == "rhiza" {
			sourceContainer = asObject(candidate)
			break
		}
	}
	if sourceContainer == nil {
		t.Fatal("manual fork source container is unavailable")
	}
	env, err := f.e.controller().environment(ctx, sourceContainer)
	if err != nil {
		t.Fatal(err)
	}
	closedRoot, err := f.e.read(ctx, f.runtime.StatefulSetUID, sourceID)
	if err != nil {
		t.Fatal(err)
	}
	manualDigest, missing, err := f.e.manualMissingSourceMembershipBinding(ctx, resource, env,
		fencing.ArchiveScope{Provider: "gcs", Bucket: "archive", Prefix: sourcePrefix, ClusterID: sourceID}, closedRoot, *fence)
	if err != nil || !missing || !validEvidenceDigest(manualDigest) {
		t.Fatalf("manual cohort binding was not derived from the absent record and canonical closed CONTROL cohort: missing=%t digest=%q err=%v", missing, manualDigest, err)
	}
	resource.Status.ManualMembershipBindingDigest = manualDigest
	if !manualMissingSourceMembershipAllowed(&resource) || str(nested(resource.Metadata, "annotations", automaticOwner)) != "" ||
		str(nested(resource.Metadata, "annotations", automaticLogical)) != "" {
		t.Fatal("fork authorization fixture is not an explicitly opted-in manual recovery child")
	}
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
		RecoveryMode: RecoveryModeFrozenSource, BucketEnvironment: &environment, ManualMembershipBindingDigest: manualDigest}
	persistRecovery := func(value Resource) {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		var encoded object
		if err := json.Unmarshal(data, &encoded); err != nil {
			t.Fatal(err)
		}
		f.f.mu.Lock()
		f.f.objects[f.e.controller().resourcePath(resourceName(&value))] = encoded
		f.f.mu.Unlock()
	}
	for _, name := range []string{"wrong-recovery-uid", "changed-spec", "wrong-manual-digest", "changed-cohort", "malformed-present-membership"} {
		t.Run(name, func(t *testing.T) {
			candidate := resource
			candidate.Spec = resource.Spec
			candidate.Metadata = make(object, len(resource.Metadata))
			for key, value := range resource.Metadata {
				candidate.Metadata[key] = value
			}
			candidate.Status = resource.Status
			candidateRequest := request
			cleanup := func() {}
			switch name {
			case "wrong-recovery-uid":
				candidate.Metadata["uid"] = "different-recovery-uid"
				persistRecovery(candidate)
				cleanup = func() { persistRecovery(resource) }
			case "changed-spec":
				candidate.Spec.AllowDataLoss = false
				persistRecovery(candidate)
				cleanup = func() { persistRecovery(resource) }
			case "wrong-manual-digest":
				candidateRequest.ManualMembershipBindingDigest = strings.Repeat("a", 64)
			case "changed-cohort":
				original := env["RHIZA_CLUSTER_MEMBERS"]
				sourceContainer["env"] = replaceEnv(list(sourceContainer["env"]), "RHIZA_CLUSTER_MEMBERS", object{"value": string(jsonBytes(sourceMembers[:2]))})
				cleanup = func() {
					sourceContainer["env"] = replaceEnv(list(sourceContainer["env"]), "RHIZA_CLUSTER_MEMBERS", object{"value": original})
				}
			case "malformed-present-membership":
				if err := tracked.Bucket.Upload(ctx, path.Join(sourcePrefix, "voters/membership.json"), strings.NewReader("{")); err != nil {
					t.Fatal(err)
				}
				cleanup = func() {
					if err := tracked.Bucket.Delete(ctx, path.Join(sourcePrefix, "voters/membership.json")); err != nil {
						t.Errorf("restore absent membership fixture: %v", err)
					}
				}
			}
			defer cleanup()
			if err := f.e.RegisterArchiveForkGenesis(ctx, candidateRequest); err == nil {
				t.Fatalf("fork registrar accepted invalid manual source evidence: %s", name)
			}
		})
	}
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
