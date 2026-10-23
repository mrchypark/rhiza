package operator

import (
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/fencing/runtimeagent"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestAuthenticatedLearnerPromotionRetriesExactCanonicalCompletion(t *testing.T) {
	ctx := t.Context()
	f := newAutomaticBootstrapGenesisFixture(t)
	// Model the actual supervised workload before the fixture computes its
	// canonical bootstrap candidate. The same root binding must be present on
	// the StatefulSet template and each existing Pod; the replacement below
	// keeps those annotations while changing only its physical incarnation.
	f.api.mu.Lock()
	stsPath := f.e.controller().stsPath("rhiza")
	supervisedSTS := asObject(f.api.objects[stsPath])
	if supervisedSTS == nil {
		f.api.mu.Unlock()
		t.Fatal("supervised StatefulSet fixture is missing")
	}
	annotations := object{fencingProtocolAnnotation: "1", fencingRootAnnotation: automaticBootstrapClusterUID}
	template := asObject(nested(supervisedSTS, "spec", "template"))
	metadata := asObject(template["metadata"])
	if metadata == nil {
		metadata = object{}
		template["metadata"] = metadata
	}
	metadata["annotations"] = annotations
	containers := list(nested(supervisedSTS, "spec", "template", "spec", "containers"))
	if len(containers) != 1 || asObject(containers[0]) == nil {
		f.api.mu.Unlock()
		t.Fatal("supervised StatefulSet fixture container is missing or ambiguous")
	}
	asObject(containers[0])["ports"] = []any{
		object{"name": "http", "containerPort": float64(8080)},
		object{"name": "peer-quic", "containerPort": float64(9090), "protocol": "UDP"},
	}
	asObject(containers[0])["image"] = "registry.example/rhiza:fixture"
	if f.api.authStatefulSet != nil {
		f.api.authStatefulSet = supervisedSTS
	}
	podsPath := f.e.controller().corePath("pods", "")
	for _, raw := range list(asObject(f.api.objects[podsPath])["items"]) {
		podMetadata := asObject(asObject(raw)["metadata"])
		podMetadata["annotations"] = object{fencingProtocolAnnotation: "1", fencingRootAnnotation: automaticBootstrapClusterUID}
	}
	f.api.mu.Unlock()
	f.e.ControllerServiceAccount = "default"
	controller, _, cluster := automaticBootstrapController(t, f)
	controlPrefix := f.e.ArchiveStoreIdentity["RHIZA_OBJSTORE_PREFIX"]
	if controlPrefix == "" || controlPrefix != controller.Prefix {
		t.Fatalf("fencer/control prefix mismatch: fencer=%q controller=%q", controlPrefix, controller.Prefix)
	}
	f.e.AutomaticControlPrefix = controlPrefix
	fencerControl := &Controller{Kube: f.e.Kube, Prefix: f.e.AutomaticControlPrefix}
	if got, want := fencerControl.autoKey(&cluster), controller.autoKey(&cluster); got != want {
		t.Fatalf("fencer/control automatic key mismatch: fencer=%q controller=%q", got, want)
	}
	clusterPath := controller.clusterPath(automaticBootstrapClusterName)
	// Establish the protected pre-fence automatic-control record before this
	// test mutates it into a canonical same-ordinal promotion intent.
	if err := controller.reconcileCluster(ctx, &cluster); err != nil {
		t.Fatalf("initialize canonical automatic control: %v", err)
	}

	// The source voter is closed through the real CONTROL fencer. The callbacks
	// stand in for the already-verified runtime evidence boundary used by the
	// automatic completion fixtures; no caller-supplied proof is trusted by the
	// promotion request.
	oldRuntime := f.cohort[2]
	oldTarget := FenceTarget{NodeID: oldRuntime.NodeID, Pod: oldRuntime.NodeID, PodUID: oldRuntime.PodUID, WALIdentity: "bootstrap-wal-2"}
	request := FenceRequest{LogicalID: cluster.Spec.LogicalID, BindingUID: automaticBootstrapClusterUID, Namespace: "test",
		StatefulSet: "rhiza", StatefulSetUID: f.runtime.StatefulSetUID, SourceClusterID: f.runtime.SourceClusterID,
		OperationID: "promotion-remove-add", Scope: ScopeVoter, Targets: []FenceTarget{oldTarget}}
	f.e.CollectRuntimeEvidence = func(_ context.Context, runtime fencing.RuntimeBinding, stop fencing.StopRequest) ([]byte, error) {
		if runtime != oldRuntime || stop.Identity.RunID != oldRuntime.RunID {
			return nil, fmt.Errorf("unexpected runtime terminal request")
		}
		return []byte("verified-terminal:" + oldRuntime.RunID), nil
	}
	f.e.VerifyRuntimeEvidence = func(_ context.Context, runtime fencing.RuntimeBinding, stop fencing.StopRequest, receipt []byte) error {
		if runtime != oldRuntime || stop.Identity.RunID != oldRuntime.RunID || string(receipt) != "verified-terminal:"+oldRuntime.RunID {
			return fmt.Errorf("terminal evidence does not bind the exact old runtime")
		}
		return nil
	}
	requestHash, err := requestHash(&request)
	if err != nil {
		t.Fatal(err)
	}
	fenceName := "fence-" + hashJSON([]string{request.BindingUID, request.OperationID})[:40]
	fence := &fenceResource{APIVersion: "rhiza.mrchypark.dev/v1alpha1", Kind: "RhizaFence",
		Metadata: object{"name": fenceName, "namespace": "test", "uid": "promotion-fence-uid", "resourceVersion": "1"}, Spec: request}
	if err := f.e.reconcileFence(ctx, v2FenceEndpoint(), fence); err != nil {
		t.Fatalf("close exact old voter through CONTROL: %v", err)
	}
	if fence.Status.Proof == nil || fence.Status.Proof.IndependentV2 == nil || fence.Status.Proof.RequestHash != requestHash {
		t.Fatalf("source voter closure lacks its exact independent proof: %+v", fence.Status.Proof)
	}

	// Replace only the physical Pod incarnation. The controller helper creates
	// and freezes its new logical identity and immutable credentials.
	const physicalPod, replacementUID = "rhiza-2", "replacement-pod-uid"
	f.api.mu.Lock()
	pods := list(asObject(f.api.objects[f.e.controller().corePath("pods", "")])["items"])
	var replacement object
	for _, raw := range pods {
		pod := asObject(raw)
		if str(nested(pod, "metadata", "name")) == physicalPod {
			replacement = pod
			break
		}
	}
	if replacement == nil {
		f.api.mu.Unlock()
		t.Fatal("same-ordinal source Pod fixture is missing")
	}
	asObject(replacement["metadata"])["uid"] = replacementUID
	replacementContainer := asObject(list(nested(replacement, "spec", "containers"))[0])
	replacementContainer["image"] = "registry.example/rhiza:fixture"
	status := asObject(list(nested(replacement, "status", "containerStatuses"))[0])
	containerID := "containerd://0123456789abcdef0123456789abcdef"
	imageID := "registry.example/rhiza@sha256:" + strings.Repeat("b", 64)
	status["name"], status["containerID"], status["image"], status["imageID"] = "rhiza", containerID, replacementContainer["image"], imageID
	status["ready"], status["restartCount"] = true, float64(0)
	status["state"] = object{"running": object{"startedAt": "2026-10-23T17:00:00Z"}}
	f.api.objects[f.e.controller().corePath("pods", physicalPod)] = replacement
	f.api.authPod, f.api.authPodName, f.api.authPodUID = replacement, physicalPod, replacementUID
	f.api.mu.Unlock()

	state, version, err := controller.loadAuto(ctx, controller.autoKey(&cluster))
	if err != nil || state == nil || version == nil {
		t.Fatalf("load canonical automatic control: state=%+v err=%v", state, err)
	}
	state.Intent = &autoIntent{Request: request, Failed: quepaxa.NodeID(oldTarget.NodeID), Proof: fence.Status.Proof,
		RecoveryName: "promotion-recovery", LearnerWAL: strings.Repeat("c", 64)}
	state.Identities[quepaxa.NodeID(oldTarget.NodeID)] = oldTarget
	var sts object
	if err := f.e.Kube.Get(ctx, f.e.controller().stsPath("rhiza"), &sts); err != nil {
		t.Fatal(err)
	}
	if ok, err := controller.prepareSameOrdinalLearner(ctx, state, sts, "rhiza"); err != nil || !ok {
		t.Fatalf("prepare exact replacement assignment: ok=%t err=%v", ok, err)
	}
	secretPath := f.e.controller().corePath("secrets", state.Intent.LearnerSecretName)
	intent := state.Intent
	f.api.mu.Lock()
	secret := asObject(f.api.objects[secretPath])
	if secret == nil {
		f.api.mu.Unlock()
		t.Fatal("same-ordinal learner Secret was not persisted by Kubernetes fixture")
	}
	secretMetadata := asObject(secret["metadata"])
	if secret["type"] != "Opaque" || secret["immutable"] != true || str(secretMetadata["name"]) != intent.LearnerSecretName ||
		str(secretMetadata["namespace"]) != f.e.Kube.Namespace || str(secretMetadata["uid"]) != intent.LearnerSecretUID ||
		str(secretMetadata["resourceVersion"]) != intent.LearnerSecretRV || str(nested(secret, "metadata", "annotations", ownerAnnotation)) != intent.Request.OperationID ||
		hashJSON(secret["data"]) != intent.LearnerSecretHash {
		f.api.mu.Unlock()
		t.Fatal("same-ordinal learner Secret fixture does not match its captured immutable identity")
	}
	f.api.mu.Unlock()
	childSpec := automaticRecoveryChildSpec(&cluster, state, intent)
	funcMetadata := object{"name": intent.RecoveryName, "namespace": "test", "uid": "promotion-child-uid", "generation": float64(1),
		"annotations": object{automaticOwner: state.BindingUID, automaticLogical: cluster.Spec.LogicalID}}
	child := Resource{APIVersion: "rhiza.mrchypark.dev/v1alpha1", Kind: "RhizaRecovery", Metadata: funcMetadata, Spec: childSpec}
	intent.RecoveryUID, intent.SpecHash = str(funcMetadata["uid"]), hashJSON(childSpec)
	if child.Spec.Membership == nil || str(child.Metadata["uid"]) != intent.RecoveryUID || hashJSON(child.Spec) != intent.SpecHash ||
		child.Spec.Membership.AbortAddition || child.Spec.Membership.OperationID != intent.RecoveryName || child.Spec.Membership.Remove != intent.Failed ||
		child.Spec.Membership.ReplacementPod != physicalPod || child.Spec.Membership.ReplacementSecret != intent.LearnerSecretName ||
		string(child.Spec.Membership.Fence.NodeID) != string(intent.Failed) || child.Spec.Membership.Fence.WALIdentity != oldTarget.WALIdentity ||
		child.Spec.Membership.Fence.WorkloadUID != oldTarget.PodUID || !child.Spec.Membership.Fence.Confirmed ||
		child.Spec.Membership.Fence.Evidence != intent.Proof.ProofID || child.Spec.Fence != (Fence{}) {
		t.Fatal("production-built child does not satisfy the complete canonical startup assignment contract")
	}
	assignment, err := sameOrdinalAssignmentDigest(state, intent)
	if err != nil {
		t.Fatalf("derive same-ordinal assignment digest: %v", err)
	}
	intent.LearnerAssignmentID = assignment
	if err := controller.saveAuto(ctx, &cluster, state, version); err != nil {
		t.Fatalf("persist canonical pending learner assignment: %v", err)
	}
	persistTestRecovery(t, f, child)
	f.api.mu.Lock()
	f.api.objects[clusterPath] = jsonObjectForPreflight(t, cluster)
	f.api.objects["/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizaclusters"] = object{"items": []any{jsonObjectForPreflight(t, cluster)}}
	f.api.mu.Unlock()

	// The runtime agent fixture provides the same authenticated enrollment path
	// as the producer-registration HTTP tests.
	runtimeDir := filepath.Join(t.TempDir(), "runtime-agent")
	if err := os.Mkdir(runtimeDir, 0700); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	runtimeService, err := runtimeagent.New(runtimeagent.Config{AuthorityID: "authority", AuthorityNamespace: "test", AuthorityServiceAccount: "runtime-agent",
		AuthorityWorkloadUID: "runtime-agent-workload", Audience: "rhiza-runtime-agent", Directory: runtimeDir,
		Auth: supervisorRegistrationAgentAuth{}, Backend: supervisorRegistrationAgentBackend{}})
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	runtimeServer := httptest.NewUnstartedServer(runtimeService.Handler())
	runtimeServer.Listener = listener
	runtimeServer.StartTLS()
	t.Cleanup(runtimeServer.Close)
	cert := runtimeServer.Certificate()
	serverName := ""
	if len(cert.DNSNames) != 0 {
		serverName = cert.DNSNames[0]
	} else if len(cert.IPAddresses) != 0 {
		serverName = cert.IPAddresses[0].String()
	}
	if serverName == "" {
		t.Fatal("runtime-agent certificate has no verifiable SAN")
	}
	tokenFile := f.e.Kube.TokenFile
	f.e.RuntimeAgent = &RuntimeAgentClient{Kube: f.e.Kube, AuthorityID: "authority", TokenFile: tokenFile,
		CA: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}), ServerName: serverName,
		Port: strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)}
	authorityServer := httptest.NewTLSServer(f.e.Handler())
	t.Cleanup(authorityServer.Close)
	client := fencing.AuthorityClient{URL: authorityServer.URL + "/v1/supervisor", TokenFile: tokenFile, Client: authorityServer.Client()}
	physicalID := fencing.Identity{SourceClusterID: f.runtime.SourceClusterID, NodeID: intent.LearnerName, PodName: physicalPod,
		PodUID: replacementUID, RunID: "promotion-run", AssignmentID: intent.LearnerAssignmentID}
	replacementNodeName := str(nested(replacement, "spec", "nodeName"))
	enrollment, err := f.e.RuntimeAgent.enrollmentBase(ctx, "test", f.runtime.StatefulSetUID, physicalID)
	if err != nil || replacementNodeName == "" || enrollment.PodName != physicalPod || enrollment.PodUID != replacementUID || enrollment.NodeName != replacementNodeName || enrollment.ContainerID != containerID || enrollment.ImageID != imageID {
		t.Fatalf("replacement Pod does not expose the complete live enrollment identity: binding=%+v err=%v", enrollment, err)
	}
	canonicalChildSpec, canonicalSpecHash, canonicalAssignment := child.Spec, intent.SpecHash, intent.LearnerAssignmentID
	canonicalMembershipFenceEvidence := child.Spec.Membership.Fence.Evidence
	child.Spec.Membership.Fence.Evidence = strings.Repeat("e", 64)
	intent.SpecHash = hashJSON(child.Spec)
	wrongAssignment, err := sameOrdinalAssignmentDigest(state, intent)
	if err != nil {
		t.Fatalf("bind deliberately wrong membership fence to its changed child spec: %v", err)
	}
	intent.LearnerAssignmentID = wrongAssignment
	if err := controller.saveAuto(ctx, &cluster, state, mustAutoVersion(t, controller, &cluster)); err != nil {
		t.Fatalf("persist mismatched membership-fence fixture: %v", err)
	}
	persistTestRecovery(t, f, child)
	wrongPhysicalID := physicalID
	wrongPhysicalID.AssignmentID = wrongAssignment
	if _, assigned, err := f.e.resolveSameOrdinalStartupGrant(ctx, f.runtime.StatefulSetUID, wrongPhysicalID); !assigned || err == nil || !strings.Contains(err.Error(), "startup recovery child differs from assignment") {
		t.Fatalf("mismatched membership-fence evidence did not reach the canonical proof guard: assigned=%t err=%v", assigned, err)
	}
	rootBeforeWrongProof, _, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.RegisterStartup(ctx, wrongPhysicalID); err == nil || !strings.Contains(err.Error(), "HTTP 409") {
		t.Fatalf("startup accepted mismatched membership fence evidence: %v", err)
	}
	rootAfterWrongProof, _, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
	if err != nil || hashJSON(rootBeforeWrongProof) != hashJSON(rootAfterWrongProof) {
		t.Fatalf("rejected wrong-proof startup mutated CONTROL: before=%+v after=%+v err=%v", rootBeforeWrongProof, rootAfterWrongProof, err)
	}
	intent.SpecHash, intent.LearnerAssignmentID = canonicalSpecHash, canonicalAssignment
	// Resource.Spec contains a pointer for Membership, so the saved spec above
	// shares that nested value. Restore the original evidence before reusing it.
	canonicalChildSpec.Membership.Fence.Evidence = canonicalMembershipFenceEvidence
	child.Spec = canonicalChildSpec
	if err := controller.saveAuto(ctx, &cluster, state, mustAutoVersion(t, controller, &cluster)); err != nil {
		t.Fatalf("restore canonical membership-fence assignment: %v", err)
	}
	persistTestRecovery(t, f, child)
	grant, err := client.RegisterStartup(ctx, physicalID)
	if err != nil || grant.Role != fencing.StartupRoleLearnerPendingRemove || grant.Identity != physicalID {
		t.Fatalf("authenticated registration did not retain the pending logical identity: grant=%+v err=%v", grant, err)
	}
	walID := grant.Identity
	walID.WALIdentity = intent.LearnerWAL
	if _, err := client.Bind(ctx, walID); err != nil {
		t.Fatalf("authenticated learner WAL bind: %v", err)
	}
	pendingBefore, _, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PromoteLearner(ctx, walID); !errors.Is(err, fencing.ErrPending) {
		t.Fatalf("promotion before canonical membership completion = %v, want HTTP 202 / ErrPending", err)
	}
	pendingAfter, _, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
	if err != nil || hashJSON(pendingBefore) != hashJSON(pendingAfter) {
		t.Fatalf("pending promotion mutated the authority root: before=%+v after=%+v err=%v", pendingBefore, pendingAfter, err)
	}
	pendingRun, ok := pendingAfter.Ledger.Runs[walID.RunID]
	if !ok || pendingRun.Runtime == nil || pendingRun.MutationProducerUID != "" {
		t.Fatalf("pending learner root unexpectedly has producer authority: %+v", pendingRun)
	}
	if _, producerUID, _, producerErr := f.e.AuthorityState.ReadProducer(ctx, *pendingRun.Runtime); producerErr == nil || producerUID != "" {
		t.Fatalf("pending learner acquired a mutation producer: uid=%q err=%v", producerUID, producerErr)
	}

	// The real completed-child verifier requires the exact journaled REMOVE and
	// ADD, including the immutable replacement Secret identity and request hash.
	member, secretUID, secretRV, err := controller.replacementMember(ctx, intent.LearnerSecretName)
	if err != nil {
		t.Fatal(err)
	}
	addOperation := membershipOperationID(&child, "add")
	member.WALIdentity = intent.LearnerWAL
	addRequest := network.MembershipChange{OperationID: addOperation, ClusterID: state.ActiveClusterID, ExpectedConfigID: 2,
		Add: &member}
	removeRequest := requestMembershipRemoval(child, oldTarget)
	child.Status.Phase = "Complete"
	child.Status.Membership = &MembershipStatus{Phase: "Complete", RemoveRequest: &removeRequest,
		Add: &MembershipAddJournal{OperationID: addOperation, ExpectedConfigID: 2, MemberID: member.ID, WALIdentity: intent.LearnerWAL,
			RequestHash: hashJSON(addRequest), SecretUID: secretUID, SecretResourceVer: secretRV}}
	state.Intent.SpecHash = hashJSON(child.Spec)
	assignment, err = sameOrdinalAssignmentDigest(state, intent)
	if err != nil || assignment != intent.LearnerAssignmentID {
		t.Fatalf("completed child changed its immutable assignment: digest=%q err=%v", assignment, err)
	}
	if err := controller.saveAuto(ctx, &cluster, state, mustAutoVersion(t, controller, &cluster)); err != nil {
		t.Fatalf("persist completed canonical child evidence: %v", err)
	}
	persistTestRecovery(t, f, child)

	rootBefore, _, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	faultBucket := &mutationCASReplyFaultBucket{Bucket: f.e.AuthorityState.bucket, loseCASAfter: 3, failReadAfterReply: true}
	f.e.AuthorityState.bucket = faultBucket
	if _, err := client.PromoteLearner(ctx, walID); err == nil {
		t.Fatal("injected post-promotion producer-pin response loss was not surfaced")
	}
	rootAfterFault, _, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	run := rootAfterFault.Ledger.Runs[walID.RunID]
	if run.Promotion == nil || run.Promotion.OperationID != request.OperationID || run.MutationProducerUID == "" || rootBefore.Ledger.Runs[walID.RunID].Promotion != nil {
		t.Fatalf("fault did not preserve the exact durable promotion and partial producer pin: before=%+v after=%+v", rootBefore.Ledger.Runs[walID.RunID], run)
	}

	receipt, err := client.PromoteLearner(ctx, walID)
	if err != nil || receipt.Validate(walID) != nil || receipt.Role != fencing.StartupRoleVoterActive || receipt.Producer == nil ||
		receipt.Producer.ProducerUID != run.MutationProducerUID || receipt.Producer.Runtime != *run.Runtime || receipt.Producer.StoreScopeHash == "" || receipt.Producer.StageIncarnation == "" {
		t.Fatalf("authenticated exact promotion retry did not return its complete producer binding: receipt=%+v err=%v", receipt, err)
	}
	rootAfterRetry, _, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	active := rootAfterRetry.Ledger.Runs[walID.RunID]
	producer, producerUID, _, err := f.e.AuthorityState.ReadProducer(ctx, *active.Runtime)
	if err != nil || active.Identity != walID || active.MutationProducerUID != producerUID || producerUID != receipt.Producer.ProducerUID ||
		active.MutationScopeHash != receipt.Producer.StoreScopeHash || active.MutationStageIncarnation != receipt.Producer.StageIncarnation ||
		producer.Phase != fencing.MutationOpen || producer.WALIdentity != walID.WALIdentity || producer.LastAdmitted != 0 {
		t.Fatalf("retry did not finish exact active scope/WAL binding: run=%+v producer=%+v err=%v", active, producer, err)
	}

	// A changed canonical child cannot be used to replay a stored promotion.
	child.Status.Membership.Add.RequestHash = strings.Repeat("d", 64)
	persistTestRecovery(t, f, child)
	beforeReject, _, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PromoteLearner(ctx, walID); err == nil {
		t.Fatal("promotion retry accepted changed canonical membership evidence")
	}
	afterReject, _, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
	if err != nil || hashJSON(beforeReject) != hashJSON(afterReject) {
		t.Fatalf("rejected changed evidence mutated the authority root: before=%+v after=%+v err=%v", beforeReject, afterReject, err)
	}

	// Even an otherwise exact replay is denied after the canonical ledger has
	// closed this logical voter identity.
	child.Status.Membership.Add.RequestHash = hashJSON(addRequest)
	persistTestRecovery(t, f, child)
	if _, err := f.e.change(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID, func(ledger *fencing.Ledger) error {
		return ledger.CloseNodes("later-voter-close", strings.Repeat("e", 64), []string{walID.NodeID})
	}); err != nil {
		t.Fatalf("close promoted logical voter identity: %v", err)
	}
	closedBefore, _, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PromoteLearner(ctx, walID); err == nil {
		t.Fatal("promotion retry accepted a canonically closed logical voter identity")
	}
	closedAfter, _, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
	if err != nil || hashJSON(closedBefore) != hashJSON(closedAfter) {
		t.Fatalf("rejected closed-identity retry mutated the authority root: before=%+v after=%+v err=%v", closedBefore, closedAfter, err)
	}
}

func requestMembershipRemoval(child Resource, old FenceTarget) network.MembershipChange {
	return network.MembershipChange{OperationID: membershipOperationID(&child, "remove"), ClusterID: child.Spec.SourceClusterID,
		ExpectedConfigID: 1, Remove: quepaxa.NodeID(old.NodeID), Fence: &network.MembershipFence{NodeID: quepaxa.NodeID(old.NodeID),
			WALIdentity: old.WALIdentity, WorkloadUID: old.PodUID, Confirmed: true, Evidence: child.Spec.Membership.Fence.Evidence}}
}
