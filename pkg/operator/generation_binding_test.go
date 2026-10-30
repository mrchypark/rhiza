package operator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/mrchypark/rhiza/pkg/recoveryanchor"
	"github.com/thanos-io/objstore"
)

func TestRetainDRSuccessorPinAcceptsOnlyExactLostStatusWrite(t *testing.T) {
	ctx := context.Background()
	child := Resource{
		Metadata: object{"name": "recovery-child", "namespace": "test", "uid": "child-uid", "resourceVersion": "1", "annotations": object{automaticOwner: "root-uid", automaticLogical: "logical"}},
		Spec:     Spec{RecoveryID: "recovery", SourceClusterID: "source", AnchorID: "anchor"},
		Status:   Status{Stage: "Starting", StatefulSetUID: "sts-uid"},
	}
	putCount := 0
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizarecoveries/recovery-child", "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizarecoveries/recovery-child/status":
			if r.Method == http.MethodGet && r.URL.Path == "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizarecoveries/recovery-child" {
				_ = json.NewEncoder(w).Encode(child)
				return
			}
			if r.Method == http.MethodPut {
				var updated Resource
				if json.NewDecoder(r.Body).Decode(&updated) != nil {
					http.Error(w, "invalid status", http.StatusBadRequest)
					return
				}
				putCount++
				child.Status = updated.Status
				child.Metadata["resourceVersion"] = "2"
				// Simulate an API server committing the CAS while the response is lost.
				http.Error(w, "response lost", http.StatusInternalServerError)
				return
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer api.Close()
	tokenPath := path.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte("test-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	controller := &Controller{Kube: &Kubernetes{BaseURL: api.URL, Namespace: "test", TokenFile: tokenPath, Client: api.Client()}}
	if err := controller.retainDRSuccessorPin(ctx, &child, "ledger-uid", "authorization-hash"); err != nil {
		t.Fatalf("exact committed status pin was not recovered after the response loss: %v", err)
	}
	want := DRSuccessorPin{LedgerUID: "ledger-uid", AuthorizationHash: "authorization-hash"}
	if child.Status.DRSuccessorPin == nil || *child.Status.DRSuccessorPin != want || putCount != 1 {
		t.Fatalf("retained pin=%+v status writes=%d, want exact pin and one CAS", child.Status.DRSuccessorPin, putCount)
	}
	if err := controller.retainDRSuccessorPin(ctx, &child, "replacement-ledger-uid", "authorization-hash"); err == nil {
		t.Fatal("a retained child pin was silently replaced")
	}
	if putCount != 1 {
		t.Fatalf("conflicting pin caused another status write: %d", putCount)
	}
	recreated := child
	recreated.Metadata = object{"name": "recovery-child", "namespace": "test", "uid": "old-child-uid", "resourceVersion": "1", "annotations": object{automaticOwner: "root-uid", automaticLogical: "logical"}}
	recreated.Status = Status{}
	if err := controller.retainDRSuccessorPin(ctx, &recreated, "ledger-uid", "authorization-hash"); err == nil {
		t.Fatal("status pin was copied onto a replacement child UID")
	}
}

// A post-DR source without its exact completed child/pin must fail closed; it
// may not fall back to or rewrite the original root-scoped bootstrap pin.
func TestPostDRMembershipRepairFailsClosedWithoutCompletedGenerationBinding(t *testing.T) {
	ctx := context.Background()
	const rootUID, stsUID = "root-uid", "sts-uid"
	const originalSource, activeTarget = "source", "target"
	const initialLedgerUID, targetLedgerUID = "initial-ledger-uid", "target-ledger-uid"

	root := ClusterResource{
		Metadata: object{"uid": rootUID, "name": "rhiza", "namespace": "test"},
		Spec:     ClusterSpec{LogicalID: "logical", StatefulSet: "rhiza", Container: "rhiza", AdoptClusterID: originalSource, Automatic: true},
		Status:   ClusterStatus{ActiveClusterID: activeTarget},
	}
	members := members()
	request := recoveryanchor.Request{
		AnchorID: "anchor", OperationID: "recovery-op", StatefulSetUID: stsUID,
		SourceClusterID: originalSource, TargetClusterID: activeTarget,
		SourcePrefix: "root/source", TargetPrefix: "root/target",
		TargetAnchorHash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		SourceMembership: "source-membership",
		Fork:             recovery.ForkResult{Tip: 1, ManifestHash: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", PrefixHash: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"},
		TargetMembership: recovery.NewMembershipRecord(activeTarget, members, "async"),
	}
	requestHash, err := recoveryanchor.RequestHash(request)
	if err != nil {
		t.Fatal(err)
	}
	targetBinding := recoveryanchor.Binding{ClusterID: activeTarget, StorageID: "target-storage"}
	assignment := fencing.BootstrapAssignment{
		OperationID: "dr-successor-op", RequestHash: "dr-request-hash", Purpose: fencing.BootstrapDRSuccessor,
		RootUID: rootUID, StatefulSetUID: stsUID, SourceClusterID: activeTarget,
		PodName: "rhiza-0", PodUID: "target-pod-uid", NodeID: "rhiza-0",
		CredentialName: "target-credentials", CredentialUID: "target-credential-uid",
		MembershipHash: fencing.BootstrapMembersHash(members), ProfileHash: "target-profile-hash",
		StoreIdentityHash: fencing.BootstrapStoreIdentityHash(storeIdentity()), LedgerUID: targetLedgerUID, Durability: "async",
		DRApproval: fencing.BootstrapDRApproval{Request: request, Receipt: recoveryanchor.Receipt{
			AnchorID: request.AnchorID, OperationID: request.OperationID, RequestHash: requestHash,
			Target: targetBinding, Generation: 2,
		}},
	}
	assignment.AuthorizationHash = fencing.BootstrapDRAuthorizationHash(assignment)
	if err := assignment.Validate(); err != nil {
		t.Fatalf("retained successor assignment is invalid: %v", err)
	}
	operation := fencing.BootstrapOperation{
		OperationID: assignment.OperationID, RequestHash: assignment.RequestHash,
		AuthorizationHash: assignment.AuthorizationHash, Purpose: fencing.BootstrapDRSuccessor,
		RootUID: rootUID, StatefulSetUID: stsUID, SourceClusterID: activeTarget,
	}
	ledger := fencing.Ledger{SourceClusterID: activeTarget}
	if err := ledger.InstallBootstrapOperation(operation); err != nil {
		t.Fatalf("retain DR successor operation: %v", err)
	}
	if err := ledger.AssignBootstrap(assignment); err != nil {
		t.Fatalf("retain DR successor assignment: %v", err)
	}
	ledgerJSON, err := json.Marshal(fenceLedger{StatefulSetUID: stsUID, Ledger: ledger})
	if err != nil {
		t.Fatal(err)
	}
	executorForPath := &FencingExecutor{Kube: &Kubernetes{Namespace: "test"}}
	ledgerName := path.Base(executorForPath.ledgerPath(stsUID, activeTarget))
	liveLedger := object{
		"metadata": object{"name": ledgerName, "namespace": "test", "uid": targetLedgerUID, "resourceVersion": "7"},
		"data":     object{"ledger.json": string(ledgerJSON)},
	}
	sts := object{"metadata": object{"name": "rhiza", "uid": stsUID}}
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/apis/apps/v1/namespaces/test/statefulsets/rhiza":
			_ = json.NewEncoder(w).Encode(sts)
		case "/api/v1/namespaces/test/configmaps/" + ledgerName:
			_ = json.NewEncoder(w).Encode(liveLedger)
		default:
			http.NotFound(w, r)
		}
	}))
	defer api.Close()
	tokenPath := path.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte("test-token"), 0o600); err != nil {
		t.Fatal(err)
	}

	bucket := objstore.NewInMemBucket()
	defer bucket.Close()
	kube := &Kubernetes{BaseURL: api.URL, Namespace: "test", TokenFile: tokenPath, Client: api.Client()}
	controller := &Controller{Kube: kube, Bucket: bucket, Prefix: "root"}
	initialPin := bootstrapLedgerPin{
		Version: 1, OperationID: "initial-bootstrap-op", RequestHash: "initial-request-hash", AuthorizationHash: "initial-auth-hash",
		RootUID: rootUID, StatefulSetUID: stsUID, SourceClusterID: originalSource,
		CredentialName: "initial-credentials", CredentialUID: "initial-credential-uid",
		MembershipHash: fencing.BootstrapMembersHash(members), ProfileHash: "initial-profile-hash", LedgerUID: initialLedgerUID,
	}
	if err := controller.immutable(ctx, controller.bootstrapLedgerPinKey(&root), jsonBytes(initialPin)); err != nil {
		t.Fatalf("seed exact original root bootstrap pin: %v", err)
	}
	if got, err := controller.readBootstrapLedgerPin(ctx, &root); err != nil || got != initialPin {
		t.Fatalf("original initial pin should remain valid under unchanged adoption: pin=%+v err=%v", got, err)
	}

	executor := &FencingExecutor{Kube: kube, Bucket: bucket, Prefix: "root"}
	state := &autoControl{Voters: []VoterPod{{NodeID: "rhiza-0", Pod: "rhiza-0"}, {NodeID: "rhiza-1", Pod: "rhiza-1"}, {NodeID: "rhiza-2", Pod: "rhiza-2"}}, Identities: map[quepaxa.NodeID]FenceTarget{}}
	binding := peerClosureBinding{root: root, rootUID: rootUID, workloadUID: stsUID}
	current := quepaxa.Cluster{ConfigID: 1, Members: members}
	_, discoveryErr := executor.refreshCompletedRepairProjection(ctx, binding, state, nil, activeTarget, &current, "rhiza-0")
	if discoveryErr == nil {
		t.Fatal("post-DR completion projection unexpectedly passed without the completed child binding")
	}
	t.Logf("post-DR discovery projection blocked: active source=%q target ledger UID=%q; retained initial pin source=%q ledger UID=%q; error=%v",
		activeTarget, targetLedgerUID, initialPin.SourceClusterID, initialPin.LedgerUID, discoveryErr)

	_, reserveErr := executor.initBootstrapPin(ctx, binding, activeTarget)
	if reserveErr == nil {
		t.Fatal("post-DR reserve unexpectedly accepted the original adoption pin for the target generation")
	}
	t.Logf("post-DR reserve blocked: root adoption=%q active source=%q; error=%v", root.Spec.AdoptClusterID, activeTarget, reserveErr)

	if got, err := controller.readBootstrapLedgerPin(ctx, &root); err != nil || got != initialPin {
		t.Fatalf("reproduction changed the original bootstrap pin: pin=%+v err=%v", got, err)
	}
	retained, retainedUID, _, err := executor.readWithUID(ctx, stsUID, activeTarget)
	if err != nil || retainedUID != targetLedgerUID || retained.Ledger.SourceClusterID != activeTarget {
		t.Fatalf("reproduction changed the target ledger identity: source=%q uid=%q err=%v", retained.Ledger.SourceClusterID, retainedUID, err)
	}
	retainedOperation, ok := retained.Ledger.Bootstrap.Operations[assignment.OperationID]
	if !ok || retainedOperation.Assignments[assignment.PodUID] != assignment {
		t.Fatal("reproduction changed the retained DR successor operation or assignment")
	}
}
