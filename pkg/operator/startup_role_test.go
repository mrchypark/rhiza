package operator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

func TestResolveStartupRoleRequiresCanonicalPendingLearnerAssignment(t *testing.T) {
	ctx := context.Background()
	bucket := autoBucket(t)
	root := autoClusterResource("cluster", "root-uid", "logical")
	root.Spec.Automatic = true
	root.Spec.AdoptClusterID = "source"
	root.Spec.StatefulSet = "rhiza"
	root.Spec.Policy = &AutoPolicy{AllowDataLoss: true}
	learnerName, learnerUID, operation := "recovery-learner-1", "learner-uid", "recovery-op"
	target := FenceTarget{NodeID: "old-voter", Pod: "rhiza-2", PodUID: "old-pod-uid", WALIdentity: "old-wal"}
	request := FenceRequest{LogicalID: root.Spec.LogicalID, BindingUID: "root-uid", Namespace: "default", StatefulSet: "rhiza", StatefulSetUID: "sts-uid", SourceClusterID: "source", OperationID: operation, Scope: ScopeVoter, Targets: []FenceTarget{target}}
	requestDigest, err := requestHash(&request)
	if err != nil {
		t.Fatal(err)
	}
	proof := &FenceProof{OperationID: request.OperationID, SourceClusterID: request.SourceClusterID, BindingUID: request.BindingUID, RequestHash: requestDigest, ProofID: "proof-id", ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true}
	state := validAutoControl()
	state.BindingUID, state.BindingHash, state.StatefulSetUID, state.ActiveClusterID = "root-uid", autoBinding(root), "sts-uid", "source"
	state.Identities = map[quepaxa.NodeID]FenceTarget{"old-voter": target}
	state.Intent = &autoIntent{Request: request, Failed: "old-voter", Proof: proof, RecoveryName: "recovery-child", LearnerName: learnerName, LearnerUID: learnerUID}
	controller := &Controller{Kube: &Kubernetes{Namespace: "default"}, Bucket: bucket, Prefix: "rhiza"}
	if err := controller.saveAuto(ctx, root, state, nil); err != nil {
		t.Fatal(err)
	}
	member, err := json.Marshal(quepaxa.Member{ID: quepaxa.NodeID(learnerName), PublicKey: quepaxa.PublicKey{1}})
	if err != nil {
		t.Fatal(err)
	}
	secret := object{"immutable": true, "metadata": object{"uid": "credential-uid", "annotations": object{ownerAnnotation: operation}}, "data": object{"member": base64.StdEncoding.EncodeToString(member)}}
	pod := object{"metadata": object{"name": learnerName, "uid": learnerUID,
		"labels":          object{"rhiza.mrchypark.dev/automatic-learner": learnerName},
		"annotations":     object{fencingRootAnnotation: "root-uid", fencingCredentialAnnotation: "credential-uid", ownerAnnotation: operation},
		"ownerReferences": []any{object{"kind": "StatefulSet", "name": "rhiza", "uid": "sts-uid", "controller": false}}}}
	var child *Resource
	var ordinarySTS, ordinarySecret object
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/namespaces/default/pods/" + learnerName:
			_ = json.NewEncoder(w).Encode(pod)
		case "/api/v1/namespaces/default/secrets/" + learnerName + "-credentials":
			_ = json.NewEncoder(w).Encode(secret)
		case "/api/v1/namespaces/default/secrets/ordinary-credentials":
			_ = json.NewEncoder(w).Encode(ordinarySecret)
		case "/apis/apps/v1/namespaces/default/statefulsets/rhiza":
			_ = json.NewEncoder(w).Encode(ordinarySTS)
		case "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/default/rhizaclusters":
			_ = json.NewEncoder(w).Encode(object{"items": []any{root}})
		case "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/default/rhizarecoveries/" + state.Intent.RecoveryName:
			if child == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(child)
		default:
			http.NotFound(w, r)
		}
	}))
	defer api.Close()
	tokenFile := filepath.Join(t.TempDir(), "service-account-token")
	if err := os.WriteFile(tokenFile, []byte("test-service-account-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	e := &FencingExecutor{Kube: &Kubernetes{BaseURL: api.URL, Namespace: "default", TokenFile: tokenFile, Client: api.Client()}, ArchiveBucket: bucket, AutomaticControlPrefix: "rhiza"}
	id := fencing.Identity{SourceClusterID: "source", NodeID: learnerName, PodName: learnerName, PodUID: learnerUID, RunID: "run-1"}
	role, err := e.resolveStartupRole(ctx, "sts-uid", id)
	if err == nil || role != "" {
		t.Fatalf("pending role without canonical authority=%q err=%v", role, err)
	}

	// A complete child is not promoted by this registration path: activation
	// requires the separate durable membership ACK/read-barrier verifier.
	spec := Spec{Membership: &MembershipSpec{OperationID: state.Intent.RecoveryName, Remove: "old-voter", ReplacementPod: learnerName, ReplacementSecret: learnerName + "-credentials", Fence: MembershipFence{NodeID: "old-voter", WALIdentity: target.WALIdentity, WorkloadUID: target.PodUID, Confirmed: true, Evidence: proof.ProofID}}, Fence: Fence{Evidence: proof.ProofID}}
	state.Intent.RecoveryUID, state.Intent.SpecHash = "child-uid", hashJSON(spec)
	if err := controller.saveAuto(ctx, root, state, mustAutoVersion(t, controller, root)); err != nil {
		t.Fatal(err)
	}
	child = &Resource{Metadata: object{"uid": "child-uid"}, Spec: spec, Status: Status{Phase: "Complete"}}
	if role, err := e.resolveStartupRole(ctx, "sts-uid", id); err == nil || role != "" {
		t.Fatalf("completed recovery was promoted: role=%q err=%v", role, err)
	}

	// Ordinary enrolled Pods retain the active-voter path only after the same
	// authenticated live-Pod lookup used by registration. This is a separate
	// ordinary voter fixture, not the previously assigned learner Pod.
	ordinaryPod := supervisedTestPod()
	ordinaryMetadata := asObject(ordinaryPod["metadata"])
	ordinaryMetadata["name"], ordinaryMetadata["uid"] = learnerName, learnerUID
	ordinaryMetadata["labels"] = object{}
	ordinaryMetadata["annotations"] = object{fencingProtocolAnnotation: "1"}
	ordinaryMetadata["ownerReferences"] = []any{object{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "rhiza", "uid": "sts-uid", "controller": true}}
	ordinarySpec := asObject(ordinaryPod["spec"])
	ordinarySpec["serviceAccountName"] = "default"
	ordinaryContainer := asObject(list(ordinarySpec["containers"])[0])
	ordinaryContainer["name"] = "rhiza"
	peerToken := "private-token-" + learnerName
	ordinaryMembers, err := json.Marshal([]quepaxa.Member{{
		ID: quepaxa.NodeID(learnerName),
		PublicKey: quepaxa.PublicKey(network.PeerPublicKey(
			types.ClusterID("source"), quepaxa.NodeID(learnerName), peerToken)),
	}})
	if err != nil {
		t.Fatal(err)
	}
	ordinaryPeerTokens, err := json.Marshal(map[string]string{learnerName: peerToken})
	if err != nil {
		t.Fatal(err)
	}
	ordinaryEnv := []any{
		object{"name": "RHIZA_CLUSTER_ID", "value": "source"},
		object{"name": "RHIZA_CLUSTER_MEMBERS", "value": string(ordinaryMembers)},
	}
	ordinaryEnvFrom := []any{object{"secretRef": object{"name": "ordinary-credentials"}}}
	ordinaryContainer["env"], ordinaryContainer["envFrom"] = ordinaryEnv, ordinaryEnvFrom
	ordinarySTS = object{"metadata": object{"name": "rhiza", "uid": "sts-uid"}, "spec": object{"template": object{
		"metadata": object{"annotations": object{fencingProtocolAnnotation: "1"}},
		"spec":     object{"containers": []any{object{"name": "rhiza", "env": ordinaryEnv, "envFrom": ordinaryEnvFrom}}},
	}}}
	ordinarySecret = object{"metadata": object{"name": "ordinary-credentials", "namespace": "default", "uid": "ordinary-secret-uid"},
		"data": object{"RHIZA_PEER_TOKENS": base64.StdEncoding.EncodeToString(ordinaryPeerTokens)}}
	pod = ordinaryPod
	if role, err := e.resolveStartupRole(ctx, "sts-uid", id); err != nil || role != fencing.StartupRoleVoterActive {
		t.Fatalf("ordinary voter role=%q err=%v", role, err)
	}
}

func mustAutoVersion(t *testing.T, c *Controller, root *ClusterResource) *objstore.ObjectVersion {
	t.Helper()
	_, version, err := c.loadAuto(t.Context(), c.autoKey(root))
	if err != nil {
		t.Fatal(err)
	}
	return version
}
