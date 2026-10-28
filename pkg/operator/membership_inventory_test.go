package operator

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestMembershipSupervisedInventoryOwnership(t *testing.T) {
	for _, scenario := range []string{"bound", "controller", "omitted-controller", "forged-owner", "missing-owner", "unbound-root", "wrong-operation", "wrong-ledger-credential", "wrong-retained-credential", "wrong-member-id", "wrong-member-key", "missing-credential", "mutable-credential", "missing-root", "unwrapped"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, r, api, c := membershipFixture(t)
			root := autoClusterResource("root", "root-uid", "logical")
			root.Spec.Container = "rhiza"
			r.Metadata["annotations"] = object{automaticOwner: "root-uid", automaticLogical: "logical"}
			asObject(nested(api.sts, "spec", "template"))["metadata"] = object{"annotations": object{fencingProtocolAnnotation: "1"}}
			pod := supervisedTestPod()
			pod["status"] = api.replacement["status"]
			ctr := asObject(list(nested(pod, "spec", "containers"))[0])
			ctr["name"], ctr["ports"] = "rhiza", nested(api.replacement, "spec", "containers").([]any)[0].(object)["ports"]
			owner := object{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "rhiza", "uid": nested(api.sts, "metadata", "uid"), "controller": false}
			canonical, err := fencing.ReplacementNodeID("learner-uid")
			if err != nil {
				t.Fatal(err)
			}
			annotations := object{fencingProtocolAnnotation: "1", fencingRootAnnotation: "root-uid", fencingCredentialAnnotation: "template-secret-uid", ownerAnnotation: r.Spec.Membership.OperationID}
			pod["metadata"] = object{"name": "learner", "uid": "learner-uid", "ownerReferences": []any{owner}, "annotations": annotations, "labels": object{"rhiza.mrchypark.dev/automatic-learner": "learner"}}
			ctr["env"] = []any{object{"name": "RHIZA_CLUSTER_ID", "value": "source"}}
			api.replacement = pod
			credentialName := (&FencingExecutor{Kube: c.Kube}).incarnationSecretName("root-uid", str(nested(api.sts, "metadata", "uid")), fencing.Identity{SourceClusterID: "source", PodName: "learner", PodUID: "learner-uid", NodeID: canonical})
			r.Spec.Membership.ReplacementSecret = credentialName
			member := voterMember("source", canonical, "new-token")
			api.membershipSecret = object{"apiVersion": "v1", "kind": "Secret", "type": "Opaque", "immutable": true, "metadata": object{"name": credentialName, "namespace": "default", "uid": "secret-uid", "resourceVersion": "1", "annotations": object{fencingRootAnnotation: "root-uid", fencingBindingAnnotation: hashJSON([]string{"root-uid", str(nested(api.sts, "metadata", "uid")), "source", "learner", "learner-uid", canonical})}}, "data": object{"member": base64.StdEncoding.EncodeToString(jsonBytes(member)), "RHIZA_PEER_TOKEN": base64.StdEncoding.EncodeToString([]byte("new-token"))}}
			api.membershipSecrets = map[string]object{credentialName: api.membershipSecret}
			api.learnerID = canonical
			delete(api.membership, "n4")
			api.membership[canonical] = network.MembershipStatus{NodeID: quepaxa.NodeID(canonical), ClusterID: "source", ConfigID: 1, WALIdentity: strings.Repeat("a", 64)}
			ledgerCredentialUID := "secret-uid"
			if scenario == "wrong-ledger-credential" {
				ledgerCredentialUID = "unexpected-secret-uid"
			}
			fencer := &FencingExecutor{Kube: c.Kube}
			ledgerPath, ledgerResource := registeredLearnerLedgerFixture(t, fencer, str(nested(api.sts, "metadata", "uid")), "source", fencing.Identity{SourceClusterID: "source", NodeID: canonical, PodName: "learner", PodUID: "learner-uid", CredentialUID: ledgerCredentialUID, CredentialName: credentialName, RunID: "registered-run"})
			state := validAutoControl()
			state.BindingUID, state.StatefulSetUID = "root-uid", str(nested(api.sts, "metadata", "uid"))
			request := FenceRequest{OperationID: r.Spec.Membership.OperationID, SourceClusterID: "source"}
			state.Intent = &autoIntent{Request: request, Proof: &FenceProof{OperationID: request.OperationID, SourceClusterID: "source", BindingUID: "root-uid", ProofID: "proof"}, Failed: r.Spec.Membership.Remove, RecoveryName: resourceName(r), RecoveryUID: str(r.Metadata["uid"]), LearnerName: "learner", LearnerUID: "learner-uid", LearnerWAL: strings.Repeat("a", 64), SpecHash: hashJSON(r.Spec)}
			state.Learners = []learnerIncarnation{{SourceClusterID: "source", OperationID: request.OperationID, CredentialUID: "secret-uid", CredentialName: credentialName, Target: FenceTarget{NodeID: canonical, Pod: "learner", PodUID: "learner-uid", WALIdentity: strings.Repeat("a", 64)}}}
			if scenario == "wrong-retained-credential" {
				state.Learners[0].CredentialUID = "replacement-secret-uid"
			}
			if scenario == "missing-credential" {
				delete(api.membershipSecrets, credentialName)
			}
			if scenario == "wrong-member-id" {
				api.membershipSecret["data"].(object)["member"] = base64.StdEncoding.EncodeToString(jsonBytes(voterMember("source", "learner", "new-token")))
			}
			if scenario == "wrong-member-key" {
				api.membershipSecret["data"].(object)["member"] = base64.StdEncoding.EncodeToString(jsonBytes(voterMember("source", canonical, "other-token")))
			}
			if err := c.saveAuto(ctx, root, state, nil); err != nil {
				t.Fatal(err)
			}
			secretCreates := 0
			switch scenario {
			case "controller":
				owner["controller"] = true
			case "omitted-controller":
				delete(owner, "controller")
			case "forged-owner":
				owner["uid"] = "other-sts"
			case "missing-owner":
				asObject(pod["metadata"])["ownerReferences"] = nil
			case "unbound-root":
				annotations[fencingRootAnnotation] = "other-root"
			case "wrong-operation":
				annotations[ownerAnnotation] = "other-op"
			case "mutable-credential":
				api.membershipSecret["immutable"] = false
			case "unwrapped":
				ctr["command"] = []any{"/usr/local/bin/rhiza"}
			}
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.URL.Path == "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/default/rhizaclusters" {
					roots := []ClusterResource{*root}
					if scenario == "missing-root" {
						roots = nil
					}
					_ = json.NewEncoder(w).Encode(struct {
						Items []ClusterResource `json:"items"`
					}{roots})
					return
				}
				if req.URL.Path == c.stsPath("rhiza") {
					_ = json.NewEncoder(w).Encode(api.sts)
					return
				}
				if req.URL.Path == ledgerPath {
					_ = json.NewEncoder(w).Encode(ledgerResource)
					return
				}
				if req.Method == http.MethodPost && req.URL.Path == c.corePath("secrets", "") {
					secretCreates++
					http.Error(w, "unexpected credential recreation", http.StatusInternalServerError)
					return
				}
				api.serve(w, req)
			}))
			defer server.Close()
			c.Kube.BaseURL, c.Kube.Client = server.URL, server.Client()
			if err := c.reconcileMembership(ctx, r); err != nil {
				t.Fatal(err)
			}
			api.membership["n1"] = membershipStatus("n1", 2, "n1", "n2")
			api.membership["n2"] = membershipStatus("n2", 2, "n1", "n2")
			if err := c.reconcileMembership(ctx, r); err != nil {
				t.Fatal(err)
			}
			if scenario == "bound" {
				if r.Status.Membership.Add == nil || r.Status.Phase == "Blocked" {
					t.Fatalf("bound learner was not journaled: %+v", r.Status)
				}
			} else if r.Status.Phase != "Blocked" || r.Status.Membership.Add != nil || len(api.changes) != 0 {
				t.Fatalf("untrusted learner accepted: %+v changes=%d", r.Status, len(api.changes))
			}
			if api.stsPuts != 0 {
				t.Fatal("membership changed the StatefulSet")
			}
			if secretCreates != 0 {
				t.Fatalf("membership attempted credential recreation: %d", secretCreates)
			}
		})
	}
}
