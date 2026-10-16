package operator

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/network"
)

func TestMembershipSupervisedInventoryOwnership(t *testing.T) {
	for _, scenario := range []string{"bound", "controller", "omitted-controller", "forged-owner", "missing-owner", "unbound-root", "wrong-operation", "wrong-credential", "mutable-credential", "missing-root", "unwrapped"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, r, api, c := membershipFixture(t)
			r.Metadata["annotations"] = object{automaticOwner: "root-uid"}
			asObject(nested(api.sts, "spec", "template"))["metadata"] = object{"annotations": object{fencingProtocolAnnotation: "1"}}
			pod := supervisedTestPod()
			pod["status"] = api.replacement["status"]
			ctr := asObject(list(nested(pod, "spec", "containers"))[0])
			ctr["name"], ctr["ports"] = "rhiza", nested(api.replacement, "spec", "containers").([]any)[0].(object)["ports"]
			owner := object{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "rhiza", "uid": nested(api.sts, "metadata", "uid"), "controller": false}
			annotations := object{fencingProtocolAnnotation: "1", fencingRootAnnotation: "root-uid", fencingCredentialAnnotation: "secret-uid", ownerAnnotation: r.Spec.Membership.OperationID}
			pod["metadata"] = object{"name": "learner", "uid": "learner-uid", "ownerReferences": []any{owner}, "annotations": annotations, "labels": object{"rhiza.mrchypark.dev/automatic-learner": "learner"}}
			api.replacement = pod
			asObject(api.membershipSecret["metadata"])["annotations"] = object{ownerAnnotation: r.Spec.Membership.OperationID}
			asObject(api.membershipSecret["data"])["member"] = base64.StdEncoding.EncodeToString(jsonBytes(voterMember("source", "learner", "new-token")))
			api.learnerID = "learner"
			delete(api.membership, "n4")
			api.membership["learner"] = network.MembershipStatus{NodeID: "learner", ClusterID: "source", ConfigID: 1, WALIdentity: strings.Repeat("a", 64)}
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
			case "wrong-credential":
				annotations[fencingCredentialAnnotation] = "other-secret"
			case "mutable-credential":
				api.membershipSecret["immutable"] = false
			case "unwrapped":
				ctr["command"] = []any{"/usr/local/bin/rhiza"}
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.URL.Path == "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/default/rhizaclusters" {
					roots := []ClusterResource{{Metadata: object{"uid": "root-uid"}, Spec: ClusterSpec{StatefulSet: "rhiza"}}}
					if scenario == "missing-root" {
						roots = nil
					}
					_ = json.NewEncoder(w).Encode(struct {
						Items []ClusterResource `json:"items"`
					}{roots})
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
		})
	}
}
