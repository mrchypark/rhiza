package operator

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestStoppingDeletesOnlyFencedStandaloneLearner(t *testing.T) {
	for _, scenario := range []string{"bound", "manual", "scale-first", "no-proof", "incomplete-proof", "voter-proof", "reused-uid", "delete-conflict", "controller", "unrelated", "wrong-owner", "wrong-operation", "wrong-credential", "changed-intent"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, _, r, api, c := controllerFixture(t, "before-ack", "before-ack")
			r.Metadata["annotations"] = object{automaticOwner: "root-uid", automaticLogical: "logical"}
			r.Spec.RecoveryID = "recover"
			r.Spec.Fence = fence(r)
			r.Status.Stage, r.Status.StatefulSetUID = "Stopping", "sts-uid"
			asObject(api.sts["metadata"])["annotations"] = object{ownerAnnotation: c.operationID(r)}
			asObject(api.sts["spec"])["replicas"] = float64(0)
			pod := supervisedTestPod()
			owner := object{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "rhiza", "uid": "sts-uid", "controller": false}
			annotations := object{fencingProtocolAnnotation: "1", fencingRootAnnotation: "root-uid", fencingCredentialAnnotation: "credential-uid", ownerAnnotation: "original-online-op"}
			pod["metadata"] = object{"name": "learner", "uid": "learner-uid", "ownerReferences": []any{owner}, "annotations": annotations, "labels": object{"rhiza.mrchypark.dev/automatic-learner": "learner"}}
			request := FenceRequest{LogicalID: "logical", BindingUID: "root-uid", Namespace: "default", StatefulSet: "rhiza", StatefulSetUID: "sts-uid", SourceClusterID: "source", OperationID: "recover", Scope: ScopeGeneration, Targets: []FenceTarget{{NodeID: "learner", Pod: "learner", PodUID: "learner-uid", WALIdentity: strings.Repeat("a", 64)}}}
			if scenario == "voter-proof" {
				request.Scope = ScopeVoter
			}
			hash, err := requestHash(&request)
			if err != nil {
				t.Fatal(err)
			}
			state := validAutoControl()
			state.BindingUID, state.StatefulSetUID = "root-uid", "sts-uid"
			state.Intent = &autoIntent{Request: request, RecoveryName: resourceName(r), RecoveryUID: str(r.Metadata["uid"]), SpecHash: hashJSON(r.Spec), Proof: &FenceProof{OperationID: request.OperationID, BindingUID: request.BindingUID, SourceClusterID: request.SourceClusterID, RequestHash: hash, ProofID: r.Spec.Fence.Evidence, ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true}}
			switch scenario {
			case "manual":
				delete(r.Metadata, "annotations")
			case "scale-first":
				asObject(api.sts["spec"])["replicas"] = float64(3)
			case "no-proof":
				state.Intent.Proof = nil
			case "incomplete-proof":
				state.Intent.Proof.StorageQuiesced = false
			case "reused-uid":
				asObject(pod["metadata"])["uid"] = "replacement-uid"
			case "controller":
				owner["controller"] = true
			case "unrelated":
				annotations[fencingRootAnnotation] = "unrelated-root"
			case "wrong-owner":
				owner["uid"] = "other-sts"
			case "wrong-operation":
				annotations[ownerAnnotation] = "other-op"
			case "wrong-credential":
				annotations[fencingCredentialAnnotation] = "other-secret"
			case "changed-intent":
				state.Intent.SpecHash = "other-spec"
			}
			root := autoClusterResource("root", "root-uid", "logical")
			if err := c.saveAuto(ctx, root, state, nil); err != nil {
				t.Fatal(err)
			}
			secret := object{"immutable": true, "metadata": object{"uid": "credential-uid", "annotations": object{ownerAnnotation: "original-online-op"}}, "data": object{"member": base64.StdEncoding.EncodeToString(jsonBytes(voterMember("source", "learner", "token")))}}
			var deletes atomic.Int32
			var absent atomic.Bool
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				switch req.URL.Path {
				case "/api/v1/namespaces/default/pods":
					items := []object{pod}
					if absent.Load() {
						items = nil
					}
					_ = json.NewEncoder(w).Encode(podList{Items: items})
					return
				case "/api/v1/namespaces/default/pods/learner":
					if req.Method != http.MethodDelete {
						t.Error("unexpected Pod mutation")
						w.WriteHeader(405)
						return
					}
					deletes.Add(1)
					var options object
					if json.NewDecoder(req.Body).Decode(&options) != nil || nested(options, "preconditions", "uid") != "learner-uid" || options["kind"] != "DeleteOptions" {
						t.Error("deletion lacked exact UID precondition")
						w.WriteHeader(400)
						return
					}
					if scenario == "delete-conflict" {
						w.WriteHeader(http.StatusConflict)
						return
					}
					w.WriteHeader(http.StatusOK)
					return
				case "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/default/rhizaclusters":
					_ = json.NewEncoder(w).Encode(struct {
						Items []ClusterResource `json:"items"`
					}{[]ClusterResource{*root}})
					return
				case "/api/v1/namespaces/default/secrets/learner-credentials":
					_ = json.NewEncoder(w).Encode(secret)
					return
				}
				api.serve(w, req)
			}))
			defer server.Close()
			c.Kube.BaseURL, c.Kube.Client = server.URL, server.Client()
			if err := c.advance(ctx, r, sourceState{stateful: api.sts}); err != nil {
				t.Fatal(err)
			}
			wantDeletes := int32(0)
			if scenario == "bound" || scenario == "delete-conflict" {
				wantDeletes = 1
			}
			wantStage := "Stopping"
			if scenario == "wrong-owner" {
				wantStage = "Starting"
			}
			if deletes.Load() != wantDeletes || r.Status.Stage != wantStage {
				t.Fatalf("deletes=%d want=%d status=%+v", deletes.Load(), wantDeletes, r.Status)
			}
			if scenario == "delete-conflict" && r.Status.Phase != "Blocked" {
				t.Fatalf("conflict advanced: %+v", r.Status)
			}
			if scenario == "bound" {
				// A successful DELETE response alone cannot advance; the next inventory read must be empty.
				absent.Store(true)
				if err := c.advance(ctx, r, sourceState{stateful: api.sts}); err != nil {
					t.Fatal(err)
				}
				if r.Status.Stage != "Starting" || deletes.Load() != 1 {
					t.Fatalf("cleanup did not converge: %+v deletes=%d", r.Status, deletes.Load())
				}
			}
		})
	}
}

func TestKubernetesDeleteRequiresUID(t *testing.T) {
	client := &Kubernetes{}
	if err := client.Delete(t.Context(), "/api/v1/namespaces/test/pods/learner", ""); err == nil || err.Error() != "Kubernetes deletion requires an exact UID" {
		t.Fatalf("empty UID deletion error=%v", err)
	}
}
