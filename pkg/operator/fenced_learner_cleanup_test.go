package operator

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

func TestStoppingDeletesOnlyFencedStandaloneLearner(t *testing.T) {
	for _, scenario := range []string{"bound", "never-bound", "unknown-run", "bind-response-lost", "tampered-ledger", "history-operation", "history-credential", "history-ledger-uid-missing", "recreated-ledger", "recreated-credential", "duplicate-history", "scope-denied", "manual", "scale-first", "no-proof", "incomplete-proof", "voter-proof", "reused-uid", "delete-conflict", "controller", "unrelated", "wrong-owner", "wrong-operation", "wrong-credential", "changed-intent", "no-admission-closure"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, _, r, api, c := controllerFixture(t, "before-ack", "before-ack")
			r.Metadata["annotations"] = object{automaticOwner: "root-uid", automaticLogical: "logical"}
			r.Spec.RecoveryID = "recover"
			r.Spec.Fence = fence(r)
			r.Status.Stage, r.Status.StatefulSetUID = "Stopping", "sts-uid"
			asObject(api.sts["metadata"])["annotations"] = object{ownerAnnotation: c.operationID(r)}
			asObject(api.sts["spec"])["replicas"] = float64(0)
			pod := supervisedTestPod()
			container := asObject(list(nested(pod, "spec", "containers"))[0])
			container["name"] = "rhiza"
			container["env"] = []any{object{"name": "RHIZA_CLUSTER_ID", "value": "source"}}
			owner := object{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "rhiza", "uid": "sts-uid", "controller": false}
			annotations := object{fencingProtocolAnnotation: "1", fencingRootAnnotation: "root-uid", fencingCredentialAnnotation: "credential-uid", ownerAnnotation: "original-online-op"}
			pod["metadata"] = object{"name": "learner", "uid": "learner-uid", "ownerReferences": []any{owner}, "annotations": annotations, "labels": object{"rhiza.mrchypark.dev/automatic-learner": "learner"}}
			nodeID, err := fencing.ReplacementNodeID("learner-uid")
			if err != nil {
				t.Fatal(err)
			}
			request := FenceRequest{LogicalID: "logical", BindingUID: "root-uid", Namespace: "default", StatefulSet: "rhiza", StatefulSetUID: "sts-uid", SourceClusterID: "source", OperationID: "recover", Scope: ScopeGeneration, Targets: []FenceTarget{{NodeID: nodeID, Pod: "learner", PodUID: "learner-uid", WALIdentity: strings.Repeat("a", 64)}}}
			if scenario == "voter-proof" {
				request.Scope = ScopeVoter
			}
			neverBound := scenario == "never-bound" || scenario == "unknown-run" || scenario == "bind-response-lost" || scenario == "tampered-ledger"
			if neverBound {
				request.Targets[0].WALIdentity = ""
			}
			hash, err := requestHash(&request)
			if err != nil {
				t.Fatal(err)
			}
			state := validAutoControl()
			state.BindingUID, state.StatefulSetUID = "root-uid", "sts-uid"
			state.Intent = &autoIntent{Request: request, RecoveryName: resourceName(r), RecoveryUID: str(r.Metadata["uid"]), SpecHash: hashJSON(r.Spec), Proof: &FenceProof{OperationID: request.OperationID, BindingUID: request.BindingUID, SourceClusterID: request.SourceClusterID, RequestHash: hash, ProofID: r.Spec.Fence.Evidence, ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true}}
			credentialName := (&FencingExecutor{Kube: c.Kube}).incarnationSecretName("root-uid", "sts-uid", fencing.Identity{SourceClusterID: "source", NodeID: nodeID, PodName: "learner", PodUID: "learner-uid"})
			ledgerUID := "ledger-uid"
			if scenario == "history-ledger-uid-missing" {
				ledgerUID = ""
			}
			state.Learners = []learnerIncarnation{{SourceClusterID: "source", OperationID: "original-online-op", LedgerUID: ledgerUID, CredentialUID: "credential-uid", CredentialName: credentialName, Target: request.Targets[0]}}
			ledger := fenceLedger{StatefulSetUID: "sts-uid", Ledger: fencing.Ledger{SourceClusterID: "source"}}
			id := fencing.Identity{SourceClusterID: "source", NodeID: nodeID, PodName: "learner", PodUID: "learner-uid", CredentialName: credentialName, CredentialUID: "credential-uid", RunID: "run"}
			if err := ledger.Ledger.Register(id); err != nil {
				t.Fatal(err)
			}
			if !neverBound && scenario != "bind-response-lost" {
				id.WALIdentity = request.Targets[0].WALIdentity
				if err := ledger.Ledger.Bind(id); err != nil {
					t.Fatal(err)
				}
			}
			if err := ledger.Ledger.CloseNodes(request.OperationID, hash, nil); err != nil {
				t.Fatal(err)
			}
			stop := *ledger.Ledger.Runs["run"].Stop
			if neverBound && scenario != "unknown-run" && scenario != "bind-response-lost" {
				if err := ledger.Ledger.RecordNeverBoundExit(stop); err != nil {
					t.Fatal(err)
				}
			} else if !neverBound {
				if err := ledger.Ledger.RecordDrain(fencing.DrainReceipt{Request: stop, WritersJoined: true}); err != nil {
					t.Fatal(err)
				}
				if err := ledger.Ledger.RecordExit(stop); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "unknown-run" {
				unknown := fencing.Identity{SourceClusterID: "source", NodeID: nodeID, PodName: "learner", PodUID: "learner-uid", CredentialName: credentialName, CredentialUID: "credential-uid", RunID: "unknown"}
				ledger.Ledger.Runs[unknown.RunID] = fencing.Run{Identity: unknown}
			}
			if scenario == "no-admission-closure" {
				ledger.Ledger.ClosedBy, ledger.Ledger.ClosedHash = "", ""
			}
			if neverBound {
				state.Intent.Proof.ProofID = "cooperative-" + hashJSON(ledger)
				if scenario == "tampered-ledger" {
					state.Intent.Proof.ProofID = "cooperative-tampered"
				}
				r.Spec.Fence.Evidence = state.Intent.Proof.ProofID
				state.Intent.SpecHash = hashJSON(r.Spec)
			}
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
			case "history-operation":
				state.Learners[0].OperationID = "other-op"
			case "history-credential":
				state.Learners[0].CredentialUID = "other-credential"
			case "recreated-ledger":
				ledgerUID = "new-ledger-uid"
			case "duplicate-history":
				state.Learners = append(state.Learners, state.Learners[0])
			}
			root := autoClusterResource("root", "root-uid", "logical")
			if err := c.saveAuto(ctx, root, state, nil); err != nil {
				t.Fatal(err)
			}
			secretToken := "token"
			if scenario == "recreated-credential" {
				secretToken = "replacement-token"
			}
			member := voterMember("source", nodeID, secretToken)
			binding := hashJSON([]string{"root-uid", "sts-uid", "source", "learner", "learner-uid", nodeID})
			secretUID := "credential-uid"
			if scenario == "recreated-credential" {
				secretUID = "new-credential-uid"
			}
			secret := object{"type": "Opaque", "immutable": true, "metadata": object{"name": credentialName, "namespace": "default", "uid": secretUID, "annotations": object{fencingRootAnnotation: "root-uid", fencingBindingAnnotation: binding}}, "data": object{"member": base64.StdEncoding.EncodeToString(jsonBytes(member)), "RHIZA_PEER_TOKEN": base64.StdEncoding.EncodeToString([]byte(secretToken))}}
			originalMemberData := str(nested(secret, "data", "member"))
			originalPeerToken := str(nested(secret, "data", "RHIZA_PEER_TOKEN"))
			var deletes atomic.Int32
			var secretDeletes atomic.Int32
			var absent atomic.Bool
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.URL.Path == (&FencingExecutor{Kube: c.Kube}).ledgerPath("sts-uid", "source") {
					name := path.Base((&FencingExecutor{Kube: c.Kube}).ledgerPath("sts-uid", "source"))
					_ = json.NewEncoder(w).Encode(object{"metadata": object{"name": name, "namespace": "default", "uid": ledgerUID}, "data": object{"ledger.json": string(jsonBytes(ledger))}})
					return
				}
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
				case c.corePath("secrets", credentialName):
					if req.Method == http.MethodDelete {
						secretDeletes.Add(1)
						w.WriteHeader(http.StatusOK)
						return
					}
					if req.Method != http.MethodGet {
						t.Errorf("unexpected Secret mutation %s", req.Method)
						w.WriteHeader(http.StatusMethodNotAllowed)
						return
					}
					_ = json.NewEncoder(w).Encode(secret)
					return
				}
				api.serve(w, req)
			}))
			defer server.Close()
			c.Kube.BaseURL, c.Kube.Client = server.URL, server.Client()
			if err := c.Kube.ConfigureDeletion("", "default"); err != nil {
				t.Fatal(err)
			}
			if scenario == "scope-denied" {
				if err := c.Kube.ConfigureDeletion("other", "default"); err != nil {
					t.Fatal(err)
				}
			}
			if err := c.advance(ctx, r, sourceState{stateful: api.sts}); err != nil {
				t.Fatal(err)
			}
			wantDeletes := int32(0)
			if scenario == "bound" || scenario == "never-bound" || scenario == "recreated-credential" || scenario == "delete-conflict" {
				wantDeletes = 1
			}
			wantStage := "Stopping"
			if scenario == "wrong-owner" {
				wantStage = "Starting"
			}
			if deletes.Load() != wantDeletes || r.Status.Stage != wantStage {
				t.Fatalf("deletes=%d want=%d status=%+v", deletes.Load(), wantDeletes, r.Status)
			}
			if scenario == "recreated-credential" {
				if deletes.Load() != 1 || secretDeletes.Load() != 0 || str(nested(secret, "metadata", "uid")) != "new-credential-uid" || str(nested(secret, "data", "member")) != originalMemberData || str(nested(secret, "data", "RHIZA_PEER_TOKEN")) != originalPeerToken || state.Learners[0].CredentialUID != "credential-uid" || nested(pod, "metadata", "annotations", fencingCredentialAnnotation) != "credential-uid" {
					t.Fatalf("recreated credential or old Pod was mutated: podDeletes=%d secretDeletes=%d secret=%+v", deletes.Load(), secretDeletes.Load(), secret)
				}
			}
			if scenario == "delete-conflict" && r.Status.Phase != "Blocked" {
				t.Fatalf("conflict advanced: %+v", r.Status)
			}
			if scenario == "bound" || scenario == "never-bound" || scenario == "recreated-credential" {
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

func TestKubernetesDeletePodRequiresUID(t *testing.T) {
	client := &Kubernetes{}
	if err := client.DeletePod(t.Context(), "test", "learner", ""); err == nil || err.Error() != "Kubernetes deletion requires an exact UID" {
		t.Fatalf("empty UID deletion error=%v", err)
	}
}
