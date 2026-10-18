package operator

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestLearnerCreatedBeforeControlCASIsRecoveredBeforeEscalation(t *testing.T) {
	for _, scenario := range []string{"recover", "wrong-root", "wrong-operation", "wrong-credential", "wrong-owner"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := t.Context()
			state := validAutoControl()
			state.StatefulSetUID = "sts-uid"
			root := autoClusterResource("root", state.BindingUID, "logical")
			state.Intent = &autoIntent{LearnerName: "learner", Request: FenceRequest{OperationID: "creation-op"}}
			pod := supervisedTestPod()
			owner := object{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "rhiza", "uid": "sts-uid", "controller": false}
			annotations := object{fencingProtocolAnnotation: "1", fencingRootAnnotation: state.BindingUID, fencingCredentialAnnotation: "credential-uid", ownerAnnotation: "creation-op"}
			pod["metadata"] = object{"name": "learner", "uid": "learner-uid", "ownerReferences": []any{owner}, "annotations": annotations, "labels": object{"rhiza.mrchypark.dev/automatic-learner": "learner"}}
			switch scenario {
			case "wrong-root":
				annotations[fencingRootAnnotation] = "wrong"
			case "wrong-operation":
				annotations[ownerAnnotation] = "wrong"
			case "wrong-credential":
				annotations[fencingCredentialAnnotation] = "wrong"
			case "wrong-owner":
				owner["uid"] = "wrong"
			}
			sts := object{"metadata": object{"name": "rhiza", "uid": "sts-uid"}, "spec": object{"template": object{"metadata": object{"annotations": object{fencingProtocolAnnotation: "1"}}}}}
			c := autoController(autoBucket(t))
			secret := object{"immutable": true, "metadata": object{"uid": "credential-uid", "annotations": object{ownerAnnotation: "creation-op"}}, "data": object{"member": base64.StdEncoding.EncodeToString(jsonBytes(voterMember("source", "learner", "token")))}}
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
				switch q.URL.Path {
				case c.corePath("pods", "learner"):
					_ = json.NewEncoder(w).Encode(pod)
				case c.corePath("secrets", "learner-credentials"):
					_ = json.NewEncoder(w).Encode(secret)
				case c.clusterPath(""):
					_ = json.NewEncoder(w).Encode(object{"items": []any{root}})
				case c.clusterPath("root") + "/status":
					_ = json.NewEncoder(w).Encode(root)
				default:
					http.NotFound(w, q)
				}
			}))
			defer server.Close()
			c.Kube.BaseURL, c.Kube.Client = server.URL, server.Client()
			c.Kube.TokenFile = t.TempDir() + "/token"
			if err := os.WriteFile(c.Kube.TokenFile, []byte("token"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := c.saveAuto(ctx, root, state, nil); err != nil {
				t.Fatal(err)
			}
			state, version, err := c.loadAuto(ctx, c.autoKey(root))
			if err != nil {
				t.Fatal(err)
			}
			if handled, err := c.recoverLearnerInventory(ctx, root, state, version, sts); err != nil || !handled {
				t.Fatalf("handled=%t err=%v", handled, err)
			}
			loaded, _, err := c.loadAuto(ctx, c.autoKey(root))
			if err != nil {
				t.Fatal(err)
			}
			if scenario != "recover" {
				if loaded.Intent.LearnerUID != "" || len(loaded.Learners) != 0 {
					t.Fatal("mismatched creation acquired authority")
				}
				return
			}
			if loaded.Intent.LearnerUID != "learner-uid" || len(loaded.Learners) != 1 || loaded.Learners[0].CredentialUID != "credential-uid" || loaded.Learners[0].Target.WALIdentity != "" {
				t.Fatalf("lost creation identity: %+v", loaded)
			}
			// Rediscovery consumes a reconcile; only a later request may include it.
			req := FenceRequest{SourceClusterID: "source", OperationID: "later-generation", Scope: ScopeGeneration}
			if err := includeLearnerHistory(loaded, &req); err != nil {
				t.Fatal(err)
			}
			if len(req.Targets) != 1 || req.Targets[0].PodUID != "learner-uid" {
				t.Fatalf("missing recovered target: %+v", req)
			}
		})
	}
}

func TestRetiredLearnerSurvivesRotationAndGenerationRequest(t *testing.T) {
	for _, neverBound := range []bool{false, true} {
		t.Run(map[bool]string{false: "bound", true: "never-bound"}[neverBound], func(t *testing.T) {
			f := newRetirementEscalationFixture(t)
			state, version := f.load(t)
			state.Intent.Aborting = true
			f.child.Spec.Membership.AbortAddition = true
			f.child.Spec.Membership.ReplacementPod = "learner"
			f.child.Status.Membership.Phase = "Aborted"
			state.Intent.SpecHash = hashJSON(f.child.Spec)
			f.fencePending = false
			if err := f.c.saveAuto(t.Context(), f.r, state, version); err != nil {
				t.Fatal(err)
			}
			state = f.advance(t)
			if len(state.Learners) != 1 || state.Learners[0].Target.PodUID != "learner-uid" || state.Intent.LearnerUID != "" {
				t.Fatalf("retirement discarded incarnation: %+v", state)
			}
			state, version = f.load(t)
			state.Intent.LearnerUID = "next-uid"
			state.Intent.LearnerWAL = strings.Repeat("c", 64)
			if neverBound {
				state.Intent.LearnerWAL = ""
			}
			state.Intent.LearnerMissingSince = time.Now()
			state.Intent.NoQuorumSince = time.Now().Add(-time.Hour)
			next := learnerIncarnation{SourceClusterID: "source", OperationID: "recover", Target: FenceTarget{NodeID: state.Intent.LearnerName, Pod: state.Intent.LearnerName, PodUID: "next-uid", WALIdentity: state.Intent.LearnerWAL}}
			if err := rememberLearner(state, next); err != nil {
				t.Fatal(err)
			}
			if err := f.c.saveAuto(t.Context(), f.r, state, version); err != nil {
				t.Fatal(err)
			}
			f.live = false
			state = f.advance(t)
			if state.Intent.Request.Scope != ScopeGeneration {
				t.Fatalf("no escalation: %+v", state.Intent)
			}
			found := map[string]bool{}
			for _, target := range state.Intent.Request.Targets {
				found[target.PodUID] = true
			}
			if !found["learner-uid"] || !found["next-uid"] {
				t.Fatalf("generation lost retired/current learner: %+v", state.Intent.Request)
			}
			if len(state.Learners) != 2 || state.Learners[0].OperationID != "recover" {
				t.Fatal("new generation rewrote creation authority")
			}
			// Continue the actual newly reserved request through full proof and Stopping.
			ctx, _, recovery, api, c := controllerFixture(t, "before-ack", "before-ack")
			c.Bucket = f.c.Bucket
			request := state.Intent.Request
			hash, err := requestHash(&request)
			if err != nil {
				t.Fatal(err)
			}
			ledger := fenceLedger{StatefulSetUID: request.StatefulSetUID, Ledger: fencing.Ledger{SourceClusterID: "source"}}
			for _, target := range request.Targets {
				id := fencing.Identity{SourceClusterID: "source", NodeID: target.NodeID, PodUID: target.PodUID, RunID: "run-" + target.NodeID}
				if err := ledger.Ledger.Register(id); err != nil {
					t.Fatal(err)
				}
				if target.WALIdentity != "" {
					id.WALIdentity = target.WALIdentity
					if err := ledger.Ledger.Bind(id); err != nil {
						t.Fatal(err)
					}
				}
			}
			retirement := f.fences[len(f.fences)-1]
			retirementHash, err := requestHash(&retirement)
			if err != nil {
				t.Fatal(err)
			}
			if err := ledger.Ledger.CloseNodes(retirement.OperationID, retirementHash, []string{"learner"}); err != nil {
				t.Fatal(err)
			}
			retiredStop := *ledger.Ledger.Runs["run-learner"].Stop
			if err := ledger.Ledger.RecordDrain(fencing.DrainReceipt{Request: retiredStop, WritersJoined: true}); err != nil {
				t.Fatal(err)
			}
			if err := ledger.Ledger.RecordExit(retiredStop); err != nil {
				t.Fatal(err)
			}
			if err := ledger.Ledger.CloseNodes(request.OperationID, hash, nil); err != nil {
				t.Fatal(err)
			}
			if *ledger.Ledger.Runs["run-learner"].Stop != retiredStop {
				t.Fatal("generation rewrote retirement evidence")
			}
			for _, run := range ledger.Ledger.Runs {
				if run.Exited {
					continue
				}
				if run.Identity.WALIdentity == "" {
					if err := ledger.Ledger.RecordNeverBoundExit(*run.Stop); err != nil {
						t.Fatal(err)
					}
					continue
				}
				if err := ledger.Ledger.RecordDrain(fencing.DrainReceipt{Request: *run.Stop, WritersJoined: true}); err != nil {
					t.Fatal(err)
				}
				if err := ledger.Ledger.RecordExit(*run.Stop); err != nil {
					t.Fatal(err)
				}
			}
			if !ledger.Ledger.Complete(request.OperationID, hash, nil) {
				t.Fatal("full generation not proven")
			}
			recovery.Metadata["name"] = request.OperationID
			recovery.Metadata["annotations"] = object{automaticOwner: state.BindingUID, automaticLogical: "logical"}
			recovery.Spec.RecoveryID = request.OperationID
			recovery.Spec.Fence = fence(recovery)
			recovery.Spec.Fence.Evidence = "cooperative-" + hashJSON(ledger)
			recovery.Status.Stage, recovery.Status.StatefulSetUID = "Stopping", "sts-uid"
			state.Intent.RecoveryName, state.Intent.RecoveryUID = request.OperationID, str(recovery.Metadata["uid"])
			state.Intent.SpecHash = hashJSON(recovery.Spec)
			state.Intent.Proof = &FenceProof{OperationID: request.OperationID, SourceClusterID: "source", BindingUID: state.BindingUID, RequestHash: hash, ProofID: recovery.Spec.Fence.Evidence, ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true}
			_, version = f.load(t)
			var pods []object
			secrets := map[string]object{}
			for idx := range state.Learners {
				entry := &state.Learners[idx]
				entry.CredentialUID = "credential-" + entry.Target.Pod
				pod := supervisedTestPod()
				pod["metadata"] = object{"name": entry.Target.Pod, "uid": entry.Target.PodUID, "ownerReferences": []any{object{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "rhiza", "uid": "sts-uid", "controller": false}}, "annotations": object{fencingProtocolAnnotation: "1", fencingRootAnnotation: state.BindingUID, fencingCredentialAnnotation: entry.CredentialUID, ownerAnnotation: entry.OperationID}, "labels": object{"rhiza.mrchypark.dev/automatic-learner": entry.Target.Pod}}
				pods = append(pods, pod)
				secrets[entry.Target.Pod+"-credentials"] = object{"immutable": true, "metadata": object{"uid": entry.CredentialUID, "annotations": object{ownerAnnotation: entry.OperationID}}, "data": object{"member": base64.StdEncoding.EncodeToString(jsonBytes(voterMember("source", entry.Target.Pod, "token")))}}
			}
			if err := c.saveAuto(ctx, f.r, state, version); err != nil {
				t.Fatal(err)
			}
			asObject(api.sts["metadata"])["annotations"] = object{ownerAnnotation: c.operationID(recovery)}
			asObject(api.sts["spec"])["replicas"] = float64(0)
			absent := false
			deleted := map[string]string{}
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
				if q.URL.Path == c.resourcePath(resourceName(recovery))+"/status" {
					if q.Method != http.MethodPut {
						t.Errorf("unexpected status method %s", q.Method)
						w.WriteHeader(http.StatusMethodNotAllowed)
						return
					}
					if err := json.NewDecoder(q.Body).Decode(recovery); err != nil {
						t.Error(err)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					_ = json.NewEncoder(w).Encode(recovery)
					return
				}
				if q.URL.Path == (&FencingExecutor{Kube: c.Kube}).ledgerPath(request.StatefulSetUID, request.SourceClusterID) {
					_ = json.NewEncoder(w).Encode(object{"data": object{"ledger.json": string(jsonBytes(ledger))}})
					return
				}
				if q.URL.Path == c.corePath("pods", "") {
					items := pods
					if absent {
						items = nil
					}
					_ = json.NewEncoder(w).Encode(podList{Items: items})
					return
				}
				if strings.HasPrefix(q.URL.Path, c.corePath("pods", "")+"/") && q.Method == http.MethodDelete {
					var opts object
					if err := json.NewDecoder(q.Body).Decode(&opts); err != nil {
						t.Error(err)
					}
					deleted[strings.TrimPrefix(q.URL.Path, c.corePath("pods", "")+"/")] = str(nested(opts, "preconditions", "uid"))
					w.WriteHeader(200)
					return
				}
				if q.URL.Path == c.clusterPath("") {
					_ = json.NewEncoder(w).Encode(object{"items": []any{f.r}})
					return
				}
				for name, secret := range secrets {
					if q.URL.Path == c.corePath("secrets", name) {
						_ = json.NewEncoder(w).Encode(secret)
						return
					}
				}
				api.serve(w, q)
			}))
			defer server.Close()
			c.Kube.BaseURL, c.Kube.Client = server.URL, server.Client()
			if err := c.Kube.ConfigureDeletion("", "default"); err != nil {
				t.Fatal(err)
			}
			if err := c.advance(ctx, recovery, sourceState{stateful: api.sts}); err != nil {
				t.Fatal(err)
			}
			if recovery.Status.Stage != "Stopping" || len(deleted) != 2 {
				t.Fatalf("cleanup=%v stage=%+v", deleted, recovery.Status)
			}
			for _, entry := range state.Learners {
				if deleted[entry.Target.Pod] != entry.Target.PodUID {
					t.Fatal("cleanup lost exact historical UID")
				}
			}
			absent = true
			if err := c.advance(ctx, recovery, sourceState{stateful: api.sts}); err != nil {
				t.Fatal(err)
			}
			if recovery.Status.Stage != "Starting" {
				t.Fatalf("empty inventory did not converge: %+v", recovery.Status)
			}
		})
	}
}

func TestLearnerCleanupLedgerReadIsInOperatorRBAC(t *testing.T) {
	data, err := os.ReadFile("../../deploy/operator/rbac.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "resources: [configmaps]\n    verbs: [get]") {
		t.Fatal("operator lacks read-only fencing ledger permission")
	}
}

func TestPruneTerminatedLearnersRequiresExactProof(t *testing.T) {
	wal := strings.Repeat("a", 64)
	request := FenceRequest{LogicalID: "logical", BindingUID: "binding", Namespace: "default", StatefulSet: "rhiza", StatefulSetUID: "sts-uid", SourceClusterID: "source", OperationID: "recover", Scope: ScopeGeneration}
	target := FenceTarget{NodeID: "learner", Pod: "learner", PodUID: "learner-uid", WALIdentity: wal}
	request.Targets = []FenceTarget{target}
	hash, err := requestHash(&request)
	if err != nil {
		t.Fatal(err)
	}
	proof := &FenceProof{OperationID: request.OperationID, SourceClusterID: request.SourceClusterID, BindingUID: request.BindingUID, RequestHash: hash, ProofID: "proof", ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true}
	for _, tc := range []struct {
		name         string
		podFound     bool
		secretFound  bool
		secretUID    string
		neverBound   bool
		neverBoundOK bool
		uncovered    bool
		activeVoter  bool
		inFlight     bool
		replaced     bool
		wantRetained bool
	}{
		{name: "proven terminated"},
		{name: "pod still present", podFound: true, wantRetained: true},
		{name: "credential still reusable", secretFound: true, secretUID: "credential-uid", wantRetained: true},
		{name: "incarnation not covered by completed request", uncovered: true, wantRetained: true},
		{name: "active generation voter", activeVoter: true, wantRetained: true},
		{name: "in-flight intent", inFlight: true, wantRetained: true},
		{name: "never-bound proof missing", neverBound: true, wantRetained: true},
		{name: "never-bound proof verified", neverBound: true, neverBoundOK: true},
		{name: "replacement supersedes same secret", secretFound: true, secretUID: "credential-uid", replaced: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := learnerIncarnation{SourceClusterID: "source", OperationID: "original-online-op", CredentialUID: "credential-uid", Target: target}
			request := request
			proof := *proof
			if tc.neverBound {
				entry.Target.WALIdentity = ""
				request.Targets = []FenceTarget{entry.Target}
			}
			if tc.uncovered {
				request.Targets = nil
			}
			hash, err := requestHash(&request)
			if err != nil {
				t.Fatal(err)
			}
			proof.RequestHash = hash
			if tc.neverBound && tc.neverBoundOK {
				ledger := neverBoundLedger(t, request)
				proof.ProofID = "cooperative-" + hashJSON(ledger)
			}
			state := validAutoControl()
			state.ActiveClusterID = "source"
			if tc.replaced {
				state.ActiveClusterID = "target"
			}
			state.Learners = []learnerIncarnation{entry}
			if tc.activeVoter {
				state.Voters = append(state.Voters, VoterPod{NodeID: quepaxa.NodeID(entry.Target.NodeID), Pod: entry.Target.Pod})
			}
			completed := &autoIntent{Request: request, Proof: &proof}
			if tc.inFlight {
				state.Intent = completed
			}
			c := pruneTestController(t, tc.podFound, tc.secretFound, tc.secretUID, entry.Target, request, tc.neverBound, tc.neverBoundOK)
			if err := c.pruneTerminatedLearners(t.Context(), state, completed); err != nil {
				t.Fatal(err)
			}
			if tc.wantRetained {
				if len(state.Learners) != 1 || len(state.PrunedLearners) != 0 {
					t.Fatalf("incomplete proof pruned history: %+v", state)
				}
				retained := FenceRequest{SourceClusterID: entry.SourceClusterID, Scope: ScopeGeneration}
				if err := includeLearnerHistory(state, &retained); err != nil || len(retained.Targets) != 1 || retained.Targets[0] != entry.Target {
					t.Fatalf("retained incarnation was not fenced: %+v err=%v", retained, err)
				}
				return
			}
			if len(state.Learners) != 0 || len(state.PrunedLearners) != 1 {
				t.Fatalf("proven incarnation was not pruned: %+v", state)
			}
			next := FenceRequest{SourceClusterID: request.SourceClusterID, Scope: ScopeGeneration}
			if err := includeLearnerHistory(state, &next); err != nil || len(next.Targets) != 0 {
				t.Fatalf("next fence retained pruned history: %+v err=%v", next, err)
			}
		})
	}
}

func TestPrunedLearnerReappearanceIsRejournaledAndBlocked(t *testing.T) {
	for _, tc := range []struct {
		name  string
		entry learnerIncarnation
	}{
		{name: "same credential uid", entry: learnerIncarnation{SourceClusterID: "source", OperationID: "new", CredentialUID: "credential-uid", Target: FenceTarget{NodeID: "learner", Pod: "learner", PodUID: "new-uid"}}},
		{name: "same pod uid", entry: learnerIncarnation{SourceClusterID: "source", OperationID: "new", CredentialUID: "new-credential", Target: FenceTarget{NodeID: "learner", Pod: "learner", PodUID: "learner-uid"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := validAutoControl()
			state.PrunedLearners = []learnerIncarnation{{SourceClusterID: "source", OperationID: "old", CredentialUID: "credential-uid", Target: FenceTarget{NodeID: "learner", Pod: "learner", PodUID: "learner-uid"}}}
			if err := rememberLearner(state, tc.entry); err != errPrunedLearnerReappeared {
				t.Fatalf("reappearance error=%v", err)
			}
			if len(state.Learners) != 1 || state.Learners[0] != tc.entry {
				t.Fatalf("reappearing incarnation was not re-journaled: %+v", state)
			}
		})
	}
}

func TestLearnerPruningIsIdempotentAndCASOrdered(t *testing.T) {
	ctx := t.Context()
	entry := learnerIncarnation{SourceClusterID: "source", OperationID: "op", CredentialUID: "credential-uid", Target: FenceTarget{NodeID: "learner", Pod: "learner", PodUID: "learner-uid", WALIdentity: strings.Repeat("a", 64)}}
	request := FenceRequest{LogicalID: "logical", BindingUID: "binding", Namespace: "default", StatefulSet: "rhiza", StatefulSetUID: "sts-uid", SourceClusterID: "source", OperationID: "recover", Scope: ScopeGeneration, Targets: []FenceTarget{entry.Target}}
	hash, err := requestHash(&request)
	if err != nil {
		t.Fatal(err)
	}
	proof := &FenceProof{OperationID: request.OperationID, SourceClusterID: request.SourceClusterID, BindingUID: request.BindingUID, RequestHash: hash, ProofID: "proof", ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true}
	completed := &autoIntent{Request: request, Proof: proof}
	c := pruneTestController(t, false, false, "", entry.Target, request, false, false)
	root := autoClusterResource("root", "binding", "logical")
	state := validAutoControl()
	state.BindingUID, state.StatefulSetUID = "binding", "sts-uid"
	state.Learners = []learnerIncarnation{entry}
	if err := c.saveAuto(ctx, root, state, nil); err != nil {
		t.Fatal(err)
	}
	loaded, version, err := c.loadAuto(ctx, c.autoKey(root))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.pruneTerminatedLearners(ctx, loaded, completed); err != nil {
		t.Fatal(err)
	}
	if err := c.pruneTerminatedLearners(ctx, loaded, completed); err != nil {
		t.Fatal(err)
	}
	if len(loaded.Learners) != 0 || len(loaded.PrunedLearners) != 1 {
		t.Fatalf("pruning was not idempotent: %+v", loaded)
	}
	if err := c.saveAuto(ctx, root, loaded, version); err != nil {
		t.Fatal(err)
	}
	stale := *loaded
	stale.Learners = []learnerIncarnation{entry}
	stale.PrunedLearners = nil
	if err := c.saveAuto(ctx, root, &stale, version); !c.Bucket.IsConditionNotMetErr(err) {
		t.Fatalf("stale snapshot overwrote prune: %v", err)
	}
	final, _, err := c.loadAuto(ctx, c.autoKey(root))
	if err != nil || len(final.Learners) != 0 || len(final.PrunedLearners) != 1 {
		t.Fatalf("stale snapshot resurrected learner history: %+v err=%v", final, err)
	}
}

func pruneTestController(t *testing.T, podFound, secretFound bool, secretUID string, target FenceTarget, request FenceRequest, neverBound, neverBoundOK bool) *Controller {
	t.Helper()
	c := autoController(autoBucket(t))
	pod := supervisedTestPod()
	pod["metadata"] = object{"name": target.Pod, "uid": target.PodUID}
	secret := object{"immutable": true, "metadata": object{"uid": secretUID}}
	var ledger *fenceLedger
	if neverBound {
		if neverBoundOK {
			ledger = neverBoundLedger(t, request)
		} else {
			ledger = &fenceLedger{StatefulSetUID: request.StatefulSetUID, Ledger: fencing.Ledger{SourceClusterID: request.SourceClusterID, Runs: map[string]fencing.Run{}}}
		}
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		switch q.URL.Path {
		case c.corePath("pods", target.Pod):
			if !podFound {
				http.NotFound(w, q)
				return
			}
			_ = json.NewEncoder(w).Encode(pod)
		case c.corePath("secrets", target.Pod+"-credentials"):
			if !secretFound {
				http.NotFound(w, q)
				return
			}
			_ = json.NewEncoder(w).Encode(secret)
		case (&FencingExecutor{Kube: c.Kube}).ledgerPath(request.StatefulSetUID, request.SourceClusterID):
			_ = json.NewEncoder(w).Encode(object{"data": object{"ledger.json": string(jsonBytes(ledger))}})
		default:
			http.NotFound(w, q)
		}
	}))
	t.Cleanup(server.Close)
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("token"), 0o600); err != nil {
		t.Fatal(err)
	}
	c.Kube = &Kubernetes{BaseURL: server.URL, Namespace: "default", TokenFile: token, Client: server.Client()}
	return c
}

func neverBoundLedger(t *testing.T, request FenceRequest) *fenceLedger {
	t.Helper()
	hash, err := requestHash(&request)
	if err != nil {
		t.Fatal(err)
	}
	ledger := &fenceLedger{StatefulSetUID: request.StatefulSetUID, Ledger: fencing.Ledger{SourceClusterID: request.SourceClusterID}}
	id := fencing.Identity{SourceClusterID: request.SourceClusterID, NodeID: request.Targets[0].NodeID, PodUID: request.Targets[0].PodUID, RunID: "run"}
	if err := ledger.Ledger.Register(id); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Ledger.CloseNodes(request.OperationID, hash, nil); err != nil {
		t.Fatal(err)
	}
	stop := *ledger.Ledger.Runs["run"].Stop
	if err := ledger.Ledger.RecordNeverBoundExit(stop); err != nil {
		t.Fatal(err)
	}
	return ledger
}
