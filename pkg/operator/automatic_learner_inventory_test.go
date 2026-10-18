package operator

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
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

func TestCompactionRequiresLedgerProof(t *testing.T) {
	for _, tc := range []struct {
		name         string
		podFound     bool
		secretUID    string
		neverBound   bool
		ledgerOK     bool
		activeVoter  bool
		inFlight     bool
		intentTarget bool
		replaced     bool
		wantRetained bool
	}{
		{name: "proven terminated", ledgerOK: true},
		{name: "pod still present", ledgerOK: true, podFound: true, wantRetained: true},
		{name: "credential still reusable", ledgerOK: true, secretUID: "credential-uid", wantRetained: true},
		{name: "active generation voter", ledgerOK: true, activeVoter: true, wantRetained: true},
		{name: "unrelated in-flight intent", ledgerOK: true, inFlight: true},
		{name: "intent-owned incarnation", ledgerOK: true, inFlight: true, intentTarget: true, wantRetained: true},
		{name: "never-bound proof missing", neverBound: true, wantRetained: true},
		{name: "never-bound proof verified", neverBound: true, ledgerOK: true},
		{name: "replacement supersedes same secret", ledgerOK: true, secretUID: "credential-uid", replaced: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := learnerIncarnation{SourceClusterID: "source", OperationID: "op", CredentialUID: "credential-uid", Target: FenceTarget{NodeID: "learner", Pod: "learner", PodUID: "learner-uid", WALIdentity: strings.Repeat("a", 64)}}
			if tc.neverBound {
				entry.Target.WALIdentity = ""
			}
			f := newCompactionFixture(t)
			if tc.podFound {
				f.podUIDs[entry.Target.Pod] = entry.Target.PodUID
			}
			if tc.secretUID != "" {
				f.secretUIDs[entry.Target.Pod] = tc.secretUID
			}
			if tc.ledgerOK {
				f.ledger = terminatedLedger(t, []learnerIncarnation{entry})
			}
			state := validAutoControl()
			state.StatefulSetUID, state.ActiveClusterID = "sts-uid", "source"
			if tc.replaced {
				state.ActiveClusterID = "target"
			}
			state.Learners = []learnerIncarnation{entry}
			if tc.activeVoter {
				state.Voters = append(state.Voters, VoterPod{NodeID: quepaxa.NodeID(entry.Target.NodeID), Pod: entry.Target.Pod})
			}
			if tc.inFlight {
				state.Intent = &autoIntent{}
				if tc.intentTarget {
					state.Intent.Request.Targets = []FenceTarget{entry.Target}
				}
			}
			changed, err := f.c.compactLearnerHistory(t.Context(), state)
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantRetained {
				if changed || len(state.Learners) != 1 {
					t.Fatalf("incomplete proof pruned history: changed=%t state=%+v", changed, state)
				}
				request := FenceRequest{SourceClusterID: entry.SourceClusterID, Scope: ScopeGeneration}
				if err := includeLearnerHistory(state, &request); err != nil || len(request.Targets) != 1 {
					t.Fatalf("retained incarnation was not fenced: %+v err=%v", request, err)
				}
				return
			}
			if !changed || len(state.Learners) != 0 {
				t.Fatalf("proven incarnation was not compacted: changed=%t state=%+v", changed, state)
			}
			request := FenceRequest{SourceClusterID: entry.SourceClusterID, Scope: ScopeGeneration}
			if err := includeLearnerHistory(state, &request); err != nil || len(request.Targets) != 0 {
				t.Fatalf("next fence retained compacted history: %+v err=%v", request, err)
			}
		})
	}
}

func TestCompactionRetriesUntilProofIsAvailable(t *testing.T) {
	ctx := t.Context()
	entry := learnerIncarnation{SourceClusterID: "source", OperationID: "op", CredentialUID: "credential-uid", Target: FenceTarget{NodeID: "learner", Pod: "learner", PodUID: "learner-uid", WALIdentity: strings.Repeat("a", 64)}}
	f := newCompactionFixture(t)
	state := validAutoControl()
	state.StatefulSetUID, state.ActiveClusterID, state.Learners = "sts-uid", "source", []learnerIncarnation{entry}
	changed, err := f.c.compactLearnerHistory(ctx, state)
	if err != nil || changed || len(state.Learners) != 1 {
		t.Fatalf("missing proof compacted history: changed=%t err=%v state=%+v", changed, err, state)
	}
	f.ledger = terminatedLedger(t, []learnerIncarnation{entry})
	changed, err = f.c.compactLearnerHistory(ctx, state)
	if err != nil || !changed || len(state.Learners) != 0 {
		t.Fatalf("late proof did not compact history: changed=%t err=%v state=%+v", changed, err, state)
	}
}

func TestCompactionIsIdempotentAndCASOrdered(t *testing.T) {
	ctx := t.Context()
	entry := learnerIncarnation{SourceClusterID: "source", OperationID: "op", CredentialUID: "credential-uid", Target: FenceTarget{NodeID: "learner", Pod: "learner", PodUID: "learner-uid", WALIdentity: strings.Repeat("a", 64)}}
	f := newCompactionFixture(t)
	f.ledger = terminatedLedger(t, []learnerIncarnation{entry})
	root := autoClusterResource("root", "binding", "logical")
	state := validAutoControl()
	state.StatefulSetUID, state.Learners = "sts-uid", []learnerIncarnation{entry}
	if err := f.c.saveAuto(ctx, root, state, nil); err != nil {
		t.Fatal(err)
	}
	loaded, version, err := f.c.loadAuto(ctx, f.c.autoKey(root))
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := f.c.compactLearnerHistory(ctx, loaded); err != nil || !changed {
		t.Fatalf("first compaction: changed=%t err=%v", changed, err)
	}
	if changed, err := f.c.compactLearnerHistory(ctx, loaded); err != nil || changed {
		t.Fatalf("second compaction was not idempotent: changed=%t err=%v", changed, err)
	}
	if err := f.c.saveAuto(ctx, root, loaded, version); err != nil {
		t.Fatal(err)
	}
	stale := *loaded
	stale.Learners = []learnerIncarnation{entry}
	if err := f.c.saveAuto(ctx, root, &stale, version); !f.c.Bucket.IsConditionNotMetErr(err) {
		t.Fatalf("stale snapshot overwrote compaction: %v", err)
	}
	final, _, err := f.c.loadAuto(ctx, f.c.autoKey(root))
	if err != nil || len(final.Learners) != 0 {
		t.Fatalf("stale snapshot resurrected learner history: %+v err=%v", final, err)
	}
}

func TestLearnerHistoryStaysBoundedAcrossManyReplacements(t *testing.T) {
	ctx := t.Context()
	f := newCompactionFixture(t)
	root := autoClusterResource("root", "binding", "logical")
	state := validAutoControl()
	state.StatefulSetUID, state.Learners = "sts-uid", nil
	if err := f.c.saveAuto(ctx, root, state, nil); err != nil {
		t.Fatal(err)
	}
	for i := range 200 {
		loaded, version, err := f.c.loadAuto(ctx, f.c.autoKey(root))
		if err != nil {
			t.Fatal(err)
		}
		entry := learnerIncarnation{SourceClusterID: "source", OperationID: fmt.Sprintf("op-%d", i), CredentialUID: fmt.Sprintf("credential-%d", i), Target: FenceTarget{NodeID: fmt.Sprintf("learner-%d", i), Pod: fmt.Sprintf("learner-%d", i), PodUID: fmt.Sprintf("learner-uid-%d", i), WALIdentity: strings.Repeat("a", 64)}}
		if err := rememberLearner(loaded, entry); err != nil {
			t.Fatal(err)
		}
		f.ledger = terminatedLedger(t, []learnerIncarnation{entry})
		changed, err := f.c.compactLearnerHistory(ctx, loaded)
		if err != nil || !changed || len(loaded.Learners) != 0 {
			t.Fatalf("cycle %d did not compact: changed=%t err=%v learners=%d", i, changed, err, len(loaded.Learners))
		}
		if err := f.c.saveAuto(ctx, root, loaded, version); err != nil {
			t.Fatal(err)
		}
	}
	final, _, err := f.c.loadAuto(ctx, f.c.autoKey(root))
	if err != nil || len(final.Learners) != 0 {
		t.Fatalf("bounded record is not readable and empty: %+v err=%v", final, err)
	}
}

func TestCompactionBlocksOversizedUnreducibleHistory(t *testing.T) {
	f := newCompactionFixture(t)
	state := validAutoControl()
	state.StatefulSetUID = "sts-uid"
	for i := range maxLearnerHistory + 1 {
		state.Learners = append(state.Learners, learnerIncarnation{SourceClusterID: "source", OperationID: fmt.Sprintf("op-%d", i), CredentialUID: fmt.Sprintf("credential-%d", i), Target: FenceTarget{NodeID: fmt.Sprintf("learner-%d", i), Pod: fmt.Sprintf("learner-%d", i), PodUID: fmt.Sprintf("learner-uid-%d", i), WALIdentity: strings.Repeat("a", 64)}})
	}
	if _, err := f.c.compactLearnerHistory(t.Context(), state); err == nil || !strings.Contains(err.Error(), "cannot be compacted") {
		t.Fatalf("unreducible history error=%v", err)
	}
}

func TestLearnerProvisioningChecksJournalCapacityBeforeMutation(t *testing.T) {
	ctx := t.Context()
	f := newCompactionFixture(t)
	r := autoClusterResource("root", "binding", "logical")
	state := validAutoControl()
	state.BindingUID, state.StatefulSetUID = "binding", "sts-uid"
	state.Intent = &autoIntent{LearnerName: "replacement", Request: FenceRequest{OperationID: "op"}, Policy: AutoPolicy{FailureGraceSeconds: 1}}
	for i := range maxLearnerHistory {
		state.Learners = append(state.Learners, learnerIncarnation{SourceClusterID: "source", OperationID: fmt.Sprintf("op-%d", i), CredentialUID: fmt.Sprintf("credential-%d", i), Target: FenceTarget{NodeID: fmt.Sprintf("learner-%d", i), Pod: fmt.Sprintf("learner-%d", i), PodUID: fmt.Sprintf("learner-uid-%d", i), WALIdentity: strings.Repeat("a", 64)}})
	}
	if err := f.c.saveAuto(ctx, r, state, nil); err != nil {
		t.Fatal(err)
	}
	loaded, version, err := f.c.loadAuto(ctx, f.c.autoKey(r))
	if err != nil {
		t.Fatal(err)
	}
	if handled, err := f.c.handleAutomaticLearner(ctx, r, loaded, version, object{}, &Resource{}); err != nil || !handled {
		t.Fatalf("full journal provisioning handled=%t err=%v", handled, err)
	}
	if r.Status.Phase != "Blocked" || !strings.Contains(r.Status.Message, "learner history is full") || f.mutations != 0 || len(loaded.Learners) != maxLearnerHistory {
		t.Fatalf("full journal mutated Kubernetes: status=%+v mutations=%d learners=%d", r.Status, f.mutations, len(loaded.Learners))
	}

	f.ledger = terminatedLedger(t, []learnerIncarnation{loaded.Learners[0]})
	if handled, err := f.c.handleAutomaticLearner(ctx, r, loaded, version, object{}, &Resource{}); err != nil || !handled {
		t.Fatalf("compaction recovery handled=%t err=%v", handled, err)
	}
	if r.Status.Phase != "Recovering" || !strings.Contains(r.Status.Message, "compacted before replacement provisioning") || f.mutations != 0 || len(loaded.Learners) != maxLearnerHistory-1 {
		t.Fatalf("compaction did not free capacity before mutation: status=%+v mutations=%d learners=%d", r.Status, f.mutations, len(loaded.Learners))
	}
}

type compactionFixture struct {
	c          *Controller
	ledger     *fenceLedger
	podUIDs    map[string]string
	secretUIDs map[string]string
	mutations  int
}

func newCompactionFixture(t *testing.T) *compactionFixture {
	t.Helper()
	f := &compactionFixture{c: autoController(autoBucket(t)), podUIDs: map[string]string{}, secretUIDs: map[string]string{}}
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("token"), 0o600); err != nil {
		t.Fatal(err)
	}
	f.c.Kube = &Kubernetes{Namespace: "default", TokenFile: token}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		switch {
		case q.URL.Path == "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/default/rhizaclusters/root/status" && q.Method == http.MethodPut:
			var next ClusterResource
			_ = json.NewDecoder(q.Body).Decode(&next)
			_ = json.NewEncoder(w).Encode(next)
		case (q.Method == http.MethodPost || q.Method == http.MethodPut) && (strings.HasPrefix(q.URL.Path, "/api/v1/namespaces/default/secrets") || strings.HasPrefix(q.URL.Path, "/api/v1/namespaces/default/pods") || strings.HasPrefix(q.URL.Path, "/api/v1/namespaces/default/services")):
			f.mutations++
			_ = json.NewEncoder(w).Encode(object{})
		case strings.HasPrefix(q.URL.Path, "/api/v1/namespaces/default/pods/"):
			name := strings.TrimPrefix(q.URL.Path, "/api/v1/namespaces/default/pods/")
			if uid := f.podUIDs[name]; uid != "" {
				_ = json.NewEncoder(w).Encode(object{"metadata": object{"name": name, "uid": uid}})
				return
			}
			http.NotFound(w, q)
		case strings.HasPrefix(q.URL.Path, "/api/v1/namespaces/default/secrets/"):
			name := strings.TrimSuffix(strings.TrimPrefix(q.URL.Path, "/api/v1/namespaces/default/secrets/"), "-credentials")
			if uid := f.secretUIDs[name]; uid != "" {
				_ = json.NewEncoder(w).Encode(object{"immutable": true, "metadata": object{"uid": uid}})
				return
			}
			http.NotFound(w, q)
		case q.URL.Path == (&FencingExecutor{Kube: f.c.Kube}).ledgerPath("sts-uid", "source"):
			if f.ledger == nil {
				http.NotFound(w, q)
				return
			}
			_ = json.NewEncoder(w).Encode(object{"data": object{"ledger.json": string(jsonBytes(f.ledger))}})
		default:
			http.NotFound(w, q)
		}
	}))
	t.Cleanup(server.Close)
	f.c.Kube.BaseURL, f.c.Kube.Client = server.URL, server.Client()
	return f
}

func terminatedLedger(t *testing.T, entries []learnerIncarnation) *fenceLedger {
	t.Helper()
	ledger := &fenceLedger{StatefulSetUID: "sts-uid", Ledger: fencing.Ledger{SourceClusterID: "source"}}
	for i, entry := range entries {
		id := fencing.Identity{SourceClusterID: entry.SourceClusterID, NodeID: entry.Target.NodeID, PodUID: entry.Target.PodUID, RunID: fmt.Sprintf("run-%d", i)}
		if err := ledger.Ledger.Register(id); err != nil {
			t.Fatal(err)
		}
		if entry.Target.WALIdentity != "" {
			id.WALIdentity = entry.Target.WALIdentity
			if err := ledger.Ledger.Bind(id); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := ledger.Ledger.CloseNodes("op", "hash", nil); err != nil {
		t.Fatal(err)
	}
	for _, run := range ledger.Ledger.Runs {
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
	return ledger
}
