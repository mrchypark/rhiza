package operator

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func compactionTarget(pod, uid, wal string) FenceTarget {
	nodeID, _ := fencing.ReplacementNodeID(uid)
	return FenceTarget{NodeID: nodeID, Pod: pod, PodUID: uid, WALIdentity: wal}
}

func TestRememberLearnerDoesNotRetrofitLedgerUID(t *testing.T) {
	target := compactionTarget("learner", "pod-uid", strings.Repeat("a", 64))
	state := validAutoControl()
	state.Learners = []learnerIncarnation{{SourceClusterID: "source", OperationID: "op", CredentialUID: "cred", CredentialName: "learner-credentials", Target: target}}
	if err := rememberLearner(state, learnerIncarnation{SourceClusterID: "source", OperationID: "op", LedgerUID: "new-ledger-uid", CredentialUID: "cred", CredentialName: "learner-credentials", Target: target}); err == nil {
		t.Fatal("live ledger read retroactively adopted by legacy history")
	}
	if state.Learners[0].LedgerUID != "" {
		t.Fatalf("legacy ledger UID changed: %+v", state.Learners[0])
	}
}

// A journaled learner keeps the identity captured when it was provisioned, so a
// later generation fence (escalation or retirement) cannot rebuild an unequal
// target and block recovery during an outage.
func TestLearnerFenceTargetCarriesTheJournaledIdentity(t *testing.T) {
	state := validAutoControl()
	captured := FenceTarget{NodeID: "learner", Pod: "learner", PodUID: "learner-uid", WALIdentity: "learner-wal", NodeName: "node-a", NodeUID: "node-uid-a", ContainerID: "containerd://c0", ImageID: "sha256:img"}
	state.Learners = []learnerIncarnation{{SourceClusterID: "source", OperationID: "op-1", CredentialUID: "cred-1", CredentialName: "learner-credentials", Target: captured}}
	got, err := learnerFenceTarget(state, "learner", "learner-uid", "learner-wal")
	if err != nil || got != captured {
		t.Fatalf("journaled identity lost: %+v", got)
	}
	request := FenceRequest{SourceClusterID: "source", Scope: ScopeGeneration, Targets: []FenceTarget{{NodeID: "rhiza-0", Pod: "rhiza-0", PodUID: "pod-0", WALIdentity: "wal-0"}}}
	request.Targets = append(request.Targets, got)
	if err := includeLearnerHistory(state, &request); err != nil {
		t.Fatalf("journaled learner blocked the generation fence: %v", err)
	}
	// A target rebuilt without the captured identity is still a conflict, so the
	// fail-closed check is unchanged.
	request.Targets[len(request.Targets)-1] = FenceTarget{NodeID: "learner", Pod: "learner", PodUID: "learner-uid", WALIdentity: "learner-wal"}
	if err := includeLearnerHistory(state, &request); err == nil {
		t.Fatal("accepted a rebuilt learner incarnation")
	}
}

func TestLearnerFenceTargetUsesFreshUIDIdentityAfterSameOrdinalHistory(t *testing.T) {
	state := validAutoControl()
	state.Learners = []learnerIncarnation{{
		SourceClusterID: "source", OperationID: "old-op", CredentialUID: "old-cred", CredentialName: "old-secret",
		Target: FenceTarget{NodeID: "old-node", Pod: "rhiza-2", PodUID: "uid-old", WALIdentity: "wal-old"},
	}}
	target, err := learnerFenceTarget(state, "rhiza-2", "uid-new", "wal-new")
	if err != nil || target.NodeID != "pod-uid-new" || target.Pod != "rhiza-2" || target.PodUID != "uid-new" || target.WALIdentity != "wal-new" {
		t.Fatalf("replacement target=%+v err=%v", target, err)
	}
	if got, err := learnerFenceTarget(state, "rhiza-2", "uid-old", "wal-old"); err != nil || got.NodeID != "old-node" {
		t.Fatalf("historical target=%+v err=%v", got, err)
	}
}

func TestLearnerCreatedBeforeControlCASIsRecoveredBeforeEscalation(t *testing.T) {
	for _, scenario := range []string{"recover", "wrong-root", "wrong-operation", "stale-template-credential", "wrong-owner"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := t.Context()
			state := validAutoControl()
			state.StatefulSetUID = "sts-uid"
			root := autoClusterResource("root", state.BindingUID, "logical")
			root.Spec.Container = "rhiza"
			state.Intent = &autoIntent{LearnerName: "learner", Request: FenceRequest{OperationID: "creation-op"}}
			pod := supervisedTestPod()
			container := asObject(list(nested(pod, "spec", "containers"))[0])
			container["name"] = "rhiza"
			container["env"] = []any{object{"name": "RHIZA_CLUSTER_ID", "value": "source"}}
			owner := object{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "rhiza", "uid": "sts-uid", "controller": false}
			annotations := object{fencingProtocolAnnotation: "1", fencingRootAnnotation: state.BindingUID, fencingCredentialAnnotation: "credential-uid", ownerAnnotation: "creation-op"}
			pod["metadata"] = object{"name": "learner", "uid": "learner-uid", "ownerReferences": []any{owner}, "annotations": annotations, "labels": object{"rhiza.mrchypark.dev/automatic-learner": "learner"}}
			switch scenario {
			case "wrong-root":
				annotations[fencingRootAnnotation] = "wrong"
			case "wrong-operation":
				annotations[ownerAnnotation] = "wrong"
			case "stale-template-credential":
				annotations[fencingCredentialAnnotation] = "wrong"
			case "wrong-owner":
				owner["uid"] = "wrong"
			}
			sts := object{"metadata": object{"name": "rhiza", "uid": "sts-uid"}, "spec": object{"template": object{"metadata": object{"annotations": object{fencingProtocolAnnotation: "1"}}}}}
			c := autoController(autoBucket(t))
			canonical, err := fencing.ReplacementNodeID("learner-uid")
			if err != nil {
				t.Fatal(err)
			}
			credentialName := (&FencingExecutor{Kube: c.Kube}).incarnationSecretName(state.BindingUID, "sts-uid", fencing.Identity{SourceClusterID: "source", PodName: "learner", PodUID: "learner-uid", NodeID: canonical})
			member := voterMember("source", canonical, "token")
			secret := object{"type": "Opaque", "immutable": true, "metadata": object{"name": credentialName, "namespace": "default", "uid": "credential-uid", "annotations": object{fencingRootAnnotation: state.BindingUID, fencingBindingAnnotation: hashJSON([]string{state.BindingUID, "sts-uid", "source", "learner", "learner-uid", canonical})}}, "data": object{"member": base64.StdEncoding.EncodeToString(jsonBytes(member)), "RHIZA_PEER_TOKEN": base64.StdEncoding.EncodeToString([]byte("token"))}}
			fencer := &FencingExecutor{Kube: c.Kube}
			ledgerPath, ledgerResource := registeredLearnerLedgerFixture(t, fencer, "sts-uid", "source", fencing.Identity{SourceClusterID: "source", NodeID: canonical, PodName: "learner", PodUID: "learner-uid", CredentialUID: "credential-uid", CredentialName: credentialName, RunID: "registered-run"})
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
				switch q.URL.Path {
				case c.corePath("pods", "learner"):
					_ = json.NewEncoder(w).Encode(pod)
				case c.corePath("secrets", credentialName):
					_ = json.NewEncoder(w).Encode(secret)
				case c.stsPath("rhiza"):
					_ = json.NewEncoder(w).Encode(sts)
				case ledgerPath:
					_ = json.NewEncoder(w).Encode(ledgerResource)
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
			if scenario != "recover" && scenario != "stale-template-credential" {
				if loaded.Intent.LearnerUID != "" || len(loaded.Learners) != 0 {
					t.Fatal("mismatched creation acquired authority")
				}
				return
			}
			if loaded.Intent.LearnerUID != "learner-uid" || len(loaded.Learners) != 1 || loaded.Learners[0].CredentialUID != "credential-uid" || loaded.Learners[0].CredentialName != credentialName || loaded.Learners[0].Target.WALIdentity != "" {
				t.Fatalf("lost creation identity: %+v", loaded)
			}
			if loaded.Learners[0].Target.NodeID != "pod-learner-uid" || loaded.Learners[0].Target.Pod != "learner" {
				t.Fatalf("supervised learner identity conflated Pod name and NodeID: %+v", loaded.Learners[0].Target)
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

func TestLearnerNodeIDAndMembershipProjectionSurviveControlReload(t *testing.T) {
	ctx := t.Context()
	state := validAutoControl()
	state.Intent = &autoIntent{Failed: "n3", Voters: append([]VoterPod(nil), state.Voters...), LearnerName: "learner", LearnerUID: "learner-uid", LearnerWAL: "learner-wal"}
	entry := learnerIncarnation{SourceClusterID: state.ActiveClusterID, OperationID: "recover", CredentialUID: "credential-uid", Target: FenceTarget{NodeID: "pod-learner-uid", Pod: "learner", PodUID: "learner-uid", WALIdentity: "learner-wal"}}
	if err := rememberLearner(state, entry); err != nil {
		t.Fatal(err)
	}
	bucket := autoBucket(t)
	r := autoClusterResource("root", state.BindingUID, "logical")
	c := autoController(bucket)
	if err := c.saveAuto(ctx, r, state, nil); err != nil {
		t.Fatal(err)
	}
	loaded, _, err := c.loadAuto(ctx, c.autoKey(r))
	if err != nil {
		t.Fatal(err)
	}
	got, err := learnerNodeID(loaded, "learner", "learner-uid")
	if err != nil || got != "pod-learner-uid" {
		t.Fatalf("reloaded learner NodeID=%q err=%v", got, err)
	}
	allowed := automaticIntentMemberships(loaded.Intent, got)
	if !automaticMembershipAllowed([]quepaxa.NodeID{"n1", "n2", "pod-learner-uid"}, allowed) || automaticMembershipAllowed([]quepaxa.NodeID{"n1", "n2", "learner"}, allowed) {
		t.Fatalf("membership projection did not use canonical NodeID: %#v", allowed)
	}
	target, err := learnerFenceTarget(loaded, "learner", "learner-uid", "learner-wal")
	if err != nil || target.NodeID != got || target.Pod != "learner" {
		t.Fatalf("retirement target=%+v err=%v", target, err)
	}
}

func TestManagedLearnerCleanupRequiresCanonicalNodeAndExactPodUID(t *testing.T) {
	canonical, err := fencing.ReplacementNodeID("pod-uid-7")
	if err != nil {
		t.Fatal(err)
	}
	target := FenceTarget{NodeID: canonical, Pod: "learner-7", PodUID: "pod-uid-7", WALIdentity: strings.Repeat("a", 64)}
	for _, tc := range []struct {
		name   string
		target FenceTarget
		pod    string
		uid    string
		want   bool
	}{
		{name: "exact canonical incarnation", target: target, pod: "learner-7", uid: "pod-uid-7", want: true},
		{name: "legacy pod name is not managed identity", target: FenceTarget{NodeID: target.Pod, Pod: target.Pod, PodUID: target.PodUID, WALIdentity: target.WALIdentity}, pod: target.Pod, uid: target.PodUID},
		{name: "wrong canonical node ID", target: FenceTarget{NodeID: "pod-other-uid", Pod: target.Pod, PodUID: target.PodUID, WALIdentity: target.WALIdentity}, pod: target.Pod, uid: target.PodUID},
		{name: "wrong live pod UID", target: target, pod: target.Pod, uid: "pod-uid-8"},
		{name: "wrong pod name", target: target, pod: "other-learner", uid: target.PodUID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := managedLearnerFenceTargetMatches(tc.target, tc.pod, tc.uid); got != tc.want {
				t.Fatalf("managed target match=%t want %t: target=%+v live=%s/%s", got, tc.want, tc.target, tc.pod, tc.uid)
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
			for idx := range state.Learners {
				canonical, err := fencing.ReplacementNodeID(state.Learners[idx].Target.PodUID)
				if err != nil {
					t.Fatal(err)
				}
				state.Learners[idx].Target.NodeID = canonical
			}
			if len(state.Learners) != 1 || state.Learners[0].Target.PodUID != "learner-uid" || state.Intent.LearnerUID != "" {
				t.Fatalf("retirement discarded incarnation: %+v", state)
			}
			state, version = f.load(t)
			for idx := range state.Learners {
				canonical, err := fencing.ReplacementNodeID(state.Learners[idx].Target.PodUID)
				if err != nil {
					t.Fatal(err)
				}
				state.Learners[idx].Target.NodeID = canonical
			}
			state.Intent.LearnerUID = "next-uid"
			state.Intent.LearnerWAL = strings.Repeat("c", 64)
			if neverBound {
				state.Intent.LearnerWAL = ""
			}
			state.Intent.LearnerMissingSince = time.Now()
			state.Intent.NoQuorumSince = time.Now().Add(-time.Hour)
			nextNodeID, err := fencing.ReplacementNodeID("next-uid")
			if err != nil {
				t.Fatal(err)
			}
			next := learnerIncarnation{SourceClusterID: "source", OperationID: "recover", Target: FenceTarget{NodeID: nextNodeID, Pod: state.Intent.LearnerName, PodUID: "next-uid", WALIdentity: state.Intent.LearnerWAL}}
			if err := rememberLearner(state, next); err != nil {
				t.Fatal(err)
			}
			if err := f.c.saveAuto(t.Context(), f.r, state, version); err != nil {
				t.Fatal(err)
			}
			// The current rotated learner is intentionally absent; its captured
			// UID/WAL must remain in the generation closure inventory.
			var currentPod object
			if err := f.c.Kube.Get(t.Context(), f.c.corePath("pods", state.Intent.LearnerName), &currentPod); !apiNotFound(err) {
				t.Fatalf("fixture current learner should be absent, got pod=%+v err=%v", currentPod, err)
			}
			f.live = false
			state = f.advance(t)
			if state.Intent.Request.Scope != ScopeGeneration {
				t.Fatalf("no escalation: %+v status=%+v", state.Intent, f.r.Status)
			}
			found := map[string]bool{}
			currentWAL := ""
			for _, target := range state.Intent.Request.Targets {
				found[target.PodUID] = true
				if target.PodUID == "next-uid" {
					currentWAL = target.WALIdentity
				}
			}
			if !found["learner-uid"] || !found["next-uid"] {
				t.Fatalf("generation lost retired/current learner: %+v", state.Intent.Request)
			}
			if len(state.Learners) != 2 || state.Learners[0].OperationID != "recover" {
				t.Fatal("new generation rewrote creation authority")
			}
			wantRetiredWAL := strings.Repeat("a", 64)
			wantCurrentWAL := strings.Repeat("c", 64)
			if neverBound {
				wantCurrentWAL = ""
			}
			if state.Learners[0].Target.WALIdentity != wantRetiredWAL || currentWAL != wantCurrentWAL {
				t.Fatalf("rotation changed learner WAL history: retired=%q current=%q", state.Learners[0].Target.WALIdentity, currentWAL)
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
			for idx := range state.Learners {
				entry := &state.Learners[idx]
				entry.CredentialUID = "credential-" + entry.Target.Pod
				entry.CredentialName = (&FencingExecutor{Kube: c.Kube}).incarnationSecretName(state.BindingUID, request.StatefulSetUID, fencing.Identity{SourceClusterID: entry.SourceClusterID, NodeID: entry.Target.NodeID, PodName: entry.Target.Pod, PodUID: entry.Target.PodUID})
				entry.LedgerUID = "ledger-uid"
			}
			retiredNodeID := ""
			for _, target := range request.Targets {
				id := fencing.Identity{SourceClusterID: "source", NodeID: target.NodeID, PodUID: target.PodUID, RunID: "run-" + target.NodeID}
				if target.NodeID != target.Pod {
					id.PodName = target.Pod
				}
				for _, entry := range state.Learners {
					if entry.Target.PodUID == target.PodUID {
						id.CredentialName, id.CredentialUID = entry.CredentialName, entry.CredentialUID
						if entry.Target.PodUID == "learner-uid" {
							retiredNodeID = entry.Target.NodeID
						}
					}
				}
				if err := ledger.Ledger.Register(id); err != nil {
					t.Fatalf("register target %+v: %v", id, err)
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
			if err := ledger.Ledger.CloseNodes(retirement.OperationID, retirementHash, []string{retiredNodeID}); err != nil {
				t.Fatal(err)
			}
			retiredRun := ledger.Ledger.Runs["run-"+retiredNodeID]
			retiredStop := *retiredRun.Stop
			if err := ledger.Ledger.RecordDrain(fencing.DrainReceipt{Request: retiredStop, WritersJoined: true}); err != nil {
				t.Fatal(err)
			}
			if err := ledger.Ledger.RecordExit(retiredStop); err != nil {
				t.Fatal(err)
			}
			if err := ledger.Ledger.CloseNodes(request.OperationID, hash, nil); err != nil {
				t.Fatal(err)
			}
			if *ledger.Ledger.Runs["run-"+retiredNodeID].Stop != retiredStop {
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
				pod := supervisedTestPod()
				pod["metadata"] = object{"name": entry.Target.Pod, "uid": entry.Target.PodUID, "ownerReferences": []any{object{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "rhiza", "uid": "sts-uid", "controller": false}}, "annotations": object{fencingProtocolAnnotation: "1", fencingRootAnnotation: state.BindingUID, fencingCredentialAnnotation: entry.CredentialUID, ownerAnnotation: entry.OperationID}, "labels": object{"rhiza.mrchypark.dev/automatic-learner": entry.Target.Pod}}
				container := asObject(list(nested(pod, "spec", "containers"))[0])
				container["name"] = "rhiza"
				container["env"] = []any{object{"name": "RHIZA_CLUSTER_ID", "value": "source"}}
				if entry.Target.PodUID == "next-uid" {
					pods = append(pods, pod)
				}
				member := voterMember("source", entry.Target.NodeID, "token")
				binding := hashJSON([]string{state.BindingUID, request.StatefulSetUID, entry.SourceClusterID, entry.Target.Pod, entry.Target.PodUID, entry.Target.NodeID})
				secrets[entry.CredentialName] = object{"type": "Opaque", "immutable": true, "metadata": object{"name": entry.CredentialName, "namespace": c.Kube.Namespace, "uid": entry.CredentialUID, "annotations": object{fencingRootAnnotation: state.BindingUID, fencingBindingAnnotation: binding}}, "data": object{"member": base64.StdEncoding.EncodeToString(jsonBytes(member)), "RHIZA_PEER_TOKEN": base64.StdEncoding.EncodeToString([]byte("token"))}}
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
					name := path.Base((&FencingExecutor{Kube: c.Kube}).ledgerPath(request.StatefulSetUID, request.SourceClusterID))
					_ = json.NewEncoder(w).Encode(object{"metadata": object{"name": name, "namespace": c.Kube.Namespace, "uid": "ledger-uid"}, "data": object{"ledger.json": string(jsonBytes(ledger))}})
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
			if recovery.Status.Stage != "Stopping" || len(deleted) != 1 {
				t.Fatalf("cleanup=%v stage=%+v", deleted, recovery.Status)
			}
			for _, entry := range state.Learners {
				if entry.Target.PodUID == "next-uid" && deleted[entry.Target.Pod] != entry.Target.PodUID {
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

func TestAdvanceAutomaticRecoversCurrentLearnerUIDThenWAL(t *testing.T) {
	f := newRetirementEscalationFixture(t)
	// Start from an intent that has reserved the learner's name but has not yet
	// created its Pod. Healthy, authenticated probes from the surviving voters
	// permit the actual automatic path to create it.
	f.api.replacement = nil
	delete(f.api.membership, "learner")
	for _, id := range []string{"n1", "n2"} {
		status := f.api.membership[id]
		status.Pending = false
		f.api.membership[id] = status
	}
	learnerSourceContainer, err := container(f.api.sts, "rhiza")
	if err != nil {
		t.Fatal(err)
	}
	_, learnerPortText, err := net.SplitHostPort(f.api.learner.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	learnerPort, err := strconv.Atoi(learnerPortText)
	if err != nil {
		t.Fatal(err)
	}
	learnerSourceContainer["ports"] = []any{
		object{"name": "http", "containerPort": float64(learnerPort)},
		object{"name": "peer-quic", "containerPort": float64(9090), "protocol": "UDP"},
	}
	state, version := f.load(t)
	state.Intent.LearnerUID = ""
	state.Intent.LearnerWAL = ""
	state.Intent.LearnerMissingSince = time.Time{}
	state.Intent.NoQuorumSince = time.Time{}
	if err := f.c.saveAuto(t.Context(), f.r, state, version); err != nil {
		t.Fatal(err)
	}

	var createdPod, createdSecret, createdService object
	podGets, podCreates := 0, 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case q.URL.Path == f.c.resourcePath("recover") && q.Method == http.MethodGet:
			_ = json.NewEncoder(w).Encode(f.child)
			return
		case q.URL.Path == f.c.resourcePath("recover")+"/status" && q.Method == http.MethodPut:
			if err := json.NewDecoder(q.Body).Decode(f.child); err != nil {
				t.Errorf("decode recovery status: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(f.child)
			return
		case q.URL.Path == f.c.clusterPath("cluster")+"/status" && q.Method == http.MethodPut:
			_ = json.NewEncoder(w).Encode(f.r)
			return
		case q.URL.Path == f.c.corePath("pods", "learner") && q.Method == http.MethodGet:
			podGets++
			if createdPod == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(createdPod)
			return
		case q.URL.Path == f.c.corePath("pods", "") && q.Method == http.MethodPost:
			if err := json.NewDecoder(q.Body).Decode(&createdPod); err != nil {
				t.Errorf("decode created learner Pod: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			asObject(createdPod["metadata"])["uid"] = "created-learner-uid"
			createdPod["status"] = object{"podIP": "127.0.0.1"}
			podCreates++
			_ = json.NewEncoder(w).Encode(createdPod)
			return
		case q.URL.Path == f.c.corePath("secrets", "learner-credentials") && q.Method == http.MethodGet:
			if createdSecret == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(createdSecret)
			return
		case q.URL.Path == f.c.corePath("secrets", "") && q.Method == http.MethodPost:
			if err := json.NewDecoder(q.Body).Decode(&createdSecret); err != nil {
				t.Errorf("decode created learner Secret: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			asObject(createdSecret["metadata"])["uid"] = "created-learner-secret-uid"
			_ = json.NewEncoder(w).Encode(createdSecret)
			return
		case q.URL.Path == f.c.corePath("services", "learner") && q.Method == http.MethodGet:
			if createdService == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(createdService)
			return
		case q.URL.Path == f.c.corePath("services", "") && q.Method == http.MethodPost:
			if err := json.NewDecoder(q.Body).Decode(&createdService); err != nil {
				t.Errorf("decode created learner Service: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(createdService)
			return
		default:
			f.api.serve(w, q)
		}
	}))
	t.Cleanup(server.Close)
	f.c.Kube.BaseURL, f.c.Kube.Client = server.URL, server.Client()
	probeCalls := 0
	probeTransport := f.c.HTTP.Transport
	f.c.HTTP.Transport = escalationProbeTransport(func(q *http.Request) (*http.Response, error) {
		if q.URL.Path == "/recovery/probe" {
			probeCalls++
		}
		return probeTransport.RoundTrip(q)
	})

	state = f.advance(t)
	if podGets < 2 || podCreates != 1 || probeCalls != 2 || createdPod == nil || createdSecret == nil || createdService == nil {
		t.Fatalf("advanceAutomatic did not probe both survivors and observe 404 then CREATE learner resources: probes=%d podGETs=%d podPOSTs=%d pod=%+v secret=%+v service=%+v intent=%+v status=%+v", probeCalls, podGets, podCreates, createdPod, createdSecret, createdService, state.Intent, f.r.Status)
	}
	if state.Intent.LearnerUID != "created-learner-uid" || state.Intent.LearnerWAL != "" || len(state.Learners) != 1 || state.Learners[0].Target.PodUID != "created-learner-uid" || f.r.Status.Phase == "Blocked" {
		t.Fatalf("first reconcile did not journal the created learner UID safely: intent=%+v learners=%+v status=%+v", state.Intent, state.Learners, f.r.Status)
	}

	wal := strings.Repeat("a", 64)
	f.api.learnerID = "learner"
	f.api.membership["learner"] = network.MembershipStatus{NodeID: "learner", ClusterID: "source", ConfigID: 3, Voters: []quepaxa.NodeID{"n1", "n2", "learner"}, WALIdentity: wal, Voting: true}
	state = f.advance(t)
	if state.Intent.LearnerUID != "created-learner-uid" || state.Intent.LearnerWAL != wal || len(state.Learners) != 1 || state.Learners[0].Target.PodUID != "created-learner-uid" || state.Learners[0].Target.WALIdentity != wal || f.r.Status.Phase == "Blocked" {
		t.Fatalf("second reconcile did not authenticate and journal the created learner WAL: intent=%+v learners=%+v status=%+v", state.Intent, state.Learners, f.r.Status)
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
		{name: "credential retired", ledgerOK: true, secretUID: "credential-uid"},
		{name: "active generation voter", ledgerOK: true, activeVoter: true, wantRetained: true},
		{name: "unrelated in-flight intent", ledgerOK: true, inFlight: true},
		{name: "intent-owned incarnation", ledgerOK: true, inFlight: true, intentTarget: true, wantRetained: true},
		{name: "never-bound proof missing", neverBound: true, wantRetained: true},
		{name: "legacy history without ledger UID", ledgerOK: true, wantRetained: true},
		{name: "never-bound proof verified", neverBound: true, ledgerOK: true},
		{name: "replacement supersedes same secret", ledgerOK: true, secretUID: "credential-uid", replaced: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := learnerIncarnation{SourceClusterID: "source", OperationID: "op", LedgerUID: "compaction-ledger-uid", CredentialUID: "credential-uid", CredentialName: "learner-credentials", Target: compactionTarget("learner", "learner-uid", strings.Repeat("a", 64))}
			if tc.name == "legacy history without ledger UID" {
				entry.LedgerUID = ""
			}
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
			if tc.secretUID != "" && f.secretUIDs[entry.Target.Pod] != "" {
				t.Fatal("proven learner credential was not deleted before compaction")
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
	entry := learnerIncarnation{SourceClusterID: "source", OperationID: "op", LedgerUID: "compaction-ledger-uid", CredentialUID: "credential-uid", CredentialName: "learner-credentials", Target: compactionTarget("learner", "learner-uid", strings.Repeat("a", 64))}
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
	entry := learnerIncarnation{SourceClusterID: "source", OperationID: "op", LedgerUID: "compaction-ledger-uid", CredentialUID: "credential-uid", CredentialName: "learner-credentials", Target: compactionTarget("learner", "learner-uid", strings.Repeat("a", 64))}
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
		pod := fmt.Sprintf("learner-%d", i)
		entry := learnerIncarnation{SourceClusterID: "source", OperationID: fmt.Sprintf("op-%d", i), LedgerUID: "compaction-ledger-uid", CredentialUID: fmt.Sprintf("credential-%d", i), CredentialName: pod + "-credentials", Target: compactionTarget(pod, fmt.Sprintf("learner-uid-%d", i), strings.Repeat("a", 64))}
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
		pod := fmt.Sprintf("learner-%d", i)
		state.Learners = append(state.Learners, learnerIncarnation{SourceClusterID: "source", OperationID: fmt.Sprintf("op-%d", i), CredentialUID: fmt.Sprintf("credential-%d", i), CredentialName: pod + "-credentials", Target: FenceTarget{NodeID: pod, Pod: pod, PodUID: fmt.Sprintf("learner-uid-%d", i), WALIdentity: strings.Repeat("a", 64)}})
	}
	if _, err := f.c.compactLearnerHistory(t.Context(), state); err == nil || !strings.Contains(err.Error(), "cannot be compacted") {
		t.Fatalf("unreducible history error=%v", err)
	}
}

func TestCompactionRequiresEveryRunForPodUID(t *testing.T) {
	ctx := t.Context()
	canonical, err := fencing.ReplacementNodeID("pod-uid")
	if err != nil {
		t.Fatal(err)
	}
	entry := learnerIncarnation{SourceClusterID: "source", OperationID: "op", LedgerUID: "compaction-ledger-uid", CredentialUID: "cred", CredentialName: "learner-credentials", Target: FenceTarget{NodeID: canonical, Pod: "learner", PodUID: "pod-uid", WALIdentity: strings.Repeat("a", 64)}}
	ledger := &fenceLedger{StatefulSetUID: "sts-uid", Ledger: fencing.Ledger{SourceClusterID: "source"}}
	clean := fencing.Identity{SourceClusterID: "source", NodeID: canonical, PodName: "learner", PodUID: "pod-uid", CredentialUID: "cred", CredentialName: "learner-credentials", RunID: "clean"}
	unknown := fencing.Identity{SourceClusterID: "source", NodeID: canonical, PodName: "learner", PodUID: "pod-uid", CredentialUID: "cred", CredentialName: "learner-credentials", RunID: "unknown"}
	if err := ledger.Ledger.Register(clean); err != nil {
		t.Fatal(err)
	}
	clean.WALIdentity = entry.Target.WALIdentity
	if err := ledger.Ledger.Bind(clean); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Ledger.CloseNodes("op", "hash", nil); err != nil {
		t.Fatal(err)
	}
	cleanRun := ledger.Ledger.Runs["clean"]
	if err := ledger.Ledger.RecordDrain(fencing.DrainReceipt{Request: *cleanRun.Stop, WritersJoined: true}); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Ledger.RecordExit(*cleanRun.Stop); err != nil {
		t.Fatal(err)
	}
	ledger.Ledger.Runs[unknown.RunID] = fencing.Run{Identity: unknown}
	f := newCompactionFixture(t)
	f.ledger = ledger
	if err := f.c.verifyTerminatedCleanup(ctx, "sts-uid", entry); err == nil {
		t.Fatal("clean run hid an unresolved run for the same Pod UID")
	}
	cleanLedger := &fenceLedger{StatefulSetUID: "sts-uid", Ledger: fencing.Ledger{SourceClusterID: "source"}}
	id := fencing.Identity{SourceClusterID: "source", NodeID: canonical, PodName: "learner", PodUID: "pod-uid", CredentialUID: "cred", CredentialName: "learner-credentials", RunID: "first"}
	if err := cleanLedger.Ledger.Register(id); err != nil {
		t.Fatal(err)
	}
	id.WALIdentity = entry.Target.WALIdentity
	if err := cleanLedger.Ledger.Bind(id); err != nil {
		t.Fatal(err)
	}
	if err := cleanLedger.Ledger.CloseNodes("op", "hash", nil); err != nil {
		t.Fatal(err)
	}
	first := cleanLedger.Ledger.Runs["first"]
	if err := cleanLedger.Ledger.RecordDrain(fencing.DrainReceipt{Request: *first.Stop, WritersJoined: true}); err != nil {
		t.Fatal(err)
	}
	if err := cleanLedger.Ledger.RecordExit(*first.Stop); err != nil {
		t.Fatal(err)
	}
	first = cleanLedger.Ledger.Runs["first"]
	// Older valid ledgers can contain more than one settled run record for a
	// Pod UID. Verify cleanup accounts for every retained receipt without trying
	// to reopen admission after the current operation has closed the node.
	second := first
	second.Identity.RunID = "second"
	secondStop := *first.Stop
	secondStop.Identity.RunID = "second"
	second.Stop = &secondStop
	secondDrain := *first.Drain
	secondDrain.Request = secondStop
	second.Drain = &secondDrain
	cleanLedger.Ledger.Runs["second"] = second
	f.ledger = cleanLedger
	if err := f.c.verifyTerminatedCleanup(ctx, "sts-uid", entry); err != nil {
		t.Fatalf("all proven runs rejected: %v", err)
	}
}

func TestCompactionNeverAdoptsMissingOrRecreatedLedgerUID(t *testing.T) {
	for _, tc := range []struct {
		name       string
		historyUID string
		objectUID  string
	}{
		{name: "legacy missing UID", historyUID: "", objectUID: "compaction-ledger-uid"},
		{name: "copied ledger under replacement UID", historyUID: "compaction-ledger-uid", objectUID: "replacement-ledger-uid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newCompactionFixture(t)
			entry := learnerIncarnation{SourceClusterID: "source", OperationID: "op", LedgerUID: tc.historyUID, CredentialUID: "cred", CredentialName: "learner-credentials", Target: compactionTarget("learner", "pod-uid", strings.Repeat("a", 64))}
			f.ledger = terminatedLedger(t, []learnerIncarnation{entry})
			f.ledgerUID = tc.objectUID
			f.secretUIDs[entry.Target.Pod] = entry.CredentialUID
			state := validAutoControl()
			state.StatefulSetUID, state.Learners = "sts-uid", []learnerIncarnation{entry}
			changed, err := f.c.compactLearnerHistory(t.Context(), state)
			if err != nil || changed || len(state.Learners) != 1 || f.secretUIDs[entry.Target.Pod] != entry.CredentialUID {
				t.Fatalf("history was adopted/deleted: changed=%t err=%v learners=%+v credential=%q", changed, err, state.Learners, f.secretUIDs[entry.Target.Pod])
			}
		})
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
		pod := fmt.Sprintf("learner-%d", i)
		state.Learners = append(state.Learners, learnerIncarnation{SourceClusterID: "source", OperationID: fmt.Sprintf("op-%d", i), CredentialUID: fmt.Sprintf("credential-%d", i), CredentialName: pod + "-credentials", Target: FenceTarget{NodeID: pod, Pod: pod, PodUID: fmt.Sprintf("learner-uid-%d", i), WALIdentity: strings.Repeat("a", 64)}})
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
	if r.Status.Phase != "Blocked" || !strings.Contains(r.Status.Message, "cannot reserve its fallback generation fence") || f.mutations != 0 || len(loaded.Learners) != maxLearnerHistory {
		t.Fatalf("full journal mutated Kubernetes: status=%+v mutations=%d learners=%d", r.Status, f.mutations, len(loaded.Learners))
	}

	f.ledger = terminatedLedger(t, append([]learnerIncarnation(nil), loaded.Learners[:4]...))
	if handled, err := f.c.handleAutomaticLearner(ctx, r, loaded, version, object{}, &Resource{}); err != nil || !handled {
		t.Fatalf("compaction recovery handled=%t err=%v", handled, err)
	}
	if r.Status.Phase != "Recovering" || !strings.Contains(r.Status.Message, "compacted before replacement provisioning") || f.mutations != 0 || len(loaded.Learners) != maxLearnerHistory-4 {
		t.Fatalf("compaction did not free capacity before mutation: status=%+v mutations=%d learners=%d", r.Status, f.mutations, len(loaded.Learners))
	}
}

func TestLearnerProvisioningReservesWorstCaseGenerationScope(t *testing.T) {
	for _, tc := range []struct {
		name    string
		voters  int
		history int
	}{
		{name: "64 voters", voters: 64},
		{name: "63 voters plus two retained", voters: 63, history: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newCompactionFixture(t)
			r := autoClusterResource("root", "binding", "logical")
			state := validAutoControl()
			state.BindingUID, state.StatefulSetUID = "binding", "sts-uid"
			state.Voters = nil
			for i := range tc.voters {
				state.Voters = append(state.Voters, VoterPod{NodeID: quepaxa.NodeID(fmt.Sprintf("rhiza-%d", i)), Pod: fmt.Sprintf("rhiza-%d", i)})
			}
			for i := range tc.history {
				pod := fmt.Sprintf("history-%d", i)
				state.Learners = append(state.Learners, learnerIncarnation{SourceClusterID: "source", OperationID: fmt.Sprintf("history-%d", i), CredentialUID: fmt.Sprintf("cred-%d", i), CredentialName: pod + "-credentials", Target: FenceTarget{NodeID: pod, Pod: pod, PodUID: fmt.Sprintf("uid-%d", i), WALIdentity: strings.Repeat("a", 64)}})
			}
			state.Intent = &autoIntent{LearnerName: "candidate"}
			if handled, err := f.c.prepareLearnerProvisioning(t.Context(), r, state, nil); err != nil || !handled || r.Status.Phase != "Blocked" || !strings.Contains(r.Status.Message, "fallback generation fence") || f.mutations != 0 {
				t.Fatalf("handled=%t err=%v status=%+v mutations=%d", handled, err, r.Status, f.mutations)
			}
		})
	}
}

func TestLearnerRetirementReceiptCannotReplaceCooperativeLedger(t *testing.T) {
	f := newCompactionFixture(t)
	target := compactionTarget("learner", "uid", strings.Repeat("a", 64))
	request := FenceRequest{LogicalID: "logical", BindingUID: "binding", Namespace: "default", StatefulSet: "rhiza", StatefulSetUID: "sts-uid", SourceClusterID: "source", OperationID: "retire", Scope: ScopeVoter, Targets: []FenceTarget{target}}
	hash, err := requestHash(&request)
	if err != nil {
		t.Fatal(err)
	}
	entry := learnerIncarnation{SourceClusterID: "source", OperationID: request.OperationID, LedgerUID: f.ledgerUID, CredentialUID: "cred", CredentialName: "learner-credentials", Target: target, Retirement: &learnerRetirement{Request: request, Proof: FenceProof{OperationID: request.OperationID, SourceClusterID: "source", BindingUID: "binding", RequestHash: hash, ProofID: "external-proof", ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true}}}
	state := validAutoControl()
	state.StatefulSetUID, state.Learners = "sts-uid", []learnerIncarnation{entry}
	changed, err := f.c.compactLearnerHistory(t.Context(), state)
	if err != nil || changed || len(state.Learners) != 1 {
		t.Fatalf("external proof without the matching ledger compacted history: changed=%t err=%v learners=%d", changed, err, len(state.Learners))
	}
}

func TestNonSupervisedLearnerInventoryCapturesCredentialUID(t *testing.T) {
	ctx, api, c := learnerFixture(t)
	sts := learnerSTS(t)
	if _, err := c.ensureAutomaticLearner(ctx, sts, "rhiza", "op-1", "source", "l-a"); err != nil {
		t.Fatal(err)
	}
	state := validAutoControl()
	state.ActiveClusterID, state.StatefulSetUID = "source", "sts-uid"
	state.Intent = &autoIntent{LearnerName: "l-a", LearnerUID: str(nested(api.pod, "metadata", "uid")), Request: FenceRequest{OperationID: "op-1"}}
	entry, err := c.learnerInventoryEntry(ctx, state, sts, api.pod)
	if err != nil || entry.CredentialUID != "learner-secret-uid" {
		t.Fatalf("entry=%+v err=%v", entry, err)
	}
}

func TestCompactionRetiresLearnerCredential(t *testing.T) {
	f := newCompactionFixture(t)
	target := compactionTarget("learner", "pod-uid", strings.Repeat("a", 64))
	request := FenceRequest{LogicalID: "logical", BindingUID: "binding", Namespace: "default", StatefulSet: "rhiza", StatefulSetUID: "sts-uid", SourceClusterID: "source", OperationID: "retire", Scope: ScopeVoter, Targets: []FenceTarget{target}}
	hash, err := requestHash(&request)
	if err != nil {
		t.Fatal(err)
	}
	entry := learnerIncarnation{SourceClusterID: "source", OperationID: "op", CredentialUID: "credential-uid", CredentialName: "learner-credentials", Target: target, Retirement: &learnerRetirement{Request: request, Proof: FenceProof{OperationID: request.OperationID, SourceClusterID: "source", BindingUID: "binding", RequestHash: hash, ProofID: "proof", ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true}}}
	entry.LedgerUID = f.ledgerUID
	f.ledger = terminatedLedger(t, []learnerIncarnation{entry})
	f.secretUIDs[target.Pod] = entry.CredentialUID
	state := validAutoControl()
	state.StatefulSetUID, state.Learners = "sts-uid", []learnerIncarnation{entry}
	changed, err := f.c.compactLearnerHistory(t.Context(), state)
	if err != nil || !changed || len(state.Learners) != 0 || f.secretUIDs[target.Pod] != "" {
		t.Fatalf("credential retirement failed: changed=%t err=%v learners=%+v secretUID=%q", changed, err, state.Learners, f.secretUIDs[target.Pod])
	}
}

func TestCompactionKeepsReplacementCredentialWithDifferentUID(t *testing.T) {
	f := newCompactionFixture(t)
	target := compactionTarget("learner", "pod-uid", strings.Repeat("a", 64))
	request := FenceRequest{LogicalID: "logical", BindingUID: "binding", Namespace: "default", StatefulSet: "rhiza", StatefulSetUID: "sts-uid", SourceClusterID: "source", OperationID: "retire", Scope: ScopeVoter, Targets: []FenceTarget{target}}
	hash, err := requestHash(&request)
	if err != nil {
		t.Fatal(err)
	}
	entry := learnerIncarnation{SourceClusterID: "source", OperationID: "op", CredentialUID: "old-credential", CredentialName: "learner-credentials", Target: target, Retirement: &learnerRetirement{Request: request, Proof: FenceProof{OperationID: request.OperationID, SourceClusterID: "source", BindingUID: "binding", RequestHash: hash, ProofID: "proof", ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true}}}
	entry.LedgerUID = f.ledgerUID
	f.ledger = terminatedLedger(t, []learnerIncarnation{entry})
	f.secretUIDs[target.Pod] = "replacement-credential"
	state := validAutoControl()
	state.StatefulSetUID, state.Learners = "sts-uid", []learnerIncarnation{entry}
	changed, err := f.c.compactLearnerHistory(t.Context(), state)
	if err != nil || !changed || len(state.Learners) != 0 || f.secretUIDs[target.Pod] != "replacement-credential" {
		t.Fatalf("replacement credential was removed: changed=%t err=%v learners=%+v secretUID=%q", changed, err, state.Learners, f.secretUIDs[target.Pod])
	}
}

type compactionFixture struct {
	c          *Controller
	ledger     *fenceLedger
	ledgerUID  string
	podUIDs    map[string]string
	secretUIDs map[string]string
	mutations  int
}

func newCompactionFixture(t *testing.T) *compactionFixture {
	t.Helper()
	f := &compactionFixture{c: autoController(autoBucket(t)), ledgerUID: "compaction-ledger-uid", podUIDs: map[string]string{}, secretUIDs: map[string]string{}}
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("token"), 0o600); err != nil {
		t.Fatal(err)
	}
	f.c.Kube = &Kubernetes{Namespace: "default", TokenFile: token}
	if err := f.c.Kube.ConfigureDeletion("", "default"); err != nil {
		t.Fatal(err)
	}
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
			if q.Method == http.MethodDelete {
				uid := f.secretUIDs[name]
				if uid == "" {
					http.NotFound(w, q)
					return
				}
				var options object
				_ = json.NewDecoder(q.Body).Decode(&options)
				if str(nested(options, "preconditions", "uid")) != uid {
					w.WriteHeader(http.StatusConflict)
					return
				}
				delete(f.secretUIDs, name)
				w.WriteHeader(http.StatusOK)
				return
			}
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
			name := path.Base((&FencingExecutor{Kube: f.c.Kube}).ledgerPath("sts-uid", "source"))
			_ = json.NewEncoder(w).Encode(object{"metadata": object{"name": name, "namespace": f.c.Kube.Namespace, "uid": f.ledgerUID}, "data": object{"ledger.json": string(jsonBytes(f.ledger))}})
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
		id := fencing.Identity{SourceClusterID: entry.SourceClusterID, NodeID: entry.Target.NodeID, PodName: entry.Target.Pod, PodUID: entry.Target.PodUID, CredentialName: entry.CredentialName, CredentialUID: entry.CredentialUID, RunID: fmt.Sprintf("run-%d", i)}
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
