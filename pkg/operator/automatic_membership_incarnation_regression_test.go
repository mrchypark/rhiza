package operator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// These regressions enter through reconcileAll and use the production
// automatic child builder. The fixture's Kube API and survivor HTTP endpoints
// are the same ones used by the existing membership controller tests.
func automaticMembershipIncarnationFixture(t *testing.T, removedPod string, recreatedStatefulSet bool) (context.Context, *operatorAPI, *Controller, *Resource) {
	t.Helper()
	ctx, r, api, c := membershipFixture(t)
	// The fixture intentionally has no RhizaCluster list endpoint. Keep the
	// existing initial-root 404 behavior while still running the production
	// child reconciliation path in reconcileAll.
	c.Automatic = false

	statefulSetUID := str(nested(api.sts, "metadata", "uid"))
	if recreatedStatefulSet {
		asObject(api.sts["metadata"])["uid"] = "recreated-sts-uid"
	}
	cluster := autoClusterResource("cluster", "binding-uid", "logical")
	state := validAutoControl()
	state.BindingUID = "binding-uid"
	state.BindingHash = autoBinding(cluster)
	state.BindingVersion = autoBindingVersion
	state.StatefulSetUID = statefulSetUID
	state.ActiveClusterID = "source"
	state.Voters = []VoterPod{{NodeID: "n1", Pod: "n1"}, {NodeID: "n2", Pod: "n2"}, {NodeID: "n3", Pod: "n3"}}
	old := FenceTarget{NodeID: "n3", Pod: removedPod, PodUID: "old-pod-uid", WALIdentity: strings.Repeat("b", 64)}
	state.Identities[quepaxa.NodeID(old.NodeID)] = old
	request := FenceRequest{
		LogicalID: "logical", BindingUID: state.BindingUID, Namespace: "default",
		StatefulSet: "rhiza", StatefulSetUID: statefulSetUID,
		SourceClusterID: "source", OperationID: "automatic-replacement-7", Scope: ScopeVoter,
		Targets: []FenceTarget{old},
	}
	fenceCalls := 0
	proof, err := (&countingFencer{count: &fenceCalls}).Fence(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	intent := &autoIntent{
		Request: request, Proof: proof, Policy: AutoPolicy{FailureGraceSeconds: 30},
		Durability: "before-ack", Voters: append([]VoterPod(nil), state.Voters...), Failed: "n3",
		RecoveryName: "recover", RecoveryUID: str(r.Metadata["uid"]),
	}
	if removedPod == "learner" {
		intent.LearnerUID, intent.LearnerPodName = "learner-uid", "learner"
		intent.LearnerName = "inc-" + shortID(string(intent.Failed)+":"+request.OperationID+":"+intent.LearnerUID)
		intent.LearnerSecretName = sameOrdinalSecretName(state.BindingUID, intent.LearnerUID)
		intent.LearnerSecretUID, intent.LearnerSecretRV = "secret-uid", "1"
		secretToken := "same-ordinal-peer-token"
		member := voterMember("source", intent.LearnerName, secretToken)
		secretData := object{
			"member":           base64.StdEncoding.EncodeToString(jsonBytes(member)),
			"RHIZA_PEER_TOKEN": base64.StdEncoding.EncodeToString([]byte(secretToken)),
		}
		intent.LearnerSecretHash = hashJSON(secretData)
		api.membershipSecrets[intent.LearnerSecretName] = object{
			"immutable": true,
			"metadata": object{"name": intent.LearnerSecretName, "uid": intent.LearnerSecretUID, "resourceVersion": intent.LearnerSecretRV,
				"annotations": object{ownerAnnotation: request.OperationID}},
			"data": secretData,
		}
		api.learnerID = intent.LearnerName
		api.membership[intent.LearnerName] = network.MembershipStatus{NodeID: quepaxa.NodeID(intent.LearnerName), ClusterID: "source", WALIdentity: "new-learner-wal"}
		pod := supervisedTestPod()
		pod["status"] = api.replacement["status"]
		container := asObject(list(nested(api.replacement, "spec", "containers"))[0])
		testContainer := asObject(list(nested(pod, "spec", "containers"))[0])
		testContainer["name"], testContainer["ports"] = "rhiza", container["ports"]
		podMetadata := asObject(pod["metadata"])
		podMetadata["name"], podMetadata["uid"] = intent.LearnerPodName, intent.LearnerUID
		podMetadata["annotations"] = object{fencingProtocolAnnotation: "1", fencingRootAnnotation: state.BindingUID}
		podMetadata["ownerReferences"] = []any{object{"apiVersion": "apps/v1", "uid": statefulSetUID, "name": "rhiza", "kind": "StatefulSet", "controller": true}}
		api.replacement = pod
	} else {
		// The second regression models the standalone learner path: the failed
		// Pod is absent, while the durable automatic REMOVE intent still names
		// the old StatefulSet incarnation.
		intent.LearnerName, intent.LearnerUID = "learner", "learner-uid"
	}
	state.Intent = intent
	desired := automaticRecoveryChildSpec(cluster, state, intent)
	intent.SpecHash = hashJSON(desired)
	if intent.LearnerPodName != "" {
		assignment, err := sameOrdinalAssignmentDigest(state, intent)
		if err != nil {
			t.Fatalf("build canonical same-ordinal assignment: %v", err)
		}
		intent.LearnerAssignmentID = assignment
	}
	if err := c.saveAuto(ctx, cluster, state, nil); err != nil {
		t.Fatalf("persist exact automatic intent: %v", err)
	}

	r.Metadata["annotations"] = object{automaticOwner: state.BindingUID, automaticLogical: cluster.Spec.LogicalID}
	r.Spec = desired
	api.replacement["metadata"].(object)["annotations"] = object{
		fencingProtocolAnnotation: "1", fencingRootAnnotation: state.BindingUID,
	}
	api.replacement["metadata"].(object)["ownerReferences"] = []any{object{
		"apiVersion": "apps/v1", "uid": statefulSetUID, "name": "rhiza", "kind": "StatefulSet", "controller": true,
	}}
	return ctx, api, c, r
}

func TestAutomaticSameOrdinalReplacementRejectedByOldUID(t *testing.T) {
	ctx, api, c, child := automaticMembershipIncarnationFixture(t, "learner", false)
	for range 2 {
		if err := c.reconcileAll(ctx); err != nil {
			t.Fatalf("reconcile automatic membership child: %v", err)
		}
	}
	if len(api.changes) != 1 || api.changes[0].Remove != "n3" || api.changes[0].Fence == nil ||
		api.changes[0].Fence.NodeID != "n3" || api.changes[0].Fence.WorkloadUID != "old-pod-uid" || api.changes[0].Fence.WALIdentity != strings.Repeat("b", 64) {
		t.Fatalf("canonical same-ordinal replacement did not dispatch the exact old-incarnation REMOVE: changes=%+v status=%+v", api.changes, child.Status)
	}

	api.membership["n1"] = membershipStatus("n1", 2, "n1", "n2")
	api.membership["n2"] = membershipStatus("n2", 2, "n1", "n2")
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatalf("journal learner ADD after durable REMOVE: %v", err)
	}
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatalf("dispatch learner ADD: %v", err)
	}
	if len(api.changes) != 3 || api.changes[2].Add == nil || string(api.changes[2].Add.ID) != api.learnerID {
		t.Fatalf("same-ordinal recovery did not progress through REMOVE confirmation and ADD: changes=%+v status=%+v", api.changes, child.Status)
	}
	if api.stsPuts != 0 {
		t.Fatalf("online membership recovery mutated StatefulSet: puts=%d", api.stsPuts)
	}
}

func TestOldAutomaticIntentDoesNotDispatchAgainstRecreatedStatefulSet(t *testing.T) {
	ctx, api, c, child := automaticMembershipIncarnationFixture(t, "missing-old-pod", true)
	for range 2 {
		if err := c.reconcileAll(ctx); err != nil {
			t.Fatalf("reconcile automatic membership child: %v", err)
		}
	}
	if len(api.changes) != 0 {
		t.Fatalf("old automatic intent dispatched %d membership changes against recreated StatefulSet; phase=%+v message=%q", len(api.changes), child.Status.Membership, child.Status.Message)
	}
	if child.Status.Phase != "Blocked" {
		t.Fatalf("recreated StatefulSet must block old automatic intent; phase=%q message=%q", child.Status.Phase, child.Status.Message)
	}
	if len(api.aborts) != 0 || api.stsPuts != 0 {
		t.Fatalf("recreated StatefulSet path mutated abort/workload state: aborts=%d stsPuts=%d", len(api.aborts), api.stsPuts)
	}
}

func TestAutomaticSameOrdinalRejectsUnboundReplacementIdentity(t *testing.T) {
	for _, scenario := range []string{"pod uid", "secret uid", "assignment"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, api, c, child := automaticMembershipIncarnationFixture(t, "learner", false)
			switch scenario {
			case "pod uid":
				asObject(api.replacement["metadata"])["uid"] = "unexpected-pod-uid"
			case "secret uid":
				secretName := child.Spec.Membership.ReplacementSecret
				asObject(api.membershipSecrets[secretName]["metadata"])["uid"] = "unexpected-secret-uid"
			case "assignment":
				root := autoClusterResource("cluster", "binding-uid", "logical")
				state, version, err := c.loadAuto(ctx, c.autoKey(root))
				if err != nil {
					t.Fatal(err)
				}
				state.Intent.LearnerAssignmentID = "wrong-assignment"
				if err := c.saveAuto(ctx, root, state, version); err != nil {
					t.Fatal(err)
				}
			}
			if err := c.reconcileAll(ctx); err != nil {
				t.Fatalf("reconcile unbound same-ordinal replacement: %v", err)
			}
			if child.Status.Phase != "Blocked" || len(api.changes) != 0 || len(api.aborts) != 0 {
				t.Fatalf("unbound %s identity reached membership mutation: status=%+v changes=%+v aborts=%+v", scenario, child.Status, api.changes, api.aborts)
			}
		})
	}
}

func TestAutomaticAbortRejectsRecreatedStatefulSet(t *testing.T) {
	ctx, api, c, child := automaticMembershipIncarnationFixture(t, "learner", true)
	root := autoClusterResource("cluster", "binding-uid", "logical")
	state, version, err := c.loadAuto(ctx, c.autoKey(root))
	if err != nil {
		t.Fatal(err)
	}
	state.Intent.Aborting = true
	child.Spec.Membership.AbortAddition = true
	state.Intent.SpecHash = hashJSON(child.Spec)
	assignment, err := sameOrdinalAssignmentDigest(state, state.Intent)
	if err != nil {
		t.Fatal(err)
	}
	state.Intent.LearnerAssignmentID = assignment
	if err := c.saveAuto(ctx, root, state, version); err != nil {
		t.Fatal(err)
	}
	child.Status.Membership = &MembershipStatus{Phase: "Aborting", Add: &MembershipAddJournal{
		OperationID: membershipOperationID(child, "add"), ExpectedConfigID: 1,
		ExpectedAbortSlot: 0, MemberID: quepaxa.NodeID(state.Intent.LearnerName), WALIdentity: "new-learner-wal",
	}}
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatalf("reconcile abort against recreated StatefulSet: %v", err)
	}
	if child.Status.Phase != "Blocked" || len(api.aborts) != 0 || len(api.changes) != 0 {
		t.Fatalf("stale abort crossed StatefulSet identity boundary: status=%+v aborts=%+v changes=%+v", child.Status.Membership, api.aborts, api.changes)
	}
}

func TestAutomaticMembershipAbortRetiresExactAddAfterLearnerReincarnation(t *testing.T) {
	for _, scenario := range []string{"next same-ordinal UID", "terminating journaled learner"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, api, c, child := automaticMembershipIncarnationFixture(t, "learner", false)
			root := autoClusterResource("cluster", "binding-uid", "logical")
			state, version, err := c.loadAuto(ctx, c.autoKey(root))
			if err != nil {
				t.Fatal(err)
			}
			state.Intent.Aborting = true
			child.Spec.Membership.AbortAddition = true
			state.Intent.SpecHash = hashJSON(child.Spec)
			assignment, err := sameOrdinalAssignmentDigest(state, state.Intent)
			if err != nil {
				t.Fatal(err)
			}
			state.Intent.LearnerAssignmentID = assignment
			if err := c.saveAuto(ctx, root, state, version); err != nil {
				t.Fatal(err)
			}
			if scenario == "next same-ordinal UID" {
				asObject(api.replacement["metadata"])["uid"] = "learner-u2-uid"
			} else {
				asObject(api.replacement["metadata"])["deletionTimestamp"] = "2026-10-24T00:00:00Z"
			}
			member, _, _, err := c.replacementMember(ctx, state.Intent.LearnerSecretName)
			if err != nil {
				t.Fatal(err)
			}
			member.WALIdentity = "new-learner-wal"
			add := network.MembershipChange{OperationID: membershipOperationID(child, "add"), ClusterID: "source", ExpectedConfigID: 1, Add: &member}
			child.Status.Membership = &MembershipStatus{Phase: "Aborting", Add: &MembershipAddJournal{
				OperationID: add.OperationID, ExpectedConfigID: add.ExpectedConfigID, ExpectedAbortSlot: add.ExpectedAbortSlot,
				MemberID: member.ID, WALIdentity: member.WALIdentity, RequestHash: hashJSON(add),
				SecretUID: state.Intent.LearnerSecretUID, SecretResourceVer: state.Intent.LearnerSecretRV,
			}}
			api.membership["n1"] = membershipStatus("n1", 1, "n1", "n2", "n3")
			api.membership["n2"] = membershipStatus("n2", 1, "n1", "n2", "n3")
			if err := c.reconcileMembership(ctx, child); err != nil {
				t.Fatalf("retire exact journaled ADD: %v", err)
			}
			if len(api.aborts) != 1 || api.aborts[0].OperationID != add.OperationID || len(api.changes) != 0 || child.Status.Membership.Phase != "Aborted" {
				t.Fatalf("exact prior ADD was not retired without authorizing a replacement UID: aborts=%+v changes=%+v status=%+v", api.aborts, api.changes, child.Status.Membership)
			}
			if api.stsPuts != 0 {
				t.Fatalf("membership retirement mutated StatefulSet: puts=%d", api.stsPuts)
			}
		})
	}
}

func TestAutomaticMembershipAbortWithoutJournalAcknowledgesBeforeFencingLearner(t *testing.T) {
	ctx, api, c, child := automaticMembershipIncarnationFixture(t, "learner", false)
	root := autoClusterResource("cluster", "binding-uid", "logical")
	state, version, err := c.loadAuto(ctx, c.autoKey(root))
	if err != nil {
		t.Fatal(err)
	}
	state.Intent.Aborting = true
	state.Intent.LearnerWAL = "new-learner-wal"
	state.Learners = []learnerIncarnation{{
		SourceClusterID: state.ActiveClusterID, OperationID: state.Intent.Request.OperationID,
		CredentialName: state.Intent.LearnerSecretName, CredentialUID: state.Intent.LearnerSecretUID,
		Target: FenceTarget{NodeID: state.Intent.LearnerName, Pod: state.Intent.LearnerPodName, PodUID: state.Intent.LearnerUID, WALIdentity: state.Intent.LearnerWAL},
	}}
	child.Spec.Membership.AbortAddition = true
	state.Intent.SpecHash = hashJSON(child.Spec)
	assignment, err := sameOrdinalAssignmentDigest(state, state.Intent)
	if err != nil {
		t.Fatal(err)
	}
	state.Intent.LearnerAssignmentID = assignment
	if err := c.saveAuto(ctx, root, state, version); err != nil {
		t.Fatal(err)
	}
	// A newly observed child may have no status journal yet. The exact
	// canonical abort must still acknowledge the pre-ADD retirement.
	child.Status.Membership = nil
	api.replacement["metadata"].(object)["uid"] = "learner-u2-uid"
	api.membership["n1"] = membershipStatus("n1", 1, "n1", "n2", "n3")
	api.membership["n2"] = membershipStatus("n2", 1, "n1", "n2", "n3")
	if err := c.reconcileMembership(ctx, child); err != nil {
		t.Fatalf("acknowledge unjournaled ADD retirement: %v", err)
	}
	if child.Status.Phase != "Aborted" || child.Status.Membership == nil || child.Status.Membership.Add != nil {
		t.Fatalf("unjournaled ADD retirement was not acknowledged: status=%+v", child.Status)
	}
	if len(api.changes) != 0 || len(api.aborts) != 0 || api.stsPuts != 0 {
		t.Fatalf("pre-ADD retirement dispatched RPC or mutated StatefulSet: changes=%+v aborts=%+v stsPuts=%d", api.changes, api.aborts, api.stsPuts)
	}
	if err := c.reconcileMembership(ctx, child); err != nil || child.Status.Phase != "Aborted" || len(api.aborts) != 0 {
		t.Fatalf("repeated nil-journal abort reconciliation was not idempotent: phase=%q err=%v aborts=%+v", child.Status.Phase, err, api.aborts)
	}
	state, version, err = c.loadAuto(ctx, c.autoKey(root))
	if err != nil {
		t.Fatal(err)
	}
	if !state.Intent.Aborting || state.Intent.LearnerUID != "learner-uid" || state.Intent.LearnerName != "inc-"+shortID("n3:automatic-replacement-7:learner-uid") {
		t.Fatalf("canonical learner identity rotated before its fencing proof: intent=%+v", state.Intent)
	}

	// The parent may rotate the logical learner identity only after it obtains
	// a proof for this exact old Pod UID and WAL incarnation.
	observedFence := false
	c.Fencer = membershipRegressionFencer(func(_ context.Context, request FenceRequest) (*FenceProof, error) {
		if !state.Intent.Aborting || state.Intent.LearnerUID != "learner-uid" || len(request.Targets) != 1 ||
			request.Targets[0] != (FenceTarget{NodeID: state.Intent.LearnerName, Pod: "learner", PodUID: "learner-uid", WALIdentity: "new-learner-wal"}) {
			t.Fatalf("parent did not fence the still-canonical old learner before rotation: intent=%+v request=%+v", state.Intent, request)
		}
		observedFence = true
		count := 0
		return (&countingFencer{count: &count}).Fence(ctx, request)
	})
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPut && strings.HasSuffix(req.URL.Path, "/rhizaclusters/cluster/status") {
			w.WriteHeader(http.StatusOK)
			return
		}
		api.serve(w, req)
	}))
	t.Cleanup(server.Close)
	c.Kube.BaseURL, c.Kube.Client = server.URL, server.Client()
	child.Status.Membership.Phase = "Aborted"
	if handled, err := c.handleAutomaticLearner(ctx, root, state, version, api.sts, child); err != nil || !handled {
		t.Fatalf("parent did not finish exact learner retirement after fencing: handled=%v err=%v", handled, err)
	}
	if !observedFence || state.Intent.Aborting || state.Intent.LearnerUID != "" || state.Intent.LearnerAttempt != 1 {
		t.Fatalf("parent failed to rotate only after observing old-UID fencing: observed=%v intent=%+v", observedFence, state.Intent)
	}
}

func TestRetiredSameOrdinalLearnerFallsBackOnlyWithExactRetirementProof(t *testing.T) {
	for _, scenario := range []string{"exact retirement", "missing retirement proof", "mismatched retirement proof"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, api, c, child := automaticMembershipIncarnationFixture(t, "learner", false)
			root := autoClusterResource("cluster", "binding-uid", "logical")
			state, version, err := c.loadAuto(ctx, c.autoKey(root))
			if err != nil {
				t.Fatal(err)
			}
			state.Intent.Aborting = true
			state.Intent.LearnerWAL = "old-learner-wal"
			state.Learners = []learnerIncarnation{{
				SourceClusterID: state.ActiveClusterID, OperationID: state.Intent.Request.OperationID,
				CredentialName: state.Intent.LearnerSecretName, CredentialUID: state.Intent.LearnerSecretUID,
				Target: FenceTarget{NodeID: state.Intent.LearnerName, Pod: state.Intent.LearnerPodName, PodUID: state.Intent.LearnerUID, WALIdentity: state.Intent.LearnerWAL},
			}}
			child.Spec.Membership.AbortAddition = true
			state.Intent.SpecHash = hashJSON(child.Spec)
			assignment, err := sameOrdinalAssignmentDigest(state, state.Intent)
			if err != nil {
				t.Fatal(err)
			}
			state.Intent.LearnerAssignmentID = assignment
			if err := c.saveAuto(ctx, root, state, version); err != nil {
				t.Fatal(err)
			}
			state, version, err = c.loadAuto(ctx, c.autoKey(root))
			if err != nil {
				t.Fatal(err)
			}
			child.Status.Membership = &MembershipStatus{Phase: "Aborted"}
			observed := false
			c.Fencer = membershipRegressionFencer(func(ctx context.Context, request FenceRequest) (*FenceProof, error) {
				want := FenceTarget{NodeID: state.Intent.LearnerName, Pod: "learner", PodUID: "learner-uid", WALIdentity: "old-learner-wal"}
				if len(request.Targets) != 1 || request.Targets[0] != want {
					t.Fatalf("parent did not request the exact old learner fencing proof: %+v", request)
				}
				observed = true
				count := 0
				return (&countingFencer{count: &count}).Fence(ctx, request)
			})
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.Method == http.MethodPut && strings.HasSuffix(req.URL.Path, "/rhizaclusters/cluster/status") {
					w.WriteHeader(http.StatusOK)
					return
				}
				api.serve(w, req)
			}))
			t.Cleanup(server.Close)
			c.Kube.BaseURL, c.Kube.Client = server.URL, server.Client()
			if handled, err := c.handleAutomaticLearner(ctx, root, state, version, api.sts, child); err != nil || !handled || !observed {
				t.Fatalf("parent did not persist exact learner retirement proof: handled=%v observed=%v err=%v", handled, observed, err)
			}
			state, version, err = c.loadAuto(ctx, c.autoKey(root))
			if err != nil {
				t.Fatal(err)
			}
			if str(nested(api.replacement, "metadata", "uid")) != "learner-uid" || nested(api.replacement, "metadata", "deletionTimestamp") != nil {
				t.Fatal("fixture must retain the exact, non-terminating U1 Pod object after fencing")
			}
			switch scenario {
			case "missing retirement proof":
				state.Learners[0].Retirement = nil
			case "mismatched retirement proof":
				state.Learners[0].Retirement.Proof.RequestHash = strings.Repeat("e", 64)
			}
			historyAfterMutation := hashJSON(state.Learners)
			if scenario == "exact retirement" {
				// Exercise the next complete parent reconcile through the real
				// learner provisioner, while keeping the retired U1 Pod live.
				learnerKube := newLearnerAPI(t)
				t.Cleanup(learnerKube.Close)
				sts := learnerSTS(t)
				asObject(sts["metadata"])["uid"] = state.StatefulSetUID
				template := asObject(nested(sts, "spec", "template"))
				template["metadata"] = object{"annotations": object{}}
				ctr := asObject(list(nested(sts, "spec", "template", "spec", "containers"))[0])
				ctr["command"] = []any{"/usr/local/bin/rhiza-fencer"}
				ctr["args"] = []any{"supervise", "--", "/usr/local/bin/rhiza"}
				ctr["securityContext"] = object{"allowPrivilegeEscalation": false, "capabilities": object{"drop": []any{"ALL"}}}
				oldPod := supervisedTestPod()
				oldMeta := asObject(oldPod["metadata"])
				oldMeta["name"], oldMeta["uid"] = "learner", "learner-uid"
				oldMeta["annotations"] = object{fencingProtocolAnnotation: "1", fencingRootAnnotation: state.BindingUID}
				oldMeta["ownerReferences"] = []any{object{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "rhiza", "uid": state.StatefulSetUID, "controller": true}}
				stsWrites, statusWrites := 0, 0
				server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					if req.Method == http.MethodGet && req.URL.Path == "/api/v1/namespaces/default/pods/learner" {
						_ = json.NewEncoder(w).Encode(oldPod)
						return
					}
					if req.Method == http.MethodPut && strings.HasSuffix(req.URL.Path, "/rhizaclusters/cluster/status") {
						statusWrites++
						_ = json.NewEncoder(w).Encode(root)
						return
					}
					if req.Method == http.MethodPut && strings.HasSuffix(req.URL.Path, "/statefulsets/rhiza") {
						stsWrites++
						http.Error(w, "unexpected StatefulSet mutation", http.StatusInternalServerError)
						return
					}
					learnerKube.serve(w, req)
				}))
				t.Cleanup(server.Close)
				c.Kube = learnerKube.kube(t)
				c.Kube.BaseURL, c.Kube.Client = server.URL, server.Client()
				if handled, err := c.handleAutomaticLearner(ctx, root, state, version, sts, nil); err != nil || !handled {
					t.Fatalf("fresh parent reconcile did not prepare standalone learner: handled=%v err=%v", handled, err)
				}
				final, _, err := c.loadAuto(ctx, c.autoKey(root))
				if err != nil {
					t.Fatal(err)
				}
				if final.Intent.LearnerName != "recover-learner-1" || final.Intent.LearnerPodName != "" || final.Intent.LearnerUID != "learner-pod-uid" ||
					len(final.Learners) != 2 || stsWrites != 0 || statusWrites != 1 || learnerKube.createCount != 3 ||
					str(nested(learnerKube.secret, "metadata", "name")) != "recover-learner-1-credentials" {
					t.Fatalf("standalone provisioning changed canonical identity/history or StatefulSet: intent=%+v learners=%+v stsWrites=%d statusWrites=%d creates=%d secret=%v", final.Intent, final.Learners, stsWrites, statusWrites, learnerKube.createCount, learnerKube.secret)
				}
				retired := false
				newLearner := false
				for _, entry := range final.Learners {
					if entry.Target.NodeID == "inc-"+shortID("n3:automatic-replacement-7:learner-uid") {
						retired = entry.Target.Pod == "learner" && entry.Target.PodUID == "learner-uid" && entry.Retirement != nil
					}
					if entry.Target.NodeID == "recover-learner-1" {
						newLearner = entry.Target.Pod == "recover-learner-1" && entry.Target.PodUID == "learner-pod-uid"
					}
				}
				if !retired || !newLearner {
					t.Fatalf("retired U1 proof history or new standalone incarnation was not retained: %+v", final.Learners)
				}
				generation := FenceRequest{SourceClusterID: final.ActiveClusterID}
				if err := includeLearnerHistory(final, &generation); err != nil {
					t.Fatalf("include retained learner history in generation fence: %v", err)
				}
				if !learnerTargetCovered(final.Learners[0].Target, generation.Targets) || !learnerTargetCovered(final.Learners[1].Target, generation.Targets) {
					t.Fatalf("generation fence omitted retired U1 or standalone learner target: %+v", generation.Targets)
				}
				return
			}
			assigned, err := c.prepareSameOrdinalLearner(ctx, state, api.sts, child.Spec.Container)
			if err == nil || assigned || hashJSON(state.Learners) != historyAfterMutation {
				t.Fatalf("missing or mismatched canonical retirement did not fail closed: assigned=%v err=%v", assigned, err)
			}
		})
	}
}

func TestAutomaticMembershipAbortWithoutJournalAcknowledgesTerminatingLearner(t *testing.T) {
	for _, nilStatus := range []bool{false, true} {
		name := "status present"
		if nilStatus {
			name = "nil status"
		}
		t.Run(name, func(t *testing.T) {
			ctx, api, c, child := automaticMembershipIncarnationFixture(t, "learner", false)
			root := autoClusterResource("cluster", "binding-uid", "logical")
			state, version, err := c.loadAuto(ctx, c.autoKey(root))
			if err != nil {
				t.Fatal(err)
			}
			state.Intent.Aborting = true
			child.Spec.Membership.AbortAddition = true
			state.Intent.SpecHash = hashJSON(child.Spec)
			assignment, err := sameOrdinalAssignmentDigest(state, state.Intent)
			if err != nil {
				t.Fatal(err)
			}
			state.Intent.LearnerAssignmentID = assignment
			if err := c.saveAuto(ctx, root, state, version); err != nil {
				t.Fatal(err)
			}
			asObject(api.replacement["metadata"])["deletionTimestamp"] = "2026-10-24T00:00:00Z"
			if !nilStatus {
				child.Status.Membership = &MembershipStatus{Phase: "Aborting"}
			}
			api.membership["n1"] = membershipStatus("n1", 1, "n1", "n2", "n3")
			api.membership["n2"] = membershipStatus("n2", 1, "n1", "n2", "n3")
			if err := c.reconcileMembership(ctx, child); err != nil {
				t.Fatalf("acknowledge unjournaled ADD retirement for terminating learner: %v", err)
			}
			if child.Status.Phase != "Aborted" || child.Status.Membership == nil || child.Status.Membership.Add != nil ||
				len(api.changes) != 0 || len(api.aborts) != 0 || api.stsPuts != 0 {
				t.Fatalf("terminating pre-ADD learner did not reach RPC-free retirement acknowledgement: status=%+v changes=%+v aborts=%+v stsPuts=%d", child.Status, api.changes, api.aborts, api.stsPuts)
			}
		})
	}
}

type membershipRegressionFencer func(context.Context, FenceRequest) (*FenceProof, error)

func (f membershipRegressionFencer) Fence(ctx context.Context, request FenceRequest) (*FenceProof, error) {
	return f(ctx, request)
}

func TestAutomaticSameOrdinalAddUsesValidatedSecretSnapshot(t *testing.T) {
	ctx, api, c, child := automaticMembershipIncarnationFixture(t, "learner", false)
	root := autoClusterResource("cluster", "binding-uid", "logical")
	state, _, err := c.loadAuto(ctx, c.autoKey(root))
	if err != nil {
		t.Fatal(err)
	}
	child.Status.Membership = &MembershipStatus{Phase: "Removing", RemoveRequest: &network.MembershipChange{
		OperationID: membershipOperationID(child, "remove"), ClusterID: "source", ExpectedConfigID: 1,
		Remove: child.Spec.Membership.Remove, Fence: &network.MembershipFence{
			NodeID: child.Spec.Membership.Fence.NodeID, WALIdentity: child.Spec.Membership.Fence.WALIdentity,
			WorkloadUID: child.Spec.Membership.Fence.WorkloadUID, Confirmed: true, Evidence: child.Spec.Membership.Fence.Evidence,
		},
	}}
	current := membershipStatus("n1", 2, "n1", "n2")
	api.membership["n1"], api.membership["n2"] = current, membershipStatus("n2", 2, "n1", "n2")
	statuses := map[quepaxa.NodeID]network.MembershipStatus{"n1": current, "n2": api.membership["n2"]}
	pods, err := c.membershipPods(ctx, child.Spec.Membership.VoterPods)
	if err != nil {
		t.Fatal(err)
	}
	secretName := state.Intent.LearnerSecretName
	secretGets := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodGet && req.URL.Path == "/api/v1/namespaces/default/secrets/"+secretName {
			secretGets++
			var secret object
			if err := json.Unmarshal(jsonBytes(api.membershipSecrets[secretName]), &secret); err != nil {
				t.Errorf("copy canonical learner Secret: %v", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			if secretGets > 1 {
				asObject(secret["metadata"])["uid"] = "recreated-secret-uid"
				asObject(secret["metadata"])["resourceVersion"] = "2"
			}
			_ = json.NewEncoder(w).Encode(secret)
			return
		}
		api.serve(w, req)
	}))
	t.Cleanup(server.Close)
	c.Kube.BaseURL, c.Kube.Client = server.URL, server.Client()
	if err := c.reconcileMembershipAdd(ctx, child, api.sts, pods, statuses, controllerFixtureAdminToken, current); err != nil {
		t.Fatalf("journal same-ordinal learner ADD: %v", err)
	}
	if secretGets != 1 || child.Status.Membership.Add == nil || child.Status.Membership.Add.SecretUID != state.Intent.LearnerSecretUID || child.Status.Membership.Add.SecretResourceVer != state.Intent.LearnerSecretRV {
		t.Fatalf("same-ordinal ADD did not reuse the one validated Secret snapshot: secretGets=%d add=%+v canonicalUID=%q canonicalRV=%q", secretGets, child.Status.Membership.Add, state.Intent.LearnerSecretUID, state.Intent.LearnerSecretRV)
	}
}

func TestAutomaticMembershipMissingIntentFailsClosed(t *testing.T) {
	ctx, api, c, child := automaticMembershipIncarnationFixture(t, "missing-old-pod", false)
	root := autoClusterResource("cluster", "binding-uid", "logical")
	state, version, err := c.loadAuto(ctx, c.autoKey(root))
	if err != nil {
		t.Fatal(err)
	}
	state.Intent = nil
	if err := c.saveAuto(ctx, root, state, version); err != nil {
		t.Fatal(err)
	}
	if _, err := c.validateAutomaticMembershipIncarnation(ctx, child, api.sts); err == nil {
		t.Fatal("automatic membership without canonical intent was authorized")
	}
	for range 2 {
		if err := c.reconcileMembership(ctx, child); err != nil {
			t.Fatalf("reconcile child with missing intent: %v", err)
		}
	}
	if len(api.changes) != 0 || len(api.aborts) != 0 {
		t.Fatalf("membership mutation dispatched without canonical intent: changes=%+v aborts=%+v", api.changes, api.aborts)
	}
}

func TestAutomaticStandaloneReplacementRequiresCanonicalPodAndSecret(t *testing.T) {
	for _, scenario := range []string{"valid", "valid with recreated source ordinal", "recreated pod UID", "recreated secret UID", "missing template protocol", "credential snapshot", "abort with recreated source ordinal before ADD", "abort with recreated source ordinal after ADD", "abort with recreated ordinal wrong owner", "abort with recreated ordinal non-controller", "abort with recreated source ordinal after ADD wrong secret UID", "abort with recreated source ordinal after ADD wrong secret RV", "abort with recreated source ordinal after ADD wrong request hash"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, api, c, child := automaticMembershipIncarnationFixture(t, "missing-old-pod", false)
			root := autoClusterResource("cluster", "binding-uid", "logical")
			if scenario == "valid with recreated source ordinal" || strings.HasPrefix(scenario, "abort with recreated") {
				root.Spec.Automatic = true
			}
			state, version, err := c.loadAuto(ctx, c.autoKey(root))
			if err != nil {
				t.Fatal(err)
			}
			intent := state.Intent
			intent.Request.OperationID = intent.RecoveryName
			proofCalls := 0
			proof, err := (&countingFencer{count: &proofCalls}).Fence(ctx, intent.Request)
			if err != nil {
				t.Fatalf("build canonical standalone proof: %v", err)
			}
			intent.Proof = proof
			abortRecreatedOrdinal := strings.HasPrefix(scenario, "abort with recreated source ordinal") || strings.HasPrefix(scenario, "abort with recreated ordinal")
			if abortRecreatedOrdinal {
				intent.Aborting = true
			}
			child.Spec = automaticRecoveryChildSpec(root, state, intent)
			intent.SpecHash = hashJSON(child.Spec)
			secretName := learnerCredentialName(intent)
			memberToken := "standalone-peer-token"
			secretUID := "secret-uid"
			podUID := intent.LearnerUID
			if scenario == "recreated pod UID" {
				podUID = "recreated-pod-uid"
			} else if scenario == "recreated secret UID" {
				secretUID = "recreated-secret-uid"
			}
			member := voterMember(state.ActiveClusterID, intent.LearnerName, memberToken)
			secretData := object{
				"member":           base64.StdEncoding.EncodeToString(jsonBytes(member)),
				"RHIZA_PEER_TOKEN": base64.StdEncoding.EncodeToString([]byte(memberToken)),
			}
			secret := object{"immutable": true,
				"metadata": object{"name": secretName, "uid": secretUID, "resourceVersion": "1", "annotations": object{ownerAnnotation: intent.Request.OperationID}},
				"data":     secretData,
			}
			api.membershipSecret = secret
			api.membershipSecrets[secretName] = secret
			api.learnerID = intent.LearnerName
			api.membership[intent.LearnerName] = network.MembershipStatus{NodeID: quepaxa.NodeID(intent.LearnerName), ClusterID: state.ActiveClusterID, ConfigID: 1, WALIdentity: strings.Repeat("c", 64)}
			pod := supervisedTestPod()
			pod["status"] = api.replacement["status"]
			podCtr := asObject(list(nested(pod, "spec", "containers"))[0])
			oldCtr := asObject(list(nested(api.replacement, "spec", "containers"))[0])
			podCtr["name"], podCtr["ports"] = "rhiza", oldCtr["ports"]
			podCtr["env"] = []any{object{"name": "RHIZA_NODE_ID", "valueFrom": object{"fieldRef": object{"fieldPath": "metadata.name"}}}}
			pod["metadata"] = object{
				"name": intent.LearnerName, "uid": podUID,
				"ownerReferences": []any{object{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "rhiza", "uid": state.StatefulSetUID, "controller": false}},
				"annotations": object{fencingProtocolAnnotation: "1", fencingRootAnnotation: state.BindingUID,
					fencingCredentialAnnotation: secretUID, ownerAnnotation: intent.Request.OperationID},
				"labels": object{"rhiza.mrchypark.dev/automatic-learner": intent.LearnerName},
			}
			api.replacement = pod
			secretGets := 0
			templateMetadata := asObject(nested(api.sts, "spec", "template", "metadata"))
			if templateMetadata == nil {
				templateMetadata = object{}
				asObject(nested(api.sts, "spec", "template"))["metadata"] = templateMetadata
			}
			templateMetadata["annotations"] = object{fencingProtocolAnnotation: "1"}
			templateMetadata["labels"] = object{"app.kubernetes.io/name": "rhiza"}
			stsCtr := asObject(list(nested(api.sts, "spec", "template", "spec", "containers"))[0])
			stsCtr["env"] = append(list(stsCtr["env"]), object{"name": "RHIZA_NODE_ID", "valueFrom": object{"fieldRef": object{"fieldPath": "metadata.name"}}})
			state.Learners = []learnerIncarnation{{SourceClusterID: state.ActiveClusterID, OperationID: intent.Request.OperationID,
				CredentialName: secretName, CredentialUID: "secret-uid",
				Target: FenceTarget{NodeID: intent.LearnerName, Pod: intent.LearnerName, PodUID: intent.LearnerUID, WALIdentity: strings.Repeat("c", 64)},
			}}
			if scenario == "valid with recreated source ordinal" || abortRecreatedOrdinal {
				state.BindingHash = autoBinding(root)
			}
			if err := c.saveAuto(ctx, root, state, version); err != nil {
				t.Fatalf("persist standalone incarnation history: %v", err)
			}
			rootList := []ClusterResource{*root}
			sourceOrdinal := supervisedTestPod()
			asObject(sourceOrdinal["metadata"])["name"] = intent.Request.Targets[0].Pod
			asObject(sourceOrdinal["metadata"])["uid"] = "source-u2-uid"
			asObject(sourceOrdinal["metadata"])["annotations"] = object{fencingProtocolAnnotation: "1", fencingRootAnnotation: state.BindingUID}
			ownerController := true
			ownerName := "rhiza"
			if scenario == "abort with recreated ordinal wrong owner" {
				ownerName = "other"
			}
			if scenario == "abort with recreated ordinal non-controller" {
				ownerController = false
			}
			asObject(sourceOrdinal["metadata"])["ownerReferences"] = []any{object{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": ownerName, "uid": state.StatefulSetUID, "controller": ownerController}}
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.Method == http.MethodGet && req.URL.Path == "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/default/rhizaclusters" {
					_ = json.NewEncoder(w).Encode(struct {
						Items []ClusterResource `json:"items"`
					}{Items: rootList})
					return
				}
				if (scenario == "valid with recreated source ordinal" || abortRecreatedOrdinal) && req.Method == http.MethodGet && req.URL.Path == "/api/v1/namespaces/default/pods/"+intent.Request.Targets[0].Pod {
					_ = json.NewEncoder(w).Encode(sourceOrdinal)
					return
				}
				if scenario == "credential snapshot" && req.Method == http.MethodGet && req.URL.Path == "/api/v1/namespaces/default/secrets/"+secretName {
					secretGets++
					var snapshot object
					if err := json.Unmarshal(jsonBytes(secret), &snapshot); err != nil {
						t.Errorf("copy canonical standalone Secret: %v", err)
						w.WriteHeader(http.StatusInternalServerError)
						return
					}
					if secretGets > 1 {
						asObject(snapshot["metadata"])["uid"] = "recreated-secret-uid"
						asObject(snapshot["metadata"])["resourceVersion"] = "2"
					}
					_ = json.NewEncoder(w).Encode(snapshot)
					return
				}
				api.serve(w, req)
			}))
			t.Cleanup(server.Close)
			c.Kube.BaseURL, c.Kube.Client = server.URL, server.Client()

			if abortRecreatedOrdinal {
				api.membership["n1"] = membershipStatus("n1", 1, "n1", "n2", "n3")
				api.membership["n2"] = membershipStatus("n2", 1, "n1", "n2", "n3")
				child.Status.Membership = &MembershipStatus{Phase: "Aborting"}
				hasJournaledAdd := strings.Contains(scenario, "after ADD")
				if hasJournaledAdd {
					member, uid, rv, err := c.replacementMember(ctx, secretName)
					if err != nil {
						t.Fatalf("read exact standalone credentials: %v", err)
					}
					member.WALIdentity = strings.Repeat("c", 64)
					request := network.MembershipChange{OperationID: membershipOperationID(child, "add"), ClusterID: "source", ExpectedConfigID: 1, Add: &member}
					child.Status.Membership.Add = &MembershipAddJournal{OperationID: request.OperationID, ExpectedConfigID: request.ExpectedConfigID,
						MemberID: member.ID, WALIdentity: member.WALIdentity, RequestHash: hashJSON(request), SecretUID: uid, SecretResourceVer: rv}
					switch scenario {
					case "abort with recreated source ordinal after ADD wrong secret UID":
						child.Status.Membership.Add.SecretUID = "different-secret-uid"
					case "abort with recreated source ordinal after ADD wrong secret RV":
						child.Status.Membership.Add.SecretResourceVer = "2"
					case "abort with recreated source ordinal after ADD wrong request hash":
						child.Status.Membership.Add.RequestHash = strings.Repeat("d", 64)
					}
				}
				if err := c.reconcileMembership(ctx, child); err != nil {
					t.Fatalf("reconcile standalone abort with recreated source ordinal: %v", err)
				}
				if strings.Contains(scenario, "wrong secret") || strings.Contains(scenario, "wrong request hash") || strings.Contains(scenario, "wrong owner") || strings.Contains(scenario, "non-controller") {
					if child.Status.Phase != "Blocked" || len(api.aborts) != 0 || len(api.changes) != 0 || api.stsPuts != 0 {
						t.Fatalf("invalid standalone abort binding was not denied: status=%+v aborts=%+v changes=%+v stsPuts=%d", child.Status, api.aborts, api.changes, api.stsPuts)
					}
					return
				}
				if child.Status.Phase != "Aborted" || len(api.changes) != 0 || api.stsPuts != 0 {
					t.Fatalf("standalone abort did not reach safe retirement: status=%+v changes=%+v stsPuts=%d", child.Status, api.changes, api.stsPuts)
				}
				if strings.HasSuffix(scenario, "before ADD") && len(api.aborts) != 0 {
					t.Fatalf("pre-ADD standalone abort dispatched RPC: %+v", api.aborts)
				}
				if hasJournaledAdd && len(api.aborts) != 1 {
					t.Fatalf("journaled standalone ADD was not retired exactly once: %+v", api.aborts)
				}
				return
			}

			for range 2 {
				if err := c.reconcileMembership(ctx, child); err != nil {
					t.Fatalf("reconcile standalone removal: %v", err)
				}
			}
			if len(api.changes) != 1 || api.changes[0].Remove != "n3" {
				t.Fatalf("standalone fixture did not reach the confirmed REMOVE boundary: changes=%+v status=%+v", api.changes, child.Status)
			}
			if scenario == "missing template protocol" {
				asObject(nested(api.sts, "spec", "template", "metadata"))["annotations"] = object{}
			}
			api.membership["n1"] = membershipStatus("n1", 2, "n1", "n2")
			api.membership["n2"] = membershipStatus("n2", 2, "n1", "n2")
			addReconciles := 2
			if scenario == "credential snapshot" {
				addReconciles = 1
			}
			for range addReconciles {
				if err := c.reconcileMembership(ctx, child); err != nil {
					t.Fatalf("reconcile standalone replacement: %v", err)
				}
			}
			if scenario == "credential snapshot" {
				if secretGets != 1 || child.Status.Membership.Add == nil || child.Status.Membership.Add.SecretUID != secretUID || child.Status.Membership.Add.SecretResourceVer != "1" {
					t.Fatalf("standalone ADD did not reuse the one validated Secret snapshot: secretGets=%d add=%+v expectedUID=%q", secretGets, child.Status.Membership.Add, secretUID)
				}
				return
			}
			if scenario == "valid" || scenario == "valid with recreated source ordinal" {
				adds := 0
				for _, change := range api.changes {
					if change.Add != nil {
						adds++
						if string(change.Add.ID) != intent.LearnerName {
							t.Fatalf("membership ADD targeted noncanonical learner %q, want %q", change.Add.ID, intent.LearnerName)
						}
					}
				}
				if adds != 1 || child.Status.Membership == nil || child.Status.Membership.Phase == "Blocked" {
					t.Fatalf("valid canonical standalone incarnation did not dispatch ADD: status=%+v changes=%+v", child.Status, api.changes)
				}
				if scenario == "valid with recreated source ordinal" {
					if api.stsPuts != 0 || len(api.membershipSecrets) != 1 {
						t.Fatalf("recreated source ordinal received workload or credential mutation: stsPuts=%d secretCount=%d", api.stsPuts, len(api.membershipSecrets))
					}
					executor := &FencingExecutor{Kube: c.Kube, ArchiveBucket: c.Bucket, AutomaticControlPrefix: c.Prefix}
					request := fencing.Identity{SourceClusterID: state.ActiveClusterID, NodeID: intent.Request.Targets[0].Pod, PodName: intent.Request.Targets[0].Pod, PodUID: "source-u2-uid", RunID: "source-u2-run"}
					grant, err := executor.resolveStartupGrant(ctx, state.StatefulSetUID, request)
					if !errors.Is(err, fencing.ErrPending) || grant.Role == fencing.StartupRoleVoterActive || grant.Role == fencing.StartupRoleLearnerPendingRemove {
						t.Fatalf("unapproved StatefulSet replacement received startup authority: grant=%+v err=%v", grant, err)
					}
				}
				return
			}
			for _, change := range api.changes {
				if change.Add != nil {
					t.Fatalf("unbound standalone incarnation reached membership ADD: %+v", api.changes)
				}
			}
			if child.Status.Phase != "Blocked" || len(api.aborts) != 0 || api.stsPuts != 0 || !strings.Contains(child.Status.Message, "standalone learner incarnation") {
				t.Fatalf("unbound standalone incarnation did not fail closed: status=%+v aborts=%+v stsPuts=%d", child.Status, api.aborts, api.stsPuts)
			}
			if scenario == "missing template protocol" && len(api.changes) != 1 {
				t.Fatalf("missing StatefulSet template protocol replayed REMOVE or dispatched ADD: changes=%+v", api.changes)
			}
		})
	}
}
