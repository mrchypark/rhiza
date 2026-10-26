package operator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

func TestSameOrdinalLearnerReservesGenerationCapacityBeforeAssignment(t *testing.T) {
	for _, tc := range []struct {
		name            string
		voterCount      int
		retainedCount   int
		retainCandidate bool
		candidateUID    string
		wantAssigned    bool
	}{
		{name: "64 voters plus candidate", voterCount: 64},
		{name: "64 retained union plus candidate", voterCount: 63, retainedCount: 1},
		{name: "63 voters plus candidate fits", voterCount: 63, wantAssigned: true},
		{name: "candidate UID differs from retained history", voterCount: 63, retainedCount: 1, retainCandidate: true, candidateUID: "next-pod-uid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			bucket := objstore.NewInMemBucket()
			c := autoController(bucket)
			r := autoClusterResource("cluster", "binding-uid-1", "logical")
			r.Spec.VoterCount = tc.voterCount
			state := validAutoControl()
			state.StatefulSetUID = "sts-uid-1"
			state.Voters = make([]VoterPod, tc.voterCount)
			state.Identities = make(map[quepaxa.NodeID]FenceTarget)
			for n := range tc.voterCount {
				nodeID := fmt.Sprintf("node-%02d", n)
				podName := fmt.Sprintf("rhiza-%d", n)
				state.Voters[n] = VoterPod{NodeID: quepaxa.NodeID(nodeID), Pod: podName}
				state.Identities[quepaxa.NodeID(nodeID)] = FenceTarget{NodeID: nodeID, Pod: podName, PodUID: fmt.Sprintf("pod-%02d", n), WALIdentity: fmt.Sprintf("wal-%02d", n)}
			}
			old := FenceTarget{NodeID: "node-00", Pod: "rhiza-0", PodUID: "old-pod-uid", WALIdentity: "old-wal"}
			state.Identities[quepaxa.NodeID(old.NodeID)] = old
			if tc.retainedCount != 0 {
				retainedNode, retainedPod, retainedUID := "retained-learner", "retained-pod", "retained-pod-uid"
				if tc.retainCandidate {
					retainedNode = "inc-" + shortID(old.NodeID+":recover-op:new-pod-uid")
					retainedPod, retainedUID = old.Pod, "new-pod-uid"
				}
				state.Learners = []learnerIncarnation{{SourceClusterID: state.ActiveClusterID, OperationID: "prior-op", CredentialName: "prior-credentials", CredentialUID: "prior-secret-uid",
					Target: FenceTarget{NodeID: retainedNode, Pod: retainedPod, PodUID: retainedUID, WALIdentity: "retained-wal"}}}
			}
			request := FenceRequest{LogicalID: r.Spec.LogicalID, BindingUID: state.BindingUID, Namespace: "default", StatefulSet: "rhiza",
				StatefulSetUID: state.StatefulSetUID, SourceClusterID: state.ActiveClusterID, OperationID: "recover-op", Scope: ScopeVoter, Targets: []FenceTarget{old}}
			requestDigest, err := requestHash(&request)
			if err != nil {
				t.Fatal(err)
			}
			state.Intent = &autoIntent{Request: request, Proof: &FenceProof{OperationID: request.OperationID, SourceClusterID: request.SourceClusterID,
				BindingUID: request.BindingUID, RequestHash: requestDigest, ProofID: "proof", ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true},
				Failed: quepaxa.NodeID(old.NodeID), Voters: append([]VoterPod(nil), state.Voters...), RecoveryName: "recover", LearnerName: "recover-learner"}
			historyBefore := hashJSON(state.Learners)
			candidateState := *state
			candidateIntent := *state.Intent
			candidateUID := tc.candidateUID
			if candidateUID == "" {
				candidateUID = "new-pod-uid"
			}
			candidateIntent.LearnerName = "inc-" + shortID(old.NodeID+":"+request.OperationID+":"+candidateUID)
			candidateState.Intent = &candidateIntent
			targets := worstCaseGenerationTargets(&candidateState)
			if tc.wantAssigned && targets != fenceTargetCapacity {
				t.Fatalf("within-capacity fixture does not hit the boundary: targets=%d capacity=%d", targets, fenceTargetCapacity)
			}
			if !tc.wantAssigned && targets <= fenceTargetCapacity {
				t.Fatalf("fixture does not exceed the generation fence capacity: targets=%d capacity=%d", targets, fenceTargetCapacity)
			}
			if err := c.saveAuto(ctx, r, state, nil); err != nil {
				t.Fatal(err)
			}
			stored, version, err := c.loadAuto(ctx, c.autoKey(r))
			if err != nil {
				t.Fatal(err)
			}

			sts := learnerSTS(t)
			asObject(sts["metadata"])["uid"] = state.StatefulSetUID
			asObject(nested(sts, "spec", "template"))["metadata"] = object{"annotations": object{fencingProtocolAnnotation: "1", fencingRootAnnotation: state.BindingUID}}
			pod := supervisedTestPod()
			podMeta := asObject(pod["metadata"])
			candidateUID = "new-pod-uid"
			if tc.candidateUID != "" {
				candidateUID = tc.candidateUID
			}
			podMeta["name"], podMeta["uid"] = old.Pod, candidateUID
			podMeta["annotations"] = object{fencingProtocolAnnotation: "1", fencingRootAnnotation: state.BindingUID}
			podMeta["ownerReferences"] = []any{object{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "rhiza", "uid": state.StatefulSetUID, "controller": true}}
			var secret object
			secretCreates, secretDeletes, statusWrites, podGets := 0, 0, 0, 0
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case req.Method == http.MethodGet && req.URL.Path == c.corePath("pods", old.Pod):
					podGets++
					_ = json.NewEncoder(w).Encode(pod)
				case req.Method == http.MethodGet && req.URL.Path == c.corePath("secrets", "rhiza-peer-credentials"):
					_ = json.NewEncoder(w).Encode(object{"data": object{"RHIZA_PEER_TOKENS": base64.StdEncoding.EncodeToString([]byte(`{}`))}})
				case req.Method == http.MethodGet && req.URL.Path == c.corePath("secrets", sameOrdinalSecretName(state.BindingUID, "new-pod-uid")):
					if secret == nil {
						w.WriteHeader(http.StatusNotFound)
						return
					}
					_ = json.NewEncoder(w).Encode(secret)
				case req.Method == http.MethodPost && req.URL.Path == c.corePath("secrets", ""):
					secretCreates++
					if err := json.NewDecoder(req.Body).Decode(&secret); err != nil {
						t.Errorf("decode learner Secret: %v", err)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					meta := asObject(secret["metadata"])
					meta["uid"], meta["resourceVersion"] = "new-secret-uid", "2"
					w.WriteHeader(http.StatusCreated)
					_ = json.NewEncoder(w).Encode(secret)
				case req.Method == http.MethodDelete:
					secretDeletes++
					w.WriteHeader(http.StatusNotFound)
				case req.Method == http.MethodPut && req.URL.Path == c.clusterPath("cluster")+"/status":
					statusWrites++
					_ = json.NewEncoder(w).Encode(r)
				default:
					http.NotFound(w, req)
				}
			}))
			t.Cleanup(server.Close)
			token := filepath.Join(t.TempDir(), "token")
			if err := os.WriteFile(token, []byte("test-token"), 0600); err != nil {
				t.Fatal(err)
			}
			c.Kube = &Kubernetes{BaseURL: server.URL, Namespace: "default", TokenFile: token, Client: server.Client()}

			handled, err := c.handleAutomaticLearner(ctx, r, stored, version, sts, nil)
			if err != nil {
				t.Fatalf("handle automatic learner: %v", err)
			}
			final, _, err := c.loadAuto(ctx, c.autoKey(r))
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantAssigned {
				if !handled || r.Status.Phase != "Recovering" || secretCreates != 1 || statusWrites != 1 || final.Intent.LearnerUID == "" || final.Intent.LearnerSecretUID == "" || len(final.Learners) != tc.retainedCount+1 {
					t.Fatalf("within-capacity same-ordinal candidate was not assigned: handled=%t phase=%q secretCreates=%d statusWrites=%d learnerUID=%q secretUID=%q history=%d",
						handled, r.Status.Phase, secretCreates, statusWrites, final.Intent.LearnerUID, final.Intent.LearnerSecretUID, len(final.Learners))
				}
				return
			}
			if !handled || r.Status.Phase != "Blocked" || secretCreates != 0 || secretDeletes != 0 || statusWrites != 1 || final.Intent.LearnerUID != "" || final.Intent.LearnerSecretUID != "" || len(final.Learners) != tc.retainedCount {
				t.Fatalf("over-capacity same-ordinal candidate was assigned: handled=%t phase=%q secretCreates=%d statusWrites=%d learnerUID=%q secretUID=%q history=%d",
					handled, r.Status.Phase, secretCreates, statusWrites, final.Intent.LearnerUID, final.Intent.LearnerSecretUID, len(final.Learners))
			}
			if tc.retainCandidate && (podGets != 1 || hashJSON(final.Learners) != historyBefore) {
				t.Fatalf("candidate UID mismatch changed retained history or performed extra Pod lookup: podGets=%d history=%d", podGets, len(final.Learners))
			}
		})
	}
}
