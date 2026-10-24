package operator

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCompactionRetainsExactCanonicalChildJournalReference(t *testing.T) {
	for _, scenario := range []string{"exact child", "changed child spec", "missing intent checkpoint", "confirmed child not found"} {
		t.Run(scenario, func(t *testing.T) {
			unresolved := scenario == "changed child spec" || scenario == "missing intent checkpoint"
			f := newCompactionFixture(t)
			f.fenceReady = true
			entry := learnerIncarnation{
				SourceClusterID: "source", OperationID: "old-attempt", CredentialName: "old-learner-credentials", CredentialUID: "old-secret-uid",
				Target: FenceTarget{NodeID: "old-learner", Pod: "old-learner", PodUID: "old-pod-uid", WALIdentity: strings.Repeat("a", 64)},
			}
			state := validAutoControl()
			state.BindingUID, state.StatefulSetUID, state.ActiveClusterID = "binding-uid-1", "sts-uid", "source"
			state.Learners = []learnerIncarnation{entry}
			state.Intent = &autoIntent{RecoveryName: "recover", RecoveryUID: "child-uid", SpecHash: "pending"}
			if scenario == "missing intent checkpoint" || scenario == "confirmed child not found" {
				state.Intent.RecoveryUID, state.Intent.SpecHash = "", ""
			}

			child := Resource{Metadata: object{
				"name": "recover", "uid": "child-uid",
				"annotations": object{automaticOwner: state.BindingUID, automaticLogical: f.resource.Spec.LogicalID},
			}, Spec: Spec{SourceClusterID: "source", Membership: &MembershipSpec{
				OperationID: "recovery-operation", ReplacementPod: "new-learner", ReplacementSecret: "new-learner-credentials",
			}}}
			add := &MembershipAddJournal{
				OperationID: membershipOperationID(&child, "add"), ExpectedConfigID: 4, MemberID: "old-learner",
				WALIdentity: entry.Target.WALIdentity, RequestHash: strings.Repeat("b", 64), SecretUID: entry.CredentialUID, SecretResourceVer: "7",
			}
			child.Status.Membership = &MembershipStatus{Add: add}
			state.Intent.SpecHash = hashJSON(child.Spec)
			if scenario == "changed child spec" {
				child.Spec.SourceClusterID = "different-source"
			}
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if scenario == "confirmed child not found" {
					http.NotFound(w, r)
					return
				}
				if r.Method != http.MethodGet || r.URL.Path != f.c.resourcePath("recover") {
					t.Errorf("unexpected API request during protected compaction: %s %s", r.Method, r.URL.Path)
					http.NotFound(w, r)
					return
				}
				_ = json.NewEncoder(w).Encode(child)
			}))
			t.Cleanup(server.Close)
			f.c.Kube.BaseURL, f.c.Kube.Client = server.URL, server.Client()

			changed, err := f.c.compactLearnerHistory(t.Context(), f.resource, state)
			if unresolved {
				if err == nil || changed || len(state.Learners) != 1 {
					t.Fatalf("unresolved child authorized compaction: changed=%t err=%v learners=%+v", changed, err, state.Learners)
				}
				return
			}
			if scenario == "confirmed child not found" {
				if err != nil || !changed || len(state.Learners) != 0 {
					t.Fatalf("confirmed missing child did not retain existing compaction behavior: changed=%t err=%v learners=%+v", changed, err, state.Learners)
				}
				return
			}
			if err != nil || changed || len(state.Learners) != 1 {
				t.Fatalf("journal-referenced standalone history was compacted: changed=%t err=%v learners=%+v", changed, err, state.Learners)
			}
		})
	}
}
