package network

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestMembershipRepairCompletionUsesTypedAuthenticatedProbeAction(t *testing.T) {
	server := NewServer(nil, nil, "cluster", false, nil)
	t.Cleanup(server.Close)
	server.SetRecoveryArchive("secret", func(context.Context) error { return nil })
	request := validMembershipRepairCompletionRequest()
	called := false
	server.SetMembershipRepairCompletion(func(_ context.Context, got MembershipRepairCompletionRequest) (MembershipRepairCompletionResponse, error) {
		called = true
		if !reflect.DeepEqual(got, request) {
			t.Fatalf("completion request=%+v want=%+v", got, request)
		}
		digest, err := MembershipRepairCompletionRequestDigest(got)
		if err != nil {
			t.Fatal(err)
		}
		return MembershipRepairCompletionResponse{
			Version: got.Version, Action: got.Action, Challenge: got.Challenge, AttemptRevision: got.AttemptRevision,
			ScopeDigest: got.ScopeDigest, RequestDigest: digest,
			ExecutorNodeID: got.ExecutorNodeID, ExecutorPodUID: got.ExecutorPodUID, ExecutorRunID: got.ExecutorRunID,
			ExecutorWALIdentity: got.ExecutorWALIdentity, TerminalSlot: got.TerminalSlot, TerminalHash: got.TerminalHash, Outcome: "completed",
		}, nil
	})
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	call := func(token string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/recovery/probe", strings.NewReader(string(body)))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		server.ServeHTTP(response, req)
		return response
	}
	if got := call("wrong"); got.Code != http.StatusForbidden || called {
		t.Fatalf("unauthorized completion status=%d called=%v", got.Code, called)
	}
	response := call("secret")
	if response.Code != http.StatusOK || !called {
		t.Fatalf("completion status=%d called=%v body=%s", response.Code, called, response.Body.String())
	}
	decoded, err := StrictDecodeMembershipRepairCompletionResponse(response.Body.Bytes(), request, "secret")
	if err != nil || decoded.Outcome != "completed" {
		t.Fatalf("decode completion response=%+v err=%v", decoded, err)
	}
	changed := request
	changed.PhaseHash = strings.Repeat("77", 32)
	if _, err := StrictDecodeMembershipRepairCompletionResponse(response.Body.Bytes(), changed, "secret"); err == nil {
		t.Fatal("completion response for another request was accepted")
	}
	wrongAction := strings.Replace(string(body), `"action":"complete"`, `"action":"probe"`, 1)
	req := httptest.NewRequest(http.MethodPost, "/recovery/probe", strings.NewReader(wrongAction))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", "application/json")
	badAction := httptest.NewRecorder()
	server.ServeHTTP(badAction, req)
	if badAction.Code != http.StatusBadRequest {
		t.Fatalf("unknown action status=%d body=%s", badAction.Code, badAction.Body.String())
	}
}

func TestMembershipRepairCompletionStrictRequestRejectsMutationAndDuplicates(t *testing.T) {
	request := validMembershipRepairCompletionRequest()
	if err := ValidateMembershipRepairCompletionRequest(request); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*MembershipRepairCompletionRequest){
		"wrong action":            func(r *MembershipRepairCompletionRequest) { r.Action = "probe" },
		"invalid challenge":       func(r *MembershipRepairCompletionRequest) { r.Challenge = "not-hex" },
		"singleton source target": func(r *MembershipRepairCompletionRequest) { r.Target.Members = r.Target.Members[1:] },
		"candidate mismatch":      func(r *MembershipRepairCompletionRequest) { r.CandidateMember.WALIdentity = strings.Repeat("88", 32) },
		"executor is candidate":   func(r *MembershipRepairCompletionRequest) { r.ExecutorNodeID = string(r.CandidateMember.ID) },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := request
			candidate.Target.Members = append([]quepaxa.Member(nil), request.Target.Members...)
			mutate(&candidate)
			if err := ValidateMembershipRepairCompletionRequest(candidate); err == nil {
				t.Fatal("accepted malformed completion scope")
			}
		})
	}
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	duplicate := strings.Replace(string(body), `"version":1,`, `"version":1,"version":1,`, 1)
	if _, err := StrictDecodeMembershipRepairCompletionRequest([]byte(duplicate)); err == nil {
		t.Fatal("accepted duplicate version field")
	}
}

func validMembershipRepairCompletionRequest() MembershipRepairCompletionRequest {
	abort := quepaxa.Slot(0)
	candidate := quepaxa.Member{ID: "candidate", URL: "https://candidate", PeerURL: "quic://candidate:7000", PublicKey: quepaxa.PublicKey{1}, WALIdentity: strings.Repeat("ef", 32)}
	return MembershipRepairCompletionRequest{
		Version: 1, Action: MembershipRepairCompletionAction, Challenge: strings.Repeat("ab", 32), AttemptRevision: 4,
		ScopeDigest: strings.Repeat("cd", 32), RepairOperationID: "repair", RepairBindingDigest: strings.Repeat("11", 32), PhaseHash: strings.Repeat("22", 32),
		JournalOperationID: "journal-add", JournalRequestHash: strings.Repeat("33", 32), SourceClusterID: "cluster", SourceConfigID: 5, AbortSlot: &abort,
		FreezeSlot: 21, FreezeHash: strings.Repeat("44", 32), TerminalSlot: 37, TerminalHash: strings.Repeat("55", 32),
		Target: quepaxa.Cluster{ConfigID: 6, Members: []quepaxa.Member{
			{ID: "voter", URL: "https://voter", PeerURL: "quic://voter:7000", PublicKey: quepaxa.PublicKey{2}, WALIdentity: strings.Repeat("66", 32)},
			candidate,
		}},
		CandidatePodUID: "candidate-pod-uid", CandidateMember: candidate, CandidateBindRef: strings.Repeat("77", 32),
		ExecutorNodeID: "voter", ExecutorPodUID: "voter-pod-uid", ExecutorRunID: "voter-run", ExecutorWALIdentity: strings.Repeat("66", 32),
	}
}
