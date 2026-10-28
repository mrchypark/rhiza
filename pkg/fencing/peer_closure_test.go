package fencing

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
)

type peerClosureAuthorityTest struct {
	caller    Identity
	request   PeerClosureRequest
	receipt   PeerClosureReceipt
	readError error
	action    string
}

func (a *peerClosureAuthorityTest) Register(_ context.Context, id Identity) (Registration, error) {
	return Registration{Identity: id}, nil
}
func (a *peerClosureAuthorityTest) Bind(_ context.Context, id Identity) error {
	a.caller = id
	return nil
}
func (a *peerClosureAuthorityTest) Stop(context.Context, Identity) (StopRequest, error) {
	return StopRequest{}, ErrPending
}
func (a *peerClosureAuthorityTest) Drain(context.Context, DrainReceipt) error         { return nil }
func (a *peerClosureAuthorityTest) Exit(context.Context, StopRequest) error           { return nil }
func (a *peerClosureAuthorityTest) NeverBoundExit(context.Context, StopRequest) error { return nil }
func (a *peerClosureAuthorityTest) RequestPeerClosure(_ context.Context, caller Identity, request PeerClosureRequest) (PeerClosureReceipt, error) {
	a.action, a.caller, a.request = "request", caller, request
	return a.receipt, nil
}
func (a *peerClosureAuthorityTest) ReadPeerClosure(_ context.Context, caller Identity, request PeerClosureRequest) (PeerClosureReceipt, error) {
	a.action, a.caller, a.request = "status", caller, request
	return PeerClosureReceipt{}, a.readError
}

func TestSessionPeerClosureUsesBoundCallerAndSeparateTarget(t *testing.T) {
	caller := testIdentity()
	caller.WALIdentity = "wal-caller"
	request := PeerClosureRequest{ExpectedConfigID: 7, ExpectedAbortSlot: uint64Pointer(3), TargetNodeID: "rhiza-1", TargetWALIdentity: "wal-target"}
	authority := &peerClosureAuthorityTest{receipt: PeerClosureReceipt{
		OperationID: "peer-op", SourceClusterID: caller.SourceClusterID, BindingUID: "root-uid", StatefulSetUID: "sts-uid", RequestHash: "request-hash",
		ExpectedConfigID: request.ExpectedConfigID, ExpectedAbortSlot: uint64Pointer(*request.ExpectedAbortSlot), TargetNodeID: request.TargetNodeID, TargetWALIdentity: request.TargetWALIdentity,
		Targets: []PeerClosureTarget{{NodeID: request.TargetNodeID, Pod: request.TargetNodeID, PodUID: "target-uid", WALIdentity: request.TargetWALIdentity}},
		ProofID: "proof", ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true,
	}}
	supervisor := &Supervisor{Authority: authority, bound: caller, ticket: "ticket"}
	client := &http.Client{Transport: sessionRoundTrip(func(req *http.Request) (*http.Response, error) {
		recorder := httptest.NewRecorder()
		supervisor.handler().ServeHTTP(recorder, req)
		return recorder.Result(), nil
	})}
	session := &Session{client: client, ticket: "ticket", identity: caller}
	receipt, err := session.RequestPeerClosure(context.Background(), request)
	if err != nil || receipt.TargetNodeID != request.TargetNodeID {
		t.Fatalf("request receipt=%+v err=%v", receipt, err)
	}
	if authority.action != "request" || authority.caller != caller || !samePeerClosureRequest(authority.request, request) {
		t.Fatalf("authority received action=%q caller=%+v request=%+v", authority.action, authority.caller, authority.request)
	}
	authority.readError = ErrNotFound
	if _, err := session.ReadPeerClosure(context.Background(), request); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing status error=%v, want ErrNotFound", err)
	}
	if authority.action != "status" || authority.caller != caller || !samePeerClosureRequest(authority.request, request) {
		t.Fatal("status request changed caller or target binding")
	}
}

func TestPeerClosureSessionRejectsReceiptForDifferentTarget(t *testing.T) {
	caller := testIdentity()
	caller.WALIdentity = "wal-caller"
	request := PeerClosureRequest{ExpectedConfigID: 2, ExpectedAbortSlot: uint64Pointer(0), TargetNodeID: "rhiza-1", TargetWALIdentity: "wal-1"}
	authority := &peerClosureAuthorityTest{receipt: PeerClosureReceipt{
		OperationID: "peer-op", SourceClusterID: caller.SourceClusterID, BindingUID: "root", StatefulSetUID: "sts", RequestHash: "hash", ExpectedConfigID: 2,
		ExpectedAbortSlot: uint64Pointer(0), TargetNodeID: "rhiza-2", TargetWALIdentity: "wal-2", Targets: []PeerClosureTarget{{NodeID: "rhiza-2", Pod: "rhiza-2", PodUID: "pod-2", WALIdentity: "wal-2"}},
		ProofID: "proof", ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true,
	}}
	supervisor := &Supervisor{Authority: authority, bound: caller, ticket: "ticket"}
	client := &http.Client{Transport: sessionRoundTrip(func(req *http.Request) (*http.Response, error) {
		recorder := httptest.NewRecorder()
		supervisor.handler().ServeHTTP(recorder, req)
		return recorder.Result(), nil
	})}
	session := &Session{client: client, ticket: "ticket", identity: caller}
	if _, err := session.RequestPeerClosure(context.Background(), request); err == nil {
		t.Fatal("receipt for a different target was accepted")
	}
}

func TestAuthorityClientRoutesPeerClosureActionsToDedicatedEndpoints(t *testing.T) {
	caller := testIdentity()
	caller.WALIdentity = "wal-caller"
	request := PeerClosureRequest{ExpectedConfigID: 4, ExpectedAbortSlot: uint64Pointer(2), TargetNodeID: "rhiza-1", TargetWALIdentity: "wal-target"}
	want := PeerClosureReceipt{
		OperationID: "op", SourceClusterID: caller.SourceClusterID, BindingUID: "root-uid", StatefulSetUID: "sts-uid", RequestHash: "request-hash",
		ExpectedConfigID: 4, ExpectedAbortSlot: uint64Pointer(2), TargetNodeID: "rhiza-1", TargetWALIdentity: "wal-target",
		Targets: []PeerClosureTarget{{NodeID: "rhiza-1", Pod: "rhiza-1", PodUID: "pod-target", WALIdentity: "wal-target"}},
		ProofID: "proof", ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true,
	}
	var requestPath, action string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestPath = r.URL.Path
		var message AuthorityMessage
		if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		action = message.Action
		if r.URL.Path == "/v1/peer-closure/status" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(want)
	}))
	defer server.Close()
	tokenPath := t.TempDir() + "/token"
	if err := os.WriteFile(tokenPath, []byte("authority-token"), 0600); err != nil {
		t.Fatal(err)
	}
	client := &AuthorityClient{URL: server.URL + "/v1/supervisor", TokenFile: tokenPath, Client: server.Client()}
	got, err := client.RequestPeerClosure(context.Background(), caller, request)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("peer closure receipt=%+v err=%v", got, err)
	}
	if requestPath != "/v1/peer-closure/request" || action != "peer-closure-request" {
		t.Fatalf("request routed path=%q action=%q", requestPath, action)
	}
	if _, err := client.ReadPeerClosure(context.Background(), caller, request); !errors.Is(err, ErrNotFound) {
		t.Fatalf("status missing error=%v, want ErrNotFound", err)
	}
	if requestPath != "/v1/peer-closure/status" || action != "peer-closure-status" {
		t.Fatalf("status routed path=%q action=%q", requestPath, action)
	}
}

func TestSupervisorRejectsTrailingPeerClosureJSON(t *testing.T) {
	caller := testIdentity()
	caller.WALIdentity = "wal-caller"
	authority := &peerClosureAuthorityTest{}
	supervisor := &Supervisor{Authority: authority, bound: caller, ticket: "ticket"}
	message := AuthorityMessage{Action: "peer-closure-status", Identity: caller, PeerClosure: &PeerClosureRequest{ExpectedConfigID: 1, ExpectedAbortSlot: uint64Pointer(0), TargetNodeID: "rhiza-1", TargetWALIdentity: "wal-1"}}
	data, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/peer-closure/status", io.NopCloser(io.MultiReader(bytes.NewReader(data), strings.NewReader(` {"unexpected":true}`))))
	req.Header.Set("Authorization", "Bearer ticket")
	recorder := httptest.NewRecorder()
	supervisor.handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusConflict || authority.action != "" {
		t.Fatalf("trailing request status=%d action=%q", recorder.Code, authority.action)
	}
}

func TestSupervisorRejectsNullAbortSlotBeforeAuthorityDispatch(t *testing.T) {
	caller := testIdentity()
	caller.WALIdentity = "wal-caller"
	authority := &peerClosureAuthorityTest{}
	supervisor := &Supervisor{Authority: authority, bound: caller, ticket: "ticket"}
	message := AuthorityMessage{Action: "peer-closure-status", Identity: caller, PeerClosure: &PeerClosureRequest{ExpectedConfigID: 1, TargetNodeID: "rhiza-1", TargetWALIdentity: "wal-1"}}
	data, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/peer-closure/status", bytes.NewReader(data))
	req.Header.Set("Authorization", "Bearer ticket")
	recorder := httptest.NewRecorder()
	supervisor.handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusConflict || authority.action != "" {
		t.Fatalf("null abort slot status=%d authority action=%q", recorder.Code, authority.action)
	}
}

func TestSupervisorRejectsUnrelatedPayloadBeforePeerClosureDispatch(t *testing.T) {
	caller := testIdentity()
	caller.WALIdentity = "wal-caller"
	authority := &peerClosureAuthorityTest{}
	supervisor := &Supervisor{Authority: authority, bound: caller, ticket: "ticket"}
	message := AuthorityMessage{
		Action: "peer-closure-status", Identity: caller,
		PeerClosure:         &PeerClosureRequest{ExpectedConfigID: 1, ExpectedAbortSlot: uint64Pointer(0), TargetNodeID: "rhiza-1", TargetWALIdentity: "wal-1"},
		RepairPhaseComplete: &MembershipRepairPhaseObserveRequest{OperationID: "repair"},
	}
	data, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/peer-closure/status", bytes.NewReader(data))
	req.Header.Set("Authorization", "Bearer ticket")
	recorder := httptest.NewRecorder()
	supervisor.handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusConflict || authority.action != "" {
		t.Fatalf("mixed payload status=%d authority action=%q", recorder.Code, authority.action)
	}
}

func TestPeerClosureRequiresExplicitAbortSlotButAcceptsZero(t *testing.T) {
	caller := testIdentity()
	caller.WALIdentity = "wal-caller"
	base := `{"expectedConfigID":1,"targetNodeID":"rhiza-1","targetWALIdentity":"wal-1"}`
	cases := []struct {
		name  string
		json  string
		valid bool
	}{
		{name: "omitted", json: base},
		{name: "null", json: `{"expectedConfigID":1,"expectedAbortSlot":null,"targetNodeID":"rhiza-1","targetWALIdentity":"wal-1"}`},
		{name: "explicit zero", json: `{"expectedConfigID":1,"expectedAbortSlot":0,"targetNodeID":"rhiza-1","targetWALIdentity":"wal-1"}`, valid: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var request PeerClosureRequest
			if err := json.Unmarshal([]byte(tc.json), &request); err != nil {
				t.Fatal(err)
			}
			err := request.Validate(caller)
			if tc.valid && err != nil {
				t.Fatalf("explicit zero rejected: %v", err)
			}
			if !tc.valid && err == nil {
				t.Fatal("missing/null abort slot accepted")
			}
		})
	}
}

func TestPeerClosureClientsRejectMissingAbortSlotBeforeTransport(t *testing.T) {
	caller := testIdentity()
	caller.WALIdentity = "wal-caller"
	missing := PeerClosureRequest{ExpectedConfigID: 1, TargetNodeID: "rhiza-1", TargetWALIdentity: "wal-1"}
	session := &Session{identity: caller}
	client := &AuthorityClient{}
	for _, action := range []string{"session-request", "session-status", "authority-request", "authority-status"} {
		t.Run(action, func(t *testing.T) {
			var err error
			switch action {
			case "session-request":
				_, err = session.RequestPeerClosure(context.Background(), missing)
			case "session-status":
				_, err = session.ReadPeerClosure(context.Background(), missing)
			case "authority-request":
				_, err = client.RequestPeerClosure(context.Background(), caller, missing)
			case "authority-status":
				_, err = client.ReadPeerClosure(context.Background(), caller, missing)
			}
			if err == nil || !strings.Contains(err.Error(), "complete bound caller and target round") {
				t.Fatalf("missing abort slot error=%v; request reached transport or was accepted", err)
			}
		})
	}
}

func uint64Pointer(value uint64) *uint64 { return &value }

func samePeerClosureRequest(left, right PeerClosureRequest) bool {
	if left.ExpectedConfigID != right.ExpectedConfigID || left.TargetNodeID != right.TargetNodeID || left.TargetWALIdentity != right.TargetWALIdentity {
		return false
	}
	if left.ExpectedAbortSlot == nil || right.ExpectedAbortSlot == nil {
		return left.ExpectedAbortSlot == nil && right.ExpectedAbortSlot == nil
	}
	return *left.ExpectedAbortSlot == *right.ExpectedAbortSlot
}
