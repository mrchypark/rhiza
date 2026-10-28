package fencing

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/mrchypark/rhiza/pkg/recoveryanchor"
)

type sessionAuthority struct {
	mu                 sync.Mutex
	id                 Identity
	stop               *StopRequest
	bootstrapStatus    BootstrapStatusResult
	runtimeAdmission   BootstrapRuntimeAdmissionResult
	runtimeAdmissionID Identity
	bootstrapOperation string
	bootstrapComplete  *BootstrapCompletion
	repairStatus       MembershipRepairStatusResult
	repairPhase        *MembershipRepairPhase
	repairErr          error
}

func TestLocalStartupRegistrationValidatesKnownFieldsAndAssignedIdentity(t *testing.T) {
	candidate := Identity{SourceClusterID: "source", NodeID: "pod-uid-1", PodName: "rhiza-0"}
	registration := Registration{Identity: Identity{
		SourceClusterID: "source", NodeID: "pod-uid-1", PodName: "rhiza-0", PodUID: "uid-1",
		CredentialUID: "secret-uid", CredentialName: "rhiza-inc-1", RunID: "run-1",
	}}
	if err := registration.validateLocalStartup(candidate); err != nil {
		t.Fatalf("valid supervisor-assigned identity rejected: %v", err)
	}

	legacy := Registration{Identity: Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "uid-legacy", RunID: "run-legacy"}}
	if err := legacy.validateLocalStartup(Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodName: "rhiza-0"}); err != nil {
		t.Fatalf("valid legacy identity rejected: %v", err)
	}

	for name, mutate := range map[string]func(*Registration){
		"wrong source":       func(r *Registration) { r.Identity.SourceClusterID = "other" },
		"wrong canonical ID": func(r *Registration) { r.Identity.NodeID = "rhiza-0" },
		"wrong Pod name":     func(r *Registration) { r.Identity.PodName = "rhiza-1" },
		"missing UID":        func(r *Registration) { r.Identity.PodUID = "" },
		"missing run":        func(r *Registration) { r.Identity.RunID = "" },
		"bound WAL":          func(r *Registration) { r.Identity.WALIdentity = "wal" },
		"credential missing": func(r *Registration) { r.Identity.CredentialName = "" },
		"child token":        func(r *Registration) { r.PeerToken = "unexpected" },
		"self member":        func(r *Registration) { r.Learner = []byte(`{"node_id":"pod-uid-1"}`) },
	} {
		t.Run(name, func(t *testing.T) {
			bad := registration
			mutate(&bad)
			if err := bad.validateLocalStartup(candidate); err == nil {
				t.Fatal("invalid local startup registration accepted")
			}
		})
	}
}

func TestSupervisorChildEnvironmentReplacesPriorCredentialValues(t *testing.T) {
	learner := `{"node_id":"pod-uid-1","public_key":"AQID"}`
	registration := Registration{
		Identity: Identity{NodeID: "pod-uid-1"}, PeerToken: "fresh-token", Learner: []byte(learner),
	}
	env := supervisorChildEnvironment([]string{
		"KEEP_ME=yes", "RHIZA_NODE_ID=old-node", "RHIZA_PEER_TOKEN=old-token",
		"RHIZA_PEER_TOKENS=old-map", "RHIZA_LEARNER=old-member",
		"RHIZA_FENCE_SOCKET=old-socket", "RHIZA_FENCE_TICKET=old-ticket",
	}, "/tmp/fence.sock", "ticket", registration)
	for key, want := range map[string]string{
		"KEEP_ME": "yes", "RHIZA_NODE_ID": "pod-uid-1", "RHIZA_PEER_TOKEN": "fresh-token",
		"RHIZA_PEER_TOKENS": "", "RHIZA_LEARNER": learner,
		"RHIZA_FENCE_SOCKET": "/tmp/fence.sock", "RHIZA_FENCE_TICKET": "ticket",
	} {
		if got, count := environmentValue(env, key); got != want || count != 1 {
			t.Fatalf("%s=(%q,count=%d), want (%q,1)", key, got, count, want)
		}
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestSupervisorChildEnvironmentProcess$")
	cmd.Env = append(env, "RHIZA_TEST_CHILD_ENV=1")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child observed incorrect environment: %v\n%s", err, output)
	}
}

func TestSupervisorChildEnvironmentProcess(t *testing.T) {
	if os.Getenv("RHIZA_TEST_CHILD_ENV") != "1" {
		return
	}
	for key, want := range map[string]string{
		"KEEP_ME": "yes", "RHIZA_NODE_ID": "pod-uid-1", "RHIZA_PEER_TOKEN": "fresh-token",
		"RHIZA_PEER_TOKENS": "", "RHIZA_LEARNER": `{"node_id":"pod-uid-1","public_key":"AQID"}`,
		"RHIZA_FENCE_SOCKET": "/tmp/fence.sock", "RHIZA_FENCE_TICKET": "ticket",
	} {
		if got := os.Getenv(key); got != want {
			t.Fatalf("child %s=%q, want %q", key, got, want)
		}
	}
}

func environmentValue(env []string, key string) (string, int) {
	wantPrefix := key + "="
	var value string
	count := 0
	for _, entry := range env {
		if strings.HasPrefix(entry, wantPrefix) {
			value = strings.TrimPrefix(entry, wantPrefix)
			count++
		}
	}
	return value, count
}

func TestSessionTransient503AndConnectionFailureRemainDistinctFromIdentity(t *testing.T) {
	var unavailable atomic.Bool
	id := testIdentity()
	socket := filepath.Join(t.TempDir(), "session.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if unavailable.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(Registration{Identity: id})
	})}
	defer server.Close()
	defer listener.Close()
	go func() { _ = server.Serve(listener) }()
	session, err := Connect(t.Context(), socket, "ticket", id.SourceClusterID, id.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	unavailable.Store(true)
	if _, err := session.WaitStop(t.Context()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("503 lost transient classification: %v", err)
	}
	server.Close()
	if _, err := session.WaitStop(t.Context()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("connection failure lost transient classification: %v", err)
	}
	if session.Identity() != id {
		t.Fatal("transient outage changed identity")
	}
}

type sessionRoundTrip func(*http.Request) (*http.Response, error)

func (f sessionRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type interruptedSessionBody struct{}

func (interruptedSessionBody) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (interruptedSessionBody) Close() error             { return nil }

func TestSessionInterruptedSuccessfulBodyIsTransient(t *testing.T) {
	s := &Session{ticket: "ticket", identity: testIdentity(), client: &http.Client{Transport: sessionRoundTrip(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: interruptedSessionBody{}}, nil
	})}}
	if _, err := s.WaitStop(t.Context()); !errors.Is(err, ErrUnavailable) || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("body interruption lost cause: %v", err)
	}
}

func TestSupervisorSocketRestartKeepsLiveListenerAndOtherFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.sock")
	live, err := listenSupervisor(path)
	if err != nil {
		t.Fatal(err)
	}
	if other, err := listenSupervisor(path); err == nil {
		other.Close()
		t.Fatal("replaced a live supervisor")
	}
	if err := live.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("owned unrelated file"), 0600); err != nil {
		t.Fatal(err)
	}
	if other, err := listenSupervisor(path); err == nil {
		other.Close()
		t.Fatal("removed a non-socket file")
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "owned unrelated file" {
		t.Fatal("non-socket file changed")
	}
}

func (a *sessionAuthority) Register(_ context.Context, id Identity) (Registration, error) {
	return Registration{Identity: id}, nil
}
func (a *sessionAuthority) Bind(_ context.Context, id Identity) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.id = id
	return nil
}
func (a *sessionAuthority) Stop(_ context.Context, id Identity) (StopRequest, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if id != a.id {
		return StopRequest{}, ErrClosed
	}
	if a.stop == nil {
		return StopRequest{}, ErrPending
	}
	return *a.stop, nil
}
func (a *sessionAuthority) Drain(context.Context, DrainReceipt) error         { return nil }
func (a *sessionAuthority) Exit(context.Context, StopRequest) error           { return nil }
func (a *sessionAuthority) NeverBoundExit(context.Context, StopRequest) error { return nil }

func (a *sessionAuthority) BootstrapStatus(_ context.Context, operationID string, identity Identity) (BootstrapStatusResult, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if identity != a.id || operationID == "" {
		return BootstrapStatusResult{}, ErrClosed
	}
	a.bootstrapOperation = operationID
	return a.bootstrapStatus, nil
}

func (a *sessionAuthority) BootstrapRuntimeAdmission(_ context.Context, operationID string, identity Identity) (BootstrapRuntimeAdmissionResult, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if operationID == "" || identity.WALIdentity != "" || identity != a.id {
		return BootstrapRuntimeAdmissionResult{}, ErrClosed
	}
	a.runtimeAdmissionID = identity
	return a.runtimeAdmission, nil
}

func (a *sessionAuthority) CompleteBootstrap(_ context.Context, identity Identity, completion BootstrapCompletion) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if identity != a.id {
		return ErrClosed
	}
	a.bootstrapComplete = &completion
	return nil
}

func (a *sessionAuthority) ReadMembershipRepair(_ context.Context, identity Identity) (MembershipRepairStatusResult, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if identity != a.id {
		return MembershipRepairStatusResult{}, ErrClosed
	}
	if a.repairErr != nil {
		return MembershipRepairStatusResult{}, a.repairErr
	}
	return a.repairStatus, nil
}

func (a *sessionAuthority) AuthorizeMembershipPhase(_ context.Context, identity Identity, phase MembershipRepairPhase) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if identity != a.id {
		return ErrClosed
	}
	if a.repairErr != nil {
		return a.repairErr
	}
	copy := cloneMembershipRepairPhase(phase)
	a.repairPhase = &copy
	return nil
}

func TestSessionRelaysBootstrapStatusAndCompletionOnlyAfterWALBind(t *testing.T) {
	podUID := "pod-uid"
	nodeID, err := ReplacementNodeID(podUID)
	if err != nil {
		t.Fatal(err)
	}
	id := Identity{SourceClusterID: "source", NodeID: nodeID, PodName: "rhiza-0", PodUID: podUID, CredentialName: "candidate-secret", CredentialUID: "candidate-secret-uid", RunID: "run-1", WALIdentity: "wal-1"}
	assignment := BootstrapAssignment{
		OperationID: "operation-1", RequestHash: strings.Repeat("a", 64), AuthorizationHash: strings.Repeat("b", 64), Purpose: BootstrapLearner,
		RootUID: "root-uid", StatefulSetUID: "sts-uid", SourceClusterID: id.SourceClusterID, PodName: id.PodName, PodUID: id.PodUID, NodeID: id.NodeID,
		CredentialName: id.CredentialName, CredentialUID: id.CredentialUID, MembershipHash: strings.Repeat("c", 64), ProfileHash: strings.Repeat("d", 64), LedgerUID: "ledger-uid",
		PublicMember: BootstrapPublicMember{ID: id.NodeID, URL: "http://rhiza-0", PeerURL: "quic://rhiza-0", PublicKey: strings.Repeat("e", 64)}, Durability: "async", StoreIdentityHash: strings.Repeat("f", 64),
	}
	assignmentSetHash := BootstrapAssignmentSetHash([]BootstrapAssignment{assignment})
	authority := &sessionAuthority{
		id:              id,
		bootstrapStatus: BootstrapStatusResult{Assignment: &assignment, AssignmentSetHash: assignmentSetHash},
	}
	supervisor := &Supervisor{Authority: authority, bound: id, ticket: "private-ticket"}
	server := httptest.NewServer(supervisor.handler())
	defer server.Close()

	client := &http.Client{Transport: &http.Transport{Proxy: nil}}
	session := &Session{client: client, ticket: "private-ticket", identity: id}
	// Route the session's supervisor URL to the in-process real handler.
	client.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		request.URL.Scheme = "http"
		request.URL.Host = strings.TrimPrefix(server.URL, "http://")
		return server.Client().Transport.RoundTrip(request)
	})

	if _, err := (&Session{client: client, ticket: "private-ticket", identity: Identity{SourceClusterID: id.SourceClusterID, NodeID: id.NodeID, PodUID: id.PodUID, RunID: id.RunID}}).BootstrapStatus(t.Context(), "operation-1"); err == nil {
		t.Fatal("unbound bootstrap status was accepted")
	}
	status, err := session.BootstrapStatus(t.Context(), "operation-1")
	if err != nil || status.Assignment == nil || *status.Assignment != assignment || status.AssignmentSetHash != assignmentSetHash {
		t.Fatalf("bootstrap status=%+v err=%v", status, err)
	}
	completion := BootstrapCompletion{
		OperationID: "operation-1", AuthorizationHash: assignment.AuthorizationHash,
		AssignmentSetHash: status.AssignmentSetHash, CertifiedBarrierRef: "9",
	}
	if err := session.CompleteBootstrap(t.Context(), completion); err != nil {
		t.Fatal(err)
	}
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if authority.bootstrapOperation != completion.OperationID || authority.bootstrapComplete == nil || *authority.bootstrapComplete != completion {
		t.Fatalf("authority received operation=%q completion=%+v", authority.bootstrapOperation, authority.bootstrapComplete)
	}
}

func TestSessionRelaysDRRuntimeAdmissionBeforeWALBind(t *testing.T) {
	request := recoveryanchor.Request{AnchorID: "ha", OperationID: "recover-1", StatefulSetUID: "sts", SourceClusterID: "source", TargetClusterID: "target", SourcePrefix: "root/source", TargetPrefix: "root/target", SourceMembership: "source-membership", FenceHash: strings.Repeat("a", 64), TargetAnchorHash: strings.Repeat("b", 64), Fork: recovery.ForkResult{Tip: 3, ManifestHash: strings.Repeat("c", 64), PrefixHash: strings.Repeat("d", 64)}, TargetMembership: recovery.NewMembershipRecord("target", []quepaxa.Member{}, "before-ack")}
	// Construct a valid durable membership for the approval fixture.
	member := quepaxa.Member{ID: "n0", URL: "http://n0", LogURL: "http://n0/log"}
	request.TargetMembership = recovery.NewMembershipRecord("target", []quepaxa.Member{member}, "before-ack")
	requestHash, err := recoveryanchor.RequestHash(request)
	if err != nil {
		t.Fatal(err)
	}
	nodeID, err := ReplacementNodeID("pod-uid-1")
	if err != nil {
		t.Fatal(err)
	}
	assignment := BootstrapAssignment{OperationID: "dr-op", RequestHash: "bootstrap-hash", AuthorizationHash: "authorization", Purpose: BootstrapDRSuccessor, RootUID: "root", StatefulSetUID: "sts", SourceClusterID: "target", PodName: "rhiza-1", PodUID: "pod-uid-1", NodeID: nodeID, CredentialName: "secret", CredentialUID: "secret-uid", MembershipHash: "membership", ProfileHash: "profile", LedgerUID: "ledger", DRApproval: BootstrapDRApproval{Request: request, Receipt: recoveryanchor.Receipt{AnchorID: request.AnchorID, OperationID: request.OperationID, RequestHash: requestHash, Target: recoveryanchor.Binding{ClusterID: "target", StorageID: "store"}, Generation: 2}}}
	id := Identity{SourceClusterID: "target", NodeID: assignment.NodeID, PodName: assignment.PodName, PodUID: assignment.PodUID, CredentialName: assignment.CredentialName, CredentialUID: assignment.CredentialUID, RunID: "run-1"}
	var remoteIdentity Identity
	remoteAction := ""
	authorityServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var message AuthorityMessage
		if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
			t.Errorf("decode authority request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		remoteAction, remoteIdentity = message.Action, message.Identity
		if message.Action != "bootstrap-runtime-admission" || message.RuntimeAdmission == nil || message.RuntimeAdmission.OperationID != assignment.OperationID {
			t.Errorf("unexpected authority request %+v", message)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(BootstrapRuntimeAdmissionResult{Assignment: assignment})
	}))
	defer authorityServer.Close()
	tokenPath := filepath.Join(t.TempDir(), "authority-token")
	if err := os.WriteFile(tokenPath, []byte("test-token"), 0600); err != nil {
		t.Fatal(err)
	}
	authority := &AuthorityClient{URL: authorityServer.URL + "/v1/supervisor", TokenFile: tokenPath, Client: authorityServer.Client()}
	supervisor := &Supervisor{Authority: authority, bound: id, ticket: "private-ticket"}
	supervisorServer := httptest.NewServer(supervisor.handler())
	defer supervisorServer.Close()
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		request.URL.Scheme = "http"
		request.URL.Host = strings.TrimPrefix(supervisorServer.URL, "http://")
		return supervisorServer.Client().Transport.RoundTrip(request)
	})}
	session := &Session{client: client, ticket: "private-ticket", identity: id}
	result, err := session.BootstrapRuntimeAdmission(t.Context(), assignment.OperationID)
	if err != nil || result.Assignment != assignment {
		t.Fatalf("pre-bind admission=%+v err=%v", result, err)
	}
	if remoteAction != "bootstrap-runtime-admission" || remoteIdentity != id || remoteIdentity.WALIdentity != "" {
		t.Fatalf("TLS authority saw action=%q identity=%+v", remoteAction, remoteIdentity)
	}
	bound := *session
	bound.identity.WALIdentity = "already-bound"
	if _, err := bound.BootstrapRuntimeAdmission(t.Context(), assignment.OperationID); err == nil {
		t.Fatal("accepted runtime admission after WAL binding")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestParseBootstrapBarrierSlotRequiresCanonicalPositiveDecimal(t *testing.T) {
	if got, err := ParseBootstrapBarrierSlot("42"); err != nil || got != 42 {
		t.Fatalf("slot=%d err=%v", got, err)
	}
	for _, value := range []string{"", "0", "00", "042", "+42", " 42", "42 ", "18446744073709551616", "slot-42"} {
		t.Run(value, func(t *testing.T) {
			if _, err := ParseBootstrapBarrierSlot(value); err == nil {
				t.Fatalf("non-canonical slot %q accepted", value)
			}
		})
	}
}

func TestSupervisorSessionBindsExactRunAndStop(t *testing.T) {
	id := testIdentity()
	authority := &sessionAuthority{id: id}
	s := &Supervisor{Authority: authority, Identity: id, bound: id, ticket: "private-ticket"}
	socket := filepath.Join(t.TempDir(), "control.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: s.handler()}
	defer server.Close()
	defer listener.Close()
	go func() { _ = server.Serve(listener) }()
	if _, err := Connect(t.Context(), socket, "wrong-ticket", id.SourceClusterID, id.NodeID); err == nil {
		t.Fatal("wrong ticket accepted")
	}
	session, err := Connect(t.Context(), socket, "private-ticket", id.SourceClusterID, id.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.BindWAL(t.Context(), "wal"); err != nil {
		t.Fatal(err)
	}
	if err := session.BindWAL(t.Context(), "other"); err == nil {
		t.Fatal("changed WAL accepted")
	}
	if _, err := session.WaitStop(t.Context()); err != ErrPending {
		t.Fatalf("unrequested stop: %v", err)
	}
	authority.mu.Lock()
	request := StopRequest{OperationID: "op", RequestHash: "hash", Identity: authority.id}
	authority.stop = &request
	authority.mu.Unlock()
	got, err := session.WaitStop(t.Context())
	if err != nil || got != request {
		t.Fatalf("stop=%+v err=%v", got, err)
	}
	wrong := request
	wrong.Identity.RunID = "other"
	if err := session.AcknowledgeDrain(t.Context(), DrainReceipt{Request: wrong, WritersJoined: true}); err == nil {
		t.Fatal("other run accepted")
	}
	if err := session.AcknowledgeDrain(t.Context(), DrainReceipt{Request: request, WritersJoined: true}); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	receipt := s.drain
	s.mu.Unlock()
	if receipt == nil || receipt.Request != request {
		t.Fatal("matching drain not delivered")
	}
}

func TestSupervisorSessionRelaysMembershipRepairDiscoveryAndExactPhaseAuthorization(t *testing.T) {
	id := testIdentity()
	authority := &sessionAuthority{id: id}
	s := &Supervisor{Authority: authority, Identity: id, bound: id, ticket: "private-ticket"}
	socketDir, err := os.MkdirTemp("/tmp", "rhizaf-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(socketDir)
	socket := filepath.Join(socketDir, "membership-repair.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: s.handler()}
	defer server.Close()
	defer listener.Close()
	go func() { _ = server.Serve(listener) }()
	session, err := Connect(t.Context(), socket, "private-ticket", id.SourceClusterID, id.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.BindWAL(t.Context(), "wal-1"); err != nil {
		t.Fatal(err)
	}
	bound := session.Identity()

	status, err := session.ReadMembershipRepair(t.Context())
	if err != nil || status.Repair != nil {
		t.Fatalf("no active repair status=%+v err=%v", status, err)
	}
	authority.mu.Lock()
	oldMember := quepaxa.Member{ID: quepaxa.NodeID(bound.NodeID), URL: "http://old", LogURL: "http://old-log", PublicKey: quepaxa.PublicKey(sha256.Sum256([]byte("old"))), WALIdentity: bound.WALIdentity}
	otherMember := quepaxa.Member{ID: "other", URL: "http://other", LogURL: "http://other-log", PublicKey: quepaxa.PublicKey(sha256.Sum256([]byte("other"))), WALIdentity: "other-wal"}
	members := []quepaxa.Member{oldMember, otherMember}
	view := MembershipRepairView{
		Authorization: MembershipRepairAuthorization{OperationID: "repair", RequestHash: strings.Repeat("a", 64), AuthorizationHash: strings.Repeat("b", 64), RootUID: "root", StatefulSetUID: "sts", LedgerUID: "ledger", SourceClusterID: bound.SourceClusterID, MembershipHash: canonicalJSONHash(members), OldIdentity: bound, Membership: quepaxa.Cluster{ConfigID: 1, Members: members}},
		Progress:      MembershipRepairProgress{Revision: 1, Stage: MembershipRepairAdmitted},
	}
	authority.repairStatus = MembershipRepairStatusResult{Repair: &view}
	authority.mu.Unlock()
	status, err = session.ReadMembershipRepair(t.Context())
	if err != nil || status.Repair == nil || status.Repair.Authorization.OperationID != "repair" {
		t.Fatalf("active repair status=%+v err=%v", status, err)
	}

	zero := quepaxa.Slot(0)
	phase := MembershipRepairPhase{Kind: MembershipRepairRemove, CandidatePodUID: "candidate", AbortSlot: &zero, Change: network.MembershipChange{OperationID: "remove", ClusterID: bound.SourceClusterID, ExpectedConfigID: 1, Remove: quepaxa.NodeID(bound.NodeID)}}
	phase.Hash, err = MembershipRepairPhaseHash(phase)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.AuthorizeMembershipPhase(t.Context(), phase); err != nil {
		t.Fatalf("exact phase authorization failed: %v", err)
	}
	authority.mu.Lock()
	if authority.repairPhase == nil || !reflect.DeepEqual(*authority.repairPhase, phase) {
		authority.mu.Unlock()
		t.Fatalf("authority received phase=%+v, want %+v", authority.repairPhase, phase)
	}
	authority.mu.Unlock()

	for _, failure := range []error{ErrUnavailable, ErrPending} {
		authority.mu.Lock()
		authority.repairErr = failure
		authority.mu.Unlock()
		if _, err := session.ReadMembershipRepair(t.Context()); err != failure {
			t.Fatalf("repair discovery error %v collapsed to %v", failure, err)
		}
	}
}
