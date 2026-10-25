package fencing

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

type pendingStartupAuthority struct {
	completionAuthority
	identity        Identity
	proposal        PendingEngineRepairProposal
	createCalls     int
	registerCalls   int
	uncertainCreate bool
	closed          bool
	events          []string
}

func (a *pendingStartupAuthority) CreatePendingEngineRepair(_ context.Context, identity Identity, proposal PendingEngineRepairProposal) error {
	a.createCalls++
	a.identity, a.proposal = identity, proposal
	a.events = append(a.events, "pending")
	if a.uncertainCreate {
		a.uncertainCreate = false
		return ErrUnavailable
	}
	return nil
}

func (a *pendingStartupAuthority) RegisterStartup(_ context.Context, identity Identity) (StartupGrant, error) {
	a.registerCalls++
	if identity != a.identity {
		return StartupGrant{}, errors.New("startup identity changed while pending")
	}
	if a.closed {
		return StartupGrant{}, ErrClosed
	}
	if a.registerCalls == 1 {
		a.events = append(a.events, "pending-response")
		return StartupGrant{}, ErrPending
	}
	// Model the production ordering: trusted runtime pin and the restricted
	// producer-less learner Run both exist before the authority returns a grant.
	a.events = append(a.events, "runtime-pinned", "learner-run-registered", "grant")
	id := identity
	id.NodeID = "candidate"
	id.AssignmentID = PendingEngineRepairAssignmentID(hashJSONValue("candidate-assignment"))
	return StartupGrant{Identity: id, Role: StartupRoleLearnerPendingRemove,
		Environment: map[string]string{"RHIZA_NODE_ID": id.NodeID, "RHIZA_LEARNER": "{}", "RHIZA_PEER_TOKEN": "fixed-token", "RHIZA_PEER_TOKENS": ""}}, nil
}

func pendingStartupFixture() (*Supervisor, *pendingStartupAuthority, PendingEngineRepairProposal) {
	id := testIdentity()
	id.PodName = "rhiza-2"
	id.NodeID = "rhiza-2"
	id.WALIdentity = ""
	a := &pendingStartupAuthority{}
	proposal := PendingEngineRepairProposal{SourceClusterID: id.SourceClusterID, BootstrapIdentity: id}
	return &Supervisor{Authority: a, Identity: id}, a, proposal
}

func TestSupervisorPendingStartupKeepsRunAndWaitsForRuntimeEnrollment(t *testing.T) {
	s, authority, proposal := pendingStartupFixture()
	authority.uncertainCreate = true
	grant, err := s.registerStartup(t.Context(), authority, &proposal, true)
	if err != nil {
		t.Fatal(err)
	}
	if grant.Role != StartupRoleLearnerPendingRemove || grant.Validate(s.Identity) != nil {
		t.Fatalf("invalid candidate learner grant: %+v", grant)
	}
	if authority.createCalls != 2 || authority.registerCalls != 2 || authority.identity.RunID != s.Identity.RunID || authority.proposal != proposal {
		t.Fatalf("pending request or physical run changed: creates=%d registers=%d id=%+v", authority.createCalls, authority.registerCalls, authority.identity)
	}
	if want := []string{"pending", "pending", "pending-response", "runtime-pinned", "learner-run-registered", "grant"}; !reflect.DeepEqual(authority.events, want) {
		t.Fatalf("grant ordering=%v, want %v", authority.events, want)
	}
	childStarted := false
	if grant.Role == StartupRoleLearnerPendingRemove {
		childStarted = true
	}
	if !childStarted || authority.events[len(authority.events)-2] != "learner-run-registered" {
		t.Fatal("child grant escaped before trusted runtime pin and learner Run registration")
	}
}

func TestSupervisorPendingStartupStopsOnClosedOrCancelledAuthority(t *testing.T) {
	t.Run("closed", func(t *testing.T) {
		s, authority, proposal := pendingStartupFixture()
		authority.closed = true
		if _, err := s.registerStartup(t.Context(), authority, &proposal, true); !errors.Is(err, ErrClosed) {
			t.Fatalf("closed pending startup error=%v", err)
		}
		if authority.registerCalls != 1 || len(authority.events) != 1 || authority.events[0] != "pending" {
			t.Fatalf("closed startup progressed: calls=%d events=%v", authority.registerCalls, authority.events)
		}
	})
	t.Run("cancelled", func(t *testing.T) {
		s, authority, proposal := pendingStartupFixture()
		ctx, cancel := context.WithCancel(t.Context())
		// Always remain pending; the cancellation must interrupt the bounded poll.
		mock := &alwaysPendingStartupAuthority{pendingStartupAuthority: authority}
		done := make(chan error, 1)
		go func() { _, err := s.registerStartup(ctx, mock, &proposal, true); done <- err }()
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled pending startup error=%v", err)
		}
		if authority.createCalls != 1 {
			t.Fatalf("pending create was not idempotently retained: %d", authority.createCalls)
		}
	})
}

type alwaysPendingStartupAuthority struct{ *pendingStartupAuthority }

func (a *alwaysPendingStartupAuthority) RegisterStartup(_ context.Context, identity Identity) (StartupGrant, error) {
	a.registerCalls++
	if identity != a.identity {
		return StartupGrant{}, errors.New("startup identity changed while pending")
	}
	return StartupGrant{}, ErrPending
}

func TestSupervisorHandlerForwardsInventoryFreeAdoptionToAuthorityClient(t *testing.T) {
	physical := testIdentity()
	physical.PodName, physical.NodeID, physical.RunID, physical.WALIdentity = "rhiza-1", "rhiza-1", "physical-run", "bound-wal"
	proposalDigest := strings.Repeat("a", 64)
	proposal, canonical := repairAdoptionRecords(proposalDigest)
	var forwarded bool
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Errorf("AuthorityClient omitted its authority credential: %q", r.Header.Get("Authorization"))
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var message AuthorityMessage
		if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
			t.Errorf("decode authority request: %v", err)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if message.Action != "engine-repair-adopt" || message.EngineRepair == nil || message.PreflightProposal != nil ||
			message.Identity != physical || message.EngineRepair.Identity != physical || message.EngineRepair.Record == nil ||
			message.EngineRepair.Record.ValidateAdoptionProposal() != nil || len(message.EngineRepair.Record.Incarnations) != 0 {
			t.Errorf("authority received unexpected or inventory-bearing adoption payload: %+v", message)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		forwarded = true
		response := EngineRepairResponse{Found: true, Record: &canonical}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer upstream.Close()
	tokenPath := filepath.Join(t.TempDir(), "authority-token")
	if err := os.WriteFile(tokenPath, []byte("test-token"), 0600); err != nil {
		t.Fatal(err)
	}
	client := &AuthorityClient{URL: upstream.URL, TokenFile: tokenPath, Client: &http.Client{Timeout: time.Second, Transport: upstream.Client().Transport},
		boundIdentity: physical, role: StartupRoleVoterActive}
	stub := &supervisorAdoptionAuthority{EngineRepairRunnerAuthority: nil, client: client}
	s := &Supervisor{Authority: stub, Identity: physical, requested: physical, bound: physical,
		grant: StartupGrant{Identity: physical, Role: StartupRoleVoterActive}, ticket: "local-ticket"}
	local := httptest.NewServer(s.handler())
	defer local.Close()
	session := &Session{ticket: "local-ticket", identity: physical, role: StartupRoleVoterActive,
		binding: &BindReceipt{Identity: physical, Role: StartupRoleVoterActive},
		client:  &http.Client{Timeout: time.Second, Transport: rewriteRequestTransport{base: local.URL}}}
	got, err := session.AdoptPendingEngineRepair(t.Context(), "pending-1", proposalDigest, proposal)
	if err != nil {
		t.Fatalf("session -> supervisor -> AuthorityClient adoption failed: %v", err)
	}
	if !forwarded || got.Digest() != canonical.Digest() || len(got.Incarnations) == 0 {
		t.Fatalf("adoption did not return canonical inventory: forwarded=%v record=%+v", forwarded, got)
	}
}

func TestSupervisorChildEnvironmentScrubsInheritedIdentityAndAppliesGrant(t *testing.T) {
	grant := StartupGrant{Identity: Identity{SourceClusterID: "source", NodeID: "candidate", PodName: "rhiza-2"},
		Role: StartupRoleLearnerPendingRemove, Environment: map[string]string{
			"RHIZA_CLUSTER_MEMBERS": "pinned-genesis", "RHIZA_PEER_TOKEN": "fixed-peer-token", "RHIZA_LEARNER": "pinned-candidate",
		}}
	env := supervisorChildEnvironment([]string{"PATH=/bin", "RHIZA_NODE_ID=stale", "RHIZA_CLUSTER_MEMBERS=stale", "RHIZA_PEER_TOKEN=stale", "RHIZA_FENCE_TICKET=stale"}, grant, "/tmp/fence.sock", "ticket")
	values := make(map[string]string)
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			values[key] = value
		}
	}
	for key, want := range map[string]string{"RHIZA_NODE_ID": "candidate", "RHIZA_CLUSTER_ID": "source", "RHIZA_STARTUP_ROLE": string(StartupRoleLearnerPendingRemove),
		"RHIZA_CLUSTER_MEMBERS": "pinned-genesis", "RHIZA_PEER_TOKEN": "fixed-peer-token", "RHIZA_LEARNER": "pinned-candidate",
		"RHIZA_FENCE_SOCKET": "/tmp/fence.sock", "RHIZA_FENCE_TICKET": "ticket"} {
		if values[key] != want {
			t.Errorf("child env %s=%q, want %q", key, values[key], want)
		}
	}
}

type supervisorAdoptionAuthority struct {
	completionAuthority
	EngineRepairRunnerAuthority
	client *AuthorityClient
}

func (a *supervisorAdoptionAuthority) AdoptPendingEngineRepair(ctx context.Context, id Identity, pendingID, digest string, record EngineRepairRecord) (EngineRepairRecord, error) {
	return a.client.AdoptPendingEngineRepair(ctx, id, pendingID, digest, record)
}

type rewriteRequestTransport struct{ base string }

func (t rewriteRequestTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	copy := request.Clone(request.Context())
	copy.URL.Scheme, copy.URL.Host = "http", strings.TrimPrefix(t.base, "http://")
	return http.DefaultTransport.RoundTrip(copy)
}

func repairAdoptionRecords(digest string) (EngineRepairRecord, EngineRepairRecord) {
	canonical, _ := repairTestRecord()
	incarnations := append([]RepairIncarnation(nil), canonical.Incarnations...)
	canonical.Phase = RepairCandidatePrepared
	canonical.Binding.FreezeSlot, canonical.Binding.FreezeHash = 0, ""
	canonical.RemovalOperation = nil
	canonical.CandidateRuntimeBinding, canonical.CandidateRuntimeHash = nil, ""
	canonical.CandidateContinuations, canonical.Incarnations = nil, nil
	canonical.BootstrapRequestDigest = digest
	proposal := canonical
	proposal.SourceAdminCredential = canonical.SourceAdminCredential
	proposal.CandidateCredential = canonical.CandidateCredential
	if err := proposal.ValidateAdoptionProposal(); err != nil {
		panic(err)
	}
	canonical.Incarnations = incarnations
	return proposal, canonical
}
