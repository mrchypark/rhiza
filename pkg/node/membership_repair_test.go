package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type membershipRepairSessionFixture struct {
	mu                sync.Mutex
	identity          fencing.Identity
	view              *fencing.MembershipRepairView
	observations      map[string]fencing.MembershipRepairSourceObservation
	reserveView       *fencing.MembershipRepairView
	reserve           *fencing.MembershipRepairReserveRequest
	statusCode        atomic.Int32
	qualificationCode atomic.Int32
	authorized        []fencing.MembershipRepairPhase
	preparedPhase     *fencing.MembershipRepairPhase
	prepared          []fencing.MembershipRepairPhasePrepareRequest
	observed          []fencing.MembershipRepairPhaseObserveRequest
	completed         []fencing.MembershipRepairPhaseObserveRequest
	closureReq        fencing.PeerClosureRequest
	closureRead       atomic.Int32
	closureCall       atomic.Int32
}

func (f *membershipRepairSessionFixture) serve(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/v1/startup":
		_ = json.NewEncoder(w).Encode(fencing.Registration{Identity: f.identity})
	case "/v1/bind":
		var identity fencing.Identity
		if err := json.NewDecoder(r.Body).Decode(&identity); err != nil {
			http.Error(w, "bad identity", http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.identity = identity
		f.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	case "/v1/membership-repair/status":
		if code := int(f.statusCode.Load()); code != 0 {
			w.WriteHeader(code)
			return
		}
		f.mu.Lock()
		view := f.view
		observations := f.observations
		f.mu.Unlock()
		var message fencing.AuthorityMessage
		if err := json.NewDecoder(r.Body).Decode(&message); err != nil || message.RepairStatus == nil {
			http.Error(w, "bad status", http.StatusBadRequest)
			return
		}
		if view != nil {
			_ = json.NewEncoder(w).Encode(fencing.MembershipRepairStatusResult{Repair: view})
			return
		}
		if message.RepairStatus.TargetPodName == "" {
			_ = json.NewEncoder(w).Encode(fencing.MembershipRepairStatusResult{})
			return
		}
		if observation, ok := observations[message.RepairStatus.TargetPodName]; ok {
			_ = json.NewEncoder(w).Encode(fencing.MembershipRepairStatusResult{Observation: &observation})
			return
		}
		w.WriteHeader(http.StatusAccepted)
	case "/v1/membership-repair/reserve":
		var message fencing.AuthorityMessage
		if err := json.NewDecoder(r.Body).Decode(&message); err != nil || message.RepairReserve == nil {
			http.Error(w, "bad reservation", http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		request := *message.RepairReserve
		f.reserve = &request
		view := f.reserveView
		f.mu.Unlock()
		_ = json.NewEncoder(w).Encode(fencing.MembershipRepairReserveResult{Repair: view})
	case "/v1/membership-repair/authorize":
		var message fencing.AuthorityMessage
		if err := json.NewDecoder(r.Body).Decode(&message); err != nil || message.RepairPhase == nil {
			http.Error(w, "bad authorization", http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.authorized = append(f.authorized, message.RepairPhase.Phase)
		f.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	case "/v1/membership-repair/qualify":
		if code := int(f.qualificationCode.Load()); code != 0 {
			w.WriteHeader(code)
			return
		}
		var message fencing.AuthorityMessage
		if err := json.NewDecoder(r.Body).Decode(&message); err != nil || message.RepairQualification == nil {
			http.Error(w, "bad qualification", http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		var proof *network.MembershipRepairProbeResponse
		if f.view != nil {
			candidate, err := membershipRepairCandidate(*f.view)
			if err == nil && candidate.Member != nil {
				proof = &network.MembershipRepairProbeResponse{CandidateMember: *candidate.Member, CandidateBindRef: fencing.BootstrapAssignmentDigest(candidate.Assignment)}
			}
		}
		f.mu.Unlock()
		_ = json.NewEncoder(w).Encode(fencing.MembershipRepairQualificationResult{OperationID: message.RepairQualification.OperationID, Preclosure: proof})
	case "/v1/membership-repair/phase/prepare":
		var message fencing.AuthorityMessage
		if err := json.NewDecoder(r.Body).Decode(&message); err != nil || message.RepairPhasePrepare == nil {
			http.Error(w, "bad phase preparation", http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.prepared = append(f.prepared, *message.RepairPhasePrepare)
		var phase *fencing.MembershipRepairPhase
		if f.view != nil {
			if len(f.view.Progress.Candidates) > 0 {
				if retained, ok := retainedRepairPhase(*f.view, f.view.Progress.Candidates[0].Assignment.PodUID, message.RepairPhasePrepare.Kind); ok {
					phase = &retained
				}
			}
			if phase == nil && f.preparedPhase != nil && f.preparedPhase.Kind == message.RepairPhasePrepare.Kind {
				copy := *f.preparedPhase
				phase = &copy
				f.view.Progress.Phases = append(f.view.Progress.Phases, copy)
			}
		}
		f.mu.Unlock()
		if phase == nil {
			http.Error(w, "phase unavailable", http.StatusConflict)
			return
		}
		_ = json.NewEncoder(w).Encode(fencing.MembershipRepairPhasePrepareResult{Phase: *phase})
	case "/v1/membership-repair/phase/observe", "/v1/membership-repair/phase/complete":
		var message fencing.AuthorityMessage
		if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
			http.Error(w, "bad phase result", http.StatusBadRequest)
			return
		}
		var request *fencing.MembershipRepairPhaseObserveRequest
		result := fencing.MembershipRepairPhaseObserveResult{}
		if r.URL.Path == "/v1/membership-repair/phase/observe" {
			request = message.RepairPhaseObserve
		} else {
			request = message.RepairPhaseComplete
			result.Complete = true
		}
		if request == nil {
			http.Error(w, "missing phase result", http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		if r.URL.Path == "/v1/membership-repair/phase/observe" {
			f.observed = append(f.observed, *request)
		} else {
			f.completed = append(f.completed, *request)
		}
		f.mu.Unlock()
		result.OperationID, result.PhaseHash = request.OperationID, request.PhaseHash
		_ = json.NewEncoder(w).Encode(result)
	case "/v1/peer-closure/status":
		f.closureRead.Add(1)
		http.NotFound(w, r)
	case "/v1/peer-closure/request":
		var message fencing.AuthorityMessage
		if err := json.NewDecoder(r.Body).Decode(&message); err != nil || message.PeerClosure == nil {
			http.Error(w, "bad closure", http.StatusBadRequest)
			return
		}
		f.closureCall.Add(1)
		f.closureReq = *message.PeerClosure
		abortSlot := *message.PeerClosure.ExpectedAbortSlot
		_ = json.NewEncoder(w).Encode(fencing.PeerClosureReceipt{
			OperationID: "closure-op", SourceClusterID: message.Identity.SourceClusterID,
			BindingUID: "root", StatefulSetUID: "sts", RequestHash: "closure-hash",
			ExpectedConfigID: message.PeerClosure.ExpectedConfigID, ExpectedAbortSlot: &abortSlot,
			TargetNodeID: message.PeerClosure.TargetNodeID, TargetWALIdentity: message.PeerClosure.TargetWALIdentity,
			Targets: []fencing.PeerClosureTarget{{NodeID: message.PeerClosure.TargetNodeID, Pod: message.PeerClosure.TargetNodeID, PodUID: "old-pod", WALIdentity: message.PeerClosure.TargetWALIdentity}},
			ProofID: "proof", ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true,
		})
	default:
		http.NotFound(w, r)
	}
}

func TestMembershipRepairStartsFromAuthorityObservedSourceSlot(t *testing.T) {
	n, identities, _, _ := newMembershipOperationNode(t)
	for i := range n.config.Members {
		id := string(n.config.Members[i].ID)
		n.config.Members[i].URL = "http://" + id + ".svc"
		n.config.Members[i].PeerURL = "quic://" + id + ".svc:9090"
	}
	caller, view := membershipRepairFixture(t, n, identities, fencing.MembershipRepairCandidateBound, fencing.MembershipRepairRemove)
	view.Progress = fencing.MembershipRepairProgress{Revision: 4, Stage: fencing.MembershipRepairAdmitted}
	membershipBytes, _ := json.Marshal(n.core.CurrentCluster().Members)
	membershipHash := sha256.Sum256(membershipBytes)
	observation := fencing.MembershipRepairSourceObservation{
		ConfigID: uint64(n.core.ConfigID()), AbortSlot: 0, TargetPodName: "c", TargetNodeID: "c",
		TargetPodUID: "old-pod", TargetWALIdentity: identities["c"], ReplacementPodUID: "new-pod",
		MembershipHash: hex.EncodeToString(membershipHash[:]),
	}
	session, authority := newMembershipRepairSession(t, caller, nil, 0)
	authority.observations = map[string]fencing.MembershipRepairSourceObservation{"c": observation}
	authority.reserveView = &view
	n.fenceSession = session
	n.ready.Store(true)
	if err := n.resumeMembershipRepair(context.Background()); err != nil {
		t.Fatalf("reserve observed repair: %v", err)
	}
	if authority.reserve == nil || authority.reserve.ExpectedConfigID != observation.ConfigID || authority.reserve.ExpectedAbortSlot == nil ||
		*authority.reserve.ExpectedAbortSlot != observation.AbortSlot || authority.reserve.TargetNodeID != observation.TargetNodeID || authority.reserve.TargetWALIdentity != observation.TargetWALIdentity {
		t.Fatalf("reservation did not preserve authority observation: %+v", authority.reserve)
	}
}

func TestMembershipRepairResumesAdmittedReservationUntilCandidateExists(t *testing.T) {
	n, identities, _, _ := newMembershipOperationNode(t)
	caller, view := membershipRepairFixture(t, n, identities, fencing.MembershipRepairCandidateBound, fencing.MembershipRepairRemove)
	view.Progress = fencing.MembershipRepairProgress{Revision: 4, Stage: fencing.MembershipRepairAdmitted}
	session, authority := newMembershipRepairSession(t, caller, &view, 0)
	authority.reserveView = &view
	n.fenceSession = session
	n.ready.Store(true)
	if err := n.resumeMembershipRepair(context.Background()); err != nil {
		t.Fatalf("resume admitted reservation: %v", err)
	}
	if authority.reserve == nil || authority.reserve.TargetNodeID != view.Authorization.OldIdentity.NodeID || authority.reserve.TargetWALIdentity != view.Authorization.OldIdentity.WALIdentity || authority.reserve.ExpectedAbortSlot == nil || *authority.reserve.ExpectedAbortSlot != uint64(*view.Authorization.ExpectedAbortSlot) {
		t.Fatalf("resume changed retained reservation tuple: %+v", authority.reserve)
	}
}

func newMembershipRepairSession(t *testing.T, identity fencing.Identity, view *fencing.MembershipRepairView, statusCode int) (*fencing.Session, *membershipRepairSessionFixture) {
	t.Helper()
	path := shortTestUnixSocketPath(t, "repair.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	startupIdentity := identity
	startupIdentity.WALIdentity = ""
	fixture := &membershipRepairSessionFixture{identity: startupIdentity, view: view}
	fixture.statusCode.Store(int32(statusCode))
	server := &http.Server{Handler: http.HandlerFunc(fixture.serve)}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close(); _ = listener.Close() })
	session, err := fencing.Connect(context.Background(), path, "ticket", identity.SourceClusterID, identity.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.BindWAL(context.Background(), identity.WALIdentity); err != nil {
		t.Fatal(err)
	}
	return session, fixture
}

func membershipRepairFixture(t *testing.T, n *Node, identities map[quepaxa.NodeID]string, stage fencing.MembershipRepairStage, phaseKind fencing.MembershipRepairPhaseKind) (fencing.Identity, fencing.MembershipRepairView) {
	t.Helper()
	membership := n.core.CurrentCluster()
	for i := range membership.Members {
		membership.Members[i].URL = "http://" + string(membership.Members[i].ID)
		membership.Members[i].PeerURL = "http://" + string(membership.Members[i].ID) + ":peer"
		membership.Members[i].LogURL = "http://" + string(membership.Members[i].ID) + ":log"
	}
	membershipBytes, err := json.Marshal(membership.Members)
	if err != nil {
		t.Fatal(err)
	}
	membershipDigest := sha256.Sum256(membershipBytes)
	caller := fencing.Identity{SourceClusterID: "cluster", NodeID: "a", PodUID: "pod-a", RunID: "run-a", WALIdentity: identities["a"]}
	old := fencing.Identity{SourceClusterID: "cluster", NodeID: "c", PodUID: "pod-c", RunID: "run-c", WALIdentity: identities["c"]}
	expectedAbortSlot := quepaxa.Slot(0)
	authorization := fencing.MembershipRepairAuthorization{
		OperationID: "repair-op", RequestHash: stringsOf('a', 64), AuthorizationHash: stringsOf('b', 64),
		RootUID: "root", StatefulSetUID: "sts", LedgerUID: "ledger", SourceClusterID: "cluster",
		MembershipHash: hex.EncodeToString(membershipDigest[:]), OldIdentity: old, Membership: membership, ExpectedAbortSlot: &expectedAbortSlot,
	}
	candidateUID := "candidate-pod"
	candidateID, err := fencing.ReplacementNodeID(candidateUID)
	if err != nil {
		t.Fatal(err)
	}
	assignment := fencing.BootstrapAssignment{
		OperationID: "candidate-init", RequestHash: stringsOf('c', 64), AuthorizationHash: stringsOf('d', 64),
		Purpose: fencing.BootstrapLearner, RootUID: authorization.RootUID, StatefulSetUID: authorization.StatefulSetUID,
		SourceClusterID: authorization.SourceClusterID, PodName: "c", PodUID: candidateUID, NodeID: candidateID,
		CredentialName: "candidate-secret", CredentialUID: "candidate-secret-uid", MembershipHash: authorization.MembershipHash,
		ProfileHash: stringsOf('e', 64), LedgerUID: authorization.LedgerUID,
	}
	candidateMember := quepaxa.Member{ID: quepaxa.NodeID(candidateID), URL: "http://candidate", PeerURL: "http://candidate-peer", LogURL: "http://candidate-log", PublicKey: peerMember("cluster", quepaxa.NodeID(candidateID), "candidate-key").PublicKey, WALIdentity: "candidate-wal"}
	candidate := fencing.MembershipRepairCandidate{Assignment: assignment, Member: &candidateMember}
	phase := fencing.MembershipRepairPhase{Kind: phaseKind, CandidatePodUID: candidateUID}
	zero := quepaxa.Slot(0)
	phase.AbortSlot = &zero
	if phaseKind == fencing.MembershipRepairRemove {
		phase.Change = network.MembershipChange{
			OperationID: "repair-remove", ClusterID: "cluster", ExpectedConfigID: uint(membership.ConfigID), ExpectedAbortSlot: 0, Remove: "c",
			Fence: &network.MembershipFence{NodeID: "c", WALIdentity: old.WALIdentity, WorkloadUID: authorization.StatefulSetUID, Confirmed: true, Evidence: "retained-closure"},
		}
	} else {
		phase.Change = network.MembershipChange{
			OperationID: "repair-add", ClusterID: "cluster", ExpectedConfigID: uint(membership.ConfigID) + 1, ExpectedAbortSlot: 0, Add: &candidateMember,
		}
	}
	phase.Hash, err = fencing.MembershipRepairPhaseHash(phase)
	if err != nil {
		t.Fatal(err)
	}
	view := fencing.MembershipRepairView{Authorization: authorization, Progress: fencing.MembershipRepairProgress{
		Revision: 3, Stage: stage, Candidates: []fencing.MembershipRepairCandidate{candidate}, Phases: []fencing.MembershipRepairPhase{phase},
	}}
	return caller, view
}

func stringsOf(value byte, count int) string {
	return strings.Repeat(string(value), count)
}

func TestMembershipRepairWorkerExecutesRetainedRemovalThroughNodePath(t *testing.T) {
	ctx := context.Background()
	n, identities, _, transport := newMembershipOperationNode(t)
	transport.disable("c")
	caller, view := membershipRepairFixture(t, n, identities, fencing.MembershipRepairOldClosed, fencing.MembershipRepairRemove)
	session, authority := newMembershipRepairSession(t, caller, &view, 0)
	n.fenceSession = session
	n.ready.Store(true)
	if err := n.resumeMembershipRepair(ctx); err != nil {
		t.Fatalf("resume retained removal: %v", err)
	}
	if got := n.core.CurrentCluster(); got.ConfigID != 2 || len(got.Members) != 2 || got.Members[0].ID != "a" || got.Members[1].ID != "b" {
		t.Fatalf("membership after retained removal: %+v", got)
	}
	if len(authority.authorized) != 1 || authority.authorized[0].Change.OperationID != "repair-remove" {
		t.Fatalf("authority did not authorize exact retained request: %+v", authority.authorized)
	}
}

func TestMembershipRepairRefusesOtherMutationAndAuthorityUncertainty(t *testing.T) {
	for _, test := range []struct {
		name       string
		statusCode int
	}{
		{name: "different request"},
		{name: "authority unavailable", statusCode: http.StatusServiceUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			n, identities, bucket, _ := newMembershipOperationNode(t)
			caller, view := membershipRepairFixture(t, n, identities, fencing.MembershipRepairOldClosed, fencing.MembershipRepairRemove)
			session, authority := newMembershipRepairSession(t, caller, &view, test.statusCode)
			n.fenceSession = session
			request := view.Progress.Phases[0].Change
			if test.statusCode == 0 {
				request.OperationID = "unapproved-remove"
			}
			err := n.changeMembership(context.Background(), request)
			if test.statusCode != 0 {
				if err != fencing.ErrUnavailable {
					t.Fatalf("uncertain authority error=%v, want ErrUnavailable", err)
				}
			} else if err != network.ErrRequestConflict {
				t.Fatalf("unapproved mutation error=%v, want conflict", err)
			}
			if len(authority.authorized) != 0 {
				t.Fatalf("unexpected phase authorization: %+v", authority.authorized)
			}
			assertNoMembershipOperation(t, context.Background(), n, bucket, 1)
		})
	}
}

func TestMembershipRepairWorkerRefusesClosureWhenLiveMembershipDiffers(t *testing.T) {
	n, identities, _, _ := newMembershipOperationNode(t)
	caller, view := membershipRepairFixture(t, n, identities, fencing.MembershipRepairCandidateBound, fencing.MembershipRepairRemove)
	session, authority := newMembershipRepairSession(t, caller, &view, 0)
	n.fenceSession = session
	n.ready.Store(true)
	if err := n.resumeMembershipRepair(context.Background()); err != network.ErrRequestConflict {
		t.Fatalf("closure with different live membership error=%v, want conflict", err)
	}
	if authority.closureRead.Load() != 0 || authority.closureCall.Load() != 0 {
		t.Fatalf("closure was requested despite a live membership mismatch: %+v", authority.closureReq)
	}
}

func TestMembershipRepairWorkerRequestsClosureForBoundCandidate(t *testing.T) {
	n, identities := newRepairCoordinatorNode(t)
	caller, view := membershipRepairFixture(t, n, identities, fencing.MembershipRepairCandidateBound, fencing.MembershipRepairRemove)
	other := view.Progress.Candidates[0]
	other.Assignment.OperationID = "other-init"
	other.Assignment.RequestHash = stringsOf('f', 64)
	other.Assignment.AuthorizationHash = stringsOf('1', 64)
	other.Assignment.PodName = "a"
	other.Assignment.PodUID = "other-pod"
	other.Assignment.NodeID, _ = fencing.ReplacementNodeID(other.Assignment.PodUID)
	other.Assignment.CredentialName = "other-credential"
	other.Assignment.CredentialUID = "other-credential-uid"
	other.Assignment.PublicMember.ID = other.Assignment.NodeID
	other.Assignment.PublicMember.PublicKey = strings.Repeat("0", 64)
	other.Assignment.PublicMember.URL = "http://other"
	other.Assignment.PublicMember.PeerURL = "quic://other"
	other.Assignment.Durability = "async"
	other.Assignment.StoreIdentityHash = stringsOf('2', 64)
	other.Member = nil
	view.Progress.Candidates = append(view.Progress.Candidates, other)
	if err := view.ValidateFor(caller); err != nil {
		t.Fatalf("test candidates are invalid: %v assignment=%+v", err, other.Assignment)
	}
	session, authority := newMembershipRepairSession(t, caller, &view, 0)
	n.fenceSession = session
	n.ready.Store(true)
	if err := n.resumeMembershipRepair(context.Background()); err != nil {
		t.Fatalf("request old-target closure: %v", err)
	}
	if authority.closureRead.Load() != 1 || authority.closureCall.Load() != 1 || authority.closureReq.TargetNodeID != "c" || authority.closureReq.TargetWALIdentity != identities["c"] || authority.closureReq.ExpectedConfigID != 1 || authority.closureReq.ExpectedAbortSlot == nil || *authority.closureReq.ExpectedAbortSlot != 0 {
		t.Fatalf("closure request did not preserve exact old target and round: %+v", authority.closureReq)
	}
	if len(authority.prepared) != 1 || authority.prepared[0].OperationID != view.Authorization.OperationID || authority.prepared[0].Kind != fencing.MembershipRepairRemove {
		t.Fatalf("CandidateBound closure did not prepare its retained REMOVE phase: %+v", authority.prepared)
	}
}

func TestMembershipRepairCandidateBoundRequiresCandidateCatchUpBeforeClosure(t *testing.T) {
	n, identities := newRepairCoordinatorNode(t)
	caller, view := membershipRepairFixture(t, n, identities, fencing.MembershipRepairCandidateBound, fencing.MembershipRepairRemove)
	session, authority := newMembershipRepairSession(t, caller, &view, 0)
	authority.qualificationCode.Store(http.StatusServiceUnavailable)
	n.fenceSession = session
	n.ready.Store(true)

	// CandidateBound records a runtime binding, not a certified learner prefix.
	// With no candidate proof available, the worker must not begin closing the
	// old voter. The existing implementation currently requests closure here.
	if err := n.resumeMembershipRepair(context.Background()); !errors.Is(err, fencing.ErrUnavailable) {
		t.Fatalf("resume unproven candidate binding error=%v, want unavailable qualification", err)
	}
	if authority.closureRead.Load() != 0 || authority.closureCall.Load() != 0 {
		t.Fatalf("closure began before candidate catch-up proof: reads=%d requests=%d request=%+v", authority.closureRead.Load(), authority.closureCall.Load(), authority.closureReq)
	}
}

func TestMembershipRepairOldClosedCreatesAndAuthorizesRemoveWhenPhaseMissing(t *testing.T) {
	n, identities, _, _ := newMembershipOperationNode(t)
	caller, view := membershipRepairFixture(t, n, identities, fencing.MembershipRepairOldClosed, fencing.MembershipRepairRemove)
	session, authority := newMembershipRepairSession(t, caller, &view, 0)
	phase := view.Progress.Phases[0]
	view.Progress.Phases = nil
	authority.preparedPhase = &phase
	n.fenceSession = session
	n.ready.Store(true)

	if err := n.resumeMembershipRepair(context.Background()); err != nil {
		t.Fatalf("resume closed old voter without retained phase: %v", err)
	}
	if len(authority.authorized) != 1 {
		t.Fatalf("closed old voter was not advanced through exact REMOVE authorization: %+v", authority.authorized)
	}
	authorizedPhase := authority.authorized[0]
	if authorizedPhase.Kind != fencing.MembershipRepairRemove || authorizedPhase.CandidatePodUID != view.Progress.Candidates[0].Assignment.PodUID ||
		authorizedPhase.Change.Remove != "c" || authorizedPhase.Change.ExpectedConfigID != uint(view.Authorization.Membership.ConfigID) ||
		authorizedPhase.Change.Fence == nil || authorizedPhase.Change.Fence.WALIdentity != identities["c"] {
		t.Fatalf("authorized REMOVE did not preserve exact old slot and candidate: %+v", authorizedPhase)
	}
}

func TestMembershipRepairAddedCompletesOnlyAfterCandidateTerminalAck(t *testing.T) {
	ctx := context.Background()
	n, identities, bucket, transport := newMembershipOperationNodeWithAdmission(t)
	transport.disable("c")
	initialCluster := n.core.CurrentCluster()
	caller, view := membershipRepairFixture(t, n, identities, fencing.MembershipRepairAdded, fencing.MembershipRepairAdd)
	candidate := &view.Progress.Candidates[0]
	member, learnerWAL := registerMembershipLearnerWithWAL(t, bucket, quepaxa.NodeID(candidate.Assignment.NodeID))
	candidate.Member = &member
	learnerCore, err := quepaxa.NewLearner(quepaxa.Config{
		NodeID: member.ID, Cluster: initialCluster, WAL: learnerWAL, Transport: transport, EnableReconfiguration: true,
	})
	if err != nil {
		t.Fatalf("create candidate learner core: %v", err)
	}
	transport.mu.Lock()
	transport.learners[member.ID] = learnerCore
	transport.mu.Unlock()
	remove := network.MembershipChange{
		OperationID: "setup-remove-c", ClusterID: "cluster", ExpectedConfigID: 1, Remove: "c",
		Fence: &network.MembershipFence{NodeID: "c", WALIdentity: identities["c"], WorkloadUID: "setup", Confirmed: true, Evidence: "setup"},
	}
	if err := n.changeMembership(ctx, remove); err != nil {
		t.Fatalf("prepare removed state: %v", err)
	}

	phase := &view.Progress.Phases[0]
	phase.Change = network.MembershipChange{OperationID: "repair-add", ClusterID: "cluster", ExpectedConfigID: uint(n.core.ConfigID()), Add: &member}
	phase.Hash, _ = fencing.MembershipRepairPhaseHash(*phase)
	transport.mu.Lock()
	terminalSlot := n.core.Tip() + 17
	transport.blockLearnerTerminalSlot, transport.learnerTerminalSlot = terminalSlot, terminalSlot
	transport.mu.Unlock()
	if err := n.changeMembership(ctx, phase.Change); !errors.Is(err, quepaxa.ErrQuorumUnavailable) {
		t.Fatalf("initial ADD without learner terminal ACK error=%v, want quorum unavailable", err)
	}

	session, authority := newMembershipRepairSession(t, caller, &view, 0)
	n.fenceSession = session
	n.ready.Store(true)
	if err := n.resumeMembershipRepair(ctx); !errors.Is(err, quepaxa.ErrQuorumUnavailable) {
		t.Fatalf("repair completion without learner terminal ACK error=%v, want quorum unavailable", err)
	}
	if len(authority.completed) != 0 {
		t.Fatalf("authority phase completed before candidate terminal ACK: %+v", authority.completed)
	}

	transport.mu.Lock()
	transport.blockLearnerTerminalSlot = 0
	transport.mu.Unlock()
	if err := n.resumeMembershipRepair(ctx); err != nil {
		t.Fatalf("resume ADD after candidate terminal ACK becomes available: %v", err)
	}
	if len(authority.observed) != 1 || len(authority.completed) != 1 || !reflect.DeepEqual(authority.completed[0], authority.observed[0]) || authority.completed[0].TerminalSlot != uint64(terminalSlot) {
		t.Fatalf("exact certified ADD was not observed and completed after ACK: observed=%+v completed=%+v", authority.observed, authority.completed)
	}
}

func TestMembershipOperationRetryUsesExactHistoricalTarget(t *testing.T) {
	ctx := context.Background()
	n, identities, bucket, transport := newMembershipOperationNodeWithAdmission(t)
	transport.disable("c")
	remove := network.MembershipChange{
		OperationID: "remove-c-first", ClusterID: "cluster", ExpectedConfigID: 1, Remove: "c",
		Fence: &network.MembershipFence{NodeID: "c", WALIdentity: identities["c"], WorkloadUID: "setup", Confirmed: true, Evidence: "setup"},
	}
	if err := n.changeMembership(ctx, remove); err != nil {
		t.Fatalf("complete first removal: %v", err)
	}
	removedTarget := n.core.CurrentCluster()
	member := registerMembershipLearner(t, bucket, "fresh")
	add := network.MembershipChange{OperationID: "add-after-removal", ClusterID: "cluster", ExpectedConfigID: 2, Add: &member}
	if err := n.changeMembership(ctx, add); err != nil {
		t.Fatalf("complete later addition: %v", err)
	}
	if n.core.CurrentCluster().ConfigID != removedTarget.ConfigID+1 {
		t.Fatalf("later configuration did not advance from exact historical target: %+v", n.core.CurrentCluster())
	}
	tip := n.core.Tip()
	if err := n.changeMembership(ctx, remove); err != nil {
		t.Fatalf("retry exact historical removal: %v", err)
	}
	if n.core.Tip() != tip {
		t.Fatalf("historical retry changed certified tip: before=%d after=%d", tip, n.core.Tip())
	}
}

func newRepairCoordinatorNode(t *testing.T) (*Node, map[quepaxa.NodeID]string) {
	t.Helper()
	ids := []quepaxa.NodeID{"a", "b", "c"}
	identities := make(map[quepaxa.NodeID]string, len(ids))
	members := make([]quepaxa.Member, 0, len(ids))
	wals := make(map[quepaxa.NodeID]*qlog.WAL, len(ids))
	for _, id := range ids {
		member := peerMember("cluster", id, string(id)+"-token")
		member.URL = "http://" + string(id)
		member.PeerURL = "http://" + string(id) + ":peer"
		member.LogURL = "http://" + string(id) + ":log"
		wal, err := qlog.Open(filepath.Join(t.TempDir(), "qlog"))
		if err != nil {
			t.Fatal(err)
		}
		walIdentity := sha256.Sum256([]byte("wal-" + string(id)))
		if err := wal.BindIdentity(walIdentity[:]); err != nil {
			t.Fatal(err)
		}
		identities[id] = hex.EncodeToString(walIdentity[:])
		member.WALIdentity = identities[id]
		members = append(members, member)
		wals[id] = wal
		t.Cleanup(func() { _ = wal.Close() })
	}
	transport := &membershipOperationTransport{cores: make(map[quepaxa.NodeID]*quepaxa.Core), disabled: make(map[quepaxa.NodeID]bool), wals: wals, learners: make(map[quepaxa.NodeID]*quepaxa.Core), learnerTerminalAcks: make(map[quepaxa.NodeID]int)}
	cluster := quepaxa.Cluster{ConfigID: 1, Members: members}
	for _, member := range members {
		core, err := quepaxa.New(quepaxa.Config{NodeID: member.ID, Cluster: cluster, WAL: wals[member.ID], Transport: transport, EnableReconfiguration: true})
		if err != nil {
			t.Fatal(err)
		}
		transport.cores[member.ID] = core
	}
	return &Node{core: transport.cores["a"]}, identities
}

func TestMembershipRepairAuthorizesExactRetainedAddBeforeAbortLifecycle(t *testing.T) {
	ctx := context.Background()
	n, identities, bucket, transport := newMembershipOperationNode(t)
	transport.disable("c")
	caller, view := membershipRepairFixture(t, n, identities, fencing.MembershipRepairRemoved, fencing.MembershipRepairAdd)

	remove := network.MembershipChange{
		OperationID: "setup-remove-c", ClusterID: "cluster", ExpectedConfigID: 1, Remove: "c",
		Fence: &network.MembershipFence{NodeID: "c", WALIdentity: identities["c"], WorkloadUID: "setup", Confirmed: true, Evidence: "setup"},
	}
	if err := n.changeMembership(ctx, remove); err != nil {
		t.Fatalf("prepare post-removal state: %v", err)
	}
	member, learnerWAL := registerMembershipLearnerWithWAL(t, bucket, quepaxa.NodeID(view.Progress.Candidates[0].Assignment.NodeID))
	_ = learnerWAL
	candidate := &view.Progress.Candidates[0]
	candidate.Member = &member
	phase := &view.Progress.Phases[0]
	phase.Change = network.MembershipChange{OperationID: "repair-add", ClusterID: "cluster", ExpectedConfigID: 2, Add: &member}
	phase.Hash, _ = fencing.MembershipRepairPhaseHash(*phase)
	view.Progress.Revision++

	current := n.core.CurrentCluster()
	target := quepaxa.Cluster{ConfigID: current.ConfigID + 1, Members: append(append([]quepaxa.Member(nil), current.Members...), member)}
	if _, err := n.core.BeginReconfigurationAt(ctx, target, 0); err != nil {
		t.Fatalf("freeze pending ADD: %v", err)
	}
	operation := membershipOperation{Request: phase.Change, Target: target}
	encoded, err := json.Marshal(operation)
	if err != nil {
		t.Fatal(err)
	}
	if err := matchVoterRegistration(ctx, bucket, n.membershipOperationKey(phase.Change), encoded, nil); err != nil {
		t.Fatalf("persist exact ADD journal: %v", err)
	}
	session, authority := newMembershipRepairSession(t, caller, &view, 0)
	n.fenceSession = session
	if err := n.abortMembership(ctx, phase.Change); err != nil {
		t.Fatalf("abort exact retained ADD: %v", err)
	}
	if len(authority.authorized) != 1 || authority.authorized[0].Hash != phase.Hash {
		t.Fatalf("abort was not gated by exact retained phase: %+v", authority.authorized)
	}
	if pending, ok := n.core.PendingReconfiguration(); ok || pending.ConfigID != 0 {
		t.Fatalf("ADD remained pending after authorized abort: %+v", pending)
	}
}
