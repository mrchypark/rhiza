package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestNodeMembershipRepairCompletionRequiresExactTerminalAck(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	n, identities, bucket, transport := newMembershipOperationNodeWithAdmission(t)
	transport.disable("c")
	caller, view := membershipRepairFixture(t, n, identities, fencing.MembershipRepairAdded, fencing.MembershipRepairAdd)
	candidate := &view.Progress.Candidates[0]
	member, learnerWAL := registerMembershipLearnerWithWAL(t, bucket, quepaxa.NodeID(candidate.Assignment.NodeID))
	defer learnerWAL.Close()
	candidate.Member = &member
	initialCluster := n.core.CurrentCluster()
	learnerCore, err := quepaxa.NewLearner(quepaxa.Config{NodeID: member.ID, Cluster: initialCluster, WAL: learnerWAL, Transport: transport, EnableReconfiguration: true})
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
	add := network.MembershipChange{OperationID: "repair-add", ClusterID: "cluster", ExpectedConfigID: uint(n.core.ConfigID()), Add: &member}
	phase := &view.Progress.Phases[0]
	phase.Change = add
	phase.Hash, err = fencing.MembershipRepairPhaseHash(*phase)
	if err != nil {
		t.Fatal(err)
	}
	transport.mu.Lock()
	blockedTerminal := n.core.Tip() + 17
	transport.blockLearnerTerminalSlot, transport.learnerTerminalSlot = blockedTerminal, blockedTerminal
	transport.mu.Unlock()
	if err := n.changeMembership(ctx, add); !errors.Is(err, quepaxa.ErrQuorumUnavailable) {
		t.Fatalf("initial ADD without candidate terminal ACK error=%v, want quorum unavailable", err)
	}
	identitySession, _ := newMembershipRepairSession(t, caller, &view, 0)
	n.fenceSession = identitySession
	n.ready.Store(true)
	request := nodeCompletionRequest(t, n, view, *candidate, add, caller)
	if _, err := n.completeMembershipRepairTerminal(ctx, request); !errors.Is(err, quepaxa.ErrQuorumUnavailable) {
		t.Fatalf("completion without candidate terminal ACK error=%v, want quorum unavailable", err)
	}

	// Reconstruct a fresh Core and runtime session from the same durable WAL,
	// then retry under a fresh authority challenge and RunID.
	restartedCore, err := quepaxa.New(quepaxa.Config{
		NodeID: "a", Cluster: initialCluster, WAL: transport.wals["a"], Transport: transport,
		EnableReconfiguration:    true,
		ReconfigurationAdmission: func(context.Context, quepaxa.Cluster, quepaxa.Slot, [32]byte) error { return nil },
	})
	if err != nil {
		t.Fatalf("replay runtime Core from durable WAL: %v", err)
	}
	transport.mu.Lock()
	transport.cores["a"] = restartedCore
	transport.mu.Unlock()
	n.core = restartedCore
	caller.RunID = "run-a-restarted"
	identitySession, _ = newMembershipRepairSession(t, caller, &view, 0)
	n.fenceSession = identitySession
	request.Challenge = stringsOf('c', 64)
	request.AttemptRevision++
	request.ScopeDigest = stringsOf('d', 64)
	request.ExecutorRunID = caller.RunID

	transport.mu.Lock()
	transport.blockLearnerTerminalSlot = 0
	transport.mu.Unlock()
	response, err := n.completeMembershipRepairTerminal(ctx, request)
	if err != nil {
		t.Fatalf("complete exact terminal after candidate ACK becomes available: %v", err)
	}
	if err := network.ValidateMembershipRepairCompletionRequest(request); err != nil {
		t.Fatalf("constructed completion request: %v", err)
	}
	if response.Outcome != "completed" || response.TerminalSlot != request.TerminalSlot || response.TerminalHash != request.TerminalHash ||
		response.ExecutorNodeID != caller.NodeID || response.ExecutorWALIdentity != caller.WALIdentity || response.RequestDigest == "" {
		t.Fatalf("completion response does not bind exact execution: %+v", response)
	}
	assertDurableArchivedMembership(t, ctx, n, bucket, quepaxa.Slot(request.TerminalSlot))

	// A later certified configuration must not substitute its latest terminal
	// for the retained ADD or make an exact historical retry fail by itself.
	n.fenceSession = nil
	laterRemoval := network.MembershipChange{
		OperationID: "later-remove-candidate", ClusterID: "cluster", ExpectedConfigID: uint(n.core.ConfigID()), Remove: member.ID,
		Fence: &network.MembershipFence{NodeID: member.ID, WALIdentity: member.WALIdentity, WorkloadUID: "later-test", Confirmed: true, Evidence: "later-test"},
	}
	if err := n.changeMembership(ctx, laterRemoval); err != nil {
		t.Fatalf("create later certified membership transition: %v", err)
	}
	caller.RunID = "run-a-historical-retry"
	identitySession, _ = newMembershipRepairSession(t, caller, &view, 0)
	n.fenceSession = identitySession
	request.Challenge = stringsOf('e', 64)
	request.AttemptRevision++
	request.ScopeDigest = stringsOf('f', 64)
	request.ExecutorRunID = caller.RunID
	historicalResponse, err := n.completeMembershipRepairTerminal(ctx, request)
	if err != nil || historicalResponse.TerminalSlot != request.TerminalSlot {
		t.Fatalf("exact historical ADD completion was rejected after a later configuration: response=%+v err=%v", historicalResponse, err)
	}
}

func TestNodeMembershipRepairCompletionRejectsCompactedChallengedTerminal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	n, identities, bucket, transport := newMembershipOperationNodeWithAdmission(t)
	transport.disable("c")
	caller, view := membershipRepairFixture(t, n, identities, fencing.MembershipRepairAdded, fencing.MembershipRepairAdd)
	candidate := &view.Progress.Candidates[0]
	member, learnerWAL := registerMembershipLearnerWithWAL(t, bucket, quepaxa.NodeID(candidate.Assignment.NodeID))
	defer learnerWAL.Close()
	candidate.Member = &member
	initialCluster := n.core.CurrentCluster()
	learnerCore, err := quepaxa.NewLearner(quepaxa.Config{NodeID: member.ID, Cluster: initialCluster, WAL: learnerWAL, Transport: transport, EnableReconfiguration: true})
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
	add := network.MembershipChange{OperationID: "repair-add", ClusterID: "cluster", ExpectedConfigID: uint(n.core.ConfigID()), Add: &member}
	phase := &view.Progress.Phases[0]
	phase.Change = add
	phase.Hash, err = fencing.MembershipRepairPhaseHash(*phase)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.changeMembership(ctx, add); err != nil {
		t.Fatalf("certify and ACK exact ADD terminal: %v", err)
	}
	transport.mu.Lock()
	delete(transport.learners, member.ID)
	transport.cores[member.ID] = learnerCore
	transport.ids = append(transport.ids, member.ID)
	transport.mu.Unlock()
	identitySession, authority := newMembershipRepairSession(t, caller, &view, 0)
	n.fenceSession = identitySession
	n.ready.Store(true)
	request := nodeCompletionRequest(t, n, view, *candidate, add, caller)
	terminal := quepaxa.Slot(request.TerminalSlot)
	server := network.NewServer(n.core, nil, "cluster", true, nil)
	t.Cleanup(server.Close)
	server.SetRecoveryArchive("admin-token", func(context.Context) error { return nil })
	server.SetMembershipRepairCompletion(n.completeMembershipRepairTerminal)
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	postCompletion := func() *httptest.ResponseRecorder {
		t.Helper()
		httpRequest := httptest.NewRequest(http.MethodPost, "/recovery/probe", strings.NewReader(string(body)))
		httpRequest.Header.Set("Authorization", "Bearer admin-token")
		httpRequest.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		server.ServeHTTP(response, httpRequest)
		return response
	}

	// Establish that this exact challenge, journal, and terminal are valid
	// before compaction; the retry below must fail specifically at history use.
	validResponse := postCompletion()
	if validResponse.Code != http.StatusOK {
		t.Fatalf("valid pre-compaction completion failed status=%d body=%s", validResponse.Code, validResponse.Body.String())
	}
	if _, err := network.StrictDecodeMembershipRepairCompletionResponse(validResponse.Body.Bytes(), request, "admin-token"); err != nil {
		t.Fatalf("decode authenticated pre-compaction completion response: %v", err)
	}

	// Build and certify an actual checkpoint beyond the terminal, then compact
	// the Core WAL so CompleteDecision must report ErrCompacted from its real
	// recovery floor rather than from a mocked callback.
	if _, _, err := n.core.Propose(ctx, []byte("post-add-before-compaction")); err != nil {
		t.Fatalf("advance beyond ADD terminal: %v", err)
	}
	index := n.core.Tip()
	prefix, ok := n.core.PrefixHash(index)
	if !ok {
		t.Fatal("missing checkpoint prefix before compaction")
	}
	membership, err := n.core.CheckpointMembership(index)
	if err != nil {
		t.Fatalf("checkpoint membership before compaction: %v", err)
	}
	next, following, err := n.core.CheckpointLeaderOrders(index)
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{
		ConfigID: n.core.ConfigID(), Index: index, RootHash: sha256.Sum256([]byte("completion-compaction-root")),
		StateHash: sha256.Sum256([]byte("completion-compaction-state")), PrefixHash: prefix,
		NextLeaderOrder: next, FollowingLeaderOrder: following, Membership: &membership,
	}
	transport.mu.RLock()
	activeCores := make([]*quepaxa.Core, 0, len(transport.cores))
	for id, core := range transport.cores {
		if !transport.disabled[id] {
			activeCores = append(activeCores, core)
		}
	}
	transport.mu.RUnlock()
	for _, core := range activeCores {
		core.SetCheckpointValidator(func(context.Context, quepaxa.CheckpointSeal) error { return nil })
		if err := core.PrepareCheckpoint(ctx, seal); err != nil {
			t.Fatalf("prepare actual checkpoint seal on member %s: %v", core.NodeID(), err)
		}
	}
	encoded, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := n.core.Propose(ctx, encoded); err != nil {
		t.Fatalf("certify checkpoint decision: %v", err)
	}
	if err := n.core.CompactThrough(index, seal.RootHash); err != nil {
		t.Fatalf("compact exact certified floor: %v", err)
	}
	if n.core.CompactionFloor() < terminal {
		t.Fatalf("actual compaction floor=%d did not cover ADD terminal %d", n.core.CompactionFloor(), terminal)
	}
	if _, err := n.core.MembershipHistory(); err != nil {
		t.Fatalf("compaction discarded retained membership history: %v", err)
	}

	beforeTip := n.core.Tip()
	walBefore, err := transport.wals["a"].Read()
	if err != nil {
		t.Fatalf("read compacted WAL before completion request: %v", err)
	}
	compactedResponse := postCompletion()
	var compactedError network.ErrorResponse
	if err := json.Unmarshal(compactedResponse.Body.Bytes(), &compactedError); err != nil {
		t.Fatalf("decode compacted completion error status=%d body=%s: %v", compactedResponse.Code, compactedResponse.Body.String(), err)
	}
	if compactedResponse.Code != http.StatusInternalServerError || compactedError.Code != "internal_error" ||
		!strings.Contains(compactedError.Error, "complete exact membership ADD terminal: QuePaxa history compacted") || strings.Contains(compactedResponse.Body.String(), `"outcome":"completed"`) {
		t.Fatalf("compacted terminal did not fail at the real compacted-history gate status=%d error=%+v body=%s", compactedResponse.Code, compactedError, compactedResponse.Body.String())
	}
	if afterTip := n.core.Tip(); afterTip != beforeTip {
		t.Fatalf("compacted completion mutated consensus tip: before=%d after=%d", beforeTip, afterTip)
	}
	walAfter, err := transport.wals["a"].Read()
	if err != nil || !reflect.DeepEqual(walAfter, walBefore) {
		t.Fatalf("compacted completion mutated consensus WAL: beforeEntries=%d afterEntries=%d err=%v", len(walBefore), len(walAfter), err)
	}
	if len(authority.completed) != 0 {
		t.Fatalf("runtime completion after compaction reached authority completion: %+v", authority.completed)
	}
}

func nodeCompletionRequest(t *testing.T, n *Node, view fencing.MembershipRepairView, candidate fencing.MembershipRepairCandidate, add network.MembershipChange, executor fencing.Identity) network.MembershipRepairCompletionRequest {
	t.Helper()
	data, err := readVoterRegistration(context.Background(), n.bucket, n.membershipOperationKey(add))
	if err != nil || len(data) == 0 {
		t.Fatalf("read exact ADD journal: bytes=%d err=%v", len(data), err)
	}
	var operation membershipOperation
	if err := json.Unmarshal(data, &operation); err != nil {
		t.Fatal(err)
	}
	requestBytes, err := json.Marshal(operation.Request)
	if err != nil {
		t.Fatal(err)
	}
	requestHash := sha256.Sum256(requestBytes)
	history, err := n.core.MembershipHistory()
	if err != nil {
		t.Fatal(err)
	}
	var freeze, terminal quepaxa.DecidedValue
	for _, transition := range history.Transitions {
		if reflect.DeepEqual(n.core.ClusterForSlot(transition.Terminal.Slot+1), operation.Target) {
			freeze, terminal = transition.Freeze, transition.Terminal
		}
	}
	if terminal.Slot == 0 {
		t.Fatal("exact ADD transition missing from history")
	}
	abort := candidateAssignmentAbortSlot(view, candidate.Assignment.PodUID)
	return network.MembershipRepairCompletionRequest{
		Version: 1, Action: network.MembershipRepairCompletionAction, Challenge: stringsOf('a', 64), AttemptRevision: view.Progress.Revision,
		ScopeDigest: stringsOf('b', 64), RepairOperationID: view.Authorization.OperationID, RepairBindingDigest: fencing.BootstrapAssignmentDigest(candidate.Assignment),
		PhaseHash: phaseHashForCandidate(view, candidate.Assignment.PodUID), JournalOperationID: operation.Request.OperationID,
		JournalRequestHash: hex.EncodeToString(requestHash[:]), SourceClusterID: string(n.config.ClusterID), SourceConfigID: operation.Request.ExpectedConfigID,
		AbortSlot: &abort, FreezeSlot: uint64(freeze.Slot), FreezeHash: hex.EncodeToString(freeze.Hash[:]),
		TerminalSlot: uint64(terminal.Slot), TerminalHash: hex.EncodeToString(terminal.Hash[:]), Target: operation.Target,
		CandidatePodUID: candidate.Assignment.PodUID, CandidateMember: *candidate.Member, CandidateBindRef: fencing.BootstrapAssignmentDigest(candidate.Assignment),
		ExecutorNodeID: executor.NodeID, ExecutorPodUID: executor.PodUID, ExecutorRunID: executor.RunID, ExecutorWALIdentity: executor.WALIdentity,
	}
}

func candidateAssignmentAbortSlot(view fencing.MembershipRepairView, podUID string) quepaxa.Slot {
	for _, phase := range view.Progress.Phases {
		if phase.CandidatePodUID == podUID && phase.Kind == fencing.MembershipRepairAdd && phase.AbortSlot != nil {
			return *phase.AbortSlot
		}
	}
	return 0
}

func phaseHashForCandidate(view fencing.MembershipRepairView, podUID string) string {
	for _, phase := range view.Progress.Phases {
		if phase.CandidatePodUID == podUID && phase.Kind == fencing.MembershipRepairAdd {
			return phase.Hash
		}
	}
	return ""
}
