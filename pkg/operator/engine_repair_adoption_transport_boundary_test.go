package operator

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	rhizaapi "github.com/mrchypark/rhiza"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

// This test deliberately sends the inventory-free survivor proposal through
// the public Session and AuthorityClient transports. The only direct store call
// creates the pending request, as a physical preflight would; adoption itself
// must cross the authenticated FencingExecutor handler and load inventory from
// the canonical root.
func TestSessionAuthorityTransportAdoptsInventoryFreePendingRepair(t *testing.T) {
	e, api, id, runtime := newMutationStoreFixture(t)
	ctx := t.Context()
	scopeHash := preparePendingArchiveGenesis(t, e, api, id, runtime)
	if _, err := e.reserveMutationProducer(ctx, runtime); err != nil {
		t.Fatal(err)
	}
	id.WALIdentity = "wal"
	if _, err := e.bindWALAtScope(ctx, runtime.StatefulSetUID, id, scopeHash); err != nil {
		t.Fatal(err)
	}
	root, _, err := e.AuthorityState.ReadRoot(ctx, runtime.StatefulSetUID, id.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	incarnations, err := fencing.SortedRepairIncarnations(root.Ledger)
	if err != nil {
		t.Fatal(err)
	}
	oldMember := quepaxa.Member{ID: quepaxa.NodeID(id.NodeID), URL: "http://old:8080", PeerURL: "quic://old:9090", LogURL: "http://old:8081", WALIdentity: id.WALIdentity, PublicKey: quepaxa.PublicKey{1}}
	survivorMember := quepaxa.Member{ID: "survivor", URL: "http://survivor:8080", PeerURL: "quic://survivor:9090", LogURL: "http://survivor:8081", WALIdentity: "survivor-wal", PublicKey: quepaxa.PublicKey{2}}
	candidateMember := quepaxa.Member{ID: "candidate", URL: "http://candidate:8080", PeerURL: "quic://candidate:9090", LogURL: "http://candidate:8081", WALIdentity: "candidate-wal", PublicKey: quepaxa.PublicKey{3}}
	binding := fencing.EngineRemovalBinding{Version: fencing.EngineRemovalBindingVersion, Mode: fencing.EngineRemovalMode,
		OperationID: "repair-operation", RequestHash: strings.Repeat("a", 64), SourceClusterID: id.SourceClusterID,
		StatefulSetUID: runtime.StatefulSetUID, OldConfigID: 1, ExpectedAbortSlot: 3, OldMemberID: id.NodeID,
		OldWALIdentity: id.WALIdentity, TargetMemberID: string(candidateMember.ID)}
	store, err := NewEngineRepairStore(e.AuthorityState, runtime.StatefulSetUID)
	if err != nil {
		t.Fatal(err)
	}
	record := fencing.EngineRepairRecord{Version: fencing.EngineRepairRecordVersion, Binding: binding,
		SourceConfig:          quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{oldMember, survivorMember}},
		SourceAdminCredential: fencing.EngineRepairAdminCredential{Name: "source-auth", UID: "source-auth-uid", AdminTokenDigest: fencing.EngineRepairAdminTokenDigest([]byte("source-admin-token"))},
		Incarnations:          incarnations, CandidateIdentity: fencing.Identity{SourceClusterID: id.SourceClusterID, NodeID: "candidate", PodName: "candidate-0",
			PodUID: "candidate-pod", RunID: "candidate-run", WALIdentity: candidateMember.WALIdentity, AssignmentID: strings.Repeat("b", 64)},
		CandidateMember: candidateMember, CandidateCredential: fencing.EngineRepairCredential{Name: "candidate-auth", UID: "candidate-auth-uid",
			PeerTokenDigest: fencing.EngineRepairPeerTokenDigest([]byte("candidate-peer-token"))}, CandidateMemberDigest: fencing.EngineRepairMemberDigest(candidateMember),
		RemoveRequestHash: strings.Repeat("e", 64), Phase: fencing.RepairCandidatePrepared}
	// Add a second, fully enrolled surviving voter. The original record and
	// pending request continue to name the canonical old member from the source
	// fixture; no test-side override changes the incumbent tuple.
	survivorID := fencing.Identity{SourceClusterID: record.Binding.SourceClusterID, NodeID: "survivor", PodName: "survivor",
		PodUID: "survivor-pod", RunID: "survivor-run"}
	survivorRuntime := runtime
	survivorRuntime.NodeID, survivorRuntime.PodName, survivorRuntime.PodUID, survivorRuntime.RunID = survivorID.NodeID, survivorID.PodName, survivorID.PodUID, survivorID.RunID
	survivorRuntime.ContainerID = "survivor-container"
	if _, err := e.change(ctx, record.Binding.StatefulSetUID, record.Binding.SourceClusterID, func(ledger *fencing.Ledger) error {
		return ledger.RegisterEnrolled(survivorID, survivorRuntime)
	}); err != nil {
		t.Fatalf("enroll the additional source survivor: %v", err)
	}
	if _, err := e.reserveMutationProducer(ctx, survivorRuntime); err != nil {
		t.Fatalf("reserve survivor producer before WAL binding: %v", err)
	}
	stages, err := e.sourceStagesFor(ctx, survivorRuntime)
	if err != nil {
		t.Fatalf("load survivor's canonical source stage incarnation: %v", err)
	}
	root, _, err = e.AuthorityState.ReadRoot(ctx, record.Binding.StatefulSetUID, record.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	survivorID.WALIdentity = "survivor-wal"
	if _, err := e.bindWALAtScopeWithStage(ctx, record.Binding.StatefulSetUID, survivorID, root.Ledger.ArchiveScopeHash, stages.incarnation); err != nil {
		t.Fatalf("bind survivor WAL before adoption: %v", err)
	}
	root, _, err = e.AuthorityState.ReadRoot(ctx, record.Binding.StatefulSetUID, record.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	incarnations, err = fencing.SortedRepairIncarnations(root.Ledger)
	if err != nil {
		t.Fatal(err)
	}
	proposal := pendingEngineRepairProposalForRecord(record)
	proposal.IncumbentReferenceDigest = strings.Repeat("d", 64)
	pending, err := store.CreateOrReadPendingEngineRepair(ctx, proposal)
	if err != nil {
		t.Fatalf("create the preflight request through the existing authority store: %v", err)
	}
	record.Binding.OperationID, record.Binding.RequestHash = pending.OperationID, pending.RequestDigest
	record.CandidateIdentity = pending.CandidateIdentity
	record.BootstrapRequestDigest = pending.RequestDigest
	record.Incarnations = nil
	if err := record.ValidateAdoptionProposal(); err != nil {
		t.Fatalf("construct inventory-free adoption proposal: %v", err)
	}

	activeID := survivorID
	installAdoptionAuthPod(t, api, activeID, record.SourceConfig.Members)
	activePhysical := activeID
	activePhysical.WALIdentity = ""
	genesisMembers, err := json.Marshal(record.SourceConfig.Members)
	if err != nil {
		t.Fatal(err)
	}
	actualAuthority := e.Handler()
	var receivedInventory bool
	transport := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/supervisor" || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		var message fencing.AuthorityMessage
		if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
			http.Error(w, "invalid test transport request", http.StatusBadRequest)
			return
		}
		switch message.Action {
		case "register":
			_ = json.NewEncoder(w).Encode(fencing.StartupGrant{Identity: activePhysical, Role: fencing.StartupRoleVoterActive,
				Environment: map[string]string{"RHIZA_CLUSTER_MEMBERS": string(genesisMembers), "RHIZA_PEER_TOKEN": "fixed-peer-token"}})
		case "bind":
			_ = json.NewEncoder(w).Encode(fencing.BindReceipt{Identity: message.Identity, Role: fencing.StartupRoleVoterActive})
		default:
			if message.EngineRepair == nil || message.EngineRepair.Record == nil {
				http.Error(w, "missing adoption record", http.StatusBadRequest)
				return
			}
			receivedInventory = len(message.EngineRepair.Record.Incarnations) != 0
			r.Body = http.NoBody
			encoded, _ := json.Marshal(message)
			forward := r.Clone(r.Context())
			forward.Body = io.NopCloser(bytes.NewReader(encoded))
			actualAuthority.ServeHTTP(w, forward)
		}
	}))
	t.Cleanup(transport.Close)
	authorityToken := e.Kube.TokenFile
	authority := &fencing.AuthorityClient{URL: transport.URL + "/v1/supervisor", TokenFile: authorityToken, Client: transport.Client()}

	// A short Unix socket and the real Session client exercise the child-side
	// supervisor protocol. The local forwarding shim delegates to the
	// AuthorityClient; the authority endpoint above is the real authenticated
	// executor handler for adoption.
	socketDir, err := os.MkdirTemp("", "rhiza-fence-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	socketPath := filepath.Join(socketDir, "s.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	local := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer local-ticket" {
			http.Error(w, "unauthorized", http.StatusForbidden)
			return
		}
		switch r.URL.Path {
		case "/v1/startup":
			grant, err := authority.RegisterStartup(r.Context(), activePhysical)
			if err != nil {
				http.Error(w, "startup failed", http.StatusConflict)
				return
			}
			_ = json.NewEncoder(w).Encode(grant)
		case "/v1/bind":
			var id fencing.Identity
			if json.NewDecoder(r.Body).Decode(&id) != nil {
				http.Error(w, "invalid bind", http.StatusBadRequest)
				return
			}
			receipt, err := authority.Bind(r.Context(), id)
			if err != nil {
				http.Error(w, "bind failed", http.StatusConflict)
				return
			}
			_ = json.NewEncoder(w).Encode(receipt)
		case "/v1/engine-repair/adopt":
			var request fencing.EngineRepairRequest
			if json.NewDecoder(r.Body).Decode(&request) != nil || request.Record == nil {
				http.Error(w, "invalid adoption", http.StatusBadRequest)
				return
			}
			adopted, err := authority.AdoptPendingEngineRepair(r.Context(), request.Identity, request.PendingID, request.PendingDigest, *request.Record)
			if err != nil {
				http.Error(w, "adoption failed", http.StatusConflict)
				return
			}
			_ = json.NewEncoder(w).Encode(fencing.EngineRepairResponse{Found: true, Record: &adopted})
		default:
			http.NotFound(w, r)
		}
	})}
	t.Cleanup(func() { _ = local.Close(); _ = listener.Close() })
	go func() { _ = local.Serve(listener) }()
	t.Setenv("RHIZA_POD_NAME", activeID.PodName)
	session, err := fencing.Connect(ctx, socketPath, "local-ticket", activeID.SourceClusterID, activeID.NodeID)
	if err != nil {
		t.Fatalf("connect child Session through supervisor socket: %v", err)
	}
	// The Session received the validated membership override. Feed that exact
	// grant environment through the public configuration parser, replacing a
	// stale parent value, then compare the resulting genesis fingerprint.
	t.Setenv("RHIZA_CLUSTER_ID", activeID.SourceClusterID)
	t.Setenv("RHIZA_NODE_ID", activeID.NodeID)
	t.Setenv("RHIZA_DATA_DIR", t.TempDir())
	t.Setenv("RHIZA_ENABLE_RECONFIGURATION", "true")
	t.Setenv("RHIZA_FENCE_SOCKET", socketPath)
	t.Setenv("RHIZA_CLUSTER_MEMBERS", "stale-parent-membership")
	t.Setenv("RHIZA_PEER_TOKEN", "stale-parent-token")
	for key, value := range session.StartupEnvironment() {
		t.Setenv(key, value)
	}
	config, err := rhizaapi.ConfigFromEnv()
	if err != nil {
		t.Fatalf("parse the actual Session grant through ConfigFromEnv: %v", err)
	}
	if !reflect.DeepEqual(config.Members, record.SourceConfig.Members) || config.PeerToken != "fixed-peer-token" || !config.EngineOwnedRemovals {
		t.Fatalf("ConfigFromEnv lost supervisor-pinned genesis config: members=%+v peerToken=%q engineOwned=%t", config.Members, config.PeerToken, config.EngineOwnedRemovals)
	}
	wantFingerprint := recovery.NewMembershipRecord(activeID.SourceClusterID, record.SourceConfig.Members, "async").Membership
	if got := recovery.NewMembershipRecord(config.ClusterID, config.Members, "async").Membership; got != wantFingerprint {
		t.Fatalf("startup grant changed immutable genesis membership fingerprint: got=%s want=%s", got, wantFingerprint)
	}
	if err := session.BindWAL(ctx, activeID.WALIdentity); err != nil {
		t.Fatalf("bind the exact survivor WAL through the Session: %v", err)
	}
	adopted, err := session.AdoptPendingEngineRepair(ctx, pending.PendingID, pending.RequestDigest, record)
	if err != nil {
		t.Fatalf("adopt inventory-free pending proposal over authenticated authority transport: %v", err)
	}
	if receivedInventory || len(adopted.Incarnations) != len(incarnations) {
		t.Fatalf("adoption did not source inventory solely from the canonical root: callerInventory=%t adopted=%d canonical=%d", receivedInventory, len(adopted.Incarnations), len(incarnations))
	}
	root, _, err = e.AuthorityState.ReadRoot(ctx, record.Binding.StatefulSetUID, record.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if len(root.Ledger.PendingEngineRepairs) != 0 {
		t.Fatal("successful transport adoption retained a pending request")
	}
	if _, registered := root.Ledger.Runs[adopted.CandidateIdentity.RunID]; registered {
		t.Fatal("adoption registered the candidate before trusted runtime enrollment")
	}
}

func installAdoptionAuthPod(t *testing.T, fixture *mutationStoreFixture, identity fencing.Identity, members []quepaxa.Member) {
	t.Helper()
	encodedMembers, err := json.Marshal(members)
	if err != nil {
		t.Fatal(err)
	}
	env := []any{
		object{"name": "RHIZA_CLUSTER_ID", "value": identity.SourceClusterID},
		object{"name": "RHIZA_NODE_ID", "value": identity.NodeID},
		object{"name": "RHIZA_CLUSTER_MEMBERS", "value": string(encodedMembers)},
	}
	pod := supervisedTestPod()
	metadata := asObject(pod["metadata"])
	physicalName := identity.PodName
	if physicalName == "" {
		physicalName = identity.NodeID
	}
	metadata["name"], metadata["namespace"], metadata["uid"] = physicalName, "test", identity.PodUID
	metadata["annotations"] = object{fencingProtocolAnnotation: "1", fencingRootAnnotation: "binding-uid"}
	metadata["ownerReferences"] = []any{object{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "rhiza", "uid": "sts", "controller": true}}
	spec := asObject(pod["spec"])
	spec["nodeName"], spec["serviceAccountName"] = "node", "default"
	container := asObject(list(spec["containers"])[0])
	container["name"], container["env"] = "rhiza", env
	sts := object{"metadata": object{"name": "rhiza", "namespace": "test", "uid": "sts"},
		"spec": object{"template": object{"spec": object{"containers": []any{object{"name": "rhiza", "env": env}}}}}}
	fixture.mu.Lock()
	fixture.authPod, fixture.authStatefulSet = pod, sts
	fixture.authPodName, fixture.authPodUID = physicalName, identity.PodUID
	fixture.mu.Unlock()
}

func TestPendingRepairOrdinalReceiptRejectsStaleAndSecondSlotProposal(t *testing.T) {
	_, store, record := engineRepairStoreFixture(t)
	proposal := pendingEngineRepairProposalForRecord(record)
	proposal.IncumbentReferenceDigest = strings.Repeat("d", 64)
	first, err := store.CreateOrReadPendingEngineRepair(t.Context(), proposal)
	if err != nil {
		t.Fatalf("create first ordinal pending request: %v", err)
	}
	retry, err := store.CreateOrReadPendingEngineRepair(t.Context(), proposal)
	if err != nil || retry != first {
		t.Fatalf("same physical proposal did not preserve its receipt: first=%+v retry=%+v err=%v", first, retry, err)
	}

	stale := proposal
	stale.BootstrapIdentity.RunID = "replacement-run-stale"
	stale.CandidateIdentity.RunID = stale.BootstrapIdentity.RunID
	if _, err := store.CreateOrReadPendingEngineRepair(t.Context(), stale); err == nil {
		t.Fatal("same-slot stale proposal with a changed physical RunID replaced the original receipt")
	}

	secondOrdinal := proposal
	secondOrdinal.StableOrdinal = 1
	secondOrdinal.BootstrapIdentity.PodName = "replacement-1"
	secondOrdinal.BootstrapIdentity.PodUID = "replacement-pod-1"
	secondOrdinal.BootstrapIdentity.RunID = "replacement-run-1"
	secondOrdinal.CandidateIdentity.PodName = secondOrdinal.BootstrapIdentity.PodName
	secondOrdinal.CandidateIdentity.PodUID = secondOrdinal.BootstrapIdentity.PodUID
	secondOrdinal.CandidateIdentity.RunID = secondOrdinal.BootstrapIdentity.RunID
	if _, err := store.CreateOrReadPendingEngineRepair(t.Context(), secondOrdinal); err == nil {
		t.Fatal("second ordinal proposal consumed a concurrent slot for the same source repair")
	}

	loaded, err := store.ListPendingEngineRepairs(t.Context(), record.Binding.SourceClusterID)
	if err != nil || len(loaded) != 1 || loaded[0] != first {
		t.Fatalf("stale or second-slot proposal changed the first receipt: loaded=%+v err=%v", loaded, err)
	}
}
