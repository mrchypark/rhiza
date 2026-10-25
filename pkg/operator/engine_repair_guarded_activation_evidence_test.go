package operator

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// This exercises the production confirmEngineRepairActivation method and its
// storage/archive plus HTTP evidence reader. The fixture's REMOVE proof is
// deliberately unit-level; it is not evidence of a real three-voter quorum.
func TestEngineRepairActivationUsesProductionCandidateEvidence(t *testing.T) {
	for _, tc := range []struct {
		name           string
		membershipEdit func(*network.MembershipStatus)
		recoveryShort  bool
		wantErr        bool
		wantError      string
		wantRecovery   bool
	}{
		{name: "exact archive membership and recovered quorum", wantRecovery: true},
		{name: "reject wrong membership config before recovery read", membershipEdit: func(s *network.MembershipStatus) { s.ConfigID-- }, wantErr: true,
			wantError: "candidate membership status differs from the canonical ADD target"},
		{name: "reject recovery tip short of frozen prefix", recoveryShort: true, wantErr: true, wantRecovery: true,
			wantError: "candidate has not applied and archived the durable engine repair ADD prefix"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, _, record, caller := engineRepairActivationEvidenceFixture(t)
			request, target, err := engineRepairCanonicalAdd(record)
			if err != nil {
				t.Fatal(err)
			}
			operation := engineRepairMembershipOperation{Request: request, Target: target}
			encoded, err := json.Marshal(operation)
			if err != nil {
				t.Fatal(err)
			}
			key := path.Join("root", record.Binding.SourceClusterID, "membership", fmt.Sprintf("%d.json", request.ExpectedConfigID))
			if err := e.ArchiveBucket.Upload(t.Context(), key, bytes.NewReader(encoded)); err != nil {
				t.Fatalf("publish exact immutable ADD object into the fixture archive: %v", err)
			}

			membership := network.MembershipStatus{
				NodeID: record.CandidateMember.ID, ClusterID: record.Binding.SourceClusterID, ConfigID: target.ConfigID,
				Voters: repairMemberIDs(target.Members), WALIdentity: record.CurrentCandidateIdentity().WALIdentity, Voting: true,
			}
			recovery := network.VoterRecoveryStatus{
				NodeID: string(record.CandidateMember.ID), ClusterID: record.Binding.SourceClusterID, Quorum: true,
				CertifiedTip: record.Binding.FreezeSlot + 33, AppliedTip: record.Binding.FreezeSlot + 33,
				ArchiveTip: record.Binding.FreezeSlot + 33,
			}
			server, seen := engineRepairActivationEvidenceServer(t, membership, recovery, "source-admin-token")
			defer server.Close()
			withEngineRepairActivationHTTP(t, server.URL)

			// First obtain the exact promotion hash through the production
			// archive, Secret, membership, and recovery readers. Only after that
			// prerequisite is met do negative cases change the readback state.
			candidateIdentity := record.CurrentCandidateIdentity()
			candidateRuntime := record.CurrentCandidateRuntime()
			if candidateRuntime == nil {
				t.Fatal("canonical candidate runtime is missing")
			}
			promotion, err := e.engineRepairCandidateActivationEvidence(t.Context(), *candidateRuntime, record)
			if err != nil {
				t.Fatalf("production candidate ADD evidence failed before promotion: %v", err)
			}
			if _, err := e.change(t.Context(), record.Binding.StatefulSetUID, record.Binding.SourceClusterID, func(ledger *fencing.Ledger) error {
				return ledger.PromoteLearner(candidateIdentity, promotion.operationID, promotion.digest)
			}); err != nil {
				t.Fatalf("record production-verified ADD promotion: %v", err)
			}
			enrollActiveEngineRepairMutationProducer(t, e, candidateIdentity, *candidateRuntime)
			if tc.recoveryShort {
				recovery.CertifiedTip--
				recovery.AppliedTip--
				recovery.ArchiveTip--
			}
			if tc.membershipEdit != nil {
				tc.membershipEdit(&membership)
			}
			seen.setStatus(membership, recovery)
			seen.reset()

			before, _, err := e.AuthorityState.ReadRoot(t.Context(), record.Binding.StatefulSetUID, record.Binding.SourceClusterID)
			if err != nil {
				t.Fatal(err)
			}
			confirmed, confirmErr := e.confirmEngineRepairActivation(t.Context(), record.Binding.StatefulSetUID, caller, record.Binding.OperationID)
			if tc.wantErr {
				if confirmErr == nil {
					t.Fatal("production activation verifier accepted mismatched candidate evidence")
				}
				if !strings.Contains(confirmErr.Error(), tc.wantError) {
					t.Fatalf("production activation verifier rejected for the wrong reason: got=%v want substring=%q", confirmErr, tc.wantError)
				}
			} else if confirmErr != nil || confirmed.Phase != fencing.RepairActive {
				t.Fatalf("production activation verifier rejected exact evidence: phase=%s err=%v", confirmed.Phase, confirmErr)
			}

			seen.mu.Lock()
			paths := append([]string(nil), seen.paths...)
			auth := append([]string(nil), seen.auth...)
			seen.mu.Unlock()
			wantPaths := []string{"/membership/status"}
			if tc.wantRecovery {
				wantPaths = append(wantPaths, "/recovery/status")
			}
			if !reflect.DeepEqual(paths, wantPaths) {
				t.Fatalf("production verifier made unexpected readback calls: got=%v want=%v", paths, wantPaths)
			}
			wantAuth := []string{"Bearer source-admin-token"}
			if tc.wantRecovery {
				wantAuth = append(wantAuth, "")
			}
			if !reflect.DeepEqual(auth, wantAuth) {
				t.Fatalf("production verifier did not use only the pinned admin credential for membership: auth=%v", auth)
			}
			after, _, err := e.AuthorityState.ReadRoot(t.Context(), record.Binding.StatefulSetUID, record.Binding.SourceClusterID)
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantErr && hashJSON(before.Ledger) != hashJSON(after.Ledger) {
				t.Fatal("rejected candidate evidence changed canonical authority state")
			}
			if !tc.wantErr {
				run := after.Ledger.Runs[confirmed.CurrentCandidateIdentity().RunID]
				if run.Promotion == nil || run.Promotion.OperationID != promotion.operationID || run.Promotion.EvidenceHash != promotion.digest ||
					run.MutationProducerUID == "" || after.Ledger.EngineRepairs[mustEngineRepairGenerationKey(t, confirmed)] != engineRepairRootReference(confirmed) {
					t.Fatal("successful confirmation did not preserve the verified promotion and canonical active repair reference")
				}
			}
		})
	}
}

func engineRepairActivationEvidenceFixture(t *testing.T) (*FencingExecutor, *mutationStoreFixture, fencing.EngineRepairRecord, fencing.Identity) {
	t.Helper()
	e, fixture, sourceIdentity, sourceRuntime := newMutationStoreFixture(t)
	scopeHash := preparePendingArchiveGenesis(t, e, fixture, sourceIdentity, sourceRuntime)
	if _, err := e.reserveMutationProducer(t.Context(), sourceRuntime); err != nil {
		t.Fatalf("reserve old voter producer: %v", err)
	}
	sourceIdentity.WALIdentity = "old-wal"
	if _, err := e.bindWALAtScope(t.Context(), sourceRuntime.StatefulSetUID, sourceIdentity, scopeHash); err != nil {
		t.Fatalf("bind old voter WAL: %v", err)
	}

	// Enroll an original surviving voter before the repair reservation so it is
	// part of the immutable initial incarnation inventory.
	caller := fencing.Identity{SourceClusterID: sourceIdentity.SourceClusterID, NodeID: "survivor", PodName: "survivor-0",
		PodUID: "survivor-pod", RunID: "survivor-run", AssignmentID: hashBytes([]byte("survivor-assignment"))}
	callerRuntime := sourceRuntime
	callerRuntime.NodeID, callerRuntime.PodName, callerRuntime.PodUID = caller.NodeID, caller.PodName, caller.PodUID
	callerRuntime.RunID, callerRuntime.AssignmentID, callerRuntime.ContainerID = caller.RunID, caller.AssignmentID, "survivor-container"
	configureMutationWorkloadFixture(t, e, fixture, caller, callerRuntime)
	if _, err := e.change(t.Context(), sourceRuntime.StatefulSetUID, sourceRuntime.SourceClusterID, func(ledger *fencing.Ledger) error {
		return ledger.RegisterEnrolled(caller, callerRuntime)
	}); err != nil {
		t.Fatalf("register surviving voter before repair reservation: %v", err)
	}
	if _, err := e.reserveMutationProducer(t.Context(), callerRuntime); err != nil {
		t.Fatalf("reserve surviving voter producer: %v", err)
	}
	caller.WALIdentity = "survivor-wal"
	callerStages, err := e.sourceStagesFor(t.Context(), callerRuntime)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.bindWALAtScopeWithStage(t.Context(), sourceRuntime.StatefulSetUID, caller, scopeHash, callerStages.incarnation); err != nil {
		t.Fatalf("bind and activate surviving voter through startup path: %v", err)
	}

	root, _, err := e.AuthorityState.ReadRoot(t.Context(), sourceRuntime.StatefulSetUID, sourceRuntime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	incarnations, err := fencing.SortedRepairIncarnations(root.Ledger)
	if err != nil {
		t.Fatal(err)
	}
	oldMember := quepaxa.Member{ID: quepaxa.NodeID(sourceIdentity.NodeID), URL: "http://old:8080", PeerURL: "quic://old:9090",
		LogURL: "http://old:8081", WALIdentity: sourceIdentity.WALIdentity, PublicKey: quepaxa.PublicKey{1}}
	survivor := quepaxa.Member{ID: quepaxa.NodeID(caller.NodeID), URL: "http://survivor:8080", PeerURL: "quic://survivor:9090",
		LogURL: "http://survivor:8081", WALIdentity: caller.WALIdentity, PublicKey: quepaxa.PublicKey{2}}
	candidateToken := "candidate-peer-token"
	candidateIdentity := fencing.Identity{SourceClusterID: sourceIdentity.SourceClusterID, NodeID: "candidate", PodName: "candidate-0",
		PodUID: "candidate-pod", RunID: "candidate-run", WALIdentity: strings.Repeat("d", 64), AssignmentID: strings.Repeat("b", 64)}
	candidateMember := quepaxa.Member{ID: quepaxa.NodeID(candidateIdentity.NodeID), URL: "http://candidate:8080", PeerURL: "quic://candidate:9090",
		LogURL: "http://candidate:8081", WALIdentity: candidateIdentity.WALIdentity,
		PublicKey: quepaxa.PublicKey(network.PeerPublicKey(types.ClusterID(candidateIdentity.SourceClusterID), quepaxa.NodeID(candidateIdentity.NodeID), candidateToken))}
	binding := fencing.EngineRemovalBinding{Version: fencing.EngineRemovalBindingVersion, Mode: fencing.EngineRemovalMode,
		OperationID: "activation-evidence-operation", RequestHash: strings.Repeat("a", 64), SourceClusterID: sourceIdentity.SourceClusterID,
		StatefulSetUID: sourceRuntime.StatefulSetUID, OldConfigID: 1, ExpectedAbortSlot: 3, OldMemberID: sourceIdentity.NodeID,
		OldWALIdentity: sourceIdentity.WALIdentity, TargetMemberID: candidateIdentity.NodeID}
	record := fencing.EngineRepairRecord{Version: fencing.EngineRepairRecordVersion, Binding: binding,
		SourceConfig:          quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{oldMember, survivor}},
		SourceAdminCredential: fencing.EngineRepairAdminCredential{Name: "source-auth", UID: "source-auth-uid", AdminTokenDigest: fencing.EngineRepairAdminTokenDigest([]byte("source-admin-token"))},
		Incarnations:          incarnations, CandidateIdentity: candidateIdentity, CandidateMember: candidateMember,
		CandidateCredential:   fencing.EngineRepairCredential{Name: "candidate-auth", UID: "candidate-auth-uid", PeerTokenDigest: fencing.EngineRepairPeerTokenDigest([]byte(candidateToken))},
		CandidateMemberDigest: fencing.EngineRepairMemberDigest(candidateMember), RemoveRequestHash: strings.Repeat("e", 64), Phase: fencing.RepairCandidatePrepared}
	store, err := NewEngineRepairStore(e.AuthorityState, binding.StatefulSetUID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateOrReadEngineRepair(t.Context(), record); err != nil {
		t.Fatalf("reserve exact candidate activation fixture: %v", err)
	}
	candidateRuntime := engineRepairStoreCandidateRuntime(record)
	candidateRuntime.NamespaceUID, candidateRuntime.BindingUID = "namespace-uid", "binding-uid"
	candidateRuntime.NodeName, candidateRuntime.NodeUID = "node", "node-uid"
	candidateRuntime.MachineID, candidateRuntime.BootID, candidateRuntime.RuntimeID = "machine-id", "boot-id", "containerd"
	candidateRuntime.SandboxID, candidateRuntime.ContainerID, candidateRuntime.ImageID = "candidate-sandbox", "containerd://candidate-container", "rhiza@sha256:"+strings.Repeat("c", 64)
	record, err = store.PinCandidateRuntime(t.Context(), binding.SourceClusterID, binding.OperationID, record.Digest(), candidateRuntime)
	if err != nil {
		t.Fatalf("pin candidate runtime: %v", err)
	}
	record = freezeEngineRepairRecord(record)
	record.StorageProofDigest, record.RemoveTerminalDigest = hashBytes([]byte("fixture storage proof")), hashBytes([]byte("fixture REMOVE terminal"))
	add := network.MembershipChange{OperationID: engineRepairAddOperationID(binding.OperationID), ClusterID: binding.SourceClusterID,
		ExpectedConfigID: record.RemovalOperation.Target.ConfigID, ExpectedAbortSlot: 0, Add: &record.CandidateMember}
	record.AddRequestHash, record.Phase = hashJSON(add), fencing.RepairAdding
	root, version, err := e.AuthorityState.ReadRoot(t.Context(), binding.StatefulSetUID, binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if err := root.Ledger.Bind(record.CandidateIdentity); err != nil {
		t.Fatalf("bind candidate WAL: %v", err)
	}
	root.Ledger.EngineRepairs[mustEngineRepairGenerationKey(t, record)] = engineRepairRootReference(record)
	if err := record.ValidateAgainstLedger(root.Ledger); err != nil {
		t.Fatalf("validate fixture ledger against active repair: %v", err)
	}
	if err := store.persistImmutableRecord(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	if err := e.AuthorityState.WriteRoot(t.Context(), binding.StatefulSetUID, binding.SourceClusterID, root, version); err != nil {
		t.Fatal(err)
	}
	_, candidateIdentity, candidateRuntime, err = store.ReadEngineRepairCurrentCandidate(t.Context(), binding.SourceClusterID, binding.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	installActiveRestartPod(t, fixture, candidateIdentity, record)

	// Pin the original administrator token at the exact Secret reference already
	// held by the repair record; the production verifier reads it through Kube.
	fixture.mu.Lock()
	fixture.objects["/api/v1/namespaces/test/secrets/"+record.SourceAdminCredential.Name] = object{
		"type":     "Opaque",
		"metadata": object{"name": record.SourceAdminCredential.Name, "namespace": "test", "uid": record.SourceAdminCredential.UID},
		"data":     object{"RHIZA_ADMIN_TOKEN": base64.StdEncoding.EncodeToString([]byte("source-admin-token"))},
	}
	fixture.mu.Unlock()
	return e, fixture, record, caller
}

func repairMemberIDs(members []quepaxa.Member) []quepaxa.NodeID {
	ids := make([]quepaxa.NodeID, len(members))
	for i := range members {
		ids[i] = members[i].ID
	}
	return ids
}

type engineRepairActivationHTTPLog struct {
	mu         sync.Mutex
	paths      []string
	auth       []string
	membership network.MembershipStatus
	recovery   network.VoterRecoveryStatus
}

func engineRepairActivationEvidenceServer(t *testing.T, membership network.MembershipStatus, recovery network.VoterRecoveryStatus, token string) (*httptest.Server, *engineRepairActivationHTTPLog) {
	t.Helper()
	seen := &engineRepairActivationHTTPLog{membership: membership, recovery: recovery}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.mu.Lock()
		seen.paths = append(seen.paths, r.URL.Path)
		seen.auth = append(seen.auth, r.Header.Get("Authorization"))
		seen.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/membership/status":
			if r.Header.Get("Authorization") != "Bearer "+token {
				http.Error(w, "unauthorized", http.StatusForbidden)
				return
			}
			seen.mu.Lock()
			status := seen.membership
			seen.mu.Unlock()
			_ = json.NewEncoder(w).Encode(status)
		case "/recovery/status":
			seen.mu.Lock()
			status := seen.recovery
			seen.mu.Unlock()
			_ = json.NewEncoder(w).Encode(status)
		default:
			http.NotFound(w, r)
		}
	}))
	return server, seen
}

func (seen *engineRepairActivationHTTPLog) setStatus(membership network.MembershipStatus, recovery network.VoterRecoveryStatus) {
	seen.mu.Lock()
	defer seen.mu.Unlock()
	seen.membership, seen.recovery = membership, recovery
}

func (seen *engineRepairActivationHTTPLog) reset() {
	seen.mu.Lock()
	defer seen.mu.Unlock()
	seen.paths, seen.auth = nil, nil
}

func withEngineRepairActivationHTTP(t *testing.T, target string) {
	t.Helper()
	base, err := url.Parse(target)
	if err != nil {
		t.Fatal(err)
	}
	previous := http.DefaultClient
	http.DefaultClient = &http.Client{Transport: engineRepairActivationRoundTripper{target: base, next: http.DefaultTransport}}
	t.Cleanup(func() { http.DefaultClient = previous })
}

type engineRepairActivationRoundTripper struct {
	target *url.URL
	next   http.RoundTripper
}

func (r engineRepairActivationRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	copy := request.Clone(request.Context())
	u := *request.URL
	u.Scheme, u.Host = r.target.Scheme, r.target.Host
	copy.URL = &u
	copy.Host = r.target.Host
	return r.next.RoundTrip(copy)
}
