package operator

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

func TestMembershipRepairSupervisorRouteReturnsReadOnlyNoActiveResult(t *testing.T) {
	root := peerClosureRoot()
	rootConfig := rootResource(root)
	bucket := objstore.NewInMemBucket()
	seedPeerClosureAutoBinding(t, bucket, "root", root, "workload-uid", "workload-uid", autoBinding(&rootConfig))
	caller := fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "caller-uid", RunID: "caller-run", WALIdentity: "wal-0"}
	ledger := fenceLedger{StatefulSetUID: "workload-uid", Ledger: fencing.Ledger{SourceClusterID: "source", Runs: map[string]fencing.Run{
		caller.RunID: {Identity: caller},
	}}}
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/apis/authentication.k8s.io/v1/tokenreviews":
			_ = json.NewEncoder(w).Encode(object{"status": object{"authenticated": true, "audiences": []any{"rhiza-fencer"}, "user": object{
				"username": "system:serviceaccount:test:default",
				"extra":    object{"authentication.kubernetes.io/pod-uid": []any{"caller-uid"}, "authentication.kubernetes.io/pod-name": []any{"rhiza-0"}},
			}}})
		case "/api/v1/namespaces/test/pods/rhiza-0":
			_ = json.NewEncoder(w).Encode(peerClosureCallerPod("8080"))
		case "/apis/apps/v1/namespaces/test/statefulsets/rhiza":
			_ = json.NewEncoder(w).Encode(peerClosureStatefulSet("8080"))
		case "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizaclusters":
			_ = json.NewEncoder(w).Encode(object{"items": []any{root}})
		case (&FencingExecutor{Kube: &Kubernetes{Namespace: "test"}}).ledgerPath("workload-uid", "source"):
			_ = json.NewEncoder(w).Encode(object{"metadata": object{"name": "ledger", "namespace": "test", "uid": "ledger-uid"}, "data": object{"ledger.json": string(jsonBytes(ledger))}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer api.Close()
	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte("test-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	executor := &FencingExecutor{Kube: &Kubernetes{BaseURL: api.URL, Namespace: "test", TokenFile: tokenPath, Client: api.Client()}, Bucket: bucket, Prefix: "root"}
	authority := httptest.NewTLSServer(executor.Handler())
	defer authority.Close()
	client := &fencing.AuthorityClient{URL: authority.URL + "/v1/supervisor", TokenFile: tokenPath, Client: authority.Client()}
	result, err := client.ReadMembershipRepair(context.Background(), caller)
	if err != nil || result.Repair != nil {
		t.Fatalf("authoritative empty repair status=%+v err=%v", result, err)
	}
	if _, err := client.QualifyMembershipRepair(context.Background(), caller, fencing.MembershipRepairQualificationRequest{OperationID: "missing-repair", CandidatePodUID: "candidate-uid"}); err == nil {
		t.Fatal("qualified a candidate without a retained repair")
	}
	if _, err := client.PrepareMembershipRepairPhase(context.Background(), caller, fencing.MembershipRepairPhasePrepareRequest{OperationID: "missing-repair", Kind: fencing.MembershipRepairRemove}); err == nil {
		t.Fatal("prepared REMOVE without a retained repair")
	}
	phaseRequest := fencing.MembershipRepairPhaseObserveRequest{OperationID: "missing-repair", PhaseHash: "phase-hash", JournalOperationID: "journal-operation", JournalRequestHash: "journal-request-hash", Target: quepaxa.Cluster{ConfigID: 2, Members: []quepaxa.Member{voterMember("source", "rhiza-0", "token-0")}}, TerminalSlot: 17}
	if _, err := client.ObserveMembershipRepairPhase(context.Background(), caller, phaseRequest); err == nil {
		t.Fatal("observed a phase without a retained repair")
	}
	if _, err := client.CompleteMembershipRepairPhase(context.Background(), caller, phaseRequest); err == nil {
		t.Fatal("completed a phase without a retained repair")
	}
}

func TestBootstrapStatusReturnsUnassignedOnlyForCleanManagedInitialPod(t *testing.T) {
	ctx := context.Background()
	const podUID = "initial-candidate-uid"
	const podName = "rhiza-2"
	root := peerClosureRoot()
	rootResourceValue := rootResource(root)
	rootResourceValue.Spec.AdoptClusterID = "source"
	rootResourceValue.Spec.Container = "rhiza"
	rootResourceValue.Spec.VoterCount = 3
	rootResourceValue.Metadata["generation"] = int64(1)
	rootResourceValue.Status.ObservedGeneration = 1
	normalizeClusterSpec(&rootResourceValue)
	root = objectFromCluster(rootResourceValue)
	sts := peerClosureStatefulSet("8080")
	stsMetadata := asObject(sts["metadata"])
	stsMetadata["name"], stsMetadata["uid"], stsMetadata["generation"] = "rhiza", "workload-uid", int64(1)
	stsSpec := asObject(sts["spec"])
	stsSpec["serviceName"] = "rhiza-headless"
	stsSpec["podManagementPolicy"] = "Parallel"
	main := asObject(list(nested(sts, "spec", "template", "spec", "containers"))[0])
	main["env"] = append(list(main["env"]),
		object{"name": "RHIZA_OBJSTORE_PROVIDER", "value": "s3"},
		object{"name": "RHIZA_OBJSTORE_BUCKET", "value": "test-bucket"},
		object{"name": "RHIZA_OBJSTORE_PREFIX", "value": "root"},
	)
	rootUID := str(nested(root, "metadata", "uid"))
	operationID := bootstrapOperationID(rootUID, "workload-uid", expectedBootstrapCandidate(&rootResourceValue, sts))
	pod := prepareProfilePod()
	podMeta := asObject(pod["metadata"])
	podMeta["name"], podMeta["uid"] = podName, podUID
	podMeta["annotations"] = object{fencingProtocolAnnotation: "1", fencingRootAnnotation: rootUID, fencingPrepareOperationAnnotation: operationID}
	podMeta["ownerReferences"] = []any{object{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "rhiza", "uid": "workload-uid", "controller": true}}
	podSpec := asObject(pod["spec"])
	podSpec["serviceAccountName"] = "default"
	for _, raw := range append(list(podSpec["containers"]), list(podSpec["initContainers"])...) {
		hasBucket, hasPrefix := false, false
		for _, envRaw := range list(asObject(raw)["env"]) {
			env := asObject(envRaw)
			if env["name"] == "RHIZA_CLUSTER_ID" {
				env["value"] = "source"
			}
			if env["name"] == "RHIZA_OBJSTORE_BUCKET" {
				env["value"], hasBucket = "test-bucket", true
			}
			if env["name"] == "RHIZA_OBJSTORE_PREFIX" {
				env["value"], hasPrefix = "root", true
			}
		}
		if !hasBucket {
			asObject(raw)["env"] = append(list(asObject(raw)["env"]), object{"name": "RHIZA_OBJSTORE_BUCKET", "value": "test-bucket"})
		}
		if !hasPrefix {
			asObject(raw)["env"] = append(list(asObject(raw)["env"]), object{"name": "RHIZA_OBJSTORE_PREFIX", "value": "root"})
		}
	}
	stsTemplateSpec := asObject(nested(sts, "spec", "template", "spec"))
	stsTemplateSpec["initContainers"] = list(podSpec["initContainers"])
	stsTemplateSpec["volumes"] = list(podSpec["volumes"])
	ledger := fenceLedger{StatefulSetUID: "workload-uid", Ledger: fencing.Ledger{SourceClusterID: "source"}}
	bucket := objstore.NewInMemBucket()
	seedPeerClosureAutoBinding(t, bucket, "root", root, "workload-uid", "workload-uid", autoBinding(&rootResourceValue))
	executor := &FencingExecutor{Kube: &Kubernetes{Namespace: "test"}, Bucket: bucket, Prefix: "root", StoreIdentity: StoreIdentity(map[string]string{
		"RHIZA_OBJSTORE_PROVIDER": "s3", "RHIZA_OBJSTORE_BUCKET": "test-bucket", "RHIZA_OBJSTORE_PREFIX": "root",
	})}
	ledgerPath := executor.ledgerPath("workload-uid", "source")
	var liveRoot = root
	var credentialSecret object
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/apis/authentication.k8s.io/v1/tokenreviews":
			_ = json.NewEncoder(w).Encode(object{"status": object{"authenticated": true, "audiences": []any{"rhiza-fencer"}, "user": object{
				"username": "system:serviceaccount:test:default",
				"extra":    object{"authentication.kubernetes.io/pod-uid": []any{podUID}, "authentication.kubernetes.io/pod-name": []any{podName}},
			}}})
		case "/api/v1/namespaces/test/pods/" + podName:
			_ = json.NewEncoder(w).Encode(pod)
		case "/apis/apps/v1/namespaces/test/statefulsets/rhiza":
			_ = json.NewEncoder(w).Encode(sts)
		case "/api/v1/namespaces/test/secrets/cluster-credentials":
			_ = json.NewEncoder(w).Encode(object{"metadata": object{"name": "cluster-credentials", "namespace": "test"}, "data": object{"members": base64.StdEncoding.EncodeToString([]byte("[]"))}})
		case "/api/v1/namespaces/test/secrets/rhiza-initial-credentials":
			if credentialSecret == nil {
				http.NotFound(w, r)
				return
			}
			_ = json.NewEncoder(w).Encode(credentialSecret)
		case "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizaclusters":
			_ = json.NewEncoder(w).Encode(object{"items": []any{liveRoot}})
		case ledgerPath:
			_ = json.NewEncoder(w).Encode(object{"metadata": object{"name": path.Base(ledgerPath), "namespace": "test", "uid": "ledger-uid", "resourceVersion": "1"}, "data": object{"ledger.json": string(jsonBytes(ledger))}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer api.Close()
	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte("test-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	executor.Kube = &Kubernetes{BaseURL: api.URL, Namespace: "test", TokenFile: tokenPath, Client: api.Client()}
	authority := httptest.NewTLSServer(executor.Handler())
	defer authority.Close()
	client := &fencing.AuthorityClient{URL: authority.URL + "/v1/supervisor", TokenFile: tokenPath, Client: authority.Client()}
	candidate := fencing.Identity{SourceClusterID: "source", NodeID: podName, PodName: podName, PodUID: podUID}
	status, err := client.BootstrapStatus(ctx, "", candidate)
	if err != nil || status.Assignment != nil || status.AssignmentSetHash != fencing.BootstrapAssignmentSetHash(nil) {
		t.Fatalf("clean initial learner was not reported unassigned: status=%+v err=%v", status, err)
	}
	ledger.Ledger.MembershipRepairHistory = map[string]fencing.MembershipRepairRecord{"retained-unknown-operation": {}}
	if _, err := client.BootstrapStatus(ctx, "", candidate); err == nil {
		t.Fatal("accepted an unassigned response with retained membership-repair history")
	}
	ledger.Ledger.MembershipRepairHistory = nil
	// With no initial pin, an exact operation query still fails closed.
	if _, err := client.BootstrapStatus(ctx, operationID, candidate); err == nil {
		t.Fatal("accepted an operation-specific status without its retained bootstrap pin")
	}
	// Seed a legitimate ordinary initial assignment after the no-repair read.
	// Empty own-init status must stay UNASSIGNED so Node can resolve the exact
	// operation next; the operation-specific lookup must return that assignment.
	members := []quepaxa.Member{voterMember("source", "rhiza-0", "token-0"), voterMember("source", "rhiza-1", "token-1"), voterMember("source", "rhiza-2", "token-2")}
	membersBytes := jsonBytes(members)
	membershipHash := hashJSON(members)
	profileHash := prepareProfileHash(sts)
	credentialName, credentialUID := "rhiza-initial-credentials", "initial-secret-uid"
	owner := &Resource{Metadata: object{"uid": rootUID}, Spec: Spec{RecoveryID: "logical"}, Status: Status{Target: "source"}}
	credentialSecret = object{"metadata": object{"name": credentialName, "namespace": "test", "uid": credentialUID, "annotations": object{ownerAnnotation: (&Controller{}).operationID(owner)}}, "immutable": true,
		"data": object{"members": base64.StdEncoding.EncodeToString(membersBytes)}}
	authorizationHash := hashJSON(struct{ RootUID, StatefulSetUID, SourceClusterID, CredentialName, CredentialUID, MembershipHash, ProfileHash string }{
		rootUID, "workload-uid", "source", credentialName, credentialUID, membershipHash, profileHash,
	})
	assignment := fencing.BootstrapAssignment{OperationID: operationID, RequestHash: hashJSON(expectedBootstrapCandidate(&rootResourceValue, sts)), AuthorizationHash: authorizationHash,
		Purpose: fencing.BootstrapInitial, RootUID: rootUID, StatefulSetUID: "workload-uid", SourceClusterID: "source", PodName: podName, PodUID: podUID, NodeID: podName,
		CredentialName: credentialName, CredentialUID: credentialUID, MembershipHash: membershipHash, ProfileHash: profileHash, LedgerUID: "ledger-uid"}
	pin := bootstrapLedgerPin{Version: 1, OperationID: operationID, RequestHash: assignment.RequestHash, AuthorizationHash: authorizationHash, RootUID: rootUID,
		StatefulSetUID: "workload-uid", SourceClusterID: "source", CredentialName: credentialName, CredentialUID: credentialUID, MembershipHash: membershipHash, ProfileHash: profileHash, LedgerUID: "ledger-uid"}
	controller := &Controller{Kube: &Kubernetes{Namespace: "test"}, Bucket: bucket, Prefix: "root"}
	if err := uploadBootstrapTestObject(ctx, bucket, controller.bootstrapCandidateKey(&rootResourceValue), expectedBootstrapCandidate(&rootResourceValue, sts)); err != nil {
		t.Fatal(err)
	}
	if err := uploadBootstrapTestObject(ctx, bucket, controller.bootstrapLedgerPinKey(&rootResourceValue), pin); err != nil {
		t.Fatal(err)
	}
	ledgerState := fencing.Ledger{SourceClusterID: "source"}
	if err := ledgerState.InstallBootstrapOperation(fencing.BootstrapOperation{OperationID: operationID, RequestHash: assignment.RequestHash, AuthorizationHash: authorizationHash,
		Purpose: fencing.BootstrapInitial, RootUID: rootUID, StatefulSetUID: "workload-uid", SourceClusterID: "source"}); err != nil {
		t.Fatal(err)
	}
	if err := ledgerState.AssignBootstrap(assignment); err != nil {
		t.Fatalf("assign ordinary initial bootstrap assignment %+v: %v", assignment, err)
	}
	ledger = fenceLedger{StatefulSetUID: "workload-uid", Ledger: ledgerState}
	status, err = client.BootstrapStatus(ctx, "", candidate)
	if err != nil || status.Assignment != nil {
		t.Fatalf("ordinary initial assignment leaked into own-init status: status=%+v err=%v", status, err)
	}
	status, err = client.BootstrapStatus(ctx, operationID, candidate)
	if err != nil || status.Assignment == nil || *status.Assignment != assignment {
		t.Fatalf("exact ordinary initial status=%+v err=%v", status, err)
	}
	liveRoot = objectFromCluster(rootResourceValue)
	asObject(liveRoot["metadata"])["uid"] = "different-root-uid"
	if _, err := client.BootstrapStatus(ctx, "", candidate); err == nil {
		t.Fatal("accepted an unassigned result after the managed root UID changed")
	}
}

func TestMembershipRepairSourceObservationIsBoundToRetainedSlotAndLivePodUID(t *testing.T) {
	ctx := context.Background()
	const sourceToken = "source-token"
	const adminToken = "admin-probe-token"
	oldUID := "old-pod-uid"
	newUID := "replacement-pod-uid"
	oldWAL := fmt.Sprintf("%x", sha256.Sum256([]byte("old-wal")))
	sourceWAL := fmt.Sprintf("%x", sha256.Sum256([]byte("source-wal")))
	var targetUIDMu sync.Mutex
	targetUID := newUID
	var sourceCore *quepaxa.Core
	var sourceArchiveBucket objstore.Bucket
	var sourceArchivePrefix string
	var sourceMembers []quepaxa.Member
	var sourceCoreMu sync.Mutex
	var allowCompletion atomic.Bool
	var completionRequestsMu sync.Mutex
	var completionRequests []network.MembershipRepairCompletionRequest

	probe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			var action struct {
				Action string `json:"action"`
			}
			body, readErr := io.ReadAll(io.LimitReader(r.Body, (64<<10)+1))
			if readErr != nil || len(body) > 64<<10 || json.Unmarshal(body, &action) != nil {
				http.Error(w, "invalid action", http.StatusBadRequest)
				return
			}
			if action.Action == network.MembershipRepairCompletionAction {
				completion, err := network.StrictDecodeMembershipRepairCompletionRequest(body)
				if err != nil || r.Header.Get("Authorization") != "Bearer "+adminToken {
					http.Error(w, "invalid completion request", http.StatusBadRequest)
					return
				}
				completionRequestsMu.Lock()
				completionRequests = append(completionRequests, completion)
				completionRequestsMu.Unlock()
				if !allowCompletion.Load() {
					http.Error(w, "candidate ACK unavailable", http.StatusServiceUnavailable)
					return
				}
				if sourceCore == nil || sourceArchiveBucket == nil {
					http.Error(w, "completion archive unavailable", http.StatusServiceUnavailable)
					return
				}
				sourceCoreMu.Lock()
				decision, completeErr := sourceCore.CompleteDecision(r.Context(), quepaxa.Slot(completion.TerminalSlot))
				if completeErr != nil || uint64(decision.Slot) != completion.TerminalSlot {
					sourceCoreMu.Unlock()
					http.Error(w, "terminal completion failed", http.StatusServiceUnavailable)
					return
				}
				writer := recovery.NewManager(sourceArchiveBucket, sourceArchivePrefix, 1)
				completeErr = writer.SyncThrough(r.Context(), sourceCore, sourceCore.Tip())
				writer.Close()
				sourceCoreMu.Unlock()
				if completeErr != nil {
					http.Error(w, "terminal archive sync failed", http.StatusServiceUnavailable)
					return
				}
				requestDigest, err := network.MembershipRepairCompletionRequestDigest(completion)
				if err != nil {
					http.Error(w, "terminal request digest failed", http.StatusInternalServerError)
					return
				}
				proof := network.MembershipRepairCompletionResponse{Version: completion.Version, Action: completion.Action, Challenge: completion.Challenge,
					AttemptRevision: completion.AttemptRevision, ScopeDigest: completion.ScopeDigest, RequestDigest: requestDigest,
					ExecutorNodeID: completion.ExecutorNodeID, ExecutorPodUID: completion.ExecutorPodUID, ExecutorRunID: completion.ExecutorRunID,
					ExecutorWALIdentity: completion.ExecutorWALIdentity, TerminalSlot: completion.TerminalSlot, TerminalHash: completion.TerminalHash, Outcome: "completed"}
				proof.MAC = signMembershipRepairCompletionResponse(proof, adminToken)
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(proof)
				return
			}
			var request network.MembershipRepairProbeRequest
			if err := json.Unmarshal(body, &request); err != nil || sourceCore == nil || sourceArchiveBucket == nil {
				http.Error(w, "invalid membership-repair probe", http.StatusBadRequest)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+adminToken {
				http.Error(w, "wrong probe token", http.StatusForbidden)
				return
			}
			sourceCoreMu.Lock()
			defer sourceCoreMu.Unlock()
			nonce := network.MembershipRepairBarrierNonce(request.Challenge)
			slot, _, err := sourceCore.Propose(r.Context(), quepaxa.EncodeReadBarrier(nonce))
			if err != nil {
				http.Error(w, "could not commit challenge barrier", http.StatusConflict)
				return
			}
			writer := recovery.NewManager(sourceArchiveBucket, sourceArchivePrefix, 1)
			if err := writer.SyncThrough(r.Context(), sourceCore, sourceCore.Tip()); err != nil {
				writer.Close()
				http.Error(w, "could not sync challenge barrier", http.StatusInternalServerError)
				return
			}
			writer.Close()
			prefix, ok := sourceCore.PrefixHash(slot)
			if !ok {
				http.Error(w, "challenge prefix unavailable", http.StatusInternalServerError)
				return
			}
			response := network.MembershipRepairProbeResponse{
				Version: request.Version, Challenge: request.Challenge, ScopeDigest: request.ScopeDigest,
				VerifierNodeID: "rhiza-0", VerifierWALIdentity: sourceWAL, Ready: true, Voting: true,
				SourceConfigID: request.SourceConfigID, SourceMembers: append([]quepaxa.Member(nil), sourceMembers...),
				AbortSlot: request.AbortSlot, ExcludedOldMemberID: request.ExcludedOldMemberID,
				ReadIndexSlot: uint64(slot), BarrierSlot: uint64(slot), BarrierNonce: hex.EncodeToString(nonce[:]),
				PrefixHash: hex.EncodeToString(prefix[:]), CandidateMember: request.CandidateMember, CandidateBindRef: request.CandidateBindRef,
				CandidatePrefixThrough: uint64(slot), CandidatePrefixHash: hex.EncodeToString(prefix[:]),
			}
			response.MAC = signMembershipRepairProbeResponse(response, adminToken)
			encodedResponse, err := json.Marshal(response)
			if err != nil {
				http.Error(w, "could not encode membership-repair probe", http.StatusInternalServerError)
				return
			}
			if _, err := network.StrictDecodeMembershipRepairProbeResponse(encodedResponse, request, adminToken); err != nil {
				t.Errorf("test runtime emitted invalid membership-repair proof: %v", err)
				http.Error(w, "invalid membership-repair probe response", http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(encodedResponse)
			return
		}
		nonce := r.URL.Query().Get("nonce")
		excluded := r.URL.Query().Get("exclude")
		sourceCoreMu.Lock()
		defer sourceCoreMu.Unlock()
		configID := uint(1)
		voters := []string{"rhiza-0", "rhiza-1"}
		readIndex, certifiedTip := uint64(1), uint64(1)
		if sourceCore != nil {
			configID = sourceCore.ConfigID()
			voters = make([]string, 0, len(sourceMembers))
			for _, member := range sourceMembers {
				voters = append(voters, string(member.ID))
			}
			certifiedTip = uint64(sourceCore.Tip())
			if certifiedTip > 0 {
				readIndex = certifiedTip
			}
		}
		status := network.VoterRecoveryStatus{
			NodeID: "rhiza-0", ClusterID: "source", ConfigID: configID, WALIdentity: sourceWAL,
			Voters: voters, Voting: true, AbortSlot: 0,
			Quorum: true, QuorumKnown: true, QuorumConfigID: configID, ReadIndexSlot: readIndex, CertifiedTip: certifiedTip,
		}
		observation := network.RecoveryProbe{Version: network.RecoveryProbeProtocolVersion, Excluded: excluded, Nonce: nonce, Status: status}
		observation.MAC = peerClosureProbeMAC(observation, adminToken)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(observation)
	}))
	defer probe.Close()
	probeURL, err := url.Parse(probe.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, port, err := net.SplitHostPort(probeURL.Host)
	if err != nil {
		t.Fatal(err)
	}

	oldMember := quepaxa.Member{ID: "rhiza-1", URL: "http://old", PeerURL: "quic://old", LogURL: "http://old-log", PublicKey: quepaxa.PublicKey(sha256.Sum256([]byte("old-key"))), WALIdentity: oldWAL}
	survivor := quepaxa.Member{ID: "rhiza-0", URL: "http://survivor", PeerURL: "quic://survivor", LogURL: "http://survivor-log", PublicKey: quepaxa.PublicKey(sha256.Sum256([]byte("survivor-key"))), WALIdentity: sourceWAL}
	thirdWAL := fmt.Sprintf("%x", sha256.Sum256([]byte("third-wal")))
	third := quepaxa.Member{ID: "rhiza-2", URL: "http://third", PeerURL: "quic://third", LogURL: "http://third-log", PublicKey: quepaxa.PublicKey(sha256.Sum256([]byte("third-key"))), WALIdentity: thirdWAL}
	members := []quepaxa.Member{survivor, oldMember, third}
	bucket := objstore.NewInMemBucket()
	archivePrefix := path.Join("root", "source")
	sourceArchiveBucket, sourceArchivePrefix, sourceMembers = bucket, archivePrefix, append([]quepaxa.Member(nil), members...)
	if err := uploadBootstrapTestObject(ctx, bucket, path.Join(archivePrefix, "voters", "membership.json"), recovery.NewMembershipRecord("source", members, "async")); err != nil {
		t.Fatal(err)
	}
	clusterTransport := &membershipRepairTestTransport{cores: make(map[quepaxa.NodeID]*quepaxa.Core), down: make(map[quepaxa.NodeID]bool), dropDecision: make(map[quepaxa.NodeID]bool)}
	allowTestVoterAdmission := func(context.Context, quepaxa.Cluster, quepaxa.Slot, [32]byte) error { return nil }
	for _, member := range members {
		wal, err := qlog.Open(path.Join(t.TempDir(), string(member.ID)))
		if err != nil {
			t.Fatal(err)
		}
		identity, err := hex.DecodeString(member.WALIdentity)
		if err != nil || wal.BindIdentity(identity) != nil {
			t.Fatalf("bind %s WAL identity", member.ID)
		}
		t.Cleanup(func() { _ = wal.Close() })
		core, err := quepaxa.New(quepaxa.Config{NodeID: member.ID, Cluster: quepaxa.Cluster{ConfigID: 1, Members: append([]quepaxa.Member(nil), members...)}, WAL: wal, Transport: clusterTransport, EnableReconfiguration: true, ReconfigurationAdmission: allowTestVoterAdmission})
		if err != nil {
			t.Fatal(err)
		}
		clusterTransport.cores[member.ID] = core
	}
	sourceCore = clusterTransport.cores[survivor.ID]
	var nonce [quepaxa.ReadBarrierNonceSize]byte
	if _, _, err := sourceCore.Propose(ctx, quepaxa.EncodeReadBarrier(nonce)); err != nil {
		t.Fatal(err)
	}
	writer := recovery.NewManager(bucket, archivePrefix, 1)
	if err := writer.SyncThrough(ctx, sourceCore, sourceCore.Tip()); err != nil {
		t.Fatal(err)
	}
	writer.Close()

	root := ClusterResource{Metadata: object{"name": "rhiza", "namespace": "test", "uid": "root-uid", "generation": float64(1)},
		Spec:   ClusterSpec{LogicalID: "logical", StatefulSet: "rhiza", Container: "rhiza", AdoptClusterID: "source", Automatic: true},
		Status: ClusterStatus{ActiveClusterID: "source", ObservedGeneration: 1}}
	normalizeClusterSpec(&root)
	rootObj := rootResource(objectFromCluster(root))
	rootConfig := rootResource(objectFromCluster(root))
	rootConfig.Spec = root.Spec
	state := autoControl{Version: 1, BindingUID: "root-uid", BindingHash: autoBinding(&rootConfig), BindingVersion: autoBindingVersion,
		StatefulSetUID: "workload-uid", ActiveClusterID: "source",
		Voters: []VoterPod{{NodeID: "rhiza-0", Pod: "rhiza-0"}, {NodeID: "rhiza-1", Pod: "rhiza-1"}, {NodeID: "rhiza-2", Pod: "rhiza-2"}},
		Identities: map[quepaxa.NodeID]FenceTarget{
			"rhiza-0": {NodeID: "rhiza-0", Pod: "rhiza-0", PodUID: "caller-pod-uid", WALIdentity: sourceWAL},
			"rhiza-1": {NodeID: "rhiza-1", Pod: "rhiza-1", PodUID: oldUID, WALIdentity: oldWAL},
			"rhiza-2": {NodeID: "rhiza-2", Pod: "rhiza-2", PodUID: "third-pod-uid", WALIdentity: thirdWAL},
		}}
	if err := bucket.Upload(ctx, path.Join("root", "automatic", "test", "logical", "control.json"), bytes.NewReader(jsonBytes(state)), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	if err := bucket.Upload(ctx, path.Join("root", "automatic-workloads", "test", "workload-uid", "binding.json"), bytes.NewReader(jsonBytes(object{"logicalID": "logical", "bindingUID": "root-uid", "bindingHash": autoBinding(&rootConfig)})), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}

	callerPod := peerClosureCallerPod(port)
	callerMetadata := asObject(callerPod["metadata"])
	callerMetadata["name"], callerMetadata["uid"] = "rhiza-0", "caller-pod-uid"
	callerMetadata["annotations"].(object)[fencingRootAnnotation] = "root-uid"
	thirdPod := peerClosureCallerPod(port)
	thirdMetadata := asObject(thirdPod["metadata"])
	thirdMetadata["name"], thirdMetadata["uid"] = "rhiza-2", "third-pod-uid"
	thirdMetadata["annotations"].(object)[fencingRootAnnotation] = "root-uid"
	sts := peerClosureStatefulSet(port)
	sts["metadata"] = object{"name": "rhiza", "uid": "workload-uid", "generation": float64(1)}
	stsSpec := asObject(sts["spec"])
	stsSpec["serviceName"], stsSpec["podManagementPolicy"] = "rhiza-headless", "Parallel"
	template := asObject(stsSpec["template"])
	template["metadata"] = object{"annotations": object{fencingProtocolAnnotation: "1"}}
	templateSpec := asObject(template["spec"])
	profileSpec := asObject(prepareProfilePod()["spec"])
	templateSpec["initContainers"], templateSpec["volumes"] = list(profileSpec["initContainers"]), list(profileSpec["volumes"])
	bootstrapCandidate := expectedBootstrapCandidate(&rootConfig, sts)
	bootstrapOpID := bootstrapOperationID("root-uid", "workload-uid", bootstrapCandidate)
	bootstrapSecretName := bootstrapSecretName(&rootConfig)
	bootstrapProfileHash := prepareProfileHash(sts)
	bootstrapMembershipHash := hashJSON(members)
	bootstrapAuthorizationHash := hashJSON(struct {
		RootUID, StatefulSetUID, SourceClusterID, CredentialName, CredentialUID, MembershipHash, ProfileHash string
	}{"root-uid", "workload-uid", "source", bootstrapSecretName, "initial-secret-uid", bootstrapMembershipHash, bootstrapProfileHash})
	bootstrapPin := bootstrapLedgerPin{Version: 1, OperationID: bootstrapOpID, RequestHash: hashJSON(bootstrapCandidate), AuthorizationHash: bootstrapAuthorizationHash,
		RootUID: "root-uid", StatefulSetUID: "workload-uid", SourceClusterID: "source", CredentialName: bootstrapSecretName,
		CredentialUID: "initial-secret-uid", MembershipHash: bootstrapMembershipHash, ProfileHash: bootstrapProfileHash, LedgerUID: "ledger-uid"}
	owner := &Resource{Metadata: object{"uid": "root-uid"}, Spec: Spec{RecoveryID: root.Spec.LogicalID}, Status: Status{Target: "source"}}
	initialSecret := object{"apiVersion": "v1", "kind": "Secret", "immutable": true, "type": "Opaque",
		"metadata": object{"name": bootstrapSecretName, "namespace": "test", "uid": "initial-secret-uid", "annotations": object{ownerAnnotation: (&Controller{}).operationID(owner)}},
		"data":     object{"members": base64.StdEncoding.EncodeToString(jsonBytes(members))}}
	bootstrapOp := fencing.BootstrapOperation{OperationID: bootstrapOpID, RequestHash: bootstrapPin.RequestHash, AuthorizationHash: bootstrapAuthorizationHash,
		Purpose: fencing.BootstrapInitial, RootUID: "root-uid", StatefulSetUID: "workload-uid", SourceClusterID: "source", AdmissionClosed: true, Complete: true}
	bootstrapHistory := &fencing.BootstrapHistory{RootUID: "root-uid", StatefulSetUID: "workload-uid", Operations: map[string]fencing.BootstrapOperation{bootstrapOpID: bootstrapOp}}
	if err := uploadBootstrapTestObject(ctx, bucket, (&Controller{Kube: &Kubernetes{Namespace: "test"}, Prefix: "root"}).bootstrapCandidateKey(&rootConfig), bootstrapCandidate); err != nil {
		t.Fatal(err)
	}
	if err := uploadBootstrapTestObject(ctx, bucket, (&Controller{Kube: &Kubernetes{Namespace: "test"}, Prefix: "root"}).bootstrapLedgerPinKey(&rootConfig), bootstrapPin); err != nil {
		t.Fatal(err)
	}
	main := asObject(list(nested(sts, "spec", "template", "spec", "containers"))[0])
	main["env"] = []any{
		object{"name": "RHIZA_CLUSTER_ID", "value": "source"}, object{"name": "RHIZA_ADMIN_TOKEN", "value": adminToken},
		object{"name": "RHIZA_CLUSTER_MEMBERS", "value": string(jsonBytes(members))}, object{"name": "RHIZA_OBJSTORE_PROVIDER", "value": "s3"},
		object{"name": "RHIZA_OBJSTORE_ENDPOINT", "value": "http://127.0.0.1:1"},
		object{"name": "RHIZA_OBJSTORE_BUCKET", "value": "test-bucket"}, object{"name": "RHIZA_OBJSTORE_PREFIX", "value": "root"},
		object{"name": "RHIZA_BIND_ADDR", "value": "0.0.0.0:8080"}, object{"name": "RHIZA_PEER_ADDR", "value": "0.0.0.0:9090"},
	}
	asObject(list(nested(callerPod, "spec", "containers"))[0])["env"] = main["env"]
	newPod := prepareProfilePod()
	newMain := asObject(list(nested(newPod, "spec", "containers"))[0])
	newInit := asObject(list(nested(newPod, "spec", "initContainers"))[0])
	for _, container := range []object{newMain, newInit} {
		setBootstrapEnv(container, "RHIZA_CLUSTER_ID", "source")
		setBootstrapEnv(container, "RHIZA_OBJSTORE_ENDPOINT", "http://127.0.0.1:1")
		setBootstrapEnv(container, "RHIZA_OBJSTORE_BUCKET", "test-bucket")
		setBootstrapEnv(container, "RHIZA_OBJSTORE_PREFIX", "root")
	}
	asObject(newPod["spec"])["nodeName"] = "worker"
	newPod["status"] = object{"podIP": "127.0.0.1", "initContainerStatuses": []any{object{
		"name": prepareInitContainerName, "containerID": "containerd://learner-init", "imageID": "sha256:learner-image", "state": object{"running": object{"startedAt": "2026-10-28T00:00:00Z"}},
	}}}
	newMetadata := asObject(newPod["metadata"])
	newMetadata["name"], newMetadata["uid"] = "rhiza-1", newUID
	newAnnotations := asObject(newMetadata["annotations"])
	newAnnotations[fencingRootAnnotation] = "root-uid"
	newAnnotations[fencingPrepareOperationAnnotation] = bootstrapOpID
	newMetadata["ownerReferences"] = []any{object{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "rhiza", "uid": "workload-uid", "controller": true}}

	ledgerPath := (&FencingExecutor{Kube: &Kubernetes{Namespace: "test"}}).ledgerPath("workload-uid", "source")
	ledger := fenceLedger{StatefulSetUID: "workload-uid", Ledger: fencing.Ledger{SourceClusterID: "source", Bootstrap: bootstrapHistory, Runs: map[string]fencing.Run{
		"caller-run": {Identity: fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "caller-pod-uid", RunID: "caller-run", WALIdentity: sourceWAL}},
		"old-run":    {Identity: fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-1", PodUID: oldUID, RunID: "old-run", WALIdentity: oldWAL}},
		"third-run":  {Identity: fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-2", PodUID: "third-pod-uid", RunID: "third-run", WALIdentity: thirdWAL}},
	}}}
	ledgerMu := sync.Mutex{}
	ledgerRV := 1
	closeOnNextLedgerConflict := false
	closeRaceNode := ""
	secretMu := sync.Mutex{}
	authMu := sync.Mutex{}
	authPodUID, authPodName := "caller-pod-uid", "rhiza-0"
	legacyCredential := object{"immutable": true, "metadata": object{"name": "cluster-credentials", "namespace": "test", "uid": "legacy-secret-uid"},
		"data": object{"members": base64.StdEncoding.EncodeToString(jsonBytes(members))}}
	secretObjects := map[string]object{
		path.Join("/api/v1/namespaces/test/secrets", bootstrapSecretName): initialSecret,
		"/api/v1/namespaces/test/secrets/cluster-credentials":             legacyCredential,
	}
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == ledgerPath && r.Method == http.MethodPut {
			ledgerMu.Lock()
			defer ledgerMu.Unlock()
			var updated object
			if err := json.NewDecoder(r.Body).Decode(&updated); err != nil {
				http.Error(w, "invalid ConfigMap", http.StatusBadRequest)
				return
			}
			if closeOnNextLedgerConflict {
				closeOnNextLedgerConflict = false
				if err := ledger.Ledger.CloseNodes("race-close", "race-close-hash", []string{closeRaceNode}); err != nil {
					t.Errorf("test could not commit closure between CAS attempts: %v", err)
					http.Error(w, "could not close init admission", http.StatusInternalServerError)
					return
				}
				ledgerRV++
				http.Error(w, "simulated resourceVersion conflict after closure committed", http.StatusConflict)
				return
			}
			ledger = fenceLedger{}
			if err := json.Unmarshal([]byte(str(nested(updated, "data", "ledger.json"))), &ledger); err != nil {
				http.Error(w, "invalid ledger", http.StatusBadRequest)
				return
			}
			ledgerRV++
			asObject(updated["metadata"])["resourceVersion"] = fmt.Sprint(ledgerRV)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(updated)
			return
		}
		if r.URL.Path == "/api/v1/namespaces/test/secrets" || strings.HasPrefix(r.URL.Path, "/api/v1/namespaces/test/secrets/") {
			secretMu.Lock()
			defer secretMu.Unlock()
			if r.Method == http.MethodPost {
				var secret object
				if err := json.NewDecoder(r.Body).Decode(&secret); err != nil {
					http.Error(w, "invalid Secret", http.StatusBadRequest)
					return
				}
				metadata := asObject(secret["metadata"])
				name := str(metadata["name"])
				metadata["namespace"], metadata["uid"] = "test", "learner-secret-uid-"+hashJSON(name)[:12]
				secretObjects[path.Join("/api/v1/namespaces/test/secrets", name)] = secret
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(secret)
				return
			}
			secret, ok := secretObjects[r.URL.Path]
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(secret)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/apis/authentication.k8s.io/v1/tokenreviews":
			var review object
			_ = json.NewDecoder(r.Body).Decode(&review)
			if str(nested(review, "spec", "token")) != sourceToken {
				http.Error(w, "wrong token", http.StatusForbidden)
				return
			}
			authMu.Lock()
			reviewedUID, reviewedName := authPodUID, authPodName
			authMu.Unlock()
			_ = json.NewEncoder(w).Encode(object{"status": object{"authenticated": true, "audiences": []any{"rhiza-fencer"}, "user": object{
				"username": "system:serviceaccount:test:default", "extra": object{"authentication.kubernetes.io/pod-uid": []any{reviewedUID}, "authentication.kubernetes.io/pod-name": []any{reviewedName}},
			}}})
		case "/api/v1/namespaces/test/pods/rhiza-0":
			_ = json.NewEncoder(w).Encode(callerPod)
		case "/api/v1/namespaces/test/pods/rhiza-2":
			_ = json.NewEncoder(w).Encode(thirdPod)
		case "/api/v1/namespaces/test/pods/rhiza-1":
			targetUIDMu.Lock()
			newMetadata["uid"] = targetUID
			targetUIDMu.Unlock()
			_ = json.NewEncoder(w).Encode(newPod)
		case "/apis/apps/v1/namespaces/test/statefulsets/rhiza":
			_ = json.NewEncoder(w).Encode(sts)
		case "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizaclusters":
			_ = json.NewEncoder(w).Encode(object{"items": []any{rootObj}})
		case "/api/v1/namespaces/test/nodes/worker":
			_ = json.NewEncoder(w).Encode(object{"metadata": object{"name": "worker", "uid": "worker-uid"}})
		case ledgerPath:
			ledgerMu.Lock()
			defer ledgerMu.Unlock()
			_ = json.NewEncoder(w).Encode(object{"metadata": object{"name": path.Base(ledgerPath), "namespace": "test", "uid": "ledger-uid", "resourceVersion": fmt.Sprint(ledgerRV)}, "data": object{"ledger.json": string(jsonBytes(ledger))}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer api.Close()
	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte(sourceToken), 0o600); err != nil {
		t.Fatal(err)
	}
	kube := &Kubernetes{BaseURL: api.URL, Namespace: "test", TokenFile: tokenPath, Client: api.Client()}
	executor := &FencingExecutor{Kube: kube, Bucket: bucket, Prefix: "root", StoreIdentity: StoreIdentity(map[string]string{"RHIZA_OBJSTORE_PROVIDER": "s3", "RHIZA_OBJSTORE_ENDPOINT": "http://127.0.0.1:1", "RHIZA_OBJSTORE_BUCKET": "test-bucket", "RHIZA_OBJSTORE_PREFIX": "root"})}
	authority := httptest.NewTLSServer(executor.Handler())
	defer authority.Close()
	client := &fencing.AuthorityClient{URL: authority.URL + "/v1/supervisor", TokenFile: tokenPath, Client: authority.Client()}
	caller := fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "caller-pod-uid", RunID: "caller-run", WALIdentity: sourceWAL}
	result, err := client.ReadMembershipRepairForTarget(ctx, caller, "rhiza-1")
	if err != nil || result.Repair != nil || result.Observation == nil || result.Observation.TargetNodeID != "rhiza-1" || result.Observation.TargetWALIdentity != oldWAL || result.Observation.TargetPodUID != oldUID || result.Observation.ReplacementPodUID != newUID {
		t.Fatalf("source observation=%+v repair=%v err=%v", result.Observation, result.Repair, err)
	}
	if _, err := client.ReadMembershipRepairForTarget(ctx, caller, "rhiza-0"); err == nil {
		t.Fatal("source caller selected its own slot for replacement")
	}
	targetUIDMu.Lock()
	targetUID = oldUID
	targetUIDMu.Unlock()
	if _, err := client.ReadMembershipRepairForTarget(ctx, caller, "rhiza-1"); err == nil {
		t.Fatal("same-UID restart was reported as a new replacement incarnation")
	}
	targetUIDMu.Lock()
	targetUID = newUID
	targetUIDMu.Unlock()
	expectedAbort := uint64(0)
	request := fencing.MembershipRepairReserveRequest{ExpectedConfigID: 1, ExpectedAbortSlot: &expectedAbort, TargetNodeID: "rhiza-1", TargetWALIdentity: oldWAL}
	reserved, err := client.ReserveMembershipRepair(ctx, caller, request)
	if err != nil || reserved.Repair == nil || len(reserved.Repair.Progress.Candidates) != 1 {
		t.Fatalf("source reserve did not persist one exact learner candidate: repair=%+v err=%v", reserved.Repair, err)
	}
	assignment := reserved.Repair.Progress.Candidates[0].Assignment
	if assignment.PodUID != newUID || assignment.NodeID != "pod-"+newUID || assignment.Purpose != fencing.BootstrapLearner {
		t.Fatalf("reserved assignment is not bound to live replacement UID: %+v", assignment)
	}
	if assignment.PublicMember.LogURL != "" {
		t.Fatalf("actual immutable learner credential unexpectedly has LogURL %q", assignment.PublicMember.LogURL)
	}
	learnerKey, err := hex.DecodeString(assignment.PublicMember.PublicKey)
	if err != nil || len(learnerKey) != len(quepaxa.PublicKey{}) {
		t.Fatalf("actual learner assignment public key is invalid: %q", assignment.PublicMember.PublicKey)
	}
	var learnerPublicKey quepaxa.PublicKey
	copy(learnerPublicKey[:], learnerKey)
	learnerConfig := &types.ExecutionConfig{
		ClusterID: "source", NodeID: types.NodeID(assignment.NodeID), PreparedPodUID: assignment.PodUID,
		DataDir: t.TempDir(), EnableReconfiguration: true, Members: append([]quepaxa.Member(nil), members...),
		Learner: &quepaxa.Member{ID: quepaxa.NodeID(assignment.PublicMember.ID), URL: assignment.PublicMember.URL,
			PeerURL: assignment.PublicMember.PeerURL, LogURL: assignment.PublicMember.LogURL, PublicKey: learnerPublicKey},
		ObjStoreProvider: "s3", ObjStoreEndpoint: "http://127.0.0.1:1", ObjStoreBucket: "test-bucket",
		ObjStorePrefix: "root", ObjStoreDurability: types.ObjectStoreDurabilityAsync,
	}
	_, _, prepareErr := node.PrepareManagedInit(ctx, learnerConfig, assignment)
	if prepareErr == nil || strings.Contains(prepareErr.Error(), "managed learner assignment differs") || strings.Contains(prepareErr.Error(), "requires its exact assigned public identity") {
		t.Fatalf("actual retained learner assignment did not pass Node binding validation before its unavailable disposable archive: %v", prepareErr)
	}
	secretMu.Lock()
	_, immutableCredentialReadBack := secretObjects[path.Join("/api/v1/namespaces/test/secrets", assignment.CredentialName)]
	secretMu.Unlock()
	if !immutableCredentialReadBack || assignment.CredentialUID == "" {
		t.Fatalf("candidate assignment did not bind read-back immutable credential: %+v", assignment)
	}
	ledgerMu.Lock()
	retained := ledger.Ledger.MembershipRepair
	ledgerMu.Unlock()
	if retained == nil || len(retained.Progress.Candidates) != 1 || retained.Progress.Candidates[0].Assignment != assignment {
		t.Fatal("candidate assignment was not retained by the ledger CAS")
	}

	authMu.Lock()
	authPodUID, authPodName = newUID, "rhiza-1"
	authMu.Unlock()
	initIdentity := fencing.Identity{SourceClusterID: "source", NodeID: assignment.NodeID, PodName: assignment.PodName, PodUID: assignment.PodUID,
		CredentialName: assignment.CredentialName, CredentialUID: assignment.CredentialUID}
	status, err := client.BootstrapStatus(ctx, "", initIdentity)
	if err != nil || status.Assignment == nil || *status.Assignment != assignment {
		t.Fatalf("readonly own-init discovery status=%+v err=%v", status, err)
	}
	ledgerMu.Lock()
	openLedgerSnapshot, err := json.Marshal(ledger)
	if err != nil {
		ledgerMu.Unlock()
		t.Fatal(err)
	}
	openLedgerRV := ledgerRV
	closeOnNextLedgerConflict, closeRaceNode = true, assignment.NodeID
	ledgerMu.Unlock()
	if _, err := client.BeginInit(ctx, assignment, "learner-attempt-closure-race"); err == nil {
		t.Fatal("real init authority path admitted work after closure won its CAS conflict")
	}
	ledgerMu.Lock()
	if _, closed := ledger.Ledger.ClosedNodes[assignment.NodeID]; !closed {
		ledgerMu.Unlock()
		t.Fatal("CAS conflict fixture did not commit node closure before retry")
	}
	if len(ledger.Ledger.MembershipRepair.Progress.Candidates[0].Attempts) != 0 {
		ledgerMu.Unlock()
		t.Fatal("failed CAS retry retained a new learner init attempt")
	}
	var restoredLedger fenceLedger
	if err := json.Unmarshal(openLedgerSnapshot, &restoredLedger); err != nil {
		ledgerMu.Unlock()
		t.Fatal(err)
	}
	ledger = restoredLedger
	ledgerRV = openLedgerRV
	closeOnNextLedgerConflict, closeRaceNode = false, ""
	ledgerMu.Unlock()
	attempt, err := client.BeginInit(ctx, assignment, "learner-attempt-1")
	if err != nil || attempt.Assignment != assignment || attempt.Scope.ContainerID != "containerd://learner-init" {
		t.Fatalf("learner init begin did not retain live execution scope: attempt=%+v err=%v", attempt, err)
	}
	outcome := fencing.InitAttemptOutcome{State: fencing.InitAttemptMaterialized, LiveCleanup: fencing.InitLiveCleanup{Mode: fencing.InitModeRestore, BucketDrainSucceeded: true, CleanupSucceeded: true, NamespaceQuiescent: true}}
	if err := client.RecordInitOutcome(ctx, assignment, "learner-attempt-1", outcome); err != nil {
		t.Fatalf("learner init outcome was not accepted: %v", err)
	}
	status, err = client.BootstrapStatus(ctx, "", initIdentity)
	if err != nil || status.Assignment == nil || len(status.Attempts) != 1 || len(status.Attempts[0].Outcomes) != 1 || status.Attempts[0].Outcomes[0] != outcome {
		t.Fatalf("learner init outcome was not discoverable: status=%+v err=%v", status, err)
	}
	initStatus := asObject(list(nested(newPod, "status", "initContainerStatuses"))[0])
	initStatus["state"] = object{"terminated": object{"containerID": "containerd://learner-init", "imageID": "sha256:learner-image", "exitCode": float64(0), "reason": "Completed"}}
	if err := executor.recordObservedInitTerminal(ctx, initIdentity, "workload-uid"); err != nil {
		t.Fatalf("learner init terminal evidence was not observed before runtime registration: %v", err)
	}
	candidate := fencing.Identity{SourceClusterID: "source", NodeID: assignment.NodeID, PodName: assignment.PodName, PodUID: assignment.PodUID,
		RunID: "learner-run-1", CredentialName: assignment.CredentialName, CredentialUID: assignment.CredentialUID}
	registration, err := executor.registerIncarnation(ctx, candidate, "workload-uid")
	if err != nil {
		t.Fatalf("retained learner assignment could not register its runtime: %v", err)
	}
	runtime := registration.Identity
	runtime.WALIdentity = fmt.Sprintf("%x", sha256.Sum256([]byte("learner-wal-1")))
	if err := executor.bindRuntime(ctx, "workload-uid", runtime); err != nil {
		t.Fatalf("learner runtime WAL bind failed: %v", err)
	}
	if err := executor.recordMembershipRepairBind(ctx, "workload-uid", runtime); err != nil {
		t.Fatalf("learner bind did not advance candidate to the bound stage: %v", err)
	}
	// Qualification is authorized by the current source voter, not by the
	// candidate's init-container identity used above for begin/outcome.
	authMu.Lock()
	authPodUID, authPodName = "caller-pod-uid", "rhiza-0"
	authMu.Unlock()
	qualified, err := client.QualifyMembershipRepair(ctx, caller, fencing.MembershipRepairQualificationRequest{
		OperationID: reserved.Repair.Authorization.OperationID, CandidatePodUID: assignment.PodUID,
	})
	if err != nil || qualified.OperationID != reserved.Repair.Authorization.OperationID || qualified.Preclosure == nil || qualified.Preclosure.CandidateMember.WALIdentity != runtime.WALIdentity || qualified.Preclosure.CandidateBindRef != fencing.BootstrapAssignmentDigest(assignment) {
		t.Fatalf("authority did not qualify exact bound candidate using certified challenge barrier: result=%+v err=%v", qualified, err)
	}

	// Drive a real REMOVE and ADD through the source Core and archived history,
	// then exercise the authority-owned completion challenge over the exact
	// /recovery/probe action. A missing candidate ACK must leave the repair Added.
	removeTarget := quepaxa.Cluster{ConfigID: 2, Members: []quepaxa.Member{survivor, third}}
	sourceCoreMu.Lock()
	removeFreeze, err := sourceCore.BeginReconfigurationAt(ctx, removeTarget, 0)
	if err == nil {
		err = sourceCore.FinishReconfiguration(ctx)
	}
	if err == nil {
		writer := recovery.NewManager(bucket, archivePrefix, 1)
		err = writer.SyncThrough(ctx, sourceCore, sourceCore.Tip())
		writer.Close()
	}
	sourceCoreMu.Unlock()
	if err != nil {
		t.Fatalf("certify and archive REMOVE transition: %v", err)
	}
	zero := quepaxa.Slot(0)
	removePhase := fencing.MembershipRepairPhase{Kind: fencing.MembershipRepairRemove, CandidatePodUID: assignment.PodUID, AbortSlot: &zero,
		Change: network.MembershipChange{OperationID: membershipRepairPhaseOperationID(reserved.Repair.Authorization.OperationID, fencing.MembershipRepairRemove),
			ClusterID: "source", ExpectedConfigID: 1, ExpectedAbortSlot: 0, Remove: oldMember.ID,
			Fence: &network.MembershipFence{NodeID: oldMember.ID, WALIdentity: oldWAL, WorkloadUID: "workload-uid", Confirmed: true, Evidence: strings.Repeat("a", 64)}}}
	removePhase.Hash, err = fencing.MembershipRepairPhaseHash(removePhase)
	if err != nil {
		t.Fatal(err)
	}
	ledgerMu.Lock()
	if err = ledger.Ledger.AdvanceMembershipRepair(reserved.Repair.Authorization.OperationID, ledger.Ledger.MembershipRepair.Progress.Revision, fencing.MembershipRepairOldClosed); err == nil {
		err = ledger.Ledger.AppendMembershipRepairPhase(removePhase)
	}
	ledgerMu.Unlock()
	if err != nil {
		t.Fatalf("retain exact REMOVE phase fixture: %v", err)
	}
	removeResult, err := client.ObserveMembershipRepairPhase(ctx, caller, fencing.MembershipRepairPhaseObserveRequest{OperationID: reserved.Repair.Authorization.OperationID,
		PhaseHash: removePhase.Hash, JournalOperationID: removePhase.Change.OperationID, JournalRequestHash: hashJSON(removePhase.Change), Target: removeTarget, TerminalSlot: uint64(removeFreeze + 16)})
	if err != nil || removeResult.Stage != fencing.MembershipRepairRemoved {
		t.Fatalf("observe certified REMOVE: result=%+v err=%v", removeResult, err)
	}
	learnerWAL, err := qlog.Open(path.Join(t.TempDir(), "candidate-voter"))
	if err != nil {
		t.Fatal(err)
	}
	candidateWAL, err := hex.DecodeString(qualified.Preclosure.CandidateMember.WALIdentity)
	if err != nil || learnerWAL.BindIdentity(candidateWAL) != nil {
		t.Fatal("bind candidate voter WAL fixture identity")
	}
	t.Cleanup(func() { _ = learnerWAL.Close() })
	candidateCore, err := quepaxa.NewLearner(quepaxa.Config{NodeID: qualified.Preclosure.CandidateMember.ID, Cluster: removeTarget, WAL: learnerWAL, Transport: clusterTransport, EnableReconfiguration: true, ReconfigurationAdmission: allowTestVoterAdmission})
	if err != nil {
		t.Fatalf("create candidate learner Core: %v", err)
	}
	clusterTransport.mu.Lock()
	clusterTransport.cores[qualified.Preclosure.CandidateMember.ID] = candidateCore
	clusterTransport.mu.Unlock()
	addPrepared, err := client.PrepareMembershipRepairPhase(ctx, caller, fencing.MembershipRepairPhasePrepareRequest{OperationID: reserved.Repair.Authorization.OperationID, Kind: fencing.MembershipRepairAdd})
	if err != nil {
		t.Fatalf("prepare exact ADD phase: %v", err)
	}
	addTarget := quepaxa.Cluster{ConfigID: 3, Members: []quepaxa.Member{survivor, third, qualified.Preclosure.CandidateMember}}
	sourceCoreMu.Lock()
	addFreeze, addErr := sourceCore.BeginReconfigurationAt(ctx, addTarget, 0)
	if addErr == nil {
		addErr = sourceCore.FinishReconfiguration(ctx)
	}
	if addErr == nil {
		writer := recovery.NewManager(bucket, archivePrefix, 1)
		addErr = writer.SyncThrough(ctx, sourceCore, sourceCore.Tip())
		writer.Close()
	}
	sourceCoreMu.Unlock()
	if addErr != nil {
		t.Fatalf("certify and archive ADD transition: %v", addErr)
	}
	sourceCoreMu.Lock()
	sourceMembers = append([]quepaxa.Member(nil), addTarget.Members...)
	sourceCoreMu.Unlock()
	addResult, err := client.ObserveMembershipRepairPhase(ctx, caller, fencing.MembershipRepairPhaseObserveRequest{OperationID: reserved.Repair.Authorization.OperationID,
		PhaseHash: addPrepared.Phase.Hash, JournalOperationID: addPrepared.Phase.Change.OperationID, JournalRequestHash: hashJSON(addPrepared.Phase.Change), Target: addTarget, TerminalSlot: uint64(addFreeze + 16)})
	if err != nil || addResult.Stage != fencing.MembershipRepairAdded {
		t.Fatalf("observe certified ADD: result=%+v err=%v", addResult, err)
	}
	completionRequest := fencing.MembershipRepairPhaseObserveRequest{OperationID: reserved.Repair.Authorization.OperationID, PhaseHash: addPrepared.Phase.Hash,
		JournalOperationID: addPrepared.Phase.Change.OperationID, JournalRequestHash: hashJSON(addPrepared.Phase.Change), Target: addTarget, TerminalSlot: uint64(addFreeze + 16)}
	if _, err := client.CompleteMembershipRepairPhase(ctx, caller, completionRequest); err == nil {
		t.Fatal("completed ADD without candidate terminal ACK")
	} else {
		t.Logf("blocked completion result: %v", err)
	}
	ledgerMu.Lock()
	retainedCompletion := ledger.Ledger.MembershipRepair.Progress.Completion
	if ledger.Ledger.MembershipRepair.Progress.Stage != fencing.MembershipRepairAdded || retainedCompletion == nil || retainedCompletion.Accepted != nil {
		stage := ledger.Ledger.MembershipRepair.Progress.Stage
		ledgerMu.Unlock()
		t.Fatalf("missing runtime ACK did not preserve Added with an unaccepted retained challenge: stage=%s completion=%+v", stage, retainedCompletion)
	}
	firstChallenge := retainedCompletion.Request.Challenge
	firstRevision := retainedCompletion.Request.AttemptRevision
	ledgerMu.Unlock()
	thirdCaller := fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-2", PodUID: "third-pod-uid", RunID: "third-run", WALIdentity: thirdWAL}
	authMu.Lock()
	authPodUID, authPodName = "third-pod-uid", "rhiza-2"
	authMu.Unlock()
	if _, err := client.CompleteMembershipRepairPhase(ctx, thirdCaller, completionRequest); err == nil {
		t.Fatal("second eligible driver completed while the retained executor ACK was unavailable")
	} else {
		t.Logf("second-driver retry result: %v", err)
	}
	authMu.Lock()
	authPodUID, authPodName = "caller-pod-uid", "rhiza-0"
	authMu.Unlock()
	wrongRequest := completionRequest
	wrongRequest.JournalRequestHash = strings.Repeat("b", 64)
	if _, err := client.CompleteMembershipRepairPhase(ctx, caller, wrongRequest); err == nil {
		t.Fatal("accepted a completion request with a different Node journal hash")
	}
	allowCompletion.Store(true)
	completed, err := client.CompleteMembershipRepairPhase(ctx, caller, completionRequest)
	if err != nil || !completed.Complete || completed.Stage != fencing.MembershipRepairComplete {
		t.Fatalf("exact certified terminal completion=%+v err=%v", completed, err)
	}
	ledgerMu.Lock()
	accepted := ledger.Ledger.MembershipRepair.Progress.Completion
	ledgerMu.Unlock()
	if accepted == nil || accepted.Accepted == nil || accepted.Request.Challenge != firstChallenge || accepted.Request.AttemptRevision != firstRevision {
		t.Fatal("retry did not accept the same retained completion challenge")
	}
	if retry, err := client.CompleteMembershipRepairPhase(ctx, caller, completionRequest); err != nil || !retry.Complete {
		t.Fatalf("lost authority response was not idempotent: result=%+v err=%v", retry, err)
	}
	completionRequestsMu.Lock()
	if len(completionRequests) != 3 || completionRequests[0].Challenge != completionRequests[1].Challenge || completionRequests[1].Challenge != completionRequests[2].Challenge ||
		completionRequests[0].AttemptRevision != completionRequests[1].AttemptRevision || completionRequests[1].AttemptRevision != completionRequests[2].AttemptRevision ||
		completionRequests[0].ExecutorNodeID != "rhiza-0" || completionRequests[1].ExecutorNodeID != "rhiza-0" || completionRequests[2].ExecutorNodeID != "rhiza-0" {
		completionRequestsMu.Unlock()
		t.Fatalf("runtime retries did not converge on the retained challenge: %+v", completionRequests)
	}
	completionRequestsMu.Unlock()

	// The next replacement of the same ordinal must use the just-completed
	// candidate as its old identity, even though the original autoControl was
	// not advanced by an operator process during the repair.
	const secondReplacementUID = "replacement-pod-uid-2"
	targetUIDMu.Lock()
	targetUID = secondReplacementUID
	newMetadata["uid"] = secondReplacementUID
	targetUIDMu.Unlock()
	if _, err := client.ReadMembershipRepairForTarget(ctx, caller, "rhiza-1"); !errors.Is(err, fencing.ErrPending) {
		t.Fatalf("completed repair projection refresh should persist before admission retry: %v", err)
	}
	controller := &Controller{Kube: kube, Bucket: bucket, Prefix: "root"}
	projected, _, err := controller.loadAuto(ctx, controller.autoKey(&root))
	if err != nil {
		t.Fatal(err)
	}
	if len(projected.Voters) != 3 || projected.Voters[1] != (VoterPod{NodeID: quepaxa.NodeID(assignment.NodeID), Pod: "rhiza-1"}) ||
		projected.Identities[quepaxa.NodeID(assignment.NodeID)] != (FenceTarget{NodeID: assignment.NodeID, Pod: "rhiza-1", PodUID: newUID, WALIdentity: runtime.WALIdentity}) {
		t.Fatalf("completed candidate was not installed into the existing autoControl projection: voters=%+v identity=%+v", projected.Voters, projected.Identities[quepaxa.NodeID(assignment.NodeID)])
	}
	next, err := client.ReadMembershipRepairForTarget(ctx, caller, "rhiza-1")
	if err != nil || next.Observation == nil || next.Observation.TargetNodeID != assignment.NodeID || next.Observation.TargetPodUID != newUID ||
		next.Observation.TargetWALIdentity != runtime.WALIdentity || next.Observation.ReplacementPodUID != secondReplacementUID {
		t.Fatalf("second same-ordinal replacement did not use the first accepted successor: observation=%+v err=%v", next.Observation, err)
	}
}

func objectFromCluster(value ClusterResource) object {
	data, _ := json.Marshal(value)
	var result object
	_ = json.Unmarshal(data, &result)
	return result
}

func signMembershipRepairProbeResponse(response network.MembershipRepairProbeResponse, token string) string {
	response.MAC = ""
	data, _ := json.Marshal(response)
	mac := hmac.New(sha256.New, []byte(token))
	_, _ = mac.Write(data)
	return hex.EncodeToString(mac.Sum(nil))
}

func signMembershipRepairCompletionResponse(response network.MembershipRepairCompletionResponse, token string) string {
	response.MAC = ""
	data, _ := json.Marshal(response)
	mac := hmac.New(sha256.New, []byte(token))
	_, _ = mac.Write(data)
	return hex.EncodeToString(mac.Sum(nil))
}

type membershipRepairTestTransport struct {
	mu           sync.RWMutex
	cores        map[quepaxa.NodeID]*quepaxa.Core
	down         map[quepaxa.NodeID]bool
	dropDecision map[quepaxa.NodeID]bool
}

func (transport *membershipRepairTestTransport) SendRecord(ctx context.Context, to quepaxa.NodeID, request quepaxa.RecordRequest) (quepaxa.Summary, error) {
	transport.mu.RLock()
	core, down := transport.cores[to], transport.down[to]
	transport.mu.RUnlock()
	if core == nil || down {
		return quepaxa.Summary{}, errors.New("replica down")
	}
	if through := core.RecordCatchUpThrough(request); through > core.Tip() {
		transport.mu.RLock()
		var source *quepaxa.Core
		for id, candidate := range transport.cores {
			if !transport.down[id] && (source == nil || candidate.Tip() > source.Tip()) {
				source = candidate
			}
		}
		transport.mu.RUnlock()
		if source != nil && source != core {
			values, _, err := source.DecisionsFrom(core.Tip()+1, 128)
			if err != nil {
				return quepaxa.Summary{}, err
			}
			if err := core.AcceptCertifiedHints(values); err != nil {
				return quepaxa.Summary{}, err
			}
		}
	}
	return core.Record(ctx, request)
}

func (transport *membershipRepairTestTransport) SendDecision(ctx context.Context, decision quepaxa.Decision) error {
	transport.mu.RLock()
	type peer struct {
		id   quepaxa.NodeID
		core *quepaxa.Core
	}
	peers := make([]peer, 0, len(transport.cores))
	for id, core := range transport.cores {
		if !transport.down[id] && !transport.dropDecision[id] {
			peers = append(peers, peer{id, core})
		}
	}
	transport.mu.RUnlock()
	success := 0
	for _, peer := range peers {
		if err := peer.core.AcceptDecision(decision); err == nil {
			success++
		}
	}
	if success >= len(transport.cores)/2+1 {
		return nil
	}
	return quepaxa.ErrQuorumUnavailable
}

func (transport *membershipRepairTestTransport) ReadTip(_ context.Context, to quepaxa.NodeID) (quepaxa.Slot, error) {
	transport.mu.RLock()
	defer transport.mu.RUnlock()
	core := transport.cores[to]
	if core == nil || transport.down[to] {
		return 0, errors.New("replica down")
	}
	return core.Tip(), nil
}

func (transport *membershipRepairTestTransport) StageValue(_ context.Context, to quepaxa.NodeID, hash quepaxa.ValueHash, value []byte) error {
	transport.mu.RLock()
	defer transport.mu.RUnlock()
	core := transport.cores[to]
	if core == nil || transport.down[to] {
		return errors.New("replica down")
	}
	return core.StageValue(hash, value)
}

func (transport *membershipRepairTestTransport) FetchValue(_ context.Context, from quepaxa.NodeID, hash quepaxa.ValueHash) ([]byte, error) {
	transport.mu.RLock()
	defer transport.mu.RUnlock()
	core := transport.cores[from]
	if core == nil || transport.down[from] {
		return nil, errors.New("replica down")
	}
	value, ok := core.Value(hash)
	if !ok {
		return nil, errors.New("value unavailable")
	}
	return value, nil
}
