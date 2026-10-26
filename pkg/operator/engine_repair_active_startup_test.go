package operator

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/fencing/runtimeagent"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestAuthenticatedActiveEngineRepairRestartRegistrationAndRefusals(t *testing.T) {
	t.Run("positive active restart advances current run before grant", func(t *testing.T) {
		e, fixture, record, previousRuntime := activeEngineRepairRestartFixture(t)
		request := fencing.Identity{SourceClusterID: record.Binding.SourceClusterID, NodeID: "candidate-0", PodName: "candidate-0",
			PodUID: record.CurrentCandidateIdentity().PodUID, RunID: "active-restart-run"}
		installActiveRestartPod(t, fixture, request, record)
		client, service, server := activeRestartRuntimeAgentClient(t, e)
		t.Cleanup(func() { server.Close(); service.Close() })
		e.RuntimeAgent = client

		registration := serveActiveRestartRegistration(t, e, request)
		if registration.Code != http.StatusOK {
			_, assigned, resolverErr := e.resolveEngineRepairActiveRestartRegistration(t.Context(), record.Binding.StatefulSetUID, request)
			if resolverErr == nil && !assigned {
				resolverErr = fmt.Errorf("active restart resolver did not claim the authenticated request")
			}
			t.Fatalf("active restart registration status=%d body=%s; resolver diagnostic=%v", registration.Code, registration.Body.String(), resolverErr)
		}
		var grant fencing.StartupGrant
		if err := json.NewDecoder(registration.Body).Decode(&grant); err != nil {
			t.Fatal(err)
		}
		if grant.Role != fencing.StartupRoleVoterActive || grant.Identity.NodeID != record.CandidateIdentity.NodeID ||
			grant.Identity.PodUID != request.PodUID || grant.Identity.RunID != request.RunID || grant.Identity.WALIdentity != "" ||
			grant.Identity.AssignmentID != record.CandidateIdentity.AssignmentID || grant.Environment["RHIZA_NODE_ID"] != record.CandidateIdentity.NodeID {
			t.Fatalf("active restart grant differs from canonical candidate: %+v", grant)
		}
		root, _, err := e.AuthorityState.ReadRoot(t.Context(), record.Binding.StatefulSetUID, record.Binding.SourceClusterID)
		if err != nil {
			t.Fatal(err)
		}
		key := mustEngineRepairGenerationKey(t, record)
		reference := root.Ledger.EngineRepairs[key]
		run, ok := root.Ledger.Runs[request.RunID]
		if !ok || reference.CandidateIdentity.RunID != request.RunID || run.Identity.RunID != request.RunID ||
			run.Identity.NodeID != record.CandidateIdentity.NodeID || run.Identity.WALIdentity != record.CandidateIdentity.WALIdentity ||
			run.Runtime == nil || run.Runtime.PodUID != request.PodUID || *run.Runtime == previousRuntime ||
			run.EngineRepairProvenance == nil || run.EngineRepairProvenance.OperationID != record.Binding.OperationID ||
			run.EngineRepairProvenance.IdentityDigest != record.IdentityDigest() ||
			run.EngineRepairProvenance.CandidateIdentity != reference.CandidateIdentity ||
			run.EngineRepairProvenance.RuntimeBindingDigest != fencing.RuntimeBindingDigest(*run.Runtime) || run.Promotion == nil ||
			run.MutationProducerUID == "" || run.MutationScopeHash == "" || run.MutationProfile == "" || run.MutationStageIncarnation == "" {
			t.Fatalf("root did not pin exact active replacement Run before granting: ref=%+v run=%+v", reference, run)
		}
		// Model a lost grant response after the root CAS: the supervisor retries
		// the same physical Run request and receives the already-pinned grant.
		retryResponse := serveActiveRestartRegistration(t, e, request)
		if retryResponse.Code != http.StatusOK {
			_, assigned, resolverErr := e.resolveEngineRepairActiveRestartRegistration(t.Context(), record.Binding.StatefulSetUID, request)
			if resolverErr == nil && !assigned {
				resolverErr = fmt.Errorf("active restart resolver did not claim the authenticated retry")
			}
			t.Fatalf("exact active restart retry status=%d body=%s; same-state resolver diagnostic=%v", retryResponse.Code, retryResponse.Body.String(), resolverErr)
		}
		var grantRetry fencing.StartupGrant
		if err := json.NewDecoder(retryResponse.Body).Decode(&grantRetry); err != nil {
			t.Fatal(err)
		}
		if grantRetry.Role != grant.Role || grantRetry.Identity != grant.Identity ||
			grantRetry.Environment["RHIZA_NODE_ID"] != grant.Environment["RHIZA_NODE_ID"] ||
			grantRetry.Environment["RHIZA_PEER_TOKEN"] != grant.Environment["RHIZA_PEER_TOKEN"] ||
			grantRetry.Environment["RHIZA_CLUSTER_MEMBERS"] != grant.Environment["RHIZA_CLUSTER_MEMBERS"] {
			t.Fatalf("exact active restart retry returned a different grant: first=%+v retry=%+v", grant, grantRetry)
		}
		retriedRoot, _, err := e.AuthorityState.ReadRoot(t.Context(), record.Binding.StatefulSetUID, record.Binding.SourceClusterID)
		if err != nil {
			t.Fatal(err)
		}
		if len(retriedRoot.Ledger.Runs) != len(root.Ledger.Runs) ||
			retriedRoot.Ledger.EngineRepairs[key] != reference ||
			hashJSON(retriedRoot.Ledger.Runs) != hashJSON(root.Ledger.Runs) {
			t.Fatalf("exact active restart retry changed root enrollment or history: before=%+v after=%+v", root.Ledger, retriedRoot.Ledger)
		}
		producer, producerUID, _, err := e.AuthorityState.ReadProducer(t.Context(), *run.Runtime)
		if err != nil || producerUID != run.MutationProducerUID || producer.Runtime != *run.Runtime ||
			producer.WALIdentity != run.Identity.WALIdentity || producer.Phase != fencing.MutationReserved ||
			producer.LastAdmitted != 0 || len(producer.Entries) != 0 {
			t.Fatalf("active restart grant preceded exact reserved producer enrollment: producer=%+v uid=%q err=%v", producer, producerUID, err)
		}
		scope, err := e.sourceMutationStorageScope(t.Context(), *run.Runtime)
		if err != nil {
			t.Fatal(err)
		}
		stages, err := e.sourceStagesFor(t.Context(), *run.Runtime)
		if err != nil {
			t.Fatal(err)
		}
		if run.MutationScopeHash != hashJSON(scope) || run.MutationProfile != e.sourceMutationProfile() || run.MutationStageIncarnation != stages.incarnation {
			t.Fatalf("active restart producer scope/profile/stage differ from authenticated storage: run=%+v scope=%+v stage=%q", run, scope, stages.incarnation)
		}
		latest, err := NewEngineRepairStore(e.AuthorityState, record.Binding.StatefulSetUID)
		if err != nil {
			t.Fatal(err)
		}
		unchanged, current, _, err := latest.ReadEngineRepairCurrentCandidate(t.Context(), record.Binding.SourceClusterID, record.Binding.OperationID)
		if err != nil || unchanged.Digest() != record.Digest() || current.RunID != request.RunID {
			t.Fatalf("active restart rewrote history or lost current pointer: record=%+v identity=%+v err=%v", unchanged, current, err)
		}
		bindIdentity := grant.Identity
		bindIdentity.WALIdentity = record.CandidateIdentity.WALIdentity
		bindResponse := serveActiveRestartRequest(t, e, "bind", bindIdentity)
		if bindResponse.Code != http.StatusOK {
			t.Fatalf("active restart WAL bind status=%d body=%s", bindResponse.Code, bindResponse.Body.String())
		}
		var receipt fencing.BindReceipt
		if err := json.NewDecoder(bindResponse.Body).Decode(&receipt); err != nil {
			t.Fatal(err)
		}
		if receipt.Identity != bindIdentity || receipt.Role != fencing.StartupRoleVoterActive || receipt.Producer == nil {
			t.Fatalf("active restart did not resume ordinary active-voter binding: %+v", receipt)
		}
	})

	for _, tc := range []struct {
		name              string
		mutate            func(*fencing.Identity, *fenceLedger)
		closingFromActive bool
		advanceCurrent    bool
		status            int
	}{
		{name: "closed root", status: http.StatusGone, mutate: func(_ *fencing.Identity, root *fenceLedger) {
			root.Ledger.ClosedBy, root.Ledger.ClosedHash = "dr-close", hashBytes([]byte("dr-closed"))
		}},
		{name: "active repair closing", status: http.StatusGone, closingFromActive: true},
		{name: "stale RunID after current pointer advances", status: http.StatusGone, advanceCurrent: true,
			mutate: func(id *fencing.Identity, _ *fenceLedger) { id.RunID = "candidate-run" }},
		{name: "wrong Pod", status: http.StatusForbidden, mutate: func(id *fencing.Identity, _ *fenceLedger) { id.PodUID = "different-pod-uid" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, fixture, record, _ := activeEngineRepairRestartFixture(t)
			request := fencing.Identity{SourceClusterID: record.Binding.SourceClusterID, NodeID: "candidate-0", PodName: "candidate-0",
				PodUID: record.CurrentCandidateIdentity().PodUID, RunID: "active-restart-run"}
			installActiveRestartPod(t, fixture, request, record)
			root, version, err := e.AuthorityState.ReadRoot(t.Context(), record.Binding.StatefulSetUID, record.Binding.SourceClusterID)
			if err != nil {
				t.Fatal(err)
			}
			if tc.advanceCurrent {
				client, service, server := activeRestartRuntimeAgentClient(t, e)
				t.Cleanup(func() { server.Close(); service.Close() })
				e.RuntimeAgent = client
				if grant := postActiveRestartRegistration(t, e, request); grant.Role != fencing.StartupRoleVoterActive || grant.Identity.RunID != request.RunID {
					t.Fatalf("initial replacement did not advance to an active Run: %+v", grant)
				}
			}
			if tc.closingFromActive {
				store, storeErr := NewEngineRepairStore(e.AuthorityState, record.Binding.StatefulSetUID)
				if storeErr != nil {
					t.Fatal(storeErr)
				}
				closing := record
				closing.Phase = fencing.RepairGenerationClosing
				closing.ClosingFrom = fencing.RepairActive
				if _, storeErr := store.CompareAndSwapEngineRepair(t.Context(), record.Binding.SourceClusterID, record.Digest(), closing); storeErr != nil {
					t.Fatalf("mark active repair as generation-closing: %v", storeErr)
				}
			} else if tc.mutate != nil {
				tc.mutate(&request, &root)
			}
			if tc.name == "closed root" {
				if err := e.AuthorityState.WriteRoot(t.Context(), record.Binding.StatefulSetUID, record.Binding.SourceClusterID, root, version); err != nil {
					t.Fatal(err)
				}
			}
			beforeReject, _, err := e.AuthorityState.ReadRoot(t.Context(), record.Binding.StatefulSetUID, record.Binding.SourceClusterID)
			if err != nil {
				t.Fatal(err)
			}
			response := serveActiveRestartRegistration(t, e, request)
			if response.Code != tc.status {
				t.Fatalf("handler status=%d, want %d; body=%s", response.Code, tc.status, response.Body.String())
			}
			latest, _, err := e.AuthorityState.ReadRoot(t.Context(), record.Binding.StatefulSetUID, record.Binding.SourceClusterID)
			if err != nil {
				t.Fatal(err)
			}
			if tc.advanceCurrent {
				store, storeErr := NewEngineRepairStore(e.AuthorityState, record.Binding.StatefulSetUID)
				if storeErr != nil {
					t.Fatal(storeErr)
				}
				_, current, _, readErr := store.ReadEngineRepairCurrentCandidate(t.Context(), record.Binding.SourceClusterID, record.Binding.OperationID)
				if readErr != nil || current.RunID != "active-restart-run" || len(latest.Ledger.Runs) != len(beforeReject.Ledger.Runs) ||
					hashJSON(latest.Ledger.Runs) != hashJSON(beforeReject.Ledger.Runs) {
					t.Fatalf("stale RunID changed the advanced current candidate or run history: current=%+v err=%v before=%+v after=%+v",
						current, readErr, beforeReject.Ledger.Runs, latest.Ledger.Runs)
				}
			} else if _, exists := latest.Ledger.Runs["active-restart-run"]; exists {
				t.Fatal("rejected active restart registered a Run")
			}
		})
	}
}

func activeEngineRepairRestartFixture(t *testing.T) (*FencingExecutor, *mutationStoreFixture, fencing.EngineRepairRecord, fencing.RuntimeBinding) {
	t.Helper()
	e, fixture, sourceIdentity, sourceRuntime := newMutationStoreFixture(t)
	scopeHash := preparePendingArchiveGenesis(t, e, fixture, sourceIdentity, sourceRuntime)
	if _, err := e.reserveMutationProducer(t.Context(), sourceRuntime); err != nil {
		t.Fatal(err)
	}
	sourceIdentity.WALIdentity = "old-wal"
	if _, err := e.bindWALAtScope(t.Context(), sourceRuntime.StatefulSetUID, sourceIdentity, scopeHash); err != nil {
		t.Fatal(err)
	}
	root, _, err := e.AuthorityState.ReadRoot(t.Context(), sourceRuntime.StatefulSetUID, sourceRuntime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	incarnations, err := fencing.SortedRepairIncarnations(root.Ledger)
	if err != nil {
		t.Fatal(err)
	}
	oldMember := quepaxa.Member{ID: quepaxa.NodeID(sourceIdentity.NodeID), URL: "http://old:8080", PeerURL: "quic://old:9090", LogURL: "http://old:8081", WALIdentity: sourceIdentity.WALIdentity, PublicKey: quepaxa.PublicKey{1}}
	survivor := quepaxa.Member{ID: "survivor", URL: "http://survivor:8080", PeerURL: "quic://survivor:9090", LogURL: "http://survivor:8081", WALIdentity: "survivor-wal", PublicKey: quepaxa.PublicKey{2}}
	token := "candidate-peer-token"
	candidateIdentity := fencing.Identity{SourceClusterID: sourceIdentity.SourceClusterID, NodeID: "candidate", PodName: "candidate-0", PodUID: "candidate-pod",
		RunID: "candidate-run", WALIdentity: strings.Repeat("d", 64), AssignmentID: strings.Repeat("b", 64)}
	candidateMember := quepaxa.Member{ID: quepaxa.NodeID(candidateIdentity.NodeID), URL: "http://candidate:8080", PeerURL: "quic://candidate:9090",
		LogURL: "http://candidate:8081", WALIdentity: candidateIdentity.WALIdentity,
		PublicKey: quepaxa.PublicKey(network.PeerPublicKey(types.ClusterID(candidateIdentity.SourceClusterID), quepaxa.NodeID(candidateIdentity.NodeID), token))}
	binding := fencing.EngineRemovalBinding{Version: fencing.EngineRemovalBindingVersion, Mode: fencing.EngineRemovalMode,
		OperationID: "active-restart-operation", RequestHash: strings.Repeat("a", 64), SourceClusterID: sourceIdentity.SourceClusterID,
		StatefulSetUID: sourceRuntime.StatefulSetUID, OldConfigID: 1, ExpectedAbortSlot: 3, OldMemberID: sourceIdentity.NodeID,
		OldWALIdentity: sourceIdentity.WALIdentity, TargetMemberID: candidateIdentity.NodeID}
	record := fencing.EngineRepairRecord{Version: fencing.EngineRepairRecordVersion, Binding: binding,
		SourceConfig:          quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{oldMember, survivor}},
		SourceAdminCredential: fencing.EngineRepairAdminCredential{Name: "source-auth", UID: "source-auth-uid", AdminTokenDigest: fencing.EngineRepairAdminTokenDigest([]byte("source-admin-token"))},
		Incarnations:          incarnations, CandidateIdentity: candidateIdentity, CandidateMember: candidateMember,
		CandidateCredential:   fencing.EngineRepairCredential{Name: "candidate-auth", UID: "candidate-auth-uid", PeerTokenDigest: fencing.EngineRepairPeerTokenDigest([]byte(token))},
		CandidateMemberDigest: fencing.EngineRepairMemberDigest(candidateMember), RemoveRequestHash: strings.Repeat("e", 64), Phase: fencing.RepairCandidatePrepared}
	store, err := NewEngineRepairStore(e.AuthorityState, binding.StatefulSetUID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateOrReadEngineRepair(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	runtime := engineRepairStoreCandidateRuntime(record)
	runtime.NamespaceUID, runtime.BindingUID = "namespace-uid", "binding-uid"
	runtime.NodeName, runtime.NodeUID = "node", "node-uid"
	runtime.MachineID, runtime.BootID, runtime.RuntimeID = "machine-id", "boot-id", "containerd"
	runtime.SandboxID, runtime.ContainerID, runtime.ImageID = "prior-sandbox", "containerd://prior-container-id-0000000000000000", "rhiza@sha256:"+strings.Repeat("c", 64)
	record, err = store.PinCandidateRuntime(t.Context(), binding.SourceClusterID, binding.OperationID, record.Digest(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	record = freezeEngineRepairRecord(record)
	record.StorageProofDigest, record.RemoveTerminalDigest = hashBytes([]byte("storage-proof")), hashBytes([]byte("remove-terminal"))
	add := network.MembershipChange{OperationID: engineRepairAddOperationID(binding.OperationID), ClusterID: binding.SourceClusterID,
		ExpectedConfigID: record.RemovalOperation.Target.ConfigID, ExpectedAbortSlot: 0, Add: &record.CandidateMember}
	record.AddRequestHash = hashJSON(add)
	record.Phase = fencing.RepairActive
	root, version, err := e.AuthorityState.ReadRoot(t.Context(), binding.StatefulSetUID, binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if err := root.Ledger.Bind(record.CandidateIdentity); err != nil {
		t.Fatal(err)
	}
	if err := root.Ledger.PromoteLearner(record.CandidateIdentity, engineRepairAddOperationID(binding.OperationID), hashBytes([]byte("promotion-proof"))); err != nil {
		t.Fatal(err)
	}
	key := mustEngineRepairGenerationKey(t, record)
	root.Ledger.EngineRepairs[key] = engineRepairRootReference(record)
	if err := record.ValidateAgainstLedger(root.Ledger); err != nil {
		t.Fatalf("construct canonical Active repair fixture: %v", err)
	}
	if err := store.persistImmutableRecord(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	if err := e.AuthorityState.WriteRoot(t.Context(), binding.StatefulSetUID, binding.SourceClusterID, root, version); err != nil {
		t.Fatal(err)
	}
	_, identity, runtime, err := store.ReadEngineRepairCurrentCandidate(t.Context(), record.Binding.SourceClusterID, record.Binding.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	// Both the store CAS fixture and the HTTP startup fixture use the same
	// already-active producer enrollment precondition.
	enrollActiveEngineRepairMutationProducer(t, e, identity, runtime)
	return e, fixture, record, runtime
}

func installActiveRestartPod(t *testing.T, fixture *mutationStoreFixture, identity fencing.Identity, record fencing.EngineRepairRecord) {
	t.Helper()
	installAdoptionAuthPod(t, fixture, identity, record.SourceConfig.Members)
	fixture.mu.Lock()
	asObject(fixture.authPod["spec"])["nodeName"] = "node"
	asObject(asObject(fixture.authPod["metadata"])["annotations"])[fencingRootAnnotation] = record.CandidateRuntimeBinding.BindingUID
	storageEnv := []any{
		object{"name": "RHIZA_OBJSTORE_PROVIDER", "value": "gcs"},
		object{"name": "RHIZA_OBJSTORE_BUCKET", "value": "archive"},
		object{"name": "RHIZA_OBJSTORE_PREFIX", "value": "root"},
	}
	appendStorageEnv := func(spec object) {
		containers := list(spec["containers"])
		container := asObject(containers[0])
		container["env"] = append(list(container["env"]), storageEnv...)
	}
	appendStorageEnv(asObject(fixture.authPod["spec"]))
	appendStorageEnv(asObject(nested(fixture.authStatefulSet, "spec", "template", "spec")))
	fixture.authPod["status"] = object{"containerStatuses": []any{object{"name": "rhiza", "containerID": "containerd://" + strings.Repeat("a", 64),
		"imageID": "rhiza@sha256:" + strings.Repeat("b", 64)}}}
	fixture.objects["/api/v1/namespaces/test"] = object{"metadata": object{"name": "test", "uid": "namespace-uid"}}
	fixture.objects["/api/v1/nodes/node"] = object{"metadata": object{"name": "node", "uid": "node-uid"},
		"status": object{"addresses": []any{object{"type": "InternalIP", "address": "127.0.0.1"}}}}
	fixture.objects["/api/v1/namespaces/test/secrets/"+record.CandidateCredential.Name] = object{"type": "Opaque",
		"metadata": object{"name": record.CandidateCredential.Name, "namespace": "test", "uid": record.CandidateCredential.UID},
		"data":     object{"RHIZA_PEER_TOKEN": base64.StdEncoding.EncodeToString([]byte("candidate-peer-token"))}}
	fixture.mu.Unlock()
}

func activeRestartRuntimeAgentClient(t *testing.T, e *FencingExecutor) (*RuntimeAgentClient, *runtimeagent.Service, *httptest.Server) {
	t.Helper()
	directory, err := os.MkdirTemp(t.TempDir(), "runtime-agent-")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	service, err := runtimeagent.New(runtimeagent.Config{AuthorityID: "authority", AuthorityNamespace: "test", AuthorityServiceAccount: "runtime-agent",
		AuthorityWorkloadUID: "runtime-agent-workload", Audience: "rhiza-runtime-agent", Directory: directory,
		Auth: supervisorRegistrationAgentAuth{}, Backend: &supervisorRegistrationAgentBackend{}})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(service.Handler())
	certificate := server.Certificate()
	serverName := ""
	if len(certificate.DNSNames) != 0 {
		serverName = certificate.DNSNames[0]
	} else if len(certificate.IPAddresses) != 0 {
		serverName = certificate.IPAddresses[0].String()
	}
	if serverName == "" {
		server.Close()
		service.Close()
		t.Fatal("runtime-agent test certificate lacks a verifiable SAN")
	}
	_, port, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		server.Close()
		service.Close()
		t.Fatal(err)
	}
	client := &RuntimeAgentClient{Kube: e.Kube, AuthorityID: "authority", TokenFile: e.Kube.TokenFile,
		CA: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw}), ServerName: serverName, Port: port}
	return client, service, server
}

func postActiveRestartRegistration(t *testing.T, e *FencingExecutor, identity fencing.Identity) fencing.StartupGrant {
	t.Helper()
	response := serveActiveRestartRegistration(t, e, identity)
	if response.Code != http.StatusOK {
		t.Fatalf("active restart registration status=%d body=%s", response.Code, response.Body.String())
	}
	var grant fencing.StartupGrant
	if err := json.NewDecoder(response.Body).Decode(&grant); err != nil {
		t.Fatal(err)
	}
	return grant
}

func serveActiveRestartRegistration(t *testing.T, e *FencingExecutor, identity fencing.Identity) *httptest.ResponseRecorder {
	return serveActiveRestartRequest(t, e, "register", identity)
}

func serveActiveRestartRequest(t *testing.T, e *FencingExecutor, action string, identity fencing.Identity) *httptest.ResponseRecorder {
	t.Helper()
	message := fencing.AuthorityMessage{Action: action, Identity: identity}
	body, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/supervisor", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer test-supervisor-token")
	response := httptest.NewRecorder()
	e.Handler().ServeHTTP(response, request)
	return response
}
