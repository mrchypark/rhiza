package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/fencing/cri"
	"github.com/mrchypark/rhiza/pkg/fencing/runtimeagent"
	"github.com/thanos-io/objstore"
)

type pendingProducerReadErrorBucket struct {
	objstore.Bucket
	key string
}

type pendingTerminalArtifactReadbackBucket struct {
	objstore.Bucket
	key          string
	mode         string
	versionReads int
}

func (b *pendingTerminalArtifactReadbackBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if name == b.key {
		switch b.mode {
		case "unreadable":
			return nil, errors.New("injected pending artifact read failure")
		case "changed bytes":
			return io.NopCloser(strings.NewReader("corrupted stored bytes")), nil
		}
	}
	return b.Bucket.Get(ctx, name)
}

func (b *pendingTerminalArtifactReadbackBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	attrs, err := b.Bucket.Attributes(ctx, name)
	if name == b.key && b.mode == "changed version" {
		b.versionReads++
	}
	if err == nil && name == b.key && b.mode == "changed version" && b.versionReads > 1 && attrs.Version != nil {
		version := *attrs.Version
		version.Value += "-changed"
		attrs.Version = &version
	}
	return attrs, err
}

func (b *pendingProducerReadErrorBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if name == b.key {
		return nil, errors.New("injected producer read permission failure")
	}
	return b.Bucket.Get(ctx, name)
}

func (b *pendingProducerReadErrorBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	if name == b.key {
		return objstore.ObjectAttributes{}, errors.New("injected producer read permission failure")
	}
	return b.Bucket.Attributes(ctx, name)
}

type pendingTerminalRuntimeBackend struct{}

func (pendingTerminalRuntimeBackend) Enroll(_ context.Context, base fencing.RuntimeBinding) (fencing.RuntimeBinding, json.RawMessage, error) {
	scope, err := json.Marshal(cri.CgroupScope{Path: "kubepods.slice/pending", Dev: 1, Ino: 2, PID: 3, StartTime: 4,
		RuntimeHash: fencing.RuntimeScopeBindingDigest(base), ProfileID: "pending-test-profile"})
	return base, scope, err
}

func (pendingTerminalRuntimeBackend) Stop(context.Context, fencing.RuntimeBinding, fencing.StopRequest, json.RawMessage) (json.RawMessage, error) {
	return json.RawMessage(`{"terminal":"verified"}`), nil
}

func (pendingTerminalRuntimeBackend) ResumeStop(ctx context.Context, binding fencing.RuntimeBinding, request fencing.StopRequest, scope json.RawMessage) (json.RawMessage, error) {
	return pendingTerminalRuntimeBackend{}.Stop(ctx, binding, request, scope)
}

func pendingRuntimeAgentClient(t *testing.T, e *FencingExecutor, directory string) (*RuntimeAgentClient, *runtimeagent.Service, *httptest.Server) {
	t.Helper()
	service, err := runtimeagent.New(runtimeagent.Config{AuthorityID: "authority", AuthorityNamespace: "test", AuthorityServiceAccount: "runtime-agent",
		AuthorityWorkloadUID: "runtime-agent-workload", Audience: "rhiza-runtime-agent", Directory: directory,
		Auth: supervisorRegistrationAgentAuth{}, Backend: pendingTerminalRuntimeBackend{}})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(service.Handler())
	cert := server.Certificate()
	serverName := ""
	if len(cert.DNSNames) != 0 {
		serverName = cert.DNSNames[0]
	} else if len(cert.IPAddresses) != 0 {
		serverName = cert.IPAddresses[0].String()
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
		CA: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}), ServerName: serverName, Port: port}
	return client, service, server
}

func pendingLearnerRequiredProfileFixture(t *testing.T) (*FencingExecutor, fencing.RuntimeBinding) {
	t.Helper()
	e, _, activeID, activeRuntime := newMutationStoreFixture(t)
	activateStoredProducer(t, e, activeID, activeRuntime)

	learnerID := fencing.Identity{
		SourceClusterID: activeID.SourceClusterID,
		NodeID:          "learner-1",
		PodName:         "learner-1",
		PodUID:          "learner-pod-uid",
		RunID:           "learner-run",
	}
	learnerRuntime := activeRuntime
	learnerRuntime.NodeID = learnerID.NodeID
	learnerRuntime.PodName = learnerID.PodName
	learnerRuntime.PodUID = learnerID.PodUID
	learnerRuntime.RunID = learnerID.RunID
	learnerRuntime.NodeName = "learner-node"
	learnerRuntime.NodeUID = "learner-node-uid"
	learnerRuntime.MachineID = "learner-machine"
	learnerRuntime.BootID = "learner-boot"
	learnerRuntime.RuntimeID = "learner-runtime"
	learnerRuntime.SandboxID = "learner-sandbox"
	learnerRuntime.ContainerID = "learner-container"
	learnerRuntime.ImageID = "learner-image"
	if _, err := e.change(t.Context(), activeRuntime.StatefulSetUID, activeRuntime.SourceClusterID, func(ledger *fencing.Ledger) error {
		if err := ledger.RegisterEnrolledWithRole(learnerID, learnerRuntime, fencing.StartupRoleLearnerPendingRemove); err != nil {
			return err
		}
		bound := learnerID
		bound.WALIdentity = "learner-wal"
		return ledger.Bind(bound)
	}); err != nil {
		t.Fatalf("persist canonical pending learner WAL binding: %v", err)
	}
	root, err := e.change(t.Context(), activeRuntime.StatefulSetUID, activeRuntime.SourceClusterID, func(ledger *fencing.Ledger) error {
		return ledger.Close("required-profile-generation-fence", strings.Repeat("f", 64))
	})
	if err != nil {
		t.Fatalf("close required-profile cohort: %v", err)
	}
	if !root.Ledger.RequireMutationJournal || fencing.RunRole(root.Ledger.Runs[learnerID.RunID]) != fencing.StartupRoleLearnerPendingRemove || root.Ledger.Runs[learnerID.RunID].MutationProducerUID != "" {
		t.Fatalf("fixture lost required profile or pending learner role: profile=%t run=%+v", root.Ledger.RequireMutationJournal, root.Ledger.Runs[learnerID.RunID])
	}
	return e, learnerRuntime
}

func TestPendingLearnerRequiredProfileProducerClosureCanSettleWithoutProducer(t *testing.T) {
	e, _ := pendingLearnerRequiredProfileFixture(t)
	root, err := e.read(t.Context(), "sts", "source")
	if err != nil {
		t.Fatal(err)
	}
	complete, err := e.closeMutationProducers(t.Context(), root, nil)
	if err != nil || !complete {
		t.Fatalf("required-profile producer closure remained blocked by canonical no-producer learner: complete=%t err=%v", complete, err)
	}
}

func TestPendingLearnerUnboundWALUsesNoProducerTerminalPath(t *testing.T) {
	e, _, activeID, activeRuntime := newMutationStoreFixture(t)
	activateStoredProducer(t, e, activeID, activeRuntime)
	learnerID := fencing.Identity{SourceClusterID: activeID.SourceClusterID, NodeID: "unbound-learner", PodName: "unbound-learner", PodUID: "unbound-pod", RunID: "unbound-run"}
	learnerRuntime := activeRuntime
	learnerRuntime.NodeID, learnerRuntime.PodName, learnerRuntime.PodUID, learnerRuntime.RunID = learnerID.NodeID, learnerID.PodName, learnerID.PodUID, learnerID.RunID
	learnerRuntime.NodeName, learnerRuntime.NodeUID, learnerRuntime.MachineID = "learner-node", "learner-node-uid", "learner-machine"
	learnerRuntime.BootID, learnerRuntime.RuntimeID, learnerRuntime.SandboxID = "learner-boot", "learner-runtime", "learner-sandbox"
	learnerRuntime.ContainerID, learnerRuntime.ImageID = "learner-container", "learner-image"
	if _, err := e.change(t.Context(), activeRuntime.StatefulSetUID, activeRuntime.SourceClusterID, func(l *fencing.Ledger) error {
		return l.RegisterEnrolledWithRole(learnerID, learnerRuntime, fencing.StartupRoleLearnerPendingRemove)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.change(t.Context(), activeRuntime.StatefulSetUID, activeRuntime.SourceClusterID, func(l *fencing.Ledger) error {
		return l.Close("unbound-pending-fence", strings.Repeat("d", 64))
	}); err != nil {
		t.Fatal(err)
	}
	state, err := e.read(t.Context(), activeRuntime.StatefulSetUID, activeRuntime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if got := state.Ledger.Runs[learnerID.RunID]; got.Identity.WALIdentity != "" || got.Stop == nil || got.MutationProducerUID != "" {
		t.Fatalf("fixture did not preserve closed WAL-unbound pending identity: %+v", got)
	}
	if complete, err := e.closeMutationProducers(t.Context(), state, nil); err != nil || !complete {
		t.Fatalf("mixed active/unbound-pending mutation barrier did not complete: complete=%t err=%v", complete, err)
	}
	e.VerifyRuntimeEvidence = func(_ context.Context, got fencing.RuntimeBinding, request fencing.StopRequest, receipt []byte) error {
		if got != learnerRuntime || request.Identity != learnerID || string(receipt) != "unbound-terminal" {
			return errors.New("runtime verifier rejected exact WAL-unbound pending run")
		}
		return nil
	}
	ref, err := e.retainIndependentTerminal(t.Context(), learnerRuntime, terminalTestStorage, []byte("unbound-terminal"))
	if err != nil || ref.EvidenceVersion != 3 || ref.ProducerUID != "" || ref.MutationReceiptDigest != "" {
		t.Fatalf("WAL-unbound pending run did not retain no-producer V3: ref=%+v err=%v", ref, err)
	}
	if _, uid, _, err := e.AuthorityState.ReadProducer(t.Context(), learnerRuntime); !errors.Is(err, errAuthorityStateNotFound) || uid != "" {
		t.Fatalf("WAL-unbound pending run acquired producer authority: uid=%q err=%v", uid, err)
	}
}

func TestPendingLearnerProducerClosureRejectsDeterministicProducerObject(t *testing.T) {
	e, runtime := pendingLearnerRequiredProfileFixture(t)
	state, err := e.read(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	run := state.Ledger.Runs[runtime.RunID]
	producer, err := fencing.NewMutationProducer(run.Identity, runtime)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.AuthorityState.WriteProducer(t.Context(), runtime, *producer, nil); err != nil {
		t.Fatalf("seed unexpected deterministic producer object: %v", err)
	}
	if complete, err := e.closeMutationProducers(t.Context(), state, nil); err == nil || complete {
		t.Fatalf("unexpected pending producer object did not block closure: complete=%t err=%v", complete, err)
	}
}

func TestPendingLearnerProducerClosureRejectsUnreadableOrMalformedProducer(t *testing.T) {
	for _, scenario := range []string{"permission failure", "malformed object"} {
		t.Run(scenario, func(t *testing.T) {
			e, runtime := pendingLearnerRequiredProfileFixture(t)
			key, _, err := e.AuthorityState.producerKey(runtime)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "permission failure" {
				e.AuthorityState.bucket = &pendingProducerReadErrorBucket{Bucket: e.AuthorityState.bucket, key: key}
			} else if err := e.AuthorityState.bucket.Upload(t.Context(), key, strings.NewReader("not-json")); err != nil {
				t.Fatalf("seed malformed deterministic producer object: %v", err)
			}
			state, err := e.read(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID)
			if err != nil {
				t.Fatal(err)
			}
			if complete, err := e.closeMutationProducers(t.Context(), state, nil); err == nil || complete {
				t.Fatalf("unreadable/malformed deterministic producer was treated as absent: complete=%t err=%v", complete, err)
			}
		})
	}
}

func TestIndependentFenceComposesActiveV2AndPendingV3(t *testing.T) {
	e, f, activeID, activeRuntime := newMutationStoreFixture(t)
	activateStoredProducer(t, e, activeID, activeRuntime)
	learnerID := fencing.Identity{SourceClusterID: activeID.SourceClusterID, NodeID: "learner-mixed", PodName: "learner-mixed", PodUID: "learner-mixed-pod", RunID: "learner-mixed-run"}
	learnerRuntime := activeRuntime
	learnerRuntime.NodeID, learnerRuntime.PodName, learnerRuntime.PodUID, learnerRuntime.RunID = learnerID.NodeID, learnerID.PodName, learnerID.PodUID, learnerID.RunID
	learnerRuntime.NodeName, learnerRuntime.NodeUID, learnerRuntime.MachineID = "learner-node", "learner-node-uid", "learner-machine"
	learnerRuntime.BootID, learnerRuntime.RuntimeID, learnerRuntime.SandboxID = "learner-boot", "learner-runtime", "learner-sandbox"
	learnerRuntime.ContainerID, learnerRuntime.ImageID = "learner-container", "learner-image"
	if _, err := e.change(t.Context(), activeRuntime.StatefulSetUID, activeRuntime.SourceClusterID, func(l *fencing.Ledger) error {
		if err := l.RegisterEnrolledWithRole(learnerID, learnerRuntime, fencing.StartupRoleLearnerPendingRemove); err != nil {
			return err
		}
		bound := learnerID
		bound.WALIdentity = "learner-mixed-wal"
		return l.Bind(bound)
	}); err != nil {
		t.Fatal(err)
	}
	sts := independentStatefulSet("gcs")
	asObject(sts["metadata"])["uid"] = activeRuntime.StatefulSetUID
	f.mu.Lock()
	f.objects[e.controller().stsPath("rhiza")] = sts
	f.objects[e.controller().corePath("pods", "")] = object{"items": []any{}}
	f.mu.Unlock()
	missingLearnerEvidence := true
	e.CollectRuntimeEvidence = func(_ context.Context, got fencing.RuntimeBinding, request fencing.StopRequest) ([]byte, error) {
		if request.Identity.RunID != got.RunID {
			return nil, errors.New("collector request differs from enrolled runtime")
		}
		if got.RunID == learnerID.RunID && missingLearnerEvidence {
			return nil, errors.New("pending learner runtime evidence is unavailable")
		}
		return []byte("terminal-" + got.RunID), nil
	}
	e.VerifyRuntimeEvidence = func(_ context.Context, got fencing.RuntimeBinding, request fencing.StopRequest, receipt []byte) error {
		if request.Identity.RunID != got.RunID || string(receipt) != "terminal-"+got.RunID {
			return errors.New("runtime verifier rejected exact run receipt")
		}
		return nil
	}
	request := FenceRequest{LogicalID: "logical", Namespace: "test", BindingUID: activeRuntime.BindingUID,
		StatefulSet: "rhiza", StatefulSetUID: activeRuntime.StatefulSetUID, SourceClusterID: activeRuntime.SourceClusterID,
		OperationID: "mixed-active-pending-generation", Scope: ScopeGeneration}
	record := &fenceResource{Metadata: object{"name": "mixed", "namespace": "test"}, Spec: request}
	if err := e.reconcileFence(t.Context(), v2FenceEndpoint(), record); err == nil {
		t.Fatal("mixed fence succeeded without pending learner runtime evidence")
	}
	stateAfterMissingEvidence, err := e.read(t.Context(), activeRuntime.StatefulSetUID, activeRuntime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if complete, err := e.closeMutationProducers(t.Context(), stateAfterMissingEvidence, nil); err != nil || !complete {
		t.Fatalf("producer barrier should be complete independently of learner runtime evidence: complete=%t err=%v", complete, err)
	}
	if proof := record.Status.Proof; proof != nil && (proof.ProcessesTerminated || proof.RecreationBlocked || proof.StorageQuiesced || proof.IndependentV2 != nil) {
		t.Fatalf("fence published success flags without complete terminal evidence: %+v", proof)
	}
	missingLearnerEvidence = false
	if err := e.reconcileFence(t.Context(), v2FenceEndpoint(), record); err != nil {
		t.Fatalf("mixed active/pending independent fence failed: %v", err)
	}
	evidence := record.Status.Proof.IndependentV2
	if evidence == nil || len(evidence.Terminals) != 2 {
		t.Fatalf("mixed cohort missing terminal references: %+v", record.Status.Proof)
	}
	versions := map[int]int{}
	for _, ref := range evidence.Terminals {
		versions[ref.EvidenceVersion]++
		if ref.EvidenceVersion == 3 && (ref.ProducerUID != "" || ref.MutationReceiptDigest != "") {
			t.Fatalf("pending reference claims producer authority: %+v", ref)
		}
	}
	if versions[2] != 1 || versions[3] != 1 {
		t.Fatalf("cohort did not retain active V2 and pending V3 separately: versions=%v", versions)
	}
	root, err := e.read(t.Context(), activeRuntime.StatefulSetUID, activeRuntime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if root.Ledger.Runs[learnerID.RunID].MutationProducerUID != "" {
		t.Fatal("pending terminal evidence granted producer authority")
	}
}

func TestPendingLearnerRequiredProfileRetainsTerminalEvidenceWithoutProducer(t *testing.T) {
	e, runtime := pendingLearnerRequiredProfileFixture(t)
	e.VerifyRuntimeEvidence = func(_ context.Context, got fencing.RuntimeBinding, request fencing.StopRequest, receipt []byte) error {
		if got != runtime || request.Identity.RunID != runtime.RunID || !bytes.Equal(receipt, []byte("verified-pending-learner-exit")) {
			return errors.New("test runtime verifier rejected pending learner evidence")
		}
		return nil
	}
	if _, err := e.retainIndependentTerminal(t.Context(), runtime, terminalTestStorage, []byte("verified-pending-learner-exit")); err != nil {
		t.Fatalf("retain canonical no-producer learner terminal evidence: %v", err)
	}
	root, err := e.read(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	run := root.Ledger.Runs[runtime.RunID]
	if fencing.RunRole(run) != fencing.StartupRoleLearnerPendingRemove || run.MutationProducerUID != "" {
		t.Fatalf("terminal evidence promoted learner or granted a producer: %+v", run)
	}
	if _, producerUID, _, err := e.AuthorityState.ReadProducer(t.Context(), runtime); err == nil || producerUID != "" {
		t.Fatalf("terminal evidence created source mutation authority: producerUID=%q err=%v", producerUID, err)
	}
	if count := terminalArtifactCount(t, e, runtime); count != 1 {
		t.Fatalf("retained terminal artifacts=%d, want exactly one", count)
	}
}

func TestPendingLearnerUnreadableArtifactIsNotPinnedIntoRoot(t *testing.T) {
	for _, mode := range []string{"unreadable", "changed bytes", "changed version"} {
		t.Run(mode, func(t *testing.T) {
			e, runtime := pendingLearnerRequiredProfileFixture(t)
			e.VerifyRuntimeEvidence = func(context.Context, fencing.RuntimeBinding, fencing.StopRequest, []byte) error { return nil }
			state, err := e.read(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID)
			if err != nil {
				t.Fatal(err)
			}
			run := state.Ledger.Runs[runtime.RunID]
			artifact := pendingLearnerTerminalEvidence{Version: 3, Kind: pendingLearnerTerminalKind, Request: *run.Stop, Runtime: runtime,
				Storage: terminalTestStorage, RuntimeReceipt: []byte("receipt")}
			artifactKey, err := terminalArtifactKey(artifact.reference("pending").ProofName)
			if err != nil {
				t.Fatal(err)
			}
			store, err := e.authorityEvidenceFor(runtime)
			if err != nil {
				t.Fatal(err)
			}
			artifactKey, err = store.objectKey(artifactKey)
			if err != nil {
				t.Fatal(err)
			}
			e.AuthorityState.bucket = &pendingTerminalArtifactReadbackBucket{Bucket: e.AuthorityState.bucket, key: artifactKey, mode: mode}
			if _, err := e.retainIndependentTerminal(t.Context(), runtime, terminalTestStorage, artifact.RuntimeReceipt); err == nil {
				t.Fatal("unreadable or mismatched stored pending artifact was accepted")
			}
			root, err := e.read(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID)
			if err != nil {
				t.Fatal(err)
			}
			if got := root.Ledger.Runs[runtime.RunID].IndependentTerminal; got != nil {
				t.Fatalf("root pinned pending terminal before exact stored artifact readback: %+v", got)
			}
		})
	}
}

func TestPendingLearnerTerminalUsesDurableRuntimeAgentReceipt(t *testing.T) {
	e, f, _, activeRuntime := newMutationStoreFixture(t)
	identity := fencing.Identity{SourceClusterID: activeRuntime.SourceClusterID, NodeID: "pending-terminal", PodName: "pending-terminal", PodUID: "pending-terminal-pod", RunID: "pending-terminal-run"}
	base := activeRuntime
	base.NodeID, base.PodName, base.PodUID, base.RunID = identity.NodeID, identity.PodName, identity.PodUID, identity.RunID
	base.NodeName, base.NodeUID, base.MachineID, base.BootID = "pending-node", "pending-node-uid", "pending-machine", "pending-boot"
	base.RuntimeID, base.SandboxID, base.ContainerID, base.ImageID = "pending-runtime", "pending-sandbox", "pending-container", "pending-image"
	directory := filepath.Join(t.TempDir(), "runtime-agent")
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	client, service, server := pendingRuntimeAgentClient(t, e, directory)
	t.Cleanup(func() { server.Close(); service.Close() })
	f.mu.Lock()
	f.objects["/api/v1/nodes/pending-node"] = object{"status": object{"addresses": []any{object{"type": "InternalIP", "address": "127.0.0.1"}}}}
	f.mu.Unlock()
	runtime, err := client.Enroll(t.Context(), base)
	if err != nil {
		t.Fatalf("actual runtime-agent enrollment failed: %v", err)
	}
	identity.AssignmentID = runtime.AssignmentID
	if _, err := e.change(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID, func(l *fencing.Ledger) error {
		if err := l.RegisterEnrolledWithRole(identity, runtime, fencing.StartupRoleLearnerPendingRemove); err != nil {
			return err
		}
		bound := identity
		bound.WALIdentity = "pending-terminal-wal"
		return l.Bind(bound)
	}); err != nil {
		t.Fatalf("persist runtime-agent-bound pending run: %v", err)
	}
	state, err := e.change(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID, func(l *fencing.Ledger) error {
		return l.CloseNodes("pending-terminal-stop", strings.Repeat("a", 64), []string{identity.NodeID})
	})
	if err != nil {
		t.Fatal(err)
	}
	stop := *state.Ledger.Runs[identity.RunID].Stop
	receipt, err := client.Stop(t.Context(), runtime, stop)
	if err != nil {
		t.Fatalf("actual runtime-agent Stop failed: %v", err)
	}
	if err := client.Verify(t.Context(), runtime, stop, receipt); err != nil {
		t.Fatalf("actual runtime-agent Verify rejected durable receipt: %v", err)
	}
	e.RuntimeAgent = client
	e.CollectRuntimeEvidence = client.Stop
	e.VerifyRuntimeEvidence = client.Verify
	state, err = e.read(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	run := state.Ledger.Runs[identity.RunID]
	artifact := pendingLearnerTerminalEvidence{Version: 3, Kind: pendingLearnerTerminalKind, Request: *run.Stop, Runtime: runtime,
		Storage: terminalTestStorage, RuntimeReceipt: receipt}
	artifactKey := terminalObjectKeyForTest(t, e, runtime, artifact.reference("pending").ProofName)
	rootKey, _, err := e.AuthorityState.rootKey(runtime.StatefulSetUID, runtime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	wrapper := &archiveTestBucket{Bucket: e.AuthorityState.bucket, loseCreate: artifactKey, loseCASResponse: rootKey}
	e.AuthorityState.bucket = wrapper
	ref, err := e.retainIndependentTerminal(t.Context(), runtime, terminalTestStorage, receipt)
	if err != nil || ref.EvidenceVersion != 3 || ref.ProducerUID != "" || ref.MutationReceiptDigest != "" {
		t.Fatalf("pending V3 terminal was not retained without producer authority: ref=%+v err=%v", ref, err)
	}
	if wrapper.loseCreate != "" || wrapper.loseCASResponse != "" || wrapper.failReadOnce != "" || terminalArtifactCount(t, e, runtime) != 1 {
		t.Fatal("pending V3 lost create/CAS response did not resolve to one exact immutable artifact")
	}
	data := terminalArtifactBytes(t, e, runtime, ref)
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 6 || fields["version"] == nil || fields["kind"] == nil || fields["request"] == nil || fields["runtime"] == nil || fields["storage"] == nil || fields["runtimeReceipt"] == nil {
		t.Fatalf("pending V3 artifact has unexpected fields, possibly a producer claim: %v", fields)
	}
	store, err := e.authorityEvidenceFor(runtime)
	if err != nil {
		t.Fatal(err)
	}
	key, err := terminalArtifactKey(ref.ProofName)
	if err != nil {
		t.Fatal(err)
	}
	raw, objectVersion, err := store.Read(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	for name, malformed := range map[string][]byte{
		"unknown producer field": append(append([]byte(nil), bytes.TrimSuffix(data, []byte("}"))...), []byte(`,"producerUID":"forged"}`)...),
		"trailing JSON":          append(append([]byte(nil), data...), []byte(" {}")...),
		"changed kind":           bytes.Replace(data, []byte(pendingLearnerTerminalKind), []byte("active-voter"), 1),
	} {
		t.Run("artifact_"+name, func(t *testing.T) {
			if _, err := decodePendingLearnerTerminalEvidence(malformed, ref, objectVersion, state, run, terminalTestStorage); err == nil {
				t.Fatal("malformed/non-pending artifact accepted")
			}
		})
	}
	for name, mutate := range map[string]func(*pendingLearnerTerminalEvidence){
		"identity pod UID": func(v *pendingLearnerTerminalEvidence) { v.Request.Identity.PodUID = "other-pod" },
		"identity assignment": func(v *pendingLearnerTerminalEvidence) { v.Request.Identity.AssignmentID = "other-assignment" },
		"runtime StatefulSet UID": func(v *pendingLearnerTerminalEvidence) { v.Runtime.StatefulSetUID = "other-sts" },
		"runtime source cluster": func(v *pendingLearnerTerminalEvidence) { v.Runtime.SourceClusterID = "other-source" },
		"storage bucket": func(v *pendingLearnerTerminalEvidence) { v.Storage.Bucket = "other-bucket" },
		"storage prefix": func(v *pendingLearnerTerminalEvidence) { v.Storage.Prefix = "other-prefix" },
		"original stop": func(v *pendingLearnerTerminalEvidence) { v.Request.OperationID = "other-stop" },
	} {
		t.Run("artifact_binding_"+name, func(t *testing.T) {
			candidate := pendingLearnerTerminalEvidence{}
			if err := json.Unmarshal(data, &candidate); err != nil {
				t.Fatal(err)
			}
			mutate(&candidate)
			malformed, err := json.Marshal(candidate)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodePendingLearnerTerminalEvidence(malformed, ref, objectVersion, state, run, terminalTestStorage); err == nil {
				t.Fatal("pending terminal artifact accepted a mismatched identity, runtime, Stop, or storage binding")
			}
		})
	}
	// A later unrelated root transition must not make an exact immutable V3
	// reference unusable. The original node Stop remains the terminal binding.
	if _, err := e.change(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID, func(l *fencing.Ledger) error {
		return l.CloseNodes("unrelated-node-close", strings.Repeat("b", 64), []string{"unrelated-node"})
	}); err != nil {
		t.Fatalf("commit unrelated later root update: %v", err)
	}
	server.Close()
	service.Close()
	restartedClient, restartedService, restartedServer := pendingRuntimeAgentClient(t, e, directory)
	t.Cleanup(func() { restartedServer.Close(); restartedService.Close() })
	restartedStore, err := NewAuthorityStateStore(e.AuthorityState.bucket, e.AuthorityState.prefix)
	if err != nil {
		t.Fatal(err)
	}
	restartedExecutor := &FencingExecutor{Kube: e.Kube, AuthorityState: restartedStore, VerifyRuntimeEvidence: restartedClient.Verify}
	if got, err := restartedExecutor.readIndependentTerminal(t.Context(), runtime, terminalTestStorage, hashJSON([]any{"restart-verify", ref})); err != nil || got != ref {
		t.Fatalf("fresh runtime-agent service did not verify retained exact receipt: got=%+v err=%v", got, err)
	}
	forged := append([]byte(nil), receipt...)
	forged[len(forged)-2] = 'x'
	if err := restartedClient.Verify(t.Context(), runtime, stop, forged); err == nil {
		t.Fatal("runtime-agent verified a different receipt after service restart")
	}
	if _, uid, _, err := restartedStore.ReadProducer(t.Context(), runtime); !errors.Is(err, errAuthorityStateNotFound) || uid != "" {
		t.Fatalf("runtime-agent terminal verification created producer authority: uid=%q err=%v", uid, err)
	}
	records, err := filepath.Glob(filepath.Join(directory, "*.json"))
	if err != nil || len(records) != 1 {
		t.Fatalf("runtime-agent test state does not contain exactly one durable record: count=%d err=%v", len(records), err)
	}
	if err := os.Remove(records[0]); err != nil {
		t.Fatal(err)
	}
	if err := restartedClient.Verify(t.Context(), runtime, stop, receipt); err == nil {
		t.Fatal("runtime-agent verified a receipt after its durable record was deleted")
	}
}
