package operator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

func newIndependentV2Fixture(t *testing.T) (*FencingExecutor, *mutationStoreFixture, *fenceResource, fencing.RuntimeBinding) {
	t.Helper()
	e, f, runtime := terminalStoreFixture(t)
	request := FenceRequest{LogicalID: "logical", Namespace: "test", BindingUID: "binding", StatefulSet: "rhiza", StatefulSetUID: runtime.StatefulSetUID,
		SourceClusterID: runtime.SourceClusterID, OperationID: "voter-fence", Scope: ScopeVoter,
		Targets: []FenceTarget{{NodeID: runtime.NodeID, Pod: runtime.NodeID, PodUID: runtime.PodUID, WALIdentity: "wal", NodeName: runtime.NodeName, NodeUID: runtime.NodeUID, ContainerID: runtime.ContainerID, ImageID: runtime.ImageID}}}
	sts := independentStatefulSet("gcs")
	asObject(sts["metadata"])["uid"] = runtime.StatefulSetUID
	ctr := asObject(list(nested(sts, "spec", "template", "spec", "containers"))[0])
	ctr["env"] = []any{object{"name": "RHIZA_OBJSTORE_PROVIDER", "value": "gcs"}, object{"name": "RHIZA_OBJSTORE_BUCKET", "value": "test-only"}}
	f.mu.Lock()
	f.objects[e.controller().stsPath("rhiza")] = sts
	f.objects[e.controller().corePath("pods", "")] = object{"items": []any{}}
	f.mu.Unlock()
	e.CollectRuntimeEvidence = func(_ context.Context, got fencing.RuntimeBinding, request fencing.StopRequest) ([]byte, error) {
		if got != runtime || request.Identity.RunID != runtime.RunID {
			return nil, errors.New("test collector target differs")
		}
		return []byte("test-provider-terminal"), nil
	}
	record := &fenceResource{Metadata: object{"name": "v2-fence", "namespace": "test"}, Spec: request}
	return e, f, record, runtime
}

func v2FenceEndpoint() string {
	return "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizafences"
}

func TestIndependentV2ReconcileWritesVerifiedProof(t *testing.T) {
	e, f, record, runtime := newIndependentV2Fixture(t)
	if err := e.reconcileFence(t.Context(), v2FenceEndpoint(), record); err != nil {
		t.Fatal(err)
	}
	proof := record.Status.Proof
	if proof == nil || proof.Independent != nil || proof.IndependentV2 == nil || len(proof.IndependentV2.Terminals) != 1 {
		t.Fatal("reconcile did not persist actual verified v2 coverage")
	}
	ref := proof.IndependentV2.Terminals[0]
	if ref.Request.Identity.RunID != runtime.RunID || ref.StorageScopeDigest != hashJSON(terminalTestStorage) {
		t.Fatal("proof lost its exact run/store binding")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.statusWrites != 1 {
		t.Fatalf("status writes=%d", f.statusWrites)
	}
	var persisted fenceResource
	if err := json.Unmarshal(jsonBytes(f.objects[v2FenceEndpoint()+"/v2-fence/status"]), &persisted); err != nil || persisted.Status.Proof == nil || persisted.Status.Proof.ProofID != proof.ProofID {
		t.Fatalf("persisted proof differs: %v", err)
	}
}

func exerciseIndependentVoterThenGeneration(t *testing.T) {
	t.Helper()
	e, f, record, runtime := newIndependentV2Fixture(t)
	// Enroll another voter before closure. It remains active during the first
	// voter fence and becomes the only captured target of generation recovery.
	id := fencing.Identity{SourceClusterID: runtime.SourceClusterID, NodeID: "n1", PodUID: "pod-1", RunID: "run-1"}
	other := runtime
	other.NodeID, other.PodUID, other.RunID, other.ContainerID = id.NodeID, id.PodUID, id.RunID, "container-1"
	if _, err := e.change(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID, func(l *fencing.Ledger) error { return l.RegisterEnrolled(id, other) }); err != nil {
		t.Fatal(err)
	}
	activateStoredProducer(t, e, id, other)
	if _, err := e.changeMutationProducer(t.Context(), other, func(p *fencing.MutationProducer) error {
		return p.Admit(1, fencing.MutationIntent{OperationID: "survivor-inflight", Kind: "delete", Key: "survivor"})
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.reconcileFence(t.Context(), v2FenceEndpoint(), record); err != nil {
		t.Fatal(err)
	}
	first := record.Status.Proof.IndependentV2.Terminals[0]
	// Admission on the survivor remains open after the other voter is fenced;
	// no generation-wide storage silence was required for the first proof.
	if _, err := e.changeMutationProducer(t.Context(), other, func(p *fencing.MutationProducer) error {
		if err := p.Admit(2, fencing.MutationIntent{OperationID: "survivor-after-fence", Kind: "delete", Key: "another"}); err != nil {
			return err
		}
		for _, sequence := range []uint64{1, 2} {
			if err := p.CancelUnprepared(sequence, strings.Repeat("a", 64)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal("survivor admission was incorrectly closed:", err)
	}
	collectedRetired := 0
	collector := func(_ context.Context, got fencing.RuntimeBinding, request fencing.StopRequest) ([]byte, error) {
		if got == runtime {
			collectedRetired++
			return nil, errors.New("retired runtime no longer reachable")
		}
		if got != other || request.Identity.RunID != other.RunID {
			return nil, errors.New("unknown runtime")
		}
		return []byte("test-provider-terminal"), nil
	}
	verifier := func(_ context.Context, got fencing.RuntimeBinding, request fencing.StopRequest, receipt []byte) error {
		if got != runtime && got != other || request.Identity.RunID != got.RunID || string(receipt) != "test-provider-terminal" {
			return errors.New("test runtime receipt differs")
		}
		return nil
	}
	restarted := &FencingExecutor{Kube: e.Kube, CollectRuntimeEvidence: collector, VerifyRuntimeEvidence: verifier}
	next := record.Spec
	next.Scope, next.OperationID = ScopeGeneration, "next-generation"
	next.Targets = []FenceTarget{{NodeID: other.NodeID, Pod: other.NodeID, PodUID: other.PodUID, WALIdentity: "wal", NodeName: other.NodeName, NodeUID: other.NodeUID, ContainerID: other.ContainerID, ImageID: other.ImageID}}
	record = &fenceResource{Metadata: object{"name": "generation", "namespace": "test"}, Spec: next}
	if err := restarted.reconcileFence(t.Context(), v2FenceEndpoint(), record); err != nil {
		t.Fatal("retired voter evidence could not compose:", err)
	}
	refs := record.Status.Proof.IndependentV2.Terminals
	if len(refs) != 2 || refs[0] != first || refs[1].Request.Identity.RunID != other.RunID || collectedRetired != 0 {
		t.Fatalf("history was skipped, recollected or rewritten: %+v", refs)
	}
	state, err := restarted.read(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if state.Ledger.Runs[runtime.RunID].Exited || state.Ledger.Runs[runtime.RunID].Drain != nil {
		t.Fatal("historical composition fabricated a cooperative drain")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.statusWrites != 2 {
		t.Fatalf("expected independently verified voter and generation proofs, got %d", f.statusWrites)
	}
}

func TestIndependentV2MissingHistoricalEnrollmentBlocksWithoutCollecting(t *testing.T) {
	e, f, record, runtime := newIndependentV2Fixture(t)
	if _, err := e.change(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID, func(l *fencing.Ledger) error {
		l.Runs["old"] = fencing.Run{Identity: fencing.Identity{SourceClusterID: runtime.SourceClusterID, NodeID: runtime.NodeID, PodUID: runtime.PodUID, RunID: "old", WALIdentity: "old-wal"}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	e.CollectRuntimeEvidence = func(context.Context, fencing.RuntimeBinding, fencing.StopRequest) ([]byte, error) {
		t.Fatal("collector was invoked for an incomplete runtime cohort")
		return nil, nil
	}
	if err := e.reconcileFence(t.Context(), v2FenceEndpoint(), record); !errors.Is(err, ErrFencePending) {
		t.Fatalf("missing historical runtime did not block: %v", err)
	}
	if record.Status.Proof != nil || f.statusWrites != 0 {
		t.Fatal("latest runtime was accepted for unknown historical work")
	}
}

func TestIndependentV2ProofRejectsChangedReferenceAndLegacyFallback(t *testing.T) {
	e, _, record, _ := newIndependentV2Fixture(t)
	if err := e.reconcileFence(t.Context(), v2FenceEndpoint(), record); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*FenceProof){
		"legacy":              func(p *FenceProof) { p.Independent = &IndependentEvidence{Version: 1} },
		"unsupported version": func(p *FenceProof) { p.IndependentV2.Version = 1 },
		"foreign request":     func(p *FenceProof) { p.IndependentV2.RequestHash = strings.Repeat("f", 64) },
		"missing coverage":    func(p *FenceProof) { p.IndependentV2.Terminals = nil },
		"duplicate run": func(p *FenceProof) {
			p.IndependentV2.Terminals = append(p.IndependentV2.Terminals, p.IndependentV2.Terminals[0])
		},
		"foreign source":             func(p *FenceProof) { p.IndependentV2.Terminals[0].Request.Identity.SourceClusterID = "other" },
		"foreign storage":            func(p *FenceProof) { p.IndependentV2.StorageScope.Prefix = "other" },
		"missing runtime digest":     func(p *FenceProof) { p.IndependentV2.Terminals[0].RuntimeHash = "" },
		"missing receipt digest":     func(p *FenceProof) { p.IndependentV2.Terminals[0].MutationReceiptDigest = "" },
		"changed well-formed digest": func(p *FenceProof) { p.IndependentV2.Terminals[0].RuntimeHash = strings.Repeat("f", 64) },
		"pruned v2 evidence":         func(p *FenceProof) { p.IndependentV2 = nil },
	} {
		t.Run(name, func(t *testing.T) {
			var copy FenceProof
			if err := json.Unmarshal(jsonBytes(record.Status.Proof), &copy); err != nil {
				t.Fatal(err)
			}
			mutate(&copy)
			if validateFenceProof(record.Spec, &copy) == nil {
				t.Fatal("altered proof accepted")
			}
			// Check semantic bindings too, not merely the top-level content hash.
			// A well-formed runtime digest requires the authority's actual immutable
			// artifact verifier; the controller cannot authenticate it from syntax.
			if name != "changed well-formed digest" {
				copy.ProofID = "independent-v2-" + hashJSON(copy.IndependentV2)
				if validateFenceProof(record.Spec, &copy) == nil {
					t.Fatal("self-consistent but semantically invalid proof accepted")
				}
			}
		})
	}
}

func TestIndependentV2AccountsForEachSamePodRuntimeAttempt(t *testing.T) {
	e, _, record, runtime := newIndependentV2Fixture(t)
	old := runtime
	old.RunID, old.ContainerID = "old-run", "old-container"
	id := fencing.Identity{SourceClusterID: old.SourceClusterID, NodeID: old.NodeID, PodUID: old.PodUID, RunID: old.RunID}
	if _, err := e.change(t.Context(), old.StatefulSetUID, old.SourceClusterID, func(l *fencing.Ledger) error { return l.RegisterEnrolled(id, old) }); err != nil {
		t.Fatal(err)
	}
	activateStoredProducer(t, e, id, old)
	collected := map[string]bool{}
	e.CollectRuntimeEvidence = func(_ context.Context, got fencing.RuntimeBinding, request fencing.StopRequest) ([]byte, error) {
		if got != runtime && got != old || request.Identity.RunID != got.RunID {
			return nil, errors.New("wrong incarnation")
		}
		collected[got.ContainerID] = true
		return []byte(got.ContainerID), nil
	}
	e.VerifyRuntimeEvidence = func(_ context.Context, got fencing.RuntimeBinding, request fencing.StopRequest, receipt []byte) error {
		if got != runtime && got != old || request.Identity.RunID != got.RunID || string(receipt) != got.ContainerID {
			return errors.New("one container cannot stand in for another")
		}
		return nil
	}
	if err := e.reconcileFence(t.Context(), v2FenceEndpoint(), record); err != nil {
		t.Fatal(err)
	}
	refs := record.Status.Proof.IndependentV2.Terminals
	if len(refs) != 2 || !collected[runtime.ContainerID] || !collected[old.ContainerID] || refs[0].RuntimeHash == refs[1].RuntimeHash {
		t.Fatal("same-Pod historical runtime attempts were collapsed")
	}
}

func TestIndependentV2InventoryChangeDuringCollectionDoesNotPublish(t *testing.T) {
	e, f, record, runtime := newIndependentV2Fixture(t)
	collect := e.CollectRuntimeEvidence
	e.CollectRuntimeEvidence = func(ctx context.Context, got fencing.RuntimeBinding, request fencing.StopRequest) ([]byte, error) {
		pod := independentPod(runtime.NodeID, "new-pod")
		asObject(list(nested(pod, "metadata", "ownerReferences"))[0])["uid"] = runtime.StatefulSetUID
		f.mu.Lock()
		f.objects[e.controller().corePath("pods", "")] = object{"items": []any{pod}}
		f.mu.Unlock()
		return collect(ctx, got, request)
	}
	if err := e.reconcileFence(t.Context(), v2FenceEndpoint(), record); !errors.Is(err, ErrFencePending) {
		t.Fatalf("changed inventory was not rejected: %v", err)
	}
	if record.Status.Proof != nil || f.statusWrites != 0 {
		t.Fatal("stale-inventory proof was published")
	}
}

func TestIndependentV2CannotPublishWithUnresolvedMutation(t *testing.T) {
	e, f, record, runtime := newIndependentV2Fixture(t)
	if _, err := e.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error {
		return p.Admit(2, fencing.MutationIntent{OperationID: "unknown", Kind: "delete", Key: "pending"})
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.reconcileFence(t.Context(), v2FenceEndpoint(), record); err == nil {
		t.Fatal("unresolved admitted work was silently omitted")
	}
	if record.Status.Proof != nil || f.statusWrites != 0 {
		t.Fatal("runtime termination alone authorized storage recovery")
	}
}

func TestIndependentV2CooperativeDrainCannotBypassAuthorityMutations(t *testing.T) {
	e, f, record, runtime := newIndependentV2Fixture(t)
	if _, err := e.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error {
		return p.Admit(2, fencing.MutationIntent{OperationID: "authority-still-pending", Kind: "delete", Key: "pending"})
	}); err != nil {
		t.Fatal(err)
	}
	hash, err := requestHash(&record.Spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.change(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID, func(l *fencing.Ledger) error {
		if err := l.CloseNodes(record.Spec.OperationID, hash, []string{runtime.NodeID}); err != nil {
			return err
		}
		run := l.Runs[runtime.RunID]
		run.Exited = true
		run.Drain = &fencing.DrainReceipt{Request: *run.Stop, WritersJoined: true}
		l.Runs[runtime.RunID] = run
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.reconcileFence(t.Context(), v2FenceEndpoint(), record); !errors.Is(err, ErrFencePending) {
		t.Fatalf("cooperative exit bypassed unresolved authority work: %v", err)
	}
	if record.Status.Proof != nil || f.statusWrites != 0 {
		t.Fatal("cooperative path fabricated settled storage")
	}
	if _, err := e.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error {
		return p.CancelUnprepared(2, strings.Repeat("c", 64))
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.reconcileFence(t.Context(), v2FenceEndpoint(), record); err != nil || record.Status.Proof == nil || !strings.HasPrefix(record.Status.Proof.ProofID, "cooperative-") {
		t.Fatalf("actually settled cooperative exit did not complete: %v", err)
	}
}

// This is a bounded wire-capacity specimen, not 64 runtime/provider executions.
func TestIndependentV2FullReferenceTransportDoesNotTruncate(t *testing.T) {
	e, _, record, _ := newIndependentV2Fixture(t)
	if err := e.reconcileFence(t.Context(), v2FenceEndpoint(), record); err != nil {
		t.Fatal(err)
	}
	request := record.Spec
	request.Scope = ScopeGeneration
	hash, err := requestHash(&request)
	if err != nil {
		t.Fatal(err)
	}
	proof := record.Status.Proof
	proof.RequestHash, proof.IndependentV2.RequestHash = hash, hash
	first := proof.IndependentV2.Terminals[0]
	proof.IndependentV2.Terminals = nil
	for i := 0; i < fenceTargetCapacity; i++ {
		ref := first
		ref.Request.Identity.RunID = strings.Repeat("r", 120) + fmt.Sprintf("%03d", i)
		ref.ProducerUID, ref.ProofUID = strings.Repeat("u", 128), strings.Repeat("p", 128)
		ref.Request.OperationID = strings.Repeat("o", 256)
		if i != 0 {
			ref.Request.Identity.NodeID, ref.Request.Identity.PodUID = strings.Repeat("n", 256), strings.Repeat("p", 256)
			ref.Request.Identity.WALIdentity = strings.Repeat("w", 128)
		}
		proof.IndependentV2.Terminals = append(proof.IndependentV2.Terminals, ref)
	}
	proof.ProofID = "independent-v2-" + hashJSON(proof.IndependentV2)
	raw := jsonBytes(proof)
	if len(raw) <= 64*1024 || len(raw) >= maxResponseBytes {
		t.Fatalf("specimen does not exercise the formerly truncating boundary: %d", len(raw))
	}
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(raw)
	}))
	t.Cleanup(api.Close)
	client := &FencingClient{URL: api.URL, TokenFile: writeToken(t), Client: api.Client()}
	got, err := client.Fence(t.Context(), request)
	if err != nil || got == nil || got.IndependentV2 == nil || len(got.IndependentV2.Terminals) != fenceTargetCapacity {
		t.Fatalf("complete supported v2 proof was truncated: %v", err)
	}
}
