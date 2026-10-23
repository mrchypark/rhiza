package operator

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

func TestIndependentVoterProofCoversCompleteTargetNodeHistory(t *testing.T) {
	e, _, record, current := newIndependentV2Fixture(t)
	previous := current
	previous.RunID = "previous-run"
	previous.ContainerID = "previous-container"
	previousIdentity := fencing.Identity{
		SourceClusterID: previous.SourceClusterID,
		NodeID:          previous.NodeID,
		PodUID:          previous.PodUID,
		RunID:           previous.RunID,
	}
	if _, err := e.change(t.Context(), current.StatefulSetUID, current.SourceClusterID, func(ledger *fencing.Ledger) error {
		return ledger.RegisterEnrolled(previousIdentity, previous)
	}); err != nil {
		t.Fatal(err)
	}
	activateStoredProducer(t, e, previousIdentity, previous)

	runtimes := map[string]fencing.RuntimeBinding{
		current.RunID:  current,
		previous.RunID: previous,
	}
	e.CollectRuntimeEvidence = func(_ context.Context, runtime fencing.RuntimeBinding, request fencing.StopRequest) ([]byte, error) {
		if runtimes[runtime.RunID] != runtime || request.Identity.RunID != runtime.RunID || runtime.Validate(request.Identity) != nil {
			return nil, errors.New("test collector target differs")
		}
		return []byte("test-provider-terminal"), nil
	}
	e.VerifyRuntimeEvidence = func(_ context.Context, runtime fencing.RuntimeBinding, request fencing.StopRequest, receipt []byte) error {
		if runtimes[runtime.RunID] != runtime || request.Identity.RunID != runtime.RunID || runtime.Validate(request.Identity) != nil || string(receipt) != "test-provider-terminal" {
			return errors.New("test runtime verifier rejected evidence")
		}
		return nil
	}
	if err := e.reconcileFence(t.Context(), v2FenceEndpoint(), record); err != nil {
		t.Fatal(err)
	}
	proof := record.Status.Proof
	if proof == nil || proof.IndependentV2 == nil || len(proof.IndependentV2.Terminals) != 2 {
		t.Fatalf("voter proof omitted a same-slot historical run: %+v", proof)
	}
	state, err := e.read(t.Context(), current.StatefulSetUID, current.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	target := record.Spec.Targets[0]
	if err := e.validateCanonicalLearnerFence(t.Context(), record.Spec, proof, target); err != nil {
		t.Fatalf("valid complete voter-scoped history was rejected: %v", err)
	}
	if err := verifyIndependentVoterProofCoverage(state, record.Spec, proof.IndependentV2); err != nil {
		t.Fatalf("valid voter-scoped coverage was rejected: %v", err)
	}

	cloneProof := func() FenceProof {
		t.Helper()
		var cloned FenceProof
		if err := json.Unmarshal(jsonBytes(proof), &cloned); err != nil {
			t.Fatal(err)
		}
		return cloned
	}
	for name, mutate := range map[string]func(*IndependentEvidenceV2){
		"omitted historical terminal": func(evidence *IndependentEvidenceV2) {
			evidence.Terminals = evidence.Terminals[:1]
		},
		"extra terminal identity": func(evidence *IndependentEvidenceV2) {
			extra := evidence.Terminals[0]
			extra.Request.Identity.RunID = "unregistered-run"
			extra.Request.OperationID = "unregistered-operation"
			evidence.Terminals = append(evidence.Terminals, extra)
		},
		"mismatched terminal identity": func(evidence *IndependentEvidenceV2) {
			evidence.Terminals[0].Request.Identity.PodUID = "different-pod"
		},
		"cross-node terminal": func(evidence *IndependentEvidenceV2) {
			evidence.Terminals[0].Request.Identity.NodeID = "another-voter"
		},
	} {
		t.Run(name, func(t *testing.T) {
			cloned := cloneProof()
			mutate(cloned.IndependentV2)
			if err := e.validateCanonicalLearnerFence(t.Context(), record.Spec, &cloned, target); err == nil {
				t.Fatal("noncanonical voter terminal history was accepted")
			}
		})
	}
}

func TestIndependentGenerationProofCoverageRemainsGenerationWide(t *testing.T) {
	e, _, record, runtime := newIndependentV2Fixture(t)
	previous := runtime
	previous.RunID = "previous-generation-run"
	previous.ContainerID = "previous-generation-container"
	previousIdentity := fencing.Identity{
		SourceClusterID: previous.SourceClusterID,
		NodeID:          previous.NodeID,
		PodUID:          previous.PodUID,
		RunID:           previous.RunID,
	}
	if _, err := e.change(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID, func(ledger *fencing.Ledger) error {
		return ledger.RegisterEnrolled(previousIdentity, previous)
	}); err != nil {
		t.Fatal(err)
	}
	activateStoredProducer(t, e, previousIdentity, previous)
	runtimes := map[string]fencing.RuntimeBinding{runtime.RunID: runtime, previous.RunID: previous}
	e.CollectRuntimeEvidence = func(_ context.Context, got fencing.RuntimeBinding, request fencing.StopRequest) ([]byte, error) {
		if runtimes[got.RunID] != got || request.Identity.RunID != got.RunID || got.Validate(request.Identity) != nil {
			return nil, errors.New("test collector target differs")
		}
		return []byte("test-provider-terminal"), nil
	}
	e.VerifyRuntimeEvidence = func(_ context.Context, got fencing.RuntimeBinding, request fencing.StopRequest, receipt []byte) error {
		if runtimes[got.RunID] != got || request.Identity.RunID != got.RunID || got.Validate(request.Identity) != nil || string(receipt) != "test-provider-terminal" {
			return errors.New("test runtime verifier rejected evidence")
		}
		return nil
	}
	record.Spec.Scope = ScopeGeneration
	record.Spec.OperationID = "generation-fence"
	if err := e.reconcileFence(t.Context(), v2FenceEndpoint(), record); err != nil {
		t.Fatal(err)
	}
	state, err := e.read(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyIndependentProofCoverage(state, record.Spec, record.Status.Proof.IndependentV2); err != nil {
		t.Fatalf("generation-wide coverage changed: %v", err)
	}
	if len(record.Status.Proof.IndependentV2.Terminals) != 2 {
		t.Fatalf("generation proof did not retain complete same-slot run history: %+v", record.Status.Proof.IndependentV2.Terminals)
	}
	if err := verifyIndependentVoterProofCoverage(state, record.Spec, record.Status.Proof.IndependentV2); err == nil {
		t.Fatal("voter-scoped verifier accepted a generation request")
	}
}
