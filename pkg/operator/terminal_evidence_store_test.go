package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

var terminalTestStorage = AttestationStoreScope{Provider: "gcs", Bucket: "test-only", Prefix: "source"}

func terminalStoreFixture(t *testing.T) (*FencingExecutor, *mutationStoreFixture, fencing.RuntimeBinding) {
	t.Helper()
	e, f, id, runtime := newMutationArchiveFixture(t)
	activateStoredProducer(t, e, id, runtime)
	appendSettledStoreMutation(t, e, runtime, 1)
	e.VerifyRuntimeEvidence = func(_ context.Context, got fencing.RuntimeBinding, request fencing.StopRequest, receipt []byte) error {
		if got != runtime || request.Identity.RunID != runtime.RunID || !bytes.Equal(receipt, []byte("test-provider-terminal")) {
			return errors.New("test runtime verifier rejected evidence")
		}
		return nil
	}
	return e, f, runtime
}

func closeTerminalTestRun(t *testing.T, e *FencingExecutor, runtime fencing.RuntimeBinding) {
	t.Helper()
	if _, err := e.change(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID, func(l *fencing.Ledger) error {
		return l.CloseNodes("voter-fence", strings.Repeat("a", 64), []string{runtime.NodeID})
	}); err != nil {
		t.Fatal(err)
	}
}

func retainTerminalTestRun(t *testing.T, e *FencingExecutor, runtime fencing.RuntimeBinding) fencing.TerminalReference {
	t.Helper()
	ref, err := e.retainIndependentTerminal(t.Context(), runtime, terminalTestStorage, []byte("test-provider-terminal"))
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func TestTerminalStoreRetainsEvidenceAcrossGenerationAndRestart(t *testing.T) {
	e, _, runtime := terminalStoreFixture(t)
	closeTerminalTestRun(t, e, runtime)
	ref := retainTerminalTestRun(t, e, runtime)
	state, err := e.change(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID, func(l *fencing.Ledger) error {
		return l.Close("generation-fence", strings.Repeat("b", 64))
	})
	if err != nil {
		t.Fatal(err)
	}
	// Compaction after retention is allowed; the immutable original snapshot
	// must remain verifiable rather than depending on the former live entries.
	if _, err := e.commitMutationArchive(t.Context(), runtime, storeCompactionPlan(t, e, runtime, 1)); err != nil {
		t.Fatal(err)
	}
	restarted := &FencingExecutor{Kube: e.Kube, AuthorityState: e.AuthorityState, VerifyRuntimeEvidence: e.VerifyRuntimeEvidence}
	evidence, got, err := restarted.readIndependentTerminal(t.Context(), runtime, terminalTestStorage, hashJSON(state))
	if err != nil || got != ref || !bytes.Equal(evidence.RuntimeReceipt, []byte("test-provider-terminal")) {
		t.Fatalf("retained original evidence not reusable: %+v %v", got, err)
	}
	if evidence.Request.OperationID != "voter-fence" || evidence.Producer.LastAdmitted != 1 {
		t.Fatal("generation composition rewrote the original terminal interval")
	}
	run := state.Ledger.Runs[runtime.RunID]
	if run.Exited || run.Drain != nil || state.Ledger.Complete("generation-fence", strings.Repeat("b", 64), nil) {
		t.Fatal("retention manufactured cooperative completion")
	}
}

func TestTerminalStoreLostCreateAndRootCASResponsesAreIdempotent(t *testing.T) {
	e, f, runtime := terminalStoreFixture(t)
	closeTerminalTestRun(t, e, runtime)
	f.mu.Lock()
	f.lostTerminalResponse, f.lostLedgerResponse = true, true
	f.mu.Unlock()
	first := retainTerminalTestRun(t, e, runtime)
	second := retainTerminalTestRun(t, e, runtime)
	if first != second {
		t.Fatal("uncertain commit generated another terminal proof")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	count := 0
	for path := range f.objects {
		if strings.Contains(path, "/rhiza-terminal-") {
			count++
		}
	}
	if count != 1 || f.lostTerminalResponse || f.lostLedgerResponse {
		t.Fatalf("expected both lost responses and exactly one proof, got %d", count)
	}
}

func TestTerminalStoreRejectsIncompleteOrUntrustedTermination(t *testing.T) {
	for _, kind := range []string{"open", "no runtime verifier", "bad runtime receipt", "unresolved mutation"} {
		t.Run(kind, func(t *testing.T) {
			e, f, runtime := terminalStoreFixture(t)
			if kind == "unresolved mutation" {
				_, err := e.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error {
					return p.Admit(2, completeStoreIntent(fencing.MutationIntent{OperationID: "unresolved", Kind: "delete", Key: "key"}))
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			if kind != "open" {
				closeTerminalTestRun(t, e, runtime)
			}
			if kind == "no runtime verifier" {
				e.VerifyRuntimeEvidence = nil
			}
			receipt := []byte("test-provider-terminal")
			if kind == "bad runtime receipt" {
				receipt = []byte("arbitrary digest is not runtime evidence")
			}
			if _, err := e.retainIndependentTerminal(t.Context(), runtime, terminalTestStorage, receipt); err == nil {
				t.Fatal("unproven termination retained")
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			for path := range f.objects {
				if strings.Contains(path, "/rhiza-terminal-") {
					t.Fatal("unverified evidence created an artifact")
				}
			}
		})
	}
}

func TestTerminalStoreRevalidatesRetainedArtifactAndTrust(t *testing.T) {
	for _, kind := range []string{"missing", "UID replaced", "mutable", "deleting", "corrupt", "trailing JSON", "unknown field", "storage", "verifier removed", "verifier rejects"} {
		t.Run(kind, func(t *testing.T) {
			e, f, runtime := terminalStoreFixture(t)
			closeTerminalTestRun(t, e, runtime)
			ref := retainTerminalTestRun(t, e, runtime)
			f.mu.Lock()
			resource := f.objects[e.terminalEvidencePath(ref.ProofName)]
			switch kind {
			case "missing":
				delete(f.objects, e.terminalEvidencePath(ref.ProofName))
			case "UID replaced":
				asObject(resource["metadata"])["uid"] = "replacement"
			case "mutable":
				resource["immutable"] = false
			case "deleting":
				asObject(resource["metadata"])["deletionTimestamp"] = "now"
			case "corrupt":
				asObject(resource["data"])["terminal.json"] = "invalid"
			case "trailing JSON":
				asObject(resource["data"])["terminal.json"] = str(nested(resource, "data", "terminal.json")) + " {}"
			case "unknown field":
				raw := str(nested(resource, "data", "terminal.json"))
				asObject(resource["data"])["terminal.json"] = strings.TrimSuffix(raw, "}") + `,"untrusted":true}`
			}
			f.mu.Unlock()
			storage := terminalTestStorage
			if kind == "storage" {
				storage.Prefix = "other"
			}
			if kind == "verifier removed" {
				e.VerifyRuntimeEvidence = nil
			}
			if kind == "verifier rejects" {
				e.VerifyRuntimeEvidence = func(context.Context, fencing.RuntimeBinding, fencing.StopRequest, []byte) error {
					return errors.New("provider evidence invalid")
				}
			}
			if _, _, err := e.readIndependentTerminal(t.Context(), runtime, storage, strings.Repeat("b", 64)); err == nil {
				t.Fatal("invalid retained evidence authorized historical composition")
			}
		})
	}
}

func TestTerminalStoreBoundedArchiveVerificationSurvivesRestart(t *testing.T) {
	e, f, runtime := terminalStoreFixture(t)
	for sequence := uint64(1); sequence <= 70; sequence++ {
		if sequence > 1 {
			appendSettledStoreMutation(t, e, runtime, sequence)
		}
		if _, err := e.commitMutationArchive(t.Context(), runtime, storeCompactionPlan(t, e, runtime, sequence)); err != nil {
			t.Fatal(err)
		}
	}
	closeTerminalTestRun(t, e, runtime)
	for attempt := 0; attempt < 3; attempt++ {
		e = &FencingExecutor{Kube: e.Kube, AuthorityState: e.AuthorityState, VerifyRuntimeEvidence: e.VerifyRuntimeEvidence}
		_, err := e.retainIndependentTerminal(t.Context(), runtime, terminalTestStorage, []byte("test-provider-terminal"))
		if attempt < 2 && !errors.Is(err, ErrFencePending) || attempt == 2 && err != nil {
			t.Fatalf("bounded verification attempt %d: %v", attempt, err)
		}
	}
	state, err := e.read(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	ref := state.Ledger.Runs[runtime.RunID].IndependentTerminal
	if ref == nil {
		t.Fatal("full archive coverage did not retain proof")
	}
	// A new generation must scan again; completed old-operation cursors cannot
	// hide subsequently observed missing ancestor evidence.
	var artifact terminalEvidence
	f.mu.Lock()
	if err := json.Unmarshal([]byte(str(nested(f.objects[e.terminalEvidencePath(ref.ProofName)], "data", "terminal.json"))), &artifact); err != nil {
		f.mu.Unlock()
		t.Fatal(err)
	}
	f.mu.Unlock()
	headPlan, err := e.readMutationArchive(t.Context(), *artifact.Producer.ArchiveHead, runtime, artifact.ProducerUID, "wal")
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := e.authorityEvidenceFor(runtime)
	if err != nil {
		t.Fatal(err)
	}
	path, err := evidence.objectKey(headPlan.Previous.Name)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.AuthorityState.bucket.Delete(t.Context(), path); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.readIndependentTerminal(t.Context(), runtime, terminalTestStorage, strings.Repeat("f", 64)); !errors.Is(err, errMutationArchiveEvidence) {
		t.Fatalf("new generation reused unverified history: %v", err)
	}
}

func TestTerminalStoreNeverBoundRunPinsProducerBeforeRetention(t *testing.T) {
	e, _, _, runtime := newMutationArchiveFixture(t)
	closeTerminalTestRun(t, e, runtime)
	e.VerifyRuntimeEvidence = func(context.Context, fencing.RuntimeBinding, fencing.StopRequest, []byte) error { return nil }
	ref := retainTerminalTestRun(t, e, runtime)
	if ref.ProducerUID == "" || ref.Request.Identity.WALIdentity != "" {
		t.Fatal("never-bound producer identity was not pinned exactly")
	}
}

func TestTerminalStoreRequiresPreparedMutationReceiptVerification(t *testing.T) {
	e, _, id, runtime := newMutationArchiveFixture(t)
	activateStoredProducer(t, e, id, runtime)
	_, err := e.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error {
		if err := p.Admit(1, completeStoreIntent(fencing.MutationIntent{StoreScopeHash: hashJSON(terminalTestStorage), OperationID: "prepared-delete", Kind: "delete", Key: "key"})); err != nil {
			return err
		}
		if err := p.Prepare(1, fencing.MutationPreparation{Profile: "gcs-json-v1", StoreScopeHash: hashJSON(terminalTestStorage), GenerationSet: true, Generation: 12}); err != nil {
			return err
		}
		hash, err := p.PreparationHash(1)
		if err != nil {
			return err
		}
		return p.Resolve(1, fencing.MutationResolution{Outcome: fencing.MutationExcluded, PreparationHash: hash, ReceiptDigest: strings.Repeat("b", 64)})
	})
	if err != nil {
		t.Fatal(err)
	}
	closeTerminalTestRun(t, e, runtime)
	e.VerifyRuntimeEvidence = func(context.Context, fencing.RuntimeBinding, fencing.StopRequest, []byte) error { return nil }
	if _, err := e.retainIndependentTerminal(t.Context(), runtime, terminalTestStorage, []byte("test-provider-terminal")); err == nil {
		t.Fatal("digest-only prepared mutation qualified terminal retention")
	}
	verified := 0
	e.VerifyMutationEvidence = func(_ context.Context, got fencing.RuntimeBinding, sequence uint64, entry fencing.MutationEntry) error {
		if got != runtime || sequence != 1 || entry.Resolution == nil || entry.Resolution.Outcome != fencing.MutationExcluded {
			return errors.New("test provider rejected mutation evidence")
		}
		verified++
		return nil
	}
	ref := retainTerminalTestRun(t, e, runtime)
	evidence, _, err := e.readIndependentTerminal(t.Context(), runtime, terminalTestStorage, strings.Repeat("c", 64))
	if err != nil || verified == 0 || evidence.Producer.Entries[1].Resolution.Outcome != fencing.MutationExcluded {
		t.Fatalf("verified exclusion was lost or relabelled: %v", err)
	}
	if ref.MutationReceiptDigest != hashJSON(evidence.Producer) {
		t.Fatal("reference did not bind the retained mutation outcomes")
	}
	// The current executor must still have the verifier after restart.
	restarted := &FencingExecutor{Kube: e.Kube, AuthorityState: e.AuthorityState, VerifyRuntimeEvidence: e.VerifyRuntimeEvidence}
	if _, _, err := restarted.readIndependentTerminal(t.Context(), runtime, terminalTestStorage, strings.Repeat("d", 64)); err == nil {
		t.Fatal("restart accepted prepared work without its receipt verifier")
	}
}

func TestTerminalStoreRejectsOtherStorePreparedLiveOrArchived(t *testing.T) {
	for _, archived := range []bool{false, true} {
		t.Run(map[bool]string{false: "live", true: "archived"}[archived], func(t *testing.T) {
			e, _, id, runtime := newMutationArchiveFixture(t)
			activateStoredProducer(t, e, id, runtime)
			run, err := e.enrolledMutationRun(t.Context(), runtime)
			if err != nil {
				t.Fatal(err)
			}
			_, err = e.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error {
				if err := p.Admit(1, completeStoreIntent(fencing.MutationIntent{StoreScopeHash: strings.Repeat("f", 64), OperationID: fencing.MutationRequestID(run.MutationProducerUID, 1), Kind: "delete", Key: "key"})); err != nil {
					return err
				}
				if err := p.Prepare(1, fencing.MutationPreparation{Profile: "gcs-json-v1", StoreScopeHash: strings.Repeat("f", 64), GenerationSet: true, Generation: 12}); err != nil {
					return err
				}
				hash, err := p.PreparationHash(1)
				if err != nil {
					return err
				}
				return p.Resolve(1, fencing.MutationResolution{Outcome: fencing.MutationExcluded, PreparationHash: hash, ReceiptDigest: strings.Repeat("b", 64)})
			})
			if err != nil {
				t.Fatal(err)
			}
			e.VerifyMutationEvidence = func(context.Context, fencing.RuntimeBinding, uint64, fencing.MutationEntry) error { return nil }
			e.VerifyRuntimeEvidence = func(context.Context, fencing.RuntimeBinding, fencing.StopRequest, []byte) error { return nil }
			if archived {
				if _, err := e.commitMutationArchive(t.Context(), runtime, storeCompactionPlan(t, e, runtime, 1)); err != nil {
					t.Fatal(err)
				}
			}
			closeTerminalTestRun(t, e, runtime)
			if _, err := e.retainIndependentTerminal(t.Context(), runtime, terminalTestStorage, []byte("test-provider-terminal")); err == nil || !strings.Contains(err.Error(), "another storage scope") {
				t.Fatalf("other-store descriptor was not rejected: %v", err)
			}
		})
	}
}

func TestTerminalStoreRejectsOversizedRuntimeReceiptBeforeArtifactCreation(t *testing.T) {
	e, f, runtime := terminalStoreFixture(t)
	closeTerminalTestRun(t, e, runtime)
	e.VerifyRuntimeEvidence = func(context.Context, fencing.RuntimeBinding, fencing.StopRequest, []byte) error {
		t.Fatal("unbounded receipt reached provider verifier")
		return nil
	}
	if _, err := e.retainIndependentTerminal(t.Context(), runtime, terminalTestStorage, bytes.Repeat([]byte("x"), 65537)); err == nil {
		t.Fatal("oversized receipt accepted")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for path := range f.objects {
		if strings.Contains(path, "/rhiza-terminal-") {
			t.Fatal("oversized receipt was persisted")
		}
	}
}
