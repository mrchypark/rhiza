package operator

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

// TestGenerationZeroRetirementRequiresOriginalDispatchReceipt keeps helper
// terminal receipts separate from the original upload dispatch. The journal
// accepts digest-shaped terminal fields, so retirement must independently
// authenticate the original dispatch receipt before adopting a proof.
func TestGenerationZeroRetirementRequiresOriginalDispatchReceipt(t *testing.T) {
	for _, tc := range []struct {
		name      string
		useHelper bool
	}{
		{name: "helper receipt cannot stand in for original dispatch", useHelper: true},
		{name: "missing original receipt cannot authorize retirement"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newGenerationZeroReceiptFixture(t)
			payload := []byte(`{"membership":"generation-zero receipt binding"}`)
			intent := f.admitUpload(t, 1, "membership/g0-receipt-binding", fencing.MutationCondition{Mode: fencing.MutationUnconditional}, payload)
			preparation := fencing.MutationPreparation{Profile: fencing.MutationHeldExecutionProfile, StoreScopeHash: intent.StoreScopeHash,
				GenerationSet: true, Generation: 0}
			run, err := f.e.enrolledMutationRun(t.Context(), f.runtime)
			if err != nil {
				t.Fatal(err)
			}
			owner := hashJSON([]any{f.runtime, run.Identity.WALIdentity, f.producer, uint64(1), intent.OperationID})
			if err := f.e.acquireMutationKey(t.Context(), intent.StoreScopeHash, intent.Key, owner, true); err != nil {
				t.Fatalf("acquire original operation key: %v", err)
			}
			if _, err := f.e.changeMutationProducer(t.Context(), f.runtime, func(p *fencing.MutationProducer) error {
				if err := p.Prepare(1, preparation); err != nil {
					return err
				}
				if err := p.GrantDispatch(1, "original-zero-dispatch"); err != nil {
					return err
				}
				if err := p.SealDispatch(1); err != nil {
					return err
				}
				// Leave the logical resolution unset. The trusted helper will
				// establish the unresolved commit from its exact storage guard;
				// no original dispatch receipt is recorded here.
				return p.Close("close-g0-receipt-binding", hashBytes([]byte("close-g0-receipt-binding")))
			}); err != nil {
				t.Fatalf("prepare closed unresolved original dispatch: %v", err)
			}

			server, client := newHeldGCSServer(t, f)
			server.mu.Lock()
			server.nextGeneration = 0
			server.mu.Unlock()
			f.e.SourceMutationStorage = client
			originalProvider, err := newGCSMutationProviderForProfile(client, f.scope, fencing.MutationHeldExecutionProfile)
			if err != nil {
				t.Fatal(err)
			}
			// The original create physically succeeds but its response is lost.
			// Do not persist a receipt: the helper's later 412/readback receipt
			// must not be adoptable as the original dispatch receipt.
			if _, err := originalProvider.uploadHeldCreate(t.Context(), intent.Key, intent, preparation, bytes.NewReader(payload)); err == nil {
				t.Fatal("fixture original create unexpectedly returned a trusted response")
			}
			server.mu.Lock()
			origin, exists := server.objects[pathJoin(f.scope.Prefix, intent.Key)]
			server.mu.Unlock()
			if !exists || !origin.temporaryHold || !bytes.Equal(origin.payload, payload) {
				t.Fatalf("lost-response fixture did not preserve the physical held create: exists=%t object=%+v", exists, origin)
			}

			repair := testEngineRepairRecord(t, f)
			guard, err := f.e.settleEngineRepairZeroGeneration(t.Context(), repair, f.runtime, f.producer, 1, f.scope)
			if err != nil {
				t.Fatalf("install generation-zero barrier and settle helper rights: %v", err)
			}
			producer, uid, _, err := f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
			if err != nil || uid != f.producer {
				t.Fatalf("read settled producer: uid=%q err=%v", uid, err)
			}
			entry := producer.Entries[1]
			if entry.GenerationZeroRights == nil || len(entry.GenerationZeroRights.Rights) == 0 {
				t.Fatal("fixture did not record a generation-zero helper receipt")
			}
			helperDigest := entry.GenerationZeroRights.Rights[0].TerminalReceiptDigest
			if !mutationDigestValidOperator(helperDigest) {
				t.Fatalf("fixture helper receipt is not a valid terminal digest: %q", helperDigest)
			}

			originalReceipt := mutationExecutionReceipt{Runtime: f.runtime, WALIdentity: run.Identity.WALIdentity, ProducerUID: f.producer,
				Sequence: 1, Intent: intent, Preparation: preparation, DispatchID: guard.DispatchID}
			if _, err := f.receipts.get(mutationExecutionReceiptName(originalReceipt)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("fixture unexpectedly has an original dispatch receipt: err=%v", err)
			}

			forgedDigest := hashBytes([]byte("unrelated-original-dispatch-receipt"))
			if tc.useHelper {
				forgedDigest = helperDigest
			}
			completion := fencing.MutationCopyCompletion{PreparationHash: guard.PreparationHash, ReceiptDigest: forgedDigest,
				GenerationZeroRightsDigest: entry.GenerationZeroRights.Digest()}
			if _, err := f.e.changeMutationProducer(t.Context(), f.runtime, func(p *fencing.MutationProducer) error {
				if err := p.RecordDispatchTerminal(1, guard.DispatchID, forgedDigest); err != nil {
					return err
				}
				return p.ConfirmCopiesTerminal(1, completion)
			}); err != nil {
				t.Fatalf("record forged-shaped original terminal fields: %v", err)
			}
			producer, uid, _, err = f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
			if err != nil || uid != f.producer {
				t.Fatalf("read forged terminal journal: uid=%q err=%v", uid, err)
			}
			entry = producer.Entries[1]

			retired, retireErr := f.e.retireGenerationZeroBarrier(t.Context(), f.runtime, f.producer, 1, entry)
			if retireErr == nil || retired {
				t.Fatalf("retirement accepted %s: retired=%t err=%v", tc.name, retired, retireErr)
			}
			state, err := f.e.readMutationKeyState(t.Context(), intent.StoreScopeHash, intent.Key)
			if err != nil || state.Barrier == nil || state.Retirement != nil {
				t.Fatalf("rejected receipt did not leave the generation-zero barrier pinned: state=%+v err=%v", state, err)
			}
			server.mu.Lock()
			origin, exists = server.objects[pathJoin(f.scope.Prefix, intent.Key)]
			server.mu.Unlock()
			if !exists || !origin.temporaryHold {
				t.Fatalf("rejected receipt released or lost the origin hold: exists=%t object=%+v", exists, origin)
			}
			evidence, err := f.e.authorityEvidenceFor(f.runtime)
			if err != nil {
				t.Fatalf("open root-bound proof store: %v", err)
			}
			proofName := (mutationGenerationZeroRetirementProof{Guard: guard}).ref()
			reader, _, err := evidence.Read(t.Context(), proofName)
			if err == nil {
				_ = reader.Close()
				t.Fatalf("rejected receipt was adopted into a retirement proof at %q", proofName)
			} else if !evidence.IsNotFound(err) {
				t.Fatalf("retirement proof lookup failed for a reason other than absence: %v", err)
			}
		})
	}
}

func TestGenerationZeroOriginal412HelperCommitAndArchivedLateReceipt(t *testing.T) {
	f := newGenerationZeroReceiptFixture(t)
	payload := []byte(`{"membership":"original 412 then helper logical commit"}`)
	intent := f.admitUpload(t, 1, "membership/g0-original-412", fencing.MutationCondition{Mode: fencing.MutationUnconditional}, payload)
	preparation := fencing.MutationPreparation{Profile: fencing.MutationHeldExecutionProfile, StoreScopeHash: intent.StoreScopeHash,
		GenerationSet: true, Generation: 0}
	run, err := f.e.enrolledMutationRun(t.Context(), f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	owner := hashJSON([]any{f.runtime, run.Identity.WALIdentity, f.producer, uint64(1), intent.OperationID})
	if err := f.e.acquireMutationKey(t.Context(), intent.StoreScopeHash, intent.Key, owner, true); err != nil {
		t.Fatal(err)
	}
	if _, err := f.e.changeMutationProducer(t.Context(), f.runtime, func(p *fencing.MutationProducer) error {
		if err := p.Prepare(1, preparation); err != nil {
			return err
		}
		if err := p.GrantDispatch(1, "original-zero-412-dispatch"); err != nil {
			return err
		}
		if err := p.SealDispatch(1); err != nil {
			return err
		}
		return p.Close("close-original-zero-412", hashBytes([]byte("close-original-zero-412")))
	}); err != nil {
		t.Fatalf("prepare original held create: %v", err)
	}

	server, client := newHeldGCSServer(t, f)
	server.mu.Lock()
	server.nextGeneration = 1
	server.objects[pathJoin(f.scope.Prefix, intent.Key)] = heldGCSObject{
		name: pathJoin(f.scope.Prefix, intent.Key), generation: 1, metageneration: 1,
		payload:       append([]byte(nil), payload...),
		metadata:      map[string]string{"rhiza-operation-id": intent.OperationID, "rhiza-payload-sha256": intent.PayloadDigest},
		temporaryHold: true,
	}
	server.mu.Unlock()
	f.e.SourceMutationStorage = client
	originalProvider, err := newGCSMutationProviderForProfile(client, f.scope, fencing.MutationHeldExecutionProfile)
	if err != nil {
		t.Fatal(err)
	}
	// This is an actual create-only provider request against the existing exact
	// object; it must receive HTTP 412, not a fabricated receipt.
	originalResult, err := originalProvider.uploadHeldCreate(t.Context(), intent.Key, intent, preparation, bytes.NewReader(payload))
	if err != nil || originalResult.Outcome != fencing.MutationRejected || !originalResult.ConditionFailed ||
		!originalResult.AllCopiesTerminal || originalResult.Generation != 0 || originalResult.PayloadDigest != "" {
		t.Fatalf("original request did not return a terminal create-only 412: result=%+v err=%v", originalResult, err)
	}
	server.mu.Lock()
	if len(server.requests) == 0 || server.requests[0].Status != 412 || server.requests[0].IfGenerationMatch != "0" {
		got := append([]heldGCSRequest(nil), server.requests...)
		server.mu.Unlock()
		t.Fatalf("original request was not observed as HTTP 412 with create-only condition: %+v", got)
	}
	server.mu.Unlock()

	repair := testEngineRepairRecord(t, f)
	guard, err := f.e.settleEngineRepairZeroGeneration(t.Context(), repair, f.runtime, f.producer, 1, f.scope)
	if err != nil {
		t.Fatalf("helper did not establish logical commit from exact held-object readback: %v", err)
	}
	server.mu.Lock()
	helperPOSTObserved := len(server.requests) == 2 && server.requests[1].Status == 412 && server.requests[1].IfGenerationMatch == "0"
	requestsAfterHelper := append([]heldGCSRequest(nil), server.requests...)
	server.mu.Unlock()
	if !helperPOSTObserved {
		t.Fatalf("helper logical commit did not follow its own actual create-only 412: %+v", requestsAfterHelper)
	}
	producer, uid, _, err := f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
	if err != nil || uid != f.producer {
		t.Fatalf("read helper-settled producer: uid=%q err=%v", uid, err)
	}
	entry := producer.Entries[1]
	if entry.Resolution == nil || entry.Resolution.Outcome != fencing.MutationCommitted || entry.Resolution.AllCopiesTerminal ||
		entry.Resolution.PayloadDigest != intent.PayloadDigest || entry.GenerationZeroRights == nil || len(entry.GenerationZeroRights.Rights) != 1 {
		t.Fatalf("helper did not preserve logical-commit/physical-pending separation: %+v", entry)
	}
	logicalResolution := *entry.Resolution
	helperReceiptDigest := entry.GenerationZeroRights.Rights[0].TerminalReceiptDigest
	if !mutationDigestValidOperator(helperReceiptDigest) {
		t.Fatalf("helper terminal receipt digest is invalid: %q", helperReceiptDigest)
	}

	// Persist the actual original 412 result only after helper settlement. This
	// models a late original completion while keeping it distinct from the
	// helper's 412/readback receipt.
	originalReceipt := mutationExecutionReceipt{Runtime: f.runtime, WALIdentity: run.Identity.WALIdentity, ProducerUID: f.producer,
		Sequence: 1, Intent: intent, Preparation: preparation, DispatchID: guard.DispatchID, Outcome: originalResult.Outcome,
		ConditionFailed: originalResult.ConditionFailed, Generation: originalResult.Generation, Metageneration: originalResult.Metageneration,
		PayloadDigest: originalResult.PayloadDigest, AllCopiesTerminal: originalResult.AllCopiesTerminal}
	originalReceipt.ReceiptDigest = originalReceipt.digest()
	if originalReceipt.ReceiptDigest == helperReceiptDigest {
		t.Fatal("original and helper terminal receipts collapsed to one digest")
	}
	if digest, err := f.receipts.put(originalReceipt); err != nil || digest != originalReceipt.ReceiptDigest {
		t.Fatalf("persist exact original 412 receipt: digest=%q err=%v", digest, err)
	}
	completion := fencing.MutationCopyCompletion{PreparationHash: guard.PreparationHash, ReceiptDigest: originalReceipt.ReceiptDigest,
		GenerationZeroRightsDigest: entry.GenerationZeroRights.Digest()}
	if _, err := f.e.changeMutationProducer(t.Context(), f.runtime, func(p *fencing.MutationProducer) error {
		if err := p.RecordDispatchTerminal(1, guard.DispatchID, originalReceipt.ReceiptDigest); err != nil {
			return err
		}
		return p.ConfirmCopiesTerminal(1, completion)
	}); err != nil {
		t.Fatalf("attach late original receipt without changing logical resolution: %v", err)
	}
	producer, uid, _, err = f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
	if err != nil || uid != f.producer {
		t.Fatalf("read terminal producer: uid=%q err=%v", uid, err)
	}
	entry = producer.Entries[1]
	if entry.Resolution == nil || *entry.Resolution != logicalResolution || entry.CopyCompletion == nil ||
		entry.CopyCompletion.ReceiptDigest != originalReceipt.ReceiptDigest || entry.Dispatches[guard.DispatchID].ReceiptDigest != originalReceipt.ReceiptDigest {
		t.Fatalf("late original receipt replaced helper logical resolution or was not bound independently: %+v", entry)
	}
	retired, err := f.e.retireGenerationZeroBarrier(t.Context(), f.runtime, f.producer, 1, entry)
	if err != nil || !retired {
		t.Fatalf("retire exact original/helper terminal evidence: retired=%t err=%v", retired, err)
	}

	if _, err := f.e.commitMutationArchive(t.Context(), f.runtime, storeCompactionPlan(t, f.e, f.runtime, 1)); err != nil {
		t.Fatalf("archive completed original/helper evidence: %v", err)
	}
	archived, err := f.e.lookupMutation(t.Context(), f.runtime.StatefulSetUID, run.Identity,
		fencing.MutationLookupRequest{ProducerUID: f.producer, Sequence: 1, Intent: intent})
	if err != nil || !archived.Archived || archived.Entry == nil || archived.Entry.CopyCompletion == nil ||
		archived.Entry.CopyCompletion.ReceiptDigest != originalReceipt.ReceiptDigest {
		t.Fatalf("archive omitted the exact original terminal receipt binding: result=%+v err=%v", archived, err)
	}
	server.mu.Lock()
	postsBeforeReplay := server.posts
	server.mu.Unlock()
	if err := f.e.ExecuteSourceMutation(t.Context(), f.runtime, f.producer, 1, intent, f.scope, originalProvider, f.receipts); err != nil {
		t.Fatalf("archived late original replay did not resolve from terminal evidence: %v", err)
	}
	server.mu.Lock()
	postsAfterReplay := server.posts
	server.mu.Unlock()
	if postsAfterReplay != postsBeforeReplay {
		t.Fatalf("archived late original replay repeated provider wire: posts=%d->%d", postsBeforeReplay, postsAfterReplay)
	}
}

func newGenerationZeroReceiptFixture(t *testing.T) heldProfileFixture {
	t.Helper()
	e, api, id, runtime := newMutationStoreFixture(t)
	e.MutationProfile = fencing.MutationHeldExecutionProfile
	preparePendingArchiveGenesis(t, e, api, id, runtime)
	scope := AttestationStoreScope{Provider: "gcs", Bucket: "archive", Prefix: "root/source"}
	if _, err := e.reserveMutationProducer(t.Context(), runtime); err != nil {
		t.Fatalf("reserve generation-zero receipt producer: %v", err)
	}
	stages, err := e.sourceStagesFor(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	id.WALIdentity = "wal-g0-receipt-binding"
	configureMutationWorkloadFixture(t, e, api, id, runtime)
	if _, err := e.bindWALAtScopeWithStage(t.Context(), runtime.StatefulSetUID, id, hashJSON(scope), stages.incarnation); err != nil {
		t.Fatalf("bind generation-zero receipt producer: %v", err)
	}
	if _, err := e.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error { return p.Activate(id) }); err != nil {
		t.Fatalf("activate generation-zero receipt producer: %v", err)
	}
	run, err := e.enrolledMutationRun(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	receipts, err := e.receiptsForRuntime(runtime)
	if err != nil {
		t.Fatal(err)
	}
	return heldProfileFixture{e: e, api: api, id: id, runtime: runtime, producer: run.MutationProducerUID,
		scope: scope, stages: stages, receipts: receipts}
}
