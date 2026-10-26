package operator

import (
	"bytes"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

// makeGuardedG0ClosureFixture reuses the existing held-GCS fixture and
// creates a real canonical G0 guard plus terminal helper rights while leaving
// the original dispatch receipt unknown.
func makeGuardedG0ClosureFixture(t *testing.T) (heldProfileFixture, fencing.EngineRepairRecord, fencing.MutationProducer, fencing.Run, fencing.MutationEntry) {
	t.Helper()
	f := newGenerationZeroReceiptFixture(t)
	payload := []byte(`{"closure":"exact generation-zero binding"}`)
	intent := f.admitUpload(t, 1, "membership/engine-repair-guarded-closure", fencing.MutationCondition{Mode: fencing.MutationUnconditional}, payload)
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
		if err := p.GrantDispatch(1, "guarded-closure-original-dispatch"); err != nil {
			return err
		}
		if err := p.SealDispatch(1); err != nil {
			return err
		}
		return p.Close("guarded-closure-old-close", hashBytes([]byte("guarded-closure-old-close")))
	}); err != nil {
		t.Fatalf("close exact original dispatch prefix: %v", err)
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
	if _, err := originalProvider.uploadHeldCreate(t.Context(), intent.Key, intent, preparation, bytes.NewReader(payload)); err == nil {
		t.Fatal("original G0 dispatch unexpectedly returned a trusted response")
	}
	server.mu.Lock()
	origin, exists := server.objects[pathJoin(f.scope.Prefix, intent.Key)]
	server.mu.Unlock()
	if !exists || !origin.temporaryHold || !bytes.Equal(origin.payload, payload) {
		t.Fatalf("fixture did not preserve the held G0 origin: exists=%t object=%+v", exists, origin)
	}
	repair := testEngineRepairRecord(t, f)
	if _, err := f.e.settleEngineRepairZeroGeneration(t.Context(), repair, f.runtime, f.producer, 1, f.scope); err != nil {
		t.Fatalf("establish exact G0 guard and helper rights: %v", err)
	}
	producer, producerUID, _, err := f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
	if err != nil || producerUID != f.producer {
		t.Fatalf("read guarded producer: uid=%q err=%v", producerUID, err)
	}
	entry, ok := producer.Entries[1]
	if !ok || entry.GenerationZeroGuard == nil || entry.GenerationZeroRights == nil ||
		!entry.GenerationZeroRights.Sealed || !entry.GenerationZeroRights.HelperRightsRetired ||
		!generationZeroOperatorRightsTerminal(entry.GenerationZeroRights) || entry.CopyCompletion != nil ||
		entry.Resolution == nil || entry.Resolution.AllCopiesTerminal {
		t.Fatalf("fixture does not separate helper closure from original physical completion: %+v", entry)
	}
	return f, repair, producer, run, entry
}

func TestEngineRepairGuardedClosureRejectsChangedCanonicalG0Bindings(t *testing.T) {
	f, repair, producer, run, entry := makeGuardedG0ClosureFixture(t)
	if err := producer.ValidateGenerationZeroGuard(f.producer, 1, repair, entry); err != nil {
		t.Fatalf("baseline exact G0 guard was not valid: %v", err)
	}
	if err := f.e.verifyGenerationZeroGuard(t.Context(), f.runtime, run, f.producer, 1, entry); err != nil {
		t.Fatalf("baseline canonical G0 closure guard did not verify: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*fencing.MutationEntry)
	}{
		{name: "wrong runtime", mutate: func(entry *fencing.MutationEntry) {
			guard := *entry.GenerationZeroGuard
			guard.Runtime.ContainerID = "other-container"
			guard.GuardDigest = guard.Digest()
			entry.GenerationZeroGuard = &guard
		}},
		{name: "wrong WAL", mutate: func(entry *fencing.MutationEntry) {
			guard := *entry.GenerationZeroGuard
			guard.WALIdentity = "other-wal"
			guard.GuardDigest = guard.Digest()
			entry.GenerationZeroGuard = &guard
		}},
		{name: "wrong dispatch", mutate: func(entry *fencing.MutationEntry) {
			guard := *entry.GenerationZeroGuard
			guard.DispatchID = "other-dispatch"
			guard.GuardDigest = guard.Digest()
			entry.GenerationZeroGuard = &guard
		}},
		{name: "wrong key", mutate: func(entry *fencing.MutationEntry) {
			guard := *entry.GenerationZeroGuard
			guard.Key = "another/object"
			guard.GuardDigest = guard.Digest()
			entry.GenerationZeroGuard = &guard
		}},
		{name: "wrong repair origin", mutate: func(entry *fencing.MutationEntry) {
			guard := *entry.GenerationZeroGuard
			guard.RepairBinding.OperationID = "other-operation"
			guard.GuardDigest = guard.Digest()
			entry.GenerationZeroGuard = &guard
		}},
		{name: "helper rights name another repair", mutate: func(entry *fencing.MutationEntry) {
			rights := *entry.GenerationZeroRights
			rights.RepairRecordHash = hashBytes([]byte("other-repair"))
			entry.GenerationZeroRights = &rights
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mutated := entry
			mutated.GenerationZeroGuard = cloneGenerationZeroGuard(entry.GenerationZeroGuard)
			mutated.GenerationZeroRights = cloneGenerationZeroRights(entry.GenerationZeroRights)
			tc.mutate(&mutated)
			if err := producer.ValidateGenerationZeroGuard(f.producer, 1, repair, mutated); err == nil {
				t.Fatal("guard validator accepted an altered canonical binding")
			}
		})
	}
}

func TestEngineRepairGuardedClosureRejectsUnsealedOrNonterminalHelperSnapshot(t *testing.T) {
	f, _, _, run, entry := makeGuardedG0ClosureFixture(t)
	tests := []struct {
		name   string
		mutate func(*fencing.MutationGenerationZeroRights)
	}{
		{name: "helper rights are not sealed", mutate: func(rights *fencing.MutationGenerationZeroRights) { rights.Sealed = false }},
		{name: "helper rights are not retired", mutate: func(rights *fencing.MutationGenerationZeroRights) { rights.HelperRightsRetired = false }},
		{name: "helper right is not terminal", mutate: func(rights *fencing.MutationGenerationZeroRights) {
			rights.Rights[0].TerminalReceiptDigest = ""
		}},
		{name: "helper right request differs", mutate: func(rights *fencing.MutationGenerationZeroRights) {
			rights.Rights[0].RequestDigest = hashBytes([]byte("another-helper-request"))
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mutated := entry
			mutated.GenerationZeroRights = cloneGenerationZeroRights(entry.GenerationZeroRights)
			tc.mutate(mutated.GenerationZeroRights)
			if err := f.e.verifyGenerationZeroGuard(t.Context(), f.runtime, run, f.producer, 1, mutated); err == nil {
				t.Fatal("guard verifier accepted a helper-right snapshot that differs from the canonical closed producer")
			}
		})
	}
}

func TestEngineRepairGuardedClosureUsesCommonStorageVerifier(t *testing.T) {
	f, repair, producer, run, entry := makeGuardedG0ClosureFixture(t)
	if err := f.e.verifyEngineRepairProducerStorageClosure(t.Context(), repair, run, f.producer, producer); err != nil {
		t.Fatalf("common storage-closure verifier rejected the exact guarded unresolved-original case: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*fencing.MutationEntry)
	}{
		{name: "guard names another dispatch", mutate: func(entry *fencing.MutationEntry) {
			guard := *entry.GenerationZeroGuard
			guard.DispatchID = "other-dispatch"
			guard.GuardDigest = guard.Digest()
			entry.GenerationZeroGuard = &guard
		}},
		{name: "guard names another runtime", mutate: func(entry *fencing.MutationEntry) {
			guard := *entry.GenerationZeroGuard
			guard.Runtime.ContainerID = "other-container"
			guard.GuardDigest = guard.Digest()
			entry.GenerationZeroGuard = &guard
		}},
		{name: "helper receipt is missing", mutate: func(entry *fencing.MutationEntry) {
			rights := cloneGenerationZeroRights(entry.GenerationZeroRights)
			rights.Rights[0].TerminalReceiptDigest = ""
			entry.GenerationZeroRights = rights
		}},
		{name: "helper rights are unsealed", mutate: func(entry *fencing.MutationEntry) {
			rights := cloneGenerationZeroRights(entry.GenerationZeroRights)
			rights.Sealed = false
			entry.GenerationZeroRights = rights
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mutated := producer
			mutated.Entries = make(map[uint64]fencing.MutationEntry, len(producer.Entries))
			for sequence, current := range producer.Entries {
				mutated.Entries[sequence] = current
			}
			changed := mutated.Entries[1]
			changed.GenerationZeroGuard = cloneGenerationZeroGuard(entry.GenerationZeroGuard)
			changed.GenerationZeroRights = cloneGenerationZeroRights(entry.GenerationZeroRights)
			tc.mutate(&changed)
			mutated.Entries[1] = changed
			if err := f.e.verifyEngineRepairProducerStorageClosure(t.Context(), repair, run, f.producer, mutated); err == nil {
				t.Fatal("common storage-closure verifier accepted a tampered canonical G0 guard/helper snapshot")
			}
		})
	}
}

func TestEngineRepairStorageEffectsProofRejectsWrongOriginAndPrefix(t *testing.T) {
	e, _, repair, _ := noProducerEngineRepairFixture(t)
	ledger := mustReadRoot(t, e, repair)
	proof := fencing.StorageEffectsExcluded{Version: fencing.StorageEffectsProofVersion, Binding: repair.Binding,
		RepairRecordHash: repair.IdentityDigest()}
	for _, incarnation := range repair.Incarnations {
		if incarnation.Identity.NodeID != repair.Binding.OldMemberID {
			continue
		}
		run := ledger.Ledger.Runs[incarnation.Identity.RunID]
		settlement, err := engineRepairNoProducerSettlement(repair, ledger.Ledger, incarnation, run)
		if err != nil {
			t.Fatalf("build settlement from canonical old run: %v", err)
		}
		proof.RunSettlements = append(proof.RunSettlements, settlement)
	}
	proof.ProofDigest = proof.Digest()
	if err := proof.Validate(repair); err != nil {
		t.Fatalf("fixture proof should bind the exact old-run inventory: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*fencing.StorageEffectsExcluded)
	}{
		{name: "wrong repair origin", mutate: func(proof *fencing.StorageEffectsExcluded) {
			proof.Binding.OperationID = "other-operation"
		}},
		{name: "wrong run identity", mutate: func(proof *fencing.StorageEffectsExcluded) {
			proof.RunSettlements[0].RunID = "other-run"
		}},
		{name: "wrong pod UID", mutate: func(proof *fencing.StorageEffectsExcluded) {
			proof.RunSettlements[0].PodUID = "other-pod"
		}},
		{name: "wrong old WAL", mutate: func(proof *fencing.StorageEffectsExcluded) {
			proof.RunSettlements[0].WALIdentity = "other-wal"
		}},
		{name: "wrong producer origin", mutate: func(proof *fencing.StorageEffectsExcluded) {
			proof.RunSettlements[0].ProducerUID = "unexpected-producer"
		}},
		{name: "missing prefix settlement", mutate: func(proof *fencing.StorageEffectsExcluded) {
			proof.RunSettlements = nil
		}},
		{name: "duplicate prefix settlement", mutate: func(proof *fencing.StorageEffectsExcluded) {
			proof.RunSettlements = append(proof.RunSettlements, proof.RunSettlements[0])
		}},
		{name: "malformed settlement digest", mutate: func(proof *fencing.StorageEffectsExcluded) {
			proof.RunSettlements[0].SettlementDigest = ""
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bad := proof
			bad.RunSettlements = append([]fencing.StorageRunSettlement(nil), proof.RunSettlements...)
			tc.mutate(&bad)
			bad.ProofDigest = bad.Digest()
			if err := bad.Validate(repair); err == nil {
				t.Fatal("storage-effects proof accepted a changed origin or incomplete old-run prefix")
			}
		})
	}
}

func cloneGenerationZeroGuard(guard *fencing.MutationGenerationZeroGuard) *fencing.MutationGenerationZeroGuard {
	if guard == nil {
		return nil
	}
	copy := *guard
	return &copy
}

func cloneGenerationZeroRights(rights *fencing.MutationGenerationZeroRights) *fencing.MutationGenerationZeroRights {
	if rights == nil {
		return nil
	}
	copy := *rights
	copy.Rights = append([]fencing.MutationGenerationZeroPhysicalRight(nil), rights.Rights...)
	return &copy
}
