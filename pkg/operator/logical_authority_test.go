package operator

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
	"google.golang.org/api/option"
)

func TestLogicalAuthorityRootProfileAndRegistrationMutationRevocation(t *testing.T) {
	ctx := t.Context()
	bucket := objstore.NewInMemBucket()
	t.Cleanup(func() { _ = bucket.Close() })
	store, err := NewAuthorityStateStore(bucket, "logical-authority")
	if err != nil {
		t.Fatal(err)
	}
	const uid, source = "logical-sts", "logical-source"
	executor := &FencingExecutor{Kube: newMissingLegacyLedgerKube(t), AuthorityState: store,
		RequireMutationJournal: true, AuthorityProfile: fencing.LogicalWriterProfileV1}
	if err := executor.initializeRoot(ctx, uid, source); err != nil {
		t.Fatal(err)
	}
	root, rootVersion, err := store.ReadRoot(ctx, uid, source)
	if err != nil {
		t.Fatal(err)
	}
	if root.Ledger.Profile != fencing.LogicalWriterProfileV1 || root.Ledger.RequireRuntimeBinding || !root.Ledger.RequireMutationJournal {
		t.Fatalf("logical root has wrong explicit profile: %+v", root.Ledger)
	}

	identity := fencing.Identity{SourceClusterID: source, NodeID: "voter-a", RunID: "run-a"}
	binding := fencing.LogicalWriterBindingV1{Version: fencing.LogicalWriterVersionV1,
		Profile: fencing.LogicalWriterProfileV1, SourceClusterID: source, NodeID: identity.NodeID,
		RunID: identity.RunID, Role: fencing.StartupRoleVoterActive}
	if err := root.Ledger.RegisterLogicalWithRole(identity, binding); err != nil {
		t.Fatal(err)
	}
	const expectedProducerUID = "logical-producer-a"
	if err := root.Ledger.BindLogicalMutationProducer(identity, expectedProducerUID); err != nil {
		t.Fatal(err)
	}
	scopeHash := hashJSON([]string{"logical-test-scope", source})
	if err := root.Ledger.BindLogicalMutationScope(identity, scopeHash, fencing.MutationExecutionProfile); err != nil {
		t.Fatal(err)
	}
	if err := root.Ledger.BindLogicalMutationStage(identity, hashJSON([]string{"logical-stage", "one"})); err != nil {
		t.Fatal(err)
	}
	run := root.Ledger.Runs[identity.RunID]
	if err := executor.validateMutationJournalProfile(root); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteRoot(ctx, uid, source, root, rootVersion); err != nil {
		t.Fatal(err)
	}
	if err := executor.reserveLogicalMutationProducer(ctx, uid, *run.LogicalBinding); err != nil {
		t.Fatal(err)
	}
	identity.WALIdentity = hashJSON([]string{"wal", identity.RunID})
	receipt, err := executor.bindLogicalSupervisorWAL(ctx, uid, identity)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.AuthorityProfile != fencing.LogicalWriterProfileV1 || receipt.LogicalBinding == nil || receipt.LogicalBinding.WALIdentity != identity.WALIdentity || receipt.Producer == nil {
		t.Fatalf("logical WAL bind returned incomplete authority receipt: %+v", receipt)
	}
	root, rootVersion, err = store.ReadRoot(ctx, uid, source)
	if err != nil {
		t.Fatal(err)
	}
	run = root.Ledger.Runs[identity.RunID]
	producer, gotProducerUID, producerVersion, err := store.ReadLogicalProducer(ctx, uid, *run.LogicalBinding)
	if err != nil || gotProducerUID != run.MutationProducerUID {
		t.Fatalf("logical producer read after WAL bind: uid=%q err=%v", gotProducerUID, err)
	}
	wrongWALBinding := *run.LogicalBinding
	wrongWALBinding.WALIdentity = hashJSON("different-wal")
	if _, _, _, err := store.ReadLogicalProducer(ctx, uid, wrongWALBinding); err == nil {
		t.Fatal("logical producer accepted a WAL different from its canonical Run")
	}
	if producer.Phase != fencing.MutationOpen || producer.WALIdentity != identity.WALIdentity {
		t.Fatalf("logical bind did not activate producer: %+v", producer)
	}
	intent := fencing.MutationIntent{ContractVersion: fencing.MutationIntentVersion, StoreScopeHash: scopeHash,
		OwnerStep:   fencing.MutationOwnerStep{Owner: "test", Workflow: "recovery", Step: "delete", Attempt: 1},
		Condition:   fencing.MutationCondition{Mode: fencing.MutationMatchGeneration, Generation: 3},
		OperationID: fencing.MutationRequestID(expectedProducerUID, 1), Kind: "delete", Key: "objects/a"}
	if err := producer.Admit(1, intent); err != nil {
		t.Fatal(err)
	}
	if err := producer.Prepare(1, fencing.MutationPreparation{Profile: fencing.MutationExecutionProfile,
		StoreScopeHash: scopeHash, GenerationSet: true, Generation: 3}); err != nil {
		t.Fatal(err)
	}
	if err := producer.GrantDispatch(1, "dispatch-in-flight"); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteLogicalProducer(ctx, uid, *run.LogicalBinding, producer, producerVersion); err != nil {
		t.Fatal(err)
	}
	if err := root.Ledger.CloseNodes("revoke-a", hashJSON("revoke-request"), []string{identity.NodeID}); err != nil {
		t.Fatal(err)
	}
	if err := producer.Close("revoke-a", hashJSON("revoke-request")); err != nil {
		t.Fatal(err)
	}
	if err := producer.Admit(2, intent); !errors.Is(err, fencing.ErrClosed) {
		t.Fatalf("revoked producer admitted new mutation: %v", err)
	}
	if err := store.WriteRoot(ctx, uid, source, root, rootVersion); err != nil {
		t.Fatal(err)
	}
	_, _, producerVersion, err = store.ReadLogicalProducer(ctx, uid, *run.LogicalBinding)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WriteLogicalProducer(ctx, uid, *run.LogicalBinding, producer, producerVersion); err != nil {
		t.Fatal(err)
	}
	if err := executor.recordLogicalRunClosure(ctx, uid, identity); !errors.Is(err, fencing.ErrPending) && !errors.Is(err, ErrFencePending) {
		t.Fatalf("logical closure accepted an unresolved admitted mutation: %v", err)
	}

	reopened, err := NewAuthorityStateStore(bucket, "logical-authority")
	if err != nil {
		t.Fatal(err)
	}
	loadedRoot, _, err := reopened.ReadRoot(ctx, uid, source)
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.validateMutationJournalProfile(loadedRoot); err != nil {
		t.Fatalf("reloaded logical root was not accepted: %v", err)
	}
	loaded, gotUID, _, err := reopened.ReadLogicalProducer(ctx, uid, *run.LogicalBinding)
	if err != nil || gotUID != expectedProducerUID || loaded.Profile != fencing.LogicalWriterProfileV1 || loaded.Phase != fencing.MutationClosed || loaded.Entries[1].Intent != intent {
		t.Fatalf("logical producer round trip lost closed journal: producer=%+v uid=%q err=%v", loaded, gotUID, err)
	}
	if _, _, _, err := reopened.ReadProducer(ctx, fencing.RuntimeBinding{}); err == nil {
		t.Fatal("logical producer was readable through the physical-runtime namespace")
	}
}

func TestLogicalRunClosureRequiresTrustedStorageVerifier(t *testing.T) {
	ctx := t.Context()
	bucket := objstore.NewInMemBucket()
	t.Cleanup(func() { _ = bucket.Close() })
	store, err := NewAuthorityStateStore(bucket, "logical-closure")
	if err != nil {
		t.Fatal(err)
	}
	const uid, source = "closure-sts", "closure-source"
	e := &FencingExecutor{Kube: newMissingLegacyLedgerKube(t), AuthorityState: store,
		RequireMutationJournal: true, AuthorityProfile: fencing.LogicalWriterProfileV1}
	if err := e.initializeRoot(ctx, uid, source); err != nil {
		t.Fatal(err)
	}
	root, version, err := store.ReadRoot(ctx, uid, source)
	if err != nil {
		t.Fatal(err)
	}
	id := fencing.Identity{SourceClusterID: source, NodeID: "voter", RunID: "run"}
	binding := fencing.LogicalWriterBindingV1{Version: fencing.LogicalWriterVersionV1, Profile: fencing.LogicalWriterProfileV1,
		SourceClusterID: source, NodeID: id.NodeID, RunID: id.RunID, Role: fencing.StartupRoleVoterActive,
		ProducerUID: "producer", StoreScopeHash: hashJSON("scope")}
	if err := root.Ledger.RegisterLogicalWithRole(id, binding); err != nil {
		t.Fatal(err)
	}
	if err := root.Ledger.BindLogicalMutationScope(id, binding.StoreScopeHash, fencing.MutationExecutionProfile); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteRoot(ctx, uid, source, root, version); err != nil {
		t.Fatal(err)
	}
	if err := e.reserveLogicalMutationProducer(ctx, uid, *root.Ledger.Runs[id.RunID].LogicalBinding); err != nil {
		t.Fatal(err)
	}
	id.WALIdentity = hashJSON("closure-wal")
	if _, err := e.bindLogicalSupervisorWAL(ctx, uid, id); err != nil {
		t.Fatal(err)
	}
	if _, err := e.change(ctx, uid, source, func(ledger *fencing.Ledger) error {
		return ledger.CloseNodes("remove-voter", hashJSON("remove-request"), []string{id.NodeID})
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.recordLogicalRunClosure(ctx, uid, id); !errors.Is(err, ErrFencePending) {
		t.Fatalf("logical closure accepted without a configured real storage reader: %v", err)
	}
	closed, _, err := store.ReadRoot(ctx, uid, source)
	if err != nil {
		t.Fatal(err)
	}
	ref := closed.Ledger.Runs[id.RunID].LogicalClosure
	if ref != nil {
		t.Fatalf("logical closure reference was recorded without a real storage proof: %+v", ref)
	}
}

func newStoppedUnboundLogicalRun(t *testing.T, suffix string) (*FencingExecutor, *AuthorityStateStore, string, fencing.Identity, fencing.LogicalWriterBindingV1) {
	t.Helper()
	ctx := t.Context()
	bucket := objstore.NewInMemBucket()
	t.Cleanup(func() { _ = bucket.Close() })
	store, err := NewAuthorityStateStore(bucket, "logical-unbound-closure-"+suffix)
	if err != nil {
		t.Fatal(err)
	}
	uid, source := "unbound-sts-"+suffix, "unbound-source-"+suffix
	e := &FencingExecutor{Kube: newMissingLegacyLedgerKube(t), AuthorityState: store,
		RequireMutationJournal: true, AuthorityProfile: fencing.LogicalWriterProfileV1}
	if err := e.initializeRoot(ctx, uid, source); err != nil {
		t.Fatal(err)
	}
	id := fencing.Identity{SourceClusterID: source, NodeID: "voter", RunID: "run-" + suffix}
	binding := fencing.LogicalWriterBindingV1{Version: fencing.LogicalWriterVersionV1, Profile: fencing.LogicalWriterProfileV1,
		SourceClusterID: source, NodeID: id.NodeID, RunID: id.RunID, Role: fencing.StartupRoleVoterActive,
		ProducerUID: "producer-" + suffix, StoreScopeHash: hashJSON([]string{"scope", suffix})}
	root, version, err := store.ReadRoot(ctx, uid, source)
	if err != nil {
		t.Fatal(err)
	}
	if err := root.Ledger.RegisterLogicalWithRole(id, binding); err != nil {
		t.Fatal(err)
	}
	if err := root.Ledger.BindLogicalMutationScope(id, binding.StoreScopeHash, fencing.MutationExecutionProfile); err != nil {
		t.Fatal(err)
	}
	if err := root.Ledger.BindLogicalMutationStage(id, hashJSON([]string{"stage", suffix})); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteRoot(ctx, uid, source, root, version); err != nil {
		t.Fatal(err)
	}
	if err := e.reserveLogicalMutationProducer(ctx, uid, binding); err != nil {
		t.Fatal(err)
	}
	if _, err := e.change(ctx, uid, source, func(ledger *fencing.Ledger) error {
		return ledger.CloseNodes("stop-"+suffix, hashJSON([]string{"stop", suffix}), []string{id.NodeID})
	}); err != nil {
		t.Fatal(err)
	}
	return e, store, uid, id, binding
}

func TestLogicalUnboundNoWorkRunCanBeClosedWithoutPhysicalExit(t *testing.T) {
	e, store, uid, id, binding := newStoppedUnboundLogicalRun(t, "success")
	if err := e.recordLogicalRunClosure(t.Context(), uid, id); err != nil {
		t.Fatalf("stopped unbound Run with exact reserved producer was not closed: %v", err)
	}
	root, _, err := store.ReadRoot(t.Context(), uid, id.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	run := root.Ledger.Runs[id.RunID]
	if run.LogicalClosure == nil || run.LogicalClosure.Kind != fencing.LogicalRunClosureUnboundNoWork ||
		run.LogicalClosure.ClosedThrough != 0 || run.Identity.WALIdentity != "" || run.Exited || run.NeverBoundExit != nil {
		t.Fatalf("unbound closure relied on physical-exit evidence or has the wrong kind: run=%+v", run)
	}
	producer, producerUID, _, err := store.ReadLogicalProducer(t.Context(), uid, binding)
	if err != nil || producerUID != binding.ProducerUID || producer.Phase != fencing.MutationClosed || producer.WALIdentity != "" ||
		producer.ClosedThrough != 0 || producer.LastAdmitted != 0 || len(producer.Entries) != 0 {
		t.Fatalf("closure did not seal the exact empty reserved producer: producer=%+v uid=%q err=%v", producer, producerUID, err)
	}
	evidenceStore, err := e.authorityEvidenceForRoot(uid, id.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	reader, _, err := evidenceStore.Read(t.Context(), run.LogicalClosure.EvidenceName)
	if err != nil {
		t.Fatal(err)
	}
	var proof logicalStorageClosureEvidence
	decodeErr := json.NewDecoder(reader).Decode(&proof)
	closeErr := reader.Close()
	if err := errors.Join(decodeErr, closeErr); err != nil || proof.ClosureKind != fencing.LogicalRunClosureUnboundNoWork ||
		proof.Binding != binding || proof.ProducerStateDigest != hashJSON(producer) || len(proof.Entries) != 0 {
		t.Fatalf("unbound closure evidence does not bind the empty producer: proof=%+v err=%v", proof, err)
	}
}

func TestLogicalStorageClosureVerifiesCompactedTerminalPrefix(t *testing.T) {
	ctx := t.Context()
	const uid, source, node, runID = "closure-archive-sts", "closure-archive-source", "voter", "run"
	scope := AttestationStoreScope{Provider: "gcs", Bucket: "archive-bucket", Prefix: "archive/" + source}
	bucket := &archiveTestBucket{Bucket: objstore.NewInMemBucket()}
	t.Cleanup(func() { _ = bucket.Close() })
	store, err := NewAuthorityStateStore(bucket, "logical-closure-archive")
	if err != nil {
		t.Fatal(err)
	}
	sts := object{"metadata": object{"uid": uid}, "spec": object{"template": object{"spec": object{"containers": []any{object{
		"name": "rhiza", "env": []any{object{"name": "RHIZA_OBJSTORE_PROVIDER", "value": "gcs"},
			object{"name": "RHIZA_OBJSTORE_BUCKET", "value": scope.Bucket},
			object{"name": "RHIZA_OBJSTORE_PREFIX", "value": "archive"}}}}}}}}
	kubeServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/apis/apps/v1/namespaces/test/statefulsets" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(object{"items": []any{sts}})
	}))
	t.Cleanup(kubeServer.Close)
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	e := &FencingExecutor{Kube: &Kubernetes{BaseURL: kubeServer.URL, Namespace: "test", TokenFile: token, Client: kubeServer.Client()},
		AuthorityState: store, RequireMutationJournal: true, AuthorityProfile: fencing.LogicalWriterProfileV1}
	if err := e.initializeRoot(ctx, uid, source); err != nil {
		t.Fatal(err)
	}
	id := fencing.Identity{SourceClusterID: source, NodeID: node, RunID: runID}
	binding := fencing.LogicalWriterBindingV1{Version: fencing.LogicalWriterVersionV1, Profile: fencing.LogicalWriterProfileV1,
		SourceClusterID: source, NodeID: node, RunID: runID, Role: fencing.StartupRoleVoterActive,
		ProducerUID: "closure-archive-producer", StoreScopeHash: hashJSON(scope)}
	root, version, err := store.ReadRoot(ctx, uid, source)
	if err != nil {
		t.Fatal(err)
	}
	if err := root.Ledger.RegisterLogicalWithRole(id, binding); err != nil {
		t.Fatal(err)
	}
	if err := root.Ledger.BindLogicalMutationProducer(id, binding.ProducerUID); err != nil {
		t.Fatal(err)
	}
	if err := root.Ledger.BindLogicalMutationScope(id, binding.StoreScopeHash, fencing.MutationExecutionProfile); err != nil {
		t.Fatal(err)
	}
	if err := root.Ledger.BindLogicalMutationStage(id, hashJSON([]string{"closure-stage", runID})); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteRoot(ctx, uid, source, root, version); err != nil {
		t.Fatal(err)
	}
	if err := e.reserveLogicalMutationProducer(ctx, uid, binding); err != nil {
		t.Fatal(err)
	}
	id.WALIdentity = hashJSON([]string{"closure-wal", runID})
	receipt, err := e.bindLogicalSupervisorWAL(ctx, uid, id)
	if err != nil || receipt.LogicalBinding == nil {
		t.Fatalf("bind logical WAL: receipt=%+v err=%v", receipt, err)
	}
	binding = *receipt.LogicalBinding
	providerServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) }))
	t.Cleanup(providerServer.Close)
	client, err := storage.NewClient(ctx, option.WithEndpoint(providerServer.URL+"/storage/v1/"), option.WithHTTPClient(providerServer.Client()), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	e.SourceMutationStorage = client
	receipts, err := e.receiptsForLogicalBinding(uid, binding)
	if err != nil {
		t.Fatal(err)
	}
	authority := logicalMutationAuthority(uid, binding)
	boundRoot, _, err := store.ReadRoot(ctx, uid, source)
	if err != nil {
		t.Fatal(err)
	}
	run := boundRoot.Ledger.Runs[runID]
	for sequence := uint64(1); sequence <= 130; sequence++ {
		if err := e.compactLogicalSourceMutationJournal(ctx, uid, binding, sequence); err != nil {
			t.Fatalf("compact before admission %d: %v", sequence, err)
		}
		intent := fencing.MutationIntent{ContractVersion: fencing.MutationIntentVersion, StoreScopeHash: binding.StoreScopeHash,
			OperationID: fencing.MutationRequestID(binding.ProducerUID, sequence), OwnerStep: fencing.MutationOwnerStep{Owner: "test", Workflow: "closure", Step: "delete", Attempt: sequence},
			Condition: fencing.MutationCondition{Mode: fencing.MutationMatchGeneration, Generation: 1}, Kind: "delete", Key: fmt.Sprintf("objects/key-%03d", sequence)}
		preparation := fencing.MutationPreparation{Profile: fencing.MutationExecutionProfile, StoreScopeHash: binding.StoreScopeHash, GenerationSet: true, Generation: 1}
		dispatchID := fmt.Sprintf("dispatch-%03d", sequence)
		_, err := e.changeMutationProducerForAuthority(ctx, authority, binding.ProducerUID, func(producer *fencing.MutationProducer) error {
			if err := producer.Admit(sequence, intent); err != nil {
				return err
			}
			if err := producer.Prepare(sequence, preparation); err != nil {
				return err
			}
			return producer.GrantDispatch(sequence, dispatchID)
		})
		if err != nil {
			t.Fatalf("admit logical mutation %d: %v", sequence, err)
		}
		preparationHash, err := func() (string, error) {
			producer, _, _, readErr := e.readMutationProducerForAuthority(ctx, authority)
			if readErr != nil {
				return "", readErr
			}
			return producer.PreparationHash(sequence)
		}()
		if err != nil {
			t.Fatal(err)
		}
		providerResult := mutationProviderResult{Outcome: fencing.MutationCommitted, Generation: 1, AllCopiesTerminal: true}
		executionReceipt := authority.makeReceipt(run, binding.ProducerUID, sequence, intent, preparation, dispatchID, providerResult)
		if digest, err := receipts.put(executionReceipt); err != nil || digest != executionReceipt.ReceiptDigest {
			t.Fatalf("persist receipt %d: digest=%q err=%v", sequence, digest, err)
		}
		_, err = e.changeMutationProducerForAuthority(ctx, authority, binding.ProducerUID, func(producer *fencing.MutationProducer) error {
			if err := producer.SealDispatch(sequence); err != nil {
				return err
			}
			if err := producer.RecordDispatchTerminal(sequence, dispatchID, executionReceipt.ReceiptDigest); err != nil {
				return err
			}
			return producer.Resolve(sequence, fencing.MutationResolution{Outcome: fencing.MutationCommitted, PreparationHash: preparationHash,
				ReceiptDigest: executionReceipt.ReceiptDigest, AllCopiesTerminal: true})
		})
		if err != nil {
			t.Fatalf("settle logical mutation %d: %v", sequence, err)
		}
	}
	producer, _, _, err := e.readMutationProducerForAuthority(ctx, authority)
	if err != nil || producer.CompactedThrough == 0 || producer.CompactedThrough >= producer.LastAdmitted {
		t.Fatalf("expected compacted prefix and retained suffix: producer=%+v err=%v", producer, err)
	}
	if _, err := e.change(ctx, uid, source, func(ledger *fencing.Ledger) error {
		return ledger.CloseNodes("remove-voter", hashJSON("close-logical-voter"), []string{node})
	}); err != nil {
		t.Fatal(err)
	}
	archiveEvidence, err := e.evidenceStoreForMutationAuthority(authority)
	if err != nil {
		t.Fatal(err)
	}
	archivePath, err := archiveEvidence.objectKey(producer.ArchiveHead.Name)
	if err != nil {
		t.Fatal(err)
	}
	bucket.failReadKey = archivePath
	if err := e.recordLogicalRunClosure(ctx, uid, id); err == nil {
		t.Fatal("transient archive read failure incorrectly verified logical closure")
	}
	stillPending, _, _, err := e.readMutationProducerForAuthority(ctx, authority)
	if err != nil || stillPending.EvidenceFault != "" {
		t.Fatalf("transient archive read failure latched permanent corruption: producer=%+v err=%v", stillPending, err)
	}
	bucket.failReadKey = ""
	if err := e.recordLogicalRunClosure(ctx, uid, id); err != nil {
		t.Fatalf("closure rejected verified compacted terminal prefix: %v", err)
	}
	closedRoot, _, err := store.ReadRoot(ctx, uid, source)
	if err != nil {
		t.Fatal(err)
	}
	closure := closedRoot.Ledger.Runs[runID].LogicalClosure
	if closure == nil {
		t.Fatal("compacted closure reference was not persisted")
	}
	evidence, err := e.authorityEvidenceForRoot(uid, source)
	if err != nil {
		t.Fatal(err)
	}
	reader, _, err := evidence.Read(ctx, closure.EvidenceName)
	if err != nil {
		t.Fatal(err)
	}
	closedProducer, _, _, err := e.readMutationProducerForAuthority(ctx, authority)
	if err != nil {
		t.Fatal(err)
	}
	var proof logicalStorageClosureEvidence
	decodeErr := json.NewDecoder(reader).Decode(&proof)
	closeErr := reader.Close()
	if err := errors.Join(decodeErr, closeErr); err != nil || proof.CompactedThrough != closedProducer.CompactedThrough || proof.ClosedThrough != 130 ||
		proof.EntryCount != 130 || proof.ClosedPrefixDigest == "" || proof.ArchiveHead == nil || len(proof.Entries) != int(closedProducer.ClosedThrough-closedProducer.CompactedThrough) {
		t.Fatalf("compacted closure proof did not bind compacted prefix plus live suffix: proof=%+v err=%v", proof, err)
	}
}

func TestLogicalUnboundNoWorkClosureRejectsAdmittedOrDispatchedProducer(t *testing.T) {
	for _, tc := range []struct {
		name  string
		entry func(fencing.MutationIntent) fencing.MutationEntry
	}{
		{name: "admitted", entry: func(intent fencing.MutationIntent) fencing.MutationEntry {
			return fencing.MutationEntry{Intent: intent}
		}},
		{name: "dispatch permission", entry: func(intent fencing.MutationIntent) fencing.MutationEntry {
			return fencing.MutationEntry{Intent: intent, Dispatches: map[string]fencing.MutationDispatch{"dispatch": {}}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, store, uid, id, binding := newStoppedUnboundLogicalRun(t, strings.ReplaceAll(tc.name, " ", "-"))
			producer, _, version, err := store.ReadLogicalProducer(t.Context(), uid, binding)
			if err != nil {
				t.Fatal(err)
			}
			intent := fencing.MutationIntent{ContractVersion: fencing.MutationIntentVersion, StoreScopeHash: binding.StoreScopeHash,
				OwnerStep:   fencing.MutationOwnerStep{Owner: "test", Workflow: "closure", Step: "reject-work", Attempt: 1},
				Condition:   fencing.MutationCondition{Mode: fencing.MutationMatchGeneration, Generation: 1},
				OperationID: fencing.MutationRequestID(binding.ProducerUID, 1), Kind: "delete", Key: "object"}
			producer.LastAdmitted = 1
			producer.Entries[1] = tc.entry(intent)
			key, identity, err := store.logicalProducerKey(uid, binding)
			if err != nil {
				t.Fatal(err)
			}
			value := struct {
				ProducerUID string                   `json:"producerUID"`
				Producer    fencing.MutationProducer `json:"producer"`
			}{ProducerUID: binding.ProducerUID, Producer: producer}
			if err := store.write(t.Context(), key, identity, value, version); err != nil {
				t.Fatal(err)
			}
			if err := e.recordLogicalRunClosure(t.Context(), uid, id); err == nil {
				t.Fatal("unbound closure accepted a producer with admitted or dispatch state")
			}
			root, _, err := store.ReadRoot(t.Context(), uid, id.SourceClusterID)
			if err != nil {
				t.Fatal(err)
			}
			if root.Ledger.Runs[id.RunID].LogicalClosure != nil {
				t.Fatal("invalid producer state produced a logical closure reference")
			}
		})
	}
}

func TestLogicalProfileSupervisorHandlerRegistersAndBindsWithoutRuntimeAgent(t *testing.T) {
	const (
		podName = "rhiza-0"
		podUID  = "pod-uid"
		stsUID  = "sts-uid"
		rootUID = "root-uid"
		source  = "source"
	)
	peerToken := "peer-token-for-rhiza-0"
	members, err := json.Marshal([]quepaxa.Member{{ID: quepaxa.NodeID(podName), URL: "http://rhiza-0:8080", PeerURL: "quic://rhiza-0:9090",
		PublicKey: quepaxa.PublicKey(network.PeerPublicKey(types.ClusterID(source), quepaxa.NodeID(podName), peerToken))}})
	if err != nil {
		t.Fatal(err)
	}
	peerTokens, _ := json.Marshal(map[string]string{podName: peerToken})
	payload, err := recovery.EmptyArchiveHeadBytes()
	if err != nil {
		t.Fatal(err)
	}
	var gcsMu sync.Mutex
	var gcsUploads atomic.Int32
	expectedOperationID := ""
	storedGCSObject := false
	storeGCS := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/b/archive-bucket/o") {
			gcsUploads.Add(1)
			if r.URL.Query().Get("ifGenerationMatch") != "0" || r.URL.Query().Get("name") != "archive/source/archive/head.bin" {
				t.Errorf("logical provider changed original name/condition: %s", r.URL.String())
			}
			gcsMu.Lock()
			storedGCSObject = true
			gcsMu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"kind": "storage#object", "bucket": "archive-bucket", "name": "archive/source/archive/head.bin",
				"generation": "31", "metageneration": "2", "size": strconv.Itoa(len(payload)),
				"metadata": map[string]string{"rhiza-operation-id": expectedOperationID, "rhiza-payload-sha256": hashBytes(payload)}})
			return
		}
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/b/archive-bucket/o/") {
			gcsMu.Lock()
			found := storedGCSObject
			gcsMu.Unlock()
			if !found {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"kind": "storage#object", "bucket": "archive-bucket", "name": "archive/source/archive/head.bin",
				"generation": "31", "metageneration": "2", "size": strconv.Itoa(len(payload)),
				"metadata": map[string]string{"rhiza-operation-id": expectedOperationID, "rhiza-payload-sha256": hashBytes(payload)}})
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(storeGCS.Close)
	image := "ghcr.io/example/rhiza@sha256:" + strings.Repeat("a", 64)
	containerSpec := object{"name": "rhiza", "image": image, "command": []any{"/usr/local/bin/rhiza"}, "securityContext": object{"runAsNonRoot": true, "allowPrivilegeEscalation": false, "readOnlyRootFilesystem": true, "capabilities": object{"drop": []any{"ALL"}}},
		"volumeMounts": []any{object{"name": "temporary", "mountPath": "/tmp"}, object{"name": "data", "mountPath": "/data"},
			object{"name": "restore-staging", "mountPath": "/ha-restore-staging", "readOnly": true},
			object{"name": "authority-token", "mountPath": "/var/run/secrets/rhiza-authority", "readOnly": true},
			object{"name": "authority-ca", "mountPath": "/fencer-ca", "readOnly": true}},
		"envFrom": []any{object{"configMapRef": object{"name": "rhiza-config"}}, object{"secretRef": object{"name": "peer-credentials"}}}}
	initSpec := object{"name": "restore-init", "image": image, "imagePullPolicy": "IfNotPresent", "command": []any{"/usr/local/bin/rhiza"}, "args": []any{"restore-init"},
		"envFrom":         []any{object{"configMapRef": object{"name": "rhiza-config"}}, object{"configMapRef": object{"name": "rhiza-object-store"}}},
		"env":             []any{object{"name": "RHIZA_HA_INIT_RESTORE_DIR", "value": "/ha-restore-staging/stage"}},
		"securityContext": object{"runAsNonRoot": true, "allowPrivilegeEscalation": false, "readOnlyRootFilesystem": true, "capabilities": object{"drop": []any{"ALL"}}},
		"volumeMounts":    []any{object{"name": "temporary", "mountPath": "/tmp"}, object{"name": "restore-staging", "mountPath": "/ha-restore-staging"}}}
	template := object{"metadata": object{"annotations": object{fencingProtocolAnnotation: "1", fencingRootAnnotation: rootUID}},
		"spec": object{"serviceAccountName": "rhiza-voter-reader", "automountServiceAccountToken": false,
			"containers": []any{containerSpec}, "initContainers": []any{initSpec},
			"volumes": []any{object{"name": "data", "emptyDir": object{}}, object{"name": "temporary", "emptyDir": object{}}, object{"name": "restore-staging", "emptyDir": object{}}}}}
	clone, err := json.Marshal(template["spec"])
	if err != nil {
		t.Fatal(err)
	}
	var podSpec object
	if err := json.Unmarshal(clone, &podSpec); err != nil {
		t.Fatal(err)
	}
	pod := object{"metadata": object{"name": podName, "uid": podUID, "annotations": object{fencingProtocolAnnotation: "1", fencingRootAnnotation: rootUID},
		"ownerReferences": []any{object{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "rhiza", "uid": stsUID, "controller": true}}}, "spec": podSpec}
	sts := object{"metadata": object{"name": "rhiza", "uid": stsUID}, "spec": object{"template": template, "volumeClaimTemplates": []any{}}}
	root := object{"apiVersion": "rhiza.mrchypark.dev/v1alpha1", "kind": "RhizaCluster", "metadata": object{"name": "rhiza", "uid": rootUID},
		"spec": object{"statefulSet": "rhiza", "container": "rhiza", "adoptClusterID": source}}
	config := object{"data": object{"RHIZA_CLUSTER_ID": source, "RHIZA_CLUSTER_MEMBERS": string(members), "RHIZA_OBJSTORE_PROVIDER": "gcs",
		"RHIZA_OBJSTORE_BUCKET": "archive-bucket", "RHIZA_OBJSTORE_PREFIX": "archive"}}
	secret := object{"data": object{"RHIZA_PEER_TOKENS": base64.StdEncoding.EncodeToString(peerTokens)}}
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/apis/authentication.k8s.io/v1/tokenreviews":
			_ = json.NewEncoder(w).Encode(object{"status": object{"authenticated": true, "audiences": []any{"rhiza-fencer"}, "user": object{
				"username": "system:serviceaccount:default:rhiza-voter-reader", "extra": object{
					"authentication.kubernetes.io/pod-uid": []any{podUID}, "authentication.kubernetes.io/pod-name": []any{podName}}}}})
		case "/api/v1/namespaces/default/pods/" + podName:
			_ = json.NewEncoder(w).Encode(pod)
		case "/api/v1/namespaces/default/configmaps/rhiza-config":
			_ = json.NewEncoder(w).Encode(config)
		case "/api/v1/namespaces/default/secrets/peer-credentials":
			_ = json.NewEncoder(w).Encode(secret)
		case "/apis/apps/v1/namespaces/default/statefulsets":
			_ = json.NewEncoder(w).Encode(object{"items": []any{sts}})
		case "/apis/apps/v1/namespaces/default/statefulsets/rhiza":
			_ = json.NewEncoder(w).Encode(sts)
		case "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/default/rhizaclusters":
			_ = json.NewEncoder(w).Encode(object{"items": []any{root}})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(api.Close)
	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	bucket := objstore.NewInMemBucket()
	t.Cleanup(func() { _ = bucket.Close() })
	store, err := NewAuthorityStateStore(bucket, "logical-http")
	if err != nil {
		t.Fatal(err)
	}
	e := &FencingExecutor{Kube: &Kubernetes{BaseURL: api.URL, Namespace: "default", TokenFile: tokenPath, Client: api.Client()},
		AuthorityState: store, RequireMutationJournal: true, AuthorityProfile: fencing.LogicalWriterProfileV1}
	e.ArchiveBucket = bucket
	gcsClient, err := storage.NewClient(t.Context(), option.WithEndpoint(storeGCS.URL+"/storage/v1/"), option.WithHTTPClient(storeGCS.Client()), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gcsClient.Close() })
	e.SourceMutationStorage = gcsClient
	if err := e.initializeRoot(t.Context(), stsUID, source); err != nil {
		t.Fatal(err)
	}
	identity := fencing.Identity{SourceClusterID: source, NodeID: podName, PodName: podName, PodUID: podUID, RunID: "logical-run"}
	call := func(message fencing.AuthorityMessage) *httptest.ResponseRecorder {
		body, marshalErr := json.Marshal(message)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		req := httptest.NewRequest(http.MethodPost, "/v1/supervisor", strings.NewReader(string(body)))
		req.Header.Set("Authorization", "Bearer pod-token")
		resp := httptest.NewRecorder()
		e.Handler().ServeHTTP(resp, req)
		return resp
	}
	registered := call(fencing.AuthorityMessage{Action: "register", Identity: identity})
	if registered.Code != http.StatusOK {
		t.Fatalf("logical registration handler failed without RuntimeAgent: status=%d body=%s", registered.Code, registered.Body)
	}
	var grant fencing.StartupGrant
	if err := json.Unmarshal(registered.Body.Bytes(), &grant); err != nil || grant.AuthorityProfile != fencing.LogicalWriterProfileV1 || grant.LogicalBinding == nil || grant.Identity != identity {
		t.Fatalf("registration did not return exact logical grant: grant=%+v err=%v", grant, err)
	}
	identity.WALIdentity = hashJSON("logical-http-wal")
	bound := call(fencing.AuthorityMessage{Action: "bind", Identity: identity})
	if bound.Code != http.StatusOK {
		t.Fatalf("logical bind handler failed: status=%d body=%s", bound.Code, bound.Body)
	}
	var receipt fencing.BindReceipt
	if err := json.Unmarshal(bound.Body.Bytes(), &receipt); err != nil || receipt.AuthorityProfile != fencing.LogicalWriterProfileV1 || receipt.LogicalBinding == nil || receipt.Producer == nil || receipt.Identity != identity {
		t.Fatalf("bind did not return exact logical receipt: receipt=%+v err=%v", receipt, err)
	}
	continued := call(fencing.AuthorityMessage{Action: "register", Identity: identity, RegistrationWALIdentity: identity.WALIdentity})
	if continued.Code != http.StatusOK {
		t.Fatalf("exact-WAL logical registration continuation failed: status=%d body=%s", continued.Code, continued.Body)
	}
	var continuedGrant fencing.StartupGrant
	if err := json.Unmarshal(continued.Body.Bytes(), &continuedGrant); err != nil || continuedGrant.AuthorityProfile != fencing.LogicalWriterProfileV1 || continuedGrant.LogicalBinding == nil || continuedGrant.LogicalBinding.WALIdentity != "" {
		t.Fatalf("continuation did not return the existing pre-bind grant: grant=%+v err=%v", continuedGrant, err)
	}
	wrongWAL := identity
	wrongWAL.WALIdentity = hashJSON("other-wal")
	rejectedContinuation := call(fencing.AuthorityMessage{Action: "register", Identity: wrongWAL, RegistrationWALIdentity: wrongWAL.WALIdentity})
	if rejectedContinuation.Code != http.StatusGone {
		t.Fatalf("logical continuation accepted a different WAL: status=%d", rejectedContinuation.Code)
	}
	genesisStatusRequest, err := json.Marshal(object{"identity": identity, "archiveGenesis": fencing.ArchiveGenesisStatusRequest{ArchiveKey: "archive/head.bin", ConfigID: recovery.ArchiveConfigID}})
	if err != nil {
		t.Fatal(err)
	}
	genesisStatusRequestHTTP := httptest.NewRequest(http.MethodPost, "/v1/source-mutations/genesis-status", bytes.NewReader(genesisStatusRequest))
	genesisStatusRequestHTTP.Header.Set("Authorization", "Bearer pod-token")
	genesisStatus := httptest.NewRecorder()
	e.Handler().ServeHTTP(genesisStatus, genesisStatusRequestHTTP)
	if genesisStatus.Code != http.StatusOK {
		t.Fatalf("logical first-start archive genesis status failed: status=%d body=%s", genesisStatus.Code, genesisStatus.Body)
	}
	var initialGenesis fencing.ArchiveGenesisStatus
	if err := json.Unmarshal(genesisStatus.Body.Bytes(), &initialGenesis); err != nil || !initialGenesis.CanInitializeEmpty {
		t.Fatalf("fresh logical root did not expose the authenticated empty-scope bootstrap contract: status=%+v err=%v", initialGenesis, err)
	}
	draft := fencing.MutationIntent{ContractVersion: fencing.MutationIntentVersion, StoreScopeHash: receipt.Producer.StoreScopeHash,
		OwnerStep: fencing.MutationOwnerStep{Owner: "rhiza", Workflow: "test", Step: "upload", Attempt: 1},
		Condition: fencing.MutationCondition{Mode: fencing.MutationCreateOnly}, Kind: "upload", Key: "archive/head.bin"}
	stageBody, err := json.Marshal(sourceStageReserveRequest{Identity: identity, Ticket: strings.Repeat("a", 64), Draft: draft})
	if err != nil {
		t.Fatal(err)
	}
	stageReserve := httptest.NewRequest(http.MethodPost, "/v1/source-stages/reserve", strings.NewReader(string(stageBody)))
	stageReserve.Header.Set("Authorization", "Bearer pod-token")
	stageReserveResponse := httptest.NewRecorder()
	e.Handler().ServeHTTP(stageReserveResponse, stageReserve)
	if stageReserveResponse.Code != http.StatusOK {
		t.Fatalf("logical source-stage reservation failed: status=%d body=%s", stageReserveResponse.Code, stageReserveResponse.Body)
	}
	digest := hashBytes(payload)
	identityHeader, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	stageSeal := httptest.NewRequest(http.MethodPost, "/v1/source-stages/"+strings.Repeat("a", 64)+"?digest="+digest+"&size="+strconv.Itoa(len(payload)), bytes.NewReader(payload))
	stageSeal.Header.Set("Authorization", "Bearer pod-token")
	stageSeal.Header.Set("Content-Type", "application/octet-stream")
	stageSeal.Header.Set(sourceStageIdentityHeader, base64.RawURLEncoding.EncodeToString(identityHeader))
	stageSealResponse := httptest.NewRecorder()
	e.Handler().ServeHTTP(stageSealResponse, stageSeal)
	if stageSealResponse.Code != http.StatusOK {
		t.Fatalf("logical source-stage sealing failed: status=%d body=%s", stageSealResponse.Code, stageSealResponse.Body)
	}
	var descriptor sourceStageDescriptor
	if err := json.Unmarshal(stageSealResponse.Body.Bytes(), &descriptor); err != nil {
		t.Fatal(err)
	}
	intent := draft
	intent.OperationID = fencing.MutationRequestID(receipt.Producer.ProducerUID, 1)
	expectedOperationID = intent.OperationID
	intent.PayloadRef, intent.PayloadDigest, intent.PayloadSize = descriptor.Ref, descriptor.Digest, descriptor.Size
	mutationBody, err := json.Marshal(object{"identity": identity, "producerUID": receipt.Producer.ProducerUID, "sequence": uint64(1), "intent": intent})
	if err != nil {
		t.Fatal(err)
	}
	mutationCall := func(route string, body []byte) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, route, strings.NewReader(string(body)))
		req.Header.Set("Authorization", "Bearer pod-token")
		resp := httptest.NewRecorder()
		e.Handler().ServeHTTP(resp, req)
		return resp
	}
	admitted := mutationCall("/v1/source-mutations/admit", mutationBody)
	if admitted.Code != http.StatusOK {
		t.Fatalf("logical mutation admission failed: status=%d body=%s", admitted.Code, admitted.Body)
	}
	logicalProducer, _, _, err := store.ReadLogicalProducer(t.Context(), stsUID, *receipt.LogicalBinding)
	if err != nil || logicalProducer.LastAdmitted != 1 || logicalProducer.Entries[1].Intent != intent || logicalProducer.Runtime != (fencing.RuntimeBinding{}) {
		t.Fatalf("logical admission did not persist exact profile-isolated intent: producer=%+v err=%v", logicalProducer, err)
	}
	execution := mutationCall("/v1/source-mutations/execute", mutationBody)
	if execution.Code != http.StatusOK || gcsUploads.Load() != 1 {
		t.Fatalf("logical mutation did not use the shared provider executor: status=%d uploads=%d body=%s", execution.Code, gcsUploads.Load(), execution.Body)
	}
	committedGenesisResponse := mutationCall("/v1/source-mutations/genesis-status", genesisStatusRequest)
	if committedGenesisResponse.Code != http.StatusOK {
		t.Fatalf("logical genesis status rejected the already-committed canonical reference: status=%d body=%s", committedGenesisResponse.Code, committedGenesisResponse.Body)
	}
	var committedGenesis fencing.ArchiveGenesisStatus
	if err := json.Unmarshal(committedGenesisResponse.Body.Bytes(), &committedGenesis); err != nil || committedGenesis.Ref == nil || committedGenesis.CanInitializeEmpty {
		t.Fatalf("logical genesis status did not return committed reference: status=%+v err=%v", committedGenesis, err)
	}
	nextIdentity := fencing.Identity{SourceClusterID: source, NodeID: "voter-b", RunID: "run-b"}
	nextBinding := fencing.LogicalWriterBindingV1{Version: fencing.LogicalWriterVersionV1, Profile: fencing.LogicalWriterProfileV1,
		SourceClusterID: source, NodeID: nextIdentity.NodeID, RunID: nextIdentity.RunID, ProducerUID: "logical-producer-b",
		StoreScopeHash: receipt.Producer.StoreScopeHash, Role: fencing.StartupRoleVoterActive}
	rootForLoser, rootVersion, err := store.ReadRoot(t.Context(), stsUID, source)
	if err != nil {
		t.Fatal(err)
	}
	if err := rootForLoser.Ledger.RegisterLogicalWithRole(nextIdentity, nextBinding); err != nil {
		t.Fatal(err)
	}
	if err := rootForLoser.Ledger.BindLogicalMutationScope(nextIdentity, nextBinding.StoreScopeHash, fencing.MutationExecutionProfile); err != nil {
		t.Fatal(err)
	}
	if err := rootForLoser.Ledger.BindLogicalMutationStage(nextIdentity, hashJSON("logical-producer-b-stage")); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteRoot(t.Context(), stsUID, source, rootForLoser, rootVersion); err != nil {
		t.Fatal(err)
	}
	if err := e.claimLogicalArchiveScopeForRegistration(t.Context(), stsUID, nextIdentity, nextBinding); err != nil {
		t.Fatalf("next logical voter could not reuse the committed canonical archive scope: %v", err)
	}
	if err := e.reserveLogicalMutationProducer(t.Context(), stsUID, nextBinding); err != nil {
		t.Fatal(err)
	}
	nextIdentity.WALIdentity = hashJSON("logical-producer-b-wal")
	if _, err := e.bindLogicalSupervisorWAL(t.Context(), stsUID, nextIdentity); err != nil {
		t.Fatalf("next logical voter WAL bind failed: %v", err)
	}
	rootForLoser, _, err = store.ReadRoot(t.Context(), stsUID, source)
	if err != nil {
		t.Fatal(err)
	}
	boundLoserBinding := *rootForLoser.Ledger.Runs[nextIdentity.RunID].LogicalBinding
	loserIntent := intent
	loserIntent.OperationID = fencing.MutationRequestID(nextBinding.ProducerUID, 1)
	if err := e.cancelLogicalArchiveGenesisLoser(t.Context(), stsUID, nextIdentity, boundLoserBinding, nextBinding.ProducerUID, 1, loserIntent); err != nil {
		t.Fatalf("logical archive-genesis loser tuple was not atomically consumed: %v", err)
	}
	loserProducer, loserProducerUID, _, err := store.ReadLogicalProducer(t.Context(), stsUID, boundLoserBinding)
	if err != nil || loserProducerUID != nextBinding.ProducerUID || loserProducer.LastAdmitted != 1 ||
		loserProducer.Entries[1].Resolution == nil || loserProducer.Entries[1].Resolution.Outcome != fencing.MutationRejected || !loserProducer.Entries[1].Resolution.AllCopiesTerminal {
		t.Fatalf("logical genesis loser is not terminally rejected: producer=%+v err=%v", loserProducer, err)
	}
	if err := e.cancelLogicalArchiveGenesisLoser(t.Context(), stsUID, nextIdentity, boundLoserBinding, nextBinding.ProducerUID, 1, loserIntent); err != nil {
		t.Fatalf("exact logical genesis loser retry was not idempotent: %v", err)
	}
	if _, err := e.change(t.Context(), stsUID, source, func(ledger *fencing.Ledger) error {
		return ledger.CloseNodes("remove-loser", hashJSON("remove-loser-request"), []string{nextIdentity.NodeID})
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.recordLogicalRunClosure(t.Context(), stsUID, nextIdentity); err != nil {
		t.Fatalf("logical Run with an exact terminal genesis cancellation did not close: %v", err)
	}
	closedLoserRoot, _, err := store.ReadRoot(t.Context(), stsUID, source)
	if err != nil || closedLoserRoot.Ledger.Runs[nextIdentity.RunID].LogicalClosure == nil {
		t.Fatalf("logical cancellation closure was not persisted: root=%+v err=%v", closedLoserRoot.Ledger, err)
	}
	logicalProducer, _, _, err = store.ReadLogicalProducer(t.Context(), stsUID, *receipt.LogicalBinding)
	if err != nil || logicalProducer.Entries[1].Resolution == nil || !logicalProducer.Entries[1].Resolution.AllCopiesTerminal || logicalProducer.Entries[1].Resolution.Outcome != fencing.MutationCommitted {
		t.Fatalf("logical provider result was not committed to its canonical producer: entry=%+v err=%v", logicalProducer.Entries[1], err)
	}
	if replay := mutationCall("/v1/source-mutations/execute", mutationBody); replay.Code != http.StatusOK || gcsUploads.Load() != 1 {
		t.Fatalf("exact execution retry redispatched or failed: status=%d uploads=%d body=%s", replay.Code, gcsUploads.Load(), replay.Body)
	}
	nextIntent := intent
	nextIntent.OperationID = fencing.MutationRequestID(receipt.Producer.ProducerUID, 2)
	nextBody, err := json.Marshal(object{"identity": identity, "producerUID": receipt.Producer.ProducerUID, "sequence": uint64(2), "intent": nextIntent})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.change(t.Context(), stsUID, source, func(ledger *fencing.Ledger) error {
		return ledger.CloseNodes("remove", hashJSON("remove-request"), []string{identity.NodeID})
	}); err != nil {
		t.Fatal(err)
	}
	denied := mutationCall("/v1/source-mutations/admit", nextBody)
	if denied.Code != http.StatusGone {
		t.Fatalf("logical source mutation admission survived root revocation: status=%d body=%s", denied.Code, denied.Body)
	}
	if err := e.recordLogicalRunClosure(t.Context(), stsUID, identity); err != nil {
		t.Fatalf("logical storage closure did not verify persisted receipt and GCS readback: %v", err)
	}
	closedRoot, _, err := store.ReadRoot(t.Context(), stsUID, source)
	if err != nil || closedRoot.Ledger.Runs[identity.RunID].LogicalClosure == nil {
		t.Fatalf("logical REMOVE did not retain verified storage-effects closure: root=%+v err=%v", closedRoot.Ledger, err)
	}
	if e.RuntimeAgent != nil {
		t.Fatal("test unexpectedly configured physical RuntimeAgent")
	}
}

func TestLogicalAuthorityRefusesLegacyRootConversion(t *testing.T) {
	ctx := t.Context()
	bucket := objstore.NewInMemBucket()
	t.Cleanup(func() { _ = bucket.Close() })
	store, err := NewAuthorityStateStore(bucket, "logical-authority-legacy")
	if err != nil {
		t.Fatal(err)
	}
	const uid, source = "legacy-sts", "legacy-source"
	if err := store.WriteRoot(ctx, uid, source, fenceLedger{StatefulSetUID: uid, Ledger: fencing.Ledger{SourceClusterID: source}}, nil); err != nil {
		t.Fatal(err)
	}
	e := &FencingExecutor{Kube: newMissingLegacyLedgerKube(t), AuthorityState: store,
		RequireMutationJournal: true, AuthorityProfile: fencing.LogicalWriterProfileV1}
	if err := e.initializeRoot(ctx, uid, source); err == nil {
		t.Fatal("logical profile selection converted an existing legacy root")
	}
	root, _, err := store.ReadRoot(ctx, uid, source)
	if err != nil || root.Ledger.Profile != "" || root.Ledger.RequireRuntimeBinding || root.Ledger.RequireMutationJournal {
		t.Fatalf("failed profile selection modified legacy root: %+v err=%v", root.Ledger, err)
	}
}

func TestLogicalProducerStoreKeyIsStableAcrossWALBindAndRootScoped(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	t.Cleanup(func() { _ = bucket.Close() })
	store, err := NewAuthorityStateStore(bucket, "logical-key")
	if err != nil {
		t.Fatal(err)
	}
	binding := fencing.LogicalWriterBindingV1{Version: fencing.LogicalWriterVersionV1, Profile: fencing.LogicalWriterProfileV1,
		SourceClusterID: "source", NodeID: "node", RunID: "run", Role: fencing.StartupRoleVoterActive,
		ProducerUID: "producer", StoreScopeHash: hashJSON("scope")}
	beforeKey, beforeID, err := store.logicalProducerKey("root-a", binding)
	if err != nil {
		t.Fatal(err)
	}
	binding.WALIdentity = hashJSON("wal")
	afterKey, afterID, err := store.logicalProducerKey("root-a", binding)
	if err != nil {
		t.Fatal(err)
	}
	otherRootKey, otherRootID, err := store.logicalProducerKey("root-b", binding)
	if err != nil {
		t.Fatal(err)
	}
	if beforeKey != afterKey || beforeID != afterID {
		t.Fatalf("one-time WAL bind changed stable producer key: %q/%q -> %q/%q", beforeKey, beforeID, afterKey, afterID)
	}
	if beforeKey == otherRootKey || beforeID == otherRootID {
		t.Fatal("logical producer key was not scoped by canonical root UID")
	}
}
