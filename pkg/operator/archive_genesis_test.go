package operator

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
	"google.golang.org/api/option"
)

func TestArchiveGenesisAdmissionOnlyAllowsExactCommittedTuple(t *testing.T) {
	intent := fencing.MutationIntent{
		Kind: "upload", Key: "archive/head.bin",
		Condition:   fencing.MutationCondition{Mode: fencing.MutationCreateOnly},
		OperationID: fencing.MutationRequestID("producer", 7),
	}
	ref := &fencing.ArchiveGenesisRef{Mutation: &fencing.ArchiveGenesisMutation{
		ProducerUID: "producer", Sequence: 7, Intent: intent,
	}}
	if err := validateArchiveGenesisAdmission(ref, "producer", 7, intent); err != nil {
		t.Fatalf("exact genesis retry was rejected: %v", err)
	}
	for _, tc := range []struct {
		name     string
		producer string
		sequence uint64
		intent   fencing.MutationIntent
	}{
		{name: "different producer", producer: "replacement", sequence: 7, intent: intent},
		{name: "different sequence", producer: "producer", sequence: 8, intent: intent},
		{name: "different payload tuple", producer: "producer", sequence: 7, intent: func() fencing.MutationIntent {
			changed := intent
			changed.PayloadDigest = "changed"
			return changed
		}()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateArchiveGenesisAdmission(ref, tc.producer, tc.sequence, tc.intent); err == nil {
				t.Fatal("different head create-only tuple was admitted after genesis")
			}
		})
	}
	ordinary := intent
	ordinary.Condition = fencing.MutationCondition{Mode: fencing.MutationMatchGeneration, Generation: 4}
	if err := validateArchiveGenesisAdmission(ref, "producer", 8, ordinary); err != nil {
		t.Fatalf("ordinary archive-head generation update was incorrectly blocked: %v", err)
	}
}

type archiveGenesisFixture struct {
	e          *FencingExecutor
	f          *mutationStoreFixture
	id         fencing.Identity
	runtime    fencing.RuntimeBinding
	producer   string
	stages     *SourceStageStore
	intent     fencing.MutationIntent
	payload    []byte
	storeScope AttestationStoreScope
}

func newArchiveGenesisFixture(t *testing.T) archiveGenesisFixture {
	t.Helper()
	e, f, id, runtime := newMutationStoreFixture(t)
	e.RequireMutationJournal = true
	if _, err := e.change(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID, func(l *fencing.Ledger) error {
		l.ArchiveGenesisState = fencing.ArchiveGenesisRegistrationPending
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	e.ArchiveBucket = objstore.NewInMemBucket()
	archiveBucket := e.ArchiveBucket
	t.Cleanup(func() { _ = archiveBucket.Close() })
	if _, err := e.reserveMutationProducer(t.Context(), runtime); err != nil {
		t.Fatal(err)
	}
	storeScope := AttestationStoreScope{Provider: "gcs", Bucket: "archive", Prefix: "root/source"}
	storeScopeHash := hashJSON(storeScope)
	stages, err := e.sourceStagesFor(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	id.WALIdentity = "wal-archive-genesis"
	if _, err := e.change(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID, func(l *fencing.Ledger) error {
		if err := l.BindMutationScope(id, storeScopeHash, fencing.MutationExecutionProfile); err != nil {
			return err
		}
		if err := l.BindMutationStage(id, stages.incarnation); err != nil {
			return err
		}
		return l.Bind(id)
	}); err != nil {
		t.Fatal(err)
	}
	_, err = e.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error { return p.Activate(id) })
	if err != nil {
		t.Fatal(err)
	}
	run, err := e.enrolledMutationRun(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	env := []any{
		object{"name": "RHIZA_CLUSTER_ID", "value": id.SourceClusterID},
		object{"name": "RHIZA_OBJSTORE_PROVIDER", "value": "gcs"},
		object{"name": "RHIZA_OBJSTORE_BUCKET", "value": "archive"},
		object{"name": "RHIZA_OBJSTORE_PREFIX", "value": "root"},
	}
	pod := supervisedTestPod()
	asObject(pod["metadata"])["name"] = id.NodeID
	asObject(pod["metadata"])["uid"] = id.PodUID
	asObject(pod["metadata"])["ownerReferences"] = []any{object{"kind": "StatefulSet", "name": "rhiza", "uid": runtime.StatefulSetUID, "controller": true}}
	asObject(list(nested(pod, "spec", "containers"))[0])["env"] = env
	sts := object{"metadata": object{"uid": runtime.StatefulSetUID}, "spec": object{"template": object{"spec": object{"containers": []any{object{"name": "rhiza", "env": env}}}}}}
	f.mu.Lock()
	f.authPod, f.authStatefulSet = pod, sts
	f.objects["/apis/apps/v1/namespaces/test/statefulsets"] = object{"items": []any{sts}}
	f.mu.Unlock()
	if err := e.claimArchiveScopeForRegistration(t.Context(), runtime.StatefulSetUID, id, runtime); err != nil {
		t.Fatal(err)
	}
	payload, err := recovery.EmptyArchiveHeadBytes()
	if err != nil {
		t.Fatal(err)
	}
	draft := stageStoreIntent()
	draft.StoreScopeHash = storeScopeHash
	draft.Key = "archive/head.bin"
	ticket := strings.Repeat("g", 64)
	if err := stages.Reserve(run.MutationProducerUID, ticket, draft); err != nil {
		t.Fatal(err)
	}
	sealed, err := stages.Seal(run.MutationProducerUID, ticket, hashBytes(payload), int64(len(payload)), bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	intent := draft
	intent.OperationID = fencing.MutationRequestID(run.MutationProducerUID, 1)
	intent.PayloadRef, intent.PayloadDigest, intent.PayloadSize = sealed.Ref, sealed.Digest, sealed.Size
	return archiveGenesisFixture{e: e, f: f, id: id, runtime: runtime, producer: run.MutationProducerUID, stages: stages, intent: intent, payload: payload, storeScope: storeScope}
}

func (f archiveGenesisFixture) admit(t *testing.T, sequence uint64, intent fencing.MutationIntent) {
	t.Helper()
	_, err := f.e.changeMutationProducer(t.Context(), f.runtime, func(p *fencing.MutationProducer) error { return p.Admit(sequence, intent) })
	if err != nil {
		t.Fatal(err)
	}
}

func (f archiveGenesisFixture) setStorage(t *testing.T, endpoint string, client *http.Client) {
	t.Helper()
	storageClient, err := storage.NewClient(t.Context(), option.WithEndpoint(endpoint+"/storage/v1/"), option.WithHTTPClient(client), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = storageClient.Close() })
	f.e.SourceMutationStorage = storageClient
}

func postArchiveGenesisStatus(t *testing.T, e *FencingExecutor, id fencing.Identity) (fencing.ArchiveGenesisStatus, int) {
	t.Helper()
	body, err := json.Marshal(struct {
		Identity fencing.Identity                    `json:"identity"`
		Genesis  fencing.ArchiveGenesisStatusRequest `json:"archiveGenesis"`
	}{Identity: id, Genesis: fencing.ArchiveGenesisStatusRequest{ArchiveKey: "archive/head.bin", ConfigID: recovery.ArchiveConfigID}})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/source-mutations/genesis-status", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer test")
	recorder := httptest.NewRecorder()
	e.handleSourceMutation(recorder, req)
	var status fencing.ArchiveGenesisStatus
	if recorder.Code == http.StatusOK {
		if err := json.Unmarshal(recorder.Body.Bytes(), &status); err != nil {
			t.Fatal(err)
		}
	}
	return status, recorder.Code
}

func TestArchiveGenesisStatusResumesAdmittedTupleAndCommitsRefsBeforeDispatch(t *testing.T) {
	var executor *FencingExecutor
	var runtime fencing.RuntimeBinding
	var uploads atomic.Int32
	payload := recoveryEmptyHeadBytes(t)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodPost || r.URL.Query().Get("ifGenerationMatch") != "0" || r.URL.Query().Get("name") != "root/source/archive/head.bin" {
			t.Errorf("unexpected genesis upload request: %s %s", r.Method, r.URL.String())
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		if executor == nil {
			t.Error("genesis request raced fixture setup")
			return
		}
		scope, _, err := executor.archiveGenesisScope(r.Context(), runtime)
		if err != nil {
			t.Errorf("derive scope before provider dispatch: %v", err)
		}
		claim, _, err := executor.AuthorityState.ReadArchiveScopeClaim(r.Context(), scope)
		if err != nil || claim.GenesisRef == nil || claim.GenesisRef.Mutation == nil {
			t.Errorf("provider dispatch occurred before durable scope claim: %+v %v", claim, err)
			http.Error(w, "missing genesis claim", http.StatusInternalServerError)
			return
		}
		root, err := executor.read(r.Context(), claim.OwnerStatefulSetUID, claim.OwnerSourceClusterID)
		if err != nil || root.Ledger.ArchiveGenesisRef == nil || !sameArchiveGenesisRef(claim.GenesisRef, root.Ledger.ArchiveGenesisRef) {
			t.Errorf("provider dispatch occurred before matching root reference: %+v %v", root.Ledger.ArchiveGenesisRef, err)
		}
		producer, _, _, err := executor.AuthorityState.ReadProducer(r.Context(), runtime)
		if err != nil || producer.Entries[claim.GenesisRef.Mutation.Sequence].Intent != claim.GenesisRef.Mutation.Intent {
			t.Errorf("provider dispatch occurred without the exact admitted tuple: %+v %v", producer, err)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Contains(body, payload) {
			t.Errorf("GCS body did not contain exact empty HEAD: %v", err)
		}
		uploads.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"kind": "storage#object", "bucket": "archive", "name": "root/source/archive/head.bin",
			"generation": "21", "metageneration": "1", "size": strconv.Itoa(len(payload)),
			"metadata": map[string]string{"rhiza-operation-id": claim.GenesisRef.Mutation.Intent.OperationID, "rhiza-payload-sha256": claim.GenesisRef.Mutation.Intent.PayloadDigest},
		})
	}))
	t.Cleanup(server.Close)
	f := newArchiveGenesisFixture(t)
	executor = f.e
	runtime = f.runtime
	f.setStorage(t, server.URL, server.Client())
	f.admit(t, 1, f.intent)
	if _, err := f.e.changeMutationProducer(t.Context(), f.runtime, func(p *fencing.MutationProducer) error {
		return p.Close("test-closed-before-genesis-ref", strings.Repeat("c", 64))
	}); err != nil {
		t.Fatal(err)
	}
	status, code := postArchiveGenesisStatus(t, f.e, f.id)
	if code != http.StatusOK || status.Ref == nil || status.Mutation == nil || status.Mutation.Entry == nil || status.Mutation.Entry.Resolution == nil || status.Mutation.Entry.Resolution.Outcome != fencing.MutationCommitted {
		t.Fatalf("status did not resume and verify the admitted genesis tuple: code=%d status=%+v", code, status)
	}
	if uploads.Load() != 1 {
		t.Fatalf("status issued %d provider writes, want exactly one", uploads.Load())
	}
}

func TestArchiveGenesisLosingAdmittedTupleIsDurablyCancelledBeforeDispatch(t *testing.T) {
	var executor *FencingExecutor
	var runtime fencing.RuntimeBinding
	var first fencing.MutationIntent
	payload := recoveryEmptyHeadBytes(t)
	var uploads atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodPost || r.URL.Query().Get("ifGenerationMatch") != "0" || r.URL.Query().Get("name") != "root/source/archive/head.bin" {
			t.Errorf("unexpected winner upload request: %s %s", r.Method, r.URL.String())
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		if executor == nil {
			t.Error("genesis request raced fixture setup")
			return
		}
		scope, _, err := executor.archiveGenesisScope(r.Context(), runtime)
		claim, _, readErr := executor.AuthorityState.ReadArchiveScopeClaim(r.Context(), scope)
		if err != nil || readErr != nil || claim.GenesisRef == nil || claim.GenesisRef.Mutation == nil || claim.GenesisRef.Mutation.Intent != first {
			t.Errorf("provider dispatched before exact winning ref was durable: %+v %v %v", claim, err, readErr)
			http.Error(w, "missing genesis claim", http.StatusInternalServerError)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Contains(body, payload) {
			t.Errorf("winner upload omitted sealed empty HEAD: %v", err)
		}
		uploads.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"kind": "storage#object", "bucket": "archive", "name": "root/source/archive/head.bin",
			"generation": "21", "metageneration": "1", "size": strconv.Itoa(len(payload)),
			"metadata": map[string]string{"rhiza-operation-id": first.OperationID, "rhiza-payload-sha256": first.PayloadDigest},
		})
	}))
	t.Cleanup(server.Close)
	f := newArchiveGenesisFixture(t)
	executor = f.e
	runtime = f.runtime
	f.setStorage(t, server.URL, server.Client())
	first = f.intent
	if err := f.e.executeSourceMutationRequest(t.Context(), "Bearer test", f.id, sourceMutationRequest{ProducerUID: f.producer, Sequence: 1, Intent: first}, true); err != nil {
		t.Fatalf("normal admission did not commit and execute its exact genesis tuple: %v", err)
	}
	if uploads.Load() != 1 {
		t.Fatalf("winning candidate issued %d uploads, want one", uploads.Load())
	}
	second := f.intent
	second.OperationID = fencing.MutationRequestID(f.producer, 2)
	ticket := strings.Repeat("h", 64)
	draft := second
	draft.OperationID, draft.PayloadRef, draft.PayloadDigest, draft.PayloadSize = "", "", "", 0
	if err := f.stages.Reserve(f.producer, ticket, draft); err != nil {
		t.Fatal(err)
	}
	sealed, err := f.stages.Seal(f.producer, ticket, hashBytes(f.payload), int64(len(f.payload)), bytes.NewReader(f.payload))
	if err != nil {
		t.Fatal(err)
	}
	second.PayloadRef, second.PayloadDigest, second.PayloadSize = sealed.Ref, sealed.Digest, sealed.Size
	f.admit(t, 2, second)
	err = f.e.executeSourceMutationRequest(t.Context(), "Bearer test", f.id, sourceMutationRequest{ProducerUID: f.producer, Sequence: 2, Intent: second}, true)
	if !errors.Is(err, errArchiveGenesisClaimed) {
		t.Fatalf("losing admitted tuple was not rejected: %v", err)
	}
	producer, _, _, err := f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	entry := producer.Entries[2]
	if entry.Intent != second || entry.Preparation != nil || len(entry.Dispatches) != 0 || entry.Resolution == nil || entry.Resolution.Outcome != fencing.MutationRejected || !entry.Resolution.AllCopiesTerminal || !entry.DispatchClosed {
		t.Fatalf("losing candidate was not terminally excluded without dispatch: %+v", entry)
	}
	if winner := producer.Entries[1]; winner.Resolution == nil || winner.Resolution.Outcome != fencing.MutationCommitted || len(winner.Dispatches) != 1 {
		t.Fatalf("claim winner did not retain its sole committed dispatch: %+v", winner)
	}
	proofKey := "archive-genesis-cancellations/" + entry.Resolution.ReceiptDigest
	evidence, err := f.e.authorityEvidenceFor(f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	reader, _, err := evidence.Read(t.Context(), proofKey)
	if err != nil {
		t.Fatalf("losing candidate cancellation receipt is not durable: %v", err)
	}
	body, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		t.Fatal(err)
	}
	var proof archiveGenesisCancellationProof
	if err := json.Unmarshal(body, &proof); err != nil || proof.ProducerUID != f.producer || proof.Sequence != 2 || proof.Intent != second || proof.WinningGenesis.Mutation == nil || proof.WinningGenesis.Mutation.Sequence != 1 {
		t.Fatalf("cancellation receipt does not bind exact loser and winner: %+v %v", proof, err)
	}
}

func recoveryEmptyHeadBytes(t *testing.T) []byte {
	t.Helper()
	data, err := recovery.EmptyArchiveHeadBytes()
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestArchiveGenesisStatusDoesNotAdmitMissingOrMismatchedReferencedTuple(t *testing.T) {
	for _, tc := range []struct {
		name     string
		sequence uint64
	}{
		{name: "missing tuple", sequence: 1},
		{name: "mismatched tuple", sequence: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) }))
			t.Cleanup(server.Close)
			f := newArchiveGenesisFixture(t)
			f.setStorage(t, server.URL, server.Client())
			scope, scopeHash, err := f.e.archiveGenesisScope(t.Context(), f.runtime)
			if err != nil {
				t.Fatal(err)
			}
			intent := f.intent
			intent.OperationID = fencing.MutationRequestID(f.producer, tc.sequence)
			ref := fencing.ArchiveGenesisRef{Version: fencing.ArchiveGenesisVersion, ConfigID: recovery.ArchiveConfigID, ScopeHash: scopeHash,
				Mutation: &fencing.ArchiveGenesisMutation{ProducerUID: f.producer, Sequence: tc.sequence, Intent: intent}}
			if err := f.e.commitArchiveGenesisRef(t.Context(), f.runtime.StatefulSetUID, f.runtime.SourceClusterID, scope, ref); err != nil {
				t.Fatal(err)
			}
			_, code := postArchiveGenesisStatus(t, f.e, f.id)
			if code != http.StatusAccepted {
				t.Fatalf("unverified journal tuple was not kept pending: status=%d", code)
			}
			producer, _, _, err := f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
			if err != nil || producer.LastAdmitted != 0 || len(producer.Entries) != 0 {
				t.Fatalf("status synthesized an admission for the referenced tuple: last=%d entries=%d err=%v", producer.LastAdmitted, len(producer.Entries), err)
			}
		})
	}
}

func TestArchiveGenesisStatusDoesNotCallHeadlessForkFreshEmpty(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.Contains(r.URL.EscapedPath(), "fork%2Fintent.json") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"kind":"storage#object","bucket":"archive","name":"root/source/fork/intent.json","generation":"7","metageneration":"1","size":"1"}`)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(server.Close)
	f := newArchiveGenesisFixture(t)
	if err := f.e.ArchiveBucket.Upload(t.Context(), "root/source/fork/intent.json", strings.NewReader("{}")); err != nil {
		t.Fatal(err)
	}
	f.setStorage(t, server.URL, server.Client())
	_, code := postArchiveGenesisStatus(t, f.e, f.id)
	if code != http.StatusConflict {
		t.Fatalf("headless fork was advertised as a fresh empty archive: status=%d", code)
	}
}
