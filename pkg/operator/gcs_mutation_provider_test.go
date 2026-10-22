package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"google.golang.org/api/option"
)

func TestGCSMutationProviderUsesOneConditionalWireRequestAndExactMetadata(t *testing.T) {
	payload := []byte(`{"membership":"exact staged bytes"}`)
	intent := fencing.MutationIntent{Kind: "upload", OperationID: "producer-sequence", PayloadDigest: hashBytes(payload), PayloadSize: int64(len(payload))}
	var requests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/b/archive/o") {
			t.Errorf("unexpected GCS request: %s %s", r.Method, r.URL.String())
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		if r.URL.Query().Get("ifGenerationMatch") != "14" || r.URL.Query().Get("name") != "root/source/membership.json" || r.URL.Query().Get("uploadType") == "resumable" {
			t.Errorf("GCS request lost original condition/name or used a resumable session: %s", r.URL.String())
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read GCS request body: %v", err)
		}
		for _, expected := range [][]byte{payload, []byte(intent.OperationID), []byte(intent.PayloadDigest)} {
			if !bytes.Contains(body, expected) {
				t.Errorf("GCS wire body omitted exact payload or immutable operation metadata")
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"kind": "storage#object", "bucket": "archive", "name": "root/source/membership.json",
			"generation": "21", "metageneration": "3", "size": strconv.Itoa(len(payload)),
			"metadata": map[string]string{"rhiza-operation-id": intent.OperationID, "rhiza-payload-sha256": intent.PayloadDigest},
		})
	}))
	defer server.Close()

	client, err := storage.NewClient(context.Background(), option.WithEndpoint(server.URL+"/storage/v1/"), option.WithHTTPClient(server.Client()), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	provider, err := newGCSMutationProvider(client, AttestationStoreScope{Provider: "gcs", Bucket: "archive", Prefix: "root/source"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := provider.upload(context.Background(), "membership.json", 14, intent, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 1 || result.Outcome != fencing.MutationCommitted || result.Generation != 21 || result.Metageneration != 3 || result.PayloadDigest != intent.PayloadDigest || !result.AllCopiesTerminal {
		t.Fatalf("GCS result did not bind the one wire commit: requests=%d result=%+v", requests.Load(), result)
	}
}

func TestGCSMutationProviderRejectsEmulatorEndpoint(t *testing.T) {
	t.Setenv("STORAGE_EMULATOR_HOST", "http://127.0.0.1:9999")
	client, err := storage.NewClient(context.Background(), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := newGCSMutationProvider(client, AttestationStoreScope{Provider: "gcs", Bucket: "archive", Prefix: "root/source"}); err == nil || !strings.Contains(err.Error(), "rejects STORAGE_EMULATOR_HOST") {
		t.Fatalf("emulator endpoint accepted for independent mutations: %v", err)
	}
}

func TestGCSMutationProviderRecoveryRequiresExactCommittedMetadata(t *testing.T) {
	payload := []byte("sealed recovery payload")
	intent := fencing.MutationIntent{Kind: "upload", OperationID: "producer-sequence", PayloadDigest: hashBytes(payload), PayloadSize: int64(len(payload))}
	for _, tc := range []struct {
		name    string
		attrs   map[string]any
		missing bool
	}{
		{name: "wrong operation", attrs: map[string]any{"generation": "21", "size": strconv.Itoa(len(payload)), "metadata": map[string]string{"rhiza-operation-id": "other", "rhiza-payload-sha256": intent.PayloadDigest}}},
		{name: "wrong digest", attrs: map[string]any{"generation": "21", "size": strconv.Itoa(len(payload)), "metadata": map[string]string{"rhiza-operation-id": intent.OperationID, "rhiza-payload-sha256": "different"}}},
		{name: "not newer than preparation", attrs: map[string]any{"generation": "21", "size": strconv.Itoa(len(payload)), "metadata": map[string]string{"rhiza-operation-id": intent.OperationID, "rhiza-payload-sha256": intent.PayloadDigest}}},
		{name: "absent object", missing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Errorf("unexpected request %s", r.Method)
					http.Error(w, "unexpected request", http.StatusMethodNotAllowed)
					return
				}
				if tc.missing {
					http.Error(w, "missing", http.StatusNotFound)
					return
				}
				attrs := map[string]any{"kind": "storage#object", "bucket": "archive", "name": "root/source/key"}
				for key, value := range tc.attrs {
					attrs[key] = value
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(attrs)
			}))
			defer server.Close()
			client, err := storage.NewClient(t.Context(), option.WithEndpoint(server.URL+"/storage/v1/"), option.WithHTTPClient(server.Client()), option.WithoutAuthentication())
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			provider, err := newGCSMutationProvider(client, AttestationStoreScope{Provider: "gcs", Bucket: "archive", Prefix: "root/source"})
			if err != nil {
				t.Fatal(err)
			}
			preparation := fencing.MutationPreparation{Generation: 20}
			if tc.name == "not newer than preparation" {
				preparation.Generation = 21
			}
			result, found, err := provider.committed(t.Context(), "key", intent, preparation)
			if err != nil || found || result.AllCopiesTerminal {
				t.Fatalf("nonmatching object was treated as a terminal commit: result=%+v found=%t err=%v", result, found, err)
			}
		})
	}
}

func TestGCSMutationTerminalUploadsCompactNormalAndRecoveredCommits(t *testing.T) {
	const recoveredSequence = uint64(70)
	objects := make(map[string]map[string]any)
	intents := make(map[string]fencing.MutationIntent)
	payloads := make(map[string][]byte)
	var mu sync.Mutex
	var uploads atomic.Int32
	var lostReply atomic.Bool
	var recoveredOperation string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const getPrefix = "/storage/v1/b/archive/o/"
		if r.Method == http.MethodGet {
			name := strings.TrimPrefix(r.URL.Path, getPrefix)
			if name == r.URL.Path {
				name = strings.TrimPrefix(r.URL.EscapedPath(), getPrefix)
			}
			mu.Lock()
			attrs, ok := objects[name]
			mu.Unlock()
			if !ok {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(attrs)
			return
		}
		if r.Method != http.MethodPost {
			t.Errorf("unexpected GCS method %s", r.Method)
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
			return
		}
		name := r.URL.Query().Get("name")
		mu.Lock()
		intent, ok := intents[name]
		payload, payloadOK := payloads[name]
		recoverOperation := recoveredOperation
		mu.Unlock()
		if !ok || !payloadOK || r.URL.Query().Get("ifGenerationMatch") != "0" {
			t.Errorf("GCS upload did not match staged create-only object %q", name)
			http.Error(w, "unexpected object", http.StatusBadRequest)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Contains(body, payload) || !bytes.Contains(body, []byte(intent.OperationID)) || !bytes.Contains(body, []byte(intent.PayloadDigest)) {
			t.Errorf("GCS upload differs from exact staged tuple for %q: %v", name, err)
			http.Error(w, "tuple mismatch", http.StatusBadRequest)
			return
		}
		generation := strconv.FormatInt(1000+int64(uploads.Load()+1), 10)
		attrs := map[string]any{
			"kind": "storage#object", "bucket": "archive", "name": name,
			"generation": generation, "metageneration": "1", "size": strconv.Itoa(len(payload)),
			"metadata": map[string]string{"rhiza-operation-id": intent.OperationID, "rhiza-payload-sha256": intent.PayloadDigest},
		}
		mu.Lock()
		objects[name] = attrs
		mu.Unlock()
		uploads.Add(1)
		if intent.OperationID == recoverOperation && lostReply.CompareAndSwap(false, true) {
			http.Error(w, "committed response lost", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(attrs)
	}))
	defer server.Close()
	client, err := storage.NewClient(t.Context(), option.WithEndpoint(server.URL+"/storage/v1/"), option.WithHTTPClient(server.Client()), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	provider, err := newGCSMutationProvider(client, AttestationStoreScope{Provider: "gcs", Bucket: "archive", Prefix: "root/source"})
	if err != nil {
		t.Fatal(err)
	}
	e, runtime, firstIntent, firstPayload, scope, receipts := mutationExecutionFixture(t)
	state, err := e.read(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	producerUID := state.Ledger.Runs[runtime.RunID].MutationProducerUID
	mu.Lock()
	recoveredOperation = fencing.MutationRequestID(producerUID, recoveredSequence)
	mu.Unlock()

	for sequence := uint64(1); sequence <= 130; sequence++ {
		if sequence > 1 {
			if err := e.compactSourceMutationJournal(t.Context(), runtime, sequence); err != nil {
				t.Fatalf("pre-admission compaction at %d: %v", sequence, err)
			}
			draft := firstIntent
			draft.OperationID = ""
			draft.Key = fmt.Sprintf("bulk/%03d.json", sequence)
			draft.OwnerStep.Step = fmt.Sprintf("bulk-%03d", sequence)
			draft.PayloadRef, draft.PayloadDigest, draft.PayloadSize = "", "", 0
			payload := []byte(fmt.Sprintf("payload-%03d", sequence))
			ticket := fmt.Sprintf("%064x", sequence)
			if err := e.SourceStages.Reserve(producerUID, ticket, draft); err != nil {
				t.Fatalf("reserve sequence %d: %v", sequence, err)
			}
			sealed, err := e.SourceStages.Seal(producerUID, ticket, hashBytes(payload), int64(len(payload)), bytes.NewReader(payload))
			if err != nil {
				t.Fatalf("seal sequence %d: %v", sequence, err)
			}
			intent := draft
			intent.OperationID = fencing.MutationRequestID(producerUID, sequence)
			intent.PayloadRef, intent.PayloadDigest, intent.PayloadSize = sealed.Ref, sealed.Digest, sealed.Size
			if _, err := e.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error { return p.Admit(sequence, intent) }); err != nil {
				t.Fatalf("admit sequence %d: %v", sequence, err)
			}
			name := "root/source/" + intent.Key
			mu.Lock()
			intents[name], payloads[name] = intent, payload
			mu.Unlock()
		} else {
			name := "root/source/" + firstIntent.Key
			mu.Lock()
			intents[name], payloads[name] = firstIntent, firstPayload
			mu.Unlock()
		}

		intent := firstIntent
		if sequence > 1 {
			mu.Lock()
			intent = intents["root/source/"+fmt.Sprintf("bulk/%03d.json", sequence)]
			mu.Unlock()
		}
		err := e.ExecuteSourceMutation(t.Context(), runtime, producerUID, sequence, intent, scope, provider, receipts)
		if sequence == recoveredSequence {
			if !errors.Is(err, ErrFencePending) {
				t.Fatalf("lost response at sequence %d was not kept pending: %v", sequence, err)
			}
			err = e.ExecuteSourceMutation(t.Context(), runtime, producerUID, sequence, intent, scope, provider, receipts)
		}
		if err != nil {
			t.Fatalf("execute sequence %d: %v", sequence, err)
		}
	}
	if uploads.Load() != 130 {
		t.Fatalf("recovered request was physically redispatched: uploads=%d", uploads.Load())
	}
	producer, _, _, err := e.AuthorityState.ReadProducer(t.Context(), runtime)
	if err != nil || producer.LastAdmitted != 130 || producer.CompactedThrough < 64 || len(producer.Entries) > fencing.MaxMutationJournalEntries || producer.ArchiveHead == nil {
		t.Fatalf("terminal GCS receipts did not compact the over-bound journal: admitted=%d compacted=%d entries=%d archive=%v err=%v", producer.LastAdmitted, producer.CompactedThrough, len(producer.Entries), producer.ArchiveHead, err)
	}
	terminalArchived := map[uint64]bool{1: false, recoveredSequence: false}
	run, err := e.enrolledMutationRun(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	for reference := producer.ArchiveHead; reference != nil; {
		plan, err := e.readMutationArchive(t.Context(), *reference, runtime, producerUID, run.Identity.WALIdentity)
		if err != nil {
			t.Fatalf("read compacted GCS mutation archive: %v", err)
		}
		for sequence := range terminalArchived {
			if entry, ok := plan.Entries[sequence]; ok {
				if entry.Resolution == nil || !entry.Resolution.AllCopiesTerminal {
					t.Fatalf("archived sequence %d lacks terminal-copy proof: %+v", sequence, entry)
				}
				terminalArchived[sequence] = true
			}
		}
		reference = plan.Previous
	}
	for sequence, found := range terminalArchived {
		if !found {
			t.Fatalf("sequence %d was not retained in verified compacted history", sequence)
		}
	}
	entry := producer.Entries[130]
	if entry.Resolution == nil || !entry.Resolution.AllCopiesTerminal {
		t.Fatalf("sequence 130 lacks durable terminal-copy evidence: %+v", entry)
	}
}

func TestGCSUnknownUploadReadbackCannotAcknowledgeOrReleaseGuard(t *testing.T) {
	for _, tc := range []struct {
		name    string
		missing bool
	}{
		{name: "object missing", missing: true},
		{name: "object metadata differs"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, runtime, intent, _, scope, receipts := mutationExecutionFixture(t)
			state, err := e.read(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID)
			if err != nil {
				t.Fatal(err)
			}
			producerUID := state.Ledger.Runs[runtime.RunID].MutationProducerUID
			var uploads, readbacks atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodPost:
					uploads.Add(1)
					http.Error(w, "response lost after uncertain dispatch", http.StatusServiceUnavailable)
				case http.MethodGet:
					readbacks.Add(1)
					if tc.missing {
						http.Error(w, "object not found", http.StatusNotFound)
						return
					}
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(map[string]any{
						"kind": "storage#object", "bucket": "archive", "name": "root/source/" + intent.Key,
						"generation": "2000", "metageneration": "1", "size": strconv.FormatInt(intent.PayloadSize, 10),
						"metadata": map[string]string{"rhiza-operation-id": "different-operation", "rhiza-payload-sha256": intent.PayloadDigest},
					})
				default:
					t.Errorf("unexpected GCS method %s", r.Method)
					http.Error(w, "unexpected request", http.StatusMethodNotAllowed)
				}
			}))
			defer server.Close()
			client, err := storage.NewClient(t.Context(), option.WithEndpoint(server.URL+"/storage/v1/"), option.WithHTTPClient(server.Client()), option.WithoutAuthentication())
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			provider, err := newGCSMutationProvider(client, scope)
			if err != nil {
				t.Fatal(err)
			}
			if err := e.ExecuteSourceMutation(t.Context(), runtime, producerUID, 1, intent, scope, provider, receipts); !errors.Is(err, ErrFencePending) {
				t.Fatalf("uncertain upload was not kept pending: %v", err)
			}
			if err := e.ExecuteSourceMutation(t.Context(), runtime, producerUID, 1, intent, scope, provider, receipts); !errors.Is(err, ErrFencePending) {
				t.Fatalf("nonmatching readback was acknowledged: %v", err)
			}
			if uploads.Load() != 1 || readbacks.Load() != 1 {
				t.Fatalf("unknown dispatch was resent or readback skipped: uploads=%d readbacks=%d", uploads.Load(), readbacks.Load())
			}
			lookup, err := e.lookupMutation(t.Context(), runtime.StatefulSetUID, fencing.Identity{SourceClusterID: runtime.SourceClusterID, NodeID: runtime.NodeID, PodUID: runtime.PodUID, RunID: runtime.RunID, WALIdentity: "wal"}, fencing.MutationLookupRequest{ProducerUID: producerUID, Sequence: 1, Intent: intent})
			dispatchesUnresolved := lookup.Entry != nil && len(lookup.Entry.Dispatches) == 1
			if lookup.Entry != nil {
				for _, dispatch := range lookup.Entry.Dispatches {
					dispatchesUnresolved = dispatchesUnresolved && dispatch.ReceiptDigest == ""
				}
			}
			if err != nil || lookup.Entry == nil || lookup.Entry.Resolution != nil || !dispatchesUnresolved {
				t.Fatalf("unknown dispatch gained a resolution/receipt: %+v err=%v", lookup, err)
			}
			owner := hashJSON([]any{runtime, "wal", producerUID, uint64(1), intent.OperationID})
			lock, err := readMutationKeyForTest(t.Context(), e, intent.StoreScopeHash, intent.Key)
			if err != nil || lock.Owner != owner {
				t.Fatalf("unknown outcome released its canonical key guard: %+v err=%v", lock, err)
			}
			if _, err := receipts.get("receipt-" + owner); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("unknown outcome unexpectedly persisted an execution receipt: err=%v", err)
			}
			closed, err := e.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error { return p.Close("close", hashBytes([]byte("close"))) })
			if err != nil || closed.BarrierComplete() {
				t.Fatalf("unresolved dispatch passed the close barrier: complete=%t err=%v", closed.BarrierComplete(), err)
			}
		})
	}
}

func TestGCSMutationProviderRejectsNoncanonicalKeyBeforePathNormalization(t *testing.T) {
	client, err := storage.NewClient(context.Background(), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	provider, err := newGCSMutationProvider(client, AttestationStoreScope{Provider: "gcs", Bucket: "archive", Prefix: "root/source"})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"nested/../target", "../source/target", "nested//target", "/target", "."} {
		if object := provider.object(key); object != nil {
			t.Errorf("noncanonical key %q was normalized into a provider target", key)
		}
	}
}

func TestGCSMutationProviderNeverRetriesAnUncertainSingleRequest(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		http.Error(w, "ambiguous provider response", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client, err := storage.NewClient(context.Background(), option.WithEndpoint(server.URL+"/storage/v1/"), option.WithHTTPClient(server.Client()), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	provider, err := newGCSMutationProvider(client, AttestationStoreScope{Provider: "gcs", Bucket: "archive", Prefix: "root/source"})
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("payload")
	intent := fencing.MutationIntent{Kind: "upload", OperationID: "producer-sequence", PayloadDigest: hashBytes(payload), PayloadSize: int64(len(payload))}
	if _, err := provider.upload(context.Background(), "key", 0, intent, bytes.NewReader(payload)); err == nil {
		t.Fatal("provider failure was accepted as committed")
	}
	if requests.Load() != 1 {
		t.Fatalf("single-request profile retried an uncertain provider call %d times", requests.Load())
	}
}

func TestConditionalGCSPreconditionFailureReleasesKeyAndAllowsFreshInvocation(t *testing.T) {
	e, runtime, firstIntent, _, scope, receipts := mutationExecutionFixture(t)
	state, err := e.read(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	producerUID := state.Ledger.Runs[runtime.RunID].MutationProducerUID
	secondPayload := []byte("replanned payload")
	secondDraft := firstIntent
	secondDraft.OperationID = ""
	secondDraft.PayloadRef, secondDraft.PayloadDigest, secondDraft.PayloadSize = "", "", 0
	secondDraft.OwnerStep.Step = "replanned-registration"
	secondTicket := strings.Repeat("e", 64)
	if err := e.SourceStages.Reserve(producerUID, secondTicket, secondDraft); err != nil {
		t.Fatal(err)
	}
	sealed, err := e.SourceStages.Seal(producerUID, secondTicket, hashBytes(secondPayload), int64(len(secondPayload)), bytes.NewReader(secondPayload))
	if err != nil {
		t.Fatal(err)
	}
	secondIntent := secondDraft
	secondIntent.OperationID = fencing.MutationRequestID(producerUID, 2)
	secondIntent.PayloadRef, secondIntent.PayloadDigest, secondIntent.PayloadSize = sealed.Ref, sealed.Digest, sealed.Size
	if _, err := e.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error { return p.Admit(2, secondIntent) }); err != nil {
		t.Fatal(err)
	}

	var requests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("ifGenerationMatch") != "0" {
			t.Errorf("create-only condition was not preserved: %s", r.URL.String())
		}
		if requests.Add(1) == 1 {
			http.Error(w, "generation precondition failed", http.StatusPreconditionFailed)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Contains(body, []byte(secondIntent.OperationID)) || !bytes.Contains(body, []byte(secondIntent.PayloadDigest)) {
			t.Errorf("replanned request did not use its new immutable tuple: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"kind": "storage#object", "bucket": "archive", "name": "root/source/" + secondIntent.Key,
			"generation": "22", "metageneration": "1", "size": strconv.Itoa(len(secondPayload)),
			"metadata": map[string]string{"rhiza-operation-id": secondIntent.OperationID, "rhiza-payload-sha256": secondIntent.PayloadDigest},
		})
	}))
	defer server.Close()
	client, err := storage.NewClient(t.Context(), option.WithEndpoint(server.URL+"/storage/v1/"), option.WithHTTPClient(server.Client()), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	provider, err := newGCSMutationProvider(client, scope)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.ExecuteSourceMutation(t.Context(), runtime, producerUID, 1, firstIntent, scope, provider, receipts); !errors.Is(err, fencing.ErrMutationConditionFailed) {
		t.Fatalf("HTTP 412 was not returned as a definite conditional rejection: %v", err)
	}
	lookup, err := e.lookupMutation(t.Context(), runtime.StatefulSetUID, fencing.Identity{SourceClusterID: runtime.SourceClusterID, NodeID: runtime.NodeID, PodUID: runtime.PodUID, RunID: runtime.RunID, WALIdentity: "wal"}, fencing.MutationLookupRequest{ProducerUID: producerUID, Sequence: 1, Intent: firstIntent})
	if err != nil || lookup.Entry == nil || lookup.Entry.Resolution == nil || lookup.Entry.Resolution.Outcome != fencing.MutationRejected || !lookup.Entry.Resolution.ConditionFailed || !lookup.Entry.Resolution.AllCopiesTerminal {
		t.Fatalf("HTTP 412 did not leave a durable terminal noncommit: %+v %v", lookup, err)
	}
	lock, err := readMutationKeyForTest(t.Context(), e, firstIntent.StoreScopeHash, firstIntent.Key)
	if err != nil || lock.Owner != "" {
		t.Fatalf("definite precondition failure retained its key lock: %+v %v", lock, err)
	}
	if err := e.ExecuteSourceMutation(t.Context(), runtime, producerUID, 2, secondIntent, scope, provider, receipts); err != nil {
		t.Fatalf("fresh invocation could not succeed after the definite rejection: %v", err)
	}
	second, err := e.lookupMutation(t.Context(), runtime.StatefulSetUID, fencing.Identity{SourceClusterID: runtime.SourceClusterID, NodeID: runtime.NodeID, PodUID: runtime.PodUID, RunID: runtime.RunID, WALIdentity: "wal"}, fencing.MutationLookupRequest{ProducerUID: producerUID, Sequence: 2, Intent: secondIntent})
	if err != nil || second.Entry == nil || second.Entry.Resolution == nil || second.Entry.Resolution.Outcome != fencing.MutationCommitted || requests.Load() != 2 {
		t.Fatalf("new tuple was not independently committed: %+v requests=%d err=%v", second, requests.Load(), err)
	}
}

func TestGCSMutationProviderUnknownExactDeleteBecomesNoncommitExclusion(t *testing.T) {
	var deletes, exactReads atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodDelete:
			deletes.Add(1)
			if r.URL.Query().Get("generation") != "42" || r.URL.Query().Get("ifGenerationMatch") != "42" {
				t.Errorf("delete did not retain the selected GCS generation: %s", r.URL.String())
			}
			http.Error(w, "ambiguous response", http.StatusServiceUnavailable)
		case http.MethodGet:
			exactReads.Add(1)
			if r.URL.Query().Get("generation") != "42" {
				t.Errorf("absence probe did not target the selected generation: %s", r.URL.String())
			}
			http.Error(w, "not found", http.StatusNotFound)
		default:
			t.Errorf("unexpected GCS method %s", r.Method)
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	}))
	defer server.Close()
	client, err := storage.NewClient(context.Background(), option.WithEndpoint(server.URL+"/storage/v1/"), option.WithHTTPClient(server.Client()), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	provider, err := newGCSMutationProvider(client, AttestationStoreScope{Provider: "gcs", Bucket: "archive", Prefix: "root/source"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := provider.delete(context.Background(), "object", 42)
	if err != nil || result.Outcome != fencing.MutationExcluded || result.Generation != 42 || result.AllCopiesTerminal {
		t.Fatalf("unknown exact-generation delete was mislabeled committed or left unbounded: result=%+v err=%v", result, err)
	}
	intent := fencing.MutationIntent{Kind: "delete", OperationID: "producer-sequence", Key: "object"}
	recovered, found, err := provider.committed(context.Background(), "object", intent, fencing.MutationPreparation{Generation: 42})
	if err != nil || !found || recovered.Outcome != fencing.MutationExcluded || recovered.Generation != 42 || recovered.AllCopiesTerminal {
		t.Fatalf("missing selected generation did not prove only noncommit exclusion: result=%+v found=%t err=%v", recovered, found, err)
	}
	if deletes.Load() != 1 || exactReads.Load() != 2 {
		t.Fatalf("uncertain delete was retried or not probed by exact generation: deletes=%d reads=%d", deletes.Load(), exactReads.Load())
	}
}

func TestSourceMutationHTTPAdmitExecutesGCSAndReturnsOriginalDurableTuple(t *testing.T) {
	bootstrap := newArchiveGenesisFixture(t)
	commitArchiveGenesisFixture(t, bootstrap)
	e, runtime := bootstrap.e, bootstrap.runtime
	id := bootstrap.id

	payload := []byte(`{"membership":"source bytes"}`)
	var gcsRequests atomic.Int32
	var expectedOperation, expectedDigest atomic.Value
	gcs := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gcsRequests.Add(1)
		if r.Method != http.MethodPost || r.URL.Query().Get("ifGenerationMatch") != "0" || r.URL.Query().Get("name") != "root/source/membership/registration.json" {
			t.Errorf("GCS call did not preserve create-only source target: %s %s", r.Method, r.URL.String())
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Contains(body, payload) {
			t.Errorf("GCS request did not contain sealed source bytes: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"kind": "storage#object", "bucket": "archive", "name": "root/source/membership/registration.json",
			"generation": "21", "metageneration": "1", "size": strconv.Itoa(len(payload)),
			"metadata": map[string]string{"rhiza-operation-id": expectedOperation.Load().(string), "rhiza-payload-sha256": expectedDigest.Load().(string)},
		})
	}))
	defer gcs.Close()
	writer, err := storage.NewClient(t.Context(), option.WithEndpoint(gcs.URL+"/storage/v1/"), option.WithHTTPClient(gcs.Client()), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	e.SourceMutationStorage = writer
	receipts, err := e.receiptsForRuntime(runtime)
	if err != nil {
		t.Fatal(err)
	}
	if receipts.evidence == nil {
		t.Fatal("canonical authority runtime selected local mutation receipts")
	}

	authority := httptest.NewTLSServer(e.Handler())
	defer authority.Close()
	client := &fencing.AuthorityClient{URL: authority.URL + "/v1/supervisor", TokenFile: e.Kube.TokenFile, Client: authority.Client()}
	binding, err := client.Bind(t.Context(), id)
	if err != nil || binding.Producer == nil {
		t.Fatalf("source producer binding unavailable: %+v %v", binding, err)
	}
	draft := stageStoreIntent()
	draft.StoreScopeHash = binding.Producer.StoreScopeHash
	draft.OwnerStep.Step = "membership-registration"
	ticket := strings.Repeat("d", 64)
	if err := client.ReserveUpload(t.Context(), ticket, draft); err != nil {
		t.Fatal(err)
	}
	sealed, err := client.SealUpload(t.Context(), ticket, payload)
	if err != nil {
		t.Fatal(err)
	}
	intent := draft
	intent.OperationID = fencing.MutationRequestID(binding.Producer.ProducerUID, 2)
	intent.PayloadRef, intent.PayloadDigest, intent.PayloadSize = sealed.Ref, sealed.Digest, sealed.Size
	expectedOperation.Store(intent.OperationID)
	expectedDigest.Store(intent.PayloadDigest)
	if err := client.Admit(t.Context(), 2, intent); err != nil {
		t.Fatal(err)
	}
	result, err := client.LookupMutation(t.Context(), id, binding.Producer.ProducerUID, 2, intent)
	if err != nil || !result.Found || result.Entry == nil || result.Entry.Intent != intent || result.Entry.Resolution == nil || result.Entry.Resolution.Outcome != fencing.MutationCommitted || result.Entry.Resolution.PayloadDigest != intent.PayloadDigest {
		t.Fatalf("source ACK lacks its original durable commit tuple: %+v %v", result, err)
	}
	if gcsRequests.Load() != 1 {
		t.Fatalf("admission executed %d GCS requests, want one", gcsRequests.Load())
	}
	owner := hashJSON([]any{runtime, id.WALIdentity, binding.Producer.ProducerUID, uint64(2), intent.OperationID})
	receipt, err := receipts.get("receipt-" + owner)
	if err != nil || receipt.Intent != intent || receipt.WALIdentity != id.WALIdentity || receipt.Generation != 21 {
		t.Fatalf("provider receipt was not preserved in root-bound object storage: %+v %v", receipt, err)
	}
}
