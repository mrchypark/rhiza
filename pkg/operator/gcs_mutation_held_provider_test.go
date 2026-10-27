package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"google.golang.org/api/option"
)

type heldCreateErrorReader struct{ err error }

func (r heldCreateErrorReader) Read([]byte) (int, error) { return 0, r.err }

func TestGCSHeldCreateWritesHoldAndFinalizesOnlyExactReceipt(t *testing.T) {
	payload := []byte(`{"sealed":"payload"}`)
	scope := AttestationStoreScope{Provider: "gcs", Bucket: "archive", Prefix: "root/source"}
	intent := fencing.MutationIntent{ContractVersion: fencing.MutationIntentVersion, StoreScopeHash: hashJSON(scope),
		OwnerStep: fencing.MutationOwnerStep{Owner: "source", Workflow: "archive", Step: "head", Attempt: 1},
		Condition: fencing.MutationCondition{Mode: fencing.MutationCreateOnly}, OperationID: "producer-sequence", Kind: "upload",
		Key: "archive/head.bin", PayloadRef: "staging/payload", PayloadDigest: hashBytes(payload), PayloadSize: int64(len(payload))}
	attrs := map[string]any{"kind": "storage#object", "bucket": "archive", "name": "root/source/archive/head.bin",
		"generation": "51", "metageneration": "1", "size": strconv.Itoa(len(payload)), "temporaryHold": true,
		"metadata": map[string]string{"rhiza-operation-id": intent.OperationID, "rhiza-payload-sha256": intent.PayloadDigest}}
	var mu sync.Mutex
	var uploadCount, updateCount int
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/b/archive/o"):
			if r.URL.Query().Get("ifGenerationMatch") != "0" || r.URL.Query().Get("uploadType") == "resumable" {
				t.Errorf("held create lost create-only/single-request constraint: %s", r.URL.String())
			}
			body, err := io.ReadAll(r.Body)
			if err != nil || !bytes.Contains(body, payload) || !bytes.Contains(body, []byte(`"temporaryHold":true`)) {
				t.Errorf("upload did not atomically include held exact payload: err=%v", err)
			}
			mu.Lock()
			uploadCount++
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(attrs)
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/archive/"):
			if r.URL.Query().Get("generation") != "51" {
				t.Errorf("byte verification did not read the exact selected generation: %s", r.URL.String())
			}
			_, _ = w.Write(payload)
		case r.Method == http.MethodGet && r.URL.Query().Get("alt") == "media":
			if r.URL.Query().Get("generation") != "51" {
				t.Errorf("byte verification did not read the exact selected generation: %s", r.URL.String())
			}
			_, _ = w.Write(payload)
		case r.Method == http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(attrs)
		case r.Method == http.MethodPatch:
			if r.URL.Query().Get("ifGenerationMatch") != "51" || r.URL.Query().Get("ifMetagenerationMatch") != "1" {
				t.Errorf("hold release was not bound to the exact generation and metageneration: %s", r.URL.String())
			}
			body, err := io.ReadAll(r.Body)
			if err != nil || !bytes.Contains(body, []byte(`"temporaryHold":false`)) {
				t.Errorf("finalizer did not request hold removal: %v", err)
			}
			mu.Lock()
			updateCount++
			attrs["temporaryHold"] = false
			attrs["metageneration"] = "2"
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(attrs)
		default:
			t.Errorf("unexpected GCS request %s %s", r.Method, r.URL.String())
			http.Error(w, "unexpected request", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	client, err := storage.NewClient(context.Background(), option.WithEndpoint(server.URL+"/storage/v1/"), option.WithHTTPClient(server.Client()), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	provider, err := newGCSHeldCreateMutationProvider(client, scope)
	if err != nil {
		t.Fatal(err)
	}
	result, err := provider.upload(context.Background(), intent.Key, 0, intent, bytes.NewReader(payload))
	if err != nil || result.Outcome != fencing.MutationCommitted || !result.AllCopiesTerminal || result.Generation != 51 || result.Metageneration != 1 {
		t.Fatalf("held upload did not return its exact terminal dispatch receipt: result=%+v err=%v", result, err)
	}
	receipt := mutationExecutionReceipt{Runtime: fencing.RuntimeBinding{SourceClusterID: "source", NodeID: "node", PodUID: "pod", RunID: "run", StatefulSetUID: "sts"},
		WALIdentity: "wal", ProducerUID: "producer", Sequence: 1, Intent: intent,
		Preparation: fencing.MutationPreparation{Profile: fencing.MutationHeldExecutionProfile, StoreScopeHash: intent.StoreScopeHash, GenerationSet: true, Generation: 0},
		DispatchID:  "dispatch", Outcome: fencing.MutationCommitted, Generation: result.Generation, Metageneration: result.Metageneration,
		PayloadDigest: result.PayloadDigest, AllCopiesTerminal: true}
	receipt.ReceiptDigest = receipt.digest()
	wrongGeneration := receipt
	wrongGeneration.Generation++
	wrongGeneration.ReceiptDigest = wrongGeneration.digest()
	if err := provider.finalizeHeldCreate(context.Background(), intent.Key, intent, wrongGeneration); err == nil {
		t.Fatal("finalizer accepted a different object generation")
	}
	mu.Lock()
	if updateCount != 0 || attrs["temporaryHold"] != true {
		mu.Unlock()
		t.Fatal("invalid terminal receipt cleared the held generation")
	}
	mu.Unlock()
	if err := provider.finalizeHeldCreate(context.Background(), intent.Key, intent, receipt); err != nil {
		t.Fatalf("exact terminal receipt could not release the hold: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if uploadCount != 1 || updateCount != 1 || attrs["temporaryHold"] != false {
		t.Fatalf("unexpected held-create lifecycle: uploads=%d updates=%d attrs=%v", uploadCount, updateCount, attrs)
	}
}

func TestGCSHeldCreateClassifiesCopy412ButNotUnknownCopyError(t *testing.T) {
	scope := AttestationStoreScope{Provider: "gcs", Bucket: "archive", Prefix: "root/source"}
	for _, tc := range []struct {
		name       string
		copyError  error
		respond412 bool
		copy412    bool
		wantReject bool
	}{
		{name: "provider 412 during copy", respond412: true, copy412: true, wantReject: true},
		{name: "provider 412 during close", respond412: true, wantReject: true},
		{name: "unrecognized payload error during copy", copyError: errors.New("payload read failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := bytes.Repeat([]byte("held-copy"), 1<<17)
			if tc.respond412 && !tc.copy412 {
				payload = nil
			}
			intent := fencing.MutationIntent{ContractVersion: fencing.MutationIntentVersion, StoreScopeHash: hashJSON(scope),
				OwnerStep: fencing.MutationOwnerStep{Owner: "source", Workflow: "archive", Step: "copy-classification", Attempt: 1},
				Condition: fencing.MutationCondition{Mode: fencing.MutationCreateOnly}, OperationID: "producer-sequence", Kind: "upload",
				Key: "archive/head.bin", PayloadRef: "staging/payload", PayloadDigest: hashBytes(payload), PayloadSize: int64(len(payload))}
			attrs := map[string]any{"kind": "storage#object", "bucket": "archive", "name": "root/source/archive/head.bin",
				"generation": "61", "metageneration": "1", "size": strconv.Itoa(len(payload)),
				"temporaryHold": true, "metadata": map[string]string{"rhiza-operation-id": intent.OperationID, "rhiza-payload-sha256": intent.PayloadDigest}}
			var requests atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/b/archive/o") {
					t.Errorf("unexpected GCS request %s %s", r.Method, r.URL.String())
					http.Error(w, "unexpected request", http.StatusMethodNotAllowed)
					return
				}
				requests.Add(1)
				if tc.respond412 {
					if tc.copy412 {
						// Reject while the SDK writer is still consuming the request pipe,
						// so the 412 is observed by io.Copy rather than Writer.Close.
						buf := make([]byte, 128)
						if n, err := r.Body.Read(buf); n == 0 || err != nil {
							t.Errorf("read initial upload bytes: n=%d err=%v", n, err)
						}
					} else if _, err := io.Copy(io.Discard, r.Body); err != nil {
						t.Errorf("drain close-stage request: %v", err)
					}
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusPreconditionFailed)
					_, _ = io.WriteString(w, `{"error":{"code":412,"message":"conditionNotMet","errors":[{"domain":"global","reason":"conditionNotMet","message":"generation precondition failed"}]}}`)
					return
				}
				if _, err := io.Copy(io.Discard, r.Body); err != nil {
					t.Errorf("drain uploaded bytes: %v", err)
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
			provider, err := newGCSHeldCreateMutationProvider(client, scope)
			if err != nil {
				t.Fatal(err)
			}
			var source io.Reader = bytes.NewReader(payload)
			if tc.copyError != nil {
				source = io.MultiReader(bytes.NewReader(payload[:128]), heldCreateErrorReader{err: tc.copyError})
			}
			result, uploadErr := provider.upload(t.Context(), intent.Key, 0, intent, source)
			wantRequests := int32(1)
			if tc.copyError != nil {
				wantRequests = 0 // A source read error must not dispatch an incomplete body.
			}
			if requests.Load() != wantRequests {
				t.Fatalf("SDK upload requests=%d, want %d", requests.Load(), wantRequests)
			}
			if tc.wantReject {
				if uploadErr != nil || result.Outcome != fencing.MutationRejected || !result.ConditionFailed || !result.AllCopiesTerminal {
					t.Fatalf("recognized copy 412 was not a terminal rejection: result=%+v err=%T %v", result, uploadErr, uploadErr)
				}
				return
			}
			if uploadErr == nil || result.AllCopiesTerminal || result.Outcome != "" {
				t.Fatalf("unknown copy error became a terminal provider result: result=%+v err=%T %v", result, uploadErr, uploadErr)
			}
		})
	}
}

func TestGCSHeldUnconditionalZeroGenerationCreatesOnlyWhenPreparedAbsent(t *testing.T) {
	payload := []byte("unconditional but absent at preparation")
	scope := AttestationStoreScope{Provider: "gcs", Bucket: "archive", Prefix: "root/source"}
	intent := fencing.MutationIntent{ContractVersion: fencing.MutationIntentVersion, StoreScopeHash: hashJSON(scope),
		OwnerStep: fencing.MutationOwnerStep{Owner: "source", Workflow: "archive", Step: "unconditional", Attempt: 1},
		Condition: fencing.MutationCondition{Mode: fencing.MutationUnconditional}, OperationID: "producer-sequence", Kind: "upload",
		Key: "archive/head.bin", PayloadRef: "staging/payload", PayloadDigest: hashBytes(payload), PayloadSize: int64(len(payload))}
	attrs := map[string]any{"kind": "storage#object", "bucket": "archive", "name": "root/source/archive/head.bin",
		"generation": "52", "metageneration": "1", "size": strconv.Itoa(len(payload)), "temporaryHold": true,
		"metadata": map[string]string{"rhiza-operation-id": intent.OperationID, "rhiza-payload-sha256": intent.PayloadDigest}}
	var uploads int
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/b/archive/o"):
			uploads++
			if r.URL.Query().Get("ifGenerationMatch") != "0" || r.URL.Query().Get("uploadType") == "resumable" {
				t.Errorf("prepared-absent unconditional write did not use a single create-only request: %s", r.URL.String())
			}
			body, err := io.ReadAll(r.Body)
			if err != nil || !bytes.Contains(body, payload) || !bytes.Contains(body, []byte(`"temporaryHold":true`)) {
				t.Errorf("unconditional zero-generation request did not atomically hold the exact payload: %v", err)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(attrs)
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/archive/"):
			if r.URL.Query().Get("generation") != "52" {
				t.Errorf("unconditional write verification did not read its exact generation: %s", r.URL.String())
			}
			_, _ = w.Write(payload)
		case r.Method == http.MethodGet && r.URL.Query().Get("alt") == "media":
			if r.URL.Query().Get("generation") != "52" {
				t.Errorf("unconditional write verification did not read its exact generation: %s", r.URL.String())
			}
			_, _ = w.Write(payload)
		case r.Method == http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(attrs)
		default:
			t.Errorf("unexpected GCS request %s %s", r.Method, r.URL.String())
			http.Error(w, "unexpected request", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	client, err := storage.NewClient(t.Context(), option.WithEndpoint(server.URL+"/storage/v1/"), option.WithHTTPClient(server.Client()), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	provider, err := newGCSHeldCreateMutationProvider(client, scope)
	if err != nil {
		t.Fatal(err)
	}
	result, err := provider.upload(t.Context(), intent.Key, 0, intent, bytes.NewReader(payload))
	if err != nil || result.Outcome != fencing.MutationCommitted || !result.AllCopiesTerminal || result.Generation != 52 || result.Metageneration != 1 || uploads != 1 {
		t.Fatalf("held profile did not safely commit the zero-generation unconditional write: result=%+v uploads=%d err=%v", result, uploads, err)
	}
	invalid := intent
	invalid.Condition = fencing.MutationCondition{Mode: fencing.MutationMatchGeneration, Generation: 4}
	if _, err := provider.upload(t.Context(), invalid.Key, 0, invalid, bytes.NewReader(payload)); err == nil || uploads != 1 {
		t.Fatalf("non-eligible zero-generation upload was dispatched: uploads=%d err=%v", uploads, err)
	}
}

func TestGCSHeldNonzeroUnconditionalRetainsFixedGenerationCAS(t *testing.T) {
	payload := []byte("fixed generation CAS")
	scope := AttestationStoreScope{Provider: "gcs", Bucket: "archive", Prefix: "root/source"}
	intent := fencing.MutationIntent{ContractVersion: fencing.MutationIntentVersion, StoreScopeHash: hashJSON(scope),
		OwnerStep: fencing.MutationOwnerStep{Owner: "source", Workflow: "archive", Step: "unconditional", Attempt: 1},
		Condition: fencing.MutationCondition{Mode: fencing.MutationUnconditional}, OperationID: "producer-sequence", Kind: "upload",
		Key: "archive/head.bin", PayloadRef: "staging/payload", PayloadDigest: hashBytes(payload), PayloadSize: int64(len(payload))}
	var uploads int
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Query().Get("ifGenerationMatch") != "51" || r.URL.Query().Get("uploadType") == "resumable" {
			t.Errorf("nonzero unconditional write did not retain its fixed-generation single request: %s %s", r.Method, r.URL.String())
			http.Error(w, "unexpected request", http.StatusMethodNotAllowed)
			return
		}
		uploads++
		body, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Contains(body, payload) || bytes.Contains(body, []byte(`"temporaryHold":true`)) {
			t.Errorf("nonzero unconditional CAS changed payload or acquired a generation-zero hold: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"kind": "storage#object", "bucket": "archive", "name": "root/source/archive/head.bin",
			"generation": "52", "metageneration": "1", "size": strconv.Itoa(len(payload)),
			"metadata": map[string]string{"rhiza-operation-id": intent.OperationID, "rhiza-payload-sha256": intent.PayloadDigest}})
	}))
	defer server.Close()
	client, err := storage.NewClient(t.Context(), option.WithEndpoint(server.URL+"/storage/v1/"), option.WithHTTPClient(server.Client()), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	provider, err := newGCSHeldCreateMutationProvider(client, scope)
	if err != nil {
		t.Fatal(err)
	}
	result, err := provider.upload(t.Context(), intent.Key, 51, intent, bytes.NewReader(payload))
	if err != nil || result.Outcome != fencing.MutationCommitted || !result.AllCopiesTerminal || result.Generation != 52 || uploads != 1 {
		t.Fatalf("nonzero unconditional write lost fixed-CAS behavior: result=%+v uploads=%d err=%v", result, uploads, err)
	}
}

func TestHeldCreateGuardIsImmutablePerScopeKeyAndTuple(t *testing.T) {
	scope := AttestationStoreScope{Provider: "gcs", Bucket: "archive", Prefix: "root/source"}
	intent := fencing.MutationIntent{ContractVersion: fencing.MutationIntentVersion, StoreScopeHash: hashJSON(scope),
		OwnerStep: fencing.MutationOwnerStep{Owner: "source", Workflow: "archive", Step: "head", Attempt: 1},
		Condition: fencing.MutationCondition{Mode: fencing.MutationCreateOnly}, OperationID: "producer-sequence", Kind: "upload",
		Key: "archive/head.bin", PayloadDigest: strings.Repeat("a", 64), PayloadSize: 0}
	request := mutationGuardedReplayRequest{Runtime: fencing.RuntimeBinding{SourceClusterID: "source", NodeID: "node", PodUID: "pod", RunID: "run", StatefulSetUID: "sts"},
		WALIdentity: "wal", ProducerUID: "producer", Sequence: 1, Intent: intent,
		Preparation:     fencing.MutationPreparation{Profile: fencing.MutationHeldExecutionProfile, StoreScopeHash: intent.StoreScopeHash, GenerationSet: true},
		PreparationHash: strings.Repeat("b", 64), DispatchID: "dispatch", GuardRootIdentity: strings.Repeat("c", 64)}
	_, firstBody, firstRef, firstDigest, err := heldCreateGuard(request)
	if err != nil {
		t.Fatal(err)
	}
	request.Intent.OperationID = "different-operation"
	_, secondBody, secondRef, secondDigest, err := heldCreateGuard(request)
	if err != nil {
		t.Fatal(err)
	}
	if firstRef != secondRef || bytes.Equal(firstBody, secondBody) || firstDigest == secondDigest {
		t.Fatal("same object key did not retain one guard slot bound to immutable tuple bytes")
	}
	rootIdentity := request.GuardRootIdentity
	store, err := newAuthorityEvidenceStore(newMemoryEvidenceObjects(), "authority", rootIdentity)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateImmutable(context.Background(), firstRef, firstBody); err != nil {
		t.Fatal(err)
	}
	if err := verifyHeldCreateGuardReadback(context.Background(), store, firstRef, firstBody); err != nil {
		t.Fatalf("exact root-scoped guard did not read back: %v", err)
	}
	if _, err := store.CreateImmutable(context.Background(), secondRef, secondBody); err == nil {
		t.Fatal("conflicting tuple reused the existing per-key immutable guard")
	}
	if err := verifyHeldCreateGuardReadback(context.Background(), store, firstRef, secondBody); err == nil {
		t.Fatal("different tuple bytes passed immutable guard readback")
	}
	request.GuardRootIdentity = strings.Repeat("d", 64)
	_, otherRootBody, otherRootRef, _, err := heldCreateGuard(request)
	if err != nil || otherRootRef != secondRef || bytes.Equal(secondBody, otherRootBody) {
		t.Fatalf("root identity was not bound inside stable per-key guard: ref=%q/%q err=%v", secondRef, otherRootRef, err)
	}
}

func TestGCSGuardedReplayRequiresRootGuardAndActualHeldGenerationBytes(t *testing.T) {
	for _, tc := range []struct {
		name         string
		condition    fencing.MutationConditionMode
		stored       []byte
		held         bool
		wantEvidence bool
	}{
		{name: "exact held bytes", stored: []byte("exact staged payload"), held: true, wantEvidence: true},
		{name: "unconditional exact held bytes", condition: fencing.MutationUnconditional, stored: []byte("exact staged payload"), held: true, wantEvidence: true},
		{name: "unconditional metadata matches but bytes differ", condition: fencing.MutationUnconditional, stored: []byte("false staged payload"), held: true},
		{name: "metadata matches but bytes differ", stored: []byte("false staged payload"), held: true},
		{name: "legacy exact bytes without hold", stored: []byte("exact staged payload")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := []byte("exact staged payload")
			scope := AttestationStoreScope{Provider: "gcs", Bucket: "archive", Prefix: "root/source"}
			condition := tc.condition
			if condition == "" {
				condition = fencing.MutationCreateOnly
			}
			intent := fencing.MutationIntent{ContractVersion: fencing.MutationIntentVersion, StoreScopeHash: hashJSON(scope),
				OwnerStep: fencing.MutationOwnerStep{Owner: "source", Workflow: "archive", Step: "head", Attempt: 1},
				Condition: fencing.MutationCondition{Mode: condition}, OperationID: "producer-sequence", Kind: "upload",
				Key: "archive/head.bin", PayloadRef: "staging/payload", PayloadDigest: hashBytes(payload), PayloadSize: int64(len(payload))}
			preparation := fencing.MutationPreparation{Profile: fencing.MutationHeldExecutionProfile, StoreScopeHash: intent.StoreScopeHash, GenerationSet: true, Generation: 0}
			rootIdentity := strings.Repeat("c", 64)
			request := mutationGuardedReplayRequest{Runtime: fencing.RuntimeBinding{SourceClusterID: "source", NodeID: "node", PodUID: "pod", RunID: "run", StatefulSetUID: "sts"},
				WALIdentity: "wal", ProducerUID: "producer", Sequence: 1, Intent: intent, Preparation: preparation,
				PreparationHash: strings.Repeat("b", 64), DispatchID: "dispatch", GuardRootIdentity: rootIdentity}
			attrs := map[string]any{"kind": "storage#object", "bucket": "archive", "name": "root/source/archive/head.bin",
				"generation": "61", "metageneration": "2", "size": strconv.Itoa(len(tc.stored)), "temporaryHold": tc.held,
				"metadata": map[string]string{"rhiza-operation-id": intent.OperationID, "rhiza-payload-sha256": intent.PayloadDigest}}
			var uploads int
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/b/archive/o"):
					uploads++
					if r.URL.Query().Get("ifGenerationMatch") != "0" || r.URL.Query().Get("uploadType") == "resumable" {
						t.Errorf("guarded replay changed the original create-only request: %s", r.URL.String())
					}
					http.Error(w, "already exists", http.StatusPreconditionFailed)
				case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/archive/"):
					if r.URL.Query().Get("generation") != "61" {
						t.Errorf("guarded replay did not read exact generation bytes: %s", r.URL.String())
					}
					_, _ = w.Write(tc.stored)
				case r.Method == http.MethodGet && r.URL.Query().Get("alt") == "media":
					if r.URL.Query().Get("generation") != "61" {
						t.Errorf("guarded replay did not read exact generation bytes: %s", r.URL.String())
					}
					_, _ = w.Write(tc.stored)
				case r.Method == http.MethodGet:
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(attrs)
				default:
					t.Errorf("unexpected GCS request %s %s", r.Method, r.URL.String())
					http.Error(w, "unexpected request", http.StatusMethodNotAllowed)
				}
			}))
			defer server.Close()
			client, err := storage.NewClient(t.Context(), option.WithEndpoint(server.URL+"/storage/v1/"), option.WithHTTPClient(server.Client()), option.WithoutAuthentication())
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			provider, err := newGCSHeldCreateMutationProvider(client, scope)
			if err != nil {
				t.Fatal(err)
			}
			store, err := newAuthorityEvidenceStore(newMemoryEvidenceObjects(), "authority", rootIdentity)
			if err != nil {
				t.Fatal(err)
			}
			evidence, err := provider.replayGuardedCreate(t.Context(), store, request, bytes.NewReader(payload))
			if tc.wantEvidence {
				if err != nil || evidence.ObjectGeneration != 61 || evidence.ObjectMetageneration != 2 || evidence.GuardRootIdentity != rootIdentity || uploads != 1 {
					t.Fatalf("exact guarded object did not resolve logically: evidence=%+v uploads=%d err=%v", evidence, uploads, err)
				}
				if err := provider.verifyGuardedCreate(t.Context(), store, request, evidence); err != nil {
					t.Fatalf("exact guard/object evidence did not verify: %v", err)
				}
			} else if err == nil || evidence.ObjectGeneration != 0 || uploads != 1 {
				t.Fatalf("metadata-only match was accepted: evidence=%+v uploads=%d err=%v", evidence, uploads, err)
			}
		})
	}
}

func TestGCSGuardedReplayCreateStoresHeldGenerationAfterRootGuard(t *testing.T) {
	payload := []byte("replay exact staged payload")
	scope := AttestationStoreScope{Provider: "gcs", Bucket: "archive", Prefix: "root/source"}
	intent := fencing.MutationIntent{ContractVersion: fencing.MutationIntentVersion, StoreScopeHash: hashJSON(scope),
		OwnerStep: fencing.MutationOwnerStep{Owner: "source", Workflow: "archive", Step: "head", Attempt: 1},
		Condition: fencing.MutationCondition{Mode: fencing.MutationUnconditional}, OperationID: "producer-sequence", Kind: "upload",
		Key: "archive/head.bin", PayloadRef: "staging/payload", PayloadDigest: hashBytes(payload), PayloadSize: int64(len(payload))}
	preparation := fencing.MutationPreparation{Profile: fencing.MutationHeldExecutionProfile, StoreScopeHash: intent.StoreScopeHash, GenerationSet: true, Generation: 0}
	rootIdentity := strings.Repeat("c", 64)
	request := mutationGuardedReplayRequest{Runtime: fencing.RuntimeBinding{SourceClusterID: "source", NodeID: "node", PodUID: "pod", RunID: "run", StatefulSetUID: "sts"},
		WALIdentity: "wal", ProducerUID: "producer", Sequence: 1, Intent: intent, Preparation: preparation,
		PreparationHash: strings.Repeat("b", 64), DispatchID: "dispatch", GuardRootIdentity: rootIdentity}
	attrs := map[string]any{"kind": "storage#object", "bucket": "archive", "name": "root/source/archive/head.bin",
		"generation": "71", "metageneration": "1", "size": strconv.Itoa(len(payload)), "temporaryHold": true,
		"metadata": map[string]string{"rhiza-operation-id": intent.OperationID, "rhiza-payload-sha256": intent.PayloadDigest}}
	var uploads int
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/b/archive/o"):
			uploads++
			body, err := io.ReadAll(r.Body)
			if r.URL.Query().Get("ifGenerationMatch") != "0" || r.URL.Query().Get("uploadType") == "resumable" || err != nil ||
				!bytes.Contains(body, payload) || !bytes.Contains(body, []byte(`"temporaryHold":true`)) {
				t.Errorf("guarded replay did not atomically create the exact held payload: err=%v", err)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(attrs)
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/archive/"):
			if r.URL.Query().Get("generation") != "71" {
				t.Errorf("guarded replay did not read its exact created generation: %s", r.URL.String())
			}
			_, _ = w.Write(payload)
		case r.Method == http.MethodGet && r.URL.Query().Get("alt") == "media":
			if r.URL.Query().Get("generation") != "71" {
				t.Errorf("guarded replay did not read the exact created generation: %s", r.URL.String())
			}
			_, _ = w.Write(payload)
		case r.Method == http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(attrs)
		default:
			t.Errorf("unexpected GCS request %s %s", r.Method, r.URL.String())
			http.Error(w, "unexpected request", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	client, err := storage.NewClient(t.Context(), option.WithEndpoint(server.URL+"/storage/v1/"), option.WithHTTPClient(server.Client()), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	provider, err := newGCSHeldCreateMutationProvider(client, scope)
	if err != nil {
		t.Fatal(err)
	}
	store, err := newAuthorityEvidenceStore(newMemoryEvidenceObjects(), "authority", rootIdentity)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := provider.replayGuardedCreate(t.Context(), store, request, bytes.NewReader(payload))
	if err != nil || evidence.ObjectGeneration != 71 || evidence.ObjectMetageneration != 1 || uploads != 1 {
		t.Fatalf("guarded replay did not create and resolve its exact held generation: evidence=%+v uploads=%d err=%v", evidence, uploads, err)
	}
	if err := provider.verifyGuardedCreate(t.Context(), store, request, evidence); err != nil {
		t.Fatalf("new held generation did not verify against its guard: %v", err)
	}
}
