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
	if requests.Load() != 1 || result.Outcome != fencing.MutationCommitted || result.Generation != 21 || result.Metageneration != 3 || result.PayloadDigest != intent.PayloadDigest {
		t.Fatalf("GCS result did not bind the one wire commit: requests=%d result=%+v", requests.Load(), result)
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
	e, fixture, id, runtime := newMutationStoreFixture(t)
	if _, err := e.reserveMutationProducer(t.Context(), runtime); err != nil {
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
	fixture.mu.Lock()
	fixture.authPod, fixture.authStatefulSet = pod, sts
	fixture.objects["/apis/apps/v1/namespaces/test/statefulsets"] = object{"items": []any{sts}}
	fixture.mu.Unlock()

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
	id.WALIdentity = "wal-source-http"
	binding, err := client.Bind(t.Context(), id)
	if err != nil || binding.Producer == nil {
		t.Fatalf("source producer binding unavailable: %+v %v", binding, err)
	}
	draft := stageStoreIntent()
	draft.StoreScopeHash = binding.Producer.StoreScopeHash
	ticket := strings.Repeat("d", 64)
	if err := client.ReserveUpload(t.Context(), ticket, draft); err != nil {
		t.Fatal(err)
	}
	sealed, err := client.SealUpload(t.Context(), ticket, payload)
	if err != nil {
		t.Fatal(err)
	}
	intent := draft
	intent.OperationID = fencing.MutationRequestID(binding.Producer.ProducerUID, 1)
	intent.PayloadRef, intent.PayloadDigest, intent.PayloadSize = sealed.Ref, sealed.Digest, sealed.Size
	expectedOperation.Store(intent.OperationID)
	expectedDigest.Store(intent.PayloadDigest)
	if err := client.Admit(t.Context(), 1, intent); err != nil {
		t.Fatal(err)
	}
	result, err := client.LookupMutation(t.Context(), id, binding.Producer.ProducerUID, 1, intent)
	if err != nil || !result.Found || result.Entry == nil || result.Entry.Intent != intent || result.Entry.Resolution == nil || result.Entry.Resolution.Outcome != fencing.MutationCommitted || result.Entry.Resolution.PayloadDigest != intent.PayloadDigest {
		t.Fatalf("source ACK lacks its original durable commit tuple: %+v %v", result, err)
	}
	if gcsRequests.Load() != 1 {
		t.Fatalf("admission executed %d GCS requests, want one", gcsRequests.Load())
	}
	owner := hashJSON([]any{runtime, id.WALIdentity, binding.Producer.ProducerUID, uint64(1), intent.OperationID})
	receipt, err := receipts.get("receipt-" + owner)
	if err != nil || receipt.Intent != intent || receipt.WALIdentity != id.WALIdentity || receipt.Generation != 21 {
		t.Fatalf("provider receipt was not preserved in root-bound object storage: %+v %v", receipt, err)
	}
}
