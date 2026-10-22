package operator

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"google.golang.org/api/option"
)

type heldProfileFixture struct {
	e        *FencingExecutor
	api      *mutationStoreFixture
	id       fencing.Identity
	runtime  fencing.RuntimeBinding
	producer string
	scope    AttestationStoreScope
	stages   *SourceStageStore
	receipts *MutationExecutionReceiptStore
}

func newHeldProfileFixture(t *testing.T) heldProfileFixture {
	t.Helper()
	e, api, id, runtime := newMutationStoreFixture(t)
	e.MutationProfile = fencing.MutationHeldExecutionProfile
	if _, err := e.reserveMutationProducer(t.Context(), runtime); err != nil {
		t.Fatal(err)
	}
	scope := AttestationStoreScope{Provider: "gcs", Bucket: "archive", Prefix: "root/source"}
	stages, err := e.sourceStagesFor(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	id.WALIdentity = "wal-held-profile"
	if _, err := e.change(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID, func(l *fencing.Ledger) error {
		if err := l.BindMutationScope(id, hashJSON(scope), fencing.MutationHeldExecutionProfile); err != nil {
			return err
		}
		if err := l.BindMutationStage(id, stages.incarnation); err != nil {
			return err
		}
		return l.Bind(id)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error { return p.Activate(id) }); err != nil {
		t.Fatal(err)
	}
	run, err := e.enrolledMutationRun(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	configureMutationWorkloadFixture(t, e, api, id, runtime)
	receipts, err := e.receiptsForRuntime(runtime)
	if err != nil {
		t.Fatal(err)
	}
	return heldProfileFixture{e: e, api: api, id: id, runtime: runtime, producer: run.MutationProducerUID,
		scope: scope, stages: stages, receipts: receipts}
}

func configureMutationWorkloadFixture(t *testing.T, e *FencingExecutor, api *mutationStoreFixture, id fencing.Identity, runtime fencing.RuntimeBinding) {
	t.Helper()
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
	api.mu.Lock()
	api.authPod, api.authStatefulSet = pod, sts
	api.objects["/apis/apps/v1/namespaces/test/statefulsets"] = object{"items": []any{sts}}
	api.mu.Unlock()
	if e.Kube == nil {
		t.Fatal("fixture has no Kubernetes client")
	}
}

func (f heldProfileFixture) admitUpload(t *testing.T, sequence uint64, key string, condition fencing.MutationCondition, payload []byte) fencing.MutationIntent {
	t.Helper()
	draft := stageStoreIntent()
	draft.StoreScopeHash = hashJSON(f.scope)
	draft.OwnerStep = fencing.MutationOwnerStep{Owner: "test", Workflow: "held-profile", Step: fmt.Sprint(sequence), Attempt: 1}
	draft.Key, draft.Condition = key, condition
	ticket := fmt.Sprintf("%064x", sequence)
	if err := f.stages.Reserve(f.producer, ticket, draft); err != nil {
		t.Fatal(err)
	}
	sealed, err := f.stages.Seal(f.producer, ticket, hashBytes(payload), int64(len(payload)), bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	intent := draft
	intent.OperationID = fencing.MutationRequestID(f.producer, sequence)
	intent.PayloadRef, intent.PayloadDigest, intent.PayloadSize = sealed.Ref, sealed.Digest, sealed.Size
	if _, err := f.e.changeMutationProducer(t.Context(), f.runtime, func(p *fencing.MutationProducer) error { return p.Admit(sequence, intent) }); err != nil {
		t.Fatal(err)
	}
	return intent
}

func (f heldProfileFixture) admitDelete(t *testing.T, sequence uint64, key string, generation int64) fencing.MutationIntent {
	t.Helper()
	intent := fencing.MutationIntent{ContractVersion: fencing.MutationIntentVersion, StoreScopeHash: hashJSON(f.scope),
		OwnerStep:   fencing.MutationOwnerStep{Owner: "test", Workflow: "held-profile", Step: fmt.Sprint(sequence), Attempt: 1},
		Condition:   fencing.MutationCondition{Mode: fencing.MutationMatchGeneration, Generation: generation},
		OperationID: fencing.MutationRequestID(f.producer, sequence), Kind: "delete", Key: key}
	if _, err := f.e.changeMutationProducer(t.Context(), f.runtime, func(p *fencing.MutationProducer) error { return p.Admit(sequence, intent) }); err != nil {
		t.Fatal(err)
	}
	return intent
}

type heldGCSObject struct {
	name           string
	generation     int64
	metageneration int64
	payload        []byte
	metadata       map[string]string
	temporaryHold  bool
}

type heldGCSServer struct {
	mu             sync.Mutex
	objects        map[string]heldGCSObject
	nextGeneration int64
	posts          int
	patches        int
	deletes        int
	loseCreate     bool
	failPatch      int
	onPatch        func()
}

func newHeldGCSServer(t *testing.T, fixture heldProfileFixture) (*heldGCSServer, *storage.Client) {
	t.Helper()
	state := &heldGCSServer{objects: make(map[string]heldGCSObject), nextGeneration: 100, loseCreate: true}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Query().Get("name")
		if name == "" {
			escapedPath := r.URL.EscapedPath()
			if strings.HasPrefix(escapedPath, "/storage/v1/b/archive/o/") {
				name, _ = url.PathUnescape(strings.TrimPrefix(escapedPath, "/storage/v1/b/archive/o/"))
			} else if strings.HasPrefix(escapedPath, "/archive/") {
				// The pinned SDK defaults NewReader to the XML API path.
				name, _ = url.PathUnescape(strings.TrimPrefix(escapedPath, "/archive/"))
			}
		}
		state.mu.Lock()
		defer state.mu.Unlock()
		switch r.Method {
		case http.MethodPost:
			state.posts++
			if name == "" {
				http.Error(w, "missing object name", http.StatusBadRequest)
				return
			}
			metadata, payload, err := readHeldGCSUpload(r)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			precondition, _ := strconv.ParseInt(r.URL.Query().Get("ifGenerationMatch"), 10, 64)
			previous, exists := state.objects[name]
			if precondition == 0 && exists || precondition > 0 && (!exists || previous.generation != precondition) {
				http.Error(w, "precondition failed", http.StatusPreconditionFailed)
				return
			}
			state.nextGeneration++
			generation := state.nextGeneration
			metageneration := int64(1)
			if exists {
				metageneration = previous.metageneration + 1
			}
			hold, _ := metadata["temporaryHold"].(bool)
			obj := heldGCSObject{name: name, generation: generation, metageneration: metageneration, payload: payload,
				metadata: stringMap(metadata["metadata"]), temporaryHold: hold}
			state.objects[name] = obj
			if precondition == 0 && state.loseCreate {
				state.loseCreate = false
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			writeHeldGCSAttrs(w, obj)
		case http.MethodGet:
			obj, exists := state.objects[name]
			if !exists {
				http.NotFound(w, r)
				return
			}
			if want := r.URL.Query().Get("generation"); want != "" && want != strconv.FormatInt(obj.generation, 10) {
				http.NotFound(w, r)
				return
			}
			if r.URL.Query().Get("alt") == "media" || strings.HasPrefix(r.URL.EscapedPath(), "/archive/") {
				w.Header().Set("X-Goog-Generation", strconv.FormatInt(obj.generation, 10))
				w.Header().Set("X-Goog-Metageneration", strconv.FormatInt(obj.metageneration, 10))
				w.Header().Set("Content-Length", strconv.Itoa(len(obj.payload)))
				_, _ = w.Write(obj.payload)
				return
			}
			writeHeldGCSAttrs(w, obj)
		case http.MethodPatch:
			state.patches++
			obj, exists := state.objects[name]
			if !exists || r.URL.Query().Get("ifGenerationMatch") != strconv.FormatInt(obj.generation, 10) ||
				r.URL.Query().Get("ifMetagenerationMatch") != strconv.FormatInt(obj.metageneration, 10) {
				http.Error(w, "precondition failed", http.StatusPreconditionFailed)
				return
			}
			if state.onPatch != nil {
				state.onPatch()
			}
			if state.failPatch > 0 {
				state.failPatch--
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			obj.temporaryHold = false
			obj.metageneration++
			state.objects[name] = obj
			writeHeldGCSAttrs(w, obj)
		case http.MethodDelete:
			state.deletes++
			obj, exists := state.objects[name]
			if !exists || r.URL.Query().Get("generation") != strconv.FormatInt(obj.generation, 10) ||
				r.URL.Query().Get("ifGenerationMatch") != strconv.FormatInt(obj.generation, 10) {
				http.Error(w, "precondition failed", http.StatusPreconditionFailed)
				return
			}
			delete(state.objects, name)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unexpected GCS method", http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(server.Close)
	client, err := storage.NewClient(t.Context(), option.WithEndpoint(server.URL+"/storage/v1/"), option.WithHTTPClient(server.Client()), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	fixture.e.SourceMutationStorage = client
	return state, client
}

func readHeldGCSUpload(r *http.Request) (map[string]any, []byte, error) {
	mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "multipart/related" || params["boundary"] == "" {
		return nil, nil, fmt.Errorf("invalid GCS multipart upload: %v", err)
	}
	reader := multipart.NewReader(r.Body, params["boundary"])
	metadataPart, err := reader.NextPart()
	if err != nil {
		return nil, nil, err
	}
	metadataBytes, readErr := io.ReadAll(metadataPart)
	closeErr := metadataPart.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return nil, nil, err
	}
	var metadata map[string]any
	if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
		return nil, nil, err
	}
	payloadPart, err := reader.NextPart()
	if err != nil {
		return nil, nil, err
	}
	payload, readErr := io.ReadAll(payloadPart)
	closeErr = payloadPart.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return nil, nil, err
	}
	return metadata, payload, nil
}

func stringMap(value any) map[string]string {
	result := make(map[string]string)
	for key, value := range asObject(value) {
		if text, ok := value.(string); ok {
			result[key] = text
		}
	}
	return result
}

func writeHeldGCSAttrs(w http.ResponseWriter, obj heldGCSObject) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"kind": "storage#object", "bucket": "archive", "name": obj.name,
		"generation": strconv.FormatInt(obj.generation, 10), "metageneration": strconv.FormatInt(obj.metageneration, 10),
		"size": strconv.Itoa(len(obj.payload)), "temporaryHold": obj.temporaryHold, "metadata": obj.metadata})
}

func heldProfileProvider(t *testing.T, fixture heldProfileFixture) *gcsMutationProvider {
	t.Helper()
	run, err := fixture.e.enrolledMutationRun(t.Context(), fixture.runtime)
	if err != nil {
		t.Fatal(err)
	}
	provider, err := newGCSMutationProviderForRun(fixture.e.SourceMutationStorage, fixture.scope, run)
	if err != nil {
		t.Fatal(err)
	}
	return provider
}

func TestHeldProfileUnknownCreateClosesThroughGuardAndPinsKeyAcrossCompaction(t *testing.T) {
	f := newHeldProfileFixture(t)
	gcs, _ := newHeldGCSServer(t, f)
	provider := heldProfileProvider(t, f)
	ctx := t.Context()
	createPayload := []byte("held generation-zero payload")
	create := f.admitUpload(t, 1, "objects/unknown", fencing.MutationCondition{Mode: fencing.MutationUnconditional}, createPayload)
	if err := f.e.ExecuteSourceMutation(ctx, f.runtime, f.producer, 1, create, f.scope, provider, f.receipts); !errors.Is(err, ErrFencePending) {
		t.Fatalf("lost create response was not kept pending: %v", err)
	}
	if obj := gcs.objects[pathJoin(f.scope.Prefix, create.Key)]; !obj.temporaryHold || !bytes.Equal(obj.payload, createPayload) {
		t.Fatalf("unknown held create was not atomically protected: %+v", obj)
	}

	update := f.admitUpload(t, 2, create.Key, fencing.MutationCondition{Mode: fencing.MutationMatchGeneration, Generation: gcs.objects[pathJoin(f.scope.Prefix, create.Key)].generation}, []byte("replacement"))
	if err := f.e.ExecuteSourceMutation(ctx, f.runtime, f.producer, 2, update, f.scope, provider, f.receipts); !errors.Is(err, ErrFencePending) {
		t.Fatalf("same-key update crossed the unresolved-create key guard: %v", err)
	}
	deleteIntent := f.admitDelete(t, 3, create.Key, gcs.objects[pathJoin(f.scope.Prefix, create.Key)].generation)
	if err := f.e.ExecuteSourceMutation(ctx, f.runtime, f.producer, 3, deleteIntent, f.scope, provider, f.receipts); !errors.Is(err, ErrFencePending) {
		t.Fatalf("same-key delete crossed the unresolved-create key guard: %v", err)
	}
	if _, err := f.e.changeMutationProducer(ctx, f.runtime, func(p *fencing.MutationProducer) error {
		if err := p.CancelUnprepared(2, hashBytes([]byte("cancel-update"))); err != nil {
			return err
		}
		return p.CancelUnprepared(3, hashBytes([]byte("cancel-delete")))
	}); err != nil {
		t.Fatal(err)
	}

	casPayload := []byte("normal CAS update")
	cas := f.admitUpload(t, 4, "objects/cas", fencing.MutationCondition{Mode: fencing.MutationUnconditional}, casPayload)
	gcs.objects[pathJoin(f.scope.Prefix, cas.Key)] = heldGCSObject{name: pathJoin(f.scope.Prefix, cas.Key), generation: 5, metageneration: 1,
		payload: []byte("old"), metadata: map[string]string{}}
	if err := f.e.ExecuteSourceMutation(ctx, f.runtime, f.producer, 4, cas, f.scope, provider, f.receipts); err != nil {
		t.Fatalf("held profile rejected ordinary nonzero-generation upload: %v", err)
	}
	producer, _, _, err := f.e.AuthorityState.ReadProducer(ctx, f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	casEntry := producer.Entries[4]
	if casEntry.Intent.Condition.Mode != fencing.MutationUnconditional || casEntry.Preparation == nil || casEntry.Preparation.Generation != 5 {
		t.Fatalf("nonzero unconditional upload did not preserve its original intent and observed fixed CAS: %+v", casEntry)
	}
	gcs.objects[pathJoin(f.scope.Prefix, "objects/delete")] = heldGCSObject{name: pathJoin(f.scope.Prefix, "objects/delete"), generation: 9, metageneration: 1, payload: []byte("old"), metadata: map[string]string{}}
	deleteOtherIntent := f.admitDelete(t, 5, "objects/delete", 9)
	if err := f.e.ExecuteSourceMutation(ctx, f.runtime, f.producer, 5, deleteOtherIntent, f.scope, provider, f.receipts); err != nil {
		t.Fatalf("held profile rejected ordinary exact-generation delete: %v", err)
	}
	if _, exists := gcs.objects[pathJoin(f.scope.Prefix, "objects/delete")]; exists || gcs.deletes != 1 {
		t.Fatal("exact-generation delete did not execute through the held profile")
	}

	for sequence := uint64(6); sequence <= 64; sequence++ {
		intent := fencing.MutationIntent{ContractVersion: fencing.MutationIntentVersion, StoreScopeHash: hashJSON(f.scope),
			OwnerStep:   fencing.MutationOwnerStep{Owner: "test", Workflow: "held-profile", Step: fmt.Sprint(sequence), Attempt: 1},
			Condition:   fencing.MutationCondition{Mode: fencing.MutationMatchGeneration, Generation: 1},
			OperationID: fencing.MutationRequestID(f.producer, sequence), Kind: "delete", Key: fmt.Sprintf("unused/%d", sequence)}
		if _, err := f.e.changeMutationProducer(ctx, f.runtime, func(p *fencing.MutationProducer) error {
			if err := p.Admit(sequence, intent); err != nil {
				return err
			}
			return p.CancelUnprepared(sequence, hashBytes([]byte(fmt.Sprint("cancel-", sequence))))
		}); err != nil {
			t.Fatalf("admit/cancel compaction filler %d: %v", sequence, err)
		}
	}
	closedRoot, err := f.e.change(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID, func(l *fencing.Ledger) error {
		return l.Close("held-profile-close", hashBytes([]byte("held-profile-close")))
	})
	if err != nil {
		t.Fatal(err)
	}
	complete, err := f.e.closeMutationProducers(ctx, closedRoot, nil)
	if err != nil || !complete {
		t.Fatalf("closed held producer did not settle its actual barrier: complete=%v err=%v", complete, err)
	}
	producer, _, _, err = f.e.AuthorityState.ReadProducer(ctx, f.runtime)
	if err != nil || !producer.BarrierComplete() {
		t.Fatalf("guarded logical resolution did not settle barrier: %+v err=%v", producer, err)
	}
	entry := producer.Entries[1]
	if entry.Intent != create || entry.GuardedLogicalResolution == nil || entry.GuardedLogicalResolution.Intent != create || entry.Resolution == nil || entry.Resolution.AllCopiesTerminal || entry.Dispatches[entry.GuardedLogicalResolution.DispatchID].ReceiptDigest != "" {
		t.Fatalf("settlement forged physical terminal evidence: %+v", entry)
	}

	if err := f.e.compactSourceMutationJournal(ctx, f.runtime, 65); err != nil {
		t.Fatalf("compact guarded logical entry: %v", err)
	}
	identity := fencing.Identity{SourceClusterID: f.runtime.SourceClusterID, NodeID: f.runtime.NodeID, PodUID: f.runtime.PodUID, RunID: f.runtime.RunID, WALIdentity: "wal-held-profile"}
	archived, err := f.e.lookupMutation(ctx, f.runtime.StatefulSetUID, identity, fencing.MutationLookupRequest{ProducerUID: f.producer, Sequence: 1, Intent: create})
	if err != nil || !archived.Archived || archived.Entry == nil || archived.Entry.GuardedLogicalResolution == nil {
		t.Fatalf("compaction or stage retirement lost guarded proof: %+v err=%v", archived, err)
	}
	key := pathJoin(f.scope.Prefix, create.Key)
	gcs.mu.Lock()
	if !gcs.objects[key].temporaryHold {
		gcs.mu.Unlock()
		t.Fatal("guarded logical settlement released the permanent late-effect hold")
	}
	delete(gcs.objects, key)
	postsBeforeRetry := gcs.posts
	gcs.mu.Unlock()
	if err := f.e.ExecuteSourceMutation(ctx, f.runtime, f.producer, 1, create, f.scope, provider, f.receipts); !errors.Is(err, ErrFencePending) {
		t.Fatalf("retry recreated an object after its guarded commit disappeared: %v", err)
	}
	gcs.mu.Lock()
	_, recreated := gcs.objects[key]
	postsAfterRetry := gcs.posts
	gcs.mu.Unlock()
	if recreated || postsAfterRetry != postsBeforeRetry {
		t.Fatalf("guarded retry resurrected the missing object: recreated=%v posts=%d->%d", recreated, postsBeforeRetry, postsAfterRetry)
	}

	restarted := &FencingExecutor{AuthorityState: f.e.AuthorityState}
	owner := hashJSON([]any{f.runtime, "wal-held-profile", f.producer, uint64(1), create.OperationID})
	if err := restarted.acquireMutationKey(ctx, create.StoreScopeHash, create.Key, owner, false); err != nil {
		t.Fatalf("restart could not observe the guarded key pin: %v", err)
	}
	if err := restarted.acquireMutationKey(ctx, create.StoreScopeHash, create.Key, owner+"replacement", false); !errors.Is(err, ErrFencePending) {
		t.Fatalf("same-key mutation owner crossed the post-compaction guard after restart: %v", err)
	}
	restarted.Kube = f.e.Kube
	restarted.SourceMutationStorage = f.e.SourceMutationStorage
	restartedReceipts, err := restarted.receiptsForRuntime(f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	gcs.mu.Lock()
	postsBeforeStaleMutations, deletesBeforeStaleMutations := gcs.posts, gcs.deletes
	gcs.mu.Unlock()
	for _, mutation := range []struct {
		name   string
		seq    uint64
		intent fencing.MutationIntent
	}{{name: "update", seq: 2, intent: update}, {name: "delete", seq: 3, intent: deleteIntent}} {
		if err := restarted.ExecuteSourceMutation(ctx, f.runtime, f.producer, mutation.seq, mutation.intent, f.scope, provider, restartedReceipts); !errors.Is(err, ErrFencePending) {
			t.Errorf("post-compaction same-key %s crossed the held guard: %v", mutation.name, err)
		}
	}
	gcs.mu.Lock()
	postsAfterStaleMutations, deletesAfterStaleMutations := gcs.posts, gcs.deletes
	gcs.mu.Unlock()
	if postsAfterStaleMutations != postsBeforeStaleMutations || deletesAfterStaleMutations != deletesBeforeStaleMutations {
		t.Fatalf("post-compaction stale mutations reached GCS: posts=%d->%d deletes=%d->%d",
			postsBeforeStaleMutations, postsAfterStaleMutations, deletesBeforeStaleMutations, deletesAfterStaleMutations)
	}
	if gcs.patches != 0 {
		t.Fatalf("unknown guarded commit incorrectly released a hold: patches=%d", gcs.patches)
	}
}

func TestHeldProfilePersistsReceiptBeforeReleaseAndRetriesAfterExecutorRestart(t *testing.T) {
	f := newHeldProfileFixture(t)
	gcs, _ := newHeldGCSServer(t, f)
	gcs.loseCreate = false
	gcs.failPatch = 1
	provider := heldProfileProvider(t, f)
	intent := f.admitUpload(t, 1, "objects/known", fencing.MutationCondition{Mode: fencing.MutationUnconditional}, []byte("known committed bytes"))
	owner := hashJSON([]any{f.runtime, "wal-held-profile", f.producer, uint64(1), intent.OperationID})
	gcs.onPatch = func() {
		receipt, receiptErr := f.receipts.get("receipt-" + owner)
		producer, uid, _, producerErr := f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
		if receiptErr != nil || producerErr != nil {
			t.Errorf("hold release was attempted before durable receipt and producer resolution: receiptErr=%v producerErr=%v", receiptErr, producerErr)
			return
		}
		entry := producer.Entries[1]
		if receipt.Intent != intent || uid != f.producer || entry.Resolution == nil || !entry.Resolution.AllCopiesTerminal {
			t.Errorf("hold release was attempted before durable receipt and producer resolution: receipt=%+v entry=%+v", receipt, entry)
		}
	}
	if err := f.e.ExecuteSourceMutation(t.Context(), f.runtime, f.producer, 1, intent, f.scope, provider, f.receipts); !errors.Is(err, ErrFencePending) {
		t.Fatalf("failed exact hold-release attempt was not kept pending: %v", err)
	}
	producer, _, _, err := f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
	if err != nil || producer.Entries[1].Resolution == nil || !producer.Entries[1].Resolution.AllCopiesTerminal {
		t.Fatalf("known success was not durably recorded before retry: %+v err=%v", producer, err)
	}
	restarted := &FencingExecutor{Kube: f.e.Kube, AuthorityState: f.e.AuthorityState, SourceMutationStorage: f.e.SourceMutationStorage, MutationProfile: fencing.MutationHeldExecutionProfile}
	restartedReceipts, err := restarted.receiptsForRuntime(f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.ExecuteSourceMutation(t.Context(), f.runtime, f.producer, 1, intent, f.scope, heldProfileProvider(t, f), restartedReceipts); err != nil {
		t.Fatalf("executor restart did not finish exact hold release from its receipt: %v", err)
	}
	obj := gcs.objects[pathJoin(f.scope.Prefix, intent.Key)]
	if gcs.posts != 1 || gcs.patches != 2 || obj.temporaryHold || obj.metageneration != 2 {
		t.Fatalf("release retry changed the upload or missed the exact hold transition: posts=%d patches=%d object=%+v", gcs.posts, gcs.patches, obj)
	}
	lock, err := readMutationKeyForTest(t.Context(), f.e, intent.StoreScopeHash, intent.Key)
	if err != nil || lock.Owner != "" {
		t.Fatalf("successful finalization did not release the canonical key lock: %+v err=%v", lock, err)
	}
}

func TestHeldProfileUnconditionalZeroGenerationWrongBytesRemainPending(t *testing.T) {
	f := newHeldProfileFixture(t)
	gcs, _ := newHeldGCSServer(t, f)
	provider := heldProfileProvider(t, f)
	intent := f.admitUpload(t, 1, "objects/wrong-bytes", fencing.MutationCondition{Mode: fencing.MutationUnconditional}, []byte("admitted payload"))
	if err := f.e.ExecuteSourceMutation(t.Context(), f.runtime, f.producer, 1, intent, f.scope, provider, f.receipts); !errors.Is(err, ErrFencePending) {
		t.Fatalf("lost response did not remain unresolved: %v", err)
	}
	key := pathJoin(f.scope.Prefix, intent.Key)
	gcs.mu.Lock()
	object := gcs.objects[key]
	object.payload = []byte("different bytes")
	gcs.objects[key] = object
	gcs.mu.Unlock()
	closedRoot, err := f.e.change(t.Context(), f.runtime.StatefulSetUID, f.runtime.SourceClusterID, func(l *fencing.Ledger) error {
		return l.Close("held-profile-wrong-bytes", hashBytes([]byte("held-profile-wrong-bytes")))
	})
	if err != nil {
		t.Fatal(err)
	}
	complete, err := f.e.closeMutationProducers(t.Context(), closedRoot, nil)
	if err != nil || complete {
		t.Fatalf("wrong object bytes incorrectly completed held-create recovery: complete=%v err=%v", complete, err)
	}
	producer, _, _, err := f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	entry := producer.Entries[1]
	if entry.GuardedLogicalResolution != nil || entry.Resolution != nil || !entry.DispatchClosed {
		t.Fatalf("wrong object bytes were treated as guarded logical settlement: %+v", entry)
	}
	lock, err := readMutationKeyForTest(t.Context(), f.e, intent.StoreScopeHash, intent.Key)
	if err != nil || lock.Owner == "" {
		t.Fatalf("wrong-byte uncertainty lost its durable key guard: %+v err=%v", lock, err)
	}
}

func pathJoin(prefix, key string) string {
	return strings.TrimSuffix(prefix, "/") + "/" + strings.TrimPrefix(key, "/")
}
