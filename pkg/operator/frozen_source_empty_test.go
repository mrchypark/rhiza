package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"strconv"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
	"google.golang.org/api/option"
)

type frozenSourceEmptyTestBucket struct {
	objstore.Bucket
}

func (frozenSourceEmptyTestBucket) BucketAttrs(context.Context) (*storage.BucketAttrs, error) {
	return &storage.BucketAttrs{Name: "archive", MetaGeneration: 1}, nil
}

func newFrozenEmptyHeldFixture(t *testing.T) heldProfileFixture {
	t.Helper()
	e, api, id, runtime := newMutationStoreFixture(t)
	authority, err := NewAuthorityStateStore(api.authorityBucket, "root")
	if err != nil {
		t.Fatal(err)
	}
	e.AuthorityState = authority
	e.MutationProfile = fencing.MutationHeldExecutionProfile
	e.RequireMutationJournal = true
	if err := e.initializeRoot(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID); err != nil {
		t.Fatal(err)
	}
	if _, err := e.change(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID, func(ledger *fencing.Ledger) error {
		ledger.RequireRuntimeBinding = true
		ledger.RequireMutationJournal = true
		return ledger.RegisterEnrolled(id, runtime)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.reserveMutationProducer(t.Context(), runtime); err != nil {
		t.Fatal(err)
	}
	scope := AttestationStoreScope{Provider: "gcs", Bucket: "archive", Prefix: "root/source"}
	stages, err := e.sourceStagesFor(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	id.WALIdentity = "wal-frozen-empty"
	if _, err := e.change(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID, func(ledger *fencing.Ledger) error {
		if err := ledger.BindMutationScope(id, hashJSON(scope), fencing.MutationHeldExecutionProfile); err != nil {
			return err
		}
		if err := ledger.BindMutationStage(id, stages.incarnation); err != nil {
			return err
		}
		return ledger.Bind(id)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.changeMutationProducer(t.Context(), runtime, func(producer *fencing.MutationProducer) error {
		return producer.Activate(id)
	}); err != nil {
		t.Fatal(err)
	}
	run, err := e.enrolledMutationRun(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	receipts, err := e.receiptsForRuntime(runtime)
	if err != nil {
		t.Fatal(err)
	}
	configureMutationWorkloadFixture(t, e, api, id, runtime)
	return heldProfileFixture{e: e, api: api, id: id, runtime: runtime, producer: run.MutationProducerUID,
		scope: scope, stages: stages, receipts: receipts}
}

func newFrozenEmptyNoEffectServer(t *testing.T, fixture heldProfileFixture) *heldGCSServer {
	t.Helper()
	state := &heldGCSServer{objects: make(map[string]heldGCSObject), nextGeneration: 100}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Query().Get("name")
		if name == "" {
			escapedPath := r.URL.EscapedPath()
			if strings.HasPrefix(escapedPath, "/storage/v1/b/archive/o/") {
				name, _ = url.PathUnescape(strings.TrimPrefix(escapedPath, "/storage/v1/b/archive/o/"))
			} else if strings.HasPrefix(escapedPath, "/archive/") {
				name, _ = url.PathUnescape(strings.TrimPrefix(escapedPath, "/archive/"))
			}
		}
		state.mu.Lock()
		defer state.mu.Unlock()
		switch r.Method {
		case http.MethodPost:
			state.posts++
			metadata, payload, err := readHeldGCSUpload(r)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if state.posts == 1 {
				// The request reached the provider, but this deterministic fault
				// returns an error before creating the object.
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			precondition, _ := strconv.ParseInt(r.URL.Query().Get("ifGenerationMatch"), 10, 64)
			_, exists := state.objects[name]
			if name == "" || precondition != 0 || exists {
				http.Error(w, "precondition failed", http.StatusPreconditionFailed)
				return
			}
			state.nextGeneration++
			obj := heldGCSObject{name: name, generation: state.nextGeneration, metageneration: 1, payload: payload,
				metadata: stringMap(metadata["metadata"])}
			obj.temporaryHold, _ = metadata["temporaryHold"].(bool)
			state.objects[name] = obj
			writeHeldGCSAttrs(w, obj)
		case http.MethodGet:
			obj, exists := state.objects[name]
			if !exists || (r.URL.Query().Get("generation") != "" && r.URL.Query().Get("generation") != strconv.FormatInt(obj.generation, 10)) {
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
	return state
}

func callFrozenSourcePreflight(t *testing.T, executor *FencingExecutor, request FrozenSourcePreflightRequest) (FrozenSourcePreflightResponse, error) {
	t.Helper()
	executor.ControllerServiceAccount = "default"
	server := httptest.NewTLSServer(executor.Handler())
	t.Cleanup(server.Close)
	body, err := json.Marshal(request)
	if err != nil {
		return FrozenSourcePreflightResponse{}, err
	}
	call, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+frozenSourcePreflightPath, bytes.NewReader(body))
	if err != nil {
		return FrozenSourcePreflightResponse{}, err
	}
	call.Header.Set("Authorization", "Bearer controller-token")
	call.Header.Set("Content-Type", "application/json")
	response, err := server.Client().Do(call)
	if err != nil {
		return FrozenSourcePreflightResponse{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return FrozenSourcePreflightResponse{}, fmt.Errorf("preflight HTTP %d", response.StatusCode)
	}
	var result FrozenSourcePreflightResponse
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return FrozenSourcePreflightResponse{}, err
	}
	return result, nil
}

func persistValidEmptyRecoveryForPreflight(t *testing.T, fixture heldProfileFixture) FrozenSourcePreflightRequest {
	t.Helper()
	spec := Spec{StatefulSet: "rhiza", SourceClusterID: fixture.runtime.SourceClusterID, RecoveryID: "source-fence",
		Durability: "async", AllowDataLoss: true,
		Fence: Fence{RecoveryID: "source-fence", ClusterID: fixture.runtime.SourceClusterID, StatefulSetUID: fixture.runtime.StatefulSetUID}}
	resource := Resource{APIVersion: "rhiza.mrchypark.dev/v1alpha1", Kind: "RhizaRecovery",
		Metadata: object{"name": "rhiza-recovery", "uid": "recovery-uid", "namespace": "test"}, Spec: spec,
		Status: Status{StatefulSetUID: fixture.runtime.StatefulSetUID, Source: fixture.runtime.SourceClusterID,
			RecoveryMode: RecoveryModeFrozenSource, RecoveryID: spec.RecoveryID, SpecHash: immutableSpecHash(spec),
			SourceDurability: "async", TargetBaseKind: "empty", FrozenEmptyGenesisPending: true}}
	data, err := json.Marshal(resource)
	if err != nil {
		t.Fatal(err)
	}
	var value object
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatal(err)
	}
	fixture.api.mu.Lock()
	fixture.api.objects[fixture.e.controller().resourcePath(resourceName(&resource))] = value
	fixture.api.mu.Unlock()
	return FrozenSourcePreflightRequest{RecoveryName: resourceName(&resource), RecoveryUID: "recovery-uid"}
}

// This covers the crash window after a durable source genesis dispatch grant
// but before any provider effect. Closure may replay only that exact held
// create; its logical resolution must remain explicitly nonterminal.
func TestFrozenEmptyRecoverySettlesUnknownHeldGenesisAfterClosure(t *testing.T) {
	f := newFrozenEmptyHeldFixture(t)
	ctx := t.Context()
	archive := objstore.NewInMemBucket()
	t.Cleanup(func() { _ = archive.Close() })
	f.e.ArchiveBucket = frozenSourceEmptyTestBucket{Bucket: archive}
	if _, err := f.e.change(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID, func(ledger *fencing.Ledger) error {
		ledger.ArchiveGenesisState = fencing.ArchiveGenesisRegistrationPending
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := f.e.claimArchiveScopeForRegistration(ctx, f.runtime.StatefulSetUID, f.id, f.runtime); err != nil {
		t.Fatal(err)
	}

	// The first real provider request fails before creating the object. The
	// resulting dispatched tuple remains unknown until closure proves it can be
	// replayed under the held guard.
	gcs := newFrozenEmptyNoEffectServer(t, f)
	payload, err := recovery.EmptyArchiveHeadBytes()
	if err != nil {
		t.Fatal(err)
	}
	intent := f.admitUpload(t, 1, "archive/head.bin", fencing.MutationCondition{Mode: fencing.MutationCreateOnly}, payload)
	if err := f.e.executeSourceMutationRequest(ctx, "Bearer test", f.id, sourceMutationRequest{
		ProducerUID: f.producer, Sequence: 1, Intent: intent,
	}, true); !errors.Is(err, ErrFencePending) {
		t.Fatalf("provider failure did not leave the exact genesis dispatch pending: %v", err)
	}
	root, err := f.e.read(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
	claim, _, claimErr := f.e.AuthorityState.ReadArchiveScopeClaim(ctx, fencing.ArchiveScope{
		Provider: "gcs", Bucket: "archive", Prefix: f.scope.Prefix, ClusterID: f.runtime.SourceClusterID,
	})
	gcs.mu.Lock()
	postsBeforeDispatch := gcs.posts
	gcs.mu.Unlock()
	if err != nil || claimErr != nil || root.Ledger.ArchiveGenesisState != fencing.ArchiveGenesisCommitted ||
		root.Ledger.ArchiveGenesisRef == nil || root.Ledger.ArchiveGenesisRef.Mutation == nil ||
		root.Ledger.ArchiveGenesisRef.Mutation.Intent != intent || claim.GenesisRef == nil ||
		!sameArchiveGenesisRef(root.Ledger.ArchiveGenesisRef, claim.GenesisRef) || postsBeforeDispatch < 1 {
		t.Fatalf("genesis authority was not committed before the failed provider write: root=%+v claim=%+v posts=%d errors=%v/%v", root.Ledger, claim, postsBeforeDispatch, err, claimErr)
	}
	producer, _, _, err := f.e.AuthorityState.ReadProducer(ctx, f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	entry := producer.Entries[1]
	var dispatchID string
	for id := range entry.Dispatches {
		dispatchID = id
	}
	if entry.Intent != intent || entry.Preparation == nil || entry.Preparation.Profile != fencing.MutationHeldExecutionProfile ||
		!entry.Preparation.GenerationSet || entry.Preparation.Generation != 0 || len(entry.Dispatches) != 1 || dispatchID == "" ||
		entry.Dispatches[dispatchID].ReceiptDigest != "" || entry.GuardedLogicalResolution != nil || entry.Resolution != nil {
		t.Fatalf("dispatched source genesis was not left unknown before provider effect: %+v", entry)
	}
	key := path.Join(f.scope.Prefix, intent.Key)
	gcs.mu.Lock()
	_, exists := gcs.objects[key]
	postsBeforeClosure := gcs.posts
	gcs.mu.Unlock()
	dataHeadExists, dataHeadErr := archive.Exists(ctx, key)
	if exists || postsBeforeClosure < 1 || dataHeadErr != nil || dataHeadExists {
		t.Fatalf("unknown initial dispatch unexpectedly had a provider effect: providerExists=%v archiveExists=%v posts=%d err=%v", exists, dataHeadExists, postsBeforeClosure, dataHeadErr)
	}

	archiveScope := fencing.ArchiveScope{Provider: "gcs", Bucket: "archive", Prefix: "root/source", ClusterID: "source"}
	scopeHash, err := archiveScope.Hash()
	if err != nil {
		t.Fatal(err)
	}
	targetID, err := recovery.FrozenTargetID(recovery.FrozenSourceIdentity{ScopeHash: scopeHash, ClusterID: "source", StatefulSetUID: f.runtime.StatefulSetUID})
	if err != nil {
		t.Fatal(err)
	}
	fenceRequest := FenceRequest{LogicalID: "guarded-empty-recovery", BindingUID: f.runtime.BindingUID, Namespace: "test", StatefulSet: "rhiza",
		StatefulSetUID: f.runtime.StatefulSetUID, SourceClusterID: "source", OperationID: "source-fence", Scope: ScopeGeneration}
	initialRecoverySpec := Spec{StatefulSet: "rhiza", SourceClusterID: "source", RecoveryID: fenceRequest.OperationID,
		Durability: "async", AllowDataLoss: true, Fence: Fence{RecoveryID: fenceRequest.OperationID,
			ClusterID: f.runtime.SourceClusterID, StatefulSetUID: f.runtime.StatefulSetUID}}
	recoveryResource := Resource{APIVersion: "rhiza.mrchypark.dev/v1alpha1", Kind: "RhizaRecovery",
		Metadata: object{"name": "rhiza-recovery", "uid": "recovery-uid", "namespace": "test"},
		Spec:     initialRecoverySpec,
		Status: Status{StatefulSetUID: f.runtime.StatefulSetUID, Source: f.runtime.SourceClusterID, Target: targetID,
			TargetBaseKind: "empty", RecoveryMode: RecoveryModeFrozenSource, Stage: "FrozenEmpty",
			RecoveryID: fenceRequest.OperationID, SpecHash: immutableSpecHash(initialRecoverySpec),
			SourceDurability: "async", FrozenEmptyGenesisPending: true}}
	resourceData, err := json.Marshal(recoveryResource)
	if err != nil {
		t.Fatal(err)
	}
	var resourceObject object
	if err := json.Unmarshal(resourceData, &resourceObject); err != nil {
		t.Fatal(err)
	}
	f.api.mu.Lock()
	f.api.objects[f.e.controller().resourcePath("rhiza-recovery")] = resourceObject
	f.api.mu.Unlock()
	preflight, err := callFrozenSourcePreflight(t, f.e, FrozenSourcePreflightRequest{RecoveryName: "rhiza-recovery", RecoveryUID: "recovery-uid"})
	if err != nil || !preflight.EmptyGenesisEligible || preflight.ClosureReady || preflight.Identity != (recovery.FrozenSourceIdentity{
		ScopeHash: scopeHash, ClusterID: f.runtime.SourceClusterID, StatefulSetUID: f.runtime.StatefulSetUID}) {
		t.Fatalf("authenticated controller preflight did not identify the exact pending genesis: response=%+v err=%v", preflight, err)
	}

	sourceMembers := []quepaxa.Member{forkTestMember(t, "guarded-empty-source")}
	targetMembers := []quepaxa.Member{forkTestMember(t, "guarded-empty-target")}
	container := list(nested(f.api.authStatefulSet, "spec", "template", "spec", "containers"))[0]
	envSlice := replaceEnv(list(asObject(container)["env"]), "RHIZA_OBJSTORE_DURABILITY", object{"value": "async"})
	envSlice = replaceEnv(envSlice, "RHIZA_CLUSTER_MEMBERS", object{"value": string(jsonBytes(sourceMembers))})
	asObject(container)["env"] = envSlice
	pod := independentPod(f.id.NodeID, f.id.PodUID)
	asObject(pod["metadata"])["ownerReferences"] = []any{object{"kind": "StatefulSet", "name": "rhiza", "uid": f.runtime.StatefulSetUID, "controller": true}}
	asObject(list(nested(pod, "spec", "containers"))[0])["env"] = envSlice
	asObject(pod["status"])["containerStatuses"] = []any{object{"containerID": "containerd://container", "imageID": "sha256:image"}}
	f.api.mu.Lock()
	f.api.objects["/apis/apps/v1/namespaces/test/statefulsets"] = object{"items": []any{f.api.authStatefulSet}}
	f.api.objects["/api/v1/namespaces/test/pods"] = object{"items": []any{pod}}
	f.api.mu.Unlock()
	if err := archive.Upload(ctx, "root/source/voters/membership.json", bytes.NewReader(jsonBytes(recovery.NewMembershipRecord("source", sourceMembers, "async"))), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	f.e.CollectRuntimeEvidence = func(context.Context, fencing.RuntimeBinding, fencing.StopRequest) ([]byte, error) {
		return []byte("verified-runtime-terminal"), nil
	}
	f.e.VerifyRuntimeEvidence = func(_ context.Context, _ fencing.RuntimeBinding, _ fencing.StopRequest, receipt []byte) error {
		if string(receipt) != "verified-runtime-terminal" {
			return errors.New("unexpected runtime terminal evidence")
		}
		return nil
	}
	fence := &fenceResource{Metadata: object{"name": "source-fence", "namespace": "test"}, Spec: fenceRequest}
	fenceCollection := "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizafences"
	if err := f.e.reconcileFence(ctx, fenceCollection, fence); err != nil {
		t.Fatalf("close source generation with guarded genesis outstanding: %v", err)
	}
	producer, _, _, err = f.e.AuthorityState.ReadProducer(ctx, f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	entry = producer.Entries[1]
	if producer.Phase != fencing.MutationClosed || entry.Resolution == nil || entry.Resolution.Outcome != fencing.MutationCommitted ||
		entry.Resolution.AllCopiesTerminal || entry.GuardedLogicalResolution == nil || len(entry.Dispatches) != 1 ||
		entry.Dispatches[dispatchID].ReceiptDigest != "" {
		t.Fatalf("closed-generation replay did not preserve a guarded nonterminal genesis: producer=%+v entry=%+v", producer, entry)
	}
	gcs.mu.Lock()
	storedObject, exists := gcs.objects[key]
	postsAtClose := gcs.posts
	gcs.mu.Unlock()
	if !exists || postsAtClose != postsBeforeDispatch+1 || !storedObject.temporaryHold || !bytes.Equal(storedObject.payload, payload) {
		t.Fatalf("closed-generation replay did not create the exact held genesis once: exists=%v posts=%d object=%+v", exists, postsAtClose, storedObject)
	}
	if err := archive.Upload(ctx, key, bytes.NewReader(storedObject.payload), objstore.WithIfNotExists()); err != nil {
		t.Fatalf("mirror exact provider object for archive reader: %v", err)
	}
	fenceName := "fence-" + hashJSON([]string{f.runtime.BindingUID, fenceRequest.OperationID})[:40]
	fence.Metadata["name"], fence.Metadata["namespace"], fence.Metadata["uid"], fence.Metadata["resourceVersion"] = fenceName, "test", "source-fence-uid", "1"
	fenceData, err := json.Marshal(fence)
	if err != nil {
		t.Fatal(err)
	}
	var fenceObject object
	if err := json.Unmarshal(fenceData, &fenceObject); err != nil {
		t.Fatal(err)
	}
	f.api.mu.Lock()
	f.api.objects[fenceCollection+"/"+fenceName] = fenceObject
	f.api.objects[fenceCollection] = object{"items": []any{fenceObject}}
	f.api.mu.Unlock()

	bucketEnvironment, err := captureFrozenBucketEnvironment(ctx, f.e.ArchiveBucket.(interface {
		BucketAttrs(context.Context) (*storage.BucketAttrs, error)
	}), "archive", "root/source")
	if err != nil {
		t.Fatal(err)
	}
	recoverySpec := Spec{StatefulSet: "rhiza", SourceClusterID: "source", RecoveryID: fenceRequest.OperationID, Durability: "async", AllowDataLoss: true,
		Fence: Fence{RecoveryID: fenceRequest.OperationID, ClusterID: f.runtime.SourceClusterID, StatefulSetUID: f.runtime.StatefulSetUID}}
	recoveryResource.Spec = recoverySpec
	recoveryResource.Status.FrozenBucketEnvironment = &bucketEnvironment
	request := ArchiveEmptyRecoveryRequest{RecoveryName: "rhiza-recovery", StatefulSetUID: f.runtime.StatefulSetUID,
		TargetClusterID: targetID, OperationID: "recovery-uid:" + fenceRequest.OperationID, SourceFenceOperationID: fenceRequest.OperationID,
		TargetDurability: "async", AllowDataLoss: true, PolicyDigest: hashJSON(recoverySpec), RecoveryMode: RecoveryModeFrozenSource,
		BucketEnvironment: &bucketEnvironment}
	targetMembership := recovery.NewMembershipRecord(targetID, targetMembers, "async")
	targetMembershipBytes := jsonBytes(targetMembership)
	request.TargetMembershipHash = authorityStateDigest(targetMembershipBytes)
	targetPrefix := path.Join("root", targetID)
	if err := archive.Upload(ctx, path.Join(targetPrefix, "voters/membership.json"), bytes.NewReader(targetMembershipBytes), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	resourceData, err = json.Marshal(recoveryResource)
	if err != nil {
		t.Fatal(err)
	}
	resourceObject = object{}
	if err := json.Unmarshal(resourceData, &resourceObject); err != nil {
		t.Fatal(err)
	}
	f.api.mu.Lock()
	f.api.objects[f.e.controller().resourcePath(request.RecoveryName)] = resourceObject
	f.api.mu.Unlock()
	closedPreflight, err := callFrozenSourcePreflight(t, f.e, FrozenSourcePreflightRequest{RecoveryName: "rhiza-recovery", RecoveryUID: "recovery-uid"})
	if err != nil || !closedPreflight.ClosureReady || !closedPreflight.EmptyGenesisEligible ||
		closedPreflight.Identity != preflight.Identity || closedPreflight.ProofDigest == "" || closedPreflight.ClosedRootDigest == "" {
		t.Fatalf("authenticated controller preflight did not verify the closed source proof: response=%+v err=%v", closedPreflight, err)
	}
	targetScope := fencing.ArchiveScope{Provider: "gcs", Bucket: "archive", Prefix: targetPrefix, ClusterID: targetID}
	wrongRequest := request
	wrongRequest.PolicyDigest = hashJSON("different-persisted-spec")
	if _, err := f.e.RegisterArchiveEmptyRecovery(ctx, wrongRequest); err == nil || !strings.Contains(err.Error(), "request differs from persisted recovery resource") {
		t.Fatalf("initial registration accepted a raw request digest that differs from the stored spec: %v", err)
	}
	if _, _, err := f.e.AuthorityState.ReadArchiveScopeClaim(ctx, targetScope); !errors.Is(err, errAuthorityStateNotFound) {
		t.Fatalf("rejected initial request created a target archive claim: %v", err)
	}
	if _, _, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, targetID); !errors.Is(err, errAuthorityStateNotFound) {
		t.Fatalf("rejected initial request created a target CONTROL root: %v", err)
	}

	registered, err := f.e.RegisterArchiveEmptyRecovery(ctx, request)
	if err != nil || registered.State != fencing.EmptyRecoveryAuthorized {
		t.Fatalf("guarded zero-tip source did not authorize a fresh target: response=%+v err=%v", registered, err)
	}
	wantPolicyDigest := emptyRecoveryPolicyDigest(request.PolicyDigest, "async", request.TargetDurability, request.AllowDataLoss)
	if registered.Authorization.PolicyDigest != wantPolicyDigest {
		t.Fatalf("empty recovery authorization policy digest=%q, want derived digest %q", registered.Authorization.PolicyDigest, wantPolicyDigest)
	}
	claimBeforeBadReplay, _, err := f.e.AuthorityState.ReadArchiveScopeClaim(ctx, targetScope)
	if err != nil {
		t.Fatal(err)
	}
	rootBeforeBadReplay, _, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, targetID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.e.RegisterArchiveEmptyRecovery(ctx, wrongRequest); err == nil || !strings.Contains(err.Error(), "request differs from persisted recovery resource") {
		t.Fatalf("replay accepted a raw request digest that differs from the unchanged stored spec: %v", err)
	}
	claimAfterBadReplay, _, claimErr := f.e.AuthorityState.ReadArchiveScopeClaim(ctx, targetScope)
	rootAfterBadReplay, _, rootErr := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, targetID)
	if claimErr != nil || rootErr != nil || hashJSON(claimBeforeBadReplay) != hashJSON(claimAfterBadReplay) || hashJSON(rootBeforeBadReplay) != hashJSON(rootAfterBadReplay) {
		t.Fatalf("rejected request replay rewrote persisted authorization: claimErr=%v rootErr=%v", claimErr, rootErr)
	}
	producer, _, _, err = f.e.AuthorityState.ReadProducer(ctx, f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	entry = producer.Entries[1]
	if entry.GuardedLogicalResolution == nil || entry.Resolution == nil || entry.Resolution.AllCopiesTerminal {
		t.Fatalf("empty recovery fabricated terminal physical evidence: %+v", entry)
	}
	targetClaim, _, err := f.e.AuthorityState.ReadArchiveScopeClaim(ctx, targetScope)
	if err != nil || targetClaim.EmptyRecoveryAuthorization == nil || targetClaim.GenesisRef != nil {
		t.Fatalf("authorization was not separately persisted before target genesis: claim=%+v err=%v", targetClaim, err)
	}
	targetRoot, _, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, targetID)
	if err != nil || targetRoot.Ledger.ArchiveGenesisState != fencing.ArchiveGenesisAwaitingRef || targetRoot.Ledger.ArchiveGenesisRef != nil ||
		targetRoot.Ledger.EmptyRecoveryAuthorization == nil {
		t.Fatalf("target root did not remain authorized but uncommitted pending a real genesis receipt: root=%+v err=%v", targetRoot.Ledger, err)
	}
	gcs.mu.Lock()
	postsAfterAuthorization := gcs.posts
	finalHead := gcs.objects[key]
	gcs.mu.Unlock()
	if postsAfterAuthorization != postsAtClose || !finalHead.temporaryHold || !bytes.Equal(finalHead.payload, payload) {
		t.Fatalf("authorization changed or released source held object: posts=%d->%d head=%+v", postsAtClose, postsAfterAuthorization, finalHead)
	}

	// A coherent authorization using the obsolete raw spec digest is invalid,
	// and exact replay must not migrate or rewrite it.
	claimBeforeLegacyReplay, claimVersion, err := f.e.AuthorityState.ReadArchiveScopeClaim(ctx, targetScope)
	if err != nil {
		t.Fatal(err)
	}
	rootBeforeLegacyReplay, rootVersion, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, targetID)
	if err != nil {
		t.Fatal(err)
	}
	claimBeforeLegacyReplay.EmptyRecoveryAuthorization.PolicyDigest = request.PolicyDigest
	rootBeforeLegacyReplay.Ledger.EmptyRecoveryAuthorization.PolicyDigest = request.PolicyDigest
	claimKey, claimIdentity, err := f.e.AuthorityState.archiveScopeClaimKey(targetScope)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.e.AuthorityState.write(ctx, claimKey, claimIdentity, claimBeforeLegacyReplay, claimVersion); err != nil {
		t.Fatalf("seed coherent legacy raw-digest archive claim: %v", err)
	}
	if err := f.e.AuthorityState.WriteRoot(ctx, f.runtime.StatefulSetUID, targetID, rootBeforeLegacyReplay, rootVersion); err != nil {
		t.Fatalf("seed coherent legacy raw-digest target root: %v", err)
	}
	legacyClaim, _, err := f.e.AuthorityState.ReadArchiveScopeClaim(ctx, targetScope)
	if err != nil {
		t.Fatal(err)
	}
	legacyRoot, _, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, targetID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.e.RegisterArchiveEmptyRecovery(ctx, request); err == nil || !strings.Contains(err.Error(), "persisted empty recovery authorization differs from request") {
		t.Fatalf("replay silently upgraded a coherent legacy raw-digest authorization: %v", err)
	}
	claimAfterLegacyReplay, _, claimErr := f.e.AuthorityState.ReadArchiveScopeClaim(ctx, targetScope)
	rootAfterLegacyReplay, _, rootErr := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, targetID)
	if claimErr != nil || rootErr != nil || hashJSON(legacyClaim) != hashJSON(claimAfterLegacyReplay) || hashJSON(legacyRoot) != hashJSON(rootAfterLegacyReplay) {
		t.Fatalf("rejected legacy authorization replay rewrote canonical state: claimErr=%v rootErr=%v", claimErr, rootErr)
	}
}

func TestFrozenEmptyGenesisPreflightRejectsMissingReferenceAndExtraHistory(t *testing.T) {
	t.Run("missing reference", func(t *testing.T) {
		f := newFrozenEmptyHeldFixture(t)
		data := objstore.NewInMemBucket()
		t.Cleanup(func() { _ = data.Close() })
		f.e.ArchiveBucket = frozenSourceEmptyTestBucket{Bucket: data}
		if _, err := f.e.change(t.Context(), f.runtime.StatefulSetUID, f.runtime.SourceClusterID, func(ledger *fencing.Ledger) error {
			ledger.ArchiveGenesisState = fencing.ArchiveGenesisRegistrationPending
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := f.e.claimArchiveScopeForRegistration(t.Context(), f.runtime.StatefulSetUID, f.id, f.runtime); err != nil {
			t.Fatal(err)
		}
		request := persistValidEmptyRecoveryForPreflight(t, f)
		_, err := f.e.frozenSourcePreflight(t.Context(), request)
		if err == nil || !strings.Contains(err.Error(), "does not bind an admitted genesis reference") {
			t.Fatalf("preflight did not reject specifically for the missing canonical genesis reference: %v", err)
		}
	})

	t.Run("additional admitted history", func(t *testing.T) {
		f := newFrozenEmptyHeldFixture(t)
		data := objstore.NewInMemBucket()
		t.Cleanup(func() { _ = data.Close() })
		f.e.ArchiveBucket = frozenSourceEmptyTestBucket{Bucket: data}
		if _, err := f.e.change(t.Context(), f.runtime.StatefulSetUID, f.runtime.SourceClusterID, func(ledger *fencing.Ledger) error {
			ledger.ArchiveGenesisState = fencing.ArchiveGenesisRegistrationPending
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := f.e.claimArchiveScopeForRegistration(t.Context(), f.runtime.StatefulSetUID, f.id, f.runtime); err != nil {
			t.Fatal(err)
		}
		newFrozenEmptyNoEffectServer(t, f)
		payload, err := recovery.EmptyArchiveHeadBytes()
		if err != nil {
			t.Fatal(err)
		}
		genesis := f.admitUpload(t, 1, "archive/head.bin", fencing.MutationCondition{Mode: fencing.MutationCreateOnly}, payload)
		if err := f.e.executeSourceMutationRequest(t.Context(), "Bearer test", f.id, sourceMutationRequest{
			ProducerUID: f.producer, Sequence: 1, Intent: genesis,
		}, true); !errors.Is(err, ErrFencePending) {
			t.Fatalf("persist exact unknown genesis reference: %v", err)
		}
		_ = f.admitUpload(t, 2, "archive/blocks/extra.bin", fencing.MutationCondition{Mode: fencing.MutationCreateOnly}, []byte("other admitted history"))
		request := persistValidEmptyRecoveryForPreflight(t, f)
		_, err = f.e.frozenSourcePreflight(t.Context(), request)
		if err == nil || !strings.Contains(err.Error(), "source contains non-winner mutation history") {
			t.Fatalf("preflight did not reject specifically for additional admitted source history: %v", err)
		}
	})
}

func TestVerifyFrozenEmptyArchiveNamespaceRejectsNestedArchiveObjects(t *testing.T) {
	ctx := t.Context()
	bucket := objstore.NewInMemBucket()
	t.Cleanup(func() { _ = bucket.Close() })
	prefix := "root/source"
	head, err := recovery.EmptyArchiveHeadBytes()
	if err != nil {
		t.Fatal(err)
	}
	if err := bucket.Upload(ctx, path.Join(prefix, "archive/head.bin"), bytes.NewReader(head)); err != nil {
		t.Fatal(err)
	}
	if err := verifyFrozenEmptyArchiveNamespace(ctx, bucket, prefix); err != nil {
		t.Fatalf("canonical empty HEAD should be the only allowed archive object: %v", err)
	}

	// This is an actual nested object, not merely an admitted journal entry.
	// Recursive iteration must expose it even when the object store represents
	// archive/blocks as a virtual directory prefix.
	if err := bucket.Upload(ctx, path.Join(prefix, "archive/blocks/extra.bin"), bytes.NewReader([]byte("unexpected extent"))); err != nil {
		t.Fatal(err)
	}
	if err := verifyFrozenEmptyArchiveNamespace(ctx, bucket, prefix); err == nil || !strings.Contains(err.Error(), "additional archive history") {
		t.Fatalf("recursive namespace validation did not reject the nested archive object: %v", err)
	}
}
