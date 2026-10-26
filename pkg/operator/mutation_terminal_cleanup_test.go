package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
	"google.golang.org/api/option"
)

var errInjectedTerminalOwnerClear = errors.New("injected terminal mutation-key owner clear failure")

type terminalOwnerClearFaultBucket struct {
	objstore.Bucket
	key        string
	remaining  int
	failedName string
	failed     mutationKeyState
	failedBody string
}

// deleteAfterTerminalOwnerClearBucket models a concurrent removal between
// verified predecessor cleanup and the next producer's first lock create.
type deleteAfterTerminalOwnerClearBucket struct {
	objstore.Bucket
	key     string
	deleted bool
}

func (b *deleteAfterTerminalOwnerClearBucket) Upload(ctx context.Context, name string, input io.Reader, options ...objstore.ObjectUploadOption) error {
	body, err := io.ReadAll(input)
	if err != nil {
		return err
	}
	if name != b.key {
		return b.Bucket.Upload(ctx, name, bytes.NewReader(body), options...)
	}
	var envelope struct {
		Value mutationKeyState `json:"value"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return err
	}
	if err := b.Bucket.Upload(ctx, name, bytes.NewReader(body), options...); err != nil {
		return err
	}
	if name == b.key && envelope.Value.Owner == "" && !b.deleted {
		b.deleted = true
		return b.Bucket.Delete(ctx, name)
	}
	return nil
}

func (b *deleteAfterTerminalOwnerClearBucket) SupportedObjectUploadOptions() []objstore.ObjectUploadOptionType {
	if supported, ok := b.Bucket.(interface {
		SupportedObjectUploadOptions() []objstore.ObjectUploadOptionType
	}); ok {
		return supported.SupportedObjectUploadOptions()
	}
	return nil
}

func (b *deleteAfterTerminalOwnerClearBucket) SupportsVersionedDelete() bool { return true }

func (b *deleteAfterTerminalOwnerClearBucket) DeleteVersion(ctx context.Context, name string, version objstore.ObjectVersion) error {
	versioned, ok := b.Bucket.(interface {
		DeleteVersion(context.Context, string, objstore.ObjectVersion) error
	})
	if ok {
		return versioned.DeleteVersion(ctx, name, version)
	}
	attrs, err := b.Bucket.Attributes(ctx, name)
	if err != nil {
		return err
	}
	if attrs.Version == nil || attrs.Version.Type != version.Type || attrs.Version.Value != version.Value {
		return errors.New("test bucket version differs")
	}
	return b.Bucket.Delete(ctx, name)
}

func (b *terminalOwnerClearFaultBucket) Upload(ctx context.Context, name string, input io.Reader, options ...objstore.ObjectUploadOption) error {
	body, err := io.ReadAll(input)
	if err != nil {
		return err
	}
	if name == b.key && b.remaining > 0 {
		var envelope struct {
			Value mutationKeyState `json:"value"`
		}
		if json.Unmarshal(body, &envelope) == nil && envelope.Value.Owner == "" {
			b.remaining--
			b.failedName = name
			b.failed = envelope.Value
			b.failedBody = string(body)
			return errInjectedTerminalOwnerClear
		}
	}
	return b.Bucket.Upload(ctx, name, bytes.NewReader(body), options...)
}

func (b *terminalOwnerClearFaultBucket) SupportsVersionedDelete() bool { return true }

func (b *terminalOwnerClearFaultBucket) DeleteVersion(ctx context.Context, name string, version objstore.ObjectVersion) error {
	versioned, ok := b.Bucket.(interface {
		DeleteVersion(context.Context, string, objstore.ObjectVersion) error
	})
	if ok {
		return versioned.DeleteVersion(ctx, name, version)
	}
	attrs, err := b.Bucket.Attributes(ctx, name)
	if err != nil {
		return err
	}
	if attrs.Version == nil || attrs.Version.Type != version.Type || attrs.Version.Value != version.Value {
		return errors.New("test bucket version differs")
	}
	return b.Bucket.Delete(ctx, name)
}

type terminalCleanupGCSObject struct {
	generation     int64
	metageneration int64
	payload        []byte
	metadata       map[string]string
}

func terminalCleanupGCSServer(t *testing.T, prefix, key string, initialGeneration int64) (*httptest.Server, *sync.Mutex, map[string]terminalCleanupGCSObject, *int64, *int, *[]string) {
	t.Helper()
	mu := &sync.Mutex{}
	objects := map[string]terminalCleanupGCSObject{prefix + "/" + key: {generation: initialGeneration, metageneration: 1, payload: []byte("old"), metadata: map[string]string{}}}
	nextGeneration := initialGeneration
	uploads := 0
	requests := []string{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Query().Get("name")
		if name == "" {
			path := r.URL.EscapedPath()
			const objectPrefix = "/storage/v1/b/archive-bucket/o/"
			if strings.HasPrefix(path, objectPrefix) {
				name, _ = urlPathUnescape(strings.TrimPrefix(path, objectPrefix))
			}
		}
		mu.Lock()
		defer mu.Unlock()
		requests = append(requests, r.Method+" "+r.URL.String()+" name="+name+" ifGenerationMatch="+r.URL.Query().Get("ifGenerationMatch"))
		switch r.Method {
		case http.MethodGet:
			object, ok := objects[name]
			if !ok {
				http.NotFound(w, r)
				return
			}
			writeTerminalCleanupGCSObject(w, name, object)
		case http.MethodPost:
			mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
			if err != nil || mediaType != "multipart/related" || params["boundary"] == "" {
				http.Error(w, "invalid multipart upload", http.StatusBadRequest)
				return
			}
			reader := multipart.NewReader(r.Body, params["boundary"])
			metadataPart, err := reader.NextPart()
			if err != nil {
				http.Error(w, "missing metadata", http.StatusBadRequest)
				return
			}
			var metadata struct {
				Name     string            `json:"name"`
				Metadata map[string]string `json:"metadata"`
			}
			if err := json.NewDecoder(metadataPart).Decode(&metadata); err != nil {
				http.Error(w, "bad metadata", http.StatusBadRequest)
				return
			}
			_ = metadataPart.Close()
			payloadPart, err := reader.NextPart()
			if err != nil {
				http.Error(w, "missing payload", http.StatusBadRequest)
				return
			}
			payload, err := io.ReadAll(payloadPart)
			_ = payloadPart.Close()
			if err != nil || metadata.Name != name {
				http.Error(w, "payload/name mismatch", http.StatusBadRequest)
				return
			}
			previous, exists := objects[name]
			condition, _ := strconv.ParseInt(r.URL.Query().Get("ifGenerationMatch"), 10, 64)
			if exists && (condition == 0 || condition != previous.generation) || !exists && condition != 0 {
				http.Error(w, "generation precondition failed", http.StatusPreconditionFailed)
				return
			}
			nextGeneration++
			metageneration := int64(1)
			if exists {
				metageneration = previous.metageneration + 1
			}
			object := terminalCleanupGCSObject{generation: nextGeneration, metageneration: metageneration,
				payload: payload, metadata: metadata.Metadata}
			objects[name] = object
			uploads++
			writeTerminalCleanupGCSObject(w, name, object)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	return server, mu, objects, &nextGeneration, &uploads, &requests
}

func urlPathUnescape(value string) (string, error) { return url.PathUnescape(value) }

func writeTerminalCleanupGCSObject(w http.ResponseWriter, name string, object terminalCleanupGCSObject) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"kind": "storage#object", "bucket": "archive-bucket", "name": name,
		"generation": strconv.FormatInt(object.generation, 10), "metageneration": strconv.FormatInt(object.metageneration, 10),
		"size": strconv.Itoa(len(object.payload)), "metadata": object.metadata})
}

func TestLogicalClientResumesTerminalKeyCleanupAfterResolve(t *testing.T) {
	ctx := context.Background()
	fixture := newLogicalManifestHTTPFixture(t, nil)
	identity := fixture.identity
	authority := &fencing.AuthorityClient{URL: fixture.server.URL + "/v1/supervisor", TokenFile: fixture.podTokenFile,
		Client: fixture.server.Client(), AuthorityProfile: fencing.LogicalWriterProfileV1}
	if _, err := authority.RegisterStartup(ctx, identity); err != nil {
		t.Fatal(err)
	}
	identity.WALIdentity = hashJSON("terminal-cleanup-wal")
	receipt, err := authority.Bind(ctx, identity)
	if err != nil || receipt.Producer == nil || receipt.LogicalBinding == nil {
		t.Fatalf("bind logical mutation client: receipt=%+v err=%v", receipt, err)
	}
	scope, err := fixture.e.sourceMutationStorageScopeForRoot(ctx, logicalManifestSTSUID, identity.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	targetKey := "membership/current"
	storageServer, gcsMu, gcsObjects, _, gcsUploads, gcsRequests := terminalCleanupGCSServer(t, scope.Prefix, targetKey, 14)
	storageClient, err := storage.NewClient(ctx, option.WithEndpoint(storageServer.URL+"/storage/v1/"), option.WithHTTPClient(storageServer.Client()), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = storageClient.Close() })
	fixture.e.SourceMutationStorage = storageClient
	retryClient, err := fencing.CreateAuthoritySourceMutationClient(t.TempDir()+"/source-retry", authority, receipt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = retryClient.Close() })
	emptyHead, err := recovery.EmptyArchiveHeadBytes()
	if err != nil {
		t.Fatal(err)
	}
	if err := retryClient.Upload(ctx, "archive/head.bin", fencing.MutationCondition{Mode: fencing.MutationCreateOnly}, emptyHead); err != nil {
		t.Fatalf("commit archive genesis before ordinary mutation: %v", err)
	}
	keyStateKey, _ := fixture.e.mutationKeyIdentity(receipt.Producer.StoreScopeHash, targetKey)
	faultBucket := &terminalOwnerClearFaultBucket{Bucket: fixture.bucket, key: keyStateKey, remaining: 1}
	fixture.state.bucket = faultBucket
	payload := []byte("new membership")
	firstErr := retryClient.Upload(ctx, targetKey, fencing.MutationCondition{Mode: fencing.MutationMatchGeneration, Generation: 14}, payload)
	if firstErr == nil || faultBucket.remaining != 0 {
		t.Fatalf("expected the exact post-Resolve owner clear to fail once: err=%v remaining=%d", firstErr, faultBucket.remaining)
	}
	producer, _, _, err := fixture.state.ReadLogicalProducer(ctx, logicalManifestSTSUID, *receipt.LogicalBinding)
	if err != nil || producer.LastAdmitted != 2 || producer.Entries[2].Resolution == nil || producer.Entries[2].Resolution.Outcome != fencing.MutationCommitted {
		t.Fatalf("fixture did not preserve the terminal mutation after failed cleanup: firstErr=%v producer=%+v readErr=%v failedName=%q targetName=%q failedKey=%+v failedBody=%s gcsUploads=%d gcsRequests=%v", firstErr, producer, err, faultBucket.failedName, keyStateKey, faultBucket.failed, faultBucket.failedBody, *gcsUploads, *gcsRequests)
	}
	oldOwner := logicalMutationAuthority(logicalManifestSTSUID, *receipt.LogicalBinding).mutationOwner(producerRunForTerminalCleanup(t, fixture, identity), receipt.Producer.ProducerUID, 2, producer.Entries[2].Intent)
	logicalAuth := logicalMutationAuthority(logicalManifestSTSUID, *receipt.LogicalBinding)
	lock, lockErr := fixture.e.readMutationKeyState(ctx, receipt.Producer.StoreScopeHash, targetKey)
	if lockErr != nil || lock.Owner != oldOwner {
		t.Fatalf("injected failure did not retain exact predecessor owner: lock=%+v wantOwner=%q err=%v", lock, oldOwner, lockErr)
	}
	clientRetryErr := retryClient.Upload(ctx, targetKey, fencing.MutationCondition{Mode: fencing.MutationMatchGeneration, Generation: 14}, payload)
	if clientRetryErr != nil && !errors.Is(clientRetryErr, fencing.ErrMutationConditionFailed) {
		t.Fatalf("original client could not settle its exact terminal tuple: %v", clientRetryErr)
	}
	lock, lockErr = fixture.e.readMutationKeyState(ctx, receipt.Producer.StoreScopeHash, targetKey)
	if lockErr != nil || lock.Owner != "" || *gcsUploads != 2 {
		t.Fatalf("original-client terminal lookup did not clear the exact owner without provider replay: lock=%+v err=%v uploads=%d", lock, lockErr, *gcsUploads)
	}
	if err := fixture.e.acquireMutationKey(ctx, receipt.Producer.StoreScopeHash, targetKey, oldOwner, false); err != nil {
		t.Fatalf("recreate only the exact predecessor lock for independent-owner regression: %v", err)
	}
	keyStateKey, _ = fixture.e.mutationKeyIdentity(receipt.Producer.StoreScopeHash, targetKey)
	deleteRaceBucket := &deleteAfterTerminalOwnerClearBucket{Bucket: fixture.state.bucket, key: keyStateKey}
	fixture.state.bucket = deleteRaceBucket
	currentOwner := hashJSON([]any{"successor-owner", oldOwner})
	replacementID, replacementBinding := registerTerminalCleanupReplacement(t, fixture, identity, *receipt.LogicalBinding)
	nextPayload := []byte("next membership")
	stages, err := fixture.e.sourceStagesForRoot(ctx, logicalManifestSTSUID, replacementID.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	draft := completeStoreIntent(fencing.MutationIntent{StoreScopeHash: replacementBinding.StoreScopeHash, Kind: "upload", Key: targetKey})
	draft.OperationID, draft.PayloadRef, draft.PayloadDigest, draft.PayloadSize = "", "", "", 0
	draft.Condition = fencing.MutationCondition{Mode: fencing.MutationMatchGeneration, Generation: 16}
	draft.OwnerStep = fencing.MutationOwnerStep{Owner: "replacement-producer", Workflow: "membership", Step: "write", Attempt: 1}
	ticket := hashJSON([]any{"terminal-cleanup-successor-stage", draft, hashBytes(nextPayload)})
	if err := stages.ReserveContext(ctx, replacementBinding.ProducerUID, ticket, draft); err != nil {
		t.Fatal(err)
	}
	sealed, err := stages.SealContext(ctx, replacementBinding.ProducerUID, ticket, hashBytes(nextPayload), int64(len(nextPayload)), bytes.NewReader(nextPayload))
	if err != nil {
		t.Fatal(err)
	}
	nextIntent := draft
	nextIntent.OperationID = fencing.MutationRequestID(replacementBinding.ProducerUID, 1)
	nextIntent.PayloadRef, nextIntent.PayloadDigest, nextIntent.PayloadSize = sealed.Ref, sealed.Digest, sealed.Size
	replacementAuthority := logicalMutationAuthority(logicalManifestSTSUID, replacementBinding)
	if _, err := fixture.e.changeMutationProducerForAuthority(ctx, replacementAuthority, replacementBinding.ProducerUID, func(p *fencing.MutationProducer) error {
		return p.Admit(1, nextIntent)
	}); err != nil {
		t.Fatalf("admit independent same-key successor: %v", err)
	}
	admitted, admittedUID, _, err := fixture.state.ReadLogicalProducer(ctx, logicalManifestSTSUID, replacementBinding)
	if err != nil || admittedUID != replacementBinding.ProducerUID || admitted.LastAdmitted != 1 || admitted.Entries[1].Intent != nextIntent {
		t.Fatalf("independent successor admission did not persist its exact tuple: producer=%+v uid=%q err=%v", admitted, admittedUID, err)
	}
	run := producerRunForTerminalCleanup(t, fixture, replacementID)
	provider, err := newGCSMutationProviderForProfile(storageClient, scope, run.MutationProfile)
	if err != nil {
		t.Fatal(err)
	}
	receipts, err := fixture.e.receiptsForLogicalBinding(logicalManifestSTSUID, replacementBinding)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.e.ExecuteLogicalSourceMutation(ctx, logicalManifestSTSUID, replacementBinding, replacementBinding.ProducerUID, 1, nextIntent, scope, provider, receipts); err != nil {
		gcsMu.Lock()
		currentObject := gcsObjects[scope.Prefix+"/"+targetKey]
		gcsMu.Unlock()
		t.Fatalf("independent successor generation-bound mutation remained blocked by terminal owner: %v intent=%+v object=%+v uploads=%d requests=%v", err, nextIntent, currentObject, *gcsUploads, *gcsRequests)
	}
	if !deleteRaceBucket.deleted {
		t.Fatal("foreign terminal owner was not cleared before successor lock acquisition")
	}
	if *gcsUploads != 3 { // genesis, original mutation, successor mutation
		producer, _, _, _ := fixture.state.ReadLogicalProducer(ctx, logicalManifestSTSUID, replacementBinding)
		gcsMu.Lock()
		currentObject := gcsObjects[scope.Prefix+"/"+targetKey]
		gcsMu.Unlock()
		t.Fatalf("terminal recovery redispatched or successor did not make exactly one provider call: uploads=%d entry=%+v object=%+v requests=%v", *gcsUploads, producer.Entries[1], currentObject, *gcsRequests)
	}
	gcsMu.Lock()
	final := gcsObjects[scope.Prefix+"/"+targetKey]
	gcsMu.Unlock()
	if final.generation != 17 || string(final.payload) != "next membership" {
		t.Fatalf("same-key successor did not publish its exact generation: object=%+v", final)
	}
	if err := fixture.e.acquireMutationKey(ctx, receipt.Producer.StoreScopeHash, targetKey, oldOwner, false); err != nil {
		t.Fatalf("recreate only the exact predecessor lock for archived-recovery regression: %v", err)
	}
	_, err = fixture.e.changeMutationProducerForAuthority(ctx, logicalAuth, receipt.Producer.ProducerUID, func(p *fencing.MutationProducer) error {
		for sequence := p.LastAdmitted + 1; sequence <= 35; sequence++ {
			intent := completeStoreIntent(fencing.MutationIntent{StoreScopeHash: receipt.Producer.StoreScopeHash, OperationID: fencing.MutationRequestID(receipt.Producer.ProducerUID, sequence),
				Kind: "delete", Key: "terminal-cleanup-padding-" + strconv.FormatUint(sequence, 10)})
			if err := p.Admit(sequence, intent); err != nil {
				return err
			}
			if err := p.CancelUnprepared(sequence, strings.Repeat("c", 64)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("append closed no-work prefix to archive the original receipt: %v", err)
	}
	if err := fixture.e.compactLogicalSourceMutationJournal(ctx, logicalManifestSTSUID, *receipt.LogicalBinding, 36); err != nil {
		t.Fatalf("compact exact terminal predecessor into authenticated archive: %v", err)
	}
	producer, _, _, err = fixture.state.ReadLogicalProducer(ctx, logicalManifestSTSUID, *receipt.LogicalBinding)
	if err != nil || producer.CompactedThrough < 2 {
		t.Fatalf("terminal predecessor was not archived before recovery: producer=%+v err=%v", producer, err)
	}
	if err := fixture.e.resumeForeignLogicalTerminalKeyOwner(ctx, replacementAuthority, receipt.Producer.StoreScopeHash, targetKey, currentOwner); err != nil {
		t.Fatalf("independent successor could not clear exact archived terminal predecessor: %v", err)
	}
	lock, lockErr = fixture.e.readMutationKeyState(ctx, receipt.Producer.StoreScopeHash, targetKey)
	if lockErr != nil || lock.Owner != "" {
		t.Fatalf("archived predecessor cleanup did not clear its lock: lock=%+v err=%v", lock, lockErr)
	}
	if err := fixture.e.acquireMutationKey(ctx, receipt.Producer.StoreScopeHash, targetKey, currentOwner, false); err != nil {
		t.Fatalf("install successor owner for preservation regression: %v", err)
	}
	mismatchKey := "membership/mismatched-owner"
	if err := fixture.e.acquireMutationKey(ctx, receipt.Producer.StoreScopeHash, mismatchKey, oldOwner, true); err != nil {
		t.Fatalf("install tuple-mismatch control owner: %v", err)
	}
	if err := fixture.e.resumeForeignLogicalTerminalKeyOwner(ctx, replacementAuthority, receipt.Producer.StoreScopeHash, mismatchKey, currentOwner); !errors.Is(err, ErrFencePending) {
		t.Fatalf("foreign cleanup accepted a receipt whose tuple key differs from the owner lock: %v", err)
	}
	mismatchLock, mismatchErr := fixture.e.readMutationKeyState(ctx, receipt.Producer.StoreScopeHash, mismatchKey)
	if mismatchErr != nil || mismatchLock.Owner != oldOwner {
		t.Fatalf("mismatched tuple cleanup changed its foreign owner: lock=%+v err=%v", mismatchLock, mismatchErr)
	}
	if err := fixture.e.resumeForeignLogicalTerminalKeyOwner(ctx, replacementAuthority, receipt.Producer.StoreScopeHash, targetKey, hashJSON([]any{"later-owner", currentOwner})); err != nil && !errors.Is(err, ErrFencePending) {
		t.Fatalf("stale archived cleanup returned an unexpected error behind a successor owner: %v", err)
	}
	lock, lockErr = fixture.e.readMutationKeyState(ctx, receipt.Producer.StoreScopeHash, targetKey)
	if lockErr != nil || lock.Owner != currentOwner {
		t.Fatalf("terminal cleanup overwrote a successor owner: lock=%+v err=%v", lock, lockErr)
	}
	uncertainKey := "membership/uncertain"
	uncertainPayload := []byte("unknown-dispatch")
	uncertainIntent := completeStoreIntent(fencing.MutationIntent{StoreScopeHash: replacementBinding.StoreScopeHash, Kind: "upload", Key: uncertainKey})
	uncertainIntent.OperationID = fencing.MutationRequestID(replacementBinding.ProducerUID, 2)
	uncertainIntent.OwnerStep = fencing.MutationOwnerStep{Owner: "replacement-producer", Workflow: "membership", Step: "uncertain", Attempt: 1}
	uncertainIntent.Condition = fencing.MutationCondition{Mode: fencing.MutationMatchGeneration, Generation: 7}
	uncertainIntent.PayloadRef = "payload-" + hashBytes(uncertainPayload)
	uncertainIntent.PayloadDigest = hashBytes(uncertainPayload)
	uncertainIntent.PayloadSize = int64(len(uncertainPayload))
	uncertainOwner := replacementAuthority.mutationOwner(run, replacementBinding.ProducerUID, 2, uncertainIntent)
	if _, err := fixture.e.changeMutationProducerForAuthority(ctx, replacementAuthority, replacementBinding.ProducerUID, func(p *fencing.MutationProducer) error {
		if err := p.Admit(2, uncertainIntent); err != nil {
			return err
		}
		if err := p.Prepare(2, fencing.MutationPreparation{Profile: fencing.MutationExecutionProfile, StoreScopeHash: replacementBinding.StoreScopeHash, GenerationSet: true, Generation: 7}); err != nil {
			return err
		}
		return p.GrantDispatch(2, "dispatch-uncertain")
	}); err != nil {
		t.Fatalf("prepare unknown-dispatch negative control: %v", err)
	}
	if err := fixture.e.acquireMutationKey(ctx, replacementBinding.StoreScopeHash, uncertainKey, uncertainOwner, true); err != nil {
		t.Fatal(err)
	}
	if err := fixture.e.resumeForeignLogicalTerminalKeyOwner(ctx, replacementAuthority, replacementBinding.StoreScopeHash, uncertainKey, hashJSON([]any{"uncertain-successor", uncertainOwner})); !errors.Is(err, ErrFencePending) {
		t.Fatalf("unknown dispatched predecessor was reclaimed without terminal evidence: %v", err)
	}
	uncertainLock, uncertainErr := fixture.e.readMutationKeyState(ctx, replacementBinding.StoreScopeHash, uncertainKey)
	if uncertainErr != nil || uncertainLock.Owner != uncertainOwner || *gcsUploads != 3 {
		t.Fatalf("unknown-dispatch control changed its lock or performed provider work: lock=%+v err=%v uploads=%d", uncertainLock, uncertainErr, *gcsUploads)
	}
}

func TestForeignLogicalTerminalKeyCleanupRequiresExactReceiptAndCanonicalEntry(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(*testing.T, *logicalManifestHTTPFixture, fencing.Identity, fencing.LogicalWriterBindingV1, *MutationExecutionReceiptStore) (string, string)
	}{
		{
			name: "cross-root receipt",
			setup: func(t *testing.T, fixture *logicalManifestHTTPFixture, identity fencing.Identity, binding fencing.LogicalWriterBindingV1, _ *MutationExecutionReceiptStore) (string, string) {
				key := "foreign-cross-root-receipt"
				intent := completeStoreIntent(fencing.MutationIntent{StoreScopeHash: binding.StoreScopeHash, Kind: "delete", Key: key})
				intent.OperationID = fencing.MutationRequestID(binding.ProducerUID, 1)
				authority := logicalMutationAuthority(logicalManifestSTSUID, binding)
				run := producerRunForTerminalCleanup(t, fixture, identity)
				receipt := authority.makeReceipt(run, binding.ProducerUID, 1, intent,
					fencing.MutationPreparation{Profile: fencing.MutationExecutionProfile, StoreScopeHash: binding.StoreScopeHash,
						GenerationSet: true, Generation: 4}, "cross-root-dispatch",
					mutationProviderResult{Outcome: fencing.MutationCommitted, Generation: 4, AllCopiesTerminal: true})
				otherEvidence, err := NewAuthorityEvidenceStore(fixture.bucket, fixture.state.prefix, "different-statefulset-root", identity.SourceClusterID)
				if err != nil {
					t.Fatal(err)
				}
				otherReceipts, err := NewRemoteMutationExecutionReceiptStore(otherEvidence)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := otherReceipts.put(receipt); err != nil {
					t.Fatal(err)
				}
				owner := authority.mutationOwner(run, binding.ProducerUID, 1, intent)
				if err := fixture.e.acquireMutationKey(t.Context(), intent.StoreScopeHash, key, owner, true); err != nil {
					t.Fatal(err)
				}
				return key, owner
			},
		},
		{
			name: "missing receipt",
			setup: func(t *testing.T, fixture *logicalManifestHTTPFixture, identity fencing.Identity, binding fencing.LogicalWriterBindingV1, _ *MutationExecutionReceiptStore) (string, string) {
				run := producerRunForTerminalCleanup(t, fixture, identity)
				intent := completeStoreIntent(fencing.MutationIntent{StoreScopeHash: binding.StoreScopeHash, Kind: "upload", Key: "foreign-missing-receipt"})
				intent.OperationID = fencing.MutationRequestID(binding.ProducerUID, 1)
				owner := logicalMutationAuthority(logicalManifestSTSUID, binding).mutationOwner(run, binding.ProducerUID, 1, intent)
				if err := fixture.e.acquireMutationKey(t.Context(), intent.StoreScopeHash, intent.Key, owner, true); err != nil {
					t.Fatal(err)
				}
				return intent.Key, owner
			},
		},
		{
			name: "valid receipt before resolution",
			setup: func(t *testing.T, fixture *logicalManifestHTTPFixture, identity fencing.Identity, binding fencing.LogicalWriterBindingV1, receipts *MutationExecutionReceiptStore) (string, string) {
				key := "foreign-receipt-before-resolution"
				intent := completeStoreIntent(fencing.MutationIntent{StoreScopeHash: binding.StoreScopeHash, Kind: "delete", Key: key})
				intent.OperationID = fencing.MutationRequestID(binding.ProducerUID, 1)
				intent.OwnerStep = fencing.MutationOwnerStep{Owner: "owner", Workflow: "cleanup", Step: "receipt-before-resolution", Attempt: 1}
				intent.Condition = fencing.MutationCondition{Mode: fencing.MutationMatchGeneration, Generation: 4}
				authority := logicalMutationAuthority(logicalManifestSTSUID, binding)
				preparation := fencing.MutationPreparation{Profile: fencing.MutationExecutionProfile, StoreScopeHash: binding.StoreScopeHash, GenerationSet: true, Generation: 4}
				if _, err := fixture.e.changeMutationProducerForAuthority(t.Context(), authority, binding.ProducerUID, func(p *fencing.MutationProducer) error {
					if err := p.Admit(1, intent); err != nil {
						return err
					}
					if err := p.Prepare(1, preparation); err != nil {
						return err
					}
					return p.GrantDispatch(1, "dispatch-before-resolution")
				}); err != nil {
					t.Fatal(err)
				}
				run := producerRunForTerminalCleanup(t, fixture, identity)
				receipt := authority.makeReceipt(run, binding.ProducerUID, 1, intent, preparation, "dispatch-before-resolution",
					mutationProviderResult{Outcome: fencing.MutationCommitted, Generation: 4, AllCopiesTerminal: true})
				if _, err := receipts.put(receipt); err != nil {
					t.Fatal(err)
				}
				owner := authority.mutationOwner(run, binding.ProducerUID, 1, intent)
				if err := fixture.e.acquireMutationKey(t.Context(), intent.StoreScopeHash, key, owner, true); err != nil {
					t.Fatal(err)
				}
				return key, owner
			},
		},
		{
			name: "nonlogical receipt",
			setup: func(t *testing.T, fixture *logicalManifestHTTPFixture, _ fencing.Identity, binding fencing.LogicalWriterBindingV1, receipts *MutationExecutionReceiptStore) (string, string) {
				key := "foreign-nonlogical-receipt"
				intent := completeStoreIntent(fencing.MutationIntent{StoreScopeHash: binding.StoreScopeHash, Kind: "upload", Key: key})
				intent.OperationID = fencing.MutationRequestID("physical-producer", 1)
				runtime := fencing.RuntimeBinding{StatefulSetUID: logicalManifestSTSUID, SourceClusterID: logicalManifestSourceID,
					NodeID: "physical-node", PodUID: "physical-pod", RunID: "physical-run"}
				receipt := mutationExecutionReceipt{Runtime: runtime, WALIdentity: "physical-wal", ProducerUID: "physical-producer", Sequence: 1,
					Intent: intent, Preparation: fencing.MutationPreparation{Profile: fencing.MutationExecutionProfile, StoreScopeHash: binding.StoreScopeHash, GenerationSet: true, Generation: 4},
					DispatchID: "physical-dispatch", Outcome: fencing.MutationCommitted, Generation: 5, AllCopiesTerminal: true}
				receipt.ReceiptDigest = receipt.digest()
				name := mutationExecutionReceiptName(receipt)
				if _, err := receipts.put(receipt); err != nil {
					t.Fatal(err)
				}
				owner := strings.TrimPrefix(name, "receipt-")
				if err := fixture.e.acquireMutationKey(t.Context(), intent.StoreScopeHash, key, owner, true); err != nil {
					t.Fatal(err)
				}
				return key, owner
			},
		},
		{
			name: "missing canonical tuple",
			setup: func(t *testing.T, fixture *logicalManifestHTTPFixture, identity fencing.Identity, binding fencing.LogicalWriterBindingV1, receipts *MutationExecutionReceiptStore) (string, string) {
				key := "foreign-canonical-entry-missing"
				intent := completeStoreIntent(fencing.MutationIntent{StoreScopeHash: binding.StoreScopeHash, Kind: "upload", Key: key})
				intent.OperationID = fencing.MutationRequestID(binding.ProducerUID, 1)
				run := producerRunForTerminalCleanup(t, fixture, identity)
				authority := logicalMutationAuthority(logicalManifestSTSUID, binding)
				receipt := authority.makeReceipt(run, binding.ProducerUID, 1, intent,
					fencing.MutationPreparation{Profile: fencing.MutationExecutionProfile, StoreScopeHash: binding.StoreScopeHash, GenerationSet: true, Generation: 4},
					"orphan-dispatch", mutationProviderResult{Outcome: fencing.MutationCommitted, Generation: 5, AllCopiesTerminal: true})
				if _, err := receipts.put(receipt); err != nil {
					t.Fatal(err)
				}
				owner := authority.mutationOwner(run, binding.ProducerUID, 1, intent)
				if err := fixture.e.acquireMutationKey(t.Context(), intent.StoreScopeHash, key, owner, true); err != nil {
					t.Fatal(err)
				}
				return key, owner
			},
		},
		{
			name: "canonical tuple intent mismatch",
			setup: func(t *testing.T, fixture *logicalManifestHTTPFixture, identity fencing.Identity, binding fencing.LogicalWriterBindingV1, receipts *MutationExecutionReceiptStore) (string, string) {
				canonical := completeStoreIntent(fencing.MutationIntent{StoreScopeHash: binding.StoreScopeHash, Kind: "delete", Key: "foreign-other-canonical-key"})
				canonical.OperationID = fencing.MutationRequestID(binding.ProducerUID, 1)
				canonical.OwnerStep = fencing.MutationOwnerStep{Owner: "owner", Workflow: "cleanup", Step: "canonical-intent", Attempt: 1}
				if _, err := fixture.e.changeMutationProducerForAuthority(t.Context(), logicalMutationAuthority(logicalManifestSTSUID, binding), binding.ProducerUID,
					func(p *fencing.MutationProducer) error { return p.Admit(1, canonical) }); err != nil {
					t.Fatal(err)
				}
				intent := canonical
				intent.Key = "foreign-mismatch-key"
				run := producerRunForTerminalCleanup(t, fixture, identity)
				authority := logicalMutationAuthority(logicalManifestSTSUID, binding)
				receipt := authority.makeReceipt(run, binding.ProducerUID, 1, intent,
					fencing.MutationPreparation{Profile: fencing.MutationExecutionProfile, StoreScopeHash: binding.StoreScopeHash, GenerationSet: true, Generation: 4},
					"mismatch-dispatch", mutationProviderResult{Outcome: fencing.MutationCommitted, Generation: 5, AllCopiesTerminal: true})
				if _, err := receipts.put(receipt); err != nil {
					t.Fatal(err)
				}
				owner := authority.mutationOwner(run, binding.ProducerUID, 1, intent)
				if err := fixture.e.acquireMutationKey(t.Context(), intent.StoreScopeHash, intent.Key, owner, true); err != nil {
					t.Fatal(err)
				}
				return intent.Key, owner
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			fixture := newLogicalManifestHTTPFixture(t, nil)
			authorityClient := &fencing.AuthorityClient{URL: fixture.server.URL + "/v1/supervisor", TokenFile: fixture.podTokenFile,
				Client: fixture.server.Client(), AuthorityProfile: fencing.LogicalWriterProfileV1}
			identity := fixture.identity
			if _, err := authorityClient.RegisterStartup(ctx, identity); err != nil {
				t.Fatalf("register owner startup: %v", err)
			}
			identity.WALIdentity = hashJSON([]string{"foreign-cleanup-wal", test.name})
			bound, err := authorityClient.Bind(ctx, identity)
			if err != nil || bound.LogicalBinding == nil {
				t.Fatalf("bind owner run: %+v %v", bound, err)
			}
			identity = bound.Identity
			binding := *bound.LogicalBinding
			_, replacement := registerTerminalCleanupReplacement(t, fixture, identity, binding)
			current := logicalMutationAuthority(logicalManifestSTSUID, replacement)
			receipts, err := fixture.e.receiptsForLogicalBinding(logicalManifestSTSUID, binding)
			if err != nil {
				t.Fatal(err)
			}
			key, owner := test.setup(t, fixture, identity, binding, receipts)
			before, err := fixture.e.readMutationKeyState(ctx, binding.StoreScopeHash, key)
			if err != nil || before.Owner != owner {
				t.Fatalf("test owner setup differs: state=%+v err=%v", before, err)
			}
			if err := fixture.e.resumeForeignLogicalTerminalKeyOwner(ctx, current, binding.StoreScopeHash, key, hashJSON([]string{"waiter", test.name})); !errors.Is(err, ErrFencePending) {
				t.Fatalf("foreign cleanup accepted %s evidence: %v", test.name, err)
			}
			after, err := fixture.e.readMutationKeyState(ctx, binding.StoreScopeHash, key)
			if err != nil || after != before {
				t.Fatalf("failed closed foreign cleanup changed owner: before=%+v after=%+v err=%v", before, after, err)
			}
		})
	}
}

func producerRunForTerminalCleanup(t *testing.T, fixture *logicalManifestHTTPFixture, identity fencing.Identity) fencing.Run {
	t.Helper()
	state, err := fixture.e.read(t.Context(), logicalManifestSTSUID, identity.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	run, ok := state.Ledger.Runs[identity.RunID]
	if !ok {
		t.Fatal("canonical Run missing")
	}
	return run
}

func registerTerminalCleanupReplacement(t *testing.T, fixture *logicalManifestHTTPFixture, old fencing.Identity,
	oldBinding fencing.LogicalWriterBindingV1) (fencing.Identity, fencing.LogicalWriterBindingV1) {
	t.Helper()
	ctx := t.Context()
	id := fencing.Identity{SourceClusterID: old.SourceClusterID, NodeID: "replacement-voter", RunID: "replacement-terminal-cleanup-run"}
	binding := oldBinding
	binding.NodeID, binding.RunID, binding.ProducerUID = id.NodeID, id.RunID, "replacement-terminal-cleanup-producer"
	binding.WALIdentity, binding.Recovery = "", fencing.LogicalRecoveryBindingV1{}
	root, version, err := fixture.state.ReadRoot(ctx, logicalManifestSTSUID, id.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if err := root.Ledger.RegisterLogicalWithRole(id, binding); err != nil {
		t.Fatalf("register independent replacement logical Run: %v", err)
	}
	if err := root.Ledger.BindLogicalMutationProducer(id, binding.ProducerUID); err != nil {
		t.Fatal(err)
	}
	if err := root.Ledger.BindLogicalMutationScope(id, binding.StoreScopeHash, fencing.MutationExecutionProfile); err != nil {
		t.Fatal(err)
	}
	if err := root.Ledger.BindLogicalMutationStage(id, hashJSON([]any{"replacement-stage", id.RunID})); err != nil {
		t.Fatal(err)
	}
	if err := fixture.state.WriteRoot(ctx, logicalManifestSTSUID, id.SourceClusterID, root, version); err != nil {
		t.Fatal(err)
	}
	if err := fixture.e.reserveLogicalMutationProducer(ctx, logicalManifestSTSUID, binding); err != nil {
		t.Fatal(err)
	}
	id.WALIdentity = hashJSON([]any{"replacement-wal", id.RunID})
	receipt, err := fixture.e.bindLogicalSupervisorWAL(ctx, logicalManifestSTSUID, id)
	if err != nil || receipt.LogicalBinding == nil {
		t.Fatalf("bind independent replacement WAL: receipt=%+v err=%v", receipt, err)
	}
	return id, *receipt.LogicalBinding
}
