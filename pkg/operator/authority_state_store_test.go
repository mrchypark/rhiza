package operator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/thanos-io/objstore"
)

var errOpaqueETagCondition = errors.New("opaque ETag condition failed")
var errInjectedAuthorityCASBeforeWrite = errors.New("injected authority CAS failure before write")

func newMissingLegacyLedgerKube(t *testing.T) *Kubernetes {
	t.Helper()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	t.Cleanup(server.Close)
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	return &Kubernetes{BaseURL: server.URL, Namespace: "test", TokenFile: token, Client: server.Client()}
}

// etagAuthorityBucket models content-derived opaque ETags, which can return to
// an earlier value after A-to-B-to-A. It delegates atomicity to InMemBucket.
type etagAuthorityBucket struct {
	objstore.Bucket
	loseNextCASResponse    bool
	failNextCASBeforeWrite bool
}

func (b *etagAuthorityBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	attrs, err := b.Bucket.Attributes(ctx, name)
	if err == nil && attrs.Version != nil {
		reader, readErr := b.Bucket.Get(ctx, name)
		if readErr != nil {
			return objstore.ObjectAttributes{}, readErr
		}
		body, readErr := io.ReadAll(reader)
		closeErr := reader.Close()
		if readErr != nil || closeErr != nil {
			return objstore.ObjectAttributes{}, errors.Join(readErr, closeErr)
		}
		digest := sha256.Sum256(body)
		attrs.Version = &objstore.ObjectVersion{Type: objstore.ETag, Value: "opaque-etag/" + hex.EncodeToString(digest[:])}
	}
	return attrs, err
}

func (b *etagAuthorityBucket) Upload(ctx context.Context, name string, reader io.Reader, options ...objstore.ObjectUploadOption) error {
	params := objstore.ApplyObjectUploadOptions(options...)
	var delegated []objstore.ObjectUploadOption
	if params.ContentType != "" {
		delegated = append(delegated, objstore.WithContentType(params.ContentType))
	}
	if params.IfNotExists {
		delegated = append(delegated, objstore.WithIfNotExists())
	} else if params.Condition != nil {
		if params.Condition.Type != objstore.ETag {
			return fmt.Errorf("test provider requires an ETag condition")
		}
		current, err := b.Attributes(ctx, name)
		if err != nil || current.Version == nil || params.Condition.Value != current.Version.Value {
			return errOpaqueETagCondition
		}
		baseAttrs, err := b.Bucket.Attributes(ctx, name)
		if err != nil || baseAttrs.Version == nil {
			return errOpaqueETagCondition
		}
		delegated = append(delegated, objstore.WithIfMatch(baseAttrs.Version))
	}
	if params.Condition != nil && b.failNextCASBeforeWrite {
		b.failNextCASBeforeWrite = false
		return errInjectedAuthorityCASBeforeWrite
	}
	if err := b.Bucket.Upload(ctx, name, reader, delegated...); err != nil {
		return err
	}
	if params.Condition != nil && b.loseNextCASResponse {
		b.loseNextCASResponse = false
		return errors.New("injected lost ETag CAS response")
	}
	return nil
}

func TestExecutorChangeDoesNotInferLostCASFromOldLogicalState(t *testing.T) {
	ctx := context.Background()
	base := objstore.NewInMemBucket()
	t.Cleanup(func() { _ = base.Close() })
	bucket := &etagAuthorityBucket{Bucket: base}
	store, err := NewAuthorityStateStore(bucket, "etag-authority")
	if err != nil {
		t.Fatal(err)
	}
	const uid, source = "lost-response-sts", "lost-response-source"
	if err := store.WriteRoot(ctx, uid, source, fenceLedger{StatefulSetUID: uid, Ledger: fencing.Ledger{SourceClusterID: source}}, nil); err != nil {
		t.Fatal(err)
	}
	executor := &FencingExecutor{AuthorityState: store}
	bucket.failNextCASBeforeWrite = true
	if _, err := executor.change(ctx, uid, source, func(*fencing.Ledger) error { return nil }); !errors.Is(err, errInjectedAuthorityCASBeforeWrite) {
		t.Fatalf("executor treated unchanged logical state as proof this CAS committed: %v", err)
	}
}

func (b *etagAuthorityBucket) IsConditionNotMetErr(err error) bool {
	return errors.Is(err, errOpaqueETagCondition) || b.Bucket.IsConditionNotMetErr(err)
}

type noConditionalAuthorityBucket struct {
	objstore.Bucket
	supported []objstore.ObjectUploadOptionType
}

func (b noConditionalAuthorityBucket) SupportedObjectUploadOptions() []objstore.ObjectUploadOptionType {
	return b.supported
}

func newAuthorityStateStoreForTest(t *testing.T) (*AuthorityStateStore, *objstore.InMemBucket) {
	t.Helper()
	bucket := objstore.NewInMemBucket()
	store, err := NewAuthorityStateStore(bucket, "test-authority")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bucket.Close() })
	return store, bucket
}

func TestAuthorityStateReopensFromObjectStorageWithoutLocalOrKubeIndex(t *testing.T) {
	store, bucket := newAuthorityStateStoreForTest(t)
	ctx := context.Background()
	const uid, source = "statefulset-uid", "source-id"
	state := fenceLedger{StatefulSetUID: uid, Ledger: fencing.Ledger{SourceClusterID: source}}
	if err := store.WriteRoot(ctx, uid, source, state, nil); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewAuthorityStateStore(bucket, "test-authority")
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := reopened.ReadRoot(ctx, uid, source)
	if err != nil || hashJSON(got) != hashJSON(state) {
		t.Fatalf("root did not reopen from canonical ObjectStorage: state=%+v err=%v", got, err)
	}
}

func TestInitializeRootDoesNotSilentlyReplaceLegacyCooperativeHistory(t *testing.T) {
	e, fixture, _, _ := newMutationStoreFixture(t)
	ctx := t.Context()
	const uid, source = "sts", "source"

	store := e.AuthorityState
	rootKey, identity, err := store.rootKey(uid, source)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.authorityBucket.Delete(ctx, rootKey); err != nil {
		t.Fatal(err)
	}
	if err := fixture.authorityBucket.Delete(ctx, store.genesisKey(identity)); err != nil {
		t.Fatal(err)
	}

	e.AuthorityState = nil
	e.LegacyCooperativeControlState = true
	oldRun := fencing.Identity{SourceClusterID: source, NodeID: "old-node", PodUID: "old-pod", RunID: "old-run"}
	if _, err := e.change(ctx, uid, source, func(ledger *fencing.Ledger) error {
		if err := ledger.Register(oldRun); err != nil {
			return err
		}
		return ledger.Close("old-fence", "old-request")
	}); err != nil {
		t.Fatal(err)
	}
	legacy, err := e.readLegacyCooperative(ctx, uid, source)
	if err != nil || legacy.Ledger.ClosedBy != "old-fence" || len(legacy.Ledger.Runs) != 1 {
		t.Fatalf("failed to seed legacy cooperative history: state=%+v err=%v", legacy.Ledger, err)
	}

	e.AuthorityState = store
	err = e.initializeRoot(ctx, uid, source)
	remote, _, remoteErr := store.ReadRoot(ctx, uid, source)
	if err == nil && remoteErr == nil && len(remote.Ledger.Runs) == 0 && remote.Ledger.ClosedBy == "" {
		t.Errorf("initializeRoot created an empty canonical root while the same root has closed legacy history")
	}
	if err == nil {
		t.Errorf("initializeRoot accepted a backend switch without reconciling legacy cooperative history")
	}
	if remoteErr == nil {
		t.Errorf("canonical root exists after refusing legacy-to-remote initialization: state=%+v", remote.Ledger)
	} else if !errors.Is(remoteErr, errAuthorityStateNotFound) {
		t.Errorf("canonical root read error = %v, want missing root after fail-closed rejection", remoteErr)
	}
	legacyAfter, legacyErr := e.readLegacyCooperative(ctx, uid, source)
	if legacyErr != nil || legacyAfter.Ledger.ClosedBy != "old-fence" || len(legacyAfter.Ledger.Runs) != 1 {
		t.Errorf("legacy history changed during backend switch: state=%+v err=%v", legacyAfter.Ledger, legacyErr)
	}
}

func TestInitializeRootAllowsNewCanonicalRootWithoutLegacyHistory(t *testing.T) {
	e, fixture, _, _ := newMutationStoreFixture(t)
	ctx := t.Context()
	const uid, source = "sts", "source"

	store := e.AuthorityState
	rootKey, identity, err := store.rootKey(uid, source)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.authorityBucket.Delete(ctx, rootKey); err != nil {
		t.Fatal(err)
	}
	if err := fixture.authorityBucket.Delete(ctx, store.genesisKey(identity)); err != nil {
		t.Fatal(err)
	}

	if err := e.initializeRoot(ctx, uid, source); err != nil {
		t.Fatalf("new canonical root without legacy history was rejected: %v", err)
	}
	root, _, err := store.ReadRoot(ctx, uid, source)
	if err != nil || root.StatefulSetUID != uid || root.Ledger.SourceClusterID != source || len(root.Ledger.Runs) != 0 {
		t.Fatalf("new canonical root was not initialized as empty: state=%+v err=%v", root, err)
	}
	if _, _, err := store.read(ctx, store.genesisKey(identity), identity); err != nil {
		t.Fatalf("new canonical root genesis was not initialized: %v", err)
	}
}

func TestInitializeRootFailsClosedWhenLegacyLedgerCannotBeVerified(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*testing.T, *FencingExecutor, *mutationStoreFixture, string, string)
	}{
		{
			name: "malformed ledger",
			configure: func(_ *testing.T, e *FencingExecutor, fixture *mutationStoreFixture, uid, source string) {
				fixture.objects[e.ledgerPath(uid, source)] = object{
					"metadata": object{"name": "legacy-ledger"},
					"data":     object{"ledger.json": "{"},
				}
			},
		},
		{
			name: "Kubernetes read error",
			configure: func(t *testing.T, e *FencingExecutor, _ *mutationStoreFixture, _, _ string) {
				server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method != http.MethodGet {
						http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
						return
					}
					w.WriteHeader(http.StatusServiceUnavailable)
				}))
				t.Cleanup(server.Close)
				e.Kube.BaseURL = server.URL
				e.Kube.Client = server.Client()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, fixture, _, _ := newMutationStoreFixture(t)
			ctx := t.Context()
			const uid, source = "sts", "source"

			store := e.AuthorityState
			rootKey, identity, err := store.rootKey(uid, source)
			if err != nil {
				t.Fatal(err)
			}
			if err := fixture.authorityBucket.Delete(ctx, rootKey); err != nil {
				t.Fatal(err)
			}
			if err := fixture.authorityBucket.Delete(ctx, store.genesisKey(identity)); err != nil {
				t.Fatal(err)
			}
			tt.configure(t, e, fixture, uid, source)

			if err := e.initializeRoot(ctx, uid, source); err == nil {
				t.Fatal("initializeRoot accepted an unverifiable legacy ledger")
			}
			if _, _, err := store.ReadRoot(ctx, uid, source); !errors.Is(err, errAuthorityStateNotFound) {
				t.Errorf("canonical root read error = %v, want missing root after fail-closed rejection", err)
			}
			if _, _, err := store.read(ctx, store.genesisKey(identity), identity); !errors.Is(err, errAuthorityStateNotFound) {
				t.Errorf("canonical genesis read error = %v, want missing genesis after fail-closed rejection", err)
			}
		})
	}
}

func TestInitializeRootRequiresKubeToVerifyFreshCanonicalRoot(t *testing.T) {
	e, fixture, _, _ := newMutationStoreFixture(t)
	ctx := t.Context()
	const uid, source = "sts", "source"

	store := e.AuthorityState
	rootKey, identity, err := store.rootKey(uid, source)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.authorityBucket.Delete(ctx, rootKey); err != nil {
		t.Fatal(err)
	}
	if err := fixture.authorityBucket.Delete(ctx, store.genesisKey(identity)); err != nil {
		t.Fatal(err)
	}
	e.Kube = nil

	if err := e.initializeRoot(ctx, uid, source); err == nil {
		t.Fatal("fresh canonical root initialized without verifying legacy Kubernetes state")
	}
	if _, _, err := store.read(ctx, rootKey, identity); !errors.Is(err, errAuthorityStateNotFound) {
		t.Errorf("canonical root read error = %v, want no root after failed verification", err)
	}
	if _, _, err := store.read(ctx, store.genesisKey(identity), identity); !errors.Is(err, errAuthorityStateNotFound) {
		t.Errorf("canonical genesis read error = %v, want no genesis after failed verification", err)
	}
}

func TestInitializeRootResumesCanonicalStateWithoutKube(t *testing.T) {
	e, _, id, runtime := newMutationStoreFixture(t)
	ctx := t.Context()
	const uid, source = "sts", "source"
	_, identity, err := e.AuthorityState.rootKey(uid, source)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.AuthorityState.ensureRootGenesis(ctx, identity); err != nil {
		t.Fatalf("seed canonical genesis: %v", err)
	}
	if _, err := e.change(ctx, uid, source, func(ledger *fencing.Ledger) error {
		ledger.RequireRuntimeBinding = true
		ledger.RequireMutationJournal = true
		ledger.ArchiveGenesisState = fencing.ArchiveGenesisRegistrationPending
		if err := ledger.RegisterEnrolled(id, runtime); err != nil {
			return err
		}
		return ledger.Close("resume-fence", "resume-request")
	}); err != nil {
		t.Fatal(err)
	}
	before, beforeVersion, err := e.AuthorityState.ReadRoot(ctx, uid, source)
	if err != nil {
		t.Fatal(err)
	}
	if before.Ledger.ClosedBy != "resume-fence" || len(before.Ledger.Runs) != 1 || !before.Ledger.RequireMutationJournal || !before.Ledger.RequireRuntimeBinding {
		t.Fatalf("resume fixture lacks closed run/profile state: %+v", before.Ledger)
	}
	e.Kube = nil

	if err := e.initializeRoot(ctx, uid, source); err != nil {
		t.Fatalf("existing canonical root could not resume without legacy Kubernetes lookup: %v", err)
	}
	after, afterVersion, err := e.AuthorityState.ReadRoot(ctx, uid, source)
	if err != nil {
		t.Fatal(err)
	}
	if hashJSON(after) != hashJSON(before) || !reflect.DeepEqual(afterVersion, beforeVersion) {
		t.Fatalf("canonical resume changed root state or version: before=%+v after=%+v", before.Ledger, after.Ledger)
	}
}

func TestArchiveGenesisRefCASRetryUsesDecodedStructuralEquality(t *testing.T) {
	store, _ := newAuthorityStateStoreForTest(t)
	ctx := context.Background()
	scope := fencing.ArchiveScope{Provider: "s3", Bucket: "archive", Prefix: "base/source", ClusterID: "source"}
	scopeHash, err := scope.Hash()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimArchiveScope(ctx, scope, 1, "sts-uid", "source", hashJSON([]string{"registration"})); err != nil {
		t.Fatal(err)
	}
	ref := fencing.ArchiveGenesisRef{
		Version: fencing.ArchiveGenesisVersion, ConfigID: 1, ScopeHash: scopeHash,
		Mutation: &fencing.ArchiveGenesisMutation{ProducerUID: "producer", Sequence: 1, Intent: fencing.MutationIntent{
			ContractVersion: fencing.MutationIntentVersion,
			StoreScopeHash:  hashJSON([]string{"mutation-scope"}),
			Condition:       fencing.MutationCondition{Mode: fencing.MutationCreateOnly},
			OperationID:     fencing.MutationRequestID("producer", 1), Kind: "upload", Key: "archive/head.bin",
			PayloadRef: "payload-ref", PayloadDigest: hashJSON([]string{"head"}), PayloadSize: 4,
		}},
	}
	wrongConfig := ref
	wrongConfig.ConfigID++
	if err := store.SetArchiveScopeGenesisRef(ctx, scope, wrongConfig); err == nil {
		t.Fatal("genesis reference changed the config ID fixed at claim creation")
	}
	if err := store.SetArchiveScopeGenesisRef(ctx, scope, ref); err != nil {
		t.Fatal(err)
	}
	got, _, err := store.ReadArchiveScopeClaim(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	if got.ConfigID != ref.ConfigID || got.GenesisRef == nil {
		t.Fatalf("committed config/reference missing: %+v", got)
	}
	// A JSON round-trip ensures nested pointers are distinct as they are after
	// reading canonical storage, while the committed reference remains exact.
	encoded, err := json.Marshal(ref)
	if err != nil {
		t.Fatal(err)
	}
	var decoded fencing.ArchiveGenesisRef
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := store.SetArchiveScopeGenesisRef(ctx, scope, decoded); err != nil {
		t.Fatalf("decoded exact reference was not an idempotent retry: %v", err)
	}
	decoded.ConfigID++
	if err := store.SetArchiveScopeGenesisRef(ctx, scope, decoded); err == nil {
		t.Fatal("different config ID replaced the immutable genesis reference")
	}
}

func TestAuthorityStateRejectsStaleRootCASAndPreservesClosure(t *testing.T) {
	store, _ := newAuthorityStateStoreForTest(t)
	ctx := context.Background()
	const uid, source = "statefulset-uid", "source-id"
	initial := fenceLedger{StatefulSetUID: uid, Ledger: fencing.Ledger{SourceClusterID: source}}
	if err := store.WriteRoot(ctx, uid, source, initial, nil); err != nil {
		t.Fatal(err)
	}
	_, oldVersion, err := store.ReadRoot(ctx, uid, source)
	if err != nil {
		t.Fatal(err)
	}
	closed := initial
	closed.Ledger.ClosedBy = "fence-op"
	closed.Ledger.ClosedHash = strings.Repeat("a", 64)
	if err := store.WriteRoot(ctx, uid, source, closed, oldVersion); err != nil {
		t.Fatal(err)
	}
	staleOpen := initial
	if err := store.WriteRoot(ctx, uid, source, staleOpen, oldVersion); !errors.Is(err, errAuthorityStateConflict) {
		t.Fatalf("stale open state replaced closed root: %v", err)
	}
	got, _, err := store.ReadRoot(ctx, uid, source)
	if err != nil || got.Ledger.ClosedBy != "fence-op" {
		t.Fatalf("closure was not retained after stale CAS: %+v %v", got, err)
	}
}

func TestAuthorityRootCannotReinitializeAfterRegisteredRootLoss(t *testing.T) {
	store, bucket := newAuthorityStateStoreForTest(t)
	ctx := context.Background()
	const uid, source = "statefulset-uid", "source-id"
	executor := &FencingExecutor{AuthorityState: store, Kube: newMissingLegacyLedgerKube(t)}
	if err := executor.initializeRoot(ctx, uid, source); err != nil {
		t.Fatal(err)
	}
	key, identity, err := store.rootKey(uid, source)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ensureRootGenesis(ctx, identity); err != nil {
		t.Fatal(err)
	}
	if err := bucket.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if err := executor.initializeRoot(ctx, uid, source); !errors.Is(err, errAuthorityRootLost) {
		t.Fatalf("missing registered root was treated as a new authority: %v", err)
	}
}

func TestMutationJournalProfileIsPinnedBeforeRootRegistration(t *testing.T) {
	ctx := context.Background()
	const uid, source = "statefulset-uid", "source-id"

	t.Run("new required root persists both requirements", func(t *testing.T) {
		store, _ := newAuthorityStateStoreForTest(t)
		executor := &FencingExecutor{AuthorityState: store, RequireMutationJournal: true, Kube: newMissingLegacyLedgerKube(t)}
		if err := executor.initializeRoot(ctx, uid, source); err != nil {
			t.Fatal(err)
		}
		state, _, err := store.ReadRoot(ctx, uid, source)
		if err != nil {
			t.Fatal(err)
		}
		if !state.Ledger.RequireMutationJournal || !state.Ledger.RequireRuntimeBinding {
			t.Fatalf("required profile was not persisted before registration: %+v", state.Ledger)
		}
	})

	t.Run("cooperative root cannot be upgraded in place", func(t *testing.T) {
		store, _ := newAuthorityStateStoreForTest(t)
		cooperative := &FencingExecutor{AuthorityState: store, Kube: newMissingLegacyLedgerKube(t)}
		if err := cooperative.initializeRoot(ctx, uid, source); err != nil {
			t.Fatal(err)
		}
		before, version, err := store.ReadRoot(ctx, uid, source)
		if err != nil {
			t.Fatal(err)
		}
		required := &FencingExecutor{AuthorityState: store, RequireMutationJournal: true}
		if err := required.initializeRoot(ctx, uid, source); err == nil {
			t.Fatal("required profile upgraded an existing cooperative root")
		}
		after, afterVersion, err := store.ReadRoot(ctx, uid, source)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(version, afterVersion) || hashJSON(before) != hashJSON(after) || after.Ledger.RequireMutationJournal {
			t.Fatalf("profile mismatch mutated the canonical root: before=%+v after=%+v", before.Ledger, after.Ledger)
		}
	})

	t.Run("nonrequired executor cannot downgrade required root", func(t *testing.T) {
		store, _ := newAuthorityStateStoreForTest(t)
		required := &FencingExecutor{AuthorityState: store, RequireMutationJournal: true, Kube: newMissingLegacyLedgerKube(t)}
		if err := required.initializeRoot(ctx, uid, source); err != nil {
			t.Fatal(err)
		}
		cooperative := &FencingExecutor{AuthorityState: store, Kube: newMissingLegacyLedgerKube(t)}
		if err := cooperative.initializeRoot(ctx, uid, source); err != nil {
			t.Fatal(err)
		}
		state, _, err := store.ReadRoot(ctx, uid, source)
		if err != nil || !state.Ledger.RequireMutationJournal || !state.Ledger.RequireRuntimeBinding {
			t.Fatalf("required root was downgraded: %+v %v", state.Ledger, err)
		}
	})
}

func TestAuthorityProducerIdentityIsStableAcrossStoreReopenAndNotServerUID(t *testing.T) {
	store, bucket := newAuthorityStateStoreForTest(t)
	runtime := authorityTestRuntime()
	id := fencing.Identity{SourceClusterID: "source", NodeID: "n1", PodUID: "pod", RunID: "run"}
	producer, err := fencing.NewMutationProducer(id, runtime)
	if err != nil {
		t.Fatal(err)
	}
	producerUID := stableMutationProducerUID(runtime)
	if producerUID == "" || strings.Contains(producerUID, "kube") {
		t.Fatalf("producer identity is not a stable authority identity: %q", producerUID)
	}
	if err := store.WriteProducer(context.Background(), runtime, *producer, nil); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewAuthorityStateStore(bucket, "test-authority")
	if err != nil {
		t.Fatal(err)
	}
	got, gotUID, _, err := reopened.ReadProducer(context.Background(), runtime)
	if err != nil || gotUID != producerUID || got.Runtime != runtime {
		t.Fatalf("producer identity changed after reopen: uid=%q producer=%+v err=%v", gotUID, got, err)
	}
}

func TestAuthorityProducerCASCannotReopenClosedProducer(t *testing.T) {
	store, _ := newAuthorityStateStoreForTest(t)
	runtime := authorityTestRuntime()
	id := fencing.Identity{SourceClusterID: "source", NodeID: "n1", PodUID: "pod", RunID: "run", WALIdentity: "wal"}
	producer, err := fencing.NewMutationProducer(id, runtime)
	if err != nil {
		t.Fatal(err)
	}
	if err := producer.Activate(id); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteProducer(context.Background(), runtime, *producer, nil); err != nil {
		t.Fatal(err)
	}
	openSnapshot := *producer
	_, _, oldVersion, err := store.ReadProducer(context.Background(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	if err := producer.Close("stop-op", strings.Repeat("b", 64)); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteProducer(context.Background(), runtime, *producer, oldVersion); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteProducer(context.Background(), runtime, openSnapshot, oldVersion); !errors.Is(err, errAuthorityStateConflict) {
		t.Fatalf("stale open producer replaced closed state: %v", err)
	}
	got, _, _, err := store.ReadProducer(context.Background(), runtime)
	if err != nil || got.Phase != fencing.MutationClosed {
		t.Fatalf("closed producer did not survive stale CAS: %+v %v", got, err)
	}
}

func authorityTestRuntime() fencing.RuntimeBinding {
	return fencing.RuntimeBinding{
		AuthorityID: "authority", NamespaceUID: "namespace-uid", BindingUID: "binding-uid",
		StatefulSetUID: "sts-uid", SourceClusterID: "source", NodeID: "n1", PodUID: "pod", RunID: "run",
		NodeName: "node-1", NodeUID: "node-uid", MachineID: "machine", BootID: "boot", RuntimeID: "runtime",
		SandboxID: "sandbox", ContainerID: "container", ImageID: "image",
	}
}

func TestAuthorityStateRejectsCorruptOrMismatchedObject(t *testing.T) {
	store, bucket := newAuthorityStateStoreForTest(t)
	ctx := context.Background()
	const uid, source = "statefulset-uid", "source-id"
	state := fenceLedger{StatefulSetUID: uid, Ledger: fencing.Ledger{SourceClusterID: source}}
	if err := store.WriteRoot(ctx, uid, source, state, nil); err != nil {
		t.Fatal(err)
	}
	key, _, _ := store.rootKey(uid, source)
	if err := bucket.Upload(ctx, key, strings.NewReader(`{"version":1,"identity":"wrong","digest":"x","value":{}}`)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.ReadRoot(ctx, uid, source); err == nil {
		t.Fatal("corrupt overwrite without CAS was accepted")
	}
}

func TestAuthorityStateEnvelopeUsesStrictJSON(t *testing.T) {
	value := json.RawMessage(`{"ok":true}`)
	if got := authorityStateDigest(value); len(got) != 64 {
		t.Fatalf("unexpected evidence digest: %q", got)
	}
}

func TestAuthorityStateSupportsOpaqueETagCASWithoutABA(t *testing.T) {
	ctx := context.Background()
	base := objstore.NewInMemBucket()
	t.Cleanup(func() { _ = base.Close() })
	bucket := &etagAuthorityBucket{Bucket: base}
	store, err := NewAuthorityStateStore(bucket, "etag-authority")
	if err != nil {
		t.Fatal(err)
	}
	const uid, source = "etag-sts", "etag-source"
	a := fenceLedger{StatefulSetUID: uid, Ledger: fencing.Ledger{SourceClusterID: source}}
	if err := store.WriteRoot(ctx, uid, source, a, nil); err != nil {
		t.Fatal(err)
	}
	_, firstVersion, err := store.ReadRoot(ctx, uid, source)
	if err != nil || firstVersion == nil || firstVersion.Type != objstore.ETag || !strings.HasPrefix(firstVersion.Value, "opaque-etag/") {
		t.Fatalf("root version is not an opaque ETag: version=%+v err=%v", firstVersion, err)
	}
	wrongType := *firstVersion
	wrongType.Type = objstore.Generation
	if err := store.WriteRoot(ctx, uid, source, a, &wrongType); err == nil {
		t.Fatal("root CAS accepted a version type different from the current object's ETag")
	}
	b := a
	b.Ledger.ClosedBy = "closed"
	b.Ledger.ClosedHash = strings.Repeat("a", 64)
	if err := store.WriteRoot(ctx, uid, source, b, firstVersion); err != nil {
		t.Fatal(err)
	}
	_, secondVersion, err := store.ReadRoot(ctx, uid, source)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WriteRoot(ctx, uid, source, a, secondVersion); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteRoot(ctx, uid, source, b, firstVersion); !errors.Is(err, errAuthorityStateConflict) {
		t.Fatalf("stale ETag succeeded after logical A-to-B-to-A: %v", err)
	}
	bucket.loseNextCASResponse = true
	closedAgain := a
	closedAgain.Ledger.ClosedBy = "new-close"
	closedAgain.Ledger.ClosedHash = strings.Repeat("b", 64)
	_, currentVersion, err := store.ReadRoot(ctx, uid, source)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WriteRoot(ctx, uid, source, closedAgain, currentVersion); err != nil {
		t.Fatalf("lost ETag CAS response was not reconciled from canonical bytes: %v", err)
	}
	reopened, err := NewAuthorityStateStore(bucket, "etag-authority")
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := reopened.ReadRoot(ctx, uid, source)
	if err != nil || got.Ledger.ClosedBy != "new-close" {
		t.Fatalf("ETag-backed root did not reopen after lost CAS response: %+v %v", got, err)
	}
}

func TestAuthorityETagRevisionIsBoundToEnvelopeDigest(t *testing.T) {
	ctx := context.Background()
	base := objstore.NewInMemBucket()
	t.Cleanup(func() { _ = base.Close() })
	store, err := NewAuthorityStateStore(&etagAuthorityBucket{Bucket: base}, "etag-authority")
	if err != nil {
		t.Fatal(err)
	}
	const uid, source = "revision-sts", "revision-source"
	state := fenceLedger{StatefulSetUID: uid, Ledger: fencing.Ledger{SourceClusterID: source}}
	if err := store.WriteRoot(ctx, uid, source, state, nil); err != nil {
		t.Fatal(err)
	}
	key, _, err := store.rootKey(uid, source)
	if err != nil {
		t.Fatal(err)
	}
	r, err := base.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(r)
	closeErr := r.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("read root envelope: %v %v", readErr, closeErr)
	}
	var envelope authorityStateEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		t.Fatal(err)
	}
	if strings.HasPrefix(envelope.Revision, "0") {
		envelope.Revision = "1" + envelope.Revision[1:]
	} else {
		envelope.Revision = "0" + envelope.Revision[1:]
	}
	tampered, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if err := base.Upload(ctx, key, strings.NewReader(string(tampered))); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.ReadRoot(ctx, uid, source); err == nil {
		t.Fatal("root envelope accepted a revision changed without updating its digest")
	}
}

func TestAuthorityStateRequiresConditionalCreateAndCASCapabilities(t *testing.T) {
	for _, test := range []struct {
		name      string
		supported []objstore.ObjectUploadOptionType
	}{
		{name: "neither", supported: nil},
		{name: "create without CAS", supported: []objstore.ObjectUploadOptionType{objstore.IfNotExists}},
		{name: "CAS without create", supported: []objstore.ObjectUploadOptionType{objstore.IfMatch}},
	} {
		t.Run(test.name, func(t *testing.T) {
			bucket := noConditionalAuthorityBucket{Bucket: objstore.NewInMemBucket(), supported: test.supported}
			t.Cleanup(func() { _ = bucket.Close() })
			if _, err := NewAuthorityStateStore(bucket, "authority"); err == nil {
				t.Fatal("authority store accepted a bucket without both conditional-write capabilities")
			}
			if _, err := NewAuthorityEvidenceStore(bucket, "authority", "root", "source"); err == nil {
				t.Fatal("authority evidence accepted a bucket without both conditional-write capabilities")
			}
		})
	}
}

func TestAuthorityProducerETagCASAndRootGenesisLossAreFailClosed(t *testing.T) {
	ctx := context.Background()
	base := objstore.NewInMemBucket()
	t.Cleanup(func() { _ = base.Close() })
	store, err := NewAuthorityStateStore(&etagAuthorityBucket{Bucket: base}, "etag-authority")
	if err != nil {
		t.Fatal(err)
	}
	runtime := authorityTestRuntime()
	id := fencing.Identity{SourceClusterID: runtime.SourceClusterID, NodeID: runtime.NodeID, PodUID: runtime.PodUID, RunID: runtime.RunID, WALIdentity: "wal"}
	producer, err := fencing.NewMutationProducer(id, runtime)
	if err != nil {
		t.Fatal(err)
	}
	if err := producer.Activate(id); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteProducer(ctx, runtime, *producer, nil); err != nil {
		t.Fatal(err)
	}
	open := *producer
	_, _, oldVersion, err := store.ReadProducer(ctx, runtime)
	if err != nil || oldVersion == nil || oldVersion.Type != objstore.ETag {
		t.Fatalf("producer did not use opaque ETag version: %+v %v", oldVersion, err)
	}
	if err := producer.Close("close-op", strings.Repeat("c", 64)); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteProducer(ctx, runtime, *producer, oldVersion); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteProducer(ctx, runtime, open, oldVersion); !errors.Is(err, errAuthorityStateConflict) {
		t.Fatalf("stale open producer replaced closed producer with ETag CAS: %v", err)
	}
	reopened, err := NewAuthorityStateStore(&etagAuthorityBucket{Bucket: base}, "etag-authority")
	if err != nil {
		t.Fatal(err)
	}
	got, _, _, err := reopened.ReadProducer(ctx, runtime)
	if err != nil || got.Phase != fencing.MutationClosed {
		t.Fatalf("closed producer did not survive ETag store reopen: %+v %v", got, err)
	}
	executor := &FencingExecutor{AuthorityState: reopened, Kube: newMissingLegacyLedgerKube(t)}
	const rootUID, sourceID = "genesis-sts", "genesis-source"
	if err := executor.initializeRoot(ctx, rootUID, sourceID); err != nil {
		t.Fatal(err)
	}
	rootKey, rootIdentity, err := reopened.rootKey(rootUID, sourceID)
	if err != nil {
		t.Fatal(err)
	}
	if err := base.Delete(ctx, rootKey); err != nil {
		t.Fatal(err)
	}
	if err := executor.initializeRoot(ctx, rootUID, sourceID); !errors.Is(err, errAuthorityRootLost) {
		t.Fatalf("ETag-backed genesis allowed root recreation after loss: %v", err)
	}
	if err := reopened.ensureRootGenesis(ctx, rootIdentity); err != nil {
		t.Fatalf("root-loss test damaged the durable genesis marker: %v", err)
	}
}
