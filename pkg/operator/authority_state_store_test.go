package operator

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/thanos-io/objstore"
)

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
	executor := &FencingExecutor{AuthorityState: store}
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
	_, oldVersion, err := store.ReadProducer(context.Background(), runtime)
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
