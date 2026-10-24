package operator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/thanos-io/objstore"
	"google.golang.org/api/googleapi"
)

var errAuthorityRootReadbackMissing = errors.New("injected confirmation readback miss")

type rootWriteReadbackMissBucket struct {
	objstore.Bucket
	primaryErr  error
	writeFailed bool
	missed      bool
}

func (b *rootWriteReadbackMissBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	if b.writeFailed && !b.missed {
		b.missed = true
		return objstore.ObjectAttributes{}, errAuthorityRootReadbackMissing
	}
	return b.Bucket.Attributes(ctx, name)
}

func (b *rootWriteReadbackMissBucket) Upload(context.Context, string, io.Reader, ...objstore.ObjectUploadOption) error {
	b.writeFailed = true
	return b.primaryErr
}

func (b *rootWriteReadbackMissBucket) IsObjNotFoundErr(err error) bool {
	return errors.Is(err, errAuthorityRootReadbackMissing) || b.Bucket.IsObjNotFoundErr(err)
}

type rootCommitThenLoseAckBucket struct {
	objstore.Bucket
	ackErr  error
	uploads int
}

func (b *rootCommitThenLoseAckBucket) Upload(ctx context.Context, name string, reader io.Reader, options ...objstore.ObjectUploadOption) error {
	b.uploads++
	if err := b.Bucket.Upload(ctx, name, reader, options...); err != nil {
		return err
	}
	if b.uploads == 1 {
		return b.ackErr
	}
	return nil
}

func TestAuthorityRootProviderStatusDiagnostics(t *testing.T) {
	store, _ := newAuthorityStateStoreForTest(t)
	const canary = "provider-message-must-not-be-a-diagnostic-code"
	statuses := []struct {
		status int
		cause  string
	}{
		{403, "permission"},
		{429, "http_429"},
		{503, "http_503"},
		{504, "timeout"},
	}
	providers := []struct {
		name string
		make func(int) error
		as   func(error) bool
	}{
		{
			name: "googleapi",
			make: func(status int) error { return &googleapi.Error{Code: status, Message: canary, Body: canary} },
			as: func(err error) bool {
				var got *googleapi.Error
				return errors.As(err, &got) && got != nil && got.Code != 0
			},
		},
		{
			name: "minio",
			make: func(status int) error { return minio.ErrorResponse{StatusCode: status, Code: canary, Message: canary} },
			as: func(err error) bool {
				var got minio.ErrorResponse
				return errors.As(err, &got) && got.StatusCode != 0
			},
		},
	}
	for _, provider := range providers {
		for _, tc := range statuses {
			for _, wrapping := range []string{"direct", "wrapped", "joined-after-primary-diagnostic"} {
				t.Run(fmt.Sprintf("%s/%s/%d", provider.name, wrapping, tc.status), func(t *testing.T) {
					original := provider.make(tc.status)
					err := original
					switch wrapping {
					case "wrapped":
						err = fmt.Errorf("provider operation: %w", original)
					case "joined-after-primary-diagnostic":
						err = errors.Join(store.rootDiagnostic("genesis", original), errAuthorityStateNotFound)
					}
					if !provider.as(err) {
						t.Fatal("SDK error type was lost from the wrapped/joined chain")
					}
					if wrapping == "joined-after-primary-diagnostic" {
						if !errors.Is(err, errAuthorityStateNotFound) {
							t.Fatal("readback NotFound was lost from the joined chain")
						}
						if !strings.HasPrefix(err.Error(), "fencing: root-init phase=genesis cause="+tc.cause+":") {
							t.Fatalf("primary provider cause was not retained: %v", err)
						}
						return
					}
					if got := store.rootCause(err); got != tc.cause {
						t.Fatalf("rootCause = %q, want %q", got, tc.cause)
					}
					if got := store.rootCause(err); strings.Contains(got, canary) {
						t.Fatalf("diagnostic code leaked provider content: %q", got)
					}
				})
			}
		}
	}
}

func TestAuthorityRootConditionalUpdateKeepsSDK503PrimaryCause(t *testing.T) {
	base := objstore.NewInMemBucket()
	t.Cleanup(func() { _ = base.Close() })
	baseStore, err := NewAuthorityStateStore(base, "conditional-root-diagnostic")
	if err != nil {
		t.Fatal(err)
	}
	const uid, source = "sts", "source"
	initial := fenceLedger{StatefulSetUID: uid, Ledger: fencing.Ledger{SourceClusterID: source, ClosedBy: "old-close"}}
	if err := baseStore.WriteRoot(t.Context(), uid, source, initial, nil); err != nil {
		t.Fatal(err)
	}
	_, version, err := baseStore.ReadRoot(t.Context(), uid, source)
	if err != nil || version == nil {
		t.Fatalf("read initial root version: version=%+v err=%v", version, err)
	}

	providerErr := &googleapi.Error{Code: 503, Message: "private provider response"}
	bucket := &rootWriteReadbackMissBucket{Bucket: base, primaryErr: providerErr}
	store, err := NewAuthorityStateStore(bucket, "conditional-root-diagnostic")
	if err != nil {
		t.Fatal(err)
	}
	next := initial
	next.Ledger.ClosedBy = "new-close"
	writeErr := store.WriteRoot(t.Context(), uid, source, next, version)
	if writeErr == nil {
		t.Fatal("conditional root write unexpectedly succeeded")
	}
	if !bucket.missed {
		t.Fatal("confirmation readback was not attempted after the conditional write failure")
	}
	if !errors.Is(writeErr, providerErr) {
		t.Errorf("WriteRoot lost primary provider error: %v", writeErr)
	}
	if !errors.Is(writeErr, errAuthorityStateNotFound) {
		t.Errorf("WriteRoot lost confirmation-readback miss: %v", writeErr)
	}
	var gotProviderErr *googleapi.Error
	if !errors.As(writeErr, &gotProviderErr) || gotProviderErr.Code != http.StatusServiceUnavailable {
		t.Errorf("WriteRoot lost SDK status: got=%v", gotProviderErr)
	}
	if !strings.HasPrefix(writeErr.Error(), "fencing: root-init phase=create cause=http_503:") {
		t.Errorf("diagnostic hid the primary 503: %v", writeErr)
	}
	if got := store.rootCause(writeErr); got != "http_503" {
		t.Errorf("primary write cause = %q, want http_503", got)
	}
	unchanged, _, readErr := baseStore.ReadRoot(t.Context(), uid, source)
	if readErr != nil || unchanged.Ledger.ClosedBy != "old-close" {
		t.Errorf("failed conditional write changed canonical root: state=%+v err=%v", unchanged.Ledger, readErr)
	}
}

func TestAuthorityRootCreateOnlyCommitThenLostAckPreservesRoot(t *testing.T) {
	base := objstore.NewInMemBucket()
	t.Cleanup(func() { _ = base.Close() })
	ackErr := &googleapi.Error{Code: 503, Message: "private lost response"}
	bucket := &rootCommitThenLoseAckBucket{Bucket: base, ackErr: ackErr}
	store, err := NewAuthorityStateStore(bucket, "root-create-lost-ack")
	if err != nil {
		t.Fatal(err)
	}
	e := &FencingExecutor{AuthorityState: store, Kube: newMissingLegacyLedgerKube(t)}
	const uid, source = "sts", "source"
	if err := e.initializeRoot(t.Context(), uid, source); err != nil {
		t.Fatalf("root create response loss was not reconciled: %v", err)
	}
	before, beforeVersion, err := store.ReadRoot(t.Context(), uid, source)
	if err != nil || before.StatefulSetUID != uid || len(before.Ledger.Runs) != 0 || beforeVersion == nil {
		t.Fatalf("root was not created exactly once: root=%+v version=%+v err=%v", before, beforeVersion, err)
	}
	if err := e.initializeRoot(t.Context(), uid, source); err != nil {
		t.Fatalf("canonical root retry failed: %v", err)
	}
	after, afterVersion, err := store.ReadRoot(t.Context(), uid, source)
	if err != nil || hashJSON(after) != hashJSON(before) || !reflect.DeepEqual(afterVersion, beforeVersion) || bucket.uploads != 2 {
		t.Fatalf("root retry changed state or rewrote committed objects: before=%+v after=%+v uploads=%d err=%v", before.Ledger, after.Ledger, bucket.uploads, err)
	}
}

func TestAuthorityRootGenesisCreateOnlyCommitThenLostAckPreservesRoot(t *testing.T) {
	base := objstore.NewInMemBucket()
	t.Cleanup(func() { _ = base.Close() })
	baseStore, err := NewAuthorityStateStore(base, "genesis-create-lost-ack")
	if err != nil {
		t.Fatal(err)
	}
	const uid, source = "sts", "source"
	id := fencing.Identity{SourceClusterID: source, NodeID: "node", PodUID: "pod", RunID: "run"}
	initial := fenceLedger{StatefulSetUID: uid, Ledger: fencing.Ledger{SourceClusterID: source}}
	if err := initial.Ledger.Register(id); err != nil {
		t.Fatal(err)
	}
	if err := baseStore.WriteRoot(t.Context(), uid, source, initial, nil); err != nil {
		t.Fatal(err)
	}
	before, beforeVersion, err := baseStore.ReadRoot(t.Context(), uid, source)
	if err != nil || len(before.Ledger.Runs) != 1 {
		t.Fatalf("seed root lacks exactly one registration: root=%+v err=%v", before, err)
	}

	ackErr := &googleapi.Error{Code: 503, Message: "private genesis response loss"}
	bucket := &rootCommitThenLoseAckBucket{Bucket: base, ackErr: ackErr}
	store, err := NewAuthorityStateStore(bucket, "genesis-create-lost-ack")
	if err != nil {
		t.Fatal(err)
	}
	e := &FencingExecutor{AuthorityState: store}
	if err := e.initializeRoot(t.Context(), uid, source); err != nil {
		t.Fatalf("genesis create response loss was not reconciled: %v", err)
	}
	after, afterVersion, err := store.ReadRoot(t.Context(), uid, source)
	if err != nil || hashJSON(after) != hashJSON(before) || !reflect.DeepEqual(afterVersion, beforeVersion) || len(after.Ledger.Runs) != 1 || bucket.uploads != 1 {
		t.Fatalf("genesis retry changed root or duplicated registration: before=%+v after=%+v uploads=%d err=%v", before.Ledger, after.Ledger, bucket.uploads, err)
	}
	_, rootIdentity, err := store.rootKey(uid, source)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.read(t.Context(), store.genesisKey(rootIdentity), rootIdentity); err != nil {
		t.Fatalf("committed genesis marker is missing: %v", err)
	}
}
