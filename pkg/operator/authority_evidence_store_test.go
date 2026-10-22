package operator

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/thanos-io/objstore"
)

func TestAuthorityEvidenceETagCASUsesOpaqueVersionAndPreventsABA(t *testing.T) {
	ctx := context.Background()
	base := objstore.NewInMemBucket()
	t.Cleanup(func() { _ = base.Close() })
	bucket := &etagAuthorityBucket{Bucket: base}
	store, err := NewAuthorityEvidenceStore(bucket, "authority", "root", "source")
	if err != nil {
		t.Fatal(err)
	}
	firstVersion, err := store.CreateImmutable(ctx, "mutable.json", []byte(`{"state":"A"}`))
	if err != nil {
		t.Fatal(err)
	}
	if firstVersion.Type != objstore.ETag || !strings.HasPrefix(firstVersion.Value, "opaque-etag/") {
		t.Fatalf("expected opaque ETag, got %+v", firstVersion)
	}
	wrongType := *firstVersion
	wrongType.Type = objstore.Generation
	if _, err := store.CompareAndSwap(ctx, "mutable.json", &wrongType, []byte(`{"state":"wrong"}`)); err == nil {
		t.Fatal("evidence CAS accepted a version type different from the current object's ETag")
	}
	secondVersion, err := store.CompareAndSwap(ctx, "mutable.json", firstVersion, []byte(`{"state":"B"}`))
	if err != nil {
		t.Fatal(err)
	}
	thirdVersion, err := store.CompareAndSwap(ctx, "mutable.json", secondVersion, []byte(`{"state":"A"}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompareAndSwap(ctx, "mutable.json", firstVersion, []byte(`{"state":"B"}`)); err == nil {
		t.Fatal("stale ETag succeeded after logical A-to-B-to-A")
	}
	if thirdVersion.Value == firstVersion.Value {
		t.Fatal("fresh CAS revision did not change opaque ETag")
	}
}

func TestAuthorityEvidenceETagCASReconcilesLostResponse(t *testing.T) {
	ctx := context.Background()
	base := objstore.NewInMemBucket()
	t.Cleanup(func() { _ = base.Close() })
	bucket := &etagAuthorityBucket{Bucket: base}
	store, err := NewAuthorityEvidenceStore(bucket, "authority", "root", "source")
	if err != nil {
		t.Fatal(err)
	}
	version, err := store.CreateImmutable(ctx, "mutable.json", []byte(`{"state":"before"}`))
	if err != nil {
		t.Fatal(err)
	}
	bucket.loseNextCASResponse = true
	if _, err := store.CompareAndSwap(ctx, "mutable.json", version, []byte(`{"state":"after"}`)); err != nil {
		t.Fatalf("lost ETag CAS response was not reconciled: %v", err)
	}
	raw, _, err := store.Read(ctx, "mutable.json")
	if err != nil {
		t.Fatal(err)
	}
	rawBytes, rawErr := io.ReadAll(raw)
	rawCloseErr := raw.Close()
	if rawErr != nil || rawCloseErr != nil || !bytes.HasPrefix(rawBytes, []byte(authorityCASBodyPrefix)) {
		t.Fatalf("ordinary Read did not preserve raw CAS object bytes: prefix=%q err=%v close=%v", rawBytes[:min(len(rawBytes), len(authorityCASBodyPrefix))], rawErr, rawCloseErr)
	}
	r, gotVersion, err := store.ReadMutable(ctx, "mutable.json")
	if err != nil {
		t.Fatal(err)
	}
	got, readErr := io.ReadAll(r)
	closeErr := r.Close()
	if readErr != nil || closeErr != nil || string(got) != `{"state":"after"}` || gotVersion == nil || gotVersion.Type != objstore.ETag {
		t.Fatalf("reopened mutable evidence=%q version=%+v read=%v close=%v", got, gotVersion, readErr, closeErr)
	}
}

func TestAuthorityEvidenceReadPreservesImmutableCASHeaderCanaryExactly(t *testing.T) {
	ctx := context.Background()
	base := objstore.NewInMemBucket()
	t.Cleanup(func() { _ = base.Close() })
	store, err := NewAuthorityEvidenceStore(&etagAuthorityBucket{Bucket: base}, "authority", "root", "source")
	if err != nil {
		t.Fatal(err)
	}
	canary := append([]byte(authorityCASBodyPrefix+strings.Repeat("a", 64)+"\n"), []byte("immutable payload bytes")...)
	if _, err := store.CreateImmutable(ctx, "payload.bin", canary); err != nil {
		t.Fatal(err)
	}
	r, _, err := store.Read(ctx, "payload.bin")
	if err != nil {
		t.Fatal(err)
	}
	got, readErr := io.ReadAll(r)
	closeErr := r.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(got, canary) {
		t.Fatalf("ordinary Read changed immutable bytes: equal=%t read=%v close=%v", bytes.Equal(got, canary), readErr, closeErr)
	}
}

func TestAuthorityMutableCASHeaderDecoderHasFixedReadBound(t *testing.T) {
	oversized := authorityCASBodyPrefix + strings.Repeat("a", 1<<20)
	source := strings.NewReader(oversized)
	decoded := unwrapAuthorityCASBody(io.NopCloser(source))
	consumed := len(oversized) - source.Len()
	if consumed > authorityCASHeaderSize {
		t.Fatalf("mutable CAS decoder consumed %d bytes before returning, header bound is %d", consumed, authorityCASHeaderSize)
	}
	got, err := io.ReadAll(decoded)
	closeErr := decoded.Close()
	if err != nil || closeErr != nil || !bytes.Equal(got, []byte(oversized)) {
		t.Fatalf("malformed header bytes were not preserved: equal=%t err=%v close=%v", bytes.Equal(got, []byte(oversized)), err, closeErr)
	}
}

func TestAuthorityEvidenceETagDoesNotImplyExactVersionDelete(t *testing.T) {
	ctx := context.Background()
	base := objstore.NewInMemBucket()
	t.Cleanup(func() { _ = base.Close() })
	bucket := &etagAuthorityBucket{Bucket: base}
	store, err := NewAuthorityEvidenceStore(bucket, "authority", "root", "source")
	if err != nil {
		t.Fatal(err)
	}
	version, err := store.CreateImmutable(ctx, "immutable.json", []byte(`{"value":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteVersion(ctx, "immutable.json", *version); err == nil {
		t.Fatalf("ETag-only provider unexpectedly claimed exact-version deletion: %v", err)
	}
	key, err := store.objectKey("immutable.json")
	if err != nil {
		t.Fatal(err)
	}
	if exists, err := base.Exists(ctx, key); err != nil || !exists {
		t.Fatalf("failed exact-version delete removed object: exists=%v err=%v", exists, err)
	}
}
