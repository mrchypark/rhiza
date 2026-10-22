package objstore

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	thanosobjstore "github.com/thanos-io/objstore"
	"google.golang.org/api/option"
)

func TestDeleteSelectedVersionPreservesLegacyBackendDelete(t *testing.T) {
	bucket := thanosobjstore.NewInMemBucket()
	if err := bucket.Upload(context.Background(), "object", strings.NewReader("value")); err != nil {
		t.Fatal(err)
	}
	if err := DeleteSelectedVersion(context.Background(), bucket, "object", nil); err != nil {
		t.Fatalf("legacy delete: %v", err)
	}
	if exists, err := bucket.Exists(context.Background(), "object"); err != nil || exists {
		t.Fatalf("legacy object exists=%t err=%v", exists, err)
	}
}

func TestDeleteSelectedVersionFailsClosedForAdvertisedCapability(t *testing.T) {
	bucket := newVersionedDeleteTestBucket()
	err := DeleteSelectedVersion(context.Background(), bucket, "object", nil)
	if !errors.Is(err, ErrDeleteVersionRequired) {
		t.Fatalf("missing selected version error=%v", err)
	}
	if bucket.deletes != 0 {
		t.Fatalf("missing version reached delete %d times", bucket.deletes)
	}
}

func TestMeteredDeleteVersionNeverFallsBackWhenUnsupported(t *testing.T) {
	ctx := context.Background()
	base := thanosobjstore.NewInMemBucket()
	if err := base.Upload(ctx, "object", strings.NewReader("keep")); err != nil {
		t.Fatal(err)
	}
	bucket := newMeteredBucket(base, &bucketMetrics{})
	if err := bucket.DeleteVersion(ctx, "object", thanosobjstore.ObjectVersion{Type: thanosobjstore.Generation, Value: "1"}); !errors.Is(err, ErrVersionedDeleteUnsupported) {
		t.Fatalf("unsupported exact delete error=%v", err)
	}
	if exists, err := base.Exists(ctx, "object"); err != nil || !exists {
		t.Fatalf("unsupported exact delete changed object: exists=%t err=%v", exists, err)
	}
}

func TestGCSDeleteVersionPinsGenerationAndPrecondition(t *testing.T) {
	var gotMethod string
	var gotQuery url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotQuery = r.Method, r.URL.Query()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client, err := storage.NewClient(context.Background(), option.WithEndpoint(server.URL+"/storage/v1/"), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	bucket := &gcsVersionedDeleteBucket{Bucket: thanosobjstore.NewInMemBucket(), handle: client.Bucket("bucket")}
	if err := bucket.DeleteVersion(context.Background(), "prefix/object", thanosobjstore.ObjectVersion{Type: thanosobjstore.Generation, Value: "41"}); err != nil {
		t.Fatal(err)
	}
	if gotMethod != http.MethodDelete || gotQuery.Get("generation") != "41" || gotQuery.Get("ifGenerationMatch") != "41" {
		t.Fatalf("GCS delete method=%q query=%v", gotMethod, gotQuery)
	}
}

func TestGCSDeleteVersionRejectsMissingOrNonGenerationVersionBeforeRequest(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client, err := storage.NewClient(context.Background(), option.WithEndpoint(server.URL+"/storage/v1/"), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	bucket := &gcsVersionedDeleteBucket{Bucket: thanosobjstore.NewInMemBucket(), handle: client.Bucket("bucket")}
	for _, version := range []thanosobjstore.ObjectVersion{
		{},
		{Type: thanosobjstore.ETag, Value: "etag"},
		{Type: thanosobjstore.Generation, Value: "0"},
		{Type: thanosobjstore.Generation, Value: strconv.FormatInt(-1, 10)},
		{Type: thanosobjstore.Generation, Value: "+1"},
		{Type: thanosobjstore.Generation, Value: "01"},
		{Type: thanosobjstore.Generation, Value: "not-a-generation"},
	} {
		if err := bucket.DeleteVersion(context.Background(), "object", version); err == nil {
			t.Fatalf("accepted invalid version %+v", version)
		}
	}
	if requests != 0 {
		t.Fatalf("invalid generation issued %d GCS requests", requests)
	}
}

type versionedDeleteTestBucket struct {
	*thanosobjstore.InMemBucket
	deletes int
}

func newVersionedDeleteTestBucket() *versionedDeleteTestBucket {
	return &versionedDeleteTestBucket{InMemBucket: thanosobjstore.NewInMemBucket()}
}

func (*versionedDeleteTestBucket) SupportsVersionedDelete() bool { return true }

func (b *versionedDeleteTestBucket) DeleteVersion(ctx context.Context, key string, version thanosobjstore.ObjectVersion) error {
	b.deletes++
	return b.InMemBucket.Delete(ctx, key)
}
