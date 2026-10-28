package objstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	thanosobjstore "github.com/thanos-io/objstore"
)

type immutableTestBucket struct {
	thanosobjstore.Bucket
	uploads   int
	deletes   int
	uploadErr error
	supported []thanosobjstore.ObjectUploadOptionType
}

func (b *immutableTestBucket) Upload(ctx context.Context, name string, reader io.Reader, opts ...thanosobjstore.ObjectUploadOption) error {
	b.uploads++
	if b.uploadErr != nil {
		return b.uploadErr
	}
	return b.Bucket.Upload(ctx, name, reader, opts...)
}

func (b *immutableTestBucket) Delete(ctx context.Context, name string) error {
	b.deletes++
	return b.Bucket.Delete(ctx, name)
}

func (b *immutableTestBucket) SupportedObjectUploadOptions() []thanosobjstore.ObjectUploadOptionType {
	if b.supported != nil {
		return b.supported
	}
	return b.Bucket.SupportedObjectUploadOptions()
}

type immutableInstrumentedTestBucket struct {
	*immutableTestBucket
	withExpectedErrs int
	readerWithErrors int
}

func (b *immutableInstrumentedTestBucket) WithExpectedErrs(thanosobjstore.IsOpFailureExpectedFunc) thanosobjstore.Bucket {
	b.withExpectedErrs++
	return b
}

func (b *immutableInstrumentedTestBucket) ReaderWithExpectedErrs(thanosobjstore.IsOpFailureExpectedFunc) thanosobjstore.BucketReader {
	b.readerWithErrors++
	return b.immutableTestBucket
}

func TestNewImmutableBucketRequiresAtomicCreateOnlyCapability(t *testing.T) {
	if _, err := NewImmutableBucket(nil); err == nil {
		t.Fatal("nil bucket accepted")
	}
	var typedNil *immutableTestBucket
	if _, err := NewImmutableBucket(typedNil); err == nil {
		t.Fatal("typed-nil bucket accepted")
	}
	if _, err := NewImmutableBucket(thanosobjstore.NewInMemBucket()); err != nil {
		t.Fatalf("valid bucket neighbor rejected: %v", err)
	}

	unsupported := &immutableTestBucket{
		Bucket:    thanosobjstore.NewInMemBucket(),
		supported: []thanosobjstore.ObjectUploadOptionType{thanosobjstore.IfMatch},
	}
	if _, err := NewImmutableBucket(unsupported); err == nil || !strings.Contains(err.Error(), "IfNotExists") {
		t.Fatalf("unsupported bucket error = %v, want IfNotExists capability error", err)
	}
	if unsupported.uploads != 0 || unsupported.deletes != 0 {
		t.Fatalf("constructor dispatched operations: uploads=%d deletes=%d", unsupported.uploads, unsupported.deletes)
	}
}

func TestImmutableBucketAllowsOnlyCreateOnlyUpload(t *testing.T) {
	ctx := context.Background()
	backend := &immutableTestBucket{Bucket: thanosobjstore.NewInMemBucket()}
	guard, err := NewImmutableBucket(backend)
	if err != nil {
		t.Fatal(err)
	}

	if err := guard.Upload(ctx, "object", strings.NewReader("first"), thanosobjstore.WithIfNotExists()); err != nil {
		t.Fatalf("create-only upload: %v", err)
	}
	if backend.uploads != 1 {
		t.Fatalf("backend uploads = %d, want 1", backend.uploads)
	}
	if !bytes.Equal(readImmutableObject(t, guard, "object"), []byte("first")) {
		t.Fatal("create-only upload content mismatch")
	}

	if err := guard.Upload(ctx, "object", strings.NewReader("replacement"), thanosobjstore.WithIfNotExists()); !backend.IsConditionNotMetErr(err) {
		t.Fatalf("duplicate create-only upload error = %v, want provider condition failure", err)
	}
	if got := string(readImmutableObject(t, guard, "object")); got != "first" {
		t.Fatalf("duplicate upload changed object to %q", got)
	}
}

func TestImmutableBucketRejectsUnconditionalAndConditionalOverwriteBeforeDispatch(t *testing.T) {
	backend := &immutableTestBucket{Bucket: thanosobjstore.NewInMemBucket()}
	guard, err := NewImmutableBucket(backend)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		opts []thanosobjstore.ObjectUploadOption
	}{
		{name: "unconditional"},
		{name: "if-match", opts: []thanosobjstore.ObjectUploadOption{thanosobjstore.WithIfMatch(&thanosobjstore.ObjectVersion{Type: thanosobjstore.ETag, Value: "v1"})}},
		{name: "if-not-match", opts: []thanosobjstore.ObjectUploadOption{thanosobjstore.WithIfNotMatch(&thanosobjstore.ObjectVersion{Type: thanosobjstore.ETag, Value: "v1"})}},
		{name: "create-only-and-if-match", opts: []thanosobjstore.ObjectUploadOption{thanosobjstore.WithIfNotExists(), thanosobjstore.WithIfMatch(&thanosobjstore.ObjectVersion{Type: thanosobjstore.ETag, Value: "v1"})}},
		{name: "create-only-and-if-not-match", opts: []thanosobjstore.ObjectUploadOption{thanosobjstore.WithIfNotExists(), thanosobjstore.WithIfNotMatch(&thanosobjstore.ObjectVersion{Type: thanosobjstore.ETag, Value: "v1"})}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := guard.Upload(context.Background(), "object", strings.NewReader("value"), test.opts...)
			if !errors.Is(err, ErrImmutableMutation) {
				t.Fatalf("upload error = %v, want ErrImmutableMutation", err)
			}
			if backend.uploads != 0 {
				t.Fatalf("backend upload calls = %d, want 0", backend.uploads)
			}
		})
	}
}

func TestImmutableBucketRejectsDeleteBeforeDispatch(t *testing.T) {
	backend := &immutableTestBucket{Bucket: thanosobjstore.NewInMemBucket()}
	guard, err := NewImmutableBucket(backend)
	if err != nil {
		t.Fatal(err)
	}

	if err := guard.Delete(context.Background(), "object"); !errors.Is(err, ErrImmutableMutation) {
		t.Fatalf("delete error = %v, want ErrImmutableMutation", err)
	}
	if backend.deletes != 0 {
		t.Fatalf("backend delete calls = %d, want 0", backend.deletes)
	}
}

func TestImmutableBucketPreservesBackendUploadOutcome(t *testing.T) {
	unknown := errors.New("response lost after possible commit")
	backend := &immutableTestBucket{Bucket: thanosobjstore.NewInMemBucket(), uploadErr: unknown}
	guard, err := NewImmutableBucket(backend)
	if err != nil {
		t.Fatal(err)
	}

	err = guard.Upload(context.Background(), "object", strings.NewReader("value"), thanosobjstore.WithIfNotExists())
	if err != unknown {
		t.Fatalf("upload error = %v, want exact backend error %v", err, unknown)
	}
	if backend.uploads != 1 {
		t.Fatalf("backend upload calls = %d, want 1", backend.uploads)
	}
}

func TestImmutableBucketDelegatesReadOperations(t *testing.T) {
	ctx := context.Background()
	backend := &immutableTestBucket{Bucket: thanosobjstore.NewInMemBucket()}
	guard, err := NewImmutableBucket(backend)
	if err != nil {
		t.Fatal(err)
	}
	if err := guard.Upload(ctx, "dir/object", strings.NewReader("readable"), thanosobjstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}

	if guard.Name() != backend.Name() || guard.Provider() != backend.Provider() {
		t.Fatalf("bucket identity changed: name=%q provider=%q", guard.Name(), guard.Provider())
	}
	probe := errors.New("classification probe")
	if guard.IsObjNotFoundErr(probe) != backend.IsObjNotFoundErr(probe) {
		t.Fatal("object-not-found classification differs from backend")
	}
	if guard.IsConditionNotMetErr(probe) != backend.IsConditionNotMetErr(probe) {
		t.Fatal("condition-failure classification differs from backend")
	}
	if guard.IsAccessDeniedErr(probe) != backend.IsAccessDeniedErr(probe) {
		t.Fatal("access-denied classification differs from backend")
	}
	if got := string(readImmutableObject(t, guard, "dir/object")); got != "readable" {
		t.Fatalf("Get returned %q", got)
	}
	ranged, err := guard.GetRange(ctx, "dir/object", 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	rangeBytes, err := io.ReadAll(ranged)
	closeImmutableReader(t, ranged)
	if err != nil || string(rangeBytes) != "ead" {
		t.Fatalf("GetRange = %q, %v", rangeBytes, err)
	}
	if exists, err := guard.Exists(ctx, "dir/object"); err != nil || !exists {
		t.Fatalf("Exists = %v, %v", exists, err)
	}
	if attrs, err := guard.Attributes(ctx, "dir/object"); err != nil || attrs.Size != int64(len("readable")) {
		t.Fatalf("Attributes = %+v, %v", attrs, err)
	}
	var names []string
	if err := guard.Iter(ctx, "dir", func(name string) error { names = append(names, name); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || names[0] != "dir/object" {
		t.Fatalf("Iter names = %v", names)
	}
	if len(guard.SupportedObjectUploadOptions()) == 0 {
		t.Fatal("upload capabilities were not delegated")
	}
}

func TestImmutableInstrumentedBucketPreservesGuardWhenFiltering(t *testing.T) {
	backend := &immutableInstrumentedTestBucket{immutableTestBucket: &immutableTestBucket{Bucket: thanosobjstore.NewInMemBucket()}}
	guard, err := NewImmutableBucket(backend)
	if err != nil {
		t.Fatal(err)
	}
	instrumented, ok := guard.(thanosobjstore.InstrumentedBucket)
	if !ok {
		t.Fatal("instrumented capability was not preserved")
	}

	filtered := instrumented.WithExpectedErrs(nil)
	if _, ok := filtered.(thanosobjstore.InstrumentedBucket); !ok {
		t.Fatal("filtered bucket lost instrumentation capability")
	}
	if err := filtered.Upload(context.Background(), "object", strings.NewReader("value")); !errors.Is(err, ErrImmutableMutation) {
		t.Fatalf("filtered bucket upload error = %v, want ErrImmutableMutation", err)
	}
	if backend.uploads != 0 || backend.withExpectedErrs != 1 {
		t.Fatalf("filtered bucket calls: uploads=%d WithExpectedErrs=%d", backend.uploads, backend.withExpectedErrs)
	}
	if _, ok := instrumented.ReaderWithExpectedErrs(nil).(thanosobjstore.BucketReader); !ok {
		t.Fatal("reader instrumentation returned a non-reader")
	}
	if backend.readerWithErrors != 1 {
		t.Fatalf("ReaderWithExpectedErrs calls = %d, want 1", backend.readerWithErrors)
	}
}

func readImmutableObject(t *testing.T, bucket thanosobjstore.Bucket, name string) []byte {
	t.Helper()
	reader, err := bucket.Get(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(reader)
	closeImmutableReader(t, reader)
	if readErr != nil {
		t.Fatal(readErr)
	}
	return data
}

func closeImmutableReader(t *testing.T, reader io.Closer) {
	t.Helper()
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
}
