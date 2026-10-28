package objstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"

	thanosobjstore "github.com/thanos-io/objstore"
)

// ErrImmutableMutation is returned when an operation could mutate or remove
// an existing object in an immutable bucket.
var ErrImmutableMutation = errors.New("immutable bucket rejects mutation")

// NewImmutableBucket wraps bucket with a create-only mutation policy. Uploads
// must use the provider's atomic IfNotExists condition, and deletes are always
// rejected before reaching the provider. Upload errors from the provider are
// returned unchanged because their outcomes may be unknown.
func NewImmutableBucket(bucket thanosobjstore.Bucket) (thanosobjstore.Bucket, error) {
	if bucket == nil || nilObjectStoreBucket(bucket) {
		return nil, errors.New("immutable bucket requires a non-nil bucket")
	}
	for _, option := range bucket.SupportedObjectUploadOptions() {
		if option == thanosobjstore.IfNotExists {
			return wrapImmutableBucket(bucket), nil
		}
	}
	return nil, fmt.Errorf("immutable bucket requires provider support for IfNotExists")
}

func nilObjectStoreBucket(bucket thanosobjstore.Bucket) bool {
	value := reflect.ValueOf(bucket)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func wrapImmutableBucket(bucket thanosobjstore.Bucket) thanosobjstore.Bucket {
	guarded := &immutableBucket{Bucket: bucket}
	if instrumented, ok := bucket.(thanosobjstore.InstrumentedBucket); ok {
		return &immutableInstrumentedBucket{immutableBucket: guarded, instrumented: instrumented}
	}
	return guarded
}

type immutableBucket struct {
	thanosobjstore.Bucket
}

func (b *immutableBucket) Upload(ctx context.Context, name string, reader io.Reader, opts ...thanosobjstore.ObjectUploadOption) error {
	params := thanosobjstore.ApplyObjectUploadOptions(opts...)
	if !params.IfNotExists || params.Condition != nil || params.IfNotMatch {
		return fmt.Errorf("%w: upload must be create-only with IfNotExists", ErrImmutableMutation)
	}
	if err := thanosobjstore.ValidateUploadOptions(b.Bucket.SupportedObjectUploadOptions(), opts...); err != nil {
		return err
	}
	return b.Bucket.Upload(ctx, name, reader, opts...)
}

func (b *immutableBucket) Delete(context.Context, string) error {
	return ErrImmutableMutation
}

// immutableInstrumentedBucket retains objstore's optional instrumentation
// capability without exposing the underlying bucket through its return value.
type immutableInstrumentedBucket struct {
	*immutableBucket
	instrumented thanosobjstore.InstrumentedBucket
}

func (b *immutableInstrumentedBucket) WithExpectedErrs(expected thanosobjstore.IsOpFailureExpectedFunc) thanosobjstore.Bucket {
	return wrapImmutableBucket(b.instrumented.WithExpectedErrs(expected))
}

func (b *immutableInstrumentedBucket) ReaderWithExpectedErrs(expected thanosobjstore.IsOpFailureExpectedFunc) thanosobjstore.BucketReader {
	return newImmutableBucketReader(b.instrumented.ReaderWithExpectedErrs(expected))
}

// immutableBucketReader deliberately exposes only BucketReader. In particular,
// it must not embed or return the provider's dynamic Bucket value: callers can
// otherwise type-assert the reader back to Upload or Delete and bypass the
// immutable mutation boundary.
type immutableBucketReader struct {
	thanosobjstore.BucketReader
}

func newImmutableBucketReader(reader thanosobjstore.BucketReader) thanosobjstore.BucketReader {
	return &immutableBucketReader{BucketReader: reader}
}

// ReaderWithExpectedErrs preserves recursive reader filtering while keeping
// each filtered result opaque to Bucket and mutation-capability assertions.
func (b *immutableBucketReader) ReaderWithExpectedErrs(expected thanosobjstore.IsOpFailureExpectedFunc) thanosobjstore.BucketReader {
	if instrumented, ok := b.BucketReader.(thanosobjstore.InstrumentedBucketReader); ok {
		return newImmutableBucketReader(instrumented.ReaderWithExpectedErrs(expected))
	}
	return newImmutableBucketReader(b.BucketReader)
}
