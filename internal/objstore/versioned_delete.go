package objstore

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/storage"
	thanosobjstore "github.com/thanos-io/objstore"
)

var (
	ErrVersionedDeleteUnsupported = errors.New("object store does not support versioned deletes")
	ErrDeleteVersionRequired      = errors.New("object version is required for versioned delete")
	ErrBucketAttrsUnsupported     = errors.New("bucket does not expose provider bucket attributes")
)

// BucketAttrsReader is an optional capability for reading provider-level
// bucket metadata without creating a second client or changing credentials.
type BucketAttrsReader interface {
	BucketAttrs(context.Context) (*storage.BucketAttrs, error)
}

func (b *gcsVersionedDeleteBucket) BucketAttrs(ctx context.Context) (*storage.BucketAttrs, error) {
	if b == nil || b.handle == nil {
		return nil, ErrBucketAttrsUnsupported
	}
	return b.handle.Attrs(ctx)
}

// VersionedDeleteBucket is an optional capability for deleting only the
// selected object version. Implementations must not fall back to Delete.
type VersionedDeleteBucket interface {
	DeleteVersion(ctx context.Context, key string, version thanosobjstore.ObjectVersion) error
}

type versionedDeleteCapability interface {
	SupportsVersionedDelete() bool
}

// SupportsVersionedDelete reports whether bucket can delete an exact selected
// version. Wrappers may implement the capability method to expose or restrict
// the capability of their underlying provider.
func SupportsVersionedDelete(bucket thanosobjstore.Bucket) bool {
	if bucket == nil {
		return false
	}
	if capability, ok := bucket.(versionedDeleteCapability); ok {
		return capability.SupportsVersionedDelete()
	}
	_, ok := bucket.(VersionedDeleteBucket)
	return ok
}

// DeleteSelectedVersion uses an exact selected version when supported and
// preserves legacy Delete behavior for providers without that capability. A
// bucket that advertises versioned deletion fails closed if the selected
// version is absent.
func DeleteSelectedVersion(ctx context.Context, bucket thanosobjstore.Bucket, key string, version *thanosobjstore.ObjectVersion) error {
	if !SupportsVersionedDelete(bucket) {
		return bucket.Delete(ctx, key)
	}
	if version == nil || version.Value == "" {
		return ErrDeleteVersionRequired
	}
	deleter, ok := bucket.(VersionedDeleteBucket)
	if !ok {
		return fmt.Errorf("%w: %T", ErrVersionedDeleteUnsupported, bucket)
	}
	return deleter.DeleteVersion(ctx, key, *version)
}

// DeleteVersion dispatches an exact version delete without allowing callers to
// accidentally substitute an unconditional delete when a capability is absent.
func DeleteVersion(ctx context.Context, bucket thanosobjstore.Bucket, key string, version thanosobjstore.ObjectVersion) error {
	if version.Value == "" {
		return ErrDeleteVersionRequired
	}
	deleter, ok := bucket.(VersionedDeleteBucket)
	if !ok || !SupportsVersionedDelete(bucket) {
		return fmt.Errorf("%w: %T", ErrVersionedDeleteUnsupported, bucket)
	}
	return deleter.DeleteVersion(ctx, key, version)
}
