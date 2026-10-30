package objstore

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/thanos-io/objstore"
)

// ManagedNamespacePrefixes uses full immutable identities, never aliases or
// hashes truncated for presentation. The provider receives FinalPrefix keys.
func ManagedNamespacePrefixes(root, uid string, generation int64, assignment, cluster string) (configPrefix, finalPrefix string, err error) {
	if !validNamespacePath(root, true) || !validNamespacePath(cluster, false) || strings.Contains(cluster, "/") || uid == "" || assignment == "" || generation < 0 || len(uid) > 256 || len(assignment) > 256 || !utf8.ValidString(uid) || !utf8.ValidString(assignment) || strings.TrimSpace(uid) != uid || strings.TrimSpace(assignment) != assignment || strings.ContainsRune(uid, 0) || strings.ContainsRune(assignment, 0) {
		return "", "", fmt.Errorf("invalid immutable managed namespace")
	}
	// A later profile cannot choose an earlier managed namespace as its root.
	// Reserving this path component makes derived namespaces non-nesting even
	// when installation roots differ, without a global registry or lock.
	for _, component := range strings.Split(root, "/") {
		if component == "clusters" {
			return "", "", fmt.Errorf("installation root contains reserved managed namespace component")
		}
	}
	configPrefix = path.Join(root, "clusters", hex.EncodeToString([]byte(uid)), "generations", strconv.FormatInt(generation, 10), "assignments", hex.EncodeToString([]byte(assignment)))
	return configPrefix, configPrefix + "/" + cluster, nil
}

func validNamespacePath(name string, allowEmpty bool) bool {
	if name == "" {
		return allowEmpty
	}
	if strings.ContainsAny(name, "\\%\x00") || strings.TrimSpace(name) != name || strings.HasPrefix(name, "/") || strings.HasSuffix(name, "/") {
		return false
	}
	for _, part := range strings.Split(name, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}

type namespaceBucket struct {
	objstore.Bucket
	prefix   string
	readOnly bool
}

func BindReadOnlyNamespace(bucket objstore.Bucket, finalPrefix string) (objstore.Bucket, error) {
	b, err := BindNamespace(bucket, finalPrefix)
	if err != nil {
		return nil, err
	}
	b.(*namespaceBucket).readOnly = true
	return b, nil
}

// BindNamespace constrains keys without rewriting or cleaning them. The fixed
// wrapper is bound once; SDK retries retain their original bucket and key.
// This is destination binding, not a bucket ACL or atomic prefix lock. Manual
// legacy writers configured for successor keys and storage-admin injection are
// outside this managed-writer boundary; ordinary old managed retries are not.
func BindNamespace(bucket objstore.Bucket, finalPrefix string) (objstore.Bucket, error) {
	if bucket == nil || !validNamespacePath(finalPrefix, false) {
		return nil, fmt.Errorf("invalid fixed bucket namespace")
	}
	return &namespaceBucket{Bucket: bucket, prefix: finalPrefix}, nil
}

func (b *namespaceBucket) check(name string, listing bool) error {
	if listing {
		name = strings.TrimSuffix(name, "/")
	}
	if !validNamespacePath(name, false) || (name != b.prefix && !strings.HasPrefix(name, b.prefix+"/")) || !listing && name == b.prefix {
		return fmt.Errorf("object key is outside fixed managed namespace")
	}
	return nil
}

func (b *namespaceBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	if b.readOnly {
		return fmt.Errorf("managed source is read-only")
	}
	if err := b.check(name, false); err != nil {
		return err
	}
	return b.Bucket.Upload(ctx, name, r, opts...)
}
func (b *namespaceBucket) Delete(ctx context.Context, name string) error {
	if b.readOnly {
		return fmt.Errorf("managed source is read-only")
	}
	if err := b.check(name, false); err != nil {
		return err
	}
	return b.Bucket.Delete(ctx, name)
}
func (b *namespaceBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if err := b.check(name, false); err != nil {
		return nil, err
	}
	return b.Bucket.Get(ctx, name)
}
func (b *namespaceBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if err := b.check(name, false); err != nil {
		return nil, err
	}
	return b.Bucket.GetRange(ctx, name, off, length)
}
func (b *namespaceBucket) Exists(ctx context.Context, name string) (bool, error) {
	if err := b.check(name, false); err != nil {
		return false, err
	}
	return b.Bucket.Exists(ctx, name)
}
func (b *namespaceBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	if err := b.check(name, false); err != nil {
		return objstore.ObjectAttributes{}, err
	}
	return b.Bucket.Attributes(ctx, name)
}
func (b *namespaceBucket) Iter(ctx context.Context, dir string, f func(string) error, opts ...objstore.IterOption) error {
	if err := b.check(dir, true); err != nil {
		return err
	}
	if dir == b.prefix {
		dir += "/"
	}
	return b.Bucket.Iter(ctx, dir, func(name string) error {
		if err := b.check(name, true); err != nil {
			return err
		}
		return f(name)
	}, opts...)
}
func (b *namespaceBucket) IterWithAttributes(ctx context.Context, dir string, f func(objstore.IterObjectAttributes) error, opts ...objstore.IterOption) error {
	if err := b.check(dir, true); err != nil {
		return err
	}
	if dir == b.prefix {
		dir += "/"
	}
	return b.Bucket.IterWithAttributes(ctx, dir, func(a objstore.IterObjectAttributes) error {
		if err := b.check(a.Name, true); err != nil {
			return err
		}
		return f(a)
	}, opts...)
}

func NewBucketForNamespace(ctx context.Context, cfg Config, finalPrefix string) (*MeteredBucket, error) {
	if !validNamespacePath(finalPrefix, false) {
		return nil, fmt.Errorf("invalid fixed bucket namespace")
	}
	if cfg.Provider == "" {
		cfg.Provider = ProviderS3
	}
	b, err := NewBucketWithoutProvisioning(ctx, cfg)
	if err != nil {
		return nil, err
	}
	b.Bucket, err = BindNamespace(b.Bucket, finalPrefix)
	if err != nil {
		_ = b.Close()
		return nil, err
	}
	return b, nil
}
