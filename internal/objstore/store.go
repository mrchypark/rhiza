package objstore

import (
	"context"
	"io"

	"github.com/thanos-io/objstore"
)

// Store wraps a thanos-io/objstore bucket with rhiza-specific paths.
type Store struct {
	bucket  objstore.Bucket
	prefix  string
}

// New creates a new object store wrapper.
func New(bucket objstore.Bucket, prefix string) *Store {
	return &Store{
		bucket: bucket,
		prefix: prefix,
	}
}

// Upload uploads data to the given path.
func (s *Store) Upload(ctx context.Context, path string, r io.Reader) error {
	return s.bucket.Upload(ctx, s.key(path), r)
}

// Get retrieves data from the given path.
func (s *Store) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	return s.bucket.Get(ctx, s.key(path))
}

// Exists checks if an object exists.
func (s *Store) Exists(ctx context.Context, path string) (bool, error) {
	return s.bucket.Exists(ctx, s.key(path))
}

// Delete removes an object.
func (s *Store) Delete(ctx context.Context, path string) error {
	return s.bucket.Delete(ctx, s.key(path))
}

// Iter iterates over objects with the given prefix.
func (s *Store) Iter(ctx context.Context, prefix string, f func(name string) error) error {
	return s.bucket.Iter(ctx, s.key(prefix), f)
}

// Close closes the underlying bucket.
func (s *Store) Close() error {
	return s.bucket.Close()
}

// key prepends the store prefix to the path.
func (s *Store) key(path string) string {
	if s.prefix == "" {
		return path
	}
	return s.prefix + "/" + path
}
