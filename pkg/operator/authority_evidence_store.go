package operator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"

	internalobjstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/thanos-io/objstore"
)

// evidenceObjects is the small consistent-object-store surface used by
// authority evidence. Mutable values require exact-version CAS; attachments are
// immutable create-only objects.
type evidenceObjects interface {
	create(context.Context, string, io.Reader, int64) (*objstore.ObjectVersion, error)
	read(context.Context, string) (io.ReadCloser, *objstore.ObjectVersion, error)
	compareAndSwap(context.Context, string, *objstore.ObjectVersion, []byte) (*objstore.ObjectVersion, error)
	deleteVersion(context.Context, string, objstore.ObjectVersion) error
}

// AuthorityEvidenceStore provides immutable attachments and mutable
// version-CAS records under the same configured object-store prefix/root
// identity used by AuthorityStateStore.
type AuthorityEvidenceStore struct {
	objects  evidenceObjects
	prefix   string
	identity string
}

func NewAuthorityEvidenceStore(bucket objstore.Bucket, prefix, rootUID, sourceID string) (*AuthorityEvidenceStore, error) {
	if rootUID == "" || sourceID == "" || prefix == "." || strings.HasPrefix(prefix, "/") || (prefix != "" && path.Clean(prefix) != prefix) {
		return nil, fmt.Errorf("fencing: authority evidence requires object store and canonical root identity")
	}
	if err := validateAuthorityCASBucket(bucket); err != nil {
		return nil, err
	}
	identity := hashJSON([]string{"rhiza-authority-root-v1", rootUID, sourceID})
	return newAuthorityEvidenceStore(objstoreEvidenceObjects{bucket: bucket}, prefix, identity)
}

func newAuthorityEvidenceStore(objects evidenceObjects, prefix, identity string) (*AuthorityEvidenceStore, error) {
	if objects == nil || !validStageDigest(identity) || prefix == "." || strings.HasPrefix(prefix, "/") || (prefix != "" && path.Clean(prefix) != prefix) {
		return nil, fmt.Errorf("fencing: invalid authority evidence identity or prefix")
	}
	base := path.Join("rhiza-authority/v1/evidence", identity)
	if prefix != "" {
		base = path.Join(prefix, base)
	}
	return &AuthorityEvidenceStore{objects: objects, prefix: base, identity: identity}, nil
}

// Identity matches AuthorityStateStore's stable root identity; it contains no
// Pod name, local path, random process ID, or lease timestamp.
func (s *AuthorityEvidenceStore) Identity() string {
	if s == nil {
		return ""
	}
	return s.identity
}

func (s *AuthorityEvidenceStore) IsNotFound(err error) bool {
	objects, ok := s.objects.(interface{ isNotFound(error) bool })
	return ok && objects.isNotFound(err)
}

func (s *AuthorityEvidenceStore) objectKey(key string) (string, error) {
	if s == nil || key == "" || strings.HasPrefix(key, "/") || strings.Contains(key, "\\") || path.Clean(key) != key || key == "." || strings.HasPrefix(key, "../") {
		return "", fmt.Errorf("fencing: invalid authority evidence key")
	}
	return path.Join(s.prefix, key), nil
}

// CreateImmutableFrom conditionally creates an exact byte stream. On any
// uncertain result, callers verify the same deterministic key and digest.
func (s *AuthorityEvidenceStore) CreateImmutableFrom(ctx context.Context, key string, input io.Reader, size int64) error {
	if ctx == nil || input == nil || size < 0 {
		return fmt.Errorf("fencing: evidence context, reader, and size are required")
	}
	name, err := s.objectKey(key)
	if err != nil {
		return err
	}
	_, err = s.objects.create(ctx, name, io.LimitReader(input, size+1), size)
	return err
}

// CreateImmutable is for small canonical records and reconciles a lost create
// response only when an exact byte-for-byte read verifies the same object.
func (s *AuthorityEvidenceStore) CreateImmutable(ctx context.Context, key string, body []byte) (*objstore.ObjectVersion, error) {
	if ctx == nil || body == nil {
		return nil, fmt.Errorf("fencing: evidence context and body are required")
	}
	name, err := s.objectKey(key)
	if err != nil {
		return nil, err
	}
	version, err := s.objects.create(ctx, name, bytes.NewReader(body), int64(len(body)))
	if err == nil {
		return version, nil
	}
	current, gotVersion, readErr := s.objects.read(ctx, name)
	if readErr != nil {
		return nil, errors.Join(err, readErr)
	}
	defer current.Close()
	got, readErr := io.ReadAll(io.LimitReader(current, int64(len(body))+1))
	if readErr == nil && bytes.Equal(got, body) {
		return gotVersion, nil
	}
	return nil, errors.Join(err, readErr, fmt.Errorf("fencing: immutable evidence key contains different bytes"))
}

// Read returns the exact stored object bytes and version. It never interprets
// record framing, so immutable payloads remain byte-for-byte transparent.
func (s *AuthorityEvidenceStore) Read(ctx context.Context, key string) (io.ReadCloser, *objstore.ObjectVersion, error) {
	if ctx == nil {
		return nil, nil, fmt.Errorf("fencing: evidence context is required")
	}
	name, err := s.objectKey(key)
	if err != nil {
		return nil, nil, err
	}
	return s.objects.read(ctx, name)
}

// ReadMutable returns the logical bytes of a mutable CAS record. Unlike Read,
// it recognizes the fixed-size CAS revision header written by CompareAndSwap.
func (s *AuthorityEvidenceStore) ReadMutable(ctx context.Context, key string) (io.ReadCloser, *objstore.ObjectVersion, error) {
	if ctx == nil {
		return nil, nil, fmt.Errorf("fencing: evidence context is required")
	}
	name, err := s.objectKey(key)
	if err != nil {
		return nil, nil, err
	}
	r, version, err := s.objects.read(ctx, name)
	if err != nil {
		return nil, nil, err
	}
	return unwrapAuthorityCASBody(r), version, nil
}

// CompareAndSwap updates one canonical record only at its exact prior version.
// If the response is lost, the same desired bytes/version are read back.
func (s *AuthorityEvidenceStore) CompareAndSwap(ctx context.Context, key string, expected *objstore.ObjectVersion, body []byte) (*objstore.ObjectVersion, error) {
	if ctx == nil || !validAuthorityObjectVersion(expected) || body == nil {
		return nil, fmt.Errorf("fencing: evidence CAS requires exact generation or ETag and body")
	}
	name, err := s.objectKey(key)
	if err != nil {
		return nil, err
	}
	revision, err := newAuthorityRevision()
	if err != nil {
		return nil, err
	}
	casBody := wrapAuthorityCASBody(revision, body)
	version, err := s.objects.compareAndSwap(ctx, name, expected, casBody)
	if err == nil {
		if !validAuthorityObjectVersion(version) || version.Type != expected.Type {
			return nil, fmt.Errorf("fencing: authority evidence CAS returned an inconsistent object version")
		}
		return version, nil
	}
	current, gotVersion, readErr := s.objects.read(ctx, name)
	if readErr != nil {
		return nil, errors.Join(err, readErr)
	}
	defer current.Close()
	got, readErr := io.ReadAll(io.LimitReader(current, int64(len(casBody))+1))
	if readErr == nil && validAuthorityObjectVersion(gotVersion) && gotVersion.Type == expected.Type && bytes.Equal(got, casBody) {
		return gotVersion, nil
	}
	return nil, errors.Join(err, readErr)
}

// DeleteVersion removes only the exact version previously observed by the
// caller. Providers without exact-version deletion fail closed; ETag CAS
// support alone does not imply exact-version deletion.
func (s *AuthorityEvidenceStore) DeleteVersion(ctx context.Context, key string, version objstore.ObjectVersion) error {
	if ctx == nil || version.Type != objstore.Generation || version.Value == "" {
		return fmt.Errorf("fencing: evidence deletion requires an exact generation; ETag deletion is unsupported")
	}
	name, err := s.objectKey(key)
	if err != nil {
		return err
	}
	return s.objects.deleteVersion(ctx, name, version)
}

type objstoreEvidenceObjects struct{ bucket objstore.Bucket }

func (o objstoreEvidenceObjects) create(ctx context.Context, key string, input io.Reader, size int64) (*objstore.ObjectVersion, error) {
	if err := o.bucket.Upload(ctx, key, input, objstore.WithIfNotExists()); err != nil {
		return nil, err
	}
	return o.version(ctx, key)
}

func (o objstoreEvidenceObjects) read(ctx context.Context, key string) (io.ReadCloser, *objstore.ObjectVersion, error) {
	before, err := o.version(ctx, key)
	if err != nil {
		return nil, nil, err
	}
	r, err := o.bucket.Get(ctx, key)
	if err != nil {
		return nil, nil, err
	}
	after, err := o.version(ctx, key)
	if err != nil {
		_ = r.Close()
		return nil, nil, err
	}
	if *before != *after {
		_ = r.Close()
		return nil, nil, fmt.Errorf("fencing: evidence object changed during read")
	}
	return r, before, nil
}

func (o objstoreEvidenceObjects) compareAndSwap(ctx context.Context, key string, expected *objstore.ObjectVersion, body []byte) (*objstore.ObjectVersion, error) {
	if !validAuthorityObjectVersion(expected) {
		return nil, fmt.Errorf("fencing: authority evidence CAS requires an exact generation or ETag version")
	}
	current, err := o.version(ctx, key)
	if err != nil {
		return nil, err
	}
	if current.Type != expected.Type {
		return nil, fmt.Errorf("fencing: authority evidence CAS version type differs from current object")
	}
	version := *expected
	if err := o.bucket.Upload(ctx, key, bytes.NewReader(body), objstore.WithIfMatch(&version)); err != nil {
		return nil, err
	}
	return o.version(ctx, key)
}

func (o objstoreEvidenceObjects) deleteVersion(ctx context.Context, key string, version objstore.ObjectVersion) error {
	return internalobjstore.DeleteVersion(ctx, o.bucket, key, version)
}

func (o objstoreEvidenceObjects) version(ctx context.Context, key string) (*objstore.ObjectVersion, error) {
	attrs, err := o.bucket.Attributes(ctx, key)
	if err != nil {
		return nil, err
	}
	if !validAuthorityObjectVersion(attrs.Version) {
		return nil, fmt.Errorf("fencing: authority evidence requires an exact generation or ETag version")
	}
	version := *attrs.Version
	return &version, nil
}

const authorityCASBodyPrefix = "\x00rhiza-cas-v1 "
const authorityCASHeaderSize = len(authorityCASBodyPrefix) + 64 + 1

func wrapAuthorityCASBody(revision string, body []byte) []byte {
	wrapped := make([]byte, 0, len(authorityCASBodyPrefix)+len(revision)+1+len(body))
	wrapped = append(wrapped, authorityCASBodyPrefix...)
	wrapped = append(wrapped, revision...)
	wrapped = append(wrapped, '\n')
	wrapped = append(wrapped, body...)
	return wrapped
}

type authorityCASBodyReader struct {
	reader io.Reader
	closer io.Closer
}

func (r authorityCASBodyReader) Read(p []byte) (int, error) { return r.reader.Read(p) }
func (r authorityCASBodyReader) Close() error               { return r.closer.Close() }

// unwrapAuthorityCASBody hides the fresh revision marker from store callers.
// It keeps the body streaming so large immutable payload reads stay bounded.
func unwrapAuthorityCASBody(input io.ReadCloser) io.ReadCloser {
	header := make([]byte, authorityCASHeaderSize)
	prefixSize := len(authorityCASBodyPrefix)
	n, err := io.ReadFull(input, header[:prefixSize])
	if err != nil || string(header[:n]) != authorityCASBodyPrefix {
		return authorityCASBodyReader{reader: io.MultiReader(bytes.NewReader(header[:n]), input), closer: input}
	}
	n, err = io.ReadFull(input, header[prefixSize:])
	if err != nil {
		return authorityCASBodyReader{reader: io.MultiReader(bytes.NewReader(header[:prefixSize+n]), input), closer: input}
	}
	revision := string(header[prefixSize : authorityCASHeaderSize-1])
	if header[authorityCASHeaderSize-1] != '\n' || !validAuthorityRevision(revision) {
		return authorityCASBodyReader{reader: io.MultiReader(bytes.NewReader(header), input), closer: input}
	}
	return authorityCASBodyReader{reader: input, closer: input}
}

func (o objstoreEvidenceObjects) isNotFound(err error) bool { return o.bucket.IsObjNotFoundErr(err) }
func (o objstoreEvidenceObjects) isConflict(err error) bool {
	return o.bucket.IsConditionNotMetErr(err)
}
