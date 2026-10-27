package objstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/thanos-io/objstore"
)

// LocalFilesystemOwner serializes all Local archives rooted in one filesystem
// directory. The root-wide scope intentionally avoids prefix-alias ambiguity.
type LocalFilesystemOwner struct {
	mu      sync.Mutex
	changed chan struct{}
	active  int
	closing bool
	poison  bool
	root    *os.File
	anchor  *os.File
	rootDir string
}

// AcquireLocalFilesystemOwner requires a dedicated filesystem root outside
// DataDir. Its advisory locks survive DataDir replacement and process exit
// releases them. Supported filesystem types are deliberately enumerated.
func AcquireLocalFilesystemOwner(rootDir, dataDir string) (*LocalFilesystemOwner, error) {
	root, err := filepath.Abs(rootDir)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return nil, err
	}
	data, err := filepath.EvalSymlinks(dataDir)
	if err != nil {
		return nil, err
	}
	if pathContains(root, data) || pathContains(data, root) {
		return nil, fmt.Errorf("Local archive root and DataDir must be disjoint")
	}
	owner, err := acquireLocalFilesystemOwner(root)
	if err != nil {
		return nil, err
	}
	owner.rootDir = root
	return owner, nil
}

func pathContains(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	return err == nil && (rel == "." || rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

func (o *LocalFilesystemOwner) enter() (func(), error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closing {
		return nil, fmt.Errorf("Local filesystem owner is closing")
	}
	o.active++
	return func() {
		o.mu.Lock()
		o.active--
		if o.changed != nil {
			close(o.changed)
			o.changed = nil
		}
		o.mu.Unlock()
	}, nil
}

// Wrap counts actual bucket calls, not caller waits. A timed-out LocalBucket
// goroutine therefore keeps the owner busy until the filesystem call returns.
func (o *LocalFilesystemOwner) Wrap(bucket objstore.Bucket) objstore.Bucket {
	return &localOwnerBucket{Bucket: bucket, owner: o}
}

// LocationHash binds v2 archive metadata to this canonical root and namespace.
func (o *LocalFilesystemOwner) LocationHash(namespace string) string {
	hash := sha256.Sum256([]byte("rhiza/local-filesystem-archive/v2\x00" + o.rootDir + "\x00" + filepath.ToSlash(filepath.Clean(namespace))))
	return hex.EncodeToString(hash[:])
}

func (o *LocalFilesystemOwner) RootDir() string { return o.rootDir }

// Held reports whether this owner still holds an unpoisoned lifetime lock.
func (o *LocalFilesystemOwner) Held() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return !o.closing && !o.poison && o.root != nil && o.anchor != nil
}

// Poison prevents a graceful close from releasing ownership after an
// unresolved mutation. The descriptor is retained until process exit.
func (o *LocalFilesystemOwner) Poison() {
	o.mu.Lock()
	o.poison = true
	o.mu.Unlock()
	retainPoisonedLocalOwner(o)
}

// Close stops admission and releases the filesystem lock only after all
// entered calls/readers drain and no operation has poisoned ownership.
func (o *LocalFilesystemOwner) Close(timeout time.Duration) error {
	o.mu.Lock()
	o.closing = true
	deadline := time.Now().Add(timeout)
	for o.active != 0 {
		if o.changed == nil {
			o.changed = make(chan struct{})
		}
		changed := o.changed
		remaining := time.Until(deadline)
		if remaining <= 0 {
			o.poison = true
			retainPoisonedLocalOwner(o)
			o.mu.Unlock()
			return fmt.Errorf("Local filesystem operations did not drain")
		}
		o.mu.Unlock()
		timer := time.NewTimer(remaining)
		select {
		case <-changed:
		case <-timer.C:
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		o.mu.Lock()
	}
	if o.poison {
		retainPoisonedLocalOwner(o)
		o.mu.Unlock()
		return fmt.Errorf("Local filesystem ownership remains poisoned until process exit")
	}
	var err error
	if o.anchor != nil {
		err = errorsJoin(err, unlockClose(o.anchor))
		o.anchor = nil
	}
	// Keep the root-directory inode lock until last. An alias may have a
	// different path-derived anchor, so dropping root first would let it pass
	// the root lock while this owner's anchor is still the only fence.
	if o.root != nil {
		err = errorsJoin(err, unlockClose(o.root))
		o.root = nil
	}
	o.mu.Unlock()
	return err
}

func errorsJoin(a, b error) error {
	return errors.Join(a, b)
}

type localOwnerBucket struct {
	objstore.Bucket
	owner *LocalFilesystemOwner
}

var poisonedLocalOwners = struct {
	sync.Mutex
	owners map[*LocalFilesystemOwner]struct{}
}{owners: make(map[*LocalFilesystemOwner]struct{})}

func retainPoisonedLocalOwner(owner *LocalFilesystemOwner) {
	poisonedLocalOwners.Lock()
	poisonedLocalOwners.owners[owner] = struct{}{}
	poisonedLocalOwners.Unlock()
}

func ownerAnchorPath(root string) string {
	hash := sha256.Sum256([]byte(root))
	return filepath.Join(filepath.Dir(root), ".rhiza-local-owner-"+hex.EncodeToString(hash[:8]))
}

func (b *localOwnerBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	done, err := b.owner.enter()
	if err != nil {
		return err
	}
	defer done()
	err = b.Bucket.Upload(ctx, name, r, opts...)
	if err != nil && !b.Bucket.IsConditionNotMetErr(err) {
		b.owner.Poison()
	}
	return err
}
func (b *localOwnerBucket) Delete(ctx context.Context, name string) error {
	done, err := b.owner.enter()
	if err != nil {
		return err
	}
	defer done()
	err = b.Bucket.Delete(ctx, name)
	if err != nil && !b.Bucket.IsObjNotFoundErr(err) {
		b.owner.Poison()
	}
	return err
}
func (b *localOwnerBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	done, err := b.owner.enter()
	if err != nil {
		return objstore.ObjectAttributes{}, err
	}
	defer done()
	return b.Bucket.Attributes(ctx, name)
}
func (b *localOwnerBucket) Exists(ctx context.Context, name string) (bool, error) {
	done, err := b.owner.enter()
	if err != nil {
		return false, err
	}
	defer done()
	return b.Bucket.Exists(ctx, name)
}
func (b *localOwnerBucket) Iter(ctx context.Context, dir string, f func(string) error, opts ...objstore.IterOption) error {
	done, err := b.owner.enter()
	if err != nil {
		return err
	}
	defer done()
	return b.Bucket.Iter(ctx, dir, f, opts...)
}
func (b *localOwnerBucket) IterWithAttributes(ctx context.Context, dir string, f func(objstore.IterObjectAttributes) error, opts ...objstore.IterOption) error {
	done, err := b.owner.enter()
	if err != nil {
		return err
	}
	defer done()
	return b.Bucket.IterWithAttributes(ctx, dir, f, opts...)
}
func (b *localOwnerBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	done, err := b.owner.enter()
	if err != nil {
		return nil, err
	}
	r, err := b.Bucket.Get(ctx, name)
	if err != nil {
		done()
		return nil, err
	}
	return &localOwnerReader{ReadCloser: r, owner: b.owner, done: done}, nil
}
func (b *localOwnerBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	done, err := b.owner.enter()
	if err != nil {
		return nil, err
	}
	r, err := b.Bucket.GetRange(ctx, name, off, length)
	if err != nil {
		done()
		return nil, err
	}
	return &localOwnerReader{ReadCloser: r, owner: b.owner, done: done}, nil
}

type localOwnerReader struct {
	io.ReadCloser
	mu       sync.Mutex
	reads    sync.WaitGroup
	closed   bool
	once     sync.Once
	close    sync.Once
	closeErr error
	owner    *LocalFilesystemOwner
	done     func()
}

func (r *localOwnerReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return 0, os.ErrClosed
	}
	r.reads.Add(1)
	r.mu.Unlock()
	defer r.reads.Done()
	return r.ReadCloser.Read(p)
}
func (r *localOwnerReader) Close() error {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	r.close.Do(func() {
		r.closeErr = r.ReadCloser.Close()
		r.reads.Wait()
		if r.closeErr != nil {
			r.owner.Poison()
		}
		r.once.Do(r.done)
	})
	return r.closeErr
}
