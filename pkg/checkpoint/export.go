package checkpoint

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"

	"github.com/thanos-io/objstore"
)

// CopyRoot copies one verified immutable root and its blocks between namespaces.
// It does not copy CURRENT, claims, pins, or maintenance metadata.
func CopyRoot(ctx context.Context, bucket objstore.Bucket, sourcePrefix, targetPrefix string, root *Checkpoint) error {
	if root == nil {
		return fmt.Errorf("checkpoint root is required")
	}
	source := NewManager(bucket, sourcePrefix, "", root.ConfigID)
	opened, err := source.OpenRoot(ctx, root.Index, root.RootHash)
	if err != nil {
		return err
	}
	if opened.Hash != root.Hash {
		return fmt.Errorf("checkpoint root identity mismatch")
	}
	for _, file := range opened.Files {
		for _, block := range file.Blocks {
			if err := copyVerifiedObject(ctx, bucket, source.key(blockObjectKey(block)), targetKey(targetPrefix, blockObjectKey(block)), block.Size, block.Hash); err != nil {
				return err
			}
		}
	}
	name := rootName(opened.Index, opened.RootHash)
	return copyVerifiedObject(ctx, bucket, source.key(name), targetKey(targetPrefix, name), maxRootSize, hexHash(opened.RootHash))
}

func targetKey(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return prefix + "/" + name
}

func hexHash(hash [32]byte) string { return fmt.Sprintf("%x", hash) }

func copyVerifiedObject(ctx context.Context, bucket objstore.Bucket, source, target string, size int64, expected string) error {
	want, err := decodeHash(expected)
	if err != nil {
		return err
	}
	r, err := bucket.Get(ctx, source)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp("", "rhiza-checkpoint-copy-*")
	if err != nil {
		_ = r.Close()
		return err
	}
	defer func() {
		_ = file.Close()
		_ = os.Remove(file.Name())
	}()
	hasher := sha256.New()
	read, readErr := io.Copy(io.MultiWriter(file, hasher), io.LimitReader(r, size+1))
	closeErr := r.Close()
	var got [32]byte
	copy(got[:], hasher.Sum(nil))
	if readErr != nil || closeErr != nil || read > size || got != want {
		return fmt.Errorf("checkpoint source object integrity mismatch")
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	uploadErr := bucket.Upload(ctx, target, file, objstore.WithIfNotExists())
	if uploadErr != nil && !bucket.IsConditionNotMetErr(uploadErr) {
		return uploadErr
	}
	r, err = bucket.Get(ctx, target)
	if err != nil {
		return err
	}
	hasher = sha256.New()
	read, err = io.Copy(hasher, io.LimitReader(r, size+1))
	closeErr = r.Close()
	got = [32]byte{}
	copy(got[:], hasher.Sum(nil))
	if err != nil || closeErr != nil || read > size || got != want {
		return fmt.Errorf("checkpoint exported object integrity mismatch")
	}
	return nil
}
