package recovery

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/thanos-io/objstore"
)

type emptyArchiveScopeFaultBucket struct {
	objstore.Bucket
	existsKey string
	iterDir   string
	err       error
}

func (b *emptyArchiveScopeFaultBucket) Exists(ctx context.Context, name string) (bool, error) {
	if name == b.existsKey {
		return false, b.err
	}
	return b.Bucket.Exists(ctx, name)
}

func (b *emptyArchiveScopeFaultBucket) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	if dir == b.iterDir {
		return b.err
	}
	return b.Bucket.Iter(ctx, dir, f, options...)
}

func TestRequireEmptyArchiveScope(t *testing.T) {
	ctx := context.Background()
	t.Run("truly empty", func(t *testing.T) {
		if err := RequireEmptyArchiveScope(ctx, objstore.NewInMemBucket(), "scope"); err != nil {
			t.Fatalf("empty scope rejected: %v", err)
		}
	})

	for _, key := range []string{
		"scope/archive/blocks/orphan.bin",
		"scope/archive/manifests/manifest.json",
		"scope/archive/extents/legacy.json",
		"scope/archive/gc-candidates/old.json",
		"scope/fork/intent.json",
		"scope/fork/result.json",
		"scope/checkpoint/CURRENT",
		"scope/checkpoint/roots/root.json",
		"scope/checkpoint/blocks/block",
		"scope/checkpoint/recovery-pins/pin",
		"scope/recovery/anchor.json",
		"scope/recovery/generation.json",
		"scope/recovery/successor.json",
	} {
		t.Run(key, func(t *testing.T) {
			bucket := objstore.NewInMemBucket()
			if err := bucket.Upload(ctx, key, strings.NewReader("prior state")); err != nil {
				t.Fatal(err)
			}
			if err := RequireEmptyArchiveScope(ctx, bucket, "scope"); err == nil {
				t.Fatalf("accepted nonempty scope containing %q", key)
			}
		})
	}

	t.Run("existing head", func(t *testing.T) {
		bucket := objstore.NewInMemBucket()
		head, err := EmptyArchiveHeadBytes()
		if err != nil {
			t.Fatal(err)
		}
		if err := bucket.Upload(ctx, "scope/archive/head.bin", bytes.NewReader(head)); err != nil {
			t.Fatal(err)
		}
		if err := RequireEmptyArchiveScope(ctx, bucket, "scope"); err == nil {
			t.Fatal("accepted scope with a valid empty HEAD")
		}
	})
}

func TestRequireEmptyArchiveScopeFailsClosedOnProbeErrors(t *testing.T) {
	fault := errors.New("probe failed")
	tests := []struct {
		name      string
		existsKey string
		iterDir   string
	}{
		{name: "exists", existsKey: "scope/fork/intent.json"},
		{name: "iter", iterDir: "scope/archive/"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket := &emptyArchiveScopeFaultBucket{Bucket: objstore.NewInMemBucket(), existsKey: tt.existsKey, iterDir: tt.iterDir, err: fault}
			if err := RequireEmptyArchiveScope(context.Background(), bucket, "scope"); !errors.Is(err, fault) {
				t.Fatalf("probe failure was not returned: %v", err)
			}
		})
	}
}

func TestRequireEmptyArchiveScopeRejectsUnsafePrefixes(t *testing.T) {
	for _, prefix := range []string{"", ".", "../scope", "scope/../other", "/scope", "scope/"} {
		t.Run(prefix, func(t *testing.T) {
			if err := RequireEmptyArchiveScope(context.Background(), objstore.NewInMemBucket(), prefix); err == nil {
				t.Fatalf("accepted unsafe prefix %q", prefix)
			}
		})
	}
}
