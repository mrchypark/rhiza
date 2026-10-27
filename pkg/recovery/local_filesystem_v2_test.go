package recovery

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	internalobjstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
)

func localFilesystemV2Fixture(t *testing.T) (objstore.Bucket, *internalobjstore.LocalFilesystemOwner) {
	t.Helper()
	base := t.TempDir()
	root, data := filepath.Join(base, "archive"), filepath.Join(base, "data")
	if err := os.Mkdir(data, 0700); err != nil {
		t.Fatal(err)
	}
	bucket, err := filesystem.NewBucket(root)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := internalobjstore.AcquireLocalFilesystemOwner(root, data)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(time.Second); err != nil && !strings.Contains(err.Error(), "poisoned") {
			t.Errorf("close filesystem owner: %v", err)
		}
	})
	return owner.Wrap(bucket), owner
}

func localFilesystemV2Binding(owner *internalobjstore.LocalFilesystemOwner, prefix string) LocalBinding {
	binding := localTestBinding()
	binding.Version = 2
	binding.ArchiveLocationHash = owner.LocationHash(prefix)
	return binding
}

func TestLocalFilesystemV2RefusesExistingV1(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	root, data := filepath.Join(base, "archive"), filepath.Join(base, "data")
	if err := os.Mkdir(data, 0700); err != nil {
		t.Fatal(err)
	}
	raw, err := filesystem.NewBucket(root)
	if err != nil {
		t.Fatal(err)
	}
	const prefix = "cluster/local/v1"
	legacy, err := CreateLocalArchive(ctx, raw, prefix, localTestBinding(), nil)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := internalobjstore.AcquireLocalFilesystemOwner(root, data)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := owner.Close(time.Second); err != nil {
			t.Errorf("close filesystem owner: %v", err)
		}
	}()
	if _, err := OpenLocalArchiveOwned(ctx, owner.Wrap(raw), prefix, owner); err == nil {
		t.Fatal("filesystem-v2 opened a v1 archive")
	}
	if _, err := PrepareLocalArchiveOwned(ctx, owner.Wrap(raw), prefix, localFilesystemV2Binding(owner, prefix), owner); !raw.IsConditionNotMetErr(err) {
		t.Fatalf("v2 creation against v1 binding error=%v, want condition conflict", err)
	}
	reopened, err := OpenLocalArchive(ctx, raw, prefix)
	if err != nil || reopened.Binding.Version != 1 || reopened.Tip() != legacy.Tip() {
		t.Fatalf("legacy archive changed: archive=%v error=%v", reopened, err)
	}
}

type localFilesystemV2RaceBucket struct {
	objstore.Bucket
	beforeUpload func(string) error
}

func (b *localFilesystemV2RaceBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	if b.beforeUpload != nil {
		if err := b.beforeUpload(name); err != nil {
			return err
		}
	}
	return b.Bucket.Upload(ctx, name, r, opts...)
}

func TestLocalFilesystemV2GenesisRacesFailClosed(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		race func(context.Context, objstore.Bucket, string) error
		want string
	}{
		{
			name: "v1 binding wins",
			want: "v1",
			race: func(ctx context.Context, raw objstore.Bucket, prefix string) error {
				_, err := CreateLocalArchive(ctx, raw, prefix, localTestBinding(), nil)
				return err
			},
		},
		{
			name: "v1 control wins after v2 binding",
			want: "v2-partial",
			race: func(ctx context.Context, raw objstore.Bucket, prefix string) error {
				control, err := json.Marshal(struct {
					Version uint `json:"version"`
				}{Version: 1})
				if err != nil {
					return err
				}
				return raw.Upload(ctx, prefix+"/local-gc-control", strings.NewReader(string(control)), objstore.WithIfNotExists())
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := t.TempDir()
			root, data := filepath.Join(base, "archive"), filepath.Join(base, "data")
			if err := os.Mkdir(data, 0700); err != nil {
				t.Fatal(err)
			}
			raw, err := filesystem.NewBucket(root)
			if err != nil {
				t.Fatal(err)
			}
			owner, err := internalobjstore.AcquireLocalFilesystemOwner(root, data)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := owner.Close(time.Second); err != nil {
					t.Errorf("close filesystem owner: %v", err)
				}
			}()
			const prefix = "cluster/local/v1"
			fired := false
			raceBucket := &localFilesystemV2RaceBucket{Bucket: owner.Wrap(raw)}
			raceBucket.beforeUpload = func(name string) error {
				if fired {
					return nil
				}
				if tc.name == "v1 binding wins" && name == prefix+"/BINDING" {
					fired = true
					return tc.race(ctx, raw, prefix)
				}
				if tc.name == "v1 control wins after v2 binding" && name == prefix+"/local-gc-control" {
					fired = true
					return tc.race(ctx, raw, prefix)
				}
				return nil
			}
			genesis, err := PrepareLocalArchiveOwned(ctx, raceBucket, prefix, localFilesystemV2Binding(owner, prefix), owner)
			if !fired || genesis != nil || err == nil {
				t.Fatalf("race fired=%t genesis=%v error=%v; want fail-closed conflict", fired, genesis, err)
			}
			if tc.want == "v1" {
				archive, err := OpenLocalArchive(ctx, raw, prefix)
				if err != nil || archive.Binding.Version != 1 {
					t.Fatalf("winning v1 archive not preserved: archive=%v error=%v", archive, err)
				}
				return
			}
			if _, err := raw.Attributes(ctx, prefix+"/HEAD"); !raw.IsObjNotFoundErr(err) {
				t.Fatalf("partial v2 race published HEAD; error=%v", err)
			}
			if _, err := OpenLocalArchiveOwned(ctx, owner.Wrap(raw), prefix, owner); err == nil {
				t.Fatal("partial v2 binding with v1 control was accepted")
			}
			if _, err := PrepareLocalArchiveOwned(ctx, raceBucket, prefix, localFilesystemV2Binding(owner, prefix), owner); err == nil {
				t.Fatal("partial genesis was silently resumed")
			}
		})
	}
}

func TestLocalFilesystemV2RejectsPartialAndMismatchedControl(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []string{"missing control", "binding hash mismatch", "location mismatch"} {
		t.Run(tc, func(t *testing.T) {
			bucket, owner := localFilesystemV2Fixture(t)
			const prefix = "cluster/local/v1"
			binding := localFilesystemV2Binding(owner, prefix)
			bindingData, err := json.Marshal(binding)
			if err != nil {
				t.Fatal(err)
			}
			if err := bucket.Upload(ctx, prefix+"/BINDING", strings.NewReader(string(bindingData)), objstore.WithIfNotExists()); err != nil {
				t.Fatal(err)
			}
			if tc != "missing control" {
				control := localMaintenanceControlV2{Version: 2, LocationHash: binding.ArchiveLocationHash, BindingHash: strings.Repeat("0", 64)}
				if tc == "location mismatch" {
					control.BindingHash = fmt.Sprintf("%x", sha256.Sum256(bindingData))
					control.LocationHash = strings.Repeat("0", 64)
				}
				data, err := json.Marshal(control)
				if err != nil {
					t.Fatal(err)
				}
				if err := bucket.Upload(ctx, prefix+"/local-gc-control", strings.NewReader(string(data)), objstore.WithIfNotExists()); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := OpenLocalArchiveOwned(ctx, bucket, prefix, owner); err == nil {
				t.Fatal("partial or mismatched v2 archive was accepted")
			}
			if _, err := rawAttributes(ctx, bucket, prefix+"/HEAD"); err == nil {
				t.Fatal("invalid archive validation published a HEAD")
			}
			if _, err := OpenLocalArchive(ctx, bucket, prefix); err == nil {
				t.Fatal("legacy opener accepted a v2 binding")
			}
		})
	}
}

func rawAttributes(ctx context.Context, bucket objstore.Bucket, name string) (objstore.ObjectAttributes, error) {
	return bucket.Attributes(ctx, name)
}

func TestLocalFilesystemV2ValidGenesisAndNeighbor(t *testing.T) {
	ctx := context.Background()
	const prefix = "cluster/local/v1"
	bucket, owner := localFilesystemV2Fixture(t)
	genesis, err := PrepareLocalArchiveOwned(ctx, bucket, prefix, localFilesystemV2Binding(owner, prefix), owner)
	if err != nil {
		t.Fatal(err)
	}
	archive, err := CreateLocalArchiveFromPreparedOwned(ctx, genesis, nil, owner)
	if err != nil {
		t.Fatalf("publish valid genesis: %v", err)
	}
	if archive.Tip() != 0 || archive.Binding.Version != 2 {
		t.Fatalf("unexpected archive: version=%d tip=%d", archive.Binding.Version, archive.Tip())
	}
	if _, err := OpenLocalArchiveOwned(ctx, bucket, prefix, owner); err != nil {
		t.Fatalf("open valid v2 archive: %v", err)
	}
	if _, err := OpenLocalArchive(ctx, bucket, prefix); err == nil {
		t.Fatal("unowned/legacy opener accepted a v2 archive")
	}
	if _, err := OpenLocalArchiveOwned(ctx, bucket, prefix+"-neighbor", owner); err == nil {
		t.Fatal("owner opened an archive at a different prefix")
	}
}
