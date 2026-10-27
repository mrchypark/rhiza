package recovery

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	internalobjstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
)

func localFilesystemFixture(t *testing.T) (objstore.Bucket, objstore.Bucket, *internalobjstore.LocalFilesystemOwner) {
	t.Helper()
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
	t.Cleanup(func() {
		if err := owner.Close(time.Second); err != nil && !strings.Contains(err.Error(), "poisoned") {
			t.Errorf("close filesystem owner: %v", err)
		}
	})
	return owner.Wrap(raw), raw, owner
}

func localFilesystemBinding(owner *internalobjstore.LocalFilesystemOwner, prefix string) LocalBinding {
	binding := localTestBinding()
	binding.ArchiveLocationHash = owner.LocationHash(prefix)
	return binding
}

func TestLocalFilesystemRejectsUnsupportedFormatWithoutMutation(t *testing.T) {
	ctx := context.Background()
	bucket, raw, owner := localFilesystemFixture(t)
	const prefix = "cluster/local/v1"
	legacy := localTestBinding()
	legacy.Version = localArchiveFormatVersion - 1
	data, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.Upload(ctx, prefix+"/BINDING", strings.NewReader(string(data)), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenLocalArchiveOwned(ctx, bucket, prefix, owner); err == nil {
		t.Fatal("unsupported archive format was accepted")
	}
	got, err := readObject(ctx, raw, prefix+"/BINDING")
	if err != nil || string(got) != string(data) {
		t.Fatalf("unsupported binding changed: got=%q err=%v", got, err)
	}
	for _, key := range []string{"local-gc-control", "HEAD"} {
		if exists, err := raw.Exists(ctx, prefix+"/"+key); err != nil || exists {
			t.Fatalf("rejection created %s: exists=%t err=%v", key, exists, err)
		}
	}
}

type localFilesystemRaceBucket struct {
	objstore.Bucket
	beforeUpload func(string) error
}

func (b *localFilesystemRaceBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	if b.beforeUpload != nil {
		if err := b.beforeUpload(name); err != nil {
			return err
		}
	}
	return b.Bucket.Upload(ctx, name, r, opts...)
}

func TestLocalFilesystemGenesisConflictsFailClosed(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name     string
		trigger  string
		complete bool
		seed     func(context.Context, objstore.Bucket, string) error
	}{
		{
			name: "remote archive binding wins", trigger: "/BINDING", complete: true,
			seed: func(ctx context.Context, raw objstore.Bucket, prefix string) error {
				_, err := CreateLocalArchive(ctx, raw, prefix, localTestBinding(), nil)
				return err
			},
		},
		{
			name: "unexpected control wins after filesystem binding", trigger: "/local-gc-control",
			seed: func(ctx context.Context, raw objstore.Bucket, prefix string) error {
				control, err := json.Marshal(localArchiveMaintenanceControl{Version: localArchiveFormatVersion, LocationHash: "wrong", BindingHash: strings.Repeat("0", 64)})
				if err != nil {
					return err
				}
				return raw.Upload(ctx, prefix+"/local-gc-control", strings.NewReader(string(control)), objstore.WithIfNotExists())
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bucket, raw, owner := localFilesystemFixture(t)
			const prefix = "cluster/local/v1"
			fired := false
			raced := &localFilesystemRaceBucket{Bucket: bucket}
			raced.beforeUpload = func(name string) error {
				if !fired && strings.HasSuffix(name, tc.trigger) {
					fired = true
					return tc.seed(ctx, raw, prefix)
				}
				return nil
			}
			genesis, err := PrepareLocalArchiveOwned(ctx, raced, prefix, localFilesystemBinding(owner, prefix), owner)
			if !fired || genesis != nil || err == nil {
				t.Fatalf("race fired=%t genesis=%v error=%v; want fail-closed conflict", fired, genesis, err)
			}
			if tc.complete {
				if _, err := OpenLocalArchive(ctx, raw, prefix); err != nil {
					t.Fatalf("winning remote archive was changed: %v", err)
				}
			} else if _, err := raw.Attributes(ctx, prefix+"/HEAD"); !raw.IsObjNotFoundErr(err) {
				t.Fatalf("partial race published HEAD; error=%v", err)
			}
			if _, err := OpenLocalArchiveOwned(ctx, bucket, prefix, owner); err == nil {
				t.Fatal("conflicting or partial genesis was accepted")
			}
			if _, err := PrepareLocalArchiveOwned(ctx, bucket, prefix, localFilesystemBinding(owner, prefix), owner); err == nil {
				t.Fatal("partial genesis was silently resumed")
			}
		})
	}
}

func TestLocalFilesystemRejectsPartialAndMismatchedControl(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []string{"missing control", "binding hash mismatch", "location mismatch"} {
		t.Run(tc, func(t *testing.T) {
			bucket, _, owner := localFilesystemFixture(t)
			const prefix = "cluster/local/v1"
			binding := localFilesystemBinding(owner, prefix)
			bindingData, err := json.Marshal(binding)
			if err != nil {
				t.Fatal(err)
			}
			if err := bucket.Upload(ctx, prefix+"/BINDING", strings.NewReader(string(bindingData)), objstore.WithIfNotExists()); err != nil {
				t.Fatal(err)
			}
			if tc != "missing control" {
				control := localArchiveMaintenanceControl{Version: localArchiveFormatVersion, LocationHash: binding.ArchiveLocationHash, BindingHash: fmt.Sprintf("%x", sha256.Sum256(bindingData))}
				if tc == "binding hash mismatch" {
					control.BindingHash = strings.Repeat("0", 64)
				} else {
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
				t.Fatal("partial or mismatched archive was accepted")
			}
			if exists, err := bucket.Exists(ctx, prefix+"/HEAD"); err != nil || exists {
				t.Fatalf("validation published HEAD: exists=%t err=%v", exists, err)
			}
		})
	}
}

func TestLocalFilesystemValidGenesisAndNeighbor(t *testing.T) {
	ctx := context.Background()
	const prefix = "cluster/local/v1"
	bucket, _, owner := localFilesystemFixture(t)
	genesis, err := PrepareLocalArchiveOwned(ctx, bucket, prefix, localFilesystemBinding(owner, prefix), owner)
	if err != nil {
		t.Fatal(err)
	}
	archive, err := CreateLocalArchiveFromPreparedOwned(ctx, genesis, nil, owner)
	if err != nil {
		t.Fatalf("publish valid genesis: %v", err)
	}
	if archive.Tip() != 0 || archive.Binding.Version != localArchiveFormatVersion {
		t.Fatalf("unexpected archive: version=%d tip=%d", archive.Binding.Version, archive.Tip())
	}
	if _, err := OpenLocalArchiveOwned(ctx, bucket, prefix, owner); err != nil {
		t.Fatalf("open valid archive: %v", err)
	}
	inspection, err := OpenLocalArchive(ctx, bucket, prefix)
	if err != nil {
		t.Fatalf("generic opener could not inspect filesystem-bound metadata: %v", err)
	}
	if inspection.Tip() != 0 || inspection.Snapshot() != nil {
		t.Fatalf("unexpected unowned inspection view: tip=%d snapshot=%v", inspection.Tip(), inspection.Snapshot())
	}
	if _, err := OpenLocalArchiveOwned(ctx, bucket, prefix+"-neighbor", owner); err == nil {
		t.Fatal("owner opened an archive at a different prefix")
	}
}

func TestLocalFilesystemUnownedOpenIsReadOnly(t *testing.T) {
	ctx := context.Background()
	const prefix = "cluster/local/v1"
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
		if err := owner.Close(time.Second); err != nil && !strings.Contains(err.Error(), "poisoned") {
			t.Errorf("close filesystem owner: %v", err)
		}
	}()
	owned := owner.Wrap(raw)
	archive, err := CreateLocalArchiveOwned(ctx, owned, prefix, localFilesystemBinding(owner, prefix), nil, owner)
	if err != nil {
		t.Fatal(err)
	}
	value := []byte("readable archived decision")
	decision := quepaxa.DecidedValue{Slot: 1, Value: value, Hash: sha256.Sum256(value), Certificate: json.RawMessage(`{}`)}
	archivedPrefix := quepaxa.AdvancePrefixHash([32]byte{}, decision.Slot, decision.Hash)
	source := localArchiveSuffix{values: []quepaxa.DecidedValue{decision}, prefixes: map[quepaxa.Slot][32]byte{0: {}, 1: archivedPrefix}, tip: 1}
	if err := archive.SyncThrough(ctx, source, source.tip); err != nil {
		t.Fatal(err)
	}
	before := filesystemTree(t, root)

	inspection, err := OpenLocalArchive(ctx, raw, prefix)
	if err != nil {
		t.Fatalf("unowned filesystem inspection: %v", err)
	}
	if inspection.Tip() != archive.Tip() || inspection.Snapshot() != nil {
		t.Fatalf("inspection view tip=%d snapshot=%v; want tip=%d and no snapshot", inspection.Tip(), inspection.Snapshot(), archive.Tip())
	}
	if prefixHash, ok := inspection.PrefixHash(1); !ok || prefixHash != archivedPrefix {
		t.Fatalf("inspection prefix at tip=%x present=%t; want %x", prefixHash, ok, archivedPrefix)
	}
	if decisions := inspection.DecisionsFrom(1, 10); len(decisions) != 1 || string(decisions[0].Value) != string(value) {
		t.Fatalf("inspection decisions=%v; want the archived decision", decisions)
	}

	mutations := []struct {
		name string
		run  func() error
	}{
		{name: "sync", run: func() error { return inspection.SyncThrough(ctx, nil, 1) }},
		{name: "publish snapshot", run: func() error { return inspection.PublishSnapshot(ctx, LocalSnapshot{}) }},
		{name: "garbage collect", run: func() error { return inspection.GarbageCollect(ctx, 1, time.Second) }},
		{name: "pin view", run: func() error { _, err := inspection.PinView(ctx, "reader", time.Minute); return err }},
		{name: "pin restore", run: func() error { _, err := inspection.PinRestore(ctx, LocalSnapshot{}, "reader", time.Minute); return err }},
		{name: "clear maintenance control", run: func() error { return inspection.RecoverMaintenanceControl(ctx, owner) }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			if err := mutation.run(); err == nil {
				t.Fatal("unowned inspection allowed archive mutation")
			}
		})
	}
	if after := filesystemTree(t, root); !reflect.DeepEqual(before, after) {
		t.Fatalf("unowned mutation attempt changed archive bytes: before=%v after=%v", before, after)
	}
}

func filesystemTree(t *testing.T, root string) map[string]string {
	t.Helper()
	files := make(map[string]string)
	err := filepath.WalkDir(root, func(name string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, name)
		if err != nil {
			return err
		}
		files[rel] = string(data)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func readObject(ctx context.Context, bucket objstore.Bucket, key string) ([]byte, error) {
	r, err := bucket.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}
