package node

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

func TestPrepareHAInitRestoreVerifiesAndStagesReadOnly(t *testing.T) {
	fixture := newHAAdoptionFixture(t)
	config := fixture.configFor(filepath.Join(t.TempDir(), "new-data"))
	config.ObjStoreProvider = "filesystem"
	config.HAInitRestoreDir = fixture.stageDir
	bucket := &restoreInitWriteTrackingBucket{Bucket: fixture.bucket}

	if err := PrepareHAInitRestore(fixture.ctx, config, bucket); err != nil {
		t.Fatalf("prepare existing verified source: %v", err)
	}
	if bucket.uploads != 0 || bucket.deletes != 0 {
		t.Fatalf("restore-init attempted remote mutation: uploads=%d deletes=%d", bucket.uploads, bucket.deletes)
	}
	index, present, err := readHAStageIndex(fixture.stageDir)
	if err != nil || !present || !index.HasCheckpoint {
		t.Fatalf("restore-init did not preserve complete checkpoint stage: present=%t index=%+v err=%v", present, index, err)
	}
}

func TestPrepareHAInitRestoreRejectsUntrustedMembershipAndMissingRoot(t *testing.T) {
	fixture := newHAAdoptionFixture(t)
	for _, test := range []struct {
		name   string
		change func(*types.ExecutionConfig)
	}{
		{name: "wrong bootstrap membership", change: func(config *types.ExecutionConfig) {
			config.Members = []quepaxa.Member{{ID: "impostor", PublicKey: peerMember(string(config.ClusterID), "impostor", "impostor-token").PublicKey}}
		}},
		{name: "missing configured source root", change: func(config *types.ExecutionConfig) {
			config.ObjStorePrefix = "missing-source-root"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := fixture.configFor(filepath.Join(t.TempDir(), "new-data"))
			config.ObjStoreProvider = "filesystem"
			config.HAInitRestoreDir = filepath.Join(t.TempDir(), "stage")
			test.change(config)
			if err := PrepareHAInitRestore(fixture.ctx, config, fixture.bucket); err == nil {
				t.Fatal("restore-init accepted source under untrusted or missing HA evidence")
			}
		})
	}
}

func TestPrepareHAInitRestoreRejectsDataDirOverlapBeforeStaging(t *testing.T) {
	fixture := newHAAdoptionFixture(t)
	config := fixture.configFor(t.TempDir())
	config.ObjStoreProvider = "filesystem"
	config.HAInitRestoreDir = filepath.Join(config.DataDir, "restore-stage")
	if err := PrepareHAInitRestore(fixture.ctx, config, fixture.bucket); err == nil {
		t.Fatal("restore-init accepted a stage directory inside DataDir")
	}
	if _, err := os.Lstat(config.HAInitRestoreDir); !os.IsNotExist(err) {
		t.Fatalf("rejected stage path exists: %v", err)
	}
	if len(mustReadDir(t, config.DataDir)) != 0 {
		t.Fatalf("rejected restore-init touched DataDir: %v", mustReadDir(t, config.DataDir))
	}
}

func TestPrepareHAInitRestoreRejectsStageSymlinkIntoDataDir(t *testing.T) {
	fixture := newHAAdoptionFixture(t)
	config := fixture.configFor(t.TempDir())
	config.ObjStoreProvider = "filesystem"
	link := filepath.Join(t.TempDir(), "stage-link")
	if err := os.Symlink(config.DataDir, link); err != nil {
		t.Fatal(err)
	}
	config.HAInitRestoreDir = filepath.Join(link, "stage")
	if err := PrepareHAInitRestore(fixture.ctx, config, fixture.bucket); err == nil {
		t.Fatal("restore-init accepted a stage path aliased into DataDir")
	}
	if len(mustReadDir(t, config.DataDir)) != 0 {
		t.Fatalf("rejected symlinked restore-init touched DataDir: %v", mustReadDir(t, config.DataDir))
	}
}

func TestHAInitRestoreReadOnlyBucketRejectsMutations(t *testing.T) {
	fixture := newHAAdoptionFixture(t)
	readOnly := haRestoreReadOnlyBucket{Bucket: fixture.bucket}
	if err := readOnly.Upload(context.Background(), "unexpected", nil); err == nil {
		t.Fatal("read-only restore bucket accepted Upload")
	}
	if err := readOnly.Delete(context.Background(), "unexpected"); err == nil {
		t.Fatal("read-only restore bucket accepted Delete")
	}
}

type restoreInitWriteTrackingBucket struct {
	objstore.Bucket
	uploads int
	deletes int
}

func (b *restoreInitWriteTrackingBucket) Upload(context.Context, string, io.Reader, ...objstore.ObjectUploadOption) error {
	b.uploads++
	return nil
}

func (b *restoreInitWriteTrackingBucket) Delete(context.Context, string) error {
	b.deletes++
	return nil
}

func mustReadDir(t *testing.T, dir string) []os.DirEntry {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	return entries
}
