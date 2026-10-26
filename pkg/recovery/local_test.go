package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
)

type localCapabilityBucket struct{ objstore.Bucket }

func (b localCapabilityBucket) SupportedObjectUploadOptions() []objstore.ObjectUploadOptionType {
	return []objstore.ObjectUploadOptionType{objstore.IfNotExists}
}

type localUncertainBucket struct {
	objstore.Bucket
	fail       bool
	headWrites int
}

func (b *localUncertainBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	if strings.HasSuffix(name, "/HEAD") {
		b.headWrites++
	}
	err := b.Bucket.Upload(ctx, name, r, opts...)
	if err == nil && b.fail && strings.HasSuffix(name, "/HEAD") {
		return errors.New("lost upload acknowledgement")
	}
	return err
}
func localTestBinding() LocalBinding {
	return LocalBinding{Version: 1, StoreUUID: strings.Repeat("a", 32), ConfigDigest: strings.Repeat("b", 64), PolicyVersion: 1, Bootstrap: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "local"}}}}
}
func TestLocalArchiveRequiresBothConditionalCapabilities(t *testing.T) {
	bucket, err := filesystem.NewBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Close()
	if _, err := CreateLocalArchive(context.Background(), localCapabilityBucket{bucket}, "local/v1", localTestBinding(), nil); err == nil {
		t.Fatal("accepted no IfMatch")
	}
	exists, err := bucket.Exists(context.Background(), "local/v1/BINDING")
	if err != nil || exists {
		t.Fatalf("capability rejection mutated binding: exists=%v err=%v", exists, err)
	}
}
func TestLocalArchiveUncertainHeadRequiresReopen(t *testing.T) {
	ctx := context.Background()
	bucket, err := filesystem.NewBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Close()
	fault := &localUncertainBucket{Bucket: bucket}
	archive, err := CreateLocalArchive(ctx, fault, "local/v1", localTestBinding(), nil)
	if err != nil {
		t.Fatal(err)
	}
	fault.fail = true
	if err := archive.publish(ctx, archive.head); err == nil {
		t.Fatal("lost ACK accepted")
	}
	calls := fault.headWrites
	if err := archive.SyncThrough(ctx, nil, 0); err == nil {
		t.Fatal("poisoned publisher continued")
	}
	if fault.headWrites != calls {
		t.Fatal("poisoned publisher retried stale CAS")
	}
	fault.fail = false
	if _, err := OpenLocalArchive(ctx, bucket, "local/v1"); err != nil {
		t.Fatalf("reopen: %v", err)
	}
}
func TestLocalArchiveMissingHeadCannotBootstrap(t *testing.T) {
	ctx := context.Background()
	bucket, err := filesystem.NewBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Close()
	if _, err := CreateLocalArchive(ctx, bucket, "local/v1", localTestBinding(), nil); err != nil {
		t.Fatal(err)
	}
	if err := bucket.Delete(ctx, "local/v1/HEAD"); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenLocalArchive(ctx, bucket, "local/v1"); err == nil {
		t.Fatal("missing head accepted")
	}
	if _, err := CreateLocalArchive(ctx, bucket, "local/v1", localTestBinding(), nil); err == nil {
		t.Fatal("binding reinitialized")
	}
}
func TestLocalArchiveGenesisImmutable(t *testing.T) {
	ctx := context.Background()
	bucket, err := filesystem.NewBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Close()
	if _, err := CreateLocalArchive(ctx, bucket, "local/v1", localTestBinding(), nil); err != nil {
		t.Fatal(err)
	}
	reader, err := bucket.Get(ctx, "local/v1/BINDING")
	if err != nil {
		t.Fatal(err)
	}
	before, _ := io.ReadAll(reader)
	_ = reader.Close()
	binding := localTestBinding()
	binding.StoreUUID = strings.Repeat("c", 32)
	if _, err := CreateLocalArchive(ctx, bucket, "local/v1", binding, nil); err == nil {
		t.Fatal("replaced binding")
	}
	reader, err = bucket.Get(ctx, "local/v1/BINDING")
	if err != nil {
		t.Fatal(err)
	}
	after, _ := io.ReadAll(reader)
	_ = reader.Close()
	if !bytes.Equal(before, after) {
		t.Fatal("binding changed")
	}
}

func TestLocalArchiveRebaseDropsHistoryAndPreservesPagedSuffix(t *testing.T) {
	ctx := context.Background()
	bucket, err := filesystem.NewBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Close()
	archive, err := CreateLocalArchive(ctx, bucket, "local/v1", localTestBinding(), nil)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	if err := wal.SetMaxBytes(4 << 20); err != nil {
		t.Fatal(err)
	}
	core, err := quepaxa.New(quepaxa.Config{NodeID: "local", Cluster: localTestBinding().Bootstrap, WAL: wal, LocalMode: true})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 12 {
		if _, _, err := core.Propose(ctx, []byte(fmt.Sprintf("before-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	index := core.Tip()
	prefix, _ := core.PrefixHash(index)
	next, following, err := core.CheckpointLeaderOrders(index)
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{ConfigID: 1, Index: index, RootHash: sha256.Sum256([]byte("local-root")), StateHash: sha256.Sum256([]byte("local-state")), PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following}
	core.SetCheckpointValidator(func(context.Context, quepaxa.CheckpointSeal) error { return nil })
	var snapshot LocalSnapshot
	_, err = core.ReclaimLocalCheckpoint(ctx, seal, func(ctx context.Context, slot quepaxa.Slot) error {
		decision, ok := core.CertifiedValue(slot)
		if !ok {
			return fmt.Errorf("seal not certified")
		}
		snapshot = LocalSnapshot{Seal: seal, Decision: decision, PortableRoot: sha256.Sum256([]byte("portable")), Descriptor: json.RawMessage(`{}`)}
		return StoreLocalSnapshot(ctx, bucket, "local/v1", snapshot)
	})
	if err != nil {
		t.Fatal(err)
	}
	// The source WAL has compacted: first attach seeds its exact certified base.
	if err := bucket.Delete(ctx, "local/v1/HEAD"); err != nil {
		t.Fatal(err)
	}
	if err := bucket.Delete(ctx, "local/v1/BINDING"); err != nil {
		t.Fatal(err)
	}
	archive, err = CreateLocalArchive(ctx, bucket, "local/v1", localTestBinding(), &snapshot)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 22 {
		if _, _, err := core.Propose(ctx, []byte(fmt.Sprintf("after-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	if err := archive.PublishSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenLocalArchive(ctx, bucket, "local/v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := reopened.PrefixHash(index - 1); ok {
		t.Fatal("rebase retained old proof")
	}
	got := prefix
	from := index + 1
	count := 0
	for from <= reopened.Tip() {
		page := reopened.DecisionsFrom(from, 7)
		if len(page) == 0 {
			t.Fatal("paged suffix gap")
		}
		for _, value := range page {
			if value.Slot != from {
				t.Fatal("noncontiguous page")
			}
			got = quepaxa.AdvancePrefixHash(got, value.Slot, value.Hash)
			from++
			count++
		}
	}
	want, _ := core.PrefixHash(core.Tip())
	if got != want || count < 22 {
		t.Fatalf("suffix proof/count: count=%d hash=%x want=%x", count, got, want)
	}
	for _, extent := range reopened.extents {
		for _, value := range extent.Decisions {
			if value.Slot <= index {
				t.Fatal("retained decoded history before base")
			}
		}
	}
}
