package rhiza_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

type readonlyProviderBucket struct {
	objstore.Bucket
	writes int
}

func (b *readonlyProviderBucket) Upload(context.Context, string, io.Reader, ...objstore.ObjectUploadOption) error {
	b.writes++
	return errors.New("unexpected source upload")
}

func (b *readonlyProviderBucket) Delete(context.Context, string) error {
	b.writes++
	return errors.New("unexpected source delete")
}

// Exercise the HA archive reader through the same production adapters and
// isolated provider setup as the embedded object-storage contract suite.
func TestEmbeddedObjectStorageHAReadOnlyView(t *testing.T) {
	if p := os.Getenv("RHIZA_EMBEDDED_PROVIDER"); p != "s3" && p != "gcs" && p != "azure" {
		t.Skip("requires an isolated Floci provider")
	}
	ctx := context.Background()
	cfg := embeddedStoreConfig(t)
	bucket := embeddedBucket(t, cfg)
	bootstrap := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: bootstrap, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	command, err := types.EncodeGraphCommand(types.GraphCommand{RequestID: "provider-checkpoint", Events: []types.GraphStreamEvent{{Stream: "provider", Kind: "created", Payload: "checkpointed"}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, command); err != nil {
		t.Fatal(err)
	}
	state, err := materializer.Open(filepath.Join(t.TempDir(), "source.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	first, ok := core.CertifiedValue(core.Tip())
	if !ok {
		t.Fatal("missing certified checkpoint input")
	}
	if err := state.ApplyBatch(ctx, []quepaxa.DecidedValue{first}); err != nil {
		t.Fatal(err)
	}
	files, index, cleanup, err := state.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	cp := checkpoint.NewManager(bucket, "ha-readonly", "", 1)
	claim, err := cp.AcquirePublisherClaim(ctx, "provider", 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	sources := make([]checkpoint.Source, 0, len(files))
	wantFiles := make(map[string][]byte)
	for _, file := range files {
		sources = append(sources, checkpoint.Source{Role: string(file.Role), Path: file.Path})
		wantFiles[string(file.Role)], err = os.ReadFile(file.Path)
		if err != nil {
			t.Fatal(err)
		}
	}
	root, err := cp.CreateFiles(ctx, claim, sources, index)
	if err != nil {
		t.Fatal(err)
	}
	claim, err = cp.BindPublisherClaim(ctx, claim, index, root.RootHash, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	prefix, ok := core.PrefixHash(core.Tip())
	if !ok {
		t.Fatal("missing checkpoint prefix")
	}
	next, following, err := core.CheckpointLeaderOrders(core.Tip())
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{ConfigID: 1, Index: core.Tip(), RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following}
	core.SetCheckpointValidator(func(ctx context.Context, candidate quepaxa.CheckpointSeal) error {
		return cp.Verify(ctx, uint64(candidate.Index), candidate.RootHash, candidate.StateHash)
	})
	if err := core.PrepareCheckpoint(ctx, seal); err != nil {
		t.Fatal(err)
	}
	sealBytes, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	sealSlot, _, err := core.Propose(ctx, sealBytes)
	if err != nil {
		t.Fatal(err)
	}
	sealDecision, ok := core.CertifiedValue(sealSlot)
	if !ok {
		t.Fatal("missing certified checkpoint seal")
	}
	archive := recovery.NewManager(bucket, "ha-readonly", 1)
	if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	sealed, ok, err := core.LatestCheckpointSeal()
	if err != nil || !ok {
		t.Fatalf("missing checkpoint seal: %v", err)
	}
	if err := archive.TrimThrough(ctx, sealed, sealDecision); err != nil {
		t.Fatal(err)
	}
	if err := cp.ReleasePublisherClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, []byte("provider-certified-value")); err != nil {
		t.Fatal(err)
	}
	if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	archive.Close()
	before := embeddedRemoteImage(t, cfg)
	readonly := &readonlyProviderBucket{Bucket: bucket}
	view, err := recovery.OpenReadOnlyView(ctx, readonly, "ha-readonly", 1, bootstrap)
	if err != nil {
		t.Fatalf("open certified fixed head: %v", err)
	}
	wantPrefix, ok := core.PrefixHash(core.Tip())
	decisions := view.Decisions()
	if !ok || view.Base() != seal.Index || view.Tip() != core.Tip() || view.PrefixHash() != wantPrefix || len(decisions) == 0 || string(decisions[len(decisions)-1].Value) != "provider-certified-value" {
		t.Fatalf("unexpected verified view: base=%d tip=%d decisions=%d", view.Base(), view.Tip(), len(decisions))
	}
	downloaded, err := view.DownloadCheckpointFiles(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("download verified checkpoint: %v", err)
	}
	if len(downloaded) != len(wantFiles) || len(downloaded) == 0 {
		t.Fatalf("checkpoint files=%d want=%d", len(downloaded), len(wantFiles))
	}
	for _, file := range downloaded {
		data, err := os.ReadFile(file.Path)
		if err != nil || !bytes.Equal(data, wantFiles[file.Role]) {
			t.Fatalf("checkpoint role %s differs: %v", file.Role, err)
		}
	}
	if _, err := recovery.OpenReadOnlyView(ctx, readonly, "missing-head", 1, bootstrap); err == nil {
		t.Fatal("missing head accepted")
	}
	if readonly.writes != 0 {
		t.Fatalf("source mutation attempts: %d", readonly.writes)
	}
	if after := embeddedRemoteImage(t, cfg); !reflect.DeepEqual(before, after) {
		t.Fatal("source objects changed during read-only verification")
	}
}
