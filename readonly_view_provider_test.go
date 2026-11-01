package rhiza_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
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

type deleteCountingProviderBucket struct {
	objstore.Bucket
	deletes int
}

func (b *deleteCountingProviderBucket) Delete(ctx context.Context, name string) error {
	b.deletes++
	return b.Bucket.Delete(ctx, name)
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
	bucket := &deleteCountingProviderBucket{Bucket: embeddedBucket(t, cfg)}
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
	command, err := types.EncodeGraphCommand(types.GraphCommand{RequestID: "provider-checkpoint", Cypher: `CREATE (:Item {id: 'checkpoint'})`, Events: []types.GraphStreamEvent{{Stream: "provider", Kind: "created", Payload: "checkpointed"}}})
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
	cp, err := checkpoint.NewAppendOnlyManager(bucket, "ha-readonly", "", 1)
	if err != nil {
		t.Fatal(err)
	}
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
	archive, err := recovery.NewAppendOnlyManager(bucket, "ha-readonly", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	oldView, err := recovery.OpenReadOnlyView(ctx, bucket, "ha-readonly", 1, bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	beforeTrim := embeddedRemoteImage(t, cfg)
	sealed, ok, err := core.LatestCheckpointSeal()
	if err != nil || !ok {
		t.Fatalf("missing checkpoint seal: %v", err)
	}
	if err := archive.TrimThrough(ctx, sealed, sealDecision); err != nil {
		t.Fatal(err)
	}
	if _, err := recovery.PrepareReadOnlyRestore(ctx, oldView, filepath.Join(t.TempDir(), "old-view")); err != nil {
		t.Fatalf("pre-trim view after trim: %v", err)
	}
	beforeGC := embeddedRemoteImage(t, cfg)
	for key, data := range beforeTrim {
		if strings.Contains(key, "/archive/blocks/") && !bytes.Equal(beforeGC[key], data) {
			t.Fatalf("trim removed or changed old extent %s", key)
		}
	}
	if err := cp.GarbageCollectFrom(ctx, nil, 1, index, 0); err == nil {
		t.Fatal("append-only checkpoint GC succeeded")
	}
	if err := archive.Cleanup(ctx, 0); err == nil {
		t.Fatal("append-only archive cleanup succeeded")
	}
	if bucket.deletes != 0 {
		t.Fatalf("append-only GC attempted %d deletes", bucket.deletes)
	}
	if after := embeddedRemoteImage(t, cfg); !reflect.DeepEqual(beforeGC, after) {
		t.Fatal("append-only GC changed source objects")
	}
	if err := cp.ReleasePublisherClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	suffix, err := types.EncodeGraphCommand(types.GraphCommand{RequestID: "provider-suffix", Cypher: `CREATE (:Item {id: 'suffix'})`, Events: []types.GraphStreamEvent{{Stream: "provider", Kind: "updated", Payload: "replayed"}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, suffix); err != nil {
		t.Fatal(err)
	}
	if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	archive.Close()
	before := embeddedRemoteImage(t, cfg)
	seen := map[string]bool{}
	for key, data := range before {
		kind := ""
		switch {
		case strings.Contains(key, "/checkpoint/roots/"):
			kind = "root"
		case strings.Contains(key, "/checkpoint/blocks/"):
			kind = "block"
		case strings.Contains(key, "/archive/blocks/"):
			kind = "extent"
		}
		if kind == "" || seen[kind] {
			continue
		}
		seen[kind] = true
		if err := bucket.Upload(ctx, key, bytes.NewReader([]byte("collision")), objstore.WithIfNotExists()); err == nil || !bucket.IsConditionNotMetErr(err) {
			t.Fatalf("%s create-only collision: %v", kind, err)
		}
		r, err := bucket.Get(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(r)
		closeErr := r.Close()
		if err != nil || closeErr != nil || !bytes.Equal(got, data) {
			t.Fatalf("%s collision overwrote object: read=%v close=%v", kind, err, closeErr)
		}
	}
	for _, kind := range []string{"root", "block", "extent"} {
		if !seen[kind] {
			t.Fatalf("missing %s object", kind)
		}
	}
	readonly := &readonlyProviderBucket{Bucket: bucket}
	view, err := recovery.OpenReadOnlyView(ctx, readonly, "ha-readonly", 1, bootstrap)
	if err != nil {
		t.Fatalf("open certified fixed head: %v", err)
	}
	wantPrefix, ok := core.PrefixHash(core.Tip())
	decisions := view.Decisions()
	if !ok || view.Base() != seal.Index || view.Tip() != core.Tip() || view.PrefixHash() != wantPrefix || len(decisions) == 0 || !bytes.Equal(decisions[len(decisions)-1].Value, suffix) {
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
	prepared, err := recovery.PrepareReadOnlyRestore(ctx, view, filepath.Join(t.TempDir(), "prepared"))
	if err != nil {
		t.Fatalf("prepare verified restore: %v", err)
	}
	if prepared.HeadHash != view.HeadHash() || prepared.Base != view.Base() || prepared.Tip != view.Tip() || prepared.PrefixHash != view.PrefixHash() {
		t.Fatalf("prepared identity differs from verified view: %+v", prepared)
	}
	identity := recovery.PreparedStartupIdentity{ClusterUID: "cluster", Generation: 0, DeploymentUID: "deployment", PodUID: "pod", NodeID: "node", SourceBinding: "ha-readonly", ConfigDigest: "fixture-config"}
	handoffDir := filepath.Join(t.TempDir(), "handoff")
	if err := recovery.PublishPreparedStartup(ctx, view, handoffDir, identity); err != nil {
		t.Fatalf("publish prepared handoff: %v", err)
	}
	verifiedDir := filepath.Join(t.TempDir(), "verified")
	handoff, err := recovery.LoadPreparedStartup(ctx, readonly, "ha-readonly", 1, bootstrap, handoffDir, verifiedDir, identity)
	if err != nil {
		t.Fatalf("load prepared handoff: %v", err)
	}
	if handoff.Directory() != verifiedDir || handoff.View().HeadHash() != view.HeadHash() || handoff.View().Tip() != view.Tip() || handoff.View().PrefixHash() != view.PrefixHash() {
		t.Fatal("verified handoff differs from captured source")
	}
	restored, err := materializer.Open(prepared.SQLitePath, 1)
	if err != nil {
		t.Fatalf("open prepared materializer: %v", err)
	}
	t.Cleanup(func() { _ = restored.Close() })
	if restored.Tip() != uint64(view.Tip()) {
		t.Fatalf("prepared materializer tip=%d want=%d", restored.Tip(), view.Tip())
	}
	for _, id := range []string{"provider-checkpoint", "provider-suffix"} {
		if receipt, found, err := restored.GraphMutationReceipt(ctx, id); err != nil || !found || receipt.Status != types.MutationCommitted {
			t.Fatalf("prepared application receipt %q: found=%t receipt=%+v err=%v", id, found, receipt, err)
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
