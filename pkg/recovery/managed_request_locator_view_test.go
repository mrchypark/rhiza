package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

// This fixture publishes real certified QCKPs without trimming HEAD. A locator
// is discovery data, not permission to replace a previously captured history.
func TestManagedRequestReadOnlyLocatorAndFixedHandoff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const policy = sqlpolicy.ManagedRequestMaterializationPolicy
	const protocol = sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol
	const window = types.DefaultIdempotencyWindowSlots
	const prefix = "request-view"
	bucket := objstore.NewInMemBucket()
	// Keep provider conditional-operation capabilities visible, but deny every
	// actual upload/delete through the reader (including conditional uploads).
	readonly := &haReadOnlyRecordingBucket{Bucket: bucket}
	t.Cleanup(func() {
		if readonly.mutations != 0 {
			t.Fatalf("read-only source attempted %d mutations", readonly.mutations)
		}
	})
	bootstrap := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "request-view", PublicKey: testPublicKey("request-view")}}}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := wal.Close(); err != nil {
			t.Error(err)
		}
	})
	core, err := quepaxa.New(quepaxa.Config{NodeID: "request-view", Cluster: bootstrap, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	producer, err := materializer.OpenManagedSQLKVWithAppendOnlyOriginals(filepath.Join(t.TempDir(), "producer.sqlite"), 1, window)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := producer.Close(); err != nil {
			t.Error(err)
		}
	})
	archive, err := NewAppendOnlyManagerWithValueValidator(bucket, prefix, 1, types.ValidateManagedKVExecutionPolicy)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(archive.Close)
	if err := archive.Load(ctx); err != nil {
		t.Fatal(err)
	}
	cp, err := checkpoint.NewAppendOnlyManagerForPolicy(bucket, prefix, t.TempDir(), policy, 1)
	if err != nil {
		t.Fatal(err)
	}
	core.SetCheckpointValidator(func(ctx context.Context, s quepaxa.CheckpointSeal) error {
		return cp.Verify(ctx, uint64(s.Index), s.RootHash, s.StateHash)
	})
	proposeLocal := func(value []byte) quepaxa.DecidedValue {
		t.Helper()
		slot, _, err := core.Propose(ctx, value)
		if err != nil {
			t.Fatal(err)
		}
		d, ok := core.CertifiedValue(slot)
		if !ok || len(d.Certificate) == 0 {
			t.Fatal("actual certified decision unavailable")
		}
		if err := producer.ApplyBatch(ctx, []quepaxa.DecidedValue{d}); err != nil {
			t.Fatal(err)
		}
		return d
	}
	propose := func(value []byte) quepaxa.DecidedValue {
		t.Helper()
		d := proposeLocal(value)
		if err := archive.SyncThrough(ctx, core, d.Slot); err != nil {
			t.Fatal(err)
		}
		return d
	}
	write := func(id string) {
		t.Helper()
		value, err := types.EncodeManagedKVCommand(types.KVCommand{RequestID: id, Operation: "put", Key: id, Value: []byte(id)})
		if err != nil {
			t.Fatal(err)
		}
		propose(value)
	}
	checkpointNow := func() checkpoint.ManagedAppendOnlyLocator {
		t.Helper()
		files, cut, release, err := producer.CheckpointFilesAt(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		sources := make([]checkpoint.Source, len(files))
		for i, f := range files {
			sources[i] = checkpoint.Source{Role: string(f.Role), Path: f.Path}
		}
		root, err := cp.CreateManagedAppendOnlyCandidate(ctx, sources, cut)
		if err != nil {
			t.Fatal(err)
		}
		prefixHash, ok := core.PrefixHash(quepaxa.Slot(cut))
		if !ok {
			t.Fatal("checkpoint prefix unavailable")
		}
		next, following, err := core.CheckpointLeaderOrders(quepaxa.Slot(cut))
		if err != nil {
			t.Fatal(err)
		}
		seal := quepaxa.CheckpointSeal{ConfigID: 1, Index: quepaxa.Slot(cut), RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefixHash, NextLeaderOrder: next, FollowingLeaderOrder: following}
		if err := core.PrepareCheckpoint(ctx, seal); err != nil {
			t.Fatal(err)
		}
		value, err := quepaxa.EncodeCheckpointSeal(seal)
		if err != nil {
			t.Fatal(err)
		}
		d := propose(value)
		l := checkpoint.ManagedAppendOnlyLocator{Policy: int(policy), Index: cut, RootHash: root.RootHash, SealDecisionSlot: uint64(d.Slot), SealDecisionHash: d.Hash}
		if err := cp.PromoteManagedAppendOnlyLocator(ctx, l); err != nil {
			t.Fatal(err)
		}
		return l
	}
	open := func() *ReadOnlyView {
		t.Helper()
		before := requestViewBucketHashes(t, ctx, bucket)
		v, err := OpenReadOnlyViewForApplicationProtocol(ctx, readonly, prefix, 1, bootstrap, policy, window, protocol, false)
		if err != nil {
			t.Fatal(err)
		}
		if after := requestViewBucketHashes(t, ctx, bucket); !reflect.DeepEqual(before, after) {
			t.Fatal("read-only discovery mutated source objects")
		}
		return v
	}
	identity := mixedPreparedIdentity()
	identity.Generation = 0
	publish := func(v *ReadOnlyView) string {
		t.Helper()
		dir := filepath.Join(t.TempDir(), "handoff")
		if err := PublishPreparedStartupForPolicy(ctx, v, dir, identity, policy, window); err != nil {
			t.Fatal(err)
		}
		return dir
	}
	load := func(t *testing.T, dir string, want *ReadOnlyView, ids ...string) {
		t.Helper()
		before := requestViewBucketHashes(t, ctx, bucket)
		p, err := LoadPreparedStartupForApplicationProtocol(ctx, readonly, prefix, 1, bootstrap, dir, filepath.Join(t.TempDir(), "verified"), identity, policy, window, protocol, false)
		if err != nil {
			t.Fatal("fixed handoff changed after CURRENT advanced", err)
		}
		if p.View().Base() != want.Base() || p.View().Tip() != want.Tip() || p.View().prefixHash != want.prefixHash || !bytes.Equal(p.View().CapturedHead(), want.CapturedHead()) || !reflect.DeepEqual(p.View().Decisions(), want.Decisions()) {
			t.Fatal("fixed handoff changed base, prefix, HEAD, or certified suffix")
		}
		seal, decision, hasBase := p.View().RecoveryBase()
		wantSeal, wantDecision, wantBase := want.RecoveryBase()
		if hasBase != wantBase || !reflect.DeepEqual(seal, wantSeal) || !reflect.DeepEqual(decision, wantDecision) || !reflect.DeepEqual(p.View().root, want.root) {
			t.Fatal("fixed handoff changed certified seal, decision, or checkpoint root")
		}
		if after := requestViewBucketHashes(t, ctx, bucket); !reflect.DeepEqual(before, after) {
			t.Fatal("read-only handoff reconstruction mutated source objects")
		}
		m, err := materializer.OpenManagedSQLKVWithAppendOnlyOriginals(filepath.Join(p.Directory(), "sqlite.db"), 1, window)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := m.Close(); err != nil {
				t.Error(err)
			}
		}()
		if m.Tip() != uint64(want.Tip()) {
			t.Fatalf("fixed H tip=%d want=%d", m.Tip(), want.Tip())
		}
		for _, id := range ids {
			if _, found, err := m.RetainedRequestOriginal(ctx, types.MutationKV, id); err != nil || !found {
				t.Fatalf("retained original %q missing: found=%t err=%v", id, found, err)
			}
		}
	}
	write("before-checkpoint")
	noCheckpoint := open()
	if noCheckpoint.Base() != 0 {
		t.Fatal("checkpointless fixture unexpectedly has a base")
	}
	noCheckpointHandoff := publish(noCheckpoint)
	l := checkpointNow()
	write("suffix")
	view := open()
	// Keep these as independent subtests: the unpatched reader must fail the
	// checkpoint assertion, while checkpointless H remains separately exercised.
	t.Run("select-certified-current-with-suffix", func(t *testing.T) {
		seal, d, ok := view.RecoveryBase()
		if !ok || uint64(view.Base()) != l.Index || seal.RootHash != l.RootHash || uint64(d.Slot) != l.SealDecisionSlot || view.Tip() != core.Tip() || len(view.Decisions()) == 0 {
			t.Fatalf("CURRENT certified base/suffix lost: base=%d tip=%d certified=%t", view.Base(), view.Tip(), ok)
		}
	})
	selectedHandoff := publish(view)
	write("after-H")
	latest := checkpointNow()
	before := requestViewBucketHashes(t, ctx, bucket)
	t.Run("checkpointless-H-survives-later-current", func(t *testing.T) {
		load(t, noCheckpointHandoff, noCheckpoint, "before-checkpoint")
	})
	t.Run("selected-H-survives-later-current", func(t *testing.T) {
		load(t, selectedHandoff, view, "before-checkpoint", "suffix")
	})
	if after := requestViewBucketHashes(t, ctx, bucket); !reflect.DeepEqual(before, after) {
		t.Fatal("read-only reconstruction mutated source objects")
	}
	if err := archive.loadHead(ctx, make(map[extentObject]Extent), true); err != nil {
		t.Fatal(err)
	}
	if archive.head.Base != 0 || core.CompactionFloor() != 0 {
		t.Fatal("read-only checkpoint selection trimmed archive or producer WAL")
	}
	t.Run("captured-checkpointless-H-does-not-import-J", func(t *testing.T) {
		before := requestViewBucketHashes(t, ctx, bucket)
		v, err := OpenReadOnlyViewAtHeadForApplicationProtocol(ctx, readonly, prefix, 1, bootstrap, noCheckpoint.CapturedHead(), policy, window, protocol, false)
		if err != nil {
			t.Fatal(err)
		}
		if v.Base() != 0 || v.Tip() != noCheckpoint.Tip() || !bytes.Equal(v.CapturedHead(), noCheckpoint.CapturedHead()) || v.prefixHash != noCheckpoint.prefixHash {
			t.Fatal("future CURRENT changed checkpointless captured H")
		}
		if after := requestViewBucketHashes(t, ctx, bucket); !reflect.DeepEqual(before, after) {
			t.Fatal("captured HEAD reader mutated source objects")
		}
	})
	t.Run("pinned-restore-excludes-unpublished-certified-tail", func(t *testing.T) {
		published := open()
		if published.Tip() != core.Tip() {
			t.Fatalf("published cut tip=%d, want producer certified tip=%d", published.Tip(), core.Tip())
		}
		before := requestViewBucketHashes(t, ctx, bucket)
		const tail = "unpublished-tail"
		value, err := types.EncodeManagedKVCommand(types.KVCommand{RequestID: tail, Operation: "put", Key: tail, Value: []byte(tail)})
		if err != nil {
			t.Fatal(err)
		}
		decision := proposeLocal(value)
		if err := core.EnsureDurableThrough(ctx, decision.Slot); err != nil {
			t.Fatal(err)
		}
		if decision.Slot <= published.Tip() || core.Tip() != decision.Slot || producer.Tip() != uint64(decision.Slot) {
			t.Fatal("fixture did not certify and apply a later local write")
		}
		if got, found, err := producer.KVGet(ctx, tail, time.Now()); err != nil || !found || string(got) != tail {
			t.Fatalf("producer tail missing: found=%t err=%v", found, err)
		}
		if after := requestViewBucketHashes(t, ctx, bucket); !reflect.DeepEqual(before, after) {
			t.Fatal("local tail unexpectedly changed published objects")
		}
		fixed, err := OpenReadOnlyViewAtHeadForApplicationProtocol(ctx, readonly, prefix, 1, bootstrap, published.CapturedHead(), policy, window, protocol, false)
		if err != nil {
			t.Fatal(err)
		}
		restored, err := PrepareReadOnlyRestoreForPolicy(ctx, fixed, filepath.Join(t.TempDir(), "restore"), policy, window)
		if err != nil {
			t.Fatal(err)
		}
		if restored.Tip != published.Tip() || restored.HeadHash != published.HeadHash() || restored.PrefixHash != published.PrefixHash() {
			t.Fatal("restore did not preserve the exact latest published cut")
		}
		// Inspect only the isolated materializer; do not open a voter/runtime.
		m, err := materializer.OpenManagedSQLKVWithAppendOnlyOriginals(restored.SQLitePath, 1, window)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := m.Close(); err != nil {
				t.Error(err)
			}
		})
		for _, key := range []string{"suffix", "after-H", tail} {
			want := key != tail
			got, found, err := m.KVGet(ctx, key, time.Now())
			if err != nil || found != want || (want && string(got) != key) {
				t.Fatalf("restored key %q: found=%t want=%t err=%v", key, found, want, err)
			}
			original, found, err := m.RetainedRequestOriginal(ctx, types.MutationKV, key)
			if err != nil || found != want {
				t.Fatalf("restored original %q: found=%t want=%t err=%v", key, found, want, err)
			}
			if want {
				local, found, err := producer.RetainedRequestOriginal(ctx, types.MutationKV, key)
				if err != nil || !found || !reflect.DeepEqual(original, local) {
					t.Fatalf("restored original/receipt %q differs from producer: found=%t err=%v", key, found, err)
				}
			}
		}
		if after := requestViewBucketHashes(t, ctx, bucket); !reflect.DeepEqual(before, after) {
			t.Fatal("read-only restore changed published objects")
		}
	})
	for _, tc := range []struct {
		name   string
		change func(*checkpoint.ManagedAppendOnlyLocator)
	}{
		{"wrong-policy", func(l *checkpoint.ManagedAppendOnlyLocator) { l.Policy = 10 }},
		{"wrong-seal-hash", func(l *checkpoint.ManagedAppendOnlyLocator) { l.SealDecisionHash[0] ^= 1 }},
		{"wrong-certified-root", func(l *checkpoint.ManagedAppendOnlyLocator) { l.RootHash[0] ^= 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := latest
			tc.change(&bad)
			data, err := json.Marshal(bad)
			if err != nil {
				t.Fatal(err)
			}
			if err := bucket.Upload(ctx, prefix+"/checkpoint/CURRENT", bytes.NewReader(data)); err != nil {
				t.Fatal(err)
			}
			before := requestViewBucketHashes(t, ctx, bucket)
			if _, err := OpenReadOnlyViewForApplicationProtocol(ctx, readonly, prefix, 1, bootstrap, policy, window, protocol, false); err == nil {
				t.Fatal("untrusted CURRENT accepted without exact policy/seal/root authentication")
			}
			if after := requestViewBucketHashes(t, ctx, bucket); !reflect.DeepEqual(before, after) {
				t.Fatal("failed read-only verification mutated source")
			}
		})
	}
}

func requestViewBucketHashes(t *testing.T, ctx context.Context, bucket objstore.Bucket) map[string][32]byte {
	t.Helper()
	objects := make(map[string][32]byte)
	if err := bucket.Iter(ctx, "", func(name string) error {
		r, err := bucket.Get(ctx, name)
		if err != nil {
			return err
		}
		data, readErr := io.ReadAll(r)
		closeErr := r.Close()
		if readErr != nil {
			return readErr
		}
		if closeErr != nil {
			return closeErr
		}
		objects[name] = sha256.Sum256(data)
		return nil
	}, objstore.WithRecursiveIter()); err != nil {
		t.Fatal(err)
	}
	return objects
}
