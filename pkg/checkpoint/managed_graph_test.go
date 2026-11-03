package checkpoint

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/thanos-io/objstore"
)

var errGraphUploadACK = errors.New("test: managed Graph upload ACK lost")

type managedGraphPublicationBucket struct {
	objstore.Bucket
	mu      sync.Mutex
	keys    []string
	deletes int
	lostACK bool
}

func (b *managedGraphPublicationBucket) Upload(ctx context.Context, key string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	b.mu.Lock()
	b.keys = append(b.keys, key)
	lost := b.lostACK
	b.mu.Unlock()
	err := b.Bucket.Upload(ctx, key, r, opts...)
	if err == nil && lost {
		return errGraphUploadACK
	}
	return err
}

func (b *managedGraphPublicationBucket) Delete(ctx context.Context, key string) error {
	b.mu.Lock()
	b.deletes++
	b.mu.Unlock()
	return b.Bucket.Delete(ctx, key)
}

func TestManagedGraphImmutableCandidateAndLocator(t *testing.T) {
	ctx := context.Background()
	m, err := materializer.OpenManagedSQLKVGraphWithOriginals(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	value, err := types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "raw\xff", Cypher: "CREATE (:Candidate {n:1})"})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 1, value); err != nil {
		t.Fatal(err)
	}
	files, cut, release, err := m.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	sources := make([]Source, len(files))
	for i, f := range files {
		sources[i] = Source{Role: string(f.Role), Path: f.Path}
	}
	b := &managedGraphPublicationBucket{Bucket: objstore.NewInMemBucket(), lostACK: true}
	cp, err := NewAppendOnlyManagerForPolicy(b, "graph", t.TempDir(), sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1)
	if err != nil {
		t.Fatal(err)
	}
	root, err := cp.CreateManagedGraphCandidate(ctx, sources, cut)
	if err != nil {
		t.Fatalf("lost immutable upload ACK was not exactly read back: %v", err)
	}
	if root.Generation != cut || root.Index != cut || root.claim != nil {
		t.Fatal("candidate used a claim or non-cut generation")
	}
	if exists, err := b.Exists(ctx, cp.key("checkpoint/CURRENT")); err != nil || exists {
		t.Fatal("unsealed candidate published CURRENT")
	}
	if _, err := cp.CreateFiles(ctx, nil, sources, cut); err == nil {
		t.Fatal("legacy claim requirement bypassed")
	}
	l := ManagedGraphLocator{Policy: 10, Index: cut, RootHash: root.RootHash, SealDecisionSlot: cut + 1, SealDecisionHash: sha256.Sum256([]byte("fixture-seal"))}
	// This tests pointer CAS/readback only. It is deliberately not a certified
	// archive proof; the recovery verifier must reject forged seal hints.
	if err := cp.PromoteManagedGraphLocator(ctx, l); err != nil {
		t.Fatal(err)
	}
	got, exists, err := cp.LoadManagedGraphLocator(ctx)
	if err != nil || !exists || got != l {
		t.Fatalf("locator differs: %+v %v", got, err)
	}
	if err := cp.PromoteManagedGraphLocator(ctx, l); err != nil {
		t.Fatal(err)
	}
	conflict := l
	conflict.SealDecisionHash[0] ^= 1
	if err := cp.PromoteManagedGraphLocator(ctx, conflict); err == nil {
		t.Fatal("same-cut locator conflict accepted")
	}
	bad := l
	bad.SealDecisionSlot = cut
	if err := cp.PromoteManagedGraphLocator(ctx, bad); err == nil {
		t.Fatal("nonpositive certified suffix accepted")
	}
	if err := cp.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if cp.Latest() == nil || cp.Latest().RootHash != root.RootHash {
		t.Fatal("profile locator load lost root")
	}
	for _, key := range b.keys {
		if strings.Contains(key, "PUBLISHER") || strings.Contains(key, "GC_LOCK") || strings.Contains(key, "recovery-pins") {
			t.Fatalf("candidate acquired external maintenance state: %s", key)
		}
	}
	if b.deletes != 0 {
		t.Fatal("append-only publication deleted an object")
	}
	directBucket := &managedGraphPublicationBucket{Bucket: objstore.NewInMemBucket(), lostACK: true}
	directManager, err := NewAppendOnlyManagerForPolicy(directBucket, "direct", t.TempDir(), sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1)
	if err != nil {
		t.Fatal(err)
	}
	appendOnlyRoot, err := directManager.CreateManagedAppendOnlyCandidate(ctx, sources, cut)
	if err != nil || appendOnlyRoot.RootHash != root.RootHash {
		t.Fatalf("direct candidate differs from immutable root: root=%v err=%v", appendOnlyRoot, err)
	}
	direct := ManagedAppendOnlyLocator{Policy: 10, Index: cut, RootHash: root.RootHash, SealDecisionSlot: cut + 1, SealDecisionHash: sha256.Sum256([]byte("direct-seal-1"))}
	if err := directManager.PromoteManagedAppendOnlyLocator(ctx, direct); err != nil {
		t.Fatalf("direct CURRENT lost-ACK readback failed: %v", err)
	}
	loaded, exists, err := directManager.LoadManagedAppendOnlyLocator(ctx, sqlpolicy.ManagedGraphRequestMaterializationPolicy)
	if err != nil || !exists || loaded != direct {
		t.Fatalf("direct locator differs: %+v %v", loaded, err)
	}
	// This is a pointer-CAS unit test only, not certificate evidence. It proves
	// that distinct decision-slot ranks at one cut are ordered without allowing
	// a lower locator to roll the discovery hint back.
	higherSlot := direct
	higherSlot.SealDecisionSlot++
	higherSlot.SealDecisionHash = sha256.Sum256([]byte("direct-seal-2"))
	if err := directManager.PromoteManagedAppendOnlyLocator(ctx, higherSlot); err != nil {
		t.Fatalf("same-cut higher decision slot was rejected: %v", err)
	}
	if err := directManager.PromoteManagedAppendOnlyLocator(ctx, direct); !errors.Is(err, ErrStaleCheckpoint) {
		t.Fatalf("late lower decision slot error=%v, want stale", err)
	}
	corruptRank := higherSlot
	corruptRank.SealDecisionHash[0] ^= 1
	if err := directManager.PromoteManagedAppendOnlyLocator(ctx, corruptRank); err == nil {
		t.Fatal("equal-rank different decision hash was accepted")
	}
	if _, _, err := directManager.LoadManagedAppendOnlyLocator(ctx, sqlpolicy.ManagedRequestMaterializationPolicy); err == nil {
		t.Fatal("locator crossed request and Graph profiles")
	}
	corrupt := []byte("bad")
	key := "graph/checkpoint/blocks/corrupt"
	if err := b.Bucket.Upload(ctx, key, bytes.NewReader(corrupt)); err != nil {
		t.Fatal(err)
	}
	if err := cp.uploadManagedGraphImmutable(ctx, key, strings.NewReader("good"), 4, sha256.Sum256([]byte("good"))); err == nil {
		t.Fatal("conflicting immutable readback accepted")
	}
}
