package checkpoint

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/thanos-io/objstore"
)

func TestManagedNoPVCRequestAppendOnlyLoadReadsExactLocator(t *testing.T) {
	ctx := context.Background()
	policy := sqlpolicy.ManagedRequestMaterializationPolicy
	bucket := objstore.NewInMemBucket()
	producer, err := materializer.OpenManagedSQLKVWithAppendOnlyOriginals(filepath.Join(t.TempDir(), "request.sqlite"), 1, types.DefaultIdempotencyWindowSlots)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	value, err := types.EncodeManagedKVCommand(types.KVCommand{RequestID: "nopvc-request", Operation: "put", Key: "k", Value: []byte("v")})
	if err != nil {
		t.Fatal(err)
	}
	if err := producer.Apply(ctx, 1, value); err != nil {
		t.Fatal(err)
	}
	files, cut, cleanup, err := producer.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	sources := make([]Source, 0, len(files))
	for _, file := range files {
		sources = append(sources, Source{Role: string(file.Role), Path: file.Path})
	}
	writer, err := NewAppendOnlyManagerForPolicy(bucket, "request", t.TempDir(), policy, 1)
	if err != nil {
		t.Fatal(err)
	}
	root, err := writer.CreateManagedAppendOnlyCandidate(ctx, sources, cut)
	if err != nil {
		t.Fatal(err)
	}
	locator := ManagedAppendOnlyLocator{Policy: int(policy), Index: cut, RootHash: root.RootHash, SealDecisionSlot: cut + 1, SealDecisionHash: sha256.Sum256([]byte("certified-seal-hint"))}
	if err := writer.PromoteManagedAppendOnlyLocator(ctx, locator); err != nil {
		t.Fatal(err)
	}
	reader, err := NewAppendOnlyManagerForPolicy(bucket, "request", t.TempDir(), policy, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Load(ctx); err != nil {
		t.Fatal("policy-9 append-only CURRENT must not be decoded as a legacy pointer", err)
	}
	loaded := reader.Latest()
	if loaded == nil || loaded.Index != cut || loaded.Generation != cut || loaded.RootHash != root.RootHash {
		t.Fatalf("policy-9 append-only root not loaded exactly: %+v", loaded)
	}

	missing, err := NewAppendOnlyManagerForPolicy(objstore.NewInMemBucket(), "missing", t.TempDir(), policy, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := missing.Load(ctx); err != nil || missing.Latest() != nil {
		t.Fatalf("new policy-9 namespace without CURRENT: latest=%v err=%v", missing.Latest(), err)
	}

	wrongBucket := objstore.NewInMemBucket()
	wrongManager, err := NewAppendOnlyManagerForPolicy(wrongBucket, "wrong", t.TempDir(), policy, 1)
	if err != nil {
		t.Fatal(err)
	}
	wrong := locator
	wrong.Policy = int(sqlpolicy.ManagedGraphRequestMaterializationPolicy)
	wrongBytes, err := json.Marshal(wrong)
	if err != nil {
		t.Fatal(err)
	}
	if err := wrongBucket.Upload(ctx, wrongManager.key("checkpoint/CURRENT"), bytes.NewReader(wrongBytes), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	if err := wrongManager.Load(ctx); err == nil {
		t.Fatal("policy-9 manager accepted policy-10 CURRENT")
	}

	wrongGenerationBucket := objstore.NewInMemBucket()
	wrongGenerationManager, err := NewAppendOnlyManagerForPolicy(wrongGenerationBucket, "generation", t.TempDir(), policy, 1)
	if err != nil {
		t.Fatal(err)
	}
	wrongRoot, err := wrongGenerationManager.uploadCandidate(ctx, sources, cut, cut+1, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	wrongGeneration := ManagedAppendOnlyLocator{Policy: int(policy), Index: cut, RootHash: wrongRoot.RootHash, SealDecisionSlot: cut + 1, SealDecisionHash: sha256.Sum256([]byte("wrong-root-generation"))}
	if err := wrongGenerationManager.PromoteManagedAppendOnlyLocator(ctx, wrongGeneration); err != nil {
		t.Fatal(err)
	}
	if err := wrongGenerationManager.Load(ctx); err == nil {
		t.Fatal("policy-9 manager accepted root generation that differs from cut")
	}

	legacyProfileManager, err := NewManagerForPolicy(objstore.NewInMemBucket(), "legacy-request", t.TempDir(), policy, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := legacyProfileManager.Load(ctx); err == nil {
		t.Fatal("policy-9 legacy manager silently treated final locator as legacy CURRENT")
	}
}
