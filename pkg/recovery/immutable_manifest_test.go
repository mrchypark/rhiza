package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

type immutableCountingBucket struct {
	objstore.Bucket
	uploads int
}

func (b *immutableCountingBucket) Upload(ctx context.Context, name string, data io.Reader, options ...objstore.ObjectUploadOption) error {
	b.uploads++
	return b.Bucket.Upload(ctx, name, data, options...)
}

func immutableManifestFixture(t *testing.T) (context.Context, *objstore.InMemBucket, ImmutableArchiveManifest, []Extent, []quepaxa.DecidedValue) {
	t.Helper()
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	wal, err := qlog.Open(filepath.Join(t.TempDir(), "source"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := wal.Close(); err != nil {
			t.Errorf("close source WAL: %v", err)
		}
	})
	cluster := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "source"}}}
	core, err := quepaxa.New(quepaxa.Config{NodeID: "source", Cluster: cluster, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	for _, command := range []types.KVCommand{
		{RequestID: "immutable-1", Operation: "put", Key: "one", Value: []byte("certified one")},
		{RequestID: "immutable-2", Operation: "put", Key: "two", Value: []byte("certified two")},
	} {
		value, err := types.EncodeKVCommand(command)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := core.Propose(ctx, value); err != nil {
			t.Fatal(err)
		}
	}
	archive := NewManager(bucket, "ordinary", 1)
	t.Cleanup(archive.Close)
	if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	extents := make([]Extent, 0, len(archive.extents))
	for _, ref := range archive.extents {
		extent, err := archive.extentForRef(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		extents = append(extents, extent)
	}
	values, tip, err := archive.DecisionsFrom(ctx, 1, 8)
	if err != nil || tip != core.Tip() {
		t.Fatalf("read source decisions: tip=%d err=%v", tip, err)
	}
	prefix, ok := core.PrefixHash(core.Tip())
	if !ok {
		t.Fatal("source tip has no prefix")
	}
	manifest := ImmutableArchiveManifest{
		ConfigID: 1, InstalledPolicyHash: sha256.Sum256([]byte("approved-installed-policy")), ProfileHash: sha256.Sum256([]byte("approved-profile")),
		WriterIdentityHash: sha256.Sum256([]byte("canonical hash of validated writer identity")),
		Tip:                core.Tip(), TipPrefix: prefix,
	}
	return ctx, bucket, manifest, extents, values
}

func TestImmutableArchiveManifestCreateOnlyReadAndCertifiedReplay(t *testing.T) {
	ctx, bucket, manifest, extents, want := immutableManifestFixture(t)
	hash, err := WriteImmutableArchiveManifest(ctx, bucket, "approved/namespace", manifest, extents)
	if err != nil {
		t.Fatal(err)
	}
	// Repeating the exact publication must settle via create-only conflict and
	// exact readback, not overwrite.
	if second, err := WriteImmutableArchiveManifest(ctx, bucket, "approved/namespace", manifest, extents); err != nil || second != hash {
		t.Fatalf("idempotent immutable publish hash=%x err=%v", second, err)
	}
	selection := ImmutableArchiveSelection{InstalledPolicyHash: manifest.InstalledPolicyHash, ProfileHash: manifest.ProfileHash, WriterIdentityHash: manifest.WriterIdentityHash}
	view, err := ReadImmutableArchiveManifest(ctx, bucket, "approved/namespace", selection, hash)
	if err != nil {
		t.Fatal(err)
	}
	if got := view.Manifest(); got.Tip != manifest.Tip || got.TipPrefix != manifest.TipPrefix || len(got.Extents) != len(extents) {
		t.Fatalf("manifest readback identity differs: tip=%d refs=%d", got.Tip, len(got.Extents))
	}
	var replayed []quepaxa.DecidedValue
	for i := range view.refs {
		extent, err := view.ReadExtent(ctx, i)
		if err != nil {
			t.Fatal(err)
		}
		replayed = append(replayed, extent.Decisions...)
	}
	wal, err := qlog.Open(filepath.Join(t.TempDir(), "observer"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.NewObserver(quepaxa.Config{NodeID: "observer", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "source"}}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	err = ReplayCertifiedThrough(ctx, core, manifest.Tip, func(_ context.Context, from quepaxa.Slot, limit int) ([]quepaxa.DecidedValue, quepaxa.Slot, error) {
		if from == 0 || int(from-1) >= len(replayed) {
			return nil, manifest.Tip, nil
		}
		end := int(from-1) + limit
		if end > len(replayed) {
			end = len(replayed)
		}
		return replayed[int(from-1):end], manifest.Tip, nil
	})
	if err != nil {
		t.Fatalf("actual certified replay failed: %v", err)
	}
	if core.Tip() != manifest.Tip || len(want) != len(replayed) {
		t.Fatalf("observer tip=%d replayed=%d want=%d", core.Tip(), len(replayed), len(want))
	}
}

func TestImmutableArchiveManifestRejectsOversizedExtentListBeforeUpload(t *testing.T) {
	ctx, baseBucket, manifest, extents, _ := immutableManifestFixture(t)
	if len(extents) == 0 {
		t.Fatal("fixture must contain at least one extent")
	}
	bucket := &immutableCountingBucket{Bucket: baseBucket}
	zeroPolicy := manifest
	zeroPolicy.InstalledPolicyHash = [32]byte{}
	if _, err := WriteImmutableArchiveManifest(ctx, bucket, "zero-policy", zeroPolicy, extents); err == nil {
		t.Fatal("zero installed-policy hash was accepted for publication")
	}
	if bucket.uploads != 0 {
		t.Fatalf("zero installed-policy hash caused %d provider uploads, want zero", bucket.uploads)
	}
	oversized := make([]Extent, maxExtentItems+1)
	for i := range oversized {
		oversized[i] = extents[0]
	}
	if _, err := WriteImmutableArchiveManifest(ctx, bucket, "oversized", manifest, oversized); err == nil {
		t.Fatal("oversized extent list was accepted")
	}
	if bucket.uploads != 0 {
		t.Fatalf("invalid oversized input caused %d provider uploads, want zero", bucket.uploads)
	}

	if _, err := WriteImmutableArchiveManifest(ctx, bucket, "valid-neighbor", manifest, extents); err != nil {
		t.Fatalf("valid publication after rejected input failed: %v", err)
	}
	wantUploads := len(extents) + 1 // each extent plus the manifest
	if bucket.uploads != wantUploads {
		t.Fatalf("valid publication uploads=%d, want %d", bucket.uploads, wantUploads)
	}
}

func TestPrepareImmutableExtentsReturnsReferencesNotEncodedBodies(t *testing.T) {
	_, _, manifest, extents, _ := immutableManifestFixture(t)
	refs, err := prepareImmutableExtents(manifest.ConfigID, manifest.Base, manifest.Tip, manifest.TipPrefix, extents)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != len(extents) {
		t.Fatalf("prepared references=%d, want %d", len(refs), len(extents))
	}
	for i := range refs {
		encoded, err := encodeExtent(extents[i])
		if err != nil {
			t.Fatal(err)
		}
		if refs[i].Object != uint64(i+1) || refs[i].Hash != sha256.Sum256(encoded) {
			t.Fatalf("prepared reference %d does not match its encoded extent", i)
		}
	}
}

func TestImmutableArchiveManifestRejectsWrongSelectionAndCorruptExtent(t *testing.T) {
	ctx, bucket, manifest, extents, _ := immutableManifestFixture(t)
	hash, err := WriteImmutableArchiveManifest(ctx, bucket, "approved/namespace", manifest, extents)
	if err != nil {
		t.Fatal(err)
	}
	selection := ImmutableArchiveSelection{InstalledPolicyHash: manifest.InstalledPolicyHash, ProfileHash: manifest.ProfileHash, WriterIdentityHash: manifest.WriterIdentityHash}
	if _, err := ReadImmutableArchiveManifest(ctx, bucket, "approved/namespace", selection, sha256.Sum256([]byte("wrong hash"))); err == nil {
		t.Fatal("wrong expected hash unexpectedly selected a manifest")
	}
	wrongSelection := selection
	wrongSelection.InstalledPolicyHash[0] ^= 1
	if _, err := ReadImmutableArchiveManifest(ctx, bucket, "approved/namespace", wrongSelection, hash); err == nil {
		t.Fatal("manifest was selected under a different installed-policy scope")
	}
	wrongSelection = selection
	wrongSelection.ProfileHash[0] ^= 1
	if _, err := ReadImmutableArchiveManifest(ctx, bucket, "approved/namespace", wrongSelection, hash); err == nil {
		t.Fatal("manifest was selected under a different profile scope")
	}
	zeroPolicy := selection
	zeroPolicy.InstalledPolicyHash = [32]byte{}
	if _, err := ReadImmutableArchiveManifest(ctx, bucket, "approved/namespace", zeroPolicy, hash); err == nil {
		t.Fatal("zero installed-policy hash was accepted for selection")
	}
	view, err := ReadImmutableArchiveManifest(ctx, bucket, "approved/namespace", selection, hash)
	if err != nil {
		t.Fatal(err)
	}
	ref := view.refs[0]
	key := immutableExtentKey("approved/namespace", selection, ref.Object, ref.Hash)
	if err := bucket.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadImmutableArchiveManifest(ctx, bucket, "approved/namespace", selection, hash); err == nil {
		t.Fatal("missing extent unexpectedly passed manifest loading")
	}
	data, err := encodeExtent(extents[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := bucket.Upload(ctx, key, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if err := bucket.Upload(ctx, key, bytes.NewReader([]byte("corrupt"))); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadImmutableArchiveManifest(ctx, bucket, "approved/namespace", selection, hash); err == nil {
		t.Fatal("corrupt extent unexpectedly passed hash validation")
	}
}

func TestImmutableArchiveScopeUsesPolicyBoundProfileAndWriterPaths(t *testing.T) {
	_, _, manifest, _, _ := immutableManifestFixture(t)
	selection := ImmutableArchiveSelection{InstalledPolicyHash: manifest.InstalledPolicyHash, ProfileHash: manifest.ProfileHash, WriterIdentityHash: manifest.WriterIdentityHash}
	scope, err := immutableScopePath(selection)
	if err != nil {
		t.Fatal(err)
	}
	want := "profile-" + fmt.Sprintf("%x", manifest.ProfileHash) + "/writer-" + fmt.Sprintf("%x", manifest.WriterIdentityHash)
	if scope != want {
		t.Fatalf("scope=%q, want %q", scope, want)
	}
	changedPolicy := selection
	changedPolicy.InstalledPolicyHash[0] ^= 1
	changedScope, err := immutableScopePath(changedPolicy)
	if err != nil {
		t.Fatal(err)
	}
	if changedScope != scope {
		t.Fatalf("installed-policy binding unexpectedly changed derived path: got %q want %q", changedScope, scope)
	}
}

func TestImmutableArchiveManifestRejectsDifferentBytesAtCreateOnlyKey(t *testing.T) {
	ctx, bucket, manifest, extents, _ := immutableManifestFixture(t)
	data, err := encodeExtent(extents[0])
	if err != nil {
		t.Fatal(err)
	}
	selection := ImmutableArchiveSelection{InstalledPolicyHash: manifest.InstalledPolicyHash, ProfileHash: manifest.ProfileHash, WriterIdentityHash: manifest.WriterIdentityHash}
	key := immutableExtentKey("conflict", selection, 1, sha256.Sum256(data))
	if err := bucket.Upload(ctx, key, bytes.NewReader([]byte("different bytes"))); err != nil {
		t.Fatal(err)
	}
	if _, err := WriteImmutableArchiveManifest(ctx, bucket, "conflict", manifest, extents); err == nil {
		t.Fatal("create-only conflict with different stored bytes was accepted")
	}
}

func TestImmutableArchiveManifestCheckpointReferenceUsesActualCheckpointVerifier(t *testing.T) {
	ctx := context.Background()
	fixture := newCertifiedBaseFixture(t)
	anchor := fixture.anchor
	rootHash := mustGenerationHash(anchor.Checkpoint.RootHash)
	stateHash := mustGenerationHash(anchor.Checkpoint.StateHash)
	manifest := ImmutableArchiveManifest{
		ConfigID: 1, InstalledPolicyHash: sha256.Sum256([]byte("target-installed-policy")), ProfileHash: sha256.Sum256([]byte("target-generation-profile")),
		WriterIdentityHash: sha256.Sum256([]byte("canonical hash of validated target writer identity")),
		Base: ImmutableArchiveBase{
			Index: quepaxa.Slot(anchor.SourceTip), PrefixHash: mustGenerationHash(anchor.SourcePrefixHash),
			AnchorHash: fixture.anchorHash,
			Checkpoint: &ImmutableCheckpointRef{Index: anchor.Checkpoint.Index, RootHash: rootHash, StateHash: stateHash},
		},
		Tip: quepaxa.Slot(anchor.SourceTip), TipPrefix: mustGenerationHash(anchor.SourcePrefixHash),
	}
	hash, err := WriteImmutableArchiveManifest(ctx, fixture.bucket, "target-generation", manifest, nil)
	if err != nil {
		t.Fatal(err)
	}
	selection := ImmutableArchiveSelection{InstalledPolicyHash: manifest.InstalledPolicyHash, ProfileHash: manifest.ProfileHash, WriterIdentityHash: manifest.WriterIdentityHash}
	view, err := ReadImmutableArchiveManifest(ctx, fixture.bucket, "target-generation", selection, hash)
	if err != nil {
		t.Fatal(err)
	}
	selected := view.Manifest().Base.Checkpoint
	if selected == nil {
		t.Fatal("manifest lost exact checkpoint reference")
	}
	root, err := fixture.checkpoints.OpenRoot(ctx, selected.Index, selected.RootHash)
	if err != nil {
		t.Fatalf("actual OpenRoot failed: %v", err)
	}
	if err := fixture.checkpoints.Verify(ctx, selected.Index, selected.RootHash, selected.StateHash); err != nil {
		t.Fatalf("actual checkpoint Verify failed: %v", err)
	}
	if root.Hash != selected.StateHash {
		t.Fatal("opened checkpoint state hash differs from immutable reference")
	}
}

func TestImmutableArchiveManifestRejectsGapPrefixAndUnsupportedBase(t *testing.T) {
	ctx, bucket, manifest, extents, _ := immutableManifestFixture(t)
	gap := append([]Extent(nil), extents...)
	gap[0].Start++
	if _, err := WriteImmutableArchiveManifest(ctx, bucket, "gap", manifest, gap); err == nil {
		t.Fatal("gapped extent was accepted")
	}
	wrongPrefix := append([]Extent(nil), extents...)
	wrongPrefix[0].EndPrefix[0] ^= 1
	if _, err := WriteImmutableArchiveManifest(ctx, bucket, "prefix", manifest, wrongPrefix); err == nil {
		t.Fatal("wrong extent prefix was accepted")
	}
	unsupported := manifest
	unsupported.Base = ImmutableArchiveBase{Index: 1, PrefixHash: [32]byte{1}}
	if _, err := WriteImmutableArchiveManifest(ctx, bucket, "base", unsupported, extents); err == nil {
		t.Fatal("base without exact checkpoint reference was accepted")
	}
	if _, err := WriteImmutableArchiveManifest(ctx, bucket, "../escape", manifest, extents); err == nil {
		t.Fatal("namespace traversal was accepted")
	}
}

func TestImmutableArchiveReaderRejectsHashValidExtentPrefixMismatch(t *testing.T) {
	ctx, bucket, manifest, extents, _ := immutableManifestFixture(t)
	if len(extents) != 1 {
		t.Fatalf("fixture extents=%d, want one", len(extents))
	}
	forged := extents[0]
	forged.EndPrefix[0] ^= 0xff
	data, err := encodeExtent(extents[0])
	if err != nil {
		t.Fatal(err)
	}
	copy(data[72:104], forged.EndPrefix[:])
	binary.BigEndian.PutUint32(data[len(data)-archiveCRCSize:], crc32.Checksum(data[:len(data)-archiveCRCSize], archiveCRCTable))

	selection := ImmutableArchiveSelection{InstalledPolicyHash: manifest.InstalledPolicyHash, ProfileHash: manifest.ProfileHash, WriterIdentityHash: manifest.WriterIdentityHash}
	hash := sha256.Sum256(data)
	ref := ImmutableExtentRef{
		Object: 1, Hash: hash, ConfigID: forged.ConfigID, Start: forged.Start, End: forged.End,
		StartPrefix: forged.StartPrefix, EndPrefix: forged.EndPrefix,
		PreviousHash: forged.PreviousHash, PreviousObject: forged.PreviousObject,
	}
	manifest.Tip = forged.End
	manifest.TipPrefix = forged.EndPrefix
	manifest.Extents = []ImmutableExtentRef{ref}
	manifestData, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestHash := sha256.Sum256(manifestData)
	if err := bucket.Upload(ctx, immutableExtentKey("forged-prefix", selection, ref.Object, ref.Hash), bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if err := bucket.Upload(ctx, immutableManifestKey("forged-prefix", selection, manifestHash), bytes.NewReader(manifestData)); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadImmutableArchiveManifest(ctx, bucket, "forged-prefix", selection, manifestHash); err == nil {
		t.Fatal("reader accepted a hash-valid extent whose decisions compute a different declared end prefix")
	}
}

func TestImmutableManifestCopyDeepClonesCheckpointMembership(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	initial := []quepaxa.Member{{ID: "a"}, {ID: "b"}, {ID: "c"}}
	transport := newArchiveTestCluster(t, initial)
	core := transport.cores["a"]
	if _, err := core.BeginReconfiguration(ctx, quepaxa.Cluster{ConfigID: 2, Members: initial[:2]}); err != nil {
		t.Fatalf("finishable membership transition begin: %v", err)
	}
	if err := core.FinishReconfiguration(ctx); err != nil {
		t.Fatalf("membership transition finish: %v", err)
	}
	current := core.CurrentCluster()
	abortedTarget := quepaxa.Cluster{ConfigID: current.ConfigID + 1, Members: append(append([]quepaxa.Member(nil), current.Members...), quepaxa.Member{ID: "lost", WALIdentity: "0000000000000000000000000000000000000000000000000000000000000000"})}
	freeze, err := core.BeginReconfiguration(ctx, abortedTarget)
	if err != nil {
		t.Fatalf("abortable membership transition begin: %v", err)
	}
	if err := core.AbortReconfiguration(ctx); err != nil {
		t.Fatalf("membership transition abort: %v", err)
	}
	for core.Tip() < 82 {
		if _, _, err := core.Propose(ctx, []byte("after abort")); err != nil {
			t.Fatalf("post-abort decision: %v", err)
		}
	}
	index := core.Tip()
	membership, err := core.CheckpointMembership(index)
	if err != nil {
		t.Fatal(err)
	}
	if len(membership.Transitions) != 1 || membership.Abort == nil || membership.Abort.Freeze.Slot != freeze {
		t.Fatalf("checkpoint membership did not retain both transition and abort: transitions=%d abort=%+v", len(membership.Transitions), membership.Abort)
	}

	bucket := objstore.NewInMemBucket()
	checkpointManager := checkpoint.NewManager(bucket, "checkpoint", t.TempDir(), 1)
	checkpointFile := filepath.Join(t.TempDir(), "state")
	policySnapshot(t, checkpointFile)
	claim, err := checkpointManager.AcquirePublisherClaim(ctx, "clone-test", 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	root, err := checkpointManager.CreateFiles(ctx, claim, []checkpoint.Source{{Role: checkpoint.RoleSQLite, Path: checkpointFile}}, uint64(index))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkpointManager.ReleasePublisherClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	prefix, ok := core.PrefixHash(index)
	if !ok {
		t.Fatal("checkpoint prefix unavailable")
	}
	nextOrder, followingOrder, err := core.CheckpointLeaderOrders(index)
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{
		ConfigID: core.ConfigID(), Index: index, RootHash: root.RootHash, StateHash: root.Hash,
		PrefixHash: prefix, NextLeaderOrder: nextOrder, FollowingLeaderOrder: followingOrder, Membership: &membership,
	}
	checkpointManagerVerify := func(_ context.Context, candidate quepaxa.CheckpointSeal) error {
		return checkpointManager.Verify(ctx, uint64(candidate.Index), candidate.RootHash, candidate.StateHash)
	}
	for _, id := range current.Members {
		member := transport.cores[id.ID]
		member.SetCheckpointValidator(checkpointManagerVerify)
		if err := member.PrepareCheckpoint(ctx, seal); err != nil {
			t.Fatalf("prepare actual membership checkpoint on %s: %v", id.ID, err)
		}
	}
	encoded, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, encoded); err != nil {
		t.Fatalf("certify actual membership checkpoint: %v", err)
	}
	sealed, exists, err := core.LatestCheckpointSeal()
	if err != nil || !exists || sealed.Membership == nil {
		t.Fatalf("checkpoint seal=%v membership=%v err=%v", exists, sealed.Membership != nil, err)
	}
	decision, ok := core.CertifiedValue(sealed.DecisionSlot)
	if !ok {
		t.Fatal("checkpoint seal certificate unavailable")
	}

	manifest := ImmutableArchiveManifest{
		ConfigID: core.ConfigID(), InstalledPolicyHash: sha256.Sum256([]byte("clone-installed-policy")), ProfileHash: sha256.Sum256([]byte("clone-profile")),
		WriterIdentityHash: sha256.Sum256([]byte("validated writer identity")),
		Base: ImmutableArchiveBase{
			Index: index, PrefixHash: prefix, Seal: &sealed.CheckpointSeal, Decision: &decision,
			Checkpoint: &ImmutableCheckpointRef{Index: uint64(index), RootHash: root.RootHash, StateHash: root.Hash},
		},
		Tip: index, TipPrefix: prefix,
	}
	hash, err := WriteImmutableArchiveManifest(ctx, bucket, "clone/namespace", manifest, nil)
	if err != nil {
		t.Fatalf("write immutable manifest with certified history base: %v", err)
	}
	selection := ImmutableArchiveSelection{InstalledPolicyHash: manifest.InstalledPolicyHash, ProfileHash: manifest.ProfileHash, WriterIdentityHash: manifest.WriterIdentityHash}
	view, err := ReadImmutableArchiveManifest(ctx, bucket, "clone/namespace", selection, hash)
	if err != nil {
		t.Fatalf("read immutable manifest with certified history base: %v", err)
	}
	original, err := json.Marshal(view.Manifest())
	if err != nil {
		t.Fatal(err)
	}
	untouchedCopy := view.Manifest()
	canonicalBytes, err := readImmutableObject(ctx, bucket, immutableManifestKey("clone/namespace", selection, hash), maxImmutableManifestSize)
	if err != nil {
		t.Fatal(err)
	}
	mutated := view.Manifest()
	mutated.Base.Seal.Membership.Genesis.Members[0].ID = "changed-genesis"
	mutated.Base.Seal.Membership.Transitions[0].Freeze.Value[0] ^= 0xff
	mutated.Base.Seal.Membership.Transitions[0].Freeze.Certificate[0] ^= 0xff
	mutated.Base.Seal.Membership.Transitions[0].Terminal.Value[0] ^= 0xff
	mutated.Base.Seal.Membership.Transitions[0].Terminal.Certificate[0] ^= 0xff
	mutated.Base.Seal.Membership.Abort.Freeze.Value[0] ^= 0xff
	mutated.Base.Seal.Membership.Abort.Freeze.Certificate[0] ^= 0xff
	mutated.Base.Seal.Membership.Abort.Terminal.Value[0] ^= 0xff
	mutated.Base.Seal.Membership.Abort.Terminal.Certificate[0] ^= 0xff
	mutated.Base.Seal.NextLeaderOrder[0] = "changed-leader"
	mutated.Base.Seal.FollowingLeaderOrder[0] = "changed-next-leader"
	got, err := json.Marshal(view.Manifest())
	if err != nil || !bytes.Equal(got, original) {
		t.Fatal("mutating one returned manifest copy changed a later copy")
	}
	otherCopy, err := json.Marshal(untouchedCopy)
	if err != nil || !bytes.Equal(otherCopy, original) {
		t.Fatal("mutating one returned manifest copy changed another outstanding copy")
	}
	if stored, err := readImmutableObject(ctx, bucket, immutableManifestKey("clone/namespace", selection, hash), maxImmutableManifestSize); err != nil || !bytes.Equal(stored, canonicalBytes) {
		t.Fatalf("stored canonical manifest changed after copy mutation: err=%v", err)
	}
}

func TestImmutableArchiveReplayRejectsFabricatedCertificate(t *testing.T) {
	ctx, bucket, manifest, extents, _ := immutableManifestFixture(t)
	extents[0].Decisions[0].Certificate = []byte(`{"not":"a certificate"}`)
	hash, err := WriteImmutableArchiveManifest(ctx, bucket, "fabricated", manifest, extents)
	if err != nil {
		t.Fatal(err)
	}
	selection := ImmutableArchiveSelection{InstalledPolicyHash: manifest.InstalledPolicyHash, ProfileHash: manifest.ProfileHash, WriterIdentityHash: manifest.WriterIdentityHash}
	view, err := ReadImmutableArchiveManifest(ctx, bucket, "fabricated", selection, hash)
	if err != nil {
		t.Fatal(err)
	}
	extent, err := view.ReadExtent(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := qlog.Open(filepath.Join(t.TempDir(), "observer"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.NewObserver(quepaxa.Config{NodeID: "observer", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "source"}}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	err = ReplayCertifiedThrough(ctx, core, manifest.Tip, func(context.Context, quepaxa.Slot, int) ([]quepaxa.DecidedValue, quepaxa.Slot, error) {
		return extent.Decisions, manifest.Tip, nil
	})
	if err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("fabricated certificate replay error=%v, want certificate rejection", err)
	}
}
