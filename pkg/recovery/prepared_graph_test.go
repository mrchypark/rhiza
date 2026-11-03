package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

// Reuse the existing real Core/apply/archive fixture and its certified proposal
// helper; only the trusted profile, paired source roles and Graph commands differ.
func newGraphPreparedFixture(t *testing.T) *mixedRecoveryFixture {
	return newGraphPreparedFixtureForMembership(t, true)
}

func newGraphPreparedFixtureForMembership(t *testing.T, reconfiguration bool) *mixedRecoveryFixture {
	t.Helper()
	ctx := context.Background()
	f := &mixedRecoveryFixture{ctx: ctx, bucket: objstore.NewInMemBucket(), window: 1024}
	f.bootstrap = quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "mixed", PublicKey: testPublicKey("mixed")}}}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	f.core, err = quepaxa.New(quepaxa.Config{NodeID: "mixed", Cluster: f.bootstrap, WAL: wal, EnableReconfiguration: reconfiguration, ValidateValue: types.ValidateManagedGraphExecutionPolicy})
	if err != nil {
		t.Fatal(err)
	}
	f.state, err = materializer.OpenManagedSQLKVGraphWithOriginals(filepath.Join(t.TempDir(), "sqlite.db"), 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.state.Close() })
	f.archive, err = NewAppendOnlyManagerWithValueValidator(f.bucket, "mixed", 1, types.ValidateManagedGraphExecutionPolicy)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.archive.Close)
	command := types.GraphCommand{RequestID: "base", Cypher: `CREATE (:Item {id: 'base'})`, Events: []types.GraphStreamEvent{{Stream: "events", Kind: "created", Payload: "base"}}}
	value, err := types.EncodeManagedGraphExecution(command)
	if err != nil {
		t.Fatal(err)
	}
	f.propose(t, value)
	files, index, cleanup, err := f.state.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cleanup)
	f.cp, err = checkpoint.NewAppendOnlyManagerForPolicy(f.bucket, "mixed", "", sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1)
	if err != nil {
		t.Fatal(err)
	}
	var sources []checkpoint.Source
	for _, file := range files {
		sources = append(sources, checkpoint.Source{Role: string(file.Role), Path: file.Path})
	}
	f.root, err = f.cp.CreateManagedGraphCandidate(ctx, sources, index)
	if err != nil {
		t.Fatal(err)
	}
	prefix, ok := f.core.PrefixHash(f.core.Tip())
	if !ok {
		t.Fatal("missing certified prefix")
	}
	next, following, err := f.core.CheckpointLeaderOrders(f.core.Tip())
	if err != nil {
		t.Fatal(err)
	}
	var membership *quepaxa.MembershipRecord
	if reconfiguration {
		history, err := f.core.CheckpointMembership(f.core.Tip())
		if err != nil {
			t.Fatal(err)
		}
		membership = &history
	}
	f.seal = quepaxa.CheckpointSeal{ConfigID: 1, Index: f.core.Tip(), RootHash: f.root.RootHash, StateHash: f.root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following, Membership: membership}
	f.core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
		return f.cp.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
	})
	if err := f.core.PrepareCheckpoint(ctx, f.seal); err != nil {
		t.Fatal(err)
	}
	sealValue, err := quepaxa.EncodeCheckpointSeal(f.seal)
	if err != nil {
		t.Fatal(err)
	}
	f.propose(t, sealValue)
	if err := f.archive.SyncThrough(ctx, f.core, f.core.Tip()); err != nil {
		t.Fatal(err)
	}
	sealed, ok, err := f.core.LatestCheckpointSeal()
	if err != nil || !ok {
		t.Fatal("missing certified seal", err)
	}
	decision, ok := f.core.CertifiedValue(sealed.DecisionSlot)
	if !ok || len(decision.Certificate) == 0 {
		t.Fatal("missing certified checkpoint decision")
	}
	locator := checkpoint.ManagedGraphLocator{Policy: int(sqlpolicy.ManagedGraphRequestMaterializationPolicy), Index: uint64(sealed.Index), RootHash: sealed.RootHash, SealDecisionSlot: uint64(decision.Slot), SealDecisionHash: decision.Hash}
	if err := f.cp.PromoteManagedGraphLocator(ctx, locator); err != nil {
		t.Fatal(err)
	}
	command.RequestID, command.Cypher = "suffix", `CREATE (:Item {id: 'suffix'})`
	command.Events[0].Payload = "suffix"
	values := make([]any, 21800)
	for i := range values {
		values[i] = float64(0.25)
	}
	command.Args = map[string]any{"values": values}
	value, err = types.EncodeManagedGraphExecution(command)
	if err != nil {
		t.Fatal(err)
	}
	if len(value) <= quepaxa.MaxReplicatedValueBytes || len(value) > types.MaxManagedGraphExecutionBytes {
		t.Fatalf("fixture Graph suffix bytes=%d, need actual new-profile-only value", len(value))
	}
	f.propose(t, value)
	if err := f.archive.SyncThrough(ctx, f.core, f.core.Tip()); err != nil {
		t.Fatal(err)
	}
	t.Logf("actual certified Graph suffix: bytes=%d slot=%d checkpoint_base=%d", len(value), f.core.Tip(), f.seal.Index)
	return f
}

func TestManagedGraphPreparedV4PairedRootAndCertifiedSuffix(t *testing.T) {
	f := newGraphPreparedFixture(t)
	legacyReader := NewManager(f.bucket, "mixed", 1)
	defer legacyReader.Close()
	if err := legacyReader.Load(f.ctx); err == nil || !strings.Contains(err.Error(), "invalid archive decision payload size") {
		t.Fatalf("default reader did not reject exact >128KiB payload boundary: %v", err)
	}
	readonly := &haReadOnlyRecordingBucket{Bucket: f.bucket}
	view, err := OpenReadOnlyViewForPolicyWithReconfiguration(f.ctx, readonly, "mixed", 1, f.bootstrap, sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1024, true)
	if err != nil {
		t.Fatal(err)
	}
	if view.Base() == 0 || view.root == nil || len(view.root.Files) != 2 || view.Tip() <= view.Base() || len(view.Decisions()) == 0 {
		t.Fatal("fixture did not supply actual paired base and nonempty certified suffix")
	}
	if f.archive.head.Base != 0 || view.Base() != f.seal.Index {
		t.Fatal("Graph locator changed archive base or failed to select certified cut")
	}
	identity := mixedPreparedIdentity()
	identity.Generation = 0
	handoff := filepath.Join(t.TempDir(), "prepared")
	if err := PublishPreparedStartupForPolicy(f.ctx, view, handoff, identity, sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1024); err != nil {
		_ = filepath.WalkDir(handoff, func(name string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			relative, _ := filepath.Rel(handoff, name)
			info, statErr := entry.Info()
			if statErr == nil {
				t.Logf("owned preparation artifact %q mode=%s size=%d", relative, info.Mode(), info.Size())
			}
			return statErr
		})
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(handoff, "completed.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest preparedStartupManifest
	if err := json.Unmarshal(raw, &manifest); err != nil || manifest.Version != 4 || len(manifest.Files) < 2 {
		t.Fatalf("manifest=%+v err=%v", manifest, err)
	}
	if manifest.Files[0].Role != "sqlite" || manifest.Files[1].Role != "graph-data" {
		t.Fatal("manifest did not preserve exact pair")
	}
	seenPages := false
	for _, artifact := range manifest.Files {
		if artifact.Role == "graph-data" && artifact.Path == "latticedb/graph.ltdb.pages" {
			seenPages = true
			if artifact.Size <= 0 {
				t.Fatal("native page data was not charged")
			}
		}
	}
	if !seenPages {
		t.Fatal("fixture did not capture actual native page data in final artifact inventory")
	}
	t.Logf("v4 actual final artifact files=%d (one SQLite role and opaque native graph-data components)", len(manifest.Files))
	if manifest.GraphLocator == nil {
		t.Fatal("v4 did not bind exact checkpoint locator")
	}
	// A changed live CURRENT must not replace the prepared source's exact cut.
	newer := *manifest.GraphLocator
	newer.SealDecisionSlot = uint64(view.Tip() + 1)
	changedCurrent, err := json.Marshal(newer)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.bucket.Upload(f.ctx, "mixed/checkpoint/CURRENT", bytes.NewReader(changedCurrent)); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "verified")
	verified, err := LoadPreparedStartupForPolicyWithReconfiguration(f.ctx, readonly, "mixed", 1, f.bootstrap, handoff, target, identity, sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1024, true)
	if err != nil {
		t.Fatal(err)
	}
	if verified.Directory() != target || target == handoff {
		t.Fatal("loader selected untrusted handoff instead of independently reconstructed directory")
	}
	actual, err := materializer.OpenManagedSQLKVGraphWithOriginals(filepath.Join(verified.Directory(), "sqlite.db"), 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer actual.Close()
	if actual.Tip() != uint64(view.Tip()) {
		t.Fatal("opened actual pair at wrong tip")
	}
	wantHistory, err := f.state.ManagedGraphHistory(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	gotHistory, err := actual.ManagedGraphHistory(f.ctx)
	if err != nil || gotHistory != wantHistory {
		t.Fatalf("opened history differs: %v", err)
	}
	for _, id := range []string{"base", "suffix"} {
		want, found, err := f.state.RetainedRequestOriginal(f.ctx, types.MutationGraph, id)
		if err != nil || !found {
			t.Fatal("producer original absent", err)
		}
		got, found, err := actual.RetainedRequestOriginal(f.ctx, types.MutationGraph, id)
		if err != nil || !found || !reflect.DeepEqual(got, want) {
			t.Fatalf("restored original %q differs: %v", id, err)
		}
	}
	rows, err := actual.GraphQuery(f.ctx, `MATCH (n:Item) RETURN n.id`, nil)
	if err != nil || len(rows.Rows) != 2 {
		t.Fatalf("paired contents+suffix=%v err=%v", rows.Rows, err)
	}
	events, _, err := actual.GraphReadStream(f.ctx, "events", 0, 10, 0)
	if err != nil || len(events) != 2 {
		t.Fatalf("paired stream+suffix=%v err=%v", events, err)
	}
	if readonly.mutations != 0 {
		t.Fatal("read-only reconstruction mutated source")
	}
	for _, policy := range []sqlpolicy.MaterializationPolicy{7, 8, 9} {
		if _, err := LoadPreparedStartupForPolicy(f.ctx, readonly, "mixed", 1, f.bootstrap, handoff, filepath.Join(t.TempDir(), "wrong"), identity, policy, 1024); err == nil {
			t.Fatalf("v4 loaded under policy%d", policy)
		}
	}
	for _, change := range []struct {
		name  string
		alter func(*preparedStartupManifest)
	}{{"role", func(m *preparedStartupManifest) { m.Files[1].Role = "sqlite" }}, {"path", func(m *preparedStartupManifest) { m.Files[1].Path = "../foreign" }}, {"size", func(m *preparedStartupManifest) { m.Files[1].Size++ }}, {"hash", func(m *preparedStartupManifest) { m.Files[1].SHA256[0] ^= 1 }}, {"locator-missing", func(m *preparedStartupManifest) { m.GraphLocator = nil }}, {"locator-hash", func(m *preparedStartupManifest) { m.GraphLocator.SealDecisionHash[0] ^= 1 }}, {"locator-outside-head", func(m *preparedStartupManifest) { m.GraphLocator.SealDecisionSlot = uint64(view.Tip() + 1) }}} {
		t.Run(change.name, func(t *testing.T) {
			changed := manifest
			changed.Files = append([]preparedGraphArtifact(nil), manifest.Files...)
			if manifest.GraphLocator != nil {
				locator := *manifest.GraphLocator
				changed.GraphLocator = &locator
			}
			change.alter(&changed)
			encoded, err := json.Marshal(changed)
			if err != nil {
				t.Fatal(err)
			}
			marker := filepath.Join(handoff, "completed.json")
			if err := os.WriteFile(marker, encoded, 0600); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := os.WriteFile(marker, raw, 0600); err != nil {
					t.Error(err)
				}
			}()
			if _, err := LoadPreparedStartupForPolicyWithReconfiguration(f.ctx, readonly, "mixed", 1, f.bootstrap, handoff, filepath.Join(t.TempDir(), "rejected"), identity, sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1024, true); err == nil {
				t.Fatal("altered descriptor accepted")
			}
		})
	}
	t.Run("native-page-bytes", func(t *testing.T) {
		name := filepath.Join(handoff, "latticedb", "graph.ltdb.pages")
		original, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		changed := bytes.Clone(original)
		changed[len(changed)/2] ^= 1
		if err := os.WriteFile(name, changed, 0600); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := os.WriteFile(name, original, 0600); err != nil {
				t.Error(err)
			}
		}()
		if _, err := LoadPreparedStartupForPolicyWithReconfiguration(f.ctx, readonly, "mixed", 1, f.bootstrap, handoff, filepath.Join(t.TempDir(), "rejected"), identity, sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1024, true); err == nil {
			t.Fatal("native page hash corruption accepted")
		}
	})
	t.Run("native-symlink", func(t *testing.T) {
		name := filepath.Join(handoff, "latticedb", "graph.ltdb")
		saved := filepath.Join(t.TempDir(), "saved-native")
		if err := os.Rename(name, saved); err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = os.Remove(name)
			if err := os.Rename(saved, name); err != nil {
				t.Error(err)
			}
		}()
		if err := os.Symlink(saved, name); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadPreparedStartupForPolicyWithReconfiguration(f.ctx, readonly, "mixed", 1, f.bootstrap, handoff, filepath.Join(t.TempDir(), "rejected"), identity, sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1024, true); err == nil {
			t.Fatal("native symlink accepted")
		}
	})
}

func TestManagedGraphReadOnlyLocatorCapturedCut(t *testing.T) {
	f := newGraphPreparedFixture(t)
	view, err := OpenReadOnlyViewForPolicyWithReconfiguration(f.ctx, f.bucket, "mixed", 1, f.bootstrap, sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1024, true)
	if err != nil {
		t.Fatal(err)
	}
	locator, found, err := f.cp.LoadManagedGraphLocator(f.ctx)
	if err != nil || !found {
		t.Fatal("actual locator missing", err)
	}
	if f.archive.head.Base != 0 || view.Base() != quepaxa.Slot(locator.Index) || view.Tip() != f.core.Tip() {
		t.Fatal("captured cut did not preserve archive base")
	}
	values := view.Decisions()
	if len(values) == 0 || values[0].Slot != view.Base()+1 || values[len(values)-1].Slot != view.Tip() {
		t.Fatal("captured suffix is not C+1..HEAD")
	}
	key := "mixed/checkpoint/CURRENT"
	for _, change := range []func(*checkpoint.ManagedGraphLocator){
		func(l *checkpoint.ManagedGraphLocator) { l.SealDecisionHash[0] ^= 1 },
		func(l *checkpoint.ManagedGraphLocator) { l.SealDecisionSlot = uint64(view.Tip() + 1) },
	} {
		bad := locator
		change(&bad)
		data, err := json.Marshal(bad)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.bucket.Upload(f.ctx, key, bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenReadOnlyViewAtHeadForPolicyWithReconfiguration(f.ctx, f.bucket, "mixed", 1, f.bootstrap, view.CapturedHead(), sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1024, true); err == nil {
			t.Fatal("unbound locator accepted against captured HEAD")
		}
	}
	if err := f.bucket.Delete(f.ctx, key); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenReadOnlyViewForPolicyWithReconfiguration(f.ctx, f.bucket, "mixed", 1, f.bootstrap, sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1024, true); err == nil {
		t.Fatal("missing locator fell back to genesis or archive Base0")
	}
	if f.archive.head.Base != 0 {
		t.Fatal("reader mutated source archive base")
	}
}

func TestManagedGraphPreparedV4LegacyNullLocatorRejected(t *testing.T) {
	identity := mixedPreparedIdentity()
	version, err := preparedVersion(sqlpolicy.LegacyMaterializationPolicy, types.DefaultIdempotencyWindowSlots)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"graphLocator", "GraphLocator"} {
		dir := t.TempDir()
		data, err := json.Marshal(preparedStartupManifest{Version: version, Identity: identity})
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(data, &fields); err != nil {
			t.Fatal(err)
		}
		fields[field] = json.RawMessage("null")
		data, err = json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "completed.json"), data, 0o600); err != nil {
			t.Fatal(err)
		}
		_, err = LoadPreparedStartup(context.Background(), objstore.NewInMemBucket(), "unused", 1, quepaxa.Cluster{}, dir, filepath.Join(t.TempDir(), "unused"), identity)
		if err == nil || !strings.Contains(err.Error(), "unexpected Graph locator") {
			t.Fatalf("legacy %s:null rejection=%v", field, err)
		}
	}
}

func TestManagedGraphReaderTrustedMembershipMode(t *testing.T) {
	for _, mode := range []bool{false, true} {
		t.Run(fmt.Sprintf("reconfiguration=%t", mode), func(t *testing.T) {
			f := newGraphPreparedFixtureForMembership(t, mode)
			if mode != (f.seal.Membership != nil) {
				t.Fatal("fixture fabricated membership mode")
			}
			if _, err := OpenReadOnlyViewForPolicy(f.ctx, f.bucket, "mixed", 1, f.bootstrap, sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1024); err == nil {
				t.Fatal("Graph trust mode inferred without configuration")
			}
			view, err := OpenReadOnlyViewForPolicyWithReconfiguration(f.ctx, f.bucket, "mixed", 1, f.bootstrap, sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1024, mode)
			if err != nil {
				t.Fatal(err)
			}
			if view.Base() != f.seal.Index || view.Tip() != f.core.Tip() || len(view.Decisions()) == 0 {
				t.Fatal("trusted mode lost actual root/suffix")
			}
			if _, err := OpenReadOnlyViewForPolicyWithReconfiguration(f.ctx, f.bucket, "mixed", 1, f.bootstrap, sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1024, !mode); err == nil || !strings.Contains(err.Error(), "membership") {
				t.Fatalf("wrong trusted mode did not preserve membership validator: %v", err)
			}
			identity := mixedPreparedIdentity()
			handoff := filepath.Join(t.TempDir(), "handoff")
			if err := PublishPreparedStartupForPolicy(f.ctx, view, handoff, identity, sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1024); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadPreparedStartupForPolicyWithReconfiguration(f.ctx, f.bucket, "mixed", 1, f.bootstrap, handoff, filepath.Join(t.TempDir(), "right"), identity, sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1024, mode); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadPreparedStartupForPolicyWithReconfiguration(f.ctx, f.bucket, "mixed", 1, f.bootstrap, handoff, filepath.Join(t.TempDir(), "wrong"), identity, sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1024, !mode); err == nil || !strings.Contains(err.Error(), "membership") {
				t.Fatalf("prepared source overrode trusted membership mode: %v", err)
			}
		})
	}
}

func TestManagedGraphPreparedV4StrictInventoryAndStreamingBounds(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "latticedb"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "checkpoint"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"sqlite.db", "certified-suffix.json", "latticedb/graph.ltdb"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(name), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := validateGraphPreparedInventory(root, false); err != nil {
		t.Fatal(err)
	}
	artifacts, err := preparedGraphArtifacts(ctx, root, false)
	if err != nil || len(artifacts) != 2 {
		t.Fatal(err)
	}
	for _, name := range []string{"foreign", "sqlite.db-wal", "latticedb/unknown"} {
		path := filepath.Join(root, name)
		if err := os.WriteFile(path, []byte("unknown"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := validateGraphPreparedInventory(root, false); err == nil {
			t.Fatal("unknown artifact accepted")
		}
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("private"), 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "latticedb", "link")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	if err := validateGraphPreparedInventory(root, false); err == nil {
		t.Fatal("symlink accepted")
	}
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "sqlite.db")
	size, hash, err := preparedGraphFileHash(ctx, path, maxPreparedGraphBytes, false)
	if err != nil || size != int64(len("sqlite.db")) || hash != sha256.Sum256([]byte("sqlite.db")) {
		t.Fatal("stream hash/size differs")
	}
	if _, _, err := preparedGraphFileHash(ctx, path, size-1, false); err == nil {
		t.Fatal("size bound ignored")
	}
	// A sparse file and canceled read test the >1GiB acceptance boundary without
	// reading/allocating a GiB or claiming a large-file streaming benchmark.
	sparse := filepath.Join(t.TempDir(), "large")
	file, err := os.Create(sparse)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(1<<30 + 1); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := preparedGraphFileHash(canceled, sparse, maxPreparedGraphBytes, false); !errors.Is(err, context.Canceled) {
		t.Fatalf("new profile retained old1GiB cap or ignored cancellation: %v", err)
	}
	if _, _, err := preparedGraphFileHash(ctx, sparse, 1<<30, false); err == nil {
		t.Fatal("caller remaining envelope ignored")
	}
	if !bytes.Equal(artifacts[0].SHA256[:], hash[:]) {
		t.Fatal("descriptor hash differs from streaming hash")
	}
}
