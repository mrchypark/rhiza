package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/ncruces/go-sqlite3/driver"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
)

type mixedRecoveryFixture struct {
	ctx       context.Context
	bucket    objstore.Bucket
	bootstrap quepaxa.Cluster
	core      *quepaxa.Core
	archive   *Manager
	state     *materializer.Materializer
	cp        *checkpoint.Manager
	root      *checkpoint.Checkpoint
	seal      quepaxa.CheckpointSeal
	window    uint64
}

func newMixedRecoveryBase(t *testing.T, window uint64, bucket objstore.Bucket) *mixedRecoveryFixture {
	t.Helper()
	if bucket == nil {
		bucket = objstore.NewInMemBucket()
	}
	f := &mixedRecoveryFixture{ctx: context.Background(), bucket: bucket, window: window}
	f.bootstrap = quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "mixed", PublicKey: testPublicKey("mixed")}}}
	w, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	f.core, err = quepaxa.New(quepaxa.Config{NodeID: "mixed", Cluster: f.bootstrap, WAL: w, EnableReconfiguration: true})
	if err != nil {
		t.Fatal(err)
	}
	f.state, err = materializer.OpenManagedSQLKV(filepath.Join(t.TempDir(), "sqlite.db"), 1, window)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.state.Close() })
	f.archive = NewManager(f.bucket, "mixed", 1)
	t.Cleanup(f.archive.Close)
	return f
}

func newMixedRecoveryFixture(t *testing.T, window uint64, withCheckpoint bool, alter func(string)) *mixedRecoveryFixture {
	t.Helper()
	f := newMixedRecoveryBase(t, window, nil)
	first, err := types.EncodeManagedKVCommand(types.KVCommand{RequestID: string([]byte{0xff}), Operation: "put", Key: string([]byte{0xfe}), Value: []byte{1}, TTLMS: 13, ObservedAtUnixMS: 100, ExpiresAtUnixMS: 113})
	if err != nil {
		t.Fatal(err)
	}
	f.propose(t, first)
	if withCheckpoint {
		files, index, cleanup, err := f.state.CheckpointFilesAt(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer cleanup()
		if alter != nil {
			alter(files[0].Path)
		}
		// The negative fixture may carry policy7 in a correctly hashed root8.
		// Its physical source root is created by the matching actual manager.
		creatorPolicy := sqlpolicy.ManagedSQLKVMaterializationPolicy
		if sqlpolicy.CheckSnapshotFileForPolicy(f.ctx, files[0].Path, creatorPolicy) != nil {
			creatorPolicy = sqlpolicy.LegacyMaterializationPolicy
		}
		creator, err := checkpoint.NewManagerForPolicy(f.bucket, "mixed", "", creatorPolicy, 1)
		if err != nil {
			t.Fatal(err)
		}
		claim, err := creator.AcquirePublisherClaim(f.ctx, "fixture", 0, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		root, err := creator.CreateFiles(f.ctx, claim, []checkpoint.Source{{Role: checkpoint.RoleSQLite, Path: files[0].Path}}, index)
		if err != nil {
			t.Fatal(err)
		}
		if root.SQLExecutionPolicy != 8 {
			root.SQLExecutionPolicy = 8
			encoded, err := json.Marshal(root)
			if err != nil {
				t.Fatal(err)
			}
			root.RootHash = sha256.Sum256(encoded)
			if err := f.bucket.Upload(f.ctx, fmt.Sprintf("mixed/checkpoint/roots/%020d_%x.json", root.Index, root.RootHash), bytes.NewReader(encoded)); err != nil {
				t.Fatal(err)
			}
		}
		f.root = root
		f.cp, err = checkpoint.NewManagerForPolicy(f.bucket, "mixed", "", sqlpolicy.ManagedSQLKVMaterializationPolicy, 1)
		if err != nil {
			t.Fatal(err)
		}
		prefix, ok := f.core.PrefixHash(f.core.Tip())
		if !ok {
			t.Fatal("missing prefix")
		}
		next, following, err := f.core.CheckpointLeaderOrders(f.core.Tip())
		if err != nil {
			t.Fatal(err)
		}
		membership, err := f.core.CheckpointMembership(f.core.Tip())
		if err != nil {
			t.Fatal(err)
		}
		f.seal = quepaxa.CheckpointSeal{ConfigID: 1, Index: f.core.Tip(), RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following, Membership: &membership}
		f.core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
			return f.cp.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
		})
		if err := f.core.PrepareCheckpoint(f.ctx, f.seal); err != nil {
			t.Fatal(err)
		}
		encoded, err := quepaxa.EncodeCheckpointSeal(f.seal)
		if err != nil {
			t.Fatal(err)
		}
		f.propose(t, encoded)
		if err := f.archive.SyncThrough(f.ctx, f.core, f.core.Tip()); err != nil {
			t.Fatal(err)
		}
		sealed, ok, err := f.core.LatestCheckpointSeal()
		if err != nil || !ok {
			t.Fatal("missing actual seal", err)
		}
		decision, ok := f.core.CertifiedValue(sealed.DecisionSlot)
		if !ok || len(decision.Certificate) == 0 {
			t.Fatal("missing signed checkpoint decision")
		}
		if err := f.archive.TrimThrough(f.ctx, sealed, decision); err != nil {
			t.Fatal(err)
		}
		if err := creator.ReleasePublisherClaim(f.ctx, claim); err != nil {
			t.Fatal(err)
		}
	}
	last, err := types.EncodeManagedKVCommand(types.KVCommand{RequestID: string([]byte{0xfe}), Operation: "cas", Key: "absent", ExpectedExists: true, Value: []byte{}, Expected: []byte{}})
	if err != nil {
		t.Fatal(err)
	}
	f.propose(t, last)
	if err := f.archive.SyncThrough(f.ctx, f.core, f.core.Tip()); err != nil {
		t.Fatal(err)
	}
	return f
}

func newMixedSQLRecoveryFixture(t *testing.T, bucket objstore.Bucket) *mixedRecoveryFixture {
	t.Helper()
	f := newMixedRecoveryBase(t, 1024, bucket)
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "create", SQL: "CREATE TABLE records(v INTEGER)"}, {RequestID: "insert", SQL: "INSERT INTO records(v) VALUES(42)"}})
	if err != nil {
		t.Fatal(err)
	}
	f.propose(t, value)
	f.propose(t, types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{}))
	if err := f.archive.SyncThrough(f.ctx, f.core, f.core.Tip()); err != nil {
		t.Fatal(err)
	}
	return f
}

func TestManagedRecoveryPolicySQLOnlyNoRoot(t *testing.T) {
	f := newMixedSQLRecoveryFixture(t, nil)
	view, err := OpenReadOnlyViewForPolicy(f.ctx, f.bucket, "mixed", 1, f.bootstrap, 8, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if view.Base() != 0 || view.root != nil {
		t.Fatal("fixture unexpectedly checkpointed")
	}
	handoff := filepath.Join(t.TempDir(), "handoff")
	if err := PublishPreparedStartupForPolicy(f.ctx, view, handoff, mixedPreparedIdentity(), 8, 1024); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadPreparedStartupForPolicy(f.ctx, f.bucket, "mixed", 1, f.bootstrap, handoff, filepath.Join(t.TempDir(), "target"), mixedPreparedIdentity(), 8, 1024); err != nil {
		t.Fatal(err)
	}
}

// Export is opt-in for the CI-only actual baseline loader qualification.
// A missing export setting is an explicit skip, never an old-loader pass.
func TestManagedRecoveryOldHandoffFixtureExport(t *testing.T) {
	root := os.Getenv("RHIZA_MANAGED_KV_OLD_HANDOFF_FIXTURE")
	if root == "" {
		t.Skip("CI fixture export was not requested")
	}
	exportOldHandoffFixture(t, root)
}

func TestManagedRecoveryPolicyFilesystemFixture(t *testing.T) {
	exportOldHandoffFixture(t, filepath.Join(t.TempDir(), "fixture"))
}

func exportOldHandoffFixture(t *testing.T, root string) {
	t.Helper()
	if !filepath.IsAbs(root) {
		t.Fatal("fixture directory must be absolute")
	}
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal("claim fixture directory", err)
	}
	bucket, err := filesystem.NewBucket(filepath.Join(root, "bucket"))
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Close()
	f := newMixedSQLRecoveryFixture(t, bucket)
	view, err := OpenReadOnlyViewForPolicy(f.ctx, bucket, "mixed", 1, f.bootstrap, 8, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if view.Base() != 0 || view.root != nil {
		t.Fatal("old-loader counterexample must have no root/anchor")
	}
	v2 := filepath.Join(root, "handoff-v2")
	identity := mixedPreparedIdentity()
	if err := PublishPreparedStartupForPolicy(f.ctx, view, v2, identity, 8, 1024); err != nil {
		t.Fatal(err)
	}
	v1 := filepath.Join(root, "handoff-v1")
	if err := os.CopyFS(v1, os.DirFS(v2)); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(v1, "completed.json"))
	if err != nil {
		t.Fatal(err)
	}
	var marker preparedStartupManifest
	if err := json.Unmarshal(raw, &marker); err != nil {
		t.Fatal(err)
	}
	marker.Version = 1
	raw, err = json.Marshal(marker)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(v1, "completed.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	meta := struct {
		Identity  PreparedStartupIdentity
		Bootstrap quepaxa.Cluster
		Prefix    string
		Tip       uint64
		Window    uint64
	}{identity, f.bootstrap, "mixed", uint64(view.Tip()), 1024}
	raw, err = json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "fixture.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("fixture metadata sha256=%x", sha256.Sum256(raw))
	t.Logf("exported actual SQL/control archive + mixed8 handoff at tip %d; old-loader execution remains a separate CI step", view.Tip())
}

func (f *mixedRecoveryFixture) propose(t *testing.T, value []byte) {
	t.Helper()
	slot, _, err := f.core.Propose(f.ctx, value)
	if err != nil {
		t.Fatal(err)
	}
	decision, ok := f.core.CertifiedValue(slot)
	if !ok || len(decision.Certificate) == 0 {
		t.Fatal("missing actual certified value")
	}
	if err := f.state.ApplyBatch(f.ctx, []quepaxa.DecidedValue{decision}); err != nil {
		t.Fatal(err)
	}
}

func mixedPreparedIdentity() PreparedStartupIdentity {
	return PreparedStartupIdentity{ClusterUID: "cluster", Generation: 1, DeploymentUID: "deployment", PodUID: "pod", NodeID: "node", SourceBinding: "source", ConfigDigest: "config"}
}

func TestManagedRecoveryPolicyRoundTrip(t *testing.T) {
	for _, window := range []uint64{1024, 131072} {
		for _, checkpointed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/%t", window, checkpointed), func(t *testing.T) {
				f := newMixedRecoveryFixture(t, window, checkpointed, nil)
				readonly := &haReadOnlyRecordingBucket{Bucket: f.bucket}
				view, err := OpenReadOnlyViewForPolicy(f.ctx, readonly, "mixed", 1, f.bootstrap, 8, window)
				if err != nil {
					t.Fatal(err)
				}
				if view.MaterializationPolicy() != 8 {
					t.Fatal("view lost profile")
				}
				handoff := filepath.Join(t.TempDir(), "handoff")
				identity := mixedPreparedIdentity()
				if err := PublishPreparedStartupForPolicy(f.ctx, view, handoff, identity, 8, window); err != nil {
					_ = filepath.WalkDir(handoff, func(path string, entry os.DirEntry, walkErr error) error {
						t.Logf("owned failed handoff entry %s type=%v err=%v", path, entry, walkErr)
						return nil
					})
					t.Fatal(err)
				}
				var manifest preparedStartupManifest
				raw, err := os.ReadFile(filepath.Join(handoff, "completed.json"))
				if err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(raw, &manifest); err != nil || manifest.Version != 2 {
					t.Fatal("wrong compatibility marker", err)
				}
				a, _ := os.Stat(filepath.Join(handoff, "completed.json"))
				b, _ := os.Stat(filepath.Join(handoff, "completion.pending"))
				if a == nil || b == nil || !os.SameFile(a, b) {
					t.Fatal("normal completion hardlink missing")
				}
				target := filepath.Join(t.TempDir(), "verified")
				verified, err := LoadPreparedStartupForPolicy(f.ctx, readonly, "mixed", 1, f.bootstrap, handoff, target, identity, 8, window)
				if err != nil {
					t.Fatal(err)
				}
				if verified.View().MaterializationPolicy() != 8 || verified.View().Tip() != view.Tip() {
					t.Fatal("view changed")
				}
				state, tip, err := materializer.InspectManagedApplicationFile(f.ctx, filepath.Join(target, "sqlite.db"), window)
				want, err2 := f.state.InspectManagedApplicationState(f.ctx)
				if err != nil || err2 != nil || tip != uint64(f.core.Tip()) || state != want {
					t.Fatalf("restored evidence mismatch %v %v", err, err2)
				}
				mat, err := materializer.OpenManagedSQLKV(filepath.Join(target, "sqlite.db"), 1, window)
				if err != nil {
					t.Fatal(err)
				}
				defer mat.Close()
				for _, id := range []string{string([]byte{0xff}), string([]byte{0xfe})} {
					receipt, found, err := mat.MutationReceipt(f.ctx, types.MutationKV, id)
					if err != nil || !found || receipt.RetryThroughSlot != receipt.Slot+window-1 {
						t.Fatal("raw receipt/window lost", receipt, err)
					}
					if id == string([]byte{0xfe}) && receipt.Applied {
						t.Fatal("false CAS changed")
					}
				}
				value, found, err := mat.KVGet(f.ctx, string([]byte{0xfe}), time.UnixMilli(101))
				if err != nil || !found || !bytes.Equal(value, []byte{1}) {
					t.Fatal("raw key lost", err)
				}
				if readonly.mutations != 0 {
					t.Fatal("source mutated")
				}
			})
		}
	}
}

func TestManagedRecoveryPolicyBeforeTarget(t *testing.T) {
	f := newMixedRecoveryFixture(t, 1024, false, nil)
	view, err := OpenReadOnlyViewForPolicy(f.ctx, f.bucket, "mixed", 1, f.bootstrap, 8, 1024)
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range []struct {
		policy sqlpolicy.MaterializationPolicy
		window uint64
	}{{0, 1024}, {7, 1024}, {8, 1025}, {8, 0}} {
		target := filepath.Join(t.TempDir(), "target")
		if _, err := PrepareReadOnlyRestoreForPolicy(f.ctx, view, target, pair.policy, pair.window); err == nil {
			t.Fatal("mismatched pair accepted")
		}
		if _, err := os.Lstat(target); !os.IsNotExist(err) {
			t.Fatal("precondition created target", err)
		}
	}
	for _, window := range []uint64{1024, 131072} {
		target := filepath.Join(t.TempDir(), "target")
		if err := PublishPreparedStartupForPolicy(f.ctx, nil, target, mixedPreparedIdentity(), 7, window); err == nil {
			t.Fatal("legacy custom handoff published")
		}
		if _, err := LoadPreparedStartupForPolicy(f.ctx, nil, "", 1, quepaxa.Cluster{}, "missing", target, mixedPreparedIdentity(), 7, window); err == nil {
			t.Fatal("legacy custom handoff loaded")
		}
		if _, err := os.Lstat(target); !os.IsNotExist(err) {
			t.Fatal("unsupported handoff created target")
		}
	}
}

func TestManagedRecoveryPolicyActualCheckpointNegative(t *testing.T) {
	for _, change := range []struct {
		name, query string
		want        string
	}{
		{"policy", `UPDATE _rhiza_meta SET value='7' WHERE key='sql_execution_policy'`, "incompatible SQL execution policy"},
		{"window", `UPDATE _rhiza_meta SET value='1025' WHERE key='sql_idempotency_window'`, "policy"},
		{"metadata", `DELETE FROM _rhiza_meta WHERE key='managed_application_policy'`, "policy"},
		{"tip", `UPDATE _rhiza_meta SET value='2' WHERE key='applied_slot'`, "applied tip"},
	} {
		t.Run(change.name, func(t *testing.T) {
			f := newMixedRecoveryFixture(t, 1024, true, func(path string) {
				db, err := driver.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				if _, err := db.Exec(change.query); err != nil {
					t.Fatal(err)
				}
			})
			// Actual root/block verification succeeds and fills the verified cache.
			if err := f.cp.Verify(f.ctx, f.root.Index, f.root.RootHash, f.root.Hash); err != nil {
				t.Fatal("fixture physical verification invalid", err)
			}
			if err := f.cp.Verify(f.ctx, f.root.Index, f.root.RootHash, f.root.Hash); err != nil {
				t.Fatal("cache hit invalid", err)
			}
			if err := verifyMixedReadOnlyCheckpoint(f.ctx, f.cp, f.root, quepaxa.Slot(f.root.Index), 1024, filepath.Join(t.TempDir(), "checkpoint")); err == nil || !strings.Contains(err.Error(), change.want) {
				t.Fatalf("cache bypassed actual file gate: %v", err)
			}
			view, err := OpenReadOnlyViewForPolicy(f.ctx, f.bucket, "mixed", 1, f.bootstrap, 8, 1024)
			if view != nil || err == nil || !strings.Contains(err.Error(), change.want) {
				t.Fatalf("semantically invalid signed source accepted: %v", err)
			}
		})
	}
}

func TestManagedRecoveryPolicyHandoffArtifacts(t *testing.T) {
	for _, artifact := range []string{"latticedb", "graph-data.db", "unknown", "sqlite.db.restore-state.json", "sqlite.db.restore-backup", ".rhiza-local-restore-left", "checkpoint/latticedb", "checkpoint/graph-data.db", "checkpoint/unknown", "checkpoint/sqlite.db.restore-backup", "sqlite-symlink", "checkpoint-symlink", "root-symlink"} {
		t.Run(artifact, func(t *testing.T) {
			f := newMixedRecoveryFixture(t, 1024, false, nil)
			view, err := OpenReadOnlyViewForPolicy(f.ctx, f.bucket, "mixed", 1, f.bootstrap, 8, 1024)
			if err != nil {
				t.Fatal(err)
			}
			handoff := filepath.Join(t.TempDir(), "handoff")
			identity := mixedPreparedIdentity()
			if err := PublishPreparedStartupForPolicy(f.ctx, view, handoff, identity, 8, 1024); err != nil {
				t.Fatal(err)
			}
			entry := filepath.Join(handoff, artifact)
			switch artifact {
			case "latticedb", "checkpoint/latticedb":
				if err := os.Mkdir(entry, 0700); err != nil {
					t.Fatal(err)
				}
			case "sqlite-symlink":
				entry = filepath.Join(handoff, "sqlite.db")
				if err := os.Rename(entry, filepath.Join(t.TempDir(), "saved.db")); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink("missing", entry); err != nil {
					t.Fatal(err)
				}
			case "checkpoint-symlink":
				entry = filepath.Join(handoff, "checkpoint")
				if err := os.Remove(entry); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(t.TempDir(), entry); err != nil {
					t.Fatal(err)
				}
			case "root-symlink":
				actual := handoff
				handoff = filepath.Join(t.TempDir(), "link")
				entry = handoff
				if err := os.Symlink(actual, handoff); err != nil {
					t.Fatal(err)
				}
			default:
				if err := os.WriteFile(entry, []byte("retained"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			before, err := os.Lstat(entry)
			if err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(t.TempDir(), "verified")
			if _, err := LoadPreparedStartupForPolicy(f.ctx, f.bucket, "mixed", 1, f.bootstrap, handoff, target, identity, 8, 1024); err == nil {
				t.Fatal("artifact accepted")
			}
			if _, err := os.Lstat(target); !os.IsNotExist(err) {
				t.Fatal("artifact rejection created target", err)
			}
			after, err := os.Lstat(entry)
			if err != nil || !os.SameFile(before, after) || before.Mode() != after.Mode() {
				t.Fatal("artifact was changed", err)
			}
			if before.Mode().IsRegular() {
				data, err := os.ReadFile(entry)
				if err != nil || string(data) != "retained" {
					t.Fatal("artifact bytes changed", err)
				}
			}
		})
	}
}

func TestManagedRecoveryPolicyVersionAndEvidence(t *testing.T) {
	f := newMixedRecoveryFixture(t, 1024, false, nil)
	view, err := OpenReadOnlyViewForPolicy(f.ctx, f.bucket, "mixed", 1, f.bootstrap, 8, 1024)
	if err != nil {
		t.Fatal(err)
	}
	identity := mixedPreparedIdentity()
	for _, version := range []int{0, 1, 3} {
		t.Run(fmt.Sprint("version-", version), func(t *testing.T) {
			handoff := filepath.Join(t.TempDir(), "handoff")
			if err := PublishPreparedStartupForPolicy(f.ctx, view, handoff, identity, 8, 1024); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(filepath.Join(handoff, "completed.json"))
			if err != nil {
				t.Fatal(err)
			}
			var marker preparedStartupManifest
			if err := json.Unmarshal(data, &marker); err != nil {
				t.Fatal(err)
			}
			marker.Version = version
			data, err = json.Marshal(marker)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(handoff, "completed.json"), data, 0600); err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(t.TempDir(), "target")
			if _, err := LoadPreparedStartupForPolicy(f.ctx, nil, "mixed", 1, f.bootstrap, handoff, target, identity, 8, 1024); err == nil || !strings.Contains(err.Error(), "identity mismatch") {
				t.Fatal("version did not fail before source access", err)
			}
			if _, err := os.Lstat(target); !os.IsNotExist(err) {
				t.Fatal("version failure created target")
			}
		})
	}
	// Version2 is rejected by the new default facade too. The actual baseline
	// binary is a separate qualification; this assertion does not replace it.
	handoff := filepath.Join(t.TempDir(), "handoff")
	if err := PublishPreparedStartupForPolicy(f.ctx, view, handoff, identity, 8, 1024); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadPreparedStartup(f.ctx, nil, "mixed", 1, f.bootstrap, handoff, filepath.Join(t.TempDir(), "old"), identity); err == nil || !strings.Contains(err.Error(), "identity mismatch") {
		t.Fatal(err)
	}
	// Mutable hashes cannot turn forged application state into certified state.
	db, err := driver.Open(filepath.Join(handoff, "sqlite.db"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`UPDATE _rhiza_kv SET value=X'99'`)
	closeErr := db.Close()
	if err != nil || closeErr != nil {
		t.Fatal(err, closeErr)
	}
	data, err := os.ReadFile(filepath.Join(handoff, "completed.json"))
	if err != nil {
		t.Fatal(err)
	}
	var marker preparedStartupManifest
	if err := json.Unmarshal(data, &marker); err != nil {
		t.Fatal(err)
	}
	marker.SQLiteHash, err = preparedFileHash(filepath.Join(handoff, "sqlite.db"), false)
	if err != nil {
		t.Fatal(err)
	}
	data, err = json.Marshal(marker)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(handoff, "completed.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "forgery-target")
	if _, err := LoadPreparedStartupForPolicy(f.ctx, f.bucket, "mixed", 1, f.bootstrap, handoff, target, identity, 8, 1024); err == nil || !strings.Contains(err.Error(), "evidence differs") {
		t.Fatal("forged logical state accepted", err)
	}
	if _, err := os.Stat(target); err != nil {
		t.Fatal("claimed failed target was removed", err)
	}
	if hash, err := preparedFileHash(filepath.Join(handoff, "sqlite.db"), false); err != nil || hash != marker.SQLiteHash {
		t.Fatal("source was changed", err)
	}
}
