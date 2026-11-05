package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

type floorRefreshChangingBucket struct {
	objstore.Bucket
	once   sync.Once
	change func()
}

func (b *floorRefreshChangingBucket) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if key == "floor-refresh/archive/head.bin" {
		b.once.Do(b.change)
	}
	return b.Bucket.Get(ctx, key)
}

// Only negative cases change archived evidence. The real archive encoder still
// checks object hashes/extent prefixes; Core must reject the substituted proof.
type floorRefreshSource struct {
	values   []quepaxa.DecidedValue
	prefixes map[quepaxa.Slot][32]byte
}

func (s floorRefreshSource) Tip() quepaxa.Slot { return quepaxa.Slot(len(s.values)) }
func (s floorRefreshSource) PrefixHash(slot quepaxa.Slot) ([32]byte, bool) {
	h, ok := s.prefixes[slot]
	return h, ok
}
func (s floorRefreshSource) DecisionsFromBounded(from quepaxa.Slot, count, budget int) ([]quepaxa.DecidedValue, quepaxa.Slot, error) {
	if from == 0 || from > s.Tip() {
		return nil, s.Tip(), nil
	}
	end := min(int(from)-1+count, len(s.values))
	return append([]quepaxa.DecidedValue(nil), s.values[int(from)-1:end]...), s.Tip(), nil
}

func TestManagedAppendOnlyVoterRefreshFromRestoredFloor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const policy = sqlpolicy.ManagedRequestMaterializationPolicy
	const protocol = sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol
	var source *Node
	var cp *checkpoint.Manager
	var learner *quepaxa.Core
	configure := func(cfg *quepaxa.Config) {
		nonce, bound := cfg.WAL.Identity()
		if !bound {
			t.Fatal("fixture WAL lacks actual registration")
		}
		var identity [32]byte
		copy(identity[:], nonce)
		cfg.ValidateValue = types.ValidateManagedKVExecutionPolicy
		cfg.ManagedAppendOnly = &quepaxa.ManagedAppendOnlyConfig{
			Binding: quepaxa.ManagedAppendOnlyBinding{ApplicationProtocol: protocol, BootstrapPlanDigest: fmt.Sprintf("%064x", 55), MemberConfigDigest: fmt.Sprintf("%064x", 66), WALIdentity: identity},
			Verify: func(ctx context.Context, candidate quepaxa.ManagedCheckpointCandidate) error {
				return source.verifyManagedAppendOnlyCandidate(ctx, candidate)
			},
		}
	}
	n, bucket, tr, _ := terminalRetryFixture(t, func(ctx context.Context, target quepaxa.Cluster, through quepaxa.Slot, prefix [32]byte) error {
		if learner == nil {
			return fmt.Errorf("actual learner unavailable")
		}
		if err := learner.WaitTip(ctx, through); err != nil {
			return err
		}
		if err := learner.EnsureDurableThrough(ctx, through); err != nil {
			return err
		}
		got, ok := learner.PrefixHash(through)
		if !ok || got != prefix {
			return fmt.Errorf("actual learner prefix differs")
		}
		found := false
		for _, member := range target.Members {
			if member.ID == "fresh" && member.WALIdentity == learner.WALIdentity() {
				found = true
			}
		}
		if !found {
			return fmt.Errorf("target lacks exact registered learner")
		}
		return nil
	}, configure)
	// The shared membership fixture intentionally constructs only membership
	// resources. Publication also needs New's initial-root completion channel.
	source = New(n.config)
	source.core, source.bucket = n.core, bucket
	bootstrap := source.core.CurrentCluster()
	source.managedStartup = &ManagedStartupConfig{expectedPolicy: policy, Identity: ManagedStartupIdentity{ApplicationProtocol: protocol}}
	var err error
	cp, err = checkpoint.NewAppendOnlyManagerForPolicy(bucket, "floor-refresh", t.TempDir(), policy, 1)
	if err != nil {
		t.Fatal(err)
	}
	source.checkpoints = cp
	archive, err := recovery.NewAppendOnlyManagerWithValueValidator(bucket, "floor-refresh", 1, types.ValidateManagedKVExecutionPolicy)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(archive.Close)
	archive.SetGroupDelay(0)
	if err := archive.Load(ctx); err != nil {
		t.Fatal(err)
	}
	source.archive = archive
	for _, core := range tr.cores {
		core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
			return cp.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
		})
	}
	mat, err := materializer.OpenManagedSQLKVWithAppendOnlyOriginals(filepath.Join(t.TempDir(), "source.sqlite"), 1, types.DefaultIdempotencyWindowSlots)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := mat.Close(); err != nil {
			t.Error(err)
		}
	})
	apply := func() {
		t.Helper()
		for from := quepaxa.Slot(mat.Tip()) + 1; from <= source.core.Tip(); {
			values, _, err := source.core.DecisionsFrom(from, 256)
			if err != nil || len(values) == 0 {
				t.Fatalf("actual materialization prefix: %v", err)
			}
			if err := mat.ApplyBatch(ctx, values); err != nil {
				t.Fatal(err)
			}
			from += quepaxa.Slot(len(values))
		}
	}
	write := func(id string) {
		t.Helper()
		value, err := types.EncodeManagedKVCommand(types.KVCommand{RequestID: id, Operation: "put", Key: id, Value: []byte(id)})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := source.core.Propose(ctx, value); err != nil {
			t.Fatal(err)
		}
		apply()
	}
	seal := func() (quepaxa.SealedCheckpoint, quepaxa.DecidedValue) {
		t.Helper()
		apply()
		if err := archive.SyncThrough(ctx, source.core, source.core.Tip()); err != nil {
			t.Fatal(err)
		}
		files, cut, release, err := mat.CheckpointFilesAt(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		sources := make([]checkpoint.Source, len(files))
		for i, file := range files {
			sources[i] = checkpoint.Source{Role: string(file.Role), Path: file.Path}
		}
		root, err := cp.CreateManagedAppendOnlyCandidate(ctx, sources, cut)
		if err != nil {
			t.Fatal(err)
		}
		prefix, ok := source.core.PrefixHash(quepaxa.Slot(cut))
		if !ok {
			t.Fatal("actual cut prefix missing")
		}
		next, following, err := source.core.CheckpointLeaderOrders(quepaxa.Slot(cut))
		if err != nil {
			t.Fatal(err)
		}
		history, err := source.core.CheckpointMembership(quepaxa.Slot(cut))
		if err != nil {
			t.Fatal(err)
		}
		s := quepaxa.CheckpointSeal{ConfigID: source.core.ConfigIDForSlot(quepaxa.Slot(cut) + 1), Index: quepaxa.Slot(cut), RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following, Membership: &history}
		value, err := quepaxa.EncodeCheckpointSeal(s)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := source.core.Propose(ctx, value); err != nil {
			t.Fatal("actual QCKP", err)
		}
		selected, ok, err := source.core.LatestManagedCheckpointSeal()
		if err != nil || !ok {
			t.Fatal("actual QCKP absent", err)
		}
		decision, ok := source.core.CertifiedValue(selected.DecisionSlot)
		if !ok {
			t.Fatal("actual QCKP certificate absent")
		}
		if err := source.promoteManagedAppendOnlySeal(ctx, selected); err != nil {
			t.Fatal(err)
		}
		return selected, decision
	}
	write("before-floor")
	base, baseDecision := seal()
	member := registerMembershipLearner(t, bucket, "fresh")
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	nonce, err := hex.DecodeString(member.WALIdentity)
	if err != nil {
		t.Fatal(err)
	}
	if err := wal.BindIdentity(nonce); err != nil {
		t.Fatal(err)
	}
	lc := quepaxa.Config{NodeID: member.ID, Cluster: bootstrap, WAL: wal, Transport: tr, EnableReconfiguration: true}
	configure(&lc)
	learner, err = quepaxa.NewLearner(lc)
	if err != nil {
		t.Fatal(err)
	}
	learner.SetCheckpointValidator(func(ctx context.Context, s quepaxa.CheckpointSeal) error {
		return cp.Verify(ctx, uint64(s.Index), s.RootHash, s.StateHash)
	})
	prior, _, err := source.core.DecisionsFrom(1, 256)
	if err != nil {
		t.Fatal(err)
	}
	if err := learner.AcceptCertifiedValues(prior); err != nil {
		t.Fatal(err)
	}
	tr.cores[member.ID] = learner
	tr.ids = append(tr.ids, member.ID)
	add := quepaxa.Cluster{ConfigID: 2, Members: append(append([]quepaxa.Member(nil), bootstrap.Members...), member)}
	if _, err := source.core.BeginReconfiguration(ctx, add); err != nil {
		t.Fatal("actual ADD freeze", err)
	}
	if err := source.core.FinishReconfiguration(ctx); err != nil {
		t.Fatal("actual ADD terminal", err)
	}
	remove := quepaxa.Cluster{ConfigID: 3, Members: []quepaxa.Member{bootstrap.Members[0], bootstrap.Members[1], member}}
	if _, err := source.core.BeginReconfiguration(ctx, remove); err != nil {
		t.Fatal("actual REMOVE freeze", err)
	}
	if err := source.core.FinishReconfiguration(ctx); err != nil {
		t.Fatal("actual REMOVE terminal", err)
	}
	write("retained-H")
	h := source.core.Tip()
	hPrefix, ok := source.core.PrefixHash(h)
	if !ok || source.core.ConfigID() != 3 {
		t.Fatal("actual config-3 H missing")
	}
	retained, _, err := source.core.DecisionsFrom(base.Index+1, 256)
	if err != nil {
		t.Fatal(err)
	}
	write("later-J")
	latest, latestDecision := seal()
	all, _, err := source.core.DecisionsFrom(1, 256)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != int(source.core.Tip()) {
		t.Fatal("fixture history exceeds one complete capture")
	}
	for _, name := range []string{"later-current", "floor-equals-H", "overlap-fork", "missing-overlap", "unexpected-base", "core-changed", "missing-floor-root", "wrong-suffix-certificate"} {
		t.Run(name, func(t *testing.T) {
			positive := name == "later-current" || name == "floor-equals-H"
			localBase, localDecision, localH, localPrefix := base, baseDecision, h, hPrefix
			if name == "floor-equals-H" {
				localBase, localDecision, localH = latest, latestDecision, latest.Index
				localPrefix, ok = source.core.PrefixHash(localH)
				if !ok {
					t.Fatal("actual floor-equals-H prefix unavailable")
				}
			}
			rwal, err := qlog.Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer rwal.Close()
			nonce, _ := hex.DecodeString(bootstrap.Members[0].WALIdentity)
			if err := rwal.BindIdentity(nonce); err != nil {
				t.Fatal(err)
			}
			cfg := quepaxa.Config{NodeID: source.config.NodeID, Cluster: bootstrap, WAL: rwal, Transport: tr, EnableReconfiguration: true}
			configure(&cfg)
			core, err := quepaxa.New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			core.SetCheckpointValidator(func(ctx context.Context, s quepaxa.CheckpointSeal) error {
				return cp.Verify(ctx, uint64(s.Index), s.RootHash, s.StateHash)
			})
			if name == "missing-floor-root" {
				bad := base.CheckpointSeal
				bad.RootHash = [32]byte{}
				if err := core.RestoreCheckpointBase(ctx, bad, baseDecision); err == nil || err.Error() != "invalid checkpoint recovery base" || core.Tip() != 0 || core.CompactionFloor() != 0 {
					t.Fatal("missing floor root established trust")
				}
				return
			}
			if err := core.RestoreCheckpointBase(ctx, localBase.CheckpointSeal, localDecision); err != nil {
				t.Fatal("restore actual A", err)
			}
			if name != "floor-equals-H" {
				if err := core.AcceptCertifiedValues(retained); err != nil {
					t.Fatal("restore actual H", err)
				}
			}
			if !core.IsVoter() || core.ConfigID() != 3 || core.Tip() != localH {
				t.Fatal("restored H is not the actual config-3 voter")
			}
			floor, root, ok := core.RecoveryRoot()
			if !ok || floor != localBase.Index || root != localBase.RootHash {
				t.Fatal("actual nonzero floor missing")
			}
			rn := New(source.config)
			rn.managedStartup = source.managedStartup
			rn.core = core
			rn.checkpoints = cp
			rn.archive = archive
			changed := false
			if name == "core-changed" {
				changing := &floorRefreshChangingBucket{Bucket: bucket, change: func() {
					if err := core.AcceptCertifiedValues(all[int(h) : int(h)+1]); err != nil {
						t.Fatal("actual concurrent certified progress", err)
					}
					changed = true
				}}
				captured, err := recovery.NewAppendOnlyManagerWithValueValidator(changing, "floor-refresh", 1, types.ValidateManagedKVExecutionPolicy)
				if err != nil {
					t.Fatal(err)
				}
				defer captured.Close()
				rn.archive = captured
			} else if !positive {
				values := make([]quepaxa.DecidedValue, len(all))
				copy(values, all)
				if name == "overlap-fork" {
					v, err := types.EncodeManagedKVCommand(types.KVCommand{RequestID: "fork", Operation: "put", Key: "fork", Value: []byte("fork")})
					if err != nil {
						t.Fatal(err)
					}
					values[int(h)-1].Value = v
					values[int(h)-1].Hash = sha256.Sum256(v)
				} else if name == "wrong-suffix-certificate" {
					values[int(h)].Certificate = bytes.Clone(baseDecision.Certificate)
				}
				if name == "missing-overlap" {
					values = values[:int(h)-1]
				}
				prefixes := map[quepaxa.Slot][32]byte{0: {}}
				for _, d := range values {
					prefixes[d.Slot] = quepaxa.AdvancePrefixHash(prefixes[d.Slot-1], d.Slot, d.Hash)
				}
				negative, err := recovery.NewAppendOnlyManagerWithValueValidator(bucket, "floor-refresh/"+name, 1, types.ValidateManagedKVExecutionPolicy)
				if err != nil {
					t.Fatal(err)
				}
				defer negative.Close()
				negative.SetGroupDelay(0)
				if err := negative.Load(ctx); err != nil {
					t.Fatal(err)
				}
				if err := negative.SyncThrough(ctx, floorRefreshSource{values: values, prefixes: prefixes}, quepaxa.Slot(len(values))); err != nil {
					t.Fatal("publish negative proof through actual archive codec", err)
				}
				if name == "unexpected-base" {
					if err := negative.TrimThrough(ctx, base, baseDecision); err != nil {
						t.Fatal("publish actual negative archive base", err)
					}
				}
				rn.archive = negative
			}
			before, _, err := core.DecisionsFrom(floor+1, 256)
			if err != nil {
				t.Fatal(err)
			}
			storageBefore := bucket.Stats()
			selectedRoot, selected, err := rn.selectManagedAppendOnlyRoot(ctx)
			storageAfter := bucket.Stats()
			if storageAfter.Uploads != storageBefore.Uploads || storageAfter.Deletes != storageBefore.Deletes {
				t.Fatal("read-only selection mutated shared object storage")
			}
			if !positive {
				if err == nil || selectedRoot != nil || selected != nil {
					t.Fatal("invalid anchored suffix accepted")
				}
				wantCause := map[string]string{
					"overlap-fork":             "restored managed append-only archive overlap differs from local certified prefix",
					"missing-overlap":          "managed append-only archive is behind the restored certified tip",
					"unexpected-base":          "managed append-only archive unexpectedly has a recovery base",
					"core-changed":             "restored managed append-only prefix anchor changed during verification",
					"wrong-suffix-certificate": "catch-up value does not match QuePaxa certificate",
				}[name]
				if wantCause == "" || !strings.Contains(err.Error(), wantCause) {
					t.Fatalf("wrong refusal boundary: %v", err)
				}
				after, _, e := core.DecisionsFrom(floor+1, 256)
				expected, expectedTip := before, localH
				if name == "core-changed" {
					if !changed || err.Error() != "restored managed append-only prefix anchor changed during verification" {
						t.Fatalf("anchor-change boundary not reached: %v", err)
					}
					expected = append(append([]quepaxa.DecidedValue(nil), before...), all[int(h)])
					expectedTip++
				}
				if e != nil || !reflect.DeepEqual(expected, after) || core.Tip() != expectedTip {
					t.Fatal("invalid proof changed local certified H")
				}
			} else if err != nil || selectedRoot == nil || selected == nil || selected.RootHash != latest.RootHash || selected.DecisionSlot != latest.DecisionSlot {
				t.Fatalf("restored config-3 voter cannot refresh later CURRENT: %v", err)
			}
			f, r, ok := core.RecoveryRoot()
			p, pok := core.PrefixHash(localH)
			if !ok || f != floor || r != root || !pok || p != localPrefix || !core.IsVoter() || core.ConfigID() != 3 || core.WALIdentity() != bootstrap.Members[0].WALIdentity {
				t.Fatal("refresh changed floor/H/binding membership")
			}
		})
	}
}
