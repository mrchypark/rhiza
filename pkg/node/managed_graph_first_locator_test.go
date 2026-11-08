package node

import (
	"context"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

type firstGraphLocatorBucket struct {
	objstore.Bucket
	afterMissing func()
}

func (b *firstGraphLocatorBucket) Attributes(ctx context.Context, key string) (objstore.ObjectAttributes, error) {
	attr, err := b.Bucket.Attributes(ctx, key)
	if b.afterMissing != nil && strings.HasSuffix(key, "/checkpoint/CURRENT") && err != nil && b.IsObjNotFoundErr(err) {
		afterMissing := b.afterMissing
		b.afterMissing = nil
		afterMissing()
	}
	return attr, err
}

func TestManagedGraphFirstLocatorAppearsBetweenStartupReads(t *testing.T) {
	for _, tc := range []string{"published", "unarchived", "absent"} {
		t.Run(tc, func(t *testing.T) {
			ctx := context.Background()
			producer, core, archive, bucket := newManagedAppendOnlySelectorFixture(t)
			seal, exists, err := core.LatestManagedCheckpointSeal()
			if err != nil || !exists || seal.DecisionSlot != 2 || archive.Tip() != 1 {
				t.Fatalf("fixture did not certify an unpublished first QCKP: exists=%t err=%v", exists, err)
			}
			decision, exists := core.CertifiedValue(seal.DecisionSlot)
			if !exists || core.ValidateCertifiedValue(ctx, decision) != nil {
				t.Fatal("fixture QCKP certificate unavailable")
			}
			readerBucket := &firstGraphLocatorBucket{Bucket: bucket}
			policy := sqlpolicy.ManagedGraphRequestMaterializationPolicy
			cp, err := checkpoint.NewAppendOnlyManagerForPolicy(readerBucket, "readonly-selector", t.TempDir(), policy, 1)
			if err != nil {
				t.Fatal(err)
			}
			wal, err := qlog.Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = wal.Close() })
			cluster := core.CurrentCluster()
			identity, err := hex.DecodeString(cluster.Members[0].WALIdentity)
			if err != nil || len(identity) != 32 {
				t.Fatal("invalid fixture WAL identity", err)
			}
			if err := wal.BindIdentity(identity); err != nil {
				t.Fatal(err)
			}
			readerArchive, err := recovery.NewAppendOnlyManagerWithValueValidator(bucket, "readonly-selector", 1, types.ValidateManagedGraphExecutionPolicy)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(readerArchive.Close)
			if err := readerArchive.Load(ctx); err != nil || readerArchive.Tip() != 1 {
				t.Fatal("load independent pre-publication archive snapshot", err)
			}
			reader := New(&types.ExecutionConfig{NodeID: producer.config.NodeID})
			reader.checkpoints, reader.archive, reader.managedStartup = cp, readerArchive, producer.managedStartup
			reader.core, err = quepaxa.New(quepaxa.Config{
				NodeID: reader.config.NodeID, Cluster: cluster, WAL: wal,
				ValidateValue: types.ValidateManagedGraphExecutionPolicy,
				ManagedAppendOnly: &quepaxa.ManagedAppendOnlyConfig{
					Binding: quepaxa.ManagedAppendOnlyBinding{
						ApplicationProtocol: reader.managedStartup.Identity.ApplicationProtocol,
						BootstrapPlanDigest: strings.Repeat("0", 63) + "1",
						MemberConfigDigest:  strings.Repeat("0", 63) + "2",
						WALIdentity:         [32]byte(identity),
					},
					Verify: func(ctx context.Context, candidate quepaxa.ManagedCheckpointCandidate) error {
						return cp.Verify(ctx, uint64(candidate.Seal.Index), candidate.Seal.RootHash, candidate.Seal.StateHash)
					},
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			reader.core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
				return cp.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
			})
			prefix, _, err := core.DecisionsFrom(1, 1)
			if err != nil || len(prefix) != 1 {
				t.Fatal("actual pre-QCKP prefix unavailable", err)
			}
			if err := reader.core.AcceptCertifiedValues(prefix); err != nil {
				t.Fatal(err)
			}
			if _, ok, err := reader.core.LatestManagedCheckpointSeal(); err != nil || ok {
				t.Fatal("reader must start with no certified QCKP", err)
			}
			mutations := -1
			readerBucket.afterMissing = func() {
				switch tc {
				case "published":
					if err := producer.promoteManagedAppendOnlySeal(ctx, seal); err != nil {
						t.Fatal("publish actual first QCKP and CURRENT", err)
					}
				case "unarchived":
					// Deliberately publish an invalid hint without its archived certificate.
					if err := producer.checkpoints.PromoteManagedAppendOnlyLocator(ctx, checkpoint.ManagedAppendOnlyLocator{
						Policy: int(policy), Index: uint64(seal.Index), RootHash: seal.RootHash,
						SealDecisionSlot: uint64(seal.DecisionSlot), SealDecisionHash: decision.Hash,
					}); err != nil {
						t.Fatal("install negative CURRENT hint", err)
					}
				}
				mutations = len(bucket.mutationSnapshot())
			}
			root, selected, err := reader.selectManagedGraphStartupRoot(ctx)
			if mutations < 0 || len(bucket.mutationSnapshot()) != mutations {
				t.Fatal("first CURRENT miss was not observed or selection wrote remote objects")
			}
			switch tc {
			case "published":
				if err != nil || root == nil || selected == nil || root.RootHash != seal.RootHash ||
					selected.DecisionSlot != seal.DecisionSlot || reader.core.Tip() != seal.DecisionSlot {
					t.Fatalf("late certified CURRENT must select the exact archived pair: selected=%+v err=%v", selected, err)
				}
			case "unarchived":
				if err == nil || !strings.Contains(err.Error(), "requires an authenticated producer root") ||
					!strings.Contains(err.Error(), "CURRENT has no certified QCKP") || root != nil || selected != nil || reader.core.Tip() != 1 {
					t.Fatalf("CURRENT without archived QCKP must not select a root or allow genesis: selected=%+v err=%v", selected, err)
				}
			case "absent":
				if err != nil || root != nil || selected != nil || reader.core.Tip() != 1 {
					t.Fatalf("absent CURRENT must leave the local genesis guards to the caller: selected=%+v err=%v", selected, err)
				}
			}
		})
	}
}
