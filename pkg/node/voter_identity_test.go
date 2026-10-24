package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

// peerMember derives the public identity a peer token grants one node, so test
// members carry no secret.
func peerMember(clusterID string, id quepaxa.NodeID, token string) quepaxa.Member {
	return quepaxa.Member{ID: id, PublicKey: quepaxa.PublicKey(network.PeerPublicKey(types.ClusterID(clusterID), id, token))}
}

func voterTestConfig(t *testing.T) *types.ExecutionConfig {
	t.Helper()
	return &types.ExecutionConfig{DataDir: t.TempDir(), ClusterID: "cluster", NodeID: "a", ObjStoreProvider: "s3", ObjStoreBucket: "bucket", Members: []quepaxa.Member{peerMember("cluster", "a", "a-token"), peerMember("cluster", "b", "b-token"), peerMember("cluster", "c", "c-token")}}
}

func registerTestVoter(t *testing.T, config *types.ExecutionConfig, bucket objstore.Bucket, enroll bool) error {
	t.Helper()
	state, err := loadVoterIdentity(config)
	if err != nil {
		return err
	}
	wal, err := qlog.Open(filepath.Join(config.DataDir, "qlog"))
	if err != nil {
		return err
	}
	defer wal.Close()
	return ensureVoterIdentity(context.Background(), config, bucket, wal, state, enroll)
}

type voterIdentityRaceBucket struct {
	objstore.Bucket
	membershipKey        string
	beforeIter           func()
	iterErr              error
	failMembershipReadAt int
	membershipReads      int
	iterKeys             []string
}

func (b *voterIdentityRaceBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if name == b.membershipKey {
		b.membershipReads++
		if b.failMembershipReadAt == b.membershipReads {
			return nil, errors.New("injected canonical membership read failure")
		}
	}
	return b.Bucket.Get(ctx, name)
}

func (b *voterIdentityRaceBucket) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	if b.beforeIter != nil {
		beforeIter := b.beforeIter
		b.beforeIter = nil
		beforeIter()
	}
	if b.iterErr != nil {
		return b.iterErr
	}
	return b.Bucket.Iter(ctx, dir, func(key string) error {
		b.iterKeys = append(b.iterKeys, key)
		return f(key)
	}, options...)
}

func TestVoterIdentityRejectsLostWALBeforeVoting(t *testing.T) {
	for _, mode := range []types.ObjectStoreDurability{types.ObjectStoreDurabilityAsync, types.ObjectStoreDurabilityBeforeAck} {
		t.Run(string(mode), func(t *testing.T) {
			config, bucket := voterTestConfig(t), objstore.NewInMemBucket()
			config.ObjStoreDurability = mode
			if err := registerTestVoter(t, config, bucket, false); err != nil {
				t.Fatal(err)
			}
			// The remote archive can still be empty: recorder promises precede
			// decided-value publication in both durability modes.
			wal, err := qlog.Open(filepath.Join(config.DataDir, "qlog"))
			if err != nil {
				t.Fatal(err)
			}
			if err := wal.Append(qlog.Entry{Slot: 1, Type: qlog.EntryReceipt, Payload: []byte("unpublished recorder state")}); err != nil {
				t.Fatal(err)
			}
			if err := wal.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := wal.Close(); err != nil {
				t.Fatal(err)
			}
			if err := registerTestVoter(t, config, bucket, false); err != nil {
				t.Fatalf("intact restart: %v", err)
			}
			lost := *config
			lost.DataDir = t.TempDir()
			if err := registerTestVoter(t, &lost, bucket, false); !errors.Is(err, ErrVoterStateLost) {
				t.Fatalf("empty replacement: %v", err)
			}
			if err := registerTestVoter(t, &lost, bucket, true); !errors.Is(err, ErrVoterStateLost) {
				t.Fatalf("enrollment bypass: %v", err)
			}
			if err := os.RemoveAll(filepath.Join(config.DataDir, "qlog")); err != nil {
				t.Fatal(err)
			}
			if err := registerTestVoter(t, config, bucket, false); !errors.Is(err, ErrVoterStateLost) {
				t.Fatalf("lost original WAL: %v", err)
			}
		})
	}
}

func TestVoterIdentityRejectsMissingManifestAndConfigurationChanges(t *testing.T) {
	config, bucket := voterTestConfig(t), objstore.NewInMemBucket()
	if err := registerTestVoter(t, config, bucket, false); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*types.ExecutionConfig){
		func(c *types.ExecutionConfig) { c.ClusterID = "other" },
		func(c *types.ExecutionConfig) { c.NodeID = "b" },
		func(c *types.ExecutionConfig) { c.Members = c.Members[:1] },
		func(c *types.ExecutionConfig) { c.ObjStorePrefix = "other" },
		func(c *types.ExecutionConfig) { c.ObjStoreDurability = types.ObjectStoreDurabilityBeforeAck },
	} {
		changed := *config
		change(&changed)
		if err := registerTestVoter(t, &changed, bucket, false); !errors.Is(err, ErrVoterStateLost) {
			t.Fatalf("configuration changed: %v", err)
		}
	}
	entries, err := filepath.Glob(filepath.Join(config.DataDir, "qlog", "manifest_*.bin"))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if err := os.Remove(entry); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := loadVoterIdentity(config); !errors.Is(err, ErrVoterStateLost) {
		t.Fatalf("missing manifest: %v", err)
	}
}

func TestMissingCanonicalMembershipRejectsDifferentRegisteredVoter(t *testing.T) {
	ctx := context.Background()
	original, bucket := voterTestConfig(t), objstore.NewInMemBucket()
	if err := registerTestVoter(t, original, bucket, false); err != nil {
		t.Fatal(err)
	}

	prefix := path.Join(string(original.ClusterID), "voters")
	oldIdentityKey := path.Join(prefix, voterIdentityKey(string(original.NodeID)))
	oldIdentity, err := readVoterRegistration(ctx, bucket, oldIdentityKey)
	if err != nil || oldIdentity == nil {
		t.Fatalf("seeded voter identity missing: present=%t err=%v", oldIdentity != nil, err)
	}
	membershipKey := path.Join(prefix, "membership.json")
	if err := bucket.Delete(ctx, membershipKey); err != nil {
		t.Fatal(err)
	}
	if data, err := readVoterRegistration(ctx, bucket, membershipKey); err != nil || data != nil {
		t.Fatalf("canonical membership was not removed from fixture: present=%t err=%v", data != nil, err)
	}

	fresh := *original
	fresh.DataDir = t.TempDir()
	fresh.NodeID = "fresh-node"
	fresh.Members = append(append([]quepaxa.Member(nil), original.Members...), peerMember("cluster", "fresh-node", "fresh-token"))
	if err := registerTestVoter(t, &fresh, bucket, false); !errors.Is(err, ErrVoterStateLost) {
		newMembership, membershipErr := readVoterRegistration(ctx, bucket, membershipKey)
		newIdentity, identityErr := readVoterRegistration(ctx, bucket, path.Join(prefix, voterIdentityKey(string(fresh.NodeID))))
		t.Fatalf("missing canonical membership error=%v, want ErrVoterStateLost; new_membership=%t membership_read_err=%v new_identity=%t identity_read_err=%v",
			err,
			newMembership != nil, membershipErr, newIdentity != nil, identityErr)
	}
	if got, err := readVoterRegistration(ctx, bucket, membershipKey); err != nil || got != nil {
		t.Fatalf("orphaned voter identity caused canonical membership write: present=%t err=%v", got != nil, err)
	}
	if got, err := readVoterRegistration(ctx, bucket, path.Join(prefix, voterIdentityKey(string(fresh.NodeID)))); err != nil || got != nil {
		t.Fatalf("orphaned voter identity caused new remote identity write: present=%t err=%v", got != nil, err)
	}
}

func TestOfflineEnrollmentRejectsOrphanedVoterIdentity(t *testing.T) {
	ctx := context.Background()
	original, bucket := voterTestConfig(t), objstore.NewInMemBucket()
	if err := registerTestVoter(t, original, bucket, false); err != nil {
		t.Fatal(err)
	}
	prefix := path.Join(string(original.ClusterID), "voters")
	membershipKey := path.Join(prefix, "membership.json")
	if err := bucket.Delete(ctx, membershipKey); err != nil {
		t.Fatal(err)
	}

	fresh := *original
	fresh.DataDir = t.TempDir()
	fresh.NodeID = "fresh-node"
	fresh.Members = append(append([]quepaxa.Member(nil), original.Members...), peerMember("cluster", "fresh-node", "fresh-token"))
	wal, err := qlog.Open(filepath.Join(fresh.DataDir, "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	if err := registerTestVoter(t, &fresh, bucket, true); !errors.Is(err, ErrVoterStateLost) {
		t.Fatalf("offline enrollment with orphaned voter identity error=%v, want ErrVoterStateLost", err)
	}
	state, stateErr := loadVoterIdentity(&fresh)
	if stateErr != nil || state.identity != nil {
		t.Fatalf("rejected offline enrollment wrote local identity: state=%+v err=%v", state.identity, stateErr)
	}
	for _, key := range []string{membershipKey, path.Join(prefix, voterIdentityKey(string(fresh.NodeID)))} {
		data, readErr := readVoterRegistration(ctx, bucket, key)
		if readErr != nil || data != nil {
			t.Fatalf("rejected offline enrollment wrote %q: present=%t err=%v", key, data != nil, readErr)
		}
	}
}

func TestSingleVoterRecoveryRejectsOrphanedMultiVoterIdentity(t *testing.T) {
	ctx := context.Background()
	original, bucket := voterTestConfig(t), objstore.NewInMemBucket()
	if err := registerTestVoter(t, original, bucket, false); err != nil {
		t.Fatal(err)
	}
	prefix := path.Join(string(original.ClusterID), "voters")
	membershipKey := path.Join(prefix, "membership.json")
	if err := bucket.Delete(ctx, membershipKey); err != nil {
		t.Fatal(err)
	}

	single := *original
	single.DataDir = t.TempDir()
	single.NodeID = "single"
	single.Members = []quepaxa.Member{peerMember("cluster", single.NodeID, "single-token")}
	if err := registerTestVoter(t, &single, bucket, false); !errors.Is(err, ErrVoterStateLost) {
		t.Fatalf("single-voter recovery with orphaned multi-voter identity error=%v, want ErrVoterStateLost", err)
	}
	for _, key := range []string{membershipKey, path.Join(prefix, voterIdentityKey(string(single.NodeID)))} {
		data, readErr := readVoterRegistration(ctx, bucket, key)
		if readErr != nil || data != nil {
			t.Fatalf("single-voter recovery wrote %q despite orphaned identity: present=%t err=%v", key, data != nil, readErr)
		}
	}
}

func TestSingleVoterRecoveryStillAllowsEmptyUnregisteredNamespace(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	config := voterTestConfig(t)
	config.NodeID = "single"
	config.Members = []quepaxa.Member{peerMember("cluster", config.NodeID, "single-token")}
	if err := registerTestVoter(t, config, bucket, false); err != nil {
		t.Fatalf("empty single-voter archive recovery: %v", err)
	}
	prefix := path.Join(string(config.ClusterID), "voters")
	for _, key := range []string{path.Join(prefix, "membership.json"), path.Join(prefix, voterIdentityKey(string(config.NodeID)))} {
		data, readErr := readVoterRegistration(ctx, bucket, key)
		if readErr != nil || data != nil {
			t.Fatalf("single-voter archive recovery wrote %q: present=%t err=%v", key, data != nil, readErr)
		}
	}
}

func TestSingleVoterRecoveryDoesNotIgnoreConcurrentMembershipOnly(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	config := voterTestConfig(t)
	config.NodeID = "single"
	config.Members = []quepaxa.Member{peerMember("cluster", config.NodeID, "single-token")}
	prefix := path.Join(string(config.ClusterID), "voters")
	membershipKey := path.Join(prefix, "membership.json")
	membership, err := json.Marshal(recovery.NewMembershipRecord(string(config.ClusterID), config.Members, string(config.ObjStoreDurability)))
	if err != nil {
		t.Fatal(err)
	}
	interleaved := &voterIdentityRaceBucket{
		Bucket: bucket, membershipKey: membershipKey,
		beforeIter: func() {
			if err := bucket.Upload(ctx, membershipKey, bytes.NewReader(membership), objstore.WithIfNotExists()); err != nil {
				t.Fatalf("publish concurrent canonical membership: %v", err)
			}
		},
	}
	if err := registerTestVoter(t, config, interleaved, false); !errors.Is(err, ErrVoterStateLost) {
		t.Fatalf("single-voter recovery ignored membership published during scan: %v", err)
	}
	if got, readErr := readVoterRegistration(ctx, bucket, membershipKey); readErr != nil || !bytes.Equal(got, membership) {
		t.Fatalf("concurrent canonical membership changed: equal=%t err=%v", bytes.Equal(got, membership), readErr)
	}
	if got, readErr := readVoterRegistration(ctx, bucket, path.Join(prefix, voterIdentityKey(string(config.NodeID)))); readErr != nil || got != nil {
		t.Fatalf("single-voter recovery wrote remote identity: present=%t err=%v", got != nil, readErr)
	}
}

func TestAssignedRestartAcceptsRereadOfPreservedGenesis(t *testing.T) {
	ctx := context.Background()
	original, bucket := voterTestConfig(t), objstore.NewInMemBucket()
	original.ObjStoreDurability = types.ObjectStoreDurabilityAsync
	if err := registerTestVoter(t, original, bucket, false); err != nil {
		t.Fatal(err)
	}
	prefix := path.Join(string(original.ClusterID), "voters")
	membershipKey := path.Join(prefix, "membership.json")
	genesis, err := readVoterRegistration(ctx, bucket, membershipKey)
	if err != nil || genesis == nil {
		t.Fatalf("seeded canonical genesis missing: present=%t err=%v", genesis != nil, err)
	}
	if err := bucket.Delete(ctx, membershipKey); err != nil {
		t.Fatal(err)
	}

	current := *original
	current.Members = []quepaxa.Member{
		peerMember("cluster", "a", "a-token"),
		peerMember("cluster", "b", "b-token"),
		peerMember("cluster", "d", "d-token"),
	}
	interleaved := &voterIdentityRaceBucket{
		Bucket: bucket, membershipKey: membershipKey,
		beforeIter: func() {
			if err := bucket.Upload(ctx, membershipKey, bytes.NewReader(genesis), objstore.WithIfNotExists()); err != nil {
				t.Fatalf("restore concurrent immutable genesis: %v", err)
			}
		},
	}
	local, err := loadVoterIdentity(&current)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := qlog.Open(filepath.Join(current.DataDir, "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	if err := ensureVoterIdentityWithHooks(ctx, &current, interleaved, wal, local, false, nil, nil, true); err != nil {
		t.Fatalf("assigned restart rejected reread of preserved genesis: %v", err)
	}
}

func TestMissingMembershipConcurrentRegistrationRechecksCanonicalWinner(t *testing.T) {
	ctx := context.Background()
	winner, bucket := voterTestConfig(t), objstore.NewInMemBucket()
	candidate := *winner
	candidate.NodeID = "b"
	candidate.DataDir = t.TempDir()
	prefix := path.Join(string(winner.ClusterID), "voters")
	membershipKey := path.Join(prefix, "membership.json")
	tracingBucket := &voterIdentityRaceBucket{
		Bucket: bucket, membershipKey: membershipKey,
		beforeIter: func() {
			if err := registerTestVoter(t, &candidate, bucket, false); err != nil {
				t.Fatalf("concurrent first registration: %v", err)
			}
		},
	}
	if err := registerTestVoter(t, winner, tracingBucket, false); err != nil {
		t.Fatalf("matching concurrent canonical registration rejected: %v", err)
	}
	if tracingBucket.membershipReads != 2 {
		t.Fatalf("membership read count after registration=%d, want initial read plus canonical re-read", tracingBucket.membershipReads)
	}
	for _, nodeID := range []string{"a", "b"} {
		data, err := readVoterRegistration(ctx, bucket, path.Join(prefix, voterIdentityKey(nodeID)))
		if err != nil || data == nil {
			t.Fatalf("concurrent voter %s identity missing: present=%t err=%v", nodeID, data != nil, err)
		}
	}
}

func TestMissingMembershipConcurrentRegistrationWithArchiveObject(t *testing.T) {
	for _, tc := range []struct {
		name     string
		publish  bool
		conflict bool
		wantErr  error
	}{
		{name: "matching canonical winner", publish: true},
		{name: "canonical still absent", wantErr: ErrVoterEnrollmentRequired},
		{name: "conflicting canonical winner", publish: true, conflict: true, wantErr: ErrVoterStateLost},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			candidate, bucket := voterTestConfig(t), objstore.NewInMemBucket()
			prefix := path.Join(string(candidate.ClusterID), "voters")
			membershipKey := path.Join(prefix, "membership.json")
			archiveKey := path.Join(string(candidate.ClusterID), "archive", "head.bin")
			var winnerMembership []byte
			tracingBucket := &voterIdentityRaceBucket{
				Bucket: bucket, membershipKey: membershipKey,
				beforeIter: func() {
					if tc.publish {
						winner := *candidate
						winner.NodeID = "b"
						winner.DataDir = t.TempDir()
						if tc.conflict {
							winner.Members = append([]quepaxa.Member(nil), candidate.Members[:2]...)
						}
						if err := registerTestVoter(t, &winner, bucket, false); err != nil {
							t.Fatalf("concurrent canonical registration: %v", err)
						}
						var readErr error
						winnerMembership, readErr = readVoterRegistration(ctx, bucket, membershipKey)
						if readErr != nil || winnerMembership == nil {
							t.Fatalf("concurrent canonical membership missing: present=%t err=%v", winnerMembership != nil, readErr)
						}
					}
					if err := bucket.Upload(ctx, archiveKey, bytes.NewReader([]byte("archive head")), objstore.WithIfNotExists()); err != nil {
						t.Fatalf("publish concurrent archive object: %v", err)
					}
				},
			}

			err := registerTestVoter(t, candidate, tracingBucket, false)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("matching concurrent canonical registration with archive rejected: %v", err)
				}
			} else if !errors.Is(err, tc.wantErr) {
				t.Fatalf("registration error=%v, want %v", err, tc.wantErr)
			}
			if len(tracingBucket.iterKeys) != 1 || tracingBucket.iterKeys[0] != path.Join(string(candidate.ClusterID), "archive")+"/" {
				t.Fatalf("prefix scan keys=%v, want early stop at archive directory", tracingBucket.iterKeys)
			}
			archiveData, archiveErr := readVoterRegistration(ctx, bucket, archiveKey)
			if archiveErr != nil || string(archiveData) != "archive head" {
				t.Fatalf("archive object changed: data=%q err=%v", archiveData, archiveErr)
			}
			membershipData, membershipErr := readVoterRegistration(ctx, bucket, membershipKey)
			identityData, identityErr := readVoterRegistration(ctx, bucket, path.Join(prefix, voterIdentityKey(string(candidate.NodeID))))
			if tc.wantErr == nil {
				if membershipErr != nil || !bytes.Equal(membershipData, winnerMembership) {
					t.Fatalf("matching canonical membership changed: equal=%t err=%v", bytes.Equal(membershipData, winnerMembership), membershipErr)
				}
				if identityErr != nil || identityData == nil {
					t.Fatalf("candidate identity missing after matching winner: present=%t err=%v", identityData != nil, identityErr)
				}
				return
			}
			if tc.publish {
				if membershipErr != nil || !bytes.Equal(membershipData, winnerMembership) {
					t.Fatalf("conflicting canonical membership changed: equal=%t err=%v", bytes.Equal(membershipData, winnerMembership), membershipErr)
				}
			} else if membershipErr != nil || membershipData != nil {
				t.Fatalf("rejected archive-only namespace gained membership: present=%t err=%v", membershipData != nil, membershipErr)
			}
			if identityErr != nil || identityData != nil {
				t.Fatalf("rejected registration wrote candidate identity: present=%t err=%v", identityData != nil, identityErr)
			}
		})
	}
}

func TestMissingMembershipRejectsDifferentConcurrentCanonicalWinner(t *testing.T) {
	ctx := context.Background()
	candidate, bucket := voterTestConfig(t), objstore.NewInMemBucket()
	candidate.ObjStoreDurability = types.ObjectStoreDurabilityAsync
	concurrent := *candidate
	concurrent.NodeID = "b"
	concurrent.DataDir = t.TempDir()
	concurrent.Members = append([]quepaxa.Member(nil), candidate.Members[:2]...)
	prefix := path.Join(string(candidate.ClusterID), "voters")
	membershipKey := path.Join(prefix, "membership.json")
	var concurrentMembership []byte
	tracingBucket := &voterIdentityRaceBucket{
		Bucket: bucket, membershipKey: membershipKey,
		beforeIter: func() {
			if err := registerTestVoter(t, &concurrent, bucket, false); err != nil {
				t.Fatalf("concurrent conflicting registration: %v", err)
			}
			var err error
			concurrentMembership, err = readVoterRegistration(ctx, bucket, membershipKey)
			if err != nil || concurrentMembership == nil {
				t.Fatalf("concurrent canonical membership missing: present=%t err=%v", concurrentMembership != nil, err)
			}
		},
	}
	err := registerTestVoter(t, candidate, tracingBucket, false)
	if !errors.Is(err, ErrVoterStateLost) {
		t.Fatalf("different concurrent canonical membership error=%v, want ErrVoterStateLost", err)
	}
	state, stateErr := loadVoterIdentity(candidate)
	if stateErr != nil || state.identity != nil {
		t.Fatalf("conflicting canonical winner wrote local voter identity: state=%+v err=%v", state.identity, stateErr)
	}
	data, readErr := readVoterRegistration(ctx, bucket, path.Join(prefix, voterIdentityKey("a")))
	if readErr != nil || data != nil {
		t.Fatalf("conflicting canonical winner wrote candidate identity: present=%t err=%v", data != nil, readErr)
	}
	data, readErr = readVoterRegistration(ctx, bucket, membershipKey)
	if readErr != nil || !bytes.Equal(data, concurrentMembership) {
		t.Fatalf("conflicting canonical winner changed: equal=%t err=%v", bytes.Equal(data, concurrentMembership), readErr)
	}
}

func TestMissingMembershipRecheckAndPrefixScanErrorsDoNotRegister(t *testing.T) {
	for _, failure := range []string{"canonical reread", "prefix scan"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			original, base := voterTestConfig(t), objstore.NewInMemBucket()
			if err := registerTestVoter(t, original, base, false); err != nil {
				t.Fatal(err)
			}
			prefix := path.Join(string(original.ClusterID), "voters")
			membershipKey := path.Join(prefix, "membership.json")
			if err := base.Delete(ctx, membershipKey); err != nil {
				t.Fatal(err)
			}

			fresh := *original
			fresh.NodeID = "fresh-node"
			fresh.DataDir = t.TempDir()
			fresh.Members = append(append([]quepaxa.Member(nil), original.Members...), peerMember("cluster", "fresh-node", "fresh-token"))
			injected := errors.New("injected prefix scan failure")
			tracingBucket := &voterIdentityRaceBucket{Bucket: base, membershipKey: membershipKey}
			if failure == "canonical reread" {
				tracingBucket.failMembershipReadAt = 2
			} else {
				tracingBucket.iterErr = injected
			}
			err := registerTestVoter(t, &fresh, tracingBucket, false)
			if err == nil {
				t.Fatal("missing canonical membership failure was accepted")
			}
			if failure == "prefix scan" && !errors.Is(err, injected) {
				t.Fatalf("prefix scan error=%v, want injected scan error", err)
			}
			state, stateErr := loadVoterIdentity(&fresh)
			if stateErr != nil || state.identity != nil {
				t.Fatalf("failed recheck wrote local voter identity: state=%+v err=%v", state.identity, stateErr)
			}
			for _, key := range []string{membershipKey, path.Join(prefix, voterIdentityKey(string(fresh.NodeID)))} {
				data, readErr := readVoterRegistration(ctx, base, key)
				if readErr != nil || data != nil {
					t.Fatalf("failed recheck wrote %q: present=%t err=%v", key, data != nil, readErr)
				}
			}
		})
	}
}

func TestVoterIdentityRequiresExplicitLegacyEnrollment(t *testing.T) {
	config, bucket := voterTestConfig(t), objstore.NewInMemBucket()
	wal, err := qlog.Open(filepath.Join(config.DataDir, "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	if err := registerTestVoter(t, config, bucket, false); !errors.Is(err, ErrVoterEnrollmentRequired) {
		t.Fatalf("automatic legacy adoption: %v", err)
	}
	if err := registerTestVoter(t, config, bucket, true); err != nil {
		t.Fatal(err)
	}
	if err := registerTestVoter(t, config, bucket, false); err != nil {
		t.Fatalf("registered restart: %v", err)
	}
}

func TestPendingLearnerIdentityUsesOnlyAuthorityCreateOnlyHook(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	base := voterTestConfig(t)
	if err := registerTestVoter(t, base, bucket, false); err != nil {
		t.Fatal(err)
	}
	learner := *base
	learner.DataDir = t.TempDir()
	learner.NodeID = "learner-new"
	learnerMember := peerMember("cluster", learner.NodeID, "learner-token")
	learner.Learner = &learnerMember
	local, err := loadVoterIdentity(&learner)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := qlog.Open(filepath.Join(learner.DataDir, "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	var registeredKey string
	bind := func(_ context.Context, _ string) error { return nil }
	write := func(ctx context.Context, key string, data []byte) error {
		registeredKey = key
		return bucket.Upload(ctx, key, bytes.NewReader(data), objstore.WithIfNotExists())
	}
	if err := ensureVoterIdentityWithHooks(ctx, &learner, bucket, wal, local, false, []func(context.Context, string) error{bind}, []func(context.Context, string, []byte) error{write}); err != nil {
		t.Fatal(err)
	}
	wantKey := path.Join("cluster", "voters", voterIdentityKey(string(learner.NodeID)))
	if registeredKey != wantKey {
		t.Fatalf("authority hook key=%q want=%q", registeredKey, wantKey)
	}
	if _, bound := wal.Identity(); !bound {
		t.Fatal("pending learner identity registration preceded WAL binding")
	}
}

func voterIdentityKey(nodeID string) string {
	return fmt.Sprintf("%x.json", sha256.Sum256([]byte(nodeID)))
}

func TestVoterIdentityResumesInterruptedRegistration(t *testing.T) {
	config, bucket := voterTestConfig(t), objstore.NewInMemBucket()
	if err := registerTestVoter(t, config, bucket, false); err != nil {
		t.Fatal(err)
	}
	state, err := loadVoterIdentity(config)
	if err != nil {
		t.Fatal(err)
	}
	// A crash after remote CAS but before the local completion marker must
	// resume with the SAME nonce, never allocate another voter identity.
	state.identity.Registered = false
	if err := writeVoterIdentity(config.DataDir, *state.identity); err != nil {
		t.Fatal(err)
	}
	if err := registerTestVoter(t, config, bucket, false); err != nil {
		t.Fatalf("retry: %v", err)
	}
	state, err = loadVoterIdentity(config)
	if err != nil || !state.identity.Registered {
		t.Fatalf("incomplete registration: %v", err)
	}
}

func TestVoterIdentityRejectsReplacementWALWithPreservedMarker(t *testing.T) {
	config, bucket := voterTestConfig(t), objstore.NewInMemBucket()
	if err := registerTestVoter(t, config, bucket, false); err != nil {
		t.Fatal(err)
	}
	marker, err := os.ReadFile(filepath.Join(config.DataDir, "qlog", "voter-identity.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(config.DataDir, "qlog")); err != nil {
		t.Fatal(err)
	}
	wal, err := qlog.Open(filepath.Join(config.DataDir, "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(config.DataDir, "qlog", "voter-identity.json"), marker, 0600); err != nil {
		t.Fatal(err)
	}
	for _, enroll := range []bool{false, true} {
		if err := registerTestVoter(t, config, bucket, enroll); !errors.Is(err, ErrVoterStateLost) {
			t.Fatalf("replacement WAL enroll=%v: %v", enroll, err)
		}
	}
}

func TestVoterIdentityCompletesPendingWALBinding(t *testing.T) {
	config, bucket := voterTestConfig(t), objstore.NewInMemBucket()
	wal, err := qlog.Open(filepath.Join(config.DataDir, "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	pending := voterIdentityFor(config)
	pending.Nonce = "0101010101010101010101010101010101010101010101010101010101010101"
	if err := writeVoterIdentity(config.DataDir, pending); err != nil {
		t.Fatal(err)
	}
	if err := registerTestVoter(t, config, bucket, false); err != nil {
		t.Fatal(err)
	}
	state, err := loadVoterIdentity(config)
	if err != nil || !state.identity.Registered || state.identity.Nonce != pending.Nonce {
		t.Fatalf("pending registration not completed: %v", err)
	}
}

func TestConcurrentVoterRegistrationHasOneWinner(t *testing.T) {
	config, bucket := voterTestConfig(t), objstore.NewInMemBucket()
	replacement := *config
	replacement.DataDir = t.TempDir()
	results := make(chan error, 2)
	for _, candidate := range []*types.ExecutionConfig{config, &replacement} {
		go func() { results <- registerTestVoter(t, candidate, bucket, false) }()
	}
	successes := 0
	for range 2 {
		err := <-results
		if err == nil {
			successes++
		} else if !errors.Is(err, ErrVoterStateLost) {
			t.Fatal(err)
		}
	}
	if successes != 1 {
		t.Fatalf("got %d registrations for one identity", successes)
	}
}

func TestVoterIdentityRejectsUnknownDurabilityHistory(t *testing.T) {
	config, bucket := voterTestConfig(t), objstore.NewInMemBucket()
	if err := registerTestVoter(t, config, bucket, false); err != nil {
		t.Fatal(err)
	}
	// A legacy record has no mode history. Editing the environment must not
	// retroactively promise that every historical acknowledgement was archived.
	state, err := loadVoterIdentity(config)
	if err != nil {
		t.Fatal(err)
	}
	record := []byte(`{"version":1,"cluster":"cluster","membership":"` + state.identity.Membership + `"}`)
	if err := bucket.Upload(context.Background(), "cluster/voters/membership.json", bytes.NewReader(record)); err != nil {
		t.Fatal(err)
	}
	if err := registerTestVoter(t, config, bucket, false); !errors.Is(err, ErrVoterStateLost) {
		t.Fatalf("unknown durability history accepted: %v", err)
	}
}

func TestLocalModeCannotRebindRegisteredVoter(t *testing.T) {
	config, bucket := voterTestConfig(t), objstore.NewInMemBucket()
	if err := registerTestVoter(t, config, bucket, false); err != nil {
		t.Fatal(err)
	}
	local := &types.ExecutionConfig{Local: true, DataDir: config.DataDir, NodeID: config.NodeID, ClusterID: config.ClusterID}
	n := New(local)
	if err := n.Open(context.Background()); !errors.Is(err, ErrVoterStateLost) || !strings.Contains(err.Error(), "node open phase local_state:") {
		n.Shutdown()
		t.Fatalf("registered directory rejection lost its cause or phase: %v", err)
	}
	if n.opened.Load() {
		t.Fatal("failed open did not run existing shutdown cleanup")
	}
	if err := registerTestVoter(t, config, bucket, false); err != nil {
		t.Fatalf("rejection altered original voter: %v", err)
	}
}
