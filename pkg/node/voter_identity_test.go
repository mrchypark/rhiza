package node

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

func voterTestConfig(t *testing.T) *types.ExecutionConfig {
	t.Helper()
	return &types.ExecutionConfig{DataDir: t.TempDir(), ClusterID: "cluster", NodeID: "a", ObjStoreProvider: "s3", ObjStoreBucket: "bucket", Members: []quepaxa.Member{{ID: "a", Token: "a-token"}, {ID: "b", Token: "b-token"}, {ID: "c", Token: "c-token"}}}
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
