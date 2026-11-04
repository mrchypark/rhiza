//go:build rhiza_local_testhooks

package quepaxa

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

func TestUnpromotedLearnerWithoutRosterWALCannotQualifyAsVoter(t *testing.T) {
	cores, transport := newManagedControlRaceCluster(t, nil)
	cluster := cores["n1"].CurrentCluster()
	const learnerID NodeID = "n4"
	for _, member := range cluster.Members {
		if member.ID == learnerID {
			t.Fatal("fixture already includes the unpromoted learner")
		}
	}

	wal, err := qlog.Open(filepath.Join(t.TempDir(), string(learnerID)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	identity := make([]byte, sha256.Size)
	for i := range identity {
		identity[i] = byte(i + 41)
	}
	if err := wal.BindIdentity(identity); err != nil {
		t.Fatal(err)
	}
	var walIdentity [sha256.Size]byte
	copy(walIdentity[:], identity)
	managed := &ManagedAppendOnlyConfig{
		Binding: ManagedAppendOnlyBinding{
			ApplicationProtocol: managedRequestAppendOnlyApplicationProtocol,
			BootstrapPlanDigest: hex.EncodeToString(make([]byte, sha256.Size)),
			MemberConfigDigest:  hex.EncodeToString(identity),
			WALIdentity:         walIdentity,
		},
		Verify: func(context.Context, ManagedCheckpointCandidate) error { return nil },
	}
	learner, err := NewLearner(Config{
		NodeID: learnerID, Cluster: cluster, WAL: wal, Transport: transport,
		ManagedAppendOnly: managed,
	})
	if err != nil {
		t.Fatalf("construct actual unpromoted learner: %v", err)
	}
	if learner.IsVoter() || learner.WALIdentity() != hex.EncodeToString(identity) {
		t.Fatalf("learner role or fresh WAL identity changed: voter=%t wal=%q", learner.IsVoter(), learner.WALIdentity())
	}
	// Probe the preserved voter predicate with an input-only clone. The real
	// learner remains unpromoted and its actual Core roster is never rewritten.
	noWALRoster := cloneCluster(cluster)
	noWALRoster.Members = append(noWALRoster.Members, Member{ID: learnerID})
	learner.mu.RLock()
	qualifies := learner.voterForClusterLocked(noWALRoster)
	learner.mu.RUnlock()
	if qualifies {
		t.Fatal("plain learner qualified in a roster entry without a WAL identity")
	}
	if learner.IsVoter() || learner.ConfigID() != cluster.ConfigID {
		t.Fatalf("predicate probe changed learner authority: voter=%t config=%d", learner.IsVoter(), learner.ConfigID())
	}

	// The public constructor also rejects a learner already named by a roster
	// entry with no WAL identity; promotion must come from a certified target.
	invalidWAL, err := qlog.Open(filepath.Join(t.TempDir(), "invalid-roster"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = invalidWAL.Close() })
	invalidCluster := cloneCluster(cluster)
	invalidCluster.Members = append(invalidCluster.Members, Member{ID: learnerID})
	if _, err := NewLearner(Config{NodeID: learnerID, Cluster: invalidCluster, WAL: invalidWAL, Transport: transport}); err == nil {
		t.Fatal("NewLearner accepted a roster entry without a bound WAL identity")
	}
}
