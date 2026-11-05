package node

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

// Reuse the real registered-membership fixture and actual learner admission.
// No certificate, membership transition, or captured archive is manufactured.
func TestPreparedAdoptionCertifiedMembershipRoundTripBound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var learner *quepaxa.Core
	verifyLearner := func(ctx context.Context, target quepaxa.Cluster, through quepaxa.Slot, prefix [32]byte) error {
		if learner == nil {
			return errors.New("actual learner unavailable")
		}
		if err := learner.WaitTip(ctx, through); err != nil {
			return err
		}
		if err := learner.EnsureDurableThrough(ctx, through); err != nil {
			return err
		}
		got, ok := learner.PrefixHash(through)
		if !ok || got != prefix {
			return errors.New("actual learner prefix differs")
		}
		for _, member := range target.Members {
			if member.ID == learner.NodeID() && member.WALIdentity == learner.WALIdentity() {
				return nil
			}
		}
		return errors.New("target lacks exact registered learner")
	}
	n, bucket, transport, _ := terminalRetryFixture(t, verifyLearner)
	bootstrap := n.core.CurrentCluster()
	archive, err := recovery.NewAppendOnlyManagerWithValueValidator(bucket, "adoption-bound", 1, types.ValidateManagedKVExecutionPolicy)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(archive.Close)
	if err := archive.Load(ctx); err != nil {
		t.Fatal(err)
	}
	var lastView *recovery.ReadOnlyView
	var lastMembership quepaxa.MembershipRecord
	var lastWritten []byte
	identity := recovery.PreparedStartupIdentity{ClusterUID: "cluster", Generation: 0, DeploymentUID: "deployment", PodUID: "pod", NodeID: "a", SourceBinding: strings.Repeat("1", 64), ConfigDigest: strings.Repeat("2", 64)}
	for cycle := 1; cycle <= 2; cycle++ {
		member := registerMembershipLearner(t, bucket, quepaxa.NodeID(fmt.Sprintf("rhiza-n-%064d", cycle)))
		wal, err := qlog.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := wal.Close(); err != nil {
				t.Error(err)
			}
		})
		nonce, err := hex.DecodeString(member.WALIdentity)
		if err != nil {
			t.Fatal(err)
		}
		if err := wal.BindIdentity(nonce); err != nil {
			t.Fatal(err)
		}
		learner, err = quepaxa.NewLearner(quepaxa.Config{NodeID: member.ID, Cluster: bootstrap, WAL: wal, Transport: transport, EnableReconfiguration: true, ReconfigurationAdmission: verifyLearner})
		if err != nil {
			t.Fatal(err)
		}
		prior, _, err := n.core.DecisionsFrom(1, 256)
		if err != nil {
			t.Fatal(err)
		}
		if err := learner.AcceptCertifiedValues(prior); err != nil {
			t.Fatal(err)
		}
		transport.cores[member.ID] = learner
		transport.ids = append(transport.ids, member.ID)
		current := n.core.CurrentCluster()
		add := quepaxa.Cluster{ConfigID: current.ConfigID + 1, Members: append(append([]quepaxa.Member(nil), current.Members...), member)}
		if _, err := n.core.BeginReconfiguration(ctx, add); err != nil {
			t.Fatal("actual ADD freeze", err)
		}
		if err := n.core.FinishReconfiguration(ctx); err != nil {
			t.Fatal("actual ADD terminal", err)
		}
		remove := quepaxa.Cluster{ConfigID: add.ConfigID + 1, Members: []quepaxa.Member{bootstrap.Members[0], bootstrap.Members[1], member}}
		if _, err := n.core.BeginReconfiguration(ctx, remove); err != nil {
			t.Fatal("actual REMOVE freeze", err)
		}
		if err := n.core.FinishReconfiguration(ctx); err != nil {
			t.Fatal("actual REMOVE terminal", err)
		}
		if err := archive.SyncThrough(ctx, n.core, n.core.Tip()); err != nil {
			t.Fatal(err)
		}
		view, err := recovery.OpenReadOnlyViewForApplicationProtocol(ctx, bucket, "adoption-bound", 1, bootstrap, sqlpolicy.ManagedRequestMaterializationPolicy, types.DefaultIdempotencyWindowSlots, sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, true)
		if err != nil {
			t.Fatal("verify actual captured history", err)
		}
		membership, err := n.core.MembershipHistory()
		if err != nil || len(membership.Transitions) != 2*cycle {
			t.Fatal("actual certified membership history missing", err)
		}
		for _, transition := range membership.Transitions {
			for _, d := range []quepaxa.DecidedValue{transition.Freeze, transition.Terminal} {
				actual, ok := n.core.CertifiedValue(d.Slot)
				if !ok || len(d.Certificate) == 0 || !reflect.DeepEqual(actual, d) {
					t.Fatal("membership lacks exact actual certificates")
				}
			}
		}
		lastView, lastMembership = view, membership
		t.Run(fmt.Sprintf("C%d", remove.ConfigID), func(t *testing.T) {
			dir := t.TempDir()
			if err := persistPreparedAdoption(dir, identity, n.core.WALIdentity(), view, &membership, ""); err != nil {
				t.Fatal("actual adoption writer", err)
			}
			name := filepath.Join(dir, "prepared-adoption.json")
			written, err := os.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			lastWritten = bytes.Clone(written)
			t.Logf("actual C%d adoption bytes=%d membership transitions=%d old_reader_bound=16384 shared_metadata_bound=%d", remove.ConfigID, len(written), len(membership.Transitions), managedPreparedAdoptionMaxBytes)
			if cycle == 2 && len(written) <= 16384 {
				t.Fatal("actual C5 fixture did not exceed the old reader bound")
			}
			got, err := loadPreparedAdoption(name)
			if err != nil {
				t.Fatalf("writer-success/read-reject: bytes=%d old_bound=16384: %v", len(written), err)
			}
			if got.Identity != identity || got.WALIdentity != n.core.WALIdentity() || got.Tip != view.Tip() || got.PrefixHash != view.PrefixHash() || !reflect.DeepEqual(got.Membership, &membership) {
				t.Fatal("round-trip changed semantic identity or certified history")
			}
			if after, err := os.ReadFile(name); err != nil || !bytes.Equal(after, written) {
				t.Fatal("reader changed adoption bytes", err)
			}
		})
	}
	t.Run("reader-boundary-preserves-certified-record", func(t *testing.T) {
		if len(lastWritten) == 0 || len(lastWritten) > managedPreparedAdoptionMaxBytes {
			t.Fatal("actual certified fixture does not fit shared metadata budget")
		}
		// JSON trailing whitespace exercises the exact resource boundary while
		// retaining the real identity, membership transitions and certificates.
		bounded := append(bytes.Clone(lastWritten), bytes.Repeat([]byte(" "), managedPreparedAdoptionMaxBytes-len(lastWritten))...)
		name := filepath.Join(t.TempDir(), "prepared-adoption.json")
		if err := os.WriteFile(name, bounded, 0600); err != nil {
			t.Fatal(err)
		}
		got, err := loadPreparedAdoption(name)
		if err != nil || got.Identity != identity || !reflect.DeepEqual(got.Membership, &lastMembership) {
			t.Fatal("exact-bound actual record rejected or changed", err)
		}
		oversize := append(bounded, ' ')
		if err := os.WriteFile(name, oversize, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadPreparedAdoption(name); err == nil {
			t.Fatal("reader accepted metadata above shared bound")
		}
		if after, err := os.ReadFile(name); err != nil || !bytes.Equal(after, oversize) {
			t.Fatal("reader changed rejected record", err)
		}
	})
	t.Run("oversize-rejected-before-publication", func(t *testing.T) {
		// Keep the actual certificates intact; only the untrusted identity
		// envelope is oversized. This is not claimed to be valid startup identity.
		oversize := identity
		oversize.ConfigDigest = strings.Repeat("x", managedPreparedAdoptionMaxBytes+1)
		dir := t.TempDir()
		if err := persistPreparedAdoption(dir, oversize, n.core.WALIdentity(), lastView, &lastMembership, ""); err == nil {
			t.Error("oversize adoption writer succeeded")
		}
		for _, file := range []string{"prepared-adoption.json", "prepared-adoption.pending"} {
			if _, err := os.Lstat(filepath.Join(dir, file)); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("oversize writer created %s: %v", file, err)
			}
		}
	})
}
