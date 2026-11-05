package quepaxa

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

func TestEnsureDurableThroughPromotesHintsWithOneSync(t *testing.T) {
	config := &Cluster{ConfigID: 1, Members: []Member{{ID: "a"}}}
	sourceWAL, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sourceWAL.Close() })
	source := newCore("a", config, sourceWAL, nil)
	for i := 1; i <= 3; i++ {
		if _, _, err := source.Propose(context.Background(), []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	values, _, err := source.DecisionsFrom(1, 3)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	core := newCore("a", config, wal, nil)
	if err := core.AcceptCertifiedHints(values); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := core.EnsureDurableThrough(ctx, 3); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled promotion: %v", err)
	}
	core.commits = newGroupCommit(func() error { return errors.New("sync failed") })
	if err := core.EnsureDurableThrough(context.Background(), 3); err == nil {
		t.Fatal("failed sync was acknowledged")
	}
	if core.durableThrough != 0 {
		t.Fatal("failed sync advanced the verified durable prefix", core.durableThrough)
	}
	for slot := Slot(1); slot <= 3; slot++ {
		if core.durable[slot] {
			t.Fatalf("failed sync marked slot %d durable", slot)
		}
	}
	var syncs atomic.Int32
	core.commits = newGroupCommit(func() error { syncs.Add(1); return wal.Sync() })
	for i := 0; i < 2; i++ {
		if err := core.EnsureDurableThrough(context.Background(), 3); err != nil {
			t.Fatal(err)
		}
	}
	if got := syncs.Load(); got != 1 {
		t.Fatalf("promotion and repeated check used %d syncs, want 1", got)
	}
	if core.durableThrough != 3 {
		t.Fatal("successful contiguous sync did not establish the exact frontier", core.durableThrough)
	}
	if err := core.EnsureDurableThrough(context.Background(), 4); err == nil {
		t.Fatal("missing prefix was acknowledged")
	}
	if core.durableThrough != 3 {
		t.Fatal("missing decision advanced the verified durable prefix", core.durableThrough)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	recovered := newCore("a", config, reopened, nil)
	if err := recovered.recover(); err != nil {
		t.Fatal(err)
	}
	if recovered.Tip() != 3 {
		t.Fatalf("restarted prefix=%d, want 3", recovered.Tip())
	}
	for _, value := range values {
		got, ok := recovered.CertifiedValue(value.Slot)
		if !ok || got.Hash != value.Hash {
			t.Fatalf("restarted slot %d differs", value.Slot)
		}
	}
}

func TestEnsureDurableThroughVerifiedPrefixSkipsOldRecordLock(t *testing.T) {
	for _, through := range []Slot{1, 2} {
		name := "same-prefix"
		if through == 2 {
			name = "new-slot"
		}
		t.Run(name, func(t *testing.T) {
			config := &Cluster{ConfigID: 1, Members: []Member{{ID: "a"}}}
			sourceWAL, err := qlog.Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = sourceWAL.Close() })
			source := newCore("a", config, sourceWAL, nil)
			for i := 1; i <= 2; i++ {
				if _, _, err := source.Propose(context.Background(), []byte{byte(i)}); err != nil {
					t.Fatal(err)
				}
			}
			values, _, err := source.DecisionsFrom(1, 2)
			if err != nil || len(values) != 2 {
				t.Fatal("actual certified source prefix unavailable", err)
			}
			dir := t.TempDir()
			wal, err := qlog.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = wal.Close() })
			core := newCore("a", config, wal, nil)
			if err := core.AcceptCertifiedHints(values); err != nil {
				t.Fatal(err)
			}
			var syncs atomic.Int32
			core.commits = newGroupCommit(func() error { syncs.Add(1); return wal.Sync() })
			if err := core.EnsureDurableThrough(context.Background(), 1); err != nil {
				t.Fatal(err)
			}
			canceled, cancel := context.WithCancel(context.Background())
			cancel()
			if err := core.EnsureDurableThrough(canceled, 1); !errors.Is(err, context.Canceled) {
				t.Fatal("verified prefix bypassed caller cancellation", err)
			}
			// This is a lock-dependency assertion, not a wall-clock benchmark:
			// a verified old prefix must not depend on reacquiring its old lock.
			oldLock := &core.recordLocks[1%uint64(len(core.recordLocks))]
			oldLock.Lock()
			ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			done := make(chan error, 1)
			go func() { done <- core.EnsureDurableThrough(ctx, through) }()
			blocked := false
			select {
			case err = <-done:
			case <-ctx.Done():
				blocked = true
			}
			oldLock.Unlock()
			if blocked {
				<-done // Releasing the owned old lock lets the canceled call drain.
				t.Fatal("verified prefix was revisited while its old record lock was held")
			}
			if err != nil || syncs.Load() != int32(through) {
				t.Fatalf("prefix through=%d syncs=%d: %v", through, syncs.Load(), err)
			}
			if err := wal.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := qlog.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			recovered := newCore("a", config, reopened, nil)
			if err := recovered.recover(); err != nil || recovered.Tip() != through {
				t.Fatalf("actual WAL reopen through=%d want=%d: %v", recovered.Tip(), through, err)
			}
			for _, expected := range values[:int(through)] {
				got, ok := recovered.CertifiedValue(expected.Slot)
				if !ok || got.Hash != expected.Hash || string(got.Value) != string(expected.Value) || string(got.Certificate) != string(expected.Certificate) {
					t.Fatal("WAL reopen changed original certified bytes", expected.Slot)
				}
			}
		})
	}
}

func TestEnsureDurableThroughConcurrentRecoveryBase(t *testing.T) {
	source, seal, decision := testCheckpointRecoveryBase(t)
	dir := t.TempDir()
	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	core := newCore("n1", source.config, wal, nil)
	first, _ := source.CertifiedValue(1)
	if err := core.AcceptCertifiedHints([]DecidedValue{first}); err != nil {
		t.Fatal(err)
	}
	core.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	entered, release := make(chan struct{}), make(chan struct{})
	core.commits = newGroupCommit(func() error {
		close(entered)
		<-release
		return wal.Sync()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- core.EnsureDurableThrough(ctx, 1) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("prefix did not reach sync")
	}
	err = core.RestoreCheckpointBase(ctx, seal, decision)
	close(release)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if core.durable[1] {
		t.Fatal("sync resurrected a compacted durability entry")
	}
	if core.durableThrough != seal.Index {
		t.Fatal("old sync changed the recovery-base frontier", core.durableThrough, seal.Index)
	}
	if err := core.EnsureDurableThrough(ctx, seal.Index); err != nil {
		t.Fatalf("covered prefix: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	recovered := newCore("n1", source.config, reopened, nil)
	if err := recovered.recover(); err != nil {
		t.Fatal(err)
	}
	if recovered.Tip() != seal.Index || recovered.CompactionFloor() != seal.Index {
		t.Fatal("checkpoint coverage lost across restart")
	}
}

func TestEnsureDurableThroughWakesRegisteredDecisionBatch(t *testing.T) {
	core, wal, values := durableFrontierCertifiedFixture(t)
	if err := core.AcceptCertifiedHints(values); err != nil {
		t.Fatal(err)
	}
	entered, release, synced := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	core.commits = newGroupCommit(func() error {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		err := wal.Sync()
		if calls.Load() == 1 {
			close(synced)
		}
		return err
	})
	// A decision must explicitly wake the registered batch, not depend on
	// the recorder's timer or enough unrelated recorder waiters arriving.
	core.commits.delay = time.Hour
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	done := make(chan error, 1)
	go func() { done <- core.EnsureDurableThrough(ctx, 3) }()
	joined, released := false, false
	t.Cleanup(func() {
		cancel()
		if !released {
			close(release)
		}
		select {
		case core.commits.wake <- struct{}{}:
		default:
		}
		if !joined {
			<-done
		}
		core.commits.mu.Lock()
		running := core.commits.running
		core.commits.mu.Unlock()
		if running {
			<-synced
		}
	})
	select {
	case <-entered:
	case err := <-done:
		joined = true
		t.Fatalf("decision did not wake its registered sync batch: %v", err)
	case <-ctx.Done():
		t.Fatal("decision waited for the recorder timer instead of waking its registered batch")
	}
	core.mu.RLock()
	premature := core.durableThrough != 0 || core.durable[1] || core.durable[2] || core.durable[3]
	core.mu.RUnlock()
	if premature {
		t.Fatal("decision became durable before the actual WAL Sync completed")
	}
	close(release)
	released = true
	err := <-done
	joined = true
	if err != nil {
		t.Fatal(err)
	}
	if err := core.EnsureDurableThrough(ctx, 3); err != nil || calls.Load() != 1 {
		t.Fatalf("covered prefix synced again: calls=%d err=%v", calls.Load(), err)
	}
	for _, expected := range values {
		got, ok := core.CertifiedValue(expected.Slot)
		if !ok || got.Hash != expected.Hash || string(got.Value) != string(expected.Value) || string(got.Certificate) != string(expected.Certificate) {
			t.Fatal("decision sync changed the certified original", expected.Slot)
		}
	}
}

func durableFrontierCertifiedFixture(t *testing.T) (*Core, *qlog.WAL, []DecidedValue) {
	t.Helper()
	config := &Cluster{ConfigID: 1, Members: []Member{{ID: "a"}}}
	sourceWAL, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sourceWAL.Close() })
	source := newCore("a", config, sourceWAL, nil)
	for i := 1; i <= 3; i++ {
		if _, _, err := source.Propose(context.Background(), []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	values, _, err := source.DecisionsFrom(1, 3)
	if err != nil || len(values) != 3 {
		t.Fatal("actual certified source prefix unavailable", err)
	}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	return newCore("a", config, wal, nil), wal, values
}

func TestEnsureDurableThroughFrontierDoesNotSkipMiddleGap(t *testing.T) {
	core, _, values := durableFrontierCertifiedFixture(t)
	// Slot 3 is genuinely synced before slot 2 is even learned. Its durable
	// certificate must not stand in for the missing contiguous middle slot.
	if err := core.AcceptCertifiedValues([]DecidedValue{values[0], values[2]}); err != nil {
		t.Fatal(err)
	}
	if err := core.EnsureDurableThrough(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if err := core.EnsureDurableThrough(context.Background(), 3); err == nil {
		t.Fatal("out-of-order durable decision bypassed missing slot 2")
	}
	if core.durableThrough != 1 || !core.durable[3] {
		t.Fatal("frontier crossed a real middle gap", core.durableThrough, core.durable[3])
	}
	if err := core.AcceptCertifiedHints(values[1:2]); err != nil {
		t.Fatal(err)
	}
	if err := core.EnsureDurableThrough(context.Background(), 3); err != nil {
		t.Fatal(err)
	}
	if core.durableThrough != 3 || !core.durable[2] {
		t.Fatal("synced gap did not establish contiguous frontier", core.durableThrough)
	}
}

func TestEnsureDurableThroughCanceledInFlightSyncDoesNotAdvanceFrontier(t *testing.T) {
	core, wal, values := durableFrontierCertifiedFixture(t)
	if err := core.AcceptCertifiedHints(values); err != nil {
		t.Fatal(err)
	}
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	core.commits = newGroupCommit(func() error {
		close(entered)
		<-release
		err := wal.Sync()
		close(finished)
		return err
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- core.EnsureDurableThrough(ctx, 3) }()
	released := false
	// Register cleanup before any assertion: release the owned sync worker
	// before the fixture's WAL cleanup, even if an assertion fails.
	t.Cleanup(func() {
		cancel()
		if !released {
			close(release)
		}
		<-finished
	})
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal("in-flight sync did not return caller cancellation", err)
	}
	if core.durableThrough != 0 {
		t.Fatal("uncertain sync advanced the frontier", core.durableThrough)
	}
	for slot := Slot(1); slot <= 3; slot++ {
		if !core.logged[slot] || core.durable[slot] {
			t.Fatal("pending append was treated as synced", slot, core.logged[slot], core.durable[slot])
		}
	}
	close(release)
	released = true
	<-finished
	if core.durableThrough != 0 {
		t.Fatal("late completion retroactively advanced canceled call's frontier", core.durableThrough)
	}
}
