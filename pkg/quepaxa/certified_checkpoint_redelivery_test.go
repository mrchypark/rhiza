package quepaxa

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

// Core-only durability test. Certificates come from the existing actual
// three-Core transport, not mockTransport or directly inserted decision maps.
// The checkpoint callback validates the two exact test identities; physical
// native paired-root proof is exercised by the separate Node regression.
func TestCertifiedCheckpointRedeliveryPreservesFailedSync(t *testing.T) {
	testCertifiedCheckpointRedeliveryDurability(t, false)
}

func TestCertifiedCheckpointRedeliveryPreservesCanceledSync(t *testing.T) {
	testCertifiedCheckpointRedeliveryDurability(t, true)
}

func testCertifiedCheckpointRedeliveryDurability(t *testing.T, cancelSync bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cores, transport := newTestCluster(t)
	source, target := cores["n1"], cores["n3"]
	for i := 0; i < 5; i++ {
		if _, _, err := source.Propose(ctx, []byte{byte(i + 1)}); err != nil {
			t.Fatal(err)
		}
	}
	sealAt := func(cut Slot, label string) CheckpointSeal {
		prefix, ok := source.PrefixHash(cut)
		if !ok {
			t.Fatal("actual prefix absent")
		}
		next, following, err := source.CheckpointLeaderOrders(cut)
		if err != nil {
			t.Fatal(err)
		}
		return CheckpointSeal{ConfigID: 1, Index: cut, RootHash: sha256.Sum256([]byte(label)), StateHash: sha256.Sum256([]byte("state-" + label)), PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following}
	}
	old, higher := sealAt(1, "old"), sealAt(4, "higher")
	for _, core := range cores {
		core.SetCheckpointValidator(func(_ context.Context, s CheckpointSeal) error {
			if (s.Index == old.Index && s.RootHash == old.RootHash && s.StateHash == old.StateHash) || (s.Index == higher.Index && s.RootHash == higher.RootHash && s.StateHash == higher.StateHash) {
				return nil
			}
			return errors.New("unknown test root")
		})
		if err := core.PrepareCheckpoint(ctx, old); err != nil {
			t.Fatal(err)
		}
	}
	transport.mu.Lock()
	transport.down["n3"] = true
	transport.dropDecision["n3"] = true
	transport.mu.Unlock()
	value, err := EncodeCheckpointSeal(old)
	if err != nil {
		t.Fatal(err)
	}
	slot, _, err := source.Propose(ctx, value)
	transport.mu.Lock()
	transport.down["n3"] = false
	transport.mu.Unlock()
	if err != nil || slot != 6 {
		t.Fatal("actual retained seal certification", slot, err)
	}
	target.mu.RLock()
	recorder := target.recorders[slot]
	target.mu.RUnlock()
	if recorder.Step != 0 || recorder.FirstCurrent != nil {
		t.Fatalf("n3 received the old checkpoint Record before its higher prepare: %+v", recorder)
	}
	stored, ok := source.CertifiedValue(slot)
	if !ok {
		t.Fatal("actual certificate missing")
	}
	decision, err := decodeDecision(stored.Certificate)
	if err != nil {
		t.Fatal(err)
	}
	decision.Proposal.Value = bytes.Clone(stored.Value)
	// Prepare only through4 before the delayed certified suffix6 is learned.
	// Unlike the cut4/slot4 RED, this cannot Sync the target's decision6.
	if err := target.PrepareCheckpoint(ctx, higher); err != nil {
		t.Fatal(err)
	}
	if err := target.AcceptDecisionHint(decision); err != nil {
		t.Fatal(err)
	}
	target.mu.RLock()
	wasDurable, wasLogged := target.durable[slot], target.logged[slot]
	target.mu.RUnlock()
	if wasDurable || wasLogged {
		t.Fatal("late actual certified hint unexpectedly synced")
	}
	sentinel := errors.New("owned test Sync fault")
	var fail atomic.Bool
	fail.Store(true)
	var syncCalls atomic.Int64
	entered, release, callbackDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	target.commits = newGroupCommit(func() error {
		call := syncCalls.Add(1)
		if cancelSync && call == 1 {
			close(entered)
			<-release
			err := target.wal.Sync()
			close(callbackDone)
			return err
		}
		if fail.Load() {
			return sentinel
		}
		return target.wal.Sync()
	})
	request := RecordRequest{Slot: slot, Step: decision.Step + 4, ConfigID: 1, Proposal: decision.Proposal}
	if cancelSync {
		caller, cancelCaller := context.WithCancel(ctx)
		defer cancelCaller()
		type outcome struct {
			summary Summary
			err     error
		}
		result := make(chan outcome, 1)
		returned := make(chan struct{})
		go func() {
			summary, err := target.Record(caller, request)
			result <- outcome{summary, err}
			close(returned)
		}()
		t.Cleanup(func() {
			cancelCaller()
			releaseOnce.Do(func() { close(release) })
			select {
			case <-returned:
			case <-time.After(time.Second):
				t.Error("owned Record goroutine did not return")
			}
			select {
			case <-callbackDone:
			case <-time.After(time.Second):
				t.Error("owned Sync callback did not return")
			}
		})
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("required Sync never blocked", ctx.Err())
		}
		cancelCaller()
		select {
		case got := <-result:
			if !errors.Is(got.err, context.Canceled) || got.summary.FirstCurrent != nil {
				t.Fatal("canceled required Sync returned ACK or lost context", got.err)
			}
		case <-ctx.Done():
			t.Fatal("canceled caller did not return", ctx.Err())
		}
	} else if _, err := target.Record(ctx, request); !errors.Is(err, sentinel) {
		t.Fatal("failed required Sync returned success or lost cause", err)
	}
	target.mu.RLock()
	durable, logged := target.durable[slot], target.logged[slot]
	target.mu.RUnlock()
	if durable || !logged || syncCalls.Load() != 1 {
		t.Fatal("failed Sync marked ACK durable or skipped original append", durable, logged, syncCalls.Load())
	}
	if cancelSync {
		releaseOnce.Do(func() { close(release) })
		select {
		case <-callbackDone:
		case <-ctx.Done():
			t.Fatal("owned Sync callback did not drain", ctx.Err())
		}
	}
	fail.Store(false)
	summary, err := target.Record(ctx, request)
	if err != nil {
		t.Fatal("normal retry of exact original", err)
	}
	if summary.FirstCurrent == nil || !sameProposal(summary.FirstCurrent, &decision.Proposal) || !bytes.Equal(summary.FirstCurrent.Value, value) || summary.Step != request.Step {
		t.Fatal("retry summary changed original certified proposal")
	}
	target.mu.RLock()
	durable = target.durable[slot]
	target.mu.RUnlock()
	if !durable || syncCalls.Load() != 2 {
		t.Fatal("normal retry did not complete required durability")
	}
	after, ok := target.CertifiedValue(slot)
	if !ok || after.Hash != stored.Hash || !bytes.Equal(after.Value, stored.Value) || !bytes.Equal(after.Certificate, stored.Certificate) {
		t.Fatal("durability changed original decision")
	}
	entries, err := target.wal.Read()
	if err != nil {
		t.Fatal(err)
	}
	decides := 0
	for _, entry := range entries {
		if entry.Slot == uint64(slot) && entry.Type == qlog.EntryDecide {
			decides++
		}
	}
	if decides != 1 {
		t.Fatalf("existing decision append count=%d want1", decides)
	}
	if fence, root, ok := target.LatestPreparedCheckpoint(); !ok || fence != higher.Index || root != higher.RootHash {
		t.Fatal("higher fence changed")
	}
	// A canceled waiter does not own the shared commit worker. Verify its
	// released flush also reaches idle before the fixture's WAL is closed.
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		target.commits.mu.Lock()
		running := target.commits.running
		target.commits.mu.Unlock()
		if !running {
			break
		}
		select {
		case <-tick.C:
		case <-ctx.Done():
			t.Fatal("owned commit worker did not reach idle", ctx.Err())
		}
	}
}
