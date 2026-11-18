package quepaxa

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestIssue328FreezeQuorumVoterLossCharacterization documents the current
// liveness limitation reported in issue 328. This is characterization of a
// known defect, not the intended target behavior for membership transitions.
func TestIssue328FreezeQuorumVoterLossCharacterization(t *testing.T) {
	t.Run("finish removal", func(t *testing.T) {
		cores, transport := reconfigCluster(t)
		target := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}
		freeze := freezeWithAB(t, boundedContext(t, 10*time.Second), cores["a"], transport, target)
		terminal := freeze + 16

		transport.fail("b")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := cores["a"].FinishReconfiguration(ctx); !errors.Is(err, ErrQuorumUnavailable) {
			t.Fatalf("finish with freeze voter b unavailable: %v", err)
		}
		assertFrozenTerminalBlocked(t, cores, terminal)

		transport.recover("b")
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := cores["a"].FinishReconfiguration(ctx); err != nil {
			t.Fatalf("finish after b recovery: %v", err)
		}
		if got := cores["a"].CurrentCluster(); !sameCluster(got, target) {
			t.Fatalf("cluster after recovery=%+v want=%+v", got, target)
		}
		if _, pending := cores["a"].PendingReconfiguration(); pending {
			t.Fatal("reconfiguration remained pending after b recovery")
		}
		assertReadWrite(t, boundedContext(t, 10*time.Second), cores["a"], "after-finish-recovery")
	})

	t.Run("abort addition", func(t *testing.T) {
		cores, transport := reconfigCluster(t)
		initial := cores["a"].CurrentCluster()
		target := Cluster{ConfigID: 2, Members: []Member{
			{ID: "a"}, {ID: "b"}, {ID: "c"},
			{ID: "d", WALIdentity: "0000000000000000000000000000000000000000000000000000000000000000"},
		}}
		freeze := freezeWithAB(t, boundedContext(t, 10*time.Second), cores["a"], transport, target)
		terminal := freeze + 16

		transport.fail("b")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := cores["a"].AbortReconfiguration(ctx); !errors.Is(err, ErrQuorumUnavailable) {
			t.Fatalf("abort with freeze voter b unavailable: %v", err)
		}
		assertFrozenTerminalBlocked(t, cores, terminal)

		transport.recover("b")
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := cores["a"].AbortReconfiguration(ctx); err != nil {
			t.Fatalf("abort after b recovery: %v", err)
		}
		if got := cores["a"].CurrentCluster(); !sameCluster(got, initial) {
			t.Fatalf("abort changed cluster: got=%+v want=%+v", got, initial)
		}
		if _, pending := cores["a"].PendingReconfiguration(); pending {
			t.Fatal("abort remained pending after b recovery")
		}
		assertReadWrite(t, boundedContext(t, 10*time.Second), cores["a"], "after-abort-recovery")
	})
}

func boundedContext(t *testing.T, timeout time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	return ctx
}

func freezeWithAB(t *testing.T, ctx context.Context, core *Core, transport *clusterTransport, target Cluster) Slot {
	t.Helper()
	// Excluding c from the freeze response makes the certified safe set exactly {a,b}.
	transport.fail("c")
	freeze, err := core.BeginReconfiguration(ctx, target)
	if err != nil {
		t.Fatalf("freeze: %v", err)
	}
	transport.recover("c")

	decision, err := core.certifiedDecision(mustCertified(t, core, freeze))
	if err != nil {
		t.Fatalf("decode freeze certificate: %v", err)
	}
	sources := summarySources(decision.Summaries)
	if len(sources) != 2 || sources[0] != "a" || sources[1] != "b" {
		t.Fatalf("freeze recorders=%v want [a b]", sources)
	}
	return freeze
}

func assertFrozenTerminalBlocked(t *testing.T, cores map[NodeID]*Core, terminal Slot) {
	t.Helper()
	core := cores["a"]
	aTarget, aPending := core.PendingReconfiguration()
	cTarget, cPending := cores["c"].PendingReconfiguration()
	if !aPending || !cPending || !sameCluster(aTarget, cTarget) {
		t.Fatalf("surviving voters pending: a=%t c=%t targets=%+v/%+v", aPending, cPending, aTarget, cTarget)
	}
	for _, id := range []NodeID{"a", "c"} {
		if got := cores[id].Tip(); got != terminal-1 {
			t.Fatalf("%s tip=%d want drained through %d", id, got, terminal-1)
		}
	}

	core.mu.RLock()
	_, attempted := core.recorders[terminal]
	_, decided := core.decided[terminal]
	core.mu.RUnlock()
	if !attempted || decided {
		t.Fatalf("a terminal attempted=%t decided=%t, want attempted and undecided", attempted, decided)
	}

	cores["c"].mu.RLock()
	_, attempted = cores["c"].recorders[terminal]
	_, decided = cores["c"].decided[terminal]
	cores["c"].mu.RUnlock()
	if attempted || decided {
		t.Fatalf("live c received terminal: attempted=%t decided=%t", attempted, decided)
	}

	for _, id := range []NodeID{"a", "c"} {
		if _, _, err := cores[id].Propose(boundedContext(t, 10*time.Second), []byte("blocked")); !errors.Is(err, ErrReconfigurationDrain) {
			t.Fatalf("%s write while frozen: %v", id, err)
		}
		if _, _, err := cores[id].ReadIndex(boundedContext(t, 200*time.Millisecond)); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("%s read while frozen: %v, want deadline", id, err)
		}
	}
}

func assertReadWrite(t *testing.T, ctx context.Context, core *Core, value string) {
	t.Helper()
	if _, _, err := core.Propose(ctx, []byte(value)); err != nil {
		t.Fatalf("write after recovery: %v", err)
	}
	if _, id, err := core.ReadIndex(ctx); err != nil || id == "" {
		t.Fatalf("read after recovery: id=%q err=%v", id, err)
	}
}
