package quepaxa

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestIssue328FreezeQuorumVoterLossCharacterization verifies that Freeze -> Terminal
// transitions survive the loss of a freeze-quorum voter whenever another clean
// old-config voter remains available, while still waiting for target quorum ACK
// on terminal installation.
func TestIssue328FreezeQuorumVoterLossCharacterization(t *testing.T) {
	t.Run("abort addition survives freeze voter loss", func(t *testing.T) {
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
		if err := cores["a"].AbortReconfiguration(ctx); err != nil {
			t.Fatalf("abort with freeze voter b unavailable: %v", err)
		}
		if got := cores["a"].CurrentCluster(); !sameCluster(got, initial) {
			t.Fatalf("abort changed cluster: got=%+v want=%+v", got, initial)
		}
		if _, pending := cores["a"].PendingReconfiguration(); pending {
			t.Fatal("abort remained pending after freeze voter loss")
		}
		if got := cores["a"].Tip(); got != terminal {
			t.Fatalf("tip=%d want %d", got, terminal)
		}
		history, err := cores["a"].MembershipHistory()
		if err != nil {
			t.Fatalf("membership history: %v", err)
		}
		if _, err := validateMembershipHistory(initial, history); err != nil {
			t.Fatalf("validate divergent freeze/abort history: %v", err)
		}
		assertReadWrite(t, boundedContext(t, 10*time.Second), cores["a"], "after-abort-freeze-voter-loss")
	})

	t.Run("finish removal of failed freeze voter", func(t *testing.T) {
		cores, transport := reconfigCluster(t)
		initial := cores["a"].CurrentCluster()
		target := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "c"}}}
		freezeWithAB(t, boundedContext(t, 10*time.Second), cores["a"], transport, target)

		transport.fail("b")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := cores["a"].FinishReconfiguration(ctx); err != nil {
			t.Fatalf("finish removal of failed freeze voter b: %v", err)
		}
		if got := cores["a"].CurrentCluster(); !sameCluster(got, target) {
			t.Fatalf("cluster after finish=%+v want=%+v", got, target)
		}
		if _, pending := cores["a"].PendingReconfiguration(); pending {
			t.Fatal("reconfiguration remained pending after removal of failed freeze voter")
		}
		history, err := cores["a"].MembershipHistory()
		if err != nil {
			t.Fatalf("membership history: %v", err)
		}
		if _, err := validateMembershipHistory(initial, history); err != nil {
			t.Fatalf("validate divergent freeze/terminal history: %v", err)
		}
		assertReadWrite(t, boundedContext(t, 10*time.Second), cores["a"], "after-finish-divergent-quorum")
	})

	t.Run("finish removal waiting on target voter recovery", func(t *testing.T) {
		cores, transport := reconfigCluster(t)
		initial := cores["a"].CurrentCluster()
		target := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}
		freeze := freezeWithAB(t, boundedContext(t, 10*time.Second), cores["a"], transport, target)
		terminal := freeze + 16

		transport.fail("b")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := cores["a"].FinishReconfiguration(ctx); !errors.Is(err, ErrQuorumUnavailable) {
			t.Fatalf("finish without target voter b: %v, want ErrQuorumUnavailable", err)
		}
		// Terminal was certified under old quorum {a, c}, advancing a to target {a, b},
		// while target {a, b} still lacks b's ACK until b recovers.
		if got := cores["a"].Tip(); got != terminal {
			t.Fatalf("tip=%d want certified terminal %d", got, terminal)
		}
		if got := cores["a"].CurrentCluster(); !sameCluster(got, target) {
			t.Fatalf("cluster=%+v want=%+v", got, target)
		}
		if _, _, err := cores["a"].Propose(boundedContext(t, 2*time.Second), []byte("blocked-without-b")); !errors.Is(err, ErrQuorumUnavailable) {
			t.Fatalf("write without target voter b: %v, want ErrQuorumUnavailable", err)
		}

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
		history, err := cores["a"].MembershipHistory()
		if err != nil {
			t.Fatalf("membership history: %v", err)
		}
		if _, err := validateMembershipHistory(initial, history); err != nil {
			t.Fatalf("validate recovered history: %v", err)
		}
		assertReadWrite(t, boundedContext(t, 10*time.Second), cores["a"], "after-finish-recovery")
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

func assertReadWrite(t *testing.T, ctx context.Context, core *Core, value string) {
	t.Helper()
	if _, _, err := core.Propose(ctx, []byte(value)); err != nil {
		t.Fatalf("write after recovery: %v", err)
	}
	if _, id, err := core.ReadIndex(ctx); err != nil || id == "" {
		t.Fatalf("read after recovery: id=%q err=%v", id, err)
	}
}
