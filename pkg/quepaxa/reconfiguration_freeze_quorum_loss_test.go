package quepaxa

import (
	"context"
	"testing"
	"time"
)

func TestFreezeQuorumVoterLossPreservesTerminalLiveness(t *testing.T) {
	t.Run("finish removal", func(t *testing.T) {
		cores, transport := reconfigCluster(t)
		target := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}
		freezeWithAB(t, boundedContext(t, 10*time.Second), cores["a"], transport, target)

		transport.failRecord("b", true)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := cores["a"].FinishReconfiguration(ctx); err != nil {
			t.Fatalf("finish with one freeze voter unavailable: %v", err)
		}
		transport.failRecord("b", false)
		if got := cores["a"].CurrentCluster(); !sameCluster(got, target) {
			t.Fatalf("cluster after finish=%+v want=%+v", got, target)
		}
		if _, pending := cores["a"].PendingReconfiguration(); pending {
			t.Fatal("reconfiguration remained pending")
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
		freezeWithAB(t, boundedContext(t, 10*time.Second), cores["a"], transport, target)

		transport.fail("b")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := cores["a"].AbortReconfiguration(ctx); err != nil {
			t.Fatalf("abort with one freeze voter unavailable: %v", err)
		}
		transport.recover("b")
		if got := cores["a"].CurrentCluster(); !sameCluster(got, initial) {
			t.Fatalf("abort changed cluster: got=%+v want=%+v", got, initial)
		}
		if _, pending := cores["a"].PendingReconfiguration(); pending {
			t.Fatal("abort remained pending")
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

func assertReadWrite(t *testing.T, ctx context.Context, core *Core, value string) {
	t.Helper()
	if _, _, err := core.Propose(ctx, []byte(value)); err != nil {
		t.Fatalf("write after recovery: %v", err)
	}
	if _, id, err := core.ReadIndex(ctx); err != nil || id == "" {
		t.Fatalf("read after recovery: id=%q err=%v", id, err)
	}
}
