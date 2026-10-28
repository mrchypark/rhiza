package quepaxa

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

type enrollmentReadTransport struct {
	Transport
	once sync.Once
	read func()
}

func (t *enrollmentReadTransport) ReadTip(ctx context.Context, id NodeID) (Slot, error) {
	t.once.Do(t.read)
	return t.Transport.ReadTip(ctx, id)
}

func TestEnrollmentReadRoundCatchUp(t *testing.T) {
	cores, transport := newTestCluster(t)
	transport.mu.Lock()
	transport.dropDecision["n2"] = true
	transport.mu.Unlock()
	if _, _, err := cores["n1"].Propose(context.Background(), []byte("write")); err != nil {
		t.Fatal(err)
	}
	transport.fail("n1")
	core := cores["n2"]
	for _, callback := range []EnrollmentCatchUp{nil, func(context.Context, NodeID, Slot) error { return errors.New("failed") }, func(context.Context, NodeID, Slot) error { return nil }} {
		if _, err := core.EnrollmentReadRound(context.Background(), callback); err == nil {
			t.Fatal("accepted incomplete catch-up")
		}
	}
	round, err := core.EnrollmentReadRound(context.Background(), func(_ context.Context, source NodeID, index Slot) error {
		if source != "n3" || index != 1 {
			t.Fatalf("catch-up source=%s index=%d", source, index)
		}
		value, ok := cores[source].CertifiedValue(index)
		if !ok {
			t.Fatal("missing source value")
		}
		return core.AcceptCertifiedValue(value)
	})
	if err != nil {
		t.Fatal(err)
	}
	if round.Source != "n2" || round.Index != 1 {
		t.Fatalf("round=%+v", round)
	}
	encoded, _ := json.Marshal(round.History)
	if round.HistoryHash != sha256.Sum256(encoded) {
		t.Fatal("incorrect history digest")
	}
	round.Cluster.Members[0].ID = "changed"
	round.History.Genesis.Members[0].ID = "changed"
	if core.CurrentCluster().Members[0].ID == "changed" {
		t.Fatal("cluster aliases core")
	}
	history, err := core.MembershipHistory()
	if err != nil || history.Genesis.Members[0].ID == "changed" {
		t.Fatal("history aliases core", err)
	}
}

func TestEnrollmentReadRoundRejectsUnstable(t *testing.T) {
	for name, mutate := range map[string]func(*Core){
		"freeze":   func(c *Core) { c.reconfiguration = &reconfigurationState{} },
		"abort":    func(c *Core) { c.lastAbort = &ConfigTransition{Terminal: DecidedValue{Slot: 1}} },
		"observer": func(c *Core) { c.observer = true },
		"config":   func(c *Core) { c.configHistory[0].cluster.ConfigID++ },
	} {
		for _, phase := range []string{"read", "catchup"} {
			t.Run(name+"/"+phase, func(t *testing.T) {
				cores, transport := newTestCluster(t)
				transport.mu.Lock()
				transport.dropDecision["n2"] = true
				transport.mu.Unlock()
				if _, _, err := cores["n1"].Propose(context.Background(), []byte("write")); err != nil {
					t.Fatal(err)
				}
				transport.fail("n1")
				core := cores["n2"]
				change := func() { core.mu.Lock(); defer core.mu.Unlock(); mutate(core) }
				if phase == "read" {
					core.transport = &enrollmentReadTransport{Transport: transport, read: change}
				}
				_, err := core.EnrollmentReadRound(context.Background(), func(_ context.Context, source NodeID, index Slot) error {
					value, ok := cores[source].CertifiedValue(index)
					if !ok {
						return errors.New("missing source value")
					}
					if err := core.AcceptCertifiedValue(value); err != nil {
						return err
					}
					if phase == "catchup" {
						change()
					}
					return nil
				})
				if err == nil {
					t.Fatal("accepted unstable membership")
				}
			})
		}
	}
}

func TestValidateEnrollmentHistoryReturnsFinalCluster(t *testing.T) {
	cores, transport := reconfigCluster(t)
	core := cores["a"]
	initial := core.CurrentCluster()
	for _, c := range cores {
		c.reconfigAdmission = func(context.Context, Cluster, Slot, [32]byte) error { return nil }
	}
	add := cloneCluster(initial)
	for _, id := range []NodeID{"d", "e", "f"} {
		wal, err := qlog.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		learner, err := NewLearner(Config{NodeID: id, Cluster: initial, WAL: wal, Transport: transport, EnableReconfiguration: true})
		if err != nil {
			t.Fatal(err)
		}
		if core.Tip() > 0 {
			values, _, err := core.DecisionsFrom(1, 128)
			if err != nil {
				t.Fatal(err)
			}
			if err := learner.AcceptCertifiedHints(values); err != nil {
				t.Fatal(err)
			}
		}
		transport.cores[id] = learner
		learner.reconfigAdmission = func(context.Context, Cluster, Slot, [32]byte) error { return nil }
		add.ConfigID++
		add.Members = append(add.Members, Member{ID: id, WALIdentity: learner.WALIdentity()})
		if _, err := core.BeginReconfiguration(context.Background(), add); err != nil {
			t.Fatal(err)
		}
		if err := core.FinishReconfiguration(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	target := cloneCluster(add)
	for _, id := range []NodeID{"c", "b", "a"} {
		target.ConfigID++
		members := make([]Member, 0, len(target.Members)-1)
		for _, member := range target.Members {
			if member.ID != id {
				members = append(members, member)
			}
		}
		target.Members = members
		if _, err := core.BeginReconfiguration(context.Background(), target); err != nil {
			t.Fatal(err)
		}
		if err := core.FinishReconfiguration(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	history, err := core.MembershipHistory()
	if err != nil {
		t.Fatal(err)
	}
	got, err := ValidateEnrollmentHistory(initial, history)
	if err != nil || !sameCluster(got, target) {
		t.Fatalf("cluster=%+v err=%v", got, err)
	}
}

func TestEnrollmentInitialIneligibleDoesNotRead(t *testing.T) {
	for _, kind := range []string{"freeze", "retired", "observer"} {
		t.Run(kind, func(t *testing.T) {
			cores, transport := reconfigCluster(t)
			core := cores["a"]
			if kind == "freeze" {
				if _, err := core.BeginReconfiguration(context.Background(), Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}); err != nil {
					t.Fatal(err)
				}
			} else if kind == "retired" {
				if _, err := core.BeginReconfiguration(context.Background(), Cluster{ConfigID: 2, Members: []Member{{ID: "b"}, {ID: "c"}}}); err != nil {
					t.Fatal(err)
				}
				if err := core.FinishReconfiguration(context.Background()); err != nil {
					t.Fatal(err)
				}
			} else {
				core.observer = true
			}
			reads := 0
			core.transport = &enrollmentReadTransport{Transport: transport, read: func() { reads++ }}
			if _, err := core.EnrollmentReadRound(context.Background(), nil); err == nil {
				t.Fatal("accepted ineligible source")
			}
			if reads != 0 {
				t.Fatal("contacted peer before rejecting")
			}
		})
	}
}

func TestEnrollmentHistoryCopiesTransitionsAndAbort(t *testing.T) {
	cores, _ := reconfigCluster(t)
	core := cores["a"]
	ctx := context.Background()
	first := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}
	if _, err := core.BeginReconfiguration(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := core.FinishReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	aborted := Cluster{ConfigID: 3, Members: []Member{{ID: "a"}, {ID: "b"}, {ID: "d", WALIdentity: strings.Repeat("d", 64)}}}
	if _, err := core.BeginReconfiguration(ctx, aborted); err != nil {
		t.Fatal(err)
	}
	if err := core.AbortReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	round, err := core.EnrollmentReadRound(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(round.History.Transitions) != 1 || round.History.Abort == nil {
		t.Fatal("missing real transition or abort")
	}
	before, err := core.MembershipHistory()
	if err != nil {
		t.Fatal(err)
	}
	for _, transition := range []*ConfigTransition{&round.History.Transitions[0], round.History.Abort} {
		for _, value := range []*DecidedValue{&transition.Freeze, &transition.Terminal} {
			if len(value.Value) == 0 || len(value.Certificate) == 0 {
				t.Fatal("missing certificate bytes")
			}
			value.Value[0] ^= 1
			value.Certificate[0] ^= 1
		}
	}
	after, err := core.MembershipHistory()
	if err != nil || !sameMembershipRecord(before, after) {
		t.Fatal("history aliases core", err)
	}
}
