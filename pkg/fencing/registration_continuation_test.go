package fencing

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestUnboundRegistrationRepeatedTransferKeepsRevocations(t *testing.T) {
	id := testIdentity()
	id.PodName, id.WALIdentity = id.NodeID, ""
	l := Ledger{SourceClusterID: id.SourceClusterID, RequireRuntimeBinding: true}
	runtime := enrolledRuntime(id, "initial")
	runtime.PodName = id.PodName
	if err := l.RegisterEnrolled(id, runtime); err != nil {
		t.Fatal(err)
	}
	wal := strings.Repeat("a", 64)
	previous := []Identity{id}
	for i := 1; i <= 3; i++ {
		next := id
		next.RunID = fmt.Sprintf("continuation-%d", i)
		runtime := enrolledRuntime(next, next.RunID)
		runtime.PodName = next.PodName
		if err := l.ContinueUnboundRegistration(next, runtime, wal); err != nil {
			t.Fatal(err)
		}
		for _, old := range previous {
			old.WALIdentity = wal
			if err := l.Bind(old); err == nil {
				t.Fatal("old Run regained Bind after another restart")
			}
			old.WALIdentity = ""
			if err := l.Register(old); err == nil {
				t.Fatal("old Run replay reopened reservation")
			}
		}
		before, _ := json.Marshal(l)
		changedRuntime := runtime
		changedRuntime.ContainerID += "-changed"
		if err := l.ContinueUnboundRegistration(next, changedRuntime, wal); err == nil {
			t.Fatal("same Run changed runtime")
		}
		after, _ := json.Marshal(l)
		if string(before) != string(after) {
			t.Fatal("rejected replay changed history")
		}
		previous = append(previous, next)
	}
	last := previous[len(previous)-1]
	last.WALIdentity = wal
	if err := l.Bind(last); err != nil {
		t.Fatal(err)
	}
	if len(l.Runs) != len(previous) {
		t.Fatal("transfer discarded prior authority evidence")
	}
}

func TestUnboundRegistrationTransferPinsWALAndRevokesOnlyPriorRun(t *testing.T) {
	id := testIdentity()
	id.PodName = id.NodeID
	id.WALIdentity = ""
	oldRuntime := enrolledRuntime(id, "old")
	oldRuntime.PodName = id.PodName
	wal := strings.Repeat("a", 64)
	for _, bindFirst := range []bool{false, true} {
		l := Ledger{SourceClusterID: id.SourceClusterID, RequireRuntimeBinding: true}
		if err := l.RegisterEnrolled(id, oldRuntime); err != nil {
			t.Fatal(err)
		}
		bound := id
		bound.WALIdentity = wal
		if bindFirst {
			if err := l.Bind(bound); err != nil {
				t.Fatal(err)
			}
		}
		next := id
		next.RunID += "-next"
		nextRuntime := enrolledRuntime(next, "new")
		nextRuntime.PodName = next.PodName
		before, _ := json.Marshal(l)
		err := l.ContinueUnboundRegistration(next, nextRuntime, wal)
		if bindFirst {
			if err == nil {
				t.Fatal("bound Run transferred as unbound")
			}
			after, _ := json.Marshal(l)
			if string(before) != string(after) {
				t.Fatal("rejected transfer mutated ledger")
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := l.Bind(bound); err == nil {
			t.Fatal("late prior Bind succeeded")
		}
		if err := l.RegisterEnrolled(id, oldRuntime); err == nil {
			t.Fatal("prior replay reopened reservation")
		}
		if err := l.BindMutationProducer(id, oldRuntime, "producer"); err == nil {
			t.Fatal("prior producer enrollment succeeded")
		}
		if err := l.ContinueUnboundRegistration(next, nextRuntime, wal); err != nil {
			t.Fatal(err)
		}
		if err := l.ContinueUnboundRegistration(next, nextRuntime, strings.Repeat("b", 64)); err == nil {
			t.Fatal("changed claim replay accepted")
		}
		other := id
		other.RunID += "-other"
		otherRuntime := enrolledRuntime(other, "other")
		otherRuntime.PodName = other.PodName
		if err := l.RegisterEnrolled(other, otherRuntime); err != nil {
			t.Fatal(err)
		}
		other.WALIdentity = strings.Repeat("b", 64)
		if err := l.Bind(other); err == nil {
			t.Fatal("different Run escaped reserved WAL")
		}
		next.WALIdentity = wal
		if err := l.Bind(next); err != nil {
			t.Fatal(err)
		}
		prior := l.Runs[id.RunID]
		if prior.Exited || prior.Stop != nil || prior.NeverBoundExit != nil || prior.IndependentTerminal != nil || l.ClosedBy != "" {
			t.Fatal("reservation revocation fabricated termination or closed the generation")
		}
	}
}
