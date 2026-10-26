package fencing

import (
	"encoding/json"
	"strings"
	"testing"
)

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
