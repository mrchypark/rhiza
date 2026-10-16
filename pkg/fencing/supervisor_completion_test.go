package fencing

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type completionAuthority struct {
	request         StopRequest
	events          []string
	drains          int
	exits           int
	transientDrains int
	transientExits  int
}

func (a *completionAuthority) Register(context.Context, Identity) error { return nil }
func (a *completionAuthority) Bind(context.Context, Identity) error     { return nil }
func (a *completionAuthority) Stop(context.Context, Identity) (StopRequest, error) {
	a.events = append(a.events, "stop")
	return a.request, nil
}
func (a *completionAuthority) Drain(_ context.Context, r DrainReceipt) error {
	a.drains++
	a.events = append(a.events, "drain")
	if r.Request != a.request {
		return errors.New("different receipt")
	}
	if a.transientDrains > 0 {
		a.transientDrains--
		return ErrUnavailable
	}
	return nil
}
func (a *completionAuthority) Exit(_ context.Context, r StopRequest) error {
	a.exits++
	a.events = append(a.events, "exit")
	if r != a.request {
		return errors.New("different exit")
	}
	if a.transientExits > 0 {
		a.transientExits--
		return ErrUnavailable
	}
	return nil
}
func (a *completionAuthority) NeverBoundExit(context.Context, StopRequest) error {
	return errors.New("bound clean receipt cannot use never-bound exception")
}

func completionSupervisor() (*Supervisor, *completionAuthority) {
	id := testIdentity()
	id.WALIdentity = "wal"
	a := &completionAuthority{request: StopRequest{OperationID: "op", RequestHash: "hash", Identity: id}}
	return &Supervisor{Authority: a, bound: id, drain: &DrainReceipt{Request: a.request, WritersJoined: true}}, a
}

func TestSupervisorCleanReceiptThenChildWaitBeforeTickCompletes(t *testing.T) {
	s, a := completionSupervisor()
	// This invokes exactly the already-Waited select branch before any ticker
	// callback. The bound child acknowledged its clean receipt then exited.
	if err := s.finishExitedChild(t.Context(), func(context.Context) error { a.events = append(a.events, "namespace"); return nil }); err != nil {
		t.Fatal(err)
	}
	if a.drains != 1 || a.exits != 1 || !reflect.DeepEqual(a.events, []string{"stop", "drain", "namespace", "exit"}) {
		t.Fatalf("evidence order=%v drains=%d exits=%d", a.events, a.drains, a.exits)
	}
}

func TestSupervisorTickAndNaturalExitShareEvidenceOrder(t *testing.T) {
	s, a := completionSupervisor()
	waits := 0
	if err := s.finishDrain(t.Context(), *s.drain, func() error { waits++; a.events = append(a.events, "wait"); return nil }, func(context.Context) error { a.events = append(a.events, "namespace"); return nil }); err != nil {
		t.Fatal(err)
	}
	if waits != 1 || !reflect.DeepEqual(a.events, []string{"stop", "drain", "wait", "namespace", "exit"}) {
		t.Fatalf("evidence order=%v waits=%d", a.events, waits)
	}
}

func TestSupervisorExitedChildCannotForgeCompletion(t *testing.T) {
	for _, scenario := range []string{"absent", "unclean", "different-operation", "different-run", "cleanup-failed"} {
		t.Run(scenario, func(t *testing.T) {
			s, a := completionSupervisor()
			switch scenario {
			case "absent":
				s.drain = nil
			case "unclean":
				s.drain.UnknownMutations = 1
			case "different-operation":
				s.drain.Request.OperationID = "other"
			case "different-run":
				s.drain.Request.Identity.RunID = "other"
			}
			err := s.finishExitedChild(t.Context(), func(context.Context) error {
				if scenario == "cleanup-failed" {
					return errors.New("namespace not empty")
				}
				return nil
			})
			if err == nil || a.exits != 0 {
				t.Fatalf("exit accepted: error=%v calls=%d", err, a.exits)
			}
		})
	}
}

func TestSupervisorExitedChildReplaysOnlySameReceiptDuringAuthorityOutage(t *testing.T) {
	s, a := completionSupervisor()
	a.transientDrains = 1
	a.transientExits = 1
	cleanups := 0
	if err := s.finishExitedChild(t.Context(), func(context.Context) error { cleanups++; return nil }); err != nil {
		t.Fatal(err)
	}
	if a.drains != 2 || a.exits != 2 || cleanups != 1 {
		t.Fatalf("drains=%d exits=%d namespace=%d", a.drains, a.exits, cleanups)
	}
}
