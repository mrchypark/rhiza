package localtesthooks

import (
	"reflect"
	"testing"
	"time"
)

func TestACKPhaseTraceBuildContract(t *testing.T) {
	if !Enabled {
		if reflect.TypeFor[ACKPhaseTrace]().Size() != 0 {
			t.Fatal("disabled trace retains timestamps")
		}
		if got := testing.AllocsPerRun(100, func() {
			trace := NewACKPhaseTrace()
			trace.Finish("propose", false)
			ObserveACKBatch(time.Time{}, 1, 2)
		}); got != 0 {
			t.Fatalf("disabled trace allocates: %v", got)
		}
		return
	}
	var events []ACKPhaseEvent
	restore := SetACKPhase(func(e ACKPhaseEvent) { events = append(events, e) })
	trace := NewACKPhaseTrace()
	trace.Finish("propose", true)
	ObserveACKBatch(time.Now(), 3, 4)
	restore()
	trace.Finish("apply", false)
	if len(events) != 2 || events[0].Phase != "propose" || !events[0].Failed || events[0].Nanos < 0 || events[1].Commands != 3 || events[1].Bytes != 4 {
		t.Fatalf("unexpected trace: %+v", events)
	}
}
