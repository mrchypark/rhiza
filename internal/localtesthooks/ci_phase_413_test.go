//go:build rhiza_local_testhooks

package localtesthooks

import (
	"sync"
	"testing"
)

func TestCIPhase413BoundedAndConcurrentStop(t *testing.T) {
	r := StartCIPhase413(1)
	RecordCIPhase413("first", [32]byte{1}, "request", true, false, true, false)
	RecordCIPhase413("dropped", [32]byte{}, "", false, false, false, false)
	events, dropped := StopCIPhase413(r)
	if len(events) != 1 || dropped != 1 || events[0].RequestID != "request" || !events[0].Quiescing {
		t.Fatalf("events=%+v dropped=%d", events, dropped)
	}
	r = StartCIPhase413(128)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				RecordCIPhase413("race", [32]byte{}, "", false, false, false, false)
			}
		}()
	}
	_, _ = StopCIPhase413(r)
	wg.Wait()
}
