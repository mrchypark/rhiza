//go:build rhiza_stall_diagnostics

package quepaxa

import (
	"context"
	"fmt"
	"log"
	"runtime/pprof"
	"sync"
	"time"
)

const stallDiagnosticsEnabled = true

// Only slot, step and peer identity are recorded; request contents are excluded.
func stallRecordContext(ctx context.Context, peer NodeID, request RecordRequest) (context.Context, func(error)) {
	ctx = pprof.WithLabels(ctx, pprof.Labels("rhiza_slot", fmt.Sprint(request.Slot), "rhiza_step", fmt.Sprint(request.Step), "rhiza_peer", string(peer)))
	pprof.SetGoroutineLabels(ctx)
	started := time.Now()
	var mu sync.Mutex
	reported, completed := false, false
	timer := time.AfterFunc(time.Second, func() {
		mu.Lock()
		defer mu.Unlock()
		if completed {
			return
		}
		reported = true
		log.Printf("stall_record begin slot=%d step=%d peer=%s", request.Slot, request.Step, peer)
	})
	return ctx, func(err error) {
		timer.Stop()
		mu.Lock()
		defer mu.Unlock()
		completed = true
		if reported {
			log.Printf("stall_record end slot=%d step=%d peer=%s duration=%s success=%t", request.Slot, request.Step, peer, time.Since(started), err == nil)
		}
	}
}
