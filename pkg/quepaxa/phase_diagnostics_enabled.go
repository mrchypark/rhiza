//go:build rhiza_phase_diagnostics

package quepaxa

import (
	"sync/atomic"
	"time"
)

const phaseDiagnosticsEnabled = true

type phaseDiagnostic struct {
	success   phaseSample
	failure   phaseSample
	decisions atomic.Uint64
}

type phaseSample struct {
	count   atomic.Uint64
	nanos   atomic.Uint64
	buckets [14]atomic.Uint64
}

var phaseDiagnostics [4]phaseDiagnostic
var phaseBoundsNS = [...]uint64{100_000, 250_000, 500_000, 1_000_000, 2_000_000, 5_000_000, 10_000_000, 20_000_000, 50_000_000, 100_000_000, 250_000_000, 500_000_000, 1_000_000_000}

func observePhase(step Step, duration time.Duration, err error) {
	d := &phaseDiagnostics[int(step%4)]
	s := &d.success
	if err != nil {
		s = &d.failure
	}
	s.count.Add(1)
	s.nanos.Add(uint64(duration))
	index := len(phaseBoundsNS)
	for i, bound := range phaseBoundsNS {
		if uint64(duration) <= bound {
			index = i
			break
		}
	}
	s.buckets[index].Add(1)
}

func observeDecision(step Step) { phaseDiagnostics[int(step%4)].decisions.Add(1) }
