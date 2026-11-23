//go:build rhiza_phase_diagnostics

package quepaxa

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestPhaseDiagnosticsCaptureSuccessAndQuorumFailure(t *testing.T) {
	cores, transport := newTestCluster(t)
	for i := range phaseDiagnostics {
		d := &phaseDiagnostics[i]
		resetPhaseSample(&d.success)
		resetPhaseSample(&d.failure)
		d.decisions.Store(0)
	}
	if _, _, err := cores["n2"].Propose(context.Background(), []byte("first")); err != nil {
		t.Fatal(err)
	}
	if phaseDiagnostics[0].success.count.Load() == 0 {
		t.Fatal("missing phase 0")
	}
	transport.fail("n3")
	transport.fail("n1")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := cores["n2"].runSlot(ctx, 100, []byte("second"), true); err == nil {
		t.Fatal("expected quorum failure")
	}
	var errors uint64
	for i := range phaseDiagnostics {
		errors += phaseDiagnostics[i].failure.count.Load()
	}
	if errors == 0 {
		t.Fatal("missing failed phase")
	}
}

func TestPhaseDiagnosticsConcurrentOverflowAndCancellation(t *testing.T) {
	d := &phaseDiagnostics[3]
	resetPhaseSample(&d.success)
	resetPhaseSample(&d.failure)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); observePhase(7, 2*time.Second, context.Canceled) }()
	}
	wg.Wait()
	if d.failure.count.Load() != 100 || d.success.count.Load() != 0 || d.failure.buckets[len(phaseBoundsNS)].Load() != 100 {
		t.Fatalf("concurrent overflow lost measurements: failure=%d success=%d overflow=%d", d.failure.count.Load(), d.success.count.Load(), d.failure.buckets[len(phaseBoundsNS)].Load())
	}
	upper, overflow := phaseP99Upper(&d.failure, 100)
	if upper != 0 || !overflow {
		t.Fatalf("overflow upper=%d overflow=%t", upper, overflow)
	}
	cores, _ := newTestCluster(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	before := phaseDiagnostics[0].failure.count.Load()
	transport := cores["n2"].transport.(*clusterTransport)
	transport.fail("n1", "n3")
	if _, err := cores["n2"].runSlot(ctx, 2, []byte("cancelled"), true); err == nil {
		t.Fatal("expected cancellation")
	}
	if phaseDiagnostics[0].failure.count.Load() <= before {
		t.Fatal("canceled quorum not recorded")
	}
}

// A zero upper bound plus overflow=true means p99 is greater than one second.
func phaseP99Upper(d *phaseSample, count uint64) (uint64, bool) {
	var cumulative uint64
	for j, bound := range phaseBoundsNS {
		cumulative += d.buckets[j].Load()
		if cumulative >= count-count/100 {
			return bound, false
		}
	}
	return 0, true
}

func resetPhaseSample(s *phaseSample) {
	s.count.Store(0)
	s.nanos.Store(0)
	for i := range s.buckets {
		s.buckets[i].Store(0)
	}
}

// BenchmarkPhaseDiagnostics reports wall time to a recorder quorum, including
// the local recorder, not complete request or durable-ACK latency.
func BenchmarkPhaseDiagnostics(b *testing.B) {
	for _, down := range []bool{false, true} {
		name := "healthy"
		if down {
			name = "one_peer_down"
		}
		b.Run(name, func(b *testing.B) {
			cores, transport := newTestCluster(b)
			if down {
				transport.fail("n1")
			}
			for i := range phaseDiagnostics {
				d := &phaseDiagnostics[i]
				resetPhaseSample(&d.success)
				resetPhaseSample(&d.failure)
				d.decisions.Store(0)
			}
			var value [8]byte
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				binary.LittleEndian.PutUint64(value[:], uint64(i))
				if _, _, err := cores["n2"].Propose(context.Background(), value[:]); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			for i := range phaseDiagnostics {
				d := &phaseDiagnostics[i]
				b.ReportMetric(float64(d.decisions.Load())/float64(b.N), fmt.Sprintf("phase%d_decisions/op", i))
				reportPhaseSample(b, i, "success", &d.success)
				reportPhaseSample(b, i, "failure", &d.failure)
			}
		})
	}
}

func reportPhaseSample(b *testing.B, phase int, outcome string, s *phaseSample) {
	prefix := fmt.Sprintf("phase%d_recordquorum_%s", phase, outcome)
	count := s.count.Load()
	b.ReportMetric(float64(count)/float64(b.N), prefix+"_calls/op")
	if count == 0 {
		return
	}
	b.ReportMetric(float64(s.nanos.Load())/float64(count), prefix+"_ns/call")
	upper, overflow := phaseP99Upper(s, count)
	if overflow {
		b.ReportMetric(float64(phaseBoundsNS[len(phaseBoundsNS)-1]), prefix+"_p99lower_ns")
		b.ReportMetric(1, prefix+"_p99upper_unbounded")
	} else {
		b.ReportMetric(float64(upper), prefix+"_p99upper_ns")
		b.ReportMetric(0, prefix+"_p99upper_unbounded")
	}
	b.ReportMetric(float64(s.buckets[len(phaseBoundsNS)].Load())/float64(count), prefix+"_over1s_fraction")
}
