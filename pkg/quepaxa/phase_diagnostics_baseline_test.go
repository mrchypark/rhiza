//go:build !rhiza_phase_diagnostics

package quepaxa

import (
	"context"
	"encoding/binary"
	"testing"
)

// Match the instrumented workload so CI can measure diagnostic overhead.
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
			var value [8]byte
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				binary.LittleEndian.PutUint64(value[:], uint64(i))
				if _, _, err := cores["n2"].Propose(context.Background(), value[:]); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
