package quepaxa

import (
	"bytes"
	"crypto/sha256"
	"runtime"
	"testing"
)

func newMemoryEvidenceCore(items, valueBytes, certificateBytes int) (*Core, error) {
	decided := make(map[Slot]DecidedValue, items)
	for i := 0; i < items; i++ {
		slot := Slot(i + 1)
		value := bytes.Repeat([]byte{byte(i)}, valueBytes)
		proposal := newProposal(highestPriority, "memory-test", value)
		certificate, err := encodeCertificate(1, Decision{
			Slot: slot, Step: 4, Proposal: proposal,
			Summaries: []Summary{{RecorderID: "memory-test", Step: 4, FirstCurrent: cloneProposal(&proposal)}},
		})
		if err != nil {
			return nil, err
		}
		if len(certificate) < certificateBytes {
			certificate = append(certificate, bytes.Repeat([]byte{' '}, certificateBytes-len(certificate))...)
		}
		hash := sha256.Sum256(value)
		decided[slot] = DecidedValue{Slot: slot, Hash: hash, Value: value, Certificate: certificate}
	}
	return &Core{decided: decided, tip: Slot(items)}, nil
}

func TestDecisionsFromBoundedBoundsCloneAllocation(t *testing.T) {
	const (
		items            = 128
		valueBytes       = MaxReplicatedValueBytes
		certificateBytes = 64 << 10
		pageBudget       = 1 << 20
	)
	core, err := newMemoryEvidenceCore(items, valueBytes, certificateBytes)
	if err != nil {
		t.Fatal(err)
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	page, tip, err := core.DecisionsFromBounded(1, items, pageBudget)
	if err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&after)
	if tip != items || len(page) == 0 || len(page) >= items {
		t.Fatalf("tip=%d page length=%d, want a bounded non-empty prefix of %d", tip, len(page), items)
	}
	var payloadBytes uint64
	for i, decision := range page {
		if decision.Slot != Slot(i+1) {
			t.Fatalf("decision %d has slot %d", i, decision.Slot)
		}
		payloadBytes += uint64(len(decision.Value) + len(decision.Certificate))
	}
	if payloadBytes > pageBudget {
		t.Fatalf("returned payload bytes=%d, limit=%d", payloadBytes, pageBudget)
	}
	allocated := after.TotalAlloc - before.TotalAlloc
	t.Logf("bounded page clone: payload=%d B TotalAlloc=%d B budget=%d B; fixture=%d decisions with %d-byte values and %d-byte certificates", payloadBytes, allocated, pageBudget, items, valueBytes, certificateBytes)
	if allocated > pageBudget+(128<<10) {
		t.Fatalf("bounded page allocated %d bytes for %d payload bytes (budget %d); expected only the returned prefix to be cloned", allocated, payloadBytes, pageBudget)
	}
	next, _, err := core.DecisionsFromBounded(page[len(page)-1].Slot+1, items, pageBudget)
	if err != nil {
		t.Fatal(err)
	}
	if len(next) == 0 || next[0].Slot != page[len(page)-1].Slot+1 {
		t.Fatalf("next page starts at %v after page ending at %d", firstMemoryDecisionSlot(next), page[len(page)-1].Slot)
	}
	runtime.KeepAlive(page)
	runtime.KeepAlive(next)
	runtime.KeepAlive(core)
}

func firstMemoryDecisionSlot(values []DecidedValue) Slot {
	if len(values) == 0 {
		return 0
	}
	return values[0].Slot
}

func BenchmarkDecisionsFromBounded(b *testing.B) {
	for _, tc := range []struct {
		name             string
		items, valueSize int
		certSize         int
	}{
		{name: "small", items: 128, valueSize: 1024, certSize: 512},
		{name: "max-value", items: 64, valueSize: MaxReplicatedValueBytes, certSize: 512},
		{name: "large-certificate", items: 64, valueSize: 4096, certSize: 64 << 10},
	} {
		b.Run(tc.name, func(b *testing.B) {
			core, err := newMemoryEvidenceCore(tc.items, tc.valueSize, tc.certSize)
			if err != nil {
				b.Fatal(err)
			}
			const payloadBudget = 1 << 20
			probe, _, err := core.DecisionsFromBounded(1, tc.items, payloadBudget)
			if err != nil {
				b.Fatal(err)
			}
			var copiedBytes int
			for _, decision := range probe {
				copiedBytes += len(decision.Value) + len(decision.Certificate)
			}
			runtime.KeepAlive(probe)
			b.ReportAllocs()
			b.SetBytes(int64(copiedBytes))
			b.ResetTimer()
			for range b.N {
				page, _, err := core.DecisionsFromBounded(1, tc.items, payloadBudget)
				if err != nil {
					b.Fatal(err)
				}
				runtime.KeepAlive(page)
			}
			b.ReportMetric(float64(copiedBytes), "payload-B/op")
			b.ReportMetric(float64(tc.items), "fixture-items")
			runtime.KeepAlive(core)
		})
	}
}
