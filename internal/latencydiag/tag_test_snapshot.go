//go:build rhiza_local_testhooks && rhiza_latency_diag

package latencydiag

// TagTestEvent exposes only fixed pre-enqueue diagnostic fields to tests.
type TagTestEvent struct {
	Stage                  string
	Tag, Shard             uint64
	Outcome                string
	StartUnixNS, EndUnixNS int64
}

// HoldTagTestFlush prevents the asynchronous writer from consuming the ring
// while a deterministic local test inspects it.
func HoldTagTestFlush() func() {
	flushMu.Lock()
	ring.drain()
	return flushMu.Unlock
}

// TagTestSnapshot drains the real bounded ring and its drop counters.
func TagTestSnapshot() ([]TagTestEvent, uint64, uint64) {
	events, dropped, preEnqueueDropped := ring.drainWithStats()
	result := make([]TagTestEvent, 0, len(events))
	for _, event := range events {
		if event.family != familyPreEnqueue && event.family != familyProposalGap {
			continue
		}
		result = append(result, TagTestEvent{
			Stage: event.stage, Tag: event.tag, Shard: uint64(event.count), Outcome: event.outcome,
			StartUnixNS: event.start.UnixNano(), EndUnixNS: event.end.UnixNano(),
		})
	}
	return result, dropped, preEnqueueDropped
}
