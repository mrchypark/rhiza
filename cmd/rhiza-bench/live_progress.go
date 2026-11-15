package main

import (
	"encoding/json"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// liveProgress is optional and contains only aggregate counts and fixed phases.
// postFaultTracking includes fault tracking and original timing bookkeeping;
// it does not count HTTP responses independently. Success and failure are
// logical outcomes, not HTTP-attempt counts. Independent atomic reads can skew.
type liveProgress struct {
	start              time.Time
	phase              atomic.Uint32
	started            atomic.Uint64
	postFaultTracking  atomic.Uint64
	completed          atomic.Uint64
	succeeded          atomic.Uint64
	failed             atomic.Uint64
	retries            atomic.Uint64
	lastCompletedNS    atomic.Int64
	writerStatus       atomic.Uint32
	captureSlowest     bool
	slowest            atomic.Pointer[slowestProgress]
	captureSlotSamples bool
	sampleMu           sync.Mutex
	sampleItems        [64]slowestProgress
	sampleCount        int
	sampleDropped      atomic.Uint64
	sampleUnknown      atomic.Uint64
}

// Samples use fixed slot modulo rather than request IDs; retain the slowest
// completed samples seen without blocking the workload. Drops include eviction.
func (p *liveProgress) recordSlotSample(s slowestProgress) {
	if !s.SlotKnown {
		p.sampleUnknown.Add(1)
		return
	}
	if s.Slot%16 != 0 {
		return
	}
	if !p.sampleMu.TryLock() {
		p.sampleDropped.Add(1)
		return
	}
	defer p.sampleMu.Unlock()
	if p.sampleCount == len(p.sampleItems) {
		p.sampleDropped.Add(1)
		min := 0
		for i := 1; i < p.sampleCount; i++ {
			if p.sampleItems[i].LatencyNS < p.sampleItems[min].LatencyNS {
				min = i
			}
		}
		if s.LatencyNS > p.sampleItems[min].LatencyNS {
			p.sampleItems[min] = s
		}
		return
	}
	p.sampleItems[p.sampleCount] = s
	p.sampleCount++
}

// slowestProgress is one completed logical request, not a p99 estimate.
// A missing or failed receipt must never be interpreted as a known slot.
type slowestProgress struct {
	StartUnixNS int64  `json:"start_unix_ns"`
	EndUnixNS   int64  `json:"measured_end_unix_ns"`
	LatencyNS   int64  `json:"latency_ns"`
	Retries     int    `json:"retries"`
	Succeeded   bool   `json:"succeeded"`
	SlotKnown   bool   `json:"slot_known"`
	Slot        uint64 `json:"slot"`
}

func slowestRequest(begin time.Time, elapsed time.Duration, retries int, succeeded, verified bool, response []byte) slowestProgress {
	s := slowestProgress{StartUnixNS: begin.UnixNano(), EndUnixNS: begin.Add(elapsed).UnixNano(), LatencyNS: elapsed.Nanoseconds(), Retries: retries, Succeeded: succeeded}
	if succeeded && verified {
		var receipt struct {
			Slot uint64 `json:"slot"`
		}
		if json.Unmarshal(response, &receipt) == nil && receipt.Slot != 0 {
			s.Slot, s.SlotKnown = receipt.Slot, true
		}
	}
	return s
}

func (p *liveProgress) recordSlowest(sample slowestProgress) {
	for previous := p.slowest.Load(); previous == nil || sample.LatencyNS > previous.LatencyNS; previous = p.slowest.Load() {
		if p.slowest.CompareAndSwap(previous, &sample) {
			return
		}
	}
}

const (
	progressDispatch = iota + 1
	progressWorkerJoin
	progressControlDrain
	progressSummarize
	progressWriteResult
)

type liveProgressSample struct {
	Sequence            int               `json:"sequence"`
	CaptureStatus       string            `json:"capture_status"`
	ElapsedMS           int64             `json:"elapsed_ms"`
	MainPhase           uint32            `json:"main_phase"`
	Started             uint64            `json:"started"`
	PostFaultTracking   uint64            `json:"post_fault_tracking"`
	Completed           uint64            `json:"completed"`
	Succeeded           uint64            `json:"succeeded"`
	Failed              uint64            `json:"failed"`
	Retries             uint64            `json:"retries"`
	LastCompletionAgeMS int64             `json:"last_completion_age_ms"`
	Slowest             *slowestProgress  `json:"slowest_completed,omitempty"`
	SlotSamples         []slowestProgress `json:"slot_samples,omitempty"`
	SlotSamplesDropped  uint64            `json:"slot_samples_dropped,omitempty"`
	SlotUnknown         uint64            `json:"slot_unknown,omitempty"`
	SlotSamplesEnabled  bool              `json:"slot_samples_enabled,omitempty"`
}

const liveProgressSampleLimit = 120

const (
	progressWriterStarting = iota
	progressWriterRunning
	progressWriterUnavailable
	progressWriterIncomplete
	progressWriterStopped
	progressWriterCapped
)

func (p *liveProgress) statusName() string {
	switch p.writerStatus.Load() {
	case progressWriterRunning:
		return "running"
	case progressWriterUnavailable:
		return "unavailable"
	case progressWriterIncomplete:
		return "incomplete"
	case progressWriterStopped:
		return "stopped"
	case progressWriterCapped:
		return "cap_reached"
	default:
		return "starting"
	}
}

func (p *liveProgress) recordCompletion(elapsedNS int64) {
	for previous := p.lastCompletedNS.Load(); elapsedNS > previous; previous = p.lastCompletedNS.Load() {
		if p.lastCompletedNS.CompareAndSwap(previous, elapsedNS) {
			break
		}
	}
	p.completed.Add(1)
}

func (p *liveProgress) sample(seq int, now time.Time) liveProgressSample {
	elapsed := now.Sub(p.start).Nanoseconds()
	last := p.lastCompletedNS.Load()
	age := int64(-1)
	if last > 0 && elapsed >= last {
		age = (elapsed - last) / int64(time.Millisecond)
	}
	s := liveProgressSample{Sequence: seq, ElapsedMS: elapsed / int64(time.Millisecond), MainPhase: p.phase.Load(), Started: p.started.Load(), PostFaultTracking: p.postFaultTracking.Load(), Completed: p.completed.Load(), Succeeded: p.succeeded.Load(), Failed: p.failed.Load(), Retries: p.retries.Load(), LastCompletionAgeMS: age, Slowest: p.slowest.Load()}
	if p.captureSlotSamples {
		s.SlotSamplesEnabled = true
		p.sampleMu.Lock()
		s.SlotSamples = append(s.SlotSamples, p.sampleItems[:p.sampleCount]...)
		p.sampleMu.Unlock()
		s.SlotSamplesDropped = p.sampleDropped.Load()
		s.SlotUnknown = p.sampleUnknown.Load()
	}
	return s
}

// writeLiveProgress never blocks request handlers on a diagnostic file.
// At most 120 fixed-size samples are retained; file errors only stop capture.
func (p *liveProgress) writeLiveProgress(path string, stop <-chan struct{}, interval time.Duration) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		p.writerStatus.Store(progressWriterUnavailable)
		return
	}
	defer f.Close()
	p.writeLiveProgressSamples(f, stop, interval)
}

func (p *liveProgress) writeLiveProgressSamples(w io.Writer, stop <-chan struct{}, interval time.Duration) {
	p.writerStatus.Store(progressWriterRunning)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	enc := json.NewEncoder(w)
	for seq := 0; seq < liveProgressSampleLimit; seq++ {
		sample := p.sample(seq, time.Now())
		sample.CaptureStatus = "sample"
		if seq == liveProgressSampleLimit-1 {
			sample.CaptureStatus = "cap_reached"
		}
		if err := enc.Encode(sample); err != nil {
			p.writerStatus.Store(progressWriterIncomplete)
			return
		}
		if seq == liveProgressSampleLimit-1 {
			p.writerStatus.Store(progressWriterCapped)
			return
		}
		select {
		case <-stop:
			p.writerStatus.Store(progressWriterStopped)
			return
		case <-ticker.C:
		}
	}
}
