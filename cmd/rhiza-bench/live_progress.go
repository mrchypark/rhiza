package main

import (
	"encoding/json"
	"io"
	"os"
	"sync/atomic"
	"time"
)

// liveProgress is optional and contains only aggregate counts and fixed phases.
// postFaultTracking includes fault tracking and original timing bookkeeping;
// it does not count HTTP responses independently. Success and failure are
// logical outcomes, not HTTP-attempt counts. Independent atomic reads can skew.
type liveProgress struct {
	start             time.Time
	phase             atomic.Uint32
	started           atomic.Uint64
	postFaultTracking atomic.Uint64
	completed         atomic.Uint64
	succeeded         atomic.Uint64
	failed            atomic.Uint64
	retries           atomic.Uint64
	lastCompletedNS   atomic.Int64
	writerStatus      atomic.Uint32
}

const (
	progressDispatch = iota + 1
	progressWorkerJoin
	progressControlDrain
	progressSummarize
	progressWriteResult
)

type liveProgressSample struct {
	Sequence            int    `json:"sequence"`
	CaptureStatus       string `json:"capture_status"`
	ElapsedMS           int64  `json:"elapsed_ms"`
	MainPhase           uint32 `json:"main_phase"`
	Started             uint64 `json:"started"`
	PostFaultTracking   uint64 `json:"post_fault_tracking"`
	Completed           uint64 `json:"completed"`
	Succeeded           uint64 `json:"succeeded"`
	Failed              uint64 `json:"failed"`
	Retries             uint64 `json:"retries"`
	LastCompletionAgeMS int64  `json:"last_completion_age_ms"`
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
	return liveProgressSample{Sequence: seq, ElapsedMS: elapsed / int64(time.Millisecond), MainPhase: p.phase.Load(), Started: p.started.Load(), PostFaultTracking: p.postFaultTracking.Load(), Completed: p.completed.Load(), Succeeded: p.succeeded.Load(), Failed: p.failed.Load(), Retries: p.retries.Load(), LastCompletionAgeMS: age}
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
