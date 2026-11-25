//go:build rhiza_local_testhooks

package localtesthooks

import (
	"runtime"
	"sync/atomic"
	"time"
)

// CIPhase413 is test-only diagnostic data; request IDs are fixture-generated.
type CIPhase413 struct {
	At               int64    `json:"unix_ns"`
	Kind             string   `json:"kind"`
	Hash             [32]byte `json:"hash"`
	RequestID        string   `json:"request_id,omitempty"`
	EpochChanged     bool     `json:"epoch_changed"`
	Closing          bool     `json:"closing"`
	Quiescing        bool     `json:"quiescing"`
	LifecycleFailure bool     `json:"lifecycle_failure"`
}

type CIPhaseRecorder413 struct {
	events  chan CIPhase413
	dropped atomic.Uint64
	active  atomic.Int64
}

var ciPhase413 atomic.Pointer[CIPhaseRecorder413]

func StartCIPhase413(capacity int) *CIPhaseRecorder413 {
	r := &CIPhaseRecorder413{events: make(chan CIPhase413, capacity)}
	ciPhase413.Store(r)
	return r
}

func RecordCIPhase413(kind string, hash [32]byte, requestID string, epochChanged, closing, quiescing, lifecycleFailure bool) {
	at := time.Now().UnixNano()
	r := ciPhase413.Load()
	if r == nil {
		return
	}
	r.active.Add(1)
	defer r.active.Add(-1)
	if ciPhase413.Load() != r {
		return
	}
	event := CIPhase413{At: at, Kind: kind, Hash: hash, RequestID: requestID, EpochChanged: epochChanged, Closing: closing, Quiescing: quiescing, LifecycleFailure: lifecycleFailure}
	select {
	case r.events <- event:
	default:
		r.dropped.Add(1)
	}
}

func StopCIPhase413(r *CIPhaseRecorder413) ([]CIPhase413, uint64) {
	ciPhase413.CompareAndSwap(r, nil)
	for r.active.Load() != 0 {
		runtime.Gosched()
	}
	var events []CIPhase413
	for {
		select {
		case event := <-r.events:
			events = append(events, event)
		default:
			return events, r.dropped.Load()
		}
	}
}
