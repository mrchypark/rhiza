// Package applydiag carries bounded internal ApplyBatch diagnostics without
// wrapping or changing the errors returned to callers.
package applydiag

import (
	"context"
	"sync/atomic"
)

type Site uint32

const (
	Unknown Site = iota
	Policy
	Writer
	Health
	GraphHistory
	Connection
	Begin
	ManagedPolicy
	SlotConflict
	SlotGap
	ApplyValue
	Receipts
	Retention
	Context
	Commit
)

func (s Site) String() string {
	switch s {
	case Policy:
		return "policy"
	case Writer:
		return "writer"
	case Health:
		return "health"
	case GraphHistory:
		return "graph-history"
	case Connection:
		return "connection"
	case Begin:
		return "begin"
	case ManagedPolicy:
		return "managed-policy"
	case SlotConflict:
		return "slot-conflict"
	case SlotGap:
		return "slot-gap"
	case ApplyValue:
		return "apply-value"
	case Receipts:
		return "receipts"
	case Retention:
		return "retention"
	case Context:
		return "context"
	case Commit:
		return "commit"
	default:
		return "unknown"
	}
}

type Recorder struct{ site atomic.Uint32 }

func (r *Recorder) Set(site Site) {
	if r != nil {
		r.site.Store(uint32(site))
	}
}
func (r *Recorder) Site() Site {
	if r == nil {
		return Unknown
	}
	return Site(r.site.Load())
}

type key struct{}

func WithRecorder(ctx context.Context, r *Recorder) context.Context {
	return context.WithValue(ctx, key{}, r)
}
func FromContext(ctx context.Context) *Recorder {
	if ctx == nil {
		return nil
	}
	r, _ := ctx.Value(key{}).(*Recorder)
	return r
}
