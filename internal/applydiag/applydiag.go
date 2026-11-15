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

// PoisonOrigin is one bounded, in-memory observation; zero is unknown. It is
// never an authority for recovery or admission.
type PoisonOrigin struct {
	Known                                             bool
	Trigger                                           PoisonTrigger
	PrimarySite                                       Site
	FailedSlotKnown                                   bool
	TipKnown                                          bool
	FailedSlot, TipBeforeReset, BatchFirst, BatchLast uint64
	PrimaryClass, CleanupClass                        ErrorClass
	PrimaryCodeKnown, CleanupCodeKnown                bool
	PrimaryCode, CleanupCode                          int
}

type PoisonTrigger uint8

const (
	PoisonUnknown PoisonTrigger = iota
	PoisonRollback
	PoisonCommit
)

func (p PoisonTrigger) String() string {
	switch p {
	case PoisonRollback:
		return "rollback"
	case PoisonCommit:
		return "commit"
	default:
		return "unknown"
	}
}

type ErrorClass uint8

const (
	ErrorUnknown ErrorClass = iota
	ErrorNone
	ErrorCanceled
	ErrorDeadline
	ErrorSQLite
	ErrorConnectionDone
	ErrorTxDone
	ErrorFilesystem
)

func (e ErrorClass) String() string {
	switch e {
	case ErrorNone:
		return "none"
	case ErrorCanceled:
		return "canceled"
	case ErrorDeadline:
		return "deadline"
	case ErrorSQLite:
		return "sqlite"
	case ErrorConnectionDone:
		return "connection-done"
	case ErrorTxDone:
		return "tx-done"
	case ErrorFilesystem:
		return "filesystem"
	default:
		return "unknown"
	}
}

type Recorder struct {
	site   atomic.Uint32
	origin atomic.Pointer[PoisonOrigin]
}

func (r *Recorder) Reset() {
	if r != nil {
		r.site.Store(uint32(Unknown))
		r.origin.Store(nil)
	}
}
func (r *Recorder) SetOrigin(origin PoisonOrigin) {
	if r != nil && origin.Known {
		value := origin
		r.origin.Store(&value)
	}
}
func (r *Recorder) Origin() PoisonOrigin {
	if r == nil {
		return PoisonOrigin{}
	}
	value := r.origin.Load()
	if value == nil {
		return PoisonOrigin{}
	}
	return *value
}

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
