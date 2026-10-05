//go:build !rhiza_local_testhooks

package localtesthooks

import "context"

// ArchiveBusyEvent has no production fields; all calls are compiled out.
type ArchiveBusyEvent struct {
	Operation string
	Resource  string
	Branch    string
	Stage     string
	Entered   bool
}

func WithArchiveBusyTrace(ctx context.Context, _ func(ArchiveBusyEvent)) context.Context { return ctx }

func HitArchiveBusy(context.Context, ArchiveBusyEvent) {}

// CarryArchiveBusyObserver is an identity when the hooks are compiled out.
func CarryArchiveBusyObserver(dst, _ context.Context) context.Context { return dst }

// WithArchiveGCPhaseTrace is an allocation-free identity outside local tests.
func WithArchiveGCPhaseTrace(ctx context.Context, _ func(string)) context.Context { return ctx }

func HitArchiveGCPhase(context.Context, string) {}

// Enabled is false when test boundary callbacks are compiled out.
const Enabled = false

// Hit is a production no-op. Boundary callbacks are only available in test
// binaries built with the rhiza_local_testhooks tag.
func Hit(string) {}
