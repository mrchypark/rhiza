//go:build !rhiza_local_testhooks

package localtesthooks

import "context"

// WithArchiveGCPhaseTrace is an allocation-free identity outside local tests.
func WithArchiveGCPhaseTrace(ctx context.Context, _ func(string)) context.Context { return ctx }

func HitArchiveGCPhase(context.Context, string) {}

// Enabled is false when test boundary callbacks are compiled out.
const Enabled = false

// Hit is a production no-op. Boundary callbacks are only available in test
// binaries built with the rhiza_local_testhooks tag.
func Hit(string) {}
