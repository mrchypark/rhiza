//go:build !rhiza_stall_diagnostics

package quepaxa

import "context"

const stallDiagnosticsEnabled = false

func stallRecordContext(ctx context.Context, _ NodeID, _ RecordRequest) (context.Context, func(error)) {
	return ctx, func(error) {}
}
