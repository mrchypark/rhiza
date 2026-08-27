package types

import "context"

// Materializer applies decided values to local state.
// Implementations exist for SQL (SQLite), Graph (LatticeDB), and KV.
type Materializer interface {
	// Apply applies a decided value to the local materialization.
	// The slot is provided for ordering guarantees.
	Apply(ctx context.Context, slot Slot, value []byte) error

	// Snapshot captures the current state as bytes.
	Snapshot(ctx context.Context) ([]byte, error)

	// Restore restores state from a snapshot.
	Restore(ctx context.Context, data []byte) error

	// Profile returns the runtime profile this materializer handles.
	Profile() Profile
}

// ReadResult is the result of a read operation.
type ReadResult struct {
	Value  []byte
	Slot   Slot
	Exists bool
}

// ReadConsistency is the consistency level for reads.
type ReadConsistency int

const (
	// ReadLocal reads from local materialization only.
	ReadLocal ReadConsistency = iota
	// ReadBarrier reads after all applied slots up to the current tip.
	ReadBarrier
	// ReadQuorum reads after quorum confirmation.
	ReadQuorum
)

// Readable can read from the local materialization.
type Readable interface {
	// Read retrieves a value by key.
	Read(ctx context.Context, key string, consistency ReadConsistency) (*ReadResult, error)

	// ReadRange retrieves values in a key range.
	ReadRange(ctx context.Context, start, end string, consistency ReadConsistency) ([]ReadResult, error)
}

// Executable can execute commands against the materialization.
type Executable interface {
	// Execute runs a command and returns the result.
	Execute(ctx context.Context, cmd []byte) ([]byte, error)
}

// MaterializerRuntime combines all materializer capabilities.
type MaterializerRuntime interface {
	Materializer
	Readable
	Executable
}
