# `BeginSnapshot` can return `ErrWriteTxActive` after the application writer has returned

## Problem

With `v0.5.0`, a successful `Update` that crosses
`WALCheckpointThresholdBytes` schedules a background checkpoint.  During the
worker's short `writeMu` ownership, the public `BeginSnapshot` method uses
`TryLock` and returns `ErrWriteTxActive` immediately.

This can make an online backup caller fail even though its only application
writer has already returned.  This report does not claim data loss or a
checkpoint/recovery failure; it reports a transient public API failure caused
by the automatic background worker.

## Public-API reproducer

Save the following as `main.go` in a new directory, then run:

```sh
go mod init snapshot-repro
go get github.com/mrchypark/latticedb-go@v0.5.0
go run .
```

```go
package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	latticedb "github.com/mrchypark/latticedb-go"
)

const (
	rounds         = 20
	payloadBytes   = 2 << 20
	probeWindow    = 250 * time.Millisecond
	checkpointSize = 1 << 20
)

func main() {
	observed := 0
	for round := 1; round <= rounds; round++ {
		attempt, err := runRound(round)
		if err != nil {
			fmt.Fprintf(os.Stderr, "round %d: %v\n", round, err)
			os.Exit(1)
		}
		if attempt == 0 {
			fmt.Printf("round %02d: no ErrWriteTxActive in %s\n", round, probeWindow)
			continue
		}
		observed++
		fmt.Printf("round %02d: ErrWriteTxActive on snapshot attempt %d\n", round, attempt)
	}
	fmt.Printf("observed ErrWriteTxActive in %d/%d rounds\n", observed, rounds)
}

func runRound(round int) (int, error) {
	dir, err := os.MkdirTemp("", fmt.Sprintf("latticedb-snapshot-%02d-", round))
	if err != nil {
		return 0, err
	}
	defer os.RemoveAll(dir)
	db, err := latticedb.Open(filepath.Join(dir, "db.ltdb"), latticedb.OpenOptions{
		Create:                      true,
		WALCheckpointThresholdBytes: checkpointSize,
	})
	if err != nil {
		return 0, err
	}
	defer db.Close()
	payload := bytes.Repeat([]byte{'x'}, payloadBytes)
	if err := db.Update(func(tx *latticedb.Tx) error {
		return tx.PutAppMetadata([]byte("payload"), payload)
	}); err != nil {
		return 0, err
	}

	// The application writer above has returned. Only LatticeDB's background
	// checkpoint can own the write slot during this probing window.
	deadline := time.Now().Add(probeWindow)
	for attempt := 1; time.Now().Before(deadline); attempt++ {
		snapshot, err := db.BeginSnapshot()
		if errors.Is(err, latticedb.ErrWriteTxActive) {
			return attempt, nil
		}
		if err != nil {
			return 0, err
		}
		if err := snapshot.Close(); err != nil {
			return 0, err
		}
	}
	return 0, nil
}
```

Observed on macOS / darwin-arm64 / Go 1.27.0 with
`github.com/mrchypark/latticedb-go v0.5.0`:

```text
observed ErrWriteTxActive in 20/20 rounds
```

The precise failed probe number is scheduler dependent, so this is an observational reproducer rather than a deterministic test.

## Source-level cause

[internal/engine/snapshot.go at v0.5.0](https://github.com/mrchypark/latticedb-go/blob/299e3003e84f85becd504217a63e63dde784b36c/internal/engine/snapshot.go#L26) implements `BeginSnapshot` with:

```go
if !db.writeMu.TryLock() {
    return nil, ErrWriteTxActive
}
```

The background checkpoint also briefly owns that mutex for WAL rotation and
publication.  The public API does not distinguish that internal contention
from an active application write transaction.

## Expected behavior and proposed scope

Keep the existing nonblocking `ErrWriteTxActive` behavior when an application
write transaction is active.  When the mutex is temporarily held only by a
background checkpoint, `BeginSnapshot` should wait/retry for that worker to
release it, ideally through a context-aware snapshot API or the same bounded
checkpoint-aware wait used by other lifecycle paths.

Add a deterministic package-level regression test that blocks the background
checkpoint while it owns the writer slot, verifies a snapshot waits or honors
its context, and separately verifies that an actual application write remains
nonblocking.

## Related issues

This is related to but not a duplicate of:

- #105, which implemented background checkpoint progress and WAL rotation.
- #128, which added context-aware `Checkpoint` and `Close`, but not snapshot
  acquisition.
- #129, which addressed multiple concurrent immutable generation pins.
