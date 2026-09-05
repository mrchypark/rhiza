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
