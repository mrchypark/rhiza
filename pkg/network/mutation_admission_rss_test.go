//go:build (linux || darwin) && !race

package network

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
)

func TestMutationAdmissionPeakRSS(t *testing.T) {
	if os.Getenv("RHIZA_MUTATION_RSS_CHILD") != "1" {
		executable, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(executable, "-test.run=^TestMutationAdmissionPeakRSS$", "-test.v")
		cmd.Env = append(os.Environ(), "RHIZA_MUTATION_RSS_CHILD=1")
		output, err := cmd.CombinedOutput()
		t.Log(string(output))
		if err != nil {
			t.Fatalf("isolated RSS measurement failed: %v", err)
		}
		return
	}

	server := NewServer(nil, nil, "cluster", true, nil)
	defer server.Close()
	const payloadBytes = 160 << 10
	charge, err := mutationCharge(types.KVCommand{RequestID: "rss-000000", Operation: "put", Key: "k", Value: make([]byte, payloadBytes)})
	if err != nil {
		t.Fatal(err)
	}
	count := maxAdmittedBytes / charge
	ids := make([]string, count)
	stripe := func(id string) uint16 {
		hash := sha256.Sum256([]byte(id))
		return uint16(hash[0])<<4 | uint16(hash[1])>>4
	}
	found := 0
	for i := 0; found < count; i++ {
		id := fmt.Sprintf("rss-%06d", i)
		if stripe(id) == stripe("hold-rss") {
			ids[found] = id
			found++
		}
	}
	unlock, err := server.lockRequest(context.Background(), "hold-rss")
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	values := make([][]byte, count)
	for i := range values {
		values[i] = make([]byte, payloadBytes)
	}
	runtime.GC()
	baseline := peakRSSBytes(t)
	ctxs, cancels := make([]context.Context, count), make([]context.CancelFunc, count)
	done := make(chan error, count)
	for i := range ids {
		ctxs[i], cancels[i] = context.WithCancel(context.Background())
		go func(ctx context.Context, id string, payload []byte) {
			_, err := server.KVPut(ctx, KVMutationRequest{RequestID: id, Key: "k", Value: payload})
			done <- err
		}(ctxs[i], ids[i], values[i])
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		server.mutationMu.Lock()
		admitted := server.mutationAdmission.count
		server.mutationMu.Unlock()
		if admitted == count {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("admitted %d of %d stripe-blocked requests", admitted, count)
		}
		time.Sleep(time.Millisecond)
	}
	peak := peakRSSBytes(t)
	if delta := peak - baseline; delta > maxAdmittedBytes {
		t.Fatalf("peak RSS rose %d bytes above caller-fixture baseline; budget=%d", delta, maxAdmittedBytes)
	}
	t.Logf("peak RSS delta over caller-fixture baseline: %d bytes (budget %d)", peak-baseline, maxAdmittedBytes)
	for _, cancel := range cancels {
		cancel()
	}
	for range ids {
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled stripe waiter error=%v", err)
		}
	}
}

func peakRSSBytes(t *testing.T) uint64 {
	t.Helper()
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		t.Fatal(err)
	}
	value := uint64(usage.Maxrss)
	if runtime.GOOS == "linux" {
		value *= 1024
	}
	return value
}
