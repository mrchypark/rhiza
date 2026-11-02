//go:build rhiza_ack_diagnostic && rhiza_local_testhooks

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
)

// This collector is absent from ordinary binaries. Its two signals delimit one
// diagnostic window; no request payload, identity or per-request log is kept.
var ackBounds = [...]int64{100000, 500000, 1000000, 2000000, 5000000, 10000000, 20000000, 50000000, 100000000, 200000000, 500000000, 1000000000, 5000000000}
var ackPhases = [...]string{"queue_oldest", "dispatch_wait", "batch_execute", "batch_apply_callback", "propose", "apply", "ack_barrier", "wal_durable", "object_durable"}

type ackPhaseSummary struct {
	Samples, Failures, Commands, Bytes uint64
	TotalNanos, MaxNanos               int64
	Buckets                            [14]uint64
}

type ackWindow struct {
	PID                  int
	StartedAt, StoppedAt string
	UpperBoundsNanos     [13]int64 // The last, fourteenth bucket is unbounded.
	Phases               map[string]*ackPhaseSummary
	Invalid              uint64
}

type ackCollector struct {
	mu     sync.Mutex
	active bool
	window ackWindow
}

func newACKCollector() *ackCollector {
	c := &ackCollector{window: ackWindow{PID: os.Getpid(), UpperBoundsNanos: ackBounds, Phases: make(map[string]*ackPhaseSummary)}}
	for _, phase := range ackPhases {
		c.window.Phases[phase] = &ackPhaseSummary{}
	}
	return c
}

func (c *ackCollector) observe(e localtesthooks.ACKPhaseEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.active {
		return
	}
	stats, ok := c.window.Phases[e.Phase]
	if !ok || e.Nanos < 0 || e.Commands < 0 || e.Bytes < 0 {
		c.window.Invalid++
		return
	}
	stats.Samples++
	if e.Failed {
		stats.Failures++
	}
	stats.Commands += uint64(e.Commands)
	stats.Bytes += uint64(e.Bytes)
	stats.TotalNanos += e.Nanos
	if e.Nanos > stats.MaxNanos {
		stats.MaxNanos = e.Nanos
	}
	bucket := len(ackBounds)
	for i, bound := range ackBounds {
		if e.Nanos <= bound {
			bucket = i
			break
		}
	}
	stats.Buckets[bucket]++
}

func (c *ackCollector) start() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active || c.window.StartedAt != "" {
		return errors.New("diagnostic window already started")
	}
	c.window.StartedAt = time.Now().UTC().Format(time.RFC3339Nano)
	c.active = true
	return nil
}

func (c *ackCollector) finish() ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.active {
		return nil, errors.New("diagnostic window not active")
	}
	c.active = false
	c.window.StoppedAt = time.Now().UTC().Format(time.RFC3339Nano)
	return json.Marshal(c.window)
}

func writeACKFile(dir, suffix string, data []byte) error {
	name := filepath.Join(dir, fmt.Sprintf("%d.%s", os.Getpid(), suffix))
	temporary := name + ".tmp"
	f, err := os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(data)
	err = errors.Join(writeErr, f.Close())
	if err == nil {
		err = os.Link(temporary, name)
	}
	return errors.Join(err, os.Remove(temporary))
}

func startACKSignalCollector(dir string) (func(), error) {
	info, err := os.Stat(dir)
	if err != nil || !filepath.IsAbs(dir) || !info.IsDir() || info.Mode().Perm() != 0700 {
		return nil, errors.New("diagnostic requires an owned mode0700 directory")
	}
	c := newACKCollector()
	restore := localtesthooks.SetACKPhase(c.observe)
	ctx, cancel := context.WithCancel(context.Background())
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGUSR1, syscall.SIGUSR2)
	if err := writeACKFile(dir, "ready", []byte(fmt.Sprintf("%d\n", os.Getpid()))); err != nil {
		signal.Stop(signals)
		restore()
		cancel()
		return nil, err
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer signal.Stop(signals)
		defer restore()
		for {
			select {
			case <-ctx.Done():
				return
			case sig := <-signals:
				var err error
				if sig == syscall.SIGUSR1 {
					err = c.start()
					if err == nil {
						err = writeACKFile(dir, "started", []byte("started\n"))
					}
				} else {
					var data []byte
					data, err = c.finish()
					if err == nil {
						err = writeACKFile(dir, "json", data)
					}
					if err == nil {
						return
					}
				}
				if err != nil {
					// A fixed failure marker, not an error string with paths.
					_ = writeACKFile(dir, "invalid", []byte("collector rejected transition or output\n"))
					return
				}
			}
		}
	}()
	var once sync.Once
	return func() { once.Do(cancel); <-done }, nil
}

func init() {
	if dir := os.Getenv("RHIZA_ACK_DIAGNOSTIC_DIR"); dir != "" {
		if _, err := startACKSignalCollector(dir); err != nil {
			log.Fatal("ACK diagnostic collector initialization rejected")
		}
	}
}
