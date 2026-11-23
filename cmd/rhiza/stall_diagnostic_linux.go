//go:build rhiza_stall_diagnostics && linux

package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"
	"time"
)

func startStallDiagnostics(ctx context.Context) func() {
	path := os.Getenv("RHIZA_STALL_TRACE_FILE")
	if path == "" {
		return func() {}
	}
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGUSR1)
	ctx, cancel := context.WithCancel(ctx)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ch:
				f, err := os.OpenFile(path+".tmp", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0600)
				if err != nil {
					log.Printf("stall diagnostic: open failed")
					continue
				}
				_, _ = f.WriteString("snapshot " + time.Now().UTC().Format(time.RFC3339Nano) + "\n")
				if err := pprof.Lookup("goroutine").WriteTo(f, 1); err != nil {
					log.Printf("stall diagnostic: snapshot failed")
					_ = f.Close()
					continue
				}
				if err := f.Close(); err != nil {
					log.Printf("stall diagnostic: close failed")
					continue
				}
				if err := os.Rename(path+".tmp", path); err != nil {
					log.Printf("stall diagnostic: publish failed")
				}
			}
		}
	}()
	return func() {
		signal.Stop(ch)
		cancel()
		<-stopped
	}
}
