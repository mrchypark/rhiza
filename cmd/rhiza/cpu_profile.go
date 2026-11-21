//go:build ci_profile

package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"runtime/pprof"
	"strconv"
	"sync/atomic"
	"time"
)

func startCPUProfile() (func() error, error) {
	port := os.Getenv("RHIZA_CI_CPU_PROFILE_PORT")
	if port == "" {
		return func() error { return nil }, nil
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return nil, fmt.Errorf("invalid RHIZA_CI_CPU_PROFILE_PORT %q", port)
	}
	listener, err := net.Listen("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(n)))
	if err != nil {
		return nil, fmt.Errorf("listen for CI CPU profile: %w", err)
	}
	mux := http.NewServeMux()
	var state atomic.Int32
	mux.HandleFunc("GET /debug/pprof/ready", func(w http.ResponseWriter, _ *http.Request) {
		if state.Load() != 2 {
			http.Error(w, "CPU profile not active", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /debug/pprof/profile", func(w http.ResponseWriter, r *http.Request) {
		seconds := int64(30)
		if raw := r.URL.Query().Get("seconds"); raw != "" {
			var err error
			seconds, err = strconv.ParseInt(raw, 10, 64)
			if err != nil || seconds < 1 || seconds > int64((1<<63-1)/int64(time.Second)) {
				http.Error(w, "invalid profile duration", http.StatusBadRequest)
				return
			}
		}
		if !state.CompareAndSwap(0, 1) {
			http.Error(w, "CPU profile already active", http.StatusConflict)
			return
		}
		defer state.Store(0)
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", `attachment; filename="profile"`)
		if err := pprof.StartCPUProfile(w); err != nil {
			w.Header().Del("Content-Disposition")
			http.Error(w, "could not start CPU profile: "+err.Error(), http.StatusInternalServerError)
			return
		}
		state.Store(2)
		timer := time.NewTimer(time.Duration(seconds) * time.Second)
		select {
		case <-timer.C:
		case <-r.Context().Done():
		}
		timer.Stop()
		state.Store(1)
		pprof.StopCPUProfile()
	})
	profileCtx, cancelProfile := context.WithCancel(context.Background())
	server := &http.Server{
		Handler: mux, ReadHeaderTimeout: 5 * time.Second,
		BaseContext: func(net.Listener) context.Context { return profileCtx },
	}
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("CI CPU profile server: %v", err)
		}
	}()
	return func() error {
		cancelProfile()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			return errors.Join(err, server.Close())
		}
		return nil
	}, nil
}
