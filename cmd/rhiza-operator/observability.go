package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

type operatorHealth struct {
	ready  atomic.Bool
	leader atomic.Bool
}

func (h *operatorHealth) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !h.ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "rhiza_operator_ready %d\nrhiza_operator_leader %d\nrhiza_operator_build_info 1\n", boolMetric(h.ready.Load()), boolMetric(h.leader.Load()))
	})
	return mux
}

func boolMetric(v bool) int {
	if v {
		return 1
	}
	return 0
}

func startOperatorHealth(ctx context.Context, listen string, health *operatorHealth) error {
	if listen == "" {
		return nil
	}
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: health.handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	go func() { _ = server.Serve(listener) }()
	return nil
}
