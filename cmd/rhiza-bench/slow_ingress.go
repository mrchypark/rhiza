package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// slowIngress delays exactly one already committed response; the server stays alive.
type slowIngress struct {
	url       string
	server    *http.Server
	transport *http.Transport
	claimed   atomic.Bool
	committed atomic.Uint64
	delayedID atomic.Int64
}

func startSlowIngress(upstream string, delay time.Duration) (*slowIngress, error) {
	u, err := url.Parse(upstream)
	if err != nil || u.Scheme != "http" || u.Host == "" {
		return nil, fmt.Errorf("invalid slow ingress URL")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	s := &slowIngress{url: "http://" + listener.Addr().String()}
	s.delayedID.Store(-1)
	transport := &http.Transport{}
	s.transport = transport
	s.server = &http.Server{ReadHeaderTimeout: 2 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, "request read failed", 400)
			return
		}
		req, err := http.NewRequestWithContext(r.Context(), r.Method, upstream+r.URL.RequestURI(), bytes.NewReader(payload))
		if err != nil {
			http.Error(w, "invalid request", 500)
			return
		}
		req.Header = r.Header.Clone()
		resp, err := transport.RoundTrip(req)
		if err != nil {
			http.Error(w, "upstream transport failed", 502)
			return
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(io.LimitReader(resp.Body, 4097))
		if err != nil {
			http.Error(w, "upstream body failed", 502)
			return
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 && validateInsertReceipt(string(payload), body) == nil && s.claimed.CompareAndSwap(false, true) {
			var request struct {
				RequestID string `json:"request_id"`
			}
			if json.Unmarshal(payload, &request) != nil {
				http.Error(w, "invalid request id", 500)
				return
			}
			id, err := strconv.ParseInt(strings.TrimPrefix(request.RequestID, "bench-"), 10, 64)
			if err != nil {
				http.Error(w, "invalid request id", 500)
				return
			}
			s.delayedID.Store(id)
			s.committed.Add(1)
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-r.Context().Done():
				return
			}
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(body)
	})}
	go s.server.Serve(listener)
	return s, nil
}

func (s *slowIngress) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if s.server.Shutdown(ctx) != nil {
		_ = s.server.Close()
	}
	s.transport.CloseIdleConnections()
}

// A timeout is a witness only for the exact delayed request and no reroute.
func (s *slowIngress) isDelayedTimeout(id int, reroute time.Time, err error) bool {
	if s.delayedID.Load() != int64(id) || !reroute.IsZero() || err == nil {
		return false
	}
	var netErr net.Error
	return errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &netErr) && netErr.Timeout())
}
