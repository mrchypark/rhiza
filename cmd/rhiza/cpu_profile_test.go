//go:build ci_profile

package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"
)

func TestCPUProfileSetup(t *testing.T) {
	t.Setenv("RHIZA_CI_CPU_PROFILE_PORT", "")
	stop, err := startCPUProfile()
	if err != nil {
		t.Fatal(err)
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	for _, port := range []string{"invalid", "0", "65536", "127.0.0.1:6060"} {
		t.Setenv("RHIZA_CI_CPU_PROFILE_PORT", port)
		if _, err := startCPUProfile(); err == nil {
			t.Fatalf("expected invalid port %q to fail", port)
		}
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	t.Setenv("RHIZA_CI_CPU_PROFILE_PORT", strconv.Itoa(listener.Addr().(*net.TCPAddr).Port))
	if _, err := startCPUProfile(); err == nil {
		t.Fatal("expected occupied port to fail")
	}
}

func TestCPUProfileShutdownCancelsActiveRequest(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	t.Setenv("RHIZA_CI_CPU_PROFILE_PORT", strconv.Itoa(port))
	stop, err := startCPUProfile()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stop() })
	result := make(chan error, 1)
	go func() {
		client := &http.Client{Timeout: 5 * time.Second}
		response, err := client.Get("http://" + addr + "/debug/pprof/profile?seconds=30")
		if err == nil {
			_, err = io.Copy(io.Discard, response.Body)
			response.Body.Close()
			if err == nil && response.StatusCode != http.StatusOK {
				err = fmt.Errorf("profile returned %s", response.Status)
			}
		}
		result <- err
	}()
	waitProfileReady(t, addr, http.StatusOK)
	response, err := http.Get("http://" + addr + "/debug/pprof/profile?seconds=1")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("concurrent profile returned %s", response.Status)
	}
	if err := stop(); err != nil {
		t.Fatalf("shutdown with active profile: %v", err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("profile request: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("profile request did not finish after shutdown")
	}
}

func waitProfileReady(t *testing.T, addr string, status int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	client := &http.Client{Timeout: time.Second}
	for {
		response, err := client.Get("http://" + addr + "/debug/pprof/ready")
		if err == nil {
			response.Body.Close()
			if response.StatusCode == status {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("profile readiness did not reach %d: %v", status, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCPUProfileCancellationClearsReady(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	t.Setenv("RHIZA_CI_CPU_PROFILE_PORT", strconv.Itoa(port))
	stop, err := startCPUProfile()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stop() })
	waitProfileReady(t, addr, http.StatusServiceUnavailable)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/debug/pprof/profile?seconds=30", nil)
		response, err := http.DefaultClient.Do(req)
		if err == nil {
			response.Body.Close()
		}
	}()
	waitProfileReady(t, addr, http.StatusOK)
	cancel()
	<-done
	waitProfileReady(t, addr, http.StatusServiceUnavailable)
}

func TestCPUProfileLoopbackAndShutdown(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	t.Setenv("RHIZA_CI_CPU_PROFILE_PORT", strconv.Itoa(port))
	stop, err := startCPUProfile()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stop() })
	response, err := http.Get("http://" + addr + "/debug/pprof/profile?seconds=1")
	if err != nil {
		t.Fatal(err)
	}
	profile, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || response.StatusCode != http.StatusOK || len(profile) == 0 {
		t.Fatalf("profile: status %s, bytes %d, error %v", response.Status, len(profile), err)
	}
	waitProfileReady(t, addr, http.StatusServiceUnavailable)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if conn, err := net.Dial("tcp4", addr); err == nil {
		conn.Close()
		t.Fatal("profile listener still open after shutdown")
	}
}
