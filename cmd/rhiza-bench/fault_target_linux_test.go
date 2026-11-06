//go:build linux

package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestFaultTargetDisposableChild(t *testing.T) {
	if os.Getenv("RHIZA_BENCH_FAULT_CHILD") != "1" {
		return
	}
	fmt.Println("ready")
	time.Sleep(time.Minute)
	os.Exit(0)
}

func faultChild(t *testing.T) (*exec.Cmd, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFaultTargetDisposableChild$")
	cmd.Env = append(os.Environ(), "RHIZA_BENCH_FAULT_CHILD=1")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	// Registered before any assertion or identity read; Wait is owned here.
	t.Cleanup(func() { cancel(); _ = cmd.Process.Kill(); _ = cmd.Wait() })
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || line != "ready\n" {
		t.Fatal("child not ready", line, err)
	}
	parent, start, err := faultProcessIdentity(cmd.Process.Pid)
	if err != nil || parent != os.Getpid() {
		t.Fatal("child identity", parent, err)
	}
	return cmd, start
}

func TestFaultPidfdIdentityAndClosedHandle(t *testing.T) {
	cmd, start := faultChild(t)
	if target, err := openFaultTarget(cmd.Process.Pid, start+"0", os.Getpid()); err == nil {
		target.close()
		t.Fatal("wrong start accepted")
	}
	if target, err := openFaultTarget(cmd.Process.Pid, start, os.Getpid()+1); err == nil {
		target.close()
		t.Fatal("wrong owner accepted")
	}
	target, err := openFaultTarget(cmd.Process.Pid, start, os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	target.close()
	if _, _, err := target.signal(); err == nil {
		t.Fatal("closed identity signalled")
	}
	// Negative opens/closed handle must not have killed the neighboring child.
	neighbor, err := openFaultTarget(cmd.Process.Pid, start, os.Getpid())
	if err != nil {
		t.Fatal("identity negatives killed child", err)
	}
	neighbor.close()
}

func TestFaultActualHTTPAndPidfdSignal(t *testing.T) {
	cmd, start := faultChild(t)
	target, err := openFaultTarget(cmd.Process.Pid, start, os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(target.close)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	release := make(chan struct{})
	var once sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
		_, _ = io.WriteString(w, `{"slot":1,"status":"committed","rows_affected":1,"retry_through_slot":65536}`)
	}))
	t.Cleanup(server.Close)
	p := &faultProgress{epoch: time.Now(), active: make(map[int]*requestInterval), signal: target.signal, token: "owned"}
	wrote := make(chan struct{}, 1)
	base := httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{WroteRequest: func(info httptrace.WroteRequestInfo) {
		if info.Err == nil {
			select {
			case wrote <- struct{}{}:
			case <-ctx.Done():
			}
		}
	}})
	requestCtx, finish := p.traceRequest(base, 0)
	done := make(chan struct{})
	var requestErr error
	go func() {
		defer close(done)
		_, _, _, requestErr = doRequest(requestCtx, server.Client(), http.MethodPost, server.URL, `{"request_id":"bench-0","sql":"INSERT INTO benchmark_writes(id) VALUES (0)"}`, 0, true)
		finish(time.Now())
	}()
	t.Cleanup(func() { cancel(); once.Do(func() { close(release) }); <-done })
	select {
	case <-wrote:
	case <-ctx.Done():
		t.Fatal("HTTP request not sent", ctx.Err())
	}
	w := httptest.NewRecorder()
	p.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/owned/fault", nil))
	if w.Code != 200 {
		t.Fatal("pidfd injection failed", w.Code, w.Body.String())
	}
	once.Do(func() { close(release) })
	<-done
	if requestErr != nil || !p.window().Overlap {
		t.Fatal("real interval refused", requestErr, p.window())
	}
	w = httptest.NewRecorder()
	p.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/owned/fault", nil))
	if w.Code != 409 {
		t.Fatal("duplicate signal accepted")
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("SIGKILL child unexpectedly exited successfully")
	}
	if ctx.Err() != nil {
		t.Fatal("child was not confirmed terminated before the test deadline", ctx.Err())
	}
	status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatal("child did not terminate by SIGKILL", cmd.ProcessState)
	}
}
