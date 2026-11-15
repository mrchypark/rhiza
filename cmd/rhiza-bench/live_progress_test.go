package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLiveProgressCompletionTimestampNeverMovesBackward(t *testing.T) {
	p := &liveProgress{start: time.Now()}
	olderCaptured, releaseOlder, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() { close(olderCaptured); <-releaseOlder; p.recordCompletion(100); close(done) }()
	<-olderCaptured
	p.recordCompletion(200)
	close(releaseOlder)
	<-done
	if got := p.lastCompletedNS.Load(); got != 200 {
		t.Fatalf("last completion=%d, want 200", got)
	}
	if got := p.completed.Load(); got != 2 {
		t.Fatalf("completed=%d, want 2", got)
	}
}

func TestLiveProgressSlowestBoundedAndNoRegression(t *testing.T) {
	p := &liveProgress{start: time.Now()}
	p.recordSlowest(slowestProgress{StartUnixNS: 10, EndUnixNS: 110, LatencyNS: 100, Retries: 1, Succeeded: true, SlotKnown: true, Slot: 7})
	const workers = 32
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p.recordSlowest(slowestProgress{StartUnixNS: 20, EndUnixNS: int64(220 + i), LatencyNS: int64(200 + i), Retries: 2})
		}(i)
	}
	wg.Wait()
	s := p.sample(0, time.Now()).Slowest
	if s == nil || s.LatencyNS != 231 || s.Retries != 2 || s.SlotKnown || s.Slot != 0 || s.Succeeded {
		t.Fatalf("unexpected slowest: %+v", s)
	}
	encoded, err := json.Marshal(p.sample(0, time.Now()))
	if err != nil || strings.Contains(string(encoded), "SECRET_SENTINEL") {
		t.Fatalf("diagnostic JSON: %q, %v", encoded, err)
	}
}

func TestSlowestRequestReceiptScope(t *testing.T) {
	for _, tc := range []struct {
		name                string
		succeeded, verified bool
		response            string
		wantKnown           bool
	}{
		{"committed", true, true, `{"slot":7}`, true},
		{"failed", false, true, `{"slot":7}`, false},
		{"unverified", true, false, `{"slot":7}`, false},
		{"missing", true, true, `{}`, false},
		{"invalid", true, true, `SECRET_SENTINEL`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := slowestRequest(time.Unix(10, 0), time.Second, 2, tc.succeeded, tc.verified, []byte(tc.response))
			if s.SlotKnown != tc.wantKnown || (s.SlotKnown && s.Slot != 7) || (!s.SlotKnown && s.Slot != 0) {
				t.Fatalf("unexpected sample: %+v", s)
			}
			encoded, err := json.Marshal(s)
			if err != nil || strings.Contains(string(encoded), "SECRET_SENTINEL") {
				t.Fatalf("sample leaked response: %q %v", encoded, err)
			}
		})
	}
}

func TestLiveProgressUpdatesAfterFaultCompletionBoundary(t *testing.T) {
	p := &liveProgress{start: time.Now()}
	var retries atomic.Uint64
	begin := time.Now()
	reached, release, result := make(chan time.Time, 1), make(chan struct{}), make(chan time.Time, 1)
	go func() {
		completed, _ := observeRequestReturn(begin, func(at time.Time) { reached <- at; <-release }, &retries, 2, p)
		result <- completed
	}()
	completed := <-reached
	if p.postFaultTracking.Load() != 0 || p.retries.Load() != 0 {
		t.Fatal("diagnostics advanced before completion was captured")
	}
	before := time.Now()
	after := time.Now()
	close(release)
	if got := <-result; !got.Equal(completed) {
		t.Fatalf("completion changed: got=%v want=%v", got, completed)
	}
	if coversFault(begin, completed, before, after) {
		t.Fatal("post-completion fault was incorrectly qualified as overlap")
	}
	if p.postFaultTracking.Load() != 1 || p.retries.Load() != 2 || retries.Load() != 2 {
		t.Fatalf("diagnostic counters not recorded")
	}
}

func TestLiveProgressIsBoundedAndContainsOnlyAggregateSchema(t *testing.T) {
	const sentinel = "SECRET_SENTINEL_request_response_and_private_path"
	path := filepath.Join(t.TempDir(), sentinel)
	p := &liveProgress{start: time.Now().Add(-time.Second)}
	p.phase.Store(progressWorkerJoin)
	p.started.Store(8)
	p.postFaultTracking.Store(7)
	p.completed.Store(6)
	p.succeeded.Store(4)
	p.failed.Store(2)
	p.retries.Store(3)
	p.lastCompletedNS.Store(int64(time.Millisecond))
	finished := make(chan struct{})
	go func() { defer close(finished); p.writeLiveProgress(path, make(chan struct{}), time.Microsecond) }()
	select {
	case <-finished:
		if p.statusName() != "cap_reached" {
			t.Fatalf("status=%s", p.statusName())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("bounded recorder did not stop")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), sentinel) {
		t.Fatal("private path leaked into progress")
	}
	if len(data) > 64*1024 {
		t.Fatalf("progress file too large: %d", len(data))
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("progress permissions=%v error=%v", info, err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 120 {
		t.Fatalf("samples=%d, want fixed cap 120", len(lines))
	}
	for i, line := range lines {
		var s liveProgressSample
		if err := json.Unmarshal([]byte(line), &s); err != nil {
			t.Fatal(err)
		}
		if s.Sequence != i || s.MainPhase != progressWorkerJoin || s.Started != 8 || s.PostFaultTracking != 7 || s.Completed != 6 || s.Succeeded != 4 || s.Failed != 2 || s.Retries != 3 || s.LastCompletionAgeMS < 0 {
			t.Fatalf("sample=%+v", s)
		}
		wantStatus := "sample"
		if i == liveProgressSampleLimit-1 {
			wantStatus = "cap_reached"
		}
		if s.CaptureStatus != wantStatus {
			t.Fatalf("sample %d status=%q want %q", i, s.CaptureStatus, wantStatus)
		}
	}
}

type progressFailureWriter struct{}

func (progressFailureWriter) Write([]byte) (int, error) {
	return 0, errors.New("SECRET_SENTINEL_diagnostic_error")
}

type progressBlockedWriter struct{ entered chan struct{} }

func (w progressBlockedWriter) Write([]byte) (int, error) { close(w.entered); select {} }

func TestLiveProgressFailureProcessHelper(t *testing.T) {
	mode := os.Getenv("RHIZA_PROGRESS_FAILURE_CHILD")
	if mode == "" {
		return
	}
	p := &liveProgress{start: time.Now()}
	if mode == "write-failure" {
		p.writeLiveProgressSamples(progressFailureWriter{}, make(chan struct{}), time.Second)
		fmt.Fprintln(os.Stdout, p.statusName())
		os.Exit(23)
	}
	if mode == "open-failure" {
		p.writeLiveProgress(os.Getenv("RHIZA_PROGRESS_CHILD_PATH"), make(chan struct{}), time.Second)
		fmt.Fprintln(os.Stdout, p.statusName())
		os.Exit(23)
	}
	if mode == "blocked-write" {
		entered := make(chan struct{})
		stop := make(chan struct{})
		go p.writeLiveProgressSamples(progressBlockedWriter{entered: entered}, stop, time.Second)
		<-entered
		close(stop)
		fmt.Fprintln(os.Stderr, "diagnostic-write-blocked")
		os.Exit(23)
	}
	os.Exit(2)
}

func TestLiveProgressFailureDoesNotChangeExitOrLeakError(t *testing.T) {
	for _, mode := range []string{"write-failure", "blocked-write"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLiveProgressFailureProcessHelper$")
			cmd.Env = append(os.Environ(), "RHIZA_PROGRESS_FAILURE_CHILD="+mode)
			out, err := cmd.CombinedOutput()
			var exitErr *exec.ExitError
			if ctx.Err() != nil || !errors.As(err, &exitErr) || exitErr.ExitCode() != 23 {
				t.Fatalf("exit=%v context=%v output=%s", err, ctx.Err(), out)
			}
			if strings.Contains(string(out), "SECRET_SENTINEL") {
				t.Fatalf("sensitive error leaked: %s", out)
			}
			if mode == "write-failure" && strings.TrimSpace(string(out)) != "incomplete" {
				t.Fatalf("diagnostic status=%q", out)
			}
			if mode == "blocked-write" && strings.TrimSpace(string(out)) != "diagnostic-write-blocked" {
				t.Fatalf("blocked status=%q", out)
			}
		})
	}
}

func TestLiveProgressFailureWithClosedStderrPreservesExit(t *testing.T) {
	for _, mode := range []string{"open-failure", "write-failure"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			path := filepath.Join(t.TempDir(), "existing")
			if err := os.WriteFile(path, []byte("sentinel"), 0600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLiveProgressFailureProcessHelper$")
			cmd.Env = append(os.Environ(), "RHIZA_PROGRESS_FAILURE_CHILD="+mode, "RHIZA_PROGRESS_CHILD_PATH="+path)
			stderr, err := cmd.StderrPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := stderr.Close(); err != nil {
				t.Fatal(err)
			}
			out, err := cmd.Output()
			var exitErr *exec.ExitError
			if ctx.Err() != nil || !errors.As(err, &exitErr) || exitErr.ExitCode() != 23 {
				t.Fatalf("exit=%v context=%v output=%s", err, ctx.Err(), out)
			}
			want := "unavailable"
			if mode == "write-failure" {
				want = "incomplete"
			}
			if strings.TrimSpace(string(out)) != want {
				t.Fatalf("status=%q want=%q", out, want)
			}
			data, err := os.ReadFile(path)
			if err != nil || string(data) != "sentinel" {
				t.Fatalf("existing file changed: %q err=%v", data, err)
			}
		})
	}
}

func TestLiveProgressSkewDoesNotInventOutstandingRequests(t *testing.T) {
	start := time.Now()
	p := &liveProgress{start: start}
	p.postFaultTracking.Store(7)
	p.lastCompletedNS.Store(int64(2 * time.Second))
	s := p.sample(0, start.Add(time.Second))
	if s.Started != 0 || s.PostFaultTracking != 7 || s.LastCompletionAgeMS != -1 {
		t.Fatalf("skewed sample=%+v", s)
	}
}

func TestLiveProgressUnavailableFileDoesNotBlock(t *testing.T) {
	p := &liveProgress{start: time.Now()}
	done := make(chan struct{})
	path := filepath.Join(t.TempDir(), "missing", "progress")
	go func() {
		defer close(done)
		p.writeLiveProgress(path, make(chan struct{}), time.Second)
	}()
	select {
	case <-done:
		if p.statusName() != "unavailable" {
			t.Fatalf("status=%s", p.statusName())
		}
	case <-time.After(time.Second):
		t.Fatal("unavailable diagnostic did not return")
	}
}
