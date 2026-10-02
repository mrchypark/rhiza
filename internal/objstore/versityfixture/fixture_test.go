package versityfixture

import (
	"context"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestRequestCountSkipsOnlyStartMetadata(t *testing.T) {
	records := []string{
		"log starts 2026-10-02 11:30:27.576484 +0900 KST m=+0.008161626",
		"",
		"- - [02/October/2026:11:30:27 +0900] 127.0.0.1 key id s3_ListAllMyBuckets - / 200 -",
		"unrecognized preserved line",
	}
	if got := RequestCount(records); got != 2 {
		t.Fatalf("RequestCount() = %d, want 2", got)
	}
}

func TestPrepareEvidenceDirPersistsOutsideGoTemp(t *testing.T) {
	tempParent := t.TempDir()
	evidenceBase := filepath.Join(t.TempDir(), "issue185-evidence")
	runDir, err := prepareEvidenceDir(evidenceBase, tempParent, "run-one")
	if err != nil {
		t.Fatal(err)
	}
	if pathContains(tempParent, runDir) || pathContains(runDir, tempParent) {
		t.Fatalf("evidence path %q is not disjoint from Go temp %q", runDir, tempParent)
	}
	info, err := os.Stat(runDir)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o700 {
		t.Fatalf("evidence run directory mode=%#o, want 0700", got)
	}
	if _, err := prepareEvidenceDir(evidenceBase, tempParent, "run-one"); err == nil {
		t.Fatal("existing run ID evidence directory was reused")
	}
}

func TestPrepareEvidenceDirRejectsGoTemp(t *testing.T) {
	tempParent := t.TempDir()
	if _, err := prepareEvidenceDir(filepath.Join(tempParent, "evidence"), tempParent, "run-one"); err == nil {
		t.Fatal("evidence directory nested under Go temp was accepted")
	}
}

func TestPrepareEvidenceDirRequiresAbsolutePath(t *testing.T) {
	if _, err := prepareEvidenceDir("relative/evidence", t.TempDir(), "run-one"); err == nil {
		t.Fatal("relative evidence path was accepted")
	}
}

func TestChildSupervisorNormalCloseAndForcedExit(t *testing.T) {
	t.Run("normal close", func(t *testing.T) {
		assertFakeChildSupervisorStops(t, false)
	})
	t.Run("ignores interrupt then forced exit", func(t *testing.T) {
		assertFakeChildSupervisorStops(t, true)
	})
}

func TestChildSupervisorParentPipeEOF(t *testing.T) {
	tempDir := t.TempDir()
	outputPath := filepath.Join(tempDir, "server-output.log")
	completionPath := filepath.Join(tempDir, "gateway-complete")
	childPIDPath := filepath.Join(tempDir, "child.pid")
	wrapperPIDPath := filepath.Join(tempDir, "wrapper.pid")
	owner := exec.Command(os.Args[0], "-test.run=^TestChildSupervisorAbruptParentHelper$")
	owner.Env = append(os.Environ(),
		"RHIZA_SUPERVISOR_OWNER=1",
		"RHIZA_SUPERVISOR_OUTPUT="+outputPath,
		"RHIZA_SUPERVISOR_COMPLETION="+completionPath,
		"RHIZA_SUPERVISOR_CHILD_PID="+childPIDPath,
		"RHIZA_SUPERVISOR_WRAPPER_PID="+wrapperPIDPath,
	)
	if output, err := owner.CombinedOutput(); err != nil {
		t.Fatalf("run abrupt-parent helper: %v\n%s", err, output)
	}
	waitForFileText(t, completionPath, "gateway_reaped=true")
	assertRecordedProcessExited(t, childPIDPath)
	assertRecordedProcessExited(t, wrapperPIDPath)
}

func TestChildSupervisorEarlyChildExit(t *testing.T) {
	tempDir := t.TempDir()
	outputPath := filepath.Join(tempDir, "server-output.log")
	completionPath := filepath.Join(tempDir, "gateway-complete")
	childPIDPath := filepath.Join(tempDir, "child.pid")
	output, err := os.OpenFile(outputPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	env := append(os.Environ(),
		"RHIZA_SUPERVISOR_FAKE_CHILD=1",
		"RHIZA_SUPERVISOR_CHILD_PID="+childPIDPath,
		"RHIZA_SUPERVISOR_EXIT_EARLY=1",
	)
	cmd, writer, err := startSupervisedChild(outputPath, completionPath, []string{
		os.Args[0], "-test.run=^TestChildSupervisorFakeChildHelper$",
	}, env)
	_ = output.Close()
	if err != nil {
		t.Fatal(err)
	}
	waited := make(chan error, 1)
	go func() { waited <- cmd.Wait() }()
	select {
	case err := <-waited:
		if err == nil {
			t.Fatal("supervisor reported success after gateway exited before pipe EOF")
		}
	case <-time.After(3 * time.Second):
		_ = writer.Close()
		t.Fatal("supervisor did not propagate early gateway exit")
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close lifetime pipe after early exit: %v", err)
	}
	waitForFileText(t, completionPath, "gateway_reaped=true")
	outputBytes, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(outputBytes), "VERSITY_GATEWAY_EXITED_BEFORE_PIPE_EOF") {
		t.Fatalf("early gateway exit was not recorded: %s", outputBytes)
	}
	completion, err := os.ReadFile(completionPath)
	if err != nil || !strings.Contains(string(completion), "child_status=0") {
		t.Fatalf("completion marker does not record early child status: %s (%v)", completion, err)
	}
	assertRecordedProcessExited(t, childPIDPath)
}

func TestChildSupervisorFakeChildHelper(t *testing.T) {
	if os.Getenv("RHIZA_SUPERVISOR_FAKE_CHILD") != "1" {
		return
	}
	if err := os.WriteFile(os.Getenv("RHIZA_SUPERVISOR_CHILD_PID"), []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("RHIZA_SUPERVISOR_EXIT_EARLY") == "1" {
		return
	}
	if os.Getenv("RHIZA_SUPERVISOR_IGNORE_INT") == "1" {
		signal.Ignore(os.Interrupt)
		for {
			time.Sleep(time.Hour)
		}
	}
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	<-interrupt
}

func TestChildSupervisorAbruptParentHelper(t *testing.T) {
	if os.Getenv("RHIZA_SUPERVISOR_OWNER") != "1" {
		return
	}
	outputPath := os.Getenv("RHIZA_SUPERVISOR_OUTPUT")
	completionPath := os.Getenv("RHIZA_SUPERVISOR_COMPLETION")
	output, err := os.OpenFile(outputPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		os.Exit(2)
	}
	cmd, writer, err := startSupervisedChild(outputPath, completionPath, []string{
		os.Args[0], "-test.run=^TestChildSupervisorFakeChildHelper$",
	}, append(os.Environ(), "RHIZA_SUPERVISOR_FAKE_CHILD=1"))
	if err != nil {
		_ = output.Close()
		os.Exit(3)
	}
	if err := os.WriteFile(os.Getenv("RHIZA_SUPERVISOR_WRAPPER_PID"), []byte(strconv.Itoa(cmd.Process.Pid)), 0o600); err != nil {
		_ = writer.Close()
		_ = cmd.Wait()
		_ = output.Close()
		os.Exit(4)
	}
	_ = output.Close()
	// Deliberately do not close writer or wait: parent exit must deliver EOF.
	os.Exit(0)
}

func assertFakeChildSupervisorStops(t *testing.T, ignoreInterrupt bool) {
	t.Helper()
	tempDir := t.TempDir()
	outputPath := filepath.Join(tempDir, "server-output.log")
	childPIDPath := filepath.Join(tempDir, "child.pid")
	output, err := os.OpenFile(outputPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	ignore := "0"
	if ignoreInterrupt {
		ignore = "1"
	}
	env := append(os.Environ(),
		"RHIZA_SUPERVISOR_FAKE_CHILD=1",
		"RHIZA_SUPERVISOR_CHILD_PID="+childPIDPath,
		"RHIZA_SUPERVISOR_IGNORE_INT="+ignore,
	)
	completionPath := filepath.Join(tempDir, "gateway-complete")
	cmd, writer, err := startSupervisedChild(outputPath, completionPath, []string{
		os.Args[0], "-test.run=^TestChildSupervisorFakeChildHelper$",
	}, env)
	_ = output.Close()
	if err != nil {
		t.Fatal(err)
	}
	server := &Server{cmd: cmd, shutdownPipe: writer, done: make(chan struct{})}
	go func() {
		waitErr := cmd.Wait()
		server.mu.Lock()
		server.err = waitErr
		server.mu.Unlock()
		close(server.done)
	}()
	waitForFile(t, childPIDPath)
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	closeErr := server.Close(closeCtx)
	if ignoreInterrupt && closeErr == nil {
		t.Fatal("Close succeeded despite force-killing interrupt-ignoring child")
	}
	if !ignoreInterrupt && closeErr != nil {
		t.Fatalf("Close failed after graceful child exit: %v", closeErr)
	}
	if err := server.Close(closeCtx); (err == nil) != (closeErr == nil) {
		t.Fatalf("repeated Close changed result: first=%v second=%v", closeErr, err)
	}
	contents, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(contents), "VERSITY_CHILD_REAPED") {
		t.Fatalf("supervisor did not record child wait: %s", contents)
	}
	forced := strings.Contains(string(contents), "forced=1")
	if forced != ignoreInterrupt {
		t.Fatalf("forced cleanup=%t, want %t; output=%s", forced, ignoreInterrupt, contents)
	}
	waitForFileText(t, completionPath, "gateway_reaped=true")
	assertRecordedProcessExited(t, childPIDPath)
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}

func waitForFileText(t *testing.T, path, text string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		contents, err := os.ReadFile(path)
		if err == nil && strings.Contains(string(contents), text) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	contents, _ := os.ReadFile(path)
	t.Fatalf("timed out waiting for %q in %s: %s", text, path, contents)
}

func assertRecordedProcessExited(t *testing.T, path string) {
	t.Helper()
	waitForFile(t, path)
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(contents)))
	if err != nil || pid <= 0 {
		t.Fatalf("invalid recorded PID %q: %v", contents, err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		process, findErr := os.FindProcess(pid)
		if findErr != nil {
			return
		}
		if signalErr := process.Signal(syscall.Signal(0)); signalErr != nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("process %d recorded at %s is still alive", pid, path)
}
