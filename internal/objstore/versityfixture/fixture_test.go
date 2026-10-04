package versityfixture

import (
	"context"
	"errors"
	"fmt"
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
		assertFakeChildSupervisorStops(t, false, false, false)
	})
	t.Run("ignores interrupt then forced exit", func(t *testing.T) {
		assertFakeChildSupervisorStops(t, true, false, false)
	})
	t.Run("failed state probe cannot treat live child as exited", func(t *testing.T) {
		assertFakeChildSupervisorStops(t, true, true, false)
	})
	t.Run("deadline before failed probe completes still reaps child", func(t *testing.T) {
		assertFakeChildSupervisorStops(t, true, true, true)
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
	assertRecordedProcessExited(t, childPIDPath, false)
	// The owner deliberately exited without Wait. In containers without an init
	// reaper, the orphan wrapper can remain a zombie after it has completed.
	assertRecordedProcessExited(t, wrapperPIDPath, true)
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
	assertRecordedProcessExited(t, childPIDPath, false)
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
		writeFakeChildReady(t)
		for {
			time.Sleep(time.Hour)
		}
	}
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	writeFakeChildReady(t)
	<-interrupt
}

func writeFakeChildReady(t *testing.T) {
	t.Helper()
	if path := os.Getenv("RHIZA_SUPERVISOR_CHILD_READY"); path != "" {
		if err := os.WriteFile(path, []byte("ready\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
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

func assertFakeChildSupervisorStops(t *testing.T, ignoreInterrupt, failStateProbe, holdProbe bool) {
	t.Helper()
	tempDir := t.TempDir()
	outputPath := filepath.Join(tempDir, "server-output.log")
	childPIDPath := filepath.Join(tempDir, "child.pid")
	childReadyPath := filepath.Join(tempDir, "child-ready")
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
		"RHIZA_SUPERVISOR_CHILD_READY="+childReadyPath,
		"RHIZA_SUPERVISOR_IGNORE_INT="+ignore,
	)
	probePath := filepath.Join(tempDir, "ps-probe.log")
	probeEnteredPath := filepath.Join(tempDir, "ps-entered")
	probeReleasePath := filepath.Join(tempDir, "ps-release")
	if failStateProbe {
		fakeBin := filepath.Join(tempDir, "bin")
		if err := os.Mkdir(fakeBin, 0o700); err != nil {
			t.Fatal(err)
		}
		fakePS := "#!/bin/sh\nprintf 'called\\n' >> \"$RHIZA_SUPERVISOR_PS_PROBE_LOG\"\n"
		if holdProbe {
			fakePS += "if [ ! -f \"$RHIZA_SUPERVISOR_PS_RELEASE\" ] && [ ! -f \"$RHIZA_SUPERVISOR_PS_ENTERED\" ]; then\n" +
				"  printf 'entered\\n' > \"$RHIZA_SUPERVISOR_PS_ENTERED\" || exit 1\n" +
				"  waited=0\n" +
				"  while [ ! -f \"$RHIZA_SUPERVISOR_PS_RELEASE\" ] && [ \"$waited\" -lt 100 ]; do\n" +
				"    sleep 0.1\n" +
				"    waited=$((waited + 1))\n" +
				"  done\n" +
				"  if [ ! -f \"$RHIZA_SUPERVISOR_PS_RELEASE\" ]; then\n" +
				"    printf 'gate_timeout\\n' >> \"$RHIZA_SUPERVISOR_PS_PROBE_LOG\"\n" +
				"    exit 1\n" +
				"  fi\n" +
				"fi\n"
		}
		fakePS += "exit 1\n"
		if err := os.WriteFile(filepath.Join(fakeBin, "ps"), []byte(fakePS), 0o755); err != nil {
			t.Fatal(err)
		}
		env = append(env, "PATH="+fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
			"RHIZA_SUPERVISOR_PS_PROBE_LOG="+probePath)
		if holdProbe {
			env = append(env, "RHIZA_SUPERVISOR_PS_ENTERED="+probeEnteredPath,
				"RHIZA_SUPERVISOR_PS_RELEASE="+probeReleasePath)
		}
	}
	completionPath := filepath.Join(tempDir, "gateway-complete")
	cmd, writer, err := startSupervisedChild(outputPath, completionPath, []string{
		os.Args[0], "-test.run=^TestChildSupervisorFakeChildHelper$",
	}, env)
	_ = output.Close()
	if err != nil {
		t.Fatal(err)
	}
	server := &Server{cmd: cmd, shutdownPipe: writer, done: make(chan struct{})}
	var closeErr error
	var deferredReleaseErr error
	var beforeCleanupState string
	bounded := func(path string) string {
		contents, err := os.ReadFile(path)
		if err != nil {
			return err.Error()
		}
		if len(contents) > 256 {
			contents = contents[:256]
		}
		return string(contents)
	}
	snapshot := func() string {
		done := false
		select {
		case <-server.done:
			done = true
		default:
		}
		return fmt.Sprintf("firstClose=%v done=%t childPID=%q probes=%q release=%q output=%q completion=%q",
			closeErr, done, bounded(childPIDPath), bounded(probePath),
			bounded(probeReleasePath), bounded(outputPath), bounded(completionPath))
	}
	go func() {
		waitErr := cmd.Wait()
		server.mu.Lock()
		server.err = waitErr
		server.mu.Unlock()
		close(server.done)
	}()
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("supervisor failure state before cleanup: %s; deferred release error: %v; after cleanup: %s",
				beforeCleanupState, deferredReleaseErr, snapshot())
		}
	})
	t.Cleanup(func() {
		beforeCleanupState = snapshot()
		if holdProbe {
			deferredReleaseErr = os.WriteFile(probeReleasePath, []byte("release\n"), 0o600)
			if deferredReleaseErr != nil {
				t.Errorf("release held ps probe: %v", deferredReleaseErr)
			}
		}
		select {
		case <-server.done:
			return
		default:
		}
		if contents, err := os.ReadFile(childPIDPath); err == nil {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(contents))); err == nil && pid > 0 {
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
		}
		_ = writer.Close()
		_ = cmd.Process.Kill()
		select {
		case <-server.done:
		case <-time.After(time.Second):
		}
	})
	waitForFile(t, childReadyPath)
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if holdProbe {
		closeResult := make(chan error, 1)
		go func() { closeResult <- server.Close(closeCtx) }()
		waitForFile(t, probeEnteredPath)
		<-closeCtx.Done()
		select {
		case closeErr = <-closeResult:
		case <-time.After(time.Second):
			t.Fatal("Close did not return after caller deadline")
		}
		if !errors.Is(closeErr, context.DeadlineExceeded) {
			t.Fatalf("Close before probe release = %v, want caller deadline", closeErr)
		}
		select {
		case <-server.done:
			t.Fatal("supervisor completed before held state probe was released")
		default:
		}
		if contents, err := os.ReadFile(outputPath); err != nil || strings.Contains(string(contents), "VERSITY_CHILD_REAPED") {
			t.Fatalf("child wait recorded before probe release: %q (%v)", contents, err)
		}
		if _, err := os.Stat(completionPath); !os.IsNotExist(err) {
			t.Fatalf("completion marker existed before probe release: %v", err)
		}
		if err := os.WriteFile(probeReleasePath, []byte("release\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	} else {
		closeErr = server.Close(closeCtx)
	}
	select {
	case <-server.done:
	case <-time.After(5 * time.Second):
		t.Fatalf("supervisor did not finish: %s", snapshot())
	}
	terminalCtx, terminalCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer terminalCancel()
	terminalErr := server.Close(terminalCtx)
	if ignoreInterrupt && terminalErr == nil {
		t.Fatal("Close succeeded despite force-killing interrupt-ignoring child")
	}
	if !ignoreInterrupt && (closeErr != nil || terminalErr != nil) {
		t.Fatalf("Close failed after graceful child exit: first=%v terminal=%v", closeErr, terminalErr)
	}
	if err := server.Close(terminalCtx); (err == nil) != (terminalErr == nil) || (err != nil && err.Error() != terminalErr.Error()) {
		t.Fatalf("repeated Close changed terminal result: first=%v terminal=%v repeated=%v", closeErr, terminalErr, err)
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
	if failStateProbe {
		probes, err := os.ReadFile(probePath)
		if err != nil || len(probes) == 0 {
			t.Fatalf("failed ps probe was not exercised: %q (%v)", probes, err)
		}
		if strings.Contains(string(probes), "gate_timeout") {
			t.Fatalf("held ps probe timed out before release: %q", probes)
		}
	}
	waitForFileText(t, completionPath, "gateway_reaped=true")
	assertRecordedProcessExited(t, childPIDPath, false)
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

func assertRecordedProcessExited(t *testing.T, path string, allowOrphanZombie bool) {
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
		if allowOrphanZombie {
			// Linux kill(0) succeeds for an exited, unreaped orphan. Only the
			// wrapper in the abrupt-owner test may use this exception; child
			// processes still require the supervisor's actual wait evidence.
			if procStat, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat")); err == nil {
				if closeParen := strings.LastIndexByte(string(procStat), ')'); closeParen >= 0 {
					fields := strings.Fields(string(procStat[closeParen+1:]))
					if len(fields) > 0 && fields[0] == "Z" {
						return
					}
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("process %d recorded at %s is still running", pid, path)
}
