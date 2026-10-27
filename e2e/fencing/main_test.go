package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"gopkg.in/yaml.v3"
)

func TestWorkflowExcludesRawKindArtifacts(t *testing.T) {
	data, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		Jobs map[string]struct {
			Steps []struct {
				Uses string `yaml:"uses"`
				With struct {
					Path string `yaml:"path"`
				} `yaml:"with"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatal(err)
	}
	for job, root := range map[string]string{"operator-chaos": "operator-chaos-results", "automatic-operator-chaos": "automatic-chaos-results", "anchor-operator-chaos": "anchor-chaos-results"} {
		found := false
		for _, step := range workflow.Jobs[job].Steps {
			if !strings.HasPrefix(step.Uses, "actions/upload-artifact@") {
				continue
			}
			paths := strings.Split(strings.TrimSpace(step.With.Path), "\n")
			if len(paths) < 3 || strings.TrimSpace(paths[0]) != root {
				continue
			}
			excluded := map[string]bool{}
			for _, path := range paths[1:] {
				excluded[strings.TrimSpace(path)] = true
			}
			if excluded["!"+root+"/kind/**"] && excluded["!"+root+"/private-startup-diagnostics/**"] {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s upload must exclude the entire raw Kind subtree", job)
		}
	}
}

func validateWorkflowStepCommands(data []byte) error {
	var workflow struct {
		Jobs map[string]struct {
			Steps []struct {
				Run  string `yaml:"run"`
				Uses string `yaml:"uses"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		return err
	}
	if len(workflow.Jobs) == 0 {
		return fmt.Errorf("workflow has no jobs")
	}
	for jobName, job := range workflow.Jobs {
		for index, step := range job.Steps {
			hasRun, hasUses := strings.TrimSpace(step.Run) != "", strings.TrimSpace(step.Uses) != ""
			if hasRun == hasUses {
				return fmt.Errorf("job %s step %d must declare exactly one step-level run or uses", jobName, index)
			}
		}
	}
	return nil
}

func TestWorkflowStepCommandContractFixtures(t *testing.T) {
	for _, tc := range []struct {
		name     string
		workflow string
		wantErr  bool
	}{
		{
			name:     "env run is not a step command",
			workflow: "jobs:\n  broken:\n    steps:\n      - name: misplaced command\n        env:\n          run: echo misplaced\n",
			wantErr:  true,
		},
		{
			name:     "step-level run is valid",
			workflow: "jobs:\n  valid-run:\n    steps:\n      - run: echo valid\n",
		},
		{
			name:     "step-level uses is valid",
			workflow: "jobs:\n  valid-uses:\n    steps:\n      - uses: actions/checkout@v4\n",
		},
		{
			name:     "run and uses together are invalid",
			workflow: "jobs:\n  both:\n    steps:\n      - run: echo invalid\n        uses: actions/checkout@v4\n",
			wantErr:  true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateWorkflowStepCommands([]byte(tc.workflow))
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateWorkflowStepCommands() error=%v, wantErr=%t", err, tc.wantErr)
			}
		})
	}
}

func TestCIWorkflowStepsHaveExactlyOneCommand(t *testing.T) {
	data, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	if err := validateWorkflowStepCommands(data); err != nil {
		t.Fatalf("CI workflow step command contract: %v", err)
	}
}

func TestRunOwnedDeploymentScaleUsesUIDAndResourceVersionPreconditions(t *testing.T) {
	dir := t.TempDir()
	argsPath := filepath.Join(dir, "kubectl-args")
	kubectlPath := filepath.Join(dir, "kubectl")
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$FENCING_KUBECTL_ARGS\"\nexit 1\n"
	if err := os.WriteFile(kubectlPath, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	t.Setenv("FENCING_KUBECTL_ARGS", argsPath)
	owned := runOwnedOperator{Name: "rhiza-operator", UID: "deployment-uid", ResourceVersion: "resource-version", Replicas: 1}
	if err := owned.setReplicas(context.Background(), 0); err == nil {
		t.Fatal("failed Kubernetes precondition was treated as a successful scale")
	}
	args, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	joined := string(args)
	for _, required := range []string{"patch\ndeployment\nrhiza-operator", "metadata/uid", "deployment-uid", "metadata/resourceVersion", "resource-version", "spec/replicas"} {
		if !strings.Contains(joined, required) {
			t.Fatalf("fake kubectl did not receive required ownership precondition %q: %s", required, joined)
		}
	}
	if strings.Count(joined, "patch") != 1 {
		t.Fatalf("precondition failure issued more than the single owned patch: %s", joined)
	}
}

func TestGCSOrdinaryRecoveryRequiresFreshSameOrdinalVoter(t *testing.T) {
	source := gcsRecoverySource{ClusterID: "cluster", Namespace: "fixture", StatefulSetUID: "sts-uid", Targets: []operator.FenceTarget{
		{NodeID: "old-0", Pod: "rhiza-0", PodUID: "old-pod", WALIdentity: "old-wal"},
		{NodeID: "survivor-1", Pod: "rhiza-1", PodUID: "pod-1", WALIdentity: "wal-1"},
		{NodeID: "survivor-2", Pod: "rhiza-2", PodUID: "pod-2", WALIdentity: "wal-2"},
	}}
	status := network.MembershipStatus{NodeID: "new-0", ClusterID: "cluster", ConfigID: 8,
		Voters: []quepaxa.NodeID{"survivor-1", "survivor-2", "new-0"}, WALIdentity: "new-wal", Voting: true}
	baseline := network.MembershipStatus{NodeID: "survivor-1", ClusterID: "cluster", ConfigID: 7,
		Voters: []quepaxa.NodeID{"old-0", "survivor-1", "survivor-2"}, WALIdentity: "wal-1", Voting: true}
	if !gcsOrdinaryBaselineMembershipReady(source, source.Targets[1], baseline) {
		t.Fatal("rejected exact initial three-voter membership")
	}
	baseline.Voters = []quepaxa.NodeID{"old-0", "survivor-1", "unexpected"}
	if gcsOrdinaryBaselineMembershipReady(source, source.Targets[1], baseline) {
		t.Fatal("accepted an initial membership with an uncaptured voter")
	}
	if !gcsOrdinaryMembershipReady(source, source.Targets[0], 7, status) {
		t.Fatal("rejected fresh same-ordinal voter with both original survivors")
	}
	tests := []struct {
		name   string
		mutate func(*network.MembershipStatus)
	}{
		{"old logical identity", func(s *network.MembershipStatus) { s.NodeID = "old-0"; s.Voters[2] = "old-0" }},
		{"old WAL", func(s *network.MembershipStatus) { s.WALIdentity = "old-wal" }},
		{"old voter remains", func(s *network.MembershipStatus) { s.Voters[2] = "old-0" }},
		{"missing survivor", func(s *network.MembershipStatus) { s.Voters[0] = "newcomer" }},
		{"duplicate voter", func(s *network.MembershipStatus) { s.Voters[2] = "survivor-1" }},
		{"no configuration advance", func(s *network.MembershipStatus) { s.ConfigID = 7 }},
		{"pending membership", func(s *network.MembershipStatus) { s.Pending = true }},
		{"not yet voting", func(s *network.MembershipStatus) { s.Voting = false }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := status
			changed.Voters = append([]quepaxa.NodeID(nil), status.Voters...)
			test.mutate(&changed)
			if gcsOrdinaryMembershipReady(source, source.Targets[0], 7, changed) {
				t.Fatal("accepted stale or incomplete ordinary-recovery voter membership")
			}
		})
	}
}

func TestGCSOrdinaryRecoveryHasExplicitRunnerMode(t *testing.T) {
	mainSource, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	mainText := string(mainSource)
	start := strings.Index(mainText, `if os.Args[1] == "run-gcs-ha-ordinary-recovery" {`)
	if start < 0 {
		t.Fatal("dedicated ordinary-recovery command is missing")
	}
	end := strings.Index(mainText[start:], "\n\tif os.Args[1] == ")
	if end < 0 {
		t.Fatal("ordinary-recovery command block is not bounded")
	}
	block := mainText[start : start+end]
	if !strings.Contains(block, "fatal(configureGCSHA(ctx))") || !strings.Contains(block, "fatal(qualifyGCSOrdinaryRecovery(ctx))") {
		t.Fatal("ordinary-recovery command does not validate the GCS fixture before running")
	}
	runScript, err := os.ReadFile("run.sh")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(runScript), `FENCING_GCS_ORDINARY_RECOVERY:-no`) || !strings.Contains(string(runScript), `exec "$FENCING_E2E_BIN" run-gcs-ha-ordinary-recovery`) {
		t.Fatal("run.sh lacks the explicit ordinary-recovery mode dispatch")
	}
}

func TestCommandPreservesExecutionAndContextErrors(t *testing.T) {
	t.Run("canceled process preserves exit and context errors", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		readyPath := filepath.Join(t.TempDir(), "command-ready")
		t.Setenv("FENCING_TEST_COMMAND_READY", readyPath)
		result := make(chan error, 1)
		go func() {
			_, err := command(ctx, "sh", "-c", `printf ready > "$FENCING_TEST_COMMAND_READY"; exec sleep 30`)
			result <- err
		}()

		watchdog := time.NewTimer(5 * time.Second)
		defer watchdog.Stop()
		poll := time.NewTicker(10 * time.Millisecond)
		defer poll.Stop()
		ready := false
		for !ready {
			select {
			case err := <-result:
				t.Fatalf("subprocess exited before its readiness marker: %v", err)
			case <-poll.C:
				if _, err := os.Stat(readyPath); err == nil {
					ready = true
				}
			case <-watchdog.C:
				cancel()
				err := <-result
				t.Fatalf("subprocess did not reach its readiness marker before watchdog: %v", err)
			}
		}
		cancel()
		err := <-result
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("command error did not preserve the explicit cancellation cause: %v", err)
		}
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("command error did not preserve the subprocess exit error: %v", err)
		}
	})

	t.Run("live subprocess failure preserves exit error", func(t *testing.T) {
		_, err := command(context.Background(), "sh", "-c", "exit 7")
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) || exitErr.ExitCode() != 7 {
			t.Fatalf("command error did not preserve the live subprocess failure: %v", err)
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("live subprocess failure was misclassified as context cancellation: %v", err)
		}
	})
}

func TestForwardCloseReportsEveryCleanupFailureAndIsIdempotent(t *testing.T) {
	logFile, err := os.CreateTemp(t.TempDir(), "closed-port-forward-log-*")
	if err != nil {
		t.Fatal(err)
	}
	if err := logFile.Close(); err != nil {
		t.Fatal(err)
	}
	waitFailure := errors.New("injected wait failure")
	forward := &forward{cmd: &exec.Cmd{Process: &os.Process{Pid: -1}}, done: make(chan error, 1), log: logFile}
	forward.done <- waitFailure

	first := forward.close()
	if first == nil || !strings.Contains(first.Error(), "kill port-forward process") ||
		!errors.Is(first, waitFailure) || !strings.Contains(first.Error(), "close port-forward log file") {
		t.Fatalf("close did not report kill, wait, and log-close failures: %v", first)
	}
	if second := forward.close(); second != first {
		t.Fatalf("repeat close did not return the retained cleanup result: first=%v second=%v", first, second)
	}
}

func TestForwardCloseTreatsItsSuccessfulKillAsExpected(t *testing.T) {
	logFile, err := os.CreateTemp(t.TempDir(), "port-forward-log-*")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("sh", "-c", "sleep 30")
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		t.Fatal(err)
	}
	forward := &forward{cmd: cmd, done: make(chan error, 1), log: logFile}
	go func() { forward.done <- cmd.Wait() }()
	if err := forward.close(); err != nil {
		t.Fatalf("expected successful owned-process cleanup, got %v", err)
	}
}

func TestQualificationLedgerCleanupPreservesBucketAndForwardErrors(t *testing.T) {
	logFile, err := os.CreateTemp(t.TempDir(), "closed-qualification-forward-log-*")
	if err != nil {
		t.Fatal(err)
	}
	if err := logFile.Close(); err != nil {
		t.Fatal(err)
	}
	bucketErr := errors.New("injected bucket close failure")
	forwardErr := errors.New("injected port-forward wait failure")
	f := &forward{cmd: &exec.Cmd{Process: &os.Process{Pid: -1}}, done: make(chan error, 1), log: logFile}
	f.done <- forwardErr

	err = closeQualificationLedgerResources(func() error { return bucketErr }, f)
	if !errors.Is(err, bucketErr) || !errors.Is(err, forwardErr) ||
		!strings.Contains(err.Error(), "close canonical authority bucket") ||
		!strings.Contains(err.Error(), "kill port-forward process") ||
		!strings.Contains(err.Error(), "close port-forward log file") {
		t.Fatalf("qualification cleanup did not preserve bucket and forward errors: %v", err)
	}
}

func TestGCSMixedSelectorUsesOneDeadlineBoundedCommand(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	work, hard, err := gcsMixedDeadlines(now, "", "")
	if err != nil || !work.Equal(hard.Add(-gcsMixedDefaultCleanupReserve)) || !hard.Equal(now.Add(gcsMixedHardLimit)) {
		t.Fatalf("default mixed deadlines differ: work=%s hard=%s err=%v", work, hard, err)
	}
	for _, selector := range []struct {
		command  string
		scenario string
		want     bool
	}{
		{command: "run-gcs-ha-mixed-pending", want: true},
		{command: "run", scenario: gcsHAMixedPendingScenario, want: true},
		{command: "run", want: false},
		{command: "run-gcs-ha", scenario: gcsHAMixedPendingScenario, want: false},
	} {
		if got := isGCSMixedPendingSelected(selector.command, selector.scenario); got != selector.want {
			t.Errorf("selector command=%q scenario=%q: got %v, want %v", selector.command, selector.scenario, got, selector.want)
		}
	}
}

func TestGCSMixedDeadlinesRejectUnsafeContract(t *testing.T) {
	now := time.Now()
	valid := func(workOffset, hardOffset time.Duration) (string, string) {
		return strconv.FormatInt(now.Add(workOffset).Unix(), 10), strconv.FormatInt(now.Add(hardOffset).Unix(), 10)
	}
	validWork, validHard := valid(40*time.Minute, 50*time.Minute)
	expiredWork, expiredHard := valid(-time.Second, time.Minute)
	wrongOrderWork, wrongOrderHard := valid(2*time.Minute, time.Minute)
	overLimitWork, overLimitHard := valid(50*time.Minute, 56*time.Minute)
	for name, pair := range map[string][2]string{
		"partial":         {validWork, ""},
		"malformed":       {"nope", validHard},
		"expired":         {expiredWork, expiredHard},
		"wrong-order":     {wrongOrderWork, wrongOrderHard},
		"over-hard-limit": {overLimitWork, overLimitHard},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := gcsMixedDeadlines(now, pair[0], pair[1]); err == nil {
				t.Fatal("unsafe deadline contract was accepted")
			}
		})
	}
}

func TestGCSMixedCleanupDeadlineSurvivesWorkCancellationButNotHardHorizon(t *testing.T) {
	hard := time.Now().Add(100 * time.Millisecond)
	workCtx, cancelWork := context.WithCancel(context.Background())
	workCtx = context.WithValue(workCtx, gcsMixedHardDeadlineKey{}, hard)
	cancelWork()
	cleanupCtx, cancelCleanup := gcsMixedCleanupContext(workCtx, time.Minute)
	defer cancelCleanup()
	if cleanupCtx.Err() != nil {
		t.Fatalf("cleanup inherited work cancellation: %v", cleanupCtx.Err())
	}
	deadline, ok := cleanupCtx.Deadline()
	if !ok || deadline.After(hard) {
		t.Fatalf("cleanup deadline exceeds hard horizon: deadline=%s hard=%s", deadline, hard)
	}
	select {
	case <-cleanupCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("cleanup context did not stop at the hard horizon")
	}
	if !errors.Is(cleanupCtx.Err(), context.DeadlineExceeded) {
		t.Fatalf("cleanup ended for the wrong reason: %v", cleanupCtx.Err())
	}
}

func TestGCSMixedCommandSignalSubprocess(t *testing.T) {
	if os.Getenv("FENCING_E2E_MIXED_SIGNAL_CHILD") != "1" {
		return
	}
	command := os.Getenv("FENCING_E2E_MIXED_SIGNAL_SELECTOR")
	scenario := os.Getenv(gcsHAScenarioEnv)
	if !isGCSMixedPendingSelected(command, scenario) {
		t.Fatalf("subprocess selector was not routed to mixed command: command=%q scenario=%q", command, scenario)
	}
	readyPath := os.Getenv("FENCING_E2E_MIXED_SIGNAL_READY")
	cleanupPath := os.Getenv("FENCING_E2E_MIXED_SIGNAL_CLEANUP")
	if readyPath == "" || cleanupPath == "" {
		t.Fatal("signal subprocess marker paths are required")
	}
	err := runGCSMixedPendingCommand(func(context.Context) error { return nil }, func(ctx context.Context) (resultErr error) {
		defer func() {
			cleanupErr := os.WriteFile(cleanupPath, []byte("deferred-cleanup\n"), 0600)
			resultErr = errors.Join(resultErr, cleanupErr)
			if os.Getenv("FENCING_E2E_MIXED_SIGNAL_CLEANUP_ERROR") == "1" {
				resultErr = errors.Join(resultErr, errors.New("injected deferred cleanup failure"))
			}
		}()
		if err := os.WriteFile(readyPath, []byte("cleanup-installed\n"), 0600); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	})
	if err == nil {
		t.Fatal("signal cancellation returned success")
	}
	t.Fatalf("mixed subprocess canceled and completed deferred cleanup: %v", err)
}

func TestGCSMixedSelectorsHandleSIGINTAndSIGTERMThroughDeferredCleanup(t *testing.T) {
	for _, tc := range []struct {
		name     string
		selector string
		signal   os.Signal
	}{
		{name: "explicit-sigint", selector: "run-gcs-ha-mixed-pending", signal: os.Interrupt},
		{name: "explicit-sigterm", selector: "run-gcs-ha-mixed-pending", signal: syscall.SIGTERM},
		{name: "environment-sigint", selector: "run", signal: os.Interrupt},
		{name: "environment-sigterm", selector: "run", signal: syscall.SIGTERM},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			readyPath := filepath.Join(dir, "ready")
			cleanupPath := filepath.Join(dir, "cleanup")
			cmd := exec.Command(os.Args[0], "-test.run=^TestGCSMixedCommandSignalSubprocess$")
			for _, entry := range os.Environ() {
				if strings.HasPrefix(entry, "FENCING_E2E_MIXED_SIGNAL_") || strings.HasPrefix(entry, gcsHAScenarioEnv+"=") ||
					strings.HasPrefix(entry, gcsMixedWorkDeadlineEnv+"=") || strings.HasPrefix(entry, gcsMixedHardDeadlineEnv+"=") {
					continue
				}
				cmd.Env = append(cmd.Env, entry)
			}
			cmd.Env = append(cmd.Env,
				"FENCING_E2E_MIXED_SIGNAL_CHILD=1",
				"FENCING_E2E_MIXED_SIGNAL_SELECTOR="+tc.selector,
				"FENCING_E2E_MIXED_SIGNAL_READY="+readyPath,
				"FENCING_E2E_MIXED_SIGNAL_CLEANUP="+cleanupPath,
			)
			if tc.name == "explicit-sigterm" {
				cmd.Env = append(cmd.Env, "FENCING_E2E_MIXED_SIGNAL_CLEANUP_ERROR=1")
			}
			if tc.selector == "run" {
				cmd.Env = append(cmd.Env, gcsHAScenarioEnv+"="+gcsHAMixedPendingScenario)
			} else {
				cmd.Env = append(cmd.Env, gcsHAScenarioEnv+"=")
			}
			output := &bytes.Buffer{}
			cmd.Stdout, cmd.Stderr = output, output
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			waitForFile := func(path string) bool {
				deadline := time.Now().Add(5 * time.Second)
				for time.Now().Before(deadline) {
					if _, err := os.Stat(path); err == nil {
						return true
					}
					time.Sleep(10 * time.Millisecond)
				}
				return false
			}
			if !waitForFile(readyPath) {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
				t.Fatalf("subprocess did not reach configured readiness: %s", output.String())
			}
			if err := cmd.Process.Signal(tc.signal); err != nil {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
				t.Fatal(err)
			}
			waited := make(chan error, 1)
			go func() { waited <- cmd.Wait() }()
			select {
			case err := <-waited:
				if err == nil {
					t.Fatalf("canceled selector exited successfully; output=%s", output.String())
				}
			case <-time.After(5 * time.Second):
				_ = cmd.Process.Kill()
				<-waited
				t.Fatalf("subprocess did not finish after cancellation; output=%s", output.String())
			}
			if _, err := os.Stat(cleanupPath); err != nil {
				t.Fatalf("deferred cleanup did not complete: %v; output=%s", err, output.String())
			}
			if !strings.Contains(output.String(), "context canceled") {
				t.Fatalf("cancellation cause was not reported: %s", output.String())
			}
			if tc.name == "explicit-sigterm" && !strings.Contains(output.String(), "injected deferred cleanup failure") {
				t.Fatalf("cleanup error was not preserved: %s", output.String())
			}
		})
	}
}

func TestOnlineVoterDiagnosticsProjectionIsBoundedAndSanitized(t *testing.T) {
	privateCanary := "private-namespace-token-123"
	input := obj{
		"operationID":                    privateCanary,
		"recoveryFound":                  true,
		"podsQuerySucceeded":             true,
		"eventsQuerySucceeded":           true,
		"recoveryQuerySucceeded":         true,
		"recovery":                       obj{"metadata": obj{"name": privateCanary}, "status": obj{"phase": "Blocked", "stage": "Adding", "membership": obj{"phase": "Adding"}}},
		"parentQuerySucceeded":           true,
		"parentRecoveryMatchesOperation": true,
		"parent":                         obj{"status": obj{"phase": "Blocked", "activeClusterID": privateCanary}},
		"secretObservations":             []any{obj{"name": privateCanary, "uid": "123e4567-e89b-12d3-a456-426614174000", "found": true, "querySucceeded": true, "immutable": true, "ownerMatchesOperation": true, "keysPresent": true}},
		"secretObservationsTruncated":    false,
		"membershipObservation":          obj{"checked": true, "available": true, "authRejected": false, "nodeMatchesAdd": true, "walMatchesAdd": true, "clusterMatchesActive": true, "voting": true, "pending": false, "status": obj{"nodeID": privateCanary, "clusterID": privateCanary, "walIdentity": privateCanary}},
		"learner": obj{
			"metadata": obj{"name": privateCanary, "uid": privateCanary},
			"status": obj{"phase": "Running", "podIP": "10.0.0.1", "conditions": []any{obj{"type": "Ready", "status": "False", "reason": "ContainersNotReady", "message": privateCanary}}, "containerStatuses": []any{
				obj{"ready": false, "restartCount": float64(7), "state": obj{"waiting": obj{"reason": "CrashLoopBackOff"}}, "lastState": obj{"terminated": obj{"reason": "Error", "exitCode": float64(23)}}},
			}},
		},
		"events":            []any{obj{"type": "Warning", "reason": "BackOff", "message": "Back-off restarting failed container private-namespace-token-123", "count": float64(15)}},
		"faultTargetPodUID": privateCanary, "faultTargetIP": "10.0.0.2", "learnerIP": "10.0.0.1",
	}
	data, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	public, err := safeJSON("online-voter-diagnostics.json", data)
	if err != nil {
		t.Fatalf("strict online-voter diagnostic projection failed: %v", err)
	}
	if bytes.Contains(public, []byte(privateCanary)) || bytes.Contains(public, []byte("10.0.0.")) || bytes.Contains(public, []byte("back-off restarting failed container")) {
		t.Fatalf("public online-voter diagnostics leaked a private identity, address or raw message: %s", public)
	}
	var got onlineVoterDiagnosticsView
	if err := json.Unmarshal(public, &got); err != nil {
		t.Fatal(err)
	}
	if !got.RecoveryFound || got.RecoveryPhase != "Blocked" || got.MembershipPhase != "Adding" || got.ParentPhase != "Blocked" || !got.ParentRecoveryMatchesOperation ||
		!got.LearnerFound || got.LearnerPhase != "Running" || len(got.Conditions) != 1 || got.Conditions[0].Type != "Ready" || got.Conditions[0].Status != "False" || got.Conditions[0].Reason != "Other" || len(got.Containers) != 1 || got.Containers[0].Ready ||
		got.SecretReferences != 1 || got.SecretsFound != 1 || got.ImmutableSecrets != 1 || !got.SecretKeysPresent || len(got.Secrets) != 1 || got.Secrets[0].UID != "123e4567-e89b-12d3-a456-426614174000" || !got.Secrets[0].Immutable || !got.Secrets[0].KeysPresent ||
		!got.MembershipChecked || !got.MembershipAvailable || got.MembershipAuthRejected || !got.MembershipNodeMatchesAdd || !got.MembershipWALMatchesAdd || !got.MembershipClusterMatchesActive || !got.MembershipVoting || got.MembershipPending ||
		got.Containers[0].RestartCount != 7 || got.Containers[0].WaitingReason != "CrashLoopBackOff" ||
		got.Containers[0].LastTerminatedReason != "Error" || got.Containers[0].LastExitCode == nil || *got.Containers[0].LastExitCode != 23 ||
		len(got.Events) != 1 || got.Events[0].Reason != "BackOff" || got.Events[0].Message != "container restart backoff" || got.Events[0].Count != 15 {
		t.Fatalf("public online-voter diagnostics omitted bounded learner failure state: %+v", got)
	}
	if !got.LearnerMatchesFaultTargetUID || got.LearnerMatchesFaultTargetIP {
		t.Fatalf("learner identity/IP reuse comparison was not reduced to safe booleans: %+v", got)
	}
	events := make([]any, 150)
	for i := range events {
		events[i] = obj{"type": "Warning", "reason": "Unhealthy", "message": "Readiness probe failed: connection refused; private=secret", "count": float64(i + 1)}
	}
	input["events"] = events
	large, _ := json.Marshal(input)
	bounded, err := publicOnlineVoterDiagnostics(large)
	if err != nil {
		t.Fatalf("large event set discarded otherwise useful diagnostics: %v", err)
	}
	if err := json.Unmarshal(bounded, &got); err != nil || len(got.Events) != 30 || got.Events[29].Message != "probe connection refused" {
		t.Fatalf("public diagnostic event cut was not bounded to the newest usable causes: %d (%v)", len(got.Events), err)
	}
	input["unapproved"] = privateCanary
	input["recoveryFound"] = "not-a-boolean"
	data, _ = json.Marshal(input)
	if _, err := safeJSON("online-voter-diagnostics.json", data); err == nil {
		t.Fatal("accepted malformed online-voter diagnostic input")
	}
}

func TestOnlineLearnerEventMessagesAreClassifiedWithoutPublishingRawText(t *testing.T) {
	for _, test := range []struct{ message, want string }{
		{"Readiness probe failed: Get https://private.example: connection refused; token=private-secret", "probe connection refused"},
		{"Readiness probe failed: HTTP probe failed with statuscode: 503 private response", "HTTP probe failed, status 503"},
		{"Liveness probe failed: context deadline exceeded private detail", "probe deadline exceeded"},
		{"Readiness probe failed: private detail", "readiness probe failed"},
	} {
		got := classifyLearnerEventMessage("Unhealthy", test.message)
		if got != test.want || strings.Contains(got, "private") || strings.Contains(got, "token") {
			t.Fatalf("unsafe or imprecise sanitized probe message %q: %q", test.message, got)
		}
	}
	stages, _ := classifyVoterStartupStages([]byte("2026/10/24 12:00:00 fencing: supervisor returned HTTP 403\n2026/10/24 12:00:01 open rhiza: connect fencing startup gate: fencing: supervisor returned HTTP 403\n"))
	if !contains(stages, "supervisor/authority_rejected_403") || !contains(stages, "startup_grant/rejected_403") {
		t.Fatalf("supervisor grant rejection status was not retained safely: %v", stages)
	}
}

func TestOnlineVoterDiagnosticsArtifactKeepsOnlyUIDBoundAllowlistedProjection(t *testing.T) {
	const learnerUID = "learner-uid-exact"
	events := []any{
		obj{"involvedObject": obj{"name": "learner", "uid": learnerUID}, "reason": "BackOff"},
		obj{"involvedObject": obj{"name": "learner", "uid": "stale-learner-uid"}, "reason": "Unhealthy"},
	}
	matched := eventsForLearnerUID(events, learnerUID)
	if len(matched) != 1 || nested(matched[0], "reason") != "BackOff" || len(eventsForLearnerUID(events, "")) != 0 {
		t.Fatal("online learner diagnostics admitted a stale same-name Pod event")
	}
	raw := t.TempDir()
	write := func(name string, data []byte) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(raw, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("actual-exit.txt", []byte("1\n"))
	write("source-head.txt", []byte(strings.Repeat("a", 40)+"\n"))
	write("image-ids.txt", []byte("ghcr.io/example/app@sha256:"+strings.Repeat("b", 64)+"\n"))
	privateDir := filepath.Join(raw, "private-online-voter-diagnostics")
	if err := os.Mkdir(privateDir, 0700); err != nil {
		t.Fatal(err)
	}
	privateJSON := []byte(`{"recoveryFound":false,"podsQuerySucceeded":true,"eventsQuerySucceeded":true,"recoveryQuerySucceeded":true,"parentQuerySucceeded":true,"secretObservationsTruncated":false,"parent":{"status":{"phase":"Blocked"}},"learner":{"metadata":{"uid":"` + learnerUID + `"},"status":{"phase":"Running","podIP":"10.0.0.1","containerStatuses":[{"ready":false,"restartCount":2,"lastState":{"terminated":{"reason":"Error","exitCode":9}}}]}},"events":[{"type":"Warning","reason":"Unhealthy","message":"Readiness probe failed for ` + learnerUID + `","count":3}],"faultTargetPodUID":"private","faultTargetIP":"10.0.0.1","learnerIP":"10.0.0.1"}`)
	if err := os.WriteFile(filepath.Join(privateDir, "online-voter-diagnostics.json"), privateJSON, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(privateDir, "learner-current-rhiza.log"), []byte("open learner: fatal private-namespace-token-123\n2026/10/24 12:00:00 fork/exec /usr/local/bin/rhiza: private-path-no-such-file\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(privateDir, "learner-previous-rhiza.log"), []byte("configure learner: private-namespace-token-123\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FENCING_E2E_OUT", raw)
	if err := safeArtifacts(raw); err != nil {
		t.Fatalf("online-voter failure artifact projection failed: %v", err)
	}
	public, err := os.ReadFile(filepath.Join(raw, "public", "online-voter-diagnostics.json"))
	if err != nil || bytes.Contains(public, []byte(learnerUID)) || bytes.Contains(public, []byte("10.0.0.1")) || bytes.Contains(public, []byte("private-namespace-token-123")) || bytes.Contains(public, []byte("open learner:")) {
		t.Fatalf("public online-voter artifact leaked identity, address or raw logs: %s (%v)", public, err)
	}
	var projected onlineVoterDiagnosticsView
	if err := json.Unmarshal(public, &projected); err != nil || len(projected.Logs) != 2 || projected.Logs[0].SHA256 == "" || projected.Logs[1].SHA256 == "" {
		t.Fatalf("sanitized current/previous learner log evidence was not preserved: %+v (%v)", projected.Logs, err)
	}
	knownFailures := map[string]bool{}
	for _, log := range projected.Logs {
		for _, failure := range log.Failures {
			knownFailures[failure] = true
		}
	}
	if !knownFailures["replica_open_failed"] || !knownFailures["role_config_failed"] || !knownFailures["fatal_error"] || !knownFailures["supervisor_child_exec_failed"] {
		t.Fatalf("sanitized startup/fatal classifications were not preserved: %+v", projected.Logs)
	}
	if !contains(projected.Logs[0].Stages, "supervisor/child_exec_failed") || bytes.Contains(public, []byte("/usr/local/bin/rhiza")) {
		t.Fatalf("supervisor failure stage was lost or raw executable path leaked: %+v", projected.Logs[0])
	}
	if _, err := os.Stat(filepath.Join(raw, "public", "private-online-voter-diagnostics")); !os.IsNotExist(err) {
		t.Fatalf("private online-voter diagnostics were included publicly: %v", err)
	}
	if !projected.PodsQuerySucceeded || !projected.EventsQuerySucceeded || !projected.RecoveryQuerySucceeded {
		t.Fatalf("private production query outcomes did not reach the public projection: %+v", projected)
	}
}

func TestBootstrapDiagnosticCapturesExactPodLogsBeforeOptionalProbes(t *testing.T) {
	const podUID = "private-exact-pod-uid"
	const rootUID = "private-exact-root-uid"
	const stsUID = "private-exact-sts-uid"
	logBody := "private-short-startup-canary-" + strings.Repeat("x", 59-len("private-short-startup-canary-"))
	if len(logBody) != 59 {
		t.Fatalf("test log length = %d, want 59", len(logBody))
	}
	for _, test := range []struct {
		name                      string
		replace                   string
		restarts                  int
		wantErr                   bool
		wantCurrent, wantPrevious bool
	}{
		{name: "stable identity", restarts: 1, wantCurrent: true, wantPrevious: true},
		{name: "current replaced during log read", replace: "current", restarts: 1, wantErr: true},
		{name: "previous replaced during log read", replace: "previous", restarts: 1, wantErr: true, wantCurrent: true},
		{name: "current container replaced during log read", replace: "current-container", restarts: 1, wantErr: true},
		{name: "previous container replaced during log read", replace: "previous-container", restarts: 1, wantErr: true, wantCurrent: true},
		{name: "current container restarted during log read", replace: "current-restart", restarts: 1, wantErr: true},
		{name: "previous container restarted during log read", replace: "previous-restart", restarts: 1, wantErr: true, wantCurrent: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			bin := t.TempDir()
			callsPath := filepath.Join(t.TempDir(), "calls.log")
			podGetCount := filepath.Join(t.TempDir(), "pod-get-count")
			script := `#!/bin/sh
printf '%s\n' "$*" >> "$CALLS"
if [ "$5" = logs ]; then printf '%s' "$LOG_BODY"; exit 0; fi
if [ "$5" = get ]; then
  case "$6" in
    pods) printf '%s' '{"items":[{"metadata":{"name":"rhiza-0","uid":"` + podUID + `","ownerReferences":[{"kind":"StatefulSet","name":"rhiza","uid":"` + stsUID + `"}]},"spec":{"containers":[{"name":"rhiza"}]},"status":{"phase":"Pending","containerStatuses":[{"name":"rhiza","containerID":"containerd://runtime-container-id","restartCount":'"$RESTARTS"',"ready":false}]}}]}';;
    pod)
      count=$(cat "$POD_GET_COUNT" 2>/dev/null || printf '0')
      count=$((count + 1))
      printf '%s' "$count" > "$POD_GET_COUNT"
      uid='` + podUID + `'
      if [ "$REPLACE_ON" = current ] && [ "$count" -ge 2 ]; then uid='replacement-pod-uid'; fi
      if [ "$REPLACE_ON" = previous ] && [ "$count" -ge 4 ]; then uid='replacement-pod-uid'; fi
      cid='containerd://runtime-container-id'
      restart="$RESTARTS"
      if [ "$REPLACE_ON" = current-container ] && [ "$count" -ge 2 ]; then cid='containerd://replacement-container-id'; fi
      if [ "$REPLACE_ON" = previous-container ] && [ "$count" -ge 4 ]; then cid='containerd://replacement-container-id'; fi
      if [ "$REPLACE_ON" = current-restart ] && [ "$count" -ge 2 ]; then restart=$((restart + 1)); fi
      if [ "$REPLACE_ON" = previous-restart ] && [ "$count" -ge 4 ]; then restart=$((restart + 1)); fi
      printf '{"metadata":{"name":"rhiza-0","uid":"%s"},"status":{"containerStatuses":[{"name":"rhiza","containerID":"%s","restartCount":%s}]}}' "$uid" "$cid" "$restart";;
    rhizacluster) printf '%s' '{"metadata":{"uid":"` + rootUID + `","generation":7},"status":{"phase":"Degraded","observedGeneration":6,"message":"private root detail"}}';;
    statefulset) printf '%s' '{"metadata":{"uid":"` + stsUID + `","generation":4,"ownerReferences":[{"kind":"RhizaCluster","uid":"` + rootUID + `"}]},"spec":{"template":{"spec":{"containers":[{"name":"rhiza","command":["/usr/local/bin/rhiza-fencer"],"args":["supervise","--","/bin/sh","-ec","if [ -n \"${RHIZA_LEARNER:-}\" ]; then while [ ! -f /data/.chaos-start ]; do sleep 0.2; done; fi; exec /usr/local/bin/rhiza-entrypoint"],"ports":[{"name":"http","containerPort":8080}]}]}}},"status":{"observedGeneration":4,"readyReplicas":0}}';;
    events|rhizarecoveries) printf '%s' '{"items":[]}';;
    *) exit 2;;
  esac
  exit 0
fi
exit 2
`
			kubectlPath := filepath.Join(bin, "kubectl")
			if err := os.WriteFile(kubectlPath, []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
			t.Setenv("CALLS", callsPath)
			t.Setenv("POD_GET_COUNT", podGetCount)
			t.Setenv("REPLACE_ON", test.replace)
			t.Setenv("RESTARTS", fmt.Sprint(test.restarts))
			t.Setenv("LOG_BODY", logBody)
			out := t.TempDir()
			captureErr := captureVoterStartupDiagnostics("", nil, out, "bootstrap")
			if (captureErr != nil) != test.wantErr {
				t.Fatalf("capture error = %v, wantErr %t", captureErr, test.wantErr)
			}
			privateDir := filepath.Join(out, "private-online-voter-diagnostics")
			data, err := os.ReadFile(filepath.Join(privateDir, "online-voter-diagnostics.json"))
			if err != nil {
				t.Fatal(err)
			}
			var snapshot obj
			if err := json.Unmarshal(data, &snapshot); err != nil {
				t.Fatal(err)
			}
			if snapshot["operationID"] != "" || snapshot["mode"] != "bootstrap" || nested(snapshot, "learner", "metadata", "uid") != podUID || nested(snapshot, "capture", "root", "metadata", "uid") != rootUID || nested(snapshot, "capture", "statefulSet", "metadata", "uid") != stsUID || nested(snapshot, "capture", "podIdentityBound") != true {
				t.Fatalf("bootstrap capture invented an operation or lost exact identity: %+v", snapshot)
			}
			_, currentErr := os.ReadFile(filepath.Join(privateDir, "learner-current-rhiza.log"))
			_, previousErr := os.ReadFile(filepath.Join(privateDir, "learner-previous-rhiza.log"))
			if (currentErr == nil) != test.wantCurrent || (previousErr == nil) != test.wantPrevious {
				t.Fatalf("trusted logs current=%t previous=%t, want current=%t previous=%t", currentErr == nil, previousErr == nil, test.wantCurrent, test.wantPrevious)
			}
			calls, err := os.ReadFile(callsPath)
			if err != nil {
				t.Fatal(err)
			}
			order := string(calls)
			firstLog := strings.Index(order, " logs rhiza-0 -c rhiza --tail=100 --limit-bytes=32768")
			rootQuery := strings.Index(order, " get rhizacluster rhiza")
			stsQuery := strings.Index(order, " get statefulset rhiza")
			eventQuery := strings.Index(order, " get events")
			if firstLog < 0 || rootQuery < 0 || stsQuery < 0 || eventQuery < 0 || firstLog > rootQuery || firstLog > stsQuery || firstLog > eventQuery {
				t.Fatalf("log capture was not first, before optional probes: %s", order)
			}
			publicBytes, err := projectOnlineVoterDiagnostics(out, data)
			if err != nil || bytes.Contains(publicBytes, []byte("private-short-startup-canary")) || bytes.Contains(publicBytes, []byte(podUID)) {
				t.Fatalf("public projection leaked the short unknown log or exact Pod UID: %s (%v)", publicBytes, err)
			}
			var public onlineVoterDiagnosticsView
			if err := json.Unmarshal(publicBytes, &public); err != nil {
				t.Fatal(err)
			}
			if public.RootPhase != "Degraded" || public.RootGeneration != 7 || public.RootObservedGeneration != 6 || !public.StatefulSetOwnedByRoot || !public.StatefulSetSupervisedLaunch || !public.LearnerIdentityBound || len(public.Logs) != 2 {
				t.Fatalf("bounded bootstrap identity/status projection incomplete: %+v", public)
			}
			if len(public.Logs[0].Stages) != 0 {
				t.Fatalf("unparsed 59-byte canary was labeled as an attested startup stage: %+v", public.Logs[0])
			}
			if public.Logs[0].Captured != test.wantCurrent || public.Logs[1].Captured != test.wantPrevious ||
				(test.wantCurrent && (public.Logs[0].Bytes != 59 || public.Logs[0].SHA256 == "" || public.Logs[0].Truncated)) ||
				(test.wantPrevious && (public.Logs[1].Bytes != 59 || public.Logs[1].SHA256 == "" || public.Logs[1].Truncated)) {
				t.Fatalf("capture completeness metadata mismatched trusted files: %+v", public.Logs)
			}
			if !test.wantCurrent && public.Logs[0].CaptureStatus != "identity_changed" || !test.wantPrevious && public.Logs[1].CaptureStatus != "identity_changed" {
				t.Fatalf("replacement Pod logs were not discarded as identity changes: %+v", public.Logs)
			}
		})
	}
}

func TestSupervisedLaunchShapeRecognizesOnlyKnownLaunchCommands(t *testing.T) {
	const fixtureGate = `if [ -n "${RHIZA_LEARNER:-}" ]; then while [ ! -f /data/.chaos-start ]; do sleep 0.2; done; fi; exec /usr/local/bin/rhiza-entrypoint`
	for _, test := range []struct {
		name string
		args []any
		want string
	}{
		{name: "direct", args: []any{"supervise", "--", "/usr/local/bin/rhiza"}, want: "expected"},
		{name: "fixture gate", args: []any{"supervise", "--", "/bin/sh", "-ec", fixtureGate}, want: "expected"},
		{name: "unrecognized shell payload", args: []any{"supervise", "--", "/bin/sh", "-ec", "exec /usr/local/bin/rhiza"}, want: "mismatch"},
	} {
		t.Run(test.name, func(t *testing.T) {
			container := obj{"command": []any{"/usr/local/bin/rhiza-fencer"}, "args": test.args}
			if got := supervisedLaunchShape(container); got != test.want {
				t.Fatalf("supervised launch shape = %q, want %q", got, test.want)
			}
		})
	}
}

func TestDiagnosticErrorCodeSeparatesMissingAPIAndContextFailures(t *testing.T) {
	for _, test := range []struct {
		name     string
		ctx      context.Context
		err      error
		previous bool
		want     string
	}{
		{name: "previous missing", ctx: context.Background(), err: errors.New("previous terminated container rhiza not found"), previous: true, want: "previous_missing"},
		{name: "API not found", ctx: context.Background(), err: errors.New("Error from server (NotFound)"), want: "api_not_found"},
		{name: "command failure", ctx: context.Background(), err: errors.New("private diagnostic text"), want: "command_failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := diagnosticErrorCode(test.ctx, test.err, test.previous); got != test.want {
				t.Fatalf("capture classification = %q, want %q", got, test.want)
			}
		})
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if got := diagnosticErrorCode(canceled, errors.New("private text"), false); got != "canceled" {
		t.Fatalf("canceled capture classification = %q", got)
	}
	deadline, cancelDeadline := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelDeadline()
	if got := diagnosticErrorCode(deadline, errors.New("private text"), false); got != "deadline_exceeded" {
		t.Fatalf("deadline capture classification = %q", got)
	}
}

func TestWorkflowPublishesOnlyPreKindFailureReceiptWhenQualificationIsSkipped(t *testing.T) {
	data, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	type step struct {
		ID   string `yaml:"id"`
		Name string `yaml:"name"`
		If   string `yaml:"if"`
		Run  string `yaml:"run"`
		Uses string `yaml:"uses"`
		With struct {
			Path string `yaml:"path"`
		} `yaml:"with"`
	}
	var workflow struct {
		Jobs map[string]struct {
			Steps []step `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatal(err)
	}
	steps := workflow.Jobs["cooperative-fencing"].Steps
	var qualify, prepare, upload *step
	for i := range steps {
		switch steps[i].ID {
		case "qualify":
			qualify = &steps[i]
		case "fencing_public":
			prepare = &steps[i]
		}
		if strings.HasPrefix(steps[i].Uses, "actions/upload-artifact@") {
			upload = &steps[i]
		}
	}
	if qualify == nil || qualify.Name != "Qualify shipped runtime agent and cooperative executor in disposable Kubernetes" {
		t.Fatal("qualification step must have a stable id for skip handling")
	}
	if prepare == nil || prepare.If != "always()" {
		t.Fatal("public evidence preparation must still run after a pre-kind failure")
	}
	branch, attempted, ok := strings.Cut(prepare.Run, "else\n")
	if !ok || !strings.Contains(branch, "steps.qualify.outcome") || !strings.Contains(branch, "qualification-not-started.json") ||
		!strings.Contains(branch, `"stage":"pre-kind"`) || !strings.Contains(branch, "priorStepOutcome") ||
		strings.Contains(branch, "actual-exit.txt") || strings.Contains(branch, "safe-artifacts") {
		t.Fatal("skipped qualification must emit only a fixed pre-kind receipt, never a fabricated driver exit or raw fallback")
	}
	if !strings.Contains(attempted, `"$RUNNER_TEMP/fencing-e2e" safe-artifacts`) || strings.Contains(attempted, "qualification-not-started.json") {
		t.Fatal("attempted qualification must continue through the strict allowlist projection")
	}
	if upload == nil || strings.TrimSpace(upload.With.Path) != "cooperative-fencing-results/public" {
		t.Fatal("workflow may upload only the sanitized public evidence directory")
	}
}

func TestGCSAckLossQualificationHasDedicatedCommand(t *testing.T) {
	data, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	mainSource := string(data)
	start := strings.Index(mainSource, `if os.Args[1] == "run-gcs-ha-ack-loss" {`)
	if start < 0 {
		t.Fatal("dedicated ACK-loss qualification command is missing")
	}
	end := strings.Index(mainSource[start:], "\n\tif os.Args[1] == ")
	if end < 0 {
		t.Fatal("dedicated ACK-loss command block is not bounded")
	}
	block := mainSource[start : start+end]
	if !strings.Contains(block, "fatal(configureGCSHA(ctx))") || !strings.Contains(block, "fatal(qualifyGCSHAAckLoss(ctx))") {
		t.Fatal("ACK-loss command does not validate the GCS profile before running its driver")
	}
}

func TestGCSAsyncNonemptyQualificationHasDedicatedCommand(t *testing.T) {
	data, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	mainSource := string(data)
	start := strings.Index(mainSource, `if os.Args[1] == "run-gcs-ha-async-nonempty" {`)
	if start < 0 {
		t.Fatal("dedicated async-nonempty qualification command is missing")
	}
	end := strings.Index(mainSource[start:], "\n\tif os.Args[1] == ")
	if end < 0 {
		t.Fatal("dedicated async-nonempty command block is not bounded")
	}
	block := mainSource[start : start+end]
	if !strings.Contains(block, "fatal(configureGCSHA(ctx))") || !strings.Contains(block, "fatal(qualifyGCSAsyncNonempty(ctx))") {
		t.Fatal("async-nonempty command does not validate the GCS profile before running its driver")
	}
}

func TestGCSDeadPodQualificationHasDedicatedCommand(t *testing.T) {
	data, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	mainSource := string(data)
	start := strings.Index(mainSource, `if os.Args[1] == "run-gcs-ha-dead-pod" {`)
	if start < 0 {
		t.Fatal("dedicated dead-Pod qualification command is missing")
	}
	end := strings.Index(mainSource[start:], "\n\tif os.Args[1] == ")
	if end < 0 {
		t.Fatal("dedicated dead-Pod command block is not bounded")
	}
	block := mainSource[start : start+end]
	if !strings.Contains(block, "fatal(configureGCSHA(ctx))") || !strings.Contains(block, "fatal(qualifyGCSDeadPod(ctx))") {
		t.Fatal("dead-Pod command does not validate the GCS profile before running its driver")
	}
}

func TestFixtureOnlyWorkflowIsExclusiveAndRunsFixedGoSelectors(t *testing.T) {
	data, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	type input struct {
		Type    string `yaml:"type"`
		Default bool   `yaml:"default"`
	}
	type step struct {
		Run string `yaml:"run"`
	}
	var workflow struct {
		On struct {
			WorkflowDispatch struct {
				Inputs map[string]input `yaml:"inputs"`
			} `yaml:"workflow_dispatch"`
		} `yaml:"on"`
		Jobs map[string]struct {
			If    string `yaml:"if"`
			Steps []step `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatal(err)
	}
	fixtureInput, ok := workflow.On.WorkflowDispatch.Inputs["fixture_only"]
	if !ok || fixtureInput.Type != "boolean" || fixtureInput.Default {
		t.Fatal("fixture_only must be an opt-in boolean that defaults to the normal full workflow")
	}
	wantConditions := map[string]string{
		"fencing-only":                    "github.event_name == 'workflow_dispatch' && inputs.fencing_only && !inputs.fixture_only",
		"fixture-only":                    "github.event_name == 'workflow_dispatch' && inputs.fixture_only",
		"cooperative-fencing":             "github.event_name != 'workflow_dispatch' || !inputs.fixture_only",
		"object-store-provider-contracts": "github.event_name != 'workflow_dispatch' || (!inputs.fencing_only && !inputs.fixture_only)",
		"embedded-object-storage-providers": "github.event_name != 'workflow_dispatch' || (!inputs.fencing_only && !inputs.fixture_only)",
		"operator-chaos":                  "github.event_name != 'workflow_dispatch' || (!inputs.fencing_only && !inputs.fixture_only)",
		"automatic-operator-chaos":        "github.event_name != 'workflow_dispatch' || (!inputs.fencing_only && !inputs.fixture_only)",
		"membership-e2e":                  "github.event_name != 'workflow_dispatch' || (!inputs.fencing_only && !inputs.fixture_only)",
		"rust-sdk":                        "github.event_name != 'workflow_dispatch' || (!inputs.fencing_only && !inputs.fixture_only)",
		"go":                              "github.event_name != 'workflow_dispatch' || (!inputs.fencing_only && !inputs.fixture_only)",
		"node-startup-versity":            "github.event_name != 'workflow_dispatch' || (!inputs.fencing_only && !inputs.fixture_only)",
		"containers":                      "github.event_name != 'workflow_dispatch' || (!inputs.fencing_only && !inputs.fixture_only)",
		"anchor-operator-chaos":           "github.event_name != 'workflow_dispatch' || (!inputs.fencing_only && !inputs.fixture_only)",
		"publish-chaos-image":             "github.repository == 'mrchypark/rhiza' && github.event_name == 'workflow_dispatch' && inputs.publish_chaos_image && !inputs.fencing_only && !inputs.fixture_only",
	}
	if len(workflow.Jobs) != len(wantConditions) {
		t.Fatalf("workflow job set changed: got %d jobs, contract covers %d", len(workflow.Jobs), len(wantConditions))
	}
	for name, want := range wantConditions {
		job, ok := workflow.Jobs[name]
		if !ok || job.If != want {
			t.Errorf("job %s condition=%q, want %q", name, job.If, want)
		}
	}
	var run string
	for _, step := range workflow.Jobs["fixture-only"].Steps {
		run += step.Run
	}
	hasLine := func(want string) bool {
		for _, line := range strings.Split(run, "\n") {
			if line == want {
				return true
			}
		}
		return false
	}
	wantEmbeddedSelector := "embedded_tests='^(TestIndependentRollbackReconcilesInterruptedCreateMarkers|TestRollbackMarkerGateCoversEveryIndependentAndCIWrite|TestIndependentGCSChaosManifestContract|TestNamespaceExpiryRendersUIDResourceVersionDelete|TestQualificationHostContainsPinnedKubectlTools|TestRuntimeAgentRenderPinsNodesAndNamespaceOwnerReferences|TestVoterCountLauncherRunRequiresPrivilegedInstallCredential|TestGCSIAM.*)$'"
	if !hasLine(wantEmbeddedSelector) || strings.Contains(run, "operator_tests=") {
		t.Fatal("fixture-only job must retain only the fixed embedded rollback/IAM/manifest/launcher selector")
	}
	wantCommands := []string{
		`go test ./pkg/operator -run '^(TestManualTerminalFrozenForkRegistersWithoutIndependentV2|TestAuthenticatedLearnerPromotionRetriesExactCanonicalCompletion)$' -count=1 -timeout=5m`,
		`go test ./pkg/node -run '^(TestMembershipRetryDurablyPublishesCommittedConfiguration|TestMembershipAdditionAbortAllowsNextRevision)$' -count=1 -timeout=5m`,
		`go test ./e2e/chaos/embedded-go -run "$embedded_tests" -count=1 -timeout=5m`,
		`go test ./pkg/fencing ./pkg/fencing/cri ./pkg/fencing/runtimeagent ./pkg/operator ./pkg/recovery ./internal/objstore ./e2e/fencing -count=1 -timeout=5m`,
		`go test -race ./pkg/node -run '^(TestMembershipRetryDurablyPublishesCommittedConfiguration|TestMembershipAdditionAbortAllowsNextRevision)$' -count=1 -timeout=5m`,
		`go test -race ./e2e/chaos/embedded-go -run "$embedded_tests" -count=1 -timeout=5m`,
		`go test -race ./pkg/fencing ./pkg/fencing/cri ./pkg/fencing/runtimeagent ./pkg/operator ./pkg/recovery ./internal/objstore ./e2e/fencing -count=1 -timeout=5m`,
	}
	for _, command := range wantCommands {
		if !hasLine(command) {
			t.Errorf("fixture-only job omits exact package/selector command %q", command)
		}
	}
	previousCommand := -1
	for _, command := range wantCommands {
		position := strings.Index(run, command)
		if position <= previousCommand {
			t.Fatalf("fixture-only commands are not in the required order: %q", command)
		}
		previousCommand = position
	}
	if strings.Count(run, "go test ") != len(wantCommands) {
		t.Fatal("fixture-only job must contain only the seven asserted focused test commands")
	}
	if strings.Contains(run, "go test ./...") || strings.Contains(run, "kind ") || strings.Contains(run, "docker ") {
		t.Fatal("fixture-only job must remain a focused Go feedback path without cluster/image work")
	}
}

func TestFreshNamespaceRendererRewritesPinnedNamespaceLookup(t *testing.T) {
	const namespace = "rhiza-fencing-isolated"
	runBytes, err := os.ReadFile("run.sh")
	if err != nil {
		t.Fatal(err)
	}
	const renderExpression = `-e "s/resourceNames: \[rhiza\]$/resourceNames: [$fencing_namespace]/"`
	if !strings.Contains(string(runBytes), renderExpression) {
		t.Fatalf("run.sh must rewrite the pinned namespace resourceNames when rendering the fresh qualification namespace")
	}
	manifest, err := os.ReadFile("../../deploy/operator/fencing/authority.yaml")
	if err != nil {
		t.Fatal(err)
	}
	rendered := strings.ReplaceAll(string(manifest), "namespace: rhiza\n", "namespace: "+namespace+"\n")
	rendered = strings.ReplaceAll(rendered, "resourceNames: [rhiza]\n", "resourceNames: ["+namespace+"]\n")
	decoder := yaml.NewDecoder(strings.NewReader(rendered))
	found := false
	for {
		var doc obj
		if err := decoder.Decode(&doc); err != nil {
			break
		}
		if doc["kind"] != "ClusterRole" || nested(doc, "metadata", "name") != "rhiza-fencer-tokenreview" {
			continue
		}
		for _, raw := range doc["rules"].([]any) {
			rule := raw.(map[string]any)
			if len(rule["resources"].([]any)) != 1 || rule["resources"].([]any)[0] != "namespaces" {
				continue
			}
			found = true
			if len(rule["resourceNames"].([]any)) != 1 || rule["resourceNames"].([]any)[0] != namespace {
				t.Fatalf("rendered namespace resourceNames=%v, want only %s", rule["resourceNames"], namespace)
			}
		}
	}
	if !found {
		t.Fatal("rendered authority manifest omitted the namespace lookup rule")
	}
}

func TestImmutableImageResolutionDescriptors(t *testing.T) {
	config := "sha256:" + strings.Repeat("a", 64)
	for _, reference := range []string{config, "containerd://" + config, "docker.io/library/import-date@" + config} {
		if got, err := immutableDigest(reference); err != nil || got != config {
			t.Fatalf("immutable reference %q: %q, %v", reference, got, err)
		}
	}
	for _, reference := range []string{"image:local", "sha256:abc", "sha256:" + strings.Repeat("z", 64)} {
		if _, err := immutableDigest(reference); err == nil {
			t.Fatalf("accepted mutable or invalid reference %q", reference)
		}
	}
	manifest := `{"digest":"` + config + `","size":708,"mediaType":"application/vnd.oci.image.manifest.v1+json","annotations":{"tag":"host"}}`
	alias := strings.Replace(manifest, `"host"`, `"operator"`, 1)
	platformAlias := strings.Replace(manifest, `,"annotations"`, `,"platform":{"os":"linux","architecture":"amd64"},"annotations"`, 1)
	platformAliasOtherTag := strings.Replace(platformAlias, `"host"`, `"operator"`, 1)
	index := func(second string) string { return `{"manifests":[` + manifest + `,` + second + `]}` }
	amd64 := `sha256:` + strings.Repeat("b", 64)
	arm64 := `sha256:` + strings.Repeat("c", 64)
	attestationDigest := `sha256:` + strings.Repeat("d", 64)
	platformManifest := func(digest string, size int, os, architecture string, variants ...string) string {
		variant := ""
		if len(variants) > 0 {
			variant = fmt.Sprintf(`,"variant":%q`, variants[0])
		}
		return fmt.Sprintf(`{"digest":%q,"size":%d,"mediaType":"application/vnd.oci.image.manifest.v1+json","platform":{"os":%q,"architecture":%q%s}}`, digest, size, os, architecture, variant)
	}
	platformlessManifest := func(digest string) string {
		return fmt.Sprintf(`{"digest":%q,"size":708,"mediaType":"application/vnd.oci.image.manifest.v1+json","platform":{"os":"","architecture":""}}`, digest)
	}
	platformIndex := func(entries ...string) string { return `{"manifests":[` + strings.Join(entries, ",") + `]}` }
	artifactIndexDigest := "sha256:e8f067edb5526e727903d6ce55cfd151fddbd7c57336b5e35949a1a6336ebe6f"
	artifactIndexChild := fmt.Sprintf(`{"digest":%q,"size":855,"mediaType":"application/vnd.oci.image.index.v1+json","platform":{"os":"","architecture":""}}`, artifactIndexDigest)
	artifactIndex := `{"config":{"digest":""},"manifests":[` + artifactIndexChild + `,` + artifactIndexChild + `]}`
	linuxAMD64 := imagePlatform{OS: "linux", Architecture: "amd64"}
	linuxARM64 := imagePlatform{OS: "linux", Architecture: "arm64"}
	linuxARMv6 := imagePlatform{OS: "linux", Architecture: "arm", Variant: "v6"}
	linuxARMv7 := imagePlatform{OS: "linux", Architecture: "arm", Variant: "v7"}
	attestationDescriptor := `{"digest":"` + attestationDigest + `","size":600,"mediaType":"application/vnd.oci.image.manifest.v1+json","platform":{"os":"unknown","architecture":"unknown"},"annotations":{"vnd.docker.reference.type":"attestation-manifest","vnd.docker.reference.digest":"` + config + `"}}`
	runnableAMD64 := platformManifest(config, 708, "linux", "amd64")
	attestationIndex := func(descriptor string) string { return platformIndex(runnableAMD64, descriptor) }
	for _, tc := range []struct {
		name     string
		body     string
		platform imagePlatform
		want     string
		valid    bool
	}{
		{name: "config manifest", body: `{"config":{"digest":"` + config + `"}}`, want: config, valid: true},
		{name: "single child without platform", body: platformIndex(platformlessManifest(config)), want: config, valid: true},
		{name: "duplicate incomplete children", body: `{"manifests":[{"digest":"` + config + `"},{"digest":"` + config + `"}]}`, valid: false},
		{name: "duplicate descriptor aliases without platform", body: index(alias), want: config, valid: true},
		{name: "run artifact duplicate platformless OCI aliases", body: artifactIndex, platform: linuxAMD64, want: artifactIndexDigest, valid: true},
		{name: "duplicate tag aliases with exact platform", body: platformIndex(platformAlias, platformAliasOtherTag), platform: linuxAMD64, want: config, valid: true},
		{name: "conflicting aliases without platform", body: index(strings.Replace(alias, config, amd64, 1)), valid: false},
		{name: "distinct platformless child digests are ambiguous", body: platformIndex(platformlessManifest(config), platformlessManifest(amd64)), valid: false},
		{name: "alias size conflict", body: index(strings.Replace(alias, `"size":708`, `"size":709`, 1)), valid: false},
		{name: "alias media type conflict", body: index(strings.Replace(alias, "application/vnd.oci.image.manifest.v1+json", "application/vnd.docker.distribution.manifest.v2+json", 1)), valid: false},
		{name: "nonmatching platform", body: index(strings.Replace(alias, `"size":708`, `"size":708,"platform":{"architecture":"arm64","os":"linux"}`, 1)), valid: false},
		{name: "valid build attestation", body: attestationIndex(attestationDescriptor), platform: linuxAMD64, want: config, valid: true},
		{name: "attestation without runnable descriptor", body: platformIndex(attestationDescriptor), platform: linuxAMD64, valid: false},
		{name: "rejects wrong attestation type", body: attestationIndex(strings.Replace(attestationDescriptor, `"attestation-manifest"`, `"other"`, 1)), platform: linuxAMD64, valid: false},
		{name: "rejects malformed attestation platform", body: attestationIndex(strings.Replace(attestationDescriptor, `"os":"unknown"`, `"os":"linux"`, 1)), platform: linuxAMD64, valid: false},
		{name: "rejects malformed attestation media type", body: attestationIndex(strings.Replace(attestationDescriptor, "application/vnd.oci.image.manifest.v1+json", "application/vnd.oci.image.index.v1+json", 1)), platform: linuxAMD64, valid: false},
		{name: "rejects attestation for another image", body: attestationIndex(strings.Replace(attestationDescriptor, config, arm64, 1)), platform: linuxAMD64, valid: false},
		{name: "rejects zero-sized attestation", body: attestationIndex(strings.Replace(attestationDescriptor, `"size":600`, `"size":0`, 1)), platform: linuxAMD64, valid: false},
		{name: "rejects missing attestation type", body: attestationIndex(strings.Replace(attestationDescriptor, `"vnd.docker.reference.type":"attestation-manifest",`, ``, 1)), platform: linuxAMD64, valid: false},
		{name: "rejects invalid attestation digest", body: attestationIndex(strings.Replace(attestationDescriptor, attestationDigest, "sha256:invalid", 1)), platform: linuxAMD64, valid: false},
		{name: "selects exact host platform among other descriptors", body: platformIndex(platformManifest(arm64, 101, "linux", "arm64"), platformManifest(amd64, 202, "linux", "amd64")), platform: linuxAMD64, want: amd64, valid: true},
		{name: "selects exact host platform independent of descriptor order", body: platformIndex(platformManifest(arm64, 303, "linux", "arm64"), platformManifest(amd64, 202, "linux", "amd64")), platform: linuxAMD64, want: amd64, valid: true},
		{name: "selects exact arm64 platform without inferred variant", body: platformIndex(platformManifest(arm64, 303, "linux", "arm64")), platform: linuxARM64, want: arm64, valid: true},
		{name: "rejects unreported arm64 variant", body: platformIndex(platformManifest(arm64, 303, "linux", "arm64", "v8")), platform: linuxARM64, valid: false},
		{name: "selects exact arm v6 variant", body: platformIndex(platformManifest(arm64, 303, "linux", "arm", "v7"), platformManifest(amd64, 202, "linux", "arm", "v6")), platform: linuxARMv6, want: amd64, valid: true},
		{name: "rejects arm v7 variant for v6 host", body: platformIndex(platformManifest(arm64, 303, "linux", "arm", "v7")), platform: linuxARMv6, valid: false},
		{name: "selects exact arm v7 variant", body: platformIndex(platformManifest(arm64, 303, "linux", "arm", "v6"), platformManifest(amd64, 202, "linux", "arm", "v7")), platform: linuxARMv7, want: amd64, valid: true},
		{name: "rejects arm v6 variant for v7 host", body: platformIndex(platformManifest(arm64, 303, "linux", "arm", "v6")), platform: linuxARMv7, valid: false},
		{name: "rejects missing host platform", body: platformIndex(platformManifest(arm64, 303, "linux", "arm64")), platform: linuxAMD64, valid: false},
		{name: "rejects missing host platform when only an attestation matches", body: platformIndex(platformManifest(arm64, 303, "linux", "arm64"), attestationDescriptor), platform: linuxAMD64, valid: false},
		{name: "selects sole child with missing platform", body: platformIndex(`{"digest":` + fmt.Sprintf("%q", amd64) + `,"size":202,"mediaType":"application/vnd.oci.image.manifest.v1+json"}`), platform: linuxAMD64, want: amd64, valid: true},
		{name: "rejects missing platform child in mixed index", body: platformIndex(platformManifest(arm64, 303, "linux", "arm64"), `{"digest":` + fmt.Sprintf("%q", amd64) + `,"size":202,"mediaType":"application/vnd.oci.image.manifest.v1+json"}`), platform: linuxAMD64, valid: false},
		{name: "rejects multiple host platform matches", body: platformIndex(platformManifest(amd64, 202, "linux", "amd64"), platformManifest(attestationDigest, 101, "linux", "amd64")), platform: linuxAMD64, valid: false},
		{name: "invalid mutable config digest", body: `{"config":{"digest":"image:local"}}`, valid: false},
		{name: "empty descriptor", body: `{}`, valid: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			platform := tc.platform
			if platform.OS == "" {
				platform = linuxAMD64
			}
			body := []byte(tc.body)
			d := fmt.Sprintf("sha256:%x", sha256.Sum256(body))
			got, err := imageDescriptor(d, body, platform)
			if tc.valid && (err != nil || got != tc.want) || !tc.valid && err == nil {
				t.Fatalf("descriptor %s: %q, %v", tc.body, got, err)
			}
			if _, err := imageDescriptor(config, body, platform); err == nil {
				t.Fatal("accepted content that did not hash to the requested digest")
			}
		})
	}
	withoutTarget := []byte(platformIndex(platformManifest(amd64, 202, "linux", "amd64")))
	withoutTargetID := fmt.Sprintf("sha256:%x", sha256.Sum256(withoutTarget))
	if _, err := imageDescriptor(withoutTargetID, withoutTarget, imagePlatform{}); err == nil {
		t.Fatal("accepted index resolution without target platform")
	}

	// The run's built image Id may be an OCI index rather than a config digest.
	// Model the produced artifact's platform index, target manifest, and config
	// descriptor, all keyed by their actual content hashes.
	hashContent := func(content string) string {
		return fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(content)))
	}
	configID := "sha256:" + strings.Repeat("e", 64)
	leaf := fmt.Sprintf(`{"config":{"digest":%q}}`, configID)
	leafID := hashContent(leaf)
	attestationID := "sha256:" + strings.Repeat("f", 64)
	builtPlatformIndex := fmt.Sprintf(`{"manifests":[{"digest":%q,"size":%d,"mediaType":"application/vnd.oci.image.manifest.v1+json","platform":{"os":"linux","architecture":"amd64"}},{"digest":%q,"size":564,"mediaType":"application/vnd.oci.image.manifest.v1+json","platform":{"os":"unknown","architecture":"unknown"}}]}`, leafID, len(leaf), attestationID)
	platformIndexID := hashContent(builtPlatformIndex)
	builtIndexID := platformIndexID
	contents := map[string][]byte{platformIndexID: []byte(builtPlatformIndex), leafID: []byte(leaf)}
	resolved := builtIndexID
	for range 2 {
		content := contents[resolved]
		next, err := imageDescriptor(resolved, content, linuxAMD64)
		if err != nil {
			t.Fatalf("resolve built descriptor %s: %v", resolved, err)
		}
		if resolved == leafID {
			if builtIndexID == configID || next != configID {
				t.Fatalf("built index/config identity: index=%s config=%s resolved=%s", builtIndexID, configID, next)
			}
			break
		}
		resolved = next
	}
	if resolved != leafID {
		t.Fatalf("built target-platform chain did not reach leaf manifest: %s", resolved)
	}
	if !imageConfigMatches(configID, configID) {
		t.Fatal("rejected the existing built config-digest identity path")
	}
	if imageConfigMatches(configID, builtIndexID) {
		t.Fatal("accepted the OCI index digest as a config identity")
	}
	if imageConfigMatches(configID, amd64) {
		t.Fatal("accepted a different built target-platform config identity")
	}
}

func TestFixtureHoldsWritersUntilSupervisorAndStoreReady(t *testing.T) {
	items := manifests()["items"].([]obj)
	var sts, job, operator bool
	for _, item := range items {
		if nested(item, "metadata", "namespace") != namespace {
			t.Fatal("fixture escaped owned namespace")
		}
		switch item["kind"] {
		case "StatefulSet":
			sts = true
			if nested(item, "spec", "updateStrategy", "type") != "OnDelete" {
				t.Fatal("qualification changed the recovery OnDelete strategy")
			}
			if nested(item, "spec", "replicas") != 0 {
				t.Fatal("writer launched before canonical supervised patch")
			}
		case "Job":
			job = true
			if nested(item, "spec", "suspend") != true {
				t.Fatal("bucket setup races store readiness")
			}
		case "Deployment":
			if nested(item, "metadata", "name") == "rhiza-operator" {
				operator = true
				if nested(item, "spec", "replicas") != 0 {
					t.Fatal("operator launched before voters")
				}
			}
		}
	}
	if !sts || !job || !operator {
		t.Fatal("fixture omitted lifecycle resource")
	}
}

func TestObjectStoreE2EManifestUsesPinnedVersityGateway(t *testing.T) {
	manifest, err := os.ReadFile("../../deploy/k8s/object-store-e2e.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"name: rhiza-versitygw",
		"app: rhiza-versitygw",
		"name: versitygw",
		"image: ghcr.io/versity/versitygw@sha256:30292fc2eeacc67a36993b01f7a7a5e3361a19cced0e80c1d71cfa2a4b0a2499",
		"args: [\"--port\", \":9000\", \"--health\", \"/_/health\", \"posix\", \"/data\"]",
		"name: ROOT_ACCESS_KEY",
		"name: ROOT_SECRET_KEY",
		"path: /_/health",
		"image: amazon/aws-cli@sha256:83f8ffe939569070c5b66d22231862ab78718766d9d8e4c44ca84dd0be5569a5",
		"aws --endpoint-url http://rhiza-versitygw:9000 s3 mb s3://rhiza",
	} {
		if !strings.Contains(string(manifest), want) {
			t.Errorf("object-store fixture missing %q", want)
		}
	}
	for _, forbidden := range []string{"quay.io/minio", "MINIO_ROOT_", "minio/mc", "mc alias", "mc mb"} {
		if strings.Contains(string(manifest), forbidden) {
			t.Errorf("object-store fixture still contains %q", forbidden)
		}
	}

	for _, path := range []string{
		"../../deploy/k8s/graph-server-3peer-e2e.yaml",
		"../../deploy/k8s/sql-server-3peer-e2e.yaml",
	} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(data), "rhiza-versitygw.rhiza-objstore-e2e.svc.cluster.local:9000") {
			t.Errorf("%s does not use the Versity Gateway service", path)
		}
	}
}

func TestFixtureKeepsStoreCoordinatesPublicAndWriterKeysLocalOnly(t *testing.T) {
	items := manifests()["items"].([]obj)
	var configMap, statefulSet, operator bool
	for _, item := range items {
		name := nested(item, "metadata", "name")
		if item["kind"] == "ConfigMap" && name == "rhiza-config" {
			configMap = true
			data := item["data"].(obj)
			for key, want := range map[string]string{
				"RHIZA_OBJSTORE_ENDPOINT": "rhiza-versitygw:9000",
				"RHIZA_OBJSTORE_BUCKET":   "rhiza",
				"RHIZA_OBJSTORE_REGION":   "us-east-1",
				"RHIZA_OBJSTORE_INSECURE": "true",
			} {
				if data[key] != want {
					t.Errorf("public store coordinate %s=%v, want %q", key, data[key], want)
				}
			}
			for _, key := range []string{"RHIZA_OBJSTORE_ACCESS_KEY", "RHIZA_OBJSTORE_SECRET_KEY"} {
				if _, ok := data[key]; ok {
					t.Errorf("writer credential %s leaked into public ConfigMap", key)
				}
			}
		}
		if item["kind"] == "Secret" && name == "rhiza-object-store" {
			t.Fatal("base fixture manifests include the local writer Secret")
		}
		if item["kind"] == "StatefulSet" && name == "rhiza" {
			statefulSet = true
			if referencesStoreWriterSecret(nested(item, "spec", "template", "spec", "containers").([]obj)[0]) {
				t.Fatal("base StatefulSet references the local writer Secret")
			}
		}
		if item["kind"] == "Deployment" && name == "rhiza-operator" {
			operator = true
			if referencesStoreWriterSecret(nested(item, "spec", "template", "spec", "containers").([]obj)[0]) {
				t.Fatal("base operator references the local writer Secret")
			}
		}
	}
	if !configMap || !statefulSet || !operator {
		t.Fatal("base fixture omitted ConfigMap, source StatefulSet, or Operator")
	}

	secret := localCooperativeStoreSecret()
	if secret["kind"] != "Secret" || nested(secret, "metadata", "name") != "rhiza-object-store" {
		t.Fatalf("local overlay Secret identity=%v/%v", secret["kind"], nested(secret, "metadata", "name"))
	}
	credentials := secret["stringData"].(obj)
	if len(credentials) != 2 || credentials["RHIZA_OBJSTORE_ACCESS_KEY"] != "fencing-local" || credentials["RHIZA_OBJSTORE_SECRET_KEY"] != "fencing-local-secret" {
		t.Fatalf("local overlay must contain only fake writer keys: %#v", credentials)
	}

	patchBytes, err := os.ReadFile("../../deploy/operator/fencing/supervised-statefulset.patch.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(patchBytes), "rhiza-object-store") || strings.Contains(string(patchBytes), "RHIZA_OBJSTORE_ACCESS_KEY") || strings.Contains(string(patchBytes), "RHIZA_OBJSTORE_SECRET_KEY") {
		t.Fatal("shipped supervised patch references or exposes the local writer Secret")
	}
	runScript, err := os.ReadFile("run.sh")
	if err != nil {
		t.Fatal(err)
	}
	run := string(runScript)
	productionPatch := strings.Index(run, `k patch statefulset rhiza --type=strategic --patch-file "$fencing_tmp/supervised.patch.yaml"`)
	localSecret := strings.Index(run, `"$FENCING_E2E_BIN" local-cooperative-store-secret`)
	statefulOverlay := strings.Index(run, `k patch statefulset rhiza --type=json -p "$local_store_ref"`)
	operatorOverlay := strings.Index(run, `k patch deployment rhiza-operator --type=json -p "$local_store_ref"`)
	fencerOverlay := strings.Index(run, `local_fencer_store_ref='[{"op":"add","path":"/spec/template/spec/containers/0/envFrom","value":[{"configMapRef":{"name":"rhiza-config"}},{"secretRef":{"name":"rhiza-object-store"}}]}]'`)
	fencerPatch := strings.Index(run, `k patch deployment rhiza-fencer --type=json -p "$local_fencer_store_ref"`)
	fencerRollout := strings.Index(run, `k rollout status deployment/rhiza-fencer --timeout=180s`)
	if productionPatch < 0 || localSecret <= productionPatch || statefulOverlay <= localSecret || operatorOverlay <= statefulOverlay || fencerOverlay <= operatorOverlay || fencerPatch <= fencerOverlay || fencerRollout <= fencerPatch {
		t.Fatal("local fake writer credential overlay must be applied after the shipped production patch")
	}
}

func referencesStoreWriterSecret(container obj) bool {
	for _, source := range container["envFrom"].([]obj) {
		if nested(source, "secretRef", "name") == "rhiza-object-store" {
			return true
		}
	}
	return false
}

func TestFixtureRestoreHasWritableTmpWithReadOnlyRoot(t *testing.T) {
	data, err := os.ReadFile("../../deploy/operator/fencing/supervised-statefulset.patch.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var patch obj
	if err := yaml.NewDecoder(strings.NewReader(string(data))).Decode(&patch); err != nil {
		t.Fatal(err)
	}
	if err := verifyScratch(patch); err != nil {
		t.Fatal(err)
	}
	nested(patch, "spec", "template", "spec").(obj)["volumes"] = []any{}
	if verifyScratch(patch) == nil {
		t.Fatal("accepted read-only root without writable scratch storage")
	}
}

func TestInitialHeadLauncherExitCleanupPrecedesTokenRemovalAndPreservesStatus(t *testing.T) {
	launcher, err := os.ReadFile("../chaos/run-votercount-gcs-chaos.sh")
	if err != nil {
		t.Fatal(err)
	}
	const startMarker = "cleanup() {"
	const endMarker = "\n}\ntrap cleanup EXIT"
	start := strings.Index(string(launcher), startMarker)
	if start < 0 {
		t.Fatal("launcher has no cleanup function")
	}
	endOffset := strings.Index(string(launcher[start:]), endMarker)
	if endOffset < 0 {
		t.Fatal("launcher cleanup function is not installed as the EXIT trap")
	}
	cleanupFunction := string(launcher[start : start+endOffset+2])

	tmp := t.TempDir()
	out := filepath.Join(tmp, "private")
	if err := os.Mkdir(out, 0700); err != nil {
		t.Fatal(err)
	}
	tokenPath := filepath.Join(out, "admin-token")
	if err := os.WriteFile(tokenPath, []byte("test-only-token"), 0600); err != nil {
		t.Fatal(err)
	}
	// A restore record marks that the fencer fault was armed. The fake driver
	// fails deliberately after recording whether the token still exists.
	if err := os.WriteFile(filepath.Join(out, gcsInitialHeadInstallStateName), []byte("test-only-state"), 0600); err != nil {
		t.Fatal(err)
	}
	eventsPath := filepath.Join(tmp, "events")
	driverPath := filepath.Join(tmp, "fencing-e2e")
	driver := "#!/bin/sh\n" +
		"printf '%s token=%s\\n' \"$1\" \"$(test -f \"$TEST_TOKEN\" && echo present || echo absent)\" >> \"$TEST_EVENTS\"\n" +
		"exit 29\n"
	if err := os.WriteFile(driverPath, []byte(driver), 0700); err != nil {
		t.Fatal(err)
	}
	script := cleanupFunction + `
set -eu
out=$TEST_OUT
admin_token_file=$TEST_TOKEN
tls_tmp=
remove_admin_token=true
mode=install
initial_head_ack_loss=yes
cleanup_namespace=false
FENCING_E2E_BIN=$TEST_DRIVER
export FENCING_E2E_OUT=$out
export RHIZA_KUBE_CONTEXT=test-context RHIZA_NAMESPACE=test-namespace RHIZA_RUN_ID=a1b2c3d4
trap cleanup EXIT
exit 23
`
	cmd := exec.Command("sh", "-c", script)
	cmd.Env = append(os.Environ(),
		"TEST_OUT="+out,
		"TEST_TOKEN="+tokenPath,
		"TEST_DRIVER="+driverPath,
		"TEST_EVENTS="+eventsPath,
	)
	err = cmd.Run()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 23 {
		t.Fatalf("cleanup changed the original launcher exit status: err=%v", err)
	}
	events, err := os.ReadFile(eventsPath)
	if err != nil {
		t.Fatalf("armed fencer cleanup was not attempted: %v", err)
	}
	if got := string(events); got != "cleanup-gcs-ha-initial-head-ack-loss token=present\n" {
		t.Fatalf("fencer cleanup did not precede local token removal: %q", got)
	}
	if _, err := os.Stat(tokenPath); !os.IsNotExist(err) {
		t.Fatalf("launcher did not remove its local token after fencer cleanup: %v", err)
	}
}

func TestAsyncNonemptyLauncherRequiresDedicatedNativeScenarioAndProjection(t *testing.T) {
	launcher, err := os.ReadFile("../chaos/run-votercount-gcs-chaos.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(launcher)
	for _, required := range []string{
		"async_nonempty=${RHIZA_GCS_ASYNC_NONEMPTY:-no}",
		"for selected in \"$initial_head_ack_loss\" \"$async_nonempty\" \"$manual_terminal\"",
		"[ \"$scenario_count\" -le 1 ]",
		"run-gcs-ha-async-nonempty",
		"FENCING_E2E_QUALIFICATION=gcs-async-nonempty",
		"actual-exit.txt",
		"source-head.txt",
		"image-ids.txt",
		"strict async-nonempty artifact projection failed",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("async-nonempty launcher contract is missing %q", required)
		}
	}
	asyncStart := strings.Index(source, "if [ \"$async_nonempty\" = yes ]; then\n  : \"${FENCING_E2E_BIN:?async-nonempty qualification requires the fencing-e2e driver}\"\n")
	nativeDispatch := strings.Index(source, "step 30-async-nonempty-collector env")
	nativeInvocation := strings.Index(source, `"$FENCING_E2E_BIN" run-gcs-ha-async-nonempty`)
	manualStart := strings.Index(source, "if [ \"$manual_terminal\" = yes ]; then\n  : \"${FENCING_E2E_BIN:?manual-terminal qualification requires the fencing-e2e driver}\"")
	branchExit := strings.Index(source, "echo 'nonempty async GCS quorum-loss and exact archived-tip recovery collector completed'\n  exit 0\nfi")
	missingMembershipStart := strings.Index(source, "if [ \"$missing_membership\" = yes ]; then\n  : \"${FENCING_E2E_BIN:?missing-membership qualification requires the fencing-e2e driver}\"")
	legacyPython := strings.Index(source, "step 30-driver python3 -u")
	if asyncStart < 0 || nativeDispatch < asyncStart || nativeInvocation < nativeDispatch ||
		branchExit < nativeInvocation || manualStart < branchExit || missingMembershipStart < manualStart ||
		legacyPython < missingMembershipStart {
		t.Fatal("async-nonempty scenario must dispatch through the native driver before the legacy Python scenario path")
	}
}

func TestManualTerminalLauncherIsExplicitAndUsesNativeApprovalCollector(t *testing.T) {
	launcher, err := os.ReadFile("../chaos/run-votercount-gcs-chaos.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(launcher)
	for _, required := range []string{
		"manual_terminal=${RHIZA_GCS_MANUAL_TERMINAL:-no}",
		"[ \"$scenario_count\" -le 1 ]",
		"run-gcs-ha-manual-terminal",
		"RHIZA_GCS_MANUAL_TERMINAL=yes",
		"FENCING_E2E_QUALIFICATION=gcs-manual-terminal",
		"strict manual-terminal artifact projection failed",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("manual-terminal launcher contract is missing %q", required)
		}
	}
	manualStart := strings.Index(source, "if [ \"$manual_terminal\" = yes ]; then\n  : \"${FENCING_E2E_BIN:?manual-terminal qualification requires the fencing-e2e driver}\"")
	nativeDispatch := strings.Index(source, "step 30-manual-terminal-collector env")
	nativeInvocation := strings.Index(source, `"$FENCING_E2E_BIN" run-gcs-ha-manual-terminal`)
	branchExit := strings.Index(source, "echo 'manual-terminal GCS challenge, independent approval, recovery, and readback collector completed'\n  exit 0\nfi")
	missingMembershipStart := strings.Index(source, "if [ \"$missing_membership\" = yes ]; then\n  : \"${FENCING_E2E_BIN:?missing-membership qualification requires the fencing-e2e driver}\"")
	legacyPython := strings.Index(source, "step 30-driver python3 -u")
	if manualStart < 0 || nativeDispatch < manualStart || nativeInvocation < nativeDispatch ||
		branchExit < nativeInvocation || missingMembershipStart < branchExit || legacyPython < missingMembershipStart {
		t.Fatal("manual-terminal qualification must use the dedicated native collector and return before the legacy Python path")
	}
}

func TestDeadPodLauncherIsExplicitAndUsesNativeCollector(t *testing.T) {
	launcher, err := os.ReadFile("../chaos/run-votercount-gcs-chaos.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(launcher)
	for _, required := range []string{
		"dead_pod=${RHIZA_GCS_DEAD_POD:-no}",
		"if [ \"$async_nonempty\" = yes ] || [ \"$missing_membership\" = yes ] || [ \"$dead_pod\" = yes ] || [ \"$mixed_pending\" = yes ]; then",
		"RHIZA_GCS_DEAD_POD must be yes or no",
		"\"$missing_membership\" \"$dead_pod\"",
		"run-gcs-ha-dead-pod",
		"FENCING_E2E_QUALIFICATION=gcs-dead-pod",
		"strict dead-Pod artifact projection failed",
		"actual-exit.txt",
		"source-head.txt",
		"image-ids.txt",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("dead-Pod launcher contract is missing %q", required)
		}
	}
	deadPodStart := strings.Index(source, "if [ \"$dead_pod\" = yes ]; then\n  : \"${FENCING_E2E_BIN:?dead-Pod qualification requires the source-matched fencing-e2e driver}\"")
	nativeDispatch := strings.Index(source, "step 30-dead-pod-collector env")
	nativeInvocation := strings.Index(source, `"$FENCING_E2E_BIN" run-gcs-ha-dead-pod`)
	branchExit := strings.Index(source, "echo 'GCS same-ordinal dead-Pod recovery collector completed'\n  exit 0\nfi")
	legacyPython := strings.Index(source, "step 30-driver python3 -u")
	if deadPodStart < 0 || nativeDispatch < deadPodStart || nativeInvocation < nativeDispatch ||
		branchExit < nativeInvocation || legacyPython < branchExit {
		t.Fatal("dead-Pod qualification must dispatch through the native collector before the legacy Python path")
	}
	finalizeStart := strings.Index(source, `if [ "$mode" = run ] && [ "$dead_pod" = yes ]; then`)
	if finalizeStart < 0 {
		t.Fatal("dead-Pod collector has no dedicated EXIT finalizer")
	}
	finalizeEnd := strings.Index(source[finalizeStart:], `rm -f "$out/.admin-token.tmp"`)
	if finalizeEnd < 0 {
		t.Fatal("dead-Pod collector has no bounded EXIT finalizer")
	}
	finalizer := source[finalizeStart : finalizeStart+finalizeEnd]
	for _, receipt := range []string{
		`> "$out/actual-exit.txt"`,
		`> "$out/source-head.txt"`,
		`> "$out/image-ids.txt"`,
		`FENCING_E2E_QUALIFICATION=gcs-dead-pod`,
		`"$FENCING_E2E_BIN" safe-artifacts`,
	} {
		if !strings.Contains(finalizer, receipt) {
			t.Fatalf("dead-Pod finalizer is missing %q", receipt)
		}
	}
}

func TestGCSDeadPodSafeArtifactsProjectOnlyRedactedEvidence(t *testing.T) {
	t.Setenv("FENCING_E2E_QUALIFICATION", gcsDeadPodQualification)
	const canary = "private-gcs-dead-pod-identity-canary"
	raw := t.TempDir()
	write := func(name string, data []byte) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(raw, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("actual-exit.txt", []byte("0\n"))
	write("source-head.txt", []byte(strings.Repeat("a", 40)+"\n"))
	write("image-ids.txt", []byte("ghcr.io/example/app@sha256:"+strings.Repeat("b", 64)+"\nghcr.io/example/operator@sha256:"+strings.Repeat("c", 64)+"\n"))
	evidence := gcsDeadPodPrivateEvidence{
		Qualification: "gcs-noPVC-single-voter-dead-pod", Status: "qualified",
		Source: gcsRecoverySource{Bucket: canary, Prefix: "private-prefix", ClusterID: canary, StatefulSet: "rhiza", StatefulSetUID: canary,
			Namespace: canary, BindingUID: canary, LogicalID: canary,
			Durability: "before-ack", Targets: []operator.FenceTarget{
				{Pod: "rhiza-0", PodUID: "old-pod", NodeID: "old-node", WALIdentity: "old-wal"},
				{Pod: "rhiza-1", PodUID: "survivor-1", NodeID: "node-1", WALIdentity: "wal-1"},
				{Pod: "rhiza-2", PodUID: "survivor-2", NodeID: "node-2", WALIdentity: "wal-2"},
			}},
		OldIncarnation: operator.FenceTarget{Pod: "rhiza-0", PodUID: "old-pod", NodeID: "old-node", WALIdentity: "old-wal"},
		FenceRequest: operator.FenceRequest{OperationID: "operation", SourceClusterID: canary, StatefulSet: "rhiza", Namespace: canary,
			LogicalID: canary, BindingUID: canary, Scope: operator.ScopeVoter, StatefulSetUID: canary,
			Targets: []operator.FenceTarget{{Pod: "rhiza-0", PodUID: "old-pod", NodeID: "old-node", WALIdentity: "old-wal"}}},
		FenceProof: operator.FenceProof{ProofID: "proof", OperationID: "operation", SourceClusterID: canary, BindingUID: canary,
			IndependentV2: &operator.IndependentEvidenceV2{Version: 2, RequestHash: strings.Repeat("d", 64), LedgerHash: strings.Repeat("e", 64),
				StorageScope:   operator.AttestationStoreScope{Provider: "gcs", Bucket: canary, Prefix: "private-prefix"},
				CurrentPodUIDs: []string{"survivor-1", "survivor-2", "new-pod"}}},
		Recovery: operator.Resource{Spec: operator.Spec{SourceClusterID: canary, RecoveryID: "operation",
			Fence:      operator.Fence{Confirmed: true, RecoveryID: "operation", ClusterID: canary, StatefulSetUID: canary, Evidence: "proof"},
			Membership: &operator.MembershipSpec{ReplacementPod: "rhiza-0"}},
			Status: operator.Status{Phase: "Complete", Stage: "Complete", Source: canary, Target: canary,
				StatefulSetUID: canary, Membership: &operator.MembershipStatus{Phase: "Complete",
					Add: &operator.MembershipAddJournal{MemberID: "new-node", WALIdentity: "new-wal"}}}},
		Replacement:     operator.FenceTarget{Pod: "rhiza-0", PodUID: "new-pod", NodeID: "new-node", WALIdentity: "new-wal"},
		BaselineAck:     gcsAcknowledgement{RequestID: "gcs-ha-ack-1", Slot: 1, Rows: `[[1,"gcs-dead-pod-baseline"]]`},
		PostRecoveryAck: gcsAcknowledgement{RequestID: "gcs-ha-ack-2", Slot: 2, Rows: `[[2,"gcs-dead-pod-post-recovery"]]`},
		ActiveRestart: gcsDeadPodRestartView{PodUID: "new-pod", NodeID: "new-node", WALIdentity: "new-wal",
			PreviousContainer: "old-container", CurrentContainer: "new-container", PreviousRun: "old-run", CurrentRun: "new-run",
			Membership: network.MembershipStatus{ClusterID: canary, NodeID: "new-node", WALIdentity: "new-wal", Voting: true,
				Voters: []quepaxa.NodeID{"new-node", "node-1", "node-2"}}},
		PostRestartAck:  gcsAcknowledgement{RequestID: "gcs-ha-ack-3", Slot: 3, Rows: `[[3,"gcs-dead-pod-post-restart"]]`},
		PostRestartRows: json.RawMessage(`[[1,"gcs-dead-pod-baseline"],[2,"gcs-dead-pod-post-recovery"],[3,"gcs-dead-pod-post-restart"]]`),
	}
	eventNames := map[string]bool{"old_logical_identity_closed": true, "post_recovery_ack_durable": true, "same_ordinal_active_restart_verified": true}
	if err := validateGCSDeadPodPrivateEvidence(evidence, eventNames); err != nil {
		t.Fatalf("actual collector-shaped evidence was rejected: %v", err)
	}
	wrongFence := evidence
	wrongFence.FenceRequest.SourceClusterID = "other-source"
	if validateGCSDeadPodPrivateEvidence(wrongFence, eventNames) == nil {
		t.Fatal("accepted a fence request bound to a different source")
	}
	otherVoterRequest := evidence
	otherVoterRequest.FenceRequest.Targets = []operator.FenceTarget{{Pod: "rhiza-1", PodUID: "survivor-1", NodeID: "node-1", WALIdentity: "wal-1"}}
	if validateGCSDeadPodPrivateEvidence(otherVoterRequest, eventNames) == nil {
		t.Fatal("accepted a voter fence request for an incarnation other than the dead Pod")
	}
	missingOldTarget := evidence
	missingOldTarget.Source.Targets = []operator.FenceTarget{
		{Pod: "rhiza-0", PodUID: "new-pod", NodeID: "new-node", WALIdentity: "new-wal"},
		evidence.Source.Targets[1], evidence.Source.Targets[2],
	}
	if validateGCSDeadPodPrivateEvidence(missingOldTarget, eventNames) == nil {
		t.Fatal("accepted source targets that omit the captured old incarnation")
	}
	missingAdd := evidence
	missingAdd.Recovery.Status.Membership = nil
	if validateGCSDeadPodPrivateEvidence(missingAdd, eventNames) == nil {
		t.Fatal("accepted recovery evidence without the durable replacement ADD")
	}
	write(gcsDeadPodEvidenceName, encoded(evidence))
	write(gcsDeadPodEventsName, []byte(
		`{"event":"old_logical_identity_closed","data":{"identity":"`+canary+`"}}`+"\n"+
			`{"event":"post_recovery_ack_durable","data":{"bucket":"`+canary+`"}}`+"\n"+
			`{"event":"same_ordinal_active_restart_verified","data":{"podUID":"`+canary+`"}}`+"\n"))
	write("same-ordinal-authenticated-binding.json", []byte(canary))

	if err := safeArtifacts(raw); err != nil {
		t.Fatalf("dead-Pod safe artifact projection failed: %v", err)
	}
	public := filepath.Join(raw, "public")
	summaryBytes, err := os.ReadFile(filepath.Join(public, gcsDeadPodSummaryName))
	if err != nil {
		t.Fatal(err)
	}
	var summary gcsDeadPodPublicSummary
	if err := json.Unmarshal(summaryBytes, &summary); err != nil {
		t.Fatal(err)
	}
	if summary.Outcome != "passed" || summary.Qualification != "gcs-single-voter-dead-pod" || !summary.SameOrdinalReplacementVerified ||
		!summary.ClosedIdentityExcluded || !summary.PostRecoveryAckReadbackVerified ||
		!summary.ActiveChildRestartVerified || !summary.PostRestartAckReadbackVerified ||
		summary.CapturedVoterCount != 3 || summary.Provider != "gcs" {
		t.Fatalf("public dead-Pod summary omitted validated assertions: %+v", summary)
	}
	if summary.PrivateEvidenceSHA256 == "" || summary.PrivateEventsSHA256 == "" {
		t.Fatal("public dead-Pod summary omitted private evidence checksums")
	}
	for _, receipt := range []string{"actual-exit.txt", "source-head.txt", "image-ids.txt"} {
		if _, err := os.Stat(filepath.Join(public, receipt)); err != nil {
			t.Fatalf("public packet omitted source/image/exit receipt %s: %v", receipt, err)
		}
	}
	imageIDs, err := os.ReadFile(filepath.Join(public, "image-ids.txt"))
	if err != nil || string(imageIDs) != "sha256:"+strings.Repeat("b", 64)+"\nsha256:"+strings.Repeat("c", 64)+"\n" {
		t.Fatalf("public immutable image receipt differs from source selection: %q, %v", imageIDs, err)
	}
	if _, err := os.Stat(filepath.Join(public, gcsDeadPodEvidenceName)); !os.IsNotExist(err) {
		t.Fatalf("private dead-Pod evidence was copied into public packet: %v", err)
	}
	publicEntries, err := os.ReadDir(public)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range publicEntries {
		data, err := os.ReadFile(filepath.Join(public, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(data, []byte(canary)) {
			t.Fatalf("public artifact %s leaked a private identity canary", entry.Name())
		}
	}
}

func TestManualTerminalDriverIsDefaultOff(t *testing.T) {
	t.Setenv("RHIZA_GCS_MANUAL_TERMINAL", "no")
	if err := qualifyGCSManualTerminal(context.Background()); err == nil || !strings.Contains(err.Error(), "default-off") {
		t.Fatalf("manual-terminal driver did not fail closed before external access: %v", err)
	}
}

func TestScratchAcceptsAPIOmittedFalsePIDOptionsOnly(t *testing.T) {
	// Matches API serialization: default-false hostPID is omitted, not emitted
	// as false; all nondefault security and scratch-volume fields are retained.
	apiJSON := `{"spec":{"template":{"spec":{"containers":[{"securityContext":{"readOnlyRootFilesystem":true,"allowPrivilegeEscalation":false},"volumeMounts":[{"name":"temporary","mountPath":"/tmp"}]}],"volumes":[{"name":"temporary","emptyDir":{}}]}}}}`
	var sts obj
	if err := json.Unmarshal([]byte(apiJSON), &sts); err != nil {
		t.Fatal(err)
	}
	if err := verifyScratch(sts); err != nil {
		t.Fatalf("API-omitted false PID options rejected: %v", err)
	}
	spec := nested(sts, "spec", "template", "spec").(obj)
	for _, key := range []string{"hostPID", "shareProcessNamespace"} {
		spec[key] = false
		if err := verifyScratch(sts); err != nil {
			t.Fatalf("explicit false %s rejected: %v", key, err)
		}
		for _, malformed := range []any{true, "false", float64(0), nil, []any{}} {
			spec[key] = malformed
			if verifyScratch(sts) == nil {
				t.Fatalf("accepted enabled/malformed %s=%#v", key, malformed)
			}
		}
		delete(spec, key)
	}
	containers := spec["containers"].([]any)
	security := nested(containers[0], "securityContext").(obj)
	security["readOnlyRootFilesystem"] = false
	if verifyScratch(sts) == nil {
		t.Fatal("accepted writable root while tolerating omitted PID defaults")
	}
	security["readOnlyRootFilesystem"] = true
	delete(security, "allowPrivilegeEscalation")
	if verifyScratch(sts) == nil {
		t.Fatal("accepted missing explicit privilege-escalation guard")
	}
}

func TestObservationFaultMatchesOnlyOperatorToSelectedVoterHTTP(t *testing.T) {
	rule, err := observationRule("10.244.0.8", "10.244.0.9", "target-uid")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"OUTPUT", "-s", "10.244.0.8", "-d", "10.244.0.9", "-p", "tcp", "--dport", "8080", "-m", "comment", "--comment", "rhiza-fencing-e2e-target-uid", "-j", "DROP"}
	if string(encoded(rule)) != string(encoded(want)) {
		t.Fatalf("observation fault escaped exact operator/target HTTP tuple: %v", rule)
	}
	for _, pair := range [][2]string{{"", "10.244.0.9"}, {"10.244.0.8", "0.0.0.0/0"}, {"10.244.0.8", "10.244.0.8"}, {"127.0.0.1", "10.244.0.9"}, {"::1", "10.244.0.9"}} {
		if _, err := observationRule(pair[0], pair[1], "target-uid"); err == nil {
			t.Fatalf("accepted unsafe observation tuple %v", pair)
		}
	}
	if _, err := observationRule("10.244.0.8", "10.244.0.9", ""); err == nil {
		t.Fatal("accepted rule without owned target UID marker")
	}
	fault := observationFault{PID: "42", Rule: rule}
	insert, remove, check := fault.args("-I"), fault.args("-D"), fault.args("-C")
	if string(encoded(insert[8:])) != string(encoded(remove[8:])) || string(encoded(remove[8:])) != string(encoded(check[8:])) {
		t.Fatal("cleanup/check did not preserve the exact owned rule")
	}
}

func TestGenerationProofRejectsUnrelatedAuthorityBeforeReadingLedger(t *testing.T) {
	request := operator.FenceRequest{LogicalID: "fencing-e2e", BindingUID: "binding", Namespace: namespace, StatefulSet: "rhiza", StatefulSetUID: "sts", SourceClusterID: "chaos-source", OperationID: "operation", Scope: operator.ScopeGeneration}
	proof := operator.FenceProof{OperationID: request.OperationID, SourceClusterID: request.SourceClusterID, BindingUID: request.BindingUID, RequestHash: fmt.Sprintf("%x", sha256.Sum256(encoded(request))), ProofID: "cooperative-proof", ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true}
	for _, change := range []func(*operator.FenceRequest, *operator.FenceProof){
		func(r *operator.FenceRequest, _ *operator.FenceProof) { r.SourceClusterID = "other" },
		func(r *operator.FenceRequest, _ *operator.FenceProof) { r.StatefulSetUID = "other" },
		func(r *operator.FenceRequest, _ *operator.FenceProof) { r.BindingUID = "other" },
		func(_ *operator.FenceRequest, p *operator.FenceProof) { p.OperationID = "other" },
		func(_ *operator.FenceRequest, p *operator.FenceProof) { p.RequestHash = "other" },
		func(_ *operator.FenceRequest, p *operator.FenceProof) { p.StorageQuiesced = false },
	} {
		r, p := request, proof
		change(&r, &p)
		if err := verifyGenerationProof(context.Background(), obj{"spec": r, "status": obj{"proof": p}}, "sts", "binding", []string{"old"}, t.TempDir()); err == nil {
			t.Fatal("unrelated/incomplete proof accepted")
		}
	}
}

func TestOnlineMembershipMappingAndExactIncarnations(t *testing.T) {
	refs := []operator.VoterPod{{NodeID: "rhiza-0", Pod: "rhiza-0"}, {NodeID: "rhiza-1", Pod: "rhiza-1"}, {NodeID: "learner", Pod: "learner"}}
	if err := validateReplacementRefs(refs, "rhiza-2", "learner"); err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]operator.VoterPod{refs[:2], {refs[0], refs[1], {NodeID: "rhiza-2", Pod: "rhiza-2"}}, {refs[0], refs[0], refs[2]}, {refs[0], refs[1], {NodeID: "learner", Pod: "rhiza-1"}}} {
		if validateReplacementRefs(bad, "rhiza-2", "learner") == nil {
			t.Fatal("accepted missing, retired, duplicated or incorrectly mapped learner")
		}
	}
	status := network.MembershipStatus{NodeID: "learner", ClusterID: "chaos-source", ConfigID: 3, WALIdentity: "wal", Voting: true, Voters: []quepaxa.NodeID{"rhiza-1", "learner", "rhiza-0"}}
	if !membershipMatches(status, refs, "learner") {
		t.Fatal("reordered exact current membership rejected")
	}
	for _, change := range []func(*network.MembershipStatus){
		func(s *network.MembershipStatus) { s.Pending = true },
		func(s *network.MembershipStatus) { s.Voting = false },
		func(s *network.MembershipStatus) { s.WALIdentity = "" },
		func(s *network.MembershipStatus) { s.Voters = []quepaxa.NodeID{"rhiza-0", "rhiza-1", "rhiza-2"} },
		func(s *network.MembershipStatus) { s.Voters = []quepaxa.NodeID{"rhiza-0", "rhiza-0", "learner"} },
	} {
		bad := status
		change(&bad)
		if membershipMatches(bad, refs, "learner") {
			t.Fatal("accepted uncertified or wrong current membership")
		}
	}
	targets := []operator.FenceTarget{{NodeID: "n", Pod: "p", PodUID: "uid", WALIdentity: "wal"}, {NodeID: "learner", Pod: "learner", PodUID: "learner-uid", WALIdentity: "learner-wal"}}
	if !sameTargets([]operator.FenceTarget{targets[1], targets[0]}, targets) {
		t.Fatal("exact reordered targets rejected")
	}
	stale := append([]operator.FenceTarget(nil), targets...)
	stale[1].PodUID = "stale"
	if sameTargets(stale, targets) || sameTargets(targets[:1], targets) {
		t.Fatal("accepted missing learner or stale Pod UID")
	}
}

func TestGenerationRetainsImmutableEarlierVoterStop(t *testing.T) {
	identity := fencing.Identity{SourceClusterID: "chaos-source", NodeID: "rhiza-2", PodUID: "old", RunID: "run", WALIdentity: "wal"}
	stop := fencing.StopRequest{OperationID: "online", RequestHash: "online-hash", Identity: identity}
	run := fencing.Run{Identity: identity, Stop: &stop, Drain: &fencing.DrainReceipt{Request: stop, WritersJoined: true}, Exited: true}
	prior := fenceLedgerState{StatefulSetUID: "sts", Ledger: fencing.Ledger{SourceClusterID: "chaos-source", Runs: map[string]fencing.Run{"run": run}}}
	out := t.TempDir()
	if err := os.WriteFile(out+"/validated-voter-proof.json", encoded(obj{"request": operator.FenceRequest{OperationID: "online"}, "ledger": prior}), 0600); err != nil {
		t.Fatal(err)
	}
	current := prior.Ledger
	current.ClosedBy, current.ClosedHash = "generation", "generation-hash"
	if err := verifyPriorVoterStops(out, current); err != nil {
		t.Fatal(err)
	}
	changed := run
	changed.Exited = false
	current.Runs["run"] = changed
	if verifyPriorVoterStops(out, current) == nil {
		t.Fatal("generation accepted altered earlier voter-stop evidence")
	}
}

func TestVoterProofRejectsWrongIncarnationBeforeLedgerRead(t *testing.T) {
	target := operator.FenceTarget{NodeID: "rhiza-2", Pod: "rhiza-2", PodUID: "live-uid", WALIdentity: "live-wal"}
	request := operator.FenceRequest{LogicalID: "fencing-e2e", BindingUID: "binding", Namespace: namespace, StatefulSet: "rhiza", StatefulSetUID: "sts", SourceClusterID: "chaos-source", OperationID: "online", Scope: operator.ScopeVoter, Targets: []operator.FenceTarget{target}}
	request.Targets[0].PodUID = "stale-uid"
	proof := operator.FenceProof{OperationID: request.OperationID, SourceClusterID: request.SourceClusterID, BindingUID: request.BindingUID, RequestHash: fmt.Sprintf("%x", sha256.Sum256(encoded(request))), ProofID: "cooperative-proof", ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true}
	if verifyScopedProof(context.Background(), obj{"spec": request, "status": obj{"proof": proof}}, "sts", "binding", []string{target.PodUID}, operator.ScopeVoter, []operator.FenceTarget{target}, t.TempDir()) == nil {
		t.Fatal("voter proof accepted stale target UID")
	}
}

func TestHistoricalVoterProofAllowsLaterLearnerButRejectsTargetTampering(t *testing.T) {
	target := operator.FenceTarget{NodeID: "rhiza-2", Pod: "rhiza-2", PodUID: "old-uid", WALIdentity: "old-wal"}
	request := operator.FenceRequest{StatefulSetUID: "sts", SourceClusterID: "chaos-source", BindingUID: "binding", OperationID: "online", Scope: operator.ScopeVoter, Targets: []operator.FenceTarget{target}}
	hash := fmt.Sprintf("%x", sha256.Sum256(encoded(request)))
	identity := fencing.Identity{SourceClusterID: request.SourceClusterID, NodeID: target.NodeID, PodUID: target.PodUID, RunID: "old-run", WALIdentity: target.WALIdentity}
	stop := fencing.StopRequest{OperationID: request.OperationID, RequestHash: hash, Identity: identity}
	run := fencing.Run{Identity: identity, Stop: &stop, Drain: &fencing.DrainReceipt{Request: stop, WritersJoined: true}, Exited: true}
	state := fenceLedgerState{StatefulSetUID: "sts", Ledger: fencing.Ledger{SourceClusterID: request.SourceClusterID, ClosedNodes: map[string]fencing.StopRequest{target.NodeID: {OperationID: request.OperationID, RequestHash: hash}}, Runs: map[string]fencing.Run{"old-run": run}}}
	proof := operator.FenceProof{OperationID: request.OperationID, SourceClusterID: request.SourceClusterID, BindingUID: request.BindingUID, RequestHash: hash, ProofID: "cooperative-" + fmt.Sprintf("%x", sha256.Sum256(encoded(state))), ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true}
	state.Ledger.Runs["learner-run"] = fencing.Run{Identity: fencing.Identity{SourceClusterID: request.SourceClusterID, NodeID: "learner", PodUID: "learner-uid", RunID: "learner-run", WALIdentity: "learner-wal"}}
	if proof.ProofID == "cooperative-"+fmt.Sprintf("%x", sha256.Sum256(encoded(state))) {
		t.Fatal("test did not advance the current ledger past proof issuance")
	}
	if err := verifyLedgerProof(state, request, proof); err != nil {
		t.Fatalf("unrelated later learner invalidated historical voter proof: %v", err)
	}
	for _, change := range []func(*fencing.Run){
		func(r *fencing.Run) { r.Identity.PodUID = "different-uid" },
		func(r *fencing.Run) { r.Identity.WALIdentity = "different-wal" },
		func(r *fencing.Run) { r.Stop.OperationID = "different-operation" },
		func(r *fencing.Run) { r.Stop.RequestHash = "different-hash" },
		func(r *fencing.Run) { r.Drain.UnknownMutations = 1 },
		func(r *fencing.Run) { r.Drain.PendingMutations = 1 },
		func(r *fencing.Run) { r.Drain.WritersJoined = false },
		func(r *fencing.Run) { r.Exited = false },
	} {
		var bad fencing.Run
		if err := json.Unmarshal(encoded(run), &bad); err != nil {
			t.Fatal(err)
		}
		change(&bad)
		state.Ledger.Runs["old-run"] = bad
		if verifyLedgerProof(state, request, proof) == nil {
			t.Fatal("historical proof accepted tampered terminal target evidence")
		}
	}
}

// The captured CI summaries omit raw bytes and attestation annotations. Rebuild
// the same graph shape so every traversed descriptor still needs its real hash.
func TestDocker29ImageConfigGraph(t *testing.T) {
	config := "sha256:78ec9d4f75a30d82997fb638f276b6b1b8ee311b05fd637784ea94f4788000a5"
	foreign := "sha256:" + strings.Repeat("b", 64)
	for _, tc := range []struct {
		name                                 string
		foreign, ambiguous, corrupt, missing bool
	}{
		{name: "index ID and running import resolve to config"},
		{name: "foreign config", foreign: true},
		{name: "ambiguous platform", ambiguous: true},
		{name: "digest mismatch", corrupt: true},
		{name: "missing immutable content", missing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			content := map[string][]byte{}
			store := func(body string) string {
				d := fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(body)))
				content[d] = []byte(body)
				return d
			}
			manifest := store(`{"config":{"digest":"` + config + `"}}`)
			runnable := `{"digest":"` + manifest + `","size":866,"mediaType":"application/vnd.oci.image.manifest.v1+json","platform":{"os":"linux","architecture":"amd64"}}`
			linuxAMD64 := imagePlatform{OS: "linux", Architecture: "amd64"}
			attestation := `{"digest":"sha256:531ca7d0ffd5ffce13835fcef138be1e7794b9a9931aef3bb234336a56dad845","size":564,"mediaType":"application/vnd.oci.image.manifest.v1+json","platform":{"os":"unknown","architecture":"unknown"},"annotations":{"vnd.docker.reference.type":"attestation-manifest","vnd.docker.reference.digest":"` + manifest + `"}}`
			if tc.ambiguous {
				otherManifest := store(`{"config":{"digest":"` + foreign + `"}}`)
				attestation = `{"digest":"` + otherManifest + `","size":866,"mediaType":"application/vnd.oci.image.manifest.v1+json","platform":{"os":"linux","architecture":"amd64"}}`
			}
			builtID := store(`{"manifests":[` + runnable + `,` + attestation + `]}`)
			alias := `{"digest":"` + builtID + `","size":855,"mediaType":"application/vnd.oci.image.index.v1+json","platform":{"os":"linux","architecture":"amd64"}}`
			importID := store(`{"manifests":[` + alias + `,` + alias + `]}`)
			if tc.corrupt {
				content[builtID] = []byte(`{}`)
			}
			if tc.missing {
				delete(content, builtID)
			}
			run := func(_ context.Context, name string, args ...string) ([]byte, error) {
				d := args[len(args)-1]
				if name != "docker" {
					t.Fatalf("unexpected command %s", name)
				}
				if args[2] == "crictl" {
					if d == config || d == foreign {
						return encoded(obj{"status": obj{"id": d}}), nil
					}
					return nil, fmt.Errorf("captured CRI lookup failed")
				}
				if data, ok := content[d]; ok {
					return data, nil
				}
				return nil, fmt.Errorf("content unavailable")
			}
			running, _, runningErr := resolveImageConfig(context.Background(), "docker.io/library/import-2026-10-26@"+importID, linuxAMD64, run)
			if tc.foreign {
				builtID = store(`{"config":{"digest":"` + foreign + `"}}`)
			}
			built, _, builtErr := resolveImageConfig(context.Background(), builtID, linuxAMD64, run)
			matches := runningErr == nil && builtErr == nil && nested(running, "status", "id") == nested(built, "status", "id")
			if matches != (!tc.foreign && !tc.ambiguous && !tc.corrupt && !tc.missing) {
				t.Fatalf("matches=%v; running=%v (%v), built=%v (%v)", matches, running, runningErr, built, builtErr)
			}
			if matches {
				for _, reference := range []string{manifest, config} {
					cri, _, err := resolveImageConfig(context.Background(), reference, linuxAMD64, run)
					if err != nil || nested(cri, "status", "id") != config {
						t.Fatalf("manifest/config ID %s: %v, %v", reference, cri, err)
					}
				}
				for _, reference := range []string{"rhiza-fencing-host:local", "sha256:" + strings.Repeat("d", 64)} {
					if _, _, err := resolveImageConfig(context.Background(), reference, linuxAMD64, run); err == nil {
						t.Fatalf("accepted reference without authoritative immutable content: %s", reference)
					}
				}
			}
		})
	}
}
