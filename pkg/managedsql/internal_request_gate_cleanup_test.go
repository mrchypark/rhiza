package managedsql

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"
)

var childTestLog = regexp.MustCompile(`(?m)^\s+[^\s:]+\.go:\d+:.*$`)

func onlyExpectedGateFailure(output, marker string) bool {
	lines := childTestLog.FindAllString(output, -1)
	return len(lines) == 1 && strings.Contains(lines[0], marker) && !strings.Contains(output, "panic:") && !strings.Contains(output, "DATA RACE")
}

func TestInternalRequestGateFailureOutputRejectsExtraErrors(t *testing.T) {
	good := "    a_test.go:1: intended failure\n"
	if !onlyExpectedGateFailure(good, "intended failure") || onlyExpectedGateFailure(good+"    host_test.go:314: unexpected EOF\n", "intended failure") {
		t.Fatal("child output checker missed an extra test error")
	}
}

// Run a real partial-body gate in a subprocess: t.Fatal must execute the
// fallback release before HTTP server cleanup rather than hang cleanup.
func TestInternalRequestPartialGateFailureUnblocksCleanup(t *testing.T) {
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, test, marker string }{
		{"checkpoint", "TestInternalRequestCheckpointAttemptSettlesBeforeClose", "intentional failure after checkpoint partial-body gate"},
		{"overlap", "TestInternalRequestIndependentPUTCancelOverlapsClose", "intentional failure after overlap partial-body gate"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, binary, "-test.run=^"+tc.test+"$", "-test.timeout=0")
			cmd.Env = append(os.Environ(), "RHIZA_TEST_GATE_FAILURE="+tc.name)
			output, err := cmd.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("failure cleanup did not finish before outer deadline: %v\n%s", ctx.Err(), output)
			}
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 1 {
				t.Fatalf("child exit=%v, want test failure code 1\n%s", err, output)
			}
			if !onlyExpectedGateFailure(string(output), tc.marker) {
				t.Fatalf("child did not report exactly the intended gate failure\n%s", output)
			}
		})
	}
}
