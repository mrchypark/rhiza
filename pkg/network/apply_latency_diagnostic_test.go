//go:build rhiza_latency_diag && rhiza_local_testhooks

package network

import (
	"context"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/latencydiag"
	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestTaggedApplyTraceSeparatesLockAndMaterialization(t *testing.T) {
	const child = "RHIZA_APPLY_TRACE_CHILD"
	if os.Getenv(child) == "1" {
		latencydiag.DisableBackgroundWriterForTest()
		latencydiag.SetLosslessForTest(true)
		TestConcurrentApplyDecisionsRemainOrdered(t)
		latencydiag.Flush()
		testApplyTraceLockAndMultipleBatches(t)
		latencydiag.Flush()
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestTaggedApplyTraceSeparatesLockAndMaterialization$")
	cmd.Env = append(os.Environ(), child+"=1", "RHIZA_LATENCY_SAMPLE_SLOTS=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("trace fixture: %v\n%s", err, out)
	}
	for _, stage := range []string{"apply_target_lock", "apply_target_wait_tip", "apply_target_material_batch"} {
		if !strings.Contains(string(out), "stage="+stage+" batch=0 slot=64 ") {
			t.Errorf("missing sampled stage %s", stage)
		}
	}
	lockObserved := false
	for _, line := range strings.Split(string(out), "\n") {
		if !strings.Contains(line, "stage=apply_target_lock batch=0 slot=272 ") {
			continue
		}
		for _, field := range strings.Fields(line) {
			if value, ok := strings.CutPrefix(field, "duration_ns="); ok {
				ns, err := strconv.ParseInt(value, 10, 64)
				if err == nil && ns > 0 {
					lockObserved = true
				}
			}
		}
	}
	if !lockObserved {
		t.Error("missing positive lock duration for target 272")
	}
	if strings.Count(string(out), "stage=apply_target_material_batch batch=0 slot=272 ") != 2 {
		t.Error("expected two actual material batches for target 272")
	}
	if !strings.Contains(string(out), "stage=apply_target_wait_tip batch=0 slot=1048576 count=0 peer=0000000000000000 outcome=error ") {
		t.Error("missing canceled wait-tip outcome for absent target")
	}
}

func testApplyTraceLockAndMultipleBatches(t *testing.T) {
	wal, err := qlog.Open(t.TempDir() + "/qlog")
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}}, wal, nil)
	material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	for i := 0; i < 272; i++ {
		statement := "INSERT INTO diag_apply VALUES (1)"
		if i == 0 {
			statement = "CREATE TABLE diag_apply (id INTEGER)"
		}
		if _, _, err := core.Propose(context.Background(), policySQL(t, statement)); err != nil {
			t.Fatal(err)
		}
	}
	latencydiag.Flush()
	server := NewServer(core, material, "cluster", true, nil)
	entered := make(chan struct{})
	acquired := make(chan struct{})
	lockWasFree := make(chan bool, 1)
	var once sync.Once
	var acquiredOnce sync.Once
	restore := localtesthooks.Set(func(name string) {
		if name == "network:apply:before-lock" {
			once.Do(func() {
				free := server.applyMu.TryLock()
				if free {
					server.applyMu.Unlock()
				}
				lockWasFree <- free
				close(entered)
			})
		}
		if name == "network:apply:after-lock" {
			acquiredOnce.Do(func() { close(acquired) })
		}
	})
	server.applyMu.Lock()
	done := make(chan error, 1)
	go func() { done <- server.applyDecisions(context.Background(), 272) }()
	select {
	case <-entered:
		if <-lockWasFree {
			server.applyMu.Unlock()
			restore()
			t.Fatal("apply reached an unheld lock")
		}
	case <-time.After(5 * time.Second):
		server.applyMu.Unlock()
		restore()
		t.Fatal("apply did not reach held lock")
	}
	select {
	case <-acquired:
		server.applyMu.Unlock()
		restore()
		t.Fatal("apply acquired a held lock")
	default:
	}
	server.applyMu.Unlock()
	if err := <-done; err != nil {
		restore()
		t.Fatal(err)
	}
	select {
	case <-acquired:
	default:
		restore()
		t.Fatal("apply did not report lock acquisition")
	}
	restore()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := server.applyDecisions(ctx, 1048576); err == nil {
		t.Fatal("missing cancellation error")
	}
}
