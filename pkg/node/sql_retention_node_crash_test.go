package node

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
)

func TestLocalRetentionNodeOpenWorkerSurvivesProcessKill(t *testing.T) {
	data := t.TempDir()
	marker := filepath.Join(t.TempDir(), "worker-completed")
	cmd := exec.Command(os.Args[0], "-test.run=^TestLocalRetentionNodeOpenCrashHelper$", "-test.timeout=4m")
	cmd.Env = append(os.Environ(), "RHIZA_RETENTION_CRASH_DATA="+data, "RHIZA_RETENTION_CRASH_MARKER="+marker)
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() { finished <- cmd.Wait() }()
	reaped := false
	defer func() {
		if !reaped {
			_ = cmd.Process.Kill()
			<-finished
		}
	}()
	deadline := time.After(3 * time.Minute)
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		select {
		case err := <-finished:
			reaped = true
			t.Fatalf("helper exited before marker: %v\n%s", err, output.String())
		case <-deadline:
			t.Fatalf("helper timed out before marker")
		case <-time.After(100 * time.Millisecond):
		}
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	err := <-finished
	reaped = true
	if err == nil {
		t.Fatal("helper unexpectedly exited successfully")
	}
	n := New(&types.ExecutionConfig{Local: true, NodeID: "local", DataDir: data})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := n.Open(ctx); err != nil {
		t.Fatalf("reopen after hard kill: %v", err)
	}
	defer n.Shutdown()
	for n.SQLRetentionBarriers().Completed == 0 {
		select {
		case <-ctx.Done():
			t.Fatalf("reopened Node.Open worker did not resume: %+v", n.SQLRetentionBarriers())
		case <-time.After(100 * time.Millisecond):
		}
	}
	result, err := n.server.Execute(ctx, localRetentionCrashRequest("rejected-retention"))
	if err != nil || result.Status != types.MutationRejected || result.ErrorCode != types.MutationErrorCodeResultRetentionFull {
		t.Fatalf("failed receipt after restart: %+v err=%v", result.MutationReceipt, err)
	}
	rows, err := n.server.Query(ctx, network.QueryRequest{SQL: "SELECT count FROM retained"})
	if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != int64(256) {
		t.Fatalf("acknowledged writes after restart: %+v err=%v", rows.Rows, err)
	}
}

func localRetentionCrashRequest(id string) network.ExecuteRequest {
	return network.ExecuteRequest{RequestID: id, SQL: "UPDATE retained SET count=count+1, payload=payload RETURNING payload", WantRows: true}
}

func TestLocalRetentionNodeOpenCrashHelper(t *testing.T) {
	data := os.Getenv("RHIZA_RETENTION_CRASH_DATA")
	if data == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	n := New(&types.ExecutionConfig{Local: true, NodeID: "local", DataDir: data})
	if err := n.Open(ctx); err != nil {
		t.Fatal(err)
	}
	defer n.Shutdown()
	execute := func(req network.ExecuteRequest) network.ExecuteResponse {
		t.Helper()
		result, err := n.server.Execute(ctx, req)
		if err != nil {
			t.Fatalf("%s: %v", req.RequestID, err)
		}
		return result
	}
	for _, req := range []network.ExecuteRequest{
		{RequestID: "create-retention", SQL: "CREATE TABLE retained (count INTEGER NOT NULL, payload BLOB NOT NULL)"},
		{RequestID: "insert-retention", SQL: "INSERT INTO retained VALUES (0, zeroblob(?))", Args: []any{materializer.MaxMutationResultBytes - 56}},
	} {
		if result := execute(req); result.Status != types.MutationCommitted {
			t.Fatalf("setup: %+v", result.MutationReceipt)
		}
	}
	for i := 0; i < 256; i++ {
		result := execute(localRetentionCrashRequest(fmt.Sprintf("result-%d", i)))
		if result.Status != types.MutationCommitted {
			t.Fatalf("fill %d: %+v", i, result.MutationReceipt)
		}
	}
	result := execute(localRetentionCrashRequest("rejected-retention"))
	if result.Status != types.MutationRejected || result.ErrorCode != types.MutationErrorCodeResultRetentionFull {
		t.Fatalf("expected capacity rejection: %+v", result.MutationReceipt)
	}
	for n.SQLRetentionBarriers().Completed == 0 {
		select {
		case <-ctx.Done():
			t.Fatalf("automatic Node.Open worker did not advance: %+v", n.SQLRetentionBarriers())
		case <-time.After(100 * time.Millisecond):
		}
	}
	if err := os.WriteFile(os.Getenv("RHIZA_RETENTION_CRASH_MARKER"), []byte("barrier complete"), 0o600); err != nil {
		t.Fatal(err)
	}
	<-ctx.Done()
}
