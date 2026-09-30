package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
)

const localCrashWALLimit = int64(2 << 20)
const localCrashPayloadBytes = 64 << 10

type localReclamationCrashRequest struct {
	DataDir      string
	ReadyFile    string
	ProgressFile string
}

func TestLocalReclamationRecoversAfterHardKillBeforePrepare(t *testing.T) {
	dataDir := t.TempDir()
	readyFile := filepath.Join(t.TempDir(), "at-boundary")
	progressFile := filepath.Join(t.TempDir(), "child-progress")
	payload, err := json.Marshal(localReclamationCrashRequest{DataDir: dataDir, ReadyFile: readyFile, ProgressFile: progressFile})
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestLocalReclamationHardKillHelper$")
	cmd.Env = append(os.Environ(), "RHIZA_LOCAL_RECLAMATION_CRASH="+string(payload))
	startLocalNodeCrashChild(t, cmd, readyFile)
	waitForLocalNodeCrashBoundary(t, cmd, readyFile, progressFile)
	if marker, err := os.ReadFile(readyFile); err != nil || string(marker) != "after-checkpoint-root-durable-before-prepare" {
		t.Fatalf("crash boundary marker=%q err=%v, want complete boundary name", marker, err)
	}

	config := types.ExecutionConfig{Local: true, NodeID: "local", DataDir: dataDir, MaxWALBytes: localCrashWALLimit}
	n := New(&config)
	if err := n.Open(context.Background()); err != nil {
		t.Fatalf("reopen after hard kill following durable snapshot publication: %v", err)
	}
	defer n.Shutdown()
	if got := n.wal.Capacity().Limit; got != localCrashWALLimit {
		t.Fatalf("reopened WAL limit=%d, want unchanged fixed limit %d", got, localCrashWALLimit)
	}
	if floor := n.core.CompactionFloor(); floor != 0 {
		t.Fatalf("floor after kill before prepare=%d, want 0", floor)
	}
	if index, root, prepared := n.core.LatestPreparedCheckpoint(); prepared {
		t.Fatalf("kill before prepare left prepared checkpoint index=%d root=%x", index, root)
	}
	roots, err := os.ReadDir(n.localStore.rootDir)
	if err != nil || len(roots) == 0 {
		t.Fatalf("durable orphan snapshot directory entries=%v err=%v, want published root", roots, err)
	}
	published := false
	for _, root := range roots {
		if !root.IsDir() {
			continue
		}
		files, readErr := os.ReadDir(filepath.Join(n.localStore.rootDir, root.Name()))
		if readErr != nil {
			continue
		}
		names := make(map[string]bool, len(files))
		for _, file := range files {
			names[file.Name()] = true
		}
		if names["descriptor.json"] && names["sqlite.db"] && names["graph.ltdb"] {
			published = true
			break
		}
	}
	if !published {
		t.Fatalf("no complete immutable checkpoint root found after published-before-prepare kill: entries=%v", roots)
	}
	if used := n.wal.Bytes(); used > localCrashWALLimit {
		t.Fatalf("reopened WAL bytes=%d exceed fixed limit %d", used, localCrashWALLimit)
	}

	for pair := range 4 {
		id := fmt.Sprintf("cycle-0-item-%d", pair)
		assertLocalCrashReceipt(t, n.server, "sql", "sql-"+id)
		assertLocalCrashReceipt(t, n.server, "graph", "graph-"+id)
	}
	assertLocalCrashSQLRows(t, n.server, 4)
	assertLocalCrashGraphEffects(t, n.server, 4)
	status, err := n.server.RequestStatus(context.Background(), network.RequestStatusRequest{Kind: "sql", RequestID: "sql-cycle-0-item-4"})
	if err != nil || status.State != "unknown_or_expired" || status.Receipt != nil {
		t.Fatalf("in-flight prewrite request status=%+v err=%v, want no committed receipt", status, err)
	}

	// The triggering operation was refused before its first durable WAL write.
	// Reissuing that same request after reopen must create exactly one row.
	receipt, err := n.server.Execute(context.Background(), network.ExecuteRequest{
		RequestID: "sql-cycle-0-item-4",
		SQL:       "INSERT INTO local_reclaim_rows (id, payload) VALUES (?, ?)",
		Args:      []any{"cycle-0-item-4", strings.Repeat("x", localCrashPayloadBytes)},
	})
	if err != nil || receipt.Status != types.MutationCommitted || receipt.Slot == 0 {
		t.Fatalf("retry exact refused request receipt=%+v err=%v", receipt.MutationReceipt, err)
	}
	assertLocalCrashSQLRows(t, n.server, 5)
	assertLocalCrashGraphEffects(t, n.server, 4)
	assertLocalCrashReceipt(t, n.server, "sql", "sql-cycle-0-item-4")
}

func TestLocalReclamationHardKillHelper(t *testing.T) {
	data := os.Getenv("RHIZA_LOCAL_RECLAMATION_CRASH")
	if data == "" {
		return
	}
	var request localReclamationCrashRequest
	if err := json.Unmarshal([]byte(data), &request); err != nil {
		t.Fatal(err)
	}
	config := types.ExecutionConfig{Local: true, NodeID: "local", DataDir: request.DataDir, MaxWALBytes: localCrashWALLimit}
	n := New(&config)
	writeLocalCrashProgress(t, request.ProgressFile, "opening")
	if err := n.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	writeLocalCrashProgress(t, request.ProgressFile, "opened")
	if _, err := n.server.Execute(context.Background(), network.ExecuteRequest{
		RequestID: "reclaim-schema",
		SQL:       "CREATE TABLE local_reclaim_rows (id TEXT PRIMARY KEY, payload TEXT NOT NULL)",
	}); err != nil {
		t.Fatal(err)
	}
	writeLocalCrashProgress(t, request.ProgressFile, "schema-committed")
	payload := strings.Repeat("x", localCrashPayloadBytes)
	for pair := range 4 {
		id := fmt.Sprintf("cycle-0-item-%d", pair)
		if _, err := n.server.Execute(context.Background(), network.ExecuteRequest{
			RequestID: "sql-" + id,
			SQL:       "INSERT INTO local_reclaim_rows (id, payload) VALUES (?, ?)",
			Args:      []any{id, payload},
		}); err != nil {
			t.Fatalf("seed SQL pair %d: %v", pair, err)
		}
		writeLocalCrashProgress(t, request.ProgressFile, fmt.Sprintf("seed-%d-sql", pair))
		if _, err := n.server.GraphExecute(context.Background(), types.GraphCommand{
			RequestID: "graph-" + id,
			Cypher:    fmt.Sprintf("CREATE (:LocalReclaimCycle {id: '%s'})", id),
			Events:    []types.GraphStreamEvent{{Stream: "local-reclaim-events", Kind: "created", Payload: id}},
		}); err != nil {
			t.Fatalf("seed graph pair %d: %v", pair, err)
		}
		writeLocalCrashProgress(t, request.ProgressFile, fmt.Sprintf("seed-%d-graph", pair))
	}
	localNodeCheckpointCrashBoundary = func(name string) {
		if name == "after-checkpoint-root-durable-before-prepare" {
			writeLocalCrashProgress(t, request.ProgressFile, "checkpoint-root-published")
			if err := publishLocalCrashMarker(request.ReadyFile, name); err != nil {
				t.Fatalf("write crash boundary marker: %v", err)
			}
			for {
				time.Sleep(time.Hour)
			}
		}
	}
	writeLocalCrashProgress(t, request.ProgressFile, "triggering-fifth-sql")
	_, err := n.server.Execute(context.Background(), network.ExecuteRequest{
		RequestID: "sql-cycle-0-item-4",
		SQL:       "INSERT INTO local_reclaim_rows (id, payload) VALUES (?, ?)",
		Args:      []any{"cycle-0-item-4", payload},
	})
	t.Fatalf("expected hard-kill hook before triggering SQL request returned; receipt=%+v err=%v", err, err)
}

func writeLocalCrashProgress(t *testing.T, path, phase string) {
	t.Helper()
	if path == "" {
		return
	}
	if err := os.WriteFile(path, []byte(phase), 0o600); err != nil {
		t.Fatalf("write child progress %q: %v", phase, err)
	}
}

func publishLocalCrashMarker(path, marker string) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".local-crash-marker-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err := tmp.WriteString(marker); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

func assertLocalCrashReceipt(t *testing.T, server *network.Server, kind, requestID string) {
	t.Helper()
	status, err := server.RequestStatus(context.Background(), network.RequestStatusRequest{Kind: kind, RequestID: requestID})
	if err != nil || status.State != string(types.MutationCommitted) || status.Receipt == nil || status.Receipt.Slot == 0 {
		t.Fatalf("%s receipt %q status=%+v err=%v", kind, requestID, status, err)
	}
}

func assertLocalCrashSQLRows(t *testing.T, server *network.Server, want int) {
	t.Helper()
	result, err := server.Query(context.Background(), network.QueryRequest{SQL: "SELECT id FROM local_reclaim_rows ORDER BY id"})
	if err != nil {
		t.Fatalf("query recovered SQL rows: %v", err)
	}
	if len(result.Rows) != want {
		t.Fatalf("recovered SQL rows=%d, want %d", len(result.Rows), want)
	}
	counts := make(map[string]int, want)
	for _, row := range result.Rows {
		if len(row) != 1 {
			t.Fatalf("SQL row=%v, want one id", row)
		}
		counts[fmt.Sprint(row[0])]++
	}
	for i := range want {
		id := fmt.Sprintf("cycle-0-item-%d", i)
		if counts[id] != 1 {
			t.Fatalf("SQL effect %q count=%d, want exactly once", id, counts[id])
		}
	}
}

func assertLocalCrashGraphEffects(t *testing.T, server *network.Server, want int) {
	t.Helper()
	result, err := server.GraphQuery(context.Background(), network.GraphQueryRequest{Cypher: "MATCH (n:LocalReclaimCycle) RETURN n.id"})
	if err != nil {
		t.Fatalf("query recovered graph nodes: %v", err)
	}
	if len(result.Rows) != want {
		t.Fatalf("recovered graph nodes=%d, want %d", len(result.Rows), want)
	}
	counts := make(map[string]int, want)
	for _, row := range result.Rows {
		if len(row) != 1 {
			t.Fatalf("graph row=%v, want one id", row)
		}
		counts[fmt.Sprint(row[0])]++
	}
	stream, err := server.GraphStreamRead(context.Background(), network.GraphStreamReadRequest{Stream: "local-reclaim-events", Limit: uint(want)})
	if err != nil || len(stream.Records) != want {
		t.Fatalf("recovered graph events=%d err=%v, want %d", len(stream.Records), err, want)
	}
	events := make(map[string]int, want)
	for _, record := range stream.Records {
		events[fmt.Sprint(record.Payload)]++
	}
	for i := range want {
		id := fmt.Sprintf("cycle-0-item-%d", i)
		if counts[id] != 1 || events[id] != 1 {
			t.Fatalf("graph effect %q node=%d event=%d, want exactly once", id, counts[id], events[id])
		}
	}
}

func TestLocalCrashWaitReportsChildExitBeforeMarker(t *testing.T) {
	readyFile := filepath.Join(t.TempDir(), "never-ready")
	progressFile := filepath.Join(t.TempDir(), "child-progress")
	cmd := exec.Command(os.Args[0], "-test.run=^TestLocalCrashEarlyExitHelper$")
	cmd.Env = append(os.Environ(), "RHIZA_LOCAL_CRASH_EARLY_EXIT=1", "RHIZA_LOCAL_CRASH_PROGRESS="+progressFile)
	startLocalNodeCrashChild(t, cmd, readyFile)
	err := awaitLocalNodeCrashBoundary(cmd, readyFile, progressFile, 15*time.Second)
	if err == nil || !strings.Contains(err.Error(), "exited before boundary") || !strings.Contains(err.Error(), "intentional-child-diagnostic") || !strings.Contains(err.Error(), "progress=\"before-marker\"") {
		t.Fatalf("early child exit diagnostic=%v, want exit status and captured output", err)
	}
}

func TestLocalCrashWaitReportsMissingMarkerAndJoinsChild(t *testing.T) {
	readyFile := filepath.Join(t.TempDir(), "never-ready")
	progressFile := filepath.Join(t.TempDir(), "child-progress")
	cmd := exec.Command(os.Args[0], "-test.run=^TestLocalCrashMissingMarkerHelper$")
	cmd.Env = append(os.Environ(), "RHIZA_LOCAL_CRASH_WAIT_FOR_MARKER=1", "RHIZA_LOCAL_CRASH_PROGRESS="+progressFile, "RHIZA_LOCAL_CRASH_READY="+readyFile+".child-ready")
	startLocalNodeCrashChild(t, cmd, readyFile)
	waited, err := awaitLocalCrashFile(cmd, readyFile+".child-ready", progressFile, 15*time.Second)
	if err != nil {
		t.Fatalf("wait for missing-marker child readiness: %v", err)
	}
	err = awaitLocalCrashFileWithWait(cmd, waited, readyFile, progressFile, 100*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "did not reach boundary signal") || !strings.Contains(err.Error(), "before 100ms") || !strings.Contains(err.Error(), "progress=\"waiting-for-marker\"") || !strings.Contains(err.Error(), "wait=signal: killed") {
		t.Fatalf("missing marker diagnostic=%v, want short timeout and child phase", err)
	}
}

func TestLocalCrashMissingMarkerHelper(t *testing.T) {
	if os.Getenv("RHIZA_LOCAL_CRASH_WAIT_FOR_MARKER") == "" {
		return
	}
	if progressFile := os.Getenv("RHIZA_LOCAL_CRASH_PROGRESS"); progressFile != "" {
		if err := os.WriteFile(progressFile, []byte("waiting-for-marker"), 0o600); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "write child progress:", err)
			os.Exit(8)
		}
	}
	if readyFile := os.Getenv("RHIZA_LOCAL_CRASH_READY"); readyFile != "" {
		if err := publishLocalCrashMarker(readyFile, "ready"); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "publish child readiness:", err)
			os.Exit(9)
		}
	}
	_, _ = fmt.Fprintln(os.Stdout, "child waiting for test marker")
	for {
		time.Sleep(time.Hour)
	}
}

func TestLocalCrashEarlyExitHelper(t *testing.T) {
	if os.Getenv("RHIZA_LOCAL_CRASH_EARLY_EXIT") == "" {
		return
	}
	if progressFile := os.Getenv("RHIZA_LOCAL_CRASH_PROGRESS"); progressFile != "" {
		if err := os.WriteFile(progressFile, []byte("before-marker"), 0o600); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "write child progress:", err)
			os.Exit(8)
		}
	}
	_, _ = fmt.Fprintln(os.Stdout, "intentional-child-diagnostic")
	os.Exit(7)
}

func startLocalNodeCrashChild(t *testing.T, cmd *exec.Cmd, readyFile string) {
	t.Helper()
	output, err := os.Create(readyFile + ".child.log")
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout, cmd.Stderr = output, output
	if err := cmd.Start(); err != nil {
		_ = output.Close()
		t.Fatal(err)
	}
	if err := output.Close(); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatal(err)
	}
}

func waitForLocalNodeCrashBoundary(t *testing.T, cmd *exec.Cmd, readyFile string, progressFiles ...string) {
	t.Helper()
	progressFile := ""
	if len(progressFiles) > 0 {
		progressFile = progressFiles[0]
	}
	if err := awaitLocalNodeCrashBoundary(cmd, readyFile, progressFile, 15*time.Second); err != nil {
		t.Fatal(err)
	}
}

func awaitLocalNodeCrashBoundary(cmd *exec.Cmd, readyFile, progressFile string, timeout time.Duration) error {
	waited, err := awaitLocalCrashFile(cmd, readyFile, progressFile, timeout)
	if err != nil {
		return err
	}
	if err := cmd.Process.Kill(); err != nil {
		waitErr := <-waited
		return fmt.Errorf("send hard process kill after boundary: %v; child=%v; %s", err, waitErr, localCrashChildDiagnostics(readyFile, progressFile))
	}
	err = <-waited
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != -1 {
		return fmt.Errorf("child termination=%v, want signal termination from Process.Kill; %s", err, localCrashChildDiagnostics(readyFile, progressFile))
	}
	return nil
}

func awaitLocalCrashFile(cmd *exec.Cmd, signalFile, progressFile string, timeout time.Duration) (<-chan error, error) {
	waited := make(chan error, 1)
	go func() { waited <- cmd.Wait() }()
	if err := awaitLocalCrashFileWithWait(cmd, waited, signalFile, progressFile, timeout); err != nil {
		return nil, err
	}
	return waited, nil
}

func awaitLocalCrashFileWithWait(cmd *exec.Cmd, waited <-chan error, signalFile, progressFile string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if _, err := os.Stat(signalFile); err == nil {
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			_ = cmd.Process.Kill()
			waitErr := <-waited
			return fmt.Errorf("check child signal %q: %v; child=%v; %s", signalFile, err, waitErr, localCrashChildDiagnostics(signalFile, progressFile))
		}
		select {
		case childErr := <-waited:
			return fmt.Errorf("child exited before boundary signal %q (wait=%v); %s", signalFile, childErr, localCrashChildDiagnostics(signalFile, progressFile))
		default:
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			waitErr := <-waited
			return fmt.Errorf("child did not reach boundary signal %q before %s (wait=%v); %s", signalFile, timeout, waitErr, localCrashChildDiagnostics(signalFile, progressFile))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func localCrashChildDiagnostics(readyFile, progressFile string) string {
	ready, _ := os.ReadFile(readyFile)
	progress, _ := os.ReadFile(progressFile)
	output, _ := os.ReadFile(readyFile + ".child.log")
	return fmt.Sprintf("ready=%q progress=%q child_output=%q", ready, progress, output)
}
