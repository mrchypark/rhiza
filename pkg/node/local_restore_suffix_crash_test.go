package node

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
)

type localRestoreSuffixCrashRequest struct {
	DataDir         string
	ReadyFile       string
	FirstSuffixSlot uint64
}

func TestLocalRestoreJournalFinalizedBeforeSuffixAndRestartReplaysAfterHardKill(t *testing.T) {
	ctx := context.Background()
	config := types.ExecutionConfig{Local: true, NodeID: "local", DataDir: t.TempDir()}
	n := New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := n.server.Execute(ctx, network.ExecuteRequest{RequestID: "suffix-schema", SQL: "CREATE TABLE local_restore_suffix (id TEXT PRIMARY KEY)"}); err != nil {
		_ = n.Shutdown()
		t.Fatal(err)
	}
	if _, err := n.server.Execute(ctx, network.ExecuteRequest{RequestID: "suffix-base", SQL: "INSERT INTO local_restore_suffix VALUES ('base')"}); err != nil {
		_ = n.Shutdown()
		t.Fatal(err)
	}
	if err := n.reclaimLocalCheckpoint(ctx); err != nil {
		_ = n.Shutdown()
		t.Fatalf("create exact Local checkpoint: %v", err)
	}
	floor := uint64(n.core.CompactionFloor())
	if floor == 0 {
		_ = n.Shutdown()
		t.Fatal("Local checkpoint did not advance recovery floor")
	}
	if _, err := n.server.Execute(ctx, network.ExecuteRequest{RequestID: "suffix-after-base", SQL: "INSERT INTO local_restore_suffix VALUES ('suffix')"}); err != nil {
		_ = n.Shutdown()
		t.Fatal(err)
	}
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}

	readyFile := filepath.Join(t.TempDir(), "after-first-suffix-apply")
	payload, err := json.Marshal(localRestoreSuffixCrashRequest{DataDir: config.DataDir, ReadyFile: readyFile, FirstSuffixSlot: floor + 1})
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestLocalRestoreSuffixHardKillHelper$")
	cmd.Env = append(os.Environ(), "RHIZA_LOCAL_RESTORE_SUFFIX_KILL="+string(payload))
	startLocalNodeCrashChild(t, cmd, readyFile)
	waitForLocalNodeCrashBoundary(t, cmd, readyFile)
	if _, err := os.Lstat(filepath.Join(config.DataDir, "sqlite.db.local-restore.json")); !os.IsNotExist(err) {
		t.Fatalf("restore journal remains after first suffix application: %v", err)
	}

	n = New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatalf("restart from exact base and replay full suffix: %v", err)
	}
	defer n.Shutdown()
	result, err := n.server.Query(ctx, network.QueryRequest{SQL: "SELECT id FROM local_restore_suffix ORDER BY id"})
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, row := range result.Rows {
		if len(row) != 1 {
			t.Fatalf("row=%v, want one column", row)
		}
		counts[fmt.Sprint(row[0])]++
	}
	if len(result.Rows) != 2 || counts["base"] != 1 || counts["suffix"] != 1 {
		t.Fatalf("restored SQL rows=%v counts=%v, want base and suffix exactly once", result.Rows, counts)
	}
}

func TestLocalRestoreSuffixHardKillHelper(t *testing.T) {
	data := os.Getenv("RHIZA_LOCAL_RESTORE_SUFFIX_KILL")
	if data == "" {
		return
	}
	var request localRestoreSuffixCrashRequest
	if err := json.Unmarshal([]byte(data), &request); err != nil {
		t.Fatal(err)
	}
	localRestoreSuffixCrashBoundary = func(slot uint64) {
		if slot != request.FirstSuffixSlot {
			return
		}
		if err := os.WriteFile(request.ReadyFile, []byte(fmt.Sprint(slot)), 0o600); err != nil {
			t.Fatalf("write suffix crash marker: %v", err)
		}
		for {
			time.Sleep(time.Hour)
		}
	}
	n := New(&types.ExecutionConfig{Local: true, NodeID: "local", DataDir: request.DataDir})
	if err := n.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Fatal("requested first-suffix hard-kill boundary was not reached")
}
