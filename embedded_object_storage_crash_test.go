package rhiza_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/mrchypark/rhiza"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

const embeddedCrashChildEnv = "RHIZA_EMBEDDED_CRASH_CHILD"

type embeddedCrashInput struct {
	Config  rhiza.Config
	AckFile string
}

func embeddedCrashRequest() rhiza.ExecuteRequest {
	return rhiza.ExecuteRequest{RequestID: "crash-ack", SQL: "INSERT INTO crash_items VALUES (7, 'acknowledged') RETURNING id, name"}
}

// The deployment has stopped its only writer before starting the replacement.
// No graceful Close, local WAL, or copied remote control can supply the restore.
func TestEmbeddedObjectStorageSIGKILLTotalDataDirLoss(t *testing.T) {
	if raw := os.Getenv(embeddedCrashChildEnv); raw != "" {
		runEmbeddedCrashChild(t, raw)
		return
	}
	cfg := embeddedStoreConfig(t)
	cfg.ClusterID = "embedded-sigkill-total-loss"
	cfg.ObjStoreSyncInterval = time.Hour
	cfg.ObjStoreGCInterval = 24 * time.Hour
	root := t.TempDir()
	ackFile := filepath.Join(root, "ack.json")
	input, err := json.Marshal(embeddedCrashInput{Config: cfg, AckFile: ackFile})
	if err != nil {
		t.Fatal(err)
	}
	logFile, err := os.Create(filepath.Join(root, "child.log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = logFile.Close() })
	watchdog, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(watchdog, os.Args[0], "-test.run=^TestEmbeddedObjectStorageSIGKILLTotalDataDirLoss$", "-test.v")
	cmd.Env = append(os.Environ(), embeddedCrashChildEnv+"="+string(input))
	cmd.Stdout, cmd.Stderr = logFile, logFile
	t.Cleanup(func() {
		if cmd.Process != nil && cmd.ProcessState == nil {
			if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				t.Errorf("cleanup kill child: %v", err)
			}
			waitErr := cmd.Wait()
			var exitErr *exec.ExitError
			if !errors.As(waitErr, &exitErr) {
				t.Errorf("cleanup reap child=%v, want SIGKILL", waitErr)
			} else if status, ok := exitErr.Sys().(syscall.WaitStatus); !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Errorf("cleanup child termination=%v, want reaped SIGKILL", exitErr)
			}
			if cmd.ProcessState == nil {
				t.Error("cleanup child has no reaped process state")
			}
		}
		if t.Failed() {
			output, err := os.ReadFile(logFile.Name())
			if err != nil {
				t.Errorf("read failed child log: %v", err)
			} else {
				t.Logf("crash child output:\n%s", output)
			}
		}
	})
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var ack rhiza.ExecuteResponse
	for {
		data, err := os.ReadFile(ackFile)
		if err == nil {
			if err := json.Unmarshal(data, &ack); err != nil {
				t.Fatalf("decode acknowledged response: %v", err)
			}
			break
		}
		if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		select {
		case <-watchdog.Done():
			output, _ := os.ReadFile(logFile.Name())
			t.Fatalf("child did not record committed ACK: %v\n%s", watchdog.Err(), output)
		case <-time.After(10 * time.Millisecond):
		}
	}
	if ack.Status != rhiza.MutationCommitted || ack.Slot == 0 || len(ack.Statements) != 1 || len(ack.Statements[0].Rows) != 1 {
		t.Fatalf("invalid child acknowledgement: %+v", ack)
	}
	ctx, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()
	readOnly := &embeddedReadOnlyBucket{Bucket: embeddedBucket(t, cfg)}
	archive, err := recovery.OpenLocalArchive(ctx, readOnly, path.Join(cfg.ObjStorePrefix, string(cfg.ClusterID), "local/v1"))
	if err != nil || archive == nil || uint64(archive.Tip()) < ack.Slot {
		t.Fatalf("archive before kill=%v error=%v, want ACK slot >=%d", archive, err, ack.Slot)
	}
	if readOnly.mutations != 0 {
		t.Fatalf("pre-kill archive inspection attempted %d remote mutations", readOnly.mutations)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	var exitErr *exec.ExitError
	if err := cmd.Wait(); !errors.As(err, &exitErr) {
		t.Fatalf("kill/reap result=%v, want SIGKILL", err)
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL || cmd.ProcessState == nil {
		t.Fatalf("child not SIGKILLed and reaped: %v", exitErr)
	}
	if err := os.RemoveAll(cfg.DataDir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(cfg.DataDir); !os.IsNotExist(err) {
		t.Fatalf("whole DataDir still exists after loss: %v", err)
	}
	db, err := rhiza.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("restore after SIGKILL and total DataDir loss: %v", err)
	}
	t.Cleanup(func() {
		if db != nil {
			_ = db.Close()
		}
	})
	check := func(want int) {
		t.Helper()
		rows, err := db.Query(ctx, rhiza.QueryRequest{SQL: "SELECT id, name FROM crash_items ORDER BY id"})
		if err != nil || len(rows.Rows) != want || !embeddedCrashJSONEqual(t, rows.Rows[0], ack.Statements[0].Rows[0]) {
			t.Fatalf("restored acknowledged rows=%+v want=%d error=%v", rows.Rows, want, err)
		}
		status, err := db.RequestStatus(ctx, rhiza.RequestStatusRequest{Kind: "sql", RequestID: embeddedCrashRequest().RequestID})
		if err != nil || status.State != "committed" || status.Receipt == nil || status.Receipt.Slot != ack.Slot {
			t.Fatalf("restored RequestStatus=%+v error=%v, want slot=%d", status, err, ack.Slot)
		}
		retry, err := db.ExecuteReturning(ctx, embeddedCrashRequest())
		if err != nil || retry.Status != rhiza.MutationCommitted || retry.Slot != ack.Slot || !embeddedCrashJSONEqual(t, retry.Statements, ack.Statements) {
			t.Fatalf("restored idempotent ACK=%+v original=%+v error=%v", retry, ack, err)
		}
	}
	check(1)
	if _, err := db.Execute(ctx, rhiza.ExecuteRequest{RequestID: "after-crash", SQL: "INSERT INTO crash_items VALUES (8, 'after-restore')"}); err != nil {
		t.Fatalf("new write after restore: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = nil
	db, err = rhiza.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("restart restored writer: %v", err)
	}
	check(2)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = nil
}

func runEmbeddedCrashChild(t *testing.T, raw string) {
	t.Helper()
	var input embeddedCrashInput
	if err := json.Unmarshal([]byte(raw), &input); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := rhiza.Open(ctx, input.Config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, rhiza.ExecuteRequest{RequestID: "crash-schema", SQL: "CREATE TABLE crash_items (id INTEGER PRIMARY KEY, name TEXT)"}); err != nil {
		t.Fatal(err)
	}
	ack, err := db.ExecuteReturning(ctx, embeddedCrashRequest())
	if err != nil || ack.Status != rhiza.MutationCommitted || ack.Slot == 0 {
		t.Fatalf("child ACK=%+v error=%v", ack, err)
	}
	data, err := json.Marshal(ack)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(input.AckFile+".pending", data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(input.AckFile+".pending", input.AckFile); err != nil {
		t.Fatal(err)
	}
	// The parent kills and reaps this process; do not Close and flush the DB.
	select {}
}

func TestEmbeddedObjectStorageUnresolvedHeldControlPreserved(t *testing.T) {
	cfg := embeddedStoreConfig(t)
	cfg.ClusterID = "embedded-unresolved-control"
	cfg.ObjStoreSyncInterval = time.Hour
	cfg.ObjStoreGCInterval = 24 * time.Hour
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := rhiza.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, rhiza.ExecuteRequest{RequestID: "held-schema", SQL: "CREATE TABLE held_items (id INTEGER PRIMARY KEY)"}); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	key := path.Join(cfg.ObjStorePrefix, string(cfg.ClusterID), "local/v1/local-gc-control")
	image := embeddedRemoteImage(t, cfg)
	var control map[string]any
	if err := json.Unmarshal(image[key], &control); err != nil {
		t.Fatal(err)
	}
	control["token"] = strings.Repeat("ab", 16)
	scenario := "valid held remote gate"
	wantReason := "remote ownership cannot be recovered automatically"
	// Remote stores have no local owner proof; their valid held gate must stay.
	// Filesystem ownership permits dead-owner recovery, but not unknown binding.
	if cfg.ObjStoreProvider == rhiza.ObjectStoreProviderFilesystem {
		control["binding_hash"] = strings.Repeat("0", 64)
		scenario = "held filesystem gate with malformed binding"
		wantReason = "invalid Local maintenance control"
	}
	t.Logf("negative restore scenario: %s", scenario)
	image[key], err = json.Marshal(control)
	if err != nil {
		t.Fatal(err)
	}
	b := embeddedBucket(t, cfg)
	if err := b.Upload(ctx, key, bytes.NewReader(image[key])); err != nil {
		t.Fatal(err)
	}
	// Read raw bytes: the ordinary fixture intentionally rejects invalid control.
	readImage := func() map[string][]byte {
		t.Helper()
		got := make(map[string][]byte)
		err := b.Iter(ctx, "", func(name string) error {
			r, err := b.Get(ctx, name)
			if err != nil {
				return err
			}
			data, readErr := io.ReadAll(r)
			closeErr := r.Close()
			got[name] = data
			return errors.Join(readErr, closeErr)
		}, objstore.WithRecursiveIter())
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	before := readImage()
	if err := os.RemoveAll(cfg.DataDir); err != nil {
		t.Fatal(err)
	}
	openCtx, stop := context.WithTimeout(ctx, 2*time.Second)
	db, err = rhiza.Open(openCtx, cfg)
	stop()
	if err == nil {
		_ = db.Close()
		t.Fatalf("restore accepted %s", scenario)
	}
	if !strings.Contains(err.Error(), wantReason) {
		t.Fatalf("restore rejected %s for wrong reason: %v; want %q", scenario, err, wantReason)
	}
	if after := readImage(); !reflect.DeepEqual(after, before) {
		t.Fatal("failed restore changed unresolved remote evidence")
	}
}

// JSON preserves the wire response while avoiding int64 versus decoded float64.
func embeddedCrashJSONEqual(t *testing.T, a, b any) bool {
	t.Helper()
	left, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	right, err := json.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	return bytes.Equal(left, right)
}
