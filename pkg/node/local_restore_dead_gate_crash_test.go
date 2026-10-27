package node

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
	"testing"
	"time"

	rhizaobjstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/thanos-io/objstore"
)

type localDeadGateCrashRequest struct {
	DataDir  string
	ObjStore string
	Ready    string
}

func TestLocalFilesystemV2RestoreAfterDeadMaintenanceHolder(t *testing.T) {
	testLocalFilesystemV2RestoreAfterDeadMaintenanceHolder(t, false)
}

func TestLocalFilesystemV2HeldGateWithMatchingDataDir(t *testing.T) {
	testLocalFilesystemV2RestoreAfterDeadMaintenanceHolder(t, true)
}

func testLocalFilesystemV2RestoreAfterDeadMaintenanceHolder(t *testing.T, keepDataDir bool) {
	t.Helper()
	root := t.TempDir()
	dataDir, archiveDir, ready := filepath.Join(root, "data"), filepath.Join(root, "archive"), filepath.Join(root, "gate-held")
	if err := os.Mkdir(dataDir, 0700); err != nil {
		t.Fatal(err)
	}
	request, err := json.Marshal(localDeadGateCrashRequest{DataDir: dataDir, ObjStore: archiveDir, Ready: ready})
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestLocalFilesystemV2MaintenanceHolderHelper$")
	cmd.Env = append(os.Environ(), "RHIZA_LOCAL_V2_DEAD_GATE_CRASH="+string(request))
	startLocalNodeCrashChild(t, cmd, ready)
	defer func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	waitForLocalDeadGateReady(t, cmd, ready)

	config := localFilesystemV2CrashConfig(filepath.Join(root, "live-data"), archiveDir)
	if err := os.Mkdir(config.DataDir, 0700); err != nil {
		t.Fatal(err)
	}
	controlKey := path.Join("dead-gate", "local/v1/local-gc-control")
	controlBefore := localDeadGateFilesystemObject(t, archiveDir, controlKey)
	var control struct {
		Version      uint   `json:"version"`
		Token        string `json:"token"`
		LocationHash string `json:"location_hash"`
		BindingHash  string `json:"binding_hash"`
	}
	if err := json.Unmarshal(controlBefore, &control); err != nil || control.Version != 2 || control.Token == "" || control.LocationHash == "" || control.BindingHash == "" {
		t.Fatalf("held v2 maintenance control=%s err=%v", controlBefore, err)
	}
	locationHash, bindingHash := control.LocationHash, control.BindingHash
	var binding struct {
		Version             uint   `json:"version"`
		ArchiveLocationHash string `json:"archive_location_hash"`
	}
	if err := json.Unmarshal(localDeadGateFilesystemObject(t, archiveDir, path.Join("dead-gate", "local/v1/BINDING")), &binding); err != nil || binding.Version != 2 || binding.ArchiveLocationHash != locationHash {
		t.Fatalf("filesystem-v2 archive binding=%+v err=%v", binding, err)
	}
	live := New(&config)
	liveCtx, cancelLive := context.WithTimeout(context.Background(), time.Second)
	liveErr := live.Open(liveCtx)
	cancelLive()
	if liveErr == nil {
		_ = live.Shutdown()
		t.Fatal("second process entered while the writer still owned the filesystem archive")
	}
	if after := localDeadGateFilesystemObject(t, archiveDir, controlKey); !bytes.Equal(controlBefore, after) {
		t.Fatalf("live-owner refusal changed control bytes: before=%s after=%s", controlBefore, after)
	}

	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	waitErr := cmd.Wait()
	var exitErr *exec.ExitError
	if !errors.As(waitErr, &exitErr) || exitErr.ExitCode() != -1 {
		t.Fatalf("writer termination=%v, want SIGKILL and reap", waitErr)
	}
	if !keepDataDir {
		if err := os.RemoveAll(dataDir); err != nil {
			t.Fatal(err)
		}
	}

	restoreConfig := localFilesystemV2CrashConfig(dataDir, archiveDir)
	restored := New(&restoreConfig)
	restoreCtx, cancelRestore := context.WithTimeout(context.Background(), 15*time.Second)
	err = restored.Open(restoreCtx)
	cancelRestore()
	if err != nil {
		t.Fatalf("restore confirmed data after SIGKILL and reap (keep DataDir=%t): %v", keepDataDir, err)
	}
	rows, err := restored.server.Query(context.Background(), network.QueryRequest{SQL: "SELECT id FROM dead_gate_rows"})
	if err != nil || len(rows.Rows) != 1 || len(rows.Rows[0]) != 1 || rows.Rows[0][0] != int64(7) {
		_ = restored.Shutdown()
		t.Fatalf("restored rows=%v err=%v, want confirmed row 7", rows.Rows, err)
	}
	controlAfter := localDeadGateFilesystemObject(t, archiveDir, controlKey)
	if err := json.Unmarshal(controlAfter, &control); err != nil || control.Version != 2 || control.Token != "" || control.LocationHash != locationHash || control.BindingHash != bindingHash {
		_ = restored.Shutdown()
		t.Fatalf("recovered v2 maintenance control=%s err=%v, want only its token cleared", controlAfter, err)
	}
	secondDir := filepath.Join(root, "concurrent-data")
	if err := os.Mkdir(secondDir, 0700); err != nil {
		_ = restored.Shutdown()
		t.Fatal(err)
	}
	concurrentConfig := localFilesystemV2CrashConfig(secondDir, archiveDir)
	concurrent := New(&concurrentConfig)
	concurrentCtx, cancelConcurrent := context.WithTimeout(context.Background(), time.Second)
	err = concurrent.Open(concurrentCtx)
	cancelConcurrent()
	if err == nil {
		_ = concurrent.Shutdown()
		_ = restored.Shutdown()
		t.Fatal("concurrent restorer entered while the first restore retained ownership")
	}
	if err := restored.Shutdown(); err != nil {
		t.Fatal(err)
	}
}

func TestLocalFilesystemV2MaintenanceHolderHelper(t *testing.T) {
	raw := os.Getenv("RHIZA_LOCAL_V2_DEAD_GATE_CRASH")
	if raw == "" {
		return
	}
	var request localDeadGateCrashRequest
	if err := json.Unmarshal([]byte(raw), &request); err != nil {
		t.Fatal(err)
	}
	config := localFilesystemV2CrashConfig(request.DataDir, request.ObjStore)
	n := New(&config)
	if err := n.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := n.server.Execute(context.Background(), network.ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE dead_gate_rows (id INTEGER PRIMARY KEY)"}); err != nil {
		t.Fatal(err)
	}
	ack, err := n.server.Execute(context.Background(), network.ExecuteRequest{RequestID: "row", SQL: "INSERT INTO dead_gate_rows VALUES (7)"})
	if err != nil || ack.Status != types.MutationCommitted || ack.Slot == 0 {
		t.Fatalf("row acknowledgement=%+v err=%v, want committed slot", ack.MutationReceipt, err)
	}
	if uint64(n.localBackup.Tip()) < ack.Slot || n.localBackup.Tip() != n.core.Tip() {
		t.Fatalf("archive tip=%d core tip=%d row ACK slot=%d; before-ack archive must contain acknowledged write", n.localBackup.Tip(), n.core.Tip(), ack.Slot)
	}
	key := path.Join(n.localBackupPrefix(), "local-gc-control")
	n.bucket.Bucket = &localDeadGateUploadBucket{Bucket: n.bucket.Bucket, key: key, ready: request.Ready}
	if err := n.localBackup.GarbageCollect(context.Background(), config.CheckpointRetainCount, config.ObjStoreGCGracePeriod); err == nil {
		t.Fatal("held-gate fixture unexpectedly returned")
	}
}

func localFilesystemV2CrashConfig(dataDir, archiveDir string) types.ExecutionConfig {
	return types.ExecutionConfig{Local: true, LocalArchiveProfile: types.LocalArchiveProfileFilesystem, NodeID: "local", ClusterID: "dead-gate", DataDir: dataDir, ObjStoreProvider: "filesystem", ObjStoreDir: archiveDir, ObjStoreDurability: types.ObjectStoreDurabilityBeforeAck, ObjStoreSyncInterval: time.Hour, CheckpointInterval: time.Hour}
}

func TestLocalFilesystemV2ProfileRejectsUnsupportedModes(t *testing.T) {
	for name, configure := range map[string]func(*types.ExecutionConfig){
		"non-local":      func(c *types.ExecutionConfig) { c.Local = false },
		"non-filesystem": func(c *types.ExecutionConfig) { c.ObjStoreProvider = "s3" },
		"async":          func(c *types.ExecutionConfig) { c.ObjStoreDurability = types.ObjectStoreDurabilityAsync },
	} {
		t.Run(name, func(t *testing.T) {
			config := localFilesystemV2CrashConfig(filepath.Join(t.TempDir(), "data"), filepath.Join(t.TempDir(), "archive"))
			configure(&config)
			n := New(&config)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := n.Open(ctx); err == nil {
				_ = n.Shutdown()
				t.Fatal("unsupported filesystem-v2 profile configuration was accepted")
			}
		})
	}
}

func TestLocalFilesystemV2RefusesLegacyArchiveUnchanged(t *testing.T) {
	root := t.TempDir()
	dataDir, archiveDir := filepath.Join(root, "data"), filepath.Join(root, "archive")
	if err := os.Mkdir(dataDir, 0700); err != nil {
		t.Fatal(err)
	}
	legacy := localFilesystemV2CrashConfig(dataDir, archiveDir)
	legacy.LocalArchiveProfile = types.LocalArchiveProfileLegacy
	n := New(&legacy)
	if err := n.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	keys := []string{
		path.Join("dead-gate", "local/v1/BINDING"),
		path.Join("dead-gate", "local/v1/HEAD"),
		path.Join("dead-gate", "local/v1/local-gc-control"),
	}
	before := localDeadGateFilesystemSnapshot(t, archiveDir, keys)
	v2 := localFilesystemV2CrashConfig(dataDir, archiveDir)
	rejected := New(&v2)
	if err := rejected.Open(context.Background()); err == nil {
		_ = rejected.Shutdown()
		t.Fatal("filesystem-v2 silently adopted a legacy-v1 archive")
	}
	after := localDeadGateFilesystemSnapshot(t, archiveDir, keys)
	if len(before) != len(after) {
		t.Fatalf("legacy archive object set changed during v2 refusal: before=%v after=%v", before, after)
	}
	for key, data := range before {
		if !bytes.Equal(data, after[key]) {
			t.Fatalf("legacy object %s changed during v2 refusal: before=%s after=%s", key, data, after[key])
		}
	}
}

func localDeadGateFilesystemSnapshot(t *testing.T, root string, keys []string) map[string][]byte {
	t.Helper()
	bucket, err := rhizaobjstore.NewBucket(rhizaobjstore.Config{Provider: rhizaobjstore.ProviderFilesystem, FilesystemDir: root})
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Close()
	objects := make(map[string][]byte)
	for _, key := range keys {
		exists, err := bucket.Exists(context.Background(), key)
		if err != nil {
			t.Fatal(err)
		}
		if exists {
			objects[key] = localDeadGateObject(t, bucket, key)
		}
	}
	return objects
}

func waitForLocalDeadGateReady(t *testing.T, cmd *exec.Cmd, ready string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			return
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			waitErr := cmd.Wait()
			output, _ := os.ReadFile(ready + ".child.log")
			t.Fatalf("child did not hold v2 archive gate: wait=%v output=%s", waitErr, output)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func localDeadGateFilesystemObject(t *testing.T, root, name string) []byte {
	t.Helper()
	bucket, err := rhizaobjstore.NewBucket(rhizaobjstore.Config{Provider: rhizaobjstore.ProviderFilesystem, FilesystemDir: root})
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Close()
	return localDeadGateObject(t, bucket, name)
}

func TestLocalFilesystemV2UnknownUploadEffectKeepsOwner(t *testing.T) {
	root := t.TempDir()
	archiveDir, dataDir := filepath.Join(root, "archive"), filepath.Join(root, "data")
	if err := os.Mkdir(dataDir, 0700); err != nil {
		t.Fatal(err)
	}
	owner, err := rhizaobjstore.AcquireLocalFilesystemOwner(archiveDir, dataDir)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := rhizaobjstore.NewBucket(rhizaobjstore.Config{Provider: rhizaobjstore.ProviderFilesystem, FilesystemDir: archiveDir})
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Close()
	const key = "effect-applied-result-unknown"
	err = owner.Wrap(&localDeadGateUnknownEffectBucket{Bucket: bucket, key: key}).Upload(context.Background(), key, bytes.NewReader([]byte("durable")))
	if err == nil {
		t.Fatal("ambiguous adapter result unexpectedly succeeded")
	}
	if got := string(localDeadGateObject(t, bucket, key)); got != "durable" {
		t.Fatalf("filesystem adapter effect=%q, want durable bytes despite unknown result", got)
	}
	if err := owner.Close(time.Second); err == nil {
		t.Fatal("owner released after an ambiguous filesystem mutation")
	}
	if _, err := rhizaobjstore.AcquireLocalFilesystemOwner(archiveDir, dataDir); err == nil {
		t.Fatal("new process entered while the unknown-effect owner remained unresolved")
	}
}

type localDeadGateUnknownEffectBucket struct {
	objstore.Bucket
	key string
}

func (b *localDeadGateUnknownEffectBucket) Upload(ctx context.Context, name string, reader io.Reader, options ...objstore.ObjectUploadOption) error {
	if err := b.Bucket.Upload(ctx, name, reader, options...); err != nil {
		return err
	}
	if name == b.key {
		return errors.New("injected unknown result after filesystem adapter write")
	}
	return nil
}

// This compatibility case deliberately preserves a v1 held token after its
// holder is killed because that protocol cannot prove process fencing.
func TestLocalLegacyGateRemainsHeldAfterWriterDeath(t *testing.T) {
	if raw := os.Getenv("RHIZA_LOCAL_LEGACY_GATE_CRASH"); raw != "" {
		var request localDeadGateCrashRequest
		if err := json.Unmarshal([]byte(raw), &request); err != nil {
			t.Fatal(err)
		}
		config := types.ExecutionConfig{Local: true, NodeID: "local", ClusterID: "legacy-gate", DataDir: request.DataDir, ObjStoreProvider: "filesystem", ObjStoreDir: request.ObjStore, ObjStoreDurability: types.ObjectStoreDurabilityBeforeAck, ObjStoreSyncInterval: time.Hour, CheckpointInterval: time.Hour}
		n := New(&config)
		if err := n.Open(context.Background()); err != nil {
			t.Fatal(err)
		}
		for _, write := range []network.ExecuteRequest{
			{RequestID: "legacy-schema", SQL: "CREATE TABLE legacy_gate_rows (id INTEGER PRIMARY KEY)"},
			{RequestID: "legacy-row", SQL: "INSERT INTO legacy_gate_rows VALUES (9)"},
		} {
			if _, err := n.server.Execute(context.Background(), write); err != nil {
				t.Fatal(err)
			}
		}
		if n.localBackup.Tip() != n.core.Tip() {
			t.Fatal("before-ack writes were not confirmed in the archive")
		}
		var head struct {
			Version uint `json:"version"`
		}
		data := localDeadGateObject(t, n.bucket, path.Join(n.localBackupPrefix(), "HEAD"))
		if err := json.Unmarshal(data, &head); err != nil || head.Version != 1 {
			t.Fatalf("legacy archive HEAD version=%d err=%v, want v1", head.Version, err)
		}
		gateKey := path.Join(n.localBackupPrefix(), "local-gc-control")
		n.bucket.Bucket = &localDeadGateUploadBucket{Bucket: n.bucket.Bucket, key: gateKey, ready: request.Ready}
		if err := n.localBackup.GarbageCollect(context.Background(), config.CheckpointRetainCount, config.ObjStoreGCGracePeriod); err == nil {
			t.Fatal("held legacy gate fixture unexpectedly returned")
		}
		return
	}

	root := t.TempDir()
	dataDir, remote := filepath.Join(root, "data"), filepath.Join(root, "archive")
	ready := filepath.Join(root, "legacy-gate-held")
	request, err := json.Marshal(localDeadGateCrashRequest{DataDir: dataDir, ObjStore: remote, Ready: ready})
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestLocalLegacyGateRemainsHeldAfterWriterDeath$")
	cmd.Env = append(os.Environ(), "RHIZA_LOCAL_LEGACY_GATE_CRASH="+string(request))
	startLocalNodeCrashChild(t, cmd, ready)
	waitForLocalNodeCrashBoundary(t, cmd, ready)

	config := types.ExecutionConfig{Local: true, NodeID: "local", ClusterID: "legacy-gate", DataDir: filepath.Join(root, "restored"), ObjStoreProvider: "filesystem", ObjStoreDir: remote, ObjStoreDurability: types.ObjectStoreDurabilityBeforeAck, ObjStoreSyncInterval: time.Hour, CheckpointInterval: time.Hour}
	reader := New(&config)
	if err := reader.openLocalBackupBucket(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer reader.bucket.Close()
	gateKey := path.Join(reader.localBackupPrefix(), "local-gc-control")
	controlBefore := localDeadGateObject(t, reader.bucket, gateKey)
	var control struct {
		Version uint   `json:"version"`
		Token   string `json:"token"`
	}
	if err := json.Unmarshal(controlBefore, &control); err != nil || control.Version != 1 || control.Token == "" {
		t.Fatalf("held legacy maintenance control=%s err=%v", controlBefore, err)
	}
	headKey := path.Join(reader.localBackupPrefix(), "HEAD")
	var head struct {
		Version uint `json:"version"`
	}
	if err := json.Unmarshal(localDeadGateObject(t, reader.bucket, headKey), &head); err != nil || head.Version != 1 {
		t.Fatalf("archive HEAD version=%d err=%v, want legacy v1", head.Version, err)
	}

	openCtx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	restorer := New(&config)
	openErr := restorer.Open(openCtx)
	if openErr == nil {
		_ = restorer.Shutdown()
		t.Fatal("restore auto-released an unowned legacy gate; legacy writers cannot prove process fencing")
	}
	controlAfter := localDeadGateObject(t, reader.bucket, gateKey)
	if !bytes.Equal(controlBefore, controlAfter) {
		t.Fatalf("legacy control changed after dead-writer restore attempt: before=%s after=%s", controlBefore, controlAfter)
	}
	if err := json.Unmarshal(localDeadGateObject(t, reader.bucket, headKey), &head); err != nil || head.Version != 1 {
		t.Fatalf("archive HEAD changed to version=%d err=%v, want legacy v1", head.Version, err)
	}
}

func localDeadGateObject(t *testing.T, bucket interface {
	Get(context.Context, string) (io.ReadCloser, error)
}, name string) []byte {
	t.Helper()
	reader, err := bucket.Get(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(reader)
	if err := errors.Join(readErr, reader.Close()); err != nil {
		t.Fatal(err)
	}
	return data
}

type localDeadGateUploadBucket struct {
	objstore.Bucket
	key   string
	ready string
}

func (b *localDeadGateUploadBucket) Upload(ctx context.Context, name string, reader io.Reader, options ...objstore.ObjectUploadOption) error {
	if name != b.key {
		return b.Bucket.Upload(ctx, name, reader, options...)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	var control struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(data, &control); err != nil {
		return err
	}
	if err := b.Bucket.Upload(ctx, name, bytes.NewReader(data), options...); err != nil {
		return err
	}
	if control.Token != "" {
		if err := os.WriteFile(b.ready, []byte(control.Token), 0600); err != nil {
			return err
		}
		select {}
	}
	return nil
}
