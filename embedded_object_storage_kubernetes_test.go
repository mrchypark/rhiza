package rhiza_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/mrchypark/rhiza"
	localobj "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

// This wrapper makes any accidental remote mutation fail during preparation.
type embeddedReadOnlyBucket struct {
	objstore.Bucket
	mutations int
}

func (b *embeddedReadOnlyBucket) Upload(context.Context, string, io.Reader, ...objstore.ObjectUploadOption) error {
	b.mutations++
	return fmt.Errorf("init remote upload forbidden")
}
func (b *embeddedReadOnlyBucket) Delete(context.Context, string) error {
	b.mutations++
	return fmt.Errorf("init remote delete forbidden")
}

type embeddedInitEvidence struct {
	Binding  recovery.LocalBinding  `json:"binding"`
	Snapshot recovery.LocalSnapshot `json:"snapshot"`
	Tip      uint64                 `json:"tip"`
}

func embeddedKubernetesInit(t *testing.T, ctx context.Context, generation int) {
	t.Helper()
	if generation == 0 {
		t.Log("RHIZA_EMBEDDED_INIT seed: no remote archive or DataDir opened")
		return
	}
	// Do not use embeddedStoreConfig: its fixture setup creates the bucket.
	bucket, err := localobj.NewBucket(localobj.Config{Provider: localobj.ProviderS3, Endpoint: strings.TrimPrefix(embeddedEndpoint(t), "http://"), Bucket: os.Getenv("RHIZA_EMBEDDED_BUCKET"), Region: "us-east-1", Insecure: true, AccessKey: embeddedTestAccess, SecretKey: embeddedTestAccess})
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Close()
	readOnly := &embeddedReadOnlyBucket{Bucket: bucket}
	const prefix = "cluster-a/local/v1"
	archive, err := recovery.OpenLocalArchive(ctx, readOnly, prefix)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := archive.Snapshot()
	if snapshot == nil || uint64(archive.Tip()) <= uint64(snapshot.Seal.Index) {
		t.Fatal("init requires a real checkpoint and archived suffix")
	}
	if len(archive.Binding.Bootstrap.Members) != 1 || archive.Binding.Bootstrap.Members[0].ID != "embedded-backup" {
		t.Fatal("unexpected Local bootstrap binding")
	}
	var descriptor struct {
		StoreUUID    string `json:"store_uuid"`
		ConfigDigest string `json:"config_digest"`
		Slot         uint64 `json:"slot"`
		RootHash     string `json:"root_hash"`
		Files        []struct {
			Role   string `json:"role"`
			Length uint64 `json:"length"`
			SHA256 string `json:"sha256"`
		} `json:"files"`
	}
	if err := json.Unmarshal(snapshot.Descriptor, &descriptor); err != nil {
		t.Fatal(err)
	}
	if descriptor.StoreUUID != archive.Binding.StoreUUID || descriptor.ConfigDigest != archive.Binding.ConfigDigest || descriptor.Slot != uint64(snapshot.Seal.Index) || descriptor.RootHash != fmt.Sprintf("%x", snapshot.Seal.RootHash) {
		t.Fatal("checkpoint store binding mismatch")
	}
	manager := checkpoint.NewManager(readOnly, prefix, "/staging", snapshot.Seal.ConfigID)
	root, err := manager.OpenRoot(ctx, uint64(snapshot.Seal.Index), snapshot.PortableRoot)
	if err != nil {
		t.Fatal(err)
	}
	files, err := manager.DownloadAndVerifyRootFiles(ctx, root, "/staging")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 || len(descriptor.Files) != 2 {
		t.Fatal("checkpoint must contain both native files")
	}
	for _, file := range files {
		data, err := os.ReadFile(file.Path)
		if err != nil {
			t.Fatal(err)
		}
		matched := false
		for _, expected := range descriptor.Files {
			if expected.Role == file.Role && expected.Length == uint64(len(data)) && expected.SHA256 == fmt.Sprintf("%x", sha256.Sum256(data)) {
				matched = true
			}
		}
		if !matched {
			t.Fatal("native checkpoint file differs from Local descriptor")
		}
	}
	if readOnly.mutations != 0 {
		t.Fatalf("init attempted %d remote mutations", readOnly.mutations)
	}
	encoded, err := json.Marshal(embeddedInitEvidence{Binding: archive.Binding, Snapshot: *snapshot, Tip: uint64(archive.Tip())})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("/staging/verified.json", encoded, 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("RHIZA_EMBEDDED_INIT generation=%d checkpoint=%d tip=%d files=%d remote_mutations=%d store=%s", generation, snapshot.Seal.Index, archive.Tip(), len(files), readOnly.mutations, archive.Binding.StoreUUID)
}

// Only main owns Open, DataDir, remote publication, and readiness.
func TestEmbeddedObjectStorageKubernetesWorker(t *testing.T) {
	stage := os.Getenv("RHIZA_EMBEDDED_K8S_STAGE")
	if stage == "" {
		t.Skip("local Kubernetes fixture entry point")
	}
	if stage != "main" && stage != "init" {
		t.Fatalf("unknown fixture stage %q", stage)
	}
	generation, err := strconv.Atoi(os.Getenv("RHIZA_EMBEDDED_GENERATION"))
	if err != nil || generation < 0 || generation > 2 {
		t.Fatal("fixture generation must be 0, 1, or 2")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	if stage == "init" {
		embeddedKubernetesInit(t, ctx, generation)
		return
	}
	cfg := embeddedCheckpointConfig(t)
	cfg.DataDir = "/data/local"
	if _, err := os.Stat(cfg.DataDir); !os.IsNotExist(err) {
		t.Fatalf("fresh emptyDir must have no DataDir before Open: %v", err)
	}
	var staged embeddedInitEvidence
	if generation > 0 {
		data, err := os.ReadFile("/staging/verified.json")
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(data, &staged); err != nil {
			t.Fatal(err)
		}
		if staged.Binding.StoreUUID == "" || staged.Tip <= uint64(staged.Snapshot.Seal.Index) {
			t.Fatal("missing init checkpoint and suffix evidence")
		}
	}
	db, err := rhiza.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Kubernetes main open: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("Kubernetes close: %v", err)
		}
	})
	if !db.Ready() {
		t.Fatal("Open returned unready database")
	}
	if generation > 0 {
		data, err := os.ReadFile(cfg.DataDir + "/local-backup.json")
		if err != nil {
			t.Fatal(err)
		}
		var binding recovery.LocalBinding
		if err := json.Unmarshal(data, &binding); err != nil {
			t.Fatal(err)
		}
		if binding.StoreUUID != staged.Binding.StoreUUID || binding.ConfigDigest != staged.Binding.ConfigDigest {
			t.Fatal("main restored a different store than init verified")
		}
	}
	if generation == 0 {
		for _, req := range []rhiza.ExecuteRequest{
			{RequestID: "schema", SQL: "CREATE TABLE backup_items (id INTEGER PRIMARY KEY, name TEXT)"},
			{RequestID: "evidence-schema", SQL: "CREATE TABLE backup_evidence (slot INTEGER)"},
		} {
			if _, err := db.Execute(ctx, req); err != nil {
				t.Fatal(err)
			}
		}
	}
	if generation == 0 {
		if _, err := db.Execute(ctx, rhiza.ExecuteRequest{RequestID: "padding-schema", SQL: "CREATE TABLE backup_padding (payload TEXT)"}); err != nil {
			t.Fatal(err)
		}
		for i := 0; embeddedCheckpointSlot(t, cfg.DataDir) == 0 && i < 48; i++ {
			if _, err := db.Execute(ctx, rhiza.ExecuteRequest{RequestID: fmt.Sprintf("padding-%d", i), SQL: "INSERT INTO backup_padding VALUES (?)", Args: []any{strings.Repeat("x", 64<<10)}}); err != nil {
				t.Fatal(err)
			}
		}
		if embeddedCheckpointSlot(t, cfg.DataDir) == 0 {
			t.Fatal("bounded WAL failed to force a real checkpoint")
		}
		t.Logf("RHIZA_EMBEDDED_CHECKPOINT slot=%d", embeddedCheckpointSlot(t, cfg.DataDir))
	}
	request := rhiza.ExecuteRequest{RequestID: "original", SQL: "INSERT INTO backup_items(name) VALUES (?) RETURNING id, name", Args: []any{"original"}}
	original, err := db.ExecuteReturning(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(original.Statements) != 1 || len(original.Statements[0].Rows) != 1 || fmt.Sprint(original.Statements[0].Rows[0]) != "[1 original]" {
		t.Fatalf("original result=%+v", original)
	}
	if generation == 0 {
		if _, err := db.Execute(ctx, rhiza.ExecuteRequest{RequestID: "evidence", SQL: "INSERT INTO backup_evidence VALUES (?)", Args: []any{int64(original.Slot)}}); err != nil {
			t.Fatal(err)
		}
	}
	evidence, err := db.Query(ctx, rhiza.QueryRequest{SQL: "SELECT slot FROM backup_evidence"})
	if err != nil || len(evidence.Rows) != 1 || fmt.Sprint(evidence.Rows[0][0]) != fmt.Sprint(original.Slot) {
		t.Fatalf("restored original slot=%v evidence=%+v err=%v", original.Slot, evidence.Rows, err)
	}
	rows, err := db.Query(ctx, rhiza.QueryRequest{SQL: "SELECT name FROM backup_items ORDER BY id"})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.Rows) != generation+1 {
		t.Fatalf("generation=%d restored rows=%+v", generation, rows.Rows)
	}
	for i := 0; i < generation; i++ {
		if got := fmt.Sprint(rows.Rows[i+1][0]); got != fmt.Sprintf("generation-%d", i) {
			t.Fatalf("missing previous generation %d: %+v", i, rows.Rows)
		}
	}
	t.Logf("RHIZA_EMBEDDED_RESTORED generation=%d rows=%d", generation, len(rows.Rows))
	name := fmt.Sprintf("generation-%d", generation)
	if _, err := db.ExecuteReturning(ctx, rhiza.ExecuteRequest{RequestID: name, SQL: "INSERT INTO backup_items(name) VALUES (?) RETURNING id, name", Args: []any{name}}); err != nil {
		t.Fatalf("main backup write: %v", err)
	}
	if err := os.WriteFile("/tmp/embedded-ready", []byte(name), 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("RHIZA_EMBEDDED_READY generation=%d", generation)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(stop)
	select {
	case <-stop:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
