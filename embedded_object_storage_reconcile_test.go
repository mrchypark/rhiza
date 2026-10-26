package rhiza_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/mrchypark/rhiza"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

type embeddedHistory struct {
	t        *testing.T
	ctx      context.Context
	cfg      rhiza.Config
	db       *rhiza.DB
	original rhiza.ExecuteResponse
	request  rhiza.ExecuteRequest
}

func newEmbeddedHistory(t *testing.T, adopt bool) *embeddedHistory {
	t.Helper()
	return newEmbeddedHistoryConfig(t, adopt, embeddedStoreConfig(t))
}

func newEmbeddedHistoryConfig(t *testing.T, adopt bool, config rhiza.Config) *embeddedHistory {
	t.Helper()
	return newEmbeddedHistoryConfigTimeout(t, adopt, config, 90*time.Second)
}

func newEmbeddedHistoryConfigTimeout(t *testing.T, adopt bool, config rhiza.Config, timeout time.Duration) *embeddedHistory {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	h := &embeddedHistory{t: t, ctx: ctx, cfg: config, request: rhiza.ExecuteRequest{RequestID: "original", SQL: "INSERT INTO backup_items(name) VALUES (?) RETURNING id, name", Args: []any{"original"}}}
	t.Cleanup(func() {
		if h.db != nil {
			if err := h.db.Close(); err != nil {
				t.Errorf("cleanup database: %v", err)
			}
		}
	})
	cfg := h.cfg
	if adopt {
		cfg = embeddedWithoutStore(cfg)
	}
	h.open(cfg)
	h.execute("schema", "CREATE TABLE backup_items (id INTEGER PRIMARY KEY, name TEXT)")
	var err error
	h.original, err = h.db.ExecuteReturning(ctx, h.request)
	if err != nil {
		t.Fatal(err)
	}
	if len(h.original.Statements) != 1 || len(h.original.Statements[0].Rows) != 1 {
		t.Fatalf("missing original result: %+v", h.original)
	}
	return h
}

func embeddedWithoutStore(cfg rhiza.Config) rhiza.Config {
	return rhiza.Config{Local: true, NodeID: cfg.NodeID, DataDir: cfg.DataDir}
}

func (h *embeddedHistory) open(cfg rhiza.Config) {
	h.t.Helper()
	if h.db != nil {
		h.t.Fatal("fixture attempted concurrent ownership")
	}
	var err error
	h.db, err = rhiza.Open(h.ctx, cfg)
	if err != nil {
		h.t.Fatalf("open: %v", err)
	}
	if !h.db.Ready() {
		h.t.Fatal("open returned unready DB")
	}
}
func (h *embeddedHistory) close() {
	h.t.Helper()
	if err := h.db.Close(); err != nil {
		h.t.Fatalf("close: %v", err)
	}
	h.db = nil
}
func (h *embeddedHistory) execute(id, sql string) {
	h.t.Helper()
	if _, err := h.db.Execute(h.ctx, rhiza.ExecuteRequest{RequestID: id, SQL: sql}); err != nil {
		h.t.Fatalf("execute %s: %v", id, err)
	}
}
func (h *embeddedHistory) append(id string) {
	h.t.Helper()
	if _, err := h.db.ExecuteReturning(h.ctx, rhiza.ExecuteRequest{RequestID: fmt.Sprintf("%x", sha256.Sum256([]byte(id))), SQL: "INSERT INTO backup_items(name) VALUES (?) RETURNING id, name", Args: []any{id}}); err != nil {
		h.t.Fatalf("append %.96q: %v", id, err)
	}
}
func (h *embeddedHistory) check(names ...string) {
	h.t.Helper()
	rows, err := h.db.Query(h.ctx, rhiza.QueryRequest{SQL: "SELECT name FROM backup_items ORDER BY id"})
	if err != nil {
		h.t.Fatal(err)
	}
	got := make([]string, 0, len(rows.Rows))
	for _, row := range rows.Rows {
		got = append(got, fmt.Sprint(row[0]))
	}
	if !reflect.DeepEqual(got, names) {
		if len(got) != len(names) {
			h.t.Fatalf("SQL row count=%d want=%d", len(got), len(names))
		}
		for i := range names {
			if got[i] != names[i] {
				h.t.Fatalf("SQL row %d=%.96q want=%.96q", i, got[i], names[i])
			}
		}
	}
	retry, err := h.db.ExecuteReturning(h.ctx, h.request)
	if err != nil || retry.Slot != h.original.Slot || !reflect.DeepEqual(retry.Statements, h.original.Statements) {
		h.t.Fatalf("original retry=%+v want=%+v err=%v", retry, h.original, err)
	}
}

func (h *embeddedHistory) rejectUnchanged() {
	h.t.Helper()
	local := embeddedLocalImage(h.t, h.cfg.DataDir)
	remote := embeddedRemoteImage(h.t, h.cfg)
	db, err := rhiza.Open(h.ctx, h.cfg)
	if db != nil {
		_ = db.Close()
	}
	if err == nil {
		h.t.Fatal("unsafe reconciliation unexpectedly succeeded")
	}
	h.t.Logf("startup refusal: %v", err)
	got := embeddedLocalImage(h.t, h.cfg.DataDir)
	// Acquire writes a PID and Release clears this coordination file even on
	// refused startup. It is not application, WAL, binding, or journal evidence.
	delete(local, "qlog/lock.qlog")
	delete(got, "qlog/lock.qlog")
	if _, existed := local["qlog/"]; !existed {
		delete(got, "qlog/")
	}
	if !reflect.DeepEqual(got, local) {
		keys := make(map[string]bool)
		for name := range local {
			keys[name] = true
		}
		for name := range got {
			keys[name] = true
		}
		names := make([]string, 0, len(keys))
		for name := range keys {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			before, beforeOK := local[name]
			after, afterOK := got[name]
			if beforeOK != afterOK || !reflect.DeepEqual(before, after) {
				h.t.Logf("changed local path=%q before_exists=%v before_bytes=%d before_sha256=%x after_exists=%v after_bytes=%d after_sha256=%x", name, beforeOK, len(before), sha256.Sum256(before), afterOK, len(after), sha256.Sum256(after))
			}
		}
		h.t.Fatal("refused startup changed local file contents")
	}
	if got := embeddedRemoteImage(h.t, h.cfg); !reflect.DeepEqual(got, remote) {
		h.t.Fatal("refused startup changed remote object contents")
	}
}

func TestEmbeddedObjectStorageEqual(t *testing.T) {
	h := newEmbeddedHistory(t, false)
	h.close()
	remote := embeddedRemoteImage(t, h.cfg)
	if len(remote) == 0 {
		t.Fatal("backup store is empty")
	}
	h.open(h.cfg)
	h.check("original")
	h.close()
	if !reflect.DeepEqual(remote, embeddedRemoteImage(t, h.cfg)) {
		t.Fatal("equal startup rewrote remote history")
	}
}

func TestEmbeddedObjectStorageFirstAdoption(t *testing.T) {
	h := newEmbeddedHistory(t, true)
	h.close()
	if got := embeddedRemoteImage(t, h.cfg); len(got) != 0 {
		t.Fatal("local-only creation wrote remote objects")
	}
	h.open(h.cfg)
	h.check("original")
	h.close()
	if err := os.RemoveAll(h.cfg.DataDir); err != nil {
		t.Fatal(err)
	}
	h.open(h.cfg)
	h.check("original")
	h.append("adopted")
	h.check("original", "adopted")
	h.close()
}

func TestEmbeddedObjectStorageLocalAheadCheckpointAndTail(t *testing.T) {
	h := newEmbeddedHistoryConfig(t, false, embeddedCheckpointConfig(t))
	names := []string{"original"}
	checkpoint := embeddedAdvanceCheckpoint(h, h.original.Slot, "shared-checkpoint", &names)
	h.close()
	oldRemote := embeddedRemoteImage(t, h.cfg)
	h.open(h.cfg)
	if shared := embeddedCheckpointSlot(t, h.cfg.DataDir); shared < checkpoint {
		t.Fatalf("shared checkpoint disappeared on reopen: %d < %d", shared, checkpoint)
	} else {
		checkpoint = shared
	}
	h.append("local-ahead-suffix")
	names = append(names, "local-ahead-suffix")
	h.append("tail")
	names = append(names, "tail")
	if latest := embeddedCheckpointSlot(t, h.cfg.DataDir); latest != checkpoint {
		t.Fatalf("test suffix unexpectedly rebased beyond the shared overlap: checkpoint=%d latest=%d", checkpoint, latest)
	}
	h.close()
	embeddedReplaceRemote(t, h.cfg, oldRemote)
	h.open(h.cfg)
	h.check(names...)
	h.close()
	if err := os.RemoveAll(h.cfg.DataDir); err != nil {
		t.Fatal(err)
	}
	h.open(h.cfg)
	h.check(names...)
	h.append("after-ahead-restore")
	h.close()
}

func TestEmbeddedObjectStorageRemoteAheadRestoresProvenExtension(t *testing.T) {
	cfg := embeddedStoreConfig(t)
	cfg.ClusterID = "embedded-remote-ahead"
	h := newEmbeddedHistoryConfig(t, false, cfg)
	h.close()
	staleLocal := embeddedLocalImage(t, h.cfg.DataDir)
	h.open(h.cfg)
	h.append("remote-ahead")
	h.close()
	embeddedReplaceLocal(t, h.cfg.DataDir, staleLocal)
	remote := embeddedRemoteImage(t, h.cfg)
	archive := embeddedGCArchive(t, h.ctx, embeddedBucket(t, h.cfg), h.cfg)
	h.open(h.cfg)
	h.check("original", "remote-ahead")
	h.close()
	after := embeddedRemoteImage(t, h.cfg)
	for key, data := range remote {
		if !reflect.DeepEqual(after[key], data) {
			t.Fatalf("restore changed existing remote evidence: %s", key)
		}
	}
	for key, data := range after {
		if _, existed := remote[key]; existed {
			continue
		}
		embeddedAssertClosedRestorePin(t, h.cfg, archive, remote, key, data)
	}
	h.open(h.cfg)
	h.append("after-remote-ahead-restore")
	h.check("original", "remote-ahead", "after-remote-ahead-restore")
	h.close()
	h.open(h.cfg)
	h.check("original", "remote-ahead", "after-remote-ahead-restore")
	h.close()
}

func embeddedAssertClosedRestorePin(t *testing.T, cfg rhiza.Config, archive *recovery.LocalArchive, remote map[string][]byte, key string, data []byte) {
	t.Helper()
	prefix := path.Join(cfg.ObjStorePrefix, string(cfg.ClusterID), "local/v1")
	decode := func(value any) {
		t.Helper()
		d := json.NewDecoder(bytes.NewReader(data))
		d.DisallowUnknownFields()
		if err := d.Decode(value); err != nil {
			t.Fatalf("invalid restore pin %s: %v", key, err)
		}
		if err := d.Decode(new(any)); err != io.EOF {
			t.Fatalf("trailing restore pin data: %s", key)
		}
	}
	validHex := func(value string, size int) bool {
		decoded, err := hex.DecodeString(value)
		return err == nil && len(decoded) == size && hex.EncodeToString(decoded) == value
	}
	switch path.Dir(key) {
	case path.Join(prefix, "view-pins"):
		var pin struct {
			Version      uint     `json:"version"`
			BindingHash  [32]byte `json:"binding_hash"`
			HeadHash     [32]byte `json:"head_hash"`
			Token        string   `json:"token"`
			LeaseUntilMS *int64   `json:"lease_until_unix_ms"`
		}
		decode(&pin)
		name := strings.TrimSuffix(path.Base(key), ".json")
		binding, err := json.Marshal(archive.Binding)
		if err != nil {
			t.Fatal(err)
		}
		if !validHex(name, 32) || key != path.Join(prefix, "view-pins", name+".json") || pin.Version != 1 || !validHex(pin.Token, 16) || pin.BindingHash != sha256.Sum256(binding) || pin.HeadHash != sha256.Sum256(remote[path.Join(prefix, "HEAD")]) || pin.LeaseUntilMS == nil || *pin.LeaseUntilMS != 0 {
			t.Fatalf("invalid or unclosed restore view pin: %s", key)
		}
	case path.Join(prefix, "checkpoint/recovery-pins"):
		var pin struct {
			ConfigID     uint                  `json:"config_id"`
			OwnerID      string                `json:"owner_id"`
			Token        string                `json:"token"`
			Index        uint64                `json:"index"`
			RootHash     string                `json:"root_hash"`
			Root         checkpoint.Checkpoint `json:"root"`
			LeaseUntilMS *int64                `json:"lease_until_unix_ms"`
		}
		decode(&pin)
		root, err := json.Marshal(pin.Root)
		if err != nil {
			t.Fatal(err)
		}
		if pin.OwnerID == "" || key != path.Join(prefix, "checkpoint/recovery-pins", fmt.Sprintf("%x", sha256.Sum256([]byte(pin.OwnerID)))) || !validHex(pin.Token, 16) || !validHex(pin.RootHash, 32) || pin.RootHash != fmt.Sprintf("%x", sha256.Sum256(root)) || pin.ConfigID != archive.Binding.Bootstrap.ConfigID || pin.Root.ConfigID != pin.ConfigID || pin.Index == 0 || pin.Root.Index != pin.Index || pin.LeaseUntilMS == nil || *pin.LeaseUntilMS <= 0 || *pin.LeaseUntilMS > time.Now().UnixMilli() {
			t.Fatalf("invalid or unclosed checkpoint recovery pin: %s", key)
		}
	default:
		t.Fatalf("restore added unexpected remote object: %s", key)
	}
}

func TestEmbeddedObjectStorageDivergedNonmutating(t *testing.T) {
	h := newEmbeddedHistory(t, false)
	h.close()
	baseLocal := embeddedLocalImage(t, h.cfg.DataDir)
	baseRemote := embeddedRemoteImage(t, h.cfg)
	h.open(h.cfg)
	h.append("branch-a")
	h.close()
	branchA := embeddedRemoteImage(t, h.cfg)
	embeddedReplaceLocal(t, h.cfg.DataDir, baseLocal)
	embeddedReplaceRemote(t, h.cfg, baseRemote)
	h.open(h.cfg)
	h.append("branch-b")
	h.close()
	branchB := embeddedRemoteImage(t, h.cfg)
	embeddedReplaceRemote(t, h.cfg, branchA)
	h.rejectUnchanged()
	embeddedReplaceRemote(t, h.cfg, branchB)
	h.open(h.cfg)
	h.check("original", "branch-b")
	h.close()
}

func TestEmbeddedObjectStoragePartialRootFailureRetry(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(fmt.Sprintf("corrupt-%v", corrupt), func(t *testing.T) {
			h := newEmbeddedHistory(t, false)
			h.close()
			original := embeddedLocalImage(t, h.cfg.DataDir)
			keys := make([]string, 0, len(original))
			for key, data := range original {
				if len(data) > 0 {
					keys = append(keys, key)
				}
			}
			sort.Strings(keys)
			if len(keys) < 2 {
				t.Fatalf("partial-root test requires at least two nonempty local files, got %v", keys)
			}
			damaged := make(map[string][]byte)
			if corrupt {
				for key := range original {
					if key == "qlog/lock.qlog" {
						damaged[key] = original[key]
						continue
					}
					if strings.HasSuffix(key, "/") {
						damaged[key] = nil
						continue
					}
					damaged[key] = []byte("rhiza deliberate test corruption\n")
				}
			} else {
				damaged[keys[0]] = original[keys[0]]
			}
			embeddedReplaceLocal(t, h.cfg.DataDir, damaged)
			h.rejectUnchanged()
			embeddedReplaceLocal(t, h.cfg.DataDir, original)
			h.open(h.cfg)
			h.check("original")
			h.append("after-repair")
			h.close()
		})
	}
}

func TestEmbeddedObjectStorageRemoteAheadMalformedRestoreJournalNonmutating(t *testing.T) {
	h := newEmbeddedHistory(t, false)
	h.close()
	staleLocal := embeddedLocalImage(t, h.cfg.DataDir)
	h.open(h.cfg)
	h.append("remote-ahead")
	h.close()
	embeddedReplaceLocal(t, h.cfg.DataDir, staleLocal)
	// Existing Go materializer journal path; no remote archive format is assumed.
	journal := filepath.Join(h.cfg.DataDir, "sqlite.db.local-restore.json")
	if err := os.WriteFile(journal, []byte("{malformed restore journal\n"), 0600); err != nil {
		t.Fatal(err)
	}
	h.rejectUnchanged()
	if err := os.Remove(journal); err != nil {
		t.Fatal(err)
	}
	h.open(h.cfg)
	h.check("original", "remote-ahead")
	h.append("after-journal-repair")
	h.check("original", "remote-ahead", "after-journal-repair")
	h.close()
}

func TestEmbeddedObjectStorageMissingBoundRemoteFailureRetry(t *testing.T) {
	h := newEmbeddedHistory(t, false)
	h.close()
	remote := embeddedRemoteImage(t, h.cfg)
	if len(remote) == 0 {
		t.Fatal("bound remote is empty")
	}
	embeddedReplaceRemote(t, h.cfg, nil)
	h.rejectUnchanged()
	embeddedReplaceRemote(t, h.cfg, remote)
	h.open(h.cfg)
	h.check("original")
	h.append("after-remote-repair")
	h.close()
}

func TestEmbeddedObjectStorageBeforeAckUnavailablePreservesRetry(t *testing.T) {
	if provider := os.Getenv("RHIZA_EMBEDDED_PROVIDER"); provider != "" && provider != "filesystem" {
		t.Skip("filesystem path failure fixture; remote providers use emulator contract checks")
	}
	for _, policy := range []struct {
		name       string
		durability rhiza.ObjectStoreDurability
	}{
		{name: "explicit-before-ack", durability: rhiza.ObjectStoreDurabilityBeforeAck},
		{name: "empty-default", durability: ""},
	} {
		t.Run(policy.name, func(t *testing.T) {
			cfg := embeddedStoreConfig(t)
			cfg.ObjStoreDurability = policy.durability
			cfg.ObjStoreSyncInterval = time.Hour
			h := newEmbeddedHistoryConfig(t, false, cfg)
			h.close()
			beforeFailureRemote := embeddedRemoteImage(t, h.cfg)
			h.open(h.cfg)
			remote := h.cfg.ObjStoreDir
			saved := remote + "-outage"
			if err := os.Rename(remote, saved); err != nil {
				t.Fatal(err)
			}
			restored := false
			stopped := false
			var closeDone chan error
			stopDB := func() bool {
				t.Helper()
				if stopped {
					return true
				}
				if closeDone == nil {
					db := h.db
					h.db = nil
					closeDone = make(chan error, 1)
					go func() { closeDone <- db.Close() }()
				}
				select {
				case err := <-closeDone:
					stopped = true
					if err != nil && !errors.Is(err, syscall.ENOTDIR) {
						t.Errorf("unexpected close error during archive outage: %v", err)
					} else if err != nil {
						t.Logf("expected archive outage close error: %v", err)
					}
					return true
				case <-time.After(20 * time.Second):
					t.Error("DB close did not complete within 20s; leaving the archive blocked rather than repairing under active workers")
					return false
				}
			}
			repair := func() {
				t.Helper()
				if restored {
					return
				}
				if !stopped {
					t.Fatal("archive repair requires confirmed DB shutdown")
				}
				// The original archive is saved separately. Anything at this owned
				// path is the injected blocker or a test-created replacement directory.
				if err := os.RemoveAll(remote); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(saved, remote); err != nil {
					t.Fatal(err)
				}
				restored = true
			}
			// LIFO cleanup stops owned workers before repairing the archive and
			// before the history fixture's ordinary close cleanup can run.
			t.Cleanup(func() {
				if stopDB() {
					repair()
				}
			})
			if err := os.WriteFile(remote, []byte("not a directory"), 0600); err != nil {
				t.Fatal(err)
			}
			request := rhiza.ExecuteRequest{RequestID: "outage", SQL: "INSERT INTO backup_items(name) VALUES (?) RETURNING id, name", Args: []any{"outage"}}
			ctx, cancel := context.WithTimeout(h.ctx, time.Second)
			_, err := h.db.ExecuteReturning(ctx, request)
			cancel()
			if err == nil {
				t.Fatal("BeforeAck reported success while remote writes were impossible")
			}
			if !stopDB() {
				t.FailNow()
			}
			repair()
			// Discard any shutdown-time remote flush so restart must reconcile the
			// locally preserved operation against the last known published prefix.
			embeddedReplaceRemote(t, h.cfg, beforeFailureRemote)
			if got := embeddedLocalImage(t, h.cfg.DataDir); len(got) == 0 {
				t.Fatal("failed publication discarded local state")
			}
			h.open(h.cfg)
			h.check("original", "outage")
			first, err := h.db.ExecuteReturning(h.ctx, request)
			if err != nil {
				t.Fatalf("retry after repair: %v", err)
			}
			second, err := h.db.ExecuteReturning(h.ctx, request)
			if err != nil || second.Slot != first.Slot || !reflect.DeepEqual(first.Statements, second.Statements) {
				t.Fatalf("idempotent outage retry=%+v original=%+v err=%v", second, first, err)
			}
			h.check("original", "outage")
			h.close()
		})
	}
}
