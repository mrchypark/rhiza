package materializer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestManagedApplicationLosslessDriver(t *testing.T) {
	m := managedMaterializer(t)
	ctx := context.Background()
	raw := []string{string([]byte{0xff}), string([]byte{0xfe}), "\ufffd", "a\x00b"}
	for i, id := range raw {
		command := types.KVCommand{RequestID: id, Key: raw[len(raw)-1-i], Operation: "put", Value: []byte{byte(i)}}
		item, err := types.EncodeManagedKVBatchItem(command)
		if err != nil {
			t.Fatal(err)
		}
		value, err := types.AssembleManagedKVBatch([][]byte{item})
		if err != nil {
			t.Fatal(err)
		}
		if err := m.Apply(ctx, uint64(i+1), value); err != nil {
			t.Fatal(err)
		}
		got, found, err := m.KVGet(ctx, command.Key, time.UnixMilli(1))
		if err != nil || !found || !bytes.Equal(got, command.Value) {
			t.Fatalf("raw key %x: %x %v %v", command.Key, got, found, err)
		}
		fingerprint, err := types.ManagedKVFingerprint(command)
		if err != nil {
			t.Fatal(err)
		}
		receipt, found, matches, err := m.KVRequestStatusFingerprint(ctx, id, fingerprint)
		if err != nil || !found || !matches || receipt.Slot != uint64(i+1) {
			t.Fatalf("raw ID %x: %+v %v %v %v", id, receipt, found, matches, err)
		}
		changed := command
		changed.Key += "x"
		if matches, err := m.KVRequestMatches(ctx, changed); err != nil || matches {
			t.Fatalf("changed raw intent: %v %v", matches, err)
		}
	}
	if m.MaterializationPolicy() != sqlpolicy.ManagedSQLKVMaterializationPolicy || m.graph != nil {
		t.Fatal("profile not fixed8 Graph-less")
	}
	for _, value := range [][]byte{nil, {}} {
		command := types.KVCommand{RequestID: fmt.Sprint("empty-", value == nil), Operation: "put", Key: "empty", Value: value}
		managedKVApply(t, m, command)
		got, found, err := m.KVGet(ctx, command.Key, time.UnixMilli(1))
		if err != nil || !found || !bytes.Equal(got, value) {
			t.Fatalf("empty read: %x %v %v", got, found, err)
		}
		var storage string
		if err := m.writer.QueryRow(`SELECT typeof(value) FROM _rhiza_kv WHERE key=?`, command.Key).Scan(&storage); err != nil {
			t.Fatal(err)
		}
		if storage != "blob" {
			t.Fatalf("empty bytes changed storage class: %s", storage)
		}
		fingerprint, err := types.ManagedKVFingerprint(command)
		if err != nil {
			t.Fatal(err)
		}
		command.Value = []byte{}
		other, err := types.ManagedKVFingerprint(command)
		if err != nil || fingerprint != other {
			t.Fatal("nil/empty fingerprint contract")
		}
	}
}

func TestManagedApplicationCommitUnknownGuards(t *testing.T) {
	for _, operation := range []string{"sql", "put", "delete", "cas"} {
		t.Run(operation, func(t *testing.T) {
			m := managedMaterializer(t)
			ctx := context.Background()
			var value []byte
			var err error
			if operation == "sql" {
				value, err = types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "sql", Statements: []types.SQLStatement{{SQL: "CREATE TABLE item(v)"}}}})
			} else {
				value, err = types.EncodeManagedKVCommand(types.KVCommand{RequestID: "kv", Operation: operation, Key: "key", Value: []byte("v")})
			}
			if err != nil {
				t.Fatal(err)
			}
			m.afterApplyCommitError = func() error { return errors.New("lost ACK") }
			if err := m.Apply(ctx, 1, value); !errors.Is(err, ErrGraphCommitUnknown) {
				t.Fatal(err)
			}
			checks := []func() error{
				func() error { _, _, err := m.KVGet(ctx, "key", time.Now()); return err },
				func() error { _, _, _, err := m.KVGetAt(ctx, "key", time.Now()); return err },
				func() error { _, _, err := m.MutationReceipt(ctx, types.MutationKV, "kv"); return err },
				func() error {
					_, err := m.KVRequestMatches(ctx, types.KVCommand{RequestID: "kv", Operation: "put", Key: "key"})
					return err
				},
				func() error { _, _, _, err := m.KVRequestStatusFingerprint(ctx, "kv", [32]byte{}); return err },
				func() error { _, _, _, err := m.SQLRequestStatusFingerprint(ctx, "sql", [32]byte{}); return err },
				func() error { _, _, err := m.QueryResultAt(ctx, "SELECT 1", nil); return err },
				func() error { _, err := m.InspectManagedApplicationState(ctx); return err },
				func() error {
					_, _, cleanup, err := m.CheckpointFilesAt(ctx)
					if cleanup != nil {
						cleanup()
					}
					return err
				},
				func() error { return m.Health(ctx) },
				func() error { return m.ValidateTip(0, value) },
				func() error { return m.Apply(ctx, 1, value) },
			}
			for i, check := range checks {
				if err := check(); !errors.Is(err, ErrGraphCommitUnknown) {
					t.Fatalf("guard %d: %v", i, err)
				}
			}
		})
	}
}

func TestManagedApplicationArtifactBeforeEffects(t *testing.T) {
	for _, artifact := range []string{"latticedb", "latticedb.restore-backup", "sqlite.db.restore-state.json", "sqlite.db.local-restore.json", ".rhiza-local-restore-old", ".rhiza-checkpoint-graph-old", "symlink"} {
		t.Run(artifact, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, artifact)
			if artifact == "symlink" {
				path = filepath.Join(dir, "latticedb")
				if err := os.Symlink("missing", path); err != nil {
					t.Fatal(err)
				}
			} else if err := os.Mkdir(path, 0700); err != nil {
				t.Fatal(err)
			}
			m, err := OpenManagedSQLKV(filepath.Join(dir, "sqlite.db"), 1, 1024)
			if err == nil {
				m.Close()
				t.Fatal("artifact accepted")
			}
			if _, err := os.Lstat(path); err != nil {
				t.Fatal("evidence changed", err)
			}
			if _, err := os.Lstat(filepath.Join(dir, "sqlite.db")); !os.IsNotExist(err) {
				t.Fatal("writer entered", err)
			}
		})
	}
}

func TestManagedApplicationReadonlyRestoreState(t *testing.T) {
	for _, name := range []string{"latticedb", "latticedb.restore-backup", "sqlite.db.restore-state.json", "sqlite.db.restore-backup", "sqlite.db.local-restore.json", ".rhiza-local-restore-left", ".rhiza-restore-left", ".rhiza-graph-restore-left", ".rhiza-checkpoint-graph-left"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			entry := filepath.Join(dir, name)
			if err := os.WriteFile(entry, []byte("retained"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := ValidateManagedRestoreState(filepath.Join(dir, "sqlite.db")); err == nil {
				t.Fatal("residue accepted")
			}
			data, err := os.ReadFile(entry)
			if err != nil || string(data) != "retained" {
				t.Fatal("readonly check changed evidence", err)
			}
			if _, err := os.Lstat(filepath.Join(dir, "sqlite.db")); !os.IsNotExist(err) {
				t.Fatal("readonly check opened writer", err)
			}
		})
	}
	if err := ValidateManagedRestoreState(filepath.Join(t.TempDir(), "sqlite.db")); err != nil {
		t.Fatal("clean state rejected", err)
	}
}

func TestManagedApplicationRestoreLeavesNoArtifacts(t *testing.T) {
	m := managedMaterializer(t)
	managedKVApply(t, m, types.KVCommand{RequestID: "restore", Operation: "put", Key: "key", Value: []byte("v")})
	files, _, cleanup, err := m.CheckpointFilesAt(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if err := m.RestoreCheckpoint(context.Background(), files); err != nil {
		t.Fatal(err)
	}
	cleanup()
	if err := ValidateManagedRestoreState(m.dbPath); err != nil {
		t.Fatalf("successful restore left its own interrupted artifacts: %v", err)
	}
}

func TestManagedApplicationSnapshotPoisonBoundary(t *testing.T) {
	m := managedMaterializer(t)
	ctx := context.Background()
	snapshot, err := m.beginSQLiteReadSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	if m.mu.TryLock() {
		m.mu.Unlock()
		t.Fatal("snapshot did not retain apply/lifetime guard")
	}
	value, err := types.EncodeManagedKVCommand(types.KVCommand{RequestID: "late", Operation: "put", Key: "k"})
	if err != nil {
		t.Fatal(err)
	}
	m.afterApplyCommitError = func() error { return errors.New("lost ACK") }
	done := make(chan error, 1)
	go func() { done <- m.Apply(ctx, 1, value) }()
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, ErrGraphCommitUnknown) {
		t.Fatal(err)
	}
	if _, err := m.beginSQLiteReadSnapshot(ctx); !errors.Is(err, ErrGraphCommitUnknown) {
		t.Fatal(err)
	}
	// A private reopen cannot erase uncertainty on the same object.
	m.mu.Lock()
	if err := m.closeConnections(); err != nil {
		m.mu.Unlock()
		t.Fatal(err)
	}
	if err := m.reopen(); err != nil {
		m.mu.Unlock()
		t.Fatal(err)
	}
	m.mu.Unlock()
	if err := m.Health(ctx); !errors.Is(err, ErrGraphCommitUnknown) {
		t.Fatal("reopen cleared poison", err)
	}
}

func managedMaterializer(t *testing.T) *Materializer {
	t.Helper()
	m, err := OpenManagedSQLKV(filepath.Join(t.TempDir(), "sqlite.db"), 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Error(err)
		}
	})
	return m
}

func managedKVApply(t *testing.T, m *Materializer, command types.KVCommand) {
	t.Helper()
	value, err := types.EncodeManagedKVCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(context.Background(), m.Tip()+1, value); err != nil {
		t.Fatal(err)
	}
}

func TestManagedApplicationPolicyOpen(t *testing.T) {
	ctx := context.Background()
	m := managedMaterializer(t)
	initial, err := m.InspectManagedApplicationState(ctx)
	if err != nil || !initial.FreshEmpty {
		t.Fatalf("fresh: %+v %v", initial, err)
	}
	if _, err := m.InspectSQLApplicationState(ctx); err == nil {
		t.Fatal("mixed accepted by SQL inspector")
	}
	path := m.dbPath
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if ordinary, err := Open(path, 1, 1024); err == nil {
		ordinary.Close()
		t.Fatal("legacy open accepted mixed")
	}
	reopened, err := OpenManagedSQLKV(path, 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	if wrong, err := OpenManagedSQLKV(path, 1, 1025); err == nil {
		wrong.Close()
		t.Fatal("window mismatch accepted")
	}
	legacyPath := filepath.Join(t.TempDir(), "sqlite.db")
	legacy, err := Open(legacyPath, 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}
	if converted, err := OpenManagedSQLKV(legacyPath, 1, 1024); err == nil {
		converted.Close()
		t.Fatal("empty legacy converted")
	}
}

func TestManagedApplicationLegacyMissingMetadataError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, err := openSQLite("file:"+path, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE sentinel(v)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	opened, err := Open(path, 1)
	if opened != nil {
		opened.Close()
	}
	if !errors.Is(err, sqlpolicy.ErrIncompatible) {
		t.Fatalf("legacy policy error changed: %v", err)
	}
}

func TestManagedApplicationControlPruningAndRestore(t *testing.T) {
	ctx := context.Background()
	m := managedMaterializer(t)
	evidenceApply(t, m, 1, "sql", `CREATE TABLE item (v INTEGER)`)
	managedKVApply(t, m, types.KVCommand{RequestID: "kv", Operation: "put", Key: "key", Value: []byte("value")})
	before, err := m.InspectManagedApplicationState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	oldUsage := m.sqlReceiptRetention
	var decisions []quepaxa.DecidedValue
	for slot := 3; slot <= 1027; slot++ {
		decisions = append(decisions, quepaxa.DecidedValue{Slot: quepaxa.Slot(slot), Value: types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{})})
	}
	if err := m.ApplyBatch(ctx, decisions); err != nil {
		t.Fatal(err)
	}
	after, err := m.InspectManagedApplicationState(ctx)
	if err != nil || after != before || m.sqlReceiptRetention != oldUsage {
		t.Fatalf("control drift: %+v %+v %v", before, after, err)
	}
	var retained int
	if err := m.writer.QueryRow(`SELECT COUNT(*) FROM _rhiza_idempotency`).Scan(&retained); err != nil || retained != 2 {
		t.Fatalf("physical retention %d %v", retained, err)
	}
	status, found, err := m.MutationReceipt(ctx, types.MutationKV, "kv")
	if err != nil || found {
		t.Fatalf("logical expiration: %+v %v %v", status, found, err)
	}
	files, _, cleanup, err := m.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if err := m.RestoreCheckpoint(ctx, files); err != nil {
		t.Fatal(err)
	}
	restored, err := m.InspectManagedApplicationState(ctx)
	if err != nil || restored != before || m.sqlReceiptRetention != oldUsage {
		t.Fatalf("restore: %+v %v", restored, err)
	}
	m.mu.Lock()
	err = m.closeConnections()
	if err != nil {
		m.mu.Unlock()
		t.Fatal(err)
	}
	if err := m.reopen(); err != nil {
		m.mu.Unlock()
		t.Fatal(err)
	}
	m.mu.Unlock()
	managedKVApply(t, m, types.KVCommand{RequestID: "next", Operation: "delete", Key: "absent"})
	if err := m.writer.QueryRow(`SELECT COUNT(*) FROM _rhiza_idempotency`).Scan(&retained); err != nil || retained != 1 {
		t.Fatalf("application prune %d %v", retained, err)
	}
	if m.sqlReceiptRetention.receipts != 0 || m.sqlReceiptRetention.resultBytes != 0 {
		t.Fatal("expired SQL accounting retained")
	}
	if state, err := m.InspectManagedApplicationState(ctx); err != nil || state.Digest == before.Digest || state.FreshEmpty {
		t.Fatalf("new state: %+v %v", state, err)
	}
}

func TestManagedApplicationRejectsUnsupportedBeforeEffects(t *testing.T) {
	m := managedMaterializer(t)
	graph, err := types.EncodeGraphBatch([]types.GraphCommand{{RequestID: "g", Cypher: `CREATE (:Item {id:'bad'})`}})
	if err != nil {
		t.Fatal(err)
	}
	notify, err := types.EncodeNotifyCommand(types.NotifyCommand{RequestID: "n", Topic: "topic", Payload: []byte("x")})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range [][]byte{graph, notify, []byte("unknown")} {
		if err := m.Apply(context.Background(), 1, value); err == nil {
			t.Fatal("unsupported accepted")
		}
		if m.Tip() != 0 || m.graph != nil {
			t.Fatal("unsupported changed tips")
		}
	}
	if state, err := m.InspectManagedApplicationState(context.Background()); err != nil || !state.FreshEmpty {
		t.Fatalf("unsupported changed state: %+v %v", state, err)
	}
}

func TestManagedApplicationRestorePolicyRefusal(t *testing.T) {
	ctx := context.Background()
	m := managedMaterializer(t)
	legacy, err := Open(filepath.Join(t.TempDir(), "sqlite.db"), 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Close()
	legacyFiles, _, legacyCleanup, err := legacy.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer legacyCleanup()
	before, err := m.InspectManagedApplicationState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.RestoreCheckpoint(ctx, legacyFiles); err == nil {
		t.Fatal("mixed imported legacy checkpoint")
	}
	after, err := m.InspectManagedApplicationState(ctx)
	if err != nil || before != after {
		t.Fatalf("refused restore changed state: %v", err)
	}
	mixedFiles, _, mixedCleanup, err := m.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer mixedCleanup()
	if err := legacy.RestoreCheckpoint(ctx, mixedFiles); err == nil {
		t.Fatal("legacy imported mixed checkpoint")
	}
	for _, files := range [][]CheckpointFile{legacyFiles, mixedFiles} {
		for _, file := range files {
			if file.Role == CheckpointSQLite {
				for _, suffix := range []string{"-wal", "-shm"} {
					if _, err := os.Lstat(file.Path + suffix); !os.IsNotExist(err) {
						t.Fatalf("source snapshot sidecar created: %s %v", suffix, err)
					}
				}
			}
		}
	}
}

func TestManagedApplicationExpiryRowidResults(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprint(reverse), func(t *testing.T) {
			m := managedMaterializer(t)
			// The same triples have different physical rowids, exactly as the
			// pinned SQLite expiry implementation observes them.
			for i := 0; i < 257; i++ {
				key := i
				if reverse {
					key = 256 - i
				}
				if _, err := m.writer.Exec(`INSERT INTO _rhiza_kv(key,value,expires_at_unix_ms) VALUES(?,?,1)`, fmt.Sprintf("k%03d", key), []byte("v")); err != nil {
					t.Fatal(err)
				}
			}
			before, err := m.InspectManagedApplicationState(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			files, _, cleanup, err := m.CheckpointFilesAt(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer cleanup()
			if err := m.RestoreCheckpoint(context.Background(), files); err != nil {
				t.Fatal(err)
			}
			after, err := m.InspectManagedApplicationState(context.Background())
			if err != nil || before != after {
				t.Fatalf("expiry snapshot changed: %+v %+v %v", before, after, err)
			}
			managedKVApply(t, m, types.KVCommand{RequestID: "delete", Operation: "delete", Key: "k256", ObservedAtUnixMS: 2})
			receipt, found, err := m.MutationReceipt(context.Background(), types.MutationKV, "delete")
			if err != nil || !found || receipt.Applied == reverse {
				t.Fatalf("expiry result %+v %v %v", receipt, found, err)
			}
		})
	}
}

func TestManagedApplicationKVExactReceipt(t *testing.T) {
	m := managedMaterializer(t)
	ctx := context.Background()
	command := types.KVCommand{RequestID: "\ufffd", Operation: "cas", Key: "absent", ExpectedExists: true, Value: []byte("new")}
	managedKVApply(t, m, command)
	fingerprint, err := types.ManagedKVFingerprint(command)
	if err != nil {
		t.Fatal(err)
	}
	receipt, found, matches, err := m.KVRequestStatusFingerprint(ctx, command.RequestID, fingerprint)
	if err != nil || !found || !matches || receipt.Applied || receipt.Slot != 1 || receipt.Status != types.MutationCommitted || receipt.RetryThroughSlot != 1024 {
		t.Fatalf("false CAS receipt: %+v %v %v %v", receipt, found, matches, err)
	}
	wrong := fingerprint
	wrong[0] ^= 1
	if got, found, matches, err := m.KVRequestStatusFingerprint(ctx, command.RequestID, wrong); err != nil || !found || matches || got != receipt {
		t.Fatalf("mismatch observation: %+v %v %v %v", got, found, matches, err)
	}
	if _, found, matches, err := m.KVRequestStatusFingerprint(ctx, "missing", fingerprint); err != nil || found || !matches {
		t.Fatalf("missing observation: %v %v %v", found, matches, err)
	}
	var controls []quepaxa.DecidedValue
	for slot := 2; slot <= 1025; slot++ {
		controls = append(controls, quepaxa.DecidedValue{Slot: quepaxa.Slot(slot), Value: types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{})})
	}
	if err := m.ApplyBatch(ctx, controls); err != nil {
		t.Fatal(err)
	}
	if _, found, matches, err := m.KVRequestStatusFingerprint(ctx, command.RequestID, fingerprint); err != nil || found || !matches {
		t.Fatalf("expired observation: %v %v %v", found, matches, err)
	}
	// Occupy the only reader connection. WaitCount proves the real observer
	// reached its SQL read, rather than merely scheduling a goroutine.
	connection, err := m.readers[0].Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	waits := m.readers[0].Stats().WaitCount
	changed := command
	changed.Operation = "put"
	value, err := types.EncodeManagedKVCommand(changed)
	if err != nil {
		t.Fatal(err)
	}
	type observation struct {
		receipt        types.MutationReceipt
		found, matches bool
		err            error
	}
	observed := make(chan observation, 1)
	go func() {
		receipt, found, matches, err := m.KVRequestStatusFingerprint(ctx, command.RequestID, fingerprint)
		observed <- observation{receipt, found, matches, err}
	}()
	deadline := time.Now().Add(3 * time.Second)
	for m.readers[0].Stats().WaitCount == waits {
		if time.Now().After(deadline) {
			t.Fatal("observer never reached actual reader pool")
		}
		runtime.Gosched()
	}
	if m.mu.TryLock() {
		m.mu.Unlock()
		t.Fatal("observer did not retain apply guard during receipt read")
	}
	applyDone := make(chan error, 1)
	go func() { applyDone <- m.Apply(ctx, 1026, value) }()
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	got := <-observed
	if got.err != nil || got.found || !got.matches {
		t.Fatalf("old expired snapshot: %+v", got)
	}
	if err := <-applyDone; err != nil {
		t.Fatal(err)
	}
	if receipt, found, matches, err := m.KVRequestStatusFingerprint(ctx, command.RequestID, fingerprint); err != nil || !found || matches || receipt.Slot != 1026 || !receipt.Applied {
		t.Fatalf("new mismatch snapshot: %+v %v %v %v", receipt, found, matches, err)
	}
	newFingerprint, err := types.ManagedKVFingerprint(changed)
	if err != nil {
		t.Fatal(err)
	}
	if got, found, matches, err := m.KVRequestStatusFingerprint(ctx, command.RequestID, newFingerprint); err != nil || !found || !matches || got.Slot != 1026 {
		t.Fatalf("new receipt: %+v %v %v %v", got, found, matches, err)
	}
}
