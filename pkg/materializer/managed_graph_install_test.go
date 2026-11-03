package materializer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	latticedb "github.com/mrchypark/latticedb-go"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
)

func managedGraphInstallFixture(t *testing.T) ManagedGraphInstall {
	return managedGraphInstallFixtureForProtocol(t, "")
}

func managedGraphInstallFixtureForProtocol(t *testing.T, protocol string) ManagedGraphInstall {
	t.Helper()
	path := filepath.Join(t.TempDir(), "sqlite.db")
	var m *Materializer
	var err error
	if protocol == "" || protocol == sqlpolicy.ManagedGraphRequestApplicationProtocol {
		m, err = OpenManagedSQLKVGraphWithOriginals(path, 1, 1024)
	} else {
		m, err = OpenManagedSQLKVGraphWithAppendOnlyOriginals(path, 1, 1024)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := m.Apply(context.Background(), 1, graphOriginalValue(t, types.GraphCommand{RequestID: "verified", Cypher: `CREATE (:Item {id: 'verified'})`})); err != nil {
		t.Fatal(err)
	}
	files, tip, cleanup, err := m.CheckpointFilesAt(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cleanup)
	for i := range files {
		length, hash, err := hashLocalRestoreFile(files[i].Path)
		if err != nil {
			t.Fatal(err)
		}
		files[i].ExpectedLength, files[i].ExpectedSHA256 = length, hash
	}
	history, err := m.ManagedGraphHistory(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// This is an installer unit boundary, not a Core certificate fixture.
	// Node authenticates the cut in its separate public startup qualification.
	h := sha256.Sum256([]byte("unit-owner-authenticated-cut"))
	return ManagedGraphInstall{ApplicationProtocol: protocol, Cut: ManagedGraphInstallCut{BindingHash: h, WALBindingHash: h, ConfigID: 1,
		CheckpointSlot: tip, CheckpointRoot: h, CheckpointStateHash: h, CheckpointPrefixHash: h,
		CertifiedTip: tip, CertifiedPrefixHash: h, ExpectedGraphHistory: history, IdempotencyWindow: 1024}, Files: files}
}

func TestManagedAppendOnlyGraphInstallAndInspection(t *testing.T) {
	protocol := sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol
	input := managedGraphInstallFixtureForProtocol(t, protocol)
	path := filepath.Join(t.TempDir(), "sqlite.db")
	m, err := OpenManagedGraphFromVerifiedPair(context.Background(), path, 1, input)
	if err != nil {
		t.Fatal(err)
	}
	if m.managedProtocol != protocol || m.MaterializationPolicy() != sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		t.Fatalf("installed profile mismatch: protocol=%q policy=%d", m.managedProtocol, m.MaterializationPolicy())
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	state, err := InspectManagedGraphInstallTarget(context.Background(), path, 1024, protocol)
	if err != nil || state.SQLiteTip != input.Cut.CertifiedTip || state.NativeHistory != input.Cut.ExpectedGraphHistory {
		t.Fatalf("append-only Graph target inspection: state=%+v err=%v", state, err)
	}
	if _, err := InspectManagedGraphInstallTarget(context.Background(), path, 1024); err == nil {
		t.Fatal("legacy Graph target inspection accepted append-only protocol")
	}
	if _, err := OpenManagedGraphFromVerifiedPair(context.Background(), path, 1, ManagedGraphInstall{Cut: input.Cut, Files: input.Files}); err == nil {
		t.Fatal("legacy Graph install recovery accepted append-only protocol")
	}
}

func TestManagedGraphInstallPairActualTarget(t *testing.T) {
	input := managedGraphInstallFixture(t)
	path := filepath.Join(t.TempDir(), "sqlite.db")
	old, err := OpenManagedSQLKVGraphWithOriginals(path, 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if err := old.Apply(context.Background(), 1, graphOriginalValue(t, types.GraphCommand{RequestID: "old", Cypher: `CREATE (:Item {id: 'old'})`})); err != nil {
		t.Fatal(err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	m, err := OpenManagedGraphFromVerifiedPair(context.Background(), path, 1, input)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if m.dbPath != path || m.Tip() != input.Cut.CertifiedTip {
		t.Fatalf("wrong target %q tip=%d", m.dbPath, m.Tip())
	}
	retainedOriginal(t, m, types.MutationGraph, "verified")
	rows, err := m.GraphQuery(context.Background(), `MATCH (n:Item) RETURN n.id`, nil)
	if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != "verified" {
		t.Fatalf("wrong opened native %v %v", rows.Rows, err)
	}
	if err := ValidateNoLocalRestore(path); err != nil {
		t.Fatal(err)
	}
}

func TestManagedGraphInstallRollbackAbsentPair(t *testing.T) {
	input := managedGraphInstallFixture(t)
	path := filepath.Join(t.TempDir(), "sqlite.db")
	previous := localRestoreFailpoint
	localRestoreFailpoint = func(site string) error {
		if site == "after-publish-graph" {
			return fmt.Errorf("injected install failure")
		}
		return nil
	}
	defer func() { localRestoreFailpoint = previous }()
	if m, err := OpenManagedGraphFromVerifiedPair(context.Background(), path, 1, input); err == nil {
		m.Close()
		t.Fatal("fault did not fail")
	}
	for _, name := range []string{path, path + "-wal", path + "-shm", path + "-journal", filepath.Join(filepath.Dir(path), "latticedb"), localRestoreJournalPath(path)} {
		if _, err := os.Lstat(name); !os.IsNotExist(err) {
			t.Fatalf("original absence not restored %s: %v", name, err)
		}
	}
}

func TestManagedGraphInstallDirtyWALHelper(t *testing.T) {
	path := os.Getenv("RHIZA_TEST_INSTALL_DIRTY_WAL")
	if path == "" {
		return
	}
	db, err := openSQLite((&url.URL{Scheme: "file", Path: filepath.ToSlash(path)}).String(), false)
	if err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{"PRAGMA journal_mode=WAL", "PRAGMA wal_autocheckpoint=0", "CREATE TABLE old_wal_only(value TEXT)", "INSERT INTO old_wal_only VALUES ('preserved')"} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatal(err)
		}
	}
	// A real abrupt process exit retains actual committed WAL bytes. It is not
	// a malformed sidecar fixture and does not leave an old writer running.
	os.Exit(0)
}

func TestManagedGraphInstallDirtyWALRollback(t *testing.T) {
	input := managedGraphInstallFixture(t)
	path := filepath.Join(t.TempDir(), "sqlite.db")
	m, err := OpenManagedSQLKVGraphWithOriginals(path, 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestManagedGraphInstallDirtyWALHelper$")
	cmd.Env = append(os.Environ(), "RHIZA_TEST_INSTALL_DIRTY_WAL="+path)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("dirty WAL child: %v %s", err, output)
	}
	if info, err := os.Stat(path + "-wal"); err != nil || info.Size() == 0 {
		t.Fatalf("no actual dirty WAL: %v %v", info, err)
	}
	// Preflight may create reader sidecars but must not discard committed WAL.
	if err := validateManagedGraphInstallTarget(context.Background(), path, 1024, sqlpolicy.ManagedGraphRequestApplicationProtocol); err != nil {
		t.Fatal(err)
	}
	beforeSQL, err := inspectLocalOriginalSQL(path)
	if err != nil {
		t.Fatal(err)
	}
	beforeGraph, _, err := inspectLocalOriginalGraph(filepath.Join(filepath.Dir(path), "latticedb"))
	if err != nil {
		t.Fatal(err)
	}
	previous := localRestoreFailpoint
	localRestoreFailpoint = func(site string) error {
		if site == "after-publish-graph" {
			return fmt.Errorf("injected install failure")
		}
		return nil
	}
	defer func() { localRestoreFailpoint = previous }()
	if opened, err := OpenManagedGraphFromVerifiedPair(context.Background(), path, 1, input); err == nil {
		opened.Close()
		t.Fatal("fault did not fail")
	}
	afterSQL, err := inspectLocalOriginalSQL(path)
	if err != nil || !reflect.DeepEqual(beforeSQL, afterSQL) {
		t.Fatalf("SQL/sidecars changed: before=%v after=%v err=%v", beforeSQL, afterSQL, err)
	}
	afterGraph, _, err := inspectLocalOriginalGraph(filepath.Join(filepath.Dir(path), "latticedb"))
	if err != nil || !reflect.DeepEqual(beforeGraph, afterGraph) {
		t.Fatalf("native changed: %v", err)
	}
	if err := ValidateNoLocalRestore(path); err != nil {
		t.Fatal(err)
	}
	db, err := openSQLite((&url.URL{Scheme: "file", Path: filepath.ToSlash(path)}).String()+"?mode=ro", false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var value string
	if err := db.QueryRow("SELECT value FROM old_wal_only").Scan(&value); err != nil || value != "preserved" {
		t.Fatalf("WAL commit lost: %q %v", value, err)
	}
}

func TestManagedGraphInstallRejectsAliasedOrInvalidCut(t *testing.T) {
	input := managedGraphInstallFixture(t)
	path := filepath.Join(t.TempDir(), "sqlite.db")
	bad := input
	bad.Cut.CheckpointRoot = [32]byte{}
	if m, err := OpenManagedGraphFromVerifiedPair(context.Background(), path, 1, bad); err == nil {
		m.Close()
		t.Fatal("missing root accepted")
	}
	bad = input
	bad.Files = append([]CheckpointFile(nil), input.Files...)
	bad.Files[0].ExpectedSHA256 = hex.EncodeToString(make([]byte, 32))
	if m, err := OpenManagedGraphFromVerifiedPair(context.Background(), path, 1, bad); err == nil {
		m.Close()
		t.Fatal("wrong hash accepted")
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("rejected input installed target: %v", err)
	}
}

func TestManagedGraphInstallPreservesPendingAndAlias(t *testing.T) {
	input := managedGraphInstallFixture(t)
	path := filepath.Join(t.TempDir(), "sqlite.db")
	m, err := OpenManagedSQLKVGraphWithOriginals(path, 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	hardLink := filepath.Join(t.TempDir(), "alias.db")
	if err := os.Link(path, hardLink); err != nil {
		t.Fatal(err)
	}
	bad := input
	bad.Files = append([]CheckpointFile(nil), input.Files...)
	bad.Files[0].Path = hardLink
	length, hash, err := hashLocalRestoreFile(path)
	if err != nil {
		t.Fatal(err)
	}
	bad.Files[0].ExpectedLength, bad.Files[0].ExpectedSHA256 = length, hash
	if opened, err := OpenManagedGraphFromVerifiedPair(context.Background(), path, 1, bad); err == nil {
		opened.Close()
		t.Fatal("hardlinked target source accepted")
	}
	if err := ValidateNoLocalRestore(path); err != nil {
		t.Fatal(err)
	}
	db, err := latticedb.Open(filepath.Join(filepath.Dir(path), "latticedb", "graph.ltdb"), latticedb.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pending := graphJournalEntry{Slot: 1, Hash: sha256.Sum256([]byte("pending"))}
	if err := db.Update(func(tx *latticedb.Tx) error { return tx.PutAppMetadata(graphPendingKey, encodeGraphPending(pending)) }); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before, _, err := inspectLocalOriginalGraph(filepath.Join(filepath.Dir(path), "latticedb"))
	if err != nil {
		t.Fatal(err)
	}
	if opened, err := OpenManagedGraphFromVerifiedPair(context.Background(), path, 1, input); err == nil {
		opened.Close()
		t.Fatal("pending native discarded")
	}
	after, _, err := inspectLocalOriginalGraph(filepath.Join(filepath.Dir(path), "latticedb"))
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("pending native changed: %v", err)
	}
}

func TestManagedGraphInstallRejectsWrongExpectedTip(t *testing.T) {
	input := managedGraphInstallFixture(t)
	path := filepath.Join(t.TempDir(), "sqlite.db")
	// Exact source bytes still fail before publication if final state is wrong.
	input.Cut.CertifiedTip++
	if opened, err := OpenManagedGraphFromVerifiedPair(context.Background(), path, 1, input); err == nil {
		opened.Close()
		t.Fatal("wrong certified tip accepted")
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("wrong cut installed %v", err)
	}
	if err := ValidateNoLocalRestore(path); err != nil {
		t.Fatal(err)
	}
}

func TestManagedGraphInstallUnknownTargetPreservesJournal(t *testing.T) {
	input := managedGraphInstallFixture(t)
	path := filepath.Join(t.TempDir(), "sqlite.db")
	m, err := OpenManagedSQLKVGraphWithOriginals(path, 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	previous := localRestoreFailpoint
	localRestoreFailpoint = func(site string) error {
		if site == "after-backup-sql" {
			if err := os.WriteFile(path, []byte("unaccounted-target"), 0o600); err != nil {
				return err
			}
			return fmt.Errorf("injected competing change")
		}
		return nil
	}
	defer func() { localRestoreFailpoint = previous }()
	if opened, err := OpenManagedGraphFromVerifiedPair(context.Background(), path, 1, input); err == nil {
		opened.Close()
		t.Fatal("unaccounted state accepted")
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "unaccounted-target" {
		t.Fatalf("unknown target overwritten: %q %v", data, err)
	}
	data, err = os.ReadFile(localRestoreJournalPath(path))
	if err != nil {
		t.Fatal(err)
	}
	var journal localRestoreJournal
	if err := json.Unmarshal(data, &journal); err != nil {
		t.Fatal(err)
	}
	if journal.ManagedGraph == nil || journal.ManagedGraph.Cut != input.Cut || journal.ManagedGraph.Digest == ([32]byte{}) || journal.LocalVersion != 0 {
		t.Fatal("journal lost exact managed cut")
	}
	if _, err := readLocalRestoreJournal(localRestoreJournalPath(path)); err == nil {
		t.Fatal("managed journal accepted as Local authority")
	}
	backup := filepath.Join(localRestoreAttemptPath(path, journal.AttemptID), "backup", filepath.Base(path))
	if !matchesLocalInventory(backup, findLocalOriginal(journal.OriginalSQL, filepath.Base(path))) {
		t.Fatal("original backup lost")
	}
}

func TestManagedGraphInstallCancellationRestoresAbsence(t *testing.T) {
	input := managedGraphInstallFixture(t)
	path := filepath.Join(t.TempDir(), "sqlite.db")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if opened, err := OpenManagedGraphFromVerifiedPair(ctx, path, 1, input); err == nil || !errors.Is(err, context.Canceled) {
		if opened != nil {
			opened.Close()
		}
		t.Fatalf("cancellation=%v", err)
	}
	if err := ValidateNoLocalRestore(path); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("cancelled installation produced target: %v", err)
	}
	if entries, err := os.ReadDir(filepath.Dir(path)); err != nil || len(entries) != 0 {
		t.Fatalf("pre-canceled install created restore artifacts: entries=%v error=%v", entries, err)
	}
}

func TestManagedGraphInstallPreCanceledPreservesUnknownState(t *testing.T) {
	input := managedGraphInstallFixture(t)
	path := filepath.Join(t.TempDir(), "sqlite.db")
	journal := []byte("unknown pre-existing restore state")
	if err := os.WriteFile(localRestoreJournalPath(path), journal, 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if opened, err := OpenManagedGraphFromVerifiedPair(ctx, path, 1, input); opened != nil || !errors.Is(err, context.Canceled) {
		if opened != nil {
			opened.Close()
		}
		t.Fatalf("pre-canceled unknown-state install: %v", err)
	}
	after, err := os.ReadFile(localRestoreJournalPath(path))
	if err != nil || !bytes.Equal(after, journal) {
		t.Fatal("pre-canceled call changed unknown journal", err)
	}
	if err := ValidateNoLocalRestore(path); err == nil {
		t.Fatal("unknown journal no longer blocks startup")
	}
}

func TestManagedGraphInstallPreflightCancellationCreatesNoIntent(t *testing.T) {
	input := managedGraphInstallFixture(t)
	path := filepath.Join(t.TempDir(), "sqlite.db")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	previous := localRestoreFailpoint
	observed := false
	localRestoreFailpoint = func(site string) error {
		if site == "managed:before-intent" {
			observed = true
			cancel()
		}
		return nil
	}
	opened, err := OpenManagedGraphFromVerifiedPair(ctx, path, 1, input)
	localRestoreFailpoint = previous
	if opened != nil {
		opened.Close()
	}
	if !observed || opened != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("actual preflight cancellation was not preserved: observed=%t error=%v", observed, err)
	}
	if entries, err := os.ReadDir(filepath.Dir(path)); err != nil || len(entries) != 0 {
		t.Fatalf("preflight cancellation created intent/artifacts: %v %v", entries, err)
	}
	// A fresh, non-canceled caller must still install the actual same pair.
	installed, err := OpenManagedGraphFromVerifiedPair(context.Background(), path, 1, input)
	if err != nil {
		t.Fatal(err)
	}
	defer installed.Close()
	retainedOriginal(t, installed, types.MutationGraph, "verified")
	if err := ValidateNoLocalRestore(path); err != nil {
		t.Fatal(err)
	}
}

func TestManagedGraphTargetInspectionReturnsActualNativeAhead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sqlite.db")
	absent, err := InspectManagedGraphInstallTarget(context.Background(), path, 1024)
	if err != nil || !absent.Absent || absent.InventoryHash == ([32]byte{}) {
		t.Fatalf("absent observation: %+v %v", absent, err)
	}
	m, err := OpenManagedSQLKVGraphWithOriginals(path, 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	value := graphOriginalValue(t, types.GraphCommand{RequestID: "actual-ahead", Cypher: `CREATE (:Item {id:'first'})`})
	if err := m.Apply(newCancelAfterGraphCommitContext(m.graph.db), 1, value); err == nil {
		t.Fatal("expected actual native/SQLite split")
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	state, err := InspectManagedGraphInstallTarget(context.Background(), path, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if state.Absent || state.SQLiteTip != 0 || state.NativeTip != 1 || len(state.Replay) != 1 || state.InventoryHash == ([32]byte{}) {
		t.Fatalf("actual replay state=%+v", state)
	}
	record := state.Replay[0]
	if !record.NativeCompleted || record.Slot != 1 || record.DecisionHash != sha256.Sum256(value) {
		t.Fatalf("not actual full decision identity: %+v", record)
	}
	if err := validateManagedGraphInstallTarget(context.Background(), path, 1024, sqlpolicy.ManagedGraphRequestApplicationProtocol); !errors.Is(err, ErrGraphApplyPending) {
		t.Fatalf("installer did not preserve pending state: %v", err)
	}
	m, err = OpenManagedSQLKVGraphWithOriginals(path, 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(context.Background(), 1, value); err != nil {
		m.Close()
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	clean, err := InspectManagedGraphInstallTarget(context.Background(), path, 1024)
	if err != nil || clean.CommonTip != 1 || clean.SQLiteTip != 1 || clean.NativeTip != 1 || len(clean.Replay) != 0 {
		t.Fatalf("reconciled observation=%+v err=%v", clean, err)
	}
}

func TestManagedGraphInstallCrashHelper(t *testing.T) {
	inputPath := os.Getenv("RHIZA_TEST_INSTALL_INPUT")
	if inputPath == "" {
		return
	}
	data, err := os.ReadFile(inputPath)
	if err != nil {
		t.Fatal(err)
	}
	var input ManagedGraphInstall
	if err := json.Unmarshal(data, &input); err != nil {
		t.Fatal(err)
	}
	phase := os.Getenv("RHIZA_TEST_INSTALL_CRASH_PHASE")
	localRestoreFailpoint = func(site string) error {
		if site == phase {
			os.Exit(73)
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	m, err := OpenManagedGraphFromVerifiedPair(ctx, os.Getenv("RHIZA_TEST_INSTALL_TARGET"), 1, input)
	if err != nil {
		t.Fatal(err)
	}
	m.Close()
	t.Fatal("requested crash boundary was not reached")
}

func TestManagedGraphInstallAuthenticatedCrashRecovery(t *testing.T) {
	template := managedGraphInstallFixture(t)
	for _, dirty := range []bool{false, true} {
		phases := []string{"managed:after-staged", "after-publish-graph", "after-publish-sql", "managed:before-open", "managed:after-open", "managed:committed", "managed:after-attempt-cleanup", "managed:final-opened"}
		if dirty {
			phases = append(phases, "after-backup-sql", "after-backup-graph")
		}
		for _, phase := range phases {
			t.Run(fmt.Sprintf("dirty=%t/%s", dirty, phase), func(t *testing.T) {
				// Model Node's temporary reconstruction independently of the
				// fixture's source: it will be gone before the next public Open.
				input := template
				input.Files = append([]CheckpointFile(nil), template.Files...)
				source := t.TempDir()
				for i := range input.Files {
					file := &input.Files[i]
					target := filepath.Join(source, fmt.Sprintf("input-%d", i))
					if err := copyLocalRestoreFileContextLimit(context.Background(), file.Path, target, int64(file.ExpectedLength)+1); err != nil {
						t.Fatal(err)
					}
					file.Path = target
				}
				data, err := json.Marshal(input)
				if err != nil {
					t.Fatal(err)
				}
				inputPath := filepath.Join(source, "input.json")
				if err := os.WriteFile(inputPath, data, 0o600); err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(t.TempDir(), "sqlite.db")
				if dirty {
					m, err := OpenManagedSQLKVGraphWithOriginals(path, 1, 1024)
					if err != nil {
						t.Fatal(err)
					}
					if err := m.Close(); err != nil {
						t.Fatal(err)
					}
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestManagedGraphInstallDirtyWALHelper$")
					cmd.Env = append(os.Environ(), "RHIZA_TEST_INSTALL_DIRTY_WAL="+path)
					output, err := cmd.CombinedOutput()
					cancel()
					if err != nil {
						t.Fatalf("dirty child: %v %s", err, output)
					}
					if err := validateManagedGraphInstallTarget(context.Background(), path, 1024, sqlpolicy.ManagedGraphRequestApplicationProtocol); err != nil {
						t.Fatal(err)
					}
				}
				beforeSQL, err := inspectLocalOriginalSQL(path)
				if err != nil {
					t.Fatal(err)
				}
				beforeGraph, hadGraph, err := inspectLocalOriginalGraph(filepath.Join(filepath.Dir(path), "latticedb"))
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestManagedGraphInstallCrashHelper$")
				cmd.Env = append(os.Environ(), "RHIZA_TEST_INSTALL_INPUT="+inputPath, "RHIZA_TEST_INSTALL_TARGET="+path, "RHIZA_TEST_INSTALL_CRASH_PHASE="+phase)
				output, runErr := cmd.CombinedOutput()
				cancel()
				var exit *exec.ExitError
				if !errors.As(runErr, &exit) || exit.ExitCode() != 73 {
					t.Fatalf("hard exit at %s: %v %s", phase, runErr, output)
				}
				t.Logf("owned child pid=%d reaped exit=%d", cmd.Process.Pid, exit.ExitCode())
				j, err := readManagedGraphInstallJournal(path)
				if err != nil {
					t.Fatal(err)
				}
				observed, err := InspectManagedGraphInstallTarget(context.Background(), path, 1024)
				if err != nil || observed.Absent || observed.InstallAttempt == nil || observed.InstallAttempt.AttemptID != j.AttemptID || observed.InstallAttempt.Cut != input.Cut || observed.InventoryHash == ([32]byte{}) {
					t.Fatalf("pending install observation is not an exact non-genesis descriptor: %+v %v", observed, err)
				}
				if err := os.RemoveAll(source); err != nil {
					t.Fatal(err)
				}
				if _, err := os.Lstat(source); !os.IsNotExist(err) {
					t.Fatal("owned original reconstruction source was not removed", err)
				}
				input.Files = observed.InstallAttempt.Files
				wrong := input
				wrong.Cut.WALBindingHash[0] ^= 1
				journalBefore, err := os.ReadFile(localRestoreJournalPath(path))
				if err != nil {
					t.Fatal(err)
				}
				if m, err := OpenManagedGraphFromVerifiedPair(context.Background(), path, 1, wrong); err == nil {
					m.Close()
					t.Fatal("different WAL binding resumed attempt")
				}
				journalAfter, err := os.ReadFile(localRestoreJournalPath(path))
				if err != nil || !reflect.DeepEqual(journalBefore, journalAfter) {
					t.Fatal("mismatched attempt journal changed")
				}
				previous := localRestoreFailpoint
				rollbackObserved := false
				localRestoreFailpoint = func(site string) error {
					if site != "managed:after-crash-rollback" {
						return nil
					}
					rollbackObserved = true
					sql, err := inspectLocalOriginalSQL(path)
					if err != nil || !reflect.DeepEqual(sql, beforeSQL) {
						return fmt.Errorf("original main/WAL/SHM not restored")
					}
					graph, exists, err := inspectLocalOriginalGraph(filepath.Join(filepath.Dir(path), "latticedb"))
					if err != nil || exists != hadGraph || !reflect.DeepEqual(graph, beforeGraph) {
						return fmt.Errorf("original native/presence not restored")
					}
					return nil
				}
				m, recoverErr := OpenManagedGraphFromVerifiedPair(context.Background(), path, 1, input)
				localRestoreFailpoint = previous
				if recoverErr != nil {
					t.Fatal(recoverErr)
				}
				defer m.Close()
				if j.Phase != "committed" && j.Phase != "final-opened" && !rollbackObserved {
					t.Fatal("precommit crash bypassed original rollback")
				}
				retainedOriginal(t, m, types.MutationGraph, "verified")
				if err := ValidateNoLocalRestore(path); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestManagedGraphInstallUnobservedOpenCrashFailsClosed(t *testing.T) {
	input := managedGraphInstallFixture(t)
	path := filepath.Join(t.TempDir(), "sqlite.db")
	inputPath := filepath.Join(t.TempDir(), "input.json")
	data, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(inputPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestManagedGraphInstallCrashHelper$")
	cmd.Env = append(os.Environ(), "RHIZA_TEST_INSTALL_INPUT="+inputPath, "RHIZA_TEST_INSTALL_TARGET="+path, "RHIZA_TEST_INSTALL_CRASH_PHASE=managed:after-open-before-inventory")
	output, runErr := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(runErr, &exit) || exit.ExitCode() != 73 {
		t.Fatalf("hard exit: %v %s", runErr, output)
	}
	t.Logf("owned child pid=%d reaped exit=%d", cmd.Process.Pid, exit.ExitCode())
	before, err := os.ReadFile(localRestoreJournalPath(path))
	if err != nil {
		t.Fatal(err)
	}
	if m, err := OpenManagedGraphFromVerifiedPair(context.Background(), path, 1, input); err == nil {
		m.Close()
		t.Fatal("unobserved changed native bytes were trusted")
	}
	after, err := os.ReadFile(localRestoreJournalPath(path))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("unknown-open journal changed")
	}
	j, err := readManagedGraphInstallJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(localRestoreAttemptPath(path, j.AttemptID)); err != nil {
		t.Fatal("unknown-open evidence removed", err)
	}
}

func TestManagedGraphInstallRetainedInputRejectsChanges(t *testing.T) {
	input := managedGraphInstallFixture(t)
	for _, kind := range []string{"missing", "changed", "symlink", "attempt-alias", "extra-input"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "sqlite.db")
			data, err := json.Marshal(input)
			if err != nil {
				t.Fatal(err)
			}
			inputPath := filepath.Join(t.TempDir(), "input.json")
			if err := os.WriteFile(inputPath, data, 0o600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestManagedGraphInstallCrashHelper$")
			cmd.Env = append(os.Environ(), "RHIZA_TEST_INSTALL_INPUT="+inputPath, "RHIZA_TEST_INSTALL_TARGET="+path, "RHIZA_TEST_INSTALL_CRASH_PHASE=managed:after-staged")
			output, runErr := cmd.CombinedOutput()
			cancel()
			var exit *exec.ExitError
			if !errors.As(runErr, &exit) || exit.ExitCode() != 73 {
				t.Fatalf("hard exit: %v %s", runErr, output)
			}
			t.Logf("owned child pid=%d reaped exit=%d", cmd.Process.Pid, exit.ExitCode())
			observed, err := InspectManagedGraphInstallTarget(context.Background(), path, 1024)
			if err != nil || observed.InstallAttempt == nil || len(observed.InstallAttempt.Files) != 2 {
				t.Fatal("missing retained input observation", err)
			}
			resume := ManagedGraphInstall{Cut: input.Cut, Files: append([]CheckpointFile(nil), observed.InstallAttempt.Files...)}
			before, err := os.ReadFile(localRestoreJournalPath(path))
			if err != nil {
				t.Fatal(err)
			}
			file := resume.Files[0]
			attempt := localRestoreAttemptPath(path, observed.InstallAttempt.AttemptID)
			switch kind {
			case "missing":
				err = os.Remove(file.Path)
			case "changed":
				err = os.WriteFile(file.Path, []byte("untrusted source"), 0o600)
			case "symlink":
				err = os.Remove(file.Path)
				if err == nil {
					err = os.Symlink(input.Files[0].Path, file.Path)
				}
			case "attempt-alias":
				resume.Files[0].Path = filepath.Join(attempt, "other-input")
				err = copyLocalRestoreFileContextLimit(context.Background(), file.Path, resume.Files[0].Path, int64(file.ExpectedLength)+1)
			case "extra-input":
				err = os.WriteFile(filepath.Join(attempt, "input", "unknown"), []byte("preserve"), 0o600)
			}
			if err != nil {
				t.Fatal(err)
			}
			if kind != "attempt-alias" {
				if _, err := InspectManagedGraphInstallTarget(context.Background(), path, 1024); err == nil {
					t.Fatal("invalid retained source was exposed")
				}
			}
			if m, err := OpenManagedGraphFromVerifiedPair(context.Background(), path, 1, resume); err == nil {
				m.Close()
				t.Fatal("invalid retained source authorized recovery")
			}
			after, err := os.ReadFile(localRestoreJournalPath(path))
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("rejected recovery changed journal", err)
			}
			if _, err := os.Lstat(attempt); err != nil {
				t.Fatal("rejected recovery removed attempt", err)
			}
		})
	}
}

func TestManagedGraphInstallResealedJournalCannotAuthorizeNativeTamper(t *testing.T) {
	input := managedGraphInstallFixture(t)
	path := filepath.Join(t.TempDir(), "sqlite.db")
	inputPath := filepath.Join(t.TempDir(), "input.json")
	data, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(inputPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestManagedGraphInstallCrashHelper$")
	cmd.Env = append(os.Environ(), "RHIZA_TEST_INSTALL_INPUT="+inputPath, "RHIZA_TEST_INSTALL_TARGET="+path, "RHIZA_TEST_INSTALL_CRASH_PHASE=managed:committed")
	output, runErr := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(runErr, &exit) || exit.ExitCode() != 73 {
		t.Fatalf("hard exit: %v %s", runErr, output)
	}
	t.Logf("owned child pid=%d reaped exit=%d", cmd.Process.Pid, exit.ExitCode())
	j, err := readManagedGraphInstallJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	db, err := latticedb.Open(filepath.Join(filepath.Dir(path), "latticedb", "graph.ltdb"), latticedb.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *latticedb.Tx) error {
		_, err := tx.QueryContext(context.Background(), `CREATE (:Item {id:'forged-without-certified-decision'})`, nil, latticedb.QueryOptions{})
		return err
	}); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// Recomputing a journal checksum must not turn an uncertified native effect
	// into producer provenance. The authentic cut and source files are unchanged.
	j.ManagedGraph.NewGraph, _, err = inspectLocalOriginalGraph(filepath.Join(filepath.Dir(path), "latticedb"))
	if err != nil {
		t.Fatal(err)
	}
	if err := writeLocalRestoreJournal(path, j); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(localRestoreJournalPath(path))
	if err != nil {
		t.Fatal(err)
	}
	if m, err := OpenManagedGraphFromVerifiedPair(context.Background(), path, 1, input); err == nil || !strings.Contains(err.Error(), "not reproducible") {
		if m != nil {
			m.Close()
		}
		t.Fatalf("resealed tamper was not rejected by authenticated-pair reconstruction: %v", err)
	}
	after, err := os.ReadFile(localRestoreJournalPath(path))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("unproven journal/evidence changed")
	}
	native, _, err := inspectLocalOriginalGraph(filepath.Join(filepath.Dir(path), "latticedb"))
	if err != nil || !reflect.DeepEqual(native, j.ManagedGraph.NewGraph) {
		t.Fatal("unknown native bytes were deleted/changed")
	}
}

func TestManagedGraphTargetInspectionRejectsMalformedOriginalMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sqlite.db")
	m, err := OpenManagedSQLKVGraphWithOriginals(path, 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.graph.db.Update(func(tx *latticedb.Tx) error { return tx.PutAppMetadata(graphOriginalBytesKey, []byte{1}) }); err != nil {
		m.Close()
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	before, _, err := inspectLocalOriginalGraph(filepath.Join(filepath.Dir(path), "latticedb"))
	if err != nil {
		t.Fatal(err)
	}
	if state, err := InspectManagedGraphInstallTarget(context.Background(), path, 1024); err == nil {
		t.Fatalf("malformed accounting treated as clean observation: %+v", state)
	}
	after, _, err := inspectLocalOriginalGraph(filepath.Join(filepath.Dir(path), "latticedb"))
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("malformed native metadata was repaired/changed")
	}
}

func TestManagedGraphInstallJournalRejectsOneStoreOriginal(t *testing.T) {
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte("shape-only")))
	files := []localRestoreFile{{Role: CheckpointSQLite, Length: 1, SHA256: hash}, {Role: CheckpointGraphData, Length: 1, SHA256: hash}}
	sql := []localRestoreOriginal{{Name: "sqlite.db", Length: 1, SHA256: hash}}
	graph := []localRestoreOriginal{{Name: "graph.ltdb", Length: 1, SHA256: hash}}
	if validManagedGraphInstallInventory("sqlite.db", sql, nil, false, files) || validManagedGraphInstallInventory("sqlite.db", nil, graph, true, files) {
		t.Fatal("one-store-only original journal accepted")
	}
	if !validManagedGraphInstallInventory("sqlite.db", nil, nil, false, files) || !validManagedGraphInstallInventory("sqlite.db", sql, graph, true, files) {
		t.Fatal("valid paired/absent inventory rejected")
	}
}
