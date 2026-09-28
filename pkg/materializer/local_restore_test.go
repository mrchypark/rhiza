package materializer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
)

func makeLocalRestoreFixture(t *testing.T) (string, LocalRestoreAuthority, []CheckpointFile, func()) {
	t.Helper()
	root := t.TempDir()
	sourceDir := filepath.Join(root, "source")
	if err := os.Mkdir(sourceDir, 0o700); err != nil {
		t.Fatal(err)
	}
	source, err := Open(filepath.Join(sourceDir, "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	var nonce [types.ReadBarrierNonceSize]byte
	nonce[0] = 1
	if err := source.Apply(context.Background(), 1, types.EncodeReadBarrier(nonce)); err != nil {
		t.Fatal(err)
	}
	files, index, cleanup, err := source.CheckpointFilesAt(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for i := range files {
		files[i].ExpectedLength, files[i].ExpectedSHA256, err = hashLocalRestoreFile(files[i].Path)
		if err != nil {
			t.Fatal(err)
		}
	}
	if index != 1 {
		t.Fatalf("checkpoint index=%d, want 1", index)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	storeUUID := stringHash("store")[:32]
	configDigest := stringHash("config")
	rootHash := stringHash("root")
	prefixHash := stringHash("prefix")
	stateHash := stringHash("state")
	authority := LocalRestoreAuthority{StoreUUID: storeUUID, ConfigDigest: configDigest, Index: 1, ConfigID: 1, RootHash: rootHash, PrefixHash: prefixHash, StateHash: stateHash}
	targetDir := filepath.Join(root, "target")
	if err := os.Mkdir(targetDir, 0o700); err != nil {
		t.Fatal(err)
	}
	target, err := Open(filepath.Join(targetDir, "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	nonce[0] = 2
	if err := target.Apply(context.Background(), 1, types.EncodeReadBarrier(nonce)); err != nil {
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(targetDir, "sqlite.db"), authority, files, cleanup
}

func stringHash(value string) string {
	h := sha256.Sum256([]byte(value))
	return hex.EncodeToString(h[:])
}

func TestOpenLocalFromBasePreservesJournalUntilFinalize(t *testing.T) {
	dbPath, authority, files, cleanup := makeLocalRestoreFixture(t)
	defer cleanup()
	before := localRestoreLiveDigest(t, dbPath)
	if err := PrepareLocalRestore(dbPath, authority, files); err != nil {
		t.Fatal(err)
	}
	wrong := authority
	wrong.Index++
	if _, err := OpenLocalFromBase(dbPath, 1, wrong, files, nil); err == nil {
		t.Fatal("wrong base authority resumed Local restore")
	}
	if after := localRestoreLiveDigest(t, dbPath); after != before {
		t.Fatal("wrong base authority changed live materializer files")
	}
	if _, err := Open(dbPath, 1); err == nil {
		t.Fatal("generic Open accepted Local restore discriminator")
	}
	m, err := OpenLocalFromBase(dbPath, 1, authority, files, nil)
	if err != nil {
		t.Fatal(err)
	}
	if m.Tip() != authority.Index {
		t.Fatalf("restored tip=%d, want %d", m.Tip(), authority.Index)
	}
	journal := localRestoreJournalPath(dbPath)
	if _, err := os.Stat(journal); err != nil {
		t.Fatalf("restore journal removed before suffix finalization: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if err := FinalizeLocalRestore(dbPath, authority); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(journal); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("journal remains after finalization: %v", err)
	}
	if m, err := Open(dbPath, 1); err != nil {
		t.Fatalf("generic Open after finalized restore: %v", err)
	} else {
		_ = m.Close()
	}
}

func TestLocalRestoreIntentResumesAfterPublicationBoundary(t *testing.T) {
	dbPath, authority, files, cleanup := makeLocalRestoreFixture(t)
	defer cleanup()
	t.Cleanup(func() { localRestoreFailpoint = nil })
	if err := PrepareLocalRestore(dbPath, authority, files); err != nil {
		t.Fatal(err)
	}
	once := true
	localRestoreFailpoint = func(name string) error {
		if name == "after-published" && once {
			once = false
			return errors.New("simulated process interruption")
		}
		return nil
	}
	_, firstErr := OpenLocalFromBase(dbPath, 1, authority, files, nil)
	localRestoreFailpoint = nil
	if firstErr == nil {
		t.Fatal("restore interruption seam did not fire")
	}
	m, err := OpenLocalFromBase(dbPath, 1, authority, files, nil)
	if err != nil {
		t.Fatalf("resume after publication interruption: %v", err)
	}
	if m.Tip() != authority.Index {
		t.Fatalf("resumed tip=%d, want %d", m.Tip(), authority.Index)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if err := FinalizeLocalRestore(dbPath, authority); err != nil {
		t.Fatal(err)
	}
}

func TestPrepareLocalRestoreRejectsSourceNotMatchingDescriptorBeforeIntent(t *testing.T) {
	dbPath, authority, files, cleanup := makeLocalRestoreFixture(t)
	defer cleanup()
	before := localRestoreLiveDigest(t, dbPath)
	files[0].ExpectedLength++
	err := PrepareLocalRestore(dbPath, authority, files)
	if err == nil || !strings.Contains(err.Error(), "differs from authorized descriptor") {
		t.Fatalf("PrepareLocalRestore error=%v, want descriptor mismatch", err)
	}
	if _, err := os.Lstat(localRestoreJournalPath(dbPath)); !os.IsNotExist(err) {
		t.Fatalf("restore intent exists after descriptor mismatch: %v", err)
	}
	if after := localRestoreLiveDigest(t, dbPath); after != before {
		t.Fatal("descriptor mismatch mutated live materializer")
	}
}

func localRestoreLiveDigest(t *testing.T, dbPath string) string {
	t.Helper()
	h := sha256.New()
	for _, path := range []string{dbPath, filepath.Join(filepath.Dir(dbPath), "latticedb")} {
		if info, err := os.Lstat(path); err != nil {
			t.Fatal(err)
		} else if info.IsDir() {
			if err := filepath.WalkDir(path, func(file string, entry os.DirEntry, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				if entry.IsDir() {
					return nil
				}
				relative, err := filepath.Rel(filepath.Dir(dbPath), file)
				if err != nil {
					return err
				}
				_, _ = io.WriteString(h, relative+"\x00")
				data, err := os.ReadFile(file)
				if err != nil {
					return err
				}
				_, _ = h.Write(data)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		} else {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = h.Write(data)
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

func TestFloorZeroCannotAuthorizeLocalRestore(t *testing.T) {
	dbPath, authority, files, cleanup := makeLocalRestoreFixture(t)
	defer cleanup()
	if err := PrepareLocalRestore(dbPath, authority, files); err != nil {
		t.Fatal(err)
	}
	if err := ValidateNoLocalRestore(dbPath); err == nil {
		t.Fatal("floor-zero Local state accepted restore journal")
	}
}

func TestLocalRestoreResumesAfterProcessInterruption(t *testing.T) {
	for _, phase := range []string{"after-intent", "after-staged", "after-backup-sql", "after-backup-graph", "after-publish-graph", "after-publish-sql", "after-validation", "after-installed", "after-cleanup"} {
		t.Run(phase, func(t *testing.T) {
			dbPath, authority, files, cleanup := makeLocalRestoreFixture(t)
			defer cleanup()
			payload, err := json.Marshal(struct {
				DBPath    string
				Authority LocalRestoreAuthority
				Files     []CheckpointFile
				Phase     string
			}{dbPath, authority, files, phase})
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestLocalRestoreCrashHelper$")
			cmd.Env = append(os.Environ(), "RHIZA_LOCAL_RESTORE_CRASH="+string(payload))
			err = cmd.Run()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != 77 {
				t.Fatalf("child exit=%v, want simulated process kill", err)
			}
			if err := PrepareLocalRestore(dbPath, authority, files); err != nil {
				t.Fatalf("reauthorize exact base after crash: %v", err)
			}
			m, err := OpenLocalFromBase(dbPath, 1, authority, files, nil)
			if err != nil {
				t.Fatalf("resume after process interruption: %v", err)
			}
			if m.Tip() != authority.Index {
				t.Fatalf("resumed materializer tip=%d, want %d", m.Tip(), authority.Index)
			}
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
			if err := FinalizeLocalRestore(dbPath, authority); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestValidateLocalGraphCheckpointFileDoesNotMutateSourceRoot(t *testing.T) {
	_, _, files, cleanup := makeLocalRestoreFixture(t)
	defer cleanup()
	var graphPath string
	for _, file := range files {
		if file.Role == CheckpointGraphData {
			graphPath = file.Path
		}
	}
	if graphPath == "" {
		t.Fatal("fixture has no graph checkpoint")
	}
	root := filepath.Dir(graphPath)
	before, _, err := inspectLocalOriginalGraph(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateLocalGraphCheckpointFile(graphPath); err != nil {
		t.Fatal(err)
	}
	after, _, err := inspectLocalOriginalGraph(root)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("read-only checkpoint validation mutated source root: before=%v after=%v", before, after)
	}
}

func TestLocalRestoreCrashHelper(t *testing.T) {
	data := os.Getenv("RHIZA_LOCAL_RESTORE_CRASH")
	if data == "" {
		return
	}
	var request struct {
		DBPath    string
		Authority LocalRestoreAuthority
		Files     []CheckpointFile
		Phase     string
	}
	if err := json.Unmarshal([]byte(data), &request); err != nil {
		t.Fatal(err)
	}
	localRestoreFailpoint = func(name string) error {
		if name == request.Phase {
			os.Exit(77)
		}
		return nil
	}
	if err := PrepareLocalRestore(request.DBPath, request.Authority, request.Files); err != nil {
		t.Fatal(err)
	}
	m, err := OpenLocalFromBase(request.DBPath, 1, request.Authority, request.Files, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if request.Phase == "after-cleanup" {
		if err := FinalizeLocalRestore(request.DBPath, request.Authority); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatalf("requested process interruption boundary %q was not reached", request.Phase)
}
