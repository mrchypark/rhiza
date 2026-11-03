package materializer

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"

	latticedb "github.com/mrchypark/latticedb-go"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
)

// ManagedGraphInstallCut is authenticated by Node, not by the installer. Node
// holds the WAL lock and rechecks this cut after Core/archive import and directly
// before installation. Hashes refer to certified provenance, not live content.
type ManagedGraphInstallCut struct {
	BindingHash          [32]byte
	WALBindingHash       [32]byte
	ConfigID             uint
	Generation           uint64
	CheckpointSlot       uint64
	CheckpointRoot       [32]byte
	CheckpointStateHash  [32]byte
	CheckpointPrefixHash [32]byte
	CertifiedTip         uint64
	CertifiedPrefixHash  [32]byte
	ExpectedGraphHistory GraphHistory
	IdempotencyWindow    uint64
}

type ManagedGraphInstall struct {
	Cut   ManagedGraphInstallCut
	Files []CheckpointFile
}

// The optional journal branch leaves existing Local journal bytes unchanged.
// Local's reader rejects this branch because LocalVersion is zero; it is never
// an invented LocalRestoreAuthority and cannot authorize Local recovery.
type managedGraphInstallJournal struct {
	Version   int                    `json:"version"`
	Protocol  string                 `json:"protocol"`
	Cut       ManagedGraphInstallCut `json:"cut"`
	Digest    [32]byte               `json:"digest"`
	NewSQL    []localRestoreOriginal `json:"new_sql,omitempty"`
	NewGraph  []localRestoreOriginal `json:"new_graph,omitempty"`
	Integrity [32]byte               `json:"integrity"`
}

// OpenManagedGraphFromVerifiedPair replaces only an exclusively owned, clean
// target using an authenticated closed snapshot pair. It never opens the old
// target writable. Missing/ambiguous provenance is not a genesis signal.
func OpenManagedGraphFromVerifiedPair(ctx context.Context, dbPath string, readers int, input ManagedGraphInstall) (*Materializer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateManagedGraphInstall(input.Cut, readers); err != nil {
		return nil, err
	}
	path, err := filepath.Abs(dbPath)
	if err != nil {
		return nil, err
	}
	if err := ValidateCleanRestoreState(path); err != nil {
		return nil, err
	}
	if _, err := os.Lstat(localRestoreJournalPath(path)); err == nil {
		opened, committed, recoverErr := recoverManagedGraphInstall(ctx, path, readers, input)
		if recoverErr != nil {
			return nil, recoverErr
		}
		if committed {
			return opened, nil
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	if err := ValidateNoLocalRestore(path); err != nil {
		return nil, err
	}
	if err := validateManagedGraphInstallTarget(ctx, path, input.Cut.IdempotencyWindow); err != nil {
		return nil, err
	}
	files, descriptors, err := managedGraphInstallSources(path, input.Files)
	if err != nil {
		return nil, err
	}
	// Read-only preflight is closed before the original main/sidecar/native
	// inventory is frozen. In particular, a live SQLite WAL is never discarded.
	originalSQL, err := inspectLocalOriginalSQL(path)
	if err != nil {
		return nil, err
	}
	sort.Slice(originalSQL, func(i, j int) bool { return originalSQL[i].Name < originalSQL[j].Name })
	originalGraph, hadGraph, err := inspectLocalOriginalGraph(filepath.Join(filepath.Dir(path), "latticedb"))
	if err != nil {
		return nil, err
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(struct {
		Protocol string
		Cut      ManagedGraphInstallCut
		Files    []localRestoreFile
	}{sqlpolicy.ManagedGraphRequestApplicationProtocol, input.Cut, descriptors})
	if err != nil {
		return nil, err
	}
	j := localRestoreJournal{Version: 1, AttemptID: hex.EncodeToString(random[:]), Phase: "intent", Files: descriptors,
		OriginalSQL: originalSQL, OriginalGraph: originalGraph, HadGraph: hadGraph,
		ManagedGraph: &managedGraphInstallJournal{Version: 1, Protocol: sqlpolicy.ManagedGraphRequestApplicationProtocol, Cut: input.Cut, Digest: sha256.Sum256(encoded)}}
	if err := hitLocalRestoreBoundary("managed:before-intent"); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := writeLocalRestoreJournal(path, j); err != nil {
		return nil, err
	}
	attempt := localRestoreAttemptPath(path, j.AttemptID)
	if err := ensureLocalRestoreDir(attempt); err != nil {
		return nil, err
	}
	inputDir := filepath.Join(attempt, "input")
	if err := ensureLocalRestoreDir(inputDir); err != nil {
		return nil, err
	}
	for i, file := range files {
		target := managedGraphInputPath(attempt, file.Role)
		if err := copyLocalRestoreFileContextLimit(ctx, file.Path, target, int64(file.ExpectedLength)+1); err != nil {
			return nil, err // Preserve incomplete input; it is not a recoverable stage.
		}
		if !matchesLocalInventory(target, localRestoreOriginal{Length: file.ExpectedLength, SHA256: file.ExpectedSHA256}) {
			return nil, fmt.Errorf("retained Graph install input differs")
		}
		files[i].Path = target
	}
	for _, dir := range []string{inputDir, attempt, filepath.Dir(path)} {
		if err := localRestoreSyncDir(dir); err != nil {
			return nil, err
		}
	}
	return runManagedGraphInstall(ctx, path, readers, input, j, files)
}

func managedGraphInputPath(attempt string, role CheckpointRole) string {
	name := "sqlite.db"
	if role == CheckpointGraphData {
		name = "graph.ltdb"
	}
	return filepath.Join(attempt, "input", name)
}

func runManagedGraphInstall(ctx context.Context, path string, readers int, input ManagedGraphInstall, j localRestoreJournal, files []CheckpointFile) (*Materializer, error) {
	attempt := localRestoreAttemptPath(path, j.AttemptID)
	stage := filepath.Join(attempt, "stage")
	for _, dir := range []string{attempt, stage, filepath.Join(stage, "latticedb"), filepath.Join(attempt, "backup")} {
		if err := ensureLocalRestoreDir(dir); err != nil {
			return nil, fmt.Errorf("preserve Graph install intent: %w", err)
		}
	}
	var newSQL, newGraph []localRestoreOriginal
	rollback := func(cause error) (*Materializer, error) {
		return nil, errors.Join(cause, rollbackManagedGraphInstall(path, attempt, j, newSQL, newGraph))
	}
	for i, file := range files {
		target := filepath.Join(stage, "sqlite.db")
		if file.Role == CheckpointGraphData {
			target = filepath.Join(stage, "latticedb", "graph.ltdb")
		}
		if err := ctx.Err(); err != nil {
			return rollback(err)
		}
		if err := copyLocalRestoreFileContextLimit(ctx, file.Path, target, int64(file.ExpectedLength)+1); err != nil {
			return rollback(err)
		}
		length, hash, err := hashLocalRestoreFile(target)
		if err != nil || length != j.Files[i].Length || hash != j.Files[i].SHA256 {
			return rollback(fmt.Errorf("copied Graph pair differs from authorized descriptor"))
		}
	}
	// Only the owned stage is opened writable. The original target is untouched.
	check, err := OpenManagedSQLKVGraphWithOriginals(filepath.Join(stage, "sqlite.db"), readers, input.Cut.IdempotencyWindow)
	if err != nil {
		return rollback(err)
	}
	err = validateManagedGraphInstalled(ctx, check, input.Cut)
	err = errors.Join(err, check.Close())
	if err != nil {
		return rollback(err)
	}
	newSQL, err = inspectLocalOriginalSQL(filepath.Join(stage, "sqlite.db"))
	if err != nil {
		return rollback(err)
	}
	for i := range newSQL {
		newSQL[i].Name = filepath.Base(path) + newSQL[i].Name[len("sqlite.db"):]
	}
	newGraph, _, err = inspectLocalOriginalGraph(filepath.Join(stage, "latticedb"))
	if err != nil {
		return rollback(err)
	}
	var expanded uint64
	for _, entries := range [][]localRestoreOriginal{newSQL, newGraph} {
		for _, item := range entries {
			if item.Length > (32<<30)-expanded {
				return rollback(fmt.Errorf("reconstructed Graph pair exceeds physical envelope"))
			}
			expanded += item.Length
		}
	}
	// The source is a closed snapshot, not a live SQLite database. If the
	// private validation open left committed WAL data, moving main alone would
	// drop it. Preserve the attempt instead of assuming close checkpointed it.
	for _, item := range newSQL {
		if item.Name == filepath.Base(path)+"-wal" && item.Length != 0 {
			return rollback(fmt.Errorf("reconstructed SQLite snapshot has outstanding WAL"))
		}
	}
	for _, dir := range []string{filepath.Join(stage, "latticedb"), stage, attempt} {
		if err := localRestoreSyncDir(dir); err != nil {
			return rollback(err)
		}
	}
	j.Phase = "staged"
	j.ManagedGraph.NewSQL = append([]localRestoreOriginal(nil), newSQL...)
	j.ManagedGraph.NewGraph = append([]localRestoreOriginal(nil), newGraph...)
	if err := writeLocalRestoreJournal(path, j); err != nil {
		return rollback(err)
	}
	if err := hitLocalRestoreBoundary("managed:after-staged"); err != nil {
		return rollback(err)
	}
	if err := ctx.Err(); err != nil {
		return rollback(err)
	}
	// Verify the entire current inventory, including absence, before moving any
	// original. backupLocalOriginals also rechecks each file immediately at move.
	if err := verifyManagedGraphOriginals(path, j); err != nil {
		return nil, fmt.Errorf("preserve changed original and Graph install intent: %w", err)
	}
	if err := backupLocalOriginals(path, attempt, &j); err != nil {
		return rollback(err)
	}
	if err := publishLocalRestore(path, stage, &j); err != nil {
		return rollback(err)
	}
	j.Phase = "opening"
	if err := writeLocalRestoreJournal(path, j); err != nil {
		return rollback(err)
	}
	if err := hitLocalRestoreBoundary("managed:before-open"); err != nil {
		return rollback(err)
	}
	opened, err := openMaterializerPolicy(path, readers, sqlpolicy.ManagedGraphRequestApplicationProtocol, input.Cut.IdempotencyWindow)
	if err == nil {
		err = validateManagedGraphInstalled(ctx, opened, input.Cut)
	}
	if err != nil {
		if opened != nil {
			err = errors.Join(err, opened.Close())
		}
		// Any files produced by this private open belong to this attempt. Close
		// all handles before inventory/rollback; never reopen the previous writer.
		newSQL, _ = inspectLocalOriginalSQL(path)
		newGraph, _, _ = inspectLocalOriginalGraph(filepath.Join(filepath.Dir(path), "latticedb"))
		return rollback(err)
	}
	if err := hitLocalRestoreBoundary("managed:after-open-before-inventory"); err != nil {
		return nil, errors.Join(err, opened.Close())
	}
	opened.stopSQLiteCheckpoints()
	opened.walCheckpointIO.Wait()
	newSQL, err = inspectLocalOriginalSQL(path)
	if err != nil {
		return nil, errors.Join(err, opened.Close())
	}
	newGraph, _, err = inspectLocalOriginalGraph(filepath.Join(filepath.Dir(path), "latticedb"))
	if err != nil {
		return nil, errors.Join(err, opened.Close())
	}
	j.ManagedGraph.NewSQL, j.ManagedGraph.NewGraph = newSQL, newGraph
	j.Phase = "opened"
	if err := writeLocalRestoreJournal(path, j); err != nil {
		return nil, errors.Join(err, opened.Close())
	}
	if err := hitLocalRestoreBoundary("managed:after-open"); err != nil {
		err = errors.Join(err, opened.Close())
		newSQL, _ = inspectLocalOriginalSQL(path)
		newGraph, _, _ = inspectLocalOriginalGraph(filepath.Join(filepath.Dir(path), "latticedb"))
		return rollback(err)
	}
	if err := opened.Close(); err != nil {
		return rollback(err)
	}
	newSQL, err = inspectLocalOriginalSQL(path)
	if err != nil {
		return nil, err
	}
	newGraph, _, err = inspectLocalOriginalGraph(filepath.Join(filepath.Dir(path), "latticedb"))
	if err != nil {
		return nil, err
	}
	j.ManagedGraph.NewSQL = newSQL
	j.ManagedGraph.NewGraph = newGraph
	j.Phase = "committed"
	if err := writeLocalRestoreJournal(path, j); err != nil {
		return rollback(err)
	}
	if err := hitLocalRestoreBoundary("managed:committed"); err != nil {
		return rollback(err)
	}
	return finishManagedGraphInstall(ctx, path, readers, input, attempt)
}

func finishManagedGraphInstall(ctx context.Context, path string, readers int, input ManagedGraphInstall, attempt string) (*Materializer, error) {
	// Keep the exact input pair through the final open. A hard exit at any
	// recorded phase must not depend on Node's temporary reconstruction files.
	for _, name := range []string{"backup", "stage"} {
		if err := os.RemoveAll(filepath.Join(attempt, name)); err != nil {
			return nil, err
		}
	}
	if err := localRestoreSyncDir(filepath.Dir(path)); err != nil {
		return nil, err
	}
	if err := hitLocalRestoreBoundary("managed:after-attempt-cleanup"); err != nil {
		return nil, err
	}
	j, err := readManagedGraphInstallJournal(path)
	if err != nil {
		return nil, err
	}
	opened, err := openMaterializerPolicy(path, readers, sqlpolicy.ManagedGraphRequestApplicationProtocol, input.Cut.IdempotencyWindow)
	if err == nil {
		err = validateManagedGraphInstalled(ctx, opened, input.Cut)
	}
	if err != nil {
		if opened != nil {
			err = errors.Join(err, opened.Close())
		}
		return nil, err
	}
	opened.stopSQLiteCheckpoints()
	opened.walCheckpointIO.Wait()
	j.ManagedGraph.NewSQL, err = inspectLocalOriginalSQL(path)
	if err != nil {
		return nil, errors.Join(err, opened.Close())
	}
	j.ManagedGraph.NewGraph, _, err = inspectLocalOriginalGraph(filepath.Join(filepath.Dir(path), "latticedb"))
	if err != nil {
		return nil, errors.Join(err, opened.Close())
	}
	j.Phase = "final-opened"
	if err := writeLocalRestoreJournal(path, j); err != nil {
		return nil, errors.Join(err, opened.Close())
	}
	if err := hitLocalRestoreBoundary("managed:final-opened"); err != nil {
		return nil, errors.Join(err, opened.Close())
	}
	if err := os.RemoveAll(attempt); err != nil {
		return nil, errors.Join(err, opened.Close())
	}
	if err := localRestoreSyncDir(filepath.Dir(path)); err != nil {
		return nil, errors.Join(err, opened.Close())
	}
	if err := os.Remove(localRestoreJournalPath(path)); err != nil {
		return nil, errors.Join(err, opened.Close())
	}
	if err := localRestoreSyncDir(filepath.Dir(path)); err != nil {
		return nil, errors.Join(err, opened.Close())
	}
	// The private inventory interval drained this worker before observation.
	// Restore normal lifecycle before publishing the newly opened handle.
	opened.walCheckpointStop = make(chan struct{})
	opened.walCheckpointOnce = sync.Once{}
	opened.walCheckpointWG.Add(1)
	go opened.runSQLiteCheckpoints()
	return opened, nil
}

func validateManagedGraphInstall(cut ManagedGraphInstallCut, readers int) error {
	zero := [32]byte{}
	if readers < 1 || cut.ConfigID == 0 || cut.BindingHash == zero || cut.WALBindingHash == zero ||
		cut.CheckpointSlot == 0 || cut.CheckpointRoot == zero || cut.CheckpointStateHash == zero || cut.CheckpointPrefixHash == zero ||
		cut.CertifiedTip < cut.CheckpointSlot || cut.CertifiedTip > math.MaxInt64 || cut.CertifiedPrefixHash == zero || cut.ExpectedGraphHistory.LastGraphSlot > cut.CertifiedTip ||
		cut.IdempotencyWindow < 1024 || cut.IdempotencyWindow > 1_048_576 {
		return fmt.Errorf("invalid authenticated Graph install cut")
	}
	if _, err := decodeGraphHistory(encodeGraphHistory(cut.ExpectedGraphHistory)); err != nil {
		return err
	}
	return nil
}

func validateManagedGraphInstalled(ctx context.Context, m *Materializer, cut ManagedGraphInstallCut) error {
	if m.Tip() != cut.CertifiedTip || m.idempotencyWindow != cut.IdempotencyWindow || !graphProtocol(m.managedProtocol) {
		return fmt.Errorf("installed Graph cut does not match certified tip/profile/window")
	}
	if err := m.ProjectionReadable(ctx); err != nil {
		return err
	}
	history, err := m.ManagedGraphHistory(ctx)
	if err != nil || history != cut.ExpectedGraphHistory {
		return fmt.Errorf("installed Graph history differs from certified reconstruction")
	}
	return nil
}

func managedGraphInstallSources(dbPath string, input []CheckpointFile) ([]CheckpointFile, []localRestoreFile, error) {
	if len(input) != 2 {
		return nil, nil, fmt.Errorf("Graph install requires two snapshot roles")
	}
	files := make([]CheckpointFile, 2)
	descriptors := make([]localRestoreFile, 2)
	var total uint64
	for _, file := range input {
		i := 0
		if file.Role == CheckpointGraphData {
			i = 1
		} else if file.Role != CheckpointSQLite {
			return nil, nil, fmt.Errorf("unsupported Graph install role")
		}
		if files[i].Path != "" || file.ExpectedLength == 0 || file.ExpectedLength > (32<<30)-total || len(file.ExpectedSHA256) != 64 || !isLowerHex(file.ExpectedSHA256) {
			return nil, nil, fmt.Errorf("invalid Graph install descriptor")
		}
		total += file.ExpectedLength
		absolute, err := filepath.Abs(file.Path)
		if err != nil {
			return nil, nil, err
		}
		graph := filepath.Join(filepath.Dir(dbPath), "latticedb")
		realSource, sourceErr := filepath.EvalSymlinks(absolute)
		realParent, parentErr := filepath.EvalSymlinks(filepath.Dir(dbPath))
		if sourceErr != nil || parentErr != nil {
			return nil, nil, fmt.Errorf("cannot resolve Graph install source/target")
		}
		realGraph := filepath.Join(realParent, "latticedb")
		realDB := filepath.Join(realParent, filepath.Base(dbPath))
		realRel, relErr := filepath.Rel(realGraph, realSource)
		if relErr != nil || realSource == realDB || realSource == realDB+"-wal" || realSource == realDB+"-shm" || realSource == realDB+"-journal" || (!filepath.IsAbs(realRel) && realRel != ".." && !strings.HasPrefix(realRel, ".."+string(filepath.Separator))) {
			return nil, nil, fmt.Errorf("Graph install source resolves into target")
		}
		rel, err := filepath.Rel(graph, absolute)
		if err != nil || absolute == dbPath || absolute == dbPath+"-wal" || absolute == dbPath+"-shm" || absolute == dbPath+"-journal" || (!filepath.IsAbs(rel) && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))) {
			return nil, nil, fmt.Errorf("Graph install source aliases target")
		}
		info, err := os.Lstat(absolute)
		if err != nil || !info.Mode().IsRegular() || uint64(info.Size()) != file.ExpectedLength {
			return nil, nil, fmt.Errorf("invalid Graph install source")
		}
		// Detect hard-link aliases as well as identical pathnames.
		targets := []string{dbPath, dbPath + "-wal", dbPath + "-shm", dbPath + "-journal"}
		if err := filepath.WalkDir(graph, func(path string, entry os.DirEntry, walkErr error) error {
			if os.IsNotExist(walkErr) && path == graph {
				return nil
			}
			if walkErr != nil {
				return walkErr
			}
			if entry.Type()&os.ModeSymlink != 0 {
				return fmt.Errorf("symlink in target native inventory")
			}
			if !entry.IsDir() {
				targets = append(targets, path)
			}
			return nil
		}); err != nil {
			return nil, nil, err
		}
		for _, target := range targets {
			if targetInfo, err := os.Stat(target); err == nil && os.SameFile(info, targetInfo) {
				return nil, nil, fmt.Errorf("Graph install source hard-links target")
			}
		}
		file.Path = absolute
		files[i] = file
		descriptors[i] = localRestoreFile{Role: file.Role, Length: file.ExpectedLength, SHA256: file.ExpectedSHA256}
	}
	if files[0].Path == "" || files[1].Path == "" {
		return nil, nil, fmt.Errorf("missing Graph install snapshot role")
	}
	if first, err := os.Stat(files[0].Path); err != nil {
		return nil, nil, err
	} else if second, err := os.Stat(files[1].Path); err != nil || os.SameFile(first, second) {
		return nil, nil, fmt.Errorf("Graph install roles alias")
	}
	return files, descriptors, nil
}

func verifyManagedGraphOriginals(path string, j localRestoreJournal) error {
	got, err := inspectLocalOriginalSQL(path)
	if err != nil {
		return err
	}
	sort.Slice(got, func(i, k int) bool { return got[i].Name < got[k].Name })
	if !reflect.DeepEqual(got, j.OriginalSQL) {
		return fmt.Errorf("original SQLite inventory changed")
	}
	graph, exists, err := inspectLocalOriginalGraph(filepath.Join(filepath.Dir(path), "latticedb"))
	if err != nil || exists != j.HadGraph || !reflect.DeepEqual(graph, j.OriginalGraph) {
		return fmt.Errorf("original native inventory changed")
	}
	return nil
}

// Validate only recovery markers in the old pair. It is never the provenance
// source and is not opened writable or silently healed by this installer.
func validateManagedGraphInstallTarget(ctx context.Context, path string, window uint64) error {
	state, err := InspectManagedGraphInstallTarget(ctx, path, window)
	if err != nil {
		return err
	}
	if state.InstallAttempt != nil {
		return fmt.Errorf("existing managed install needs exact authenticated attempt recovery")
	}
	if len(state.Replay) != 0 {
		return fmt.Errorf("%w: existing native state needs authenticated same-slot replay", ErrGraphApplyPending)
	}
	return nil
}

// ManagedGraphReplayRecord is observed native metadata, not certification.
type ManagedGraphReplayRecord struct {
	Slot            uint64
	DecisionHash    [32]byte
	NativeCompleted bool
}

type ManagedGraphTargetState struct {
	Absent        bool
	SQLiteTip     uint64
	CommonTip     uint64
	NativeTip     uint64
	SQLiteHistory GraphHistory
	NativeHistory GraphHistory
	Replay        []ManagedGraphReplayRecord
	InventoryHash [32]byte
	// Non-nil means target tips are not inspected: a journal may explain a
	// partially moved pair. The observed cut still needs actual Node authority.
	InstallAttempt *ManagedGraphInstallAttemptObservation
}

type ManagedGraphInstallAttemptObservation struct {
	AttemptID string
	Phase     string
	Cut       ManagedGraphInstallCut
	// Files are observed immutable input bytes, never authentication of Cut.
	Files []CheckpointFile
}

// InspectManagedGraphInstallTarget never opens a writer or heals native state.
// The caller excludes replacement and authenticates every Replay record against
// its actual Core before allowing same-slot reconciliation. Absent is not a
// genesis/admission signal; InventoryHash is observation, not authority.
func InspectManagedGraphInstallTarget(ctx context.Context, dbPath string, window uint64) (state ManagedGraphTargetState, resultErr error) {
	path, err := filepath.Abs(dbPath)
	if err != nil {
		return state, err
	}
	if err := ValidateCleanRestoreState(path); err != nil {
		return state, err
	}
	if _, err := os.Lstat(localRestoreJournalPath(path)); err == nil {
		j, err := readManagedGraphInstallJournal(path)
		if err != nil {
			return state, err
		}
		if j.ManagedGraph.Cut.IdempotencyWindow != window {
			return state, fmt.Errorf("managed install observation window differs")
		}
		files, err := retainedManagedGraphInputs(path, j)
		if err != nil {
			return state, err
		}
		state.InstallAttempt = &ManagedGraphInstallAttemptObservation{AttemptID: j.AttemptID, Phase: j.Phase, Cut: j.ManagedGraph.Cut, Files: files}
		data, err := encodeManagedGraphInstallJournal(j)
		if err != nil {
			return state, err
		}
		state.InventoryHash = sha256.Sum256(data)
		return state, nil
	} else if !os.IsNotExist(err) {
		return state, err
	}
	if err := ValidateNoLocalRestore(path); err != nil {
		return state, err
	}
	if window < 1024 || window > 1_048_576 {
		return state, fmt.Errorf("invalid Graph receipt window")
	}
	if err := inspectManagedGraphInstallTarget(ctx, path, window, &state); err != nil {
		return state, err
	}
	sql, err := inspectLocalOriginalSQL(path)
	if err != nil {
		return state, err
	}
	graph, hadGraph, err := inspectLocalOriginalGraph(filepath.Join(filepath.Dir(path), "latticedb"))
	if err != nil {
		return state, err
	}
	data, err := json.Marshal(struct {
		SQL, Graph []localRestoreOriginal
		HadGraph   bool
	}{sql, graph, hadGraph})
	if err != nil {
		return state, err
	}
	state.InventoryHash = sha256.Sum256(data)
	return state, nil
}

func inspectManagedGraphInstallTarget(ctx context.Context, path string, window uint64, state *ManagedGraphTargetState) error {
	info, statErr := os.Lstat(path)
	graphPath := filepath.Join(filepath.Dir(path), "latticedb", "graph.ltdb")
	_, graphErr := os.Lstat(graphPath)
	if os.IsNotExist(statErr) && os.IsNotExist(graphErr) {
		for _, remnant := range []string{path + "-wal", path + "-shm", path + "-journal", filepath.Dir(graphPath)} {
			if _, err := os.Lstat(remnant); !os.IsNotExist(err) {
				return fmt.Errorf("incomplete previous Graph pair has remnants")
			}
		}
		state.Absent = true
		return nil
	}
	if statErr != nil || graphErr != nil || !info.Mode().IsRegular() {
		return fmt.Errorf("incomplete previous Graph pair")
	}
	inventory, _, err := inspectLocalOriginalGraph(filepath.Dir(graphPath))
	if err != nil {
		return err
	}
	for _, item := range inventory {
		if !knownManagedGraphNativeArtifact(item.Name) {
			return fmt.Errorf("unrecognised previous native artifact %q; preserve it", item.Name)
		}
	}
	_, tip, err := InspectManagedGraphSQLFile(ctx, path, window)
	if err != nil {
		return err
	}
	sqlite, err := openSQLite((&url.URL{Scheme: "file", Path: filepath.ToSlash(path)}).String()+"?mode=ro", false)
	if err != nil {
		return err
	}
	wantHistory, historyErr := sqliteGraphHistory(ctx, sqlite)
	if err := errors.Join(historyErr, sqlite.Close()); err != nil {
		return err
	}
	state.SQLiteTip, state.CommonTip, state.SQLiteHistory = tip, tip, wantHistory
	db, err := latticedb.Open(graphPath, latticedb.OpenOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	viewErr := db.View(func(tx *latticedb.Tx) error {
		format, _, err := tx.GetAppMetadata(graphFormatKey)
		if err != nil || !bytes.Equal(format, graphOriginalStoragePolicy) {
			return fmt.Errorf("previous Graph policy is not recognised")
		}
		pendingData, found, err := tx.GetAppMetadata(graphPendingKey)
		if err != nil || found && len(pendingData) == 0 {
			return fmt.Errorf("previous Graph publication marker is malformed")
		}
		pending, err := decodeGraphPending(pendingData)
		if err != nil {
			return err
		}
		data, _, err := tx.GetAppMetadata(graphTipKey)
		if err != nil {
			return err
		}
		nativeTip, err := decodeGraphTip(data)
		if err != nil || nativeTip > math.MaxInt64 {
			return fmt.Errorf("previous Graph completion marker is invalid")
		}
		data, _, err = tx.GetAppMetadata(graphHistoryKey)
		if err != nil {
			return err
		}
		history, err := decodeGraphHistory(data)
		if err != nil || history.LastGraphSlot != nativeTip {
			return fmt.Errorf("previous native Graph history mismatch")
		}
		state.NativeTip, state.NativeHistory = nativeTip, history
		data, _, err = tx.GetAppMetadata(graphJournalKey)
		if err != nil {
			return err
		}
		journal, err := decodeGraphJournal(data)
		if err != nil {
			return err
		}
		for _, entry := range journal {
			if entry.Slot > nativeTip || entry.Hash == ([32]byte{}) {
				return fmt.Errorf("previous Graph journal exceeds native completion")
			}
		}
		unapplied := pendingGraphJournal(journal, tip)
		if nativeTip > tip && (len(unapplied) == 0 || unapplied[len(unapplied)-1].Slot != nativeTip) {
			return fmt.Errorf("previous Graph journal does not cover native completion")
		}
		folded := wantHistory
		for _, entry := range unapplied {
			folded, err = foldGraphHistory(folded, entry.Slot, entry.Hash)
			if err != nil {
				return err
			}
			state.Replay = append(state.Replay, ManagedGraphReplayRecord{Slot: entry.Slot, DecisionHash: entry.Hash, NativeCompleted: true})
		}
		if folded != history {
			return fmt.Errorf("previous paired Graph history cannot be reconciled by native journal")
		}
		if pending != nil {
			if pending.Slot <= tip || pending.Slot <= nativeTip || pending.Slot > math.MaxInt64 || pending.Hash == ([32]byte{}) {
				return fmt.Errorf("previous pending Graph marker is inconsistent")
			}
			state.Replay = append(state.Replay, ManagedGraphReplayRecord{Slot: pending.Slot, DecisionHash: pending.Hash})
		}
		return nil
	})
	if viewErr == nil {
		// Reuse the native metadata validator without opening a writer. This
		// checks retained originals/accounting, not canonical live Graph contents.
		observed := &graphState{db: db, originals: true, durableTip: state.NativeTip, idempotencyWindow: window}
		viewErr = observed.validateOriginalAccounting()
	}
	return errors.Join(viewErr, db.Close())
}

// Rollback is an inventory operation, never a database open. Unknown targets or
// missing backups stop it before removal, leaving the bound journal for repair.
func rollbackManagedGraphInstall(path, attempt string, j localRestoreJournal, newSQL, newGraph []localRestoreOriginal) error {
	return rollbackManagedGraphInstallKeepingInput(path, attempt, j, newSQL, newGraph, false)
}

func rollbackManagedGraphInstallKeepingInput(path, attempt string, j localRestoreJournal, newSQL, newGraph []localRestoreOriginal, keepInput bool) error {
	dir := filepath.Dir(path)
	backup := filepath.Join(attempt, "backup")
	currentSQL, err := inspectLocalOriginalSQL(path)
	if err != nil {
		return fmt.Errorf("preserve Graph install journal: %w", err)
	}
	for _, item := range currentSQL {
		original := findLocalOriginal(j.OriginalSQL, item.Name)
		staged := findLocalOriginal(newSQL, item.Name)
		if item != original && item != staged {
			return fmt.Errorf("preserve unexpected SQLite target during rollback")
		}
	}
	for _, item := range j.OriginalSQL {
		backupPath := filepath.Join(backup, item.Name)
		if _, err := os.Lstat(backupPath); err == nil {
			if !matchesLocalInventory(backupPath, item) {
				return fmt.Errorf("preserve changed SQLite backup")
			}
		} else if !os.IsNotExist(err) || !matchesLocalInventory(filepath.Join(dir, item.Name), item) {
			return fmt.Errorf("preserve missing SQLite original")
		}
	}
	graphPath := filepath.Join(dir, "latticedb")
	graphBackup := filepath.Join(backup, "latticedb")
	graph, graphExists, err := inspectLocalOriginalGraph(graphPath)
	if err != nil {
		return err
	}
	_, graphBackupErr := os.Lstat(graphBackup)
	if graphBackupErr == nil {
		if !j.HadGraph || verifyLocalDirectoryInventory(graphBackup, j.OriginalGraph) != nil {
			return fmt.Errorf("preserve invalid native backup")
		}
	} else if !os.IsNotExist(graphBackupErr) || (j.HadGraph && (!graphExists || !reflect.DeepEqual(graph, j.OriginalGraph))) {
		return fmt.Errorf("preserve missing native original")
	}
	if graphExists && !reflect.DeepEqual(graph, j.OriginalGraph) && !reflect.DeepEqual(graph, newGraph) {
		return fmt.Errorf("preserve unexpected native target")
	}
	// All original backups and target contents are now accounted for.
	for _, item := range currentSQL {
		if item != findLocalOriginal(j.OriginalSQL, item.Name) {
			if err := os.Remove(filepath.Join(dir, item.Name)); err != nil {
				return err
			}
		}
	}
	for _, item := range j.OriginalSQL {
		from := filepath.Join(backup, item.Name)
		if _, err := os.Lstat(from); err == nil {
			if _, err := os.Lstat(filepath.Join(dir, item.Name)); err == nil {
				// An identical original may only be present after partial rollback.
				if !matchesLocalInventory(filepath.Join(dir, item.Name), item) {
					return fmt.Errorf("SQLite target changed during rollback")
				}
				if err := os.Remove(filepath.Join(dir, item.Name)); err != nil {
					return err
				}
			}
			if err := localRestoreRename(from, filepath.Join(dir, item.Name)); err != nil {
				return err
			}
		}
	}
	if graphBackupErr == nil {
		if graphExists {
			if err := os.RemoveAll(graphPath); err != nil {
				return err
			}
		}
		if err := localRestoreRename(graphBackup, graphPath); err != nil {
			return err
		}
	} else if !j.HadGraph && graphExists {
		if err := os.RemoveAll(graphPath); err != nil {
			return err
		}
	}
	if err := localRestoreSyncDir(dir); err != nil {
		return err
	}
	if err := verifyManagedGraphOriginals(path, j); err != nil {
		return err
	}
	if keepInput {
		for _, name := range []string{"stage", "backup"} {
			if err := os.RemoveAll(filepath.Join(attempt, name)); err != nil {
				return err
			}
		}
		return localRestoreSyncDir(attempt)
	}
	if err := os.RemoveAll(attempt); err != nil {
		return err
	}
	if err := localRestoreSyncDir(dir); err != nil {
		return err
	}
	if err := os.Remove(localRestoreJournalPath(path)); err != nil {
		return err
	}
	return localRestoreSyncDir(dir)
}

func encodeManagedGraphInstallJournal(j localRestoreJournal) ([]byte, error) {
	if j.ManagedGraph == nil {
		return nil, fmt.Errorf("missing managed install binding")
	}
	binding := *j.ManagedGraph
	binding.Integrity = [32]byte{}
	j.ManagedGraph = &binding
	data, err := json.Marshal(j)
	if err != nil {
		return nil, err
	}
	binding.Integrity = sha256.Sum256(data)
	return json.Marshal(j)
}

func readManagedGraphInstallJournal(path string) (localRestoreJournal, error) {
	var j localRestoreJournal
	info, err := os.Lstat(localRestoreJournalPath(path))
	if err != nil {
		return j, err
	}
	if !info.Mode().IsRegular() || info.Size() > 64<<10 {
		return j, fmt.Errorf("invalid managed install journal file")
	}
	f, err := os.Open(localRestoreJournalPath(path))
	if err != nil {
		return j, err
	}
	data, readErr := io.ReadAll(io.LimitReader(f, (64<<10)+1))
	readErr = errors.Join(readErr, f.Close())
	if readErr != nil || len(data) > 64<<10 {
		return j, errors.Join(readErr, fmt.Errorf("invalid managed install journal size"))
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&j); err != nil {
		return j, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return j, fmt.Errorf("managed install journal trailing data")
	}
	if j.ManagedGraph == nil || j.ManagedGraph.Version != 1 || j.ManagedGraph.Protocol != sqlpolicy.ManagedGraphRequestApplicationProtocol || j.LocalVersion != 0 || j.Version != 1 || len(j.AttemptID) != 32 || !isLowerHex(j.AttemptID) {
		return j, fmt.Errorf("invalid managed install journal identity")
	}
	if err := validateManagedGraphInstall(j.ManagedGraph.Cut, 1); err != nil {
		return j, err
	}
	canonical, err := encodeManagedGraphInstallJournal(j)
	if err != nil || !bytes.Equal(canonical, data) {
		return j, fmt.Errorf("managed install journal integrity mismatch")
	}
	switch j.Phase {
	case "staged", "backup-sql", "backup-graph", "publish-graph", "published", "opening", "opened", "committed", "final-opened":
	default:
		return j, fmt.Errorf("managed install journal has no fully verified recoverable stage")
	}
	if !validManagedGraphInstallInventory(path, j.OriginalSQL, j.OriginalGraph, j.HadGraph, j.Files) || !validManagedGraphInstallInventory(path, j.ManagedGraph.NewSQL, j.ManagedGraph.NewGraph, true, j.Files) {
		return j, fmt.Errorf("managed install journal inventory invalid")
	}
	return j, nil
}

func validManagedGraphInstallInventory(path string, sql, graph []localRestoreOriginal, hadGraph bool, files []localRestoreFile) bool {
	hadSQL := findLocalOriginal(sql, filepath.Base(path)).Name != ""
	if hadSQL != hadGraph || (!hadSQL && len(sql) != 0) {
		return false
	}
	for _, item := range graph {
		if !knownManagedGraphNativeArtifact(item.Name) {
			return false
		}
	}
	copySQL := append([]localRestoreOriginal(nil), sql...)
	base := filepath.Base(path)
	for i := range copySQL {
		name := copySQL[i].Name
		if name != base && name != base+"-wal" && name != base+"-shm" && name != base+"-journal" {
			return false
		}
		copySQL[i].Name = "sqlite.db" + name[len(base):]
	}
	sort.Slice(copySQL, func(i, k int) bool { return copySQL[i].Name < copySQL[k].Name })
	return validRestoreInventory(localRestoreJournal{Files: files, OriginalSQL: copySQL, OriginalGraph: graph, HadGraph: hadGraph})
}

func knownManagedGraphNativeArtifact(name string) bool {
	switch name {
	case "graph.ltdb", "graph.ltdb-ids", "graph.ltdb-wal", "graph.ltdb-wal.base", "graph.ltdb.layout", "graph.ltdb.lock", "graph.ltdb.pages", "graph.ltdb.pages.layout",
		"graph.ltdb/ids.json", "graph.ltdb/state.json", "graph.ltdb/wal.log", "graph.ltdb/wal.base", "graph.ltdb/state.json.lock", "graph.ltdb/state.json.layout", "graph.ltdb/state.json.pages", "graph.ltdb/state.json.pages.layout":
		return true
	default:
		return false
	}
}

func recoverManagedGraphInstall(ctx context.Context, path string, readers int, input ManagedGraphInstall) (*Materializer, bool, error) {
	j, err := readManagedGraphInstallJournal(path)
	if err != nil {
		return nil, false, fmt.Errorf("preserve unverified managed install: %w", err)
	}
	files, descriptors, err := managedGraphInstallSources(path, input.Files)
	if err != nil {
		return nil, false, err
	}
	encoded, err := json.Marshal(struct {
		Protocol string
		Cut      ManagedGraphInstallCut
		Files    []localRestoreFile
	}{sqlpolicy.ManagedGraphRequestApplicationProtocol, input.Cut, descriptors})
	if err != nil {
		return nil, false, err
	}
	if j.ManagedGraph.Cut != input.Cut || j.ManagedGraph.Digest != sha256.Sum256(encoded) || !reflect.DeepEqual(j.Files, descriptors) {
		return nil, false, fmt.Errorf("preserve managed install bound to different authenticated cut")
	}
	attempt := localRestoreAttemptPath(path, j.AttemptID)
	retained, err := retainedManagedGraphInputs(path, j)
	if err != nil {
		return nil, false, err
	}
	for _, file := range files {
		rel, err := filepath.Rel(attempt, file.Path)
		if err != nil || (!filepath.IsAbs(rel) && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))) {
			if err != nil || file.Path != managedGraphInputPath(attempt, file.Role) {
				return nil, false, fmt.Errorf("install recovery source aliases owned attempt")
			}
		}
		length, hash, err := hashLocalRestoreFile(file.Path)
		if err != nil || length != file.ExpectedLength || hash != file.ExpectedSHA256 {
			return nil, false, fmt.Errorf("install recovery source bytes changed")
		}
	}
	if err := validateManagedGraphAttemptTree(path, attempt, j); err != nil {
		return nil, false, err
	}
	if err := verifyManagedGraphPhaseProjection(ctx, path, readers, input, j, retained); err != nil {
		return nil, false, err
	}
	newSQL, newGraph := j.ManagedGraph.NewSQL, j.ManagedGraph.NewGraph
	if j.Phase == "opening" || j.Phase == "opened" || j.Phase == "committed" || j.Phase == "final-opened" {
		newSQL, newGraph, err = observeKnownManagedGraphInstalled(path, j)
		if err != nil {
			return nil, false, err
		}
	}
	if j.Phase == "committed" || j.Phase == "final-opened" {
		var state ManagedGraphTargetState
		if err := inspectManagedGraphInstallTarget(ctx, path, input.Cut.IdempotencyWindow, &state); err != nil {
			return nil, false, err
		}
		if len(state.Replay) != 0 || state.SQLiteTip != input.Cut.CertifiedTip || state.NativeHistory != input.Cut.ExpectedGraphHistory {
			return nil, false, fmt.Errorf("committed install no longer matches authenticated cut")
		}
		opened, err := finishManagedGraphInstall(ctx, path, readers, input, attempt)
		return opened, true, err
	}
	if err := rollbackManagedGraphInstallKeepingInput(path, attempt, j, newSQL, newGraph, true); err != nil {
		return nil, false, err
	}
	if err := hitLocalRestoreBoundary("managed:after-crash-rollback"); err != nil {
		return nil, false, err
	}
	j.Phase = "intent"
	j.ManagedGraph.NewSQL, j.ManagedGraph.NewGraph = nil, nil
	if err := writeLocalRestoreJournal(path, j); err != nil {
		return nil, false, err
	}
	opened, err := runManagedGraphInstall(ctx, path, readers, input, j, retained)
	return opened, true, err
}

// The caller still authenticates Cut. This only binds fixed owned paths to the
// original exact descriptors and actual bytes; arbitrary attempt paths are not
// accepted as source and journal checksums are not producer authentication.
func retainedManagedGraphInputs(path string, j localRestoreJournal) ([]CheckpointFile, error) {
	attempt := localRestoreAttemptPath(path, j.AttemptID)
	if err := validateLocalRestoreAttemptDir(path, j, false); err != nil {
		return nil, err
	}
	info, err := os.Lstat(filepath.Join(attempt, "input"))
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("preserve missing or invalid managed install input directory")
	}
	files := make([]CheckpointFile, len(j.Files))
	for i, descriptor := range j.Files {
		file := managedGraphInputPath(attempt, descriptor.Role)
		if !matchesLocalInventory(file, localRestoreOriginal{Length: descriptor.Length, SHA256: descriptor.SHA256}) {
			return nil, fmt.Errorf("preserve missing or changed managed install input")
		}
		files[i] = CheckpointFile{Role: descriptor.Role, Path: file, ExpectedLength: descriptor.Length, ExpectedSHA256: descriptor.SHA256}
	}
	entries, err := os.ReadDir(filepath.Join(attempt, "input"))
	if err != nil || len(entries) != 2 {
		return nil, fmt.Errorf("preserve unknown managed install input artifact")
	}
	return files, nil
}

func observeKnownManagedGraphInstalled(path string, j localRestoreJournal) ([]localRestoreOriginal, []localRestoreOriginal, error) {
	sql, err := inspectLocalOriginalSQL(path)
	if err != nil {
		return nil, nil, err
	}
	base := filepath.Base(path)
	for _, item := range sql {
		want := findLocalOriginal(j.ManagedGraph.NewSQL, item.Name)
		if item == want {
			continue
		}
		// These are only fresh coordination sidecars of an otherwise byte-exact
		// installed pair. A nonempty WAL or changed main is never discarded.
		if want.Name == "" && ((item.Name == base+"-wal" && item.Length == 0) || (item.Name == base+"-shm" && item.Length == 32768)) {
			continue
		}
		return nil, nil, fmt.Errorf("preserve unknown bytes after managed install open")
	}
	for _, want := range j.ManagedGraph.NewSQL {
		if want.Name == base && findLocalOriginal(sql, want.Name) != want {
			return nil, nil, fmt.Errorf("installed main differs from journal")
		}
	}
	graph, exists, err := inspectLocalOriginalGraph(filepath.Join(filepath.Dir(path), "latticedb"))
	if err != nil || !exists || !reflect.DeepEqual(graph, j.ManagedGraph.NewGraph) {
		return nil, nil, fmt.Errorf("preserve unknown native bytes after managed install open")
	}
	return sql, graph, nil
}

func validateManagedGraphAttemptTree(path, attempt string, j localRestoreJournal) error {
	others, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".rhiza-local-restore-*"))
	if err != nil {
		return err
	}
	for _, other := range others {
		if other != attempt {
			return fmt.Errorf("preserve unrelated managed restore remnant")
		}
	}
	if _, err := os.Lstat(attempt); os.IsNotExist(err) && (j.Phase == "committed" || j.Phase == "final-opened") {
		return nil
	}
	if err := validateLocalRestoreAttemptDir(path, j, false); err != nil {
		return err
	}
	allowed := make(map[string]localRestoreOriginal)
	dirs := map[string]bool{".": true, "input": true, "stage": true, "stage/latticedb": true, "backup": true, "backup/latticedb": true}
	for _, file := range j.Files {
		rel := "input/" + filepath.Base(managedGraphInputPath(attempt, file.Role))
		allowed[rel] = localRestoreOriginal{Length: file.Length, SHA256: file.SHA256}
	}
	add := func(prefix string, items []localRestoreOriginal, stageSQL bool) {
		for _, item := range items {
			name := item.Name
			if stageSQL {
				name = "sqlite.db" + name[len(filepath.Base(path)):]
			}
			rel := prefix + "/" + name
			allowed[rel] = item
			for parent := filepath.ToSlash(filepath.Dir(rel)); parent != "."; parent = filepath.ToSlash(filepath.Dir(parent)) {
				dirs[parent] = true
			}
		}
	}
	add("stage", j.ManagedGraph.NewSQL, true)
	add("stage/latticedb", j.ManagedGraph.NewGraph, false)
	add("backup", j.OriginalSQL, false)
	add("backup/latticedb", j.OriginalGraph, false)
	return filepath.WalkDir(attempt, func(current string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(attempt, current)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("preserve symlink in managed install attempt")
		}
		if entry.IsDir() {
			if !dirs[rel] {
				return fmt.Errorf("preserve unknown managed install directory")
			}
			return nil
		}
		want, known := allowed[rel]
		if !known {
			return fmt.Errorf("preserve unknown managed install artifact")
		}
		length, hash, err := hashLocalRestoreFile(current)
		if err != nil || length != want.Length || hash != want.SHA256 {
			return fmt.Errorf("preserve changed managed install artifact")
		}
		return nil
	})
}

// Re-derive the known installed bytes from the actual authenticated snapshot
// inputs. Journal checksums are corruption detection, never a substitute for
// producer provenance or permission to trust arbitrary native bytes.
func verifyManagedGraphPhaseProjection(ctx context.Context, path string, readers int, input ManagedGraphInstall, j localRestoreJournal, files []CheckpointFile) (resultErr error) {
	attempt := localRestoreAttemptPath(path, j.AttemptID)
	if err := ensureLocalRestoreDir(attempt); err != nil {
		return err
	}
	dir, err := os.MkdirTemp(attempt, "verify-*")
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, os.RemoveAll(dir)) }()
	if err := ensureLocalRestoreDir(filepath.Join(dir, "latticedb")); err != nil {
		return err
	}
	for _, file := range files {
		target := filepath.Join(dir, "sqlite.db")
		if file.Role == CheckpointGraphData {
			target = filepath.Join(dir, "latticedb", "graph.ltdb")
		}
		if err := copyLocalRestoreFileContextLimit(ctx, file.Path, target, int64(file.ExpectedLength)+1); err != nil {
			return err
		}
		length, hash, err := hashLocalRestoreFile(target)
		if err != nil || length != file.ExpectedLength || hash != file.ExpectedSHA256 {
			return fmt.Errorf("recovery proof snapshot bytes differ")
		}
	}
	cycles := 1
	live := false
	switch j.Phase {
	case "opened":
		cycles, live = 2, true
	case "committed":
		cycles = 2
	case "final-opened":
		cycles, live = 3, true
	}
	var opened *Materializer
	defer func() {
		if opened != nil {
			resultErr = errors.Join(resultErr, opened.Close())
		}
	}()
	for i := 0; i < cycles; i++ {
		opened, err = OpenManagedSQLKVGraphWithOriginals(filepath.Join(dir, "sqlite.db"), readers, input.Cut.IdempotencyWindow)
		if err != nil {
			return err
		}
		if err := validateManagedGraphInstalled(ctx, opened, input.Cut); err != nil {
			return err
		}
		if i+1 == cycles && live {
			opened.stopSQLiteCheckpoints()
			opened.walCheckpointIO.Wait()
			break
		}
		if err := opened.Close(); err != nil {
			return err
		}
		opened = nil
	}
	sql, err := inspectLocalOriginalSQL(filepath.Join(dir, "sqlite.db"))
	if err != nil {
		return err
	}
	for i := range sql {
		sql[i].Name = filepath.Base(path) + sql[i].Name[len("sqlite.db"):]
	}
	graph, _, err := inspectLocalOriginalGraph(filepath.Join(dir, "latticedb"))
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(sql, j.ManagedGraph.NewSQL) || !reflect.DeepEqual(graph, j.ManagedGraph.NewGraph) {
		return fmt.Errorf("journal installed inventory is not reproducible from authenticated pair")
	}
	return nil
}
