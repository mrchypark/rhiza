package materializer

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
)

const localRestoreJournalVersion = 1

// localRestoreFailpoint is a package-private crash-boundary seam used by the
// restore protocol tests. Production code leaves it nil.
var localRestoreFailpoint func(string) error

// Narrow syscall seams for deterministic Local restore I/O fault tests.
var (
	localRestoreWriteFile = func(file *os.File, data []byte) (int, error) { return file.Write(data) }
	localRestoreSyncFile  = func(file *os.File) error { return file.Sync() }
	localRestoreSyncDir   = syncDirectory
	localRestoreRename    = os.Rename
)

// LocalRestoreAuthority is supplied only after the caller validates the exact
// compacted WAL base and its immutable checkpoint descriptor.
type LocalRestoreAuthority struct {
	StoreUUID    string
	ConfigDigest string
	Index        uint64
	ConfigID     uint
	RootHash     string
	PrefixHash   string
	StateHash    string
}

type localRestoreFile struct {
	Role   CheckpointRole `json:"role"`
	Length uint64         `json:"length"`
	SHA256 string         `json:"sha256"`
}

type localRestoreOriginal struct {
	Name   string `json:"name"`
	Length uint64 `json:"length"`
	SHA256 string `json:"sha256"`
}

type localRestoreJournal struct {
	Version       int                    `json:"version"`
	LocalVersion  int                    `json:"local_version"`
	StoreUUID     string                 `json:"store_uuid"`
	ConfigDigest  string                 `json:"config_digest"`
	Index         uint64                 `json:"index"`
	ConfigID      uint                   `json:"config_id"`
	RootHash      string                 `json:"root_hash"`
	PrefixHash    string                 `json:"prefix_hash"`
	StateHash     string                 `json:"state_hash"`
	AttemptID     string                 `json:"attempt_id"`
	Phase         string                 `json:"phase"`
	Files         []localRestoreFile     `json:"files"`
	OriginalSQL   []localRestoreOriginal `json:"original_sql"`
	OriginalGraph []localRestoreOriginal `json:"original_graph"`
	HadGraph      bool                   `json:"had_graph"`
}

// PrepareLocalRestore durably authorizes one exact-base restore attempt after
// the caller has validated the WAL base and immutable source descriptor. It
// does not move or rewrite the live materializer.
func PrepareLocalRestore(dbPath string, authority LocalRestoreAuthority, files []CheckpointFile) error {
	dbPath, err := filepath.Abs(dbPath)
	if err != nil {
		return err
	}
	if err := validateLocalRestoreAuthority(authority); err != nil {
		return err
	}
	inputs, err := inspectLocalRestoreSources(files)
	if err != nil {
		return err
	}
	journalPath := localRestoreJournalPath(dbPath)
	if journal, err := readLocalRestoreJournal(journalPath); err == nil {
		if !journal.matches(authority, inputs) {
			return fmt.Errorf("pending Local restore journal is bound to a different WAL base; preserve existing files")
		}
		if err := validateLocalRestoreAttemptDir(dbPath, journal, true); err != nil {
			return err
		}
		if journal.Phase == "intent" || journal.Phase == "staged" {
			if err := validateLocalRestoreOriginalPolicy(dbPath, journal); err != nil {
				return err
			}
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := rejectUnjournaledLocalRestoreArtifacts(dbPath); err != nil {
		return err
	}
	if err := sqlpolicy.CheckExisting(context.Background(), dbPath); err != nil {
		return fmt.Errorf("incompatible Local SQLite original: %w", err)
	}
	if err := ValidateLocalGraphStorage(filepath.Join(filepath.Dir(dbPath), "latticedb", "graph.ltdb"), false); err != nil {
		return fmt.Errorf("incompatible Local Graph original: %w", err)
	}
	originalSQL, err := inspectLocalOriginalSQL(dbPath)
	if err != nil {
		return err
	}
	sort.Slice(originalSQL, func(i, j int) bool { return originalSQL[i].Name < originalSQL[j].Name })
	originalGraph, hadGraph, err := inspectLocalOriginalGraph(filepath.Join(filepath.Dir(dbPath), "latticedb"))
	if err != nil {
		return err
	}
	var attempt [16]byte
	if _, err := rand.Read(attempt[:]); err != nil {
		return err
	}
	journal := localRestoreJournal{
		Version: localRestoreJournalVersion, LocalVersion: localRestoreJournalVersion, StoreUUID: authority.StoreUUID, ConfigDigest: authority.ConfigDigest,
		Index: authority.Index, ConfigID: authority.ConfigID, RootHash: authority.RootHash, PrefixHash: authority.PrefixHash, StateHash: authority.StateHash,
		AttemptID: hex.EncodeToString(attempt[:]), Phase: "intent", Files: inputs,
		OriginalSQL: originalSQL, OriginalGraph: originalGraph, HadGraph: hadGraph,
	}
	if err := writeLocalRestoreJournal(dbPath, journal); err != nil {
		return err
	}
	if localRestoreFailpoint != nil {
		return localRestoreFailpoint("after-intent")
	}
	return nil
}

// OpenLocalFromBase resumes only a restore journal authorized for the exact
// selected WAL base. Node finalizes this base restore before replaying any WAL
// suffix so the journal never spans legitimate post-base activity.
func OpenLocalFromBase(dbPath string, readerCount int, authority LocalRestoreAuthority, files []CheckpointFile, indexes []types.GraphNodePropertyIndex, idempotencyWindow ...uint64) (*Materializer, error) {
	dbPath, err := filepath.Abs(dbPath)
	if err != nil {
		return nil, err
	}
	inputs, err := inspectLocalRestoreSources(files)
	if err != nil {
		return nil, err
	}
	journal, err := readLocalRestoreJournal(localRestoreJournalPath(dbPath))
	if err != nil {
		return nil, fmt.Errorf("open Local restore journal: %w", err)
	}
	if !journal.matches(authority, inputs) {
		return nil, fmt.Errorf("Local restore journal does not match the selected WAL base and verified source")
	}
	// "installed" means the authorized pair at N was opened and validated.
	// Node finalizes the journal before it allows any post-N suffix application.
	wasInstalled := journal.Phase == "installed"
	if err := resumeLocalRestore(dbPath, journal, files); err != nil {
		return nil, fmt.Errorf("resume exact-base Local restore: %w", err)
	}
	m, err := openMaterializer(dbPath, readerCount, idempotencyWindow...)
	if err != nil {
		return nil, err
	}
	if !wasInstalled {
		if err := m.validateRestoredSnapshot(); err != nil {
			_ = m.Close()
			return nil, fmt.Errorf("validate installed Local base: %w", err)
		}
		if err := hitLocalRestoreBoundary("after-validation"); err != nil {
			_ = m.Close()
			return nil, err
		}
	}
	if m.Tip() != authority.Index || m.graphTip() != authority.Index {
		_ = m.Close()
		return nil, fmt.Errorf("installed Local base tips SQL=%d Graph=%d, require exact authorized index %d", m.Tip(), m.graphTip(), authority.Index)
	}
	if err := ensureGraphNodePropertyIndexes(m.graph, indexes); err != nil {
		_ = m.Close()
		return nil, err
	}
	m.graphNodePropertyIndexes = append([]types.GraphNodePropertyIndex(nil), indexes...)
	journal.Phase = "installed"
	if err := writeLocalRestoreJournal(dbPath, journal); err != nil {
		_ = m.Close()
		return nil, err
	}
	if err := hitLocalRestoreBoundary("after-installed"); err != nil {
		_ = m.Close()
		return nil, err
	}
	return m, nil
}

// FinalizeLocalRestore removes the retained original pair after the exact base
// pair is installed and validated, before the caller applies any WAL suffix.
func FinalizeLocalRestore(dbPath string, authority LocalRestoreAuthority) error {
	dbPath, err := filepath.Abs(dbPath)
	if err != nil {
		return err
	}
	journal, err := readLocalRestoreJournal(localRestoreJournalPath(dbPath))
	if err != nil {
		return err
	}
	if !journal.matchesAuthority(authority) || journal.Phase != "installed" {
		return fmt.Errorf("Local restore is not durably installed for the selected WAL base")
	}
	attemptDir := localRestoreAttemptPath(dbPath, journal.AttemptID)
	if err := os.RemoveAll(attemptDir); err != nil {
		return fmt.Errorf("remove Local restore stage and original backup: %w", err)
	}
	if err := localRestoreSyncDir(filepath.Dir(dbPath)); err != nil {
		return err
	}
	if err := hitLocalRestoreBoundary("after-cleanup"); err != nil {
		return err
	}
	if err := os.Remove(localRestoreJournalPath(dbPath)); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := localRestoreSyncDir(filepath.Dir(dbPath)); err != nil {
		return err
	}
	return nil
}

// ValidateNoLocalRestore rejects a floor-zero Local database with a restore
// journal or unexplained restore backups; no unsealed/pending value authorizes
// replacing the materializer.
func ValidateNoLocalRestore(dbPath string) error {
	dbPath, err := filepath.Abs(dbPath)
	if err != nil {
		return err
	}
	if _, err := os.Lstat(localRestoreJournalPath(dbPath)); err == nil {
		return fmt.Errorf("floor-zero Local WAL cannot authorize an interrupted restore; preserve the journal")
	} else if !os.IsNotExist(err) {
		return err
	}
	return rejectUnjournaledLocalRestoreArtifacts(dbPath)
}

func localRestoreJournalPath(dbPath string) string { return dbPath + ".local-restore.json" }

func localRestoreAttemptPath(dbPath, attempt string) string {
	return filepath.Join(filepath.Dir(dbPath), ".rhiza-local-restore-"+attempt)
}

func validateLocalRestoreAuthority(a LocalRestoreAuthority) error {
	if len(a.StoreUUID) != 32 || len(a.ConfigDigest) != 64 || a.Index == 0 || a.ConfigID == 0 || !isLowerHex(a.StoreUUID) || !isLowerHex(a.ConfigDigest) || len(a.RootHash) != 64 || len(a.PrefixHash) != 64 || len(a.StateHash) != 64 || !isLowerHex(a.RootHash) || !isLowerHex(a.PrefixHash) || !isLowerHex(a.StateHash) {
		return fmt.Errorf("invalid exact-base Local restore authority")
	}
	return nil
}

func isLowerHex(s string) bool {
	if s == "" || strings.ToLower(s) != s {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

func inspectLocalRestoreSources(files []CheckpointFile) ([]localRestoreFile, error) {
	if len(files) != 2 {
		return nil, fmt.Errorf("Local restore requires exactly the fixed SQLite and Graph roles")
	}
	byRole := make(map[CheckpointRole]CheckpointFile, 2)
	for _, file := range files {
		if (file.Role != CheckpointSQLite && file.Role != CheckpointGraphData) || file.Path == "" || byRole[file.Role].Path != "" {
			return nil, fmt.Errorf("invalid Local restore role set")
		}
		byRole[file.Role] = file
	}
	if byRole[CheckpointSQLite].Path == "" || byRole[CheckpointGraphData].Path == "" {
		return nil, fmt.Errorf("Local restore source is missing a fixed role")
	}
	if err := sqlpolicy.CheckSnapshotFile(context.Background(), byRole[CheckpointSQLite].Path); err != nil {
		return nil, fmt.Errorf("validate Local SQLite checkpoint source: %w", err)
	}
	if err := validateLocalGraphCheckpointFile(byRole[CheckpointGraphData].Path); err != nil {
		return nil, fmt.Errorf("validate Local Graph checkpoint source: %w", err)
	}
	out := make([]localRestoreFile, 0, 2)
	for _, role := range []CheckpointRole{CheckpointSQLite, CheckpointGraphData} {
		file := byRole[role]
		length, hash, err := hashLocalRestoreFile(file.Path)
		if err != nil {
			return nil, err
		}
		if file.ExpectedLength == 0 || len(file.ExpectedSHA256) != 64 || !isLowerHex(file.ExpectedSHA256) {
			return nil, fmt.Errorf("missing or invalid authorized Local checkpoint metadata for role %s", role)
		}
		if file.ExpectedLength != length || file.ExpectedSHA256 != hash {
			return nil, fmt.Errorf("Local checkpoint source role %s differs from authorized descriptor", role)
		}
		out = append(out, localRestoreFile{Role: role, Length: length, SHA256: hash})
	}
	return out, nil
}

func validateLocalGraphCheckpointFile(source string) error {
	info, err := os.Lstat(source)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("invalid Local Graph checkpoint source")
	}
	return validateLocalGraphStorage(source, false, true)
}

func (j localRestoreJournal) matches(a LocalRestoreAuthority, files []localRestoreFile) bool {
	return j.matchesAuthority(a) && equalLocalRestoreFiles(j.Files, files)
}

func (j localRestoreJournal) matchesAuthority(a LocalRestoreAuthority) bool {
	return validateLocalRestoreAuthority(a) == nil && j.Version == localRestoreJournalVersion && j.LocalVersion == localRestoreJournalVersion && j.StoreUUID == a.StoreUUID && j.ConfigDigest == a.ConfigDigest && j.Index == a.Index && j.ConfigID == a.ConfigID && j.RootHash == a.RootHash && j.PrefixHash == a.PrefixHash && j.StateHash == a.StateHash && isLowerHex(j.AttemptID) && len(j.AttemptID) == 32 && validLocalRestorePhase(j.Phase)
}

func equalLocalRestoreFiles(a, b []localRestoreFile) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func readLocalRestoreJournal(path string) (localRestoreJournal, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return localRestoreJournal{}, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() > 64<<10 {
		return localRestoreJournal{}, fmt.Errorf("invalid Local restore journal file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return localRestoreJournal{}, err
	}
	var j localRestoreJournal
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&j); err != nil {
		return localRestoreJournal{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return localRestoreJournal{}, fmt.Errorf("Local restore journal has trailing data")
	}
	if j.Version != localRestoreJournalVersion || j.LocalVersion != localRestoreJournalVersion || !isLowerHex(j.AttemptID) || len(j.AttemptID) != 32 || !validLocalRestorePhase(j.Phase) {
		return localRestoreJournal{}, fmt.Errorf("invalid Local restore journal identity or phase")
	}
	if err := validateLocalRestoreAuthority(LocalRestoreAuthority{StoreUUID: j.StoreUUID, ConfigDigest: j.ConfigDigest, Index: j.Index, ConfigID: j.ConfigID, RootHash: j.RootHash, PrefixHash: j.PrefixHash, StateHash: j.StateHash}); err != nil {
		return localRestoreJournal{}, err
	}
	if !validRestoreInventory(j) {
		return localRestoreJournal{}, fmt.Errorf("invalid Local restore journal inventory")
	}
	return j, nil
}

func validLocalRestorePhase(phase string) bool {
	switch phase {
	case "intent", "staged", "backup-sql", "backup-graph", "publish-graph", "published", "installed":
		return true
	default:
		return false
	}
}

func validRestoreInventory(j localRestoreJournal) bool {
	if len(j.Files) != 2 || j.Files[0].Role != CheckpointSQLite || j.Files[1].Role != CheckpointGraphData {
		return false
	}
	for _, file := range j.Files {
		if file.Length == 0 || len(file.SHA256) != 64 || !isLowerHex(file.SHA256) {
			return false
		}
	}
	for _, entries := range [][]localRestoreOriginal{j.OriginalSQL, j.OriginalGraph} {
		last := ""
		for _, entry := range entries {
			clean := filepath.Clean(filepath.FromSlash(entry.Name))
			if entry.Name <= last || filepath.IsAbs(entry.Name) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) || filepath.ToSlash(clean) != entry.Name || len(entry.SHA256) != 64 || !isLowerHex(entry.SHA256) {
				return false
			}
			last = entry.Name
		}
	}
	allowedSQL := map[string]bool{"sqlite.db": true, "sqlite.db-wal": true, "sqlite.db-shm": true, "sqlite.db-journal": true}
	for _, item := range j.OriginalSQL {
		if !allowedSQL[item.Name] {
			return false
		}
	}
	return j.HadGraph == (len(j.OriginalGraph) != 0)
}

func writeLocalRestoreJournal(dbPath string, j localRestoreJournal) error {
	data, err := json.Marshal(j)
	if err != nil {
		return err
	}
	dir := filepath.Dir(dbPath)
	f, err := os.CreateTemp(dir, ".rhiza-local-restore-journal-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err = f.Chmod(0o600); err == nil {
		n, writeErr := localRestoreWriteFile(f, data)
		err = writeErr
		if err == nil && n != len(data) {
			err = io.ErrShortWrite
		}
	}
	if err == nil {
		err = localRestoreSyncFile(f)
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err := localRestoreRename(tmp, localRestoreJournalPath(dbPath)); err != nil {
		return err
	}
	return localRestoreSyncDir(dir)
}

type localRestoreFileWriter struct{ file *os.File }

func (w localRestoreFileWriter) Write(data []byte) (int, error) {
	return localRestoreWriteFile(w.file, data)
}

func hitLocalRestoreBoundary(name string) error {
	if localRestoreFailpoint != nil {
		return localRestoreFailpoint(name)
	}
	return nil
}

func inspectLocalOriginalSQL(dbPath string) ([]localRestoreOriginal, error) {
	var result []localRestoreOriginal
	for _, name := range []string{filepath.Base(dbPath), filepath.Base(dbPath) + "-wal", filepath.Base(dbPath) + "-shm", filepath.Base(dbPath) + "-journal"} {
		path := filepath.Join(filepath.Dir(dbPath), name)
		item, exists, err := inventoryLocalFile(path, name)
		if err != nil {
			return nil, err
		}
		if exists {
			result = append(result, item)
		}
	}
	return result, nil
}

func inspectLocalOriginalGraph(path string) ([]localRestoreOriginal, bool, error) {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return nil, false, fmt.Errorf("invalid Local graph original inventory")
	}
	var result []localRestoreOriginal
	err = filepath.WalkDir(path, func(current string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink in Local graph original inventory")
		}
		if entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(path, current)
		if err != nil {
			return err
		}
		item, exists, err := inventoryLocalFile(current, filepath.ToSlash(relative))
		if err != nil {
			return err
		}
		if exists {
			result = append(result, item)
		}
		return nil
	})
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, true, err
}

func inventoryLocalFile(path, name string) (localRestoreOriginal, bool, error) {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return localRestoreOriginal{}, false, nil
	}
	if err != nil {
		return localRestoreOriginal{}, false, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() < 0 {
		return localRestoreOriginal{}, false, fmt.Errorf("invalid Local original file %s", name)
	}
	length, hash, err := hashLocalRestoreFile(path)
	return localRestoreOriginal{Name: name, Length: length, SHA256: hash}, true, err
}

func hashLocalRestoreFile(path string) (uint64, string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return 0, "", err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return 0, "", fmt.Errorf("invalid Local restore file %s", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	hash := sha256.New()
	length, err := io.Copy(hash, f)
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return 0, "", err
	}
	return uint64(length), hex.EncodeToString(hash.Sum(nil)), nil
}

func rejectUnjournaledLocalRestoreArtifacts(dbPath string) error {
	for _, path := range []string{dbPath + ".restore-backup", filepath.Join(filepath.Dir(dbPath), "latticedb.restore-backup")} {
		if _, err := os.Lstat(path); err == nil {
			return fmt.Errorf("unexplained Local restore backup at %s; preserve existing files", path)
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	matches, err := filepath.Glob(filepath.Join(filepath.Dir(dbPath), ".rhiza-local-restore-*"))
	if err != nil {
		return err
	}
	if len(matches) != 0 {
		return fmt.Errorf("unexplained Local restore attempt directory; preserve existing files")
	}
	return nil
}

func validateLocalRestoreAttemptDir(dbPath string, j localRestoreJournal, allowMissing bool) error {
	path := localRestoreAttemptPath(dbPath, j.AttemptID)
	info, err := os.Lstat(path)
	if os.IsNotExist(err) && allowMissing {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("invalid managed Local restore attempt directory")
	}
	return nil
}

func validateLocalRestoreOriginalPolicy(dbPath string, j localRestoreJournal) error {
	dir := filepath.Dir(dbPath)
	backup := filepath.Join(localRestoreAttemptPath(dbPath, j.AttemptID), "backup")
	sqlPath := dbPath
	if _, err := os.Lstat(sqlPath); os.IsNotExist(err) {
		sqlPath = filepath.Join(backup, filepath.Base(dbPath))
	}
	if _, err := os.Lstat(sqlPath); err == nil {
		if err := sqlpolicy.CheckExisting(context.Background(), sqlPath); err != nil {
			return fmt.Errorf("incompatible Local SQLite original: %w", err)
		}
	}
	graphPath := filepath.Join(dir, "latticedb", "graph.ltdb")
	if _, err := os.Lstat(graphPath); os.IsNotExist(err) {
		graphPath = filepath.Join(backup, "latticedb", "graph.ltdb")
	}
	if _, err := os.Lstat(graphPath); err == nil {
		if err := ValidateLocalGraphStorage(graphPath, false); err != nil {
			return fmt.Errorf("incompatible Local Graph original: %w", err)
		}
	}
	return nil
}

func resumeLocalRestore(dbPath string, j localRestoreJournal, files []CheckpointFile) error {
	if j.Phase == "installed" {
		return nil
	}
	attempt := localRestoreAttemptPath(dbPath, j.AttemptID)
	if err := validateLocalRestoreAttemptDir(dbPath, j, true); err != nil {
		return err
	}
	stage := filepath.Join(attempt, "stage")
	backup := filepath.Join(attempt, "backup")
	if err := ensureLocalRestoreDir(attempt); err != nil {
		return err
	}
	if err := ensureLocalRestoreDir(stage); err != nil {
		return err
	}
	if err := ensureLocalRestoreDir(filepath.Join(stage, "latticedb")); err != nil {
		return err
	}
	if err := ensureLocalRestoreDir(backup); err != nil {
		return err
	}
	for _, source := range files {
		name := "sqlite.db"
		if source.Role == CheckpointGraphData {
			name = filepath.Join("latticedb", "graph.ltdb")
		}
		target := filepath.Join(stage, name)
		if err := copyLocalRestoreFile(source.Path, target); err != nil {
			return err
		}
	}
	for i, target := range []string{filepath.Join(stage, "sqlite.db"), filepath.Join(stage, "latticedb", "graph.ltdb")} {
		length, hash, err := hashLocalRestoreFile(target)
		if err != nil || length != j.Files[i].Length || hash != j.Files[i].SHA256 {
			return fmt.Errorf("staged Local checkpoint role %s differs from verified source", j.Files[i].Role)
		}
	}
	if err := localRestoreSyncDir(filepath.Join(stage, "latticedb")); err != nil {
		return err
	}
	if err := localRestoreSyncDir(stage); err != nil {
		return err
	}
	if err := localRestoreSyncDir(attempt); err != nil {
		return err
	}
	j.Phase = "staged"
	if err := writeLocalRestoreJournal(dbPath, j); err != nil {
		return err
	}
	if err := hitLocalRestoreBoundary("after-staged"); err != nil {
		return err
	}
	if err := backupLocalOriginals(dbPath, attempt, &j); err != nil {
		return err
	}
	if err := publishLocalRestore(dbPath, stage, &j); err != nil {
		return err
	}
	j.Phase = "published"
	if err := writeLocalRestoreJournal(dbPath, j); err != nil {
		return err
	}
	if localRestoreFailpoint != nil {
		return localRestoreFailpoint("after-published")
	}
	return nil
}

func ensureLocalRestoreDir(path string) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		if err := os.Mkdir(path, 0o700); err != nil {
			return err
		}
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("invalid managed Local restore directory")
	}
	return nil
}

func copyLocalRestoreFile(source, target string) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	if err := os.Remove(target); err != nil && !os.IsNotExist(err) {
		return err
	}
	out, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, err = io.Copy(localRestoreFileWriter{file: out}, in)
	if err == nil {
		err = localRestoreSyncFile(out)
	}
	if closeErr := out.Close(); err == nil {
		err = closeErr
	}
	return err
}

func backupLocalOriginals(dbPath, attempt string, j *localRestoreJournal) error {
	backup := filepath.Join(attempt, "backup")
	dir := filepath.Dir(dbPath)
	for _, item := range j.OriginalSQL {
		source, target := filepath.Join(dir, item.Name), filepath.Join(backup, item.Name)
		if _, err := os.Lstat(target); err == nil {
			if !matchesLocalInventory(target, item) {
				return fmt.Errorf("Local restore backup conflicts with recorded original %s", item.Name)
			}
			continue
		} else if !os.IsNotExist(err) {
			return err
		}
		if !matchesLocalInventory(source, item) {
			return fmt.Errorf("Local original %s changed after restore authorization", item.Name)
		}
		if err := localRestoreRename(source, target); err != nil {
			return err
		}
		if err := localRestoreSyncDir(dir); err != nil {
			return err
		}
		if err := localRestoreSyncDir(backup); err != nil {
			return err
		}
		j.Phase = "backup-sql"
		if err := writeLocalRestoreJournal(dbPath, *j); err != nil {
			return err
		}
		if err := hitLocalRestoreBoundary("after-backup-sql"); err != nil {
			return err
		}
	}
	graph := filepath.Join(dir, "latticedb")
	graphBackup := filepath.Join(backup, "latticedb")
	if j.HadGraph {
		if _, err := os.Lstat(graphBackup); err == nil {
			if err := verifyLocalDirectoryInventory(graphBackup, j.OriginalGraph); err != nil {
				return err
			}
		} else if !os.IsNotExist(err) {
			return err
		} else {
			if err := verifyLocalDirectoryInventory(graph, j.OriginalGraph); err != nil {
				return fmt.Errorf("Local graph original changed after authorization: %w", err)
			}
			if err := localRestoreRename(graph, graphBackup); err != nil {
				return err
			}
			if err := localRestoreSyncDir(dir); err != nil {
				return err
			}
			if err := localRestoreSyncDir(backup); err != nil {
				return err
			}
			j.Phase = "backup-graph"
			if err := writeLocalRestoreJournal(dbPath, *j); err != nil {
				return err
			}
			if err := hitLocalRestoreBoundary("after-backup-graph"); err != nil {
				return err
			}
		}
	}
	return nil
}

func publishLocalRestore(dbPath, stage string, j *localRestoreJournal) error {
	dir := filepath.Dir(dbPath)
	graph := filepath.Join(dir, "latticedb")
	graphBackup := filepath.Join(localRestoreAttemptPath(dbPath, j.AttemptID), "backup", "latticedb")
	if j.HadGraph {
		if err := verifyLocalDirectoryInventory(graphBackup, j.OriginalGraph); err != nil {
			return fmt.Errorf("Local graph original backup is unavailable: %w", err)
		}
	} else if _, err := os.Lstat(graph); err == nil {
		length, hash, hashErr := hashLocalRestoreFile(filepath.Join(graph, "graph.ltdb"))
		if hashErr != nil || length != j.Files[1].Length || hash != j.Files[1].SHA256 {
			return fmt.Errorf("unexpected Local Graph target before restore publication")
		}
	}
	if err := os.RemoveAll(graph); err != nil {
		return err
	}
	if err := localRestoreRename(filepath.Join(stage, "latticedb"), graph); err != nil {
		return err
	}
	if err := localRestoreSyncDir(stage); err != nil {
		return err
	}
	if err := localRestoreSyncDir(dir); err != nil {
		return err
	}
	j.Phase = "publish-graph"
	if err := writeLocalRestoreJournal(dbPath, *j); err != nil {
		return err
	}
	if err := hitLocalRestoreBoundary("after-publish-graph"); err != nil {
		return err
	}
	mainBackup := filepath.Join(localRestoreAttemptPath(dbPath, j.AttemptID), "backup", filepath.Base(dbPath))
	if original := findLocalOriginal(j.OriginalSQL, filepath.Base(dbPath)); original.Name != "" {
		if !matchesLocalInventory(mainBackup, original) {
			return fmt.Errorf("Local SQLite original backup is unavailable")
		}
	} else if _, err := os.Lstat(dbPath); err == nil {
		length, hash, hashErr := hashLocalRestoreFile(dbPath)
		if hashErr != nil || length != j.Files[0].Length || hash != j.Files[0].SHA256 {
			return fmt.Errorf("unexpected Local SQLite target before restore publication")
		}
	}
	if err := os.Remove(dbPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := localRestoreRename(filepath.Join(stage, "sqlite.db"), dbPath); err != nil {
		return err
	}
	if err := localRestoreSyncDir(stage); err != nil {
		return err
	}
	if err := localRestoreSyncDir(dir); err != nil {
		return err
	}
	j.Phase = "published"
	if err := writeLocalRestoreJournal(dbPath, *j); err != nil {
		return err
	}
	if err := hitLocalRestoreBoundary("after-publish-sql"); err != nil {
		return err
	}
	return nil
}

func matchesLocalInventory(path string, want localRestoreOriginal) bool {
	length, hash, err := hashLocalRestoreFile(path)
	return err == nil && length == want.Length && hash == want.SHA256
}

func findLocalOriginal(entries []localRestoreOriginal, name string) localRestoreOriginal {
	for _, entry := range entries {
		if entry.Name == name {
			return entry
		}
	}
	return localRestoreOriginal{}
}

func verifyLocalDirectoryInventory(path string, want []localRestoreOriginal) error {
	got, exists, err := inspectLocalOriginalGraph(path)
	if err != nil {
		return err
	}
	if !exists || len(got) != len(want) {
		return fmt.Errorf("Local graph backup does not match restore inventory")
	}
	for i := range want {
		if got[i] != want[i] {
			return fmt.Errorf("Local graph backup does not match restore inventory")
		}
	}
	return nil
}
