package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

type haRestoreStageIndex struct {
	Version             uint                      `json:"version"`
	SourcePrefix        string                    `json:"source_prefix"`
	ArchiveConfigID     uint                      `json:"archive_config_id"`
	BootstrapConfigID   uint                      `json:"bootstrap_config_id"`
	BootstrapMembership recovery.MembershipRecord `json:"bootstrap_membership"`
	Tip                 uint64                    `json:"tip"`
	PrefixHash          string                    `json:"prefix_hash"`
	AnchorHash          string                    `json:"anchor_hash,omitempty"`
	HasCheckpoint       bool                      `json:"has_checkpoint"`
	Index               uint64                    `json:"index"`
	RootHash            string                    `json:"root_hash"`
	StateHash           string                    `json:"state_hash"`
	Files               []haRestoreFile           `json:"files"`
}

type haRestoreFile struct {
	Role   string `json:"role"`
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

const haRestoreStageIndexMaxBytes int64 = 64 << 10

// stageHACheckpointFiles downloads and verifies precisely the selected native
// checkpoint into a new directory. The published directory appears in one
// rename only after every file and the local index are durable. It neither
// opens DataDir nor touches qlog, voter identity, networking, or authority.
// The caller must independently validate selection against HA evidence and
// must arrange any required archive/root retention while this read runs.
func stageHACheckpointFiles(ctx context.Context, bucket objstore.Bucket, selection recovery.HAArchiveRestoreSelection, stageDir string) error {
	if bucket == nil || selection.SourcePrefix == "" || selection.ArchiveConfigID == 0 || selection.BootstrapConfigID == 0 ||
		selection.BootstrapMembership.Version != recovery.MembershipRecordVersion ||
		selection.Tip == 0 || selection.PrefixHash == ([32]byte{}) || stageDir == "" {
		return fmt.Errorf("incomplete HA checkpoint restore selection")
	}
	if selection.HasCheckpoint {
		if selection.CheckpointIndex == 0 || selection.CheckpointIndex > uint64(selection.Tip) ||
			selection.CheckpointRootHash == ([32]byte{}) || selection.CheckpointStateHash == ([32]byte{}) {
			return fmt.Errorf("incomplete HA checkpoint root selection")
		}
	} else if selection.CheckpointIndex != 0 || selection.CheckpointRootHash != ([32]byte{}) || selection.CheckpointStateHash != ([32]byte{}) {
		return fmt.Errorf("checkpointless HA restore selection contains a root")
	}

	finalDir, err := filepath.Abs(stageDir)
	if err != nil {
		return fmt.Errorf("resolve HA checkpoint stage directory: %w", err)
	}
	parent := filepath.Dir(finalDir)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return fmt.Errorf("create HA checkpoint stage parent: %w", err)
	}
	if _, err := os.Lstat(finalDir); err == nil {
		return validateExistingHAStage(ctx, bucket, selection, finalDir)
	} else if !os.IsNotExist(err) {
		return err
	}

	workDir, err := os.MkdirTemp(parent, ".ha-restore-stage-*")
	if err != nil {
		return fmt.Errorf("create temporary HA checkpoint stage: %w", err)
	}
	defer os.RemoveAll(workDir)
	if err := os.Chmod(workDir, 0o700); err != nil {
		return err
	}

	index := haRestoreStageIndex{
		Version: 1, SourcePrefix: selection.SourcePrefix, ArchiveConfigID: selection.ArchiveConfigID,
		BootstrapConfigID: selection.BootstrapConfigID, Tip: uint64(selection.Tip),
		BootstrapMembership: selection.BootstrapMembership,
		PrefixHash:          hex.EncodeToString(selection.PrefixHash[:]), AnchorHash: hex.EncodeToString(selection.AnchorHash[:]),
		HasCheckpoint: selection.HasCheckpoint,
	}
	if selection.HasCheckpoint {
		index.Index = selection.CheckpointIndex
		index.RootHash = hex.EncodeToString(selection.CheckpointRootHash[:])
		index.StateHash = hex.EncodeToString(selection.CheckpointStateHash[:])
		manager := checkpoint.NewManager(bucket, selection.SourcePrefix, "", selection.ArchiveConfigID)
		root, err := manager.OpenRoot(ctx, selection.CheckpointIndex, selection.CheckpointRootHash)
		if err != nil {
			return fmt.Errorf("open selected HA checkpoint root: %w", err)
		}
		if root.Hash != selection.CheckpointStateHash {
			return fmt.Errorf("selected HA checkpoint state hash mismatch")
		}
		downloadDir := filepath.Join(workDir, "files")
		if err := os.Mkdir(downloadDir, 0o700); err != nil {
			return err
		}
		files, err := manager.DownloadAndVerifyRootFiles(ctx, root, downloadDir)
		if err != nil {
			return fmt.Errorf("download selected HA checkpoint files: %w", err)
		}
		if err := validateHAStageFiles(files); err != nil {
			return err
		}
		index.Files = make([]haRestoreFile, 0, len(files))
		for _, file := range files {
			name := filepath.Base(file.Path)
			if name == "." || name == string(filepath.Separator) || strings.Contains(name, string(filepath.Separator)) {
				return fmt.Errorf("invalid HA checkpoint file name")
			}
			digest, size, err := hashHAStageFile(file.Path)
			if err != nil {
				return fmt.Errorf("hash staged HA checkpoint file %q: %w", file.Role, err)
			}
			if size != file.Size {
				return fmt.Errorf("staged HA checkpoint file %q has size %d, root declares %d", file.Role, size, file.Size)
			}
			index.Files = append(index.Files, haRestoreFile{
				Role: file.Role, Name: filepath.Join("files", name), Size: file.Size, SHA256: digest,
			})
		}
	}
	if err := writeHAStageIndex(workDir, index); err != nil {
		return err
	}
	if selection.HasCheckpoint {
		if err := syncDirectory(filepath.Join(workDir, "files")); err != nil {
			return fmt.Errorf("sync staged checkpoint files: %w", err)
		}
	}
	if err := os.Rename(workDir, finalDir); err != nil {
		return fmt.Errorf("publish HA checkpoint stage: %w", err)
	}
	if err := syncDirectory(parent); err != nil {
		return fmt.Errorf("sync HA checkpoint stage parent: %w", err)
	}
	return nil
}

func validateExistingHAStage(ctx context.Context, bucket objstore.Bucket, selection recovery.HAArchiveRestoreSelection, stageDir string) error {
	index, exists, err := readHAStageIndex(stageDir)
	if err != nil || !exists {
		if err == nil {
			err = fmt.Errorf("existing HA stage is empty")
		}
		return err
	}
	want := haRestoreStageIndex{
		Version: 1, SourcePrefix: selection.SourcePrefix, ArchiveConfigID: selection.ArchiveConfigID,
		BootstrapConfigID: selection.BootstrapConfigID, Tip: uint64(selection.Tip),
		BootstrapMembership: selection.BootstrapMembership,
		PrefixHash:          hex.EncodeToString(selection.PrefixHash[:]), AnchorHash: hex.EncodeToString(selection.AnchorHash[:]),
		HasCheckpoint: selection.HasCheckpoint,
	}
	if selection.HasCheckpoint {
		want.Index = selection.CheckpointIndex
		want.RootHash = hex.EncodeToString(selection.CheckpointRootHash[:])
		want.StateHash = hex.EncodeToString(selection.CheckpointStateHash[:])
	}
	if index.Version != want.Version || index.SourcePrefix != want.SourcePrefix || index.ArchiveConfigID != want.ArchiveConfigID ||
		index.BootstrapConfigID != want.BootstrapConfigID || index.Tip != want.Tip || index.PrefixHash != want.PrefixHash ||
		index.BootstrapMembership != want.BootstrapMembership ||
		index.AnchorHash != want.AnchorHash || index.HasCheckpoint != want.HasCheckpoint || index.Index != want.Index ||
		index.RootHash != want.RootHash || index.StateHash != want.StateHash {
		return fmt.Errorf("existing HA stage does not match verified selection")
	}
	if !selection.HasCheckpoint {
		if len(index.Files) != 0 {
			return fmt.Errorf("checkpointless HA stage contains files")
		}
		return nil
	}
	manager := checkpoint.NewManager(bucket, selection.SourcePrefix, "", selection.ArchiveConfigID)
	root, err := manager.OpenRoot(ctx, selection.CheckpointIndex, selection.CheckpointRootHash)
	if err != nil || root.Hash != selection.CheckpointStateHash {
		return fmt.Errorf("existing HA stage root is unavailable or mismatched")
	}
	_, err = verifyHAStagedFilesAgainstRoot(stageDir, *index, root)
	return err
}

func resolveHAStagePath(dataDir, stageDir string) (string, error) {
	if dataDir == "" || stageDir == "" {
		return "", fmt.Errorf("HA init restore and DataDir paths are required")
	}
	dataAbs, err := filepath.Abs(dataDir)
	if err != nil {
		return "", err
	}
	stageAbs, err := filepath.Abs(stageDir)
	if err != nil {
		return "", err
	}
	if pathsOverlap(dataAbs, stageAbs) {
		return "", fmt.Errorf("HA init restore staging must be outside DataDir")
	}
	dataResolved, err := filepath.EvalSymlinks(dataAbs)
	if err != nil {
		return "", fmt.Errorf("resolve DataDir: %w", err)
	}
	stageParent, err := filepath.EvalSymlinks(filepath.Dir(stageAbs))
	if err != nil {
		return "", fmt.Errorf("resolve HA init restore parent: %w", err)
	}
	resolvedStage := filepath.Join(stageParent, filepath.Base(stageAbs))
	if info, err := os.Lstat(stageAbs); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("HA init restore stage path must not be a symlink")
		}
		resolvedStage, err = filepath.EvalSymlinks(stageAbs)
		if err != nil {
			return "", err
		}
	} else if !os.IsNotExist(err) {
		return "", err
	}
	if pathsOverlap(dataResolved, resolvedStage) {
		return "", fmt.Errorf("HA init restore staging resolves inside or over DataDir")
	}
	return stageAbs, nil
}

func pathsOverlap(a, b string) bool {
	rel, err := filepath.Rel(a, b)
	if err == nil && (rel == "." || rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))) {
		return true
	}
	rel, err = filepath.Rel(b, a)
	return err == nil && (rel == "." || rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

func readHAStageIndex(stageDir string) (*haRestoreStageIndex, bool, error) {
	info, err := os.Lstat(stageDir)
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, false, fmt.Errorf("HA init restore stage is not a real directory")
	}
	entries, err := os.ReadDir(stageDir)
	if err != nil {
		return nil, false, err
	}
	if len(entries) == 0 {
		return nil, false, nil
	}
	if len(entries) < 1 || len(entries) > 2 {
		return nil, false, fmt.Errorf("HA init restore stage has unexpected entries")
	}
	hasIndex, hasFiles := false, false
	for _, entry := range entries {
		switch entry.Name() {
		case "index.json":
			hasIndex = true
		case "files":
			hasFiles = true
		default:
			return nil, false, fmt.Errorf("HA init restore stage has unexpected entry %q", entry.Name())
		}
	}
	if !hasIndex {
		return nil, false, fmt.Errorf("HA init restore stage is missing index.json")
	}
	indexPath := filepath.Join(stageDir, "index.json")
	indexInfo, err := os.Lstat(indexPath)
	if err != nil || !indexInfo.Mode().IsRegular() || indexInfo.Mode()&os.ModeSymlink != 0 || indexInfo.Size() < 0 || indexInfo.Size() > haRestoreStageIndexMaxBytes {
		return nil, false, fmt.Errorf("HA init restore stage index is unsafe")
	}
	indexFile, err := os.Open(indexPath)
	if err != nil {
		return nil, false, err
	}
	defer indexFile.Close()
	openedInfo, err := indexFile.Stat()
	if err != nil || !openedInfo.Mode().IsRegular() || !os.SameFile(indexInfo, openedInfo) || openedInfo.Size() < 0 || openedInfo.Size() > haRestoreStageIndexMaxBytes {
		return nil, false, fmt.Errorf("HA init restore stage index changed or is unsafe")
	}
	data, err := io.ReadAll(io.LimitReader(indexFile, haRestoreStageIndexMaxBytes+1))
	if err != nil {
		return nil, false, err
	}
	if int64(len(data)) > haRestoreStageIndexMaxBytes {
		return nil, false, fmt.Errorf("HA init restore stage index exceeds %d bytes", haRestoreStageIndexMaxBytes)
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	var index haRestoreStageIndex
	if err := decoder.Decode(&index); err != nil {
		return nil, false, fmt.Errorf("decode HA init restore stage index: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, false, fmt.Errorf("HA init restore stage index has trailing data")
	}
	if index.HasCheckpoint {
		if !hasFiles {
			return nil, false, fmt.Errorf("HA init restore checkpoint stage is missing files directory")
		}
		filesInfo, err := os.Lstat(filepath.Join(stageDir, "files"))
		if err != nil || !filesInfo.IsDir() || filesInfo.Mode()&os.ModeSymlink != 0 {
			return nil, false, fmt.Errorf("HA init restore staged files directory is unsafe")
		}
	} else if hasFiles || len(index.Files) != 0 || index.Index != 0 || index.RootHash != "" || index.StateHash != "" {
		return nil, false, fmt.Errorf("checkpointless HA init stage contains checkpoint files")
	}
	return &index, true, nil
}

func verifyHAStagedCheckpointFiles(stageDir string, index haRestoreStageIndex, expectedPrefix string, archiveConfigID, bootstrapConfigID uint, root *checkpoint.Checkpoint, core *quepaxa.Core, currentArchiveTip quepaxa.Slot, expectedAnchor [32]byte) ([]materializer.CheckpointFile, error) {
	if root == nil || core == nil || expectedPrefix == "" || archiveConfigID == 0 || bootstrapConfigID == 0 {
		return nil, fmt.Errorf("HA init restore evidence is incomplete")
	}
	if !index.HasCheckpoint || index.Version != 1 || index.SourcePrefix != expectedPrefix || index.ArchiveConfigID != archiveConfigID ||
		index.BootstrapConfigID != bootstrapConfigID || root.ConfigID != archiveConfigID ||
		index.Tip > uint64(currentArchiveTip) || index.Tip < index.Index || index.Index != root.Index ||
		index.RootHash != hex.EncodeToString(root.RootHash[:]) || index.StateHash != hex.EncodeToString(root.Hash[:]) ||
		index.AnchorHash != hex.EncodeToString(expectedAnchor[:]) {
		return nil, fmt.Errorf("HA init restore index does not match current certified checkpoint")
	}
	prefixHash, err := decodeStageHash(index.PrefixHash)
	if err != nil {
		return nil, fmt.Errorf("invalid staged HA archive prefix hash: %w", err)
	}
	observedPrefix, ok := core.PrefixHash(quepaxa.Slot(index.Tip))
	if !ok || observedPrefix != prefixHash {
		return nil, fmt.Errorf("HA init restore source prefix differs from current certified history")
	}
	if actualAnchor := core.GenerationAnchorHash(); actualAnchor != expectedAnchor {
		return nil, fmt.Errorf("HA init restore generation anchor differs from current core")
	}
	return verifyHAStagedFilesAgainstRoot(stageDir, index, root)
}

func verifyHAStagedHistoryOnly(index haRestoreStageIndex, expectedPrefix string, archiveConfigID, bootstrapConfigID uint, core *quepaxa.Core, currentArchiveTip quepaxa.Slot, expectedAnchor [32]byte) error {
	if core == nil || expectedPrefix == "" || archiveConfigID == 0 || bootstrapConfigID == 0 || index.HasCheckpoint ||
		index.Version != 1 || index.SourcePrefix != expectedPrefix || index.ArchiveConfigID != archiveConfigID ||
		index.BootstrapConfigID != bootstrapConfigID || len(index.Files) != 0 || index.Index != 0 || index.RootHash != "" || index.StateHash != "" ||
		index.Tip == 0 || index.Tip > uint64(currentArchiveTip) || index.AnchorHash != hex.EncodeToString(expectedAnchor[:]) {
		return fmt.Errorf("checkpointless HA init evidence does not match current archive")
	}
	prefixHash, err := decodeStageHash(index.PrefixHash)
	if err != nil {
		return fmt.Errorf("invalid staged HA archive prefix hash: %w", err)
	}
	observedPrefix, ok := core.PrefixHash(quepaxa.Slot(index.Tip))
	if !ok || observedPrefix != prefixHash {
		return fmt.Errorf("checkpointless HA init source prefix differs from current certified history")
	}
	if core.GenerationAnchorHash() != expectedAnchor {
		return fmt.Errorf("checkpointless HA init generation anchor differs from current core")
	}
	return nil
}

func verifyHAStagedFilesAgainstRoot(stageDir string, index haRestoreStageIndex, root *checkpoint.Checkpoint) ([]materializer.CheckpointFile, error) {
	if root == nil || !index.HasCheckpoint || index.Index != root.Index || index.RootHash != hex.EncodeToString(root.RootHash[:]) || index.StateHash != hex.EncodeToString(root.Hash[:]) {
		return nil, fmt.Errorf("HA init restore stage does not identify the selected checkpoint")
	}
	if err := validateHAStageFiles(root.Files); err != nil {
		return nil, err
	}
	if len(index.Files) != len(root.Files) {
		return nil, fmt.Errorf("HA init restore file set differs from certified root")
	}
	byRole := make(map[string]haRestoreFile, len(index.Files))
	for _, file := range index.Files {
		if file.Role == "" || byRole[file.Role].Role != "" {
			return nil, fmt.Errorf("HA init restore index has duplicate file roles")
		}
		byRole[file.Role] = file
	}
	materialFiles := make([]materializer.CheckpointFile, 0, len(root.Files))
	for _, file := range root.Files {
		staged, ok := byRole[file.Role]
		name := ""
		switch file.Role {
		case checkpoint.RoleSQLite:
			name = "sqlite.db"
		case checkpoint.RoleGraphData:
			name = "graph-data.db"
		default:
			return nil, fmt.Errorf("unsupported certified HA checkpoint role %q", file.Role)
		}
		if !ok || staged.Name != filepath.Join("files", name) || staged.Size != file.Size || staged.SHA256 == "" {
			return nil, fmt.Errorf("HA init restore file metadata differs from certified root")
		}
		filePath := filepath.Join(stageDir, staged.Name)
		info, err := os.Lstat(filePath)
		if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() != staged.Size {
			return nil, fmt.Errorf("HA init restore checkpoint file is missing or unsafe")
		}
		if err := verifyHAStageFileAgainstDescriptor(filePath, file, staged.SHA256); err != nil {
			return nil, fmt.Errorf("HA init restore checkpoint file differs from certified root: %w", err)
		}
		materialFiles = append(materialFiles, materializer.CheckpointFile{Role: materializer.CheckpointRole(file.Role), Path: filePath, ExpectedLength: uint64(file.Size), ExpectedSHA256: file.Hash})
	}
	return materialFiles, nil
}

func decodeStageHash(value string) ([32]byte, error) {
	var hash [32]byte
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != len(hash) {
		return hash, fmt.Errorf("expected 32-byte hash")
	}
	copy(hash[:], decoded)
	return hash, nil
}

func hashFromStage(value string) [32]byte {
	hash, _ := decodeStageHash(value)
	return hash
}

// validateHAStageOnlyQlog is called after qlog.Acquire and before opening the
// materializer. Identity/WAL details are checked separately; any other entry
// means this is not a fresh DataDir that may adopt an init-stage checkpoint.
func validateHAStageOnlyQlog(dataDir string) error {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return err
	}
	if len(entries) != 1 || entries[0].Name() != "qlog" || !entries[0].IsDir() {
		return fmt.Errorf("DataDir contains state beyond the fresh qlog directory")
	}
	return nil
}

func hashHAStageFile(name string) (string, int64, error) {
	file, err := os.Open(name)
	if err != nil {
		return "", 0, err
	}
	defer file.Close()
	hash := sha256.New()
	size, err := io.Copy(hash, file)
	if err != nil {
		return "", size, err
	}
	return hex.EncodeToString(hash.Sum(nil)), size, nil
}

func verifyHAStageFileAgainstDescriptor(name string, descriptor checkpoint.File, expectedSHA256 string) error {
	file, err := os.Open(name)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != descriptor.Size {
		return fmt.Errorf("staged file size or type differs from checkpoint root")
	}
	whole := sha256.New()
	var total int64
	for _, block := range descriptor.Blocks {
		want, err := hex.DecodeString(block.Hash)
		if err != nil || len(want) != sha256.Size || block.Size <= 0 {
			return fmt.Errorf("checkpoint root has an invalid block descriptor")
		}
		blockHash := sha256.New()
		read, err := io.CopyN(io.MultiWriter(whole, blockHash), file, block.Size)
		if err != nil || read != block.Size || !strings.EqualFold(hex.EncodeToString(blockHash.Sum(nil)), block.Hash) {
			return fmt.Errorf("staged file block differs from checkpoint root")
		}
		total += read
	}
	var extra [1]byte
	if count, err := file.Read(extra[:]); count != 0 || err != io.EOF || total != descriptor.Size {
		return fmt.Errorf("staged file length differs from checkpoint root")
	}
	if !strings.EqualFold(hex.EncodeToString(whole.Sum(nil)), expectedSHA256) {
		return fmt.Errorf("staged file checksum differs from staging index")
	}
	return nil
}

func validateHAStageFiles(files []checkpoint.File) error {
	seen := make(map[string]bool, len(files))
	for _, file := range files {
		// Root descriptors intentionally omit Path; downloaded files carry it.
		// The stage assigns fixed local names from Role and verifies the bytes.
		if file.Size <= 0 || file.Hash == "" || seen[file.Role] {
			return fmt.Errorf("invalid or duplicate HA checkpoint file")
		}
		if file.Role != checkpoint.RoleSQLite && file.Role != checkpoint.RoleGraphData {
			return fmt.Errorf("unsupported HA checkpoint file role %q", file.Role)
		}
		seen[file.Role] = true
	}
	if !seen[checkpoint.RoleSQLite] {
		return fmt.Errorf("HA checkpoint is missing its SQLite materializer")
	}
	return nil
}

func writeHAStageIndex(dir string, index haRestoreStageIndex) error {
	data, err := json.Marshal(index)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".ha-restore-index-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, filepath.Join(dir, "index.json")); err != nil {
		return err
	}
	return syncDirectory(dir)
}

func syncDirectory(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}
