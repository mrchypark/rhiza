package node

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
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

const (
	localCheckpointVersion  = 1
	localCheckpointMaxBytes = uint64(32 << 30)
	localDescriptorMaxBytes = 16 << 10
)

// localNodeCheckpointCrashBoundary is a package-private subprocess-test seam.
// Production leaves it nil; it marks durable root publication before Core
// appends the prepared-checkpoint record.
var localNodeCheckpointCrashBoundary func(string)

// localRestoreSuffixCrashBoundary is an opt-in subprocess-test seam used to
// stop after an individual suffix decision has been applied.
var localRestoreSuffixCrashBoundary func(uint64)

// Narrow syscall seams for deterministic Local snapshot publication fault tests.
var (
	localCheckpointWriteFile = func(file *os.File, data []byte) (int, error) { return file.Write(data) }
	localCheckpointSyncFile  = func(file *os.File) error { return file.Sync() }
	localCheckpointSyncDir   = func(dir string) error { return syncLocalDirOS(dir) }
	localCheckpointRename    = os.Rename
)

func hitLocalNodeCheckpointCrashBoundary(name string) {
	if localNodeCheckpointCrashBoundary != nil {
		localNodeCheckpointCrashBoundary(name)
	}
}

var (
	localRootDomain  = []byte("rhiza/local-checkpoint-root/v1\x00")
	localStateDomain = []byte("rhiza/local-checkpoint-state/v1\x00")
)

type localStorePolicy struct {
	Version      uint   `json:"version"`
	StoreUUID    string `json:"store_uuid"`
	ConfigDigest string `json:"config_digest"`
}

type localFileDescriptor struct {
	Role   string `json:"role"`
	Name   string `json:"name"`
	Length uint64 `json:"length"`
	SHA256 string `json:"sha256"`
}

type localCheckpointDescriptor struct {
	Version         uint                  `json:"version"`
	StoreUUID       string                `json:"store_uuid"`
	ConfigDigest    string                `json:"config_digest"`
	ConfigID        uint                  `json:"config_id"`
	Slot            uint64                `json:"slot"`
	Prefix          string                `json:"prefix"`
	ExecutionPolicy string                `json:"execution_policy"`
	Files           []localFileDescriptor `json:"files"`
	StateHash       string                `json:"state_hash"`
	RootHash        string                `json:"root_hash"`
}

type localCheckpointIdentity struct {
	policy       localStorePolicy
	configDigest string
	rootDir      string
}

func openLocalCheckpointIdentity(config *types.ExecutionConfig) (localCheckpointIdentity, error) {
	configJSON, err := json.Marshal(struct {
		Version uint                           `json:"version"`
		Cluster string                         `json:"cluster"`
		Node    string                         `json:"node"`
		Local   bool                           `json:"local"`
		Indexes []types.GraphNodePropertyIndex `json:"indexes"`
	}{localCheckpointVersion, string(config.ClusterID), string(config.NodeID), config.Local, config.LocalGraphNodePropertyIndexes})
	if err != nil {
		return localCheckpointIdentity{}, err
	}
	digest := sha256.Sum256(append([]byte("rhiza/local-store-config/v1\x00"), configJSON...))
	configDigest := hex.EncodeToString(digest[:])
	path := filepath.Join(config.DataDir, "local-store.json")
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() > 4096 {
			return localCheckpointIdentity{}, fmt.Errorf("invalid Local store policy file")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return localCheckpointIdentity{}, err
		}
		var policy localStorePolicy
		if err := decodeLocalJSON(data, &policy); err != nil || policy.Version != localCheckpointVersion || len(policy.StoreUUID) != 32 || policy.ConfigDigest != configDigest {
			return localCheckpointIdentity{}, fmt.Errorf("Local store policy does not match configured identity; preserve existing data")
		}
		if _, err := hex.DecodeString(policy.StoreUUID); err != nil {
			return localCheckpointIdentity{}, fmt.Errorf("Local store UUID is invalid")
		}
		if noWALManifest(config.DataDir) && localMaterializedDataExists(config.DataDir) {
			return localCheckpointIdentity{}, fmt.Errorf("Local policy exists but its WAL manifest is missing; preserve existing data")
		}
		return localCheckpointIdentity{policy: policy, configDigest: configDigest, rootDir: filepath.Join(config.DataDir, "local-checkpoints")}, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return localCheckpointIdentity{}, err
	}
	if localDataExists(config.DataDir) {
		return localCheckpointIdentity{}, fmt.Errorf("Local data has no store policy; preserve existing data (migration is unsupported)")
	}
	var uuid [16]byte
	if _, err := rand.Read(uuid[:]); err != nil {
		return localCheckpointIdentity{}, err
	}
	policy := localStorePolicy{Version: localCheckpointVersion, StoreUUID: hex.EncodeToString(uuid[:]), ConfigDigest: configDigest}
	data, err := json.Marshal(policy)
	if err != nil {
		return localCheckpointIdentity{}, err
	}
	if err := atomicLocalFile(config.DataDir, path, data); err != nil {
		return localCheckpointIdentity{}, err
	}
	return localCheckpointIdentity{policy: policy, configDigest: configDigest, rootDir: filepath.Join(config.DataDir, "local-checkpoints")}, nil
}

func localDataExists(dataDir string) bool {
	if localMaterializedDataExists(dataDir) {
		return true
	}
	segments, _ := filepath.Glob(filepath.Join(dataDir, "qlog", "seg_*.log"))
	for _, path := range segments {
		if info, err := os.Lstat(path); err == nil && info.Size() != 0 {
			return true
		}
	}
	manifests, _ := filepath.Glob(filepath.Join(dataDir, "qlog", "manifest_*.bin"))
	return len(manifests) != 0 || len(segments) != 0
}

func localMaterializedDataExists(dataDir string) bool {
	for _, path := range []string{filepath.Join(dataDir, "sqlite.db"), filepath.Join(dataDir, "latticedb"), filepath.Join(dataDir, "local-checkpoints")} {
		if info, err := os.Lstat(path); err == nil && (info.Size() != 0 || info.IsDir()) {
			if info.IsDir() {
				entries, readErr := os.ReadDir(path)
				if readErr == nil && len(entries) == 0 {
					continue
				}
			}
			return true
		}
	}
	return false
}

func noWALManifest(dataDir string) bool {
	manifests, _ := filepath.Glob(filepath.Join(dataDir, "qlog", "manifest_*.bin"))
	return len(manifests) == 0
}

func (identity localCheckpointIdentity) ensureRootDir() error {
	if err := os.MkdirAll(identity.rootDir, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(identity.rootDir)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("Local checkpoint root is not a managed directory")
	}
	return nil
}

func (identity localCheckpointIdentity) Publish(ctx context.Context, files []materializer.CheckpointFile, slot uint64, prefix [32]byte, configIDs ...uint) (localCheckpointDescriptor, error) {
	if slot == 0 || prefix == ([32]byte{}) || len(files) != 2 {
		return localCheckpointDescriptor{}, fmt.Errorf("invalid Local checkpoint input")
	}
	if err := identity.ensureRootDir(); err != nil {
		return localCheckpointDescriptor{}, err
	}
	tmp, err := os.MkdirTemp(identity.rootDir, ".capture-")
	if err != nil {
		return localCheckpointDescriptor{}, err
	}
	defer os.RemoveAll(tmp)
	roles := map[materializer.CheckpointRole]localFileDescriptor{
		materializer.CheckpointSQLite:    {Role: string(materializer.CheckpointSQLite), Name: "sqlite.db"},
		materializer.CheckpointGraphData: {Role: string(materializer.CheckpointGraphData), Name: "graph.ltdb"},
	}
	var total uint64
	ordered := make([]localFileDescriptor, 0, 2)
	for _, file := range files {
		fd, ok := roles[file.Role]
		if !ok || fd.Length != 0 || file.Path == "" {
			return localCheckpointDescriptor{}, fmt.Errorf("invalid Local checkpoint file roles")
		}
		fd.Length, fd.SHA256, err = copyAndHashLocalFile(ctx, file.Path, filepath.Join(tmp, fd.Name), localCheckpointMaxBytes-total)
		if err != nil {
			return localCheckpointDescriptor{}, err
		}
		if fd.Length == 0 {
			return localCheckpointDescriptor{}, fmt.Errorf("Local checkpoint role %s is empty", fd.Role)
		}
		total += fd.Length
		roles[file.Role] = fd
	}
	for _, role := range []materializer.CheckpointRole{materializer.CheckpointSQLite, materializer.CheckpointGraphData} {
		fd := roles[role]
		if fd.Length == 0 {
			return localCheckpointDescriptor{}, fmt.Errorf("Local checkpoint is missing %s", role)
		}
		ordered = append(ordered, fd)
	}
	var configID uint
	if len(configIDs) > 1 {
		return localCheckpointDescriptor{}, fmt.Errorf("invalid Local checkpoint configuration authority")
	}
	if len(configIDs) == 1 {
		configID = configIDs[0]
	}
	base := localCheckpointDescriptor{Version: localCheckpointVersion, StoreUUID: identity.policy.StoreUUID, ConfigDigest: identity.configDigest, ConfigID: configID, Slot: slot, Prefix: hex.EncodeToString(prefix[:]), ExecutionPolicy: "sql+graph/v1", Files: ordered}
	base.StateHash = localStateHash(ordered)
	base.RootHash, err = localDescriptorRoot(base)
	if err != nil {
		return localCheckpointDescriptor{}, err
	}
	descriptor, err := json.Marshal(base)
	if err != nil || len(descriptor) > localDescriptorMaxBytes {
		return localCheckpointDescriptor{}, fmt.Errorf("Local checkpoint descriptor exceeds its bound")
	}
	if err := writeSyncedExclusive(filepath.Join(tmp, "descriptor.json"), descriptor); err != nil {
		return localCheckpointDescriptor{}, err
	}
	if err := syncLocalDir(tmp); err != nil {
		return localCheckpointDescriptor{}, err
	}
	root := filepath.Join(identity.rootDir, base.RootHash)
	if _, err := os.Lstat(root); err == nil {
		existing, _, verifyErr := identity.OpenExact(mustLocalHash(base.RootHash), slot, prefix)
		if verifyErr != nil || existing.RootHash != base.RootHash || existing.StateHash != base.StateHash {
			if verifyErr == nil {
				verifyErr = fmt.Errorf("existing Local checkpoint root differs")
			}
			return localCheckpointDescriptor{}, fmt.Errorf("Local checkpoint root already exists and cannot be reused: %w", verifyErr)
		}
		return existing, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return localCheckpointDescriptor{}, err
	}
	if err := localCheckpointRename(tmp, root); err != nil {
		return localCheckpointDescriptor{}, err
	}
	if err := syncLocalDir(identity.rootDir); err != nil {
		return localCheckpointDescriptor{}, err
	}
	return base, nil
}

func mustLocalHash(value string) [32]byte {
	var output [32]byte
	decoded, _ := hex.DecodeString(value)
	copy(output[:], decoded)
	return output
}

func (identity localCheckpointIdentity) OpenExact(rootHash [32]byte, slot uint64, prefix [32]byte) (localCheckpointDescriptor, []materializer.CheckpointFile, error) {
	if rootHash == ([32]byte{}) || slot == 0 || prefix == ([32]byte{}) {
		return localCheckpointDescriptor{}, nil, fmt.Errorf("invalid Local checkpoint root request")
	}
	rootInfo, err := os.Lstat(identity.rootDir)
	if err != nil {
		return localCheckpointDescriptor{}, nil, err
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return localCheckpointDescriptor{}, nil, fmt.Errorf("Local checkpoint root is not a managed directory")
	}
	dir := filepath.Join(identity.rootDir, hex.EncodeToString(rootHash[:]))
	info, err := os.Lstat(dir)
	if err != nil {
		return localCheckpointDescriptor{}, nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return localCheckpointDescriptor{}, nil, fmt.Errorf("Local checkpoint root is not a managed directory")
	}
	contents, err := os.ReadDir(dir)
	if err != nil || len(contents) != 3 {
		names := make([]string, 0, len(contents))
		for _, entry := range contents {
			names = append(names, entry.Name())
		}
		return localCheckpointDescriptor{}, nil, fmt.Errorf("Local checkpoint root has unexpected contents %v: %v", names, err)
	}
	for _, entry := range contents {
		if entry.Type()&os.ModeSymlink != 0 || (entry.Name() != "descriptor.json" && entry.Name() != "sqlite.db" && entry.Name() != "graph.ltdb") {
			return localCheckpointDescriptor{}, nil, fmt.Errorf("Local checkpoint root has unexpected entry %q", entry.Name())
		}
	}
	path := filepath.Join(dir, "descriptor.json")
	info, err = os.Lstat(path)
	if err != nil {
		return localCheckpointDescriptor{}, nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() > localDescriptorMaxBytes {
		return localCheckpointDescriptor{}, nil, fmt.Errorf("invalid Local checkpoint descriptor file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return localCheckpointDescriptor{}, nil, err
	}
	var descriptor localCheckpointDescriptor
	if err := decodeLocalJSON(data, &descriptor); err != nil {
		return localCheckpointDescriptor{}, nil, err
	}
	if descriptor.Version != localCheckpointVersion || descriptor.StoreUUID != identity.policy.StoreUUID || descriptor.ConfigDigest != identity.configDigest || descriptor.Slot != slot || descriptor.Prefix != hex.EncodeToString(prefix[:]) || descriptor.ExecutionPolicy != "sql+graph/v1" || descriptor.RootHash != hex.EncodeToString(rootHash[:]) || localStateHash(descriptor.Files) != descriptor.StateHash {
		return localCheckpointDescriptor{}, nil, fmt.Errorf("Local checkpoint descriptor does not match the exact recovered root")
	}
	computed, err := localDescriptorRoot(descriptor)
	if err != nil || computed != descriptor.RootHash {
		return localCheckpointDescriptor{}, nil, fmt.Errorf("Local checkpoint root hash mismatch")
	}
	if len(descriptor.Files) != 2 || descriptor.Files[0].Role != string(materializer.CheckpointSQLite) || descriptor.Files[0].Name != "sqlite.db" || descriptor.Files[1].Role != string(materializer.CheckpointGraphData) || descriptor.Files[1].Name != "graph.ltdb" {
		return localCheckpointDescriptor{}, nil, fmt.Errorf("Local checkpoint has unsupported fixed roles")
	}
	var total uint64
	files := make([]materializer.CheckpointFile, 0, 2)
	for _, fd := range descriptor.Files {
		if fd.Length == 0 || fd.Length > localCheckpointMaxBytes-total || filepath.Base(fd.Name) != fd.Name || strings.Contains(fd.Name, "..") {
			return localCheckpointDescriptor{}, nil, fmt.Errorf("invalid Local checkpoint file descriptor")
		}
		filePath := filepath.Join(dir, fd.Name)
		fileInfo, err := os.Lstat(filePath)
		if err != nil || fileInfo.Mode()&os.ModeSymlink != 0 || !fileInfo.Mode().IsRegular() || uint64(fileInfo.Size()) != fd.Length {
			return localCheckpointDescriptor{}, nil, fmt.Errorf("Local checkpoint file does not match descriptor")
		}
		hash, length, err := hashLocalFile(filePath, localCheckpointMaxBytes-total)
		if err != nil || length != fd.Length || hash != fd.SHA256 {
			return localCheckpointDescriptor{}, nil, fmt.Errorf("Local checkpoint file hash mismatch")
		}
		total += length
		role := materializer.CheckpointRole(fd.Role)
		files = append(files, materializer.CheckpointFile{Role: role, Path: filePath, ExpectedLength: fd.Length, ExpectedSHA256: fd.SHA256})
	}
	return descriptor, files, nil
}

func (identity localCheckpointIdentity) OpenExpected(expected quepaxa.LocalCheckpointRoot) (localCheckpointDescriptor, []materializer.CheckpointFile, error) {
	descriptor, files, err := identity.OpenExact(expected.RootHash, uint64(expected.Index), expected.PrefixHash)
	if err != nil {
		return localCheckpointDescriptor{}, nil, err
	}
	if descriptor.ConfigID != expected.ConfigID || expected.StateHash != ([32]byte{}) && descriptor.StateHash != hex.EncodeToString(expected.StateHash[:]) {
		return localCheckpointDescriptor{}, nil, fmt.Errorf("Local checkpoint descriptor does not match WAL configuration/state authority")
	}
	return descriptor, files, nil
}

func (identity localCheckpointIdentity) OpenRoot(rootHash [32]byte) (localCheckpointDescriptor, []materializer.CheckpointFile, error) {
	dir := filepath.Join(identity.rootDir, hex.EncodeToString(rootHash[:]))
	path := filepath.Join(dir, "descriptor.json")
	info, err := os.Lstat(path)
	if err != nil {
		return localCheckpointDescriptor{}, nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() > localDescriptorMaxBytes {
		return localCheckpointDescriptor{}, nil, fmt.Errorf("invalid Local checkpoint descriptor file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return localCheckpointDescriptor{}, nil, err
	}
	var descriptor localCheckpointDescriptor
	if err := decodeLocalJSON(data, &descriptor); err != nil {
		return localCheckpointDescriptor{}, nil, err
	}
	prefixBytes, err := hex.DecodeString(descriptor.Prefix)
	if err != nil || len(prefixBytes) != 32 {
		return localCheckpointDescriptor{}, nil, fmt.Errorf("invalid Local checkpoint prefix")
	}
	var prefix [32]byte
	copy(prefix[:], prefixBytes)
	return identity.OpenExact(rootHash, descriptor.Slot, prefix)
}

// Collect removes only fully validated, unreferenced roots. Any unknown entry,
// malformed descriptor, or missing live root aborts before the first deletion.
func (identity localCheckpointIdentity) Collect(roots []quepaxa.LocalCheckpointRoot) error {
	if err := identity.ensureRootDir(); err != nil {
		return err
	}
	entries, err := os.ReadDir(identity.rootDir)
	if err != nil {
		return err
	}
	live := make(map[string]struct{}, len(roots))
	rootByName := make(map[string]quepaxa.LocalCheckpointRoot, len(roots))
	for _, root := range roots {
		name := hex.EncodeToString(root.RootHash[:])
		live[name] = struct{}{}
		rootByName[name] = root
	}
	validated := make([]string, 0, len(entries))
	seenLive := make(map[string]struct{}, len(live))
	for _, entry := range entries {
		name := entry.Name()
		if len(name) != 64 || strings.ToLower(name) != name {
			return fmt.Errorf("unknown entry in managed Local checkpoint directory")
		}
		decoded, err := hex.DecodeString(name)
		if err != nil || len(decoded) != 32 || !entry.IsDir() {
			return fmt.Errorf("invalid managed Local checkpoint root entry")
		}
		var root [32]byte
		copy(root[:], decoded)
		path := filepath.Join(identity.rootDir, name, "descriptor.json")
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return fmt.Errorf("read Local checkpoint descriptor before GC: %w", readErr)
		}
		var descriptor localCheckpointDescriptor
		if err := decodeLocalJSON(data, &descriptor); err != nil {
			return err
		}
		prefixBytes, err := hex.DecodeString(descriptor.Prefix)
		if err != nil || len(prefixBytes) != 32 {
			return fmt.Errorf("invalid Local checkpoint prefix before GC")
		}
		var prefix [32]byte
		copy(prefix[:], prefixBytes)
		if _, _, err := identity.OpenExact(root, descriptor.Slot, prefix); err != nil {
			return fmt.Errorf("validate Local checkpoint before GC: %w", err)
		}
		if expected, ok := rootByName[name]; ok {
			if _, _, err := identity.OpenExpected(expected); err != nil {
				return fmt.Errorf("validate live Local checkpoint tuple before GC: %w", err)
			}
			seenLive[name] = struct{}{}
		}
		validated = append(validated, filepath.Join(identity.rootDir, name))
	}
	for root := range live {
		if _, ok := seenLive[root]; !ok {
			return fmt.Errorf("live Local checkpoint root %s is absent", root)
		}
	}
	for _, path := range validated {
		if _, ok := live[filepath.Base(path)]; ok {
			continue
		}
		if err := os.RemoveAll(path); err != nil {
			return fmt.Errorf("delete unreferenced Local checkpoint: %w", err)
		}
	}
	return syncLocalDir(identity.rootDir)
}

func (n *Node) validateLocalRoots(core *quepaxa.Core) error {
	if n.localStore == nil {
		return fmt.Errorf("Local checkpoint store is unavailable")
	}
	roots, err := core.LocalCheckpointRoots(context.Background())
	if err != nil {
		return fmt.Errorf("enumerate Local checkpoint roots: %w", err)
	}
	for _, root := range roots {
		descriptor, _, err := n.localStore.OpenExpected(root)
		if err != nil {
			return fmt.Errorf("validate every Local WAL checkpoint root: %w", err)
		}
		if n.localRoot == nil || descriptor.Slot >= n.localRoot.Slot {
			n.localRoot = &descriptor
		}
	}
	return nil
}

func localDescriptorRoot(descriptor localCheckpointDescriptor) (string, error) {
	descriptor.RootHash = ""
	descriptor.StateHash = ""
	data, err := json.Marshal(descriptor)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	_, _ = hash.Write(localRootDomain)
	_, _ = hash.Write(data)
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func localStateHash(files []localFileDescriptor) string {
	hash := sha256.New()
	_, _ = hash.Write(localStateDomain)
	for _, file := range files {
		_, _ = io.WriteString(hash, file.Role+"\x00"+file.Name+"\x00")
		var length [8]byte
		for i := range length {
			length[7-i] = byte(file.Length >> (i * 8))
		}
		_, _ = hash.Write(length[:])
		_, _ = io.WriteString(hash, file.SHA256+"\x00")
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func copyAndHashLocalFile(ctx context.Context, sourcePath, targetPath string, maxBytes uint64) (uint64, string, error) {
	sourceInfo, err := os.Lstat(sourcePath)
	if err != nil {
		return 0, "", err
	}
	if sourceInfo.Mode()&os.ModeSymlink != 0 || !sourceInfo.Mode().IsRegular() || uint64(sourceInfo.Size()) > maxBytes {
		return 0, "", fmt.Errorf("invalid or oversized Local checkpoint source")
	}
	source, err := os.Open(sourcePath)
	if err != nil {
		return 0, "", err
	}
	defer source.Close()
	target, err := os.OpenFile(targetPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return 0, "", err
	}
	hash := sha256.New()
	reader := io.LimitReader(source, int64(maxBytes)+1)
	count, copyErr := io.Copy(io.MultiWriter(localCheckpointFileWriter{file: target}, hash), contextReader{ctx: ctx, reader: reader})
	if copyErr == nil && uint64(count) > maxBytes {
		copyErr = fmt.Errorf("Local checkpoint exceeds %d bytes", maxBytes)
	}
	if copyErr == nil {
		copyErr = localCheckpointSyncFile(target)
	}
	if closeErr := target.Close(); copyErr == nil {
		copyErr = closeErr
	}
	if copyErr != nil {
		return 0, "", copyErr
	}
	return uint64(count), hex.EncodeToString(hash.Sum(nil)), nil
}

func hashLocalFile(path string, maxBytes uint64) (string, uint64, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer file.Close()
	hash := sha256.New()
	count, err := io.Copy(hash, io.LimitReader(file, int64(maxBytes)+1))
	if err != nil {
		return "", 0, err
	}
	if uint64(count) > maxBytes {
		return "", 0, fmt.Errorf("Local checkpoint file exceeds its bound")
	}
	return hex.EncodeToString(hash.Sum(nil)), uint64(count), nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(data []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(data)
}

func decodeLocalJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("trailing Local JSON data")
	}
	canonical, err := json.Marshal(target)
	if err != nil || !bytes.Equal(canonical, data) {
		return fmt.Errorf("Local JSON is not canonical")
	}
	return nil
}

func atomicLocalFile(dir, path string, data []byte) error {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(dir)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("Local store directory is not managed")
	}
	tmp, err := os.CreateTemp(dir, ".local-policy-")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if err := tmp.Chmod(0600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if _, err := os.Lstat(path); err == nil {
		return fmt.Errorf("Local store policy already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	return syncLocalDir(dir)
}

func writeSyncedExclusive(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	n, err := localCheckpointWriteFile(file, data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		file.Close()
		return err
	}
	if err := localCheckpointSyncFile(file); err != nil {
		file.Close()
		return err
	}
	return file.Close()
}

func syncLocalDir(path string) error {
	return localCheckpointSyncDir(path)
}

func syncLocalDirOS(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

type localCheckpointFileWriter struct{ file *os.File }

func (w localCheckpointFileWriter) Write(data []byte) (int, error) {
	return localCheckpointWriteFile(w.file, data)
}

func localFreeBytes(path string) (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, err
	}
	return uint64(stat.Bavail) * uint64(stat.Bsize), nil
}

func (identity localCheckpointIdentity) checkpointDiskUsage() (uint64, error) {
	info, err := os.Lstat(identity.rootDir)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return 0, fmt.Errorf("Local checkpoint root is not a managed directory")
	}
	var total uint64
	err = filepath.WalkDir(identity.rootDir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("Local checkpoint store contains a symlink")
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Size() < 0 {
			return fmt.Errorf("Local checkpoint store contains an unsupported file")
		}
		if total > ^uint64(0)-uint64(info.Size()) {
			return fmt.Errorf("Local checkpoint disk accounting overflow")
		}
		total += uint64(info.Size())
		return nil
	})
	return total, err
}

func (identity localCheckpointIdentity) requireFreeBytes(dataDir string, additional ...uint64) error {
	free, err := localFreeBytes(dataDir)
	if err != nil {
		return err
	}
	var required uint64
	for _, value := range additional {
		if required > ^uint64(0)-value {
			return fmt.Errorf("Local disk-space estimate overflow")
		}
		required += value
	}
	debt, err := identity.checkpointDiskUsage()
	if err != nil {
		return err
	}
	if required > ^uint64(0)-debt {
		return fmt.Errorf("Local disk-space estimate overflow")
	}
	required += debt
	if free < required {
		return fmt.Errorf("Local checkpoint needs an estimated %d free bytes including existing snapshot debt; %d available", required, free)
	}
	return nil
}

func sealFromLocalDescriptor(core *quepaxa.Core, descriptor localCheckpointDescriptor) (quepaxa.CheckpointSeal, error) {
	rootBytes, err := hex.DecodeString(descriptor.RootHash)
	if err != nil || len(rootBytes) != 32 {
		return quepaxa.CheckpointSeal{}, fmt.Errorf("invalid Local checkpoint root hash")
	}
	var root [32]byte
	copy(root[:], rootBytes)
	stateBytes, err := hex.DecodeString(descriptor.StateHash)
	if err != nil || len(stateBytes) != 32 {
		return quepaxa.CheckpointSeal{}, fmt.Errorf("invalid Local checkpoint state hash")
	}
	var state [32]byte
	copy(state[:], stateBytes)
	prefixBytes, err := hex.DecodeString(descriptor.Prefix)
	if err != nil || len(prefixBytes) != 32 {
		return quepaxa.CheckpointSeal{}, fmt.Errorf("invalid Local checkpoint prefix")
	}
	var prefix [32]byte
	copy(prefix[:], prefixBytes)
	index := quepaxa.Slot(descriptor.Slot)
	next, following, err := core.CheckpointLeaderOrders(index)
	if err != nil {
		return quepaxa.CheckpointSeal{}, err
	}
	return quepaxa.CheckpointSeal{ConfigID: core.ConfigIDForSlot(index), Index: index, RootHash: root, StateHash: state, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following, GenerationAnchorHash: core.GenerationAnchorHash()}, nil
}
