package checkpoint

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/thanos-io/objstore"
)

const (
	chunkSize         = 4 << 20
	maxCheckpointSize = 16 << 30
	maxChunks         = maxCheckpointSize/chunkSize + 1
	maxRootSize       = 1 << 20
)

type Chunk struct {
	Hash [32]byte `json:"hash"`
	Size int64    `json:"size"`
}

// Checkpoint is an immutable, content-addressed recovery root.
type Checkpoint struct {
	ConfigID uint     `json:"config_id"`
	Index    uint64   `json:"index"`
	Hash     [32]byte `json:"hash"`
	Size     int64    `json:"size"`
	Chunks   []Chunk  `json:"chunks"`
	RootHash [32]byte `json:"-"`
}

type Manager struct {
	bucket      objstore.Bucket
	prefix      string
	localDir    string
	configID    uint
	checkpoints []Checkpoint
	mu          sync.Mutex
}

func NewManager(bucket objstore.Bucket, prefix, localDir string, configID ...uint) *Manager {
	m := &Manager{bucket: bucket, prefix: prefix, localDir: localDir}
	if len(configID) != 0 {
		m.configID = configID[0]
	}
	return m
}

func (m *Manager) Load(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	byRoot := make(map[[32]byte]Checkpoint)
	err := m.bucket.Iter(ctx, m.key("checkpoint/roots"), func(name string) error {
		index, rootHash, ok := parseRootKey(name)
		if !ok {
			return nil
		}
		root, err := m.readRoot(ctx, name, rootHash)
		if err != nil {
			return err
		}
		if root.Index != index {
			return fmt.Errorf("checkpoint root index mismatch")
		}
		byRoot[root.RootHash] = root
		return nil
	})
	if err != nil {
		return err
	}
	m.checkpoints = m.checkpoints[:0]
	for _, root := range byRoot {
		m.checkpoints = append(m.checkpoints, root)
	}
	m.sortRoots()
	return nil
}

// Create is the bounded in-memory convenience API. CreateReader is the
// streaming protocol used by the runtime.
func (m *Manager) Create(ctx context.Context, data []byte, index uint64) error {
	_, err := m.CreateReader(ctx, bytes.NewReader(data), index)
	return err
}

func (m *Manager) CreateReader(ctx context.Context, reader io.Reader, index uint64) (*Checkpoint, error) {
	if index == 0 {
		return nil, fmt.Errorf("checkpoint index is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	spool, err := os.CreateTemp(m.localDir, ".rhiza-checkpoint-upload-*")
	if err != nil {
		return nil, err
	}
	spoolPath := spool.Name()
	defer func() {
		_ = spool.Close()
		_ = os.Remove(spoolPath)
	}()
	root := Checkpoint{ConfigID: m.configID, Index: index}
	stateHash := sha256.New()
	buffer := make([]byte, chunkSize)
	for {
		n, readErr := io.ReadFull(reader, buffer)
		if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
			return nil, readErr
		}
		if n != 0 {
			if root.Size+int64(n) > maxCheckpointSize || len(root.Chunks) >= maxChunks {
				return nil, fmt.Errorf("checkpoint exceeds %d bytes", maxCheckpointSize)
			}
			data := buffer[:n]
			hash := sha256.Sum256(data)
			if _, err := spool.Write(data); err != nil {
				return nil, err
			}
			_, _ = stateHash.Write(data)
			root.Size += int64(n)
			root.Chunks = append(root.Chunks, Chunk{Hash: hash, Size: int64(n)})
		}
		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			break
		}
	}
	if root.Size == 0 {
		return nil, fmt.Errorf("empty checkpoint")
	}
	copy(root.Hash[:], stateHash.Sum(nil))
	data, err := json.Marshal(root)
	if err != nil {
		return nil, err
	}
	root.RootHash = sha256.Sum256(data)
	rootName := m.key(rootKey(root))
	if exists, err := m.bucket.Exists(ctx, rootName); err != nil {
		return nil, fmt.Errorf("check checkpoint root: %w", err)
	} else if exists {
		existing, err := m.readRoot(ctx, rootName, root.RootHash)
		if err != nil {
			return nil, err
		}
		known := false
		for _, checkpoint := range m.checkpoints {
			known = known || checkpoint.RootHash == existing.RootHash
		}
		if !known {
			m.checkpoints = append(m.checkpoints, existing)
			m.sortRoots()
		}
		return &existing, nil
	}
	if _, err := spool.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	for _, chunk := range root.Chunks {
		chunkData := buffer[:chunk.Size]
		if _, err := io.ReadFull(spool, chunkData); err != nil {
			return nil, err
		}
		name := m.key(chunkKey(root.RootHash, chunk.Hash))
		if exists, err := m.bucket.Exists(ctx, name); err != nil {
			return nil, fmt.Errorf("check checkpoint chunk: %w", err)
		} else if !exists {
			if err := m.bucket.Upload(ctx, name, bytes.NewReader(chunkData)); err != nil {
				return nil, fmt.Errorf("upload checkpoint chunk: %w", err)
			}
		}
	}
	if err := m.bucket.Upload(ctx, rootName, bytes.NewReader(data)); err != nil {
		return nil, fmt.Errorf("publish checkpoint root: %w", err)
	}
	m.checkpoints = append(m.checkpoints, root)
	m.sortRoots()
	log.Printf("checkpoint root published: index=%d size=%d chunks=%d", index, root.Size, len(root.Chunks))
	copy := root
	return &copy, nil
}

func (m *Manager) Latest() *Checkpoint {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.checkpoints) == 0 {
		return nil
	}
	root := m.checkpoints[len(m.checkpoints)-1]
	root.Chunks = append([]Chunk(nil), root.Chunks...)
	return &root
}

func (m *Manager) OpenRoot(ctx context.Context, index uint64, rootHash [32]byte) (*Checkpoint, error) {
	name := m.key(fmt.Sprintf("checkpoint/roots/%020d_%x.json", index, rootHash))
	root, err := m.readRoot(ctx, name, rootHash)
	if err != nil {
		return nil, err
	}
	if root.Index != index {
		return nil, fmt.Errorf("checkpoint root index mismatch")
	}
	return &root, nil
}

func (m *Manager) Download(ctx context.Context, index uint64, dstPath string) error {
	file, err := os.OpenFile(dstPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	err = m.ReadTo(ctx, index, file)
	if err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	return err
}

func (m *Manager) DownloadRoot(ctx context.Context, index uint64, rootHash [32]byte, dstPath string) error {
	root, err := m.OpenRoot(ctx, index, rootHash)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(dstPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	err = m.verifyAndWrite(ctx, *root, file)
	if err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	return err
}

func (m *Manager) Read(ctx context.Context, index uint64) ([]byte, error) {
	var data bytes.Buffer
	if err := m.ReadTo(ctx, index, &data); err != nil {
		return nil, err
	}
	return data.Bytes(), nil
}

func (m *Manager) ReadRoot(ctx context.Context, index uint64, rootHash [32]byte) ([]byte, error) {
	root, err := m.OpenRoot(ctx, index, rootHash)
	if err != nil {
		return nil, err
	}
	var data bytes.Buffer
	if err := m.verifyAndWrite(ctx, *root, &data); err != nil {
		return nil, err
	}
	return data.Bytes(), nil
}

func (m *Manager) ReadTo(ctx context.Context, index uint64, writer io.Writer) error {
	m.mu.Lock()
	var target *Checkpoint
	for i := range m.checkpoints {
		if m.checkpoints[i].Index == index {
			if target != nil {
				m.mu.Unlock()
				return fmt.Errorf("checkpoint %d has multiple candidate roots; a sealed root hash is required", index)
			}
			copy := m.checkpoints[i]
			target = &copy
		}
	}
	m.mu.Unlock()
	if target == nil {
		return fmt.Errorf("checkpoint %d not found", index)
	}
	return m.verifyAndWrite(ctx, *target, writer)
}

// Verify independently strong-reads every object referenced by a proposed
// checkpoint seal before the recorder may vote for it.
func (m *Manager) Verify(ctx context.Context, index uint64, rootHash, stateHash [32]byte) error {
	name := m.key(fmt.Sprintf("checkpoint/roots/%020d_%x.json", index, rootHash))
	root, err := m.readRoot(ctx, name, rootHash)
	if err != nil {
		return err
	}
	if root.Index != index || root.Hash != stateHash {
		return fmt.Errorf("checkpoint seal identity mismatch")
	}
	return m.verifyAndWrite(ctx, root, io.Discard)
}

func (m *Manager) verifyAndWrite(ctx context.Context, target Checkpoint, writer io.Writer) error {
	stateHash := sha256.New()
	var size int64
	for _, chunk := range target.Chunks {
		r, err := m.bucket.Get(ctx, m.key(chunkKey(target.RootHash, chunk.Hash)))
		if err != nil {
			return fmt.Errorf("download checkpoint chunk: %w", err)
		}
		data, readErr := io.ReadAll(io.LimitReader(r, chunk.Size+1))
		closeErr := r.Close()
		if readErr != nil {
			return readErr
		}
		if closeErr != nil {
			return closeErr
		}
		if int64(len(data)) != chunk.Size || sha256.Sum256(data) != chunk.Hash {
			return fmt.Errorf("checkpoint %d chunk integrity mismatch", target.Index)
		}
		if _, err := writer.Write(data); err != nil {
			return err
		}
		_, _ = stateHash.Write(data)
		size += int64(len(data))
	}
	var hash [32]byte
	copy(hash[:], stateHash.Sum(nil))
	if size != target.Size || hash != target.Hash {
		return fmt.Errorf("checkpoint %d root integrity mismatch", target.Index)
	}
	return nil
}

func (m *Manager) Cleanup(ctx context.Context, keep int) error {
	if keep < 1 {
		return fmt.Errorf("checkpoint retention must be positive")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.checkpoints) <= keep {
		return nil
	}
	remove := m.checkpoints[:len(m.checkpoints)-keep]
	for _, root := range remove {
		if err := m.bucket.Delete(ctx, m.key(rootKey(root))); err != nil && !m.bucket.IsObjNotFoundErr(err) {
			return err
		}
	}
	m.checkpoints = append([]Checkpoint(nil), m.checkpoints[len(remove):]...)
	return nil
}

// GarbageCollect removes old candidate roots and chunks not referenced by any
// retained root. The grace period protects concurrent checkpoint publishers.
func (m *Manager) GarbageCollect(ctx context.Context, retain map[[32]byte]struct{}, keep int, grace time.Duration) error {
	if keep < 1 || grace < 0 {
		return fmt.Errorf("invalid checkpoint GC policy")
	}
	if err := m.Load(ctx); err != nil {
		return err
	}
	m.mu.Lock()
	keepRoots := make(map[[32]byte]struct{}, len(retain)+keep)
	for hash := range retain {
		keepRoots[hash] = struct{}{}
	}
	for i := len(m.checkpoints) - 1; i >= 0 && len(m.checkpoints)-i <= keep; i-- {
		keepRoots[m.checkpoints[i].RootHash] = struct{}{}
	}
	cutoff := time.Now().Add(-grace)
	kept := make([]Checkpoint, 0, len(m.checkpoints))
	removed := make(map[[32]byte]struct{})
	for _, root := range m.checkpoints {
		name := m.key(rootKey(root))
		_, explicit := keepRoots[root.RootHash]
		attributes, err := m.bucket.Attributes(ctx, name)
		if err != nil {
			m.mu.Unlock()
			return err
		}
		if explicit || attributes.LastModified.After(cutoff) {
			kept = append(kept, root)
			continue
		}
		if err := m.bucket.Delete(ctx, name); err != nil && !m.bucket.IsObjNotFoundErr(err) {
			m.mu.Unlock()
			return err
		}
		removed[root.RootHash] = struct{}{}
	}
	m.checkpoints = kept
	keepPrefixes := make(map[string]struct{}, len(kept)+len(removed))
	for _, root := range kept {
		keepPrefixes[m.key(chunkRootPrefix(root.RootHash))] = struct{}{}
	}
	for root := range removed {
		keepPrefixes[m.key(chunkRootPrefix(root))] = struct{}{}
	}
	m.mu.Unlock()
	return m.bucket.IterWithAttributes(ctx, m.key("checkpoint/chunks"), func(attributes objstore.IterObjectAttributes) error {
		for prefix := range keepPrefixes {
			if strings.HasPrefix(attributes.Name, prefix) {
				return nil
			}
		}
		modified, ok := attributes.LastModified()
		if !ok {
			objectAttributes, err := m.bucket.Attributes(ctx, attributes.Name)
			if err != nil {
				return err
			}
			modified = objectAttributes.LastModified
		}
		if modified.After(cutoff) {
			return nil
		}
		if err := m.bucket.Delete(ctx, attributes.Name); err != nil && !m.bucket.IsObjNotFoundErr(err) {
			return err
		}
		return nil
	}, objstore.WithUpdatedAt(), objstore.WithRecursiveIter())
}

func (m *Manager) readRoot(ctx context.Context, name string, expected [32]byte) (Checkpoint, error) {
	r, err := m.bucket.Get(ctx, name)
	if err != nil {
		return Checkpoint{}, err
	}
	defer r.Close()
	data, err := io.ReadAll(io.LimitReader(r, maxRootSize+1))
	if err != nil {
		return Checkpoint{}, err
	}
	if len(data) > maxRootSize || sha256.Sum256(data) != expected {
		return Checkpoint{}, fmt.Errorf("checkpoint root integrity mismatch")
	}
	var root Checkpoint
	if err := decodePersistedJSON(data, &root); err != nil {
		return Checkpoint{}, err
	}
	root.RootHash = expected
	if err := m.validateRoot(root); err != nil {
		return Checkpoint{}, err
	}
	return root, nil
}

func (m *Manager) validateRoot(root Checkpoint) error {
	if root.ConfigID != m.configID || root.Index == 0 || root.Size <= 0 || root.Size > maxCheckpointSize || len(root.Chunks) == 0 || len(root.Chunks) > maxChunks {
		return fmt.Errorf("invalid checkpoint root")
	}
	var size int64
	for i, chunk := range root.Chunks {
		if chunk.Size <= 0 || chunk.Size > chunkSize || (i+1 < len(root.Chunks) && chunk.Size != chunkSize) {
			return fmt.Errorf("invalid checkpoint chunk layout")
		}
		size += chunk.Size
	}
	if size != root.Size {
		return fmt.Errorf("checkpoint size mismatch")
	}
	return nil
}

func decodePersistedJSON(data []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("trailing JSON data")
	}
	return nil
}

func chunkRootPrefix(root [32]byte) string { return fmt.Sprintf("checkpoint/chunks/%x/", root) }
func chunkKey(root, chunk [32]byte) string {
	return fmt.Sprintf("%s%x.chunk", chunkRootPrefix(root), chunk)
}
func rootKey(root Checkpoint) string {
	return fmt.Sprintf("checkpoint/roots/%020d_%x.json", root.Index, root.RootHash)
}

func parseRootKey(name string) (uint64, [32]byte, bool) {
	base := strings.TrimSuffix(path.Base(name), ".json")
	parts := strings.SplitN(base, "_", 2)
	if len(parts) != 2 || len(parts[1]) != 64 {
		return 0, [32]byte{}, false
	}
	index, err := strconv.ParseUint(parts[0], 10, 64)
	decoded, hashErr := hex.DecodeString(parts[1])
	if err != nil || hashErr != nil || len(decoded) != 32 {
		return 0, [32]byte{}, false
	}
	var hash [32]byte
	copy(hash[:], decoded)
	return index, hash, true
}

func (m *Manager) key(value string) string {
	if m.prefix == "" {
		return value
	}
	return m.prefix + "/" + value
}

func (m *Manager) sortRoots() {
	sort.Slice(m.checkpoints, func(i, j int) bool {
		if m.checkpoints[i].Index != m.checkpoints[j].Index {
			return m.checkpoints[i].Index < m.checkpoints[j].Index
		}
		return bytes.Compare(m.checkpoints[i].RootHash[:], m.checkpoints[j].RootHash[:]) < 0
	})
}
