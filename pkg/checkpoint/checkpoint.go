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
	blockSize         = 64 << 20
	maxCheckpointSize = 32 << 30
	maxBlocks         = 512
	maxRootSize       = 64 << 10
	RoleSQLite        = "sqlite"
	RoleGraphData     = "graph-data"
)

type Block struct {
	Hash string `json:"hash"`
	Size int64  `json:"size"`
}

type File struct {
	Role   string  `json:"role"`
	Hash   string  `json:"hash"`
	Size   int64   `json:"size"`
	Blocks []Block `json:"blocks"`
	Path   string  `json:"-"`
}

type Source struct{ Role, Path string }

type Checkpoint struct {
	ConfigID uint     `json:"config_id"`
	Index    uint64   `json:"index"`
	Hash     [32]byte `json:"hash"`
	Size     int64    `json:"size"`
	Files    []File   `json:"files"`
	RootHash [32]byte `json:"-"`
}

type currentPointer struct {
	Index    uint64 `json:"index"`
	RootHash string `json:"root_hash"`
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
	if len(configID) > 0 {
		m.configID = configID[0]
	}
	return m
}

// Load performs two exact GETs: CURRENT and its immutable root.
func (m *Manager) Load(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, err := m.bucket.Get(ctx, m.key("checkpoint/CURRENT"))
	if err != nil {
		if m.bucket.IsObjNotFoundErr(err) {
			m.checkpoints = nil
			return nil
		}
		return err
	}
	data, readErr := io.ReadAll(io.LimitReader(r, maxRootSize+1))
	closeErr := r.Close()
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}
	var pointer currentPointer
	if err := decodePersistedJSON(data, &pointer); err != nil {
		return err
	}
	hash, err := decodeHash(pointer.RootHash)
	if err != nil {
		return fmt.Errorf("invalid CURRENT: %w", err)
	}
	root, err := m.readRoot(ctx, m.key(rootName(pointer.Index, hash)), hash)
	if err != nil {
		return err
	}
	if root.Index != pointer.Index {
		return fmt.Errorf("CURRENT index mismatch")
	}
	m.checkpoints = []Checkpoint{root}
	return nil
}

func (m *Manager) loadAll(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var roots []Checkpoint
	err := m.bucket.Iter(ctx, m.key("checkpoint/roots"), func(name string) error {
		index, hash, ok := parseRootKey(name)
		if !ok {
			return nil
		}
		root, err := m.readRoot(ctx, name, hash)
		if err != nil {
			return err
		}
		if root.Index != index {
			return fmt.Errorf("checkpoint root index mismatch")
		}
		roots = append(roots, root)
		return nil
	})
	if err != nil {
		return err
	}
	m.checkpoints = roots
	m.sortRoots()
	return nil
}

func (m *Manager) CreateFiles(ctx context.Context, sources []Source, index uint64) (*Checkpoint, error) {
	if index == 0 {
		return nil, fmt.Errorf("checkpoint index is required")
	}
	if err := validateSources(sources); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	root := Checkpoint{ConfigID: m.configID, Index: index}
	conditional := false
	for _, option := range m.bucket.SupportedObjectUploadOptions() {
		conditional = conditional || option == objstore.IfNotExists
	}
	blocks := 0
	for _, source := range sources {
		file, err := m.uploadFile(ctx, source, conditional)
		if err != nil {
			return nil, err
		}
		blocks += len(file.Blocks)
		if blocks > maxBlocks {
			return nil, fmt.Errorf("checkpoint exceeds %d blocks", maxBlocks)
		}
		root.Size += file.Size
		root.Files = append(root.Files, file)
	}
	if root.Size > maxCheckpointSize {
		return nil, fmt.Errorf("checkpoint exceeds %d bytes", maxCheckpointSize)
	}
	root.Hash = stateHash(root.Files)
	data, err := json.Marshal(root)
	if err != nil {
		return nil, err
	}
	if len(data) > maxRootSize {
		return nil, fmt.Errorf("checkpoint root exceeds %d bytes", maxRootSize)
	}
	root.RootHash = sha256.Sum256(data)
	if err := m.bucket.Upload(ctx, m.key(rootName(index, root.RootHash)), bytes.NewReader(data)); err != nil {
		return nil, fmt.Errorf("publish checkpoint root: %w", err)
	}
	pointer, _ := json.Marshal(currentPointer{Index: index, RootHash: hex.EncodeToString(root.RootHash[:])})
	if err := m.bucket.Upload(ctx, m.key("checkpoint/CURRENT"), bytes.NewReader(pointer)); err != nil {
		return nil, fmt.Errorf("publish checkpoint CURRENT: %w", err)
	}
	m.checkpoints = append(m.checkpoints, root)
	m.sortRoots()
	log.Printf("checkpoint root published: index=%d size=%d files=%d", index, root.Size, len(root.Files))
	copy := root
	return &copy, nil
}

func (m *Manager) uploadFile(ctx context.Context, source Source, conditional bool) (File, error) {
	input, err := os.Open(source.Path)
	if err != nil {
		return File{}, err
	}
	defer input.Close()
	file := File{Role: source.Role}
	hasher := sha256.New()
	buffer := make([]byte, blockSize)
	type uploadBlock struct {
		block  Block
		hash   [32]byte
		offset int64
	}
	var uploads []uploadBlock
	for {
		n, readErr := io.ReadFull(input, buffer)
		if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
			return File{}, readErr
		}
		if n > 0 {
			data := buffer[:n]
			hash := sha256.Sum256(data)
			_, _ = hasher.Write(data)
			offset := file.Size
			file.Size += int64(n)
			block := Block{Hash: hex.EncodeToString(hash[:]), Size: int64(n)}
			file.Blocks = append(file.Blocks, block)
			uploads = append(uploads, uploadBlock{block: block, hash: hash, offset: offset})
		}
		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			break
		}
	}
	if file.Size == 0 {
		return File{}, fmt.Errorf("empty checkpoint file %q", source.Role)
	}
	file.Hash = hex.EncodeToString(hasher.Sum(nil))
	if err := runParallel(ctx, len(uploads), func(ctx context.Context, index int) error {
		upload := uploads[index]
		reader := io.NewSectionReader(input, upload.offset, upload.block.Size)
		key := m.key(blockKey(upload.hash))
		var err error
		if conditional {
			err = m.bucket.Upload(ctx, key, reader, objstore.WithIfNotExists())
			if m.bucket.IsConditionNotMetErr(err) {
				err = nil
			}
		} else {
			err = m.bucket.Upload(ctx, key, reader)
		}
		return err
	}); err != nil {
		return File{}, fmt.Errorf("upload checkpoint block: %w", err)
	}
	return file, nil
}

func runParallel(ctx context.Context, count int, work func(context.Context, int) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int)
	var wg sync.WaitGroup
	var once sync.Once
	var firstErr error
	workers := 4
	if count < workers {
		workers = count
	}
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				if err := work(ctx, index); err != nil {
					once.Do(func() { firstErr = err; cancel() })
					return
				}
			}
		}()
	}
dispatch:
	for index := 0; index < count; index++ {
		select {
		case jobs <- index:
		case <-ctx.Done():
			break dispatch
		}
	}
	close(jobs)
	wg.Wait()
	if firstErr == nil {
		firstErr = ctx.Err()
	}
	return firstErr
}

func validateSources(sources []Source) error {
	if len(sources) < 1 || len(sources) > 2 {
		return fmt.Errorf("checkpoint requires one or two fixed-role files")
	}
	seen := map[string]bool{}
	for i, source := range sources {
		if source.Path == "" || seen[source.Role] || source.Role != RoleSQLite && source.Role != RoleGraphData {
			return fmt.Errorf("invalid checkpoint source")
		}
		if i == 0 && source.Role != RoleSQLite || i == 1 && source.Role != RoleGraphData {
			return fmt.Errorf("checkpoint sources are not in fixed role order")
		}
		seen[source.Role] = true
	}
	if !seen[RoleSQLite] || len(sources) == 2 && !seen[RoleGraphData] {
		return fmt.Errorf("invalid checkpoint role set")
	}
	return nil
}

func (m *Manager) Latest() *Checkpoint {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.checkpoints) == 0 {
		return nil
	}
	root := m.checkpoints[len(m.checkpoints)-1]
	return &root
}

func (m *Manager) OpenRoot(ctx context.Context, index uint64, rootHash [32]byte) (*Checkpoint, error) {
	root, err := m.readRoot(ctx, m.key(rootName(index, rootHash)), rootHash)
	if err != nil {
		return nil, err
	}
	if root.Index != index {
		return nil, fmt.Errorf("checkpoint root index mismatch")
	}
	return &root, nil
}

func (m *Manager) DownloadRootFiles(ctx context.Context, index uint64, rootHash [32]byte, dir string) ([]File, error) {
	root, err := m.OpenRoot(ctx, index, rootHash)
	if err != nil {
		return nil, err
	}
	files := make([]File, 0, len(root.Files))
	for _, file := range root.Files {
		file.Path = filePath(dir, file.Role)
		if err := m.downloadFile(ctx, index, file); err != nil {
			for _, created := range files {
				_ = os.Remove(created.Path)
			}
			return nil, err
		}
		files = append(files, file)
	}
	return files, nil
}

func filePath(dir, role string) string {
	if role == RoleSQLite {
		return path.Join(dir, "sqlite.db")
	}
	return path.Join(dir, "graph-data.db")
}

func (m *Manager) downloadFile(ctx context.Context, index uint64, file File) error {
	output, err := os.OpenFile(file.Path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	complete := false
	defer func() {
		if !complete {
			_ = os.Remove(file.Path)
		}
	}()
	if err := output.Truncate(file.Size); err != nil {
		_ = output.Close()
		return err
	}
	offsets := make([]int64, len(file.Blocks))
	for i := 1; i < len(offsets); i++ {
		offsets[i] = offsets[i-1] + file.Blocks[i-1].Size
	}
	if err := runParallel(ctx, len(file.Blocks), func(ctx context.Context, blockIndex int) error {
		block := file.Blocks[blockIndex]
		hash, err := decodeHash(block.Hash)
		if err != nil {
			return err
		}
		r, err := m.bucket.Get(ctx, m.key(blockKey(hash)))
		if err != nil {
			return err
		}
		hasher := sha256.New()
		written, copyErr := io.Copy(io.NewOffsetWriter(output, offsets[blockIndex]), io.TeeReader(io.LimitReader(r, block.Size+1), hasher))
		closeErr := r.Close()
		var got [32]byte
		copy(got[:], hasher.Sum(nil))
		if copyErr != nil || closeErr != nil || written != block.Size || got != hash {
			return fmt.Errorf("checkpoint %d block integrity mismatch", index)
		}
		return nil
	}); err != nil {
		_ = output.Close()
		return err
	}
	if err := output.Sync(); err != nil {
		_ = output.Close()
		return err
	}
	if _, err := output.Seek(0, io.SeekStart); err != nil {
		_ = output.Close()
		return err
	}
	hasher := sha256.New()
	if _, err := io.Copy(hasher, output); err != nil || hex.EncodeToString(hasher.Sum(nil)) != file.Hash {
		_ = output.Close()
		return fmt.Errorf("checkpoint %d file integrity mismatch", index)
	}
	if err := output.Close(); err != nil {
		return err
	}
	complete = true
	return nil
}

func (m *Manager) Verify(ctx context.Context, index uint64, rootHash, state [32]byte) error {
	root, err := m.OpenRoot(ctx, index, rootHash)
	if err != nil {
		return err
	}
	if root.Hash != state {
		return fmt.Errorf("checkpoint seal identity mismatch")
	}
	for _, file := range root.Files {
		hasher := sha256.New()
		var size int64
		for _, block := range file.Blocks {
			hash, err := decodeHash(block.Hash)
			if err != nil {
				return err
			}
			r, err := m.bucket.Get(ctx, m.key(blockKey(hash)))
			if err != nil {
				return err
			}
			data, readErr := io.ReadAll(io.LimitReader(r, block.Size+1))
			closeErr := r.Close()
			if readErr != nil || closeErr != nil || int64(len(data)) != block.Size || sha256.Sum256(data) != hash {
				return fmt.Errorf("checkpoint block integrity mismatch")
			}
			_, _ = hasher.Write(data)
			size += int64(len(data))
		}
		if size != file.Size || hex.EncodeToString(hasher.Sum(nil)) != file.Hash {
			return fmt.Errorf("checkpoint file integrity mismatch")
		}
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
		if err := m.bucket.Delete(ctx, m.key(rootName(root.Index, root.RootHash))); err != nil && !m.bucket.IsObjNotFoundErr(err) {
			return err
		}
	}
	m.checkpoints = append([]Checkpoint(nil), m.checkpoints[len(remove):]...)
	return nil
}

func (m *Manager) GarbageCollect(ctx context.Context, retain map[[32]byte]struct{}, keep int, grace time.Duration) error {
	if keep < 1 || grace < 0 {
		return fmt.Errorf("invalid checkpoint GC policy")
	}
	if err := m.loadAll(ctx); err != nil {
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
	kept, removed := make([]Checkpoint, 0, len(m.checkpoints)), make([]Checkpoint, 0)
	for _, root := range m.checkpoints {
		name := m.key(rootName(root.Index, root.RootHash))
		attributes, err := m.bucket.Attributes(ctx, name)
		if err != nil {
			m.mu.Unlock()
			return err
		}
		_, explicit := keepRoots[root.RootHash]
		if explicit || attributes.LastModified.After(cutoff) {
			kept = append(kept, root)
			continue
		}
		if err := m.bucket.Delete(ctx, name); err != nil && !m.bucket.IsObjNotFoundErr(err) {
			m.mu.Unlock()
			return err
		}
		removed = append(removed, root)
	}
	m.checkpoints = kept
	live := map[[32]byte]struct{}{}
	for _, root := range append(append([]Checkpoint(nil), kept...), removed...) {
		for _, file := range root.Files {
			for _, block := range file.Blocks {
				hash, err := decodeHash(block.Hash)
				if err != nil {
					m.mu.Unlock()
					return err
				}
				live[hash] = struct{}{}
			}
		}
	}
	m.mu.Unlock()
	return m.bucket.IterWithAttributes(ctx, m.key("checkpoint/blocks"), func(attributes objstore.IterObjectAttributes) error {
		hash, ok := parseBlockKey(attributes.Name)
		if !ok {
			return nil
		}
		if _, ok := live[hash]; ok {
			return nil
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
	if err != nil || len(data) > maxRootSize || sha256.Sum256(data) != expected {
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
	if root.ConfigID != m.configID || root.Index == 0 || root.Size <= 0 || root.Size > maxCheckpointSize || len(root.Files) < 1 || len(root.Files) > 2 {
		return fmt.Errorf("invalid checkpoint root")
	}
	seen, size, blocks := map[string]bool{}, int64(0), 0
	for _, file := range root.Files {
		if seen[file.Role] || file.Role != RoleSQLite && file.Role != RoleGraphData || file.Size <= 0 || len(file.Blocks) == 0 {
			return fmt.Errorf("invalid checkpoint file")
		}
		seen[file.Role] = true
		var fileSize int64
		for i, block := range file.Blocks {
			if _, err := decodeHash(block.Hash); err != nil || block.Size <= 0 || block.Size > blockSize || i+1 < len(file.Blocks) && block.Size != blockSize {
				return fmt.Errorf("invalid checkpoint block layout")
			}
			fileSize += block.Size
			blocks++
		}
		if fileSize != file.Size {
			return fmt.Errorf("checkpoint file size mismatch")
		}
		if _, err := decodeHash(file.Hash); err != nil {
			return err
		}
		size += file.Size
	}
	if !seen[RoleSQLite] || blocks > maxBlocks || size != root.Size || root.Hash != stateHash(root.Files) {
		return fmt.Errorf("checkpoint root mismatch")
	}
	return nil
}

func stateHash(files []File) [32]byte {
	h := sha256.New()
	for _, file := range files {
		_, _ = io.WriteString(h, file.Role+":"+file.Hash+":"+strconv.FormatInt(file.Size, 10)+"\n")
	}
	var result [32]byte
	copy(result[:], h.Sum(nil))
	return result
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

func decodeHash(value string) ([32]byte, error) {
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size {
		return [32]byte{}, fmt.Errorf("invalid SHA-256 digest")
	}
	var hash [32]byte
	copy(hash[:], decoded)
	return hash, nil
}

func blockKey(hash [32]byte) string { return fmt.Sprintf("checkpoint/blocks/%x.block", hash) }
func parseBlockKey(name string) ([32]byte, bool) {
	hash, err := decodeHash(strings.TrimSuffix(path.Base(name), ".block"))
	return hash, err == nil
}
func rootName(index uint64, hash [32]byte) string {
	return fmt.Sprintf("checkpoint/roots/%020d_%x.json", index, hash)
}
func parseRootKey(name string) (uint64, [32]byte, bool) {
	parts := strings.SplitN(strings.TrimSuffix(path.Base(name), ".json"), "_", 2)
	if len(parts) != 2 {
		return 0, [32]byte{}, false
	}
	index, err := strconv.ParseUint(parts[0], 10, 64)
	hash, hashErr := decodeHash(parts[1])
	return index, hash, err == nil && hashErr == nil
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
