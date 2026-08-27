package checkpoint

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"slices"
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

var ErrStaleCheckpoint = errors.New("stale checkpoint candidate")

var (
	ErrPublisherBusy   = errors.New("checkpoint publisher is active")
	ErrPublisherFenced = errors.New("checkpoint publisher was fenced")
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
	claim    *PublisherClaim
}

type PublisherClaim struct {
	ConfigID      uint   `json:"config_id"`
	Generation    uint64 `json:"generation"`
	OwnerID       string `json:"owner_id"`
	LeaseUntilMS  int64  `json:"lease_until_unix_ms"`
	ReservedIndex uint64 `json:"reserved_index"`
	BoundIndex    uint64 `json:"bound_index,omitempty"`
	RootHash      string `json:"root_hash,omitempty"`
	version       *objstore.ObjectVersion
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
	certified   *Checkpoint
	mu          sync.Mutex
	verifiedMu  sync.Mutex
	verified    map[string]int64
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
			m.certified = nil
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
	copy := root
	m.certified = &copy
	return nil
}

func (m *Manager) AcquirePublisherClaim(ctx context.Context, owner string, minExclusive uint64, lease time.Duration) (*PublisherClaim, error) {
	if owner == "" || lease <= 0 {
		return nil, fmt.Errorf("publisher owner and positive lease are required")
	}
	if !slices.Contains(m.bucket.SupportedObjectUploadOptions(), objstore.IfNotExists) || !slices.Contains(m.bucket.SupportedObjectUploadOptions(), objstore.IfMatch) {
		return nil, fmt.Errorf("checkpoint publisher requires conditional object writes")
	}
	key := m.key("checkpoint/PUBLISHER")
	for range 4 {
		current, err := m.readPublisherClaim(ctx)
		if err != nil && !m.bucket.IsObjNotFoundErr(err) {
			return nil, err
		}
		now := time.Now()
		if err == nil && current.LeaseUntilMS > now.UnixMilli() {
			return nil, ErrPublisherBusy
		}
		generation, floor := uint64(1), minExclusive
		var options []objstore.ObjectUploadOption
		if err == nil {
			generation = current.Generation + 1
			floor = max(floor, current.ReservedIndex, current.BoundIndex)
			options = append(options, objstore.WithIfMatch(current.version))
		} else {
			options = append(options, objstore.WithIfNotExists())
		}
		claim := &PublisherClaim{ConfigID: m.configID, Generation: generation, OwnerID: owner, LeaseUntilMS: now.Add(lease).UnixMilli(), ReservedIndex: floor + 1}
		if err := m.uploadPublisherClaim(ctx, key, claim, options...); err != nil {
			if m.bucket.IsConditionNotMetErr(err) {
				continue
			}
			return nil, err
		}
		return m.readPublisherClaim(ctx)
	}
	return nil, ErrPublisherBusy
}

func (m *Manager) BindPublisherClaim(ctx context.Context, claim *PublisherClaim, index uint64, root [32]byte, lease time.Duration) (*PublisherClaim, error) {
	current, err := m.readPublisherClaim(ctx)
	if err != nil {
		return nil, err
	}
	if claim == nil || current.Generation != claim.Generation || current.OwnerID != claim.OwnerID || current.LeaseUntilMS <= time.Now().UnixMilli() || index < current.ReservedIndex {
		return nil, ErrPublisherFenced
	}
	current.BoundIndex, current.RootHash, current.LeaseUntilMS = index, hex.EncodeToString(root[:]), time.Now().Add(lease).UnixMilli()
	if err := m.uploadPublisherClaim(ctx, m.key("checkpoint/PUBLISHER"), current, objstore.WithIfMatch(current.version)); err != nil {
		if m.bucket.IsConditionNotMetErr(err) {
			return nil, ErrPublisherFenced
		}
		return nil, err
	}
	return m.readPublisherClaim(ctx)
}

func (m *Manager) RenewPublisherClaim(ctx context.Context, claim *PublisherClaim, lease time.Duration) (*PublisherClaim, error) {
	current, err := m.readPublisherClaim(ctx)
	if err != nil {
		return nil, err
	}
	if claim == nil || current.Generation != claim.Generation || current.OwnerID != claim.OwnerID || current.BoundIndex != claim.BoundIndex || current.RootHash != claim.RootHash || current.LeaseUntilMS <= time.Now().UnixMilli() {
		return nil, ErrPublisherFenced
	}
	current.LeaseUntilMS = time.Now().Add(lease).UnixMilli()
	if err := m.uploadPublisherClaim(ctx, m.key("checkpoint/PUBLISHER"), current, objstore.WithIfMatch(current.version)); err != nil {
		if m.bucket.IsConditionNotMetErr(err) {
			return nil, ErrPublisherFenced
		}
		return nil, err
	}
	return m.readPublisherClaim(ctx)
}

func (m *Manager) ValidatePublisherClaim(ctx context.Context, owner string, index uint64, root [32]byte) error {
	claim, err := m.readPublisherClaim(ctx)
	if err != nil {
		return err
	}
	if claim.ConfigID != m.configID || claim.OwnerID != owner || claim.LeaseUntilMS <= time.Now().UnixMilli() || claim.BoundIndex != index || claim.RootHash != hex.EncodeToString(root[:]) {
		return ErrPublisherFenced
	}
	return nil
}

func (m *Manager) ReleasePublisherClaim(ctx context.Context, claim *PublisherClaim) error {
	current, err := m.readPublisherClaim(ctx)
	if err != nil {
		return err
	}
	if claim == nil || current.Generation != claim.Generation || current.OwnerID != claim.OwnerID {
		return ErrPublisherFenced
	}
	current.LeaseUntilMS = time.Now().UnixMilli()
	if err := m.uploadPublisherClaim(ctx, m.key("checkpoint/PUBLISHER"), current, objstore.WithIfMatch(current.version)); err != nil && m.bucket.IsConditionNotMetErr(err) {
		return ErrPublisherFenced
	} else {
		return err
	}
}

func (m *Manager) readPublisherClaim(ctx context.Context) (*PublisherClaim, error) {
	key := m.key("checkpoint/PUBLISHER")
	for range 4 {
		before, err := m.bucket.Attributes(ctx, key)
		if err != nil {
			return nil, err
		}
		r, err := m.bucket.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(io.LimitReader(r, maxRootSize+1))
		closeErr := r.Close()
		if readErr != nil || closeErr != nil || len(data) > maxRootSize {
			if readErr != nil {
				return nil, readErr
			}
			if closeErr != nil {
				return nil, closeErr
			}
			return nil, fmt.Errorf("checkpoint publisher claim exceeds size limit")
		}
		after, err := m.bucket.Attributes(ctx, key)
		if err != nil {
			return nil, err
		}
		if before.Version == nil || after.Version == nil || *before.Version != *after.Version {
			continue
		}
		var claim PublisherClaim
		if err := decodePersistedJSON(data, &claim); err != nil {
			return nil, err
		}
		if claim.ConfigID != m.configID || claim.Generation == 0 || claim.OwnerID == "" || claim.ReservedIndex == 0 {
			return nil, fmt.Errorf("invalid checkpoint publisher claim")
		}
		claim.version = after.Version
		return &claim, nil
	}
	return nil, fmt.Errorf("checkpoint publisher claim did not stabilize")
}

func (m *Manager) uploadPublisherClaim(ctx context.Context, key string, claim *PublisherClaim, options ...objstore.ObjectUploadOption) error {
	copy := *claim
	copy.version = nil
	data, err := json.Marshal(copy)
	if err != nil {
		return err
	}
	return m.bucket.Upload(ctx, key, bytes.NewReader(data), options...)
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
	knownBlocks := make(map[string]int64)
	if m.certified != nil {
		for _, file := range m.certified.Files {
			for _, block := range file.Blocks {
				knownBlocks[block.Hash] = block.Size
			}
		}
	}
	blocks := 0
	for _, source := range sources {
		file, err := m.uploadFile(ctx, source, conditional, knownBlocks)
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
	rootKey := m.key(rootName(index, root.RootHash))
	var uploadOptions []objstore.ObjectUploadOption
	if conditional {
		uploadOptions = append(uploadOptions, objstore.WithIfNotExists())
	}
	if err := m.bucket.Upload(ctx, rootKey, bytes.NewReader(data), uploadOptions...); err != nil && !(conditional && m.bucket.IsConditionNotMetErr(err)) {
		return nil, fmt.Errorf("publish checkpoint root: %w", err)
	}
	log.Printf("checkpoint candidate published: index=%d size=%d files=%d", index, root.Size, len(root.Files))
	copy := root
	return &copy, nil
}

// PromoteCertifiedCurrent advances CURRENT only after the checkpoint seal is
// decided. Conditional writes make the pointer monotonic across stale writers.
func (m *Manager) PromoteCertifiedCurrent(ctx context.Context, root *Checkpoint) error {
	if root == nil {
		return fmt.Errorf("checkpoint root is required")
	}
	if _, err := m.OpenRoot(ctx, root.Index, root.RootHash); err != nil {
		return err
	}
	if !slices.Contains(m.bucket.SupportedObjectUploadOptions(), objstore.IfNotExists) || !slices.Contains(m.bucket.SupportedObjectUploadOptions(), objstore.IfMatch) {
		return fmt.Errorf("checkpoint CURRENT requires conditional object writes")
	}

	key := m.key("checkpoint/CURRENT")
	pointer, err := json.Marshal(currentPointer{Index: root.Index, RootHash: hex.EncodeToString(root.RootHash[:])})
	if err != nil {
		return err
	}
	for range 4 {
		attributes, attrErr := m.bucket.Attributes(ctx, key)
		if attrErr != nil {
			if !m.bucket.IsObjNotFoundErr(attrErr) {
				return attrErr
			}
			err = m.bucket.Upload(ctx, key, bytes.NewReader(pointer), objstore.WithIfNotExists())
		} else {
			if attributes.Version == nil {
				return fmt.Errorf("checkpoint CURRENT has no conditional-write version")
			}
			r, getErr := m.bucket.Get(ctx, key)
			if getErr != nil {
				return getErr
			}
			data, readErr := io.ReadAll(io.LimitReader(r, maxRootSize+1))
			closeErr := r.Close()
			if readErr != nil || closeErr != nil {
				if readErr != nil {
					return readErr
				}
				return closeErr
			}
			var current currentPointer
			if decodeErr := decodePersistedJSON(data, &current); decodeErr != nil {
				return decodeErr
			}
			currentHash, decodeErr := decodeHash(current.RootHash)
			if decodeErr != nil {
				return decodeErr
			}
			if current.Index > root.Index {
				currentRoot, openErr := m.OpenRoot(ctx, current.Index, currentHash)
				if openErr != nil {
					return openErr
				}
				m.rememberCertified(*currentRoot)
				return fmt.Errorf("%w: CURRENT is %d, candidate is %d", ErrStaleCheckpoint, current.Index, root.Index)
			}
			if current.Index == root.Index && currentHash != root.RootHash {
				return fmt.Errorf("checkpoint CURRENT cannot move from %d to conflicting index %d", current.Index, root.Index)
			}
			if current.Index == root.Index {
				m.rememberCertified(*root)
				return nil
			}
			err = m.bucket.Upload(ctx, key, bytes.NewReader(pointer), objstore.WithIfMatch(attributes.Version))
		}
		if err == nil {
			m.rememberCertified(*root)
			return nil
		}
		if !m.bucket.IsConditionNotMetErr(err) {
			return fmt.Errorf("promote checkpoint CURRENT: %w", err)
		}
	}
	return fmt.Errorf("promote checkpoint CURRENT: concurrent writers did not converge")
}

func (m *Manager) rememberCertified(root Checkpoint) {
	m.mu.Lock()
	defer m.mu.Unlock()
	copy := root
	m.certified = &copy
	for _, existing := range m.checkpoints {
		if existing.RootHash == root.RootHash {
			return
		}
	}
	m.checkpoints = append(m.checkpoints, root)
	m.sortRoots()
}

func (m *Manager) uploadFile(ctx context.Context, source Source, conditional bool, known map[string]int64) (File, error) {
	input, err := os.Open(source.Path)
	if err != nil {
		return File{}, err
	}
	defer input.Close()
	info, err := input.Stat()
	if err != nil {
		return File{}, err
	}
	if info.Size() <= 0 || info.Size() > maxCheckpointSize {
		return File{}, fmt.Errorf("invalid checkpoint file size")
	}
	file := File{Role: source.Role, Size: info.Size()}
	type uploadBlock struct {
		block  Block
		hash   [32]byte
		offset int64
	}
	var uploads []uploadBlock
	for offset := int64(0); offset < file.Size; offset += blockSize {
		size := min(int64(blockSize), file.Size-offset)
		hasher := sha256.New()
		read, err := io.Copy(hasher, io.NewSectionReader(input, offset, size))
		if err != nil {
			return File{}, fmt.Errorf("hash checkpoint block: %w", err)
		}
		if read != size {
			return File{}, fmt.Errorf("hash checkpoint block: read %d of %d bytes", read, size)
		}
		var hash [32]byte
		copy(hash[:], hasher.Sum(nil))
		block := Block{Hash: hex.EncodeToString(hash[:]), Size: size}
		file.Blocks = append(file.Blocks, block)
		uploads = append(uploads, uploadBlock{block: block, hash: hash, offset: offset})
	}
	file.Hash = fileDescriptorHash(file.Blocks)
	if err := runParallel(ctx, len(uploads), func(ctx context.Context, index int) error {
		upload := uploads[index]
		if known[upload.block.Hash] == upload.block.Size {
			return nil
		}
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
	if m.certified == nil {
		return nil
	}
	root := *m.certified
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
	m.verifiedMu.Lock()
	defer m.verifiedMu.Unlock()
	if err := m.loadVerifiedBlocks(); err != nil {
		return err
	}
	type verifyBlock struct {
		hash [32]byte
		size int64
	}
	var pending []verifyBlock
	for _, file := range root.Files {
		for _, block := range file.Blocks {
			hash, err := decodeHash(block.Hash)
			if err != nil {
				return err
			}
			if m.verified[block.Hash] != block.Size {
				pending = append(pending, verifyBlock{hash: hash, size: block.Size})
			}
		}
	}
	if len(pending) == 0 {
		return nil
	}
	if err := runParallel(ctx, len(pending), func(ctx context.Context, blockIndex int) error {
		block := pending[blockIndex]
		r, err := m.bucket.Get(ctx, m.key(blockKey(block.hash)))
		if err != nil {
			return err
		}
		hasher := sha256.New()
		read, readErr := io.Copy(hasher, io.LimitReader(r, block.size+1))
		closeErr := r.Close()
		var got [32]byte
		copy(got[:], hasher.Sum(nil))
		if readErr != nil || closeErr != nil || read != block.size || got != block.hash {
			return fmt.Errorf("checkpoint block integrity mismatch")
		}
		return nil
	}); err != nil {
		return err
	}
	for _, block := range pending {
		m.verified[hex.EncodeToString(block.hash[:])] = block.size
	}
	return m.storeVerifiedBlocks()
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
	verifiedKeep := map[string]int64{}
	for _, root := range kept {
		for _, file := range root.Files {
			for _, block := range file.Blocks {
				verifiedKeep[block.Hash] = block.Size
			}
		}
	}
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
	if err := m.pruneVerifiedBlocks(verifiedKeep); err != nil {
		return err
	}
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

func (m *Manager) pruneVerifiedBlocks(keep map[string]int64) error {
	m.verifiedMu.Lock()
	defer m.verifiedMu.Unlock()
	if err := m.loadVerifiedBlocks(); err != nil {
		return err
	}
	changed := false
	for hash, size := range m.verified {
		if keep[hash] != size {
			delete(m.verified, hash)
			changed = true
		}
	}
	if !changed {
		return nil
	}
	return m.storeVerifiedBlocks()
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
		if file.Hash != fileDescriptorHash(file.Blocks) {
			return fmt.Errorf("checkpoint file descriptor mismatch")
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

func fileDescriptorHash(blocks []Block) string {
	h := sha256.New()
	for _, block := range blocks {
		_, _ = io.WriteString(h, block.Hash+":"+strconv.FormatInt(block.Size, 10)+"\n")
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (m *Manager) loadVerifiedBlocks() error {
	if m.verified != nil {
		return nil
	}
	m.verified = make(map[string]int64)
	if m.localDir == "" {
		return nil
	}
	data, err := os.ReadFile(path.Join(m.localDir, "checkpoint-verified-blocks.json"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := decodePersistedJSON(data, &m.verified); err != nil {
		return fmt.Errorf("read verified checkpoint blocks: %w", err)
	}
	for hash, size := range m.verified {
		if _, err := decodeHash(hash); err != nil || size <= 0 || size > blockSize {
			return fmt.Errorf("invalid verified checkpoint block cache")
		}
	}
	return nil
}

func (m *Manager) storeVerifiedBlocks() error {
	if m.localDir == "" {
		return nil
	}
	data, err := json.Marshal(m.verified)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(m.localDir, ".checkpoint-verified-blocks-*")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryName, path.Join(m.localDir, "checkpoint-verified-blocks.json")); err != nil {
		return err
	}
	directory, err := os.Open(m.localDir)
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	return errors.Join(syncErr, closeErr)
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
