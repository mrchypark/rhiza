package recovery

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sort"
	"sync"
	"time"

	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

var ErrLocalHEADAdvanced = errors.New("Local HEAD advanced")

func (a *LocalArchive) localManager() *checkpoint.Manager {
	return checkpoint.NewManager(a.bucket, a.prefix, "", a.Binding.Bootstrap.ConfigID)
}

// GarbageCollect retains the current snapshot, latest keep committed snapshots,
// and all active restore pins. Zero selects the default of three snapshots.
// It never runs implicitly during read-only archive opening.
func (a *LocalArchive) GarbageCollect(ctx context.Context, keep int, grace time.Duration) error {
	if keep == 0 {
		keep = 3
	}
	if keep < 1 || grace < 0 {
		return fmt.Errorf("invalid Local GC policy")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.localManager().WithLocalMaintenanceContext(ctx, func(ctx context.Context) error {
		if err := a.checkLocalHead(ctx); err != nil {
			return err
		}
		head := a.head
		if head.Version == 1 {
			head.Version = 2
			if head.Snapshot != nil {
				head.Catalog = []LocalSnapshot{*head.Snapshot}
			}
			if err := a.publish(ctx, head); err != nil {
				return err
			}
		}
		catalog := append([]LocalSnapshot(nil), a.head.Catalog...)
		sort.Slice(catalog, func(i, j int) bool { return catalog[i].Seal.Index > catalog[j].Seal.Index })
		retain, retired := map[[32]byte]struct{}{}, map[[32]byte]struct{}{}
		tailMetadata := map[[32]byte]bool{}
		for _, extent := range a.extents {
			for _, value := range extent.Decisions {
				seal, ok, err := quepaxa.DecodeCheckpointSeal(value.Value)
				if err != nil {
					return err
				}
				if !ok {
					continue
				}
				artifact, err := a.ReadSnapshot(ctx, seal.RootHash)
				if err != nil {
					return err
				}
				if err := a.validateSnapshot(artifact, false); err != nil {
					return err
				}
				retain[artifact.PortableRoot] = struct{}{}
				tailMetadata[seal.RootHash] = true
			}
		}
		active, err := a.localManager().LocalRecoveryActive(ctx)
		if err != nil {
			return err
		}
		viewActive, err := a.activeLocalViews(ctx)
		if err != nil {
			return err
		}
		active = active || viewActive
		seen := map[[32]byte]bool{}
		for i, snapshot := range catalog {
			if seen[snapshot.PortableRoot] {
				return fmt.Errorf("duplicate Local retention root")
			}
			seen[snapshot.PortableRoot] = true
			stored, err := a.ReadSnapshot(ctx, snapshot.Seal.RootHash)
			if err != nil {
				// A previous acknowledged retirement may have removed both objects
				// before its catalog prune. Only a non-live committed pair can resume.
				_, tailRoot := retain[snapshot.PortableRoot]
				eligible := i >= keep && !active && !tailRoot && (a.head.Snapshot == nil || snapshot.PortableRoot != a.head.Snapshot.PortableRoot)
				if !a.bucket.IsObjNotFoundErr(err) || !eligible {
					return err
				}
				present, existsErr := a.localManager().LocalRootExists(ctx, uint64(snapshot.Seal.Index), snapshot.PortableRoot)
				if existsErr != nil {
					return existsErr
				}
				if present {
					return err
				}
				retired[snapshot.PortableRoot] = struct{}{}
				continue
			}
			want, _ := json.Marshal(snapshot)
			got, _ := json.Marshal(stored)
			if !bytes.Equal(want, got) {
				return fmt.Errorf("Local retention metadata mismatch")
			}
			if i < keep || a.head.Snapshot != nil && snapshot.PortableRoot == a.head.Snapshot.PortableRoot {
				if _, err := a.localManager().OpenRoot(ctx, uint64(snapshot.Seal.Index), snapshot.PortableRoot); err != nil {
					return err
				}
				retain[snapshot.PortableRoot] = struct{}{}
			} else {
				retired[snapshot.PortableRoot] = struct{}{}
			}
		}
		if a.head.Snapshot != nil && !seen[a.head.Snapshot.PortableRoot] {
			return fmt.Errorf("current snapshot missing from Local retention catalog")
		}
		remove, err := a.localExtentCandidates(ctx, grace)
		if err != nil {
			return err
		}
		if active {
			remove = nil
		}
		if err := a.localManager().GarbageCollectLocal(ctx, retired, retain, grace); err != nil {
			return err
		}
		for _, name := range remove {
			if err := a.bucket.Delete(ctx, name); err != nil && !a.bucket.IsObjNotFoundErr(err) {
				return err
			}
		}
		return a.pruneLocalCatalog(ctx, retired, active, tailMetadata)
	})
}

// Prune only after every deletion has acknowledged success. Present retired
// roots retain their eligibility across grace periods and active restore pins.
func (a *LocalArchive) pruneLocalCatalog(ctx context.Context, retired map[[32]byte]struct{}, active bool, tailMetadata map[[32]byte]bool) error {
	head := a.head
	head.Catalog = make([]LocalSnapshot, 0, len(a.head.Catalog))
	for _, snapshot := range a.head.Catalog {
		if _, candidate := retired[snapshot.PortableRoot]; candidate {
			present, err := a.localManager().LocalRootExists(ctx, uint64(snapshot.Seal.Index), snapshot.PortableRoot)
			if err != nil {
				return err
			}
			if !present && !active && !tailMetadata[snapshot.Seal.RootHash] {
				key := a.key(fmt.Sprintf("snapshots/%x.json", snapshot.Seal.RootHash))
				if err := a.bucket.Delete(ctx, key); err != nil && !a.bucket.IsObjNotFoundErr(err) {
					return err
				}
				continue
			}
		}
		head.Catalog = append(head.Catalog, snapshot)
	}
	if len(head.Catalog) == len(a.head.Catalog) {
		return nil
	}
	return a.publish(ctx, head)
}

func (a *LocalArchive) localExtentCandidates(ctx context.Context, grace time.Duration) ([]string, error) {
	live := map[string]bool{}
	hash := a.head.Tail
	for hash != ([32]byte{}) {
		name := fmt.Sprintf("extents/%x", hash)
		if live[a.key(name)] {
			return nil, fmt.Errorf("Local extent cycle")
		}
		data, err := a.read(ctx, name, maxExtentSize)
		if err != nil {
			return nil, err
		}
		if sha256.Sum256(data) != hash {
			return nil, fmt.Errorf("Local extent hash mismatch")
		}
		extent, err := decodeExtent(data)
		if err != nil {
			return nil, err
		}
		live[a.key(name)] = true
		hash = extent.PreviousHash
	}
	var remove []string
	cutoff := time.Now().Add(-grace)
	if err := a.bucket.Iter(ctx, a.key("extents"), func(name string) error {
		decoded, err := hex.DecodeString(path.Base(name))
		if err != nil || len(decoded) != 32 || name != a.key(fmt.Sprintf("extents/%x", decoded)) {
			return fmt.Errorf("unknown Local extent metadata: %s", name)
		}
		if live[name] {
			return nil
		}
		attrs, err := a.bucket.Attributes(ctx, name)
		if err != nil {
			return err
		}
		if !attrs.LastModified.IsZero() && attrs.LastModified.Before(cutoff) {
			remove = append(remove, name)
		}
		return nil
	}, objstore.WithRecursiveIter()); err != nil {
		return nil, err
	}
	return remove, nil
}

func (a *LocalArchive) validateLocalCatalog() error {
	seen := map[[32]byte]bool{}
	current := a.head.Snapshot == nil
	validator := &LocalArchive{Binding: a.Binding}
	for _, snapshot := range a.head.Catalog {
		if err := validator.validateSnapshot(snapshot, true); err != nil {
			return err
		}
		if seen[snapshot.PortableRoot] {
			return fmt.Errorf("duplicate Local catalog root")
		}
		seen[snapshot.PortableRoot] = true
		if a.head.Snapshot != nil {
			want, _ := json.Marshal(a.head.Snapshot)
			got, _ := json.Marshal(snapshot)
			if bytes.Equal(want, got) {
				current = true
			}
		}
	}
	if !current {
		return fmt.Errorf("Local catalog lacks current snapshot")
	}
	return nil
}

func (a *LocalArchive) checkLocalHead(ctx context.Context) error {
	if a.failure != nil {
		return a.failure
	}
	fresh, err := OpenLocalArchive(ctx, a.bucket, a.prefix)
	if err != nil {
		return err
	}
	if !sameObjectVersion(a.version, fresh.version) {
		return fmt.Errorf("%w; reopen required", ErrLocalHEADAdvanced)
	}
	return nil
}

// PinRestore registers before downloading. The gate excludes GC through root
// existence validation and pin publication. Renewal cannot resurrect expired pins.
func (a *LocalArchive) PinRestore(ctx context.Context, snapshot LocalSnapshot, owner string, lease time.Duration) (*LocalRestorePin, error) {
	return a.pinLocalView(ctx, &snapshot, owner, lease)
}

// PinView protects the exact captured HEAD, including a snapshotless tail.
func (a *LocalArchive) PinView(ctx context.Context, owner string, lease time.Duration) (*LocalRestorePin, error) {
	return a.pinLocalView(ctx, nil, owner, lease)
}

func (a *LocalArchive) pinLocalView(ctx context.Context, selected *LocalSnapshot, owner string, lease time.Duration) (*LocalRestorePin, error) {
	if owner == "" || lease <= 0 {
		return nil, fmt.Errorf("Local restore pin requires owner and positive lease")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	pin := &LocalRestorePin{}
	err := a.localManager().WithLocalMaintenanceContext(ctx, func(ctx context.Context) error {
		if err := a.checkLocalHead(ctx); err != nil {
			return err
		}
		snapshot := selected
		if snapshot == nil {
			snapshot = a.head.Snapshot
		}
		allowed := false
		if snapshot != nil && a.head.Snapshot != nil {
			want, _ := json.Marshal(snapshot)
			got, _ := json.Marshal(a.head.Snapshot)
			allowed = bytes.Equal(want, got)
		}
		for _, old := range a.head.Catalog {
			if snapshot == nil {
				break
			}
			want, _ := json.Marshal(snapshot)
			got, _ := json.Marshal(old)
			if bytes.Equal(want, got) {
				allowed = true
			}
		}
		if snapshot != nil && !allowed {
			return fmt.Errorf("Local restore root is not committed")
		}
		// Every seal artifact in this captured suffix is needed by staged import,
		// even when it has not become the HEAD snapshot yet.
		dependencies := map[[32]byte]LocalSnapshot{}
		if snapshot != nil {
			dependencies[snapshot.PortableRoot] = *snapshot
		}
		for _, extent := range a.extents {
			for _, value := range extent.Decisions {
				seal, ok, err := quepaxa.DecodeCheckpointSeal(value.Value)
				if err != nil {
					return err
				}
				if !ok {
					continue
				}
				artifact, err := a.ReadSnapshot(ctx, seal.RootHash)
				if err != nil {
					return err
				}
				if err := a.validateSnapshot(artifact, false); err != nil {
					return err
				}
				dependencies[artifact.PortableRoot] = artifact
			}
		}
		manager := a.localManager()
		for hash, artifact := range dependencies {
			root, err := manager.OpenRoot(ctx, uint64(artifact.Seal.Index), hash)
			if err != nil {
				return err
			}
			part, err := manager.PinRecoveryRoot(ctx, root, fmt.Sprintf("%s/%x", owner, hash), lease)
			if err != nil {
				return err
			}
			pin.parts = append(pin.parts, part)
		}
		var token [16]byte
		if _, err := rand.Read(token[:]); err != nil {
			return err
		}
		key := fmt.Sprintf("view-pins/%x.json", sha256.Sum256([]byte(owner)))
		options := []objstore.ObjectUploadOption{objstore.WithIfNotExists()}
		existing, version, err := a.readLocalView(ctx, key)
		if err == nil {
			if existing.LeaseUntilMS > time.Now().UnixMilli() {
				return checkpoint.ErrPublisherBusy
			}
			options = []objstore.ObjectUploadOption{objstore.WithIfMatch(version)}
		} else if !a.bucket.IsObjNotFoundErr(err) {
			return err
		}
		headData, err := json.Marshal(a.head)
		if err != nil {
			return err
		}
		record := localViewRecord{Version: 1, BindingHash: a.head.BindingHash, Token: hex.EncodeToString(token[:]), HeadHash: sha256.Sum256(headData), LeaseUntilMS: time.Now().Add(lease).UnixMilli()}
		if err := a.writeLocalView(ctx, key, record, options...); err != nil {
			return err
		}
		pin.archive, pin.key, pin.token = a, key, record.Token
		return pin.validate(ctx, lease)
	})
	if err != nil {
		return nil, pin.reject(err)
	}
	if err := pin.validate(ctx, lease); err != nil {
		return nil, pin.reject(err)
	}
	return pin, err
}

func (p *LocalRestorePin) reject(cause error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return errors.Join(cause, p.Close(ctx))
}

func (p *LocalRestorePin) validate(ctx context.Context, lease time.Duration) error {
	earliest, err := checkpoint.LocalRecoveryPinDeadline(ctx, p.parts)
	if err != nil {
		return err
	}
	record, _, err := p.archive.readLocalView(ctx, p.key)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	minimum := time.Now().Add(lease - lease/4)
	if record.Token != p.token || time.UnixMilli(record.LeaseUntilMS).Before(minimum) || !earliest.IsZero() && earliest.Before(minimum) {
		return checkpoint.ErrPublisherFenced
	}
	return nil
}

// LocalRestorePin protects all portable roots referenced by a captured HEAD.
// Every part must renew successfully before the caller may continue restoring.
type LocalRestorePin struct {
	mu         sync.Mutex
	parts      []*checkpoint.RecoveryPin
	archive    *LocalArchive
	key, token string
	closed     bool
}

func (p *LocalRestorePin) Renew(ctx context.Context, lease time.Duration) error {
	if p == nil {
		return checkpoint.ErrPublisherFenced
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.closed || lease <= 0 {
		return checkpoint.ErrPublisherFenced
	}
	for _, part := range p.parts {
		if err := part.Renew(ctx, lease); err != nil {
			return err
		}
	}
	if err := p.updateView(ctx, lease, false); err != nil {
		return err
	}
	return p.validate(ctx, lease)
}

func (p *LocalRestorePin) Close(ctx context.Context) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	for _, part := range p.parts {
		if err := part.Close(ctx); err != nil {
			return err
		}
	}
	if p.key != "" {
		if err := p.updateView(ctx, 0, true); err != nil {
			return err
		}
	}
	p.closed = true
	return nil
}

type localViewRecord struct {
	Version      uint     `json:"version"`
	BindingHash  [32]byte `json:"binding_hash"`
	HeadHash     [32]byte `json:"head_hash"`
	Token        string   `json:"token"`
	LeaseUntilMS int64    `json:"lease_until_unix_ms"`
}

func (a *LocalArchive) readLocalView(ctx context.Context, key string) (localViewRecord, *objstore.ObjectVersion, error) {
	var record localViewRecord
	before, err := a.bucket.Attributes(ctx, a.key(key))
	if err != nil {
		return record, nil, err
	}
	data, err := a.read(ctx, key, 4096)
	if err != nil {
		return record, nil, err
	}
	after, err := a.bucket.Attributes(ctx, a.key(key))
	if err != nil {
		return record, nil, err
	}
	if before.Version == nil || !sameObjectVersion(before.Version, after.Version) {
		return record, nil, fmt.Errorf("Local view pin changed during read")
	}
	if err := strictLocalJSON(data, &record); err != nil {
		return record, nil, err
	}
	binding, _ := json.Marshal(a.Binding)
	token, err := hex.DecodeString(record.Token)
	if err != nil || len(token) != 16 || record.Version != 1 || record.BindingHash != sha256.Sum256(binding) || record.HeadHash == ([32]byte{}) || record.LeaseUntilMS < 0 {
		return record, nil, fmt.Errorf("invalid Local view pin")
	}
	return record, after.Version, nil
}
func (a *LocalArchive) writeLocalView(ctx context.Context, key string, record localViewRecord, options ...objstore.ObjectUploadOption) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if err := a.bucket.Upload(ctx, a.key(key), bytes.NewReader(data), options...); err != nil {
		return err
	}
	got, _, err := a.readLocalView(ctx, key)
	if err != nil {
		return err
	}
	if got != record {
		return fmt.Errorf("Local view pin readback mismatch")
	}
	return nil
}
func (p *LocalRestorePin) updateView(ctx context.Context, lease time.Duration, close bool) error {
	if p.archive == nil {
		return checkpoint.ErrPublisherFenced
	}
	record, version, err := p.archive.readLocalView(ctx, p.key)
	if err != nil {
		return err
	}
	if record.Token != p.token || !close && record.LeaseUntilMS <= time.Now().UnixMilli() {
		return checkpoint.ErrPublisherFenced
	}
	record.LeaseUntilMS = 0
	if !close {
		record.LeaseUntilMS = time.Now().Add(lease).UnixMilli()
	}
	return p.archive.writeLocalView(ctx, p.key, record, objstore.WithIfMatch(version))
}
func (a *LocalArchive) activeLocalViews(ctx context.Context) (bool, error) {
	active := false
	err := a.bucket.Iter(ctx, a.key("view-pins"), func(name string) error {
		base := path.Base(name)
		if len(base) != 69 || base[64:] != ".json" {
			return fmt.Errorf("unknown Local view pin key")
		}
		if decoded, err := hex.DecodeString(base[:64]); err != nil || len(decoded) != 32 {
			return fmt.Errorf("invalid Local view pin key")
		}
		key := path.Join("view-pins", path.Base(name))
		record, version, err := a.readLocalView(ctx, key)
		if err != nil {
			return err
		}
		if record.LeaseUntilMS > time.Now().UnixMilli() {
			active = true
			return nil
		}
		if record.LeaseUntilMS == 0 {
			return nil
		}
		record.LeaseUntilMS = 0
		return a.writeLocalView(ctx, key, record, objstore.WithIfMatch(version))
	})
	return active, err
}
