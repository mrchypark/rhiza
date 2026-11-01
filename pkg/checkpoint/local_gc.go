package checkpoint

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	internalobjstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/thanos-io/objstore"
)

var ErrLocalMaintenanceBusy = errors.New("Local maintenance busy; retry after holder completes")

type localMaintenanceControl struct {
	Version      uint   `json:"version"`
	Token        string `json:"token"`
	LocationHash string `json:"location_hash"`
	BindingHash  string `json:"binding_hash"`
}

func (m *Manager) SetLocalControlBinding(locationHash, bindingHash string) {
	m.localControlLocationHash = locationHash
	m.localControlBindingHash = bindingHash
}

// LocalRootExists checks the canonical immutable root while the Local gate is held.
func (m *Manager) LocalRootExists(ctx context.Context, index uint64, hash [32]byte) (bool, error) {
	return m.bucket.Exists(ctx, m.key(rootName(index, hash)))
}

// LocalRecoveryActive is called under the Local maintenance gate. Archive GC
// conservatively keeps every extent while a captured restore view is active.
func (m *Manager) LocalRecoveryActive(ctx context.Context) (bool, error) {
	roots, err := m.activeRecoveryRoots(ctx)
	return len(roots) != 0, err
}

// WithLocalMaintenanceContext serializes Local writes, pins, and deletion.
// Cancellation stops RPC admission at acknowledgement boundaries. Ambiguous
// mutations keep the gate held without automatic takeover; recovery requires
// verified writer termination and resolution of outstanding storage operations.
func (m *Manager) WithLocalMaintenanceContext(ctx context.Context, work func(context.Context) error) error {
	if m.appendOnly {
		return fmt.Errorf("Local maintenance is disabled for append-only manager")
	}
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		err := m.withLocalMaintenanceContextOnce(ctx, work)
		if !errors.Is(err, ErrLocalMaintenanceBusy) {
			return err
		}
		retry := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			retry.Stop()
			return ctx.Err()
		case <-timer.C:
			retry.Stop()
			return err
		case <-retry.C:
		}
	}
}

func (m *Manager) withLocalMaintenanceContextOnce(ctx context.Context, work func(context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	requestCtx := ctx
	admission := &localOperation{request: context.WithoutCancel(ctx), timeout: localRPCTimeout}
	ctx = context.WithValue(context.WithoutCancel(ctx), localOperationKey{}, admission)
	controlVersion := types.LocalArchiveFormatVersion
	gate := NewManager(LocalBucket(m.bucket), m.prefix, "", m.configID)
	gate.localControlLocationHash = m.localControlLocationHash
	gate.localControlBindingHash = m.localControlBindingHash
	key := m.key("local-gc-control")
	data, version, err := gate.readStableLeaseObject(ctx, key, 4096)
	var state localMaintenanceControl
	options := []objstore.ObjectUploadOption{objstore.WithIfNotExists()}
	if err == nil {
		if err := decodePersistedJSON(data, &state); err != nil {
			return err
		}
		if state.Version != controlVersion || state.LocationHash != m.localControlLocationHash || state.BindingHash != m.localControlBindingHash {
			return fmt.Errorf("invalid Local maintenance control")
		}
		if state.Token != "" {
			return ErrLocalMaintenanceBusy
		}
		options = []objstore.ObjectUploadOption{objstore.WithIfMatch(version)}
	} else if !m.bucket.IsObjNotFoundErr(err) {
		return err
	} else if m.localControlBindingHash != "" {
		return fmt.Errorf("Local maintenance control missing")
	}
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return err
	}
	state = localMaintenanceControl{Version: controlVersion, Token: hex.EncodeToString(token[:]), LocationHash: m.localControlLocationHash, BindingHash: m.localControlBindingHash}
	encoded, _ := json.Marshal(state)
	if err := gate.bucket.Upload(ctx, key, bytes.NewReader(encoded), options...); err != nil {
		if m.bucket.IsConditionNotMetErr(err) {
			return ErrLocalMaintenanceBusy
		}
		return err
	}
	got, heldVersion, err := gate.readStableLeaseObject(ctx, key, 4096)
	if err != nil {
		return err
	}
	if !bytes.Equal(got, encoded) {
		return fmt.Errorf("Local maintenance readback mismatch")
	}
	var workErr error
	operation := &localOperation{request: requestCtx, timeout: localRPCTimeout}
	ctx = context.WithValue(ctx, localOperationKey{}, operation)
	if requestCtx.Err() == nil {
		workErr = work(ctx)
	}
	if operation.uncertain.Load() {
		return fmt.Errorf("Local maintenance remains held after uncertain work; verify writer termination and outstanding operations before recovery: %w", workErr)
	}
	// The callback must await every storage operation before releasing the gate.
	releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	free, _ := json.Marshal(localMaintenanceControl{Version: controlVersion, LocationHash: m.localControlLocationHash, BindingHash: m.localControlBindingHash})
	releaseCtx = context.WithValue(releaseCtx, localOperationKey{}, &localOperation{request: context.Background(), timeout: 5 * time.Second})
	if err := gate.bucket.Upload(releaseCtx, key, bytes.NewReader(free), objstore.WithIfMatch(heldVersion)); err != nil {
		// A lost release ACK is settled only by a stable, exact free-state readback.
		verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer verifyCancel()
		verifyCtx = context.WithValue(verifyCtx, localOperationKey{}, &localOperation{request: verifyCtx, timeout: 5 * time.Second})
		verifyCtx = context.WithValue(verifyCtx, localReadbackKey{}, true)
		got, version, readErr := gate.readStableLeaseObject(verifyCtx, key, 4096)
		if readErr != nil || version == nil || heldVersion == nil || *version == *heldVersion || !bytes.Equal(got, free) {
			return fmt.Errorf("Local maintenance release (work: %v; readback: %v): %w", workErr, readErr, err)
		}
	}
	if workErr != nil {
		return workErr
	}
	return requestCtx.Err()
}

// RecoverLocalMaintenanceControl is the sole bound held-token recovery operation.
// Callers must finish owner-authorized read-only validation before invoking it.
func (m *Manager) RecoverLocalMaintenanceControl(ctx context.Context, owner *internalobjstore.LocalFilesystemOwner) error {
	if m.localControlBindingHash == "" || owner == nil || !owner.Held() || owner.LocationHash(m.prefix) != m.localControlLocationHash {
		return fmt.Errorf("Local control recovery requires the filesystem owner")
	}
	gate := NewManager(LocalBucket(m.bucket), m.prefix, "", m.configID)
	gate.SetLocalControlBinding(m.localControlLocationHash, m.localControlBindingHash)
	key := m.key("local-gc-control")
	data, version, err := gate.readStableLeaseObject(ctx, key, 4096)
	if err != nil {
		return fmt.Errorf("read Local maintenance control: %w", err)
	}
	var state localMaintenanceControl
	if err := decodePersistedJSON(data, &state); err != nil || state.Version != types.LocalArchiveFormatVersion || state.LocationHash != m.localControlLocationHash || state.BindingHash != m.localControlBindingHash {
		return fmt.Errorf("invalid Local maintenance control")
	}
	if state.Token == "" {
		return nil
	}
	free, err := json.Marshal(localMaintenanceControl{Version: types.LocalArchiveFormatVersion, LocationHash: m.localControlLocationHash, BindingHash: m.localControlBindingHash})
	if err != nil {
		return err
	}
	if err := gate.bucket.Upload(ctx, key, bytes.NewReader(free), objstore.WithIfMatch(version)); err != nil {
		return fmt.Errorf("release recovered Local maintenance control: %w", err)
	}
	got, _, err := gate.readStableLeaseObject(ctx, key, 4096)
	if err != nil {
		return fmt.Errorf("Local maintenance recovery readback: %w", err)
	}
	if !bytes.Equal(got, free) {
		return fmt.Errorf("Local maintenance recovery readback mismatch")
	}
	return nil
}

func (m *Manager) LocalMaintenanceResolved(ctx context.Context) (bool, error) {
	if m.localControlBindingHash == "" {
		return true, nil
	}
	data, _, err := m.readStableLeaseObject(ctx, m.key("local-gc-control"), 4096)
	if err != nil {
		return false, err
	}
	var state localMaintenanceControl
	if err := decodePersistedJSON(data, &state); err != nil || state.Version != types.LocalArchiveFormatVersion || state.LocationHash != m.localControlLocationHash || state.BindingHash != m.localControlBindingHash {
		return false, fmt.Errorf("invalid Local maintenance control")
	}
	return state.Token == "", nil
}

// GarbageCollectLocal is called inside WithLocalMaintenanceContext. Only explicitly
// retired committed roots are deleted; prepared roots remain publication-safe.
func (m *Manager) GarbageCollectLocal(ctx context.Context, retired, retain map[[32]byte]struct{}, grace time.Duration) error {
	if m.appendOnly {
		return fmt.Errorf("Local checkpoint garbage collection is disabled for append-only manager")
	}
	if _, wrapped := m.bucket.(*localBucket); !wrapped {
		return NewManager(LocalBucket(m.bucket), m.prefix, m.localDir, m.configID).GarbageCollectLocal(ctx, retired, retain, grace)
	}
	if grace < 0 {
		return fmt.Errorf("invalid Local GC grace")
	}
	if err := m.bucket.Iter(ctx, m.key("checkpoint/roots"), func(name string) error {
		index, hash, ok := parseRootKey(name)
		if !ok || name != m.key(rootName(index, hash)) {
			return fmt.Errorf("unknown Local checkpoint root: %s", name)
		}
		return nil
	}, objstore.WithRecursiveIter()); err != nil {
		return err
	}
	pinned, err := m.activeRecoveryRoots(ctx)
	if err != nil {
		return err
	}
	if err := m.loadAll(ctx); err != nil {
		return err
	}
	m.mu.Lock()
	roots := append([]Checkpoint(nil), m.checkpoints...)
	m.mu.Unlock()
	live := map[string]bool{}
	var remove []string
	cutoff := time.Now().Add(-grace)
	for _, root := range roots {
		_, obsolete := retired[root.RootHash]
		_, keep := retain[root.RootHash]
		_, pin := pinned[root.RootHash]
		name := m.key(rootName(root.Index, root.RootHash))
		attrs, err := m.bucket.Attributes(ctx, name)
		if err != nil {
			return err
		}
		if obsolete && !keep && !pin && !attrs.LastModified.IsZero() && attrs.LastModified.Before(cutoff) {
			remove = append(remove, name)
		} else {
			for _, file := range root.Files {
				for _, block := range file.Blocks {
					live[m.key(blockObjectKey(block))] = true
				}
			}
		}
	}
	for _, root := range pinned {
		for _, file := range root.Files {
			for _, block := range file.Blocks {
				live[m.key(blockObjectKey(block))] = true
			}
		}
	}
	// Validate the complete listing before the first destructive operation.
	var blocks []string
	if err := m.bucket.Iter(ctx, m.key("checkpoint/blocks"), func(name string) error {
		hash, ok := parseBlockKey(name)
		canonical := blockKey(hash)
		_, generation, suffixed := strings.Cut(strings.TrimSuffix(path.Base(name), ".block"), "_")
		if suffixed {
			value, err := strconv.ParseUint(generation, 10, 64)
			if err != nil {
				return nil
			}
			canonical = blockGenerationKey(hash, value)
		}
		if !ok || name != m.key(canonical) || live[name] {
			return nil
		}
		attrs, err := m.bucket.Attributes(ctx, name)
		if err != nil {
			return err
		}
		if !attrs.LastModified.IsZero() && attrs.LastModified.Before(cutoff) {
			blocks = append(blocks, name)
		}
		return nil
	}, objstore.WithRecursiveIter()); err != nil {
		return err
	}
	for _, name := range append(remove, blocks...) {
		if err := m.bucket.Delete(ctx, name); err != nil && !m.bucket.IsObjNotFoundErr(err) {
			return err
		}
	}
	return nil
}

// LocalRecoveryPinDeadline validates every identity without extending leases.
// The caller must compare the returned minimum again after other admission RPCs.
func LocalRecoveryPinDeadline(ctx context.Context, pins []*RecoveryPin) (time.Time, error) {
	var earliest time.Time
	for _, pin := range pins {
		if pin == nil || pin.manager == nil {
			return time.Time{}, ErrPublisherFenced
		}
		record, err := pin.manager.readRecoveryPin(ctx, pin.key)
		if err != nil {
			return time.Time{}, err
		}
		expected := pin.record
		if record.ConfigID != expected.ConfigID || record.OwnerID != expected.OwnerID || record.Token != expected.Token || record.Index != expected.Index || record.RootHash != expected.RootHash {
			return time.Time{}, ErrPublisherFenced
		}
		deadline := time.UnixMilli(record.LeaseUntilMS)
		if earliest.IsZero() || deadline.Before(earliest) {
			earliest = deadline
		}
	}
	if !earliest.IsZero() && !earliest.After(time.Now()) {
		return time.Time{}, ErrPublisherFenced
	}
	return earliest, nil
}
