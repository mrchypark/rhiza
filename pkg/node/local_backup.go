package node

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
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/ncruces/go-sqlite3/driver"
)

func (n *Node) localBackupPrefix() string {
	return path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID), "local/v1")
}

func validateLocalFilesystemArchiveNamespace(root, prefix, clusterID string) error {
	if path.IsAbs(prefix) || filepath.IsAbs(prefix) || path.IsAbs(clusterID) || filepath.IsAbs(clusterID) {
		return fmt.Errorf("filesystem-v2 Local archive prefix and cluster ID must be relative")
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	rootReal, rootErr := filepath.EvalSymlinks(rootAbs)
	if rootErr == nil {
		info, err := os.Stat(rootReal)
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return fmt.Errorf("filesystem-v2 Local archive root is not a directory")
		}
	} else if !errors.Is(rootErr, os.ErrNotExist) {
		return fmt.Errorf("resolve filesystem-v2 Local archive root: %w", rootErr)
	}
	namespace := path.Join(prefix, clusterID, "local/v1")
	clean := path.Clean(namespace)
	if path.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, "../") {
		return fmt.Errorf("filesystem-v2 Local archive namespace escapes its root")
	}
	archivePath := filepath.Join(rootAbs, filepath.FromSlash(clean))
	rel, err := filepath.Rel(rootAbs, archivePath)
	if err != nil {
		return err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return fmt.Errorf("filesystem-v2 Local archive namespace escapes its root")
	}
	if rootErr == nil {
		if err := rejectLocalArchiveSymlinks(rootReal, clean); err != nil {
			return err
		}
	}
	return nil
}

func rejectLocalArchiveSymlinks(root, namespace string) error {
	current := root
	parts := strings.Split(filepath.FromSlash(namespace), string(filepath.Separator))
	for i, part := range parts {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("filesystem-v2 Local archive namespace contains a symlink")
		}
		if i < len(parts)-1 && !info.IsDir() {
			return fmt.Errorf("filesystem-v2 Local archive namespace contains a non-directory path component")
		}
		if i == len(parts)-1 {
			if !info.IsDir() {
				return fmt.Errorf("filesystem-v2 Local archive namespace is not a directory")
			}
			return filepath.WalkDir(current, func(name string, entry os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if entry.Type()&os.ModeSymlink != 0 {
					return fmt.Errorf("filesystem-v2 Local archive namespace contains a symlink")
				}
				return nil
			})
		}
	}
	return nil
}

func (n *Node) localRestoreArchive(archive *recovery.LocalArchive) *recovery.LocalArchive {
	if n.localOwner != nil {
		return archive.ReadOnly()
	}
	return archive
}

func (n *Node) openLocalBackupBucket(ctx context.Context) error {
	c := n.config
	provider := objectstore.Provider(c.ObjStoreProvider)
	if provider == "" {
		if c.ObjStoreDir != "" {
			provider = objectstore.ProviderFilesystem
		} else {
			provider = objectstore.ProviderS3
		}
	}
	filesystemDir := c.ObjStoreDir
	var owner *objectstore.LocalFilesystemOwner
	if c.LocalArchiveProfile == types.LocalArchiveProfileFilesystem {
		if err := validateLocalFilesystemArchiveNamespace(filesystemDir, c.ObjStorePrefix, string(c.ClusterID)); err != nil {
			return err
		}
		var err error
		owner, err = objectstore.AcquireLocalFilesystemOwner(filesystemDir, c.DataDir)
		if err != nil {
			return err
		}
		filesystemDir = owner.RootDir()
	}
	bucket, err := objectstore.NewBucketWithContext(ctx, objectstore.Config{
		Provider: provider, FilesystemDir: filesystemDir, Prefix: c.ObjStorePrefix,
		Endpoint: c.ObjStoreEndpoint, Bucket: c.ObjStoreBucket, Region: c.ObjStoreRegion, Insecure: c.ObjStoreInsecure, MaxRetries: c.ObjStoreRetries,
		AccessKey: c.ObjStoreAccessKey, SecretKey: c.ObjStoreSecretKey, SessionToken: c.ObjStoreSessionToken, ServiceAccount: c.ObjStoreServiceAccount,
		AzureTenantID: c.ObjStoreAzureTenantID, AzureClientID: c.ObjStoreAzureClientID, AzureClientSecret: c.ObjStoreAzureClientSecret,
		AzureStorageAccount: c.ObjStoreAzureStorageAccount, AzureStorageAccountKey: c.ObjStoreAzureStorageAccountKey, AzureConnectionString: c.ObjStoreAzureConnectionString, AzureUserAssignedID: c.ObjStoreAzureUserAssignedID,
	})
	if err != nil {
		if owner != nil {
			_ = owner.Close(time.Second)
		}
		return err
	}
	if owner != nil {
		bucket.Bucket = owner.Wrap(bucket.Bucket)
		n.localOwner = owner
	}
	n.bucket = bucket
	return nil
}

// prepareLocalBackup runs with the original stable qlog lock held, before
// identity creation or writable recovery. The supplied deployment guarantees
// exclusive remote ownership, including during offline recovery.
func (n *Node) prepareLocalBackup(ctx context.Context) (resultErr error) {
	if err := n.openLocalBackupBucket(ctx); err != nil {
		return err
	}
	if err := n.finishLocalBackupImport(); err != nil {
		return err
	}
	bindingKey := path.Join(n.localBackupPrefix(), "BINDING")
	exists, err := n.bucket.Exists(ctx, bindingKey)
	if err != nil {
		return err
	}
	marker := filepath.Join(n.config.DataDir, "local-backup.json")
	saved, markerErr := os.ReadFile(marker)
	if markerErr != nil && !errors.Is(markerErr, os.ErrNotExist) {
		return markerErr
	}
	if !exists {
		if markerErr == nil {
			return fmt.Errorf("previously bound Local archive binding is missing; preserve local data")
		}
		headExists, err := n.bucket.Exists(ctx, path.Join(n.localBackupPrefix(), "HEAD"))
		if err != nil {
			return err
		}
		if headExists {
			return fmt.Errorf("unbound Local archive head exists")
		}
		nonempty := false
		if err := n.bucket.Iter(ctx, n.localBackupPrefix()+"/", func(string) error { nonempty = true; return nil }); err != nil {
			return err
		}
		if nonempty {
			return fmt.Errorf("unbound Local archive namespace is not empty")
		}
		return nil
	}
	var archive *recovery.LocalArchive
	if n.localOwner != nil {
		archive, err = recovery.OpenLocalArchiveOwned(ctx, n.bucket, n.localBackupPrefix(), n.localOwner)
	} else {
		archive, err = recovery.OpenLocalArchive(ctx, n.bucket, n.localBackupPrefix())
	}
	if err != nil {
		return err
	}
	if n.config.LocalArchiveProfile == types.LocalArchiveProfileFilesystem {
		if archive.Binding.Version != 2 || archive.Binding.ArchiveLocationHash != n.localOwner.LocationHash(n.localBackupPrefix()) {
			return fmt.Errorf("Local archive profile or filesystem location differs; automatic migration is unsupported")
		}
	} else if archive.Binding.Version != 1 {
		return fmt.Errorf("Local archive requires its explicit filesystem-v2 profile")
	}
	maintenanceResolved := true
	if n.localOwner != nil {
		maintenanceResolved, err = archive.MaintenanceResolved(ctx)
		if err != nil {
			return err
		}
	}
	bindingData, err := json.Marshal(archive.Binding)
	if err != nil {
		return err
	}
	if markerErr == nil && !bytes.Equal(saved, bindingData) {
		return fmt.Errorf("Local archive binding differs from previous attachment")
	}
	policyPath := filepath.Join(n.config.DataDir, "local-store.json")
	_, policyErr := os.Lstat(policyPath)
	if policyErr != nil && !errors.Is(policyErr, os.ErrNotExist) {
		return policyErr
	}
	n.localBackup = archive
	if errors.Is(policyErr, os.ErrNotExist) {
		if localDataExists(n.config.DataDir) {
			return fmt.Errorf("Local data has no policy; preserve existing data")
		}
		if err := n.stageLocalBackupImport(ctx, n.localRestoreArchive(archive), n.localOwner); err != nil {
			return err
		}
		return n.recoverStagedLocalMaintenance(ctx, archive, maintenanceResolved)
	}
	identity, err := openLocalCheckpointIdentity(n.config)
	if err != nil {
		return err
	}
	n.localStore = &identity
	if identity.policy.StoreUUID != archive.Binding.StoreUUID || identity.configDigest != archive.Binding.ConfigDigest {
		return fmt.Errorf("Local archive belongs to a different store or configuration")
	}
	wal, err := qlog.OpenReadOnly(filepath.Join(n.config.DataDir, "qlog"))
	if err != nil {
		return err
	}
	limit := n.config.MaxWALBytes
	if limit == 0 {
		limit = 4 << 30
	}
	if err := wal.SetMaxBytes(limit); err != nil {
		_ = wal.Close()
		return err
	}
	core, err := quepaxa.New(quepaxa.Config{NodeID: n.config.NodeID, Cluster: *n.loadClusterConfig(), WAL: wal, LocalMode: true})
	if err == nil {
		err = n.validateLocalRoots(core)
	}
	if err != nil {
		_ = wal.Close()
		return err
	}
	// Pending recorder state belongs to local recovery; never replace it with a
	// remote snapshot that could erase uncertainty from an interrupted write.
	if core.RecorderTip() > core.Tip() {
		if err := wal.Close(); err != nil {
			return err
		}
		wal, err = qlog.OpenLocalDeferredCleanup(filepath.Join(n.config.DataDir, "qlog"))
		if err != nil {
			return err
		}
		if err := wal.SetMaxBytes(limit); err != nil {
			_ = wal.Close()
			return err
		}
		core, err = quepaxa.New(quepaxa.Config{NodeID: n.config.NodeID, Cluster: *n.loadClusterConfig(), WAL: wal, LocalMode: true})
		if err == nil {
			err = n.validateLocalRoots(core)
		}
		if err == nil {
			err = core.RecoverThrough(ctx, core.RecorderTip())
		}
		if err != nil {
			_ = wal.Close()
			return fmt.Errorf("recover original Local recorder before archive reconciliation: %w", err)
		}
	}
	common := min(core.Tip(), archive.Tip())
	localPrefix, localOK := core.PrefixHash(common)
	if common == 0 {
		localPrefix, localOK = [32]byte{}, true
	}
	remotePrefix, remoteOK := archive.PrefixHash(common)
	if !localOK || !remoteOK || localPrefix != remotePrefix {
		_ = wal.Close()
		return fmt.Errorf("Local and remote histories diverge or prefix proof is unavailable")
	}
	localTip := core.Tip()
	if archive.Tip() > localTip {
		if err := n.validateLocalBackupMaterialized(ctx, core); err != nil {
			return errors.Join(err, wal.Close())
		}
	}
	if err := wal.Close(); err != nil {
		return err
	}
	if archive.Tip() > localTip || !maintenanceResolved && archive.Tip() == localTip {
		if err := n.stageLocalBackupImport(ctx, n.localRestoreArchive(archive), n.localOwner); err != nil {
			return err
		}
		return n.recoverStagedLocalMaintenance(ctx, archive, maintenanceResolved)
	}
	if !maintenanceResolved {
		return fmt.Errorf("v2 Local maintenance is held; startup cannot clear it without staged archive replay")
	}
	n.localBackupNeedsSnapshot = localTip > archive.Tip()
	if markerErr == nil {
		return nil
	}
	return atomicLocalFile(n.config.DataDir, marker, bindingData)
}

// A held v2 token is cleared only after the archive has passed the same full
// staged replay and install path used for restoring a lost or behind DataDir.
func (n *Node) recoverStagedLocalMaintenance(ctx context.Context, archive *recovery.LocalArchive, alreadyResolved bool) error {
	if n.localOwner == nil || alreadyResolved {
		return nil
	}
	return archive.RecoverMaintenanceControl(ctx, n.localOwner)
}

// Inspect a copy, including any uncheckpointed SQLite WAL, so even SQLite's
// read-only sidecar handling cannot change the surviving evidence.
func (n *Node) validateLocalBackupMaterialized(ctx context.Context, core *quepaxa.Core) error {
	source := filepath.Join(n.config.DataDir, "sqlite.db")
	if _, err := os.Lstat(source); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "rhiza-local-materialized-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	for _, suffix := range []string{"", "-wal"} {
		input, err := os.Open(source + suffix)
		if suffix != "" && errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		output, err := os.OpenFile(filepath.Join(dir, "sqlite.db")+suffix, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return errors.Join(err, input.Close())
		}
		_, copyErr := io.Copy(output, input)
		if err := errors.Join(copyErr, input.Close(), output.Close()); err != nil {
			return err
		}
	}
	db, err := driver.Open((&url.URL{Scheme: "file", Path: filepath.ToSlash(filepath.Join(dir, "sqlite.db"))}).String() + "?mode=ro")
	if err != nil {
		return err
	}
	defer db.Close()
	var slotText, hashText string
	if err := db.QueryRowContext(ctx, `SELECT (SELECT value FROM _rhiza_meta WHERE key='applied_slot'), (SELECT value FROM _rhiza_meta WHERE key='applied_hash')`).Scan(&slotText, &hashText); err != nil {
		return fmt.Errorf("inspect original Local materialized evidence: %w", err)
	}
	slot, err := strconv.ParseUint(slotText, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid original Local materialized slot: %w", err)
	}
	hash, err := hex.DecodeString(hashText)
	if err != nil || len(hash) != sha256.Size {
		return fmt.Errorf("invalid original Local materialized hash")
	}
	if quepaxa.Slot(slot) > core.Tip() {
		return fmt.Errorf("original Local materialized slot %d is ahead of certified log tip %d; preserve existing data", slot, core.Tip())
	}
	if slot > uint64(core.CompactionFloor()) {
		decision, ok := core.CertifiedValue(quepaxa.Slot(slot))
		if !ok {
			return fmt.Errorf("original Local materialized slot %d has no certified decision", slot)
		}
		want := sha256.Sum256(decision.Value)
		if !bytes.Equal(hash, want[:]) {
			return fmt.Errorf("original Local materialized slot %d hash conflicts with certified decision", slot)
		}
	}
	return nil
}

func (n *Node) initializeLocalBackup(ctx context.Context) error {
	n.localSnapshotMu.Lock()
	defer n.localSnapshotMu.Unlock()
	return n.initializeLocalBackupLocked(ctx)
}

func (n *Node) initializeLocalBackupLocked(ctx context.Context) error {
	if n.localBackup == nil {
		binding := recovery.LocalBinding{Version: 1, StoreUUID: n.localStore.policy.StoreUUID, ConfigDigest: n.localStore.configDigest, PolicyVersion: localCheckpointVersion, Bootstrap: *n.loadClusterConfig()}
		if n.config.LocalArchiveProfile == types.LocalArchiveProfileFilesystem {
			binding.Version = 2
			binding.ArchiveLocationHash = n.localOwner.LocationHash(n.localBackupPrefix())
		}
		// Persist intent first. After any uncertain genesis, absence is an error,
		// rather than authorization to silently initialize another archive.
		data, err := json.Marshal(binding)
		if err != nil {
			return err
		}
		if err := atomicLocalFile(n.config.DataDir, filepath.Join(n.config.DataDir, "local-backup.json"), data); err != nil {
			return err
		}
		var genesis *recovery.LocalArchiveGenesis
		if n.localOwner != nil {
			genesis, err = recovery.PrepareLocalArchiveOwned(ctx, n.bucket, n.localBackupPrefix(), binding, n.localOwner)
			if err != nil {
				return err
			}
		}
		var base *recovery.LocalSnapshot
		if n.core.CompactionFloor() > 0 {
			seal, ok, err := n.core.LatestCheckpointSeal()
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("Local floor has no certified checkpoint seal")
			}
			snapshot, err := n.uploadLocalBackupSnapshot(ctx, seal.CheckpointSeal, seal.DecisionSlot)
			if err != nil {
				return err
			}
			base = &snapshot
		}
		var archive *recovery.LocalArchive
		if n.localOwner != nil {
			archive, err = recovery.CreateLocalArchiveFromPreparedOwned(ctx, genesis, base, n.localOwner)
		} else {
			archive, err = recovery.CreateLocalArchive(ctx, n.bucket, n.localBackupPrefix(), binding, base)
		}
		if err != nil {
			return err
		}
		n.localBackup = archive
		n.localBackupNeedsSnapshot = n.core.Tip() > 0 && base == nil
	}
	if err := n.syncLocalBackupLocked(ctx); err != nil {
		return err
	}
	n.localBackupReady = true
	return nil
}

// The caller holds localSnapshotMu from before sealing through publication.
func (n *Node) publishLocalBackupCheckpoint(ctx context.Context, seal quepaxa.CheckpointSeal, slot quepaxa.Slot) error {
	if n.bucket == nil || !n.config.Local {
		return nil
	}
	snapshot, err := n.uploadLocalBackupSnapshot(ctx, seal, slot)
	if err != nil {
		return err
	}
	if err := n.initializeLocalBackupLocked(ctx); err != nil {
		return err
	}
	err = retryLocalMaintenance(ctx, func() error {
		return n.localBackup.PublishSnapshot(ctx, snapshot)
	})
	if err != nil {
		return err
	}
	hitLocalNodeCheckpointCrashBoundary("after-local-backup-published-before-compaction")
	n.localBackupNeedsSnapshot = false
	return nil
}

func (n *Node) uploadLocalBackupSnapshot(ctx context.Context, seal quepaxa.CheckpointSeal, decisionSlot quepaxa.Slot) (recovery.LocalSnapshot, error) {
	descriptor, files, err := n.localStore.OpenExpected(quepaxa.LocalCheckpointRoot{RootHash: seal.RootHash, Index: seal.Index, PrefixHash: seal.PrefixHash, StateHash: seal.StateHash, ConfigID: seal.ConfigID})
	if err != nil {
		return recovery.LocalSnapshot{}, err
	}
	decision, ok := n.core.CertifiedValue(decisionSlot)
	if !ok {
		return recovery.LocalSnapshot{}, fmt.Errorf("Local snapshot seal decision is unavailable")
	}
	sources := make([]checkpoint.Source, 0, len(files))
	for _, file := range files {
		sources = append(sources, checkpoint.Source{Role: string(file.Role), Path: file.Path})
	}
	manager := checkpoint.NewManager(n.bucket, n.localBackupPrefix(), "", seal.ConfigID)
	var portable *checkpoint.Checkpoint
	err = retryLocalMaintenance(ctx, func() error {
		var err error
		portable, err = manager.CreateLocalFiles(ctx, sources, uint64(seal.Index))
		return err
	})
	if err != nil {
		return recovery.LocalSnapshot{}, err
	}
	dir, err := os.MkdirTemp(n.config.DataDir, ".local-backup-verify-")
	if err != nil {
		return recovery.LocalSnapshot{}, err
	}
	defer os.RemoveAll(dir)
	downloaded, err := manager.DownloadRootFiles(ctx, uint64(seal.Index), portable.RootHash, dir)
	if err != nil {
		return recovery.LocalSnapshot{}, err
	}
	if err := verifyLocalBackupFiles(descriptor, downloaded); err != nil {
		return recovery.LocalSnapshot{}, err
	}
	encoded, err := json.Marshal(descriptor)
	if err != nil {
		return recovery.LocalSnapshot{}, err
	}
	snapshot := recovery.LocalSnapshot{Seal: seal, Decision: decision, PortableRoot: portable.RootHash, Descriptor: encoded}
	if err := recovery.StoreLocalSnapshot(ctx, n.bucket, n.localBackupPrefix(), snapshot); err != nil {
		return recovery.LocalSnapshot{}, err
	}
	return snapshot, nil
}

func verifyLocalBackupFiles(descriptor localCheckpointDescriptor, files []checkpoint.File) error {
	if len(files) != 2 || len(descriptor.Files) != 2 {
		return fmt.Errorf("Local archive requires both snapshot roles")
	}
	seen := make(map[string]bool)
	for _, file := range files {
		if seen[file.Role] {
			return fmt.Errorf("duplicate Local snapshot role")
		}
		seen[file.Role] = true
		matched := false
		for _, expected := range descriptor.Files {
			if expected.Role == file.Role {
				source, err := os.Open(file.Path)
				if err != nil {
					return err
				}
				h := sha256.New()
				size, copyErr := io.Copy(h, io.LimitReader(source, int64(expected.Length)+1))
				closeErr := source.Close()
				if copyErr != nil || closeErr != nil || size != int64(expected.Length) || fmt.Sprintf("%x", h.Sum(nil)) != expected.SHA256 {
					return fmt.Errorf("Local portable snapshot differs from descriptor")
				}
				matched = true
			}
		}
		if !matched {
			return fmt.Errorf("unexpected Local snapshot role")
		}
	}
	return nil
}

func (n *Node) downloadLocalBackupRoot(ctx context.Context, identity localCheckpointIdentity, snapshot recovery.LocalSnapshot, stage string) error {
	var descriptor localCheckpointDescriptor
	if err := decodeLocalJSON(snapshot.Descriptor, &descriptor); err != nil {
		return err
	}
	manager := checkpoint.NewManager(n.bucket, n.localBackupPrefix(), "", snapshot.Seal.ConfigID)
	download, err := os.MkdirTemp(stage, ".download-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(download)
	files, err := manager.DownloadRootFiles(ctx, uint64(snapshot.Seal.Index), snapshot.PortableRoot, download)
	if err != nil {
		return err
	}
	if err := verifyLocalBackupFiles(descriptor, files); err != nil {
		return err
	}
	materialFiles := make([]materializer.CheckpointFile, 0, 2)
	for _, file := range files {
		materialFiles = append(materialFiles, materializer.CheckpointFile{Role: materializer.CheckpointRole(file.Role), Path: file.Path})
	}
	published, err := identity.Publish(ctx, materialFiles, uint64(snapshot.Seal.Index), snapshot.Seal.PrefixHash, snapshot.Seal.ConfigID)
	if err != nil {
		return err
	}
	if published.RootHash != descriptor.RootHash || descriptor.RootHash != fmt.Sprintf("%x", snapshot.Seal.RootHash) {
		return fmt.Errorf("Local archive descriptor authority mismatch")
	}
	return nil
}

func (n *Node) stageLocalBackupImport(ctx context.Context, archive *recovery.LocalArchive, owner *objectstore.LocalFilesystemOwner) (resultErr error) {
	pins := newLocalRestorePins(ctx)
	if owner != nil {
		pins = newOwnerLocalRestorePins(ctx, owner)
	}
	ctx = pins.ctx
	defer func() {
		resultErr = errors.Join(resultErr, context.Cause(ctx))
		if resultErr == nil && n.opened.Load() {
			n.localBackupRestorePins = pins
			return
		}
		resultErr = errors.Join(resultErr, pins.close())
	}()
	// Original restore evidence belongs to the original WAL. Equal-history
	// startup can resume it normally; remote replacement must not bypass it.
	if err := materializer.ValidateNoLocalRestore(filepath.Join(n.config.DataDir, "sqlite.db")); err != nil {
		return fmt.Errorf("resolve original Local restore evidence before archive replacement: %w", err)
	}
	journal := filepath.Join(n.config.DataDir, ".local-backup-import")
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := pins.acquire(archive); err != nil {
		return err
	}
	if err := n.beginLocalBackupStage(archive.Binding); err != nil {
		return err
	}
	stage := filepath.Join(journal, "next")
	config := *n.config
	config.DataDir = stage
	policy := localStorePolicy{Version: archive.Binding.PolicyVersion, StoreUUID: archive.Binding.StoreUUID, ConfigDigest: archive.Binding.ConfigDigest}
	encoded, err := json.Marshal(policy)
	if err != nil {
		return err
	}
	if err := atomicLocalFile(stage, filepath.Join(stage, "local-store.json"), encoded); err != nil {
		return err
	}
	identity, err := openLocalCheckpointIdentity(&config)
	if err != nil {
		return err
	}
	if archive.Binding.Bootstrap.ConfigID != n.loadClusterConfig().ConfigID || len(archive.Binding.Bootstrap.Members) != 1 || archive.Binding.Bootstrap.Members[0].ID != n.config.NodeID {
		return fmt.Errorf("Local archive bootstrap identity differs")
	}
	wal, err := qlog.OpenLocalDeferredCleanup(filepath.Join(stage, "qlog"))
	if err != nil {
		return err
	}
	defer wal.Close()
	limit := n.config.MaxWALBytes
	if limit == 0 {
		limit = 4 << 30
	}
	if err := wal.SetMaxBytes(limit); err != nil {
		return err
	}
	core, err := quepaxa.New(quepaxa.Config{NodeID: n.config.NodeID, Cluster: *n.loadClusterConfig(), WAL: wal, LocalMode: true})
	if err != nil {
		return err
	}
	if snapshot := archive.Snapshot(); snapshot != nil {
		if err := pins.confirm(); err != nil {
			return err
		}
		if err := n.downloadLocalBackupRoot(ctx, identity, *snapshot, stage); err != nil {
			return err
		}
		core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
			_, _, err := identity.OpenExpected(quepaxa.LocalCheckpointRoot{RootHash: seal.RootHash, Index: seal.Index, PrefixHash: seal.PrefixHash, StateHash: seal.StateHash, ConfigID: seal.ConfigID})
			return err
		})
		if err := core.RestoreLocalArchiveBase(ctx, snapshot.Seal, snapshot.Decision); err != nil {
			return err
		}
	}
	for core.Tip() < archive.Tip() {
		if err := pins.confirm(); err != nil {
			return err
		}
		values := archive.DecisionsFrom(core.Tip()+1, 256)
		if len(values) == 0 {
			return fmt.Errorf("Local archive omitted restore suffix")
		}
		for _, value := range values {
			seal, checkpoint, err := quepaxa.DecodeCheckpointSeal(value.Value)
			if err != nil {
				return err
			}
			if checkpoint {
				snapshot, err := archive.ReadSnapshot(ctx, seal.RootHash)
				if err != nil {
					return err
				}
				if snapshot.Decision.Slot != value.Slot || snapshot.Decision.Hash != value.Hash || !bytes.Equal(snapshot.Decision.Certificate, value.Certificate) {
					return fmt.Errorf("Local checkpoint artifact differs from certified suffix")
				}
				if err := n.downloadLocalBackupRoot(ctx, identity, snapshot, stage); err != nil {
					return err
				}
			}
		}
		if err := core.ImportLocalArchive(ctx, values); err != nil {
			return err
		}
	}
	prefix, ok := core.PrefixHash(core.Tip())
	if core.Tip() == 0 {
		prefix, ok = [32]byte{}, true
	}
	remotePrefix, remoteOK := archive.PrefixHash(archive.Tip())
	if !ok || !remoteOK || prefix != remotePrefix {
		return fmt.Errorf("Local import final prefix mismatch")
	}
	if err := wal.Sync(); err != nil {
		return err
	}
	if err := wal.Close(); err != nil {
		return err
	}
	// The existing Local restore protocol requires a compatible original pair.
	// Create that empty pair only inside staging; main startup installs the
	// verified base and finalizes its restore journal before suffix replay.
	empty, err := materializer.Open(filepath.Join(stage, "sqlite.db"), 1)
	if err != nil {
		return err
	}
	if err := empty.Close(); err != nil {
		return err
	}
	binding, err := json.Marshal(archive.Binding)
	if err != nil {
		return err
	}
	if err := atomicLocalFile(stage, filepath.Join(stage, "local-backup.json"), binding); err != nil {
		return err
	}
	if err := n.writeLocalBackupInstallJournal(journal); err != nil {
		return err
	}
	if err := pins.confirm(); err != nil {
		return err
	}
	return n.finishLocalBackupImportContext(ctx)
}

type localBackupInstallItem struct {
	Name string `json:"name"`
	Old  bool   `json:"old"`
	New  bool   `json:"new"`
}

func (n *Node) writeLocalBackupInstallJournal(journal string) error {
	owner, err := n.readLocalBackupStageOwner()
	if err != nil {
		return err
	}
	if owner == nil || journal != filepath.Join(n.config.DataDir, ".local-backup-import") {
		return fmt.Errorf("Local import installation requires owned staging")
	}
	names := map[string]bool{"sqlite.db": true, "sqlite.db-wal": true, "sqlite.db-shm": true, "latticedb": true, "local-checkpoints": true, "local-store.json": true, "local-backup.json": true}
	for _, root := range []string{n.config.DataDir, filepath.Join(journal, "next")} {
		entries, err := os.ReadDir(filepath.Join(root, "qlog"))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		for _, entry := range entries {
			if entry.Name() != "lock.qlog" {
				names[filepath.Join("qlog", entry.Name())] = true
			}
		}
	}
	var items []localBackupInstallItem
	for name := range names {
		_, oldErr := os.Lstat(filepath.Join(n.config.DataDir, name))
		_, newErr := os.Lstat(filepath.Join(journal, "next", name))
		if oldErr != nil && !errors.Is(oldErr, os.ErrNotExist) {
			return oldErr
		}
		if newErr != nil && !errors.Is(newErr, os.ErrNotExist) {
			return newErr
		}
		items = append(items, localBackupInstallItem{Name: name, Old: oldErr == nil, New: newErr == nil})
	}
	data, err := json.Marshal(items)
	if err != nil {
		return err
	}
	return atomicLocalFile(journal, filepath.Join(journal, "install.json"), data)
}
func (n *Node) finishLocalBackupImport() error {
	return n.finishLocalBackupImportContext(context.Background())
}

func (n *Node) finishLocalBackupImportContext(ctx context.Context) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	journal := filepath.Join(n.config.DataDir, ".local-backup-import")
	data, err := os.ReadFile(filepath.Join(journal, "install.json"))
	if errors.Is(err, os.ErrNotExist) {
		return n.discardLocalBackupPreinstall()
	}
	if err != nil {
		return err
	}
	info, err := os.Lstat(journal)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("invalid Local import installation directory")
	}
	owner, err := n.readLocalBackupStageOwner()
	if err != nil {
		return err
	}
	// A pre-existing committed install.json retains its original retry semantics.
	// New journals additionally carry the owner, which must match when present.
	if owner != nil {
		if err := n.verifyLocalBackupStageOwner(owner); err != nil {
			return err
		}
	}
	var items []localBackupInstallItem
	if err := decodeLocalJSON(data, &items); err != nil {
		return err
	}
	for _, item := range items {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		clean := filepath.Clean(item.Name)
		if clean != item.Name || filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, "../") || len(clean) > 256 || clean == "qlog/lock.qlog" {
			return fmt.Errorf("invalid Local import journal path")
		}
		target := filepath.Join(n.config.DataDir, item.Name)
		next := filepath.Join(journal, "next", item.Name)
		previous := filepath.Join(journal, "previous", item.Name)
		if item.Old {
			if _, err := os.Lstat(previous); errors.Is(err, os.ErrNotExist) {
				if err := os.MkdirAll(filepath.Dir(previous), 0700); err != nil {
					return err
				}
				if err := os.Rename(target, previous); err != nil {
					return err
				}
				if err := syncLocalDirOS(filepath.Dir(target)); err != nil {
					return err
				}
				if err := syncLocalDirOS(filepath.Dir(previous)); err != nil {
					return err
				}
			} else if err != nil {
				return err
			}
		}
		if item.New {
			if _, err := os.Lstat(next); err == nil {
				if err := os.Rename(next, target); err != nil {
					return err
				}
				if err := syncLocalDirOS(filepath.Dir(next)); err != nil {
					return err
				}
				if err := syncLocalDirOS(filepath.Dir(target)); err != nil {
					return err
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
	// Keep the intent with committed recovery evidence until normal startup
	// removes the installed journal. A later owner-only retry removes the intent.
	return nil
}
func (n *Node) startLocalBackup(ctx context.Context) {
	n.localGCContext, n.localGCCancel = context.WithCancel(ctx)
	interval := n.config.ObjStoreSyncInterval
	if interval == 0 {
		interval = time.Minute
	}
	checkpointInterval := n.config.CheckpointInterval
	if checkpointInterval <= 0 {
		checkpointInterval = 15 * time.Minute
	}
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		syncTicker := time.NewTicker(interval)
		defer syncTicker.Stop()
		checkpointTicker := time.NewTicker(checkpointInterval)
		defer checkpointTicker.Stop()
		gcTicker := time.NewTicker(n.config.ObjStoreGCInterval)
		defer gcTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-gcTicker.C:
				n.collectLocalBackup(ctx)
			case <-syncTicker.C:
				if err := n.syncLocalBackup(ctx); err != nil {
					log.Printf("Local backup sync: %v", err)
				}
				tailLimit := n.config.CheckpointTailBytes
				if tailLimit <= 0 {
					tailLimit = 64 << 20
				}
				snapshot := n.localBackup.Snapshot()
				if n.wal.Bytes() >= tailLimit && (snapshot == nil || n.core.Tip() > snapshot.Decision.Slot) {
					if err := n.reclaimLocalCheckpoint(ctx); err != nil && !errors.Is(err, network.ErrLocalMaintenanceRefused) {
						log.Printf("Local backup tail checkpoint: %v", err)
					}
				}
			case <-checkpointTicker.C:
				if snapshot := n.localBackup.Snapshot(); snapshot != nil && n.core.Tip() <= snapshot.Decision.Slot {
					continue
				}
				if err := n.reclaimLocalCheckpoint(ctx); err != nil && !errors.Is(err, network.ErrLocalMaintenanceRefused) {
					log.Printf("Local backup checkpoint: %v", err)
				}
			}
		}
	}()
}
