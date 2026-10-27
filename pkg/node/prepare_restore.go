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
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

const (
	preparedRestoreMarkerName  = "prepared-restore.json"
	preparedRestorePendingName = ".prepared-restore.next"
)

var preparedRestoreCrashBoundary func(string)

type preparedRestoreMarker struct {
	Version        uint   `json:"version"`
	Phase          string `json:"phase"`
	PodUID         string `json:"pod_uid"`
	NodeID         string `json:"node_id"`
	ClusterID      string `json:"cluster_id"`
	SourceHash     string `json:"source_hash"`
	StoreHash      string `json:"store_hash"`
	BootstrapHash  string `json:"bootstrap_hash"`
	BaseIndex      uint64 `json:"base_index"`
	BaseRootHash   string `json:"base_root_hash"`
	BaseStateHash  string `json:"base_state_hash"`
	BasePrefixHash string `json:"base_prefix_hash"`
	CertifiedTip   uint64 `json:"certified_tip"`
	TipPrefixHash  string `json:"tip_prefix_hash"`
}

type preparedStoreIdentity struct {
	Provider            string `json:"provider"`
	Endpoint            string `json:"endpoint,omitempty"`
	Bucket              string `json:"bucket,omitempty"`
	Prefix              string `json:"prefix"`
	Region              string `json:"region,omitempty"`
	FilesystemDir       string `json:"filesystem_dir,omitempty"`
	AzureStorageAccount string `json:"azure_storage_account,omitempty"`
}

type preparedSourceIdentity struct {
	ClusterID     string `json:"cluster_id"`
	StoreHash     string `json:"store_hash"`
	BootstrapHash string `json:"bootstrap_hash"`
}

// PrepareRestore installs certified SQL and Graph state for a fresh learner
// without opening its runtime WAL, registering its voter identity, or starting
// any listener or background publisher.
func PrepareRestore(ctx context.Context, config *types.ExecutionConfig) (resultErr error) {
	if err := validatePreparedRestoreConfig(config); err != nil {
		return err
	}
	return prepareRestore(ctx, config, nil)
}

// prepareRestore accepts an injected bucket only for package tests; production
// always resolves the configured provider through NewBucketWithContext.
func prepareRestore(ctx context.Context, config *types.ExecutionConfig, injected *objectstore.MeteredBucket) (resultErr error) {
	if err := validatePreparedRestoreConfig(config); err != nil {
		return err
	}
	if config.ObjStoreDurability == "" {
		config.ObjStoreDurability = types.ObjectStoreDurabilityAsync
	}
	baseMarker, err := preparedMarkerForConfig(config)
	if err != nil {
		return err
	}
	intentMarker := baseMarker
	intentMarker.Phase = "preparing"
	if err := os.MkdirAll(config.DataDir, 0700); err != nil {
		return err
	}
	if _, err := os.Stat(config.DataDir); err != nil {
		return err
	}
	_, markerExistsBeforeLock, err := readPreparedRestoreMarkerForConfig(config.DataDir, intentMarker)
	if err != nil {
		return err
	}
	if !markerExistsBeforeLock {
		if err := validatePreparedRestoreInventory(config.DataDir, "fresh-preflight"); err != nil {
			return err
		}
	}
	_, lock, err := qlog.Acquire(filepath.Join(config.DataDir, "qlog"))
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, lock.Release()) }()

	marker, markerExists, err := readPreparedRestoreMarkerForConfig(config.DataDir, intentMarker)
	if err != nil {
		return err
	}
	if markerExists {
		if !preparedMarkerMatchesConfig(marker, baseMarker) {
			return fmt.Errorf("prepared restore marker does not match this Pod, node, source, or store; preserve DataDir")
		}
		if marker.Phase == "complete" {
			return nil
		}
		if marker.Phase != "preparing" {
			return fmt.Errorf("prepared restore marker has unknown phase %q; preserve DataDir", marker.Phase)
		}
		if err := validatePreparedRestoreInventory(config.DataDir, marker.Phase); err != nil {
			return err
		}
	} else {
		if err := validatePreparedRestoreInventory(config.DataDir, "fresh"); err != nil {
			return err
		}
		if preparedRestoreCrashBoundary != nil {
			preparedRestoreCrashBoundary("intent-before-write")
		}
		if err := writePreparedRestoreIntent(config.DataDir, intentMarker); err != nil {
			return fmt.Errorf("record prepared restore intent: %w", err)
		}
		if preparedRestoreCrashBoundary != nil {
			preparedRestoreCrashBoundary("intent-durable")
		}
	}
	if markerExists && marker.Phase == "preparing" && marker.CertifiedTip == 0 {
		if err := writePreparedRestoreIntent(config.DataDir, intentMarker); err != nil {
			return fmt.Errorf("resume prepared restore intent: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	observerDir, err := os.MkdirTemp("", ".rhiza-prepare-observer-")
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, os.RemoveAll(observerDir)) }()
	observerWAL, err := qlog.Open(observerDir)
	if err != nil {
		return fmt.Errorf("open temporary history verifier: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, observerWAL.Close()) }()
	if preparedRestoreCrashBoundary != nil {
		preparedRestoreCrashBoundary("observer-open")
	}

	provider := objectstore.Provider(config.ObjStoreProvider)
	if provider == "" {
		provider = objectstore.ProviderS3
	}
	bucket := injected
	if bucket == nil {
		bucket, err = objectstore.NewBucketWithContext(ctx, objectstore.Config{
			Provider: provider, FilesystemDir: config.ObjStoreDir, Prefix: config.ObjStorePrefix,
			Endpoint: config.ObjStoreEndpoint, Bucket: config.ObjStoreBucket, Region: config.ObjStoreRegion,
			Insecure: config.ObjStoreInsecure, MaxRetries: config.ObjStoreRetries,
			AccessKey: config.ObjStoreAccessKey, SecretKey: config.ObjStoreSecretKey, SessionToken: config.ObjStoreSessionToken,
			ServiceAccount: config.ObjStoreServiceAccount, AzureTenantID: config.ObjStoreAzureTenantID,
			AzureClientID: config.ObjStoreAzureClientID, AzureClientSecret: config.ObjStoreAzureClientSecret,
			AzureStorageAccount: config.ObjStoreAzureStorageAccount, AzureStorageAccountKey: config.ObjStoreAzureStorageAccountKey,
			AzureConnectionString: config.ObjStoreAzureConnectionString, AzureUserAssignedID: config.ObjStoreAzureUserAssignedID,
		})
		if err != nil {
			return fmt.Errorf("open restore source: %w", err)
		}
		defer func() { resultErr = errors.Join(resultErr, bucket.Close()) }()
	}

	prefix := path.Join(config.ObjStorePrefix, string(config.ClusterID))
	archive := recovery.NewManager(bucket, prefix, 1)
	defer archive.Close()
	if err := archive.Load(ctx); err != nil {
		return fmt.Errorf("load certified source archive: %w", err)
	}
	if archive.Sealed() {
		return recovery.ErrArchiveSealed
	}
	if archive.Tip() == 0 {
		return fmt.Errorf("certified source has no materialized history")
	}
	if len(config.Members) > 1 && !archive.CASSupported() {
		return fmt.Errorf("prepared learner restore requires conditional shared archive writes")
	}
	checkpoints := checkpoint.NewManager(bucket, prefix, config.DataDir, 1)
	if err := checkpoints.Load(ctx); err != nil {
		return fmt.Errorf("load certified checkpoint manifest: %w", err)
	}
	owner, err := startupRecoveryOwner("prepare-" + string(config.NodeID))
	if err != nil {
		return err
	}
	guard, err := newStartupRecoveryGuard(ctx, archive, owner)
	if err != nil {
		return fmt.Errorf("pin prepared restore source: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, closePreparedRecovery(guard)) }()
	snapshot := guard.Snapshot()
	if snapshot == nil {
		return fmt.Errorf("prepared learner restore requires a pinned archive snapshot")
	}
	if archive.Sealed() {
		return recovery.ErrArchiveSealed
	}

	n := &Node{config: config, bucket: bucket, archive: archive, checkpoints: checkpoints}
	cluster := n.loadClusterConfig()
	core, err := quepaxa.NewObserver(quepaxa.Config{NodeID: quepaxa.NodeID("prepare-" + string(config.NodeID)), Cluster: *cluster, WAL: observerWAL, EnableReconfiguration: config.EnableReconfiguration})
	if err != nil {
		return fmt.Errorf("create passive history verifier: %w", err)
	}
	n.core = core
	core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
		return checkpoints.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
	})
	generationCheckpoint, err := n.installGenerationAnchor(guard)
	if err != nil {
		return fmt.Errorf("verify source generation anchor: %w", err)
	}
	if seal, decision, ok := snapshot.RecoveryBase(); ok && core.Tip() < seal.Index {
		if err := core.RestoreCheckpointBase(guard.Context(), seal, decision); err != nil {
			return fmt.Errorf("verify certified source checkpoint base: %w", err)
		}
	}
	for core.Tip() < snapshot.Tip() {
		values, _, err := snapshot.DecisionsFrom(guard.Context(), core.Tip()+1, 256)
		if err != nil {
			return fmt.Errorf("read certified source suffix: %w", err)
		}
		if len(values) == 0 {
			return fmt.Errorf("certified source omitted slot %d", core.Tip()+1)
		}
		for _, decision := range values {
			if err := types.ValidateExecutionPolicy(decision.Value); err != nil {
				return err
			}
		}
		if err := core.AcceptCertifiedValues(values); err != nil {
			return fmt.Errorf("verify certified source decisions: %w", err)
		}
	}
	if core.Tip() != snapshot.Tip() {
		return fmt.Errorf("verified source tip %d differs from pinned tip %d", core.Tip(), snapshot.Tip())
	}

	root, err := n.preparedCheckpoint(guard, generationCheckpoint)
	if err != nil {
		return err
	}
	if root == nil && core.CompactionFloor() > 0 {
		return fmt.Errorf("certified history was compacted without an available checkpoint")
	}
	baseMarker.BaseIndex = 0
	if root != nil {
		baseMarker.BaseIndex = root.Index
		baseMarker.BaseRootHash = hex.EncodeToString(root.RootHash[:])
		baseMarker.BaseStateHash = hex.EncodeToString(root.Hash[:])
		if prefixHash, ok := core.PrefixHash(quepaxa.Slot(root.Index)); ok {
			baseMarker.BasePrefixHash = hex.EncodeToString(prefixHash[:])
		} else {
			return fmt.Errorf("selected checkpoint prefix is not in certified history")
		}
	}
	tipPrefix, ok := core.PrefixHash(core.Tip())
	if !ok {
		return fmt.Errorf("certified source tip has no prefix hash")
	}
	baseMarker.CertifiedTip = uint64(core.Tip())
	baseMarker.TipPrefixHash = hex.EncodeToString(tipPrefix[:])
	baseMarker.Phase = "preparing"
	if err := writePreparedRestoreMarker(config.DataDir, baseMarker); err != nil {
		return fmt.Errorf("record prepared restore intent: %w", err)
	}

	material, err := materializer.Open(filepath.Join(config.DataDir, "sqlite.db"), 4)
	if err != nil {
		return fmt.Errorf("open prepared materializer: %w", err)
	}
	n.material = material
	defer func() {
		if n.material != nil {
			resultErr = errors.Join(resultErr, n.material.Close())
			n.material = nil
		}
	}()
	if err := material.ConfigureLocalGraphNodePropertyIndexes(config.LocalGraphNodePropertyIndexes); err != nil {
		return err
	}
	if material.Tip() > uint64(core.Tip()) {
		return fmt.Errorf("existing materialized tip %d is ahead of certified source %d", material.Tip(), core.Tip())
	}
	if material.Tip() > 0 {
		decision, ok := core.CertifiedValue(quepaxa.Slot(material.Tip()))
		if !ok {
			return fmt.Errorf("existing materialized tip %d has no certified decision", material.Tip())
		}
		if err := material.ValidateTip(material.Tip(), decision.Value); err != nil {
			return fmt.Errorf("validate existing materialized state: %w", err)
		}
	}
	if root != nil && root.Index > material.Tip() {
		tempDir, err := os.MkdirTemp("", ".rhiza-prepare-checkpoint-")
		if err != nil {
			return err
		}
		defer func() { resultErr = errors.Join(resultErr, os.RemoveAll(tempDir)) }()
		files, err := checkpoints.DownloadRootFiles(guard.Context(), root.Index, root.RootHash, tempDir)
		if err != nil {
			return fmt.Errorf("download selected certified checkpoint: %w", err)
		}
		materialFiles := make([]materializer.CheckpointFile, 0, len(files))
		for _, file := range files {
			materialFiles = append(materialFiles, materializer.CheckpointFile{Role: materializer.CheckpointRole(file.Role), Path: file.Path})
		}
		if err := material.RestoreCheckpoint(guard.Context(), materialFiles); err != nil {
			return fmt.Errorf("install SQL and Graph checkpoint: %w", err)
		}
	}
	if err := n.replayLocalDecisions(guard.Context()); err != nil {
		return fmt.Errorf("replay certified SQL and Graph suffix: %w", err)
	}
	if material.Tip() != uint64(core.Tip()) {
		return fmt.Errorf("prepared materializer tip %d differs from certified source tip %d", material.Tip(), core.Tip())
	}
	if decision, ok := core.CertifiedValue(core.Tip()); ok {
		if err := material.ValidateTip(uint64(core.Tip()), decision.Value); err != nil {
			return fmt.Errorf("verify prepared SQL and Graph tip: %w", err)
		}
	}
	if err := guard.Check(ctx.Err()); err != nil {
		return err
	}
	if err := material.Close(); err != nil {
		return fmt.Errorf("sync prepared materializer: %w", err)
	}
	n.material = nil
	baseMarker.Phase = "complete"
	if err := writePreparedRestoreMarker(config.DataDir, baseMarker); err != nil {
		return fmt.Errorf("commit prepared restore marker: %w", err)
	}
	return nil
}

func validatePreparedRestoreInventory(dataDir, phase string) error {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return err
	}
	allowed := map[string]bool{"qlog": true}
	if phase == "preparing" {
		for _, name := range []string{preparedRestoreMarkerName, preparedRestorePendingName, "sqlite.db", "sqlite.db-wal", "sqlite.db-shm", "sqlite.db-journal", "latticedb"} {
			allowed[name] = true
		}
	}
	for _, entry := range entries {
		name := entry.Name()
		if !allowed[name] {
			return fmt.Errorf("unknown path %q in prepared restore DataDir; preserve DataDir", name)
		}
		info, err := os.Lstat(filepath.Join(dataDir, name))
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink %q in prepared restore DataDir; preserve DataDir", name)
		}
		if name == "qlog" {
			if !info.IsDir() {
				return fmt.Errorf("qlog path is not a directory; preserve DataDir")
			}
			children, err := os.ReadDir(filepath.Join(dataDir, name))
			if err != nil {
				return err
			}
			for _, child := range children {
				if child.Name() != "lock.qlog" {
					return fmt.Errorf("unknown qlog path %q in prepared restore DataDir; preserve DataDir", child.Name())
				}
				childInfo, err := os.Lstat(filepath.Join(dataDir, name, child.Name()))
				if err != nil {
					return err
				}
				if !childInfo.Mode().IsRegular() || childInfo.Mode()&os.ModeSymlink != 0 {
					return fmt.Errorf("invalid qlog lock file in prepared restore DataDir")
				}
			}
		} else if name == "latticedb" {
			if !info.IsDir() {
				return fmt.Errorf("prepared restore path %q is not a directory", name)
			}
		} else if name == preparedRestorePendingName {
			if !info.Mode().IsRegular() || info.Size() > 64<<10 {
				return fmt.Errorf("invalid prepared restore pending marker; preserve DataDir")
			}
		} else if !info.Mode().IsRegular() {
			return fmt.Errorf("prepared restore path %q is not a regular file", name)
		}
	}
	if phase == "fresh-preflight" {
		return nil
	}
	return nil
}

func validatePreparedRestoreConfig(config *types.ExecutionConfig) error {
	if config == nil || config.DataDir == "" || config.ClusterID == "" || config.NodeID == "" || config.PreparedPodUID == "" {
		return fmt.Errorf("prepared restore requires DataDir, cluster ID, node ID, and Pod UID")
	}
	if config.PreparedPodUID != strings.TrimSpace(config.PreparedPodUID) {
		return fmt.Errorf("prepared Pod UID is not canonical")
	}
	if config.Local || !config.EnableReconfiguration || config.Learner == nil || config.Learner.ID != config.NodeID || len(config.Members) < 2 {
		return fmt.Errorf("prepared restore requires a reconfigurable fresh learner with bootstrap voters")
	}
	if err := validateVoterMembership(config); err != nil {
		return err
	}
	configured, err := validateObjectStoreConfig(config)
	if err != nil {
		return err
	}
	if !configured {
		return fmt.Errorf("prepared restore requires shared object storage")
	}
	return nil
}

func (n *Node) preparedCheckpoint(guard *startupRecoveryGuard, generation *checkpoint.Checkpoint) (*checkpoint.Checkpoint, error) {
	seal, sealed, err := n.core.LatestCheckpointSeal()
	if err != nil {
		return nil, err
	}
	if current := n.checkpoints.Latest(); current != nil {
		anchorCurrent := !sealed && generation != nil && current.Index == generation.Index && current.RootHash == generation.RootHash
		if !anchorCurrent && (!sealed || current.Index > uint64(seal.Index) || current.Index == uint64(seal.Index) && current.RootHash != seal.RootHash) {
			return nil, fmt.Errorf("checkpoint CURRENT is not backed by certified recovery evidence")
		}
	}
	var root *checkpoint.Checkpoint
	if sealed {
		root, err = n.checkpoints.OpenRoot(guard.Context(), uint64(seal.Index), seal.RootHash)
		if err != nil {
			return nil, err
		}
		if root.Hash != seal.StateHash {
			return nil, fmt.Errorf("certified checkpoint state hash mismatch")
		}
	} else if generation != nil {
		copyRoot := *generation
		root = &copyRoot
	}
	if root != nil {
		root, err = guard.PinRoot(guard.Context(), n.checkpoints, root, guard.Owner())
		if err != nil {
			return nil, fmt.Errorf("pin selected checkpoint root: %w", guard.Check(err))
		}
	}
	return root, nil
}

func closePreparedRecovery(guard *startupRecoveryGuard) error {
	guard.cancel()
	<-guard.done
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	closeCtx = localtesthooks.CarryRecoveryPinObserver(closeCtx, guard.ctx)
	root, snapshot := guard.root, guard.snapshot
	closeErr := guard.Check(nil)
	if root != nil {
		closeErr = errors.Join(closeErr, root.Close(closeCtx))
	}
	if snapshot != nil {
		closeErr = errors.Join(closeErr, snapshot.Close(closeCtx))
	}
	return closeErr
}

func preparedMarkerForConfig(config *types.ExecutionConfig) (preparedRestoreMarker, error) {
	store := objectstore.Provider(config.ObjStoreProvider)
	if store == "" {
		if config.ObjStoreDir != "" {
			store = objectstore.ProviderFilesystem
		} else {
			store = objectstore.ProviderS3
		}
	}
	endpoint, err := preparedEndpointIdentity(config.ObjStoreEndpoint)
	if err != nil {
		return preparedRestoreMarker{}, err
	}
	filesystemDir := ""
	if store == objectstore.ProviderFilesystem && config.ObjStoreDir != "" {
		filesystemDir, err = filepath.Abs(config.ObjStoreDir)
		if err != nil {
			return preparedRestoreMarker{}, err
		}
	}
	storeData, err := json.Marshal(preparedStoreIdentity{
		Provider: string(store), Endpoint: endpoint, Bucket: config.ObjStoreBucket,
		Prefix: path.Clean(config.ObjStorePrefix), Region: config.ObjStoreRegion,
		FilesystemDir: filesystemDir, AzureStorageAccount: config.ObjStoreAzureStorageAccount,
	})
	if err != nil {
		return preparedRestoreMarker{}, err
	}
	storeHash := sha256.Sum256(storeData)
	bootstrap := recovery.NewMembershipRecord(string(config.ClusterID), config.Members, string(config.ObjStoreDurability))
	bootstrapHash, err := hex.DecodeString(bootstrap.Membership)
	if err != nil {
		return preparedRestoreMarker{}, err
	}
	sourceData, err := json.Marshal(preparedSourceIdentity{
		ClusterID: string(config.ClusterID), StoreHash: hex.EncodeToString(storeHash[:]),
		BootstrapHash: bootstrap.Membership,
	})
	if err != nil {
		return preparedRestoreMarker{}, err
	}
	sourceHash := sha256.Sum256(sourceData)
	return preparedRestoreMarker{
		Version: 1, PodUID: config.PreparedPodUID, NodeID: string(config.NodeID), ClusterID: string(config.ClusterID),
		SourceHash: hex.EncodeToString(sourceHash[:]), StoreHash: hex.EncodeToString(storeHash[:]),
		BootstrapHash: hex.EncodeToString(bootstrapHash),
	}, nil
}

func preparedEndpointIdentity(value string) (string, error) {
	if value == "" {
		return "", nil
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" {
		parsed, err = url.Parse("//" + strings.TrimPrefix(strings.TrimPrefix(value, "https://"), "http://"))
		if err != nil || parsed.Host == "" {
			return "", fmt.Errorf("invalid object-store endpoint identity")
		}
	}
	parsed.User = nil
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.Scheme + "://" + parsed.Host + strings.TrimRight(parsed.EscapedPath(), "/"), nil
}

func preparedMarkerMatchesConfig(marker, expected preparedRestoreMarker) bool {
	return marker.Version == 1 && marker.PodUID == expected.PodUID && marker.NodeID == expected.NodeID &&
		marker.ClusterID == expected.ClusterID && marker.SourceHash == expected.SourceHash &&
		marker.StoreHash == expected.StoreHash && marker.BootstrapHash == expected.BootstrapHash
}

func readPreparedRestoreMarker(dataDir string) (preparedRestoreMarker, bool, error) {
	var marker preparedRestoreMarker
	name := filepath.Join(dataDir, preparedRestoreMarkerName)
	info, err := os.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return marker, false, nil
	}
	if err != nil {
		return marker, false, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > 64<<10 {
		return marker, true, fmt.Errorf("prepared restore marker is not a bounded regular file")
	}
	file, err := os.Open(name)
	if err != nil {
		return marker, true, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 64<<10))
	if err != nil {
		return marker, true, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&marker); err != nil {
		return marker, true, fmt.Errorf("decode prepared restore marker: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return marker, true, fmt.Errorf("prepared restore marker has trailing data")
	}
	canonical, err := json.Marshal(marker)
	if err != nil || !bytes.Equal(bytes.TrimSpace(data), canonical) {
		return marker, true, fmt.Errorf("prepared restore marker is not canonical")
	}
	if err := validatePreparedMarkerContents(marker); err != nil {
		return marker, true, err
	}
	return marker, true, nil
}

func readPreparedRestoreMarkerForConfig(dataDir string, intent preparedRestoreMarker) (preparedRestoreMarker, bool, error) {
	marker, exists, err := readPreparedRestoreMarker(dataDir)
	if err == nil || !exists {
		return marker, exists, err
	}
	name := filepath.Join(dataDir, preparedRestoreMarkerName)
	info, statErr := os.Lstat(name)
	if statErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > 64<<10 {
		return marker, true, errors.Join(err, statErr, fmt.Errorf("prepared restore marker is not a bounded regular file"))
	}
	data, readErr := os.ReadFile(name)
	if readErr != nil {
		return marker, true, errors.Join(err, readErr)
	}
	expected, marshalErr := json.Marshal(intent)
	if marshalErr == nil && len(data) <= len(expected) && bytes.Equal(expected[:len(data)], data) {
		return intent, true, nil
	}
	return marker, true, fmt.Errorf("invalid interrupted prepared restore marker; preserve DataDir: %w", err)
}

func writePreparedRestoreIntent(dataDir string, marker preparedRestoreMarker) error {
	marker.Phase = "preparing"
	data, err := json.Marshal(marker)
	if err != nil {
		return err
	}
	name := filepath.Join(dataDir, preparedRestoreMarkerName)
	flags := os.O_WRONLY | os.O_CREATE | os.O_EXCL
	if info, err := os.Lstat(name); err == nil {
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > int64(len(data)) {
			return fmt.Errorf("prepared restore marker is not a bounded regular intent file")
		}
		existing, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		if len(existing) > len(data) || !bytes.Equal(data[:len(existing)], existing) {
			return fmt.Errorf("prepared restore marker is not an interrupted intent; preserve DataDir")
		}
		flags = os.O_WRONLY | os.O_TRUNC
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := writePreparedFile(name, flags, data, func() {
		if preparedRestoreCrashBoundary != nil {
			preparedRestoreCrashBoundary("intent-write-start")
		}
	}); err != nil {
		return err
	}
	return syncPreparedDirectory(dataDir)
}

func writePreparedRestoreMarker(dataDir string, marker preparedRestoreMarker) error {
	data, err := json.Marshal(marker)
	if err != nil {
		return err
	}
	pending := filepath.Join(dataDir, preparedRestorePendingName)
	current, exists, err := readPreparedRestoreMarker(dataDir)
	if err != nil {
		return err
	}
	if !exists || current.Phase != "preparing" || !preparedMarkerMatchesConfig(current, marker) {
		return fmt.Errorf("prepared restore update lacks matching durable intent; preserve DataDir")
	}
	if info, err := os.Lstat(pending); err == nil {
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > 64<<10 {
			return fmt.Errorf("pending prepared restore marker is not a bounded regular file; preserve DataDir")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := writePreparedFile(pending, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, data, func() {
		if preparedRestoreCrashBoundary != nil {
			preparedRestoreCrashBoundary("marker-pending-write-start")
		}
	}); err != nil {
		return err
	}
	if err := os.Rename(pending, filepath.Join(dataDir, preparedRestoreMarkerName)); err != nil {
		return err
	}
	return syncPreparedDirectory(dataDir)
}

func writePreparedFile(name string, flags int, data []byte, beforeWrite func()) error {
	file, err := os.OpenFile(name, flags, 0600)
	if err != nil {
		return err
	}
	if beforeWrite != nil {
		beforeWrite()
	}
	written, writeErr := io.Copy(file, bytes.NewReader(data))
	if writeErr == nil && written != int64(len(data)) {
		writeErr = io.ErrShortWrite
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	return errors.Join(writeErr, syncErr, closeErr)
}

func syncPreparedDirectory(name string) error {
	dir, err := os.Open(name)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func validatePreparedRestoreMarker(config *types.ExecutionConfig) error {
	if config.PreparedPodUID == "" {
		return nil
	}
	expected, err := preparedMarkerForConfig(config)
	if err != nil {
		return err
	}
	marker, exists, err := readPreparedRestoreMarker(config.DataDir)
	if err != nil {
		return err
	}
	if !exists || marker.Phase != "complete" || !preparedMarkerMatchesConfig(marker, expected) {
		return fmt.Errorf("prepared restore marker is missing or mismatched; preserve DataDir")
	}
	if err := validatePreparedMarkerContents(marker); err != nil {
		return err
	}
	return nil
}

func validatePreparedMarkerContents(marker preparedRestoreMarker) error {
	if marker.Version != 1 || marker.Phase != "preparing" && marker.Phase != "complete" {
		return fmt.Errorf("prepared restore marker has unsupported version or phase")
	}
	if marker.Phase == "preparing" && marker.CertifiedTip == 0 {
		if marker.BaseIndex != 0 || marker.BaseRootHash != "" || marker.BaseStateHash != "" || marker.BasePrefixHash != "" || marker.TipPrefixHash != "" {
			return fmt.Errorf("prepared restore intent contains partial source evidence")
		}
	} else if marker.CertifiedTip == 0 || marker.TipPrefixHash == "" {
		return fmt.Errorf("prepared restore marker lacks certified tip evidence")
	}
	digests := []string{marker.SourceHash, marker.StoreHash, marker.BootstrapHash}
	if marker.CertifiedTip > 0 {
		digests = append(digests, marker.TipPrefixHash)
	}
	for _, value := range digests {
		decoded, err := hex.DecodeString(value)
		if err != nil || len(decoded) != sha256.Size {
			return fmt.Errorf("prepared restore marker contains an invalid digest")
		}
	}
	if marker.BaseIndex == 0 {
		if marker.BaseRootHash != "" || marker.BaseStateHash != "" || marker.BasePrefixHash != "" {
			return fmt.Errorf("prepared restore marker has checkpoint hashes without a base")
		}
	} else {
		for _, value := range []string{marker.BaseRootHash, marker.BaseStateHash, marker.BasePrefixHash} {
			decoded, err := hex.DecodeString(value)
			if err != nil || len(decoded) != sha256.Size {
				return fmt.Errorf("prepared restore marker contains an invalid checkpoint digest")
			}
		}
		if marker.BaseIndex > marker.CertifiedTip {
			return fmt.Errorf("prepared restore marker base exceeds certified tip")
		}
	}
	return nil
}
