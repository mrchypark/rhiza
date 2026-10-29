package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

// StartupChecker checks current authority for this prepared identity. It is not
// a reservation or permission for future application writes.
type StartupChecker interface {
	ValidatePreparedStartup(context.Context, recovery.PreparedStartupIdentity) error
}

type PreparedStartupConfig struct {
	Prepared *recovery.VerifiedPreparedStartup
	Checker  StartupChecker
	// Identity is required for a restart after adoption when Prepared is nil.
	Identity recovery.PreparedStartupIdentity
}

var ErrPreparedWriteAdmissionUnavailable = errors.New("prepared startup write admission is unavailable")

// OpenPrepared performs bounded local recovery but cannot serve until the
// actual application mutation gate is wired.
func (n *Node) OpenPrepared(ctx context.Context, cfg PreparedStartupConfig) error {
	if n == nil || n.opened.Load() || n.preparedStartup != nil {
		return fmt.Errorf("node already opened or configured")
	}
	n.preparedStartup = &cfg
	return n.Open(ctx)
}

func (n *Node) openPrepared(ctx context.Context, cfg PreparedStartupConfig) error {
	if n == nil || n.config == nil || n.config.Local || cfg.Checker == nil {
		return fmt.Errorf("prepared startup requires a verified handoff and live checker")
	}
	identity := cfg.Identity
	if cfg.Prepared != nil {
		if cfg.Prepared.View() == nil {
			return fmt.Errorf("invalid prepared handoff")
		}
		identity = cfg.Prepared.Identity()
	}
	if identity.NodeID == "" || identity.NodeID != string(n.config.NodeID) {
		return fmt.Errorf("prepared node identity mismatch")
	}
	if n.config.Learner == nil {
		return fmt.Errorf("prepared startup currently requires a new learner")
	}
	if err := validateVoterMembership(n.config); err != nil {
		return err
	}
	if n.config.PeerToken == "" || n.config.PeerToken == n.config.AdminToken {
		return fmt.Errorf("prepared learner requires distinct peer and admin tokens")
	}
	source, config, err := n.preparedBindingDigests()
	if err != nil {
		return err
	}
	if identity.SourceBinding != source || identity.ConfigDigest != config {
		return fmt.Errorf("prepared storage or configuration binding mismatch")
	}
	if err := cfg.Checker.ValidatePreparedStartup(ctx, identity); err != nil {
		return fmt.Errorf("prepared startup authority: %w", err)
	}
	if cfg.Prepared == nil {
		if err := n.verifyPreparedAdoption(ctx, identity); err != nil {
			return err
		}
		return ErrPreparedWriteAdmissionUnavailable
	}
	if err := n.recoverPreparedV1Alpha2(ctx, cfg.Prepared); err != nil {
		return err
	}
	return ErrPreparedWriteAdmissionUnavailable
}

func (n *Node) verifyPreparedAdoption(ctx context.Context, identity recovery.PreparedStartupIdentity) (resultErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	dir := n.config.DataDir
	data, err := readPreparedAdoption(filepath.Join(dir, "prepared-adoption.json"))
	if err != nil {
		return fmt.Errorf("prepared adoption is missing or invalid")
	}
	var adoption preparedAdoption
	if err := json.Unmarshal(data, &adoption); err != nil || adoption.Identity != identity || adoption.WALIdentity == "" {
		return fmt.Errorf("prepared adoption identity mismatch")
	}
	_, lock, err := qlog.Acquire(filepath.Join(dir, "qlog"))
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, lock.Release()) }()
	local, err := loadVoterIdentity(n.config)
	if err != nil {
		return err
	}
	if local.identity == nil || !local.identity.Registered || local.identity.Nonce != adoption.WALIdentity {
		return fmt.Errorf("prepared WAL registration mismatch")
	}
	wal, err := qlog.OpenReadOnly(filepath.Join(dir, "qlog"))
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, wal.Close()) }()
	cluster := n.loadClusterConfig()
	transport := network.NewTransport(n.config.ClusterID, n.config.NodeID, cluster, n.config.PeerToken)
	transport.SetAdminToken(n.config.AdminToken)
	defer func() { resultErr = errors.Join(resultErr, transport.Close()) }()
	newCore := quepaxa.New
	if n.config.Learner != nil {
		newCore = quepaxa.NewLearner
	}
	core, err := newCore(quepaxa.Config{NodeID: n.config.NodeID, Cluster: *cluster, WAL: wal, Transport: transport, EnableReconfiguration: n.config.EnableReconfiguration})
	if err != nil {
		return err
	}
	prefix, ok := core.PrefixHash(adoption.Tip)
	if core.WALIdentity() != adoption.WALIdentity || core.Tip() < adoption.Tip || !ok || prefix != adoption.PrefixHash {
		return fmt.Errorf("prepared WAL continuity mismatch")
	}
	return nil
}

// recoverPreparedV1Alpha2 installs verified history in the actual local WAL.
// It does not open listeners, admit writes, or grant authority.
// The caller must control DataDir and prevent changes after loading prepared.
func (n *Node) recoverPreparedV1Alpha2(ctx context.Context, prepared *recovery.VerifiedPreparedStartup) (resultErr error) {
	if n == nil || n.config == nil || n.config.Local || n.config.NodeID == "" || n.config.DataDir == "" || prepared == nil || prepared.View() == nil || n.opened.Load() {
		return fmt.Errorf("invalid non-serving prepared recovery")
	}
	dataDir, err := filepath.Abs(n.config.DataDir)
	if err != nil {
		return err
	}
	preparedDir, err := filepath.Abs(prepared.Directory())
	if err != nil || dataDir != preparedDir {
		return fmt.Errorf("prepared directory does not match DataDir")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	_, lock, err := qlog.Acquire(filepath.Join(dataDir, "qlog"))
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, lock.Release()) }()
	localVoter, err := loadVoterIdentity(n.config)
	if err != nil {
		return err
	}
	wal, err := qlog.Open(filepath.Join(dataDir, "qlog"))
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, wal.Close()) }()
	maxBytes := n.config.MaxWALBytes
	if maxBytes == 0 {
		maxBytes = 4 << 30
	}
	if maxBytes < 0 {
		return fmt.Errorf("invalid WAL limit")
	}
	if err := wal.SetMaxBytes(maxBytes); err != nil {
		return err
	}
	if configured, err := validateObjectStoreConfig(n.config); err != nil {
		return err
	} else if !configured {
		return fmt.Errorf("prepared startup requires a configured target store")
	}
	provider := objectstore.Provider(n.config.ObjStoreProvider)
	if provider == "" {
		provider = objectstore.ProviderS3
	}
	bucket, err := objectstore.NewBucketWithoutProvisioning(ctx, objectstore.Config{
		Provider: provider, FilesystemDir: n.config.ObjStoreDir, Prefix: n.config.ObjStorePrefix,
		Endpoint: n.config.ObjStoreEndpoint, Bucket: n.config.ObjStoreBucket, Region: n.config.ObjStoreRegion,
		Insecure: n.config.ObjStoreInsecure, MaxRetries: n.config.ObjStoreRetries,
		AccessKey: n.config.ObjStoreAccessKey, SecretKey: n.config.ObjStoreSecretKey, SessionToken: n.config.ObjStoreSessionToken,
		ServiceAccount: n.config.ObjStoreServiceAccount, AzureTenantID: n.config.ObjStoreAzureTenantID,
		AzureClientID: n.config.ObjStoreAzureClientID, AzureClientSecret: n.config.ObjStoreAzureClientSecret,
		AzureStorageAccount: n.config.ObjStoreAzureStorageAccount, AzureStorageAccountKey: n.config.ObjStoreAzureStorageAccountKey,
		AzureConnectionString: n.config.ObjStoreAzureConnectionString, AzureUserAssignedID: n.config.ObjStoreAzureUserAssignedID,
	})
	if err != nil {
		return fmt.Errorf("open target store: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, bucket.Close()) }()
	if err := ensureVoterIdentityManaged(ctx, n.config, bucket, wal, localVoter, false, true); err != nil {
		return err
	}
	cluster := n.loadClusterConfig()
	transport := network.NewTransport(n.config.ClusterID, n.config.NodeID, cluster, n.config.PeerToken)
	transport.SetAdminToken(n.config.AdminToken)
	defer func() { resultErr = errors.Join(resultErr, transport.Close()) }()
	newCore := quepaxa.New
	if n.config.Learner != nil {
		newCore = quepaxa.NewLearner
	}
	core, err := newCore(quepaxa.Config{NodeID: n.config.NodeID, Cluster: *cluster, WAL: wal, Transport: transport, EnableReconfiguration: n.config.EnableReconfiguration})
	if err != nil {
		return err
	}
	view := prepared.View()
	if core.Tip() > view.Tip() {
		return fmt.Errorf("existing WAL is ahead of prepared history; preserve it for restart")
	}
	if core.Tip() == view.Tip() {
		prefix, ok := core.PrefixHash(view.Tip())
		if !ok || prefix != view.PrefixHash() {
			return fmt.Errorf("existing WAL differs from prepared history")
		}
		return persistPreparedAdoption(dataDir, prepared.Identity(), core.WALIdentity(), view)
	}
	seal, decision, hasBase := view.RecoveryBase()
	if core.Tip() < view.Base() {
		if core.Tip() != 0 {
			return fmt.Errorf("existing WAL ends before prepared checkpoint base; preserve it for reconciliation")
		}
		if !hasBase {
			return fmt.Errorf("prepared history requires an unsupported generation base")
		}
		core.SetCheckpointValidator(func(ctx context.Context, candidate quepaxa.CheckpointSeal) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if !reflect.DeepEqual(candidate, seal) {
				return fmt.Errorf("checkpoint differs from verified source")
			}
			return nil
		})
		if err := core.RestoreCheckpointBase(ctx, seal, decision); err != nil {
			return fmt.Errorf("install certified base: %w", err)
		}
	}
	if view.Base() > 0 {
		if !hasBase {
			return fmt.Errorf("prepared history requires an unsupported generation base")
		}
		prefix, ok := core.PrefixHash(view.Base())
		if !ok || prefix != seal.PrefixHash {
			return fmt.Errorf("existing WAL checkpoint prefix differs from prepared history")
		}
	}
	for _, value := range view.Decisions() {
		if value.Slot > core.Tip() {
			break
		}
		existing, ok := core.CertifiedValue(value.Slot)
		if !ok || !reflect.DeepEqual(existing, value) {
			return fmt.Errorf("existing WAL certified prefix differs from prepared history")
		}
	}
	for _, value := range view.Decisions() {
		if value.Slot <= core.Tip() {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := core.AcceptCertifiedValues([]quepaxa.DecidedValue{value}); err != nil {
			return fmt.Errorf("install certified suffix: %w", err)
		}
	}
	prefix, ok := core.PrefixHash(view.Tip())
	if core.Tip() != view.Tip() || !ok || prefix != view.PrefixHash() {
		return fmt.Errorf("local WAL does not match prepared history")
	}
	return persistPreparedAdoption(dataDir, prepared.Identity(), core.WALIdentity(), view)
}

type preparedAdoption struct {
	Identity    recovery.PreparedStartupIdentity `json:"identity"`
	WALIdentity string                           `json:"walIdentity"`
	Tip         quepaxa.Slot                     `json:"tip"`
	PrefixHash  [32]byte                         `json:"prefixHash"`
}

func persistPreparedAdoption(dir string, identity recovery.PreparedStartupIdentity, walID string, view *recovery.ReadOnlyView) error {
	if walID == "" {
		return fmt.Errorf("real WAL identity is missing")
	}
	want, err := json.Marshal(preparedAdoption{Identity: identity, WALIdentity: walID, Tip: view.Tip(), PrefixHash: view.PrefixHash()})
	if err != nil {
		return err
	}
	name := filepath.Join(dir, "prepared-adoption.json")
	if existing, err := readPreparedAdoption(name); err == nil {
		if !bytes.Equal(existing, want) {
			return fmt.Errorf("prepared adoption identity mismatch")
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, "prepared-adoption.pending"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(want)
	if writeErr == nil {
		writeErr = f.Sync()
	}
	if err := errors.Join(writeErr, f.Close()); err != nil {
		return err
	}
	if err := os.Link(filepath.Join(dir, "prepared-adoption.pending"), name); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("adoption outcome unknown: %w", err)
	}
	if err := errors.Join(d.Sync(), d.Close()); err != nil {
		return fmt.Errorf("adoption outcome unknown: %w", err)
	}
	return nil
}

func readPreparedAdoption(name string) (data []byte, resultErr error) {
	info, err := os.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > 16384 {
		return nil, fmt.Errorf("invalid adoption file")
	}
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, f.Close()) }()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return nil, fmt.Errorf("adoption file changed")
	}
	data, err = io.ReadAll(io.LimitReader(f, 16385))
	if err != nil {
		return nil, err
	}
	if len(data) > 16384 {
		return nil, fmt.Errorf("adoption file too large")
	}
	return data, nil
}

// PreparedBindingDigests identifies the configured source and consensus inputs;
// no credentials are embedded in the returned digests.
func (n *Node) preparedBindingDigests() (string, string, error) {
	provider := n.config.ObjStoreProvider
	if provider == "" {
		provider = string(objectstore.ProviderS3)
	}
	endpoint := n.config.ObjStoreEndpoint
	account := n.config.ObjStoreAzureStorageAccount
	fsDir := ""
	if provider == string(objectstore.ProviderFilesystem) {
		var err error
		fsDir = n.config.ObjStoreDir
		if fsDir == "" {
			fsDir = "./objstore"
		}
		fsDir, err = filepath.Abs(fsDir)
		if err != nil {
			return "", "", err
		}
	}
	if provider == string(objectstore.ProviderAzure) && n.config.ObjStoreAzureConnectionString != "" {
		parts := map[string]string{}
		items := strings.Split(n.config.ObjStoreAzureConnectionString, ";")
		for i, item := range items {
			if item == "" && i == len(items)-1 {
				continue
			}
			key, value, ok := strings.Cut(item, "=")
			if !ok {
				return "", "", fmt.Errorf("invalid Azure connection descriptor")
			}
			key = strings.ToLower(strings.TrimSpace(key))
			if _, exists := parts[key]; exists {
				return "", "", fmt.Errorf("duplicate Azure connection field")
			}
			parts[key] = value
		}
		if parts["accountname"] == "" || parts["accountname"] != account || parts["blobendpoint"] == "" {
			return "", "", fmt.Errorf("Azure connection requires matching account and explicit blob endpoint")
		}
		endpoint = parts["blobendpoint"]
	}
	if strings.Contains(endpoint, "://") {
		u, err := url.Parse(endpoint)
		if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
			return "", "", fmt.Errorf("invalid storage endpoint")
		}
	}
	source, _ := json.Marshal(struct {
		Provider, Endpoint, Bucket, Prefix, Account, FilesystemDir, Cluster, Region string
		Insecure                                                                    bool
	}{provider, endpoint, n.config.ObjStoreBucket, path.Clean(n.config.ObjStorePrefix), account, fsDir, string(n.config.ClusterID), n.config.ObjStoreRegion, n.config.ObjStoreInsecure})
	config, _ := json.Marshal(struct {
		Cluster         string
		Node            string
		Members         []quepaxa.Member
		Learner         *quepaxa.Member
		Durability      string
		Reconfiguration bool
	}{string(n.config.ClusterID), string(n.config.NodeID), n.config.Members, n.config.Learner, string(n.config.ObjStoreDurability), n.config.EnableReconfiguration})
	return fmt.Sprintf("%x", sha256.Sum256(source)), fmt.Sprintf("%x", sha256.Sum256(config)), nil
}

func (n *Node) PreparedStartupBindingDigests() (string, string, error) {
	if n == nil || n.config == nil || n.config.Local || n.config.NodeID == "" || n.config.ClusterID == "" {
		return "", "", fmt.Errorf("invalid prepared startup configuration")
	}
	return n.preparedBindingDigests()
}
