package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/mrchypark/rhiza/internal/atomicdir"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

// PreparedStartupIdentity binds prepared data to caller-supplied identities;
// it does not prove authority, source ownership, or permission to start.
type PreparedStartupIdentity struct {
	ClusterUID    string `json:"clusterUID"`
	Generation    int64  `json:"generation"`
	DeploymentUID string `json:"deploymentUID"`
	PodUID        string `json:"podUID"`
	NodeID        string `json:"nodeID"`
	SourceBinding string `json:"sourceBinding"`
	ConfigDigest  string `json:"configDigest"`
}

type preparedStartupManifest struct {
	Version      int                             `json:"version"`
	Identity     PreparedStartupIdentity         `json:"identity"`
	Head         []byte                          `json:"head"`
	SQLiteHash   [32]byte                        `json:"sqliteHash"`
	SuffixHash   [32]byte                        `json:"suffixHash"`
	Files        []preparedGraphArtifact         `json:"files,omitempty"`
	GraphLocator *checkpoint.ManagedGraphLocator `json:"graphLocator,omitempty"`
}

// VerifiedPreparedStartup is verified local data, never startup authority.
type VerifiedPreparedStartup struct {
	dir      string
	view     *ReadOnlyView
	identity PreparedStartupIdentity
}

func (p *VerifiedPreparedStartup) Directory() string                 { return p.dir }
func (p *VerifiedPreparedStartup) View() *ReadOnlyView               { return p.view }
func (p *VerifiedPreparedStartup) Identity() PreparedStartupIdentity { return p.identity }

func validPreparedIdentity(i PreparedStartupIdentity) bool {
	return i.ClusterUID != "" && i.Generation >= 0 && i.DeploymentUID != "" && i.PodUID != "" && i.NodeID != "" && i.SourceBinding != "" && i.ConfigDigest != ""
}

func validatePreparedProfile(identity PreparedStartupIdentity, policy sqlpolicy.MaterializationPolicy, window uint64, applicationProtocol string) error {
	if err := validateReadOnlyProfile(policy, window, applicationProtocol); err != nil {
		return err
	}
	if (policy == sqlpolicy.ManagedRequestMaterializationPolicy || policy == sqlpolicy.ManagedGraphRequestMaterializationPolicy) && identity.Generation != 0 {
		return fmt.Errorf("final managed recovery requires generation 0")
	}
	return nil
}

// PublishPreparedStartup prepares a new directory, then syncs data and writes a
// completion marker. The caller controls its parent and excludes replacement.
// A failed attempt remains in place. An error after marker publication is an
// uncertain outcome and must be resolved by loading and verifying the result.
func PublishPreparedStartup(ctx context.Context, view *ReadOnlyView, targetDir string, identity PreparedStartupIdentity) error {
	return PublishPreparedStartupForPolicy(ctx, view, targetDir, identity, sqlpolicy.LegacyMaterializationPolicy, types.DefaultIdempotencyWindowSlots)
}

// PublishPreparedStartupForPolicy publishes compatibility data, not authority.
func PublishPreparedStartupForPolicy(ctx context.Context, view *ReadOnlyView, targetDir string, identity PreparedStartupIdentity, expected sqlpolicy.MaterializationPolicy, window uint64) error {
	version, err := preparedVersion(expected, window)
	if err != nil {
		return err
	}
	if view == nil {
		return fmt.Errorf("read-only view is not verified")
	}
	if err := validatePreparedProfile(identity, expected, window, view.applicationProtocol); err != nil {
		return err
	}
	if !validPreparedIdentity(identity) {
		return fmt.Errorf("invalid prepared startup identity")
	}
	result, err := PrepareReadOnlyRestoreForPolicy(ctx, view, targetDir, expected, window)
	if err != nil {
		return err
	}
	parent, err := os.Open(filepath.Dir(targetDir))
	if err != nil {
		return fmt.Errorf("sync preparation parent: %w", err)
	}
	if err := errors.Join(parent.Sync(), parent.Close()); err != nil {
		return fmt.Errorf("sync preparation parent: %w", err)
	}
	m := preparedStartupManifest{Version: version, Identity: identity, Head: view.CapturedHead()}
	if expected == sqlpolicy.ManagedGraphRequestMaterializationPolicy && view.graphLocator == nil {
		return fmt.Errorf("prepared Graph source lacks exact certified locator")
	}
	// Reuse the existing locator wire shape for policy 9. Its absence in v3
	// preserves the original archive-only selection, including old handoffs.
	if view.graphLocator != nil {
		selected := *view.graphLocator
		m.GraphLocator = &selected
	}
	if expected == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		m.Files, err = preparedGraphArtifacts(ctx, targetDir, true)
		if err != nil {
			return err
		}
		m.SQLiteHash = m.Files[0].SHA256
	} else if m.SQLiteHash, err = preparedFileHash(result.SQLitePath, true); err != nil {
		return err
	}
	if m.SuffixHash, err = preparedFileHash(result.SuffixPath, true); err != nil {
		return err
	}
	if expected == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		if err := validateGraphPreparedInventory(targetDir, false); err != nil {
			return err
		}
	} else if expected == sqlpolicy.LegacyMaterializationPolicy {
		if _, err = preparedGraphTree(targetDir, true); err != nil {
			return err
		}
	} else if err := validateMixedPreparedInventory(targetDir, false); err != nil {
		return err
	}
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(targetDir, "completion.pending"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(data)
	if writeErr == nil {
		writeErr = f.Sync()
	}
	closeErr := f.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Link(filepath.Join(targetDir, "completion.pending"), filepath.Join(targetDir, "completed.json")); err != nil {
		return err
	}
	dir, err := os.Open(targetDir)
	if err != nil {
		return fmt.Errorf("completion publication outcome unknown: %w", err)
	}
	err = dir.Sync()
	closeErr = dir.Close()
	if err != nil {
		return fmt.Errorf("completion publication outcome unknown: %w", errors.Join(err, closeErr))
	}
	if closeErr != nil {
		return fmt.Errorf("completion publication outcome unknown: %w", closeErr)
	}
	return nil
}

// LoadPreparedStartup verifies captured history and reconstructs it into a new
// caller-controlled target. The init data is not adopted. Both parents must be
// controlled against replacement; failed targets remain for inspection. This
// does not authenticate the mutable manifest or grant permission to write.
func LoadPreparedStartup(ctx context.Context, bucket objstore.Bucket, prefix string, configID uint, bootstrap quepaxa.Cluster, handoffDir, verifiedTargetDir string, expected PreparedStartupIdentity) (*VerifiedPreparedStartup, error) {
	return LoadPreparedStartupForPolicy(ctx, bucket, prefix, configID, bootstrap, handoffDir, verifiedTargetDir, expected, sqlpolicy.LegacyMaterializationPolicy, types.DefaultIdempotencyWindowSlots)
}

func LoadPreparedStartupForPolicy(ctx context.Context, bucket objstore.Bucket, prefix string, configID uint, bootstrap quepaxa.Cluster, handoffDir, verifiedTargetDir string, expected PreparedStartupIdentity, policy sqlpolicy.MaterializationPolicy, window uint64) (*VerifiedPreparedStartup, error) {
	return loadPreparedStartupForPolicy(ctx, bucket, prefix, configID, bootstrap, handoffDir, verifiedTargetDir, expected, policy, window, nil, "")
}

func LoadPreparedStartupForPolicyWithReconfiguration(ctx context.Context, bucket objstore.Bucket, prefix string, configID uint, bootstrap quepaxa.Cluster, handoffDir, verifiedTargetDir string, expected PreparedStartupIdentity, policy sqlpolicy.MaterializationPolicy, window uint64, reconfiguration bool) (*VerifiedPreparedStartup, error) {
	return loadPreparedStartupForPolicy(ctx, bucket, prefix, configID, bootstrap, handoffDir, verifiedTargetDir, expected, policy, window, &reconfiguration, "")
}

// LoadPreparedStartupForApplicationProtocol verifies the handoff and captured
// source head against a trusted final append-only protocol before independent
// reconstruction. The protocol is never inferred from files or manifest data.
func LoadPreparedStartupForApplicationProtocol(ctx context.Context, bucket objstore.Bucket, prefix string, configID uint, bootstrap quepaxa.Cluster, handoffDir, verifiedTargetDir string, expected PreparedStartupIdentity, policy sqlpolicy.MaterializationPolicy, window uint64, applicationProtocol string, reconfiguration bool) (*VerifiedPreparedStartup, error) {
	if err := validatePreparedProfile(expected, policy, window, applicationProtocol); err != nil {
		return nil, err
	}
	return loadPreparedStartupForPolicy(ctx, bucket, prefix, configID, bootstrap, handoffDir, verifiedTargetDir, expected, policy, window, &reconfiguration, applicationProtocol)
}

// LoadPreparedStartupForPolicyIntoStagingAndPromote verifies and reconstructs
// into an absent sibling staging directory, then atomically promotes it to the
// absent final directory. The returned verified object names the final path.
// A failed reconstruction leaves staging for diagnosis; this function never
// removes partial evidence or replaces an existing final directory.
func LoadPreparedStartupForPolicyIntoStagingAndPromote(ctx context.Context, bucket objstore.Bucket, prefix string, configID uint, bootstrap quepaxa.Cluster, handoffDir, stagingDir, finalDir string, expected PreparedStartupIdentity, policy sqlpolicy.MaterializationPolicy, window uint64, applicationProtocol string, reconfiguration bool) (*VerifiedPreparedStartup, error) {
	prepared, err := loadPreparedStartupForPolicy(ctx, bucket, prefix, configID, bootstrap, handoffDir, stagingDir, expected, policy, window, &reconfiguration, applicationProtocol)
	if err != nil {
		return nil, err
	}
	if err := promotePreparedStartupDirectory(stagingDir, finalDir); err != nil {
		return nil, err
	}
	prepared.dir = filepath.Clean(finalDir)
	return prepared, nil
}

func promotePreparedStartupDirectory(stagingDir, finalDir string) error {
	if !filepath.IsAbs(stagingDir) || !filepath.IsAbs(finalDir) || filepath.Clean(stagingDir) != stagingDir || filepath.Clean(finalDir) != finalDir || stagingDir == finalDir || filepath.Dir(stagingDir) != filepath.Dir(finalDir) || filepath.Base(stagingDir) == "." || filepath.Base(finalDir) == "." {
		return fmt.Errorf("invalid prepared startup promotion paths")
	}
	if err := atomicdir.RenameNoReplace(stagingDir, finalDir); err != nil {
		return fmt.Errorf("promote prepared startup: %w", err)
	}
	return nil
}

type preparedStartupParse struct {
	manifest     preparedStartupManifest
	handoffState materializer.ManagedApplicationState
	handoffTip   uint64
	suffix       []byte
}

// ValidatePreparedStartupDirectoryForPolicy validates only bounded local
// prepared evidence. It never opens source history or grants startup authority.
func ValidatePreparedStartupDirectoryForPolicy(ctx context.Context, handoffDir string, expected PreparedStartupIdentity, policy sqlpolicy.MaterializationPolicy, window uint64, applicationProtocol string) error {
	_, err := parsePreparedStartupDirectory(ctx, handoffDir, expected, policy, window, applicationProtocol)
	return err
}

func parsePreparedStartupDirectory(ctx context.Context, handoffDir string, expected PreparedStartupIdentity, policy sqlpolicy.MaterializationPolicy, window uint64, applicationProtocol string) (preparedStartupParse, error) {
	var result preparedStartupParse
	if err := validatePreparedProfile(expected, policy, window, applicationProtocol); err != nil {
		return result, err
	}
	version, err := preparedVersion(policy, window)
	if err != nil {
		return result, err
	}
	if !validPreparedIdentity(expected) {
		return result, fmt.Errorf("invalid expected identity")
	}
	info, err := os.Lstat(handoffDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return result, fmt.Errorf("invalid preparation directory")
	}
	data, err := readPreparedMarker(filepath.Join(handoffDir, "completed.json"))
	if err != nil {
		return result, err
	}
	if len(data) > maxArchiveHeadSize*2 {
		return result, fmt.Errorf("preparation marker too large")
	}
	var m preparedStartupManifest
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err = d.Decode(&m); err != nil {
		return result, err
	}
	if err = d.Decode(new(any)); err != io.EOF {
		return result, fmt.Errorf("invalid preparation marker")
	}
	if m.Version != version || m.Identity != expected {
		return result, fmt.Errorf("preparation identity mismatch")
	}
	if policy != sqlpolicy.ManagedGraphRequestMaterializationPolicy && len(m.Files) != 0 {
		return result, fmt.Errorf("unexpected prepared artifact inventory")
	}
	if policy == sqlpolicy.ManagedGraphRequestMaterializationPolicy && (m.GraphLocator == nil || len(m.Files) == 0) {
		return result, fmt.Errorf("prepared Graph locator/profile mismatch")
	}
	if policy != sqlpolicy.ManagedGraphRequestMaterializationPolicy && policy != sqlpolicy.ManagedRequestMaterializationPolicy {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(data, &fields); err != nil {
			return result, err
		}
		for field := range fields {
			if strings.EqualFold(field, "graphLocator") {
				return result, fmt.Errorf("unexpected Graph locator in legacy preparation")
			}
		}
	}
	if len(m.Head) == 0 {
		return result, fmt.Errorf("preparation head missing")
	}
	if policy == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		if err := validateGraphPreparedInventory(handoffDir, true); err != nil {
			return result, err
		}
		actual, err := preparedGraphArtifacts(ctx, handoffDir, false)
		if err != nil {
			return result, err
		}
		if !equalGraphArtifacts(actual, m.Files) {
			return result, fmt.Errorf("prepared Graph artifact inventory differs")
		}
	}
	if managedSQLKVPolicy(policy) {
		if err := validateMixedPreparedInventory(handoffDir, true); err != nil {
			return result, err
		}
		result.handoffState, result.handoffTip, err = inspectManagedFileForPolicy(ctx, filepath.Join(handoffDir, "sqlite.db"), window, policy, applicationProtocol)
		if err != nil {
			return result, fmt.Errorf("inspect mixed handoff: %w", err)
		}
	}
	var sqlHash [32]byte
	var hashErr error
	if policy == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		sqlHash = m.Files[0].SHA256
	} else {
		sqlHash, hashErr = preparedFileHash(filepath.Join(handoffDir, "sqlite.db"), false)
	}
	if hashErr != nil || sqlHash != m.SQLiteHash {
		return result, fmt.Errorf("prepared SQLite integrity mismatch")
	}
	suffix := filepath.Join(handoffDir, "certified-suffix.json")
	suffixHash, hashErr := preparedFileHash(suffix, false)
	if hashErr != nil || suffixHash != m.SuffixHash {
		return result, fmt.Errorf("prepared suffix integrity mismatch")
	}
	result.suffix, err = os.ReadFile(suffix)
	if err != nil {
		return result, err
	}
	result.manifest = m
	return result, nil
}

// VerifyRestoredPreparedStartupForPolicy verifies an already materialized
// DataDir against a prepared handoff and its already verified source view. It
// never reconstructs or replaces DataDir; qlog and adoption-pending files are
// unrelated local state and remain untouched.
func VerifyRestoredPreparedStartupForPolicy(ctx context.Context, dataDir, handoffDir string, view *ReadOnlyView, expected PreparedStartupIdentity, policy sqlpolicy.MaterializationPolicy, window uint64, applicationProtocol string) (*VerifiedPreparedStartup, error) {
	if view == nil || !view.verified || view.expectedPolicy != policy || view.idempotencyWindow != window || view.applicationProtocol != applicationProtocol {
		return nil, fmt.Errorf("restored prepared source view profile mismatch")
	}
	parsed, err := parsePreparedStartupDirectory(ctx, handoffDir, expected, policy, window, applicationProtocol)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(parsed.manifest.Head, view.CapturedHead()) {
		return nil, fmt.Errorf("restored prepared source head mismatch")
	}
	wantSuffix, err := json.Marshal(view.Decisions())
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(parsed.suffix, wantSuffix) {
		return nil, fmt.Errorf("prepared suffix differs from verified history")
	}
	info, err := os.Lstat(dataDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("invalid restored data directory")
	}
	for _, name := range []string{"qlog", "checkpoint"} {
		path := filepath.Join(dataDir, name)
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("unsafe restored local directory %q", name)
		}
	}
	for _, name := range []string{"prepared-adoption.json", "prepared-adoption.pending"} {
		path := filepath.Join(dataDir, name)
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("unsafe restored adoption file %q", name)
		}
	}
	sqlitePath := filepath.Join(dataDir, "sqlite.db")
	suffixPath := filepath.Join(dataDir, "certified-suffix.json")
	for _, path := range []string{sqlitePath, suffixPath} {
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("invalid restored prepared file")
		}
	}
	restoredSuffix, err := os.ReadFile(suffixPath)
	if err != nil || !bytes.Equal(restoredSuffix, wantSuffix) {
		return nil, fmt.Errorf("restored suffix differs from verified history")
	}
	if policy == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		if _, err := preparedGraphArtifacts(ctx, dataDir, false); err != nil {
			return nil, fmt.Errorf("invalid restored Graph preparation: %w", err)
		}
	}
	if policy == sqlpolicy.LegacyMaterializationPolicy {
		materialized, err := materializer.Open(sqlitePath, 1, window)
		if err != nil {
			return nil, fmt.Errorf("open restored prepared state: %w", err)
		}
		if materialized.Tip() != uint64(view.Tip()) {
			_ = materialized.Close()
			return nil, fmt.Errorf("restored prepared tip differs from verified history")
		}
		if err := materialized.Close(); err != nil {
			return nil, fmt.Errorf("close restored prepared state: %w", err)
		}
	} else {
		state, tip, err := inspectManagedFileForPolicy(ctx, sqlitePath, window, policy, applicationProtocol)
		if err != nil || tip != uint64(view.Tip()) || state != parsed.handoffState {
			return nil, fmt.Errorf("restored prepared state or tip differs from verified history")
		}
	}
	return &VerifiedPreparedStartup{dir: dataDir, view: view, identity: expected}, nil
}

func loadPreparedStartupForPolicy(ctx context.Context, bucket objstore.Bucket, prefix string, configID uint, bootstrap quepaxa.Cluster, handoffDir, verifiedTargetDir string, expected PreparedStartupIdentity, policy sqlpolicy.MaterializationPolicy, window uint64, trustedReconfiguration *bool, applicationProtocol string) (*VerifiedPreparedStartup, error) {
	parsed, err := parsePreparedStartupDirectory(ctx, handoffDir, expected, policy, window, applicationProtocol)
	if err != nil {
		return nil, err
	}
	m := parsed.manifest
	view, err := openReadOnlyViewAtHeadForPolicy(ctx, bucket, prefix, configID, bootstrap, m.Head, policy, window, m.GraphLocator, trustedReconfiguration, applicationProtocol)
	if err != nil {
		return nil, err
	}
	if managedSQLKVPolicy(policy) && parsed.handoffTip != uint64(view.Tip()) {
		return nil, fmt.Errorf("mixed handoff tip differs from verified history")
	}
	want, err := json.Marshal(view.Decisions())
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(parsed.suffix, want) {
		return nil, fmt.Errorf("prepared suffix differs from verified history")
	}
	verified, err := PrepareReadOnlyRestoreForPolicy(ctx, view, verifiedTargetDir, policy, window)
	if err != nil {
		return nil, fmt.Errorf("reconstruct prepared state in %s: %w", verifiedTargetDir, err)
	}
	if policy == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		if _, err := preparedGraphArtifacts(ctx, verifiedTargetDir, true); err != nil {
			return nil, err
		}
	} else if _, err = preparedFileHash(verified.SQLitePath, true); err != nil {
		return nil, err
	}
	if _, err = preparedFileHash(verified.SuffixPath, true); err != nil {
		return nil, err
	}
	if policy == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		if err := validateGraphPreparedInventory(verifiedTargetDir, false); err != nil {
			return nil, err
		}
	} else if policy == sqlpolicy.LegacyMaterializationPolicy {
		if _, err = preparedGraphTree(verifiedTargetDir, true); err != nil {
			return nil, err
		}
	} else {
		if err := validateMixedPreparedInventory(verifiedTargetDir, false); err != nil {
			return nil, err
		}
		state, tip, err := inspectManagedFileForPolicy(ctx, verified.SQLitePath, window, policy, applicationProtocol)
		if err != nil {
			return nil, err
		}
		if tip != parsed.handoffTip || state != parsed.handoffState {
			return nil, fmt.Errorf("mixed handoff evidence differs from independent reconstruction")
		}
	}
	for _, dir := range []string{verifiedTargetDir, filepath.Dir(verifiedTargetDir)} {
		f, err := os.Open(dir)
		if err != nil {
			return nil, err
		}
		if err := errors.Join(f.Sync(), f.Close()); err != nil {
			return nil, fmt.Errorf("sync reconstruction directory %s: %w", dir, err)
		}
	}
	return &VerifiedPreparedStartup{dir: verifiedTargetDir, view: view, identity: expected}, nil
}

func preparedVersion(policy sqlpolicy.MaterializationPolicy, window uint64) (int, error) {
	if err := validateReadOnlyPolicy(policy, window); err != nil {
		return 0, err
	}
	if policy == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		return 4, nil
	}
	if policy == sqlpolicy.ManagedRequestMaterializationPolicy {
		return 3, nil
	}
	if managedSQLKVPolicy(policy) {
		return 2, nil
	}
	if window != types.DefaultIdempotencyWindowSlots {
		return 0, fmt.Errorf("legacy handoff requires the default idempotency window")
	}
	return 1, nil
}

// The caller excludes source replacement. Lstat prevents following entries
// outside its handoff/cache before any SQLite inspector opens them.
func validateMixedPreparedInventory(root string, completed bool) error {
	info, err := os.Lstat(root)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("invalid mixed preparation directory")
	}
	if err := materializer.ValidateManagedRestoreState(filepath.Join(root, "sqlite.db")); err != nil {
		return err
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	seen := make(map[string]bool, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		info, err := os.Lstat(filepath.Join(root, name))
		if err != nil {
			return err
		}
		if name == "checkpoint" {
			if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("invalid mixed checkpoint cache")
			}
			if err := validateMixedCheckpointCache(filepath.Join(root, name)); err != nil {
				return err
			}
		} else {
			switch name {
			case "sqlite.db", "sqlite.db-wal", "sqlite.db-shm", "certified-suffix.json", "completed.json", "completion.pending":
			default:
				return fmt.Errorf("unsupported mixed preparation entry %q", name)
			}
			if !info.Mode().IsRegular() {
				return fmt.Errorf("invalid mixed preparation file %q", name)
			}
		}
		seen[name] = true
	}
	for _, name := range []string{"checkpoint", "sqlite.db", "certified-suffix.json"} {
		if !seen[name] {
			return fmt.Errorf("missing mixed preparation entry %q", name)
		}
	}
	if completed && !seen["completed.json"] {
		return fmt.Errorf("mixed completion marker missing")
	}
	return nil
}

func validateMixedCheckpointCache(dir string) error {
	if err := materializer.ValidateManagedRestoreState(filepath.Join(dir, "sqlite.db")); err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		switch entry.Name() {
		case "sqlite.db", "sqlite.db-wal", "sqlite.db-shm":
		default:
			return fmt.Errorf("unsupported mixed checkpoint cache entry %q", entry.Name())
		}
		info, err := os.Lstat(filepath.Join(dir, entry.Name()))
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("invalid mixed checkpoint cache file")
		}
	}
	return nil
}

func readPreparedMarker(name string) (data []byte, resultErr error) {
	info, err := os.Lstat(name)
	if err != nil || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("invalid preparation marker")
	}
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, f.Close()) }()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return nil, fmt.Errorf("preparation marker changed")
	}
	limit := int64(maxArchiveHeadSize * 2)
	data, err = io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("preparation marker too large")
	}
	return data, nil
}

func preparedGraphTree(root string, sync bool) (map[string][32]byte, error) {
	graph := filepath.Join(root, "latticedb")
	result := make(map[string][32]byte)
	if _, err := os.Lstat(graph); errors.Is(err, os.ErrNotExist) {
		return result, nil
	} else if err != nil {
		return nil, err
	}
	var dirs []string
	err := filepath.WalkDir(graph, func(name string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("invalid graph symlink")
		}
		if entry.IsDir() {
			dirs = append(dirs, name)
			return nil
		}
		rel, err := filepath.Rel(graph, name)
		if err != nil {
			return err
		}
		hash, err := preparedFileHash(name, sync)
		if err != nil {
			return err
		}
		result[rel] = hash
		return nil
	})
	if err != nil {
		return nil, err
	}
	if sync {
		for i := len(dirs) - 1; i >= 0; i-- {
			f, err := os.Open(dirs[i])
			if err != nil {
				return nil, err
			}
			syncErr := f.Sync()
			closeErr := f.Close()
			if err = errors.Join(syncErr, closeErr); err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

func preparedFileHash(name string, sync bool) (sum [32]byte, resultErr error) {
	info, err := os.Lstat(name)
	if err != nil || !info.Mode().IsRegular() || info.Size() > 1<<30 {
		return sum, fmt.Errorf("invalid prepared file")
	}
	f, err := os.Open(name)
	if err != nil {
		return sum, err
	}
	defer func() { resultErr = errors.Join(resultErr, f.Close()) }()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return sum, fmt.Errorf("prepared file changed")
	}
	h := sha256.New()
	if _, err = io.Copy(h, io.LimitReader(f, 1<<30+1)); err != nil {
		return sum, err
	}
	if sync {
		if err = f.Sync(); err != nil {
			return sum, err
		}
	}
	copy(sum[:], h.Sum(nil))
	return sum, nil
}
