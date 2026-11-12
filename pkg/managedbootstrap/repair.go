package managedbootstrap

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"reflect"

	"github.com/mrchypark/rhiza/internal/atomicdir"
	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/managedsql"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	thanosobjstore "github.com/thanos-io/objstore"
)

const RepairHandoffPath = "/var/lib/rhiza/repair.json"
const RepairFileLimit = operator.V1Alpha2RepairRecordLimit

const repairRestoredMarkerName = "repair-restored.json"
const repairRestoredMarkerLimit = 16 << 10
const repairPreparedMarkerHashLimit = 1 << 30

type repairRestoredMarker struct {
	Version        int                              `json:"version"`
	Identity       recovery.PreparedStartupIdentity `json:"identity"`
	PlanDigest     string                           `json:"planDigest"`
	CompletionHash [32]byte                         `json:"completionHash"`
	HeadHash       [32]byte                         `json:"headHash"`
}

type RepairGateFile struct {
	Version int                               `json:"version"`
	Repair  operator.V1Alpha2RepairRecord     `json:"repair"`
	Binding operator.V1Alpha2RepairPodBinding `json:"binding"`
}

func CheckRepairGate(ctx context.Context, kube *operator.Kubernetes, ref operator.V1Alpha2ClusterRef, generation int64, podName, podUID, identity string, allowCompleted bool) (operator.V1Alpha2BootstrapPlan, RepairGateFile, error) {
	var empty operator.V1Alpha2BootstrapPlan
	var output RepairGateFile
	store := &operator.V1Alpha2StatusStore{Kube: kube}
	a, err := operator.LoadV1Alpha2Admission(ctx, store, ref)
	if err != nil {
		return empty, output, err
	}
	r, ok := a.Repair()
	if !ok || r.PreparedHandoff == nil {
		return empty, output, ErrLearnerAdmissionPending
	}
	if !bytes.Equal(bytes.TrimSpace(a.Transition()), []byte("null")) && len(bytes.TrimSpace(a.Transition())) != 0 {
		return empty, output, errors.New("repair overlaps authority transition")
	}
	p := r.Plan
	if p.ClusterUID != ref.UID || p.Generation != generation || p.Generation != a.Authority().Generation || p.DeploymentUID != a.Authority().Binding.DeploymentUID || p.StorageID != a.Authority().Binding.StorageID {
		return empty, output, errors.New("repair execution binding changed")
	}
	if r.State == "Completed" && p.Replacement.PodUID != podUID {
		// The previous repair stays in status until the controller records
		// this new Pod's plan. Its terminal never authorizes the new Pod.
		return empty, output, ErrLearnerAdmissionPending
	}
	if r.State != "Closed" && !(allowCompleted && r.State == "Completed") {
		return empty, output, errors.New("repair startup is not authorized")
	}
	if p.Replacement.PodName != podName || p.Replacement.PodUID != podUID {
		return empty, output, errors.New("repair execution binding changed")
	}
	member := operator.V1Alpha2BootstrapMember{PodUID: podUID, Member: quepaxa.Member{ID: p.Replacement.NodeID, PublicKey: p.Replacement.PublicKey, PeerURL: p.Replacement.PeerURL}}
	plan, err := CheckGateMember(ctx, kube, ref, generation, podName, podUID, identity, &member)
	if err != nil {
		return empty, output, err
	}
	init, err := operator.LoadV1Alpha2ApplicationInit(ctx, store, ref)
	if err != nil || init.Assignment().State != "Consumed" || init.Assignment().Plan.BootstrapPlanDigest != p.BootstrapPlanDigest || init.Assignment().Plan.PhysicalBinding != r.PreparedHandoff.PhysicalBinding {
		return empty, output, errors.New("repair differs from immutable initialization")
	}
	live, err := operator.LoadV1Alpha2Admission(ctx, store, ref)
	if err != nil {
		return empty, output, err
	}
	next, exists := live.Repair()
	if !exists || live.Authority() != a.Authority() || !reflect.DeepEqual(next.Plan, r.Plan) || !reflect.DeepEqual(next.PreparedHandoff, r.PreparedHandoff) || next.State != r.State || next.Completion != nil || (next.AddCompletion != nil && next.Plan.Sequence != operator.V1Alpha2RepairAddThenRemove) || (len(bytes.TrimSpace(live.Transition())) != 0 && !bytes.Equal(bytes.TrimSpace(live.Transition()), []byte("null"))) {
		return empty, output, errors.New("repair changed during gate verification")
	}
	binding, err := operator.ValidateV1Alpha2RepairPodBinding(ctx, store, ref, p)
	if err != nil {
		return empty, output, err
	}
	return plan, RepairGateFile{Version: 1, Repair: r, Binding: binding}, nil
}

// The saved origin identifies this Pod, not the replacement in a later repair.
// This gate does not authorize WAL creation or bypass runtime adoption checks.
func CheckSavedRepairGate(ctx context.Context, kube *operator.Kubernetes, ref operator.V1Alpha2ClusterRef, generation int64, podName, podUID, identity string, saved RepairGateFile) (operator.V1Alpha2BootstrapPlan, error) {
	var empty operator.V1Alpha2BootstrapPlan
	p, h := saved.Repair.Plan, saved.Repair.PreparedHandoff
	if saved.Version != 1 || saved.Repair.State != "Closed" || saved.Repair.Completion != nil || h == nil || p.ClusterUID != ref.UID || p.Generation != generation || p.Replacement.PodUID != podUID || p.Replacement.PodName != podName || h.PlanDigest != p.Digest() || len(h.Head) == 0 || h.HeadHash != fmt.Sprintf("%x", sha256.Sum256(h.Head)) {
		return empty, errors.New("invalid immutable repair origin")
	}
	member := operator.V1Alpha2BootstrapMember{PodUID: podUID, Member: quepaxa.Member{ID: p.Replacement.NodeID, PublicKey: p.Replacement.PublicKey, PeerURL: p.Replacement.PeerURL}}
	plan, err := CheckGateMember(ctx, kube, ref, generation, podName, podUID, identity, &member)
	if err != nil {
		return empty, err
	}
	store := &operator.V1Alpha2StatusStore{Kube: kube}
	init, err := operator.LoadV1Alpha2ApplicationInit(ctx, store, ref)
	if err != nil || init.Assignment().State != "Consumed" || init.Assignment().Plan.BootstrapPlanDigest != p.BootstrapPlanDigest || init.Assignment().Plan.PhysicalBinding != h.PhysicalBinding || init.Authority().Binding.DeploymentUID != p.DeploymentUID || init.Authority().Binding.StorageID != p.StorageID {
		return empty, errors.New("repair origin differs from immutable initialization")
	}
	admission, err := operator.LoadV1Alpha2Admission(ctx, store, ref)
	if err != nil {
		return empty, err
	}
	if raw := bytes.TrimSpace(admission.Transition()); len(raw) != 0 && !bytes.Equal(raw, []byte("null")) {
		return empty, errors.New("repair overlaps authority transition")
	}
	binding, err := operator.ValidateV1Alpha2RepairPodBinding(ctx, store, ref, p)
	if err != nil {
		return empty, err
	}
	if binding != saved.Binding {
		return empty, errors.New("repair origin owner chain changed")
	}
	return plan, nil
}

func WriteGateFile(identity, output string, value any) error {
	if err := ValidateHandoffPath(identity, output); err != nil {
		return err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(data) > RepairFileLimit {
		return errors.New("handoff exceeds bounded file budget")
	}
	f, err := os.CreateTemp(filepath.Dir(output), ".rhiza-handoff-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	if err := os.Rename(tmp, output); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(output))
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}

func WriteImmutableRepairFile(identity, output string, value RepairGateFile) error {
	return WriteImmutableGateFile(identity, output, value)
}

// Link publishes without replacement, including when concurrent awaiters race.
func WriteImmutableGateFile(identity, output string, value any) error {
	if err := ValidateHandoffPath(identity, output); err != nil {
		return err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(data) > RepairFileLimit {
		return errors.New("handoff exceeds bounded file budget")
	}
	f, err := os.CreateTemp(filepath.Dir(output), ".rhiza-repair-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	if err := os.Link(tmp, output); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	info, err := os.Lstat(output)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return errors.New("invalid immutable repair origin")
	}
	published, err := os.Open(output)
	if err != nil {
		return err
	}
	opened, err := published.Stat()
	if err != nil || !os.SameFile(info, opened) {
		_ = published.Close()
		return errors.New("repair origin changed while opening")
	}
	actual, err := io.ReadAll(io.LimitReader(published, RepairFileLimit+1))
	if err == nil && !bytes.Equal(actual, data) {
		err = errors.New("immutable repair origin differs")
	}
	if err == nil {
		err = published.Sync()
	}
	err = errors.Join(err, published.Close())
	if err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(output))
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}

func ReadGateFile(path string, dst any, limit int64, private bool) error {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || private && info.Mode().Perm()&0077 != 0 {
		return errors.New("invalid handoff file")
	}
	f, err := os.Open(path)
	if err != nil {
		return errors.New("handoff unavailable")
	}
	opened, err := f.Stat()
	if err != nil || !os.SameFile(info, opened) {
		_ = f.Close()
		return errors.New("handoff changed while opening")
	}
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	err = errors.Join(err, f.Close())
	if err != nil || int64(len(data)) > limit {
		return errors.New("invalid handoff file")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if d.Decode(dst) != nil || d.Decode(new(any)) != io.EOF {
		return errors.New("invalid handoff encoding")
	}
	return nil
}

func repairPreparationFailed(err error) error {
	return fmt.Errorf("repair preparation failed: %w", err)
}

func repairPreparedVerificationFailed(err error) error {
	return fmt.Errorf("repair prepared data verification failed: %w", err)
}

func readRepairRestoredMarker(dataDir, planDigest string, expected recovery.PreparedStartupIdentity) (repairRestoredMarker, bool, error) {
	var marker repairRestoredMarker
	info, err := os.Lstat(dataDir)
	if errors.Is(err, os.ErrNotExist) {
		return marker, false, nil
	}
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return marker, false, errors.New("unsafe repair data directory")
	}
	name := filepath.Join(dataDir, repairRestoredMarkerName)
	info, err = os.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return marker, false, nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 || info.Size() > repairRestoredMarkerLimit {
		return marker, true, errors.New("invalid repair readiness marker")
	}
	f, err := os.Open(name)
	if err != nil {
		return marker, true, errors.New("repair readiness marker unavailable")
	}
	opened, err := f.Stat()
	if err != nil || !os.SameFile(info, opened) {
		_ = f.Close()
		return marker, true, errors.New("repair readiness marker changed")
	}
	data, readErr := io.ReadAll(io.LimitReader(f, repairRestoredMarkerLimit+1))
	closeErr := f.Close()
	if readErr != nil || closeErr != nil || len(data) > repairRestoredMarkerLimit {
		return marker, true, errors.New("invalid repair readiness marker")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&marker) != nil || decoder.Decode(new(any)) != io.EOF {
		return marker, true, errors.New("invalid repair readiness marker encoding")
	}
	if marker.Version != 1 || marker.CompletionHash == [32]byte{} || marker.HeadHash == [32]byte{} {
		return marker, true, errors.New("invalid repair readiness marker")
	}
	if marker.Identity != expected {
		return marker, true, errors.New("repair readiness marker identity mismatch")
	}
	if marker.PlanDigest == "" || marker.PlanDigest != planDigest {
		return marker, true, errors.New("repair readiness marker plan mismatch")
	}
	return marker, true, nil
}

func writeRepairRestoredMarker(dataDir string, marker repairRestoredMarker) error {
	info, err := os.Lstat(dataDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("unsafe repair data directory")
	}
	data, err := json.Marshal(marker)
	if err != nil || len(data) > repairRestoredMarkerLimit {
		return errors.New("invalid repair readiness marker")
	}
	f, err := os.CreateTemp(dataDir, ".repair-restored-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := errors.Join(f.Sync(), f.Close()); err != nil {
		return err
	}
	name := filepath.Join(dataDir, repairRestoredMarkerName)
	if err := os.Link(tmp, name); err != nil {
		return fmt.Errorf("publish repair readiness marker: %w", err)
	}
	published, err := os.Lstat(name)
	if err != nil || !published.Mode().IsRegular() || published.Mode().Perm() != 0o600 {
		return errors.New("invalid published repair readiness marker")
	}
	dir, err := os.Open(dataDir)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func hashRepairPreparedCompletion(preparedDir string) ([32]byte, error) {
	var sum [32]byte
	name := filepath.Join(preparedDir, "completed.json")
	info, err := os.Lstat(name)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > repairPreparedMarkerHashLimit {
		return sum, errors.New("invalid repair prepared completion marker")
	}
	f, err := os.Open(name)
	if err != nil {
		return sum, err
	}
	opened, err := f.Stat()
	if err != nil || !os.SameFile(info, opened) {
		_ = f.Close()
		return sum, errors.New("repair prepared completion marker changed")
	}
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(f, repairPreparedMarkerHashLimit+1))
	closeErr := f.Close()
	if err != nil || closeErr != nil || n > repairPreparedMarkerHashLimit {
		return sum, errors.Join(err, closeErr, errors.New("invalid repair prepared completion marker"))
	}
	copy(sum[:], h.Sum(nil))
	return sum, nil
}

func repairDataDirDurableEvidence(dataDir string) (bool, error) {
	pending := filepath.Join(dataDir, "prepared-adoption.pending")
	if _, err := os.Lstat(pending); err == nil {
		return true, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	qlog := filepath.Join(dataDir, "qlog")
	info, err := os.Lstat(qlog)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return false, errors.New("unsafe durable qlog evidence")
	}
	entries, err := os.ReadDir(qlog)
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		if entry.Name() != "lock.qlog" {
			return true, nil
		}
	}
	return false, nil
}

func inspectRepairRestoredState(dataDir, planDigest string, expected recovery.PreparedStartupIdentity) (repairRestoredMarker, bool, error) {
	marker, present, err := readRepairRestoredMarker(dataDir, planDigest, expected)
	if err != nil || present {
		return marker, present, err
	}
	durable, err := repairDataDirDurableEvidence(dataDir)
	if err != nil {
		return marker, false, fmt.Errorf("repair durable local evidence inspection failed: %w", err)
	}
	if durable {
		return marker, false, errors.New("repair readiness marker absent while durable local qlog/identity evidence exists; preserve DataDir")
	}
	return marker, false, nil
}

func inspectRepairPreparedDirectory(path string) (exists, complete bool, err error) {
	if err := safeRepairDirectoryParent(path); err != nil {
		return false, false, err
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return false, false, errors.New("unsafe repair prepared directory")
	}
	marker, err := os.Lstat(filepath.Join(path, "completed.json"))
	if errors.Is(err, os.ErrNotExist) {
		return true, false, nil
	}
	if err != nil {
		return false, false, err
	}
	if !marker.Mode().IsRegular() {
		return false, false, errors.New("unsafe repair prepared completion marker")
	}
	return true, true, nil
}

func safeRepairDirectoryParent(path string) error {
	info, err := os.Lstat(filepath.Dir(path))
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("unsafe repair directory parent")
	}
	return nil
}

func reserveRepairSiblingPath(parent, prefix string) (string, error) {
	if prefix == "" || filepath.Base(prefix) != prefix {
		return "", errors.New("invalid repair sibling prefix")
	}
	if err := safeRepairDirectoryParent(filepath.Join(parent, prefix)); err != nil {
		return "", err
	}
	f, err := os.CreateTemp(parent, prefix)
	if err != nil {
		return "", err
	}
	name := f.Name()
	if err := errors.Join(f.Close(), os.Remove(name)); err != nil {
		return "", err
	}
	return name, nil
}

func quarantineRepairDirectory(path, prefix string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("unsafe repair quarantine source")
	}
	parent := filepath.Dir(path)
	target, err := reserveRepairSiblingPath(parent, prefix)
	if err != nil {
		return "", err
	}
	if err := atomicdir.RenameNoReplace(path, target); err != nil {
		return "", fmt.Errorf("quarantine repair directory: %w", err)
	}
	return target, nil
}

func ensureRepairPreparedDirectory(path string, validate, publish func() error) error {
	exists, complete, err := inspectRepairPreparedDirectory(path)
	if err != nil {
		return err
	}
	if complete {
		if err := validate(); err == nil {
			return nil
		}
		complete = false
	}
	if exists && !complete {
		if _, err := quarantineRepairDirectory(path, "."+filepath.Base(path)+"-quarantine-"); err != nil {
			return err
		}
	}
	if !complete {
		if err := publish(); err != nil {
			return err
		}
		exists, complete, err = inspectRepairPreparedDirectory(path)
		if err != nil {
			return err
		}
		if !exists || !complete {
			return errors.New("repair prepared directory publication is incomplete")
		}
		if err := validate(); err != nil {
			return err
		}
	}
	return nil
}

func ensureUnadoptedRepairDataDirAbsent(dataDir string) error {
	if err := safeRepairDirectoryParent(dataDir); err != nil {
		return err
	}
	info, err := os.Lstat(dataDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("unsafe repair data directory")
	}
	adoption, err := os.Lstat(filepath.Join(dataDir, "prepared-adoption.json"))
	if err == nil {
		if adoption.Mode().IsRegular() {
			return errors.New("repair adoption appeared during preparation")
		}
		return errors.New("unsafe repair adoption record")
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	_, err = quarantineRepairDirectory(dataDir, "."+filepath.Base(dataDir)+"-quarantine-")
	return err
}

func validateRepairPromotionPaths(stagingDir, finalDir string) error {
	stagingDir = filepath.Clean(stagingDir)
	finalDir = filepath.Clean(finalDir)
	if stagingDir == finalDir || filepath.Dir(stagingDir) != filepath.Dir(finalDir) || filepath.Base(stagingDir) == "." || filepath.Base(finalDir) == "." {
		return errors.New("ambiguous repair staging and final directories")
	}
	if err := safeRepairDirectoryParent(finalDir); err != nil {
		return err
	}
	if _, err := os.Lstat(stagingDir); !errors.Is(err, os.ErrNotExist) {
		return errors.New("repair staging directory is not absent")
	}
	if _, err := os.Lstat(finalDir); !errors.Is(err, os.ErrNotExist) {
		return errors.New("repair final directory is not absent")
	}
	return nil
}

// The returned cleanup owns only the SDK handle. Failed/partial local evidence
// is preserved; an existing adopted DataDir is never reconstructed over itself.
func ManagedRepairConfig(ctx context.Context, kube *operator.Kubernetes, ref operator.V1Alpha2ClusterRef, podName, podUID, identity, bootstrapPath, repairPath string, profile operator.V1Alpha2RuntimeProfile) (managedsql.Config, func() error, error) {
	var empty managedsql.Config
	noop := func() error { return nil }
	if err := operator.ValidateV1Alpha2ApplicationProfile(profile.ApplicationProtocol, profile.EvidenceFormat); err != nil {
		return empty, noop, err
	}
	if filepath.Clean(repairPath) == filepath.Clean(bootstrapPath) || filepath.Clean(repairPath) == filepath.Clean(identity) {
		return empty, noop, errors.New("distinct repair handoff required")
	}
	var saved RepairGateFile
	if err := ReadGateFile(repairPath, &saved, RepairFileLimit, true); err != nil {
		return empty, noop, err
	}
	plan, err := CheckSavedRepairGate(ctx, kube, ref, 0, podName, podUID, identity, saved)
	if err != nil {
		return empty, noop, err
	}
	var bootstrap operator.V1Alpha2BootstrapPlan
	if err := ReadGateFile(bootstrapPath, &bootstrap, 65536, false); err != nil || !reflect.DeepEqual(bootstrap, plan) {
		return empty, noop, errors.New("bootstrap handoff differs from committed original plan")
	}
	cfg, err := ManagedRuntimeConfig(ctx, kube, ref, podUID, identity, profile, plan)
	if err != nil {
		return empty, noop, err
	}
	cfg.RepairOrigin, err = os.ReadFile(repairPath)
	if err != nil {
		return empty, noop, err
	}
	// Match the exact bytes handed to the runtime to the validated origin.
	var exact RepairGateFile
	if json.Unmarshal(cfg.RepairOrigin, &exact) != nil || !reflect.DeepEqual(exact, saved) {
		return empty, noop, errors.New("repair origin changed while loading")
	}
	replacement := saved.Repair.Plan.Replacement
	u, err := url.Parse(replacement.PeerURL)
	if err != nil {
		return empty, noop, errors.New("invalid repair peer endpoint")
	}
	_, port, err := net.SplitHostPort(u.Host)
	if err != nil || port != fmt.Sprint(profile.PeerPort) {
		return empty, noop, errors.New("repair peer port mismatch")
	}
	cfg.Runtime.PeerAddr = u.Host
	cfg.Runtime.Learner = &quepaxa.Member{ID: replacement.NodeID, PublicKey: replacement.PublicKey, PeerURL: replacement.PeerURL}
	physical, member, err := node.New(&cfg.Runtime).PreparedStartupBindingDigestsForProtocol(cfg.Plan.ApplicationProtocol)
	if err != nil {
		return empty, noop, err
	}
	capture := saved.Repair.PreparedHandoff
	namespace, err := node.ManagedNamespaceFor(cfg.StorageRoot, ref.UID, cfg.Plan.Generation, cfg.Plan.AssignmentID, string(cfg.Runtime.ClusterID))
	if err != nil || capture.SourceRoot != cfg.StorageRoot || capture.SourcePrefix != namespace.FinalPrefix || capture.PhysicalBinding != physical || cfg.Plan.PhysicalBinding != physical {
		return empty, noop, errors.New("repair physical profile mismatch")
	}
	dataInfo, err := os.Lstat(cfg.Runtime.DataDir)
	if err == nil && (!dataInfo.IsDir() || dataInfo.Mode()&os.ModeSymlink != 0) {
		return empty, noop, errors.New("unsafe repair data directory")
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return empty, noop, errors.New("adoption inspection failed")
	}
	if err := safeRepairDirectoryParent(cfg.Runtime.DataDir); err != nil {
		return empty, noop, errors.New("adoption inspection failed")
	}
	adoption, err := os.Lstat(filepath.Join(cfg.Runtime.DataDir, "prepared-adoption.json"))
	if err == nil {
		if !adoption.Mode().IsRegular() {
			return empty, noop, errors.New("unsafe adoption record")
		}
		return cfg, noop, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return empty, noop, errors.New("adoption inspection failed")
	}
	// Only the current closed handoff can create a first adopted learner.
	_, current, err := CheckRepairGate(ctx, kube, ref, 0, podName, podUID, identity, false)
	if err != nil || !repairGateCompatible(saved, current) {
		return empty, noop, errors.New("historical repair requires existing adoption and WAL")
	}
	c := cfg.Runtime
	if c.ObjStoreProvider != "s3" && c.ObjStoreProvider != "gcs" && c.ObjStoreProvider != "azure" {
		return empty, noop, errors.New("unsupported managed repair storage")
	}
	bucket, err := objectstore.NewBucketWithoutProvisioning(ctx, objectstore.Config{Provider: objectstore.Provider(c.ObjStoreProvider), Endpoint: c.ObjStoreEndpoint, Bucket: c.ObjStoreBucket, Region: c.ObjStoreRegion, Insecure: c.ObjStoreInsecure, MaxRetries: c.ObjStoreRetries, AccessKey: c.ObjStoreAccessKey, SecretKey: c.ObjStoreSecretKey, SessionToken: c.ObjStoreSessionToken, ServiceAccount: c.ObjStoreServiceAccount, AzureTenantID: c.ObjStoreAzureTenantID, AzureClientID: c.ObjStoreAzureClientID, AzureClientSecret: c.ObjStoreAzureClientSecret, AzureStorageAccount: c.ObjStoreAzureStorageAccount, AzureStorageAccountKey: c.ObjStoreAzureStorageAccountKey, AzureConnectionString: c.ObjStoreAzureConnectionString, AzureUserAssignedID: c.ObjStoreAzureUserAssignedID})
	if err != nil {
		return empty, noop, errors.New("repair source unavailable")
	}
	closeBucket := func() error { return bucket.Close() }
	fail := func(cause error) (managedsql.Config, func() error, error) {
		return empty, noop, errors.Join(cause, bucket.Close())
	}
	readonly, err := objectstore.BindReadOnlyNamespace(bucket, capture.SourcePrefix)
	if err != nil {
		return fail(err)
	}
	trusted := quepaxa.Cluster{ConfigID: 1, Members: cfg.Runtime.Members}
	policy, window, err := types.ManagedApplicationPolicy(cfg.Plan.ApplicationProtocol)
	if err != nil {
		return fail(err)
	}
	protocol := cfg.Plan.ApplicationProtocol
	var view *recovery.ReadOnlyView
	if sqlpolicy.IsManagedAppendOnlyApplicationProtocol(protocol) {
		view, err = recovery.OpenReadOnlyViewAtHeadForApplicationProtocol(ctx, readonly, capture.SourcePrefix, 1, trusted, capture.Head, policy, window, protocol, c.EnableReconfiguration)
	} else {
		view, err = recovery.OpenReadOnlyViewAtHeadForPolicyWithReconfiguration(ctx, readonly, capture.SourcePrefix, 1, trusted, capture.Head, policy, window, c.EnableReconfiguration)
	}
	if err != nil {
		return fail(errors.New("fixed repair source verification failed"))
	}
	if err := VerifyRepairControl(ctx, readonly, trusted, view, *capture); err != nil {
		return fail(err)
	}
	expected := recovery.PreparedStartupIdentity{ClusterUID: ref.UID, Generation: cfg.Plan.Generation, DeploymentUID: cfg.Plan.DeploymentUID, PodUID: podUID, NodeID: string(cfg.Runtime.NodeID), SourceBinding: physical, ConfigDigest: member}
	restoredMarker, markerPresent, err := inspectRepairRestoredState(cfg.Runtime.DataDir, capture.PlanDigest, expected)
	if err != nil {
		return fail(fmt.Errorf("repair readiness marker validation failed: %w", err))
	}
	preparedDir := filepath.Join(filepath.Dir(cfg.Runtime.DataDir), "repair-prepared-"+capture.PlanDigest)
	if err := ensureRepairPreparedDirectory(preparedDir, func() error {
		return recovery.ValidatePreparedStartupDirectoryForPolicy(ctx, preparedDir, expected, policy, window, protocol)
	}, func() error {
		return recovery.PublishPreparedStartupForPolicy(ctx, view, preparedDir, expected, policy, window)
	}); err != nil {
		return fail(repairPreparationFailed(err))
	}
	completionHash, err := hashRepairPreparedCompletion(preparedDir)
	if err != nil {
		return fail(repairPreparedVerificationFailed(err))
	}
	if markerPresent {
		if restoredMarker.CompletionHash != completionHash || restoredMarker.HeadHash != view.HeadHash() {
			return fail(errors.New("repair readiness marker differs from verified preparation"))
		}
		prepared, err := recovery.VerifyRestoredPreparedStartupForPolicy(ctx, cfg.Runtime.DataDir, preparedDir, view, expected, policy, window, protocol)
		if err != nil {
			return fail(repairPreparedVerificationFailed(err))
		}
		cfg.Prepared = prepared
		return cfg, closeBucket, nil
	}
	if err := ensureUnadoptedRepairDataDirAbsent(cfg.Runtime.DataDir); err != nil {
		return fail(repairPreparedVerificationFailed(err))
	}
	stagingDir, err := reserveRepairSiblingPath(filepath.Dir(cfg.Runtime.DataDir), ".repair-data-staging-"+capture.PlanDigest+"-")
	if err != nil {
		return fail(repairPreparedVerificationFailed(err))
	}
	if err := validateRepairPromotionPaths(stagingDir, cfg.Runtime.DataDir); err != nil {
		return fail(repairPreparedVerificationFailed(err))
	}
	prepared, err := recovery.LoadPreparedStartupForPolicyIntoStagingAndPromote(ctx, readonly, capture.SourcePrefix, 1, trusted, preparedDir, stagingDir, cfg.Runtime.DataDir, expected, policy, window, protocol, c.EnableReconfiguration)
	if err != nil {
		return fail(repairPreparedVerificationFailed(err))
	}
	marker := repairRestoredMarker{Version: 1, Identity: expected, PlanDigest: capture.PlanDigest, CompletionHash: completionHash, HeadHash: view.HeadHash()}
	if err := writeRepairRestoredMarker(cfg.Runtime.DataDir, marker); err != nil {
		return fail(repairPreparedVerificationFailed(fmt.Errorf("write repair readiness marker: %w", err)))
	}
	cfg.Prepared = prepared
	return cfg, closeBucket, nil
}

func repairGateCompatible(saved, live RepairGateFile) bool {
	if saved.Version != live.Version || saved.Repair.State != "Closed" || live.Repair.State != "Closed" || saved.Repair.Completion != nil || live.Repair.Completion != nil || !reflect.DeepEqual(saved.Repair.Plan, live.Repair.Plan) || !reflect.DeepEqual(saved.Repair.PreparedHandoff, live.Repair.PreparedHandoff) || saved.Binding != live.Binding {
		return false
	}
	if saved.Repair.AddCompletion != nil {
		return reflect.DeepEqual(saved.Repair.AddCompletion, live.Repair.AddCompletion)
	}
	return live.Repair.AddCompletion == nil || live.Repair.Plan.Sequence == operator.V1Alpha2RepairAddThenRemove
}

// Reuse the actual consensus replay and checkpoint verifier to bind the fixed
// source to its REMOVE terminal/target, rather than parsing control wire data.
func VerifyRepairControl(ctx context.Context, bucket thanosobjstore.Bucket, bootstrap quepaxa.Cluster, view *recovery.ReadOnlyView, capture operator.V1Alpha2RepairHandoff) (resultErr error) {
	dir, err := os.MkdirTemp("", "rhiza-repair-control-*")
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, os.RemoveAll(dir)) }()
	wal, err := qlog.Open(filepath.Join(dir, "wal"))
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, wal.Close()) }()
	// The view's expected policy was selected from the already authenticated
	// runtime protocol during source verification, not inferred from wire magic.
	var validateValue func([]byte) error
	if view.MaterializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		validateValue = types.ValidateManagedGraphExecutionPolicy
	}
	core, err := quepaxa.NewObserver(quepaxa.Config{NodeID: "repair-control-verifier", Cluster: bootstrap, WAL: wal, EnableReconfiguration: true, ValidateValue: validateValue})
	if err != nil {
		return err
	}
	manager, err := checkpoint.NewManagerForPolicy(bucket, capture.SourcePrefix, "", view.MaterializationPolicy(), 1)
	if err != nil {
		return err
	}
	core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
		return manager.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
	})
	if seal, decision, ok := view.RecoveryBase(); ok {
		if err := core.RestoreCheckpointBase(ctx, seal, decision); err != nil {
			return errors.New("repair checkpoint membership verification failed")
		}
	}
	if err := core.AcceptCertifiedValues(view.Decisions()); err != nil {
		return errors.New("repair certified suffix verification failed")
	}
	history, err := core.MembershipHistory()
	if err != nil {
		return errors.New("repair membership history unavailable")
	}
	found := false
	for _, transition := range history.Transitions {
		if uint64(transition.Terminal.Slot) == capture.RemoveTerminalSlot && fmt.Sprintf("%x", transition.Terminal.Hash) == capture.RemoveTerminalHash {
			found = true
		}
	}
	if capture.PreparedConfig != nil {
		prefixHash := view.PrefixHash()
		if !reflect.DeepEqual(core.CurrentCluster(), *capture.PreparedConfig) || uint64(view.Tip()) != capture.PreparedSlot || fmt.Sprintf("%x", prefixHash) != capture.PreparedHash || fmt.Sprintf("%x", view.HeadHash()) != capture.HeadHash {
			return errors.New("fixed source does not prove captured ADD-first configuration and prefix")
		}
		return nil
	}
	if !found || !reflect.DeepEqual(core.CurrentCluster(), capture.Survivors) || fmt.Sprintf("%x", view.HeadHash()) != capture.HeadHash {
		return errors.New("fixed source does not prove captured REMOVE target")
	}
	return nil
}
