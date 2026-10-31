package main

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

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/managedsql"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	thanosobjstore "github.com/thanos-io/objstore"
)

const repairHandoffPath = "/var/lib/rhiza/repair.json"
const repairFileLimit = operator.V1Alpha2RepairRecordLimit

type repairGateFile struct {
	Version int                               `json:"version"`
	Repair  operator.V1Alpha2RepairRecord     `json:"repair"`
	Binding operator.V1Alpha2RepairPodBinding `json:"binding"`
}

func checkRepairGate(ctx context.Context, kube *operator.Kubernetes, ref operator.V1Alpha2ClusterRef, generation int64, podName, podUID, identity string, allowCompleted bool) (operator.V1Alpha2BootstrapPlan, repairGateFile, error) {
	var empty operator.V1Alpha2BootstrapPlan
	var output repairGateFile
	store := &operator.V1Alpha2StatusStore{Kube: kube}
	a, err := operator.LoadV1Alpha2Admission(ctx, store, ref)
	if err != nil {
		return empty, output, err
	}
	r, ok := a.Repair()
	if !ok || r.PreparedHandoff == nil {
		return empty, output, errLearnerAdmissionPending
	}
	if r.State != "Closed" && !(allowCompleted && r.State == "Completed") {
		return empty, output, errors.New("repair startup is not authorized")
	}
	if !bytes.Equal(bytes.TrimSpace(a.Transition()), []byte("null")) && len(bytes.TrimSpace(a.Transition())) != 0 {
		return empty, output, errors.New("repair overlaps authority transition")
	}
	p := r.Plan
	if p.ClusterUID != ref.UID || p.Generation != generation || p.Generation != a.Authority().Generation || p.DeploymentUID != a.Authority().Binding.DeploymentUID || p.StorageID != a.Authority().Binding.StorageID || p.Replacement.PodName != podName || p.Replacement.PodUID != podUID {
		return empty, output, errors.New("repair execution binding changed")
	}
	member := operator.V1Alpha2BootstrapMember{PodUID: podUID, Member: quepaxa.Member{ID: p.Replacement.NodeID, PublicKey: p.Replacement.PublicKey, PeerURL: p.Replacement.PeerURL}}
	plan, err := checkGateMember(ctx, kube, ref, generation, podName, podUID, identity, &member)
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
	if !exists || live.Authority() != a.Authority() || !reflect.DeepEqual(next.Plan, r.Plan) || !reflect.DeepEqual(next.PreparedHandoff, r.PreparedHandoff) || next.State != r.State || (len(bytes.TrimSpace(live.Transition())) != 0 && !bytes.Equal(bytes.TrimSpace(live.Transition()), []byte("null"))) {
		return empty, output, errors.New("repair changed during gate verification")
	}
	binding, err := operator.ValidateV1Alpha2RepairPodBinding(ctx, store, ref, p)
	if err != nil {
		return empty, output, err
	}
	return plan, repairGateFile{Version: 1, Repair: r, Binding: binding}, nil
}

// The saved origin identifies this Pod, not the replacement in a later repair.
// This gate does not authorize WAL creation or bypass runtime adoption checks.
func checkSavedRepairGate(ctx context.Context, kube *operator.Kubernetes, ref operator.V1Alpha2ClusterRef, generation int64, podName, podUID, identity string, saved repairGateFile) (operator.V1Alpha2BootstrapPlan, error) {
	var empty operator.V1Alpha2BootstrapPlan
	p, h := saved.Repair.Plan, saved.Repair.PreparedHandoff
	if saved.Version != 1 || saved.Repair.State != "Closed" || saved.Repair.Completion != nil || h == nil || p.ClusterUID != ref.UID || p.Generation != generation || p.Replacement.PodUID != podUID || p.Replacement.PodName != podName || h.PlanDigest != p.Digest() || len(h.Head) == 0 || h.HeadHash != fmt.Sprintf("%x", sha256.Sum256(h.Head)) {
		return empty, errors.New("invalid immutable repair origin")
	}
	member := operator.V1Alpha2BootstrapMember{PodUID: podUID, Member: quepaxa.Member{ID: p.Replacement.NodeID, PublicKey: p.Replacement.PublicKey, PeerURL: p.Replacement.PeerURL}}
	plan, err := checkGateMember(ctx, kube, ref, generation, podName, podUID, identity, &member)
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

func writeGateFile(identity, output string, value any) error {
	if err := validateHandoffPath(identity, output); err != nil {
		return err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(data) > repairFileLimit {
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

func writeImmutableRepairFile(identity, output string, value repairGateFile) error {
	return writeImmutableGateFile(identity, output, value)
}

// Link publishes without replacement, including when concurrent awaiters race.
func writeImmutableGateFile(identity, output string, value any) error {
	if err := validateHandoffPath(identity, output); err != nil {
		return err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(data) > repairFileLimit {
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
	actual, err := io.ReadAll(io.LimitReader(published, repairFileLimit+1))
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

func readGateFile(path string, dst any, limit int64, private bool) error {
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

// The returned cleanup owns only the SDK handle. Failed/partial local evidence
// is preserved; an existing adopted DataDir is never reconstructed over itself.
func managedRepairConfig(ctx context.Context, kube *operator.Kubernetes, ref operator.V1Alpha2ClusterRef, podName, podUID, identity, bootstrapPath, repairPath string, profile operator.V1Alpha2RuntimeProfile) (managedsql.Config, func() error, error) {
	var empty managedsql.Config
	noop := func() error { return nil }
	if filepath.Clean(repairPath) == filepath.Clean(bootstrapPath) || filepath.Clean(repairPath) == filepath.Clean(identity) {
		return empty, noop, errors.New("distinct repair handoff required")
	}
	var saved repairGateFile
	if err := readGateFile(repairPath, &saved, repairFileLimit, true); err != nil {
		return empty, noop, err
	}
	plan, err := checkSavedRepairGate(ctx, kube, ref, 0, podName, podUID, identity, saved)
	if err != nil {
		return empty, noop, err
	}
	var bootstrap operator.V1Alpha2BootstrapPlan
	if err := readGateFile(bootstrapPath, &bootstrap, 65536, false); err != nil || !reflect.DeepEqual(bootstrap, plan) {
		return empty, noop, errors.New("bootstrap handoff differs from committed original plan")
	}
	cfg, err := managedRuntimeConfig(ctx, kube, ref, podUID, identity, profile, plan)
	if err != nil {
		return empty, noop, err
	}
	cfg.RepairOrigin, err = os.ReadFile(repairPath)
	if err != nil {
		return empty, noop, err
	}
	// Match the exact bytes handed to the runtime to the validated origin.
	var exact repairGateFile
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
	physical, member, err := node.New(&cfg.Runtime).PreparedStartupBindingDigests()
	if err != nil {
		return empty, noop, err
	}
	capture := saved.Repair.PreparedHandoff
	namespace, err := node.ManagedNamespaceFor(cfg.StorageRoot, ref.UID, cfg.Plan.Generation, cfg.Plan.AssignmentID, string(cfg.Runtime.ClusterID))
	if err != nil || capture.SourceRoot != cfg.StorageRoot || capture.SourcePrefix != namespace.FinalPrefix || capture.PhysicalBinding != physical || cfg.Plan.PhysicalBinding != physical {
		return empty, noop, errors.New("repair physical profile mismatch")
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
	_, current, err := checkRepairGate(ctx, kube, ref, 0, podName, podUID, identity, false)
	if err != nil || !reflect.DeepEqual(current, saved) {
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
	view, err := recovery.OpenReadOnlyViewAtHead(ctx, readonly, capture.SourcePrefix, 1, trusted, capture.Head)
	if err != nil {
		return fail(errors.New("fixed repair source verification failed"))
	}
	if err := verifyRepairControl(ctx, readonly, trusted, view, *capture); err != nil {
		return fail(err)
	}
	expected := recovery.PreparedStartupIdentity{ClusterUID: ref.UID, Generation: cfg.Plan.Generation, DeploymentUID: cfg.Plan.DeploymentUID, PodUID: podUID, NodeID: string(cfg.Runtime.NodeID), SourceBinding: physical, ConfigDigest: member}
	preparedDir := filepath.Join(filepath.Dir(cfg.Runtime.DataDir), "repair-prepared-"+capture.PlanDigest)
	if _, err := os.Lstat(preparedDir); errors.Is(err, os.ErrNotExist) {
		if err := recovery.PublishPreparedStartup(ctx, view, preparedDir, expected); err != nil {
			return fail(errors.New("repair preparation failed"))
		}
	} else if err != nil {
		return fail(err)
	}
	prepared, err := recovery.LoadPreparedStartup(ctx, readonly, capture.SourcePrefix, 1, trusted, preparedDir, cfg.Runtime.DataDir, expected)
	if err != nil {
		return fail(errors.New("repair prepared data verification failed"))
	}
	cfg.Prepared = prepared
	return cfg, closeBucket, nil
}

// Reuse the actual consensus replay and checkpoint verifier to bind the fixed
// source to its REMOVE terminal/target, rather than parsing control wire data.
func verifyRepairControl(ctx context.Context, bucket thanosobjstore.Bucket, bootstrap quepaxa.Cluster, view *recovery.ReadOnlyView, capture operator.V1Alpha2RepairHandoff) (resultErr error) {
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
	core, err := quepaxa.NewObserver(quepaxa.Config{NodeID: "repair-control-verifier", Cluster: bootstrap, WAL: wal, EnableReconfiguration: true})
	if err != nil {
		return err
	}
	manager := checkpoint.NewManager(bucket, capture.SourcePrefix, "", 1)
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
	if !found || !reflect.DeepEqual(core.CurrentCluster(), capture.Survivors) || fmt.Sprintf("%x", view.HeadHash()) != capture.HeadHash {
		return errors.New("fixed source does not prove captured REMOVE target")
	}
	return nil
}
