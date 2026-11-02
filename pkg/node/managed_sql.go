package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"path"
	"slices"
	"time"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

// ManagedSQLWriteChecker must check the exact live pending request, not just
// general readiness. Only the private SQL host implements this boundary.
type ManagedSQLWriteChecker interface {
	ValidateManagedSQLWrite(context.Context, ManagedStartupIdentity, ManagedRuntimeState, network.ExecuteRequest) error
}

// ManagedSQLRecoveryChecker authorizes learning the exact pending original,
// not submitting it or admitting a new application write.
type ManagedSQLRecoveryChecker interface {
	ValidateManagedSQLRecovery(context.Context, ManagedStartupIdentity, network.ExecuteRequest) error
}

// ManagedSQLReadChecker validates live local-read authority without a quorum
// round or the active-write reservation exception.
type ManagedSQLReadChecker interface {
	ValidateManagedSQLRead(context.Context, ManagedStartupIdentity, ManagedRuntimeState) error
}

type ManagedPeerLearnChecker interface {
	ValidateManagedPeerLearn(context.Context, ManagedStartupIdentity) error
}

func (n *Node) managedLearnReady() bool {
	// Client catch-up readiness is separate from accepting a certified peer
	// decision: the latter can be needed to resolve a recorded slot. Keep the
	// verified managed runtime and live authority gates, while ordinary client
	// traffic remains gated by n.ready and the admission checks.
	if !n.opened.Load() || !n.managedPeerVerified.Load() {
		log.Printf("managed QUIC Learned refused stage=runtime opened=%t verified=%t", n.opened.Load(), n.managedPeerVerified.Load())
		return false
	}
	checker, ok := n.managedStartup.Checker.(ManagedPeerLearnChecker)
	if !ok {
		return n.Ready()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := checker.ValidateManagedPeerLearn(ctx, n.managedStartup.Identity); err != nil {
		log.Printf("managed QUIC Learned refused stage=live-authority error_type=%T", err)
		return false
	}
	return true
}

func (n *Node) CheckManagedLocalRead(ctx context.Context) error {
	if _, err := n.ManagedState(); err != nil {
		return err
	}
	if !n.managedInitiallyAdmitted.Load() {
		if err := n.AdmitManaged(ctx); err != nil {
			return err
		}
	}
	if !n.core.IsVoter() {
		return network.ErrNotReady
	}
	state := n.managedRuntimeState()
	checker, ok := n.managedStartup.Checker.(ManagedSQLReadChecker)
	if !ok {
		return network.ErrNotReady
	}
	return checker.ValidateManagedSQLRead(ctx, n.managedStartup.Identity, state)
}

type ManagedSQLSnapshot struct {
	Application materializer.SQLApplicationState
	Runtime     ManagedRuntimeState
}

func (n *Node) managedSQLBucket(ctx context.Context) (*objectstore.MeteredBucket, error) {
	c := n.config
	provider := objectstore.Provider(c.ObjStoreProvider)
	if provider == "" {
		provider = objectstore.ProviderS3
	}
	return objectstore.NewBucketForNamespace(ctx, objectstore.Config{
		Provider: provider, FilesystemDir: c.ObjStoreDir, Prefix: c.ObjStorePrefix,
		Endpoint: c.ObjStoreEndpoint, Bucket: c.ObjStoreBucket, Region: c.ObjStoreRegion, Insecure: c.ObjStoreInsecure, MaxRetries: c.ObjStoreRetries,
		AccessKey: c.ObjStoreAccessKey, SecretKey: c.ObjStoreSecretKey, SessionToken: c.ObjStoreSessionToken,
		ServiceAccount: c.ObjStoreServiceAccount, AzureTenantID: c.ObjStoreAzureTenantID, AzureClientID: c.ObjStoreAzureClientID, AzureClientSecret: c.ObjStoreAzureClientSecret,
		AzureStorageAccount: c.ObjStoreAzureStorageAccount, AzureStorageAccountKey: c.ObjStoreAzureStorageAccountKey, AzureConnectionString: c.ObjStoreAzureConnectionString, AzureUserAssignedID: c.ObjStoreAzureUserAssignedID,
	}, path.Join(c.ObjStorePrefix, string(c.ClusterID)))
}

func (n *Node) managedSQLScope(i ManagedStartupIdentity) ([]byte, string, error) {
	policy, _, err := types.ManagedApplicationPolicy(i.ApplicationProtocol)
	if err != nil {
		return nil, "", err
	}
	if policy == sqlpolicy.ManagedSQLKVMaterializationPolicy && (i.Member.Generation != 0 || i.EvidenceFormat != types.ManagedApplicationProtocolSQLKV) {
		return nil, "", fmt.Errorf("unsupported managed application generation or evidence format")
	}
	source, _, err := n.preparedBindingDigestsForProtocol(i.ApplicationProtocol)
	if err != nil || source != i.PhysicalBinding {
		return nil, "", errors.Join(fmt.Errorf("initial SQL physical binding mismatch"), err)
	}
	common, err := n.ManagedBootstrapPlanDigestForProtocol(source, i.EvidenceFormat, i.ApplicationProtocol)
	if err != nil || common != i.BootstrapPlanDigest {
		return nil, "", errors.Join(fmt.Errorf("initial SQL bootstrap binding mismatch"), err)
	}
	b, err := json.Marshal(struct {
		Version                                                                                                                              int
		ClusterUID, AssignmentID, IntentHash, DeploymentUID, StorageID, PhysicalBinding, BootstrapPlanDigest, EvidenceFormat, FinalNamespace string
		Generation                                                                                                                           int64
	}{1, i.Member.ClusterUID, i.AssignmentID, i.IntentHash, i.Member.DeploymentUID, i.StorageID, i.PhysicalBinding, i.BootstrapPlanDigest, i.EvidenceFormat, path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID)), i.Member.Generation})
	return b, path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID)), err
}

// PrepareManagedSQLSource creates/verifies exact birth intent, not prefix-wide
// empty proof or execution ownership. The host separately requires canonical
// never-reserved history; all managed writers have fixed disjoint namespaces.
func (n *Node) PrepareManagedSQLSource(ctx context.Context, i ManagedStartupIdentity, allowNew bool) (empty bool, resultErr error) {
	want, prefix, err := n.managedSQLScope(i)
	if err != nil {
		return false, err
	}
	bucket, err := n.managedSQLBucket(ctx)
	if err != nil {
		return false, err
	}
	defer func() { resultErr = errors.Join(resultErr, bucket.Close()) }()
	key := path.Join(prefix, "voters/managed-sql-initialization.json")
	got, err := readVoterRegistration(ctx, bucket, key)
	if err != nil {
		return false, err
	}
	if got == nil {
		if !allowNew {
			return false, fmt.Errorf("managed SQL initialization provenance is missing")
		}
		notEmpty := errors.New("existing source cannot be initialized as a new SQL namespace")
		if err := bucket.Iter(ctx, prefix+"/", func(string) error { return notEmpty }, objstore.WithRecursiveIter()); err != nil {
			if !errors.Is(err, notEmpty) {
				return false, err
			}
			// Another initial member may have recorded the same birth intent
			// since our GET. This is exact record readback, not a prefix claim.
			claimed, readErr := readVoterRegistration(ctx, bucket, key)
			if readErr != nil || !bytes.Equal(claimed, want) {
				return false, errors.Join(err, readErr)
			}
		}
		if !slices.Contains(bucket.SupportedObjectUploadOptions(), objstore.IfNotExists) {
			return false, fmt.Errorf("initial SQL namespace requires conditional writes")
		}
		if err := matchVoterRegistration(ctx, bucket, key, want, nil); err != nil {
			return false, err
		}
	} else if !bytes.Equal(got, want) {
		return false, fmt.Errorf("initial SQL namespace belongs to another assignment")
	}
	started, err := readVoterRegistration(ctx, bucket, path.Join(prefix, "voters/managed-sql-application-start.json"))
	if err != nil {
		return false, err
	}
	if started != nil && !bytes.Equal(started, want) {
		return false, fmt.Errorf("application-start binding mismatch")
	}
	head, err := bucket.Exists(ctx, path.Join(prefix, "archive/head.bin"))
	if err != nil {
		return false, err
	}
	return started == nil && !head, nil
}

func (n *Node) ManagedArchiveAvailable(ctx context.Context) (bool, error) {
	bucket, err := n.managedSQLBucket(ctx)
	if err != nil {
		return false, err
	}
	exists, checkErr := bucket.Exists(ctx, path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID), "archive/head.bin"))
	return exists, errors.Join(checkErr, bucket.Close())
}

// VerifyManagedSQLFreshSource checks the exact birth and retained source before
// E0. It does not replace the host's canonical never-reserved history check.
func (n *Node) VerifyManagedSQLFreshSource(ctx context.Context, i ManagedStartupIdentity) (resultErr error) {
	want, prefix, err := n.managedSQLScope(i)
	if err != nil {
		return err
	}
	bucket, err := n.managedSQLBucket(ctx)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, bucket.Close()) }()
	got, err := readVoterRegistration(ctx, bucket, path.Join(prefix, "voters/managed-sql-initialization.json"))
	if err != nil {
		return err
	}
	if !bytes.Equal(got, want) {
		return fmt.Errorf("exact once-new SQL namespace provenance required")
	}
	started, err := readVoterRegistration(ctx, bucket, path.Join(prefix, "voters/managed-sql-application-start.json"))
	if err != nil {
		return err
	}
	if started != nil {
		return fmt.Errorf("application-start history prevents fresh SQL initialization")
	}
	exists, err := bucket.Exists(ctx, path.Join(prefix, "archive/head.bin"))
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	source, err := objectstore.BindReadOnlyNamespace(bucket, prefix)
	if err != nil {
		return err
	}
	view, err := recovery.OpenReadOnlyViewForPolicy(ctx, source, prefix, 1, *n.loadClusterConfig(), n.materializationPolicy(), n.receiptWindow())
	if err != nil {
		return err
	}
	if view.Base() != 0 {
		return fmt.Errorf("existing source base prevents fresh SQL initialization")
	}
	for _, v := range view.Decisions() {
		if err := managedSQLControlOnly(v.Value); err != nil {
			return err
		}
	}
	return nil
}

func managedSQLControlOnly(value []byte) error {
	if len(value) == 0 {
		return nil
	}
	_, schedule, e1 := types.DecodeLeaderSchedule(value)
	barrier, e2 := types.DecodeReadBarrier(value)
	_, seal, e3 := types.DecodeCheckpointSeal(value)
	reconfig, e4 := types.DecodeReconfiguration(value)
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil || !(schedule || barrier || seal || reconfig) {
		return fmt.Errorf("application source history prevents fresh SQL initialization")
	}
	return nil
}

// InspectManagedSQL rejects histories outside this bounded SQL-only host. An
// unresolved recorder is never empty-state proof, even when SQLite is empty.
func (n *Node) InspectManagedSQL(ctx context.Context, fresh bool) (ManagedSQLSnapshot, error) {
	return n.inspectManagedSQL(ctx, fresh, false)
}

// InspectManagedSQLRead verifies only the materializer's actual applied,
// certified prefix. It neither replays nor consumes an undecided suffix.
func (n *Node) InspectManagedSQLRead(ctx context.Context) (ManagedSQLSnapshot, error) {
	return n.inspectManagedSQL(ctx, false, true)
}

func (n *Node) inspectManagedSQL(ctx context.Context, fresh, localRead bool) (ManagedSQLSnapshot, error) {
	if n.materializationPolicy() != sqlpolicy.LegacyMaterializationPolicy {
		return ManagedSQLSnapshot{}, fmt.Errorf("mixed runtime requires mixed application evidence")
	}
	digest, empty, runtime, err := n.inspectManagedApplication(ctx, fresh, localRead)
	return ManagedSQLSnapshot{Application: materializer.SQLApplicationState{Digest: digest, FreshEmpty: empty}, Runtime: runtime}, err
}

type ManagedKVSnapshot struct {
	Application materializer.ManagedApplicationState
	Runtime     ManagedRuntimeState
}

// InspectManagedKV is mixed evidence only, not public request status or ACK.
func (n *Node) InspectManagedKV(ctx context.Context, fresh bool) (ManagedKVSnapshot, error) {
	if n.materializationPolicy() != sqlpolicy.ManagedSQLKVMaterializationPolicy {
		return ManagedKVSnapshot{}, fmt.Errorf("mixed evidence requires the mixed application profile")
	}
	digest, empty, runtime, err := n.inspectManagedApplication(ctx, fresh, false)
	return ManagedKVSnapshot{Application: materializer.ManagedApplicationState{Digest: digest, FreshEmpty: empty}, Runtime: runtime}, err
}

func (n *Node) inspectApplicationState(ctx context.Context, m *materializer.Materializer) ([32]byte, bool, error) {
	if n.materializationPolicy() == sqlpolicy.ManagedSQLKVMaterializationPolicy {
		state, err := m.InspectManagedApplicationState(ctx)
		return state.Digest, state.FreshEmpty, err
	}
	state, err := m.InspectSQLApplicationState(ctx)
	return state.Digest, state.FreshEmpty, err
}

func (n *Node) inspectManagedApplication(ctx context.Context, fresh, localRead bool) (digest [32]byte, empty bool, runtime ManagedRuntimeState, resultErr error) {
	// Keep one certified-prefix/adoption check for both evidence domains.
	fail := func(err error) ([32]byte, bool, ManagedRuntimeState, error) {
		return [32]byte{}, false, ManagedRuntimeState{}, err
	}
	if _, err := n.ManagedState(); err != nil {
		return fail(err)
	}
	if !localRead {
		if err := n.replayLocalDecisions(ctx); err != nil {
			return fail(err)
		}
	}
	n.replayMu.Lock()
	defer n.replayMu.Unlock()
	before := n.managedRuntimeState()
	floor := n.core.CompactionFloor()
	if fresh && n.materializationPolicy() == sqlpolicy.ManagedSQLKVMaterializationPolicy && floor != 0 {
		return fail(fmt.Errorf("compacted application history is not fresh-state proof"))
	}
	if floor > 0 {
		adoption := n.managedStartup.verifiedAdoption
		if adoption == nil || adoption.BaseSeal == nil || adoption.WALIdentity != before.WALIdentity {
			return fail(fmt.Errorf("managed host requires a verified prepared checkpoint adoption"))
		}
		base, root, ok := n.core.RecoveryRoot()
		prefix, prefixOK := n.core.PrefixHash(adoption.BaseSeal.Index)
		adoptedPrefix, adoptedOK := n.core.PrefixHash(adoption.Tip)
		membership, membershipErr := n.core.MembershipHistory()
		if !ok || base != adoption.BaseSeal.Index || root != adoption.BaseSeal.RootHash || !prefixOK || prefix != adoption.BaseSeal.PrefixHash || !adoptedOK || adoptedPrefix != adoption.PrefixHash || n.core.Tip() < adoption.Tip || membershipErr != nil || adoption.Membership == nil || !membershipHistoryHasPrefix(*adoption.Membership, membership) {
			return fail(errors.Join(fmt.Errorf("managed host checkpoint adoption no longer matches actual WAL"), membershipErr))
		}
	}
	if before.WALIdentity == "" || before.AppliedTip > before.CertifiedTip || !localRead && (before.RecorderTip > before.CertifiedTip || before.AppliedTip != before.CertifiedTip) {
		return fail(fmt.Errorf("managed host requires registered, resolved history"))
	}
	from := floor + 1
	for uint64(from) <= before.AppliedTip {
		values, _, err := n.core.DecisionsFrom(from, 256)
		if err != nil || len(values) == 0 {
			return fail(fmt.Errorf("managed history unavailable: %w", err))
		}
		for _, v := range values {
			if uint64(v.Slot) > before.AppliedTip {
				break
			}
			if uint64(v.Slot) == before.AppliedTip {
				if err := n.material.ValidateTip(before.AppliedTip, v.Value); err != nil {
					return fail(err)
				}
			}
			if len(v.Value) != 0 {
				if n.materializationPolicy() == sqlpolicy.ManagedSQLKVMaterializationPolicy {
					if err := n.validateExecutionPolicy(v.Value); err != nil {
						return fail(err)
					}
					if fresh {
						if err := managedSQLControlOnly(v.Value); err != nil {
							return fail(err)
						}
					}
					from = v.Slot + 1
					continue
				}
				_, sql, err := types.DecodeSQLBatch(v.Value)
				if err != nil || sql && fresh {
					return fail(fmt.Errorf("application history prevents fresh SQL initialization"))
				}
				if !sql {
					_, schedule, e1 := types.DecodeLeaderSchedule(v.Value)
					barrier, e2 := types.DecodeReadBarrier(v.Value)
					_, seal, e3 := types.DecodeCheckpointSeal(v.Value)
					reconfig, e4 := types.DecodeReconfiguration(v.Value)
					if e1 != nil || e2 != nil || e3 != nil || e4 != nil || !(schedule || barrier || seal || reconfig) {
						return fail(fmt.Errorf("non-SQL application history"))
					}
				}
			}
			from = v.Slot + 1
		}
	}
	digest, empty, err := n.inspectApplicationState(ctx, n.material)
	if err != nil {
		return fail(err)
	}
	if fresh && !empty {
		return fail(fmt.Errorf("application state is not fresh empty"))
	}
	if before != n.managedRuntimeState() {
		return fail(fmt.Errorf("history changed during application inspection"))
	}
	return digest, empty, before, nil
}

func (n *Node) ExecuteManagedSQL(ctx context.Context, req network.ExecuteRequest) (network.ExecuteResponse, error) {
	state, err := n.ManagedState()
	if err != nil {
		return network.ExecuteResponse{}, err
	}
	checker, ok := n.managedStartup.Checker.(ManagedSQLWriteChecker)
	if !ok {
		return network.ExecuteResponse{}, fmt.Errorf("exact managed SQL reservation checker unavailable")
	}
	if err := network.ValidateExecuteRequest(req); err != nil {
		return network.ExecuteResponse{}, err
	}
	if err := checker.ValidateManagedSQLWrite(ctx, n.managedStartup.Identity, state, req); err != nil {
		return network.ExecuteResponse{}, err
	}
	want, prefix, err := n.managedSQLScope(n.managedStartup.Identity)
	if err != nil {
		return network.ExecuteResponse{}, err
	}
	claimed, err := readVoterRegistration(ctx, n.bucket, path.Join(prefix, "voters/managed-sql-initialization.json"))
	if err != nil || !bytes.Equal(claimed, want) {
		return network.ExecuteResponse{}, errors.Join(fmt.Errorf("exact initial SQL namespace provenance required before execution"), err)
	}
	if err := matchVoterRegistration(ctx, n.bucket, path.Join(prefix, "voters/managed-sql-application-start.json"), want, nil); err != nil {
		return network.ExecuteResponse{}, fmt.Errorf("record irreversible SQL application start: %w", err)
	}
	return n.server.Execute(ctx, req)
}

func (n *Node) ResolveManagedSQL(ctx context.Context, req network.ExecuteRequest) (network.ResolveExecuteResult, error) {
	if _, err := n.ManagedState(); err != nil {
		return network.ResolveExecuteResult{}, err
	}
	result, err := n.server.ResolveExecute(ctx, req)
	if err != nil || result.State != network.ResolveExecuteUnknownOrExpired {
		return result, err
	}
	checker, ok := n.managedStartup.Checker.(ManagedSQLRecoveryChecker)
	if !ok {
		return result, fmt.Errorf("exact managed SQL recovery checker unavailable")
	}
	if err := checker.ValidateManagedSQLRecovery(ctx, n.managedStartup.Identity, req); err != nil {
		return result, err
	}
	target, _, err := n.core.ReadIndex(ctx)
	if err != nil {
		return result, err
	}
	if err := n.replayLocalDecisions(ctx); err != nil {
		return result, err
	}
	state := n.managedRuntimeState()
	if state.CertifiedTip < uint64(target) || state.AppliedTip != state.CertifiedTip || state.RecorderTip > state.CertifiedTip {
		return result, fmt.Errorf("managed SQL recovery has unresolved certified history")
	}
	if err := checker.ValidateManagedSQLRecovery(ctx, n.managedStartup.Identity, req); err != nil {
		return result, err
	}
	return n.server.ResolveExecute(ctx, req)
}

func (n *Node) QueryManagedSQL(ctx context.Context, req network.QueryRequest) (network.QueryResponse, error) {
	if req.Consistency != "" && req.Consistency != "local" && req.Consistency != "linearizable" {
		return network.QueryResponse{}, fmt.Errorf("%w: consistency must be local or linearizable", network.ErrInvalidRequest)
	}
	if err := n.CheckManagedLocalRead(ctx); err != nil {
		return network.QueryResponse{}, err
	}
	if req.Consistency == "linearizable" {
		if err := n.managedStartup.Checker.ValidateManagedAdmission(ctx, n.managedStartup.Identity, n.managedRuntimeState()); err != nil {
			return network.QueryResponse{}, err
		}
	}
	// The private server retains its lifecycle/read limits and enforces the
	// quorum ReadIndex barrier only for explicit linearizable queries.
	return n.server.Query(ctx, req)
}
