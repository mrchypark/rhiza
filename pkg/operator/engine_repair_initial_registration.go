package operator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"

	objmetrics "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

// resolveInitialRegistrationGrant performs the read-only half of initial
// admission. It requires an already-existing canonical root and verifies the
// authenticated static workload against its immutable archive genesis. The
// registration marker is persisted later, atomically with the enrolled Run.
func (e *FencingExecutor) resolveInitialRegistrationGrant(ctx context.Context, statefulSetUID string, request fencing.Identity) (fencing.StartupGrant, fencing.InitialRegistrationProvenance, error) {
	closed := func() (fencing.StartupGrant, fencing.InitialRegistrationProvenance, error) {
		return fencing.StartupGrant{}, fencing.InitialRegistrationProvenance{}, fencing.ErrClosed
	}
	physical := request.PodName
	if physical == "" {
		physical = request.NodeID
	}
	if e == nil || e.AuthorityState == nil || e.ArchiveBucket == nil || e.Kube == nil || ctx == nil ||
		statefulSetUID == "" || request.SourceClusterID == "" || request.NodeID == "" || request.NodeID != physical ||
		request.PodUID == "" || request.RunID == "" || request.WALIdentity != "" || request.AssignmentID != "" {
		return closed()
	}

	// Static startup resolution authenticates Pod/StatefulSet ownership and
	// verifies the Pod's immutable member key against its Secret-backed token.
	grant, static, err := e.resolveStaticVoterStartupGrant(ctx, statefulSetUID, request)
	if err != nil {
		return fencing.StartupGrant{}, fencing.InitialRegistrationProvenance{}, err
	}
	if !static || grant.Identity != request || grant.Role != fencing.StartupRoleVoterActive {
		return closed()
	}
	ordinal, err := engineRepairPodOrdinal(physical)
	if err != nil {
		return closed()
	}

	root, _, err := e.AuthorityState.ReadRoot(ctx, statefulSetUID, request.SourceClusterID)
	if err != nil {
		// A missing root must remain distinct from this path; only ordinary
		// first-generation initialization may create a new authority root.
		return fencing.StartupGrant{}, fencing.InitialRegistrationProvenance{}, fmt.Errorf("fencing: initial registration canonical root read: %w", err)
	}
	if root.StatefulSetUID != statefulSetUID || root.Ledger.SourceClusterID != request.SourceClusterID ||
		root.Ledger.ClosedBy != "" || root.Ledger.ClosedHash != "" {
		return closed()
	}
	if err := e.validateMutationJournalProfile(root); err != nil {
		return fencing.StartupGrant{}, fencing.InitialRegistrationProvenance{}, err
	}
	if !root.Ledger.RequireMutationJournal || !root.Ledger.RequireRuntimeBinding ||
		root.Ledger.ArchiveGenesisState != fencing.ArchiveGenesisCommitted || root.Ledger.ArchiveGenesisRef == nil ||
		root.Ledger.ArchiveGenesisRef.Validate() != nil {
		return closed()
	}

	sts, err := e.initialRegistrationStatefulSet(ctx, statefulSetUID, request, physical)
	if err != nil {
		return fencing.StartupGrant{}, fencing.InitialRegistrationProvenance{}, err
	}
	store, err := e.storageScope(ctx, sts, request.SourceClusterID)
	if err != nil {
		return fencing.StartupGrant{}, fencing.InitialRegistrationProvenance{}, err
	}
	scope := fencing.ArchiveScope{Provider: store.Provider, Bucket: store.Bucket, Prefix: store.Prefix, ClusterID: request.SourceClusterID}
	scopeHash, err := scope.Hash()
	if err != nil || root.Ledger.ArchiveScopeHash != scopeHash || root.Ledger.ArchiveGenesisRef.ScopeHash != scopeHash {
		return closed()
	}
	if err := root.Ledger.ArchiveGenesisRef.ValidateForScope(scope); err != nil {
		return closed()
	}

	var members []types.NodeConfig
	decoder := json.NewDecoder(strings.NewReader(grant.Environment["RHIZA_CLUSTER_MEMBERS"]))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&members); err != nil || decoder.Decode(new(any)) != io.EOF || len(members) < 2 {
		return closed()
	}
	if ordinal >= len(members) || string(members[ordinal].ID) != physical {
		return closed()
	}
	containers := list(nested(sts, "spec", "template", "spec", "containers"))
	if len(containers) == 0 {
		return closed()
	}
	storeEnv, err := e.controller().environment(ctx, asObject(containers[0]))
	if err != nil || storeEnv["RHIZA_CLUSTER_ID"] != request.SourceClusterID || storeEnv["RHIZA_CLUSTER_MEMBERS"] == "" {
		return closed()
	}
	membership := recovery.NewMembershipRecord(request.SourceClusterID, members, storeEnv["RHIZA_OBJSTORE_DURABILITY"])
	// The canonical membership record is read-only evidence; this admission
	// path never creates or repairs it.
	membershipKey := path.Join(scope.Prefix, "voters", "membership.json")
	genesisBytes, err := readInitialRegistrationObject(ctx, e.ArchiveBucket, membershipKey)
	if err != nil {
		return fencing.StartupGrant{}, fencing.InitialRegistrationProvenance{}, err
	}
	wantGenesis, err := json.Marshal(membership)
	if err != nil || !bytes.Equal(genesisBytes, wantGenesis) {
		return closed()
	}
	genesisDigest := sha256.Sum256(genesisBytes)
	membershipHash := hex.EncodeToString(genesisDigest[:])

	identityKeyHash := sha256.Sum256([]byte(request.NodeID))
	identityKey := path.Join(scope.Prefix, "voters", fmt.Sprintf("%x.json", identityKeyHash[:]))
	_, err = readInitialRegistrationObject(ctx, e.ArchiveBucket, identityKey)
	if err == nil {
		return closed()
	}
	if !errors.Is(err, errInitialRegistrationObjectNotFound) {
		return fencing.StartupGrant{}, fencing.InitialRegistrationProvenance{}, err
	}

	provenance := fencing.InitialRegistrationProvenance{
		Version: fencing.InitialRegistrationProvenanceVersion, IdentityDigest: fencing.InitialRegistrationIdentityDigest(request),
		MembershipHash: membershipHash, StoreScopeHash: scopeHash, RunIdentity: request,
	}
	if err := validateInitialRegistrationLedger(&root.Ledger, request, provenance, ordinal, statefulSetUID); err != nil {
		return fencing.StartupGrant{}, fencing.InitialRegistrationProvenance{}, err
	}
	grant.InitialRegistration = &provenance
	return grant, provenance, nil
}

// initialRegistrationMarkerRequired prevents a caller from bypassing the
// typed preflight result by sending an ordinary active-voter registration
// after canonical genesis exists but its own immutable registration is absent.
// It is read-only and does not create a root, identity, producer, or Run.
func (e *FencingExecutor) initialRegistrationMarkerRequired(ctx context.Context, statefulSetUID string, request fencing.Identity) (bool, error) {
	if e == nil || !e.RequireMutationJournal || e.ArchiveBucket == nil || ctx == nil {
		return false, nil
	}
	root, _, err := e.AuthorityState.ReadRoot(ctx, statefulSetUID, request.SourceClusterID)
	if err != nil {
		return false, err
	}
	// The first registrant must be able to establish canonical genesis through
	// the existing bootstrap path; this guard handles only later first joins.
	if root.Ledger.ArchiveGenesisState == fencing.ArchiveGenesisRegistrationPending ||
		root.Ledger.ArchiveGenesisState == fencing.ArchiveGenesisAwaitingRef {
		return false, nil
	}
	grant, static, err := e.resolveStaticVoterStartupGrant(ctx, statefulSetUID, request)
	if err != nil || !static {
		return false, err
	}
	physical := request.PodName
	if physical == "" {
		physical = request.NodeID
	}
	if grant.Identity != request {
		return false, fencing.ErrClosed
	}
	sts, err := e.initialRegistrationStatefulSet(ctx, statefulSetUID, request, physical)
	if err != nil {
		return false, err
	}
	store, err := e.storageScope(ctx, sts, request.SourceClusterID)
	if err != nil {
		return false, err
	}
	scope := fencing.ArchiveScope{Provider: store.Provider, Bucket: store.Bucket, Prefix: store.Prefix, ClusterID: request.SourceClusterID}
	containers := list(nested(sts, "spec", "template", "spec", "containers"))
	if len(containers) == 0 {
		return false, fencing.ErrClosed
	}
	storeEnv, err := e.controller().environment(ctx, asObject(containers[0]))
	if err != nil {
		return false, err
	}
	var members []types.NodeConfig
	decoder := json.NewDecoder(strings.NewReader(grant.Environment["RHIZA_CLUSTER_MEMBERS"]))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&members); err != nil || decoder.Decode(new(any)) != io.EOF || len(members) < 2 {
		return false, fencing.ErrClosed
	}
	wantGenesis, err := json.Marshal(recovery.NewMembershipRecord(request.SourceClusterID, members, storeEnv["RHIZA_OBJSTORE_DURABILITY"]))
	if err != nil {
		return false, err
	}
	membershipKey := path.Join(scope.Prefix, "voters", "membership.json")
	genesis, err := readInitialRegistrationObject(ctx, e.ArchiveBucket, membershipKey)
	if errors.Is(err, errInitialRegistrationObjectNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !bytes.Equal(genesis, wantGenesis) {
		return false, fencing.ErrClosed
	}
	identityKeyHash := sha256.Sum256([]byte(request.NodeID))
	identityKey := path.Join(scope.Prefix, "voters", fmt.Sprintf("%x.json", identityKeyHash[:]))
	_, err = readInitialRegistrationObject(ctx, e.ArchiveBucket, identityKey)
	if errors.Is(err, errInitialRegistrationObjectNotFound) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return false, nil
}

func (e *FencingExecutor) initialRegistrationStatefulSet(ctx context.Context, uid string, request fencing.Identity, physical string) (object, error) {
	var pod object
	if err := e.Kube.Get(ctx, e.controller().corePath("pods", physical), &pod); err != nil {
		return nil, err
	}
	if str(nested(pod, "metadata", "uid")) != request.PodUID || str(nested(pod, "metadata", "name")) != physical || nested(pod, "metadata", "deletionTimestamp") != nil {
		return nil, fencing.ErrClosed
	}
	owner := ""
	for _, raw := range list(nested(pod, "metadata", "ownerReferences")) {
		candidate := asObject(raw)
		if candidate["controller"] != true || candidate["kind"] != "StatefulSet" || str(candidate["uid"]) != uid {
			continue
		}
		if owner != "" {
			return nil, fmt.Errorf("fencing: initial registration StatefulSet owner is ambiguous")
		}
		owner = str(candidate["name"])
	}
	if owner == "" {
		return nil, fencing.ErrClosed
	}
	var sts object
	if err := e.Kube.Get(ctx, e.controller().stsPath(owner), &sts); err != nil {
		return nil, err
	}
	if str(nested(sts, "metadata", "uid")) != uid || str(nested(sts, "metadata", "name")) != owner || nested(sts, "metadata", "deletionTimestamp") != nil {
		return nil, fencing.ErrClosed
	}
	return sts, nil
}

func validateInitialRegistrationLedger(ledger *fencing.Ledger, identity fencing.Identity, provenance fencing.InitialRegistrationProvenance, ordinal int, statefulSetUID string) error {
	if ledger == nil || ledger.SourceClusterID != identity.SourceClusterID || ledger.ClosedBy != "" || ledger.ClosedHash != "" ||
		!ledger.RequireMutationJournal || !ledger.RequireRuntimeBinding || ledger.ArchiveGenesisState != fencing.ArchiveGenesisCommitted ||
		ledger.ArchiveScopeHash != provenance.StoreScopeHash || ledger.ArchiveGenesisRef == nil ||
		ledger.ArchiveGenesisRef.ScopeHash != provenance.StoreScopeHash || provenance.ValidateForIdentity(identity) != nil {
		return fencing.ErrClosed
	}
	for _, pending := range ledger.PendingEngineRepairs {
		if pending.SourceClusterID == identity.SourceClusterID && pending.StatefulSetUID == statefulSetUID &&
			(pending.StableOrdinal == ordinal || pending.OldMemberID == identity.NodeID) {
			return fencing.ErrClosed
		}
	}
	for _, ref := range ledger.EngineRepairs {
		if ref.Binding.SourceClusterID == identity.SourceClusterID && ref.Binding.StatefulSetUID == statefulSetUID &&
			(ref.Binding.OldMemberID == identity.NodeID || ref.CandidateIdentity.PodName == identity.PodName || ref.CandidateIdentity.PodUID == identity.PodUID) {
			return fencing.ErrClosed
		}
	}
	for runID, run := range ledger.Runs {
		if run.Identity.NodeID != identity.NodeID {
			continue
		}
		if runID == identity.RunID && run.Identity == identity && run.Identity.WALIdentity == "" && run.Stop == nil &&
			run.InitialRegistration != nil && *run.InitialRegistration == provenance {
			continue
		}
		return fencing.ErrClosed
	}
	return nil
}

// Continuation uses the caller's intact WAL, never a nonce recovered from authority.
// Final Bind still checks the historical WAL in its own canonical CAS.
func validateRegistrationContinuation(ledger *fencing.Ledger, identity fencing.Identity, wal, uid string) error {
	nonce, err := hex.DecodeString(wal)
	if err != nil || len(nonce) != 32 || ledger == nil || identity.WALIdentity != "" || identity.AssignmentID != "" ||
		identity.PodName != identity.NodeID || ledger.SourceClusterID != identity.SourceClusterID ||
		ledger.ClosedBy != "" || ledger.ClosedHash != "" {
		return fencing.ErrClosed
	}
	if _, closed := ledger.ClosedNodes[identity.NodeID]; closed {
		return fencing.ErrClosed
	}
	for _, pending := range ledger.PendingEngineRepairs {
		if pending.OldMemberID == identity.NodeID {
			return fencing.ErrClosed
		}
	}
	for _, ref := range ledger.EngineRepairs {
		if ref.Binding.OldMemberID == identity.NodeID || ref.CandidateIdentity.PodName == identity.PodName {
			return fencing.ErrClosed
		}
	}
	found := false
	for _, run := range ledger.Runs {
		if run.Identity.NodeID != identity.NodeID || run.Identity.WALIdentity == "" {
			continue
		}
		if run.Identity.WALIdentity != wal || run.Identity.PodUID != identity.PodUID || run.Identity.PodName != identity.PodName ||
			run.Stop != nil || run.Runtime == nil || run.Runtime.StatefulSetUID != uid || run.Runtime.Validate(run.Identity) != nil {
			return fencing.ErrClosed
		}
		found = true
	}
	if !found {
		return fencing.ErrClosed
	}
	return nil
}

var errInitialRegistrationObjectNotFound = errors.New("fencing: initial registration object not found")

func readInitialRegistrationObject(ctx context.Context, bucket objstore.Bucket, key string) ([]byte, error) {
	reader, err := bucket.Get(objmetrics.WithExpectedNotFound(ctx), key)
	if bucket.IsObjNotFoundErr(err) {
		return nil, errInitialRegistrationObjectNotFound
	}
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	data, err := io.ReadAll(io.LimitReader(reader, 64<<10))
	if err != nil {
		return nil, err
	}
	if len(data) == 0 || len(data) >= 64<<10 {
		return nil, fmt.Errorf("fencing: initial registration object is invalid")
	}
	return data, nil
}
