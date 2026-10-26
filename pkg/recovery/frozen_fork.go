package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"slices"

	"github.com/thanos-io/objstore"
)

const FrozenSourceManualTerminalEvidenceKind = "manual-terminal-v1"

// FrozenSourceIdentity is the stable identity used to arbitrate one target
// from a closed source generation. It deliberately excludes recovery and
// fence operation IDs, which can differ between competing requests.
type FrozenSourceIdentity struct {
	ScopeHash      string `json:"scopeHash"`
	ClusterID      string `json:"clusterID"`
	StatefulSetUID string `json:"statefulSetUID"`
}

// FrozenSourceFrontier records the exact mutable HEAD and all immutable
// archive objects needed to validate the source state copied into a target.
type FrozenSourceFrontier struct {
	ConfigID              uint                   `json:"configID"`
	HeadVersion           objstore.ObjectVersion `json:"headVersion"`
	HeadDigest            string                 `json:"headDigest"`
	BucketMetageneration  string                 `json:"bucketMetageneration"`
	LifecyclePolicyDigest string                 `json:"lifecyclePolicyDigest"`
	Base                  uint64                 `json:"base"`
	Tip                   uint64                 `json:"tip"`
	TailHash              string                 `json:"tailHash,omitempty"`
	TailObject            uint64                 `json:"tailObject,omitempty"`
	ManifestHash          string                 `json:"manifestHash"`
	Extents               []FrozenExtent         `json:"extents"`
	Checkpoint            *FrozenCheckpoint      `json:"checkpoint,omitempty"`
	LineageHash           string                 `json:"lineageHash,omitempty"`
}

// FrozenBucketEnvironment is an externally verified snapshot of the bucket
// policy governing source retention. The lifecycle digest must be computed
// over a canonical normalized policy after rejecting any delete rule that can
// match the source scope, including broad and prefixless rules.
type FrozenBucketEnvironment struct {
	Metageneration  string `json:"metageneration"`
	LifecycleDigest string `json:"lifecycleDigest"`
}

// FrozenBucketEnvironmentVerifier must reread bucket metadata and verify both
// the exact metageneration and safe normalized lifecycle policy. The recovery
// package's object-store interface cannot inspect bucket metadata itself.
type FrozenBucketEnvironmentVerifier func(context.Context, FrozenBucketEnvironment) error

type FrozenExtent struct {
	ConfigID uint   `json:"configID"`
	Start    uint64 `json:"start"`
	End      uint64 `json:"end"`
	Hash     string `json:"hash"`
	ObjectID uint64 `json:"objectID"`
}

type FrozenCheckpoint struct {
	Index     uint64 `json:"index"`
	RootHash  string `json:"rootHash"`
	StateHash string `json:"stateHash"`
}

// FrozenSourceBinding binds a recovery copy to the terminal-evidence kind and
// digest. The operator authenticates either IndependentV2 or the typed manual
// terminal attestation before constructing this binding.
type FrozenSourceBinding struct {
	Identity                FrozenSourceIdentity `json:"identity"`
	EvidenceKind            string               `json:"evidenceKind,omitempty"`
	FenceProofDigest        string               `json:"fenceProofDigest,omitempty"`
	ManualAttestationDigest string               `json:"manualAttestationDigest,omitempty"`
	ClosedRootDigest        string               `json:"closedRootDigest"`
	Frontier                FrozenSourceFrontier `json:"frontier"`
}

// FrozenForkOptions selects the same anchored target materialization as
// ForkOptions while requiring a validated closed-source binding.
type FrozenForkOptions struct {
	ForkOptions
	SourceIdentity          FrozenSourceIdentity            `json:"sourceIdentity"`
	EvidenceKind            string                          `json:"evidenceKind,omitempty"`
	FenceProofDigest        string                          `json:"fenceProofDigest,omitempty"`
	ManualAttestationDigest string                          `json:"manualAttestationDigest,omitempty"`
	ClosedRootDigest        string                          `json:"closedRootDigest"`
	BucketEnvironment       FrozenBucketEnvironment         `json:"bucketEnvironment"`
	VerifyEnvironment       FrozenBucketEnvironmentVerifier `json:"-"`
}

// FrozenForkResult is the ordinary fork result plus the exact source frontier
// that the immutable target intent and generation anchor bind.
type FrozenForkResult struct {
	ForkResult
	Source FrozenSourceBinding `json:"source"`
}

// FrozenTargetID returns a target cluster ID derived only from the stable
// source scope, cluster, and StatefulSet identity.
func FrozenTargetID(source FrozenSourceIdentity) (string, error) {
	if !validForkHash(source.ScopeHash) || source.ClusterID == "" || source.StatefulSetUID == "" {
		return "", fmt.Errorf("recovery: frozen source identity is incomplete")
	}
	data, err := json.Marshal(source)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return "rhiza-r-" + hex.EncodeToString(sum[:]), nil
}

// ForkFrozenSource materializes a verified, nonempty frontier into the
// deterministic target for a source identity. It performs no writes, pins,
// seals, renewals, or deletions under SourcePrefix. The caller must first
// authenticate and persist the complete source-closure proof and select this
// recovery mode immutably; proof digests here are bindings, not validation.
func ForkFrozenSource(ctx context.Context, bucket objstore.Bucket, options FrozenForkOptions) (FrozenForkResult, error) {
	var empty FrozenForkResult
	if bucket == nil || ctx == nil || !supportsCAS(bucket) || len(options.SourceBootstrap.Members) == 0 || len(options.TargetMembers) == 0 || options.TargetMembership != NewMembershipRecord(options.TargetMembership.Cluster, options.TargetMembers, options.TargetMembership.Durability) {
		return empty, fmt.Errorf("recovery: frozen fork requires conditional storage and immutable memberships")
	}
	sourcePrefix, targetPrefix, err := forkPrefixes(options.SourcePrefix, options.TargetPrefix)
	if err != nil {
		return empty, err
	}
	identity := options.SourceIdentity
	targetID, err := FrozenTargetID(identity)
	if err != nil || identity.ClusterID != path.Base(sourcePrefix) {
		return empty, fmt.Errorf("recovery: frozen fork source identity does not match source prefix")
	}
	if options.TargetMembership.Cluster != targetID || targetPrefix != path.Join(path.Dir(sourcePrefix), targetID) {
		return empty, fmt.Errorf("recovery: frozen fork target is not deterministic for source identity")
	}
	manualTerminal := options.EvidenceKind == FrozenSourceManualTerminalEvidenceKind && options.FenceProofDigest == "" && validForkHash(options.ManualAttestationDigest)
	independent := options.EvidenceKind == "" && options.ManualAttestationDigest == "" && validForkHash(options.FenceProofDigest)
	if options.OperationID == "" || !validForkMemberIDs(forkMemberIDs(options.SourceBootstrap.Members)) ||
		(!manualTerminal && !independent) || !validForkHash(options.ClosedRootDigest) ||
		options.BucketEnvironment.Metageneration == "" || !validForkHash(options.BucketEnvironment.LifecycleDigest) || options.VerifyEnvironment == nil {
		return empty, fmt.Errorf("recovery: frozen fork operation and proof bindings are required")
	}
	if err := options.VerifyEnvironment(ctx, options.BucketEnvironment); err != nil {
		return empty, fmt.Errorf("recovery: source bucket environment is not verified: %w", err)
	}

	source := NewManager(bucket, sourcePrefix, ArchiveConfigID)
	defer source.Close()
	frontier, snapshot, headBytes, err := captureFrozenSource(ctx, source)
	if err != nil {
		return empty, err
	}
	frontier.BucketMetageneration = options.BucketEnvironment.Metageneration
	frontier.LifecyclePolicyDigest = options.BucketEnvironment.LifecycleDigest
	binding := FrozenSourceBinding{
		Identity: identity, EvidenceKind: options.EvidenceKind, FenceProofDigest: options.FenceProofDigest,
		ManualAttestationDigest: options.ManualAttestationDigest, ClosedRootDigest: options.ClosedRootDigest, Frontier: frontier,
	}
	bootstrap, err := bootstrapHash(options.SourceBootstrap)
	if err != nil {
		return empty, err
	}
	targetMembershipHash, err := membershipHash(options.TargetMembership)
	if err != nil {
		return empty, err
	}
	intent := forkIntent{
		Version: 3, OperationID: options.OperationID, SourcePrefix: sourcePrefix,
		TargetPrefix: targetPrefix, MemberIDs: forkMemberIDs(options.SourceBootstrap.Members),
		ManifestHash: frontier.ManifestHash, SourceBootstrapHash: bootstrap,
		TargetMembershipHash: targetMembershipHash, FrozenSource: &binding,
	}
	intentBytes, err := json.Marshal(intent)
	if err != nil || len(intentBytes) > maxHeadSize {
		return empty, fmt.Errorf("recovery: frozen source intent exceeds the bounded fork record size")
	}
	if existing, exists, err := readForkIntent(ctx, bucket, targetPrefix); err != nil {
		return empty, err
	} else if exists {
		if !sameFrozenForkIntent(existing, intent) {
			return empty, fmt.Errorf("recovery: frozen target belongs to a different source or operation")
		}
		if result, ok, err := readForkResult(ctx, bucket, targetPrefix); err != nil {
			return empty, err
		} else if ok {
			if result.FrozenSource == nil || !sameFrozenSourceBinding(*result.FrozenSource, binding) || result.ManifestHash != frontier.ManifestHash {
				return empty, fmt.Errorf("recovery: frozen fork result does not match its intent")
			}
			if err := verifyFrozenFrontier(ctx, bucket, sourcePrefix, binding.Frontier, headBytes); err != nil {
				return empty, fmt.Errorf("recovery: frozen source changed after fork: %w", err)
			}
			if err := verifyCachedFrozenForkResult(ctx, bucket, targetPrefix, options, result); err != nil {
				return empty, err
			}
			if err := options.VerifyEnvironment(ctx, options.BucketEnvironment); err != nil {
				return empty, fmt.Errorf("recovery: source bucket environment changed during cached verification: %w", err)
			}
			return FrozenForkResult{ForkResult: result, Source: binding}, nil
		}
	} else if err := ensureForkTargetEmpty(ctx, bucket, targetPrefix); err != nil {
		return empty, err
	}
	if err := acquireForkIntent(ctx, bucket, targetPrefix, intent); err != nil {
		return empty, err
	}

	result := ForkResult{Tip: frontier.Tip, ManifestHash: frontier.ManifestHash, FrozenSource: &binding}
	anchor, err := materializeGeneration(ctx, bucket, result, sourcePrefix, targetPrefix, options.OperationID, options.SourceBootstrap, options.TargetMembership, snapshot, headBytes, func(ctx context.Context) error {
		return options.VerifyEnvironment(ctx, options.BucketEnvironment)
	})
	if err != nil {
		return empty, err
	}
	result.PrefixHash = anchor.SourcePrefixHash
	if err := verifyFrozenFrontier(ctx, bucket, sourcePrefix, binding.Frontier, headBytes); err != nil {
		return empty, fmt.Errorf("recovery: frozen source changed while target was materialized: %w", err)
	}
	if err := options.VerifyEnvironment(ctx, options.BucketEnvironment); err != nil {
		return empty, fmt.Errorf("recovery: source bucket environment changed while target was materialized: %w", err)
	}
	if err := writeForkResult(ctx, bucket, targetPrefix, result); err != nil {
		return empty, err
	}
	return FrozenForkResult{ForkResult: result, Source: binding}, nil
}

func captureFrozenSource(ctx context.Context, manager *Manager) (FrozenSourceFrontier, *RecoverySnapshot, []byte, error) {
	var zero FrozenSourceFrontier
	if manager == nil || ctx == nil {
		return zero, nil, nil, fmt.Errorf("recovery: frozen source reader is unavailable")
	}
	key := manager.key("archive/head.bin")
	attributes, data, _, err := manager.readStableObject(ctx, key, nil, false, maxArchiveHeadSize)
	if err != nil {
		return zero, nil, nil, fmt.Errorf("recovery: read frozen source HEAD: %w", err)
	}
	if attributes.Version == nil {
		return zero, nil, nil, fmt.Errorf("recovery: frozen source HEAD has no stable object version")
	}
	headVersion := *attributes.Version
	head, err := decodeHead(data)
	if err != nil {
		return zero, nil, nil, fmt.Errorf("recovery: decode frozen source HEAD: %w", err)
	}
	canonical, err := encodeHead(head)
	if err != nil || !bytes.Equal(canonical, data) {
		return zero, nil, nil, fmt.Errorf("recovery: frozen source HEAD is not canonical")
	}
	if head.ConfigID != ArchiveConfigID || head.Generation == 0 || head.Tip == 0 || head.Tip < head.Base || (head.Tip > head.Base) != (head.TailHash != [32]byte{}) {
		return zero, nil, nil, fmt.Errorf("recovery: frozen fork requires a valid initialized nonzero HEAD")
	}
	if err := manager.Load(ctx); err != nil {
		return zero, nil, nil, fmt.Errorf("recovery: verify frozen source archive: %w", err)
	}
	version, ok := manager.HeadVersion()
	if !ok || version != headVersion {
		return zero, nil, nil, fmt.Errorf("recovery: frozen source HEAD changed during capture")
	}
	manager.mu.Lock()
	loadedHead := manager.head
	refs := slices.Clone(manager.extents)
	manager.mu.Unlock()
	if !archiveHeadsEqual(loadedHead, head) {
		return zero, nil, nil, fmt.Errorf("recovery: frozen source HEAD differs from verified archive")
	}
	if head.Tip == 0 {
		return zero, nil, nil, fmt.Errorf("recovery: zero-tip source requires explicit empty recovery")
	}
	frontier := FrozenSourceFrontier{
		ConfigID: head.ConfigID, HeadVersion: headVersion,
		HeadDigest: digestBytes(data), Base: uint64(head.Base), Tip: uint64(head.Tip),
		TailHash: hex.EncodeToString(head.TailHash[:]), TailObject: head.TailObject,
		ManifestHash: digestBytes(canonical), Extents: make([]FrozenExtent, 0, len(refs)),
	}
	if head.TailHash == ([32]byte{}) {
		frontier.TailHash = ""
	}
	for _, ref := range refs {
		frontier.Extents = append(frontier.Extents, FrozenExtent{
			ConfigID: ref.ConfigID, Start: uint64(ref.Start), End: uint64(ref.End),
			Hash: hex.EncodeToString(ref.hash[:]), ObjectID: ref.object,
		})
	}
	if seal, _, ok := (&RecoverySnapshot{head: head}).RecoveryBase(); ok {
		frontier.Checkpoint = &FrozenCheckpoint{Index: uint64(seal.Index), RootHash: hex.EncodeToString(seal.RootHash[:]), StateHash: hex.EncodeToString(seal.StateHash[:])}
	}
	if head.BaseAnchor != nil || head.LineageAnchor != nil {
		anchorHash, ok := (&RecoverySnapshot{head: head}).GenerationAnchor()
		if !ok || anchorHash == ([32]byte{}) {
			return zero, nil, nil, fmt.Errorf("recovery: frozen source lineage anchor is invalid")
		}
		frontier.LineageHash = hex.EncodeToString(anchorHash[:])
	}
	snapshot := &RecoverySnapshot{manager: manager, head: head, refs: refs}
	return frontier, snapshot, data, nil
}

func verifyFrozenFrontier(ctx context.Context, bucket objstore.Bucket, prefix string, expected FrozenSourceFrontier, headBytes []byte) error {
	manager := NewManager(bucket, prefix, ArchiveConfigID)
	defer manager.Close()
	frontier, _, data, err := captureFrozenSource(ctx, manager)
	if err != nil {
		return err
	}
	frontier.BucketMetageneration = expected.BucketMetageneration
	frontier.LifecyclePolicyDigest = expected.LifecyclePolicyDigest
	if !sameFrozenFrontier(frontier, expected) || !bytes.Equal(data, headBytes) {
		return fmt.Errorf("source frontier does not match captured version and bytes")
	}
	return nil
}

func verifyCachedFrozenForkResult(ctx context.Context, bucket objstore.Bucket, targetPrefix string, options FrozenForkOptions, result ForkResult) error {
	if result.FrozenSource == nil || validateFrozenForkResult(result) != nil {
		return fmt.Errorf("recovery: cached frozen fork result has no valid source binding")
	}
	anchor, anchorHash, err := ReadGenerationAnchor(ctx, bucket, targetPrefix)
	if err != nil {
		return fmt.Errorf("recovery: read cached frozen target anchor: %w", err)
	}
	if anchor.FrozenSource == nil || !sameFrozenSourceBinding(*anchor.FrozenSource, *result.FrozenSource) || anchor.SourceManifest != result.ManifestHash || anchor.SourceTip != result.Tip {
		return fmt.Errorf("recovery: cached frozen target anchor does not match result")
	}
	if _, err := VerifyGenerationAnchor(ctx, bucket, targetPrefix, options.TargetMembership, anchorHash); err != nil {
		return fmt.Errorf("recovery: verify cached frozen target anchor: %w", err)
	}
	return nil
}

func validateFrozenForkResult(result ForkResult) error {
	if result.FrozenSource == nil {
		return nil
	}
	if err := validateFrozenBinding(*result.FrozenSource); err != nil {
		return err
	}
	frontier := result.FrozenSource.Frontier
	if result.Tip != frontier.Tip || result.ManifestHash != frontier.ManifestHash {
		return fmt.Errorf("recovery: frozen fork result differs from its source frontier")
	}
	return nil
}

func validateFrozenAnchorSource(anchor GenerationAnchor) error {
	if anchor.FrozenSource == nil {
		return nil
	}
	if err := validateFrozenBinding(*anchor.FrozenSource); err != nil {
		return err
	}
	frontier := anchor.FrozenSource.Frontier
	if anchor.SourceTip != frontier.Tip || anchor.SourceManifest != frontier.ManifestHash {
		return fmt.Errorf("recovery: generation anchor differs from its frozen source frontier")
	}
	return nil
}

func validateFrozenBinding(binding FrozenSourceBinding) error {
	targetID, err := FrozenTargetID(binding.Identity)
	frontier := binding.Frontier
	manualTerminal := binding.EvidenceKind == FrozenSourceManualTerminalEvidenceKind && binding.FenceProofDigest == "" && validForkHash(binding.ManualAttestationDigest)
	independent := binding.EvidenceKind == "" && binding.ManualAttestationDigest == "" && binding.FenceProofDigest != "" && validForkHash(binding.FenceProofDigest)
	if err != nil || (!manualTerminal && !independent) || !validForkHash(binding.ClosedRootDigest) || frontier.ConfigID != ArchiveConfigID || frontier.Tip == 0 || frontier.Tip < frontier.Base || frontier.HeadVersion.Value == "" || (frontier.HeadVersion.Type != objstore.Generation && frontier.HeadVersion.Type != objstore.ETag) || !validForkHash(frontier.HeadDigest) || frontier.BucketMetageneration == "" || !validForkHash(frontier.LifecyclePolicyDigest) || !validForkHash(frontier.ManifestHash) || targetID == "" {
		return fmt.Errorf("recovery: invalid frozen source binding")
	}
	if frontier.ManifestHash != frontier.HeadDigest || frontier.Tip > frontier.Base && (!validForkHash(frontier.TailHash) || frontier.TailObject == 0) || frontier.Tip == frontier.Base && frontier.TailHash != "" {
		return fmt.Errorf("recovery: frozen frontier is incomplete")
	}
	for _, extent := range frontier.Extents {
		if extent.ConfigID != ArchiveConfigID || extent.Start > extent.End || !validForkHash(extent.Hash) || extent.ObjectID == 0 {
			return fmt.Errorf("recovery: frozen frontier has an invalid extent reference")
		}
	}
	if frontier.Checkpoint != nil && (frontier.Checkpoint.Index > frontier.Tip || !validForkHash(frontier.Checkpoint.RootHash) || !validForkHash(frontier.Checkpoint.StateHash)) {
		return fmt.Errorf("recovery: frozen frontier has an invalid checkpoint reference")
	}
	if frontier.LineageHash != "" && !validForkHash(frontier.LineageHash) {
		return fmt.Errorf("recovery: frozen frontier has an invalid lineage reference")
	}
	return nil
}

func frozenTargetPrefixMatches(sourcePrefix, targetPrefix string, identity FrozenSourceIdentity) bool {
	sourcePrefix, targetPrefix, err := forkPrefixes(sourcePrefix, targetPrefix)
	if err != nil {
		return false
	}
	targetID, err := FrozenTargetID(identity)
	return err == nil && identity.ClusterID == path.Base(sourcePrefix) && targetPrefix == path.Join(path.Dir(sourcePrefix), targetID)
}

func sameFrozenForkIntent(a, b forkIntent) bool {
	return a.Version == 3 && b.Version == 3 && a.OperationID == b.OperationID && a.SourcePrefix == b.SourcePrefix && a.TargetPrefix == b.TargetPrefix && a.ManifestHash == b.ManifestHash && a.SourceBootstrapHash == b.SourceBootstrapHash && a.TargetMembershipHash == b.TargetMembershipHash && bytes.Equal(mustJSON(a.MemberIDs), mustJSON(b.MemberIDs)) && a.FrozenSource != nil && b.FrozenSource != nil && sameFrozenSourceBinding(*a.FrozenSource, *b.FrozenSource)
}

func sameFrozenSourceBinding(a, b FrozenSourceBinding) bool {
	return a.Identity == b.Identity && a.EvidenceKind == b.EvidenceKind && a.FenceProofDigest == b.FenceProofDigest &&
		a.ManualAttestationDigest == b.ManualAttestationDigest && a.ClosedRootDigest == b.ClosedRootDigest && sameFrozenFrontier(a.Frontier, b.Frontier)
}

func sameForkResult(a, b ForkResult) bool {
	if a.Tip != b.Tip || a.PrefixHash != b.PrefixHash || a.ManifestHash != b.ManifestHash || (a.FrozenSource == nil) != (b.FrozenSource == nil) {
		return false
	}
	return a.FrozenSource == nil || sameFrozenSourceBinding(*a.FrozenSource, *b.FrozenSource)
}

func sameGenerationAnchor(a, b GenerationAnchor) bool {
	if a.Version != b.Version || a.OperationID != b.OperationID || a.SourcePrefix != b.SourcePrefix || a.TargetPrefix != b.TargetPrefix || a.SourceManifest != b.SourceManifest || a.SourceTip != b.SourceTip || a.SourcePrefixHash != b.SourcePrefixHash || a.SourceBootstrap != b.SourceBootstrap || a.TargetMembership != b.TargetMembership || a.Checkpoint != b.Checkpoint || (a.FrozenSource == nil) != (b.FrozenSource == nil) {
		return false
	}
	return a.FrozenSource == nil || sameFrozenSourceBinding(*a.FrozenSource, *b.FrozenSource)
}

func sameFrozenFrontier(a, b FrozenSourceFrontier) bool {
	if a.ConfigID != b.ConfigID || a.HeadVersion != b.HeadVersion || a.HeadDigest != b.HeadDigest || a.BucketMetageneration != b.BucketMetageneration || a.LifecyclePolicyDigest != b.LifecyclePolicyDigest || a.Base != b.Base || a.Tip != b.Tip || a.TailHash != b.TailHash || a.TailObject != b.TailObject || a.ManifestHash != b.ManifestHash || a.LineageHash != b.LineageHash || len(a.Extents) != len(b.Extents) {
		return false
	}
	if (a.Checkpoint == nil) != (b.Checkpoint == nil) || a.Checkpoint != nil && *a.Checkpoint != *b.Checkpoint {
		return false
	}
	for i := range a.Extents {
		if a.Extents[i] != b.Extents[i] {
			return false
		}
	}
	return true
}

func digestBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
