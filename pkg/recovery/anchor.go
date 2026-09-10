package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

type GenerationCheckpoint struct {
	Index     uint64 `json:"index"`
	RootHash  string `json:"root_hash"`
	StateHash string `json:"state_hash"`
}

// GenerationAnchor is the immutable, externally fenced trust transition from
// verified source history to a new target bootstrap. It carries no credentials.
type GenerationAnchor struct {
	Version          int                  `json:"version"`
	OperationID      string               `json:"operation_id"`
	SourcePrefix     string               `json:"source_prefix"`
	TargetPrefix     string               `json:"target_prefix"`
	SourceManifest   string               `json:"source_manifest"`
	SourceTip        uint64               `json:"source_tip"`
	SourcePrefixHash string               `json:"source_prefix_hash"`
	SourceBootstrap  string               `json:"source_bootstrap_hash"`
	TargetMembership MembershipRecord     `json:"target_membership"`
	Checkpoint       GenerationCheckpoint `json:"checkpoint"`
}

func GenerationAnchorKey(prefix string) string { return forkKey(prefix, "recovery/anchor.json") }

func ReadGenerationAnchor(ctx context.Context, bucket objstore.Bucket, prefix string) (GenerationAnchor, [32]byte, error) {
	r, err := bucket.Get(ctx, GenerationAnchorKey(prefix))
	if err != nil {
		return GenerationAnchor{}, [32]byte{}, err
	}
	data, readErr := io.ReadAll(io.LimitReader(r, maxHeadSize+1))
	closeErr := r.Close()
	if readErr != nil || closeErr != nil || len(data) > maxHeadSize {
		if readErr != nil {
			return GenerationAnchor{}, [32]byte{}, fmt.Errorf("read generation anchor: %w", readErr)
		}
		return GenerationAnchor{}, [32]byte{}, fmt.Errorf("read generation anchor: %w", closeErr)
	}
	var anchor GenerationAnchor
	if err := json.Unmarshal(data, &anchor); err != nil {
		return GenerationAnchor{}, [32]byte{}, fmt.Errorf("invalid generation anchor")
	}
	canonical, _ := json.Marshal(anchor)
	if !bytes.Equal(data, canonical) {
		return GenerationAnchor{}, [32]byte{}, fmt.Errorf("invalid generation anchor")
	}
	if err := ValidateGenerationAnchor(anchor); err != nil {
		return GenerationAnchor{}, [32]byte{}, err
	}
	requested, err := normalizeForkPrefix(prefix)
	if err != nil || anchor.TargetPrefix != requested {
		return GenerationAnchor{}, [32]byte{}, fmt.Errorf("generation anchor target prefix does not match request")
	}
	return anchor, sha256.Sum256(data), nil
}

func ValidateGenerationAnchor(anchor GenerationAnchor) error {
	if anchor.Version != 1 || anchor.OperationID == "" || anchor.SourcePrefix == "" || anchor.TargetPrefix == "" || anchor.SourceTip == 0 || !validForkHash(anchor.SourceManifest) || !validForkHash(anchor.SourcePrefixHash) || !validForkHash(anchor.SourceBootstrap) || anchor.TargetMembership.Version != 1 || anchor.Checkpoint.Index != anchor.SourceTip || !validForkHash(anchor.Checkpoint.RootHash) || !validForkHash(anchor.Checkpoint.StateHash) {
		return fmt.Errorf("invalid generation anchor")
	}
	return nil
}

func bootstrapHash(cluster quepaxa.Cluster) (string, error) {
	data, err := json.Marshal(cluster)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func membershipHash(membership MembershipRecord) (string, error) {
	data, err := json.Marshal(membership)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func parseGenerationHash(value string) ([32]byte, error) {
	var hash [32]byte
	if !validForkHash(value) {
		return hash, fmt.Errorf("invalid generation hash")
	}
	_, err := hex.Decode(hash[:], []byte(value))
	return hash, err
}

// GenerationAnchorPin holds the checkpoint root named by an anchor through a
// bootstrap or recovery operation. Callers must keep it renewed until a later
// certified target checkpoint has made the anchor root dispensable.
type GenerationAnchorPin struct {
	Anchor GenerationAnchor
	Hash   [32]byte
	*checkpoint.RecoveryPin
}

// PinGenerationAnchor reads and validates a target anchor, verifies its
// immutable checkpoint descriptor, and pins that root against checkpoint GC.
func PinGenerationAnchor(ctx context.Context, bucket objstore.Bucket, prefix, owner string, lease time.Duration) (*GenerationAnchorPin, error) {
	anchor, hash, err := ReadGenerationAnchor(ctx, bucket, prefix)
	if err != nil {
		return nil, err
	}
	rootHash, err := parseGenerationHash(anchor.Checkpoint.RootHash)
	if err != nil {
		return nil, err
	}
	stateHash, err := parseGenerationHash(anchor.Checkpoint.StateHash)
	if err != nil {
		return nil, err
	}
	manager := checkpoint.NewManager(bucket, prefix, "", 1)
	root, err := manager.OpenRoot(ctx, anchor.Checkpoint.Index, rootHash)
	if err != nil {
		return nil, err
	}
	if err := manager.Verify(ctx, root.Index, root.RootHash, stateHash); err != nil {
		return nil, err
	}
	pin, err := manager.PinRecoveryRoot(ctx, root, owner, lease)
	if err != nil {
		return nil, err
	}
	return &GenerationAnchorPin{Anchor: anchor, Hash: hash, RecoveryPin: pin}, nil
}

// VerifyGenerationAnchor validates the immutable target transition before
// node startup. expectedMembership must be read from the target's immutable
// voter registration; this function intentionally does not infer authority
// from an object-store anchor alone. It verifies archive lineage and the fork
// intent/result bindings, but does not require the original anchor checkpoint
// to remain present after a newer certified target checkpoint has superseded it.
func VerifyGenerationAnchor(ctx context.Context, bucket objstore.Bucket, prefix string, expectedMembership MembershipRecord, expectedAnchorHash [32]byte) (GenerationAnchor, error) {
	if expectedAnchorHash == ([32]byte{}) || expectedMembership.Version != 1 {
		return GenerationAnchor{}, fmt.Errorf("expected generation anchor and immutable membership are required")
	}
	anchor, hash, err := ReadGenerationAnchor(ctx, bucket, prefix)
	if err != nil || hash != expectedAnchorHash || anchor.TargetMembership != expectedMembership {
		return GenerationAnchor{}, fmt.Errorf("generation anchor does not match target registration")
	}
	intent, ok, err := readForkIntent(ctx, bucket, prefix)
	if err != nil || !ok || intent.Version != 2 || intent.OperationID != anchor.OperationID || intent.SourcePrefix != anchor.SourcePrefix || intent.TargetPrefix != anchor.TargetPrefix || intent.ManifestHash != anchor.SourceManifest || intent.SourceBootstrapHash != anchor.SourceBootstrap {
		return GenerationAnchor{}, fmt.Errorf("generation anchor does not match fork intent")
	}
	membershipHash, err := membershipHash(expectedMembership)
	if err != nil || intent.TargetMembershipHash != membershipHash {
		return GenerationAnchor{}, fmt.Errorf("generation anchor does not match target membership intent")
	}
	result, ok, err := readForkResult(ctx, bucket, prefix)
	if err != nil || !ok || result.Tip != anchor.SourceTip || result.ManifestHash != anchor.SourceManifest || result.PrefixHash != anchor.SourcePrefixHash {
		return GenerationAnchor{}, fmt.Errorf("generation anchor does not match fork result")
	}
	archive := NewManager(bucket, prefix, 1)
	defer archive.Close()
	if err := archive.Load(ctx); err != nil {
		return GenerationAnchor{}, err
	}
	if got, ok := archive.GenerationAnchor(); !ok || got != expectedAnchorHash {
		return GenerationAnchor{}, fmt.Errorf("target archive does not retain generation lineage")
	}
	archive.mu.Lock()
	base, basePrefix, baseAnchor, baseSeal := archive.head.Base, archive.head.BasePrefix, archive.head.BaseAnchor, archive.head.BaseSeal
	archive.mu.Unlock()
	if base < quepaxa.Slot(anchor.SourceTip) {
		return GenerationAnchor{}, fmt.Errorf("target archive base precedes generation anchor")
	}
	if base == quepaxa.Slot(anchor.SourceTip) {
		if basePrefix != mustGenerationHash(anchor.SourcePrefixHash) {
			return GenerationAnchor{}, fmt.Errorf("target archive anchor base does not match generation anchor")
		}
		if baseAnchor != nil {
			if baseAnchor.Hash != expectedAnchorHash {
				return GenerationAnchor{}, fmt.Errorf("target archive anchor base does not match generation anchor")
			}
		} else if baseSeal == nil || baseSeal.GenerationAnchorHash != expectedAnchorHash {
			return GenerationAnchor{}, fmt.Errorf("target archive checkpoint base does not match generation lineage")
		}
		if baseAnchor != nil {
			rootHash, err := parseGenerationHash(anchor.Checkpoint.RootHash)
			if err != nil {
				return GenerationAnchor{}, err
			}
			stateHash, err := parseGenerationHash(anchor.Checkpoint.StateHash)
			if err != nil {
				return GenerationAnchor{}, err
			}
			checkpoints := checkpoint.NewManager(bucket, prefix, "", 1)
			root, err := checkpoints.OpenRoot(ctx, anchor.Checkpoint.Index, rootHash)
			if err != nil || checkpoints.Verify(ctx, root.Index, root.RootHash, stateHash) != nil {
				return GenerationAnchor{}, fmt.Errorf("generation anchor checkpoint is unavailable or invalid")
			}
		}
	}
	return anchor, nil
}

func mustGenerationHash(value string) [32]byte {
	hash, _ := parseGenerationHash(value)
	return hash
}

// MaterializeGeneration verifies source certificates, applies the verified
// suffix into a temporary materializer, and publishes a target-only root and
// empty anchored archive. The caller must have fenced every source writer.
func MaterializeGeneration(ctx context.Context, bucket objstore.Bucket, result ForkResult, sourcePrefix, targetPrefix, operationID string, sourceBootstrap quepaxa.Cluster, targetMembership MembershipRecord) (GenerationAnchor, error) {
	if bucket == nil || operationID == "" || len(sourceBootstrap.Members) == 0 || targetMembership.Version != 1 || result.Tip == 0 || !validForkHash(result.ManifestHash) || result.PrefixHash != "" && !validForkHash(result.PrefixHash) {
		return GenerationAnchor{}, fmt.Errorf("invalid generation materialization")
	}
	source := NewManager(bucket, sourcePrefix, 1)
	defer source.Close()
	if err := source.Load(ctx); err != nil {
		return GenerationAnchor{}, err
	}
	snapshot, err := source.BeginRecoverySnapshot(ctx, "materialize-"+shortHash(operationID), forkLease)
	if err != nil {
		return GenerationAnchor{}, err
	}
	defer closeSnapshot(snapshot)
	if snapshot.Tip() != quepaxa.Slot(result.Tip) {
		return GenerationAnchor{}, fmt.Errorf("materialization source tip does not match result")
	}
	headData, err := encodeHead(snapshot.head)
	headHash := sha256.Sum256(headData)
	if err != nil || hex.EncodeToString(headHash[:]) != result.ManifestHash {
		return GenerationAnchor{}, fmt.Errorf("materialization source manifest does not match result")
	}
	if anchor, ok, err := resumeMaterializedGeneration(ctx, bucket, result, sourcePrefix, targetPrefix, operationID, sourceBootstrap, targetMembership); err != nil {
		return GenerationAnchor{}, err
	} else if ok {
		return anchor, nil
	}
	seal, decision, hasCheckpoint := snapshot.RecoveryBase()
	sourceCP := checkpoint.NewManager(bucket, sourcePrefix, "", 1)
	var root *checkpoint.Checkpoint
	var rootPin *checkpoint.RecoveryPin
	var sourceAnchor GenerationAnchor
	anchorHash, anchored := snapshot.GenerationAnchor()
	if anchored {
		stored, storedHash, anchorErr := ReadGenerationAnchor(ctx, bucket, sourcePrefix)
		if anchorErr != nil || storedHash != anchorHash {
			return GenerationAnchor{}, fmt.Errorf("source anchored lineage does not match archive")
		}
		expectedSourceMembership := NewMembershipRecord(stored.TargetMembership.Cluster, sourceBootstrap.Members, stored.TargetMembership.Durability)
		if _, anchorErr := VerifyGenerationAnchor(ctx, bucket, sourcePrefix, expectedSourceMembership, anchorHash); anchorErr != nil {
			return GenerationAnchor{}, fmt.Errorf("verify source anchored lineage: %w", anchorErr)
		}
		sourceAnchor = stored
	}
	if hasCheckpoint {
		root, err = sourceCP.OpenRoot(ctx, uint64(seal.Index), seal.RootHash)
		if err != nil {
			return GenerationAnchor{}, fmt.Errorf("open source materialization root: %w", err)
		}
		if root.Hash != seal.StateHash {
			return GenerationAnchor{}, fmt.Errorf("source materialization root state hash mismatch")
		}
	} else if anchored {
		rootHash, hashErr := parseGenerationHash(sourceAnchor.Checkpoint.RootHash)
		if hashErr != nil {
			return GenerationAnchor{}, hashErr
		}
		root, err = sourceCP.OpenRoot(ctx, sourceAnchor.Checkpoint.Index, rootHash)
		if err != nil {
			return GenerationAnchor{}, err
		}
		stateHash, _ := parseGenerationHash(sourceAnchor.Checkpoint.StateHash)
		if root.Hash != stateHash {
			return GenerationAnchor{}, fmt.Errorf("source anchored root state hash mismatch")
		}
	} else if snapshot.head.Base != 0 {
		return GenerationAnchor{}, fmt.Errorf("source archive has an unsupported recovery base")
	}
	if root != nil {
		if err := sourceCP.Verify(ctx, root.Index, root.RootHash, root.Hash); err != nil {
			return GenerationAnchor{}, err
		}
		rootPin, err = sourceCP.PinRecoveryRoot(ctx, root, "materialize-"+shortHash(operationID), forkLease)
		if err != nil {
			return GenerationAnchor{}, err
		}
		defer closeRootPin(rootPin)
	}
	guard := newForkEvidenceGuard(ctx, snapshot, rootPin)
	defer guard.Close()
	ctx = guard.Context()
	dir, err := os.MkdirTemp("", "rhiza-generation-materialize-*")
	if err != nil {
		return GenerationAnchor{}, err
	}
	defer os.RemoveAll(dir)
	var files []checkpoint.File
	if root != nil {
		files, err = sourceCP.DownloadAndVerifyRootFiles(ctx, root, dir)
		if err != nil {
			return GenerationAnchor{}, err
		}
	}
	workDir := dir + "/target"
	if err := os.Mkdir(workDir, 0o700); err != nil {
		return GenerationAnchor{}, err
	}
	material, err := materializer.Open(workDir+"/sqlite.db", 1)
	if err != nil {
		return GenerationAnchor{}, err
	}
	defer material.Close()
	converted := make([]materializer.CheckpointFile, 0, len(files))
	for _, file := range files {
		converted = append(converted, materializer.CheckpointFile{Role: materializer.CheckpointRole(file.Role), Path: file.Path})
	}
	if len(converted) != 0 {
		if err := material.RestoreCheckpoint(ctx, converted); err != nil {
			return GenerationAnchor{}, fmt.Errorf("restore source materialization checkpoint: %w", err)
		}
	}
	wal, err := qlog.Open(dir + "/verify-wal")
	if err != nil {
		return GenerationAnchor{}, err
	}
	defer wal.Close()
	// A base-zero source has no seal from which to infer membership mode, so
	// enable transition verification. A checkpointed fixed-membership source
	// retains legacy fixed verification unless its seal carries a history.
	core, err := quepaxa.NewObserver(quepaxa.Config{NodeID: "rhiza-generation-verifier", Cluster: sourceBootstrap, WAL: wal, EnableReconfiguration: !hasCheckpoint || seal.Membership != nil})
	if err != nil {
		return GenerationAnchor{}, err
	}
	core.SetCheckpointValidator(func(ctx context.Context, candidate quepaxa.CheckpointSeal) error {
		return sourceCP.Verify(ctx, uint64(candidate.Index), candidate.RootHash, candidate.StateHash)
	})
	if anchored {
		prefixHash, _ := parseGenerationHash(sourceAnchor.SourcePrefixHash)
		rootHash, _ := parseGenerationHash(sourceAnchor.Checkpoint.RootHash)
		if err := core.InstallFencedGenerationBase(ctx, quepaxa.FencedGenerationBase{ConfigID: sourceBootstrap.ConfigID, Index: quepaxa.Slot(sourceAnchor.SourceTip), PrefixHash: prefixHash, RootHash: rootHash, AnchorHash: anchorHash}); err != nil {
			return GenerationAnchor{}, err
		}
	}
	if hasCheckpoint {
		if err := core.RestoreCheckpointBase(ctx, seal, decision); err != nil {
			return GenerationAnchor{}, err
		}
	}
	for core.Tip() < snapshot.Tip() {
		values, _, err := snapshot.DecisionsFrom(ctx, core.Tip()+1, 256)
		if err != nil || len(values) == 0 {
			return GenerationAnchor{}, fmt.Errorf("read source materialization suffix: %w", err)
		}
		if err := core.AcceptCertifiedValues(values); err != nil {
			return GenerationAnchor{}, err
		}
		if err := material.ApplyBatch(ctx, values); err != nil {
			return GenerationAnchor{}, err
		}
	}
	prefix, ok := core.PrefixHash(core.Tip())
	if !ok || result.PrefixHash != "" && hex.EncodeToString(prefix[:]) != result.PrefixHash {
		return GenerationAnchor{}, fmt.Errorf("source materialization prefix mismatch")
	}
	targetCP := checkpoint.NewManager(bucket, targetPrefix, "", 1)
	checkpointFiles, index, cleanup, err := material.CheckpointFilesAt(ctx)
	if err != nil {
		return GenerationAnchor{}, err
	}
	defer cleanup()
	if index != result.Tip {
		return GenerationAnchor{}, fmt.Errorf("materialized tip does not match source tip")
	}
	sources := make([]checkpoint.Source, 0, len(checkpointFiles))
	for _, file := range checkpointFiles {
		sources = append(sources, checkpoint.Source{Role: string(file.Role), Path: file.Path})
	}
	if err := targetCP.Load(ctx); err != nil {
		return GenerationAnchor{}, err
	}
	targetRoot := targetCP.Latest()
	if targetRoot != nil {
		if targetRoot.Index != index {
			return GenerationAnchor{}, fmt.Errorf("target checkpoint CURRENT does not match materialized tip")
		}
		matches, err := checkpointSourcesMatch(ctx, targetCP, targetRoot, sources)
		if err != nil || !matches {
			return GenerationAnchor{}, fmt.Errorf("target checkpoint CURRENT does not match materialized state")
		}
	} else {
		claim, err := targetCP.AcquireGenerationClaim(ctx, "generation-"+shortHash(operationID), index, forkLease)
		if err != nil {
			return GenerationAnchor{}, err
		}
		defer targetCP.ReleasePublisherClaim(context.Background(), claim)
		targetRoot, err = targetCP.CreateFiles(ctx, claim, sources, index)
		if err != nil {
			return GenerationAnchor{}, err
		}
		claim, err = targetCP.BindPublisherClaim(ctx, claim, index, targetRoot.RootHash, forkLease)
		if err != nil {
			return GenerationAnchor{}, err
		}
		if err := targetCP.PromoteCertifiedCurrent(ctx, targetRoot); err != nil {
			return GenerationAnchor{}, err
		}
	}
	bootstrap, err := bootstrapHash(sourceBootstrap)
	if err != nil {
		return GenerationAnchor{}, err
	}
	if err := guard.Check(); err != nil {
		return GenerationAnchor{}, err
	}
	if err := source.Load(ctx); err != nil || !archiveHeadsEqual(source.head, snapshot.head) {
		return GenerationAnchor{}, fmt.Errorf("source archive changed during materialization")
	}
	anchor := GenerationAnchor{Version: 1, OperationID: operationID, SourcePrefix: sourcePrefix, TargetPrefix: targetPrefix, SourceManifest: result.ManifestHash, SourceTip: result.Tip, SourcePrefixHash: hex.EncodeToString(prefix[:]), SourceBootstrap: bootstrap, TargetMembership: targetMembership, Checkpoint: GenerationCheckpoint{Index: targetRoot.Index, RootHash: hex.EncodeToString(targetRoot.RootHash[:]), StateHash: hex.EncodeToString(targetRoot.Hash[:])}}
	data, _ := json.Marshal(anchor)
	if err := bucket.Upload(ctx, GenerationAnchorKey(targetPrefix), bytes.NewReader(data), objstore.WithIfNotExists()); err != nil && !bucket.IsConditionNotMetErr(err) {
		return GenerationAnchor{}, err
	}
	stored, anchorHash, err := ReadGenerationAnchor(ctx, bucket, targetPrefix)
	if err != nil || stored != anchor {
		return GenerationAnchor{}, fmt.Errorf("generation anchor conflict")
	}
	target := NewManager(bucket, targetPrefix, 1)
	defer target.Close()
	if err := target.InitializeFencedGeneration(ctx, quepaxa.Slot(result.Tip), prefix, anchorHash); err != nil {
		return GenerationAnchor{}, err
	}
	return anchor, nil
}

func checkpointSourcesMatch(ctx context.Context, manager *checkpoint.Manager, root *checkpoint.Checkpoint, sources []checkpoint.Source) (bool, error) {
	dir, err := os.MkdirTemp("", "rhiza-generation-current-*")
	if err != nil {
		return false, err
	}
	defer os.RemoveAll(dir)
	files, err := manager.DownloadAndVerifyRootFiles(ctx, root, dir)
	if err != nil || len(files) != len(sources) {
		return false, err
	}
	want := make(map[string]string, len(sources))
	for _, source := range sources {
		want[source.Role] = source.Path
	}
	for _, file := range files {
		source, ok := want[file.Role]
		if !ok {
			return false, nil
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}
		left, err := os.Open(source)
		if err != nil {
			return false, err
		}
		leftHash := sha256.New()
		_, leftErr := io.Copy(leftHash, left)
		leftCloseErr := left.Close()
		if leftErr != nil || leftCloseErr != nil {
			if leftErr != nil {
				return false, leftErr
			}
			return false, leftCloseErr
		}
		right, err := os.Open(file.Path)
		if err != nil {
			return false, err
		}
		rightHash := sha256.New()
		_, rightErr := io.Copy(rightHash, right)
		rightCloseErr := right.Close()
		if rightErr != nil || rightCloseErr != nil {
			if rightErr != nil {
				return false, rightErr
			}
			return false, rightCloseErr
		}
		if !bytes.Equal(leftHash.Sum(nil), rightHash.Sum(nil)) {
			return false, nil
		}
	}
	return true, nil
}

// resumeMaterializedGeneration recognizes the durable point after target root
// and anchor publication. Replaying from the source here would try to acquire
// an obsolete publisher floor and could overwrite a fenced target generation.
func resumeMaterializedGeneration(ctx context.Context, bucket objstore.Bucket, result ForkResult, sourcePrefix, targetPrefix, operationID string, sourceBootstrap quepaxa.Cluster, targetMembership MembershipRecord) (GenerationAnchor, bool, error) {
	anchor, anchorHash, err := ReadGenerationAnchor(ctx, bucket, targetPrefix)
	if bucket.IsObjNotFoundErr(err) {
		return GenerationAnchor{}, false, nil
	}
	if err != nil {
		return GenerationAnchor{}, false, err
	}
	bootstrap, err := bootstrapHash(sourceBootstrap)
	if err != nil || anchor.OperationID != operationID || anchor.SourcePrefix != sourcePrefix || anchor.TargetPrefix != targetPrefix || anchor.SourceManifest != result.ManifestHash || anchor.SourceTip != result.Tip || anchor.SourceBootstrap != bootstrap || anchor.TargetMembership != targetMembership || result.PrefixHash != "" && anchor.SourcePrefixHash != result.PrefixHash {
		return GenerationAnchor{}, false, fmt.Errorf("published generation anchor does not match fork request")
	}
	intent, present, err := readForkIntent(ctx, bucket, targetPrefix)
	targetHash, hashErr := membershipHash(targetMembership)
	if err != nil || !present || intent.Version != 2 || intent.OperationID != operationID || intent.SourcePrefix != sourcePrefix || intent.TargetPrefix != targetPrefix || intent.ManifestHash != result.ManifestHash || intent.SourceBootstrapHash != bootstrap || hashErr != nil || intent.TargetMembershipHash != targetHash {
		return GenerationAnchor{}, false, fmt.Errorf("published generation anchor does not match fork intent")
	}
	pin, err := PinGenerationAnchor(ctx, bucket, targetPrefix, "resume-"+shortHash(operationID), forkLease)
	if err != nil {
		return GenerationAnchor{}, false, fmt.Errorf("verify published generation root: %w", err)
	}
	defer closeRootPin(pin.RecoveryPin)
	prefix, err := parseGenerationHash(anchor.SourcePrefixHash)
	if err != nil {
		return GenerationAnchor{}, false, err
	}
	target := NewManager(bucket, targetPrefix, 1)
	defer target.Close()
	if err := target.Load(ctx); err != nil {
		return GenerationAnchor{}, false, err
	}
	if err := target.InitializeFencedGeneration(ctx, quepaxa.Slot(anchor.SourceTip), prefix, anchorHash); err != nil {
		return GenerationAnchor{}, false, err
	}
	if got, ok := target.GenerationAnchor(); !ok || got != anchorHash {
		return GenerationAnchor{}, false, fmt.Errorf("published target archive does not match generation anchor")
	}
	return anchor, true, nil
}
