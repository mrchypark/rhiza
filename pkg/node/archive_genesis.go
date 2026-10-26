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
	"path"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

type archiveGenesisClient interface {
	Upload(context.Context, string, fencing.MutationCondition, []byte) error
	ArchiveGenesisStatus(context.Context, string, uint) (fencing.ArchiveGenesisStatus, error)
}

func ensureArchiveGenesis(ctx context.Context, client archiveGenesisClient, bucket objstore.Bucket, scope fencing.ArchiveScope, mutationScopeHash string, membership recovery.MembershipRecord) error {
	if client == nil || bucket == nil || mutationScopeHash == "" {
		return fmt.Errorf("fencing: archive genesis client or scope is unavailable")
	}
	scopeHash, err := scope.Hash()
	if err != nil {
		return err
	}
	status, err := client.ArchiveGenesisStatus(ctx, "archive/head.bin", recovery.ArchiveConfigID)
	if err != nil {
		return fmt.Errorf("read canonical archive genesis: %w", err)
	}
	if err := validateArchiveGenesisStatus(status, scopeHash, mutationScopeHash); err != nil {
		return err
	}
	if status.EmptyRecoveryAuthorization != nil {
		if err := validateEmptyRecoveryArchiveGenesis(status, scopeHash, membership); err != nil {
			return err
		}
	}
	initialEmptyRecoveryAuthorization := status.EmptyRecoveryAuthorization
	if status.Ref == nil {
		if status.EmptyRecoveryAuthorization == nil && !status.CanInitializeEmpty {
			return fmt.Errorf("fencing: canonical archive genesis is not initialized")
		}
		if err := recovery.RequireEmptyArchiveScope(ctx, bucket, scope.Prefix); err != nil {
			return err
		}
		emptyHead, err := recovery.EmptyArchiveHeadBytes()
		if err != nil {
			return err
		}
		uploadErr := client.Upload(ctx, "archive/head.bin", fencing.MutationCondition{Mode: fencing.MutationCreateOnly}, emptyHead)
		status, err = client.ArchiveGenesisStatus(ctx, "archive/head.bin", recovery.ArchiveConfigID)
		if err != nil {
			return fmt.Errorf("confirm canonical archive genesis: %w", err)
		}
		if err := validateArchiveGenesisStatus(status, scopeHash, mutationScopeHash); err != nil {
			return err
		}
		if (initialEmptyRecoveryAuthorization == nil) != (status.EmptyRecoveryAuthorization == nil) ||
			(initialEmptyRecoveryAuthorization != nil && !sameEmptyRecoveryAuthorization(*initialEmptyRecoveryAuthorization, *status.EmptyRecoveryAuthorization)) {
			return fmt.Errorf("fencing: canonical empty recovery authorization changed during genesis registration")
		}
		if status.EmptyRecoveryAuthorization != nil {
			if err := validateEmptyRecoveryArchiveGenesis(status, scopeHash, membership); err != nil {
				return err
			}
		}
		if status.Ref == nil {
			if uploadErr != nil {
				return fmt.Errorf("create canonical archive genesis: %w", uploadErr)
			}
			return fmt.Errorf("fencing: archive genesis upload has no canonical reference")
		}
	}
	if status.Ref.Mutation != nil {
		emptyHead, err := recovery.EmptyArchiveHeadBytes()
		if err != nil {
			return err
		}
		if err := validateMutationArchiveGenesis(status, emptyHead); err != nil {
			return err
		}
		if status.EmptyRecoveryAuthorization != nil {
			if err := validateEmptyRecoveryArchiveGenesis(status, scopeHash, membership); err != nil {
				return err
			}
		}
	} else if status.Ref.Fork != nil {
		if err := verifyForkArchiveGenesis(ctx, bucket, scope.Prefix, membership, *status.Ref.Fork); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("fencing: archive genesis reference has no supported proof")
	}
	return requireCurrentUnsealedArchive(ctx, bucket, scope.Prefix)
}

func sameEmptyRecoveryAuthorization(a, b fencing.ArchiveEmptyRecoveryAuthorization) bool {
	left, errLeft := json.Marshal(a)
	right, errRight := json.Marshal(b)
	return errLeft == nil && errRight == nil && bytes.Equal(left, right)
}

func validateEmptyRecoveryArchiveGenesis(status fencing.ArchiveGenesisStatus, scopeHash string, membership recovery.MembershipRecord) error {
	authorization := status.EmptyRecoveryAuthorization
	if authorization == nil || authorization.Validate() != nil || authorization.TargetScopeHash != scopeHash ||
		authorization.TargetClusterID != membership.Cluster || authorization.TargetDurability != membership.Durability ||
		authorization.SourceTip != 0 || (status.EmptyRecoveryState != fencing.EmptyRecoveryAuthorized && status.EmptyRecoveryState != fencing.EmptyRecoveryCommitted) {
		return fmt.Errorf("fencing: canonical empty recovery authorization differs from target scope")
	}
	encoded, err := json.Marshal(membership)
	if err != nil || archiveGenesisBytesDigest(encoded) != authorization.TargetMembershipHash {
		return fmt.Errorf("fencing: canonical empty recovery membership differs")
	}
	emptyHead, err := recovery.EmptyArchiveHeadBytes()
	if err != nil || archiveGenesisBytesDigest(emptyHead) != authorization.TargetEmptyHeadDigest {
		return fmt.Errorf("fencing: canonical empty recovery does not authorize the empty archive HEAD")
	}
	return nil
}

func validateArchiveGenesisStatus(status fencing.ArchiveGenesisStatus, scopeHash, mutationScopeHash string) error {
	if err := status.Validate(mutationScopeHash, recovery.ArchiveConfigID); err != nil {
		return err
	}
	if status.ScopeHash != scopeHash {
		return fmt.Errorf("fencing: canonical archive genesis stable scope differs")
	}
	return nil
}

func validateMutationArchiveGenesis(status fencing.ArchiveGenesisStatus, emptyHead []byte) error {
	ref := status.Ref
	if ref == nil || ref.Mutation == nil || status.Mutation == nil || !status.Mutation.Found || status.Mutation.Entry == nil {
		return fmt.Errorf("fencing: archive genesis mutation evidence is unavailable")
	}
	intent := ref.Mutation.Intent
	digest := sha256.Sum256(emptyHead)
	digestHex := hex.EncodeToString(digest[:])
	if intent.Key != "archive/head.bin" || intent.Kind != "upload" || intent.Condition.Mode != fencing.MutationCreateOnly ||
		intent.PayloadDigest != digestHex || intent.PayloadSize != int64(len(emptyHead)) || status.Mutation.Entry.Intent != intent {
		return fmt.Errorf("fencing: archive genesis mutation does not bind the canonical empty HEAD")
	}
	resolution := status.Mutation.Entry.Resolution
	if resolution == nil || resolution.Outcome != fencing.MutationCommitted || !resolution.AllCopiesTerminal ||
		resolution.ConditionFailed || resolution.PayloadDigest != digestHex || !validArchiveGenesisDigest(resolution.PreparationHash) ||
		!validArchiveGenesisDigest(resolution.ReceiptDigest) {
		return fmt.Errorf("fencing: archive genesis mutation lacks terminal commit evidence")
	}
	return nil
}

func requireCurrentUnsealedArchive(ctx context.Context, bucket objstore.Bucket, prefix string) error {
	archive := recovery.NewManager(bucket, prefix, recovery.ArchiveConfigID)
	defer archive.Close()
	if err := archive.Load(ctx); err != nil {
		return fmt.Errorf("load canonical archive genesis HEAD: %w", err)
	}
	if _, exists := archive.HeadVersion(); !exists {
		return fmt.Errorf("fencing: canonical archive genesis has no persisted HEAD")
	}
	if archive.Sealed() {
		return recovery.ErrArchiveSealed
	}
	return nil
}

func verifyForkArchiveGenesis(ctx context.Context, bucket objstore.Bucket, prefix string, membership recovery.MembershipRecord, proof fencing.ArchiveGenesisFork) error {
	if proof.TargetPrefix != prefix || proof.ConfigID != recovery.ArchiveConfigID || proof.AnchorKey != recovery.GenerationAnchorKey(prefix) {
		return fmt.Errorf("fencing: archive fork genesis identity differs")
	}
	anchor, anchorHash, err := recovery.ReadGenerationAnchor(ctx, bucket, prefix)
	if err != nil {
		return fmt.Errorf("read archive fork anchor: %w", err)
	}
	verified, err := recovery.VerifyGenerationAnchor(ctx, bucket, prefix, membership, anchorHash)
	if err != nil {
		return fmt.Errorf("verify archive fork anchor: %w", err)
	}
	if verified != anchor || anchor.TargetPrefix != prefix {
		return fmt.Errorf("fencing: verified archive fork anchor differs")
	}
	intentDigest, err := archiveGenesisObjectDigest(ctx, bucket, path.Join(prefix, "fork/intent.json"))
	if err != nil {
		return err
	}
	resultDigest, err := archiveGenesisObjectDigest(ctx, bucket, path.Join(prefix, "fork/result.json"))
	if err != nil {
		return err
	}
	lineageData, err := readArchiveGenesisObject(ctx, bucket, path.Join(prefix, "recovery/generation.json"))
	if err != nil {
		return err
	}
	var lineage struct {
		Fork             recovery.ForkResult       `json:"fork"`
		OperationID      string                    `json:"operation_id"`
		Source           string                    `json:"source"`
		SourceDurability string                    `json:"source_durability"`
		SourceMembership string                    `json:"source_membership"`
		TargetMembership recovery.MembershipRecord `json:"target_membership"`
		Version          int                       `json:"version"`
	}
	decoder := json.NewDecoder(bytes.NewReader(lineageData))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&lineage); err != nil || decoder.Decode(new(any)) != io.EOF {
		return fmt.Errorf("fencing: invalid archive fork lineage")
	}
	canonical, err := json.Marshal(lineage)
	if err != nil || !bytes.Equal(canonical, lineageData) || lineage.Version != 1 || lineage.OperationID != anchor.OperationID ||
		lineage.Source != path.Base(anchor.SourcePrefix) || lineage.SourceDurability == "" || !validArchiveGenesisDigest(lineage.SourceMembership) ||
		lineage.TargetMembership != membership || lineage.Fork.Tip != anchor.SourceTip || lineage.Fork.ManifestHash != anchor.SourceManifest ||
		lineage.Fork.PrefixHash != anchor.SourcePrefixHash {
		return fmt.Errorf("fencing: archive fork lineage does not match its verified anchor")
	}
	membershipBytes, err := json.Marshal(membership)
	if err != nil {
		return err
	}
	membershipHash := sha256.Sum256(membershipBytes)
	anchorHashHex := hex.EncodeToString(anchorHash[:])
	if proof.AnchorDigest != anchorHashHex || proof.AnchorHash != anchorHashHex || proof.IntentDigest != intentDigest ||
		proof.ResultDigest != resultDigest || proof.LineageDigest != archiveGenesisBytesDigest(lineageData) ||
		proof.MembershipHash != hex.EncodeToString(membershipHash[:]) || proof.ConfigID != recovery.ArchiveConfigID ||
		proof.BaseTip != anchor.SourceTip || proof.BasePrefixHash != anchor.SourcePrefixHash {
		return fmt.Errorf("fencing: archive fork genesis reference differs from verified records")
	}
	return nil
}

func archiveGenesisObjectDigest(ctx context.Context, bucket objstore.Bucket, key string) (string, error) {
	data, err := readArchiveGenesisObject(ctx, bucket, key)
	if err != nil {
		return "", err
	}
	return archiveGenesisBytesDigest(data), nil
}

func readArchiveGenesisObject(ctx context.Context, bucket objstore.Bucket, key string) ([]byte, error) {
	reader, err := bucket.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	data, readErr := io.ReadAll(io.LimitReader(reader, 1<<20+1))
	closeErr := reader.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return nil, fmt.Errorf("read archive genesis evidence: %w", err)
	}
	if len(data) > 1<<20 {
		return nil, fmt.Errorf("archive genesis evidence exceeds size limit")
	}
	return data, nil
}

func archiveGenesisBytesDigest(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func validArchiveGenesisDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == value
}
