package fencing

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/thanos-io/objstore"
)

func hashJSON(value any) string {
	encoded, _ := json.Marshal(value)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func archiveGenesisMutationRefForTest(t *testing.T, scope ArchiveScope) ArchiveGenesisRef {
	t.Helper()
	scopeHash, err := scope.Hash()
	if err != nil {
		t.Fatal(err)
	}
	mutationScopeHash := hashJSON([]string{"mutation-execution-scope"})
	intent := MutationIntent{
		ContractVersion: MutationIntentVersion,
		StoreScopeHash:  mutationScopeHash,
		Condition:       MutationCondition{Mode: MutationCreateOnly},
		OperationID:     MutationRequestID("producer-uid", 1),
		Kind:            "upload",
		Key:             "archive/head.bin",
		PayloadRef:      "stage-ref",
		PayloadDigest:   hashJSON([]string{"head-bytes"}),
		PayloadSize:     10,
	}
	return ArchiveGenesisRef{Version: ArchiveGenesisVersion, ConfigID: 7, ScopeHash: scopeHash, Mutation: &ArchiveGenesisMutation{ProducerUID: "producer-uid", Sequence: 1, Intent: intent}}
}

func TestArchiveGenesisRefSeparatesStableAndMutationScopes(t *testing.T) {
	scope := ArchiveScope{Provider: "s3", Bucket: "bucket", Prefix: "base/source", ClusterID: "source"}
	ref := archiveGenesisMutationRefForTest(t, scope)
	if err := ref.ValidateForScope(scope); err != nil {
		t.Fatalf("valid ref with distinct stable and mutation scope hashes rejected: %v", err)
	}
	status := ArchiveGenesisStatus{ScopeHash: ref.ScopeHash, MutationScopeHash: ref.Mutation.Intent.StoreScopeHash, ConfigID: ref.ConfigID, Ref: &ref, Mutation: &MutationLookupResult{Found: true, Entry: &MutationEntry{Intent: ref.Mutation.Intent}}}
	if err := status.Validate(ref.Mutation.Intent.StoreScopeHash, ref.ConfigID); err != nil {
		t.Fatalf("valid status rejected: %v", err)
	}
	if err := status.Validate(ref.Mutation.Intent.StoreScopeHash, ref.ConfigID+1); err == nil {
		t.Fatal("status accepted a genesis config ID different from the registered archive identity")
	}
	status.ScopeHash = hashJSON([]string{"different-stable-scope"})
	if err := status.Validate(ref.Mutation.Intent.StoreScopeHash, ref.ConfigID); err == nil {
		t.Fatal("status accepted a reference from a different stable scope")
	}
	status.ScopeHash = ref.ScopeHash
	status.MutationScopeHash = hashJSON([]string{"different-mutation-scope"})
	if err := status.Validate(ref.Mutation.Intent.StoreScopeHash, ref.ConfigID); err == nil {
		t.Fatal("status accepted a mutation scope different from the bound producer")
	}
}

func TestArchiveGenesisForkAcceptsOnlyCanonicalManualMembershipBinding(t *testing.T) {
	scope := ArchiveScope{Provider: "gcs", Bucket: "archive", Prefix: "root/target", ClusterID: "target"}
	scopeHash, err := scope.Hash()
	if err != nil {
		t.Fatal(err)
	}
	digest := hashJSON([]string{"manual-source-cohort"})
	ref := ArchiveGenesisRef{Version: ArchiveGenesisVersion, ConfigID: 1, ScopeHash: scopeHash, Fork: &ArchiveGenesisFork{
		AnchorKey: "recovery/generation-anchor.json", AnchorDigest: digest, IntentDigest: digest, ResultDigest: digest,
		LineageDigest: digest, TargetPrefix: scope.Prefix, MembershipHash: digest,
		ManualMembershipBindingDigest: digest, ConfigID: 1, BaseTip: 1, BasePrefixHash: digest, AnchorHash: digest,
	}}
	if err := ref.ValidateForScope(scope); err != nil {
		t.Fatalf("valid fork with a manual source-cohort binding rejected: %v", err)
	}
	ref.Fork.ManualMembershipBindingDigest = "not-a-digest"
	if err := ref.ValidateForScope(scope); err == nil {
		t.Fatal("fork accepted a malformed manual source-cohort binding")
	}
}

func TestArchiveEmptyRecoveryAuthorizationIsSeparateAndBoundToEmptySource(t *testing.T) {
	digest := hashJSON([]string{"evidence"})
	auth := ArchiveEmptyRecoveryAuthorization{
		Version: ArchiveGenesisVersion, StatefulSetUID: "sts-uid", TargetClusterID: "target",
		OperationID: "resource-uid:recovery-1", SourceFenceOperationID: "recovery-1", ConfigID: 1,
		SourceTip: 0, SourceScopeHash: hashJSON([]string{"source-scope"}), TargetScopeHash: hashJSON([]string{"target-scope"}),
		SourceGenesisRefDigest: digest, SourceHeadDigest: digest, TargetEmptyHeadDigest: digest, SourceHeadVersion: objstore.ObjectVersion{Type: objstore.ETag, Value: "opaque-etag"},
		SourceRootDigest: digest, SourceFenceDigest: digest, SourceMembershipHash: digest, SourceDurability: "async",
		TargetMembershipHash: digest, TargetDurability: "before-ack", AllowDataLoss: true, PolicyDigest: digest,
	}
	if err := auth.Validate(); err != nil {
		t.Fatalf("valid empty recovery authorization rejected: %v", err)
	}
	bad := auth
	bad.SourceTip = 1
	if err := bad.Validate(); err == nil {
		t.Fatal("nonzero source tip accepted for empty recovery")
	}
	bad = auth
	bad.SourceScopeHash = bad.TargetScopeHash
	if err := bad.Validate(); err == nil {
		t.Fatal("same source and target scope accepted")
	}
	bad = auth
	bad.SourceHeadVersion = objstore.ObjectVersion{Type: objstore.ETag}
	if err := bad.Validate(); err == nil {
		t.Fatal("empty object version accepted")
	}
	bad = auth
	bad.AllowDataLoss = false
	if err := bad.Validate(); err == nil {
		t.Fatal("async source authorization accepted without explicit data-loss policy")
	}
	manual := auth
	manual.RecoveryMode = "frozen-source-v1"
	manual.SourcePrefix, manual.BucketName, manual.BucketMetageneration = "root/source", "archive", "1"
	manual.LifecyclePolicyDigest = digest
	manual.ManualMembershipBindingDigest = digest
	if err := manual.Validate(); err != nil {
		t.Fatalf("valid frozen-source manual cohort binding rejected: %v", err)
	}
	badManual := manual
	badManual.ManualMembershipBindingDigest = "not-a-digest"
	if err := badManual.Validate(); err == nil {
		t.Fatal("frozen authorization accepted a malformed manual cohort binding")
	}
	legacyManual := auth
	legacyManual.ManualMembershipBindingDigest = digest
	if err := legacyManual.Validate(); err == nil {
		t.Fatal("legacy authorization accepted a frozen-source manual cohort binding")
	}

	auth.SourceHeadDigest = hashJSON([]string{"sealed-source-head"})
	auth.TargetEmptyHeadDigest = hashJSON([]string{"unsealed-target-empty-head"})
	status := ArchiveGenesisStatus{ScopeHash: auth.TargetScopeHash, MutationScopeHash: digest, ConfigID: 1,
		EmptyRecoveryAuthorization: &auth, EmptyRecoveryState: EmptyRecoveryAuthorized}
	if err := status.Validate(digest, 1); err != nil {
		t.Fatalf("authorization status without mutation ref rejected: %v", err)
	}
	status.EmptyRecoveryState = EmptyRecoveryCommitted
	if err := status.Validate(digest, 1); err == nil {
		t.Fatal("committed empty recovery status without target receipt accepted")
	}
	status.EmptyRecoveryAuthorization = nil
	status.EmptyRecoveryState = EmptyRecoveryAuthorized
	if err := status.Validate(digest, 1); err == nil {
		t.Fatal("empty recovery state detached from authorization accepted")
	}

	ref := ArchiveGenesisRef{Version: ArchiveGenesisVersion, ConfigID: 1, ScopeHash: auth.TargetScopeHash,
		Mutation: &ArchiveGenesisMutation{ProducerUID: "target-producer", Sequence: 1, Intent: MutationIntent{
			ContractVersion: MutationIntentVersion, StoreScopeHash: digest, OperationID: MutationRequestID("target-producer", 1),
			Kind: "upload", Key: "archive/head.bin", PayloadRef: "target-empty", PayloadDigest: auth.TargetEmptyHeadDigest, PayloadSize: 10,
			Condition: MutationCondition{Mode: MutationCreateOnly},
		}}}
	status = ArchiveGenesisStatus{ScopeHash: auth.TargetScopeHash, MutationScopeHash: digest, ConfigID: 1, Ref: &ref,
		Mutation:                   &MutationLookupResult{Found: true, Entry: &MutationEntry{Intent: ref.Mutation.Intent}},
		EmptyRecoveryAuthorization: &auth, EmptyRecoveryState: EmptyRecoveryAuthorized}
	if err := status.Validate(digest, 1); err != nil {
		t.Fatalf("separate sealed source and unsealed target HEAD digests rejected: %v", err)
	}
	ref.Mutation.Intent.PayloadDigest = auth.SourceHeadDigest
	status.Ref = &ref
	status.Mutation = &MutationLookupResult{Found: true, Entry: &MutationEntry{Intent: ref.Mutation.Intent}}
	if err := status.Validate(digest, 1); err == nil {
		t.Fatal("target genesis ref accepted the sealed source HEAD digest instead of target empty HEAD")
	}
}

func TestArchiveGenesisRefRequiresExactHeadCreateOnlyIntentAndConfig(t *testing.T) {
	scope := ArchiveScope{Provider: "gcs", Bucket: "bucket", Prefix: "base/source", ClusterID: "source"}
	ref := archiveGenesisMutationRefForTest(t, scope)
	if err := ref.ValidateForScope(scope); err != nil {
		t.Fatal(err)
	}
	badKey := ref
	mutation := *ref.Mutation
	mutation.Intent = ref.Mutation.Intent
	mutation.Intent.Key = "archive/other.bin"
	badKey.Mutation = &mutation
	if err := badKey.ValidateForScope(scope); err == nil {
		t.Fatal("accepted genesis mutation for a different object key")
	}
	badCondition := ref
	mutation = *ref.Mutation
	mutation.Intent = ref.Mutation.Intent
	mutation.Intent.Condition.Generation = 1
	badCondition.Mutation = &mutation
	if err := badCondition.ValidateForScope(scope); err == nil {
		t.Fatal("accepted create-only intent with a generation")
	}
	badConfig := ref
	badConfig.ConfigID = 0
	if err := badConfig.ValidateForScope(scope); err == nil {
		t.Fatal("accepted genesis without an immutable archive config ID")
	}
	wrongPrefix := scope
	wrongPrefix.Prefix = "other/source"
	if err := ref.ValidateForScope(wrongPrefix); err == nil {
		t.Fatal("accepted genesis reference for a different complete archive prefix")
	}
}

func TestArchiveGenesisEmptyStatusDoesNotInventConfigID(t *testing.T) {
	status := ArchiveGenesisStatus{ScopeHash: hashJSON([]string{"scope"}), MutationScopeHash: hashJSON([]string{"mutation"}), ConfigID: 1, CanInitializeEmpty: true}
	if err := status.Validate(status.MutationScopeHash, 1); err != nil {
		t.Fatal(err)
	}
	status.ConfigID = 2
	if err := status.Validate(status.MutationScopeHash, 1); err == nil {
		t.Fatal("empty status accepted a config ID different from the registration claim")
	}
}
