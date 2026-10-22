package fencing

import (
	"testing"
)

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
