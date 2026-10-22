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
	"os"
	"path"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"google.golang.org/api/googleapi"
)

// gcsMutationProvider is the first executable source profile: JSON API,
// single-request uploads only. The caller owns cross-producer key serialization.
type gcsMutationProvider struct {
	client  *storage.Client
	scope   AttestationStoreScope
	profile string
}

func newGCSMutationProvider(client *storage.Client, scope AttestationStoreScope) (*gcsMutationProvider, error) {
	return newGCSMutationProviderForProfile(client, scope, fencing.MutationExecutionProfile)
}

// newGCSHeldCreateMutationProvider opts a producer into the separately
// versioned held-create profile. The legacy constructor remains v1-only.
func newGCSHeldCreateMutationProvider(client *storage.Client, scope AttestationStoreScope) (*gcsMutationProvider, error) {
	return newGCSMutationProviderForProfile(client, scope, fencing.MutationHeldExecutionProfile)
}

func newGCSMutationProviderForProfile(client *storage.Client, scope AttestationStoreScope, profile string) (*gcsMutationProvider, error) {
	if os.Getenv("STORAGE_EMULATOR_HOST") != "" {
		return nil, fmt.Errorf("fencing: independent GCS mutation profile rejects STORAGE_EMULATOR_HOST")
	}
	if client == nil || (profile != fencing.MutationExecutionProfile && profile != fencing.MutationHeldExecutionProfile) || scope.Provider != "gcs" || scope.Bucket == "" || scope.Prefix == "" || path.Clean(scope.Prefix) != scope.Prefix || scope.Prefix == "." || strings.HasPrefix(scope.Prefix, "/") || scope.Endpoint != "" || scope.Account != "" || scope.ConnectionStringDigest != "" {
		return nil, fmt.Errorf("fencing: GCS mutation provider requires an exact bucket scope")
	}
	return &gcsMutationProvider{client: client, scope: scope, profile: profile}, nil
}

func newGCSMutationProviderForRun(client *storage.Client, scope AttestationStoreScope, run fencing.Run) (*gcsMutationProvider, error) {
	return newGCSMutationProviderForProfile(client, scope, run.MutationProfile)
}

func (p *gcsMutationProvider) object(key string) *storage.ObjectHandle {
	if key == "" || key == "." || key == ".." || strings.HasPrefix(key, "/") || strings.HasPrefix(key, "../") || path.Clean(key) != key {
		return nil
	}
	prefix := path.Clean(p.scope.Prefix)
	name := path.Join(prefix, key)
	if key == "" || strings.HasPrefix(key, "/") || name == prefix || !strings.HasPrefix(name, prefix+"/") {
		return nil
	}
	return p.client.Bucket(p.scope.Bucket).Object(name)
}

func (p *gcsMutationProvider) observe(ctx context.Context, key string) (int64, bool, error) {
	object := p.object(key)
	if object == nil {
		return 0, false, fmt.Errorf("fencing: GCS object key escapes the enrolled prefix")
	}
	attrs, err := object.Retryer(storage.WithPolicy(storage.RetryNever)).Attrs(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if attrs.Generation <= 0 {
		return 0, false, fmt.Errorf("fencing: GCS returned an invalid object generation")
	}
	return attrs.Generation, true, nil
}

func (p *gcsMutationProvider) upload(ctx context.Context, key string, generation int64, intent fencing.MutationIntent, payload io.Reader) (mutationProviderResult, error) {
	if p.profile == fencing.MutationHeldExecutionProfile && generation == 0 {
		preparation := fencing.MutationPreparation{Profile: p.profile, StoreScopeHash: intent.StoreScopeHash, GenerationSet: true, Generation: generation}
		return p.uploadHeldCreate(ctx, key, intent, preparation, payload)
	}
	if (p.profile != fencing.MutationExecutionProfile && p.profile != fencing.MutationHeldExecutionProfile) || generation < 0 || intent.Kind != "upload" || payload == nil || !p.matchesIntentScope(intent) {
		return mutationProviderResult{}, fmt.Errorf("fencing: invalid GCS upload descriptor")
	}
	conditions := storage.Conditions{GenerationMatch: generation}
	if generation == 0 {
		conditions = storage.Conditions{DoesNotExist: true}
	}
	object := p.object(key)
	if object == nil {
		return mutationProviderResult{}, fmt.Errorf("fencing: GCS object key escapes the enrolled prefix")
	}
	writer := object.Retryer(storage.WithPolicy(storage.RetryNever)).If(conditions).NewWriter(ctx)
	writer.ChunkSize = 0 // No resumable session or hidden chunk replay in this profile.
	writer.Metadata = map[string]string{"rhiza-operation-id": intent.OperationID, "rhiza-payload-sha256": intent.PayloadDigest}
	if _, err := io.Copy(writer, payload); err != nil {
		_ = writer.CloseWithError(err)
		if gcsConditionFailed(err) {
			return mutationProviderResult{Outcome: fencing.MutationRejected, ConditionFailed: true, Generation: generation, AllCopiesTerminal: true}, nil
		}
		return mutationProviderResult{}, err
	}
	if err := writer.Close(); err != nil {
		if gcsConditionFailed(err) {
			return mutationProviderResult{Outcome: fencing.MutationRejected, ConditionFailed: true, Generation: generation, AllCopiesTerminal: true}, nil
		}
		return mutationProviderResult{}, err
	}
	attrs := writer.Attrs()
	if attrs == nil || attrs.Generation <= 0 || attrs.Size != intent.PayloadSize || attrs.Metadata["rhiza-operation-id"] != intent.OperationID || attrs.Metadata["rhiza-payload-sha256"] != intent.PayloadDigest {
		return mutationProviderResult{}, fmt.Errorf("fencing: GCS committed object metadata differs from original mutation")
	}
	// This profile uses one non-resumable JSON API request with RetryNever. The
	// successful response and exact returned generation/metadata therefore close
	// its only physical permission; a stale replay is also blocked by the same
	// generation precondition.
	return mutationProviderResult{Outcome: fencing.MutationCommitted, Generation: attrs.Generation, Metageneration: attrs.Metageneration, PayloadDigest: intent.PayloadDigest, AllCopiesTerminal: true}, nil
}

// uploadHeldCreate is the only initial-dispatch path for the held-v2 profile.
// The hold is part of the create request itself, so no interval exists in which
// a committed generation-zero object is visible without the late-effect guard.
// The caller must keep the hold until its exact terminal receipt is durable.
func (p *gcsMutationProvider) uploadHeldCreate(ctx context.Context, key string, intent fencing.MutationIntent, preparation fencing.MutationPreparation, payload io.Reader) (mutationProviderResult, error) {
	if p.profile != fencing.MutationHeldExecutionProfile || !fencing.IsHeldCreateEligible(intent, preparation) || intent.PayloadSize < 0 || !mutationDigestValidOperator(intent.PayloadDigest) || payload == nil || !p.matchesIntentScope(intent) {
		return mutationProviderResult{}, fmt.Errorf("fencing: invalid held GCS create descriptor")
	}
	object := p.object(key)
	if object == nil {
		return mutationProviderResult{}, fmt.Errorf("fencing: GCS object key escapes the enrolled prefix")
	}
	writer := object.Retryer(storage.WithPolicy(storage.RetryNever)).If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	writer.ChunkSize = 0
	writer.TemporaryHold = true
	writer.Metadata = map[string]string{"rhiza-operation-id": intent.OperationID, "rhiza-payload-sha256": intent.PayloadDigest}
	if _, err := io.Copy(writer, payload); err != nil {
		_ = writer.CloseWithError(err)
		return mutationProviderResult{}, err
	}
	if err := writer.Close(); err != nil {
		if gcsConditionFailed(err) {
			return mutationProviderResult{Outcome: fencing.MutationRejected, ConditionFailed: true, Generation: 0, AllCopiesTerminal: true}, nil
		}
		return mutationProviderResult{}, err
	}
	attrs := writer.Attrs()
	if attrs == nil || attrs.Generation <= 0 {
		return mutationProviderResult{}, fmt.Errorf("fencing: held GCS create returned no generation")
	}
	if err := p.verifyHeldCreateGeneration(ctx, key, attrs.Generation, intent); err != nil {
		return mutationProviderResult{}, err
	}
	// A successful response to the single RetryNever JSON upload closes this
	// dispatch. The hold remains until the caller persists the receipt and then
	// invokes finalizeHeldCreate; it is not cleared on this path.
	return mutationProviderResult{Outcome: fencing.MutationCommitted, Generation: attrs.Generation, Metageneration: attrs.Metageneration,
		PayloadDigest: intent.PayloadDigest, AllCopiesTerminal: true}, nil
}

// heldCreateAuthorization is assembled only after the canonical per-key guard
// has been read back from protected authority state. The provider does not
// create or infer that authorization itself.
type heldCreateAuthorization struct {
	Runtime           fencing.RuntimeBinding
	WALIdentity       string
	ProducerUID       string
	Sequence          uint64
	PreparationHash   string
	DispatchID        string
	GuardRootIdentity string
	GuardRef          string
	GuardDigest       string
}

// settleHeldCreate replays exactly the admitted create-only payload after the
// canonical guard is committed, then proves the winning held generation's
// bytes. A 412 is useful only after the same exact-generation byte check; an
// arbitrary pre-existing object or matching user metadata is never proof.
func (p *gcsMutationProvider) settleHeldCreate(ctx context.Context, key string, intent fencing.MutationIntent, preparation fencing.MutationPreparation, payload io.Reader, authorization heldCreateAuthorization) (fencing.MutationGuardedLogicalResolution, error) {
	if !fencing.IsHeldCreateEligible(intent, preparation) || intent.PayloadSize < 0 || !mutationDigestValidOperator(intent.PayloadDigest) ||
		p.profile != fencing.MutationHeldExecutionProfile || !p.matchesIntentScope(intent) ||
		preparation.Profile != fencing.MutationHeldExecutionProfile || !preparation.GenerationSet || preparation.Generation != 0 || preparation.StoreScopeHash != intent.StoreScopeHash || payload == nil ||
		authorization.Runtime.SourceClusterID == "" || authorization.Runtime.NodeID == "" || authorization.Runtime.PodUID == "" || authorization.Runtime.RunID == "" || authorization.Runtime.StatefulSetUID == "" ||
		authorization.WALIdentity == "" || authorization.ProducerUID == "" || authorization.Sequence == 0 || authorization.DispatchID == "" || !mutationDigestValidOperator(authorization.PreparationHash) ||
		authorization.GuardRootIdentity == "" || !mutationDigestValidOperator(authorization.GuardRootIdentity) || authorization.GuardRef == "" || len(authorization.GuardRef) > 512 ||
		authorization.GuardRef == "." || strings.HasPrefix(authorization.GuardRef, "/") || path.Clean(authorization.GuardRef) != authorization.GuardRef || strings.HasPrefix(authorization.GuardRef, "../") || !mutationDigestValidOperator(authorization.GuardDigest) {
		return fencing.MutationGuardedLogicalResolution{}, fmt.Errorf("fencing: invalid guarded held-create settlement descriptor")
	}
	object := p.object(key)
	if object == nil {
		return fencing.MutationGuardedLogicalResolution{}, fmt.Errorf("fencing: GCS object key escapes the enrolled prefix")
	}
	writer := object.Retryer(storage.WithPolicy(storage.RetryNever)).If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	writer.ChunkSize = 0
	writer.TemporaryHold = true
	writer.Metadata = map[string]string{"rhiza-operation-id": intent.OperationID, "rhiza-payload-sha256": intent.PayloadDigest}
	if _, err := io.Copy(writer, payload); err != nil {
		_ = writer.CloseWithError(err)
		return fencing.MutationGuardedLogicalResolution{}, err
	}
	if err := writer.Close(); err != nil && !gcsConditionFailed(err) {
		return fencing.MutationGuardedLogicalResolution{}, err
	}
	attrs, err := object.Retryer(storage.WithPolicy(storage.RetryNever)).Attrs(ctx)
	if err != nil {
		return fencing.MutationGuardedLogicalResolution{}, err
	}
	if attrs.Generation <= 0 {
		return fencing.MutationGuardedLogicalResolution{}, fmt.Errorf("fencing: guarded held create has no current generation")
	}
	if err := p.verifyHeldCreateGeneration(ctx, key, attrs.Generation, intent); err != nil {
		return fencing.MutationGuardedLogicalResolution{}, err
	}
	return fencing.MutationGuardedLogicalResolution{
		Version: fencing.MutationGuardedLogicalResolutionVersion, Runtime: authorization.Runtime, WALIdentity: authorization.WALIdentity,
		ProducerUID: authorization.ProducerUID, Sequence: authorization.Sequence, Intent: intent, Preparation: preparation,
		PreparationHash: authorization.PreparationHash, DispatchID: authorization.DispatchID, Profile: fencing.MutationHeldExecutionProfile,
		GuardRootIdentity: authorization.GuardRootIdentity, GuardVersion: fencing.MutationGuardedLogicalResolutionVersion,
		GuardRef: authorization.GuardRef, GuardDigest: authorization.GuardDigest,
		ObjectGeneration: attrs.Generation, ObjectMetageneration: attrs.Metageneration, ObjectSize: attrs.Size, PayloadDigest: intent.PayloadDigest,
	}, nil
}

// finalizeHeldCreate clears a hold only for a self-consistent, terminal
// successful receipt supplied after canonical receipt/journal persistence.
// The exact generation and metageneration preconditions prevent it from
// touching a replacement; readback makes a lost response safely retryable.
func (p *gcsMutationProvider) finalizeHeldCreate(ctx context.Context, key string, intent fencing.MutationIntent, receipt mutationExecutionReceipt) error {
	if p.profile != fencing.MutationHeldExecutionProfile || !p.matchesIntentScope(intent) || !fencing.IsHeldCreateEligible(intent, receipt.Preparation) || receipt.Intent != intent || receipt.Outcome != fencing.MutationCommitted ||
		!receipt.AllCopiesTerminal || receipt.Generation <= 0 || receipt.Metageneration <= 0 || receipt.PayloadDigest != intent.PayloadDigest ||
		receipt.ReceiptDigest == "" || receipt.ReceiptDigest != receipt.digest() || receipt.Preparation.Profile != fencing.MutationHeldExecutionProfile || receipt.Preparation.Generation != 0 {
		return fmt.Errorf("fencing: held GCS create lacks a terminal canonical receipt")
	}
	object := p.object(key)
	if object == nil {
		return fmt.Errorf("fencing: GCS object key escapes the enrolled prefix")
	}
	target := object.Generation(receipt.Generation).Retryer(storage.WithPolicy(storage.RetryNever))
	attrs, err := target.Attrs(ctx)
	if err != nil {
		return err
	}
	if attrs.Generation != receipt.Generation || attrs.Metageneration < receipt.Metageneration || attrs.Metadata["rhiza-operation-id"] != intent.OperationID || attrs.Metadata["rhiza-payload-sha256"] != intent.PayloadDigest || attrs.Size != intent.PayloadSize {
		return fmt.Errorf("fencing: held GCS create no longer matches its terminal receipt")
	}
	if !attrs.TemporaryHold {
		if attrs.Metageneration != receipt.Metageneration+1 {
			return fmt.Errorf("fencing: held GCS create was modified outside terminal finalization")
		}
		return p.verifyHeldCreateBytes(ctx, target, intent)
	}
	if attrs.Metageneration != receipt.Metageneration {
		return fmt.Errorf("fencing: held GCS create metadata changed before terminal finalization")
	}
	if err := p.verifyHeldCreateBytes(ctx, target, intent); err != nil {
		return err
	}
	_, updateErr := target.If(storage.Conditions{GenerationMatch: receipt.Generation, MetagenerationMatch: attrs.Metageneration}).Update(ctx, storage.ObjectAttrsToUpdate{TemporaryHold: false})
	if updateErr == nil {
		return nil
	}
	// The update may have committed while its response was lost. Only the same
	// exact generation with the hold now absent closes this retry.
	confirmed, readErr := target.Attrs(ctx)
	if readErr == nil && confirmed.Generation == receipt.Generation && confirmed.Metageneration == receipt.Metageneration+1 && !confirmed.TemporaryHold &&
		confirmed.Metadata["rhiza-operation-id"] == intent.OperationID && confirmed.Metadata["rhiza-payload-sha256"] == intent.PayloadDigest && confirmed.Size == intent.PayloadSize {
		return p.verifyHeldCreateBytes(ctx, target, intent)
	}
	return errors.Join(updateErr, readErr)
}

func (p *gcsMutationProvider) verifyHeldCreateGeneration(ctx context.Context, key string, generation int64, intent fencing.MutationIntent) error {
	if generation <= 0 || intent.PayloadSize < 0 {
		return fmt.Errorf("fencing: invalid held generation verification descriptor")
	}
	object := p.object(key)
	if object == nil {
		return fmt.Errorf("fencing: GCS object key escapes the enrolled prefix")
	}
	target := object.Generation(generation).Retryer(storage.WithPolicy(storage.RetryNever))
	attrs, err := target.Attrs(ctx)
	if err != nil {
		return err
	}
	if attrs.Generation != generation || attrs.Metageneration <= 0 || !attrs.TemporaryHold || attrs.Size != intent.PayloadSize ||
		attrs.Metadata["rhiza-operation-id"] != intent.OperationID || attrs.Metadata["rhiza-payload-sha256"] != intent.PayloadDigest {
		return fmt.Errorf("fencing: GCS generation is not the exact held mutation object")
	}
	return p.verifyHeldCreateBytes(ctx, target, intent)
}

func (p *gcsMutationProvider) verifyHeldCreateBytes(ctx context.Context, target *storage.ObjectHandle, intent fencing.MutationIntent) error {
	if intent.PayloadSize < 0 || intent.PayloadSize == int64(^uint64(0)>>1) {
		return fmt.Errorf("fencing: invalid held payload size")
	}
	reader, err := target.NewReader(ctx)
	if err != nil {
		return err
	}
	hash := sha256.New()
	n, copyErr := io.CopyN(hash, reader, intent.PayloadSize+1)
	closeErr := reader.Close()
	if copyErr != nil && !errors.Is(copyErr, io.EOF) {
		return errors.Join(copyErr, closeErr)
	}
	if closeErr != nil {
		return closeErr
	}
	if n != intent.PayloadSize || hex.EncodeToString(hash.Sum(nil)) != intent.PayloadDigest {
		return fmt.Errorf("fencing: held GCS generation bytes differ from the admitted payload")
	}
	return nil
}

type heldCreateGuardRecord struct {
	Version         int                         `json:"version"`
	RootIdentity    string                      `json:"rootIdentity"`
	Runtime         fencing.RuntimeBinding      `json:"runtime"`
	WALIdentity     string                      `json:"walIdentity"`
	ProducerUID     string                      `json:"producerUID"`
	Sequence        uint64                      `json:"sequence"`
	Intent          fencing.MutationIntent      `json:"intent"`
	Preparation     fencing.MutationPreparation `json:"preparation"`
	PreparationHash string                      `json:"preparationHash"`
	DispatchID      string                      `json:"dispatchID"`
}

func (p *gcsMutationProvider) replayGuardedCreate(ctx context.Context, evidenceStore *AuthorityEvidenceStore, request mutationGuardedReplayRequest, payload io.Reader) (fencing.MutationGuardedLogicalResolution, error) {
	if p.profile != fencing.MutationHeldExecutionProfile || evidenceStore == nil || evidenceStore.Identity() != request.GuardRootIdentity || !p.matchesIntentScope(request.Intent) {
		return fencing.MutationGuardedLogicalResolution{}, fmt.Errorf("fencing: held-create replay is outside its registered authority scope")
	}
	_, recordBytes, guardRef, guardDigest, err := heldCreateGuard(request)
	if err != nil {
		return fencing.MutationGuardedLogicalResolution{}, err
	}
	if _, err := evidenceStore.CreateImmutable(ctx, guardRef, recordBytes); err != nil {
		return fencing.MutationGuardedLogicalResolution{}, err
	}
	if err := verifyHeldCreateGuardReadback(ctx, evidenceStore, guardRef, recordBytes); err != nil {
		return fencing.MutationGuardedLogicalResolution{}, err
	}
	authorization := heldCreateAuthorization{Runtime: request.Runtime, WALIdentity: request.WALIdentity, ProducerUID: request.ProducerUID,
		Sequence: request.Sequence, PreparationHash: request.PreparationHash, DispatchID: request.DispatchID,
		GuardRootIdentity: request.GuardRootIdentity, GuardRef: guardRef, GuardDigest: guardDigest}
	return p.settleHeldCreate(ctx, request.Intent.Key, request.Intent, request.Preparation, payload, authorization)
}

func (p *gcsMutationProvider) verifyGuardedCreate(ctx context.Context, evidenceStore *AuthorityEvidenceStore, request mutationGuardedReplayRequest, evidence fencing.MutationGuardedLogicalResolution) error {
	if p.profile != fencing.MutationHeldExecutionProfile || evidenceStore == nil || evidenceStore.Identity() != request.GuardRootIdentity || !p.matchesIntentScope(request.Intent) {
		return fmt.Errorf("fencing: guarded held-create proof is outside its registered authority scope")
	}
	_, recordBytes, guardRef, guardDigest, err := heldCreateGuard(request)
	if err != nil {
		return err
	}
	if evidence.Version != fencing.MutationGuardedLogicalResolutionVersion || evidence.Runtime != request.Runtime || evidence.WALIdentity != request.WALIdentity ||
		evidence.ProducerUID != request.ProducerUID || evidence.Sequence != request.Sequence || evidence.Intent != request.Intent || evidence.Preparation != request.Preparation ||
		evidence.PreparationHash != request.PreparationHash || evidence.DispatchID != request.DispatchID || evidence.Profile != fencing.MutationHeldExecutionProfile ||
		evidence.GuardRootIdentity != request.GuardRootIdentity || evidence.GuardVersion != fencing.MutationGuardedLogicalResolutionVersion ||
		evidence.GuardRef != guardRef || evidence.GuardDigest != guardDigest || evidence.ObjectGeneration <= 0 || evidence.ObjectMetageneration <= 0 ||
		evidence.ObjectSize != request.Intent.PayloadSize || evidence.PayloadDigest != request.Intent.PayloadDigest {
		return fmt.Errorf("fencing: guarded held-create proof differs from the admitted tuple")
	}
	if err := verifyHeldCreateGuardReadback(ctx, evidenceStore, guardRef, recordBytes); err != nil {
		return err
	}
	if err := p.verifyHeldCreateGeneration(ctx, request.Intent.Key, evidence.ObjectGeneration, request.Intent); err != nil {
		return err
	}
	attrs, err := p.object(request.Intent.Key).Generation(evidence.ObjectGeneration).Retryer(storage.WithPolicy(storage.RetryNever)).Attrs(ctx)
	if err != nil {
		return err
	}
	if attrs.Metageneration != evidence.ObjectMetageneration || attrs.Size != evidence.ObjectSize {
		return fmt.Errorf("fencing: guarded held-create generation metadata changed")
	}
	return nil
}

func heldCreateGuard(request mutationGuardedReplayRequest) (heldCreateGuardRecord, []byte, string, string, error) {
	if request.Runtime.SourceClusterID == "" || request.Runtime.NodeID == "" || request.Runtime.PodUID == "" || request.Runtime.RunID == "" || request.Runtime.StatefulSetUID == "" ||
		request.WALIdentity == "" || request.ProducerUID == "" || request.Sequence == 0 || request.DispatchID == "" || request.Intent.Kind != "upload" ||
		!fencing.IsHeldCreateEligible(request.Intent, request.Preparation) || request.Intent.StoreScopeHash == "" ||
		!mutationDigestValidOperator(request.PreparationHash) || request.GuardRootIdentity == "" || !mutationDigestValidOperator(request.GuardRootIdentity) {
		return heldCreateGuardRecord{}, nil, "", "", fmt.Errorf("fencing: incomplete guarded held-create request")
	}
	record := heldCreateGuardRecord{Version: fencing.MutationGuardedLogicalResolutionVersion, RootIdentity: request.GuardRootIdentity,
		Runtime: request.Runtime, WALIdentity: request.WALIdentity, ProducerUID: request.ProducerUID, Sequence: request.Sequence,
		Intent: request.Intent, Preparation: request.Preparation, PreparationHash: request.PreparationHash, DispatchID: request.DispatchID}
	body, err := json.Marshal(record)
	if err != nil {
		return heldCreateGuardRecord{}, nil, "", "", err
	}
	guardDigest := authorityStateDigest(body)
	keyDigest := hashJSON([]string{"rhiza-held-create-guard-v1", request.Intent.StoreScopeHash, request.Intent.Key})
	guardRef := path.Join("mutation-guards", keyDigest+".json")
	return record, body, guardRef, guardDigest, nil
}

func verifyHeldCreateGuardReadback(ctx context.Context, store *AuthorityEvidenceStore, key string, expected []byte) error {
	reader, _, err := store.Read(ctx, key)
	if err != nil {
		return err
	}
	actual, readErr := io.ReadAll(io.LimitReader(reader, int64(len(expected))+1))
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(actual, expected) {
		return errors.Join(readErr, closeErr, fmt.Errorf("fencing: immutable held-create guard differs"))
	}
	return nil
}

func mutationDigestValidOperator(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == strings.ToLower(value)
}

func gcsConditionFailed(err error) bool {
	var apiErr *googleapi.Error
	return errors.As(err, &apiErr) && apiErr.Code == 412
}

func (p *gcsMutationProvider) delete(ctx context.Context, key string, generation int64) (mutationProviderResult, error) {
	if (p.profile != fencing.MutationExecutionProfile && p.profile != fencing.MutationHeldExecutionProfile) || generation <= 0 {
		return mutationProviderResult{}, fmt.Errorf("fencing: GCS deletion requires a fixed nonzero generation")
	}
	object := p.object(key)
	if object == nil {
		return mutationProviderResult{}, fmt.Errorf("fencing: GCS object key escapes the enrolled prefix")
	}
	target := object.Generation(generation).Retryer(storage.WithPolicy(storage.RetryNever))
	if err := target.If(storage.Conditions{GenerationMatch: generation}).Delete(ctx); err != nil {
		if gcsConditionFailed(err) {
			return mutationProviderResult{Outcome: fencing.MutationRejected, ConditionFailed: true, Generation: generation, AllCopiesTerminal: true}, nil
		}
		if errors.Is(err, storage.ErrObjectNotExist) {
			return mutationProviderResult{Outcome: fencing.MutationExcluded, Generation: generation}, nil
		}
		// A lost or conditional response is not a commit. Absence of the exact
		// selected generation proves only that any delayed fixed-generation
		// delete cannot affect a replacement generation.
		if _, attrsErr := target.Attrs(ctx); errors.Is(attrsErr, storage.ErrObjectNotExist) {
			return mutationProviderResult{Outcome: fencing.MutationExcluded, Generation: generation}, nil
		}
		return mutationProviderResult{}, err
	}
	return mutationProviderResult{Outcome: fencing.MutationCommitted, Generation: generation, AllCopiesTerminal: true}, nil
}

func (p *gcsMutationProvider) committed(ctx context.Context, key string, intent fencing.MutationIntent, preparation fencing.MutationPreparation) (mutationProviderResult, bool, error) {
	if (p.profile != fencing.MutationExecutionProfile && p.profile != fencing.MutationHeldExecutionProfile) || !p.matchesIntentScope(intent) ||
		(p.profile == fencing.MutationHeldExecutionProfile && intent.Kind == "upload" && preparation.Generation == 0) {
		return mutationProviderResult{}, false, fmt.Errorf("fencing: held GCS profile requires its guarded settlement path")
	}
	object := p.object(key)
	if object == nil {
		return mutationProviderResult{}, false, fmt.Errorf("fencing: GCS object key escapes the enrolled prefix")
	}
	if intent.Kind == "delete" {
		if preparation.Generation <= 0 {
			return mutationProviderResult{}, false, fmt.Errorf("fencing: uncertain GCS deletion lacks its selected generation")
		}
		_, err := object.Generation(preparation.Generation).Retryer(storage.WithPolicy(storage.RetryNever)).Attrs(ctx)
		if errors.Is(err, storage.ErrObjectNotExist) {
			return mutationProviderResult{Outcome: fencing.MutationExcluded, Generation: preparation.Generation}, true, nil
		}
		if err != nil {
			return mutationProviderResult{}, false, err
		}
		return mutationProviderResult{}, false, nil
	}
	if intent.Kind != "upload" {
		return mutationProviderResult{}, false, nil
	}
	attrs, err := object.Retryer(storage.WithPolicy(storage.RetryNever)).Attrs(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return mutationProviderResult{}, false, nil
	}
	if err != nil {
		return mutationProviderResult{}, false, err
	}
	if attrs.Generation <= 0 || attrs.Size != intent.PayloadSize || attrs.Metadata["rhiza-operation-id"] != intent.OperationID || attrs.Metadata["rhiza-payload-sha256"] != intent.PayloadDigest ||
		(preparation.Generation == 0 && attrs.Generation <= 0) || (preparation.Generation > 0 && attrs.Generation <= preparation.Generation) {
		return mutationProviderResult{}, false, nil
	}
	return mutationProviderResult{Outcome: fencing.MutationCommitted, Generation: attrs.Generation, Metageneration: attrs.Metageneration, PayloadDigest: intent.PayloadDigest, AllCopiesTerminal: true}, true, nil
}

func (p *gcsMutationProvider) matchesIntentScope(intent fencing.MutationIntent) bool {
	return p != nil && intent.StoreScopeHash != "" && hashJSON(p.scope) == intent.StoreScopeHash
}
