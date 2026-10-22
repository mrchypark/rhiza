package operator

import (
	"context"
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
	client *storage.Client
	scope  AttestationStoreScope
}

func newGCSMutationProvider(client *storage.Client, scope AttestationStoreScope) (*gcsMutationProvider, error) {
	if os.Getenv("STORAGE_EMULATOR_HOST") != "" {
		return nil, fmt.Errorf("fencing: independent GCS mutation profile rejects STORAGE_EMULATOR_HOST")
	}
	if client == nil || scope.Provider != "gcs" || scope.Bucket == "" || scope.Prefix == "" || path.Clean(scope.Prefix) != scope.Prefix || scope.Prefix == "." || strings.HasPrefix(scope.Prefix, "/") || scope.Endpoint != "" || scope.Account != "" || scope.ConnectionStringDigest != "" {
		return nil, fmt.Errorf("fencing: GCS mutation provider requires an exact bucket scope")
	}
	return &gcsMutationProvider{client: client, scope: scope}, nil
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
	if generation < 0 || intent.Kind != "upload" || payload == nil {
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

func gcsConditionFailed(err error) bool {
	var apiErr *googleapi.Error
	return errors.As(err, &apiErr) && apiErr.Code == 412
}

func (p *gcsMutationProvider) delete(ctx context.Context, key string, generation int64) (mutationProviderResult, error) {
	if generation <= 0 {
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
