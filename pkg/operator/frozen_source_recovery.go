package operator

import (
	"context"
	"fmt"
	"path"
	"strings"

	internalobjstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

func (c *Controller) frozenSourceIdentity(r *Resource, source sourceState) (recovery.FrozenSourceIdentity, error) {
	identity := StoreIdentity(source.env)
	basePrefix := strings.Trim(identity["RHIZA_OBJSTORE_PREFIX"], "/")
	sourcePrefix := path.Join(basePrefix, r.Spec.SourceClusterID)
	if basePrefix == "." || sourcePrefix != source.prefix || identity["RHIZA_OBJSTORE_PROVIDER"] != "gcs" || identity["RHIZA_OBJSTORE_BUCKET"] == "" {
		return recovery.FrozenSourceIdentity{}, fmt.Errorf("fencing: frozen recovery requires the exact configured GCS source scope")
	}
	scope := fencing.ArchiveScope{Provider: "gcs", Bucket: identity["RHIZA_OBJSTORE_BUCKET"], Prefix: sourcePrefix, ClusterID: r.Spec.SourceClusterID}
	scopeHash, err := scope.Hash()
	if err != nil {
		return recovery.FrozenSourceIdentity{}, err
	}
	return recovery.FrozenSourceIdentity{ScopeHash: scopeHash, ClusterID: r.Spec.SourceClusterID, StatefulSetUID: r.Status.StatefulSetUID}, nil
}

func (c *Controller) frozenSourcePreflight(ctx context.Context, r *Resource) (FrozenSourcePreflightResponse, error) {
	preflight, ok := c.Fencer.(FrozenSourcePreflighter)
	if !ok || preflight == nil {
		return FrozenSourcePreflightResponse{}, fmt.Errorf("fencing: authenticated frozen-source preflight is unavailable")
	}
	uid := str(r.Metadata["uid"])
	if resourceName(r) == "" || uid == "" || r.Status.StatefulSetUID == "" || r.Status.RecoveryMode != RecoveryModeFrozenSource {
		return FrozenSourcePreflightResponse{}, fmt.Errorf("fencing: frozen-source recovery identity is not persisted")
	}
	response, err := preflight.FrozenSourcePreflight(ctx, FrozenSourcePreflightRequest{RecoveryName: resourceName(r), RecoveryUID: uid})
	if err != nil {
		return FrozenSourcePreflightResponse{}, err
	}
	if response.Identity.StatefulSetUID != r.Status.StatefulSetUID || response.Identity.ClusterID != r.Spec.SourceClusterID {
		return FrozenSourcePreflightResponse{}, fmt.Errorf("fencing: canonical frozen-source identity differs from the persisted recovery")
	}
	return response, nil
}

func (c *Controller) frozenSourceClosure(ctx context.Context, r *Resource, source sourceState) (recovery.FrozenSourceIdentity, string, string, error) {
	response, err := c.frozenSourcePreflight(ctx, r)
	if err != nil {
		return recovery.FrozenSourceIdentity{}, "", "", err
	}
	if !response.ClosureReady {
		return response.Identity, "", "", ErrFencePending
	}
	if r.Status.FrozenEmptyGenesisPending && !response.EmptyGenesisEligible {
		return response.Identity, "", "", fmt.Errorf("fencing: canonical empty genesis eligibility changed after closure")
	}
	identity, err := c.frozenSourceIdentity(r, source)
	if err != nil || identity != response.Identity {
		return response.Identity, "", "", fmt.Errorf("fencing: configured archive scope differs from canonical frozen-source identity")
	}
	return response.Identity, response.ProofDigest, response.ClosedRootDigest, nil
}

func (c *Controller) validatePendingFrozenEmptyGenesis(ctx context.Context, r *Resource, source sourceState, closedRootDigest string) error {
	response, err := c.frozenSourcePreflight(ctx, r)
	if err != nil {
		return err
	}
	identity, err := c.frozenSourceIdentity(r, source)
	if err != nil || identity != response.Identity {
		return fmt.Errorf("fencing: configured archive scope differs from canonical frozen-source identity")
	}
	if !response.EmptyGenesisEligible {
		return fmt.Errorf("fencing: canonical source root has no exact eligible empty genesis")
	}
	if closedRootDigest != "" && (!response.ClosureReady || response.ClosedRootDigest != closedRootDigest) {
		return fmt.Errorf("fencing: canonical closed root changed after its independent proof")
	}
	return nil
}

func (c *Controller) frozenTargetFor(r *Resource, source sourceState) (string, error) {
	identity, err := c.frozenSourceIdentity(r, source)
	if err != nil {
		return "", err
	}
	return recovery.FrozenTargetID(identity)
}

func (c *Controller) hasSuccessorReservation(ctx context.Context, sourcePrefix string) (bool, error) {
	return c.Bucket.Exists(ctx, path.Join(sourcePrefix, "recovery/successor.json"))
}

func (c *Controller) captureFrozenBucketEnvironment(ctx context.Context, bucketName, sourcePrefix string) (recovery.FrozenBucketEnvironment, error) {
	reader, ok := c.Bucket.(internalobjstore.BucketAttrsReader)
	if !ok {
		return recovery.FrozenBucketEnvironment{}, fmt.Errorf("frozen source: configured bucket cannot read lifecycle metadata")
	}
	return captureFrozenBucketEnvironment(ctx, reader, bucketName, sourcePrefix)
}

func (c *Controller) frozenBucketEnvironmentVerifier(bucketName, sourcePrefix string) recovery.FrozenBucketEnvironmentVerifier {
	return func(ctx context.Context, expected recovery.FrozenBucketEnvironment) error {
		reader, ok := c.Bucket.(internalobjstore.BucketAttrsReader)
		if !ok {
			return fmt.Errorf("frozen source: configured bucket cannot recheck lifecycle metadata")
		}
		return verifyFrozenBucketEnvironment(ctx, reader, bucketName, sourcePrefix, expected)
	}
}

func (e *FencingExecutor) verifyFrozenBucketEnvironment(ctx context.Context, bucketName, sourcePrefix string, expected recovery.FrozenBucketEnvironment) error {
	if e == nil || e.ArchiveBucket == nil {
		return fmt.Errorf("frozen source: registrar bucket is unavailable")
	}
	reader, ok := e.ArchiveBucket.(internalobjstore.BucketAttrsReader)
	if !ok {
		return fmt.Errorf("frozen source: registrar bucket cannot read lifecycle metadata")
	}
	return verifyFrozenBucketEnvironment(ctx, reader, bucketName, sourcePrefix, expected)
}

func frozenBucketMetageneration(environment *recovery.FrozenBucketEnvironment) string {
	if environment == nil {
		return ""
	}
	return environment.Metageneration
}

func frozenBucketLifecycleDigest(environment *recovery.FrozenBucketEnvironment) string {
	if environment == nil {
		return ""
	}
	return environment.LifecycleDigest
}
