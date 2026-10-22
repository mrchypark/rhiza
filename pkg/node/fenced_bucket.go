package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/fencing"
	thanosobjstore "github.com/thanos-io/objstore"
)

type sourceBucketMutator interface {
	Upload(context.Context, string, fencing.MutationCondition, []byte) error
	Delete(context.Context, string, int64) error
	Close() error
}

type fencedBucket struct {
	*objectstore.MeteredBucket
	source sourceBucketMutator
	prefix string
}

func (b *fencedBucket) Upload(ctx context.Context, name string, reader io.Reader, options ...thanosobjstore.ObjectUploadOption) error {
	if b.source == nil {
		return fmt.Errorf("fencing: protected object writes are unavailable")
	}
	if err := thanosobjstore.ValidateUploadOptions(b.SupportedObjectUploadOptions(), options...); err != nil {
		return err
	}
	params := thanosobjstore.ApplyObjectUploadOptions(options...)
	if params.ContentType != "" || params.IfNotMatch {
		return fmt.Errorf("fencing: object-store upload option is not represented by the mutation contract")
	}
	key, err := b.mutationKey(name)
	if err != nil {
		return err
	}
	condition := fencing.MutationCondition{Mode: fencing.MutationUnconditional}
	if params.IfNotExists {
		condition.Mode = fencing.MutationCreateOnly
	}
	if params.Condition != nil {
		if params.Condition.Type != thanosobjstore.Generation || params.IfNotExists {
			return fmt.Errorf("fencing: only exact GCS generation conditions are supported")
		}
		generation, parseErr := strconv.ParseInt(params.Condition.Value, 10, 64)
		if parseErr != nil || generation <= 0 || strconv.FormatInt(generation, 10) != params.Condition.Value {
			return fmt.Errorf("fencing: invalid selected GCS generation")
		}
		condition = fencing.MutationCondition{Mode: fencing.MutationMatchGeneration, Generation: generation}
	}
	const maxMutationPayload = 64 << 20
	payload, err := io.ReadAll(io.LimitReader(reader, maxMutationPayload+1))
	if err != nil {
		return err
	}
	if len(payload) > maxMutationPayload {
		return fmt.Errorf("fencing: upload exceeds protected source payload limit")
	}
	return b.source.Upload(ctx, key, condition, payload)
}

func (b *fencedBucket) Delete(context.Context, string) error {
	return fmt.Errorf("fencing: protected deletion requires an exact selected object generation")
}

func (b *fencedBucket) DeleteVersion(ctx context.Context, name string, version thanosobjstore.ObjectVersion) error {
	if b.source == nil {
		return fmt.Errorf("fencing: protected object deletion is unavailable")
	}
	if version.Type != thanosobjstore.Generation {
		return objectstore.ErrDeleteVersionRequired
	}
	generation, err := strconv.ParseInt(version.Value, 10, 64)
	if err != nil || generation <= 0 || strconv.FormatInt(generation, 10) != version.Value {
		return objectstore.ErrDeleteVersionRequired
	}
	key, err := b.mutationKey(name)
	if err != nil {
		return err
	}
	return b.source.Delete(ctx, key, generation)
}

func (b *fencedBucket) SupportsVersionedDelete() bool { return b.source != nil }

func (b *fencedBucket) IsConditionNotMetErr(err error) bool {
	return errors.Is(err, fencing.ErrMutationConditionFailed) || b.MeteredBucket.IsConditionNotMetErr(err)
}

func (b *fencedBucket) SupportedObjectUploadOptions() []thanosobjstore.ObjectUploadOptionType {
	return []thanosobjstore.ObjectUploadOptionType{thanosobjstore.IfNotExists, thanosobjstore.IfMatch}
}

func (b *fencedBucket) Close() error {
	var err error
	if b.source != nil {
		err = b.source.Close()
	}
	if b.MeteredBucket != nil {
		err = errors.Join(err, b.MeteredBucket.Close())
	}
	return err
}

func (b *fencedBucket) mutationKey(name string) (string, error) {
	if name == "" || path.Clean(name) != name || strings.HasPrefix(name, "/") {
		return "", fmt.Errorf("fencing: invalid object key")
	}
	if !strings.HasPrefix(name, b.prefix+"/") {
		return "", fmt.Errorf("fencing: object key is outside the enrolled source prefix")
	}
	key := strings.TrimPrefix(name, b.prefix+"/")
	if key == "" || path.Clean(key) != key || strings.HasPrefix(key, "../") {
		return "", fmt.Errorf("fencing: invalid source object key")
	}
	return key, nil
}

var _ thanosobjstore.Bucket = (*fencedBucket)(nil)
var _ objectstore.VersionedDeleteBucket = (*fencedBucket)(nil)
