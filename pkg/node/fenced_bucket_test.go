package node

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/fencing"
	thanosobjstore "github.com/thanos-io/objstore"
)

type recordingSourceMutator struct {
	key       string
	payload   []byte
	condition fencing.MutationCondition
	deleted   int64
	err       error
}

func (m *recordingSourceMutator) Upload(_ context.Context, key string, condition fencing.MutationCondition, payload []byte) error {
	m.key, m.condition, m.payload = key, condition, append([]byte(nil), payload...)
	return m.err
}

func TestFencedBucketClassifiesRelayedMutationConditionFailure(t *testing.T) {
	base, err := objectstore.NewBucket(objectstore.Config{Provider: objectstore.ProviderFilesystem, FilesystemDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()
	mutator := &recordingSourceMutator{err: fencing.ErrMutationConditionFailed}
	bucket := &fencedBucket{MeteredBucket: base, source: mutator, prefix: "cluster"}
	err = bucket.Upload(context.Background(), "cluster/object", strings.NewReader("stale"), thanosobjstore.WithIfNotExists())
	if !errors.Is(err, fencing.ErrMutationConditionFailed) || !bucket.IsConditionNotMetErr(err) {
		t.Fatalf("relayed 412 was not visible to the archive caller: err=%v classified=%t", err, bucket.IsConditionNotMetErr(err))
	}
}
func (m *recordingSourceMutator) Delete(_ context.Context, key string, generation int64) error {
	m.key, m.deleted = key, generation
	return nil
}
func (*recordingSourceMutator) Close() error { return nil }

func TestFencedBucketMapsOnlyRepresentableMutationConditions(t *testing.T) {
	base, err := objectstore.NewBucket(objectstore.Config{Provider: objectstore.ProviderFilesystem, FilesystemDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	mutator := &recordingSourceMutator{}
	bucket := &fencedBucket{MeteredBucket: base, source: mutator, prefix: "prefix/cluster"}
	if err := bucket.Upload(context.Background(), "prefix/cluster/object", strings.NewReader("sealed bytes"), thanosobjstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	if mutator.key != "object" || string(mutator.payload) != "sealed bytes" || mutator.condition.Mode != fencing.MutationCreateOnly {
		t.Fatalf("unexpected source upload: key=%q payload=%q condition=%+v", mutator.key, mutator.payload, mutator.condition)
	}
	version := &thanosobjstore.ObjectVersion{Type: thanosobjstore.Generation, Value: "42"}
	if err := bucket.Upload(context.Background(), "prefix/cluster/object", strings.NewReader("replacement"), thanosobjstore.WithIfMatch(version)); err != nil {
		t.Fatal(err)
	}
	if mutator.condition.Mode != fencing.MutationMatchGeneration || mutator.condition.Generation != 42 {
		t.Fatalf("generation condition was not retained: %+v", mutator.condition)
	}
	if err := bucket.DeleteVersion(context.Background(), "prefix/cluster/object", *version); err != nil {
		t.Fatal(err)
	}
	if mutator.key != "object" || mutator.deleted != 42 {
		t.Fatalf("version delete lost selected generation: key=%q generation=%d", mutator.key, mutator.deleted)
	}
	if err := bucket.Delete(context.Background(), "prefix/cluster/object"); err == nil {
		t.Fatal("key-only delete was allowed to bypass exact-generation admission")
	}
}

func TestFencedBucketRejectsUnrepresentableConditionsBeforeMutation(t *testing.T) {
	base, err := objectstore.NewBucket(objectstore.Config{Provider: objectstore.ProviderFilesystem, FilesystemDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	mutator := &recordingSourceMutator{}
	bucket := &fencedBucket{MeteredBucket: base, source: mutator, prefix: "cluster"}
	version := &thanosobjstore.ObjectVersion{Type: thanosobjstore.ETag, Value: "etag"}
	err = bucket.Upload(context.Background(), "cluster/object", io.NopCloser(strings.NewReader("bytes")), thanosobjstore.WithIfMatch(version))
	if err == nil || mutator.key != "" {
		t.Fatal("unsupported ETag condition was silently dropped")
	}
	if err := bucket.Upload(context.Background(), "outside/object", strings.NewReader("bytes")); err == nil || mutator.key != "" {
		t.Fatal("out-of-scope key reached the mutation client")
	}
}

func TestSupervisedNodeRejectsExplicitWriterCredentials(t *testing.T) {
	config := voterTestConfig(t)
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	config.ObjStoreServiceAccount = "/var/run/writer.json"
	if err := rejectSupervisedWriterCredentials(config); err == nil {
		t.Fatal("supervised node accepted configured writer credentials")
	}
	config.ObjStoreServiceAccount = ""
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/var/run/writer.json")
	if err := rejectSupervisedWriterCredentials(config); err == nil {
		t.Fatal("supervised node accepted ambient key-file credentials")
	}
}
