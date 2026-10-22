package operator

import (
	"context"
	"errors"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

type frozenBucketAttrsReader struct {
	attrs *storage.BucketAttrs
	err   error
}

func (r frozenBucketAttrsReader) BucketAttrs(context.Context) (*storage.BucketAttrs, error) {
	return r.attrs, r.err
}

func TestCaptureFrozenBucketEnvironmentRejectsUnsafeLifecycle(t *testing.T) {
	for _, test := range []struct {
		name string
		rule storage.LifecycleRule
	}{
		{name: "unconditional delete", rule: storage.LifecycleRule{Action: storage.LifecycleAction{Type: storage.DeleteAction}}},
		{name: "overlapping delete prefix", rule: storage.LifecycleRule{Action: storage.LifecycleAction{Type: storage.DeleteAction}, Condition: storage.LifecycleCondition{MatchesPrefix: []string{"root/archive/sub"}}}},
		{name: "parent delete prefix", rule: storage.LifecycleRule{Action: storage.LifecycleAction{Type: storage.DeleteAction}, Condition: storage.LifecycleCondition{MatchesPrefix: []string{"root"}}}},
		{name: "empty delete prefix", rule: storage.LifecycleRule{Action: storage.LifecycleAction{Type: storage.DeleteAction}, Condition: storage.LifecycleCondition{MatchesPrefix: []string{""}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			reader := frozenBucketAttrsReader{attrs: &storage.BucketAttrs{Name: "bucket", MetaGeneration: 3, Lifecycle: storage.Lifecycle{Rules: []storage.LifecycleRule{test.rule}}}}
			if _, err := captureFrozenBucketEnvironment(context.Background(), reader, "bucket", "root/archive"); err == nil {
				t.Fatal("accepted lifecycle rule that can delete managed objects")
			}
		})
	}
}

func TestCaptureFrozenBucketEnvironmentCanonicalizesAndVerifies(t *testing.T) {
	first := storage.Lifecycle{Rules: []storage.LifecycleRule{
		{Action: storage.LifecycleAction{Type: storage.SetStorageClassAction, StorageClass: "NEARLINE"}, Condition: storage.LifecycleCondition{MatchesPrefix: []string{"other/b", "other/a"}, MatchesStorageClasses: []string{"COLDLINE", "STANDARD"}}},
		{Action: storage.LifecycleAction{Type: storage.AbortIncompleteMPUAction}, Condition: storage.LifecycleCondition{AgeInDays: 7}},
	}}
	second := storage.Lifecycle{Rules: []storage.LifecycleRule{
		{Action: storage.LifecycleAction{Type: storage.AbortIncompleteMPUAction}, Condition: storage.LifecycleCondition{AgeInDays: 7}},
		{Action: storage.LifecycleAction{Type: storage.SetStorageClassAction, StorageClass: "NEARLINE"}, Condition: storage.LifecycleCondition{MatchesPrefix: []string{"other/a", "other/b"}, MatchesStorageClasses: []string{"STANDARD", "COLDLINE"}}},
	}}
	reader := frozenBucketAttrsReader{attrs: &storage.BucketAttrs{Name: "bucket", MetaGeneration: 3, Lifecycle: first}}
	want, err := captureFrozenBucketEnvironment(context.Background(), reader, "bucket", "root/archive")
	if err != nil {
		t.Fatal(err)
	}
	reader.attrs.Lifecycle = second
	if err := verifyFrozenBucketEnvironment(context.Background(), reader, "bucket", "root/archive", want); err != nil {
		t.Fatalf("semantically identical reordered lifecycle policy rejected: %v", err)
	}
	reader.attrs.MetaGeneration++
	if err := verifyFrozenBucketEnvironment(context.Background(), reader, "bucket", "root/archive", want); err == nil {
		t.Fatal("accepted changed bucket metageneration")
	}
	reader.attrs.MetaGeneration--
	reader.attrs.Lifecycle.Rules[0].Condition.AgeInDays = 8
	if err := verifyFrozenBucketEnvironment(context.Background(), reader, "bucket", "root/archive", want); err == nil {
		t.Fatal("accepted changed lifecycle policy")
	}
}

func TestFrozenBucketEnvironmentFailsClosedOnMissingOrUnreadableAttributes(t *testing.T) {
	for _, test := range []struct {
		name   string
		reader frozenBucketAttrsReader
	}{
		{name: "nil attributes", reader: frozenBucketAttrsReader{}},
		{name: "missing bucket identity", reader: frozenBucketAttrsReader{attrs: &storage.BucketAttrs{MetaGeneration: 1}}},
		{name: "missing metageneration", reader: frozenBucketAttrsReader{attrs: &storage.BucketAttrs{Name: "bucket"}}},
		{name: "read error", reader: frozenBucketAttrsReader{err: errors.New("unavailable")}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := captureFrozenBucketEnvironment(context.Background(), test.reader, "bucket", "root/archive"); err == nil {
				t.Fatal("expected fail-closed bucket metadata error")
			}
		})
	}
	for _, prefix := range []string{"", ".", "/root/archive", "root/../archive", "root/archive/"} {
		if _, err := captureFrozenBucketEnvironment(context.Background(), frozenBucketAttrsReader{attrs: &storage.BucketAttrs{Name: "bucket", MetaGeneration: 1}}, "bucket", prefix); err == nil {
			t.Fatalf("accepted malformed source prefix %q", prefix)
		}
	}
	if err := verifyFrozenBucketEnvironment(context.Background(), frozenBucketAttrsReader{}, "bucket", "root/archive", recovery.FrozenBucketEnvironment{}); err == nil || !strings.Contains(err.Error(), "incomplete") {
		t.Fatalf("invalid expected environment error=%v", err)
	}
}

func TestCaptureFrozenBucketEnvironmentRejectsUnknownAction(t *testing.T) {
	reader := frozenBucketAttrsReader{attrs: &storage.BucketAttrs{Name: "bucket", MetaGeneration: 1, Lifecycle: storage.Lifecycle{Rules: []storage.LifecycleRule{{Action: storage.LifecycleAction{Type: "FutureAction"}}}}}}
	if _, err := captureFrozenBucketEnvironment(context.Background(), reader, "bucket", "root/archive"); err == nil {
		t.Fatal("accepted unknown lifecycle action")
	}
}

func TestCaptureFrozenBucketEnvironmentRequiresExpectedBucketIdentity(t *testing.T) {
	reader := frozenBucketAttrsReader{attrs: &storage.BucketAttrs{Name: "actual-bucket", MetaGeneration: 1}}
	if _, err := captureFrozenBucketEnvironment(context.Background(), reader, "", "root/archive"); err == nil {
		t.Fatal("accepted empty expected bucket name")
	}
	if _, err := captureFrozenBucketEnvironment(context.Background(), reader, "configured-bucket", "root/archive"); err == nil {
		t.Fatal("accepted attributes from a different bucket")
	}
	want, err := captureFrozenBucketEnvironment(context.Background(), reader, "actual-bucket", "root/archive")
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyFrozenBucketEnvironment(context.Background(), reader, "", "root/archive", want); err == nil {
		t.Fatal("verified environment without an expected bucket name")
	}
	if err := verifyFrozenBucketEnvironment(context.Background(), reader, "configured-bucket", "root/archive", want); err == nil {
		t.Fatal("verified environment from a different bucket")
	}
}
