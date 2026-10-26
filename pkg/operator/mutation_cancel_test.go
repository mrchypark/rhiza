package operator

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/thanos-io/objstore"
)

type mutationCASReplyFaultBucket struct {
	objstore.Bucket
	loseCASReply       bool
	loseCASAfter       int
	failReadAfterReply bool
	failNextRead       bool
}

func (b *mutationCASReplyFaultBucket) Upload(ctx context.Context, name string, reader io.Reader, options ...objstore.ObjectUploadOption) error {
	params := objstore.ApplyObjectUploadOptions(options...)
	if err := b.Bucket.Upload(ctx, name, reader, options...); err != nil {
		return err
	}
	lose := params.Condition != nil && b.loseCASReply
	if params.Condition != nil && b.loseCASAfter > 0 {
		b.loseCASAfter--
		lose = lose || b.loseCASAfter == 0
	}
	if lose {
		b.loseCASReply = false
		b.failNextRead = b.failReadAfterReply
		return errors.New("injected lost canonical CAS response")
	}
	return nil
}

func (b *mutationCASReplyFaultBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if b.failNextRead {
		b.failNextRead = false
		return nil, errors.New("injected canonical CAS readback outage")
	}
	return b.Bucket.Get(ctx, name)
}

func TestStoredAllocatedCancellationLostResponseRetainsOriginalResult(t *testing.T) {
	for _, tc := range []struct {
		name           string
		readbackFails  bool
		wantFirstError bool
	}{
		{name: "exact readback resolves lost response"},
		{name: "readback unavailable remains unknown", readbackFails: true, wantFirstError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, fixture, id, runtime := newMutationStoreFixture(t)
			activateStoredProducer(t, e, id, runtime)
			intent := completeStoreIntent(fencing.MutationIntent{OperationID: "cancel-original", Kind: "upload", Key: "key", PayloadRef: "stage", PayloadDigest: strings.Repeat("a", 64)})
			transition := func(p *fencing.MutationProducer) error {
				return p.AdmitAndCancel(1, intent, strings.Repeat("b", 64))
			}
			faultBucket := &mutationCASReplyFaultBucket{Bucket: fixture.authorityBucket, loseCASReply: true, failReadAfterReply: tc.readbackFails}
			e.AuthorityState.bucket = faultBucket
			_, firstErr := e.changeMutationProducer(t.Context(), runtime, transition)
			if (firstErr != nil) != tc.wantFirstError {
				t.Fatalf("first cancellation result error=%v, wantError=%t", firstErr, tc.wantFirstError)
			}
			// A new authority process retries the identical pure transition. There is no
			// second allocation; a successful exact readback may resolve response loss,
			// while an unavailable readback must remain visibly uncertain.
			restarted := &FencingExecutor{Kube: e.Kube, AuthorityState: e.AuthorityState}
			p, err := restarted.changeMutationProducer(t.Context(), runtime, transition)
			if err != nil {
				t.Fatal(err)
			}
			entry := p.Entries[1]
			if p.LastAdmitted != 1 || entry.Intent != intent || entry.Preparation != nil || entry.Resolution == nil || entry.Resolution.Outcome != fencing.MutationRejected || !entry.DispatchClosed {
				t.Fatal("lost response allocated another tuple or lost original cancellation")
			}
			if _, err := restarted.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error {
				return p.Prepare(1, fencing.MutationPreparation{Profile: fencing.MutationExecutionProfile, StoreScopeHash: intent.StoreScopeHash, GenerationSet: true, Generation: 12})
			}); err == nil {
				t.Fatal("canceled allocation became executable")
			}
		})
	}
}
