package operator

import (
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

func TestStoredAllocatedCancellationLostResponseRetainsOriginalResult(t *testing.T) {
	e, fixture, id, runtime := newMutationStoreFixture(t)
	activateStoredProducer(t, e, id, runtime)
	intent := completeStoreIntent(fencing.MutationIntent{OperationID: "cancel-original", Kind: "upload", Key: "key", PayloadRef: "stage", PayloadDigest: strings.Repeat("a", 64)})
	transition := func(p *fencing.MutationProducer) error {
		return p.AdmitAndCancel(1, intent, strings.Repeat("b", 64))
	}
	fixture.mu.Lock()
	fixture.lostResponse = true
	fixture.mu.Unlock()
	if _, err := e.changeMutationProducer(t.Context(), runtime, transition); err == nil {
		t.Fatal("lost cancellation response did not remain uncertain")
	}
	// A new authority process retries the identical pure transition. There is no
	// intermediate persisted entry that a worker could prepare or dispatch.
	restarted := &FencingExecutor{Kube: e.Kube}
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
}
