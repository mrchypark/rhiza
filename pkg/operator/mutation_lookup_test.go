package operator

import (
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

func mutationLookupFixture(t *testing.T) (*FencingExecutor, *mutationStoreFixture, fencing.Identity, fencing.RuntimeBinding, fencing.MutationIntent) {
	t.Helper()
	e, f, id, runtime := newMutationStoreFixture(t)
	if _, err := e.reserveMutationProducer(t.Context(), runtime); err != nil {
		t.Fatal(err)
	}
	scope := strings.Repeat("a", 64)
	id.WALIdentity = "wal"
	if _, err := e.change(t.Context(), "sts", "source", func(l *fencing.Ledger) error {
		return l.BindMutationScope(id, scope, fencing.MutationExecutionProfile)
	}); err != nil {
		t.Fatal(err)
	}
	activateStoredProducer(t, e, id, runtime)
	run, err := e.enrolledMutationRun(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	intent := completeStoreIntent(fencing.MutationIntent{
		OperationID: fencing.MutationRequestID(run.MutationProducerUID, 1), Kind: "upload", Key: "key",
		PayloadRef: "private/staged-object", PayloadDigest: strings.Repeat("b", 64),
	})
	return e, f, id, runtime, intent
}

func TestMutationLookupAuthenticatedHTTPReturnsLiveEntryWithoutWrite(t *testing.T) {
	e, f, id, runtime, intent := mutationLookupFixture(t)
	run, err := e.enrolledMutationRun(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error { return p.Admit(1, intent) }); err != nil {
		t.Fatal(err)
	}
	pod := supervisedTestPod()
	asObject(pod["metadata"])["name"] = id.NodeID
	asObject(pod["metadata"])["uid"] = id.PodUID
	asObject(pod["metadata"])["ownerReferences"] = []any{object{"kind": "StatefulSet", "name": "rhiza", "uid": "sts", "controller": true}}
	asObject(list(nested(pod, "spec", "containers"))[0])["env"] = []any{object{"name": "RHIZA_CLUSTER_ID", "value": "source"}}
	sts := object{"metadata": object{"uid": "sts"}, "spec": object{"template": object{"spec": object{"containers": []any{object{"name": "rhiza", "env": []any{object{"name": "RHIZA_CLUSTER_ID", "value": "source"}}}}}}}}
	f.mu.Lock()
	f.authPod, f.authStatefulSet = pod, sts
	beforeVersion := f.version
	f.mu.Unlock()
	authority := httptest.NewTLSServer(e.Handler())
	t.Cleanup(authority.Close)
	client := fencing.AuthorityClient{URL: authority.URL + "/v1/supervisor", TokenFile: e.Kube.TokenFile, Client: authority.Client()}
	result, err := client.LookupMutation(t.Context(), id, run.MutationProducerUID, 1, intent)
	if err != nil || !result.Found || result.Archived || result.Entry == nil || result.Entry.Intent != intent {
		t.Fatalf("authenticated live lookup failed: %+v %v", result, err)
	}
	f.mu.Lock()
	afterVersion := f.version
	f.mu.Unlock()
	if beforeVersion != afterVersion {
		t.Fatal("live lookup mutated producer or root state")
	}
}

func TestMutationLookupRejectsForeignProducerAndChangedIntent(t *testing.T) {
	e, _, id, runtime, intent := mutationLookupFixture(t)
	run, err := e.enrolledMutationRun(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error { return p.Admit(1, intent) }); err != nil {
		t.Fatal(err)
	}
	foreign := intent
	foreign.OperationID = fencing.MutationRequestID("foreign", 1)
	if _, err := e.lookupMutation(t.Context(), "sts", id, fencing.MutationLookupRequest{ProducerUID: "foreign", Sequence: 1, Intent: foreign}); err == nil {
		t.Fatal("foreign producer lookup accepted")
	}
	changed := intent
	changed.PayloadDigest = strings.Repeat("c", 64)
	if _, err := e.lookupMutation(t.Context(), "sts", id, fencing.MutationLookupRequest{ProducerUID: run.MutationProducerUID, Sequence: 1, Intent: changed}); err == nil {
		t.Fatal("changed original intent accepted")
	}
}

func TestMutationLookupUsesArchiveForCompactedSequenceWithoutReadmission(t *testing.T) {
	e, _, id, runtime, intent := mutationLookupFixture(t)
	if _, err := e.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error {
		if err := p.Admit(1, intent); err != nil {
			return err
		}
		return p.CancelUnprepared(1, strings.Repeat("c", 64))
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.commitMutationArchive(t.Context(), runtime, storeCompactionPlan(t, e, runtime, 1)); err != nil {
		t.Fatal(err)
	}
	run, err := e.enrolledMutationRun(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	var result fencing.MutationLookupResult
	for range 3 {
		result, err = e.lookupMutation(t.Context(), "sts", id, fencing.MutationLookupRequest{ProducerUID: run.MutationProducerUID, Sequence: 1, Intent: intent})
		if err == nil {
			break
		}
		if !errors.Is(err, ErrFencePending) {
			t.Fatal(err)
		}
	}
	if err != nil || !result.Found || !result.Archived || result.Entry == nil || result.Entry.Resolution == nil || result.Entry.Resolution.Outcome != fencing.MutationRejected {
		t.Fatalf("compacted original result lost or changed: %+v %v", result, err)
	}
	if _, err := e.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error { return p.Admit(1, intent) }); !errors.Is(err, fencing.ErrMutationArchived) {
		t.Fatalf("lookup reopened archived sequence: %v", err)
	}
}
