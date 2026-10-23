package operator

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

// An old process may remain alive after canonical closure. Its still-valid
// identity must not be able to admit a new source-object mutation.
func TestClosedIdentityCannotAdmitSourceMutationWhileAuthenticated(t *testing.T) {
	f := newArchiveGenesisFixture(t)
	commitArchiveGenesisFixture(t, f)

	var sourceRequests atomic.Int32
	source := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sourceRequests.Add(1)
		http.Error(w, "unexpected source storage request", http.StatusInternalServerError)
	}))
	t.Cleanup(source.Close)

	authority := httptest.NewTLSServer(f.e.Handler())
	t.Cleanup(authority.Close)
	client := &fencing.AuthorityClient{URL: authority.URL + "/v1/supervisor", TokenFile: f.e.Kube.TokenFile, Client: authority.Client()}
	binding, err := client.Bind(t.Context(), f.id)
	if err != nil || binding.Producer == nil {
		t.Fatalf("old identity could not establish its valid source session: %+v %v", binding, err)
	}
	producerUID := binding.Producer.ProducerUID

	beforeProducer, _, _, err := f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
	if err != nil || beforeProducer.LastAdmitted != 1 || beforeProducer.Phase != fencing.MutationOpen {
		t.Fatalf("unexpected producer state before closure: %+v %v", beforeProducer, err)
	}

	// Prepare a distinct update to the already-committed HEAD. Its exact
	// generation condition makes it a valid next mutation if admission succeeds.
	payload := []byte("replacement archive HEAD")
	draft := stageStoreIntent()
	draft.StoreScopeHash = binding.Producer.StoreScopeHash
	draft.OwnerStep = fencing.MutationOwnerStep{Owner: "test", Workflow: "closed-identity", Step: "head-update", Attempt: 1}
	draft.Key = "archive/head.bin"
	draft.Condition = fencing.MutationCondition{Mode: fencing.MutationMatchGeneration, Generation: 21}
	ticket := strings.Repeat("d", 64)
	if err := client.ReserveUpload(t.Context(), ticket, draft); err != nil {
		t.Fatalf("old identity could not reserve its pre-close HEAD payload: %v", err)
	}
	sealed, err := client.SealUpload(t.Context(), ticket, payload)
	if err != nil {
		t.Fatalf("old identity could not seal its pre-close HEAD payload: %v", err)
	}
	intent := draft
	intent.OperationID = fencing.MutationRequestID(producerUID, 2)
	intent.PayloadRef, intent.PayloadDigest, intent.PayloadSize = sealed.Ref, sealed.Digest, sealed.Size
	// Keep the fixture's committed-genesis storage provider available for
	// registration and staging checks; observe only the post-close mutation path.
	f.setStorage(t, source.URL, source.Client())

	closeHash := strings.Repeat("e", 64)
	if _, err := f.e.change(t.Context(), f.runtime.StatefulSetUID, f.runtime.SourceClusterID, func(l *fencing.Ledger) error {
		return l.CloseNodes("close-old-identity", closeHash, []string{f.id.NodeID})
	}); err != nil {
		t.Fatalf("canonical voter close failed: %v", err)
	}
	closed, err := f.e.read(t.Context(), f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
	if err != nil || closed.Ledger.ClosedNodes[f.id.NodeID].RequestHash != closeHash {
		t.Fatalf("test did not establish canonical closure for the old identity: %+v %v", closed.Ledger.ClosedNodes, err)
	}

	// Send the new source mutation through the real HTTPS authority route. A 410
	// maps specifically to ErrClosed; auth failures and pending responses do not.
	err = client.Admit(t.Context(), 2, intent)
	if !errors.Is(err, fencing.ErrClosed) {
		t.Fatalf("closed old identity admission returned %v, want ErrClosed", err)
	}
	if got := sourceRequests.Load(); got != 0 {
		t.Fatalf("closed old identity reached source storage %d times", got)
	}
	afterProducer, _, _, err := f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
	if err != nil || afterProducer.LastAdmitted != beforeProducer.LastAdmitted || afterProducer.Phase != beforeProducer.Phase {
		t.Fatalf("rejected admission changed producer state: before=%+v after=%+v err=%v", beforeProducer, afterProducer, err)
	}

	// The same token/session still authenticates for a read-only lookup, proving
	// denial came from the closed canonical identity rather than process death or
	// credential expiry.
	lookup, err := client.Lookup(t.Context(), 2, intent)
	if err != nil || lookup.Found {
		t.Fatalf("old identity was not still authenticated for read-only lookup: %+v %v", lookup, err)
	}
	if sourceRequests.Load() != 0 {
		t.Fatalf("read-only verification unexpectedly reached source storage: %d", sourceRequests.Load())
	}
}
