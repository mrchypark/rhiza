package operator

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// These regressions describe the required safety/liveness contract. The original
// red receipts are retained; positive composition now exercises the actual store
// and production reconcile path rather than assuming an unpersisted v1 proof.
func TestIndependentQuietWindowCannotAuthorizeOutstandingMutation(t *testing.T) {
	_, scope := independentFixture(t)
	response := independentEvidenceFor(scope.attestationRequest())
	accepted, release, completed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(accepted)
		<-release
		// The provider applies the accepted request only after both observations.
		close(completed)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer provider.Close()
	requestDone := make(chan error, 1)
	go func() {
		result, err := provider.Client().Post(provider.URL, "application/octet-stream", strings.NewReader("late mutation"))
		if result != nil {
			_ = result.Body.Close()
		}
		requestDone <- err
	}()
	defer func() {
		close(release)
		if err := <-requestDone; err != nil {
			t.Error(err)
		}
	}()
	<-accepted
	select {
	case <-completed:
		t.Fatal("provider applied mutation before the proof attempt")
	default:
	}
	proof, err := validateIndependent(scope, response)
	if err == nil && proof.StorageQuiesced {
		t.Error("quiet-window-only evidence authorized storage while an accepted mutation could still complete")
	}
}

func TestIndependentHistoricalRunNeedsItsOwnRuntimeBinding(t *testing.T) {
	_, scope := independentFixture(t)
	old := scope.runs[0]
	old.RunID = "historical-run-without-container-binding"
	// Same Pod and WAL, but another runtime attempt. Its old container identity
	// was not enrolled; a response listing only the latest container is insufficient.
	scope.runs = append(scope.runs, old)
	response := independentEvidenceFor(scope.attestationRequest())
	if _, err := validateIndependent(scope, response); err == nil {
		t.Error("one latest container was accepted as termination evidence for an unenrolled historical run")
	}
}

func TestIndependentRetiredVoterEvidenceMustComposeIntoGeneration(t *testing.T) {
	exerciseIndependentVoterThenGeneration(t)
}
