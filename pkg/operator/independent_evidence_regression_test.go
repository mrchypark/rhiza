package operator

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

// These regressions describe the required safety/liveness contract. They are
// intentionally red on the reviewed v1 contract; never weaken the assertions to
// qualify dead-Pod healing without the missing evidence implementation.
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
	request, generationScope := independentFixture(t)
	retired := request.Targets[0]
	priorScope := generationScope
	priorScope.request.Scope = ScopeVoter
	priorScope.request.Targets = []FenceTarget{retired}
	priorScope.targets = []FenceTarget{retired}
	priorScope.currentPodUIDs = []string{retired.PodUID}
	priorScope.runs = nil
	for _, run := range generationScope.runs {
		if run.NodeID == retired.NodeID {
			priorScope.runs = append(priorScope.runs, run)
		}
	}
	var err error
	priorScope.requestHash, err = requestHash(&priorScope.request)
	if err != nil {
		t.Fatal(err)
	}
	prior, err := validateIndependent(priorScope, independentEvidenceFor(priorScope.attestationRequest()))
	if err != nil {
		t.Fatal(err)
	}
	// A completed independent fence exists, but the current ledger has nowhere
	// to persist its per-run terminal reference. Do not fix this by skipping runs.
	state := fenceLedger{StatefulSetUID: request.StatefulSetUID, Ledger: fencing.Ledger{
		SourceClusterID: request.SourceClusterID, ClosedBy: "next-generation", ClosedHash: "next-request",
		Runs: make(map[string]fencing.Run),
	}}
	for _, run := range generationScope.runs {
		state.Ledger.Runs[run.RunID] = fencing.Run{Identity: fencing.Identity{
			SourceClusterID: request.SourceClusterID, NodeID: run.NodeID,
			PodUID: run.PodUID, RunID: run.RunID, WALIdentity: run.WALIdentity,
		}}
	}
	request.Targets = request.Targets[1:]
	request.OperationID = "next-generation"
	hash, err := requestHash(&request)
	if err != nil {
		t.Fatal(err)
	}
	_, err = buildIndependentScope(state, request, nil, hash, "next-ledger", []string{request.Targets[0].PodUID}, priorScope.storage)
	if err != nil {
		t.Errorf("validated proof %q for retired voter %q cannot be composed: %v; persist terminal receipts instead of discarding uncovered runs", prior.ProofID, retired.NodeID, err)
	}
}
