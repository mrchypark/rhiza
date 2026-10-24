package operator

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

func TestStartupAfterAutomaticGenerationEscalationIsNotAdmitted(t *testing.T) {
	runAuthenticatedLearnerPromotionRetriesExactCanonicalCompletion(t, func(t *testing.T, setup authenticatedPendingLearnerSetup) {
		ctx, f := setup.ctx, setup.f
		fencerClient, ok := setup.controller.Fencer.(*FencingClient)
		if !ok || !strings.HasSuffix(setup.client.URL, "/v1/supervisor") {
			t.Fatalf("fixture does not expose the existing authenticated fencing endpoint: fencer=%T supervisorURL=%q", setup.controller.Fencer, setup.client.URL)
		}
		// The bootstrap helper only exercises promotion and leaves its Fencer
		// client at the handler root. Ordinary online dispatch uses the existing
		// /v1/fences route, so bind this test client to that route explicitly.
		fencerClient.URL = strings.TrimSuffix(setup.client.URL, "/v1/supervisor") + httpFenceRequestPath
		identity := setup.physicalID
		if setup.grant.Role != fencing.StartupRoleLearnerPendingRemove || setup.grant.Identity != identity {
			t.Fatalf("open same-ordinal learner registration was not resolved by production authority: grant=%+v identity=%+v", setup.grant, identity)
		}
		if setup.state.BootstrapGenesis == nil {
			t.Fatal("helper no longer exposes the bootstrap marker that production completion clears")
		}
		// This callback seeds the exact postcondition of automatic_execution.go's
		// normal completed-recovery transition. The helper manually seeds a
		// pending learner intent before returning, so it otherwise retains this
		// bootstrap-only marker and is correctly rejected by the normal online
		// generation path.
		setup.state.BootstrapGenesis = nil
		// The helper's manual learner-intent seed predates ordinary intent
		// bookkeeping. Reconstitute the policy/durability/voter snapshot that a
		// normal post-bootstrap online intent carries, then rebuild its child
		// with the production builder and refresh the exact assignment digest.
		// The independently verified removal proof and immutable Secret remain
		// unchanged.
		setup.intent.Policy = *setup.cluster.Spec.Policy
		setup.intent.Durability = "async"
		setup.intent.Voters = append([]VoterPod(nil), setup.state.Voters...)
		setup.child.Spec = automaticRecoveryChildSpec(&setup.cluster, setup.state, setup.intent)
		setup.intent.SpecHash = hashJSON(setup.child.Spec)
		assignment, err := sameOrdinalAssignmentDigest(setup.state, setup.intent)
		if err != nil {
			t.Fatalf("rebind same-ordinal assignment to normal post-bootstrap intent: %v", err)
		}
		setup.intent.LearnerAssignmentID = assignment
		persistTestRecovery(t, f, setup.child)
		setup.intent.NoQuorumSince = time.Now().Add(-10 * time.Minute)
		setup.intent.RetirementSince = time.Now().Add(-10 * time.Minute)
		head, err := recovery.EmptyArchiveHeadBytes()
		if err != nil {
			t.Fatalf("encode post-bootstrap canonical archive HEAD: %v", err)
		}
		if err := f.archive.Upload(ctx, "root/source/archive/head.bin", bytes.NewReader(head), objstore.WithIfNotExists()); err != nil {
			t.Fatalf("seed ordinary post-bootstrap archive HEAD on the fixture store: %v", err)
		}
		if err := setup.controller.saveAuto(ctx, &setup.cluster, setup.state, mustAutoVersion(t, setup.controller, &setup.cluster)); err != nil {
			t.Fatalf("seed normal post-bootstrap automatic control state: %v", err)
		}
		oldIntentBytes := jsonBytes(setup.intent)
		oldIntentHash := hashBytes(oldIntentBytes)
		childBeforeClosure := hashJSON(setup.child)
		secretPath := f.e.controller().corePath("secrets", setup.intent.LearnerSecretName)
		var secretBefore object
		if err := f.e.Kube.Get(ctx, secretPath, &secretBefore); err != nil {
			t.Fatalf("read exact immutable learner credential before escalation: %v", err)
		}
		secretBeforeHash := hashJSON(secretBefore)
		rootBefore, _, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
		if err != nil || rootBefore.Ledger.ClosedBy != "" {
			t.Fatalf("fixture must begin with the same canonical root open: root=%+v err=%v", rootBefore, err)
		}

		// The open learner registration above and the following denied retry use
		// one supervisor, one CONTROL store, one automatic-control bucket, and
		// the same physical Pod identity. Count node lookups as an enrollment
		// boundary; the open registration has already exercised the agent.
		runtimeNodes := &runtimeNodeLookupCapture{next: f.e.Kube.Client.Transport}
		f.e.Kube.Client = &http.Client{Transport: runtimeNodes}
		openNodeLookups := 0
		for _, op := range rootBefore.Ledger.Runs {
			if op.Identity.PodUID == identity.PodUID && op.Identity.RunID == identity.RunID && op.Runtime != nil {
				openNodeLookups++
			}
		}
		if openNodeLookups != 1 {
			t.Fatalf("open learner grant did not persist exactly one runtime enrollment: matching runs=%d", openNodeLookups)
		}

		state, version, err := setup.controller.loadAuto(ctx, setup.controller.autoKey(&setup.cluster))
		if err != nil || state.Intent == nil || state.BootstrapGenesis != nil {
			t.Fatalf("reload normal post-bootstrap learner control: state=%+v err=%v", state, err)
		}
		disposition, err := setup.controller.assessAutomaticEscalation(ctx, &setup.cluster, state, version, setup.sts)
		if err != nil || disposition != automaticEscalationStop || state.Intent == nil || state.Intent.Request.Scope != ScopeGeneration || state.Intent.Supersedes == "" {
			t.Fatalf("production assessment did not reserve generation escalation: disposition=%v intent=%+v err=%v", disposition, state.Intent, err)
		}
		supersededPath := setup.controller.Prefix + "/automatic/test/" + setup.cluster.Spec.LogicalID + "/superseded/" + state.Intent.Supersedes + ".json"
		supersededReader, err := setup.controller.Bucket.Get(ctx, supersededPath)
		if err != nil {
			t.Fatalf("read immutable superseded learner intent: %v", err)
		}
		supersededBytes, readErr := io.ReadAll(supersededReader)
		closeErr := supersededReader.Close()
		if readErr != nil || closeErr != nil || !bytes.Equal(supersededBytes, oldIntentBytes) || hashBytes(supersededBytes) != oldIntentHash {
			t.Fatalf("superseded learner payload differs from the exact prior intent: read=%v close=%v", readErr, closeErr)
		}
		state, version, err = setup.controller.loadAuto(ctx, setup.controller.autoKey(&setup.cluster))
		if err != nil || state.Intent == nil || state.Intent.Request.Scope != ScopeGeneration || state.Intent.Proof != nil {
			t.Fatalf("reload CAS-reserved proofless generation intent: state=%+v err=%v", state, err)
		}
		controlAfterAssessmentHash := hashJSON(state)
		capturedFence := &fenceCreateCaptureTransport{next: f.e.Kube.Client.Transport}
		f.e.Kube.Client = &http.Client{Transport: capturedFence}
		if err := setup.controller.advanceAutomatic(ctx, &setup.cluster, state, version, setup.sts); err != nil {
			t.Fatalf("dispatch generation intent through production controller: %v", err)
		}
		if capturedFence.record == nil {
			t.Fatalf("production dispatch did not POST a RhizaFence request: API calls=%v", capturedFence.calls)
		}
		fence := *capturedFence.record
		if hashJSON(fence.Spec) != hashJSON(state.Intent.Request) {
			t.Fatalf("dispatched RhizaFence differs from canonical generation intent: got=%+v want=%+v", fence.Spec, state.Intent.Request)
		}
		requestDigest, err := requestHash(&state.Intent.Request)
		if err != nil {
			t.Fatal(err)
		}
		reconcileErr := f.e.reconcileFence(ctx, v2FenceEndpoint(), &fence)
		closedRoot, _, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
		if err != nil || closedRoot.Ledger.ClosedBy != state.Intent.Request.OperationID || closedRoot.Ledger.ClosedHash != requestDigest {
			t.Fatalf("production executor did not commit exact canonical generation closure: reconcile=%v err=%v root=%+v", reconcileErr, err, closedRoot)
		}
		if reconcileErr == nil || fence.Status.Proof != nil {
			t.Fatalf("expected closure before terminal proof completion: reconcile=%v proof=%+v", reconcileErr, fence.Status.Proof)
		}
		closedLedgerHash := hashJSON(closedRoot.Ledger)
		if hashJSON(setup.child) != childBeforeClosure {
			t.Fatal("generation closure changed the pending same-ordinal recovery child")
		}
		var secretAfterClosure object
		if err := f.e.Kube.Get(ctx, secretPath, &secretAfterClosure); err != nil || hashJSON(secretAfterClosure) != secretBeforeHash {
			t.Fatalf("generation closure changed the immutable learner credential: err=%v", err)
		}
		if !bytes.Equal(supersededBytes, oldIntentBytes) || hashBytes(supersededBytes) != oldIntentHash {
			t.Fatal("generation closure changed the immutable superseded learner intent")
		}
		controlAfterDispatch, _, err := setup.controller.loadAuto(ctx, setup.controller.autoKey(&setup.cluster))
		if err != nil || hashJSON(controlAfterDispatch) != controlAfterAssessmentHash {
			t.Fatalf("executor closure changed automatic control: err=%v", err)
		}
		enrollmentLookupsBeforeDenial := runtimeNodes.lookups

		identity.RunID = strings.Repeat("9", 32)
		var captured bytes.Buffer
		previousWriter, previousFlags := log.Writer(), log.Flags()
		log.SetOutput(&captured)
		log.SetFlags(0)
		t.Cleanup(func() { log.SetOutput(previousWriter); log.SetFlags(previousFlags) })
		status := &statusRecordingTransport{next: setup.client.Client.Transport}
		client := setup.client
		client.Client = &http.Client{Transport: status}
		grant, registerErr := client.RegisterStartup(ctx, identity)
		if !errors.Is(registerErr, fencing.ErrClosed) || status.code != http.StatusGone || grant.Role != "" || grant.Identity != (fencing.Identity{}) {
			t.Fatalf("same physical learner restart after generation closure must be denied early: status=%d grant=%+v err=%v", status.code, grant, registerErr)
		}
		if runtimeNodes.lookups != enrollmentLookupsBeforeDenial {
			t.Fatalf("early closed-generation denial reached RuntimeAgent enrollment: node lookups before=%d after=%d", enrollmentLookupsBeforeDenial, runtimeNodes.lookups)
		}
		rootAfterDenial, _, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
		if err != nil || hashJSON(rootAfterDenial.Ledger) != closedLedgerHash {
			t.Fatalf("denied learner restart changed canonical root: err=%v", err)
		}
		if _, exists := rootAfterDenial.Ledger.Runs[identity.RunID]; exists {
			t.Fatalf("denied learner restart created attempted run %q", identity.RunID)
		}
		controlAfterDenial, _, err := setup.controller.loadAuto(ctx, setup.controller.autoKey(&setup.cluster))
		if err != nil || hashJSON(controlAfterDenial) != controlAfterAssessmentHash {
			t.Fatalf("denied learner restart changed automatic control: err=%v", err)
		}
		var childAfter Resource
		if err := f.e.Kube.Get(ctx, f.e.controller().resourcePath(setup.intent.RecoveryName), &childAfter); err != nil || hashJSON(childAfter) != childBeforeClosure {
			t.Fatalf("denied learner restart changed recovery child: err=%v", err)
		}
		var secretAfterDenial object
		if err := f.e.Kube.Get(ctx, secretPath, &secretAfterDenial); err != nil || hashJSON(secretAfterDenial) != secretBeforeHash {
			t.Fatalf("denied learner restart changed immutable credential: err=%v", err)
		}
		lines := strings.Split(strings.TrimSpace(captured.String()), "\n")
		if len(lines) != 1 {
			t.Fatalf("closed same-ordinal learner registration emitted %d denial records: %q", len(lines), captured.String())
		}
		var event startupRegistrationDenialEvent
		if err := json.Unmarshal([]byte(lines[0]), &event); err != nil {
			t.Fatalf("decode same-ordinal learner denial event: %v", err)
		}
		if event.Stage != startupRegistrationStageGrant || event.GrantResolved || event.HTTPStatus != http.StatusGone ||
			event.AttemptedRunID != identity.RunID || event.PodUID != identity.PodUID || event.RequestedNodeID != identity.NodeID ||
			event.ResolvedNodeID != "" || event.AssignmentID != "" || event.RuntimeContainerID != "" {
			t.Fatalf("closed learner denial occurred after resolving/enrolling its grant: %+v", event)
		}
	})
}

type namespaceAliasTransport struct {
	next     http.RoundTripper
	from, to string
}

func (t namespaceAliasTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	copy := request.Clone(request.Context())
	urlCopy := *request.URL
	copy.URL = &urlCopy
	copy.URL.Path = strings.Replace(copy.URL.Path, "/namespaces/"+t.from+"/", "/namespaces/"+t.to+"/", 1)
	if copy.Method == http.MethodPut && strings.HasSuffix(copy.URL.Path, "/rhizaclusters/cluster/status") {
		body, err := io.ReadAll(copy.Body)
		if err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body)), Header: make(http.Header), Request: copy}, nil
	}
	return t.next.RoundTrip(copy)
}

type fenceCreateCaptureTransport struct {
	next   http.RoundTripper
	record *fenceResource
	calls  []string
}

type runtimeNodeLookupCapture struct {
	next     http.RoundTripper
	lookups int
}

func (t *runtimeNodeLookupCapture) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/api/v1/nodes/") {
		t.lookups++
	}
	return t.next.RoundTrip(request)
}

func (t *fenceCreateCaptureTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	copy := request.Clone(request.Context())
	t.calls = append(t.calls, copy.Method+" "+copy.URL.Path)
	if copy.Method == http.MethodPost && copy.URL.Path == v2FenceEndpoint() {
		body, err := io.ReadAll(copy.Body)
		if err != nil {
			return nil, err
		}
		var record fenceResource
		if err := json.Unmarshal(body, &record); err != nil {
			return nil, err
		}
		t.record = &record
		copy.Body = io.NopCloser(bytes.NewReader(body))
	}
	response, err := t.next.RoundTrip(copy)
	if err == nil && copy.Method == http.MethodGet && response.StatusCode == http.StatusOK && strings.Contains(copy.URL.Path, "/rhizafences/") {
		body, readErr := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		var record fenceResource
		if json.Unmarshal(body, &record) == nil {
			t.record = &record
		}
		response.Body = io.NopCloser(bytes.NewReader(body))
	}
	return response, err
}
