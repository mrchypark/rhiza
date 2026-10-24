package operator

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type outerRevisionTestFencer func(context.Context, FenceRequest) (*FenceProof, error)

func (f outerRevisionTestFencer) Fence(ctx context.Context, request FenceRequest) (*FenceProof, error) {
	return f(ctx, request)
}

func startupRevisionProbeResponse(req *http.Request, node string) *http.Response {
	response := progressProbeResponse(req, node, true)
	var probe network.RecoveryProbe
	if err := json.NewDecoder(response.Body).Decode(&probe); err != nil {
		panic(err)
	}
	_ = response.Body.Close()
	probe.MAC = controllerFixtureRecoveryProbeMAC(probe, controllerFixtureAdminToken)
	data, _ := json.Marshal(probe)
	response.Body = io.NopCloser(bytes.NewReader(data))
	response.ContentLength = int64(len(data))
	return response
}

func TestAdvanceAutomaticRevisesChildBeforeCheckingNewLearnerHealth(t *testing.T) {
	f := newRetirementEscalationFixture(t)
	state, version := f.load(t)
	intent := state.Intent
	intent.LearnerAttempt = 1
	intent.LearnerName = intent.RecoveryName + "-learner-1"
	intent.LearnerUID = "learner-l1-uid"
	intent.LearnerPodName = ""
	intent.LearnerWAL = "learner-l1-wal"
	intent.LearnerSecretName = intent.LearnerName + "-credentials"
	intent.LearnerSecretUID = "learner-l1-secret-uid"
	intent.LearnerSecretRV = "1"
	intent.LearnerSecretHash = hashJSON(object{"member": "member", "RHIZA_PEER_TOKEN": "token"})
	intent.LearnerMissingSince = time.Time{}
	intent.NoQuorumSince = time.Time{}
	intent.Aborting = false
	intent.PreviousSpecHash = ""

	oldAttempt := *intent
	oldAttempt.LearnerAttempt = 0
	oldAttempt.LearnerName = intent.RecoveryName + "-learner-0"
	oldAttempt.LearnerUID = "learner-u1-uid"
	oldAttempt.Aborting = true
	oldAbortSpec := automaticRecoveryChildSpec(f.r, state, &oldAttempt)
	f.child.Spec = oldAbortSpec
	f.child.Status.Phase = "Recovering"
	f.child.Status.Membership = &MembershipStatus{Phase: "Aborted"}
	intent.SpecHash = hashJSON(oldAbortSpec)
	retirementRequest := intent.Request
	retirementRequest.OperationID = intent.RecoveryName + "-learner-fence-0"
	retirementRequest.Targets = []FenceTarget{{NodeID: oldAttempt.LearnerName, Pod: oldAttempt.LearnerName, PodUID: oldAttempt.LearnerUID, WALIdentity: "learner-u1-wal"}}
	retirementCalls := 0
	retirementProof, err := (&countingFencer{count: &retirementCalls}).Fence(t.Context(), retirementRequest)
	if err != nil {
		t.Fatalf("build prior exact learner retirement proof: %v", err)
	}
	state.Learners = []learnerIncarnation{
		{SourceClusterID: state.ActiveClusterID, OperationID: intent.Request.OperationID, CredentialName: oldAttempt.LearnerName + "-credentials", CredentialUID: "learner-u1-secret-uid",
			Target: retirementRequest.Targets[0], Retirement: &learnerRetirement{Request: retirementRequest, Proof: *retirementProof}},
		{SourceClusterID: state.ActiveClusterID, OperationID: intent.Request.OperationID, CredentialName: intent.LearnerSecretName, CredentialUID: intent.LearnerSecretUID,
			Target: FenceTarget{NodeID: intent.LearnerName, Pod: intent.LearnerName, PodUID: intent.LearnerUID, WALIdentity: intent.LearnerWAL}},
	}
	for _, voter := range []string{"n1", "n2"} {
		status := f.api.membership[voter]
		status.Pending = false
		f.api.membership[voter] = status
	}
	f.c.HTTP = &http.Client{Transport: escalationProbeTransport(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/recovery/probe" {
			for idx, peer := range f.api.peers {
				if req.URL.Host == strings.TrimPrefix(peer.URL, "http://") {
					return startupRevisionProbeResponse(req, string(f.api.voters[idx].ID)), nil
				}
			}
			return nil, io.ErrUnexpectedEOF
		}
		return http.DefaultTransport.RoundTrip(req)
	})}
	if err := f.c.saveAuto(t.Context(), f.r, state, version); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	learnerPod := supervisedTestPod()
	asObject(learnerPod["metadata"])["name"] = intent.LearnerName
	asObject(learnerPod["metadata"])["uid"] = intent.LearnerUID
	learnerPod["status"] = object{"podIP": "127.0.0.1"}
	learnerMetadata := asObject(learnerPod["metadata"])
	learnerMetadata["annotations"] = object{fencingProtocolAnnotation: "1", fencingRootAnnotation: state.BindingUID, fencingCredentialAnnotation: intent.LearnerSecretUID, ownerAnnotation: intent.Request.OperationID}
	learnerContainer := asObject(list(nested(learnerPod, "spec", "containers"))[0])
	learnerContainer["ports"] = []any{object{"name": "http", "containerPort": float64(port)}}
	base := f.c.Kube.Client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	recoveryPath := f.c.resourcePath(intent.RecoveryName)
	f.c.Kube.Client = &http.Client{Transport: escalationProbeTransport(func(req *http.Request) (*http.Response, error) {
		if req.Method == http.MethodGet && req.URL.Path == f.c.corePath("pods", intent.LearnerName) {
			data, err := json.Marshal(learnerPod)
			if err != nil {
				return nil, err
			}
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(data)), ContentLength: int64(len(data)), Request: req}, nil
		}
		if req.Method == http.MethodPut && req.URL.Path == recoveryPath {
			data, err := io.ReadAll(req.Body)
			if err != nil {
				return nil, err
			}
			var replacement Resource
			if err := json.Unmarshal(data, &replacement); err != nil {
				return nil, err
			}
			*f.child = replacement
			req.Body = io.NopCloser(bytes.NewReader(data))
		}
		return base.RoundTrip(req)
	})}

	revisionSubmitted := false
	for range 5 {
		state, version = f.load(t)
		if err := f.c.advanceAutomatic(t.Context(), f.r, state, version, f.api.sts); err != nil {
			t.Fatalf("advance automatic replacement: %v", err)
		}
		state, _ = f.load(t)
		if f.child.Spec.Membership != nil && !f.child.Spec.Membership.AbortAddition && hashJSON(f.child.Spec) == state.Intent.SpecHash {
			revisionSubmitted = true
		}
		if !revisionSubmitted && !state.Intent.LearnerMissingSince.IsZero() {
			t.Fatalf("new learner health was checked before its exact non-abort child revision: missingSince=%s childAbort=%t childHash=%s authorizedHash=%s",
				state.Intent.LearnerMissingSince, f.child.Spec.Membership.AbortAddition, hashJSON(f.child.Spec), state.Intent.SpecHash)
		}
		if revisionSubmitted && !state.Intent.LearnerMissingSince.IsZero() {
			return
		}
	}
	t.Fatalf("parent did not submit the child revision before recording learner health: child=%+v intent=%+v", f.child.Spec.Membership, state.Intent)
}

func TestAdvanceAutomaticDoesNotDeferReplacedLearnerRetirement(t *testing.T) {
	for _, scenario := range []struct {
		name        string
		childExists bool
	}{
		{name: "before child creation"},
		{name: "old abort child", childExists: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			f := newRetirementEscalationFixture(t)
			state, version := f.load(t)
			intent := state.Intent
			intent.LearnerAttempt = 1
			intent.LearnerName = intent.RecoveryName + "-learner-1"
			intent.LearnerPodName = "rhiza-2"
			intent.LearnerUID = "learner-u1-uid"
			intent.LearnerWAL = "learner-u1-wal"
			intent.LearnerSecretName = sameOrdinalSecretName(state.BindingUID, intent.LearnerUID)
			intent.LearnerSecretUID = "learner-u1-secret-uid"
			intent.LearnerSecretRV = "1"
			intent.LearnerSecretHash = hashJSON(object{"member": "member", "RHIZA_PEER_TOKEN": "token"})
			intent.LearnerMissingSince = time.Time{}
			intent.NoQuorumSince = time.Time{}
			intent.Aborting = false
			intent.PreviousSpecHash = ""
			oldAttempt := *intent
			oldAttempt.LearnerAttempt = 0
			oldAttempt.LearnerName = intent.RecoveryName + "-learner-0"
			oldAttempt.LearnerPodName = intent.LearnerPodName
			oldAttempt.LearnerUID = "learner-u0-uid"
			oldAttempt.LearnerWAL = "learner-u0-wal"
			oldAttempt.LearnerSecretName = sameOrdinalSecretName(state.BindingUID, oldAttempt.LearnerUID)
			oldAttempt.LearnerSecretUID = "learner-u0-secret-uid"
			oldAttempt.Aborting = true
			oldSpec := automaticRecoveryChildSpec(f.r, state, &oldAttempt)
			f.child.Spec = oldSpec
			f.child.Status.Phase = "Recovering"
			f.child.Status.Membership = &MembershipStatus{Phase: "Aborted"}
			intent.SpecHash = hashJSON(oldSpec)
			if !scenario.childExists {
				intent.RecoveryUID = ""
				intent.SpecHash = ""
			}
			retirementRequest := intent.Request
			retirementRequest.OperationID = intent.RecoveryName + "-learner-fence-0"
			retirementRequest.Targets = []FenceTarget{{NodeID: oldAttempt.LearnerName, Pod: oldAttempt.LearnerName, PodUID: oldAttempt.LearnerUID, WALIdentity: oldAttempt.LearnerWAL}}
			retirementCalls := 0
			retirementProof, err := (&countingFencer{count: &retirementCalls}).Fence(t.Context(), retirementRequest)
			if err != nil {
				t.Fatalf("build prior exact learner retirement proof: %v", err)
			}
			state.Learners = []learnerIncarnation{
				{SourceClusterID: state.ActiveClusterID, OperationID: intent.Request.OperationID, CredentialName: oldAttempt.LearnerSecretName, CredentialUID: oldAttempt.LearnerSecretUID,
					Target: retirementRequest.Targets[0], Retirement: &learnerRetirement{Request: retirementRequest, Proof: *retirementProof}},
				{SourceClusterID: state.ActiveClusterID, OperationID: intent.Request.OperationID, CredentialName: intent.LearnerSecretName, CredentialUID: intent.LearnerSecretUID,
					Target: FenceTarget{NodeID: intent.LearnerName, Pod: learnerPhysicalPod(intent), PodUID: intent.LearnerUID, WALIdentity: intent.LearnerWAL}},
			}
			for _, voter := range []string{"n1", "n2"} {
				status := f.api.membership[voter]
				status.Pending = false
				f.api.membership[voter] = status
			}
			f.c.HTTP = &http.Client{Transport: escalationProbeTransport(func(req *http.Request) (*http.Response, error) {
				if req.URL.Path == "/recovery/probe" {
					for idx, peer := range f.api.peers {
						if req.URL.Host == strings.TrimPrefix(peer.URL, "http://") {
							return startupRevisionProbeResponse(req, string(f.api.voters[idx].ID)), nil
						}
					}
					return nil, io.ErrUnexpectedEOF
				}
				return http.DefaultTransport.RoundTrip(req)
			})}
			liveReplacement := supervisedTestPod()
			asObject(liveReplacement["metadata"])["name"] = learnerPhysicalPod(intent)
			asObject(liveReplacement["metadata"])["uid"] = "learner-u2-uid"
			base := f.c.Kube.Client.Transport
			if base == nil {
				base = http.DefaultTransport
			}
			recoveryPath := f.c.resourcePath(intent.RecoveryName)
			childExists := scenario.childExists
			f.c.Kube.Client = &http.Client{Transport: escalationProbeTransport(func(req *http.Request) (*http.Response, error) {
				if req.Method == http.MethodGet && req.URL.Path == f.c.corePath("pods", learnerPhysicalPod(intent)) {
					data, err := json.Marshal(liveReplacement)
					if err != nil {
						return nil, err
					}
					return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(data)), ContentLength: int64(len(data)), Request: req}, nil
				}
				if req.Method == http.MethodGet && req.URL.Path == recoveryPath && !childExists {
					return &http.Response{StatusCode: http.StatusNotFound, Header: make(http.Header), Body: http.NoBody, Request: req}, nil
				}
				if req.Method == http.MethodPost && req.URL.Path == f.c.resourcePath("") && !childExists {
					data, err := io.ReadAll(req.Body)
					if err != nil {
						return nil, err
					}
					var created Resource
					if err := json.Unmarshal(data, &created); err != nil {
						return nil, err
					}
					created.Metadata["uid"], created.Metadata["resourceVersion"] = "new-resource-uid", "1"
					childExists = true
					*f.child = created
					data, err = json.Marshal(created)
					if err != nil {
						return nil, err
					}
					return &http.Response{StatusCode: http.StatusCreated, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(data)), ContentLength: int64(len(data)), Request: req}, nil
				}
				if req.Method == http.MethodPut && req.URL.Path == recoveryPath {
					data, err := io.ReadAll(req.Body)
					if err != nil {
						return nil, err
					}
					var replacement Resource
					if err := json.Unmarshal(data, &replacement); err != nil {
						return nil, err
					}
					*f.child = replacement
					req.Body = io.NopCloser(bytes.NewReader(data))
				}
				return base.RoundTrip(req)
			})}
			if err := f.c.saveAuto(t.Context(), f.r, state, version); err != nil {
				t.Fatal(err)
			}
			if scenario.childExists == (state.Intent.RecoveryUID == "") {
				t.Fatalf("fixture did not preserve the intended child checkpoint: childExists=%t recoveryUID=%q", scenario.childExists, state.Intent.RecoveryUID)
			}
			if state.Learners[1].Target.Pod != "rhiza-2" {
				t.Fatalf("same-ordinal history lost the physical Pod identity: %+v", state.Learners[1])
			}
			initialSTSPuts := f.api.stsPuts
			advance := func() *autoControl {
				t.Helper()
				state, version := f.load(t)
				if err := f.c.advanceAutomatic(t.Context(), f.r, state, version, f.api.sts); err != nil {
					t.Fatalf("advance replaced learner retirement: %v", err)
				}
				state, _ = f.load(t)
				return state
			}
			state = advance()
			if state.Intent.LearnerMissingSince.IsZero() || state.Intent.Aborting {
				t.Fatalf("replaced learner did not enter bounded health grace: %+v", state.Intent)
			}
			state.Intent.LearnerMissingSince = time.Now().Add(-time.Duration(state.Intent.Policy.FailureGraceSeconds+1) * time.Second)
			version = mustAutoVersion(t, f.c, f.r)
			if err := f.c.saveAuto(t.Context(), f.r, state, version); err != nil {
				t.Fatal(err)
			}
			state = advance()
			if !state.Intent.Aborting || state.Intent.LearnerUID != "learner-u1-uid" || state.Intent.LearnerAttempt != 1 {
				t.Fatalf("replacement UID blocked exact U1 retirement reservation: %+v", state.Intent)
			}
			for range 3 {
				state = advance()
				if len(f.fences) != 0 && hashJSON(f.child.Spec) != hashJSON(automaticRecoveryChildSpec(f.r, state, state.Intent)) {
					t.Fatalf("requested fencing before U1's exact abort child was propagated: fences=%d childHash=%s desiredHash=%s", len(f.fences), hashJSON(f.child.Spec), hashJSON(automaticRecoveryChildSpec(f.r, state, state.Intent)))
				}
				if f.child.Spec.Membership.AbortAddition && f.child.Spec.Membership.ReplacementPod == learnerPhysicalPod(intent) && hashJSON(f.child.Spec) == state.Intent.SpecHash {
					break
				}
			}
			if !f.child.Spec.Membership.AbortAddition || f.child.Spec.Membership.ReplacementPod != learnerPhysicalPod(intent) || hashJSON(f.child.Spec) != state.Intent.SpecHash {
				t.Fatalf("exact U1 abort child was not submitted: child=%+v childHash=%s desiredHash=%s intent=%+v", f.child.Spec.Membership, hashJSON(f.child.Spec), hashJSON(automaticRecoveryChildSpec(f.r, state, state.Intent)), state.Intent)
			}
			if state.Intent.LearnerAttempt != 1 || state.Intent.LearnerUID != "learner-u1-uid" || len(f.fences) != 0 || f.api.stsPuts != initialSTSPuts {
				t.Fatalf("replacement learner was adopted or fenced before exact abort propagation: intent=%+v fences=%d stsPuts=%d", state.Intent, len(f.fences), f.api.stsPuts-initialSTSPuts)
			}
		})
	}
}

func TestAutomaticLearnerRequiresCurrentAbortGenerationAcknowledgment(t *testing.T) {
	for _, scenario := range []struct {
		name       string
		oldSpec    bool
		generation int64
		observed   int64
		wantFence  bool
	}{
		{name: "old abort spec and matching old acknowledgment", oldSpec: true, generation: 4, observed: 4},
		{name: "new spec with stale old acknowledgment", generation: 5, observed: 4},
		{name: "new spec with current acknowledgment", generation: 5, observed: 5, wantFence: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			f := newRetirementEscalationFixture(t)
			state, version := f.load(t)
			intent := state.Intent
			intent.LearnerAttempt = 1
			intent.LearnerName = intent.RecoveryName + "-learner-1"
			intent.LearnerPodName = "rhiza-2"
			intent.LearnerUID = "learner-u1-uid"
			intent.LearnerWAL = "learner-u1-wal"
			intent.LearnerMissingSince = time.Time{}
			intent.LearnerSecretName = sameOrdinalSecretName(state.BindingUID, intent.LearnerUID)
			intent.LearnerSecretUID = "learner-u1-secret-uid"
			intent.LearnerSecretRV = "1"
			peerToken := "learner-u1-peer-token"
			member := voterMember("source", intent.LearnerName, peerToken)
			secretData := object{"member": base64.StdEncoding.EncodeToString(jsonBytes(member)), "RHIZA_PEER_TOKEN": base64.StdEncoding.EncodeToString([]byte(peerToken))}
			intent.LearnerSecretHash = hashJSON(secretData)
			f.api.membershipSecrets[intent.LearnerSecretName] = object{
				"immutable": true,
				"metadata":  object{"name": intent.LearnerSecretName, "uid": intent.LearnerSecretUID, "resourceVersion": intent.LearnerSecretRV, "annotations": object{ownerAnnotation: intent.Request.OperationID}},
				"data":      secretData,
			}
			intent.Aborting = true
			desired := automaticRecoveryChildSpec(f.r, state, intent)
			f.child.Metadata["generation"] = float64(scenario.generation)
			f.child.Status.Phase = "Aborted"
			f.child.Status.ObservedGeneration = scenario.observed
			f.child.Status.Membership = &MembershipStatus{Phase: "Aborted"}
			if scenario.oldSpec {
				old := *intent
				old.LearnerAttempt = 0
				old.LearnerName = old.RecoveryName + "-learner-0"
				old.LearnerUID = "learner-u0-uid"
				old.LearnerWAL = "learner-u0-wal"
				old.LearnerSecretName = sameOrdinalSecretName(state.BindingUID, old.LearnerUID)
				f.child.Spec = automaticRecoveryChildSpec(f.r, state, &old)
				intent.SpecHash = hashJSON(f.child.Spec)
			} else {
				f.child.Spec = desired
				intent.SpecHash = hashJSON(desired)
			}
			if err := f.c.saveAuto(t.Context(), f.r, state, version); err != nil {
				t.Fatal(err)
			}

			handled, err := f.c.handleAutomaticLearner(t.Context(), f.r, state, version, f.api.sts, f.child)
			if err != nil {
				t.Fatalf("handle learner abort acknowledgment: %v", err)
			}
			if gotFence := len(f.fences) != 0; gotFence != scenario.wantFence {
				t.Fatalf("fencing with old/current abort acknowledgment = %t, want %t (handled=%t, status=%+v)", gotFence, scenario.wantFence, handled, f.child.Status)
			}
			if scenario.wantFence && !handled {
				t.Fatal("current exact abort acknowledgment did not enter the existing fencing path")
			}
			if !scenario.wantFence && handled {
				t.Fatal("stale or old abort acknowledgment should wait without handling the learner")
			}
		})
	}
}

func TestAutomaticAbortGenerationAcknowledgmentAcrossAPIServerWrites(t *testing.T) {
	f := newRetirementEscalationFixture(t)
	state, version := f.load(t)
	intent := state.Intent
	intent.LearnerAttempt = 1
	intent.LearnerName = intent.RecoveryName + "-learner-1"
	intent.LearnerPodName = "rhiza-2"
	intent.LearnerUID = "learner-u1-uid"
	intent.LearnerWAL = "learner-u1-wal"
	intent.LearnerSecretName = sameOrdinalSecretName(state.BindingUID, intent.LearnerUID)
	intent.LearnerSecretUID = "learner-u1-secret-uid"
	intent.Aborting = true
	state.Learners = []learnerIncarnation{{SourceClusterID: state.ActiveClusterID, OperationID: intent.Request.OperationID,
		CredentialName: intent.LearnerSecretName, CredentialUID: intent.LearnerSecretUID,
		Target: FenceTarget{NodeID: intent.LearnerName, Pod: learnerPhysicalPod(intent), PodUID: intent.LearnerUID, WALIdentity: intent.LearnerWAL}}}
	old := *intent
	old.LearnerAttempt = 0
	old.LearnerName = old.RecoveryName + "-learner-0"
	old.LearnerUID = "learner-u0-uid"
	old.LearnerWAL = "learner-u0-wal"
	old.LearnerSecretName = sameOrdinalSecretName(state.BindingUID, old.LearnerUID)
	f.child.Spec = automaticRecoveryChildSpec(f.r, state, &old)
	f.child.Status = Status{Phase: "Aborted", ObservedGeneration: 4, Membership: &MembershipStatus{Phase: "Aborted"}}
	f.child.Metadata["generation"], f.child.Metadata["resourceVersion"] = float64(4), "1"
	intent.PreviousSpecHash = hashJSON(f.child.Spec)
	intent.SpecHash = hashJSON(automaticRecoveryChildSpec(f.r, state, intent))
	if err := f.c.saveAuto(t.Context(), f.r, state, version); err != nil {
		t.Fatal(err)
	}
	var staleStatus Resource
	staleBytes, err := json.Marshal(f.child)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(staleBytes, &staleStatus); err != nil {
		t.Fatal(err)
	}
	staleStatus.Status.Message = "late old abort status"
	staleStatus.Status.ObservedGeneration = 4
	oldResourceVersion := str(f.child.Metadata["resourceVersion"])
	initialSTSPuts := f.api.stsPuts
	for _, id := range []string{"n1", "n2"} {
		status := f.api.membership[id]
		status.Pending = false
		f.api.membership[id] = status
	}
	f.c.HTTP = &http.Client{Transport: escalationProbeTransport(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/recovery/probe" {
			for idx, peer := range f.api.peers {
				if req.URL.Host == strings.TrimPrefix(peer.URL, "http://") {
					return startupRevisionProbeResponse(req, string(f.api.voters[idx].ID)), nil
				}
			}
			return nil, io.ErrUnexpectedEOF
		}
		return http.DefaultTransport.RoundTrip(req)
	})}
	base := f.c.Kube.Client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	path := f.c.resourcePath(intent.RecoveryName)
	current := *f.child
	response := func(req *http.Request, code int, value any) (*http.Response, error) {
		data, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: code, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(data)), ContentLength: int64(len(data)), Request: req}, nil
	}
	f.c.Kube.Client = &http.Client{Transport: escalationProbeTransport(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == path && req.Method == http.MethodGet {
			return response(req, http.StatusOK, current)
		}
		if req.URL.Path == path && req.Method == http.MethodPut {
			var next Resource
			if err := json.NewDecoder(req.Body).Decode(&next); err != nil {
				return nil, err
			}
			if str(next.Metadata["resourceVersion"]) != str(current.Metadata["resourceVersion"]) {
				return response(req, http.StatusConflict, object{"message": "resourceVersion conflict"})
			}
			if hashJSON(next.Spec) != hashJSON(current.Spec) {
				next.Metadata["generation"] = float64(number(current.Metadata["generation"]) + 1)
			}
			next.Status = current.Status
			next.Metadata["resourceVersion"] = "2"
			current = next
			*f.child = current
			return response(req, http.StatusOK, current)
		}
		if req.URL.Path == path+"/status" && req.Method == http.MethodPut {
			var next Resource
			if err := json.NewDecoder(req.Body).Decode(&next); err != nil {
				return nil, err
			}
			if str(next.Metadata["resourceVersion"]) != str(current.Metadata["resourceVersion"]) {
				return response(req, http.StatusConflict, object{"message": "resourceVersion conflict"})
			}
			current.Status = next.Status
			current.Metadata["resourceVersion"] = "3"
			*f.child = current
			return response(req, http.StatusOK, current)
		}
		if req.Method == http.MethodGet && req.URL.Path == f.c.corePath("pods", "rhiza-2") {
			pod := supervisedTestPod()
			asObject(pod["metadata"])["name"], asObject(pod["metadata"])["uid"] = "rhiza-2", "learner-u2-uid"
			return response(req, http.StatusOK, pod)
		}
		return base.RoundTrip(req)
	})}

	for range 5 {
		state, version = f.load(t)
		if err := f.c.advanceAutomatic(t.Context(), f.r, state, version, f.api.sts); err != nil {
			t.Fatalf("advance child toward U1 abort revision: %v", err)
		}
		state, _ = f.load(t)
		if hashJSON(current.Spec) == state.Intent.SpecHash && current.Spec.Membership.AbortAddition {
			break
		}
	}
	state, _ = f.load(t)
	if !current.Spec.Membership.AbortAddition || hashJSON(current.Spec) != state.Intent.SpecHash || number(current.Metadata["generation"]) != 5 {
		t.Fatalf("parent did not PUT the exact U1 revision: child=%+v intent=%+v", current, state.Intent)
	}
	if str(staleStatus.Metadata["resourceVersion"]) != oldResourceVersion {
		t.Fatalf("stale status fixture lost its original resourceVersion: %v", staleStatus.Metadata["resourceVersion"])
	}
	if current.Status.Phase != "Aborted" || current.Status.Membership == nil || current.Status.Membership.Phase != "Aborted" || current.Status.ObservedGeneration != 4 {
		t.Fatalf("main spec PUT did not preserve the old status ACK: %+v", current.Status)
	}
	if f.api.stsPuts != initialSTSPuts {
		t.Fatalf("abort revision changed StatefulSet: PUTs=%d", f.api.stsPuts-initialSTSPuts)
	}
	if err := f.c.Kube.Put(t.Context(), path+"/status", &staleStatus, &staleStatus); err == nil {
		t.Fatal("late status write with the old resourceVersion unexpectedly succeeded")
	} else {
		var apiErr *APIError
		if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusConflict {
			t.Fatalf("late status write error=%v, want HTTP 409", err)
		}
	}
	if current.Status.ObservedGeneration != 4 || number(current.Metadata["generation"]) != 5 {
		t.Fatalf("stale status PUT changed the stored child: %+v", current)
	}
	state, version = f.load(t)
	if err := f.c.advanceAutomatic(t.Context(), f.r, state, version, f.api.sts); err != nil {
		t.Fatalf("parent advance with stale status: %v", err)
	}
	if len(f.fences) != 0 {
		t.Fatalf("parent fenced U1 from the old status ACK: %+v", f.fences)
	}
	var snapshot Resource
	if err := f.c.Kube.Get(t.Context(), path, &snapshot); err != nil {
		t.Fatalf("read child reconcile snapshot: %v", err)
	}
	if err := f.c.reconcile(t.Context(), &snapshot); err != nil {
		t.Fatalf("child reconcile current abort revision: %v", err)
	}
	if current.Status.Phase != "Aborted" || current.Status.Membership == nil || current.Status.Membership.Phase != "Aborted" || current.Status.ObservedGeneration != 5 {
		t.Fatalf("child did not persist current-generation abort ACK: %+v", current.Status)
	}
	state, version = f.load(t)
	if err := f.c.advanceAutomatic(t.Context(), f.r, state, version, f.api.sts); err != nil {
		t.Fatalf("parent advance with current ACK: %v", err)
	}
	if len(f.fences) != 1 || f.fences[0].Targets[0].Pod != "rhiza-2" || f.fences[0].Targets[0].PodUID != "learner-u1-uid" {
		t.Fatalf("current ACK did not authorize exact U1 physical retirement: %+v", f.fences)
	}
}

func TestAdvanceAutomaticRecoversCreatedChildAfterPostResponseLoss(t *testing.T) {
	for _, scenario := range []struct {
		name       string
		corrupt    string
		checkpoint bool
		aborting   bool
	}{
		{name: "adopt exact created child", checkpoint: true, aborting: true},
		{name: "adopt exact non-abort child before unavailable health", checkpoint: true},
		{name: "reject wrong spec", corrupt: "spec", aborting: true},
		{name: "reject wrong binding", corrupt: "binding", aborting: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			f := newRetirementEscalationFixture(t)
			state, version := f.load(t)
			intent := state.Intent
			intent.LearnerAttempt = 1
			intent.LearnerName = intent.RecoveryName + "-learner-1"
			intent.LearnerPodName = "rhiza-2"
			intent.LearnerUID = "learner-u1-uid"
			intent.LearnerWAL = "learner-u1-wal"
			intent.LearnerMissingSince = time.Time{}
			intent.LearnerSecretName = sameOrdinalSecretName(state.BindingUID, intent.LearnerUID)
			intent.LearnerSecretUID = "learner-u1-secret-uid"
			intent.LearnerSecretRV = "1"
			intent.LearnerSecretHash = hashJSON(object{"member": "member", "RHIZA_PEER_TOKEN": "token"})
			intent.Aborting = scenario.aborting
			intent.NoQuorumSince = time.Time{}
			intent.RecoveryUID, intent.SpecHash = "", ""
			state.Learners = []learnerIncarnation{{SourceClusterID: state.ActiveClusterID, OperationID: intent.Request.OperationID,
				CredentialName: intent.LearnerSecretName, CredentialUID: intent.LearnerSecretUID,
				Target: FenceTarget{NodeID: intent.LearnerName, Pod: learnerPhysicalPod(intent), PodUID: intent.LearnerUID, WALIdentity: intent.LearnerWAL}}}
			if err := f.c.saveAuto(t.Context(), f.r, state, version); err != nil {
				t.Fatal(err)
			}
			for _, id := range []string{"n1", "n2"} {
				status := f.api.membership[id]
				status.Pending = false
				f.api.membership[id] = status
			}
			f.c.HTTP = &http.Client{Transport: escalationProbeTransport(func(req *http.Request) (*http.Response, error) {
				if req.URL.Path == "/recovery/probe" {
					for idx, peer := range f.api.peers {
						if req.URL.Host == strings.TrimPrefix(peer.URL, "http://") {
							return startupRevisionProbeResponse(req, string(f.api.voters[idx].ID)), nil
						}
					}
					return nil, io.ErrUnexpectedEOF
				}
				return http.DefaultTransport.RoundTrip(req)
			})}
			base := f.c.Kube.Client.Transport
			if base == nil {
				base = http.DefaultTransport
			}
			childPath := f.c.resourcePath(intent.RecoveryName)
			var stored Resource
			created := false
			var childRequests []string
			response := func(req *http.Request, code int, value any) (*http.Response, error) {
				data, err := json.Marshal(value)
				if err != nil {
					return nil, err
				}
				return &http.Response{StatusCode: code, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(data)), ContentLength: int64(len(data)), Request: req}, nil
			}
			f.c.Kube.Client = &http.Client{Transport: escalationProbeTransport(func(req *http.Request) (*http.Response, error) {
				childRequests = append(childRequests, req.Method+" "+req.URL.Path)
				if req.URL.Path == childPath && req.Method == http.MethodGet {
					if !created {
						return response(req, http.StatusNotFound, object{"message": "not found"})
					}
					return response(req, http.StatusOK, stored)
				}
				if req.URL.Path == f.c.resourcePath("") && req.Method == http.MethodPost {
					if err := json.NewDecoder(req.Body).Decode(&stored); err != nil {
						return nil, err
					}
					stored.Metadata["uid"], stored.Metadata["resourceVersion"], stored.Metadata["generation"] = "created-child-uid", "1", float64(1)
					switch scenario.corrupt {
					case "spec":
						stored.Spec.Container = "unexpected-container"
					case "binding":
						asObject(stored.Metadata["annotations"])[automaticOwner] = "other-binding"
					}
					created = true
					return nil, io.ErrUnexpectedEOF
				}
				if req.Method == http.MethodGet && req.URL.Path == f.c.corePath("pods", "rhiza-2") {
					pod := supervisedTestPod()
					podUID := "learner-u2-uid"
					if !scenario.aborting {
						podUID = "learner-u1-uid"
					}
					asObject(pod["metadata"])["name"], asObject(pod["metadata"])["uid"] = "rhiza-2", podUID
					return response(req, http.StatusOK, pod)
				}
				return base.RoundTrip(req)
			})}

			state, version = f.load(t)
			if !scenario.aborting {
				posted := Resource{
					APIVersion: "rhiza.mrchypark.dev/v1alpha1", Kind: "RhizaRecovery",
					Metadata: object{"name": intent.RecoveryName, "namespace": f.c.Kube.Namespace, "annotations": object{automaticOwner: state.BindingUID, automaticLogical: f.r.Spec.LogicalID}},
					Spec:     automaticRecoveryChildSpec(f.r, state, state.Intent),
				}
				if err := f.c.Kube.Post(t.Context(), f.c.resourcePath(""), &posted, &posted); err == nil {
					t.Fatal("POST response loss was not surfaced to the first caller")
				}
			} else if err := f.c.advanceAutomatic(t.Context(), f.r, state, version, f.api.sts); err == nil {
				t.Fatal("POST response loss was not surfaced to the first caller")
			}
			state, _ = f.load(t)
			if !created || state.Intent.RecoveryUID != "" || (!scenario.aborting && (state.Intent.SpecHash != "" || state.Intent.LearnerAssignmentID != "")) {
				t.Fatalf("fixture did not model a stored child with a lost parent checkpoint: created=%t intent=%+v", created, state.Intent)
			}
			state, version = f.load(t)
			if err := f.c.advanceAutomatic(t.Context(), f.r, state, version, f.api.sts); err != nil {
				t.Fatalf("retry after child creation response loss: %v", err)
			}
			state, _ = f.load(t)
			if scenario.checkpoint {
				if f.r.Status.Phase == "Blocked" || state.Intent.RecoveryUID != "created-child-uid" || state.Intent.SpecHash != hashJSON(automaticRecoveryChildSpec(f.r, state, state.Intent)) {
					t.Fatalf("parent did not checkpoint the existing correctly-bound child after response loss: phase=%s message=%q child=%s intent=%+v childRequests=%v", f.r.Status.Phase, f.r.Status.Message, stored.Metadata["uid"], state.Intent, childRequests)
				}
				if !scenario.aborting && (state.Intent.Aborting || !state.Intent.LearnerMissingSince.IsZero()) {
					t.Fatalf("health-unavailable child was treated as lost before exact identity checkpoint: intent=%+v", state.Intent)
				}
				if !scenario.aborting {
					podGets := 0
					for _, request := range childRequests {
						if request == "GET "+f.c.corePath("pods", "rhiza-2") {
							podGets++
						}
					}
					if podGets != 0 {
						t.Fatalf("parent rechecked learner health instead of checkpointing the exact child first: podGets=%d requests=%v", podGets, childRequests)
					}
				}
			} else if f.r.Status.Phase != "Blocked" || state.Intent.RecoveryUID != "" || state.Intent.SpecHash != "" {
				t.Fatalf("parent checkpointed a child with invalid %s binding/spec: phase=%s intent=%+v", scenario.corrupt, f.r.Status.Phase, state.Intent)
			}
			if len(f.fences) != 0 {
				t.Fatalf("uncheckpointed child UID authorized a learner fence: %+v", f.fences)
			}
		})
	}
}

func TestBoundClusterReconcilesChildRevisionCheckpoints(t *testing.T) {
	for _, scenario := range []struct {
		name             string
		postResponseLost bool
		reservedRevision bool
	}{
		{name: "stored child after lost POST response", postResponseLost: true},
		{name: "reserved spec hash before child PUT", reservedRevision: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			c, r, sts := boundClusterFixture(t, voterMembers(3))
			voters, err := derivedVoters(r, sts)
			if err != nil {
				t.Fatal(err)
			}
			state := &autoControl{
				Version: 1, BindingUID: str(r.Metadata["uid"]), BindingHash: autoBinding(r), BindingVersion: autoBindingVersion,
				StatefulSetUID: str(nested(sts, "metadata", "uid")), ActiveClusterID: r.Spec.AdoptClusterID,
				Voters: voters, Identities: map[quepaxa.NodeID]FenceTarget{},
			}
			request := FenceRequest{
				LogicalID: r.Spec.LogicalID, BindingUID: state.BindingUID, Namespace: c.Kube.Namespace,
				StatefulSet: r.Spec.StatefulSet, StatefulSetUID: state.StatefulSetUID, SourceClusterID: state.ActiveClusterID,
				OperationID: "outer-revision-operation", Scope: ScopeGeneration,
			}
			for _, voter := range voters {
				request.Targets = append(request.Targets, FenceTarget{NodeID: string(voter.NodeID), Pod: voter.Pod})
			}
			requestDigest, err := requestHash(&request)
			if err != nil {
				t.Fatal(err)
			}
			proof := &FenceProof{
				OperationID: request.OperationID, SourceClusterID: request.SourceClusterID, BindingUID: request.BindingUID,
				RequestHash: requestDigest, ProofID: "outer-revision-proof", ProcessesTerminated: true,
				RecreationBlocked: true, StorageQuiesced: true,
			}
			state.Intent = &autoIntent{
				Request: request, Proof: proof, RecoveryName: "outer-revision-child", Durability: "before-ack",
				Policy: *r.Spec.Policy, Voters: append([]VoterPod(nil), voters...),
			}
			desired := automaticRecoveryChildSpec(r, state, state.Intent)
			childUID := "outer-revision-child-uid"
			if scenario.reservedRevision {
				state.Intent.RecoveryUID = childUID
				state.Intent.SpecHash = hashJSON(desired)
				state.Intent.PreviousSpecHash = hashJSON(Spec{StatefulSet: r.Spec.StatefulSet, Container: "previous", SourceClusterID: state.ActiveClusterID, Durability: "before-ack", RecoveryID: state.Intent.RecoveryName, Fence: desired.Fence})
			}
			if err := c.saveAuto(t.Context(), r, state, nil); err != nil {
				t.Fatal(err)
			}
			stored := Resource{}
			created := false
			if scenario.reservedRevision {
				stored = Resource{
					APIVersion: "rhiza.mrchypark.dev/v1alpha1", Kind: "RhizaRecovery",
					Metadata: object{"name": state.Intent.RecoveryName, "namespace": c.Kube.Namespace, "uid": childUID, "resourceVersion": "1", "generation": float64(1), "annotations": object{automaticOwner: state.BindingUID, automaticLogical: r.Spec.LogicalID}},
					Spec:     Spec{StatefulSet: r.Spec.StatefulSet, Container: "previous", SourceClusterID: state.ActiveClusterID, Durability: "before-ack", RecoveryID: state.Intent.RecoveryName, Fence: desired.Fence},
				}
				created = true
			}
			var childPuts int
			response := func(req *http.Request, code int, value any) (*http.Response, error) {
				data, err := json.Marshal(value)
				if err != nil {
					return nil, err
				}
				return &http.Response{StatusCode: code, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(data)), ContentLength: int64(len(data)), Request: req}, nil
			}
			base := c.Kube.Client.Transport
			c.Kube.Client.Transport = escalationProbeTransport(func(req *http.Request) (*http.Response, error) {
				if req.URL.Path == c.resourcePath(state.Intent.RecoveryName) {
					switch req.Method {
					case http.MethodGet:
						if !created {
							return response(req, http.StatusNotFound, object{"message": "not found"})
						}
						return response(req, http.StatusOK, stored)
					case http.MethodPut:
						if err := json.NewDecoder(req.Body).Decode(&stored); err != nil {
							return nil, err
						}
						stored.Metadata["resourceVersion"] = "2"
						childPuts++
						return response(req, http.StatusOK, stored)
					}
				}
				if req.URL.Path == c.resourcePath("") && req.Method == http.MethodPost {
					if err := json.NewDecoder(req.Body).Decode(&stored); err != nil {
						return nil, err
					}
					stored.Metadata["uid"], stored.Metadata["resourceVersion"], stored.Metadata["generation"] = childUID, "1", float64(1)
					created = true
					return nil, io.ErrUnexpectedEOF
				}
				return base.RoundTrip(req)
			})
			c.Fencer = outerRevisionTestFencer(func(context.Context, FenceRequest) (*FenceProof, error) {
				return proof, nil
			})

			if scenario.postResponseLost {
				if err := c.reconcileCluster(t.Context(), r); err == nil || !created || childPuts != 0 {
					t.Fatalf("first reconcile did not model POST response loss: err=%v created=%t puts=%d", err, created, childPuts)
				}
			}
			err = c.reconcileCluster(t.Context(), r)
			if err != nil {
				t.Fatalf("outer reconcile: %v", err)
			}
			loaded, _, err := c.loadAuto(t.Context(), c.autoKey(r))
			if err != nil {
				t.Fatal(err)
			}
			if r.Status.Phase == "Blocked" {
				t.Fatalf("outer reconcile blocked child revision progress: message=%q intent=%+v", r.Status.Message, loaded.Intent)
			}
			if loaded.Intent.RecoveryUID != childUID || loaded.Intent.SpecHash != hashJSON(desired) {
				t.Fatalf("outer reconcile did not preserve exact child UID/spec checkpoint: intent=%+v", loaded.Intent)
			}
			if scenario.reservedRevision && (childPuts != 1 || hashJSON(stored.Spec) != hashJSON(desired)) {
				t.Fatalf("reserved child revision was not submitted exactly once: puts=%d spec=%+v", childPuts, stored.Spec)
			}
		})
	}
}

func TestAdvanceAutomaticDoesNotPutRecreatedChildUID(t *testing.T) {
	f := newRetirementEscalationFixture(t)
	state, version := f.load(t)
	state.Intent.NoQuorumSince = time.Time{}
	for _, voter := range []string{"n1", "n2"} {
		status := f.api.membership[voter]
		status.Pending = false
		f.api.membership[voter] = status
	}
	f.child.Spec.Membership.AbortAddition = true
	f.child.Status.Phase = "Complete"
	previous := hashJSON(f.child.Spec)
	desired := automaticRecoveryChildSpec(f.r, state, state.Intent)
	state.Intent.SpecHash = hashJSON(desired)
	state.Intent.PreviousSpecHash = previous
	if err := f.c.saveAuto(t.Context(), f.r, state, version); err != nil {
		t.Fatal(err)
	}

	base := f.c.Kube.Client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	f.c.HTTP = &http.Client{Transport: escalationProbeTransport(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/recovery/probe" {
			for idx, peer := range f.api.peers {
				if req.URL.Host == strings.TrimPrefix(peer.URL, "http://") {
					return startupRevisionProbeResponse(req, string(f.api.voters[idx].ID)), nil
				}
			}
			return nil, io.ErrUnexpectedEOF
		}
		return http.DefaultTransport.RoundTrip(req)
	})}
	path := f.c.resourcePath(state.Intent.RecoveryName)
	childGets, childPuts := 0, 0
	f.c.Kube.Client = &http.Client{Transport: escalationProbeTransport(func(req *http.Request) (*http.Response, error) {
		if req.Method == http.MethodGet && req.URL.Path == path {
			childGets++
			if childGets == 2 {
				var recreated Resource
				data, err := json.Marshal(f.child)
				if err != nil {
					return nil, err
				}
				if err := json.Unmarshal(data, &recreated); err != nil {
					return nil, err
				}
				recreated.Metadata["uid"] = "recreated-resource-uid"
				data, err = json.Marshal(recreated)
				if err != nil {
					return nil, err
				}
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(data)), ContentLength: int64(len(data)), Request: req}, nil
			}
		}
		if req.Method == http.MethodPut && req.URL.Path == path {
			childPuts++
		}
		return base.RoundTrip(req)
	})}

	if err := f.c.advanceAutomatic(t.Context(), f.r, state, version, f.api.sts); err != nil {
		t.Fatalf("advance automatic with recreated child: %v", err)
	}
	if f.r.Status.Phase != "Blocked" || !strings.Contains(f.r.Status.Message, "recovery resource UID changed") {
		t.Fatalf("expected recreated child UID to be blocked before mutation, got %+v", f.r.Status)
	}
	if childGets < 2 {
		t.Fatalf("race fixture did not reach revision read: child GETs=%d", childGets)
	}
	if childPuts != 0 {
		t.Fatalf("recreated child was mutated before UID rejection: PUTs=%d", childPuts)
	}
}
