package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/thanos-io/objstore"
)

type mixedControlTestBucket struct {
	data     []byte
	versions []objstore.ObjectVersion
	attrs    int
}

type mixedControlMapBucket struct {
	data     map[string][]byte
	versions map[string]objstore.ObjectVersion
}

func (b *mixedControlMapBucket) Attributes(_ context.Context, key string) (objstore.ObjectAttributes, error) {
	version, ok := b.versions[key]
	if !ok {
		return objstore.ObjectAttributes{}, fmt.Errorf("missing version for %s", key)
	}
	return objstore.ObjectAttributes{Version: &version}, nil
}

func (b *mixedControlMapBucket) Get(_ context.Context, key string) (io.ReadCloser, error) {
	data, ok := b.data[key]
	if !ok {
		return nil, fmt.Errorf("missing object %s", key)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (b *mixedControlTestBucket) Attributes(context.Context, string) (objstore.ObjectAttributes, error) {
	i := b.attrs
	b.attrs++
	if i >= len(b.versions) {
		i = len(b.versions) - 1
	}
	v := b.versions[i]
	return objstore.ObjectAttributes{Version: &v}, nil
}

func (b *mixedControlTestBucket) Get(context.Context, string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(b.data)), nil
}

func TestGCSMixedAutoControlPreflightPinsExactNonAbortingIntent(t *testing.T) {
	request := operator.FenceRequest{OperationID: "source-fence", SourceClusterID: "source", StatefulSetUID: "sts-uid"}
	source := gcsRecoverySource{Namespace: "ns", LogicalID: "logical", BindingUID: "binding", StatefulSetUID: "sts-uid", ClusterID: "source"}
	recovery := operator.Resource{Metadata: map[string]any{"name": "recovery"}, Spec: operator.Spec{
		StatefulSet: "rhiza", SourceClusterID: "source", RecoveryID: "source-fence",
		Membership: &operator.MembershipSpec{ReplacementPod: "rhiza-2", ReplacementSecret: "learner-credential"},
	}}
	checkpoint := gcsMixedPendingCheckpoint{Recovery: recovery, RecoveryUID: "recovery-uid", CredentialUID: "credential-uid",
		CredentialRV: "17", CredentialHash: strings.Repeat("a", 64),
		Learner:   operator.FenceTarget{NodeID: "rhiza-2", Pod: "rhiza-2", PodUID: "learner-pod-uid"},
		LearnerID: fencing.Identity{NodeID: "logical-learner", RunID: "old-run", AssignmentID: strings.Repeat("b", 64)}}
	intent := gcsMixedAutoIntentProjection{Request: request, RecoveryName: "recovery", RecoveryUID: checkpoint.RecoveryUID,
		SpecHash: digestJSON(recovery.Spec), LearnerName: "logical-learner", LearnerPodName: "rhiza-2", LearnerUID: "learner-pod-uid",
		LearnerSecretName: "learner-credential", LearnerSecretUID: checkpoint.CredentialUID, LearnerSecretRV: checkpoint.CredentialRV,
		LearnerSecretHash: checkpoint.CredentialHash, LearnerAssignmentID: checkpoint.LearnerID.AssignmentID}
	control := gcsMixedAutoControlProjection{Version: 1, BindingUID: source.BindingUID, StatefulSetUID: source.StatefulSetUID,
		ActiveClusterID: source.ClusterID, Intent: &intent}
	encode := func(value gcsMixedAutoControlProjection) []byte {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	valid := encode(control)
	if digest, err := validateGCSMixedAutoControl(valid, gcsMixedAutoControlExpectation{Source: source, Checkpoint: checkpoint, FenceRequest: request}); err != nil || digest == "" {
		t.Fatalf("exact non-aborting control intent rejected: digest=%q err=%v", digest, err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*gcsMixedAutoControlProjection)
	}{
		{name: "missing intent", mutate: func(c *gcsMixedAutoControlProjection) { c.Intent = nil }},
		{name: "aborting intent", mutate: func(c *gcsMixedAutoControlProjection) { c.Intent.Aborting = true }},
		{name: "changed recovery uid", mutate: func(c *gcsMixedAutoControlProjection) { c.Intent.RecoveryUID = "other-uid" }},
		{name: "changed learner credential", mutate: func(c *gcsMixedAutoControlProjection) { c.Intent.LearnerSecretUID = "other-credential" }},
		{name: "changed assignment", mutate: func(c *gcsMixedAutoControlProjection) { c.Intent.LearnerAssignmentID = "other-assignment" }},
		{name: "changed request", mutate: func(c *gcsMixedAutoControlProjection) { c.Intent.Request.OperationID = "other-fence" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mutated := control
			intentCopy := *control.Intent
			mutated.Intent = &intentCopy
			tc.mutate(&mutated)
			if _, err := validateGCSMixedAutoControl(encode(mutated), gcsMixedAutoControlExpectation{Source: source, Checkpoint: checkpoint, FenceRequest: request}); err == nil {
				t.Fatal("accepted missing, aborting, or changed canonical intent")
			}
		})
	}
	bucket := &mixedControlTestBucket{data: valid, versions: []objstore.ObjectVersion{{Type: objstore.Generation, Value: "1"}, {Type: objstore.Generation, Value: "2"}}}
	if _, _, err := readGCSMixedAutoControl(t.Context(), bucket, "prefix/automatic/ns/logical/control.json",
		gcsMixedAutoControlExpectation{Source: source, Checkpoint: checkpoint, FenceRequest: request}); err == nil {
		t.Fatal("accepted automatic control snapshot whose version changed during read")
	}
}

func TestGCSMixedEscalationControlPinsFullSupersededIntentAndAllowsNoQuorumProgress(t *testing.T) {
	oldRequest := operator.FenceRequest{OperationID: "old-voter-op", Scope: operator.ScopeVoter, SourceClusterID: "source",
		BindingUID: "binding", StatefulSetUID: "sts-uid", LogicalID: "logical", Namespace: "ns", StatefulSet: "rhiza"}
	newRequest := oldRequest
	newRequest.OperationID, newRequest.Scope = "auto-generation", operator.ScopeGeneration
	newRequest.Targets = []operator.FenceTarget{{NodeID: "voter", Pod: "voter-pod", PodUID: "voter-uid", WALIdentity: "voter-wal"}}
	source := gcsRecoverySource{Namespace: "ns", LogicalID: "logical", BindingUID: "binding", StatefulSetUID: "sts-uid", ClusterID: "source"}
	recovery := operator.Resource{Metadata: map[string]any{"name": "old-recovery"}, Spec: operator.Spec{StatefulSet: "rhiza", SourceClusterID: "source",
		RecoveryID: "old-voter-op", Membership: &operator.MembershipSpec{ReplacementPod: "rhiza-2", ReplacementSecret: "learner-secret"}}}
	checkpoint := gcsMixedPendingCheckpoint{Recovery: recovery, RecoveryUID: "old-child-uid", CredentialUID: "credential-uid", CredentialRV: "9",
		CredentialHash: strings.Repeat("a", 64), Learner: operator.FenceTarget{NodeID: "learner", Pod: "rhiza-2", PodUID: "learner-pod-uid"},
		LearnerID: fencing.Identity{NodeID: "learner", PodName: "rhiza-2", PodUID: "learner-pod-uid", AssignmentID: strings.Repeat("b", 64)}}
	checkpoint.LearnerRequestTarget = operator.FenceTarget{NodeID: checkpoint.LearnerID.NodeID, Pod: checkpoint.LearnerID.PodName,
		PodUID: checkpoint.LearnerID.PodUID}
	policy := operator.AutoPolicy{FailureGraceSeconds: 30, CooldownSeconds: 60, MaxRecoveriesPerDay: 3}
	voters := []operator.VoterPod{{NodeID: "voter-0", Pod: "rhiza-0"}, {NodeID: "voter-1", Pod: "rhiza-1"}, {NodeID: "voter-2", Pod: "rhiza-2"}}
	initial := gcsMixedAutoIntentProjection{Request: oldRequest, Policy: policy, Durability: "before-ack", Voters: voters,
		RecoveryName: "old-recovery", RecoveryUID: checkpoint.RecoveryUID, SpecHash: digestJSON(recovery.Spec), LearnerName: checkpoint.LearnerID.NodeID,
		LearnerPodName: checkpoint.Learner.Pod, LearnerUID: checkpoint.Learner.PodUID, LearnerSecretName: "learner-secret",
		LearnerSecretUID: checkpoint.CredentialUID, LearnerSecretRV: checkpoint.CredentialRV, LearnerSecretHash: checkpoint.CredentialHash,
		LearnerAssignmentID: checkpoint.LearnerID.AssignmentID}
	old := initial
	old.NoQuorumSince = time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	oldBytes, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	oldContentHash := sha256.Sum256(oldBytes)
	supersedes := initial.RecoveryName + "-" + fmt.Sprintf("%x", oldContentHash[:])
	current := gcsMixedAutoControlProjection{Version: 1, BindingUID: source.BindingUID, StatefulSetUID: source.StatefulSetUID,
		ActiveClusterID: source.ClusterID, Intent: &gcsMixedAutoIntentProjection{Request: newRequest, Policy: policy, Durability: "before-ack",
			Voters: voters, RecoveryName: newRequest.OperationID, Supersedes: supersedes},
		Learners: []gcsMixedLearnerHistoryProjection{{SourceClusterID: source.ClusterID, OperationID: oldRequest.OperationID,
			CredentialName: "learner-secret", CredentialUID: checkpoint.CredentialUID, Target: checkpoint.LearnerRequestTarget}}}
	currentBytes, err := json.Marshal(current)
	if err != nil {
		t.Fatal(err)
	}
	controlKey := "prefix/automatic/ns/logical/control.json"
	oldKey := "prefix/automatic/ns/logical/superseded/" + supersedes + ".json"
	bucket := &mixedControlMapBucket{data: map[string][]byte{controlKey: currentBytes, oldKey: oldBytes}, versions: map[string]objstore.ObjectVersion{
		controlKey: {Type: objstore.Generation, Value: "21"}, oldKey: {Type: objstore.Generation, Value: "11"}}}
	version, digest, oldVersion, gotSupersedes, oldDigest, err := verifyGCSMixedEscalatedControl(t.Context(), bucket, controlKey, "prefix", source, checkpoint,
		initial, oldRequest, newRequest)
	if err != nil || version.Value != "21" || digest == "" || oldVersion != "11" || gotSupersedes != supersedes || oldDigest != fmt.Sprintf("%x", oldContentHash[:]) {
		t.Fatalf("accepted canonical supersession with legitimate NoQuorumSince progression incorrectly: version=%+v digest=%q old=%q supersedes=%q oldDigest=%q err=%v", version, digest, oldVersion, gotSupersedes, oldDigest, err)
	}
	for _, tc := range []struct {
		name            string
		initialNoQuorum time.Time
		oldNoQuorum     time.Time
		wantReject      bool
	}{
		{name: "prepartition zero advances to quorum-loss timestamp", initialNoQuorum: time.Time{}, oldNoQuorum: old.NoQuorumSince},
		{name: "nonzero quorum-loss timestamp advances", initialNoQuorum: time.Date(2026, 10, 25, 11, 55, 0, 0, time.UTC), oldNoQuorum: old.NoQuorumSince},
		{name: "superseded timestamp zero from prepartition zero", initialNoQuorum: time.Time{}, oldNoQuorum: time.Time{}, wantReject: true},
		{name: "superseded timestamp zero from nonzero initial", initialNoQuorum: time.Date(2026, 10, 25, 11, 55, 0, 0, time.UTC), oldNoQuorum: time.Time{}, wantReject: true},
		{name: "quorum-loss evidence regressed", initialNoQuorum: old.NoQuorumSince, oldNoQuorum: time.Date(2026, 10, 25, 11, 55, 0, 0, time.UTC), wantReject: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidateInitial := initial
			candidateInitial.NoQuorumSince = tc.initialNoQuorum
			candidateOld := candidateInitial
			candidateOld.NoQuorumSince = tc.oldNoQuorum
			candidateBytes, err := json.Marshal(candidateOld)
			if err != nil {
				t.Fatal(err)
			}
			candidateHash := sha256.Sum256(candidateBytes)
			candidateSupersedes := candidateInitial.RecoveryName + "-" + fmt.Sprintf("%x", candidateHash[:])
			candidateIntent := *current.Intent
			candidateIntent.Supersedes = candidateSupersedes
			candidateCurrent := current
			candidateCurrent.Intent = &candidateIntent
			candidateControl, err := json.Marshal(candidateCurrent)
			if err != nil {
				t.Fatal(err)
			}
			candidateKey := "prefix/automatic/ns/logical/superseded/" + candidateSupersedes + ".json"
			bucket.data[controlKey] = candidateControl
			bucket.data[candidateKey] = candidateBytes
			bucket.versions[controlKey] = objstore.ObjectVersion{Type: objstore.Generation, Value: "23"}
			bucket.versions[candidateKey] = objstore.ObjectVersion{Type: objstore.Generation, Value: "13"}
			_, _, _, _, _, err = verifyGCSMixedEscalatedControl(t.Context(), bucket, controlKey, "prefix", source, checkpoint,
				candidateInitial, oldRequest, newRequest)
			if tc.wantReject && err == nil {
				t.Fatal("accepted a superseded intent without valid nonzero monotonic quorum-loss evidence")
			}
			if !tc.wantReject && err != nil {
				t.Fatalf("rejected valid prepartition/quorum-loss timestamp progression: %v", err)
			}
		})
	}
	changed := old
	changed.LearnerSecretUID = "replaced-credential"
	changedBytes, err := json.Marshal(changed)
	if err != nil {
		t.Fatal(err)
	}
	changedHash := sha256.Sum256(changedBytes)
	changedSupersedes := initial.RecoveryName + "-" + fmt.Sprintf("%x", changedHash[:])
	changedCurrent := current
	changedIntent := *current.Intent
	changedIntent.Supersedes = changedSupersedes
	changedCurrent.Intent = &changedIntent
	changedControlBytes, err := json.Marshal(changedCurrent)
	if err != nil {
		t.Fatal(err)
	}
	bucket.data[controlKey] = changedControlBytes
	bucket.data["prefix/automatic/ns/logical/superseded/"+changedSupersedes+".json"] = changedBytes
	bucket.versions["prefix/automatic/ns/logical/superseded/"+changedSupersedes+".json"] = objstore.ObjectVersion{Type: objstore.Generation, Value: "12"}
	bucket.versions[controlKey] = objstore.ObjectVersion{Type: objstore.Generation, Value: "22"}
	if _, _, _, _, _, err := verifyGCSMixedEscalatedControl(t.Context(), bucket, controlKey, "prefix", source, checkpoint, initial, oldRequest, newRequest); err == nil {
		t.Fatal("accepted an altered historical learner credential in the immutable Supersedes payload")
	}
}

func TestGCSMixedProofAndDenialJoinAcceptsEitherOrderAndJoinsFailures(t *testing.T) {
	proof := gcsMixedProofWaitResult{Request: operator.FenceRequest{OperationID: "generation"}, Proof: operator.FenceProof{ProofID: "proof"}, Observed: "fence"}
	denial := gcsMixedDenialWaitResult{ErrClosedRow: "client: closed", Event: gcsStartupDenialEvent{AttemptedRunID: strings.Repeat("a", 32)}}
	for _, tc := range []struct {
		name       string
		proofFirst bool
	}{
		{name: "proof completes before denial", proofFirst: true},
		{name: "denial completes before proof", proofFirst: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proofGate, denialGate := make(chan struct{}), make(chan struct{})
			proofReturned, denialReturned := make(chan struct{}), make(chan struct{})
			joined := make(chan struct {
				proof  gcsMixedProofWaitResult
				denial gcsMixedDenialWaitResult
				err    error
			}, 1)
			go func() {
				gotProof, gotDenial, err := waitForGCSMixedProofAndDenial(t.Context(),
					func(ctx context.Context) gcsMixedProofWaitResult {
						select {
						case <-proofGate:
							close(proofReturned)
							return proof
						case <-ctx.Done():
							close(proofReturned)
							return gcsMixedProofWaitResult{Err: ctx.Err()}
						}
					},
					func(ctx context.Context) gcsMixedDenialWaitResult {
						select {
						case <-denialGate:
							close(denialReturned)
							return denial
						case <-ctx.Done():
							close(denialReturned)
							return gcsMixedDenialWaitResult{Err: ctx.Err()}
						}
					})
				joined <- struct {
					proof  gcsMixedProofWaitResult
					denial gcsMixedDenialWaitResult
					err    error
				}{gotProof, gotDenial, err}
			}()
			firstReturned, secondReturned := proofReturned, denialReturned
			firstGate, secondGate := proofGate, denialGate
			if !tc.proofFirst {
				firstReturned, secondReturned = denialReturned, proofReturned
				firstGate, secondGate = denialGate, proofGate
			}
			close(firstGate)
			select {
			case <-firstReturned:
			case <-time.After(time.Second):
				t.Fatal("first observation did not complete")
			}
			select {
			case result := <-joined:
				t.Fatalf("join returned before the second observation: %+v", result)
			default:
			}
			close(secondGate)
			select {
			case result := <-joined:
				if result.err != nil || result.proof.Proof.ProofID != proof.Proof.ProofID || result.denial.Event.AttemptedRunID != denial.Event.AttemptedRunID {
					t.Fatalf("join lost one of the exact observations: %+v", result)
				}
			case <-time.After(time.Second):
				t.Fatal("join did not finish after both observations")
			}
			select {
			case <-secondReturned:
			default:
				t.Fatal("second observation did not return")
			}
		})
	}
	for _, failProof := range []bool{true, false} {
		name := "denial failure cancels and joins proof"
		if failProof {
			name = "proof failure cancels and joins denial"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			var siblingReturned atomic.Bool
			proofWait := func(ctx context.Context) gcsMixedProofWaitResult {
				if failProof {
					return gcsMixedProofWaitResult{Err: errors.New("proof failure")}
				}
				<-ctx.Done()
				siblingReturned.Store(true)
				return gcsMixedProofWaitResult{Err: ctx.Err()}
			}
			denialWait := func(ctx context.Context) gcsMixedDenialWaitResult {
				if !failProof {
					return gcsMixedDenialWaitResult{Err: errors.New("denial failure")}
				}
				<-ctx.Done()
				siblingReturned.Store(true)
				return gcsMixedDenialWaitResult{Err: ctx.Err()}
			}
			_, _, err := waitForGCSMixedProofAndDenial(ctx, proofWait, denialWait)
			want := "proof failure"
			if !failProof {
				want = "denial failure"
			}
			if err == nil || !strings.Contains(err.Error(), want) || !siblingReturned.Load() {
				t.Fatalf("failure did not cancel and join the sibling waiter: err=%v siblingReturned=%v", err, siblingReturned.Load())
			}
		})
	}
}

func TestGCSMixedEventFileCleanupFailureIsReturned(t *testing.T) {
	good, err := os.CreateTemp(t.TempDir(), "mixed-events-*.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	if err := closeGCSMixedEventFile(good); err != nil {
		t.Fatalf("successful event-file sync/close reported an error: %v", err)
	}
	if err := closeGCSMixedEventFile(good); err == nil {
		t.Fatal("ignored event-file cleanup errors on an already-closed file")
	}
}

func TestGCSMixedLearnerHistoryKeepsRequestProjectionSeparateFromRuntimeEvidence(t *testing.T) {
	request := operator.FenceRequest{OperationID: "source-fence", SourceClusterID: "source", StatefulSetUID: "sts-uid"}
	source := gcsRecoverySource{Namespace: "ns", LogicalID: "logical", BindingUID: "binding", StatefulSetUID: "sts-uid", ClusterID: "source"}
	recovery := operator.Resource{Metadata: map[string]any{"name": "recovery"}, Spec: operator.Spec{StatefulSet: "rhiza", SourceClusterID: "source",
		RecoveryID: "source-fence", Membership: &operator.MembershipSpec{ReplacementPod: "rhiza-2", ReplacementSecret: "learner-credential"}}}
	identity := fencing.Identity{SourceClusterID: "source", NodeID: "logical-learner", PodName: "rhiza-2", PodUID: "learner-uid",
		RunID: "learner-run", WALIdentity: "learner-wal", AssignmentID: strings.Repeat("b", 64)}
	runtime := mixedRuntime(source, identity)
	pod := obj{"metadata": obj{"name": identity.PodName, "uid": identity.PodUID}, "spec": obj{"nodeName": runtime.NodeName},
		"status": obj{"containerStatuses": []any{obj{"name": "rhiza", "containerID": runtime.ContainerID, "imageID": runtime.ImageID}}}}
	// This is the production inventory projection: capturedFenceIdentity fills
	// NodeName/container/image but leaves NodeUID absent.
	requestTarget := capturedTarget(operator.FenceTarget{NodeID: identity.NodeID, Pod: identity.PodName, PodUID: identity.PodUID, WALIdentity: identity.WALIdentity}, pod)
	evidenceTarget, err := captureGCSMixedPendingTarget(identity, runtime, pod, obj{"metadata": obj{"name": runtime.NodeName, "uid": runtime.NodeUID}})
	if err != nil {
		t.Fatal(err)
	}
	if requestTarget.NodeUID != "" || evidenceTarget.NodeUID != runtime.NodeUID {
		t.Fatalf("fixture did not preserve production request/runtime distinction: request=%+v evidence=%+v", requestTarget, evidenceTarget)
	}
	checkpoint := gcsMixedPendingCheckpoint{Recovery: recovery, RecoveryUID: "recovery-uid", CredentialUID: "credential-uid", CredentialRV: "17",
		CredentialHash: strings.Repeat("a", 64), Learner: evidenceTarget, LearnerID: identity}
	intent := gcsMixedAutoIntentProjection{Request: request, RecoveryName: "recovery", RecoveryUID: checkpoint.RecoveryUID,
		SpecHash: digestJSON(recovery.Spec), LearnerName: identity.NodeID, LearnerPodName: identity.PodName, LearnerUID: identity.PodUID,
		LearnerSecretName: "learner-credential", LearnerSecretUID: checkpoint.CredentialUID, LearnerSecretRV: checkpoint.CredentialRV,
		LearnerSecretHash: checkpoint.CredentialHash, LearnerAssignmentID: identity.AssignmentID}
	control := gcsMixedAutoControlProjection{Version: 1, BindingUID: source.BindingUID, StatefulSetUID: source.StatefulSetUID,
		ActiveClusterID: source.ClusterID, Intent: &intent, Learners: []gcsMixedLearnerHistoryProjection{{SourceClusterID: source.ClusterID,
			OperationID: request.OperationID, CredentialName: "learner-credential", CredentialUID: checkpoint.CredentialUID, Target: requestTarget}}}
	data, err := json.Marshal(control)
	if err != nil {
		t.Fatal(err)
	}
	version := objstore.ObjectVersion{Type: objstore.Generation, Value: "7"}
	bucket := &mixedControlTestBucket{data: data, versions: []objstore.ObjectVersion{version, version}}
	got, gotVersion, digest, err := readGCSMixedLearnerRequestTarget(t.Context(), bucket, "prefix/automatic/ns/logical/control.json",
		gcsMixedAutoControlExpectation{Source: source, Checkpoint: checkpoint, FenceRequest: request}, runtime)
	if err != nil {
		t.Fatalf("production-shaped empty-NodeUID request projection rejected: %v", err)
	}
	if got != requestTarget || got.NodeUID != "" || gotVersion != version || digest == "" {
		t.Fatalf("canonical request projection/version/digest changed: target=%+v version=%+v digest=%q", got, gotVersion, digest)
	}
	voters := []operator.FenceTarget{
		mixedTarget("voter-0", "rhiza-0", "voter-uid-0", "wal-0"),
		mixedTarget("voter-1", "rhiza-1", "voter-uid-1", "wal-1"),
		mixedTarget("voter-2", "rhiza-2", "old-ordinal-uid", "wal-2"),
	}
	expectedTargets, err := newGCSMixedPendingTargets(voters, voters[2], evidenceTarget, got, identity, runtime)
	if err != nil {
		t.Fatalf("canonical same-ordinal history projection rejected by target constructor: %v", err)
	}
	if expectedTargets.ExpectedGenerationTargets[3] != requestTarget || expectedTargets.PendingLearnerEvidence.NodeUID != runtime.NodeUID {
		t.Fatalf("generation request did not preserve the exact history projection beside full runtime evidence: %+v", expectedTargets)
	}

	for _, tc := range []struct {
		name    string
		mutate  func(*gcsMixedAutoControlProjection)
		runtime fencing.RuntimeBinding
	}{
		{name: "changed canonical node uid", mutate: func(c *gcsMixedAutoControlProjection) { c.Learners[0].Target.NodeUID = "other-node-uid" }, runtime: runtime},
		{name: "changed canonical container", mutate: func(c *gcsMixedAutoControlProjection) { c.Learners[0].Target.ContainerID = "containerd://other" }, runtime: runtime},
		{name: "changed canonical pod uid", mutate: func(c *gcsMixedAutoControlProjection) { c.Learners[0].Target.PodUID = "other-pod-uid" }, runtime: runtime},
		{name: "changed canonical assignment operation", mutate: func(c *gcsMixedAutoControlProjection) { c.Learners[0].OperationID = "other-operation" }, runtime: runtime},
		{name: "runtime node uid replaced", mutate: func(*gcsMixedAutoControlProjection) {}, runtime: func() fencing.RuntimeBinding { r := runtime; r.NodeUID = "other-node-uid"; return r }()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mutated := control
			intentCopy := *control.Intent
			mutated.Intent = &intentCopy
			mutated.Learners = append([]gcsMixedLearnerHistoryProjection(nil), control.Learners...)
			tc.mutate(&mutated)
			mutatedData, err := json.Marshal(mutated)
			if err != nil {
				t.Fatal(err)
			}
			versions := []objstore.ObjectVersion{version, version}
			mutatedBucket := &mixedControlTestBucket{data: mutatedData, versions: versions}
			if _, _, _, err := readGCSMixedLearnerRequestTarget(t.Context(), mutatedBucket, "key",
				gcsMixedAutoControlExpectation{Source: source, Checkpoint: checkpoint, FenceRequest: request}, tc.runtime); err == nil {
				t.Fatal("accepted changed canonical learner projection or runtime binding")
			}
		})
	}
}

func TestGCSMixedStartupDenialParserRequiresOneExactCorrelatedEvent(t *testing.T) {
	expected := gcsStartupDenialExpectation{Namespace: "ns", StatefulSetUID: "sts-uid", PodUID: "pod-uid", SourceClusterID: "source",
		RequestedNodeID: "rhiza-2", OldRunID: strings.Repeat("b", 32), CanonicalRunIDs: []string{strings.Repeat("b", 32), strings.Repeat("d", 32)}}
	valid := gcsStartupDenialEvent{Event: "startup_registration_denial", Action: "register", Result: "closed", HTTPStatus: 410,
		Stage: "startup-grant", Namespace: expected.Namespace, StatefulSetUID: expected.StatefulSetUID, PodUID: expected.PodUID,
		SourceClusterID: expected.SourceClusterID, AttemptedRunID: strings.Repeat("c", 32), RequestedNodeID: expected.RequestedNodeID,
		GrantResolved: false}
	encode := func(event gcsStartupDenialEvent) []byte {
		encoded, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		return append([]byte(`2026/10/25 12:00:00 `), append(encoded, '\n')...)
	}
	before := []byte(`2026/10/25 11:59:59 preflight` + "\n")
	after := append(append([]byte(nil), before...), encode(valid)...)
	parsed, row, err := parseGCSStartupDenialDelta(before, after, expected)
	if err != nil || parsed.AttemptedRunID != valid.AttemptedRunID || row == "" {
		t.Fatalf("valid event did not correlate: parsed=%+v row=%q err=%v", parsed, row, err)
	}
	for _, tc := range []struct {
		name string
		post []byte
	}{
		{name: "no new event", post: before},
		{name: "wrong stage", post: append(append([]byte(nil), before...), encode(func() gcsStartupDenialEvent { e := valid; e.Stage = "producer-reserve"; return e }())...)},
		{name: "old run reused", post: append(append([]byte(nil), before...), encode(func() gcsStartupDenialEvent { e := valid; e.AttemptedRunID = expected.OldRunID; return e }())...)},
		{name: "resolved grant", post: append(append([]byte(nil), before...), encode(func() gcsStartupDenialEvent { e := valid; e.GrantResolved = true; return e }())...)},
		{name: "resolved identity populated", post: append(append([]byte(nil), before...), encode(func() gcsStartupDenialEvent { e := valid; e.ResolvedNodeID = "logical"; return e }())...)},
		{name: "runtime populated", post: append(append([]byte(nil), before...), encode(func() gcsStartupDenialEvent { e := valid; e.RuntimeContainerID = "containerd://unexpected"; return e }())...)},
		{name: "canonical run reused", post: append(append([]byte(nil), before...), encode(func() gcsStartupDenialEvent { e := valid; e.AttemptedRunID = expected.CanonicalRunIDs[1]; return e }())...)},
		{name: "duplicate event", post: append(append([]byte(nil), before...), append(encode(valid), encode(valid)...)...)},
		{name: "log rotated", post: encode(valid)},
		{name: "malformed event", post: append(append([]byte(nil), before...), []byte(`2026/10/25 12:00:00 {"event":"startup_registration_denial",bad}`+"\n")...)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := parseGCSStartupDenialDelta(before, tc.post, expected); err == nil {
				t.Fatal("accepted missing, stale, ambiguous, or malformed denial evidence")
			}
		})
	}
	if _, _, err := parseGCSStartupDenialDelta(before, before, expected); !errors.Is(err, errGCSStartupDenialPending) {
		t.Fatalf("unchanged complete snapshot was not classified as pending: %v", err)
	}
	incomplete := append(append([]byte(nil), before...), []byte("2026/10/25 12:00:00 partial")...)
	if _, _, err := parseGCSStartupDenialDelta(before, incomplete, expected); err == nil || errors.Is(err, errGCSStartupDenialPending) {
		t.Fatalf("incomplete trailing row did not fail closed: %v", err)
	}
	otherPod := valid
	otherPod.PodUID = "another-voter-pod"
	otherPod.AttemptedRunID = strings.Repeat("e", 32)
	if _, _, err := parseGCSStartupDenialDelta(before, append(append([]byte(nil), before...), encode(otherPod)...), expected); !errors.Is(err, errGCSStartupDenialPending) {
		t.Fatalf("complete nonqualifying event was not classified as pending: %v", err)
	}
	if _, _, err := parseGCSStartupDenialDelta(before, append(append([]byte(nil), before...), append(encode(otherPod), encode(valid)...)...), expected); err != nil {
		t.Fatalf("unrelated voter denial should be filtered before exact learner candidate count: %v", err)
	}
	if _, _, err := parseGCSStartupDenialDelta(before, append(append([]byte(nil), before...), append(encode(valid), encode(otherPod)...)...), expected); err != nil {
		t.Fatalf("learner denial should remain the sole candidate when an unrelated voter event follows: %v", err)
	}
	otherPodWithOptional := otherPod
	otherPodWithOptional.ResolvedNodeID = "other-logical-node"
	otherPodWithOptional.AssignmentID = strings.Repeat("f", 64)
	otherPodWithOptional.RuntimeContainerID = "containerd://other-runtime"
	if _, _, err := parseGCSStartupDenialDelta(before, append(append([]byte(nil), before...), append(encode(otherPodWithOptional), encode(valid)...)...), expected); err != nil {
		t.Fatalf("learner row inherited optional fields from preceding voter row: %v", err)
	}
	if _, _, err := parseGCSStartupDenialDelta(before, append(append([]byte(nil), before...), append(encode(valid), encode(otherPodWithOptional)...)...), expected); err != nil {
		t.Fatalf("learner event changed when optional-field voter row followed it: %v", err)
	}
	missingPod := valid
	missingPod.PodUID = ""
	if _, _, err := parseGCSStartupDenialDelta(before, append(append([]byte(nil), before...), encode(missingPod)...), expected); err == nil {
		t.Fatal("accepted a denial event whose PodUID could not be safely filtered")
	}
	if _, _, err := parseGCSStartupDenialDelta(before, append(append([]byte(nil), before...), append(encode(valid), encode(valid)...)...), expected); err == nil {
		t.Fatal("accepted two denial events for the exact learner PodUID")
	}
}

func TestGCSMixedNaturalDeniedStartupWaitsAcrossUnchangedLogsUntilOneDenial(t *testing.T) {
	fixture := installGCSMixedFakeKubectl(t, true)
	oldNamespace := namespace
	namespace = "ns"
	t.Cleanup(func() { namespace = oldNamespace })
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	source := gcsRecoverySource{Namespace: "ns", StatefulSetUID: "sts-uid", Container: "rhiza", FencerPod: "authority-pod"}
	pinned, err := captureGCSAuthorityLogSource(ctx, source)
	if err != nil {
		t.Fatalf("pin fake authority source: %v", err)
	}
	expected := gcsStartupDenialExpectation{Namespace: "ns", StatefulSetUID: "sts-uid", PodUID: "learner-pod-uid",
		SourceClusterID: "source", RequestedNodeID: "rhiza-2", OldRunID: strings.Repeat("b", 32), CanonicalRunIDs: []string{strings.Repeat("b", 32)}}
	logs, errClosed, terminated, event, postflight, err := waitForGCSMixedNaturalDeniedStartup(ctx, source, pinned, fixture.preflight,
		expected, 0, "containerd://c0", fixture.imageID, fixture.podSpecHash)
	if err != nil {
		t.Fatalf("waiter failed before one later natural denial: %v", err)
	}
	if logs == nil || errClosed == "" || terminated != "containerd://c1" || event.AttemptedRunID != strings.Repeat("c", 32) ||
		!bytes.Equal(postflight, append(append([]byte(nil), fixture.preflight...), fixture.denialRow...)) {
		t.Fatalf("waiter returned an incomplete denial tuple: logs=%q errClosed=%q terminated=%q event=%+v postflight=%q",
			logs, errClosed, terminated, event, postflight)
	}
	if reads := fixture.logReads(); reads < 4 {
		t.Fatalf("waiter did not poll several times across unchanged logs: authority reads=%d", reads)
	}
}

func TestGCSMixedNaturalDeniedStartupWithoutEventFailsAtDeadline(t *testing.T) {
	fixture := installGCSMixedFakeKubectl(t, false)
	oldNamespace := namespace
	namespace = "ns"
	t.Cleanup(func() { namespace = oldNamespace })
	source := gcsRecoverySource{Namespace: "ns", StatefulSetUID: "sts-uid", Container: "rhiza", FencerPod: "authority-pod"}
	pinned, err := captureGCSAuthorityLogSource(t.Context(), source)
	if err != nil {
		t.Fatalf("pin fake authority source: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 1200*time.Millisecond)
	defer cancel()
	expected := gcsStartupDenialExpectation{Namespace: "ns", StatefulSetUID: "sts-uid", PodUID: "learner-pod-uid",
		SourceClusterID: "source", RequestedNodeID: "rhiza-2", OldRunID: strings.Repeat("b", 32), CanonicalRunIDs: []string{strings.Repeat("b", 32)}}
	_, _, _, _, _, err = waitForGCSMixedNaturalDeniedStartup(ctx, source, pinned, fixture.preflight, expected, 0,
		"containerd://c0", fixture.imageID, fixture.podSpecHash)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("missing denial did not fail at the existing deadline: %v", err)
	}
	if reads := fixture.logReads(); reads < 1 {
		t.Fatalf("waiter reached its deadline without inspecting authority logs: reads=%d", reads)
	}
}

type gcsMixedFakeKubectlFixture struct {
	preflight, denialRow []byte
	imageID, podSpecHash string
	authorityPodPath     string
	logReads             func() int
}

func installGCSMixedFakeKubectl(t *testing.T, emitDenial bool) gcsMixedFakeKubectlFixture {
	t.Helper()
	dir := t.TempDir()
	digest := strings.Repeat("a", 64)
	image := "registry.example/rhiza@sha256:" + digest
	imageID := "docker-pullable://" + image
	preflight := []byte("2026/10/25 11:59:59 pinned preflight row\n")
	event := gcsStartupDenialEvent{Event: "startup_registration_denial", Action: "register", Result: "closed", HTTPStatus: 410,
		Stage: "startup-grant", Namespace: "ns", StatefulSetUID: "sts-uid", PodUID: "learner-pod-uid", SourceClusterID: "source",
		AttemptedRunID: strings.Repeat("c", 32), RequestedNodeID: "rhiza-2", GrantResolved: false}
	eventJSON, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	denialRow := append([]byte("2026/10/25 12:00:00 "), append(eventJSON, '\n')...)
	writeJSON := func(name string, value any) string {
		t.Helper()
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	deploymentPath := writeJSON("deployment.json", obj{"metadata": obj{"uid": "authority-deployment-uid", "namespace": "ns"}, "spec": obj{"replicas": 1}})
	replicasetPath := writeJSON("replicaset.json", obj{"metadata": obj{"uid": "authority-rs-uid", "ownerReferences": []any{
		obj{"kind": "Deployment", "uid": "authority-deployment-uid", "controller": true}}}})
	authorityPodPath := writeJSON("authority-pod.json", obj{"metadata": obj{"name": "authority-pod", "uid": "authority-pod-uid", "namespace": "ns",
		"ownerReferences": []any{obj{"kind": "ReplicaSet", "name": "authority-rs", "uid": "authority-rs-uid", "controller": true}}},
		"spec": obj{"containers": []any{obj{"name": "authority", "image": image}}},
		"status": obj{"phase": "Running", "containerStatuses": []any{obj{"name": "authority", "ready": true,
			"containerID": "containerd://authority-c0", "imageID": imageID, "restartCount": 0}}}})
	podSpec := obj{"containers": []any{obj{"name": "rhiza", "image": image}}}
	learnerPod := func(restarted bool) obj {
		containerID, restarts := "containerd://c0", 0
		lastState := obj{}
		if restarted {
			containerID, restarts = "containerd://c2", 2
			lastState = obj{"terminated": obj{"containerID": "containerd://c1", "exitCode": 1}}
		}
		return obj{"metadata": obj{"name": "rhiza-2", "uid": "learner-pod-uid", "namespace": "ns",
			"ownerReferences": []any{obj{"kind": "StatefulSet", "uid": "sts-uid", "controller": true}}},
			"spec": podSpec, "status": obj{"phase": "Running", "containerStatuses": []any{obj{"name": "rhiza", "containerID": containerID,
				"imageID": imageID, "restartCount": restarts, "lastState": lastState}}}}
	}
	learnerZeroPath := writeJSON("learner-zero.json", learnerPod(false))
	learnerCurrentPath := writeJSON("learner-current.json", learnerPod(true))
	beforePath := filepath.Join(dir, "before.log")
	if err := os.WriteFile(beforePath, preflight, 0600); err != nil {
		t.Fatal(err)
	}
	afterPath := filepath.Join(dir, "after.log")
	if err := os.WriteFile(afterPath, append(append([]byte(nil), preflight...), denialRow...), 0600); err != nil {
		t.Fatal(err)
	}
	countPath := filepath.Join(dir, "authority-logs-count")
	previousLogsPath := filepath.Join(dir, "learner-previous.log")
	if err := os.WriteFile(previousLogsPath, []byte("2026/10/25 12:00:00 "+fencing.ErrClosed.Error()+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	script := `#!/bin/sh
set -eu
case " $* " in
  *" get deployment rhiza-fencer "*) cat "$GCS_MIXED_FAKE_DEPLOYMENT" ;;
  *" get replicaset authority-rs "*) cat "$GCS_MIXED_FAKE_REPLICASET" ;;
  *" get pod authority-pod "*) cat "$GCS_MIXED_FAKE_AUTHORITY_POD" ;;
  *" get pod rhiza-2 "*)
    n=0
    if [ -f "$GCS_MIXED_FAKE_LOG_COUNT" ]; then n=$(cat "$GCS_MIXED_FAKE_LOG_COUNT"); fi
    if [ "$n" -ge 3 ]; then cat "$GCS_MIXED_FAKE_LEARNER_CURRENT"; else cat "$GCS_MIXED_FAKE_LEARNER_ZERO"; fi
    ;;
  *" logs authority-pod "*)
    n=0
    if [ -f "$GCS_MIXED_FAKE_LOG_COUNT" ]; then n=$(cat "$GCS_MIXED_FAKE_LOG_COUNT"); fi
    n=$((n + 1))
    printf '%s\n' "$n" > "$GCS_MIXED_FAKE_LOG_COUNT"
    if [ "$GCS_MIXED_FAKE_EMIT_DENIAL" = 1 ] && [ "$n" -ge 3 ]; then cat "$GCS_MIXED_FAKE_AFTER"; else cat "$GCS_MIXED_FAKE_BEFORE"; fi
    ;;
  *" logs rhiza-2 "*) cat "$GCS_MIXED_FAKE_PREVIOUS_LOG" ;;
  *) printf 'unexpected fake kubectl invocation: %s\n' "$*" >&2; exit 19 ;;
esac
`
	if err := os.WriteFile(filepath.Join(dir, "kubectl"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("RHIZA_OPERATOR_IMAGE", image)
	t.Setenv("GCS_MIXED_FAKE_DEPLOYMENT", deploymentPath)
	t.Setenv("GCS_MIXED_FAKE_REPLICASET", replicasetPath)
	t.Setenv("GCS_MIXED_FAKE_AUTHORITY_POD", authorityPodPath)
	t.Setenv("GCS_MIXED_FAKE_LEARNER_ZERO", learnerZeroPath)
	t.Setenv("GCS_MIXED_FAKE_LEARNER_CURRENT", learnerCurrentPath)
	t.Setenv("GCS_MIXED_FAKE_BEFORE", beforePath)
	t.Setenv("GCS_MIXED_FAKE_AFTER", afterPath)
	t.Setenv("GCS_MIXED_FAKE_LOG_COUNT", countPath)
	t.Setenv("GCS_MIXED_FAKE_PREVIOUS_LOG", previousLogsPath)
	t.Setenv("GCS_MIXED_FAKE_EMIT_DENIAL", map[bool]string{true: "1", false: "0"}[emitDenial])
	return gcsMixedFakeKubectlFixture{preflight: preflight, denialRow: denialRow, imageID: imageID,
		podSpecHash: digestJSON(podSpec), authorityPodPath: authorityPodPath, logReads: func() int {
			data, err := os.ReadFile(countPath)
			if err != nil {
				return 0
			}
			count, _ := strconv.Atoi(strings.TrimSpace(string(data)))
			return count
		}}
}

func TestGCSMixedAuthorityLogReadDistinguishesCancellationCaptureFailureAndIdentityDrift(t *testing.T) {
	t.Run("canceled capture preserves context error", func(t *testing.T) {
		installGCSMixedFakeKubectl(t, false)
		oldNamespace := namespace
		namespace = "ns"
		t.Cleanup(func() { namespace = oldNamespace })
		source := gcsRecoverySource{Namespace: "ns", StatefulSetUID: "sts-uid", Container: "rhiza", FencerPod: "authority-pod"}
		pinned, err := captureGCSAuthorityLogSource(t.Context(), source)
		if err != nil {
			t.Fatalf("pin fake authority source: %v", err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err = readGCSAuthorityLogs(ctx, source, pinned)
		if !errors.Is(err, context.Canceled) || strings.Contains(fmt.Sprint(err), "identity changed") {
			t.Fatalf("canceled capture was misclassified: %v", err)
		}
	})

	t.Run("live capture failure is not identity drift", func(t *testing.T) {
		installGCSMixedFakeKubectl(t, false)
		oldNamespace := namespace
		namespace = "ns"
		t.Cleanup(func() { namespace = oldNamespace })
		source := gcsRecoverySource{Namespace: "ns", StatefulSetUID: "sts-uid", Container: "rhiza", FencerPod: "authority-pod"}
		pinned, err := captureGCSAuthorityLogSource(t.Context(), source)
		if err != nil {
			t.Fatalf("pin fake authority source: %v", err)
		}
		t.Setenv("GCS_MIXED_FAKE_AUTHORITY_POD", filepath.Join(t.TempDir(), "missing-authority-pod.json"))
		_, err = readGCSAuthorityLogs(t.Context(), source, pinned)
		if err == nil || strings.Contains(err.Error(), "identity changed") || !strings.Contains(err.Error(), "capture failed") {
			t.Fatalf("live capture failure was not distinguished from identity drift: %v", err)
		}
	})

	t.Run("successful recapture with changed identity remains fatal", func(t *testing.T) {
		fixture := installGCSMixedFakeKubectl(t, false)
		oldNamespace := namespace
		namespace = "ns"
		t.Cleanup(func() { namespace = oldNamespace })
		source := gcsRecoverySource{Namespace: "ns", StatefulSetUID: "sts-uid", Container: "rhiza", FencerPod: "authority-pod"}
		pinned, err := captureGCSAuthorityLogSource(t.Context(), source)
		if err != nil {
			t.Fatalf("pin fake authority source: %v", err)
		}
		data, err := os.ReadFile(fixture.authorityPodPath)
		if err != nil {
			t.Fatal(err)
		}
		var pod obj
		if err := json.Unmarshal(data, &pod); err != nil {
			t.Fatal(err)
		}
		pod["metadata"].(map[string]any)["uid"] = "replacement-authority-pod-uid"
		updated, err := json.Marshal(pod)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(fixture.authorityPodPath, updated, 0600); err != nil {
			t.Fatal(err)
		}
		_, err = readGCSAuthorityLogs(t.Context(), source, pinned)
		if err == nil || !strings.Contains(err.Error(), "identity changed") {
			t.Fatalf("changed authority identity was not rejected: %v", err)
		}
	})
}

func TestGCSMixedForwardClosePreservesUnexpectedExitAndLogCloseFailure(t *testing.T) {
	startForwardProcess := func(command string) *forward {
		t.Helper()
		log, err := os.CreateTemp(t.TempDir(), "mixed-forward-*.log")
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command("sh", "-c", command)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		f := &forward{cmd: cmd, done: make(chan error, 1), log: log}
		go func() { f.done <- cmd.Wait() }()
		t.Cleanup(func() { _ = f.close() })
		return f
	}

	t.Run("unexpected process exit remains fatal", func(t *testing.T) {
		f := startForwardProcess("exit 23")
		deadline := time.Now().Add(time.Second)
		for len(f.done) == 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if len(f.done) == 0 {
			t.Fatal("fake forward did not exit before close")
		}
		if err := f.close(); err == nil {
			t.Fatal("unexpected nonzero forward exit was treated as successful cleanup")
		}
	})

	t.Run("log close failure remains fatal", func(t *testing.T) {
		f := startForwardProcess("sleep 30")
		if err := f.log.Close(); err != nil {
			t.Fatal(err)
		}
		if err := f.close(); err == nil {
			t.Fatal("owned process/log cleanup failure was not reported")
		}
	})

	t.Run("owned close completes before remote termination event", func(t *testing.T) {
		f := startForwardProcess("sleep 30")
		sequence := []string{"last-source-use"}
		if err := f.close(); err != nil {
			t.Fatalf("owned forward close failed before target termination: %v", err)
		}
		sequence = append(sequence, "owned-forward-close")
		sequence = append(sequence, "remote-target-termination")
		if len(sequence) != 3 || sequence[0] != "last-source-use" || sequence[1] != "owned-forward-close" || sequence[2] != "remote-target-termination" {
			t.Fatalf("forward cleanup did not precede target termination: %v", sequence)
		}
	})
}

func TestGCSMixedSourceForwardsCloseAfterLastUseBeforeGenerationTerminalization(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not locate mixed regression test source")
	}
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, filepath.Join(filepath.Dir(testFile), "gcs_ha_mixed_pending.go"), nil, 0)
	if err != nil {
		t.Fatalf("parse mixed qualifier source: %v", err)
	}
	var qualify *ast.FuncDecl
	ast.Inspect(parsed, func(node ast.Node) bool {
		if function, ok := node.(*ast.FuncDecl); ok && function.Name.Name == "qualifyGCSMixedPending" {
			qualify = function
			return false
		}
		return true
	})
	if qualify == nil {
		t.Fatal("mixed qualifier function is missing")
	}
	callHasString := func(call *ast.CallExpr, value string) bool {
		for _, argument := range call.Args {
			literal, ok := argument.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				continue
			}
			decoded, err := strconv.Unquote(literal.Value)
			if err == nil && decoded == value {
				return true
			}
		}
		return false
	}
	var baselineEnd, deploymentRead, quorumWaitEnd, generationWait token.Pos
	var observerClosePositions, survivorClosePositions []token.Pos
	ast.Inspect(qualify.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch function := call.Fun.(type) {
		case *ast.Ident:
			if function.Name == "writeGCSAcknowledgedRow" && callHasString(call, "gcs-mixed-pending-baseline") {
				baselineEnd = call.End()
			}
			if function.Name == "get" && callHasString(call, "deployment") && callHasString(call, "rhiza-fencer") {
				deploymentRead = call.Pos()
			}
			if function.Name == "wait" && callHasString(call, "one current survivor observes lost quorum") {
				quorumWaitEnd = call.End()
			}
			if function.Name == "waitForGCSMixedPendingFence" {
				generationWait = call.Pos()
			}
		case *ast.SelectorExpr:
			if function.Sel.Name != "close" {
				break
			}
			if receiver, ok := function.X.(*ast.Ident); ok && receiver.Name == "observer" {
				observerClosePositions = append(observerClosePositions, call.Pos())
			}
			if receiver, ok := function.X.(*ast.Ident); ok && receiver.Name == "forward" {
				survivorClosePositions = append(survivorClosePositions, call.Pos())
			}
		}
		return true
	})
	observerClosedAfterUse := false
	for _, position := range observerClosePositions {
		observerClosedAfterUse = observerClosedAfterUse || (baselineEnd != token.NoPos && position > baselineEnd && position < deploymentRead)
	}
	if !observerClosedAfterUse {
		t.Fatal("baseline observer must be explicitly closed after its final baseline ACK and before source-fence work")
	}
	survivorClosedAfterUse := false
	for _, position := range survivorClosePositions {
		survivorClosedAfterUse = survivorClosedAfterUse || (quorumWaitEnd != token.NoPos && position > quorumWaitEnd && position < generationWait)
	}
	if !survivorClosedAfterUse {
		t.Fatal("survivor forwards must be explicitly closed after the final quorum-loss read and before generation terminalization")
	}
}

func TestGCSMixedStartupDenialErrClosedRowIsExactAndUnambiguous(t *testing.T) {
	valid := []byte("2026/10/25 12:00:00 " + fencing.ErrClosed.Error() + "\n")
	if row, err := selectGCSClientErrClosedRow(valid); err != nil || row == "" {
		t.Fatalf("exact supervisor ErrClosed was not selected: row=%q err=%v", row, err)
	}
	for name, logs := range map[string][]byte{
		"missing":             []byte("2026/10/25 12:00:00 startup failed\n"),
		"unstructured":        []byte("prefix " + fencing.ErrClosed.Error() + "\n"),
		"wrapped error":       []byte("2026/10/25 12:00:00 context: " + fencing.ErrClosed.Error() + "\n"),
		"ambiguous duplicate": append(append([]byte(nil), valid...), valid...),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := selectGCSClientErrClosedRow(logs); err == nil {
				t.Fatal("accepted missing or ambiguous client ErrClosed evidence")
			}
		})
	}
}

func TestGCSMixedPendingTargetsKeepRequestAndQuorumDomainsSeparate(t *testing.T) {
	voters := []operator.FenceTarget{
		mixedTarget("voter-0", "voter-pod-0", "voter-uid-0", "wal-0"),
		mixedTarget("voter-1", "voter-pod-1", "voter-uid-1", "wal-1"),
		mixedTarget("voter-2", "voter-pod-2", "voter-uid-2", "wal-2"),
	}
	learner := mixedTarget("learner-0", "learner-pod", "learner-uid", "learner-wal")
	identity := fencing.Identity{SourceClusterID: "source", NodeID: learner.NodeID, PodName: learner.Pod, PodUID: learner.PodUID,
		RunID: "learner-run", WALIdentity: learner.WALIdentity, AssignmentID: strings.Repeat("a", 64)}
	learnerRuntime := mixedRuntime(gcsRecoverySource{}, identity)
	got, err := newGCSMixedPendingTargets(voters, voters[1], learner, learner, identity, learnerRuntime)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.OriginalVoters) != 3 || len(got.CurrentSurvivors) != 2 || len(got.ExpectedGenerationTargets) != 4 ||
		got.CurrentSurvivors[0] == voters[1] || got.CurrentSurvivors[1] == voters[1] {
		t.Fatalf("mixed target domains were not kept distinct: %+v", got)
	}
	for _, bad := range []struct {
		name     string
		voters   []operator.FenceTarget
		removed  operator.FenceTarget
		learner  operator.FenceTarget
		identity fencing.Identity
	}{
		{name: "missing source voter", voters: voters[:2], removed: voters[1], learner: learner, identity: identity},
		{name: "removed target outside original cohort", voters: voters, removed: mixedTarget("foreign", "foreign-pod", "foreign-uid", "foreign-wal"), learner: learner, identity: identity},
		{name: "learner aliases source voter", voters: voters, removed: voters[1], learner: voters[0], identity: identity},
		{name: "learner UID mismatch", voters: voters, removed: voters[1], learner: learner, identity: func() fencing.Identity { i := identity; i.PodUID = "stale-uid"; return i }()},
	} {
		t.Run(bad.name, func(t *testing.T) {
			if _, err := newGCSMixedPendingTargets(bad.voters, bad.removed, bad.learner, bad.learner, bad.identity, mixedRuntime(gcsRecoverySource{}, bad.identity)); err == nil {
				t.Fatal("accepted incomplete or aliased mixed target expectation")
			}
		})
	}
}

func TestGCSMixedPendingTargetUsesCanonicalRuntimeAndAllowsOnlyRemovedOrdinalReuse(t *testing.T) {
	voters := []operator.FenceTarget{
		mixedTarget("voter-0", "rhiza-0", "voter-uid-0", "wal-0"),
		mixedTarget("voter-1", "rhiza-1", "voter-uid-1", "wal-1"),
		mixedTarget("voter-2", "rhiza-2", "voter-uid-2", "wal-2"),
	}
	removed := voters[2]
	identity := fencing.Identity{SourceClusterID: "source", NodeID: "learner-new", PodName: removed.Pod, PodUID: "replacement-uid",
		RunID: "replacement-run", WALIdentity: "replacement-wal", AssignmentID: strings.Repeat("c", 64)}
	runtime := mixedRuntime(gcsRecoverySource{BindingUID: "binding", StatefulSetUID: "sts"}, identity)
	pod := obj{"metadata": obj{"name": identity.PodName, "uid": identity.PodUID},
		"spec":   obj{"nodeName": runtime.NodeName},
		"status": obj{"containerStatuses": []any{obj{"name": "rhiza", "containerID": runtime.ContainerID, "imageID": runtime.ImageID}}}}
	node := obj{"metadata": obj{"name": runtime.NodeName, "uid": runtime.NodeUID}}
	captured, err := captureGCSMixedPendingTarget(identity, runtime, pod, node)
	if err != nil {
		t.Fatalf("capturedTarget projection rejected canonical same-ordinal learner: %v", err)
	}
	if captured.NodeUID != runtime.NodeUID || captured.Pod != removed.Pod || captured.PodUID == removed.PodUID {
		t.Fatalf("captured request target did not preserve physical-incarnation distinction: %+v", captured)
	}
	requestProjection := captured
	requestProjection.NodeUID = ""
	if _, err := newGCSMixedPendingTargets(voters, removed, captured, requestProjection, identity, runtime); err != nil {
		t.Fatalf("canonical same-ordinal replacement was rejected: %v", err)
	}

	for _, tc := range []struct {
		name    string
		pod     obj
		node    obj
		id      fencing.Identity
		runtime fencing.RuntimeBinding
	}{
		{name: "node uid reuse", pod: pod, node: obj{"metadata": obj{"name": runtime.NodeName, "uid": "foreign-node-uid"}}, id: identity, runtime: runtime},
		{name: "pod moved from canonical node", pod: obj{"metadata": pod["metadata"], "spec": obj{"nodeName": "other-node"}, "status": pod["status"]}, node: node, id: identity, runtime: runtime},
		{name: "container incarnation changed", pod: obj{"metadata": pod["metadata"], "spec": pod["spec"], "status": obj{"containerStatuses": []any{obj{"name": "rhiza", "containerID": "containerd://changed", "imageID": runtime.ImageID}}}}, node: node, id: identity, runtime: runtime},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := captureGCSMixedPendingTarget(tc.id, tc.runtime, tc.pod, tc.node); err == nil {
				t.Fatal("accepted learner runtime identity that differs from canonical evidence")
			}
		})
	}

	for _, tc := range []struct {
		name    string
		learner operator.FenceTarget
		id      fencing.Identity
		runtime fencing.RuntimeBinding
	}{
		{name: "same uid reused", learner: func() operator.FenceTarget { target := captured; target.PodUID = removed.PodUID; return target }(), id: func() fencing.Identity { id := identity; id.PodUID = removed.PodUID; return id }(), runtime: func() fencing.RuntimeBinding { r := runtime; r.PodUID = removed.PodUID; return r }()},
		{name: "survivor pod collision", learner: func() operator.FenceTarget { target := captured; target.Pod = voters[0].Pod; return target }(), id: func() fencing.Identity { id := identity; id.PodName = voters[0].Pod; return id }(), runtime: func() fencing.RuntimeBinding { r := runtime; r.PodName = voters[0].Pod; return r }()},
		{name: "logical identity reused", learner: func() operator.FenceTarget { target := captured; target.NodeID = removed.NodeID; return target }(), id: func() fencing.Identity { id := identity; id.NodeID = removed.NodeID; return id }(), runtime: func() fencing.RuntimeBinding { r := runtime; r.NodeID = removed.NodeID; return r }()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			projection := tc.learner
			projection.NodeUID = ""
			if _, err := newGCSMixedPendingTargets(voters, removed, tc.learner, projection, tc.id, tc.runtime); err == nil {
				t.Fatal("accepted UID/logical/survivor identity collision")
			}
		})
	}
}

func TestMixedPendingScenarioSelectsItsSourceGateBeforeManifestRendering(t *testing.T) {
	t.Setenv(gcsHAScenarioEnv, "")
	t.Setenv(gcsMixedRemoveGateSource, "")
	if err := configureGCSMixedPendingSelection(); err != nil {
		t.Fatal(err)
	}
	if os.Getenv(gcsMixedRemoveGateSource) != "" {
		t.Fatal("default scenario unexpectedly enabled the REMOVE gate")
	}
	t.Setenv(gcsHAScenarioEnv, gcsHAMixedPendingScenario)
	if err := configureGCSMixedPendingSelection(); err != nil {
		t.Fatal(err)
	}
	if got := os.Getenv(gcsMixedRemoveGateSource); got != gcsHAMixedPendingSource {
		t.Fatalf("mixed selector gate source=%q, want %q", got, gcsHAMixedPendingSource)
	}
	var statefulSet obj
	for _, item := range manifests()["items"].([]obj) {
		if item["kind"] == "StatefulSet" && nested(item, "metadata", "name") == "rhiza" {
			statefulSet = item
		}
	}
	containers, _ := nested(statefulSet, "spec", "template", "spec", "containers").([]obj)
	if len(containers) != 1 {
		t.Fatal("mixed selector manifest omitted the source container")
	}
	var found bool
	for _, item := range containers[0]["env"].([]obj) {
		if item["name"] == gcsMixedRemoveGateSource && item["value"] == gcsHAMixedPendingSource {
			found = true
		}
	}
	if !found {
		t.Fatal("mixed selector did not pin the gate into the source Pod template")
	}
	t.Setenv(gcsMixedRemoveGateSource, "wrong-source")
	if err := configureGCSMixedPendingSelection(); err == nil {
		t.Fatal("accepted a gate source that differs from the dedicated source fixture")
	}
	t.Setenv(gcsHAScenarioEnv, "unknown")
	if err := configureGCSMixedPendingSelection(); err == nil {
		t.Fatal("accepted an unknown GCS HA scenario selector")
	}
}

func TestGCSMixedGatePreflightAndPostFaultRejectIdentityViolations(t *testing.T) {
	clean := gcsMixedGateSnapshot{}
	if !gcsMixedGateHasNoViolations(clean) {
		t.Fatal("clean gate telemetry was rejected")
	}
	for _, tc := range []struct {
		name   string
		mutate func(*gcsMixedGateSnapshot)
	}{
		{name: "unexpected add", mutate: func(s *gcsMixedGateSnapshot) { s.UnexpectedAdditions = 1 }},
		{name: "capture failure", mutate: func(s *gcsMixedGateSnapshot) { s.CaptureFailures = 1 }},
		{name: "identity violation", mutate: func(s *gcsMixedGateSnapshot) { s.IdentityViolations = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := clean
			tc.mutate(&snapshot)
			if gcsMixedGateHasNoViolations(snapshot) {
				t.Fatalf("mixed gate verifier accepted violation telemetry: %+v", snapshot)
			}
		})
	}
}

func TestGCSMixedPendingProofRequiresThreeV2VotersAndOneCanonicalV3Learner(t *testing.T) {
	voters := []operator.FenceTarget{
		mixedTarget("voter-0", "voter-pod-0", "voter-uid-0", "wal-0"),
		mixedTarget("voter-1", "voter-pod-1", "voter-uid-1", "wal-1"),
		mixedTarget("voter-2", "voter-pod-2", "voter-uid-2", "wal-2"),
	}
	learnerTarget := mixedTarget("learner-0", "learner-pod", "learner-uid", "learner-wal")
	learnerID := fencing.Identity{SourceClusterID: "source", NodeID: learnerTarget.NodeID, PodName: learnerTarget.Pod, PodUID: learnerTarget.PodUID,
		RunID: "learner-run", WALIdentity: learnerTarget.WALIdentity, AssignmentID: strings.Repeat("a", 64)}
	source := gcsRecoverySource{ClusterID: "source", Namespace: "qualification", LogicalID: "logical", StatefulSet: "rhiza",
		StatefulSetUID: "sts-uid", BindingUID: "binding-uid", Bucket: "bucket", Prefix: "source-prefix", Targets: voters}
	learnerRequestTarget := learnerTarget
	learnerRequestTarget.NodeUID = ""
	expected, err := newGCSMixedPendingTargets(voters, voters[2], learnerTarget, learnerRequestTarget, learnerID, mixedRuntime(source, learnerID))
	if err != nil {
		t.Fatal(err)
	}
	if expected.PendingLearnerEvidence.NodeUID == "" || expected.PendingLearner.NodeUID != "" || expected.ExpectedGenerationTargets[3] != learnerRequestTarget {
		t.Fatalf("runtime evidence and production request projection were not kept distinct: evidence=%+v request=%+v targets=%+v",
			expected.PendingLearnerEvidence, expected.PendingLearner, expected.ExpectedGenerationTargets)
	}
	request := operator.FenceRequest{LogicalID: source.LogicalID, BindingUID: source.BindingUID, Namespace: source.Namespace,
		StatefulSet: source.StatefulSet, StatefulSetUID: source.StatefulSetUID, SourceClusterID: source.ClusterID,
		OperationID: "generation-fence", Scope: operator.ScopeGeneration, Targets: expected.ExpectedGenerationTargets}
	requestHash := digestJSON(request)
	removedVoterPriorRequest := operator.FenceRequest{LogicalID: source.LogicalID, BindingUID: source.BindingUID, Namespace: source.Namespace,
		StatefulSet: source.StatefulSet, StatefulSetUID: source.StatefulSetUID, SourceClusterID: source.ClusterID,
		OperationID: "removed-voter-prior-fence", Scope: operator.ScopeVoter, Targets: []operator.FenceTarget{expected.RemovedVoter}}
	scope := operator.AttestationStoreScope{Provider: "gcs", Bucket: source.Bucket, Prefix: source.Prefix}
	scopeDigest := digestJSON(scope)
	root := fenceLedgerState{StatefulSetUID: source.StatefulSetUID, Ledger: fencing.Ledger{SourceClusterID: source.ClusterID,
		ClosedBy: request.OperationID, ClosedHash: requestHash, RequireRuntimeBinding: true, RequireMutationJournal: true, Runs: map[string]fencing.Run{}}}
	var voterRuns []gcsMixedVoterRun
	var terminals []fencing.TerminalReference
	var removedVoterRef fencing.TerminalReference
	removedVoterTerminalIndex := -1
	for i, target := range voters {
		id := fencing.Identity{SourceClusterID: source.ClusterID, NodeID: target.NodeID, PodName: target.Pod, PodUID: target.PodUID,
			RunID: fmt.Sprintf("voter-run-%d", i), WALIdentity: target.WALIdentity}
		runtime := mixedRuntime(source, id)
		terminalRequest := request
		if target == expected.RemovedVoter {
			terminalRequest = removedVoterPriorRequest
		}
		ref := mixedTerminal(2, terminalRequest, id, runtime, scopeDigest, "producer-"+id.RunID)
		if target == expected.RemovedVoter {
			removedVoterRef = ref
			removedVoterTerminalIndex = len(terminals)
		}
		root.Ledger.Runs[id.RunID] = fencing.Run{Identity: id, Role: fencing.StartupRoleVoterActive, Stop: &ref.Request,
			Runtime: &runtime, MutationProducerUID: ref.ProducerUID, IndependentTerminal: &ref}
		voterRuns = append(voterRuns, gcsMixedVoterRun{Target: target, Identity: id, Runtime: runtime, Producer: ref.ProducerUID})
		terminals = append(terminals, ref)
	}
	expected.RetainedRemovedVoterTerminal = removedVoterRef
	learnerRuntime := mixedRuntime(source, learnerID)
	learnerRef := mixedTerminal(3, request, learnerID, learnerRuntime, scopeDigest, "")
	root.Ledger.Runs[learnerID.RunID] = fencing.Run{Identity: learnerID, Role: fencing.StartupRoleLearnerPendingRemove, Stop: &learnerRef.Request,
		Runtime: &learnerRuntime, IndependentTerminal: &learnerRef}
	terminals = append(terminals, learnerRef)
	evidence := &operator.IndependentEvidenceV2{Version: 2, RequestHash: requestHash, LedgerHash: strings.Repeat("b", 64), StorageScope: scope, Terminals: terminals}
	proof := operator.FenceProof{OperationID: request.OperationID, SourceClusterID: source.ClusterID, BindingUID: source.BindingUID,
		RequestHash: requestHash, ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true, IndependentV2: evidence}
	proof.ProofID = "independent-v2-" + digestJSON(evidence)
	item := obj{"spec": request, "status": obj{"proof": proof}}
	if _, _, err := verifyGCSMixedPendingGenerationProof(item, source, expected, voterRuns, root); err != nil {
		t.Fatalf("valid mixed source proof rejected: %v", err)
	}
	if err := verifyGCSMixedDeniedRootSafety(root, root, request, proof, expected, "new-supervisor-run"); err != nil {
		t.Fatalf("unchanged exact closure and terminal set rejected: %v", err)
	}
	if !hasGCSMixedRootVersionEvidence(&objstore.ObjectVersion{Type: objstore.ETag, Value: "etag-before"},
		&objstore.ObjectVersion{Type: objstore.ETag, Value: "etag-after"}) {
		t.Fatal("distinct pre/post CAS versions should be retained as evidence without requiring equality")
	}
	if !hasGCSMixedRootVersionEvidence(&objstore.ObjectVersion{Type: objstore.Generation, Value: "41"},
		&objstore.ObjectVersion{Type: objstore.Generation, Value: "42"}) {
		t.Fatal("distinct generation pre/post CAS versions should be retained")
	}
	if hasGCSMixedRootVersionEvidence(nil, &objstore.ObjectVersion{Type: objstore.ETag, Value: "etag-after"}) ||
		!hasGCSMixedRootVersionEvidence(&objstore.ObjectVersion{Type: objstore.ETag, Value: "etag-before"}, &objstore.ObjectVersion{Type: objstore.ETag, Value: "etag-after"}) {
		t.Fatal("incomplete CAS version evidence was accepted or complete differing versions were rejected")
	}

	cloneRoot := func(value fenceLedgerState) fenceLedgerState {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		var clone fenceLedgerState
		if err := json.Unmarshal(data, &clone); err != nil {
			t.Fatal(err)
		}
		return clone
	}
	safeMetadataChange := cloneRoot(root)
	safeMetadataChange.Ledger.ArchiveScopeHash = strings.Repeat("9", 64)
	if err := verifyGCSMixedDeniedRootSafety(root, safeMetadataChange, request, proof, expected, "new-supervisor-run"); err != nil {
		t.Fatalf("unrelated ledger metadata with unchanged closure/refs should be allowed: %v", err)
	}
	lateLedgerTransition := cloneRoot(root)
	removedRun := lateLedgerTransition.Ledger.Runs[removedVoterRef.Request.Identity.RunID]
	if removedRun.Stop == nil {
		t.Fatal("removed voter fixture has no retained prior stop request")
	}
	if err := lateLedgerTransition.Ledger.RecordDrain(fencing.DrainReceipt{Request: *removedRun.Stop, WritersJoined: true}); err != nil {
		t.Fatalf("construct legal post-terminal drain transition: %v", err)
	}
	if err := lateLedgerTransition.Ledger.RecordExit(*removedRun.Stop); err != nil {
		t.Fatalf("construct legal post-terminal exit transition: %v", err)
	}
	if err := verifyGCSMixedDeniedRootSafety(root, lateLedgerTransition, request, proof, expected, "new-supervisor-run"); err != nil {
		t.Fatalf("valid monotonic RecordDrain/RecordExit update was rejected: %v", err)
	}
	closureCheckpoint := cloneRoot(root)
	for runID, run := range closureCheckpoint.Ledger.Runs {
		if runID == removedVoterRef.Request.Identity.RunID {
			continue // the removed voter retains its prior V2 terminal reference.
		}
		run.IndependentTerminal = nil
		closureCheckpoint.Ledger.Runs[runID] = run
	}
	if err := verifyGCSMixedDeniedRootSafety(closureCheckpoint, root, request, proof, expected, "new-supervisor-run"); err != nil {
		t.Fatalf("rejected closure checkpoint whose independent terminals completed after closure: %v", err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*fencing.Run)
	}{
		{name: "changed late drain request", mutate: func(run *fencing.Run) {
			run.Drain.Request.RequestHash = strings.Repeat("9", 64)
		}},
		{name: "changed stop request", mutate: func(run *fencing.Run) {
			run.Stop.RequestHash = strings.Repeat("9", 64)
		}},
		{name: "exit state regressed", mutate: func(run *fencing.Run) { run.Exited = false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			invalid := cloneRoot(lateLedgerTransition)
			run := invalid.Ledger.Runs[removedVoterRef.Request.Identity.RunID]
			tc.mutate(&run)
			invalid.Ledger.Runs[removedVoterRef.Request.Identity.RunID] = run
			if err := verifyGCSMixedDeniedRootSafety(lateLedgerTransition, invalid, request, proof, expected, "new-supervisor-run"); err == nil {
				t.Fatal("accepted a changed stop/drain or regressed exit state")
			}
		})
	}
	for _, tc := range []struct {
		name   string
		mutate func(*fenceLedgerState)
	}{
		{name: "closed operation changed", mutate: func(state *fenceLedgerState) { state.Ledger.ClosedBy = "other-fence" }},
		{name: "closed request hash changed", mutate: func(state *fenceLedgerState) { state.Ledger.ClosedHash = strings.Repeat("9", 64) }},
		{name: "terminal reference changed", mutate: func(state *fenceLedgerState) {
			run := state.Ledger.Runs[learnerID.RunID]
			ref := *run.IndependentTerminal
			ref.ProofUID = "different-proof-uid"
			run.IndependentTerminal = &ref
			state.Ledger.Runs[learnerID.RunID] = run
		}},
		{name: "attempt run added", mutate: func(state *fenceLedgerState) {
			state.Ledger.Runs["new-supervisor-run"] = fencing.Run{Identity: fencing.Identity{SourceClusterID: source.ClusterID, NodeID: learnerID.NodeID,
				PodName: learnerID.PodName, PodUID: learnerID.PodUID, RunID: "new-supervisor-run"}}
		}},
		{name: "learner promoted", mutate: func(state *fenceLedgerState) {
			run := state.Ledger.Runs[learnerID.RunID]
			run.Promotion = &fencing.LearnerPromotion{OperationID: "unapproved", EvidenceHash: strings.Repeat("b", 64)}
			state.Ledger.Runs[learnerID.RunID] = run
		}},
		{name: "learner producer added", mutate: func(state *fenceLedgerState) {
			run := state.Ledger.Runs[learnerID.RunID]
			run.MutationProducerUID = "unexpected-producer"
			state.Ledger.Runs[learnerID.RunID] = run
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			after := cloneRoot(root)
			tc.mutate(&after)
			if err := verifyGCSMixedDeniedRootSafety(root, after, request, proof, expected, "new-supervisor-run"); err == nil {
				t.Fatal("accepted changed closure, terminal, run admission, promotion, or producer authority")
			}
		})
	}

	for _, tc := range []struct {
		name   string
		mutate func(*operator.FenceRequest, *operator.IndependentEvidenceV2)
	}{
		{name: "learner omitted from request", mutate: func(r *operator.FenceRequest, _ *operator.IndependentEvidenceV2) { r.Targets = r.Targets[:3] }},
		{name: "extra request target", mutate: func(r *operator.FenceRequest, _ *operator.IndependentEvidenceV2) {
			r.Targets = append(r.Targets, mixedTarget("extra-voter", "extra-pod", "extra-uid", "extra-wal"))
		}},
		{name: "unrequested fifth terminal", mutate: func(_ *operator.FenceRequest, e *operator.IndependentEvidenceV2) {
			extraTarget := mixedTarget("extra-learner", "extra-learner-pod", "extra-learner-uid", "extra-learner-wal")
			extraID := fencing.Identity{SourceClusterID: source.ClusterID, NodeID: extraTarget.NodeID, PodName: extraTarget.Pod, PodUID: extraTarget.PodUID,
				RunID: "extra-learner-run", WALIdentity: extraTarget.WALIdentity, AssignmentID: strings.Repeat("b", 64)}
			extraRuntime := mixedRuntime(source, extraID)
			e.Terminals = append(e.Terminals, mixedTerminal(3, request, extraID, extraRuntime, scopeDigest, ""))
		}},
		{name: "learner V2 instead of V3", mutate: func(_ *operator.FenceRequest, e *operator.IndependentEvidenceV2) {
			e.Terminals[3].EvidenceVersion = 2
			e.Terminals[3].ProducerUID = "producer-learner"
			e.Terminals[3].MutationReceiptDigest = strings.Repeat("c", 64)
		}},
		{name: "source voter V3 instead of V2", mutate: func(_ *operator.FenceRequest, e *operator.IndependentEvidenceV2) {
			e.Terminals[0].EvidenceVersion = 3
			e.Terminals[0].ProducerUID = ""
			e.Terminals[0].MutationReceiptDigest = ""
		}},
		{name: "duplicate terminal", mutate: func(_ *operator.FenceRequest, e *operator.IndependentEvidenceV2) { e.Terminals[3] = e.Terminals[2] }},
		{name: "removed voter prior reference rewritten", mutate: func(_ *operator.FenceRequest, e *operator.IndependentEvidenceV2) {
			e.Terminals[removedVoterTerminalIndex].Request.OperationID = "rewritten-prior-fence"
		}},
		{name: "removed voter unknown prior hash", mutate: func(_ *operator.FenceRequest, e *operator.IndependentEvidenceV2) {
			e.Terminals[removedVoterTerminalIndex].Request.RequestHash = strings.Repeat("9", 64)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := request
			candidate.Targets = append([]operator.FenceTarget(nil), request.Targets...)
			candidateEvidence := *evidence
			candidateEvidence.Terminals = append([]fencing.TerminalReference(nil), evidence.Terminals...)
			tc.mutate(&candidate, &candidateEvidence)
			candidateHash := digestJSON(candidate)
			candidateEvidence.RequestHash = candidateHash
			candidateProof := proof
			candidateProof.RequestHash = candidateHash
			candidateProof.IndependentV2 = &candidateEvidence
			candidateProof.ProofID = "independent-v2-" + digestJSON(&candidateEvidence)
			candidateItem := obj{"spec": candidate, "status": obj{"proof": candidateProof}}
			if _, _, err := verifyGCSMixedPendingGenerationProof(candidateItem, source, expected, voterRuns, root); err == nil {
				t.Fatal("accepted mixed proof with changed target or terminal role")
			}
		})
	}
}

func TestGCSMixedLearnerC0MustMatchCanonicalCheckpoint(t *testing.T) {
	for _, tc := range []struct {
		name                string
		checkpoint, current string
		wantErr             bool
	}{
		{name: "genuine checkpoint C0", checkpoint: "containerd://c0", current: "containerd://c0"},
		{name: "missing checkpoint container", current: "containerd://c0", wantErr: true},
		{name: "missing current container", checkpoint: "containerd://c0", wantErr: true},
		{name: "current container is replacement C1", checkpoint: "containerd://c0", current: "containerd://c1", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := verifyGCSMixedCheckpointContainerID(tc.checkpoint, tc.current)
			if tc.wantErr && err == nil {
				t.Fatal("accepted an empty or changed learner container identity")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("rejected the checkpointed learner container: %v", err)
			}
		})
	}
}

func mixedTarget(node, pod, uid, wal string) operator.FenceTarget {
	return operator.FenceTarget{NodeID: node, Pod: pod, PodUID: uid, WALIdentity: wal, NodeName: "node-" + node, NodeUID: "node-uid-" + node,
		ContainerID: "containerd://" + node, ImageID: "sha256:" + strings.Repeat("a", 64)}
}

func mixedRuntime(source gcsRecoverySource, id fencing.Identity) fencing.RuntimeBinding {
	bindingUID, statefulSetUID := source.BindingUID, source.StatefulSetUID
	if bindingUID == "" {
		bindingUID = "binding"
	}
	if statefulSetUID == "" {
		statefulSetUID = "sts"
	}
	return fencing.RuntimeBinding{AuthorityID: "authority", NamespaceUID: "ns-uid", BindingUID: bindingUID, StatefulSetUID: statefulSetUID,
		SourceClusterID: id.SourceClusterID, NodeID: id.NodeID, PodName: id.PodName, PodUID: id.PodUID, RunID: id.RunID, AssignmentID: id.AssignmentID,
		NodeName: "node-" + id.NodeID, NodeUID: "node-uid-" + id.NodeID, MachineID: "machine", BootID: "boot", RuntimeID: "runtime",
		SandboxID: "sandbox", ContainerID: "containerd://" + id.NodeID, ImageID: "sha256:" + strings.Repeat("a", 64)}
}

func mixedTerminal(version int, request operator.FenceRequest, id fencing.Identity, runtime fencing.RuntimeBinding, scopeDigest, producer string) fencing.TerminalReference {
	stop := fencing.StopRequest{OperationID: request.OperationID, RequestHash: digestJSON(request), Identity: id}
	ref := fencing.TerminalReference{EvidenceVersion: version, Request: stop, RuntimeHash: fencing.RuntimeBindingDigest(runtime), ProofName: "proof-" + id.RunID,
		ProofUID: "proof-uid-" + id.RunID, ProofDigest: strings.Repeat("d", 64), RuntimeReceiptDigest: strings.Repeat("e", 64), StorageScopeDigest: scopeDigest,
		ProducerUID: producer}
	if version == 2 {
		ref.MutationReceiptDigest = strings.Repeat("f", 64)
	}
	return ref
}

func digestJSON(value any) string {
	sum := sha256.Sum256(encoded(value))
	return fmt.Sprintf("%x", sum)
}
